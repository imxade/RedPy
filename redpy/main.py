import argparse
import asyncio
import time
import utils.parser as resp


async def serveRedis():
    config = getConfig()
    if config["role"] == "slave":
        reader, writer = await asyncio.open_connection(
            config["master_host"], config["master_port"]
        )
        await _handshake(config["port"], reader, writer)
        asyncio.create_task(handleClient(config, reader, writer))
    server = await asyncio.start_server(
        lambda r, w: handleClient(config, r, w), config["host"], config["port"]
    )
    print(f"Starting server on {config['host']}:{config['port']}")
    async with server:
        await server.serve_forever()


def getConfig():
    flag = argparse.ArgumentParser()
    flag.add_argument("--dir", default=".", help="Directory for redis files")
    flag.add_argument("--dbfilename", default="dump.rdb", help="Database filename")
    flag.add_argument("--port", default="6379", help="Port to listen on")
    flag.add_argument("--replicaof", help="Replication target (format: HOST PORT)")
    args = flag.parse_args()
    config = {
        "store": {},
        "port": args.port,
        "host": "0.0.0.0",
        "role": "master",
        "dbfilename": args.dbfilename,
        "dir": args.dir,
        "master_replid": "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
        "master_repl_offset": "0",
        "replicaof": args.replicaof,
        "master_host": None,
        "master_port": None,
        "replicas": [],
        "streams": {},
        "streamIDs": {},
        "streamEvent": asyncio.Event(),
        "writeCmds": {"set", "del", "incr", "xadd"},
        "replOffset": 0,
        "replAcks": {},
        "ackEvent": asyncio.Event(),
    }
    resp.read_key_val_from_db(config["dir"], config["dbfilename"], config["store"])
    if args.replicaof:
        config["role"] = "slave"
        config["master_host"], config["master_port"] = args.replicaof.split()
    return config


async def handleClient(config, reader, writer):
    session = {"multi": False, "execQ": [], "processedBytes": 0}
    while True:
        raw = await reader.read(1024)
        if not raw:
            break
        print("RAW: ", repr(raw))
        commands = raw.replace(b"\r\n*", b"\r\n *").replace(b" *\r", b"*\r").split(b" ")
        print("COMMANDS: ", repr(commands))
        for command in commands:
            if not command:
                continue
            parsed = resp.parse(command)
            cmd, args = parsed[0].lower(), parsed[1:]
            if config["role"] == "master":
                if cmd == "psync":
                    config["replicas"].append((reader, writer))
                    config["replAcks"][writer] = 0
                if cmd == "replconf" and args and args[0].lower() == "getack":
                    await propagate(config, command)
                    continue
                if cmd == "replconf" and args[0].lower() == "ack":
                    config["replAcks"][writer] = int(args[1])
                    config["ackEvent"].set()
                    continue
                if cmd in config["writeCmds"]:
                    await propagate(config, command)
            if cmd == "wait":
                num = int(args[0])
                timeout = int(args[1])
                result = await waitForReplicas(config, num, timeout)
                writer.write(resp.encode(result))
                await writer.drain()
                continue
            responses = await execute(session, config, cmd, args)
            session["processedBytes"] += len(command)
            if config["role"] == "slave" and cmd not in ["info", "get", "replconf"]:
                continue
            if responses:
                for rsp in responses:
                    writer.write(resp.encode(rsp))
                    await writer.drain()


async def execute(session, config, cmd, args):
    print("CMD:", repr(cmd), "ARGS:", repr(args))
    config["store"] = cleanStore(config["store"].copy())
    if session["multi"] and cmd not in {"exec", "discard"}:
        session["execQ"].append((cmd, args))
        return ["+QUEUED"]
    if cmd == "ping":
        return ["+PONG"]
    elif cmd == "echo":
        return [" ".join(args)]
    elif cmd == "set" and len(args) > 1:
        val = None
        print("Setting value: ", args)
        if len(args) == 2:
            val = args[1], -1
        elif args[3].isdigit():
            if args[2].lower() == "px":
                val = args[1], time.time() + int(args[3]) / 1000
        if val:
            config.get("store")[args[0]] = val
            return ["+OK"]
    elif cmd == "get" and len(args) == 1:
        result = config.get("store").get(args[0])
        return [result[0] if result else None]
    elif cmd == "del" and len(args) > 0:
        count = 0
        for arg in args:
            if arg in config["store"]:
                del config["store"][arg]
                count += 1
        return [count]
    elif cmd == "config" and len(args) > 1 and args[0].lower() == "get":
        result = []
        for attr in args[1:]:
            if attr in config:
                result.extend([attr, config[attr]])
        return [[el for el in result]]
    elif cmd == "keys" and len(args) == 1 and args[0] == "*":
        return [[el for el in list(config["store"].keys())]]
    elif cmd == "info" and len(args) == 1:
        result = f"role:{config["role"]}\n"
        if config["role"] == "master":
            result += f"master_replid:{config["master_replid"]}\n"
            result += f"master_repl_offset:{config["master_repl_offset"]}\n"
        return [result]
    elif cmd == "replconf":
        if args and args[0].lower() == "getack":
            return [["REPLCONF", "ACK", str(session["processedBytes"])]]
        return ["+OK"]
    elif cmd == "psync":
        fullsync = (
            f"+FULLRESYNC {config["master_replid"]} {config["master_repl_offset"]}"
        )
        file_contents = bytes.fromhex(
            "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        )
        return [fullsync, file_contents]
    elif cmd == "type" and len(args) == 1:
        result = "none"
        if config.get("streams").get(args[0]):
            result = "stream"
        elif config.get("store").get(args[0]):
            result = "string"
        return ["+" + result]
    elif cmd == "xadd" and len(args) > 2:
        return [
            xAdd(config["streamIDs"], config["streams"], config["streamEvent"], args)
        ]
    elif cmd == "xrange" and len(args) == 3:
        return [xRange(config["streams"].get(args[0], []), args[1], args[2])]
    elif cmd == "xread" and len(args) > 2 and len(args) % 2 != 0:
        return [await xRead(config["streams"], config["streamEvent"], args)]
    elif cmd == "xlen":
        return [len(config["streams"].get(args[0], []))]
    elif cmd == "incr":
        return [increment(config["store"], args[0])]
    elif cmd == "multi":
        session["multi"] = True
        return ["+OK"]
    elif cmd == "exec":
        if not session["multi"]:
            return ["-ERR EXEC without MULTI"]
        execQ = session["execQ"]
        session["multi"], session["execQ"] = False, []
        result = []
        for exCmd, exArgs in execQ:
            result.extend(await execute(session, config, exCmd, exArgs))
        return [result]
    elif cmd == "discard":
        if not session["multi"]:
            return ["-ERR DISCARD without MULTI"]
        session["multi"], session["execQ"] = False, []
        return ["+OK"]


def cleanStore(store):
    for key, value in list(store.items()):
        if value[1] != -1 and value[1] < time.time():
            del store[key]
    return store


async def _handshake(port, reader, writer):
    for cmd in (
        ["PING"],
        ["REPLCONF", "listening-port", port],
        ["REPLCONF", "capa", "psync2"],
        ["PSYNC", "?", "+-1"],
    ):
        writer.write(resp.encode(cmd))
        await writer.drain()
        await reader.readline()
    await receiveRdbFile(reader)


async def receiveRdbFile(reader):
    response = await reader.readline()
    size = int(response[1:-2])
    await reader.read(size)


async def propagate(config, raw):
    print("Propagating: ", repr(raw))
    config["replOffset"] += len(raw)
    for _, writer in config.get("replicas", []):
        writer.write(raw)
        await writer.drain()


async def waitForReplicas(config, num, timeoutMs):
    await sendGetAckToReplicas(config)
    deadline = time.time() + timeoutMs / 1000
    while True:
        count = sum(
            1
            for offset in config["replAcks"].values()
            if offset >= config["replOffset"]
        )
        if count >= num:
            return count
        now = time.time()
        if now >= deadline:
            break
        try:
            await asyncio.wait_for(config["ackEvent"].wait(), timeout=deadline - now)
        except asyncio.TimeoutError:
            break
        config["ackEvent"].clear()
    return sum(
        1 for offset in config["replAcks"].values() if offset >= config["replOffset"]
    )


async def sendGetAckToReplicas(config):
    msg = resp.encode(["REPLCONF", "GETACK", "*"])
    for _, writer in config["replicas"]:
        writer.write(msg)
        await writer.drain()


def xAdd(streamIDs, streams, streamEvent, args):
    streamName, entryID, *keyvals = args
    if len(keyvals) % 2 != 0:
        return "-ERR XADD requires field-value pairs"
    lastID = streamIDs.get(streamName)
    ms, sn, error = _parseEntryID(entryID, lastID)
    if error:
        return error
    streams.setdefault(streamName, []).append((ms, sn, keyvals))
    streamIDs[streamName] = (ms, sn)
    streamEvent.set()
    return f"{ms}-{sn}"


def _parseEntryID(entryID, lastID):
    if entryID == "*":
        ms = int(time.time() * 1000)
        sn = lastID[1] + 1 if lastID and lastID[0] == ms else 0
        return ms, sn, None
    try:
        ms_str, sn_str = entryID.split("-")
        ms = int(ms_str)
        if sn_str == "*":
            sn = lastID[1] + 1 if lastID and lastID[0] == ms else 0
            sn = 1 if sn == ms == 0 else sn
        else:
            sn = int(sn_str)
    except Exception:
        return None, None, "-ERR The ID specified in XADD must be a valid format"
    if ms <= 0 and sn <= 0:
        return None, None, "-ERR The ID specified in XADD must be greater than 0-0"
    if lastID and (ms < lastID[0] or (ms == lastID[0] and sn <= lastID[1])):
        return (
            None,
            None,
            "-ERR The ID specified in XADD is equal or smaller than the target stream top item",
        )
    return ms, sn, None


def xRange(entries, start, stop):
    result = []
    if not entries:
        return []
    lastMS, lastSN = entries[-1][0], entries[-1][1]
    startMS, startSN = _parseRange(start, 0, 0)
    stopMS, stopSN = _parseRange(stop, lastMS, lastSN)
    for ms, sn, keyvals in entries:
        if ms < startMS or (ms == startMS and sn < startSN):
            continue
        if ms > stopMS or (ms == stopMS and sn > stopSN):
            break
        result.append([f"{ms}-{sn}", keyvals.copy()])
    print("Streams in range: ", repr(result))
    return result


def _parseRange(idStr, defaultMS, defaultSN):
    if idStr == "-":
        return (0, 0)
    elif idStr == "+":
        return defaultMS, defaultSN
    elif "-" in idStr:
        ms, sn = idStr.split("-")
        return int(ms), int(sn)
    else:
        return int(idStr), 0


async def xRead(streams, streamEvent, args):
    block = False
    timeout = 0
    start = 1
    if args[0].lower() == "block":
        block = True
        timeout = int(args[1])
        start = 3
    keys = args[start : start + (len(args) - start) // 2]
    ids = args[start + len(keys) :]
    resolvedIds = []
    for key, idStr in zip(keys, ids):
        if idStr == "$":
            stream = streams.get(key, [])
            resolvedIds.append(f"{stream[-1][0]}-{stream[-1][1]}" if stream else "0-0")
        else:
            resolvedIds.append(idStr)
    if block:
        await _waitForKey(streamEvent, timeout)
    output = []
    for key, idStr in zip(keys, resolvedIds):
        results = xRange(streams.get(key, []), idStr, "+")
        groups = results[1:] if results[0][0] == idStr else results
        if groups:
            output.append([key, groups])
    return output if output else None


async def _waitForKey(streamEvent, timeout):
    streamEvent.clear()
    if timeout == 0:
        await streamEvent.wait()
    else:
        try:
            await asyncio.wait_for(streamEvent.wait(), timeout=timeout / 1000)
        except asyncio.TimeoutError:
            pass


def increment(store, key):
    val, exp = store.get(key, ("0", -1))
    print("INCR:", repr((val, exp)))
    if not val.isdigit():
        return "-ERR value is not an integer or out of range"
    newVal = int(val) + 1
    store[key] = (str(newVal), exp)
    return newVal


def main():
    asyncio.run(serveRedis())


if __name__ == "__main__":
    main()
