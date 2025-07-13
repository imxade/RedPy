"""
parser.py
Credit: 
- https://github.com/nikhil31sai/redis-python/blob/master/utils/parser.py
"""

import datetime
import struct
import os

def parse(data):
    """ Parse RESP data and return the corresponding Python object """
    if not data:
        raise ValueError("Empty data cannot be parsed")

    type_indicator = data[0:1]
    
    if type_indicator == b"+":
        return _parse_simple_string(data)
    elif type_indicator == b"-":
        return _parse_error(data)
    elif type_indicator == b":":
        return _parse_integer(data)
    elif type_indicator == b"$":
        return _parse_bulk_string(data)
    elif type_indicator == b"*":
        return _parse_array(data)
    elif type_indicator == b"_":
        return None  # RESP3 Null Bulk String
    elif type_indicator == b"#":
        return _parse_boolean(data)
    elif type_indicator == b",":
        return _parse_double(data)
    elif type_indicator == b"(":
        return _parse_big_number(data)
    elif type_indicator == b"!":
        return _parse_bulk_error(data)
    elif type_indicator == b"=":
        return _parse_verbatim_string(data)
    elif type_indicator == b"%":
        return _parse_map(data)
    elif type_indicator == b"~":
        return _parse_set(data)
    elif type_indicator == b">":
        return _parse_push(data)
    else:
        raise ValueError("Unsupported RESP data type")

def _parse_simple_string(data):
    """ Parse a RESP simple string """
    if not data.startswith(b"+"):
        raise ValueError("Not a valid RESP simple string")
    return data[1:-2].decode('utf-8')

def _parse_error(data):
    """ Parse a RESP error """
    if not data.startswith(b"-"):
        raise ValueError("Not a valid RESP error")
    return data[1:-2].decode('utf-8')

def _parse_integer(data):
    """ Parse a RESP integer """
    if not data.startswith(b":"):
        raise ValueError("Not a valid RESP integer")
    return int(data[1:-2])

def _parse_bulk_string(data):
    """ Parse a RESP bulk string """
    if data.startswith(b"$-1\r\n"):
        return None
    if not data.startswith(b"$"):
        raise ValueError("Not a valid RESP bulk string")

    parts = data[1:].split(b'\r\n', 1)
    length = int(parts[0])
    
    if length == -1:
        return None
    
    string_data = parts[1][:length]
    return string_data.decode('utf-8')

def _parse_array(data):
    """ Parse a RESP array """
    if not data.startswith(b"*"):
        raise ValueError("Not a valid RESP array")

    parts = data[1:].split(b'\r\n', 1)
    length = int(parts[0])
    
    if length == -1:
        return None

    elements = []
    data_remaining = parts[1]
    while length > 0:
        element, data_remaining = _decode_next_element(data_remaining)
        elements.append(element)
        length -= 1

    return elements

def _parse_boolean(data):
    """ Parse a RESP boolean (RESP3) """
    if not data.startswith(b"#"):
        raise ValueError("Not a valid RESP boolean")
    return data[1:-2] == b"true"

def _parse_double(data):
    """ Parse a RESP double (RESP3) """
    if not data.startswith(b","):
        raise ValueError("Not a valid RESP double")
    return float(data[1:-2])

def _parse_big_number(data):
    """ Parse a RESP big number (RESP3) """
    if not data.startswith(b"("):
        raise ValueError("Not a valid RESP big number")
    return data[1:-2].decode('utf-8')

def _parse_bulk_error(data):
    """ Parse a RESP bulk error (RESP3) """
    if not data.startswith(b"!"):
        raise ValueError("Not a valid RESP bulk error")
    return data[1:-2].decode('utf-8')

def _parse_verbatim_string(data):
    """ Parse a RESP verbatim string (RESP3) """
    if not data.startswith(b"="):
        raise ValueError("Not a valid RESP verbatim string")
    return data[1:-2].decode('utf-8')

def _parse_map(data):
    """ Parse a RESP map (RESP3) """
    if not data.startswith(b"%"):
        raise ValueError("Not a valid RESP map")
    # Parsing maps requires key-value pairs handling
    # Implement accordingly if needed
    raise NotImplementedError("Map parsing is not implemented")

def _parse_set(data):
    """ Parse a RESP set (RESP3) """
    if not data.startswith(b"~"):
        raise ValueError("Not a valid RESP set")
    # Parsing sets requires handling unique elements
    # Implement accordingly if needed
    raise NotImplementedError("Set parsing is not implemented")

def _parse_push(data):
    """ Parse a RESP push (RESP3) """
    if not data.startswith(b">"):
        raise ValueError("Not a valid RESP push")
    # Parsing push data requires special handling
    # Implement accordingly if needed
    raise NotImplementedError("Push parsing is not implemented")

def _decode_next_element(data):
    """ Helper method to decode the next RESP element in an array """
    if data.startswith(b"$"):
        length, rest = data[1:].split(b'\r\n', 1)
        length = int(length)
        if length == -1:
            return None, rest
        string_data = rest[:length]
        return string_data.decode('utf-8'), rest[length+2:]
    elif data.startswith(b":"):
        end = data.find(b'\r\n')
        return int(data[1:end]), data[end+2:]
    elif data.startswith(b"+"):
        end = data.find(b'\r\n')
        return data[1:end].decode('utf-8'), data[end+2:]
    elif data.startswith(b"-"):
        end = data.find(b'\r\n')
        return data[1:end].decode('utf-8'), data[end+2:]
    elif data.startswith(b"*"):
        end = data.find(b'\r\n')
        length = int(data[1:end])
        if length == -1:
            return None, data[end+2:]
        elements = []
        data_remaining = data[end+2:]
        while length > 0:
            element, data_remaining = _decode_next_element(data_remaining)
            elements.append(element)
            length -= 1
        return elements, data_remaining
    else:
        raise ValueError("Unsupported RESP element")


# resp_helper.py

def encode(data):
    """ Encode Python data into RESP format """
    if isinstance(data, str):
        return _encode_simple_string(data)
    elif isinstance(data, int):
        return _encode_integer(data)
    elif isinstance(data, bytes):
        return _encode_bulk_string(data)
    elif isinstance(data, list):
        return _encode_array(data)
    elif data is None:
        return _encode_bulk_string(None)
    elif isinstance(data, bool):
        return _encode_boolean(data)
    elif isinstance(data, float):
        return _encode_double(data)
    elif isinstance(data, dict):
        return _encode_map(data)
    elif isinstance(data, set):
        return _encode_set(data)
    elif isinstance(data, Push):
        return _encode_push(data)
    else:
        raise TypeError("Unsupported data type for RESP encoding")

def _encode_simple_string(s):
    """ Encode a simple string as RESP protocol """
    if not isinstance(s, str):
        raise TypeError("Simple string must be a str")
    if s.startswith(("+", "-")):
        return f"{s}\r\n".encode('utf-8')
    else:
        s = s.encode()
        return b"$" + str(len(s)).encode() + b"\r\n" + s + b"\r\n"

def _encode_integer(i):
    """ Encode an integer as RESP protocol """
    if not isinstance(i, int):
        raise TypeError("Integer must be an int")
    return f":{i}\r\n".encode('utf-8')

def _encode_bulk_string(s):
    """ Encode a bulk string as RESP protocol """
    if s is None:
        return b"$-1\r\n"
    if isinstance(s, str):
        s = s.encode("utf-8")
    elif not isinstance(s, bytes):
        raise TypeError("Bulk string must be bytes or str")
    return b"$" + str(len(s)).encode() + b"\r\n" + s

def _encode_array(elements):
    """ Encode an array as RESP protocol """
    if not isinstance(elements, list):
        raise TypeError("Array must be a list")
    encoded_elements = [encode(element) for element in elements]
    length = len(encoded_elements)
    return f"*{length}\r\n".encode('utf-8') + b"".join(encoded_elements)

def _encode_boolean(b):
    """ Encode a boolean as RESP protocol (RESP3) """
    if not isinstance(b, bool):
        raise TypeError("Boolean must be a bool")
    return f"#{str(b).lower()}\r\n".encode('utf-8')

def _encode_double(d):
    """ Encode a double as RESP protocol (RESP3) """
    if not isinstance(d, float):
        raise TypeError("Double must be a float")
    return f",{d}\r\n".encode('utf-8')

def _encode_big_number(bn):
    """ Encode a big number as RESP protocol (RESP3) """
    if not isinstance(bn, str):
        raise TypeError("Big number must be a str")
    return f"({bn}\r\n".encode('utf-8')

def _encode_bulk_error(error):
    """ Encode a bulk error as RESP protocol (RESP3) """
    if not isinstance(error, str):
        raise TypeError("Bulk error must be a str")
    return f"!{error}\r\n".encode('utf-8')

def _encode_verbatim_string(s):
    """ Encode a verbatim string as RESP protocol (RESP3) """
    if not isinstance(s, str):
        raise TypeError("Verbatim string must be a str")
    return f"={s}\r\n".encode('utf-8')

def _encode_map(m):
    """ Encode a map as RESP protocol (RESP3) """
    if not isinstance(m, dict):
        raise TypeError("Map must be a dict")
    encoded_pairs = []
    for k, v in m.items():
        encoded_pairs.append(encode(k))
        encoded_pairs.append(encode(v))
    length = len(encoded_pairs) // 2
    return f"%{length}\r\n".encode('utf-8') + b"".join(encoded_pairs)

def _encode_set(s):
    """ Encode a set as RESP protocol (RESP3) """
    if not isinstance(s, set):
        raise TypeError("Set must be a set")
    encoded_elements = [encode(element) for element in s]
    length = len(encoded_elements)
    return f"~{length}\r\n".encode('utf-8') + b"".join(encoded_elements)

def _encode_push(push):
    """ Encode a push as RESP protocol (RESP3) """
    if not isinstance(push, Push):
        raise TypeError("Push must be an instance of Push")
    return f">{push.data}\r\n".encode('utf-8')

class Push:
    def __init__(self, data):
        if not isinstance(data, str):
            raise TypeError("Push data must be a str")
        self.data = data



def read_rdb_key(dir, dbfilename):
    rdb_file_loc = dir + "/" + dbfilename
    with open(rdb_file_loc, "rb") as f:
        while operand := f.read(1):
            if operand == b"\xfb":
                break
        numKeys = struct.unpack("B", f.read(1))[0]
        f.read(1)

        ans = []

        for i in range(numKeys):
            
            top = f.read(1)
            if top == b"\xfc":
                f.read(8+1)
            elif top == b"\xfd":
                f.read(4+1)

            length = struct.unpack("B", f.read(1))[0]
            if length >> 6 == 0b00:
                length = length & 0b00111111
            else:
                length = 0
            ans.append(f.read(length).decode())

            length = struct.unpack("B", f.read(1))[0]
            if length >> 6 == 0b00:
                length = length & 0b00111111
            else:
                length = 0
            f.read(length)

        return ans
    
def read_rdb_val(dir, dbfilename, key):
    rdb_file_loc = dir + "/" + dbfilename
    with open(rdb_file_loc, "rb") as f:
        while operand := f.read(1):
            if operand == b"\xfb":
                break
        numKeys = struct.unpack("B", f.read(1))[0]
        f.read(1)
        print("NumKeys: ", numKeys)
        expired = False
        for i in range(numKeys):
            print(i)
            top = f.read(1)
            if top == b"\xfc":
                milliTime = int.from_bytes(f.read(8), byteorder="little")
                if milliTime < datetime.datetime.now().timestamp()*1000 :
                    expired = True
                f.read(1)
            elif top == b"\xfd":
                secTime = int.from_bytes(f.read(4), byteorder="little")
                if secTime < datetime.datetime.now().timestamp():
                    expired = True
                f.read(1)

            length = struct.unpack("B", f.read(1))[0]
            if length >> 6 == 0b00:
                length = length & 0b00111111
            else:
                length = 0
            currKey = f.read(length).decode()

            length = struct.unpack("B", f.read(1))[0]
            if length >> 6 == 0b00:
                length = length & 0b00111111
            else:
                length = 0
            print("Key:", currKey)
            val = f.read(length).decode()
            print("Value:", val)
            if currKey == key:
                if not expired:
                    return val
                else:
                    return None

        return ""
    

def read_key_val_from_db(dir, dbfilename, data):
    rdb_file_loc = os.path.join(dir, dbfilename)
    if not os.path.isfile(rdb_file_loc):
        return
    with open(rdb_file_loc, "rb") as f:
        while operand := f.read(1):
            if operand == b"\xfb":
                break
        numKeys = struct.unpack("B", f.read(1))[0]
        f.read(1)
        print("NumKeys: ", numKeys)
        for i in range(numKeys):
            expired = False
            print(i)
            top = f.read(1)
            if top == b"\xfc":
                milliTime = int.from_bytes(f.read(8), byteorder="little")
                now = datetime.datetime.now().timestamp()*1000 
                if milliTime < now:
                    print("milliTime: ", milliTime)
                    print("now: ", now)
                    expired = True
                f.read(1)
            elif top == b"\xfd":
                secTime = int.from_bytes(f.read(4), byteorder="little")
                if secTime < datetime.datetime.now().timestamp():
                    expired = True
                f.read(1)

            length = struct.unpack("B", f.read(1))[0]
            if length >> 6 == 0b00:
                length = length & 0b00111111
            else:
                length = 0
            currKey = f.read(length).decode()

            length = struct.unpack("B", f.read(1))[0]
            if length >> 6 == 0b00:
                length = length & 0b00111111
            else:
                length = 0
            print("Key:", currKey)
            val = f.read(length).decode()
            print("Value:", val)
            if not expired:
                data[currKey] = (val, -1)

        return