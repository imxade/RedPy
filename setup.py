from setuptools import setup, find_packages

setup(
    name='redpy',
    version='0.1.0',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'redpy=redpy:main',
        ],
    },
    author='Rituraj Basak',
    description='A Redis clone in Python',
    url="https://codeberg.org/zz/redpy", 
    license='MIT',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
    ],
    install_requires=[
    ],
)
