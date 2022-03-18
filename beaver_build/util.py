import contextlib
import os
import zlib


@contextlib.contextmanager
def working_directory(directory):
    """
    Temporarily change the working directory using a context manager.
    """
    current = os.getcwd()
    os.chdir(directory)
    yield directory
    os.chdir(current)


class Crc32:
    """
    Compute the CRC32 using the :mod:`hashlib` interface.
    """
    def __init__(self, data: bytes = b"", value: int = 0) -> None:
        self.crc32 = zlib.crc32(data, value)

    def __int__(self):
        return self.crc32

    def update(self, data: bytes):
        self.crc32 = zlib.crc32(data, self.crc32)

    def digest(self) -> bytes:
        return self.crc32.to_bytes(4, 'big')

    def hexdigest(self) -> str:
        return self.digest().hex()
