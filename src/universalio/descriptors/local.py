import pathlib
import aiofiles
import aiofiles.os
import aiofiles.ospath
from functools import lru_cache
import os
from .base import FileWriter, FileReader, PathResourceDescriptor, SynchronousDescriptor


class _LocalFileWriterContextManager:

    def __init__(self, path):
        self.path = path
        self._handle = None

    async def __aenter__(self):
        self._handle = await aiofiles.open(self.path, "wb")
        return FileWriter(self._handle)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._handle.close()


class _LocalFileReaderContextManager:

    def __init__(self, path, chunk_size=None):
        self.path = path
        self.chunk_size = chunk_size
        self._handle = None

    async def __aenter__(self):
        self._handle = await aiofiles.open(self.path, "rb")
        return FileReader(self._handle, self.chunk_size)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._handle.close()


class LocalDescriptor(PathResourceDescriptor, SynchronousDescriptor):

    def __init__(self, path):
        PathResourceDescriptor.__init__(self, pathlib.Path(path))

    @lru_cache(maxsize=None)
    def is_dir(self):
        return self.path.is_dir()

    @lru_cache(maxsize=None)
    def is_file(self):
        return self.path.is_file()

    def exists(self):
        return self.path.exists()

    def remove(self):
        return self.path.unlink()

    def list(self):
        for f in os.scandir(self.path):
            yield LocalDescriptor(f.path)

    def reader(self, chunk_size=None):
        return _LocalFileReaderContextManager(self.path, chunk_size)

    def writer(self):
        return _LocalFileWriterContextManager(self.path)

    @staticmethod
    def match_location(location):
        # Absolute path on mapped drive, e.g. C:\ or linux-flavoured C:/
        if location[1:3] == ":\\" or location[1:3] == ":/":
            return True
        # Absolute path on network, e.g. \\server\fileshare
        if location[0:2] == r"\\":
            return True
        # Absolute paths on posix machines
        if location[0] == "/":
            return True
        # Home paths
        if location[0] == "~":
            return True
        if "://" in location:
            return False
        return False

    @staticmethod
    def create_from_location(location: str):
        return LocalDescriptor(pathlib.Path(location).absolute())
