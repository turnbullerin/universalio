import pathlib
import aiofiles
import aiofiles.os
import aiofiles.ospath
from functools import lru_cache
import os
from .base import FileWriter, FileReader, PathResourceDescriptor, SynchronousDescriptor


class LocalFileWriterContextManager:

    class Writer(FileWriter):

        def __init__(self, handle):
            super().__init__()
            self.handle = handle

        async def write_chunk(self, chunk):
            await self.handle.write(chunk)

    def __init__(self, path):
        self.path = path
        self._handle = None

    async def __aenter__(self):
        self._handle = await aiofiles.open(self.path, "wb")
        return LocalFileReaderContextManager.Reader(self._handle)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._handle.close()


class LocalFileReaderContextManager:

    class Reader(FileReader):

        def __init__(self, handle):
            super().__init__()
            self.handle = handle

        async def chunks(self, chunk_size=1048576):
            chunk = await self.handle.read(chunk_size)
            while chunk:
                yield chunk
                chunk = await self.handle.read(chunk_size)

    def __init__(self, path):
        self.path = path
        self._handle = None

    async def __aenter__(self):
        self._handle = await aiofiles.open(self.path, "rb")
        return LocalFileReaderContextManager.Reader(self._handle)

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

    @lru_cache(maxsize=None)
    def exists(self):
        return self.path.exists()

    def parent(self):
        return LocalDescriptor(self._parent_path())

    def child(self, child):
        return LocalDescriptor(self._child_path(child))

    def list(self):
        for f in os.scandir(self.path):
            yield LocalDescriptor(f.path)

    def reader(self):
        return LocalFileReaderContextManager(self.path)

    def writer(self):
        return LocalFileWriterContextManager(self.path)



