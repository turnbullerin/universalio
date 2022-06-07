import pathlib
import aiohttp
import requests
from .base import FileWriter, FileReader, UriResourceDescriptor


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


class HttpDescriptor(UriResourceDescriptor):

    def __init__(self, uri):
        super().__init__(uri)

    def is_dir(self):
        return self.uri.endswith("/")

    def is_file(self):
        return not self.uri.endswith("/")

    def exists(self):
        resp = requests.head(self.uri)
        return resp.status_code == 200

    async def exists_async(self):
        pass

    def parent(self):
        pass

    def child(self, child):
        pass

    def list(self):
        pass

    def reader(self):
        pass

    def writer(self):
        pass

