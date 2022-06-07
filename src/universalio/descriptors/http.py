import aiohttp
import atexit
from autoinject import injector
from .base import FileWriter, FileReader, UriResourceDescriptor, AsynchronousDescriptor, ConnectionRegistry


class HttpWriterContextManager:

    class Writer(FileWriter):

        def __init__(self, session, path):
            super().__init__(session)
            self.path = path
            self._buffer = None

        def write(self, chunk):
            if self._buffer is None:
                self._buffer = chunk
            else:
                self._buffer += chunk

        def finalize(self):
            async with self.handle.put(self.path, self._buffer) as resp:
                pass
            self._buffer = None

    def __init__(self, uri, session):
        self.uri = uri
        self.session = session
        self._writer = None

    async def __aenter__(self):
        self._writer = HttpWriterContextManager.Writer(self.session, self.uri)
        return self._writer

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self._writer.finalize()


class HttpReaderContextManager:

    class Reader(FileReader):

        def read(self, chunk_size=None):
            chunk_size = chunk_size or self.chunk_size
            async for chunk in self.handle.iter_chunked(chunk_size):
                yield chunk

    def __init__(self, uri, session):
        self.uri = uri
        self.session = session
        self._handle = None
        self._get = None

    async def __aenter__(self):
        self._get = await self.session.get(self.uri)
        self._handle = await self._get.__aenter__()
        return HttpReaderContextManager.Reader(self._handle)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._get.__aexit__(exc_type, exc_val, exc_tb)


@injector.injectable
class HttpSessionRegistry:

    def __init__(self):
        self.session = aiohttp.ClientSession()
        atexit.register(HttpSessionRegistry.exit, self)

    def exit(self):
        self.session.close()

    def __getattr__(self, item):
        return getattr(self.session, item)


class HttpDescriptor(UriResourceDescriptor, AsynchronousDescriptor):

    session: HttpSessionRegistry = None

    @injector.construct
    def __init__(self, uri):
        super().__init__(uri)

    async def canonical(self):
        async with self.session.head(self.uri) as response:
            if response.status in [301]:
                return self._create_descriptor(response.headers.get("Location")).canonical()
        return self

    async def detect_encoding_async(self):
        async with self.session.head(self.uri) as response:
            response.raise_for_status()
            if "Content-Type" in response.headers:
                h = response.headers.get("Content-Type")
                if "charset=" in h:
                    p = h.find("charset=") + 8
                    if ";" in h[p:]:
                        return h[p:h.find(";", p)]
                    else:
                        return h[p:]
        return await super().detect_encoding_async()

    async def is_dir_async(self):
        # By convention
        return self.uri.endswith("/")

    async def is_file_async(self):
        # By convention
        return not self.uri.endswith("/")

    async def exists_async(self):
        async with self.session.head(self.uri) as response:
            response.raise_for_status()
            return True

    async def list_async(self):
        return []

    async def remove_async(self):
        async with self.session.delete(self.uri) as response:
            response.raise_for_status()

    def reader(self):
        return HttpReaderContextManager(self.uri, self.session)

    def writer(self):
        return HttpWriterContextManager(self.uri, self.session)
