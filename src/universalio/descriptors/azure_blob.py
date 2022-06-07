import hashlib
from functools import lru_cache
from .base import FileWriter, FileReader, UriResourceDescriptor, AsynchronousDescriptor, ConnectionRegistry
from universalio import GlobalLoopContext
from autoinject import injector
import azure.storage.blob.aio as asb
from urllib.parse import urlparse


class SFTPWriterContextManager:

    class Writer(FileWriter):

        def __init__(self, handle):
            super().__init__()
            self.handle = handle

        async def write_chunk(self, chunk):
            await self.handle.write(chunk)

    def __init__(self, connection, path):
        self.conn = connection
        self.path = path
        self._connection = None
        self._client = None
        self._handle = None

    async def __aenter__(self):
        self._connection = await self.conn
        self._client = await self._connection.start_sftp_client()
        self._cm = self._client.open(str(self.path), "wb")
        self._handle = await self._cm.__aenter__()
        return SFTPWriterContextManager.Writer(self._handle)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._cm.__aexit__(exc_type, exc_val, exc_tb)
        self._client.exit()


class SFTPReaderContextManager:

    class Reader(FileReader):

        def __init__(self, handle):
            super().__init__()
            self.handle = handle

        async def chunks(self, chunk_size=1048576):
            chunk = await self.handle.read(chunk_size)
            while chunk:
                yield chunk
                chunk = await self.handle.read(chunk_size)

    def __init__(self, connection, path):
        self.conn = connection
        self.path = path
        self._connection = None
        self._client = None
        self._handle = None

    async def __aenter__(self):
        self._connection = await self.conn
        self._client = await self._connection.start_sftp_client()
        self._cm = self._client.open(str(self.path), "rb")
        self._handle = await self._cm.__aenter__()
        return SFTPReaderContextManager.Reader(self._handle)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._cm.__aexit__(exc_type, exc_val, exc_tb)
        self._client.exit()


@injector.injectable
class _AzureBlobHostRegistry(ConnectionRegistry):

    def _construct_key(self, connect_str, *args, **kwargs):
        return hashlib.sha512(connect_str.encode("utf-8")).hexdigest()

    async def _create_connection(self, connect_str, *args, **kwargs):
        return asb.BlobServiceClient.from_connection_string(connect_str)

    async def _close_connection(self, conn):
        await conn.close()


class AzureBlobDescriptor(UriResourceDescriptor, AsynchronousDescriptor):

    host_manager: _AzureBlobHostRegistry = None
    loop: GlobalLoopContext = None

    @injector.construct
    def __init__(self, uri, connect_str):
        UriResourceDescriptor.__init__(self, uri, True)
        self.connect_str = connect_str

    async def _connect(self):
        return await self.host_manager.connect(self.connect_str)

    async def _get_container_client(self):
        conn = await self._connect()
        return conn.get_container_client(self.container)

    async def _get_blob_client(self):
        conn = await self._connect()
        path = str(self.path)[1:]
        if path == "":
            return conn.get_container_client(self.container)
        return conn.get_blob_client(self.container, path)

    async def is_dir_async(self):
        blob = await self._get_blob_client()
        if isinstance(blob, asb.ContainerClient):
            return True
        if await blob.exists():
            return False
        return await self.exists_async()

    async def is_file_async(self):
        blob = await self._get_blob_client()
        if isinstance(blob, asb.ContainerClient):
            return False
        return await blob.exists()

    async def list_async(self):
        if not await self.is_file_async():
            container = await self._get_container_client()
            found = []
            skip = 0
            kwargs = {}
            if not str(self.path) == "/":
                skip = len(str(self.path))
                kwargs["name_starts_with"] = str(self.path)[1:] + "/"
            else:
                skip = 0
            async for item in container.list_blobs(**kwargs):
                n = item.name[skip:]
                if "/" in n:
                    n = n[:n.find("/")]
                    if n in found:
                        continue
                    found.append(n)
                yield self.child(n)

    async def exists_async(self):
        container = await self._get_container_client()
        async for item in container.list_blobs(name_starts_with=str(self.path)[1:]):
            return True
        return False

    def _create_descriptor(self, *args, **kwargs):
        return AzureBlobDescriptor(*args, connect_str=self.connect_str, **kwargs)

    def reader(self):
        return None

    def writer(self):
        return None

    @staticmethod
    def match_location(location):
        if not location.lower().startswith("https://") or location.lower().startswith("http://"):
            return False
        p = urlparse(location)
        return str(p.path) != "/" and str(p.hostname).endswith(".blob.core.windows.net")

    @staticmethod
    def create_from_location(location: str):
        return AzureBlobDescriptor(location)
