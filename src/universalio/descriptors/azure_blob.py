import hashlib
from functools import lru_cache
from .base import FileWriter, FileReader, UriResourceDescriptor, AsynchronousDescriptor, ConnectionRegistry
from universalio import GlobalLoopContext
from autoinject import injector
import azure.storage.blob.aio as asb
from urllib.parse import urlparse
import asyncio

AZURE_BLOB_UPLOAD_BUFFER = 5 * 1024 * 1024


class _AzureBlobWriterContextManager:

    class BlobWriter(FileWriter):

        def __init__(self, handle, buffer_chunk_size=None):
            super().__init__(handle)
            self.id = 1
            self.block_ids = []
            self.tasks = []
            self.buffer_chunk_size = buffer_chunk_size or AZURE_BLOB_UPLOAD_BUFFER
            self._buffer = None

        async def write(self, chunk):
            self._buffer = chunk if self._buffer is None else self._buffer + chunk
            if self.buffer_chunk_size:
                while len(self._buffer) >= self.buffer_chunk_size:
                    await self._write(self._buffer[0:self.buffer_chunk_size])
                    self._buffer = self._buffer[self.buffer_chunk_size:]
            else:
                await self._write(self._buffer)
                self._buffer = None

        async def _write(self, chunk):
            blob_id = None
            async with asyncio.Lock():
                blob_id = "{:064X}".format(self.id)
                self.id += 1
            self.block_ids.append(blob_id)
            self.tasks.append(asyncio.create_task(self.handle.stage_block(
                blob_id,
                chunk,
                len(chunk)
            )))

        async def finalize(self):
            if self._buffer:
                await self._write(self._buffer)
                self._buffer = None
            await asyncio.gather(*self.tasks)
            await self.handle.commit_block_list(self.block_ids)

    def __init__(self, blob_client):
        if blob_client is None:
            raise ValueError("Cannot write to a directory")
        self.client = blob_client
        self._handle = None

    async def __aenter__(self):
        self._real_client = await self.client
        self._handle = _AzureBlobWriterContextManager.BlobWriter(self._real_client)
        return self._handle

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            await self._handle.finalize()
        self._real_client = None
        self._handle = None


class _AzureBlobReaderContextManager:

    class BlobReader(FileReader):

        async def read(self, chunk_size=None):
            async for chunk in self.handle.chunks():
                yield chunk

    def __init__(self, blob_client, chunk_size=None):
        if blob_client is None:
            raise ValueError("Cannot read from a directory")
        self.client = blob_client
        self.chunk_size = chunk_size
        self._real_client = None
        self._stream = None

    async def __aenter__(self):
        kwargs = {}
        self._real_client = await self.client
        if self.chunk_size:
            kwargs["config"] = {
                "max_chunk_get_size": int(self.chunk_size)
            }
        self._stream = await self._real_client.download_blob(**kwargs)
        return _AzureBlobReaderContextManager.BlobReader(self._stream)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._real_client = None
        self._stream = None


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
            return None
        return conn.get_blob_client(self.container, path)

    async def remove_async(self):
        blob = await self._get_blob_client()
        if blob is None:
            raise ValueError("Cannot remove container")
        return await blob.delete_blob("include")

    async def is_dir_async(self):
        blob = await self._get_blob_client()
        if blob is None:
            return True
        if await blob.exists():
            return False
        return await self.exists_async()

    async def is_file_async(self):
        blob = await self._get_blob_client()
        if blob is None:
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

    async def _do_rename_async(self, target):
        pass

    async def exists_async(self):
        container = await self._get_container_client()
        async for item in container.list_blobs(name_starts_with=str(self.path)[1:]):
            return True
        return False

    def _create_descriptor(self, *args, **kwargs):
        return AzureBlobDescriptor(*args, connect_str=self.connect_str, **kwargs)

    def reader(self, chunk_size=None):
        return _AzureBlobReaderContextManager(self._get_blob_client(), chunk_size)

    def writer(self):
        return _AzureBlobWriterContextManager(self._get_blob_client())

    async def _do_rmdir_async(self):
        pass

    async def _do_mkdir_async(self):
        pass

    async def is_local_to(self, target_resource):
        if not isinstance(target_resource, AzureBlobDescriptor):
            return False

    async def _properties(self):
        return await self._cached_async("properties", self._get_properties)

    async def _get_properties(self):
        blob = await self._get_blob_client()
        if blob is None:
            return None
        return await blob.get_blob_properties()

    async def mtime_async(self):
        props = await self._properties()
        if props is None:
            return None
        return props.last_modified

    async def atime_async(self):
        props = await self._properties()
        if props is None:
            return None
        return props.last_accessed_on

    async def crtime_async(self):
        props = await self._properties()
        if props is None:
            return None
        return props.creation_time

    async def size_async(self):
        props = await self._properties()
        if props is None:
            return None
        return props.size

    @staticmethod
    def match_location(location):
        if not location.lower().startswith("https://") or location.lower().startswith("http://"):
            return False
        p = urlparse(location)
        return str(p.path) != "/" and str(p.hostname).endswith(".blob.core.windows.net")

    @staticmethod
    def create_from_location(location: str):
        return AzureBlobDescriptor(location)
