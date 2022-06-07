import asyncssh
import hashlib
from functools import lru_cache
from .base import FileWriter, FileReader, UriResourceDescriptor, AsynchronousDescriptor, ConnectionRegistry
from universalio import GlobalLoopContext
from autoinject import injector


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
        self._handle = self._client.open(str(self.path), "wb")
        return SFTPWriterContextManager.Writer(self._handle)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._handle.close()
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
        self._handle = self._client.open(str(self.path), "rb")
        return SFTPReaderContextManager.Reader(self._handle)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._handle.close()
        self._client.exit()


@injector.injectable
class _SFTPHostManager(ConnectionRegistry):

    def _construct_key(self, host, port, username, *args, **kwargs):
        return hashlib.sha512("{}|{}|{}|".format(host, port, username).encode("utf-8")).hexdigest()

    async def _create_connection(self, host, port, username, password, known_hosts, *args, **kwargs):
        conn = await asyncssh.create_connection(
            asyncssh.SSHClient,
            host,
            port=port,
            username=username,
            password=password,
            # TODO Fix this to allow the user to input known hosts
            known_hosts=known_hosts
        )
        return conn[0]

    async def _close_connection(self, conn):
        conn.close()


class SFTPDescriptor(UriResourceDescriptor, AsynchronousDescriptor):

    host_manager: _SFTPHostManager = None
    loop: GlobalLoopContext = None

    @injector.construct
    def __init__(self, uri, username=None, password=None):
        UriResourceDescriptor.__init__(self, uri)
        self.username = username
        self.password = password

    async def _connect(self):
        return await self.host_manager.connect(self.hostname, self.port, self.username, self.password, None)

    @lru_cache(maxsize=None)
    async def is_dir_async(self):
        conn = await self._connect()
        async with conn.start_sftp_client() as sftp:
            return await sftp.isdir(str(self.path))

    @lru_cache(maxsize=None)
    async def is_file_async(self):
        conn = await self._connect()
        async with conn.start_sftp_client() as sftp:
            return await sftp.isfile(str(self.path))

    async def list_async(self):
        conn = await self._connect()
        async with conn.start_sftp_client() as sftp:
            async for sftp_name in sftp.scandir(str(self.path)):
                yield self.child(sftp_name.filename)

    @lru_cache(maxsize=None)
    async def exists_async(self):
        conn = await self._connect()
        async with conn.start_sftp_client() as sftp:
            return await sftp.exists(self.path)

    def _create_descriptor(self, *args, **kwargs):
        return SFTPDescriptor(*args, username=self.username, password=self.password, **kwargs)

    def reader(self):
        return SFTPReaderContextManager(self._connect(), self.path)

    def writer(self):
        return SFTPWriterContextManager(self._connect(), self.path)

    @staticmethod
    def match_location(location):
        return location.lower().startswith("sftp://")

    @staticmethod
    def create_from_location(location: str):
        return SFTPDescriptor(location)
