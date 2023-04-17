import asyncssh
import hashlib
from .base import FileWriter, FileReader, UriResourceDescriptor, AsynchronousDescriptor, ConnectionRegistry
from universalio import GlobalLoopContext
from autoinject import injector
import zirconium as zr
from urllib.parse import urlparse


class _SFTPWriterContextManager:

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
        return FileWriter(self._handle)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._cm.__aexit__(exc_type, exc_val, exc_tb)
        self._client.exit()


class _SFTPReaderContextManager:

    def __init__(self, connection, path, chunk_size=None):
        self.conn = connection
        self.path = path
        self.chunk_size = None
        self._connection = None
        self._client = None
        self._handle = None

    async def __aenter__(self):
        self._connection = await self.conn
        self._client = await self._connection.start_sftp_client()
        self._cm = self._client.open(str(self.path), "rb")
        self._handle = await self._cm.__aenter__()
        return FileReader(self._handle, self.chunk_size)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._cm.__aexit__(exc_type, exc_val, exc_tb)
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

    async def is_dir_async(self):
        conn = await self._connect()
        async with conn.start_sftp_client() as sftp:
            return await sftp.isdir(str(self.path))

    async def is_file_async(self):
        conn = await self._connect()
        async with conn.start_sftp_client() as sftp:
            return await sftp.isfile(str(self.path))

    async def list_async(self):
        conn = await self._connect()
        async with conn.start_sftp_client() as sftp:
            async for sftp_name in sftp.scandir(str(self.path)):
                yield self.child(sftp_name.filename)

    async def exists_async(self):
        conn = await self._connect()
        async with conn.start_sftp_client() as sftp:
            return await sftp.exists(str(self.path))

    async def remove_async(self):
        conn = await self._connect()
        async with conn.start_sftp_client() as sftp:
            await sftp.remove(str(self.path))

    def _create_descriptor(self, *args, **kwargs):
        return SFTPDescriptor(*args, username=self.username, password=self.password, **kwargs)

    def reader(self, chunk_size=None):
        return _SFTPReaderContextManager(self._connect(), self.path, chunk_size)

    def writer(self):
        self.clear_cache()
        return _SFTPWriterContextManager(self._connect(), self.path)

    async def is_local_to_async(self, target_resource):
        if not isinstance(target_resource, SFTPDescriptor):
            return False
        return target_resource.hostname == self.hostname and target_resource.port == self.port

    async def _local_copy_async(self, target_resource, chunk_size=None, **kwargs):
        conn = await self._connect()
        async with conn.start_sftp_client() as sftp:
            await sftp.copy(str(self.path), str(target_resource.path))

    async def _local_move_file_async(self, target_resource, chunk_size=None, **kwargs):
        conn = await self._connect()
        async with conn.start_sftp_client() as sftp:
            await sftp.rename(str(self.path), str(target_resource.path))

    async def _do_rmdir_async(self):
        conn = await self._connect()
        async with conn.start_sftp_client() as sftp:
            await sftp.rmdir(str(self.path))

    async def _do_mkdir_async(self):
        conn = await self._connect()
        async with conn.start_sftp_client() as sftp:
            await sftp.mkdir(str(self.path))

    async def _stat(self):
        conn = await self._connect()
        async with conn.start_sftp_client() as sftp:
            st = await sftp.stat(str(self.path))
            return st, sftp.version()

    async def mtime_async(self):
        stat, ver = await self._cached_async("stat", self._stat)
        return stat.mtime

    async def atime_async(self):
        stat, ver = await self._cached_async("stat", self._stat)
        return stat.atime

    async def crtime_async(self):
        stat, ver = await self._cached_async("stat", self._stat)
        if ver >= 4:
            return stat.crtime
        return None

    async def size_async(self):
        stat = await self._cached_async("stat", self._stat)
        return stat.size

    async def _supports_fast_rename_async(self):
        return True

    @staticmethod
    def match_location(location):
        return location.lower().startswith("sftp://")

    @staticmethod
    @injector.inject
    def create_from_location(location: str, config: zr.ApplicationConfig):
        p = urlparse(location)
        un = config.get(("universalio", "sftp", p.hostname, "username"), None)
        pw = config.get(("universalio", "sftp", p.hostname, "password"), None)
        return SFTPDescriptor(location, un, pw)
