import asyncssh
import asyncio
import atexit
import hashlib
from autoinject import injector
from functools import lru_cache
from .base import FileWriter, FileReader, UriResourceDescriptor, AsynchronousDescriptor, ConnectionRegistry
from universalio import GlobalLoopContext
from autoinject import injector


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
        pass

    def writer(self):
        pass



