import asyncssh
import asyncio
from functools import lru_cache
from .base import FileWriter, FileReader, UriResourceDescriptor, AsynchronousDescriptor


class SFTPDescriptor(UriResourceDescriptor, AsynchronousDescriptor):

    def __init__(self, uri, username=None, password=None):
        super().__init__(uri)
        self.username = username
        self.password = password

    def _connect(self):
        return asyncssh.connect(
            self.hostname,
            username=self.username,
            password=self.password,
            port=self.port,
            # TODO Fix this to allow the user to input known hosts
            known_hosts=None
        )

    @lru_cache(maxsize=None)
    async def is_dir_async(self):
        async with self._connect() as conn:
            async with conn.start_sftp_client() as sftp:
                return await sftp.isdir("/" + self.path)

    @lru_cache(maxsize=None)
    async def is_file_async(self):
        async with self._connect() as conn:
            async with conn.start_sftp_client() as sftp:
                return await sftp.isfile("/" + self.path)

    async def list_async(self):
        pass

    @lru_cache(maxsize=None)
    async def exists_async(self):
        async with self._connect() as conn:
            async with conn.start_sftp_client() as sftp:
                return await sftp.exists(self.path)

    def parent(self):
        pass

    def child(self, child):
        pass

    def reader(self):
        pass

    def writer(self):
        pass



