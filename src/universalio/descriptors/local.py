import pathlib
import aiofiles
import aiofiles.os
import aiofiles.ospath
from functools import lru_cache
import os
import shutil
from .base import FileWriter, FileReader, PathResourceDescriptor, SynchronousDescriptor
import sys


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

    def is_dir(self):
        return self._cached("is_dir", self.path.is_dir)

    def is_file(self):
        return self._cached("is_file", self.path.is_file)

    def exists(self):
        return self._cached("exists", self.path.exists)

    def remove(self):
        self.clear_cache()
        return self.path.unlink()

    def mtime(self):
        return self._cached("stat", self.path.stat).st_mtime

    def atime(self):
        return self._cached("stat", self.path.stat).st_atime

    def crtime(self):
        return self._cached("stat", self.path.stat).st_ctime

    def size(self):
        return self._cached("stat", self.path.stat).st_size

    def list(self):
        for f in os.scandir(self.path):
            yield LocalDescriptor(f.path)

    def _do_rename(self, target):
        self.path.rename(target.path)

    def reader(self, chunk_size=None):
        return _LocalFileReaderContextManager(self.path, chunk_size)

    def writer(self):
        self.clear_cache()
        return _LocalFileWriterContextManager(self.path)

    async def is_local_to(self, target_resource):
        return isinstance(target_resource, LocalDescriptor)

    async def _do_rmdir_async(self):
        await self.loop.execute(self.path.rmdir)

    async def _do_mkdir_async(self):
        await self.loop.execute(self.path.mkdir)

    async def _local_move_file_async(self, target_resource, **kwargs):
        await self.loop.execute(shutil.move, self.path, target_resource.path)

    async def _local_move_dir_async(self, target_resource, **kwargs):
        await self.loop.execute(shutil.move, self.path, target_resource.path)

    async def _local_copy_async(self, target_resource, chunk_size=None, **kwargs):
        await self.loop.execute(shutil.copy2, self.path, target_resource.path)

    async def _local_copy_dir_async(self, target_resource, recursive=True, **kwargs):
        v = sys.version_info
        if recursive and v.major == 3 and v.minor >= 8:
            await self.loop.execute(shutil.copytree, self.path, target_resource.path, dirs_exist_ok=True)
        elif recursive and not await target_resource.exists_async():
            await self.loop.execute(shutil.copytree, self.path, target_resource.path)
        else:
            await super()._local_copy_dir_async(target_resource, recursive, **kwargs)

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
