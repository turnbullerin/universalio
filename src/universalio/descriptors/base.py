import abc
import pathlib
from urllib.parse import urlsplit, quote_plus
import asyncio
from autoinject import injector
import atexit
from universalio import GlobalLoopContext
import hashlib
import datetime

DEFAULT_CHUNK_SIZE = 5 * 1024 * 1024


class UNIOError(OSError):
    pass


class ConnectionRegistry(abc.ABC):

    loop: GlobalLoopContext = None

    @injector.construct
    def __init__(self):
        self.host_cache = {}
        atexit.register(ConnectionRegistry.exit, self)

    async def connect(self, *args, **kwargs):
        key = self._construct_key(*args, **kwargs)
        if key not in self.host_cache:
            self.host_cache[key] = await self._create_connection(*args, **kwargs)
        return self.host_cache[key]

    def exit(self):
        self.loop.run(self.exit_async())

    async def exit_async(self):
        for key in self.host_cache:
            await self._close_connection(self.host_cache[key])
        self.host_cache = {}

    @abc.abstractmethod
    def _construct_key(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    async def _create_connection(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    async def _close_connection(self, conn):
        pass


class FileReader:

    def __init__(self, handle=None, chunk_size=None):
        super().__init__()
        self.handle = handle
        self.chunk_size = chunk_size if chunk_size else DEFAULT_CHUNK_SIZE

    async def read(self, chunk_size=None):
        chunk_size = chunk_size or self.chunk_size
        chunk = await self.handle.read(chunk_size)
        while chunk:
            yield chunk
            chunk = await self.handle.read(chunk_size)


class FileWriter:

    def __init__(self, handle=None):
        super().__init__()
        self.handle = handle

    async def write(self, chunk):
        await self.handle.write(chunk)


class ResourceDescriptor(abc.ABC):

    loop: GlobalLoopContext = None

    @injector.construct
    def __init__(self):
        self._cache = {}

    def clear_cache(self, cache_key=None):
        if cache_key is None:
            self._cache = {}
        elif cache_key in self._cache:
            del self._cache[cache_key]

    def _set_cache(self, cache_key, value):
        self._cache[cache_key] = value

    def _cached(self, cache_key, cb, *args, **kwargs):
        if cache_key not in self._cache:
            self._cache[cache_key] = cb(*args, **kwargs)
        return self._cache[cache_key]

    async def _cached_async(self, cache_key, coro_func, *args, **kwargs):
        if cache_key not in self._cache:
            self._cache[cache_key] = await coro_func(*args, **kwargs)
        return self._cache[cache_key]

    @abc.abstractmethod
    def is_dir(self):
        pass

    @abc.abstractmethod
    def exists(self):
        pass

    @abc.abstractmethod
    def remove(self):
        pass

    @abc.abstractmethod
    def basename(self):
        pass

    @abc.abstractmethod
    def parent(self):
        pass

    @abc.abstractmethod
    def child(self, child):
        pass

    @abc.abstractmethod
    def reader(self) -> FileReader:
        pass

    @abc.abstractmethod
    def writer(self) -> FileWriter:
        pass

    @abc.abstractmethod
    def list(self):
        pass

    @abc.abstractmethod
    def is_file(self):
        pass

    @abc.abstractmethod
    def mtime(self):
        pass

    @abc.abstractmethod
    def atime(self):
        pass

    @abc.abstractmethod
    def crtime(self):
        pass

    @abc.abstractmethod
    def size(self):
        pass

    @abc.abstractmethod
    async def mtime_async(self) -> datetime.datetime:
        pass

    @abc.abstractmethod
    async def atime_async(self):
        pass

    @abc.abstractmethod
    async def crtime_async(self):
        pass

    @abc.abstractmethod
    async def size_async(self):
        pass

    @abc.abstractmethod
    async def is_file_async(self):
        pass

    @abc.abstractmethod
    async def is_dir_async(self):
        pass

    @abc.abstractmethod
    async def remove_async(self):
        pass

    @abc.abstractmethod
    async def exists_async(self):
        pass

    @abc.abstractmethod
    async def list_async(self):
        pass

    @abc.abstractmethod
    def joinpath(self, *paths):
        pass

    def __truediv__(self, path):
        return self.joinpath(path)

    @abc.abstractmethod
    async def _do_rmdir_async(self):
        pass

    @abc.abstractmethod
    async def _do_mkdir_async(self):
        pass

    def is_empty(self):
        return self.loop.run(self.is_empty_async())

    async def is_empty_async(self):
        async for x in self.list_async():
            return False
        return True

    def fingerprint(self):
        return self.loop.run(self.fingerprint_async())

    async def fingerprint_async(self):
        mt = await self.mtime_async()
        size = await self.size_async()
        if mt and size:
            return "{}_{}".format(mt.strftime("%Y%m%d%H%M%S"), size)
        return await self.file_hash_async()

    def file_hash(self):
        return self.loop.run(self.file_hash_async())

    async def file_hash_async(self):
        return self._cached_async("file_hash", self._calculate_file_hash_async)

    async def _calculate_file_hash_async(self):
        h = hashlib.sha256()
        async with self.reader() as reader:
            for chunk in reader.read():
                h.update(chunk)
        return h.hexdigest()

    def rmdir(self, recursive=False):
        return self.loop.run(self.rmdir_async(recursive))

    def mkdir(self, recursive=False):
        return self.loop.run(self.mkdir_async(recursive))

    async def rmdir_async(self, recursive=False):
        if not await self.exists_async():
            return
        if recursive:
            await self._do_recursive_rmdir()
        elif not await self.is_empty_async():
            raise UNIOError("Directory {} is not empty".format(self))
        await self._do_rmdir_async()

    async def _do_recursive_rmdir(self):
        tasks = []
        async for x in self.list_async():
            if await x.is_dir_async():
                tasks.append(asyncio.create_task(x.rmdir_async(True)))
            else:
                tasks.append(asyncio.create_task(x.remove()))
        await asyncio.gather(*tasks)

    async def mkdir_async(self, recursive=False):
        if await self.exists_async():
            return
        parent = self.parent()
        if recursive:
            await parent.mkdir_async(True)
        elif not await parent.exists_async():
            raise UNIOError("Parent directory {} doesn't exist".format(parent))
        await self._do_mkdir_async()

    def detect_encoding(self):
        return self.loop.run(self.detect_encoding_async())

    async def detect_encoding_async(self):
        return "utf-8"

    def text(self, encoding=None):
        encoding = encoding or self.detect_encoding()
        return self.read().decode(encoding)

    def read(self):
        return self.loop.run(self.read_async())

    def write(self, data):
        return self.loop.run(self.write_async(data))

    async def write_async(self, data):
        async with self.writer() as h:
            await h.write(data)

    async def read_async(self):
        byts = None
        async with self.reader() as h:
            async for x in h.read():
                if byts is None:
                    byts = x
                else:
                    byts += x
        return byts

    def _create_descriptor(self, *args, **kwargs):
        return self.__class__(*args, **kwargs)

    def move(self, target_resource, **kwargs):
        return self.loop.run(self.move_async(target_resource, **kwargs))

    async def move_async(self, target_resource, **kwargs):
        if not isinstance(target_resource, ResourceDescriptor):
            target_resource = self.joinpath(target_resource)
        if not await self.exists_async():
            raise UNIOError("Resource {} does not exist".format(self))
        if await self.is_dir_async():
            await self._move_dir_async(target_resource, **kwargs)
        else:
            await self._move_file_async(target_resource, **kwargs)
        return target_resource

    async def _move_dir_async(self, target_resource, **kwargs):
        if self.is_local_to(target_resource):
            return await self._local_move_dir_async(target_resource, **kwargs)
        else:
            return await self._do_move_dir_async(target_resource, **kwargs)

    async def _do_move_dir_async(self, target_resource, **kwargs):
        await self._copy_dir_async(target_resource, recursive=True, **kwargs)
        await self.rmdir_async(recursive=True)

    async def _local_move_dir_async(self, target_resource, **kwargs):
        return await self._do_move_dir_async(target_resource, **kwargs)

    async def _move_file_async(self, target_resource, **kwargs):
        if (not kwargs.get("allow_overwrite", False)) and await target_resource.exists_async():
            raise UNIOError("File {} already exists".format(target_resource))
        if self.is_local_to(target_resource):
            return await self._local_move_file_async(target_resource, **kwargs)
        else:
            return await self._do_move_file_async(target_resource, **kwargs)

    async def _do_move_file_async(self, target_resource, **kwargs):
        await self._copy_file_async(target_resource, as_subfolder=False, **kwargs)
        await self.remove()

    async def _local_move_file_async(self, target_resource, **kwargs):
        return await self._do_move_file_async(target_resource, **kwargs)

    def copy(self, target_resource, **kwargs):
        return self.loop.run(self.copy_async(target_resource, **kwargs))

    async def copy_async(self, target_resource, **kwargs):
        if not isinstance(target_resource, ResourceDescriptor):
            target_resource = self.joinpath(target_resource)
        # Can't copy what doesn't exist
        if not await self.exists_async():
            raise UNIOError("Resource {} does not exist".format(self))
        if await self.is_dir_async():
            await self._copy_dir_async(target_resource, **kwargs)
        else:
            await self._copy_file_async(target_resource, **kwargs)
        return target_resource

    async def _copy_file_async(self, target_resource, allow_overwrite=False, _skip_dir_check=False, **kwargs):
        # Check if we are copying a file to a directory and, if so, use the basename
        if (not _skip_dir_check) and await target_resource.is_dir_async():
            target_resource = target_resource.child(self.basename())

        # Can't copy if not allowed to overwrite and the other exists
        if (not allow_overwrite) and await target_resource.exists_async():
            raise UNIOError("Resource {} already exists".format(target_resource))

        # Delegate copy to another method so we can override it as needed
        if await self.is_local_to(target_resource):
            await self._local_copy_async(target_resource, **kwargs)
        else:
            await self._do_copy_async(target_resource, **kwargs)

    async def _do_copy_async(self, target_resource, chunk_size=None, **kwargs):
        async with self.reader() as reader:
            async with target_resource.writer() as writer:
                async for chunk in reader.read(chunk_size):
                    await writer.write(chunk)

    async def _local_copy_async(self, target_resource, chunk_size=None, **kwargs):
        await self._do_copy_async(target_resource, chunk_size, **kwargs)

    async def _copy_dir_async(self, target_resource, require_not_exists=True, **kwargs):
        # Can't copy a dir to a file
        if await target_resource.is_file_async():
            raise UNIOError("Cannot copy directory {} to file {}".format(self, target_resource))
        # Default copies the shutil.copytree() behaviour by requiring the target not exist
        # Can disable to overwrite the existing files if they exist
        # Note that allow_overwrite is respected by the file copying itself
        if require_not_exists and await target_resource.exists_async():
            raise UNIOError("Target directory {} already exists".format(target_resource))
        if await self.is_local_to(target_resource):
            await self._local_copy_dir_async(target_resource, **kwargs)
        else:
            await self._do_copy_dir_async(target_resource, **kwargs)

    async def _do_copy_dir_async(self, target_dir, recursive=True, preliminary_check=False, make_stub_dirs=False, **kwargs):
        if preliminary_check and not kwargs.get("allow_overwrite", False):
            async for file, target_file in self.crawl(target_dir, recursive=recursive):
                if target_file.exists():
                    raise UNIOError("Resource {} already exists".format(target_file))
        tasks = []
        async for res, target_res in self.crawl(target_dir, recursive or make_stub_dirs, recursive):
            if await res.is_dir():
                await res.mkdir_async()
            else:
                tasks.append(asyncio.create_task(res._copy_file_async(
                    target_res,
                    _skip_dir_check=True,  # We know we already took the basename, so we can skip this check
                    **kwargs
                )))
        await asyncio.gather(*tasks)

    async def _local_copy_dir_async(self, target_resource, recursive=True, **kwargs):
        await self._do_copy_dir_async(target_resource, recursive, **kwargs)

    async def crawl(self, mirror_resource=None, include_directories=False, recursive=True):
        work = [(self, mirror_resource)]
        while work:
            src, trg = work.pop()
            async for file in src.list_async():
                mirror_file = None if trg is None else trg.child(file.basename())
                check_dir = await file.is_dir()
                if check_dir or include_directories:
                    yield file if mirror_file is None else (file, mirror_file)
                    if check_dir:
                        work.append((file, mirror_file))

    async def is_local_to(self, target_resource):
        return False


class AsynchronousDescriptor(ResourceDescriptor, abc.ABC):

    def is_file(self):
        return self.loop.run(self.is_file_async())

    def is_dir(self):
        return self.loop.run(self.is_dir_async())

    def exists(self):
        return self.loop.run(self.exists_async())

    def list(self):
        for f in self.loop.run(self.list_async()):
            yield f

    def remove(self):
        return self.loop.run(self.remove_async())

    def mtime(self):
        return self.loop.run(self.mtime_async())

    def atime(self):
        return self.loop.run(self.atime_async())

    def crtime(self):
        return self.loop.run(self.crtime_async())

    def size(self):
        return self.loop.run(self.size_async())


class SynchronousDescriptor(ResourceDescriptor, abc.ABC):

    async def is_file_async(self):
        return await self.loop.execute(self.is_file)

    async def is_dir_async(self):
        return await self.loop.execute(self.is_dir)

    async def exists_async(self):
        return await self.loop.execute(self.exists)

    async def list_async(self):
        for x in await self.loop.execute(self.list):
            yield x

    async def remove_async(self):
        return await self.loop.execute(self.remove)

    async def mtime_async(self):
        return await self.loop.execute(self.mtime)

    async def atime_async(self):
        return await self.loop.execute(self.atime)

    async def crtime_async(self):
        return await self.loop.execute(self.crtime)

    async def size_async(self):
        return await self.loop.execute(self.size)


class PathResourceDescriptor(ResourceDescriptor, abc.ABC):

    def __init__(self, path):
        ResourceDescriptor.__init__(self)
        self.path = path

    def __str__(self):
        return str(self.path)

    def __repr__(self):
        return str(self.path)

    def parent(self):
        return self._create_descriptor(self.path.parent)

    def child(self, child):
        return self.joinpath(child)

    def basename(self):
        return self.path.name

    def joinpath(self, *paths):
        return self._create_descriptor(self.path.joinpath(*paths))


class UriResourceDescriptor(PathResourceDescriptor, abc.ABC):

    def __init__(self, uri, top_path_special=False, trailing_slashes_matter=False):
        super().__init__(None)
        self.uri = None
        self.hostname = None
        self.scheme = None
        self.container = None
        self.path = None
        self._is_dir = False
        self.trailing_slashes_matter = trailing_slashes_matter
        self._set_uri(uri, top_path_special)

    def _set_uri(self, uri, top_path_special=False):
        self.uri = uri
        p = urlsplit(uri)
        self.hostname = p.hostname
        self.port = p.port
        self.scheme = p.scheme
        self.container = None
        if self.trailing_slashes_matter and p.path.endswith("/"):
            self._is_dir = True
        pieces = [x for x in str(p.path).split("/") if x.strip() != ""]
        if top_path_special:
            self.container = pieces[0]
            self.path = pathlib.PurePosixPath("/{}".format("/".join(pieces[1:])))
        else:
            self.path = pathlib.PurePosixPath("/{}".format("/".join(pieces)))

    def _path_to_uri(self, path):
        pieces = []
        if self.container:
            pieces.append(self.container)
        append_trailing = self.trailing_slashes_matter and str(path).endswith("/")
        pieces.extend([x for x in str(path).split("/") if not x == ""])
        return "{}://{}/{}".format(
            self.scheme,
            self.hostname if self.port is None else "{}:{}".format(self.hostname, self.port),
            "/".join([quote_plus(p) for p in pieces]) + ("/" if append_trailing else "")
        )

    def __str__(self):
        return str(self.uri)

    def __repr__(self):
        return str(self.uri)

    def basename(self):
        if self.trailing_slashes_matter and self._is_dir:
            return self.path.name + '/'
        else:
            return self.path.name

    def parent(self):
        new_path = self.path.parent
        if self.trailing_slashes_matter:
            new_path = str(new_path) + "/"
        return self._create_descriptor(self._path_to_uri(new_path))

    def joinpath(self, *paths):
        new_path = self.path.joinpath(*paths)
        if self.trailing_slashes_matter and str(paths[-1]).endswith("/"):
            new_path = str(new_path) + "/"
        return self._create_descriptor(self._path_to_uri(new_path))
