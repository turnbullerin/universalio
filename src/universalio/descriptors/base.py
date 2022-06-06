import abc
import pathlib
from urllib.parse import urlsplit
import asyncio
from autoinject import injector
import atexit
from universalio import GlobalLoopContext

DEFAULT_CHUNK_SIZE = 1048576


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


class FileReader(abc.ABC):

    def __init__(self):
        pass

    @abc.abstractmethod
    async def chunks(self, chunk_size=DEFAULT_CHUNK_SIZE):
        pass


class FileWriter(abc.ABC):

    def __init__(self):
        pass

    @abc.abstractmethod
    async def write_chunk(self, chunk):
        pass


class ResourceDescriptor(abc.ABC):

    loop: GlobalLoopContext = None

    @injector.construct
    def __init__(self):
        pass

    @abc.abstractmethod
    def is_dir(self):
        pass

    @abc.abstractmethod
    def exists(self):
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
    async def is_file_async(self):
        pass

    @abc.abstractmethod
    async def is_dir_async(self):
        pass

    @abc.abstractmethod
    async def exists_async(self):
        pass

    @abc.abstractmethod
    async def list_async(self):
        pass

    def _create_descriptor(self, *args, **kwargs):
        return self.__class__(*args, **kwargs)

    def copy_all_to(self, target_dir, allow_overwrite=False, chunk_size=DEFAULT_CHUNK_SIZE, recursive=False):
        asyncio.gather(self.copy_all_to_async(target_dir, allow_overwrite, chunk_size, recursive, await_completion=True))

    async def copy_all_to_async(self, target_dir, allow_overwrite=False, chunk_size=DEFAULT_CHUNK_SIZE, recursive=False, await_completion=False):
        if not await self.is_dir_async():
            raise ValueError("Resource {} is not a directory".format(self))
        work = [(self, target_dir)]
        tasks = []
        while work:
            src, trg = work.pop()
            async for file in src.list_async():
                if await file.is_file():
                    tasks.append(asyncio.create_task(self.copy_from_async(file, trg.child(file.basename()))))
                else:
                    work.append((file, trg.child(file.basename())))
        if await_completion:
            await asyncio.gather(*tasks)
            return None
        return tasks

    def copy_from(self, source_resource, allow_overwrite=False, chunk_size=DEFAULT_CHUNK_SIZE):
        self.loop.run(self.copy_from_async(source_resource, allow_overwrite, chunk_size))

    async def copy_from_async(self, source_resource, allow_overwrite=False, chunk_size=DEFAULT_CHUNK_SIZE):
        await source_resource.copy_to_async(self, allow_overwrite, chunk_size)

    def copy_to(self, target_resource, allow_overwrite=False, chunk_size=DEFAULT_CHUNK_SIZE):
        self.loop.run(self.copy_from_async(target_resource, allow_overwrite, chunk_size))

    async def copy_to_async(self, target_resource, allow_overwrite=False, chunk_size=DEFAULT_CHUNK_SIZE):
        if not await self.is_file_async():
            raise ValueError("Resource {} is not a file".format(self))
        if (not allow_overwrite) and await target_resource.exists_async():
            raise ValueError("Resource {} already exists".format(target_resource))
        await self._do_copy_async(target_resource, chunk_size)

    async def _do_copy_async(self, target_resource, chunk_size=DEFAULT_CHUNK_SIZE):
        async with self.reader() as reader:
            async with target_resource.writer() as writer:
                async for chunk in reader.chunks(chunk_size):
                    await writer.write_chunk(chunk)


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
        return self._create_descriptor(self.path.parent / child)

    def basename(self):
        return self.path.name


class UriResourceDescriptor(PathResourceDescriptor, abc.ABC):

    def __init__(self, uri):
        self.uri = uri
        p = urlsplit(self.uri)
        self.hostname = p.hostname
        self.port = p.port
        self.scheme = p.scheme
        PathResourceDescriptor.__init__(self, pathlib.PurePosixPath(p.path))

    def _path_to_uri(self, path):
        return "{}://{}/{}".format(
            self.scheme,
            self.hostname if self.port is None else "{}:{}".format(self.hostname, self.port),
            str(path).lstrip("/")
        )

    def __str__(self):
        return str(self.uri)

    def __repr__(self):
        return str(self.uri)

    def parent(self):
        return self._create_descriptor(self._path_to_uri(self.path.parent))

    def child(self, child):
        return self._create_descriptor(self._path_to_uri(self.path / child))
