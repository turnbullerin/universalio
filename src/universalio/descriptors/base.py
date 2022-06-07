import abc
import pathlib
from urllib.parse import urlsplit, quote_plus
import asyncio
from autoinject import injector
import atexit
from universalio import GlobalLoopContext

DEFAULT_CHUNK_SIZE = 5 * 1024 * 1024


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
        pass

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

    def text(self, encoding="utf-8"):
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

    def copy_all_to(self, target_dir, allow_overwrite=False, chunk_size=DEFAULT_CHUNK_SIZE, recursive=False):
        self.loop.run(self.copy_all_to_async(target_dir, allow_overwrite, chunk_size, recursive, await_completion=True))

    async def copy_all_to_async(self, target_dir, allow_overwrite=False, chunk_size=DEFAULT_CHUNK_SIZE, recursive=False, await_completion=False):
        if not await self.is_dir_async():
            raise ValueError("Resource {} is not a directory".format(self))
        work = [(self, target_dir)]
        tasks = []
        while work:
            src, trg = work.pop()
            async for file in src.list_async():
                if await file.is_file():
                    tasks.append(asyncio.create_task(file.copy_to_async(trg.child(file.basename()), allow_overwrite, chunk_size)))
                else:
                    work.append((file, trg.child(file.basename())))
        if await_completion:
            await asyncio.gather(*tasks)
            return []
        return tasks

    def copy_from(self, source_resource, allow_overwrite=False, chunk_size=DEFAULT_CHUNK_SIZE):
        self.loop.run(self.copy_from_async(source_resource, allow_overwrite, chunk_size))

    def copy_to(self, target_resource, allow_overwrite=False, chunk_size=DEFAULT_CHUNK_SIZE):
        self.loop.run(self.copy_to_async(target_resource, allow_overwrite, chunk_size))

    async def copy_from_async(self, source_resource, allow_overwrite=False, chunk_size=DEFAULT_CHUNK_SIZE):
        await source_resource.copy_to_async(self, allow_overwrite, chunk_size)

    async def copy_to_async(self, target_resource, allow_overwrite=False, chunk_size=DEFAULT_CHUNK_SIZE):
        if not await self.is_file_async():
            raise ValueError("Resource {} is not a file".format(self))
        if (not allow_overwrite) and await target_resource.exists_async():
            raise ValueError("Resource {} already exists".format(target_resource))
        await self._do_copy_async(target_resource, chunk_size)

    async def _do_copy_async(self, target_resource, chunk_size=DEFAULT_CHUNK_SIZE):
        async with self.reader() as reader:
            async with target_resource.writer() as writer:
                async for chunk in reader.read(chunk_size):
                    await writer.write(chunk)


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
        return self._create_descriptor(self.path / child)

    def basename(self):
        return self.path.name


class UriResourceDescriptor(PathResourceDescriptor, abc.ABC):

    def __init__(self, uri, top_path_special=False):
        self.uri = uri
        p = urlsplit(self.uri)
        self.hostname = p.hostname
        self.port = p.port
        self.scheme = p.scheme
        self.container = None
        pieces = [x for x in str(p.path).split("/") if x.strip() != ""]
        if top_path_special:
            self.container = pieces[0]
            PathResourceDescriptor.__init__(self, pathlib.PurePosixPath("/{}".format("/".join(pieces[1:]))))
        else:
            PathResourceDescriptor.__init__(self, pathlib.PurePosixPath("/{}".format("/".join(pieces))))

    def _path_to_uri(self, path):
        pieces = []
        if self.container:
            pieces.append(self.container)
        pieces.extend([x for x in str(path).split("/") if not x == ""])
        return "{}://{}/{}".format(
            self.scheme,
            self.hostname if self.port is None else "{}:{}".format(self.hostname, self.port),
            "/".join([quote_plus(p) for p in pieces])
        )

    def __str__(self):
        return str(self.uri)

    def __repr__(self):
        return str(self.uri)

    def parent(self):
        return self._create_descriptor(self._path_to_uri(self.path.parent))

    def child(self, child):
        return self._create_descriptor(self._path_to_uri(self.path / child))
