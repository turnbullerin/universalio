import abc
from urllib.parse import urlsplit
import asyncio
from functools import wraps, partial

DEFAULT_CHUNK_SIZE = 1048576


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
        asyncio.run(self.copy_from_async(source_resource, allow_overwrite, chunk_size))

    async def copy_from_async(self, source_resource, allow_overwrite=False, chunk_size=DEFAULT_CHUNK_SIZE):
        await source_resource.copy_to_async(self, allow_overwrite, chunk_size)

    def copy_to(self, target_resource, allow_overwrite=False, chunk_size=DEFAULT_CHUNK_SIZE):
        asyncio.run(self.copy_from_async(target_resource, allow_overwrite, chunk_size))

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
        return asyncio.run(self.is_file_async())

    def is_dir(self):
        return asyncio.run(self.is_dir_async())

    def exists(self):
        return asyncio.run(self.exists_async())

    def list(self):
        for f in asyncio.run(self.list_async()):
            yield f


class SynchronousDescriptor(ResourceDescriptor, abc.ABC):

    async def is_file_async(self):
        return await asyncio.get_running_loop().run_in_executor(None, self.is_file)

    async def is_dir_async(self):
        return await asyncio.get_running_loop().run_in_executor(None, self.is_dir)

    async def exists_async(self):
        return await asyncio.get_running_loop().run_in_executor(None, self.exists)

    async def list_async(self):
        async for x in await asyncio.get_running_loop().run_in_executor(None, self.list):
            yield x


class PathResourceDescriptor(ResourceDescriptor, abc.ABC):

    def __init__(self, path):
        super().__init__()
        self.path = path

    def __str__(self):
        return str(self.path)

    def __repr__(self):
        return str(self.path)

    def parent(self):
        return self.__class__(self.path.parent)

    def child(self, child):
        return self.__class__(self.path / child)

    def basename(self):
        return self.path.name


class UriResourceDescriptor(ResourceDescriptor, abc.ABC):

    def __init__(self, uri):
        super().__init__()
        self.uri = uri
        p = urlsplit(self.uri)
        self.hostname = p.hostname
        self.port = p.port
        self.scheme = p.scheme
        self.path = p.path[1:]

    def __str__(self):
        return str(self.uri)

    def __repr__(self):
        return str(self.uri)

    def _parent_path(self):
        p = urlsplit(self.uri)
        return "{}://{}/{}".format(p.scheme, p.netloc, "/".join(p.path.split("/")[:-1]))

    def basename(self):
        p = urlsplit(self.uri)
        return p.path.split("/")[-1]

