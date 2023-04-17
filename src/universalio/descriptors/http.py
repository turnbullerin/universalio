import aiohttp
import atexit
from autoinject import injector
import json
import datetime
from universalio import GlobalLoopContext
from .base import FileWriter, FileReader, UriResourceDescriptor, AsynchronousDescriptor, UNIOError, ConnectionRegistry


class HttpWriterContextManager:

    class Writer(FileWriter):

        def __init__(self, session, path):
            super().__init__(session)
            self.path = path
            self._buffer = None

        async def write(self, chunk):
            if self._buffer is None:
                self._buffer = chunk
            else:
                self._buffer += chunk

        async def finalize(self):
            async with self.handle.put(self.path, data=self._buffer) as resp:
                pass
            self._buffer = None

    def __init__(self, uri, session):
        self.uri = uri
        self._session = session
        self._writer = None

    async def __aenter__(self):
        self._writer = HttpWriterContextManager.Writer(await self._session, self.uri)
        return self._writer

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            await self._writer.finalize()


class HttpReaderContextManager:

    class Reader(FileReader):

        async def read(self, chunk_size=None):
            chunk_size = chunk_size or self.chunk_size
            async for chunk in self.handle.content.iter_chunked(chunk_size):
                yield chunk

    def __init__(self, uri, session):
        self.uri = uri
        self._session_coro = session
        self._session = None
        self._handle = None
        self._get = None

    async def __aenter__(self):
        self._session = await self._session_coro
        self._get = await self._session.get(self.uri)
        self._handle = await self._get.__aenter__()
        return HttpReaderContextManager.Reader(self._handle)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._get.__aexit__(exc_type, exc_val, exc_tb)


@injector.injectable
class HttpSessionRegistry(ConnectionRegistry):

    def _construct_key(self, *args, **kwargs):
        return "1"

    async def _create_connection(self, *args, **kwargs):
        return aiohttp.ClientSession()

    async def _close_connection(self, conn):
        await conn.close()


class HttpDescriptor(UriResourceDescriptor, AsynchronousDescriptor):

    session: HttpSessionRegistry = None
    loop: GlobalLoopContext = None

    @injector.construct
    def __init__(self, uri):
        super().__init__(uri, trailing_slashes_matter=True)
        self._is_canonical = None

    def _send_headers(self):
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36",
        }

    async def _client(self):
        return await self.session.connect()

    async def _head(self):
        await self.canonicalize()
        return await self._cached_async("head", self._head_call)

    async def _options(self):
        await self.canonicalize()
        return await self._cached_async("options", self._options_call)

    async def _options_call(self):
        headers = {}
        client = await self._client()
        async with client.head(self.uri, headers=self._send_headers()) as response:
            if response.status == 200:
                headers = response.headers
        return headers

    async def _head_call(self):
        headers, status = {}, None
        client = await self._client()
        async with client.head(self.uri, headers=self._send_headers()) as response:
            status = response.status
            if response.status == 200:
                headers = response.headers
        return headers, status

    async def _supports_http_method(self, method):
        headers = await self._options()
        if "Allow" not in headers:
            return False
        method = method.upper()
        for m in headers.get("Allow").split(","):
            if m.strip().upper() == method:
                return True
        return False

    async def canonicalize(self):
        if self._is_canonical:
            return
        check = True
        while check:
            check = False
            headers, status = await self._head_call()
            if status in [301, 302, 307, 308] and "Location" in headers:
                self._set_uri(headers.get("Location"))
                check = True
            else:
                self._set_cache("head", (headers, status))
                self._is_canonical = True

    async def detect_encoding_async(self):
        headers, status = await self._head()
        if "Content-Type" in headers:
            h = headers.get("Content-Type")
            if "charset=" in h:
                p = h.find("charset=") + 8
                if ";" in h[p:]:
                    return h[p:h.find(";", p)]
                else:
                    return h[p:]
        return await super().detect_encoding_async()

    async def is_dir_async(self):
        # By convention
        return self.uri.endswith("/")

    async def is_file_async(self):
        # By convention
        return not self.uri.endswith("/")

    async def fingerprint_async(self):
        headers, status = await self._head()
        if status >= 300:
            return None
        if "ETag" in headers:
            return headers.get("ETag")
        return await super().fingerprint_async()

    async def exists_async(self):
        headers, status = await self._head()
        return status == 200

    async def list_async(self):
        if self._supports_http_method("PROPFIND"):
            #raise NotImplementedError("WebDAV listings not yet implemented")
            return
        head, stat = self._head()
        if not stat == 200:
            return
        content = await self.read_async()
        txt = content.decode(await self.detect_encoding_async())
        ctype = head.get("Content-Type", "text/html").lower()
        if ";" in ctype:
            ctype = ctype[:ctype.find(";")]
        if ctype == "application/json":
            json_list = json.loads(txt)
            if not isinstance(json_list, list):
                raise UNIOError("Invalid JSON response to GET on a directory")
            for f in json_list:
                yield self.child(f)
        elif ctype in ("text/html", "application/xhtml+xml"):
            #raise NotImplementedError("Direct HTML parsing for children not yet available")
            return
        elif ctype in ("text/xml", "application/xml"):
            # Need to check if this is poor server support for text/html or application/xhtml+xml
            #raise NotImplementedError("Direct XML parsing for children not yet available")
            return
        else:
            return

    async def _do_rmdir_async(self):
        await self.remove_async()

    async def _do_mkdir_async(self):
        if self._supports_http_method("MKCOL"):
            client = await self._client()
            async with client.request("MKCOL", self.uri) as resp:
                resp.raise_for_status()

    async def _supports_fast_rename_async(self):
        opts = await self._options()
        methods = [x.strip().upper() for x in opts.get("Accept", "")]
        return "MOVE" in methods

    async def _local_move_file_async(self, target, **kwargs):
        opts = await self._options()
        methods = [x.strip().upper() for x in opts.get("Accept", "")]
        # Fast way if the server supports HTTP MOVE requests (e.g. WebDAV)
        if "MOVE" in methods:
            headers = {
                "Destination": target.uri
            }
            client = await self._client()
            async with client.request("MOVE", self.uri, headers=headers) as resp:
                resp.raise_for_status()

        # Slightly less efficient, but still fast if it supports COPY and DELETE
        elif "COPY" in methods and "DELETE" in methods:
            headers = {
                "Destination": target.uri
            }
            client = await self._client()
            async with client.request("COPY", self.uri, headers=headers) as resp:
                resp.raise_for_status()
            await self.remove_async()

        else:
            await super()._do_move_file_async(target, **kwargs)

    async def _local_copy_async(self, target, chunk_size=None, **kwargs):
        if await self._supports_http_method("COPY"):
            headers = {
                "Destination": target.uri
            }
            client = await self._client()
            async with client.request("COPY", self.uri, headers=headers) as resp:
                resp.raise_for_status()
        else:
            await super()._do_copy_async(target, chunk_size, **kwargs)

    async def remove_async(self):
        if not await self._supports_http_method("DELETE"):
            raise UNIOError("Delete not supported on this uri: {}".format(self.uri))
        client = await self._client()
        async with client.delete(self.uri) as response:
            response.raise_for_status()

    def _parse_http_datetime(self, s):
        if s is None or s == "":
            return None
        return datetime.datetime.strptime(str(s), "%a, %d %b %Y %H:%M:%S GMT") \
            .replace(tzinfo=datetime.timezone.utc)

    async def mtime_async(self):
        head, stat = await self._head()
        return self._parse_http_datetime(head.get("Last-Modified", None))

    async def atime_async(self):
        head, stat = await self._head()
        return self._parse_http_datetime(head.get("Last-Accessed", None))

    async def crtime_async(self):
        head, stat = await self._head()
        return self._parse_http_datetime(head.get("Content-Created", None))

    async def size_async(self):
        head, stat = await self._head()
        return self._parse_http_datetime(head.get("Content-Length", None))

    def reader(self):
        return HttpReaderContextManager(self.uri, self._client())

    def writer(self):
        self.clear_cache()
        return HttpWriterContextManager(self.uri, self._client())

    def is_local_to_async(self, resource):
        if not isinstance(resource, HttpDescriptor):
            return False
        return self.hostname == resource.hostname

    @staticmethod
    def match_location(location):
        return location.lower().startswith("http://") or location.lower().startswith("https://")

    @staticmethod
    def create_from_location(location: str):
        return HttpDescriptor(location)
