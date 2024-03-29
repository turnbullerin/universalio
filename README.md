# UniversalIO

UniversalIO is a library for Python developers looking to seamlessly integrate multiple different
file storage solutions. 

The concept was born out of a need for a project to allow users to specify files which might be in the
Azure Blob Storage, Azure File Store, on a network drive, or on a SFTP server and then manipulate these
files. In addition, there was a need to write the same file back to multiple different locations. The
solution was to wrap the specific API for those storage solutions in an API that allowed the system to
operate on them consistently, as well as coordinating file transfers between them. The need for batch
operations led to the adoption of asyncio and its implementations for most of the backend, though 
non-asynchronous methods are available as well. 


## Operations Supported

- is_dir()
- is_file()
- exists()
- parent()
- child(child_name)
- basename()
- joinpath(*paths)
- remove()
- read(block_size)
- write(bytes)
- text(encoding)
- copy()
- move()
- mkdir(recursive)
- rmdir(recursive)
- crawl()
- detect_encoding()
- is_local_to(resource)

Asynchronous versions of most of the above exist with the suffix _async


## Storage Solutions Supported

- Local/network drives (via aiofiles)
- SFTP (via asyncssh)
- Azure Blob Storage (via azure.storage.blob.aio)
- HTTP servers (via aiohttp)
  - Requires GET support for read access
  - Requires PUT support for write access
  - Requires DELETE support for delete
  - Supports MKCOL for basic directory/collection creation
  - Supports COPY, falls back to a GET and a PUT, for file copying
  - Supports MOVE, falls back to a COPY and a DELETE, for file moving


## Planned Enhancements

- Some restructuring to match the pathlib API as much as possible
- Support for pulling credentials from TOML files in user's home directory
- Support for Azure File Storage
- Support for FTP and FTPS
- Support for direct HTTP access and HTML scraping
- Support for a batch file uploader/downloader
- Support for glob() matching and other searching options
- Support for pathlib-style with_name() to change the file name
- Support for touch()
- Support for writing text directly
- Support for HTTP authentication methods, including a custom method to support custom API calls
- More direct support for file copy/moving if the descriptor and connection is the same
- Considering support for SSH/SCP
- Considering support for relative file paths on local/network drives (challenging to understand what the root would be?)
- Considering support for chmod/etc (but not supported on Windows)
- Considering support for symbolic link creation (but complex on Windows)