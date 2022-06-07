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
- read(block_size)
- write(bytes)
- text(encoding)
- copy_from()
- copy_to()
- copy_all_to()

Asynchronous versions of most of the above exist with the suffix _async


## Storage Solutions Supported

- Local/network drives (via aiofiles)
- SFTP (via asyncssh)
- Azure Blob Storage (via azure.storage.blob.aio)


## Planned Enhancements

- Some restructuring to match the pathlib API as much as possible
- Support for pulling credentials from TOML files in user's home directory
- Support for Azure File Storage
- Support for FTP and FTPS
- Support for direct HTTP access and HTML scraping
- Support for a batch file uploader/downloader
- Support for / operator as a synonym for .child(), like pathlib does
- Support for .joinpath() to skip creating descriptors when going two-levels deep
- Support for glob() matching
- Support for recursive listing
- Support for pathlib-style with_name() to change the file name
- Support for file metadata via stat
- Support for mkdir() and rmdir() as well as recursive rmdir() and recursive mkdir()
- Support for file renaming, removing
- Support for touch()
- Support for writing text directly
- More direct support for file copy/moving if the descriptor and connection is the same
- Considering support for SSH/SCP
- Considering support for relative file paths on local/network drives
- Considering support for chmod/etc (but not supported on Windows)
- Considering support for symbolic link creation (but complex on Windows)