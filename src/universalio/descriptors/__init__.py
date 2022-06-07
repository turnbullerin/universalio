from importlib.util import find_spec as _find_spec

from .local import LocalDescriptor

if _find_spec("aiohttp"):
    from .http import HttpDescriptor

if _find_spec("asyncssh"):
    from .sftp import SFTPDescriptor

if _find_spec("azure.storage.blob.aio"):
    from .azure_blob import AzureBlobDescriptor
