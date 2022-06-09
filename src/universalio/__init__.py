from importlib.util import find_spec as _find_spec
from autoinject import injector as _injector

from .global_loop import GlobalLoopContext
from .fileman import FileManager
from .fileman import _FileWrapper


@_injector.inject
def _setup_file_manager(f: FileManager):
    import universalio.descriptors as desc
    # Check local file system first
    f.register_descriptor_class(desc.LocalDescriptor, 0)
    # Check HTTP structures
    if hasattr(desc, "SFTPDescriptor"):
        f.register_descriptor_class(desc.SFTPDescriptor, 100)
    if hasattr(desc, "AzureBlobDescriptor"):
        f.register_descriptor_class(desc.AzureBlobDescriptor, 100)
    # Fallback for HTTP
    if hasattr(desc, "HttpDescriptor"):
        f.register_descriptor_class(desc.HttpDescriptor, 10000)


_setup_file_manager()

FileWrapper = _FileWrapper()
