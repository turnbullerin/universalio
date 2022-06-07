from .global_loop import GlobalLoopContext
from .fileman import FileManager
from .fileman import _FileWrapper
from autoinject import injector as _injector


@_injector.inject
def _setup_file_manager(f: FileManager):
    import universalio.descriptors as desc
    f.register_descriptor_class(desc.LocalDescriptor, 0)
    f.register_descriptor_class(desc.SFTPDescriptor, 100)


_setup_file_manager()

FileWrapper = _FileWrapper()
