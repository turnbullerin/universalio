from autoinject import injector
from universalio.descriptors.base import ResourceDescriptor
import universalio.descriptors as desc

# Backwards support for Python 3.7
import importlib.util
if importlib.util.find_spec("importlib.metadata"):
    from importlib.metadata import entry_points
else:
    from importlib_metadata import entry_points


@injector.injectable
class FileManager:

    def __init__(self, include_entry_points=True):
        self.registry = {}
        self.registry_order = None
        # Check local file system first
        self.register_descriptor_class(desc.LocalDescriptor, 0)
        # Todo, move these to their own module eventually
        if hasattr(desc, "SFTPDescriptor"):
            self.register_descriptor_class(desc.SFTPDescriptor, 100)
        if hasattr(desc, "AzureBlobDescriptor"):
            self.register_descriptor_class(desc.AzureBlobDescriptor, 100)
        if include_entry_points:
            auto_register = entry_points(group="universalio.descriptors")
            for ep in auto_register:
                cls = ep.load()
                weight = 100
                if hasattr(cls, "weight"):
                    weight = getattr(cls, "weight")
                self.register_descriptor_class(cls, weight)
        # Fallback for HTTP
        if hasattr(desc, "HttpDescriptor"):
            self.register_descriptor_class(desc.HttpDescriptor, 10000)

    def register_descriptor_class(self, descriptor_class: type, weight: int=0):
        self.registry[descriptor_class.__name__] = (descriptor_class, weight)
        self.registry_order = None

    def get_descriptor(self, location) -> ResourceDescriptor:
        if isinstance(location, ResourceDescriptor):
            return location
        if self.registry_order is None:
            sort_me = [(key, self.registry[key][1]) for key in self.registry]
            sort_me.sort(key=lambda x: x[1])
            self.registry_order = [x[0] for x in sort_me]
        for key in self.registry_order:
            if self.registry[key][0].match_location(location):
                return self.registry[key][0].create_from_location(location)
        raise ValueError("No descriptor class found for location {}".format(location))


class _FileWrapper:

    file_manager: FileManager = None

    @injector.construct
    def __init__(self):
        pass

    def __call__(self, location) -> ResourceDescriptor:
        return self.file_manager.get_descriptor(location)
