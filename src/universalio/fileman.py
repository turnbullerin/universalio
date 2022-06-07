from autoinject import injector


@injector.injectable
class FileManager:

    def __init__(self):
        self.registry = {}
        self.registry_order = None

    def register_descriptor_class(self, descriptor_class: type, weight: int=0):
        self.registry[descriptor_class.__name__] = (descriptor_class, weight)
        self.registry_order = None

    def get_descriptor(self, location):
        if self.registry_order is None:
            sort_me = [(key, self.registry[key][1]) for key in self.registry]
            sort_me.sort(key=lambda x: x[1])
            self.registry_order = [x[0] for x in sort_me]
        for key in self.registry_order:
            if self.registry[key].match_location(location):
                return self.registry[key].create_from_location(location)
        raise ValueError("No descriptor class found for location {}".format(location))


class _FileWrapper:

    file_manager: FileManager = None

    @injector.construct
    def __init__(self):
        pass

    def __call__(self, location):
        return self.file_manager.get_descriptor(location)
