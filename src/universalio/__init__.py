from .global_loop import GlobalLoopContext
from .fileman import FileManager
from .fileman import _FileWrapper
from universalio.batch.batch import BatchFileCopy
import zirconium


@zirconium.configure
def _config_file_manager(config):
    # Default configuration paths
    config.register_file("~/.universalio.toml", weight=-5)
    config.register_file("./.universalio.toml", weight=-4)


FileWrapper = _FileWrapper()


__version__ = "0.0.6"
