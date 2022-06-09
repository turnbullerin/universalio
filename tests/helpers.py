import os


def recursive_rmdir(directory, keep=None, contents_only=False):
    for f in os.scandir(directory):
        if f.is_dir():
            recursive_rmdir(f.path)
        elif keep and f.name in keep:
            continue
        else:
            os.remove(f.path)
    if not contents_only:
        os.rmdir(directory)
