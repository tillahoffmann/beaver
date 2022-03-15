import contextlib
import os


@contextlib.contextmanager
def working_directory(directory):
    """
    Temporarily change the working directory using a context manager.
    """
    current = os.getcwd()
    os.chdir(directory)
    yield directory
    os.chdir(current)
