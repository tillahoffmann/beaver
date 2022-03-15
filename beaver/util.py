import asyncio
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


async def gather_artifacts(*artifacts):
    """
    Gather one or more artifacts and wrap them in a coroutine.
    """
    await asyncio.gather(*(artifact() for artifact in artifacts))


async def funny_coverage(a, b):
    if a == b:
        return
    print(a, b)
