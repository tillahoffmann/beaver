import aiohttp
import asyncio
import logging
import typing
from .base import Artifact, File, Transformation


LOGGER = logging.getLogger(__name__)


class Sleep(Transformation):
    """
    Sleeps for a given number of seconds and create any file artifact outputs.

    Args:
        time: Number of seconds to sleep for.
    """
    def __init__(self, outputs, inputs, *, time):
        super().__init__(outputs, inputs)
        self.time = time

    async def execute(self):
        LOGGER.info("running %s for %f seconds...", self, self.time)
        await asyncio.sleep(self.time)
        for output in self.outputs:
            if isinstance(output, File):
                with open(output.name, 'w'):
                    pass
                LOGGER.info("created %s", output)
        LOGGER.info("completed %s", self)


class Download(Transformation):
    """
    Download a file and verify its digest.

    Args:
        output: Output artifact for the downloaded data.
        url: Url to download from.
        digest: Expected digest of the downloaded data.
    """
    def __init__(self, output: File, url: str, digest: typing.Union[str, bytes]) -> None:
        super().__init__(output, [])
        self.digest = bytes.fromhex(digest) if isinstance(digest, str) else digest
        self.url = url

    async def execute(self):
        # Abort if we already have the right file.
        output, = self.outputs
        if output.digest == self.digest:
            return
        # Download the file.
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url) as response:
                with open(output.name, 'wb') as fp:
                    fp.write(await response.read())
        if output.digest != self.digest:
            raise ValueError(f"expected digest `{self.digest.hex()}` but got "
                             f"`{output.digest.hex()}` for {output}")


class Shell(Transformation):
    """
    Execute a shell command.
    """
    def __init__(self, outputs: typing.Iterable[Artifact], inputs: typing.Iterable[Artifact],
                 cmd, **kwargs) -> None:
        super().__init__(outputs, inputs)
        if isinstance(cmd, typing.Iterable) and not isinstance(cmd, str):
            cmd = " ".join(f"'{x}'" if " " in x else x for x in cmd)
        elif not isinstance(cmd, str):
            raise TypeError(cmd)
        self.cmd = cmd
        self.kwargs = kwargs

    async def execute(self) -> None:
        process = await asyncio.subprocess.create_subprocess_shell(self.cmd, **self.kwargs)
        status = await process.wait()
        if status:
            raise RuntimeError(f"{self} failed with status code {status}")


class Functional(Transformation):
    """
    Apply a python function.
    """
    def __init__(self, outputs: typing.Iterable[Artifact], inputs: typing.Iterable[Artifact],
                 func: typing.Callable, *args, **kwargs) -> None:
        super().__init__(outputs, inputs)
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def execute(self):
        return self.func(self.outputs, self.inputs, *self.args, **self.kwargs)
