import aiohttp
import asyncio
import hashlib
from loguru import logger
import typing
from . import artifacts


class Transformation:
    """
    Transformation that generates outputs given inputs.

    .. note::

       A transformation will be executed if any of its inputs or outputs have changed since the last
       execution. Consequently, manual changes to generated artifacts will be overwritten. A missing
       output is considered "changed" and will be generated if it is missing.

    Args:
        outputs: Artifacts to generate.
        inputs: Artifacts consumed by the transformation.
    """
    def __init__(self, outputs: typing.Iterable["artifacts.Artifact"],
                 inputs: typing.Iterable["artifacts.Artifact"]) -> None:
        self.inputs = artifacts.normalize_artifacts(inputs)
        for input in self.inputs:
            input.children.append(self)
        self.outputs = artifacts.normalize_artifacts(outputs)
        for output in self.outputs:
            output.parent = self
        self.future = None

    def __iter__(self):
        for output in self.outputs:
            yield output

    def evaluate_composite_digests(self) -> dict["artifacts.Artifact", bytes]:
        # Initialise empty digests for the outputs.
        digests = {o.name: None for o in self.outputs}

        # Evaluate input digests and abort if any of them are missing.
        input_hasher = hashlib.sha256()
        for i in self.inputs:
            if i.digest is None:
                return digests
            input_hasher.update(i.digest)

        # Construct composite digests for the outputs.
        for output in self.outputs:
            if output.digest is None:
                continue
            hasher = input_hasher.copy()
            hasher.update(output.digest)
            digests[output.name] = hasher.hexdigest()
        return digests

    async def __call__(self) -> None:
        # Wait for all inputs to have been generated.
        await asyncio.gather(*(i() for i in self.inputs))

        # Check if any composite indices have changed. If no, there's nothing further to be done.
        composite_digests = self.evaluate_composite_digests()
        if all(digest is not None and digest == self.COMPOSITE_DIGESTS.get(name) for name, digest
               in composite_digests.items()):
            logger.debug('no inputs or outputs of {} have changed', self)
            return

        # Create a future if required and wait for it to complete.
        if not self.future:
            self.future = asyncio.create_task(self._execute_with_semaphore())
        result = await self.future
        if result is not None:
            raise ValueError("transformations should return `None`")

        # Update the composite digests.
        self.COMPOSITE_DIGESTS.update(self.evaluate_composite_digests())

    async def _execute_with_semaphore(self):
        """
        Private wrapper to execute a transformation wrapped in a semaphore to limit concurrency.
        """
        if self.SEMAPHORE is not None:
            async with self.SEMAPHORE:
                return await self.execute()
        return await self.execute()

    async def execute(self):
        """
        Execute the transformation.
        """
        raise NotImplementedError

    def __repr__(self):
        inputs = ', '.join(map(repr, self.inputs))
        outputs = ', '.join(map(repr, self.outputs))
        return f"{self.__class__.__name__}([{inputs}] -> [{outputs}])"

    COMPOSITE_DIGESTS = {}
    SEMAPHORE = None


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
        logger.info("running %s for %f seconds...", self, self.time)
        await asyncio.sleep(self.time)
        for output in self.outputs:
            if isinstance(output, artifacts.File):
                with open(output.name, 'w'):
                    pass
                logger.info("created %s", output)
        logger.info("completed %s", self)


class Download(Transformation):
    """
    Download a file and verify its digest.

    Args:
        output: Output artifact for the downloaded data.
        url: Url to download from.
        digest: Expected digest of the downloaded data.
    """
    def __init__(self, output: "artifacts.File", url: str, digest: typing.Union[str, bytes]) \
            -> None:
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
    def __init__(self, outputs: typing.Iterable["artifacts.Artifact"],
                 inputs: typing.Iterable["artifacts.Artifact"], cmd, **kwargs) -> None:
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
    def __init__(self, outputs: typing.Iterable["artifacts.Artifact"], inputs:
                 typing.Iterable["artifacts.Artifact"], func: typing.Callable, *args, **kwargs) \
            -> None:
        super().__init__(outputs, inputs)
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def execute(self):
        return self.func(self.outputs, self.inputs, *self.args, **self.kwargs)
