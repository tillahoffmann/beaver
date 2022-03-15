import aiohttp
import asyncio
import hashlib
import logging
import os
import re
import typing
from . import artifacts


LOGGER = logging.getLogger(__name__)


class Transformation:
    """
    Transformation that generates outputs given inputs.

    Args:
        outputs: Artifacts to generate.
        inputs: Artifacts consumed by the transformation.

    Attributes:
        COMPOSITE_DIGESTS: Mapping of artifact names to composite digests. See
            :func:`evaluate_composite_digests` for details.
        SEMAPHORE: Semaphore to limit the number of concurrent transformations being executed.
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

    def evaluate_composite_digests(self) -> dict[str, bytes]:
        """
        Evaluate composite digests for all :attr:`outputs` of the transformation.

        A composite digest is defined as the digest of the digests of inputs and the digest of the
        output, i.e. it is a joint, concise summary of both inputs and outputs. If any composite
        digest differs from the digest in :attr:`COMPOSITE_DIGESTS` or is :code:`None`, the
        transformation needs to be executed. A composite digest is :code:`None` if the corresponding
        output digest is :code:`None` or any input digests are :code:`None`.

        Returns:
            digests: Mapping from output names to composite digests.
        """
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
        # Wait for all inputs artifacts.
        await asyncio.gather(*self.inputs)
        # Create a future if required and wait for it to complete.
        if not self.future:
            self.future = asyncio.create_task(self._execute())
        await self.future

    async def _execute(self) -> None:
        """
        Private wrapper to determine whether the transformation needs to be executed and wrap it in
        a semaphore to limit concurrency.
        """
        # Check if any composite indices have changed. If no, there's nothing further to be done.
        composite_digests = self.evaluate_composite_digests()
        stale_artifacts = [name for name, digest in composite_digests.items() if digest is None or
                           digest != self.COMPOSITE_DIGESTS.get(name)]
        if not stale_artifacts:
            LOGGER.debug("composite digests %s are unchanged; %s will not be executed",
                         composite_digests, self)
            LOGGER.info("artifacts [%s] are up to date", ", ".join(o.name for o in self.outputs))
            return

        LOGGER.info("artifacts [%s] are stale; schedule transformation", ", ".join(stale_artifacts))

        if self.SEMAPHORE is None:
            await self.execute()
        else:
            async with self.SEMAPHORE:
                await self.execute()

        # Update the composite digests.
        composite_digests = self.evaluate_composite_digests()
        LOGGER.info("generated artifacts [%s]", ", ".join(o.name for o in self.outputs))
        self.COMPOSITE_DIGESTS.update(composite_digests)

    async def execute(self) -> None:
        """
        Execute the transformation.
        """
        raise NotImplementedError

    def __repr__(self):
        inputs = ", ".join(map(repr, self.inputs))
        outputs = ", ".join(map(repr, self.outputs))
        return f"{self.__class__.__name__}([{inputs}] -> [{outputs}])"

    def __await__(self):
        # See https://stackoverflow.com/a/57078217/1150961 for details.
        return (yield from self().__await__())

    COMPOSITE_DIGESTS: typing.Mapping[str, bytes] = {}
    SEMAPHORE: typing.Optional[asyncio.Semaphore] = None


class Sleep(Transformation):
    """
    Sleeps for a given number of seconds and create any file artifact outputs. Mostly used for
    testing and debugging.

    Args:
        outputs: Artifacts to generate.
        inputs: Artifacts consumed by the transformation.
        time: Number of seconds to sleep for.
    """
    def __init__(self, outputs: typing.Iterable["artifacts.Artifact"],
                 inputs: typing.Iterable["artifacts.Artifact"], *, time: float) -> None:
        super().__init__(outputs, inputs)
        self.time = time

    async def execute(self) -> None:
        LOGGER.debug("running %s for %f seconds...", self, self.time)
        await asyncio.sleep(self.time)
        for output in self.outputs:
            if isinstance(output, artifacts.File):
                with open(output.name, "w") as fp:
                    fp.write(output.name)
                LOGGER.debug("created %s", output)
        LOGGER.debug("completed %s", self)


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

    async def execute(self) -> None:
        # Abort if we already have the right file.
        output, = self.outputs
        if output.digest == self.digest:
            return
        # Download the file.
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url) as response:
                with open(output.name, "wb") as fp:
                    fp.write(await response.read())
        if output.digest != self.digest:
            raise ValueError(f"expected digest `{self.digest.hex()}` but got "
                             f"`{output.digest.hex()}` for {output}")


class Shell(Transformation):
    r"""
    Execute a shell command.

    The transformation supports variable substitution using f-strings and Makefile syntax:

    - :code:`$@` represents the first output.
    - :code:`$<` represents the first input.
    - :code:`$^` represents all inputs.
    - :code:`$$@` represents the literal :code:`$@`, i.e. double dollars are properly escaped.
    - :code:`{outputs[0].name}` represents the name of the first output. The available f-string
      variables are :attr:`outputs` and :attr:`inputs`, each a list of :class:`Artifact`\ s.

    Environment variables are inherited by default, but global environment variables for all
    :class:`Shell` transformations can be specified in :attr:`ENV`, and specific environment
    variables can be specified using the :attr:`env` argument. Environment variables that are not
    "truthy" are removed from the environment of the transformation.

    Args:
        outputs: Artifacts to generate.
        inputs: Artifacts consumed by the transformation.
        cmd: Command to execute.
        env: Mapping of environment variables.
        **kwargs: Keyword arguments passed to :func:`asyncio.subprocess.create_subprocess_shell`.

    Raises:
        TypeError: If the :attr:`cmd` is neither a string nor a sequence of strings.

    Attributes:
        ENV: Default mapping of environment variables for all :class:`Shell` transformations.
    """
    def __init__(self, outputs: typing.Iterable["artifacts.Artifact"],
                 inputs: typing.Iterable["artifacts.Artifact"],
                 cmd: typing.Union[str, typing.Iterable[str]], *, env: dict[str, str] = None,
                 **kwargs) -> None:
        super().__init__(outputs, inputs)
        if isinstance(cmd, typing.Iterable) and not isinstance(cmd, str):
            cmd = " ".join(f"'{x}'" if " " in x else x for x in cmd)
        elif not isinstance(cmd, str):
            raise TypeError(cmd)
        self.cmd = cmd
        self.env = env or {}
        self.kwargs = kwargs

    async def execute(self) -> None:
        # Apply format-string substitution.
        cmd = self.cmd.format(outputs=self.outputs, inputs=self.inputs)
        # Apply Makefile-style substitutions.
        rules = {
            r"@": self.outputs[0],
            r"<": self.inputs[0] if self.inputs else None,
            r"\^": " ".join(input.name for input in self.inputs)
        }
        for key, value in rules.items():
            cmd = re.sub(r"(?<!\$)\$" + key, str(value), cmd)
        # Call the process.
        env = os.environ | self.ENV | self.env
        env = {key: value for key, value in env.items() if value}
        process = await asyncio.subprocess.create_subprocess_shell(cmd, env=env, **self.kwargs)
        status = await process.wait()
        if status:
            raise RuntimeError(f"{self} failed with status code {status}")

    ENV: typing.Mapping[str, str] = {}


class Functional(Transformation):
    """
    Apply a python function.

    Args:
        outputs: Artifacts to generate.
        inputs: Artifacts consumed by the transformation.
        func: Function to execute.
        *args: Positional arguments passed to :attr:`func`.
        *kwargs: Keyword arguments passed to :attr:`func`.
    """
    def __init__(self, outputs: typing.Iterable["artifacts.Artifact"], inputs:
                 typing.Iterable["artifacts.Artifact"], func: typing.Callable, *args, **kwargs) \
            -> None:
        super().__init__(outputs, inputs)
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def execute(self) -> None:
        return self.func(self.outputs, self.inputs, *self.args, **self.kwargs)
