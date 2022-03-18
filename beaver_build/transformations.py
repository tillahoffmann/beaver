import aiohttp
import asyncio
import hashlib
import logging
import os
import re
import sys
import typing
from . import artifacts


LOGGER = logging.getLogger(__name__)


def cancel_all_transformations() -> None:
    """
    Cancel all running transformations.
    """
    try:
        tasks = asyncio.all_tasks()
    except RuntimeError:
        return

    for task in tasks:
        task.cancel()


class Transformation:
    """
    Base class for transformations that generates outputs given inputs. Inheriting classes should
    implement :meth:`execute`.

    Args:
        outputs: Artifacts to generate.
        inputs: Artifacts consumed by the transformation.

    Attributes:
        COMPOSITE_DIGESTS: Mapping of artifact names to composite digests. See
            :func:`evaluate_composite_digests` for details.
        SEMAPHORE: Semaphore to limit the number of concurrent transformations being executed.
        DRY_RUN: Whether to show transformations without executing them.
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
        Evaluate composite digests for all :code:`outputs` of the transformation.

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
            LOGGER.info("\U0001f7e2 artifacts [%s] are up to date",
                        ", ".join(o.name for o in self.outputs))
            return
        LOGGER.debug("composite digests of %s have changed: %s", ", ".join(stale_artifacts),
                     composite_digests)

        if self.DRY_RUN:
            LOGGER.info("\U0001f7e1 artifacts [%s] are stale; transformation not scheduled because "
                        "of dry run", ", ".join(stale_artifacts))
            return

        LOGGER.info("\U0001f7e1 artifacts [%s] are stale; schedule transformation",
                    ", ".join(stale_artifacts))

        try:
            if self.SEMAPHORE is None:
                await self.execute()
            else:
                async with self.SEMAPHORE:
                    await self.execute()

            # Update the composite digests.
            composite_digests = self.evaluate_composite_digests()
            LOGGER.info("\u2705 generated artifacts [%s]", ", ".join(o.name for o in self.outputs))
            self.COMPOSITE_DIGESTS.update(composite_digests)
        except Exception as ex:
            LOGGER.error("\u274c failed to generate artifacts [%s]: %s",
                         ", ".join(o.name for o in self.outputs), ex)
            raise

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

    COMPOSITE_DIGESTS: dict[str, bytes] = {}
    SEMAPHORE: typing.Optional[asyncio.Semaphore] = None
    DRY_RUN: bool = False


class _Sleep(Transformation):
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

    Example:
        >>> bb.Download("20news.tar.gz", url="https://ndownloader.figshare.com/files/5975967",
        ...             digest="8f1b2514ca22a5ade8fbb9cfa5727df95fa587f4c87b786e15c759fa66d95610")
        Download([] -> [File(20news.tar.gz)])
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


class Subprocess(Transformation):
    r"""
    Execute a subprocess.

    The transformation supports variable substitution using f-strings and Makefile syntax:

    - :code:`$@` represents the first output.
    - :code:`$<` represents the first input.
    - :code:`$^` represents all inputs.
    - :code:`$$@` represents the literal :code:`$@`, i.e. double dollars are properly escaped.
    - :code:`{outputs[0].name}` represents the name of the first output. The available f-string
      variables are :code:`outputs` and :code:`inputs`, each a list of
      :class:`Artifact <.artifacts.Artifact>`\ s.
    - :code:`$!` represents the current python interpreter.

    Environment variables are inherited by default, but global environment variables for all
    :class:`Subprocess` transformations can be specified in :attr:`ENV`, and specific environment
    variables can be specified using the :code:`env` argument. Environment variables that are
    :code:`None` are removed from the environment of the transformation.

    Args:
        outputs: Artifacts to generate.
        inputs: Artifacts consumed by the transformation.
        cmd: Command to execute. Must be a single string if :code:`shell` is truthy and a sequence
            of strings otherwise.
        env: Mapping of environment variables.
        shell: Whether to execute the command through the shell, e.g. to expand the home directory
            :code:`~` or pipe information between processes or files using :code:`|`, :code:`<`, or
            :code:`>`. See :class:`subprocess.Popen` for details, including security considerations.
        **kwargs: Keyword arguments passed to :func:`asyncio.subprocess.create_subprocess_shell` (if
            :code:`shell == True`) or :func:`asyncio.subprocess.create_subprocess_exec` (if
            :code:`shell == False`).

    Raises:
        ValueError: If :code:`shell == True` and :code:`cmd` is not a string, or
            :code:`shell == False` and :code:`cmd` is not a list of strings.

    Attributes:
        ENV: Default mapping of environment variables for all :class:`Subprocess` transformations.

    Example:
        >>> bb.Subprocess("copy.txt", "input.txt", ["cp", "$<", "$@"])
        Subprocess([File(input.txt)] -> [File(copy.txt)])
    """
    def __init__(self, outputs: typing.Iterable["artifacts.Artifact"],
                 inputs: typing.Iterable["artifacts.Artifact"],
                 cmd: typing.Union[str, typing.Iterable[str]], *, env: dict[str, str] = None,
                 shell: bool = False, **kwargs) -> None:
        super().__init__(outputs, inputs)
        if shell and not isinstance(cmd, str):
            raise ValueError(f"`cmd` must be a string if `shell == True` but got {cmd}")
        if not shell and not isinstance(cmd, list):
            raise ValueError(f"`cmd` must be a list of strings if `shell == False` but got {cmd}")
        self.cmd = cmd
        self.shell = shell
        self.env = env or {}
        self.kwargs = kwargs

    def _apply_substitutions(self, part: str) -> str:
        part = part.format(outputs=self.outputs, inputs=self.inputs)
        # Apply Makefile-style and python interpreter substitutions.
        rules = {
            r"@": self.outputs[0],
            r"<": self.inputs[0] if self.inputs else None,
            r"\^": " ".join(input.name for input in self.inputs),
            r"!": sys.executable,
        }
        for key, value in rules.items():
            part = re.sub(r"(?<!\$)\$" + key, str(value), part)
        return part

    async def execute(self) -> None:
        # Prepare the command.
        if self.shell:
            cmd = self._apply_substitutions(self.cmd)
        else:
            cmd = [self._apply_substitutions(str(part)) for part in self.cmd]
        LOGGER.info("\u2699\ufe0f execute %s command `%s`", "shell" if self.shell else "subprocess",
                    cmd)
        # Call the process.
        env = os.environ | self.ENV | self.env
        env = {key: str(value) for key, value in env.items() if value is not None}
        if self.shell:
            process = await asyncio.subprocess.create_subprocess_shell(cmd, env=env, **self.kwargs)
        else:
            process = await asyncio.subprocess.create_subprocess_exec(*cmd, env=env, **self.kwargs)
        status = await process.wait()
        if status:
            raise RuntimeError(f"{self} failed with status code {status}")

    ENV: dict[str, str] = {}


class Shell(Subprocess):
    """
    Execute a command in the shell. See :class:`Subprocess` for details.

    Example:
    >>> Shell("output.txt", None, "echo hello > output.txt")
    Shell([] -> [File(output.txt)])
    """
    def __init__(self, outputs: typing.Iterable["artifacts.Artifact"],
                 inputs: typing.Iterable["artifacts.Artifact"],
                 cmd: str, *, env: dict[str, str] = None, **kwargs) -> None:
        super().__init__(outputs, inputs, cmd, env=env, shell=True, **kwargs)


class Functional(Transformation):
    """
    Apply a python function.

    Args:
        outputs: Artifacts to generate.
        inputs: Artifacts consumed by the transformation.
        func: Function to execute.
        *args: Positional arguments passed to :code:`func`.
        *kwargs: Keyword arguments passed to :code:`func`.
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
