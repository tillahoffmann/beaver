import aiohttp
import asyncio
import logging
import os
import re
import shlex
import sys
import time
import typing
from . import artifacts
from . import util


LOGGER = logging.getLogger(__name__)


def cancel_all_transforms() -> None:
    """
    Cancel all running transforms.
    """
    try:
        tasks = asyncio.all_tasks()
    except RuntimeError:
        return

    for task in tasks:
        task.cancel()


class Transform:
    """
    Base class for transforms that generates outputs given inputs. Inheriting classes should
    implement :meth:`execute`.

    Args:
        outputs: Artifacts to generate.
        inputs: Artifacts consumed by the transform.

    Attributes:
        stale_outputs: Sequence of outputs that are stale and need to be updated.
        COMPOSITE_DIGESTS: Mapping of artifact names to composite digests. See
            :func:`evaluate_composite_digests` for details.
        SEMAPHORE: Semaphore to limit the number of concurrent transforms being executed.
        DRY_RUN: Whether to show transforms without executing them.
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
        Evaluate composite digests for all :code:`outputs` of the transform.

        A composite digest is defined as the digest of the digests of inputs and the digest of the
        output, i.e. it is a joint, concise summary of both inputs and outputs. If any composite
        digest differs from the digest in :attr:`COMPOSITE_DIGESTS` or is :code:`None`, the
        transform needs to be executed. A composite digest is :code:`None` if the corresponding
        output digest is :code:`None` or any input digests are :code:`None`.

        Returns:
            digests: Mapping from output names to composite digests.
        """
        # Initialise empty digests for the outputs.
        composite_digests = {o.name: None for o in self.outputs}

        # Evaluate input digests and abort if any of them are missing.
        input_digest = util.Crc32()
        for i in self.inputs:
            if i.digest is None:
                return composite_digests
            input_digest.update(bytes.fromhex(i.digest))

        # Construct composite digests for the outputs.
        for output in self.outputs:
            if output.digest is None:
                continue
            digest = util.Crc32(bytes.fromhex(output.digest), int(input_digest)).hexdigest()
            composite_digests[output.name] = digest
        return composite_digests

    async def __call__(self) -> None:
        # Wait for all inputs artifacts.
        await asyncio.gather(*self.inputs)

        async with self.SEMAPHORE or util.noop_context():
            # Figure out which outputs are stale.
            stale_artifacts = self.stale_outputs
            if not stale_artifacts:
                LOGGER.info("\U0001f7e2 artifacts %s are up to date", self.outputs)
                return

            if self.DRY_RUN:
                LOGGER.info("\U0001f7e1 artifacts %s are stale; dry run", stale_artifacts)
                return

            LOGGER.info("\U0001f7e1 artifacts %s are stale; running transform", stale_artifacts)

            # If the transform is already in progress, just wait for it and exit.
            if not self.future:
                self.future = asyncio.Future()
                try:
                    await self.execute()

                    # Update the composite digests.
                    composite_digests = self.evaluate_composite_digests()
                    LOGGER.info("\u2705 generated artifacts %s", self.outputs)
                    self.COMPOSITE_DIGESTS.update(composite_digests)
                    self.future.set_result(None)
                except Exception as ex:
                    LOGGER.error("\u274c failed to generate artifacts %s: %s", self.outputs, ex)
                    self.future.set_exception(ex)

            await self.future

    @property
    def stale_outputs(self) -> typing.Iterable["artifacts.Artifact"]:
        # Get the outputs whose composite digests are `None` or different from the library of
        # composite digests.
        composite_digests = self.evaluate_composite_digests()
        return [output for output in self.outputs if composite_digests[output.name] is None or
                composite_digests[output.name] != self.COMPOSITE_DIGESTS.get(output.name)]

    async def execute(self) -> None:
        """
        Execute the transform.
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


class _Sleep(Transform):
    """
    Sleeps for a given number of seconds and create any file artifact outputs. Mostly used for
    testing and debugging.

    Args:
        outputs: Artifacts to generate.
        inputs: Artifacts consumed by the transform.
        time: Number of seconds to sleep for.
    """
    def __init__(self, outputs: typing.Iterable["artifacts.Artifact"],
                 inputs: typing.Iterable["artifacts.Artifact"], *, sleep: float) -> None:
        super().__init__(outputs, inputs)
        self.sleep = sleep
        self.start = None
        self.end = None

    async def execute(self) -> None:
        self.start = time.time()
        LOGGER.debug("running %s for %f seconds...", self, self.sleep)
        await asyncio.sleep(self.sleep)
        for output in self.outputs:
            if isinstance(output, artifacts.File):
                with open(output.name, "w") as fp:
                    fp.write(output.name)
                LOGGER.debug("created %s", output)
        LOGGER.debug("completed %s", self)
        self.end = time.time()


class Download(Transform):
    """
    Download a file.

    Args:
        output: Output artifact for the downloaded data.
        url: Url to download from.

    Example:
        >>> data = bb.File("20news.tar.gz", expected_digest="af604312")
        >>> bb.Download(data, url="https://ndownloader.figshare.com/files/5975967")
        Download([] -> [File(20news.tar.gz)])
    """
    def __init__(self, output: "artifacts.File", url: str) -> None:
        super().__init__(output, [])
        self.url = url

    async def execute(self) -> None:
        # Abort if we already have the right file.
        # TODO: This should really rely on the digest of the URL rather than the artifact.
        output, = self.outputs
        if output.expected_digest and output.digest == output.expected_digest:
            return
        # Download the file.
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url) as response:
                with open(output.name, "wb") as fp:
                    fp.write(await response.read())


class Subprocess(Transform):
    r"""
    Execute a subprocess.

    The transform supports variable substitution using f-strings and Makefile syntax:

    - :code:`$@` represents the first output.
    - :code:`$<` represents the first input.
    - :code:`$^` represents all inputs.
    - :code:`$$@` represents the literal :code:`$@`, i.e. double dollars are properly escaped.
    - :code:`{outputs[0].name}` represents the name of the first output. The available f-string
      variables are :code:`outputs` and :code:`inputs`, each a list of
      :class:`Artifact <.artifacts.Artifact>`\ s.
    - :code:`$!` represents the current python interpreter.

    Environment variables are inherited by default, but global environment variables for all
    :class:`Subprocess` transforms can be specified in :attr:`ENV`, and specific environment
    variables can be specified using the :code:`env` argument. Environment variables that are
    :code:`None` are removed from the environment of the transform.

    Args:
        outputs: Artifacts to generate.
        inputs: Artifacts consumed by the transform.
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
        ENV: Default mapping of environment variables for all :class:`Subprocess` transforms.

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
            pretty_cmd = cmd = self._apply_substitutions(self.cmd)
        else:
            cmd = [self._apply_substitutions(str(part)) for part in self.cmd]
            pretty_cmd = " ".join(map(shlex.quote, cmd))
        LOGGER.info("\u2699\ufe0f execute %s command `%s`", "shell" if self.shell else "subprocess",
                    pretty_cmd)
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


class Functional(Transform):
    """
    Apply a python function.

    Args:
        outputs: Artifacts to generate.
        inputs: Artifacts consumed by the transform.
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