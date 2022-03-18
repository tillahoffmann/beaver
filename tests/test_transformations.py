import asyncio
from beaver_build import artifacts as ba
from beaver_build import transformations as bt
import os
import pytest
import time
from unittest import mock


@pytest.mark.timing
def test_execution_time(tempdir):
    bt._Sleep("input.txt", [], sleep=.1)
    bt._Sleep("intermediate_0.txt", "input.txt", sleep=.2)
    [bt._Sleep(f"intermediate_1_{i}.txt", "intermediate_0.txt",
               sleep=(i + 1) / 5) for i in range(3)]
    output0, = bt._Sleep(
        "output_0.txt", [f"intermediate_1_{i}.txt" for i in range(3)], sleep=.5)
    output1, = bt._Sleep(
        "output_1.txt", ["intermediate_1_0.txt", "intermediate_0.txt"], sleep=.75)
    start = time.time()
    asyncio.run(ba.gather_artifacts(output0, output1))
    duration = time.time() - start
    assert abs(duration - 1.4) < 0.1


def test_raise_if_multiple_parents():
    artifact = ba.Artifact("dummy")
    bt.Transformation(artifact, [])
    with pytest.raises(RuntimeError):
        bt.Transformation(artifact, [])


def test_shell_command(tempdir):
    output, = bt.Shell("directory/output.txt", None, "echo hello > $@")
    asyncio.run(output())
    assert output.digest == "363a3020"


# See https://stackoverflow.com/a/59351425/1150961 for details.
class AsyncMockResponse:
    def __init__(self, content: bytes):
        self.read = mock.AsyncMock(return_value=content)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


def test_download(tempdir):
    mock_response = AsyncMockResponse(b"hello world")
    with mock.patch("aiohttp.ClientSession.get", return_value=mock_response):
        output = ba.File("directory/output.txt", "0d4a1185")
        bt.Download(output, "invalid-url")
        asyncio.run(output())
        mock_response.read.assert_called_once()


def test_raise_if_download_wrong_file(tempdir):
    mock_response = AsyncMockResponse(b"bye world")
    with mock.patch("aiohttp.ClientSession.get", return_value=mock_response):
        output = ba.File("directory/output.txt", "0d4a1185")
        bt.Download(output, "invalid-url")
        with pytest.raises(ValueError) as exinfo:
            asyncio.run(output())
        assert str(exinfo.value).startswith("expected digest")
        mock_response.read.assert_called_once()


def test_download_exists(tempdir):
    with open("output.txt", "wb") as fp:
        fp.write(b"hello world")
    output = ba.File("output.txt", "0d4a1185")
    bt.Download(output, "invalid-url")
    asyncio.run(output())


def test_raise_if_missing_output_file(tempdir):
    async def target(*args):
        pass

    output, = bt.Functional("output.txt", None, target)
    with pytest.raises(FileNotFoundError) as exinfo:
        asyncio.run(output())
    assert str(exinfo.value).startswith(f"{output.parent} did not generate")


def test_input_none_digest():
    target = mock.AsyncMock(return_value=None)
    output, = bt.Functional(ba.Artifact("output"), ba.Artifact("input"), target)
    asyncio.run(output())
    target.assert_called_once()


def test_caching(tempdir):
    calls = []

    async def target(outputs, *args):
        for file in outputs:
            with open(file.name, "w") as fp:
                fp.write(file.name)
        calls.append(None)

    transform = bt.Functional("output.txt", None, target)
    for _ in range(3):
        asyncio.run(ba.gather_artifacts(*transform.outputs))
        assert len(calls) == 1


@pytest.mark.parametrize('shell', [False, True])
def test_raise_if_invalid_subprocess_cmd(shell):
    with pytest.raises(ValueError):
        bt.Subprocess(None, None, 1, shell=shell)


def test_raise_if_shell_error():
    with pytest.raises(RuntimeError):
        output, = bt.Shell("output.txt", None, "not-a-command")
        asyncio.run(output())


@pytest.mark.timing
@pytest.mark.parametrize("use_semaphore", [False, True])
def test_concurrency_with_semaphore(use_semaphore: bool):
    outputs = [output for i in range(9) for output
               in bt._Sleep(ba.Artifact(f"{i}"), None, sleep=.1)]
    start = time.time()
    asyncio.run(ba.gather_artifacts(*outputs, num_concurrent=3 if use_semaphore else None))
    actual_duration = time.time() - start
    expected_duration = .3 if use_semaphore else .1
    assert abs(actual_duration - expected_duration) < .1


@pytest.mark.parametrize("cmd, expected", [
    ("transform $< $@", "transform input1.txt output1.txt"),
    ("transform $^ $@", "transform input1.txt input2.txt output1.txt"),
    ("transform {inputs[1]} {outputs[0].name}", "transform input2.txt output1.txt")
])
def test_shell_substitution(cmd, expected, tempdir):
    # Create dummy files.
    for filename in ["input1.txt", "input2.txt", "output1.txt", "output2.txt"]:
        with open(filename, "w") as fp:
            fp.write(filename)

    # Mock the execution of the subprocess.
    create_subprocess_shell = mock.AsyncMock()
    asyncio.run(create_subprocess_shell()).wait = mock.AsyncMock(return_value=0)
    create_subprocess_shell.reset_mock()
    with mock.patch("asyncio.subprocess.create_subprocess_shell", create_subprocess_shell):
        transform = bt.Shell(["output1.txt", "output2.txt"], ["input1.txt", "input2.txt"], cmd)
        asyncio.run(ba.gather_artifacts(transform))
        create_subprocess_shell.assert_called_once()
        assert create_subprocess_shell.call_args[0][0] == expected


@pytest.mark.parametrize("ENV, env", [
    ({}, {"BEAVER_TEST_VARIABLE": "FOO"}),
    ({"BEAVER_TEST_VARIABLE": "FOO"}, {}),
    ({"BEAVER_TEST_VARIABLE": "FOO"}, {"BEAVER_TEST_VARIABLE": "BAR"}),
    ({"BEAVER_TEST_VARIABLE": "FOO"}, {"BEAVER_TEST_VARIABLE": None}),
    ({}, {}),
])
def test_shell_environment_variables(ENV, env, tempdir):
    bt.Shell.ENV = ENV
    os.environ["BEAVER_TEST_VARIABLE"] = "BAZ"
    output, = bt.Shell("output.txt", None, "echo $BEAVER_TEST_VARIABLE > $@", env=env)
    asyncio.run(ba.gather_artifacts(output))
    with open("output.txt") as fp:
        actual = fp.read().strip() or None
    expected = (os.environ | ENV | env)["BEAVER_TEST_VARIABLE"]
    assert actual == expected


@pytest.mark.parametrize("dry_run", [False, True])
def test_dry_run(dry_run):
    transformation = bt.Transformation("dummy.txt", None)
    bt.Transformation.DRY_RUN = dry_run

    if dry_run:
        asyncio.run(ba.gather_artifacts(transformation))
    else:
        with pytest.raises(NotImplementedError):
            asyncio.run(ba.gather_artifacts(transformation))


def test_cancel_long_running_transformation():
    async def target():
        transformation = bt.Shell(ba.Artifact("long-running"), None,
                                  "for x in `seq 60`; do sleep 1 && echo $x; done")
        task = asyncio.create_task(ba.gather_artifacts(transformation))
        bt.cancel_all_transformations()
        with pytest.raises(asyncio.CancelledError):
            await task
    asyncio.run(target())


def test_subprocess(tempdir):
    dummy, = bt.Subprocess("dummy.txt", None, ["sh", "-c", "echo hello > dummy.txt"])
    asyncio.run(ba.gather_artifacts(dummy))
    assert dummy.digest == "363a3020"


def test_subprocess_env(tempdir):
    dummy, = bt.Subprocess("dummy.txt", None, ["sh", "-c", "echo $MYVAR > dummy.txt"],
                           env={"MYVAR": 0.0})
    asyncio.run(ba.gather_artifacts(dummy))
    assert dummy.read().strip() == "0.0"
