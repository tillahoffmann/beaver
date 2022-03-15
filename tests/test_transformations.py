import asyncio
from beaver import artifacts as ba
from beaver import transformations as bt
import os
import pytest
import time
from unittest import mock


def test_execution_time(tempdir):
    bt.Sleep("input.txt", [], time=.1)
    bt.Sleep("intermediate_0.txt", "input.txt", time=.2)
    [bt.Sleep(f"intermediate_1_{i}.txt", "intermediate_0.txt",
              time=(i + 1) / 5) for i in range(3)]
    output0, = bt.Sleep(
        "output_0.txt", [f"intermediate_1_{i}.txt" for i in range(3)], time=.5)
    output1, = bt.Sleep(
        "output_1.txt", ["intermediate_1_0.txt", "intermediate_0.txt"], time=.75)
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
    output, = bt.Shell("directory/output.txt", None, "echo hello > $@".split())
    asyncio.run(output())
    assert output.digest.hex() == "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03"


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
        output, = bt.Download(
            "directory/output.txt", "invalid-url",
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        )
        asyncio.run(output())
        mock_response.read.assert_called_once()


def test_raise_if_download_wrong_file(tempdir):
    mock_response = AsyncMockResponse(b"bye world")
    with mock.patch("aiohttp.ClientSession.get", return_value=mock_response):
        output, = bt.Download(
            "output.txt", "invalid-url",
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        )
        with pytest.raises(ValueError) as exinfo:
            asyncio.run(output())
        assert str(exinfo.value).startswith("expected digest")
        mock_response.read.assert_called_once()


def test_download_exists(tempdir):
    with open("output.txt", "wb") as fp:
        fp.write(b"hello world")
    output, = bt.Download(
        "output.txt", "invalid-url",
        "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    )
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


def test_raise_if_invalid_shell_cmd():
    with pytest.raises(TypeError):
        bt.Shell(None, None, 1)


def test_raise_if_shell_error():
    with pytest.raises(RuntimeError):
        output, = bt.Shell("output.txt", None, "not-a-command")
        asyncio.run(output())


@pytest.mark.parametrize("use_semaphore", [False, True])
def test_concurrency_with_semaphore(use_semaphore):
    outputs = [output for i in range(9) for output in bt.Sleep(ba.Artifact(f"{i}"), None, time=.1)]
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
