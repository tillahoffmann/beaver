import asyncio
import pytest
import beaver
import tempfile
import time
from unittest import mock


@pytest.fixture(autouse=True)
def clean_slate():
    beaver.Artifact.REGISTRY.clear()
    beaver.Transformation.COMPOSITE_DIGESTS.clear()


@pytest.fixture
def tempdir():
    with tempfile.TemporaryDirectory() as tmp, beaver.working_directory(tmp):
        yield tmp


def test_execution_time(tempdir):
    beaver.Sleep('input.txt', [], time=.1)
    beaver.Sleep('intermediate_0.txt', 'input.txt', time=.2)
    [beaver.Sleep(f"intermediate_1_{i}.txt", 'intermediate_0.txt',
                  time=(i + 1) / 5) for i in range(3)]
    output0, = beaver.Sleep(
        'output_0.txt', [f"intermediate_1_{i}.txt" for i in range(3)], time=.5)
    output1, = beaver.Sleep(
        'output_1.txt', ["intermediate_1_0.txt", "intermediate_0.txt"], time=.75)
    start = time.time()
    asyncio.run(beaver.gather_artifacts(output0, output1))
    duration = time.time() - start
    assert abs(duration - 1.4) < 0.1


def test_duplicate_transformation():
    artifact = beaver.Artifact('dummy')
    beaver.Transformation(artifact, [])
    with pytest.raises(RuntimeError):
        beaver.Transformation(artifact, [])


def test_duplicate_artifact():
    beaver.Artifact('dummy')
    with pytest.raises(RuntimeError):
        beaver.Artifact('dummy')


def test_shell_command(tempdir):
    output, = beaver.Shell("output.txt", None, "echo hello > output.txt".split())
    asyncio.run(output())
    assert output.digest.hex() == '5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03'


# See https://stackoverflow.com/a/59351425/1150961 for details.
class AsyncMockResponse:
    def __init__(self, content: bytes):
        self._content = content
        self.num_reads = 0

    async def read(self) -> bytes:
        self.num_reads += 1
        return self._content

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


def test_download(tempdir):
    mock_response = AsyncMockResponse(b"hello world")
    with mock.patch('aiohttp.ClientSession.get', return_value=mock_response):
        output, = beaver.Download(
            "output.txt", "invalid-url",
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        )
        asyncio.run(output())
        assert mock_response.num_reads == 1


def test_download_wrong_file(tempdir):
    mock_response = AsyncMockResponse(b"bye world")
    with mock.patch('aiohttp.ClientSession.get', return_value=mock_response):
        output, = beaver.Download(
            "output.txt", "invalid-url",
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        )
        with pytest.raises(ValueError) as exinfo:
            asyncio.run(output())
        assert mock_response.num_reads == 1
        assert str(exinfo.value).startswith("expected digest")


def test_download_exists(tempdir):
    with open("output.txt", "wb") as fp:
        fp.write(b"hello world")
    output, = beaver.Download(
        "output.txt", "invalid-url",
        "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    )
    asyncio.run(output())


def test_missing_output_file(tempdir):
    async def target(*args):
        pass

    output, = beaver.Functional("output.txt", None, target)
    with pytest.raises(FileNotFoundError) as exinfo:
        asyncio.run(output())
    assert str(exinfo.value).startswith(f'{output.parent} did not generate')


def test_missing_input_file(tempdir):
    input = beaver.File("missing.txt")
    with pytest.raises(FileNotFoundError) as exinfo:
        asyncio.run(input())
    assert str(exinfo.value).startswith(f'{input} does not exist')


def test_invalid_artifact_type():
    with pytest.raises(TypeError):
        beaver.normalize_artifacts(1)


def test_implicit_non_file_artifact():
    beaver.Artifact('non-file')
    with pytest.raises(ValueError):
        beaver.normalize_artifacts('non-file')


def test_file_glob(tempdir):
    transform = beaver.Sleep(["file1.txt", "file2.txt", "other.txt"], None, time=0)
    asyncio.run(beaver.gather_artifacts(*transform.outputs))
    artifacts = beaver.File.glob("file*.txt")
    assert len(artifacts) == 2


def test_non_none_return():
    async def target(*args):
        return 1

    output, = beaver.Functional("output.txt", None, target)
    with pytest.raises(ValueError):
        asyncio.run(output())


def test_input_none_digest():
    target = mock.AsyncMock(return_value=None)
    output, = beaver.Functional(beaver.Artifact("output"), beaver.Artifact("input"), target)
    asyncio.run(output())
    target.assert_called_once()


def test_caching(tempdir):
    calls = []

    async def target(outputs, *args):
        for file in outputs:
            with open(file.name, 'w') as fp:
                fp.write(file.name)
        calls.append(None)

    transform = beaver.Functional("output.txt", None, target)
    for _ in range(3):
        asyncio.run(beaver.gather_artifacts(*transform.outputs))
        assert len(calls) == 1


def test_invalid_shell_cmd():
    with pytest.raises(TypeError):
        beaver.Shell(None, None, 1)


def test_shell_error():
    with pytest.raises(RuntimeError):
        output, = beaver.Shell("output.txt", None, "not-a-command")
        asyncio.run(output())


def test_funny_coverage():
    asyncio.run(beaver.funny_coverage(1, 1))
    asyncio.run(beaver.funny_coverage(1, 2))
