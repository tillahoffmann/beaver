import asyncio
from beaver import artifacts as ba
import pytest


def test_raise_if_invalid_artifact_type():
    with pytest.raises(TypeError):
        ba.normalize_artifacts(1)


def test_raise_if_implicit_non_file_artifact():
    ba.Artifact("non-file")
    with pytest.raises(ValueError):
        ba.normalize_artifacts("non-file")


def test_file_glob(tempdir):
    for filename in ["file1.txt", "file2.txt", "other.txt"]:
        with open(filename, "w") as fp:
            fp.write(filename)
    artifacts = ba.File.glob("file*.txt")
    assert len(artifacts) == 2


def test_raise_if_duplicate_artifact():
    ba.Artifact("dummy")
    with pytest.raises(ValueError):
        ba.Artifact("dummy")


def test_raise_if_missing_input_file():
    input = ba.File("missing.txt")
    with pytest.raises(FileNotFoundError) as exinfo:
        asyncio.run(input())
    assert str(exinfo.value).startswith(f'{input} does not exist')


def test_warn_if_whitespace(caplog: pytest.LogCaptureFixture):
    ba.File("file with whitespace.txt")
    assert caplog.text.strip().endswith("expect the unexpected")


def test_raise_if_invalid_gather():
    with pytest.raises(TypeError):
        asyncio.run(ba.gather_artifacts(None))
