import asyncio
from beaver_build import artifacts as ba
from beaver_build import transformations as bt
import os
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


def test_raise_if_duplicate_name_with_different_type():
    ba.Artifact("dummy")
    with pytest.raises(ValueError):
        ba.File("dummy")


def test_raise_if_missing_input_file():
    input = ba.File("missing.txt")
    with pytest.raises(FileNotFoundError) as exinfo:
        asyncio.run(input())
    assert str(exinfo.value).startswith(f"{input} does not exist")


def test_warn_if_whitespace(caplog: pytest.LogCaptureFixture):
    ba.File("file with whitespace.txt")
    assert caplog.text.strip().endswith("expect the unexpected")


def test_raise_if_invalid_gather():
    with pytest.raises(TypeError):
        asyncio.run(ba.gather_artifacts(None))


def test_normalize_transformation_outputs():
    transformation = bt.Transformation(["output1.txt", "output2.txt"], None)
    assert ba.normalize_artifacts(transformation) == transformation.outputs
    assert ba.normalize_artifacts([transformation]) == transformation.outputs


def test_group(tempdir):
    with ba.group_artifacts("outer") as outer, ba.group_artifacts("inner") as inner:
        artifact, = bt.Shell("artifact.txt", None, "echo hello > $@")

    assert outer.name == "outer"
    assert inner.name == "outer/inner"
    assert artifact.name == "outer/inner/artifact.txt"

    assert not os.path.isfile(artifact.name)
    asyncio.run(ba.gather_artifacts(outer))
    assert os.path.isfile(artifact.name)


def test_group_reuse():
    with ba.group_artifacts("group") as group:
        pass
    with ba.group_artifacts("group") as other:
        pass
    assert other is group


def test_nested_groups():
    with ba.group_artifacts("outer", "inner") as groups:
        artifact = ba.Artifact("artifact")
    assert artifact.name == "outer/inner/artifact"
    assert len(groups) == 2


def test_group_name_conflict():
    ba.Artifact("group")
    with pytest.raises(ValueError), ba.group_artifacts("group"):
        pass


@pytest.mark.parametrize("groups", [["1"], ["1", "2"]])
@pytest.mark.parametrize("squeeze", [False, True])
def test_nested_group_squeeze(groups, squeeze):
    with ba.group_artifacts(*groups, squeeze=squeeze) as result:
        pass

    if len(groups) == 1 and squeeze:
        assert isinstance(result, ba.Group)
    else:
        assert isinstance(result, list)
