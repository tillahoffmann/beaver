import beaver_build
import beaver_build.cli
import logging
import os
import pytest
import re
import subprocess


def test_cli(tempdir, caplog: pytest.LogCaptureFixture):
    filename = os.path.join(os.path.dirname(__file__), "beaver.py")
    with beaver_build.working_directory(tempdir):
        args = [f"--file={filename}", "build", "output.txt"]
        with caplog.at_level(logging.INFO):
            beaver_build.cli.__main__(args)
        assert beaver_build.Artifact.REGISTRY["output.txt"].digest == "60cdcd6d"

        beaver_build.Artifact.REGISTRY.clear()
        beaver_build.Transformation.COMPOSITE_DIGESTS.clear()

        # Verify that two transformations were executed by inspecting the logs.
        assert "artifacts [File(pre/input1.txt), File(pre/input2.txt)] are stale" in caplog.text
        assert "generated artifacts [File(pre/input1.txt), File(pre/input2.txt)]" in caplog.text
        assert "artifacts [File(output.txt)] are stale" in caplog.text
        assert "generated artifacts [File(output.txt)]" in caplog.text

        # Run again and verify that no transformations were executed.
        caplog.clear()
        with caplog.at_level(logging.INFO):
            beaver_build.cli.__main__(args)

        assert "artifacts [File(pre/input1.txt), File(pre/input2.txt)] are up to date" \
            in caplog.text
        assert "artifacts [File(output.txt)] are up to date" in caplog.text


def test_missing_beaver_file(tempdir, caplog: pytest.LogCaptureFixture):
    assert beaver_build.cli.__main__(["--digest=missing-file", "build", "some-target"])
    assert "cannot be loaded from" in caplog.text


@pytest.mark.parametrize("stale", [False, True])
@pytest.mark.parametrize("raw", [False, True])
@pytest.mark.parametrize("run", [False, True])
@pytest.mark.parametrize("pattern", [None, "out"])
def test_list(stale: bool, raw: bool, run: bool, pattern: str, tempdir: str,
              capsys: pytest.CaptureFixture):
    filename = os.path.join(os.path.dirname(__file__), "beaver.py")
    with beaver_build.working_directory(tempdir):
        args = [f"--file={filename}"]
        if run:
            beaver_build.cli.__main__(args + ["build", "pre/input1.txt"])
            beaver_build.reset()
        args.append("list")
        if stale:
            args.append("--stale")
        if raw:
            args.append("--raw")
        if pattern:
            args.append(pattern)
        beaver_build.cli.__main__(args)

    expected = ["output.txt"]
    if not (stale and run):
        expected.extend(["pre/input1.txt", "pre/input2.txt"])
    expected = [name for name in expected if pattern is None or re.match(pattern, name)]

    stdout, _ = capsys.readouterr()

    for name in expected:
        assert name in stdout
    assert stdout


def test_entrypoint():
    subprocess.check_call(["beaver", "-h"])
