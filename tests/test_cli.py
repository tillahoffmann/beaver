import beaver_build as bb
from beaver_build import cli
import logging
import os
import pytest
import re
import subprocess


TEST_BEAVER_FILE = os.path.join(os.path.dirname(__file__), "beaver.py")


def test_build(tempdir, caplog: pytest.LogCaptureFixture):
    args = [f"--file={TEST_BEAVER_FILE}", "--log_level=debug", "build", "output.txt"]
    with caplog.at_level(logging.INFO):
        cli.__main__(args)
    assert bb.Artifact.REGISTRY["output.txt"].digest == "60cdcd6d"

    # Verify that two transforms were executed by inspecting the logs.
    assert "artifacts [File(pre/input1.txt), File(pre/input2.txt)] are stale" in caplog.text
    assert "generated artifacts [File(pre/input1.txt), File(pre/input2.txt)]" in caplog.text
    assert "artifacts [File(output.txt)] are stale" in caplog.text
    assert "generated artifacts [File(output.txt)]" in caplog.text

    # Run again and verify that no transforms were executed.
    bb.reset()
    caplog.clear()
    with caplog.at_level(logging.DEBUG):
        cli.__main__(args)

    assert "artifacts [File(pre/input1.txt), File(pre/input2.txt)] are up to date" \
        in caplog.text
    assert "artifacts [File(output.txt)] are up to date" in caplog.text


def test_missing_cache_file(tempdir, caplog: pytest.LogCaptureFixture):
    assert cli.__main__(["--cache=missing-file", "build", "some-target"])
    assert "cannot be loaded from" in caplog.text


@pytest.mark.parametrize("stale", [False, True])
@pytest.mark.parametrize("raw", [False, True])
@pytest.mark.parametrize("run", [False, True])
@pytest.mark.parametrize("pattern", ["--all", "out"])
def test_list(stale: bool, raw: bool, run: bool, pattern: str, tempdir: str,
              capsys: pytest.CaptureFixture):
    args = [f"--file={TEST_BEAVER_FILE}"]
    if run:
        cli.__main__(args + ["build", "pre/input1.txt"])
        bb.reset()
    args.append("list")
    if stale:
        args.append("--stale")
    if raw:
        args.append("--raw")
    args.append(pattern)
    cli.__main__(args)

    expected = ["output.txt"]
    if not (stale and run):
        expected.extend(["pre/input1.txt", "pre/input2.txt"])
    expected = [name for name in expected if pattern is None or re.match(pattern, name)]

    stdout, _ = capsys.readouterr()

    for name in expected:
        assert name in stdout
    assert stdout


@pytest.mark.parametrize("pattern", ["--all", "out"])
def test_reset(tempdir, caplog: pytest.LogCaptureFixture, pattern: str):
    args = [f"--file={TEST_BEAVER_FILE}"]

    # Ensure that there aren't any composite digests to start with.
    cli.__main__(args + ["reset", pattern])
    assert "artifact `output.txt` did not have a composite digest" in caplog.text
    bb.reset()

    # Build everything and then check that the reset did something.
    cli.__main__(args + ["build", "--all"])
    bb.reset()
    caplog.clear()

    cli.__main__(args + ["reset", pattern])
    assert f"reset {3 if pattern == '--all' else 1} composite digests" in caplog.text


def test_no_artifact(tempdir, caplog: pytest.LogCaptureFixture):
    cli.__main__([f"--file={TEST_BEAVER_FILE}", "list"])
    assert "patterns did not match any artifacts" in caplog.text


def test_entrypoint():
    subprocess.check_call(["beaver", "-h"])
