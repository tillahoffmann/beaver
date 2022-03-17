import beaver_build
import beaver_build.cli
import logging
import os
import pytest
import subprocess


def test_cli(tempdir, caplog: pytest.LogCaptureFixture):
    filename = os.path.join(os.path.dirname(__file__), "beaver.py")
    with beaver_build.working_directory(tempdir):
        args = [f"--file={filename}", "output.txt"]
        with caplog.at_level(logging.INFO):
            beaver_build.cli.__main__(args)
        assert beaver_build.Artifact.REGISTRY["output.txt"].digest.hex() \
            == "f1caa5abc8bbd5cf09eb2785aad9b9b43ba905caa5937a9f98fade98fcee954b"

        beaver_build.Artifact.REGISTRY.clear()
        beaver_build.Transformation.COMPOSITE_DIGESTS.clear()

        # Verify that two transformations were executed by inspecting the logs.
        assert "artifacts [input1.txt, input2.txt] are stale" in caplog.text
        assert "generated artifacts [input1.txt, input2.txt]" in caplog.text
        assert "artifacts [output.txt] are stale" in caplog.text
        assert "generated artifacts [output.txt]" in caplog.text

        # Run again and verify that no transformations were executed.
        caplog.clear()
        with caplog.at_level(logging.INFO):
            beaver_build.cli.__main__(args)

        assert "artifacts [input1.txt, input2.txt] are up to date" in caplog.text
        assert "artifacts [output.txt] are up to date" in caplog.text


def test_missing_beaver_file(tempdir, caplog: pytest.LogCaptureFixture):
    assert beaver_build.cli.__main__(["--digest=missing-file", "some-target"])
    assert 'cannot be loaded from' in caplog.text


def test_entrypoint():
    subprocess.check_call(["beaver", "-h"])
