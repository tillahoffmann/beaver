import beaver_build
import logging
import pytest
import tempfile


logging.raiseExceptions = False


@pytest.fixture(autouse=True)
def clean_slate():
    beaver_build.reset()
    beaver_build.cancel_all_transforms()


@pytest.fixture
def tempdir():
    with tempfile.TemporaryDirectory() as tmp, beaver_build.working_directory(tmp):
        yield tmp
