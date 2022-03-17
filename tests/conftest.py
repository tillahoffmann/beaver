import beaver_build
import pytest
import tempfile


@pytest.fixture(autouse=True)
def clean_slate():
    beaver_build.reset()


@pytest.fixture
def tempdir():
    with tempfile.TemporaryDirectory() as tmp, beaver_build.working_directory(tmp):
        yield tmp
