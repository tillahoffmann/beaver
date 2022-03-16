import beaver
import pytest
import tempfile


@pytest.fixture(autouse=True)
def clean_slate():
    beaver.reset()


@pytest.fixture
def tempdir():
    with tempfile.TemporaryDirectory() as tmp, beaver.working_directory(tmp):
        yield tmp
