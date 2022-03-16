import beaver
import pytest
import tempfile


@pytest.fixture(autouse=True)
def clean_slate():
    beaver.ArtifactFactory.REGISTRY.clear()
    beaver.Transformation.COMPOSITE_DIGESTS.clear()
    beaver.Transformation.SEMAPHORE = None
    beaver.Shell.ENV.clear()
    beaver.Group.STACK = []


@pytest.fixture
def tempdir():
    with tempfile.TemporaryDirectory() as tmp, beaver.working_directory(tmp):
        yield tmp
