import beaver_build as bb
import logging
import pytest
import tempfile


logging.raiseExceptions = False
bb.context.STRICT_CONTEXT_MANAGEMENT = True


@pytest.fixture(autouse=True)
def clean_slate():
    bb.reset()
    bb.cancel_all_transforms()


@pytest.fixture(autouse=True)
def tempdir():
    with tempfile.TemporaryDirectory() as tmp, bb.working_directory(tmp):
        yield tmp


@pytest.fixture(autouse=True)
def context(request):
    if "no_auto_context" in request.keywords:
        yield
    else:
        with bb.Context() as context:
            yield context
