import beaver_build as bb
import pytest


def test_context_reentry(context: bb.Context):
    with pytest.raises(RuntimeError), context:
        pass


@pytest.mark.no_auto_context
def test_no_current_context():
    with pytest.raises(RuntimeError):
        bb.get_current_context()
