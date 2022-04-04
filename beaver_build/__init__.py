import json
import logging
from .artifacts import ArtifactFactory, Group
from .transforms import Shell, Transform
from .artifacts import *  # noqa: F401, F403
from .transforms import *   # noqa: F401, F403
from .util import *  # noqa: F401, F403


LOGGER = logging.getLogger(__name__)
CACHE_VERSION = 'alpha'


def reset() -> None:
    """
    Reset all global variables.
    """
    ArtifactFactory.REGISTRY.clear()
    Group.STACK.clear()
    Shell.ENV.clear()
    Transform.DRY_RUN = False
    Transform._SEMAPHORE = None


def save_cache(filename: str) -> None:
    """
    Save all cached information.
    """
    cache = {
        "version": CACHE_VERSION,
        "artifact_metadata": {
            name: artifact.metadata for name, artifact in ArtifactFactory.REGISTRY.items()
            if artifact.metadata
        },
    }
    with open(filename, "w") as fp:
        json.dump(cache, fp, indent=4)


def load_cache(filename: str) -> None:
    """
    Load all cached information.
    """
    try:
        with open(filename) as fp:
            cache = json.load(fp)
    except FileNotFoundError:
        LOGGER.debug("did not load cache because the file `%s` does not exist", filename)
        return

    assert cache["version"] == CACHE_VERSION, \
        f"expected cache version `{CACHE_VERSION}` but got `{cache['version']}`"

    for name, metadata in cache.get("artifact_metadata", {}).items():
        if artifact := ArtifactFactory.REGISTRY.get(name):
            artifact.metadata.update(metadata)
