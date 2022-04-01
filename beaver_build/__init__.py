import json
import logging
from .artifacts import ArtifactFactory, File, Group
from .transforms import Shell, Transform
from .artifacts import *  # noqa: F401, F403
from .transforms import *   # noqa: F401, F403
from .util import *  # noqa: F401, F403


LOGGER = logging.getLogger(__name__)


def reset() -> None:
    """
    Reset all global variables.
    """
    ArtifactFactory.REGISTRY.clear()
    File.DIGESTS.clear()
    Group.STACK.clear()
    Shell.ENV.clear()
    Transform.COMPOSITE_DIGESTS.clear()
    Transform.DRY_RUN = False
    Transform.SEMAPHORE = None


def save_cache(filename: str) -> None:
    """
    Save all cached information.
    """
    cache = {
        "composite_digests": Transform.COMPOSITE_DIGESTS,
        "file_digests": File.DIGESTS,
    }
    with open(filename, "w") as fp:
        json.dump(cache, fp, indent=4)
    LOGGER.debug("saved %d composite digests to `%s`", len(Transform.COMPOSITE_DIGESTS),
                 filename)
    LOGGER.debug("saved %d file digests to `%s`", len(File.DIGESTS), filename)


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

    # Load composite digests.
    if composite_digests := cache.get("composite_digests"):
        composite_digests = {name: digest for name, digest in composite_digests.items()
                             if name in ArtifactFactory.REGISTRY}
        Transform.COMPOSITE_DIGESTS = composite_digests
        LOGGER.debug("loaded %d composite digests from `%s`", composite_digests, filename)

    # Load file digests.
    if file_digests := cache.get("file_digests"):
        file_digests = {name: digest for name, digest in cache["file_digests"].items()
                        if isinstance(ArtifactFactory.REGISTRY.get(name), File)}
        File.DIGESTS = file_digests
        LOGGER.debug("loaded %d file digests from `%s`", file_digests, filename)
