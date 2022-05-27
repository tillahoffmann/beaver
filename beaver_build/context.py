import json
import logging
import os
import re
import typing
from . import artifacts


LOGGER = logging.getLogger(__name__)


class Context:
    """
    Context manager responsible for managing and updating state.
    """
    CURRENT_CONTEXT: "Context" = None
    CACHE_VERSION = "alpha"

    def __init__(self):
        self.artifacts = {}
        self.properties = {}

    def __enter__(self) -> "Context":
        if Context.CURRENT_CONTEXT is not None:
            raise RuntimeError("only one context can be active at the same time")
        Context.CURRENT_CONTEXT = self
        return self

    def __exit__(self, *_) -> None:
        if Context.CURRENT_CONTEXT is not self:
            raise RuntimeError("another context is active")  # pragma: no cover
        Context.CURRENT_CONTEXT = None

    def get_properties(self, key) -> dict:
        """
        Get a modifiable dictionary of properties for the given key, e.g. a particular
        :cls:`Transform`.
        """
        return self.properties.setdefault(key, {})

    def match_artifacts(self, patterns: typing.Iterable[str], all: bool = False) \
            -> typing.Iterable["artifacts.Artifact"]:
        """
        Obtain all artifacts that match any of the patterns.

        Args:
            patterns: Sequence of patterns to match artifacts against.
            all: Whether to return all artifacts.

        Returns:
            matching: All matching artifacts.
        """
        if all:
            return self.artifacts.values()
        artifacts = [value for key, value in self.artifacts.items()
                     if any(re.match(pattern, key) for pattern in patterns)]
        if artifacts:
            LOGGER.debug("patterns matched %d artifacts", len(artifacts))
        else:
            LOGGER.warning("patterns did not match any artifacts")
        return artifacts

    def dump(self, filename: str) -> None:
        """
        Save all cached information.
        """
        cache = {
            "version": self.CACHE_VERSION,
            "artifact_metadata": {
                name: artifact.metadata for name, artifact in self.artifacts.items()
                if artifact.metadata
            },
        }
        with open(filename, "w") as fp:
            json.dump(cache, fp, indent=4)

    def load(self, filename: str) -> None:
        """
        Load all cached information.
        """
        with open(filename) as fp:
            cache = json.load(fp)

        if cache["version"] != self.CACHE_VERSION:  # pragma: no cover
            raise ValueError(f"expected cache version `{self.CACHE_VERSION}` but got "
                             f"`{cache['version']}`")

        for name, metadata in cache.get("artifact_metadata", {}).items():
            if artifact := self.artifacts.get(name):
                artifact.metadata.update(metadata)


DEFAULT_CONTEXT = Context()
STRICT_CONTEXT_MANAGEMENT = int(os.environ.get("BEAVER_STRICT_CONTEXT_MANAGEMENT", 0))


def get_current_context() -> Context:
    """
    Get the current context.
    """
    if Context.CURRENT_CONTEXT is None:
        if STRICT_CONTEXT_MANAGEMENT:
            raise RuntimeError("no context is active")
        else:
            return DEFAULT_CONTEXT  # pragma: no cover
    return Context.CURRENT_CONTEXT
