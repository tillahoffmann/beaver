import argparse
import asyncio
import importlib
import json
import logging
import typing
from .artifacts import Artifact, gather_artifacts
from .transformations import Transformation


LOGGER = logging.getLogger("beaver")


def __main__(args: typing.Iterable[str] = None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--digest", "-d", help="file containing composite artifact digests",
                        default=".beaverdigests")
    parser.add_argument("--file", "-f", help="file containing artifact and transform definitions",
                        default="dam.py")
    parser.add_argument("artifacts", help="artifacts to generate", nargs="+")
    args = parser.parse_args(args)

    # Load the composite digests.
    try:
        with open(args.digest) as fp:
            Transformation.COMPOSITE_DIGESTS = json.load(fp)
    except FileNotFoundError:
        pass

    # Load the artifact and transformation configuration.
    spec = importlib.util.spec_from_file_location("config", args.file)
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)
    LOGGER.debug("loaded %d artifacts from `%s`", len(Artifact.REGISTRY), args.file)

    # Get the targets we want to build and wait for them to complete.
    artifacts = [Artifact.REGISTRY[name] for name in args.artifacts]
    asyncio.run(gather_artifacts(*artifacts))

    # Save the updated composite digests.
    with open(args.digest, "w") as fp:
        json.dump(Transformation.COMPOSITE_DIGESTS, fp, indent=4)


if __name__ == "__main__":  # pragma: no cover
    __main__()
