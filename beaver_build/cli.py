import argparse
import asyncio
import importlib
import json
import logging
import typing
from .artifacts import ArtifactFactory, gather_artifacts
from .transformations import cancel_all_transformations, Transformation


LOGGER = logging.getLogger("beaver")


class Formatter(logging.Formatter):
    # Based on https://stackoverflow.com/a/56944256/1150961.
    cyan = "\x1b[36;20m"
    green = "\x1b[32;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    colors_by_level = {
        'debug': cyan,
        'info': green,
        'warning': yellow,
        'error': red,
        'critical': bold_red
    }

    def format(self, record: logging.LogRecord) -> str:
        record._prefix = self.colors_by_level[record.levelname.lower()]
        record._suffix = self.reset
        return super().format(record)


def __main__(args: typing.Iterable[str] = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--digest", "-d", help="file containing composite artifact digests",
                        default=".beaverdigests")
    parser.add_argument("--file", "-f", help="file containing artifact and transform definitions",
                        default="beaver.py")
    parser.add_argument("--num_concurrent", "-c", help="number of concurrent transformations",
                        type=int, default=1)
    parser.add_argument("--dry-run", "-n", help="print transformations without executing them",
                        action="store_true")
    parser.add_argument("--log_level", "-l", help="level of log messages to emit", default="info",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], type=str.upper)
    parser.add_argument("artifacts", help="artifacts to generate", nargs="+")
    args = parser.parse_args(args)

    # Configure logging to stderr.
    root_logger = logging.getLogger()
    root_logger.setLevel(args.log_level)
    handler = logging.StreamHandler()
    handler.setFormatter(Formatter("\U0001f9ab %(_prefix)s%(levelname)s%(_suffix)s: %(message)s"))
    root_logger.addHandler(handler)

    Transformation.DRY_RUN = args.dry_run

    # Load the artifact and transformation configuration.
    try:
        spec = importlib.util.spec_from_file_location("config", args.file)
        config = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(config)
        LOGGER.debug("loaded %d artifacts from `%s`", len(ArtifactFactory.REGISTRY), args.file)
    except FileNotFoundError:
        LOGGER.error("beaver configuration cannot be loaded from %s", args.file)
        return 1

    # Load the composite digests but only retain the composite digests of known artifacts.
    try:
        with open(args.digest) as fp:
            composite_digests = json.load(fp)
        composite_digests = {name: digest for name, digest in composite_digests.items()
                             if name in ArtifactFactory.REGISTRY}
        Transformation.COMPOSITE_DIGESTS = composite_digests
        LOGGER.debug("loaded %d composite digests from `%s`", len(composite_digests), args.digest)
    except FileNotFoundError:
        LOGGER.debug("did not load composite digests because the file `%s` does not exist",
                     args.digest)
        pass

    try:
        # Get the targets we want to build and wait for them to complete.
        artifacts = [ArtifactFactory.REGISTRY[name] for name in args.artifacts]
        asyncio.run(gather_artifacts(*artifacts, num_concurrent=args.num_concurrent))
    finally:
        # Save the updated composite digests.
        with open(args.digest, "w") as fp:
            json.dump(Transformation.COMPOSITE_DIGESTS, fp, indent=4)
        LOGGER.debug("saved %d composite digests to `%s`", len(Transformation.COMPOSITE_DIGESTS),
                     args.digest)
        cancel_all_transformations()


if __name__ == "__main__":  # pragma: no cover
    __main__()
