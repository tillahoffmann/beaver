import argparse
import asyncio
import importlib
import json
import logging
import re
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


def build_artifacts(args: argparse.Namespace) -> int:
    """
    Build one or more artifacts.
    """
    Transformation.DRY_RUN = args.dry_run

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


def list_artifacts(args: argparse.Namespace) -> int:
    """
    List artifacts, possibly matching a pattern.
    """
    lines = []
    for name, artifact in sorted(ArtifactFactory.REGISTRY.items()):
        if args.pattern and not re.match(args.pattern, name):
            continue
        if args.stale and not artifact.is_stale:
            continue
        if args.raw:
            prefix = ""
        elif artifact.is_stale:
            prefix = "\U0001f7e1 "
        else:
            prefix = "\U0001f7e2 "
        lines.append(f'{prefix}{name}')
    print('\n'.join(lines))


def build_parser() -> argparse.ArgumentParser:
    # Top-level parser for common arguments.
    parser = argparse.ArgumentParser()
    parser.add_argument("--digest", "-d", help="file containing composite artifact digests",
                        default=".beaverdigests")
    parser.add_argument("--file", "-f", help="file containing artifact and transform definitions",
                        default="beaver.py")
    parser.add_argument("--log_level", "-l", help="level of log messages to emit", default="info",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], type=str.upper)
    subparsers = parser.add_subparsers(title="command", help="command to execute")

    # Subparser for building artifacts.
    build_parser = subparsers.add_parser("build", help=build_artifacts.__doc__.strip())
    build_parser.add_argument("--num_concurrent", "-c", help="number of concurrent transformations",
                              type=int, default=1)
    build_parser.add_argument("--dry-run", "-n", action="store_true",
                              help="print transformations without executing them")
    build_parser.add_argument("artifacts", help="artifacts to generate", nargs="+")
    build_parser.set_defaults(func=build_artifacts)

    # Subparser for listing artifacts.
    list_parser = subparsers.add_parser("list", help=list_artifacts.__doc__.strip())
    list_parser.add_argument("--stale", "-s", help="list only stale artifacts", action="store_true")
    list_parser.add_argument("--raw", "-r", help="list names only without status indicators",
                             action="store_true")
    list_parser.add_argument("pattern", help="pattern to match artifacts against", nargs="?")
    list_parser.set_defaults(func=list_artifacts)
    return parser


def __main__(args: typing.Iterable[str] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(args)

    # Configure logging to stderr.
    root_logger = logging.getLogger()
    root_logger.setLevel(args.log_level)
    handler = logging.StreamHandler()
    handler.setFormatter(Formatter("\U0001f9ab %(_prefix)s%(levelname)s%(_suffix)s: %(message)s"))
    root_logger.addHandler(handler)

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

    # Execute the subcommand.
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover
    __main__()
