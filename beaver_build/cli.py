import argparse
import asyncio
import importlib
import logging
import re
import typing
from . import load_cache, save_cache
from .artifacts import Artifact, ArtifactFactory, gather_artifacts, Group
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
    Build artifacts.
    """
    Transformation.DRY_RUN = args.dry_run

    try:
        # Get the targets we want to build and wait for them to complete.
        artifacts = match_artifacts(args)
        asyncio.run(gather_artifacts(*artifacts, num_concurrent=args.num_concurrent))
    finally:
        cancel_all_transformations()


def list_artifacts(args: argparse.Namespace) -> int:
    """
    List artifacts.
    """
    lines = []
    for artifact in match_artifacts(args):
        if args.stale and not artifact.is_stale:
            continue
        if args.raw:
            prefix = ""
        elif artifact.parent is None and not isinstance(artifact, Group):
            prefix = '\u26aa '
        elif artifact.is_stale:
            prefix = "\U0001f7e1 "
        else:
            prefix = "\U0001f7e2 "
        lines.append(f'{prefix}{artifact.name}')
    print('\n'.join(lines))


def reset_composite_digests(args: argparse.Namespace) -> int:
    """
    Reset the composite digest of artifacts
    """
    num_reset = 0
    for artifact in match_artifacts(args):
        if artifact.name not in ArtifactFactory.REGISTRY:  # pragma: no cover
            raise RuntimeError(f"artifact `{artifact.name}` is not in the registry")
        elif not Transformation.COMPOSITE_DIGESTS.pop(artifact.name, None):
            LOGGER.info("artifact `%s` did not have a composite digest", artifact.name)
        else:
            num_reset += 1
    LOGGER.info("reset %d composite digests", num_reset)


def match_artifacts(args: argparse.Namespace) -> typing.Iterable[Artifact]:
    """
    Obtain all artifacts that match any of the patterns.
    """
    if args.all:
        return ArtifactFactory.REGISTRY.values()
    artifacts = [
        value for key, value in ArtifactFactory.REGISTRY.items()
        if any(re.match(pattern, key) for pattern in args.patterns)
    ]
    if not artifacts:
        LOGGER.warning("patterns did not match any artifacts; use `--all` to select all artifacts")
    return artifacts


def add_pattern_arguments(parser: argparse.ArgumentParser) -> None:
    """
    Add arguments to be used for matching artifacts to a parser.
    """
    parser.add_argument("--all", "-a", help="match all artifacts", action="store_true")
    parser.add_argument("patterns", help="patterns to match artifacts against", nargs="*")


def build_parser() -> argparse.ArgumentParser:
    # Top-level parser for common arguments.
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache", "-c", default=".beavercache",
                        help="file containing cached information, including composite digests")
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
    build_parser.set_defaults(func=build_artifacts)

    # Subparser for listing artifacts.
    list_parser = subparsers.add_parser("list", help=list_artifacts.__doc__.strip())
    list_parser.add_argument("--stale", "-s", help="list only stale artifacts", action="store_true")
    list_parser.add_argument("--raw", "-r", help="list names only without status indicators",
                             action="store_true")
    list_parser.set_defaults(func=list_artifacts)

    # Subparser for resetting the composite digest of artifacts.
    reset_parser = subparsers.add_parser("reset", help=reset_composite_digests.__doc__.strip())
    reset_parser.set_defaults(func=reset_composite_digests)

    for subparser in [build_parser, list_parser, reset_parser]:
        add_pattern_arguments(subparser)

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

    # Load the cache and execute the subcommand.
    load_cache(args.cache)
    result = args.func(args)
    save_cache(args.cache)
    return result


if __name__ == "__main__":  # pragma: no cover
    __main__()
