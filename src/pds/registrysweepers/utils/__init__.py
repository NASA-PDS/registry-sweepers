import argparse
import logging
from argparse import Namespace
from typing import List
from typing import Union

log = logging.getLogger(__name__)


def parse_args(description: str = "", epilog: str = "") -> Namespace:
    """
    Provides a consistent CLI for sweepers.  May need to be re-thought in future but a standardized interface makes
    sense for the time being.
    """
    ap = argparse.ArgumentParser(
        description=description,
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("-b", "--base-URL", required=True, type=str)
    ap.add_argument("-l", "--log-file", default=None, required=False, help="file to write the log messages")
    ap.add_argument(
        "-L",
        "--log-level",
        default="ERROR",
        required=False,
        type=parse_log_level,
        help="Python logging level as an int or string like INFO for logging.INFO [%(default)s]",
    )
    ap.add_argument(
        "-p",
        "--password",
        default=None,
        required=False,
        help="password to login to the registry db, leaving it blank if db does not require login",
    )
    ap.add_argument(
        "-u",
        "--username",
        default=None,
        required=False,
        help="username to login to the registry db, leaving it blank if db does not require login",
    )
    ap.add_argument("--insecure", action="store_true", default=False, help="skip verification of the host certificates")

    args = ap.parse_args()
    return args


def parse_log_level(input: str) -> int:
    """Given a numeric or uppercase descriptive log level, return the associated int"""
    try:
        result = int(input)
    except ValueError:
        result = getattr(logging, input.upper())
    return result


def _vid_as_tuple_of_int(lidvid: str):
    major_version, minor_version = lidvid.split("::")[1].split(".")
    return (int(major_version), int(minor_version))


def configure_logging(filepath: Union[str, None], log_level: int):
    logging.root.handlers = []
    handlers: List[logging.StreamHandler] = [logging.StreamHandler()]

    if filepath:
        handlers.append(logging.FileHandler(filepath))

    logging.basicConfig(level=log_level, format="%(asctime)s::%(name)s::%(levelname)s::%(message)s", handlers=handlers)
