import argparse
import collections
import functools
import json
import logging
import urllib.parse
from argparse import Namespace
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Mapping
from typing import Optional
from typing import Union

import requests

Host = collections.namedtuple("Host", ["cross_cluster_remotes", "password", "url", "username", "verify"])

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
    ap.add_argument(
        "-c",
        "--ccs-remotes",
        default=[],
        nargs="*",
        help="names of additional opensearch cross-cluster remotes, space-separated",
    )
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
    ap.add_argument("-v", "--verify", action="store_true", default=False, help="verify the host certificates")

    args = ap.parse_args()
    return args


def parse_log_level(input: str) -> int:
    """Given a numeric or uppercase descriptive log level, return the associated int"""
    try:
        result = int(input)
    except ValueError:
        result = getattr(logging, input)
    return result


def _vid_as_tuple_of_int(lidvid: str):
    major_version, minor_version = lidvid.split("::")[1].split(".")
    return (int(major_version), int(minor_version))


def configure_logging(filepath: Union[str, None], log_level: int):
    logging.root.handlers = []
    handlers: List[logging.StreamHandler] = [logging.StreamHandler()]

    if filepath:
        handlers.append(logging.FileHandler(filepath))

    logging.basicConfig(level=log_level, format="%(asctime)s::%(levelname)s::%(message)s", handlers=handlers)


def query_registry_db(
    host: Host,
    query: Dict,
    _source: Dict,
    index_name: str = "registry",
    page_size: int = 10000,
    scroll_validity_duration_minutes: int = 10,
) -> Iterable[Dict]:
    """
    Given an OpenSearch host and query/_source, return an iterable collection of hits

    Example query: {"bool": {"must": [{"terms": {"ops:Tracking_Meta/ops:archive_status": ["archived", "certified"]}}]}}
    Example _source: {"includes": ["lidvid"]}
    """

    req_content = {
        "query": query,
        "_source": _source,
        "size": page_size,
    }

    log.debug(f"Initiating query: {req_content}")

    cross_cluster_indexes = [f"{node}:{index_name}" for node in host.cross_cluster_remotes]
    path = ",".join([index_name] + cross_cluster_indexes) + f"/_search?scroll={scroll_validity_duration_minutes}m"
    served_hits = 0

    more_data_exists = True
    while more_data_exists:
        resp = requests.get(
            urllib.parse.urljoin(host.url, path),
            auth=(host.username, host.password),
            verify=host.verify,
            json=req_content,
        )
        resp.raise_for_status()

        data = resp.json()
        path = "_search/scroll"
        req_content = {"scroll": f"{scroll_validity_duration_minutes}m", "scroll_id": data["_scroll_id"]}

        total_hits = data["hits"]["total"]["value"]
        log.info(f"   paging query ({served_hits} to {min(served_hits + page_size, total_hits)} of {total_hits})")
        for hit in data["hits"]["hits"]:
            yield hit
            served_hits += 1

        more_data_exists = served_hits < data["hits"]["total"]["value"]

    if "scroll_id" in req_content:
        path = f'_search/scroll/{req_content["scroll_id"]}'
        requests.delete(urllib.parse.urljoin(host.url, path), auth=(host.username, host.password), verify=host.verify)


def query_registry_db_or_mock(mock_f: Optional[Callable[[str], Iterable[Dict]]], mock_query_id: str):
    if mock_f is not None:

        def mock_wrapper(
            host: Host,
            query: Dict,
            _source: Dict,
            index_name: str = "registry",
            page_size: int = 10000,
            scroll_validity_duration_minutes: int = 10,
        ) -> Iterable[Dict]:
            return mock_f(mock_query_id)  # type: ignore  # see None-check above

        return mock_wrapper
    else:
        return query_registry_db


def get_extant_lidvids(host: Host) -> Iterable[str]:
    """
    Given an OpenSearch host, return all extant LIDVIDs
    """

    log.info("Retrieving extant LIDVIDs")

    query = {"bool": {"must": [{"terms": {"ops:Tracking_Meta/ops:archive_status": ["archived", "certified"]}}]}}
    _source = {"includes": ["lidvid"]}

    results = query_registry_db(host, query, _source, scroll_validity_duration_minutes=1)

    return map(lambda doc: doc["_source"]["lidvid"], results)


def write_updated_docs(host: Host, ids_and_updates: Mapping[str, Dict], index_name: str = "registry"):
    """
    Given an OpenSearch host and a mapping of doc ids onto updates to those docs, write bulk updates to documents in db.
    """
    log.info("Bulk update %d documents", len(ids_and_updates))

    bulk_updates = []
    cross_cluster_indexes = [f"{node}:{index_name}" for node in host.cross_cluster_remotes]
    headers = {"Content-Type": "application/x-ndjson"}
    path = ",".join([index_name] + cross_cluster_indexes) + "/_bulk"

    for lidvid, update_content in ids_and_updates.items():
        bulk_updates.append(json.dumps({"update": {"_id": lidvid}}))
        bulk_updates.append(json.dumps({"doc": update_content}))

    bulk_data = "\n".join(bulk_updates) + "\n"

    log.info(f"writing bulk update for {len(bulk_updates)} products...")
    response = requests.put(
        urllib.parse.urljoin(host.url, path),
        auth=(host.username, host.password),
        data=bulk_data,
        headers=headers,
        verify=host.verify,
    )
    response.raise_for_status()

    response_content = response.json()
    if response_content.get("errors"):
        for item in response_content["items"]:
            if "error" in item:
                log.error("update error (%d): %s", item["status"], str(item["error"]))
    else:
        log.info("bulk updates were successful")
