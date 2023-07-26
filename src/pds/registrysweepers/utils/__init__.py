import argparse
import collections
import functools
import json
import logging
import urllib.parse
from argparse import Namespace
from datetime import datetime
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Mapping
from typing import Optional
from typing import Union
from urllib.error import HTTPError

import requests
from retry import retry
from retry.api import retry_call

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

    log.info(f"Initiating query: {req_content}")

    cross_cluster_indexes = [f"{node}:{index_name}" for node in host.cross_cluster_remotes]
    path = ",".join([index_name] + cross_cluster_indexes) + f"/_search?scroll={scroll_validity_duration_minutes}m"
    served_hits = 0

    last_info_log_at_percentage = 0
    log.info("Query progress: 0%")

    more_data_exists = True
    while more_data_exists:
        resp = retry_call(
            requests.get,
            fargs=[urllib.parse.urljoin(host.url, path)],
            fkwargs={"auth": (host.username, host.password), "verify": host.verify, "json": req_content},
            tries=4,
            delay=2,
            backoff=2,
            logger=log,
        )
        resp.raise_for_status()

        data = resp.json()
        path = "_search/scroll"
        req_content = {"scroll": f"{scroll_validity_duration_minutes}m", "scroll_id": data["_scroll_id"]}

        total_hits = data["hits"]["total"]["value"]
        log.debug(f"   paging query ({served_hits} to {min(served_hits + page_size, total_hits)} of {total_hits})")

        for hit in data["hits"]["hits"]:
            served_hits += 1

            percentage_of_hits_served = int(served_hits / total_hits * 100)
            if last_info_log_at_percentage is None or percentage_of_hits_served >= (last_info_log_at_percentage + 5):
                last_info_log_at_percentage = percentage_of_hits_served
                log.info(f"Query progress: {percentage_of_hits_served}%")

            yield hit

        more_data_exists = served_hits < data["hits"]["total"]["value"]

    if "scroll_id" in req_content:
        path = f'_search/scroll/{req_content["scroll_id"]}'
        retry_call(
            requests.delete,
            fargs=[urllib.parse.urljoin(host.url, path)],
            fkwargs={"auth": (host.username, host.password), "verify": host.verify},
            tries=4,
            delay=2,
            backoff=2,
            logger=log,
        )

    log.info("Query complete!")


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
    bulk_update_chunk_threshold = 10000  # threshold is statements count.  There are two products per statement
    bulk_update_products_threshold = int(bulk_update_chunk_threshold / 2)

    bulk_updates: List[str] = []
    for lidvid, update_content in ids_and_updates.items():
        if len(bulk_updates) >= bulk_update_chunk_threshold:
            log.info(
                f"Bulk update chunk threshold reached ({bulk_update_chunk_threshold} statements, {bulk_update_products_threshold} products), writing chunk to db..."
            )
            _write_bulk_updates_chunk(host, index_name, bulk_updates)
            bulk_updates = []
        bulk_updates.append(json.dumps({"update": {"_id": lidvid}}))
        bulk_updates.append(json.dumps({"doc": update_content}))

    remaining_products_to_write_count = int(len(bulk_updates) / 2)
    log.info(f"Writing updates for {remaining_products_to_write_count} remaining products to db...")
    _write_bulk_updates_chunk(host, index_name, bulk_updates)


@retry(exceptions=(HTTPError, RuntimeError), tries=4, delay=2, backoff=2, logger=log)
def _write_bulk_updates_chunk(host: Host, index_name: str, bulk_updates: Iterable[str]):
    cross_cluster_indexes = [f"{node}:{index_name}" for node in host.cross_cluster_remotes]
    headers = {"Content-Type": "application/x-ndjson"}
    path = ",".join([index_name] + cross_cluster_indexes) + "/_bulk"

    bulk_data = "\n".join(bulk_updates) + "\n"

    response = requests.put(
        urllib.parse.urljoin(host.url, path),
        auth=(host.username, host.password),
        data=bulk_data,
        headers=headers,
        verify=host.verify,
    )

    # N.B. HTTP status 200 is insufficient as a success check for _bulk API.
    # See: https://github.com/elastic/elasticsearch/issues/41434
    response.raise_for_status()
    response_content = response.json()
    if response_content.get("errors"):
        warn_types = {"document_missing_exception"}  # these types represent bad data, not bad sweepers behaviour
        items_with_problems = [item for item in response_content["items"] if "error" in item["update"]]

        if log.isEnabledFor(logging.WARNING):
            items_with_warnings = [
                item for item in items_with_problems if item["update"]["error"]["type"] in warn_types
            ]
            warning_aggregates = aggregate_update_error_types(items_with_warnings)
            for error_type, reason_aggregate in warning_aggregates.items():
                for error_reason, ids in reason_aggregate.items():
                    log.warning(
                        f"Attempt to update the following documents failed due to {error_type} ({error_reason}): {ids}"
                    )

        if log.isEnabledFor(logging.ERROR):
            items_with_errors = [
                item for item in items_with_problems if item["update"]["error"]["type"] not in warn_types
            ]
            error_aggregates = aggregate_update_error_types(items_with_errors)
            for error_type, reason_aggregate in error_aggregates.items():
                for error_reason, ids in reason_aggregate.items():
                    log.error(
                        f"Attempt to update the following documents failed unexpectedly due to {error_type} ({error_reason}): {ids}"
                    )


def aggregate_update_error_types(items: Iterable[Dict]) -> Mapping[str, Dict[str, List[str]]]:
    """Return a nested aggregation of ids, aggregated first by error type, then by reason"""
    agg: Dict[str, Dict[str, List[str]]] = {}
    for item in items:
        id = item["update"]["_id"]
        error = item["update"]["error"]
        error_type = error["type"]
        error_reason = error["reason"]
        if error_type not in agg:
            agg[error_type] = {}

        if error_reason not in agg[error_type]:
            agg[error_type][error_reason] = []

        agg[error_type][error_reason].append(id)

    return agg


def coerce_list_type(db_value: Any) -> List[Any]:
    """
    Coerce a non-array-typed legacy db record into a list containing itself as the only element, or return the
    original argument if it is already an array (list).  This is sometimes necessary to support legacy db records which
    did not wrap singleton properties in an enclosing array.
    """

    return (
        db_value
        if type(db_value) is list
        else [
            db_value,
        ]
    )


def get_human_readable_elapsed_since(begin: datetime) -> str:
    elapsed_seconds = (datetime.now() - begin).total_seconds()
    h = int(elapsed_seconds / 3600)
    m = int(elapsed_seconds % 3600 / 60)
    s = int(elapsed_seconds % 60)
    return (f"{h}h" if h else "") + (f"{m}m" if m else "") + f"{s}s"
