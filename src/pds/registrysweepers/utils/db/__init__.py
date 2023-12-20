import json
import logging
import sys
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Mapping
from typing import Optional
from typing import Union

from opensearchpy import OpenSearch
from pds.registrysweepers.utils.db.update import Update
from pds.registrysweepers.utils.misc import get_random_hex_id
from retry import retry
from retry.api import retry_call

log = logging.getLogger(__name__)


def query_registry_db(
    client: OpenSearch,
    query: Dict,
    _source: Dict,
    index_name: str = "registry",
    page_size: int = 10000,
    scroll_keepalive_minutes: int = 10,
) -> Iterable[Dict]:
    """
    Given an OpenSearch client and query/_source, return an iterable collection of hits

    Example query: {"query: {"bool": {"must": [{"terms": {"ops:Tracking_Meta/ops:archive_status": ["archived", "certified"]}}]}}}
    Example _source: {"includes": ["lidvid"]}
    """

    scroll_keepalive = f"{scroll_keepalive_minutes}m"
    request_timeout = 20
    query_id = get_random_hex_id()  # This is just used to differentiate queries during logging
    full_query_content = json.dumps(query)
    logged_query_content = full_query_content if len(full_query_content) < 5000 else "<LARGE QUERY CONTENT OMITTED>"
    log.info(f"Initiating query with id {query_id}: {logged_query_content}")
    served_hits = 0

    last_info_log_at_percentage = 0
    log.info(f"Query {query_id} progress: 0%")

    more_data_exists = True
    scroll_id = None
    while more_data_exists:
        if scroll_id is None:

            def fetch_func():
                return client.search(
                    index=index_name,
                    body=query,
                    scroll=scroll_keepalive,
                    request_timeout=request_timeout,
                    size=page_size,
                    _source_includes=_source.get("includes", []),  # TODO: Break out from the enclosing _source object
                    _source_excludes=_source.get("excludes", []),  # TODO: Break out from the enclosing _source object
                )

        else:

            def fetch_func(_scroll_id: str = scroll_id):
                return client.scroll(scroll_id=_scroll_id, scroll=scroll_keepalive, request_timeout=request_timeout)

        results = retry_call(
            fetch_func,
            tries=6,
            delay=2,
            backoff=2,
            logger=log,
        )
        scroll_id = results.get("_scroll_id")

        total_hits = results["hits"]["total"]["value"]
        if served_hits == 0:
            log.info(f"Query {query_id} returns {total_hits} total hits")
        log.debug(
            f"   paging query {query_id} ({served_hits} to {min(served_hits + page_size, total_hits)} of {total_hits})"
        )

        response_hits = results["hits"]["hits"]
        for hit in response_hits:
            served_hits += 1

            percentage_of_hits_served = int(served_hits / total_hits * 100)
            if last_info_log_at_percentage is None or percentage_of_hits_served >= (last_info_log_at_percentage + 5):
                last_info_log_at_percentage = percentage_of_hits_served
                log.info(f"Query {query_id} progress: {percentage_of_hits_served}%")

            yield hit

        # This is a temporary, ad-hoc guard against empty/erroneous responses which do not return non-200 status codes.
        # Previously, this has cause infinite loops in production due to served_hits sticking and never reaching the
        # expected total hits value.
        # TODO: Remove this upon implementation of https://github.com/NASA-PDS/registry-sweepers/issues/42
        hits_data_present_in_response = len(response_hits) > 0
        if not hits_data_present_in_response and served_hits < total_hits:
            log.error(
                f"Response for query {query_id} contained no hits when hits were expected.  Returned data is incomplete (got {served_hits} of {total_hits} total hits).  Response was: {results}"
            )
            break

        more_data_exists = served_hits < results["hits"]["total"]["value"]

    retry_call(
        client.clear_scroll,
        fkwargs={"scroll_id": scroll_id},
        tries=6,
        delay=2,
        backoff=2,
        logger=log,
    )

    log.info(f"Query {query_id} complete!")


def query_registry_db_or_mock(mock_f: Optional[Callable[[str], Iterable[Dict]]], mock_query_id: str):
    if mock_f is not None:

        def mock_wrapper(
            client: OpenSearch,
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


def write_updated_docs(
    client: OpenSearch,
    updates: Iterable[Update],
    index_name: str = "registry",
    bulk_chunk_max_update_count: Union[int, None] = None,
):
    log.info("Updating a lazily-generated collection of product documents...")
    updated_doc_count = 0

    bulk_buffer_max_size_mb = 30.0
    bulk_buffer_size_mb = 0.0
    bulk_updates_buffer: List[str] = []
    for update in updates:
        buffered_updates_count = len(bulk_updates_buffer) // 2
        buffer_at_size_threshold = bulk_buffer_size_mb >= bulk_buffer_max_size_mb
        buffer_at_update_count_threshold = (
            bulk_chunk_max_update_count is not None and buffered_updates_count >= bulk_chunk_max_update_count
        )
        flush_threshold_reached = buffer_at_size_threshold or buffer_at_update_count_threshold
        threshold_log_str = (
            f"{bulk_buffer_max_size_mb}MB" if buffer_at_size_threshold else f"{bulk_chunk_max_update_count}docs"
        )

        if flush_threshold_reached:
            log.info(
                f"Bulk update buffer has reached {threshold_log_str} threshold - writing {buffered_updates_count} document updates to db..."
            )
            _write_bulk_updates_chunk(client, index_name, bulk_updates_buffer)
            bulk_updates_buffer = []
            bulk_buffer_size_mb = 0.0

        update_statement_strs = update_as_statements(update)

        for s in update_statement_strs:
            bulk_buffer_size_mb += sys.getsizeof(s) / 1024**2

        bulk_updates_buffer.extend(update_statement_strs)
        updated_doc_count += 1

    if len(bulk_updates_buffer) > 0:
        log.info(f"Writing documents updates for {buffered_updates_count} remaining products to db...")
        _write_bulk_updates_chunk(client, index_name, bulk_updates_buffer)

    log.info(f"Updated documents for {updated_doc_count} total products!")


def update_as_statements(update: Update) -> Iterable[str]:
    """Given an Update, convert it to an ElasticSearch-style set of request body content strings"""
    update_objs = [{"update": {"_id": update.id}}, {"doc": update.content}]
    updates_strs = [json.dumps(obj) for obj in update_objs]
    return updates_strs


@retry(tries=6, delay=2, backoff=2, logger=log)
def _write_bulk_updates_chunk(client: OpenSearch, index_name: str, bulk_updates: Iterable[str]):
    bulk_data = "\n".join(bulk_updates) + "\n"

    request_timeout = 60
    response_content = client.bulk(index=index_name, body=bulk_data, request_timeout=request_timeout)

    if response_content.get("errors"):
        warn_types = {"document_missing_exception"}  # these types represent bad data, not bad sweepers behaviour
        items_with_problems = [item for item in response_content["items"] if "error" in item["update"]]

        def get_ids_list_str(ids: List[str]) -> str:
            max_display_ids = 50
            ids_count = len(ids)
            if ids_count <= max_display_ids or log.isEnabledFor(logging.DEBUG):
                return str(ids)
            else:
                return f"{str(ids[:max_display_ids])} <list of {ids_count} ids truncated - enable DEBUG logging for full list>"

        if log.isEnabledFor(logging.WARNING):
            items_with_warnings = [
                item for item in items_with_problems if item["update"]["error"]["type"] in warn_types
            ]
            warning_aggregates = aggregate_update_error_types(items_with_warnings)
            for error_type, reason_aggregate in warning_aggregates.items():
                for error_reason, ids in reason_aggregate.items():
                    ids_str = get_ids_list_str(ids)
                    log.warning(
                        f"Attempt to update the following documents failed due to {error_type} ({error_reason}): {ids_str}"
                    )

        if log.isEnabledFor(logging.ERROR):
            items_with_errors = [
                item for item in items_with_problems if item["update"]["error"]["type"] not in warn_types
            ]
            error_aggregates = aggregate_update_error_types(items_with_errors)
            for error_type, reason_aggregate in error_aggregates.items():
                for error_reason, ids in reason_aggregate.items():
                    ids_str = get_ids_list_str(ids)
                    log.error(
                        f"Attempt to update the following documents failed unexpectedly due to {error_type} ({error_reason}): {ids_str}"
                    )
    else:
        log.info("Successfully wrote bulk update chunk")


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


def get_extant_lidvids(client: OpenSearch) -> Iterable[str]:
    """
    Given an OpenSearch host, return all extant LIDVIDs
    """

    log.info("Retrieving extant LIDVIDs")

    query = {
        "query": {"bool": {"must": [{"terms": {"ops:Tracking_Meta/ops:archive_status": ["archived", "certified"]}}]}}
    }
    _source = {"includes": ["lidvid"]}

    results = query_registry_db(client, query, _source, scroll_keepalive_minutes=1)

    return map(lambda doc: doc["_source"]["lidvid"], results)
