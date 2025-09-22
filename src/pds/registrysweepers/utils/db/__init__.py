import json
import logging
import math
import sys
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Mapping
from typing import Optional
from typing import Union

from opensearchpy import OpenSearch
from pds.registrysweepers.utils.db.update import Update
from pds.registrysweepers.utils.misc import get_ids_list_str
from pds.registrysweepers.utils.misc import get_random_hex_id
from pds.registrysweepers.utils.misc import limit_log_length
from retry import retry
from retry.api import retry_call
from tqdm import tqdm

log = logging.getLogger(__name__)


def query_registry_db_with_scroll(
    client: OpenSearch,
    index_name: str,
    query: Dict,
    _source: Dict,
    page_size: int = 10000,
    scroll_keepalive_minutes: int = 10,
    request_timeout_seconds: int = 20,
) -> Iterable[Dict]:
    """
    Given an OpenSearch client and query/_source, return an iterable collection of hits

    Example query: {"query: {"bool": {"must": [{"terms": {"ops:Tracking_Meta/ops:archive_status": ["archived", "certified"]}}]}}}
    Example _source: {"includes": ["lidvid"]}
    """

    scroll_keepalive = f"{scroll_keepalive_minutes}m"
    query_id = get_random_hex_id()  # This is just used to differentiate queries during logging
    log.debug(limit_log_length(f"Initiating query (id {query_id}) of index {index_name}: {json.dumps(query)}"))

    served_hits = 0

    last_info_log_at_percentage = 0
    log.debug(limit_log_length(f"Query {query_id} progress: 0%"))

    more_data_exists = True
    scroll_id = None
    while more_data_exists:
        if scroll_id is None:

            def fetch_func():
                return client.search(
                    index=index_name,
                    body=query,
                    scroll=scroll_keepalive,
                    request_timeout=request_timeout_seconds,
                    size=page_size,
                    _source_includes=_source.get("includes", []),  # TODO: Break out from the enclosing _source object
                    _source_excludes=_source.get("excludes", []),  # TODO: Break out from the enclosing _source object
                )

        else:

            def fetch_func(_scroll_id: str = scroll_id):
                return client.scroll(
                    scroll_id=_scroll_id, scroll=scroll_keepalive, request_timeout=request_timeout_seconds
                )

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
            log.debug(limit_log_length(f"Query {query_id} returns {total_hits} total hits"))

        response_hits = results["hits"]["hits"]
        for hit in response_hits:
            served_hits += 1

            percentage_of_hits_served = int(served_hits / total_hits * 100)
            if last_info_log_at_percentage is None or percentage_of_hits_served >= (last_info_log_at_percentage + 5):
                last_info_log_at_percentage = percentage_of_hits_served
                log.debug(limit_log_length(f"Query {query_id} progress: {percentage_of_hits_served}%"))

            yield hit

        # This is a temporary, ad-hoc guard against empty/erroneous responses which do not return non-200 status codes.
        # Previously, this has cause infinite loops in production due to served_hits sticking and never reaching the
        # expected total hits value.
        # TODO: Remove this upon implementation of https://github.com/NASA-PDS/registry-sweepers/issues/42
        hits_data_present_in_response = len(response_hits) > 0
        if not hits_data_present_in_response and served_hits < total_hits:
            log.error(
                limit_log_length(
                    f"Response for query {query_id} contained no hits when hits were expected.  Returned data is incomplete (got {served_hits} of {total_hits} total hits).  Response was: {results}"
                )
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

    log.debug(limit_log_length(f"Query {query_id} complete!"))


def query_registry_db_with_search_after(
    client: OpenSearch,
    index_name: str,
    query: Dict,
    _source: Dict,
    page_size: int = 5000,
    limit: Union[int, None] = None,
    sort_fields: Union[List[str], None] = None,
    request_timeout_seconds: int = 20,
) -> Iterable[Dict]:
    """
    Given an OpenSearch client and query/_source, return an iterable collection of hits

    Example query: {"query: {"bool": {"must": [{"terms": {"ops:Tracking_Meta/ops:archive_status": ["archived", "certified"]}}]}}}
    Example _source: {"includes": ["lidvid"]}
    """

    # Use 'lidvid' by default, though this may cause problems later as it may not exist for all documents - edunn 20230105
    # it is not recommended to use _id per https://www.elastic.co/guide/en/elasticsearch/reference/6.8/search-request-search-after.html
    # (see first !IMPORTANT! note)
    sort_fields = sort_fields or ["lidvid"]

    # TODO: stop accepted {'query': <content>} and start accepting just <content> itself, to prevent the need for this guard
    if "search_after" in query.keys():
        log.error(
            limit_log_length(
                f'Provided query object contains "search_after" content when none should exist - was a dict object reused?: got {query}.'
            )
        )
        log.info(limit_log_length("Discarding erroneous search_after values."))
        query.pop("search_after")

    query_id = get_random_hex_id()  # This is just used to differentiate queries during logging

    served_hits = 0
    current_page = 1
    more_data_exists = True
    search_after_values: Union[List, None] = None
    expected_pages = None
    total_hits = get_query_hits_count(client, index_name, query)
    expected_hits = limit if limit is not None else total_hits
    limit_log_msg_part = f" (limited to {expected_hits} hits)" if limit is not None else ""
    log.debug(
        limit_log_length(f"Query {query_id} returns {total_hits} total hits{limit_log_msg_part}: {json.dumps(query)}")
    )

    with tqdm(total=expected_hits, desc=f"Query {query_id}") as pbar:
        while more_data_exists:
            # Manually set sort - this is required for subsequent calls, despite being passed in fetch_func's call to
            # client.search as sort kwarg.
            # It is unclear why this issue is only presenting now - edunn 20241023
            # It appears that OpenSearch.search() sort kwarg behaves inconsistently if the values contain certain
            #  characters.  It is unclear which of /: is the issue but it is suggested that :-+^ may be problematic - edunn 20241105
            #  Related: https://discuss.elastic.co/t/query-a-field-that-has-a-colon/323966
            #           https://discuss.elastic.co/t/problem-with-colon-in-fieldname-where-can-i-find-naming-guidelines/5437/4
            #           https://discuss.elastic.co/t/revisiting-colons-in-field-names/25005
            # TODO: investigate and open ticket with opensearch-py if confirmed
            special_characters = {"/", ":"}
            query["sort"] = [f for f in sort_fields if any(c in f for c in special_characters)]

            if search_after_values is not None:
                query["search_after"] = search_after_values
                log.debug(
                    limit_log_length(
                        f"Query {query_id} paging {page_size} hits (page {current_page} of {expected_pages}) with sort fields {sort_fields} and search-after values {search_after_values}"
                    )
                )

            def fetch_func():
                return client.search(
                    index=index_name,
                    body=query,
                    request_timeout=request_timeout_seconds,
                    size=page_size,
                    sort=sort_fields,
                    _source_includes=_source.get("includes", []),  # TODO: Break out from the enclosing _source object
                    _source_excludes=_source.get("excludes", []),  # TODO: Break out from the enclosing _source object,
                    track_total_hits=True,
                )

            results = retry_call(
                fetch_func,
                tries=6,
                delay=2,
                backoff=2,
                logger=log,
            )

            total_hits = results["hits"]["total"]["value"]
            current_page += 1
            expected_pages = math.ceil(total_hits / page_size)

            response_hits = results["hits"]["hits"]
            for hit in response_hits:
                served_hits += 1
                pbar.update()
                yield hit

                if limit is not None and served_hits >= limit:
                    log.debug(limit_log_length(f"Query {query_id} complete! (limit of {expected_hits} hits reached)"))
                    return

                # simpler to set the value after every hit than worry about OBO errors detecting the last hit in the page
                search_after_values = [hit["_source"].get(field) for field in sort_fields]

            # Flatten single-element search-after-values.  Attempting to sort/search-after on MCP AOSS by
            # ops:Harvest_Info/ops:harvest_date_time is throwing
            #     RequestError(400, 'parsing_exception', 'Expected [VALUE_STRING] or [VALUE_NUMBER] or
            #     [VALUE_BOOLEAN] or [VALUE_NULL] but found [START_ARRAY] inside search_after.')
            # It is unclear why this issue is only presenting now - edunn 20241023
            if search_after_values is not None:
                for idx, value in enumerate(search_after_values):
                    if isinstance(value, list):
                        if len(value) == 1:
                            search_after_values[idx] = value[0]
                        else:
                            raise ValueError(
                                f"Failed to flatten array-like search-after value {value} into single element"
                            )

            # This is a temporary, ad-hoc guard against empty/erroneous responses which do not return non-200 status codes.
            # Previously, this has cause infinite loops in production due to served_hits sticking and never reaching the
            # expected total hits value.
            # TODO: Remove this upon implementation of https://github.com/NASA-PDS/registry-sweepers/issues/42
            hits_data_present_in_response = len(response_hits) > 0
            if not hits_data_present_in_response and served_hits < total_hits:
                log.error(
                    limit_log_length(
                        f"Response for query {query_id} contained no hits when hits were expected.  Returned data is incomplete (got {served_hits} of {total_hits} total hits).  Response was: {results}"
                    )
                )
                break

            more_data_exists = served_hits < results["hits"]["total"]["value"]

    log.debug(limit_log_length(f"Query {query_id} complete!"))


def query_registry_db_or_mock(
    mock_f: Optional[Callable[[str], Iterable[Dict]]],
    mock_query_id: str,
    use_search_after: bool = True,
):
    if mock_f is not None:

        def mock_wrapper(
            client: OpenSearch,
            index_name: str,
            query: Dict,
            _source: Dict,
            page_size: int = 10000,
            scroll_validity_duration_minutes: int = 10,
            request_timeout_seconds: int = 20,
            sort_fields: Union[List[str], None] = None,
        ) -> Iterable[Dict]:
            return mock_f(mock_query_id)  # type: ignore  # see None-check above

        return mock_wrapper
    elif use_search_after:
        return query_registry_db_with_search_after
    else:
        return query_registry_db_with_scroll


def write_updated_docs(
    client: OpenSearch,
    updates: Iterable[Update],
    index_name: str,
    bulk_chunk_max_update_count: Union[int, None] = None,
    as_upsert: bool = False,
):
    log.info(limit_log_length("Writing document updates..."))
    buffered_updates_count = 0
    updated_doc_count = 0
    total_writes_skipped = 0

    bulk_buffer_max_size_mb = 30.0
    bulk_buffer_size_mb = 0.0
    bulk_updates_buffer: List[str] = []
    writes_skipped_since_flush = 0
    for update in updates:
        if update.skip_write is True:
            total_writes_skipped += 1
            writes_skipped_since_flush += 1
            continue

        buffered_updates_count = len(bulk_updates_buffer) // 2
        updates_processed_since_flush = buffered_updates_count + writes_skipped_since_flush
        buffer_at_size_threshold = bulk_buffer_size_mb >= bulk_buffer_max_size_mb
        buffer_at_update_count_threshold = (
            bulk_chunk_max_update_count is not None and updates_processed_since_flush >= bulk_chunk_max_update_count
        )
        flush_threshold_reached = buffer_at_size_threshold or buffer_at_update_count_threshold
        threshold_log_str = (
            f"{bulk_buffer_max_size_mb}MB"
            if buffer_at_size_threshold
            else f"{bulk_chunk_max_update_count}docs (including {writes_skipped_since_flush} which will be skipped)"
        )

        if flush_threshold_reached:
            log.debug(
                limit_log_length(
                    f"Bulk update buffer has reached {threshold_log_str} threshold - writing {buffered_updates_count} document updates to db..."
                )
            )
            _write_bulk_updates_chunk(client, index_name, bulk_updates_buffer)
            bulk_updates_buffer = []
            bulk_buffer_size_mb = 0.0
            writes_skipped_since_flush = 0

        update_statement_strs = update_as_statements(update, as_upsert=as_upsert)

        for s in update_statement_strs:
            bulk_buffer_size_mb += sys.getsizeof(s) / 1024**2

        bulk_updates_buffer.extend(update_statement_strs)
        updated_doc_count += 1

    if buffered_updates_count > 0:
        log.debug(
            limit_log_length(f"Writing documents updates for {buffered_updates_count} remaining products to db...")
        )
        _write_bulk_updates_chunk(client, index_name, bulk_updates_buffer)

    log.info(
        limit_log_length(
            f"Wrote document updates for {updated_doc_count} products and skipped {total_writes_skipped} doc updates"
        )
    )


def update_as_statements(update: Update, as_upsert: bool = False) -> Iterable[str]:
    """
    Given an Update, convert it to an ElasticSearch-style set of request body content strings
    Optionally, specify as upsert (index if does not already exist)
    """
    metadata_statement: Dict[str, Any] = {"update": {"_id": update.id}}
    if update.has_versioning_information():
        metadata_statement["if_primary_term"] = update.primary_term
        metadata_statement["if_seq_no"] = update.seq_no
    content_statement = {"doc": update.content, "doc_as_upsert": as_upsert}
    update_objs = [metadata_statement, content_statement]
    updates_strs = [json.dumps(obj) for obj in update_objs]
    return updates_strs


@retry(tries=6, delay=15, backoff=2, logger=log)
def _write_bulk_updates_chunk(client: OpenSearch, index_name: str, bulk_updates: List[str]):
    if len(bulk_updates) == 0:
        log.debug(limit_log_length("_write_bulk_updates_chunk received empty arg bulk_updates - skipping"))

    bulk_data = "\n".join(bulk_updates) + "\n"

    request_timeout = 180
    response_content = client.bulk(index=index_name, body=bulk_data, request_timeout=request_timeout)

    if response_content.get("errors"):
        warn_types = {
            "document_missing_exception",
            "document_missing_in_index_exception",
        }  # these types represent bad data, not bad sweepers behaviour
        items_with_problems = [item for item in response_content["items"] if "error" in item["update"]]
        if any(
            item["update"]["status"] == 429 and item["update"]["error"]["type"] == "circuit_breaking_exception"
            for item in items_with_problems
        ):
            raise RuntimeWarning(
                "Bulk updates response includes item with status HTTP429, circuit_breaking_exception/throttled - chunk will need to be resubmitted"
            )

        if log.isEnabledFor(logging.WARNING):
            items_with_warnings = [
                item for item in items_with_problems if item["update"]["error"]["type"] in warn_types
            ]
            warning_aggregates = aggregate_update_error_types(items_with_warnings)
            for error_type, reason_aggregate in warning_aggregates.items():
                for error_reason, ids in reason_aggregate.items():
                    ids_str = get_ids_list_str(ids)
                    log.warning(
                        limit_log_length(
                            f"Attempt to update the following {len(ids)} documents failed due to {error_type} ({error_reason}): {ids_str}"
                        )
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
                        limit_log_length(
                            f"Attempt to update the following {len(ids)} documents failed unexpectedly due to {error_type} ({error_reason}): {ids_str}"
                        )
                    )
    else:
        log.debug(limit_log_length("Successfully wrote bulk update chunk"))


def aggregate_update_error_types(items: Iterable[Dict]) -> Mapping[str, Dict[str, List[str]]]:
    """Return a nested aggregation of ids, aggregated first by error type, then by reason"""
    agg: Dict[str, Dict[str, List[str]]] = {}
    for item in items:
        id = item["update"]["_id"]
        error = item["update"]["error"]
        raw_error_type = error["type"]
        document_missing_type_str = "document_missing_in_index_exception"
        error_type = (
            document_missing_type_str if str(raw_error_type).startswith(document_missing_type_str) else raw_error_type
        )
        error_reason = error["reason"] if error["type"] != document_missing_type_str else ""
        if error_type not in agg:
            agg[error_type] = {}

        if error_reason not in agg[error_type]:
            agg[error_type][error_reason] = []

        agg[error_type][error_reason].append(id)

    return agg


@retry(tries=6, delay=15, backoff=2, logger=log)
def get_query_hits_count(client: OpenSearch, index_name: str, query: Dict) -> int:
    response = client.search(index=index_name, body=query, size=0, _source_includes=[], track_total_hits=True)

    return response["hits"]["total"]["value"]
