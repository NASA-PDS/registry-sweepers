import argparse
import logging
import sys
import time
from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Set

import opensearchpy.helpers
import requests
from opensearchpy import OpenSearch
from pds.registrysweepers.legacy_registry_sync.opensearch_loaded_product import get_already_loaded_lidvids
from pds.registrysweepers.legacy_registry_sync.solr_doc_export_to_opensearch import SolrOsWrapperIter
from pds.registrysweepers.legacy_registry_sync.solr_doc_export_to_opensearch import SYNC_RUN_ID_FIELD
from pds.registrysweepers.utils import configure_logging
from pds.registrysweepers.utils.db.client import get_opensearch_client_from_environment
from pds.registrysweepers.utils.misc import is_dev_mode
from pds.registrysweepers.utils.misc import limit_log_length
from solr_to_es.solrSource import SlowSolrDocs  # type: ignore

log = logging.getLogger(__name__)


SOLR_URL = "https://pds.nasa.gov/services/search/search"
OS_INDEX = "en-legacy-registry"
MAX_RETRIES = 5

# Stale documents are only pruned if the run wrote at least this fraction of the documents already in the index.
# This prevents a truncated or empty Solr response from wiping the index.
MIN_PRUNE_RATIO = 0.9
PRUNE_REQUEST_TIMEOUT_SECONDS = 3600
DRY_RUN_STALE_SAMPLE_SIZE = 20
# OpenSearch Serverless caps a page at 10,000 hits
EXISTING_IDS_PAGE_SIZE = 5000
EXISTING_IDS_REQUEST_TIMEOUT_SECONDS = 120


def get_online_resources() -> Dict[str, str]:
    """Get online resource from Solr."""
    online_resources = {}
    rows = 2000
    start = 0
    while True:
        log.debug("pull online resource from solr, starting at %i", start)
        response = requests.get(
            f"https://pds.nasa.gov/services/search/search?q=data_class:Resource&wt=json&qt=all&rows={rows}&start={start}"
        )
        docs = response.json()["response"]["docs"]
        for doc in docs:
            if "lid" in doc and "resource_url" in doc:
                online_resources[doc["lid"]] = doc["resource_url"][0]

        if len(docs) < rows:
            break

        start += rows

    return online_resources


def create_legacy_registry_index(es_conn: OpenSearch) -> None:
    """
    Creates if not already created the legacy_registry index.

    @param es_conn: elasticsearch.ElasticSearch instance for the ElasticSearch or OpenSearch connection
    @return:
    """
    if not es_conn.indices.exists(OS_INDEX):
        log.info("create index %s", OS_INDEX)
        es_conn.indices.create(index=OS_INDEX, body={})
    log.info("index created %s", OS_INDEX)


def iterate_existing_ids(client: OpenSearch, page_size: int = EXISTING_IDS_PAGE_SIZE) -> Iterator[str]:
    """
    Streams the ids of every document in the legacy_registry index, one page at a time.

    Pages with search_after sorted on _id, as the scroll API is not usable on OpenSearch Serverless. An empty index
    yields nothing.

    @param client: OpenSearch client from the opensearchpy library
    @param page_size: number of hits requested per page, at most 10,000
    """
    search_after: Optional[List[Any]] = None
    while True:
        body: Dict[str, Any] = {"query": {"match_all": {}}, "sort": [{"_id": "asc"}], "_source": False}
        if search_after is not None:
            body["search_after"] = search_after

        response = client.search(
            index=OS_INDEX, body=body, size=page_size, request_timeout=EXISTING_IDS_REQUEST_TIMEOUT_SECONDS
        )
        hits = response["hits"]["hits"]
        for hit in hits:
            yield hit["_id"]

        if len(hits) < page_size:
            return
        search_after = hits[-1]["sort"]


def prune_guard_passes(docs_written: int, docs_before: int) -> bool:
    """Whether a run which wrote docs_written documents to an index holding docs_before may prune stale documents."""
    return docs_written >= docs_before * MIN_PRUNE_RATIO


def prune_stale_documents(client: OpenSearch, run_id: int, docs_before: int, docs_written: int) -> int:
    """
    Deletes documents from the legacy_registry index which were not written by the given sync run, i.e. those which
    no longer exist in Solr.

    Pruning is skipped if the run wrote fewer than MIN_PRUNE_RATIO of the documents which were previously in the index.

    @param client: OpenSearch client from the opensearchpy library
    @param run_id: identifier of the sync run which has just completed
    @param docs_before: number of documents in the index before the run started
    @param docs_written: number of documents successfully written by the run
    @return: number of documents deleted
    """
    if not prune_guard_passes(docs_written, docs_before):
        log.error(
            "Skipping prune of %s: run wrote %d documents but index previously held %d (minimum ratio %.2f)",
            OS_INDEX,
            docs_written,
            docs_before,
            MIN_PRUNE_RATIO,
        )
        return 0

    # make the documents written by this run visible, so that superseded versions aren't mistaken for stale ones
    client.indices.refresh(index=OS_INDEX)

    # documents which predate the run id field match too, as they are missing the term
    query = {"query": {"bool": {"must_not": [{"term": {SYNC_RUN_ID_FIELD: run_id}}]}}}
    response = client.delete_by_query(
        index=OS_INDEX, body=query, conflicts="proceed", request_timeout=PRUNE_REQUEST_TIMEOUT_SECONDS
    )
    deleted = response.get("deleted", 0)
    log.info("Pruned %d stale documents from %s", deleted, OS_INDEX)
    return deleted


def build_solr_actions(client: OpenSearch, force: bool, run_id: int) -> SolrOsWrapperIter:
    """
    Builds the iterable of OpenSearch actions for every document in Solr. Reads from OpenSearch, never writes.
    """
    solr_itr = SlowSolrDocs(SOLR_URL, "*", rows=500)

    if force:
        prod_ids = {}
    else:
        prod_ids = get_already_loaded_lidvids(
            product_classes=["Product_Context", "Product_Collection", "Product_Bundle"], es_conn=client
        )

    online_resources = get_online_resources()

    return SolrOsWrapperIter(
        solr_itr, OS_INDEX, found_ids=prod_ids, online_resources=online_resources, force=force, run_id=run_id
    )


def preview_sync(client: OpenSearch, force: bool = False) -> Dict[str, Any]:
    """
    Reports what a sync run would write to and prune from the legacy_registry index, without modifying OpenSearch.
    Reads the full Solr index and the ids currently in the legacy_registry index.

    @param client: OpenSearch client from the opensearchpy library
    @param force: as for run()
    @return: dictionary of statistics about the planned run
    """
    existing_ids: Set[str] = set()
    if client.indices.exists(OS_INDEX):
        log.info("Reading existing document ids from %s", OS_INDEX)
        existing_ids = set(iterate_existing_ids(client))

    es_actions = build_solr_actions(client, force=force, run_id=int(time.time() * 1000))

    docs_to_write = 0
    solr_ids: Set[str] = set()
    for os_doc in es_actions:
        docs_to_write += 1
        solr_ids.add(os_doc["_id"])

    stale_ids = sorted(existing_ids - solr_ids)
    stats: Dict[str, Any] = {
        "docs_before": len(existing_ids),
        "docs_to_write": docs_to_write,
        "docs_to_add": len(solr_ids - existing_ids),
        "stale_ids": stale_ids,
        "prune_guard_passes": prune_guard_passes(docs_to_write, len(existing_ids)),
    }

    log.info("DRY RUN - no changes made to OpenSearch")
    log.info("Documents currently in %s: %d", OS_INDEX, stats["docs_before"])
    log.info("Documents which would be written: %d (%d new)", stats["docs_to_write"], stats["docs_to_add"])
    log.info("Documents which would be pruned: %d", len(stale_ids))
    for stale_id in stale_ids[:DRY_RUN_STALE_SAMPLE_SIZE]:
        log.info("  stale: %s", stale_id)
    if len(stale_ids) > DRY_RUN_STALE_SAMPLE_SIZE:
        log.info("  ... and %d more", len(stale_ids) - DRY_RUN_STALE_SAMPLE_SIZE)
    if not stats["prune_guard_passes"]:
        log.warning(
            "Prune would be SKIPPED: run would write %d documents but index holds %d (minimum ratio %.2f)",
            docs_to_write,
            len(existing_ids),
            MIN_PRUNE_RATIO,
        )

    return stats


def run(
    client: OpenSearch,
    log_filepath: Optional[str] = None,
    log_level: int = logging.INFO,
    force: bool = False,
    dry_run: bool = False,
) -> None:
    """
    Runs the Solr Legacy Registry synchronization with OpenSearch.

    @param client: OpenSearch client from the opensearchpy library
    @param log_filepath:
    @param log_level:
    @param force:
    @param dry_run: if True, only report what would be written and pruned. OpenSearch is read but never modified.
    @return:
    """

    configure_logging(filepath=log_filepath, log_level=log_level)

    if dry_run:
        preview_sync(client, force=force)
        return

    create_legacy_registry_index(es_conn=client)

    run_id = int(time.time() * 1000)
    docs_before = client.count(index=OS_INDEX)["count"]

    es_actions = build_solr_actions(client, force=force, run_id=run_id)
    dev_mode = is_dev_mode()

    docs_written = 0
    failure_count = 0
    for operation_successful, operation_info in opensearchpy.helpers.streaming_bulk(
        client, es_actions, chunk_size=50, max_chunk_bytes=50000000, max_retries=5, initial_backoff=10, timeout=120
    ):
        if operation_successful:
            docs_written += 1
        else:
            failure_count += 1
            log.error(limit_log_length(operation_info))

        if dev_mode:
            break

    if dev_mode:
        log.info("Skipping prune of stale documents in dev mode")
    elif failure_count > 0:
        log.error("Skipping prune of stale documents: %d documents failed to write", failure_count)
    else:
        prune_stale_documents(client, run_id=run_id, docs_before=docs_before, docs_written=docs_written)

    print(es_actions._seen_domains)
    print(es_actions._seen_node_ids)


def main() -> None:
    """Main entry point for the legacy registry sync console script."""
    parser = argparse.ArgumentParser(
        description="PDS Legacy Registry Sync - sync the legacy Solr registry to OpenSearch",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Report what would be written and pruned, without modifying OpenSearch
  %(prog)s --dry-run

  # Full sync to OpenSearch
  %(prog)s
  %(prog)s --force
  %(prog)s --force --log-file sync.log
        """,
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be written and pruned. OpenSearch is read but never modified",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite the node for documents that already have one set in the registry",
    )

    # Logging arguments
    parser.add_argument(
        "--log-file",
        help="Log file path (default: stdout)",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Log level (default: INFO)",
    )

    args = parser.parse_args()

    # Convert log level string to integer
    log_level = getattr(logging, args.log_level)

    try:
        client = get_opensearch_client_from_environment(verify_certs=not is_dev_mode())
        run(client=client, log_filepath=args.log_file, log_level=log_level, force=args.force, dry_run=args.dry_run)
    except KeyboardInterrupt:
        print("\nSync interrupted by user")
        sys.exit(1)


if __name__ == "__main__":
    main()
