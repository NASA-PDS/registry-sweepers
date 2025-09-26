import logging
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Union

import opensearchpy.helpers
import requests
from opensearchpy import OpenSearch
from pds.registrysweepers.legacy_registry_sync.opensearch_loaded_product import get_already_loaded_lidvids
from pds.registrysweepers.legacy_registry_sync.solr_doc_export_to_opensearch import SolrOsWrapperIter
from pds.registrysweepers.utils import configure_logging
from pds.registrysweepers.utils.misc import is_dev_mode
from pds.registrysweepers.utils.misc import limit_log_length
from solr_to_es.solrSource import SlowSolrDocs  # type: ignore

log = logging.getLogger(__name__)

SOLR_URL = "https://pds.nasa.gov/services/search/search"
OS_INDEX = "en-legacy-registry"
MAX_RETRIES = 5


def get_online_resources() -> Dict[str, str]:
    """Get online resource from Solr."""
    online_resources = {}
    rows = 2000
    start = 0
    while True:
        log.info("pull online resource from solr, starting at %i", start)
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


def create_legacy_registry_index(es_conn=None):
    """
    Creates if not already created the legacy_registry index.

    @param es_conn: elasticsearch.ElasticSearch instance for the ElasticSearch or OpenSearch connection
    @return:
    """
    if not es_conn.indices.exists(OS_INDEX):
        log.info("create index %s", OS_INDEX)
        es_conn.indices.create(index=OS_INDEX, body={})
    log.info("index created %s", OS_INDEX)


def run(
    client: OpenSearch,
    log_filepath: Union[str, None] = None,
    log_level: int = logging.INFO,
):
    """
    Runs the Solr Legacy Registry synchronization with OpenSearch.

    @param client: OpenSearch client from the opensearchpy library
    @param log_filepath:
    @param log_level:
    @return:
    """

    configure_logging(filepath=log_filepath, log_level=log_level)

    solr_itr = SlowSolrDocs(SOLR_URL, "*", rows=500)

    create_legacy_registry_index(es_conn=client)

    prod_ids = get_already_loaded_lidvids(
        product_classes=["Product_Context", "Product_Collection", "Product_Bundle"], es_conn=client
    )

    online_resources = get_online_resources()

    es_actions = SolrOsWrapperIter(solr_itr, OS_INDEX, found_ids=prod_ids, online_resources=online_resources)
    dev_mode = is_dev_mode()

    for operation_successful, operation_info in opensearchpy.helpers.streaming_bulk(
        client, es_actions, chunk_size=50, max_chunk_bytes=50000000, max_retries=5, initial_backoff=10, timeout=120
    ):
        if not operation_successful:
            log.error(limit_log_length(operation_info))

        if dev_mode:
            break

    print(es_actions._seen_domains)
    print(es_actions._seen_node_ids)


def dry_run(
    log_filepath: Union[str, None] = None,
    log_level: int = logging.INFO,
    max_docs: Optional[int] = None,
    show_sample_docs: bool = True,
    sample_size: int = 5,
) -> Dict[str, Any]:
    """
    Performs a dry run of the Solr Legacy Registry synchronization without interacting with OpenSearch.
    This function only retrieves data from Solr and shows what would be processed.

    @param log_filepath: Path to log file (default: stdout)
    @param log_level: Logging level
    @param max_docs: Maximum number of documents to process (default: None for all)
    @param show_sample_docs: Whether to show sample documents (default: True)
    @param sample_size: Number of sample documents to show (default: 5)
    @return: Dictionary with statistics about the dry run
    """

    configure_logging(filepath=log_filepath, log_level=log_level)

    log.info("Starting dry run - Solr data retrieval only")
    log.info("=" * 60)

    # Get online resources from Solr
    log.info("Retrieving online resources from Solr...")
    online_resources = get_online_resources()
    log.info("Retrieved %d online resources", len(online_resources))

    # Initialize Solr iterator
    log.info("Initializing Solr document iterator...")
    solr_itr = SlowSolrDocs(SOLR_URL, "*", rows=500)

    # Statistics tracking
    stats: Dict[str, Any] = {
        "total_docs": 0,
        "docs_with_lidvid": 0,
        "docs_without_lidvid": 0,
        "seen_domains": set(),
        "seen_node_ids": set(),
        "node_distribution": {},
        "product_class_distribution": {},
        "node_by_product_class": {},
        "sample_docs": [],
        "errors": [],
    }

    # Use the existing SolrOsWrapperIter to analyze documents without OpenSearch
    # We pass empty found_ids since we're not checking against OpenSearch
    wrapper = SolrOsWrapperIter(solr_itr, OS_INDEX, found_ids=set(), online_resources=online_resources)

    try:
        for i, os_doc in enumerate(wrapper):
            if max_docs and i >= max_docs:
                log.info("Reached maximum document limit: %d", max_docs)
                break

            # Extract the original Solr document from the OpenSearch document
            solr_doc = os_doc["_source"]

            # Analyze the document
            stats["total_docs"] += 1

            # Check for lidvid
            if "lidvid" in solr_doc:
                stats["docs_with_lidvid"] += 1
            else:
                stats["docs_without_lidvid"] += 1

            # Get node assignment (already computed by SolrOsWrapperIter)
            node = solr_doc.get("node", "UNK")
            stats["node_distribution"][node] = stats["node_distribution"].get(node, 0) + 1

            # Track product class distribution
            product_class = "Unknown"
            if "product_class" in solr_doc:
                product_class = (
                    solr_doc["product_class"][0]
                    if isinstance(solr_doc["product_class"], list)
                    else solr_doc["product_class"]
                )
                stats["product_class_distribution"][product_class] = (
                    stats["product_class_distribution"].get(product_class, 0) + 1
                )

            # Track node by product class breakdown
            if node not in stats["node_by_product_class"]:
                stats["node_by_product_class"][node] = {}
            stats["node_by_product_class"][node][product_class] = (
                stats["node_by_product_class"][node].get(product_class, 0) + 1
            )

            # Collect sample documents
            if show_sample_docs and len(stats["sample_docs"]) < sample_size:
                sample_doc = {
                    "lid": solr_doc.get("lid", "N/A"),
                    "lidvid": solr_doc.get("lidvid", "N/A"),
                    "product_class": solr_doc.get("product_class", ["N/A"])[0]
                    if isinstance(solr_doc.get("product_class"), list)
                    else solr_doc.get("product_class", "N/A"),
                    "node": node,
                    "resource_url": solr_doc.get("resource_url", ["N/A"])[0]
                    if isinstance(solr_doc.get("resource_url"), list)
                    else solr_doc.get("resource_url", "N/A"),
                    "modification_date": solr_doc.get("modification_date", ["N/A"])[0]
                    if isinstance(solr_doc.get("modification_date"), list)
                    else solr_doc.get("modification_date", "N/A"),
                }
                stats["sample_docs"].append(sample_doc)

            # Update seen domains and node IDs from the wrapper
            stats["seen_domains"].update(wrapper._seen_domains)
            stats["seen_node_ids"].update(wrapper._seen_node_ids)

            if i % 1000 == 0 and i > 0:
                log.info("Processed %d documents...", i)

    except StopIteration:
        log.info("Finished processing all documents from Solr")
    except Exception as e:
        log.error("Error during dry run: %s", str(e))
        stats["errors"].append(str(e))

    # Print results
    log.info("=" * 60)
    log.info("DRY RUN RESULTS")
    log.info("=" * 60)
    log.info("Total documents processed: %d", stats["total_docs"])
    log.info("Documents with lidvid: %d", stats["docs_with_lidvid"])
    log.info("Documents without lidvid: %d", stats["docs_without_lidvid"])
    log.info("Online resources retrieved: %d", len(online_resources))

    log.info("\nNode Distribution:")
    for node, count in sorted(stats["node_distribution"].items(), key=lambda x: x[1], reverse=True):
        log.info("  %s: %d documents", node, count)

    log.info("\nProduct Class Distribution (top 10):")
    sorted_classes = sorted(stats["product_class_distribution"].items(), key=lambda x: x[1], reverse=True)
    for product_class, count in sorted_classes[:10]:
        log.info("  %s: %d documents", product_class, count)

    log.info("\nSeen Domains (%d total):", len(stats["seen_domains"]))
    for domain in sorted(stats["seen_domains"]):
        log.info("  %s", domain)

    log.info("\nSeen Node IDs (%d total):", len(stats["seen_node_ids"]))
    for node_id in sorted(stats["seen_node_ids"]):
        log.info("  %s", node_id)

    if show_sample_docs and stats["sample_docs"]:
        log.info("\nSample Documents:")
        for i, doc in enumerate(stats["sample_docs"], 1):
            log.info("  Sample %d:", i)
            log.info("    LID: %s", doc["lid"])
            log.info("    LIDVID: %s", doc["lidvid"])
            log.info("    Product Class: %s", doc["product_class"])
            log.info("    Assigned Node: %s", doc["node"])
            log.info("    Resource URL: %s", doc["resource_url"])
            log.info("    Modification Date: %s", doc["modification_date"])
            log.info("")

    if stats["errors"]:
        log.info("\nErrors encountered:")
        for error in stats["errors"]:
            log.info("  %s", error)

    log.info("=" * 60)
    log.info("Dry run completed successfully")

    return stats


def main():
    """Main entry point for the legacy registry sync console script."""
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        description="PDS Legacy Registry Sync - Test Solr data retrieval or sync to OpenSearch",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run - test Solr data retrieval only
  %(prog)s --dry-run --max-docs 10
  %(prog)s --dry-run --max-docs 100 --log-file dry_run.log

  # Dry run without showing sample documents
  %(prog)s --dry-run --max-docs 50 --no-samples

  # Dry run with more sample documents
  %(prog)s --dry-run --max-docs 20 --sample-size 10
        """,
    )

    # Dry run flag
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform dry run - only interact with Solr, no OpenSearch operations",
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

    # Dry run arguments
    parser.add_argument(
        "--max-docs",
        type=int,
        help="Maximum number of documents to process (default: process all)",
    )
    parser.add_argument(
        "--no-samples",
        action="store_true",
        help="Don't show sample documents",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=5,
        help="Number of sample documents to show (default: 5)",
    )

    args = parser.parse_args()

    # Check if dry-run flag is provided
    if not args.dry_run:
        print("PDS Legacy Registry Sync")
        print("=" * 40)
        print("ERROR: This console script currently only supports dry-run mode.")
        print("")
        print("To test Solr data retrieval, please use:")
        print("  %s --dry-run [options]" % sys.argv[0])
        print("")
        print("Full end-to-end OpenSearch synchronization via console script")
        print("has not been implemented yet. Use the run() function directly")
        print("in your Python code for full synchronization.")
        print("")
        print("For help with dry-run options:")
        print("  %s --dry-run --help" % sys.argv[0])
        sys.exit(1)

    # Convert log level string to integer
    import logging

    log_level = getattr(logging, args.log_level)

    # Run dry-run mode
    try:
        print("Starting PDS Legacy Registry Sync - DRY RUN MODE")
        print("=" * 60)
        print("This will only interact with Solr - no OpenSearch operations")
        print("=" * 60)

        stats = dry_run(
            log_filepath=args.log_file,
            log_level=log_level,
            max_docs=args.max_docs,
            show_sample_docs=not args.no_samples,
            sample_size=args.sample_size,
        )

        print("\n" + "=" * 60)
        print("DRY RUN SUMMARY")
        print("=" * 60)
        print(f"Total documents processed: {stats['total_docs']}")
        print(f"Documents with lidvid: {stats['docs_with_lidvid']}")
        print(f"Documents without lidvid: {stats['docs_without_lidvid']}")
        print(f"Unique domains found: {len(stats['seen_domains'])}")
        print(f"Unique node IDs found: {len(stats['seen_node_ids'])}")
        print(f"Node distribution: {len(stats['node_distribution'])} different nodes")
        print(f"Product classes found: {len(stats['product_class_distribution'])} different classes")

        # Show node distribution details
        if stats["node_distribution"]:
            print("\nNode Distribution:")
            for node, count in sorted(stats["node_distribution"].items(), key=lambda x: x[1], reverse=True):
                print(f"  {node}: {count} documents")

        # Show node distribution by product class
        if stats["node_by_product_class"]:
            print("\nNode Distribution by Product Class:")
            for node in sorted(
                stats["node_distribution"].keys(), key=lambda x: stats["node_distribution"][x], reverse=True
            ):
                node_total = stats["node_distribution"][node]
                print(f"\n  {node} ({node_total} total documents):")

                # Sort product classes by count within this node
                node_classes = stats["node_by_product_class"][node]
                sorted_classes = sorted(node_classes.items(), key=lambda x: x[1], reverse=True)

                for product_class, count in sorted_classes:
                    percentage = (count / node_total) * 100
                    print(f"    {product_class}: {count} documents ({percentage:.1f}%)")

        # Show product class distribution details (top 10)
        if stats["product_class_distribution"]:
            print("\nProduct Class Distribution (top 10):")
            sorted_classes = sorted(stats["product_class_distribution"].items(), key=lambda x: x[1], reverse=True)
            for product_class, count in sorted_classes[:10]:
                print(f"  {product_class}: {count} documents")

        if stats["errors"]:
            print(f"Errors encountered: {len(stats['errors'])}")
            for error in stats["errors"]:
                print(f"  - {error}")

        print("\nDry run completed successfully!")

    except KeyboardInterrupt:
        print("\nDry run interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nDry run failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
