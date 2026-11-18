"""
Ancestry orphan repair.

The ancestry sweeper reaches a non-aggregate product only through the registry-refs documents of a collection which is
not yet stamped with the current ancestry version.  A product which is loaded, overwritten or listed in refs after its
collection was swept therefore stays without ancestry.  This step runs after the ancestry sweeper and repairs such
products without re-harvesting their collections:

1. page (search_after) through the non-aggregate, non-superseded products which have no ancestor refs or a stamp below
   the current ancestry version
2. for each batch, look up the registry-refs documents which list the products (one terms query per batch)
3. keep the parent collections which exist in the registry (as the ancestry sweeper only ever processes those)
4. write exactly what the ancestry sweeper writes for such a product (ProductUpdateRecord -> update_from_record -> the
   same painless script) through write_updated_docs

A product whose parent cannot be confirmed by a refs document is never guessed: nothing is written to it (in particular
it is not stamped), so every later run examines it again and repairs it as soon as a refs document appears.

The repairs are written by default.  ``dry_run=True`` (``--dry-run`` on the command line) writes nothing: it lists the
products that would be repaired on stdout, followed by the summary.  ``output_file`` (``--output-file``) is an optional
additional destination for the same listing, with or without ``dry_run``.
"""
import logging
import sys
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Optional
from typing import Sequence
from typing import Set
from typing import TextIO

from opensearchpy import OpenSearch
from pds.registrysweepers.ancestry.productupdaterecord import ProductUpdateRecord
from pds.registrysweepers.ancestry.utils import update_from_record
from pds.registrysweepers.ancestry_orphan_repair.constants import AGGREGATE_PRODUCT_CLASSES
from pds.registrysweepers.ancestry_orphan_repair.constants import ANCESTOR_REFS_KEY
from pds.registrysweepers.ancestry_orphan_repair.constants import ANCESTRY_VERSION_KEY
from pds.registrysweepers.ancestry_orphan_repair.constants import COLLECTION_LOOKUP_CHUNK_SIZE
from pds.registrysweepers.ancestry_orphan_repair.constants import COLLECTION_PRODUCT_CLASS
from pds.registrysweepers.ancestry_orphan_repair.constants import CURRENT_ANCESTRY_VERSION
from pds.registrysweepers.ancestry_orphan_repair.constants import DEFAULT_BATCH_SIZE
from pds.registrysweepers.ancestry_orphan_repair.constants import LIDVID_FIELD
from pds.registrysweepers.ancestry_orphan_repair.constants import MAX_PAGE_SIZE
from pds.registrysweepers.ancestry_orphan_repair.constants import PRODUCT_CLASS_FIELD
from pds.registrysweepers.ancestry_orphan_repair.constants import REFS_BATCH_FIELD
from pds.registrysweepers.ancestry_orphan_repair.constants import REFS_COLLECTION_FIELD
from pds.registrysweepers.ancestry_orphan_repair.constants import REFS_PRODUCT_FIELD
from pds.registrysweepers.ancestry_orphan_repair.constants import SUPERSEDED_BY_KEY
from pds.registrysweepers.utils import configure_logging
from pds.registrysweepers.utils import parse_args
from pds.registrysweepers.utils.db import write_updated_docs
from pds.registrysweepers.utils.db.client import get_userpass_opensearch_client
from pds.registrysweepers.utils.db.indexing import ensure_index_mapping
from pds.registrysweepers.utils.db.multitenancy import resolve_multitenant_index_name
from pds.registrysweepers.utils.db.update import Update
from pds.registrysweepers.utils.misc import chunked
from pds.registrysweepers.utils.misc import coerce_list_type
from pds.registrysweepers.utils.misc import limit_log_length
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid
from retry.api import retry_call

log = logging.getLogger(__name__)

STATUS_REPAIRABLE = "repairable"
STATUS_NO_REFS_LISTING = "no_refs_listing"
STATUS_COLLECTION_NOT_IN_REGISTRY = "collection_not_in_registry"

# registry-refs documents each list many products, so a small page is warranted when reading them
REFS_PAGE_SIZE = 500

# log progress at INFO every this many examined products
PROGRESS_LOG_INTERVAL = 100_000

# per-document outcomes of a bulk update
WRITE_UPDATED = "updated"
WRITE_NOOP = "noop"
WRITE_MISSING = "missing"
WRITE_FAILED = "failed"


@dataclass
class OrphanRepairReport:
    """Counts describing one run.  Every examined product is counted in exactly one of the outcome categories."""

    dry_run: bool = False
    products_examined: int = 0
    invalid_lidvid: int = 0

    # a refs document lists the product under at least one collection which exists in the registry
    repairable: int = 0

    # unresolved: never stamped and never written, by design.  These are data problems for the node.
    no_refs_listing: int = 0
    listed_only_under_collection_not_in_registry: int = 0

    # write outcomes (not in a dry run), taken from the bulk responses
    repaired: int = 0
    already_current: int = 0
    vanished_before_write: int = 0
    write_failed: int = 0

    @property
    def unresolved(self) -> int:
        return self.no_refs_listing + self.listed_only_under_collection_not_in_registry

    def summary_lines(self) -> List[str]:
        lines = [
            "Ancestry orphan repair - " + ("DRY RUN (nothing was written)" if self.dry_run else "APPLIED"),
            f"  products examined (no ancestry, or ancestry stamp below v{CURRENT_ANCESTRY_VERSION}): {self.products_examined}",
            f"  repairable (a refs document lists the product under a collection in the registry): {self.repairable}",
        ]
        if not self.dry_run:
            lines += [
                f"  repaired: {self.repaired}",
                f"  already up to date (no change needed): {self.already_current}",
                f"  vanished between read and write: {self.vanished_before_write}",
                f"  write failures (see log): {self.write_failed}",
            ]
        lines += [
            f"  unresolved (not stamped, nothing written, examined again by every run): {self.unresolved}",
            f"    no refs document lists the product: {self.no_refs_listing}",
            f"    listed only under a collection which is not in the registry: "
            f"{self.listed_only_under_collection_not_in_registry}",
        ]
        if self.invalid_lidvid:
            lines.append(f"  skipped, unparseable lidvid: {self.invalid_lidvid}")
        if self.unresolved:
            lines.append(
                "  The unresolved products are data problems for the node (their collection inventory was never "
                "loaded or lists them incorrectly); they are repaired automatically once a refs document appears."
            )
        return lines

    def absorb_write_outcomes(self, outcomes: Dict[str, str]) -> None:
        values = list(outcomes.values())
        self.repaired = values.count(WRITE_UPDATED)
        self.already_current = values.count(WRITE_NOOP)
        self.vanished_before_write = values.count(WRITE_MISSING)
        self.write_failed = values.count(WRITE_FAILED)


class _BulkOutcomeRecorder:
    """
    Wraps the client handed to write_updated_docs() so that the per-document outcome of each bulk response can be
    reported, since write_updated_docs() itself only logs them.  Outcomes are keyed by document id, so a chunk which
    is resubmitted by the write path's retry is not counted twice.
    """

    def __init__(self, client: OpenSearch):
        self._client = client
        self.outcomes: Dict[str, str] = {}

    def bulk(self, *args, **kwargs):
        response = self._client.bulk(*args, **kwargs)
        for item in response.get("items", []):
            result = item.get("update", {})
            doc_id = result.get("_id")
            if doc_id is None:
                continue
            if "error" in result:
                error_type = str(result["error"].get("type", ""))
                self.outcomes[doc_id] = WRITE_MISSING if error_type.startswith("document_missing") else WRITE_FAILED
            elif result.get("result") == "noop":
                self.outcomes[doc_id] = WRITE_NOOP
            else:
                self.outcomes[doc_id] = WRITE_UPDATED
        return response

    def __getattr__(self, name: str):
        return getattr(self._client, name)


class _ListingSink:
    """
    Optional TSV of examined products (status, product lidvid, confirmed collection lidvids), written identically to
    every destination (stdout and/or a file).  Without destinations nothing is listed.
    """

    HEADER = "status\tproduct_lidvid\tcollection_lidvids\n"

    def __init__(self, handles: Sequence[TextIO], include_unresolved: bool):
        self._handles = list(handles)
        self._include_unresolved = include_unresolved
        for handle in self._handles:
            handle.write(self.HEADER)

    def record(self, status: str, product: PdsLidVid, collections: Iterable[str]) -> None:
        if not self._handles:
            return
        if status != STATUS_REPAIRABLE and not self._include_unresolved:
            return
        line = f"{status}\t{product}\t{','.join(sorted(collections))}\n"
        for handle in self._handles:
            handle.write(line)


def build_orphan_products_query(include_superseded: bool = False) -> Dict:
    """
    Non-aggregate products (and not superseded, unless requested) which have no ancestor refs or whose ancestry stamp is
    below the current version.
    """
    must_not: List[Dict[str, Any]] = [{"terms": {PRODUCT_CLASS_FIELD: AGGREGATE_PRODUCT_CLASSES}}]
    if not include_superseded:
        must_not.append({"exists": {"field": SUPERSEDED_BY_KEY}})

    lacks_ancestry = {
        "bool": {
            "should": [
                {"bool": {"must_not": [{"exists": {"field": ANCESTOR_REFS_KEY}}]}},
                {"bool": {"must_not": [{"range": {ANCESTRY_VERSION_KEY: {"gte": CURRENT_ANCESTRY_VERSION}}}]}},
            ],
            "minimum_should_match": 1,
        }
    }

    return {"query": {"bool": {"must_not": must_not, "filter": [lacks_ancestry]}}}


def iterate_pages_search_after(
    client: OpenSearch,
    index_name: str,
    query: Dict,
    source_includes: List[str],
    sort_fields: List[str],
    page_size: int,
    request_timeout_seconds: int = 60,
) -> Iterator[List[Dict]]:
    """
    Yield pages of hits using search_after (never from/size, and never a page larger than the 10,000 cap).

    Unlike query_registry_db_with_search_after(), termination does not depend on the total hit count, which shrinks
    while repairs are being written (repaired documents leave the result set) and would end the paging early.  A page
    shorter than page_size ends the iteration.
    """
    if not 1 <= page_size <= MAX_PAGE_SIZE:
        raise ValueError(f"page_size must be between 1 and {MAX_PAGE_SIZE} (got {page_size})")

    search_after: Optional[List] = None
    while True:
        body: Dict[str, Any] = dict(query)
        if search_after is not None:
            body["search_after"] = search_after

        results = retry_call(
            client.search,
            fkwargs={
                "index": index_name,
                "body": body,
                "size": page_size,
                "sort": sort_fields,
                "_source_includes": source_includes,
                "request_timeout": request_timeout_seconds,
            },
            tries=6,
            delay=2,
            backoff=2,
            logger=log,
        )

        hits = results["hits"]["hits"]
        if len(hits) == 0:
            return

        yield hits

        if len(hits) < page_size:
            return

        search_after = hits[-1]["sort"]


class _CollectionRegistry:
    """Caches which collection LIDVIDs exist in the registry, so each is looked up at most once per run."""

    def __init__(self, client: OpenSearch, registry_index: str):
        self._client = client
        self._registry_index = registry_index
        self._exists: Dict[str, bool] = {}

    def existing_among(self, collection_lidvids: Iterable[str]) -> Set[str]:
        wanted = set(collection_lidvids)
        uncached = sorted(c for c in wanted if c not in self._exists)

        for chunk in chunked(uncached, COLLECTION_LOOKUP_CHUNK_SIZE):
            query = {
                "query": {
                    "bool": {
                        "filter": [
                            {"term": {PRODUCT_CLASS_FIELD: COLLECTION_PRODUCT_CLASS}},
                            {"terms": {LIDVID_FIELD: chunk}},
                        ]
                    }
                }
            }
            found: Set[str] = set()
            # one more than the chunk, so that the single page is always a short one
            for page in iterate_pages_search_after(
                self._client, self._registry_index, query, [LIDVID_FIELD], [LIDVID_FIELD], len(chunk) + 1
            ):
                for hit in page:
                    found.add(hit["_source"][LIDVID_FIELD])

            for c in chunk:
                self._exists[c] = c in found

        return {c for c in wanted if self._exists[c]}


def find_listing_collections(client: OpenSearch, refs_index: str, product_lidvids: List[str]) -> Dict[str, Set[str]]:
    """
    One terms query (paged if it matches many refs documents) for the whole batch.  Returns, for each product listed by
    at least one refs document, the LIDVIDs of the collections whose refs documents list it.
    """
    wanted = set(product_lidvids)
    query = {"query": {"terms": {REFS_PRODUCT_FIELD: product_lidvids}}}
    listing: Dict[str, Set[str]] = {}

    for page in iterate_pages_search_after(
        client,
        refs_index,
        query,
        [REFS_COLLECTION_FIELD, REFS_BATCH_FIELD, REFS_PRODUCT_FIELD],
        [REFS_COLLECTION_FIELD, REFS_BATCH_FIELD],
        REFS_PAGE_SIZE,
    ):
        for hit in page:
            source = hit["_source"]
            collections = [c for c in coerce_list_type(source.get(REFS_COLLECTION_FIELD)) if c]
            listed = wanted.intersection(p for p in coerce_list_type(source.get(REFS_PRODUCT_FIELD)) if p)
            for product in listed:
                listing.setdefault(product, set()).update(collections)

    return listing


def _parse_collection_lidvids(product: PdsLidVid, collection_lidvids: Iterable[str]) -> List[PdsLidVid]:
    parsed = []
    for collection_lidvid in sorted(collection_lidvids):
        try:
            parsed.append(PdsLidVid.from_string(collection_lidvid))
        except ValueError as err:
            log.warning(limit_log_length(f"Ignoring unparseable collection lidvid for product {product}: {err}"))
    return parsed


def _generate_updates(
    client: OpenSearch,
    report: OrphanRepairReport,
    sink: _ListingSink,
    batch_size: int,
    include_superseded: bool,
) -> Iterator[Update]:
    """
    Stream the updates for repairable products.  Memory is bounded by one batch plus the collection cache; unresolved
    products are only counted.
    """
    registry_index = resolve_multitenant_index_name(client, "registry")
    refs_index = resolve_multitenant_index_name(client, "registry-refs")
    collections = _CollectionRegistry(client, registry_index)
    next_progress_log_at = PROGRESS_LOG_INTERVAL

    pages = iterate_pages_search_after(
        client,
        registry_index,
        build_orphan_products_query(include_superseded),
        [LIDVID_FIELD],
        [LIDVID_FIELD],
        batch_size,
    )
    for hits in pages:
        products: List[PdsLidVid] = []
        for hit in hits:
            report.products_examined += 1
            try:
                products.append(PdsLidVid.from_string(hit.get("_source", {}).get(LIDVID_FIELD) or hit["_id"]))
            except (ValueError, KeyError) as err:
                report.invalid_lidvid += 1
                log.warning(limit_log_length(f'Skipping document "{hit.get("_id")}" with unparseable lidvid: {err}'))

        listing = find_listing_collections(client, refs_index, [str(p) for p in products])
        existing = collections.existing_among(c for cs in listing.values() for c in cs)

        for product in products:
            listed_under = listing.get(str(product), set())
            confirmed = listed_under & existing

            if not listed_under:
                report.no_refs_listing += 1
                sink.record(STATUS_NO_REFS_LISTING, product, [])
                continue

            parent_collections = _parse_collection_lidvids(product, confirmed)
            if not parent_collections:
                report.listed_only_under_collection_not_in_registry += 1
                sink.record(STATUS_COLLECTION_NOT_IN_REGISTRY, product, listed_under)
                continue

            report.repairable += 1
            sink.record(STATUS_REPAIRABLE, product, confirmed)

            # identical to what the ancestry sweeper builds for a non-aggregate product of each of its collections
            record = ProductUpdateRecord(product=product, direct_ancestor_refs=parent_collections)
            yield update_from_record(record)

        if report.products_examined >= next_progress_log_at:
            next_progress_log_at += PROGRESS_LOG_INTERVAL
            log.info(
                limit_log_length(
                    f"Examined {report.products_examined} products: {report.repairable} repairable, "
                    f"{report.unresolved} unresolved"
                )
            )


def run(
    client: OpenSearch,
    log_filepath: Optional[str] = None,
    log_level: int = logging.INFO,
    dry_run: bool = False,
    output_file: Optional[str] = None,
    output_all: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    include_superseded: bool = False,
) -> OrphanRepairReport:
    """
    Repair non-aggregate products which lack ancestry where a registry-refs document confirms their parent collection.

    @param dry_run: write nothing; list the products that would be repaired on stdout as TSV, followed by the summary.
        Without it the repairs are written.
    @param output_file: optional path to which the same TSV listing (status, product, collections) is also written:
        the products that would be repaired with dry_run, the products that are repaired otherwise
    @param output_all: also list the unresolved products in the listing (stdout and output_file)
    @param batch_size: products examined and refs terms looked up per round trip (1..10,000)
    @param include_superseded: also repair superseded versions (they are skipped by default)
    """
    configure_logging(filepath=log_filepath, log_level=log_level)

    if not 1 <= batch_size <= MAX_PAGE_SIZE:
        raise ValueError(f"batch_size must be between 1 and {MAX_PAGE_SIZE} (got {batch_size})")

    mode = "dry run" if dry_run else "writing"
    log.info(limit_log_length(f"Starting ancestry orphan repair for ancestry v{CURRENT_ANCESTRY_VERSION} ({mode})..."))

    report = OrphanRepairReport(dry_run=dry_run)

    file_handle = open(output_file, "w", encoding="utf-8") if output_file else None
    try:
        # logging goes to stderr, so the listing and the summary on stdout stay parseable
        handles: List[TextIO] = [h for h in (sys.stdout if dry_run else None, file_handle) if h is not None]
        sink = _ListingSink(handles, include_unresolved=output_all)
        updates = _generate_updates(client, report, sink, batch_size, include_superseded)

        if not dry_run:
            # as in the ancestry sweeper, which has normally already ensured these
            registry_index = resolve_multitenant_index_name(client, "registry")
            for metadata_key in [ANCESTOR_REFS_KEY, ANCESTRY_VERSION_KEY]:
                ensure_index_mapping(client, registry_index, metadata_key, "keyword")

            recorder = _BulkOutcomeRecorder(client)
            write_updated_docs(recorder, updates, index_name=registry_index)  # type: ignore[arg-type]
            report.absorb_write_outcomes(recorder.outcomes)
        else:
            for _ in updates:
                pass
    finally:
        if file_handle is not None:
            file_handle.close()

    # The summary block is printed once as plain text (below for a dry run, by main() otherwise); the log gets a single
    # line, so that a run at INFO level does not repeat every line of the summary behind a timestamp.
    if dry_run:
        # the listing has been written; the summary comes last
        print("\n".join(report.summary_lines()), flush=True)
        outcome = f"{report.repairable} repairable"
    else:
        outcome = f"{report.repaired} repaired"
    log.info(
        limit_log_length(
            f"Orphan repair complete: {report.products_examined} examined, {outcome}, {report.unresolved} unresolved"
        )
    )

    return report


def _configure_cli(ap) -> None:
    ap.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="write nothing; print the products which would be repaired to stdout as TSV (status, product lidvid, "
        "collection lidvids), followed by the summary",
    )
    ap.add_argument(
        "--output-file",
        default=None,
        help="also write the listing of the products (the same TSV as --dry-run prints) to this file: the products "
        "which would be repaired with --dry-run, the products which are repaired otherwise",
    )
    ap.add_argument(
        "--output-all",
        action="store_true",
        default=False,
        help="also list the unresolved products (no refs listing / collection not in registry) in the listing "
        "(stdout with --dry-run, and --output-file)",
    )
    ap.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"products examined and refs terms looked up per round trip, 1..{MAX_PAGE_SIZE} [%(default)s]",
    )
    ap.add_argument(
        "--include-superseded",
        action="store_true",
        default=False,
        help="also repair superseded versions of products (skipped by default)",
    )


def main() -> None:
    cli_description = f"""
    Repair non-aggregate products which have no ancestry ({ANCESTOR_REFS_KEY}) or an ancestry stamp below v{CURRENT_ANCESTRY_VERSION}
    where a registry-refs document confirms their parent collection and that collection is in the registry.

    Writes exactly what the ancestry sweeper writes.  Products whose parent cannot be confirmed are reported and left
    untouched (not stamped).  Writes the repairs unless --dry-run is given, which lists the products to stdout
    instead.  Run it after the ancestry sweeper, with no other sweeper running on the node.
    """
    args = parse_args(description=cli_description, configure_parser=_configure_cli)
    client = get_userpass_opensearch_client(
        endpoint_url=args.base_URL, username=args.username, password=args.password, verify_certs=not args.insecure
    )

    report = run(
        client=client,
        log_level=args.log_level,
        log_filepath=args.log_file,
        dry_run=args.dry_run,
        output_file=args.output_file,
        output_all=args.output_all,
        batch_size=args.batch_size,
        include_superseded=args.include_superseded,
    )
    if not args.dry_run:  # with --dry-run, run() has already printed it after the listing
        print("\n".join(report.summary_lines()))


if __name__ == "__main__":
    main()
