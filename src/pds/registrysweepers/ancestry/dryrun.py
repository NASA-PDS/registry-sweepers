"""
Dry run of the ancestry sweeper.

The dry run reads everything the real run reads and generates every update the real run would write, but nothing is
sent to the registry: the client it is given is a ReadOnlyOpenSearchClient, and this module does not import the
functions which write.  Updates are counted as they stream by, so memory use does not grow with the number of
non-aggregate products.
"""
import logging
from dataclasses import dataclass
from typing import Iterable
from typing import List
from typing import Optional
from typing import TextIO

from pds.registrysweepers.ancestry.constants import ANCESTRY_REFS_METADATA_KEY
from pds.registrysweepers.ancestry.productupdaterecord import ProductUpdateRecord
from pds.registrysweepers.ancestry.productupdaterecord import UpdateKind
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY
from pds.registrysweepers.utils.db.indexing import _get_existing_mapping_type
from pds.registrysweepers.utils.db.multitenancy import resolve_multitenant_index_name
from pds.registrysweepers.utils.misc import limit_log_length

log = logging.getLogger(__name__)

LINKS_FILE_HEADER = "collection_lidvid\tbundle_lidvid_added"


@dataclass
class AncestryDryRunReport:
    """Counts of the document updates a real run would write, by kind.  Nothing was written."""

    pending_collections: int = 0
    collection_completions: int = 0
    bundles: int = 0
    stamped_collections_linked: int = 0
    stamped_collection_links: int = 0
    nonaggregates: int = 0
    unclassified: int = 0
    mapping_lines: Optional[List[str]] = None

    @property
    def total_updates(self) -> int:
        return (
            self.pending_collections
            + self.collection_completions
            + self.bundles
            + self.stamped_collections_linked
            + self.nonaggregates
            + self.unclassified
        )

    def summary_lines(self) -> List[str]:
        lines = [
            f"Ancestry sweeper v{SWEEPERS_ANCESTRY_VERSION} - DRY RUN (nothing was written to the registry)",
            f"  document updates a real run would write: {self.total_updates}",
            f"    pending collections (collection documents, bundle ancestry): {self.pending_collections}",
            f"    pending collections marked complete (second update, after their non-aggregates): "
            f"{self.collection_completions}",
            f"    bundles: {self.bundles}",
            f"    collections which already carry v{SWEEPERS_ANCESTRY_VERSION} and only get a bundle link added "
            f"(#301): {self.stamped_collections_linked} ({self.stamped_collection_links} bundle links)",
            f"    non-aggregate products: {self.nonaggregates}",
        ]
        if self.unclassified:
            lines.append(f"    updates of unknown kind: {self.unclassified}")
        lines.extend(self.mapping_lines or [])
        return lines


def _bundle_lidvids_added(record: ProductUpdateRecord) -> List[str]:
    return sorted(str(lidvid) for lidvid in record.direct_ancestor_lidvid_refs)


def _mapping_report_lines(client) -> List[str]:
    """Report, by reading the mappings, which ones a real run would create.  Nothing is created."""
    lines = []
    targets = [
        ("registry", [ANCESTRY_REFS_METADATA_KEY, SWEEPERS_ANCESTRY_VERSION_METADATA_KEY]),
        ("registry-refs", [SWEEPERS_ANCESTRY_VERSION_METADATA_KEY]),
    ]
    for index_label, keys in targets:
        index_name = resolve_multitenant_index_name(client, index_label)
        for key in keys:
            existing_type = _get_existing_mapping_type(client, index_name, key)
            if existing_type is None:
                state = "missing - a real run would create it as keyword"
            elif existing_type == "keyword":
                state = "present"
            else:
                state = f"mapped as {existing_type} - a real run would fail"
            lines.append(f"    mapping {key} in {index_name}: {state}")
    return ["  index mappings (read only):"] + lines


def summarise_dry_run(
    client, records: Iterable[ProductUpdateRecord], output_file: Optional[str] = None
) -> AncestryDryRunReport:
    """
    Consume the records a real run would write, counting them by kind, and report.  The client must be read-only.
    @param output_file: optional path of a TSV listing the bundle links which would be added to collections which already
        carry the current version (collection_lidvid, bundle_lidvid_added)
    """
    report = AncestryDryRunReport()

    handle: Optional[TextIO] = open(output_file, "w", encoding="utf-8") if output_file else None
    try:
        if handle is not None:
            handle.write(LINKS_FILE_HEADER + "\n")

        for record in records:
            if record.kind == UpdateKind.PENDING_COLLECTION:
                report.pending_collections += 1
            elif record.kind == UpdateKind.COLLECTION_COMPLETE:
                report.collection_completions += 1
            elif record.kind == UpdateKind.BUNDLE:
                report.bundles += 1
            elif record.kind == UpdateKind.STAMPED_COLLECTION_LINK:
                report.stamped_collections_linked += 1
                bundle_lidvids = _bundle_lidvids_added(record)
                report.stamped_collection_links += len(bundle_lidvids)
                if handle is not None:
                    for bundle_lidvid in bundle_lidvids:
                        handle.write(f"{record.product}\t{bundle_lidvid}\n")
            elif record.kind == UpdateKind.NONAGGREGATE:
                report.nonaggregates += 1
            else:
                report.unclassified += 1
    finally:
        if handle is not None:
            handle.close()

    report.mapping_lines = _mapping_report_lines(client)

    for line in report.summary_lines():
        log.info(limit_log_length(line))
        print(line)

    return report
