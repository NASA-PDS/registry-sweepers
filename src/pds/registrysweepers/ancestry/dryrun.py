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

from pds.registrysweepers.ancestry.bundlelinks import open_links_file
from pds.registrysweepers.ancestry.bundlelinks import write_bundle_links
from pds.registrysweepers.ancestry.constants import ANCESTRY_REFS_METADATA_KEY
from pds.registrysweepers.ancestry.productupdaterecord import ProductUpdateRecord
from pds.registrysweepers.ancestry.productupdaterecord import UpdateKind
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY
from pds.registrysweepers.utils.db.indexing import _get_existing_mapping_type
from pds.registrysweepers.utils.db.multitenancy import resolve_multitenant_index_name
from pds.registrysweepers.utils.misc import limit_log_length

log = logging.getLogger(__name__)


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
            "",
            f"Document updates a real run would write: {self.total_updates}",
            f"  Pending collections: {self.pending_collections}",
            f"  Pending collections marked complete afterwards: {self.collection_completions}",
            f"  Bundles: {self.bundles}",
            f"  Collections already at v{SWEEPERS_ANCESTRY_VERSION} that only get a bundle link added: "
            f"{self.stamped_collections_linked} ({self.stamped_collection_links} bundle links)",
            f"  Non-aggregate products: {self.nonaggregates}",
        ]
        if self.unclassified:
            lines.append(f"    updates of unknown kind: {self.unclassified}")
        lines.extend(self.mapping_lines or [])
        return lines


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
            lines.append(f"  {index_name}  {key}: {state}")
    return ["", "Index mappings (read only):"] + lines


def summarise_dry_run(
    client, records: Iterable[ProductUpdateRecord], output_file: Optional[str] = None
) -> AncestryDryRunReport:
    """
    Consume the records a real run would write, counting them by kind, and report.  The client must be read-only.
    @param output_file: optional path of a TSV listing the bundle links which would be added to collections which already
        carry the current version (collection_lidvid, bundle_lidvid_added)
    """
    report = AncestryDryRunReport()

    with open_links_file(output_file) as handle:
        for record in records:
            if record.kind == UpdateKind.PENDING_COLLECTION:
                report.pending_collections += 1
            elif record.kind == UpdateKind.COLLECTION_COMPLETE:
                report.collection_completions += 1
            elif record.kind == UpdateKind.BUNDLE:
                report.bundles += 1
            elif record.kind == UpdateKind.STAMPED_COLLECTION_LINK:
                report.stamped_collections_linked += 1
                report.stamped_collection_links += write_bundle_links(handle, record)
            elif record.kind == UpdateKind.NONAGGREGATE:
                report.nonaggregates += 1
            else:
                report.unclassified += 1

    report.mapping_lines = _mapping_report_lines(client)

    # The report is printed once as a plain block; the log gets a single line, so that a run at INFO level does not
    # repeat every line of the report behind a timestamp.
    print("\n".join(report.summary_lines()))
    log.info(limit_log_length(f"Dry run complete: {report.total_updates} document updates would be written; none were"))

    return report
