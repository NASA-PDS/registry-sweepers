"""
Listing of the bundle links added to collections which already carry the current ancestry version.

The listing is a TSV with one row per link, written while the update records stream by, so memory use does not grow with
the number of links.  The dry run and the real run write it the same way: a dry run lists the links it would add, a real
run lists the links it adds.
"""
from contextlib import contextmanager
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Optional
from typing import TextIO

from pds.registrysweepers.ancestry.productupdaterecord import ProductUpdateRecord
from pds.registrysweepers.ancestry.productupdaterecord import UpdateKind

LINKS_FILE_HEADER = "collection_lidvid\tbundle_lidvid_added"


def bundle_lidvids_added(record: ProductUpdateRecord) -> List[str]:
    return sorted(str(lidvid) for lidvid in record.direct_ancestor_lidvid_refs)


@contextmanager
def open_links_file(path: Optional[str]) -> Iterator[Optional[TextIO]]:
    """
    Open the listing at the given path and write its header, yielding the handle (None when no path is given).  The
    file is flushed and closed on exit, including when the body raises.
    """
    if not path:
        yield None
        return

    with open(path, "w", encoding="utf-8") as handle:
        handle.write(LINKS_FILE_HEADER + "\n")
        yield handle


def write_bundle_links(handle: Optional[TextIO], record: ProductUpdateRecord) -> int:
    """Write a row per bundle link added by the record (if there is a handle), returning the number of links"""
    bundle_lidvids = bundle_lidvids_added(record)
    if handle is not None:
        for bundle_lidvid in bundle_lidvids:
            handle.write(f"{record.product}\t{bundle_lidvid}\n")
    return len(bundle_lidvids)


def tee_bundle_links(records: Iterable[ProductUpdateRecord], handle: TextIO) -> Iterator[ProductUpdateRecord]:
    """Yield the records unchanged, writing the bundle links of the ones which add any as they pass"""
    for record in records:
        if record.kind == UpdateKind.STAMPED_COLLECTION_LINK:
            write_bundle_links(handle, record)
        yield record
