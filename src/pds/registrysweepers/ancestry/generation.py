import logging
from collections import namedtuple
from collections.abc import Iterator
from itertools import chain
from typing import Dict
from typing import Generator
from typing import Iterable
from typing import List
from typing import Mapping
from typing import Set
from typing import Tuple

import psutil  # type: ignore
from opensearchpy import OpenSearch
from pds.registrysweepers.ancestry.constants import ANCESTRY_REFS_METADATA_KEY
from pds.registrysweepers.ancestry.productupdaterecord import ProductUpdateRecord
from pds.registrysweepers.ancestry.queries import query_for_all_collections
from pds.registrysweepers.ancestry.queries import query_for_collection_nonaggregate_refs
from pds.registrysweepers.ancestry.queries import query_for_pending_bundles
from pds.registrysweepers.ancestry.queries import query_for_pending_collections
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY
from pds.registrysweepers.utils.misc import coerce_list_type
from pds.registrysweepers.utils.misc import limit_log_length
from pds.registrysweepers.utils.productidentifiers.factory import PdsProductIdentifierFactory
from pds.registrysweepers.utils.productidentifiers.pdslid import PdsLid
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid

log = logging.getLogger(__name__)

# It's necessary to track which registry-refs documents have been processed during this run.  This cannot be derived
# by repeating the query, as the sweeper may be running concurrently with harvest, and document content may change.
# RefDocBookkeepingEntry is used to ensure that only those documents which have been processed and have not been
# externally modified during sweeper execution will be marked as processed with the current sweeper version.
RefDocBookkeepingEntry = namedtuple("RefDocBookkeepingEntry", ["id", "primary_term", "seq_no"])


def get_ancestry_by_collection_lidvid(collections_docs: Iterable[Dict]) -> Mapping[PdsLidVid, ProductUpdateRecord]:
    # Instantiate the collections' ProductUpdateRecords, keyed by collection LIDVID for fast access

    ancestry_by_collection_lidvid = {}
    for doc in collections_docs:
        try:
            collection_lidvid = PdsLidVid.from_string(doc["_source"]["lidvid"])
            ancestry_by_collection_lidvid[collection_lidvid] = ProductUpdateRecord(product=collection_lidvid)
        except (ValueError, KeyError) as err:
            log.warning(
                limit_log_length(
                    f'Failed to instantiate ProductUpdateRecord from document in index "{doc.get("_index")}" with id "{doc.get("_id")}" due to {type(err)}: {err}'
                )
            )
            continue

    return ancestry_by_collection_lidvid


def get_ancestry_by_collection_lid(
    ancestry_by_collection_lidvid: Mapping[PdsLidVid, ProductUpdateRecord]
) -> Mapping[PdsLid, Set[ProductUpdateRecord]]:
    # Create a dict of pointer-sets to the newly-instantiated records, binned/keyed by LID for fast access when a bundle
    #  only refers to a LID rather than a specific LIDVID
    ancestry_by_collection_lid: Dict[PdsLid, Set[ProductUpdateRecord]] = {}
    for record in ancestry_by_collection_lidvid.values():
        if record.product.lid not in ancestry_by_collection_lid:
            ancestry_by_collection_lid[record.product.lid] = set()
        ancestry_by_collection_lid[record.product.lid].add(record)

    return ancestry_by_collection_lid


def get_ancestry_version(doc: Dict) -> int:
    """Return the ancestry version stamped on a document, or 0 if it is missing or unparseable."""
    try:
        return int(doc["_source"].get(SWEEPERS_ANCESTRY_VERSION_METADATA_KEY, 0))
    except (TypeError, ValueError):
        return 0


class StampedCollectionBundleLinks:
    """
    Tracks the collections which already carry the current ancestry version, so that a bundle which was loaded (or
    re-harvested) after its collection was processed can still be added to that collection's ancestor refs.

    A stamped collection is only given an update record when a bundle LIDVID is missing from its existing ancestor refs,
    so a repeat run yields no records.  The record only adds the bundle: the collection's member products are not
    touched, and the collection is not given a stamp which it did not already carry.
    """

    def __init__(self, stamped_collections_docs: Iterable[Dict]):
        self._existing_ancestor_refs: Dict[PdsLidVid, Set[str]] = {}
        self._lidvids_by_lid: Dict[PdsLid, Set[PdsLidVid]] = {}
        self._update_records: Dict[PdsLidVid, ProductUpdateRecord] = {}

        for doc in stamped_collections_docs:
            try:
                collection_lidvid = PdsLidVid.from_string(doc["_source"]["lidvid"])
            except (ValueError, KeyError) as err:
                log.warning(
                    limit_log_length(
                        f'Failed to parse LIDVID from collection document in index "{doc.get("_index")}" with id "{doc.get("_id")}" due to {type(err)}: {err}'
                    )
                )
                continue

            existing_refs = coerce_list_type(doc["_source"].get(ANCESTRY_REFS_METADATA_KEY) or [])
            self._existing_ancestor_refs[collection_lidvid] = {str(ref) for ref in existing_refs}
            self._lidvids_by_lid.setdefault(collection_lidvid.lid, set()).add(collection_lidvid)

    def has_lidvid(self, collection_lidvid: PdsLidVid) -> bool:
        return collection_lidvid in self._existing_ancestor_refs

    def lidvids_for_lid(self, collection_lid: PdsLid) -> Set[PdsLidVid]:
        return self._lidvids_by_lid.get(collection_lid, set())

    def link(self, collection_lidvid: PdsLidVid, bundle_lidvid: PdsLidVid) -> None:
        """Add the bundle to the stamped collection, unless the collection already lists it."""
        if str(bundle_lidvid) in self._existing_ancestor_refs[collection_lidvid]:
            return

        if collection_lidvid not in self._update_records:
            self._update_records[collection_lidvid] = ProductUpdateRecord(product=collection_lidvid)
        self._update_records[collection_lidvid].add_direct_ancestor_ref(bundle_lidvid)

    @property
    def update_records(self) -> Iterable[ProductUpdateRecord]:
        return self._update_records.values()


def split_collections_docs_by_ancestry_version(collections_docs: Iterable[Dict]) -> Tuple[List[Dict], List[Dict]]:
    """Split collection docs into (pending, stamped), where stamped docs carry the current ancestry version or newer."""
    pending: List[Dict] = []
    stamped: List[Dict] = []
    for doc in collections_docs:
        (stamped if get_ancestry_version(doc) >= SWEEPERS_ANCESTRY_VERSION else pending).append(doc)

    return pending, stamped


def process_collection_bundle_ancestry(
        client: OpenSearch) -> Iterable[ProductUpdateRecord]:
    """
    Because the number of bundles and collections is relatively small, we can process all bundle-ancestries at once to
    leverage the existing code.
    :param client: OpenSearch client
    :return:
    """

    log.info(limit_log_length("Generating ProductUpdateRecords for collections' bundle-ancestries..."))
    bundles_docs = list(query_for_pending_bundles(client))

    # Collections which already carry the current ancestry version are not reprocessed, but a bundle which was loaded
    # after they were stamped must still be added to their ancestor refs, so they are retrieved alongside the pending ones.
    collections_docs, stamped_collections_docs = split_collections_docs_by_ancestry_version(
        query_for_all_collections(client)
    )
    stamped_collection_links = StampedCollectionBundleLinks(stamped_collections_docs)

    # Prepare empty ancestry records for collections, with fast access by LID or LIDVID
    collection_update_records_by_collection_lidvid: Mapping[PdsLidVid, ProductUpdateRecord] = get_ancestry_by_collection_lidvid(
        collections_docs
    )
    collection_update_records_by_collection_lid: Mapping[PdsLid, Set[ProductUpdateRecord]] = get_ancestry_by_collection_lid(
        collection_update_records_by_collection_lidvid
    )

    bundle_update_records_by_bundle_lidvid: Mapping[PdsLidVid, ProductUpdateRecord] = {record.product: record for record in bundle_update_records_from_docs(bundles_docs)}

    # For each bundle, add it to the bundle-ancestry of every collection it references
    for bundle_doc in bundles_docs:
        try:
            bundle_lidvid = PdsLidVid.from_string(bundle_doc["_source"]["lidvid"])
            bundle_update_record = bundle_update_records_by_bundle_lidvid[bundle_lidvid]
            referenced_collection_identifiers = [
                PdsProductIdentifierFactory.from_string(id)
                for id in coerce_list_type(bundle_doc["_source"]["ref_lid_collection"])
            ]
        except (ValueError, KeyError) as err:
            log.warning(
                limit_log_length(
                    f'Failed to parse LIDVID and/or collection reference identifiers from document in index "{bundle_doc.get("_index")}" with id "{bundle_doc.get("_id")}" due to {type(err)}: {err}'
                )
            )
            continue

        # For each collection identifier
        #   - if a LIDVID is specified, add bundle to that LIDVID's record
        #   - else if a LID is specified, add bundle to the record of every LIDVID with that LID
        # Pending collections always receive the bundle.  Collections which are already stamped receive it only if
        # they do not already list it.
        for identifier in referenced_collection_identifiers:
            if isinstance(identifier, PdsLidVid):
                if identifier in collection_update_records_by_collection_lidvid:
                    collection_update_records_by_collection_lidvid[identifier].add_direct_ancestor_ref(bundle_lidvid)
                elif stamped_collection_links.has_lidvid(identifier):
                    stamped_collection_links.link(identifier, bundle_lidvid)
                else:
                    log.warning(
                        limit_log_length(
                            f"Collection {identifier} referenced by bundle {bundle_lidvid} "
                            f"does not exist in registry - skipping"
                        )
                        # TODO: need to defer this update per https://github.com/NASA-PDS/registry-sweepers/issues/188
                    )
            elif isinstance(identifier, PdsLid):
                pending_collection_records = collection_update_records_by_collection_lid.get(identifier.lid, set())
                stamped_collection_lidvids = stamped_collection_links.lidvids_for_lid(identifier.lid)
                for collection_record in pending_collection_records:
                    collection_record.add_direct_ancestor_ref(bundle_lidvid)
                for stamped_collection_lidvid in stamped_collection_lidvids:
                    stamped_collection_links.link(stamped_collection_lidvid, bundle_lidvid)

                if not pending_collection_records and not stamped_collection_lidvids:
                    log.warning(
                        limit_log_length(
                            f"No versions of collection {identifier} referenced by bundle {bundle_lidvid} "
                            f"exist in registry - skipping"
                        )
                    )
                    #     TODO: need to defer this update per https://github.com/NASA-PDS/registry-sweepers/issues/188
                    #      This is tricky, as we don't know how to handle future versions yet to be created.
            else:
                raise RuntimeError(
                    f"Encountered product identifier of unknown type {identifier.__class__} "
                    f"(should be PdsLidVid or PdsLid)"
                )
        bundle_update_record.mark_processed()

    stamped_collection_update_records = list(stamped_collection_links.update_records)
    if stamped_collection_update_records:
        log.info(
            limit_log_length(
                f"Adding bundles to the ancestor refs of {len(stamped_collection_update_records)} collections "
                f"which already carry ancestry version {SWEEPERS_ANCESTRY_VERSION}"
            )
        )

    collection_and_bundle_update_records = chain(
        collection_update_records_by_collection_lidvid.values(),
        stamped_collection_update_records,
        bundle_update_records_by_bundle_lidvid.values(),
    )
    return collection_and_bundle_update_records


def bundle_update_records_from_docs(docs: Iterable[dict]) -> Iterable[ProductUpdateRecord]:
    """
    Generate ProductUpdateRecords from bundle docs, indicating whether they are up-to-date or require processing.
    """
    for doc in docs:
        try:
            sweeper_version_in_doc = doc["_source"].get(SWEEPERS_ANCESTRY_VERSION_METADATA_KEY, 0)
            skip_write = sweeper_version_in_doc >= SWEEPERS_ANCESTRY_VERSION
            yield ProductUpdateRecord(product=PdsLidVid.from_string(doc["_source"]["lidvid"]),
                                      skip_write=skip_write)
        except (ValueError, KeyError) as err:
            log.warning(
                limit_log_length(
                    f'Failed to instantiate ProductUpdateRecord from document in index "{doc.get("_index")}" with id "{doc.get("_id")}" due to {type(err)}: {err}'
                )
            )


def process_collection_ancestries_for_nonaggregates(client) -> Iterator[ProductUpdateRecord]:
    """
    Process each non-up-to-date collection, yielding updates for its descendant nonaggregate products, then an update for the collection itself to mark it as up-to-date.
    """

    # iterate over collections (and their member nonaggregate products) which require ancestry updates
    pending_collections_docs = query_for_pending_collections(client)
    pending_collections = iter(PdsLidVid.from_string(record["_source"]["lidvid"]) for record in pending_collections_docs)
    # TODO: add orphan processing step. not sure if it belongs here or as a separate step after ancestry has completed - edunn 20251112

    for collection_lidvid in pending_collections:
        collection_nonaggregate_refs = query_for_collection_nonaggregate_refs(client, collection_lidvid)

        for nonaggregate_lidvid in collection_nonaggregate_refs:
            nonagg_update_record = ProductUpdateRecord(product=nonaggregate_lidvid,
                                                       direct_ancestor_refs=[collection_lidvid])
            yield nonagg_update_record

        # finally, collection can be updated to mark it as complete
        collection_complete_update_record = ProductUpdateRecord(collection_lidvid)
        collection_complete_update_record.mark_processed()
        yield collection_complete_update_record
