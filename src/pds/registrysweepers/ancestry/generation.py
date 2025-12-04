import logging
from collections import namedtuple
from collections.abc import Iterator
from itertools import chain
from typing import Dict
from typing import Generator
from typing import Iterable
from typing import Mapping
from typing import Set

import psutil  # type: ignore
from opensearchpy import OpenSearch
from pds.registrysweepers.ancestry.productupdaterecord import ProductUpdateRecord
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
    collections_docs = list(query_for_pending_collections(client))

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

        # skip processing if bundle is up-to-date
        if bundle_update_record.skippable:
            continue

        # For each collection identifier
        #   - if a LIDVID is specified, add bundle to that LIDVID's record
        #   - else if a LID is specified, add bundle to the record of every LIDVID with that LID
        for identifier in referenced_collection_identifiers:
            if isinstance(identifier, PdsLidVid):
                try:
                    collection_record = collection_update_records_by_collection_lidvid[identifier]
                    collection_record.add_direct_ancestor_ref(bundle_lidvid)
                except KeyError:
                    log.warning(
                        limit_log_length(
                            f"Collection {identifier} referenced by bundle {bundle_lidvid} "
                            f"does not exist in registry - skipping"
                        )
                        # TODO: need to defer this update per https://github.com/NASA-PDS/registry-sweepers/issues/188
                    )
            elif isinstance(identifier, PdsLid):
                try:
                    for collection_record in collection_update_records_by_collection_lid[identifier.lid]:
                        collection_record.add_direct_ancestor_ref(bundle_lidvid)
                except KeyError:
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

    collection_and_bundle_update_records = chain(collection_update_records_by_collection_lidvid.values(), bundle_update_records_by_bundle_lidvid.values())
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
