import logging
from collections import namedtuple
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Mapping
from typing import Set

import psutil  # type: ignore
from opensearchpy import OpenSearch
from pds.registrysweepers.ancestry.ancestryrecord import AncestryRecord
from pds.registrysweepers.ancestry.queries import get_bundle_ancestry_records_query
from pds.registrysweepers.ancestry.queries import get_collection_ancestry_records_bundles_query
from pds.registrysweepers.ancestry.queries import get_collection_ancestry_records_collections_query
from pds.registrysweepers.ancestry.queries import get_nonaggregate_ancestry_records_for_collection_lid_query
from pds.registrysweepers.ancestry.typedefs import DbMockTypeDef
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY
from pds.registrysweepers.utils.bigdict.spilldict import SpillDict
from pds.registrysweepers.utils.misc import bin_elements
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


def get_bundle_ancestry_records(client: OpenSearch, db_mock: DbMockTypeDef = None) -> Iterable[AncestryRecord]:
    log.info(limit_log_length("Generating AncestryRecords for bundles..."))
    docs = get_bundle_ancestry_records_query(client, db_mock)
    for doc in docs:
        try:
            sweeper_version_in_doc = doc["_source"].get(SWEEPERS_ANCESTRY_VERSION_METADATA_KEY, 0)
            skip_write = sweeper_version_in_doc >= SWEEPERS_ANCESTRY_VERSION
            yield AncestryRecord(lidvid=PdsLidVid.from_string(doc["_source"]["lidvid"]), skip_write=skip_write)
        except (ValueError, KeyError) as err:
            log.warning(
                limit_log_length(
                    f'Failed to instantiate AncestryRecord from document in index "{doc.get("_index")}" with id "{doc.get("_id")}" due to {type(err)}: {err}'
                )
            )
            continue


def get_ancestry_by_collection_lidvid(collections_docs: Iterable[Dict]) -> Mapping[PdsLidVid, AncestryRecord]:
    # Instantiate the AncestryRecords, keyed by collection LIDVID for fast access

    ancestry_by_collection_lidvid = {}
    for doc in collections_docs:
        try:
            sweeper_version_in_doc = doc["_source"].get(SWEEPERS_ANCESTRY_VERSION_METADATA_KEY, 0)
            skip_write = sweeper_version_in_doc >= SWEEPERS_ANCESTRY_VERSION
            lidvid = PdsLidVid.from_string(doc["_source"]["lidvid"])
            ancestry_by_collection_lidvid[lidvid] = AncestryRecord(lidvid=lidvid, skip_write=skip_write)
        except (ValueError, KeyError) as err:
            log.warning(
                limit_log_length(
                    f'Failed to instantiate AncestryRecord from document in index "{doc.get("_index")}" with id "{doc.get("_id")}" due to {type(err)}: {err}'
                )
            )
            continue

    return ancestry_by_collection_lidvid


def get_ancestry_by_collection_lid(
    ancestry_by_collection_lidvid: Mapping[PdsLidVid, AncestryRecord]
) -> Mapping[PdsLid, Set[AncestryRecord]]:
    # Create a dict of pointer-sets to the newly-instantiated records, binned/keyed by LID for fast access when a bundle
    #  only refers to a LID rather than a specific LIDVID
    ancestry_by_collection_lid: Dict[PdsLid, Set[AncestryRecord]] = {}
    for record in ancestry_by_collection_lidvid.values():
        if record.lidvid.lid not in ancestry_by_collection_lid:
            ancestry_by_collection_lid[record.lidvid.lid] = set()
        ancestry_by_collection_lid[record.lidvid.lid].add(record)

    return ancestry_by_collection_lid


def get_collection_ancestry_records(
    client: OpenSearch, registry_db_mock: DbMockTypeDef = None
) -> Iterable[AncestryRecord]:
    log.info(limit_log_length("Generating AncestryRecords for collections..."))
    bundles_docs = get_collection_ancestry_records_bundles_query(client, registry_db_mock)
    collections_docs = list(get_collection_ancestry_records_collections_query(client, registry_db_mock))

    # Prepare empty ancestry records for collections, with fast access by LID or LIDVID
    ancestry_by_collection_lidvid: Mapping[PdsLidVid, AncestryRecord] = get_ancestry_by_collection_lidvid(
        collections_docs
    )
    ancestry_by_collection_lid: Mapping[PdsLid, Set[AncestryRecord]] = get_ancestry_by_collection_lid(
        ancestry_by_collection_lidvid
    )

    # For each bundle, add it to the bundle-ancestry of every collection it references
    for doc in bundles_docs:
        try:
            bundle_lidvid = PdsLidVid.from_string(doc["_source"]["lidvid"])
            referenced_collection_identifiers = [
                PdsProductIdentifierFactory.from_string(id)
                for id in coerce_list_type(doc["_source"]["ref_lid_collection"])
            ]
        except (ValueError, KeyError) as err:
            log.warning(
                limit_log_length(
                    f'Failed to parse LIDVID and/or collection reference identifiers from document in index "{doc.get("_index")}" with id "{doc.get("_id")}" due to {type(err)}: {err}'
                )
            )
            continue

        # For each collection identifier
        #   - if a LIDVID is specified, add bundle to that LIDVID's record
        #   - else if a LID is specified, add bundle to the record of every LIDVID with that LID
        for identifier in referenced_collection_identifiers:
            if isinstance(identifier, PdsLidVid):
                try:
                    ancestry_by_collection_lidvid[identifier].explicit_parent_bundle_lidvids.add(bundle_lidvid)
                except KeyError:
                    log.warning(
                        limit_log_length(
                            f"Collection {identifier} referenced by bundle {bundle_lidvid} "
                            f"does not exist in registry - skipping"
                        )
                    )
            elif isinstance(identifier, PdsLid):
                try:
                    for record in ancestry_by_collection_lid[identifier.lid]:
                        record.explicit_parent_bundle_lidvids.add(bundle_lidvid)
                except KeyError:
                    log.warning(
                        limit_log_length(
                            f"No versions of collection {identifier} referenced by bundle {bundle_lidvid} "
                            f"exist in registry - skipping"
                        )
                    )
            else:
                raise RuntimeError(
                    f"Encountered product identifier of unknown type {identifier.__class__} "
                    f"(should be PdsLidVid or PdsLid)"
                )

    # We could retain the keys for better performance, as they're used by the non-aggregate record generation, but this
    # is cleaner, so we'll regenerate the dict from the records later unless performance is a problem.
    return ancestry_by_collection_lidvid.values()


def generate_nonaggregate_and_collection_records_iteratively(
    client: OpenSearch,
    all_collections_records: Iterable[AncestryRecord],
    registry_db_mock: DbMockTypeDef = None,
) -> Iterable[AncestryRecord]:
    """
    Iteratively generate nonaggregate records in chunks, each chunk sharing a common collection LID.  This
    prevents the need to simultaneously store data in memory for a large volume of nonaggregate records.

    AncestryRecords for non-aggregate products which are present in collections which do not all share the same LID will
    be incomplete, and must be merged separately prior to db write, with all collection-LID-specific AncestryRecords
    generated prior to that merge/write.  This deferral is handled separately in generate_updates().

    After non-aggregate records are generated, the corresponding collections' records are updated, such that they are
    only processed and marked up-to-date if their non-aggregates have successfully been updated.
    """

    collection_records_by_lid = bin_elements(all_collections_records, lambda r: r.lidvid.lid)

    for lid, collections_records_for_lid in collection_records_by_lid.items():
        if all([record.skip_write for record in collections_records_for_lid]):
            log.debug(limit_log_length(f"Skipping updates for up-to-date collection family: {str(lid)}"))
            continue
        else:
            log.info(
                limit_log_length(
                    f"Processing all versions of collection {str(lid)}: {[str(id) for id in sorted([r.lidvid for r in collections_records_for_lid])]}"
                )
            )

        for non_aggregate_record in get_nonaggregate_ancestry_records_for_collection_lid(
            client, lid, collections_records_for_lid, registry_db_mock
        ):
            yield non_aggregate_record

        for collection_record in collections_records_for_lid:
            yield collection_record


def get_nonaggregate_ancestry_records_for_collection_lid(
    client: OpenSearch,
    collection_lid: PdsLid,
    collection_ancestry_records: Iterable[AncestryRecord],
    registry_db_mock: DbMockTypeDef = None,
) -> Iterable[AncestryRecord]:
    log.info(
        limit_log_length(
            f"Generating AncestryRecords for non-aggregate products of collections with LID {str(collection_lid)}, using non-chunked input/output..."
        )
    )

    collection_refs_query_docs = get_nonaggregate_ancestry_records_for_collection_lid_query(
        client, collection_lid, registry_db_mock
    )

    return generate_nonaggregate_ancestry_records(collection_ancestry_records, collection_refs_query_docs)


def generate_nonaggregate_ancestry_records(
    collection_ancestry_records: Iterable[AncestryRecord], collection_refs_query_docs: Iterable[Dict[str, Any]]
) -> Iterable[AncestryRecord]:
    collection_records_by_lidvid = {r.lidvid: r for r in collection_ancestry_records}

    nonaggregate_ancestry_records_by_lidvid = SpillDict(spill_threshold=100000, merge=AncestryRecord.combine)

    # For each collection, add the collection and its bundle ancestry to all products the collection contains
    for doc in collection_refs_query_docs:
        try:
            if doc["_id"].split("::")[2].startswith("S"):
                log.info(limit_log_length(f'Skipping secondary-collection document {doc["_id"]}'))
                continue

            collection_lidvid = PdsLidVid.from_string(doc["_source"]["collection_lidvid"])
            referenced_lidvids = [PdsLidVid.from_string(s) for s in doc["_source"]["product_lidvid"]]
            nonaggregate_lidvids = [id for id in referenced_lidvids if id.is_basic_product()]

            erroneous_lidvids = [id for id in referenced_lidvids if not id.is_basic_product()]
            if len(erroneous_lidvids) > 0:
                log.error(
                    limit_log_length(
                        f'registry-refs document with id {doc["_id"]} references one or more aggregate products in its product_lidvid refs list: {[str(id) for id in erroneous_lidvids]}'
                    )
                )

        except IndexError:
            doc_id = doc["_id"]
            log.warning(limit_log_length(f'Encountered document with unexpected _id: "{doc_id}"'))
            continue
        except (ValueError, KeyError) as err:
            log.warning(
                limit_log_length(
                    f'Failed to parse collection and/or product LIDVIDs from document in index "{doc.get("_index")}" with id "{doc.get("_id")}" due to {type(err).__name__}: {err}'
                )
            )
            continue

        try:
            collection_record = collection_records_by_lidvid[collection_lidvid]
        except KeyError:
            log.debug(
                limit_log_length(
                    f'Failed to resolve history for page {doc.get("_id")} in index {doc.get("_index")} with collection_lidvid {collection_lidvid} - no such collection exists in registry.'
                )
            )
            continue

        for lidvid in nonaggregate_lidvids:
            lidvid_id = str(lidvid)
            if lidvid_id not in nonaggregate_ancestry_records_by_lidvid:
                nonaggregate_ancestry_records_by_lidvid[lidvid_id] = AncestryRecord(lidvid=lidvid)

            record: AncestryRecord = nonaggregate_ancestry_records_by_lidvid[lidvid_id]
            record.attach_parent_record(collection_record)

    return nonaggregate_ancestry_records_by_lidvid.values()
