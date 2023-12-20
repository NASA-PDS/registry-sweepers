import logging
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Mapping
from typing import Set

from opensearchpy import OpenSearch
from pds.registrysweepers.ancestry.ancestryrecord import AncestryRecord
from pds.registrysweepers.ancestry.queries import DbMockTypeDef
from pds.registrysweepers.ancestry.queries import get_bundle_ancestry_records_query
from pds.registrysweepers.ancestry.queries import get_collection_ancestry_records_bundles_query
from pds.registrysweepers.ancestry.queries import get_collection_ancestry_records_collections_query
from pds.registrysweepers.ancestry.queries import get_collections_containing_products_query
from pds.registrysweepers.ancestry.queries import get_nonaggregate_ancestry_records_query
from pds.registrysweepers.utils.misc import coerce_list_type
from pds.registrysweepers.utils.productidentifiers.factory import PdsProductIdentifierFactory
from pds.registrysweepers.utils.productidentifiers.pdslid import PdsLid
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid

log = logging.getLogger(__name__)


def get_bundle_ancestry_records(client: OpenSearch, db_mock: DbMockTypeDef = None) -> Iterable[AncestryRecord]:
    log.info("Generating AncestryRecords for bundles...")
    docs = get_bundle_ancestry_records_query(client, db_mock)
    for doc in docs:
        try:
            yield AncestryRecord(lidvid=PdsLidVid.from_string(doc["_source"]["lidvid"]))
        except (ValueError, KeyError) as err:
            log.warning(
                'Failed to instantiate AncestryRecord from document in index "%s" with id "%s" due to %s: %s',
                doc.get("_index"),
                doc.get("_id"),
                type(err),
                err,
            )
            continue


def get_ancestry_by_collection_lidvid(collections_docs: Iterable[Dict]) -> Mapping[PdsLidVid, AncestryRecord]:
    # Instantiate the AncestryRecords, keyed by collection LIDVID for fast access

    ancestry_by_collection_lidvid = {}
    for doc in collections_docs:
        try:
            lidvid = PdsLidVid.from_string(doc["_source"]["lidvid"])
            ancestry_by_collection_lidvid[lidvid] = AncestryRecord(lidvid=lidvid)
        except (ValueError, KeyError) as err:
            log.warning(
                'Failed to instantiate AncestryRecord from document in index "%s" with id "%s" due to %s: %s',
                doc.get("_index"),
                doc.get("_id"),
                type(err),
                err,
            )
            continue

    return ancestry_by_collection_lidvid


def get_collection_aliases_by_lid(collections_docs: Iterable[Dict]) -> Dict[PdsLid, Set[PdsLid]]:
    aliases_by_lid: Dict[PdsLid, Set[PdsLid]] = {}
    for doc in collections_docs:
        alternate_ids: List[str] = doc["_source"].get("alternate_ids", [])
        lids: Set[PdsLid] = {PdsProductIdentifierFactory.from_string(id).lid for id in alternate_ids}
        for lid in lids:
            if lid not in aliases_by_lid:
                aliases_by_lid[lid] = set()
            aliases_by_lid[lid].update(lids)

    return aliases_by_lid


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
    log.info("Generating AncestryRecords for collections...")
    bundles_docs = get_collection_ancestry_records_bundles_query(client, registry_db_mock)
    collections_docs = list(get_collection_ancestry_records_collections_query(client, registry_db_mock))

    # Prepare LID alias sets for every LID
    collection_aliases_by_lid: Dict[PdsLid, Set[PdsLid]] = get_collection_aliases_by_lid(collections_docs)

    # Prepare empty ancestry records for collections, with fast access by LID or LIDVID
    ancestry_by_collection_lidvid = get_ancestry_by_collection_lidvid(collections_docs)
    ancestry_by_collection_lid = get_ancestry_by_collection_lid(ancestry_by_collection_lidvid)

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
                'Failed to parse LIDVID and/or collection reference identifiers from document in index "%s" with id "%s" due to %s: %s',
                doc.get("_index"),
                doc.get("_id"),
                type(err),
                err,
            )
            continue

        # For each identifier
        #   - if a LIDVID is specified, add bundle to that LIDVID's record
        #   - else if a LID is specified, add bundle to the record of every LIDVID with that LID
        for identifier in referenced_collection_identifiers:
            if isinstance(identifier, PdsLidVid):
                try:
                    ancestry_by_collection_lidvid[identifier].parent_bundle_lidvids.add(bundle_lidvid)
                except KeyError:
                    log.warning(
                        f"Collection {identifier} referenced by bundle {bundle_lidvid} "
                        f"does not exist in registry - skipping"
                    )
            elif isinstance(identifier, PdsLid):
                try:
                    for alias in collection_aliases_by_lid[identifier]:
                        for record in ancestry_by_collection_lid[alias]:
                            record.parent_bundle_lidvids.add(bundle_lidvid)
                except KeyError:
                    log.warning(
                        f"No versions of collection {identifier} referenced by bundle {bundle_lidvid} "
                        f"exist in registry - skipping"
                    )
            else:
                raise RuntimeError(
                    f"Encountered product identifier of unknown type {identifier.__class__} "
                    f"(should be PdsLidVid or PdsLid)"
                )

    # We could retain the keys for better performance, as they're used by the non-aggregate record generation, but this
    # is cleaner, so we'll regenerate the dict from the records later unless performance is a problem.
    return ancestry_by_collection_lidvid.values()


def get_nonaggregate_ancestry_records(
    client: OpenSearch,
    collection_ancestry_records: Iterable[AncestryRecord],
    registry_db_mock: DbMockTypeDef = None,
) -> Iterable[AncestryRecord]:
    log.info("Generating AncestryRecords for non-aggregate products...")

    # Generate lookup for the parent bundles of all collections - these will be applied to non-aggregate products too.
    bundle_ancestry_by_collection_lidvid = {
        record.lidvid: record.parent_bundle_lidvids for record in collection_ancestry_records
    }

    collection_refs_query_docs = get_nonaggregate_ancestry_records_query(client, registry_db_mock)

    # registry-refs deals with pages of references - each collection has 1-N pages
    # For each target page of collection references, fetch all collection reference pages which share any non-aggregate
    # member refs with it, then lazily generate AncestryRecords for each of the target's referenced non-aggregates.
    for target_collection_page_doc in collection_refs_query_docs:
        try:
            nonaggregate_lidvid_refs_from_target: List[str] = target_collection_page_doc["_source"]["product_lidvid"]
            collection_pages_having_shared_products: List[Dict[str, Any]] = [
                doc["_source"]
                for doc in get_collections_containing_products_query(
                    nonaggregate_lidvid_refs_from_target, client, registry_db_mock
                )
            ]

            # Convert member refs lists into sets to avoid O(collections*nonaggs) lookup - probably not required though
            for collection_page in collection_pages_having_shared_products:
                collection_page["product_lidvid"] = set(collection_page["product_lidvid"])

            for nonagg_ref in nonaggregate_lidvid_refs_from_target:
                nonagg_lidvid = PdsLidVid.from_string(nonagg_ref)
                nonagg_bundle_ancestors: Set[PdsLidVid] = set()  # bundle ancestors of non-agg product
                nonagg_collection_ancestors: Set[PdsLidVid] = set()  # collection ancestors of non-agg product

                for collection_page in collection_pages_having_shared_products:
                    collection_lidvid = PdsLidVid.from_string(collection_page["collection_lidvid"])
                    collection_bundle_ancestors = bundle_ancestry_by_collection_lidvid[
                        collection_lidvid
                    ]  # bundle ancestors of collections sharing any non-agg member refs with the target collection

                    if nonagg_ref in collection_page["product_lidvid"]:
                        nonagg_bundle_ancestors.update(collection_bundle_ancestors)
                        nonagg_collection_ancestors.add(collection_lidvid)

                yield AncestryRecord(
                    lidvid=nonagg_lidvid,
                    parent_collection_lidvids=nonagg_collection_ancestors,
                    parent_bundle_lidvids=nonagg_bundle_ancestors,
                )

        except (ValueError, KeyError) as err:
            log.warning(
                'Failed to parse collection, collection referencing shared product, and/or product LIDVIDs referenced by document in index "%s" with id "%s" due to %s: %s',
                target_collection_page_doc.get("_index"),
                target_collection_page_doc.get("_id"),
                type(err).__name__,
                err,
            )
            continue
