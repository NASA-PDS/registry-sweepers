import gc
import logging
import os
import shutil
import sys
import tempfile
from collections import namedtuple
from datetime import datetime
from datetime import timedelta
from typing import Dict
from typing import Iterable
from typing import List
from typing import Mapping
from typing import Set
from typing import Union

import psutil  # type: ignore
from opensearchpy import OpenSearch
from pds.registrysweepers.ancestry.ancestryrecord import AncestryRecord
from pds.registrysweepers.ancestry.queries import get_bundle_ancestry_records_query
from pds.registrysweepers.ancestry.queries import get_collection_ancestry_records_bundles_query
from pds.registrysweepers.ancestry.queries import get_collection_ancestry_records_collections_query
from pds.registrysweepers.ancestry.queries import get_nonaggregate_ancestry_records_for_collection_lid_query
from pds.registrysweepers.ancestry.queries import get_nonaggregate_ancestry_records_query
from pds.registrysweepers.ancestry.runtimeconstants import AncestryRuntimeConstants
from pds.registrysweepers.ancestry.typedefs import DbMockTypeDef
from pds.registrysweepers.ancestry.utils import dump_history_to_disk
from pds.registrysweepers.ancestry.utils import gb_mem_to_size
from pds.registrysweepers.ancestry.utils import load_partial_history_to_records
from pds.registrysweepers.ancestry.utils import make_history_serializable
from pds.registrysweepers.ancestry.utils import merge_matching_history_chunks
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY
from pds.registrysweepers.utils.db import Update
from pds.registrysweepers.utils.db import write_updated_docs
from pds.registrysweepers.utils.db.multitenancy import resolve_multitenant_index_name
from pds.registrysweepers.utils.misc import bin_elements
from pds.registrysweepers.utils.misc import coerce_list_type
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
    log.info("Generating AncestryRecords for bundles...")
    docs = get_bundle_ancestry_records_query(client, db_mock)
    for doc in docs:
        try:
            sweeper_version_in_doc = doc["_source"].get(SWEEPERS_ANCESTRY_VERSION_METADATA_KEY, 0)
            skip_write = sweeper_version_in_doc >= SWEEPERS_ANCESTRY_VERSION
            yield AncestryRecord(lidvid=PdsLidVid.from_string(doc["_source"]["lidvid"]), skip_write=skip_write)
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
            sweeper_version_in_doc = doc["_source"].get(SWEEPERS_ANCESTRY_VERSION_METADATA_KEY, 0)
            skip_write = sweeper_version_in_doc >= SWEEPERS_ANCESTRY_VERSION
            lidvid = PdsLidVid.from_string(doc["_source"]["lidvid"])
            ancestry_by_collection_lidvid[lidvid] = AncestryRecord(lidvid=lidvid, skip_write=skip_write)
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
                    for record in ancestry_by_collection_lid[identifier.lid]:
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


def generate_nonaggregate_and_collection_records_iteratively(
    client: OpenSearch,
    all_collections_records: Iterable[AncestryRecord],
    registry_db_mock: DbMockTypeDef = None,
) -> Iterable[AncestryRecord]:
    """
    Iteratively generate nonaggregate records in chunks, each chunk sharing a common collection LID.  This
    prevents the need to simultaneously store data in memory for a large volume of nonaggregate records.

    After non-aggregate records are generated, the corresponding collections' records are updated, such that they are
    only processed and marked up-to-date if their non-aggregates have successfully been updated.
    """

    collection_records_by_lid = bin_elements(all_collections_records, lambda r: r.lidvid.lid)

    for lid, collections_records_for_lid in collection_records_by_lid.items():
        if all([record.skip_write for record in collections_records_for_lid]):
            log.debug(f"Skipping updates for up-to-date collection family: {str(lid)}")
            continue
        else:
            log.info(
                f"Processing all versions of collection {str(lid)}: {[str(id) for id in sorted([r.lidvid for r in collections_records_for_lid])]}"
            )

        for non_aggregate_record in get_nonaggregate_ancestry_records_for_collection_lid(
            client, lid, collections_records_for_lid, registry_db_mock
        ):
            yield non_aggregate_record

        for collection_record in collections_records_for_lid:
            yield collection_record


def get_nonaggregate_ancestry_records(
    client: OpenSearch,
    collection_ancestry_records: Iterable[AncestryRecord],
    registry_db_mock: DbMockTypeDef = None,
    utilize_chunking: bool = True,
) -> Iterable[AncestryRecord]:
    f = (
        _get_nonaggregate_ancestry_records_with_chunking
        if utilize_chunking
        else _get_nonaggregate_ancestry_records_without_chunking
    )
    return f(client, collection_ancestry_records, registry_db_mock)


def get_nonaggregate_ancestry_records_for_collection_lid(
    client: OpenSearch,
    collection_lid: PdsLid,
    collection_ancestry_records: Iterable[AncestryRecord],
    registry_db_mock: DbMockTypeDef = None,
) -> Iterable[AncestryRecord]:
    log.info(
        f"Generating AncestryRecords for non-aggregate products of collections with LID {str(collection_lid)}, using non-chunked input/output..."
    )

    # Generate lookup for the parent bundles of all collections - these will be applied to non-aggregate products too.
    bundle_ancestry_by_collection_lidvid = {
        record.lidvid: record.parent_bundle_lidvids for record in collection_ancestry_records
    }

    collection_refs_query_docs = get_nonaggregate_ancestry_records_for_collection_lid_query(
        client, collection_lid, registry_db_mock
    )

    nonaggregate_ancestry_records_by_lidvid = {}
    # For each collection, add the collection and its bundle ancestry to all products the collection contains
    for doc in collection_refs_query_docs:
        try:
            if doc["_id"].split("::")[2].startswith("S"):
                log.info(f'Skipping secondary-collection document {doc["_id"]}')
                continue

            collection_lidvid = PdsLidVid.from_string(doc["_source"]["collection_lidvid"])
            referenced_lidvids = [PdsLidVid.from_string(s) for s in doc["_source"]["product_lidvid"]]
            nonaggregate_lidvids = [id for id in referenced_lidvids if id.is_basic_product()]

            erroneous_lidvids = [id for id in referenced_lidvids if not id.is_basic_product()]
            if len(erroneous_lidvids) > 0:
                log.error(
                    f'registry-refs document with id {doc["_id"]} references one or more aggregate products in its product_lidvid refs list: {[str(id) for id in erroneous_lidvids]}'
                )

        except IndexError as err:
            doc_id = doc["_id"]
            log.warning(f'Encountered document with unexpected _id: "{doc_id}"')
        except (ValueError, KeyError) as err:
            log.warning(
                'Failed to parse collection and/or product LIDVIDs from document in index "%s" with id "%s" due to %s: %s',
                doc.get("_index"),
                doc.get("_id"),
                type(err).__name__,
                err,
            )
            continue

        try:
            bundle_ancestry = bundle_ancestry_by_collection_lidvid[collection_lidvid]
        except KeyError:
            log.debug(
                f'Failed to resolve history for page {doc.get("_id")} in index {doc.get("_index")} with collection_lidvid {collection_lidvid} - no such collection exists in registry.'
            )
            continue

        for lidvid in nonaggregate_lidvids:
            if lidvid not in nonaggregate_ancestry_records_by_lidvid:
                nonaggregate_ancestry_records_by_lidvid[lidvid] = AncestryRecord(lidvid=lidvid)

            record = nonaggregate_ancestry_records_by_lidvid[lidvid]
            record.parent_bundle_lidvids.update(bundle_ancestry)
            record.parent_collection_lidvids.add(collection_lidvid)

    return nonaggregate_ancestry_records_by_lidvid.values()


def _get_nonaggregate_ancestry_records_without_chunking(
    client: OpenSearch,
    collection_ancestry_records: Iterable[AncestryRecord],
    registry_db_mock: DbMockTypeDef = None,
) -> Iterable[AncestryRecord]:
    log.info("Generating AncestryRecords for non-aggregate products, using non-chunked input/output...")

    # Generate lookup for the parent bundles of all collections - these will be applied to non-aggregate products too.
    bundle_ancestry_by_collection_lidvid = {
        record.lidvid: record.parent_bundle_lidvids for record in collection_ancestry_records
    }

    collection_refs_query_docs = get_nonaggregate_ancestry_records_query(client, registry_db_mock)

    nonaggregate_ancestry_records_by_lidvid = {}
    # For each collection, add the collection and its bundle ancestry to all products the collection contains
    for doc in collection_refs_query_docs:
        try:
            if doc["_id"].split("::")[2].startswith("S"):
                log.info(f'Skipping secondary-collection document {doc["_id"]}')
                continue

            collection_lidvid = PdsLidVid.from_string(doc["_source"]["collection_lidvid"])
            nonaggregate_lidvids = [PdsLidVid.from_string(s) for s in doc["_source"]["product_lidvid"]]
        except (ValueError, KeyError) as err:
            log.warning(
                'Failed to parse collection and/or product LIDVIDs from document in index "%s" with id "%s" due to %s: %s',
                doc.get("_index"),
                doc.get("_id"),
                type(err).__name__,
                err,
            )
            continue

        try:
            bundle_ancestry = bundle_ancestry_by_collection_lidvid[collection_lidvid]
        except KeyError:
            log.debug(
                f'Failed to resolve history for page {doc.get("_id")} in index {doc.get("_index")} with collection_lidvid {collection_lidvid} - no such collection exists in registry.'
            )
            continue

        for lidvid in nonaggregate_lidvids:
            if lidvid not in nonaggregate_ancestry_records_by_lidvid:
                nonaggregate_ancestry_records_by_lidvid[lidvid] = AncestryRecord(lidvid=lidvid)

            record = nonaggregate_ancestry_records_by_lidvid[lidvid]
            record.parent_bundle_lidvids.update(bundle_ancestry)
            record.parent_collection_lidvids.add(collection_lidvid)

    return nonaggregate_ancestry_records_by_lidvid.values()


def _get_nonaggregate_ancestry_records_with_chunking(
    client: OpenSearch,
    collection_ancestry_records: Iterable[AncestryRecord],
    registry_db_mock: DbMockTypeDef = None,
) -> Iterable[AncestryRecord]:
    log.info("Generating AncestryRecords for non-aggregate products, using chunked input/output...")

    # Generate lookup for the parent bundles of all collections - these will be applied to non-aggregate products too.
    bundle_ancestry_by_collection_lidvid = {
        record.lidvid: record.parent_bundle_lidvids for record in collection_ancestry_records
    }

    using_cache_override = bool(os.environ.get("TMP_OVERRIDE_DIR"))
    if using_cache_override:
        on_disk_cache_dir: str = os.environ.get("TMP_OVERRIDE_DIR")  # type: ignore
        os.makedirs(on_disk_cache_dir, exist_ok=True)
    else:
        on_disk_cache_dir = tempfile.mkdtemp(prefix="ancestry-merge-dump_")
    log.debug(f"dumping partial non-aggregate ancestry result-sets to {on_disk_cache_dir}")

    collection_refs_query_docs = get_nonaggregate_ancestry_records_query(client, registry_db_mock)
    touched_ref_documents: List[RefDocBookkeepingEntry] = []

    baseline_memory_usage = psutil.virtual_memory().percent
    user_configured_max_memory_usage = AncestryRuntimeConstants.max_acceptable_memory_usage
    available_processing_memory = user_configured_max_memory_usage - baseline_memory_usage
    disk_dump_memory_threshold = baseline_memory_usage + (
        available_processing_memory / 3.0
    )  # peak expected memory use is during merge, where two dump files are open simultaneously. 1.0 added for overhead after testing revealed 2.5 was insufficient
    log.info(
        f"Max memory use set at {user_configured_max_memory_usage}% - dumps will trigger when memory usage reaches {disk_dump_memory_threshold:.1f}%"
    )
    chunk_size_max = (
        0  # populated based on the largest encountered chunk.  see split_chunk_if_oversized() for explanation
    )

    most_recent_attempted_collection_lidvid: Union[PdsLidVid, None] = None
    nonaggregate_ancestry_records_by_lidvid = {}

    for doc in collection_refs_query_docs:
        try:
            collection_lidvid = PdsLidVid.from_string(doc["_source"]["collection_lidvid"])
            most_recent_attempted_collection_lidvid = collection_lidvid

            try:
                bundle_ancestry = bundle_ancestry_by_collection_lidvid[collection_lidvid]
            except KeyError:
                log.debug(
                    f'Failed to resolve history for page {doc.get("_id")} in index {doc.get("_index")} with collection_lidvid {collection_lidvid} - no such collection exists in registry.'
                )
                continue

            for nonaggregate_lidvid_str in doc["_source"]["product_lidvid"]:
                if nonaggregate_lidvid_str not in nonaggregate_ancestry_records_by_lidvid:
                    nonaggregate_ancestry_records_by_lidvid[nonaggregate_lidvid_str] = {
                        "lidvid": nonaggregate_lidvid_str,
                        "parent_collection_lidvids": set(),
                        "parent_bundle_lidvids": set(),
                    }

                record_dict = nonaggregate_ancestry_records_by_lidvid[nonaggregate_lidvid_str]
                record_dict["parent_bundle_lidvids"].update({str(id) for id in bundle_ancestry})
                record_dict["parent_collection_lidvids"].add(str(collection_lidvid))

                if psutil.virtual_memory().percent >= disk_dump_memory_threshold:
                    log.info(
                        f"Memory threshold {disk_dump_memory_threshold:.1f}% reached - dumping serialized history to disk for {len(nonaggregate_ancestry_records_by_lidvid)} products"
                    )
                    make_history_serializable(nonaggregate_ancestry_records_by_lidvid)
                    dump_history_to_disk(on_disk_cache_dir, nonaggregate_ancestry_records_by_lidvid)
                    chunk_size_max = max(
                        chunk_size_max, sys.getsizeof(nonaggregate_ancestry_records_by_lidvid)
                    )  # slightly problematic due to reuse of pointers vs actual values, but let's try it
                    nonaggregate_ancestry_records_by_lidvid = {}
                    last_dump_time = datetime.now()

            # mark collection for metadata update
            touched_ref_documents.append(
                RefDocBookkeepingEntry(id=doc["_id"], primary_term=doc["_primary_term"], seq_no=doc["_seq_no"])
            )

        except (ValueError, KeyError) as err:
            if (
                isinstance(err, KeyError)
                and most_recent_attempted_collection_lidvid not in bundle_ancestry_by_collection_lidvid
            ):
                probable_cause = f'[Probable Cause]: Collection primary document with id "{doc["_source"].get("collection_lidvid")}" not found in index {resolve_multitenant_index_name(client, "registry")} for {resolve_multitenant_index_name(client, "registry-refs")} doc with id "{doc.get("_id")}"'
            elif isinstance(err, ValueError):
                probable_cause = f'[Probable Cause]: Failed to parse collection and/or product LIDVIDs from document with id "{doc.get("_id")}" in index "{doc.get("_index")}" due to {type(err).__name__}: {err}'
            else:
                probable_cause = f"Unknown error due to {type(err).__name__}: {err}"

            log.warning(probable_cause)
            continue

    # don't forget to yield non-disk-dumped records
    make_history_serializable(nonaggregate_ancestry_records_by_lidvid)
    chunk_size_max = max(chunk_size_max, sys.getsizeof(nonaggregate_ancestry_records_by_lidvid))
    for history_dict in nonaggregate_ancestry_records_by_lidvid.values():
        try:
            yield AncestryRecord.from_dict(history_dict)
        except ValueError as err:
            log.warning(err)
    del nonaggregate_ancestry_records_by_lidvid
    gc.collect()

    # merge/yield the disk-dumped records
    remaining_chunk_filepaths = list(os.path.join(on_disk_cache_dir, fn) for fn in os.listdir(on_disk_cache_dir))
    disk_swap_space_utilized_gb = sum(os.stat(filepath).st_size for filepath in remaining_chunk_filepaths) / 1024**3
    log.info(
        f"On-disk swap comprised of {len(remaining_chunk_filepaths)} files totalling {disk_swap_space_utilized_gb:.1f}GB"
    )
    while len(remaining_chunk_filepaths) > 0:
        # use of pop() here is important - see comment in merge_matching_history_chunks() where
        # ancestry.utils.split_chunk_if_oversized() is called, for justification
        active_filepath = remaining_chunk_filepaths.pop()
        merge_matching_history_chunks(active_filepath, remaining_chunk_filepaths, max_chunk_size=chunk_size_max)

        records_from_file = load_partial_history_to_records(active_filepath)
        for record in records_from_file:
            yield record

    if not using_cache_override:
        shutil.rmtree(on_disk_cache_dir)

    # See race condition comment in function def
    update_refs_document_metadata(client, touched_ref_documents)


def update_refs_document_metadata(client: OpenSearch, docs: List[RefDocBookkeepingEntry]):
    """
    Write ancestry version metadata for all collection-page documents for which AncestryRecords were successfully
    produced.
    Subject to a race condition where this will be called when the final AncestryRecord is yielded, and therefore may
    write metadata before the final page of AncestryRecords is written to the db (which may fail).  This is an
    acceptably-small risk for now given that we can detect orphaned documents, but may need to be refactored later.
    """

    def generate_update(doc: RefDocBookkeepingEntry) -> Update:
        return Update(
            id=doc.id,
            primary_term=doc.primary_term,
            seq_no=doc.seq_no,
            content={SWEEPERS_ANCESTRY_VERSION_METADATA_KEY: SWEEPERS_ANCESTRY_VERSION},
        )

    updates = map(generate_update, docs)
    logging.info(
        f"Updating {len(docs)} registry-refs docs with {SWEEPERS_ANCESTRY_VERSION_METADATA_KEY}={SWEEPERS_ANCESTRY_VERSION}"
    )
    write_updated_docs(
        client,
        updates,
        index_name=resolve_multitenant_index_name(client, "registry-refs"),
        bulk_chunk_max_update_count=20000,
    )
    logging.info("registry-refs metadata update complete")
