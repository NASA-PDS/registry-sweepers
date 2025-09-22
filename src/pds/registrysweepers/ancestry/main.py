import json
import logging
import os
import tempfile
from itertools import chain
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Set
from typing import TextIO
from typing import Tuple
from typing import Union

from opensearchpy import OpenSearch
from pds.registrysweepers.ancestry.ancestryrecord import AncestryRecord
from pds.registrysweepers.ancestry.constants import METADATA_PARENT_BUNDLE_KEY
from pds.registrysweepers.ancestry.constants import METADATA_PARENT_COLLECTION_KEY
from pds.registrysweepers.ancestry.generation import generate_nonaggregate_and_collection_records_iteratively
from pds.registrysweepers.ancestry.generation import get_bundle_ancestry_records
from pds.registrysweepers.ancestry.generation import get_collection_ancestry_records
from pds.registrysweepers.ancestry.queries import get_existing_ancestry_for_product
from pds.registrysweepers.ancestry.queries import get_orphaned_documents
from pds.registrysweepers.ancestry.queries import get_orphaned_documents_count
from pds.registrysweepers.ancestry.typedefs import DbMockTypeDef
from pds.registrysweepers.ancestry.utils import update_from_record
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY
from pds.registrysweepers.utils import configure_logging
from pds.registrysweepers.utils import parse_args
from pds.registrysweepers.utils.db import write_updated_docs
from pds.registrysweepers.utils.db.client import get_userpass_opensearch_client
from pds.registrysweepers.utils.db.indexing import ensure_index_mapping
from pds.registrysweepers.utils.db.multitenancy import resolve_multitenant_index_name
from pds.registrysweepers.utils.db.update import Update
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid

log = logging.getLogger(__name__)


def run(
    client: OpenSearch,
    log_filepath: Union[str, None] = None,
    log_level: int = logging.INFO,
    registry_mock_query_f: Optional[Callable[[str], Iterable[Dict]]] = None,
    ancestry_records_accumulator: Optional[List[AncestryRecord]] = None,
    bulk_updates_sink: Optional[List[Tuple[str, Dict[str, List]]]] = None,
):
    configure_logging(filepath=log_filepath, log_level=log_level)

    log.info(f"Starting ancestry v{SWEEPERS_ANCESTRY_VERSION} sweeper processing...")

    bundle_records = list(get_bundle_ancestry_records(client, registry_mock_query_f))
    preresolved_collection_records = get_collection_ancestry_records(
        client, registry_mock_query_f
    )  # used to generate nonagg/collection records iteratively
    collection_and_nonaggregate_records = generate_nonaggregate_and_collection_records_iteratively(
        client, preresolved_collection_records, registry_mock_query_f
    )

    ancestry_records = chain(collection_and_nonaggregate_records, bundle_records)
    ancestry_records_to_write = filter(lambda r: not r.skip_write, ancestry_records)
    deferred_records_file = tempfile.NamedTemporaryFile(mode="w+", delete=False)
    updates = generate_updates(
        ancestry_records_to_write, deferred_records_file.name, ancestry_records_accumulator, bulk_updates_sink
    )

    if bulk_updates_sink is None:
        log.info("Ensuring metadata keys are present in database index...")
        for metadata_key in [
            METADATA_PARENT_BUNDLE_KEY,
            METADATA_PARENT_COLLECTION_KEY,
            SWEEPERS_ANCESTRY_VERSION_METADATA_KEY,
        ]:
            ensure_index_mapping(client, resolve_multitenant_index_name(client, "registry"), metadata_key, "keyword")

        for metadata_key in [
            SWEEPERS_ANCESTRY_VERSION_METADATA_KEY,
        ]:
            ensure_index_mapping(
                client, resolve_multitenant_index_name(client, "registry-refs"), metadata_key, "keyword"
            )

        log.info("Writing bulk updates to database...")
        write_updated_docs(
            client,
            updates,
            index_name=resolve_multitenant_index_name(client, "registry"),
        )
        log.info("Generating updates from deferred records...")
        deferred_updates = generate_deferred_updates(client, deferred_records_file.name, registry_mock_query_f)

        log.info("Writing deferred updates to database...")
        write_updated_docs(
            client,
            deferred_updates,
            index_name=resolve_multitenant_index_name(client, "registry"),
        )
    else:
        # consume generator to dump bulk updates to sink
        for _ in updates:
            pass

    log.info("Checking indexes for orphaned documents")
    index_names = [resolve_multitenant_index_name(client, index_label) for index_label in ["registry", "registry-refs"]]
    for index_name in index_names:
        if log.isEnabledFor(logging.DEBUG):
            orphaned_docs = get_orphaned_documents(client, registry_mock_query_f, index_name)
            orphaned_doc_ids = [doc.get("_id") for doc in orphaned_docs]
            orphaned_doc_ids_str = str(orphaned_doc_ids)
            orphaned_doc_count = len(orphaned_doc_ids)
        else:
            orphaned_doc_ids_str = "<run with debug logging enabled to view list of orphaned lidvids>"

            # Currently, mocks are only implemented for iterating over document collections, not accessing the
            # enclosing query response metadata.  This is a shortcoming which should be addressed, but in the meantime
            # this bandaid will allow functional tests to complete when a client is not provided, i.e. during functional
            # testing.
            # TODO: refactor mock framework to provide access to arbitrary queries, not just the hits themselves
            def orphan_counter_mock(_, __):
                return -1

            orphan_counter_f = get_orphaned_documents_count if client is not None else orphan_counter_mock
            orphaned_doc_count = orphan_counter_f(client, index_name)

        if orphaned_doc_count > 0:
            log.warning(
                f'Detected {orphaned_doc_count} orphaned documents in index "{index_name} - please inform developers": {orphaned_doc_ids_str}'
            )

    log.info("Ancestry sweeper processing complete!")


def generate_updates(
    ancestry_records: Iterable[AncestryRecord],
    deferred_records_filepath: str,
    ancestry_records_accumulator=None,
    bulk_updates_sink=None,
) -> Iterable[Update]:
    """
    Given a collection of AncestryRecords, yield corresponding Update objects, excluding any deferred updates, which
    must be generated seperately.

    Ideally, there should be one record per product, but this is not necessarily the case due to the potential of
    nonaggregate products to be shared between collections with different LIDs.  In that case, it is necessary to:
      - defer processing of all records which conflict with a previously-processed record
      - ensure all non-deferred records have been written to the db
      - retrieve the conflicting records which have been written to db, since the streaming collection-iterative
        approach prevents efficiently detecting conflicts until the first partial history is already processed/written.
      - merge all deferred/retrieved partial histories into a full history for each distinct product lidvid
      - yield those full-history updates, which will overwrite the partial histories initially written to db
    """
    updated_doc_ids: Set[str] = set()

    log.info("Generating document bulk updates for AncestryRecords...")

    # stream/yield Updates for AncestryRecords, deferring processing of conflicting AncestryRecords and storing them in
    # a temporary file
    with open(deferred_records_filepath, mode="w+") as deferred_records_file:
        for record in ancestry_records:
            # Tee the stream of records into the accumulator, if one was provided (functional testing).
            if ancestry_records_accumulator is not None:
                ancestry_records_accumulator.append(record)

            if record.lidvid.is_collection() and len(record.resolve_parent_bundle_lidvids()) == 0:
                log.warning(f"Collection {record.lidvid} is not referenced by any bundle.")

            update = update_from_record(record)

            # Tee the stream of bulk update KVs into the accumulator, if one was provided (functional testing).
            if bulk_updates_sink is not None:
                bulk_updates_sink.append((update.id, update.content))

            if update.id in updated_doc_ids:
                log.debug(
                    f"Multiple updates detected for doc_id {update.id} - deferring subsequent parts"
                    " - storing in {deferred_updates_file.name}"
                )
                deferred_records_file.write(json.dumps(record.to_dict(sort_lists=False)) + "\n")
                deferred_records_file.flush()
                continue

            updated_doc_ids.add(update.id)
            yield update


def generate_deferred_updates(
    client: OpenSearch, deferred_records_filepath: str, registry_db_mock: DbMockTypeDef = None
) -> Iterable[Update]:
    # Merge all deferred records with matching lidvids
    with open(deferred_records_filepath, "r") as deferred_records_file:  # type: ignore
        deferred_records_by_lidvid: Dict[PdsLidVid, AncestryRecord] = {}
        for l in deferred_records_file.readlines():
            record = AncestryRecord.from_dict(json.loads(l))
            if record.lidvid in deferred_records_by_lidvid:
                deferred_records_by_lidvid[record.lidvid].update_with(record)
            else:
                deferred_records_by_lidvid.update({record.lidvid: record})

    # Retrieve the first partial history (already written to db) for each lidvid, merge with its deferred history,
    # then yield a full-history-update for that lidvid
    for record in deferred_records_by_lidvid.values():
        doc = get_existing_ancestry_for_product(client, record.lidvid, registry_db_mock)
        try:
            partial_record_from_db = AncestryRecord.from_dict(
                {
                    "lidvid": doc["_source"]["lidvid"],
                    "parent_bundle_lidvids": doc["_source"][METADATA_PARENT_BUNDLE_KEY],
                    "parent_collection_lidvids": doc["_source"][METADATA_PARENT_COLLECTION_KEY],
                }
            )
            record.update_with(partial_record_from_db)
            update = update_from_record(record)
            yield update
        except (KeyError, ValueError) as err:
            log.error(f'Failed to parse valid AncestryRecord from document with id "{doc["_id"]}: {err}"')

        # TODO: Check that ancestry version is equal to current, throw if not.


if __name__ == "__main__":
    cli_description = f"""
    Update registry records for non-latest LIDVIDs with up-to-date direct ancestry metadata ({METADATA_PARENT_BUNDLE_KEY} and {METADATA_PARENT_COLLECTION_KEY}).

    Retrieves existing published LIDVIDs from the registry, determines membership identities for each LID, and writes updated docs back to registry db
    """

    args = parse_args(description=cli_description)
    client = get_userpass_opensearch_client(
        endpoint_url=args.base_URL, username=args.username, password=args.password, verify_certs=not args.insecure
    )

    run(
        client=client,
        log_level=args.log_level,
        log_filepath=args.log_file,
    )
