import logging
from itertools import chain
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from opensearchpy import OpenSearch
from pds.registrysweepers.ancestry.ancestryrecord import AncestryRecord
from pds.registrysweepers.ancestry.constants import ANCESTRY_REFS_METADATA_KEY
from pds.registrysweepers.ancestry.generation import process_collection_ancestries_for_nonaggregates
from pds.registrysweepers.ancestry.generation import process_collection_bundle_ancestry
from pds.registrysweepers.ancestry.productupdaterecord import ProductUpdateRecord
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

log = logging.getLogger(__name__)


def run(
        client: OpenSearch,
        log_filepath: Union[str, None] = None,
        log_level: int = logging.INFO,
        ancestry_records_accumulator: Optional[List[AncestryRecord]] = None,
        bulk_updates_sink: Optional[List[Tuple[str, Dict[str, List]]]] = None,
):
    configure_logging(filepath=log_filepath, log_level=log_level)

    log.info(f"Starting ancestry v{SWEEPERS_ANCESTRY_VERSION} sweeper processing...")

    log.info("Updating bundle ancestries for collections...")
    bundle_and_collection_update_records = process_collection_bundle_ancestry(client)

    logging.info("Updating collection ancestries for non-aggregate products...")
    collection_nonaggregate_refs_updates = process_collection_ancestries_for_nonaggregates(client)

    product_update_records_to_write = filter(lambda r: not r._skip_write, chain(bundle_and_collection_update_records,
                                                                                collection_nonaggregate_refs_updates))
    updates = convert_records_to_updates(
        product_update_records_to_write, ancestry_records_accumulator, bulk_updates_sink
    )

    # TODO: BOOKMARK LMAO - CONTINUE WORK HERE VVV

    if bulk_updates_sink is None:
        log.info("Ensuring metadata keys are present in database index...")
        for metadata_key in [
            ANCESTRY_REFS_METADATA_KEY,
            SWEEPERS_ANCESTRY_VERSION_METADATA_KEY,
        ]:
            ensure_index_mapping(client, resolve_multitenant_index_name(client, "registry"), metadata_key, "keyword")

        for metadata_key in [
            # TODO: need to check whether values for these are actually updated for the refs docs, and whether they
            #  should even be - edunn 20251112
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

    else:
        # consume generator to dump bulk updates to sink
        for _ in updates:
            pass

    # TODO: reimplement orphan checking for ancestry sweeper - edunn 20251112
    log.critical("Skipping checks for for orphaned documents - requires reimplementation")
    # index_names = [resolve_multitenant_index_name(client, index_label) for index_label in ["registry", "registry-refs"]]
    # for index_name in index_names:
    #     if log.isEnabledFor(logging.DEBUG):
    #         orphaned_docs = get_orphaned_documents(client, registry_mock_query_f, index_name)
    #         orphaned_doc_ids = [doc.get("_id") for doc in orphaned_docs]
    #         orphaned_doc_ids_str = str(orphaned_doc_ids)
    #         orphaned_doc_count = len(orphaned_doc_ids)
    #     else:
    #         orphaned_doc_ids_str = "<run with debug logging enabled to view list of orphaned lidvids>"
    #
    #         # Currently, mocks are only implemented for iterating over document collections, not accessing the
    #         # enclosing query response metadata.  This is a shortcoming which should be addressed, but in the meantime
    #         # this bandaid will allow functional tests to complete when a client is not provided, i.e. during functional
    #         # testing.
    #         # TODO: refactor mock framework to provide access to arbitrary queries, not just the hits themselves
    #         def orphan_counter_mock(_, __):
    #             return -1
    #
    #         orphan_counter_f = get_orphaned_documents_count if client is not None else orphan_counter_mock
    #         orphaned_doc_count = orphan_counter_f(client, index_name)
    #
    #     if orphaned_doc_count > 0:
    #         log.warning(
    #             f'Detected {orphaned_doc_count} orphaned documents in index "{index_name} - please inform developers": {orphaned_doc_ids_str}'
    #         )

    log.info("Ancestry sweeper processing complete!")


def convert_records_to_updates(
        update_records: Iterable[ProductUpdateRecord],
        update_records_accumulator=None,
        bulk_updates_sink=None,
) -> Iterable[Update]:
    """
    Given a collection of ProductUpdateRecords, yield corresponding Update objects.

    Unlike prior implementations, this function does not have to reconcile deferred updates, as updates are now
    accumulative and do not have to be written to the db in a single operation

    """
    log.info("Generating ancestry document bulk updates for ProductUpdateRecords...")

    for record in update_records:
        # Tee the stream of records into the accumulator, if one was provided (functional testing).
        if update_records_accumulator is not None:
            update_records_accumulator.append(record)

        update = update_from_record(record)

        # Tee the stream of bulk update KVs into the accumulator, if one was provided (functional testing).
        if bulk_updates_sink is not None:
            bulk_updates_sink.append(update)

        yield update


if __name__ == "__main__":
    cli_description = f"""

    Update registry records for ancestry-pending products with up-to-date direct ancestry metadata ({ANCESTRY_REFS_METADATA_KEY}).

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
