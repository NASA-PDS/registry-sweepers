import argparse
import logging
from itertools import batched
from itertools import chain
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Union

from opensearchpy import OpenSearch
from pds.registrysweepers.ancestry.ancestryrecord import AncestryRecord
from pds.registrysweepers.ancestry.bundlelinks import open_links_file
from pds.registrysweepers.ancestry.bundlelinks import tee_bundle_links
from pds.registrysweepers.ancestry.constants import ANCESTRY_REFS_METADATA_KEY
from pds.registrysweepers.ancestry.dryrun import AncestryDryRunReport
from pds.registrysweepers.ancestry.dryrun import summarise_dry_run
from pds.registrysweepers.ancestry.generation import process_collection_ancestries_for_nonaggregates
from pds.registrysweepers.ancestry.generation import process_collection_bundle_ancestry
from pds.registrysweepers.ancestry.productupdaterecord import ProductUpdateRecord
from pds.registrysweepers.ancestry.queries import get_deferred_update_documents_without_content
from pds.registrysweepers.ancestry.queries import get_orphaned_documents
from pds.registrysweepers.ancestry.utils import update_from_record
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY
from pds.registrysweepers.utils import configure_logging
from pds.registrysweepers.utils import parse_args
from pds.registrysweepers.utils.db import bulk_delete_documents
from pds.registrysweepers.utils.db import write_updated_docs
from pds.registrysweepers.utils.db.client import get_userpass_opensearch_client
from pds.registrysweepers.utils.db.indexing import ensure_index_mapping
from pds.registrysweepers.utils.db.multitenancy import resolve_multitenant_index_name
from pds.registrysweepers.utils.db.readonly import ReadOnlyOpenSearchClient
from pds.registrysweepers.utils.db.update import Update

log = logging.getLogger(__name__)


def run(
        client: OpenSearch,
        log_filepath: Union[str, None] = None,
        log_level: int = logging.INFO,
        ancestry_records_accumulator: Optional[List[AncestryRecord]] = None,
        bulk_updates_sink: Optional[List[Tuple[str, Dict[str, List]]]] = None,
        dry_run: bool = False,
        output_file: Optional[str] = None,
) -> Optional[AncestryDryRunReport]:
    """
    @param dry_run: read and generate everything, but write nothing to the registry; report what would be written
    @param output_file: path of a TSV (collection_lidvid, bundle_lidvid_added) listing the bundle links added to
        collections which already carry the current ancestry version - the links which would be added, in a dry run
    """
    configure_logging(filepath=log_filepath, log_level=log_level)

    if dry_run:
        # Everything below this point, including the reads, goes through a client which cannot write.
        client = ReadOnlyOpenSearchClient(client)  # type: ignore[assignment]

    log.info(f"Starting ancestry v{SWEEPERS_ANCESTRY_VERSION} sweeper processing{' (DRY RUN)' if dry_run else ''}...")

    product_update_records_to_write = generate_product_update_records(client)

    if dry_run:
        return summarise_dry_run(client, product_update_records_to_write, output_file=output_file)

    with open_links_file(output_file) as links_handle:
        if links_handle is not None:
            product_update_records_to_write = tee_bundle_links(product_update_records_to_write, links_handle)

        _write_updates(
            client, product_update_records_to_write, ancestry_records_accumulator, bulk_updates_sink
        )

    # Give any orphan documents their previously-resolved metadata
    # TODO: extract this to function edunn 20260916
    registry_index_name = resolve_multitenant_index_name(client, 'registry')
    deferred_index_name = f"{registry_index_name}-deferred-updates"

    # get all product documents which are missing ancestry metadata
    orphaned_docs = get_orphaned_documents(client, registry_index_name)
    orphaned_doc_ids: Set[str] = {doc.get("_id") for doc in orphaned_docs}  # type: ignore

    # get all update content which is waiting to be attached to a product document
    deferred_update_docs = get_deferred_update_documents_without_content(client, registry_index_name)
    deferred_update_doc_ids: Set[str] = {doc.get("_id") for doc in deferred_update_docs}  # type: ignore

    # get the set of doc_ids which can merge update content onto an extant product document
    pending_update_doc_ids: Set[str] = orphaned_doc_ids.intersection(deferred_update_doc_ids)  # type: ignore

    # perform fetch/merge/write in batches
    successfully_merged_doc_count = 0
    for batch in batched(pending_update_doc_ids, 1000):
        response = client.mget(index=f'{registry_index_name}-deferred-updates', body={'ids': batch})
        pending_content_docs = response['docs']

        pending_updates = iter(Update(id=doc['_id'], content=doc['_source']) for doc in pending_content_docs)

        write_updated_docs(
            client,
            pending_updates,
            index_name=resolve_multitenant_index_name(client, "registry"),
        )

        # Force a refresh to prevent read-after-write consistency from hiding the applied updates
        client.indices.refresh(index=registry_index_name)

    remaining_orphaned_docs = get_orphaned_documents(client, registry_index_name)
    remaining_orphaned_doc_ids = {doc.get("_id") for doc in remaining_orphaned_docs}
    remaining_orphaned_docs_count = len(remaining_orphaned_doc_ids)

    successfully_merged_doc_ids = pending_update_doc_ids.difference(remaining_orphaned_doc_ids)
    successfully_merged_doc_count += len(successfully_merged_doc_ids)

    if successfully_merged_doc_count > 0:
        log.info(f"Applied {successfully_merged_doc_count} deferred updates to documents in {registry_index_name}")

        # clean up applied updates
        bulk_delete_documents(client, deferred_index_name, successfully_merged_doc_ids)

    if remaining_orphaned_docs_count > 0:
        log.info(f'{remaining_orphaned_docs_count} orphaned documents remain in {registry_index_name}')

    log.info("Ancestry sweeper processing complete!")
    return None


def _write_updates(
        client: OpenSearch,
        update_records: Iterable[ProductUpdateRecord],
        ancestry_records_accumulator: Optional[List[AncestryRecord]],
        bulk_updates_sink: Optional[List[Tuple[str, Dict[str, List]]]],
) -> None:
    """Convert the records to document updates and write them to the registry (or dump them to the sink)"""
    updates = convert_records_to_updates(update_records, ancestry_records_accumulator, bulk_updates_sink)

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
            #  Update: appears not to be, but no time to confirm right now - edunn 20260630
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
            defer_on_missing_document=True,
        )

    else:
        # consume generator to dump bulk updates to sink
        for _ in updates:
            pass


def generate_product_update_records(client: OpenSearch) -> Iterable[ProductUpdateRecord]:
    """Generate the records of every document update the sweeper needs to write (the reads only; nothing is written)"""
    log.info("Updating bundle ancestries for collections...")
    bundle_and_collection_update_records = process_collection_bundle_ancestry(client)

    logging.info("Updating collection ancestries for non-aggregate products...")
    collection_nonaggregate_refs_updates = process_collection_ancestries_for_nonaggregates(client)

    return filter(lambda r: not r._skip_write, chain(bundle_and_collection_update_records,
                                                     collection_nonaggregate_refs_updates))


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


def _configure_cli(ap: argparse.ArgumentParser) -> None:
    ap.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="read and generate everything but write nothing to the registry; report the counts of updates which "
        "would be written",
    )
    ap.add_argument(
        "--output-file",
        default=None,
        help="write a TSV (collection_lidvid, bundle_lidvid_added) of the bundle links added to collections which "
        "already carry the current ancestry version to this file; with --dry-run, the links which would be added",
    )


def main() -> None:
    cli_description = f"""

    Update registry records for ancestry-pending products with up-to-date direct ancestry metadata ({ANCESTRY_REFS_METADATA_KEY}).

    Retrieves existing published LIDVIDs from the registry, determines membership identities for each LID, and writes updated docs back to registry db

    With --dry-run, nothing is written: the counts of the updates which would be written are reported instead.
    """

    args = parse_args(description=cli_description, configure_parser=_configure_cli)

    client = get_userpass_opensearch_client(
        endpoint_url=args.base_URL, username=args.username, password=args.password, verify_certs=not args.insecure
    )

    run(
        client=client,
        log_level=args.log_level,
        log_filepath=args.log_file,
        dry_run=args.dry_run,
        output_file=args.output_file,
    )


if __name__ == "__main__":
    main()
