import logging
import os
from typing import Iterator

from pds.registrysweepers.utils import configure_logging
from pds.registrysweepers.utils.db import query_registry_db_with_search_after, write_updated_docs, get_query_hits_count
from pds.registrysweepers.utils.db.client import get_opensearch_client_from_environment
from pds.registrysweepers.utils.db.indexing import ensure_index_mapping
from pds.registrysweepers.utils.db.update import Update

from pds.registrysweepers.driver import run as run_sweepers


def ensure_valid_state(dest_index_name: str):
    """Ensure that all necessary preconditions for safe/successful reindexing are met"""
    with get_opensearch_client_from_environment() as client:
        try:
            # ensure that the destination is a temporary index, to prevent inadvertently writing to a real index
            allowed_prefixes = {'temp', 'edunn'}
            if not any([dest_index_name.startswith(prefix) for prefix in allowed_prefixes]):
                raise ValueError(
                    f'Destination index name {dest_index_name} is not prefixed with one of {allowed_prefixes} and may not be the intended value - aborting')

            # ensure correct destination index configuration
            try:
                dynamic_mapping_disabled = client.indices.get(dest_index_name)[dest_index_name]['mappings'][
                                               'dynamic'] == 'false'
                if not dynamic_mapping_disabled:
                    raise ValueError

            except (KeyError, ValueError):
                raise RuntimeError(
                    f'Index "{dest_index_name}" is not correctly configured - "dynamic" mapping setting is not set to "false - aborting"')

            # other conditions may be populated later

        except Exception as err:
            logging.error(err)
            exit(1)


def migrate_bulk_data(src_index_name: str, dest_index_name: str):
    """Stream documents from source index to destination index, which may """
    if get_outstanding_document_count(src_index_name, dest_index_name, as_proportion=True) < 0.1:
        logging.warning(f'Less than 10% of documents outstanding - skipping bulk streaming migration stage')
        return

    with get_opensearch_client_from_environment() as client:
        try:
            # ensure that sort field is in mapping to facilitate execution of reindexing sweeper
            necessary_mappings = {
                'lidvid': 'keyword'
            }
            for property_name, property_mapping_type in necessary_mappings.items():
                ensure_index_mapping(client, dest_index_name, property_name, property_mapping_type)

            sort_keys = sorted(necessary_mappings.keys())

            docs = query_registry_db_with_search_after(client, src_index_name, {"query": {"match_all": {}}}, {},
                                                       sort_fields=sort_keys, request_timeout_seconds=20)
            updates = map(lambda doc: Update(id=doc['_id'], content=doc['_source']), docs)
            write_updated_docs(client, updates, dest_index_name, as_upsert=True)

            # TODO: Implement non-redundant pickup after partial completion, if possible
        except Exception as err:
            print(f'Reindex from {src_index_name} to {dest_index_name} failed: {err}')
            exit(1)


def ensure_doc_consistency(src_index_name: str, dest_index_name: str):
    """
    Ensure that all documents present in the source index are also present in the destination index.
    Discovers and fixes any quiet failures encountered during bulk document streaming.
    Yes, this could be accomplished within the bulk streaming, but implementation is simpler this way and there is less
    opportunity for error.
    """

    logging.info(f'Ensuring document consistency - {get_outstanding_document_count(src_index_name, dest_index_name)} documents remain to copy from {src_index_name} to {dest_index_name}')

    with get_opensearch_client_from_environment() as client:
        for doc_id in enumerate_outstanding_doc_ids(src_index_name, dest_index_name):
            try:
                src_doc = client.get(src_index_name, doc_id)
                client.create(dest_index_name, doc_id, src_doc['_source'])
                logging.info(f'Created missing doc with id {doc_id}')
            except Exception as err:
                logging.error(f'Failed to create doc with id "{doc_id}": {err}')

    if not get_outstanding_document_count(src_index_name, dest_index_name) == 0:
        raise RuntimeError(
            f'Failed to ensure consistency - there is remaining disparity in document count between indices "{src_index_name}" and "{dest_index_name}"')


def enumerate_outstanding_doc_ids(src_index_name: str, dest_index_name: str) -> Iterator[str]:
    with get_opensearch_client_from_environment() as client:
        
        pseudoid_field = "lidvid"

        src_docs = iter(query_registry_db_with_search_after(client, src_index_name, {"query": {"match_all": {}}},
                                                              {"includes": [pseudoid_field]},
                                                              sort_fields=[pseudoid_field], request_timeout_seconds=20))
        dest_docs = iter(query_registry_db_with_search_after(client, dest_index_name, {"query": {"match_all": {}}},
                                                               {"includes": [pseudoid_field]},
                                                               sort_fields=[pseudoid_field], request_timeout_seconds=20))

        # yield any documents which are present in source but not in destination
        try:
            src_doc = next(src_docs)
            dest_doc = next(dest_docs)

            while True:
                src_doc_pseudoid = src_doc["_source"][pseudoid_field]
                src_doc_id = src_doc["_id"]
                dest_doc_pseudoid = dest_doc["_source"][pseudoid_field]
                dest_doc_id = dest_doc["_id"]


                if src_doc_pseudoid < dest_doc_pseudoid:  # if id present in src but not dest
                    yield src_doc_id
                    src_doc = next(src_docs)
                elif dest_doc_pseudoid < src_doc_pseudoid:  # if id present in dest but not src
                    logging.warning(
                        f'Document with id "{dest_doc_pseudoid}" is present in destination index {dest_index_name} file but not in source index {src_index_name}')
                    dest_doc = next(dest_docs)
                else:  # if id is present in both files
                    src_doc = next(src_docs)
                    dest_doc = next(dest_docs)
        except StopIteration:
            pass

        # yield any remaining documents in source iterable
        try:
            src_doc = next(src_docs)
            while True:
                src_doc_pseudoid = src_doc["_source"][pseudoid_field]

                yield src_doc_pseudoid
                src_doc = next(src_docs)
        except StopIteration:
            pass


def get_outstanding_document_count(src_index_name: str, dest_index_name: str, as_proportion: bool = False) -> int:
    """return count(src) - count(dest)"""
    with get_opensearch_client_from_environment() as client:
        src_docs_count = get_query_hits_count(client, src_index_name, {"query": {"match_all": {}}})
        dest_docs_count = get_query_hits_count(client, dest_index_name, {"query": {"match_all": {}}})

    outstanding_docs_count = src_docs_count - dest_docs_count
    return (outstanding_docs_count / src_docs_count) if as_proportion else outstanding_docs_count


def run_sweepers():
    """Run sweepers on the migrated data"""
    try:
        run_sweepers()
    except Exception as err:
        logging.error(f'Post-reindex sweeper execution failed with {err}')
        exit(1)


if __name__ == '__main__':
    configure_logging(filepath=None, log_level=logging.INFO)

    src_node_name = 'geo'
    dest_pseudonode_name = f'edunn-{src_node_name}'  # TODO: change this from 'edunn' to 'temp'

    # set node id env var to facilitate sweepers
    os.environ["MULTITENANCY_NODE_ID"] = dest_pseudonode_name

    # change these to use a resolution function later - need to decouple resolve_multitenant_index_name() from env var
    src_index_name = f'{src_node_name}-registry'
    dest_index_name = f'{dest_pseudonode_name}-registry'

    ensure_valid_state(dest_index_name)
    migrate_bulk_data(src_index_name, dest_index_name)
    ensure_doc_consistency(src_index_name, dest_index_name)
    # run_sweepers()
