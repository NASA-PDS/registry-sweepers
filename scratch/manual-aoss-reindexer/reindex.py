import logging
import os

from pds.registrysweepers.utils.db import query_registry_db_with_search_after, write_updated_docs
from pds.registrysweepers.utils.db.client import get_opensearch_client_from_environment
from pds.registrysweepers.utils.db.indexing import ensure_index_mapping
from pds.registrysweepers.utils.db.update import Update

from pds.registrysweepers.driver import run as run_sweepers

if __name__ == '__main__':

    src_node_name = 'geo'
    dest_pseudonode_name = f'edunn-{src_node_name}'  # TODO: change this from 'edunn' to 'temp'

    # set node id env var to facilitate sweepers
    os.environ["MULTITENANCY_NODE_ID"] = dest_pseudonode_name

    # change these to use a resolution function later - need to decouple resolve_multitenant_index_name() from env var
    src_index_name = f'{src_node_name}-registry'
    dest_index_name = f'{dest_pseudonode_name}-registry'

    with get_opensearch_client_from_environment() as client:

        # ensure that all necessary preconditions for safe/successful reindexing are met
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

        # TODO: Implement consistency check to ensure that all data was successfully migrated

        # Run sweepers on the migrated data
        try:
            run_sweepers()
        except Exception as err:
            logging.error(f'Post-reindex sweeper execution failed with {err}')
            exit(1)