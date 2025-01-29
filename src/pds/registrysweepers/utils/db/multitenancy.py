import logging
import os

from opensearchpy import OpenSearch


def resolve_multitenant_index_name(index_type: str):
    supported_index_types = {"registry", "registry-refs", "registry-dd"}
    node_id = os.environ.get("MULTITENANCY_NODE_ID", "").strip(" ")

    if node_id == "":
        return index_type
    elif index_type not in supported_index_types:
        raise ValueError(f'index_type "{index_type}" not supported (expected one of {supported_index_types})')
    else:
        return f"{node_id}-{index_type}"


def index_exists(client: OpenSearch, index_or_alias_name: str) -> bool:
    # counterintuitively, indices.exists does not return False if its argument is an alias.
    # Possibly a bug in opensearch-py: https://github.com/opensearch-project/opensearch-py/issues/888
    return client.indices.exists(index_or_alias_name) and not client.indices.exists_alias(index_or_alias_name)


def resolve_index_name_if_aliased(client: OpenSearch, index_or_alias_name: str) -> str:
    if index_exists(client, index_or_alias_name):
        return index_or_alias_name
    elif client.indices.exists_alias(index_or_alias_name):
        index_name = next(iter(client.indices.get_alias('index_or_alias_name').keys()))
        logging.debug(f'Resolved alias {index_or_alias_name} to index {index_name}')
        return index_name
    else:
        raise ValueError(f'Could not resolve index for index_or_alias_name "{index_or_alias_name}"')
