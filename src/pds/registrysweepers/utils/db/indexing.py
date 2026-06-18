import logging

import retry
from opensearchpy import OpenSearch

logger = logging.getLogger(__name__)

MAPPING_POLL_MAX_TRIES = 10
MAPPING_POLL_DELAY_SECONDS = 1


class UnexpectedMappingTypeError(RuntimeError):
    pass


class MissingMappingError(RuntimeError):
    pass


def _get_existing_mapping_type(client: OpenSearch, index_name: str, property_name: str) -> str | None:
    """
    Returns the current mapped type for a given property, or None if the mapping does not yet exist.
    Supports nested paths using '/' as separator (e.g. 'ops:Provenance/ops:reindexed_at').
    """
    response = client.indices.get_mapping(index=index_name)
    properties = (
        response.get(index_name, {})
        .get("mappings", {})
        .get("properties", {})
    )
    parent, _, child = property_name.partition("/")
    if not child:
        return properties.get(parent, {}).get("type")
    nested_properties = properties.get(parent, {}).get("properties", {})
    return nested_properties.get(child, {}).get("type")


def _check_mapping(client: OpenSearch, index_name: str, property_name: str, expected_property_type: str) -> None:
    """
    Checks whether the mapping for a given property has propagated with the expected type.
    - Returns silently if the mapping is present and correct.
    - Raises UnexpectedMappingTypeError if the type is wrong.
    - Raises MissingMappingError if the mapping does not yet exist.
    """
    existing_type = _get_existing_mapping_type(client, index_name, property_name)

    if existing_type == expected_property_type:
        return
    elif existing_type is None:
        raise MissingMappingError(f"Mapping for '{property_name}' missing from index.")
    else:
        raise UnexpectedMappingTypeError(
            f"Mapping for '{property_name}' in index '{index_name}' exists with type "
            f"'{existing_type}', but expected '{expected_property_type}'. Cannot remap - manual reindexing to new index "
            f"is necessary."
        )


@retry.retry(
    exceptions=MissingMappingError,
    tries=MAPPING_POLL_MAX_TRIES,
    delay=MAPPING_POLL_DELAY_SECONDS,
    logger=logger,
)
def _await_mapping_creation(client: OpenSearch, index_name: str, property_name: str, property_type: str):
    _check_mapping(client, index_name, property_name, property_type)


def ensure_index_mapping(client: OpenSearch, index_name: str, property_name: str, property_type: str) -> None:
    """
    Ensures a given property mapping exists with the expected type in the given index,
    polling until the mapping is confirmed active or retries are exhausted.

    Raises:
        UnexpectedMappingTypeError: immediately, if the mapping exists with a conflicting type.
        MissingMappingError: if the mapping fails to propagate within the retry budget.
    """
    try:
        _check_mapping(client, index_name, property_name, property_type)
        return
    except MissingMappingError:
        logger.info(
            f"Mapping for '{property_name}' not yet present in index '{index_name}' - creating mapping with type {property_type}."
        )
        parent, _, child = property_name.partition("/")
        if child:
            mapping_body: dict = {"properties": {parent: {"properties": {child: {"type": property_type}}}}}
        else:
            mapping_body = {"properties": {parent: {"type": property_type}}}
        client.indices.put_mapping(index=index_name, body=mapping_body)

    _await_mapping_creation(client, index_name, property_name, property_type)
