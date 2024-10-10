from opensearchpy import OpenSearch


def ensure_index_mapping(client: OpenSearch, index_name: str, property_name: str, property_type: str):
    """
    Provides an easy-to-use wrapper for ensuring the presence of a given property name/type in a given index.
    N.B. This cannot change the type of a mapping, as modification/deletion is impossible in ES/OS.  If the mapping
    already exists, matching type or not, the function will gracefully fail and log an HTTP400 error.
    """
    client.indices.put_mapping(index=index_name, body={"properties": {property_name: {"type": property_type}}})
