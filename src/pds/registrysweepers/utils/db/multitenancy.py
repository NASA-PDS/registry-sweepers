import os


def resolve_multitenant_index_name(index_type: str):
    supported_index_types = {"registry", "registry-refs"}
    node_id = os.environ.get("MULTITENANCY_NODE_ID", "").strip(" ")

    if node_id == "":
        return index_type
    elif index_type not in supported_index_types:
        raise ValueError(f'index_type "{index_type}" not supported (expected one of {supported_index_types})')
    else:
        return f"{node_id}-{index_type}"
