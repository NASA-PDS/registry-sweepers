import json
import os
from typing import Union

from opensearchpy.client import OpenSearch


def get_opensearch_client_from_environment(verify_certs: bool = True) -> OpenSearch:
    """Extract necessary details from the existing (at time of development) runtime environment and construct a client"""
    # TODO: consider re-working these environment variables at some point

    endpoint_url = os.environ["PROV_ENDPOINT"]
    creds_str = os.environ.get("PROV_CREDENTIALS", "").strip()
    if len(creds_str) > 0:
        creds_dict = json.loads(creds_str)
        username, password = creds_dict.popitem()
    else:
        username, password = None, None

    return get_opensearch_client(endpoint_url, username, password, verify_certs)


def get_opensearch_client(
    endpoint_url: str, username: Union[str, None] = None, password: Union[str, None] = None, verify_certs: bool = True
) -> OpenSearch:
    try:
        scheme, host, port_str = endpoint_url.replace("://", ":", 1).split(":")
        port = int(port_str)
    except ValueError:
        raise ValueError(
            f'Failed to parse (scheme, host, port) from endpoint value - expected value of form <scheme>://<host>:<port> (got "{endpoint_url}")'
        )

    use_ssl = scheme.lower() == "https"
    auth = (username, password) if username is not None else None

    return OpenSearch(
        hosts=[{"host": host, "port": int(port)}], http_auth=auth, use_ssl=use_ssl, verify_certs=verify_certs
    )
