import json
import logging
import os
from typing import Union

import requests
from opensearchpy.client import OpenSearch

log = logging.getLogger()


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
    if not (username is None) == (password is None):
        raise ValueError(f"must provide both username and password, or neither")

    credentials_supplied = username is not None
    auth = (username, password) if credentials_supplied else None

    try:
        scheme, host, port_str = endpoint_url.replace("://", ":", 1).split(":")
        port = int(port_str)
    except ValueError:
        raise ValueError(
            f'Failed to parse (scheme, host, port) from endpoint value - expected value of form <scheme>://<host>:<port> (got "{endpoint_url}")'
        )

    use_ssl = scheme.lower() == "https"

    test_url = f"{endpoint_url}/_cat/indices"
    try:
        log.info(
            f'Testing access to OpenSearch endpoint {test_url} with{"out" if not credentials_supplied else ""} user/pass credentials...'
        )
        resp = requests.get(test_url, auth=auth)
        resp.raise_for_status()
        log.info(f"Access to {test_url} confirmed!")
    except requests.HTTPError as err:
        log.error(f"Request to {test_url} failed with {err}")
        raise err

    client = OpenSearch(
        hosts=[{"host": host, "port": int(port)}], http_auth=auth, use_ssl=use_ssl, verify_certs=verify_certs
    )

    log.info("Testing OpenSearch client with ping...")
    ping_response = client.ping()
    if ping_response:
        log.info(f"OpenSearch client ping test succeeded")
    else:
        msg = f"OpenSearch client ping test failed"
        log.error(msg)
        raise RuntimeError(msg)

    log.info("Testing OpenSearch client with index list operation...")
    try:
        client.indices.get("*")
        log.info(f"OpenSearch client index list test succeeded")
    except Exception as err:
        msg = f"OpenSearch client index list test failed with {err}"
        log.error(msg)
        raise RuntimeError(msg)

    return client
