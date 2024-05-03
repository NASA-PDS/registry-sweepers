import json
import logging
import os
from typing import Union

import boto3
import requests
from opensearchpy import AWSV4SignerAuth
from opensearchpy.client import OpenSearch
from opensearchpy.connection.http_requests import RequestsHttpConnection
from requests_aws4auth import AWS4Auth

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

    if not credentials_supplied:
        log.info("No credentials supplied - returning AWS IAM client")
        return get_aws_opensearch_client(endpoint_url)

    auth = (username, password)

    try:
        scheme, host, port_str = endpoint_url.replace("://", ":", 1).split(":")
        port = int(port_str)
    except ValueError:
        raise ValueError(
            f'Failed to parse (scheme, host, port) from endpoint value - expected value of form <scheme>://<host>:<port> (got "{endpoint_url}")'
        )

    use_ssl = scheme.lower() == "https"

    client = OpenSearch(
        hosts=[{"host": host, "port": int(port)}], http_auth=auth, use_ssl=use_ssl, verify_certs=verify_certs
    )

    return client


def get_aws_opensearch_client(endpoint_url: str) -> OpenSearch:
    try:
        scheme, host, port_str = endpoint_url.replace("://", ":", 1).split(":")
        port = int(port_str)
    except ValueError:
        raise ValueError(
            f'Failed to parse (scheme, host, port) from endpoint value - expected value of form <scheme>://<host>:<port> (got "{endpoint_url}")'
        )

    credentials = boto3.Session().get_credentials()
    auth = AWSV4SignerAuth(credentials, "us-west-2", "aoss")
    client = OpenSearch(
        hosts=[{"host": host, "port": int(port)}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
    )

    log.info("Testing AWS IAM OpenSearch client with search operation...")
    try:
        resp = client.search()
        log.info(f"OpenSearch client search test succeeded")
    except Exception as err:
        msg = f"OpenSearch client search test failed with {err}"
        log.error(msg)
        raise RuntimeError(msg)

    return client
