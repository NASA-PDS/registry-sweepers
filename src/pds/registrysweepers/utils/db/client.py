import json
import os

import requests
from botocore.credentials import Credentials
from opensearchpy import OpenSearch
from opensearchpy import RequestsAWSV4SignerAuth
from opensearchpy import RequestsHttpConnection
from requests_aws4auth import AWS4Auth  # type: ignore


def get_opensearch_client_from_environment(verify_certs: bool = True) -> OpenSearch:
    """Extract necessary details from the existing (at time of development) runtime environment and construct a client"""
    # TODO: consider re-working these environment variables at some point

    endpoint_url_env_var_key = "PROV_ENDPOINT"
    userpass_env_var_key = "PROV_CREDENTIALS"
    iam_role_env_var_key = "SWEEPERS_IAM_ROLE_NAME"

    endpoint_url = os.environ.get(endpoint_url_env_var_key) or None
    if endpoint_url is None:
        raise EnvironmentError(f'env var "{endpoint_url_env_var_key}" is required')

    creds_str = os.environ.get("PROV_CREDENTIALS") or None
    iam_role_name = os.environ.get(iam_role_env_var_key) or None

    if creds_str is not None and iam_role_name is not None:
        raise EnvironmentError(f'Only one of env vars ["{userpass_env_var_key}", "{iam_role_env_var_key}"] may be set')
    if creds_str is not None:
        creds_dict = json.loads(creds_str)
        username, password = creds_dict.popitem()
        return get_userpass_opensearch_client(endpoint_url, username, password, verify_certs)
    elif iam_role_name is not None:
        return get_aws_aoss_client_from_ssm(endpoint_url, iam_role_name)
    else:
        raise EnvironmentError(f'One of env vars ["{userpass_env_var_key}", "{iam_role_env_var_key}"] must be set')


def get_userpass_opensearch_client(
    endpoint_url: str, username: str, password: str, verify_certs: bool = True
) -> OpenSearch:
    try:
        scheme, host, port_str = endpoint_url.replace("://", ":", 1).split(":")
        port = int(port_str)
    except ValueError:
        raise ValueError(
            f'Failed to parse (scheme, host, port) from endpoint value - expected value of form <scheme>://<host>:<port> (got "{endpoint_url}")'
        )

    use_ssl = scheme.lower() == "https"
    auth = (username, password)

    return OpenSearch(
        hosts=[{"host": host, "port": int(port)}], http_auth=auth, use_ssl=use_ssl, verify_certs=verify_certs
    )


def get_aws_credentials_from_ssm(iam_role_name: str) -> Credentials:
    response = requests.get(f"http://169.254.169.254/latest/meta-data/iam/security-credentials/{iam_role_name}")
    content = response.json()

    access_key_id = content["AccessKeyId"]
    secret_access_key = content["SecretAccessKey"]
    token = content["Token"]
    credentials = Credentials(access_key_id, secret_access_key, token)

    return credentials


def get_aws_aoss_client_from_ssm(endpoint_url: str, iam_role_name: str) -> OpenSearch:
    # https://opensearch.org/blog/aws-sigv4-support-for-clients/
    credentials = get_aws_credentials_from_ssm(iam_role_name)
    auth = RequestsAWSV4SignerAuth(credentials, "us-west-2")
    return get_aws_opensearch_client(endpoint_url, auth)


def get_aws_opensearch_client(endpoint_url: str, auth: AWS4Auth) -> OpenSearch:
    try:
        scheme, host = endpoint_url.replace("://", ":", 1).split(":")
    except ValueError:
        raise ValueError(
            f'Failed to parse (scheme, host) from endpoint value - expected value of form <scheme>://<host>:<port> (got "{endpoint_url}")'
        )

    use_ssl = scheme.lower() == "https"

    return OpenSearch(
        hosts=[{"host": host, "port": 443}],
        http_auth=auth,
        use_ssl=use_ssl,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
    )
