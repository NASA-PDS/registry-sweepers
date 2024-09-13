import json
import logging
import os
from typing import Union

import boto3
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
        try:
            creds_dict = json.loads(creds_str)
            username, password = creds_dict.popitem()
        except Exception as err:
            logging.error(err)
            raise ValueError(f'Failed to parse username/password from PROV_CREDENTIALS value "{creds_str}": {err}')

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


def get_aws_credentials_from_ec2_metadata_service(iam_role_name: str) -> Credentials:
    url = f"http://169.254.169.254/latest/meta-data/iam/security-credentials/{iam_role_name}"
    response = requests.get(url)
    if response.status_code != 200:
        raise RuntimeError(f"Got HTTP{response.status_code} when attempting to retrieve SSM credentials from {url}")
    content = response.json()

    access_key_id = content["AccessKeyId"]
    secret_access_key = content["SecretAccessKey"]
    token = content["Token"]
    credentials = Credentials(access_key_id, secret_access_key, token)

    return credentials


def get_assumed_identity() -> str:
    sts_client = boto3.client("sts")

    response = sts_client.get_caller_identity()

    arn = response["Arn"]
    logging.info(f"Caller ARN: {arn}")

    if "assumed-role" in arn:
        role_name = arn.split("/")[-2]
        logging.info(f"Role Name: {role_name}")
    else:
        role_name = None
        logging.info("The credentials are not associated with an assumed role.")

    return role_name


def get_aws_aoss_client_from_ssm(endpoint_url: str, iam_role_name: str) -> OpenSearch:
    # https://opensearch.org/blog/aws-sigv4-support-for-clients/
    logging.info(f'Expect role name "{iam_role_name}"')
    get_assumed_identity()

    credentials = boto3.Session().get_credentials()

    # TODO: DELETE ME DO NOT MERGE
    logging.info(credentials.token)
    # END TODO
    auth = RequestsAWSV4SignerAuth(credentials, "us-west-2", "aoss")
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
