import json
import logging
import os
import sys
from typing import Iterable, Union, List

import requests
from opensearchpy import RequestsAWSV4SignerAuth, OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

from pds.registrysweepers.utils.db import query_registry_db_with_search_after, write_updated_docs, Update, \
    _write_bulk_updates_chunk
from pds.registrysweepers.utils.db.client import get_opensearch_client_from_environment, get_aws_aoss_client_from_ssm, \
    get_aws_credentials_from_ssm

#START VALIDATED CODE
iam_role_name = 'temp-mcp-ec2-opensearch-role'
aoss_host = 'b3rqys09xmx9i19yn64i.us-west-2.aoss.amazonaws.com'

credentials = get_aws_credentials_from_ssm(iam_role_name)

auth = RequestsAWSV4SignerAuth(credentials, 'us-west-2', 'aoss')

# ## VALIDATED
# url = f'https://{aoss_host_endpoint_url}/_search'
# response = requests.post(url=url, data="", auth=auth)
# print(response.content)
# exit(0)

## VALIDATED
def get_client() -> OpenSearch:
    client = OpenSearch(
            hosts=[{"host": aoss_host, "port": 443}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )
    return client

base_dir = os.path.expanduser('~/upload-data')
relevant_filepaths = [os.path.join(base_dir, fn) for fn in os.listdir(base_dir) if os.path.splitext(fn)[1] == '.create']

with get_client() as client:
    for fp in relevant_filepaths:
        print(f'uploading {fp}')
        filename = os.path.split(fp)[-1]
        index_name = os.path.splitext(filename)[0] + '_test'  # this is actually not used, as the index is specified in the create statement
        with open(fp) as infile:
            _write_bulk_updates_chunk(client, index_name, map(lambda l: l.strip(), infile.readlines()))
