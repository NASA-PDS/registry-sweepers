import os

from opensearchpy import RequestsAWSV4SignerAuth, OpenSearch, RequestsHttpConnection

from pds.registrysweepers.utils.db import _write_bulk_updates_chunk
from pds.registrysweepers.utils.db.client import get_aws_credentials_from_ssm

iam_role_name = 'temp-mcp-ec2-opensearch-role'
aoss_host = 'b3rqys09xmx9i19yn64i.us-west-2.aoss.amazonaws.com'

credentials = get_aws_credentials_from_ssm(iam_role_name)

auth = RequestsAWSV4SignerAuth(credentials, 'us-west-2', 'aoss')

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
        index_name = os.path.splitext(filename)[0] # this is actually not used, as the index is specified in the create statement
        with open(fp) as infile:
            _write_bulk_updates_chunk(client, index_name, map(lambda l: l.strip(), infile.readlines()))
