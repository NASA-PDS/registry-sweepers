import os

from opensearchpy import RequestsAWSV4SignerAuth, OpenSearch, RequestsHttpConnection

from pds.registrysweepers.utils.db import query_registry_db_with_search_after
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

index_configs = [
    {'name': 'registry', 'sort_fields': ['lidvid']},
    {'name': 'registry-refs', 'sort_fields': ['collection_lidvid', 'reference_type', 'batch_id']},
    {'name': 'registry-dd', 'sort_fields': ['_id']},
]

node_names = [
    'img',
    'rms',
    'psa',
    'en',
    'geo',
]

def all_docs_query():
    return {
    "query": {
        "match_all": {}
    }
}

dump_parent_path = os.path.expanduser('~/serverless-opensearch-dumps')
os.makedirs(dump_parent_path, exist_ok=True)

for node_name in node_names:
    for index_config in index_configs:
        index_type_name = index_config['name']
        index_name = f'{node_name}-{index_type_name}'
        sort_fields = index_config['sort_fields']

        output_filename = index_name
        output_filepath = os.path.join(dump_parent_path, output_filename)

        print(f'Dumping {index_name} to {output_filepath}')

        client = get_client()
        try:
            all_docs = query_registry_db_with_search_after(client, index_name, all_docs_query(), {"includes": sort_fields}, sort_fields=sort_fields, request_timeout_seconds=20)
            with open(output_filepath, 'w+') as outfile:
                outfile.writelines(map(lambda doc: f'{doc.get("_id", "")}\n', all_docs))
        except Exception as err:
            print(f'{index_name} failed with {err}')
        client.close()
