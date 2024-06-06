import os

from pds.registrysweepers.utils.db import query_registry_db_with_search_after
from pds.registrysweepers.utils.db.client import get_opensearch_client_from_environment

index_configs = [
    {'name': 'registry', 'sort_fields': ['lidvid']},
    {'name': 'registry-refs', 'sort_fields': ['collection_lidvid', 'reference_type', 'batch_id']},
    {'name': 'registry-dd', 'sort_fields': ['_id']},
]

host_endpoints = [
    'https://search-img-prod-tlnl5qlzgk5iknwhiemnc6aogy.us-west-2.es.amazonaws.com:443',
    'https://search-rms-prod-hgkpolys7ww6cdoogbx5gsfy6m.us-west-2.es.amazonaws.com:443',
    'https://search-psa-prod-fec4omrmor3hhu5cflf7zigvtm.us-west-2.es.amazonaws.com:443',
    'https://search-en-prod-di7dor7quy7qwv3husi2wt5tde.us-west-2.es.amazonaws.com:443',
    'https://search-geo-prod-6iz6lwiw6luyffpsq52ndsrtbu.us-west-2.es.amazonaws.com:443'
]

def all_docs_query():
    return {
    "query": {
        "match_all": {}
    }
}

dump_parent_path = os.path.expanduser('~/Documents/opensearch-diff-dumps')
os.makedirs(dump_parent_path, exist_ok=True)

for endpoint in host_endpoints:
    os.environ['PROV_ENDPOINT'] = endpoint
    client = get_opensearch_client_from_environment()

    for index_config in index_configs:
        index_name = index_config['name']
        sort_fields = index_config['sort_fields']

        output_filename = os.environ['PROV_ENDPOINT'].split('//')[1].split('.')[0].split('-')[1] + '-' + index_name + '.esdump'
        output_filepath = os.path.join(dump_parent_path, output_filename)

        print(f'Dumping {endpoint}/{index_name} to {output_filepath}')

        all_docs = query_registry_db_with_search_after(client, index_name, all_docs_query(), {"includes": sort_fields}, sort_fields=sort_fields, request_timeout_seconds=20)
        with open(output_filepath, 'w+') as outfile:
            outfile.writelines(map(lambda doc: f'{doc.get("_id", "")}\n', all_docs))
