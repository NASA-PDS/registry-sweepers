import json
import os
import urllib.parse

import requests

base_dir = os.path.expanduser('~/Documents/opensearch-diff-dumps')
relevant_filepaths = [os.path.join(base_dir, fn) for fn in os.listdir(base_dir) if os.path.splitext(fn)[1] == '.diff' \
                      and os.stat(os.path.join(base_dir, fn)).st_size != 0]

host_endpoints = {
    'img': 'https://search-img-prod-tlnl5qlzgk5iknwhiemnc6aogy.us-west-2.es.amazonaws.com:443',
    'rms': 'https://search-rms-prod-hgkpolys7ww6cdoogbx5gsfy6m.us-west-2.es.amazonaws.com:443',
    'psa': 'https://search-psa-prod-fec4omrmor3hhu5cflf7zigvtm.us-west-2.es.amazonaws.com:443',
    'en': 'https://search-en-prod-di7dor7quy7qwv3husi2wt5tde.us-west-2.es.amazonaws.com',
    'geo': 'https://search-geo-prod-6iz6lwiw6luyffpsq52ndsrtbu.us-west-2.es.amazonaws.com:443'
}

for fp in relevant_filepaths:
    aoss_index_name = os.path.splitext(os.path.split(fp)[1])[0]
    node_name, es_index_name = aoss_index_name.split('-', maxsplit=1)
    host_url = host_endpoints[node_name]

    output_filepath = f'{os.path.splitext(fp)[0]}.create'

    with open(fp) as in_f, \
            open(output_filepath, 'w+') as out_f:
        for line in in_f.readlines():
            lidvid = line.strip()
            doc_url = f'{host_url}/{es_index_name}/_doc/{urllib.parse.quote_plus(lidvid)}'

            print(f'Fetching {doc_url}')
            user = os.environ['ES_USER']
            password = os.environ['ES_PASSWORD']
            response = requests.get(url=doc_url, auth=(user, password))
            content = response.json()

            create_statement_body = {'create': {'_index': aoss_index_name, '_id': content['_id']}}
            create_content_body = content['_source']

            out_f.write(json.dumps(create_statement_body) + '\n')
            out_f.write(json.dumps(create_content_body) + '\n')
