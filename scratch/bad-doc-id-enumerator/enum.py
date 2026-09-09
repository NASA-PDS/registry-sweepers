"""
Enumerate all document ids in an OpenSearch Serverless (AOSS) index using
Point-in-Time (PIT) + search_after pagination, and write them to a local file.

Target: AWS OpenSearch Serverless (AOSS).
- The Scroll API is NOT supported on AOSS (never has been) — PIT is the
  supported deep-pagination mechanism, available on AOSS since Nov 2024.
- The `_shard_doc` sort field (Elasticsearch's automatic PIT tiebreaker)
  is NOT supported in OpenSearch/AOSS. Must be specified some other way.
- Sorting on `_id` is discouraged (requires fielddata, expensive).
- We sort on `_doc` (Lucene index order) instead: it's unique within a
  frozen PIT, requires no fielddata, and needs no schema changes.

Assumes `client` is an already-instantiated opensearchpy.OpenSearch object
pointed at your AOSS collection endpoint (with AWS SigV4 auth configured).

run in sweepers venv with
MULTITENANCY_NODE_ID='*' PROV_ENDPOINT='<opensearch url>' SWEEPERS_IAM_ROLE_NAME='<iam role name>' nohup python -u enum.py > run.log 2>&1 < /dev/null &
"""

import json
import os


from pds.registrysweepers.utils.db.client import get_opensearch_client_from_environment
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid

def enumerate_ids(
    client,
    index_name: str,
    output_file: str,
    page_size: int = 1000,
    keep_alive: str = "2m",
) -> int:
    """
    Enumerate every document _id in `index_name` via PIT + search_after,
    writing one id per line to `output_file`.

    Returns the total count of ids written.
    """
    # 1. Open a Point in Time. This freezes a consistent view of the index
    #    for the duration of the enumeration.
    print(f'Opening PIT for index {index_name}')
    pit_resp = client.create_pit(index=index_name, params={"keep_alive": keep_alive})
    pit_id = pit_resp["pit_id"]
    print(f'Instantiated PIT with id {pit_id}')

    search_after = None
    total = 0
    total_read = 0
    print('enumeration begin')
    try:
        with open(output_file, "w", buffering=1) as f:
            while True:
                body = {
                    "size": page_size,
                    "query": {"match_all": {}},
                    "pit": {"id": pit_id, "keep_alive": keep_alive},
                    # `_doc` = Lucene's internal index order. It is stable
                    # and unique for the life of a PIT, needs no fielddata,
                    # and (unlike `_shard_doc`) is actually supported here.
                    "sort": [{"_doc": "asc"}],
                    "_source": False,  # we only want _id, don't ship the body
                }
                if search_after is not None:
                    body["search_after"] = search_after

                resp = client.search(body=body)
                hits = resp["hits"]["hits"]
                total_read += len(hits)
                print(f'STATUS: {total_read} docs checked')

                if not hits:
                    break

                    for hit in hits:
                        try:
                            _id = hit["_id"]
                            parsed = PdsLidVid.from_string(_id)
                        except Exception as err:
                            print(f'bad document: {_id}')
                            f.write(_id + "\n")
                            total += 1

                    # PIT id can rotate between requests; always carry the
                    # latest one forward.
                    pit_id = resp.get("pit_id", pit_id)
                    search_after = hits[-1]["sort"]

                    if len(hits) < page_size:
                        # Last page was partial -> nothing left to fetch.
                        break

                finally:
                # 2. Always release the PIT, win or lose. Serverless collections
                #    still consume resources holding a PIT open.
                print(f'PROCESS TERMINATING - DELETING {pit_id}')
                try:
                    client.delete_pit(body={"pit_id": [pit_id]})
                except Exception as e:
                    print(f"Warning: failed to delete PIT {pit_id}: {e}")

            return total

        if __name__ == "__main__":
            client = get_opensearch_client_from_environment()

            INDEX_NAME = f"{os.environ['MULTITENANCY_NODE_ID']}-registry"
            OUTPUT_FILE = "doc_ids.txt"

            count = enumerate_ids(client, INDEX_NAME, OUTPUT_FILE)
            print(f"Wrote {count} document ids to {OUTPUT_FILE}")