#!/usr/bin/env bash
# Bulk-set "ops:Tracking_Meta"."ops:archive_status" = "archived"
# on every document in the OpenSearch index `geo-registry-structured`.
#
# Adjust HOST/PORT below if your cluster isn't on localhost:9200.

set -euo pipefail

HOST="https://localhost:9200"
INDEX="geo-registry-structured"
AUTH="admin:admin"

curl -sk -u "${AUTH}" -X POST \
  "${HOST}/${INDEX}/_search" \
  -H 'Content-Type: application/json' \
  -d '{
    "query": { "match_all": {} },
    "_source": ["_id"]
  }'
echo