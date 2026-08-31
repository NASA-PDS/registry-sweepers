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
  "${HOST}/${INDEX}/_update_by_query?conflicts=proceed&refresh=true&wait_for_completion=true" \
  -H 'Content-Type: application/json' \
  -d '{
    "query": { "match_all": {} },
    "script": {
      "lang": "painless",
      "source": "if (ctx._source[\u0027ops:Tracking_Meta\u0027] == null) { ctx._source[\u0027ops:Tracking_Meta\u0027] = new HashMap(); } ctx._source[\u0027ops:Tracking_Meta\u0027][\u0027ops:archive_status\u0027] = \u0027archived\u0027;"
    }
  }'
echo
