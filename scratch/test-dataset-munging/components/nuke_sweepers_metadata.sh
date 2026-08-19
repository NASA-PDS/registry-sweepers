#!/usr/bin/env bash
#
# Removes all top-level fields whose name starts with "ops:Registry_Sweepers"
# or "ops:Provenance" from every document in an OpenSearch index.
#
# Usage: ./remove_fields.sh
#
# Notes:
#  - Uses the _update_by_query API with a Painless script.
#  - -k skips TLS cert verification (adjust if you have a valid cert / CA bundle).
#  - conflicts=proceed lets it continue past version conflicts.
#  - slices=auto parallelizes the reindex work across shards.

set -euo pipefail

HOST="https://localhost:9200"
INDEX="geo-registry-structured"
AUTH="admin:admin"

echo "Removing 'ops:Registry_Sweepers*' and 'ops:Provenance*' fields from index '${INDEX}'..."

PAINLESS_SRC='Iterator it = ctx._source.keySet().iterator(); while (it.hasNext()) { String key = it.next(); if (key.startsWith(\"ops:Registry_Sweepers\") || key.startsWith(\"ops:Provenance\")) { it.remove(); } }'

RESPONSE=$(curl -sk -u "${AUTH}" -X POST \
  "${HOST}/${INDEX}/_update_by_query?conflicts=proceed&slices=auto&wait_for_completion=true&refresh=true" \
  -H 'Content-Type: application/json' \
  -d "{
    \"query\": { \"match_all\": {} },
    \"script\": {
      \"lang\": \"painless\",
      \"source\": \"${PAINLESS_SRC}\"
    }
  }")

echo "${RESPONSE}" | python3 -m json.tool 2>/dev/null || echo "${RESPONSE}"

echo "Done."