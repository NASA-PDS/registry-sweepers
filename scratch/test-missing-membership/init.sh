#!/usr/bin/env bash
# restore_documents.sh
# Statement of purpose: reindex the previously harvested document content
# back into the target index. Should you require an efficient meatbag,
# use the bulk variant further down. Otherwise, the sequential loop
# suffices.

set -euo pipefail

# ---- configuration ----------------------------------------------------
HOST="https://localhost:9200"
AUTH="admin:admin"

DOCS_FILE="${1:-init.ndjson}"


command -v jq >/dev/null 2>&1 || { echo "Meatbag error: 'jq' is not installed."; exit 1; }
[ -f "$DOCS_FILE" ] || { echo "Meatbag error: $DOCS_FILE not found."; exit 1; }

# ---- sequential restore (simple, one request per document) -------------
while IFS= read -r line; do
    [ -z "$line" ] && continue
    id=$(echo "$line" | jq -r '.lidvid')
    source=$(echo "$line" | jq -c '._source')
    index=$(echo "$line" | jq -r '.index')

    echo "Restoring: $id to index $index content $source"


    status=$(curl -sS -k -u "$AUTH" -o /dev/null -w '%{http_code}' \
        -X PUT "$HOST/$index/_doc/$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1], safe=''))" "$id")" \
        -H 'Content-Type: application/json' \
        -d "$source")

    if [[ "$status" =~ ^2 ]]; then
        echo "Restored: $id"
    else
        echo "Warning: failed to restore $id (HTTP $status)" >&2
    fi
done < "$DOCS_FILE"

echo "Restore pass complete."