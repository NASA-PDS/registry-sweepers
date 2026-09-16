#!/usr/bin/env bash
# restore_documents.sh
# Statement of purpose: reindex the previously harvested document content
# back into the target index. Should you require an efficient meatbag,
# use the bulk variant further down. Otherwise, the sequential loop
# suffices.

set -euo pipefail

# ---- configuration ----------------------------------------------------
HOST="https://localhost:9200"
INDEX="geo-registry-structured"   # corrected to match observed data; see notes
AUTH="admin:admin"

DOCS_FILE="${1:-documents.ndjson}"

command -v jq >/dev/null 2>&1 || { echo "Meatbag error: 'jq' is not installed."; exit 1; }
[ -f "$DOCS_FILE" ] || { echo "Meatbag error: $DOCS_FILE not found."; exit 1; }

# ---- sequential restore (simple, one request per document) -------------
while IFS= read -r line; do
    [ -z "$line" ] && continue
    id=$(echo "$line" | jq -r '._id')
    source=$(echo "$line" | jq -c '._source')

    status=$(curl -sS -k -u "$AUTH" -o /dev/null -w '%{http_code}' \
        -X PUT "$HOST/$INDEX/_doc/$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1], safe=''))" "$id")" \
        -H 'Content-Type: application/json' \
        -d "$source")

    if [[ "$status" =~ ^2 ]]; then
        echo "Restored: $id"
    else
        echo "Warning: failed to restore $id (HTTP $status)" >&2
    fi
done < "$DOCS_FILE"

echo "Restore pass complete."

# ---- alternative: bulk restore (faster for large document counts) ------
# Uncomment to use the _bulk API instead of the loop above.
#
# BULK_FILE="bulk_payload.ndjson"
# : > "$BULK_FILE"
# while IFS= read -r line; do
#     id=$(echo "$line" | jq -r '._id')
#     source=$(echo "$line" | jq -c '._source')
#     printf '%s\n' "{\"index\":{\"_index\":\"$INDEX\",\"_id\":\"$id\"}}" >> "$BULK_FILE"
#     printf '%s\n' "$source" >> "$BULK_FILE"
# done < "$DOCS_FILE"
#
# curl -sS -k -u "$AUTH" -X POST "$HOST/_bulk" \
#     -H 'Content-Type: application/x-ndjson' \
#     --data-binary "@$BULK_FILE"
