#!/usr/bin/env bash
# backup_documents.sh
# Statement of purpose: extract document identifiers from a stored search
# response, then retrieve and persist the full content of each such
# document from the target index. Termination of ambiguity is complete.

set -euo pipefail

# ---- configuration ----------------------------------------------------
HOST="https://localhost:9200"
INDEX="geo-registry-structured"   # corrected to match observed data; see notes
AUTH="admin:admin"

RESPONSE_FILE="${1:-search_response.json}"   # the JSON blob you pasted, saved to disk
IDS_FILE="doc_ids.txt"
DOCS_FILE="documents.ndjson"                  # one JSON object per line: {"_id":..., "_source":...}

# ---- dependency check --------------------------------------------------
command -v jq >/dev/null 2>&1 || { echo "Meatbag error: 'jq' is not installed."; exit 1; }
[ -f "$RESPONSE_FILE" ] || { echo "Meatbag error: $RESPONSE_FILE not found."; exit 1; }

## ---- 1. extract document IDs from the search response ------------------
#jq -r '.hits.hits[]._id' "$RESPONSE_FILE" > "$IDS_FILE"
#
COUNT=$(wc -l < "$IDS_FILE")
#echo "Extracted $COUNT document identifier(s) into $IDS_FILE."

# ---- 2. retrieve and store the content of each document -----------------
: > "$DOCS_FILE"
while IFS= read -r id; do
    [ -z "$id" ] && continue
    resp=$(curl -sS -k -u "$AUTH" "$HOST/$INDEX/_doc/$id")

    found=$(echo "$resp" | jq -r '.found // false')
    if [ "$found" != "true" ]; then
        echo "Warning: document not found or retrieval failed: $id" >&2
        continue
    fi

    echo "$resp" | jq -c '{_id: ._id, _source: ._source}' >> "$DOCS_FILE"
    echo "Retrieved: $id"
done < "$IDS_FILE"

echo "Stored $(wc -l < "$DOCS_FILE") document(s) in $DOCS_FILE."
