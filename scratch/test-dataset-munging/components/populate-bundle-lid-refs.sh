#!/bin/bash
set -euo pipefail

HOST="https://localhost:9200"
INDEX="geo-registry-structured"
AUTH="admin:admin"

# URL-encode a string (encodes ':' as %3A, etc.)
urlencode() {
  local string="${1}"
  local strlen=${#string}
  local encoded=""
  local pos c o
  for (( pos=0 ; pos<strlen ; pos++ )); do
    c=${string:$pos:1}
    case "$c" in
      [-_.~a-zA-Z0-9] ) o="${c}" ;;
      * ) printf -v o '%%%02X' "'$c" ;;
    esac
    encoded+="${o}"
  done
  echo "${encoded}"
}

update_doc() {
  local doc_id="$1"
  shift
  local collections=("$@")

  # Build JSON array of collection LIDs
  local json_array="["
  local first=true
  for c in "${collections[@]}"; do
    if [ "$first" = true ]; then
      first=false
    else
      json_array+=","
    fi
    json_array+="\"${c}\""
  done
  json_array+="]"

  local encoded_id
  encoded_id=$(urlencode "${doc_id}")

  echo "Updating ${doc_id} ..."
  curl -s -k -u "${AUTH}" -X POST \
    "${HOST}/${INDEX}/_update/${encoded_id}" \
    -H 'Content-Type: application/json' \
    -d "{\"doc\": {\"ref_lid_collection\": ${json_array}}}"
  echo ""
}

##############################
# mars2020.spice bundle
##############################
SPICE_COLLECTIONS=(
  "urn:nasa:pds:mars2020.spice:document"
  "urn:nasa:pds:mars2020.spice:spice_kernels"
)
update_doc "urn:nasa:pds:mars2020.spice::1.0" "${SPICE_COLLECTIONS[@]}"
update_doc "urn:nasa:pds:mars2020.spice::2.0" "${SPICE_COLLECTIONS[@]}"
update_doc "urn:nasa:pds:mars2020.spice::3.0" "${SPICE_COLLECTIONS[@]}"

##############################
# mars2020.spice_test1 bundle
##############################
SPICE_TEST1_COLLECTIONS=(
  "urn:nasa:pds:mars2020.spice_test1:document"
  "urn:nasa:pds:mars2020.spice_test1:spice_kernels"
)
update_doc "urn:nasa:pds:mars2020.spice_test1::1.0" "${SPICE_TEST1_COLLECTIONS[@]}"

##############################
# mars2020.spice_test2 bundle
##############################
SPICE_TEST2_COLLECTIONS=(
  "urn:nasa:pds:mars2020.spice_test2:document"
  "urn:nasa:pds:mars2020.spice_test2:spice_kernels"
)
update_doc "urn:nasa:pds:mars2020.spice_test2::1.0" "${SPICE_TEST2_COLLECTIONS[@]}"
update_doc "urn:nasa:pds:mars2020.spice_test2::2.0" "${SPICE_TEST2_COLLECTIONS[@]}"

echo "Done."