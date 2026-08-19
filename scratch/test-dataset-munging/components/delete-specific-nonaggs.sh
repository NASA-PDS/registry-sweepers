#!/bin/bash

#curl -k -u admin:admin -X POST \
#  "https://localhost:9200/geo-registry-structured/_delete_by_query" \
#  -H "Content-Type: application/json" \
#  -d '{
#    "query": {
#      "bool": {
#        "must": [
#          { "term": { "ops:Registry_Sweepers.ops:ancestor_refs": "urn:nasa:pds:mars2020.spice:spice_kernels::3.0" } }
#        ]
#      }
#    }
#  }'

awk '{print "{\"delete\":{\"_index\":\"geo-registry-structured\",\"_id\":\""$0"\"}}"}' doc_ids.txt | \
curl -sS -k -u "admin:admin" -X POST "https://localhost:9200/_bulk" \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary @-