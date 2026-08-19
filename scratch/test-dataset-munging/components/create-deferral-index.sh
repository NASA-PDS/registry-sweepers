#!/bin/bash

curl -k -u admin:admin -X PUT "https://localhost:9200/geo-registry-structured-deferred-updates" \
  -H "Content-Type: application/json" \
  -d '{
  "mappings": {
    "dynamic": "false",
    "dynamic_templates": [
      {
        "strings": {
          "match_mapping_type": "string",
          "mapping": {
            "type": "keyword"
          }
        }
      }
    ]
  }
}'
