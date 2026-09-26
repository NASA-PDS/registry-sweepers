curl -k -u admin:admin -X POST \
  "https://localhost:9200/geo-registry/_delete_by_query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "match_all": {}
    }
  }'

curl -k -u admin:admin -X POST \
  "https://localhost:9200/geo-registry-refs/_delete_by_query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "match_all": {}
    }
  }'