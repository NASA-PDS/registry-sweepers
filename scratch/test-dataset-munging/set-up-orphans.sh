#!/bin/bash

cd components

# perform idempotent setup
bash ./set-archived.sh
bash ./populate-bundle-lid-refs.sh

# reset deferral index
curl -sS -k -u "admin:admin" -X DELETE "https://localhost:9200/geo-registry-structured-deferred-updates"
bash ./create-deferral-index.sh

# reset sweepers metadata
bash ./nuke_sweepers_metadata.sh

# delete orphan non-aggs
bash ./delete-specific-nonaggs.sh

