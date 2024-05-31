# Managed Opensearch to Serverless Opensearch Manual Sync

### Summary
Logstash migrations (from es to aoss in this case) may fail, resulting in missing documents.

The scripts in this directory facilitate the following process for an enumerated collection of hosts/nodes:

1. Dump all extant lidvids from es (managed opensearch) to disk - one file per index
2. Dump all extant lidvids from aoss to disk - one file per index
3. Prepare one-way diffs for each index to identify es docs which are missing from aoss (relies on lexically-sorted es/aoss dumps)
4. Fetch all documents for all lidvids in each diff, prepare them as OpenSearch bulk update content, and dump the content to disk
5. Upload the content to aoss

The scripts are aptly-named and should be reader-friendly.  If this needs to be used again as some sort of process, refactoring is highly recommended, as this was coded as a one-off.