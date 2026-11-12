# Index Mapping Files

The DB test suite requires three OpenSearch index mapping files in this directory.
These files are **not committed** because they are sourced from the production registry
deployment and must be supplied manually.

## Expected files

| File | Index type |
|---|---|
| `registry.json` | Main registry product index |
| `registry-refs.json` | Collection membership refs index |
| `registry-dd.json` | Data dictionary index |

## Accepted formats

Each file may be:

- A **full create-index body**: `{"settings": {...}, "mappings": {"properties": {...}}}`
- A **bare mappings block**: `{"mappings": {"properties": {...}}}`
- A **bare properties block**: `{"properties": {...}}`

The loader normalises all three to the full create-index form before creating test indices.

## Source

<!-- TODO: record the source repo/version these mappings were taken from -->
<!-- e.g. "Taken from registry-mgr repo tag v3.2.0, file config/index_create_registry.json" -->

## Effect of missing files

- **Locally** (without `CI=1`): DB tests are **skipped** with a message naming the missing files.
- **In CI** (`CI=1`): DB tests **fail** with the same message.
