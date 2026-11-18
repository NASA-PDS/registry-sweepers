# Index Mapping Files

The DB test suite creates each test's indices (`<node>-registry-structured`, `-refs`, `-dd`)
from the three mapping files in this directory. They are committed, and are the only source
of mappings for the tests: nothing is fetched or generated at test time.

## Files

| File | Index type |
|---|---|
| `registry.json` | Main registry product index |
| `registry-refs.json` | Collection membership refs index |
| `registry-dd.json` | Data dictionary index |

## Source

The files are **structured-metadata** mappings (nested object fields, addressed in code with
dotted paths such as `ops:Harvest_Info.ops:harvest_date_time`), which is the only mode the DB
tests cover.

They were derived from the **legacy flat-schema** production mappings for `en-registry`,
`en-registry-refs` and `en-registry-dd` (`GET <index>/_mapping` output, with slash-joined
field names such as `ops:Harvest_Info/ops:harvest_date_time`) using
[`convert_flat_to_structured.py`](convert_flat_to_structured.py):

```sh
python tests/resources/mappings/convert_flat_to_structured.py en-registry.json tests/resources/mappings/registry.json
```

The conversion is lossless: each `A/B` field becomes `A: {properties: {B: ...}}` with its
type unchanged, and mapping-level options (`dynamic`, `dynamic_templates`) are copied as-is.
The script fails if a name would be both a leaf field and an object after conversion.

<!-- TODO: record which deployment and date the en-registry* mappings were exported from. -->

To refresh, prefer exporting `GET <node>-registry-structured*/_mapping` from a structured
deployment directly (it can be dropped in as-is); otherwise export the flat mappings and
re-run the converter.

## Accepted formats

The loader (`tests/support/indices.py`) accepts and normalises any of:

- A `GET <index>/_mapping` response: `{"<index>": {"mappings": {...}}}`
- A **full create-index body**: `{"settings": {...}, "mappings": {"properties": {...}}}`
- A **bare mappings block**: `{"mappings": {"properties": {...}}}`
- A **bare properties block**: `{"properties": {...}}`

## Effect of missing files

- **Locally** (without `CI=1`): DB tests are **skipped** with a message naming the missing files.
- **In CI** (`CI=1`): DB tests **fail** with the same message.
