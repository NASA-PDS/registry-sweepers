# PDS4 Registry / OpenSearch Membership Query Solution

## Goal

Enable users to query and sort by **any PDS metadata attribute** (hundreds of possible fields) while ensuring results are limited to **members of a specific bundle or collection version** — without duplicating metadata or doing expensive joins.

---

## Core Challenge

- Products (data, collections, bundles) live in a single index, `products_full_metadata`.
- Membership (collection/bundle → member) is versioned and potentially huge (millions of members).
- We must allow queries like:

  ```
  /products/collection::2.0/members?q=start_date>2025-01-01&sort=target,desc
  ```

  and guarantee results include **only** the members of that collection version.

---

## Solution Overview: “Terms-Lookup Membership Filter”

### High-Level Idea
Use OpenSearch’s [`terms` lookup feature](https://opensearch.org/docs/latest/opensearch/query-dsl/term/#terms-query) to supply a cached list of member IDs for a given collection/bundle version.

All queries and sorts run directly against the `products_full_metadata` index — the system of record for all metadata.

---

## Indices

### 1. `products_full_metadata`
- One document per product (child).
- `_id = child_lidvid`
- Contains **all** PDS metadata attributes.
- Supports arbitrary filtering and sorting.

### 2. `members_by_parent_version`
- One document per membership edge.
- Fields:
  ```json
  {
    "parent_lidvid": "urn:nasa:pds:my_col::2.0",
    "child_lidvid": "urn:nasa:pds:my_prod::1.0"
  }
  ```
- Authoritative list of relationships.
- Used by sweepers and for analytics.

### 3. `terms_holder`
- Stores membership ID lists (“chunks”) per parent LIDVID.
- Example documents:

  ```json
  {
    "_id": "urn:nasa:pds:my_col::2.0#chunk_000",
    "parent_lidvid": "urn:nasa:pds:my_col::2.0",
    "chunk": 0,
    "ids": ["child::A::1.0", "child::B::1.0", ...]
  }
  ```

- Optional “meta” document:
  ```json
  {
    "_id": "urn:nasa:pds:my_col::2.0#meta",
    "parent_lidvid": "urn:nasa:pds:my_col::2.0",
    "chunks": 37,
    "generated_at": "2025-10-10T00:00:00Z"
  }
  ```

- Typical chunk size: **20k–50k IDs**.

---

## Write-Time Flow

1. **Detect new collection/bundle version** (via sweeper).
2. Compute membership Δ vs previous version.
3. Read all active members for that parent LIDVID from `members_by_parent_version`.
4. Split into chunks (≈25k IDs).
5. Write or update the corresponding `terms_holder` chunk docs and meta doc.

Example pseudocode:

```python
def build_terms_holder(parent_lidvid):
    members = scan(index="members_by_parent_version",
                   query={"term": {"parent_lidvid": parent_lidvid}},
                   _source=["child_lidvid"])
    ids = [m["_source"]["child_lidvid"] for m in members]
    CHUNK = 25000
    for i, chunk in enumerate(chunks(ids, CHUNK)):
        upsert(index="terms_holder",
               id=f"{parent_lidvid}#chunk_{i:03d}",
               doc={"parent_lidvid": parent_lidvid, "chunk": i, "ids": chunk})
    upsert(index="terms_holder",
           id=f"{parent_lidvid}#meta",
           doc={"parent_lidvid": parent_lidvid, "chunks": math.ceil(len(ids)/CHUNK)})
```

---

## Read-Time Flow

### Step 1: Open a PIT
Create a [Point-in-Time (PIT)](https://opensearch.org/docs/latest/search/pit/) to enable stable pagination.

```json
POST /products_full_metadata/_pit?keep_alive=1m
```

### Step 2: Query With Terms-Lookup
Use user’s arbitrary filters/sorts, plus a membership filter that references the `terms_holder` chunks.

```json
POST /products_full_metadata/_search
{
  "pit": { "id": "<PIT_ID>", "keep_alive": "1m" },
  "size": 200,
  "sort": [
    { "start_date": "asc" },
    { "_id": "asc" }
  ],
  "query": {
    "bool": {
      "filter": [
        { "range": { "start_date": { "gte": "2025-01-01" } } }
      ],
      "must": [
        {
          "bool": {
            "should": [
              { "terms": { "_id": { "index": "terms_holder", "id": "urn:nasa:pds:my_col::2.0#chunk_000", "path": "ids" } } },
              { "terms": { "_id": { "index": "terms_holder", "id": "urn:nasa:pds:my_col::2.0#chunk_001", "path": "ids" } } },
              { "terms": { "_id": { "index": "terms_holder", "id": "urn:nasa:pds:my_col::2.0#chunk_002", "path": "ids" } } }
            ],
            "minimum_should_match": 1
          }
        }
      ]
    }
  }
}
```

### Step 3: Pagination
Use `search_after` with the `sort` values from the last hit:

```json
"search_after": ["2025-03-14T00:00:00Z", "urn:nasa:pds:child::ABC::1.0"]
```

Keep PIT alive between pages.

---

## Sorting & Filtering

- **Any filter**: Executed directly in `products_full_metadata`.
- **Any sort**: Also executed in `products_full_metadata`.
  Example:
  ```json
  "sort": [
    { "target.keyword": "asc" },
    { "start_date": "desc" },
    { "_id": "asc" }
  ]
  ```
- Ensure all sort fields have `doc_values: true` (default) and text fields have `.keyword` subfields.

---

## Scaling & Performance Notes

| Item | Recommendation |
|------|----------------|
| Chunk size | 20k–50k IDs per chunk |
| Clauses per query | ≤ 1024 `should` clauses (OpenSearch default limit) |
| Hot parents | Split requests or use optional materialized view |
| Caching | Terms lookups are cached; meta doc guides chunk count |
| Updates | Overwrite chunks on new parent version (Δ rebuild only) |
| Storage | Each parent version adds only a few small docs (KBs total) |

---

## Advantages

✅ **Filter/sort by any metadata field** — executed in one index.
✅ **Accurate membership by parent version** — enforced via lookup filter.
✅ **No joins or full-doc duplication** — only small ID-set docs added.
✅ **Supports stable pagination** using PIT + search_after.
✅ **Efficient updates** — only a few docs written per parent version.

---

## Summary Architecture Diagram

```
                 ┌───────────────────────────────┐
                 │        products_full_metadata │
                 │ (all PDS metadata, all fields)│
                 └────────────┬──────────────────┘
                              ▲
                              │  filter/sort/return full docs
                              │
   terms lookup (membership)  │
                              │
 ┌────────────────────────────┴────────────────────────────┐
 │                 terms_holder                            │
 │  • one doc per chunk (~25k IDs)                         │
 │  • referenced via "terms" lookup                        │
 │  • rebuilt by sweeper on new parent version              │
 └──────────────────────────────────────────────────────────┘
```

---

## Implementation Summary

| Component | Purpose |
|------------|----------|
| `members_by_parent_version` | Source of truth for parent→child edges |
| `terms_holder` | Chunked membership lists used in terms lookup |
| `products_full_metadata` | All metadata for filtering/sorting |
| **Sweeper** | Computes Δ, maintains chunks |
| **API** | Runs one OpenSearch query using PIT + search_after, filters results to membership via terms-lookup |

---

## References

- [OpenSearch Terms Query with Terms Lookup](https://opensearch.org/docs/latest/opensearch/query-dsl/term/#terms-query)
- [Point-in-Time API](https://opensearch.org/docs/latest/search/pit/)
- [Bool Query and Minimum Should Match](https://opensearch.org/docs/latest/opensearch/query-dsl/bool/)
