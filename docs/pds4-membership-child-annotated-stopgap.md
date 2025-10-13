# PDS4 Registry – Child-Annotated Membership (Stopgap)
**Using `introduced_by_parent_vid*` and `superseded_by_parent*` fields on child docs**

> This design augments _child product documents_ with per-parent, version-aware membership metadata. It enables queries like “members of `collection::K` with arbitrary filters/sorts” **without creating a separate edge/view index**. It’s a pragmatic stopgap that’s easy to bolt onto the current model, with the tradeoff that new releases may require **reindexing thousands of child docs**.

---

## Goals

- Keep using the existing `products_full_metadata` index as the _single_ place where all searchable fields live.
- Allow **filter & sort by any attribute** (hundreds of PDS fields) while scoping results to a parent **LIDVID**.
- Minimize new infrastructure (no additional join/query fan‑out), accepting that accumulating collections may trigger **fan‑out updates** to many child docs.

---

## High-Level Approach

Add a **per-parent membership timeline** to each child document. Each timeline records:
- **which parent** (bundle/collection LID) the child belongs to, and
- the **version interval** in which the membership is valid (introduced in version X.Y, optionally superseded/removed in version Z.W).

A query for “members of `parent::K`” becomes: search only child docs where there exists a per-parent timeline entry for that parent LID such that:
```
introduced_version <= K  AND  (superseded_version is null OR superseded_version > K)
```

All other user predicates and sorts run natively on the child document (since the child holds the entire metadata).

---

## Suggested Field Model

Two implementation patterns are viable. **Pick one**.

### Option A — Major/Minor fields (closest to current proposal)
- `memberships` (**nested**) array with objects:
  - `parent_lid` (keyword) — LID of the parent bundle/collection
  - `introduced_major` (integer)
  - `introduced_minor` (integer)
  - `superseded_major` (integer, null when active)
  - `superseded_minor` (integer, null when active)
  - `parent_type` (keyword: `collection` | `bundle`) _(optional)_

> **Why nested?** Multiple parents per child require independent timelines per parent. `nested` ensures per-entry Boolean logic is evaluated correctly.

**Pros:** mirrors PDS VID structure; easy to write.
**Cons:** Range logic must consider two integers; queries get verbose.

### Option B — Encoded version number (simpler queries)
Add a **single** sortable numeric for each boundary by encoding a VID (major.minor) to an integer:
```
encoded = major * 1_000_000 + minor
```
- `memberships` (**nested**) objects:
  - `parent_lid` (keyword)
  - `introduced_enc` (long)
  - `superseded_enc` (long, null when active)
  - `parent_type` (keyword, optional)

**Pros:** clean range comparisons, simpler sort/filter.
**Cons:** Requires encoding/decoding VIDs in ETL and client code.

> The examples below use **Option B (encoded)** for clarity; swap in major/minor ranges if you prefer Option A.

---

## Example Mapping Snippet

```json
{
  "mappings": {
    "properties": {
      "lidvid":        { "type": "keyword" },
      "lid":           { "type": "keyword" },
      "title":         { "type": "text", "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } } },
      "start_date":    { "type": "date" },
      "stop_date":     { "type": "date" },
      "product_class": { "type": "keyword" },

      "memberships": {
        "type": "nested",
        "properties": {
          "parent_lid":    { "type": "keyword" },
          "parent_type":   { "type": "keyword" },
          "introduced_enc":{ "type": "long"    },
          "superseded_enc":{ "type": "long", "null_value": 9223372036854775807 }  // treat null as +∞
        }
      }
    }
  }
}
```

> Setting `null_value` for `superseded_enc` to a very large number (e.g., `Long.MAX_VALUE`) lets you avoid `exists` checks and use a single range comparison.

---

## Query Examples

### A) Members of `urn:nasa:pds:my_col::2.0` (arbitrary filters, arbitrary sort)

Let `K = encode(major=2, minor=0) = 2_000_000`.

```json
POST products_full_metadata/_search
{
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
          "nested": {
            "path": "memberships",
            "query": {
              "bool": {
                "filter": [
                  { "term": { "memberships.parent_lid": "urn:nasa:pds:my_col" } },
                  { "range": { "memberships.introduced_enc": { "lte": 2000000 } } },
                  { "range": { "memberships.superseded_enc": { "gt": 2000000 } } }
                ]
              }
            }
          }
        }
      ]
    }
  }
}
```

### B) Members of the **bundle** as of `bundle::5.1`, sorted by `target.keyword` then `start_date desc`

Let `K = encode(5,1) = 5_000_001`.

```json
"sort": [
  { "target.keyword": "asc" },
  { "start_date": "desc" },
  { "_id": "asc" }
],
"query": {
  "bool": {
    "must": [
      {
        "nested": {
          "path": "memberships",
          "query": {
            "bool": {
              "filter": [
                { "term":  { "memberships.parent_lid": "urn:nasa:pds:my_bundle" } },
                { "term":  { "memberships.parent_type": "bundle" } },
                { "range": { "memberships.introduced_enc": { "lte": 5000001 } } },
                { "range": { "memberships.superseded_enc": { "gt": 5000001 } } }
              ]
            }
          }
        }
      }
    ]
  }
}
```

> Because the sort runs on the child doc, you can sort/filter by **any** attribute present in the product mapping, while the `nested` clause constrains membership as of version K.

---

## Write-Time / Sweeper Logic

When a new **parent version** (bundle/collection) is released:

1. **Load inventories/labels** for `parent::n` and previous `parent::n-1`.
2. **Diff:** compute `A = added`, `R = removed` sets of **child LIDVIDs**.
3. **For each `a ∈ A` (added/introduced):**
   - Update the child doc (bulk update) to **add** a `memberships` entry for this `parent_lid` with:
     - `introduced_enc = encode(n_major, n_minor)`
     - `superseded_enc = null` (or the +∞ sentinel)
     - `parent_type` as appropriate
4. **For each `r ∈ R` (removed/superseded):**
   - Update the child doc’s `memberships` entry for this `parent_lid` by setting:
     - `superseded_enc = encode(n_major, n_minor)`
5. **Re‑adds** (removed earlier then reintroduced): append a **new** membership entry with a later `introduced_enc` (do not overwrite history).
6. **Multiple parents:** simply maintain multiple `memberships` entries (one per parent).

**Notes**
- Use **bulk updates** (5–20k ops/batch) with retry/backoff.
- Prefer a deterministic `scripted_upsert` to append/patch the correct `memberships` element for the given `parent_lid`.
- Keep your Δ computation as a streaming merge-diff over sorted inventories to avoid memory spikes.

---

## Operational Considerations

- **Indexing cost:** Accumulating collections can touch **thousands to millions** of child docs over time. The sweeper’s runtime grows with Δ size (adds+removes).
- **Nested query cost:** `nested` fields introduce additional query overhead; keep the `memberships` object **minimal** (just parent id & version bounds).
- **Pagination:** use PIT + `search_after` for stable paging on any user-provided sort.
- **Mapping stability:** avoid changing field types in `memberships`; treat it as a stable contract.
- **Backfill:** You can backfill membership timelines by replaying inventories/labels in chronological order and writing `introduced_enc` then `superseded_enc` values accordingly.

---

## Advantages (Why this Stopgap)

- ✅ **Minimal infra change** — no new indices required for reads; everything runs in `products_full_metadata`.
- ✅ **Arbitrary filters & sorts** — all metadata lives together with the child doc.
- ✅ **Version-correct membership** — precise as-of semantics via version bounds.
- ✅ **Simple to reason about** — the child “knows” its parent timelines.

---

## Drawbacks / Risks

- ⚠️ **Write amplification:** Each accumulating release may update **many** child docs (fan‑out).
- ⚠️ **Nested query overhead:** `nested` adds CPU/memory cost and can impact latency at scale.
- ⚠️ **Multi-parent complexity:** Children that belong to many parents carry larger `memberships` arrays.
- ⚠️ **Re-adds/history:** Requires appending multiple timeline fragments per parent for accurate history.
- ⚠️ **Rollback/edits:** Correcting past inventories requires fixing child timelines accordingly.

> If these become problematic, graduate to an **edge index** or **materialized views** later without changing the external API semantics.

---

## Encoders / Helpers

```python
def encode_vid(major: int, minor: int) -> int:
    return major * 1_000_000 + minor

def decode_vid(encoded: int) -> tuple[int, int]:
    return encoded // 1_000_000, encoded % 1_000_000
```

---

## Migration Plan

1. **Add mapping** for `memberships` (nested) to `products_full_metadata`.
2. **Backfill** by replaying existing inventories to set `introduced_enc`/`superseded_enc`.
3. **Switch** API `/members` to add the `nested` constraint shown above (others unchanged).
4. **Operate** the Δ-sweeper each time a new parent version appears.
5. **Monitor** indexing throughput and query latency; if hotspots emerge, consider transitioning to the **edge/indexed-terms** or **view** strategies.

---

## TL;DR

This stopgap makes membership **version-aware** by **annotating child docs** with per-parent timelines. It preserves “filter & sort by any attribute” and is easy to adopt, at the cost of **reindexing** a potentially large set of child docs for each accumulating release and paying the **nested** query tax at read time.
