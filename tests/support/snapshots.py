from typing import List, Optional


# Fields that are volatile across runs and should be dropped during normalisation.
# Extend this list only when a real snapshot test proves a field is non-deterministic.
_VOLATILE_FIELDS: frozenset = frozenset()


def dump(db, index: str, *, fields: Optional[List[str]] = None) -> List[dict]:
    """Return all documents in the given logical index, sorted by _id.

    Args:
        db: RegistryDb handle.
        index: Logical index type ("registry", "registry-refs", "registry-dd").
        fields: If provided, project each document's _source to only these fields.
    """
    physical = db.index_names[index]
    db.client.indices.refresh(index=physical)

    source_filter: dict = {"includes": fields} if fields else {}
    response = db.client.search(
        index=physical,
        body={"query": {"match_all": {}}, "size": 10000, "_source": source_filter or True},
    )

    hits = response["hits"]["hits"]
    total = response["hits"]["total"]["value"]
    assert total < 10000, f"dump() found {total} docs — increase size or use scroll for large indices"

    docs = [{"_id": h["_id"], "_source": _normalise(h["_source"])} for h in hits]
    return sorted(docs, key=lambda d: d["_id"])


def _normalise(source: dict) -> dict:
    return {k: v for k, v in source.items() if k not in _VOLATILE_FIELDS}
