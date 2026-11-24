from typing import List
from typing import Optional

from pds.registrysweepers.ancestry.constants import ANCESTRY_REFS_METADATA_KEY
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY


# Fields that are volatile across runs and should be dropped during normalisation.
# Extend this list only when a real snapshot test proves a field is non-deterministic.
_VOLATILE_FIELDS: frozenset = frozenset()

# List-valued fields whose element order is not meaningful and not deterministic, so they
# are sorted before comparison. Extend only when a real snapshot proves the order varies.
# - ANCESTRY_REFS_METADATA_KEY: the sweeper builds it from a set (ancestryrecord.py) and
#   appends to it in a Painless script, so the order varies from run to run.
_UNORDERED_LIST_FIELDS: frozenset = frozenset({ANCESTRY_REFS_METADATA_KEY})

# Snapshot-only: fields whose value is replaced by a placeholder when it equals the current
# constant, so snapshots don't churn when the constant changes. Any other value (e.g. a stale
# version) is kept verbatim so it still shows up in the diff. Applied by snapshot_matcher at
# serialisation time, NOT by dump(), so targeted assertions still see the real value.
# - SWEEPERS_ANCESTRY_VERSION: bumped only for breaking changes that force reprocessing of
#   every document; targeted assertions check the actual value.
_CURRENT_VALUE_PLACEHOLDERS: dict = {
    SWEEPERS_ANCESTRY_VERSION_METADATA_KEY: (SWEEPERS_ANCESTRY_VERSION, "<SWEEPERS_ANCESTRY_VERSION>"),
}

def dump(db, index: str, *, fields: Optional[List[str]] = None) -> List[dict]:
    """Return all documents in the given logical index, sorted by _id.

    Args:
        db: RegistryDb handle.
        index: Logical index type ("registry", "registry-refs", "registry-dd", "deferred-updates").
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
    return {
        k: sorted(v) if k in _UNORDERED_LIST_FIELDS and isinstance(v, list) else v
        for k, v in source.items()
        if k not in _VOLATILE_FIELDS
    }


def snapshot_matcher(*, data, path):
    """syrupy PropertyMatcher: swap current-constant values for stable placeholders."""
    if path:
        key = path[-1][0]
        if key in _CURRENT_VALUE_PLACEHOLDERS:
            current, placeholder = _CURRENT_VALUE_PLACEHOLDERS[key]
            if data == current:
                return placeholder
    return data
