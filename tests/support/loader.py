import json
import logging
from pathlib import Path
from typing import Iterable, Optional

from opensearchpy.helpers import bulk

from pds.registrysweepers.ancestry.constants import ANCESTRY_REFS_METADATA_KEY
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY
from tests.support.documents import SeedDoc

log = logging.getLogger(__name__)

FIXTURES_DIR = Path(__file__).parent.parent / "resources" / "fixtures"

# Fields added by ensure_index_mapping at sweeper runtime; may be absent from the user-supplied
# mapping files but are still valid when present in seed data.
_RUNTIME_ADDED_FIELDS = frozenset({
    ANCESTRY_REFS_METADATA_KEY,
    SWEEPERS_ANCESTRY_VERSION_METADATA_KEY,
})

_KNOWN_DOC_KEYS = frozenset({"index", "_id", "_source"})


def _to_seed_doc(doc, *, index: Optional[str]) -> SeedDoc:
    if isinstance(doc, SeedDoc):
        return doc
    if not isinstance(doc, dict):
        raise TypeError(f"Expected SeedDoc or dict, got {type(doc)}")

    unknown = set(doc.keys()) - _KNOWN_DOC_KEYS
    if unknown:
        raise ValueError(f"Unknown top-level keys in doc dict: {sorted(unknown)}")

    doc_index = doc.get("index") or index
    if doc_index is None:
        raise ValueError(
            'dict doc is missing "index" key and no index= argument was provided'
        )
    if "_id" not in doc:
        raise ValueError('doc dict is missing required "_id" key')
    if "_source" not in doc:
        raise ValueError('doc dict is missing required "_source" key')

    return SeedDoc(index=doc_index, id=doc["_id"], source=doc["_source"])


def load(db, docs: Iterable, *, index: Optional[str] = None, validate: bool = True) -> None:
    seed_docs = [_to_seed_doc(d, index=index) for d in docs]
    if not seed_docs:
        return

    if validate:
        from tests.support.indices import load_mappings
        mappings = load_mappings()
        _validate_docs(seed_docs, mappings)

    actions = [
        {
            "_index": db.index_names[sd.index],
            "_id": sd.id,
            "_source": sd.source,
        }
        for sd in seed_docs
    ]

    successes, errors = bulk(db.client, actions, raise_on_error=False, stats_only=False)
    if errors:
        failing = [
            f"{e['index']['_id']}: {e['index'].get('error', {}).get('reason', 'unknown')}"
            for e in errors
            if "index" in e
        ]
        raise RuntimeError(f"Bulk index errors:\n" + "\n".join(failing))

    affected_indices = {db.index_names[sd.index] for sd in seed_docs}
    for idx in affected_indices:
        db.client.indices.refresh(index=idx)


def load_json(db, path) -> None:
    full_path = FIXTURES_DIR / path if not Path(path).is_absolute() else Path(path)
    docs = json.loads(full_path.read_text())
    if not isinstance(docs, list):
        raise ValueError(f"Fixture file must contain a JSON array, got {type(docs).__name__}")
    load(db, docs)


def _validate_docs(seed_docs: list, mappings: dict) -> None:
    errors = {}
    for sd in seed_docs:
        mapping_props = _get_properties(mappings.get(sd.index, {}))
        valid_fields = _extract_mapping_fields(mapping_props) | _RUNTIME_ADDED_FIELDS
        if not valid_fields:
            continue
        doc_fields = set(sd.source.keys())
        unknown = doc_fields - valid_fields
        if unknown:
            errors[sd.id] = sorted(unknown)

    if errors:
        lines = [f"  {doc_id}: {fields}" for doc_id, fields in errors.items()]
        raise ValueError(
            "Unknown fields in seed docs (builders or fixtures may be using stale field names):\n"
            + "\n".join(lines)
        )


def _get_properties(mapping_body: dict) -> dict:
    if "mappings" in mapping_body:
        return mapping_body["mappings"].get("properties", {})
    if "properties" in mapping_body:
        return mapping_body["properties"]
    return {}


def _extract_mapping_fields(properties: dict, prefix: str = "") -> set:
    fields = set()
    for name, defn in properties.items():
        full = f"{prefix}.{name}" if prefix else name
        fields.add(full)
        if isinstance(defn, dict) and "properties" in defn:
            fields.update(_extract_mapping_fields(defn["properties"], full))
    return fields
