import copy
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from uuid import uuid4

import pytest
from opensearchpy import OpenSearch

log = logging.getLogger(__name__)

MAPPINGS_DIR = Path(__file__).parent.parent / "resources" / "mappings"
MAPPING_FILES = {
    "registry": "registry.json",
    "registry-refs": "registry-refs.json",
    "registry-dd": "registry-dd.json",
}

_mappings_cache: Optional[Dict] = None


def load_mappings() -> Dict[str, dict]:
    global _mappings_cache
    if _mappings_cache is not None:
        return _mappings_cache

    missing = [
        str(MAPPINGS_DIR / fname)
        for name, fname in MAPPING_FILES.items()
        if not (MAPPINGS_DIR / fname).exists()
    ]
    if missing:
        raise FileNotFoundError(
            f"Missing mapping file(s): {missing}. "
            f"See {MAPPINGS_DIR / 'README.md'} for instructions."
        )

    result = {}
    for index_type, fname in MAPPING_FILES.items():
        body = json.loads((MAPPINGS_DIR / fname).read_text())
        result[index_type] = _normalise_mapping(body)

    _mappings_cache = result
    return result


_KNOWN_TOP_LEVEL_KEYS = frozenset({"mappings", "properties", "settings", "aliases"})


def _normalise_mapping(body: dict) -> dict:
    """Normalise to {"settings": ..., "mappings": {"properties": {...}}} form."""
    # Handle the OpenSearch get-mapping API response: {"<index-name>": {"mappings": {...}}}
    if len(body) == 1:
        key = next(iter(body))
        if key not in _KNOWN_TOP_LEVEL_KEYS:
            body = body[key]

    if "mappings" in body:
        return body
    if "properties" in body:
        return {"mappings": {"properties": body["properties"]}}
    # Bare properties without a wrapper key — treat the whole dict as properties
    if "settings" not in body and "aliases" not in body:
        return {"mappings": {"properties": body}}
    return body


def _merge_replica_setting(mapping_body: dict) -> dict:
    """Add number_of_replicas=0 unless the supplied mapping already specifies it."""
    body = copy.deepcopy(mapping_body)
    settings = body.setdefault("settings", {})
    # number_of_replicas may be nested under settings.index or at the top level
    index_settings = settings.get("index", settings)
    if "number_of_replicas" not in index_settings:
        settings["number_of_replicas"] = 0  # single-node test cluster
    return body


@dataclass
class RegistryDb:
    client: OpenSearch
    node_id: str
    index_names: Dict[str, str]

    def load(self, docs: Iterable, *, index: Optional[str] = None, validate: bool = True) -> None:
        from tests.support.loader import load
        load(self, docs, index=index, validate=validate)

    def load_json(self, path) -> None:
        from tests.support.loader import load_json
        load_json(self, path)

    def refresh(self) -> None:
        self.client.indices.refresh(index=f"{self.node_id}-*")

    def dump(self, index: str, *, fields: Optional[List[str]] = None) -> list:
        from tests.support.snapshots import dump
        return dump(self, index, fields=fields)


@pytest.fixture
def registry_db(opensearch_url, opensearch_client, monkeypatch):
    node_id = "t" + uuid4().hex[:10]

    monkeypatch.setenv("MULTITENANCY_NODE_ID", node_id)
    monkeypatch.setenv("PROV_ENDPOINT", opensearch_url)
    monkeypatch.setenv("PROV_CREDENTIALS", '{"admin":"admin"}')
    monkeypatch.setenv("DEV_MODE", "1")

    try:
        mappings = load_mappings()
    except FileNotFoundError as exc:
        if os.environ.get("CI"):
            pytest.fail(str(exc))
        else:
            pytest.skip(str(exc))

    index_names = {
        "registry": f"{node_id}-registry-structured",
        "registry-refs": f"{node_id}-registry-structured-refs",
        "registry-dd": f"{node_id}-registry-structured-dd",
        "deferred-updates": f"{node_id}-registry-structured-deferred-updates",
    }

    for logical, physical in index_names.items():
        if logical == "deferred-updates":
            opensearch_client.indices.create(
                index=physical,
                body={"settings": {"number_of_replicas": 0}},
            )
        else:
            body = _merge_replica_setting(mappings[logical])
            opensearch_client.indices.create(index=physical, body=body)

    db = RegistryDb(
        client=opensearch_client,
        node_id=node_id,
        index_names=index_names,
    )

    yield db

    opensearch_client.indices.delete(index=f"{node_id}-*", ignore_unavailable=True)
