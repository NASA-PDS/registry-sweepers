"""
In-memory stand-in for the OpenSearch client, sufficient for the registry and registry-refs queries made by the
ancestry sweeper and the orphan repair.  Nothing here can reach a real endpoint.

It evaluates the query clauses actually sent (bool / terms / term / exists / range), honours size, sort and
search_after, rejects pages above the 10,000 cap, and applies bulk updates by interpreting the ancestry deduplication
script (after checking that the script sent is the ancestry sweeper's own), so repairs change the data the next query
sees.
"""
import json
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Set

from pds.registrysweepers.ancestry.constants import ANCESTRY_DEDUPLICATION_SCRIPT_MINIFIED
from pds.registrysweepers.ancestry.constants import ANCESTRY_REFS_METADATA_KEY
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY
from pds.registrysweepers.provenance.constants import METADATA_SUCCESSOR_KEY

REFS = ANCESTRY_REFS_METADATA_KEY
STAMP = SWEEPERS_ANCESTRY_VERSION_METADATA_KEY


def _values(source: Dict, field: str) -> List[Any]:
    value = source.get(field)
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def matches(source: Dict, query: Dict) -> bool:
    ((kind, spec),) = query.items()

    if kind == "match_all":
        return True
    if kind == "bool":
        for clause in spec.get("filter", []) + spec.get("must", []):
            if not matches(source, clause):
                return False
        for clause in spec.get("must_not", []):
            if matches(source, clause):
                return False
        should = spec.get("should", [])
        if should:
            if sum(matches(source, clause) for clause in should) < spec.get("minimum_should_match", 1):
                return False
        return True
    if kind in ("term", "terms"):
        ((field, wanted),) = spec.items()
        wanted = wanted if kind == "terms" else [wanted]
        return any(v in wanted for v in _values(source, field))
    if kind == "exists":
        return len(_values(source, spec["field"])) > 0
    if kind == "range":
        ((field, bounds),) = spec.items()
        values = _values(source, field)
        if not values:
            return False
        return all(
            (op != "gte" or v >= bound) and (op != "lt" or v < bound) and (op != "gt" or v > bound)
            for v in values
            for op, bound in bounds.items()
        )
    raise NotImplementedError(f"fake registry does not implement query clause {kind}")


class FakeIndices:
    def __init__(self):
        self.put_mapping_calls: List[Dict] = []

    def exists(self, name):
        return True

    def exists_alias(self, name):
        return False

    def get_mapping(self, index):
        return {index: {"mappings": {"properties": {REFS: {"type": "keyword"}, STAMP: {"type": "keyword"}}}}}

    def put_mapping(self, index, body):
        self.put_mapping_calls.append({"index": index, "body": body})


class FakeRegistry:
    def __init__(self):
        self.registry: Dict[str, Dict] = {}  # keyed by lidvid (the document id)
        self.refs: List[Dict] = []  # registry-refs _source dicts
        self.indices = FakeIndices()
        self.search_calls: List[Dict] = []
        self.bulk_calls: List[Dict] = []
        self.written_ids: List[str] = []
        self.vanish_before_bulk: Set[str] = set()

    # ---- data builders -------------------------------------------------------------------------------------------
    def add_product(
        self,
        lidvid: str,
        product_class: str = "Product_Observational",
        stamp: Optional[int] = None,
        ancestor_refs: Optional[List[str]] = None,
        superseded_by: Optional[str] = None,
    ) -> None:
        source: Dict[str, Any] = {"lidvid": lidvid, "product_class": product_class}
        if stamp is not None:
            source[STAMP] = stamp
        if ancestor_refs is not None:
            source[REFS] = list(ancestor_refs)
        if superseded_by is not None:
            source[METADATA_SUCCESSOR_KEY] = superseded_by
        self.registry[lidvid] = source

    def add_collection(self, lidvid: str, stamp: Optional[int] = SWEEPERS_ANCESTRY_VERSION) -> None:
        self.add_product(lidvid, "Product_Collection", stamp=stamp, ancestor_refs=[])

    def add_refs(self, collection_lidvid: str, product_lidvids: Iterable[str], batch_id: Optional[int] = None) -> None:
        batch_id = len(self.refs) if batch_id is None else batch_id
        self.refs.append(
            {
                "collection_lidvid": collection_lidvid,
                "batch_id": batch_id,
                "product_lidvid": list(product_lidvids),
            }
        )

    # ---- client surface ------------------------------------------------------------------------------------------
    def _docs(self, index: str):
        if index.endswith("registry-refs"):
            return [
                (f'{d["collection_lidvid"]}#{d["batch_id"]}', d)
                for d in sorted(self.refs, key=lambda d: (d["collection_lidvid"], d["batch_id"]))
            ]
        return sorted(self.registry.items())

    def search(self, index: str, body: Dict, size: int = 10, sort: Optional[List[str]] = None, **kwargs) -> Dict:
        assert size <= 10_000, f"page size {size} exceeds the 10,000 cap"
        assert "from" not in body and "from_" not in kwargs, "from/size paging must not be used"
        self.search_calls.append({"index": index, "body": body, "size": size, "sort": sort})

        query = body.get("query", {"match_all": {}})
        hits = [(doc_id, src) for doc_id, src in self._docs(index) if matches(src, query)]
        total = len(hits)

        if sort:
            hits = sorted(hits, key=lambda h: [h[1].get(f) for f in sort])
            if "search_after" in body:
                after = list(body["search_after"])
                hits = [h for h in hits if [h[1].get(f) for f in sort] > after]

        page = hits[:size]
        includes = kwargs.get("_source_includes") or None
        return {
            "hits": {
                "total": {"value": total, "relation": "eq"},
                "hits": [
                    {
                        "_id": doc_id,
                        "_index": index,
                        "_source": {k: v for k, v in src.items() if includes is None or k in includes},
                        "sort": [src.get(f) for f in sort] if sort else [],
                    }
                    for doc_id, src in page
                ],
            }
        }

    def bulk(self, index: str, body: str, request_timeout: Optional[int] = None) -> Dict:
        self.bulk_calls.append({"index": index, "body": body})
        lines = [json.loads(line) for line in body.splitlines() if line.strip()]
        assert len(lines) % 2 == 0

        for doc_id in self.vanish_before_bulk:
            self.registry.pop(doc_id, None)

        items = []
        for action, content in zip(lines[::2], lines[1::2]):
            doc_id = action["update"]["_id"]
            self.written_ids.append(doc_id)
            script = content["script"]
            assert script["source"] == ANCESTRY_DEDUPLICATION_SCRIPT_MINIFIED
            assert script["lang"] == "painless"

            if doc_id not in self.registry:
                items.append(
                    {
                        "update": {
                            "_id": doc_id,
                            "status": 404,
                            "error": {"type": "document_missing_exception", "reason": "[_doc][x]: document missing"},
                        }
                    }
                )
                continue

            source = self.registry[doc_id]
            changed = False
            # interpretation of ANCESTRY_DEDUPLICATION_SCRIPT_MINIFIED
            if source.get(REFS) is None:
                source[REFS] = []
                changed = True
            for item in script["params"]["new_items"]:
                if item not in source[REFS]:
                    source[REFS].append(item)
                    changed = True
            if source.get(STAMP) != SWEEPERS_ANCESTRY_VERSION:
                source[STAMP] = SWEEPERS_ANCESTRY_VERSION
                changed = True
            items.append({"update": {"_id": doc_id, "status": 200, "result": "updated" if changed else "noop"}})

        return {"errors": any("error" in i["update"] for i in items), "items": items}
