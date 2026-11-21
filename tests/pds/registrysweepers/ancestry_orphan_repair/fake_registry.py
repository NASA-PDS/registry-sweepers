"""
In-memory stand-in for the OpenSearch client, sufficient for the registry and registry-refs queries made by the
ancestry sweeper and the orphan repair.  Nothing here can reach a real endpoint.

It evaluates the query clauses actually sent (bool / terms / term / exists / range), honours size, sort and
search_after, rejects pages above the 10,000 cap, and applies bulk updates by interpreting the ancestry deduplication
script (after checking that the script sent is the ancestry sweeper's own), so repairs change the data the next query
sees.

It also models the deferral index ("<registry index>-deferred-updates", keyed by product LIDVID): scripted upserts into
it, mget, match_all search sorted by _id, and plain "doc" merges onto registry documents, which is everything the
ancestry sweeper's deferral and merge steps use.  Document deletion (opensearchpy.helpers.bulk) is applied through
FakeRegistry.delete_deferred, which tests patch into ancestry.main.
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


DEFERRED_SUFFIX = "-deferred-updates"


class FakeIndices:
    def __init__(self, registry: "FakeRegistry"):
        self._registry = registry
        self.put_mapping_calls: List[Dict] = []
        self.refresh_calls: List[str] = []

    def exists(self, name):
        if name.endswith(DEFERRED_SUFFIX):
            return self._registry.has_deferred_index
        return True

    def refresh(self, index):
        self.refresh_calls.append(index)

    def exists_alias(self, name):
        return False

    def get_mapping(self, index):
        # structured indices map a dotted field name as nested object properties (ops:Registry_Sweepers -> ops:...)
        properties: Dict[str, Any] = {}
        for key in (REFS, STAMP):
            node = properties
            *objects, leaf = key.split(".")
            for name in objects:
                node = node.setdefault(name, {"properties": {}})["properties"]
            node[leaf] = {"type": "keyword"}
        return {index: {"mappings": {"properties": properties}}}

    def put_mapping(self, index, body):
        self.put_mapping_calls.append({"index": index, "body": body})


class FakeRegistry:
    def __init__(self):
        self.registry: Dict[str, Dict] = {}  # keyed by lidvid (the document id)
        self.refs: List[Dict] = []  # registry-refs _source dicts
        self.deferred: Dict[str, Dict] = {}  # deferral index _source dicts, keyed by product lidvid
        self.has_deferred_index = True
        self.indices = FakeIndices(self)
        self.search_calls: List[Dict] = []
        self.mget_calls: List[Dict] = []
        self.bulk_calls: List[Dict] = []
        self.written_ids: List[str] = []  # ids written to the registry index
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

    def add_deferral(self, product_lidvid: str, ancestor_refs: Iterable[str], stamp: int = SWEEPERS_ANCESTRY_VERSION):
        """A deferral entry as the ancestry sweeper leaves it (the script's output on an empty upsert document)."""
        self.deferred[product_lidvid] = {REFS: list(ancestor_refs), STAMP: stamp}

    def delete_deferred(self, client, index, doc_ids, chunk_size=1000) -> None:
        """Stand-in for utils.db.bulk_delete_documents."""
        assert index.endswith(DEFERRED_SUFFIX)
        for doc_id in doc_ids:
            self.deferred.pop(doc_id, None)

    # ---- client surface ------------------------------------------------------------------------------------------
    def _docs(self, index: str):
        if index.endswith(DEFERRED_SUFFIX):
            return sorted(self.deferred.items())
        if index.endswith("-refs"):
            return [
                (f'{d["collection_lidvid"]}#{d["batch_id"]}', d)
                for d in sorted(self.refs, key=lambda d: (d["collection_lidvid"], d["batch_id"]))
            ]
        return sorted(self.registry.items())

    def mget(self, index: str, body: Dict, **kwargs) -> Dict:
        assert index.endswith(DEFERRED_SUFFIX), "only the deferral index is read by id"
        self.mget_calls.append({"index": index, "ids": list(body["ids"])})
        docs = []
        for doc_id in body["ids"]:
            if doc_id in self.deferred:
                docs.append({"_id": doc_id, "_index": index, "found": True, "_source": dict(self.deferred[doc_id])})
            else:
                docs.append({"_id": doc_id, "_index": index, "found": False})
        return {"docs": docs}

    def search(self, index: str, body: Dict, size: int = 10, sort: Optional[List[str]] = None, **kwargs) -> Dict:
        assert size <= 10_000, f"page size {size} exceeds the 10,000 cap"
        assert "from" not in body and "from_" not in kwargs, "from/size paging must not be used"
        self.search_calls.append({"index": index, "body": body, "size": size, "sort": sort})

        query = body.get("query", {"match_all": {}})
        hits = [(doc_id, src) for doc_id, src in self._docs(index) if matches(src, query)]
        total = len(hits)

        def sort_values(doc_id, src):
            return [doc_id if f == "_id" else src.get(f) for f in sort]

        if sort:
            hits = sorted(hits, key=lambda h: sort_values(*h))
            if "search_after" in body:
                after = list(body["search_after"])
                hits = [h for h in hits if sort_values(*h) > after]

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
                        "sort": sort_values(doc_id, src) if sort else [],
                    }
                    for doc_id, src in page
                ],
            }
        }

    @staticmethod
    def _apply_ancestry_script(source: Dict, script: Dict) -> bool:
        """Interpretation of ANCESTRY_DEDUPLICATION_SCRIPT_MINIFIED.  Returns whether the document changed."""
        assert script["source"] == ANCESTRY_DEDUPLICATION_SCRIPT_MINIFIED
        assert script["lang"] == "painless"
        changed = False
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
        return changed

    def bulk(self, index: str, body: str, request_timeout: Optional[int] = None) -> Dict:
        self.bulk_calls.append({"index": index, "body": body})
        lines = [json.loads(line) for line in body.splitlines() if line.strip()]
        assert len(lines) % 2 == 0

        for doc_id in self.vanish_before_bulk:
            self.registry.pop(doc_id, None)

        deferral_index = index.endswith(DEFERRED_SUFFIX)
        store = self.deferred if deferral_index else self.registry
        items = []
        for action, content in zip(lines[::2], lines[1::2]):
            doc_id = action["update"]["_id"]
            if not deferral_index:
                self.written_ids.append(doc_id)

            if doc_id not in store:
                if deferral_index:
                    # the ancestry sweeper only ever writes the deferral index as a scripted upsert onto {}
                    assert content.get("scripted_upsert") is True and content.get("upsert") == {}
                    store[doc_id] = {}
                else:
                    items.append(
                        {
                            "update": {
                                "_id": doc_id,
                                "status": 404,
                                "error": {
                                    "type": "document_missing_exception",
                                    "reason": "[_doc][x]: document missing",
                                },
                            }
                        }
                    )
                    continue

            source = store[doc_id]
            if "script" in content:
                changed = self._apply_ancestry_script(source, content["script"])
            else:
                # a plain partial-document merge (the deferral merge step): fields are replaced, arrays included
                assert content.get("doc_as_upsert") is False
                changed = any(source.get(k) != v for k, v in content["doc"].items())
                source.update(content["doc"])
            items.append({"update": {"_id": doc_id, "status": 200, "result": "updated" if changed else "noop"}})

        return {"errors": any("error" in i["update"] for i in items), "items": items}
