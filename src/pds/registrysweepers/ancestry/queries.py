import logging
from enum import auto
from enum import Enum
from typing import Dict
from typing import Iterable

from opensearchpy import OpenSearch
from pds.registrysweepers.ancestry.runtimeconstants import AncestryRuntimeConstants
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY
from pds.registrysweepers.utils.db import get_query_hits_count
from pds.registrysweepers.utils.db import query_registry_db_with_search_after
from pds.registrysweepers.utils.db.indexing import ensure_index_has_mapping
from pds.registrysweepers.utils.db.multitenancy import resolve_multitenant_index_name
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid

log = logging.getLogger(__name__)


class ProductClass(Enum):
    BUNDLE = (auto(),)
    COLLECTION = (auto(),)
    NON_AGGREGATE = auto()


def product_class_query_factory(cls: ProductClass) -> Dict:
    queries: Dict[ProductClass, Dict] = {
        ProductClass.BUNDLE: {"bool": {"filter": [{"term": {"product_class": "Product_Bundle"}}]}},
        ProductClass.COLLECTION: {"bool": {"filter": [{"term": {"product_class": "Product_Collection"}}]}},
        ProductClass.NON_AGGREGATE: {
            "bool": {"must_not": [{"terms": {"product_class": ["Product_Bundle", "Product_Collection"]}}]}
        },
    }

    return {"query": queries[cls]}


def _is_at_current_ancestry_version() -> Dict:
    """
    Query clause matching docs stamped with exactly the current ancestry version; use under must_not to select
    docs which require processing.

    The version field is mapped as keyword (see ancestry/main.py), so a range clause would compare versions as
    strings ("9" >= "10"), and once SWEEPERS_ANCESTRY_VERSION reached 10 every doc stamped 2-9 would be treated as
    up to date. An exact match avoids that. Docs stamped by a newer sweeper version are treated as pending, so
    they are reprocessed after a downgrade.

    Built at call time rather than import time, so the current version is always read from this module.
    """
    return {"term": {SWEEPERS_ANCESTRY_VERSION_METADATA_KEY: str(SWEEPERS_ANCESTRY_VERSION)}}


def query_for_pending_bundles(client: OpenSearch) -> Iterable[Dict]:
    """Query the registry for all bundle LIDVIDs which require ancestry processing"""
    from pds.registrysweepers.utils.db import query_registry_db_with_search_after

    query = product_class_query_factory(ProductClass.BUNDLE)
    _source = {"includes": ["lidvid", "ref_lid_collection", SWEEPERS_ANCESTRY_VERSION_METADATA_KEY]}
    docs = query_registry_db_with_search_after(client, resolve_multitenant_index_name(client, "registry"), query,
                                               _source)

    return docs


def query_for_pending_collections(client: OpenSearch) -> Iterable[Dict]:
    """Query the registry for all collection LIDVIDs which require ancestry processing"""
    from pds.registrysweepers.utils.db import query_registry_db_with_search_after

    query = product_class_query_factory(ProductClass.COLLECTION)
    query["query"]["bool"].update(
        {"must_not": [_is_at_current_ancestry_version()]}
    )

    _source = {"includes": ["lidvid", SWEEPERS_ANCESTRY_VERSION_METADATA_KEY]}
    docs = query_registry_db_with_search_after(client, resolve_multitenant_index_name(client, "registry"), query,
                                               _source)

    return docs


def query_for_collection_nonaggregate_refs(
        client: OpenSearch, collection_lidvid: PdsLidVid
) -> Iterable[PdsLidVid]:
    # Query the registry-refs index for the contents of the given collection
    from pds.registrysweepers.utils.db import query_registry_db_with_search_after

    query: Dict = {
        "query": {
            "bool": {
                "must_not": [_is_at_current_ancestry_version()],
                "filter": [{"term": {"collection_lidvid": str(collection_lidvid)}}],
            }
        },
        "seq_no_primary_term": True,
    }
    _source = {"includes": ["collection_lidvid", "batch_id", "product_lidvid"]}

    # each document will have many product lidvids, so a smaller page size is warranted here
    docs = query_registry_db_with_search_after(
        client,
        resolve_multitenant_index_name(client, "registry-refs"),
        query,
        _source,
        page_size=AncestryRuntimeConstants.nonaggregate_ancestry_records_query_page_size,
        request_timeout_seconds=30,
        sort_fields=["batch_id"],
    )

    for doc in docs:
        for ref in doc["_source"].get("product_lidvid", []):
            yield PdsLidVid.from_string(ref)


def _orphaned_docs_query() -> Dict:
    return {"query": {"bool": {"must_not": [_is_at_current_ancestry_version()]}}}


def get_orphaned_documents(client: OpenSearch, index_name: str) -> Iterable[Dict]:
    # Query an index for documents without an up-to-date ancestry version reference - this would indicate a product
    # which is orphaned and is getting missed in processing

    _source: Dict = {"includes": []}
    docs = query_registry_db_with_search_after(client, index_name, _orphaned_docs_query(), _source)

    return docs


def get_orphaned_documents_count(client: OpenSearch, index_name: str) -> int:
    # Query an index documents without an up-to-date ancestry version reference - this would indicate a product which is
    # orphaned and is getting missed in processing
    return get_query_hits_count(client, index_name, _orphaned_docs_query())


def get_deferred_update_documents_without_content(client: OpenSearch, index_name: str) -> Iterable[Dict]:
    deferred_updates_index_name = f"{index_name}-deferred-updates"
    query: Dict = {"query": {"match_all": {}}}
    source: Dict = {"includes": []}

    # Sort on _id rather than _doc: _doc values are shard-local Lucene doc ids, which tie across shards and are not
    # stable across segment merges, so paging on them with search_after silently skips documents. Sorting on _id
    # requires the index to have mapping metadata, which a bare, never-written deferred-updates index lacks.
    ensure_index_has_mapping(client, deferred_updates_index_name)
    docs = query_registry_db_with_search_after(client, deferred_updates_index_name, query, source, sort_fields=["_id"])

    return docs
