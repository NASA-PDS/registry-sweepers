import logging
from enum import auto
from enum import Enum
from typing import Dict
from typing import Iterable

from opensearchpy import OpenSearch
from pds.registrysweepers.ancestry.constants import ANCESTRY_REFS_METADATA_KEY
from pds.registrysweepers.ancestry.runtimeconstants import AncestryRuntimeConstants
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY
from pds.registrysweepers.utils.db import get_query_hits_count
from pds.registrysweepers.utils.db.multitenancy import resolve_multitenant_index_name
from pds.registrysweepers.utils.productidentifiers.pdslid import PdsLid
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


def query_for_pending_bundles(client: OpenSearch) -> Iterable[Dict]:
    """Query the registry for all bundle LIDVIDs which require ancestry processing"""
    from pds.registrysweepers.utils.db import query_registry_db_with_search_after

    query = product_class_query_factory(ProductClass.BUNDLE)
    _source = {"includes": ["lidvid", "ref_lid_collection", SWEEPERS_ANCESTRY_VERSION_METADATA_KEY]}
    docs = query_registry_db_with_search_after(client, resolve_multitenant_index_name(client, "registry"), query, _source)

    return docs


def query_for_pending_collections(client: OpenSearch) -> Iterable[Dict]:
    """Query the registry for all collection LIDVIDs which require ancestry processing"""
    from pds.registrysweepers.utils.db import query_registry_db_with_search_after

    query = product_class_query_factory(ProductClass.COLLECTION)
    query["query"]["bool"].update(
        {"must_not": [{"range": {SWEEPERS_ANCESTRY_VERSION_METADATA_KEY: {"gte": SWEEPERS_ANCESTRY_VERSION}}}]}
    )

    _source = {"includes": ["lidvid", SWEEPERS_ANCESTRY_VERSION_METADATA_KEY]}
    docs = query_registry_db_with_search_after(client, resolve_multitenant_index_name(client, "registry"), query, _source)

    return docs


def get_nonaggregate_ancestry_records_query(client: OpenSearch) -> Iterable[Dict]:
    # Query the registry-refs index for the contents of all collections
    from pds.registrysweepers.utils.db import query_registry_db_with_search_after

    query: Dict = {
        "query": {
            "bool": {
                "must_not": [{"range": {SWEEPERS_ANCESTRY_VERSION_METADATA_KEY: {"gte": SWEEPERS_ANCESTRY_VERSION}}}]
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
        sort_fields=["collection_lidvid", "batch_id"],
    )

    return docs


def query_for_collection_nonaggregate_refs(
    client: OpenSearch, collection_lidvid: PdsLidVid
) -> Iterable[PdsLidVid]:
    # Query the registry-refs index for the contents of the given collection
    from pds.registrysweepers.utils.db import query_registry_db_with_search_after

    query: Dict = {
        "query": {
            "bool": {
                "must_not": [{"range": {SWEEPERS_ANCESTRY_VERSION_METADATA_KEY: {"gte": SWEEPERS_ANCESTRY_VERSION}}}],
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


_orphaned_docs_query = {
    "query": {
        "bool": {"must_not": [{"range": {SWEEPERS_ANCESTRY_VERSION_METADATA_KEY: {"gte": SWEEPERS_ANCESTRY_VERSION}}}]}
    }
}


def get_orphaned_documents(client: OpenSearch, index_name: str) -> Iterable[Dict]:
    # Query an index for documents without an up-to-date ancestry version reference - this would indicate a product
    # which is orphaned and is getting missed in processing
    from pds.registrysweepers.utils.db import query_registry_db_with_search_after

    _source: Dict = {"includes": []}

    sort_fields_override = (
        ["collection_lidvid", "batch_id"] if "registry-refs" in index_name else None
    )  # use default for registry

    docs = query_registry_db_with_search_after(client, index_name, _orphaned_docs_query, _source, sort_fields=sort_fields_override)

    return docs


def get_orphaned_documents_count(client: OpenSearch, index_name: str) -> int:
    # Query an index documents without an up-to-date ancestry version reference - this would indicate a product which is
    # orphaned and is getting missed in processing
    return get_query_hits_count(client, index_name, _orphaned_docs_query)
