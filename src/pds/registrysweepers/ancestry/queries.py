import logging
from enum import auto
from enum import Enum
from typing import Dict
from typing import Iterable

from opensearchpy import OpenSearch
from pds.registrysweepers.ancestry.constants import METADATA_PARENT_BUNDLE_KEY
from pds.registrysweepers.ancestry.constants import METADATA_PARENT_COLLECTION_KEY
from pds.registrysweepers.ancestry.runtimeconstants import AncestryRuntimeConstants
from pds.registrysweepers.ancestry.typedefs import DbMockTypeDef
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY
from pds.registrysweepers.utils.db import get_query_hits_count
from pds.registrysweepers.utils.db import query_registry_db_or_mock
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


def get_bundle_ancestry_records_query(client: OpenSearch, db_mock: DbMockTypeDef = None) -> Iterable[Dict]:
    query = product_class_query_factory(ProductClass.BUNDLE)
    _source = {"includes": ["lidvid", SWEEPERS_ANCESTRY_VERSION_METADATA_KEY]}
    query_f = query_registry_db_or_mock(db_mock, "get_bundle_ancestry_records", use_search_after=True)
    docs = query_f(client, resolve_multitenant_index_name(client, "registry"), query, _source)

    return docs


def get_collection_ancestry_records_bundles_query(client: OpenSearch, db_mock: DbMockTypeDef = None) -> Iterable[Dict]:
    query = product_class_query_factory(ProductClass.BUNDLE)
    _source = {"includes": ["lidvid", "ref_lid_collection"]}
    query_f = query_registry_db_or_mock(db_mock, "get_collection_ancestry_records_bundles", use_search_after=True)
    docs = query_f(client, resolve_multitenant_index_name(client, "registry"), query, _source)

    return docs


def get_collection_ancestry_records_collections_query(
    client: OpenSearch, db_mock: DbMockTypeDef = None
) -> Iterable[Dict]:
    # Query the registry for all collection identifiers
    query = product_class_query_factory(ProductClass.COLLECTION)
    _source = {"includes": ["lidvid", SWEEPERS_ANCESTRY_VERSION_METADATA_KEY]}
    query_f = query_registry_db_or_mock(db_mock, "get_collection_ancestry_records_collections", use_search_after=True)
    docs = query_f(client, resolve_multitenant_index_name(client, "registry"), query, _source)

    return docs


def get_nonaggregate_ancestry_records_query(client: OpenSearch, registry_db_mock: DbMockTypeDef) -> Iterable[Dict]:
    # Query the registry-refs index for the contents of all collections
    query: Dict = {
        "query": {
            "bool": {
                "must_not": [{"range": {SWEEPERS_ANCESTRY_VERSION_METADATA_KEY: {"gte": SWEEPERS_ANCESTRY_VERSION}}}]
            }
        },
        "seq_no_primary_term": True,
    }
    _source = {"includes": ["collection_lidvid", "batch_id", "product_lidvid"]}
    query_f = query_registry_db_or_mock(registry_db_mock, "get_nonaggregate_ancestry_records", use_search_after=True)

    # each document will have many product lidvids, so a smaller page size is warranted here
    docs = query_f(
        client,
        resolve_multitenant_index_name(client, "registry-refs"),
        query,
        _source,
        page_size=AncestryRuntimeConstants.nonaggregate_ancestry_records_query_page_size,
        request_timeout_seconds=30,
        sort_fields=["collection_lidvid", "batch_id"],
    )

    return docs


def get_nonaggregate_ancestry_records_for_collection_lid_query(
    client: OpenSearch, collection_lid: PdsLid, registry_db_mock: DbMockTypeDef
) -> Iterable[Dict]:
    # Query the registry-refs index for the contents of all collections
    query: Dict = {
        "query": {
            "bool": {
                "must_not": [{"range": {SWEEPERS_ANCESTRY_VERSION_METADATA_KEY: {"gte": SWEEPERS_ANCESTRY_VERSION}}}],
                "filter": [{"term": {"collection_lid": str(collection_lid)}}],
            }
        },
        "seq_no_primary_term": True,
    }
    _source = {"includes": ["collection_lidvid", "batch_id", "product_lidvid"]}
    query_f = query_registry_db_or_mock(
        registry_db_mock,
        f"get_nonaggregate_ancestry_records_for_collection_lid-{collection_lid}",
        use_search_after=True,
    )

    # each document will have many product lidvids, so a smaller page size is warranted here
    docs = query_f(
        client,
        resolve_multitenant_index_name(client, "registry-refs"),
        query,
        _source,
        page_size=AncestryRuntimeConstants.nonaggregate_ancestry_records_query_page_size,
        request_timeout_seconds=30,
        sort_fields=["collection_lidvid", "batch_id"],
    )

    return docs


_orphaned_docs_query = {
    "query": {
        "bool": {"must_not": [{"range": {SWEEPERS_ANCESTRY_VERSION_METADATA_KEY: {"gte": SWEEPERS_ANCESTRY_VERSION}}}]}
    }
}


def get_orphaned_documents(client: OpenSearch, registry_db_mock: DbMockTypeDef, index_name: str) -> Iterable[Dict]:
    # Query an index for documents without an up-to-date ancestry version reference - this would indicate a product
    # which is orphaned and is getting missed in processing
    _source: Dict = {"includes": []}
    query_f = query_registry_db_or_mock(registry_db_mock, "get_orphaned_ancestry_docs", use_search_after=True)

    sort_fields_override = (
        ["collection_lidvid", "batch_id"] if "registry-refs" in index_name else None
    )  # use default for registry

    docs = query_f(client, index_name, _orphaned_docs_query, _source, sort_fields=sort_fields_override)

    return docs


def get_orphaned_documents_count(client: OpenSearch, index_name: str) -> int:
    # Query an index documents without an up-to-date ancestry version reference - this would indicate a product which is
    # orphaned and is getting missed in processing
    return get_query_hits_count(client, index_name, _orphaned_docs_query)


def get_existing_ancestry_for_product(
    client: OpenSearch, product_lidvid: PdsLidVid, registry_db_mock: DbMockTypeDef
) -> Dict:
    # Retrieve ancestry for a single document.  It would be simpler to just pull it by id, but this is compatible with
    # the existing functional testing framework.
    query: Dict = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"lidvid": str(product_lidvid)}},
                ],
            }
        },
    }
    _source = {
        "includes": [
            "lidvid",
            METADATA_PARENT_BUNDLE_KEY,
            METADATA_PARENT_COLLECTION_KEY,
            SWEEPERS_ANCESTRY_VERSION_METADATA_KEY,
        ]
    }
    query_f = query_registry_db_or_mock(
        registry_db_mock, f"get_existing_ancestry_for_product-{product_lidvid}", use_search_after=True
    )

    docs = query_f(
        client,
        resolve_multitenant_index_name(client, "registry"),
        query,
        _source,
    )

    return list(docs)[0]
