"""Pytest fixtures for ancestry module tests.

Provides reusable test data fixtures representing common test scenarios.
"""
import pytest

from .builders import build_bundle
from .builders import build_collection
from .builders import build_product
from .builders import CollectionRefsBuilder
from .builders import ProductDocumentBuilder


@pytest.fixture
def simple_collection_hierarchy():
    """Minimal hierarchy: Bundle -> Collection -> 2 Products.

    Structure:
        test_bundle (Product_Bundle)
        └── test_collection (Product_Collection)
            ├── test_product_1 (Product_Observational)
            └── test_product_2 (Product_Observational)

    Returns:
        Dict with 'bundles', 'collections', and 'collection_refs' lists
    """
    bundle_lidvid = "urn:nasa:pds:test_bundle::1.0"
    collection_lidvid = "urn:nasa:pds:test_collection::1.0"
    collection_lid = "urn:nasa:pds:test_collection"

    return {
        'bundles': [
            build_bundle(bundle_lidvid, collection_lid, "Test Bundle")
        ],
        'collections': [
            build_collection(collection_lidvid, "Test Collection")
        ],
        'collection_refs': [
            CollectionRefsBuilder(collection_lidvid)
                .with_product("urn:nasa:pds:test_collection:product_1::1.0")
                .with_product("urn:nasa:pds:test_collection:product_2::1.0")
                .build()
        ]
    }


@pytest.fixture
def complex_multi_version_hierarchy():
    """Complex scenario with multiple versions and cross-references.

    Structure:
        bundle_v1 -> collection_v1 -> [product_1_v1, product_2_v1]
        bundle_v2 -> collection_v2 -> [product_1_v2, product_2_v2, product_3_v1]

    Returns:
        Dict with 'bundles', 'collections', and 'collection_refs' lists
    """
    bundle_lid = "urn:nasa:pds:mission:bundle"
    collection_lid = "urn:nasa:pds:mission:collection"

    return {
        'bundles': [
            build_bundle(f"{bundle_lid}::1.0", collection_lid, "Mission Bundle v1.0"),
            build_bundle(f"{bundle_lid}::2.0", collection_lid, "Mission Bundle v2.0"),
        ],
        'collections': [
            build_collection(f"{collection_lid}::1.0", "Mission Collection v1.0"),
            build_collection(f"{collection_lid}::2.0", "Mission Collection v2.0"),
        ],
        'collection_refs': [
            CollectionRefsBuilder(f"{collection_lid}::1.0")
                .with_product(f"{collection_lid}:product_1::1.0")
                .with_product(f"{collection_lid}:product_2::1.0")
                .build(),
            CollectionRefsBuilder(f"{collection_lid}::2.0")
                .with_product(f"{collection_lid}:product_1::2.0")
                .with_product(f"{collection_lid}:product_2::2.0")
                .with_product(f"{collection_lid}:product_3::1.0")
                .build(),
        ]
    }


@pytest.fixture
def empty_collection_hierarchy():
    """Collection with no member products.

    Structure:
        test_bundle (Product_Bundle)
        └── empty_collection (Product_Collection)
            └── (no members)

    Returns:
        Dict with 'bundles', 'collections', and 'collection_refs' lists
    """
    bundle_lidvid = "urn:nasa:pds:test_bundle::1.0"
    collection_lidvid = "urn:nasa:pds:empty_collection::1.0"
    collection_lid = "urn:nasa:pds:empty_collection"

    return {
        'bundles': [
            build_bundle(bundle_lidvid, collection_lid, "Bundle with Empty Collection")
        ],
        'collections': [
            build_collection(collection_lidvid, "Empty Collection")
        ],
        'collection_refs': []  # No member products
    }


@pytest.fixture
def large_collection_hierarchy():
    """Collection with many member products (for performance testing).

    Structure:
        bundle -> collection -> 100 products

    Returns:
        Dict with 'bundles', 'collections', and 'collection_refs' lists
    """
    bundle_lidvid = "urn:nasa:pds:large_bundle::1.0"
    collection_lidvid = "urn:nasa:pds:large_collection::1.0"
    collection_lid = "urn:nasa:pds:large_collection"

    # Generate 100 product LIDVIDs
    product_lidvids = [
        f"urn:nasa:pds:large_collection:product_{i:04d}::1.0"
        for i in range(100)
    ]

    return {
        'bundles': [
            build_bundle(bundle_lidvid, collection_lid, "Large Bundle")
        ],
        'collections': [
            build_collection(collection_lidvid, "Large Collection")
        ],
        'collection_refs': [
            CollectionRefsBuilder(collection_lidvid)
                .with_products(product_lidvids)
                .build()
        ]
    }


@pytest.fixture
def malformed_documents_scenario():
    """Documents with missing required fields for error handling tests.

    Returns:
        Dict with 'malformed_collections' and 'malformed_bundles' lists
    """
    return {
        'malformed_collections': [
            # Missing lidvid field
            {
                "_source": {
                    "product_class": "Product_Collection",
                    "title": "Collection Missing LIDVID"
                },
                "_id": "unknown"
            },
            # Missing product_class field
            {
                "_source": {
                    "lidvid": "urn:nasa:pds:malformed::1.0",
                    "title": "Collection Missing Product Class"
                },
                "_id": "urn:nasa:pds:malformed::1.0"
            }
        ],
        'malformed_bundles': [
            # Bundle with empty alternate_ids
            {
                "_source": {
                    "lidvid": "urn:nasa:pds:bad_bundle::1.0",
                    "product_class": "Product_Bundle",
                    "alternate_ids": []
                },
                "_id": "urn:nasa:pds:bad_bundle::1.0"
            }
        ]
    }


@pytest.fixture
def versioned_products_scenario():
    """Products with various ancestry version states.

    Returns:
        Dict with products at different version states
    """
    from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION

    return {
        'needs_update': [
            # No version field - needs update
            build_collection("urn:nasa:pds:no_version::1.0", "No Version"),
            # Old version - needs update
            build_collection(
                "urn:nasa:pds:old_version::1.0",
                "Old Version",
                ancestry_version=SWEEPERS_ANCESTRY_VERSION - 1
            )
        ],
        'current_version': [
            # Already at current version - skip
            build_collection(
                "urn:nasa:pds:current::1.0",
                "Current Version",
                ancestry_version=SWEEPERS_ANCESTRY_VERSION
            )
        ],
        'future_version': [
            # Future version (shouldn't happen, but test defensively)
            build_collection(
                "urn:nasa:pds:future::1.0",
                "Future Version",
                ancestry_version=SWEEPERS_ANCESTRY_VERSION + 1
            )
        ]
    }


@pytest.fixture
def multi_batch_refs():
    """Collection refs split across multiple batches.

    Some collections have their member products split across multiple
    batch documents in the registry-refs index.

    Returns:
        Dict with collection and multiple ref batch documents
    """
    collection_lidvid = "urn:nasa:pds:multi_batch::1.0"

    return {
        'collections': [
            build_collection(collection_lidvid, "Multi-batch Collection")
        ],
        'collection_refs': [
            CollectionRefsBuilder(collection_lidvid)
                .with_batch_id(1)
                .with_products([
                    f"urn:nasa:pds:multi_batch:product_{i:04d}::1.0"
                    for i in range(10)
                ])
                .build(),
            CollectionRefsBuilder(collection_lidvid)
                .with_batch_id(2)
                .with_products([
                    f"urn:nasa:pds:multi_batch:product_{i:04d}::1.0"
                    for i in range(10, 20)
                ])
                .build(),
            CollectionRefsBuilder(collection_lidvid)
                .with_batch_id(3)
                .with_products([
                    f"urn:nasa:pds:multi_batch:product_{i:04d}::1.0"
                    for i in range(20, 30)
                ])
                .build(),
        ]
    }
