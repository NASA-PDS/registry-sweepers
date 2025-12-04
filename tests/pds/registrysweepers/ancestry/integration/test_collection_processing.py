"""Integration tests for collection ancestry processing"""
import pytest
from pds.registrysweepers.ancestry.generation import process_collection_ancestries_for_nonaggregates

from ..builders import build_collection
from ..builders import CollectionRefsBuilder
from ..mock_opensearch import create_search_response
from ..mock_opensearch import query_matches_product_class


class TestCollectionProcessing:
    """Test collection ancestry processing for non-aggregate products"""

    def test_collection_with_multiple_members(self, mock_opensearch_client):
        """Collection with multiple member products gets correct ancestry"""
        collection_lidvid = "urn:nasa:pds:mission:data::1.0"
        collection = build_collection(collection_lidvid, "Mission Data Collection")

        refs = CollectionRefsBuilder(collection_lidvid) \
            .with_product("urn:nasa:pds:mission:data:product1::1.0") \
            .with_product("urn:nasa:pds:mission:data:product2::1.0") \
            .with_product("urn:nasa:pds:mission:data:product3::1.0") \
            .build()

        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Collection"),
            response_data=create_search_response([collection])
        )
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry-refs.*",
            query_matcher=lambda q: collection_lidvid in str(q),
            response_data=create_search_response([refs])
        )

        # Execute
        records = list(process_collection_ancestries_for_nonaggregates(mock_opensearch_client))

        # Verify we got records for the products
        product_records = [r for r in records if 'product' in str(r.product)]
        assert len(product_records) == 3

        # Each product should have the collection as ancestor
        for record in product_records:
            ancestor_strs = [str(a) for a in record.direct_ancestor_refs]
            assert any(collection_lidvid in a for a in ancestor_strs)

    def test_collection_with_no_members(self, mock_opensearch_client):
        """Empty collection should still be marked complete"""
        collection_lidvid = "urn:nasa:pds:empty_collection::1.0"
        collection = build_collection(collection_lidvid, "Empty Collection")

        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Collection"),
            response_data=create_search_response([collection])
        )
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry-refs.*",
            query_matcher=lambda q: True,
            response_data=create_search_response([])  # No members
        )

        # Execute
        records = list(process_collection_ancestries_for_nonaggregates(mock_opensearch_client))

        # Should have at least the collection marked as complete
        collection_records = [r for r in records if str(r.product) == collection_lidvid]
        assert len(collection_records) >= 1

    def test_multiple_collections_processed(self, mock_opensearch_client):
        """Multiple collections are all processed"""
        collections = [
            build_collection("urn:nasa:pds:collection1::1.0", "Collection 1"),
            build_collection("urn:nasa:pds:collection2::1.0", "Collection 2"),
            build_collection("urn:nasa:pds:collection3::1.0", "Collection 3"),
        ]

        refs = [
            CollectionRefsBuilder("urn:nasa:pds:collection1::1.0")
                .with_product("urn:nasa:pds:collection1:product1::1.0")
                .build(),
            CollectionRefsBuilder("urn:nasa:pds:collection2::1.0")
                .with_product("urn:nasa:pds:collection2:product1::1.0")
                .build(),
            CollectionRefsBuilder("urn:nasa:pds:collection3::1.0")
                .with_product("urn:nasa:pds:collection3:product1::1.0")
                .build(),
        ]

        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Collection"),
            response_data=create_search_response(collections)
        )
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry-refs.*",
            query_matcher=lambda q: True,
            response_data=create_search_response(refs)
        )

        # Execute
        records = list(process_collection_ancestries_for_nonaggregates(mock_opensearch_client))

        # Should have records for all products from all collections
        product_records = [r for r in records if 'product1' in str(r.product)]
        assert len(product_records) >= 3

    def test_collection_with_batched_refs(self, mock_opensearch_client, multi_batch_refs):
        """Collection with refs split across multiple batches processes all products"""
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Collection"),
            response_data=create_search_response(multi_batch_refs['collections'])
        )
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry-refs.*",
            query_matcher=lambda q: True,
            response_data=create_search_response(multi_batch_refs['collection_refs'])
        )

        # Execute
        records = list(process_collection_ancestries_for_nonaggregates(mock_opensearch_client))

        # Should have records for all 30 products (3 batches × 10 products each)
        product_records = [r for r in records if 'product_' in str(r.product)]
        assert len(product_records) == 30

    def test_large_collection_memory_efficiency(self, mock_opensearch_client, large_collection_hierarchy):
        """Processing large collection should not load all members into memory at once"""
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Collection"),
            response_data=create_search_response(large_collection_hierarchy['collections'])
        )
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry-refs.*",
            query_matcher=lambda q: True,
            response_data=create_search_response(large_collection_hierarchy['collection_refs'])
        )

        # Execute - should use generator/iterator pattern
        records_generator = process_collection_ancestries_for_nonaggregates(mock_opensearch_client)

        # Verify it's a generator (doesn't materialize all at once)
        import types
        assert isinstance(records_generator, (types.GeneratorType, map, filter))

        # Consume and verify count
        records = list(records_generator)
        product_records = [r for r in records if 'product_' in str(r.product)]
        assert len(product_records) == 100
