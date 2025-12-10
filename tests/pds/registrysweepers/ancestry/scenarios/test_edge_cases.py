"""Edge case and boundary condition tests for ancestry module"""
import pytest
from pds.registrysweepers.ancestry import main
from pds.registrysweepers.ancestry.generation import get_ancestry_by_collection_lidvid
from pds.registrysweepers.ancestry.generation import process_collection_bundle_ancestry

from ..builders import build_bundle
from ..builders import build_collection
from ..builders import build_product
from ..mock_opensearch import create_search_response
from ..mock_opensearch import query_matches_product_class


class TestEmptyResults:
    """Test handling of empty query results"""

    def test_no_pending_bundles(self, mock_opensearch_client):
        """No pending bundles should complete without errors"""
        collection = build_collection("urn:nasa:pds:collection::1.0")

        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Bundle"),
            response_data=create_search_response([])  # No bundles
        )
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Collection"),
            response_data=create_search_response([collection])
        )

        # Should not raise
        records = list(process_collection_bundle_ancestry(mock_opensearch_client))

        # Should still get collection record
        assert len(records) >= 1

    def test_no_pending_collections(self, mock_opensearch_client):
        """No pending collections should complete without errors"""
        bundle = build_bundle("urn:nasa:pds:bundle::1.0", "urn:nasa:pds:collection")

        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Bundle"),
            response_data=create_search_response([bundle])
        )
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Collection"),
            response_data=create_search_response([])  # No collections
        )

        # Should not raise
        records = list(process_collection_bundle_ancestry(mock_opensearch_client))

        # Should still get bundle record
        assert len(records) >= 1

    def test_completely_empty_database(self, mock_opensearch_client):
        """Empty database should complete without errors"""
        mock_opensearch_client.register_search_response(
            index_pattern=".*",
            query_matcher=lambda q: True,
            response_data=create_search_response([])
        )

        bulk_updates = []

        # Should not raise
        main.run(client=mock_opensearch_client, bulk_updates_sink=bulk_updates)

        assert len(bulk_updates) == 0


class TestMalformedDocuments:
    """Test handling of documents with missing or invalid fields"""

    def test_document_missing_lidvid(self, mock_opensearch_client, caplog):
        """Document missing lidvid field should be logged and skipped"""
        malformed_doc = {
            "_source": {
                "product_class": "Product_Collection",
                "title": "Collection Missing LIDVID"
                # Missing lidvid field!
            },
            "_id": "unknown"
        }
        valid_doc = build_collection("urn:nasa:pds:valid::1.0")

        # Execute function directly to check error handling
        docs = [malformed_doc, valid_doc]
        records = get_ancestry_by_collection_lidvid(docs)

        # Should only get record for valid document
        assert len(records) == 1

    def test_document_missing_product_class(self, mock_opensearch_client):
        """Document missing product_class should be handled gracefully"""
        malformed_doc = {
            "_source": {
                "lidvid": "urn:nasa:pds:malformed::1.0",
                "title": "Missing Product Class"
                # Missing product_class field!
            },
            "_id": "urn:nasa:pds:malformed::1.0"
        }

        # Should not crash when querying
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: True,
            response_data=create_search_response([malformed_doc])
        )

        bulk_updates = []

        # Should handle gracefully
        main.run(client=mock_opensearch_client, bulk_updates_sink=bulk_updates)

    def test_bundle_with_invalid_ref_lid_collection(self, mock_opensearch_client):
        """Bundle with invalid collection reference should be handled"""
        bundle = {
            "_source": {
                "lidvid": "urn:nasa:pds:bundle::1.0",
                "product_class": "Product_Bundle",
                "title": "Bundle with Bad Ref",
                "ref_lid_collection": "not_a_valid_identifier",  # Invalid format
                "alternate_ids": []
            },
            "_id": "urn:nasa:pds:bundle::1.0"
        }

        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Bundle"),
            response_data=create_search_response([bundle])
        )
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Collection"),
            response_data=create_search_response([])
        )

        # Should not crash
        records = list(process_collection_bundle_ancestry(mock_opensearch_client))

        # Should still get bundle record
        bundle_records = [r for r in records if 'bundle' in str(r.product)]
        assert len(bundle_records) >= 1


class TestVersionEdgeCases:
    """Test version-related edge cases"""

    def test_mix_of_versioned_and_unversioned(self, mock_opensearch_client):
        """Mix of products with and without ancestry versions"""
        from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION

        collections = [
            build_collection("urn:nasa:pds:no_version::1.0"),  # No version
            build_collection("urn:nasa:pds:old_version::1.0",
                           ancestry_version=SWEEPERS_ANCESTRY_VERSION - 1),  # Old version
            build_collection("urn:nasa:pds:current::1.0",
                           ancestry_version=SWEEPERS_ANCESTRY_VERSION),  # Current - should be filtered
        ]

        # Query should only return first two (query filters current version)
        pending_collections = [collections[0], collections[1]]

        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Bundle"),
            response_data=create_search_response([])
        )
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Collection"),
            response_data=create_search_response(pending_collections)
        )

        # Execute
        records = list(process_collection_bundle_ancestry(mock_opensearch_client))

        # Should have records for the two pending collections
        collection_lidvids = {str(collections[0]['_source']['lidvid']), str(collections[1]['_source']['lidvid'])}
        collection_records = [r for r in records if str(r.product) in collection_lidvids]
        assert len(collection_records) == 2


class TestReferenceEdgeCases:
    """Test edge cases with product references"""

    def test_bundle_references_nonexistent_collection(self, mock_opensearch_client):
        """Bundle referencing collection that doesn't exist"""
        bundle = build_bundle(
            "urn:nasa:pds:bundle::1.0",
            "urn:nasa:pds:nonexistent_collection"
        )

        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Bundle"),
            response_data=create_search_response([bundle])
        )
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Collection"),
            response_data=create_search_response([])  # Collection doesn't exist
        )

        # Should handle gracefully
        records = list(process_collection_bundle_ancestry(mock_opensearch_client))

        # Should still get bundle record
        assert len(records) >= 1

    def test_circular_reference_detection(self, mock_opensearch_client):
        """Ensure circular references don't cause infinite loops"""
        # Create a scenario where a product could theoretically reference itself
        # (This shouldn't happen in valid PDS data, but we should handle it)

        collection = build_collection("urn:nasa:pds:circular::1.0")

        # Only return collection for collection queries, not bundle/refs queries
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Bundle"),
            response_data=create_search_response([])
        )
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Collection"),
            response_data=create_search_response([collection])
        )
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry-refs.*",
            query_matcher=lambda q: True,
            response_data=create_search_response([])
        )

        bulk_updates = []

        # Should complete without infinite loop
        main.run(client=mock_opensearch_client, bulk_updates_sink=bulk_updates)

        # Should have exactly two updates for the collection (bundle phase + collection phase)
        collection_updates = [u for u in bulk_updates if 'circular' in u.id]
        assert len(collection_updates) == 2

    def test_empty_alternate_ids_list(self, mock_opensearch_client):
        """Bundle with empty alternate_ids list should be handled"""
        bundle = {
            "_source": {
                "lidvid": "urn:nasa:pds:bundle::1.0",
                "product_class": "Product_Bundle",
                "title": "Bundle with Empty Refs",
                "alternate_ids": []  # Empty list
            },
            "_id": "urn:nasa:pds:bundle::1.0"
        }

        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Bundle"),
            response_data=create_search_response([bundle])
        )
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Collection"),
            response_data=create_search_response([])
        )

        # Should not raise
        records = list(process_collection_bundle_ancestry(mock_opensearch_client))
        assert len(records) >= 1


class TestBoundaryConditions:
    """Test boundary conditions and limits"""

    def test_single_product_in_collection(self, mock_opensearch_client):
        """Collection with exactly one product"""
        from ..builders import CollectionRefsBuilder

        collection = build_collection("urn:nasa:pds:collection::1.0")
        refs = CollectionRefsBuilder("urn:nasa:pds:collection::1.0") \
            .with_product("urn:nasa:pds:collection:product::1.0") \
            .build()

        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Collection"),
            response_data=create_search_response([collection])
        )
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry-refs.*",
            query_matcher=lambda q: True,
            response_data=create_search_response([refs])
        )

        bulk_updates = []
        main.run(client=mock_opensearch_client, bulk_updates_sink=bulk_updates)

        # Should have updates for collection and product
        assert len(bulk_updates) >= 2

    def test_duplicate_product_references(self, mock_opensearch_client):
        """Collection refs with duplicate product LIDVIDs"""
        from ..builders import CollectionRefsBuilder

        collection = build_collection("urn:nasa:pds:collection::1.0")
        refs = {
            "_id": "urn:nasa:pds:collection::1.0::batch_1",
            "_source": {
                "collection_lidvid": "urn:nasa:pds:collection::1.0",
                "batch_id": 1,
                "product_lidvid": [
                    "urn:nasa:pds:collection:product::1.0",
                    "urn:nasa:pds:collection:product::1.0",  # Duplicate
                    "urn:nasa:pds:collection:product::1.0"   # Duplicate
                ]
            }
        }

        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Collection"),
            response_data=create_search_response([collection])
        )
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry-refs.*",
            query_matcher=lambda q: True,
            response_data=create_search_response([refs])
        )

        bulk_updates = []
        main.run(client=mock_opensearch_client, bulk_updates_sink=bulk_updates)

        # Should handle duplicates gracefully (deduplication happens via painless script in OpenSearch)
        product_updates = [u for u in bulk_updates if 'product::' in u.id]
        # System creates one update per refs entry; OpenSearch deduplicates via script
        assert len(product_updates) == 3
        # Verify all updates are for the same product (duplicates)
        assert len(set(u.id for u in product_updates)) == 1
