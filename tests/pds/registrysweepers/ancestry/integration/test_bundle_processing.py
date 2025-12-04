"""Integration tests for bundle-collection ancestry processing"""
import pytest
from pds.registrysweepers.ancestry.generation import process_collection_bundle_ancestry

from ..builders import build_bundle
from ..builders import build_collection
from ..mock_opensearch import create_search_response
from ..mock_opensearch import query_matches_product_class


class TestBundleProcessing:
    """Test bundle-to-collection ancestry linking"""

    def test_bundle_links_to_collection(self, mock_opensearch_client):
        """Bundle referencing a collection creates ancestry link"""
        bundle_lidvid = "urn:nasa:pds:test_bundle::1.0"
        collection_lidvid = "urn:nasa:pds:test_collection::1.0"
        collection_lid = "urn:nasa:pds:test_collection"

        bundle = build_bundle(bundle_lidvid, collection_lid, "Test Bundle")
        collection = build_collection(collection_lidvid, "Test Collection")

        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Bundle"),
            response_data=create_search_response([bundle])
        )
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Collection"),
            response_data=create_search_response([collection])
        )

        # Execute
        records = list(process_collection_bundle_ancestry(mock_opensearch_client))

        # Should have records for both bundle and collection
        assert len(records) >= 2

        # Find collection record
        collection_records = [r for r in records if str(r.product) == collection_lidvid]
        assert len(collection_records) == 1

        collection_record = collection_records[0]
        ancestor_strs = [str(a) for a in collection_record.direct_ancestor_refs]

        # Collection should have bundle as ancestor
        assert any(bundle_lidvid in a for a in ancestor_strs)

    def test_bundle_references_multiple_collections(self, mock_opensearch_client):
        """Bundle referencing multiple collections links to all"""
        bundle_lidvid = "urn:nasa:pds:multi_bundle::1.0"
        collection1_lidvid = "urn:nasa:pds:collection1::1.0"
        collection2_lidvid = "urn:nasa:pds:collection2::1.0"
        collection1_lid = "urn:nasa:pds:collection1"
        collection2_lid = "urn:nasa:pds:collection2"

        # Bundle with multiple collection references
        bundle = {
            "_source": {
                "lidvid": bundle_lidvid,
                "product_class": "Product_Bundle",
                "title": "Multi-Collection Bundle",
                "ref_lid_collection": [collection1_lid, collection2_lid],
                "alternate_ids": [collection1_lid, collection2_lid]
            },
            "_id": bundle_lidvid
        }

        collections = [
            build_collection(collection1_lidvid, "Collection 1"),
            build_collection(collection2_lidvid, "Collection 2"),
        ]

        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Bundle"),
            response_data=create_search_response([bundle])
        )
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Collection"),
            response_data=create_search_response(collections)
        )

        # Execute
        records = list(process_collection_bundle_ancestry(mock_opensearch_client))

        # Both collections should have bundle as ancestor
        for collection_lidvid in [collection1_lidvid, collection2_lidvid]:
            collection_records = [r for r in records if str(r.product) == collection_lidvid]
            assert len(collection_records) >= 1

            collection_record = collection_records[0]
            ancestor_strs = [str(a) for a in collection_record.direct_ancestor_refs]
            assert any(bundle_lidvid in a for a in ancestor_strs)

    def test_multiple_bundles_reference_same_collection(self, mock_opensearch_client):
        """Multiple bundles can reference the same collection"""
        collection_lidvid = "urn:nasa:pds:shared_collection::1.0"
        collection_lid = "urn:nasa:pds:shared_collection"
        bundle1_lidvid = "urn:nasa:pds:bundle1::1.0"
        bundle2_lidvid = "urn:nasa:pds:bundle2::1.0"

        bundles = [
            build_bundle(bundle1_lidvid, collection_lid, "Bundle 1"),
            build_bundle(bundle2_lidvid, collection_lid, "Bundle 2"),
        ]

        collection = build_collection(collection_lidvid, "Shared Collection")

        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Bundle"),
            response_data=create_search_response(bundles)
        )
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Collection"),
            response_data=create_search_response([collection])
        )

        # Execute
        records = list(process_collection_bundle_ancestry(mock_opensearch_client))

        # Collection should have both bundles as ancestors
        collection_records = [r for r in records if str(r.product) == collection_lidvid]
        assert len(collection_records) >= 1

        collection_record = collection_records[0]
        ancestor_strs = [str(a) for a in collection_record.direct_ancestor_refs]

        # Should have both bundles
        assert any(bundle1_lidvid in a for a in ancestor_strs)
        assert any(bundle2_lidvid in a for a in ancestor_strs)

    def test_bundle_without_collections(self, mock_opensearch_client):
        """Bundle with no collection references still gets processed"""
        bundle_lidvid = "urn:nasa:pds:standalone_bundle::1.0"
        bundle = {
            "_source": {
                "lidvid": bundle_lidvid,
                "product_class": "Product_Bundle",
                "title": "Standalone Bundle",
                "alternate_ids": []
            },
            "_id": bundle_lidvid
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

        # Execute - should not raise
        records = list(process_collection_bundle_ancestry(mock_opensearch_client))

        # Should still get a record for the bundle
        bundle_records = [r for r in records if str(r.product) == bundle_lidvid]
        assert len(bundle_records) >= 1

    def test_collection_without_bundles(self, mock_opensearch_client):
        """Collection not referenced by any bundle still gets processed"""
        collection_lidvid = "urn:nasa:pds:orphan_collection::1.0"
        collection = build_collection(collection_lidvid, "Orphan Collection")

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

        # Execute
        records = list(process_collection_bundle_ancestry(mock_opensearch_client))

        # Should still get a record for the collection
        collection_records = [r for r in records if str(r.product) == collection_lidvid]
        assert len(collection_records) >= 1

        # Collection should have no ancestors
        collection_record = collection_records[0]
        assert len(collection_record.direct_ancestor_refs) == 0
