"""Integration tests for complete ancestry processing pipeline"""
import json

import pytest
from pds.registrysweepers.ancestry import main
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY

from ..builders import build_bundle
from ..builders import build_collection
from ..builders import build_product
from ..mock_opensearch import create_search_response
from ..mock_opensearch import query_matches_product_class


class TestFullPipeline:
    """Test complete ancestry.run() pipeline with mocked OpenSearch"""

    def test_basic_hierarchy_processing(self, mock_opensearch_client, simple_collection_hierarchy):
        """Test Bundle -> Collection -> Products creates correct ancestry"""
        # Setup mock responses for all queries
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Bundle"),
            response_data=create_search_response(simple_collection_hierarchy['bundles'])
        )
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Collection"),
            response_data=create_search_response(simple_collection_hierarchy['collections'])
        )
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry-refs.*",
            query_matcher=lambda q: True,
            response_data=create_search_response(simple_collection_hierarchy['collection_refs'])
        )

        # Accumulators to capture intermediate results
        bulk_updates = []

        # Execute
        main.run(client=mock_opensearch_client, bulk_updates_sink=bulk_updates)

        # Verify bulk updates were generated
        assert len(bulk_updates) > 0

        # Parse and verify updates
        updates_by_id = {u.id: u for u in bulk_updates}

        # Verify collection has bundle as ancestor
        collection_id = "urn:nasa:pds:test_collection::1.0"
        assert collection_id in updates_by_id
        collection_update = updates_by_id[collection_id]
        assert 'ops:Provenance/ops:ancestor_refs' in collection_update.content

        # Verify products have collection as ancestor
        product_ids = [
            "urn:nasa:pds:test_collection:product_1::1.0",
            "urn:nasa:pds:test_collection:product_2::1.0"
        ]
        for product_id in product_ids:
            if product_id in updates_by_id:
                product_update = updates_by_id[product_id]
                refs = product_update.content.get('ops:Provenance/ops:ancestor_refs', [])
                # Should reference collection
                assert any('test_collection' in str(ref) for ref in refs)

    def test_version_skipping(self, mock_opensearch_client):
        """Products already at current version should be filtered by query"""
        # Collection already has current ancestry version
        collection_with_version = build_collection(
            "urn:nasa:pds:collection::1.0",
            ancestry_version=SWEEPERS_ANCESTRY_VERSION
        )

        # Query should filter this out, so return empty results
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Bundle"),
            response_data=create_search_response([])
        )
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Collection"),
            response_data=create_search_response([])  # Filtered by version in query
        )

        bulk_updates = []

        # Execute
        main.run(client=mock_opensearch_client, bulk_updates_sink=bulk_updates)

        # Should have no updates since no products need processing
        assert len(bulk_updates) == 0

    def test_empty_results_completes_successfully(self, mock_opensearch_client):
        """No pending products should complete without errors"""
        # Return empty results for all queries
        mock_opensearch_client.register_search_response(
            index_pattern=".*",
            query_matcher=lambda q: True,
            response_data=create_search_response([])
        )

        bulk_updates = []

        # Should not raise
        main.run(client=mock_opensearch_client, bulk_updates_sink=bulk_updates)

        # Should not generate any updates
        assert len(bulk_updates) == 0

    def test_ancestry_version_stamped_on_all_updates(self, mock_opensearch_client, simple_collection_hierarchy):
        """All updates should include current ancestry version"""
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Bundle"),
            response_data=create_search_response(simple_collection_hierarchy['bundles'])
        )
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Collection"),
            response_data=create_search_response(simple_collection_hierarchy['collections'])
        )
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry-refs.*",
            query_matcher=lambda q: True,
            response_data=create_search_response(simple_collection_hierarchy['collection_refs'])
        )

        bulk_updates = []

        # Execute
        main.run(client=mock_opensearch_client, bulk_updates_sink=bulk_updates)

        # Verify all updates have version stamp
        for update in bulk_updates:
            assert SWEEPERS_ANCESTRY_VERSION_METADATA_KEY in update.content
            assert update.content[SWEEPERS_ANCESTRY_VERSION_METADATA_KEY] == SWEEPERS_ANCESTRY_VERSION

    def test_deduplication_script_included(self, mock_opensearch_client, simple_collection_hierarchy):
        """All updates should include deduplication script"""
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Bundle"),
            response_data=create_search_response(simple_collection_hierarchy['bundles'])
        )
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: query_matches_product_class(q, "Product_Collection"),
            response_data=create_search_response(simple_collection_hierarchy['collections'])
        )
        mock_opensearch_client.register_search_response(
            index_pattern=".*registry-refs.*",
            query_matcher=lambda q: True,
            response_data=create_search_response(simple_collection_hierarchy['collection_refs'])
        )

        bulk_updates = []

        # Execute
        main.run(client=mock_opensearch_client, bulk_updates_sink=bulk_updates)

        # Verify all updates have inline script
        for update in bulk_updates:
            assert update.inline_script_content is not None
            assert len(update.inline_script_content) > 0
