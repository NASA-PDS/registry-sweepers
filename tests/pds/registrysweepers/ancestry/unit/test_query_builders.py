"""Unit tests for query builder functions in queries.py"""
import pytest
from pds.registrysweepers.ancestry.queries import product_class_query_factory
from pds.registrysweepers.ancestry.queries import ProductClass
from pds.registrysweepers.ancestry.queries import query_for_pending_bundles
from pds.registrysweepers.ancestry.queries import query_for_pending_collections
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY


class TestProductClassQueryFactory:
    """Test product_class_query_factory constructs correct queries"""

    def test_bundle_query_structure(self):
        """Verify bundle query includes correct product_class filter"""
        query = product_class_query_factory(ProductClass.BUNDLE)

        assert "query" in query
        assert "bool" in query["query"]
        assert "filter" in query["query"]["bool"]
        filters = query["query"]["bool"]["filter"]
        assert len(filters) == 1
        assert filters[0] == {"term": {"product_class": "Product_Bundle"}}

    def test_collection_query_structure(self):
        """Verify collection query includes correct product_class filter"""
        query = product_class_query_factory(ProductClass.COLLECTION)

        assert "query" in query
        assert "bool" in query["query"]
        assert "filter" in query["query"]["bool"]
        filters = query["query"]["bool"]["filter"]
        assert len(filters) == 1
        assert filters[0] == {"term": {"product_class": "Product_Collection"}}

    def test_non_aggregate_query_structure(self):
        """Verify non-aggregate query excludes bundles and collections"""
        query = product_class_query_factory(ProductClass.NON_AGGREGATE)

        assert "query" in query
        assert "bool" in query["query"]
        assert "must_not" in query["query"]["bool"]
        must_not = query["query"]["bool"]["must_not"]
        assert len(must_not) == 1
        assert must_not[0] == {"terms": {"product_class": ["Product_Bundle", "Product_Collection"]}}


class TestQueryForPendingBundles:
    """Test query_for_pending_bundles constructs and executes correct queries"""

    def test_query_includes_bundle_filter(self, mock_opensearch_client):
        """Verify query filters for Product_Bundle"""
        from ..mock_opensearch import create_empty_response

        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: True,
            response_data=create_empty_response()
        )

        # Execute
        list(query_for_pending_bundles(mock_opensearch_client))

        # Verify
        assert len(mock_opensearch_client.search_calls) >= 1
        call = mock_opensearch_client.search_calls[0]
        query = call['body']

        # Check for bundle filter
        assert "query" in query
        filters = query["query"]["bool"]["filter"]
        assert {"term": {"product_class": "Product_Bundle"}} in filters

    def test_source_fields_requested(self, mock_opensearch_client):
        """Verify only necessary fields are retrieved"""
        from ..mock_opensearch import create_empty_response

        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: True,
            response_data=create_empty_response()
        )

        # Execute
        list(query_for_pending_bundles(mock_opensearch_client))

        # Verify _source includes expected fields
        # _source is passed as _source_includes in kwargs
        # The first call is for count (size=0), second call has the actual query
        actual_query_call = [c for c in mock_opensearch_client.search_calls if c['kwargs'].get('size', 0) > 0][0]
        includes = actual_query_call['kwargs']['_source_includes']
        assert 'lidvid' in includes
        assert 'ref_lid_collection' in includes


class TestQueryForPendingCollections:
    """Test query_for_pending_collections constructs correct queries"""

    def test_query_includes_collection_filter(self, mock_opensearch_client):
        """Verify query filters for Product_Collection"""
        from ..mock_opensearch import create_empty_response

        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: True,
            response_data=create_empty_response()
        )

        # Execute
        list(query_for_pending_collections(mock_opensearch_client))

        # Verify
        assert len(mock_opensearch_client.search_calls) >= 1
        call = mock_opensearch_client.search_calls[0]
        query = call['body']

        # Check for collection filter
        assert "query" in query
        filters = query["query"]["bool"]["filter"]
        assert {"term": {"product_class": "Product_Collection"}} in filters

    def test_query_excludes_current_version(self, mock_opensearch_client):
        """Verify query excludes products already at current ancestry version"""
        from ..mock_opensearch import create_empty_response

        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: True,
            response_data=create_empty_response()
        )

        # Execute
        list(query_for_pending_collections(mock_opensearch_client))

        # Verify - get the actual query call (not the count call)
        actual_query_call = [c for c in mock_opensearch_client.search_calls if c['kwargs'].get('size', 0) > 0][0]
        query = actual_query_call['body']

        # Check for version exclusion
        must_not = query["query"]["bool"]["must_not"]
        assert len(must_not) >= 1

        # Find the range query for ancestry version
        version_filter = None
        for clause in must_not:
            if "range" in clause:
                version_filter = clause
                break

        assert version_filter is not None
        assert SWEEPERS_ANCESTRY_VERSION_METADATA_KEY in version_filter["range"]
        assert version_filter["range"][SWEEPERS_ANCESTRY_VERSION_METADATA_KEY]["gte"] == SWEEPERS_ANCESTRY_VERSION

    def test_source_fields_include_lidvid_and_version(self, mock_opensearch_client):
        """Verify query requests lidvid and version fields"""
        from ..mock_opensearch import create_empty_response

        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: True,
            response_data=create_empty_response()
        )

        # Execute
        list(query_for_pending_collections(mock_opensearch_client))

        # Verify _source includes expected fields
        # Get the actual query call (not the count call)
        actual_query_call = [c for c in mock_opensearch_client.search_calls if c['kwargs'].get('size', 0) > 0][0]
        includes = actual_query_call['kwargs']['_source_includes']
        assert 'lidvid' in includes
        assert SWEEPERS_ANCESTRY_VERSION_METADATA_KEY in includes

    def test_uses_registry_index(self, mock_opensearch_client):
        """Verify query targets the registry index"""
        from ..mock_opensearch import create_empty_response

        mock_opensearch_client.register_search_response(
            index_pattern=".*registry.*",
            query_matcher=lambda q: True,
            response_data=create_empty_response()
        )

        # Execute
        list(query_for_pending_collections(mock_opensearch_client))

        # Verify index name contains 'registry'
        call = mock_opensearch_client.search_calls[0]
        assert 'registry' in call['index']
