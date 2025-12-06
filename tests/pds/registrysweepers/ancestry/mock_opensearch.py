"""Mock OpenSearch client for testing ancestry module.

This module provides a mock implementation of the OpenSearch client that:
1. Validates queries against expected patterns
2. Returns configured test responses
3. Tracks all search and bulk calls for verification in tests
"""
import logging
import re
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

logger = logging.getLogger(__name__)


class MockOpenSearchClient:
    """Mock OpenSearch client for testing.

    Validates queries against expected patterns and returns configured responses.
    Tracks all search and bulk calls for verification.

    Example:
        client = MockOpenSearchClient()
        client.register_search_response(
            index_pattern="registry",
            query_matcher=lambda q: "Product_Collection" in str(q),
            response_data={'hits': {'hits': [...]}}
        )

        # Use in tests
        results = client.search(index="registry", body={"query": ...})
    """

    def __init__(self):
        """Initialize the mock client."""
        self.search_calls: List[Dict] = []
        self.bulk_calls: List[Dict] = []
        self._search_responses: List[Tuple[str, Callable, Dict]] = []
        self._default_search_response = {'hits': {'hits': [], 'total': {'value': 0, 'relation': 'eq'}}}
        self._tenant_prefix: Optional[str] = None
        self.indices = MockIndicesClient()

    def register_search_response(
        self,
        index_pattern: str,
        query_matcher: Callable[[Dict], bool],
        response_data: Dict
    ) -> None:
        """Register a response for matching search queries.

        Responses are matched in order of registration. The first matching
        response will be returned.

        Args:
            index_pattern: Regex pattern to match index name (e.g., "registry.*")
            query_matcher: Function that returns True if query body matches
            response_data: Response dict to return for matching queries
        """
        self._search_responses.append((index_pattern, query_matcher, response_data))

    def search(self, index: str, body: Dict, **kwargs) -> Dict:
        """Mock OpenSearch search operation.

        Args:
            index: Index name to search
            body: Query body dict
            **kwargs: Additional search parameters (e.g., size, _source)

        Returns:
            Response dict matching OpenSearch format

        Raises:
            ValueError: If no registered response matches the query
        """
        call_record = {
            'index': index,
            'body': body,
            'kwargs': kwargs
        }
        self.search_calls.append(call_record)

        logger.debug(f"Mock search called: index={index}, query={body.get('query', {})}")

        # Find matching response
        for idx_pattern, matcher, response in self._search_responses:
            if re.match(idx_pattern, index):
                try:
                    if matcher(body):
                        logger.debug(f"Matched response for pattern: {idx_pattern}")
                        return response
                except Exception as e:
                    logger.warning(f"Query matcher raised exception: {e}")
                    continue

        # Return default empty response if no match
        logger.debug("No matching response found, returning default empty result")
        return self._default_search_response

    def bulk(self, body: str, **kwargs) -> Dict:
        """Mock OpenSearch bulk operation.

        Args:
            body: NDJSON formatted bulk request body
            **kwargs: Additional bulk parameters

        Returns:
            Success response dict
        """
        self.bulk_calls.append({
            'body': body,
            'kwargs': kwargs
        })

        logger.debug(f"Mock bulk called with {len(body.splitlines())} lines")

        # Return success response
        return {
            'errors': False,
            'items': [],
            'took': 1
        }


    def reset(self) -> None:
        """Clear all recorded calls and responses.

        Useful for test cleanup or resetting state between test phases.
        """
        self.search_calls.clear()
        self.bulk_calls.clear()
        self._search_responses.clear()
        logger.debug("Mock client reset")

    def set_tenant_prefix(self, prefix: str) -> None:
        """Set the tenant prefix for multi-tenant index resolution.

        Args:
            prefix: Tenant prefix (e.g., "tenant1_")
        """
        self._tenant_prefix = prefix

    def get_search_call_count(self) -> int:
        """Get the number of search calls made.

        Returns:
            Number of search calls
        """
        return len(self.search_calls)

    def get_bulk_call_count(self) -> int:
        """Get the number of bulk calls made.

        Returns:
            Number of bulk calls
        """
        return len(self.bulk_calls)

    def get_search_calls_for_index(self, index_pattern: str) -> List[Dict]:
        """Get all search calls matching an index pattern.

        Args:
            index_pattern: Regex pattern to match index names

        Returns:
            List of matching search call records
        """
        return [
            call for call in self.search_calls
            if re.match(index_pattern, call['index'])
        ]


class MockIndicesClient:
    """Mock for OpenSearch indices operations."""

    def __init__(self):
        """Initialize the mock indices client."""
        self.put_mapping_calls: List[Dict] = []
        self.exists_calls: List[str] = []

    def put_mapping(self, index: str, body: Dict, **kwargs) -> Dict:
        """Mock put_mapping operation.

        Args:
            index: Index name
            body: Mapping definition
            **kwargs: Additional parameters

        Returns:
            Success response dict
        """
        self.put_mapping_calls.append({
            'index': index,
            'body': body,
            'kwargs': kwargs
        })
        logger.debug(f"Mock put_mapping called: index={index}")
        return {'acknowledged': True}

    def exists(self, index: str) -> bool:
        """Mock index existence check.

        Args:
            index: Index name to check

        Returns:
            Always returns True (index exists)
        """
        self.exists_calls.append(index)
        logger.debug(f"Mock exists called: index={index}")
        return True

    def exists_alias(self, name: str) -> bool:
        """Mock alias existence check.

        Args:
            name: Alias name to check

        Returns:
            Always returns False (no aliases in mock)
        """
        logger.debug(f"Mock exists_alias called: name={name}")
        return False


def create_search_response(hits: List[Dict], total: Optional[int] = None) -> Dict:
    """Helper to create a properly formatted OpenSearch search response.

    Args:
        hits: List of hit documents (should have _source and _id fields)
        total: Optional total hit count (defaults to len(hits))

    Returns:
        Dict in OpenSearch response format
    """
    if total is None:
        total = len(hits)

    return {
        'hits': {
            'total': {'value': total, 'relation': 'eq'},
            'hits': hits
        }
    }


def create_empty_response() -> Dict:
    """Helper to create an empty OpenSearch search response.

    Returns:
        Dict representing no results
    """
    return create_search_response([])


def query_matches_term(query_body: Dict, field: str, value: str) -> bool:
    """Helper to check if a query contains a specific term match.

    Args:
        query_body: The query body dict
        field: Field name to check
        value: Expected value

    Returns:
        True if query contains term match for field=value
    """
    try:
        query = query_body.get('query', {})
        bool_query = query.get('bool', {})
        must_clauses = bool_query.get('must', [])
        filter_clauses = bool_query.get('filter', [])

        # Check both must and filter clauses
        for clause in must_clauses + filter_clauses:
            if 'term' in clause:
                term = clause['term']
                if field in term and term[field] == value:
                    return True
        return False
    except (KeyError, TypeError, AttributeError):
        return False


def query_matches_product_class(query_body: Dict, product_class: str) -> bool:
    """Helper to check if a query is filtering by product_class.

    Args:
        query_body: The query body dict
        product_class: Expected product class value

    Returns:
        True if query filters by this product class
    """
    return query_matches_term(query_body, 'product_class', product_class)
