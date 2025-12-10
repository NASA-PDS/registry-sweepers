"""Pytest configuration for ancestry module tests.

This module provides shared fixtures and configuration for all ancestry tests.
"""
import logging

import pytest

from .fixtures import *  # Import all fixtures
from .mock_opensearch import MockOpenSearchClient


# Configure logging for tests
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


@pytest.fixture
def mock_opensearch_client():
    """Provide a fresh mock OpenSearch client for each test.

    The client tracks all search and bulk operations and allows registering
    expected responses.

    Example:
        def test_something(mock_opensearch_client):
            mock_opensearch_client.register_search_response(
                index_pattern="registry",
                query_matcher=lambda q: True,
                response_data={'hits': {'hits': []}}
            )

            # Use client in test...

    Returns:
        MockOpenSearchClient instance
    """
    client = MockOpenSearchClient()
    yield client
    # Cleanup after test
    client.reset()


@pytest.fixture
def mock_opensearch_with_tenancy():
    """Provide a mock OpenSearch client configured for multi-tenancy.

    Returns:
        MockOpenSearchClient instance with tenant prefix set
    """
    client = MockOpenSearchClient()
    client.set_tenant_prefix("test_tenant_")
    yield client
    client.reset()


@pytest.fixture(autouse=True)
def reset_logging():
    """Reset logging configuration between tests.

    This prevents log pollution between tests.
    """
    yield
    # Clear handlers after each test
    for logger_name in logging.Logger.manager.loggerDict:
        logger = logging.getLogger(logger_name)
        logger.handlers.clear()
        logger.propagate = True


# Pytest configuration hooks

def pytest_configure(config):
    """Register custom markers for ancestry tests."""
    config.addinivalue_line(
        "markers",
        "unit: Unit tests for individual functions and classes"
    )
    config.addinivalue_line(
        "markers",
        "integration: Integration tests for complete workflows"
    )
    config.addinivalue_line(
        "markers",
        "scenario: Scenario tests for edge cases and error handling"
    )
    config.addinivalue_line(
        "markers",
        "slow: Tests that take significant time to run"
    )


def pytest_collection_modifyitems(config, items):
    """Automatically mark tests based on their location.

    Tests in unit/ directory get @pytest.mark.unit
    Tests in integration/ directory get @pytest.mark.integration
    Tests in scenarios/ directory get @pytest.mark.scenario
    """
    for item in items:
        # Get the relative path of the test file
        rel_path = str(item.fspath)

        if '/unit/' in rel_path:
            item.add_marker(pytest.mark.unit)
        elif '/integration/' in rel_path:
            item.add_marker(pytest.mark.integration)
        elif '/scenarios/' in rel_path:
            item.add_marker(pytest.mark.scenario)
