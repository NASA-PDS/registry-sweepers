# Ancestry Module Test Suite

This directory contains tests for the ancestry module, organized into a modern, maintainable structure.

## Test Organization

The test suite is organized into three main categories:

```
tests/pds/registrysweepers/ancestry/
├── unit/                          # Unit tests for individual functions/classes
├── integration/                   # Integration tests for complete workflows
├── scenarios/                     # Edge cases and error handling tests
├── builders.py                    # Test data builders
├── fixtures.py                    # Reusable pytest fixtures
├── mock_opensearch.py            # Mock OpenSearch client
├── conftest.py                   # Shared pytest configuration
├── test_ancestry.py              # Legacy functional tests (Phase 1)
├── test_ancestryrecord.py        # Legacy record tests
└── test_utils.py                 # Legacy utility tests
```

## Phase 1: Test Infrastructure (Current)

Phase 1 establishes the foundation for the new testing approach:

- ✅ **Test data builders** (`builders.py`): Programmatic test data construction
- ✅ **Pytest fixtures** (`fixtures.py`): Reusable test scenarios
- ✅ **Mock OpenSearch client** (`mock_opensearch.py`): Query validation and response mocking
- ✅ **Shared configuration** (`conftest.py`): Automatic test marking and fixtures

### Key Components

#### Test Data Builders

Builders provide a clean, fluent API for constructing test data:

```python
from .builders import ProductDocumentBuilder, CollectionRefsBuilder

# Build a collection
collection = ProductDocumentBuilder(
    lidvid="urn:nasa:pds:mission:data::1.0",
    product_class="Product_Collection"
).with_ancestry_version(6).build()

# Build collection member references
refs = CollectionRefsBuilder("urn:nasa:pds:mission:data::1.0")
    .with_product("urn:nasa:pds:mission:data:product1::1.0")
    .with_product("urn:nasa:pds:mission:data:product2::1.0")
    .build()
```

#### Pytest Fixtures

Pre-built test scenarios available in all tests:

```python
def test_something(simple_collection_hierarchy):
    # simple_collection_hierarchy provides:
    # - bundles: List of bundle documents
    # - collections: List of collection documents
    # - collection_refs: List of refs documents
    pass
```

Available fixtures:
- `simple_collection_hierarchy` - Basic Bundle → Collection → Products
- `complex_multi_version_hierarchy` - Multiple versions
- `empty_collection_hierarchy` - Collection with no members
- `large_collection_hierarchy` - 100+ products for performance testing
- `malformed_documents_scenario` - Missing fields for error testing
- `versioned_products_scenario` - Various version states
- `multi_batch_refs` - Split across multiple batch documents

#### Mock OpenSearch Client

The mock client validates queries and tracks operations:

```python
def test_query_construction(mock_opensearch_client):
    # Register expected response
    mock_opensearch_client.register_search_response(
        index_pattern="registry",
        query_matcher=lambda q: "Product_Collection" in str(q),
        response_data={'hits': {'hits': [...]}}
    )

    # Run code that queries OpenSearch
    result = some_function(mock_opensearch_client)

    # Verify queries were made correctly
    assert len(mock_opensearch_client.search_calls) == 1
    call = mock_opensearch_client.search_calls[0]
    assert call['index'] == 'registry'
```

## Running Tests

### Run all ancestry tests:
```bash
pytest tests/pds/registrysweepers/ancestry/
```

### Run by category:
```bash
pytest tests/pds/registrysweepers/ancestry/ -m unit
pytest tests/pds/registrysweepers/ancestry/ -m integration
pytest tests/pds/registrysweepers/ancestry/ -m scenario
```

### Run specific test directory:
```bash
pytest tests/pds/registrysweepers/ancestry/unit/
pytest tests/pds/registrysweepers/ancestry/integration/
pytest tests/pds/registrysweepers/ancestry/scenarios/
```

### Run legacy tests only:
```bash
pytest tests/pds/registrysweepers/ancestry/test_ancestry.py
```

## Test Markers

Tests are automatically marked based on their directory:
- `@pytest.mark.unit` - Tests in `unit/` directory
- `@pytest.mark.integration` - Tests in `integration/` directory
- `@pytest.mark.scenario` - Tests in `scenarios/` directory
- `@pytest.mark.slow` - Tests that take significant time (manual marking)

## Writing New Tests

### Unit Test Example

```python
# tests/pds/registrysweepers/ancestry/unit/test_something.py
import pytest
from pds.registrysweepers.ancestry import queries

def test_query_builder(mock_opensearch_client):
    """Test that query is constructed correctly"""
    # Arrange
    mock_opensearch_client.register_search_response(
        index_pattern="registry",
        query_matcher=lambda q: True,
        response_data={'hits': {'hits': []}}
    )

    # Act
    result = queries.query_for_pending_collections(mock_opensearch_client)
    list(result)  # Consume generator

    # Assert
    assert len(mock_opensearch_client.search_calls) == 1
    query = mock_opensearch_client.search_calls[0]['body']['query']
    assert 'Product_Collection' in str(query)
```

### Integration Test Example

```python
# tests/pds/registrysweepers/ancestry/integration/test_pipeline.py
import pytest
from pds.registrysweepers.ancestry import main

def test_full_pipeline(mock_opensearch_client, simple_collection_hierarchy):
    """Test complete ancestry processing"""
    # Setup mock responses
    # ... register responses for bundles, collections, refs ...

    # Execute
    main.run(client=mock_opensearch_client)

    # Verify
    assert len(mock_opensearch_client.bulk_calls) > 0
```

## Migration Status

This test infrastructure is part of a 6-phase migration to improve test quality:

- **Phase 1 (Current)**: ✅ New test infrastructure
- **Phase 2**: Unit tests
- **Phase 3**: Integration tests
- **Phase 4**: Edge case tests
- **Phase 5**: Production code cleanup
- **Phase 6**: Legacy test removal

Legacy tests (`test_ancestry.py`, etc.) will remain until Phase 6 to ensure no regression.

## Contributing

When adding new tests:
1. Use the builders and fixtures for test data
2. Place tests in the appropriate directory (unit/integration/scenarios)
3. Use descriptive test names and docstrings
4. Verify mocks using `mock_opensearch_client.search_calls` and `bulk_calls`
