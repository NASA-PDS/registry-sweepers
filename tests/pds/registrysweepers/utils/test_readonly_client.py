import pytest
from pds.registrysweepers.utils.db.readonly import ReadOnlyClientViolation
from pds.registrysweepers.utils.db.readonly import ReadOnlyOpenSearchClient

from ..ancestry.mock_opensearch import MockOpenSearchClient


def test_reads_pass_through():
    mock = MockOpenSearchClient()
    client = ReadOnlyOpenSearchClient(mock)

    client.search(index="registry", body={})
    client.mget(index="registry", body={"ids": ["a"]})
    client.indices.exists("registry")
    client.indices.exists_alias("registry")
    client.indices.get_mapping(index="registry")

    assert mock.call_log == ["search", "mget", "indices.exists", "indices.exists_alias", "indices.get_mapping"]


@pytest.mark.parametrize("method", ["bulk", "index", "update", "delete", "delete_by_query", "reindex", "transport"])
def test_client_writes_are_refused(method):
    mock = MockOpenSearchClient()

    with pytest.raises(ReadOnlyClientViolation):
        getattr(ReadOnlyOpenSearchClient(mock), method)

    assert mock.call_log == []


@pytest.mark.parametrize("method", ["put_mapping", "create", "delete", "refresh", "put_settings", "put_alias"])
def test_index_writes_are_refused(method):
    mock = MockOpenSearchClient()

    with pytest.raises(ReadOnlyClientViolation):
        getattr(ReadOnlyOpenSearchClient(mock).indices, method)

    assert mock.call_log == []
