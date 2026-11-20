"""
DB-backed tests for query_registry_db_with_search_after pagination.

Regression: search_after cursors were built from hit["_source"], so sorting on a
metadata field (_id, _doc) that never appears in _source produced a [null] cursor
after the first page. With _id, OpenSearch returned no hits and the query was
silently truncated; with _doc, it raised a null_pointer_exception and the caller
crashed after retries.
"""
import pytest
from opensearchpy.helpers import bulk
from pds.registrysweepers.utils.db import query_registry_db_with_search_after

from tests.support.builders import build_product

PAGE_SIZE = 2
DOC_COUNT = 5  # spans three pages, the last one partial


def _load_unmapped(registry_db, ids):
    index = registry_db.index_names["deferred-updates"]
    bulk(registry_db.client, [{"_index": index, "_id": i, "_source": {"payload": i}} for i in ids])
    registry_db.refresh()
    return index


@pytest.mark.parametrize("sort_field", ["_id", "_doc"])
def test_metadata_sort_field_paginates_all_hits(registry_db, sort_field):
    ids = [f"doc-{n}" for n in range(DOC_COUNT)]
    index = _load_unmapped(registry_db, ids)

    hits = list(
        query_registry_db_with_search_after(
            registry_db.client,
            index,
            {"query": {"match_all": {}}},
            {"includes": []},
            page_size=PAGE_SIZE,
            sort_fields=[sort_field],
        )
    )

    returned_ids = [h["_id"] for h in hits]
    assert sorted(returned_ids) == sorted(ids)
    assert len(returned_ids) == len(set(returned_ids)), "pages must not overlap"


def test_source_sort_field_paginates_all_hits(registry_db):
    """The default lidvid sort, whose value is also in _source, must keep working."""
    lidvids = [f"urn:nasa:pds:paging:product{n}::1.0" for n in range(DOC_COUNT)]
    registry_db.load([build_product(lidvid) for lidvid in lidvids], index="registry")
    registry_db.refresh()

    hits = list(
        query_registry_db_with_search_after(
            registry_db.client,
            registry_db.index_names["registry"],
            {"query": {"match_all": {}}},
            {"includes": ["lidvid"]},
            page_size=PAGE_SIZE,
        )
    )

    assert [h["_source"]["lidvid"] for h in hits] == sorted(lidvids)
