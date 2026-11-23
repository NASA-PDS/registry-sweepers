"""
DB-backed test for the reindexer's search_after pagination over harvest timestamps.

Regression: the reindexer paged on harvest_date_time alone, so when products sharing a
timestamp straddled a page boundary, the search_after cursor skipped the rest of the tie
group. When the outstanding set fits in one batch, the sweeper finishes without flagging
the skipped products.
"""
from functools import partial

from pds.registrysweepers.reindexer import main as reindexer_main
from pds.registrysweepers.reindexer.constants import REINDEXER_FLAG_METADATA_KEY
from pds.registrysweepers.utils.db import query_registry_db_with_search_after

from tests.support.builders import build_product

PAGE_SIZE = 2


def test_tied_harvest_times_across_page_boundaries_are_all_reindexed(registry_db, monkeypatch):
    monkeypatch.setattr(
        reindexer_main,
        "query_registry_db_with_search_after",
        partial(query_registry_db_with_search_after, page_size=PAGE_SIZE),
    )

    # Tie groups of 3 and 4 with page size 2: every page boundary but one splits a tie group.
    harvest_days = [1, 1, 1, 2, 2, 2, 2, 3]
    docs = []
    for n, day in enumerate(harvest_days):
        doc = build_product(f"urn:nasa:pds:reindex:ties:product{n}::1.0")
        doc["_source"]["ops:Harvest_Info.ops:harvest_date_time"] = [f"2024-01-0{day}T00:00:00.000Z"]  # dotted, as in production
        docs.append(doc)
    registry_db.load(docs, index="registry")
    registry_db.refresh()

    reindexer_main.run(client=registry_db.client)
    registry_db.refresh()

    unflagged = registry_db.client.search(
        index=registry_db.index_names["registry"],
        body={"query": {"bool": {"must_not": [{"exists": {"field": REINDEXER_FLAG_METADATA_KEY}}]}}},
        _source=False,
        size=len(docs),
    )["hits"]["hits"]
    assert sorted(h["_id"] for h in unflagged) == []
