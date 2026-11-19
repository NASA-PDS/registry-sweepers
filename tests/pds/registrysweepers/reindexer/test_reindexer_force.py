from datetime import datetime
from datetime import timezone

import pytest
from pds.registrysweepers.reindexer import main as reindexer
from pds.registrysweepers.reindexer.constants import REINDEXER_FLAG_METADATA_KEY
from pds.registrysweepers.reindexer.main import generate_updates
from pds.registrysweepers.reindexer.main import get_docs_query

SWEEPER_START = datetime(2026, 11, 13, 12, 30, 15)


def test_default_query_excludes_any_previously_reindexed_document():
    query = get_docs_query(SWEEPER_START)

    assert query["query"]["bool"]["must_not"] == [{"exists": {"field": REINDEXER_FLAG_METADATA_KEY}}]


def test_forced_query_only_excludes_documents_reindexed_by_the_current_run():
    query = get_docs_query(SWEEPER_START, force=True)

    # documents stamped by a previous run have an earlier flag value, so they are included
    assert query["query"]["bool"]["must_not"] == [
        {"range": {REINDEXER_FLAG_METADATA_KEY: {"gte": SWEEPER_START.isoformat()}}}
    ]


def test_forced_query_still_excludes_documents_harvested_during_the_run():
    default_range = get_docs_query(SWEEPER_START)["query"]["bool"]["must"]
    forced_range = get_docs_query(SWEEPER_START, force=True)["query"]["bool"]["must"]

    assert forced_range == default_range
    assert default_range["range"]["ops:Harvest_Info/ops:harvest_date_time"]["lt"] == (
        SWEEPER_START.astimezone(timezone.utc).isoformat()
    )


def test_updates_force_rewrite_only_when_forced():
    docs = [{"_id": "urn:nasa:pds:bundle::1.0", "_source": {"lidvid": "urn:nasa:pds:bundle::1.0"}}]

    unforced_updates = list(generate_updates(SWEEPER_START, ["lidvid"], docs))
    forced_updates = list(generate_updates(SWEEPER_START, ["lidvid"], docs, force=True))

    assert [u.force_rewrite for u in unforced_updates] == [False]
    assert [u.force_rewrite for u in forced_updates] == [True]
    assert forced_updates[0].content == {REINDEXER_FLAG_METADATA_KEY: SWEEPER_START.isoformat()}


@pytest.mark.parametrize("force", [False, True])
def test_run_writes_updates_with_force_rewrite_matching_force(monkeypatch, force):
    written_updates = []

    def fake_write_updated_docs(client, updates, **kwargs):
        written_updates.extend(updates)

    docs = [{"_id": "urn:nasa:pds:bundle::1.0", "_source": {"lidvid": "urn:nasa:pds:bundle::1.0"}}]
    monkeypatch.setattr(reindexer, "configure_logging", lambda **kwargs: None)
    monkeypatch.setattr(reindexer, "resolve_multitenant_index_name", lambda client, label: "registry")
    monkeypatch.setattr(reindexer, "ensure_index_mapping", lambda *args, **kwargs: None)
    monkeypatch.setattr(reindexer, "fetch_dd_field_types", lambda client: {})
    monkeypatch.setattr(reindexer, "get_query_hits_count", lambda *args, **kwargs: 0)
    monkeypatch.setattr(reindexer, "get_mapping_field_types_by_field_name", lambda client, index: {"lidvid": "keyword"})
    monkeypatch.setattr(reindexer, "accumulate_missing_mappings", lambda *args, **kwargs: {})
    monkeypatch.setattr(reindexer, "query_registry_db_with_search_after", lambda *args, **kwargs: iter(docs))
    monkeypatch.setattr(reindexer, "write_updated_docs", fake_write_updated_docs)

    reindexer.run(client=object(), force=force)

    assert [u.force_rewrite for u in written_updates] == [force]
