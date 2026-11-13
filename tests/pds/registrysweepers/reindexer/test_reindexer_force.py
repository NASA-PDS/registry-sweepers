from datetime import datetime
from datetime import timezone

from pds.registrysweepers.reindexer.constants import REINDEXER_FLAG_METADATA_KEY
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
