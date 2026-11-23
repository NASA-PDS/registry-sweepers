"""
Regression: accumulate_missing_mappings crashed on any problem doc (one with a missing
mapping) whose harvest timestamp it could not read. It looked the timestamp up as a nested
ops:Harvest_Info object, which production documents never have (they use the dotted
top-level key), and took element [0] of the resulting None: an uncaught TypeError. A bare
string was parsed from its first character, which failed; if no problem doc had a parseable
time, the summary log crashed on None.isoformat().
"""
import logging

import pytest
from pds.registrysweepers.reindexer.main import accumulate_missing_mappings

HARVEST_TIME_KEY = "ops:Harvest_Info.ops:harvest_date_time"
UNMAPPED_FIELD = "pds:Unmapped.pds:field"  # has a DD type but no index mapping, so the doc is a "problem doc"
DD_TYPES = {UNMAPPED_FIELD: "keyword"}
MAPPINGS = {HARVEST_TIME_KEY: "date"}

_ABSENT = object()


def _doc(doc_id, harvest_time=_ABSENT):
    source = {UNMAPPED_FIELD: "x"}
    if harvest_time is not _ABSENT:
        source[HARVEST_TIME_KEY] = harvest_time
    return {"_id": doc_id, "_source": source}


def _range_log(caplog):
    [message] = [r.getMessage() for r in caplog.records if "harvest timestamps" in r.getMessage()]
    return message


@pytest.mark.parametrize(
    "doc",
    [
        _doc("absent"),
        _doc("null", None),
        _doc("empty-list", []),
        _doc("unparseable", ["not a date"]),
        _doc("not-a-string", [20240101]),
        # the nested shape production never uses is not read as a timestamp
        {"_id": "nested", "_source": {UNMAPPED_FIELD: "x", "ops:Harvest_Info": {"ops:harvest_date_time": ["2024-01-01T00:00:00Z"]}}},
    ],
    ids=lambda d: d["_id"],
)
def test_unusable_harvest_time_does_not_crash(caplog, doc):
    with caplog.at_level(logging.WARNING):
        missing = accumulate_missing_mappings(DD_TYPES, MAPPINGS, [doc])

    assert missing == {UNMAPPED_FIELD: "keyword"}
    assert f"from document {doc['_id']}" in caplog.text
    assert "unknown" in _range_log(caplog)


@pytest.mark.parametrize("harvest_time", [["2024-01-02T03:04:05.000Z"], "2024-01-02T03:04:05.000Z"], ids=["list", "bare-string"])
def test_harvest_time_is_parsed(caplog, harvest_time):
    with caplog.at_level(logging.WARNING):
        accumulate_missing_mappings(DD_TYPES, MAPPINGS, [_doc("d", harvest_time)])

    assert "between 2024-01-02T03:04:05+00:00 and 2024-01-02T03:04:05+00:00" in _range_log(caplog)


def test_range_covers_parseable_docs_and_skips_unparseable_ones(caplog):
    docs = [
        _doc("late", ["2024-03-01T00:00:00.000Z"]),
        _doc("missing"),
        _doc("early", ["2024-01-01T00:00:00.000Z"]),
        _doc("bad", ["garbage"]),
    ]

    with caplog.at_level(logging.WARNING):
        accumulate_missing_mappings(DD_TYPES, MAPPINGS, docs)

    assert "between 2024-01-01T00:00:00+00:00 and 2024-03-01T00:00:00+00:00" in _range_log(caplog)
