"""
Regression: accumulate_missing_mappings read problem docs' harvest versions from the legacy
flat-schema key "ops:Harvest_Info/ops:harvest_version". Registry documents use the dotted
key "ops:Harvest_Info.ops:harvest_version", so the lookup raised a swallowed KeyError and
the harvest-versions summary was always empty.
"""
import logging

import pytest
from pds.registrysweepers.reindexer.main import accumulate_missing_mappings

HARVEST_TIME_KEY = "ops:Harvest_Info.ops:harvest_date_time"
HARVEST_VERSION_KEY = "ops:Harvest_Info.ops:harvest_version"
UNMAPPED_FIELD = "pds:Unmapped.pds:field"  # has a DD type but no index mapping, so the doc is a "problem doc"
DD_TYPES = {UNMAPPED_FIELD: "keyword"}
MAPPINGS = {HARVEST_TIME_KEY: "date", HARVEST_VERSION_KEY: "keyword"}
HARVEST_TIME = ["2024-01-01T00:00:00.000Z"]

_ABSENT = object()


def _doc(doc_id, version=_ABSENT, problem=True):
    source = {HARVEST_TIME_KEY: HARVEST_TIME}
    if version is not _ABSENT:
        source[HARVEST_VERSION_KEY] = version
    if problem:
        source[UNMAPPED_FIELD] = "x"
    return {"_id": doc_id, "_source": source}


def _reported_versions(caplog, docs):
    with caplog.at_level(logging.WARNING):
        accumulate_missing_mappings(DD_TYPES, MAPPINGS, docs)
    [message] = [r.getMessage() for r in caplog.records if "harvest versions" in r.getMessage()]
    return message.split("harvest versions ", 1)[1]


def test_versions_of_problem_docs_are_reported(caplog):
    docs = [_doc("a", ["3.8.1"]), _doc("b", ["3.7.6"]), _doc("c", ["3.8.1"])]

    assert _reported_versions(caplog, docs) == "['3.7.6', '3.8.1']"


def test_versions_of_fully_mapped_docs_are_not_reported(caplog):
    docs = [_doc("problem", ["3.8.1"]), _doc("fine", ["4.0.0"], problem=False)]

    assert _reported_versions(caplog, docs) == "['3.8.1']"


def test_bare_string_version_is_not_split_into_characters(caplog):
    assert _reported_versions(caplog, [_doc("a", "3.8.1")]) == "['3.8.1']"


@pytest.mark.parametrize("version", [_ABSENT, None, []], ids=["absent", "null", "empty-list"])
def test_missing_version_is_skipped(caplog, version):
    docs = [_doc("missing", version), _doc("present", ["3.8.1"])]

    assert _reported_versions(caplog, docs) == "['3.8.1']"
