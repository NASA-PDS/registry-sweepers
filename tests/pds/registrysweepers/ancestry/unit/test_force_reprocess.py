"""Unit tests for the force (reprocess everything) behaviour of the ancestry sweeper"""
import json

import pytest
from pds.registrysweepers.ancestry import generation
from pds.registrysweepers.ancestry import main as ancestry_main
from pds.registrysweepers.ancestry.constants import ANCESTRY_DEDUPLICATION_SCRIPT_FORCED_MINIFIED
from pds.registrysweepers.ancestry.constants import ANCESTRY_DEDUPLICATION_SCRIPT_MINIFIED
from pds.registrysweepers.ancestry.constants import ANCESTRY_REFS_METADATA_KEY
from pds.registrysweepers.ancestry.generation import bundle_update_records_from_docs
from pds.registrysweepers.ancestry.main import convert_records_to_updates
from pds.registrysweepers.ancestry.productupdaterecord import ProductUpdateRecord
from pds.registrysweepers.ancestry.queries import query_for_collection_nonaggregate_refs
from pds.registrysweepers.ancestry.queries import query_for_pending_collections
from pds.registrysweepers.ancestry.utils import update_from_record
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY
from pds.registrysweepers.utils.db import update_as_statements
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid

from ..mock_opensearch import create_empty_response

BUNDLE_LIDVID = "urn:nasa:pds:bundle::1.0"
COLLECTION_LIDVID = "urn:nasa:pds:bundle:collection::1.0"


def _up_to_date_bundle_doc():
    return {
        "_id": BUNDLE_LIDVID,
        "_source": {
            "lidvid": BUNDLE_LIDVID,
            "ref_lid_collection": [COLLECTION_LIDVID],
            SWEEPERS_ANCESTRY_VERSION_METADATA_KEY: SWEEPERS_ANCESTRY_VERSION,
        },
    }


def _data_query_call(mock_opensearch_client):
    # the first search call is the hits count, the call with a non-zero size is the real query
    return [c for c in mock_opensearch_client.search_calls if c["kwargs"].get("size", 0) > 0][0]


def _register_empty_response(mock_opensearch_client):
    mock_opensearch_client.register_search_response(
        index_pattern=".*registry.*", query_matcher=lambda q: True, response_data=create_empty_response()
    )


def _has_version_range_exclusion(query):
    must_not = query["query"]["bool"].get("must_not", [])
    return any(SWEEPERS_ANCESTRY_VERSION_METADATA_KEY in clause.get("range", {}) for clause in must_not)


class TestPendingCollectionsQueryForce:
    def test_version_exclusion_is_present_by_default(self, mock_opensearch_client):
        _register_empty_response(mock_opensearch_client)

        list(query_for_pending_collections(mock_opensearch_client))

        assert _has_version_range_exclusion(_data_query_call(mock_opensearch_client)["body"])

    def test_version_exclusion_is_omitted_when_forced(self, mock_opensearch_client):
        _register_empty_response(mock_opensearch_client)

        list(query_for_pending_collections(mock_opensearch_client, force=True))

        query = _data_query_call(mock_opensearch_client)["body"]
        assert not _has_version_range_exclusion(query)
        # still restricted to collections
        assert {"term": {"product_class": "Product_Collection"}} in query["query"]["bool"]["filter"]


class TestCollectionNonaggregateRefsQueryForce:
    def test_version_exclusion_is_present_by_default(self, mock_opensearch_client):
        _register_empty_response(mock_opensearch_client)

        list(query_for_collection_nonaggregate_refs(mock_opensearch_client, PdsLidVid.from_string(COLLECTION_LIDVID)))

        assert _has_version_range_exclusion(_data_query_call(mock_opensearch_client)["body"])

    def test_version_exclusion_is_omitted_when_forced(self, mock_opensearch_client):
        _register_empty_response(mock_opensearch_client)

        list(
            query_for_collection_nonaggregate_refs(
                mock_opensearch_client, PdsLidVid.from_string(COLLECTION_LIDVID), force=True
            )
        )

        query = _data_query_call(mock_opensearch_client)["body"]
        assert not _has_version_range_exclusion(query)
        # still restricted to the requested collection
        assert {"term": {"collection_lidvid": COLLECTION_LIDVID}} in query["query"]["bool"]["filter"]


class TestBundleUpdateRecordsForce:
    def test_up_to_date_bundle_is_skippable_by_default(self):
        records = list(bundle_update_records_from_docs([_up_to_date_bundle_doc()]))

        assert [r.skippable for r in records] == [True]

    def test_up_to_date_bundle_is_not_skippable_when_forced(self):
        records = list(bundle_update_records_from_docs([_up_to_date_bundle_doc()], force=True))

        assert [r.skippable for r in records] == [False]


class TestGenerationForce:
    def test_bundle_ancestry_processing_forwards_force(self, monkeypatch):
        collection_query_force_values = []

        def fake_collections_query(client, force=False):
            collection_query_force_values.append(force)
            return [{"_id": COLLECTION_LIDVID, "_source": {"lidvid": COLLECTION_LIDVID}}]

        monkeypatch.setattr(generation, "query_for_pending_bundles", lambda client: [_up_to_date_bundle_doc()])
        monkeypatch.setattr(generation, "query_for_pending_collections", fake_collections_query)

        unforced_records = list(generation.process_collection_bundle_ancestry(None))
        forced_records = list(generation.process_collection_bundle_ancestry(None, force=True))

        assert collection_query_force_values == [False, True]
        # the up-to-date bundle is only skippable when not forced
        assert any(r.skippable for r in unforced_records)
        assert not any(r.skippable for r in forced_records)
        # bundle ancestry is still generated for the collection
        collection_record = next(r for r in forced_records if str(r.product) == COLLECTION_LIDVID)
        assert {str(ref) for ref in collection_record.direct_ancestor_refs} == {BUNDLE_LIDVID, "urn:nasa:pds:bundle"}

    def test_nonaggregate_ancestry_processing_forwards_force(self, monkeypatch):
        collection_query_force_values = []
        refs_query_force_values = []
        member_lidvid = PdsLidVid.from_string("urn:nasa:pds:bundle:collection:product::1.0")

        def fake_collections_query(client, force=False):
            collection_query_force_values.append(force)
            return [{"_id": COLLECTION_LIDVID, "_source": {"lidvid": COLLECTION_LIDVID}}]

        def fake_refs_query(client, collection_lidvid, force=False):
            refs_query_force_values.append(force)
            return [member_lidvid]

        monkeypatch.setattr(generation, "query_for_pending_collections", fake_collections_query)
        monkeypatch.setattr(generation, "query_for_collection_nonaggregate_refs", fake_refs_query)

        records = list(generation.process_collection_ancestries_for_nonaggregates(None, force=True))

        assert collection_query_force_values == [True]
        assert refs_query_force_values == [True]
        # one record for the member product, one marking the collection itself as processed
        assert [str(r.product) for r in records] == [str(member_lidvid), COLLECTION_LIDVID]


class TestForcedUpdateScript:
    product = PdsLidVid.from_string("urn:nasa:pds:bundle:collection:product::1.0")

    def _record(self):
        return ProductUpdateRecord(self.product, direct_ancestor_refs=[PdsLidVid.from_string(COLLECTION_LIDVID)])

    def test_default_update_script_skips_the_write_when_nothing_changed(self):
        update = update_from_record(self._record())

        assert update.inline_script_content == ANCESTRY_DEDUPLICATION_SCRIPT_MINIFIED
        assert "ctx.op='none'" in update.inline_script_content
        assert not update.force_rewrite

    def test_forced_update_script_does_not_skip_the_write(self):
        update = update_from_record(self._record(), force=True)

        assert update.inline_script_content == ANCESTRY_DEDUPLICATION_SCRIPT_FORCED_MINIFIED
        assert "ctx.op" not in update.inline_script_content
        assert update.force_rewrite

    def test_forced_script_differs_from_default_only_by_the_noop_guard(self):
        assert ANCESTRY_DEDUPLICATION_SCRIPT_MINIFIED == (
            ANCESTRY_DEDUPLICATION_SCRIPT_FORCED_MINIFIED + "if(!c){ctx.op='none';}"
        )

    def test_forced_update_writes_the_same_content_as_the_default_update(self):
        assert update_from_record(self._record(), force=True).content == update_from_record(self._record()).content

    def test_forced_update_statement_is_a_script_without_detect_noop(self):
        update = update_from_record(self._record(), force=True)
        _, content_str = update_as_statements(update)

        statement = json.loads(content_str)
        assert statement["script"]["source"] == ANCESTRY_DEDUPLICATION_SCRIPT_FORCED_MINIFIED
        assert statement["script"]["params"] == {"new_items": update.content[ANCESTRY_REFS_METADATA_KEY]}
        assert COLLECTION_LIDVID in statement["script"]["params"]["new_items"]
        assert "detect_noop" not in statement

    def test_default_update_statement_is_unchanged(self):
        update = update_from_record(self._record())
        _, content_str = update_as_statements(update)

        assert json.loads(content_str) == {
            "script": {
                "source": ANCESTRY_DEDUPLICATION_SCRIPT_MINIFIED,
                "lang": "painless",
                "params": {"new_items": update.content[ANCESTRY_REFS_METADATA_KEY]},
            }
        }

    def test_convert_records_to_updates_forwards_force(self):
        unforced_updates = list(convert_records_to_updates([self._record()]))
        forced_updates = list(convert_records_to_updates([self._record()], force=True))

        assert [u.inline_script_content for u in unforced_updates] == [ANCESTRY_DEDUPLICATION_SCRIPT_MINIFIED]
        assert [u.inline_script_content for u in forced_updates] == [ANCESTRY_DEDUPLICATION_SCRIPT_FORCED_MINIFIED]


@pytest.mark.parametrize("force", [False, True])
def test_run_forwards_force_to_generation_and_writes_matching_updates(monkeypatch, force):
    generation_force_values = []
    written_updates = []

    def fake_bundle_ancestry(client, force=False):
        generation_force_values.append(force)
        return [ProductUpdateRecord(PdsLidVid.from_string(COLLECTION_LIDVID))]

    def fake_nonaggregate_ancestry(client, force=False):
        generation_force_values.append(force)
        return []

    def fake_write_updated_docs(client, updates, **kwargs):
        written_updates.extend(updates)

    monkeypatch.setattr(ancestry_main, "configure_logging", lambda **kwargs: None)
    monkeypatch.setattr(ancestry_main, "process_collection_bundle_ancestry", fake_bundle_ancestry)
    monkeypatch.setattr(ancestry_main, "process_collection_ancestries_for_nonaggregates", fake_nonaggregate_ancestry)
    monkeypatch.setattr(ancestry_main, "ensure_index_mapping", lambda *args, **kwargs: None)
    monkeypatch.setattr(ancestry_main, "resolve_multitenant_index_name", lambda client, label: label)
    monkeypatch.setattr(ancestry_main, "write_updated_docs", fake_write_updated_docs)

    ancestry_main.run(client=object(), force=force)

    assert generation_force_values == [force, force]
    assert [u.force_rewrite for u in written_updates] == [force]
    expected_script = ANCESTRY_DEDUPLICATION_SCRIPT_FORCED_MINIFIED if force else ANCESTRY_DEDUPLICATION_SCRIPT_MINIFIED
    assert [u.inline_script_content for u in written_updates] == [expected_script]
