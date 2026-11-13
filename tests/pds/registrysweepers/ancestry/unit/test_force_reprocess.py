"""Unit tests for the force (reprocess everything) behaviour of the ancestry sweeper"""
from pds.registrysweepers.ancestry import generation
from pds.registrysweepers.ancestry.generation import bundle_update_records_from_docs
from pds.registrysweepers.ancestry.queries import query_for_collection_nonaggregate_refs
from pds.registrysweepers.ancestry.queries import query_for_pending_collections
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY
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
