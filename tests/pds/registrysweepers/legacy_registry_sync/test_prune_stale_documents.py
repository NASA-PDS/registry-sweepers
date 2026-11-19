import copy
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

from pds.registrysweepers.legacy_registry_sync import legacy_registry_sync
from pds.registrysweepers.legacy_registry_sync.legacy_registry_sync import iterate_existing_ids
from pds.registrysweepers.legacy_registry_sync.legacy_registry_sync import MIN_PRUNE_RATIO
from pds.registrysweepers.legacy_registry_sync.legacy_registry_sync import OS_INDEX
from pds.registrysweepers.legacy_registry_sync.legacy_registry_sync import preview_sync
from pds.registrysweepers.legacy_registry_sync.legacy_registry_sync import prune_stale_documents
from pds.registrysweepers.legacy_registry_sync.solr_doc_export_to_opensearch import SolrOsWrapperIter
from pds.registrysweepers.legacy_registry_sync.solr_doc_export_to_opensearch import SYNC_RUN_ID_FIELD

RUN_ID = 1700000000000


class TestRunIdStamping(unittest.TestCase):
    SOLR_DOC = {"lid": "urn:nasa:pds:x", "lidvid": "urn:nasa:pds:x::1.0", "resource_url": ["https://pds.nasa.gov/a"]}

    def test_run_id_stamped_on_document(self):
        wrapper = SolrOsWrapperIter([self.SOLR_DOC], "test-index", found_ids={}, run_id=RUN_ID)
        os_doc = next(wrapper)
        self.assertEqual(RUN_ID, os_doc["_source"][SYNC_RUN_ID_FIELD])

    def test_no_run_id_leaves_document_unstamped(self):
        wrapper = SolrOsWrapperIter([self.SOLR_DOC], "test-index", found_ids={})
        os_doc = next(wrapper)
        self.assertNotIn(SYNC_RUN_ID_FIELD, os_doc["_source"])


class TestPruneStaleDocuments(unittest.TestCase):
    def setUp(self):
        self.client = MagicMock()
        self.client.delete_by_query.return_value = {"deleted": 7}

    def test_deletes_documents_not_stamped_with_run_id(self):
        deleted = prune_stale_documents(self.client, run_id=RUN_ID, docs_before=100, docs_written=100)

        self.assertEqual(7, deleted)
        self.client.indices.refresh.assert_called_once_with(index=OS_INDEX)
        kwargs = self.client.delete_by_query.call_args.kwargs
        self.assertEqual(OS_INDEX, kwargs["index"])
        self.assertEqual(
            {"query": {"bool": {"must_not": [{"term": {SYNC_RUN_ID_FIELD: RUN_ID}}]}}},
            kwargs["body"],
        )

    def test_prune_does_not_use_scroll(self):
        self.client.scroll.side_effect = ScrollNotSupportedError("scroll is not usable on OpenSearch Serverless")
        self.client.clear_scroll.side_effect = ScrollNotSupportedError("scroll is not usable on OpenSearch Serverless")
        self.client.search.side_effect = ScrollNotSupportedError("prune must not list documents")

        deleted = prune_stale_documents(self.client, run_id=RUN_ID, docs_before=100, docs_written=100)

        self.assertEqual(7, deleted)

    def test_refreshes_before_deleting(self):
        order = []
        self.client.indices.refresh.side_effect = lambda **_: order.append("refresh")
        self.client.delete_by_query.side_effect = lambda **_: order.append("delete") or {"deleted": 0}

        prune_stale_documents(self.client, run_id=RUN_ID, docs_before=10, docs_written=10)

        self.assertEqual(["refresh", "delete"], order)

    def test_skips_prune_when_run_wrote_too_few_documents(self):
        docs_before = 1000
        docs_written = int(docs_before * MIN_PRUNE_RATIO) - 1

        deleted = prune_stale_documents(self.client, run_id=RUN_ID, docs_before=docs_before, docs_written=docs_written)

        self.assertEqual(0, deleted)
        self.client.delete_by_query.assert_not_called()

    def test_skips_prune_when_run_wrote_nothing_to_populated_index(self):
        deleted = prune_stale_documents(self.client, run_id=RUN_ID, docs_before=50, docs_written=0)

        self.assertEqual(0, deleted)
        self.client.delete_by_query.assert_not_called()

    def test_prunes_at_exact_minimum_ratio(self):
        prune_stale_documents(self.client, run_id=RUN_ID, docs_before=1000, docs_written=int(1000 * MIN_PRUNE_RATIO))

        self.client.delete_by_query.assert_called_once()

    def test_prunes_when_index_was_empty(self):
        prune_stale_documents(self.client, run_id=RUN_ID, docs_before=0, docs_written=0)

        self.client.delete_by_query.assert_called_once()


if __name__ == "__main__":
    unittest.main()


class ScrollNotSupportedError(RuntimeError):
    """Raised by fake clients on any use of the scroll API, which OpenSearch Serverless does not support."""


class FakeIndexClient:
    """Serves search_after pages of the given ids sorted on _id, and fails on any scroll request."""

    def __init__(self, ids):
        self.ids = sorted(ids)
        self.requests = []

    def search(self, index=None, body=None, size=10, **kwargs):
        if "scroll" in kwargs:
            raise ScrollNotSupportedError("scroll is not usable on OpenSearch Serverless")
        self.requests.append({"index": index, "body": copy.deepcopy(body), "size": size})
        after = body.get("search_after")
        remaining = [i for i in self.ids if after is None or i > after[0]]
        hits = [{"_id": i, "sort": [i]} for i in remaining[:size]]
        return {"hits": {"hits": hits}}

    def scroll(self, *args, **kwargs):
        raise ScrollNotSupportedError("scroll is not usable on OpenSearch Serverless")

    clear_scroll = scroll


class TestIterateExistingIds(unittest.TestCase):
    def test_pages_until_exhausted_and_passes_last_sort_value(self):
        ids = [f"id-{i}" for i in range(7)]
        client = FakeIndexClient(ids)

        result = list(iterate_existing_ids(client, page_size=3))

        self.assertEqual(sorted(ids), result)
        self.assertEqual(3, len(client.requests))
        self.assertEqual([None, ["id-2"], ["id-5"]], [r["body"].get("search_after") for r in client.requests])
        for request in client.requests:
            self.assertEqual(OS_INDEX, request["index"])
            self.assertEqual(3, request["size"])
            self.assertEqual([{"_id": "asc"}], request["body"]["sort"])
            self.assertIs(False, request["body"]["_source"])

    def test_extra_request_when_ids_fill_the_last_page_exactly(self):
        client = FakeIndexClient(["a", "b", "c", "d"])

        result = list(iterate_existing_ids(client, page_size=2))

        self.assertEqual(["a", "b", "c", "d"], result)
        self.assertEqual(3, len(client.requests))

    def test_empty_index_yields_nothing(self):
        client = FakeIndexClient([])

        self.assertEqual([], list(iterate_existing_ids(client)))
        self.assertEqual(1, len(client.requests))

    def test_is_streaming(self):
        client = FakeIndexClient([str(i) for i in range(10)])

        iterator = iterate_existing_ids(client, page_size=4)
        next(iterator)

        self.assertEqual(1, len(client.requests))

    def test_default_page_size_is_within_serverless_limit(self):
        self.assertLessEqual(legacy_registry_sync.EXISTING_IDS_PAGE_SIZE, 10000)


class TestPreviewSync(unittest.TestCase):
    def _preview(self, existing_ids, solr_ids, index_exists=True):
        client = MagicMock()
        client.indices.exists.return_value = index_exists
        client.search.side_effect = FakeIndexClient(existing_ids).search
        client.scroll.side_effect = ScrollNotSupportedError("scroll is not usable on OpenSearch Serverless")
        client.clear_scroll.side_effect = ScrollNotSupportedError("scroll is not usable on OpenSearch Serverless")
        with patch.object(legacy_registry_sync, "build_solr_actions") as build_solr_actions:
            build_solr_actions.return_value = [{"_id": _id} for _id in solr_ids]
            stats = preview_sync(client)
        return client, stats

    def test_reports_added_and_stale_documents(self):
        _, stats = self._preview(existing_ids=["a", "b", "c"], solr_ids=["b", "c", "d"])

        self.assertEqual(3, stats["docs_before"])
        self.assertEqual(3, stats["docs_to_write"])
        self.assertEqual(1, stats["docs_to_add"])
        self.assertEqual(["a"], stats["stale_ids"])

    def test_never_modifies_opensearch(self):
        client, _ = self._preview(existing_ids=["a", "b"], solr_ids=["b"])

        client.delete_by_query.assert_not_called()
        client.indices.create.assert_not_called()
        client.indices.refresh.assert_not_called()
        client.bulk.assert_not_called()

    def test_pages_existing_ids_across_several_requests(self):
        existing_ids = [f"id-{i:05d}" for i in range(legacy_registry_sync.EXISTING_IDS_PAGE_SIZE * 2 + 1)]
        client, stats = self._preview(existing_ids=existing_ids, solr_ids=existing_ids)

        self.assertEqual(len(existing_ids), stats["docs_before"])
        self.assertEqual([], stats["stale_ids"])
        self.assertEqual(3, client.search.call_count)

    def test_reports_when_prune_guard_would_skip(self):
        _, stats = self._preview(existing_ids=[str(i) for i in range(100)], solr_ids=["0"])

        self.assertFalse(stats["prune_guard_passes"])
        self.assertEqual(99, len(stats["stale_ids"]))

    def test_missing_index_has_no_stale_documents(self):
        _, stats = self._preview(existing_ids=[], solr_ids=["a", "b"], index_exists=False)

        self.assertEqual(0, stats["docs_before"])
        self.assertEqual(2, stats["docs_to_add"])
        self.assertEqual([], stats["stale_ids"])
        self.assertTrue(stats["prune_guard_passes"])

    def test_run_with_dry_run_only_previews(self):
        client = MagicMock()
        with patch.object(legacy_registry_sync, "preview_sync") as preview, patch.object(
            legacy_registry_sync, "build_solr_actions"
        ) as build_solr_actions:
            legacy_registry_sync.run(client, dry_run=True)

        preview.assert_called_once_with(client, force=False)
        build_solr_actions.assert_not_called()
        client.indices.create.assert_not_called()


def test_the_solr_query_asks_for_every_version_not_only_the_latest():
    from unittest.mock import patch

    from pds.registrysweepers.legacy_registry_sync import legacy_registry_sync as sync

    with patch.object(sync, "SlowSolrDocs") as solr_docs, patch.object(sync, "get_online_resources", return_value={}), patch.object(
        sync, "get_already_loaded_lidvids", return_value={}
    ), patch.object(sync, "SolrOsWrapperIter"):
        sync.build_solr_actions(object(), force=True, run_id=1)  # type: ignore[arg-type]

    assert solr_docs.call_args.args[:2] == (sync.SOLR_URL, "*&qt=all")
