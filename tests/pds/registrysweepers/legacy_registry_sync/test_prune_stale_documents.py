import copy
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

from opensearchpy.exceptions import NotFoundError

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


class FakeServerlessClient:
    """
    An in-memory legacy index which behaves like OpenSearch Serverless for the calls the prune makes: there is no
    refresh, no delete-by-query and no scroll, documents are listed with search_after and deleted by id in bulk.
    """

    def __init__(self, docs, visible_after_polls=0, failing_ids=()):
        self.docs = dict(docs)  # id -> run id (None when the document predates the run id field)
        self.visible_after_polls = visible_after_polls
        self.failing_ids = set(failing_ids)
        self.calls = []
        self.indices = MagicMock()
        self.indices.refresh.side_effect = NotFoundError(404, "")

    def delete_by_query(self, **kwargs):
        raise NotFoundError(404, "")

    def scroll(self, **kwargs):
        raise ScrollNotSupportedError("scroll is not usable on OpenSearch Serverless")

    def count(self, index, body):
        self.calls.append("count")
        run_id = body["query"]["term"][SYNC_RUN_ID_FIELD]
        matching = [d for d, r in self.docs.items() if r == run_id]
        if self.visible_after_polls > 0:
            self.visible_after_polls -= 1
            return {"count": len(matching) // 2}
        return {"count": len(matching)}

    def search(self, index, body, size, request_timeout=None):
        self.calls.append("search")
        query = body["query"]
        if "bool" in query:
            run_id = query["bool"]["must_not"][0]["term"][SYNC_RUN_ID_FIELD]
            ids = sorted(d for d, r in self.docs.items() if r != run_id)
        else:
            ids = sorted(self.docs)
        after = body.get("search_after")
        if after:
            ids = [d for d in ids if d > after[0]]
        return {"hits": {"hits": [{"_id": d, "sort": [d]} for d in ids[:size]]}}

    def bulk(self, body, request_timeout=None):
        self.calls.append("bulk")
        items = []
        for action in body:
            doc_id = action["delete"]["_id"]
            if doc_id in self.failing_ids:
                items.append({"delete": {"_id": doc_id, "status": 429, "error": {"type": "rejected"}}})
            elif doc_id in self.docs:
                del self.docs[doc_id]
                items.append({"delete": {"_id": doc_id, "status": 200, "result": "deleted"}})
            else:
                items.append({"delete": {"_id": doc_id, "status": 404, "result": "not_found"}})
        return {"errors": any(i["delete"]["status"] >= 400 and i["delete"]["status"] != 404 for i in items), "items": items}


def current_and_stale(current=100, stale=3):
    docs = {f"urn:x:current{i:03d}::1.0": RUN_ID for i in range(current)}
    docs.update({f"urn:x:stale{i:03d}::1.0": 111 for i in range(stale)})
    docs["urn:x:unstamped::1.0"] = None  # predates the run id field
    return docs


@patch("pds.registrysweepers.legacy_registry_sync.legacy_registry_sync.time.sleep", MagicMock())
class TestPruneStaleDocuments(unittest.TestCase):
    def test_deletes_only_the_documents_not_stamped_with_the_run_id(self):
        client = FakeServerlessClient(current_and_stale())

        deleted = prune_stale_documents(client, run_id=RUN_ID, docs_before=104, docs_written=100)

        self.assertEqual(4, deleted)  # three stale and the unstamped one
        self.assertEqual(100, len(client.docs))
        self.assertTrue(all(run_id == RUN_ID for run_id in client.docs.values()))

    def test_deletes_with_one_bulk_request_and_never_uses_delete_by_query(self):
        client = FakeServerlessClient(current_and_stale())  # delete_by_query and scroll raise on this client

        prune_stale_documents(client, run_id=RUN_ID, docs_before=104, docs_written=100)

        self.assertEqual(1, client.calls.count("bulk"))

    def test_deletes_in_batches_of_the_configured_size(self):
        client = FakeServerlessClient(current_and_stale(stale=7))
        with patch.object(legacy_registry_sync, "PRUNE_BATCH_SIZE", 3):
            deleted = prune_stale_documents(client, run_id=RUN_ID, docs_before=108, docs_written=100)

        self.assertEqual(8, deleted)
        self.assertEqual(3, client.calls.count("bulk"))  # 8 documents in batches of 3

    def test_lists_all_stale_ids_before_deleting_any(self):
        client = FakeServerlessClient(current_and_stale(stale=7))
        with patch.object(legacy_registry_sync, "PRUNE_BATCH_SIZE", 2):
            prune_stale_documents(client, run_id=RUN_ID, docs_before=108, docs_written=100)

        searches = [i for i, c in enumerate(client.calls) if c == "search"]
        bulks = [i for i, c in enumerate(client.calls) if c == "bulk"]
        self.assertLess(max(searches), min(bulks))

    def test_documents_already_absent_count_as_deleted_and_failures_are_reported_not_raised(self):
        client = FakeServerlessClient(current_and_stale(stale=3), failing_ids={"urn:x:stale001::1.0"})

        with self.assertLogs(legacy_registry_sync.log, level="ERROR") as logs:
            deleted = prune_stale_documents(client, run_id=RUN_ID, docs_before=104, docs_written=100)

        self.assertEqual(3, deleted)  # one of the four failed
        self.assertIn("Failed to delete 1 of 4 stale documents", logs.output[0])
        self.assertIn("urn:x:stale001::1.0", client.docs)

    def test_falls_back_to_deleting_one_by_one_when_bulk_delete_is_rejected(self):
        client = FakeServerlessClient(current_and_stale())
        client.bulk = MagicMock(side_effect=NotFoundError(404, ""))

        def delete(index, id):
            if id not in client.docs:
                raise NotFoundError(404, "")
            del client.docs[id]

        client.delete = delete

        deleted = prune_stale_documents(client, run_id=RUN_ID, docs_before=104, docs_written=100)

        self.assertEqual(4, deleted)
        self.assertEqual(100, len(client.docs))

    def test_waits_for_the_written_documents_before_listing_and_deleting(self):
        client = FakeServerlessClient(current_and_stale(), visible_after_polls=2)

        prune_stale_documents(client, run_id=RUN_ID, docs_before=104, docs_written=100)

        self.assertEqual(["count", "count", "count", "search", "bulk"], client.calls)

    def test_skips_the_prune_when_the_written_documents_never_become_searchable(self):
        client = FakeServerlessClient(current_and_stale(), visible_after_polls=10**6)
        clock = iter([0, 1, 2, 10**6])

        with patch.object(legacy_registry_sync.time, "monotonic", lambda: next(clock)):
            deleted = prune_stale_documents(client, run_id=RUN_ID, docs_before=104, docs_written=100)

        self.assertEqual(0, deleted)
        self.assertEqual(104, len(client.docs))

    def test_accepts_a_stable_count_slightly_below_the_number_written(self):
        docs = current_and_stale(current=98, stale=2)
        client = FakeServerlessClient(docs)

        deleted = prune_stale_documents(client, run_id=RUN_ID, docs_before=101, docs_written=100)  # 100 written, 98 distinct

        self.assertEqual(3, deleted)

    def test_skips_prune_when_run_wrote_too_few_documents(self):
        docs_before = 1000
        docs_written = int(docs_before * MIN_PRUNE_RATIO) - 1
        client = FakeServerlessClient(current_and_stale())

        deleted = prune_stale_documents(client, run_id=RUN_ID, docs_before=docs_before, docs_written=docs_written)

        self.assertEqual(0, deleted)
        self.assertNotIn("bulk", client.calls)

    def test_skips_prune_when_run_wrote_nothing_to_populated_index(self):
        client = FakeServerlessClient(current_and_stale())

        deleted = prune_stale_documents(client, run_id=RUN_ID, docs_before=50, docs_written=0)

        self.assertEqual(0, deleted)
        self.assertNotIn("bulk", client.calls)

    def test_prunes_at_exact_minimum_ratio(self):
        client = FakeServerlessClient(current_and_stale(current=9, stale=0))  # 9 stamped and 1 unstamped: 10 before

        deleted = prune_stale_documents(client, run_id=RUN_ID, docs_before=10, docs_written=9)

        self.assertEqual(1, deleted)  # 9 of 10 is exactly the minimum ratio, so the unstamped document is pruned

    def test_prunes_nothing_when_the_index_was_empty(self):
        client = FakeServerlessClient({})

        self.assertEqual(0, prune_stale_documents(client, run_id=RUN_ID, docs_before=0, docs_written=0))


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
