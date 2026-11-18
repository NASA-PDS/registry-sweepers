import json
import logging

from pds.registrysweepers.utils.db import write_updated_docs
from pds.registrysweepers.utils.db.update import Update


class _RecordingClient:
    def __init__(self):
        self.bulk_bodies = []

    def bulk(self, index, body, request_timeout=None):
        self.bulk_bodies.append(body)
        return {"errors": False, "items": []}


def _written_ids(client):
    ids = []
    for body in client.bulk_bodies:
        lines = [json.loads(line) for line in body.splitlines() if line]
        ids += [line["update"]["_id"] for line in lines if "update" in line]
    return ids


def test_a_single_update_is_written():
    client = _RecordingClient()

    write_updated_docs(client, [Update(id="urn:a:b::1.0", content={"k": "v"})], index_name="registry")

    assert _written_ids(client) == ["urn:a:b::1.0"]


def test_all_updates_are_written():
    client = _RecordingClient()
    updates = [Update(id=f"urn:a:b{i}::1.0", content={"k": "v"}) for i in range(3)]

    write_updated_docs(client, updates, index_name="registry")

    assert _written_ids(client) == [u.id for u in updates]


def test_no_updates_writes_nothing():
    client = _RecordingClient()

    write_updated_docs(client, [], index_name="registry")

    assert client.bulk_bodies == []


# ---------------------------------------------------------------------------------------------------------------------
# per-item outcomes of the bulk responses
# ---------------------------------------------------------------------------------------------------------------------


class _ScriptedClient:
    """Answers each bulk item according to the prefix of its id: ok / same / gone / blocked / throttled."""

    def __init__(self):
        self.bulk_calls = 0
        self.throttled_once = set()
        self.applied = set()

    def bulk(self, index, body, request_timeout=None):
        self.bulk_calls += 1
        lines = [json.loads(line) for line in body.splitlines() if line]
        items = []
        for action in lines[::2]:
            doc_id = action["update"]["_id"]
            if doc_id.startswith("gone"):
                error = {"type": "document_missing_exception", "reason": "document missing"}
                items.append({"update": {"_id": doc_id, "status": 404, "error": error}})
            elif doc_id.startswith("blocked"):
                error = {"type": "indexing_block_exception", "reason": "blocked by: [maximum OCU capacity reached]"}
                items.append({"update": {"_id": doc_id, "status": 429, "error": error}})
            elif doc_id.startswith("throttled") and doc_id not in self.throttled_once:
                self.throttled_once.add(doc_id)
                error = {"type": "circuit_breaking_exception", "reason": "throttled"}
                items.append({"update": {"_id": doc_id, "status": 429, "error": error}})
            elif doc_id.startswith("same") or doc_id.startswith("throttled"):
                items.append({"update": {"_id": doc_id, "status": 200, "result": "noop"}})
            else:
                # a document which was already written reports noop when the same update is submitted again
                result = "noop" if doc_id in self.applied else "updated"
                self.applied.add(doc_id)
                items.append({"update": {"_id": doc_id, "status": 200, "result": result}})
        return {"errors": any("error" in i["update"] for i in items), "items": items}


def _updates(*ids):
    return [Update(id=i, content={"k": "v"}) for i in ids]


def test_counts_are_taken_from_the_items_of_every_chunk_not_from_the_number_of_attempts(caplog):
    client = _ScriptedClient()
    updates = _updates(
        "ok1", "blocked1", "gone1", "same1",  # chunk 1
        "ok2", "ok3", "blocked2", "blocked3",  # chunk 2
        "gone2", "ok4", "blocked4",  # chunk 3 (the last, partial one)
    )

    with caplog.at_level(logging.INFO):
        summary = write_updated_docs(client, updates, index_name="registry", bulk_chunk_max_update_count=4)

    assert client.bulk_calls == 3
    assert (summary.attempted, summary.succeeded, summary.noop, summary.missing, summary.failed) == (11, 4, 1, 2, 4)
    assert summary.attempted == summary.succeeded + summary.noop + summary.missing + summary.failed
    assert summary.failed_by_type == {"indexing_block_exception": 4}
    line = next(r.getMessage() for r in caplog.records if r.getMessage().startswith("Document updates for"))
    assert "11 products" in line
    assert "4 updated" in line and "1 already up to date" in line and "2 vanished" in line
    assert "4 failed (indexing_block_exception: 4)" in line
    assert "skipped 0 doc updates" in line


def test_skipped_updates_are_not_counted_as_attempted(caplog):
    client = _ScriptedClient()
    updates = _updates("ok1", "ok2") + [Update(id="skipme", content={}, skip_write=True)]

    summary = write_updated_docs(client, updates, index_name="registry")

    assert (summary.attempted, summary.succeeded, summary.skipped) == (2, 2, 1)


def test_a_chunk_resubmitted_after_throttling_does_not_turn_written_documents_into_failures_or_noops(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda seconds: None)
    client = _ScriptedClient()

    summary = write_updated_docs(client, _updates("ok1", "ok2", "throttled1", "blocked1"), index_name="registry")

    assert client.bulk_calls == 2  # the whole chunk was submitted a second time
    # ok1 and ok2 were written by the first submission and are "noop" in the second response of the real service
    assert (summary.attempted, summary.succeeded, summary.noop, summary.failed) == (4, 2, 1, 1)
    assert summary.attempted == summary.succeeded + summary.noop + summary.missing + summary.failed
