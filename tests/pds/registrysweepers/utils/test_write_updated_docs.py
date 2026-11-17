import json

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
