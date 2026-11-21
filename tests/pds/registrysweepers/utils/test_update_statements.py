import json
import unittest

from pds.registrysweepers.utils.db import update_as_statements
from pds.registrysweepers.utils.db import write_updated_docs
from pds.registrysweepers.utils.db.update import Update


def _content_statement(update: Update, **kwargs) -> dict:
    metadata_str, content_str = update_as_statements(update, **kwargs)
    return json.loads(content_str)


class _FakeBulkClient:
    """Minimal stand-in for an OpenSearch client which records bulk request bodies"""

    def __init__(self):
        self.bulk_bodies = []

    def bulk(self, index, body, **kwargs):
        self.bulk_bodies.append(body)
        return {"errors": False, "items": []}


class UpdateStatementsDetectNoopTestCase(unittest.TestCase):
    content = {"ops:Provenance/ops:superseded_by": "urn:nasa:pds:bundle::2.0"}

    def test_force_rewrite_is_false_by_default(self):
        self.assertFalse(Update(id="urn:nasa:pds:bundle::1.0", content=self.content).force_rewrite)

    def test_detect_noop_is_absent_by_default(self):
        statement = _content_statement(Update(id="urn:nasa:pds:bundle::1.0", content=self.content))

        self.assertDictEqual({"doc": self.content, "doc_as_upsert": False}, statement)

    def test_detect_noop_is_absent_by_default_with_upsert(self):
        statement = _content_statement(Update(id="urn:nasa:pds:bundle::1.0", content=self.content), as_upsert=True)

        self.assertDictEqual({"doc": self.content, "doc_as_upsert": True}, statement)

    def test_detect_noop_is_false_when_forced(self):
        statement = _content_statement(Update(id="urn:nasa:pds:bundle::1.0", content=self.content, force_rewrite=True))

        self.assertDictEqual({"doc": self.content, "doc_as_upsert": False, "detect_noop": False}, statement)

    def test_detect_noop_is_false_when_forced_with_upsert(self):
        statement = _content_statement(
            Update(id="urn:nasa:pds:bundle::1.0", content=self.content, force_rewrite=True), as_upsert=True
        )

        self.assertDictEqual({"doc": self.content, "doc_as_upsert": True, "detect_noop": False}, statement)

    def test_force_does_not_alter_the_metadata_statement(self):
        unforced_metadata = update_as_statements(Update(id="urn:nasa:pds:bundle::1.0", content=self.content))[0]
        forced_metadata = update_as_statements(
            Update(id="urn:nasa:pds:bundle::1.0", content=self.content, force_rewrite=True)
        )[0]

        self.assertEqual(unforced_metadata, forced_metadata)

    def test_script_updates_do_not_carry_detect_noop(self):
        update = Update(
            id="urn:nasa:pds:bundle::1.0",
            inline_script_content="ctx.op='none';",
            inline_script_new_items=["urn:nasa:pds:bundle::1.0"],
            force_rewrite=True,
        )

        for as_upsert in (False, True):
            with self.subTest(as_upsert=as_upsert):
                statement = _content_statement(update, as_upsert=as_upsert)

                self.assertNotIn("detect_noop", statement)
                self.assertEqual("ctx.op='none';", statement["script"]["source"])
                self.assertEqual(["urn:nasa:pds:bundle::1.0"], statement["script"]["params"]["new_items"])

    def test_write_updated_docs_sends_detect_noop_false_only_for_forced_updates(self):
        client = _FakeBulkClient()
        updates = [
            Update(id="urn:nasa:pds:bundle::1.0", content=self.content),
            Update(id="urn:nasa:pds:bundle::2.0", content=self.content, force_rewrite=True),
        ]

        write_updated_docs(client, updates, index_name="registry")

        # one bulk request, whose lines alternate between metadata and content statements
        statements = [json.loads(line) for line in client.bulk_bodies[0].splitlines()]
        self.assertNotIn("detect_noop", statements[1])
        self.assertIs(False, statements[3]["detect_noop"])


if __name__ == "__main__":
    unittest.main()
