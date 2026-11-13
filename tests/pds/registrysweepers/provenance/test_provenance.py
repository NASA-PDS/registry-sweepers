import copy
import itertools
import json
import os.path
import unittest
from unittest import mock

from pds.registrysweepers import provenance
from pds.registrysweepers.provenance import METADATA_SUCCESSOR_KEY
from pds.registrysweepers.provenance import ProvenanceRecord
from pds.registrysweepers.provenance import SWEEPERS_PROVENANCE_VERSION
from pds.registrysweepers.provenance import SWEEPERS_PROVENANCE_VERSION_METADATA_KEY
from pds.registrysweepers.provenance.versioning import SWEEPERS_BROKEN_PROVENANCE_VERSION_METADATA_KEY
from pds.registrysweepers.utils.db import Update


class ProvenanceBasicFunctionalTestCase(unittest.TestCase):
    input_file_path = os.path.abspath(
        "./tests/pds/registrysweepers/ancestry/resources/test_provenance_mock_ProvenanceBasicFunctionalTestCase.json"
    )

    extant_lidvids = [
        "urn:nasa:pds:bundle::1.0",
        "urn:nasa:pds:bundle::1.1",
        "urn:nasa:pds:bundle::2.0",
        "urn:nasa:pds:bundle:collection::10.0",
        "urn:nasa:pds:bundle:collection::10.1",
        "urn:nasa:pds:bundle:collection::20.0",
        "urn:nasa:pds:bundle:collection:product::100.0",
        "urn:nasa:pds:bundle:collection:product::100.1",
        "urn:nasa:pds:bundle:collection:product::200.0",
    ]

    def test_correct_provenance_produced(self):
        expected_provenance = {
            "urn:nasa:pds:bundle::1.0": "urn:nasa:pds:bundle::1.1",
            "urn:nasa:pds:bundle::1.1": "urn:nasa:pds:bundle::2.0",
            "urn:nasa:pds:bundle::2.0": None,
            "urn:nasa:pds:bundle:collection::10.0": "urn:nasa:pds:bundle:collection::10.1",
            "urn:nasa:pds:bundle:collection::10.1": "urn:nasa:pds:bundle:collection::20.0",
            "urn:nasa:pds:bundle:collection::20.0": None,
            "urn:nasa:pds:bundle:collection:product::100.0": "urn:nasa:pds:bundle:collection:product::100.1",
            "urn:nasa:pds:bundle:collection:product::100.1": "urn:nasa:pds:bundle:collection:product::200.0",
            "urn:nasa:pds:bundle:collection:product::200.0": None,
        }

        def crude_update_hash(update: Update) -> str:
            d = {"id": update.id, "content": update.content}
            return json.dumps(d, sort_keys=True)

        records = [ProvenanceRecord.from_source({"lidvid": lidvid}) for lidvid in self.extant_lidvids]
        record_chains = provenance.group_and_link_records_into_chains(records)

        updates = provenance.generate_updates(itertools.chain(*record_chains))
        expected_updates = [
            Update(
                id=k,
                content={
                    "ops:Provenance/ops:superseded_by": v,
                    SWEEPERS_PROVENANCE_VERSION_METADATA_KEY: SWEEPERS_PROVENANCE_VERSION,
                    SWEEPERS_BROKEN_PROVENANCE_VERSION_METADATA_KEY: None,
                },
            )
            for k, v in expected_provenance.items()
        ]

        self.assertSetEqual(
            set(crude_update_hash(u) for u in expected_updates), set(crude_update_hash(u) for u in updates)
        )


class _FakeSearchClient:
    """Minimal stand-in for an OpenSearch client which serves canned search responses in order and records requests"""

    def __init__(self, responses):
        self.responses = list(responses)
        self.request_bodies = []

    def search(self, index, body, **kwargs):
        self.request_bodies.append(copy.deepcopy(body))
        return self.responses[len(self.request_bodies) - 1]


def _composite_response(lids, after_key=None):
    aggregation = {"buckets": [{"key": {"lid": lid}, "doc_count": 1} for lid in lids]}
    if after_key is not None:
        aggregation["after_key"] = after_key
    return {"hits": {"total": {"value": 3}}, "aggregations": {"unique_lids": aggregation}}


class ProvenanceForceTestCase(unittest.TestCase):
    up_to_date_source = {
        "lidvid": "urn:nasa:pds:bundle::1.0",
        METADATA_SUCCESSOR_KEY: None,
        SWEEPERS_PROVENANCE_VERSION_METADATA_KEY: SWEEPERS_PROVENANCE_VERSION,
    }

    def test_up_to_date_record_is_skippable_by_default(self):
        record = ProvenanceRecord.from_source(self.up_to_date_source)

        self.assertTrue(record.skip_write)

    def test_up_to_date_record_is_not_skippable_when_forced(self):
        record = ProvenanceRecord.from_source(self.up_to_date_source, force=True)

        self.assertFalse(record.skip_write)

    def test_forced_record_produces_update_which_will_be_written(self):
        records = [ProvenanceRecord.from_source(self.up_to_date_source, force=True)]

        updates = list(provenance.generate_updates(records))

        self.assertEqual(1, len(updates))
        self.assertFalse(updates[0].skip_write)
        self.assertEqual(SWEEPERS_PROVENANCE_VERSION, updates[0].content[SWEEPERS_PROVENANCE_VERSION_METADATA_KEY])

    def test_fetch_all_lids_pages_through_every_lid_without_version_filter(self):
        client = _FakeSearchClient(
            [_composite_response(["lid-a", "lid-b"], after_key={"lid": "lid-b"}), _composite_response(["lid-c"])]
        )

        with mock.patch.object(provenance, "resolve_multitenant_index_name", return_value="registry"):
            lids = list(provenance.fetch_all_lids(client, page_size=2))

        self.assertListEqual(["lid-a", "lid-b", "lid-c"], lids)
        self.assertEqual(2, len(client.request_bodies))
        self.assertNotIn("after", client.request_bodies[0]["aggs"]["unique_lids"]["composite"])
        self.assertDictEqual({"lid": "lid-b"}, client.request_bodies[1]["aggs"]["unique_lids"]["composite"]["after"])
        for body in client.request_bodies:
            self.assertNotIn(SWEEPERS_PROVENANCE_VERSION_METADATA_KEY, json.dumps(body))

    def test_fetch_target_lids_filters_by_version_unless_forced(self):
        unforced_client = _FakeSearchClient([_composite_response([])])
        forced_client = _FakeSearchClient([_composite_response(["lid-a"])])

        with mock.patch.object(provenance, "resolve_multitenant_index_name", return_value="registry"):
            list(provenance.fetch_target_lids(unforced_client))
            forced_lids = list(provenance.fetch_target_lids(forced_client, force=True))

        self.assertIn(SWEEPERS_PROVENANCE_VERSION_METADATA_KEY, json.dumps(unforced_client.request_bodies[0]))
        self.assertNotIn(SWEEPERS_PROVENANCE_VERSION_METADATA_KEY, json.dumps(forced_client.request_bodies[0]))
        self.assertListEqual(["lid-a"], forced_lids)

    def test_run_passes_force_through_to_lid_and_record_generation(self):
        with mock.patch.object(provenance, "configure_logging"), mock.patch.object(
            provenance, "ensure_index_mapping"
        ), mock.patch.object(provenance, "resolve_multitenant_index_name", return_value="registry"), mock.patch.object(
            provenance, "write_updated_docs"
        ), mock.patch.object(
            provenance, "fetch_target_lids", return_value=iter([])
        ) as fetch_target_lids, mock.patch.object(
            provenance, "generate_record_chains", return_value=iter([])
        ) as generate_record_chains:
            provenance.run(client=object(), force=True)

        fetch_target_lids.assert_called_once()
        self.assertTrue(fetch_target_lids.call_args.kwargs["force"])
        self.assertTrue(generate_record_chains.call_args.kwargs["force"])


if __name__ == "__main__":
    unittest.main()
