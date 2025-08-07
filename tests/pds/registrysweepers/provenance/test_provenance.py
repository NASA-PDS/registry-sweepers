import itertools
import json
import os.path
import unittest

from build.lib.pds.registrysweepers.provenance.versioning import SWEEPERS_BROKEN_PROVENANCE_VERSION_METADATA_KEY
from pds.registrysweepers import provenance
from pds.registrysweepers.provenance import ProvenanceRecord
from pds.registrysweepers.provenance import SWEEPERS_PROVENANCE_VERSION
from pds.registrysweepers.provenance import SWEEPERS_PROVENANCE_VERSION_METADATA_KEY
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


if __name__ == "__main__":
    unittest.main()
