import json
import os
import shutil
import tempfile
import unittest

from pds.registrysweepers.ancestry.utils import load_history_from_filepath
from pds.registrysweepers.ancestry.utils import make_history_serializable
from pds.registrysweepers.ancestry.utils import write_history_to_filepath


class TestMakeHistorySerializableTestCase(unittest.TestCase):
    def test_basic_behaviour(self):
        input = {
            r["lidvid"]: r
            for r in [
                {
                    "lidvid": f"a:b:c:d:e:{x}::1.0",
                    "parent_collection_lidvids": {"a:b:c:d:e::1.0"},
                    "parent_bundle_lidvids": {"a:b:c:d::1.0"},
                }
                for x in ["A", "B", "C"]
            ]
        }

        expected = {
            str(r["lidvid"]): r
            for r in [
                {
                    "lidvid": f"a:b:c:d:e:{x}::1.0",
                    "parent_collection_lidvids": ["a:b:c:d:e::1.0"],
                    "parent_bundle_lidvids": ["a:b:c:d::1.0"],
                }
                for x in ["A", "B", "C"]
            ]
        }

        make_history_serializable(input)

        self.assertDictEqual(expected, input)


if __name__ == "__main__":
    unittest.main()
