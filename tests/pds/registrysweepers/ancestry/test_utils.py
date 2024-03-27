import json
import os
import shutil
import tempfile
import unittest

from pds.registrysweepers.ancestry import AncestryRecord
from pds.registrysweepers.ancestry.utils import load_history_from_filepath
from pds.registrysweepers.ancestry.utils import make_history_serializable
from pds.registrysweepers.ancestry.utils import merge_matching_history_chunks
from pds.registrysweepers.ancestry.utils import write_history_to_filepath
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid


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


class TestMergeMatchingHistoryChunksTestCase(unittest.TestCase):
    def setUp(self):
        setup_fp = os.path.abspath(
            "./tests/pds/registrysweepers/ancestry/test_utils_merge_matching_history_chunks.json"
        )
        with open(setup_fp) as setup_infile:
            setup_content = json.load(setup_infile)

        self.temp_dir = tempfile.mkdtemp()
        for fn, file_content in setup_content["inputs"].items():
            fp = os.path.join(self.temp_dir, fn)
            write_history_to_filepath(file_content, fp)

        self.dest_fp = os.path.join(self.temp_dir, "dest.dump")
        self.src_fps = [os.path.join(self.temp_dir, f"src{i}.dump") for i in range(1, 3)]

        self.expected_outputs = setup_content["outputs"]

    def tearDown(self):
        shutil.rmtree(self.temp_dir)
        self.temp_dir = None

    def test_merges_correctly(self):
        merge_matching_history_chunks(self.dest_fp, self.src_fps)
        for fn, expected_content in self.expected_outputs.items():
            fp = os.path.join(self.temp_dir, fn)
            resultant_content = load_history_from_filepath(fp)
            self.assertDictEqual(expected_content, resultant_content)


if __name__ == "__main__":
    unittest.main()
