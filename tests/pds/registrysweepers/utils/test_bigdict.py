# test_autodict.py
import os
import tempfile
import unittest

from pds.registrysweepers.utils.bigdict.autodict import AutoDict
from pds.registrysweepers.utils.bigdict.autodict import DictDict
from pds.registrysweepers.utils.bigdict.autodict import SqliteDict


class TestAutoDict(unittest.TestCase):
    def setUp(self):
        # use a unique temp file per test
        self.db_path = os.path.join(tempfile.gettempdir(), f"autodict_test_{os.getpid()}.sqlite")
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_autodict_starts_in_memory(self):
        auto = AutoDict(item_count_threshold=3, db_path=self.db_path)
        self.assertEqual(auto.backend, "DictDict")
        auto.close()

    def test_autodict_put_get_and_upgrade(self):
        auto = AutoDict(item_count_threshold=3, db_path=self.db_path)
        # Insert below threshold
        auto.put("a", 1)
        auto.put("b", 2)
        self.assertEqual(len(auto), 2)
        self.assertTrue("a" in auto)
        self.assertEqual(auto.get("b"), 2)
        self.assertEqual(auto.backend, "DictDict")

        # Add more to exceed threshold -> triggers upgrade
        auto["c"] = 3
        auto["d"] = 4  # exceeds threshold
        self.assertEqual(auto.backend, "SqliteDict")
        self.assertEqual(len(auto), 4)
        self.assertTrue(auto.has("c"))
        self.assertEqual(auto["d"], 4)

        # Items/keys/values should still work
        keys = set(auto.keys())
        self.assertTrue({"a", "b", "c", "d"}.issubset(keys))
        vals = set(auto.values())
        self.assertTrue({1, 2, 3, 4}.issubset(vals))

        # Pop should remove and return value
        popped = auto.pop("a")
        self.assertEqual(popped, 1)
        self.assertFalse(auto.has("a"))
        auto.close()

    def test_autodict_backend_persists(self):
        auto = AutoDict(item_count_threshold=1, db_path=self.db_path)
        self.assertEqual(auto.backend, "DictDict")
        auto["x"] = 10
        self.assertEqual(auto.backend, "DictDict")
        auto["y"] = 20  # triggers upgrade quickly
        self.assertEqual(auto.backend, "SqliteDict")
        # should be backed by SQLite file now
        self.assertIsInstance(auto._dict, SqliteDict)
        auto.close()


if __name__ == "__main__":
    unittest.main()
