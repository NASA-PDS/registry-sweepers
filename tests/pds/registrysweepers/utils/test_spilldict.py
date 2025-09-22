import os
import tempfile
import unittest

from pds.registrysweepers.utils.bigdict.spilldict import SpillDict


class TestSpillDict(unittest.TestCase):
    def setUp(self):
        # Simple merge function - sum of values
        self.merge_fn = lambda x, y: x + y
        self.db_path = os.path.join(tempfile.gettempdir(), f"spilldict_test_{os.getpid()}.sqlite")
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_cache_only_behavior(self):
        sd = SpillDict(spill_threshold=5, merge=self.merge_fn, db_path=self.db_path)
        sd.put("a", 1)
        sd.put("b", 2)

        # Still below threshold, all in cache
        self.assertIn("a", sd)
        self.assertIn("b", sd)
        self.assertEqual(len(sd), 2)
        self.assertEqual(sd.get("a"), 1)
        self.assertEqual(sd["b"], 2)
        sd.close()

    def test_spill_occurs_after_threshold(self):
        sd = SpillDict(spill_threshold=3, merge=self.merge_fn, spill_proportion=0.5, db_path=self.db_path)
        # Add items to exceed threshold
        for i in range(6):
            sd.put(f"k{i}", i)

        # We should have spilled three elements to SqliteDict
        self.assertEqual(len(sd), 6)
        self.assertTrue(len(sd._spill) == 4)

        # All keys accessible
        for i in range(6):
            self.assertIn(f"k{i}", sd)
            self.assertEqual(sd.get(f"k{i}"), i)

        sd.close()

    def test_conflict_merging(self):
        """
        Fill cache, spill to SQLite,
        then re-add conflicting keys with new values to trigger merge.
        """
        sd = SpillDict(spill_threshold=3, merge=self.merge_fn, db_path=self.db_path)

        # Preload some data and force a spill
        for i in range(1, 4):
            sd.put(f"k{i}", i)
        # Force spill by adding extra items
        sd.put("extra1", 10)
        sd.put("extra2", 20)

        # Check conflicting key is in _spill
        self.assertNotIn("k1", sd._cache)
        self.assertIn("k1", sd._spill)
        self.assertEqual(1, sd.get("k1"))

        # Add conflicting key back to cache with higher value to test merge
        sd.put("k1", 500)
        self.assertIn("k1", sd._cache)
        self.assertIn("k1", sd._spill)
        self.assertEqual(501, sd.get("k1"))

        # Force another spill to trigger merging
        sd.put("another", 30)

        # Check conflicting item has been merged properly to _spill
        self.assertNotIn("k1", sd._cache)
        self.assertIn("k1", sd._spill)
        self.assertEqual(501, sd.get("k1"))
        sd.close()

    def test_pop(self):
        sd = SpillDict(spill_threshold=2, merge=self.merge_fn, db_path=self.db_path)
        sd.put("x", 1)
        sd.put("y", 2)
        sd.put("z", 3)  # triggers spill
        self.assertTrue(len(sd) == 3)

        self.assertIn("x", sd)
        val_x = sd.pop("x")
        self.assertEqual(val_x, 1)
        self.assertNotIn("x", sd)

        self.assertIn("y", sd)
        val_y = sd.pop("y")
        self.assertEqual(val_y, 2)
        self.assertNotIn("y", sd)

        self.assertIn("z", sd)
        val_z = sd.pop("z")
        self.assertEqual(val_z, 3)
        self.assertNotIn("z", sd)

    def test_iters_and_len(self):
        sd = SpillDict(spill_threshold=2, merge=self.merge_fn, db_path=self.db_path)
        sd._spill.put("x", 1)
        sd._spill.put("y", 2)
        sd._spill.put("z", 3)

        sd._cache.put("y", 20)
        sd._cache.put("z", 30)
        sd._cache.put("A", 40)

        self.assertListEqual(["A", "x", "y", "z"], sorted(sd.keys()))
        self.assertListEqual([1, 22, 33, 40], sorted(sd.values()))
        self.assertSetEqual({("x", 1), ("y", 22), ("z", 33), ("A", 40)}, set(sd.items()))
        self.assertEqual(4, len(sd))

        sd.close()


if __name__ == "__main__":
    unittest.main()
