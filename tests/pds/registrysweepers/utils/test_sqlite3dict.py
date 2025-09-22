import os
import pickle
import tempfile
import unittest

from pds.registrysweepers.utils.bigdict.sqlite3dict import SqliteDict


class TestSqliteDictPutManyConflicts(unittest.TestCase):
    def setUp(self):
        self.db_path = os.path.join(tempfile.gettempdir(), f"sqlitedict_test_{os.getpid()}.sqlite")
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self.sqlite_dict = SqliteDict(self.db_path)

    def tearDown(self):
        self.sqlite_dict.close()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_put_many_returning_conflicts(self):
        # initial insert
        rows = [(f"k{i}", i) for i in range(5)]
        conflicts = self.sqlite_dict.put_many_returning_conflicts(rows)
        self.assertEqual(conflicts, [])
        self.assertEqual(len(self.sqlite_dict), 5)

        # insert with some conflicts
        new_rows = [(f"k{i}", i * 10) for i in range(3, 8)]
        conflicts = self.sqlite_dict.put_many_returning_conflicts(new_rows)
        # keys 3 and 4 should conflict
        self.assertCountEqual(conflicts, ["k3", "k4"])
        # Non-conflicting keys 5,6,7 should be inserted
        self.assertEqual(len(self.sqlite_dict), 8)
        # Verify values
        for i in range(3):
            # existing values remain unchanged
            self.assertEqual(self.sqlite_dict.get(f"k{i}"), i)
        for i in range(5, 8):
            # new values inserted
            self.assertEqual(self.sqlite_dict.get(f"k{i}"), i * 10)


class TestSqliteDictGetMany(unittest.TestCase):
    def setUp(self):
        self.db_path = os.path.join(tempfile.gettempdir(), f"sqlitedict_test_{os.getpid()}.sqlite")
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self.sqlite_dict = SqliteDict(self.db_path)

    def tearDown(self):
        self.sqlite_dict.close()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_get_many(self):
        kvs = {f"k{i}": i for i in range(5)}
        self.sqlite_dict.put_many(kvs.items())

        rows = self.sqlite_dict.get_many(kvs.keys())
        self.assertSetEqual(set(kvs.items()), set(rows))


if __name__ == "__main__":
    unittest.main()
