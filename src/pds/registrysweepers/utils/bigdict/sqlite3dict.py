import pickle
import sqlite3
from typing import Any, Optional, Iterator, Tuple, Iterable

from src.pds.registrysweepers.utils.bigdict.base import BigDict
from src.pds.registrysweepers.utils.misc import iterate_pages_of_size


class SqliteDict(BigDict):
    """SQLite-backed BigDict for large datasets"""

    def __init__(self, db_path: str):
        self.table_name = 'bigdict'
        self._db_path = db_path
        self._conn = sqlite3.connect(self._db_path)
        self._conn.execute('PRAGMA journal_mode = WAL')  # enable write-ahead logging for sanic.gif (>4x when tested)
        self._conn.execute('PRAGMA synchronous = OFF')  # db is transient - corruption-on-crash is acceptable
        self._conn.execute(f"""
                           CREATE TABLE IF NOT EXISTS {self.table_name}
                           (
                               key
                               TEXT
                               PRIMARY
                               KEY,
                               value
                               BLOB
                           )
                           """)
        self._conn.commit()

    def put(self, key: str, value: Any) -> None:
        blob = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        with self._conn:
            self._conn.execute(
                f"REPLACE INTO {self.table_name} (key, value) VALUES (?, ?)",
                (key, blob)
            )

    def put_many(self, kv_pairs: Iterable[Tuple[str, Any]], batch_size: int = 500) -> None:
        """
        Insert or replace multiple key/value pairs efficiently in one transaction.

        :param kv_pairs: sequence of (key, value) pairs
        """
        # Pre-pickle everything to avoid pickling inside the transaction loop
        to_insert = [
            (key, pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL))
            for key, value in kv_pairs
        ]
        for batch in iterate_pages_of_size(batch_size, to_insert):
            with self._conn:
                self._conn.executemany(
                    f"REPLACE INTO {self.table_name} (key, value) VALUES (?, ?)",
                    batch
                )

    def get(self, key: str) -> Optional[Any]:
        cur = self._conn.execute(
            f"SELECT value FROM {self.table_name} WHERE key = ?", (key,)
        )
        row = cur.fetchone()
        if row is None:
            return None
        return pickle.loads(row[0])

    def pop(self, key: str) -> Optional[Any]:
        val = self.get(key)
        if val is not None:
            with self._conn:
                self._conn.execute(f"DELETE FROM {self.table_name} WHERE key = ?", (key,))
        return val

    def has(self, key: str) -> bool:
        cur = self._conn.execute(
            f"SELECT 1 FROM {self.table_name} WHERE key = ? LIMIT 1", (key,)
        )
        return cur.fetchone() is not None

    def __iter__(self) -> Iterator[str]:
        cur = self._conn.execute(f"SELECT key FROM {self.table_name}")
        for (key,) in cur:
            yield key

    def __len__(self) -> int:
        cur = self._conn.execute(f"SELECT COUNT(*) FROM {self.table_name}")
        (count,) = cur.fetchone()
        return count

    def values(self) -> Iterator[Any]:
        cur = self._conn.execute(f"SELECT value FROM {self.table_name}")
        for (value,) in cur:
            yield pickle.loads(value)

    def items(self) -> Iterator[tuple[str, Any]]:
        cur = self._conn.execute(f"SELECT key, value FROM {self.table_name}")
        for (key, value) in cur:
            yield key, pickle.loads(value)

    def close(self):
        """Close the SQLite connection."""
