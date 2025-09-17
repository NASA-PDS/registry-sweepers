import pickle
import sqlite3
from typing import Any, Optional, Iterator

from src.pds.registrysweepers.utils.bigdict.base import BigDict


class SqliteDict(BigDict):
    """SQLite-backed BigDict for large datasets"""

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._conn = sqlite3.connect(self._db_path)
        self._conn.execute('PRAGMA journal_mode = WAL')  # enable write-ahead logging for sanic.gif (>4x when tested)
        self._conn.execute("""
                           CREATE TABLE IF NOT EXISTS bigdict
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
                "REPLACE INTO bigdict (key, value) VALUES (?, ?)",
                (key, blob)
            )

    def get(self, key: str) -> Optional[Any]:
        cur = self._conn.execute(
            "SELECT value FROM bigdict WHERE key = ?", (key,)
        )
        row = cur.fetchone()
        if row is None:
            return None
        return pickle.loads(row[0])

    def pop(self, key: str) -> Optional[Any]:
        val = self.get(key)
        if val is not None:
            with self._conn:
                self._conn.execute("DELETE FROM bigdict WHERE key = ?", (key,))
        return val

    def has(self, key: str) -> bool:
        cur = self._conn.execute(
            "SELECT 1 FROM bigdict WHERE key = ? LIMIT 1", (key,)
        )
        return cur.fetchone() is not None

    def __iter__(self) -> Iterator[str]:
        cur = self._conn.execute("SELECT key FROM bigdict")
        for (key,) in cur:
            yield key

    def __len__(self) -> int:
        cur = self._conn.execute("SELECT COUNT(*) FROM bigdict")
        (count,) = cur.fetchone()
        return count

    def close(self):
        """Close the SQLite connection."""
