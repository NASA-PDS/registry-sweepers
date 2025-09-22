import logging
import os
import tempfile
from typing import Any
from typing import Iterator
from typing import Optional

from pds.registrysweepers.utils.bigdict.base import BigDict
from pds.registrysweepers.utils.bigdict.dictdict import DictDict
from pds.registrysweepers.utils.bigdict.sqlite3dict import SqliteDict


class AutoDict(BigDict):
    """
    A dictionary that starts as an in-memory DictDict but
    automatically switches to a SqliteDict when its size exceeds
    item_count_threshold.
    """

    def __init__(self, item_count_threshold: int, db_path: Optional[str] = None):
        """
        :param item_count_threshold: max in-memory items before switching to SQLite.
        :param db_path: optional explicit SQLite file path. If None, a temp file is used.
        """
        self.item_count_threshold = item_count_threshold
        self._db_path = db_path or os.path.join(tempfile.gettempdir(), f"autodict_{os.getpid()}_{id(self)}.sqlite")
        self._dict: BigDict = DictDict()  # start with in-memory

    def _check_upgrade(self) -> None:
        """If threshold exceeded and still using DictDict, switch to SqliteDict."""
        if isinstance(self._dict, DictDict) and len(self._dict) > self.item_count_threshold:
            # Create new SqliteDict and copy items over
            sqlite_dict = SqliteDict(self._db_path)
            logging.info(f"AutoDict disk flood threshold reached ({self.item_count_threshold})")
            logging.info(f"Switching AutoDict backend from {type(self._dict).__name__} to {type(sqlite_dict).__name__}")
            sqlite_dict.put_many(self._dict.items())
            self._dict = sqlite_dict

    def put(self, key: str, value: Any) -> None:
        self._dict.put(key, value)
        self._check_upgrade()

    def get(self, key: str) -> Optional[Any]:
        return self._dict.get(key)

    def pop(self, key: str) -> Optional[Any]:
        return self._dict.pop(key)

    def has(self, key: str) -> bool:
        return self._dict.has(key)

    def __iter__(self) -> Iterator[str]:
        return iter(self._dict)

    def __len__(self) -> int:
        return len(self._dict)

    def keys(self) -> Iterator[str]:
        return self._dict.keys()

    def values(self) -> Iterator[Any]:
        return self._dict.values()

    def items(self) -> Iterator[tuple[str, Any]]:
        return self._dict.items()

    def __getitem__(self, key: str) -> Any:
        return self._dict[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.put(key, value)

    def __contains__(self, key: str) -> bool:
        return key in self._dict

    def close(self):
        """Close underlying SQLite connection if we have one."""
        if isinstance(self._dict, SqliteDict):
            self._dict.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    @property
    def backend(self) -> str:
        """Return the name of the current backend ('DictDict' or 'SqliteDict') for unit testing"""
        return type(self._dict).__name__
