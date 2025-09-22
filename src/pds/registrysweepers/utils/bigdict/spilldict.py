# in bigdict.py
import logging
import math
import os
import tempfile
from itertools import islice
from typing import Any
from typing import Callable
from typing import Iterator
from typing import Optional
from typing import Tuple

from more_itertools import batched
from pds.registrysweepers.utils.bigdict.base import BigDict
from pds.registrysweepers.utils.bigdict.dictdict import DictDict
from pds.registrysweepers.utils.bigdict.sqlite3dict import SqliteDict


class SpillDict(BigDict):
    """
    A hybrid dictionary with an in-memory cache and a SQLite spillover

    - Fast access for recently-added items in _cache
    - When cache length exceeds item_count_threshold, items spill into SQLite
    - When spilling an item with a key already existing in the Sqlite db, the conflict is managed according to a
      function provided by the caller during initialisation
    """

    _cache: DictDict
    _spill: SqliteDict
    _item_merge_fn: Callable[[Any, Any], Any]
    _spill_proportion: float

    def __init__(
        self,
        spill_threshold: int,
        merge: Callable[[Any, Any], Any],
        spill_proportion: float = 0.9,
        db_path: Optional[str] = None,
    ):
        """
        :param spill_threshold: maximum fast cache items before spilling to disk
        :param merge: function merge(new, existing) -> merged_value
        :param spill_proportion: proportion of filled cache to spill to disk
        :param db_path: path to SQLite DB file; temp file if None
        """
        self.spill_threshold = spill_threshold
        self._item_merge_fn = merge
        self._spill_proportion = spill_proportion
        self._db_path = db_path or os.path.join(tempfile.gettempdir(), f"spilldict_{os.getpid()}_{id(self)}.sqlite")
        self._cache = DictDict()
        self._spill = SqliteDict(self._db_path)

    def _spill_if_needed(self):
        """Spill items from cache into the SQLite store when threshold exceeded."""
        if len(self._cache) <= self.spill_threshold:
            return

        spill_count = math.ceil(self.spill_threshold * self._spill_proportion)
        logging.info(
            f"Spill threshold {self.spill_threshold} reached - spilling {spill_count} items from cache to disk"
        )

        items_to_spill = list(islice(self._cache.items(), spill_count))

        # Move the first {spill_count} items to the SqliteDict
        for batch in batched(items_to_spill, spill_count):
            conflicting_ids = self._spill.put_many_returning_conflicts(batch)
            if conflicting_ids:
                merged_items = {}
                existing_items = self._spill.get_many(conflicting_ids)
                for k, spilled_value in existing_items:
                    cached_value = self._cache[k]
                    merged_item = self._item_merge_fn(cached_value, spilled_value)
                    merged_items[k] = merged_item

                logging.info(f"Merged {len(merged_items)} conflicting items during spill operation")

                # Write the merged items back to _spill, overwriting
                self._spill.put_many(merged_items.items())

        # Pop the spilled/merged items from _cache
        for k, _ in items_to_spill:
            self._cache.pop(k)

    def put(self, key: str, value: Any) -> None:
        """Insert into cache, then spill if needed."""
        self._cache[key] = value
        self._spill_if_needed()

    def get(self, key: str) -> Optional[Any]:
        """Get the union of cached and spilled data for a key"""
        cached_value = self._cache.get(key)
        spilled_value = self._spill.get(key)

        if cached_value is not None and spilled_value is not None:
            return self._item_merge_fn(cached_value, spilled_value)
        else:
            return cached_value or spilled_value

    def pop(self, key: str) -> Optional[Any]:
        """Union cached and spilled data for a key, and pop from both"""
        if key in self._cache:
            cached_value = self._cache.pop(key)
        else:
            cached_value = None

        spilled_value = self._spill.pop(key)
        if cached_value is not None and spilled_value is not None:
            return self._item_merge_fn(cached_value, spilled_value)
        else:
            return cached_value or spilled_value

    def has(self, key: str) -> bool:
        """Check in cache, then SQLite."""
        return key in self._cache or self._spill.has(key)

    def __iter__(self) -> Iterator[str]:
        """Iterate over the union of cached and spilled keys"""

        for key in self._cache:
            yield key

        for key in self._spill:
            if key in self._cache:
                continue
            yield key

    def __len__(self) -> int:
        """Total unique count across cache and spill."""
        return len(self._cache) + sum(1 for k in self._spill if k not in self._cache)

    def keys(self) -> Iterator[str]:
        return iter(self)

    def values(self) -> Iterator[Any]:
        for key, value in self.items():
            yield value

    def items(self) -> Iterator[Tuple[str, Any]]:
        remaining_cached_keys = set(self._cache.keys())
        for key, spilled_value in self._spill.items():
            if key in self._cache:
                cached_value = self._cache[key]
                yield key, self._item_merge_fn(cached_value, spilled_value)
                remaining_cached_keys.remove(key)
            else:
                yield key, spilled_value

        for key in remaining_cached_keys:
            yield key, self._cache[key]

    def __getitem__(self, key: str) -> Any:
        val = self.get(key)
        if val is None:
            raise KeyError(key)
        return val

    def __setitem__(self, key: str, value: Any) -> None:
        self.put(key, value)

    def __contains__(self, key: str) -> bool:
        return self.has(key)

    def close(self):
        self._spill.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
