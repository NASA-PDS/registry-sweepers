from typing import Any
from typing import Iterator
from typing import Optional

from pds.registrysweepers.utils.bigdict.base import BigDict


class DictDict(BigDict):
    """Baseline in-memory dictionary-backed BigDict, for speed"""

    def __init__(self):
        self._store: dict[str, Any] = {}

    def put(self, key: str, value: Any) -> None:
        self._store[key] = value

    def get(self, key: str) -> Optional[Any]:
        return self._store.get(key)

    def pop(self, key: str) -> Optional[Any]:
        return self._store.pop(key, None)

    def has(self, key: str) -> bool:
        return key in self._store

    def __iter__(self) -> Iterator[str]:
        return iter(self._store)

    def __len__(self) -> int:
        return len(self._store)
