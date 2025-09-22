from abc import ABC
from abc import abstractmethod
from typing import Any
from typing import Iterator
from typing import Optional


class BigDict(ABC):
    """Abstract base class for a big dictionary-like object."""

    # TODO: investigate lmdb as an alternative backend (as standalone, or for spill) in case it provides superior
    #  performance.  It's used in https://pypi.org/project/bigdict/ - edunn 20250922

    @abstractmethod
    def put(self, key: str, value: Any) -> None:
        pass

    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        pass

    @abstractmethod
    def pop(self, key: str) -> Optional[Any]:
        pass

    @abstractmethod
    def has(self, key: str) -> bool:
        pass

    @abstractmethod
    def __iter__(self) -> Iterator[str]:
        pass

    @abstractmethod
    def __len__(self) -> int:
        pass

    def keys(self) -> Iterator[str]:
        return iter(self)

    def values(self) -> Iterator[Any]:
        for k in self:
            yield self.get(k)

    def items(self) -> Iterator[tuple[str, Any]]:
        for k in self:
            yield (k, self.get(k))

    # Allow bracket syntax for getting/setting
    def __getitem__(self, key: str) -> Any:
        val = self.get(key)
        if val is None:
            raise KeyError(key)
        return val

    def __setitem__(self, key: str, value: Any) -> None:
        self.put(key, value)

    def __contains__(self, key: str) -> bool:
        return self.has(key)
