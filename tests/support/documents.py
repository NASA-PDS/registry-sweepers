from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class SeedDoc:
    index: Literal["registry", "registry-refs", "registry-dd"]
    id: str
    source: dict
