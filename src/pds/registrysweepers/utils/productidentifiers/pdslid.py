from __future__ import annotations

from typing import List

from pds.registrysweepers.utils.productidentifiers.pdsproductidentifier import PdsProductIdentifier


class PdsLid(PdsProductIdentifier):
    def __init__(self, value: str):
        self._value = value

    @property
    def value(self) -> str:
        return self._value

    @property
    def lid(self) -> PdsLid:
        return self

    def __str__(self):
        return self.value

    @staticmethod
    def from_string(lid: str) -> PdsLid:
        return PdsLid(lid)

    def __eq__(self, other):
        if not isinstance(other, PdsLid):
            return False
        return self.value == other.value

    def __hash__(self):
        return hash(self._value)

    def __repr__(self):
        return f'PdsLid("{self.value}")'

    @property
    def _fields(self) -> List[str]:
        return self.value.split(":")

    @property
    def _fields_count(self):
        """Return the number of name fields contained in this LID"""
        return len(self._fields)

    def is_bundle(self):
        return self._fields_count == 4

    def is_collection(self):
        return self._fields_count == 5

    def is_basic_product(self):
        return self._fields_count == 6

    def _get_field(self, index: int) -> str:
        try:
            return self.value.split(":")[index]
        except IndexError:
            return ""

    @property
    def national_agency_name(self) -> str:
        return self._get_field(1)

    @property
    def archiving_agency_name(self) -> str:
        return self._get_field(2)

    @property
    def bundle_name(self) -> str:
        return self._get_field(3)

    @property
    def collection_name(self) -> str | None:
        value = self._get_field(4)
        return value if value != "" else None

    @property
    def basic_product_name(self) -> str | None:
        value = self._get_field(5)
        return value if value != "" else None

    @property
    def parent_bundle_lid(self) -> PdsLid | None:
        if self.is_bundle():
            return None

        return PdsLid.from_string(":".join(self._fields[:4]))

    @property
    def parent_collection_lid(self) -> PdsLid | None:
        if self.is_bundle() or self.is_collection():
            return None

        return PdsLid.from_string(":".join(self._fields[:5]))
