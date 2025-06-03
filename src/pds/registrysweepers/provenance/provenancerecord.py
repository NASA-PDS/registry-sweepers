from __future__ import annotations

from typing import Dict
from typing import Optional

from pds.registrysweepers.provenance.constants import METADATA_SUCCESSOR_KEY
from pds.registrysweepers.provenance.versioning import SWEEPERS_PROVENANCE_VERSION
from pds.registrysweepers.provenance.versioning import SWEEPERS_PROVENANCE_VERSION_METADATA_KEY
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid


class ProvenanceRecord:
    lidvid: PdsLidVid
    _successor: Optional[PdsLidVid]
    skip_write: bool

    def __init__(self, lidvid: PdsLidVid, successor: Optional[PdsLidVid], skip_write: bool = False):
        self.lidvid = lidvid
        self._successor = successor
        self.skip_write = skip_write

    @property
    def successor(self) -> Optional[PdsLidVid]:
        return self._successor

    def set_successor(self, successor: PdsLidVid):
        if successor != self._successor:
            self._successor = successor
            self.skip_write = False

    @staticmethod
    def from_source(_source: Dict) -> ProvenanceRecord:
        successor_exists_in_doc = METADATA_SUCCESSOR_KEY in _source
        successor = _source.get(METADATA_SUCCESSOR_KEY)

        # It is assumed prima facie that any document processed with an up-to-date version of the sweeper is up-to-date.
        # If the value of successor is changed, this assumption is invalidated in the setter.
        # The check that a (null or non-null) successor value is explicitly defined in the doc is probably redundant,
        # but can stay for the moment
        skip_write = (
            successor_exists_in_doc
            and _source.get(SWEEPERS_PROVENANCE_VERSION_METADATA_KEY, 0) >= SWEEPERS_PROVENANCE_VERSION
        )

        return ProvenanceRecord(
            lidvid=PdsLidVid.from_string(_source["lidvid"]), successor=successor, skip_write=skip_write
        )

    @staticmethod
    def from_doc(doc: Dict) -> ProvenanceRecord:
        return ProvenanceRecord.from_source(doc["_source"])
