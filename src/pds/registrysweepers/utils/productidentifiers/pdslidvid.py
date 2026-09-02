from __future__ import annotations

import functools

from pds.registrysweepers.utils.productidentifiers.pdslid import PdsLid
from pds.registrysweepers.utils.productidentifiers.pdsproductidentifier import PdsProductIdentifier
from pds.registrysweepers.utils.productidentifiers.pdsvid import PdsVid


@functools.total_ordering
class PdsLidVid(PdsProductIdentifier):
    def __init__(self, lid: PdsLid, vid: PdsVid):
        self._lid = lid
        self.vid = vid

    @property
    def lid(self) -> PdsLid:
        return self._lid

    @staticmethod
    def from_string(lidvid_str: str) -> PdsLidVid:
        try:
            lid_chunk, vid_chunk = lidvid_str.split(PdsProductIdentifier.LIDVID_SEPARATOR)
            lid = PdsLid(lid_chunk)
            vid = PdsVid.from_string(vid_chunk)
            return PdsLidVid(lid, vid)
        except ValueError as err:
            raise RuntimeError(f"Failed to parse LIDVID {lidvid_str}: {err}") from err
    def __str__(self):
        return str(self.lid) + PdsProductIdentifier.LIDVID_SEPARATOR + str(self.vid)

    def __hash__(self):
        return hash(str(self))

    def __repr__(self):
        return f"PdsLidVid({str(self)})"

    def __eq__(self, other):
        if not isinstance(other, PdsLidVid):
            return False

        return self.lid == other.lid and self.vid == other.vid

    def __lt__(self, other: PdsLidVid):
        if self.lid != other.lid:
            raise ValueError(
                f"Comparison is only defined between LIDVIDs with identical LIDs (got {self.lid}, {other.lid})"
            )

        return self.vid < other.vid
