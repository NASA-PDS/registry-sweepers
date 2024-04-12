from __future__ import annotations

import json
from dataclasses import dataclass
from dataclasses import field
from typing import Callable
from typing import Set

from pds.registrysweepers.ancestry.typedefs import SerializableAncestryRecordTypeDef
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid


@dataclass
class AncestryRecord:
    lidvid: PdsLidVid
    parent_collection_lidvids: Set[PdsLidVid] = field(default_factory=set)
    parent_bundle_lidvids: Set[PdsLidVid] = field(default_factory=set)

    # flag to track records which are used during processing, but should not be written to db, for example if an
    # equivalent record is known to already exist due to up-to-date ancestry version flag in the source document
    skip_write: bool = False

    def __post_init__(self):
        if not isinstance(self.lidvid, PdsLidVid):
            raise ValueError('Cannot initialise AncestryRecord with non-PdsLidVid value for "lidvid"')

    def __repr__(self):
        return f"AncestryRecord(lidvid={self.lidvid}, parent_collection_lidvids={sorted([str(x) for x in self.parent_collection_lidvids])}, parent_bundle_lidvids={sorted([str(x) for x in self.parent_bundle_lidvids])})"

    def __hash__(self):
        return hash(self.lidvid)

    def to_dict(self, sort_lists: bool = True) -> SerializableAncestryRecordTypeDef:
        list_f: Callable = lambda x: sorted(x) if sort_lists else list(x)

        return {
            "lidvid": str(self.lidvid),
            "parent_collection_lidvids": list_f(str(lidvid) for lidvid in self.parent_collection_lidvids),
            "parent_bundle_lidvids": list_f(str(lidvid) for lidvid in self.parent_bundle_lidvids),
        }

    @staticmethod
    def from_dict(d: SerializableAncestryRecordTypeDef, skip_write: bool = False) -> AncestryRecord:
        try:
            return AncestryRecord(
                lidvid=PdsLidVid.from_string(d["lidvid"]),  # type: ignore
                parent_collection_lidvids=set(
                    PdsLidVid.from_string(lidvid) for lidvid in d["parent_collection_lidvids"]
                ),
                parent_bundle_lidvids=set(PdsLidVid.from_string(lidvid) for lidvid in d["parent_bundle_lidvids"]),
                skip_write=skip_write,
            )
        except (KeyError, ValueError) as err:
            raise ValueError(
                f'Could not parse valid AncestryRecord from provided dict due to "{err.__class__.__name__}: {err}" (got {json.dumps(d)})'
            )

    def update_with(self, other: AncestryRecord):
        """
        Given another AncestryRecord object with the same lidvid, add its parent histories to those of this
        AncestryRecord.  Used to merge partial histories.
        """

        if self.lidvid != other.lidvid:
            raise ValueError(
                f"lidvid mismatch in call to AncestryRecord.updateWith() (got {other.lidvid}, should be {self.lidvid})"
            )

        self.parent_bundle_lidvids.update(other.parent_bundle_lidvids)
        self.parent_collection_lidvids.update(other.parent_collection_lidvids)
