from __future__ import annotations

import json
from dataclasses import dataclass
from dataclasses import field
from itertools import chain
from typing import Callable
from typing import List
from typing import Set

from pds.registrysweepers.ancestry.typedefs import SerializableAncestryRecordTypeDef
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid


@dataclass
class AncestryRecord:
    lidvid: PdsLidVid
    # parent lidvid members are used to directly attach history elements
    explicit_parent_collection_lidvids: Set[PdsLidVid] = field(default_factory=set)
    explicit_parent_bundle_lidvids: Set[PdsLidVid] = field(default_factory=set)
    # parent record members are used to attach collection AncestryRecords to those of descendant non-aggregate products
    # this prevents an enormous amount of unnecessary duplication (i.e. memory use)
    _parent_records: List[AncestryRecord] = field(default_factory=list)

    # flag to track records which are used during processing, but should not be written to db, for example if an
    # equivalent record is known to already exist due to up-to-date ancestry version flag in the source document
    skip_write: bool = False

    def resolve_parent_bundle_lidvids(self) -> Set[PdsLidVid]:
        """
        Return a set of all bundle LIDVIDs this AncestryRecord references, either explicitly or via association with a
        parent AncestryRecord.
        :return:
        """
        derived_parent_bundle_lidvids = chain(
            *[record.resolve_parent_bundle_lidvids() for record in self._parent_records]
        )
        return self.explicit_parent_bundle_lidvids.union(derived_parent_bundle_lidvids)

    def resolve_parent_collection_lidvids(self) -> Set[PdsLidVid]:
        """
        Return a set of all collection LIDVIDs this AncestryRecord references, either explicitly or via association with a
        parent AncestryRecord.
        :return:
        """

        derived_parent_collection_lidvids = chain(
            *[record.resolve_parent_collection_lidvids() for record in self._parent_records]
        )
        return self.explicit_parent_collection_lidvids.union(derived_parent_collection_lidvids)

    def __post_init__(self):
        if not isinstance(self.lidvid, PdsLidVid):
            raise ValueError('Cannot initialise AncestryRecord with non-PdsLidVid value for "lidvid"')

    def __repr__(self):
        return f"AncestryRecord(lidvid={self.lidvid}, parent_collection_lidvids={sorted([str(x) for x in self.resolve_parent_collection_lidvids()])}, parent_bundle_lidvids={sorted([str(x) for x in self.resolve_parent_bundle_lidvids()])})"

    def __hash__(self):
        return hash(self.lidvid)

    def to_dict(self, sort_lists: bool = True) -> SerializableAncestryRecordTypeDef:
        list_f: Callable = lambda x: sorted(x) if sort_lists else list(x)

        return {
            "lidvid": str(self.lidvid),
            "parent_collection_lidvids": list_f(str(lidvid) for lidvid in self.resolve_parent_collection_lidvids()),
            "parent_bundle_lidvids": list_f(str(lidvid) for lidvid in self.resolve_parent_bundle_lidvids()),
        }

    @staticmethod
    def from_dict(d: SerializableAncestryRecordTypeDef, skip_write: bool = False) -> AncestryRecord:
        try:
            return AncestryRecord(
                lidvid=PdsLidVid.from_string(d["lidvid"]),  # type: ignore
                explicit_parent_collection_lidvids=set(
                    PdsLidVid.from_string(lidvid) for lidvid in d["parent_collection_lidvids"]
                ),
                explicit_parent_bundle_lidvids=set(
                    PdsLidVid.from_string(lidvid) for lidvid in d["parent_bundle_lidvids"]
                ),
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

        self.explicit_parent_bundle_lidvids.update(other.resolve_parent_bundle_lidvids())
        self.explicit_parent_collection_lidvids.update(other.resolve_parent_collection_lidvids())
        self._parent_records.extend(other._parent_records)

    def attach_parent_record(self, record: AncestryRecord):
        """
        Attach a parent record to this AncestryRecord, whose parents will be inherited by this AncestryRecord.
        :param record:
        :return:
        """
        self._parent_records.append(record)

        if record.lidvid.is_bundle():
            self.explicit_parent_bundle_lidvids.add(record.lidvid)
        elif record.lidvid.is_collection():
            self.explicit_parent_collection_lidvids.add(record.lidvid)
        else:
            raise ValueError(
                f"Cannot attach a non-aggregate AncestryRecord as parent of another AncestryRecord (got {record.lidvid})"
            )

    @staticmethod
    def combine(first: AncestryRecord, second: AncestryRecord) -> AncestryRecord:
        """Returns a non-mutating union of two AncestryRecords."""
        if first.lidvid != second.lidvid:
            raise ValueError(
                f"Cannot combine AncestryRecord objects with different lidvids (got {first.lidvid} and {second.lidvid})"
            )
        lidvid = first.lidvid
        combined_record = AncestryRecord(lidvid)
        combined_record.update_with(first)
        combined_record.update_with(second)
        return combined_record

    def union(self, other: AncestryRecord) -> AncestryRecord:
        """Returns a non-mutating union of two AncestryRecords."""

        return AncestryRecord.combine(self, other)
