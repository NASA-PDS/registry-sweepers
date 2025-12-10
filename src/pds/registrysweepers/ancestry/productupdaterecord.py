from collections.abc import Callable
from collections.abc import Iterable
from dataclasses import dataclass
from dataclasses import field
from typing import Optional
from typing import Set

from pds.registrysweepers.ancestry.typedefs import SerializableAncestryRecordTypeDef
from pds.registrysweepers.utils.productidentifiers.pdslid import PdsLid
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid
from pds.registrysweepers.utils.productidentifiers.pdsproductidentifier import PdsProductIdentifier


@dataclass
class ProductUpdateRecord:
    """
    Replaces AncestryRecord, now that only direct ancestor references are being tracked, which is a much-simpler use case than AncestryRecord was designed for.
    These updates are treated cumulatively, so multiple ProductUpdate instances for the same product can be created and applied in sequence.
    """

    _direct_ancestor_refs: Set[PdsProductIdentifier] = field(default_factory=set)
    _skip_write: bool = False
    _complete: bool = False

    def __init__(self, product: PdsLidVid, direct_ancestor_refs: Optional[Iterable[PdsProductIdentifier]] = None, skip_write: bool = False):
        if not isinstance(product, PdsLidVid):
            raise ValueError('Cannot initialise ProductUpdateRecord with non-PdsLidVid value for "product"')
        self._product = product
        self._direct_ancestor_refs = set(direct_ancestor_refs or [])
        self._skip_write = skip_write
        self._complete = False

    def __post_init__(self):
        if not isinstance(self._product, PdsLidVid):
            raise ValueError('Cannot initialise ProductUpdateRecord with non-PdsLidVid value for "product"')

    def __repr__(self):
        return f"ProductUpdateRecord({self._product=}, {self._skip_write=}, {self._complete=}, direct_ancestor_refs={sorted([str(x) for x in self._direct_ancestor_refs])})"

    def __hash__(self):
        return hash(self._product)

    def to_dict(self, sort_lists: bool = True) -> SerializableAncestryRecordTypeDef:
        list_f: Callable = lambda x: sorted(x) if sort_lists else list(x)

        return {
            "lidvid": str(self.product),
            "direct_ancestor_refs": list_f(str(lidvid) for lidvid in self.direct_ancestor_refs),
        }

    @property
    def product(self):
        return self._product

    @property
    def skippable(self):
        return self._skip_write

    def mark_processed(self):
        """
        Mark this product as complete, indicating that all descendant references to it have been successfully written to the database.
        """
        self._complete = True

    def add_direct_ancestor_ref(self, ref: PdsProductIdentifier):
        self._direct_ancestor_refs.add(ref)

    def add_direct_ancestor_refs(self, refs: Iterable[PdsProductIdentifier]):
        self._direct_ancestor_refs.update(refs)

    @property
    def direct_ancestor_lid_refs(self) -> Set[PdsLid]:
        return {ref.lid for ref in self._direct_ancestor_refs}

    @property
    def direct_ancestor_lidvid_refs(self) -> Set[PdsLidVid]:
        return {ref for ref in self._direct_ancestor_refs if isinstance(ref, PdsLidVid)}

    @property
    def direct_ancestor_refs(self) -> Set[PdsProductIdentifier]:
        """Return the set of distinct LID and LIDVID direct ancestor, including all LIDs implied by LIDVIDs"""
        return self.direct_ancestor_lid_refs | self.direct_ancestor_lidvid_refs
