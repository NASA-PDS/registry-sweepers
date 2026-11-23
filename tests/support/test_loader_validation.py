"""
Seed-doc validation must match the shape of production registry documents, which store
fields under dotted top-level _source keys and never as nested objects.
"""
import pytest

from tests.support.documents import SeedDoc
from tests.support.indices import load_mappings
from tests.support.loader import _validate_docs


def _validate(source):
    _validate_docs([SeedDoc(index="registry", id="d", source=source)], load_mappings())


def test_dotted_leaf_keys_are_accepted():
    _validate({"lidvid": "urn:x::1.0", "ops:Harvest_Info.ops:harvest_date_time": ["2024-01-01T00:00:00Z"]})


def test_nested_object_is_rejected_with_a_hint():
    with pytest.raises(ValueError, match=r"ops:Harvest_Info \(a mapping object: use its dotted leaf keys"):
        _validate({"lidvid": "urn:x::1.0", "ops:Harvest_Info": {"ops:harvest_date_time": ["2024-01-01T00:00:00Z"]}})


def test_unknown_field_is_rejected():
    with pytest.raises(ValueError, match=r"ops:Sweepers/ops:ancestry_version"):
        _validate({"lidvid": "urn:x::1.0", "ops:Sweepers/ops:ancestry_version": 1})
