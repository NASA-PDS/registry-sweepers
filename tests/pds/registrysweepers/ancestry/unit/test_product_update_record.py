"""Unit tests for ProductUpdateRecord class"""
import pytest
from pds.registrysweepers.ancestry.productupdaterecord import ProductUpdateRecord
from pds.registrysweepers.utils.productidentifiers.pdslid import PdsLid
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid


class TestProductUpdateRecordInit:
    """Test ProductUpdateRecord initialization"""

    def test_create_with_lidvid_string(self):
        """Can create record with LIDVID string"""
        lidvid = PdsLidVid.from_string("urn:nasa:pds:collection::1.0")
        record = ProductUpdateRecord(lidvid)

        assert record.product == lidvid
        assert len(record.direct_ancestor_refs) == 0
        assert not record.skippable

    def test_create_with_initial_ancestors(self):
        """Can create record with initial ancestor references"""
        lidvid = PdsLidVid.from_string("urn:nasa:pds:collection::1.0")
        ancestor = PdsLidVid.from_string("urn:nasa:pds:bundle::1.0")
        record = ProductUpdateRecord(lidvid, direct_ancestor_refs=[ancestor])

        assert len(record.direct_ancestor_refs) == 2
        assert ancestor in record.direct_ancestor_refs
        assert ancestor.lid in record.direct_ancestor_refs

    def test_create_with_skip_write_flag(self):
        """Can create record with skip_write flag"""
        lidvid = PdsLidVid.from_string("urn:nasa:pds:collection::1.0")
        record = ProductUpdateRecord(lidvid, skip_write=True)

        assert record.skippable

    def test_invalid_product_type_raises_error(self):
        """Creating record with non-LIDVID raises ValueError"""
        # This should raise during __init__ since we validate the type
        with pytest.raises((ValueError, AttributeError)):
            ProductUpdateRecord("not_a_lidvid_object")


class TestAddDirectAncestorRef:
    """Test adding ancestor references"""

    def test_add_ancestor_with_lidvid(self):
        """Adding a LIDVID ancestor stores full LIDVID"""
        product = PdsLidVid.from_string("urn:nasa:pds:collection::1.0")
        ancestor = PdsLidVid.from_string("urn:nasa:pds:bundle::2.0")
        record = ProductUpdateRecord(product)

        record.add_direct_ancestor_ref(ancestor)

        assert len(record.direct_ancestor_refs) == 2
        assert ancestor in record.direct_ancestor_refs
        assert ancestor.lid in record.direct_ancestor_refs

    def test_add_ancestor_with_lid(self):
        """Adding a LID ancestor stores LID only"""
        product = PdsLidVid.from_string("urn:nasa:pds:collection::1.0")
        ancestor = PdsLid.from_string("urn:nasa:pds:bundle")
        record = ProductUpdateRecord(product)

        record.add_direct_ancestor_ref(ancestor)

        assert len(record.direct_ancestor_refs) == 1
        assert ancestor.lid in record.direct_ancestor_lid_refs

    def test_add_multiple_ancestors(self):
        """Adding multiple ancestors maintains all references"""
        product = PdsLidVid.from_string("urn:nasa:pds:product::1.0")
        ancestor1 = PdsLidVid.from_string("urn:nasa:pds:collection1::1.0")
        ancestor2 = PdsLidVid.from_string("urn:nasa:pds:collection2::1.0")
        record = ProductUpdateRecord(product)

        record.add_direct_ancestor_ref(ancestor1)
        record.add_direct_ancestor_ref(ancestor2)

        assert len(record.direct_ancestor_refs) == 4
        assert ancestor1 in record.direct_ancestor_refs
        assert ancestor1.lid in record.direct_ancestor_refs
        assert ancestor2 in record.direct_ancestor_refs
        assert ancestor2.lid in record.direct_ancestor_refs

    def test_add_duplicate_ancestor_deduplicates(self):
        """Adding same ancestor twice deduplicates (set behavior)"""
        product = PdsLidVid.from_string("urn:nasa:pds:collection::1.0")
        ancestor = PdsLidVid.from_string("urn:nasa:pds:bundle::1.0")
        record = ProductUpdateRecord(product)

        record.add_direct_ancestor_ref(ancestor)
        record.add_direct_ancestor_ref(ancestor)

        assert len(record.direct_ancestor_refs) == 2
        assert ancestor in record.direct_ancestor_refs
        assert ancestor.lid in record.direct_ancestor_refs

    def test_add_direct_ancestor_refs_bulk(self):
        """add_direct_ancestor_refs adds multiple ancestors at once"""
        product = PdsLidVid.from_string("urn:nasa:pds:product::1.0")
        ancestors = [
            PdsLidVid.from_string("urn:nasa:pds:collection1::1.0"),
            PdsLidVid.from_string("urn:nasa:pds:collection2::1.0"),
            PdsLidVid.from_string("urn:nasa:pds:collection3::1.0"),
        ]
        record = ProductUpdateRecord(product)

        record.add_direct_ancestor_refs(ancestors)

        assert len(record.direct_ancestor_refs) == 6
        for ancestor in ancestors:
            assert ancestor in record.direct_ancestor_refs
            assert ancestor.lid in record.direct_ancestor_refs


class TestDirectAncestorProperties:
    """Test properties for accessing ancestor references"""

    def test_direct_ancestor_lid_refs_returns_lids(self):
        """direct_ancestor_lid_refs returns only LID references"""
        product = PdsLidVid.from_string("urn:nasa:pds:product::1.0")
        lid_ancestor = PdsLid.from_string("urn:nasa:pds:bundle")
        lidvid_ancestor = PdsLidVid.from_string("urn:nasa:pds:collection::1.0")
        record = ProductUpdateRecord(product)

        record.add_direct_ancestor_ref(lid_ancestor)
        record.add_direct_ancestor_ref(lidvid_ancestor)

        lid_refs = record.direct_ancestor_lid_refs
        assert len(lid_refs) == 2  # Both have LIDs
        assert lid_ancestor.lid in lid_refs
        assert lidvid_ancestor.lid in lid_refs

    def test_direct_ancestor_lidvid_refs_returns_lidvids(self):
        """direct_ancestor_lidvid_refs returns only LIDVID references"""
        product = PdsLidVid.from_string("urn:nasa:pds:product::1.0")
        lid_ancestor = PdsLid.from_string("urn:nasa:pds:bundle")
        lidvid_ancestor = PdsLidVid.from_string("urn:nasa:pds:collection::1.0")
        record = ProductUpdateRecord(product)

        record.add_direct_ancestor_ref(lid_ancestor)
        record.add_direct_ancestor_ref(lidvid_ancestor)

        lidvid_refs = record.direct_ancestor_lidvid_refs
        assert len(lidvid_refs) == 1  # Only the LIDVID
        assert lidvid_ancestor in lidvid_refs

    def test_direct_ancestor_refs_returns_all(self):
        """direct_ancestor_refs returns union of LID and LIDVID refs"""
        product = PdsLidVid.from_string("urn:nasa:pds:product::1.0")
        lid_ancestor = PdsLid.from_string("urn:nasa:pds:bundle")
        lidvid_ancestor = PdsLidVid.from_string("urn:nasa:pds:collection::1.0")
        record = ProductUpdateRecord(product)

        record.add_direct_ancestor_ref(lid_ancestor)
        record.add_direct_ancestor_ref(lidvid_ancestor)

        all_refs = record.direct_ancestor_refs
        # Should have both the LID and LIDVID as separate entries
        assert len(all_refs) >= 2


class TestMarkProcessed:
    """Test marking record as complete"""

    def test_mark_processed_sets_complete_flag(self):
        """mark_processed sets internal completion state"""
        product = PdsLidVid.from_string("urn:nasa:pds:collection::1.0")
        record = ProductUpdateRecord(product)

        # Initially not complete
        assert record._complete is False

        record.mark_processed()

        # After marking, should be complete
        assert record._complete is True


class TestToDictSerialization:
    """Test serialization to dictionary"""

    def test_to_dict_includes_lidvid(self):
        """to_dict includes product LIDVID"""
        product = PdsLidVid.from_string("urn:nasa:pds:collection::1.0")
        record = ProductUpdateRecord(product)

        result = record.to_dict()

        assert "lidvid" in result
        assert result["lidvid"] == "urn:nasa:pds:collection::1.0"

    def test_to_dict_includes_ancestor_refs(self):
        """to_dict includes direct ancestor references"""
        product = PdsLidVid.from_string("urn:nasa:pds:product::1.0")
        ancestor = PdsLidVid.from_string("urn:nasa:pds:collection::1.0")
        record = ProductUpdateRecord(product, direct_ancestor_refs=[ancestor])

        result = record.to_dict()

        assert "direct_ancestor_refs" in result
        assert isinstance(result["direct_ancestor_refs"], list)
        assert "urn:nasa:pds:collection::1.0" in result["direct_ancestor_refs"]

    def test_to_dict_sorts_by_default(self):
        """to_dict sorts ancestor lists by default"""
        product = PdsLidVid.from_string("urn:nasa:pds:product::1.0")
        ancestors = [
            PdsLidVid.from_string("urn:nasa:pds:collection_c::1.0"),
            PdsLidVid.from_string("urn:nasa:pds:collection_a::1.0"),
            PdsLidVid.from_string("urn:nasa:pds:collection_b::1.0"),
        ]
        record = ProductUpdateRecord(product)
        record.add_direct_ancestor_refs(ancestors)

        result = record.to_dict()

        refs = result["direct_ancestor_refs"]
        # Should be sorted alphabetically
        assert refs == sorted(refs)

    def test_to_dict_can_skip_sorting(self):
        """to_dict can skip sorting when requested"""
        product = PdsLidVid.from_string("urn:nasa:pds:product::1.0")
        ancestor = PdsLidVid.from_string("urn:nasa:pds:collection::1.0")
        record = ProductUpdateRecord(product, direct_ancestor_refs=[ancestor])

        result = record.to_dict(sort_lists=False)

        assert "direct_ancestor_refs" in result
        assert isinstance(result["direct_ancestor_refs"], list)


class TestRecordHash:
    """Test hash and equality behavior"""

    def test_hash_based_on_product(self):
        """Record hash is based on product LIDVID"""
        product = PdsLidVid.from_string("urn:nasa:pds:collection::1.0")
        record1 = ProductUpdateRecord(product)
        record2 = ProductUpdateRecord(product)

        # Same product should have same hash
        assert hash(record1) == hash(record2)

    def test_can_use_in_set(self):
        """Records can be used in sets (hashable)"""
        product1 = PdsLidVid.from_string("urn:nasa:pds:collection1::1.0")
        product2 = PdsLidVid.from_string("urn:nasa:pds:collection2::1.0")
        record1 = ProductUpdateRecord(product1)
        record2 = ProductUpdateRecord(product2)

        record_set = {record1, record2}
        assert len(record_set) == 2

    def test_same_product_deduplicates_in_set(self):
        """Records for same product deduplicate in set"""
        product = PdsLidVid.from_string("urn:nasa:pds:collection::1.0")
        record1 = ProductUpdateRecord(product)
        record2 = ProductUpdateRecord(product)

        record_set = {record1, record2}
        assert len(record_set) == 1


class TestRecordRepr:
    """Test string representation"""

    def test_repr_includes_product(self):
        """String representation includes product LIDVID"""
        product = PdsLidVid.from_string("urn:nasa:pds:collection::1.0")
        record = ProductUpdateRecord(product)

        repr_str = repr(record)

        assert "urn:nasa:pds:collection::1.0" in repr_str

    def test_repr_includes_skip_write(self):
        """String representation includes skip_write flag"""
        product = PdsLidVid.from_string("urn:nasa:pds:collection::1.0")
        record = ProductUpdateRecord(product, skip_write=True)

        repr_str = repr(record)

        assert "_skip_write=True" in repr_str
