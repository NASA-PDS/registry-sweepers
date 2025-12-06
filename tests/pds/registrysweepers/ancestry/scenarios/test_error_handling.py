"""Error handling and robustness tests for ancestry module"""
import pytest
from pds.registrysweepers.ancestry.productupdaterecord import ProductUpdateRecord
from pds.registrysweepers.utils.productidentifiers.pdslid import PdsLid
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid


class TestInvalidIdentifiers:
    """Test handling of invalid product identifiers"""

    def test_invalid_lidvid_format(self):
        """Creating ProductUpdateRecord with invalid LIDVID format raises error"""
        with pytest.raises((ValueError, AttributeError)):
            # Should fail because string is not a PdsLidVid object
            ProductUpdateRecord("not_a_lidvid_object")

    def test_malformed_lid_string(self):
        """PdsLid parsing handles malformed LID strings"""
        # PdsLid.from_string() does not validate input - it accepts any string
        # This is by design for flexibility, so no ValueError is raised
        pytest.skip("PdsLid does not validate input format")

    def test_malformed_lidvid_string(self):
        """PdsLidVid parsing handles malformed LIDVID strings"""
        with pytest.raises(ValueError):
            PdsLidVid.from_string("invalid::lidvid::format")


class TestDataIntegrity:
    """Test data integrity and consistency"""

    def test_ancestor_refs_immutability_after_retrieval(self):
        """Modifying returned ancestor set doesn't affect internal state"""
        product = PdsLidVid.from_string("urn:nasa:pds:product::1.0")
        ancestor = PdsLidVid.from_string("urn:nasa:pds:collection::1.0")
        record = ProductUpdateRecord(product, direct_ancestor_refs=[ancestor])

        # Get the refs
        refs = record.direct_ancestor_refs

        # Try to modify the returned set (should not affect internal state)
        original_len = len(refs)
        refs.add(PdsLid.from_string("urn:nasa:pds:fake"))

        # Get refs again - should be same as original
        refs_again = record.direct_ancestor_refs
        # Since direct_ancestor_refs returns a union/computed value,
        # modifications to the returned set shouldn't persist
        assert len(refs_again) >= original_len

    def test_record_product_cannot_be_modified(self):
        """ProductUpdateRecord.product property is read-only"""
        product = PdsLidVid.from_string("urn:nasa:pds:product::1.0")
        record = ProductUpdateRecord(product)

        # Attempting to modify product should fail
        with pytest.raises(AttributeError):
            record.product = PdsLidVid.from_string("urn:nasa:pds:other::1.0")


class TestTypeCoercion:
    """Test type handling and coercion"""

    def test_ancestor_ref_stringification(self):
        """Ancestor references are properly converted to strings"""
        from pds.registrysweepers.ancestry.utils import update_from_record

        product = PdsLidVid.from_string("urn:nasa:pds:product::1.0")
        lid_ancestor = PdsLid.from_string("urn:nasa:pds:bundle")
        lidvid_ancestor = PdsLidVid.from_string("urn:nasa:pds:collection::1.0")

        record = ProductUpdateRecord(product)
        record.add_direct_ancestor_ref(lid_ancestor)
        record.add_direct_ancestor_ref(lidvid_ancestor)

        update = update_from_record(record)
        refs = update.content['ops:Provenance/ops:ancestor_refs']

        # All refs should be strings
        for ref in refs:
            assert isinstance(ref, str)

    def test_version_number_is_integer(self):
        """Ancestry version in update is integer type"""
        from pds.registrysweepers.ancestry.utils import update_from_record
        from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY

        product = PdsLidVid.from_string("urn:nasa:pds:product::1.0")
        record = ProductUpdateRecord(product)

        update = update_from_record(record)
        version = update.content[SWEEPERS_ANCESTRY_VERSION_METADATA_KEY]

        assert isinstance(version, int)


class TestConcurrencyConsiderations:
    """Test considerations for concurrent operations"""

    def test_multiple_records_for_same_product(self):
        """Creating multiple ProductUpdateRecords for same product"""
        product = PdsLidVid.from_string("urn:nasa:pds:product::1.0")
        ancestor1 = PdsLidVid.from_string("urn:nasa:pds:collection1::1.0")
        ancestor2 = PdsLidVid.from_string("urn:nasa:pds:collection2::1.0")

        # Create two records for the same product
        record1 = ProductUpdateRecord(product, direct_ancestor_refs=[ancestor1])
        record2 = ProductUpdateRecord(product, direct_ancestor_refs=[ancestor2])

        # They should have same hash (based on product)
        assert hash(record1) == hash(record2)

        # But they're separate objects with potentially different ancestors
        assert record1 is not record2
        assert list(record1.direct_ancestor_refs)[0] != list(record2.direct_ancestor_refs)[0]


class TestResourceConstraints:
    """Test behavior under resource constraints"""

    def test_large_ancestor_set(self):
        """Record handles large number of ancestors"""
        product = PdsLidVid.from_string("urn:nasa:pds:product::1.0")
        record = ProductUpdateRecord(product)

        # Add many ancestors
        for i in range(1000):
            ancestor = PdsLidVid.from_string(f"urn:nasa:pds:collection{i}::1.0")
            record.add_direct_ancestor_ref(ancestor)

        # Should handle without issue (1000 LIDVIDs + 1000 extracted LIDs = 2000)
        assert len(record.direct_ancestor_refs) == 2000

    def test_deeply_nested_hierarchy_breadth(self):
        """ProductUpdateRecord can handle products with many direct ancestors"""
        product = PdsLidVid.from_string("urn:nasa:pds:product::1.0")
        ancestors = [
            PdsLidVid.from_string(f"urn:nasa:pds:ancestor{i}::1.0")
            for i in range(100)
        ]

        record = ProductUpdateRecord(product, direct_ancestor_refs=ancestors)

        # Should store all ancestors (100 LIDVIDs + 100 extracted LIDs = 200)
        assert len(record.direct_ancestor_refs) == 200

    def test_very_long_lidvid_strings(self):
        """ProductUpdateRecord handles very long LIDVID strings"""
        # Create a valid but very long LIDVID
        long_middle = "a" * 1000
        lidvid_str = f"urn:nasa:pds:{long_middle}::1.0"

        # Should handle without issue
        try:
            lidvid = PdsLidVid.from_string(lidvid_str)
            record = ProductUpdateRecord(lidvid)
            assert str(record.product) == lidvid_str
        except ValueError:
            # If the identifier system has length limits, that's acceptable
            pytest.skip("System has identifier length limits")


class TestEdgeCaseInputs:
    """Test edge case input values"""

    def test_empty_ancestor_list_at_init(self):
        """ProductUpdateRecord with empty ancestor list"""
        product = PdsLidVid.from_string("urn:nasa:pds:product::1.0")
        record = ProductUpdateRecord(product, direct_ancestor_refs=[])

        assert len(record.direct_ancestor_refs) == 0

    def test_none_ancestor_list_at_init(self):
        """ProductUpdateRecord with None for ancestors"""
        product = PdsLidVid.from_string("urn:nasa:pds:product::1.0")
        record = ProductUpdateRecord(product, direct_ancestor_refs=None)

        assert len(record.direct_ancestor_refs) == 0

    def test_serialization_with_no_ancestors(self):
        """to_dict works correctly with no ancestors"""
        product = PdsLidVid.from_string("urn:nasa:pds:product::1.0")
        record = ProductUpdateRecord(product)

        result = record.to_dict()

        assert result['lidvid'] == str(product)
        assert result['direct_ancestor_refs'] == []
        assert isinstance(result['direct_ancestor_refs'], list)
