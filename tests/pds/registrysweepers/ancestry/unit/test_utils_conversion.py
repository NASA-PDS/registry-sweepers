"""Unit tests for utility functions in utils.py

Focuses on the conversion and update creation functions.
"""
import pytest
from pds.registrysweepers.ancestry.constants import ANCESTRY_REFS_METADATA_KEY
from pds.registrysweepers.ancestry.productupdaterecord import ProductUpdateRecord
from pds.registrysweepers.ancestry.utils import update_from_record
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY
from pds.registrysweepers.utils.productidentifiers.pdslid import PdsLid
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid


class TestUpdateFromRecord:
    """Test conversion of ProductUpdateRecord to Update"""

    def test_creates_update_with_product_id(self):
        """Update ID matches product LIDVID"""
        product = PdsLidVid.from_string("urn:nasa:pds:collection::1.0")
        record = ProductUpdateRecord(product)

        update = update_from_record(record)

        assert update.id == "urn:nasa:pds:collection::1.0"

    def test_includes_ancestry_refs_in_content(self):
        """Update content includes ancestor references"""
        product = PdsLidVid.from_string("urn:nasa:pds:product::1.0")
        ancestor = PdsLidVid.from_string("urn:nasa:pds:collection::1.0")
        record = ProductUpdateRecord(product, direct_ancestor_refs=[ancestor])

        update = update_from_record(record)

        assert ANCESTRY_REFS_METADATA_KEY in update.content
        refs = update.content[ANCESTRY_REFS_METADATA_KEY]
        assert isinstance(refs, list)
        assert "urn:nasa:pds:collection::1.0" in refs

    def test_includes_ancestry_version_in_content(self):
        """Update content includes current ancestry version"""
        product = PdsLidVid.from_string("urn:nasa:pds:collection::1.0")
        record = ProductUpdateRecord(product)

        update = update_from_record(record)

        assert SWEEPERS_ANCESTRY_VERSION_METADATA_KEY in update.content
        version = update.content[SWEEPERS_ANCESTRY_VERSION_METADATA_KEY]
        assert version == SWEEPERS_ANCESTRY_VERSION
        assert isinstance(version, int)

    def test_includes_deduplication_script(self):
        """Update includes inline script for deduplication"""
        product = PdsLidVid.from_string("urn:nasa:pds:collection::1.0")
        record = ProductUpdateRecord(product)

        update = update_from_record(record)

        # Should have inline script content for deduplication
        assert update.inline_script_content is not None
        assert len(update.inline_script_content) > 0

    def test_handles_multiple_ancestors(self):
        """Update correctly includes multiple ancestor references"""
        product = PdsLidVid.from_string("urn:nasa:pds:product::1.0")
        ancestors = [
            PdsLidVid.from_string("urn:nasa:pds:collection1::1.0"),
            PdsLidVid.from_string("urn:nasa:pds:collection2::1.0"),
            PdsLidVid.from_string("urn:nasa:pds:collection3::1.0"),
        ]
        record = ProductUpdateRecord(product)
        record.add_direct_ancestor_refs(ancestors)

        update = update_from_record(record)

        refs = update.content[ANCESTRY_REFS_METADATA_KEY]
        assert len(refs) == 6
        for ancestor in ancestors:
            assert str(ancestor) in refs
            assert str(ancestor.lid) in refs

    def test_handles_mixed_lid_and_lidvid_ancestors(self):
        """Update handles both LID and LIDVID ancestor references"""
        product = PdsLidVid.from_string("urn:nasa:pds:product::1.0")
        lid_ancestor = PdsLid.from_string("urn:nasa:pds:bundle")
        lidvid_ancestor = PdsLidVid.from_string("urn:nasa:pds:collection::1.0")
        record = ProductUpdateRecord(product)

        record.add_direct_ancestor_ref(lid_ancestor)
        record.add_direct_ancestor_ref(lidvid_ancestor)

        update = update_from_record(record)

        refs = update.content[ANCESTRY_REFS_METADATA_KEY]
        # Should contain both references (as strings)
        assert len(refs) >= 2
        assert any("bundle" in ref for ref in refs)
        assert any("collection" in ref for ref in refs)

    def test_handles_empty_ancestors(self):
        """Update correctly handles record with no ancestors"""
        product = PdsLidVid.from_string("urn:nasa:pds:collection::1.0")
        record = ProductUpdateRecord(product)

        update = update_from_record(record)

        refs = update.content[ANCESTRY_REFS_METADATA_KEY]
        assert isinstance(refs, list)
        assert len(refs) == 0

    def test_ancestor_refs_are_strings(self):
        """Ancestor references in update are strings, not objects"""
        product = PdsLidVid.from_string("urn:nasa:pds:product::1.0")
        ancestor = PdsLidVid.from_string("urn:nasa:pds:collection::1.0")
        record = ProductUpdateRecord(product, direct_ancestor_refs=[ancestor])

        update = update_from_record(record)

        refs = update.content[ANCESTRY_REFS_METADATA_KEY]
        for ref in refs:
            assert isinstance(ref, str)
