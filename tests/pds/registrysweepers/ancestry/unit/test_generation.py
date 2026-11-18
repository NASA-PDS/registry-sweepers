"""Unit tests for ancestry.generation helpers"""
import pytest
from pds.registrysweepers.ancestry.generation import bundle_update_records_from_docs
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY


def _bundle_doc(version):
    source = {"lidvid": "urn:nasa:pds:bundle::1.0", "product_class": "Product_Bundle"}
    if version is not None:
        source[SWEEPERS_ANCESTRY_VERSION_METADATA_KEY] = version
    return {"_id": source["lidvid"], "_source": source}


class TestBundleUpdateRecordsFromDocs:
    """Bundles are skipped only when stamped with exactly the current ancestry version"""

    @pytest.mark.parametrize(
        "stored_version, expect_skip",
        [
            (None, False),
            (SWEEPERS_ANCESTRY_VERSION - 1, False),
            (SWEEPERS_ANCESTRY_VERSION, True),
            (SWEEPERS_ANCESTRY_VERSION + 1, False),  # stamped by a newer sweeper: reprocess after downgrade
        ],
    )
    def test_skip_write_only_at_current_version(self, stored_version, expect_skip):
        (record,) = list(bundle_update_records_from_docs([_bundle_doc(stored_version)]))
        assert record._skip_write is expect_skip
