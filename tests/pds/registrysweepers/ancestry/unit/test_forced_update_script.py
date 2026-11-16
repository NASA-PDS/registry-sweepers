"""Unit tests for the forced-rewrite variant of the ancestry update script"""
import json

import pytest
from pds.registrysweepers.ancestry.constants import ANCESTRY_DEDUPLICATION_SCRIPT_FORCED_MINIFIED
from pds.registrysweepers.ancestry.constants import ANCESTRY_DEDUPLICATION_SCRIPT_MINIFIED
from pds.registrysweepers.ancestry.constants import ANCESTRY_REFS_METADATA_KEY
from pds.registrysweepers.ancestry.productupdaterecord import ProductUpdateRecord
from pds.registrysweepers.ancestry.utils import update_from_record
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY
from pds.registrysweepers.utils.db import update_as_statements
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid

COLLECTION_LIDVID = "urn:nasa:pds:bundle:collection::1.0"

# The default script as it was written before the forced variant existed, with its placeholders interpolated.
EXPECTED_DEFAULT_SCRIPT = (
    "boolean c=false;"
    "if(ctx._source['ops:Registry_Sweepers.ops:ancestor_refs']==null)"
    "{ctx._source['ops:Registry_Sweepers.ops:ancestor_refs']=[];c=true;}"
    "def e=new HashSet();"
    "for(i in ctx._source['ops:Registry_Sweepers.ops:ancestor_refs']){e.add(i);}"
    "for(i in params.new_items){if(!e.contains(i)){ctx._source['ops:Registry_Sweepers.ops:ancestor_refs'].add(i);c=true;}}"
    f"if(ctx._source['{SWEEPERS_ANCESTRY_VERSION_METADATA_KEY}']!={SWEEPERS_ANCESTRY_VERSION})"
    f"{{ctx._source['{SWEEPERS_ANCESTRY_VERSION_METADATA_KEY}']={SWEEPERS_ANCESTRY_VERSION};c=true;}}"
    "if(!c){ctx.op='none';}"
)


class TestForcedUpdateScript:
    product = PdsLidVid.from_string("urn:nasa:pds:bundle:collection:product::1.0")

    def _record(self):
        return ProductUpdateRecord(self.product, direct_ancestor_refs=[PdsLidVid.from_string(COLLECTION_LIDVID)])

    def test_default_script_is_unchanged(self):
        assert ANCESTRY_DEDUPLICATION_SCRIPT_MINIFIED == EXPECTED_DEFAULT_SCRIPT

    def test_scripts_have_no_unreplaced_placeholders(self):
        assert "PLACEHOLDER" not in ANCESTRY_DEDUPLICATION_SCRIPT_MINIFIED
        assert "PLACEHOLDER" not in ANCESTRY_DEDUPLICATION_SCRIPT_FORCED_MINIFIED

    def test_forced_script_differs_from_default_only_by_the_noop_guard(self):
        assert ANCESTRY_DEDUPLICATION_SCRIPT_MINIFIED == (
            ANCESTRY_DEDUPLICATION_SCRIPT_FORCED_MINIFIED + "if(!c){ctx.op='none';}"
        )

    def test_forced_script_does_not_set_ctx_op(self):
        assert "ctx.op" not in ANCESTRY_DEDUPLICATION_SCRIPT_FORCED_MINIFIED

    def test_default_update_uses_default_script(self):
        update = update_from_record(self._record())

        assert update.inline_script_content == ANCESTRY_DEDUPLICATION_SCRIPT_MINIFIED
        assert not update.force_rewrite

    def test_forced_update_uses_forced_script(self):
        update = update_from_record(self._record(), force=True)

        assert update.inline_script_content == ANCESTRY_DEDUPLICATION_SCRIPT_FORCED_MINIFIED
        assert update.force_rewrite

    def test_forced_update_writes_the_same_new_items_as_the_default_update(self):
        forced_update = update_from_record(self._record(), force=True)
        default_update = update_from_record(self._record())

        assert forced_update.inline_script_new_items == default_update.inline_script_new_items
        assert forced_update.content is None

    @pytest.mark.parametrize("as_upsert", [False, True])
    def test_forced_update_statement_is_a_script_without_detect_noop(self, as_upsert):
        update = update_from_record(self._record(), force=True)
        _, content_str = update_as_statements(update, as_upsert=as_upsert)

        statement = json.loads(content_str)
        assert statement["script"]["source"] == ANCESTRY_DEDUPLICATION_SCRIPT_FORCED_MINIFIED
        assert statement["script"]["params"] == {"new_items": update.inline_script_new_items}
        assert COLLECTION_LIDVID in statement["script"]["params"]["new_items"]
        assert ANCESTRY_REFS_METADATA_KEY in statement["script"]["source"]
        assert "detect_noop" not in statement

    def test_default_update_statement_is_unchanged(self):
        update = update_from_record(self._record())
        _, content_str = update_as_statements(update)

        assert json.loads(content_str) == {
            "script": {
                "source": ANCESTRY_DEDUPLICATION_SCRIPT_MINIFIED,
                "lang": "painless",
                "params": {"new_items": update.inline_script_new_items},
            }
        }
