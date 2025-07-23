import unittest

from pds.registrysweepers.ancestry.ancestryrecord import AncestryRecord
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid


class AncestryRecordTestCase(unittest.TestCase):
    def test_serialization(self):
        lidvid_str = "a:b:c:d:e:f::1.0"
        collection_lidvid_strs = ["a:b:c:d:e::1.0", "a:b:c:d:e::2.0"]
        bundle_lidvid_strs = ["a:b:c:d::1.0", "a:b:c:d::2.0"]

        record = AncestryRecord(
            lidvid=PdsLidVid.from_string(lidvid_str),
            explicit_parent_collection_lidvids=set(PdsLidVid.from_string(id) for id in collection_lidvid_strs),
            explicit_parent_bundle_lidvids=set(PdsLidVid.from_string(id) for id in bundle_lidvid_strs),
        )

        expected_dict_repr = {
            "lidvid": "a:b:c:d:e:f::1.0",
            "parent_collection_lidvids": ["a:b:c:d:e::1.0", "a:b:c:d:e::2.0"],
            "parent_bundle_lidvids": ["a:b:c:d::1.0", "a:b:c:d::2.0"],
        }

        self.assertEqual(record, AncestryRecord.from_dict(expected_dict_repr))
        self.assertEqual(expected_dict_repr, record.to_dict())

    def test_update_with_basic_functionality(self):
        lidvid_str = "a:b:c:d:e:f::1.0"
        mismatched_lidvid_str = "a:b:c:d:e:f::2.0"
        collection_lidvid_strs = ["a:b:c:d:e::1.0", "a:b:c:d:e::2.0"]
        bundle_lidvid_strs = ["a:b:c:d::1.0", "a:b:c:d::2.0"]

        dest = AncestryRecord(
            lidvid=PdsLidVid.from_string(lidvid_str),
            explicit_parent_collection_lidvids={
                PdsLidVid.from_string(collection_lidvid_strs[0]),
            },
            explicit_parent_bundle_lidvids={
                PdsLidVid.from_string(bundle_lidvid_strs[0]),
            },
        )

        src = AncestryRecord(
            lidvid=PdsLidVid.from_string(lidvid_str),
            explicit_parent_collection_lidvids={
                PdsLidVid.from_string(collection_lidvid_strs[1]),
            },
            explicit_parent_bundle_lidvids={
                PdsLidVid.from_string(bundle_lidvid_strs[1]),
            },
        )

        expected = AncestryRecord(
            lidvid=PdsLidVid.from_string(lidvid_str),
            explicit_parent_collection_lidvids={PdsLidVid.from_string(id) for id in collection_lidvid_strs},
            explicit_parent_bundle_lidvids={PdsLidVid.from_string(id) for id in bundle_lidvid_strs},
        )

        dest.update_with(src)
        self.assertEqual(expected, dest, "update_with() works")

        bad_src = AncestryRecord(
            lidvid=PdsLidVid.from_string(mismatched_lidvid_str),
            explicit_parent_collection_lidvids={
                PdsLidVid.from_string(collection_lidvid_strs[1]),
            },
            explicit_parent_bundle_lidvids={
                PdsLidVid.from_string(bundle_lidvid_strs[1]),
            },
        )

        # test update_with() raises ValueError on mismatched lidvids
        self.assertRaises(ValueError, lambda: dest.update_with(bad_src))

    def test_resolve_collection_lidvids_with_inheritance(self):
        parent_record_bundle_lidvids = {PdsLidVid.from_string(s) for s in ["a:b:c:d::1.0"]}
        parent_record_collection_lidvids = {PdsLidVid.from_string(s) for s in ["a:b:c:d:e::1.0"]}
        # the fact that a collection is being given a parent collection is a testism
        parent_record = AncestryRecord(
            PdsLidVid.from_string("a:b:c:d:parent::1.0"),
            explicit_parent_bundle_lidvids=parent_record_bundle_lidvids,
            explicit_parent_collection_lidvids=parent_record_collection_lidvids)

        explicitly_empty_record = AncestryRecord(PdsLidVid.from_string("a:b:c:d:e:f::1.0"))

        explicitly_empty_record.attach_parent_record(parent_record)
        self.assertEqual(parent_record_bundle_lidvids, explicitly_empty_record.resolve_parent_bundle_lidvids(),
                         'child inherits bundle history correctly')
        expected_collection_ancestry = parent_record_collection_lidvids.union({parent_record.lidvid})
        self.assertEqual(expected_collection_ancestry, explicitly_empty_record.resolve_parent_collection_lidvids(),
                         'child inherits collection history correctly')


if __name__ == "__main__":
    unittest.main()
