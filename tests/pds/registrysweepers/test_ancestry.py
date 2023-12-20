import itertools
import os.path
import unittest
from typing import Dict
from typing import List
from typing import Tuple

from pds.registrysweepers import ancestry
from pds.registrysweepers.ancestry import AncestryRecord
from pds.registrysweepers.ancestry import get_collection_ancestry_records
from pds.registrysweepers.ancestry import get_nonaggregate_ancestry_records
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid

from tests.mocks.registryquerymock import RegistryQueryMock


class AncestryBasicTestCase(unittest.TestCase):
    input_file_path = os.path.abspath("./tests/pds/registrysweepers/test_ancestry_mock_AncestryFunctionalTestCase.json")
    registry_query_mock = RegistryQueryMock(input_file_path)

    ancestry_records: List[AncestryRecord] = []
    bulk_updates: List[Tuple[str, Dict[str, List]]] = []

    expected_bundle_ancestry_by_collection = {
        "a:b:c:bundle:lidrefcollection::1.0": {"a:b:c:bundle::1.0"},
        "a:b:c:bundle:lidrefcollection::2.0": {"a:b:c:bundle::1.0"},
        "a:b:c:bundle:lidvidrefcollection::1.0": {"a:b:c:bundle::1.0"},
        "a:b:c:bundle:lidvidrefcollection::2.0": {
            # intentionally empty
        },
    }

    expected_collection_ancestry_by_nonaggregate = {
        "a:b:c:bundle:lidrefcollection:collectionsharedproduct::1.0": {
            "a:b:c:bundle:lidrefcollection::1.0",
            "a:b:c:bundle:lidrefcollection::2.0",
        },
        "a:b:c:bundle:lidrefcollection:collectionuniqueproduct::1.0": {
            "a:b:c:bundle:lidrefcollection::1.0",
        },
        "a:b:c:bundle:lidrefcollection:collectionuniqueproduct::2.0": {
            "a:b:c:bundle:lidrefcollection::2.0",
        },
        "a:b:c:bundle:lidvidrefcollection:collectionsharedproduct::1.0": {
            "a:b:c:bundle:lidvidrefcollection::1.0",
            "a:b:c:bundle:lidvidrefcollection::2.0",
        },
        "a:b:c:bundle:lidvidrefcollection:collectionuniqueproduct::1.0": {
            "a:b:c:bundle:lidvidrefcollection::1.0",
        },
        "a:b:c:bundle:lidvidrefcollection:collectionuniqueproduct::2.0": {
            "a:b:c:bundle:lidvidrefcollection::2.0",
        },
    }

    @classmethod
    def setUpClass(cls) -> None:
        ancestry.run(
            client=None,
            registry_mock_query_f=cls.registry_query_mock.get_mocked_query,
            ancestry_records_accumulator=cls.ancestry_records,
            bulk_updates_sink=cls.bulk_updates,
        )

        cls.bundle_records = [r for r in cls.ancestry_records if r.lidvid.is_bundle()]
        cls.collection_records = [r for r in cls.ancestry_records if r.lidvid.is_collection()]
        cls.nonaggregate_records = [r for r in cls.ancestry_records if r.lidvid.is_basic_product()]

        cls.records_by_lidvid_str = {str(r.lidvid): r for r in cls.ancestry_records}
        cls.bundle_records_by_lidvid_str = {str(r.lidvid): r for r in cls.ancestry_records if r.lidvid.is_bundle()}
        cls.collection_records_by_lidvid_str = {
            str(r.lidvid): r for r in cls.ancestry_records if r.lidvid.is_collection()
        }
        cls.nonaggregate_records_by_lidvid_str = {
            str(r.lidvid): r for r in cls.ancestry_records if r.lidvid.is_basic_product()
        }

        cls.updates_by_lidvid_str = {id: content for id, content in cls.bulk_updates}

    def test_bundles_have_no_ancestry(self):
        for record in self.bundle_records:
            self.assertTrue(len(record.parent_bundle_lidvids) == 0)
            self.assertTrue(len(record.parent_collection_lidvids) == 0)

    def test_collections_have_no_collection_ancestry(self):
        for record in self.collection_records:
            self.assertTrue(len(record.parent_collection_lidvids) == 0)

    def test_collections_have_correct_bundle_ancestry(self):
        for record in self.collection_records:
            expected_bundle_ancestry = set(self.expected_bundle_ancestry_by_collection[str(record.lidvid)])
            self.assertEqual(expected_bundle_ancestry, set(str(id) for id in record.parent_bundle_lidvids))

    def test_nonaggregates_have_correct_collection_ancestry(self):
        for record in self.nonaggregate_records:
            expected_collection_ancestry = set(self.expected_collection_ancestry_by_nonaggregate[str(record.lidvid)])
            self.assertEqual(expected_collection_ancestry, set(str(id) for id in record.parent_collection_lidvids))

    def test_nonaggregates_have_correct_bundle_ancestry(self):
        print(
            "#### N.B. This test will always fail if test_nonaggregates_have_correct_collection_ancestry() fails! ####"
        )
        for record in self.nonaggregate_records:
            parent_collection_id_strs = set(str(id) for id in record.parent_collection_lidvids)
            parent_bundle_id_strs = set(str(id) for id in record.parent_bundle_lidvids)
            expected_bundle_id_strs = set(
                itertools.chain(*[self.expected_bundle_ancestry_by_collection[id] for id in parent_collection_id_strs])
            )
            self.assertEqual(expected_bundle_id_strs, parent_bundle_id_strs)

    def test_correct_bulk_update_kvs_are_produced(self):
        for record in self.ancestry_records:
            update = self.updates_by_lidvid_str[str(record.lidvid)]
            self.assertEqual(
                set(str(lidvid) for lidvid in record.parent_bundle_lidvids),
                set(update["ops:Provenance/ops:parent_bundle_identifier"]),
            )
            self.assertEqual(
                set(str(lidvid) for lidvid in record.parent_collection_lidvids),
                set(update["ops:Provenance/ops:parent_collection_identifier"]),
            )

        for doc_id, update in self.bulk_updates:
            record = self.records_by_lidvid_str[doc_id]
            self.assertEqual(
                set(update["ops:Provenance/ops:parent_bundle_identifier"]),
                set(str(lidvid) for lidvid in record.parent_bundle_lidvids),
            )
            self.assertEqual(
                set(update["ops:Provenance/ops:parent_collection_identifier"]),
                set(str(lidvid) for lidvid in record.parent_collection_lidvids),
            )


class AncestryAlternateIdsTestCase(unittest.TestCase):
    input_file_path = os.path.abspath(
        "./tests/pds/registrysweepers/test_ancestry_mock_AncestryAlternateIdsTestCase.json"
    )
    registry_query_mock = RegistryQueryMock(input_file_path)

    ancestry_records: List[AncestryRecord] = []
    bulk_updates: List[Tuple[str, Dict[str, List]]] = []

    @classmethod
    def setUpClass(cls) -> None:
        ancestry.run(
            client=None,
            registry_mock_query_f=cls.registry_query_mock.get_mocked_query,
            ancestry_records_accumulator=cls.ancestry_records,
            bulk_updates_sink=cls.bulk_updates,
        )

        cls.bundle_records = [r for r in cls.ancestry_records if r.lidvid.is_bundle()]
        cls.collection_records = [r for r in cls.ancestry_records if r.lidvid.is_collection()]
        cls.nonaggregate_records = [r for r in cls.ancestry_records if r.lidvid.is_basic_product()]

        cls.records_by_lidvid_str = {str(r.lidvid): r for r in cls.ancestry_records}
        cls.bundle_records_by_lidvid_str = {str(r.lidvid): r for r in cls.ancestry_records if r.lidvid.is_bundle()}
        cls.collection_records_by_lidvid_str = {
            str(r.lidvid): r for r in cls.ancestry_records if r.lidvid.is_collection()
        }
        cls.nonaggregate_records_by_lidvid_str = {
            str(r.lidvid): r for r in cls.ancestry_records if r.lidvid.is_basic_product()
        }

        cls.updates_by_lidvid_str = {id: content for id, content in cls.bulk_updates}

    def test_collection_aliases_propagate_from_bundle_collection_lid_references(self):
        """
        Test that when a bundle references a collection by LID, its LIDVID is added to the bundle ancestry for the
        LIDVIDs of all collection LIDVIDs sharing a LID alias with the referenced collection.
        """
        collections = [c for c in self.collection_records if c.lidvid.collection_name.upper() == "CL"]
        self.assertEqual(3, len(collections))

        expected_parent_bundle_lidvids = {
            "_:_:_:B::1.0",
            "_:_:_:b::2.0",
            "_:_:_:b::3.0",
        }  # all LID-referenced collections should contain ancestry for these three bundles
        for collection in collections:
            parent_bundle_lidvids = {str(lidvid) for lidvid in collection.parent_bundle_lidvids}
            msg = f"Collection {collection} should have parent_bundle_lidvids={expected_parent_bundle_lidvids} (got {parent_bundle_lidvids})"
            self.assertSetEqual(expected_parent_bundle_lidvids, parent_bundle_lidvids, msg=msg)

    def test_collection_aliases_do_not_propagate_from_bundle_collection_lidvid_references(self):
        """
        Test that when a bundle references a collection by LIDVID, the bundle LIDVID is *not* added to the bundle
        ancestry for non-referenced collections which share a LID with the reference collection.
        """

        collection_v1: AncestryRecord = self.collection_records_by_lidvid_str["_:_:_:B:CLV::1.0"]
        v1_expected_parent_bundle_lidvids = {"_:_:_:B::1.0"}
        self.assertSetEqual(v1_expected_parent_bundle_lidvids, {str(lv) for lv in collection_v1.parent_bundle_lidvids})

        collection_v2: AncestryRecord = self.collection_records_by_lidvid_str["_:_:_:b:CLV::2.0"]
        v2_expected_parent_bundle_lidvids = {"_:_:_:b::2.0"}
        self.assertSetEqual(v2_expected_parent_bundle_lidvids, {str(lv) for lv in collection_v2.parent_bundle_lidvids})

        collection_v3: AncestryRecord = self.collection_records_by_lidvid_str["_:_:_:b:clv::3.0"]
        v1_expected_parent_bundle_lidvids = {"_:_:_:b::3.0"}
        self.assertSetEqual(v1_expected_parent_bundle_lidvids, {str(lv) for lv in collection_v3.parent_bundle_lidvids})

    def test_collection_aliases_propagate_from_bundle_collection_lid_references_to_nonaggregates(self):
        """
        Test that when a bundle references a collection by LID, its LIDVID is added to the bundle ancestry for all
        nonaggregate products within all collections that share a LID alias with it.
        """
        collection_records = [r for r in self.collection_records if r.lidvid.collection_name.upper() == "CL"]
        collection_lids = [r.lidvid.lid for r in collection_records]
        self.assertEqual(3, len(collection_records))

        products = [p for p in self.nonaggregate_records if p.lidvid.parent_collection_lid in collection_lids]

        expected_parent_bundle_lidvids = {
            "_:_:_:B::1.0",
            "_:_:_:b::2.0",
            "_:_:_:b::3.0",
        }  # all LID-referenced collections should contain ancestry for these three bundles
        for product in products:
            parent_bundle_lidvids = {str(lidvid) for lidvid in product.parent_bundle_lidvids}
            msg = f"Product {product} should have parent_bundle_lidvids={expected_parent_bundle_lidvids} (got {parent_bundle_lidvids})"
            self.assertSetEqual(expected_parent_bundle_lidvids, parent_bundle_lidvids, msg=msg)

    def test_collection_aliases_do_not_propagate_from_bundle_collection_lidvid_references_to_nonaggregates(self):
        """
        Test that when a bundle references a collection by LID, the bundle's LIDVID is *not* added to the bundle
        ancestry for nonaggregate products within non-referenced collections which share a LID alias with the referenced
        collection.
        """

        product_v1: AncestryRecord = self.nonaggregate_records_by_lidvid_str["_:_:_:B:CLV:product::1.0"]
        v1_expected_parent_bundle_lidvids = {"_:_:_:B::1.0"}
        self.assertSetEqual(v1_expected_parent_bundle_lidvids, {str(lv) for lv in product_v1.parent_bundle_lidvids})

        product_v2: AncestryRecord = self.nonaggregate_records_by_lidvid_str["_:_:_:b:CLV:product::2.0"]
        v2_expected_parent_bundle_lidvids = {"_:_:_:b::2.0"}
        self.assertSetEqual(v2_expected_parent_bundle_lidvids, {str(lv) for lv in product_v2.parent_bundle_lidvids})

        product_v3: AncestryRecord = self.nonaggregate_records_by_lidvid_str["_:_:_:b:clv:product::3.0"]
        v1_expected_parent_bundle_lidvids = {"_:_:_:b::3.0"}
        self.assertSetEqual(v1_expected_parent_bundle_lidvids, {str(lv) for lv in product_v3.parent_bundle_lidvids})


class AncestryMalformedDocsTestCase(unittest.TestCase):
    input_file_path = os.path.abspath(
        "./tests/pds/registrysweepers/test_ancestry_mock_AncestryMalformedDocsTestCase.json"
    )
    registry_query_mock = RegistryQueryMock(input_file_path)

    ancestry_records: List[AncestryRecord] = []
    bulk_updates: List[Tuple[str, Dict[str, List]]] = []

    def test_ancestry_completes_without_fatal_error(self):
        ancestry.run(
            client=None,
            registry_mock_query_f=self.registry_query_mock.get_mocked_query,
            ancestry_records_accumulator=self.ancestry_records,
            bulk_updates_sink=self.bulk_updates,
        )

        self.bundle_records = [r for r in self.ancestry_records if r.lidvid.is_bundle()]
        self.collection_records = [r for r in self.ancestry_records if r.lidvid.is_collection()]
        self.nonaggregate_records = [r for r in self.ancestry_records if r.lidvid.is_basic_product()]

        self.records_by_lidvid_str = {str(r.lidvid): r for r in self.ancestry_records}
        self.bundle_records_by_lidvid_str = {str(r.lidvid): r for r in self.ancestry_records if r.lidvid.is_bundle()}
        self.collection_records_by_lidvid_str = {
            str(r.lidvid): r for r in self.ancestry_records if r.lidvid.is_collection()
        }
        self.nonaggregate_records_by_lidvid_str = {
            str(r.lidvid): r for r in self.ancestry_records if r.lidvid.is_basic_product()
        }

        self.updates_by_lidvid_str = {id: content for id, content in self.bulk_updates}


class AncestryLegacyTypesTestCase(unittest.TestCase):
    input_file_path = os.path.abspath(
        "./tests/pds/registrysweepers/test_ancestry_mock_AncestryLegacyTypesTestCase.json"
    )
    registry_query_mock = RegistryQueryMock(input_file_path)

    def test_collection_refs_parsing(self):
        query_mock_f = self.registry_query_mock.get_mocked_query
        collection_ancestry_records = list(get_collection_ancestry_records(None, query_mock_f))

        self.assertEqual(1, len(collection_ancestry_records))

        expected_collection_lidvid = PdsLidVid.from_string("a:b:c:bundle:lidrefcollection::1.0")
        expected_parent_bundle_lidvid = PdsLidVid.from_string("a:b:c:bundle::1.0")
        expected_record = AncestryRecord(
            lidvid=expected_collection_lidvid,
            parent_collection_lidvids=set(),
            parent_bundle_lidvids={expected_parent_bundle_lidvid},
        )
        self.assertEqual(expected_record, collection_ancestry_records[0])


class AncestryMemoryOptimizedTestCase(unittest.TestCase):
    input_file_path = os.path.abspath(
        "./tests/pds/registrysweepers/test_ancestry_mock_AncestryMemoryOptimizedTestCase.json"
    )
    registry_query_mock = RegistryQueryMock(input_file_path)

    def test_ancestor_reference_aggregation(self):
        """
        Test that memory-optimized reimplementation of get_nonaggregate_ancestry_records() correctly aggregates
        references from queries.
        Does NOT test correctness those queries themselves, though those have been tested manually and are simple.
        """
        bundle = PdsLidVid.from_string("a:b:c:bundle::1.0")

        collection1_1 = PdsLidVid.from_string("a:b:c:bundle:first_collection::1.0")
        collection1_2 = PdsLidVid.from_string("a:b:c:bundle:first_collection::2.0")
        collection2_1 = PdsLidVid.from_string("a:b:c:bundle:second_collection::1.0")
        overlapping_collections = {collection1_1, collection2_1}
        nonoverlapping_collections = {collection1_2}
        collections = overlapping_collections.union(nonoverlapping_collections)

        product1_1 = PdsLidVid.from_string("a:b:c:bundle:first_collection:first_unique_product::1.0")
        product1_2 = PdsLidVid.from_string("a:b:c:bundle:first_collection:first_unique_product::2.0")
        product2_1 = PdsLidVid.from_string("a:b:c:bundle:first_collection:second_unique_product::1.0")
        product_common = PdsLidVid.from_string("a:b:c:bundle:first_collection:overlapping_product::1.0")

        collection_ancestry_records = [AncestryRecord(lidvid=c, parent_bundle_lidvids={bundle}) for c in collections]

        query_mock_f = self.registry_query_mock.get_mocked_query
        collection_ancestry_records = set(
            get_nonaggregate_ancestry_records(None, collection_ancestry_records, query_mock_f)
        )

        self.assertEqual(
            4,
            len(collection_ancestry_records),
            msg="Correct number of updates is produced.  Assumes no duplicate updates.",
        )
        expected_records = [
            AncestryRecord(
                lidvid=product1_1, parent_bundle_lidvids={bundle}, parent_collection_lidvids={collection1_1}
            ),
            AncestryRecord(
                lidvid=product1_2, parent_bundle_lidvids={bundle}, parent_collection_lidvids={collection1_2}
            ),
            AncestryRecord(
                lidvid=product2_1, parent_bundle_lidvids={bundle}, parent_collection_lidvids={collection2_1}
            ),
            AncestryRecord(
                lidvid=product_common, parent_bundle_lidvids={bundle}, parent_collection_lidvids=overlapping_collections
            ),
        ]

        for record in expected_records:
            self.assertIn(record, collection_ancestry_records, msg=f"Expected record is produced")


if __name__ == "__main__":
    unittest.main()
