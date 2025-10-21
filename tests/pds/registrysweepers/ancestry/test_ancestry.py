import itertools
import logging
import os.path
import tempfile
import unittest
from typing import Dict
from typing import List
from typing import Tuple

from pds.registrysweepers.ancestry import main as ancestry
from pds.registrysweepers.ancestry.ancestryrecord import AncestryRecord
from pds.registrysweepers.ancestry.constants import METADATA_PARENT_BUNDLE_KEY
from pds.registrysweepers.ancestry.constants import METADATA_PARENT_COLLECTION_KEY
from pds.registrysweepers.ancestry.generation import generate_nonaggregate_and_collection_records_iteratively
from pds.registrysweepers.ancestry.generation import get_collection_ancestry_records
from pds.registrysweepers.ancestry.main import generate_deferred_updates
from pds.registrysweepers.ancestry.main import generate_updates
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY
from pds.registrysweepers.utils import configure_logging
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid

from tests.mocks.registryquerymock import RegistryQueryMock


class AncestryBasicTestCase(unittest.TestCase):
    input_file_path = os.path.abspath(
        "./tests/pds/registrysweepers/ancestry/resources/test_ancestry_mock_AncestryFunctionalTestCase.json"
    )
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

    def test_correct_record_counts(self):
        self.assertEqual(1, len(self.bundle_records))
        self.assertEqual(4, len(self.collection_records))
        self.assertEqual(6, len(self.nonaggregate_records))

    def test_correct_update_counts(self):
        self.assertEqual(11, len(self.updates_by_lidvid_str))

    def test_bundles_have_no_ancestry(self):
        for record in self.bundle_records:
            self.assertTrue(len(record.resolve_parent_bundle_lidvids()) == 0)
            self.assertTrue(len(record.resolve_parent_collection_lidvids()) == 0)

    def test_collections_have_no_collection_ancestry(self):
        for record in self.collection_records:
            self.assertTrue(len(record.resolve_parent_collection_lidvids()) == 0)

    def test_collections_have_correct_bundle_ancestry(self):
        for record in self.collection_records:
            expected_bundle_ancestry = set(self.expected_bundle_ancestry_by_collection[str(record.lidvid)])
            self.assertEqual(expected_bundle_ancestry, set(str(id) for id in record.resolve_parent_bundle_lidvids()))

    def test_nonaggregates_have_correct_collection_ancestry(self):
        for record in self.nonaggregate_records:
            expected_collection_ancestry = set(self.expected_collection_ancestry_by_nonaggregate[str(record.lidvid)])
            self.assertEqual(
                expected_collection_ancestry, set(str(id) for id in record.resolve_parent_collection_lidvids())
            )

    def test_nonaggregates_have_correct_bundle_ancestry(self):
        print(
            "#### N.B. This test will always fail if test_nonaggregates_have_correct_collection_ancestry() fails! ####"
        )
        for record in self.nonaggregate_records:
            parent_collection_id_strs = set(str(id) for id in record.resolve_parent_collection_lidvids())
            parent_bundle_id_strs = set(str(id) for id in record.resolve_parent_bundle_lidvids())
            expected_bundle_id_strs = set(
                itertools.chain(*[self.expected_bundle_ancestry_by_collection[id] for id in parent_collection_id_strs])
            )
            self.assertEqual(expected_bundle_id_strs, parent_bundle_id_strs)

    def test_correct_bulk_update_kvs_are_produced(self):
        for record in self.ancestry_records:
            update = self.updates_by_lidvid_str[str(record.lidvid)]
            self.assertEqual(
                set(str(lidvid) for lidvid in record.resolve_parent_bundle_lidvids()),
                set(update["ops:Provenance/ops:parent_bundle_identifier"]),
            )
            self.assertEqual(
                set(str(lidvid) for lidvid in record.resolve_parent_collection_lidvids()),
                set(update["ops:Provenance/ops:parent_collection_identifier"]),
            )

            self.assertEqual(SWEEPERS_ANCESTRY_VERSION, update[SWEEPERS_ANCESTRY_VERSION_METADATA_KEY])

        for doc_id, update in self.bulk_updates:
            record = self.records_by_lidvid_str[doc_id]
            self.assertEqual(
                set(update["ops:Provenance/ops:parent_bundle_identifier"]),
                set(str(lidvid) for lidvid in record.resolve_parent_bundle_lidvids()),
            )
            self.assertEqual(
                set(update["ops:Provenance/ops:parent_collection_identifier"]),
                set(str(lidvid) for lidvid in record.resolve_parent_collection_lidvids()),
            )

            self.assertEqual(SWEEPERS_ANCESTRY_VERSION, update[SWEEPERS_ANCESTRY_VERSION_METADATA_KEY])


class AncestryMalformedDocsTestCase(unittest.TestCase):
    input_file_path = os.path.abspath(
        "./tests/pds/registrysweepers/ancestry/resources/test_ancestry_mock_AncestryMalformedDocsTestCase.json"
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

        self.assertEqual(1, len(self.bundle_records))
        self.assertEqual(1, len(self.collection_records))
        self.assertEqual(2, len(self.nonaggregate_records))

        self.updates_by_lidvid_str = {id: content for id, content in self.bulk_updates}


class AncestryLegacyTypesTestCase(unittest.TestCase):
    input_file_path = os.path.abspath(
        "./tests/pds/registrysweepers/ancestry/resources/test_ancestry_mock_AncestryLegacyTypesTestCase.json"
    )
    registry_query_mock = RegistryQueryMock(input_file_path)

    def test_collection_refs_parsing(self):
        query_mock_f = self.registry_query_mock.get_mocked_query
        collection_ancestry_records = list(get_collection_ancestry_records(None, [], registry_db_mock=query_mock_f))

        self.assertEqual(1, len(collection_ancestry_records))

        expected_collection_lidvid = PdsLidVid.from_string("a:b:c:bundle:lidrefcollection::1.0")
        expected_parent_bundle_lidvid = PdsLidVid.from_string("a:b:c:bundle::1.0")
        expected_record = AncestryRecord(
            lidvid=expected_collection_lidvid,
            explicit_parent_collection_lidvids=set(),
            explicit_parent_bundle_lidvids={expected_parent_bundle_lidvid},
        )
        self.assertEqual(expected_record, collection_ancestry_records[0])


# TODO: reimplement to reflect latest memory optimisations - edunn 20250723
# class AncestryMemoryOptimizedTestCase(unittest.TestCase):
#     input_file_path = os.path.abspath(
#         "./tests/pds/registrysweepers/ancestry/resources/test_ancestry_mock_AncestryMemoryOptimizedTestCase.json"
#     )
#     registry_query_mock = RegistryQueryMock(input_file_path)
#
#     def test_ancestor_reference_aggregation(self):
#         """
#         Test that memory-optimized reimplementation of get_nonaggregate_ancestry_records() correctly aggregates
#         references from queries.
#         Does NOT test correctness those queries themselves, though those have been tested manually and are simple.
#         """
#         bundle = PdsLidVid.from_string("a:b:c:bundle::1.0")
#
#         collection1_1 = PdsLidVid.from_string("a:b:c:bundle:first_collection::1.0")
#         collection1_2 = PdsLidVid.from_string("a:b:c:bundle:first_collection::2.0")
#         collection2_1 = PdsLidVid.from_string("a:b:c:bundle:second_collection::1.0")
#         overlapping_collections = {collection1_1, collection2_1}
#         nonoverlapping_collections = {collection1_2}
#         collections = overlapping_collections.union(nonoverlapping_collections)
#
#         product1_1 = PdsLidVid.from_string("a:b:c:bundle:first_collection:first_unique_product::1.0")
#         product1_2 = PdsLidVid.from_string("a:b:c:bundle:first_collection:first_unique_product::2.0")
#         product2_1 = PdsLidVid.from_string("a:b:c:bundle:first_collection:second_unique_product::1.0")
#         product_common = PdsLidVid.from_string("a:b:c:bundle:first_collection:overlapping_product::1.0")
#
#         collection_ancestry_records = [AncestryRecord(lidvid=c, explicit_parent_bundle_lidvids={bundle}) for c in collections]
#
#         query_mock_f = self.registry_query_mock.get_mocked_query
#         collection_ancestry_records = set(
#             get_nonaggregate_ancestry_records(None, collection_ancestry_records, query_mock_f)
#         )
#
#         self.assertEqual(
#             4,
#             len(collection_ancestry_records),
#             msg="Correct number of updates is produced.  Assumes no duplicate updates.",
#         )
#         expected_records = [
#             AncestryRecord(
#                 lidvid=product1_1, parent_bundle_lidvids={bundle}, parent_collection_lidvids={collection1_1}
#             ),
#             AncestryRecord(
#                 lidvid=product1_2, parent_bundle_lidvids={bundle}, parent_collection_lidvids={collection1_2}
#             ),
#             AncestryRecord(
#                 lidvid=product2_1, parent_bundle_lidvids={bundle}, parent_collection_lidvids={collection2_1}
#             ),
#             AncestryRecord(
#                 lidvid=product_common, parent_bundle_lidvids={bundle}, parent_collection_lidvids=overlapping_collections
#             ),
#         ]
#
#         for record in expected_records:
#             self.assertIn(record, collection_ancestry_records, msg=f"Expected record is produced")


class AncestryDeferredPartialUpdatesTestCase(unittest.TestCase):
    input_file_path = os.path.abspath(
        "./tests/pds/registrysweepers/ancestry/resources/test_ancestry_mock_AncestryDeferredPartialUpdatesTestCase.json"
    )
    registry_query_mock = RegistryQueryMock(input_file_path)

    def test_ancestor_partial_history_accumulation(self):
        """
        TODO: document
        """

        configure_logging(filepath=None, log_level=logging.DEBUG)

        mb = PdsLidVid.from_string("a:b:c:matching_bundle::1.0")
        nmb = PdsLidVid.from_string("a:b:c:nonmatching_bundle::1.0")
        mc = PdsLidVid.from_string("a:b:c:matching_bundle:matching_collection::1.0")
        nmc = PdsLidVid.from_string("a:b:c:nonmatching_bundle:nonmatching_collection::1.0")

        mup1 = PdsLidVid.from_string(
            "a:b:c:matching_bundle:matching_collection:matching_collection_unique_product_1::1.0"
        )
        mup2 = PdsLidVid.from_string(
            "a:b:c:matching_bundle:matching_collection:matching_collection_unique_product_2::1.0"
        )
        nmup1 = PdsLidVid.from_string(
            "a:b:c:nonmatching_bundle:nonmatching_collection:nonmatching_collection_unique_product_1::1.0"
        )
        nmup2 = PdsLidVid.from_string(
            "a:b:c:nonmatching_bundle:nonmatching_collection:nonmatching_collection_unique_product_2::1.0"
        )
        op = PdsLidVid.from_string("a:b:c:matching_bundle:matching_collection:overlapping_product::1.0")

        query_mock_f = self.registry_query_mock.get_mocked_query
        collection_ancestry_records = [
            AncestryRecord(lidvid=mc, explicit_parent_bundle_lidvids={mb}, explicit_parent_collection_lidvids=set()),
            AncestryRecord(lidvid=nmc, explicit_parent_bundle_lidvids={nmb}, explicit_parent_collection_lidvids=set()),
        ]

        collection_and_nonaggregate_records = list(
            generate_nonaggregate_and_collection_records_iteratively(None, collection_ancestry_records, query_mock_f)
        )

        deferred_records_file = tempfile.NamedTemporaryFile(mode="w+", delete=False)
        non_deferred_updates = list(
            generate_updates(collection_and_nonaggregate_records, deferred_records_file.name, None, None)
        )
        deferred_updates = list(generate_deferred_updates(None, deferred_records_file.name, query_mock_f))
        updates = non_deferred_updates + deferred_updates
        os.remove(deferred_records_file.name)

        # TODO: increase to two nonmatching collections and two shared products

        incomplete_opu1 = next(
            u for u in updates if u.id == str(op) and len(u.content[METADATA_PARENT_COLLECTION_KEY]) == 1
        )
        self.assertIn(str(mb), incomplete_opu1.content[METADATA_PARENT_BUNDLE_KEY])
        self.assertNotIn(str(nmb), incomplete_opu1.content[METADATA_PARENT_BUNDLE_KEY])
        self.assertIn(str(mc), incomplete_opu1.content[METADATA_PARENT_COLLECTION_KEY])
        self.assertNotIn(str(nmc), incomplete_opu1.content[METADATA_PARENT_COLLECTION_KEY])

        opu1 = next(u for u in updates if u.id == str(op) and len(u.content[METADATA_PARENT_COLLECTION_KEY]) > 1)
        self.assertIn(str(mb), opu1.content[METADATA_PARENT_BUNDLE_KEY])
        self.assertIn(str(nmb), opu1.content[METADATA_PARENT_BUNDLE_KEY])
        self.assertIn(str(mc), opu1.content[METADATA_PARENT_COLLECTION_KEY])
        self.assertIn(str(nmc), opu1.content[METADATA_PARENT_COLLECTION_KEY])


if __name__ == "__main__":
    unittest.main()
