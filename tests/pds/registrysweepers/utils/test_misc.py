import unittest

from pds.registrysweepers.utils.misc import bin_elements
from pds.registrysweepers.utils.misc import build_nested_update
from pds.registrysweepers.utils.misc import coerce_list_type
from pds.registrysweepers.utils.misc import coerce_non_list_type
from pds.registrysweepers.utils.misc import get_nested_attr
from pds.registrysweepers.utils.misc import has_nested_attr
from pds.registrysweepers.utils.misc import iterate_pages_of_size


class IteratePagesOfTestCase(unittest.TestCase):
    def test_basic_functionality(self):
        page_size = 2
        input = [1, 2, 3, 4, 5, 6]
        output = list(iterate_pages_of_size(page_size, input))
        expected = [[1, 2], [3, 4], [5, 6]]
        self.assertListEqual(expected, output)

    def test_partial_final_page(self):
        page_size = 2
        input = [1, 2, 3]
        output = list(iterate_pages_of_size(page_size, input))
        expected = [[1, 2], [3]]
        self.assertListEqual(expected, output)

    def test_empty_input(self):
        self.assertEqual([], list(iterate_pages_of_size(1, [])))

    def test_invalid_page_size(self):
        self.assertRaises(ValueError, lambda: list(iterate_pages_of_size(0, [1, 2, 3])))


class CoerceListTypeTestCase(unittest.TestCase):
    def test_basic_behaviour(self):
        value = "value"
        self.assertListEqual([value], coerce_list_type(value))

    def test_noop(self):
        arr_value = ["value"]
        self.assertListEqual(arr_value, coerce_list_type(arr_value))


class CoerceNonListTypeTestCase(unittest.TestCase):
    def test_basic_behaviour(self):
        value = "value"
        arr_value = [value]
        self.assertEqual(value, coerce_non_list_type(arr_value))

    def test_noop(self):
        value = "value"
        self.assertEqual(value, coerce_non_list_type(value))

    def test_non_singleton(self):
        non_singleton = ["some", "values"]
        with self.assertRaises(ValueError):
            coerce_non_list_type(non_singleton)

    def test_unsupported_null(self):
        non_singleton = []
        with self.assertRaises(ValueError):
            coerce_non_list_type(non_singleton)

    def test_null_support(self):
        arr_null = []
        self.assertEqual(None, coerce_non_list_type(arr_null, support_null=True))


class BinElementsTestCase(unittest.TestCase):
    def test_basic_functionality(self):
        elements = ["here", "are", "some", "strings"]

        strings_by_first_letter = bin_elements(elements, lambda s: s[0])
        self.assertSetEqual({"here"}, set(strings_by_first_letter["h"]))
        self.assertSetEqual({"are"}, set(strings_by_first_letter["a"]))
        self.assertSetEqual({"some", "strings"}, set(strings_by_first_letter["s"]))


class HasNestedAttrTestCase(unittest.TestCase):
    def test_flat_key_present(self):
        self.assertTrue(has_nested_attr({"k": "v"}, "k"))

    def test_flat_key_absent(self):
        self.assertFalse(has_nested_attr({}, "k"))

    def test_nested_key_present(self):
        src = {"ops:Provenance": {"ops:superseded_by": None}}
        self.assertTrue(has_nested_attr(src, "ops:Provenance/ops:superseded_by"))

    def test_nested_key_present_with_none_value(self):
        # Must return True even when the value is None — distinguishes "set to None" from "absent"
        src = {"ops:Provenance": {"ops:superseded_by": None}}
        self.assertTrue(has_nested_attr(src, "ops:Provenance/ops:superseded_by"))

    def test_nested_key_absent_parent_missing(self):
        self.assertFalse(has_nested_attr({}, "ops:Provenance/ops:superseded_by"))

    def test_nested_key_absent_child_missing(self):
        self.assertFalse(has_nested_attr({"ops:Provenance": {}}, "ops:Provenance/ops:superseded_by"))

    def test_non_dict_parent_returns_false(self):
        self.assertFalse(has_nested_attr({"ops:Provenance": "not-a-dict"}, "ops:Provenance/ops:superseded_by"))


class GetNestedAttrTestCase(unittest.TestCase):
    def test_flat_key(self):
        self.assertEqual("v", get_nested_attr({"k": "v"}, "k"))

    def test_two_level_path(self):
        src = {"ops:Provenance": {"ops:superseded_by": "urn:foo"}}
        self.assertEqual("urn:foo", get_nested_attr(src, "ops:Provenance/ops:superseded_by"))

    def test_missing_parent_returns_default(self):
        self.assertIsNone(get_nested_attr({}, "ops:Provenance/ops:superseded_by"))

    def test_missing_child_returns_default(self):
        src = {"ops:Provenance": {}}
        self.assertIsNone(get_nested_attr(src, "ops:Provenance/ops:superseded_by"))

    def test_non_dict_parent_returns_default(self):
        src = {"ops:Provenance": "not-a-dict"}
        self.assertEqual("fallback", get_nested_attr(src, "ops:Provenance/ops:superseded_by", "fallback"))

    def test_explicit_default(self):
        self.assertEqual(0, get_nested_attr({}, "ops:Sweepers/ops:version", 0))


class BuildNestedUpdateTestCase(unittest.TestCase):
    def test_flat_path_passthrough(self):
        self.assertEqual({"lidvid": "urn:foo"}, build_nested_update({"lidvid": "urn:foo"}))

    def test_two_level_path(self):
        result = build_nested_update({"ops:Provenance/ops:superseded_by": None})
        self.assertEqual({"ops:Provenance": {"ops:superseded_by": None}}, result)

    def test_sibling_paths_merged_under_shared_parent(self):
        result = build_nested_update({
            "ops:Provenance/ops:superseded_by": None,
            "ops:Provenance/ops:registry_sweepers_provenance_hotfixed_version": 2,
        })
        self.assertEqual(
            {"ops:Provenance": {
                "ops:superseded_by": None,
                "ops:registry_sweepers_provenance_hotfixed_version": 2,
            }},
            result,
        )

    def test_mixed_flat_and_nested(self):
        result = build_nested_update({
            "lidvid": "urn:foo",
            "ops:Provenance/ops:superseded_by": None,
        })
        self.assertEqual({"lidvid": "urn:foo", "ops:Provenance": {"ops:superseded_by": None}}, result)

    def test_different_parents_not_merged(self):
        result = build_nested_update({
            "ops:Provenance/ops:superseded_by": None,
            "ops:Harvest_Info/ops:harvest_date_time": "2025-01-01",
        })
        self.assertEqual(
            {
                "ops:Provenance": {"ops:superseded_by": None},
                "ops:Harvest_Info": {"ops:harvest_date_time": "2025-01-01"},
            },
            result,
        )


if __name__ == "__main__":
    unittest.main()
