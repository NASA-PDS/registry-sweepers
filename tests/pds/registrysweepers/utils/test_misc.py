import os
import unittest
from unittest import mock

from pds.registrysweepers.utils.misc import bin_elements
from pds.registrysweepers.utils.misc import coerce_list_type
from pds.registrysweepers.utils.misc import coerce_non_list_type
from pds.registrysweepers.utils.misc import FORCE_REPROCESS_ENV_VAR
from pds.registrysweepers.utils.misc import iterate_pages_of_size
from pds.registrysweepers.utils.misc import resolve_force_reprocess


class ResolveForceReprocessTestCase(unittest.TestCase):
    def test_disabled_by_default(self):
        with mock.patch.dict(os.environ, clear=False):
            os.environ.pop(FORCE_REPROCESS_ENV_VAR, None)
            self.assertFalse(resolve_force_reprocess())

    def test_enabled_by_cli_flag(self):
        with mock.patch.dict(os.environ, clear=False):
            os.environ.pop(FORCE_REPROCESS_ENV_VAR, None)
            self.assertTrue(resolve_force_reprocess(True))

    def test_enabled_by_environment_variable(self):
        with mock.patch.dict(os.environ, {FORCE_REPROCESS_ENV_VAR: "true"}):
            self.assertTrue(resolve_force_reprocess(False))

    def test_falsy_environment_variable_does_not_enable(self):
        with mock.patch.dict(os.environ, {FORCE_REPROCESS_ENV_VAR: "false"}):
            self.assertFalse(resolve_force_reprocess(False))

    def test_invalid_environment_variable_value_is_rejected(self):
        with mock.patch.dict(os.environ, {FORCE_REPROCESS_ENV_VAR: "maybe"}):
            with self.assertRaises(ValueError):
                resolve_force_reprocess(False)


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


if __name__ == "__main__":
    unittest.main()
