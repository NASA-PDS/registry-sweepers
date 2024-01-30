import unittest

from pds.registrysweepers.utils.misc import coerce_list_type
from pds.registrysweepers.utils.misc import coerce_non_list_type
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


if __name__ == "__main__":
    unittest.main()
