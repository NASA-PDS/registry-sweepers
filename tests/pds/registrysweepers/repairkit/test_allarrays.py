import unittest

from pds.registrysweepers.repairkit import allarrays


class AllArrays(unittest.TestCase):
    def test_valid_field(self):
        src = {"apple": ["orange"]}
        repair = allarrays.repair(src, "apple")
        self.assertEqual({}, repair)

    def test_invalid_field(self):
        src = {"apple": "orange"}
        repair = allarrays.repair(src, "apple")
        self.assertEqual({"apple": ["orange"]}, repair)

    def test_exclusion_logic(self):
        src = {
            "lid": "urn:nasa:pds:clementine_lwir_bt:data_flatfield:ff034ag.img",
            "vid": "1.0",
            "lidvid": "urn:nasa:pds:clementine_lwir_bt:data_flatfield:ff034ag.img::1.0",
            "title": "Clementine LWIR brightness temperature flat field product: ff034ag.img",
            "product_class": "Product_Observational",
            "_package_id": "c0491371-49f8-4e34-9d1c-f94ef1217b57",
            "ops:Provenance/ops:parent_collection_identifier": ["urn:nasa:pds:clementine_lwir_bt:data_flatfield::1.0"],
            "ops:Provenance/ops:parent_bundle_identifier": ["urn:nasa:pds:clementine_lwir_bt::1.0"],
            "ops:Provenance/ops:registry_sweepers_repairkit_version": 2,
            "ops:Provenance/someStringTypedProp": "someValue",
        }

        repairs = {}

        for fieldname in src:
            if fieldname not in allarrays.EXCLUDED_PROPERTIES:
                repairs.update(allarrays.repair(src, fieldname))

        self.assertDictEqual({}, repairs, "test that excluded (string-typed) fields do not result in repair changes")


if __name__ == "__main__":
    unittest.main()
