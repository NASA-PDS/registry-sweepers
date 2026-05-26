import unittest

from pds.registrysweepers.legacy_registry_sync.solr_doc_export_to_opensearch import SolrOsWrapperIter
from pds.registrysweepers.legacy_registry_sync.solr_doc_export_to_opensearch import UNKNOWN_NODE

TEST_INDEX = "test-legacy-registry"


def _make_wrapper(found_ids=None, online_resources=None, force=False):
    return SolrOsWrapperIter([], TEST_INDEX, found_ids=found_ids, online_resources=online_resources, force=force)


class TestGetNodeForceFlag(unittest.TestCase):
    """Tests for the force flag behavior in _get_node."""

    LIDVID = "urn:nasa:pds:bundle:collection:product::1.0"
    STORED_NODE = "PDS_GEO"

    def _doc_with_resource_url(self):
        # pds-ppi.igpp.ucla.edu maps to PDS_PPI in NODE_DOMAINS
        return {
            "lid": "urn:nasa:pds:bundle:collection:product",
            "lidvid": self.LIDVID,
            "resource_url": ["https://pds-ppi.igpp.ucla.edu/data/something"],
        }

    def test_preserves_existing_node_by_default(self):
        """Without force, an existing registry node is returned unchanged."""
        found_ids = {self.LIDVID: self.STORED_NODE}
        wrapper = _make_wrapper(found_ids=found_ids)
        node = wrapper._get_node(self._doc_with_resource_url())
        self.assertEqual(self.STORED_NODE, node)

    def test_force_overrides_existing_node(self):
        """With force=True, the heuristic chain runs even when a registry node exists."""
        found_ids = {self.LIDVID: self.STORED_NODE}
        wrapper = _make_wrapper(found_ids=found_ids, force=True)
        node = wrapper._get_node(self._doc_with_resource_url())
        # Should be derived from resource_url domain, not the stored value
        self.assertEqual("PDS_PPI", node)
        self.assertNotEqual(self.STORED_NODE, node)

    def test_none_registry_node_falls_through_to_heuristics(self):
        """Without force, a None registry node still triggers heuristic derivation."""
        found_ids = {self.LIDVID: None}
        wrapper = _make_wrapper(found_ids=found_ids)
        node = wrapper._get_node(self._doc_with_resource_url())
        self.assertEqual("PDS_PPI", node)

    def test_missing_lidvid_falls_through_to_heuristics(self):
        """Without force, a doc with no lidvid falls through to heuristics."""
        doc = {"lid": "urn:nasa:pds:bundle:collection:product", "resource_url": ["https://pds-ppi.igpp.ucla.edu/data/"]}
        wrapper = _make_wrapper(found_ids={})
        node = wrapper._get_node(doc)
        self.assertEqual("PDS_PPI", node)

    def test_force_true_default_is_false(self):
        """force defaults to False."""
        wrapper = _make_wrapper()
        self.assertFalse(wrapper.force)

    def test_force_true_stored(self):
        """force=True is stored on the instance."""
        wrapper = _make_wrapper(force=True)
        self.assertTrue(wrapper.force)


class TestGetNodeHeuristics(unittest.TestCase):
    """Tests for the heuristic node-derivation chain."""

    def _wrapper(self, online_resources=None):
        return _make_wrapper(found_ids={}, online_resources=online_resources)

    def test_esa_agency(self):
        doc = {"lid": "urn:esa:psa:mission", "agency_name": ["esa"]}
        self.assertEqual("PSA", self._wrapper()._get_node(doc))

    def test_unknown_agency(self):
        doc = {"lid": "urn:x:y:z", "agency_name": ["Unknown"]}
        self.assertEqual(UNKNOWN_NODE, self._wrapper()._get_node(doc))

    def test_eng_product_class(self):
        doc = {"lid": "urn:nasa:pds:context:target:planet", "product_class": ["Product_Context"]}
        self.assertEqual("PDS_ENG", self._wrapper()._get_node(doc))

    def test_unk_product_class(self):
        doc = {"lid": "urn:nasa:pds:x", "product_class": ["Product_Zipped"]}
        self.assertEqual(UNKNOWN_NODE, self._wrapper()._get_node(doc))

    def test_resource_url_known_domain(self):
        doc = {"lid": "urn:nasa:pds:x", "resource_url": ["https://pds-geosciences.wustl.edu/data/"]}
        self.assertEqual("PDS_GEO", self._wrapper()._get_node(doc))

    def test_resource_url_unknown_domain(self):
        # Unknown domain falls through; no node_id or file_ref_url either → UNK
        doc = {"lid": "urn:nasa:pds:x", "resource_url": ["https://unknown.example.com/data/"]}
        self.assertEqual(UNKNOWN_NODE, self._wrapper()._get_node(doc))

    def test_resource_ref_resolved_via_online_resources(self):
        lidvid_ref = "urn:nasa:pds:bundle:collection::1.0"
        lid_ref = "urn:nasa:pds:bundle:collection"
        online_resources = {lid_ref: "https://pds-atmospheres.nmsu.edu/data/resource"}
        doc = {"lid": "urn:nasa:pds:x", "resource_ref": [lidvid_ref]}
        self.assertEqual("PDS_ATM", self._wrapper(online_resources=online_resources)._get_node(doc))

    def test_node_id_lookup(self):
        doc = {"lid": "urn:nasa:pds:x", "node_id": ["Geosciences"]}
        self.assertEqual("PDS_GEO", self._wrapper()._get_node(doc))

    def test_file_ref_url_fallback(self):
        # get_node_from_file_ref uses dirs[6] after splitting on "/";
        # a URL like https://host/a/b/c/geo/... splits to dirs[6]=="geo"
        url = "https://pds.nasa.gov/data/pds4/releases/geo/bundle/file.xml"
        doc = {"lid": "urn:nasa:pds:x", "file_ref_url": [url]}
        self.assertEqual("PDS_GEO", self._wrapper()._get_node(doc))

    def test_no_clues_returns_unknown(self):
        doc = {"lid": "urn:nasa:pds:x"}
        self.assertEqual(UNKNOWN_NODE, self._wrapper()._get_node(doc))


if __name__ == "__main__":
    unittest.main()
