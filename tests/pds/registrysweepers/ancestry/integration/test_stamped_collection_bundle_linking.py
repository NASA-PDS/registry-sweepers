"""
Tests for linking a bundle to a collection which already carries the current ancestry version.

All registry access is mocked.  The mock client does not interpret query filters, so queries are distinguished here by
whether they exclude already-stamped collections (the pending-collections query) or not (the all-collections query).
"""
from typing import Dict
from typing import List
from typing import Optional

from pds.registrysweepers.ancestry import main
from pds.registrysweepers.ancestry.constants import ANCESTRY_REFS_METADATA_KEY
from pds.registrysweepers.ancestry.generation import process_collection_bundle_ancestry
from pds.registrysweepers.ancestry.utils import update_from_record
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY

from ..builders import CollectionRefsBuilder
from ..mock_opensearch import create_search_response
from ..mock_opensearch import query_matches_product_class

CURRENT = SWEEPERS_ANCESTRY_VERSION
OLD = SWEEPERS_ANCESTRY_VERSION - 1

BUNDLE_LID = "urn:nasa:pds:b"
BUNDLE_LIDVID = f"{BUNDLE_LID}::1.0"
COLLECTION_LID = "urn:nasa:pds:b:data"


def make_bundle(lidvid: str = BUNDLE_LIDVID, refs=(COLLECTION_LID,), version: Optional[int] = None) -> Dict:
    source = {"lidvid": lidvid, "product_class": "Product_Bundle", "ref_lid_collection": list(refs)}
    if version is not None:
        source[SWEEPERS_ANCESTRY_VERSION_METADATA_KEY] = version
    return {"_id": lidvid, "_source": source}


def make_collection(lidvid: str, version: Optional[int] = CURRENT, ancestor_refs=None) -> Dict:
    source: Dict = {"lidvid": lidvid, "product_class": "Product_Collection"}
    if version is not None:
        source[SWEEPERS_ANCESTRY_VERSION_METADATA_KEY] = version
    if ancestor_refs is not None:
        source[ANCESTRY_REFS_METADATA_KEY] = ancestor_refs
    return {"_id": lidvid, "_source": source}


def excludes_stamped_collections(query_body: Dict) -> bool:
    return bool(query_body.get("query", {}).get("bool", {}).get("must_not"))


def register_registry(client, bundles: List[Dict], collections: List[Dict], refs: Optional[List[Dict]] = None):
    """Register what the registry would return, honouring the version filter of the pending-collections query"""
    client.reset()
    client.register_search_response(
        index_pattern=".*registry-refs.*",
        query_matcher=lambda q: True,
        response_data=create_search_response(refs or []),
    )
    client.register_search_response(
        index_pattern=".*registry.*",
        query_matcher=lambda q: query_matches_product_class(q, "Product_Bundle"),
        response_data=create_search_response(bundles),
    )
    pending = [c for c in collections if c["_source"].get(SWEEPERS_ANCESTRY_VERSION_METADATA_KEY, 0) < CURRENT]
    client.register_search_response(
        index_pattern=".*registry.*",
        query_matcher=lambda q: query_matches_product_class(q, "Product_Collection") and excludes_stamped_collections(q),
        response_data=create_search_response(pending),
    )
    client.register_search_response(
        index_pattern=".*registry.*",
        query_matcher=lambda q: query_matches_product_class(q, "Product_Collection"),
        response_data=create_search_response(collections),
    )


def run_sweeper(client) -> Dict[str, List]:
    """Run the sweeper and return the updates it wrote, by document id (a pending collection is written more than once)"""
    updates: List = []
    main.run(client=client, bulk_updates_sink=updates)
    updates_by_id: Dict[str, List] = {}
    for update in updates:
        updates_by_id.setdefault(update.id, []).append(update)
    return updates_by_id


def refs_written(updates: List) -> List[str]:
    return [ref for update in updates for ref in update.content[ANCESTRY_REFS_METADATA_KEY]]


def records_by_product(records) -> Dict[str, object]:
    return {str(r.product): r for r in records if not r.skippable}


class TestStampedCollectionBundleLinking:
    def test_collection_stamped_before_bundle_loaded_is_linked(self, mock_opensearch_client):
        collection_lidvid = f"{COLLECTION_LID}::1.0"
        register_registry(
            mock_opensearch_client,
            bundles=[make_bundle()],
            collections=[make_collection(collection_lidvid, ancestor_refs=[])],
        )

        updates = run_sweeper(mock_opensearch_client)

        assert BUNDLE_LIDVID in refs_written(updates[collection_lidvid])

    def test_bundle_stamped_and_collection_stamped_empty_is_linked(self, mock_opensearch_client):
        collection_lidvid = f"{COLLECTION_LID}::1.0"
        register_registry(
            mock_opensearch_client,
            bundles=[make_bundle(version=CURRENT)],
            collections=[make_collection(collection_lidvid)],  # no ancestor_refs field at all
        )

        updates = run_sweeper(mock_opensearch_client)

        assert BUNDLE_LIDVID in refs_written(updates[collection_lidvid])
        assert BUNDLE_LIDVID not in updates  # the stamped bundle itself is not rewritten

    def test_collection_stamped_by_a_newer_sweeper_version_is_left_alone(self, mock_opensearch_client):
        collection_lidvid = f"{COLLECTION_LID}::1.0"
        register_registry(
            mock_opensearch_client,
            bundles=[make_bundle(version=CURRENT)],
            collections=[make_collection(collection_lidvid, version=CURRENT + 1, ancestor_refs=[])],
        )

        updates = run_sweeper(mock_opensearch_client)

        assert collection_lidvid not in updates  # linking would overwrite the newer stamp with the current one

    def test_already_linked_collection_is_not_written(self, mock_opensearch_client):
        collection_lidvid = f"{COLLECTION_LID}::1.0"
        register_registry(
            mock_opensearch_client,
            bundles=[make_bundle(version=CURRENT)],
            collections=[make_collection(collection_lidvid, ancestor_refs=[BUNDLE_LID, BUNDLE_LIDVID])],
        )

        assert run_sweeper(mock_opensearch_client) == {}

    def test_single_valued_ancestor_refs_field_is_understood(self, mock_opensearch_client):
        collection_lidvid = f"{COLLECTION_LID}::1.0"
        register_registry(
            mock_opensearch_client,
            bundles=[make_bundle(version=CURRENT)],
            collections=[make_collection(collection_lidvid, ancestor_refs=BUNDLE_LIDVID)],
        )

        assert run_sweeper(mock_opensearch_client) == {}

    def test_only_the_missing_bundle_is_added(self, mock_opensearch_client):
        collection_lidvid = f"{COLLECTION_LID}::1.0"
        other_bundle_lidvid = "urn:nasa:pds:other::1.0"
        register_registry(
            mock_opensearch_client,
            bundles=[make_bundle(version=CURRENT), make_bundle(other_bundle_lidvid, version=CURRENT)],
            collections=[make_collection(collection_lidvid, ancestor_refs=["urn:nasa:pds:other", other_bundle_lidvid])],
        )

        updates = run_sweeper(mock_opensearch_client)

        written = refs_written(updates[collection_lidvid])
        assert BUNDLE_LIDVID in written
        assert other_bundle_lidvid not in written

    def test_lid_reference_with_several_collection_versions(self, mock_opensearch_client):
        linked = f"{COLLECTION_LID}::1.0"
        stamped_empty = f"{COLLECTION_LID}::2.0"
        pending = f"{COLLECTION_LID}::3.0"
        register_registry(
            mock_opensearch_client,
            bundles=[make_bundle(version=CURRENT)],
            collections=[
                make_collection(linked, ancestor_refs=[BUNDLE_LID, BUNDLE_LIDVID]),
                make_collection(stamped_empty, ancestor_refs=[]),
                make_collection(pending, version=None),
            ],
        )

        updates = run_sweeper(mock_opensearch_client)

        assert linked not in updates
        assert BUNDLE_LIDVID in refs_written(updates[stamped_empty])
        assert BUNDLE_LIDVID in refs_written(updates[pending])

    def test_lidvid_reference_links_only_the_referenced_version(self, mock_opensearch_client):
        referenced = f"{COLLECTION_LID}::1.0"
        other = f"{COLLECTION_LID}::2.0"
        register_registry(
            mock_opensearch_client,
            bundles=[make_bundle(refs=[referenced], version=CURRENT)],
            collections=[make_collection(referenced, ancestor_refs=[]), make_collection(other, ancestor_refs=[])],
        )

        updates = run_sweeper(mock_opensearch_client)

        assert BUNDLE_LIDVID in refs_written(updates[referenced])
        assert other not in updates

    def test_bundle_without_registered_collection_writes_nothing_for_collections(self, mock_opensearch_client):
        unrelated = "urn:nasa:pds:unrelated:data::1.0"
        register_registry(
            mock_opensearch_client,
            bundles=[make_bundle()],
            collections=[make_collection(unrelated, ancestor_refs=[])],
        )

        updates = run_sweeper(mock_opensearch_client)

        assert unrelated not in updates
        assert set(updates) == {BUNDLE_LIDVID}  # only the pending bundle is stamped

    def test_collection_products_are_not_read_or_written(self, mock_opensearch_client):
        collection_lidvid = f"{COLLECTION_LID}::1.0"
        product_lidvid = f"{COLLECTION_LID}:p1::1.0"
        register_registry(
            mock_opensearch_client,
            bundles=[make_bundle(version=CURRENT)],
            collections=[make_collection(collection_lidvid, ancestor_refs=[])],
            refs=[CollectionRefsBuilder(collection_lidvid).with_product(product_lidvid).build()],
        )

        updates = run_sweeper(mock_opensearch_client)

        assert set(updates) == {collection_lidvid}
        assert mock_opensearch_client.get_search_calls_for_index(".*registry-refs.*") == []

    def test_stamped_collection_is_not_marked_complete_by_this_path(self, mock_opensearch_client):
        # The only difference between the update written to the stamped collection and any other collection update is
        # the added bundle; the stamp it already carries is unchanged.
        collection_lidvid = f"{COLLECTION_LID}::1.0"
        register_registry(
            mock_opensearch_client,
            bundles=[make_bundle(version=CURRENT)],
            collections=[make_collection(collection_lidvid, ancestor_refs=[])],
        )

        updates = run_sweeper(mock_opensearch_client)[collection_lidvid]

        assert len(updates) == 1
        assert updates[0].content[SWEEPERS_ANCESTRY_VERSION_METADATA_KEY] == CURRENT
        assert updates[0].inline_script_content is not None

    def test_write_uses_the_normal_update_for_a_record(self, mock_opensearch_client):
        collection_lidvid = f"{COLLECTION_LID}::1.0"
        register_registry(
            mock_opensearch_client,
            bundles=[make_bundle(version=CURRENT)],
            collections=[make_collection(collection_lidvid, ancestor_refs=[])],
        )

        records = records_by_product(process_collection_bundle_ancestry(mock_opensearch_client))

        assert [update_from_record(records[collection_lidvid])] == run_sweeper(mock_opensearch_client)[collection_lidvid]

    def test_second_run_writes_nothing(self, mock_opensearch_client):
        collection_lidvid = f"{COLLECTION_LID}::1.0"
        bundles = [make_bundle()]
        collections = [make_collection(collection_lidvid, ancestor_refs=[])]
        register_registry(mock_opensearch_client, bundles, collections)

        first_run = run_sweeper(mock_opensearch_client)
        assert set(first_run) == {collection_lidvid, BUNDLE_LIDVID}

        # apply the writes the way the deduplicating script does
        collections[0]["_source"][ANCESTRY_REFS_METADATA_KEY].extend(refs_written(first_run[collection_lidvid]))
        bundles[0]["_source"][SWEEPERS_ANCESTRY_VERSION_METADATA_KEY] = CURRENT
        register_registry(mock_opensearch_client, bundles, collections)

        assert run_sweeper(mock_opensearch_client) == {}

    def test_collection_with_old_stamp_still_uses_the_normal_path(self, mock_opensearch_client):
        collection_lidvid = f"{COLLECTION_LID}::1.0"
        register_registry(
            mock_opensearch_client,
            bundles=[make_bundle(version=CURRENT)],
            collections=[make_collection(collection_lidvid, version=OLD, ancestor_refs=[])],
            refs=[CollectionRefsBuilder(collection_lidvid).with_product(f"{COLLECTION_LID}:p1::1.0").build()],
        )

        updates = run_sweeper(mock_opensearch_client)

        assert BUNDLE_LIDVID in refs_written(updates[collection_lidvid])
        assert f"{COLLECTION_LID}:p1::1.0" in updates  # pending collections still process their products
