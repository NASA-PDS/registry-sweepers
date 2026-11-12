"""
DB-backed ancestry sweeper tests.

Each test follows: arrange (load seed docs) → act (ancestry.main.run) → refresh → assert.
"""
import pytest

from pds.registrysweepers.ancestry import main as ancestry_main
from pds.registrysweepers.ancestry.constants import ANCESTRY_REFS_METADATA_KEY
from pds.registrysweepers.ancestry.runtimeconstants import AncestryRuntimeConstants
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY

from tests.support.builders import (
    build_bundle,
    build_collection,
    build_product,
    CollectionRefsBuilder,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _source_by_id(docs: list, doc_id: str) -> dict:
    for d in docs:
        if d["_id"] == doc_id:
            return d["_source"]
    raise KeyError(f"doc {doc_id!r} not found in dump")


def _refs(source: dict) -> set:
    return set(source.get(ANCESTRY_REFS_METADATA_KEY, []))


# ---------------------------------------------------------------------------
# Scenario 1: basic hierarchy
# ---------------------------------------------------------------------------

def test_basic_hierarchy(registry_db):
    """Bundle → Collection → 2 Products — verify ancestry is set on all docs."""
    bundle_lidvid = "urn:nasa:pds:basic:bundle::1.0"
    collection_lid = "urn:nasa:pds:basic:collection"
    collection_lidvid = f"{collection_lid}::1.0"
    product_1 = f"{collection_lid}:p1::1.0"
    product_2 = f"{collection_lid}:p2::1.0"

    registry_db.load(
        [build_bundle(bundle_lidvid, collection_lid),
         build_collection(collection_lidvid),
         build_product(product_1),
         build_product(product_2)],
        index="registry",
    )
    registry_db.load([
        CollectionRefsBuilder(collection_lidvid)
            .with_product(product_1)
            .with_product(product_2)
            .to_seed(),
    ])

    ancestry_main.run(client=registry_db.client)
    registry_db.refresh()

    reg_docs = registry_db.dump("registry")

    bundle_src = _source_by_id(reg_docs, bundle_lidvid)
    assert bundle_src[SWEEPERS_ANCESTRY_VERSION_METADATA_KEY] == SWEEPERS_ANCESTRY_VERSION

    collection_src = _source_by_id(reg_docs, collection_lidvid)
    assert collection_src[SWEEPERS_ANCESTRY_VERSION_METADATA_KEY] == SWEEPERS_ANCESTRY_VERSION
    assert "urn:nasa:pds:basic:bundle" in _refs(collection_src)
    assert bundle_lidvid in _refs(collection_src)

    for prod_id in (product_1, product_2):
        prod_refs = _refs(_source_by_id(reg_docs, prod_id))
        assert collection_lid in prod_refs
        assert collection_lidvid in prod_refs


# ---------------------------------------------------------------------------
# Scenario 2: LID reference resolves to all collection versions
# ---------------------------------------------------------------------------

def test_lid_ref_resolves_to_all_collection_versions(registry_db):
    """Bundle with a LID-only collection ref gets ancestry written to both v1.0 and v2.0."""
    registry_db.load_json("ancestry/lid_ref_hierarchy.json")

    ancestry_main.run(client=registry_db.client)
    registry_db.refresh()

    reg_docs = registry_db.dump("registry")

    for version in ("1.0", "2.0"):
        coll_id = f"urn:nasa:pds:lid_ref_collection::{version}"
        coll_refs = _refs(_source_by_id(reg_docs, coll_id))
        assert "urn:nasa:pds:lid_ref_bundle" in coll_refs, f"v{version} missing bundle LID"
        assert "urn:nasa:pds:lid_ref_bundle::1.0" in coll_refs, f"v{version} missing bundle LIDVID"

    prod_a_refs = _refs(_source_by_id(reg_docs, "urn:nasa:pds:lid_ref_collection:product_a::1.0"))
    assert "urn:nasa:pds:lid_ref_collection" in prod_a_refs
    assert "urn:nasa:pds:lid_ref_collection::1.0" in prod_a_refs

    prod_b_refs = _refs(_source_by_id(reg_docs, "urn:nasa:pds:lid_ref_collection:product_b::1.0"))
    assert "urn:nasa:pds:lid_ref_collection" in prod_b_refs
    assert "urn:nasa:pds:lid_ref_collection::2.0" in prod_b_refs


# ---------------------------------------------------------------------------
# Scenario 3: multiple collection versions
# ---------------------------------------------------------------------------

def test_multiple_collection_versions(registry_db):
    """Two collection versions with separate products each get correct ancestry."""
    bundle_lidvid = "urn:nasa:pds:multi_version:bundle::1.0"
    collection_lid = "urn:nasa:pds:multi_version:collection"
    coll_v1 = f"{collection_lid}::1.0"
    coll_v2 = f"{collection_lid}::2.0"
    prod_v1 = f"{collection_lid}:p1::1.0"
    prod_v2 = f"{collection_lid}:p1::2.0"

    registry_db.load(
        [build_bundle(bundle_lidvid, collection_lid),
         build_collection(coll_v1),
         build_collection(coll_v2),
         build_product(prod_v1),
         build_product(prod_v2)],
        index="registry",
    )
    registry_db.load([
        CollectionRefsBuilder(coll_v1).with_product(prod_v1).to_seed(),
        CollectionRefsBuilder(coll_v2).with_product(prod_v2).to_seed(),
    ])

    ancestry_main.run(client=registry_db.client)
    registry_db.refresh()

    reg_docs = registry_db.dump("registry")

    for coll_id in (coll_v1, coll_v2):
        coll_refs = _refs(_source_by_id(reg_docs, coll_id))
        assert bundle_lidvid in coll_refs
        assert "urn:nasa:pds:multi_version:bundle" in coll_refs

    p1_refs = _refs(_source_by_id(reg_docs, prod_v1))
    assert coll_v1 in p1_refs and collection_lid in p1_refs

    p2_refs = _refs(_source_by_id(reg_docs, prod_v2))
    assert coll_v2 in p2_refs and collection_lid in p2_refs


# ---------------------------------------------------------------------------
# Scenario 4: refs pagination
# ---------------------------------------------------------------------------

def test_refs_pagination(registry_db, monkeypatch):
    """With page_size=1, multiple ref-query pages are needed; all products still get ancestry."""
    monkeypatch.setattr(AncestryRuntimeConstants, "nonaggregate_ancestry_records_query_page_size", 1)

    registry_db.load_json("ancestry/paginated_refs.json")

    ancestry_main.run(client=registry_db.client)
    registry_db.refresh()

    reg_docs = registry_db.dump("registry")
    collection_lid = "urn:nasa:pds:paginated_collection"
    collection_lidvid = f"{collection_lid}::1.0"

    for i in range(1, 4):
        prod_id = f"{collection_lid}:product_{i}::1.0"
        prod_refs = _refs(_source_by_id(reg_docs, prod_id))
        assert collection_lidvid in prod_refs, f"product_{i} missing collection LIDVID ref"
        assert collection_lid in prod_refs, f"product_{i} missing collection LID ref"


# ---------------------------------------------------------------------------
# Scenario 5: idempotency
# ---------------------------------------------------------------------------

def test_idempotency(registry_db):
    """Running the sweeper twice produces the same result as running it once."""
    bundle_lidvid = "urn:nasa:pds:idem:bundle::1.0"
    collection_lid = "urn:nasa:pds:idem:collection"
    collection_lidvid = f"{collection_lid}::1.0"
    product_id = f"{collection_lid}:p1::1.0"

    registry_db.load(
        [build_bundle(bundle_lidvid, collection_lid),
         build_collection(collection_lidvid),
         build_product(product_id)],
        index="registry",
    )
    registry_db.load([CollectionRefsBuilder(collection_lidvid).with_product(product_id).to_seed()])

    ancestry_main.run(client=registry_db.client)
    registry_db.refresh()
    ancestry_main.run(client=registry_db.client)
    registry_db.refresh()

    reg_docs = registry_db.dump("registry")

    prod_refs = _refs(_source_by_id(reg_docs, product_id))
    assert prod_refs == {collection_lid, collection_lidvid}

    coll_refs = _refs(_source_by_id(reg_docs, collection_lidvid))
    assert coll_refs == {"urn:nasa:pds:idem:bundle", bundle_lidvid}

    assert _source_by_id(reg_docs, bundle_lidvid)[SWEEPERS_ANCESTRY_VERSION_METADATA_KEY] == SWEEPERS_ANCESTRY_VERSION


# ---------------------------------------------------------------------------
# Scenario 6: deferred updates (orphan processing)
# ---------------------------------------------------------------------------

def test_deferred_updates(registry_db):
    """
    Product exists in registry-refs but NOT in registry during first run.
    Its update is deferred.  After the product is added to registry, a second
    run applies the deferred ancestry.
    """
    collection_lid = "urn:nasa:pds:deferred:collection"
    collection_lidvid = f"{collection_lid}::1.0"
    bundle_lidvid = "urn:nasa:pds:deferred:bundle::1.0"
    orphan_product_id = f"{collection_lid}:orphan::1.0"

    registry_db.load(
        [build_bundle(bundle_lidvid, collection_lid),
         build_collection(collection_lidvid)],
        index="registry",
    )
    registry_db.load([
        CollectionRefsBuilder(collection_lidvid).with_product(orphan_product_id).to_seed(),
    ])
    # orphan_product_id intentionally NOT in registry yet

    ancestry_main.run(client=registry_db.client)
    registry_db.refresh()

    deferred_docs = registry_db.dump("deferred-updates")
    deferred_ids = {d["_id"] for d in deferred_docs}
    assert orphan_product_id in deferred_ids

    # Add the product to registry and re-run
    registry_db.load([build_product(orphan_product_id)], index="registry", validate=False)
    ancestry_main.run(client=registry_db.client)
    registry_db.refresh()

    reg_docs = registry_db.dump("registry")
    prod_refs = _refs(_source_by_id(reg_docs, orphan_product_id))
    assert collection_lidvid in prod_refs
    assert collection_lid in prod_refs


# ---------------------------------------------------------------------------
# Scenario 7: empty database
# ---------------------------------------------------------------------------

def test_empty_database(registry_db):
    """Sweeper completes without error on an empty registry."""
    ancestry_main.run(client=registry_db.client)
    registry_db.refresh()

    assert registry_db.dump("registry") == []
    assert registry_db.dump("registry-refs") == []
