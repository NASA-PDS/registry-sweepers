"""
DB-backed ancestry sweeper tests.

Each test follows: arrange (load seed docs) → act (ancestry.main.run) → refresh → assert.
Assertions are targeted checks for the behaviour under test, plus a whole-state
snapshot (syrupy JSON) for regression. Regenerate snapshots with
`pytest -m db -n 0 --snapshot-update` (syrupy cannot update reliably under xdist).
"""
import pytest
from pds.registrysweepers.ancestry import main as ancestry_main
from pds.registrysweepers.ancestry import queries as ancestry_queries
from pds.registrysweepers.ancestry.constants import ANCESTRY_REFS_METADATA_KEY
from pds.registrysweepers.ancestry.queries import get_orphaned_documents
from pds.registrysweepers.ancestry.queries import query_for_collection_nonaggregate_refs
from pds.registrysweepers.ancestry.queries import query_for_pending_collections
from pds.registrysweepers.ancestry.runtimeconstants import AncestryRuntimeConstants
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid

from tests.support.builders import build_bundle
from tests.support.builders import build_collection
from tests.support.builders import build_product
from tests.support.builders import CollectionRefsBuilder
from tests.support.builders import ProductDocumentBuilder


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

def test_basic_hierarchy(registry_db, snapshot):
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

    assert reg_docs == snapshot


# ---------------------------------------------------------------------------
# Scenario 2: LID reference resolves to all collection versions
# ---------------------------------------------------------------------------

def test_lid_ref_resolves_to_all_collection_versions(registry_db, snapshot):
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

    assert reg_docs == snapshot


# ---------------------------------------------------------------------------
# Scenario 3: multiple collection versions
# ---------------------------------------------------------------------------

def test_multiple_collection_versions(registry_db, snapshot):
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

    assert reg_docs == snapshot


# ---------------------------------------------------------------------------
# Scenario 4: refs pagination
# ---------------------------------------------------------------------------

def test_refs_pagination(registry_db, monkeypatch, snapshot):
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

    assert reg_docs == snapshot


# ---------------------------------------------------------------------------
# Scenario 5: idempotency
# ---------------------------------------------------------------------------

def test_idempotency(registry_db, snapshot):
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
    after_first_run = registry_db.dump("registry")

    ancestry_main.run(client=registry_db.client)
    registry_db.refresh()
    reg_docs = registry_db.dump("registry")

    assert reg_docs == after_first_run, "second run changed registry state"

    prod_refs = _refs(_source_by_id(reg_docs, product_id))
    assert prod_refs == {collection_lid, collection_lidvid}

    coll_refs = _refs(_source_by_id(reg_docs, collection_lidvid))
    assert coll_refs == {"urn:nasa:pds:idem:bundle", bundle_lidvid}

    assert _source_by_id(reg_docs, bundle_lidvid)[SWEEPERS_ANCESTRY_VERSION_METADATA_KEY] == SWEEPERS_ANCESTRY_VERSION

    assert reg_docs == snapshot


# ---------------------------------------------------------------------------
# Scenario 6: deferred updates (orphan processing)
# ---------------------------------------------------------------------------

def test_deferred_updates(registry_db, snapshot):
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
    assert deferred_docs == snapshot(name="deferred_after_first_run")

    # Add the product to registry and re-run
    registry_db.load([build_product(orphan_product_id)], index="registry", validate=False)
    ancestry_main.run(client=registry_db.client)
    registry_db.refresh()

    reg_docs = registry_db.dump("registry")
    prod_refs = _refs(_source_by_id(reg_docs, orphan_product_id))
    assert collection_lidvid in prod_refs
    assert collection_lid in prod_refs

    assert reg_docs == snapshot(name="registry_after_second_run")


# ---------------------------------------------------------------------------
# Scenario 7: empty database
# ---------------------------------------------------------------------------

def test_empty_database(registry_db):
    """Sweeper completes without error on an empty registry."""
    ancestry_main.run(client=registry_db.client)
    registry_db.refresh()

    assert registry_db.dump("registry") == []
    assert registry_db.dump("registry-refs") == []


# ---------------------------------------------------------------------------
# Ported coverage (plan §3.2): DB counterparts of mock-based tests in
# ancestry/integration/ and ancestry/scenarios/, where the real database checks
# behaviour the mock either fakes or cannot observe. The mock tests are kept.
# ---------------------------------------------------------------------------

def _bundle(lidvid: str, *collection_lids: str) -> dict:
    builder = ProductDocumentBuilder(lidvid=lidvid, product_class="Product_Bundle")
    for lid in collection_lids:
        builder = builder.with_collection_lid_reference(lid)
    return builder.build()


def _lid(lidvid: str) -> str:
    return lidvid.split("::")[0]


# Scenario 8: version filtering ---------------------------------------------

def test_version_filtering(registry_db, snapshot):
    """
    Only collections below the current ancestry version are processed; the filter is
    a range clause in the OpenSearch query, which the mock never executes.

    Ports: integration/test_full_pipeline.py::test_version_skipping,
           scenarios/test_edge_cases.py::test_mix_of_versioned_and_unversioned
    """
    unversioned = "urn:nasa:pds:vf:unversioned::1.0"
    old_version = "urn:nasa:pds:vf:old::1.0"
    current = "urn:nasa:pds:vf:current::1.0"
    bundle = "urn:nasa:pds:vf:bundle::1.0"

    registry_db.load(
        [_bundle(bundle, _lid(unversioned), _lid(old_version), _lid(current)),
         build_collection(unversioned),
         build_collection(old_version, ancestry_version=SWEEPERS_ANCESTRY_VERSION - 1),
         build_collection(current, ancestry_version=SWEEPERS_ANCESTRY_VERSION),
         build_product(f"{_lid(current)}:p1::1.0")],
        index="registry",
    )
    registry_db.load([CollectionRefsBuilder(current).with_product(f"{_lid(current)}:p1::1.0").to_seed()])

    ancestry_main.run(client=registry_db.client)
    registry_db.refresh()
    reg_docs = registry_db.dump("registry")

    for coll_id in (unversioned, old_version):
        src = _source_by_id(reg_docs, coll_id)
        assert src[SWEEPERS_ANCESTRY_VERSION_METADATA_KEY] == SWEEPERS_ANCESTRY_VERSION
        assert _refs(src) == {bundle, _lid(bundle)}, f"{coll_id} should have been processed"

    # Already-current collection is skipped entirely: no bundle ancestry, and its
    # members are not processed either.
    assert ANCESTRY_REFS_METADATA_KEY not in _source_by_id(reg_docs, current)
    assert ANCESTRY_REFS_METADATA_KEY not in _source_by_id(reg_docs, f"{_lid(current)}:p1::1.0")

    assert reg_docs == snapshot


# Scenario 9: Painless deduplication script ---------------------------------

def test_deduplication_script(registry_db, snapshot):
    """
    Duplicate refs within a run, and refs already present from an earlier run, are not
    duplicated in the stored ancestor_refs. Deduplication happens in a Painless script
    executed by OpenSearch, which the mock cannot run.

    Ports: scenarios/test_edge_cases.py::test_duplicate_product_references,
           integration/test_full_pipeline.py::test_deduplication_script_included
    """
    collection_lid = "urn:nasa:pds:dedup_collection"
    product = f"{collection_lid}:product::1.0"

    registry_db.load_json("ancestry/duplicate_refs.json")  # refs list the product 3 times
    ancestry_main.run(client=registry_db.client)
    registry_db.refresh()

    refs_after_run_1 = _source_by_id(registry_db.dump("registry"), product)[ANCESTRY_REFS_METADATA_KEY]
    assert sorted(refs_after_run_1) == sorted([collection_lid, f"{collection_lid}::1.0"])

    # A new collection version also lists the product. Its LID ref is already present.
    registry_db.load([build_collection(f"{collection_lid}::2.0")], index="registry")
    registry_db.load([CollectionRefsBuilder(f"{collection_lid}::2.0").with_products([product, product]).to_seed()])
    ancestry_main.run(client=registry_db.client)
    registry_db.refresh()
    reg_docs = registry_db.dump("registry")

    refs_after_run_2 = _source_by_id(reg_docs, product)[ANCESTRY_REFS_METADATA_KEY]
    assert sorted(refs_after_run_2) == sorted([collection_lid, f"{collection_lid}::1.0", f"{collection_lid}::2.0"])

    assert reg_docs == snapshot


# Scenario 10: several updates to one document ------------------------------

def test_multiple_updates_merge_into_one_document(registry_db, snapshot):
    """
    Two bundles referencing one collection, and one bundle referencing two collections:
    every link survives the bulk script updates into the stored document.

    Ports: integration/test_bundle_processing.py::test_multiple_bundles_reference_same_collection,
           integration/test_bundle_processing.py::test_bundle_references_multiple_collections
    """
    shared = "urn:nasa:pds:merge:shared::1.0"
    coll_x = "urn:nasa:pds:merge:x::1.0"
    coll_y = "urn:nasa:pds:merge:y::1.0"
    bundle_1 = "urn:nasa:pds:merge:bundle1::1.0"
    bundle_2 = "urn:nasa:pds:merge:bundle2::1.0"
    bundle_xy = "urn:nasa:pds:merge:bundle_xy::1.0"

    registry_db.load(
        [_bundle(bundle_1, _lid(shared)),
         _bundle(bundle_2, _lid(shared)),
         _bundle(bundle_xy, _lid(coll_x), _lid(coll_y)),
         build_collection(shared),
         build_collection(coll_x),
         build_collection(coll_y)],
        index="registry",
    )

    ancestry_main.run(client=registry_db.client)
    registry_db.refresh()
    reg_docs = registry_db.dump("registry")

    assert _refs(_source_by_id(reg_docs, shared)) == {bundle_1, _lid(bundle_1), bundle_2, _lid(bundle_2)}
    for coll_id in (coll_x, coll_y):
        assert _refs(_source_by_id(reg_docs, coll_id)) == {bundle_xy, _lid(bundle_xy)}

    assert reg_docs == snapshot


# Scenario 11: unlinked aggregates are still stamped ------------------------

def test_unlinked_aggregates_are_stamped(registry_db, snapshot):
    """
    Bundles and collections with nothing to link to still get the ancestry version written,
    so the next run no longer treats them as pending. The mock tests only check that
    an in-memory record is generated, not that the stamp is stored.

    Ports: integration/test_bundle_processing.py::test_bundle_without_collections,
           scenarios/test_edge_cases.py::test_bundle_references_nonexistent_collection,
           integration/test_bundle_processing.py::test_collection_without_bundles,
           integration/test_collection_processing.py::test_collection_with_no_members
    """
    bundle_alone = "urn:nasa:pds:unlinked:bundle_alone::1.0"
    bundle_dangling = "urn:nasa:pds:unlinked:bundle_dangling::1.0"
    bundle_parent = "urn:nasa:pds:unlinked:bundle_parent::1.0"
    coll_no_bundle = "urn:nasa:pds:unlinked:coll_no_bundle::1.0"
    coll_no_members = "urn:nasa:pds:unlinked:coll_no_members::1.0"
    member = f"{_lid(coll_no_bundle)}:p1::1.0"

    registry_db.load(
        [_bundle(bundle_alone),
         _bundle(bundle_dangling, "urn:nasa:pds:unlinked:does_not_exist"),
         _bundle(bundle_parent, _lid(coll_no_members)),
         build_collection(coll_no_bundle),
         build_collection(coll_no_members),
         build_product(member)],
        index="registry",
    )
    registry_db.load([CollectionRefsBuilder(coll_no_bundle).with_product(member).to_seed()])

    ancestry_main.run(client=registry_db.client)
    registry_db.refresh()
    reg_docs = registry_db.dump("registry")

    for agg_id in (bundle_alone, bundle_dangling, bundle_parent, coll_no_bundle, coll_no_members):
        assert _source_by_id(reg_docs, agg_id).get(SWEEPERS_ANCESTRY_VERSION_METADATA_KEY) == SWEEPERS_ANCESTRY_VERSION, (
            f"{agg_id} was not stamped with the ancestry version"
        )
    assert _refs(_source_by_id(reg_docs, coll_no_bundle)) == set()
    assert _refs(_source_by_id(reg_docs, member)) == {_lid(coll_no_bundle), coll_no_bundle}

    # The sweeper's own "not at current version" query. (query_for_pending_bundles is not
    # used here: it deliberately returns every bundle and skips up-to-date ones in Python.)
    assert [d["_id"] for d in get_orphaned_documents(registry_db.client, registry_db.index_names["registry"])] == []
    assert list(query_for_pending_collections(registry_db.client)) == []
    assert registry_db.dump("deferred-updates") == [], "nothing should be deferred: every target exists"

    assert reg_docs == snapshot


# Scenario 12: malformed documents ------------------------------------------

def test_malformed_documents(registry_db, snapshot):
    """
    A product with no product_class and a bundle whose ref_lid_collection is not a valid
    identifier do not stop the run; the valid hierarchy alongside them is processed.

    Ports: scenarios/test_edge_cases.py::test_document_missing_product_class (skipped as a
           mock test: "mock is incompatible with PR #256"),
           scenarios/test_edge_cases.py::test_bundle_with_invalid_ref_lid_collection
    """
    registry_db.load_json("ancestry/malformed_docs.json")

    ancestry_main.run(client=registry_db.client)
    registry_db.refresh()
    reg_docs = registry_db.dump("registry")

    collection = "urn:nasa:pds:malformed:collection::1.0"
    bundle = "urn:nasa:pds:malformed:bundle::1.0"
    assert _refs(_source_by_id(reg_docs, collection)) == {bundle, _lid(bundle)}
    assert _refs(_source_by_id(reg_docs, "urn:nasa:pds:malformed:collection:product::1.0")) == {
        _lid(collection), collection,
    }

    bad_ref_bundle = _source_by_id(reg_docs, "urn:nasa:pds:malformed:bad_ref_bundle::1.0")
    assert bad_ref_bundle.get(SWEEPERS_ANCESTRY_VERSION_METADATA_KEY) == SWEEPERS_ANCESTRY_VERSION

    # Unreferenced and unclassified: nothing should touch it.
    assert ANCESTRY_REFS_METADATA_KEY not in _source_by_id(reg_docs, "urn:nasa:pds:malformed:no_class::1.0")

    assert reg_docs == snapshot


# ---------------------------------------------------------------------------
# Scenario 13: version comparison on the keyword-typed version field
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("current_version", [8, 9, 10])
def test_pending_queries_compare_versions_numerically(registry_db, monkeypatch, current_version):
    """
    Regression: the ancestry version field is mapped as keyword (ancestry/main.py), so a range
    query compared versions as strings ("9" >= "10"). Once SWEEPERS_ANCESTRY_VERSION reached 10,
    every doc stamped 2-9 would have been treated as up to date and silently skipped.

    The pending/orphan queries must treat a doc as pending iff its version is not exactly the
    current one (so docs from a newer sweeper are reprocessed after a downgrade).
    """
    monkeypatch.setattr(ancestry_queries, "SWEEPERS_ANCESTRY_VERSION", current_version)

    stored_versions = [None, 7, 8, 9, 10, 11]
    lidvid = {v: f"urn:nasa:pds:vcmp:coll_{v if v is not None else 'none'}::1.0" for v in stored_versions}
    registry_db.load(
        [build_collection(lidvid[v], ancestry_version=v) for v in stored_versions],
        index="registry",
    )

    # One collection whose refs batches carry different versions.
    refs_collection = "urn:nasa:pds:vcmp:refs_coll::1.0"
    refs_docs = []
    for batch_id, v in enumerate(stored_versions, start=1):
        doc = CollectionRefsBuilder(refs_collection).with_batch_id(batch_id).with_product(
            f"urn:nasa:pds:vcmp:refs_coll:p_{v if v is not None else 'none'}::1.0"
        ).build()
        if v is not None:
            doc["_source"][SWEEPERS_ANCESTRY_VERSION_METADATA_KEY] = v
        refs_docs.append(doc)
    registry_db.load(refs_docs, index="registry-refs")

    expected_pending = {v for v in stored_versions if v != current_version}

    pending_collections = {d["_id"] for d in ancestry_queries.query_for_pending_collections(registry_db.client)}
    assert pending_collections == {lidvid[v] for v in expected_pending}

    orphans = {d["_id"] for d in get_orphaned_documents(registry_db.client, registry_db.index_names["registry"])}
    assert orphans == {lidvid[v] for v in expected_pending}

    pending_refs = {
        str(p) for p in query_for_collection_nonaggregate_refs(registry_db.client, PdsLidVid.from_string(refs_collection))
    }
    assert pending_refs == {
        f"urn:nasa:pds:vcmp:refs_coll:p_{v if v is not None else 'none'}::1.0" for v in expected_pending
    }
