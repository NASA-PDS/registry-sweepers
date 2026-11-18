"""
How the orphan repair interacts with the deferral index (the ancestry sweeper's handling of updates for products which
did not yet exist, <registry>-deferred-updates) on the structured-metadata branch.

The real ancestry sweeper (ancestry.main.run, including its orphan/deferral merge step) and the real driver are run
against the in-memory fake registry; nothing here can reach a real endpoint.
"""
import copy
import sys

import pytest
from pds.registrysweepers import driver
from pds.registrysweepers.ancestry import main as ancestry_main
from pds.registrysweepers.ancestry.productupdaterecord import ProductUpdateRecord
from pds.registrysweepers.ancestry.utils import update_from_record
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry_orphan_repair import constants
from pds.registrysweepers.ancestry_orphan_repair import main as orphan_repair
from pds.registrysweepers.ancestry_orphan_repair.main import run as repair
from pds.registrysweepers.utils.db import write_updated_docs
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid

from .fake_registry import FakeRegistry
from .fake_registry import REFS
from .fake_registry import STAMP

C1 = "urn:nasa:pds:mission:data::1.0"
C1_LID = "urn:nasa:pds:mission:data"
C2 = "urn:nasa:pds:mission:data::2.0"
MISSING_COLLECTION = "urn:nasa:pds:gone:data::1.0"


def product(n: int) -> str:
    return f"urn:nasa:pds:mission:data:p{n:05d}::1.0"


# products of the scenario below
OVERWRITTEN = product(1)  # existed when its collection was swept, then overwritten by Harvest (no deferral)
DEFERRED = product(2)  # missing when its collection was swept, loaded later (deferral entry)
LISTED_LATE = product(3)  # listed in a refs document written after the collection was stamped
UNLISTED = product(4)  # no refs document lists it


@pytest.fixture(autouse=True)
def _no_multitenancy(monkeypatch):
    monkeypatch.delenv("MULTITENANCY_NODE_ID", raising=False)


@pytest.fixture
def registry(monkeypatch) -> FakeRegistry:
    fake = FakeRegistry()
    # opensearchpy.helpers.bulk is not modelled by the fake: the deletion of merged deferral entries is applied directly
    monkeypatch.setattr(ancestry_main, "bulk_delete_documents", fake.delete_deferred)
    return fake


def sweep_then_harvest(registry: FakeRegistry) -> None:
    """
    1. the ancestry sweeper runs while DEFERRED is not yet in the registry: its update is deferred
    2. Harvest then loads DEFERRED, overwrites OVERWRITTEN (wiping its ancestry), loads LISTED_LATE and its refs
       document, and loads UNLISTED (which nothing lists)
    """
    registry.add_collection(C1, stamp=None)
    registry.add_product(OVERWRITTEN)
    registry.add_refs(C1, [OVERWRITTEN, DEFERRED])

    ancestry_main.run(registry)

    assert registry.registry[C1][STAMP] == SWEEPERS_ANCESTRY_VERSION
    assert registry.registry[OVERWRITTEN][STAMP] == SWEEPERS_ANCESTRY_VERSION
    assert sorted(registry.deferred) == [DEFERRED]  # the update for the missing product was deferred, not lost

    registry.add_product(OVERWRITTEN)  # Harvest overwrites the document: ancestry and stamp are gone
    registry.add_product(DEFERRED)  # Harvest loads the product the sweeper had to defer
    registry.add_product(LISTED_LATE)
    registry.add_refs(C1, [LISTED_LATE])  # a refs document written after C1 was stamped
    registry.add_product(UNLISTED)


def doc(registry: FakeRegistry, lidvid: str):
    return copy.deepcopy(registry.registry[lidvid])


# ---------------------------------------------------------------------------------------------------------------------
# 1. order of execution
# ---------------------------------------------------------------------------------------------------------------------


def test_the_default_suite_runs_the_repair_directly_after_ancestry():
    names = driver.DEFAULT_SWEEPERS
    assert names.index("ancestry-orphan-repair") == names.index("ancestry") + 1
    assert driver.order_sweepers(["ancestry-orphan-repair", "ancestry"]) == ["ancestry", "ancestry-orphan-repair"]


def test_the_deferral_merge_runs_inside_the_ancestry_sweeper_so_the_repair_follows_it(registry):
    """One ancestry run sweeps (nothing left to do) AND merges: after it returns the deferred product is complete."""
    sweep_then_harvest(registry)
    assert STAMP not in registry.registry[DEFERRED]

    ancestry_main.run(registry)

    assert sorted(registry.registry[DEFERRED][REFS]) == sorted([C1, C1_LID])
    assert registry.registry[DEFERRED][STAMP] == SWEEPERS_ANCESTRY_VERSION
    assert registry.deferred == {}  # the entry was consumed


def test_driver_run_in_default_order_leaves_the_repair_only_what_the_deferral_cannot_reach(registry, monkeypatch):
    sweep_then_harvest(registry)
    reports = []
    real_repair = driver.SWEEPER_REGISTRY["ancestry-orphan-repair"]

    def capture(**kwargs):
        reports.append(real_repair(**kwargs))

    monkeypatch.setitem(driver.SWEEPER_REGISTRY, "ancestry-orphan-repair", capture)
    monkeypatch.setattr(driver, "get_opensearch_client_from_environment", lambda verify_certs: registry)
    # the requested order is wrong on purpose: the driver puts the repair right after ancestry
    monkeypatch.setattr(sys, "argv", ["registry-sweepers", "--only", "ancestry-orphan-repair", "ancestry"])
    assert list(registry.deferred) == [DEFERRED]

    driver.run()

    (report,) = reports
    # DEFERRED was merged by the ancestry sweeper first, so the repair never saw it
    assert report.products_examined == 3
    assert report.pending_deferral == 0
    assert (report.repaired, report.unresolved) == (2, 1)
    assert registry.deferred == {}
    for lidvid in (OVERWRITTEN, DEFERRED, LISTED_LATE):
        assert sorted(registry.registry[lidvid][REFS]) == sorted([C1, C1_LID])
        assert registry.registry[lidvid][STAMP] == SWEEPERS_ANCESTRY_VERSION
    assert STAMP not in registry.registry[UNLISTED] and REFS not in registry.registry[UNLISTED]


# ---------------------------------------------------------------------------------------------------------------------
# 2. what remains for the repair
# ---------------------------------------------------------------------------------------------------------------------


def test_after_the_ancestry_run_the_repair_repairs_overwritten_and_late_listed_products_only(registry):
    sweep_then_harvest(registry)
    ancestry_main.run(registry)  # sweep (collection already stamped) and deferral merge
    deferred_doc_before = doc(registry, DEFERRED)
    writes_before = len(registry.written_ids)

    dry_run = repair(registry, dry_run=True)
    assert (dry_run.products_examined, dry_run.repairable, dry_run.unresolved) == (3, 2, 1)
    assert len(registry.written_ids) == writes_before  # dry run writes nothing

    applied = repair(registry)

    assert applied.repaired == 2
    assert sorted(registry.written_ids[writes_before:]) == sorted([OVERWRITTEN, LISTED_LATE])
    assert doc(registry, DEFERRED) == deferred_doc_before  # not selected, not written
    for lidvid in (OVERWRITTEN, LISTED_LATE):
        assert sorted(registry.registry[lidvid][REFS]) == sorted([C1, C1_LID])
        assert registry.registry[lidvid][STAMP] == SWEEPERS_ANCESTRY_VERSION
    assert STAMP not in registry.registry[UNLISTED] and REFS not in registry.registry[UNLISTED]


def test_the_deferral_merge_shrinks_the_repair_workload_by_the_products_it_repaired(registry, monkeypatch):
    """
    The same data, once as on a branch without the deferral (the update for the missing product is simply lost) and once
    with it.  The product the deferral covers is repairable work only in the first case.
    """
    sweep_then_harvest(registry)
    without_deferral = FakeRegistry()
    without_deferral.registry = copy.deepcopy(registry.registry)
    without_deferral.refs = copy.deepcopy(registry.refs)  # and no deferral entry: it was never kept

    ancestry_main.run(without_deferral)
    ancestry_main.run(registry)

    assert repair(without_deferral).repairable == 3  # includes DEFERRED
    assert repair(registry).repairable == 2  # DEFERRED was already repaired by the merge


def test_a_product_with_a_pending_deferral_entry_is_not_touched_by_the_repair(registry):
    """The repair runs before the merge could (the order is wrong, or the merge failed): it must leave the entry alone."""
    sweep_then_harvest(registry)
    before = doc(registry, DEFERRED)
    entry_before = copy.deepcopy(registry.deferred[DEFERRED])
    registry.add_refs(C1, [DEFERRED])  # a refs document lists it too, so it would otherwise be repairable
    writes_before = len(registry.written_ids)

    for dry_run in (True, False):
        report = repair(registry, dry_run=dry_run)

        assert report.pending_deferral == 1
        assert report.products_examined == 4
        assert doc(registry, DEFERRED) == before  # no refs, no stamp
        assert registry.deferred[DEFERRED] == entry_before  # the entry is the merge's to consume

    assert DEFERRED not in registry.written_ids[writes_before:]
    # the others are still repaired in the same batch
    assert sorted(registry.registry[OVERWRITTEN][REFS]) == sorted([C1, C1_LID])


def test_the_merge_then_completes_what_the_repair_left_and_the_repair_has_nothing_more_to_do(registry):
    sweep_then_harvest(registry)
    repair(registry)

    ancestry_main.run(registry)  # merge: DEFERRED is orphaned and has an entry

    assert sorted(registry.registry[DEFERRED][REFS]) == sorted([C1, C1_LID])
    assert registry.deferred == {}
    second = repair(registry)
    assert second.products_examined == 1  # only UNLISTED remains
    assert second.unresolved == 1 and second.repaired == 0


@pytest.mark.parametrize("listed_in_refs", [True, False])
def test_pending_deferral_is_counted_apart_from_unresolved(registry, listed_in_refs):
    registry.add_collection(C1)
    registry.add_product(product(10))
    registry.add_deferral(product(10), [C1, C1_LID])
    if listed_in_refs:
        registry.add_refs(C1, [product(10)])

    report = repair(registry)

    assert report.pending_deferral == 1
    assert report.unresolved == 0 and report.repairable == 0
    assert registry.bulk_calls == []
    assert "left to the deferral merge" in "\n".join(report.summary_lines())


def test_every_examined_product_is_counted_exactly_once(registry):
    registry.add_collection(C1)
    registry.add_product(product(1))  # repairable
    registry.add_product(product(2))  # no refs listing
    registry.add_product(product(3))  # listed only under a collection which is not in the registry
    registry.add_product(product(4))  # pending deferral
    registry.add_refs(C1, [product(1)])
    registry.add_refs(MISSING_COLLECTION, [product(3)])
    registry.add_deferral(product(4), [C1, C1_LID])

    report = repair(registry)

    assert report.products_examined == 4
    assert (report.repairable, report.no_refs_listing, report.listed_only_under_collection_not_in_registry) == (1, 1, 1)
    assert report.pending_deferral == 1
    assert (
        report.repairable + report.unresolved + report.pending_deferral + report.invalid_lidvid
        == report.products_examined
    )


def test_a_product_whose_parent_cannot_be_found_is_never_stamped_next_to_deferred_ones(registry):
    registry.add_collection(C1)
    registry.add_product(product(1))  # no refs listing
    registry.add_product(product(2))  # only under a collection which is not in the registry
    registry.add_product(product(3))  # pending deferral
    registry.add_refs(MISSING_COLLECTION, [product(2)])
    registry.add_deferral(product(3), [C1, C1_LID])
    before = copy.deepcopy(registry.registry)

    for dry_run in (True, False):
        repair(registry, dry_run=dry_run)

    assert registry.registry == before
    assert registry.written_ids == [] and registry.bulk_calls == []
    assert list(registry.deferred) == [product(3)]


# ---------------------------------------------------------------------------------------------------------------------
# 3. conflicts between repair writes and deferral entries
# ---------------------------------------------------------------------------------------------------------------------


def test_repair_style_write_then_merge_is_idempotent_and_leaves_only_a_harmless_entry(registry):
    """
    The order the repair avoids by skipping pending entries: the repair's own update lands first, the merge runs after.
    The merge only picks up orphaned documents, so it skips the now-stamped product; nothing is duplicated or lost.
    """
    registry.add_collection(C1)
    registry.add_product(product(1))
    registry.add_refs(C1, [product(1)])
    registry.add_deferral(product(1), [C1, C1_LID])
    update = update_from_record(
        ProductUpdateRecord(PdsLidVid.from_string(product(1)), direct_ancestor_refs=[PdsLidVid.from_string(C1)])
    )

    write_updated_docs(registry, [update], index_name="registry")
    after_repair_write = doc(registry, product(1))
    write_updated_docs(registry, [update], index_name="registry")  # the script appends only missing refs
    assert doc(registry, product(1)) == after_repair_write

    ancestry_main.run(registry)

    assert doc(registry, product(1)) == after_repair_write
    assert sorted(registry.registry[product(1)][REFS]) == sorted([C1, C1_LID])
    assert list(registry.deferred) == [product(1)]  # never merged, so never deleted: stale but harmless


def test_merge_after_repair_style_write_would_replace_not_append(registry):
    """
    Documents why the repair must not write a product while its entry is pending: the merge writes the entry's content
    as a partial document, which REPLACES ancestor_refs rather than appending to them.
    """
    registry.add_collection(C1)
    registry.add_product(product(1), stamp=None)
    registry.add_deferral(product(1), [C1])  # an entry which holds fewer refs than the product could end up with

    write_updated_docs(
        registry,
        [update_from_record(ProductUpdateRecord(PdsLidVid.from_string(product(1)), [PdsLidVid.from_string(C2)]))],
        index_name="registry",
    )
    registry.registry[product(1)].pop(STAMP)  # the product is orphaned again (e.g. overwritten) before the merge runs
    ancestry_main.run(registry)

    assert registry.registry[product(1)][REFS] == [C1]  # C2 and the LID are gone: the repair must leave such products


# ---------------------------------------------------------------------------------------------------------------------
# the fixed write path, the deferral index lookup and the structured field and index names
# ---------------------------------------------------------------------------------------------------------------------


def test_a_single_update_to_a_missing_document_is_deferred(registry):
    update = update_from_record(
        ProductUpdateRecord(PdsLidVid.from_string(product(1)), direct_ancestor_refs=[PdsLidVid.from_string(C1)])
    )

    write_updated_docs(registry, iter([update]), index_name="registry", defer_on_missing_document=True)

    assert list(registry.deferred) == [product(1)]
    assert sorted(registry.deferred[product(1)][REFS]) == sorted([C1, C1_LID])


def test_a_single_update_to_an_existing_document_is_written_with_deferral_enabled(registry):
    registry.add_product(product(1))
    update = update_from_record(
        ProductUpdateRecord(PdsLidVid.from_string(product(1)), direct_ancestor_refs=[PdsLidVid.from_string(C1)])
    )

    write_updated_docs(registry, iter([update]), index_name="registry", defer_on_missing_document=True)

    assert registry.registry[product(1)][STAMP] == SWEEPERS_ANCESTRY_VERSION
    assert registry.deferred == {}


def test_deferral_entries_are_looked_up_per_chunk_of_a_batch(registry, monkeypatch):
    monkeypatch.setattr(orphan_repair, "DEFERRAL_LOOKUP_CHUNK_SIZE", 3)
    registry.add_collection(C1)
    for n in range(10):
        registry.add_product(product(n))
    registry.add_refs(C1, [product(n) for n in range(10)])

    repair(registry, batch_size=10)

    assert [len(c["ids"]) for c in registry.mget_calls] == [3, 3, 3, 1]
    assert sorted(i for c in registry.mget_calls for i in c["ids"]) == [product(n) for n in range(10)]


def test_without_a_deferral_index_nothing_is_looked_up(registry):
    registry.has_deferred_index = False
    registry.add_collection(C1)
    registry.add_product(product(1))
    registry.add_refs(C1, [product(1)])

    report = repair(registry)

    assert registry.mget_calls == []
    assert report.pending_deferral == 0 and report.repaired == 1


def test_multitenant_structured_index_names_are_used_for_every_request(registry, monkeypatch):
    monkeypatch.setenv("MULTITENANCY_NODE_ID", "en")
    registry.add_collection(C1)
    registry.add_product(product(1))
    registry.add_product(product(2))
    registry.add_refs(C1, [product(1)])
    registry.add_deferral(product(2), [C1, C1_LID])

    repair(registry)

    searched = {c["index"] for c in registry.search_calls}
    assert searched == {"en-registry-structured", "en-registry-structured-refs"}
    assert {c["index"] for c in registry.mget_calls} == {"en-registry-structured-deferred-updates"}
    assert {c["index"] for c in registry.bulk_calls} == {"en-registry-structured"}
    # the deferral index name is the one the ancestry sweeper derives from the resolved registry index name
    assert registry.mget_calls[0]["index"] == "en-registry-structured" + constants.DEFERRED_UPDATES_INDEX_SUFFIX


def test_field_names_are_the_structured_metadata_keys():
    assert constants.ANCESTOR_REFS_KEY == "ops:Registry_Sweepers.ops:ancestor_refs"
    assert constants.ANCESTRY_VERSION_KEY == "ops:Registry_Sweepers.ops:ancestry_software_version"
    assert constants.SUPERSEDED_BY_KEY == "ops:Registry_Sweepers.ops:superseded_by"
    assert constants.CURRENT_ANCESTRY_VERSION == SWEEPERS_ANCESTRY_VERSION == 8
    assert (constants.REFS_COLLECTION_FIELD, constants.REFS_PRODUCT_FIELD, constants.REFS_BATCH_FIELD) == (
        "collection_lidvid",
        "product_lidvid",
        "batch_id",
    )

    query = orphan_repair.build_orphan_products_query()["query"]["bool"]
    assert {"exists": {"field": constants.SUPERSEDED_BY_KEY}} in query["must_not"]
    should = query["filter"][0]["bool"]["should"]
    assert {"bool": {"must_not": [{"exists": {"field": constants.ANCESTOR_REFS_KEY}}]}} in should
    assert {
        "bool": {"must_not": [{"range": {constants.ANCESTRY_VERSION_KEY: {"gte": SWEEPERS_ANCESTRY_VERSION}}}]}
    } in should


def test_the_selection_uses_the_same_stamp_predicate_as_the_deferral_merge():
    from pds.registrysweepers.ancestry.queries import _orphaned_docs_query

    merge_predicate = _orphaned_docs_query["query"]["bool"]["must_not"][0]
    should = orphan_repair.build_orphan_products_query()["query"]["bool"]["filter"][0]["bool"]["should"]

    # every document the merge treats as orphaned (stamp missing or below current) is selected by the repair, too
    assert {"bool": {"must_not": [merge_predicate]}} in should


def test_dry_run_lists_the_product_left_to_the_deferral_merge_only_with_output_all(registry, capsys, tmp_path):
    sweep_then_harvest(registry)
    out = tmp_path / "listing.tsv"
    entry_before = copy.deepcopy(registry.deferred[DEFERRED])
    writes_before = len(registry.written_ids)

    repair(registry, dry_run=True)
    only_repairable = capsys.readouterr().out
    repair(registry, dry_run=True, output_all=True, output_file=str(out))
    everything = capsys.readouterr().out

    assert f"pending_deferral\t{DEFERRED}\t" not in only_repairable
    assert f"pending_deferral\t{DEFERRED}\t" in everything.splitlines()
    listing_lines = out.read_text().splitlines()
    assert everything.splitlines()[: len(listing_lines)] == listing_lines  # file and stdout listing are identical
    assert everything.splitlines()[len(listing_lines)].startswith("Ancestry orphan repair - DRY RUN")  # summary last
    assert registry.deferred[DEFERRED] == entry_before  # untouched
    assert len(registry.written_ids) == writes_before
