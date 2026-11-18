import copy
import json
import logging
import sys

import pytest
from pds.registrysweepers.ancestry.generation import process_collection_ancestries_for_nonaggregates
from pds.registrysweepers.ancestry.productupdaterecord import ProductUpdateRecord
from pds.registrysweepers.ancestry.queries import product_class_query_factory
from pds.registrysweepers.ancestry.queries import ProductClass
from pds.registrysweepers.ancestry.utils import update_from_record
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry_orphan_repair import main as orphan_repair
from pds.registrysweepers.ancestry_orphan_repair.constants import MAX_PAGE_SIZE
from pds.registrysweepers.ancestry_orphan_repair.main import build_orphan_products_query
from pds.registrysweepers.ancestry_orphan_repair.main import iterate_pages_search_after
from pds.registrysweepers.ancestry_orphan_repair.main import OrphanRepairReport
from pds.registrysweepers.ancestry_orphan_repair.main import run
from pds.registrysweepers.utils.db import write_updated_docs
from pds.registrysweepers.utils.db.update import Update
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid

from .fake_registry import FakeRegistry
from .fake_registry import REFS
from .fake_registry import STAMP

C1 = "urn:nasa:pds:mission:data::1.0"
C1_LID = "urn:nasa:pds:mission:data"
C2 = "urn:nasa:pds:mission:data::2.0"
MISSING_COLLECTION = "urn:nasa:pds:gone:data::1.0"


def product(n: int, collection_lid: str = "urn:nasa:pds:mission:data") -> str:
    return f"{collection_lid}:p{n:05d}::1.0"


@pytest.fixture(autouse=True)
def _no_multitenancy(monkeypatch):
    monkeypatch.delenv("MULTITENANCY_NODE_ID", raising=False)


@pytest.fixture
def registry() -> FakeRegistry:
    return FakeRegistry()


def snapshot(registry: FakeRegistry):
    return copy.deepcopy(registry.registry)


# ---------------------------------------------------------------------------------------------------------------------
# selection
# ---------------------------------------------------------------------------------------------------------------------


def test_selects_only_non_aggregate_non_superseded_products_lacking_ancestry(registry):
    registry.add_collection(C1)
    registry.add_product(product(1))  # no ancestry at all
    registry.add_product(product(2), stamp=SWEEPERS_ANCESTRY_VERSION, ancestor_refs=[C1, C1_LID])  # complete
    registry.add_product(product(3), stamp=SWEEPERS_ANCESTRY_VERSION - 1, ancestor_refs=[C1, C1_LID])  # old stamp
    registry.add_product(product(4), stamp=SWEEPERS_ANCESTRY_VERSION, ancestor_refs=[])  # stamped but empty
    registry.add_product(product(5), superseded_by=product(6))  # superseded
    registry.add_product("urn:nasa:pds:mission::1.0", "Product_Bundle")  # aggregate
    registry.add_refs(C1, [product(n) for n in range(1, 7)])

    report = run(registry, output_file=None)

    # collection and bundle are excluded by class, product 5 by supersession, product 2 because it is complete
    assert report.products_examined == 3
    assert report.repairable == 3


def test_superseded_products_are_selected_on_request(registry):
    registry.add_collection(C1)
    registry.add_product(product(1), superseded_by=product(2))
    registry.add_refs(C1, [product(1)])

    assert run(registry).products_examined == 0
    assert run(registry, include_superseded=True).products_examined == 1


def test_non_aggregate_rule_matches_the_ancestry_sweepers():
    ancestry_rule = product_class_query_factory(ProductClass.NON_AGGREGATE)["query"]["bool"]["must_not"][0]

    assert build_orphan_products_query()["query"]["bool"]["must_not"][0] == ancestry_rule


# ---------------------------------------------------------------------------------------------------------------------
# repair
# ---------------------------------------------------------------------------------------------------------------------


def test_repairs_a_product_whose_collection_is_already_stamped(registry):
    registry.add_collection(C1, stamp=SWEEPERS_ANCESTRY_VERSION)
    registry.add_product(product(1))
    registry.add_refs(C1, [product(1)])

    report = run(registry)

    assert report.repairable == 1 and report.repaired == 1
    assert sorted(registry.registry[product(1)][REFS]) == sorted([C1, C1_LID])
    assert registry.registry[product(1)][STAMP] == SWEEPERS_ANCESTRY_VERSION
    # the collection is left untouched
    assert registry.written_ids == [product(1)]


def _bulk_body(updates) -> str:
    lines = []
    for update in updates:
        lines.append(json.dumps({"update": {"_id": update.id}}))
        lines.append(
            json.dumps(
                {
                    "script": {
                        "source": update.inline_script_content,
                        "lang": "painless",
                        "params": {"new_items": update.content[REFS]},
                    }
                }
            )
        )
    return "\n".join(lines) + "\n"


def test_repair_is_identical_to_a_normal_ancestry_run(registry):
    """Run the real ancestry generation against unstamped collections and compare the resulting product documents."""
    normal = FakeRegistry()
    for r, collection_stamp in ((normal, None), (registry, SWEEPERS_ANCESTRY_VERSION)):
        r.add_collection(C1, stamp=collection_stamp)
        r.add_collection(C2, stamp=collection_stamp)
        for n in range(1, 4):
            r.add_product(product(n))
        r.add_refs(C1, [product(1), product(2)])
        r.add_refs(C2, [product(2), product(3)])

    product_updates = [
        update_from_record(record)
        for record in process_collection_ancestries_for_nonaggregates(normal)
        if ":p" in str(record.product)
    ]
    normal.bulk("registry", _bulk_body(product_updates))

    run(registry)

    for n in range(1, 4):
        assert sorted(registry.registry[product(n)][REFS]) == sorted(normal.registry[product(n)][REFS])
        assert registry.registry[product(n)][STAMP] == normal.registry[product(n)][STAMP]
    assert sorted(registry.registry[product(2)][REFS]) == sorted([C1, C2, C1_LID])


def test_update_sent_is_the_ancestry_sweepers_own_update(registry, monkeypatch):
    registry.add_collection(C1)
    registry.add_product(product(1))
    registry.add_refs(C1, [product(1)])
    sent = []
    original = orphan_repair.write_updated_docs

    def spy(client, updates, index_name, **kwargs):
        updates = list(updates)
        sent.extend(updates)
        return original(client, updates, index_name, **kwargs)

    monkeypatch.setattr(orphan_repair, "write_updated_docs", spy)

    run(registry)

    expected = update_from_record(
        ProductUpdateRecord(PdsLidVid.from_string(product(1)), direct_ancestor_refs=[PdsLidVid.from_string(C1)])
    )
    assert len(sent) == 1
    assert sent[0] == expected  # same id, content (refs and stamp) and inline painless script


def test_product_listed_under_two_collection_versions_gets_both(registry):
    registry.add_collection(C1)
    registry.add_collection(C2)
    registry.add_product(product(1))
    registry.add_refs(C1, [product(1)])
    registry.add_refs(C2, [product(1)])

    report = run(registry)

    assert report.repairable == 1 and report.products_examined == 1
    assert sorted(registry.registry[product(1)][REFS]) == sorted([C1, C2, C1_LID])
    assert registry.written_ids == [product(1)]  # one update for the product, not one per collection


def test_only_collections_in_the_registry_are_written(registry):
    registry.add_collection(C1)
    registry.add_product(product(1))
    registry.add_refs(C1, [product(1)])
    registry.add_refs(MISSING_COLLECTION, [product(1)])

    run(registry)

    assert sorted(registry.registry[product(1)][REFS]) == sorted([C1, C1_LID])


# ---------------------------------------------------------------------------------------------------------------------
# unresolved products are never guessed, never stamped
# ---------------------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("dry_run", [True, False])
def test_unresolved_products_are_counted_and_left_untouched(registry, dry_run):
    registry.add_collection(C1)
    registry.add_product(product(1))  # nothing lists it
    registry.add_product(product(2), stamp=3)  # old stamp, nothing lists it
    registry.add_product(product(3))  # listed only under a collection which is not in the registry
    registry.add_refs(MISSING_COLLECTION, [product(3)])
    registry.add_refs(C1, [product(99)])  # lists some other product
    before = snapshot(registry)

    report = run(registry, dry_run=dry_run)

    assert report.products_examined == 3
    assert report.no_refs_listing == 2
    assert report.listed_only_under_collection_not_in_registry == 1
    assert report.unresolved == 3
    assert report.repairable == 0 and report.repaired == 0
    assert registry.bulk_calls == []
    assert registry.written_ids == []
    assert registry.registry == before  # in particular, no stamp was added or changed
    assert STAMP not in registry.registry[product(1)] and registry.registry[product(2)][STAMP] == 3


def test_resolved_products_are_repaired_next_to_unresolved_ones_and_only_they_are_written(registry):
    registry.add_collection(C1)
    registry.add_product(product(1))
    registry.add_product(product(2))
    registry.add_refs(C1, [product(2)])

    report = run(registry)

    assert (report.repaired, report.unresolved) == (1, 1)
    assert registry.written_ids == [product(2)]
    assert STAMP not in registry.registry[product(1)] and REFS not in registry.registry[product(1)]


def test_unresolved_product_is_repaired_once_a_refs_document_appears(registry):
    registry.add_collection(C1)
    registry.add_product(product(1))

    assert run(registry).unresolved == 1
    registry.add_refs(C1, [product(1)])
    report = run(registry)

    assert report.repaired == 1 and report.unresolved == 0


def test_summary_reports_unresolved_separately_with_a_hint(registry):
    registry.add_product(product(1))
    registry.add_product(product(2))
    registry.add_refs(MISSING_COLLECTION, [product(2)])

    for dry_run in (True, False):
        text = "\n".join(run(registry, dry_run=dry_run).summary_lines())
        assert "no refs document lists the product: 1" in text
        assert "not in the registry: 1" in text
        assert "unresolved" in text and "data problems for the node" in text


# ---------------------------------------------------------------------------------------------------------------------
# dry run, idempotency, vanished products
# ---------------------------------------------------------------------------------------------------------------------


def test_dry_run_writes_nothing_and_reports_counts(registry):
    registry.add_collection(C1)
    registry.add_product(product(1))
    registry.add_product(product(2))
    registry.add_refs(C1, [product(1)])
    before = snapshot(registry)

    report = run(registry, dry_run=True)

    assert report.dry_run
    assert (report.products_examined, report.repairable, report.repaired) == (2, 1, 0)
    assert registry.bulk_calls == []
    assert registry.registry == before
    assert registry.indices.put_mapping_calls == []
    assert "DRY RUN" in report.summary_lines()[0]


def test_second_run_writes_nothing(registry):
    registry.add_collection(C1)
    for n in range(1, 4):
        registry.add_product(product(n))
    registry.add_refs(C1, [product(n) for n in range(1, 4)])

    first = run(registry)
    bulks_after_first = len(registry.bulk_calls)
    state_after_first = snapshot(registry)
    second = run(registry)

    assert first.repaired == 3
    assert second.products_examined == 0 and second.repairable == 0 and second.repaired == 0
    assert len(registry.bulk_calls) == bulks_after_first
    assert registry.registry == state_after_first


def test_a_product_with_an_old_stamp_and_current_refs_just_gets_stamped(registry):
    registry.add_collection(C1)
    registry.add_product(product(1), stamp=6, ancestor_refs=[C1, C1_LID])
    registry.add_refs(C1, [product(1)])

    run(registry)

    assert registry.registry[product(1)][REFS] == [C1, C1_LID]  # nothing duplicated
    assert registry.registry[product(1)][STAMP] == SWEEPERS_ANCESTRY_VERSION


def test_existing_refs_are_kept_and_only_missing_ones_added(registry):
    registry.add_collection(C1)
    registry.add_product(product(1), stamp=6, ancestor_refs=["urn:nasa:pds:other::1.0"])
    registry.add_refs(C1, [product(1)])

    run(registry)

    assert registry.registry[product(1)][REFS][0] == "urn:nasa:pds:other::1.0"
    assert set(registry.registry[product(1)][REFS]) == {"urn:nasa:pds:other::1.0", C1, C1_LID}


def test_product_vanishing_between_read_and_write_does_not_abort_the_run(registry, caplog):
    registry.add_collection(C1)
    for n in range(1, 4):
        registry.add_product(product(n))
    registry.add_refs(C1, [product(n) for n in range(1, 4)])
    registry.vanish_before_bulk = {product(2)}

    with caplog.at_level(logging.WARNING):
        report = run(registry)

    assert report.repairable == 3
    assert report.repaired == 2
    assert report.vanished_before_write == 1
    assert product(2) not in registry.registry
    assert REFS in registry.registry[product(1)] and REFS in registry.registry[product(3)]


def test_a_single_repairable_product_is_written(registry):
    registry.add_collection(C1)
    registry.add_product(product(1))
    registry.add_refs(C1, [product(1)])

    assert run(registry).repaired == 1


class FlakyRegistry(FakeRegistry):
    """A registry whose bulk endpoint refuses some documents, as OpenSearch Serverless does at its OCU limit."""

    def __init__(self):
        super().__init__()
        self.blocked = set()  # refused with indexing_block_exception on every submission
        self.throttle_once = set()  # refused with circuit_breaking_exception on the first submission only

    def bulk(self, index, body, request_timeout=None):
        lines = body.splitlines()
        pairs = list(zip(lines[::2], lines[1::2]))
        refused = {}
        allowed = []
        for action, content in pairs:
            doc_id = json.loads(action)["update"]["_id"]
            if doc_id in self.blocked:
                refused[doc_id] = (429, "indexing_block_exception", "blocked by: [maximum OCU capacity reached]")
            elif doc_id in self.throttle_once:
                self.throttle_once.discard(doc_id)
                refused[doc_id] = (429, "circuit_breaking_exception", "throttled")
            else:
                allowed.append((action, content))

        response = super().bulk(index, "\n".join(line for pair in allowed for line in pair) + "\n", request_timeout)
        by_id = {item["update"]["_id"]: item for item in response["items"]}
        items = []
        for action, _ in pairs:
            doc_id = json.loads(action)["update"]["_id"]
            if doc_id in refused:
                status, error_type, reason = refused[doc_id]
                items.append({"update": {"_id": doc_id, "status": status, "error": {"type": error_type, "reason": reason}}})
            else:
                items.append(by_id[doc_id])
        return {"errors": any("error" in item["update"] for item in items), "items": items}


def _write_in_chunks_of(monkeypatch, chunk_size):
    original = orphan_repair.write_updated_docs

    def chunked_write(client, updates, index_name, **kwargs):
        return original(client, updates, index_name, bulk_chunk_max_update_count=chunk_size, **kwargs)

    monkeypatch.setattr(orphan_repair, "write_updated_docs", chunked_write)


def test_failed_items_across_several_chunks_are_counted_per_item(monkeypatch):
    registry = FlakyRegistry()
    registry.add_collection(C1)
    for n in range(12):
        registry.add_product(product(n))
    registry.add_refs(C1, [product(n) for n in range(12)])
    registry.blocked = {product(3), product(4), product(9)}
    registry.vanish_before_bulk = {product(6)}
    _write_in_chunks_of(monkeypatch, 5)

    report = run(registry)

    assert len(registry.bulk_calls) == 3
    assert report.repairable == 12
    assert (report.repaired, report.already_current, report.vanished_before_write, report.write_failed) == (8, 0, 1, 3)
    assert report.repairable == report.repaired + report.already_current + report.vanished_before_write + report.write_failed
    assert report.write_failures_by_type == {"indexing_block_exception": 3}
    assert all(REFS not in registry.registry[product(n)] for n in (3, 4, 9))


def test_failures_are_stated_in_the_summary_and_the_completion_line(monkeypatch, capsys):
    flaky = FlakyRegistry()
    flaky.add_collection(C1)
    for n in range(4):
        flaky.add_product(product(n))
    flaky.add_product(product(10))  # unresolved
    flaky.add_refs(C1, [product(n) for n in range(4)])
    flaky.blocked = {product(1), product(2)}
    messages = _capture_info(monkeypatch)

    _run_cli(monkeypatch, flaky)

    out = capsys.readouterr().out
    assert "  repaired: 2" in out
    assert "  write failures: 2 (indexing_block_exception: 2)" in out
    assert "Re-run to repair the remaining products; the repair is idempotent." in out
    completion = [m for m in messages if m.startswith("Orphan repair complete")]
    assert completion == ["Orphan repair complete: 5 examined, 2 repaired, 2 failed, 1 unresolved"]


def test_a_rerun_repairs_what_failed_and_the_summary_has_no_failure_text_when_nothing_failed(monkeypatch):
    registry = FlakyRegistry()
    registry.add_collection(C1)
    for n in range(6):
        registry.add_product(product(n))
    registry.add_refs(C1, [product(n) for n in range(6)])
    registry.blocked = {product(2), product(5)}

    first = run(registry)
    registry.blocked = set()
    second = run(registry)

    assert (first.repaired, first.write_failed, second.repaired, second.write_failed) == (4, 2, 2, 0)
    assert "Re-run" not in "\n".join(second.summary_lines())
    assert "  write failures: 0\n" in "\n".join(second.summary_lines()) + "\n"


def test_documents_written_before_a_throttled_chunk_was_resubmitted_are_still_reported_as_repaired(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda seconds: None)
    registry = FlakyRegistry()
    registry.add_collection(C1)
    for n in range(5):
        registry.add_product(product(n))
    registry.add_refs(C1, [product(n) for n in range(5)])
    registry.throttle_once = {product(4)}

    report = run(registry)

    assert len(registry.bulk_calls) == 2  # the chunk was submitted twice
    assert (report.repaired, report.already_current, report.write_failed) == (5, 0, 0)


def test_write_outcomes_distinguish_noop_from_updated_vanished_and_failed():
    class Client:
        def bulk(self, **kwargs):
            return {
                "errors": True,
                "items": [
                    {"update": {"_id": "a", "result": "updated"}},
                    {"update": {"_id": "b", "result": "noop"}},
                    {"update": {"_id": "c", "status": 404, "error": {"type": "document_missing_exception", "reason": "x"}}},
                    {"update": {"_id": "d", "status": 400, "error": {"type": "mapper_parsing_exception", "reason": "x"}}},
                ],
            }

    updates = [Update(id=i, content={"k": "v"}) for i in "abcd"]
    summary = write_updated_docs(Client(), updates, index_name="registry")
    report = OrphanRepairReport(dry_run=False)
    report.absorb_write_summary(summary)

    assert (report.repaired, report.already_current, report.vanished_before_write, report.write_failed) == (1, 1, 1, 1)
    assert report.write_failures_by_type == {"mapper_parsing_exception": 1}


# ---------------------------------------------------------------------------------------------------------------------
# paging and request economy
# ---------------------------------------------------------------------------------------------------------------------


def test_pages_beyond_one_page_are_all_examined(registry):
    registry.add_collection(C1)
    count = 25
    for n in range(count):
        registry.add_product(product(n))
    registry.add_refs(C1, [product(n) for n in range(count)])

    report = run(registry, batch_size=10)

    assert report.products_examined == count and report.repaired == count
    product_pages = [c for c in registry.search_calls if c["index"] == "registry" and "lidvid" == c["sort"][0]]
    assert len(product_pages) >= 3
    assert all(c["size"] <= 10 for c in registry.search_calls if c["index"] == "registry" and c["body"])
    # later pages continue from the last sort value rather than skipping with from/size
    assert any("search_after" in c["body"] for c in product_pages)


def test_unresolved_products_beyond_one_page_do_not_end_paging_early(registry):
    registry.add_collection(C1)
    for n in range(30):
        registry.add_product(product(n))
    registry.add_refs(C1, [product(n) for n in range(0, 30, 2)])  # every other product is listed

    report = run(registry, batch_size=5)

    assert report.products_examined == 30
    assert report.repaired == 15 and report.unresolved == 15


def test_paging_survives_documents_leaving_the_result_set_between_pages(registry):
    for n in range(23):
        registry.add_product(product(n))

    seen = []
    for hits in iterate_pages_search_after(
        registry, "registry", build_orphan_products_query(), ["lidvid"], ["lidvid"], page_size=5
    ):
        seen += [h["_id"] for h in hits]
        for h in hits:  # repair the page just read, as a concurrent write would
            registry.registry[h["_id"]][STAMP] = SWEEPERS_ANCESTRY_VERSION
            registry.registry[h["_id"]][REFS] = ["x"]

    assert seen == sorted(product(n) for n in range(23))


def test_page_size_above_the_cap_is_rejected(registry):
    with pytest.raises(ValueError):
        list(iterate_pages_search_after(registry, "registry", {"query": {"match_all": {}}}, [], ["lidvid"], MAX_PAGE_SIZE + 1))
    with pytest.raises(ValueError):
        run(registry, batch_size=MAX_PAGE_SIZE + 1)
    with pytest.raises(ValueError):
        run(registry, batch_size=0)


def test_refs_are_looked_up_per_batch_not_per_product(registry):
    registry.add_collection(C1)
    for n in range(40):
        registry.add_product(product(n))
    registry.add_refs(C1, [product(n) for n in range(40)])

    run(registry, batch_size=10)

    refs_queries = [c for c in registry.search_calls if c["index"] == "registry-refs"]
    product_pages = [
        c for c in registry.search_calls if c["index"] == "registry" and "must_not" in json.dumps(c["body"])
    ]
    # one terms query per batch of 10, not 40; the product query makes one extra (empty) call to find the end
    assert len(refs_queries) == 4
    assert len(product_pages) == 5
    assert all(len(next(iter(c["body"]["query"]["terms"].values()))) <= 10 for c in refs_queries)


def test_collection_existence_is_looked_up_once_per_collection(registry):
    registry.add_collection(C1)
    for n in range(40):
        registry.add_product(product(n))
    registry.add_refs(C1, [product(n) for n in range(40)])

    run(registry, batch_size=10)

    collection_lookups = [
        c for c in registry.search_calls if c["index"] == "registry" and '"terms": {"lidvid"' in json.dumps(c["body"])
    ]
    assert len(collection_lookups) == 1


def test_refs_matching_more_documents_than_a_page_are_paged(registry, monkeypatch):
    monkeypatch.setattr(orphan_repair, "REFS_PAGE_SIZE", 2)
    registry.add_collection(C1)
    for n in range(7):
        registry.add_product(product(n))
        registry.add_refs(C1, [product(n)], batch_id=n)  # seven refs documents match one batch

    report = run(registry, batch_size=10)

    assert report.repaired == 7


def test_nothing_is_held_for_unresolved_products(registry):
    """Unresolved products are counted as they stream past, never accumulated."""
    for n in range(50):
        registry.add_product(product(n))

    report = run(registry, batch_size=7)

    assert report.unresolved == 50
    assert registry.bulk_calls == []


# ---------------------------------------------------------------------------------------------------------------------
# listing file
# ---------------------------------------------------------------------------------------------------------------------


def test_output_file_lists_repairable_products_and_optionally_the_rest(registry, tmp_path):
    registry.add_collection(C1)
    registry.add_product(product(1))
    registry.add_product(product(2))
    registry.add_product(product(3))
    registry.add_refs(C1, [product(1)])
    registry.add_refs(MISSING_COLLECTION, [product(3)])

    only_repairable = tmp_path / "repairable.tsv"
    everything = tmp_path / "all.tsv"
    run(registry, dry_run=True, output_file=str(only_repairable))
    run(registry, dry_run=True, output_file=str(everything), output_all=True)

    assert only_repairable.read_text().splitlines() == [
        "status\tproduct_lidvid\tcollection_lidvids",
        f"repairable\t{product(1)}\t{C1}",
    ]
    assert everything.read_text().splitlines()[1:] == [
        f"repairable\t{product(1)}\t{C1}",
        f"no_refs_listing\t{product(2)}\t",
        f"collection_not_in_registry\t{product(3)}\t{MISSING_COLLECTION}",
    ]


# ---------------------------------------------------------------------------------------------------------------------
# --dry-run (listing on stdout) and the default (writing); --output-file as an additional destination
# ---------------------------------------------------------------------------------------------------------------------

HEADER = "status\tproduct_lidvid\tcollection_lidvids"


def _populate_mixed(registry: FakeRegistry) -> None:
    registry.add_collection(C1)
    registry.add_product(product(1))  # repairable
    registry.add_product(product(2))  # no refs listing
    registry.add_product(product(3))  # listed only under a collection which is not in the registry
    registry.add_refs(C1, [product(1)])
    registry.add_refs(MISSING_COLLECTION, [product(3)])


def _split_stdout(text: str):
    """The listing lines (up to the first line of the summary) and the summary lines."""
    lines = text.splitlines()
    summary_start = next(i for i, line in enumerate(lines) if line.startswith("Ancestry orphan repair"))
    return lines[:summary_start], lines[summary_start:]


def test_dry_run_lists_repairable_products_to_stdout_then_the_summary_and_writes_nothing(registry, capsys):
    _populate_mixed(registry)
    before = snapshot(registry)

    report = run(registry, dry_run=True)

    listing, summary = _split_stdout(capsys.readouterr().out)
    assert listing == [HEADER, f"repairable\t{product(1)}\t{C1}"]
    assert summary == report.summary_lines()  # the summary is last
    assert "DRY RUN" in summary[0]
    assert "--output-file" not in "\n".join(summary)
    assert (report.repairable, report.repaired) == (1, 0)
    assert registry.bulk_calls == [] and registry.written_ids == []
    assert registry.registry == before
    assert registry.indices.put_mapping_calls == []


def test_dry_run_stdout_is_not_interleaved_with_log_lines(registry, capsys):
    _populate_mixed(registry)

    run(registry, dry_run=True, log_level=logging.DEBUG)

    captured = capsys.readouterr()
    assert "Starting ancestry orphan repair" not in captured.out  # log records go to stderr only
    assert "::INFO::" not in captured.out
    assert "::INFO::Starting ancestry orphan repair" in captured.err


def test_dry_run_with_output_all_lists_the_unresolved_products_too(registry, capsys):
    _populate_mixed(registry)

    run(registry, dry_run=True, output_all=True)

    listing, _ = _split_stdout(capsys.readouterr().out)
    assert listing[1:] == [
        f"repairable\t{product(1)}\t{C1}",
        f"no_refs_listing\t{product(2)}\t",
        f"collection_not_in_registry\t{product(3)}\t{MISSING_COLLECTION}",
    ]


def test_dry_run_with_output_file_writes_identical_content_to_stdout_and_the_file(registry, capsys, tmp_path):
    _populate_mixed(registry)
    out = tmp_path / "listing.tsv"

    run(registry, dry_run=True, output_file=str(out), output_all=True)

    listing, _ = _split_stdout(capsys.readouterr().out)
    assert out.read_text().splitlines() == listing
    assert out.read_text().startswith(HEADER + "\n")
    assert len(listing) == 4
    assert registry.bulk_calls == []








def test_output_file_without_dry_run_lists_the_repaired_products(registry, capsys, tmp_path):
    _populate_mixed(registry)
    out = tmp_path / "repaired.tsv"

    report = run(registry, output_file=str(out))

    assert capsys.readouterr().out == ""
    assert out.read_text().splitlines() == [HEADER, f"repairable\t{product(1)}\t{C1}"]
    assert report.repaired == 1 and registry.written_ids == [product(1)]  # the listed product is the one written


def test_output_file_without_dry_run_and_output_all_adds_the_unresolved_products(registry, tmp_path):
    _populate_mixed(registry)
    out = tmp_path / "all.tsv"

    run(registry, output_file=str(out), output_all=True)

    assert out.read_text().splitlines()[1:] == [
        f"repairable\t{product(1)}\t{C1}",
        f"no_refs_listing\t{product(2)}\t",
        f"collection_not_in_registry\t{product(3)}\t{MISSING_COLLECTION}",
    ]




def test_output_file_help_does_not_imply_a_dry_run():
    import argparse

    ap = argparse.ArgumentParser()
    orphan_repair._configure_cli(ap)
    help_text = {a.dest: a.help for a in ap._actions}

    assert "dry run" not in help_text["output_file"].lower()
    assert help_text["output_file"].startswith("also write the listing")
    assert "stdout" in help_text["dry_run"]


# the module command line


def _run_cli(monkeypatch, registry, *args):
    monkeypatch.setattr(orphan_repair, "get_userpass_opensearch_client", lambda **kwargs: registry)
    monkeypatch.setattr(sys, "argv", ["ancestry_orphan_repair", "--base-URL", "http://registry.invalid", *args])
    orphan_repair.main()


def test_cli_dry_run_prints_the_listing_then_the_summary_once(monkeypatch, registry, capsys):
    _populate_mixed(registry)

    _run_cli(monkeypatch, registry, "--dry-run")

    out = capsys.readouterr().out
    listing, summary = _split_stdout(out)
    assert listing == [HEADER, f"repairable\t{product(1)}\t{C1}"]
    assert out.count("Ancestry orphan repair - DRY RUN") == 1
    assert registry.bulk_calls == []





def test_default_run_writes_the_repairs_and_prints_only_the_summary_from_the_cli(monkeypatch, registry, capsys):
    _populate_mixed(registry)

    _run_cli(monkeypatch, registry)

    out = capsys.readouterr().out
    assert out.splitlines()[0].startswith("Ancestry orphan repair - APPLIED")
    assert HEADER not in out and "repairable\t" not in out
    assert registry.written_ids == [product(1)]


def test_default_run_prints_no_listing_and_no_summary_from_run(registry, capsys):
    _populate_mixed(registry)

    report = run(registry)

    assert capsys.readouterr().out == ""
    assert not report.dry_run and report.repaired == 1
    assert registry.written_ids == [product(1)]


def _capture_info(monkeypatch):
    messages = []
    monkeypatch.setattr(orphan_repair.log, "info", lambda msg, *a, **k: messages.append(str(msg)))
    return messages


def test_cli_dry_run_prints_the_summary_once_and_logs_a_single_completion_line(monkeypatch, registry, capsys):
    _populate_mixed(registry)
    messages = _capture_info(monkeypatch)

    _run_cli(monkeypatch, registry, "--dry-run")

    out = capsys.readouterr().out
    assert out.count("Ancestry orphan repair - DRY RUN") == 1
    assert out.count("products examined") == 1
    assert out.count("data problems for the node") == 1
    completion = [m for m in messages if m.startswith("Orphan repair complete")]
    assert completion == ["Orphan repair complete: 3 examined, 1 repairable, 2 unresolved"]
    assert not [m for m in messages if "products examined" in m or "DRY RUN" in m or "data problems" in m]
    assert not [m for m in messages if m.startswith("  ")]


def test_cli_default_run_prints_the_summary_once_and_logs_a_single_completion_line(monkeypatch, registry, capsys):
    _populate_mixed(registry)
    messages = _capture_info(monkeypatch)

    _run_cli(monkeypatch, registry)

    out = capsys.readouterr().out
    assert out.count("Ancestry orphan repair - APPLIED") == 1
    assert out.count("  repaired: 1") == 1
    assert out.count("data problems for the node") == 1
    completion = [m for m in messages if m.startswith("Orphan repair complete")]
    assert completion == ["Orphan repair complete: 3 examined, 1 repaired, 2 unresolved"]
    assert not [m for m in messages if "repaired:" in m or "APPLIED" in m or "data problems" in m]
    assert not [m for m in messages if m.startswith("  ")]


def test_cli_output_file_without_dry_run_lists_the_repaired_products(monkeypatch, registry, capsys, tmp_path):
    _populate_mixed(registry)
    out_file = tmp_path / "repaired.tsv"

    _run_cli(monkeypatch, registry, "--output-file", str(out_file))

    out = capsys.readouterr().out
    assert HEADER not in out
    assert out_file.read_text().splitlines() == [HEADER, f"repairable\t{product(1)}\t{C1}"]
    assert registry.written_ids == [product(1)]


def test_cli_dry_run_with_output_file_writes_identical_content_to_stdout_and_the_file(
    monkeypatch, registry, capsys, tmp_path
):
    _populate_mixed(registry)
    out_file = tmp_path / "listing.tsv"

    _run_cli(monkeypatch, registry, "--dry-run", "--output-file", str(out_file), "--output-all")

    listing, _ = _split_stdout(capsys.readouterr().out)
    assert out_file.read_text().splitlines() == listing
    assert len(listing) == 4
    assert registry.bulk_calls == []


def test_apply_is_not_a_recognised_argument(monkeypatch, registry, capsys):
    _populate_mixed(registry)

    with pytest.raises(SystemExit) as excinfo:
        _run_cli(monkeypatch, registry, "--apply")

    assert excinfo.value.code == 2
    assert "unrecognized arguments: --apply" in capsys.readouterr().err
    assert registry.bulk_calls == [] and registry.search_calls == []
