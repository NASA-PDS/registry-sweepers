"""
Tests for the ancestry sweeper's dry run.

All registry access is mocked.  The scenario has, in one registry:
  - a pending collection (C1) with a pending bundle (B1) and two non-aggregate products (P1, P2) in its refs
  - a collection which already carries the current version but lacks its bundle link (C2, bundle B2)
"""
import runpy
import sys

import pytest
from pds.registrysweepers.ancestry import dryrun
from pds.registrysweepers.ancestry import main
from pds.registrysweepers.ancestry.dryrun import LINKS_FILE_HEADER
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.utils.db.readonly import ReadOnlyClientViolation
from pds.registrysweepers.utils.db.readonly import ReadOnlyOpenSearchClient

from ..builders import CollectionRefsBuilder
from .test_stamped_collection_bundle_linking import make_bundle
from .test_stamped_collection_bundle_linking import make_collection
from .test_stamped_collection_bundle_linking import register_registry

CURRENT = SWEEPERS_ANCESTRY_VERSION

B1_LID = "urn:nasa:pds:b1"
B1 = f"{B1_LID}::1.0"
C1_LID = "urn:nasa:pds:b1:data1"
C1 = f"{C1_LID}::1.0"
P1 = "urn:nasa:pds:b1:data1:p1::1.0"
P2 = "urn:nasa:pds:b1:data1:p2::1.0"

B2_LID = "urn:nasa:pds:b2"
B2 = f"{B2_LID}::1.0"
C2_LID = "urn:nasa:pds:b2:data2"
C2 = f"{C2_LID}::1.0"

READ_CALLS = {"search", "mget", "indices.exists", "indices.exists_alias", "indices.get_mapping"}


def register_scenario(client):
    register_registry(
        client,
        bundles=[make_bundle(B1, refs=[C1_LID]), make_bundle(B2, refs=[C2_LID], version=CURRENT)],
        collections=[make_collection(C1, version=None), make_collection(C2, ancestor_refs=[])],
        refs=[CollectionRefsBuilder(C1).with_product(P1).with_product(P2).build()],
    )


def assert_registry_untouched(client):
    assert client.bulk_calls == []
    assert client.indices.put_mapping_calls == []
    assert set(client.call_log) <= READ_CALLS, f"calls which are not reads: {set(client.call_log) - READ_CALLS}"


class TestDryRun:
    def test_issues_no_write_of_any_kind(self, mock_opensearch_client):
        register_scenario(mock_opensearch_client)

        main.run(client=mock_opensearch_client, dry_run=True)

        assert_registry_untouched(mock_opensearch_client)
        assert "bulk" not in mock_opensearch_client.call_log
        for forbidden in ("indices.put_mapping", "indices.create", "indices.delete", "delete", "delete_by_query"):
            assert forbidden not in mock_opensearch_client.call_log

    def test_still_reads_the_registry(self, mock_opensearch_client):
        register_scenario(mock_opensearch_client)

        main.run(client=mock_opensearch_client, dry_run=True)

        assert mock_opensearch_client.get_search_calls_for_index(".*registry-refs.*")  # non-aggregate refs were read
        assert mock_opensearch_client.get_search_calls_for_index("registry$")

    def test_counts_updates_by_kind(self, mock_opensearch_client):
        register_scenario(mock_opensearch_client)

        report = main.run(client=mock_opensearch_client, dry_run=True)

        assert report.pending_collections == 1  # C1
        assert report.collection_completions == 1  # C1, stamped after its non-aggregates
        assert report.bundles == 1  # B1; B2 is already stamped and is not rewritten
        assert report.stamped_collections_linked == 1  # C2
        assert report.stamped_collection_links == 1  # B2 -> C2
        assert report.nonaggregates == 2  # P1, P2
        assert report.unclassified == 0
        assert report.total_updates == 6

    def test_total_matches_what_a_real_run_writes(self, mock_opensearch_client):
        register_scenario(mock_opensearch_client)
        report = main.run(client=mock_opensearch_client, dry_run=True)

        written = []
        main.run(client=mock_opensearch_client, bulk_updates_sink=written)

        assert report.total_updates == len(written)

    def test_summary_is_logged_and_printed(self, mock_opensearch_client, capsys, monkeypatch):
        register_scenario(mock_opensearch_client)

        logged = []
        monkeypatch.setattr(dryrun.log, "info", logged.append)  # run() reconfigures logging, which defeats caplog

        main.run(client=mock_opensearch_client, dry_run=True)

        out = capsys.readouterr().out
        assert "DRY RUN (nothing was written to the registry)" in out
        assert "Document updates a real run would write: 6" in out
        assert "only get a bundle link added: 1 (1 bundle links)" in out
        assert "Non-aggregate products: 2" in out
        assert logged == ["Dry run complete: 6 document updates would be written; none were"]

    def test_reports_the_mappings_a_real_run_would_create_without_creating_them(self, mock_opensearch_client, capsys):
        register_scenario(mock_opensearch_client)

        main.run(client=mock_opensearch_client, dry_run=True)

        out = capsys.readouterr().out
        assert "missing - a real run would create it as keyword" in out
        assert mock_opensearch_client.indices.put_mapping_calls == []
        assert mock_opensearch_client.indices.mappings == {}

    def test_output_file_lists_only_the_bundle_links(self, mock_opensearch_client, tmp_path):
        register_scenario(mock_opensearch_client)
        output_file = tmp_path / "links.tsv"

        main.run(client=mock_opensearch_client, dry_run=True, output_file=str(output_file))

        assert output_file.read_text().splitlines() == [LINKS_FILE_HEADER, f"{C2}\t{B2}"]
        assert LINKS_FILE_HEADER == "collection_lidvid\tbundle_lidvid_added"

    def test_output_file_with_nothing_to_link_has_only_the_header(self, mock_opensearch_client, tmp_path):
        register_registry(mock_opensearch_client, bundles=[], collections=[])
        output_file = tmp_path / "links.tsv"

        report = main.run(client=mock_opensearch_client, dry_run=True, output_file=str(output_file))

        assert output_file.read_text().splitlines() == [LINKS_FILE_HEADER]
        assert report.total_updates == 0

    def test_a_collection_linked_to_several_bundles_gets_a_row_each(self, mock_opensearch_client, tmp_path):
        other_bundle = "urn:nasa:pds:b3::1.0"
        register_registry(
            mock_opensearch_client,
            bundles=[
                make_bundle(B2, refs=[C2_LID], version=CURRENT),
                make_bundle(other_bundle, refs=[C2_LID], version=CURRENT),
            ],
            collections=[make_collection(C2, ancestor_refs=[])],
        )
        output_file = tmp_path / "links.tsv"

        report = main.run(client=mock_opensearch_client, dry_run=True, output_file=str(output_file))

        assert output_file.read_text().splitlines() == [LINKS_FILE_HEADER, f"{C2}\t{B2}", f"{C2}\t{other_bundle}"]
        assert (report.stamped_collections_linked, report.stamped_collection_links) == (1, 2)

    def test_output_file_is_refused_without_dry_run(self, mock_opensearch_client, tmp_path):
        with pytest.raises(ValueError):
            main.run(client=mock_opensearch_client, output_file=str(tmp_path / "links.tsv"))

        assert mock_opensearch_client.call_log == []

    def test_run_hands_the_generation_a_client_which_cannot_write(self, mock_opensearch_client, monkeypatch):
        attempted_writes = []

        def generate_and_try_to_write(client):
            attempted_writes.append(client)
            client.bulk(body="")
            return []

        monkeypatch.setattr(main, "generate_product_update_records", generate_and_try_to_write)

        with pytest.raises(ReadOnlyClientViolation):
            main.run(client=mock_opensearch_client, dry_run=True)

        assert len(attempted_writes) == 1
        assert mock_opensearch_client.bulk_calls == []

    def test_a_write_reached_in_a_dry_run_fails_instead_of_writing(self, mock_opensearch_client):
        # the dry run hands the sweeper a client which cannot write, so a regression that reaches a write path stops
        guarded = ReadOnlyOpenSearchClient(mock_opensearch_client)

        with pytest.raises(ReadOnlyClientViolation):
            guarded.bulk(body="")
        with pytest.raises(ReadOnlyClientViolation):
            guarded.indices.put_mapping(index="registry", body={})

        assert mock_opensearch_client.bulk_calls == []
        assert mock_opensearch_client.indices.put_mapping_calls == []


class TestWithoutDryRun:
    def test_still_writes(self, mock_opensearch_client):
        register_scenario(mock_opensearch_client)

        main.run(client=mock_opensearch_client)

        assert len(mock_opensearch_client.bulk_calls) >= 1
        assert len(mock_opensearch_client.indices.put_mapping_calls) == 3  # two on registry, one on registry-refs

    def test_returns_no_report(self, mock_opensearch_client):
        register_scenario(mock_opensearch_client)

        assert main.run(client=mock_opensearch_client) is None


class TestModuleCli:
    def run_cli(self, monkeypatch, client, args):
        monkeypatch.setattr(main, "get_userpass_opensearch_client", lambda **kwargs: client)
        monkeypatch.setattr(sys, "argv", ["ancestry", "--base-URL", "https://example.invalid", *args])
        runpy.run_module("pds.registrysweepers.ancestry", run_name="__main__")

    def test_dry_run_option(self, mock_opensearch_client, monkeypatch, capsys):
        register_scenario(mock_opensearch_client)

        self.run_cli(monkeypatch, mock_opensearch_client, ["--dry-run"])

        assert "DRY RUN" in capsys.readouterr().out
        assert_registry_untouched(mock_opensearch_client)

    def test_output_file_option(self, mock_opensearch_client, monkeypatch, tmp_path):
        register_scenario(mock_opensearch_client)
        output_file = tmp_path / "links.tsv"

        self.run_cli(monkeypatch, mock_opensearch_client, ["--dry-run", "--output-file", str(output_file)])

        assert output_file.read_text().splitlines() == [LINKS_FILE_HEADER, f"{C2}\t{B2}"]

    def test_output_file_requires_dry_run(self, mock_opensearch_client, monkeypatch, tmp_path):
        register_scenario(mock_opensearch_client)

        with pytest.raises(SystemExit) as exit_info:
            self.run_cli(monkeypatch, mock_opensearch_client, ["--output-file", str(tmp_path / "links.tsv")])

        assert exit_info.value.code != 0
        assert mock_opensearch_client.call_log == []

    def test_without_dry_run_it_writes(self, mock_opensearch_client, monkeypatch):
        register_scenario(mock_opensearch_client)

        self.run_cli(monkeypatch, mock_opensearch_client, [])

        assert len(mock_opensearch_client.bulk_calls) >= 1
