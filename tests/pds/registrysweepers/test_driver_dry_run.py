import logging
import sys

import pytest
from pds.registrysweepers import driver


def _run_driver_with_args(monkeypatch, args):
    """Run the driver with fake sweepers, returning [(sweeper name, keyword arguments it was called with)]"""
    sweeper_calls = []
    clients_created = []

    def _make_sweeper(name):
        def _run(**kwargs):
            sweeper_calls.append((name, {k: v for k, v in kwargs.items() if k not in ("client", "log_level")}))

        return _run

    def _create_client(**kwargs):
        clients_created.append(kwargs)
        return object()

    monkeypatch.setattr(driver, "configure_logging", lambda *args, **kwargs: None)
    monkeypatch.setattr(driver, "parse_log_level", lambda *_: logging.INFO)
    monkeypatch.setattr(driver, "get_opensearch_client_from_environment", _create_client)
    monkeypatch.setattr(driver, "is_dev_mode", lambda: False)
    monkeypatch.setattr(driver, "get_human_readable_elapsed_since", lambda *_: "0s")
    monkeypatch.setattr(driver, "limit_log_length", lambda text: text)

    for name in ("provenance", "ancestry", "reindexer", "legacy-sync"):
        monkeypatch.setitem(driver.SWEEPER_REGISTRY, name, _make_sweeper(name))

    monkeypatch.setattr(sys, "argv", ["registry-sweepers", *args])
    try:
        driver.run()
    finally:
        # a refused run must not have connected to, let alone run anything against, the registry
        if sweeper_calls == []:
            assert clients_created == []

    return sweeper_calls


def test_dry_run_of_ancestry_is_passed_to_ancestry(monkeypatch):
    assert _run_driver_with_args(monkeypatch, ["--only", "ancestry", "--dry-run"]) == [
        ("ancestry", {"dry_run": True, "output_file": None})
    ]


def test_output_file_is_passed_to_ancestry(monkeypatch):
    assert _run_driver_with_args(monkeypatch, ["--only", "ancestry", "--dry-run", "--output-file", "links.tsv"]) == [
        ("ancestry", {"dry_run": True, "output_file": "links.tsv"})
    ]


def test_without_dry_run_ancestry_is_called_as_before(monkeypatch):
    assert _run_driver_with_args(monkeypatch, ["--only", "ancestry"]) == [("ancestry", {})]


@pytest.mark.parametrize(
    "args",
    [
        ["--dry-run"],  # the default suite includes sweepers which write
        ["--only", "provenance", "--dry-run"],
        ["--only", "reindexer", "--dry-run"],
        ["--only", "legacy-sync", "--dry-run"],
        ["--only", "ancestry", "provenance", "--dry-run"],
        ["--only", "provenance", "ancestry", "--dry-run"],
    ],
)
def test_dry_run_is_refused_unless_every_selected_sweeper_supports_it(monkeypatch, capsys, args):
    with pytest.raises(SystemExit) as exit_info:
        _run_driver_with_args(monkeypatch, args)

    assert exit_info.value.code == 2  # argparse usage error
    error = capsys.readouterr().err
    assert "--dry-run is only supported by ancestry" in error
    assert "would write for real" in error


def test_the_error_names_the_sweepers_which_would_write(monkeypatch, capsys):
    with pytest.raises(SystemExit):
        _run_driver_with_args(monkeypatch, ["--only", "ancestry", "provenance", "--dry-run"])

    assert "would write for real with provenance" in capsys.readouterr().err


def test_output_file_without_dry_run_is_passed_to_ancestry(monkeypatch):
    assert _run_driver_with_args(monkeypatch, ["--only", "ancestry", "--output-file", "links.tsv"]) == [
        ("ancestry", {"output_file": "links.tsv"})
    ]


def test_output_file_is_passed_to_ancestry_only(monkeypatch):
    assert _run_driver_with_args(monkeypatch, ["--only", "provenance", "ancestry", "--output-file", "links.tsv"]) == [
        ("provenance", {}),
        ("ancestry", {"output_file": "links.tsv"}),
    ]


def test_output_file_with_the_default_suite_is_passed_to_ancestry(monkeypatch):
    calls = _run_driver_with_args(monkeypatch, ["--output-file", "links.tsv"])

    assert dict(calls)["ancestry"] == {"output_file": "links.tsv"}
    assert dict(calls)["provenance"] == {}
    assert dict(calls)["reindexer"] == {}


def test_output_file_without_ancestry_warns_that_it_has_no_effect(monkeypatch, caplog):
    with caplog.at_level(logging.WARNING):
        calls = _run_driver_with_args(monkeypatch, ["--only", "provenance", "--output-file", "links.tsv"])

    assert calls == [("provenance", {})]
    assert any("--output-file only applies to the ancestry sweeper" in message for message in caplog.messages)


def test_output_file_with_ancestry_does_not_warn(monkeypatch, caplog):
    with caplog.at_level(logging.WARNING):
        _run_driver_with_args(monkeypatch, ["--only", "ancestry", "--output-file", "links.tsv"])

    assert not any("--output-file" in message for message in caplog.messages)
