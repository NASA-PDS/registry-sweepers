import logging
import sys

from pds.registrysweepers import driver


def _run_driver_with_args(monkeypatch, args):
    sweeper_calls = []

    def _make_sweeper(name):
        def _run(*, client, log_level):
            sweeper_calls.append(name)

        return _run

    monkeypatch.setattr(driver, "configure_logging", lambda *args, **kwargs: None)
    monkeypatch.setattr(driver, "parse_log_level", lambda *_: logging.INFO)
    monkeypatch.setattr(driver, "get_opensearch_client_from_environment", lambda **kwargs: object())
    monkeypatch.setattr(driver, "is_dev_mode", lambda: False)
    monkeypatch.setattr(driver, "get_human_readable_elapsed_since", lambda *_: "0s")
    monkeypatch.setattr(driver, "limit_log_length", lambda text: text)

    monkeypatch.setattr(driver.repairkit, "run", _make_sweeper("repairkit"))
    monkeypatch.setattr(driver.provenance, "run", _make_sweeper("provenance"))
    monkeypatch.setattr(driver.ancestry, "run", _make_sweeper("ancestry"))
    monkeypatch.setattr(driver.reindexer, "run", _make_sweeper("reindexer"))
    monkeypatch.setattr(driver.legacy_registry_sync, "run", _make_sweeper("legacy_sync"))

    monkeypatch.setattr(sys, "argv", ["registry-sweepers", *args])
    driver.run()

    return sweeper_calls


def test_run_with_no_flags_runs_default_sweepers(monkeypatch):
    sweeper_calls = _run_driver_with_args(monkeypatch, [])

    assert sweeper_calls == ["repairkit", "provenance", "ancestry", "reindexer"]


def test_run_with_only_and_named_flag_runs_only_selected_default_sweeper(monkeypatch):
    sweeper_calls = _run_driver_with_args(monkeypatch, ["--only", "--repairkit"])

    assert sweeper_calls == ["repairkit"]


def test_run_with_only_and_multiple_named_flags_runs_all_selected_default_sweepers(monkeypatch):
    sweeper_calls = _run_driver_with_args(monkeypatch, ["--only", "--ancestry", "--provenance"])

    assert sweeper_calls == ["provenance", "ancestry"]
