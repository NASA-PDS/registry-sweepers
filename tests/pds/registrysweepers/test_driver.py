import logging
import sys

from pds.registrysweepers import driver


def _run_driver_with_args(monkeypatch, args, sweeper_kwargs=None):
    sweeper_calls = []

    def _make_sweeper(name):
        def _run(*, client, log_level, **kwargs):
            sweeper_calls.append(name)
            if sweeper_kwargs is not None:
                sweeper_kwargs[name] = kwargs

        return _run

    monkeypatch.setattr(driver, "configure_logging", lambda *args, **kwargs: None)
    monkeypatch.setattr(driver, "parse_log_level", lambda *_: logging.INFO)
    monkeypatch.setattr(driver, "get_opensearch_client_from_environment", lambda **kwargs: object())
    monkeypatch.setattr(driver, "is_dev_mode", lambda: False)
    monkeypatch.setattr(driver, "get_human_readable_elapsed_since", lambda *_: "0s")
    monkeypatch.setattr(driver, "limit_log_length", lambda text: text)

    monkeypatch.setitem(driver.SWEEPER_REGISTRY, "provenance", _make_sweeper("provenance"))
    monkeypatch.setitem(driver.SWEEPER_REGISTRY, "ancestry", _make_sweeper("ancestry"))
    monkeypatch.setitem(driver.SWEEPER_REGISTRY, "reindexer", _make_sweeper("reindexer"))
    monkeypatch.setitem(driver.SWEEPER_REGISTRY, "legacy-sync", _make_sweeper("legacy-sync"))
    monkeypatch.setitem(driver.SWEEPER_REGISTRY, "ancestry-orphan-repair", _make_sweeper("ancestry-orphan-repair"))

    monkeypatch.setattr(sys, "argv", ["registry-sweepers", *args])
    driver.run()

    return sweeper_calls


def test_run_with_no_flags_runs_default_sweepers(monkeypatch):
    sweeper_calls = _run_driver_with_args(monkeypatch, [])

    assert sweeper_calls == ["provenance", "ancestry", "reindexer"]


def test_run_only_single_sweeper(monkeypatch):
    sweeper_calls = _run_driver_with_args(monkeypatch, ["--only", "ancestry"])

    assert sweeper_calls == ["ancestry"]


def test_run_only_multiple_sweepers(monkeypatch):
    sweeper_calls = _run_driver_with_args(monkeypatch, ["--only", "ancestry", "provenance"])

    assert sweeper_calls == ["ancestry", "provenance"]


def test_run_only_legacy_sync(monkeypatch):
    sweeper_calls = _run_driver_with_args(monkeypatch, ["--only", "legacy-sync"])

    assert sweeper_calls == ["legacy-sync"]


def test_orphan_repair_is_not_part_of_the_default_suite(monkeypatch):
    assert "ancestry-orphan-repair" not in _run_driver_with_args(monkeypatch, [])


def test_run_only_orphan_repair_is_a_dry_run_by_default(monkeypatch):
    kwargs = {}
    sweeper_calls = _run_driver_with_args(monkeypatch, ["--only", "ancestry-orphan-repair"], kwargs)

    assert sweeper_calls == ["ancestry-orphan-repair"]
    assert kwargs["ancestry-orphan-repair"] == {"apply": False, "output_file": None, "output_all": False}


def test_orphan_repair_receives_apply_and_output_options_and_other_sweepers_do_not(monkeypatch):
    kwargs = {}
    args = ["--only", "ancestry", "ancestry-orphan-repair", "--apply", "--output-file", "/tmp/x.tsv", "--output-all"]
    _run_driver_with_args(monkeypatch, args, kwargs)

    assert kwargs["ancestry-orphan-repair"] == {"apply": True, "output_file": "/tmp/x.tsv", "output_all": True}
    assert kwargs["ancestry"] == {}


def test_orphan_repair_runs_after_ancestry_whatever_the_requested_order(monkeypatch):
    sweeper_calls = _run_driver_with_args(
        monkeypatch, ["--only", "ancestry-orphan-repair", "reindexer", "ancestry", "provenance"]
    )

    assert sweeper_calls == ["reindexer", "ancestry", "ancestry-orphan-repair", "provenance"]
