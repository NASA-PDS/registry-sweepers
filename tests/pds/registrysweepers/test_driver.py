import logging
import sys

import pytest
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

    assert sweeper_calls == ["provenance", "ancestry", "ancestry-orphan-repair", "reindexer"]


def test_run_only_single_sweeper(monkeypatch):
    sweeper_calls = _run_driver_with_args(monkeypatch, ["--only", "ancestry"])

    assert sweeper_calls == ["ancestry"]


def test_run_only_multiple_sweepers(monkeypatch):
    sweeper_calls = _run_driver_with_args(monkeypatch, ["--only", "ancestry", "provenance"])

    assert sweeper_calls == ["ancestry", "provenance"]


def test_run_only_legacy_sync(monkeypatch):
    sweeper_calls = _run_driver_with_args(monkeypatch, ["--only", "legacy-sync"])

    assert sweeper_calls == ["legacy-sync"]


def test_orphan_repair_is_part_of_the_default_suite_and_writes_like_the_other_sweepers(monkeypatch):
    kwargs = {}
    sweeper_calls = _run_driver_with_args(monkeypatch, [], kwargs)

    assert sweeper_calls.index("ancestry-orphan-repair") == sweeper_calls.index("ancestry") + 1
    assert "dry_run" not in kwargs["ancestry-orphan-repair"]
    assert all(kwargs[name] == {} for name in ("provenance", "ancestry", "reindexer"))


def test_run_only_orphan_repair_writes_by_default(monkeypatch):
    kwargs = {}
    sweeper_calls = _run_driver_with_args(monkeypatch, ["--only", "ancestry-orphan-repair"], kwargs)

    assert sweeper_calls == ["ancestry-orphan-repair"]
    assert kwargs["ancestry-orphan-repair"] == {"output_file": None, "output_all": False}


def test_orphan_repair_receives_output_options_and_other_sweepers_do_not(monkeypatch):
    kwargs = {}
    args = ["--only", "provenance", "ancestry-orphan-repair", "--output-file", "/tmp/x.tsv", "--output-all"]
    _run_driver_with_args(monkeypatch, args, kwargs)

    assert kwargs["ancestry-orphan-repair"] == {"output_file": "/tmp/x.tsv", "output_all": True}
    assert kwargs["provenance"] == {}


def test_dry_run_is_accepted_with_only_the_orphan_repair_and_reaches_nothing_else(monkeypatch):
    kwargs = {}
    sweeper_calls = _run_driver_with_args(monkeypatch, ["--only", "ancestry-orphan-repair", "--dry-run"], kwargs)

    assert sweeper_calls == ["ancestry-orphan-repair"]
    assert kwargs["ancestry-orphan-repair"] == {"dry_run": True, "output_file": None, "output_all": False}


def test_dry_run_with_output_file_reaches_the_orphan_repair(monkeypatch):
    kwargs = {}
    args = ["--only", "ancestry-orphan-repair", "--dry-run", "--output-file", "/tmp/x.tsv", "--output-all"]
    _run_driver_with_args(monkeypatch, args, kwargs)

    assert kwargs["ancestry-orphan-repair"] == {"dry_run": True, "output_file": "/tmp/x.tsv", "output_all": True}


@pytest.mark.parametrize(
    "selection, unsupported",
    [
        ([], "provenance, ancestry, reindexer"),  # the default suite includes sweepers which write
        (["--only", "ancestry", "ancestry-orphan-repair"], "ancestry"),
        (["--only", "provenance"], "provenance"),
        (["--only", "ancestry"], "ancestry"),
        (["--only", "reindexer"], "reindexer"),
        (["--only", "legacy-sync"], "legacy-sync"),
    ],
)
def test_dry_run_is_rejected_unless_every_selected_sweeper_supports_it(monkeypatch, capsys, selection, unsupported):
    sweeper_calls = []

    with pytest.raises(SystemExit) as excinfo:
        sweeper_calls = _run_driver_with_args(monkeypatch, [*selection, "--dry-run"])

    assert excinfo.value.code == 2
    assert sweeper_calls == []  # nothing ran
    err = capsys.readouterr().err
    assert "--dry-run is only supported by ancestry-orphan-repair" in err
    assert f"would write for real with {unsupported}." in err
    assert "--only ancestry-orphan-repair --dry-run" in err


def test_dry_run_sweepers_is_the_list_the_guard_uses():
    assert driver.DRY_RUN_SWEEPERS == ["ancestry-orphan-repair"]


def test_apply_is_not_a_recognised_argument(monkeypatch, capsys):
    sweeper_calls = []

    with pytest.raises(SystemExit) as excinfo:
        sweeper_calls = _run_driver_with_args(monkeypatch, ["--only", "ancestry-orphan-repair", "--apply"])

    assert excinfo.value.code == 2
    assert sweeper_calls == []
    assert "unrecognized arguments: --apply" in capsys.readouterr().err


def test_orphan_repair_runs_after_ancestry_whatever_the_requested_order(monkeypatch):
    sweeper_calls = _run_driver_with_args(
        monkeypatch, ["--only", "ancestry-orphan-repair", "reindexer", "ancestry", "provenance"]
    )

    assert sweeper_calls == ["reindexer", "ancestry", "ancestry-orphan-repair", "provenance"]


@pytest.mark.parametrize(
    "selection",
    [[], ["--only", "ancestry", "ancestry-orphan-repair"]],
)
def test_output_file_is_refused_when_more_than_one_listing_sweeper_is_selected(monkeypatch, capsys, selection):
    sweeper_calls = []

    with pytest.raises(SystemExit) as excinfo:
        sweeper_calls = _run_driver_with_args(monkeypatch, [*selection, "--output-file", "/tmp/x.tsv"])

    assert excinfo.value.code == 2
    assert sweeper_calls == []  # nothing ran
    err = capsys.readouterr().err
    assert "--output-file would be written by more than one sweeper (ancestry, ancestry-orphan-repair)" in err


def test_output_file_is_accepted_with_a_single_listing_sweeper(monkeypatch):
    kwargs = {}
    _run_driver_with_args(monkeypatch, ["--only", "ancestry-orphan-repair", "--output-file", "/tmp/x.tsv"], kwargs)

    assert kwargs["ancestry-orphan-repair"]["output_file"] == "/tmp/x.tsv"
