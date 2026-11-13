import logging
import sys

from pds.registrysweepers import driver
from pds.registrysweepers.utils.misc import FORCE_REPROCESS_ENV_VAR


def _run_driver_with_args(monkeypatch, args, sweeper_kwargs=None):
    """
    Run the driver with stubbed sweepers and return the names of the sweepers called, in order.
    If a dict is provided as sweeper_kwargs, it is populated with the kwargs each sweeper was called with.
    """
    sweeper_calls = []

    def _make_sweeper(name):
        def _run(**kwargs):
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


def test_force_is_disabled_by_default(monkeypatch):
    monkeypatch.delenv(FORCE_REPROCESS_ENV_VAR, raising=False)
    sweeper_kwargs = {}

    _run_driver_with_args(monkeypatch, [], sweeper_kwargs)

    assert {name: kwargs["force"] for name, kwargs in sweeper_kwargs.items()} == {
        "provenance": False,
        "ancestry": False,
        "reindexer": False,
    }


def test_force_flag_is_passed_to_all_default_sweepers(monkeypatch):
    monkeypatch.delenv(FORCE_REPROCESS_ENV_VAR, raising=False)
    sweeper_kwargs = {}

    _run_driver_with_args(monkeypatch, ["--force"], sweeper_kwargs)

    assert {name: kwargs["force"] for name, kwargs in sweeper_kwargs.items()} == {
        "provenance": True,
        "ancestry": True,
        "reindexer": True,
    }


def test_overwrite_flag_is_an_alias_of_force(monkeypatch):
    monkeypatch.delenv(FORCE_REPROCESS_ENV_VAR, raising=False)
    sweeper_kwargs = {}

    _run_driver_with_args(monkeypatch, ["--only", "ancestry", "--overwrite"], sweeper_kwargs)

    assert sweeper_kwargs["ancestry"]["force"] is True


def test_force_environment_variable_enables_force(monkeypatch):
    monkeypatch.setenv(FORCE_REPROCESS_ENV_VAR, "true")
    sweeper_kwargs = {}

    _run_driver_with_args(monkeypatch, ["--only", "provenance"], sweeper_kwargs)

    assert sweeper_kwargs["provenance"]["force"] is True


def test_force_is_not_passed_to_sweepers_which_do_not_support_it(monkeypatch):
    monkeypatch.delenv(FORCE_REPROCESS_ENV_VAR, raising=False)
    sweeper_kwargs = {}

    _run_driver_with_args(monkeypatch, ["--only", "legacy-sync", "--force"], sweeper_kwargs)

    assert "force" not in sweeper_kwargs["legacy-sync"]
