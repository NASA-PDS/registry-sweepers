#
# Copyright © 2023, California Institute of Technology ("Caltech").
# U.S. Government sponsorship acknowledged.
#
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# • Redistributions of source code must retain the above copyright notice,
#   this list of conditions and the following disclaimer.
# • Redistributions must reproduce the above copyright notice, this list of
#   conditions and the following disclaimer in the documentation and/or other
#   materials provided with the distribution.
# • Neither the name of Caltech nor its operating division, the Jet Propulsion
#   Laboratory, nor the names of its contributors may be used to endorse or
#   promote products derived from this software without specific prior written
#   permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#
# Python driver for provenance (OUTDATED TODO: Update documentation)
# ============================
#
# This script is provided to support the scheduled execution of PDS Registry
# Provenance, typically in AWS via Event Bridge and ECS/Fargate.
#
# This script makes the following assumptions for its run-time:
#
# - The EN (i.e. primary) OpenSearch endpoint is provided in the environment
#   variable PROV_ENDPOINT
# - The username/password is provided as a JSON key/value in the environment
#   variable PROV_CREDENTIALS
# - The remotes available through cross cluster search to be processed are
#   provided as a JSON list of strings - each string containing the space
#   separated list of remotes (as they appear on the provenance command line)
#   Each set of remotes is used in an execution of provenance. The value of
#   this is specified in the environment variable PROV_REMOTES. If this
#   variable is empty or not defined, provenance is run without specifying
#   remotes and only the PROV_ENDPOINT is processed.
# - The directory containing the provenance.py file is in PATH and is
#   executable.
#
#
import argparse
import functools
import inspect
import json
import logging
import os
from datetime import datetime
from typing import Callable
from typing import List

from pds.registrysweepers import legacy_registry_sync
from pds.registrysweepers import provenance
from pds.registrysweepers import repairkit
from pds.registrysweepers.ancestry import main as ancestry
from pds.registrysweepers.ancestry_orphan_repair import main as ancestry_orphan_repair
from pds.registrysweepers.reindexer import main as reindexer
from pds.registrysweepers.utils import configure_logging
from pds.registrysweepers.utils import parse_log_level
from pds.registrysweepers.utils.db.client import get_opensearch_client_from_environment
from pds.registrysweepers.utils.misc import get_human_readable_elapsed_since
from pds.registrysweepers.utils.misc import is_dev_mode
from pds.registrysweepers.utils.misc import limit_log_length


SWEEPER_REGISTRY = {
    "provenance": provenance.run,
    "ancestry": ancestry.run,
    "reindexer": reindexer.run,
    "legacy-sync": legacy_registry_sync.run,
    "ancestry-orphan-repair": ancestry_orphan_repair.run,
}

ORPHAN_REPAIR_SWEEPER = "ancestry-orphan-repair"

# Sweepers which write a listing to --output-file.
LISTING_SWEEPERS = ["ancestry", ORPHAN_REPAIR_SWEEPER]

# The orphan repair is part of the default suite and runs right after ancestry.  Like every other sweeper, it writes.
DEFAULT_SWEEPERS = ["provenance", "ancestry", ORPHAN_REPAIR_SWEEPER, "reindexer"]

# The sweepers which implement --dry-run.  Any other sweeper would write for real, so --dry-run is refused unless every
# selected sweeper is in this list.
DRY_RUN_SWEEPERS = [ORPHAN_REPAIR_SWEEPER]


def order_sweepers(names: List[str]) -> List[str]:
    """
    Keep the requested order, except that the orphan repair repairs what the ancestry sweeper leaves behind and so
    always runs immediately after it when both are selected.
    """
    ordered = list(names)
    if ORPHAN_REPAIR_SWEEPER in ordered and "ancestry" in ordered:
        ordered.remove(ORPHAN_REPAIR_SWEEPER)
        ordered.insert(ordered.index("ancestry") + 1, ORPHAN_REPAIR_SWEEPER)
    return ordered


def run():
    configure_logging(filepath=None, log_level=logging.INFO)
    log = logging.getLogger(__name__)

    dev_mode = is_dev_mode()
    if dev_mode:
        log.warning(limit_log_length("Operating in development mode - host verification disabled"))
        import urllib3

        urllib3.disable_warnings()

    log_level = parse_log_level(os.environ.get("LOGLEVEL", "INFO"))

    def run_factory(sweeper_f: Callable, **sweeper_options) -> Callable:
        return functools.partial(
            sweeper_f,
            client=get_opensearch_client_from_environment(verify_certs=True if not dev_mode else False),
            # enable for development if required - not necessary in production
            # log_filepath='registry-sweepers.log',
            log_level=log_level,
            **sweeper_options,
        )

    def module_name(f: Callable) -> str:
        mod = inspect.getmodule(f)
        return mod.__name__ if mod is not None else repr(f)

    parser = argparse.ArgumentParser(
        prog="registry-sweepers",
        description="sweeps the PDS registry with different routines meant to run regularly on the database",
    )
    parser.add_argument(
        "--only",
        nargs="+",
        metavar="SWEEPER",
        choices=SWEEPER_REGISTRY.keys(),
        help=f"Only run the specified sweeper(s). Choices: {', '.join(SWEEPER_REGISTRY.keys())}",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help=f"Read and generate everything but write nothing to the registry, and report what would be written.  Only "
        f"supported by: {', '.join(DRY_RUN_SWEEPERS)}.  Every selected sweeper must support it, so combine it with "
        f"--only.",
    )
    parser.add_argument(
        "--output-file",
        default=None,
        help=f"Only applies to {ORPHAN_REPAIR_SWEEPER}: also write the listing of the products it repairs (or, with "
        "--dry-run, would repair) as TSV to this file.",
    )
    parser.add_argument(
        "--output-all",
        action="store_true",
        default=False,
        help=f"Only applies to {ORPHAN_REPAIR_SWEEPER}: also list the unresolved products in the listing.",
    )

    args = parser.parse_args()

    names = order_sweepers(args.only if args.only else DEFAULT_SWEEPERS)
    sweepers = [SWEEPER_REGISTRY[name] for name in names]

    if args.dry_run:
        writers = [name for name in names if name not in DRY_RUN_SWEEPERS]
        if writers:
            parser.error(
                f"--dry-run is only supported by {', '.join(DRY_RUN_SWEEPERS)}, and would write for real with "
                f"{', '.join(writers)}.  Select only the sweepers which support it, e.g. --only {DRY_RUN_SWEEPERS[0]} "
                f"--dry-run."
            )

    # --output-file names one path.  The orphan repair lists products in it and the ancestry sweeper (on its own
    # branch) lists bundle links in it, so it is refused when more than one of them is selected rather than letting
    # the second overwrite the first.
    listing_sweepers = [name for name in names if name in LISTING_SWEEPERS]
    if args.output_file and len(listing_sweepers) > 1:
        parser.error(
            f"--output-file would be written by more than one sweeper ({', '.join(listing_sweepers)}), each with a "
            f"different listing.  Select one of them with --only."
        )

    # Passed only for a dry run, so that a normal run calls the sweepers exactly as before.
    dry_run_options = {"dry_run": True} if args.dry_run else {}
    orphan_repair_options = {"output_file": args.output_file, "output_all": args.output_all}
    if ORPHAN_REPAIR_SWEEPER not in names and (args.output_file or args.output_all):
        log.warning(limit_log_length(f"--output-file/--output-all have no effect without {ORPHAN_REPAIR_SWEEPER}"))

    sweeper_descriptions = [module_name(f) for f in sweepers]
    log.info(limit_log_length(f"Running sweepers: {sweeper_descriptions}"))

    total_execution_begin = datetime.now()

    sweeper_execution_duration_strs = []

    for name, sweeper in zip(names, sweepers):
        sweeper_execution_begin = datetime.now()
        sweeper_options = dict(dry_run_options) if name in DRY_RUN_SWEEPERS else {}
        if name == ORPHAN_REPAIR_SWEEPER:
            sweeper_options.update(orphan_repair_options)
        run_sweeper_f = run_factory(sweeper, **sweeper_options)

        run_sweeper_f()

        sweeper_execution_duration_strs.append(
            f"{module_name(sweeper)}: {get_human_readable_elapsed_since(sweeper_execution_begin)}"
        )

    log.info(
        limit_log_length(
            f"Sweepers successfully executed in {get_human_readable_elapsed_since(total_execution_begin)}\n   "
            + "\n   ".join(sweeper_execution_duration_strs)
        )
    )
