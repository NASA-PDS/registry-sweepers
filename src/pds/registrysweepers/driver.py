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

from pds.registrysweepers import legacy_registry_sync
from pds.registrysweepers import provenance
from pds.registrysweepers import repairkit
from pds.registrysweepers.ancestry import main as ancestry
from pds.registrysweepers.reindexer import main as reindexer
from pds.registrysweepers.utils import configure_logging
from pds.registrysweepers.utils import parse_log_level
from pds.registrysweepers.utils.db.client import get_opensearch_client_from_environment
from pds.registrysweepers.utils.misc import get_human_readable_elapsed_since
from pds.registrysweepers.utils.misc import is_dev_mode
from pds.registrysweepers.utils.misc import limit_log_length


def run():
    configure_logging(filepath=None, log_level=logging.INFO)
    log = logging.getLogger(__name__)

    dev_mode = is_dev_mode()
    if dev_mode:
        log.warning(limit_log_length("Operating in development mode - host verification disabled"))
        import urllib3

        urllib3.disable_warnings()

    log_level = parse_log_level(os.environ.get("LOGLEVEL", "INFO"))

    def run_factory(sweeper_f: Callable) -> Callable:
        return functools.partial(
            sweeper_f,
            client=get_opensearch_client_from_environment(verify_certs=True if not dev_mode else False),
            # enable for development if required - not necessary in production
            # log_filepath='registry-sweepers.log',
            log_level=log_level,
        )

    parser = argparse.ArgumentParser(
        prog="registry-sweepers",
        description="sweeps the PDS registry with different routines meant to run regularly on the database",
    )

    # define optional sweepers
    parser.add_argument("--legacy-sync", action="store_true")
    optional_sweepers = {"legacy_sync": legacy_registry_sync.run}

    args = parser.parse_args()

    # Define default sweepers to be run here, in order of execution
    sweepers = [
        repairkit.run,
        provenance.run,
        ancestry.run,
        reindexer.run,
    ]

    for option, sweeper in optional_sweepers.items():
        if getattr(args, option):
            sweepers.append(sweeper)

    sweeper_descriptions = [inspect.getmodule(f).__name__ for f in sweepers]
    log.info(limit_log_length(f"Running sweepers: {sweeper_descriptions}"))

    total_execution_begin = datetime.now()

    sweeper_execution_duration_strs = []

    for sweeper in sweepers:
        sweeper_execution_begin = datetime.now()
        run_sweeper_f = run_factory(sweeper)

        run_sweeper_f()

        sweeper_name = inspect.getmodule(sweeper).__name__
        sweeper_execution_duration_strs.append(
            f"{sweeper_name}: {get_human_readable_elapsed_since(sweeper_execution_begin)}"
        )

    log.info(
        limit_log_length(
            f"Sweepers successfully executed in {get_human_readable_elapsed_since(total_execution_begin)}\n   "
            + "\n   ".join(sweeper_execution_duration_strs)
        )
    )
