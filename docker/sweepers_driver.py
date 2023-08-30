#! /usr/bin/env python3
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
# Python driver for provenance
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

import functools
import inspect
import json
import logging
import os
from datetime import datetime
from typing import Callable

from pds.registrysweepers import provenance, ancestry, repairkit
from pds.registrysweepers.utils import configure_logging, get_human_readable_elapsed_since, parse_log_level

configure_logging(filepath=None, log_level=logging.INFO)
log = logging.getLogger(__name__)

dev_mode = str(os.environ.get("DEV_MODE")).lower() not in {'none', '', '0', 'false'}
if dev_mode:
    log.warning('Operating in development mode - host verification disabled')
    import urllib3

    urllib3.disable_warnings()

opensearch_endpoint = os.environ.get('PROV_ENDPOINT', '')
if opensearch_endpoint.strip() == '':
    raise RuntimeError('Environment variable PROV_ENDPOINT must be provided')
log.info(f'Targeting OpenSearch endpoint "{opensearch_endpoint}"')

try:
    provCredentialsStr = os.environ["PROV_CREDENTIALS"]
except KeyError:
    raise RuntimeError('Environment variable PROV_CREDENTIALS must be provided')

try:
    provCredentials = json.loads(provCredentialsStr)
    username = list(provCredentials.keys())[0]
    password = provCredentials[username]
except Exception as err:
    logging.error(err)
    raise ValueError(f'Failed to parse username/password from PROV_CREDENTIALS value "{provCredentialsStr}": {err}')

log_level = parse_log_level(os.environ.get('LOGLEVEL', 'INFO'))


def run_factory(sweeper_f: Callable) -> Callable:
    return functools.partial(
        sweeper_f,
        base_url=opensearch_endpoint,
        username=username,
        password=password,
        log_filepath='provenance.log',
        log_level=log_level,
        verify_host_certs=True if not dev_mode else False
    )


# Define sweepers to be run here, in order of execution
sweepers = [
    repairkit.run,
    provenance.run,
    ancestry.run
]

sweeper_descriptions = [inspect.getmodule(f).__name__ for f in sweepers]
log.info(f'Running sweepers: {sweeper_descriptions}')

execution_begin = datetime.now()

for sweeper in sweepers:
    run_sweeper_f = run_factory(sweeper)
    run_sweeper_f()

log.info(f'Sweepers successfully executed in {get_human_readable_elapsed_since(execution_begin)}')
