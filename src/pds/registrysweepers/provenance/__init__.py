#! /usr/bin/env python3
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
# provenance
# ==========
#
# Determines if a particular document has been superseded by a more
# recent version, if upon which it has, sets the field
# ops:Provenance/ops:superseded_by to the id of the superseding document.
#
# It is important to note that the document is updated, not any dependent
# index.
#
import functools
import itertools
import logging
from typing import Dict
from typing import Iterable
from typing import List
from typing import Mapping
from typing import Union

from opensearchpy import OpenSearch
from pds.registrysweepers.provenance.constants import METADATA_SUCCESSOR_KEY
from pds.registrysweepers.provenance.provenancerecord import ProvenanceRecord
from pds.registrysweepers.provenance.versioning import SWEEPERS_PROVENANCE_VERSION
from pds.registrysweepers.provenance.versioning import SWEEPERS_PROVENANCE_VERSION_METADATA_KEY
from pds.registrysweepers.utils import configure_logging
from pds.registrysweepers.utils import parse_args
from pds.registrysweepers.utils.db import query_registry_db_with_search_after
from pds.registrysweepers.utils.db import write_updated_docs
from pds.registrysweepers.utils.db.client import get_userpass_opensearch_client
from pds.registrysweepers.utils.db.multitenancy import resolve_multitenant_index_name
from pds.registrysweepers.utils.db.update import Update
from pds.registrysweepers.utils.productidentifiers.pdslid import PdsLid

log = logging.getLogger(__name__)


def get_records_for_lid(client: OpenSearch, lid) -> Iterable[ProvenanceRecord]:
    log.info(f"Fetching docs and generating records for LID {lid}...")

    query = {
        "query": {"bool": {
            "must": [
                {"terms": {"ops:Tracking_Meta/ops:archive_status": ["archived", "certified"]}},
                {"term": {"lid": lid}}
            ]}}
    }
    _source = {"includes": ["lidvid", METADATA_SUCCESSOR_KEY, SWEEPERS_PROVENANCE_VERSION_METADATA_KEY]}

    docs = query_registry_db_with_search_after(
        client, resolve_multitenant_index_name(client, "registry"), query, _source
    )

    for doc in docs:
        try:
            yield ProvenanceRecord.from_doc(doc)
        except ValueError as err:
            log.warning(
                f'Failed to parse ProvenanceRecord from doc with id {doc["_id"]} due to {err} - source was {doc["_source"]}'
            )


def fetch_target_lids(client: OpenSearch, current_provenance_version: int = 99) -> Iterable[
    PdsLid]:  # TODO: remove version stub value

    def fetch_lids_chunk():
        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "terms": {
                                "ops:Tracking_Meta/ops:archive_status": ["archived", "certified"]
                            }
                        },
                        {
                            "bool": {
                                "should": [
                                    {
                                        "bool": {
                                            "must_not": {
                                                "exists": {
                                                    "field": METADATA_SUCCESSOR_KEY}}}
                                    },
                                    {
                                        "range": {
                                            "someVersionKey": {
                                                "lt": current_provenance_version
                                            }
                                        }
                                    }
                                ],
                                "minimum_should_match": 1}}]}},
            "aggs": {
                "unique_lids": {
                    "terms": {
                        "field": "lid",
                        "size": 10
                    }
                }
            },
            "size": 0
        }

        return client.search(
            index=resolve_multitenant_index_name(client, "registry"),
            body=query,
            size=0,
            _source_includes=[],
            track_total_hits=True,
        )

    # LIDs from the previous chunk are stored to avoid deplication in the event that  indexing lag causes LIDs to
    # persist in results
    previous_chunk_lids = set()
    response = fetch_lids_chunk()
    lids = [bucket['key'] for bucket in response["aggregations"]["unique_lids"]["buckets"]]

    while len(lids) > 0:
        for lid in lids:
            if lid not in previous_chunk_lids:
                yield lid

        logging.info(f"Fetched {len(lids)} LIDs from registry")
        previous_chunk_lids = set(lids)
        response = fetch_lids_chunk()
        lids = [bucket['key'] for bucket in response["aggregations"]["unique_lids"]["buckets"]]

    logging.info('No docs remain to process')


def generate_record_chains(client: OpenSearch, lids: Iterable[PdsLid]) -> Iterable[List[ProvenanceRecord]]:
    """
    Create an iterable of unsorted collections of records which share LIDs.
    :param client:
    """

    for lid in lids:
        record_chain = list(get_records_for_lid(client, lid))
        link_records_in_chain(record_chain)
        yield record_chain


def link_records_in_chain(record_chain: List[ProvenanceRecord]):
    """
    Given a List of ProvenanceRecords sharing the same LID, sort the list and create all elements' successor links
    """

    # this can theoretically be disabled for a minor performance improvement as records are already sorted when queried
    # but the benefit is likely to be minimal, and it's safer not to assume
    record_chain.sort(key=lambda record: record.lidvid)

    for i in range(len(record_chain) - 1):
        record = record_chain[i]
        successor_record = record_chain[i + 1]
        record.set_successor(successor_record.lidvid)


def run(
        client: OpenSearch,
        log_filepath: Union[str, None] = None,
        log_level: int = logging.INFO,
):
    configure_logging(filepath=log_filepath, log_level=log_level)

    log.info(f"Starting provenance v{SWEEPERS_PROVENANCE_VERSION} sweeper processing...")

    target_lids = fetch_target_lids(client)
    record_chains = generate_record_chains(client, target_lids)
    updates = generate_updates(itertools.chain.from_iterable(record_chains))

    write_updated_docs(
        client,
        updates,
        index_name=resolve_multitenant_index_name(client, "registry"),
    )

    log.info("Completed provenance sweeper processing!")


def generate_updates(records: Iterable[ProvenanceRecord]) -> Iterable[Update]:
    update_count = 0
    skipped_count = 0
    for record in records:
        update_content = {
            METADATA_SUCCESSOR_KEY: str(record.successor) if record.successor else None,
            SWEEPERS_PROVENANCE_VERSION_METADATA_KEY: SWEEPERS_PROVENANCE_VERSION,
        }

        if record.skip_write:
            skipped_count += 1
        else:
            update_count += 1
            yield Update(id=str(record.lidvid), content=update_content)

    log.info(f"Generated provenance updates for {update_count} products, skipping {skipped_count} up-to-date products")


if __name__ == "__main__":
    cli_description = f"""
    Update registry records for non-latest LIDVIDs with up-to-date direct successor metadata ({METADATA_SUCCESSOR_KEY}).

    Retrieves existing published LIDVIDs from the registry, determines history for each LID, and writes updated docs back to registry db.
    """

    cli_epilog = """EXAMPLES:

    - command for opensearch running in a container with the sockets published at 9200 for data ingested for full day March 11, 2020:

      registrysweepers.py -b https://localhost:9200 -p admin -u admin

    - getting more help on availables arguments and what is expected:

      registrysweepers.py --help

    """

    args = parse_args(description=cli_description, epilog=cli_epilog)
    client = get_userpass_opensearch_client(
        endpoint_url=args.base_URL, username=args.username, password=args.password, verify_certs=not args.insecure
    )

    run(
        client=client,
        log_level=args.log_level,
        log_filepath=args.log_file,
    )
