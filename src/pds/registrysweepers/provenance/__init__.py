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
from collections.abc import Collection
from collections.abc import Set
from time import sleep
from typing import Dict
from typing import Iterable
from typing import List
from typing import Mapping
from typing import Union

from opensearchpy import OpenSearch
from pds.registrysweepers.provenance.constants import METADATA_SUCCESSOR_KEY
from pds.registrysweepers.provenance.provenancerecord import ProvenanceRecord
from pds.registrysweepers.provenance.versioning import SWEEPERS_BROKEN_PROVENANCE_VERSION_METADATA_KEY
from pds.registrysweepers.provenance.versioning import SWEEPERS_PROVENANCE_VERSION
from pds.registrysweepers.provenance.versioning import SWEEPERS_PROVENANCE_VERSION_METADATA_KEY
from pds.registrysweepers.utils import configure_logging
from pds.registrysweepers.utils import parse_args
from pds.registrysweepers.utils.db import query_registry_db_with_search_after
from pds.registrysweepers.utils.db import write_updated_docs
from pds.registrysweepers.utils.db.client import get_userpass_opensearch_client
from pds.registrysweepers.utils.db.indexing import ensure_index_mapping
from pds.registrysweepers.utils.db.multitenancy import resolve_multitenant_index_name
from pds.registrysweepers.utils.db.update import Update
from pds.registrysweepers.utils.misc import chunked
from pds.registrysweepers.utils.misc import get_ids_list_str
from pds.registrysweepers.utils.misc import group_by_key
from pds.registrysweepers.utils.misc import limit_log_length
from pds.registrysweepers.utils.productidentifiers.pdslid import PdsLid
from tqdm import tqdm

log = logging.getLogger(__name__)


def get_records_for_lids(client: OpenSearch, lids: Collection[PdsLid]) -> Iterable[ProvenanceRecord]:
    ids_str = get_ids_list_str(lids, 3)  # type: ignore
    log.info(limit_log_length(f"Fetching docs and generating records for {len(lids)} LIDs: {ids_str}"))

    query = {
        "query": {
            "bool": {
                "must": [
                    {"terms": {"ops:Tracking_Meta/ops:archive_status": ["archived", "certified"]}},
                    {"terms": {"lid": lids}},
                ]
            }
        }
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
                limit_log_length(
                    f'Failed to parse ProvenanceRecord from doc with id {doc["_id"]} due to {err} - source was {doc["_source"]}'
                )
            )


def fetch_target_lids(client: OpenSearch) -> Iterable[PdsLid]:
    # This page size determines how many LIDs are fetched at a time.  This value should be set high enough that the
    # updates produced from a single page are safely sufficient to trigger a buffer flush in
    # pds.registrysweepers.utils.db.write_updated_docs()
    #
    # If it is not, this will not impede correct operation, but will result in the sweeper terminating early and
    # requiring many runs to fully complete instead of completing with a single run.
    #
    # It must also allow for sufficient back-pressure to build such that it does not re-query before any updates have an
    # opportunity to start indexing (i.e. affecting the query results)
    agg_page_size = 25000

    def fetch_lids_chunk():
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"terms": {"ops:Tracking_Meta/ops:archive_status": ["archived", "certified"]}},
                        {
                            "bool": {
                                "should": [
                                    {
                                        "bool": {
                                            "must_not": {"exists": {"field": SWEEPERS_PROVENANCE_VERSION_METADATA_KEY}}
                                        }
                                    },
                                    {
                                        "range": {
                                            SWEEPERS_PROVENANCE_VERSION_METADATA_KEY: {
                                                "lt": SWEEPERS_PROVENANCE_VERSION
                                            }
                                        }
                                    },
                                ],
                                "minimum_should_match": 1,
                            }
                        },
                    ]
                }
            },
            "aggs": {"unique_lids": {"terms": {"field": "lid", "size": agg_page_size}}},
            "size": 0,
            "track_total_hits": True,
        }

        return client.search(
            index=resolve_multitenant_index_name(client, "registry"),
            body=query,
            size=0,
            _source_includes=[],
            track_total_hits=True,
            request_timeout=20,
        )

    # LIDs from the previous chunk are stored to avoid duplication in the event that  indexing lag causes LIDs to
    # persist in results
    previous_chunk_lids: Set[str] = set()
    consecutive_chunk_repetition_stall_threshold = 3
    consecutive_chunk_repetitions = 0

    response = fetch_lids_chunk()

    lids = {bucket["key"] for bucket in response["aggregations"]["unique_lids"]["buckets"]}

    with tqdm(desc="Provenance sweeper progress (approximate)", total=response["hits"]["total"]["value"]) as pbar:
        while len(lids) > 0:
            new_lids_count = 0
            for bucket in response["aggregations"]["unique_lids"]["buckets"]:
                lid = bucket["key"]
                doc_count = bucket["doc_count"]
                if lid not in previous_chunk_lids:
                    new_lids_count += 1
                    pbar.update(doc_count)
                    yield lid

            logging.info(f"Fetched {new_lids_count} new LIDs from registry (of {len(lids)} total LIDs)")

            is_last_page = len(lids) < agg_page_size
            if is_last_page:
                break

            # Handle consecutive result chunk repetitions
            if consecutive_chunk_repetitions > consecutive_chunk_repetition_stall_threshold:
                logging.error(
                    f"Fetched LIDs have not changed in {consecutive_chunk_repetition_stall_threshold + 1} consecutive "
                    f"attempts - OpenSearch indexing has stalled or aggregation page size {agg_page_size} is "
                    f"insufficient to trigger a write buffer flush - ending iteration early."
                )
                return
            elif lids == previous_chunk_lids:
                consecutive_chunk_repetitions += 1
                sleep_time_seconds = 5**consecutive_chunk_repetitions
                logging.warning(
                    f"Fetched chunk contains identical LIDs to previous chunk - sleeping {sleep_time_seconds} seconds"
                )
                sleep(sleep_time_seconds)
            else:
                consecutive_chunk_repetitions = 0

            previous_chunk_lids = set(lids)
            response = fetch_lids_chunk()
            lids = {bucket["key"] for bucket in response["aggregations"]["unique_lids"]["buckets"]}

    logging.info("No docs remain to process")


def generate_record_chains(
    client: OpenSearch, lids: Iterable[PdsLid], lid_batch_size=5000
) -> Iterable[List[ProvenanceRecord]]:
    """
    Create an iterable of unsorted collections of records which share LIDs.
    :param client:
    """
    for lid_batch in chunked(lids, lid_batch_size):
        unbucketed_records = get_records_for_lids(client, lid_batch)
        for record_chain in group_and_link_records_into_chains(unbucketed_records):
            yield record_chain


def group_and_link_records_into_chains(records: Iterable[ProvenanceRecord]) -> Iterable[List[ProvenanceRecord]]:
    """
    Given a collection of Provenance records, group them by LID and link the records within each group
    Broken out from generate_record_chains() to allow for test stubbing without a client.
    """

    record_chains_by_lid = group_by_key(records, lambda r: r.lidvid.lid)
    for record_chain in record_chains_by_lid.values():
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

    log.info(limit_log_length(f"Starting provenance v{SWEEPERS_PROVENANCE_VERSION} sweeper processing..."))

    ensure_index_mapping(
        client,
        resolve_multitenant_index_name(client, "registry"),
        SWEEPERS_PROVENANCE_VERSION_METADATA_KEY,
        "integer",
    )

    target_lids = fetch_target_lids(client)
    record_chains = generate_record_chains(client, target_lids)
    updates = generate_updates(itertools.chain.from_iterable(record_chains))

    write_updated_docs(
        client, updates, index_name=resolve_multitenant_index_name(client, "registry"), bulk_chunk_max_update_count=5000
    )

    log.info(limit_log_length("Completed provenance sweeper processing!"))


def generate_updates(records: Iterable[ProvenanceRecord]) -> Iterable[Update]:
    update_count = 0
    skippable_count = 0
    for record in records:
        update_content = {
            METADATA_SUCCESSOR_KEY: str(record.successor) if record.successor else None,
            SWEEPERS_PROVENANCE_VERSION_METADATA_KEY: SWEEPERS_PROVENANCE_VERSION,
            SWEEPERS_BROKEN_PROVENANCE_VERSION_METADATA_KEY: None,  # see comment in versioning.py for context - edunn
        }

        if record.skip_write:
            skippable_count += 1

        update_count += 1

        yield Update(id=str(record.lidvid), content=update_content, skip_write=record.skip_write)

    log.info(
        limit_log_length(
            f"Generated provenance updates for {update_count} products, ({skippable_count} up-to-date products will be skipped)"
        )
    )


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
