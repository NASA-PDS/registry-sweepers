"""repairkit is an executable package

The reason repairkit is an executable package is for extension as new repairs
are needed in the future. They can be added by updating the REPAIR_TOOLS mapping
with the new field name and functional requirements. All the additions can then
be modules with this executable package.
"""
import collections
import logging
import re
from typing import Dict
from typing import Iterable
from typing import Union

from opensearchpy import OpenSearch
from pds.registrysweepers.repairkit import allarrays
from pds.registrysweepers.repairkit.versioning import SWEEPERS_REPAIRKIT_VERSION
from pds.registrysweepers.repairkit.versioning import SWEEPERS_REPAIRKIT_VERSION_METADATA_KEY
from pds.registrysweepers.utils import configure_logging
from pds.registrysweepers.utils import parse_args
from pds.registrysweepers.utils.db import get_query_hits_count
from pds.registrysweepers.utils.db import query_registry_db_with_search_after
from pds.registrysweepers.utils.db import write_updated_docs
from pds.registrysweepers.utils.db.client import get_userpass_opensearch_client
from pds.registrysweepers.utils.db.indexing import ensure_index_mapping
from pds.registrysweepers.utils.db.multitenancy import resolve_multitenant_index_name
from pds.registrysweepers.utils.db.update import Update
from pds.registrysweepers.utils.misc import limit_log_length

"""
dictionary repair tools is {field_name:[funcs]} where field_name can be:
  1: re.compile().fullmatch for the equivalent of "fred" == "fred"
  2: re.compile().match for more complex matching of subparts of the string

and funcs are:
def function_name (document:{}, fieldname:str)->{}

and the return an empty {} if no changes and {fieldname:new_value} for repairs

Examples

re.compile("^ops:Info/.+").match("ops:Info/ops:filesize")->match object
re.compile("^ops:Info/.+").fullmatch("ops:Info/ops:filesize")->match object
re.compile("^ops:Info/").match("ops:Info/ops:filesize")->match object
re.compile("^ops:Info/").fullmatch("ops:Info/ops:filesize")->None

To get str_a == str_b, re.compile(str_a).fullmatch

"""

REPAIR_TOOLS = {
    re.compile(".").match: [allarrays.repair],
}

log = logging.getLogger(__name__)


def generate_updates(
    docs: Iterable[Dict], repairkit_version_metadata_key: str, repairkit_version: int
) -> Iterable[Update]:
    """Lazily generate necessary Update objects for a collection of db documents"""
    repair_already_logged_to_error = False

    for document in docs:
        id = document["_id"]
        src = document["_source"]
        repairs = {repairkit_version_metadata_key: int(repairkit_version)}
        for fieldname, data in src.items():
            for regex, funcs in REPAIR_TOOLS.items():
                if regex(fieldname):
                    for func in funcs:
                        repairs.update(func(src, fieldname))

        document_needed_fixing = len(set(repairs).difference({repairkit_version_metadata_key})) > 0
        if document_needed_fixing and not repair_already_logged_to_error:
            log.error(
                limit_log_length(
                    "repairkit sweeper detects documents in need of repair - please ~harass~ *request* node user to update their harvest version"
                )
            )
            repair_already_logged_to_error = True
        yield Update(id=id, content=repairs)


def run(
    client: OpenSearch,
    log_filepath: Union[str, None] = None,
    log_level: int = logging.INFO,
):
    configure_logging(filepath=log_filepath, log_level=log_level)
    log.info(limit_log_length(f"Starting repairkit v{SWEEPERS_REPAIRKIT_VERSION} sweeper processing..."))

    def get_unprocessed_docs_query():
        return {
            "query": {
                "bool": {
                    "must_not": [
                        {"range": {SWEEPERS_REPAIRKIT_VERSION_METADATA_KEY: {"gte": SWEEPERS_REPAIRKIT_VERSION}}}
                    ]
                }
            }
        }

    # page_size and bulk_chunk_max_update_count constraints are necessary to avoid choking nodes with very-large docs
    # i.e. ATM and GEO
    index_name = resolve_multitenant_index_name(client, "registry")
    update_max_chunk_size = 20000
    while get_query_hits_count(client, index_name, get_unprocessed_docs_query()) > 0:
        all_docs = query_registry_db_with_search_after(
            client,
            index_name,
            get_unprocessed_docs_query(),
            {},
            page_size=500,
            limit=update_max_chunk_size,
            request_timeout_seconds=180,
        )
        updates = generate_updates(all_docs, SWEEPERS_REPAIRKIT_VERSION_METADATA_KEY, SWEEPERS_REPAIRKIT_VERSION)
        ensure_index_mapping(
            client,
            resolve_multitenant_index_name(client, "registry"),
            SWEEPERS_REPAIRKIT_VERSION_METADATA_KEY,
            "integer",
        )
        write_updated_docs(
            client,
            updates,
            index_name=resolve_multitenant_index_name(client, "registry"),
            bulk_chunk_max_update_count=update_max_chunk_size,
        )

    log.info(limit_log_length("Repairkit sweeper processing complete!"))


if __name__ == "__main__":
    args = parse_args(description="sweep through the registry documents and fix common errors")
    client = get_userpass_opensearch_client(
        endpoint_url=args.base_URL, username=args.username, password=args.password, verify_certs=not args.insecure
    )

    run(
        client=client,
        log_level=args.log_level,
        log_filepath=args.log_file,
    )
