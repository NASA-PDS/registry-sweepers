"""repairkit is an executable package

The reason repairkit is an executable package is for extension as new repairs
are needed in the future. They can be added by updating the REPAIR_TOOLS mapping
with the new field name and functional requirements. All the additions can then
be modules with this executable package.
"""
import logging
import re
from typing import Dict
from typing import Iterable
from typing import Union

from opensearchpy import OpenSearch
from pds.registrysweepers.repairkit.versioning import SWEEPERS_REPAIRKIT_VERSION
from pds.registrysweepers.utils import configure_logging
from pds.registrysweepers.utils import parse_args
from pds.registrysweepers.utils import query_registry_db
from pds.registrysweepers.utils.db.client import get_opensearch_client
from pds.registrysweepers.utils.db.indexing import ensure_index_mapping
from pds.registrysweepers.utils.db.update import Update
from pds.registrysweepers.utils.misc import get_sweeper_version_metadata_key

from . import allarrays
from ..utils.db import write_updated_docs

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
    re.compile("^ops:Data_File_Info/").match: [allarrays.repair],
    re.compile("^ops:Label_File_Info/").match: [allarrays.repair],
}

log = logging.getLogger(__name__)


def generate_updates(
    docs: Iterable[Dict], repairkit_version_metadata_key: str, repairkit_version: int
) -> Iterable[Update]:
    """Lazily generate necessary Update objects for a collection of db documents"""
    for document in docs:
        id = document["_id"]
        src = document["_source"]
        repairs = {repairkit_version_metadata_key: int(repairkit_version)}
        log.debug(f"applying repairkit sweeper to document: {id}")
        for fieldname, data in src.items():
            for regex, funcs in REPAIR_TOOLS.items():
                if regex(fieldname):
                    for func in funcs:
                        repairs.update(func(src, fieldname))

        if repairs:
            log.debug(f"Writing repairs to document: {id}")
            yield Update(id=id, content=repairs)


def run(
    client: OpenSearch,
    log_filepath: Union[str, None] = None,
    log_level: int = logging.INFO,
):
    configure_logging(filepath=log_filepath, log_level=log_level)
    log.info(f"Starting repairkit v{SWEEPERS_REPAIRKIT_VERSION} sweeper processing...")

    repairkit_version_metadata_key = get_sweeper_version_metadata_key("repairkit")

    unprocessed_docs_query = {
        "query": {
            "bool": {"must_not": [{"range": {repairkit_version_metadata_key: {"gte": SWEEPERS_REPAIRKIT_VERSION}}}]}
        }
    }

    all_docs = query_registry_db(client, unprocessed_docs_query, {})
    updates = generate_updates(all_docs, repairkit_version_metadata_key, SWEEPERS_REPAIRKIT_VERSION)
    ensure_index_mapping(client, "registry", repairkit_version_metadata_key, "integer")
    write_updated_docs(client, updates)

    log.info("Repairkit sweeper processing complete!")


if __name__ == "__main__":
    args = parse_args(description="sweep through the registry documents and fix common errors")
    client = get_opensearch_client(
        endpoint_url=args.base_URL, username=args.username, password=args.password, verify_certs=not args.insecure
    )

    run(
        client=client,
        log_level=args.log_level,
        log_filepath=args.log_file,
    )
