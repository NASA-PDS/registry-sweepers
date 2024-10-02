import json
import logging
import os
import tempfile
from itertools import chain
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Set
from typing import TextIO
from typing import Tuple
from typing import Union

from opensearchpy import OpenSearch
from pds.registrysweepers.reindexer.constants import REINDEXER_FLAG_METADATA_KEY
from pds.registrysweepers.utils import configure_logging
from pds.registrysweepers.utils import parse_args
from pds.registrysweepers.utils.db import write_updated_docs
from pds.registrysweepers.utils.db.client import get_userpass_opensearch_client
from pds.registrysweepers.utils.db.indexing import ensure_index_mapping
from pds.registrysweepers.utils.db.multitenancy import resolve_multitenant_index_name
from pds.registrysweepers.utils.db.update import Update
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid


def run(
    client: OpenSearch,
    log_filepath: Union[str, None] = None,
    log_level: int = logging.INFO,
):
    ensure_index_mapping(client, resolve_multitenant_index_name("registry"), REINDEXER_FLAG_METADATA_KEY, "date")

    print("complete")
    pass


if __name__ == "__main__":
    cli_description = f"""
    Tests untested documents in registry index to ensure that all properties are present in the index mapping (i.e. that
    they are searchable).  Mapping types are derived from <<<to be determined>>>

    When a document is tested, metadata attribute {REINDEXER_FLAG_METADATA_KEY} is given a value equal to the timestamp
    at sweeper runtime. The presence of attribute {REINDEXER_FLAG_METADATA_KEY} indicates that the document has been
    tested and may be skipped in future.

    Writing a new value to this attribute triggers a re-index of the entire document, ensuring that the document is
    fully-searchable.

    """

    args = parse_args(description=cli_description)
    client = get_userpass_opensearch_client(
        endpoint_url=args.base_URL, username=args.username, password=args.password, verify_certs=not args.insecure
    )

    run(
        client=client,
        log_level=args.log_level,
        log_filepath=args.log_file,
    )
