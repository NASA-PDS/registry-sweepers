import json
import logging
import sys
from time import sleep
from typing import Union

import opensearchpy.helpers
from opensearchpy import OpenSearch
from pds.registrysweepers.legacy_registry_sync.opensearch_loaded_product import get_already_loaded_lidvids
from pds.registrysweepers.legacy_registry_sync.solr_doc_export_to_opensearch import SolrOsWrapperIter
from pds.registrysweepers.utils import configure_logging
from pds.registrysweepers.utils.misc import is_dev_mode
from solr_to_es.solrSource import SlowSolrDocs  # type: ignore

log = logging.getLogger(__name__)

SOLR_URL = "https://pds.nasa.gov/services/search/search"
OS_INDEX = "en-legacy-registry"
MAX_RETRIES = 5


def create_legacy_registry_index(es_conn=None):
    """
    Creates if not already created the legacy_registry index.

    @param es_conn: elasticsearch.ElasticSearch instance for the ElasticSearch or OpenSearch connection
    @return:
    """
    if not es_conn.indices.exists(OS_INDEX):
        log.info("create index %s", OS_INDEX)
        es_conn.indices.create(index=OS_INDEX, body={})
    log.info("index created %s", OS_INDEX)


def run(
    client: OpenSearch,
    log_filepath: Union[str, None] = None,
    log_level: int = logging.INFO,
):
    """
    Runs the Solr Legacy Registry synchronization with OpenSearch.

    @param client: OpenSearch client from the opensearchpy library
    @param log_filepath:
    @param log_level:
    @return:
    """

    configure_logging(filepath=log_filepath, log_level=log_level)

    solr_itr = SlowSolrDocs(SOLR_URL, "*", rows=500)

    create_legacy_registry_index(es_conn=client)

    prod_ids = get_already_loaded_lidvids(
        product_classes=["Product_Context", "Product_Collection", "Product_Bundle"], es_conn=client
    )

    es_actions = SolrOsWrapperIter(solr_itr, OS_INDEX, found_ids=prod_ids)
    dev_mode = is_dev_mode()
    for operation_successful, operation_info in opensearchpy.helpers.streaming_bulk(
        client, es_actions, chunk_size=50, max_chunk_bytes=50000000, max_retries=5, initial_backoff=10
    ):
        if not operation_successful:
            log.error(operation_info)

        if dev_mode:
            break
