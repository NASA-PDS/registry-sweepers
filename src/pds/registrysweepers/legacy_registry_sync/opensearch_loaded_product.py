from typing import Dict
from typing import List
from typing import Optional

import opensearchpy  # type: ignore
from opensearchpy import OpenSearch
from pds.registrysweepers.utils.db import query_registry_db_with_search_after

# Optional Environment variable  used for the Cross Cluster Search
# connections aliases. Each element is separated by a ","
CCS_CONN = "CCS_CONN"


def get_already_loaded_lidvids(
    product_classes: Optional[List[str]] = None, es_conn: Optional[OpenSearch] = None
) -> Dict[str, Optional[str]]:
    """
    Get the lidvids of the PDS4 products already loaded in the (new) registry.
    Note that this function should not be applied to the product classes Product_Observational or documents, there would be too many results.

    @param product_classes: list of the product classes you are interested in,
    e.g. "Product_Bundle", "Product_Collection" ...
    @param es_conn: elasticsearch.ElasticSearch instance for the ElasticSearch or OpenSearch connection
    @return: dict mapping already-loaded PDS4 lidvid to ops:Harvest_Info/ops:node_name (or None if absent)
    """

    query = {"query": {"bool": {"should": [], "minimum_should_match": 1}}, "fields": ["_id"]}

    prod_class_prop = "pds:Identification_Area/pds:product_class"
    node_name_field = "ops:Harvest_Info/ops:node_name"

    if product_classes is not None:
        query["query"]["bool"]["should"] = [
            dict(match_phrase={prod_class_prop: prod_class}) for prod_class in product_classes
        ]

    sort_field = "ops:Harvest_Info/ops:harvest_date_time"
    prod_id_resp = query_registry_db_with_search_after(
        es_conn,
        "registry",
        _source={"includes": ["_id", sort_field, node_name_field], "excludes": []},
        query=query,
        sort_fields=[sort_field],
    )

    return {p["_id"]: p.get("_source", {}).get(node_name_field) for p in prod_id_resp}
