import os

import opensearchpy  # type: ignore

# Optional Environment variable  used for the Cross Cluster Search
# connections aliases. Each element is separated by a ","
CCS_CONN = "CCS_CONN"


def get_already_loaded_lidvids(product_classes=None, es_conn=None):
    """
    Get the lidvids of the PDS4 products already loaded in the (new) registry.
    Note that this function should not be applied to the product classes Product_Observational or documents, there would be too many results.

    @param product_classes: list of the product classes you are interested in,
    e.g. "Product_Bundle", "Product_Collection" ...
    @param es_conn: elasticsearch.ElasticSearch instance for the ElasticSearch or OpenSearch connection
    @return: the list of the already loaded PDS4 lidvid
    """

    query = {"query": {"bool": {"should": [], "minimum_should_match": 1}}, "fields": ["_id"]}

    prod_class_prop = "pds:Identification_Area/pds:product_class"

    if product_classes is not None:
        query["query"]["bool"]["should"] = [
            dict(match_phrase={prod_class_prop: prod_class}) for prod_class in product_classes
        ]

    prod_id_resp = opensearchpy.helpers.scan(es_conn, index=["registry"], query=query, scroll="3m")
    return [p["_id"] for p in prod_id_resp]
