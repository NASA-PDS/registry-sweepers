import logging
import os
from datetime import datetime
from urllib.parse import urlparse

from pds.registrysweepers.utils.misc import limit_log_length

log = logging.getLogger(__name__)

UNKNOWN_NODE = "UNK"

NODE_FOLDERS = {
    "atmos": "PDS_ATM",
    "en": "PDS_ENG",
    "geo": "PDS_GEO",
    "img": "PDS_IMG",
    "naif": "PDS_NAIF",
    "ppi": "PDS_PPI",
    "rings": "PDS_RMS",
    "rs": "PDS_RS",
    "sbn": "PDS_SBN",
}

ENG_PRODUCT_CLASSES = {
    "Product_Context",
    "Product_XML_Schema",
    "Product_Instrument_PDS3",
    "Product_Mission_PDS3",
    "Product_Instrument_Host_PDS3",
    "Product_Target_PDS3",
}
UNK_PRODUCT_CLASSES = {"Product_SIP_Deep_Archive", "Product_Zipped", "Product_Ancillary"}

NODE_DOMAINS = {
    # JAXA
    "www.darts.isas.jaxa.jp": "JAXA",
    "darts.isas.jaxa.jp": "JAXA",
    # KPDS
    "www.kari.re.kr": "KPDS",
    # PSA
    "psa.esac.esa.int": "PSA",
    # ATM
    "atmos.nmsu.edu": "PDS_ATM",
    "pds-atmospheres.nmsu.edu": "PDS_ATM",
    "mars.nasa.gov": "PDS_IMG",
    "planetarydata.jpl.nasa.gov": "PDS_IMG",
    # ENG
    "starbrite.jpl.nasa.gov": "PDS_ENG",
    "starbase.jpl.nasa.gov": "PDS_ENG",
    "pds.nasa.gov": "PDS_ENG",
    # GEO
    "ode.rsl.wustl.edu": "PDS_GEO",
    "pds-speclib.rsl.wustl.edu": "PDS_GEO",
    "an.rsl.wustl.edu": "PDS_GEO",
    "pds-geosciences.wustl.edu": "PDS_GEO",
    # IMG
    "astrogeology.usgs.gov": "PDS_IMG",
    "mars.jpl.nasa.gov": "PDS_IMG",
    "d2g5bbjkxk8tlv.cloudfront.net": "PDS_IMG",
    "pdsimage2.wr.usgs.gov": "PDS_IMG",
    "static.mars.asu.edu": "PDS_IMG",
    "pds.shadowcam.im-ldi.com": "PDS_IMG",
    "pds.lroc.asu.edu": "PDS_IMG",
    "grspds.lpl.arizona.edu": "PDS_IMG",
    "pds-imaging.jpl.nasa.gov": "PDS_IMG",
    "pdsmaps.wr.usgs.gov": "PDS_IMG",
    "pdsimg.jpl.nasa.gov": "PDS_IMG",
    "pdsimage.wr.usgs.gov": "PDS_IMG",
    # NAIF
    "wgc.jpl.nasa.gov:8443": "PDS_NAIF",
    "naif.jpl.nasa.gov": "PDS_NAIF",
    # PPI
    "pds-ppi.igpp.ucla.edu": "PDS_PPI",
    "ppi.pds.nasa.gov": "PDS_PPI",
    "pgs-ppi.igpp.ucla.edu": "PDS_PPI",
    "www-pw.physics.uiowa.edu": "PDS_PPI",
    # RMS
    "pds-rings.seti.org": "PDS_RMS",
    # SBN
    "pds-smallbodies.astro.umd.edu": "PDS_SBN",
    "arcnav.psi.edu": "PDS_SBN",
    "sbn.psi.edu": "PDS_SBN",
    "pdssbn.astro.umd.edu": "PDS_SBN",
    "sbnarchive.psi.edu": "PDS_SBN",
    "www.psi.edu": "PDS_SBN",
}


NODE_ID = {
    "Engineering": "PDS_ENG",
    "Planetary Rings": "PDS_RMS",
    "Imaging": "PDS_IMG",
    "Planetary Atmospheres": "PDS_ATM",
    "Navigation and Ancillary Information Facility": "PDS_NAIF",
    "Planetary Plasma Interactions": "PDS_PPI",
    "Small Bodies": "PDS_SBN",
    "Geosciences": "PDS_GEO",
}

DEFAULT_MODIFICATION_DATE = datetime(1950, 1, 1, 0, 0, 0)


class MissingIdentifierError(Exception):
    pass


def pds4_id_field_fun(doc):
    """
    Compute the unique identifier in the new registry from a document in the legacy registry

    @param doc: document from the legacy registry
    @return: lidvid
    """
    if "lidvid" in doc:
        return doc["lidvid"]
    else:
        raise MissingIdentifierError()


def get_online_resource_id(resource_ref: str) -> str:
    """
    Extract the LID (Logical Identifier) from a resource reference that might be a LIDVID.

    The resource_ref can be either:
    - A LID (Logical Identifier): urn:nasa:pds:mission_name:data_type::1.0
    - A LIDVID (Logical Identifier + Version): urn:nasa:pds:mission_name:data_type::1.0

    This function extracts just the LID part by removing the version component.

    @param resource_ref: The resource reference from the document
    @return: The LID part of the resource reference
    """
    # If it contains "::", it's likely a LIDVID, so we take the LID part (before "::")
    if "::" in resource_ref:
        # Split on "::" and take the first part (LID)
        parts = resource_ref.split("::")
        return parts[0]

    # If no "::" found, return as-is (it's already a LID)
    return resource_ref


def get_node_from_file_ref(file_ref: str):
    """
    Thanks to the file system storage of the labels in the legacy registry we can retrieve the
    Discipline Node in charge of each label.

    @param file_ref: location of the XML PDS4 Label in the legacy registry
    @return: the Discipline Node code used in the (new) registry.
    """
    dirs = file_ref.split("/")
    node_dir = dirs[6]
    return NODE_FOLDERS.get(node_dir, "PDS_ENG")


class SolrOsWrapperIter:
    def __init__(self, solr_itr, es_index, found_ids=None, online_resources=None):
        """
        Iterable on the Solr legacy registry documents returning the migrated document for each iteration (next).
        The migrated documents contains in addition to the Solr document properties:
        - one identifier matching the one used in the new registry
        - the Discipline Node responsible for the product
        - a flag set to True if the current document was loaded in the new registry.

        @param solr_itr: iterator on the solr documents. SlowSolrDocs instance from the solr-to-es repository
        @param es_index: OpenSearch/ElasticSearch index name
        @param found_ids: list of the lidvid already available in the new registry
        @param rolls_over_target: artificially increase the number entries by re-running the loop n times
        """
        self.index = es_index
        self.type = "update"
        self.id_field_fun = pds4_id_field_fun
        self.found_ids = found_ids
        self.online_resources = online_resources
        self._solr_itr = iter(solr_itr)
        self._seen_domains = set()
        self._seen_node_ids = set()

    def __iter__(self):
        return self

    def _get_node(self, doc: dict) -> str:
        """Infer the node from the url resource's DNS"""

        if "agency_name" in doc:
            agency = doc["agency_name"][0]
            if agency == "esa":
                return "PSA"
            elif agency == "Unknown":
                return UNKNOWN_NODE

        if "product_class" in doc:
            product_class = doc["product_class"][0]
            if product_class in ENG_PRODUCT_CLASSES:
                # we automatically assign specific product classes to ENG
                return "PDS_ENG"
            elif product_class in UNK_PRODUCT_CLASSES:  # we don't bother to attribute a node to other product classes
                return UNKNOWN_NODE

        if "resource_url" in doc:
            url = doc["resource_url"][0]
            domain = urlparse(url).netloc
            self._seen_domains.add(domain)
            if domain in NODE_DOMAINS:
                return NODE_DOMAINS[domain]

        if "resource_ref" in doc:
            online_resource_id = get_online_resource_id(doc["resource_ref"][0])
            if online_resource_id in self.online_resources:
                url = self.online_resources.get(online_resource_id)
                domain = urlparse(url).netloc
                self._seen_domains.add(domain)
                if domain in NODE_DOMAINS:
                    return NODE_DOMAINS[domain]
            else:
                log.warning("Skipping not found online resource '%s' of doc %s", online_resource_id, doc["lid"])

        if "node_id" in doc:
            node_id = doc["node_id"][0]
            self._seen_node_ids.add(node_id)
            if node_id in NODE_ID:
                return NODE_ID[node_id]

        if "file_ref_url" in doc:
            url = doc["file_ref_url"][0]
            return get_node_from_file_ref(url)

        log.warning(
            "Unable to attribute node for product %s, none of resource_url, resource_ref, file_ref_url, node_id were found",
            doc["lid"],
        )

        return UNKNOWN_NODE

    def solr_doc_to_os_doc(self, doc):
        new_doc = dict()
        new_doc["_index"] = self.index
        new_doc["_type"] = self.type
        new_doc["doc_as_upsert"] = True

        # remove empty fields
        new_doc["_source"] = {}
        for k, v in doc.items():
            # manage dates
            if "date" in k:
                # only keep the latest modification date, for kibana
                if k == "modification_date":
                    v = [v[-1]]

                # validate dates
                try:
                    v = [datetime.fromisoformat(v[0].replace("Z", ""))]
                    new_doc["_source"][k] = v
                except ValueError:
                    log.warning(
                        limit_log_length(
                            f"Date {v} for field {k} is invalid, assign default datetime 01-01-1950 instead"
                        )
                    )
                    new_doc["_source"][k] = [datetime(1950, 1, 1, 0, 0, 0)]
            elif "year" in k:
                if len(v[0]) > 0:
                    new_doc["_source"][k] = v
                else:
                    log.warning(limit_log_length(f"Year {v} for field {k} is invalid"))
            else:
                new_doc["_source"][k] = v

        # add modification date because kibana needs it for its time field
        if "modification_date" not in new_doc["_source"]:
            new_doc["_source"]["modification_date"] = [DEFAULT_MODIFICATION_DATE]

        if self.id_field_fun:
            id = self.id_field_fun(doc)
            new_doc["_id"] = id
            new_doc["_source"]["found_in_registry"] = "true" if id in self.found_ids else "false"

        new_doc["_source"]["node"] = self._get_node(doc)
        return new_doc

    def __next__(self):
        while True:
            # skip rows without an id
            try:
                doc = next(self._solr_itr)
                return self.solr_doc_to_os_doc(doc)
            except MissingIdentifierError as e:
                log.warning(limit_log_length(str(e)))
