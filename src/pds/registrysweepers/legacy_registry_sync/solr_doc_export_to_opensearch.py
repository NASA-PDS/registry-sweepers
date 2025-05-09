import logging
import os
from datetime import datetime

log = logging.getLogger(__name__)

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


def get_node_from_file_ref(file_ref: str):
    """
    Thanks to the file system storage of the labels in the legacy registry we can retrieve the
    Discipline Node in charge of each label.

    @param file_ref: location of the XML PDS4 Label in the legacy registry
    @return: the Discipline Node code used in the (new) registry.
    """
    file_path = os.fspath(file_ref)
    path = os.path.normpath(file_path)
    dirs = path.split(os.sep)
    return NODE_FOLDERS.get(dirs[4], "PDS_EN")


class SolrOsWrapperIter:
    def __init__(self, solr_itr, es_index, found_ids=None):
        """
        Iterable on the Solr legacy registry documents returning the migrated document for each iteration (next).
        The migrated documents contains in addition to the Solr document properties:
        - one identifier matching the one used in the new registry
        - the Discipline Node responsible for the product
        - a flag set to True if the current document was loaded in the new registry.

        @param solr_itr: iterator on the solr documents. SlowSolrDocs instance from the solr-to-es repository
        @param es_index: OpenSearch/ElasticSearch index name
        @param found_ids: list of the lidvid already available in the new registry
        """
        self.index = es_index
        self.type = "_doc"
        self.id_field_fun = pds4_id_field_fun
        self.found_ids = found_ids
        self.solr_itr = iter(solr_itr)

    def __iter__(self):
        return self

    def solr_doc_to_os_doc(self, doc):
        new_doc = dict()
        new_doc["_index"] = self.index
        new_doc["_type"] = self.type

        # remove empty fields
        new_doc["_source"] = {}
        for k, v in doc.items():
            # get the node from the data string
            # for example : /data/pds4/releases/ppi/galileo-traj-jup-20230818
            if k == "file_ref_location":
                new_doc["_source"]["node"] = get_node_from_file_ref(v[0])

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
                    log.warning("Date %s for field %s is invalid, assign default datetime 01-01-1950 instead", v, k)
                    new_doc["_source"][k] = [datetime(1950, 1, 1, 0, 0, 0)]
            elif "year" in k:
                if len(v[0]) > 0:
                    new_doc["_source"][k] = v
                else:
                    log.warning("Year %s for field %s is invalid", v, k)
            else:
                new_doc["_source"][k] = v

        # add modification date because kibana needs it for its time field
        if "modification_date" not in new_doc["_source"]:
            new_doc["_source"]["modification_date"] = [DEFAULT_MODIFICATION_DATE]

        if self.id_field_fun:
            id = self.id_field_fun(doc)
            new_doc["_id"] = id
            new_doc["_source"]["found_in_registry"] = "true" if id in self.found_ids else "false"

        return new_doc

    def __next__(self):
        while True:
            try:
                doc = next(self.solr_itr)
                return self.solr_doc_to_os_doc(doc)
            except MissingIdentifierError as e:
                log.warning(str(e))
