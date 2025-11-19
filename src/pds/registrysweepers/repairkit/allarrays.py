"""change single strings to array of strings"""
import json
import logging
from typing import Dict

from pds.registrysweepers.utils.misc import limit_log_length

log = logging.getLogger(__name__)

# exclude the following properties from array conversion even if targeted - they are expected to be string-typed
EXCLUDED_PROPERTIES = {
    "lid",
    "vid",
    "lidvid",
    "title",
    "product_class",
    "_package_id",
}


def repair(document: Dict, fieldname: str) -> Dict:
    # don't touch the enumerated exclusions, or any registry-sweepers metadata property
    if fieldname in EXCLUDED_PROPERTIES or fieldname.startswith("ops:Provenance"):
        return {}

    if isinstance(document[fieldname], str):
        log.debug(
            limit_log_length(
                f"found string in doc {document.get('_id')} for field {fieldname} where it should be an array"
            )
        )
        return {fieldname: [document[fieldname]]}
    return {}
