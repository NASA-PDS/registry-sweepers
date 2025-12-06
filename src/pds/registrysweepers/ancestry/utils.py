import gc
import json
import logging
import os
import pickle
import sys
from datetime import datetime
from typing import Dict
from typing import Iterable
from typing import List
from typing import Set
from typing import Union

from pds.registrysweepers.ancestry.ancestryrecord import AncestryRecord
from pds.registrysweepers.ancestry.constants import ANCESTRY_DEDUPLICATION_SCRIPT_MINIFIED
from pds.registrysweepers.ancestry.constants import ANCESTRY_REFS_METADATA_KEY
from pds.registrysweepers.ancestry.productupdaterecord import ProductUpdateRecord
from pds.registrysweepers.ancestry.typedefs import SerializableAncestryRecordTypeDef
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY
from pds.registrysweepers.utils.db import Update
from pds.registrysweepers.utils.misc import limit_log_length

log = logging.getLogger(__name__)


def make_history_serializable(history: Dict[str, Dict[str, Union[str, Set[str], List[str]]]]):
    """Convert history with set attributes into something able to be dumped to JSON"""
    log.debug(limit_log_length("Converting history into serializable types..."))
    for lidvid in history.keys():
        history[lidvid]["parent_bundle_lidvids"] = list(history[lidvid]["parent_bundle_lidvids"])
        history[lidvid]["parent_collection_lidvids"] = list(history[lidvid]["parent_collection_lidvids"])
    log.debug(limit_log_length("    complete!"))


def load_history_from_filepath(filepath) -> Dict[str, SerializableAncestryRecordTypeDef]:
    with open(filepath, "rb") as f:
        return pickle.load(f)


def write_history_to_filepath(history: Dict[str, SerializableAncestryRecordTypeDef], filepath: str):
    with open(filepath, "w+b") as outfile:
        pickle.dump(history, outfile)


def dump_history_to_disk(parent_dir: str, history: Dict[str, SerializableAncestryRecordTypeDef]) -> str:
    """Dump set of history records to disk and return the filepath"""
    output_filepath = os.path.join(parent_dir, datetime.now().isoformat().replace(":", "-"))
    log.debug(limit_log_length(f"Dumping history to {output_filepath} for later merging..."))
    write_history_to_filepath(history, output_filepath)
    log.debug(limit_log_length("    complete!"))

    return output_filepath


def split_content_chunk_if_oversized(
    max_chunk_size: Union[int, None], parent_dir: str, content: Dict
) -> Union[str, None]:
    """
    To keep memory usage near expected bounds, it's necessary to avoid accumulation into a merge destination chunk such
    that its size balloons beyond the size of a pre-merge chunk.  This is achieved by splitting the chunk approximately
    in half, if its size exceeds the given threshold, and returning the newly-created chunk's filepath for addition to
    the processing queue.
    """
    if max_chunk_size is None:
        return None

    if not sys.getsizeof(content) > max_chunk_size:
        return None

    split_content = {}
    collection_keys = list(content.keys())
    for k in collection_keys[::2]:  # pick every second key
        split_content[k] = content.pop(k)

    split_filepath = dump_history_to_disk(parent_dir, split_content)
    log.debug(limit_log_length(f"split off excess chunk content to new file: {split_filepath}"))
    return split_filepath


def load_partial_history_to_records(fn: str) -> Iterable[AncestryRecord]:
    with open(fn, "r") as infile:
        content: Dict[str, SerializableAncestryRecordTypeDef] = json.load(infile)

    for history_dict in content.values():
        yield AncestryRecord.from_dict(history_dict)


def gb_mem_to_size(desired_mem_usage_gb) -> int:
    # rough estimated ratio of memory size to sys.getsizeof() report
    return desired_mem_usage_gb / 3.1 * 2621536


def update_from_record(record: ProductUpdateRecord) -> Update:
    doc_id = str(record.product)
    content = {
        ANCESTRY_REFS_METADATA_KEY: [str(id) for id in record.direct_ancestor_refs],
        SWEEPERS_ANCESTRY_VERSION_METADATA_KEY: int(SWEEPERS_ANCESTRY_VERSION),
    }

    return Update(id=doc_id, content=content, inline_script_content=ANCESTRY_DEDUPLICATION_SCRIPT_MINIFIED)
