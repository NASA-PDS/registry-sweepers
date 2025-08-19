import os
from abc import ABC

from pds.registrysweepers.utils.misc import parse_boolean_env_var


class AncestryRuntimeConstants(ABC):
    # how many registry-refs documents (each collection has multiple docs for batches of member non-aggregates)
    # Decrease to reduce peak memory demand - increases runtime
    nonaggregate_ancestry_records_query_page_size: int = int(
        os.environ.get("ANCESTRY_NONAGGREGATE_QUERY_PAGE_SIZE", 500)
    )

    # non-aggregate history batches will be dumped to disk periodically as memory usage reaches this threshold
    max_acceptable_memory_usage: int = int(os.environ.get("ANCESTRY_DISK_DUMP_MEMORY_THRESHOLD", 80))

    # Expects a value like "true" or "1"
    disable_chunking: bool = parse_boolean_env_var("ANCESTRY_DISABLE_CHUNKING")

    # Not yet implemented
    # db_write_timeout_seconds = int(os.environ.get('DB_WRITE_TIMEOUT_SECONDS'), 90)
