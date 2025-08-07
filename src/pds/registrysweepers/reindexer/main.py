import logging
import math
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from time import sleep
from typing import Collection
from typing import Dict
from typing import Iterable
from typing import Union

import dateutil.parser
from opensearchpy import OpenSearch
from pds.registrysweepers.reindexer.constants import REINDEXER_FLAG_METADATA_KEY
from pds.registrysweepers.utils import configure_logging
from pds.registrysweepers.utils import parse_args
from pds.registrysweepers.utils.db import get_query_hits_count
from pds.registrysweepers.utils.db import query_registry_db_with_search_after
from pds.registrysweepers.utils.db import write_updated_docs
from pds.registrysweepers.utils.db.client import get_userpass_opensearch_client
from pds.registrysweepers.utils.db.indexing import ensure_index_mapping
from pds.registrysweepers.utils.db.multitenancy import resolve_multitenant_index_name
from pds.registrysweepers.utils.db.update import Update
from pds.registrysweepers.utils.misc import limit_log_length
from tqdm import tqdm

log = logging.getLogger(__name__)


def get_docs_query(filter_to_harvested_before: datetime):
    """
    Return a query to get all docs which haven't been reindexed by this sweeper and which haven't been harvested
    since this sweeper process instance started running

    i.e.
    - Query all documents
    - Exclude anything which has already been processed, to avoid redundant reprocessing
    - Exclude anything which was harvested in the middle of this sweeper running, since this can cause erroneous results
      due to inconsistency in the document set across query calls which are expected to be identical.
    """
    # TODO: Remove this once query_registry_db_with_search_after is modified to remove mutation side-effects
    return {
        "query": {
            "bool": {
                "must_not": [{"exists": {"field": REINDEXER_FLAG_METADATA_KEY}}],
                "must": {
                    "range": {
                        "ops:Harvest_Info/ops:harvest_date_time": {
                            "lt": filter_to_harvested_before.astimezone(timezone.utc).isoformat()
                        }
                    }
                },
            }
        }
    }


def fetch_dd_field_types(client: OpenSearch) -> Dict[str, str]:
    dd_index_name = resolve_multitenant_index_name(client, "registry-dd")
    name_key = "es_field_name"
    type_key = "es_data_type"
    dd_docs = query_registry_db_with_search_after(
        client,
        dd_index_name,
        _source={"includes": ["es_field_name", "es_data_type"]},
        query={"query": {"match_all": {}}},
        sort_fields=[name_key],
    )
    doc_sources = iter(doc["_source"] for doc in dd_docs)
    dd_types = {
        source[name_key]: source[type_key] for source in doc_sources if name_key in source and type_key in source
    }
    return dd_types


def get_mapping_field_types_by_field_name(client: OpenSearch, index_name: str) -> Dict[str, str]:
    return {
        k: v["type"] for k, v in client.indices.get_mapping(index_name)[index_name]["mappings"]["properties"].items()  # type: ignore
    }


def accumulate_missing_mappings(
    dd_field_types_by_name: Dict[str, str], mapping_field_types_by_field_name: Dict[str, str], docs: Iterable[dict]
) -> Dict[str, str]:
    """
    Iterate over all properties of all docs, test whether they are present in the given set of mapping keys, and
    return a mapping of the missing properties onto their types.
    @param dd_field_types_by_name: a mapping of document property names onto their types, derived from the data-dictionary db data
    @param mapping_field_types_by_field_name: a mapping of document property names onto their types, derived from the existing index mappings
    @param docs: an iterable collection of product documents
    """

    # Static mappings for fields not defined in the data dictionaries
    # NoneType indicates that the property is to be excluded.
    # Anything with prefix 'ops:Provenance' is excluded, as these properties are the responsibility of their
    #  respective sweepers.
    special_case_property_types_by_name = {
        "@timestamp": None,
        "@version": None,
        "_package_id": None,
        "description": "text",
        "lid": "keyword",
        "lidvid": "keyword",
        "ops:Harvest_Info/ops:harvest_date_time": "date",
        "ops:Label_File_Info/ops:json_blob": None,
        "product_class": "keyword",
        "ref_lid_associate": "keyword",
        "ref_lid_collection": "keyword",
        "ref_lid_collection_secondary": "keyword",
        "ref_lid_data": "keyword",
        "ref_lid_document": "keyword",
        "ref_lid_facility": "keyword",
        "ref_lid_instrument": "keyword",
        "ref_lid_instrument_host": "keyword",
        "ref_lid_investigation": "keyword",
        "ref_lid_target": "keyword",
        "ref_lid_telescope": "keyword",
        "title": "text",
        # 'vid'  # TODO: need to determine what this should be, as keyword lexical(?) sorting will be a problem
    }

    missing_mapping_updates: Dict[str, str] = {}

    canonical_type_undefined_property_names = set()  # used to prevent duplicate WARN logs
    bad_mapping_property_names = set()  # used to log mappings requiring manual attention
    sweepers_missing_property_names = set()

    earliest_problem_doc_harvested_at = None
    latest_problem_doc_harvested_at = None
    problematic_harvest_versions = set()
    problem_docs_count = 0
    total_docs_count = 0
    for doc in docs:
        problem_detected_in_document_already = False
        total_docs_count += 1

        for property_name, value in doc["_source"].items():
            # Resolve canonical type from data dictionary or - failing that - from the hardcoded types
            canonical_type = dd_field_types_by_name.get(property_name) or special_case_property_types_by_name.get(
                property_name
            )
            current_mapping_type = mapping_field_types_by_field_name.get(property_name)

            mapping_missing = property_name not in mapping_field_types_by_field_name
            canonical_type_is_defined = canonical_type is not None
            mapping_is_bad = (
                canonical_type != current_mapping_type
                and canonical_type is not None
                and current_mapping_type is not None
            )

            if (
                not canonical_type_is_defined
                and property_name not in special_case_property_types_by_name
                and not property_name.startswith("ops:Provenance")
                and property_name not in canonical_type_undefined_property_names
            ):
                log.warning(
                    limit_log_length(
                        f"Property {property_name} does not have an entry in the data dictionary index or hardcoded mappings - this may indicate a problem"
                    )
                )
                canonical_type_undefined_property_names.add(property_name)

            if mapping_is_bad and property_name not in bad_mapping_property_names:
                log.warning(
                    limit_log_length(
                        f'Property {property_name} is defined in data dictionary index or hardcoded mappings as type "{canonical_type}" but exists in index mapping as type "{current_mapping_type}")'
                    )
                )
                bad_mapping_property_names.add(property_name)

            if (mapping_missing or mapping_is_bad) and not problem_detected_in_document_already:
                problem_detected_in_document_already = True
                problem_docs_count += 1
                attr_value = doc["_source"].get("ops:Harvest_Info/ops:harvest_date_time", None)
                try:
                    doc_harvest_time = dateutil.parser.isoparse(attr_value[0]).astimezone(timezone.utc)

                    earliest_problem_doc_harvested_at = min(
                        doc_harvest_time, earliest_problem_doc_harvested_at or doc_harvest_time
                    )
                    latest_problem_doc_harvested_at = max(
                        doc_harvest_time, latest_problem_doc_harvested_at or doc_harvest_time
                    )
                except (KeyError, ValueError) as err:
                    log.warning(
                        limit_log_length(
                            f'Unable to parse first element of "ops:Harvest_Info/ops:harvest_date_time" as ISO-formatted date from document {doc["_id"]}: {attr_value} ({err})'
                        )
                    )

                try:
                    problematic_harvest_versions.update(doc["_source"]["ops:Harvest_Info/ops:harvest_version"])
                except KeyError as err:
                    # Noisy log temporarily disabled but may be re-enabled at jpadams' discretion
                    # log.warning(limit_log_length(f'Unable to extract harvest version from document {doc["_id"]}: {err}'))
                    pass

            if mapping_missing and property_name not in missing_mapping_updates:
                if canonical_type_is_defined:
                    log.info(
                        limit_log_length(
                            f'Property {property_name} will be updated to type "{canonical_type}" from data dictionary'
                        )
                    )
                    missing_mapping_updates[property_name] = canonical_type  # type: ignore
                elif property_name.startswith(
                    "ops:Provenance"
                ):  # TODO: extract this to a constant, used by all metadata key definitions
                    # mappings for registry-sweepers are the responsibility of their respective sweepers and should not
                    # be touched by the reindexer sweeper
                    if property_name not in sweepers_missing_property_names:
                        log.warning(
                            limit_log_length(
                                f"Property {property_name} is missing from the index mapping, but is a sweepers metadata attribute and will not be fixed here. Please run the full set of sweepers on this index"
                            )
                        )
                        sweepers_missing_property_names.add(property_name)
                else:
                    # if there is no canonical type and it is not a provenance metadata key, do nothing, per jpadams
                    pass

    log.info(
        limit_log_length(
            f"RESULT: Detected {format_hits_count(problem_docs_count)} docs with {len(missing_mapping_updates)} missing mappings and {len(bad_mapping_property_names)} mappings conflicting with the DD, out of a total of {format_hits_count(total_docs_count)} docs"
        )
    )

    if problem_docs_count > 0:
        log.warning(
            limit_log_length(
                f"RESULT: Problems were detected with docs having harvest timestamps between {earliest_problem_doc_harvested_at.isoformat()} and {latest_problem_doc_harvested_at.isoformat()}"  # type: ignore
            )
        )
        log.warning(
            limit_log_length(
                f"RESULT: Problems were detected with docs having harvest versions {sorted(problematic_harvest_versions)}"
            )
        )

    if len(missing_mapping_updates) > 0:
        log.info(
            limit_log_length(
                f"RESULT: Mappings will be added for the following properties: {sorted(missing_mapping_updates.keys())}"
            )
        )

    if len(canonical_type_undefined_property_names) > 0:
        log.info(
            limit_log_length(
                f"RESULT: Mappings were not found in the DD or static types for the following properties: {sorted(canonical_type_undefined_property_names)}"
            )
        )

    if len(bad_mapping_property_names) > 0:
        log.error(
            limit_log_length(
                f"RESULT: The following mappings have a type which does not match the type described by the data dictionary: {bad_mapping_property_names} - in-place update is not possible, data will need to be manually reindexed with manual updates (or that functionality must be added to this sweeper"
            )
        )

    return missing_mapping_updates


def generate_updates(
    timestamp: datetime, extant_mapping_keys: Collection[str], docs: Iterable[Dict]
) -> Iterable[Update]:
    for document in docs:
        id = document["_id"]
        extant_mapping_keys = set(extant_mapping_keys)
        document_field_names = set(document["_source"].keys())
        document_fields_missing_from_mappings = document_field_names.difference(extant_mapping_keys)
        if len(document_fields_missing_from_mappings) > 0:
            logging.debug(
                f"Missing mappings {document_fields_missing_from_mappings} detected when attempting to create Update for doc with id {id} - skipping"
            )

        yield Update(id=id, content={REINDEXER_FLAG_METADATA_KEY: timestamp.isoformat()})


def format_hits_count(count: int) -> str:
    """Format hits count in a more human-friendly manner for logs"""
    if count < 1e4:
        return str(count)
    elif count < 1e5:
        adjusted_count = count / 1e3
        return "{:,.1f}K".format(adjusted_count)
    elif count < 1e6:
        adjusted_count = count / 1e3
        return "{:,.0f}K".format(adjusted_count)
    else:
        adjusted_count = count / 1e6
        return "{:,.2f}M".format(adjusted_count)


def run(
    client: OpenSearch,
    log_filepath: Union[str, None] = None,
    log_level: int = logging.INFO,
):
    configure_logging(filepath=log_filepath, log_level=log_level)

    sweeper_start_timestamp = datetime.now()
    products_index_name = resolve_multitenant_index_name(client, "registry")
    ensure_index_mapping(client, products_index_name, REINDEXER_FLAG_METADATA_KEY, "date")

    dd_field_types_by_field_name = fetch_dd_field_types(client)

    def get_updated_hits_count():
        return get_query_hits_count(client, products_index_name, get_docs_query(sweeper_start_timestamp))

    # AOSS was becoming overloaded during iteration while accumulating missing mappings on populous nodes, so it is
    # necessary to impose a limit for how many products are iterated over before a batch of updates is created and
    # written.  This allows incremental progress to be made and limits the amount of work discarded in the event of an
    # overload condition.
    # Using the harvest timestamp as a sort field acts as a soft guarantee of consistency of query results between the
    # searches performed during accumulate_missing_mappings() and generate_updates(), and then a final check is applied
    # within generate_updates() to ensure that the second stage (update generation) hasn't picked up any products which
    # weren't processed in the first stage (missing mapping accumulation)
    batch_size_limit = 100000
    sort_fields = ["ops:Harvest_Info/ops:harvest_date_time"]
    total_outstanding_doc_count = get_updated_hits_count()

    with tqdm(
        total=total_outstanding_doc_count,
        desc="Reindexer sweeper progress",
    ) as pbar:
        current_batch_size = min(batch_size_limit, total_outstanding_doc_count)
        final_batch_is_processed = False
        while not final_batch_is_processed:
            mapping_field_types_by_field_name = get_mapping_field_types_by_field_name(client, products_index_name)

            missing_mappings = accumulate_missing_mappings(
                dd_field_types_by_field_name,
                mapping_field_types_by_field_name,
                query_registry_db_with_search_after(
                    client,
                    products_index_name,
                    _source={},
                    query=get_docs_query(sweeper_start_timestamp),
                    limit=batch_size_limit,
                    sort_fields=sort_fields,
                ),
            )
            for property, mapping_typename in missing_mappings.items():
                log.info(
                    limit_log_length(
                        f"Updating index {products_index_name} with missing mapping ({property}, {mapping_typename})"
                    )
                )
                ensure_index_mapping(client, products_index_name, property, mapping_typename)

            updated_mapping_keys = get_mapping_field_types_by_field_name(client, products_index_name).keys()
            updates = generate_updates(
                sweeper_start_timestamp,
                updated_mapping_keys,
                query_registry_db_with_search_after(
                    client,
                    products_index_name,
                    _source={},
                    query=get_docs_query(sweeper_start_timestamp),
                    limit=batch_size_limit,
                    sort_fields=sort_fields,
                ),
            )
            log.info(
                limit_log_length(
                    f"Updating newly-processed documents with {REINDEXER_FLAG_METADATA_KEY}={sweeper_start_timestamp.isoformat()}..."
                )
            )
            write_updated_docs(
                client,
                updates,
                index_name=products_index_name,
            )

            # If the current batch isn't a full page, it must be the last page and all updates are pending.
            # Terminate loop on this basis to avoid lots of redundant updates.
            final_batch_is_processed = current_batch_size < batch_size_limit
            pbar.update(current_batch_size)

            # Update batch size for next page of hits
            current_batch_size = min(batch_size_limit, get_updated_hits_count())

    log.info(limit_log_length("Completed reindexer sweeper processing!"))


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
