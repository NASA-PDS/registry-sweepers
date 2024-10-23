import logging
from datetime import datetime
from datetime import timezone
from typing import Collection
from typing import Dict
from typing import Iterable
from typing import Union

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
from tqdm import tqdm

log = logging.getLogger(__name__)


def get_docs_query(filter_to_harvested_before: datetime):
    """
    Return a query to get all docs which haven't been reindexed by this sweeper and which haven't been harvested
    since this sweeper process instance started running
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
    dd_index_name = resolve_multitenant_index_name("registry-dd")
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
        k: v["type"] for k, v in client.indices.get_mapping(index_name)[index_name]["mappings"]["properties"].items()
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
    missing_mapping_updates: Dict[str, str] = {}

    dd_not_defines_type_property_names = set()  # used to prevent duplicate WARN logs
    bad_mapping_property_names = set()  # used to log mappings requiring manual attention

    earliest_problem_doc_harvested_at = None
    latest_problem_doc_harvested_at = None
    problematic_harvest_versions = set()
    problem_docs_count = 0
    total_docs_count = 0
    for doc in docs:
        problem_detected_in_document_already = False
        total_docs_count += 1

        for property_name, value in doc["_source"].items():
            canonical_type = dd_field_types_by_name.get(property_name)
            current_mapping_type = mapping_field_types_by_field_name.get(property_name)

            mapping_missing = property_name not in mapping_field_types_by_field_name
            dd_defines_type_for_property = property_name in dd_field_types_by_name
            mapping_is_bad = all(
                [canonical_type != current_mapping_type, canonical_type is not None, current_mapping_type is not None]
            )

            if not dd_defines_type_for_property and property_name not in dd_not_defines_type_property_names:
                log.warning(
                    f"Property {property_name} does not have an entry in the DD index - this may indicate a problem"
                )
                dd_not_defines_type_property_names.add(property_name)

            if mapping_is_bad and property_name not in bad_mapping_property_names:
                log.warning(
                    f'Property {property_name} is defined in data dictionary as type "{canonical_type}" but exists in index mapping as type "{current_mapping_type}".)'
                )
                bad_mapping_property_names.add(property_name)

            if (mapping_missing or mapping_is_bad) and not problem_detected_in_document_already:
                problem_detected_in_document_already = True
                problem_docs_count += 1
                try:
                    doc_harvest_time = datetime.fromisoformat(
                        doc["_source"]["ops:Harvest_Info/ops:harvest_date_time"][0].replace("Z", ""),
                    )
                    earliest_problem_doc_harvested_at = min(
                        doc_harvest_time, earliest_problem_doc_harvested_at or datetime.max
                    )
                    latest_problem_doc_harvested_at = max(
                        doc_harvest_time, latest_problem_doc_harvested_at or datetime.min
                    )
                except (KeyError, ValueError) as err:
                    log.warning(
                        f'Unable to parse "ops:Harvest_Info/ops:harvest_date_time" as zulu-formatted date from document {doc["_id"]}: {err}'
                    )

                try:
                    problematic_harvest_versions.update(doc["_source"]["ops:Harvest_Info/ops:harvest_version"])
                except KeyError as err:
                    log.warning(f'Unable to extract harvest version from document {doc["_id"]}: {err}')

            if mapping_missing and property_name not in missing_mapping_updates:
                if dd_defines_type_for_property:
                    log.info(
                        f'Property {property_name} will be updated to type "{canonical_type}" from data dictionary'
                    )
                    missing_mapping_updates[property_name] = canonical_type  # type: ignore
                else:
                    default_type = "keyword"
                    log.warning(
                        f'Property {property_name} is missing from the index mappings and does not have an entry in the data dictionary index - defaulting to type "{default_type}"'
                    )
                    missing_mapping_updates[property_name] = default_type

    log.info(
        f"RESULT: Detected {problem_docs_count} docs with {len(missing_mapping_updates)} missing mappings and {len(bad_mapping_property_names)} mappings conflicting with the DD, out of a total of {total_docs_count} docs"
    )

    if problem_docs_count > 0:
        log.warning(
            f"RESULT: Problems were detected with docs having harvest timestamps between {earliest_problem_doc_harvested_at.isoformat()} and {latest_problem_doc_harvested_at.isoformat()}"
            # type: ignore
        )
        log.warning(
            f"RESULT: Problems were detected with docs having harvest versions {sorted(problematic_harvest_versions)}"
        )

    if len(missing_mapping_updates) > 0:
        log.info(
            f"RESULT: Mappings will be added for the following properties: {sorted(missing_mapping_updates.keys())}"
        )

    if len(dd_not_defines_type_property_names) > 0:
        log.info(
            f"RESULT: Mappings were not found in the DD for the following properties, and a default type will be applied: {sorted(dd_not_defines_type_property_names)}"
        )

    if len(bad_mapping_property_names) > 0:
        log.error(
            f"RESULT: The following mappings have a type which does not match the type described by the data dictionary: {bad_mapping_property_names} - in-place update is not possible, data will need to be manually reindexed with manual updates (or that functionality must be added to this sweeper"
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
        if len(document_fields_missing_from_mappings) == 0:
            yield Update(id=id, content={REINDEXER_FLAG_METADATA_KEY: timestamp.isoformat()})
        else:
            logging.debug(
                f"Missing mappings {document_fields_missing_from_mappings} detected when attempting to create Update for doc with id {id} - skipping"
            )


def run(
    client: OpenSearch,
    log_filepath: Union[str, None] = None,
    log_level: int = logging.INFO,
):
    configure_logging(filepath=log_filepath, log_level=log_level)

    sweeper_start_timestamp = datetime.now()
    products_index_name = resolve_multitenant_index_name("registry")
    ensure_index_mapping(client, products_index_name, REINDEXER_FLAG_METADATA_KEY, "date")

    dd_field_types_by_field_name = fetch_dd_field_types(client)

    # AOSS was becoming overloaded during iteration while accumulating missing mappings on populous nodes, so it is
    # necessary to impose a limit for how many products are iterated over before a batch of updates are created and
    # written.  This allows incremental progress to be made and limits the amount of work discarded in the event of an
    # overload condition.
    # Using the harvest timestamp as a sort field acts as a soft guarantee of consistency of query results between the
    # searches performed during accumulate_missing_mappings() and generate_updates(), and then a final check is applied
    # within generate_updates() to ensure that the second stage (update generation) hasn't picked up any products which
    # weren't processed in the first stage (missing mapping accumulation)
    batch_size_limit = 100000
    sort_fields = ["ops:Harvest_Info/ops:harvest_date_time"]
    with tqdm(
        total=get_query_hits_count(client, products_index_name, get_docs_query(sweeper_start_timestamp)),
        desc=f"Reindexer sweeper progress",
    ) as pbar:
        while get_query_hits_count(client, products_index_name, get_docs_query(sweeper_start_timestamp)) > 0:
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
                log.info(f"Updating index {products_index_name} with missing mapping ({property}, {mapping_typename})")
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
                f"Updating newly-processed documents with {REINDEXER_FLAG_METADATA_KEY}={sweeper_start_timestamp.isoformat()}..."
            )
            write_updated_docs(
                client,
                updates,
                index_name=products_index_name,
            )

            pbar.update(batch_size_limit)

    log.info("Completed reindexer sweeper processing!")


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
