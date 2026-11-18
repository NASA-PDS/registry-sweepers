# Registry Sweepers

This package provides supplementary metadata generation for registry documents, which is required for registry-api to function correctly, and for common user queries. Execution is idempotent and should be scheduled on a recurring basis.

### Components

#### [RepairKit](https://github.com/NASA-PDS/registry-sweepers/blob/main/src/pds/registrysweepers/repairkit/__init__.py)
The repairkit sweeper applies idempotent transformations to targeted subsets of properties, for example ensuring that all properties expected to have array-like values are in fact arrays (as opposed to single-element arrays being flattened to strings during harvest).  Documents are processed based on whether their `ops:Provenance/ops:registry_sweepers_repairkit_version` metadata value is up-to-date relative to the sweeper codebase.

> **Note:** RepairKit is currently disabled. It is not compatible with the current registry and requires refactoring before it can be executed.

#### [Provenance](https://github.com/NASA-PDS/registry-sweepers/blob/main/src/pds/registrysweepers/provenance.py)
The provenance sweeper generates metadata for linking each version-superseded product with the versioned product which supersedes it.  The value of the successor is stored in the `ops:Provenance/ops:superseded_by` property.  This property will not be set for the latest version of any product. All documents are processed, but db writes are optimised based on whether their `ops:Provenance/ops:registry_sweepers_provenance_version` metadata value is up-to-date relative to the sweeper codebase.

#### [Ancestry](https://github.com/NASA-PDS/registry-sweepers/blob/main/src/pds/registrysweepers/ancestry/__init__.py)
The ancestry sweeper generates membership metadata for each product, i.e. which bundle lidvids and which collection lidvids reference a given product. These values will be stored in properties `ops:Provenance/ops:parent_bundle_identifier` and `ops:Provenance/ops:parent_collection_identifier`, respectively. All bundles/collections are processed to populate a lookup table, but db writes are optimised based on whether their `ops:Provenance/ops:registry_sweepers_provenance_version` metadata value is up-to-date relative to the sweeper codebase, and collection non-aggregate reference pages in registry-refs are skipped entirely if they are marked as up-to-date.

[Accepts environment variables to tune performance](./src/pds/registrysweepers/ancestry/runtimeconstants.py), primarily trading increased runtime duration for reduced peak memory usage.

#### [Ancestry Orphan Repair](https://github.com/NASA-PDS/registry-sweepers/blob/main/src/pds/registrysweepers/ancestry_orphan_repair/main.py)
The ancestry sweeper reaches a non-aggregate product only through the `registry-refs` documents of a collection which is not yet stamped with the current ancestry version. A product which is loaded, overwritten by Harvest, or listed in refs after its collection was swept therefore stays without ancestry, and re-harvesting every collection is not sustainable. The orphan repair is a separate, brute-force step which runs after the ancestry sweeper:

1. It pages (`search_after`) through the non-aggregate, non-superseded products which have no `ops:Provenance/ops:ancestor_refs` or an `ops:Provenance/ops:registry_sweepers_ancestry_version` below the current ancestry version.
2. For each batch of those products it looks up, with a single `terms` query, the `registry-refs` documents which list them, and keeps the parent collections which exist in the registry.
3. It writes exactly what the ancestry sweeper writes for such a product (the collection LIDVID and LID, and the version stamp), through the same `ProductUpdateRecord`, `update_from_record`, painless script and `write_updated_docs` path, so the result is identical to a normal run, retries and error handling included. Bundle LIDVIDs are not part of a non-aggregate product's ancestry, in the repair as in a normal run.

It **only repairs where a refs document confirms the parent**. A product that no refs document lists, or which is listed only under a collection that is not in the registry, is never guessed: nothing is written to it and it is not stamped, so every run examines it again and repairs it as soon as a refs document appears. These products are counted and reported separately as *unresolved*; they are data problems for the node (a collection inventory that was never loaded or that does not list them). Re-examining them costs a product query and a refs query per batch on every run.

Like every other sweeper, it **writes by default**. It reports counts (products examined, repairable, unresolved by reason, and repaired / already up to date / vanished before write / failed). Pass `--dry-run` to write nothing and list the products which would be repaired instead. A product that vanishes between read and write is counted and does not abort the run.

| Flags | Registry writes | stdout | `--output-file FILE` (optional) |
|---|---|---|---|
| none (default) | the repairs | the summary counts only | the listing of the repaired products |
| `--dry-run` | none | the listing of the products which would be repaired (TSV: `status`, `product_lidvid`, `collection_lidvids`), then the summary | the same listing, identical to stdout |

`--output-all` adds the unresolved products (no refs listing, collection not in the registry) to the listing, on stdout and in the file. Log messages go to stderr, so stdout holds only the listing and the summary; the summary is always last.

```bash
# write the repairs
python -m pds.registrysweepers.ancestry_orphan_repair --base-URL "${PROV_ENDPOINT}" --log-level INFO

# write the repairs, keeping a list of the repaired products
python -m pds.registrysweepers.ancestry_orphan_repair --base-URL "${PROV_ENDPOINT}" --output-file repaired.tsv

# dry run: print the products which would be repaired to stdout, then the summary
python -m pds.registrysweepers.ancestry_orphan_repair --base-URL "${PROV_ENDPOINT}" --dry-run

# dry run, also saving the listing to a file, and including the unresolved products
python -m pds.registrysweepers.ancestry_orphan_repair --base-URL "${PROV_ENDPOINT}" --dry-run --output-file repairable.tsv --output-all

# through the driver (credentials and MULTITENANCY_NODE_ID from the environment, as for the other sweepers)
pds-registry-sweepers --only ancestry ancestry-orphan-repair
pds-registry-sweepers --only ancestry-orphan-repair --dry-run
```

Through the driver, `--output-file` and `--output-all` apply to `ancestry-orphan-repair` only. `--dry-run` is accepted only when every selected sweeper supports it (currently only `ancestry-orphan-repair`), so the default suite refuses it; use `--only ancestry-orphan-repair --dry-run`.

Options: `--batch-size` (products examined and refs terms looked up per round trip, default 2000, at most 10000), `--include-superseded` (superseded versions are skipped by default), `--dry-run`, `--output-file` / `--output-all` (see above). It is part of the default suite, where it writes like the other sweepers, and the driver always runs it immediately after `ancestry` when both are selected. As for every sweeper, run it with no other sweeper running on the same node.

Limits:
- It cannot repair a product that no refs document lists, nor one whose only listing collection is missing from the registry.
- It does not fix a collection that was stamped without a link to its bundle ([#301](https://github.com/NASA-PDS/registry-sweepers/issues/301)); it only writes non-aggregate products.
- A product that already has a current stamp and ancestry for one collection is not examined, even if it is also listed by another collection whose refs were written after that collection was stamped, so the second collection is missing from its ancestry. A product that is new to the registry is not affected. Reconciling counts per collection would find these without reading every product ([#308](https://github.com/NASA-PDS/registry-sweepers/issues/308)).
- It repairs a product only from the refs documents as they are now: it does not check that the collection's inventory is complete.

#### [Reindexer](https://github.com/NASA-PDS/registry-sweepers/blob/main/src/pds/registrysweepers/reindexer/main.py)
The reindexer sweeper ensures that the registry index mappings are updated with all fields available in the registry-dd index, and then triggers reindexation on all products which have not yet been successfully processed previously.  This ensures that all products are searchable on all fields, provided a field type mapping is defined in the registry-dd index at the time of processing.

#### [Legacy Registry Sync](https://github.com/NASA-PDS/registry-sweepers/blob/main/src/pds/registrysweepers/legacy_registry_sync/legacy_registry_sync.py)
The legacy registry sync tool migrates data from the legacy Solr-based PDS registry to the new OpenSearch-based registry. It includes a dry-run mode for testing Solr data retrieval without affecting OpenSearch.

**Console Script Usage:**

```bash
# Install the package first
pip install -e .

# Run dry-run mode (Solr data retrieval only, no OpenSearch operations)
pds-legacy-registry-sync --dry-run --max-docs 10

# Comprehensive dry-run with logging
pds-legacy-registry-sync --dry-run --max-docs 100 --log-file dry_run.log

# Dry-run without showing sample documents
pds-legacy-registry-sync --dry-run --max-docs 50 --no-samples

# Dry-run with more sample documents
pds-legacy-registry-sync --dry-run --max-docs 20 --sample-size 10

# Dry-run with payload output for offline inspection
pds-legacy-registry-sync --dry-run --max-docs 100 --output-file /tmp/payload.jsonl

# Get help
pds-legacy-registry-sync --dry-run --help
```

**Dry-Run Features:**
- **Solr-only operations**: No OpenSearch connection required
- **Data analysis**: Shows node distribution, product class breakdown, and data quality metrics
- **Sample documents**: Displays sample documents with key fields
- **Progress tracking**: Logs progress every 1000 documents
- **Error handling**: Continues processing and reports errors
- **Payload export**: Optionally writes OpenSearch payloads as JSON lines to a file for offline inspection (default: `/tmp/legacy_registry_sync_payload.jsonl`)

**Dry-Run Output:**
The tool provides comprehensive statistics including:
- Total documents processed
- Documents with/without lidvid
- Node distribution (PDS_ENG, PDS_IMG, etc.)
- Product class distribution
- Node-by-product-class breakdown with percentages
- Sample documents with key fields
- Domain and node ID analysis

**Full synchronization** (Solr + OpenSearch) is available programmatically via the `run()` function but is not yet implemented as a console script option.

## On-Demand Execution

Use `pds-registry-sweepers` to run the sweepers from the command line.

```bash
# Run the full default suite (provenance, ancestry, ancestry orphan repair, reindexer)
pds-registry-sweepers

# Run a single sweeper
pds-registry-sweepers --only ancestry

# Run multiple specific sweepers
pds-registry-sweepers --only ancestry provenance

# Run the legacy registry sync sweeper
pds-registry-sweepers --only legacy-sync
```

- With no flags, the full default suite runs: `provenance`, `ancestry`, `ancestry-orphan-repair`, `reindexer`. The orphan repair runs right after `ancestry` and writes, like the other sweepers.
- With `--only <name> [name ...]`, only the named sweeper(s) run. Available names: `provenance`, `ancestry`, `reindexer`, `legacy-sync`, `ancestry-orphan-repair` (`--dry-run` is accepted only when every selected sweeper supports it, i.e. with `--only ancestry-orphan-repair`; see above).

`PROV_ENDPOINT` and (for non-AWS OpenSearch) `PROV_CREDENTIALS` environment variables are required. See the Developer Quickstart section for details.

## Developer Quickstart

### Prerequisites

#### Dependencies
- Python >=3.12

#### Environment Variables
```
## General Config
LOGLEVEL=INFO   # an integer log level (0, 1, 2, 3) or string matching a
                # log level (e.g. `INFO`). Default: `INFO`


## Applicable when using AWS AOSS
MULTITENANCY_NODE_ID=                 # If running in a multitenant environment, the id of the node.
                                      # Previously, distinguished registry/registry-refs index instances

SWEEPERS_IAM_ROLE_NAME=<value>        # AWS IAM role name, if targeting AWS AOSS

PROV_ENDPOINT=https://localhost:9200  # OpenSearch host url and port

## Applicable when using non-AWS OpenSearch
PROV_CREDENTIALS={"admin": "admin"}   # OpenSearch username/password
                                      # Only applicable for hosts other than AWS AOSS

## For Developers
DEV_MODE=1      #disables host verification

# tqdm dependency may cause fatal crashes on some architectures when
# breakpoints are used in debug mode with Cython speedup extension enabled
PYDEVD_USE_CYTHON=NO // disables Cython speedup extension
```

With `--only legacy-sync`, the "registry" alias mapping all the discipline nodes indexes is required.

Use the connection aliases found in the 'Connections' tab of the Engineering Node OpenSearch Domain on AWS.

https://us-west-2.console.aws.amazon.com/aos/home?region=us-west-2#opensearch/domains/en-prod?tabId=ccs

After cloning the repository, and setting the repository root as the current working directory install the package with `pip install -e .`

The wrapper script for the suite of components may be run with `python ./docker/sweepers_driver.py`

Alternatively, registry-sweepers may be built from its [Dockerfile](./docker/Dockerfile) with `docker image build --file ./docker/Dockerfile .` and run as a container, providing those same environment variables when running the container.

### Performance

#### Rough Benchmarks
When run against the production OpenSearch instance with ~1.1M products, no cross-cluster remotes, and (only) ~1k multi-version products, from a local development machine, the runtime is ~20min on first run and ~12min subsequently.  It appears that OpenSearch optimizes away no-op update calls, resulting in significant speedup despite the fact that registry-sweepers reprocesses metadata from scratch, every run.

The overwhelming bottleneck ops are the O(docs_count) db writes in ancestry.


## Code of Conduct

All users and developers of the NASA-PDS software are expected to abide by our [Code of Conduct](https://github.com/NASA-PDS/.github/blob/main/CODE_OF_CONDUCT.md). Please read this to ensure you understand the expectations of our community.


## Development

To develop this project, use your favorite text editor, or an integrated development environment with Python support, such as [PyCharm](https://www.jetbrains.com/pycharm/).


### Contributing

For information on how to contribute to NASA-PDS codebases please take a look at our [Contributing guidelines](https://github.com/NASA-PDS/.github/blob/main/CONTRIBUTING.md).


## Build

    pip install build
    python3 -m build .


## Publication

NASA PDS packages can publish automatically using the [Roundup Action](https://github.com/NASA-PDS/roundup-action), which leverages GitHub Actions to perform automated continuous integration and continuous delivery. A default workflow that includes the Roundup is provided in the `.github/workflows/unstable-cicd.yaml` file. (Unstable here means an interim release.)


### Manual Publication

Create the package:

    python3 -m build .

Publish it as a Github release.

Publish on PyPI (you need a PyPI account and configure `$HOME/.pypirc`):

    pip install twine
    twine upload dist/*

Or publish on the Test PyPI (you need a Test PyPI account and configure `$HOME/.pypirc`):

    pip install twine
    twine upload --repository testpypi dist/*


## CI/CD

The template repository comes with our two "standard" CI/CD workflows, `stable-cicd` and `unstable-cicd`. The unstable build runs on any push to `main` (± ignoring changes to specific files) and the stable build runs on push of a release branch of the form `release/<release version>`. Both of these make use of our GitHub actions build step, [Roundup](https://github.com/NASA-PDS/roundup-action). The `unstable-cicd` will generate (and constantly update) a SNAPSHOT release. If you haven't done a formal software release you will end up with a `v0.0.0-SNAPSHOT` release (see NASA-PDS/roundup-action#56 for specifics).
