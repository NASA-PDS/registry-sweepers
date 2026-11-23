# Registry Sweepers

This package provides supplementary metadata generation for registry documents, which is required for registry-api to function correctly, and for common user queries. Execution is idempotent and should be scheduled on a recurring basis.

### Components

#### [RepairKit](https://github.com/NASA-PDS/registry-sweepers/blob/main/src/pds/registrysweepers/repairkit/__init__.py)
The repairkit sweeper applies idempotent transformations to targeted subsets of properties, for example ensuring that all properties expected to have array-like values are in fact arrays (as opposed to single-element arrays being flattened to strings during harvest).  Documents are processed based on whether their `ops:Provenance/ops:registry_sweepers_repairkit_version` metadata value is up-to-date relative to the sweeper codebase.

> **Note:** RepairKit is currently disabled. It is not compatible with the current registry and requires refactoring before it can be executed.

#### [Provenance](https://github.com/NASA-PDS/registry-sweepers/blob/main/src/pds/registrysweepers/provenance.py)
The provenance sweeper generates metadata for linking each version-superseded product with the versioned product which supersedes it.  The value of the successor is stored in the `ops:Registry_Sweepers/ops:superseded_by` property.  This property will not be set for the latest version of any product. All documents are processed, but db writes are optimised based on whether their `ops:Registry_Sweepers/ops:provenance_software_version` metadata value is up-to-date relative to the sweeper codebase.

#### [Ancestry](https://github.com/NASA-PDS/registry-sweepers/blob/main/src/pds/registrysweepers/ancestry/__init__.py)

The ancestry sweeper generates membership metadata for each product, i.e. which bundle and collection identifiers (LID
and LIDVID) reference a given product. These values will be stored in property `ops:Registry_Sweepers.ops:ancestor_refs)`.

Ancestry is processed collection-by-collection.  Any ancestry metadata generated for products which do not yet exist in registry is stored in a deferral index, and merged if/when the corresponding document is written to the registry.  Processing is skipped for collections (and their respective child non-aggregates) where the collection is considered up-to-date, i.e. where their `ops:Registry_Sweepers/ops:ancestry_software_version` metadata value is up-to-date relative to the sweeper codebase.

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
# Run the full default suite (provenance, ancestry, reindexer)
pds-registry-sweepers

# Run a single sweeper
pds-registry-sweepers --only ancestry

# Run multiple specific sweepers
pds-registry-sweepers --only ancestry provenance

# Run the legacy registry sync sweeper
pds-registry-sweepers --only legacy-sync
```

- With no flags, the full default suite runs: `provenance`, `ancestry`, `reindexer`.
- With `--only <name> [name ...]`, only the named sweeper(s) run. Available names: `provenance`, `ancestry`, `reindexer`, `legacy-sync`.

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


## Testing

Install the dev extras first: `pip install --editable '.[dev]'`.

Tests come in two kinds:

- **Unit tests** need nothing running.
- **DB tests** run the sweepers against a real OpenSearch (`opensearchproject/opensearch:2.19.x`, version pinned in [`tests/support/opensearch_server.py`](tests/support/opensearch_server.py)). Any test that uses the `registry_db` fixture is marked `db` automatically.

```sh
pytest -m "not db"   # unit tests only; no Docker or OpenSearch needed
pytest -m db         # DB tests only
tox                  # unit tests (py312, py313), docs and lint; never needs Docker
tox -e db            # DB tests on py3.13 with coverage, as CI runs them
```

Tests run in parallel by default (`-n auto`). Pass `-n 0` to run them serially, which is easier to debug.

### Where the DB tests get OpenSearch

1. **`TEST_OPENSEARCH_URL`.** If this is set (e.g. `http://localhost:9200`), the tests use that instance, which must have security disabled.
2. **Devcontainer sidecar.** The [`.devcontainer/claude-code`](.devcontainer/claude-code) devcontainer starts an OpenSearch sidecar and sets `TEST_OPENSEARCH_URL=http://opensearch:9200`, so DB tests work there with no extra setup.
3. **testcontainers.** If `TEST_OPENSEARCH_URL` is unset and Docker is available, pytest starts a throwaway OpenSearch container for the session and removes it at exit. CI's `db-testing` job uses this route.

If none of these is available, DB tests are **skipped** locally. With `CI` set, they **fail** instead, so a CI job can't pass without having run them.

Each test gets its own indices under a random node prefix (`MULTITENANCY_NODE_ID`), which resolve to `<node>-registry-structured`, `-refs`, `-dd` and `-deferred-updates`. The indices are deleted when the test finishes, so tests can share one instance and run in parallel. Only structured-metadata mode is tested.

### Index mappings

The test indices are created from the committed mapping files in [`tests/resources/mappings/`](tests/resources/mappings/). That README says where they came from and how to refresh them. If any are missing, DB tests skip (or fail under `CI`) with a message naming the missing files.

Seed documents are checked against these mappings when loaded. A field that isn't in the mapping, such as a stale flat-schema name like `ops:Sweepers/ops:ancestry_version`, fails the load with an error listing the unknown fields. So does a nested object such as `{"ops:Harvest_Info": {...}}`: registry documents store fields under dotted top-level keys (`"ops:Harvest_Info.ops:harvest_date_time"`), so seed documents must too. Pass `validate=False` to `registry_db.load()` to seed deliberately invalid documents.

### Writing a DB test

Request the `registry_db` fixture, seed documents, run the sweeper with `registry_db.client`, then inspect the indices. Documents can be seeded three ways, and all three go through the same loader:

```python
from pds.registrysweepers.ancestry import main as ancestry
from tests.support.builders import CollectionRefsBuilder, build_bundle, build_collection, build_product


def test_example(registry_db, snapshot):
    # Builders: best for small, readable cases
    registry_db.load(
        [build_bundle("urn:nasa:pds:b::1.0", "urn:nasa:pds:b:c"),
         build_collection("urn:nasa:pds:b:c::1.0"),
         build_product("urn:nasa:pds:b:c:p::1.0")],
        index="registry",
    )
    registry_db.load([CollectionRefsBuilder("urn:nasa:pds:b:c::1.0").with_product("urn:nasa:pds:b:c:p::1.0").to_seed()])

    # Raw dicts: {"_id": ..., "_source": {...}}, plus "index" or the index= argument
    # JSON files: a list of such dicts under tests/resources/fixtures/, best for larger or irregular data
    # registry_db.load_json("ancestry/lid_ref_hierarchy.json")

    ancestry.run(client=registry_db.client)

    registry_db.refresh()
    docs = registry_db.dump("registry")   # sorted by _id
    assert docs == snapshot               # plus targeted asserts on the fields the test is about
```

See [`tests/pds/registrysweepers/ancestry/db/test_ancestry.py`](tests/pds/registrysweepers/ancestry/db/test_ancestry.py) for complete examples.

### Snapshots

DB tests compare dumped index contents with JSON snapshots under `__snapshots__/` next to the test file, using [syrupy](https://github.com/syrupy-project/syrupy). Two kinds of value are normalised so that snapshots stay stable:

- Ancestry reference lists are sorted.
- The current ancestry software version is written as `<SWEEPERS_ANCESTRY_VERSION>`, so bumping the version doesn't rewrite every snapshot.

A mismatch fails with a diff. After an intended behaviour change, regenerate the snapshots and review the diff before committing:

```sh
pytest -m db -n 0 --snapshot-update
```

### End-to-end smoke test

[`tests/pds/registrysweepers/test_driver_smoke.py`](tests/pds/registrysweepers/test_driver_smoke.py) runs `docker/sweepers_driver.py` as a subprocess with the default sweeper set against a seeded registry. It checks that the entry point wires everything together and exits cleanly. Sweeper logic is covered by the in-process DB tests.


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
