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

#### [Reindexer](https://github.com/NASA-PDS/registry-sweepers/blob/main/src/pds/registrysweepers/reindexer/main.py)
The reindexer sweeper ensures that the registry index mappings are updated with all fields available in the registry-dd index, and then triggers reindexation on all products which have not yet been successfully processed previously.  This ensures that all products are searchable on all fields, provided a field type mapping is defined in the registry-dd index at the time of processing.

#### [Legacy Registry Sync](https://github.com/NASA-PDS/registry-sweepers/blob/main/src/pds/registrysweepers/legacy_registry_sync/legacy_registry_sync.py)
The legacy registry sync tool migrates data from the legacy Solr-based PDS registry to the new OpenSearch-based registry. Every run writes all Solr documents to the `en-legacy-registry` index, then prunes documents which are no longer in Solr. Pruning is skipped if any document failed to write, or if the run wrote fewer than 90% of the documents already in the index.

**Console Script Usage:**

```bash
# Install the package first
pip install -e .

# Report what would be written and pruned, without modifying OpenSearch
pds-legacy-registry-sync --dry-run

# Full sync
pds-legacy-registry-sync

# Full sync, overwriting the node of documents which already have one
pds-legacy-registry-sync --force --log-file sync.log

# Get help
pds-legacy-registry-sync --help
```

The sweepers driver accepts the same flag:

```bash
python ./docker/sweepers_driver.py --only legacy-sync --dry-run
```

**Dry-Run Behavior:**
- OpenSearch is read but never modified: the index is not created, written to, or pruned
- Reads the full Solr index and the document ids currently in `en-legacy-registry`
- Logs the number of documents that would be written, how many of those are new, and how many would be pruned
- Logs a sample of the lidvids that would be pruned
- Warns if the prune would be skipped by the 90% safeguard

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

# Force every document in the index to be reprocessed and re-stamped
pds-registry-sweepers --force

# Equivalent, via environment variable (e.g. for containerized runs)
SWEEPERS_FORCE_REPROCESS=true pds-registry-sweepers
```

- With no flags, the full default suite runs: `provenance`, `ancestry`, `reindexer`.
- With `--only <name> [name ...]`, only the named sweeper(s) run. Available names: `provenance`, `ancestry`, `reindexer`, `legacy-sync`.
- With `--force` (alias `--overwrite`) or `SWEEPERS_FORCE_REPROCESS=true`, the `provenance`, `ancestry` and `reindexer` sweepers ignore the sweeper version metadata already present on documents, and reprocess and re-stamp every document in the index. `legacy-sync` ignores this option. The flag is also accepted by the `provenance`, `ancestry` and `reindexer` modules when run directly (e.g. `python -m pds.registrysweepers.provenance --force`).

A forced run touches the whole index, so expect a runtime closer to a first run than a routine one.

Every forced document is physically rewritten, even if the regenerated values equal those already stored. This is what makes documents searchable on a field that was added to the index mapping after they were written. The `provenance` and `reindexer` sweepers send their partial-document bulk updates with `"detect_noop": false`; without it OpenSearch skips an update which changes nothing. The `ancestry` sweeper uses an inline script, which has no `detect_noop` option, so a forced run uses a variant of the script that omits its `ctx.op = 'none'` no-op guard. What `ancestry` writes is the same either way. A forced `ancestry` run issues one update per ancestry record, so a document with several records is rewritten once per record.

The `ancestry` sweeper merges regenerated ancestry references into those already on a document; it does not remove references that are no longer valid.

These write behaviours have not been verified against a live OpenSearch Serverless (AOSS) collection. Verify a forced run on a representative index before relying on it in production.

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
