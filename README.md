# Registry Sweepers

This package provides supplementary metadata generation for registry documents, which is required for registry-api to function correctly, and for common user queries. Execution is idempotent and should be scheduled on a recurring basis.

### Components

#### [RepairKit](https://github.com/NASA-PDS/registry-sweepers/blob/main/src/pds/registrysweepers/repairkit/__init__.py)
The repairkit sweeper applies idempotent transformations to targeted subsets of properties, for example ensuring that all properties expected to have array-like values are in fact arrays (as opposed to single-element arrays being flattened to strings during harvest).  Documents are processed based on whether their `ops:Provenance/ops:registry_sweepers_repairkit_version` metadata value is up-to-date relative to the sweeper codebase.

#### [Provenance](https://github.com/NASA-PDS/registry-sweepers/blob/main/src/pds/registrysweepers/provenance.py)
The provenance sweeper generates metadata for linking each version-superseded product with the versioned product which supersedes it.  The value of the successor is stored in the `ops:Provenance/ops:superseded_by` property.  This property will not be set for the latest version of any product. All documents are processed, but db writes are optimised based on whether their `ops:Provenance/ops:registry_sweepers_provenance_version` metadata value is up-to-date relative to the sweeper codebase.

#### [Ancestry](https://github.com/NASA-PDS/registry-sweepers/blob/main/src/pds/registrysweepers/ancestry/__init__.py)
The ancestry sweeper generates membership metadata for each product, i.e. which bundle lidvids and which collection lidvids reference a given product. These values will be stored in properties `ops:Provenance/ops:parent_bundle_identifier` and `ops:Provenance/ops:parent_collection_identifier`, respectively. All bundles/collections are processed to populate a lookup table, but db writes are optimised based on whether their `ops:Provenance/ops:registry_sweepers_provenance_version` metadata value is up-to-date relative to the sweeper codebase, and collection non-aggregate reference pages in registry-refs are skipped entirely if they are marked as up-to-date.

[Accepts environment variables to tune performance](./src/pds/registrysweepers/ancestry/runtimeconstants.py), primarily trading increased runtime duration for reduced peak memory usage.

## Developer Quickstart

### Prerequisites

#### Dependencies
- Python >=3.9

#### Environment Variables
```
MULTITENANCY_NODE_ID=  // If running in a multitenant environment, the id of the node, used to distinguish registry/registry-refs index instances
PROV_CREDENTIALS={"admin": "admin"}  // OpenSearch username/password
PROV_ENDPOINT=https://localhost:9200  // OpenSearch host url and port
LOGLEVEL - an integer log level or anycase string matching a python log level like `INFO` (optional - defaults to `INFO`))
DEV_MODE=1  // disables host verification

// tqdm dependency may cause fatal crashes on some architectures when breakpoints are used in debug mode with Cython speedup extension enabled
PYDEVD_USE_CYTHON=NO // disables Cython speedup extension
```

With `--legacy-sync` option, you also need the list of the cross-cluster-search node configured to access all the node's OpensSearch domains:

```
CCS_CONN=naif-prod-ccs,rms-prod,sbnumd-prod-ccs,geo-prod-ccs,atm-prod-ccs,sbnpsi-prod-ccs,ppi-prod-ccs,img-prod-ccs
```

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


### Installation

Install in editable mode and with extra developer dependencies into your virtual environment of choice:

    pip install --editable '.[dev]'

Configure the `pre-commit` hooks:

    pre-commit install
    pre-commit install -t pre-push
    pre-commit install -t prepare-commit-msg
    pre-commit install -t commit-msg

These hooks check code formatting and also aborts commits that contain secrets such as passwords or API keys. However, a one time setup is required in your global Git configuration. See [the wiki entry on Git Secrets](https://github.com/NASA-PDS/nasa-pds.github.io/wiki/Git-and-Github-Guide#git-secrets) to learn how.

### Packaging

To isolate and be able to re-produce the environment for this package, you should use a [Python Virtual Environment](https://docs.python.org/3/tutorial/venv.html). To do so, run:

    python -m venv venv

Then exclusively use `venv/bin/python`, `venv/bin/pip`, etc.

If you have `tox` installed and would like it to create your environment and install dependencies for you run:

    tox --devenv <name you'd like for env> -e dev

Dependencies for development are specified as the `dev` `extras_require` in `setup.cfg`; they are installed into the virtual environment as follows:

    pip install --editable '.[dev]'

All the source code is in a sub-directory under `src`.


### Tests

This section describes testing for your package.

A complete "build" including test execution, linting (`mypy`, `black`, `flake8`, etc.), and documentation build is executed via:

    tox


#### Unit tests

Your project should have built-in unit tests, functional, validation, acceptance, etc., tests.

For unit testing, check out the [unittest](https://docs.python.org/3/library/unittest.html) module, built into Python 3.

Tests objects should be in packages `test` modules or preferably in project 'tests' directory which mirrors the project package structure.

Our unit tests are launched with command:

    pytest

If you want your tests to run automatically as you make changes start up `pytest` in watch mode with:

    ptw


## Build

    pip install wheel
    python setup.py sdist bdist_wheel


## Publication

NASA PDS packages can publish automatically using the [Roundup Action](https://github.com/NASA-PDS/roundup-action), which leverages GitHub Actions to perform automated continuous integration and continuous delivery. A default workflow that includes the Roundup is provided in the `.github/workflows/unstable-cicd.yaml` file. (Unstable here means an interim release.)


### Manual Publication

Create the package:

    python setup.py bdist_wheel

Publish it as a Github release.

Publish on PyPI (you need a PyPI account and configure `$HOME/.pypirc`):

    pip install twine
    twine upload dist/*

Or publish on the Test PyPI (you need a Test PyPI account and configure `$HOME/.pypirc`):

    pip install twine
    twine upload --repository testpypi dist/*

## CI/CD

The template repository comes with our two "standard" CI/CD workflows, `stable-cicd` and `unstable-cicd`. The unstable build runs on any push to `main` (± ignoring changes to specific files) and the stable build runs on push of a release branch of the form `release/<release version>`. Both of these make use of our GitHub actions build step, [Roundup](https://github.com/NASA-PDS/roundup-action). The `unstable-cicd` will generate (and constantly update) a SNAPSHOT release. If you haven't done a formal software release you will end up with a `v0.0.0-SNAPSHOT` release (see NASA-PDS/roundup-action#56 for specifics).
