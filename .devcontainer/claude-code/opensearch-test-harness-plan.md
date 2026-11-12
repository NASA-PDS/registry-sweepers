# Plan: OpenSearch-backed test harness for registry-sweepers

**Audience:** Claude Code, running in the repo's `.devcontainer/claude-code` devcontainer.
**Scope:** Shared test infrastructure, plus database-backed tests for the **ancestry** sweeper. Other sweepers come later and are out of scope.
**Repo state this plan was written against:** `main` as of 2026-11-11 (commit date). If files referenced below have moved or changed shape, stop and reconcile before proceeding.

---

## 0. Decisions already made (do not revisit)

| # | Decision |
|---|---|
| D1 | Framework is **pytest**. Existing `unittest.TestCase` tests stay as they are; pytest runs them. Don't convert them as part of this work. |
| D2 | Target is **self-hosted OpenSearch**, not AOSS. AOSS compatibility is explicitly not a goal. |
| D3 | OpenSearch image is **`opensearchproject/opensearch:2.19.x`**. Define the version in exactly one place (see §2.1), with the comment `# TODO: confirm this matches the production managed-OpenSearch version`. |
| D4 | **Structured-metadata mode only.** Every DB test sets `MULTITENANCY_NODE_ID`, so indices resolve to `<node>-registry-structured`, `<node>-registry-structured-refs`, `<node>-registry-structured-dd`. Legacy (flat) mode is not tested. |
| D5 | Index mappings are **static files supplied by the user** under `tests/resources/mappings/` (§2.3). Do not fetch, generate or invent mappings. If they're missing, DB tests must fail (in CI) or skip (locally) with a clear message naming the missing files. |
| D6 | Fixture data can be written as **raw dicts, JSON files, or builders**. All three produce one canonical document form and go through **one loader** (§2.4–2.6). |
| D7 | Results are checked with **targeted assertions and snapshot comparison** (§2.7). |
| D8 | Logic tests call `pds.registrysweepers.ancestry.main.run(client=...)` directly. **One** end-to-end smoke test runs the driver as a subprocess (§3.3). |
| D9 | CI: a **separate DB job** in the branch workflow, on a single Python version (3.13), running on every push or PR (§4). |
| D10 | Devcontainer: an **OpenSearch sidecar** via docker compose. Inside the devcontainer, tests reach it through `TEST_OPENSEARCH_URL` (§1). Elsewhere (developer machines with Docker, and GitHub Actions) tests use **testcontainers**. |

---

## 1. Phase 0: Devcontainer sidecar (do this first, then STOP)

You can't rebuild the container you're running in. Make these changes, commit them, then **stop and ask the user to rebuild the devcontainer** before starting Phase 1.

1. Convert `.devcontainer/claude-code/devcontainer.json` from `build` to `dockerComposeFile`:
   - Add `.devcontainer/claude-code/compose.yaml` with two services:
     - `devcontainer`: built from the existing `Dockerfile` with the same build args. Move `runArgs` caps (`NET_ADMIN`, `NET_RAW`) to `cap_add`, move the named-volume `mounts` to `volumes`, bind-mount the workspace (`../..:/workspace:cached`), keep `containerEnv` as `environment`, and use `command: sleep infinity`.
     - `opensearch`: image from D3 (the same pinned tag as §2.1; leave a comment cross-referencing it). Environment: `discovery.type=single-node`, `DISABLE_SECURITY_PLUGIN=true`, `DISABLE_INSTALL_DEMO_CONFIG=true`, `OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m`. Add a healthcheck that curls `http://localhost:9200/_cluster/health`. Don't publish ports to the host unless that's needed.
   - In `devcontainer.json`, set `"service": "devcontainer"`, `"runServices": ["devcontainer", "opensearch"]`, `"workspaceFolder": "/workspace"`, and remove `workspaceMount`/`runArgs`/`mounts`, which now live in compose. Add `TEST_OPENSEARCH_URL=http://opensearch:9200` to the devcontainer service environment.
2. `init-firewall.sh`: after the existing allowed-domain loop, resolve the sidecar with `getent hosts opensearch` and `ipset add allowed-domains <ip>`. The existing host-`/24` rule may already cover it, but don't rely on that. Add a verification step similar to the existing ones: `curl --connect-timeout 5 http://opensearch:9200` must succeed.
3. Update `.devcontainer/claude-code/README.md` to document the sidecar and `TEST_OPENSEARCH_URL`.

**Checkpoint:** commit, then tell the user: "Phase 0 done. Please rebuild the devcontainer, then confirm `curl http://opensearch:9200` works from inside it."

---

## 2. Phase 1: Shared test infrastructure

### Proposed layout

```
tests/
  conftest.py                      # root: markers, CLI options, container lifecycle (NEW)
  support/                         # (NEW) importable helpers, no tests
    __init__.py
    opensearch_server.py           # §2.1–2.2: endpoint discovery and container lifecycle
    indices.py                     # §2.3: mapping loading, per-test index set create/teardown
    documents.py                   # §2.4: canonical document type
    loader.py                      # §2.5: the single loader, plus mapping validation
    builders.py                    # §2.6: builders moved from ancestry/, fixed
    snapshots.py                   # §2.7: state dump and normalisation
  resources/
    mappings/
      README.md                    # what files go here and where they come from (user to complete)
      registry.json                # USER-SUPPLIED
      registry-refs.json           # USER-SUPPLIED
      registry-dd.json             # USER-SUPPLIED
    fixtures/
      ancestry/                    # JSON fixture files for ancestry DB tests
  pds/registrysweepers/ancestry/
    builders.py                    # becomes a re-export shim of tests/support/builders.py
    db/                            # (NEW) ancestry DB tests, §3
      __init__.py
      conftest.py
      test_*.py
      __snapshots__/
  pds/registrysweepers/test_driver_smoke.py   # (NEW) §3.3
```

Add `syrupy` and `testcontainers[opensearch]` to the `dev` extras in `setup.cfg`. Pin each to the latest version compatible with the existing `pytest~=7.4` and Python 3.12/3.13, and verify that by installing. If either requires a newer pytest, stop and report rather than bumping pytest silently.

### 2.1 Endpoint discovery and container lifecycle (`opensearch_server.py`, root `conftest.py`)

- `OPENSEARCH_IMAGE = "opensearchproject/opensearch:2.19.<patch>"` with the TODO from D3. Use the latest 2.19 patch tag and verify it exists.
- Resolve the endpoint in this order:
  1. `TEST_OPENSEARCH_URL` if set → use it, don't start a container (the devcontainer path, and any developer with their own instance).
  2. Otherwise, if Docker is reachable → start a testcontainers `OpenSearchContainer` from `OPENSEARCH_IMAGE` with security disabled. Check the module's constructor arguments in the installed version; don't assume them.
  3. Otherwise → DB tests are **skipped** with the reason "no OpenSearch available: set TEST_OPENSEARCH_URL or start Docker", **unless** `CI` is set, in which case they **fail**.
- **xdist is on by default** (`setup.cfg` → `[tool:pytest] addopts = -n auto`). The container must start **once per test session, not once per worker**:
  - Start it in `pytest_configure` **only on the controller** (`not hasattr(config, "workerinput")`) and only if DB tests were collected or could be (simplest: start lazily, guarded by the `-m` expression, or always start unless `-m "not db"`; pick the simplest one that works and document it).
  - Export the resolved URL as `TEST_OPENSEARCH_URL` in `os.environ` before workers spawn, so workers take path 1. Verify that workers inherit it; if they don't, pass it through `config.workerinput` via `pytest_configure_node`.
  - Stop the container in `pytest_unconfigure` on the controller.
  - Wait for readiness: cluster health is at least `yellow`, with a timeout of about 120s and a clear error on timeout.
- Session-scoped fixture `opensearch_url` returns the URL (or skips/fails per rule 3).
- Session-scoped fixture `opensearch_client` returns `opensearchpy.OpenSearch` for that URL.

### 2.2 Markers and options (root `conftest.py`)

- Register a `db` marker. **Apply it automatically** in `pytest_collection_modifyitems` to any test that uses the `registry_db` fixture (§2.3), so authors never have to remember it.
- `pytest -m "not db"` must work with no Docker and no OpenSearch, and must not start a container.
- Snapshot updating uses syrupy's built-in `--snapshot-update`. Don't add a custom flag; document syrupy's.

### 2.3 Per-test index sets (`indices.py`)

- Load `tests/resources/mappings/{registry,registry-refs,registry-dd}.json`. Accept either a full create-index body (`{"settings":…, "mappings":…}`) or a bare `{"properties":…}` / `{"mappings":…}`, and normalise. If any file is missing, raise an error naming all missing files and pointing at `tests/resources/mappings/README.md` (skip or fail per §2.1 rule 3).
- Write `tests/resources/mappings/README.md`: list the expected filenames and accepted formats, and leave a placeholder for the user to record the source repo/version. Don't guess the source.
- Function-scoped fixture **`registry_db`**:
  - Generates `node_id = "t" + uuid4().hex[:10]` (lowercase, valid as an index prefix).
  - `monkeypatch.setenv`: `MULTITENANCY_NODE_ID=node_id`, `PROV_ENDPOINT=<url>`, `PROV_CREDENTIALS='{"admin":"admin"}'` (a dummy, required because without it `get_opensearch_client_from_environment` falls back to the AOSS/SSM path), `DEV_MODE=1`.
  - Creates `<node>-registry-structured`, `<node>-registry-structured-refs`, `<node>-registry-structured-dd` from the mappings, **and** `<node>-registry-structured-deferred-updates` with no mappings (or minimal ones). `write_updated_docs(..., defer_on_missing_document=True)` raises if that index doesn't exist; see `src/pds/registrysweepers/utils/db/__init__.py`.
  - Use `number_of_replicas: 0` (single node) unless the supplied settings say otherwise; override it if they do, and comment why.
  - Yields a small handle object, `RegistryDb(client, node_id, index_names: dict[str, str], load(...), refresh(), dump(...))`, that wraps §2.5 and §2.7.
  - Teardown: `client.indices.delete(f"{node_id}-*")`. It must run even if the test fails.
- Isolation relies on the per-test `node_id` prefix, which makes tests safe under xdist. Never touch indices outside the test's own prefix.

### 2.4 Canonical document form (`documents.py`)

```python
@dataclass(frozen=True)
class SeedDoc:
    index: Literal["registry", "registry-refs", "registry-dd"]   # logical index type, not the physical name
    id: str
    source: dict
```

Raw-dict and JSON inputs are accepted in either of these shapes and converted to `SeedDoc`:
- `{"index": "...", "_id": "...", "_source": {...}}`, or
- `{"_id": "...", "_source": {...}}` plus an `index=` argument to the loader. The second shape is what the existing builders already emit, so keep it valid.

JSON fixture files hold a list of the first shape. Reject unknown top-level keys with a clear error.

### 2.5 The single loader (`loader.py`)

- `load(db, docs: Iterable[SeedDoc | dict], *, index=None, validate=True)` → bulk-index with explicit `_id`s into the test's physical indices, then **refresh** the affected indices. Raise on any bulk-item error, listing the failing IDs and reasons.
- `load_json(db, path)` → reads a fixture file (relative to `tests/resources/fixtures/`) and calls `load`.
- **Mapping validation** (on by default; `validate=False` turns it off for a deliberate "unknown field" test):
  - Flatten each `_source` into field paths, expanding dotted keys the way OpenSearch does (`ops:Registry_Sweepers.ops:ancestry_software_version` → nested path).
  - Every path must exist in the mapping for that index type, **except** fields the sweeper itself adds at runtime through `ensure_index_mapping`. Keep an explicit allow-list in `loader.py`, built from the constants (`ANCESTRY_REFS_METADATA_KEY`, `SWEEPERS_ANCESTRY_VERSION_METADATA_KEY`, etc.), not from string literals.
  - On failure, raise an error listing the unknown paths per document, with a hint that builders or fixtures may be using stale field names.
  - This exists because the current builders emit the stale field name `ops:Sweepers/ops:ancestry_version` while the code uses `ops:Registry_Sweepers.ops:ancestry_software_version`. Validation is what stops that from happening again.

### 2.6 Builders (`tests/support/builders.py`)

- Move `ProductDocumentBuilder`, `CollectionRefsBuilder`, `build_bundle`, `build_collection` and `build_product` from `tests/pds/registrysweepers/ancestry/builders.py` into `tests/support/builders.py`.
- **Fix field names** to match the current code: derive every key from the source constants (e.g. `SWEEPERS_ANCESTRY_VERSION_METADATA_KEY`), never from hand-typed strings. Check every emitted key against the user-supplied `registry.json` / `registry-refs.json` once they're available.
- Builders can keep returning `{"_id", "_source"}`. Add `.to_seed(index)` / module-level helpers returning `SeedDoc`, so `db.load(...)` accepts builder output directly.
- Leave `tests/pds/registrysweepers/ancestry/builders.py` as a re-export shim so existing imports keep working.
- **Run the existing ancestry mock-based tests after the move and the field fixes.** If any assert on the old field names, update them to the constants and note each change in the PR description. Don't delete or weaken existing assertions.

### 2.7 Snapshots (`snapshots.py`)

- `db.dump(index="registry", *, fields=None)` → returns every document of that logical index as a list sorted by `_id`, each `{"_id", "_source"}`. Optionally project to `fields` to keep snapshots focused. Refresh before reading. Use `search_after` or scroll, sized for test data; a simple `size=10000` with an assertion that `total < 10000` is acceptable.
- Normalisation hook: drop or replace volatile values (timestamps, harvest dates, anything time- or run-dependent). Start with an explicit list and add to it only when a real snapshot proves something volatile. Freeze time with the existing `freezegun` dependency where that's simpler.
- Use syrupy with its JSON extension (`syrupy.extensions.json.JSONSnapshotExtension`), so snapshots are readable, diffable `.json` files under the test directory's `__snapshots__/`.
- Convention: each scenario test gets targeted assertions for the behaviour it's about, plus one `assert db.dump(...) == snapshot` for whole-state regression.

---

## 3. Phase 2: Ancestry DB tests

Location: `tests/pds/registrysweepers/ancestry/db/`. Each test follows **arrange** (load seed docs) → **act** (`ancestry.main.run(client=db.client)`; it reads index names through `MULTITENANCY_NODE_ID`) → **refresh** → **assert** (targeted assertions plus a snapshot).

### 3.1 Scenarios (minimum set)

Mix builder-based and JSON-fixture-based arrangement across these, so **both styles are exercised**: at least two tests each.

1. **Basic hierarchy:** one bundle → one collection (referenced by LIDVID) → N non-aggregate products via refs. Assert each product gets the right parent collection and bundle ancestry, and that the ancestry version key is set to `SWEEPERS_ANCESTRY_VERSION`.
2. **Bundle references a collection by LID:** resolves to the right collection version(s).
3. **Multiple collection versions:** products appear in refs of more than one collection version, and ancestry includes all of them.
4. **Refs pagination and batching:** a collection whose members span multiple refs docs (`batch_id` 1..k). Set `ANCESTRY_NONAGGREGATE_QUERY_PAGE_SIZE` (via `monkeypatch`) small enough that paging actually happens.
5. **Idempotency:** run twice; the second run makes no changes (snapshot after run 1 == snapshot after run 2). Docs already at the current ancestry version are skipped.
6. **Deferred updates and orphans:** refs name a product that isn't in `registry` yet. Run 1 writes to `-deferred-updates`; then load the product and run again; the deferred content gets merged. Assert the doc is no longer orphaned.
7. **Empty database:** run completes without error and writes nothing.

If a scenario reveals a real bug, don't fix production code in this work. Mark that test `xfail(strict=True, reason="<bug description>")` and list it in the PR description.

### 3.2 Ported coverage

Look at `ancestry/integration/` and `ancestry/scenarios/`. Where an existing mock-based test checks behaviour that the real database would check better, add a DB counterpart. **Keep the mock-based test.** Don't delete existing tests.

### 3.3 Driver smoke test (`tests/pds/registrysweepers/test_driver_smoke.py`)

- Uses `registry_db` and loads scenario 1's data.
- Runs the production entry point as a **subprocess**: `[sys.executable, "docker/sweepers_driver.py"]`, with the environment from `registry_db` (`PROV_ENDPOINT`, `PROV_CREDENTIALS`, `DEV_MODE`, `MULTITENANCY_NODE_ID`) and a timeout.
- First try the **default sweeper set** (no `--only`), since that's what production runs. If provenance or reindexer need fixture data beyond the scope of this work, fall back to `--only ancestry`, add `# TODO: extend smoke test to default sweeper set once provenance/reindexer DB fixtures exist`, and say so in the PR description.
- Assert exit code 0 and spot-check one or two ancestry fields on one product. No snapshot.

---

## 4. Phase 3: CI and tox

### 4.1 tox (`tox.ini`)

- Running bare `tox` in CI passes posargs to every env, and `lint` uses `{posargs:--all}`, so don't run DB filtering through bare-`tox` posargs.
- Add `[testenv:db]`: `basepython = python3.13`, `extras = dev`, `commands = pytest -m db --cov=src --cov-report=xml {posargs}`, and `passenv = TEST_OPENSEARCH_URL, CI, DOCKER_HOST, TESTCONTAINERS_*`.
- Make `py312`/`py313` run `pytest -m "not db" …`, so the unit envs never need Docker.
- Leave `envlist` as it is (no `db`), so a developer's plain `tox` doesn't need Docker. Document `tox -e db` in the README.

### 4.2 GitHub Actions (`.github/workflows/branch_cicd.yaml`)

- Keep the existing job for unit tests, docs and lint; it now runs `tox` (which no longer includes DB tests, per 4.1).
- Add a second job, `db-testing`, that runs alongside it: same checkout/Python 3.13/cache steps and the same `if: github.actor != 'pdsen-ci'` guard, then `pip install --editable '.[dev]'` and `tox -e db`. Docker is already on `ubuntu-latest`, so testcontainers works with no `services:` block. Set `timeout-minutes` (for example, 20).
- Upload the coverage XML from the DB job as an artifact. Don't wire it into Sonar unless the existing setup already takes coverage from CI; check `sonar-project.properties` and report.
- Don't change `stable-cicd.yaml` or `unstable-cicd.yaml` (out of scope; see §6).

---

## 5. Phase 4: Docs and verification

1. Add a "Testing" section to `README.md` (or `docs/`, wherever the project keeps developer docs) covering: `pytest -m "not db"` (no Docker needed), `pytest -m db` / `tox -e db`, `TEST_OPENSEARCH_URL`, the devcontainer sidecar, `--snapshot-update`, how to write a test with builders vs JSON, and the mapping files.
2. **Verification checklist.** All must pass before you report done; paste the outputs in the PR description.
   - [ ] In the devcontainer: `pytest -m db` passes against the sidecar.
   - [ ] In the devcontainer: `pytest -m "not db"` passes. Existing tests behave as before.
   - [ ] `pytest -m db -n 4` passes, so the xdist isolation works. Run it twice in a row to catch leftover indices.
   - [ ] After a run, `curl $TEST_OPENSEARCH_URL/_cat/indices` shows no `t*-registry-structured*` indices.
   - [ ] With the mapping files temporarily renamed: locally the DB tests skip with the clear message; with `CI=1` they fail with it.
   - [ ] Validation check: loading a doc with `ops:Sweepers/ops:ancestry_version` fails validation with the stale-field hint.
   - [ ] Snapshot check: changing a seed value makes the snapshot test fail with a readable diff; `--snapshot-update` regenerates it.
   - [ ] `tox -e lint` passes (pre-commit: flake8, mypy etc. apply to `tests/support/` too).
   - [ ] Push the branch and confirm both CI jobs go green.
3. You can't test the testcontainers path (no Docker in the devcontainer). Say so explicitly and rely on the CI run for that path.

---

## 6. Out of scope, and actions for the user

**Out of scope:** DB tests for provenance, repairkit, reindexer and legacy-sync; fetching mappings dynamically; AOSS; legacy flat-schema mode; any production code changes (bugs found → `xfail` and report); release workflows (`stable-cicd.yaml` / `unstable-cicd.yaml`).

**The user needs to:**
- Supply the three mapping files under `tests/resources/mappings/` and fill in their source in the README there. **Phase 2 can't pass until these exist.** If they're absent when you reach Phase 2, finish Phase 1 (which can be built and unit-tested without them), then stop and ask.
- Rebuild the devcontainer after Phase 0.
- Confirm the OpenSearch version (the D3 TODO).
- Make `db-testing` a **required status check** in branch protection (a repository setting; can't be done from code).
- Decide about fork PRs: `branch_cicd.yaml` triggers on **push to non-`main` branches** and uses `secrets.ADMIN_GITHUB_TOKEN`, so PRs from forks won't run either job. Adding a `pull_request` trigger is a separate decision with security implications; flag it, don't do it.

## 7. Delivery

- Work on a new branch, e.g. `test/opensearch-harness`, with one commit per phase.
- Open a PR whose description covers: a summary per phase, the verification checklist output, every changed existing test (from §2.6), any `xfail`s with bug descriptions, and the user actions from §6.
