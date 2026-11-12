# Claude Code Devcontainer

This directory contains a devcontainer configuration for running Claude Code in an isolated Docker environment.

## What are devcontainers?

[Devcontainers](https://containers.dev/) are Docker-based development environments which are self-contained, and run a headless IDE. They provide:
- Secure, isolated workspace (i.e. Claude Code can only access project files and other files explicitly mounted to the container)
- One-click, consistent development environment setups across team members or for fast onboarding

## Usage with JetBrains IDEs

1. Open this project in PyCharm
2. Open devcontainer.json in the IDE
3. Click the gutter icon to build/run a devcontainer, and follow the wizard.

## Usage with VSCode (AI-generated, untested)

1. Install the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers) in VS Code
2. Open this project in VS Code
3. Click "Reopen in Container" when prompted (or use Command Palette → "Dev Containers: Reopen in Container")

## Usage

Once the container is running, open a terminal in VS Code and run:

```bash
claude
```

Claude Code will have access to your project files but remain isolated from your host system.

## OpenSearch test sidecar

The devcontainer is defined with docker compose (`compose.yaml`) and runs two services:

- `devcontainer`: the Claude Code / development environment (built from `Dockerfile`)
- `opensearch`: a single-node OpenSearch instance (security plugin disabled) for the database-backed tests

The devcontainer waits for OpenSearch to report healthy before starting, and the firewall (`init-firewall.sh`)
explicitly allows traffic to it. Inside the devcontainer it is reachable at `http://opensearch:9200`, and the
`TEST_OPENSEARCH_URL` environment variable is set to that address. When `TEST_OPENSEARCH_URL` is set, the test suite
uses that instance instead of starting its own container via testcontainers (which isn't possible here, as the
devcontainer has no Docker access).

```bash
curl http://opensearch:9200   # check the sidecar is up
pytest -m db                  # run the database-backed tests against it
pytest -m "not db"            # run everything else
```

Notes:

- The OpenSearch image version is pinned in `compose.yaml`; keep it in sync with `OPENSEARCH_IMAGE` in
  `tests/support/opensearch_server.py`.
- OpenSearch uses roughly 1 GB of memory (512 MB heap). If you use Docker Desktop, make sure its memory
  allowance covers this in addition to the devcontainer.
- Python 3.13 (default) and 3.12 are both installed, so all `tox` environments can run.
- Test indices are created per test with a random prefix and deleted afterwards. To inspect the sidecar's state:
  `curl http://opensearch:9200/_cat/indices?v`.
- Data in the sidecar is not persisted between container rebuilds.

## Learn More

Full documentation: https://docs.claude.com/en/docs/claude-code/devcontainer
