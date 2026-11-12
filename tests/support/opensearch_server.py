import logging
import os
import time
from typing import Optional

from opensearchpy import OpenSearch

log = logging.getLogger(__name__)

# TODO: confirm this matches the production managed-OpenSearch version.
# Keep in sync with the opensearch service image in .devcontainer/claude-code/compose.yaml.
OPENSEARCH_IMAGE = "opensearchproject/opensearch:2.19.4"

_container = None


def resolve_url() -> Optional[str]:
    return os.environ.get("TEST_OPENSEARCH_URL") or None


def docker_available() -> bool:
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


def start_container() -> str:
    global _container
    from testcontainers.community.opensearch import OpenSearchContainer
    _container = OpenSearchContainer(image=OPENSEARCH_IMAGE, security_enabled=False)
    _container.start()
    config = _container.get_config()
    return f"http://{config['host']}:{config['port']}"


def stop_container() -> None:
    global _container
    if _container is not None:
        try:
            _container.stop()
        except Exception:
            pass
        _container = None


def wait_for_ready(url: str, timeout: int = 120) -> None:
    client = OpenSearch(hosts=[url])
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            health = client.cluster.health(wait_for_status="yellow", request_timeout=5)
            if health.get("status") in ("yellow", "green"):
                return
        except Exception:
            pass
        time.sleep(2)
    raise TimeoutError(f"OpenSearch at {url} did not become ready within {timeout}s")
