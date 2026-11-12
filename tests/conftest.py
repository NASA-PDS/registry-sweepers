import os

import pytest

from tests.support import opensearch_server
from tests.support.indices import registry_db  # noqa: F401 — imported so pytest discovers the fixture


def pytest_configure(config):
    config.addinivalue_line("markers", "db: tests that require a live OpenSearch instance")

    if hasattr(config, "workerinput"):
        # xdist worker: pick up the URL the controller published
        url = config.workerinput.get("TEST_OPENSEARCH_URL")
        if url:
            os.environ["TEST_OPENSEARCH_URL"] = url
        return

    # Controller: skip container startup when the caller explicitly excluded db tests
    mark_expr = getattr(config.option, "markexpr", "")
    if mark_expr == "not db":
        return

    if opensearch_server.resolve_url():
        return  # sidecar or manually configured instance available

    if opensearch_server.docker_available():
        url = opensearch_server.start_container()
        opensearch_server.wait_for_ready(url)
        os.environ["TEST_OPENSEARCH_URL"] = url


@pytest.hookimpl(optionalhook=True)
def pytest_configure_node(node):
    """Pass the OpenSearch URL to xdist workers via their workerinput dict."""
    url = os.environ.get("TEST_OPENSEARCH_URL")
    if url:
        node.workerinput["TEST_OPENSEARCH_URL"] = url


def pytest_unconfigure(config):
    if not hasattr(config, "workerinput"):
        opensearch_server.stop_container()


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "registry_db" in getattr(item, "fixturenames", []):
            item.add_marker(pytest.mark.db)


@pytest.fixture(scope="session")
def opensearch_url():
    url = os.environ.get("TEST_OPENSEARCH_URL")
    if url:
        return url
    if os.environ.get("CI"):
        pytest.fail("no OpenSearch available: set TEST_OPENSEARCH_URL or start Docker")
    pytest.skip("no OpenSearch available: set TEST_OPENSEARCH_URL or start Docker")


@pytest.fixture(scope="session")
def opensearch_client(opensearch_url):
    from opensearchpy import OpenSearch
    return OpenSearch(hosts=[opensearch_url])
