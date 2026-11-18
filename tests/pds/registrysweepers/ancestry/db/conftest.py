import pytest
from syrupy.extensions.json import JSONSnapshotExtension

from tests.support.snapshots import snapshot_matcher


@pytest.fixture
def snapshot(snapshot):
    return snapshot.with_defaults(extension_class=JSONSnapshotExtension, matcher=snapshot_matcher)
