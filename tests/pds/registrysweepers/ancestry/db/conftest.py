import pytest
from syrupy.extensions.json import JSONSnapshotExtension


@pytest.fixture
def snapshot(snapshot):
    return snapshot.use_extension(JSONSnapshotExtension)
