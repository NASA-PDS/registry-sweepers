"""
End-to-end smoke test: run the production entry point (docker/sweepers_driver.py) as a
subprocess against a per-test registry, using the default sweeper set, as production does.

Logic is covered by the in-process DB tests under ancestry/db/; this only proves that the
driver wires everything together and exits cleanly.
"""
import os
import subprocess
import sys
from pathlib import Path

from pds.registrysweepers.ancestry.constants import ANCESTRY_REFS_METADATA_KEY
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY

from tests.support.builders import build_bundle
from tests.support.builders import build_collection
from tests.support.builders import build_product
from tests.support.builders import CollectionRefsBuilder

REPO_ROOT = Path(__file__).resolve().parents[3]
DRIVER = REPO_ROOT / "docker" / "sweepers_driver.py"
TIMEOUT_SECONDS = 300


def test_driver_smoke(registry_db):
    """sweepers_driver.py (default sweeper set) exits 0 and writes ancestry."""
    bundle_lidvid = "urn:nasa:pds:smoke:bundle::1.0"
    collection_lid = "urn:nasa:pds:smoke:collection"
    collection_lidvid = f"{collection_lid}::1.0"
    product_id = f"{collection_lid}:p1::1.0"

    registry_db.load(
        [build_bundle(bundle_lidvid, collection_lid),
         build_collection(collection_lidvid),
         build_product(product_id)],
        index="registry",
    )
    registry_db.load([CollectionRefsBuilder(collection_lidvid).with_product(product_id).to_seed()])

    # registry_db has already set PROV_ENDPOINT, PROV_CREDENTIALS, DEV_MODE and
    # MULTITENANCY_NODE_ID in os.environ via monkeypatch.
    result = subprocess.run(
        [sys.executable, str(DRIVER)],
        cwd=REPO_ROOT,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=TIMEOUT_SECONDS,
    )
    assert result.returncode == 0, (
        f"driver exited {result.returncode}\n--- stdout ---\n{result.stdout}\n--- stderr ---\n{result.stderr}"
    )

    registry_db.refresh()
    docs = {d["_id"]: d["_source"] for d in registry_db.dump("registry")}

    product = docs[product_id]
    assert product[SWEEPERS_ANCESTRY_VERSION_METADATA_KEY] == SWEEPERS_ANCESTRY_VERSION
    assert set(product[ANCESTRY_REFS_METADATA_KEY]) == {collection_lid, collection_lidvid}
