"""Smoke test: driver.run() with --only ancestry completes without error."""
import sys

import pytest

import pds.registrysweepers.driver as driver

from tests.support.builders import (
    build_bundle,
    build_collection,
    build_product,
    CollectionRefsBuilder,
)


def test_driver_ancestry_smoke(registry_db, monkeypatch):
    """driver.run(--only ancestry) completes without raising on a minimal registry."""
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

    monkeypatch.setattr(sys, "argv", ["registry-sweepers", "--only", "ancestry"])

    driver.run()
    registry_db.refresh()

    docs = registry_db.dump("registry")
    assert len(docs) == 3
