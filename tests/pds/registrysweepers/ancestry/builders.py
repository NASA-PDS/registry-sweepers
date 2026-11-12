# Re-export shim — builders live in tests/support/builders.py.
# Field name fix: ops_ancestry_version now emits SWEEPERS_ANCESTRY_VERSION_METADATA_KEY
# ("ops:Registry_Sweepers.ops:ancestry_software_version") instead of the stale
# "ops:Sweepers/ops:ancestry_version".
from tests.support.builders import (  # noqa: F401
    build_bundle,
    build_collection,
    build_product,
    CollectionRefsBuilder,
    ProductDocumentBuilder,
)
