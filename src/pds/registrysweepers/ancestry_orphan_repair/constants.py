"""
Field names and limits used by the ancestry orphan repair.

Every registry field name the repair depends on is defined (or imported) here.  The ancestry, stamp and superseded keys
are imported from the ancestry and provenance sweepers, so they always match what those sweepers read and write; on this
branch they are the structured ``ops:Registry_Sweepers.*`` keys:

- ``ops:Registry_Sweepers.ops:ancestor_refs``
- ``ops:Registry_Sweepers.ops:ancestry_software_version``
- ``ops:Registry_Sweepers.ops:superseded_by``
"""
from pds.registrysweepers.ancestry.constants import ANCESTRY_REFS_METADATA_KEY
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION
from pds.registrysweepers.ancestry.versioning import SWEEPERS_ANCESTRY_VERSION_METADATA_KEY
from pds.registrysweepers.provenance.constants import METADATA_SUCCESSOR_KEY

# written by the ancestry sweeper; a product lacking the former, or carrying a stamp below the latter, is an orphan
ANCESTOR_REFS_KEY = ANCESTRY_REFS_METADATA_KEY
ANCESTRY_VERSION_KEY = SWEEPERS_ANCESTRY_VERSION_METADATA_KEY
CURRENT_ANCESTRY_VERSION = SWEEPERS_ANCESTRY_VERSION

# set by the provenance sweeper on versions which have been superseded
SUPERSEDED_BY_KEY = METADATA_SUCCESSOR_KEY

# registry index
LIDVID_FIELD = "lidvid"
PRODUCT_CLASS_FIELD = "product_class"
COLLECTION_PRODUCT_CLASS = "Product_Collection"
# non-aggregate products are those which are neither of these (same rule as ancestry.queries.ProductClass.NON_AGGREGATE)
AGGREGATE_PRODUCT_CLASSES = ["Product_Bundle", "Product_Collection"]

# registry-refs index (one document per batch of a collection's member products)
REFS_COLLECTION_FIELD = "collection_lidvid"
REFS_PRODUCT_FIELD = "product_lidvid"
REFS_BATCH_FIELD = "batch_id"

# Updates for products which did not yet exist when the ancestry sweeper wrote them are held in
# "<resolved registry index name><suffix>", keyed by product LIDVID, until the ancestry sweeper merges them onto the
# product document (see ancestry.main.run and utils.db.write_updated_docs)
DEFERRED_UPDATES_INDEX_SUFFIX = "-deferred-updates"

# Product LIDVIDs checked for a pending deferral entry per mget (the ancestry sweeper's own merge uses 1,000)
DEFERRAL_LOOKUP_CHUNK_SIZE = 1_000

# OpenSearch Serverless caps a page at 10,000 hits, and a request with a larger size returns garbage rather than failing
MAX_PAGE_SIZE = 10_000

# Products examined (and terms looked up in registry-refs) per round trip.  Well below the default index.max_terms_count
# of 65,536 and the page cap above.
DEFAULT_BATCH_SIZE = 2_000

# Collection LIDVIDs checked for existence per query
COLLECTION_LOOKUP_CHUNK_SIZE = 1_000
