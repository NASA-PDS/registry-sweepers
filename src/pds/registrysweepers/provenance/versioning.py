# Defines constants used for versioning updated documents with the in-use version of sweepers
# SWEEPERS_VERSION must be incremented any time sweepers is changed in a way which requires reprocessing of
# previously-processed data
from pds.registrysweepers.utils.misc import get_sweeper_version_metadata_key

SWEEPERS_PROVENANCE_VERSION = 2
# At some point, provenance version metadata attributes were indexed differently as integer/keyword (should be integer)
# As a hotfix, a new key is being used - eventually it will be necessary to re-index all nodes under corrected mappings,
# as mappings may not be changed - edunn 20250730
SWEEPERS_PROVENANCE_VERSION_METADATA_KEY = get_sweeper_version_metadata_key("provenance_hotfixed")
SWEEPERS_BROKEN_PROVENANCE_VERSION_METADATA_KEY = get_sweeper_version_metadata_key("provenance")
