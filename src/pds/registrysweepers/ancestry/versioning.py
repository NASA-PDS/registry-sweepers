# Defines constants used for versioning updated documents with the in-use version of sweepers
# SWEEPERS_VERSION must be incremented any time sweepers is changed in a way which requires reprocessing of
# previously-processed data
from pds.registrysweepers.utils.misc import get_sweeper_version_metadata_key

SWEEPERS_ANCESTRY_VERSION = 6
SWEEPERS_ANCESTRY_VERSION_METADATA_KEY = get_sweeper_version_metadata_key("ancestry")
