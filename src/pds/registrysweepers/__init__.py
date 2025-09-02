# -*- coding: utf-8 -*-
"""PDS Registry Sweepers."""
import importlib.resources

import pds.registrysweepers.provenance

__version__ = VERSION = importlib.resources.files(__name__).joinpath("VERSION.txt").read_text().strip()
