"""Standalone MFUP/2 server — config from environment.

    uvicorn mfup_fastapi.app:app

This module is the ONLY place that builds an app at import time
(``app = create_app()`` for uvicorn's module:attr convention). Library
consumers use MfupEngine/MfupConfig directly and never import this module.
"""

from __future__ import annotations

import logging

from . import create_app

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

app = create_app()
