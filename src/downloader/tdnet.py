from __future__ import annotations

import logging
import os
import pathlib
from datetime import date
from typing import List

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# TODO: Implement actual TDnet API calls when available.

def download(day: date) -> List[pathlib.Path]:
    """Stub for TDnet PDF download. Returns empty list for now."""
    logger.warning("TDnet download not implemented. Returning empty list.")
    return [] 