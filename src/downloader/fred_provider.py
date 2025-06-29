from __future__ import annotations

import logging
import os
from datetime import date
from typing import Dict, List

from ._base import BaseDownloader

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

try:
    from fredapi import Fred  # type: ignore
except ImportError:  # pragma: no cover
    Fred = None  # type: ignore


class FREDSeriesDownloader(BaseDownloader):
    """Download single FRED series for a given date."""

    def __init__(self, series_id: str):
        self.series_id = series_id
        self.name = f"fred:{series_id}"
        api_key = os.getenv("FRED_API_KEY", "")
        self._fred = Fred(api_key=api_key) if Fred else None

    def download(self, target_date: date) -> List[Dict[str, object]]:  # noqa: D401
        if self._fred is None:
            logger.warning("fredapi not installed; skipping %s", self.series_id)
            return []
        try:
            value = self._fred.get_series(self.series_id, observation_start=target_date, observation_end=target_date)
            if value.empty:
                return []
            return [
                {
                    "series_id": self.series_id,
                    "ts_date": target_date,
                    "value": float(value.iloc[0]),
                    "src": "FRED",
                }
            ]
        except Exception as exc:  # noqa: BLE001
            logger.exception("FRED fetch error for %s: %s", self.series_id, exc)
            return [] 