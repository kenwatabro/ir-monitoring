# ruff: noqa: E402
from __future__ import annotations

"""Aggregate macro downloader using provider plugins.

Configured via MACRO_SERIES env var; each code maps to a provider instance.
"""

import logging
import os
from datetime import date
from typing import Dict, List

from ._base import BaseDownloader
from .fred_provider import FREDSeriesDownloader

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# Registry mapping prefix → factory
_PROVIDER_FACTORIES: Dict[str, callable[[str], BaseDownloader]] = {
    "fred": lambda code: FREDSeriesDownloader(code),
}


def _build_downloaders(series_codes: List[str]) -> List[BaseDownloader]:
    downloaders: List[BaseDownloader] = []
    for code in series_codes:
        code = code.strip()
        if ":" in code:  # explicit prefix e.g. fred:T10Y2Y
            prefix, sid = code.split(":", 1)
        else:  # default to fred
            prefix, sid = "fred", code
        factory = _PROVIDER_FACTORIES.get(prefix)
        if not factory:
            logger.warning("Unknown provider prefix: %s", prefix)
            continue
        downloaders.append(factory(sid))
    return downloaders


class MacroAggregator(BaseDownloader):
    """Download macro series defined by env var for given date."""

    name = "macro-aggregator"

    def __init__(self):
        series_env = os.getenv("MACRO_SERIES", "T10Y2Y,PMI_US")
        self.series_list = [s.strip() for s in series_env.split(",") if s.strip()]
        self.providers = _build_downloaders(self.series_list)

    def download(self, target_date: date) -> List[Dict[str, object]]:  # noqa: D401
        results: List[Dict[str, object]] = []
        for downloader in self.providers:
            results.extend(downloader.download(target_date))
        return results


# Convenience functional API (backward-compat)
_aggregator = MacroAggregator()

def download(target_date: date) -> List[Dict[str, object]]:  # noqa: D401
    return _aggregator.download(target_date) 