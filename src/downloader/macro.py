# ruff: noqa: E402
from __future__ import annotations

"""Aggregate macro downloader using provider plugins.

Configured via MACRO_SERIES env var; each code maps to a provider instance.
"""

import logging
import os
from datetime import date
from typing import Dict, List
import glob
import yaml

from ._base import BaseDownloader
from .fred_provider import FREDSeriesDownloader
from .estat_provider import EStatSeriesDownloader
from .bojstat_provider import BoJSeriesDownloader

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# Registry mapping prefix → factory
_PROVIDER_FACTORIES: Dict[str, callable[[str], BaseDownloader]] = {
    "fred": lambda code: FREDSeriesDownloader(code),
    "estat": lambda code: EStatSeriesDownloader(code),
    "boj": lambda code: BoJSeriesDownloader(code),
}


def _load_yaml_series(dirpath: str = "config/macro_series") -> List[str]:
    """Load series definitions from YAML files under *dirpath*.

    Each YAML file should follow the schema::

        category: rates          # optional metadata
        prefix: fred            # defaults to "fred" if omitted
        series:
          - DGS10
          - DGS2
          - ...

    Returns a list of fully-qualified series codes such as "fred:DGS10".
    """
    series: List[str] = []
    pattern = os.path.join(dirpath, "*.yml")
    for path in glob.glob(pattern):
        try:
            with open(path, "r", encoding="utf-8") as fp:
                doc = yaml.safe_load(fp) or {}
        except Exception as exc:  # pragma: no cover
            logger.warning("Failed to parse YAML %s: %s", path, exc)
            continue
        prefix = doc.get("prefix", "fred")
        for s in doc.get("series", []):
            s = str(s).strip()
            if not s:
                continue
            series.append(f"{prefix}:{s}")
    return series


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
        # Load series from YAML files
        yaml_series = _load_yaml_series()

        # Check environment override
        env_series_raw = os.getenv("MACRO_SERIES", "")
        env_series = [s.strip() for s in env_series_raw.split(",") if s.strip()]

        # If env variable is set (non-empty), prefer that list exclusively
        # This behavior supports unit-testsや一時検証で「対象系列を限定」したいケース
        if env_series:
            self.series_list = env_series
        else:
            self.series_list = yaml_series

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
