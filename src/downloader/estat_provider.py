"""e-Stat (政府統計) series downloader.

This provider fetches a single series identified by *statsDataId* and optional
additional key parameters (e.g. category codes) for a given target date.

Design notes
------------
* API docs: https://www.e-stat.go.jp/api/api-info/e-stat-manual3-0
* We rely on the basic `getStatsData` endpoint and expect JSON response.
* Authentication is via the `ESTAT_APP_ID` environment variable.
* Currently we support **daily** date granularity by simply matching the
  `time` field against `YYYY-MM-DD`. For monthly/quarterly series, users
  should pass the first day of the period; non-matching rows will be skipped.

Limitations
-----------
* This is a minimal implementation intended to cover typical macro indicators
  such as CPI (statsDataId="0000000203" etc.). Advanced filtering,
  pagination, and retry logic are intentionally omitted for brevity.
"""

from __future__ import annotations

import logging
import os
from datetime import date
from typing import Dict, List

import requests

from ._base import BaseDownloader

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

ESTAT_BASE = "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData"


class EStatSeriesDownloader(BaseDownloader):
    """Download a single e-Stat series (statsDataId) for a given date."""

    def __init__(self, series_id: str):
        """Parameters
        ----------
        series_id
            e-Stat `statsDataId` (e.g. "0003412316"). Additional query
            parameters can be appended after a semicolon, e.g.
            ``"0003412316;cdCat01=0001"``.
        """
        self.raw_series_id = series_id
        # split optional query parameters
        if ";" in series_id:
            sid, *kv_pairs = series_id.split(";")
            self.stats_data_id = sid
            self.extra_params = dict(p.split("=", 1) for p in kv_pairs if "=" in p)
        else:
            self.stats_data_id = series_id
            self.extra_params: Dict[str, str] = {}
        self.name = f"estat:{self.stats_data_id}"

        app_id = os.getenv("ESTAT_APP_ID")
        if not app_id:
            logger.warning("ESTAT_APP_ID is not set; %s will be skipped", self.name)
        self._app_id = app_id

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def download(self, target_date: date) -> List[Dict[str, object]]:  # noqa: D401
        if not self._app_id:
            return []

        params = {
            "appId": self._app_id,
            "statsDataId": self.stats_data_id,
            "metaGetFlg": "N",  # reduce payload
            "cntGetFlg": "N",
            "sectionHeaderFlg": "2",  # key names instead of codes
            "annotationGetFlg": "N",
        }
        params.update(self.extra_params)  # user-provided filters

        try:
            resp = requests.get(ESTAT_BASE, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            # Basic validation
            if data.get("GET_STATS_DATA") is None:
                logger.warning("Unexpected e-Stat response format for %s", self.name)
                return []
            stat_data = data["GET_STATS_DATA"].get("STAT_DATA", {})
            values = stat_data.get("VALUE", [])
            if not values:
                return []

            results: List[Dict[str, object]] = []
            for row in values:
                # e-Stat returns time in YYYYMM or YYYY-MM-DD; normalise
                time_str = str(row.get("time"))
                if len(time_str) == 6:
                    d_candidate = date.fromisoformat(
                        f"{time_str[:4]}-{time_str[4:6]}-01"
                    )
                else:
                    try:
                        d_candidate = date.fromisoformat(time_str)
                    except ValueError:
                        continue
                if d_candidate != target_date:
                    continue

                # Build simple result dict
                try:
                    value = float(row.get("value"))
                except (TypeError, ValueError):
                    continue

                results.append(
                    {
                        "series_id": self.raw_series_id,
                        "ts_date": d_candidate,
                        "value": value,
                        "src": "eStat",
                    }
                )
            return results
        except Exception as exc:  # noqa: BLE001
            logger.exception("e-Stat fetch error for %s: %s", self.name, exc)
            return []
