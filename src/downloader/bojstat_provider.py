"""BOJ-STAT (日銀統計) series downloader.

BOJ Time-Series Data Search provides CSV download endpoints where the query
string includes *Statistics code* & *Series code* combinations. The simplest
way to fetch one series for a given date is to call the CSV endpoint and
filter the row.

This implementation is intentionally minimal and supports URLs of the form:
https://www.stat-search.boj.or.jp/ssi/cgi-bin/famecgi2csv?key_series=JCB.JP90+&from=2020-01-01&to=2020-01-01

However, instead of exposing the full URL, we expect *series_id* to be the
`key_series` parameter (e.g. "FJKM901@BBM2110N") used by BoJ site. Users can
inspect the website to obtain the identifier.

For detailed docs see:
https://www.stat-search.boj.or.jp/guide/index.html
"""

from __future__ import annotations

import csv
import logging
import os
from datetime import date
from typing import Dict, List

import requests

from ._base import BaseDownloader

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

BOJ_CSV_BASE = "https://www.stat-search.boj.or.jp/ssi/cgi-bin/famecgi2csv"


class BoJSeriesDownloader(BaseDownloader):
    """Download single BOJ-STAT series for a given date."""

    def __init__(self, series_id: str):
        self.series_id = series_id  # key_series parameter value
        self.name = f"boj:{series_id}"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def download(self, target_date: date) -> List[Dict[str, object]]:  # noqa: D401
        params = {
            "key_series": self.series_id,
            "from": target_date.isoformat(),
            "to": target_date.isoformat(),
            "csvfmt": "csv",
        }
        try:
            resp = requests.get(BOJ_CSV_BASE, params=params, timeout=60)
            resp.raise_for_status()
            reader = csv.reader(resp.text.splitlines())
            rows = list(reader)
            if len(rows) < 2:
                return []  # no data
            header, *data_rows = rows
            results: List[Dict[str, object]] = []
            # Expect date in first column, value in second
            for r in data_rows:
                if not r:
                    continue
                ts_str = r[0]
                if ts_str != target_date.isoformat():
                    continue
                try:
                    value = float(r[1])
                except (IndexError, ValueError):
                    continue
                results.append(
                    {
                        "series_id": self.series_id,
                        "ts_date": target_date,
                        "value": value,
                        "src": "BOJ-STAT",
                    }
                )
            return results
        except Exception as exc:  # noqa: BLE001
            logger.exception("BOJ fetch error for %s: %s", self.series_id, exc)
            return []
