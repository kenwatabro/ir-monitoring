from __future__ import annotations

"""Downloader for ESG / Governance metrics.

Currently supports:
- EDINET "非財務情報" CSV (quarterly)
- Financial Modeling Prep ESG Score API (free tier)

Functions return list of dicts compatible with `esg_scores` table.
"""

import logging
import os
from datetime import date
from typing import List, Dict

import requests

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

FMP_BASE = "https://financialmodelingprep.com/api/v4/esg-environmental-social-governance-data"


def _fetch_fmp(ticker: str) -> List[Dict[str, object]]:
    api_key = os.getenv("FMP_API_KEY", "demo")
    url = f"{FMP_BASE}?symbol={ticker}&apikey={api_key}"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            return []
        latest = data[0]
        period_end = date.fromisoformat(latest["date"])
        results: List[Dict[str, object]] = []
        for metric in ["environmentScore", "socialScore", "governanceScore"]:
            val = latest.get(metric)
            if val is None:
                continue
            results.append(
                {
                    "code_jpx": None,
                    "provider": "FMP",
                    "metric": metric[0].upper(),  # E/S/G
                    "period_end": period_end,
                    "score": float(val),
                    "src": "FMP",
                }
            )
        return results
    except Exception as exc:  # noqa: BLE001
        logger.exception("FMP ESG fetch error for %s: %s", ticker, exc)
        return []


# TODO: EDINET 非財務 CSV parsing once sample file prepared


def download(ticker: str) -> List[Dict[str, object]]:  # noqa: D401
    """Fetch ESG scores for *ticker* (e.g. 7203.T)."""
    return _fetch_fmp(ticker) 