"""Downloader for J-Quants 無料プラン『信用取引週末残高』(weekly margin interest).

Notes
-----
* 無料プランでは **12 週遅延** データのみ取得可能。
* 認証には環境変数のいずれかを用いる。

    1. JQUANTS_REFRESH_TOKEN (推奨)
    2. JQUANTS_MAIL + JQUANTS_PASSWORD

* インポート負荷を避けるため、`jquantsapi` が無い環境ではスキップします。
"""

from __future__ import annotations

import logging
import os
from datetime import date
from typing import Dict, List

from ._base import BaseDownloader

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

try:
    import jquantsapi  # type: ignore
except ImportError:  # pragma: no cover
    jquantsapi = None  # type: ignore


class JQuantsCreditDownloader(BaseDownloader):
    """Download weekly margin outstanding (信用残) for specified `target_date`."""

    name = "jquants-credit"

    def __init__(self, codes: List[str] | None = None):
        self.codes = codes  # optional filter list

        refresh_token = os.getenv("JQUANTS_REFRESH_TOKEN")
        mail = os.getenv("JQUANTS_MAIL")
        passwd = os.getenv("JQUANTS_PASSWORD")

        if jquantsapi is None:
            logger.warning("jquantsapi not installed; credit data will be skipped")
            self._client = None
        else:
            try:
                self._client = jquantsapi.Client(
                    refresh_token=refresh_token, mail_address=mail, password=passwd
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception("Failed to init J-Quants client: %s", exc)
                self._client = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def download(self, target_date: date) -> List[Dict[str, object]]:  # noqa: D401
        if self._client is None:
            return []

        try:
            ymd = target_date.strftime("%Y%m%d")
            try:
                if hasattr(self._client, "get_markets_weekly_margin_interest"):
                    df = self._client.get_markets_weekly_margin_interest(
                        date_yyyymmdd=ymd
                    )
                else:
                    raise AttributeError
            except Exception:  # any error → fallback to range API (free plan)
                df = self._client.get_weekly_margin_range(start_dt=ymd, end_dt=ymd)
            if df is None or df.empty:  # type: ignore[attr-defined]
                return []
            results: List[Dict[str, object]] = []
            for _, row in df.iterrows():  # type: ignore[attr-defined]
                code = str(row["Code"]).zfill(4)
                if self.codes and code not in self.codes:
                    continue

                ts_date = target_date  # response Date is same as requested
                try:
                    s_vol = float(row["ShortMarginTradeVolume"])
                    l_vol = float(row["LongMarginTradeVolume"])
                except (KeyError, ValueError):
                    continue

                results.extend(
                    [
                        {
                            "code_jpx": code,
                            "metric": "short_margin_balance",
                            "ts_date": ts_date,
                            "value": s_vol,
                            "src": "J-Quants",
                        },
                        {
                            "code_jpx": code,
                            "metric": "long_margin_balance",
                            "ts_date": ts_date,
                            "value": l_vol,
                            "src": "J-Quants",
                        },
                    ]
                )
            return results
        except Exception as exc:  # noqa: BLE001
            logger.exception("J-Quants credit fetch error: %s", exc)
            return []
