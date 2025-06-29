# ruff: noqa: E402
from __future__ import annotations

"""Downloader for market & flow related series.

- Daily OHLCV via Stooq or Yahoo Finance
- JPX 信用残高 CSV
- TDnet 見出し (Yanoshin JSON) → 簡易 sentiment スコア

すべて **必須依存なし**で動作するように requests と pandas の optional import
とし、取得失敗時は空リストを返してパイプライン全体を停止させない方針。"""

import csv
import logging
import os
import pathlib
from datetime import date
from typing import Dict, List

import requests

# OOP 統一のための Downloader 基底クラス
from ._base import BaseDownloader

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

DATA_DIR = pathlib.Path(os.getenv("RAW_DIR", "data/raw"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------------------------------
# Stooq OHLCV
# --------------------------------------------------------------------------------------

STOOQ_BASE = "https://stooq.com/q/d/l/"


def _fetch_stooq(code: str, target_date: date) -> List[Dict[str, object]]:
    """Fetch single-day OHLCV for JP equity code (XXXX.JP)."""
    symbol = f"{code}.JP"
    params = {"i": "d", "s": symbol.lower()}
    url = f"{STOOQ_BASE}"
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        reader = csv.DictReader(resp.text.splitlines())
        for row in reader:
            if row["Date"] != target_date.isoformat():
                continue
            return [
                {
                    "code_jpx": code,
                    "metric": "close",
                    "ts_date": target_date,
                    "value": float(row["Close"] or 0),
                    "src": "Stooq",
                },
                {
                    "code_jpx": code,
                    "metric": "volume",
                    "ts_date": target_date,
                    "value": float(row["Volume"] or 0),
                    "src": "Stooq",
                },
            ]
        return []
    except Exception as exc:  # noqa: BLE001
        logger.exception("Stooq fetch error for %s: %s", code, exc)
        return []


# --------------------------------------------------------------------------------------
# Yanoshin headline API
# --------------------------------------------------------------------------------------

YANOSHIN_BASE = os.getenv(
    "YANOSHIN_API_BASE", "https://webapi.yanoshin.jp/webapi/tdnet/list"
)


def _fetch_tdnet_headlines(target_date: date) -> List[Dict[str, object]]:
    """Fetch TDnet JSON & produce simple binary sentiment (1 if guidance up)."""
    ymd = target_date.strftime("%Y%m%d")
    url = f"{YANOSHIN_BASE}/{ymd}.json"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items") if isinstance(data, dict) else data
        results: List[Dict[str, object]] = []
        for obj in items:
            if "Tdnet" in obj:
                d = obj["Tdnet"]
                code = d.get("company_code") or d.get("code")
                headline = d.get("title")
            else:
                code = obj.get("code") or obj.get("security_code")
                headline = obj.get("title") or obj.get("headline")
            if not (code and headline):
                continue
            sentiment = (
                1 if any(k in headline for k in ["上方修正", "自社株買", "増配"]) else 0
            )
            results.append(
                {
                    "code_jpx": code,
                    "metric": "headline_sentiment",
                    "ts_date": target_date,
                    "value": sentiment,
                    "src": "Yanoshin",
                }
            )
        return results
    except Exception as exc:  # noqa: BLE001
        logger.exception("Yanoshin headline fetch error: %s", exc)
        return []


# --------------------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------------------


def _download_impl(target_date: date, codes: List[str]) -> List[Dict[str, object]]:
    """実際のダウンロード処理 (以前の download 関数)。"""

    results: List[Dict[str, object]] = []

    # OHLCV & volume
    for code in codes:
        results.extend(_fetch_stooq(code, target_date))

    # TDnet headlines (not per-code filter here for simplicity)
    results.extend(_fetch_tdnet_headlines(target_date))

    # TODO: JPX 信用残高 (weekly) fetch

    return results


# -------------------------------------------------------------
# Class-based Downloader
# -------------------------------------------------------------


class MarketDownloader(BaseDownloader):
    """指定コード群に対するマーケットデータ Downloader。"""

    name = "market"

    def __init__(self, codes: List[str]):
        self.codes = codes

    def download(self, target_date: date) -> List[Dict[str, object]]:  # noqa: D401
        return _download_impl(target_date, self.codes)


# 従来 API ラッパ（即席利用向け）


def download(
    target_date: date, codes: List[str]
) -> List[Dict[str, object]]:  # noqa: D401
    """Backward-compatible functional API."""
    return MarketDownloader(codes).download(target_date)
