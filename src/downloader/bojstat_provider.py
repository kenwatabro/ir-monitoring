"""BOJ-STAT (日銀統計) series downloader.

BOJ Time-Series Data Search provides CSV download endpoints where the query
string includes *Statistics code* & *Series code* combinations. This
implementation uses the `bojpy` module for simplified data access.

The implementation is designed to fetch daily data series and accumulate
historical data over time, rather than targeting specific dates.

For detailed docs see:
https://www.stat-search.boj.or.jp/guide/index.html
"""

# ruff: noqa: E402
from __future__ import annotations

import logging
import os
from datetime import date
from typing import Dict, List

import pandas as pd
from bojpy import boj

from ._base import BaseDownloader

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# -----------------------------------------------------------------------------
# Built-in alias → key_series mapping
# -----------------------------------------------------------------------------
# NOTE: `key_series` 文字列は日本銀行「時系列統計データ検索」サイトの
#       CSV ダウンロード URL 内 `key_series` パラメータ値と 1:1 で対応します。
#       ここではマーケット分析で特に重要な指標を中心に定義し、実際のコードは
#       必ずご自身で確認してください（下記はあくまで例示用プレースホルダ）。
#       新規指標を追加したい場合はこの辞書に追記するだけで利用可能です。

_ALIAS_MAP: dict[str, dict[str, str]] = {
    # --- マネタリーベース ---
    "マネタリーベース平均残高（前年比）": {
        "id": "MD01'MABS1AN11@",
        "category": "monetary_base_yoy",
        "importance": "A",
    },
    "マネタリーベース平均残高": {
        "id": "MD01'MABS1AN11",
        "category": "monetary_base_level",
        "importance": "B",
    },
    # --- 政策金利（基準貸付利率） ---
    "基準貸付利率（日次）": {
        "id": "IR01'MADR1Z@D",
        "category": "policy_rate_daily",
        "importance": "A",
    },
    "基準貸付利率（月次）": {
        "id": "IR01'MADR1M",
        "category": "policy_rate_monthly",
        "importance": "B",
    },
    # --- O/N コール ---
    "無担保コールレート・O/N（日次平均）": {
        "id": "FM01'STRDCLUCON",
        "category": "call_rate_daily",
        "importance": "A",
    },
    "無担保コールレート・O/N（月平均）": {
        "id": "FM02'STRACLUCON",
        "category": "call_rate_monthly_avg",
        "importance": "B",
    },
    # --- 企業物価指数（CGPI） ---
    "国内企業物価指数 総平均（前年比）": {
        "id": "PR01'PRCG20_2200000000%",
        "category": "cgpi_domestic_yoy",
        "importance": "A",
    },
    "輸出物価指数（円ベース）総平均（前年比）": {
        "id": "PR01'PRCG20_2400000000%",
        "category": "cgpi_export_yoy",
        "importance": "B",
    },
    "輸入物価指数（円ベース）総平均（前年比）": {
        "id": "PR01'PRCG20_2600000000%",
        "category": "cgpi_import_yoy",
        "importance": "B",
    },
    # --- サービス物価指数（SPPI） ---
    "サービス物価指数（SPPI）総平均（前年比）": {
        "id": "PR02'PRCS20_5200000000%",
        "category": "sppi_allitems_yoy",
        "importance": "A",
    },
    # --- マネーストック ---
    "M2 平残前年比": {
        "id": "MD02'MAM1YAM2M2MO",
        "category": "money_stock_M2_yoy",
        "importance": "A",
    },
    "M2 平残": {
        "id": "MD02'MAM1NAM2M2MO",
        "category": "money_stock_M2_level",
        "importance": "B",
    },
    # --- 銀行貸出（全体） ---
    "総貸出平残（銀行計）前年比": {
        "id": "MD13'FAAPOBAL1@",
        "category": "bank_loans_total_yoy",
        "importance": "A",
    },
    "貸出金／末残／銀行等合計": {
        "id": "MD11'DLCLAEDBLTTO",
        "category": "bank_loans_total_level",
        "importance": "B",
    },
    # --- 為替関連 ---
    "実質実効為替レート指数": {
        "id": "FM09'FX180110002",
        "category": "reer",
        "importance": "B",
    },
    # --- 短観（企業マインド） ---
    "短観・業況判断DI／大企業／製造業／実績": {
        "id": "CO'TK99F1000601GCQ01000",  # 要確認（年度によって変更の可能性あり）
        "category": "tankan_business_cond_L_manu",
        "importance": "B",
    },
    # --- 国際収支 ---
    "経常収支": {
        "id": "BP01'BPBP6JYNCB",
        "category": "current_account",
        "importance": "B",
    },
}


# Helper to expose alias list to other modules / CLI


def available_aliases() -> list[str]:  # pragma: no cover  – util function
    """Return list of built-in alias names."""

    return sorted(_ALIAS_MAP.keys())


class BoJSeriesDownloader(BaseDownloader):
    """Download BOJ-STAT series data using bojpy module.

    This downloader fetches daily data series and accumulates historical data
    over time. It supports both individual series and multiple series processing.

    Parameters
    ----------
    series_ids
        *Either* a single series ID string or a list of series IDs.
        Can be raw `key_series` strings recognised by BOJ-STAT **or**
        built-in alias names defined in ``_ALIAS_MAP``.
    """

    def __init__(self, series_ids: str | List[str]):
        if isinstance(series_ids, str):
            series_ids = [series_ids]

        self.series_configs = []

        # Process each series ID
        for series_id in series_ids:
            config = self._resolve_series_config(series_id)
            self.series_configs.append(config)

        # Set name based on number of series
        if len(self.series_configs) == 1:
            self.name = f"boj:{self.series_configs[0]['display_name']}"
        else:
            self.name = f"boj:multiple_series({len(self.series_configs)})"

    def _resolve_series_config(self, series_id: str) -> dict:
        """Resolve series configuration from alias or direct ID."""
        if series_id in _ALIAS_MAP:  # resolve alias → real key_series
            meta = _ALIAS_MAP[series_id]
            resolved = meta.get("id")
            if resolved is None:
                raise KeyError(
                    "Alias metadata for '%s' must contain 'id' key. Available keys: %s"
                    % (series_id, list(meta.keys()))
                )
            return {
                "series_id": resolved,
                "display_name": series_id,
                "category": meta.get("category"),
                "importance": meta.get("importance"),
                "is_alias": True,
            }
        else:
            # Assume user passed real key_series already
            return {
                "series_id": series_id,
                "display_name": series_id,
                "category": None,
                "importance": None,
                "is_alias": False,
            }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def download(self, target_date: date | None = None) -> List[Dict[str, object]]:  # noqa: D401
        """Download all historical data for configured series.

        Note: target_date parameter is maintained for interface compatibility
        but is ignored. All historical data is fetched.
        """
        all_results: List[Dict[str, object]] = []

        # Process each series configuration
        for config in self.series_configs:
            series_id = config["series_id"]
            display_name = config["display_name"]

            logger.info("Fetching data for series: %s (%s)", display_name, series_id)

            try:
                # Use bojpy to get data series (all historical data)
                df = boj.get_data_series(series=series_id)

                if df is None or df.empty:
                    logger.warning("No data returned for series %s", series_id)
                    continue

                # Convert date index to proper datetime
                df.index = pd.to_datetime(df.index)

                # Process all data points in the DataFrame
                for timestamp, row in df.iterrows():
                    # Get the first (and typically only) column value
                    value = row.iloc[0] if len(row) > 0 else None

                    if pd.isna(value):
                        continue

                    # Convert timestamp to date
                    data_date = timestamp.date()

                    result_record = {
                        "series_id": series_id,
                        "ts_date": data_date,
                        "value": float(value),
                        "src": "BOJ-STAT",
                        "display_name": display_name,
                        "category": config.get("category"),
                        "importance": config.get("importance"),
                        "is_alias": config.get("is_alias", False),
                    }

                    all_results.append(result_record)

                logger.info(
                    "Successfully fetched %d data points for series %s",
                    len([r for r in all_results if r["series_id"] == series_id]),
                    display_name,
                )

            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "BOJ fetch error for %s using bojpy: %s", series_id, exc
                )
                continue

        logger.info("Total data points fetched: %d", len(all_results))
        return all_results

    def download_single_series(self, series_id: str) -> List[Dict[str, object]]:
        """Download data for a single series (utility method)."""
        temp_downloader = BoJSeriesDownloader(series_id)
        return temp_downloader.download()

    def get_available_series(self) -> List[str]:
        """Get list of configured series IDs."""
        return [config["series_id"] for config in self.series_configs]

    def get_series_info(self) -> List[Dict[str, object]]:
        """Get detailed information about configured series."""
        return [
            {
                "series_id": config["series_id"],
                "display_name": config["display_name"],
                "category": config.get("category"),
                "importance": config.get("importance"),
                "is_alias": config.get("is_alias", False),
            }
            for config in self.series_configs
        ]
