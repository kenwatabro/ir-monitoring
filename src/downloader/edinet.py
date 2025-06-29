from __future__ import annotations

import logging
import os
import pathlib
import tempfile
import zipfile
from datetime import date
from typing import List

import requests
from dotenv import load_dotenv

# OOP 統一のための Downloader 基底クラス
from ._base import FileDownloader

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

EDINET_BASE_URL = os.getenv("EDINET_BASE_URL", "https://api.edinet-fsa.go.jp/api/v2")
RAW_DIR = pathlib.Path(os.getenv("RAW_DIR", "data/raw"))
RAW_DIR.mkdir(parents=True, exist_ok=True)


def _edinet_list(day: date) -> List[dict]:
    """Fetch list of EDINET documents for a given day.

    Parameters
    ----------
    day: date
        Target publication date (JST).

    Returns
    -------
    List[dict]
        JSON list of document metadata.
    """
    params = {
        "date": day.strftime("%Y-%m-%d"),
        "type": 2,
        "Subscription-Key": os.getenv("EDINET_API_KEY", ""),
    }

    logger.info("Fetching EDINET list for %s via v2", day)
    headers = {"User-Agent": "ir-monitoring-bot/0.1"}
    resp = requests.get(f"{EDINET_BASE_URL}/documents.json", params=params, headers=headers, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data.get("results", [])


def _download_impl(day: date) -> List[pathlib.Path]:
    """実際のダウンロード処理本体 (以前の download 関数)。"""
    results: List[pathlib.Path] = []
    for item in _edinet_list(day):
        doc_id = item.get("docID")
        if not doc_id:
            continue
        dest_dir = RAW_DIR / day.strftime("%Y/%m/%d") / doc_id
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / f"{doc_id}.zip"
        if dest_path.exists():
            logger.debug("Skip existing %s", dest_path)
            results.append(dest_path)
            continue

        try:
            _download_single(doc_id, dest_path)
            results.append(dest_path)
        except Exception as e:  # noqa: BLE001
            logger.exception("Failed to download %s: %s", doc_id, e)
    return results


def _download_single(doc_id: str, dest_path: pathlib.Path) -> None:
    """Download a single document ZIP from EDINET."""
    params = {
        "type": 1,  # ZIP file
        "Subscription-Key": os.getenv("EDINET_API_KEY", ""),
    }

    headers = {"User-Agent": "ir-monitoring-bot/0.1"}

    resp = requests.get(
        f"{EDINET_BASE_URL}/documents/{doc_id}",
        params=params,
        headers=headers,
        stream=True,
        timeout=300,
    )
    resp.raise_for_status()
    # Save incrementally to avoid memory blow-up
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                tmp.write(chunk)
        tmp_path = pathlib.Path(tmp.name)
    tmp_path.replace(dest_path)

    # Validate ZIP integrity; EDINET may return JSON error body with 200 OK
    if not zipfile.is_zipfile(dest_path):
        logger.warning("Invalid ZIP (likely JSON error) received for %s, removing", doc_id)
        dest_path.unlink(missing_ok=True)
        raise ValueError("Received non-ZIP content from EDINET API")

    logger.info("Saved %s", dest_path)


# -------------------------------------------------------------
# Class-based Downloader
# -------------------------------------------------------------


class EdinetDownloader(FileDownloader):
    """EDINET ZIP ドキュメントを日付単位で取得する Downloader。"""

    name = "edinet"

    def download(self, target_date: date) -> List[pathlib.Path]:  # noqa: D401
        return _download_impl(target_date)


# 既存 API を壊さないための関数ラッパ
_downloader = EdinetDownloader()


def download(day: date) -> List[pathlib.Path]:  # noqa: D401
    """Backward-compatible functional API."""
    return _downloader.download(day) 