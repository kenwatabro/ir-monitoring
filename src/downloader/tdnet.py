# ruff: noqa: E402
import csv
import logging
import os
import pathlib
import tempfile
from datetime import date
from typing import List

import requests

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# Base directory to store raw TDnet PDFs
RAW_DIR = pathlib.Path(os.getenv("RAW_DIR", "data/raw"))
RAW_DIR.mkdir(parents=True, exist_ok=True)

# TDnet daily CSV list base URL
LIST_BASE_URL = "https://www.release.tdnet.info/inbs"
YANOSHIN_API_BASE = os.getenv(
    "YANOSHIN_API_BASE", "https://webapi.yanoshin.jp/webapi/tdnet/list"
)

# OOP 統一のための Downloader 基底クラス
from ._base import FileDownloader


def _fetch_list_api(day: date) -> List[dict]:
    """Fetch list via webapi.yanoshin.jp (JSON).

    Returns empty list if any error occurs.
    """
    ymd = day.strftime("%Y%m%d")
    url = f"{YANOSHIN_API_BASE}/{ymd}.json"
    logger.debug("Fetching TDnet list via yanoshin API: %s", url)
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code != 200:
            logger.warning("Yanoshin API returned status %s", resp.status_code)
            return []
        data = resp.json()
        items = (
            data.get("items") if isinstance(data, dict) else data
        )  # API sometimes returns list at top
        results: List[dict] = []
        for obj in items:
            try:
                # Yanoshin API v2 (2024-) wraps payload under 'Tdnet' key
                if "Tdnet" in obj:
                    rec = obj["Tdnet"]
                    filename = rec.get("document_url", "").split("/")[-1]
                    code = rec.get("company_code")
                else:
                    filename = (
                        obj.get("filename")
                        or obj.get("document_url", "").split("/")[-1]
                    )
                    code = obj.get("code") or obj.get("security_code")
                if filename and filename.lower().endswith(".pdf"):
                    results.append({"code": code, "filename": filename})
            except Exception:  # noqa: BLE001
                continue
        return results
    except Exception as e:  # noqa: BLE001
        logger.exception("Yanoshin API failure: %s", e)
        return []


def _fetch_list(day: date) -> List[dict]:
    """Try Yanoshin API first; fallback to CSV scraping."""
    results = _fetch_list_api(day)
    if results:
        return results
    logger.info("Falling back to official CSV scraping for %s", day)
    return _fetch_list_scrape(day)


def _fetch_list_scrape(day: date) -> List[dict]:
    """Original CSV scraping implementation."""
    results: List[dict] = []
    ymd = day.strftime("%Y%m%d")
    for idx in range(1, 10):
        csv_name = f"I_list_{idx:03d}_{ymd}.csv"
        url = f"{LIST_BASE_URL}/{csv_name}"
        logger.debug("Fetching TDnet CSV part %s", url)
        resp = requests.get(url, timeout=60)
        if resp.status_code == 404:
            if idx == 1:
                logger.warning("No TDnet CSV found for %s", day)
            break
        resp.raise_for_status()
        reader = csv.reader(resp.text.splitlines())
        for row in reader:
            if not row or row[0].startswith("#"):
                continue
            try:
                code = row[0].strip()
                filename = row[3].strip()
            except IndexError:
                continue
            if not filename.lower().endswith(".pdf"):
                continue
            results.append({"code": code, "filename": filename})
    return results


def _download_pdf(filename: str, dest_path: pathlib.Path) -> None:
    """Download single PDF from TDnet server."""
    url = f"{LIST_BASE_URL}/{filename}"
    resp = requests.get(url, stream=True, timeout=300)
    resp.raise_for_status()
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                tmp.write(chunk)
        tmp_path = pathlib.Path(tmp.name)
    tmp_path.replace(dest_path)
    logger.info("Saved %s", dest_path)


def _download_impl(day: date) -> List[pathlib.Path]:
    """実際のダウンロード処理本体 (以前の download 関数)。"""
    saved: List[pathlib.Path] = []
    for item in _fetch_list(day):
        filename = item["filename"]
        doc_id = pathlib.Path(filename).stem  # remove .pdf
        dest_dir = RAW_DIR / day.strftime("%Y/%m/%d") / doc_id
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / filename
        if dest_path.exists():
            logger.debug("Skip existing %s", dest_path)
            saved.append(dest_path)
            continue
        try:
            _download_pdf(filename, dest_path)
            saved.append(dest_path)
        except Exception as e:  # noqa: BLE001
            logger.exception("Failed to download TDnet file %s: %s", filename, e)
    return saved


# -------------------------------------------------------------
# Class-based Downloader
# -------------------------------------------------------------


class TdnetDownloader(FileDownloader):
    """TDnet PDF を日付単位で取得する Downloader。"""

    name = "tdnet"

    def download(self, target_date: date) -> List[pathlib.Path]:  # noqa: D401
        return _download_impl(target_date)


# 既存 API を壊さないための関数ラッパ
_downloader = TdnetDownloader()


def download(day: date) -> List[pathlib.Path]:  # noqa: D401
    """Backward-compatible functional API."""
    return _downloader.download(day)
