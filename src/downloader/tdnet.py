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


def _fetch_list(day: date) -> List[dict]:
    """Fetch TDnet daily disclosure list as list of dicts.

    The daily list is split into multiple CSV files: I_list_001_YYYYMMDD.csv, I_list_002_....
    This function keeps downloading sequential parts until it receives a 404.
    """
    results: List[dict] = []
    ymd = day.strftime("%Y%m%d")
    for idx in range(1, 10):  # assume at most 9 parts per day
        csv_name = f"I_list_{idx:03d}_{ymd}.csv"
        url = f"{LIST_BASE_URL}/{csv_name}"
        logger.debug("Fetching TDnet list part %s", url)
        resp = requests.get(url, timeout=60)
        if resp.status_code == 404:
            if idx == 1:
                logger.warning("No TDnet list found for %s", day)
            break
        resp.raise_for_status()
        text = resp.text
        reader = csv.reader(text.splitlines())
        # TDnet CSV header fields (spec as of 2024-05):
        # 0: 証券コード 1: 発表日時 2: タイトル 3: ファイル名 4: 備考
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


def download(day: date) -> List[pathlib.Path]:
    """Download all TDnet PDFs for the given day.

    Returns list of saved file paths.
    """
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