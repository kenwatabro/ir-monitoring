from __future__ import annotations

import logging
import os
import pathlib
import tempfile
from datetime import date
from typing import List

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

EDINET_BASE_URL = "https://disclosure.edinet-fsa.go.jp/api/v1"
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
        "type": 2,  # JSON list
    }
    api_key = os.getenv("EDINET_API_KEY")
    if api_key:
        params["Subscription-Key"] = api_key

    logger.info("Fetching EDINET list for %s", day)
    resp = requests.get(f"{EDINET_BASE_URL}/documents.json", params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data.get("results", [])


def download(day: date) -> List[pathlib.Path]:
    """Download all EDINET documents of the day and save to RAW_DIR.

    Returns list of saved file paths.
    """
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
    }
    resp = requests.get(f"{EDINET_BASE_URL}/documents/{doc_id}", params=params, stream=True, timeout=300)
    resp.raise_for_status()
    # Save incrementally to avoid memory blow-up
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                tmp.write(chunk)
        tmp_path = pathlib.Path(tmp.name)
    tmp_path.replace(dest_path)
    logger.info("Saved %s", dest_path) 