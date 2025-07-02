from __future__ import annotations

import logging
import os
import pathlib
import tempfile
import zipfile
from datetime import date
from typing import List
import time

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

# Which docTypeCode has ZIP (=XBRL) available
_ZIP_TYPES = {"120", "130"}  # 有価証券報告書・四半期報告書など
# その他は PDF のみ


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
    resp = requests.get(
        f"{EDINET_BASE_URL}/documents.json", params=params, headers=headers, timeout=60
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("results", [])


def _is_corporate_report(meta: dict) -> bool:
    """Return True if the EDINET metadata represents a corporate (listed company) report.

    EDINET API v2 metadata fields:

    * ordinanceCode "010"   … 会社法/金融商品取引法に基づく通常の有報・四半期報など
    * fundCode      None     … 投資信託・投資法人の場合は 5 桁程度のコードが入る

    アセットマネジメント会社が提出する投資信託の報告書等を除外するため、
    上記の組み合わせ（ordinanceCode=="010" かつ fundCode が空）を満たす
    レコードのみをダウンロード対象とする。
    """

    # ordinanceCode はゼロ詰め 3 桁の文字列として返る
    # fundCode は投資信託等の場合にのみ文字列で入り、それ以外は None
    return meta.get("ordinanceCode") == "010" and not meta.get("fundCode")


def _download_impl(day: date) -> List[pathlib.Path]:
    """実際のダウンロード処理本体 (以前の download 関数)。"""
    results: List[pathlib.Path] = []
    for item in _edinet_list(day):
        # 投資信託・ETF 等の書類を除外
        if not _is_corporate_report(item):
            logger.debug(
                "Skip non-corporate report: %s ordinance=%s fund=%s",
                item.get("docID"),
                item.get("ordinanceCode"),
                item.get("fundCode"),
            )
            continue
        doc_id = item.get("docID")
        dtype = str(item.get("docTypeCode", ""))
        xbrl_flag = str(item.get("xbrlFlag")) == "1"
        if not doc_id:
            continue
        dest_dir = RAW_DIR / "EDINET" / day.strftime("%Y/%m/%d") / doc_id
        dest_dir.mkdir(parents=True, exist_ok=True)

        is_zip = xbrl_flag and dtype in _ZIP_TYPES
        ext = "zip" if is_zip else "pdf"
        dest_path = dest_dir / f"{doc_id}.{ext}"
        if dest_path.exists():
            results.append(dest_path)
            continue

        try:
            if is_zip:
                _download_single(doc_id, dest_path, attempts=3)
            else:
                _download_pdf_only(doc_id, dest_path)
            results.append(dest_path)
        except Exception as e:  # noqa: BLE001
            logger.exception("Failed to download %s: %s", doc_id, e)
    return results


def _download_single(
    doc_id: str, dest_path: pathlib.Path, *, attempts: int = 3, backoff: int = 20
) -> None:
    """Download a single document ZIP with simple retry logic.

    EDINET は docID 公開後に ZIP 生成が遅延するため、先に取得を試みると
    JSON エラー本文を 200 OK で返してくる場合がある。その際は
    `zipfile.is_zipfile()` が False になるので、一定間隔で再試行する。

    Parameters
    ----------
    doc_id : str
        Document ID
    dest_path : Path
        保存先パス
    attempts : int, default 3
        最大リトライ回数
    backoff : int, default 20
        リトライ間隔（秒）
    """
    params = {
        "type": 1,  # ZIP file
        "Subscription-Key": os.getenv("EDINET_API_KEY", ""),
    }

    headers = {"User-Agent": "ir-monitoring-bot/0.1"}

    for attempt in range(1, attempts + 1):
        resp = requests.get(
            f"{EDINET_BASE_URL}/documents/{doc_id}",
            params=params,
            headers=headers,
            stream=True,
            timeout=300,
        )
        resp.raise_for_status()

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    tmp.write(chunk)
            tmp_path = pathlib.Path(tmp.name)
        tmp_path.replace(dest_path)

        if zipfile.is_zipfile(dest_path):
            logger.info("Saved %s", dest_path)
            return

        # Not a valid ZIP → likely still processing
        dest_path.unlink(missing_ok=True)
        if attempt < attempts:
            logger.info(
                "ZIP not ready for %s (attempt %d/%d): retrying in %ss",
                doc_id,
                attempt,
                attempts,
                backoff,
            )
            time.sleep(backoff)
        else:
            raise ValueError("Received non-ZIP content from EDINET API after retries")


# ------------------------------------------------------------------
# PDF download helper for non-XBRL documents
# ------------------------------------------------------------------


def _download_pdf_only(doc_id: str, dest_path: pathlib.Path) -> None:
    """Download PDF for documents where ZIP is not provided."""

    params = {
        "type": 2,  # PDF
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

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                tmp.write(chunk)
        tmp_path = pathlib.Path(tmp.name)
    tmp_path.replace(dest_path)
    logger.info("Saved PDF %s", dest_path)


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
