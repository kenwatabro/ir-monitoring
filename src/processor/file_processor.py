"""File processor for automatic data extraction and DB storage."""

from __future__ import annotations

import logging
import math
import os
import pathlib
import tempfile
import zipfile
from typing import Dict, List, Optional

from src import db as db_module
from src.parser import xbrl as xbrl_parser
from src.parser import ocr as ocr_parser
from src.parser.xbrl import FinanceFact

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# Feature flag for star-schema migration
USE_STAR_SCHEMA = os.getenv("USE_STAR_SCHEMA", "false").lower() == "true"


class ProcessResult:
    """Result of file processing operation."""

    def __init__(self, success: bool, file_path: pathlib.Path, doc_id: str):
        self.success = success
        self.file_path = file_path
        self.doc_id = doc_id
        self.xbrl_facts_count = 0
        self.finance_facts_count = 0  # Star-schema facts count
        self.pdf_pages_count = 0
        self.error_message: Optional[str] = None


class FileProcessor:
    """統合ファイル処理クラス - PDFとXBRLファイルからデータを抽出してDBに保存"""

    def __init__(self):
        self.logger = logger

    def process_edinet_file(self, file_path: pathlib.Path) -> ProcessResult:
        """EDINETファイル（ZIP/PDF）を処理してDBに保存

        Args:
            file_path: EDINETファイルのパス

        Returns:
            ProcessResult: 処理結果
        """
        doc_id = file_path.stem.split(".")[0]
        result = ProcessResult(True, file_path, doc_id)

        try:
            if file_path.suffix.lower() == ".zip":
                result = self._process_edinet_zip(file_path, doc_id)
            elif file_path.suffix.lower() == ".pdf":
                result = self._process_edinet_pdf(file_path, doc_id)
            else:
                result.success = False
                result.error_message = f"Unsupported file type: {file_path.suffix}"

            self.logger.info(
                "EDINET file processed: %s (success=%s, xbrl_facts=%d, finance_facts=%d, pdf_pages=%d)",
                file_path.name,
                result.success,
                result.xbrl_facts_count,
                result.finance_facts_count,
                result.pdf_pages_count,
            )

        except Exception as e:
            self.logger.exception("Error processing EDINET file %s: %s", file_path, e)
            result.success = False
            result.error_message = str(e)

        return result

    def process_tdnet_file(self, file_path: pathlib.Path) -> ProcessResult:
        """TDnet PDFを処理してDBに保存

        Args:
            file_path: TDnet PDFファイルのパス

        Returns:
            ProcessResult: 処理結果
        """
        doc_id = file_path.stem.split(".")[0]
        result = ProcessResult(True, file_path, doc_id)

        try:
            if file_path.suffix.lower() == ".pdf":
                result = self._process_tdnet_pdf(file_path, doc_id)
            else:
                result.success = False
                result.error_message = f"Unsupported file type: {file_path.suffix}"

            self.logger.info(
                "TDnet file processed: %s (success=%s, pdf_pages=%d)",
                file_path.name,
                result.success,
                result.pdf_pages_count,
            )

        except Exception as e:
            self.logger.exception("Error processing TDnet file %s: %s", file_path, e)
            result.success = False
            result.error_message = str(e)

        return result

    def _process_edinet_zip(self, zip_path: pathlib.Path, doc_id: str) -> ProcessResult:
        """EDINET ZIPファイルを処理（XBRLファクト抽出）"""
        result = ProcessResult(True, zip_path, doc_id)

        if not zipfile.is_zipfile(zip_path):
            result.success = False
            result.error_message = "Not a valid ZIP file"
            return result

        with tempfile.TemporaryDirectory() as temp_dir:
            # ZIPファイルを展開
            with zipfile.ZipFile(zip_path, "r") as zip_file:
                zip_file.extractall(temp_dir)

            # XBRLファイルを検索
            temp_path = pathlib.Path(temp_dir)
            xbrl_files = list(temp_path.rglob("*.xbrl"))

            if not xbrl_files:
                self.logger.warning("No XBRL files found in %s", zip_path)
                return result

            # 最大サイズのXBRLファイルを選択（通常はPublicDoc.xbrl）
            xbrl_file = max(xbrl_files, key=lambda f: f.stat().st_size)

            # XBRLファクト抽出
            if USE_STAR_SCHEMA:
                # 新しいStar-Schema対応API使用
                finance_facts = self._extract_finance_facts_star_schema(
                    xbrl_file, doc_id
                )
                result.finance_facts_count = len(finance_facts)

                # TODO: 新しいfact_financeテーブルに保存
                db_module.upsert_finance_facts(doc_id, finance_facts)
                self.logger.info(
                    "Extracted %d finance facts (star-schema) for doc_id: %s",
                    len(finance_facts),
                    doc_id,
                )
            else:
                # 既存のレガシー処理
                facts = self._extract_xbrl_facts(xbrl_file, doc_id)
                result.xbrl_facts_count = len(facts)

                # データベースに保存
                if facts:
                    db_module.upsert_facts(facts)
                    self.logger.info(
                        "Saved %d XBRL facts for doc_id: %s", len(facts), doc_id
                    )

        return result

    def _process_edinet_pdf(self, pdf_path: pathlib.Path, doc_id: str) -> ProcessResult:
        """EDINET PDFファイルを処理（テキスト抽出）"""
        result = ProcessResult(True, pdf_path, doc_id)

        # PDFテキスト抽出
        pdf_texts = self._extract_pdf_texts(pdf_path, doc_id)
        result.pdf_pages_count = len(pdf_texts)

        # データベースに保存
        if pdf_texts:
            db_module.upsert_pdf_texts(pdf_texts)
            self.logger.info(
                "Saved %d PDF text pages for doc_id: %s", len(pdf_texts), doc_id
            )

        return result

    def _process_tdnet_pdf(self, pdf_path: pathlib.Path, doc_id: str) -> ProcessResult:
        """TDnet PDFファイルを処理（テキスト抽出）"""
        result = ProcessResult(True, pdf_path, doc_id)

        # PDFテキスト抽出
        pdf_texts = self._extract_pdf_texts(pdf_path, doc_id)
        result.pdf_pages_count = len(pdf_texts)

        # データベースに保存
        if pdf_texts:
            db_module.upsert_pdf_texts(pdf_texts)
            self.logger.info(
                "Saved %d PDF text pages for doc_id: %s", len(pdf_texts), doc_id
            )

        return result

    def _extract_xbrl_facts(
        self, xbrl_path: pathlib.Path, doc_id: str
    ) -> List[Dict[str, object]]:
        """XBRLファイルからファクトを抽出"""
        try:
            facts = xbrl_parser.extract_facts(str(xbrl_path))

            # DBスキーマに合わせてファクトを変換
            db_facts = []
            for fact in facts:
                value = fact.get("value")

                # 数値データのみを処理（文字列データは除外）
                if value is None or value == "":
                    continue

                # 数値に変換可能かチェック
                try:
                    # 数値型に変換を試行
                    float(value)
                    is_numeric = True
                except (ValueError, TypeError):
                    # 数値でない場合はログに記録して除外
                    self.logger.debug(
                        f"Skipping non-numeric fact: {fact.get('name')} = {value}"
                    )
                    is_numeric = False

                # 数値データのみを保存
                if is_numeric:
                    # unit値の正規化（NoneやNaN、空文字列の場合は'N/A'を設定）
                    unit_value = fact.get("unit")
                    if (
                        unit_value is None
                        or unit_value == ""
                        or (isinstance(unit_value, float) and math.isnan(unit_value))
                    ):
                        unit_value = "N/A"

                    db_fact = {
                        "doc_id": doc_id,
                        "item": fact.get("name", ""),
                        "context": fact.get("context", ""),
                        "unit": unit_value,
                        "decimals": fact.get("decimals"),
                        "value": value,
                    }
                    db_facts.append(db_fact)

            return db_facts

        except Exception as e:
            self.logger.exception(
                "Error extracting XBRL facts from %s: %s", xbrl_path, e
            )
            return []

    def _extract_finance_facts_star_schema(
        self, xbrl_path: pathlib.Path, doc_id: str
    ) -> List[FinanceFact]:
        """XBRLファイルからStar-Schema用FinanceFactを抽出

        新しいextract_finance_facts APIを使用してStar-Schema対応の
        正規化されたFinanceFactオブジェクトを生成する。
        """
        try:
            # 新しいAPIを使用してFinanceFactリストを取得
            finance_facts = xbrl_parser.extract_finance_facts(str(xbrl_path))

            self.logger.debug(
                "Extracted %d finance facts from %s using star-schema API",
                len(finance_facts),
                xbrl_path,
            )

            return finance_facts

        except Exception as e:
            self.logger.exception(
                "Error extracting finance facts (star-schema) from %s: %s", xbrl_path, e
            )
            return []

    def _extract_pdf_texts(
        self, pdf_path: pathlib.Path, doc_id: str
    ) -> List[Dict[str, object]]:
        """PDFファイルからテキストを抽出"""
        try:
            texts = ocr_parser.extract_text(pdf_path)

            # DBスキーマに合わせてテキストを変換
            db_texts = []
            for page_no, text in enumerate(texts, 1):
                db_text = {
                    "doc_id": doc_id,
                    "page_no": page_no,
                    "text": text,
                    "avg_confidence": 0.0,  # OCRライブラリから取得可能であれば設定
                    "error_flag": False,
                    "error_type": None,
                }
                db_texts.append(db_text)

            return db_texts

        except Exception as e:
            self.logger.exception("Error extracting PDF texts from %s: %s", pdf_path, e)
            return []
