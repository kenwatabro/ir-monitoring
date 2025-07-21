from __future__ import annotations

import logging
import os
import pathlib
import uuid
from datetime import date, timedelta

from dotenv import load_dotenv

from src import db as db_module
from src.downloader import edinet, tdnet
from src.downloader.storage import calc_sha256
from src.downloader.macro import MacroAggregator
from src.processor import FileProcessor

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))


class AuditLogger:
    """Very simple console audit for now."""

    @staticmethod
    def log(level: str, module: str, action: str, detail: dict | None = None) -> None:  # noqa: D401
        logger.info("%s | %s | %s | %s", level, module, action, detail)


def run_since(since: date, days: int = 1) -> None:
    run_id = uuid.uuid4()
    AuditLogger.log("INFO", "runner", "start", {"run_id": str(run_id)})

    # Initialize file processor
    processor = FileProcessor()

    for offset in range(days):
        day = since + timedelta(days=offset)
        AuditLogger.log("INFO", "downloader.edinet", "download", {"date": str(day)})
        edinet_results = edinet.download(day)
        AuditLogger.log("INFO", "downloader.tdnet", "download", {"date": str(day)})
        tdnet_results = tdnet.download(day)

        # register in DB
        _register_documents(
            day, "EDINET", edinet_results, xbrl_flag=True, pdf_flag=False
        )
        _register_documents(day, "TDnet", tdnet_results, xbrl_flag=False, pdf_flag=True)

        # 🆕 NEW: File processing pipeline
        AuditLogger.log("INFO", "processor", "start", {"date": str(day)})
        _process_files(processor, edinet_results, tdnet_results, day)
        AuditLogger.log("INFO", "processor", "complete", {"date": str(day)})

        # Macro series fetch & upsert
        _upsert_macro(day)

        AuditLogger.log(
            "INFO",
            "runner",
            "download_complete",
            {"edinet": len(edinet_results), "tdnet": len(tdnet_results)},
        )

    AuditLogger.log("INFO", "runner", "end", {"run_id": str(run_id)})


def _register_documents(
    pub_date: date,
    source: str,
    files: list[pathlib.Path],
    *,
    xbrl_flag: bool,
    pdf_flag: bool,
) -> None:
    for path in files:
        sha256 = calc_sha256(path)
        size = path.stat().st_size
        doc_id = path.stem.split(".")[0]
        record = {
            "doc_id": doc_id,
            "source": source,
            "doc_type": "unknown",
            "pub_date": pub_date,
            "file_path": str(path),
            "sha256": sha256,
            "size_bytes": size,
            "xbrl_flag": xbrl_flag,
            "pdf_flag": pdf_flag,
        }
        db_module.upsert_document(record)


def _process_files(
    processor: FileProcessor,
    edinet_results: list[pathlib.Path],
    tdnet_results: list[pathlib.Path],
    day: date,
) -> None:
    """Process downloaded files with FileProcessor."""
    edinet_success = 0
    tdnet_success = 0
    total_xbrl_facts = 0
    total_pdf_pages = 0

    # Process EDINET files
    for file_path in edinet_results:
        try:
            result = processor.process_edinet_file(file_path)
            if result.success:
                edinet_success += 1
                total_xbrl_facts += result.xbrl_facts_count
                total_pdf_pages += result.pdf_pages_count
            else:
                logger.warning(
                    "Failed to process EDINET file %s: %s",
                    file_path,
                    result.error_message,
                )
        except Exception as e:
            logger.exception("Error processing EDINET file %s: %s", file_path, e)

    # Process TDnet files
    for file_path in tdnet_results:
        try:
            result = processor.process_tdnet_file(file_path)
            if result.success:
                tdnet_success += 1
                total_pdf_pages += result.pdf_pages_count
            else:
                logger.warning(
                    "Failed to process TDnet file %s: %s",
                    file_path,
                    result.error_message,
                )
        except Exception as e:
            logger.exception("Error processing TDnet file %s: %s", file_path, e)

    AuditLogger.log(
        "INFO",
        "processor",
        "summary",
        {
            "date": str(day),
            "edinet_processed": edinet_success,
            "tdnet_processed": tdnet_success,
            "total_xbrl_facts": total_xbrl_facts,
            "total_pdf_pages": total_pdf_pages,
        },
    )


def _upsert_macro(day: date) -> None:
    """Fetch macro series via MacroAggregator and upsert into DB."""
    aggregator = MacroAggregator()
    rows = aggregator.download(day)
    for rec in rows:
        db_module.upsert_macro_series(rec)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run IR ETL pipeline")
    parser.add_argument("--since", type=lambda s: date.fromisoformat(s), required=True)
    parser.add_argument("--days", type=int, default=1)
    args = parser.parse_args()

    run_since(args.since, args.days)
