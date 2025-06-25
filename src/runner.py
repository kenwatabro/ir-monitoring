from __future__ import annotations

import logging
import os
import uuid
from datetime import date, timedelta

from dotenv import load_dotenv

from src.downloader import edinet, tdnet

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

    for offset in range(days):
        day = since + timedelta(days=offset)
        AuditLogger.log("INFO", "downloader.edinet", "download", {"date": str(day)})
        edinet_results = edinet.download(day)
        AuditLogger.log("INFO", "downloader.tdnet", "download", {"date": str(day)})
        tdnet_results = tdnet.download(day)
        AuditLogger.log("INFO", "runner", "download_complete", {"edinet": len(edinet_results), "tdnet": len(tdnet_results)})

    AuditLogger.log("INFO", "runner", "end", {"run_id": str(run_id)})


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run IR ETL pipeline")
    parser.add_argument("--since", type=lambda s: date.fromisoformat(s), required=True)
    parser.add_argument("--days", type=int, default=1)
    args = parser.parse_args()

    run_since(args.since, args.days) 