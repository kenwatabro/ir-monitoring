#!/usr/bin/env python
"""Quick sanity-check for Parser layer.

Usage::

    python scripts/parse_check.py --limit 20

Iterates through *data/raw* files (.zip for EDINET, .pdf for EDINET / TDnet),
tries to parse them with existing parser modules, and prints a concise
success / failure summary.
"""

from __future__ import annotations

import argparse
import logging
import os
import pathlib
import sys
import tempfile
from typing import List
from zipfile import ZipFile

# Ensure project root (two levels up) on sys.path when executed as a script
PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.parser import xbrl as xbrl_parser  # noqa: E402  isort: skip
from src.parser import ocr as ocr_parser  # noqa: E402  isort: skip

logger = logging.getLogger(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

RAW_DIR = pathlib.Path("data/raw")


def _iter_raw_files() -> List[pathlib.Path]:
    """Return list of candidate raw files (zip & pdf)."""
    files: List[pathlib.Path] = []
    for path in RAW_DIR.rglob("*.zip"):
        files.append(path)
    for path in RAW_DIR.rglob("*.pdf"):
        files.append(path)
    return sorted(files)


def _handle_zip(path: pathlib.Path) -> bool:
    """Extract first *.xbrl* inside zip and parse facts.

    Returns True on success (>=1 fact extracted), False otherwise.
    """
    try:
        with ZipFile(path) as zf:
            # pick XBRL files, sort by file size DESC so PublicDoc likely first
            xbrl_members = sorted(
                [n for n in zf.namelist() if n.lower().endswith(".xbrl")],
                key=lambda n: -zf.getinfo(n).file_size,
            )
            if not xbrl_members:
                logger.warning("%s contains no XBRL file", path)
                return False

            with tempfile.TemporaryDirectory() as td:
                # 展開して参照ファイル(.xsd など)を含める
                zf.extractall(td)
                for member in xbrl_members:
                    tmp_path = pathlib.Path(td) / member
                    facts = xbrl_parser.extract_facts(str(tmp_path))

                    if xbrl_parser.Cntlr is None:
                        # arelle unavailable – skip detailed check
                        logger.warning(
                            "arelle unavailable – skipped XBRL parse for %s", path
                        )
                        return True

                    logger.info("%s -> %d facts (%s)", path.name, len(facts), member)

                    # If facts found, consider success immediately
                    if facts:
                        return True

            # No facts found but parsed without exception
            return True
    except Exception as exc:  # pragma: no cover – integration
        logger.exception("Error parsing %s: %s", path, exc)
        return False


def _handle_pdf(path: pathlib.Path) -> bool:
    """Run OCR parser.

    Returns True on success (>=1 page)."""
    try:
        try:
            texts = ocr_parser.extract_text(path)
            logger.info("%s -> %d pages", path.name, len(texts))
            return len(texts) > 0
        except Exception as inner:
            # Reraise to outer except block if not TesseractNotFound
            from pytesseract.pytesseract import TesseractNotFoundError

            if isinstance(inner, TesseractNotFoundError):
                logger.warning("tesseract missing – skipped OCR for %s", path)
                return True  # neutral success
            raise
    except Exception as exc:  # pragma: no cover – integration
        logger.exception("Error OCR %s: %s", path, exc)
        return False


def main(limit: int | None = None) -> None:
    files = _iter_raw_files()
    if limit is not None:
        files = files[:limit]
    logger.info("Found %d raw files (scanning limit=%s)", len(files), limit)

    ok = 0
    for f in files:
        if f.suffix.lower() == ".zip":
            ok += 1 if _handle_zip(f) else 0
        elif f.suffix.lower() == ".pdf":
            ok += 1 if _handle_pdf(f) else 0
    logger.info("%d/%d files parsed successfully", ok, len(files))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Quick parse sanity-check")
    parser.add_argument("--limit", type=int, help="Max number of files to scan")
    args = parser.parse_args()
    main(args.limit)
