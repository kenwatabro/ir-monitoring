from __future__ import annotations

import logging
import os
import pathlib
import tempfile
from typing import List

import pdfplumber
import pytesseract
from PIL import Image

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

OCR_LANG = os.getenv("OCR_LANG", "jpn+eng+kor+chi_sim")


def extract_text(pdf_path: pathlib.Path) -> List[str]:
    """Extract text per page from PDF using OCR.

    Returns list of page texts.
    """
    texts: List[str] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp_png:
                page_image = page.to_image(resolution=300)
                page_image.save(tmp_png.name, format="PNG")
                text = pytesseract.image_to_string(
                    Image.open(tmp_png.name), lang=OCR_LANG
                )
                texts.append(text)
                os.unlink(tmp_png.name)
    logger.debug("Extracted %d pages from %s", len(texts), pdf_path)
    return texts
