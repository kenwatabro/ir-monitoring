from __future__ import annotations

import logging
import os
import shutil
import pathlib
import tempfile
from typing import List, Sequence

import pdfplumber
import pytesseract
from PIL import Image, ImageOps

# Parallelism
from concurrent.futures import ProcessPoolExecutor, as_completed

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

OCR_LANG = os.getenv("OCR_LANG", "jpn+eng+kor+chi_sim")

# Optional fast renderer (poppler) --------------------------------------------------
try:
    from pdf2image import convert_from_path  # type: ignore

    _PDF2IMAGE_AVAILABLE = True
except ImportError:  # pragma: no cover
    convert_from_path = None  # type: ignore
    _PDF2IMAGE_AVAILABLE = False

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _ocr_page(img: "Image.Image") -> str:  # type: ignore[name-defined]
    """OCR single PIL image with adaptive timeout & optional resize."""

    # Downscale extremely large images (long edge > 2500 px)
    if max(img.size) > 2500:
        img = ImageOps.contain(img, (2500, 2500), Image.LANCZOS)

    # Dynamic timeout: base 15s + 1s per 2 megapixels
    mpixels = (img.width * img.height) / 1_000_000
    timeout = 15 + int(mpixels / 2)

    return pytesseract.image_to_string(img, lang=OCR_LANG, timeout=timeout)  # type: ignore[arg-type]


def _detect_poppler_dir() -> str | None:
    # 1) 明示指定があればそれを使う
    for var in ("POPPLER_PATH", "POPPLER_BIN"):
        if p := os.getenv(var):
            return p
    # 2) PATH から pdftoppm を探し、その親ディレクトリを返す
    ppm = shutil.which("pdftoppm")
    return os.path.dirname(ppm) if ppm else None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def extract_text(pdf_path: pathlib.Path) -> List[str]:
    """Extract text per page from PDF using OCR.

    1) If **pdf2image** (poppler) が利用可能なら高速で変換。
    2) それ以外は従来の pdfplumber.to_image() にフォールバック。
    """

    texts: List[str] = []

    poppler_dir = _detect_poppler_dir()

    if _PDF2IMAGE_AVAILABLE:
        try:
            images: Sequence[Image.Image] = convert_from_path(
                str(pdf_path),
                dpi=250,
                thread_count=2,
                timeout=120,
                poppler_path=poppler_dir,
            )

            # Parallel OCR to utilise multiple CPU cores
            with ProcessPoolExecutor(max_workers=os.cpu_count() or 2) as exe:
                futures = {
                    exe.submit(_ocr_page, img): idx for idx, img in enumerate(images)
                }
                # Preserve order by page
                results = [""] * len(images)
                for fut in as_completed(futures):
                    idx = futures[fut]
                    try:
                        results[idx] = fut.result()
                    except Exception as ocr_exc:  # pragma: no cover
                        logger.error(
                            "OCR failed on page %d of %s: %s", idx, pdf_path, ocr_exc
                        )

            texts.extend(results)
            logger.debug(
                "Extracted %d pages from %s via pdf2image", len(texts), pdf_path
            )
            return texts
        except Exception as exc:  # pragma: no cover
            logger.warning(
                "pdf2image failed for %s: %s – falling back to pdfplumber",
                pdf_path,
                exc,
            )

    # Fallback: pdfplumber
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp_png:
                page_image = page.to_image(resolution=300)
                page_image.save(tmp_png.name, format="PNG")
                text = pytesseract.image_to_string(
                    Image.open(tmp_png.name), lang=OCR_LANG, timeout=15
                )
                texts.append(text)
                os.unlink(tmp_png.name)
    logger.debug("Extracted %d pages from %s via pdfplumber", len(texts), pdf_path)
    return texts
