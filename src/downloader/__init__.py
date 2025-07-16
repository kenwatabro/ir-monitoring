"""Downloader subpackage.

主に外部 API / ウェブサイトから生データを取得する各 Downloader を提供する。

環境変数ベースの認証トークン（FRED_API_KEY, ESTAT_APP_ID, JQUANTS_REFRESH_TOKEN
など）が多数存在するため、`.env` ファイルを利用している環境では *dotenv* を
自動ロードしておく方が便利である。ここで一度だけ `load_dotenv()` を呼び出す。
"""

from __future__ import annotations

import logging
import os

# ---------------------------------------------------------------------------
# Load .env at package import time (silently if file not present)
# ---------------------------------------------------------------------------

try:
    from dotenv import load_dotenv

    # Search upwards from project root; fallback to cwd
    # This mirrors typical `python-dotenv` behaviour when no path specified.
    load_dotenv()  # noqa: S607 – safe side-effect
except ImportError:  # pragma: no cover – optional dependency
    logging.getLogger(__name__).debug(
        "python-dotenv is not installed; skipping automatic .env loading"
    )

# Export commonly used env vars for sub-modules (optional helper)
FRED_API_KEY = os.getenv("FRED_API_KEY")
ESTAT_APP_ID = os.getenv("ESTAT_APP_ID")
JQUANTS_REFRESH_TOKEN = os.getenv("JQUANTS_REFRESH_TOKEN")

# Re-export sub-modules for convenience
from . import fred_provider  # noqa: F401,E402
from . import edinet, esg, macro, market, tdnet  # noqa: F401,E402
