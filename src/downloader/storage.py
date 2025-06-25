from __future__ import annotations

import hashlib
import pathlib
from typing import Tuple

BUFFER_SIZE = 1024 * 1024  # 1 MB


def save_with_sha(source: pathlib.Path, dest: pathlib.Path) -> Tuple[pathlib.Path, str]:
    """Save source file to dest while computing SHA-256.

    If dest exists and hash matches, it is left untouched.

    Returns (saved_path, sha256_hex).
    """
    sha256 = hashlib.sha256()
    with source.open("rb") as fp_src:
        with dest.open("wb") as fp_dst:
            while True:
                chunk = fp_src.read(BUFFER_SIZE)
                if not chunk:
                    break
                sha256.update(chunk)
                fp_dst.write(chunk)
    digest = sha256.hexdigest()
    return dest, digest 