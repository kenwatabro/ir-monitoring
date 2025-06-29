from __future__ import annotations

"""Core abstractions for Downloader layer.

Every data source should implement `BaseDownloader.download` and return
`list[dict]` that is compatible with its target table.
"""

from abc import ABC, abstractmethod
from datetime import date
from typing import Dict, List


class BaseDownloader(ABC):
    """Abstract base class for all downloader plugins."""

    name: str = "base"

    @abstractmethod
    def download(self, target_date: date) -> List[Dict[str, object]]:  # noqa: D401
        """Download data for *target_date*.

        Each subclass must return a list of row dictionaries that can be directly
        passed to the corresponding `db.upsert_*` function.  Implementation may
        decide the exact keys/values, but should adhere to guidelines in docs.
        """

    def __repr__(self) -> str:  # pragma: no cover
        return f"<{self.__class__.__name__}:{self.name}>" 