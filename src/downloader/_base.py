# ruff: noqa: E402
from __future__ import annotations

"""Core abstractions for Downloader layer.

Every data source should implement `BaseDownloader.download` and return
`list[dict]` that is compatible with its target table.
"""

from abc import ABC, abstractmethod
from datetime import date
import pathlib
from typing import Dict, Generic, List, TypeVar


# -------------------------------------------------------------
# Generic abstractions
# -------------------------------------------------------------

# 汎用Downloader の型パラメータ
T_co = TypeVar("T_co", covariant=True)


class GenericDownloader(ABC, Generic[T_co]):
    """ジェネリックな Downloader 抽象基底クラス。

    戻り値の型を型パラメータ *T_co* で柔軟に指定できるようにすることで、
    ドキュメント(FilePath)系とテーブル(RowDict)系双方の Downloader を
    一貫したインターフェースで表現できるようにする。
    """

    # サブクラスで識別用に上書きする
    name: str = "generic"

    @abstractmethod
    def download(self, target_date: date) -> T_co:  # noqa: D401
        """*target_date* 向けにデータを取得して返す。"""


class BaseDownloader(GenericDownloader[List[Dict[str, object]]]):
    """テーブルへの upsert を目的とした Downloader 基底クラス。"""

    name: str = "base"

    @abstractmethod
    def download(self, target_date: date) -> List[Dict[str, object]]:  # noqa: D401
        """Download data for *target_date* and return list of row dictionaries."""


# -------------------------------------------------------------
# File (document) oriented Downloader
# -------------------------------------------------------------


class FileDownloader(GenericDownloader[List[pathlib.Path]]):
    """ファイル（PDF/ZIP 等）のダウンロードを行う Downloader 基底クラス。"""

    name: str = "file-base"

    @abstractmethod
    def download(self, target_date: date) -> List[pathlib.Path]:  # noqa: D401
        """Download files for *target_date* and return a list of saved paths."""
