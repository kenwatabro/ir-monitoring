import pathlib
from datetime import date

import pytest

from src.downloader.edinet import EdinetDownloader
from src.downloader.esg import ESGDownloader
from src.downloader.market import MarketDownloader


@pytest.fixture(autouse=True)
def patch_network(monkeypatch):
    """各 Downloader 内部のネットワーク依存関数をスタブ化。"""

    # EDINET
    monkeypatch.setattr(
        "src.downloader.edinet._download_impl",
        lambda _day: [pathlib.Path("/tmp/dummy.zip")],
    )

    # Market
    monkeypatch.setattr(
        "src.downloader.market._download_impl",
        lambda _day, _codes: [{"code_jpx": "0000", "metric": "close", "value": 100}],
    )

    # ESG
    monkeypatch.setattr(
        "src.downloader.esg._download_impl",
        lambda _ticker: [{"code_jpx": None, "metric": "E", "score": 50}],
    )


def test_edinet_downloader():
    dl = EdinetDownloader()
    res = dl.download(date.today())
    assert res and isinstance(res[0], pathlib.Path)


def test_market_downloader():
    dl = MarketDownloader(["0000"])
    res = dl.download(date.today())
    assert res and isinstance(res[0], dict) and res[0]["metric"] == "close"


def test_esg_downloader():
    dl = ESGDownloader("TEST")
    res = dl.download(date.today())
    assert res and res[0]["metric"] == "E"
