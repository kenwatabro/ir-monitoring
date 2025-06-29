from datetime import date
from typing import List

from src.downloader import market as market_mod


def fake_stooq(code: str, target_date: date):
    return [
        {
            "code_jpx": code,
            "metric": "close",
            "ts_date": target_date,
            "value": 100.0,
            "src": "Stooq",
        }
    ]


def fake_tdnet(target_date: date):
    return []  # no headlines for test


class FakeCreditDownloader:
    def __init__(self, codes: List[str]):
        self.codes = codes

    def download(self, target_date: date):
        return [
            {
                "code_jpx": self.codes[0],
                "metric": "short_margin_balance",
                "ts_date": target_date,
                "value": 1000.0,
                "src": "J-Quants",
            }
        ]


def test_market_downloader(monkeypatch):
    d = date(2024, 7, 1)
    codes = ["7203"]

    # Monkeypatch internal helpers
    monkeypatch.setattr(market_mod, "_fetch_stooq", fake_stooq)
    monkeypatch.setattr(market_mod, "_fetch_tdnet_headlines", fake_tdnet)
    monkeypatch.setattr(market_mod, "JQuantsCreditDownloader", FakeCreditDownloader)

    rows = market_mod.download(d, codes)

    # Should include both Stooq and credit rows
    metrics = {r["metric"] for r in rows}
    assert {"close", "short_margin_balance"}.issubset(metrics)
