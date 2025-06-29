from datetime import date

import responses

from src.downloader.macro import MacroAggregator

ESTAT_URL = "https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData"


def _estat_dummy_response(series_id: str, day: date):
    time_str = day.isoformat()
    return {
        "GET_STATS_DATA": {
            "STAT_DATA": {
                "CLASS_OBJ": [],
                "VALUE": [
                    {
                        "time": time_str,
                        "value": "123.4",
                    }
                ],
            }
        }
    }


@responses.activate
def test_macro_estat_single():
    """MacroAggregator should parse e-Stat JSON and return value list."""

    d = date(2024, 7, 1)
    series_code = "estat:TEST123"

    # Register mock HTTP
    body = _estat_dummy_response("TEST123", d)
    responses.add(
        responses.GET,
        ESTAT_URL,
        json=body,
        status=200,
    )

    # Env variable to target our test series only
    import os

    os.environ["MACRO_SERIES"] = series_code
    os.environ["ESTAT_APP_ID"] = "DUMMY"  # required by downloader

    aggr = MacroAggregator()
    rows = aggr.download(d)
    assert rows == [
        {
            "series_id": "TEST123",
            "ts_date": d,
            "value": 123.4,
            "src": "eStat",
        }
    ]
