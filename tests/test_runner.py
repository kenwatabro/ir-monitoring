from datetime import date

import pytest

from src import runner as runner_module


def dummy_download(_):
    return []


@pytest.mark.parametrize("days", [1, 2])
def test_run_since(monkeypatch, days):
    # Patch download functions to avoid network
    monkeypatch.setattr(runner_module.edinet, "download", dummy_download)
    monkeypatch.setattr(runner_module.tdnet, "download", dummy_download)
    monkeypatch.setattr(runner_module.db_module, "upsert_document", lambda *_args, **_kw: None)

    since = date.today()
    # Should not raise exceptions
    runner_module.run_since(since, days) 