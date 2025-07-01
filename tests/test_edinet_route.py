import pathlib
from datetime import date


import src.downloader.edinet as ed


def test_edinet_routing(monkeypatch, tmp_path):
    day = date(2024, 7, 1)
    sample_list = [
        {"docID": "ZIP1", "docTypeCode": "120", "xbrlFlag": "1"},
        {"docID": "PDF1", "docTypeCode": "140", "xbrlFlag": "0"},
    ]

    monkeypatch.setattr(ed, "_edinet_list", lambda d: sample_list)

    calls = {"zip": [], "pdf": []}

    def fake_zip(doc_id, dest, **kw):
        calls["zip"].append((doc_id, dest))
        dest.touch()

    def fake_pdf(doc_id, dest):
        calls["pdf"].append((doc_id, dest))
        dest.touch()

    monkeypatch.setattr(ed, "_download_single", fake_zip)
    monkeypatch.setattr(ed, "_download_pdf_only", fake_pdf)
    monkeypatch.setattr(ed, "RAW_DIR", pathlib.Path(tmp_path))

    paths = ed.EdinetDownloader().download(day)

    assert len(paths) == 2
    assert calls["zip"][0][0] == "ZIP1"
    assert calls["pdf"][0][0] == "PDF1"
