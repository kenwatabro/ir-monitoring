import types

import pytest

from src.parser import xbrl as xbrl_module
from src.parser import ocr as ocr_module


# ---------------------------------------------------------------------------
# XBRL
# ---------------------------------------------------------------------------


class _DummyQName:
    def __init__(self, local_name: str):
        self.localName = local_name


class _DummyFact:
    def __init__(self):
        self.qname = _DummyQName("NetSales")
        self.contextID = "FY2025"
        self.unitID = "JPY"
        self.decimals = 0
        self.value = "1000"


class _DummyModel:
    def __init__(self):
        self.facts = [_DummyFact(), _DummyFact()]

    # arelle API compatibility ------------------------------------------------
    def assertValid(self):  # noqa: D401 – keep same signature as arelle
        return True


class _DummyModelManager:
    @staticmethod
    def initialize(_cntlr):  # noqa: D401
        return _DummyModelManager()

    def load(self, _path):  # noqa: D401
        return _DummyModel()


class _DummyCntlr:
    class Cntlr:  # pragma: no cover – mimic arelle.Cntlr.Cntlr
        def __init__(self, *_, **__):
            pass


@pytest.mark.parametrize("n_facts", [1, 3])
def test_extract_facts(monkeypatch, n_facts):
    """extract_facts should return list of dict matching dummy model."""

    # Prepare dummy model with *n_facts* items
    dummy_model = _DummyModel()
    dummy_model.facts = [_DummyFact() for _ in range(n_facts)]

    class _ParamModelManager(_DummyModelManager):
        def load(self, _path):
            return dummy_model

    # Patch arelle-like components
    monkeypatch.setattr(
        xbrl_module, "Cntlr", types.SimpleNamespace(Cntlr=_DummyCntlr.Cntlr)
    )

    # Patch ModelManager.initialize to return an object whose load() supplies dummy_model
    def _fake_initialize(_cntlr):  # noqa: D401
        return types.SimpleNamespace(load=lambda _path: dummy_model)

    monkeypatch.setattr(
        xbrl_module, "ModelManager", types.SimpleNamespace(initialize=_fake_initialize)
    )

    facts = xbrl_module.extract_facts("dummy.xbrl")
    assert isinstance(facts, list)
    assert len(facts) == n_facts
    assert all("name" in f for f in facts)


# ---------------------------------------------------------------------------
# OCR
# ---------------------------------------------------------------------------


class _DummyPage:
    def to_image(self, resolution=300):  # noqa: D401
        class _DummyImage:
            def save(self, filename, *_, **__):  # noqa: D401
                # Create empty file to satisfy extract_text cleanup
                open(filename, "wb").close()

        return _DummyImage()


class _DummyPDF:
    def __init__(self, n_pages: int = 2):
        self.pages = [_DummyPage() for _ in range(n_pages)]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False


@pytest.mark.parametrize("n_pages", [1, 2, 4])
def test_extract_text(monkeypatch, tmp_path, n_pages):
    """extract_text should return *n_pages* texts via OCR pipeline."""

    # Patch pdfplumber.open to return dummy PDF with *n_pages* pages
    monkeypatch.setattr(
        ocr_module,
        "pdfplumber",
        types.SimpleNamespace(open=lambda *_: _DummyPDF(n_pages)),
    )

    # Patch pytesseract.image_to_string to bypass OCR engine
    monkeypatch.setattr(
        ocr_module,
        "pytesseract",
        types.SimpleNamespace(image_to_string=lambda *_args, **_kw: "dummy text"),
    )

    # Patch PIL.Image.open to avoid real image decoding
    monkeypatch.setattr(
        ocr_module, "Image", types.SimpleNamespace(open=lambda *_args, **_kw: object())
    )

    pdf_path = tmp_path / "sample.pdf"
    pdf_path.write_bytes(b"%PDF-1.4")  # dummy content

    texts = ocr_module.extract_text(pdf_path)
    assert isinstance(texts, list)
    assert len(texts) == n_pages
    assert texts[0] == "dummy text"
