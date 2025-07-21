from __future__ import annotations

import logging
import os
from typing import List
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from functools import lru_cache
import csv
from typing import Optional, Dict, Any


logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

try:
    from arelle import Cntlr, ModelManager
except ImportError:  # pragma: no cover
    Cntlr = None  # type: ignore
    ModelManager = None  # type: ignore


class XbrlFact(dict):
    """Lightweight fact representation."""


def extract_facts(xbrl_path: str) -> List[XbrlFact]:
    """Extract facts from an XBRL instance file.

    Parameters
    ----------
    xbrl_path: str
        Path to .xbrl instance file.
    """
    if Cntlr is None:
        logger.error("arelle is not installed; cannot parse XBRL")
        return []

    # arelle API as of 2025 no longer supports *unitTest* arg
    try:
        cntlr = Cntlr.Cntlr(hasGui=False)
    except TypeError:
        # Older arelle: parameter-less ctor
        cntlr = Cntlr.Cntlr()

    # arelle の内部 logger に必要な属性をセット
    try:
        cntlr.startLogging(
            logFileName="logToBuffer", logLevel="ERROR", logToBuffer=True
        )  # type: ignore[arg-type]
    except Exception:  # pragma: no cover – 古い arelle 互換
        pass

    model = ModelManager.initialize(cntlr).load(xbrl_path)
    # Some arelle versions don't expose assertValid. Skip if absent.
    if hasattr(model, "assertValid"):
        model.assertValid()

    facts: List[XbrlFact] = []
    for fact in model.facts:
        facts.append(
            XbrlFact(
                {
                    "name": fact.qname.localName,
                    "context": fact.contextID,
                    "unit": fact.unitID,
                    "decimals": fact.decimals,
                    "value": fact.value,
                    # Keep original context object for downstream period parsing (not serializable)
                    "_context_obj": getattr(fact, "context", None),
                }
            )
        )
    logger.debug("Extracted %d facts from %s", len(facts), xbrl_path)
    return facts


# ---------------------------------------------------------------------------
# Star-schema oriented structures
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FinanceFact:
    """Normalized finance fact ready for *fact_finance* table.

    NOTE: *doc_id* は上位層 (processor) でバインドするため、本クラスでは保持しない。
    """

    metric_id: str  # 変換済みメトリック ID (例: NET_SALES)
    tag_name: str  # 元の XBRL タグ名 (localName)
    context_id: str  # XBRL contextRef (FY2025Q4 等)
    period_end: Optional[datetime]
    period_type: Optional[str]  # "instant" | "duration" 等
    unit: Optional[str]
    decimals: Optional[int | str]
    value_raw: float
    value_converted: Optional[float] = None  # 単位倍率適用後 – 未実装


# ---------------------------------------------------------------------------
# Metric mapping utilities
# ---------------------------------------------------------------------------


@lru_cache(maxsize=1)
def _load_metric_map(csv_path: Path | None = None) -> Dict[str, str]:
    """Load *XBRL tag* -> *metric_id* mapping CSV.

    The CSV must contain two columns: `tag` and `metric_id`.
    If *csv_path* is *None* or file does not exist, returns an empty mapping.
    The result is cached (LRU) to avoid re-loading.
    """

    if csv_path is None:
        # default path under project root: *resources/metric_alias.csv*
        csv_path = Path(__file__).resolve().parents[2] / "resources/metric_alias.csv"

    if not csv_path.exists():
        logger.info(
            "Metric alias CSV not found at %s – using identity mapping", csv_path
        )
        return {}

    mapping: Dict[str, str] = {}
    with csv_path.open(newline="", encoding="utf-8") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            tag = row.get("tag") or ""
            metric = row.get("metric_id") or ""
            if tag and metric:
                mapping[tag] = metric
    logger.info("Loaded %d metric aliases from %s", len(mapping), csv_path)
    return mapping


def _map_metric(tag_name: str, metric_map: Dict[str, str]) -> str:
    """Return mapped *metric_id* or fallback to tag name upper-snakecase."""

    metric = metric_map.get(tag_name)
    if metric:
        return metric
    # Fallback: convert CamelCase -> SNAKE_CASE (very naïve)
    snake = []
    for ch in tag_name:
        if ch.isupper() and snake:
            snake.append("_")
        snake.append(ch.upper())
    return "".join(snake)


# ---------------------------------------------------------------------------
# Enhanced extraction APIs
# ---------------------------------------------------------------------------


def extract_finance_facts(
    xbrl_path: str | Path,
    *,
    metric_map_csv: str | Path | None = None,
) -> list[FinanceFact]:
    """Extract *FinanceFact* list from given XBRL instance file.

    This function converts raw arelle *fact* objects into normalized
    `FinanceFact` suitable for loading into the new star-schema (*fact_finance*).

    Parameters
    ----------
    xbrl_path:
        Path to *.xbrl* instance.
    metric_map_csv:
        Optional path to mapping CSV (tag -> metric_id).
    """

    raw_facts: list[XbrlFact] = extract_facts(str(xbrl_path))

    metric_map = _load_metric_map(Path(metric_map_csv) if metric_map_csv else None)

    finance_facts: list[FinanceFact] = []
    for f in raw_facts:
        try:
            tag_name = str(f.get("name"))
            value_raw = float(f.get("value"))  # may raise ValueError
        except (TypeError, ValueError):
            # Skip non-numeric facts
            continue

        metric_id = _map_metric(tag_name, metric_map)

        # Context / period extraction – best-effort using arelle attributes if present
        period_end: Optional[datetime] = None
        period_type: Optional[str] = None

        _ctx: Any = f.get("_context_obj")  # stored by *extract_facts* if available
        if _ctx is not None:
            try:
                if getattr(_ctx, "isInstantPeriod", False):
                    period_type = "instant"
                    period_end = getattr(_ctx, "endDatetime", None)
                else:
                    period_type = "duration"
                    period_end = getattr(_ctx, "endDatetime", None)
            except Exception:  # pragma: no cover – defensive
                pass

        finance_facts.append(
            FinanceFact(
                metric_id=metric_id,
                tag_name=tag_name,
                context_id=str(f.get("context")),
                period_end=period_end,
                period_type=period_type,
                unit=f.get("unit"),
                decimals=f.get("decimals"),
                value_raw=value_raw,
            )
        )

    logger.debug("Converted %d finance facts from %s", len(finance_facts), xbrl_path)
    return finance_facts
