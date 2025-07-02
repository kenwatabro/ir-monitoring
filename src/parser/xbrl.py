from __future__ import annotations

import logging
import os
from typing import List

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
                }
            )
        )
    logger.debug("Extracted %d facts from %s", len(facts), xbrl_path)
    return facts
