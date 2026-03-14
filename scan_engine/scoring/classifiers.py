"""
Pure classifiers extracted from step12_acceptance.py.

All functions are pure: (inputs) -> output, no side effects.
"""

import math
import re
import pandas as pd


def operating_mode(iv_maturity_level: int) -> str:
    """
    Returns a human-readable Operating_Mode tag that makes the IV data context
    explicit in every READY/CONDITIONAL row.

    Level 1 (<20d): CHART_DRIVEN — no vol edge measurement possible
    Level 2 (20-60d): CHART_DRIVEN — early IV, relative rank unreliable
    Level 3 (60-120d): CHART_ASSISTED — partial IV context, rank directional only
    Level 4 (120-180d): VOL_INFORMED — sufficient history for IV_Rank signals
    Level 5 (180d+):  FULL_CONTEXT — mature surface, regime and rank trustworthy
    """
    _map = {
        1: "CHART_DRIVEN (IMMATURE: <20d IV history — no vol edge measurement)",
        2: "CHART_DRIVEN (EARLY: 20-60d IV history — chart signals primary)",
        3: "CHART_ASSISTED (PARTIAL: 60-120d IV history — IV rank directional only)",
        4: "VOL_INFORMED (DEVELOPING: 120-180d IV history — IV_Rank valid)",
        5: "FULL_CONTEXT (MATURE: 180d+ IV history — full vol surface available)",
    }
    return _map.get(int(iv_maturity_level) if iv_maturity_level else 1,
                    "CHART_DRIVEN (IMMATURE: <20d IV history — no vol edge measurement)")


def dqs_confidence_band(dqs_score, max_band: str = 'MEDIUM') -> str:
    """
    Translate a DQS_Score (0-100) into a confidence_band, capped at max_band.

    Tiers mirror the DQS_Status thresholds:
        DQS >= 75 (Strong)   -> MEDIUM  (or HIGH if max_band allows)
        DQS 50-74 (Eligible) -> LOW
        DQS < 50 (Weak)      -> LOW

    max_band: enforced ceiling -- R2.3a (Acceptable liq) caps at MEDIUM,
              R3.1/R3.2 (Good/Excellent liq) may allow HIGH.
    """
    try:
        dqs = float(dqs_score) if dqs_score is not None and pd.notna(dqs_score) else 0.0
    except (TypeError, ValueError):
        dqs = 0.0

    if dqs >= 75:
        raw = 'HIGH'
    else:
        raw = 'LOW'

    _order = {'LOW': 0, 'MEDIUM': 1, 'HIGH': 2}
    cap = _order.get(max_band, 2)
    if _order.get(raw, 0) > cap:
        return max_band
    return raw


def classify_strategy_type(strategy_name: str) -> str:
    """
    Classify strategy into DIRECTIONAL, INCOME, or VOLATILITY.

    Returns:
        'DIRECTIONAL' | 'INCOME' | 'VOLATILITY' | 'UNKNOWN'
    """
    strategy_name_lower = strategy_name.lower()

    income_keywords = [
        r'\bcovered call\b', r'\bnaked put\b', r'\bcsp\b',
        r'\bbull put spread\b', r'\bbear call spread\b', r'\bcredit spread\b',
        r'\biron condor\b', r'\bbuy-write\b', r'\bcash-secured put\b',
        r'\bpmcc\b',
    ]
    directional_keywords = [
        r'\blong call\b', r'\blong put\b', r'\bleap call\b', r'\bleap put\b',
        r'\bbull call spread\b', r'\bbear put spread\b',
        r'\bcall debit spread\b', r'\bput debit spread\b', r'\bvertical spread\b',
    ]
    volatility_keywords = [
        r'\bstraddle\b', r'\bstrangle\b', r'\bbutterfly\b', r'\bcondor\b',
    ]

    for keyword_regex in income_keywords:
        if re.search(keyword_regex, strategy_name_lower):
            return 'INCOME'
    for keyword_regex in directional_keywords:
        if re.search(keyword_regex, strategy_name_lower):
            return 'DIRECTIONAL'
    for keyword_regex in volatility_keywords:
        if re.search(keyword_regex, strategy_name_lower):
            return 'VOLATILITY'

    return 'UNKNOWN'


def assign_capital_bucket(strategy_type: str, dte, strategy_name: str) -> str:
    """
    Assign Capital_Bucket based on time horizon + structure type.

    TACTICAL:  short-dated directional (DTE <= 60) or volatility strategies
    STRATEGIC: LEAPS or long-dated directional (DTE > 60)
    DEFENSIVE: income strategies (CSP, BW, CC, credit spreads)
    """
    sn = (strategy_name or "").lower()
    if "leap" in sn:
        return "STRATEGIC"
    if strategy_type == "DIRECTIONAL":
        try:
            dte_f = float(dte)
            if math.isnan(dte_f):
                dte_f = 45.0
        except (TypeError, ValueError):
            dte_f = 45.0
        return "STRATEGIC" if dte_f > 60 else "TACTICAL"
    if strategy_type == "INCOME":
        return "DEFENSIVE"
    if strategy_type == "VOLATILITY":
        return "TACTICAL"
    return "TACTICAL"
