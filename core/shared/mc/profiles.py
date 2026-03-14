"""
Strategy profiles for the unified MC engine.

Maps strategy names to P&L model dispatch keys, income/directional
classification, and scenario-specific thresholds.

Unifies 3 separate classifiers:
  - scan_engine/mc_position_sizing.py: _classify_strategy()
  - core/management/mc_management.py: _roll_mc_profile()
  - core/management/mc_management.py: mc_exit_vs_hold() inline classification
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StrategyProfile:
    """
    Describes how to simulate a strategy in the MC engine.

    pnl_model: dispatch key for pnl_models.compute_terminal_pnl()
    is_income: True for short-premium strategies (CC, CSP, BW)
    is_long_premium: True for debit strategies (long call/put, LEAPS)
    assignment_relevant: True if P(assignment) is meaningful
    required_context: fields the P&L model needs from the position row
    optional_context: fields used if available (e.g., cost_basis for BW)
    roll_ev_floor: max debit ($) for roll to still be net-positive
    hold_worth_threshold: P(recovery) threshold for hold to be competitive
    """
    name: str
    pnl_model: str
    is_income: bool
    is_long_premium: bool
    assignment_relevant: bool
    required_context: tuple[str, ...] = ("strike", "premium", "dte")
    optional_context: tuple[str, ...] = ()
    roll_ev_floor: float = -50.0
    hold_worth_threshold: float = 0.45


# ── Profile registry ─────────────────────────────────────────────────────────

PROFILES: dict[str, StrategyProfile] = {
    "LONG_CALL": StrategyProfile(
        name="LONG_CALL",
        pnl_model="long_option",
        is_income=False,
        is_long_premium=True,
        assignment_relevant=False,
        roll_ev_floor=-100.0,
        hold_worth_threshold=0.40,
    ),
    "LONG_PUT": StrategyProfile(
        name="LONG_PUT",
        pnl_model="long_option",
        is_income=False,
        is_long_premium=True,
        assignment_relevant=False,
        roll_ev_floor=-100.0,
        hold_worth_threshold=0.40,
    ),
    "BUY_WRITE": StrategyProfile(
        name="BUY_WRITE",
        pnl_model="stock_plus_short_call",
        is_income=True,
        is_long_premium=False,
        assignment_relevant=True,
        optional_context=("cost_basis",),
        roll_ev_floor=-50.0,
        hold_worth_threshold=0.55,
    ),
    "COVERED_CALL": StrategyProfile(
        name="COVERED_CALL",
        pnl_model="stock_plus_short_call",
        is_income=True,
        is_long_premium=False,
        assignment_relevant=True,
        optional_context=("cost_basis",),
        roll_ev_floor=-50.0,
        hold_worth_threshold=0.55,
    ),
    "CSP": StrategyProfile(
        name="CSP",
        pnl_model="short_put",
        is_income=True,
        is_long_premium=False,
        assignment_relevant=True,
        roll_ev_floor=-30.0,
        hold_worth_threshold=0.50,
    ),
    "PMCC": StrategyProfile(
        name="PMCC",
        pnl_model="pmcc",
        is_income=True,
        is_long_premium=False,
        assignment_relevant=True,
        optional_context=("leap_strike", "net_debit"),
        roll_ev_floor=-75.0,
        hold_worth_threshold=0.45,
    ),
    "MULTI_LEG": StrategyProfile(
        name="MULTI_LEG",
        pnl_model="short_put",
        is_income=True,
        is_long_premium=False,
        assignment_relevant=True,
        roll_ev_floor=-30.0,
        hold_worth_threshold=0.50,
    ),
}

# Alias expansions — all resolve to the same profile
_ALIASES: dict[str, str] = {
    "CC": "COVERED_CALL",
    "CASH_SECURED_PUT": "CSP",
    "SHORT_PUT": "CSP",
    "PUT_CREDIT_SPREAD": "CSP",
    "BULL_PUT_SPREAD": "CSP",
    "CALL_CREDIT_SPREAD": "COVERED_CALL",
    "BEAR_CALL_SPREAD": "COVERED_CALL",
    "LEAP": "LONG_CALL",
    "ULTRA_LEAP": "LONG_CALL",
    "LEAPS": "LONG_CALL",
    "LEAPS_CALL": "LONG_CALL",
    "LEAP_CALL": "LONG_CALL",
    "LEAPS_PUT": "LONG_PUT",
    "LEAP_PUT": "LONG_PUT",
    "BUY_CALL": "LONG_CALL",
    "BUY_PUT": "LONG_PUT",
    "LONG_CALL_DIAGONAL": "LONG_CALL",
    "LONG_PUT_DIAGONAL": "LONG_PUT",
    "STRADDLE": "LONG_CALL",
    "STRANGLE": "LONG_CALL",
    "LONG_STRADDLE": "LONG_CALL",
    "LONG_STRANGLE": "LONG_CALL",
    "COVERED_CALL_DIAGONAL": "COVERED_CALL",
    "IRON_CONDOR": "MULTI_LEG",
    "IRON_BUTTERFLY": "MULTI_LEG",
    "CALL_DIAGONAL": "PMCC",
}

# Keyword fallback for unrecognised strategy strings
_KEYWORD_MAP: list[tuple[tuple[str, ...], str]] = [
    (("PMCC",), "PMCC"),
    (("BUY_WRITE", "COVERED_CALL", "CC"), "COVERED_CALL"),
    (("CSP", "CASH_SECURED", "PUT_SELL"), "CSP"),
    (("LONG_CALL", "BUY_CALL", "LEAPS_CALL", "LEAP_CALL", "LEAPS"), "LONG_CALL"),
    (("LONG_PUT", "BUY_PUT", "LEAPS_PUT", "LEAP_PUT"), "LONG_PUT"),
    (("IRON_CONDOR", "IRON_BUTTERFLY", "SPREAD"), "MULTI_LEG"),
    (("LONG", "LEAP", "STRADDLE", "STRANGLE", "DEBIT"), "LONG_CALL"),
    (("COVERED", "CC_", "CALL_SELL"), "COVERED_CALL"),
]

# Default profile for truly unknown strategies
_DEFAULT = StrategyProfile(
    name="DEFAULT",
    pnl_model="long_option",
    is_income=False,
    is_long_premium=True,
    assignment_relevant=False,
    roll_ev_floor=-50.0,
    hold_worth_threshold=0.45,
)


def resolve_profile(strategy_name: str) -> StrategyProfile:
    """
    Map a strategy name string to a StrategyProfile.

    Resolution order:
      1. Exact match in PROFILES
      2. Alias lookup
      3. Keyword scan (PMCC checked before CC to avoid substring collision)
      4. DEFAULT profile
    """
    s = str(strategy_name).upper().replace("-", "_").replace(" ", "_")

    # 1. Exact match
    if s in PROFILES:
        return PROFILES[s]

    # 2. Alias
    if s in _ALIASES:
        return PROFILES[_ALIASES[s]]

    # 3. Keyword scan
    for keywords, profile_key in _KEYWORD_MAP:
        if any(kw in s for kw in keywords):
            return PROFILES[profile_key]

    return _DEFAULT
