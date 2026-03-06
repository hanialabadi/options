"""
Roll Candidate Engine
=====================
When doctrine decides ROLL is the right action, this engine answers:
    "Roll to WHAT — and at what cost?"

It fetches the live option chain for the underlying, filters for contracts
that match the thesis (strategy type + directional bias + DTE window),
scores each candidate on delta alignment, liquidity, and cost-effectiveness,
and returns the top 3 ranked candidates with execution detail.

Design principles:
  - Only called when Action=ROLL (never speculatively)
  - Reuses existing SchwabClient (no new auth)
  - Respects the roll doctrine per strategy:
      LONG_CALL / LONG_PUT : roll to lower IV or further DTE, same directional delta
      BUY_WRITE / COVERED_CALL : roll to next cycle OTM call, preserve income
      SHORT_PUT : roll down-and-out to reduce assignment risk
  - Output: Roll_Candidate_1/2/3 columns with strike, expiry, mid price, delta,
    IV, OI, and cost-to-roll (net debit/credit)
  - Market-hours aware: if market closed, returns best estimate from last chain
    (logged as stale)

McMillan Ch.3 (Rolling Covered Calls) / Ch.4 (Managing Long Options) /
Passarelli Ch.6 (Roll Management) / Natenberg Ch.5 (Vol Edge)
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from core.management.cycle1.identity.constants import FIDELITY_MARGIN_RATE

logger = logging.getLogger(__name__)

# The yield scoring benchmark: a roll candidate must earn at least the Fidelity margin rate
# (10.375%/yr) to cover carrying cost. Earning 2× the rate = excellent (score=1.0).
# McMillan Ch.3: "Any roll that fails to cover the financing cost is negative carry — don't do it."
_YIELD_BENCHMARK_EXCELLENT = FIDELITY_MARGIN_RATE * 2   # ~20.75%/yr = score 1.0
_YIELD_BENCHMARK_NEUTRAL   = FIDELITY_MARGIN_RATE       # ~10.375%/yr = score 0.5 (just covering carry)

# ── Roll mode constants ────────────────────────────────────────────────────────
# Roll mode is determined by the current position's delta at the time doctrine fires.
# PRE_ITM   : delta 0.55–0.70 — roll window is open, credit still available.
#             Standard DTE cycle; prioritize credit rolls above cost basis.
# EMERGENCY : delta > 0.70 — deep ITM, extrinsic nearly gone.
#             Must extend DTE to 45–150 to find any strike above basis for a
#             small debit or break-even roll. Near-dated rolls are pure debit.
# NORMAL    : delta < 0.55 — discretionary roll (50% premium capture, etc.)
# WEEKLY          : Structural fragility detected. Two triggers (either → WEEKLY):
#                   1. Equity_Integrity = BROKEN  → unconditional (structure already failed)
#                   2. Equity_Integrity = WEAKENING + earnings ≤45d or unknown
#                   Fragile position — roll shorter (7–21 DTE) to minimize commitment
#                   and preserve flexibility. Cannot afford to be locked in 45+ days
#                   with structural deterioration already underway.
#                   McMillan Ch.3: "When a position is structurally impaired, shorter
#                   cycles reduce compounding risk — don't over-commit with a long roll."
#
# BROKEN_RECOVERY  : Structural failure + gamma dominance + IV < realized vol.
#                   Three conditions (all required → BROKEN_RECOVERY, overrides WEEKLY):
#                   1. Equity_Integrity = BROKEN
#                   2. Gamma drag > threshold × theta:
#                        1.8× when Gamma_ROC_3D ≤ 0.25 (structurally elevated)
#                        2.4× when Gamma_ROC_3D > 0.25 (spike guard — intraday artifact)
#                   3. IV < HV  (selling vol below realized — negative edge)
#                   Weekly is wrong here: gamma ∝ 1/√T, so 30–45 DTE cuts gamma drag
#                   significantly more than theta. Sacrifices speed for survivability.
#                   McMillan Ch.3: "Reduce basis, but do not fight the tape with short
#                   gamma when structure is broken."
#                   Natenberg Ch.7: "Short gamma when IV < realized vol is structurally
#                   negative expectancy — extend duration to reduce daily drag."
_ROLL_MODE_NORMAL          = "NORMAL"
_ROLL_MODE_PRE_ITM         = "PRE_ITM"          # 0.55 < delta ≤ 0.70
_ROLL_MODE_EMERGENCY       = "EMERGENCY"         # delta > 0.70
_ROLL_MODE_WEEKLY          = "WEEKLY"            # fragile: BROKEN or WEAKENING + earnings ≤45d
_ROLL_MODE_BROKEN_RECOVERY = "BROKEN_RECOVERY"   # BROKEN + gamma dominant + IV < HV
_ROLL_MODE_INCOME_SAME     = "INCOME_SAME"       # SIDEWAYS_INCOME: prefer same-strike credit rolls

# ── Strategy roll configuration ───────────────────────────────────────────────

# (min_dte, max_dte, target_dte) for roll expiry selection
# NORMAL mode — standard income cycle
_ROLL_DTE_WINDOWS: Dict[str, Tuple[int, int, int]] = {
    "LONG_CALL":     (45, 180, 90),   # Roll out enough to buy time; not so far it's cheap
    "LONG_PUT":      (45, 180, 90),
    "BUY_WRITE":     (21, 60,  45),   # Next CC cycle — one to two months out
    "COVERED_CALL":  (21, 60,  45),
    "SHORT_PUT":     (21, 60,  45),   # Roll down-and-out: lower strike, further DTE
    "CSP":           (21, 60,  45),
    "LEAP":          (270, 730, 365), # Deeper LEAP roll
}

# PRE_ITM mode — same DTE cycle but bias toward above-basis credit strikes
# DTE window unchanged; filtering and scoring differ (see _select_roll_candidates)
_ROLL_DTE_WINDOWS_PRE_ITM: Dict[str, Tuple[int, int, int]] = {
    "BUY_WRITE":     (21, 75,  45),   # Slightly wider — find a strike above basis
    "COVERED_CALL":  (21, 75,  45),
    "SHORT_PUT":     (21, 60,  45),
    "CSP":           (21, 60,  45),
}

# EMERGENCY mode — extended DTE to find any viable above-basis strike
# Near-dated is pure debit; must go out 45–150 DTE to find credit or break-even
_ROLL_DTE_WINDOWS_EMERGENCY: Dict[str, Tuple[int, int, int]] = {
    "BUY_WRITE":     (45, 150, 90),   # Extended: skip near-dated pure-debit territory
    "COVERED_CALL":  (45, 150, 90),
    "SHORT_PUT":     (45, 120, 75),
    "CSP":           (45, 120, 75),
}

# WEEKLY mode — short-cycle rolls for fragile positions with structural deterioration
# Triggers: Equity_Integrity=BROKEN (unconditional) OR WEAKENING + earnings ≤45d.
# Shorter commitment = maximum flexibility to reassess / exit / pivot.
# McMillan Ch.3: do not over-commit when underlying is structurally impaired.
_ROLL_DTE_WINDOWS_WEEKLY: Dict[str, Tuple[int, int, int]] = {
    "BUY_WRITE":     (7, 21,  14),   # One-to-two week cycles only
    "COVERED_CALL":  (7, 21,  14),
    "SHORT_PUT":     (7, 21,  14),
    "CSP":           (7, 21,  14),
}

# Target delta ranges for roll candidates (call delta is positive)
_ROLL_DELTA_TARGETS: Dict[str, Tuple[float, float]] = {
    "LONG_CALL":     (0.35, 0.65),   # Stay near ATM — roll to regain delta efficiency
    "LONG_PUT":      (-0.65, -0.35),
    "BUY_WRITE":     (0.20, 0.40),   # OTM call for income
    "COVERED_CALL":  (0.20, 0.40),
    "SHORT_PUT":     (-0.35, -0.15), # Lower strike = less assignment risk
    "CSP":           (-0.35, -0.15),
    "LEAP":          (0.60, 0.85),   # Roll to ITM/deep LEAP to preserve intrinsic
}

# PRE_ITM delta targets — tighter OTM range, ensure new strike has room
_ROLL_DELTA_TARGETS_PRE_ITM: Dict[str, Tuple[float, float]] = {
    "BUY_WRITE":     (0.20, 0.40),   # Same — still OTM but must be above basis
    "COVERED_CALL":  (0.20, 0.40),
}

# EMERGENCY delta targets — accept slightly higher delta (less OTM) to find viable strikes
# With stock already deep ITM on current strike, new strikes above basis will be moderately ITM
_ROLL_DELTA_TARGETS_EMERGENCY: Dict[str, Tuple[float, float]] = {
    "BUY_WRITE":     (0.20, 0.55),   # Wider — accept up to delta 0.55 to find above-basis strikes
    "COVERED_CALL":  (0.20, 0.55),
    "SHORT_PUT":     (-0.45, -0.15),
    "CSP":           (-0.45, -0.15),
}

# WEEKLY mode delta targets — standard OTM range (don't tighten, just shorten DTE)
# Keep OTM positioning for income; the weekly is about cycle length, not strike placement.
_ROLL_DELTA_TARGETS_WEEKLY: Dict[str, Tuple[float, float]] = {
    "BUY_WRITE":     (0.20, 0.40),
    "COVERED_CALL":  (0.20, 0.40),
    "SHORT_PUT":     (-0.35, -0.15),
    "CSP":           (-0.35, -0.15),
}

# BROKEN_RECOVERY mode — 30–45 DTE, near-ATM delta.
# Gamma ∝ 1/√T: moving from 14 DTE to 35 DTE cuts gamma drag by ~√(35/14) ≈ 1.6×
# while theta drops by only ~30%. Net: better carry per unit of gamma risk.
# Near-ATM (delta 0.25–0.40) collects more premium to offset negative IV/HV edge.
# Natenberg Ch.7: "When IV < realized, maximize theta/gamma ratio by extending DTE."
_ROLL_DTE_WINDOWS_BROKEN_RECOVERY: Dict[str, Tuple[int, int, int]] = {
    "BUY_WRITE":     (28, 60, 35),   # 4–8 weeks: enough to outrun gamma, not so far it's cheap
    "COVERED_CALL":  (28, 60, 35),
    "SHORT_PUT":     (28, 60, 35),
    "CSP":           (28, 60, 35),
}

_ROLL_DELTA_TARGETS_BROKEN_RECOVERY: Dict[str, Tuple[float, float]] = {
    "BUY_WRITE":     (0.25, 0.40),   # Near-ATM: higher premium to offset IV/HV disadvantage
    "COVERED_CALL":  (0.25, 0.40),
    "SHORT_PUT":     (-0.40, -0.20),
    "CSP":           (-0.40, -0.20),
}

# INCOME_SAME mode — SIDEWAYS_INCOME regime: prefer same-strike credit rolls.
# McMillan Ch.3: in range-bound behaviour, roll OUT in time, not UP in strike.
# Accept delta up to 0.55 so same-strike ATM/near-ATM candidates are valid.
# Standard 30-45 DTE: next monthly cycle, same income tempo.
_ROLL_DTE_WINDOWS_INCOME_SAME: Dict[str, Tuple[int, int, int]] = {
    "BUY_WRITE":     (21, 60,  45),
    "COVERED_CALL":  (21, 60,  45),
    "SHORT_PUT":     (21, 60,  45),
    "CSP":           (21, 60,  45),
}

_ROLL_DELTA_TARGETS_INCOME_SAME: Dict[str, Tuple[float, float]] = {
    "BUY_WRITE":     (0.20, 0.55),   # Accept same-strike near-ATM → credit roll
    "COVERED_CALL":  (0.20, 0.55),
    "SHORT_PUT":     (-0.55, -0.15), # Accept higher-delta (nearer) put strikes
    "CSP":           (-0.55, -0.15),
}

# How many candidates to return
_TOP_N = 3

# Schwab call throttle
_CHAIN_DELAY_SEC = 0.5


def find_roll_candidates(
    df: pd.DataFrame,
    schwab_client,
    session_chain_cache: Optional[Dict[str, dict]] = None,
    action_mask: Optional[pd.Series] = None,
) -> pd.DataFrame:
    """
    For every row where Action=ROLL (or covered by action_mask), fetch the chain
    and attach top-3 roll candidates as Roll_Candidate_1 / Roll_Candidate_2 /
    Roll_Candidate_3 JSON columns.

    action_mask: optional boolean Series override — use when pre-staging candidates
        for HOLD rows with active blocking conditions (dead_cat_bounce, iv_depressed)
        so they are ready the moment the condition resolves.

    session_chain_cache: shared dict {ticker: chain_response} passed from
        LiveGreeksProvider to avoid duplicate chain calls in the same run.

    Returns df with added Roll_Candidate_* columns.
    """
    df = df.copy()

    roll_mask = action_mask if action_mask is not None else (df["Action"] == "ROLL")
    if not roll_mask.any():
        return df

    # Ensure output columns exist
    for i in (1, 2, 3):
        col = f"Roll_Candidate_{i}"
        if col not in df.columns:
            df[col] = None

    session_cache = session_chain_cache or {}
    option_roll_rows = df[roll_mask & (df["AssetType"] == "OPTION")]

    # BUY_WRITE stock legs flagged for candidates (HOLD+HIGH, EXIT) may not have
    # a matching option leg in the mask. Resolve by finding the paired option leg
    # from the same TradeID and adding it to the processing set.
    _stock_flagged = df[roll_mask & (df["AssetType"] == "STOCK")]
    _extra_option_idxs = []
    if not _stock_flagged.empty and "TradeID" in df.columns:
        for _sidx in _stock_flagged.index:
            _tid = df.loc[_sidx, "TradeID"]
            _paired_opts = df[
                (df["TradeID"] == _tid) & (df["AssetType"] == "OPTION")
            ]
            for _oidx in _paired_opts.index:
                if _oidx not in option_roll_rows.index:
                    _extra_option_idxs.append(_oidx)
    if _extra_option_idxs:
        # Deduplicate by index (drop_duplicates() fails on DataFrames with list columns)
        _all_idxs = list(option_roll_rows.index) + _extra_option_idxs
        _unique_idxs = list(dict.fromkeys(_all_idxs))  # preserve order, remove dupes
        option_roll_rows = df.loc[_unique_idxs]

    processed_tickers = set()

    for idx in option_roll_rows.index:
        row      = df.loc[idx]
        ticker   = str(row.get("Underlying_Ticker", "") or "")
        strategy = str(row.get("Strategy", "") or "")
        cp       = str(row.get("Call/Put", "") or "").upper()
        strike        = float(row.get("Strike", 0) or 0)
        dte           = float(row.get("DTE", 0) or 0)
        ul_price      = float(row.get("UL Last", 0) or 0)
        current_iv    = float(row.get("IV_Now") if pd.notna(row.get("IV_Now")) else
                              row.get("IV_30D", 0) or 0)
        net_cost_basis = float(row.get("Net_Cost_Basis_Per_Share", 0) or 0)
        cum_premium   = float(row.get("Cumulative_Premium_Collected", 0) or 0)
        hv_20d        = float(row.get("HV_20D", 0) or 0)

        # Net_Cost_Basis_Per_Share is set by BuyWriteLedger on the STOCK leg only.
        # The OPTION leg row has 0/NaN → breakeven_after_roll computes as None → shows "?".
        # Two-pass fallback mirrors BuyWriteLedger.enrich():
        #   1. Same-TradeID stock leg (normal BUY_WRITE pairing)
        #   2. Any STOCK leg for same ticker (handles COVERED_CALL option-only positions)
        if net_cost_basis == 0 and "TradeID" in df.columns:
            _trade_id = row.get("TradeID")
            _stock_legs = df[(df["TradeID"] == _trade_id) & (df["AssetType"] == "STOCK")]
            if _stock_legs.empty and ticker and "Underlying_Ticker" in df.columns:
                # Pass 2: any STOCK row for same ticker with a valid basis
                _ticker_stocks = df[
                    (df["Underlying_Ticker"] == ticker) &
                    (df["AssetType"] == "STOCK") &
                    df["Net_Cost_Basis_Per_Share"].notna()
                ]
                if not _ticker_stocks.empty:
                    _stock_legs = _ticker_stocks
            if not _stock_legs.empty:
                _ncb = float(_stock_legs.iloc[0].get("Net_Cost_Basis_Per_Share", 0) or 0)
                if _ncb > 0:
                    net_cost_basis = _ncb
            if net_cost_basis == 0 and not _stock_legs.empty:
                # Also lift cum_premium from stock leg if missing on option row
                _cp = float(_stock_legs.iloc[0].get("Cumulative_Premium_Collected", 0) or 0)
                if _cp > 0:
                    cum_premium = _cp

        if not ticker or ul_price == 0:
            continue

        # Determine call/put from strategy if not explicit
        if not cp or cp not in ("C", "P", "CALL", "PUT"):
            cp = "C" if "CALL" in strategy or strategy in ("BUY_WRITE", "COVERED_CALL") else "P"
        cp_normalized = cp[0]  # "C" or "P"

        # Get roll config for this strategy
        strategy_key = _strategy_key(strategy, dte)

        # ── Roll mode detection ────────────────────────────────────────────────
        # The current position's delta determines WHICH search mode to use.
        # This is the core architectural split: pre-ITM is optimization mode
        # (credit-first, standard DTE); emergency is rescue mode (extended DTE,
        # above-basis hard filter, may produce no-viable-roll verdict).
        current_delta = abs(float(row.get("Delta", row.get("Short_Call_Delta", 0)) or 0))
        if current_delta > 0.70:
            roll_mode = _ROLL_MODE_EMERGENCY
        elif current_delta > 0.55:
            roll_mode = _ROLL_MODE_PRE_ITM
        else:
            roll_mode = _ROLL_MODE_NORMAL

        # ── WEEKLY mode override (fragility gate) ─────────────────────────────
        # Fires when the underlying stock is structurally impaired AND there is
        # earnings risk — either within the standard 45-day window OR unknown.
        # In this condition, committing to a 21–60 DTE roll is imprudent:
        # the position may need to be unwound or restructured before the new
        # expiry if deterioration continues.
        # Overrides NORMAL and PRE_ITM — do NOT override EMERGENCY (which is
        # in rescue mode with extended-DTE above-basis filtering that must be
        # preserved). At high delta (PRE_ITM), structural fragility still means
        # shorter commitment — WEEKLY is the correct override even when delta
        # is elevated (0.55–0.70), because DTE is not the crisis, structure is.
        # Guard: WEEKLY mode only makes sense for positions with DTE within
        # weekly-cycle range (≤90 DTE). LEAPS positions (DTE 200+) should not
        # compress to 7–21 DTE cycles regardless of structural fragility —
        # the investment thesis is measured in years, not weeks.
        _weekly_eligible = dte <= 90
        if roll_mode in (_ROLL_MODE_NORMAL, _ROLL_MODE_PRE_ITM) and _weekly_eligible:
            _ei_state = str(row.get("Equity_Integrity_State", "") or "").strip().upper()
            if not _ei_state or _ei_state == "HEALTHY":
                # Equity_Integrity_State is only populated on STOCK legs; OPTION legs
                # carry HEALTHY by default. Two-pass lookup:
                # 1. Same-TradeID stock leg (paired BUY_WRITE / COVERED_CALL)
                # 2. Any STOCK row for same ticker (isolated short call over same underlying)
                _ei_from_stock = ""
                if "TradeID" in df.columns:
                    _trade_id_ei = row.get("TradeID")
                    _stock_ei = df[(df["TradeID"] == _trade_id_ei) & (df["AssetType"] == "STOCK")]
                    if not _stock_ei.empty:
                        _ei_from_stock = str(_stock_ei.iloc[0].get("Equity_Integrity_State", "") or "").strip().upper()
                if not _ei_from_stock and ticker and "Underlying_Ticker" in df.columns:
                    # No same-trade stock leg — check any stock row for this ticker
                    # (handles isolated short calls where the stock leg has a different TradeID)
                    _ticker_stock = df[
                        (df["Underlying_Ticker"] == ticker) & (df["AssetType"] == "STOCK")
                    ]
                    if not _ticker_stock.empty:
                        _ei_from_stock = str(_ticker_stock.iloc[0].get("Equity_Integrity_State", "") or "").strip().upper()
                if not _ei_from_stock:
                    # Last resort: query DuckDB for the most recent Equity_Integrity_State
                    # on any stock leg for this ticker. This handles the case where the
                    # stock leg was present in a prior run but the broker export now shows
                    # only the option leg (position restructured / partial export).
                    try:
                        import duckdb as _ddb
                        from pathlib import Path as _Path
                        _db_path = _Path(__file__).parents[4] / "data" / "pipeline.duckdb"
                        _con = _ddb.connect(str(_db_path), read_only=True)
                        _hist_ei = _con.execute("""
                            SELECT Equity_Integrity_State
                            FROM management_recommendations
                            WHERE Underlying_Ticker = ?
                              AND AssetType = 'STOCK'
                              AND Equity_Integrity_State IS NOT NULL
                              AND Equity_Integrity_State NOT IN ('', 'HEALTHY', 'N/A')
                            ORDER BY Snapshot_TS DESC
                            LIMIT 1
                        """, [ticker]).fetchdf()
                        _con.close()
                        if not _hist_ei.empty:
                            _ei_from_stock = str(_hist_ei.iloc[0]["Equity_Integrity_State"] or "").strip().upper()
                    except Exception:
                        pass
                if _ei_from_stock in ("BROKEN", "WEAKENING"):
                    _ei_state = _ei_from_stock
                    logger.debug(
                        f"[RollEngine] {ticker} EI inherited from stock leg: {_ei_state}"
                    )
            if _ei_state == "BROKEN":
                # Structural failure confirmed. Default: WEEKLY (shortest commitment).
                # Exception: BROKEN_RECOVERY overrides WEEKLY when ALL three hold:
                #   1. Gamma drag > threshold × theta (1.8× stable, 2.4× spiking)
                #   2. IV < HV (IV/HV < 0.90)  — selling vol below realized: negative edge
                #   3. Strike within 30% of spot — gamma is physically meaningful
                # In this regime, weekly DTE makes the gamma problem WORSE (gamma ∝ 1/√T).
                # Moving to 30–45 DTE cuts daily gamma drag without proportionally
                # cutting theta. Sacrifices recovery speed for structural survivability.
                # Natenberg Ch.7: "Short gamma when IV < realized is negative expectancy."
                # ChatGPT Mode B: broken structure + HV > IV → move to 30–45 DTE.
                # Spike guard: if Gamma_ROC_3D > 0.25 (gamma accelerating = intraday spike),
                # raise threshold to 2.4× so BROKEN_RECOVERY doesn't flip on a single
                # day's gamma jump. Gamma spikes intraday; structural drag is persistent.
                import math as _math_rc
                _rc_theta   = abs(float(row.get("Theta", 0) or 0))
                _rc_gamma   = abs(float(row.get("Gamma", 0) or 0))
                _rc_hv      = float(row.get("HV_20D", 0.20) or 0.20)
                _rc_iv      = float(row.get("IV", 0) or row.get("IV_30D", 0) or 0)
                if _rc_hv >= 1.0: _rc_hv /= 100.0
                if _rc_iv >= 1.0: _rc_iv /= 100.0
                _rc_spot    = float(row.get("Last", 0) or row.get("UL Last", 0) or 0)
                _rc_strike  = float(row.get("Strike", 0) or 0)
                _rc_sigma   = _rc_spot * (_rc_hv / _math_rc.sqrt(252)) if _rc_spot > 0 else 0
                _rc_gdrag   = 0.5 * _rc_gamma * (_rc_sigma ** 2)
                _rc_otm_pct = (abs(_rc_strike - _rc_spot) / _rc_spot
                               if _rc_strike > 0 and _rc_spot > 0 else 1.0)

                # Spike guard (ChatGPT: smooth gamma/theta to prevent daily regime flipping).
                # If Gamma_ROC_3D > 0.25, gamma is accelerating sharply — likely an
                # intraday or single-session spike, not a persistent structural regime.
                # Raise the threshold to 2.4× (vs normal 1.8×) so BROKEN_RECOVERY only
                # fires when the drag is confirmed structurally elevated, not just today's peak.
                # Gamma_ROC_3D ≤ 0.25 → stable/declining gamma → use normal 1.8× threshold.
                _rc_gamma_roc3 = float(row.get("Gamma_ROC_3D") or 0)
                _rc_gdrag_threshold = (
                    2.4 if _rc_gamma_roc3 > 0.25  # spike guard: require stronger confirmation
                    else 1.8                        # structurally elevated: normal threshold
                )

                _rc_gamma_dominant = (
                    _rc_theta > 0
                    and _rc_gdrag > _rc_theta * _rc_gdrag_threshold
                    and _rc_otm_pct <= 0.30
                )
                _rc_iv_depressed = (
                    _rc_iv > 0 and _rc_hv > 0
                    and (_rc_iv / _rc_hv) < 0.90      # IV at least 10% below HV
                )
                if _rc_gamma_dominant and _rc_iv_depressed:
                    roll_mode = _ROLL_MODE_BROKEN_RECOVERY
                    logger.info(
                        f"[RollEngine] {ticker} → BROKEN_RECOVERY mode: "
                        f"BROKEN + gamma_drag={_rc_gdrag:.4f} ({_rc_gdrag/_rc_theta:.1f}×theta, "
                        f"threshold={_rc_gdrag_threshold}×, Gamma_ROC_3D={_rc_gamma_roc3:.2f}) "
                        f"+ IV/HV={_rc_iv/_rc_hv:.2f} (depressed)"
                    )
                else:
                    roll_mode = _ROLL_MODE_WEEKLY
                    logger.info(
                        f"[RollEngine] {ticker} → WEEKLY mode: Equity_Integrity=BROKEN "
                        f"(gamma_dominant={_rc_gamma_dominant} [threshold={_rc_gdrag_threshold}×], "
                        f"iv_depressed={_rc_iv_depressed}, Gamma_ROC_3D={_rc_gamma_roc3:.2f})"
                    )
            elif _ei_state == "WEAKENING":
                # Deteriorating but not yet broken — only go weekly if earnings
                # compound the risk (≤45d) or earnings date is unknown.
                _earnings_date_raw = row.get("Earnings Date")
                _earnings_risk = True  # default: assume risk if date unknown
                if _earnings_date_raw not in (None, "", "nan", "N/A") and not (
                    isinstance(_earnings_date_raw, float) and pd.isna(_earnings_date_raw)
                ):
                    try:
                        _ed = pd.to_datetime(str(_earnings_date_raw), errors="coerce")
                        if pd.notna(_ed):
                            _days_to_earnings = (_ed - pd.Timestamp.now()).days
                            _earnings_risk = _days_to_earnings <= 45
                    except Exception:
                        _earnings_risk = True  # parse failure = treat as unknown = risk
                if _earnings_risk:
                    roll_mode = _ROLL_MODE_WEEKLY
                    logger.info(
                        f"[RollEngine] {ticker} → WEEKLY mode: "
                        f"Equity_Integrity=WEAKENING, earnings_risk={_earnings_risk}"
                    )

        # ── Income-friendly regimes: prefer same-strike credit rolls ────────
        # Position_Regime is computed by compute_position_trajectory() (Cycle 2.85).
        # When the position is in a range-bound income regime, rolling UP in strike
        # (standard OTM delta targeting) produces debits that erode collected premium.
        # Instead, roll OUT in time at the same or near strike to collect credit.
        # McMillan Ch.3: "Income overlay profits from range-bound behaviour — don't
        # chase strikes when the stock isn't trending."
        # Applies to both SIDEWAYS_INCOME and MEAN_REVERSION — both are income-friendly.
        # Only overrides NORMAL or PRE_ITM — emergency/weekly modes have structural priority.
        if roll_mode in (_ROLL_MODE_NORMAL, _ROLL_MODE_PRE_ITM):
            _pos_regime = str(row.get("Position_Regime", "") or "").strip()
            if _pos_regime in ("SIDEWAYS_INCOME", "MEAN_REVERSION"):
                roll_mode = _ROLL_MODE_INCOME_SAME
                logger.info(
                    f"[RollEngine] {ticker} → INCOME_SAME mode: "
                    f"Position_Regime={_pos_regime} — prefer same-strike credit rolls"
                )

        # Select DTE window and delta range based on mode
        if roll_mode == _ROLL_MODE_EMERGENCY:
            dte_window  = _ROLL_DTE_WINDOWS_EMERGENCY.get(strategy_key,
                            _ROLL_DTE_WINDOWS.get(strategy_key, (45, 150, 90)))
            delta_range = _ROLL_DELTA_TARGETS_EMERGENCY.get(strategy_key,
                            _ROLL_DELTA_TARGETS.get(strategy_key, (0.25, 0.55)))
        elif roll_mode == _ROLL_MODE_PRE_ITM:
            dte_window  = _ROLL_DTE_WINDOWS_PRE_ITM.get(strategy_key,
                            _ROLL_DTE_WINDOWS.get(strategy_key, (21, 75, 45)))
            delta_range = _ROLL_DELTA_TARGETS_PRE_ITM.get(strategy_key,
                            _ROLL_DELTA_TARGETS.get(strategy_key, (0.20, 0.40)))
        elif roll_mode == _ROLL_MODE_BROKEN_RECOVERY:
            # BROKEN + gamma dominant + IV < HV: extend DTE to cut gamma drag.
            # Gamma ∝ 1/√T — doubling DTE from 14→35d cuts gamma by ~√2.5 ≈ 1.6×.
            # Near-ATM delta collects more premium to offset the IV/HV disadvantage.
            dte_window  = _ROLL_DTE_WINDOWS_BROKEN_RECOVERY.get(strategy_key,
                            _ROLL_DTE_WINDOWS.get(strategy_key, (28, 60, 35)))
            delta_range = _ROLL_DELTA_TARGETS_BROKEN_RECOVERY.get(strategy_key,
                            _ROLL_DELTA_TARGETS.get(strategy_key, (0.25, 0.40)))
        elif roll_mode == _ROLL_MODE_WEEKLY:
            dte_window  = _ROLL_DTE_WINDOWS_WEEKLY.get(strategy_key,
                            _ROLL_DTE_WINDOWS.get(strategy_key, (7, 21, 14)))
            delta_range = _ROLL_DELTA_TARGETS_WEEKLY.get(strategy_key,
                            _ROLL_DELTA_TARGETS.get(strategy_key, (0.20, 0.40)))
        elif roll_mode == _ROLL_MODE_INCOME_SAME:
            dte_window  = _ROLL_DTE_WINDOWS_INCOME_SAME.get(strategy_key,
                            _ROLL_DTE_WINDOWS.get(strategy_key, (21, 60, 45)))
            delta_range = _ROLL_DELTA_TARGETS_INCOME_SAME.get(strategy_key,
                            _ROLL_DELTA_TARGETS.get(strategy_key, (0.20, 0.55)))
        else:
            dte_window  = _ROLL_DTE_WINDOWS.get(strategy_key, (30, 120, 60))
            delta_range = _ROLL_DELTA_TARGETS.get(strategy_key, (0.25, 0.50))

        # ── DTE window extension when current_dte is near max_dte ──────────
        # Problem: when current_dte is already close to max_dte, the "must roll OUT"
        # filter (actual_dte > current_dte) + max_dte ceiling leaves a window so
        # narrow that no available expiry falls within it.
        # Example: WEEKLY mode (7, 21, 14), current_dte=17 → only DTE 18–21 survive.
        # If the next available weekly expiry is at DTE 24, zero candidates pass.
        # Fix: extend max_dte when the remaining window is < 7 DTE. This preserves
        # the short-cycle INTENT while ensuring at least one cycle is reachable.
        # The target_dte stays the same — scoring still prefers the original target.
        _min_dte_w, _max_dte_w, _target_dte_w = dte_window
        _remaining_window = _max_dte_w - int(dte)
        if _remaining_window < 7 and dte > 0 and dte <= _max_dte_w * 3:
            # Only extend when current DTE is in the same order of magnitude as the
            # window — avoids nonsensical extensions (e.g. LEAPS DTE=689 in WEEKLY).
            # Use +35 days to bridge monthly-only chains (monthlies are ~30d apart).
            _extended_max = int(dte) + 35  # bridge at least one monthly cycle gap
            if _extended_max > _max_dte_w:
                dte_window = (_min_dte_w, _extended_max, _target_dte_w)
                logger.info(
                    f"[RollEngine] {ticker} DTE window extended: current_dte={dte:.0f} "
                    f"near max={_max_dte_w}, remaining window was {_remaining_window}d → "
                    f"extended to ({_min_dte_w}, {_extended_max}, {_target_dte_w})"
                )

        # Timing-aware adjustments to roll target parameters.
        # McMillan Ch.3: in a breakout UP on a BW/CC, roll further OTM and further
        # out in time — give the stock room to run and collect more premium.
        # In a breakdown, roll closer-in (smaller debit) and slightly lower delta.
        # Note: timing adjustments apply on top of roll_mode — they are additive.
        # EXCEPTION: WEEKLY mode skips timing adjustments — the short-cycle window
        # is a hard constraint (fragility). A BREAKOUT_UP signal must not extend it.
        if roll_mode != _ROLL_MODE_WEEKLY:
            timing = _get_roll_timing(row)
            dte_window, delta_range = _adjust_for_timing(
                timing, strategy_key, dte_window, delta_range, ul_price, strike
            )

        # ── Calendar context awareness ─────────────────────────────────────────
        # Wire in expiry_proximity_flag() from scan_engine.calendar_context:
        #
        # 1. PIN_RISK / GAMMA_CRITICAL (≤3 DTE) → WEEKLY override (if not EMERGENCY)
        #    Don't extend DTE into a position that's about to expire with pin risk.
        #    McMillan Ch.7: close or roll before Thursday if within 2% of strike.
        #
        # 2. PRE_HOLIDAY_EXPIRY (≤7 DTE into long weekend) → WEEKLY override + note
        #    Theta acceleration + non-trading gap = roll today, not Monday.
        #
        # 3. THETA_ACCELERATING (≤7 DTE, normal) → note only (no mode change)
        #    Informs candidate rationale: final-week decay is non-linear.
        #
        # 4. Pre-long-weekend with ≤21 DTE current position → theta collection note
        #    Rolling into a position that captures weekend theta is an advantage.
        #    Passarelli Ch.6: Friday rolls collect Sat+Sun theta at no extra risk.
        calendar_note = ""
        try:
            from scan_engine.calendar_context import (
                expiry_proximity_flag, get_calendar_context, is_pre_long_weekend,
            )
            _prox_flag, _prox_note = expiry_proximity_flag(
                dte=dte, strategy=strategy_key,
                ul_last=ul_price, strike=strike,
            )
            _ctx = get_calendar_context()

            if _prox_flag in ("PIN_RISK", "GAMMA_CRITICAL", "PRE_HOLIDAY_EXPIRY"):
                if roll_mode not in (_ROLL_MODE_EMERGENCY,):
                    _prev_mode = roll_mode
                    roll_mode = _ROLL_MODE_WEEKLY
                    dte_window  = _ROLL_DTE_WINDOWS_WEEKLY.get(strategy_key,
                                    _ROLL_DTE_WINDOWS.get(strategy_key, (7, 21, 14)))
                    delta_range = _ROLL_DELTA_TARGETS_WEEKLY.get(strategy_key,
                                    _ROLL_DELTA_TARGETS.get(strategy_key, (0.20, 0.40)))
                    logger.info(
                        f"[RollEngine] {ticker} → WEEKLY mode override: "
                        f"calendar={_prox_flag} (was {_prev_mode})"
                    )
                calendar_note = f" | 📅 {_prox_note}"

            elif _prox_flag == "THETA_ACCELERATING":
                calendar_note = f" | 📅 {_prox_note}"

            elif _ctx.is_pre_long_weekend and dte <= 21:
                # Current position has ≤21 DTE and today is the last session before
                # a long weekend — rolling now captures the extra theta gap.
                _gap = _ctx.theta_bleed_days
                calendar_note = (
                    f" | 📅 Pre-long-weekend roll advantage: "
                    f"+{_gap} extra theta days collected by rolling today "
                    f"(Passarelli Ch.6: Friday rolls collect weekend theta at no added risk)."
                )

            elif _ctx.is_friday and dte <= 21:
                # Normal Friday with near-expiry position — theta advantage note only.
                calendar_note = (
                    f" | 📅 Friday roll: captures Sat+Sun theta decay in new position "
                    f"(Passarelli Ch.6)."
                )

        except Exception as _cal_err:
            logger.debug(f"[RollEngine] calendar_context unavailable for {ticker}: {_cal_err}")

        # Fetch chain (reuse session cache to avoid duplicate calls)
        chain = _get_chain(ticker, schwab_client, session_cache)
        if not chain:
            logger.warning(f"[RollEngine] No chain for {ticker} — skipping roll candidates")
            continue

        if ticker not in processed_tickers:
            processed_tickers.add(ticker)
            time.sleep(_CHAIN_DELAY_SEC)

        # ── Earnings proximity (Phase 1b) ──────────────────────────────────────
        # Compute days_to_earnings for the scoring penalty in _score_candidate().
        # The penalty fires when earnings fall BEFORE this candidate's expiry DTE,
        # meaning the new position would be held through the IV event.
        # Source: "Earnings Date" column from doctrine CSV (injected by DoctrineAuthority).
        _days_to_earnings: int | None = None
        _earn_raw = row.get("Earnings Date")
        if _earn_raw not in (None, "", "nan", "N/A") and not (
            isinstance(_earn_raw, float) and pd.isna(_earn_raw)
        ):
            try:
                _ed = pd.to_datetime(str(_earn_raw), errors="coerce")
                if pd.notna(_ed):
                    _days_to_earnings = (_ed.normalize() - pd.Timestamp.now().normalize()).days
            except Exception:
                pass

        # Find candidates
        candidates = _select_roll_candidates(
            chain            = chain,
            cp               = cp_normalized,
            ul_price         = ul_price,
            current_strike   = strike,
            current_dte      = dte,
            current_iv       = current_iv,
            dte_window       = dte_window,
            delta_range      = delta_range,
            strategy_key     = strategy_key,
            net_cost_basis   = net_cost_basis,
            hv_20d           = hv_20d,
            roll_mode        = roll_mode,
            days_to_earnings = _days_to_earnings,
        )

        # ── No-viable-roll assessment (emergency mode only) ───────────────────
        # If we're in EMERGENCY mode and no candidates survive the above-basis
        # filter, surface an explicit "assignment preferable" verdict as
        # Roll_Candidate_1 so the dashboard and checklist can surface it.
        if roll_mode == _ROLL_MODE_EMERGENCY and not candidates:
            viability = _assess_roll_viability(
                ul_price=ul_price, net_cost_basis=net_cost_basis,
                current_strike=strike, current_delta=current_delta,
                strategy_key=strategy_key,
            )
            df.at[idx, "Roll_Candidate_1"] = json.dumps(viability)
            logger.info(
                f"[RollEngine] {ticker} EMERGENCY mode — no viable above-basis roll found. "
                f"Verdict: {viability.get('verdict')}"
            )
            continue

        # ── INCOME_SAME credit-preference re-ranking ──────────────────────────
        # In SIDEWAYS_INCOME, credit rolls are structurally correct — they reduce
        # basis and continue the income cycle. Debit rolls erode collected premium.
        # After scoring, re-sort: credits first (by score), then debits (by score).
        # This ensures the top candidate is always the best credit roll, even if a
        # far-OTM debit roll scored higher on raw delta proximity.
        if roll_mode == _ROLL_MODE_INCOME_SAME and candidates:
            # Pre-compute cost for re-ranking
            for _c in candidates:
                _c["_pre_cost"] = _estimate_roll_cost(row, _c)
            # Partition into credits and debits
            _credits = [c for c in candidates if c["_pre_cost"].get("type") == "credit"]
            _debits  = [c for c in candidates if c["_pre_cost"].get("type") != "credit"]
            # Credits first (already sorted by score), then debits
            candidates = _credits + _debits
            # Clean up pre-computed cost (will be re-assigned below)
            for _c in candidates:
                _c.pop("_pre_cost", None)
            if _credits:
                logger.info(
                    f"[RollEngine] {ticker} INCOME_SAME: {len(_credits)} credit candidate(s) "
                    f"prioritised over {len(_debits)} debit candidate(s)"
                )

        for rank, cand in enumerate(candidates[:_TOP_N], start=1):
            cost_to_roll = _estimate_roll_cost(row, cand)
            cand["roll_from_strike"]  = strike
            cand["roll_from_dte"]     = int(dte)
            cand["roll_from_iv"]      = round(current_iv, 4)
            cand["cost_to_roll"]      = cost_to_roll
            cand["roll_mode"]         = roll_mode   # surface to dashboard
            # Trader-quality derived fields
            cand.update(_compute_trader_metrics(
                cand, cost_to_roll, net_cost_basis, cum_premium, ul_price, strategy_key
            ))
            cand["calendar_note"]     = calendar_note  # surfaced in dashboard trade-off line
            cand["roll_rationale"]    = _build_roll_rationale(
                cand, strategy_key, ul_price, net_cost_basis, roll_mode=roll_mode,
                position_signals={
                    "adx_14":                float(row.get("adx_14") or 0),
                    "roc_20":                float(row.get("roc_20") or 0),
                    "momentum_slope":        float(row.get("momentum_slope") or 0),
                    "MomentumVelocity_State": str(row.get("MomentumVelocity_State") or ""),
                    "iv_vs_hv_gap":          float(row.get("IV_vs_HV_Gap") or 0),
                    "drift_direction":       str(row.get("Drift_Direction") or ""),
                    "drift_magnitude":       str(row.get("Drift_Magnitude") or ""),
                },
                calendar_note=calendar_note,
            )
            df.at[idx, f"Roll_Candidate_{rank}"] = json.dumps(cand)

    rolled_count = roll_mask.sum()
    filled_count = df[roll_mask]["Roll_Candidate_1"].notna().sum()
    logger.info(
        f"[RollEngine] {filled_count}/{rolled_count} ROLL rows populated with candidates."
    )
    return df


# ── Vol surface helpers ────────────────────────────────────────────────────────

def _build_vol_surface(
    chain: dict,
    cp: str,
    ul_price: float,
    min_dte: int,
    max_dte: int,
) -> Dict[str, Dict]:
    """
    Pre-scan the full chain to extract ATM IV and put skew per expiry.

    Returns a dict keyed by expiry_date_str with:
      {
        "atm_iv":    float | None  — IV of the closest-to-ATM strike
        "skew_ratio": float | None — put_otm_iv / atm_iv  (>1 = put skew present)
        "term_ivs":  {exp_str: atm_iv, ...}  — for term structure slope (populated
                                                after all expiries are processed)
      }

    Skew interpretation (Gatheral Ch.1 / Sinclair Ch.4):
      skew_ratio > 1.05 : put wing is bid — market pricing downside risk
      skew_ratio < 0.95 : unusual; call wing bid (rare for most equities)
      skew_ratio = 1.0  : flat smile — no skew at this expiry

    Term structure slope = (far_atm_iv - near_atm_iv) / (far_dte - near_dte)
      positive slope : contango — farther expiries are richer (normal backwardation
                        in equity options). Selling near term = selling cheap vol;
                        rolling to farther DTE picks up the contango premium.
      negative slope : backwardation (event-driven) — near-term IV spike; farther
                        expiries are cheaper. Common before earnings/macro events.
                        Rolling to farther DTE AFTER the event = buying cheap vol.

    Only scans DTE window [min_dte, max_dte] to avoid irrelevant expiries.
    """
    exp_map_key = "callExpDateMap" if cp == "C" else "putExpDateMap"
    exp_map     = chain.get(exp_map_key, {})
    # Also always pull put map for skew (put skew = put OTM IV vs ATM call IV)
    put_map     = chain.get("putExpDateMap", {})

    surface: Dict[str, Dict] = {}

    for exp_key, strikes_map in exp_map.items():
        parts = exp_key.split(":")
        if len(parts) != 2:
            continue
        exp_date_str = parts[0]
        try:
            exp_dte = int(parts[1])
        except ValueError:
            continue
        if exp_dte < min_dte or exp_dte > max_dte:
            continue

        # ── ATM IV — find the strike closest to ul_price ─────────────────
        atm_iv: float | None = None
        best_distance = float("inf")
        for strike_str, contracts in strikes_map.items():
            if not contracts:
                continue
            try:
                s = float(strike_str)
                dist = abs(s - ul_price)
                if dist < best_distance:
                    c = contracts[0]
                    iv_raw = c.get("volatility", c.get("impliedVolatility", None))
                    if iv_raw is not None:
                        iv_val = float(iv_raw) / 100.0
                        if iv_val > 0:
                            atm_iv = iv_val
                            best_distance = dist
            except (TypeError, ValueError):
                continue

        # ── Put skew — OTM put IV / ATM IV ────────────────────────────────
        # OTM put = strike ~85–90% of spot (25-delta proxy). Gatheral Ch.1:
        # "Equity skew arises from the demand for downside protection."
        # We use the put at ~87.5% of spot (midpoint of 80–95% OTM range).
        skew_ratio: float | None = None
        if atm_iv and atm_iv > 0 and ul_price > 0:
            _otm_target = ul_price * 0.875   # ~87.5% of spot = typical 25Δ put region
            _put_strikes_map = put_map.get(exp_key, {})
            _put_best_dist = float("inf")
            _put_otm_iv: float | None = None
            for _ps_str, _pcontracts in _put_strikes_map.items():
                if not _pcontracts:
                    continue
                try:
                    _ps = float(_ps_str)
                    # Only OTM puts (strike < ul_price)
                    if _ps >= ul_price:
                        continue
                    _dist = abs(_ps - _otm_target)
                    if _dist < _put_best_dist:
                        _pc = _pcontracts[0]
                        _piv_raw = _pc.get("volatility", _pc.get("impliedVolatility", None))
                        if _piv_raw is not None:
                            _piv = float(_piv_raw) / 100.0
                            if _piv > 0:
                                _put_otm_iv = _piv
                                _put_best_dist = _dist
                except (TypeError, ValueError):
                    continue
            if _put_otm_iv and _put_otm_iv > 0:
                skew_ratio = _put_otm_iv / atm_iv

        surface[exp_date_str] = {
            "atm_iv":    atm_iv,
            "skew_ratio": skew_ratio,
            "dte":       exp_dte,
        }

    # ── Term structure slope ───────────────────────────────────────────────
    # Computed across all expiries with valid ATM IV in the window.
    # slope = (IV_far - IV_near) / (DTE_far - DTE_near)
    # Injected into each expiry entry so _score_candidate can use it.
    sorted_expiries = sorted(
        [(v["dte"], k, v["atm_iv"]) for k, v in surface.items() if v["atm_iv"] is not None],
        key=lambda x: x[0],
    )
    for exp_date_str, entry in surface.items():
        entry["term_slope"] = None  # default: no slope computed

    if len(sorted_expiries) >= 2:
        near_dte, near_key, near_iv = sorted_expiries[0]
        far_dte,  far_key,  far_iv  = sorted_expiries[-1]
        if far_dte > near_dte and near_iv and far_iv:
            _slope = (far_iv - near_iv) / (far_dte - near_dte)
            for _, entry in surface.items():
                entry["term_slope"] = round(_slope, 6)

    return surface


# ── Candidate selection ────────────────────────────────────────────────────────

def _select_roll_candidates(
    chain: dict,
    cp: str,
    ul_price: float,
    current_strike: float,
    current_dte: float,
    current_iv: float,
    dte_window: Tuple[int, int, int],
    delta_range: Tuple[float, float],
    strategy_key: str,
    net_cost_basis: float = 0.0,
    hv_20d: float = 0.0,
    roll_mode: str = _ROLL_MODE_NORMAL,
    days_to_earnings: int | None = None,
) -> List[Dict[str, Any]]:
    """
    Parse the chain, filter for roll-eligible contracts, score and rank them.
    Returns ranked list of candidate dicts.

    roll_mode controls two mode-specific behaviors:
      PRE_ITM   — above-basis strikes are strongly preferred (scored higher);
                  credit rolls sorted to top; debit rolls included but penalized.
      EMERGENCY — above-basis is a HARD FILTER: any strike at or below net_cost_basis
                  is excluded entirely. Only above-basis rolls surface. If none exist,
                  returns empty list → caller generates no-viable-roll verdict.
      NORMAL    — standard scoring, no basis filter.
    """
    min_dte, max_dte, target_dte = dte_window
    delta_min, delta_max = delta_range
    is_short_vol = strategy_key in ("BUY_WRITE", "COVERED_CALL", "SHORT_PUT", "CSP")
    _emergency_basis_filter = (
        roll_mode == _ROLL_MODE_EMERGENCY
        and is_short_vol
        and net_cost_basis > 0
    )

    # Phase 2 — vol surface pre-scan.
    # Build ATM IV, put skew, and term structure slope before the main scoring loop.
    # This avoids re-scanning the chain per-strike and gives each candidate
    # expiry-level surface data for a richer IV score in _score_candidate().
    vol_surface = _build_vol_surface(
        chain    = chain,
        cp       = cp,
        ul_price = ul_price,
        min_dte  = min_dte,
        max_dte  = max_dte,
    )

    exp_map_key = "callExpDateMap" if cp == "C" else "putExpDateMap"
    exp_map     = chain.get(exp_map_key, {})

    candidates: List[Dict[str, Any]] = []

    # ── Diagnostic: log available expiries vs DTE window ─────────────────
    _diag_expiries = []
    for _ek in exp_map:
        _pts = _ek.split(":")
        if len(_pts) == 2:
            try:
                _diag_expiries.append((_pts[0], int(_pts[1])))
            except ValueError:
                pass
    _diag_expiries.sort(key=lambda x: x[1])
    _diag_in_window = [
        f"{d}(DTE{e})" for d, e in _diag_expiries
        if min_dte <= e <= max_dte and e > current_dte
    ]
    logger.info(
        f"[RollEngine] _select: mode={roll_mode} cp={cp} "
        f"dte_window=({min_dte},{max_dte},{target_dte}) current_dte={current_dte:.0f} "
        f"delta_range=({delta_min:.2f},{delta_max:.2f}) "
        f"chain_expiries={len(_diag_expiries)} in_window={len(_diag_in_window)} "
        f"{_diag_in_window[:5]}"
    )

    _diag_dte_rejected = 0
    _diag_rollout_rejected = 0
    _diag_mid_rejected = 0
    _diag_delta_rejected = 0
    _diag_liq_rejected = 0
    _diag_basis_rejected = 0

    for exp_key, strikes_map in exp_map.items():
        # exp_key format: "2026-04-17:55"
        parts = exp_key.split(":")
        if len(parts) != 2:
            continue
        exp_date_str = parts[0]
        try:
            actual_dte = int(parts[1])
        except ValueError:
            continue

        # Filter by DTE window — must be FURTHER out than current (rolling out)
        if actual_dte < min_dte or actual_dte > max_dte:
            _diag_dte_rejected += len(strikes_map)
            continue
        if actual_dte <= current_dte:
            _diag_rollout_rejected += len(strikes_map)
            continue  # Cannot roll to same or shorter DTE

        for strike_str, contracts in strikes_map.items():
            if not contracts:
                continue
            c = contracts[0]

            # Extract contract data
            try:
                strike      = float(strike_str)
                delta_raw   = float(c.get("delta", 0) or 0)
                gamma       = float(c.get("gamma", 0) or 0)
                vega        = float(c.get("vega",  0) or 0)
                theta       = float(c.get("theta", 0) or 0)
                bid         = float(c.get("bid",   0) or 0)
                ask         = float(c.get("ask",   0) or 0)
                bid_size    = int(c.get("bidSize",  0) or 0)
                ask_size    = int(c.get("askSize",  0) or 0)
                oi          = int(c.get("openInterest", 0) or 0)
                volume      = int(c.get("totalVolume",  0) or 0)
                iv_raw      = c.get("volatility", c.get("impliedVolatility", None))
                iv          = float(iv_raw) / 100.0 if iv_raw is not None else np.nan
                mid         = (bid + ask) / 2.0 if bid > 0 and ask > 0 else float(c.get("mark", 0) or 0)
            except (TypeError, ValueError):
                continue

            if mid <= 0 or bid <= 0:
                _diag_mid_rejected += 1
                continue

            # Delta filter — calls positive, puts negative
            if cp == "C":
                if not (delta_min <= delta_raw <= delta_max):
                    _diag_delta_rejected += 1
                    continue
            else:
                # Put deltas are negative: delta_range like (-0.35, -0.15)
                if not (delta_range[0] <= delta_raw <= delta_range[1]):
                    _diag_delta_rejected += 1
                    continue

            # Liquidity filter — now includes bid/ask size for book-depth awareness
            spread_pct = ((ask - bid) / mid * 100) if mid > 0 else 999
            liq_grade  = _grade_liquidity(oi, volume, spread_pct, bid_size=bid_size, ask_size=ask_size)
            if liq_grade == "ILLIQUID":
                _diag_liq_rejected += 1
                continue

            # ── EMERGENCY mode: hard filter — above net cost basis only ──────
            # McMillan Ch.3: "If no roll above your cost basis exists, assignment
            # is preferable to locking in a debit roll that still realizes a loss."
            # Only apply to short vol strategies where basis comparison makes sense.
            if _emergency_basis_filter and strike <= net_cost_basis:
                _diag_basis_rejected += 1
                continue  # below/at basis — would not rescue position

            # Phase 2 — pull per-expiry surface data for this candidate
            _surf = vol_surface.get(exp_date_str, {})
            _atm_iv    = _surf.get("atm_iv")
            _skew_ratio = _surf.get("skew_ratio")
            _term_slope = _surf.get("term_slope")

            # Score: composite of delta proximity, DTE fit, IV, theta yield, liquidity
            score = _score_candidate(
                delta            = abs(delta_raw),
                target_delta     = abs((delta_range[0] + delta_range[1]) / 2),
                actual_dte       = actual_dte,
                target_dte       = target_dte,
                iv               = iv,
                current_iv       = current_iv,
                liq_grade        = liq_grade,
                strategy_key     = strategy_key,
                mid              = mid,
                theta            = theta,
                net_cost_basis   = net_cost_basis,
                hv_20d           = hv_20d,
                ul_price         = ul_price,
                strike           = strike,
                roll_mode        = roll_mode,
                days_to_earnings = days_to_earnings,
                atm_iv           = _atm_iv,
                skew_ratio       = _skew_ratio,
                term_slope       = _term_slope,
            )

            candidates.append({
                "strike":     round(strike, 2),
                "expiry":     exp_date_str,
                "dte":        actual_dte,
                "delta":      round(delta_raw, 3),
                "gamma":      round(gamma, 4),
                "vega":       round(vega, 4),
                "theta":      round(theta, 4),
                "iv":         round(iv, 4) if not np.isnan(iv) else None,
                "bid":        round(bid, 2),
                "ask":        round(ask, 2),
                "mid":        round(mid, 2),
                "bid_size":   bid_size,
                "ask_size":   ask_size,
                "oi":         oi,
                "volume":     volume,
                "spread_pct": round(spread_pct, 1),
                "liq_grade":  liq_grade,
                "score":      round(score, 4),
                "hv_20d":     round(hv_20d, 4) if hv_20d else None,
                "atm_iv":     round(_atm_iv, 4) if _atm_iv is not None else None,
                "skew_ratio": round(_skew_ratio, 3) if _skew_ratio is not None else None,
                "term_slope": round(_term_slope, 6) if _term_slope is not None else None,
            })

    # ── Diagnostic summary ──────────────────────────────────────────────
    logger.info(
        f"[RollEngine] _select filter summary: "
        f"dte_rejected={_diag_dte_rejected} rollout_rejected={_diag_rollout_rejected} "
        f"mid_rejected={_diag_mid_rejected} delta_rejected={_diag_delta_rejected} "
        f"liq_rejected={_diag_liq_rejected} basis_rejected={_diag_basis_rejected} "
        f"→ {len(candidates)} candidates survived"
    )

    # Sort by score descending
    candidates.sort(key=lambda x: x["score"], reverse=True)
    return candidates


# ── Scoring ───────────────────────────────────────────────────────────────────

def _score_candidate(
    delta: float,
    target_delta: float,
    actual_dte: int,
    target_dte: int,
    iv: float,
    current_iv: float,
    liq_grade: str,
    strategy_key: str,
    mid: float = 0.0,
    theta: float = 0.0,
    net_cost_basis: float = 0.0,
    hv_20d: float = 0.0,
    ul_price: float = 0.0,
    strike: float = 0.0,
    roll_mode: str = _ROLL_MODE_NORMAL,
    days_to_earnings: int | None = None,
    atm_iv: float | None = None,
    skew_ratio: float | None = None,
    term_slope: float | None = None,
) -> float:
    """
    Composite score (higher = better roll candidate). Think like a trader.

    McMillan Ch.3 / Passarelli Ch.6 / Natenberg Ch.11 / Gatheral Ch.1:

    Components and weights:
      1. Delta proximity  (25%) — strike placement relative to directional thesis
      2. Annualized yield (25%) — premium efficiency on net cost basis (Passarelli Ch.6)
                                   blended 70/30 with theta efficiency (Phase 1c)
      3. DTE fit          (20%) — time horizon alignment with strategy cycle
      4. Liquidity        (20%) — execution feasibility (THIN grade = credit is theoretical)
      5. IV advantage     (10%) — vol edge: IV/HV ratio + vol surface signals (Phase 2)
                                   atm_iv vs strike iv (skew position)
                                   term structure slope (contango vs backwardation)

    Post-scoring multipliers:
      Earnings-in-window penalty (Phase 1b) — if earnings fall before this contract
      expires, the new position carries the IV collapse event. Natenberg Ch.8:
      "IV collapses post-earnings — all extrinsic value earned by selling the event
      is surrendered on the morning after." Penalty is strategy-conditional:
      higher for short-vol (selling the event is structurally dangerous) than
      long-vol (IV crush hurts but does not guarantee a loss).

    Yield replaces the old DTE-as-primary approach: two strikes at same DTE but
    different premiums should score differently — the one that actually earns
    more per dollar of capital deployed wins (McMillan: "maximize income per cycle").
    """
    is_short_vol = strategy_key in ("BUY_WRITE", "COVERED_CALL", "SHORT_PUT", "CSP")

    # 1. Delta proximity (25%) — closer to target = better strike placement
    max_delta_deviation = 0.30
    delta_dev   = abs(delta - target_delta)
    delta_score = max(0.0, 1.0 - delta_dev / max_delta_deviation)

    # 2. Annualized yield on capital (25%) — the trader's primary filter for income strategies
    # Passarelli Ch.6: "Choose the roll that maximizes premium per day per dollar at risk."
    # Benchmark: must exceed Fidelity's 10.375%/yr margin rate to have positive carry.
    # Excellent = 2× margin rate (~20.75%/yr), neutral = margin rate (just covering carry),
    # poor = below margin rate (negative carry — this roll loses money on financing alone).
    # McMillan Ch.3: "Any roll that fails to cover the financing cost is negative carry."
    yield_score = 0.5  # neutral default
    if actual_dte > 0 and mid > 0:
        if is_short_vol and net_cost_basis > 0:
            # Per-share income annualized on net cost basis
            annualized_yield = (mid / net_cost_basis) * (365 / actual_dte)
            # Score: 0 at 0%/yr, 0.5 at margin rate (10.375%), 1.0 at 2× margin rate (~20.75%)
            yield_score = min(1.0, annualized_yield / _YIELD_BENCHMARK_EXCELLENT)
        elif is_short_vol and ul_price > 0:
            # Fallback: yield on current stock price
            annualized_yield = (mid / ul_price) * (365 / actual_dte)
            yield_score = min(1.0, annualized_yield / _YIELD_BENCHMARK_EXCELLENT)
        elif not is_short_vol:
            # Long vol: score by extrinsic value as % of premium paid
            # More extrinsic = more time value = more room for thesis to develop
            intrinsic = max(0.0, ul_price - strike) if ul_price > strike else 0.0
            extrinsic = max(0.0, mid - intrinsic)
            extrinsic_pct = extrinsic / mid if mid > 0 else 0.5
            yield_score = min(1.0, extrinsic_pct * 1.5)  # 67% extrinsic = perfect

    # Phase 1c — theta efficiency blend into yield_score (30% weight within yield component)
    # theta_per_dollar = |theta| / mid: daily theta earned per dollar of premium.
    # Passarelli Ch.6: "The efficient roll maximizes theta collected per dollar committed."
    # A strike with the same premium but higher theta/dollar harvests the carry faster —
    # less exposure to adverse moves per unit of income collected.
    # Benchmark: 0.033/day per dollar at mid = 1.0% daily = perfect (annualizes to ~12%/yr
    # of the premium itself, which exceeds any realistic carry benchmark).
    # Blend: 70% raw yield + 30% theta efficiency — preserves the capital basis anchor
    # while rewarding candidates that harvest carry faster.
    if theta != 0 and mid > 0:
        theta_per_dollar = abs(theta) / mid   # daily decay per $1 of premium
        # Scale: 0.033/day = score 1.0 (1%/day); anything above is capped
        theta_score = min(1.0, theta_per_dollar / 0.033)
        yield_score = 0.70 * yield_score + 0.30 * theta_score

    # 3. DTE fit (20%) — time horizon match
    max_dte_deviation = 60
    dte_dev   = abs(actual_dte - target_dte)
    dte_score = max(0.0, 1.0 - dte_dev / max_dte_deviation)

    # 4. IV advantage (10%) — volatility edge: IV/HV + vol surface signals (Phase 2)
    #
    # Base signal: IV/HV ratio (Natenberg Ch.11)
    #   Short vol: IV > HV = positive edge (selling rich premium)
    #   Long vol:  IV < HV = positive edge (buying cheap vol)
    #
    # Surface signal 1 — skew position (Gatheral Ch.1 / Sinclair Ch.4):
    #   skew_ratio = OTM put IV / ATM IV. Equity options almost always have skew > 1.
    #   For SHORT PUT / CSP: high skew (>1.15) means you're in the put wing bid —
    #     the market is paying you extra for downside protection. Strong short-vol edge.
    #   For LONG CALL: high skew means farther OTM calls are cheaper relative to ATM.
    #     Rolling to a slightly higher-delta (closer-to-ATM) call is more efficient.
    #   Implementation: skew bonus/penalty on iv_score (capped to avoid dominating)
    #
    # Surface signal 2 — term structure slope (Gatheral Ch.2):
    #   Positive slope (contango): farther expiries carry more IV. For short-vol rolls
    #     to farther DTE, you're selling into richer vol — term structure is favorable.
    #   Negative slope (backwardation): near-term is spiked (event). Rolling farther
    #     = buying the spike at near-term and selling cheap farther out. Avoid for
    #     income strategies; potentially useful for long vol (buy now, vol collapses).
    #   Implementation: slope adjustment added to iv_score
    iv_score = 0.5
    _iv_valid = not (np.isnan(iv) if isinstance(iv, float) else False) and iv > 0

    if _iv_valid:
        if hv_20d > 0:
            iv_hv_ratio = iv / hv_20d
            if is_short_vol:
                iv_score = min(1.0, max(0.0, iv_hv_ratio - 0.5))  # 1.5× = perfect
            else:
                iv_score = min(1.0, max(0.0, 1.5 - iv_hv_ratio))
        elif current_iv > 0:
            iv_ratio = iv / current_iv
            if is_short_vol:
                iv_score = max(0.0, min(1.0, iv_ratio - 0.5))
            else:
                iv_score = max(0.0, min(1.0, 1.5 - iv_ratio))

    # Phase 2a — skew adjustment
    # Applies when we have atm_iv to compare strike IV against the ATM level.
    # This captures the skew position: is this strike in the put wing (bid) or
    # call wing (cheap)? The ratio iv/atm_iv tells us where on the smile we are.
    # For short-vol PUT strategies: a strike with iv > atm_iv is in the put wing —
    #   the market is paying extra for that strike. This is positive edge.
    # For short-vol CALL strategies: iv ≈ atm_iv is ideal (ATM = richest premium/delta).
    #   iv > atm_iv on calls = unusual; may indicate straddle bid or event.
    # For long vol: iv < atm_iv = buying the cheap wing (OTM options cheaper vs ATM).
    if _iv_valid and atm_iv and atm_iv > 0:
        _strike_vs_atm = iv / atm_iv  # >1 = this strike is in the "expensive" wing
        if is_short_vol:
            # Selling expensive wing = positive edge
            _skew_adj = min(0.10, max(-0.10, (_strike_vs_atm - 1.0) * 0.5))
        else:
            # Buying cheap wing = positive edge (lower iv/atm_iv = better for long vol)
            _skew_adj = min(0.10, max(-0.10, (1.0 - _strike_vs_atm) * 0.5))
        iv_score = min(1.0, max(0.0, iv_score + _skew_adj))

    # Phase 2b — put skew ratio awareness (Gatheral Ch.1)
    # skew_ratio = OTM put IV / ATM IV for this expiry.
    # High skew (>1.15) means the market is paying a lot for downside protection.
    # For short put strategies: high skew means you're selling expensive protection —
    #   extra premium per dollar of risk. Positive edge; mild bonus.
    # For long call strategies: high skew means OTM puts are expensive relative to
    #   calls. Irrelevant or slight negative (capital is being pulled to put wing).
    # Adjustment is small (±0.05 max) to inform without dominating the IV score.
    if skew_ratio is not None and skew_ratio > 0:
        if is_short_vol and strategy_key in ("SHORT_PUT", "CSP"):
            # High put skew = market paying more for puts = selling puts has more edge
            _skew_ratio_adj = min(0.05, max(-0.05, (skew_ratio - 1.0) * 0.25))
            iv_score = min(1.0, max(0.0, iv_score + _skew_ratio_adj))

    # Phase 2c — term structure adjustment (Gatheral Ch.2)
    # term_slope > 0 (contango): farther expiries carry more IV.
    #   For short-vol rolling farther: you're rolling into richer premium — favorable.
    #   Magnitude: if slope is large (e.g., 0.001 IV/day = 0.1 vol over 100d), that's
    #   a meaningful premium pickup on a longer roll.
    # term_slope < 0 (backwardation): farther expiries are cheaper.
    #   Rolling farther = selling cheaper vol. Negative for income strategies.
    # Adjustment capped at ±0.08 to prevent term structure dominating the score.
    if term_slope is not None:
        # Normalize: 0.001 IV/day = "moderate" contango for equities
        _slope_adj = min(0.08, max(-0.08, term_slope * 80))
        if is_short_vol:
            # Contango (positive slope) = favorable for short-vol extension rolls
            iv_score = min(1.0, max(0.0, iv_score + _slope_adj))
        else:
            # Long vol: backwardation (negative slope) after event = buy cheap
            iv_score = min(1.0, max(0.0, iv_score - _slope_adj))

    # 5. Liquidity (20%) — raised from 10%
    # A roll candidate with THIN/ILLIQUID grade cannot reliably fill at mid.
    # Theoretical yield is irrelevant if execution cost eats the credit.
    # McMillan Ch.3: "A roll that can't fill at a reasonable price is not a roll."
    liq_map = {"EXCELLENT": 1.0, "GOOD": 0.75, "ACCEPTABLE": 0.5, "THIN": 0.25, "ILLIQUID": 0.0}
    liq_score = liq_map.get(liq_grade, 0.5)

    # Weights: delta 25%, yield 25%, DTE 20%, liquidity 20%, IV 10%
    # Rationale: liquidity raised from 10% to 20% (execution feasibility is binary —
    # a THIN strike with 30%+ spread will not fill at mid, making the credit theoretical).
    # Delta reduced from 30% to 25% (small delta deviation matters less than executability).
    base_score = (0.25 * delta_score + 0.25 * yield_score +
                  0.20 * dte_score   + 0.10 * iv_score +
                  0.20 * liq_score)

    # ── Mode-specific score adjustments ─────────────────────────────────────
    # PRE_ITM: credit-first prioritization + above-basis bonus.
    # EMERGENCY: above-basis strikes already hard-filtered, so add a bonus for
    # how far ABOVE basis the strike is (more rescue distance = better).
    # INCOME_SAME: same-strike proximity bonus — prefer rolling OUT, not UP.
    mode_bonus = 0.0
    if roll_mode in (_ROLL_MODE_PRE_ITM, _ROLL_MODE_EMERGENCY) and is_short_vol:
        if net_cost_basis > 0 and strike > 0:
            if strike > net_cost_basis:
                # Bonus proportional to how far above basis: e.g. +3% above = +0.06 bonus
                basis_pct_above = (strike - net_cost_basis) / net_cost_basis
                mode_bonus += min(0.15, basis_pct_above * 2.0)  # cap at 0.15
            elif roll_mode == _ROLL_MODE_PRE_ITM and strike <= net_cost_basis:
                # Below-basis: soft penalty in PRE_ITM (not hard-filtered, but deprioritized)
                mode_bonus -= 0.10
    elif roll_mode == _ROLL_MODE_INCOME_SAME and is_short_vol:
        # SIDEWAYS_INCOME: same-strike credit rolls are structurally correct.
        # McMillan Ch.3: in range-bound behaviour, roll OUT in time, not UP in strike.
        # Bonus: strikes near current strike get up to +0.20 (dominant influence).
        # Penalty: strikes far from current (delta <0.30) lose up to -0.10.
        # This flips the scoring from "target OTM delta" to "target same strike".
        if ul_price > 0 and strike > 0:
            # How close is this candidate's strike to the current price?
            # delta ~0.50 = ATM = same strike; delta ~0.20 = far OTM
            # Use delta proximity to ATM as proxy (simpler, no need for current_strike)
            _atm_proximity = 1.0 - abs(delta - 0.50) / 0.30  # 1.0 at delta=0.50, 0 at delta=0.20/0.80
            _atm_proximity = max(0.0, min(1.0, _atm_proximity))
            mode_bonus += _atm_proximity * 0.20  # up to +0.20 for ATM/same-strike
            # Penalize far-OTM strikes that would require a debit to roll UP
            if delta < 0.30:
                mode_bonus -= 0.10

    # ── Phase 1b — Earnings-in-window penalty ───────────────────────────────
    # If earnings fall BEFORE this contract's expiry, the new position is forced
    # to hold through the IV event. Natenberg Ch.8: "Post-earnings IV crush destroys
    # extrinsic value regardless of direction — the premium collected for selling the
    # event is surrendered in the gap-down (or gap-up) open."
    # Strategy-conditional: short-vol sellers are structurally exposed (they sold the
    # event = short gamma through the uncertainty). Long-vol buyers are hurt by IV
    # crush but the directional move may compensate.
    # Severity tiers mirror practical trading risk horizons:
    #   ≤7d  : earnings THIS week — almost certain IV event inside this contract
    #   ≤21d : earnings within 3 weeks — high probability of IV crush before expiry
    #   >21d : earnings inside window but not imminent — soft warning
    earnings_multiplier = 1.0
    if days_to_earnings is not None and days_to_earnings >= 0 and days_to_earnings < actual_dte:
        if days_to_earnings <= 7:
            # Earnings this week — structural IV collapse virtually guaranteed inside DTE
            earnings_multiplier = 0.70 if is_short_vol else 0.82
        elif days_to_earnings <= 21:
            # Earnings within 3 weeks — high probability of carry disruption
            earnings_multiplier = 0.80 if is_short_vol else 0.88
        else:
            # Earnings inside DTE but >3 weeks — moderate awareness penalty
            earnings_multiplier = 0.90 if is_short_vol else 0.93

    return min(1.0, (base_score + mode_bonus) * earnings_multiplier)


# ── Liquidity grading ─────────────────────────────────────────────────────────

def _grade_liquidity(
    oi: int,
    volume: int,
    spread_pct: float,
    bid_size: int = 0,
    ask_size: int = 0,
) -> str:
    """
    Grade execution liquidity. OI is inventory; bid/ask size is liquidity NOW.

    Sinclair (Volatility Trading): realized PnL depends on execution quality,
    not theoretical value. A wide bid/ask or thin book size means mid fills
    are optimistic — the theoretical credit is not the realized credit.

    bid_size / ask_size: number of contracts at the bid/ask right now.
    - < 5 contracts at bid on a sell leg = fragile fill; cap grade at THIN.
    - ≥ 50 contracts = deep book; mild EXCELLENT boost (already at ceiling for high OI).
    """
    # Base grade from OI + spread (existing logic)
    if oi >= 5000 and spread_pct < 10:
        grade = "EXCELLENT"
    elif oi >= 500 and spread_pct < 3:
        grade = "EXCELLENT"
    elif oi >= 1000 and spread_pct < 10:
        grade = "GOOD"
    elif oi >= 100 and spread_pct < 5:
        grade = "GOOD"
    elif oi >= 25 and spread_pct < 10:
        grade = "ACCEPTABLE"
    elif oi >= 5 and volume >= 100:
        grade = "ACCEPTABLE"
    elif oi >= 5:
        grade = "THIN"
    else:
        return "ILLIQUID"

    # ── bid/ask size downgrade (Sinclair: execution quality beats theoretical value) ──
    # The relevant size is the side you're hitting:
    #   - Opening a short (selling): bid_size matters (you're lifting the bid)
    #   - Closing a long (selling): bid_size matters
    #   - Opening a long (buying):  ask_size matters (you're hitting the ask)
    # We use min(bid_size, ask_size) as a conservative proxy when not known directionally.
    _book_size = min(bid_size, ask_size) if bid_size > 0 and ask_size > 0 else max(bid_size, ask_size)
    _GRADE_ORDER = ["ILLIQUID", "THIN", "ACCEPTABLE", "GOOD", "EXCELLENT"]

    if _book_size > 0:
        if _book_size < 5 and spread_pct > 3:
            # Thin book + wide spread: cap at THIN regardless of OI
            # Rationale: OI is stale inventory; a 3-contract book means 3-lot fills max
            # before price moves. McMillan: "always use limit at mid or better" —
            # but if book is < 5 lots, mid may gap on partial fill.
            _target = "THIN"
            if _GRADE_ORDER.index(grade) > _GRADE_ORDER.index(_target):
                grade = _target
        elif _book_size < 10 and spread_pct > 5:
            # Slightly thin + moderately wide: cap at ACCEPTABLE
            _target = "ACCEPTABLE"
            if _GRADE_ORDER.index(grade) > _GRADE_ORDER.index(_target):
                grade = _target

    return grade


# ── Roll cost ─────────────────────────────────────────────────────────────────

def _estimate_roll_cost(current_row: pd.Series, candidate: Dict[str, Any]) -> Dict[str, Any]:
    """
    Estimate the net cost to roll: close current + open candidate.

    For LONG options:
        Close (sell) current at bid → candidate mid = net debit/credit
    For SHORT options (CC, BW, CSP):
        Close (buy) current at ask → sell candidate at bid = net credit/debit
    """
    try:
        strategy = str(current_row.get("Strategy", "") or "")
        qty      = float(current_row.get("Quantity", 1) or 1)
        is_short = qty < 0

        current_bid = float(current_row.get("Bid", 0) or 0)
        current_ask = float(current_row.get("Ask", 0) or 0)
        current_mid = (current_bid + current_ask) / 2 if current_bid and current_ask else float(current_row.get("Last", 0) or 0)

        cand_mid = float(candidate.get("mid", 0) or 0)
        cand_bid = float(candidate.get("bid", 0) or 0)

        if is_short:
            # Short: buy back at ask (close), sell new at bid (open)
            close_cost = current_ask  # pay to close
            open_proceeds = cand_bid  # receive to open
            net = open_proceeds - close_cost  # positive = net credit
            label = "credit" if net >= 0 else "debit"
        else:
            # Long: sell current at bid (close), buy new at ask (open)
            close_proceeds = current_bid
            open_cost = candidate.get("ask", cand_mid)
            net = close_proceeds - open_cost  # negative = net debit
            label = "debit" if net <= 0 else "credit"

        return {
            "net_per_contract":  round(net, 2),
            "net_total":         round(net * abs(qty) * 100, 2),
            "type":              label,
            "contracts":         int(abs(qty)),
        }
    except Exception:
        return {"net_per_contract": None, "net_total": None, "type": "unknown", "contracts": 1}


# ── Roll rationale ────────────────────────────────────────────────────────────

def _compute_trader_metrics(
    cand: Dict[str, Any],
    cost_to_roll: Dict[str, Any],
    net_cost_basis: float,
    cum_premium: float,
    ul_price: float,
    strategy_key: str,
) -> Dict[str, Any]:
    """
    Compute the numbers a trader actually uses to evaluate a roll candidate.

    McMillan Ch.3: before executing a roll, a trader answers:
      1. What is my new breakeven after paying/receiving the roll cost?
      2. What annualized yield does this new strike provide on my capital?
      3. How far OTM is the new strike — what's the probability of it expiring worthless?
      4. Is the theta efficient — how many $/day will I earn while I wait?

    All of these go into Roll_Candidate_* as structured fields so the dashboard
    and doctrine can surface them without re-computing.
    """
    mid        = float(cand.get("mid", 0) or 0)
    strike     = float(cand.get("strike", 0) or 0)
    dte        = int(cand.get("dte", 1) or 1)
    iv         = float(cand.get("iv", 0) or 0)
    theta      = float(cand.get("theta", 0) or 0)  # per-share per-day (negative)
    delta      = abs(float(cand.get("delta", 0) or 0))

    is_short_vol = strategy_key in ("BUY_WRITE", "COVERED_CALL", "SHORT_PUT", "CSP")
    net_roll     = float(cost_to_roll.get("net_per_contract", 0) or 0)  # positive=credit, neg=debit
    contracts    = max(1, int(cost_to_roll.get("contracts", 1) or 1))

    # ── 1. Breakeven after roll ───────────────────────────────────────────────
    # For BW/CC short call: new breakeven = net_cost_basis + roll_credit (or - roll_debit)
    # net_per_contract is already per-share (computed from bid/ask option prices directly).
    # Positive net_roll = credit received → reduces breakeven
    # Negative net_roll = debit paid    → raises breakeven
    breakeven_after_roll = None
    if net_cost_basis > 0 and is_short_vol:
        breakeven_after_roll = round(net_cost_basis - net_roll, 2)

    # ── 2. Annualized yield on capital ───────────────────────────────────────
    # Passarelli Ch.6: the metric that actually tells you if the roll is worth doing
    annualized_yield_pct = None
    capital = net_cost_basis if net_cost_basis > 0 else ul_price
    if is_short_vol and mid > 0 and capital > 0 and dte > 0:
        annualized_yield_pct = round((mid / capital) * (365 / dte) * 100, 1)

    # ── 3. OTM percentage ─────────────────────────────────────────────────────
    # McMillan Ch.3: distance from spot tells you how much the stock can move before
    # the call goes ITM. More OTM = more upside captured, less premium.
    otm_pct = None
    if ul_price > 0 and strike > 0:
        if is_short_vol:
            # CC/BW short call: OTM = how far above spot the strike is
            otm_pct = round(((strike - ul_price) / ul_price) * 100, 1)
        else:
            # Long call: ITM/OTM vs spot
            otm_pct = round(((strike - ul_price) / ul_price) * 100, 1)

    # ── 4. Prob OTM at expiry (delta approximation) ───────────────────────────
    # Natenberg Ch.8: delta ≈ N(d1) ≈ probability of expiring ITM for calls.
    # Prob expiring worthless (OTM at expiry) ≈ 1 - delta for short calls.
    prob_otm_at_expiry = None
    if is_short_vol and delta > 0:
        prob_otm_at_expiry = round((1.0 - delta) * 100, 1)  # % probability expires worthless

    # ── 5. Theta per day ($) ─────────────────────────────────────────────────
    # Passarelli Ch.6: daily theta income tells you what you're "earning" while you wait.
    # Theta from chain is per-share per-day (negative). Multiply by 100 for per-contract.
    theta_per_day_dollars = None
    if theta != 0 and contracts > 0:
        # For short vol: we RECEIVE theta (positive income)
        theta_per_day_dollars = round(abs(theta) * 100 * contracts, 2)

    # ── 6. IV vs assignment risk cross-check ─────────────────────────────────
    # If IV of new candidate is much higher than current, that's a warning:
    # high IV often precedes a vol crush that kills premium quickly after entry.
    # Natenberg Ch.11: don't roll into a vol spike without awareness.
    iv_vs_current = None
    current_iv = float(cand.get("roll_from_iv", 0) or 0)
    if current_iv > 0 and iv > 0:
        iv_change_pct = round(((iv - current_iv) / current_iv) * 100, 1)
        iv_vs_current = iv_change_pct  # positive = new strike has higher IV

    return {
        "breakeven_after_roll":    breakeven_after_roll,
        "annualized_yield_pct":    annualized_yield_pct,
        "otm_pct":                 otm_pct,
        "prob_otm_at_expiry":      prob_otm_at_expiry,
        "theta_per_day_dollars":   theta_per_day_dollars,
        "iv_vs_current_pct":       iv_vs_current,
        "cum_premium_collected":   round(cum_premium, 2) if cum_premium else None,
    }


def _build_roll_rationale(
    candidate: Dict[str, Any],
    strategy_key: str,
    ul_price: float,
    net_cost_basis: float = 0.0,
    roll_mode: str = _ROLL_MODE_NORMAL,
    position_signals: Optional[Dict[str, Any]] = None,
    calendar_note: str = "",
) -> str:
    """
    Trader-quality rationale: the numbers that actually drive the roll decision.

    A real trader evaluating a roll asks in this order:
      1. What am I rolling FROM and TO (strike, expiry, moneyness)?
      2. What does this cost/earn?
      3. What is my new breakeven — can I profit if assigned?
      4. What yield does this generate on my capital?
      5. What is the probability this expires worthless?
      6. How much theta income per day while I hold this?
      7. Is there a vol risk I should know about?

    McMillan Ch.3 / Passarelli Ch.6 / Natenberg Ch.11
    """
    strike   = candidate.get("strike")
    expiry   = candidate.get("expiry", "?")
    dte      = candidate.get("dte", "?")
    delta    = candidate.get("delta")
    iv       = candidate.get("iv")
    liq      = candidate.get("liq_grade", "?")
    oi       = candidate.get("oi", 0)
    cost     = candidate.get("cost_to_roll", {})
    net      = cost.get("net_per_contract")
    ctype    = cost.get("type", "")
    contracts = cost.get("contracts", 1)

    # Derived trader metrics (computed by _compute_trader_metrics)
    be_after   = candidate.get("breakeven_after_roll")
    yield_pct  = candidate.get("annualized_yield_pct")
    prob_otm   = candidate.get("prob_otm_at_expiry")
    theta_day  = candidate.get("theta_per_day_dollars")
    iv_chg     = candidate.get("iv_vs_current_pct")
    otm_pct    = candidate.get("otm_pct")

    is_short_vol = strategy_key in ("BUY_WRITE", "COVERED_CALL", "SHORT_PUT", "CSP")

    # ── Moneyness label ───────────────────────────────────────────────────────
    moneyness = ""
    if strike is not None and ul_price > 0:
        pct = (strike - ul_price) / ul_price * 100
        if abs(pct) < 0.5:
            moneyness = "ATM"
        elif pct > 0:
            moneyness = f"{pct:+.1f}% OTM"
        else:
            moneyness = f"{abs(pct):.1f}% ITM"

    # ── Cost string ───────────────────────────────────────────────────────────
    # Use pre-computed net_total (net_per_contract × contracts × 100) from cost_to_roll dict.
    # Do NOT recompute here — net_per_contract × contracts misses the ×100 multiplier.
    if net is not None:
        total_net = cost.get("net_total")  # already includes ×100
        total_str = f"${abs(total_net):.0f}" if total_net is not None else f"${abs(net * contracts * 100):.0f}"
        if ctype == "credit":
            cost_str = f"Net credit ${abs(net):.2f}/share ({total_str} total received)"
        else:
            cost_str = f"Net debit ${abs(net):.2f}/share ({total_str} total paid)"
    else:
        cost_str = "execution cost TBD"

    # ── Format key metrics ────────────────────────────────────────────────────
    iv_str      = f"{iv:.0%}" if iv else "?"
    delta_str   = f"{delta:.3f}" if delta is not None else "?"
    be_str      = f"${be_after:.2f}" if be_after is not None else "?"
    yield_str   = f"{yield_pct:.1f}%/yr" if yield_pct is not None else "?"
    prob_str    = f"{prob_otm:.0f}%" if prob_otm is not None else "?"
    theta_str   = f"${theta_day:.2f}/day" if theta_day is not None else "?"
    cp_label    = "C" if "CALL" in strategy_key or strategy_key in ("BUY_WRITE", "COVERED_CALL") else "P"

    # ── Vol risk warning ──────────────────────────────────────────────────────
    iv_warning = ""
    if iv_chg is not None and is_short_vol and iv_chg > 20:
        iv_warning = (
            f" ⚠️ IV on candidate is {iv_chg:+.0f}% vs current — "
            f"elevated IV may crush quickly after entry (Natenberg Ch.11: vol risk)."
        )
    elif iv_chg is not None and not is_short_vol and iv_chg < -20:
        iv_warning = (
            f" ⚠️ IV on candidate is {iv_chg:+.0f}% vs current — "
            f"buying compressed vol may not recover in time."
        )

    # ── Breakeven context for BW/CC ───────────────────────────────────────────
    be_context = ""
    if be_after is not None and strike is not None and is_short_vol:
        if strike >= be_after:
            be_context = f" If assigned at ${strike:.0f}: +${(strike - be_after):.2f}/share profit."
        else:
            be_context = (
                f" ⚠️ If assigned at ${strike:.0f}: −${(be_after - strike):.2f}/share loss vs net basis. "
                f"Consider rolling higher."
            )

    # ── Margin carry context ───────────────────────────────────────────────────
    margin_note = ""
    if yield_pct is not None and is_short_vol:
        margin_rate_pct = FIDELITY_MARGIN_RATE * 100
        if yield_pct < margin_rate_pct:
            margin_note = (
                f" ⚠️ Yield {yield_pct:.1f}%/yr BELOW Fidelity margin 10.375%/yr — "
                f"negative carry, this roll loses ${(margin_rate_pct - yield_pct):.1f}%/yr on financing."
            )
        else:
            carry_cover = yield_pct / margin_rate_pct
            margin_note = f" Covers margin {carry_cover:.1f}× (10.375%/yr Fidelity rate)."

    # ── Roll mode prefix ──────────────────────────────────────────────────────
    _mode_prefix = ""
    if roll_mode == _ROLL_MODE_WEEKLY:
        _mode_prefix = "🟡 [WEEKLY CYCLE — fragile position] "
    elif roll_mode == _ROLL_MODE_PRE_ITM:
        _mode_prefix = "⚠️ [PRE-ITM WINDOW] "
    elif roll_mode == _ROLL_MODE_EMERGENCY:
        _emg_strike = candidate.get("strike", 0) or 0
        _basis_gap  = (_emg_strike - net_cost_basis) if net_cost_basis > 0 and _emg_strike > 0 else None
        _emg_basis  = (
            f"Strike ${_emg_strike:.2f} is ${_basis_gap:.2f} ABOVE net cost ${net_cost_basis:.2f}. "
            if _basis_gap is not None and _basis_gap > 0
            else ""
        )
        _mode_prefix = f"🚨 [EMERGENCY — extended DTE search] {_emg_basis}"

    # ── Debit roll judgment (BUY_WRITE / COVERED_CALL only) ──────────────────
    # When the roll requires a net debit, the engine answers three questions
    # that determine whether the debit is justified (McMillan Ch.3).
    #
    # Weighted scoring (mirrors professional hierarchy):
    #   Trend strength  40% — ADX is the most durable signal. A confirmed trend
    #                         is the primary justification for paying a debit.
    #                         McMillan Ch.3: "roll the debit only in a confirmed trend."
    #   Momentum        30% — Near-term continuation (ROC, slope, velocity state).
    #                         More reactive than ADX but confirms short-term thesis.
    #   IV fairness     30% — Cost check: is the buy-back / new premium fairly priced?
    #                         Does not affect the thesis, only the cost of the roll.
    #
    # Score interpretation:
    #   ≥ 0.70  → ✅ Justified     — thesis intact, cost fair
    #   0.40–0.69 → ⚠️ Marginal   — proceed only if directional conviction is high
    #   < 0.40  → ❌ Questionable  — consider letting expire and re-selling next cycle
    _debit_judgment = ""
    if ctype == "debit" and net is not None and position_signals:
        _adx        = float(position_signals.get("adx_14") or 0)
        _roc20      = float(position_signals.get("roc_20") or 0)
        _mom_slope  = float(position_signals.get("momentum_slope") or 0)
        _mom_raw    = position_signals.get("MomentumVelocity_State") or ""
        _mom_state  = (getattr(_mom_raw, 'value', None) or str(_mom_raw).split('.')[-1]).upper()
        _iv_hv_gap  = float(position_signals.get("iv_vs_hv_gap") or 0)
        _drift_dir  = str(position_signals.get("drift_direction") or "").upper()
        _drift_mag  = str(position_signals.get("drift_magnitude") or "").upper()

        # Q1 (40%): Trend strength — ADX is the anchor signal.
        # ADX ≥ 25: confirmed trend (full score). 18–24: developing (partial). <18: choppy (zero).
        # McMillan Ch.3 / Natenberg Ch.8: ADX > 25 = directional conviction confirmed.
        if _adx >= 25:
            _q1_score = 1.0
            _q1_label = f"ADX={_adx:.0f} ✅ confirmed trend"
        elif _adx >= 18:
            _q1_score = 0.5
            _q1_label = f"ADX={_adx:.0f} ⚠️ developing trend"
        else:
            _q1_score = 0.0
            _q1_label = f"ADX={_adx:.0f} ❌ no trend / choppy"

        # Q2 (30%): Upside price momentum — ROC20 + velocity state + slope.
        # Uses PRICE momentum signals only. Drift_Direction/Magnitude are Greek drift
        # (delta ROC) — not stock price direction — and must NOT be used here.
        # roc_20 is stored as a raw percentage (e.g. 6.18 = 6.18%), not decimal.
        _roc20_str  = f"{_roc20:+.1f}%"
        _q2_roc_ok  = _roc20 > 2.0   # >+2% price ROC (not Greek ROC)
        _q2_mom_ok  = any(s in _mom_state for s in ("STRONG", "BULLISH", "TRENDING"))
        _q2_slp_ok  = _mom_slope > 0
        if _q2_roc_ok and (_q2_mom_ok or _q2_slp_ok):
            _q2_score = 1.0
            _q2_label = f"ROC20={_roc20_str} ✅ positive momentum"
        elif _q2_roc_ok or _q2_slp_ok:
            _q2_score = 0.5
            _q2_label = f"ROC20={_roc20_str} ⚠️ weak momentum"
        else:
            _q2_score = 0.0
            _q2_label = f"ROC20={_roc20_str} ❌ negative / no momentum"

        # Q3 (30%): IV fairness — is the cost of the roll reasonable?
        # IV < HV (gap < 0): buying back the short at fair/cheap vol. Full score.
        # IV ≈ HV (gap 0–5pt): acceptable. Partial.
        # IV > HV + 5pt: buying back expensive optionality. Score zero.
        if _iv_hv_gap <= 0:
            _q3_score = 1.0
            _q3_label = f"IV-HV={_iv_hv_gap:+.1f}pt ✅ IV cheap/fair"
        elif _iv_hv_gap <= 5.0:
            _q3_score = 0.5
            _q3_label = f"IV-HV={_iv_hv_gap:+.1f}pt ⚠️ IV slightly elevated"
        else:
            _q3_score = 0.0
            _q3_label = f"IV-HV={_iv_hv_gap:+.1f}pt ❌ IV rich — buy-back expensive"

        # Weighted composite score
        _score = round(0.40 * _q1_score + 0.30 * _q2_score + 0.30 * _q3_score, 2)

        # Hard veto: REVERSING momentum state caps verdict at Marginal regardless of score.
        # McMillan Ch.3: "do not pay a debit into a reversal — wait for momentum to stabilise."
        _mom_reversing = any(s in _mom_state for s in ("REVERSING", "LATE_CYCLE", "EXHAUSTED"))
        if _score >= 0.70 and _mom_reversing:
            _verdict = (
                f"⚠️ Marginal (score {_score:.0%}, capped) — "
                f"momentum state={_mom_state.title()} vetoes Justified; "
                "wait for reversal to stabilise before paying debit"
            )
        elif _score >= 0.70:
            _verdict = f"✅ Justified (score {_score:.0%}) — thesis intact, debit defensible"
        elif _score >= 0.40:
            _verdict = f"⚠️ Marginal (score {_score:.0%}) — proceed only with strong directional conviction"
        else:
            _verdict = f"❌ Questionable (score {_score:.0%}) — consider letting expire, re-sell next cycle"

        _debit_judgment = (
            f" | Debit roll judgment: {_verdict}. "
            f"Trend 40%: {_q1_label}. "
            f"Momentum 30%: {_q2_label}. "
            f"IV 30%: {_q3_label}. "
            f"(McMillan Ch.3: pay debit only when bullish thesis confirmed)"
        )

    # ── Assemble rationale by strategy type ──────────────────────────────────
    if strategy_key in ("BUY_WRITE", "COVERED_CALL"):
        return (
            f"{_mode_prefix}Roll to {strike}{cp_label} {expiry} ({dte}d, {moneyness}) | "
            f"δ={delta_str} IV={iv_str} OI={oi:,} liq={liq} | "
            f"{cost_str} | "
            f"Yield: {yield_str} on capital{margin_note} · θ={theta_str} income · "
            f"{prob_str} prob expires worthless · "
            f"New breakeven: {be_str}.{be_context}{iv_warning}"
            f"{_debit_judgment} "
            f"(McMillan Ch.3 / Passarelli Ch.6)"
            f"{calendar_note}"
        )
    elif strategy_key in ("SHORT_PUT", "CSP"):
        return (
            f"Roll to {strike}{cp_label} {expiry} ({dte}d, {moneyness}) | "
            f"δ={delta_str} IV={iv_str} OI={oi:,} liq={liq} | "
            f"{cost_str} | "
            f"Yield: {yield_str}{margin_note} · θ={theta_str}/day · "
            f"{prob_str} prob expires worthless · "
            f"Assignment price: ${strike:.2f}.{iv_warning} "
            f"(McMillan Ch.6 / Passarelli Ch.6)"
            f"{calendar_note}"
        )
    elif strategy_key in ("LONG_CALL", "LONG_PUT", "LEAP"):
        # Time value % of mid — what fraction of the premium you're paying is pure extrinsic
        _mid = candidate.get("mid", 0) or 0
        _intrinsic = max(0.0, (ul_price - strike) if cp_label == "C" else (strike - ul_price)) if strike else 0.0
        _extrinsic = max(0.0, _mid - _intrinsic)
        _tv_pct_str = f"{_extrinsic/_mid:.0%} extrinsic" if _mid > 0 else "extrinsic: ?"

        # IV vs HV edge — complete sentence (Natenberg Ch.11: buy cheap vol when IV < HV)
        _hv = candidate.get("hv_20d")
        if iv and _hv and _hv > 0:
            _iv_hv_ratio = iv / _hv
            if _iv_hv_ratio < 0.85:
                _iv_hv_str = f"IV={iv_str} vs HV={_hv:.0%} — cheap vol edge (IV/HV={_iv_hv_ratio:.2f})"
            elif _iv_hv_ratio > 1.20:
                _iv_hv_str = f"IV={iv_str} vs HV={_hv:.0%} — buying rich vol (IV/HV={_iv_hv_ratio:.2f}) ⚠️"
            else:
                _iv_hv_str = f"IV={iv_str} vs HV={_hv:.0%} — fair vol (IV/HV={_iv_hv_ratio:.2f})"
        elif iv:
            _iv_hv_str = f"IV={iv_str} (HV unavailable)"
        else:
            _iv_hv_str = "IV unavailable"

        return (
            f"Roll to {strike}{cp_label} {expiry} ({dte}d, {moneyness}) | "
            f"δ={delta_str} IV={iv_str} OI={oi:,} liq={liq} | "
            f"{cost_str} | "
            f"{_tv_pct_str} · θ cost={theta_str} · "
            f"{_iv_hv_str}.{iv_warning} "
            f"(McMillan Ch.4 / Natenberg Ch.11)"
            f"{calendar_note}"
        )
    else:
        return (
            f"Roll to {strike} {expiry} ({dte}d, {moneyness}) | "
            f"δ={delta_str} IV={iv_str} liq={liq} | "
            f"{cost_str}.{iv_warning}"
            f"{calendar_note}"
        )


# ── Roll viability assessment ─────────────────────────────────────────────────

def _assess_roll_viability(
    ul_price: float,
    net_cost_basis: float,
    current_strike: float,
    current_delta: float,
    strategy_key: str,
) -> Dict[str, Any]:
    """
    Called when EMERGENCY mode search finds zero viable above-basis candidates.

    Returns a structured verdict dict that Roll_Candidate_1 is set to, so
    the dashboard and checklist can surface an explicit "no viable roll" message
    rather than silently showing no candidates.

    McMillan Ch.3: "If the only rolls available are below your cost basis, you
    face a binary choice: pay a debit to buy time (trading dollars for days), or
    accept assignment and redeploy capital. Only accept the debit if your thesis
    remains intact and the stock is likely to recover above your basis before
    the next expiry."

    Verdict logic:
      - ASSIGNMENT_PREFERABLE : stock price > net_cost_basis → assignment is profitable
      - DEBIT_ROLL_EVALUATE   : stock price < net_cost_basis → assignment locks in loss;
                                evaluate debit roll vs holding stock naked
      - NO_DATA               : insufficient basis data to evaluate
    """
    verdict = "NO_DATA"
    rationale = "Insufficient cost basis data to evaluate. Run pipeline to populate."

    if net_cost_basis > 0 and ul_price > 0:
        loss_at_assignment = net_cost_basis - current_strike  # positive = loss
        if current_strike >= net_cost_basis:
            # Assignment at current strike is already profitable — just take it
            profit = current_strike - net_cost_basis
            verdict   = "ASSIGNMENT_PREFERABLE"
            rationale = (
                f"No above-basis credit roll found in 45–150 DTE range. "
                f"Assignment at ${current_strike:.2f} is PROFITABLE (+${profit:.2f}/share above net cost ${net_cost_basis:.2f}). "
                f"Accept assignment and redeploy capital. "
                f"(McMillan Ch.3: when assignment is profitable, no need to roll)"
            )
        else:
            # Assignment at current strike locks in a loss
            loss = net_cost_basis - current_strike
            stock_recovery_needed = net_cost_basis - ul_price
            verdict   = "DEBIT_ROLL_EVALUATE"
            rationale = (
                f"No viable above-basis roll found in 45–150 DTE extended search. "
                f"Assignment at ${current_strike:.2f} would realize −${loss:.2f}/share loss "
                f"vs net cost ${net_cost_basis:.2f}. "
                f"Stock at ${ul_price:.2f} needs +${stock_recovery_needed:.2f}/share (+{stock_recovery_needed/ul_price:.0%}) "
                f"to reach breakeven. "
                f"Options: (1) Accept assignment and harvest tax loss; "
                f"(2) Pay debit to roll 90+ DTE and hold for recovery — only if thesis intact; "
                f"(3) Buy back call now and hold stock naked if strongly bullish. "
                f"(McMillan Ch.3: no economically rational covered call roll exists at this time)"
            )

    return {
        "verdict":              verdict,
        "roll_mode":            _ROLL_MODE_EMERGENCY,
        "no_viable_roll":       True,
        "current_strike":       round(current_strike, 2),
        "net_cost_basis":       round(net_cost_basis, 2) if net_cost_basis else None,
        "ul_price":             round(ul_price, 2),
        "current_delta":        round(current_delta, 3),
        "roll_rationale":       rationale,
        "score":                0.0,
    }


# ── Strategy key resolution ───────────────────────────────────────────────────

def _strategy_key(strategy: str, dte: float) -> str:
    """Map df Strategy column to our config keys."""
    s = (strategy or "").upper()
    if "BUY_WRITE" in s:
        return "BUY_WRITE"
    if "COVERED_CALL" in s:
        return "COVERED_CALL"
    if "SHORT_PUT" in s or "CSP" in s:
        return "SHORT_PUT"
    if "LONG_CALL" in s or "LONG_PUT" in s:
        return "LEAP" if dte >= 270 else ("LONG_CALL" if "CALL" in s else "LONG_PUT")
    return "LONG_CALL"


# ── Chain fetch ───────────────────────────────────────────────────────────────

def _get_chain(ticker: str, schwab_client, session_cache: dict) -> Optional[dict]:
    """Fetch chain with session cache. Reuses LiveGreeksProvider cache if available."""
    if ticker in session_cache:
        return session_cache[ticker]
    try:
        schwab_client.ensure_valid_token()
        chain = schwab_client.get_chains(
            symbol     = ticker,
            strikeCount= 30,      # Wider range for roll candidates
            range      = "ALL",
            strategy   = "SINGLE",
        )
        if chain:
            session_cache[ticker] = chain
        return chain
    except Exception as e:
        logger.warning(f"[RollEngine] Chain fetch failed for {ticker}: {e}")
        return None


# ── Roll timing intelligence ───────────────────────────────────────────────────

def _get_roll_timing(row: pd.Series) -> str:
    """
    Classify current market timing for roll candidate selection.
    Returns: "BREAKOUT_UP" | "BREAKOUT_DOWN" | "CHOPPY" | "RELEASING" | "NEUTRAL"

    This is a simplified version of DoctrineAuthority._classify_roll_timing(),
    reimplemented here to avoid a circular import. Uses the same signal columns.
    """
    chop     = float(row.get('choppiness_index', 50) or 50)
    ker      = float(row.get('kaufman_efficiency_ratio', 0.5) or 0.5)
    adx      = float(row.get('adx_14', 25) or 25)
    roc_5    = float(row.get('roc_5', 0) or 0)
    roc_10   = float(row.get('roc_10', 0) or 0)
    bb_z     = float(row.get('bb_width_z', 0) or 0)

    def _sn(col):
        v = row.get(col, '') or ''
        return (getattr(v, 'value', None) or str(v).split('.')[-1]).upper()

    range_eff = _sn('RangeEfficiency_State')
    trend_int = _sn('TrendIntegrity_State')
    mom_vel   = _sn('MomentumVelocity_State')
    dir_bal   = _sn('DirectionalBalance_State')
    comp_mat  = _sn('CompressionMaturity_State')

    if (trend_int == 'STRONG_TREND' and dir_bal == 'BUYER_DOMINANT'
            and mom_vel in ('ACCELERATING', 'TRENDING') and ker > 0.55
            and roc_5 > 2.0 and chop < 50):
        return "BREAKOUT_UP"

    if (dir_bal == 'SELLER_DOMINANT' and mom_vel in ('ACCELERATING', 'TRENDING')
            and roc_5 < -2.0 and roc_10 < -4.0 and chop < 55):
        return "BREAKOUT_DOWN"

    if (chop > 61.8 and ker < 0.35
            and range_eff in ('INEFFICIENT_RANGE', 'NOISY')
            and trend_int in ('NO_TREND', 'TREND_EXHAUSTED')
            and adx < 20 and abs(roc_5) < 2.0):
        return "CHOPPY"

    if (comp_mat in ('RELEASING', 'POST_EXPANSION') and bb_z > 0.5 and adx < 25):
        return "RELEASING"

    return "NEUTRAL"


def _adjust_for_timing(
    timing: str,
    strategy_key: str,
    dte_window: Tuple[int, int, int],
    delta_range: Tuple[float, float],
    ul_price: float,
    current_strike: float,
) -> Tuple[Tuple[int, int, int], Tuple[float, float]]:
    """
    Adjust roll search parameters based on market timing.

    McMillan Ch.3 breakout adjustments:
      BREAKOUT_UP  → roll further OTM (higher strike), further DTE.
                     Give the stock room to run; collect more time premium.
                     Use OTM buffer of ~5-8% from spot.
      BREAKOUT_DOWN → roll closer-in (shorter DTE ok, lower debit).
                     Cheap defensive roll — minimize cost, don't over-reach.
      CHOPPY       → widen DTE slightly (wait for better strike to become available)
                     but tighten delta to avoid paying for strikes that whipsaw.
      RELEASING    → neutral parameters — breakout not yet confirmed.
      NEUTRAL      → no adjustment, use defaults.

    Returns adjusted (dte_window, delta_range).
    """
    min_dte, max_dte, target_dte = dte_window
    delta_min, delta_max = delta_range
    is_short_vol = strategy_key in ("BUY_WRITE", "COVERED_CALL", "SHORT_PUT", "CSP")

    if timing == "BREAKOUT_UP" and is_short_vol:
        # Roll further OTM (lower delta = more room for stock to move)
        # Roll further in time (more premium, more room to be wrong on timing)
        return (
            (min_dte, max_dte + 30, target_dte + 15),   # extend DTE window
            (max(0.10, delta_min - 0.08), max(0.30, delta_max - 0.08)),  # shift OTM
        )

    if timing == "BREAKOUT_DOWN" and is_short_vol:
        # Defensive: shorter DTE is fine (cheaper to close current),
        # slightly tighten delta (don't want too far OTM if stock keeps dropping)
        return (
            (min_dte, min(max_dte, target_dte + 15), target_dte),  # tighter window
            (delta_min, min(delta_max, 0.35)),  # moderate delta cap
        )

    if timing == "CHOPPY" and is_short_vol:
        # Extend DTE slightly — wait for better strikes to become meaningful.
        # Tighten delta lower bound to avoid thin OTM strikes that whipsaw.
        return (
            (min_dte + 7, max_dte + 14, target_dte + 7),
            (max(0.15, delta_min), min(0.35, delta_max)),  # tighter range
        )

    # RELEASING, NEUTRAL, long vol strategies — use defaults unchanged
    return dte_window, delta_range
