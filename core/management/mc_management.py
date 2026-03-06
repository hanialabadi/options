"""
Monte Carlo — Management System
================================

Four distinct MC functions for active position management.
Each answers a question that rule-based doctrine cannot answer with
a point estimate — it needs a *distribution* over future paths.

Functions
---------

1. mc_roll_wait_cost(row, wait_days, ...)
   → "If I wait N days before rolling, what happens to the roll credit
      and assignment risk?"
   Used by: _apply_execution_readiness() to augment STAGE_AND_RECHECK
   rationale with: P(credit improves ≥ 20%), P(assignment breach in wait window)

2. mc_exit_vs_hold(row, ...)
   → "What is the probability this position recovers to breakeven vs
      decaying to max loss over remaining DTE?"
   Used by: HOLD gate in engine — when doctrine says HOLD, MC adds
   P(recovery) and P(max_loss) to the rationale so the trader can
   size the risk of waiting.

3. mc_assignment_risk(row, ...)
   → "What is the probability the short strike is ITM at expiry?"
   Used by: CSP / CC / BUY_WRITE income positions — continuous gauge
   that updates every pipeline run. Triggers urgency escalation when
   P(assign) crosses thresholds.

4. mc_triple_barrier(row, ...)
   → "Which barrier is hit first: profit-take, stop-loss, or time-expiry?"
   Used by: all option positions — adds triple-barrier probabilities
   that inform whether the position is likely to exit via gain, loss,
   or time decay. Strategy-aware verdicts (income vs directional).

Design principles
-----------------
- GBM only (no jumps) — conservative; doesn't model earnings gaps.
  Earnings proximity is handled by doctrine gates separately.
- HV_20D preferred (already in management row); falls back to IV_30D → 0.25
- All results are per-run snapshots. They don't replace doctrine — they
  *augment* the rationale text and add MC_* columns for dashboard display.
- Non-blocking: any exception returns a safe default dict.

References
----------
  Natenberg Ch.12: position sizing from P10 loss distribution
  McMillan Ch.3:   2% account hard cap per trade
  Passarelli Ch.6: decouple roll decision from roll execution timing
  Cohen Ch.5:      probability of assignment as core income management metric
"""

from __future__ import annotations

import json
import logging
import numpy as np
import pandas as pd
from typing import Optional

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────
N_PATHS       = 2_000
TRADING_DAYS  = 252
HV_FALLBACK   = 0.25     # 25% annualised — conservative management fallback
SEED          = 42

# Regime-aware sigma selector — replaces flat EWMA as Priority 1 sigma source.
# Falls back gracefully to EWMA → static HV if unavailable.
try:
    from core.management.regime_sigma_selector import regime_sigma as _regime_sigma
    _REGIME_SIGMA_AVAILABLE = True
except Exception:
    _REGIME_SIGMA_AVAILABLE = False
    _regime_sigma = None  # type: ignore[assignment]

# EWMA import — kept as fallback if regime_sigma unavailable
try:
    import sys as _sys
    import os as _os
    _repo = _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))
    if _repo not in _sys.path:
        _sys.path.insert(0, _repo)
    from scan_engine.ewma_vol import ewma_vol as _ewma_vol
    _EWMA_AVAILABLE = True
except Exception:
    _EWMA_AVAILABLE = False
    _ewma_vol = None  # type: ignore[assignment]


# ── Shared helpers ────────────────────────────────────────────────────────────

def _hv(row: pd.Series) -> float:
    """
    Best available annualised HV as decimal from a management row.

    Priority:
      1. EWMA(λ=0.94) from price_history DuckDB — forward-leaning
      2. Static HV columns from management row
      3. HV_FALLBACK (25%)
    """
    hv, _ = _hv_with_source(row)
    return hv


def _hv_with_source(row: pd.Series) -> tuple[float, str]:
    """
    Same as _hv() but also returns a source label for audit/display.

    Returns (hv_decimal, source_label) where source_label is one of:
      'EWMA'     — computed from EWMA(λ=0.94) on recent price history
      'HV_20D'   — 20-day rolling HV from management row
      'HV_30D'   — 30-day rolling HV from management row
      'IV_30D'   — IV proxy (HV unavailable)
      'FALLBACK' — hardcoded 25% fallback
    """
    # Priority 1: Regime-aware sigma (HMM blend) — replaces flat EWMA
    # Falls back internally to EWMA if HMM unavailable or insufficient history.
    ticker = (row.get("Ticker") or row.get("ticker")
              or row.get("Underlying_Ticker") or row.get("underlying_ticker"))
    if ticker:
        if _REGIME_SIGMA_AVAILABLE and _regime_sigma is not None:
            try:
                result = _regime_sigma(str(ticker))
                if 0.01 <= result.sigma <= 5.0:
                    # source label preserves HMM vs EWMA_FALLBACK distinction
                    return result.sigma, result.source
            except Exception:
                pass
        elif _EWMA_AVAILABLE and _ewma_vol is not None:
            # Direct EWMA fallback if regime_sigma module not importable
            try:
                ewma = _ewma_vol(str(ticker))
                if ewma is not None and 0.01 <= ewma <= 5.0:
                    return ewma, "EWMA"
            except Exception:
                pass

    # Priority 2: static columns in management row
    for col in ("HV_20D", "hv_20d", "HV_30D", "hv_30", "IV_30D", "iv_30d", "IV_Now"):
        val = row.get(col)
        if val is not None and pd.notna(val):
            v = float(val)
            if v > 1.0:       # stored as percentage
                v /= 100.0
            if 0.01 <= v <= 5.0:
                _label = "HV_20D" if "20" in col else ("HV_30D" if "30" in col and "IV" not in col else ("IV_30D" if "IV" in col or "iv" in col else col))
                return v, _label
    return HV_FALLBACK, "FALLBACK"


def _spot(row: pd.Series) -> Optional[float]:
    """Current underlying price from management row."""
    for col in ("UL Last", "Underlying_Last", "last_price", "Last"):
        val = row.get(col)
        if val is not None and pd.notna(val):
            v = float(val)
            if v > 0:
                return v
    return None


def _coerce_float(val) -> float:
    """Convert a value to float, returning 0.0 on None/NaN/empty string."""
    if val is None:
        return 0.0
    try:
        v = float(val)
        return v if v == v else 0.0  # NaN != NaN → 0.0
    except (TypeError, ValueError):
        return 0.0


def _short_leg(row: pd.Series) -> tuple[float, float, str]:
    """
    Resolve (strike, dte, option_type) for income positions.

    BUY_WRITE / COVERED_CALL rows carry doctrine on the STOCK leg, not the
    OPTION leg.  TradeLegEnrichment broadcasts Short_Call_* columns onto every
    leg row so they are always available — prefer those over the raw leg values.

    Returns (strike, dte, option_type) where option_type is 'c' or 'p'.
    Falls back to raw Strike / DTE / Option_Type if Short_Call_* absent.
    All values are NaN-safe: invalid or missing → 0.0.
    """
    # Short_Call_Strike / Short_Call_DTE — broadcast onto stock leg by enrichment.
    # _coerce_float handles NaN from CSV reads (pandas reads missing as float NaN).
    strike = _coerce_float(row.get("Short_Call_Strike")) or _coerce_float(row.get("Strike"))
    dte    = _coerce_float(row.get("Short_Call_DTE"))    or _coerce_float(row.get("DTE"))

    # Option type: covered calls are always calls; CSP/put strategies are puts.
    raw_type = str(row.get("Option_Type", "") or "").lower()
    strategy = str(row.get("Strategy", row.get("Entry_Structure", "")) or "").upper()
    if raw_type.startswith("p") or any(t in strategy for t in ("PUT", "CSP", "CASH_SECURED")):
        option_type = "p"
    else:
        option_type = "c"   # BUY_WRITE / COVERED_CALL default

    return strike, dte, option_type


def _gbm_terminal(spot: float, hv: float, t_years: float,
                  n: int, rng: np.random.Generator) -> np.ndarray:
    """GBM terminal prices. Risk-neutral drift = 0."""
    z      = rng.standard_normal(n)
    log_r  = (-0.5 * hv**2) * t_years + hv * np.sqrt(t_years) * z
    return spot * np.exp(log_r)


def _gbm_daily_paths(spot: float, hv: float, n_days: int,
                     n_paths: int, rng: np.random.Generator) -> np.ndarray:
    """
    GBM daily price paths for triple-barrier simulation.

    Returns shape (n_paths, n_days + 1) where column 0 = spot.
    Risk-neutral drift = 0. Daily step = σ/√252.

    Lopez de Prado (0.683): "The triple-barrier method requires
    simulating full paths, not just terminal values."
    """
    dt = 1.0 / TRADING_DAYS
    drift = -0.5 * hv**2 * dt
    vol = hv * np.sqrt(dt)
    z = rng.standard_normal((n_paths, n_days))
    log_returns = drift + vol * z
    # Cumulative sum of log returns, prepend 0 for initial spot
    cum_log = np.concatenate(
        [np.zeros((n_paths, 1)), np.cumsum(log_returns, axis=1)], axis=1
    )
    return spot * np.exp(cum_log)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Roll Wait-Cost
# ─────────────────────────────────────────────────────────────────────────────

def mc_roll_wait_cost(
    row: pd.Series,
    roll_candidate: Optional[dict] = None,
    wait_days: int = 3,
    n_paths: int = N_PATHS,
    rng: Optional[np.random.Generator] = None,
) -> dict:
    """
    Simulate the cost/benefit of waiting `wait_days` before executing a roll.

    Uses the current position's short strike and the roll candidate's target
    strike to estimate:
      - P(credit improves ≥ 20%) — fraction of paths where waiting produces
        a materially better roll credit (Passarelli Ch.6: "wait for a dip")
      - P(assignment breach) — fraction of paths where underlying crosses
        current short strike during the wait window (triggers immediate action)
      - expected_credit_delta — median change in roll credit over wait period

    Parameters
    ----------
    row            : management position row (from positions_latest.csv)
    roll_candidate : dict from Roll_Candidate_1 JSON (may be None)
    wait_days      : number of trading days to simulate waiting
    n_paths, rng   : MC parameters

    Returns
    -------
    dict with keys:
      MC_Wait_P_Improve    – P(credit improves ≥ 20%) [0-1]
      MC_Wait_P_Assign     – P(short strike breach in wait window) [0-1]
      MC_Wait_Credit_Delta – median roll credit change ($, per contract)
      MC_Wait_Days         – wait_days used
      MC_Wait_Note         – human-readable summary
      MC_Wait_Verdict      – 'WAIT' | 'ACT_NOW' | 'SKIP' (MC skip)
    """
    _default = {
        "MC_Wait_P_Improve":    np.nan,
        "MC_Wait_P_Assign":     np.nan,
        "MC_Wait_Credit_Delta": np.nan,
        "MC_Wait_Days":         wait_days,
        "MC_Wait_Note":         "MC_SKIP",
        "MC_Wait_Verdict":      "SKIP",
    }

    spot = _spot(row)
    if spot is None:
        _default["MC_Wait_Note"] = "MC_SKIP: no spot price"
        return _default

    short_strike, dte, _opt_type = _short_leg(row)
    if short_strike <= 0:
        _default["MC_Wait_Note"] = "MC_SKIP: no short strike"
        return _default

    if dte < 1:
        _default["MC_Wait_Note"] = "MC_SKIP: DTE < 1"
        return _default

    hv, hv_src = _hv_with_source(row)
    is_put   = _opt_type == "p"
    t_wait   = min(wait_days, int(dte)) / TRADING_DAYS
    t_expiry = dte / TRADING_DAYS

    # Detect long options (LONG_CALL, LONG_PUT, LEAPS) — breach semantics are inverted:
    # For income/short: breach = stock crosses short strike (adverse event → ACT_NOW)
    # For long options: ITM = desired. Adverse = stock moves AGAINST the option direction.
    #   LONG_PUT adverse = stock rises ABOVE strike (put goes OTM)
    #   LONG_CALL adverse = stock falls BELOW strike (call goes OTM)
    strategy = str(row.get("Strategy", row.get("Entry_Structure", "")) or "").upper()
    _long_strategies = {"LONG_CALL", "LONG_PUT", "LEAPS_CALL", "LEAPS_PUT",
                        "BUY_CALL", "BUY_PUT", "LEAP_CALL", "LEAP_PUT"}
    is_long_option = any(s in strategy for s in _long_strategies)

    if rng is None:
        rng = np.random.default_rng(SEED)

    # ── Simulate wait-period paths ─────────────────────────────────────────
    s_wait = _gbm_terminal(spot, hv, t_wait, n_paths, rng)

    if is_long_option:
        # For long options: p_assign_wait = P(adverse move — option goes further OTM)
        # LONG_PUT: adverse = stock rises above strike
        # LONG_CALL: adverse = stock falls below strike
        if is_put:
            adverse = s_wait > short_strike   # stock rises above put strike = put loses value
        else:
            adverse = s_wait < short_strike   # stock falls below call strike = call loses value
        p_assign_wait = float(np.mean(adverse))   # P(adverse move in wait window)
    else:
        # Short options: adverse breach = stock crosses short strike (assignment risk)
        if is_put:
            breach = s_wait < short_strike
        else:
            breach = s_wait > short_strike
        p_assign_wait = float(np.mean(breach))

    # ── Roll credit delta: how much better/worse is the roll if we wait? ──
    # Proxy: roll credit ≈ current_short_iv × f(moneyness, remaining_dte)
    # Simplified: roll credit changes proportionally to option extrinsic change.
    # We approximate this as: credit_today = row["mid"] (current option mid)
    # After wait: option mid shifts as underlying moves. We use Black-Scholes
    # intrinsic + extrinsic approximation.
    current_mid = float(row.get("Last", row.get("Premium_Entry", 0)) or 0)

    # For each path, approximate new option mid at S_wait using intrinsic proxy
    t_remain = (dte - wait_days) / TRADING_DAYS
    if t_remain > 0 and hv > 0:
        # Extrinsic approximation: σ√T_remain × S_wait × ATM_fraction
        # Good enough for the credit-improvement question (not pricing precision)
        if is_put:
            intrinsic_wait = np.maximum(short_strike - s_wait, 0.0)
        else:
            intrinsic_wait = np.maximum(s_wait - short_strike, 0.0)
        extrinsic_wait = hv * np.sqrt(t_remain) * s_wait * 0.4  # 0.4 ≈ ATM N(d2) proxy
        mid_wait = intrinsic_wait + extrinsic_wait
        credit_delta = mid_wait - current_mid   # positive = option gained value

        if is_long_option:
            # For long options: "better roll" = option gained more value (directional gain)
            # credit_delta > 0 = option is worth more → roll position is improving
            credit_delta_per_contract = credit_delta * 100
            # p_improve for long options: P(option value increased ≥20% of current mid)
            p_improve = float(np.mean(credit_delta_per_contract >= current_mid * 100 * 0.20))
        else:
            # For income seller: "better roll" = option got cheaper → collect MORE net credit
            credit_delta_per_contract = -credit_delta * 100  # negative option price = better credit for closer
            p_improve = float(np.mean(credit_delta_per_contract >= current_mid * 100 * 0.20))
        median_credit_delta = float(np.median(credit_delta_per_contract))
    else:
        p_improve = 0.0
        median_credit_delta = 0.0

    # ── Verdict ────────────────────────────────────────────────────────────
    if is_long_option:
        # For long options: verdict based on adverse move risk (option going OTM)
        # and whether waiting is likely to improve option value.
        # ACT_NOW: adverse move likely (option at risk of losing value) AND no improvement expected
        # WAIT:    improvement likely AND adverse risk low
        # HOLD:    no clear signal either way
        if p_assign_wait >= 0.40 and p_improve < 0.20:
            verdict = "ACT_NOW"
            verdict_reason = f"high risk option goes OTM in {wait_days}d ({p_assign_wait:.0%}) and value improvement unlikely"
        elif p_improve >= 0.35 and p_assign_wait < 0.25:
            verdict = "WAIT"
            verdict_reason = f"{p_improve:.0%} chance option gains ≥20% value if you wait {wait_days}d"
        else:
            verdict = "HOLD"
            verdict_reason = f"mixed signals: adverse risk {p_assign_wait:.0%}, improvement {p_improve:.0%}"
        note = (
            f"MC roll wait-cost ({n_paths:,}p, wait={wait_days}d, σ={hv*100:.0f}%[{hv_src}]): "
            f"P(ITM)={1-p_assign_wait:.0%} | P(value+20%)={p_improve:.0%} | "
            f"median option value Δ={median_credit_delta:+.0f}/contract → {verdict}"
        )
    else:
        # Short options: assignment breach risk drives urgency
        if p_assign_wait >= 0.25:
            verdict = "ACT_NOW"
            verdict_reason = f"assignment breach risk {p_assign_wait:.0%} in {wait_days}d wait window"
        elif p_improve >= 0.35 and p_assign_wait < 0.10:
            verdict = "WAIT"
            verdict_reason = f"{p_improve:.0%} chance credit improves ≥20% if you wait {wait_days}d"
        else:
            verdict = "ACT_NOW"
            verdict_reason = f"credit improvement unlikely ({p_improve:.0%}); execute now"
        note = (
            f"MC roll wait-cost ({n_paths:,}p, wait={wait_days}d, σ={hv*100:.0f}%[{hv_src}]): "
            f"P(thesis)={1-p_assign_wait:.0%} | P(credit+20%)={p_improve:.0%} | "
            f"median option value Δ={median_credit_delta:+.0f}/contract → {verdict}"
        )

    return {
        "MC_Wait_P_Improve":    round(p_improve, 4),
        "MC_Wait_P_Assign":     round(p_assign_wait, 4),
        "MC_Wait_Credit_Delta": round(median_credit_delta, 2),
        "MC_Wait_Days":         wait_days,
        "MC_Wait_Note":         note,
        "MC_Wait_Verdict":      verdict,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 2. Exit vs Hold
# ─────────────────────────────────────────────────────────────────────────────

def mc_exit_vs_hold(
    row: pd.Series,
    n_paths: int = N_PATHS,
    rng: Optional[np.random.Generator] = None,
) -> dict:
    """
    Simulate the distribution of outcomes if you HOLD the position to expiry.

    Answers: "If I hold, what fraction of paths recover to at-least-breakeven
    vs decay to full loss?"

    Used to quantify the cost of the HOLD decision:
      - When P(recovery) is high → HOLD is justified
      - When P(recovery) is low AND P(max_loss) is high → consider EXIT now

    Supports long options (debit) and short puts/calls (credit strategies).

    Returns
    -------
    dict with keys:
      MC_Hold_P_Recovery   – P(P&L ≥ 0 at expiry) [0-1]
      MC_Hold_P_MaxLoss    – P(option expires at max loss) [0-1]
      MC_Hold_P10          – 10th-pct P&L per contract ($)
      MC_Hold_P50          – median P&L per contract ($)
      MC_Hold_EV           – expected value per contract ($)
      MC_Hold_Note         – human-readable summary
      MC_Hold_Verdict      – 'HOLD_JUSTIFIED' | 'EXIT_NOW' | 'MONITOR' | 'SKIP'
    """
    _default = {
        "MC_Hold_P_Recovery": np.nan,
        "MC_Hold_P_MaxLoss":  np.nan,
        "MC_Hold_P10":        np.nan,
        "MC_Hold_P50":        np.nan,
        "MC_Hold_EV":         np.nan,
        "MC_Hold_Note":       "MC_SKIP",
        "MC_Hold_Verdict":    "SKIP",
    }

    spot = _spot(row)
    if spot is None:
        _default["MC_Hold_Note"] = "MC_SKIP: no spot price"
        return _default

    strike    = float(row.get("Strike", 0) or 0)
    dte       = float(row.get("DTE", 0) or 0)
    entry_px  = float(row.get("Premium_Entry", row.get("Last", 0)) or 0)
    current_px = float(row.get("Last", 0) or 0)

    if strike <= 0 or dte < 1 or entry_px <= 0:
        _default["MC_Hold_Note"] = "MC_SKIP: missing strike/DTE/entry_price"
        return _default

    hv, hv_src = _hv_with_source(row)
    is_put  = str(row.get("Option_Type", "put") or "").lower().startswith("p")
    is_long = str(row.get("Position_Side", "long") or "long").lower() == "long"
    t       = dte / TRADING_DAYS

    if rng is None:
        rng = np.random.default_rng(SEED)

    s_T = _gbm_terminal(spot, hv, t, n_paths, rng)

    # ── Compute P&L at expiry per contract ─────────────────────────────────
    if is_put:
        intrinsic = np.maximum(strike - s_T, 0.0)
    else:
        intrinsic = np.maximum(s_T - strike, 0.0)

    if is_long:
        # Long option: P&L = intrinsic at expiry - entry price paid
        pnl = (intrinsic - entry_px) * 100
        max_loss_threshold = -entry_px * 100 * 0.90   # 90% of premium = "max loss"
    else:
        # Short option (income): P&L = premium received - assignment loss
        pnl = (entry_px - intrinsic) * 100
        # Max loss for short put = (strike - entry_px) * 100
        max_loss_threshold = -(strike - entry_px) * 100 * 0.85

    p_recovery = float(np.mean(pnl >= 0))
    p_max_loss  = float(np.mean(pnl <= max_loss_threshold))
    p10 = float(np.percentile(pnl, 10))
    p50 = float(np.percentile(pnl, 50))
    ev  = float(np.mean(pnl))

    # ── Verdict ────────────────────────────────────────────────────────────
    # Compare hold EV vs locking current P&L (current_px - entry_px) * 100
    current_pnl = (current_px - entry_px) * 100 if is_long else (entry_px - current_px) * 100

    # Strategy type determines which thresholds are meaningful.
    #
    # SHORT options (BUY_WRITE, CSP, CC, iron condor legs):
    #   P(max_loss) ≥ 40% is a genuine emergency — these strategies have
    #   capped upside and defined (but real) downside.  The 40% threshold
    #   correctly flags deteriorating income positions.
    #
    # LONG options (LONG_CALL, LONG_PUT, LEAPS):
    #   P(max_loss) is structurally 40–60% for any OTM long option — that
    #   is not an emergency, it is the mathematical consequence of buying
    #   optionality.  Using p_max_loss ≥ 40% here would fire EXIT_NOW on
    #   nearly every long option regardless of merit.
    #   For long options, the signal that matters is P(recovery): if fewer
    #   than 35% of paths recover to breakeven AND EV is also negative,
    #   only then is EXIT warranted.  This avoids noise-escalating positions
    #   with positive EV or reasonable recovery odds.
    #
    # Hull Ch.17: "For a long option, the probability of expiring worthless
    #   is the complement of delta (roughly), not a risk signal by itself."
    # McMillan Ch.4: "Never exit a long option solely because it is losing
    #   time value — exit when the directional thesis breaks down."
    _strategy_str = str(row.get("Strategy", row.get("Entry_Structure", "")) or "").upper()
    _long_strat_keywords = {"LONG_CALL", "LONG_PUT", "LEAPS_CALL", "LEAPS_PUT",
                            "BUY_CALL", "BUY_PUT", "LEAP_CALL", "LEAP_PUT", "LEAPS"}
    _is_long_option_strat = any(kw in _strategy_str for kw in _long_strat_keywords) or is_long

    if _is_long_option_strat:
        # Long option verdict: recovery-first, not max-loss-first
        if p_recovery >= 0.55 and p_max_loss < 0.25 and ev > current_pnl:
            verdict = "HOLD_JUSTIFIED"
            v_reason = f"P(recovery)={p_recovery:.0%} > 55%, EV=${ev:+.0f} > locked ${current_pnl:+.0f}"
        elif p_recovery < 0.35 and ev < 0:
            # Both recovery AND EV are negative — thesis has broken down
            verdict = "EXIT_NOW"
            v_reason = (
                f"P(recovery)={p_recovery:.0%} < 35% AND EV=${ev:+.0f} < 0 — "
                f"thesis breakdown: both probability and EV argue against holding"
            )
        elif ev < current_pnl * 0.5 and current_pnl > 0:
            # Holding worth less than 50% of locked gain — opportunity cost
            verdict = "EXIT_NOW"
            v_reason = (
                f"Holding EV (${ev:+.0f}) < 50% of locked gain (${current_pnl:+.0f}) — "
                f"locking gain dominates"
            )
        else:
            verdict = "MONITOR"
            v_reason = f"P(recovery)={p_recovery:.0%}, EV=${ev:+.0f} — long option, monitor thesis integrity"
    else:
        # Short option / income strategy: max-loss threshold is meaningful
        if p_recovery >= 0.55 and p_max_loss < 0.15 and ev > current_pnl:
            verdict = "HOLD_JUSTIFIED"
            v_reason = f"P(recovery)={p_recovery:.0%} > 55%, EV=${ev:+.0f} > locked ${current_pnl:+.0f}"
        elif p_max_loss >= 0.40 or (ev < current_pnl * 0.5 and current_pnl > 0):
            verdict = "EXIT_NOW"
            v_reason = (
                f"P(max_loss)={p_max_loss:.0%} ≥ 40% OR holding EV (${ev:+.0f}) "
                f"< 50% of locked gain (${current_pnl:+.0f})"
            )
        else:
            verdict = "MONITOR"
            v_reason = f"P(recovery)={p_recovery:.0%}, EV=${ev:+.0f} — marginal; monitor next session"

    note = (
        f"MC hold simulation ({n_paths:,}p, DTE={int(dte)}, σ={hv*100:.0f}%[{hv_src}]): "
        f"P(recover)={p_recovery:.0%} | P(max_loss)={p_max_loss:.0%} | "
        f"P10=${p10:+,.0f} | P50=${p50:+,.0f} | EV=${ev:+,.0f} → {verdict}"
    )

    return {
        "MC_Hold_P_Recovery": round(p_recovery, 4),
        "MC_Hold_P_MaxLoss":  round(p_max_loss, 4),
        "MC_Hold_P10":        round(p10, 2),
        "MC_Hold_P50":        round(p50, 2),
        "MC_Hold_EV":         round(ev, 2),
        "MC_Hold_Note":       note,
        "MC_Hold_Verdict":    verdict,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 3. Assignment Risk (income positions only)
# ─────────────────────────────────────────────────────────────────────────────

def mc_assignment_risk(
    row: pd.Series,
    n_paths: int = N_PATHS,
    rng: Optional[np.random.Generator] = None,
) -> dict:
    """
    Compute probability that a short strike is ITM at expiry.

    For income strategies (CSP, CC, BUY_WRITE, iron condor legs):
      - P(assign_expiry)  — fraction of GBM paths where short strike is ITM
      - P(touch_before)   — fraction where underlying touches the strike
                            at ANY point during DTE (not just at expiry);
                            more conservative than P(assign_expiry)
      - MC_Assign_Urgency — 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL'

    Thresholds (Cohen Ch.5 + Natenberg Ch.12):
      < 15%  → LOW      — well cushioned, theta working as planned
      15-30% → MEDIUM   — monitor; consider tightening or rolling
      30-50% → HIGH     — roll now to reduce assignment risk
      > 50%  → CRITICAL — deeply ITM; execute roll or exit immediately

    Returns
    -------
    dict with keys:
      MC_Assign_P_Expiry  – P(ITM at expiry) [0-1]
      MC_Assign_P_Touch   – P(touches strike any time during DTE) [0-1]
      MC_Assign_Urgency   – 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL'
      MC_Assign_Note      – human-readable summary
    """
    _default = {
        "MC_Assign_P_Expiry":  np.nan,
        "MC_Assign_P_Touch":   np.nan,
        "MC_Assign_Urgency":   "SKIP",
        "MC_Assign_Note":      "MC_SKIP",
    }

    spot = _spot(row)
    if spot is None:
        _default["MC_Assign_Note"] = "MC_SKIP: no spot price"
        return _default

    strike, dte, _opt_type = _short_leg(row)
    if strike <= 0 or dte < 1:
        _default["MC_Assign_Note"] = "MC_SKIP: missing strike/DTE"
        return _default

    hv, hv_src = _hv_with_source(row)
    is_put = _opt_type == "p"
    t      = dte / TRADING_DAYS

    if rng is None:
        rng = np.random.default_rng(SEED)

    # ── P(ITM at expiry) — terminal distribution ───────────────────────────
    s_T = _gbm_terminal(spot, hv, t, n_paths, rng)
    if is_put:
        itm_expiry = s_T < strike
    else:
        itm_expiry = s_T > strike
    p_expiry = float(np.mean(itm_expiry))

    # ── P(touch before expiry) — barrier approximation ────────────────────
    # For a one-sided barrier, P(touch) ≈ 2 × Φ(-|log(K/S)| / (σ√T))
    # when drift=0 (risk-neutral GBM). This is the reflection principle result.
    # Natenberg: barrier probability gives a more conservative assignment estimate.
    import math
    if hv > 0 and t > 0:
        log_moneyness = abs(math.log(strike / spot))
        sigma_t       = hv * math.sqrt(t)
        # Standard normal CDF via math.erfc
        z             = log_moneyness / sigma_t
        p_touch = 2.0 * (0.5 * math.erfc(z / math.sqrt(2)))
        p_touch = min(p_touch, 1.0)
    else:
        p_touch = p_expiry

    # ── Urgency classification ─────────────────────────────────────────────
    # Use P(expiry) as primary signal (Cohen Ch.5 delta-proxy method)
    if p_expiry < 0.15:
        urgency = "LOW"
        u_note  = "well cushioned — theta working as planned"
    elif p_expiry < 0.30:
        urgency = "MEDIUM"
        u_note  = "monitor; consider rolling further OTM or reducing size"
    elif p_expiry < 0.50:
        urgency = "HIGH"
        u_note  = "elevated assignment risk — roll NOW to reduce exposure"
    else:
        urgency = "CRITICAL"
        u_note  = "deeply ITM — execute roll or exit immediately"

    otm_pct = (spot - strike) / strike * 100 if not is_put else (strike - spot) / strike * 100
    note = (
        f"MC assignment risk ({n_paths:,}p, DTE={int(dte)}, σ={hv*100:.0f}%[{hv_src}]): "
        f"P(assign@expiry)={p_expiry:.0%} | P(touch_anytime)={p_touch:.0%} | "
        f"OTM={otm_pct:+.1f}% → {urgency}: {u_note}"
    )

    return {
        "MC_Assign_P_Expiry": round(p_expiry, 4),
        "MC_Assign_P_Touch":  round(p_touch, 4),
        "MC_Assign_Urgency":  urgency,
        "MC_Assign_Note":     note,
    }


# ─────────────────────────────────────────────────────────────────────────────
# 4. Triple-Barrier (Lopez de Prado)
# ─────────────────────────────────────────────────────────────────────────────

def mc_triple_barrier(
    row: pd.Series,
    profit_target_pct: float = 0.50,
    stop_loss_pct: float = -0.50,
    n_paths: int = N_PATHS,
    rng: Optional[np.random.Generator] = None,
) -> dict:
    """
    Triple-barrier simulation: which barrier is hit first?

    Lopez de Prado (0.683): "Label each path by the first barrier crossed —
    upper (profit-take), lower (stop-loss), or vertical (time-expiry).
    The probability distribution across barriers reveals the regime
    character of the position."

    Barriers (applied to option P&L, not stock price):
      - Upper barrier: option P&L reaches +profit_target_pct from entry
      - Lower barrier: option P&L reaches stop_loss_pct from entry
      - Vertical barrier: DTE expires without touching either

    Strategy-aware verdicts:
      - Income (BW/CSP/CC): P(time) high = FAVORABLE (theta working)
      - Directional (LONG_CALL/LONG_PUT): P(profit) high = FAVORABLE

    Parameters
    ----------
    row               : management position row
    profit_target_pct : upper barrier as fraction of entry price (default 0.50 = +50%)
    stop_loss_pct     : lower barrier as fraction of entry price (default -0.50 = -50%)
    n_paths, rng      : MC parameters

    Returns
    -------
    dict with keys:
      MC_TB_P_Profit  – P(profit barrier hit first) [0-1]
      MC_TB_P_Stop    – P(stop-loss barrier hit first) [0-1]
      MC_TB_P_Time    – P(time barrier hit first) [0-1]
      MC_TB_Verdict   – 'FAVORABLE' | 'UNFAVORABLE' | 'NEUTRAL' | 'SKIP'
      MC_TB_Note      – human-readable summary
    """
    _default = {
        "MC_TB_P_Profit": np.nan,
        "MC_TB_P_Stop":   np.nan,
        "MC_TB_P_Time":   np.nan,
        "MC_TB_Verdict":  "SKIP",
        "MC_TB_Note":     "MC_SKIP",
    }

    spot = _spot(row)
    if spot is None:
        _default["MC_TB_Note"] = "MC_SKIP: no spot price"
        return _default

    strike   = float(row.get("Strike", 0) or 0)
    dte      = int(float(row.get("DTE", 0) or 0))
    entry_px = float(row.get("Premium_Entry", row.get("Last", 0)) or 0)

    if strike <= 0 or dte < 2 or entry_px <= 0:
        _default["MC_TB_Note"] = "MC_SKIP: missing strike/DTE/entry_price"
        return _default

    hv, hv_src = _hv_with_source(row)
    is_put  = str(row.get("Option_Type", "call") or "").lower().startswith("p")
    is_long = str(row.get("Position_Side", "long") or "long").lower() == "long"

    if rng is None:
        rng = np.random.default_rng(SEED)

    # ── Generate daily paths ─────────────────────────────────────────────
    paths = _gbm_daily_paths(spot, hv, dte, n_paths, rng)
    # paths shape: (n_paths, dte+1)

    # ── Approximate option value along each path ─────────────────────────
    # Intrinsic component
    if is_put:
        intrinsic = np.maximum(strike - paths, 0.0)
        intrinsic_entry = max(strike - spot, 0.0)
    else:
        intrinsic = np.maximum(paths - strike, 0.0)
        intrinsic_entry = max(spot - strike, 0.0)

    # Extrinsic component with sqrt-time decay (matches theta's sqrt(T) behavior)
    # At day 0: option_value ≈ entry_px. At expiry: option_value = intrinsic only.
    extrinsic_entry = max(entry_px - intrinsic_entry, 0.0)
    days = np.arange(dte + 1)                              # (dte+1,)
    remaining_frac = np.maximum(0, (dte - days) / dte)
    time_decay = np.sqrt(remaining_frac)                    # (dte+1,)
    # option_value(path, day) = intrinsic(path, day) + extrinsic × decay(day)
    option_values = intrinsic + extrinsic_entry * time_decay[np.newaxis, :]

    # Option P&L per contract along each path (relative to entry)
    if is_long:
        pnl_paths = (option_values - entry_px) * 100
    else:
        pnl_paths = (entry_px - option_values) * 100

    # ── Barrier levels (absolute P&L per contract) ───────────────────────
    profit_barrier = abs(entry_px * 100 * profit_target_pct)
    stop_barrier   = entry_px * 100 * stop_loss_pct  # negative

    # ── Vectorized first-barrier detection ───────────────────────────────
    # For each path, find the first day where P&L crosses profit or stop
    hit_profit = pnl_paths >= profit_barrier      # shape (n_paths, dte+1)
    hit_stop   = pnl_paths <= stop_barrier         # shape (n_paths, dte+1)

    # argmax on boolean finds first True; if no True, returns 0
    # We need to distinguish "hit at day 0" from "never hit"
    first_profit_day = np.argmax(hit_profit, axis=1)  # (n_paths,)
    first_stop_day   = np.argmax(hit_stop, axis=1)    # (n_paths,)

    # Mask: did it ever hit?
    ever_profit = hit_profit.any(axis=1)  # (n_paths,)
    ever_stop   = hit_stop.any(axis=1)    # (n_paths,)

    # Set non-hits to dte+1 (beyond vertical barrier)
    first_profit_day = np.where(ever_profit, first_profit_day, dte + 1)
    first_stop_day   = np.where(ever_stop, first_stop_day, dte + 1)

    # Which barrier is first?
    profit_first = (first_profit_day < first_stop_day) & (first_profit_day <= dte)
    stop_first   = (first_stop_day < first_profit_day) & (first_stop_day <= dte)
    # If both hit same day, whichever is checked first (profit wins ties — conservative)
    same_day     = (first_profit_day == first_stop_day) & ever_profit & ever_stop
    profit_first = profit_first | same_day
    time_first   = ~profit_first & ~stop_first

    p_profit = float(np.mean(profit_first))
    p_stop   = float(np.mean(stop_first))
    p_time   = float(np.mean(time_first))

    # ── Strategy-aware verdict ───────────────────────────────────────────
    # Lopez de Prado: the stop-loss barrier is the adverse outcome.
    # P(stop) is the primary danger signal for all strategies.
    #
    # Income: P(profit) = theta working (option decays to profit barrier).
    #   P(profit) + P(time) are BOTH favorable outcomes — premium collected.
    # Directional: P(profit) = thesis playing out. P(time) = theta erosion = bad.
    strategy_str = str(row.get("Strategy", row.get("Entry_Structure", "")) or "").upper()
    _long_kw = {"LONG_CALL", "LONG_PUT", "LEAPS_CALL", "LEAPS_PUT",
                "BUY_CALL", "BUY_PUT", "LEAP_CALL", "LEAP_PUT", "LEAPS"}
    is_directional = any(kw in strategy_str for kw in _long_kw)
    is_income = _is_income_strategy(strategy_str)

    if is_directional:
        # Directional: P(profit) high = thesis intact, P(time) high = theta eroding
        if p_profit >= 0.40 and p_stop < 0.30:
            verdict = "FAVORABLE"
        elif p_stop >= 0.35 or (p_time >= 0.50 and p_profit < 0.25):
            verdict = "UNFAVORABLE"
        else:
            verdict = "NEUTRAL"
    elif is_income:
        # Income: P(profit) + P(time) are both favorable (premium retained)
        p_favorable = p_profit + p_time
        if p_favorable >= 0.60 and p_stop < 0.25:
            verdict = "FAVORABLE"
        elif p_stop >= 0.35:
            verdict = "UNFAVORABLE"
        else:
            verdict = "NEUTRAL"
    else:
        if p_profit >= 0.40 and p_stop < 0.30:
            verdict = "FAVORABLE"
        elif p_stop >= 0.35:
            verdict = "UNFAVORABLE"
        else:
            verdict = "NEUTRAL"

    strat_label = "directional" if is_directional else ("income" if is_income else "unknown")
    note = (
        f"MC triple-barrier ({n_paths:,}p, DTE={dte}, σ={hv*100:.0f}%[{hv_src}], "
        f"targets=+{profit_target_pct:.0%}/-{abs(stop_loss_pct):.0%}): "
        f"P(profit)={p_profit:.0%} | P(stop)={p_stop:.0%} | P(time)={p_time:.0%} "
        f"→ {verdict} [{strat_label}]"
    )

    return {
        "MC_TB_P_Profit": round(p_profit, 4),
        "MC_TB_P_Stop":   round(p_stop, 4),
        "MC_TB_P_Time":   round(p_time, 4),
        "MC_TB_Verdict":  verdict,
        "MC_TB_Note":     note,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Batch runner — apply all four to a management DataFrame
# ─────────────────────────────────────────────────────────────────────────────

# Income strategy name tokens — determines which functions run
_INCOME_TOKENS = {
    "CSP", "CASH_SECURED_PUT", "COVERED_CALL", "BUY_WRITE", "CC",
    "IRON_CONDOR", "IRON_BUTTERFLY", "PUT_CREDIT_SPREAD", "CALL_CREDIT_SPREAD",
    "BULL_PUT_SPREAD", "BEAR_CALL_SPREAD",
}

def _is_income_strategy(strategy_name: str) -> bool:
    s = strategy_name.upper().replace(" ", "_").replace("-", "_")
    return any(t in s for t in _INCOME_TOKENS)


def run_management_mc(
    df: pd.DataFrame,
    wait_days: int = 3,
    n_paths: int = N_PATHS,
    seed: Optional[int] = SEED,
) -> pd.DataFrame:
    """
    Apply all four management MC functions to every applicable row in `df`.

    Rules:
      - mc_roll_wait_cost  → rows where Action in ('ROLL', 'STAGE_AND_RECHECK')
                             OR Execution_Readiness == 'STAGE_AND_RECHECK'
      - mc_exit_vs_hold    → rows where Action in ('HOLD', 'HOLD_FOR_REVERSION')
      - mc_assignment_risk → rows where strategy is income (CSP/CC/BW/IC/etc.)
      - mc_triple_barrier  → all rows with Strike, DTE, and entry price

    All columns are added/updated in-place. Existing columns preserved.
    Non-blocking: exceptions per-row → MC_*_Note = MC_ERROR.

    Parameters
    ----------
    df        : positions_latest.csv DataFrame (after generate_recommendations)
    wait_days : days to simulate in roll wait-cost calculation
    n_paths   : GBM paths per row
    seed      : RNG seed

    Returns
    -------
    df with MC_Wait_*, MC_Hold_*, MC_Assign_*, MC_TB_* columns added
    """
    if df.empty:
        return df

    rng = np.random.default_rng(seed)

    # Pre-allocate all MC columns with correct dtypes
    _float_cols = [
        "MC_Wait_P_Improve", "MC_Wait_P_Assign", "MC_Wait_Credit_Delta",
        "MC_Hold_P_Recovery", "MC_Hold_P_MaxLoss", "MC_Hold_P10",
        "MC_Hold_P50", "MC_Hold_EV",
        "MC_Assign_P_Expiry", "MC_Assign_P_Touch",
        "MC_TB_P_Profit", "MC_TB_P_Stop", "MC_TB_P_Time",
    ]
    _str_cols = [
        "MC_Wait_Note", "MC_Wait_Verdict",
        "MC_Hold_Note", "MC_Hold_Verdict",
        "MC_Assign_Note", "MC_Assign_Urgency",
        "MC_TB_Note", "MC_TB_Verdict",
    ]
    _int_cols = ["MC_Wait_Days"]

    for col in _float_cols:
        if col not in df.columns:
            df[col] = pd.array([np.nan] * len(df), dtype="Float64")
    for col in _str_cols:
        if col not in df.columns:
            df[col] = pd.array([""] * len(df), dtype="string")
    for col in _int_cols:
        if col not in df.columns:
            df[col] = pd.array([pd.NA] * len(df), dtype="Int64")

    roll_count = hold_count = assign_count = tb_count = 0

    for idx, row in df.iterrows():
        action   = str(row.get("Action", "") or "").upper()
        er       = str(row.get("Execution_Readiness", "") or "").upper()
        strategy = str(row.get("Strategy", row.get("Strategy_Name", "")) or "")

        # ── Roll wait-cost: ROLL actions + STAGE_AND_RECHECK ─────────────
        if action in ("ROLL", "ROLL_WAIT") or er == "STAGE_AND_RECHECK":
            try:
                # Parse Roll_Candidate_1 for the roll context
                _rc1 = None
                _rc1_raw = row.get("Roll_Candidate_1")
                if _rc1_raw and str(_rc1_raw) not in ("", "nan", "None"):
                    try:
                        _rc1 = json.loads(str(_rc1_raw))
                    except Exception:
                        pass

                mc_w = mc_roll_wait_cost(
                    row=row,
                    roll_candidate=_rc1,
                    wait_days=wait_days,
                    n_paths=n_paths,
                    rng=rng,
                )
                for col, val in mc_w.items():
                    if col in df.columns:
                        df.at[idx, col] = val
                roll_count += 1
            except Exception as e:
                df.at[idx, "MC_Wait_Note"] = f"MC_ERROR: {e}"

        # ── Exit vs Hold: HOLD actions ────────────────────────────────────
        if action in ("HOLD", "HOLD_FOR_REVERSION", "REVALIDATE"):
            try:
                mc_h = mc_exit_vs_hold(row=row, n_paths=n_paths, rng=rng)
                for col, val in mc_h.items():
                    if col in df.columns:
                        df.at[idx, col] = val
                hold_count += 1
            except Exception as e:
                df.at[idx, "MC_Hold_Note"] = f"MC_ERROR: {e}"

        # ── Assignment risk: all income positions ─────────────────────────
        if _is_income_strategy(strategy):
            try:
                mc_a = mc_assignment_risk(row=row, n_paths=n_paths, rng=rng)
                for col, val in mc_a.items():
                    if col in df.columns:
                        df.at[idx, col] = val
                assign_count += 1
            except Exception as e:
                df.at[idx, "MC_Assign_Note"] = f"MC_ERROR: {e}"

        # ── Triple-barrier: all option positions with strike/DTE/entry ───
        _tb_strike = float(row.get("Strike", 0) or 0)
        _tb_dte    = float(row.get("DTE", 0) or 0)
        _tb_entry  = float(row.get("Premium_Entry", row.get("Last", 0)) or 0)
        if _tb_strike > 0 and _tb_dte >= 2 and _tb_entry > 0:
            try:
                mc_tb = mc_triple_barrier(row=row, n_paths=n_paths, rng=rng)
                for col, val in mc_tb.items():
                    if col in df.columns:
                        df.at[idx, col] = val
                tb_count += 1
            except Exception as e:
                df.at[idx, "MC_TB_Note"] = f"MC_ERROR: {e}"

    logger.info(
        f"🎲 Management MC: roll_wait={roll_count} | hold={hold_count} | "
        f"assignment={assign_count} | triple_barrier={tb_count} rows processed"
    )
    return df
