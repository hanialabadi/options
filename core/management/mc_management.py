"""
Monte Carlo — Management System
================================

Five distinct MC functions for active position management.
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

5. mc_roll_ev_comparison(row, roll_candidate, ...)
   → "Is rolling to this candidate better than holding or closing?"
   Used by: ROLL actions with candidates — compares EV(hold current)
   vs EV(roll to candidate) vs EV(close now). Strategy-aware profiles
   (income models stock+call, directional models option-only, PMCC
   models LEAP+short). Non-blocking annotation.
   Passarelli Ch.6: "Roll decision is EV comparison, not gut feeling."

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
N_PATHS_ROLL  = 10_000   # higher precision for roll EV comparisons (tight HOLD vs ROLL ranking)
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
    from core.shared.finance_utils import normalize_iv
    for col in ("HV_20D", "hv_20d", "HV_30D", "hv_30", "IV_30D", "iv_30d", "IV_Now"):
        val = row.get(col)
        if val is not None and pd.notna(val):
            v = normalize_iv(float(val))
            if v is not None and 0.01 <= v <= 5.0:
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
    strategy = str(row.get("Strategy", row.get("Entry_Structure", "")) or "").upper().replace("-", "_").replace(" ", "_")
    if raw_type.startswith("p") or any(t in strategy for t in ("PUT", "CSP", "CASH_SECURED")):
        option_type = "p"
    else:
        option_type = "c"   # BUY_WRITE / COVERED_CALL default

    return strike, dte, option_type


def _gbm_terminal(spot: float, hv: float, t_years: float,
                  n: int, rng: np.random.Generator,
                  drift: float = 0.0) -> np.ndarray:
    """GBM terminal prices. Delegates to shared path generator."""
    from core.shared.mc.paths import gbm_terminal
    dte = max(int(round(t_years * TRADING_DAYS)), 1)
    return gbm_terminal(spot, hv, dte, n, rng, drift=drift)


def _gbm_daily_paths(spot: float, hv: float, n_days: int,
                     n_paths: int, rng: np.random.Generator,
                     drift: float = 0.0,
                     iv_schedule: Optional[np.ndarray] = None) -> np.ndarray:
    """GBM daily price paths. Delegates to shared path generator."""
    from core.shared.mc.paths import gbm_daily_paths
    return gbm_daily_paths(spot, hv, n_days, n_paths, rng,
                           iv_schedule=iv_schedule, drift=drift)


def _daily_carry(row: pd.Series) -> float:
    """
    Pre-computed daily margin carry cost ($/day) for this position.

    Uses Daily_Margin_Cost from MarginCarryCalculator — consistent with
    doctrine gates and dashboard display.

    Returns 0.0 for:
      - Retirement/cash-funded positions (Is_Retirement=True)
      - Positions with no borrowing (Daily_Margin_Cost=0 or absent)
      - Option-only positions (carry lives on the stock leg)

    Guardrail: never recalculate inline — use the pre-computed column only.
    """
    # Retirement check — redundant with Daily_Margin_Cost=0, but explicit
    is_ret = row.get("Is_Retirement")
    if is_ret is True or str(is_ret).upper() in ("TRUE", "1"):
        return 0.0
    val = row.get("Daily_Margin_Cost")
    if val is None or (isinstance(val, float) and val != val):  # NaN check
        return 0.0
    cost = float(val)
    return cost if cost > 0 else 0.0


# ─────────────────────────────────────────────────────────────────────────────
# 1. Roll Wait-Cost
# ─────────────────────────────────────────────────────────────────────────────

def mc_roll_wait_cost(
    row: pd.Series,
    roll_candidate: Optional[dict] = None,
    wait_days: int = 3,
    n_paths: int = N_PATHS,
    rng: Optional[np.random.Generator] = None,
    *,
    prebuilt_wait_terminal: Optional[np.ndarray] = None,
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
    strategy = str(row.get("Strategy", row.get("Entry_Structure", "")) or "").upper().replace("-", "_").replace(" ", "_")
    _long_strategies = {"LONG_CALL", "LONG_PUT", "LEAPS_CALL", "LEAPS_PUT",
                        "BUY_CALL", "BUY_PUT", "LEAP_CALL", "LEAP_PUT"}
    is_long_option = any(s in strategy for s in _long_strategies)

    if rng is None:
        rng = np.random.default_rng(SEED)

    # ── Simulate wait-period paths ─────────────────────────────────────────
    if prebuilt_wait_terminal is not None:
        s_wait = prebuilt_wait_terminal
    else:
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
        elif p_assign_wait < 0.10:
            # Low breach risk + low improvement = position is stable.
            # No urgency to act — theta decays naturally. Don't escalate
            # urgency for a deeply OTM short option that isn't threatened.
            verdict = "HOLD"
            verdict_reason = f"low breach risk ({p_assign_wait:.0%}), credit stable — hold for theta decay"
        else:
            # Moderate breach risk (10-25%) + low improvement = mild urgency
            verdict = "ACT_NOW"
            verdict_reason = f"moderate breach risk ({p_assign_wait:.0%}) and credit improvement unlikely ({p_improve:.0%})"
        note = (
            f"MC roll wait-cost ({n_paths:,}p, wait={wait_days}d, σ={hv*100:.0f}%[{hv_src}]): "
            f"P(thesis)={1-p_assign_wait:.0%} | P(credit+20%)={p_improve:.0%} | "
            f"median option value Δ={median_credit_delta:+.0f}/contract → {verdict}"
        )

    # ── Margin carry drain during wait period ────────────────────────────
    carry_per_day = _daily_carry(row)
    carry_drag_wait = round(carry_per_day * wait_days, 2)

    # Adjust median credit delta by carry drag — waiting costs real money
    median_credit_delta_after_carry = round(median_credit_delta - carry_drag_wait, 2)

    # Carry can tip a WAIT → ACT_NOW if carry drain exceeds expected credit gain
    if verdict == "WAIT" and carry_drag_wait > 0 and median_credit_delta_after_carry < 0:
        verdict = "ACT_NOW"
        note += f" | Carry override: ${carry_drag_wait:.0f} wait-carry > credit gain"

    if carry_drag_wait > 0:
        note += f" | Carry: ${carry_drag_wait:.0f}/{wait_days}d"

    return {
        "MC_Wait_P_Improve":    round(p_improve, 4),
        "MC_Wait_P_Assign":     round(p_assign_wait, 4),
        "MC_Wait_Credit_Delta": round(median_credit_delta, 2),
        "MC_Wait_Credit_Delta_After_Carry": median_credit_delta_after_carry,
        "MC_Wait_Carry_Drag":   carry_drag_wait,
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
    *,
    prebuilt_terminal: Optional[np.ndarray] = None,
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

    # Regime-adjusted drift
    from core.shared.mc.inputs import resolve_regime_drift
    _drift, _drift_src = resolve_regime_drift(row)

    if rng is None:
        rng = np.random.default_rng(SEED)

    if prebuilt_terminal is not None:
        s_T = prebuilt_terminal
    else:
        s_T = _gbm_terminal(spot, hv, t, n_paths, rng, drift=_drift)

    # ── Compute P&L at expiry per contract ─────────────────────────────────
    # Income strategies (BW/CC/CSP) model the COMBINED position, not just
    # the option leg. A BUY_WRITE's hold decision depends on stock+call P&L,
    # not call P&L alone.
    # (McMillan Ch.3: "The buy-write profit mechanism is assignment + premium
    # collection — model the whole position, not the short call in isolation.")
    _strategy_str = str(row.get("Strategy", row.get("Entry_Structure", "")) or "").upper().replace("-", "_").replace(" ", "_")
    _is_bw_cc = any(kw in _strategy_str for kw in ("BUY_WRITE", "COVERED_CALL", "CC"))
    _is_csp = any(kw in _strategy_str for kw in ("CSP", "CASH_SECURED_PUT"))

    if _is_bw_cc:
        # Combined position: long stock + short call
        # Stock P&L: (S_T - effective_cost) per share
        # Call P&L: premium received - max(S_T - strike, 0) per share
        # When S_T < strike: call expires worthless, keep premium, stock P&L = S_T - cost
        # When S_T > strike: assigned at strike, total = (strike - cost) + premium
        _eff_cost = float(row.get("Net_Cost_Basis_Per_Share", 0) or 0)
        if _eff_cost <= 0:
            _broker_basis = abs(float(row.get("Basis", 0) or 0))
            _qty_abs = abs(float(row.get("Quantity", 1) or 1))
            _eff_cost = (_broker_basis / _qty_abs) if _qty_abs > 0 and _broker_basis > 0 else spot
        _call_intrinsic = np.maximum(s_T - strike, 0.0)
        # Per share: stock gain/loss + call premium - call assignment cost
        _pnl_per_share = (s_T - _eff_cost) + entry_px - _call_intrinsic
        # When S_T > strike: = (strike - eff_cost) + entry_px (capped upside, certain)
        # When S_T < strike: = (S_T - eff_cost) + entry_px (stock loss offset by full premium)
        n_shares = abs(float(row.get("Quantity", 100) or 100))
        pnl = _pnl_per_share * n_shares
        # Max loss: stock drops to hard stop, call expires worthless
        _hard_stop_pct = 0.20  # 20% below cost basis
        max_loss_threshold = -(_eff_cost * _hard_stop_pct) * n_shares

    elif _is_csp:
        # Short put: premium received - assignment cost if ITM
        _put_intrinsic = np.maximum(strike - s_T, 0.0)
        pnl = (entry_px - _put_intrinsic) * 100
        max_loss_threshold = -(strike - entry_px) * 100 * 0.85

    elif is_put:
        intrinsic = np.maximum(strike - s_T, 0.0)
        if is_long:
            pnl = (intrinsic - entry_px) * 100
            max_loss_threshold = -entry_px * 100 * 0.90
        else:
            pnl = (entry_px - intrinsic) * 100
            max_loss_threshold = -(strike - entry_px) * 100 * 0.85
    else:
        intrinsic = np.maximum(s_T - strike, 0.0)
        if is_long:
            pnl = (intrinsic - entry_px) * 100
            max_loss_threshold = -entry_px * 100 * 0.90
        else:
            pnl = (entry_px - intrinsic) * 100
            max_loss_threshold = -(strike - entry_px) * 100 * 0.85

    # ── Margin carry drain over hold period ──────────────────────────────
    carry_per_day = _daily_carry(row)
    carry_drag_total = carry_per_day * dte  # total $ carry over remaining DTE

    # EV before carry (raw simulation)
    ev_before_carry = float(np.mean(pnl))

    # Subtract carry from P&L — each path incurs the same fixed daily cost
    pnl_after_carry = pnl - carry_drag_total

    p_recovery = float(np.mean(pnl_after_carry >= 0))
    p_max_loss  = float(np.mean(pnl_after_carry <= max_loss_threshold))
    p10 = float(np.percentile(pnl_after_carry, 10))
    p50 = float(np.percentile(pnl_after_carry, 50))
    ev  = float(np.mean(pnl_after_carry))

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
    _strategy_str = str(row.get("Strategy", row.get("Entry_Structure", "")) or "").upper().replace("-", "_").replace(" ", "_")
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

    _carry_tag = f" | Carry: ${carry_drag_total:,.0f}/{int(dte)}d" if carry_drag_total > 0 else ""
    _drift_tag = f" | μ={_drift:+.1%}[{_drift_src}]" if _drift != 0.0 else ""
    note = (
        f"MC hold simulation ({n_paths:,}p, DTE={int(dte)}, σ={hv*100:.0f}%[{hv_src}]): "
        f"P(recover)={p_recovery:.0%} | P(max_loss)={p_max_loss:.0%} | "
        f"P10=${p10:+,.0f} | P50=${p50:+,.0f} | EV=${ev:+,.0f}"
        f"{_carry_tag}{_drift_tag} → {verdict}"
    )

    return {
        "MC_Hold_P_Recovery": round(p_recovery, 4),
        "MC_Hold_P_MaxLoss":  round(p_max_loss, 4),
        "MC_Hold_P10":        round(p10, 2),
        "MC_Hold_P50":        round(p50, 2),
        "MC_Hold_EV":         round(ev, 2),
        "MC_Hold_EV_Before_Carry": round(ev_before_carry, 2),
        "MC_Hold_Carry_Drag": round(carry_drag_total, 2),
        "MC_Hold_Drift":      round(_drift, 4),
        "MC_Hold_Drift_Source": _drift_src,
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
    *,
    prebuilt_terminal: Optional[np.ndarray] = None,
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

    # Regime-adjusted drift
    from core.shared.mc.inputs import resolve_regime_drift
    _drift, _ = resolve_regime_drift(row)

    if rng is None:
        rng = np.random.default_rng(SEED)

    # ── P(ITM at expiry) — terminal distribution ───────────────────────────
    if prebuilt_terminal is not None:
        s_T = prebuilt_terminal
    else:
        s_T = _gbm_terminal(spot, hv, t, n_paths, rng, drift=_drift)
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
    *,
    prebuilt_daily: Optional[np.ndarray] = None,
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
    if prebuilt_daily is not None:
        paths = prebuilt_daily
    else:
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
    strategy_str = str(row.get("Strategy", row.get("Entry_Structure", "")) or "").upper().replace("-", "_").replace(" ", "_")
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


# ── 5. Stock Recovery Comparison ─────────────────────────────────────────────
#
# "Should I cover idle shares with CCs or keep them naked for upside?"
#
# Compares two scenarios via GBM simulation:
#   A. Current coverage: covered shares earn premium + cap, idle shares naked
#   B. Full coverage: all shares earn premium + cap
#
# The tradeoff: idle shares preserve unlimited upside if the stock rallies
# above the CC strike; covered shares sacrifice upside for certain premium.
# MC quantifies this tradeoff as EV difference across the path distribution.
#
# Called from run_all.py ticker recovery reconciler (NOT from run_management_mc
# — this operates at ticker level, not per-option-row level).
#
# References:
#   McMillan Ch.3: recovery = stock appreciation + premium collection
#   Jabbour Ch.4:  recovery is a portfolio-level commitment
#   Natenberg Ch.12: distribution-based position analysis

def mc_stock_recovery_comparison(
    spot: float,
    cost_basis: float,
    cc_strike: float,
    cc_premium_ps_mo: float,
    covered_shares: int,
    total_shares: int,
    hv: float,
    horizon_months: int = 6,
    n_paths: int = 2_000,
    rng: np.random.Generator | None = None,
) -> dict:
    """Compare EV of current coverage vs full coverage for stock recovery.

    Parameters
    ----------
    spot : current stock price
    cost_basis : net cost basis per share (after premium credits)
    cc_strike : CC strike price (for upside cap on covered shares)
    cc_premium_ps_mo : estimated CC premium per share per month
    covered_shares : currently covered share count
    total_shares : total share count (covered + idle)
    hv : annualised HV as decimal (for stock path simulation)
    horizon_months : simulation horizon (default 6)
    n_paths : number of GBM paths
    rng : numpy RNG (optional, for reproducibility)

    Returns
    -------
    dict with MC_Recovery_* columns for df_final.
    """
    _default = {
        "MC_Recovery_EV_Current":        np.nan,
        "MC_Recovery_EV_Full":           np.nan,
        "MC_Recovery_P_Recover_Current": np.nan,
        "MC_Recovery_P_Recover_Full":    np.nan,
        "MC_Recovery_EV_Delta":          np.nan,
        "MC_Recovery_Recommend_Cover":   False,
        "MC_Recovery_Verdict":           "SKIP",
        "MC_Recovery_Note":              "MC_SKIP",
    }

    idle_shares = total_shares - covered_shares
    gap = cost_basis - spot

    if spot <= 0 or cost_basis <= 0 or gap <= 0:
        _default["MC_Recovery_Note"] = "MC_SKIP: no recovery gap"
        return _default
    if idle_shares < 100:
        _default["MC_Recovery_Note"] = "MC_SKIP: no meaningful idle shares"
        return _default
    if cc_strike <= 0 or cc_premium_ps_mo <= 0:
        _default["MC_Recovery_Note"] = "MC_SKIP: missing CC strike or premium"
        return _default
    if hv <= 0:
        hv = HV_FALLBACK

    if rng is None:
        rng = np.random.default_rng(SEED)

    # Simulate terminal stock price at horizon
    t_years = horizon_months / 12.0
    s_T = _gbm_terminal(spot, hv, t_years, n_paths, rng)

    # Cumulative premium over horizon (per share)
    cum_premium = cc_premium_ps_mo * horizon_months

    # ── Scenario A: current coverage ──────────────────────────────────────
    # Covered shares: stock capped at strike + premium earned
    # Idle shares: stock uncapped, no premium
    covered_value = np.minimum(s_T, cc_strike) + cum_premium
    idle_value = s_T
    pnl_a = (
        (covered_value - cost_basis) * covered_shares
        + (idle_value - cost_basis) * idle_shares
    )

    # ── Scenario B: full coverage ─────────────────────────────────────────
    # All shares: stock capped at strike + premium earned
    full_value = np.minimum(s_T, cc_strike) + cum_premium
    pnl_b = (full_value - cost_basis) * total_shares

    # ── Metrics ───────────────────────────────────────────────────────────
    ev_a = float(np.mean(pnl_a))
    ev_b = float(np.mean(pnl_b))
    p_recover_a = float(np.mean(pnl_a >= 0))
    p_recover_b = float(np.mean(pnl_b >= 0))
    ev_delta = ev_b - ev_a

    # Verdict: prefer full coverage when EV is higher
    if ev_delta > 0:
        verdict = "COVER_IDLE"
    elif ev_delta < -total_shares * 0.10:  # idle upside materially better
        verdict = "KEEP_IDLE"
    else:
        verdict = "NEUTRAL"

    return {
        "MC_Recovery_EV_Current":        round(ev_a, 2),
        "MC_Recovery_EV_Full":           round(ev_b, 2),
        "MC_Recovery_P_Recover_Current": round(p_recover_a, 4),
        "MC_Recovery_P_Recover_Full":    round(p_recover_b, 4),
        "MC_Recovery_EV_Delta":          round(ev_delta, 2),
        "MC_Recovery_Recommend_Cover":   verdict == "COVER_IDLE",
        "MC_Recovery_Verdict":           verdict,
        "MC_Recovery_Note": (
            f"MC Recovery ({horizon_months}mo, {n_paths} paths): "
            f"EV current=${ev_a:,.0f}, EV full=${ev_b:,.0f} "
            f"(delta=${ev_delta:+,.0f}), "
            f"P(recover) {p_recover_a:.0%} vs {p_recover_b:.0%}"
        ),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 6. Roll EV Comparison (strategy-aware, modular)
# ─────────────────────────────────────────────────────────────────────────────

# Signal profiles: strategy family → MC configuration.
# Each profile defines how to model the position P&L and what roll
# improvement looks like. This is the "modular, not blanket" design.
#
# References:
#   McMillan Ch.3: income roll = combined stock+call P&L
#   Passarelli Ch.6: roll decision is EV comparison, not gut feeling
#   Natenberg Ch.5: vol edge changes with new strike/DTE — model it

_ROLL_MC_PROFILES = {
    "INCOME_SHORT_CALL": {
        # BUY_WRITE, COVERED_CALL: model combined stock+short_call P&L
        "model": "stock_plus_short_call",
        "roll_ev_floor": -50.0,       # $50 debit roll can be net-positive
        "hold_worth_threshold": 0.55, # P(recovery) for hold to be competitive
    },
    "CSP": {
        # Cash-secured put: model short put P&L (no stock leg)
        "model": "short_put",
        "roll_ev_floor": -30.0,
        "hold_worth_threshold": 0.50,
    },
    "LONG_CALL": {
        # Long call: model call P&L alone (debit position)
        "model": "long_option",
        "roll_ev_floor": -100.0,      # long options can justify larger debit rolls
        "hold_worth_threshold": 0.40,
    },
    "LONG_PUT": {
        "model": "long_option",
        "roll_ev_floor": -100.0,
        "hold_worth_threshold": 0.40,
    },
    "PMCC": {
        # PMCC: model LEAP call + short call combined
        "model": "pmcc",
        "roll_ev_floor": -75.0,
        "hold_worth_threshold": 0.45,
    },
    "MULTI_LEG": {
        # Iron condor, spreads: model net credit position
        "model": "short_put",         # simplified — net credit model
        "roll_ev_floor": -30.0,
        "hold_worth_threshold": 0.50,
    },
    "DEFAULT": {
        "model": "long_option",
        "roll_ev_floor": -50.0,
        "hold_worth_threshold": 0.45,
    },
}


def _roll_mc_profile(strategy: str) -> dict:
    """Map strategy name to MC signal profile."""
    s = strategy.upper().replace("-", "_").replace(" ", "_")
    # PMCC check BEFORE CC/BW — "PMCC" contains "CC" substring
    if "PMCC" in s:
        return _ROLL_MC_PROFILES["PMCC"]
    if any(kw in s for kw in ("BUY_WRITE", "COVERED_CALL", "CC")):
        return _ROLL_MC_PROFILES["INCOME_SHORT_CALL"]
    if any(kw in s for kw in ("CSP", "CASH_SECURED_PUT", "SHORT_PUT")):
        return _ROLL_MC_PROFILES["CSP"]
    if any(kw in s for kw in ("LONG_CALL", "BUY_CALL", "LEAPS_CALL", "LEAP_CALL", "LEAPS")):
        return _ROLL_MC_PROFILES["LONG_CALL"]
    if any(kw in s for kw in ("LONG_PUT", "BUY_PUT", "LEAPS_PUT", "LEAP_PUT")):
        return _ROLL_MC_PROFILES["LONG_PUT"]
    if any(kw in s for kw in ("IRON_CONDOR", "IRON_BUTTERFLY", "SPREAD")):
        return _ROLL_MC_PROFILES["MULTI_LEG"]
    return _ROLL_MC_PROFILES["DEFAULT"]


def mc_roll_ev_comparison(
    row: pd.Series,
    roll_candidate: Optional[dict] = None,
    n_paths: int = N_PATHS,
    rng: Optional[np.random.Generator] = None,
    *,
    prebuilt_hold_terminal: Optional[np.ndarray] = None,
    prebuilt_roll_terminal: Optional[np.ndarray] = None,
) -> dict:
    """
    Compare EV(hold current position) vs EV(roll to candidate) vs EV(close now).

    Strategy-aware: uses per-family signal profiles to model the right P&L.
    Income strategies model combined stock+option; directional strategies
    model option-only P&L. PMCC models LEAP+short call.

    Answers the question: "Is rolling to this candidate better than holding
    or just closing the position?"

    Highest value for:
    - Debit rolls on damaged positions (churn guard fires)
    - EMERGENCY mode where closing might be better than rolling
    - Any roll where net cost is significant

    Parameters
    ----------
    row            : management position row
    roll_candidate : dict from Roll_Candidate_1 JSON (must have
                     strike, mid, dte keys; optional: delta, iv)
    n_paths, rng   : MC parameters

    Returns
    -------
    dict with keys:
      MC_Roll_EV_Hold    – EV if we hold current position to expiry ($)
      MC_Roll_EV_Roll    – EV if we roll to candidate (net of roll cost) ($)
      MC_Roll_EV_Close   – EV if we close now (realized P&L) ($)
      MC_Roll_EV_Delta   – EV_roll - EV_hold ($, positive = roll is better)
      MC_Roll_P_Hold_Win – P(hold outperforms roll) [0-1]
      MC_Roll_Verdict    – 'ROLL_BETTER' | 'HOLD_BETTER' | 'CLOSE_BETTER' | 'MARGINAL' | 'SKIP'
      MC_Roll_Note       – human-readable summary
      MC_Roll_Profile    – strategy profile used (for audit)
    """
    _default = {
        "MC_Roll_EV_Hold":    np.nan,
        "MC_Roll_EV_Roll":    np.nan,
        "MC_Roll_EV_Close":   np.nan,
        "MC_Roll_EV_Delta":   np.nan,
        "MC_Roll_P_Hold_Win": np.nan,
        "MC_Roll_Verdict":    "SKIP",
        "MC_Roll_Note":       "MC_SKIP",
        "MC_Roll_Profile":    "",
    }

    # ── Guard clauses ────────────────────────────────────────────────────────
    if roll_candidate is None or not isinstance(roll_candidate, dict):
        _default["MC_Roll_Note"] = "MC_SKIP: no roll candidate"
        return _default

    spot = _spot(row)
    if spot is None:
        _default["MC_Roll_Note"] = "MC_SKIP: no spot price"
        return _default

    # Current position
    cur_strike = float(row.get("Strike", row.get("Short_Call_Strike", 0)) or 0)
    cur_dte    = float(row.get("DTE", row.get("Short_Call_DTE", 0)) or 0)
    cur_entry  = float(row.get("Premium_Entry", row.get("Last", 0)) or 0)
    cur_mid    = float(row.get("Last", 0) or 0)

    # Roll candidate
    new_strike = float(roll_candidate.get("strike", 0) or 0)
    new_dte    = float(roll_candidate.get("dte", roll_candidate.get("actual_dte", 0)) or 0)
    new_mid    = float(roll_candidate.get("mid", roll_candidate.get("mid_price", 0)) or 0)

    if cur_strike <= 0 or new_strike <= 0 or new_dte < 1:
        _default["MC_Roll_Note"] = "MC_SKIP: missing strike/DTE data"
        return _default

    # ── Strategy profile ─────────────────────────────────────────────────────
    strategy = str(row.get("Strategy", row.get("Entry_Structure", "")) or "")
    profile = _roll_mc_profile(strategy)
    model_type = profile["model"]

    hv, hv_src = _hv_with_source(row)
    is_put = str(row.get("Option_Type", "") or "").lower().startswith("p")

    # Regime-adjusted drift
    from core.shared.mc.inputs import resolve_regime_drift
    _drift, _drift_src = resolve_regime_drift(row)

    if rng is None:
        rng = np.random.default_rng(SEED)

    # ── Net roll cost (with slippage) ────────────────────────────────────────
    # Close current: sell at mid × 0.98 (slippage)
    # Open new: buy at mid × 1.02 (slippage)
    # For short options: close = buy-to-close (pay mid×1.02), open = sell-to-open (receive mid×0.98)
    _is_short = model_type in ("stock_plus_short_call", "short_put", "pmcc")
    if _is_short:
        close_cost = cur_mid * 1.02 if cur_mid > 0 else 0.0   # buy-to-close
        open_credit = new_mid * 0.98                            # sell-to-open
        net_roll_cost = close_cost - open_credit                # positive = debit
    else:
        close_proceeds = cur_mid * 0.98 if cur_mid > 0 else 0.0  # sell-to-close
        open_cost = new_mid * 1.02                                 # buy-to-open
        net_roll_cost = open_cost - close_proceeds                 # positive = debit

    net_roll_cost_per_contract = net_roll_cost * 100

    # ── Simulate terminal prices ─────────────────────────────────────────────
    t_hold = max(cur_dte, 1) / TRADING_DAYS
    t_roll = new_dte / TRADING_DAYS

    if prebuilt_hold_terminal is not None:
        s_T_hold = prebuilt_hold_terminal
    else:
        s_T_hold = _gbm_terminal(spot, hv, t_hold, n_paths, rng, drift=_drift)
    if prebuilt_roll_terminal is not None:
        s_T_roll = prebuilt_roll_terminal
    else:
        s_T_roll = _gbm_terminal(spot, hv, t_roll, n_paths, rng, drift=_drift)

    # ── Compute P&L per strategy profile ─────────────────────────────────────
    if model_type == "stock_plus_short_call":
        # Income: combined stock + short call position
        _eff_cost = float(row.get("Net_Cost_Basis_Per_Share", 0) or 0)
        if _eff_cost <= 0:
            _broker_basis = abs(float(row.get("Basis", 0) or 0))
            _qty = abs(float(row.get("Quantity", 1) or 1))
            _eff_cost = (_broker_basis / _qty) if _qty > 0 and _broker_basis > 0 else spot
        n_shares = abs(float(row.get("Quantity", 100) or 100))

        # HOLD: stock + current short call to expiry
        cur_call_intrinsic = np.maximum(s_T_hold - cur_strike, 0.0)
        pnl_hold = ((s_T_hold - _eff_cost) + cur_entry - cur_call_intrinsic) * n_shares

        # ROLL: stock + new short call to new expiry
        new_call_intrinsic = np.maximum(s_T_roll - new_strike, 0.0)
        pnl_roll_gross = ((s_T_roll - _eff_cost) + new_mid - new_call_intrinsic) * n_shares
        pnl_roll = pnl_roll_gross - net_roll_cost_per_contract * (n_shares / 100)

        # CLOSE: close option, keep stock (stock P&L at hold horizon)
        pnl_close_option = (cur_entry - cur_mid) * n_shares if cur_mid > 0 else 0.0
        pnl_close = (s_T_hold - _eff_cost) * n_shares + pnl_close_option

    elif model_type == "short_put":
        # Short put: premium - assignment cost
        # HOLD
        cur_put_intrinsic = np.maximum(cur_strike - s_T_hold, 0.0)
        pnl_hold = (cur_entry - cur_put_intrinsic) * 100

        # ROLL
        new_put_intrinsic = np.maximum(new_strike - s_T_roll, 0.0)
        pnl_roll = (new_mid - new_put_intrinsic) * 100 - net_roll_cost_per_contract

        # CLOSE
        pnl_close = np.full(n_paths, (cur_entry - cur_mid) * 100 if cur_mid > 0 else 0.0)

    elif model_type == "pmcc":
        # PMCC: long LEAP call + short near-term call
        # Simplified: model short call roll, LEAP is held constant
        leap_strike = float(row.get("LEAP_Call_Strike", row.get("PMCC_LEAP_Strike", 0)) or 0)
        leap_mid = float(row.get("PMCC_LEAP_Mid", 0) or 0)

        # HOLD: short call to expiry
        if is_put:
            cur_intrinsic = np.maximum(cur_strike - s_T_hold, 0.0)
        else:
            cur_intrinsic = np.maximum(s_T_hold - cur_strike, 0.0)
        pnl_hold = (cur_entry - cur_intrinsic) * 100

        # ROLL: new short call to new expiry
        if is_put:
            new_intrinsic = np.maximum(new_strike - s_T_roll, 0.0)
        else:
            new_intrinsic = np.maximum(s_T_roll - new_strike, 0.0)
        pnl_roll = (new_mid - new_intrinsic) * 100 - net_roll_cost_per_contract

        # CLOSE: close short call, keep LEAP
        pnl_close = np.full(n_paths, (cur_entry - cur_mid) * 100 if cur_mid > 0 else 0.0)

    else:
        # Long option: option P&L only
        if is_put:
            cur_intrinsic = np.maximum(cur_strike - s_T_hold, 0.0)
            new_intrinsic = np.maximum(new_strike - s_T_roll, 0.0)
        else:
            cur_intrinsic = np.maximum(s_T_hold - cur_strike, 0.0)
            new_intrinsic = np.maximum(s_T_roll - new_strike, 0.0)

        # HOLD: current option to expiry
        pnl_hold = (cur_intrinsic - cur_entry) * 100

        # ROLL: new option to expiry minus roll cost
        pnl_roll = (new_intrinsic - new_mid) * 100 - net_roll_cost_per_contract

        # CLOSE: sell current option now
        pnl_close = np.full(n_paths, (cur_mid - cur_entry) * 100 if cur_mid > 0 else 0.0)

    # ── Margin carry drain ─────────────────────────────────────────────────
    carry_per_day = _daily_carry(row)
    carry_hold = carry_per_day * max(cur_dte, 1)   # carry cost if holding to expiry
    carry_roll = carry_per_day * new_dte            # carry cost over new position lifetime

    # EV before carry (raw simulation)
    ev_hold_before  = float(np.mean(pnl_hold))
    ev_roll_before  = float(np.mean(pnl_roll))

    # Subtract carry from hold and roll paths — each path incurs fixed daily cost
    pnl_hold = pnl_hold - carry_hold
    pnl_roll = pnl_roll - carry_roll
    # Close = immediate exit, no further carry

    # ── Aggregate metrics ────────────────────────────────────────────────────
    ev_hold  = float(np.mean(pnl_hold))
    ev_roll  = float(np.mean(pnl_roll))
    ev_close = float(np.mean(pnl_close)) if isinstance(pnl_close, np.ndarray) else float(pnl_close)
    ev_delta = ev_roll - ev_hold

    p_hold_wins = float(np.mean(pnl_hold > pnl_roll))

    # ── Verdict ──────────────────────────────────────────────────────────────
    # Materiality threshold: $25/contract — avoid noise verdicts
    _MATERIAL = 25.0

    if ev_roll > ev_hold + _MATERIAL and ev_roll > ev_close + _MATERIAL:
        verdict = "ROLL_BETTER"
    elif ev_close > ev_hold + _MATERIAL and ev_close > ev_roll + _MATERIAL:
        verdict = "CLOSE_BETTER"
    elif ev_hold > ev_roll + _MATERIAL:
        verdict = "HOLD_BETTER"
    elif abs(ev_delta) <= _MATERIAL:
        verdict = "MARGINAL"
    else:
        verdict = "MARGINAL"

    # Profile label for audit
    _profile_label = model_type.upper()

    _carry_tag = ""
    if carry_hold > 0 or carry_roll > 0:
        _carry_tag = f" | Carry: hold=${carry_hold:,.0f}, roll=${carry_roll:,.0f}"

    note = (
        f"MC roll EV ({n_paths:,}p, σ={hv*100:.0f}%[{hv_src}], "
        f"profile={_profile_label}): "
        f"EV_hold=${ev_hold:+,.0f} | EV_roll=${ev_roll:+,.0f} "
        f"(net_cost=${net_roll_cost_per_contract:+,.0f}) | "
        f"EV_close=${ev_close:+,.0f} | "
        f"Δ=${ev_delta:+,.0f} | P(hold>roll)={p_hold_wins:.0%}"
        f"{_carry_tag}"
        f"{f' | μ={_drift:+.1%}' if _drift != 0.0 else ''}"
        f" → {verdict}"
    )

    return {
        "MC_Roll_EV_Hold":    round(ev_hold, 2),
        "MC_Roll_EV_Roll":    round(ev_roll, 2),
        "MC_Roll_EV_Close":   round(ev_close, 2),
        "MC_Roll_EV_Delta":   round(ev_delta, 2),
        "MC_Roll_P_Hold_Win": round(p_hold_wins, 4),
        "MC_Roll_EV_Hold_Before_Carry": round(ev_hold_before, 2),
        "MC_Roll_EV_Roll_Before_Carry": round(ev_roll_before, 2),
        "MC_Roll_Carry_Hold": round(carry_hold, 2),
        "MC_Roll_Carry_Roll": round(carry_roll, 2),
        "MC_Roll_Drift":      round(_drift, 4),
        "MC_Roll_Verdict":    verdict,
        "MC_Roll_Note":       note,
        "MC_Roll_Profile":    _profile_label,
    }


# ── MC Roll Rerank: 5→3 by EV ────────────────────────────────────────────────

def mc_roll_rerank(
    row: pd.Series,
    candidates: list[dict],
    n_paths: int = N_PATHS_ROLL,
    rng: Optional[np.random.Generator] = None,
    *,
    prebuilt_hold_terminal: Optional[np.ndarray] = None,
) -> dict:
    """
    Evaluate up to 5 heuristic-shortlisted roll candidates on the same GBM
    paths and return the MC-ranked top 3.

    Step 2 of the 5→MC→3 pipeline:
      Step 1: roll_candidate_engine shortlists 5 (heuristic score + expiry diversity)
      Step 2: THIS FUNCTION — MC rerank to top 3 by carry-adjusted EV
      Step 3: mc_roll_final_comparison — HOLD/EXIT/ROLL#1/#2/#3

    Each candidate is evaluated on shared hold-horizon paths (for the current
    position) and its own roll-horizon paths (for its specific DTE).

    Parameters
    ----------
    row            : management position row
    candidates     : list of up to 5 candidate dicts (from Roll_Candidate_1..5)
    n_paths        : MC paths (default N_PATHS_ROLL=10K for ranking precision)
    rng            : numpy random generator
    prebuilt_hold_terminal : optional shared hold terminal paths

    Returns
    -------
    dict with:
      mc_ranked     : list of up to 3 candidate dicts, sorted by EV descending
      mc_ranked_evs : list of (ev, p_profit, cvar) tuples for each ranked candidate
      rerank_note   : human-readable audit trail
      all_evs       : list of (candidate_index, ev) for all 5 (audit)
    """
    _default = {
        "mc_ranked": candidates[:3],
        "mc_ranked_evs": [],
        "rerank_note": "RERANK_SKIP",
        "all_evs": [],
    }

    if not candidates:
        return _default

    # ── Resolve shared inputs ────────────────────────────────────────────
    spot = _spot(row)
    if spot is None or spot <= 0:
        _default["rerank_note"] = "RERANK_SKIP: no spot"
        return _default

    hv, hv_src = _hv_with_source(row)
    cur_dte = float(row.get("DTE", 0) or 0)
    if cur_dte < 1:
        _default["rerank_note"] = "RERANK_SKIP: DTE < 1"
        return _default

    strategy = str(row.get("Strategy", row.get("Entry_Structure", "")) or "")
    profile = _roll_mc_profile(strategy)
    model_type = profile["model"]
    is_put = str(row.get("Option_Type", "") or "").lower().startswith("p")
    _is_short = model_type in ("stock_plus_short_call", "short_put", "pmcc")

    cur_strike = float(row.get("Strike", row.get("Short_Call_Strike", 0)) or 0)
    cur_entry = float(row.get("Premium_Entry", row.get("Last", 0)) or 0)
    cur_mid = float(row.get("Last", 0) or 0)

    if cur_strike <= 0:
        _default["rerank_note"] = "RERANK_SKIP: no strike"
        return _default

    # Regime-adjusted drift
    from core.shared.mc.inputs import resolve_regime_drift
    _drift, _drift_src = resolve_regime_drift(row)

    if rng is None:
        rng = np.random.default_rng(SEED)

    carry_per_day = _daily_carry(row)

    # ── Income-specific fields ───────────────────────────────────────────
    _eff_cost = spot  # default
    n_shares = 100.0
    if model_type == "stock_plus_short_call":
        _eff_cost = float(row.get("Net_Cost_Basis_Per_Share", 0) or 0)
        if _eff_cost <= 0:
            _broker_basis = abs(float(row.get("Basis", 0) or 0))
            _qty = abs(float(row.get("Quantity", 1) or 1))
            _eff_cost = (_broker_basis / _qty) if _qty > 0 and _broker_basis > 0 else spot
        n_shares = abs(float(row.get("Quantity", 100) or 100))

    # ── Build hold terminal paths ────────────────────────────────────────
    t_hold = max(cur_dte, 1) / TRADING_DAYS
    if prebuilt_hold_terminal is not None:
        s_T_hold = prebuilt_hold_terminal
    else:
        s_T_hold = _gbm_terminal(spot, hv, t_hold, n_paths, rng, drift=_drift)

    # ── Evaluate each candidate ──────────────────────────────────────────
    scored = []
    for ci, cand in enumerate(candidates):
        new_strike = float(cand.get("strike", 0) or 0)
        new_dte = float(cand.get("dte", cand.get("actual_dte", 0)) or 0)
        new_mid = float(cand.get("mid", cand.get("mid_price", 0)) or 0)

        if new_strike <= 0 or new_dte < 1:
            scored.append((ci, float("-inf"), 0.0, float("-inf")))
            continue

        # Roll-horizon terminal paths (each candidate may have different DTE)
        t_roll = new_dte / TRADING_DAYS
        _cand_rng = np.random.default_rng(SEED + 10 + ci)
        s_T_roll = _gbm_terminal(spot, hv, t_roll, n_paths, _cand_rng, drift=_drift)

        # Net roll cost with slippage
        if _is_short:
            close_cost = cur_mid * 1.02 if cur_mid > 0 else 0.0
            open_credit = new_mid * 0.98
            net_roll_cost = (close_cost - open_credit) * 100
        else:
            close_proceeds = cur_mid * 0.98 if cur_mid > 0 else 0.0
            open_cost = new_mid * 1.02
            net_roll_cost = (open_cost - close_proceeds) * 100

        # P&L model dispatch (same as mc_roll_ev_comparison)
        if model_type == "stock_plus_short_call":
            new_call_intrinsic = np.maximum(s_T_roll - new_strike, 0.0)
            pnl = ((s_T_roll - _eff_cost) + new_mid - new_call_intrinsic) * n_shares
            pnl = pnl - net_roll_cost * (n_shares / 100)
        elif model_type == "short_put":
            new_put_intrinsic = np.maximum(new_strike - s_T_roll, 0.0)
            pnl = (new_mid - new_put_intrinsic) * 100 - net_roll_cost
        elif model_type == "pmcc":
            if is_put:
                new_intrinsic = np.maximum(new_strike - s_T_roll, 0.0)
            else:
                new_intrinsic = np.maximum(s_T_roll - new_strike, 0.0)
            pnl = (new_mid - new_intrinsic) * 100 - net_roll_cost
        else:
            # Long option
            if is_put:
                new_intrinsic = np.maximum(new_strike - s_T_roll, 0.0)
            else:
                new_intrinsic = np.maximum(s_T_roll - new_strike, 0.0)
            pnl = (new_intrinsic - new_mid) * 100 - net_roll_cost

        # Carry adjustment
        carry_roll = carry_per_day * new_dte
        pnl = pnl - carry_roll

        ev = float(np.mean(pnl))
        p_profit = float(np.mean(pnl > 0))
        tail_mask = pnl <= np.percentile(pnl, 10)
        cvar = float(np.mean(pnl[tail_mask])) if tail_mask.any() else float(np.percentile(pnl, 10))

        scored.append((ci, ev, p_profit, cvar))

    # ── Rank by EV descending, keep top 3 ────────────────────────────────
    scored.sort(key=lambda x: x[1], reverse=True)
    top3_indices = [s[0] for s in scored[:3]]
    mc_ranked = [candidates[i] for i in top3_indices]
    mc_ranked_evs = [(s[1], s[2], s[3]) for s in scored[:3]]

    # Build audit note
    _parts = []
    for rank, (ci, ev, pp, cvar) in enumerate(scored[:3], 1):
        cand = candidates[ci]
        _parts.append(
            f"#{rank}: K={cand.get('strike',0)} "
            f"DTE={cand.get('dte', cand.get('actual_dte','?'))} "
            f"EV=${ev:+,.0f} P(profit)={pp:.0%}"
        )
    # Note if MC reranked differently from heuristic
    heuristic_order = list(range(len(candidates)))
    mc_order = [s[0] for s in scored]
    reranked = mc_order[:3] != heuristic_order[:3]
    _rerank_tag = " [MC RERANKED]" if reranked else " [order unchanged]"

    rerank_note = f"MC rerank ({n_paths:,}p): {' | '.join(_parts)}{_rerank_tag}"

    return {
        "mc_ranked": mc_ranked,
        "mc_ranked_evs": mc_ranked_evs,
        "rerank_note": rerank_note,
        "all_evs": [(s[0], s[1]) for s in scored],
    }


# ── MC Roll Final Comparison ──────────────────────────────────────────────────
# Unified evaluation: HOLD / EXIT / ROLL-to-each-candidate / splits
# All options evaluated on the same GBM paths. Best EV wins.

def mc_roll_final_comparison(
    row: pd.Series,
    mc_ranked_candidates: list[dict],
    mc_ranked_evs: list[tuple],
    n_paths: int = N_PATHS_ROLL,
    rng: Optional[np.random.Generator] = None,
    *,
    prebuilt_hold_terminal: Optional[np.ndarray] = None,
) -> dict:
    """
    Evaluate ALL action options on the same GBM paths. Best EV wins.

    Step 3 of the 5→MC→3 pipeline. Options evaluated:

      HOLD          – keep all N contracts at current position to expiry
      EXIT          – close everything now
      ROLL_ALL_1    – roll all N → candidate #1
      ROLL_ALL_2    – roll all N → candidate #2
      ROLL_ALL_3    – roll all N → candidate #3
      SPLIT_1_2     – N/2 → #1, N/2 → #2         (qty ≥ 4 only)
      SPLIT_1_3     – N/2 → #1, N/2 → #3         (qty ≥ 4 only)
      SPLIT_2_3     – N/2 → #2, N/2 → #3         (qty ≥ 4 only)
      PARTIAL_CLOSE – close N/3, roll 2N/3 → #1   (qty ≥ 4 only)

    Returns backward-compatible MC_Roll_* columns plus:
      MC_Roll_EV_Cand1/2/3 : per-candidate EV
      MC_Roll_Best_Action  : winning action type
      MC_Roll_All_Actions  : JSON list of all scored actions

    Passarelli Ch.6: "With size, you can split the roll."
    Natenberg Ch.19: "MC comparison requires common random numbers."
    """
    _default = {
        "MC_Roll_EV_Hold":    np.nan,
        "MC_Roll_EV_Roll":    np.nan,
        "MC_Roll_EV_Close":   np.nan,
        "MC_Roll_EV_Delta":   np.nan,
        "MC_Roll_P_Hold_Win": np.nan,
        "MC_Roll_EV_Hold_Before_Carry": np.nan,
        "MC_Roll_EV_Roll_Before_Carry": np.nan,
        "MC_Roll_Carry_Hold": np.nan,
        "MC_Roll_Carry_Roll": np.nan,
        "MC_Roll_EV_Cand1":  np.nan,
        "MC_Roll_EV_Cand2":  np.nan,
        "MC_Roll_EV_Cand3":  np.nan,
        "MC_Roll_Verdict":    "SKIP",
        "MC_Roll_Note":       "MC_SKIP",
        "MC_Roll_Profile":    "",
        "MC_Roll_Best_Action": "",
        "MC_Roll_All_Actions": "[]",
        "MC_Split_Best":      "",
        "MC_Split_Note":      "",
        "MC_Split_Verdict":   "",
        "MC_Split_Paths":     "[]",
    }

    if not mc_ranked_candidates:
        _default["MC_Roll_Note"] = "MC_SKIP: no candidates"
        return _default

    # ── Resolve shared inputs ────────────────────────────────────────────
    spot = _spot(row)
    if spot is None or spot <= 0:
        _default["MC_Roll_Note"] = "MC_SKIP: no spot price"
        return _default

    hv, hv_src = _hv_with_source(row)
    cur_dte = float(row.get("DTE", 0) or 0)
    if cur_dte < 1:
        _default["MC_Roll_Note"] = "MC_SKIP: DTE < 1"
        return _default

    strategy = str(row.get("Strategy", row.get("Entry_Structure", "")) or "")
    profile = _roll_mc_profile(strategy)
    model_type = profile["model"]
    _is_short = model_type in ("stock_plus_short_call", "short_put", "pmcc")
    is_put = str(row.get("Option_Type", "") or "").lower().startswith("p")
    _profile_label = model_type.upper()

    cur_strike = float(row.get("Strike", row.get("Short_Call_Strike", 0)) or 0)
    cur_entry = float(row.get("Premium_Entry", row.get("Last", 0)) or 0)
    cur_mid = float(row.get("Last", 0) or 0)
    qty = abs(int(float(row.get("Quantity", 1) or 1)))

    if cur_strike <= 0:
        _default["MC_Roll_Note"] = "MC_SKIP: no strike"
        return _default

    carry_per_day = _daily_carry(row)

    # Regime-adjusted drift
    from core.shared.mc.inputs import resolve_regime_drift
    _drift, _drift_src = resolve_regime_drift(row)

    if rng is None:
        rng = np.random.default_rng(SEED)

    # Income-specific fields
    _eff_cost = spot
    n_shares = 100.0
    if model_type == "stock_plus_short_call":
        _eff_cost = float(row.get("Net_Cost_Basis_Per_Share", 0) or 0)
        if _eff_cost <= 0:
            _broker_basis = abs(float(row.get("Basis", 0) or 0))
            _qty_raw = abs(float(row.get("Quantity", 1) or 1))
            _eff_cost = (_broker_basis / _qty_raw) if _qty_raw > 0 and _broker_basis > 0 else spot
        n_shares = abs(float(row.get("Quantity", 100) or 100))

    # ── Parse candidates ─────────────────────────────────────────────────
    parsed = []
    for c in mc_ranked_candidates[:3]:
        c_strike = float(c.get("strike", 0) or 0)
        c_dte = float(c.get("dte", c.get("actual_dte", 0)) or 0)
        c_mid = float(c.get("mid", c.get("mid_price", 0)) or 0)
        c_edge = c.get("primary_edge", "")
        c_expiry = c.get("expiry", "")
        if c_strike > 0 and c_dte >= 1:
            parsed.append({
                "strike": c_strike, "dte": c_dte, "mid": c_mid,
                "edge": c_edge, "expiry": c_expiry,
            })
    if not parsed:
        _default["MC_Roll_Note"] = "MC_SKIP: no valid candidates"
        return _default

    # ── Build GBM terminal prices ────────────────────────────────────────
    # Hold horizon
    t_hold = max(cur_dte, 1) / TRADING_DAYS
    if prebuilt_hold_terminal is not None and len(prebuilt_hold_terminal) == n_paths:
        s_T_hold = prebuilt_hold_terminal
    else:
        s_T_hold = _gbm_terminal(spot, hv, t_hold, n_paths,
                                 np.random.default_rng(SEED), drift=_drift)

    # Per-candidate roll horizons (each candidate may have different DTE)
    s_T_cands = []
    for ci, p in enumerate(parsed):
        t_roll = p["dte"] / TRADING_DAYS
        s_T_cands.append(
            _gbm_terminal(spot, hv, t_roll, n_paths,
                          np.random.default_rng(SEED + 10 + ci), drift=_drift)
        )

    # ── P&L helper ───────────────────────────────────────────────────────
    def _pnl(n_ct: int, action: str, cand_idx: int = 0) -> np.ndarray:
        """
        Per-path P&L in dollars for `n_ct` contracts.

        action: "hold" | "exit" | "roll"
        cand_idx: which parsed candidate to roll to (0, 1, 2)
        """
        mult = n_ct

        if action == "exit":
            if _is_short:
                pnl_per = (cur_entry - cur_mid) * 100
            else:
                pnl_per = (cur_mid - cur_entry) * 100
            return np.full(n_paths, pnl_per * mult)

        if action == "hold":
            s_T = s_T_hold
            strike, premium = cur_strike, cur_entry
            carry = carry_per_day * max(cur_dte, 1) * mult
        else:  # roll
            ci = min(cand_idx, len(parsed) - 1)
            s_T = s_T_cands[ci]
            strike = parsed[ci]["strike"]
            premium = parsed[ci]["mid"]
            carry = carry_per_day * parsed[ci]["dte"] * mult

        # Net roll cost (slippage-adjusted)
        roll_cost = 0.0
        if action == "roll":
            if _is_short:
                close_cost = cur_mid * 1.02 if cur_mid > 0 else 0.0
                open_credit = premium * 0.98
                roll_cost = (close_cost - open_credit) * 100 * mult
            else:
                close_proceeds = cur_mid * 0.98 if cur_mid > 0 else 0.0
                open_cost = premium * 1.02
                roll_cost = (open_cost - close_proceeds) * 100 * mult

        # Strategy-specific P&L model
        if model_type == "stock_plus_short_call":
            call_intrinsic = np.maximum(s_T - strike, 0.0)
            pnl = ((s_T - _eff_cost) + premium - call_intrinsic) * (mult * 100)
            pnl = pnl - roll_cost
        elif model_type == "short_put":
            put_intrinsic = np.maximum(strike - s_T, 0.0)
            pnl = (premium - put_intrinsic) * 100 * mult
            pnl = pnl - roll_cost
        elif model_type == "pmcc":
            if is_put:
                intrinsic = np.maximum(strike - s_T, 0.0)
            else:
                intrinsic = np.maximum(s_T - strike, 0.0)
            if action == "hold":
                pnl = (premium - intrinsic) * 100 * mult
            else:
                pnl = (premium - intrinsic) * 100 * mult - roll_cost
        else:
            # Long option
            if is_put:
                intrinsic = np.maximum(strike - s_T, 0.0)
            else:
                intrinsic = np.maximum(s_T - strike, 0.0)
            if action == "hold":
                pnl = (intrinsic - premium) * 100 * mult
            else:
                pnl = (intrinsic - premium) * 100 * mult - roll_cost

        # Subtract carry (exit has zero carry — immediate)
        pnl = pnl - carry
        return pnl

    # ── Enumerate ALL options ────────────────────────────────────────────
    options = []

    # HOLD — all contracts at current position
    options.append({"type": "HOLD", "label": f"Hold all {qty}",
                    "calc": lambda: _pnl(qty, "hold")})

    # EXIT — close everything
    options.append({"type": "EXIT", "label": f"Exit all {qty}",
                    "calc": lambda: _pnl(qty, "exit")})

    # ROLL_ALL to each candidate
    for ci, p in enumerate(parsed):
        _ci = ci  # capture for lambda
        options.append({
            "type": f"ROLL_ALL_{ci+1}",
            "label": f"Roll all {qty} → #{ci+1} (K={p['strike']}, DTE={int(p['dte'])})",
            "calc": lambda _c=_ci: _pnl(qty, "roll", _c),
        })

    # Split options — only if qty ≥ 4
    if qty >= 4:
        half = qty // 2
        remainder = qty - half
        two_thirds = int(qty * 2 / 3)
        one_third = qty - two_thirds

        # Splits across pairs of candidates
        for ci in range(len(parsed)):
            for cj in range(ci + 1, len(parsed)):
                _ci, _cj = ci, cj
                options.append({
                    "type": f"SPLIT_{ci+1}_{cj+1}",
                    "label": (f"Split {half}→#{ci+1} (K={parsed[ci]['strike']}), "
                              f"{remainder}→#{cj+1} (K={parsed[cj]['strike']})"),
                    "calc": lambda _a=_ci, _b=_cj: (
                        _pnl(half, "roll", _a) + _pnl(remainder, "roll", _b)
                    ),
                })

        # Partial close + roll to best
        if one_third >= 1 and two_thirds >= 2:
            options.append({
                "type": "PARTIAL_CLOSE",
                "label": f"Close {one_third}, roll {two_thirds} → #1",
                "calc": lambda: _pnl(one_third, "exit") + _pnl(two_thirds, "roll", 0),
            })

    # ── Evaluate all options on same paths ───────────────────────────────
    scored = []
    for opt in options:
        total_pnl = opt["calc"]()
        ev = float(np.mean(total_pnl))
        p_profit = float(np.mean(total_pnl > 0))
        p5 = np.percentile(total_pnl, 5)
        cvar_5 = float(np.mean(total_pnl[total_pnl <= p5])) if (total_pnl <= p5).any() else float(p5)
        p10 = float(np.percentile(total_pnl, 10))
        p90 = float(np.percentile(total_pnl, 90))

        scored.append({
            "type": opt["type"],
            "label": opt["label"],
            "ev": round(ev, 2),
            "p_profit": round(p_profit, 4),
            "cvar_5": round(cvar_5, 2),
            "p10": round(p10, 2),
            "p90": round(p90, 2),
        })

    scored.sort(key=lambda x: x["ev"], reverse=True)
    best_action = scored[0]

    # ── Extract backward-compatible MC_Roll_* columns ────────────────────
    ev_hold = next((s["ev"] for s in scored if s["type"] == "HOLD"), np.nan)
    ev_exit = next((s["ev"] for s in scored if s["type"] == "EXIT"), np.nan)
    ev_roll_best = next((s["ev"] for s in scored if s["type"] == "ROLL_ALL_1"), np.nan)

    ev_delta = (ev_roll_best - ev_hold) if not (np.isnan(ev_roll_best) or np.isnan(ev_hold)) else np.nan

    # P(hold > roll) — path-by-path
    pnl_hold_arr = _pnl(qty, "hold")
    pnl_roll_arr = _pnl(qty, "roll", 0)
    p_hold_wins = float(np.mean(pnl_hold_arr > pnl_roll_arr))

    # Carry
    carry_hold = carry_per_day * max(cur_dte, 1) * qty
    carry_roll = carry_per_day * parsed[0]["dte"] * qty if parsed else 0.0

    # Per-candidate EVs
    ev_cands = []
    for ci in range(3):
        key = f"ROLL_ALL_{ci+1}"
        ev_c = next((s["ev"] for s in scored if s["type"] == key), np.nan)
        ev_cands.append(ev_c)

    # ── Verdict ──────────────────────────────────────────────────────────
    _MATERIAL = 25.0

    if best_action["type"] == "HOLD":
        verdict = "HOLD_BETTER"
    elif best_action["type"] == "EXIT":
        verdict = "CLOSE_BETTER"
    elif best_action["type"].startswith("ROLL_ALL"):
        # Check if it's materially better than hold
        if best_action["ev"] > ev_hold + _MATERIAL:
            verdict = "ROLL_BETTER"
        elif abs(best_action["ev"] - ev_hold) <= _MATERIAL:
            verdict = "MARGINAL"
        else:
            verdict = "HOLD_BETTER"
    elif best_action["type"].startswith("SPLIT") or best_action["type"] == "PARTIAL_CLOSE":
        # Split won — check materiality vs best single-candidate roll
        if best_action["ev"] > ev_roll_best + _MATERIAL:
            verdict = "SPLIT_BETTER"
        elif best_action["ev"] > ev_hold + _MATERIAL:
            verdict = "ROLL_BETTER"  # split is best, but not materially better than roll-all
        else:
            verdict = "MARGINAL"
    else:
        verdict = "MARGINAL"

    # ── Build note ───────────────────────────────────────────────────────
    _top3_parts = []
    for i, s in enumerate(scored[:4]):
        _tag = "★" if i == 0 else " "
        _top3_parts.append(
            f"{_tag}{s['type']}: EV=${s['ev']:+,.0f} P={s['p_profit']:.0%}"
        )
    note = (
        f"MC final ({n_paths:,}p, σ={hv*100:.0f}%[{hv_src}], "
        f"profile={_profile_label}, qty={qty}): "
        + " | ".join(_top3_parts)
        + f" → {verdict}"
    )

    # ── Split-specific outputs (backward compatible) ─────────────────────
    split_actions = [s for s in scored if s["type"].startswith("SPLIT") or s["type"] == "PARTIAL_CLOSE"]
    non_split_best = next((s for s in scored if not s["type"].startswith("SPLIT") and s["type"] != "PARTIAL_CLOSE"), scored[0])

    split_verdict = ""
    split_best = ""
    split_note = ""
    if split_actions:
        top_split = split_actions[0] if split_actions else None
        if top_split and top_split["ev"] > non_split_best["ev"] + _MATERIAL * qty:
            split_verdict = "SPLIT_BETTER"
        elif top_split and abs(top_split["ev"] - non_split_best["ev"]) <= _MATERIAL * qty:
            split_verdict = "MARGINAL"
        else:
            split_verdict = "ALL_IN_BETTER"
        split_best = top_split["type"] if top_split else ""
        split_note = (
            f"Best split: {split_best} EV=${top_split['ev']:+,.0f} vs "
            f"best non-split: {non_split_best['type']} EV=${non_split_best['ev']:+,.0f}"
        ) if top_split else ""

    return {
        # Backward-compatible MC_Roll_* columns
        "MC_Roll_EV_Hold":    round(ev_hold, 2),
        "MC_Roll_EV_Roll":    round(ev_roll_best, 2),
        "MC_Roll_EV_Close":   round(ev_exit, 2),
        "MC_Roll_EV_Delta":   round(ev_delta, 2) if not np.isnan(ev_delta) else np.nan,
        "MC_Roll_P_Hold_Win": round(p_hold_wins, 4),
        "MC_Roll_EV_Hold_Before_Carry": round(ev_hold + carry_hold, 2),
        "MC_Roll_EV_Roll_Before_Carry": round(ev_roll_best + carry_roll, 2) if not np.isnan(ev_roll_best) else np.nan,
        "MC_Roll_Carry_Hold": round(carry_hold, 2),
        "MC_Roll_Carry_Roll": round(carry_roll, 2),
        "MC_Roll_Verdict":    verdict,
        "MC_Roll_Note":       note,
        "MC_Roll_Profile":    _profile_label,
        # Per-candidate EVs
        "MC_Roll_EV_Cand1":   round(ev_cands[0], 2) if not np.isnan(ev_cands[0]) else np.nan,
        "MC_Roll_EV_Cand2":   round(ev_cands[1], 2) if len(ev_cands) > 1 and not np.isnan(ev_cands[1]) else np.nan,
        "MC_Roll_EV_Cand3":   round(ev_cands[2], 2) if len(ev_cands) > 2 and not np.isnan(ev_cands[2]) else np.nan,
        # New: unified action ranking
        "MC_Roll_Best_Action": best_action["type"],
        "MC_Roll_All_Actions": json.dumps(scored),
        # Drift audit
        "MC_Roll_Drift":      round(_drift, 4),
        # Split outputs (backward compatible)
        "MC_Split_Best":      split_best,
        "MC_Split_Note":      split_note,
        "MC_Split_Verdict":   split_verdict,
        "MC_Split_Paths":     json.dumps(split_actions),
    }


def run_management_mc(
    df: pd.DataFrame,
    wait_days: int = 3,
    n_paths: int = N_PATHS,
    seed: Optional[int] = SEED,
) -> pd.DataFrame:
    """
    Apply all five management MC functions to every applicable row in `df`.

    Rules:
      - mc_roll_wait_cost      → rows where Action in ('ROLL', 'STAGE_AND_RECHECK')
                                  OR Execution_Readiness == 'STAGE_AND_RECHECK'
      - mc_roll_rerank         → ROLL rows: rerank 5 candidates to top 3 by EV
      - mc_roll_final_comparison→ ROLL rows: HOLD/EXIT/ROLL#1/#2/#3 comparison
      - mc_exit_vs_hold        → rows where Action in ('HOLD', 'HOLD_FOR_REVERSION')
      - mc_assignment_risk     → rows where strategy is income (CSP/CC/BW/IC/etc.)
      - mc_triple_barrier      → all rows with Strike, DTE, and entry price

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
    df with MC_Wait_*, MC_Roll_*, MC_Hold_*, MC_Assign_*, MC_TB_* columns added
    """
    if df.empty:
        return df

    rng = np.random.default_rng(seed)

    # Pre-allocate all MC columns with correct dtypes
    _float_cols = [
        "MC_Wait_P_Improve", "MC_Wait_P_Assign", "MC_Wait_Credit_Delta",
        "MC_Wait_Credit_Delta_After_Carry", "MC_Wait_Carry_Drag",
        "MC_Roll_EV_Hold", "MC_Roll_EV_Roll", "MC_Roll_EV_Close",
        "MC_Roll_EV_Delta", "MC_Roll_P_Hold_Win",
        "MC_Roll_EV_Hold_Before_Carry", "MC_Roll_EV_Roll_Before_Carry",
        "MC_Roll_Carry_Hold", "MC_Roll_Carry_Roll",
        # Per-candidate EV from final comparison
        "MC_Roll_EV_Cand1", "MC_Roll_EV_Cand2", "MC_Roll_EV_Cand3",
        "MC_Hold_P_Recovery", "MC_Hold_P_MaxLoss", "MC_Hold_P10",
        "MC_Hold_P50", "MC_Hold_EV",
        "MC_Hold_EV_Before_Carry", "MC_Hold_Carry_Drag",
        "MC_Hold_Drift",
        "MC_Roll_Drift",
        "MC_Exit_Drift",
        "MC_Assign_P_Expiry", "MC_Assign_P_Touch",
        "MC_TB_P_Profit", "MC_TB_P_Stop", "MC_TB_P_Time",
    ]
    _str_cols = [
        "MC_Wait_Note", "MC_Wait_Verdict",
        "MC_Roll_Note", "MC_Roll_Verdict", "MC_Roll_Profile",
        "MC_Roll_Rerank_Note",  # MC rerank audit trail
        "MC_Roll_Best_Action", "MC_Roll_All_Actions",  # unified action ranking
        "MC_Hold_Note", "MC_Hold_Verdict",
        "MC_Hold_Drift_Source",
        "MC_Assign_Note", "MC_Assign_Urgency",
        "MC_TB_Note", "MC_TB_Verdict",
        "MC_Split_Best", "MC_Split_Note", "MC_Split_Verdict", "MC_Split_Paths",
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

    roll_count = roll_ev_count = hold_count = assign_count = tb_count = 0

    for idx, row in df.iterrows():
        action   = str(row.get("Action", "") or "").upper()
        er       = str(row.get("Execution_Readiness", "") or "").upper()
        strategy = str(row.get("Strategy", row.get("Strategy_Name", "")) or "")

        # ── Phase 3: build shared paths once per row ─────────────────────
        # All MC functions for this row use the same spot/hv/dte, so we
        # generate terminal + daily GBM paths once and pass them through.
        _shared_spot = _spot(row)
        _shared_hv, _ = _hv_with_source(row)
        _shared_dte = float(row.get("DTE", 0) or 0)
        _shared_t = _shared_dte / TRADING_DAYS if _shared_dte > 0 else 0

        # Vol schedule: EWMA→HV blend for daily paths (vol clustering)
        _shared_vol_schedule = None
        if _shared_dte >= 2:
            try:
                from core.shared.mc.vol_blend import resolve_vol_schedule
                _ticker = (row.get("Ticker") or row.get("ticker")
                           or row.get("Underlying_Ticker") or "")
                _shared_vol_schedule, _vol_src = resolve_vol_schedule(
                    str(_ticker) if _ticker else None,
                    _shared_hv,
                    int(_shared_dte),
                )
            except Exception:
                _shared_vol_schedule = None

        _shared_terminal = None
        _shared_daily = None
        if _shared_spot and _shared_spot > 0 and _shared_dte >= 1:
            _shared_rng_t = np.random.default_rng(SEED)
            _shared_terminal = _gbm_terminal(
                _shared_spot, _shared_hv, _shared_t, n_paths, _shared_rng_t
            )
            # Daily paths (different sub-seed) — built lazily below
            # only when needed (HOLD optimal exit or triple-barrier)

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

                # Build wait-horizon terminal paths (short horizon: wait_days)
                # Roll functions use higher path count for tighter EV ranking
                _n_roll = N_PATHS_ROLL
                _wait_terminal = None
                if _shared_spot and _shared_spot > 0 and _shared_dte >= 1:
                    _t_wait = min(wait_days, int(_shared_dte)) / TRADING_DAYS
                    _wait_rng = np.random.default_rng(SEED + 3)
                    _wait_terminal = _gbm_terminal(
                        _shared_spot, _shared_hv, _t_wait, _n_roll, _wait_rng
                    )

                mc_w = mc_roll_wait_cost(
                    row=row,
                    roll_candidate=_rc1,
                    wait_days=wait_days,
                    n_paths=_n_roll,
                    rng=rng,
                    prebuilt_wait_terminal=_wait_terminal,
                )
                for col, val in mc_w.items():
                    if col in df.columns:
                        df.at[idx, col] = val
                roll_count += 1
            except Exception as e:
                df.at[idx, "MC_Wait_Note"] = f"MC_ERROR: {e}"

        # ── Roll MC pipeline: 5→MC→3→final comparison ────────────────────
        # Runs for any row with roll candidates attached — not just ROLL action.
        # EXIT rows with candidates need MC comparison: maybe rolling is better
        # than exiting. HOLD rows with pre-staged candidates also benefit.
        # Step 1: Parse all heuristic candidates (up to 5)
        # Step 2: MC rerank to top 3 by EV
        # Step 3: Final comparison — HOLD/EXIT/ROLL/SPLIT all in one pass
        _has_roll_candidates = any(
            row.get(f"Roll_Candidate_{i}") not in (None, "", "nan", "None")
            for i in range(1, 6)
        )
        if _has_roll_candidates:
            _all_candidates = []
            for _ri in range(1, 6):  # Roll_Candidate_1..5
                _rci_raw = row.get(f"Roll_Candidate_{_ri}")
                if _rci_raw and str(_rci_raw) not in ("", "nan", "None"):
                    try:
                        _rci = json.loads(str(_rci_raw))
                        if isinstance(_rci, dict):
                            _all_candidates.append(_rci)
                    except Exception:
                        pass

            if _all_candidates:
                try:
                    # Rebuild hold terminal at higher path count for MC rerank
                    _hold_terminal_hires = _shared_terminal
                    if _shared_spot and _shared_spot > 0 and _shared_dte >= 1:
                        _hold_rng_hires = np.random.default_rng(SEED)
                        _hold_terminal_hires = _gbm_terminal(
                            _shared_spot, _shared_hv, _shared_t, _n_roll, _hold_rng_hires
                        )

                    # Step 2: MC rerank 5→3
                    rerank_result = mc_roll_rerank(
                        row=row,
                        candidates=_all_candidates,
                        n_paths=_n_roll,
                        rng=rng,
                        prebuilt_hold_terminal=_hold_terminal_hires,
                    )
                    _mc_ranked = rerank_result["mc_ranked"]
                    _mc_ranked_evs = rerank_result["mc_ranked_evs"]
                    df.at[idx, "MC_Roll_Rerank_Note"] = rerank_result["rerank_note"]

                    # Step 3: Unified final comparison
                    # Evaluates HOLD/EXIT/ROLL_ALL_1/2/3 + splits (if qty≥4)
                    # All on same paths. Best EV wins.
                    mc_final = mc_roll_final_comparison(
                        row=row,
                        mc_ranked_candidates=_mc_ranked,
                        mc_ranked_evs=_mc_ranked_evs,
                        n_paths=_n_roll,
                        rng=rng,
                        prebuilt_hold_terminal=_hold_terminal_hires,
                    )
                    for col, val in mc_final.items():
                        if col in df.columns:
                            df.at[idx, col] = val
                    roll_ev_count += 1

                except Exception as e:
                    df.at[idx, "MC_Roll_Note"] = f"MC_ERROR: {e}"

        # ── Exit vs Hold: HOLD actions ────────────────────────────────────
        # Shared terminal paths already built above; daily paths built
        # lazily here for mc_optimal_exit.
        if action in ("HOLD", "HOLD_FOR_REVERSION", "REVIEW"):
            if _shared_spot and _shared_spot > 0 and _shared_dte >= 1 and _shared_daily is None:
                _shared_rng_d = np.random.default_rng(SEED + 1)
                from core.shared.mc.paths import gbm_daily_paths as _shared_gbm_daily
                _shared_daily = _shared_gbm_daily(
                    _shared_spot, _shared_hv, int(_shared_dte),
                    n_paths, _shared_rng_d,
                    iv_schedule=_shared_vol_schedule,
                )

            try:
                mc_h = mc_exit_vs_hold(
                    row=row, n_paths=n_paths, rng=rng,
                    prebuilt_terminal=_shared_terminal,
                )
                for col, val in mc_h.items():
                    if col in df.columns:
                        df.at[idx, col] = val
                hold_count += 1
            except Exception as e:
                df.at[idx, "MC_Hold_Note"] = f"MC_ERROR: {e}"

            # Run MC optimal exit in same pass (avoids second loop in run_all.py)
            _asset = str(row.get("AssetType", "") or "").upper()
            if _asset == "OPTION" and _shared_daily is not None:
                try:
                    from core.management.mc_optimal_exit import mc_optimal_exit
                    _oe_result = mc_optimal_exit(
                        row, rng=rng, prebuilt_paths=_shared_daily,
                    )
                    for col, val in _oe_result.items():
                        if col not in df.columns:
                            df[col] = np.nan if col != "MC_Exit_Note" else ""
                        df.at[idx, col] = val
                except Exception:
                    pass

        # ── Assignment risk: all income positions ─────────────────────────
        # Reuses shared terminal paths (same spot/hv/dte horizon)
        if _is_income_strategy(strategy):
            try:
                mc_a = mc_assignment_risk(
                    row=row, n_paths=n_paths, rng=rng,
                    prebuilt_terminal=_shared_terminal,
                )
                for col, val in mc_a.items():
                    if col in df.columns:
                        df.at[idx, col] = val
                assign_count += 1
            except Exception as e:
                df.at[idx, "MC_Assign_Note"] = f"MC_ERROR: {e}"

        # ── Triple-barrier: all option positions with strike/DTE/entry ───
        # Build daily paths lazily if not already built (HOLD rows built
        # them above; non-HOLD rows build here on demand)
        _tb_strike = float(row.get("Strike", 0) or 0)
        _tb_dte    = float(row.get("DTE", 0) or 0)
        _tb_entry  = float(row.get("Premium_Entry", row.get("Last", 0)) or 0)
        if _tb_strike > 0 and _tb_dte >= 2 and _tb_entry > 0:
            if _shared_daily is None and _shared_spot and _shared_spot > 0 and _shared_dte >= 2:
                _shared_rng_d = np.random.default_rng(SEED + 1)
                from core.shared.mc.paths import gbm_daily_paths as _shared_gbm_daily
                _shared_daily = _shared_gbm_daily(
                    _shared_spot, _shared_hv, int(_shared_dte),
                    n_paths, _shared_rng_d,
                    iv_schedule=_shared_vol_schedule,
                )
            try:
                mc_tb = mc_triple_barrier(
                    row=row, n_paths=n_paths, rng=rng,
                    prebuilt_daily=_shared_daily,
                )
                for col, val in mc_tb.items():
                    if col in df.columns:
                        df.at[idx, col] = val
                tb_count += 1
            except Exception as e:
                df.at[idx, "MC_TB_Note"] = f"MC_ERROR: {e}"

    logger.info(
        f"🎲 Management MC: roll_wait={roll_count} | roll_ev={roll_ev_count} | "
        f"hold={hold_count} | assignment={assign_count} | "
        f"triple_barrier={tb_count} rows processed"
    )
    return df
