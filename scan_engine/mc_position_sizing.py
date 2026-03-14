"""
Monte Carlo Position Sizing — P&L Path Simulation
==================================================

Replaces the flat ATR cap with a distribution-aware P10 loss boundary.

Design
------
For each candidate row we simulate N_PATHS price paths for the underlying
over the remaining DTE using Geometric Brownian Motion calibrated to the
position's *realized* volatility (hv_30 preferred) with an IV skew overlay.

For each path we compute the option P&L at expiry using a simplified
intrinsic + time-value model (full Black-Scholes at expiry = intrinsic;
mid-life = intrinsic + remaining extrinsic estimated from IV surface).

Outputs per row
---------------
  MC_P10_Loss              – 10th-percentile P&L (the "bad day" loss boundary, $)
  MC_P50_Outcome           – median P&L ($)
  MC_P90_Gain              – 90th-percentile P&L ($)
  MC_Win_Probability       – fraction of paths that end profitable
  MC_Assign_Prob           – fraction of paths where short strikes are ITM at expiry
                             (income strategies only; NaN for long directionals)
  MC_Max_Contracts         – max contracts such that CVaR loss ≤ account × max_risk_pct
                             (no ceiling; let CVaR math determine true contract count)
  MC_Sizing_Note           – human-readable sizing rationale
  MC_Paths_Used            – number of paths actually simulated (audit)
  MC_CVaR                  – Conditional VaR (expected loss in worst 10% of paths, $)
  MC_CVaR_P10_Ratio        – CVaR / P10 ratio; >1.5 indicates fat-tailed distribution

Capital Efficiency outputs (for cross-strategy ranking)
---------------------------------------------------------
  Max_Loss_Per_Contract    – worst-case loss per contract ($); longs=premium, shorts=CVaR
  Breakeven_Distance_Pct  – how far underlying must move to break even (% of spot)
  Delta_Per_1k             – delta exposure per $1,000 deployed (directional leverage)
  Vega_Per_1k              – vega exposure per $1,000 deployed (vol sensitivity leverage)
  Theta_Per_1k             – daily theta per $1,000 deployed (time decay per dollar)
  Return_Potential_Per_1k  – P90 outcome per $1,000 deployed (upside per dollar)
  Capital_Efficiency_Score – composite 0–100 ranking score:
                             40% win probability + 30% breakeven reachability +
                             20% return/risk ratio + 10% tail thinness

Contract modelling
------------------
  LONG_CALL / LONG_PUT / LEAP  → long premium; loss capped at debit paid
  CSP / COVERED_CALL / CC      → short premium; loss = assignment - credit
  STRADDLE / STRANGLE          → net debit long vol; P&L from larger move
  CASH_SECURED_PUT             → short put, same as CSP
  PMCC (DIAGONAL_CALL)         → long LEAP + short call; loss capped at net debit

Volatility input priority
--------------------------
  1. hv_30 (30-day realized — most stable for short-dated options)
  2. hv_20 / HV_30_D_Cur
  3. IV30_Call (IV proxy when HV absent)
  4. 0.30 (30% annualized hard fallback)

Calibration
-----------
  Natenberg Ch.12: size to the P10 loss (1 std-dev adverse move stop proxy).
  Cohen Ch.5:      ATR-based sizing is a 1.5-day-move proxy; MC generalises
                   this to the full DTE distribution.
  McMillan Ch.3:   Never size a position so that a single loss exceeds 2% of
                   account (the `max_risk_pct` cap).
"""

from __future__ import annotations

import logging
import numpy as np
import pandas as pd
from typing import Optional

from .ewma_vol import ewma_vol
from core.shared.mc.paths import (
    gbm_terminal, gbm_terminal_with_jumps, JumpConfig, TRADING_DAYS,
)
from core.shared.mc.pnl_models import compute_terminal_pnl

logger = logging.getLogger(__name__)

# ── Simulation constants ────────────────────────────────────────────────────
N_PATHS        = 5_000   # paths per ticker — 5K stabilises CVaR tail (worst 10% uses 500 paths)
MIN_DTE        = 1       # floor to avoid division-by-zero on same-day expiry
HV_FALLBACK    = 0.30    # 30% annualised — conservative fallback when HV/IV unavailable
MAX_RISK_PCT   = 0.02    # 2% account risk per trade (McMillan Ch.3 hard cap)
SEED           = 42      # reproducible runs; None = random each call

# ── Jump-diffusion parameters (Merton 1976) ───────────────────────────────
# Capital Survival Audit Phase 4: GBM systematically underestimates tail risk
# because it assumes continuous paths with no jumps. Adding Poisson jump process
# produces fat-tailed distributions that better model actual options markets.
# Reference: Gatheral Ch.2 — "The Volatility Surface", jump-diffusion models.
JUMP_ENABLED   = True    # Feature flag — set False to revert to pure GBM
JUMP_INTENSITY = 0.05    # λ: ~5% probability of a jump per day (~12 jumps/year)
JUMP_MEAN      = -0.03   # μ_J: average jump size is -3% (negative skew — crashes > rallies)
JUMP_STD       = 0.05    # σ_J: jump magnitude std dev (5% — some jumps are small, some large)


# ── Strategy classification ─────────────────────────────────────────────────
_LONG_PREMIUM   = {"LONG_CALL", "LONG_PUT", "LEAP", "ULTRA_LEAP",
                   "LONG_CALL_DIAGONAL", "LONG_PUT_DIAGONAL",
                   "STRADDLE", "STRANGLE", "LONG_STRADDLE", "LONG_STRANGLE"}
_SHORT_PUT      = {"CASH_SECURED_PUT", "CSP", "PUT_CREDIT_SPREAD", "BULL_PUT_SPREAD"}
_SHORT_CALL     = {"COVERED_CALL", "CC", "CALL_CREDIT_SPREAD", "BEAR_CALL_SPREAD"}
_DIAGONAL_CALL  = {"PMCC"}  # diagonal: long LEAP call + short near-term call
_INCOME         = _SHORT_PUT | _SHORT_CALL | {"IRON_CONDOR", "IRON_BUTTERFLY",
                                               "BUY_WRITE", "COVERED_CALL_DIAGONAL"}


def _resolve_hv(row: pd.Series) -> tuple[float, str]:
    """
    Return (annualised_vol_decimal, source_label) from best available source.

    Priority:
      1. EWMA(λ=0.94) from price_history DuckDB — forward-leaning, reacts
         faster to vol expansion/crush than flat HV windows (RiskMetrics 1994)
      2. hv_30 / HV_30_D_Cur — 30-day realised HV (backward-looking average)
      3. hv_20 / HV_20_D_Cur — 20-day realised HV
      4. IV30_Call / Implied_Volatility — IV proxy when HV absent
      5. HV_FALLBACK (30%) — last resort

    After resolving HV, applies an IV floor: when IV > HV by >20%, the market
    is pricing in expected future vol that HV hasn't captured yet (Bennett:
    "total volatility = diffusive + jump volatility"). Using HV alone would
    underestimate the real distribution width.

    Returns (vol, source) so callers can log which source was used.
    """
    hv_val = None
    hv_src = "HV_FALLBACK"

    # Priority 1: EWMA from DuckDB price history (forward-leaning)
    ticker = row.get("Ticker") or row.get("ticker")
    if ticker:
        try:
            ewma = ewma_vol(str(ticker))
            if ewma is not None and 0.01 <= ewma <= 5.0:
                hv_val, hv_src = ewma, f"EWMA(λ=0.94,{ticker})"
        except Exception:
            pass  # fall through to static columns

    # Priority 2-4: static columns from snapshot
    if hv_val is None:
        for col in ("hv_30", "HV_30_D_Cur", "hv_20", "HV_20_D_Cur",
                    "hv_60", "HV_60_D_Cur", "IV30_Call", "Implied_Volatility"):
            val = row.get(col)
            if val is not None and pd.notna(val):
                v = float(val)
                if v > 1.0:   # stored as percentage (e.g. 28.5 → 0.285)
                    v /= 100.0
                if 0.01 <= v <= 5.0:
                    hv_val, hv_src = v, col
                    break

    if hv_val is None:
        hv_val = HV_FALLBACK

    # ── IV floor: when market-implied vol exceeds realized vol by >20%,
    # use a blend that acknowledges the market's forward-looking view.
    # Bennett: "total vol = diffusive vol + jump vol" — HV only captures diffusive.
    # Blend: 70% HV + 30% IV — anchored on realized but respects the market signal.
    _iv_raw = None
    for _iv_col in ("iv_30d", "IV30_Call", "Implied_Volatility"):
        _iv_v = row.get(_iv_col)
        if _iv_v is not None and pd.notna(_iv_v):
            _iv_f = float(_iv_v)
            if _iv_f > 1.0:
                _iv_f /= 100.0
            if 0.01 <= _iv_f <= 5.0:
                _iv_raw = _iv_f
                break

    if _iv_raw is not None and _iv_raw > hv_val * 1.20:
        # IV exceeds HV by >20% — blend to capture expected future vol
        blended = 0.70 * hv_val + 0.30 * _iv_raw
        hv_src = f"{hv_src}+IV_blend"
        hv_val = blended

    return hv_val, hv_src


def _resolve_spot(row: pd.Series) -> Optional[float]:
    """Return current underlying price from best available column."""
    for col in ("last_price", "Last", "Close", "close", "Spot"):
        val = row.get(col)
        if val is not None and pd.notna(val):
            v = float(val)
            if v > 0:
                return v
    return None


def _resolve_premium(row: pd.Series) -> Optional[float]:
    """Return option mid-price (entry premium) per share."""
    # Mid_Price is the canonical pipeline output column (Step 9B / scan_view).
    # "Mid" / "mid" are aliases used in older code paths.
    for col in ("Mid_Price", "Mid", "mid", "Last", "last", "Total_Debit"):
        val = row.get(col)
        if val is not None and pd.notna(val):
            v = float(val)
            if v > 0:
                return v
    return None


def _resolve_pmcc_legs(row: pd.Series) -> Optional[dict]:
    """Extract PMCC dual-leg parameters.  Returns None if LEAP data missing."""
    leap_strike = row.get("PMCC_LEAP_Strike")
    leap_mid = row.get("PMCC_LEAP_Mid") or row.get("PMCC_LEAP_Last")
    net_debit = row.get("PMCC_Net_Debit")
    if leap_strike is None or pd.isna(leap_strike):
        return None
    if leap_mid is None or pd.isna(leap_mid):
        return None
    return {
        "leap_strike": float(leap_strike),
        "leap_premium": float(leap_mid),
        "net_debit": float(net_debit) if net_debit is not None and pd.notna(net_debit) else None,
    }


def _classify_strategy(strategy_name: str) -> str:
    """Return 'LONG', 'SHORT_PUT', 'SHORT_CALL', 'INCOME', 'DIAGONAL_CALL', or 'UNKNOWN'."""
    s = str(strategy_name).upper().replace(" ", "_").replace("-", "_")
    if s in _DIAGONAL_CALL:
        return "DIAGONAL_CALL"
    if s in _LONG_PREMIUM:
        return "LONG"
    if s in _SHORT_PUT:
        return "SHORT_PUT"
    if s in _SHORT_CALL:
        return "SHORT_CALL"
    if s in _INCOME:
        return "INCOME"
    # Fallback: keyword scan
    if any(k in s for k in ("LONG", "LEAP", "STRADDLE", "STRANGLE", "DEBIT")):
        return "LONG"
    if any(k in s for k in ("CSP", "PUT_SELL", "CASH_SECURED")):
        return "SHORT_PUT"
    if any(k in s for k in ("COVERED", "CC_", "CALL_SELL")):
        return "SHORT_CALL"
    return "UNKNOWN"


def simulate_pnl_paths(
    spot: float,
    strike: float,
    hv_annual: float,
    dte: int,
    premium: float,
    option_type: str,          # 'call' or 'put'
    strategy_class: str,       # 'LONG', 'SHORT_PUT', 'SHORT_CALL', 'INCOME', 'DIAGONAL_CALL'
    n_paths: int = N_PATHS,
    rng: Optional[np.random.Generator] = None,
    *,
    leap_strike: Optional[float] = None,
    net_debit: Optional[float] = None,
    macro_calibration: Optional[dict] = None,
) -> np.ndarray:
    """
    Simulate `n_paths` option P&L outcomes (per-share, $) at expiry.

    Model: GBM for underlying price, intrinsic value at expiry.

    Parameters
    ----------
    spot         : current underlying price
    strike       : option strike (short call strike for DIAGONAL_CALL)
    hv_annual    : annualised HV as decimal (e.g. 0.285)
    dte          : days to expiry (will be floored at MIN_DTE)
    premium      : option mid-price per share (debit paid / credit received)
    option_type  : 'call' or 'put'
    strategy_class: 'LONG' | 'SHORT_PUT' | 'SHORT_CALL' | 'INCOME' | 'DIAGONAL_CALL'
    n_paths      : number of GBM paths
    rng          : optional numpy Generator for reproducibility
    leap_strike  : LEAP call strike (DIAGONAL_CALL only)
    net_debit    : total net debit per share (DIAGONAL_CALL only)

    Returns
    -------
    np.ndarray of shape (n_paths,) — P&L per share in $
    """
    if rng is None:
        rng = np.random.default_rng(SEED)

    dte_safe = max(dte, MIN_DTE)

    # ── GBM terminal prices via shared path generator ────────────────────
    if JUMP_ENABLED:
        # Build JumpConfig, applying macro calibration if present
        _j_intensity = JUMP_INTENSITY
        _j_mean = JUMP_MEAN
        _j_std = JUMP_STD
        if macro_calibration is not None:
            _j_intensity *= macro_calibration.get("jump_intensity_mult", 1.0)
            _j_std *= macro_calibration.get("jump_std_mult", 1.0)
            _j_mean += macro_calibration.get("jump_mean_adj", 0.0)

        jc = JumpConfig(intensity=_j_intensity, mean=_j_mean, std=_j_std)
        s_T = gbm_terminal_with_jumps(
            spot, hv_annual, dte_safe, n_paths, rng, jc
        )
    else:
        s_T = gbm_terminal(spot, hv_annual, dte_safe, n_paths, rng)

    # ── P&L via shared pnl_models dispatch ───────────────────────────────
    is_call = str(option_type).lower().startswith("c")

    # Map strategy_class to shared pnl_model key
    _model_map = {
        "LONG": "long_option",
        "SHORT_PUT": "short_put",
        "SHORT_CALL": "short_call",
        "DIAGONAL_CALL": "pmcc",
    }
    # INCOME class: dispatch by call/put
    if strategy_class == "INCOME":
        model_key = "short_call" if is_call else "short_put"
    else:
        model_key = _model_map.get(strategy_class, "long_option")

    pnl = compute_terminal_pnl(
        model=model_key,
        s_terminal=s_T,
        strike=strike,
        premium=premium,
        is_call=is_call,
        leap_strike=leap_strike or 0.0,
        net_debit=net_debit or 0.0,
    )

    # Per-share → per-contract scale happens outside (caller multiplies by 100)
    return pnl


def mc_size_row(
    row: pd.Series,
    account_balance: float,
    max_risk_pct: float = MAX_RISK_PCT,
    n_paths: int = N_PATHS,
    rng: Optional[np.random.Generator] = None,
) -> dict:
    """
    Run MC simulation for a single candidate row and return sizing dict.

    Returns a dict with MC_* keys. All monetary values are per-strategy
    (i.e. already multiplied by 100 shares/contract).
    """
    result = {
        "MC_P10_Loss":        np.nan,
        "MC_P50_Outcome":     np.nan,
        "MC_P90_Gain":        np.nan,
        "MC_Win_Probability": np.nan,
        "MC_Assign_Prob":     np.nan,
        "MC_Max_Contracts":   1,
        "MC_Sizing_Note":     "MC_SKIPPED",
        "MC_Paths_Used":      0,
        "Sizing_Method_Used": "FIXED",   # overwritten if MC runs
    }

    # ── Resolve inputs ──────────────────────────────────────────────────────
    spot = _resolve_spot(row)
    if spot is None or spot <= 0:
        result["MC_Sizing_Note"] = "MC_SKIP: no valid spot price"
        return result

    # Selected_Strike is the Step 9B canonical name; Strike is the fallback
    # used in earlier pipeline steps and LEAP rows.
    strike_raw = row.get("Selected_Strike")
    if strike_raw is None or (isinstance(strike_raw, float) and pd.isna(strike_raw)):
        strike_raw = row.get("Strike")
    if strike_raw is None or (isinstance(strike_raw, float) and pd.isna(strike_raw)):
        result["MC_Sizing_Note"] = "MC_SKIP: no Selected_Strike"
        return result
    # Multi-leg strategies store strikes as JSON list e.g. "[100.0, 120.0]"
    # PMCC: [leap_strike, short_strike] — use short strike as primary
    # Others: use closer strike (conservative)
    _strike_str = str(strike_raw).strip()
    if _strike_str.startswith("["):
        import json as _json
        try:
            _strikes = _json.loads(_strike_str)
            strike = float(max(_strikes))  # short call strike (higher) for PMCC
        except (ValueError, TypeError):
            result["MC_Sizing_Note"] = f"MC_SKIP: unparseable strike list {_strike_str}"
            return result
    else:
        strike = float(strike_raw)
    if strike <= 0:
        result["MC_Sizing_Note"] = "MC_SKIP: strike ≤ 0"
        return result

    dte_raw = row.get("Actual_DTE") or row.get("Target_DTE") or row.get("Min_DTE")
    if dte_raw is None or pd.isna(dte_raw):
        result["MC_Sizing_Note"] = "MC_SKIP: no DTE"
        return result
    dte = max(int(float(dte_raw)), MIN_DTE)

    premium = _resolve_premium(row)
    if premium is None or premium <= 0:
        result["MC_Sizing_Note"] = "MC_SKIP: no valid premium"
        return result

    hv, hv_source = _resolve_hv(row)

    option_type    = str(row.get("Option_Type", "call") or "call").lower()
    strategy_name  = str(row.get("Strategy_Name", "") or "")
    strategy_class = _classify_strategy(strategy_name)

    # ── PMCC diagonal: resolve LEAP leg parameters ─────────────────────────
    _pmcc_kw = {}
    if strategy_class == "DIAGONAL_CALL":
        pmcc_legs = _resolve_pmcc_legs(row)
        if pmcc_legs is not None:
            _pmcc_kw["leap_strike"] = pmcc_legs["leap_strike"]
            _pmcc_kw["net_debit"] = pmcc_legs["net_debit"] or premium
            # Override premium to net debit for sizing (max loss = net debit)
            premium = pmcc_legs["net_debit"] or premium
        else:
            # Fallback: treat as long call on the short call strike
            strategy_class = "LONG"
        # PMCC option_type is 'pmcc' from step10; force 'call' for the P&L model
        option_type = "call"

    # ── Macro event calibration ─────────────────────────────────────────────
    # When the position is within a macro event week, MC uses empirical
    # event impact data to calibrate jump parameters. This produces fatter
    # tails that reflect actual macro-driven market reactions.
    _macro_cal = None
    _is_macro_week = bool(row.get("Is_Macro_Week", False))
    _macro_type = str(row.get("Macro_Next_Type", "") or "").upper()
    if _is_macro_week and _macro_type:
        try:
            from core.shared.data_layer.macro_event_impact import get_mc_macro_calibration
            _macro_cal = get_mc_macro_calibration(_macro_type)
        except Exception:
            pass  # non-blocking — fall back to default jump params

    # ── Run simulation ──────────────────────────────────────────────────────
    try:
        pnl_per_share = simulate_pnl_paths(
            spot=spot,
            strike=strike,
            hv_annual=hv,
            dte=dte,
            premium=premium,
            option_type=option_type,
            strategy_class=strategy_class,
            n_paths=n_paths,
            rng=rng,
            macro_calibration=_macro_cal,
            **_pmcc_kw,
        )
    except Exception as exc:
        logger.warning(f"MC simulation failed for {row.get('Ticker','?')}: {exc}")
        result["MC_Sizing_Note"] = f"MC_ERROR: {exc}"
        return result

    pnl_per_contract = pnl_per_share * 100.0   # standard 100-share multiplier

    # ── IV crush overlay for long options at elevated IV ──────────────────
    # Natenberg: "mean-reversion characteristics of volatility"
    # Passarelli: "risk of a decline in IV" for long vega strategies
    # When IV_Rank > 70 and IV > HV, deduct expected vega drag from each path.
    # Crush estimate: vega × (IV - HV) × crush_factor, where crush_factor
    # represents the expected fraction of the gap that mean-reverts over DTE.
    # Empirical: ~50% reversion over 30-40 DTE (Natenberg term structure).
    _iv_crush_applied = False
    if strategy_class == "LONG":
        _ivr_mc = pd.to_numeric(row.get('IV_Rank_20D'), errors='coerce')
        _iv_mc  = pd.to_numeric(row.get('iv_30d') or row.get('Implied_Volatility'), errors='coerce')
        _hv_mc  = pd.to_numeric(row.get('hv_30') or row.get('HV30'), errors='coerce')
        _vega_mc = pd.to_numeric(row.get('Vega'), errors='coerce')

        if (pd.notna(_ivr_mc) and float(_ivr_mc) > 70
                and pd.notna(_iv_mc) and pd.notna(_hv_mc)
                and pd.notna(_vega_mc) and float(_vega_mc) > 0):
            iv_f = float(_iv_mc)
            hv_f = float(_hv_mc)
            # Both stored as percentage (e.g. 35.8)
            gap_pts = iv_f - hv_f
            if gap_pts > 0:
                crush_factor = 0.50  # expect 50% reversion
                # vega is per 1% IV move per share; gap in percentage points
                vega_drag_per_share = float(_vega_mc) * gap_pts * crush_factor
                pnl_per_contract -= vega_drag_per_share * 100.0
                _iv_crush_applied = True

    # ── Defined-risk floor for long/diagonal options ─────────────────────
    # A long call/put cannot lose more than premium paid; PMCC cannot lose
    # more than net debit.  Floor all paths at -premium × 100.
    if strategy_class in ("LONG", "DIAGONAL_CALL"):
        _max_loss = premium * 100.0
        pnl_per_contract = np.maximum(pnl_per_contract, -_max_loss)

    p10 = float(np.percentile(pnl_per_contract, 10))
    p50 = float(np.percentile(pnl_per_contract, 50))
    p90 = float(np.percentile(pnl_per_contract, 90))
    win_prob = float(np.mean(pnl_per_contract > 0))

    # ── CVaR (Conditional Value at Risk / Expected Shortfall) ───────────────
    # CVaR = mean of all paths in the worst 10% tail.
    # Unlike P10 (a single quantile), CVaR captures the *shape* of the tail:
    # a GBM tail for a short DTE deep-ITM option can be 2-4× worse than P10.
    # Artzner et al. (1999): CVaR is a coherent risk measure; P10 is not.
    # Natenberg Ch.12: tail sizing should reflect expected loss in worst cases,
    # not the boundary of the worst case.
    tail_mask = pnl_per_contract <= p10
    cvar = float(np.mean(pnl_per_contract[tail_mask])) if tail_mask.any() else p10

    # Assignment probability: fraction of paths ITM at expiry (income strategies)
    is_call = option_type.startswith("c")
    if strategy_class in ("SHORT_PUT", "INCOME", "SHORT_CALL"):
        if is_call:
            assign_prob = float(np.mean(pnl_per_share * 100 < 0))  # loss ↔ ITM for short
        else:
            assign_prob = float(np.mean(pnl_per_contract < 0))
    elif strategy_class == "DIAGONAL_CALL":
        # PMCC: short call assignment risk = fraction of paths where
        # spread is at max profit (S_T > short strike → short call ITM)
        # Approximate from P&L: max gain paths are the assigned paths
        _max_gain = (strike - _pmcc_kw.get("leap_strike", strike)) - premium if _pmcc_kw else 0
        assign_prob = float(np.mean(pnl_per_contract >= _max_gain * 100 * 0.95)) if _max_gain > 0 else np.nan
    else:
        assign_prob = np.nan

    # ── Sizing: max contracts s.t. CVaR loss ≤ account × max_risk_pct ──────
    # CVaR replaces raw P10 as the sizing denominator.
    # P10 = boundary of worst 10%; CVaR = expected loss *within* that boundary.
    # For normal GBM: CVaR ≈ P10 × 1.25. For fat-tailed/short-DTE: up to 3×.
    # This makes the sizing conservative exactly when tails are most dangerous.
    risk_budget  = account_balance * max_risk_pct   # e.g. $100k × 2% = $2,000
    cvar_loss_abs = abs(min(cvar, 0.0))             # 0 if CVaR is positive (rare)

    if cvar_loss_abs > 0:
        max_contracts = int(risk_budget / cvar_loss_abs)
    else:
        # CVaR positive → entire tail is profitable → size up to 3× standard
        max_contracts = 3

    max_contracts = max(1, max_contracts)  # floor 1; no ceiling — let CVaR math decide

    # CVaR-to-P10 ratio: quantifies tail fatness (1.0 = normal; >1.5 = fat tail)
    cvar_p10_ratio = abs(cvar / p10) if p10 != 0 else 1.0

    # ── Capital Efficiency Metrics ──────────────────────────────────────────
    # These 6 metrics allow ranking trades by capital efficiency independently of
    # DQS/TQS signal quality. A high-DQS + high-efficiency trade ranks above a
    # high-DQS + low-efficiency trade (e.g. $27k LEAP vs $2.5k shorter-DTE directional).
    #
    # All "Per_1k" metrics normalise exposure to $1,000 deployed for apples-to-apples
    # comparison across strategies with very different premium levels.
    #
    # References:
    #   Natenberg Ch.6: breakeven analysis; Ch.12: capital efficiency in sizing
    #   Cohen Ch.3: delta per dollar as a position leverage metric
    #   McMillan Ch.11: vega exposure normalisation for cross-strategy comparison

    capital_deployed = premium * 100.0   # per contract (100 shares)
    per_1k = (1000.0 / capital_deployed) if capital_deployed > 0 else np.nan

    # 1. Max loss per contract ($) — worst case for longs is full premium; shorts = unbounded
    #    For longs/diagonals: max loss = capital deployed (premium/net debit paid)
    #    For income/short: max loss = CVaR tail (assignment/gap risk)
    if strategy_class in ("LONG", "DIAGONAL_CALL"):
        max_loss_per_contract = capital_deployed   # premium/net_debit paid, fully at risk
    else:
        max_loss_per_contract = abs(cvar) if cvar < 0 else capital_deployed

    # 2. Breakeven distance (%) — how far underlying must move to break even at expiry
    #    Long call: breakeven = strike + premium; Long put: breakeven = strike - premium
    #    PMCC: breakeven = leap_strike + net_debit
    #    Expressed as % of spot price (higher = harder to reach)
    is_call_be = option_type.startswith("c")
    if strategy_class == "DIAGONAL_CALL" and _pmcc_kw.get("leap_strike"):
        breakeven_price = _pmcc_kw["leap_strike"] + premium
        breakeven_distance_pct = ((breakeven_price - spot) / spot) * 100.0
    elif is_call_be:
        breakeven_price = strike + premium
        breakeven_distance_pct = ((breakeven_price - spot) / spot) * 100.0
    else:
        breakeven_price = strike - premium
        breakeven_distance_pct = ((spot - breakeven_price) / spot) * 100.0

    # 3. Delta per $1,000 deployed — directional leverage per dollar
    #    Higher = more directional bang per buck (shorts have negative delta)
    _delta_raw = row.get("Delta") or row.get("delta")
    delta_val = float(_delta_raw) if _delta_raw is not None and pd.notna(_delta_raw) else np.nan
    delta_per_1k = (delta_val * 100.0 * per_1k) if (not np.isnan(delta_val) and not np.isnan(per_1k)) else np.nan

    # 4. Vega per $1,000 deployed — vol sensitivity per dollar
    #    Higher absolute value = more vega exposure per dollar (vol-sensitive position)
    _vega_raw = row.get("Vega") or row.get("vega")
    vega_val = float(_vega_raw) if _vega_raw is not None and pd.notna(_vega_raw) else np.nan
    vega_per_1k = (vega_val * 100.0 * per_1k) if (not np.isnan(vega_val) and not np.isnan(per_1k)) else np.nan

    # 5. Theta per $1,000 deployed — daily time decay cost per dollar
    #    For long options: theta is negative (daily cost of carry)
    #    For short options: theta is positive (daily income)
    _theta_raw = row.get("Theta") or row.get("theta")
    theta_val = float(_theta_raw) if _theta_raw is not None and pd.notna(_theta_raw) else np.nan
    theta_per_1k = (theta_val * 100.0 * per_1k) if (not np.isnan(theta_val) and not np.isnan(per_1k)) else np.nan

    # 6. Return potential per $1,000 deployed — P90 outcome normalised to capital
    #    = (P90_gain_per_contract / capital_deployed) × (1000 / capital_deployed)
    #    Represents the upside multiple per $1k at the 90th-percentile outcome
    return_potential_per_1k = ((p90 / capital_deployed) * per_1k * 1000.0
                                if (capital_deployed > 0 and not np.isnan(per_1k) and p90 > 0)
                                else np.nan)

    # ── Capital Efficiency Score (0–100) ────────────────────────────────────
    # Composite ranking score. Higher = better efficiency per dollar deployed.
    # Components (all normalised to 0–100 contribution):
    #   40 pts — MC_Win_Probability (primary: expected profitability)
    #   30 pts — Breakeven reachability: lower breakeven_distance_pct = more reachable
    #             Score = max(0, 30 - breakeven_distance_pct × 1.5) [~20% move = 0 pts]
    #   20 pts — Return/risk ratio: P90 / max_loss_per_contract (capped at 2.0 ratio = full 20 pts)
    #   10 pts — Tail thinness: lower CVaR/P10 ratio = thinner tail = 10 pts; fat tail = 0
    _ce_win    = win_prob * 40.0
    _ce_be     = max(0.0, 30.0 - abs(breakeven_distance_pct) * 1.5) if not np.isnan(breakeven_distance_pct) else 15.0
    _rr_ratio  = (p90 / max_loss_per_contract) if (max_loss_per_contract > 0 and p90 > 0) else 0.0
    _ce_rr     = min(20.0, _rr_ratio * 10.0)   # 2.0 ratio → 20 pts
    _ce_tail   = max(0.0, 10.0 - (cvar_p10_ratio - 1.0) * 10.0)  # ratio=1.0 → 10 pts; ≥2.0 → 0 pts
    capital_efficiency_score = round(_ce_win + _ce_be + _ce_rr + _ce_tail, 1)

    # ── Build human-readable note ───────────────────────────────────────────
    hv_pct = hv * 100
    note = (
        f"MC({n_paths:,}p, DTE={dte}, σ={hv_pct:.0f}% [{hv_source}]): "
        f"CVaR=${cvar:+.0f} | P10=${p10:+.0f} | P50=${p50:+.0f} | P90=${p90:+.0f} | "
        f"TailFat={cvar_p10_ratio:.2f}x | Win={win_prob:.0%} | "
        f"MaxC={max_contracts} (CVaR≤{max_risk_pct:.0%}×${account_balance:,.0f}) | "
        f"CapEff={capital_efficiency_score:.0f}/100 | "
        f"BE±{abs(breakeven_distance_pct):.1f}%"
    )
    if not np.isnan(assign_prob):
        note += f" | AssignProb={assign_prob:.0%}"
    if _iv_crush_applied:
        note += " | IV_CRUSH_ADJ"
    if _macro_cal is not None:
        _cal_src = _macro_cal.get("calibration_source", "default")
        _cal_n = _macro_cal.get("n_events", 0)
        note += f" | MACRO_CAL({_macro_type},{_cal_src},n={_cal_n})"

    result.update({
        "MC_CVaR":                      round(cvar, 2),
        "MC_CVaR_P10_Ratio":            round(cvar_p10_ratio, 3),
        "MC_P10_Loss":                  round(p10, 2),
        "MC_P50_Outcome":               round(p50, 2),
        "MC_P90_Gain":                  round(p90, 2),
        "MC_Win_Probability":           round(win_prob, 4),
        "MC_Assign_Prob":               round(assign_prob, 4) if not np.isnan(assign_prob) else np.nan,
        "MC_Max_Contracts":             max_contracts,
        "MC_Sizing_Note":               note,
        "MC_Paths_Used":                n_paths,
        "Sizing_Method_Used":           "MC_CVaR",
        # ── Capital Efficiency ──────────────────────────────────────────────
        "Max_Loss_Per_Contract":        round(max_loss_per_contract, 2),
        "Breakeven_Distance_Pct":       round(breakeven_distance_pct, 2) if not np.isnan(breakeven_distance_pct) else np.nan,
        "Delta_Per_1k":                 round(delta_per_1k, 3) if not np.isnan(delta_per_1k) else np.nan,
        "Vega_Per_1k":                  round(vega_per_1k, 3) if not np.isnan(vega_per_1k) else np.nan,
        "Theta_Per_1k":                 round(theta_per_1k, 3) if not np.isnan(theta_per_1k) else np.nan,
        "Return_Potential_Per_1k":      round(return_potential_per_1k, 2) if not np.isnan(return_potential_per_1k) else np.nan,
        "Capital_Efficiency_Score":     capital_efficiency_score,
    })
    return result


def compute_vince_f_star(
    ticker: str,
    strategy_name: str,
    db_path: str = "data/pipeline.duckdb",
    conn=None,
) -> dict:
    """
    Compute Vince optimal-f and TWR from closed_trades history for a ticker/strategy.

    Vince (1992) — The Mathematics of Money Management:
      TWR  = ∏[i=1,N] (1 + f × (-Trade_i / Biggest_Loss))
      f*   = argmax(TWR) over f ∈ (0, 1)
      G    = TWR^(1/N)   — geometric mean (growth rate per trade)

    Key asymmetry: overbetting destroys capital faster than equivalent underbetting gains.
    A trade with f* = 0.20 → contract fraction = 0.20 of maximum theoretical allocation.

    Parameters
    ----------
    ticker        : underlying symbol
    strategy_name : strategy type (e.g. 'COVERED_CALL')
    db_path       : path to pipeline.duckdb
    conn          : optional existing DuckDB connection to reuse (avoids exclusive-lock
                    conflict when the pipeline already holds a write connection to the
                    same file).  If None, opens a new read_only connection.

    Returns
    -------
    dict with keys:
        f_star          : optimal fraction (0.0–1.0); None if insufficient history
        twr             : Terminal Wealth Relative at f*
        geometric_mean  : per-trade geometric growth rate at f*
        n_trades        : number of qualifying closed trades used
        biggest_loss    : absolute worst loss in the trade set ($)
        source          : 'CLOSED_TRADES' | 'DOCTRINE_FEEDBACK' | 'NONE'
        note            : human-readable summary
    """
    result = {
        "f_star": None,
        "twr": None,
        "geometric_mean": None,
        "n_trades": 0,
        "biggest_loss": None,
        "source": "NONE",
        "note": "VINCE_SKIP: no history",
    }

    try:
        from core.shared.data_layer.duckdb_utils import get_domain_connection, DbDomain
        _owns_conn = conn is None
        if _owns_conn:
            conn = get_domain_connection(DbDomain.MANAGEMENT, read_only=True)

        # --- Attempt 1: closed_trades P&L series ---------------------------------
        rows = conn.execute("""
            SELECT PnL_Dollar
            FROM closed_trades
            WHERE Underlying_Ticker = ?
              AND Strategy = ?
              AND PnL_Dollar IS NOT NULL
              AND NOT isnan(PnL_Dollar)
            ORDER BY closed_at DESC
        """, [ticker, strategy_name]).fetchall()

        pnl_series = [float(r[0]) for r in rows if r[0] is not None]

        # Fallback 1: same ticker, related strategy family (e.g. BUY_WRITE ↔ COVERED_CALL)
        if len(pnl_series) < 3:
            strategy_family = _vince_strategy_family(strategy_name)
            rows2 = conn.execute("""
                SELECT PnL_Dollar
                FROM closed_trades
                WHERE Underlying_Ticker = ?
                  AND PnL_Dollar IS NOT NULL
                  AND NOT isnan(PnL_Dollar)
                  AND Strategy IN ({})
                ORDER BY closed_at DESC
            """.format(",".join("?" * len(strategy_family))),
            [ticker] + list(strategy_family)).fetchall()
            pnl_series = [float(r[0]) for r in rows2 if r[0] is not None]

        # Fallback 2: cross-ticker, same strategy family — strategy-level f*
        # Uses all closed trades across tickers for calibration when per-ticker is sparse.
        # Less precise but better than no history at all.
        if len(pnl_series) < 3:
            strategy_family = _vince_strategy_family(strategy_name)
            rows3 = conn.execute("""
                SELECT PnL_Dollar
                FROM closed_trades
                WHERE PnL_Dollar IS NOT NULL
                  AND NOT isnan(PnL_Dollar)
                  AND Strategy IN ({})
                ORDER BY closed_at DESC
            """.format(",".join("?" * len(strategy_family))),
            list(strategy_family)).fetchall()
            pnl_series = [float(r[0]) for r in rows3 if r[0] is not None]
            if len(pnl_series) >= 3:
                result["source"] = "STRATEGY_FAMILY"  # pre-flag family-level calibration

        if _owns_conn:
            conn.close()

        if len(pnl_series) >= 3:
            f_star, twr, gm = _compute_f_star(pnl_series)
            # source may have been pre-set to STRATEGY_FAMILY by fallback 2 above
            source_label = result.get("source") or "CLOSED_TRADES"
            if source_label == "NONE":
                source_label = "CLOSED_TRADES"
            result.update({
                "f_star":         f_star,
                "twr":            twr,
                "geometric_mean": gm,
                "n_trades":       len(pnl_series),
                "biggest_loss":   min(pnl_series),
                "source":         source_label,
                "note":           (
                    f"VINCE({source_label}, n={len(pnl_series)}, f*={f_star:.3f}, "
                    f"TWR={twr:.3f}, G={gm:.4f}, "
                    f"worst=${min(pnl_series):+.0f})"
                ),
            })
            return result

    except Exception as exc:
        result["note"] = f"VINCE_SKIP: db error — {exc}"
        return result

    result["note"] = f"VINCE_SKIP: <3 closed trades for {ticker}/{strategy_name}"
    return result


def _vince_strategy_family(strategy_name: str) -> set:
    """Return the family group a strategy belongs to for cross-strategy P&L pooling."""
    s = strategy_name.upper()
    income_family = {"COVERED_CALL", "CC", "BUY_WRITE", "CASH_SECURED_PUT", "CSP",
                     "PUT_CREDIT_SPREAD", "BULL_PUT_SPREAD", "CALL_CREDIT_SPREAD",
                     "BEAR_CALL_SPREAD", "IRON_CONDOR", "IRON_BUTTERFLY"}
    long_call_family = {"LONG_CALL", "LEAP", "ULTRA_LEAP", "LONG_CALL_DIAGONAL"}
    long_put_family = {"LONG_PUT", "LONG_PUT_DIAGONAL"}
    vol_family = {"STRADDLE", "STRANGLE", "LONG_STRADDLE", "LONG_STRANGLE"}
    for fam in (income_family, long_call_family, long_put_family, vol_family):
        if s in fam:
            return fam
    return {strategy_name}


def _compute_f_star(pnl_series: list) -> tuple:
    """
    Compute Vince optimal-f by grid search.

    TWR(f) = ∏[i] (1 + f × (-pnl_i / biggest_loss))
    where biggest_loss is the worst (most negative) trade in the series.

    Searches f ∈ [0.01, 0.99] in 99 steps; returns (f_star, TWR_at_fstar, G_at_fstar).
    If no trades are losses, returns f*=0.25 (conservative default — positive expectation
    but no loss data to anchor the f-curve).

    Reference: Vince (1992) — The Mathematics of Money Management, Ch.2–4.
    """
    arr = np.array(pnl_series, dtype=float)
    losses = arr[arr < 0]
    if len(losses) == 0:
        # All winners — no loss curve to anchor; default to conservative 25%
        # Use wins scaled by their own magnitude as denominator
        biggest_win = abs(float(arr.max())) or 1.0
        f_default = 0.25
        hpr_default = 1.0 + f_default * (arr / biggest_win)
        twr_default = float(np.prod(hpr_default))
        gm_default  = float(twr_default ** (1.0 / len(arr)))
        return f_default, twr_default, gm_default

    # biggest_loss: absolute magnitude of the worst trade (denominator in Vince formula)
    # Vince: HPR_i = 1 + f × (trade_i / biggest_loss)
    # where biggest_loss is the POSITIVE magnitude of the worst loss.
    # For a loss of -$2182: HPR = 1 + f × (-2182 / 2182) = 1 - f  → 0 at f=1.0
    # For a win  of +$575: HPR = 1 + f × (+575 / 2182) = 1 + 0.263f → always > 1
    # Reference: Vince (1992) Ch.2 — "divide every trade by the biggest loss"
    biggest_loss = abs(float(losses.min()))

    best_f, best_twr = 0.01, -np.inf
    f_range = np.linspace(0.01, 0.99, 99)

    for f in f_range:
        # HPR_i = 1 + f × (trade_i / biggest_loss)
        # Ruin boundary: HPR ≤ 0 for the worst trade at f ≥ 1.0 (enforced by grid max=0.99)
        hpr = 1.0 + f * (arr / biggest_loss)
        if np.any(hpr <= 0):
            break   # f values beyond this point produce ruin for some trade
        twr = float(np.prod(hpr))
        if twr > best_twr:
            best_twr = twr
            best_f = float(f)

    n = len(arr)
    if best_twr <= 0:
        return 0.01, 1.0, 1.0

    geometric_mean = float(best_twr ** (1.0 / n))
    return best_f, best_twr, geometric_mean


def run_mc_sizing(
    df: pd.DataFrame,
    account_balance: float = 100_000.0,
    max_risk_pct: float = MAX_RISK_PCT,
    n_paths: int = N_PATHS,
    seed: Optional[int] = SEED,
) -> pd.DataFrame:
    """
    Apply MC position sizing to every row in `df`.

    Adds MC_* columns. Preserves all existing columns.
    Rows where MC cannot run (missing data) get MC_SKIP status — the
    ATR_SCALED / FIXED sizing from step13 still provides the fallback
    Thesis_Max_Envelope for those rows.

    Parameters
    ----------
    df            : Step 12 output DataFrame
    account_balance: portfolio value in $
    max_risk_pct  : max fraction of account at risk per trade (default 2%)
    n_paths       : number of GBM paths per ticker
    seed          : RNG seed for reproducibility; None = random

    Returns
    -------
    df with MC_* columns added / updated
    """
    if df.empty:
        return df

    rng = np.random.default_rng(seed)

    # Pre-allocate columns with correct dtypes to avoid pandas FutureWarning
    _float_mc_cols = [
        "MC_CVaR", "MC_CVaR_P10_Ratio",
        "MC_P10_Loss", "MC_P50_Outcome", "MC_P90_Gain",
        "MC_Win_Probability", "MC_Assign_Prob",
        # Capital efficiency metrics
        "Max_Loss_Per_Contract", "Breakeven_Distance_Pct",
        "Delta_Per_1k", "Vega_Per_1k", "Theta_Per_1k",
        "Return_Potential_Per_1k", "Capital_Efficiency_Score",
    ]
    _int_mc_cols   = ["MC_Max_Contracts", "MC_Paths_Used"]
    _str_mc_cols   = ["MC_Sizing_Note", "Sizing_Method_Used"]
    for col in _float_mc_cols:
        if col not in df.columns:
            df[col] = pd.array([np.nan] * len(df), dtype="Float64")
    for col in _int_mc_cols:
        if col not in df.columns:
            df[col] = pd.array([pd.NA] * len(df), dtype="Int64")
    for col in _str_mc_cols:
        if col not in df.columns:
            df[col] = pd.array([""] * len(df), dtype="string")
    mc_cols = _float_mc_cols + _int_mc_cols + _str_mc_cols

    skipped = 0
    for idx, row in df.iterrows():
        try:
            mc = mc_size_row(
                row=row,
                account_balance=account_balance,
                max_risk_pct=max_risk_pct,
                n_paths=n_paths,
                rng=rng,
            )
        except Exception as _row_err:
            logger.debug(f"MC row {row.get('Ticker','?')} failed: {_row_err}")
            mc = {
                "MC_P10_Loss": np.nan, "MC_P50_Outcome": np.nan, "MC_P90_Gain": np.nan,
                "MC_Win_Probability": np.nan, "MC_Assign_Prob": np.nan,
                "MC_Max_Contracts": 1, "MC_Sizing_Note": f"MC_ERROR: {_row_err}",
                "MC_Paths_Used": 0, "Sizing_Method_Used": "FIXED",
            }
        for col, val in mc.items():
            if col in df.columns or col in mc_cols:
                df.at[idx, col] = val
            # Sizing_Method_Used is already in df from ATR step; overwrite only if MC ran
            if col == "Sizing_Method_Used" and mc.get("MC_Paths_Used", 0) > 0:
                df.at[idx, "Sizing_Method_Used"] = val

        if mc.get("MC_Paths_Used", 0) == 0:
            skipped += 1

    ran = len(df) - skipped
    logger.info(
        f"⚡ MC Sizing: {ran}/{len(df)} rows simulated "
        f"({skipped} skipped — missing spot/strike/DTE/premium)"
    )
    return df
