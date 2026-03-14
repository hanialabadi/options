"""
Monte Carlo — Optimal Exit Timing
===================================

Simulates day-by-day P&L paths for an active option position to find the
DTE at which expected P&L peaks before theta acceleration destroys it.

Problem
-------
Doctrine says HOLD, but *how long*? Theta decay is non-linear — the option
loses pennies/day early but dollars/day near expiration. The optimal exit
is the day where E[P&L] peaks (price appreciation minus theta bleed).

Model
-----
Uses shared MCEngine for GBM daily paths; applies macro event IV schedule
and Brenner-Subrahmanyam valuation locally (macro logic stays here, not
in the simulation core).

Outputs per position
--------------------
  MC_Optimal_Exit_DTE     – number of days from now to optimal exit
  MC_Optimal_Exit_Date    – calendar date of optimal exit
  MC_Exit_Peak_EV         – expected P&L at optimal exit ($)
  MC_Exit_Terminal_EV     – expected P&L if held to expiry ($)
  MC_Exit_Theta_Crossover – day where theta > expected daily gain
  MC_Exit_Note            – human-readable context

References
----------
  Passarelli Ch.3: "Time decay accelerates in final 30 days"
  Natenberg Ch.7:  "Theta of an ATM option increases as expiration approaches"
  Jabbour Ch.2:    "Key Time Decay Principle: greatest in last 30 days"
"""

from __future__ import annotations

import logging
import numpy as np
import pandas as pd
from typing import Optional

from core.shared.mc.paths import TRADING_DAYS, gbm_daily_paths
from core.shared.mc.valuation import brenner_option_value, BRENNER_COEFFICIENT, MONEYNESS_DECAY_RATE

logger = logging.getLogger(__name__)

N_PATHS      = 2_000
HV_FALLBACK  = 0.25
SEED         = 42


def mc_optimal_exit(
    row: pd.Series,
    n_paths: int = N_PATHS,
    rng: Optional[np.random.Generator] = None,
    *,
    prebuilt_paths: Optional[np.ndarray] = None,
) -> dict:
    """
    Find optimal exit timing for an active option position.

    Parameters
    ----------
    row            : management position row
    n_paths        : number of MC paths
    rng            : numpy random generator
    prebuilt_paths : optional (n_paths, dte+1) daily price paths from
                     a shared MCEngine — avoids regenerating paths when
                     mc_exit_vs_hold already built them for this row.

    Returns
    -------
    dict with MC_Optimal_Exit_* keys
    """
    _default = {
        "MC_Optimal_Exit_DTE":      np.nan,
        "MC_Exit_Peak_EV":          np.nan,
        "MC_Exit_Terminal_EV":      np.nan,
        "MC_Exit_Theta_Crossover":  np.nan,
        "MC_Exit_Note":             "MC_SKIP",
    }

    # ── Resolve inputs ───────────────────────────────────────────────────
    spot = _get_spot(row)
    if spot is None:
        _default["MC_Exit_Note"] = "MC_SKIP: no spot price"
        return _default

    strike = float(row.get("Strike", 0) or 0)
    if strike <= 0:
        _default["MC_Exit_Note"] = "MC_SKIP: no strike"
        return _default

    dte = float(row.get("DTE", 0) or 0)
    if dte < 3:
        _default["MC_Exit_Note"] = "MC_SKIP: DTE < 3 (too close to expiry)"
        return _default

    # Entry basis (premium paid or received)
    basis = float(row.get("Basis", row.get("Last", 0)) or 0)
    quantity = float(row.get("Quantity", 1) or 1)
    is_long = quantity > 0

    # Option type
    opt_type = str(row.get("Option_Type", "") or "").upper()
    is_call = opt_type.startswith("C")

    # HV
    hv = HV_FALLBACK
    for col in ("HV_20D", "hv_20d", "HV_30D", "hv_30", "IV_Now", "IV_30D"):
        val = row.get(col)
        if val is not None and pd.notna(val):
            v = float(val)
            if v > 1.0:
                v /= 100.0
            if 0.01 <= v <= 5.0:
                hv = v
                break

    # IV for option valuation
    iv = hv
    for col in ("IV_Now", "iv_30d", "IV_30D"):
        val = row.get(col)
        if val is not None and pd.notna(val):
            v = float(val)
            if v > 1.0:
                v /= 100.0
            if 0.01 <= v <= 5.0:
                iv = v
                break

    # ── Macro event awareness ─────────────────────────────────────────────
    _macro_day, _macro_iv_bump, _macro_iv_crush = None, 0.0, 0.0
    _macro_type, _macro_note = "", ""

    try:
        _days_to_macro = float(row.get("Days_To_Macro", 999) or 999)
        _macro_type = str(row.get("Macro_Next_Type", "") or "").upper()
        if _days_to_macro < dte and _macro_type:
            _macro_day = int(_days_to_macro)
            from core.shared.data_layer.macro_event_impact import get_mc_macro_calibration
            _macro_cal = get_mc_macro_calibration(_macro_type)
            if _macro_cal:
                _avg_vix_chg = _macro_cal.get("avg_vix_change_pct")
                _cal_source = _macro_cal.get("calibration_source", "default")
                if _avg_vix_chg is not None and _avg_vix_chg != 0:
                    _macro_iv_bump = max(-0.20, min(0.30, _avg_vix_chg))
                else:
                    _macro_iv_bump = 0.08
                    _cal_source = "default"
                _macro_iv_crush = _macro_iv_bump * 0.70
                _macro_note = (
                    f"{_macro_type} in {_macro_day}d — "
                    f"IV adj {_macro_iv_bump:+.0%} event / "
                    f"{-_macro_iv_crush:+.0%} post ({_cal_source})"
                )
    except Exception:
        _macro_day = None

    # ── Regime-adjusted drift ────────────────────────────────────────────
    from core.shared.mc.inputs import resolve_regime_drift
    _drift, _drift_src = resolve_regime_drift(row)

    # ── Build daily price paths ────────────────────────────────────────────
    n_days = int(dte)

    if prebuilt_paths is not None:
        # Reuse paths from shared MCEngine (Phase 2 optimisation)
        prices = prebuilt_paths
    else:
        # Vol schedule: EWMA→HV blend for vol clustering
        _vol_schedule = None
        try:
            from core.shared.mc.vol_blend import resolve_vol_schedule
            _ticker = (row.get("Ticker") or row.get("ticker")
                       or row.get("Underlying_Ticker") or "")
            _vol_schedule, _ = resolve_vol_schedule(
                str(_ticker) if _ticker else None, hv, n_days,
            )
        except Exception:
            pass

        # Build path modifier for macro event vol amplification
        _path_mod = None
        if _macro_day is not None and 0 < _macro_day < n_days:
            _event_vol_mult = min(1.0 + abs(_macro_iv_bump) * 3, 2.5)
            _md = _macro_day  # capture for closure

            def _path_mod(log_returns):
                log_returns[:, _md] *= _event_vol_mult
                return log_returns

        if rng is None:
            rng = np.random.default_rng(SEED)

        prices = gbm_daily_paths(
            spot, hv, n_days, n_paths, rng,
            iv_schedule=_vol_schedule,
            path_modifier=_path_mod,
            drift=_drift,
        )

    # ── Value option at each day ───────────────────────────────────────────
    daily_ev = np.zeros(n_days + 1)
    daily_median_val = np.zeros(n_days + 1)

    for d in range(n_days + 1):
        s_d = prices[:, d]
        remaining_dte = dte - d

        if remaining_dte <= 0:
            # At expiry: intrinsic only
            opt_val = brenner_option_value(s_d, strike, iv, 0, is_call)
        else:
            # Macro IV schedule: pre-event ramp, event peak, post-event crush
            iv_d = iv
            if _macro_day is not None:
                if d == _macro_day:
                    iv_d = iv * (1.0 + _macro_iv_bump)
                elif d > _macro_day:
                    iv_d = iv * (1.0 - _macro_iv_crush)
                elif d >= _macro_day - 2:
                    _ramp = (_macro_day - d) / 2.0
                    iv_d = iv * (1.0 + _macro_iv_bump * (1.0 - _ramp))

            opt_val = brenner_option_value(
                s_d, strike, iv_d, int(remaining_dte), is_call
            )

        # P&L per contract
        if is_long:
            pnl = (opt_val - basis) * abs(quantity) * 100
        else:
            pnl = (basis - opt_val) * abs(quantity) * 100

        daily_ev[d] = float(np.median(pnl))
        daily_median_val[d] = float(np.median(opt_val))

    # ── Find optimal exit day ──────────────────────────────────────────────
    optimal_day = int(np.argmax(daily_ev))
    peak_ev = daily_ev[optimal_day]
    terminal_ev = daily_ev[-1]

    _macro_ev = None
    if _macro_day is not None and 0 <= _macro_day <= n_days:
        _macro_ev = daily_ev[_macro_day]

    # ── Theta crossover ────────────────────────────────────────────────────
    theta_crossover = np.nan
    for d in range(1, n_days):
        theta_loss = daily_median_val[d - 1] - daily_median_val[d]
        price_gain = daily_ev[d] - daily_ev[d - 1]
        if theta_loss > 0 and price_gain < -theta_loss * abs(quantity) * 50:
            theta_crossover = d
            break

    # ── Build note ─────────────────────────────────────────────────────────
    note_parts = [f"Optimal exit in {optimal_day}d (of {n_days}d remaining)"]
    note_parts.append(f"Peak EV=${peak_ev:+,.0f}")
    note_parts.append(f"Terminal EV=${terminal_ev:+,.0f}")
    if peak_ev > terminal_ev * 1.2:
        note_parts.append("theta acceleration dominates after peak")
    if not np.isnan(theta_crossover):
        note_parts.append(f"theta crossover at day {int(theta_crossover)}")
    if _macro_note:
        note_parts.append(_macro_note)
        if _macro_ev is not None:
            note_parts.append(f"EV at {_macro_type}=${_macro_ev:+,.0f}")
            if optimal_day >= _macro_day:
                note_parts.append(
                    f"peak aligns with macro event — hold through {_macro_type}"
                )

    if _drift != 0.0:
        note_parts.append(f"μ={_drift:+.1%}")

    return {
        "MC_Optimal_Exit_DTE":      optimal_day,
        "MC_Exit_Peak_EV":          round(peak_ev, 2),
        "MC_Exit_Terminal_EV":      round(terminal_ev, 2),
        "MC_Exit_Theta_Crossover":  int(theta_crossover) if not np.isnan(theta_crossover) else np.nan,
        "MC_Exit_Drift":            round(_drift, 4),
        "MC_Exit_Note":             " | ".join(note_parts),
    }


# ── Helpers ──────────────────────────────────────────────────────────────────

def _get_spot(row: pd.Series) -> Optional[float]:
    for col in ("UL Last", "Underlying_Last", "last_price", "Last"):
        val = row.get(col)
        if val is not None and pd.notna(val):
            try:
                v = float(val)
                if v > 0:
                    return v
            except (TypeError, ValueError):
                pass
    return None
