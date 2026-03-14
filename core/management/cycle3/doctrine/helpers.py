"""
Shared helper functions for management doctrine evaluation.

Extracted from engine.py to eliminate duplication across strategy modules.
These functions are used by multiple strategy-specific doctrine files.
"""

import math
import logging
from typing import Dict, Any, Optional

import pandas as pd
import numpy as np

from .thresholds import (
    ADX_STRONG_TREND,
    ADX_TRENDING,
    ADX_VERY_WEAK_TREND,
    ADX_WEAK_TREND,
    CHOPPINESS_BASE,
    CHOPPINESS_FIBONACCI_HIGH,
    CHOPPINESS_RANGE_BOUND,
    HYSTERESIS_PNL_CLEAR_MARGIN,
    HYSTERESIS_ROC5_CLEAR_THRESHOLD,
    HYSTERESIS_ROC5_EXIT_THRESHOLD,
    KER_HIGH,
    KER_VERY_LOW,
    MACRO_CATALYST_DAYS_EXTENDED,
    MACRO_CATALYST_DAYS_THRESHOLD,
    MACRO_CATALYST_DTE_MIN,
    MACRO_CATALYST_EXTENDED_DTE_MIN,
    MACRO_CATALYST_EXTENDED_IV_PCTILE_MIN,
    MACRO_CATALYST_EXTENDED_THETA_BLEED_MAX,
    OBV_SLOPE_FLAT_THRESHOLD,
    POSITION_AGE_THESIS_DEGRADATION_MIN,
    PRIOR_EXIT_PNL_RECOVERY_REQUIRED,
    PRIOR_EXIT_PRICE_MOVE_CLEAR,
    ROC5_BREAKOUT_DOWN,
    ROC10_BREAKDOWN_ACCELERATION,
    ROC_MOMENTUM_THRESHOLD,
    RSI_BEARISH_OVERSOLD,
    RSI_NEUTRAL,
    SIGMA_ROC5_Z_ADVERSE,
    SIGMA_DRIFT_Z_ADVERSE,
    SIGMA_ROC5_Z_CLEAR,
    SIGMA_DAILY_VOL_FLOOR,
    SIGMA_DRIFT_STALENESS_Z,
)

logger = logging.getLogger(__name__)

_SQRT_5 = math.sqrt(5)
_SQRT_252 = math.sqrt(252)


def compute_direction_adverse_signals(
    roc5: float,
    price_drift: float,
    hv_20d: float,
    is_put: bool,
) -> tuple:
    """Compute direction-adverse signals using HV-normalized z-scores.

    Replaces fixed-percent thresholds (ROC5 >= 1.5%, drift >= 2%) with
    z-score normalization against the stock's own realized volatility.
    Falls back to raw-percent thresholds when HV_20D is unavailable.

    Natenberg Ch.5 / Hull Ch.2: volatility is annualized std dev of returns;
    square-root-of-time scaling translates annual vol to shorter horizons.

    Args:
        roc5: 5-day rate of change in PERCENT (e.g. 3.66 means +3.66%)
        price_drift: total price drift as DECIMAL fraction (e.g. 0.05 means +5%)
        hv_20d: 20-day historical volatility as DECIMAL (e.g. 0.28 means 28%)
        is_put: True for long puts, False for long calls

    Returns:
        (roc5_adverse, drift_adverse, roc5_z, drift_z, used_sigma)
        - roc5_adverse: bool — ROC5 exceeds adverse threshold
        - drift_adverse: bool — Price drift exceeds adverse threshold
        - roc5_z: float|None — z-score of ROC5 (None if fallback to raw %)
        - drift_z: float|None — z-score of drift (None if fallback to raw %)
        - used_sigma: bool — True if sigma normalization was used
    """
    # Guard: HV_20D missing, zero, negative, or NaN → fall back to raw %
    _hv_valid = (
        hv_20d is not None
        and not (isinstance(hv_20d, float) and math.isnan(hv_20d))
        and hv_20d > 0
    )

    if not _hv_valid:
        # HV unavailable — direction signal is INDETERMINATE.
        # Return (False, False) so neither direction-adverse EXIT nor
        # direction-confirming suppression fires on incomplete data.
        # Callers should log a warning when used_sigma is False.
        return (False, False, None, None, False)

    # Sigma normalization
    daily_sigma = max(hv_20d / _SQRT_252, SIGMA_DAILY_VOL_FLOOR)
    five_day_sigma = daily_sigma * _SQRT_5

    # ROC5 is in percent (e.g. 3.66), convert to decimal for z-score
    roc5_decimal = roc5 / 100.0
    roc5_z = roc5_decimal / five_day_sigma   # signed z-score
    drift_z = price_drift / daily_sigma       # signed z-score

    # Direction-aware adverse detection
    if is_put:
        # Put: stock going UP is adverse → positive z-scores are adverse
        _roc5_adv = roc5_z >= SIGMA_ROC5_Z_ADVERSE
        _drift_adv = drift_z >= SIGMA_DRIFT_Z_ADVERSE
    else:
        # Call: stock going DOWN is adverse → negative z-scores are adverse
        _roc5_adv = roc5_z <= -SIGMA_ROC5_Z_ADVERSE
        _drift_adv = drift_z <= -SIGMA_DRIFT_Z_ADVERSE

    return (_roc5_adv, _drift_adv, roc5_z, drift_z, True)


def safe_pnl_pct(row: pd.Series) -> Optional[float]:
    """Read P&L % with fallback: Total_GL_Decimal -> PnL_Total/Basis -> None.

    Returns None when P&L data is truly unavailable, allowing callers to
    distinguish 'no data' from 'breakeven' and skip gates accordingly.
    """
    gl = row.get('Total_GL_Decimal')
    if pd.notna(gl):
        return float(gl)
    pnl_total = row.get('PnL_Total')
    basis = row.get('Basis')
    if pd.notna(pnl_total) and pd.notna(basis) and abs(float(basis or 0)) > 0:
        return float(pnl_total) / abs(float(basis))
    return None


def safe_row_float(row, *cols, default: float = 0.0) -> float:
    """NaN-safe numeric read from a pandas row with column fallback chain.

    Python's ``or`` operator treats NaN as truthy, so
    ``float(row.get('A') or row.get('B') or 0)`` silently returns NaN
    when the first column contains NaN — bypassing all fallbacks.

    This helper uses ``pd.notna()`` to properly detect NaN/None before
    falling through to the next candidate column or the default.

    Usage::

        dte = safe_row_float(row, 'Short_Call_DTE', 'DTE', default=999)
        delta = abs(safe_row_float(row, 'Short_Call_Delta', 'Delta'))
    """
    for col in cols:
        val = row.get(col)
        if pd.notna(val):
            return float(val)
    return float(default)


def build_journey_note(row: pd.Series, current_action: str, ul_last: float) -> str:
    """
    Build a compact trade journey note for inclusion in any rationale string.

    Reads the Prior_* columns injected by run_all.py (2.95 Journey Context block)
    and returns a one-liner that tells the continuous story of the trade so far,
    with RAG book citations for each transition type.

    Returns empty string if no prior context is available (first run or DB failure).

    RAG grounding:
      EXIT->HOLD  -- McMillan Ch.4: re-entry after failed exit
      ROLL->HOLD  -- Passarelli Ch.6: don't force roll into choppy market
      HOLD n-day  -- Passarelli Ch.5: patience while theta works
      HOLD->EXIT  -- Natenberg Ch.11: edge is being consumed
    """
    prior_action = str(row.get("Prior_Action") or "").strip().upper()
    prior_price = row.get("Prior_UL_Last")
    prior_ts = row.get("Prior_Snapshot_TS")
    days_ago = row.get("Prior_Days_Ago")

    if not prior_action or prior_action in ("", "NONE", "NAN"):
        return ""  # first run -- no history

    # Format prior timestamp as human-readable
    try:
        _ts_str = pd.Timestamp(prior_ts).strftime("%b %-d") if prior_ts is not None else "prior run"
    except Exception:
        _ts_str = "prior run"

    # Days label
    try:
        _days = float(days_ago)
        _days_str = f"{_days:.0f}d ago" if _days >= 1 else "today"
    except (TypeError, ValueError):
        _days_str = "recently"

    # Price change since last signal
    _price_note = ""
    try:
        _pp = float(prior_price)
        _delta_pct = (ul_last - _pp) / _pp
        _dir = "\u2191" if _delta_pct > 0 else "\u2193"
        _price_note = f" (stock {_dir}{abs(_delta_pct):.1%} since then: ${_pp:.2f} \u2192 ${ul_last:.2f})"
    except (TypeError, ValueError):
        pass

    # Transition-aware citation
    if prior_action == "EXIT" and current_action == "HOLD":
        cite = (
            "Exit signal not acted on \u2014 stock retraced. "
            "Re-evaluate: retracement or new downtrend? "
            "(McMillan Ch.4: re-entry after failed exit)"
        )
    elif prior_action == "EXIT" and current_action == "EXIT":
        cite = "Exit signal persists \u2014 urgency confirmed (McMillan Ch.4)."
    elif prior_action == "ROLL" and current_action == "HOLD":
        cite = (
            "Roll blocked by timing gate \u2014 pre-staged candidates ready when market clarifies "
            "(Passarelli Ch.6: don't force a roll into choppy market)."
        )
    elif prior_action == "HOLD" and current_action == "HOLD":
        cite = f"Holding {_days_str} \u2014 thesis monitoring continues (Passarelli Ch.5: patience while theta works)."
    elif prior_action == "HOLD" and current_action == "EXIT":
        cite = "Condition deteriorated since last HOLD \u2014 escalating to EXIT (Natenberg Ch.11: edge consumed)."
    elif prior_action == "HOLD" and current_action == "ROLL":
        cite = "Timing gate cleared \u2014 executing pre-staged roll (Passarelli Ch.6)."
    elif prior_action == "TRIM" and current_action == "HOLD":
        cite = "Partial trim executed \u2014 holding remaining position (McMillan Ch.4: scale out, not all-or-nothing)."
    elif current_action == "BUYBACK":
        cite = "Carry inversion detected \u2014 buy back short call, hold stock unencumbered (Given Ch.6 + Jabbour Ch.11)."
    elif prior_action == "BUYBACK" and current_action == "HOLD":
        cite = "Short call bought back \u2014 holding stock only until structure resolves (McMillan Ch.3: uncap position)."
    else:
        cite = f"Prior: {prior_action} \u2192 Now: {current_action}."

    return f"\U0001f4d6 Journey ({_ts_str}, {_days_str}): Prior signal was **{prior_action}**{_price_note}. {cite}"


def check_thesis_degradation(row: pd.Series) -> Optional[Dict[str, str]]:
    """
    Cross-temporal thesis check: compare frozen entry chart states vs current states.

    Returns an escalation dict if the regime that justified the trade has structurally
    shifted, else None.

    McMillan Ch.4 / Passarelli Ch.2: position management requires thesis persistence.

    CRITICAL -- vol regime check is direction-aware:
      Long vol  (LONG_CALL, LONG_PUT, LEAP): COMPRESSED->EXTREME is thesis CONFIRMING.
        Natenberg Ch.11: the correct entry for long vol is during compression;
        expansion is the payoff. Flagging this as degradation is wrong.
      Short vol (COVERED_CALL, BUY_WRITE, SHORT_PUT): COMPRESSED->EXTREME is thesis
        BREAKING -- sold premium into low vol, now vol spikes against the position.
    """
    def _sn(val):
        """Normalize enum objects and plain strings to the bare uppercase name."""
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        s = str(val).strip()
        if not s or s in ('nan', 'None', 'N/A'):
            return None
        return (getattr(val, 'value', None) or s.split('.')[-1]).upper()

    # Minimum position age before thesis degradation can fire
    _entry_ts = row.get('Entry_Snapshot_TS') or row.get('Snapshot_TS')
    _snap_ts = row.get('Snapshot_TS')
    try:
        _entry_dt = pd.to_datetime(_entry_ts)
        _snap_dt = pd.to_datetime(_snap_ts)
        _age_days = (_snap_dt - _entry_dt).total_seconds() / 86400
    except Exception:
        _age_days = 999  # unknown age -- allow checks
    if _age_days < POSITION_AGE_THESIS_DEGRADATION_MIN:
        return None

    entry_trend = _sn(row.get('Entry_Chart_State_TrendIntegrity'))
    current_trend = _sn(row.get('TrendIntegrity_State'))
    entry_vol = _sn(row.get('Entry_Chart_State_VolatilityState'))
    current_vol = _sn(row.get('VolatilityState_State'))
    entry_struct = _sn(row.get('Entry_Chart_State_PriceStructure'))
    current_struct = _sn(row.get('PriceStructure_State'))

    # Determine if this is a long-vol or short-vol position
    strategy = str(row.get('Strategy', '') or '').upper()
    qty = float(row.get('Quantity', 1) or 1)
    is_long_vol = (
        'LONG_CALL' in strategy or
        'LONG_PUT' in strategy or
        ('LEAP' in strategy and qty > 0)
    )

    degradations = []

    # Trend collapse
    if entry_trend == 'STRONG_TREND' and current_trend in ('TREND_EXHAUSTED', 'NO_TREND'):
        _entry_bearish_context = entry_struct in ('STRUCTURE_BROKEN', 'STRUCTURAL_DOWN')
        _current_bearish_signals = (
            float(row.get('roc_20', 0) or 0) < -5
            or float(row.get('adx_14', 0) or 0) > ADX_TRENDING
            or float(row.get('rsi_14', RSI_NEUTRAL) or RSI_NEUTRAL) < RSI_BEARISH_OVERSOLD
        )
        if is_long_vol and _entry_bearish_context and _current_bearish_signals:
            pass  # Bearish downtrend decelerating -- not thesis break
        else:
            degradations.append(f"trend collapsed ({entry_trend}\u2192{current_trend})")

    # Vol regime flip -- direction-aware
    if entry_vol == 'COMPRESSED' and current_vol in ('EXPANDING', 'EXTREME'):
        if is_long_vol:
            pass  # COMPRESSED->EXTREME for long vol = thesis CONFIRMING
        else:
            degradations.append(f"vol regime shifted ({entry_vol}\u2192{current_vol})")
    elif entry_vol in ('EXPANDING', 'EXTREME') and current_vol == 'COMPRESSED':
        if is_long_vol:
            degradations.append(f"vol regime shifted ({entry_vol}\u2192{current_vol})")

    # Structure broken
    if entry_struct in ('STRUCTURAL_UP', 'STRUCTURAL_DOWN') and current_struct == 'STRUCTURE_BROKEN':
        degradations.append(f"price structure broken ({entry_struct}\u2192{current_struct})")

    # ── Signal Hub: institutional signal degradation (annotation-only) ──

    # RSI divergence (Murphy 0.691: "divergence is a serious warning")
    _rsi_div = str(row.get('RSI_Divergence', 'None') or 'None')
    if _rsi_div == 'Bearish_Divergence' and not is_long_vol:
        degradations.append("RSI bearish divergence — hidden weakness (Murphy 0.691)")
    elif _rsi_div == 'Bullish_Divergence' and is_long_vol:
        if any(s in strategy for s in ('LONG_PUT', 'BUY_PUT', 'LEAPS_PUT')):
            degradations.append("RSI bullish divergence — bearish thesis weakening (Murphy 0.691)")

    # Weekly trend conflict (Murphy 0.634: "weekly signals filter daily")
    _weekly_bias = str(row.get('Weekly_Trend_Bias', 'Unknown') or 'Unknown')
    if _weekly_bias == 'CONFLICTING':
        degradations.append("weekly trend conflicts with daily (Murphy 0.634)")

    if not degradations:
        return None
    return {"text": "; ".join(degradations)}


def classify_roll_timing(row: pd.Series, build_intraday_advisory_fn=None) -> dict:
    """
    Market timing intelligence for roll decisions.

    Classifies current market conditions as BREAKOUT, CHOPPY, or NEUTRAL to
    determine whether to roll immediately, wait for a better entry, or proceed
    normally.

    McMillan Ch.3: "The most costly mistake in a buy-write is rolling the call
      into a choppy market."
    Passarelli Ch.6: "Wait for directional clarity before redeploying premium."
    Natenberg Ch.8: Range efficiency and trend confirmation as roll timing gates.

    Returns dict with keys:
      timing       : "BREAKOUT_UP" | "BREAKOUT_DOWN" | "CHOPPY" | "NEUTRAL" | "RELEASING" | "DEAD_CAT_BOUNCE"
      urgency_mod  : "CRITICAL" | "HIGH" | "MEDIUM" | "LOW" | None
      action_mod   : "ROLL_NOW" | "WAIT" | "PROCEED"
      reason       : human-readable explanation
    """
    # Dead-cat bounce suppression gate
    recovery_state = (row.get('RecoveryQuality_State') or '').upper()
    if 'DEAD_CAT' in recovery_state or recovery_state == 'DEAD_CAT_BOUNCE':
        resolution = row.get('RecoveryQuality_Resolution_Reason', '')
        return {
            "timing": "DEAD_CAT_BOUNCE",
            "urgency_mod": "LOW",
            "action_mod": "WAIT",
            "reason": (
                f"Dead-cat bounce detected \u2014 structure has NOT changed. "
                f"{resolution}. "
                f"Wait for: higher low + break above prior swing high + ROC10 > 0 + EMA20 turning up. "
                f"Passarelli Ch.6: don't adapt the roll to noise."
            )
        }

    # Read all relevant signal columns
    chop = float(row.get('choppiness_index', CHOPPINESS_BASE) or CHOPPINESS_BASE)
    ker = float(row.get('kaufman_efficiency_ratio', 0.5) or 0.5)
    adx = float(row.get('adx_14', ADX_TRENDING) or ADX_TRENDING)
    bb_z = float(row.get('bb_width_z', 0) or 0)
    roc_5 = float(row.get('roc_5', 0) or 0)
    roc_10 = float(row.get('roc_10', 0) or 0)

    def _sn(col):
        v = row.get(col, '') or ''
        return (getattr(v, 'value', None) or str(v).split('.')[-1]).upper()

    range_eff = _sn('RangeEfficiency_State')
    trend_int = _sn('TrendIntegrity_State')
    mom_vel = _sn('MomentumVelocity_State')
    dir_bal = _sn('DirectionalBalance_State')
    comp_mat = _sn('CompressionMaturity_State')

    # ── Signal Hub: squeeze + OBV context ──
    _squeeze_fired = bool(row.get('Keltner_Squeeze_Fired', False))
    _squeeze_on = bool(row.get('Keltner_Squeeze_On', False))
    _obv_slope = float(row.get('OBV_Slope', 0) or 0)

    # BREAKOUT_UP
    _bu_primary = (trend_int == 'STRONG_TREND'
                   and dir_bal == 'BUYER_DOMINANT'
                   and mom_vel in ('ACCELERATING', 'TRENDING'))
    _bu_secondary = (ker > KER_HIGH
                     and roc_5 > ROC_MOMENTUM_THRESHOLD
                     and chop < CHOPPINESS_BASE
                     and mom_vel in ('ACCELERATING', 'TRENDING'))
    # Keltner squeeze fire = breakout from compression (Raschke / Murphy 0.739)
    _bu_squeeze = (_squeeze_fired and roc_5 > 0 and adx > ADX_VERY_WEAK_TREND)
    is_breakout_up = _bu_primary or _bu_secondary or _bu_squeeze

    # BREAKOUT_DOWN
    is_breakout_down = (
        trend_int in ('STRONG_TREND', 'WEAK_TREND')
        and dir_bal == 'SELLER_DOMINANT'
        and mom_vel in ('ACCELERATING', 'TRENDING')
        and roc_5 < ROC5_BREAKOUT_DOWN
        and roc_10 < ROC10_BREAKDOWN_ACCELERATION
        and chop < CHOPPINESS_RANGE_BOUND
    )

    # CHOPPY
    is_choppy = (
        chop > CHOPPINESS_FIBONACCI_HIGH
        and ker < KER_VERY_LOW
        and range_eff in ('INEFFICIENT_RANGE', 'NOISY')
        and trend_int in ('NO_TREND', 'TREND_EXHAUSTED')
        and adx < ADX_WEAK_TREND
        and abs(roc_5) < ROC_MOMENTUM_THRESHOLD
    )
    # OBV flat confirms lack of conviction in choppy markets (Murphy Ch.7)
    if not is_choppy and chop > CHOPPINESS_BASE and abs(_obv_slope) < OBV_SLOPE_FLAT_THRESHOLD and adx < ADX_TRENDING:
        is_choppy = (abs(roc_5) < ROC_MOMENTUM_THRESHOLD)

    # COMPRESSION_RELEASING
    is_releasing = (
        comp_mat in ('RELEASING', 'POST_EXPANSION')
        and bb_z > 0.5
        and adx < ADX_TRENDING
    )

    if is_breakout_up:
        _advisory = build_intraday_advisory_fn(row, "BREAKOUT_UP") if build_intraday_advisory_fn else None
        return {
            "timing": "BREAKOUT_UP",
            "urgency_mod": "CRITICAL",
            "action_mod": "ROLL_NOW",
            "reason": (
                f"BREAKOUT upward confirmed: chop={chop:.0f}, KER={ker:.2f}, "
                f"ADX={adx:.0f}, ROC5=+{roc_5:.1f}%. "
                f"Roll immediately \u2014 gamma acceleration will make this expensive to delay "
                f"(McMillan Ch.3: Roll Timing / Natenberg Ch.8)."
            ),
            "intraday_advisory": _advisory,
        }

    if is_breakout_down:
        _advisory = build_intraday_advisory_fn(row, "BREAKOUT_DOWN") if build_intraday_advisory_fn else None
        return {
            "timing": "BREAKOUT_DOWN",
            "urgency_mod": "HIGH",
            "action_mod": "ROLL_NOW",
            "reason": (
                f"BREAKDOWN confirmed: chop={chop:.0f}, ROC5={roc_5:.1f}%, ROC10={roc_10:.1f}%. "
                f"Roll call down/out now \u2014 premium cheap and stock falling toward cost basis "
                f"(McMillan Ch.3: Defensive Roll / Passarelli Ch.6)."
            ),
            "intraday_advisory": _advisory,
        }

    if is_choppy:
        return {
            "timing": "CHOPPY",
            "urgency_mod": "LOW",
            "action_mod": "WAIT",
            "reason": (
                f"Market CHOPPY: chop={chop:.0f} (>{CHOPPINESS_FIBONACCI_HIGH:.0f}), KER={ker:.2f} (<{KER_VERY_LOW}), "
                f"ADX={adx:.0f} (<{ADX_WEAK_TREND}). "
                f"Rolling now risks collecting thin premium into a whipsawing market \u2014 "
                f"wait for directional clarity (McMillan Ch.3 / Passarelli Ch.6: Timing)."
            ),
            "intraday_advisory": None,
        }

    if is_releasing:
        return {
            "timing": "RELEASING",
            "urgency_mod": "MEDIUM",
            "action_mod": "WAIT",
            "reason": (
                f"Compression RELEASING (BB_width_z={bb_z:.2f}, ADX={adx:.0f}): "
                f"breakout direction not yet confirmed \u2014 wait 1-2 sessions for clarity "
                f"before rolling to avoid picking the wrong strike (Natenberg Ch.8)."
            ),
            "intraday_advisory": None,
        }

    return {
        "timing": "NEUTRAL",
        "urgency_mod": None,
        "action_mod": "PROCEED",
        "reason": "",
        "intraday_advisory": None,
    }


def build_intraday_roll_advisory(row: pd.Series, timing: str) -> dict:
    """
    Intraday timing advisory for CRITICAL/HIGH urgency ROLL decisions.

    The system uses end-of-day price history -- it cannot see intraday candles,
    VWAP, or real-time volume. This advisory surfaces:
      1. Live proxy signals derived from available data (UL Last, Delta, IV, ATR)
      2. Manual verification checklist.

    This is NOT a gate -- the ROLL decision stands. The advisory answers:
    "Is right now within today's session the ideal execution window?"

    Passarelli Ch.6 / McMillan Ch.3: intraday timing matters for fills.
    """
    ul_last = float(row.get('UL Last', 0) or 0)
    ul_prev = float(row.get('UL_Prev_Close', 0) or 0)
    delta_now = abs(float(row.get('Delta', 0) or 0))
    delta_ent = abs(float(row.get('Delta_Entry', 0) or 0))
    iv_now = float(row.get('IV_30D', 0) or 0)
    iv_entry = float(row.get('IV_30D_Entry', 0) or 0)
    atr_14 = float(row.get('ATR_14', 0) or 0)
    adx = float(row.get('adx_14', ADX_TRENDING) or ADX_TRENDING)

    signals = {}
    notes = []

    # 1. Intraday momentum proxy
    intraday_chg_pct = 0.0
    atr_multiple = 0.0
    if ul_prev > 0 and ul_last > 0:
        intraday_chg_pct = (ul_last - ul_prev) / ul_prev * 100
        signals['intraday_chg_pct'] = round(intraday_chg_pct, 2)
        if timing == 'BREAKOUT_UP' and intraday_chg_pct > 1.5:
            notes.append(
                f"Stock +{intraday_chg_pct:.1f}% today \u2014 momentum running HOT. "
                f"Verify on 5-min chart: impulse wave or deceleration candles? "
                f"Roll at next intraday pullback to EMA5/VWAP for tighter fill."
            )
        elif timing == 'BREAKOUT_UP' and intraday_chg_pct < 0.3:
            notes.append(
                f"Daily trend BREAKOUT_UP but today's move is only +{intraday_chg_pct:.1f}%. "
                f"Momentum may be pausing \u2014 present moment may be ideal roll window."
            )
        elif timing == 'BREAKOUT_DOWN' and intraday_chg_pct < -1.5:
            notes.append(
                f"Stock {intraday_chg_pct:.1f}% today \u2014 breakdown running HOT. "
                f"Execute roll before further drop makes new sale premium thinner."
            )

    # 2. Delta acceleration proxy
    if delta_ent > 0 and delta_now > 0:
        delta_drift = (delta_now - delta_ent) / delta_ent * 100
        signals['delta_drift_pct'] = round(delta_drift, 1)
        if abs(delta_drift) > 20:
            direction = "risen" if delta_drift > 0 else "fallen"
            notes.append(
                f"Delta has {direction} {abs(delta_drift):.0f}% from entry "
                f"(entry: {delta_ent:.2f} \u2192 now: {delta_now:.2f}). "
                f"{'Gamma is accelerating \u2014 roll delay = more expensive buyback.' if delta_drift > 0 else 'Gamma decelerating \u2014 premium thinning on new sale.'}"
            )

    # 3. IV expansion proxy
    if iv_entry > 0 and iv_now > 0:
        iv_drift_pct = (iv_now - iv_entry) / iv_entry * 100
        signals['iv_drift_pct'] = round(iv_drift_pct, 1)
        if iv_drift_pct > 15:
            notes.append(
                f"IV has expanded {iv_drift_pct:.0f}% since entry (entry: {iv_entry:.1%} \u2192 now: {iv_now:.1%}). "
                f"Volatility expansion \u2014 new call premium will be richer than typical. "
                f"Good timing for the sell leg."
            )
        elif iv_drift_pct < -15:
            notes.append(
                f"IV has contracted {abs(iv_drift_pct):.0f}% since entry \u2014 premium is cheaper than expected. "
                f"Consider rolling to a closer strike to compensate for vol compression."
            )

    # 4. ATR context
    if atr_14 > 0 and ul_last > 0 and ul_prev > 0:
        intraday_abs = abs(ul_last - ul_prev)
        atr_multiple = intraday_abs / atr_14
        signals['atr_multiple'] = round(atr_multiple, 2)
        if atr_multiple > 1.5:
            notes.append(
                f"Today's move ({intraday_abs:.2f}) = {atr_multiple:.1f}\u00d7 ATR_14 ({atr_14:.2f}). "
                f"Extended intraday move \u2014 spreads likely wider. Use limit orders only."
            )
        elif atr_multiple < 0.3:
            notes.append(
                f"Today's move is only {atr_multiple:.1f}\u00d7 ATR \u2014 stock is quiet. "
                f"Spreads should be tight. Normal execution expected."
            )

    # 5. ADX trend strength context
    if adx > ADX_STRONG_TREND:
        signals['adx_strength'] = 'STRONG'
        notes.append(
            f"ADX={adx:.0f} (strong trend). Directional conviction \u2014 "
            f"less risk of intraday reversal."
        )
    elif adx < ADX_VERY_WEAK_TREND:
        signals['adx_strength'] = 'WEAK'
        notes.append(
            f"ADX={adx:.0f} (weak trend). Despite timing signal, directional conviction is low. "
            f"Verify on 15-min chart before executing."
        )

    # Manual verification checklist
    checklist = []
    if timing in ('BREAKOUT_UP', 'BREAKOUT_DOWN'):
        checklist = [
            {"item": "Momentum deceleration",
             "description": "On 5-min chart: are bars getting smaller? Wicks forming? RSI(5) diverging?"},
            {"item": "VWAP position",
             "description": "Is price above VWAP (bullish) or below (bearish)? Ideal: pullback to VWAP."},
            {"item": "EMA5/EMA8 angle",
             "description": "Are fast EMAs pointing in breakout direction or flattening/curling back?"},
            {"item": "Reversal candle structure",
             "description": "Check last 3 candles on 5-min for doji, engulfing, hammer/reversal."},
            {"item": "Volume on the move",
             "description": "Is intraday volume above average? Low volume = false breakout risk."},
            {"item": "Bid/ask spread on the call",
             "description": "Check live option chain now. During fast moves, MM spreads widen 2-4x."},
        ]
    else:
        checklist = [
            {"item": "Support level test",
             "description": "Is stock near known support? Confirm break before rolling down."},
        ]

    # Overall intraday confidence
    proxy_confirm_count = sum([
        intraday_chg_pct > 1.5 if timing == 'BREAKOUT_UP' else intraday_chg_pct < -1.5,
        delta_now > delta_ent * 1.1,
        iv_now > iv_entry if timing == 'BREAKOUT_UP' else iv_now < iv_entry,
        atr_multiple > 0.5 if atr_14 > 0 else False,
    ])
    if proxy_confirm_count >= 3:
        proxy_verdict = "EXECUTE_NOW"
        proxy_color = "red"
        proxy_summary = (
            f"{proxy_confirm_count}/4 live proxies confirm the breakout. "
            f"Execute roll during current window \u2014 do not wait for next session."
        )
    elif proxy_confirm_count >= 2:
        proxy_verdict = "FAVORABLE_WINDOW"
        proxy_color = "orange"
        proxy_summary = (
            f"{proxy_confirm_count}/4 live proxies align. "
            f"Good execution window \u2014 verify the 6-item checklist above before sending order."
        )
    else:
        proxy_verdict = "VERIFY_FIRST"
        proxy_color = "blue"
        proxy_summary = (
            f"Only {proxy_confirm_count}/4 live proxies confirm. "
            f"Daily timing signal fired but intraday proxies are mixed. "
            f"Verify manual checklist before executing \u2014 consider waiting 30-60 min."
        )

    return {
        "proxy_verdict": proxy_verdict,
        "proxy_color": proxy_color,
        "proxy_summary": proxy_summary,
        "signals": signals,
        "notes": notes,
        "checklist": checklist,
    }


# ── Calendar-Aware Urgency Adjustment ─────────────────────────────────────
def theta_bleed_adjusted_urgency(
    urgency: str,
    dte: float,
    is_pre_long_weekend: bool,
    is_long_premium: bool,
) -> str:
    """Escalate LOW→MEDIUM urgency when theta bleed risk is elevated.

    When DTE ≤ 21 and entering a long weekend with long premium exposure,
    the position loses 3-4 days of theta with zero stock movement.
    LOW urgency underestimates the risk — escalate to MEDIUM so the
    dashboard surfaces the position in the "act soon" band.

    Passarelli Ch.6, Natenberg Ch.11.
    """
    urgency = str(urgency or '').upper()
    if (
        urgency == 'LOW'
        and is_pre_long_weekend
        and is_long_premium
        and 0 < dte <= 21
    ):
        return 'MEDIUM'
    return urgency


# ── Recovery State Detection ────────────────────────────────────────────────
# Recovery mode is a position STATE, not a separate doctrine.
# When activated, the proposal engine shifts from "exit vs hold" to
# "optimize next premium cycle" — stop recommending EXIT on positions
# where exit locks in permanent capital destruction and premium collection
# is the economically rational repair path.
#
# Jabbour Ch.4: Repair Strategies; McMillan Ch.3: Basis Reduction.

_RECOVERY_LOSS_THRESHOLD = 0.25     # 25%+ unrealized loss from cost basis
_RECOVERY_IV_FLOOR = 0.15           # IV must be above 15% to generate income
_RECOVERY_IV_RANK_FLOOR = 5         # IV rank must be above 5 (not permanently crushed)


def detect_recovery_state(
    row: pd.Series,
    spot: float,
    effective_cost: float,
) -> dict:
    """Detect whether a position is in recovery mode (capital repair via premium).

    Recovery mode activates when:
    1. Stock is 25%+ below effective cost basis (economic damage is done)
    2. Premium has been collected (income strategy active, not first cycle)
    3. IV is high enough to generate meaningful premium (income viable)

    Guardrails — recovery is NOT viable when:
    - IV permanently depressed (can't generate income)
    - Thesis fundamentally BROKEN (structural collapse, not just price decline)

    Returns dict with:
        is_recovery      : bool — position is in recovery mode
        recovery_viable  : bool — income generation still possible
        exit_recovery    : str — reason to exit recovery, or "" if viable
        context          : dict — recovery-specific data for rationale building

    Jabbour Ch.4: Repair Strategies; McMillan Ch.3: Basis Reduction.
    """
    result = {
        "is_recovery": False,
        "recovery_viable": False,
        "exit_recovery": "",
        "context": {},
    }

    if effective_cost <= 0 or spot <= 0:
        return result

    # ── 1. Economic state: is this position deeply underwater? ───────────
    loss_pct = (spot - effective_cost) / effective_cost  # negative = loss
    if loss_pct > -_RECOVERY_LOSS_THRESHOLD:
        return result  # not deep enough to qualify as recovery

    # ── 2. Income strategy active? (2+ cycles of premium collected) ──────
    # A single cycle doesn't establish a recovery path — the trader may not
    # have committed to holding through the loss yet.
    cum_premium = float(row.get('Cumulative_Premium_Collected', 0) or 0)
    gross_premium = float(row.get('Gross_Premium_Collected', 0) or 0)
    premium_collected = cum_premium or gross_premium
    cycle_count = int(row.get('_cycle_count', 1) or 1)
    if premium_collected <= 0 or cycle_count < 2:
        return result  # not yet on recovery path

    # ── 3. IV viability: can we still generate income? ───────────────────
    # NaN-safe IV reads: NaN is truthy in Python so `NaN or fallback` fails.
    # Stock legs: IV_Now/IV_Contract are always NaN (correct — no contract IV).
    # Use IV_30D/IV_Underlying_30D (underlying ATM from iv_term_history).
    _iv_raw = None
    for _iv_col in ('IV_30D', 'IV_Underlying_30D', 'IV_Now', 'IV_Contract'):
        _iv_candidate = row.get(_iv_col)
        if pd.notna(_iv_candidate):
            _iv_raw = _iv_candidate
            break
    from core.shared.finance_utils import normalize_iv as _normalize_iv
    iv_now = _normalize_iv(float(_iv_raw) if _iv_raw is not None else 0.0) or 0.0

    _rank_raw = None
    for _rk_col in ('IV_Rank', 'IV_Percentile', 'CC_IV_Rank'):
        _rk_candidate = row.get(_rk_col)
        if pd.notna(_rk_candidate):
            _rank_raw = _rk_candidate
            break
    iv_rank = float(_rank_raw) if _rank_raw is not None else 50.0

    iv_viable = iv_now > _RECOVERY_IV_FLOOR and iv_rank > _RECOVERY_IV_RANK_FLOOR

    # ── 4. Guardrail: thesis fundamentally broken? ───────────────────────
    thesis = str(row.get('Thesis_State', '') or '').upper()
    ei_state = str(row.get('Equity_Integrity_State', '') or '').upper()

    # Thesis BROKEN = fundamental reason to hold is gone — exit recovery.
    # Without thesis, premium collection is bagholder automation.
    if thesis == 'BROKEN':
        result["exit_recovery"] = (
            "Thesis BROKEN — fundamental reason to hold is gone. "
            "Recovery via premium is not viable without directional thesis. "
            "(McMillan Ch.4: exit failed repair)"
        )
        return result

    # NOTE: Equity BROKEN is NOT a recovery guardrail. A stock at -50%+
    # will almost always have BROKEN equity integrity (EMAs declining, support
    # gone). That's the symptom, not the reason to abandon recovery. The
    # thesis check above is the fundamental guardrail — if thesis is INTACT,
    # the stock may still recover even with broken price structure.

    if not iv_viable:
        result["exit_recovery"] = (
            f"IV permanently depressed ({iv_now:.0%}, rank {iv_rank:.0f}) — "
            f"cannot generate meaningful premium. "
            f"Recovery path uneconomical. (McMillan Ch.3: income floor)"
        )
        return result

    # ── 6. Recovery mode is active — build context ───────────────────────
    cycle_count = int(row.get('_cycle_count', 1) or 1)
    stock_basis_raw = float(row.get('Basis', 0) or 0)
    qty = abs(float(row.get('Quantity', 0) or 0))
    if qty > 0 and stock_basis_raw > 0:
        stock_basis_per_share = stock_basis_raw / qty
    else:
        stock_basis_per_share = effective_cost

    premium_per_share = premium_collected / max(qty, 1) if qty > 0 else premium_collected
    gap_to_breakeven = effective_cost - spot

    # Monthly income estimate — use IV-implied fresh-cycle premium, not
    # the dying remnant of the current call.  At DTE 7 with a nearly
    # worthless call, (Premium_Entry / DTE) * 30 ≈ $0 and falsely declares
    # recovery "uneconomical at inf months."  A fresh 30-day cycle at the
    # same IV would generate meaningful premium.
    # Formula matches dashboard: 0.4 × IV × Spot / √52 × 4.3 (monthly).
    from core.shared.finance_utils import monthly_income as _monthly_income
    dte = safe_row_float(row, 'Short_Call_DTE', 'DTE', default=30)
    last_premium = safe_row_float(row, 'Premium_Entry', 'Last')
    _current_monthly = _monthly_income(last_premium, dte)

    # IV-implied fresh-cycle monthly income (what a new 30-day call pays)
    _iv_implied_monthly = 0.0
    if iv_now > 0 and spot > 0:
        _weekly_premium_est = 0.4 * iv_now * spot / (52 ** 0.5)
        _iv_implied_monthly = _weekly_premium_est * 4.3

    # Use the higher of current-call projection and IV-implied.
    # Near expiry, IV-implied dominates.  Mid-cycle, current call may be
    # more accurate (e.g. ATM call with rich premium vs low-IV forward).
    monthly_income = max(_current_monthly, _iv_implied_monthly)

    # Margin drag
    margin_daily = safe_row_float(row, 'Margin_Cost_Daily')
    margin_monthly = margin_daily * 30
    net_monthly = monthly_income - margin_monthly if margin_monthly > 0 else monthly_income

    months_to_breakeven = (
        gap_to_breakeven / net_monthly
        if net_monthly > 0 and gap_to_breakeven > 0
        else float('inf')
    )

    # Earnings context
    days_to_earnings = float(row.get('days_to_earnings', 999) or 999)
    earnings_beat_rate = float(row.get('Earnings_Beat_Rate', 0) or 0)
    earnings_crush = float(row.get('Earnings_Avg_IV_Crush_Pct', 0) or 0)
    earnings_move_ratio = float(row.get('Earnings_Move_Ratio', 0) or 0)

    # Macro context
    days_to_macro = safe_row_float(row, 'Days_To_Macro', default=999.0)
    macro_event = str(row.get('Macro_Next_Event', '') or '')

    # Chart context — is stock basing?
    adx = float(row.get('adx_14', 25) or 25)
    trend_int = str(row.get('TrendIntegrity_State', '') or '').upper()
    stock_basing = adx < 20 and trend_int in ('NO_TREND', 'TREND_EXHAUSTED', '')

    result["is_recovery"] = True
    result["recovery_viable"] = True
    result["context"] = {
        "loss_pct": loss_pct,
        "gap_to_breakeven": gap_to_breakeven,
        "premium_collected_per_share": premium_per_share,
        "cycles_completed": cycle_count,
        "monthly_income": monthly_income,
        "net_monthly": net_monthly,
        "months_to_breakeven": months_to_breakeven,
        "iv_now": iv_now,
        "iv_rank": iv_rank,
        # Catalysts
        "days_to_earnings": days_to_earnings,
        "earnings_beat_rate": earnings_beat_rate,
        "earnings_crush_pct": earnings_crush,
        "earnings_move_ratio": earnings_move_ratio,
        "days_to_macro": days_to_macro,
        "macro_event": macro_event,
        # Stock behavior
        "stock_basing": stock_basing,
        "adx": adx,
    }
    return result


# ── Moderate Recovery Detection ─────────────────────────────────────────
# Bridges the gap between "approaching hard stop" (-10% to -20%) and
# "deep recovery" (-25%+).  The goal is to catch moderate drawdowns EARLY
# and offer roll-down / basis-reduction paths BEFORE they become extreme.
#
# McMillan Ch.3: "Reduce cost basis aggressively once the stock moves
# against the position — don't wait for the hard stop."
# Jabbour Ch.4: "The time to repair is when the damage is manageable."

_MODERATE_RECOVERY_LOSS_THRESHOLD = 0.10   # 10%+ loss activates moderate recovery
_MODERATE_RECOVERY_LOSS_CEILING = 0.25     # 25%+ → deep recovery (existing)


def detect_moderate_recovery_state(
    row: pd.Series,
    spot: float,
    effective_cost: float,
) -> dict:
    """Detect moderate underwater position with viable roll-recovery path.

    Activates for positions at -10% to -25% drawdown with:
    - At least 1 cycle of premium collected (income path established)
    - IV high enough to generate meaningful premium
    - Thesis not fundamentally BROKEN

    Returns dict with:
        is_moderate_recovery : bool — position qualifies for moderate recovery
        context              : dict — recovery-specific data for gate rationale
    """
    result = {"is_moderate_recovery": False, "context": {}}

    if effective_cost <= 0 or spot <= 0:
        return result

    loss_pct = (spot - effective_cost) / effective_cost  # negative = loss

    # Must be in moderate range: -10% to -25%
    if loss_pct > -_MODERATE_RECOVERY_LOSS_THRESHOLD:
        return result  # not deep enough
    if loss_pct <= -_MODERATE_RECOVERY_LOSS_CEILING:
        return result  # deep recovery handles this (existing detect_recovery_state)

    # Income strategy: at least some premium collected (1 cycle, less strict
    # than deep recovery's 2-cycle requirement — catch drawdowns early).
    cum_premium = float(row.get('Cumulative_Premium_Collected', 0) or 0)
    gross_premium = float(row.get('Gross_Premium_Collected', 0) or 0)
    premium_collected = cum_premium or gross_premium
    if premium_collected <= 0:
        return result  # no income history — can't evaluate recovery path

    # IV viability (same floors as deep recovery)
    from core.shared.finance_utils import normalize_iv as _normalize_iv
    iv_now = _normalize_iv(safe_row_float(row, 'IV_Now', 'IV_30D')) or 0.0
    iv_rank = safe_row_float(row, 'IV_Rank', 'IV_Percentile', default=50)

    if iv_now <= _RECOVERY_IV_FLOOR or iv_rank <= _RECOVERY_IV_RANK_FLOOR:
        return result  # can't generate meaningful premium

    # Thesis not BROKEN
    thesis = str(row.get('Thesis_State', '') or '').upper()
    if thesis == 'BROKEN':
        return result

    # ── Build context: post-roll economics estimate ─────────────────────
    cycle_count = int(row.get('_cycle_count', 1) or 1)
    qty = abs(float(row.get('Quantity', 0) or 0))
    premium_per_share = premium_collected / max(qty, 1) if qty > 0 else premium_collected
    gap_to_breakeven = effective_cost - spot

    from core.shared.finance_utils import monthly_income as _monthly_income
    dte = safe_row_float(row, 'Short_Call_DTE', 'DTE', default=30)
    last_premium = safe_row_float(row, 'Premium_Entry', 'Last')
    _current_monthly = _monthly_income(last_premium, dte)

    # IV-implied fresh-cycle monthly income (same as detect_recovery_state)
    _iv_implied_monthly = 0.0
    if iv_now > 0 and spot > 0:
        _weekly_premium_est = 0.4 * iv_now * spot / (52 ** 0.5)
        _iv_implied_monthly = _weekly_premium_est * 4.3
    monthly_income = max(_current_monthly, _iv_implied_monthly)

    margin_daily = safe_row_float(row, 'Margin_Cost_Daily')
    margin_monthly = margin_daily * 30
    net_monthly = monthly_income - margin_monthly if margin_monthly > 0 else monthly_income

    months_to_breakeven = (
        gap_to_breakeven / net_monthly
        if net_monthly > 0 and gap_to_breakeven > 0
        else float('inf')
    )

    result["is_moderate_recovery"] = True
    result["context"] = {
        "loss_pct": loss_pct,
        "gap_to_breakeven": gap_to_breakeven,
        "premium_collected_per_share": premium_per_share,
        "cycles_completed": cycle_count,
        "monthly_income": monthly_income,
        "net_monthly": net_monthly,
        "months_to_breakeven": months_to_breakeven,
        "iv_now": iv_now,
        "iv_rank": iv_rank,
        "thesis": thesis,
    }
    return result


# ── Recovery Premium Mode Detection ───────────────────────────────────────
# Determines whether a damaged buy-write should switch from trade management
# mode to recovery premium mode (multi-cycle basis reduction optimization).
#
# Delegates to existing detect_recovery_state() and detect_moderate_recovery_state()
# for eligibility + base context, then extends with premium-mode-specific fields.

def should_enter_recovery_premium_mode(
    row: pd.Series,
    spot: float,
    effective_cost: float,
) -> dict:
    """Determine if a buy-write should enter Recovery Premium Mode.

    Delegates eligibility checks to existing detect_recovery_state() and
    detect_moderate_recovery_state() — no duplication of IV/thesis/premium guards.
    Adds premium-mode-specific fields (account type, rally signals, strike
    discipline, annualized yield).

    Returns dict with:
        should_activate : bool — enter recovery premium mode
        context         : dict — all data needed for recovery premium strategy
        exit_reason     : str — why mode was NOT activated (empty if activated)
    """
    from core.management.cycle3.doctrine.thresholds import (
        RECOVERY_MONTHS_TO_BREAKEVEN_EXIT,
    )

    result = {"should_activate": False, "context": {}, "exit_reason": ""}

    if effective_cost <= 0 or spot <= 0:
        result["exit_reason"] = "invalid_cost_or_spot"
        return result

    # ── Delegate to existing recovery detectors ───────────────────────────
    # deep recovery (≥25% loss, 2+ cycles) or moderate (≥10% loss, 1+ cycle)
    deep = detect_recovery_state(row, spot, effective_cost)
    moderate = detect_moderate_recovery_state(row, spot, effective_cost)

    if deep["is_recovery"] and deep["recovery_viable"]:
        base_ctx = deep["context"]
    elif moderate["is_moderate_recovery"]:
        base_ctx = moderate["context"]
    else:
        # Neither detector activated — diagnose specific reason
        loss_pct = (spot - effective_cost) / effective_cost
        thesis = str(row.get('Thesis_State', '') or '').upper()
        if loss_pct > -0.10:
            result["exit_reason"] = f"loss_insufficient ({loss_pct:.1%} vs -10% floor)"
        elif thesis == 'BROKEN':
            result["exit_reason"] = "thesis_broken"
        elif deep.get("exit_recovery"):
            result["exit_reason"] = deep["exit_recovery"]
        else:
            # Check IV — detectors reject when IV too low for viable income
            iv_now = float(row.get('IV_Now', 0) or row.get('IV_30D', 0) or 0)
            if iv_now < 100:
                iv_now = iv_now  # already decimal or raw
            from core.management.cycle3.doctrine.thresholds import RECOVERY_PREMIUM_IV_FLOOR
            if iv_now < RECOVERY_PREMIUM_IV_FLOOR:
                result["exit_reason"] = "iv_too_low"
            else:
                result["exit_reason"] = "no_income_path"
        return result

    # ── Thesis guard (both detectors already check, but be explicit) ──────
    thesis = str(row.get('Thesis_State', '') or '').upper()
    if thesis == 'BROKEN':
        result["exit_reason"] = "thesis_broken"
        return result

    # ── Uneconomical recovery guard ───────────────────────────────────────
    months_to_be = base_ctx.get("months_to_breakeven", float('inf'))
    if months_to_be > RECOVERY_MONTHS_TO_BREAKEVEN_EXIT and months_to_be != float('inf'):
        result["exit_reason"] = f"recovery_uneconomical ({months_to_be:.0f} months)"
        return result

    # ── Extend base context with premium-mode-specific fields ─────────────
    # Account type
    from core.shared.finance_utils import is_retirement_account as _is_retire
    account = str(row.get('Account', '') or '')
    is_retirement = _is_retire(account)

    # Recalculate margin as zero for Roth (base context may include margin)
    if is_retirement:
        base_ctx["net_monthly"] = base_ctx.get("monthly_income", 0)
        base_ctx["margin_monthly"] = 0.0
        # Recalculate months_to_breakeven without margin drag
        gap = base_ctx.get("gap_to_breakeven", 0)
        net_mo = base_ctx["net_monthly"]
        base_ctx["months_to_breakeven"] = (
            gap / net_mo if net_mo > 0 and gap > 0 else float('inf')
        )
    else:
        base_ctx.setdefault("margin_monthly", safe_row_float(row, 'Margin_Cost_Daily') * 30)

    # Strike / call state
    from core.shared.finance_utils import effective_cost_per_share as _ecp
    strike = safe_row_float(row, 'Short_Call_Strike', 'Strike')
    dte = safe_row_float(row, 'Short_Call_DTE', 'DTE', default=30)
    has_active_call = dte > 0 and dte < 900 and strike > 0
    delta = abs(safe_row_float(row, 'Short_Call_Delta', 'Delta'))
    last_premium = safe_row_float(row, 'Premium_Entry', 'Last')

    # Broker cost per share
    _, broker_cost_ps, _ = _ecp(row, spot_fallback=effective_cost)
    if broker_cost_ps <= 0:
        broker_cost_ps = effective_cost
    qty = abs(safe_row_float(row, 'Quantity'))

    # Annualized yield on current stock value
    net_monthly = base_ctx.get("net_monthly", 0)
    annualized_yield = (net_monthly * 12) / spot if spot > 0 and net_monthly > 0 else 0.0

    # Basis reduction efficiency
    premium_ps = base_ctx.get("premium_collected_per_share", 0)
    basis_reduction_pct = premium_ps / broker_cost_ps if broker_cost_ps > 0 else 0.0

    # Strike vs cost basis
    strike_vs_cost = (strike - effective_cost) / effective_cost if effective_cost > 0 and strike > 0 else 0.0

    # ── Merge base context + extended fields ──────────────────────────────
    result["should_activate"] = True
    result["context"] = {
        # From existing detector
        "loss_pct": base_ctx.get("loss_pct", 0),
        "gap_to_breakeven": base_ctx.get("gap_to_breakeven", 0),
        "premium_collected_per_share": premium_ps,
        "cycles_completed": base_ctx.get("cycles_completed", 1),
        "monthly_income": base_ctx.get("monthly_income", 0),
        "net_monthly": net_monthly,
        "margin_monthly": base_ctx.get("margin_monthly", 0),
        "months_to_breakeven": base_ctx.get("months_to_breakeven", float('inf')),
        "iv_now": base_ctx.get("iv_now", 0),
        "iv_rank": base_ctx.get("iv_rank", 50),
        # Premium-mode extensions
        "annualized_yield": annualized_yield,
        "basis_reduction_pct": basis_reduction_pct,
        "has_active_call": has_active_call,
        "strike": strike,
        "strike_vs_cost": strike_vs_cost,
        "dte": dte,
        "delta": delta,
        "last_premium": last_premium,
        "is_retirement": is_retirement,
        "roc5": safe_row_float(row, 'ROC5', default=0.0),
        "roc10": safe_row_float(row, 'ROC10', default=0.0),
        "adx": base_ctx.get("adx", float(row.get('adx_14', 25) or 25)),
        "rsi": float(row.get('rsi_14', 50) or 50),
        "stock_basing": base_ctx.get("stock_basing", False),
        "thesis": thesis,
        "days_to_earnings": base_ctx.get("days_to_earnings", float(row.get('days_to_earnings', 999) or 999)),
        "earnings_beat_rate": base_ctx.get("earnings_beat_rate", float(row.get('Earnings_Beat_Rate', 0) or 0)),
        "days_to_macro": safe_row_float(row, 'Days_To_Macro', default=999.0),
        "macro_event": str(row.get('Macro_Next_Event', '') or ''),
        "days_since_roll": safe_row_float(row, 'Days_Since_Last_Roll', default=999.0),
        "spot": spot,
        "effective_cost": effective_cost,
        "broker_cost_per_share": broker_cost_ps,
        "qty": qty,
    }
    return result


# ── Boundary Hysteresis — reusable gate stabilizer ────────────────────────
# Prevents EXIT↔HOLD flip-flop on positions near gate thresholds.
# Once EXIT triggers, the signal must clear by a margin to flip to HOLD.

# Normalized gate-family identifiers. Each family maps to a list of known
# Doctrine_Source substrings that belong to that family.  This is a
# controlled lookup — not arbitrary free-text search.
GATE_FAMILY_IDS: Dict[str, list] = {
    'DIRECTION_ADVERSE': [
        'Direction Adverse',
        'Direction Adverse — Catalyst',
        'Direction Adverse — SRS',
        'Direction Adverse — New Position Grace',
    ],
    'THETA_BLEED': [
        'Theta Bleed',
        'Time Value Exhausted',
    ],
    'THETA_DOMINANT': [
        'Theta Awareness',
        'Theta Dominance',
        'Multi-Leg Theta Management',
        'Time-to-Impulse',
    ],
    'PROFIT_CAPTURE': [
        'Profit Capture',
        'Weak Entry Profit Capture',
        'Winner Management',
    ],
    'RECOVERY_IMPOSSIBLE': [
        'Recovery Impossible',
    ],
}


def _matches_gate_family(doctrine_source: str, gate_family: str) -> bool:
    """Check if a Doctrine_Source string belongs to a gate family."""
    substrings = GATE_FAMILY_IDS.get(gate_family, [])
    if not substrings or not doctrine_source:
        return False
    src_upper = doctrine_source.upper()
    return any(s.upper() in src_upper for s in substrings)


def check_hysteresis(
    prior_action: str,
    prior_doctrine_source: str,
    gate_family: str,
    current_signal: float,
    exit_threshold: float,
    clear_threshold: float,
    pnl_pct: Optional[float],
    pnl_exit_threshold: float,
    pnl_clear_margin: float,
) -> tuple:
    """Determine if a prior EXIT should persist due to hysteresis.

    Only applies when:
      1. prior_action == 'EXIT'
      2. prior_doctrine_source matches the same gate_family

    NaN/None inputs → "cannot determine clearance" → EXIT persists.

    Args:
        prior_action: Previous run's Action for this position.
        prior_doctrine_source: Previous run's Doctrine_Source.
        gate_family: Normalized key (e.g. 'DIRECTION_ADVERSE').
        current_signal: Current signal value (e.g. roc_5). NaN-safe.
        exit_threshold: Signal threshold that originally triggered EXIT.
        clear_threshold: Signal must cross this to CLEAR the EXIT.
        pnl_pct: Current P&L percentage (None/NaN = cannot clear).
        pnl_exit_threshold: P&L threshold used by the gate (e.g. -0.10).
        pnl_clear_margin: Additional P&L improvement needed (e.g. 0.05).

    Returns:
        (should_force_exit: bool, reason: str)
    """
    # Only applies to prior EXIT from the same gate family
    if prior_action != 'EXIT':
        return (False, '')
    if not _matches_gate_family(prior_doctrine_source, gate_family):
        return (False, '')

    # NaN signal → cannot determine clearance → exit persists
    if current_signal is None or (isinstance(current_signal, float) and math.isnan(current_signal)):
        return (True, 'cannot clear: current_signal is NaN')

    # NaN P&L → cannot determine clearance → exit persists
    if pnl_pct is None or (isinstance(pnl_pct, float) and math.isnan(pnl_pct)):
        return (True, 'cannot clear: pnl_pct is NaN')

    # Check if signal has cleared the hysteresis band
    # For puts: exit_threshold is positive (e.g. +1.5), clear is positive (e.g. +0.5)
    #   Signal cleared when signal <= clear_threshold
    # For calls: exit_threshold is negative (e.g. -1.5), clear is negative (e.g. -0.5)
    #   Signal cleared when signal >= clear_threshold
    if exit_threshold >= 0:
        signal_cleared = current_signal <= clear_threshold
    else:
        signal_cleared = current_signal >= clear_threshold

    # Check if P&L has improved past the clear margin
    pnl_cleared = pnl_pct >= (pnl_exit_threshold + pnl_clear_margin)

    if not (signal_cleared and pnl_cleared):
        reasons = []
        if not signal_cleared:
            reasons.append(
                f'signal {current_signal:+.1f}% not cleared '
                f'(need {"<=" if exit_threshold >= 0 else ">="}{clear_threshold:+.1f}%)'
            )
        if not pnl_cleared:
            reasons.append(
                f'P&L {pnl_pct:.1%} not cleared '
                f'(need >={pnl_exit_threshold + pnl_clear_margin:.1%})'
            )
        return (True, f'hysteresis: {"; ".join(reasons)}')

    return (False, '')


def check_prior_exit_persistence(
    row: "pd.Series",
    is_put: bool,
    *,
    macro_days: Optional[float] = None,
    macro_type: Optional[str] = None,
) -> tuple:
    """Cross-gate EXIT persistence: prevent one-day EXIT→HOLD flip-flop.

    Unlike check_hysteresis() (gate-family-specific), this checks whether
    ANY prior EXIT should persist because conditions haven't materially improved.

    Clearance requires EITHER:
      - P&L improved by ≥ PRIOR_EXIT_PNL_RECOVERY_REQUIRED (5pp), OR
      - Favorable price move ≥ PRIOR_EXIT_PRICE_MOVE_CLEAR (2%)

    Exception: if a HIGH-impact macro event (FOMC/CPI/NFP) is within
    MACRO_CATALYST_DAYS_THRESHOLD and DTE ≥ MACRO_CATALYST_DTE_MIN,
    the macro event IS the expected catalyst — clear the prior EXIT.

    Args:
        row: Position data (Prior_Action, Prior_PnL_Pct, PnL_Pct,
             Price_Drift_Pct, DTE, Prior_Doctrine_Source, etc.)
        is_put: True for put positions, False for calls.
        macro_days: Days to next macro event (None if unknown).
        macro_type: Macro event type (FOMC, CPI, NFP, GDP, PCE).

    Returns:
        (should_persist_exit: bool, reason: str, macro_catalyst: bool)
    """
    prior_action = str(row.get('Prior_Action', '') or '').upper()
    if prior_action != 'EXIT':
        return (False, '', False)

    dte = float(row.get('DTE', 999) or 999)
    pnl_pct = _safe_float(row.get('PnL_Pct', None))
    prior_pnl = _safe_float(row.get('Prior_PnL_Pct', None))
    drift_pct = _safe_float(row.get('Price_Drift_Pct', None))
    prior_source = str(row.get('Prior_Doctrine_Source', '') or '')

    # ── Macro catalyst exception ──────────────────────────────────────────
    _HIGH_IMPACT = {'FOMC', 'CPI', 'NFP'}
    macro_catalyst = False
    if (macro_days is not None
            and macro_type is not None
            and macro_type in _HIGH_IMPACT
            and macro_days <= MACRO_CATALYST_DAYS_THRESHOLD
            and dte >= MACRO_CATALYST_DTE_MIN):
        macro_catalyst = True
        return (
            False,
            f"Prior EXIT cleared: {macro_type} in {macro_days:.0f}d is vol catalyst "
            f"for long premium at DTE={dte:.0f} (Bennett: macro compress/expand vol). "
            f"Hold through event if conviction intact.",
            True,
        )

    # ── Extended macro window for event-driven long premium ──────────────
    # Long premium positions with high IV, low theta bleed, intact thesis,
    # and sufficient DTE get a wider macro catalyst window (7d vs 5d).
    # Prevents false EXIT persistence when the catalyst is 6-7 days out
    # but still clearly the thesis driver. (NVDA put / FOMC boundary case.)
    _LONG_PREMIUM = {'LONG_PUT', 'LONG_CALL', 'LEAPS_CALL', 'LEAPS_PUT',
                     'BUY_CALL', 'BUY_PUT', 'LEAP_CALL', 'LEAP_PUT',
                     'LONG_STRADDLE', 'LONG_STRANGLE'}
    _strategy = str(row.get('Strategy', '') or row.get('Entry_Structure', '') or '').upper()
    if (macro_days is not None
            and macro_type is not None
            and macro_type in _HIGH_IMPACT
            and MACRO_CATALYST_DAYS_THRESHOLD < macro_days <= MACRO_CATALYST_DAYS_EXTENDED
            and _strategy in _LONG_PREMIUM
            and dte >= MACRO_CATALYST_EXTENDED_DTE_MIN):
        _thesis = str(row.get('Thesis_State', '') or '').upper()
        _conviction = str(row.get('Conviction_Status', '') or '').upper()
        _recovery = str(row.get('Recovery_Feasibility', '') or '').upper()
        _iv_pctile = _safe_float(row.get('IV_Percentile', None))
        _last_price = _safe_float(row.get('Last', None))
        _theta = _safe_float(row.get('Net_Theta', None) or row.get('Theta', None))

        # Compute theta bleed as % of premium per day
        _theta_bleed_pct = None
        if _theta is not None and _last_price is not None and _last_price > 0:
            _theta_bleed_pct = abs(_theta) / (_last_price * 100)

        # IV condition: high IV_Percentile OR positive IV vs HV gap (event vol bid)
        _iv_vs_hv = _safe_float(row.get('IV_vs_HV_Gap', None))
        _iv_ok = (
            (_iv_pctile is not None and _iv_pctile >= MACRO_CATALYST_EXTENDED_IV_PCTILE_MIN)
            or (_iv_vs_hv is not None and _iv_vs_hv > 0)
        )

        _qualifies = (
            _thesis == 'INTACT'
            and _conviction in ('STABLE', 'IMPROVING')
            and _recovery in ('FEASIBLE', 'LIKELY')
            and _iv_ok
            and (_theta_bleed_pct is None or _theta_bleed_pct <= MACRO_CATALYST_EXTENDED_THETA_BLEED_MAX)
        )

        if _qualifies:
            macro_catalyst = True
            return (
                False,
                f"Prior EXIT cleared (extended window): {macro_type} in {macro_days:.0f}d is vol catalyst "
                f"for {_strategy} at DTE={dte:.0f}, IV_Pctile={_iv_pctile:.0f}%, "
                f"thesis={_thesis}, conviction={_conviction}. "
                f"(Bennett: macro event justifies holding event-driven long premium.)",
                True,
            )

    # ── P&L recovery check ────────────────────────────────────────────────
    pnl_recovered = False
    if pnl_pct is not None and prior_pnl is not None:
        pnl_improvement = pnl_pct - prior_pnl
        pnl_recovered = pnl_improvement >= PRIOR_EXIT_PNL_RECOVERY_REQUIRED
    elif pnl_pct is not None:
        # No prior P&L to compare — cannot confirm recovery
        pnl_recovered = False

    # ── Favorable price move check ────────────────────────────────────────
    price_moved = False
    if drift_pct is not None:
        # For puts: negative drift (stock falling) is favorable
        # For calls: positive drift (stock rising) is favorable
        favorable_move = -drift_pct if is_put else drift_pct
        price_moved = favorable_move >= PRIOR_EXIT_PRICE_MOVE_CLEAR

    # ── Resolution ────────────────────────────────────────────────────────
    if pnl_recovered or price_moved:
        clear_reason = []
        if pnl_recovered:
            clear_reason.append(f"P&L improved {pnl_pct - prior_pnl:+.1%}")
        if price_moved:
            clear_reason.append(f"favorable price move {drift_pct:+.1%}")
        return (False, f"Prior EXIT cleared: {'; '.join(clear_reason)}", False)

    # EXIT persists
    reasons = []
    if pnl_pct is not None and prior_pnl is not None:
        reasons.append(f"P&L {pnl_pct - prior_pnl:+.1%} (need ≥{PRIOR_EXIT_PNL_RECOVERY_REQUIRED:+.0%})")
    else:
        reasons.append("P&L recovery unknown (missing prior data)")
    if drift_pct is not None:
        reasons.append(f"price move {drift_pct:+.1%} (need ≥{PRIOR_EXIT_PRICE_MOVE_CLEAR:+.0%} favorable)")
    else:
        reasons.append("price move unknown")

    return (
        True,
        f"Prior EXIT persists ({prior_source}): conditions not materially improved — "
        f"{'; '.join(reasons)}. (McMillan Ch.4: exit signals that persist are not noise.)",
        False,
    )


def _safe_float(val) -> Optional[float]:
    """Convert to float, returning None for NaN/None/invalid."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


# ── Forward-Economics Guard ──────────────────────────────────────────────
# Lightweight forward-income computation for hard-stop override decisions.
# Same formula as the recovery state detector (IV-implied monthly income)
# but without requiring recovery mode context (cycles, premium history).
# Used by buy_write, stock_only, and circuit breaker guards.
# ─────────────────────────────────────────────────────────────────────────

def compute_forward_income_economics(
    row: "pd.Series",
    spot: float,
    effective_cost: float,
) -> dict:
    """Compute forward income economics for hard-stop override decisions.

    Returns a dict with:
        viable         – enough data + positive net income to compute
        monthly_income – estimated monthly premium per share (IV-implied)
        margin_monthly – monthly margin cost per share
        net_monthly    – monthly_income − margin_monthly
        gap_to_breakeven – effective_cost − spot
        months_to_breakeven – gap / net_monthly (inf if not viable)
        iv_now         – current IV (decimal)
        iv_rank        – current IV rank (0-100)
    """
    result = {
        "viable": False,
        "monthly_income": 0.0,
        "margin_monthly": 0.0,
        "net_monthly": 0.0,
        "gap_to_breakeven": 0.0,
        "months_to_breakeven": float("inf"),
        "iv_now": 0.0,
        "iv_rank": 0.0,
    }

    if effective_cost <= 0 or spot <= 0:
        return result

    # IV data
    iv_now = safe_row_float(row, "IV_Now", "IV_30D", default=0.0)
    if iv_now > 5:          # stored as percentage (e.g. 109.2)
        iv_now /= 100.0
    iv_rank = safe_row_float(row, "IV_Rank", "IV_Rank_30D", "IV_Percentile", default=50.0)
    result["iv_now"] = iv_now
    result["iv_rank"] = iv_rank

    from core.management.cycle3.doctrine.thresholds import FORWARD_ECON_IV_MIN_VIABLE
    if iv_now < FORWARD_ECON_IV_MIN_VIABLE:
        return result   # IV too low for meaningful premium

    # IV-implied fresh-cycle monthly income (same formula as recovery state)
    # 0.4 × IV × Spot / √52  →  weekly premium;  ×4.3  →  monthly
    weekly_premium_est = 0.4 * iv_now * spot / (52 ** 0.5)
    iv_implied_monthly = weekly_premium_est * 4.3

    # Also check current-call income (may be richer mid-cycle)
    from core.shared.finance_utils import monthly_income as _monthly_income
    dte = safe_row_float(row, "Short_Call_DTE", "DTE", default=30.0)
    last_premium = safe_row_float(row, "Premium_Entry", "Last", default=0.0)
    current_monthly = _monthly_income(last_premium, dte)

    monthly_income = max(current_monthly, iv_implied_monthly)

    # Margin drag
    margin_daily = safe_row_float(row, "Daily_Margin_Cost", "Margin_Cost_Daily", default=0.0)
    margin_monthly = margin_daily * 30
    net_monthly = monthly_income - margin_monthly if margin_monthly > 0 else monthly_income

    gap_to_breakeven = effective_cost - spot
    months_to_breakeven = (
        gap_to_breakeven / net_monthly
        if net_monthly > 0 and gap_to_breakeven > 0
        else float("inf")
    )

    result.update({
        "viable": net_monthly > 0,
        "monthly_income": monthly_income,
        "margin_monthly": margin_monthly,
        "net_monthly": net_monthly,
        "gap_to_breakeven": gap_to_breakeven,
        "months_to_breakeven": months_to_breakeven,
    })
    return result
