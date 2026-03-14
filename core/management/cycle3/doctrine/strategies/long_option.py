"""
Long option doctrine — LONG_CALL, LONG_PUT, BUY_CALL, BUY_PUT, LEAPS_CALL, LEAPS_PUT.

Extracted from DoctrineAuthority._long_option_doctrine (engine.py lines 3533-6029).
"""

import logging
from typing import Dict

import pandas as pd

from core.management.cycle1.identity.constants import FIDELITY_MARGIN_RATE_DAILY
from core.management.cycle3.doctrine.gate_result import (
    STATE_ACTIONABLE,
    STATE_NEUTRAL_CONFIDENT,
)
from core.management.cycle3.doctrine.helpers import (
    safe_pnl_pct,
    safe_row_float,
    check_thesis_degradation,
)
from core.management.cycle3.doctrine.comparators.action_ev_long import (
    compare_actions_long_option,
)

logger = logging.getLogger(__name__)


def long_option_doctrine(row: pd.Series, result: Dict) -> Dict:
    price_state = str(row.get('PriceStructure_State', 'UNKNOWN') or 'UNKNOWN').upper()
    price_drift = safe_row_float(row, 'Price_Drift_Pct')
    delta_entry = safe_row_float(row, 'Delta_Entry')
    delta_now = abs(safe_row_float(row, 'Delta'))
    dte = row.get('DTE', 999)
    dte = dte if dte is not None and not (isinstance(dte, float) and pd.isna(dte)) else 999
    strategy = str(row.get('Strategy', '') or '').upper()
    is_leap = 'LEAP' in strategy or dte >= 180
    ticker_net_delta = safe_row_float(row, '_Ticker_Net_Delta')
    ticker_has_stock = bool(row.get('_Ticker_Has_Stock', False))

    # Pyramid tier + winner lifecycle defaults (always present in result)
    result["Pyramid_Tier"] = int(row.get('Pyramid_Tier', 0) or 0)
    result["Winner_Lifecycle"] = str(row.get('Winner_Lifecycle', 'THESIS_UNPROVEN') or 'THESIS_UNPROVEN')

    # ── Calendar gates (evaluated first — expiration overrides all other holds) ────
    # Natenberg Ch.15: at ≤3 DTE, gamma → ∞ near strike. McMillan Ch.7: pin risk.
    # Hull Ch.18: theta is non-linear in the final week.
    try:
        from scan_engine.calendar_context import expiry_proximity_flag, get_calendar_context
        _ul_last_cal  = safe_row_float(row, 'UL Last')
        _strike_cal   = safe_row_float(row, 'Strike')
        _exp_flag, _exp_note = expiry_proximity_flag(
            dte=dte,
            strategy=strategy,
            ul_last=_ul_last_cal,
            strike=_strike_cal,
        )
        if _exp_flag == 'PIN_RISK':
            result.update({
                "Action": "EXIT",
                "Urgency": "CRITICAL",
                "Exit_Trigger_Type": "GAMMA",
                "Rationale": _exp_note,
                "Doctrine_Source": "McMillan Ch.7 + Natenberg Ch.15: Pin Risk",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result
        elif _exp_flag == 'GAMMA_CRITICAL':
            result.update({
                "Action": "ROLL",
                "Urgency": "HIGH",
                "Rationale": _exp_note,
                "Doctrine_Source": "Natenberg Ch.15: Gamma Critical — Force Decision",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result
        elif _exp_flag == 'PRE_HOLIDAY_EXPIRY':
            # Upgrade urgency on the way out of this function — stored for later
            _pre_holiday_expiry_escalate = True
        else:
            _pre_holiday_expiry_escalate = False

        # Pre-holiday entry check: if today is pre-long-weekend and DTE is short,
        # escalate any HOLD → note the calendar risk for long premium
        _cal_ctx = get_calendar_context()
        _cal_bleed_note = ""
        if _cal_ctx.is_pre_long_weekend and not is_leap and dte <= 21:
            _cal_bleed_note = (
                f" ⚠️ Calendar: {_cal_ctx.theta_bleed_days} non-trading days ahead — "
                f"long premium bleeds theta with no stock movement. "
                f"Passarelli Ch.6: pre-holiday hold cost is {_cal_ctx.theta_bleed_days}× daily theta."
            )
    except Exception as _cal_err:
        logger.debug(f"Calendar context skipped for long option: {_cal_err}")
        _pre_holiday_expiry_escalate = False
        _cal_bleed_note = ""

    # ── Scale_Trigger_Price re-check (persisted from prior SCALE_UP run) ────────────
    # When a prior run emitted SCALE_UP with a pullback trigger level,
    # that level is stored in Scale_Trigger_Price per-TradeID.
    # If UL Last has now touched or crossed through the trigger → upgrade to
    # SCALE_UP with Urgency=HIGH ("pullback arrived — act now").
    # McMillan Ch.4: "The pullback to support IS the add signal — don't wait for confirmation."
    _prior_trigger = row.get('Scale_Trigger_Price')
    _prior_add_c   = row.get('Scale_Add_Contracts')
    _ul_now_sc     = safe_row_float(row, 'UL Last')
    if (
        pd.notna(_prior_trigger)
        and float(_prior_trigger or 0) > 0
        and _ul_now_sc > 0
    ):
        _prior_trigger_f = float(_prior_trigger)
        _prior_add_c_i   = int(_prior_add_c or 1)
        # For long calls (bullish): pullback = price drops DOWN to trigger
        # For long puts (bearish): pullback = price rallies UP to trigger
        _strat_sc = str(row.get('Strategy', '') or '').upper()
        _is_long_put_sc  = any(s in _strat_sc for s in ('LONG_PUT', 'BUY_PUT', 'LEAPS_PUT'))
        _is_long_call_sc = any(s in _strat_sc for s in ('LONG_CALL', 'BUY_CALL', 'LEAPS_CALL'))
        _trigger_touched = (
            (_is_long_call_sc and _ul_now_sc <= _prior_trigger_f * 1.005) or  # within 0.5% below trigger
            (_is_long_put_sc  and _ul_now_sc >= _prior_trigger_f * 0.995) or  # within 0.5% above trigger
            (not _is_long_call_sc and not _is_long_put_sc and abs(_ul_now_sc - _prior_trigger_f) / _prior_trigger_f <= 0.005)
        )
        if _trigger_touched:
            result.update({
                "Action": "SCALE_UP",
                "Urgency": "HIGH",
                "Scale_Trigger_Price": round(_prior_trigger_f, 2),
                "Scale_Add_Contracts": _prior_add_c_i,
                "Rationale": (
                    f"⬆️🎯 Scale trigger reached: UL=${_ul_now_sc:.2f} touched pullback level "
                    f"${_prior_trigger_f:.2f}. "
                    f"Add {_prior_add_c_i} contract(s) now — pullback-to-support entry "
                    f"confirmed (McMillan Ch.4: Pyramid on Strength, act on the pullback)."
                ),
                "Doctrine_Source": "McMillan Ch.4: Scale Trigger Activated",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True,
            })
            return result

    # 2a. Portfolio delta redundancy: long call on ticker already long from stock
    # McMillan Ch.4: redundant delta exposure from options on a stock-heavy position
    # LEAPs (DTE >= 180) require a much larger loss to trigger trim — they have time to recover.
    #
    # Action escalation by quantity:
    #   qty > 1 → TRIM (close half, reduce overlap, keep some exposure)
    #   qty == 1 → EXIT (nothing left to trim to; close the whole position)
    # This prevents an infinite TRIM loop where the user trimmed to 1 contract
    # and the gate fires again recommending "trim" a single contract in half.
    _pnl_quick = safe_pnl_pct(row) or 0.0
    _delta_redundancy_threshold = -0.35 if is_leap else -0.15
    _qty_for_redundancy = abs(safe_row_float(row, 'Quantity', default=1.0))
    if ticker_has_stock and ticker_net_delta > 0.8 and _pnl_quick < _delta_redundancy_threshold:
        if _qty_for_redundancy <= 1:
            # Single contract — TRIM would leave 0 contracts (meaningless half-close).
            # Route to EXIT: close the full redundant position.
            result.update({
                "Action": "EXIT",
                "Urgency": "LOW",
                "Rationale": (
                    f"Ticker net delta={ticker_net_delta:.2f} already long from stock. "
                    f"Long call adds redundant directional exposure at a loss — close this position. "
                    f"Single contract: no further trimming possible; EXIT to remove overlap "
                    f"(McMillan Ch.4: Portfolio Delta Management)."
                ),
                "Doctrine_Source": "McMillan Ch.4: Portfolio Delta Management",
                "Decision_State": STATE_NEUTRAL_CONFIDENT,
                "Required_Conditions_Met": True
            })
        else:
            result.update({
                "Action": "TRIM",
                "Urgency": "LOW",
                "Rationale": (
                    f"Ticker net delta={ticker_net_delta:.2f} already long from stock. "
                    f"Long call adds redundant directional exposure at a loss — consider trimming "
                    f"(McMillan Ch.4: Portfolio Delta Management)."
                ),
                "Doctrine_Source": "McMillan Ch.4: Portfolio Delta Management",
                "Decision_State": STATE_NEUTRAL_CONFIDENT,
                "Required_Conditions_Met": True
            })
        return result

    # ── Recently-Rolled Cooldown gate (Signal Coherence Gate 1) ─────────
    # Directional strategies (LONG_CALL/LONG_PUT/LEAPS_CALL): 1 trading-day cooldown.
    # Faster-moving signals justify a shorter window than income strategies.
    # Suppresses same-day re-roll noise (Natenberg Ch.7, Jabbour Ch.8).
    _COOLDOWN_DAYS_LO = 1
    _days_since_roll_lo = row.get('Days_Since_Last_Roll')
    _thesis_for_cooldown_lo = str(row.get('Thesis_State', '') or '').upper()
    if (
        pd.notna(_days_since_roll_lo)
        and float(_days_since_roll_lo) < _COOLDOWN_DAYS_LO
        and _thesis_for_cooldown_lo in ('INTACT', 'UNKNOWN', '')
    ):
        result.update({
            "Action": "HOLD",
            "Urgency": "LOW",
            "Rationale": (
                f"Recently-rolled cooldown: current leg opened {int(_days_since_roll_lo)}d ago "
                f"(< {_COOLDOWN_DAYS_LO}d window). Thesis is {_thesis_for_cooldown_lo or 'UNKNOWN'} — "
                f"suppressing discretionary ROLL to prevent same-day flip-flop. "
                f"Natenberg Ch.7: 'Frequent adjustments cost more than the risk they mitigate.' "
                f"Jabbour Ch.8: 'Repair is a dangerous misnomer for overtrading.'"
            ),
            "Doctrine_Source": "Natenberg Ch.7 + Jabbour Ch.8: Recently-Rolled Cooldown",
            "Decision_State": STATE_NEUTRAL_CONFIDENT,
            "Required_Conditions_Met": True,
        })
        return result

    # 1. Thesis invalidation: structure broken + price moved against position
    # McMillan Ch.4: cut long options when directional thesis is structurally broken
    # LEAP modulation: LEAPs bought time specifically for drawdowns. Structure can
    # repair over 300+ days. CRITICAL only for short-dated where theta compounds
    # the structural damage. LEAPs get HIGH (plan exit) not CRITICAL (act today).
    if "STRUCTURE_BROKEN" in price_state and price_drift < -0.05:
        if is_leap:
            _struct_urgency = "HIGH"
            _struct_rationale = (
                f"Technical structure broken ({price_state}) with {price_drift:.1%} adverse drift. "
                f"BUT DTE={dte:.0f} — LEAP has time for structure repair. "
                f"Theta bleed {_theta_bleed:.1%}/day is manageable. "
                f"Revalidate thesis: if conviction lost, exit on next bounce. "
                f"If thesis intact, structure may rebuild (McMillan Ch.4: LEAPs tolerate "
                f"structural breaks that short-dated options cannot)."
            )
        else:
            _struct_urgency = "CRITICAL"
            _struct_rationale = (
                f"Technical structure broken ({price_state}) with {price_drift:.1%} adverse drift. "
                f"Directional thesis invalidated (McMillan Ch.4)."
            )
        result.update({
            "Action": "EXIT",
            "Urgency": _struct_urgency,
            "Exit_Trigger_Type": "CAPITAL",
            "Rationale": _struct_rationale,
            "Doctrine_Source": "McMillan Ch.4: Structural Exit",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True
        })
        return result

    # 2. Delta / sensitivity collapse (Passarelli Ch.2: Greek Drift)
    # Option has moved deep OTM — paying full theta for a coin flip
    if delta_entry > 0:
        sensitivity_ratio = delta_now / delta_entry
        if sensitivity_ratio < 0.40:
            result.update({
                "Action": "EXIT",
                "Urgency": "HIGH",
                "Rationale": f"Delta collapsed to {sensitivity_ratio:.0%} of entry ({delta_entry:.2f}→{delta_now:.2f}). Position now lottery-ticket risk (Passarelli Ch.2).",
                "Doctrine_Source": "Passarelli Ch.2: Greek Drift",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result

    # Read pnl_pct early — needed for triggers 2b, 2c, 2d, and 3
    pnl_pct = safe_pnl_pct(row) or 0.0

    # Compute option-level gain % from price: (Last - |Premium_Entry|) / |Premium_Entry|
    # This is the correct denominator for profit targets on long options.
    # Total_GL_Decimal uses cost-basis which includes broker charges and may be diluted
    # by multi-leg trades — option price gain is the canonical measure (McMillan Ch.4).
    _last_raw    = safe_row_float(row, 'Last')
    _bid_raw     = safe_row_float(row, 'Bid')
    # Use max(Last, Bid) as the realizable price floor.  For deeply ITM options,
    # Last can be a stale EOD trade price BELOW intrinsic (arbitrage floor), which
    # makes _time_val = 0 and triggers a false TV-exhausted EXIT.  The bid is the
    # actual price the market will pay right now — it cannot be below intrinsic
    # (market makers enforce this).  Using max(Last, Bid) prevents stale-Last
    # from creating phantom 0% TV readings.
    _last_price  = max(_last_raw, _bid_raw) if _bid_raw > 0 else _last_raw
    _entry_price = abs(safe_row_float(row, 'Premium_Entry', default=0.0))
    option_gain_pct = (_last_price - _entry_price) / _entry_price if _entry_price > 0 else 0.0

    # Pyramid tier and winner lifecycle (Cycle 2.96 — Murphy pyramid rules)
    _pyramid_tier = int(row.get('Pyramid_Tier', 0) or 0)
    _winner_lifecycle = str(row.get('Winner_Lifecycle', 'THESIS_UNPROVEN') or 'THESIS_UNPROVEN').upper()

    # Deep-ITM intrinsic metrics — used for winner management gates below
    _ul_last  = safe_row_float(row, 'UL Last', 'Underlying_Price_Entry')
    _strike   = safe_row_float(row, 'Strike')
    _cp        = str(row.get('Call/Put', '') or '').upper()
    _is_put    = 'P' in _cp
    _option_type_label = "long put" if _is_put else "long call"
    _intrinsic = max(0.0, _strike - _ul_last) if _is_put else max(0.0, _ul_last - _strike)
    _time_val  = max(0.0, _last_price - _intrinsic)
    _tv_pct    = (_time_val / _last_price) if _last_price > 0 else 0.0  # time val as % of option price

    # ── Direction-awareness helper ─────────────────────────────────────
    # Price_Drift_Pct and Drift_Direction are raw stock movement.
    # For a LONG_PUT, stock going UP is thesis-adverse.
    # For a LONG_CALL, stock going DOWN is thesis-adverse.
    # Compute once, use across all gates.
    _drift_raw_da = row.get('Drift_Direction', '') or ''
    drift_dir = (getattr(_drift_raw_da, 'value', None) or str(_drift_raw_da).split('.')[-1]).upper()
    _roc5  = safe_row_float(row, 'roc_5')
    _roc10 = safe_row_float(row, 'roc_10')
    _hv_20d = safe_row_float(row, 'HV_20D') if pd.notna(row.get('HV_20D')) else 0.0

    _adverse_drift_dir = 'UP' if _is_put else 'DOWN'
    # Sigma-normalized adverse detection: compare moves in the stock's OWN
    # volatility distribution rather than fixed percentages.
    # Natenberg Ch.5 / Hull Ch.2: σ-scaling makes gates cross-ticker fair.
    # When HV_20D is unavailable, direction signals are INDETERMINATE —
    # neither adverse nor confirming fires. No silent raw-% fallback.
    from core.management.cycle3.doctrine.helpers import compute_direction_adverse_signals
    _roc5_adverse, _drift_is_adverse, _roc5_z, _drift_z, _used_sigma = (
        compute_direction_adverse_signals(_roc5, price_drift, _hv_20d, _is_put)
    )
    # Persist z-scores for audit trail (queryable in management_recommendations)
    result["ROC5_Z"] = _roc5_z
    result["Drift_Z"] = _drift_z
    result["Sigma_Mode"] = _used_sigma
    # Sigma info for rationale strings — shows z-scores when available
    if _used_sigma:
        _sigma_tag = (f" [σ-mode: roc5_z={_roc5_z:+.1f}, drift_z={_drift_z:+.1f}, "
                      f"HV={_hv_20d:.0%}]")
    else:
        _sigma_tag = " [⚠ HV_20D missing — direction signals indeterminate]"
    _theta_bleed = safe_row_float(row, 'Theta_Bleed_Daily_Pct')

    # Entry quality assessment — determines how much patience the position gets
    # Strong entry (structure + trend confirm direction): more rope
    # Weak entry (RANGE_BOUND/NO_TREND at entry): shorter leash
    # McMillan Ch.4: "Positions entered without clear structural confirmation
    #   deserve less patience — cut sooner."
    _entry_ps = str(row.get('Entry_Chart_State_PriceStructure', '') or '').upper()
    _entry_ti = str(row.get('Entry_Chart_State_TrendIntegrity', '') or '').upper()
    if _is_put:
        _strong_entry = _entry_ps in ('STRUCTURAL_DOWN',) and _entry_ti in ('STRONG_TREND', 'WEAK_TREND')
    else:
        _strong_entry = _entry_ps in ('STRUCTURAL_UP',) and _entry_ti in ('STRONG_TREND', 'WEAK_TREND')
    _weak_entry = _entry_ps in ('RANGE_BOUND', 'NO_TREND', 'UNKNOWN', '') or _entry_ti in ('NO_TREND', 'UNKNOWN', '')

    if _strong_entry:
        _entry_quality = 'STRONG'; _pnl_threshold_da = -0.25
    elif _weak_entry:
        _entry_quality = 'WEAK'; _pnl_threshold_da = -0.10
    else:
        _entry_quality = 'NEUTRAL'; _pnl_threshold_da = -0.15

    # ── Cross-gate prior-EXIT persistence (McMillan Ch.4) ───────────────
    # If the prior day's doctrine said EXIT (from any gate) and no hard-stop
    # or structural gate fires today, check if conditions materially improved.
    # If not, the prior EXIT persists — one good day doesn't erase a real signal.
    # Placed AFTER hard stops (pin risk, gamma, delta collapse, hard stop loss)
    # but BEFORE soft gates (theta, direction, winner mgmt) so it catches
    # cases where the original exit gate barely doesn't fire on a mildly better day.
    from core.management.cycle3.doctrine.helpers import check_prior_exit_persistence
    _macro_days_v1 = safe_row_float(row, 'Days_To_Macro', default=999.0) if pd.notna(row.get('Days_To_Macro')) else None
    _macro_type_v1 = str(row.get('Macro_Next_Type', '') or '') or None
    _persist_exit, _persist_reason, _macro_catalyst_v1 = check_prior_exit_persistence(
        row, _is_put,
        macro_days=_macro_days_v1,
        macro_type=_macro_type_v1,
    )
    if _macro_catalyst_v1:
        result["Macro_Catalyst_Protected"] = True
    if _persist_exit:
        result.update({
            "Action": "EXIT",
            "Urgency": "MEDIUM",
            "Rationale": (
                f"{_persist_reason}{_sigma_tag}"
            ),
            "Doctrine_Source": "McMillan Ch.4: Prior EXIT Persistence",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True,
        })
        return result

    # C4 audit fix: Deep-ITM / Time-value-exhausted exit gate.
    # When time value < 10% of option price AND option is winning (positive P&L):
    # The option has become essentially pure intrinsic — you're holding a synthetic stock
    # position paying theta for near-zero extrinsic benefit. Converting to stock or
    # closing to capture intrinsic is more capital-efficient.
    #
    # Condition requires POSITIVE P&L (option is a winner already) because:
    # - On a winner, tv_pct < 10% means you've captured the move and intrinsic is working.
    #   Holding further bleeds theta for minimal additional premium — better to close.
    # - On a loser with tv_pct < 10%, this is deep OTM (opposite: mostly time value lost),
    #   which is already caught by the delta-collapse gate above.
    # LEAP exemption: LEAPs by design have many DTE remaining; tv_pct < 10% at a large
    # intrinsic gain means the position is deeply ITM and working — no urgency to exit.
    #
    # Natenberg Ch.7: "A deeply ITM long option becomes a synthetic stock — the theta
    # cost is now a pure carrying cost. Converting is almost always more efficient."
    # Cohen Ch.3: "When time value < 10% and the position is profitable, exiting
    # captures the gain cleanly — the remaining extrinsic adds little to the total return."
    _c4_pnl = safe_pnl_pct(row) or 0.0
    if (
        not is_leap
        and _last_price > 0
        and _tv_pct < 0.10        # less than 10% of option price is time value
        and _intrinsic > 0        # option IS in the money (has real intrinsic)
        and _c4_pnl > 0.05        # position is profitable (≥5% gain) — this is a winner, not a loss
    ):
        result.update({
            "Action": "EXIT",
            "Urgency": "MEDIUM",
            "Rationale": (
                f"⏰ Time value exhausted: ${_time_val:.2f} ({_tv_pct:.0%} of ${_last_price:.2f}) — "
                f"only {_tv_pct:.0%} extrinsic remaining. "
                f"Option is {1-_tv_pct:.0%} intrinsic (${_intrinsic:.2f}/share). "
                f"Holding further pays theta with no meaningful additional premium. "
                f"Close to capture the intrinsic gain cleanly; "
                f"if still bullish, convert to stock position or re-enter next cycle at current delta. "
                f"(Natenberg Ch.7: deeply ITM long = synthetic stock carrying theta cost; "
                f"Cohen Ch.3: exit when tv_pct < 10% on a winning position)"
            ),
            "Doctrine_Source": "Natenberg Ch.7 + Cohen Ch.3: Time Value Exhausted Exit (C4)",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True,
        })
        return result

    # 2b. Thesis staleness: consumed fraction of option life, no price movement, significant loss
    # McMillan Ch.4: "time-to-be-right" — cut if thesis not confirming
    # Entry quality adjusts patience: STRONG=1/2, NEUTRAL=1/3, WEAK=1/4 of option life
    days_held = safe_row_float(row, 'Days_In_Trade')
    original_dte = dte + days_held  # approximate original DTE at entry
    _time_fraction = 2 if _entry_quality == 'STRONG' else (4 if _entry_quality == 'WEAK' else 3)
    # Staleness: drift is "small" if below z=3.0σ (sigma mode).
    # When HV missing: indeterminate — treat as NOT flat (don't gate on incomplete data).
    from core.management.cycle3.doctrine.thresholds import SIGMA_DRIFT_STALENESS_Z
    _drift_is_flat = (abs(_drift_z) < SIGMA_DRIFT_STALENESS_Z if _used_sigma
                      else False)
    if (original_dte > 0
            and days_held >= original_dte / _time_fraction
            and _drift_is_flat
            and pnl_pct < -0.30):
        result.update({
            "Action": "ROLL",
            "Urgency": "MEDIUM",
            "Rationale": (
                f"Thesis not confirming: {days_held:.0f}d held ({days_held/original_dte:.0%} of life), "
                f"drift only {price_drift:+.1%}, P&L={pnl_pct:.0%}. "
                f"Roll to later expiry or exit if conviction lost (McMillan Ch.4: Time-to-be-Right)."
            ),
            "Doctrine_Source": "McMillan Ch.4: Time-to-be-Right",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True
        })
        return result

    # 2a-trend. Entry-Trend Invalidation Gate
    # ─────────────────────────────────────────────────────────────────────────────
    # A directional long option's thesis is founded on the trend at entry.
    # If the entry had STRONG_TREND/WEAK_TREND and NOW trend is NO_TREND or
    # reversed, the thesis is structurally invalid — not a "wait and see."
    # EXIT HIGH immediately. Do NOT oscillate between HOLD/ROLL/EXIT.
    # (Audit: AMZN/MSFT/META Feb-2026 — trend collapsed Day 0, oscillated 10d, -55%)
    # Natenberg Ch.5: position no longer sensible under new conditions.
    # ─────────────────────────────────────────────────────────────────────────────
    _current_ti = str(row.get('TrendIntegrity_State', '') or '').upper()
    if (not is_leap
            and _entry_ti in ('STRONG_TREND', 'WEAK_TREND')
            and _current_ti in ('NO_TREND', 'TREND_EXHAUSTED', '')
            and pnl_pct < 0):
        # Trend that justified entry has collapsed
        result.update({
            "Action": "EXIT",
            "Urgency": "HIGH",
            "Rationale": (
                f"TREND_INVALIDATED — entry trend was {_entry_ti}, now {_current_ti or 'UNKNOWN'}. "
                f"Directional thesis is structurally broken (not degraded). "
                f"P&L={pnl_pct:.0%}, DTE={dte}. "
                f"Entry quality: {_entry_quality} (leash: {_pnl_threshold_da:.0%}). "
                f"(Natenberg Ch.5: position no longer sensible under new conditions.)"
            ),
            "Doctrine_Source": "Natenberg Ch.5 + Audit Feb-2026",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True
        })
        return result

    # 2b-dir. Direction-Adverse Thesis Confirmation Gate (NEW)
    # ─────────────────────────────────────────────────────────────────────────────
    # For long directional options, the stock moving AGAINST the thesis direction
    # is the single most damaging scenario — theta bleeds AND delta works against
    # the position simultaneously.
    #
    # Condition (ALL must be true):
    #   1. ROC5 confirms adverse direction (up for puts, down for calls)
    #   2. Drift_Direction confirms adverse
    #   3. DTE < 45 (theta acceleration zone)
    #   4. P&L below entry-quality-adjusted threshold
    #   5. Not a LEAP (LEAPs have longer thesis runways)
    #
    # Three pathways:
    #   A. Thesis INTACT + catalyst near + conviction OK → HOLD HIGH (escape)
    #   B. Roll conditions met (thesis intact, confirming signal, IV affordable) → ROLL
    #   C. All else → EXIT (Natenberg: close losing strategy; Jabbour: don't roll a bad trade)
    #
    # Doctrine:
    #   Natenberg Ch.5: "A position that initially seemed sensible may under new
    #     conditions represent a losing strategy."
    #   Jabbour Ch.7: "Rolling fails to recognize the position may be a bad trade."
    #   Given Ch.9: "Directional trades should have a time stop."
    #   Krishnan: "Hope is not an investment strategy."
    # ─────────────────────────────────────────────────────────────────────────────
    # OR not AND: a slow grind (ROC5 flat but drift > 2%) must still trigger.
    # Magnitude thresholds filter noise; P&L threshold prevents false positives on winners.
    _any_adverse_signal = (_roc5_adverse or _drift_is_adverse)
    _dir_adverse_detected = (
        _any_adverse_signal
        and dte < 45
        and pnl_pct < _pnl_threshold_da
    )

    # Hysteresis: if prior run was EXIT from the same gate family,
    # require the signal to clear a wider band before flipping back to HOLD.
    # Prevents EXIT↔HOLD flip-flop on borderline positions.
    # When sigma normalization is active, pass z-score instead of raw ROC5.
    from core.management.cycle3.doctrine.helpers import check_hysteresis
    from core.management.cycle3.doctrine.thresholds import (
        HYSTERESIS_ROC5_EXIT_THRESHOLD, HYSTERESIS_ROC5_CLEAR_THRESHOLD,
        HYSTERESIS_PNL_CLEAR_MARGIN,
        SIGMA_ROC5_Z_ADVERSE, SIGMA_ROC5_Z_CLEAR,
    )
    _prior_doctrine_src = str(row.get('Prior_Doctrine_Source', '') or '')
    if _used_sigma:
        _hyst_signal = _roc5_z
        _hyst_exit = SIGMA_ROC5_Z_ADVERSE if _is_put else -SIGMA_ROC5_Z_ADVERSE
        _hyst_clear = SIGMA_ROC5_Z_CLEAR if _is_put else -SIGMA_ROC5_Z_CLEAR
    else:
        _hyst_signal = _roc5
        _hyst_exit = HYSTERESIS_ROC5_EXIT_THRESHOLD if _is_put else -HYSTERESIS_ROC5_EXIT_THRESHOLD
        _hyst_clear = HYSTERESIS_ROC5_CLEAR_THRESHOLD if _is_put else -HYSTERESIS_ROC5_CLEAR_THRESHOLD
    _hysteresis_exit, _hysteresis_reason = check_hysteresis(
        prior_action=str(row.get('Prior_Action', '') or '').upper(),
        prior_doctrine_source=_prior_doctrine_src,
        gate_family='DIRECTION_ADVERSE',
        current_signal=_hyst_signal,
        exit_threshold=_hyst_exit,
        clear_threshold=_hyst_clear,
        pnl_pct=pnl_pct,  # pass None/NaN through — hysteresis treats missing as "cannot clear"
        pnl_exit_threshold=_pnl_threshold_da,
        pnl_clear_margin=HYSTERESIS_PNL_CLEAR_MARGIN,
    )
    if _hysteresis_exit and not _dir_adverse_detected:
        _dir_adverse_detected = True  # force EXIT path — signal hasn't cleared

    _recovery_raw_da = str(row.get('Recovery_Feasibility', '') or '').upper()
    _already_impossible = _recovery_raw_da == 'IMPOSSIBLE'

    if _dir_adverse_detected and not _already_impossible and not is_leap:
        # New-position grace: scan engine just recommended this trade — don't
        # override with trailing price action the scan already evaluated.
        # McMillan Ch.4: "Give a new position time to develop its thesis."
        # Floor: -40%+ loss on day 0 = catastrophic event, no grace for that.
        if days_held < 2 and (pnl_pct is None or pnl_pct > -0.40):
            result.update({
                "Action": "HOLD",
                "Urgency": "LOW",
                "Rationale": (
                    f"Direction ADVERSE for {_option_type_label}: "
                    f"stock {'rallying' if _is_put else 'falling'} (ROC5={_roc5:+.1f}%, "
                    f"Drift={drift_dir}, Price_Drift={price_drift:+.1%}), P&L={pnl_pct:.0%}. "
                    f"However: position opened {days_held:.0f}d ago — scan engine evaluated this "
                    f"price action and recommended entry. Grace period: reassess after day 2. "
                    f"Entry quality: {_entry_quality} (leash: {_pnl_threshold_da:.0%}). "
                    f"(McMillan Ch.4: new positions need time to develop thesis.)"
                ),
                "Doctrine_Source": "McMillan Ch.4: Direction Adverse — New Position Grace",
                "Decision_State": STATE_NEUTRAL_CONFIDENT,
                "Required_Conditions_Met": True,
            })
            return result

        _thesis_state_da = str(row.get('Thesis_State', '') or '').upper()
        _conv_status_da  = str(row.get('Conviction_Status', '') or '').upper()

        # Catalyst check: earnings within 14 days OR HIGH-impact macro event
        # within 5 days (CPI, FOMC, NFP) as a potential reversal trigger.
        # Natenberg Ch.5: "Forward probability shifts materially around known events."
        _has_catalyst = False
        _catalyst_label = ""
        try:
            _earn_date = row.get('Earnings_Date')
            _snap_ts = row.get('Snapshot_TS')
            if _earn_date and _snap_ts:
                _earn_dt = pd.to_datetime(_earn_date)
                _snap_dt = pd.to_datetime(_snap_ts)
                _days_to_earn = (_earn_dt.normalize() - _snap_dt.normalize()).days
                if 0 < _days_to_earn <= 14:
                    _has_catalyst = True
                    _catalyst_label = f"earnings in {_days_to_earn}d"
        except Exception as _earn_err:
            logger.debug(f"Earnings catalyst check skipped: {_earn_err}")

        # Macro catalyst: HIGH-impact event within 5 days
        if not _has_catalyst:
            try:
                _macro_days = safe_row_float(row, 'Days_To_Macro', default=99.0)
                _macro_impact = str(row.get('Macro_Impact', '') or '').upper()
                _macro_type = str(row.get('Macro_Next_Type', '') or '')
                if _macro_impact == 'HIGH' and 0 <= _macro_days <= 5:
                    _has_catalyst = True
                    _catalyst_label = f"{_macro_type} in {_macro_days:.0f}d"
            except Exception:
                pass

        _escape = (
            _thesis_state_da == 'INTACT'
            and _conv_status_da in ('STABLE', 'STRENGTHENING')
            and _has_catalyst
        )

        if _escape:
            result.update({
                "Action": "HOLD",
                "Urgency": "HIGH",
                "Rationale": (
                    f"Direction ADVERSE for {_option_type_label}: "
                    f"stock {'rallying' if _is_put else 'falling'} (ROC5={_roc5:+.1f}%, "
                    f"Drift={drift_dir}, Price_Drift={price_drift:+.1%}). "
                    f"However: Thesis={_thesis_state_da}, Conviction={_conv_status_da}, "
                    f"{_catalyst_label} — potential reversal catalyst. "
                    f"HOLD with tight review — exit if catalyst fails to reverse direction. "
                    f"Entry quality: {_entry_quality} (leash: {_pnl_threshold_da:.0%}). "
                    f"(Given Ch.9: time stop with catalyst exception; "
                    f"Natenberg Ch.5: forward probability shifts with catalyst.)"
                ),
                "Doctrine_Source": "Given Ch.9 + Natenberg Ch.5: Direction Adverse — Catalyst Hold",
                "Decision_State": STATE_NEUTRAL_CONFIDENT,
                "Required_Conditions_Met": True,
            })
            return result
        else:
            # ── Sector Relative Strength modulation ───────────────────
            # Absolute direction is adverse, but is the stock genuinely
            # outperforming its sector?  If the stock is underperforming,
            # the directional thesis may still be valid on a relative basis.
            # Sinclair (0.753): "Exit when you are wrong, not when you are losing."
            # Natenberg: "Risk management must consider context of underlying movement."
            # SRS provides that context — sector movement is not thesis failure.
            _srs_raw = str(row.get('Sector_Relative_Strength', '') or '').upper()
            _srs_z   = safe_row_float(row, 'Sector_RS_ZScore') if pd.notna(row.get('Sector_RS_ZScore')) else 0.0
            _srs_bench = str(row.get('Sector_Benchmark', 'SPY') or 'SPY')

            # SRS override conditions: absolute damage too severe for relative to save
            # Jabbour (0.712): rolling a bad trade is denial
            # AUDIT FIX: "already rolled" must check ACTUAL contract change, not
            # Prior_Action (which is the engine's recommendation, not user execution).
            # Evidence of actual roll: Expiration differs from Expiration_Entry.
            _prior_action_da = str(row.get('Prior_Action', '') or '').upper()
            _exp_current = str(row.get('Expiration', '') or '')
            _exp_entry   = str(row.get('Expiration_Entry', '') or '')
            _already_rolled = (
                _exp_current != _exp_entry
                and _exp_entry != ''
                and _exp_current != ''
            )
            _srs_override = (
                pnl_pct < -0.40          # absolute damage too severe
                or _already_rolled        # Jabbour: already rolled once, cut it
                or dte < 10               # no time for relative thesis to play out
            )

            # SRS modulation matrix:
            #   OUTPERFORMING  → thesis broken, EXIT (stock leading sector)
            #   NEUTRAL        → no relative edge, proceed with roll/exit logic
            #   UNDERPERFORMING → relative thesis intact, ROLL if eligible
            #   MICRO_BREAKDOWN/BROKEN → strong relative signal, HOLD HIGH
            _srs_favorable = _srs_raw in ('UNDERPERFORMING', 'MICRO_BREAKDOWN', 'BROKEN')

            if _srs_favorable and not _srs_override:
                # Stock is lagging its sector — directional thesis may be intact
                if _srs_raw in ('MICRO_BREAKDOWN', 'BROKEN'):
                    # Strong relative signal: stock is deeply weak vs sector
                    result.update({
                        "Action": "HOLD",
                        "Urgency": "HIGH",
                        "Rationale": (
                            f"Direction ADVERSE for {_option_type_label}: "
                            f"stock {'rallying' if _is_put else 'falling'} (ROC5={_roc5:+.1f}%, "
                            f"Price_Drift={price_drift:+.1%}), P&L={pnl_pct:.0%}. "
                            f"BUT sector-relative thesis REINFORCED: "
                            f"SRS={_srs_raw} (z={_srs_z:+.1f}) vs {_srs_bench}. "
                            f"Stock significantly underperforming sector — adverse move is "
                            f"market-driven, not thesis failure. "
                            f"Entry quality: {_entry_quality}. "
                            f"(Sinclair: exit when wrong, not when losing; "
                            f"Natenberg: sector context modulates risk assessment.)"
                        ),
                        "Doctrine_Source": "Sinclair + Natenberg: Direction Adverse — SRS Thesis Intact",
                        "Decision_State": STATE_NEUTRAL_CONFIDENT,
                        "Required_Conditions_Met": True,
                    })
                    return result
                else:
                    # UNDERPERFORMING: relative thesis intact → downgrade to ROLL
                    # Bennett (0.721): roll to extend runway when thesis intact
                    _iv_pctile_da = safe_row_float(row, 'IV_Percentile', default=50.0) if pd.notna(row.get('IV_Percentile')) else 50.0
                    _iv_depth_da = int(safe_row_float(row, 'IV_Percentile_Depth')) if pd.notna(row.get('IV_Percentile_Depth')) else 0
                    _iv_pctile_reliable = _iv_depth_da >= 45
                    # Jabbour Ch.4: roll on a losing long is new capital.
                    # When depth unreliable, fall back to IV-HV gap (Natenberg Ch.3).
                    _iv_gap_srs = row.get('IV_vs_HV_Gap')
                    _iv_gap_srs_valid = pd.notna(_iv_gap_srs)
                    _iv_gap_srs = float(_iv_gap_srs) if _iv_gap_srs_valid else 0.0
                    if _iv_pctile_reliable:
                        _iv_srs_affordable = _iv_pctile_da <= 50
                    else:
                        _iv_srs_affordable = (not _iv_gap_srs_valid) or (_iv_gap_srs <= 0)
                    _roll_affordable = _iv_srs_affordable and dte <= 30
                    if _roll_affordable:
                        result.update({
                            "Action": "ROLL",
                            "Urgency": "MEDIUM",
                            "Rationale": (
                                f"Direction ADVERSE for {_option_type_label}: "
                                f"stock {'rallying' if _is_put else 'falling'} (ROC5={_roc5:+.1f}%, "
                                f"Price_Drift={price_drift:+.1%}), P&L={pnl_pct:.0%}. "
                                f"Sector-relative thesis intact: SRS={_srs_raw} "
                                f"(z={_srs_z:+.1f}) vs {_srs_bench}. "
                                f"Stock underperforming sector — relative weakness buys time, not immunity. "
                                f"Roll to 60+ DTE. {f'IV gap={_iv_gap_srs:+.1f}% (vol edge, {_iv_depth_da}d history)' if not _iv_pctile_reliable and _iv_gap_srs_valid else f'IV_Pctile={_iv_pctile_da:.0f}% (affordable)'}. "
                                f"Entry quality: {_entry_quality}. "
                                f"(Bennett: extend thesis runway when thesis intact; "
                                f"Sinclair: exit when wrong — relative weakness says not yet wrong.)"
                            ),
                            "Doctrine_Source": "Bennett + Sinclair: Direction Adverse — SRS Roll-for-Time",
                            "Decision_State": STATE_ACTIONABLE,
                            "Required_Conditions_Met": True,
                        })
                        return result
                    else:
                        result.update({
                            "Action": "HOLD",
                            "Urgency": "HIGH",
                            "Rationale": (
                                f"Direction ADVERSE for {_option_type_label}: "
                                f"stock {'rallying' if _is_put else 'falling'} (ROC5={_roc5:+.1f}%, "
                                f"Price_Drift={price_drift:+.1%}), P&L={pnl_pct:.0%}. "
                                f"Sector-relative thesis intact: SRS={_srs_raw} "
                                f"(z={_srs_z:+.1f}) vs {_srs_bench}. "
                                f"Roll not affordable ({f'IV_Pctile={_iv_pctile_da:.0f}%' if _iv_pctile_reliable else f'IV gap={_iv_gap_srs:+.1f}% ({_iv_depth_da}d history — no vol edge)'}{', DTE>' + str(int(dte)) if dte > 30 else ''}). "
                                f"HOLD with tight review — exit if SRS flips to NEUTRAL+. "
                                f"Entry quality: {_entry_quality}. "
                                f"(Sinclair: relative weakness is not thesis failure.)"
                            ),
                            "Doctrine_Source": "Sinclair: Direction Adverse — SRS HOLD (roll unavailable)",
                            "Decision_State": STATE_NEUTRAL_CONFIDENT,
                            "Required_Conditions_Met": True,
                        })
                        return result

            # ── SRS = OUTPERFORMING or NEUTRAL: proceed with roll/exit ──
            # Stock is leading or matching sector → thesis is genuinely failing
            _mom_slope_da = safe_row_float(row, 'momentum_slope')
            _confirming_signal = (_mom_slope_da < 0 if _is_put else _mom_slope_da > 0)
            _ev_ratio_da = safe_row_float(row, 'EV_Feasibility_Ratio') if pd.notna(row.get('EV_Feasibility_Ratio')) else float('nan')
            _iv_pctile_da = safe_row_float(row, 'IV_Percentile', default=50.0) if pd.notna(row.get('IV_Percentile')) else 50.0
            _iv_depth_da = int(safe_row_float(row, 'IV_Percentile_Depth')) if pd.notna(row.get('IV_Percentile_Depth')) else 0
            _iv_pctile_reliable = _iv_depth_da >= 45
            if not _srs_override:
                pass  # _already_rolled already set from contract evidence above

            # Jabbour Ch.4 + Natenberg Ch.3: Roll on a losing long is new capital.
            # When IV_Percentile is reliable (depth >= 45): use percentile <= 50.
            # When unreliable: fall back to IV-HV gap. Long options BUY vol —
            # require gap <= 0 (IV at or below HV = vol edge for buyer).
            # If BOTH percentile AND gap are unavailable, fail-open (don't double-penalize).
            _iv_gap_for_roll = row.get('IV_vs_HV_Gap')
            _iv_gap_valid = pd.notna(_iv_gap_for_roll)
            _iv_gap_for_roll = float(_iv_gap_for_roll) if _iv_gap_valid else 0.0
            if _iv_pctile_reliable:
                _iv_roll_affordable = _iv_pctile_da <= 50
            else:
                _iv_roll_affordable = (not _iv_gap_valid) or (_iv_gap_for_roll <= 0)

            _roll_conditions = (
                _thesis_state_da == 'INTACT'
                and _confirming_signal
                and (not pd.isna(_ev_ratio_da) and _ev_ratio_da < 1.0)
                and _iv_roll_affordable
                and dte <= 30
                and not _already_rolled
            )

            _srs_note = f" SRS={_srs_raw} (z={_srs_z:+.1f}) vs {_srs_bench} — no relative edge." if _srs_raw else ""

            if _roll_conditions:
                result.update({
                    "Action": "ROLL",
                    "Urgency": "MEDIUM",
                    "Rationale": (
                        f"Direction ADVERSE for {_option_type_label}: "
                        f"stock {'rallying' if _is_put else 'falling'} (ROC5={_roc5:+.1f}%, "
                        f"Drift={drift_dir}, Price_Drift={price_drift:+.1%}), P&L={pnl_pct:.0%}. "
                        f"Thesis INTACT with confirming signal (slope={_mom_slope_da:+.3f}) "
                        f"and {f'IV gap={_iv_gap_for_roll:+.1f}% (vol edge for buyer, {_iv_depth_da}d history)' if not _iv_pctile_reliable and _iv_gap_valid else f'IV_Percentile={_iv_pctile_da:.0f}% (affordable roll)'}.{_srs_note} "
                        f"Roll to 60+ DTE to extend thesis runway. Max 1 roll per position. "
                        f"Entry quality: {_entry_quality}. "
                        f"(Bennett: reduce time decay via roll when thesis intact; "
                        f"Lopez de Prado: extend vertical barrier for thesis room.)"
                    ),
                    "Doctrine_Source": "Bennett + Lopez de Prado: Direction Adverse Roll-for-Time",
                    "Decision_State": STATE_ACTIONABLE,
                    "Required_Conditions_Met": True,
                })
                return result
            else:
                # ── EV Feasibility Escape (Nison + Chan) ────────────────
                # When only ONE adverse signal fires AND breakeven is well
                # within expected move, the position is not yet wrong — it's
                # losing but mathematically recoverable.
                # Nison (0.723): "Exit if, and only if, we expect the move
                #   to continue. Losing money ≠ being wrong."
                # Chan (0.684): "Wait for reversion is dangerous UNLESS you
                #   have a model." Breakeven < 0.5× expected move IS a model.
                # Given (0.755): "Directional trades should have a TIME stop."
                #   When time runs out, EV is irrelevant.
                # Jabbour (0.790): "Close and re-evaluate." → overrides when
                #   BOTH signals fire (strong adverse conviction).
                _both_adverse = (_roc5_adverse and _drift_is_adverse)
                # AUDIT FIX: Original_DTE was never populated — always defaulted to 0,
                # permanently disabling the EV feasibility escape. DTE_Entry is the
                # actual frozen entry DTE from Cycle 1 freeze.py.
                _original_dte = safe_row_float(row, 'DTE_Entry')
                _time_remaining_pct = (dte / _original_dte) if _original_dte > 0 else 0.0
                _ev_feasible = (
                    not pd.isna(_ev_ratio_da)
                    and _ev_ratio_da < 0.50
                )
                _time_has_room = _time_remaining_pct >= 0.50

                if (not _both_adverse
                    and _ev_feasible
                    and _time_has_room
                    and not _srs_override):
                    result.update({
                        "Action": "HOLD",
                        "Urgency": "MEDIUM",
                        "Rationale": (
                            f"Direction ADVERSE (marginal) for {_option_type_label}: "
                            f"stock {'rallying' if _is_put else 'falling'} (ROC5={_roc5:+.1f}%, "
                            f"Price_Drift={price_drift:+.1%}), P&L={pnl_pct:.0%}. "
                            f"BUT breakeven feasible: EV_Ratio={_ev_ratio_da:.2f}× "
                            f"(< 0.50× expected move). Time remaining: {_time_remaining_pct:.0%} "
                            f"of original DTE. Single adverse signal = noise, not trend. "
                            f"HOLD with TIME STOP: exit if DTE falls below 50% of "
                            f"original ({_original_dte * 0.5:.0f}d) or second adverse signal "
                            f"confirms direction.{_srs_note} "
                            f"Entry quality: {_entry_quality}. "
                            f"(Nison: losing money ≠ being wrong — exit only when move "
                            f"expected to continue; Chan: reversion rational when model "
                            f"supports it; Given: TIME stop paramount for directional trades.)"
                        ),
                        "Doctrine_Source": "Nison + Chan + Given: EV Feasible — Hold with Time Stop",
                        "Decision_State": STATE_NEUTRAL_CONFIDENT,
                        "Required_Conditions_Met": True,
                    })
                    return result

                # ── Standard EXIT: roll blocked, EV escape not available ──
                _fail_reasons = []
                if _thesis_state_da != 'INTACT':
                    _fail_reasons.append(f"Thesis={_thesis_state_da}")
                if not _confirming_signal:
                    _fail_reasons.append(f"no confirming momentum (slope={_mom_slope_da:+.3f})")
                if pd.isna(_ev_ratio_da) or _ev_ratio_da >= 1.0:
                    _fail_reasons.append(f"EV_Ratio={'N/A' if pd.isna(_ev_ratio_da) else f'{_ev_ratio_da:.2f}'} (breakeven beyond expected move)")
                if not _iv_roll_affordable:
                    if _iv_pctile_reliable:
                        _fail_reasons.append(f"IV_Pctile={_iv_pctile_da:.0f}% (expensive to roll)")
                    elif _iv_gap_valid:
                        _fail_reasons.append(
                            f"IV gap={_iv_gap_for_roll:+.1f}% with {_iv_depth_da}d history — "
                            f"no vol edge for buyer (Jabbour: reevaluate as fresh entry)"
                        )
                if dte > 30:
                    _fail_reasons.append(f"DTE={dte:.0f} (not in theta acceleration zone)")
                if _already_rolled:
                    _fail_reasons.append("already rolled once")
                if _both_adverse:
                    _fail_reasons.append("BOTH adverse signals confirm direction")
                if _ev_feasible and not _time_has_room:
                    _fail_reasons.append(f"time stop: {_time_remaining_pct:.0%} of DTE remaining (Given)")
                if _srs_override:
                    _override_reasons = []
                    if pnl_pct < -0.40:
                        _override_reasons.append(f"P&L={pnl_pct:.0%} (absolute damage too severe)")
                    if 'ROLL' in _prior_action_da:
                        _override_reasons.append("already rolled (Jabbour)")
                    if dte < 10:
                        _override_reasons.append(f"DTE={dte:.0f} (no time)")
                    _fail_reasons.append(f"SRS override: {', '.join(_override_reasons)}")

                result.update({
                    "Action": "EXIT",
                    "Urgency": "MEDIUM",
                    "Rationale": (
                        f"Direction ADVERSE for {_option_type_label}: "
                        f"stock {'rallying' if _is_put else 'falling'} (ROC5={_roc5:+.1f}%, "
                        f"Drift={drift_dir}, Price_Drift={price_drift:+.1%}), P&L={pnl_pct:.0%}.{_sigma_tag} "
                        f"Roll blocked: {'; '.join(_fail_reasons)}.{_srs_note} "
                        f"Cut losses — direction and time both working against the position. "
                        f"Entry quality: {_entry_quality} (leash: {_pnl_threshold_da:.0%}). "
                        f"(Natenberg Ch.5: position no longer sensible under new conditions; "
                        f"Jabbour Ch.7: don't roll a bad trade; "
                        f"Krishnan: hope is not an investment strategy.)"
                    ),
                    "Doctrine_Source": "Natenberg Ch.5 + Jabbour Ch.7: Direction Adverse EXIT",
                    "Decision_State": STATE_ACTIONABLE,
                    "Required_Conditions_Met": True,
                })
                return result

    # 2c. Theta dominance + flat/adverse drift — escalate urgency
    # Passarelli Ch.2: theta eroding premium without directional contribution
    # Use .value or split on '.' to handle both enum objects and plain strings
    _greek_raw = row.get('GreekDominance_State', '') or ''
    greek_dom = (getattr(_greek_raw, 'value', None) or str(_greek_raw).split('.')[-1]).upper()
    # drift_dir already parsed in direction-awareness helper above
    _mom_raw = row.get('MomentumVelocity_State', '') or ''
    mom_state = (getattr(_mom_raw, 'value', None) or str(_mom_raw).split('.')[-1]).upper()
    if (greek_dom == 'THETA_DOMINANT'
            and drift_dir in ('FLAT', _adverse_drift_dir)
            and mom_state in ('STALLING', 'REVERSING')
            and dte <= 60
            and pnl_pct < -0.20):
        # Ticker-context branch: part of a multi-leg structure?
        # If yes, the generic "monitor" hold is wrong — the question is specifically
        # whether to EXIT THIS LEG to stop the collective theta bleed while keeping
        # the rest of the structure intact (Natenberg Ch.4: leg-level risk management).
        _tc_trade_count  = int(row.get('_Ticker_Trade_Count', 1) or 1)
        _tc_net_theta    = safe_row_float(row, '_Ticker_Net_Theta')
        _tc_net_vega     = safe_row_float(row, '_Ticker_Net_Vega')
        _tc_structure    = str(row.get('_Ticker_Structure_Class', '') or '')
        _tc_strategy_mix = str(row.get('_Ticker_Strategy_Mix', '') or '')
        _tc_ticker       = str(row.get('Underlying_Ticker', '') or '')
        _leg_theta       = safe_row_float(row, 'Theta')

        # Structures where the short-dated leg IS the income engine — theta is
        # the intended payoff, not a leak. Exiting it would destroy the structure.
        # CALL_DIAGONAL, PUT_DIAGONAL: short near-dated leg harvests theta by design.
        # INCOME_WITH_LEGS: BUY_WRITE/CC — the short call IS the strategy.
        # SINGLE_LEG: no multi-leg context to compare against.
        _income_structures = {"CALL_DIAGONAL", "PUT_DIAGONAL", "INCOME_WITH_LEGS", "SINGLE_LEG"}
        _is_income_leg = _tc_structure in _income_structures

        if _tc_trade_count > 1 and not is_leap and not _is_income_leg:
            # This short-dated leg is bleeding theta inside a long-vol/speculative structure.
            # Ticker net theta tells us the full daily cost across all legs.
            # Action: EXIT this leg specifically — not the whole position.
            _theta_daily_cost = abs(_tc_net_theta) * 100  # approximate dollar/day
            _leg_daily_cost   = abs(_leg_theta) * 100
            result.update({
                "Action": "EXIT",
                "Urgency": "MEDIUM",
                "Rationale": (
                    f"THETA_DOMINANT at DTE={dte:.0f} with {mom_state} momentum — no directional payoff. "
                    f"This leg costs ${_leg_daily_cost:.0f}/day theta. "
                    f"Collective {_tc_ticker} structure ({_tc_structure}) bleeds "
                    f"${_theta_daily_cost:.0f}/day total across {_tc_trade_count} trades. "
                    f"Exit THIS leg to stop the short-dated theta leak — "
                    f"keep the remaining legs ({_tc_strategy_mix.replace(strategy + ',', '').replace(',' + strategy, '').strip(',')}) intact. "
                    f"(Natenberg Ch.4: leg-level theta management in multi-leg structures.)"
                ),
                "Doctrine_Source": "Natenberg Ch.4: Multi-Leg Theta Management",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
        else:
            result.update({
                "Action": "HOLD",
                "Urgency": "MEDIUM",
                "Rationale": (
                    f"THETA_DOMINANT with flat price ({drift_dir}) and {mom_state} momentum at DTE={dte:.0f}. "
                    f"Theta consuming remaining premium without directional payoff — monitor closely. "
                    f"Exit if no catalyst within 10d (Passarelli Ch.2: Theta Awareness)."
                ),
                "Doctrine_Source": "Passarelli Ch.2: Theta Awareness",
                "Decision_State": STATE_NEUTRAL_CONFIDENT,
                "Required_Conditions_Met": True
            })
        return result

    # 2c-ii. Time-to-Impulse Gate — Range-bound decay without catalyst
    # ─────────────────────────────────────────────────────────────────────────────
    # For long options, being RANGE_BOUND with no compression and no momentum
    # signal is NOT a valid hold thesis — theta eats the position alive while
    # waiting for a move that may not come within the remaining DTE.
    #
    # This gate fires REGARDLESS of P&L (unlike 2c which requires pnl < -0.20).
    # A flat-P&L position is still at risk: theta is running, no catalyst exists,
    # and the clock is ticking.
    #
    # Condition (ALL must be true):
    #   1. DTE ≤ 60 (theta acceleration zone for short-dated; tighter window)
    #      OR DTE ≤ 180 and is_leap (LEAPs: wider window, same principle applies)
    #   2. GreekDominance = THETA_DOMINANT (theta > delta contribution)
    #   3. No compression signal (bb_width_z > -0.5, no EARLY/MID_COMPRESSION)
    #   4. No directional momentum building (ROC5, ROC10 both flat or negative)
    #   5. Price is range-bound (choppiness > 55 OR ADX < 18)
    #
    # Output: HOLD with Urgency=HIGH + "WATCH — breakout required within X days"
    #
    # Exempt conditions (gate does NOT fire):
    #   - Compression coiling (valid hold context — energy building)
    #   - Bottoming reversal with RSI < 42 (momentum recovering)
    #   - Position already profitable (option_gain_pct > 0.15) — let winners run
    #   - Already caught by 2c (pnl < -0.20 + theta dominant already handled)
    #
    # Doctrine:
    #   McMillan Ch.4: "Time is the enemy of long options in a sideways market —
    #     if the stock isn't moving toward your strike, you're bleeding."
    #   Passarelli Ch.2: "Theta doesn't pause for indecision — if there's no
    #     catalyst, there's no thesis."
    # ─────────────────────────────────────────────────────────────────────────────

    _tti_dte_raw = row.get('DTE', 999) or 999
    try:
        _tti_dte = float(_tti_dte_raw)
    except (TypeError, ValueError):
        _tti_dte = 999.0

    _tti_greek_raw = row.get('GreekDominance_State', '') or ''
    _tti_greek = (getattr(_tti_greek_raw, 'value', None) or str(_tti_greek_raw).split('.')[-1]).upper()
    _tti_theta_dominant = (_tti_greek == 'THETA_DOMINANT')

    _tti_comp_raw = str(row.get('CompressionMaturity', '') or '').split('.')[-1].upper()
    _tti_bb_z = safe_row_float(row, 'bb_width_z')
    _tti_compressing = (
        _tti_bb_z < -0.5
        or _tti_comp_raw in ('EARLY_COMPRESSION', 'MID_COMPRESSION', 'MATURE_COMPRESSION')
    )

    _tti_roc5  = safe_row_float(row, 'roc_5')
    _tti_roc10 = safe_row_float(row, 'roc_10')
    _tti_no_momentum = (_tti_roc5 <= 0 and _tti_roc10 <= 0)

    _tti_chop  = safe_row_float(row, 'choppiness_index', default=50.0)
    _tti_adx   = safe_row_float(row, 'adx_14', default=25.0)
    _tti_range_bound = (_tti_chop > 55 or _tti_adx < 18)

    _tti_mom_vel_raw = str(row.get('MomentumVelocity_State', '') or '').split('.')[-1].upper()
    _tti_bottoming = (_tti_mom_vel_raw == 'REVERSING'
                      and safe_row_float(row, 'rsi_14', default=50.0) < 42)

    # Compute option gain inline — option_gain_pct is not yet defined at this gate.
    # Use max(Last, Bid) for same stale-Last protection as the main TV calculation.
    _tti_last_raw  = safe_row_float(row, 'Last')
    _tti_bid_raw   = safe_row_float(row, 'Bid')
    _tti_last_p    = max(_tti_last_raw, _tti_bid_raw) if _tti_bid_raw > 0 else _tti_last_raw
    _tti_entry_p = abs(safe_row_float(row, 'Premium_Entry', default=0.0))
    _tti_option_gain = (_tti_last_p - _tti_entry_p) / _tti_entry_p if _tti_entry_p > 0 else 0.0

    # DTE threshold: short-dated (≤60) OR LEAP inside 180 DTE
    _tti_is_leap_local = 'LEAP' in str(row.get('Strategy', '') or '').upper() or _tti_dte >= 180
    _tti_dte_in_zone = (
        (_tti_dte <= 60 and not _tti_is_leap_local)
        or (_tti_dte <= 180 and _tti_is_leap_local)
    )

    # Exempt: already profitable, compressing (if not adverse), bottoming, or already caught by gate 2c
    # Direction fix: compression + adverse drift = likely breakout AGAINST thesis, not valid hold
    _tti_compression_direction_ok = (_tti_compressing and not _drift_is_adverse)
    _tti_exempt = (
        _tti_option_gain > 0.15           # already a winner
        or _tti_compression_direction_ok  # energy building — valid hold ONLY if not drifting against thesis
        or _tti_bottoming                 # reversing from oversold — valid hold
        or (pnl_pct < -0.20 and _tti_theta_dominant)  # already caught by gate 2c
    )

    if (not _tti_exempt
            and _tti_dte_in_zone
            and _tti_theta_dominant
            and _tti_no_momentum
            and _tti_range_bound):

        # Days budget: how many days before theta erodes another 20% of current premium
        _tti_theta_per_day = abs(safe_row_float(row, 'Theta', default=0.0))
        if _tti_theta_per_day > 0 and _tti_last_p > 0:
            _tti_20pct_budget = (_tti_last_p * 0.20) / _tti_theta_per_day
            _tti_budget_str = f"{int(_tti_20pct_budget)}d"
        else:
            _tti_budget_str = f"{max(5, int(_tti_dte // 4))}d"  # fallback: quarter of remaining DTE

        _tti_urgency = "HIGH" if _tti_dte <= 30 else "MEDIUM"

        # Ticker-context branch: short-dated leg inside a multi-leg structure
        _tti_trade_count  = int(row.get('_Ticker_Trade_Count', 1) or 1)
        _tti_net_theta    = safe_row_float(row, '_Ticker_Net_Theta')
        _tti_structure    = str(row.get('_Ticker_Structure_Class', '') or '')
        _tti_strategy_mix = str(row.get('_Ticker_Strategy_Mix', '') or '')
        _tti_ticker       = str(row.get('Underlying_Ticker', '') or '')
        _tti_leg_theta_d  = abs(safe_row_float(row, 'Theta', default=0.0)) * 100

        # Same exemption as gate 2c: diagonal/income structures use the short-dated
        # leg to harvest theta — it is the intended payoff, not a leak to stop.
        _tti_income_structures = {"CALL_DIAGONAL", "PUT_DIAGONAL", "INCOME_WITH_LEGS", "SINGLE_LEG"}
        _tti_is_income_leg = _tti_structure in _tti_income_structures

        if _tti_trade_count > 1 and not _tti_is_leap_local and not _tti_is_income_leg:
            _tti_total_theta_d = abs(_tti_net_theta) * 100
            _remaining_legs = _tti_strategy_mix.replace(strategy + ',', '').replace(',' + strategy, '').strip(',')
            result.update({
                "Action":  "EXIT",
                "Urgency": _tti_urgency,
                "Rationale": (
                    f"⚠️ RANGE_BOUND with no catalyst: ADX={_tti_adx:.0f}, "
                    f"choppiness={_tti_chop:.0f}, ROC5={_tti_roc5:+.1f}%. "
                    f"This leg costs ${_tti_leg_theta_d:.0f}/day theta with {_tti_dte:.0f} DTE remaining. "
                    f"Collective {_tti_ticker} structure ({_tti_structure}) bleeds "
                    f"${_tti_total_theta_d:.0f}/day across all legs. "
                    f"No breakout within ~{_tti_budget_str} = exit THIS leg to stop the short-dated bleed. "
                    f"Keep remaining legs ({_remaining_legs}) intact. "
                    f"(Natenberg Ch.4: leg-level theta management; McMillan Ch.4: no catalyst = no thesis.)"
                ),
                "Doctrine_Source": "Natenberg Ch.4: Multi-Leg Theta Management",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True,
            })
        else:
            result.update({
                "Action":   "HOLD",
                "Urgency":  _tti_urgency,
                "Rationale": (
                    f"⚠️ WATCH — breakout required within ~{_tti_budget_str}. "
                    f"Position is RANGE_BOUND with no momentum catalyst: "
                    f"ADX={_tti_adx:.0f}, choppiness={_tti_chop:.0f}, "
                    f"ROC5={_tti_roc5:+.1f}%, ROC10={_tti_roc10:+.1f}%. "
                    f"GreekDominance=THETA_DOMINANT with {_tti_dte:.0f} DTE — "
                    f"theta is consuming premium without directional payoff. "
                    f"No compression coiling detected (bb_width_z={_tti_bb_z:.2f}). "
                    f"Required action: either a directional breakout must materialize, "
                    f"or roll to a later expiry to buy more time for the thesis. "
                    f"Exit if no impulse within ~{_tti_budget_str} "
                    f"(McMillan Ch.4: time is the enemy in a sideways market; "
                    f"Passarelli Ch.2: no catalyst = no thesis)."
                ),
                "Doctrine_Source": "McMillan Ch.4 + Passarelli Ch.2: Time-to-Impulse",
                "Decision_State": STATE_NEUTRAL_CONFIDENT,
                "Required_Conditions_Met": True,
            })
        return result

    # 2d. Forward Expectancy Gate (Expected Move vs Required Move)
    # ─────────────────────────────────────────────────────────────────────────────
    # Guards against holding a position where the required move to breakeven
    # exceeds 1.5× the 10-day 1-sigma expected move (computed from IV, not HV).
    #
    # This is NOT a panic exit — it is a structural feasibility check.
    # The position needs price to move MORE than the market implies is likely
    # within a near-term rolling window. Holding costs theta every day while
    # the probability of recovery remains structurally low.
    #
    # Condition (ALL must be true for ROLL MEDIUM):
    #   1. EV_Feasibility_Ratio > 1.5 (required move > 1.5× 10D expected move)
    #   2. DTE < 45 (theta acceleration zone — time is now the enemy)
    #   3. pnl_pct < -0.20 (already down enough that recovery is non-trivial)
    #
    # Note: EV_50pct_Feasibility_Ratio is the 50% recovery analog — softer gate.
    # If only the 50% ratio is low, we add a warning but do not escalate.
    #
    # Doctrine:
    #   McMillan Ch.4: "Forward probability of reaching strike drives option value.
    #     When the required move exceeds what the market expects in your timeframe,
    #     the expected value of holding approaches zero."
    # ─────────────────────────────────────────────────────────────────────────────

    _ev_ratio      = safe_row_float(row, 'EV_Feasibility_Ratio') if pd.notna(row.get('EV_Feasibility_Ratio')) else float('nan')
    _ev_50_ratio   = safe_row_float(row, 'EV_50pct_Feasibility_Ratio') if pd.notna(row.get('EV_50pct_Feasibility_Ratio')) else float('nan')
    _theta_bleed   = safe_row_float(row, 'Theta_Bleed_Daily_Pct')
    _req_move_be   = safe_row_float(row, 'Required_Move_Breakeven')
    _req_move_50   = safe_row_float(row, 'Required_Move_50pct')
    _em_10         = safe_row_float(row, 'Expected_Move_10D')
    _conv_status   = str(row.get('Conviction_Status', '') or '').upper()
    _det_streak_raw = row.get('Delta_Deterioration_Streak', 0)
    _det_streak    = int(_det_streak_raw) if pd.notna(_det_streak_raw) and _det_streak_raw else 0

    if (not pd.isna(_ev_ratio)
            and _ev_ratio > 1.5
            and dte < 45
            and pnl_pct < -0.20):
        result.update({
            "Action":   "ROLL",
            "Urgency":  "MEDIUM",
            "Rationale": (
                f"Forward expectancy gate: required move to breakeven "
                f"(${_req_move_be:.1f}) is {_ev_ratio:.1f}× the 10-day "
                f"expected move (${_em_10:.1f}, IV-based). "
                f"50% recovery target (${_req_move_50:.1f}) ratio: {_ev_50_ratio:.2f}×. "
                f"With DTE={dte:.0f}d remaining, structural recovery probability is low. "
                f"Roll to extend time or widen strike "
                f"(McMillan Ch.4: Forward Expectancy)."
            ),
            "Doctrine_Source": "McMillan Ch.4: Forward Expectancy",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True
        })
        return result

    # 2e. Conviction Decay Escalation
    # ─────────────────────────────────────────────────────────────────────────────
    # Fires when delta has deteriorated consistently for 3+ consecutive cycles
    # AND the position is already at a significant loss with limited time.
    #
    # Delta trajectory is the most reliable early-warning signal for long options.
    # A sustained falling delta signals the market is moving structurally against
    # the thesis — not just a temporary headwind.
    #
    # Condition (ALL must be true):
    #   1. Conviction_Status == REVERSING (streak >= 3 consecutive deteriorating)
    #   2. Delta_Deterioration_Streak >= 3 (explicit count guard)
    #   3. DTE < 45
    #   4. pnl_pct < -0.20
    #
    # Doctrine:
    #   Passarelli Ch.2: "Conviction decay is not a temporary setback — it is
    #     the market telling you that time and direction are both against you."
    # ─────────────────────────────────────────────────────────────────────────────

    if (_conv_status == 'REVERSING'
            and _det_streak >= 3
            and dte < 45
            and pnl_pct < -0.20):
        result.update({
            "Action":   "ROLL",
            "Urgency":  "MEDIUM",
            "Rationale": (
                f"Conviction decay: delta deteriorating for {_det_streak} "
                f"consecutive cycles (Conviction={_conv_status}). "
                f"Position moving structurally OTM without reversal signal. "
                f"Roll or exit before theta accelerates at DTE={dte:.0f}d "
                f"(Passarelli Ch.2: Conviction Decay)."
            ),
            "Doctrine_Source": "Passarelli Ch.2: Conviction Decay",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True
        })
        return result

    # 2f. Theta Bleed Warning (non-escalating — appended to downstream rationale)
    # ─────────────────────────────────────────────────────────────────────────────
    # When theta is consuming > 3% of remaining premium per day on a long-premium
    # position, this is flagged in the rationale regardless of the final action.
    # It does NOT change the action — it adds urgency context.
    # ─────────────────────────────────────────────────────────────────────────────
    _theta_bleed_flag = row.get('Theta_Opportunity_Cost_Flag', False)
    if _theta_bleed_flag and _theta_bleed > 3.0 and not is_leap:
        # Append to existing rationale (result may already have content from 2a-2e)
        _existing = result.get('Rationale', '')
        result['Rationale'] = (
            _existing.rstrip() +
            f" ⚠️ Theta bleed: {_theta_bleed:.1f}%/day of remaining premium."
        )

    # 2g. Four-Dimension Thesis Health Evaluation
    # (Previously labeled 2d — renumbered to accommodate 2d/2e/2f forward expectancy gates)
    # ─────────────────────────────────────────────────────────────────────────────
    # A long option has ONE way to win: stock must move in the thesis direction by expiry.
    # Whether to HOLD, ROLL, or EXIT is therefore a thesis health question — NOT
    # a single-factor vol-ratio trigger. We score 4 dimensions and decide accordingly.
    #
    # Doctrine anchors:
    #   McMillan Ch.4: "Don't hold a directional position against the trend."
    #   Natenberg Ch.5: "Vol edge is only valuable when direction aligns."
    #   Passarelli Ch.5: "Compression setups are valid thesis contexts for long options."
    #   Cohen Ch.4: "Coiling price + converging MAs is a legitimate entry and hold context."
    #
    # Dimension A — Structure Direction
    #   Is the price structure pointing toward the strike?
    #   For calls: stock falling = broken, stock rising = intact
    #   For puts: stock rising = broken, stock falling = intact
    #   Signals: ROC20 direction, momentum_slope sign, ADX expanding
    #   INTACT: roc20 > 0 OR (slope > 0 AND adx expanding) → structure supports thesis
    #
    # Dimension B — Compression Setup
    #   Is the stock coiling/compressing (energy building toward a breakout)?
    #   Signals: bb_width_z < -0.8 (significant compression), CompressionMaturity=EARLY_COMPRESSION
    #   Compression is a VALID hold context for a long call — it's the pre-breakout phase.
    #   COMPRESSING:  bb_width_z < -0.8 OR compression_maturity in (EARLY_COMPRESSION, MID_COMPRESSION)
    #   EXPANDING:    bb_width_z > 0.5 → volatility expanding (breakout may be happening)
    #   NEUTRAL:      otherwise
    #
    # Dimension C — Vol Regime Context
    #   Is the vol environment favourable for a long call holder?
    #   Signals: HV vs IV ratio, iv_surface_shape (CONTANGO = term vol rising = favours long)
    #   NOTE: HV > IV alone is NOT a sell signal — direction of realized vol matters.
    #   FAVOURABLE:  iv_surface_shape=CONTANGO OR HV/IV < 1.10 → vol not working against us
    #   UNFAVOURABLE: HV > IV × 1.20 AND surface=BACKWARDATION → expensive vol + term structure hostile
    #   NEUTRAL:     otherwise
    #
    # Dimension D — Alignment Score
    #   Long call PASS conditions (any one sufficient to hold):
    #     D1: Compression setup (B=COMPRESSING) + slope not deeply negative (slope > -0.01)
    #     D2: Early acceleration — ROC5 > 0 AND ROC10 > 0 after compression
    #     D3: Momentum reversing upward from oversold — MomentumVelocity=REVERSING AND RSI < 45
    #        (bottoming pattern — long call can benefit from bounce toward strike)
    # ─────────────────────────────────────────────────────────────────────────────

    hv = safe_row_float(row, 'HV_20D')
    iv_live = row.get('IV_Now')
    iv = float(iv_live) if iv_live is not None and not pd.isna(iv_live) and float(iv_live or 0) > 0 \
         else safe_row_float(row, 'IV_30D')
    iv_source = "live" if (iv_live is not None and not pd.isna(iv_live) and float(iv_live or 0) > 0) else "daily"

    # Only evaluate thesis health when the position is losing meaningfully.
    # Positions in (-15%, 0%) fall through intentionally: a mildly losing position
    # without deep structural breakdown doesn't warrant thesis-level intervention.
    # Those are still covered by Gate 1 (drift), Gate 2.5 (thesis satisfaction),
    # and calendar gates (DTE/pin risk) evaluated above.
    if pnl_pct < -0.15:
        roc5         = safe_row_float(row, 'roc_5')
        roc10        = safe_row_float(row, 'roc_10')
        roc20        = safe_row_float(row, 'roc_20')
        mom_slope    = safe_row_float(row, 'momentum_slope')
        adx          = safe_row_float(row, 'adx_14')
        rsi          = safe_row_float(row, 'rsi_14', default=50.0)
        bb_width_z   = safe_row_float(row, 'bb_width_z')
        choppiness   = safe_row_float(row, 'choppiness_index', default=50.0)
        trend_state  = str(row.get('TrendIntegrity_State', '') or '').split('.')[-1].upper()
        mom_velocity = str(row.get('MomentumVelocity_State', '') or '').split('.')[-1].upper()
        comp_raw     = str(row.get('CompressionMaturity', '') or '').split('.')[-1].upper()
        iv_surface   = str(row.get('iv_surface_shape', '') or '').split('.')[-1].upper()

        # ── Prior-run watch metrics (injected by run_all.py step 2.95) ───────
        # These enable directional drift detection — not just a snapshot verdict.
        # None when no prior run exists (first time seeing this trade).
        _prior_bb_width_z   = row.get('Prior_bb_width_z')
        _prior_mom_slope    = row.get('Prior_momentum_slope')
        _prior_adx          = row.get('Prior_adx')
        _prior_rsi          = row.get('Prior_rsi')
        _have_prior         = all(
            v is not None and not (isinstance(v, float) and pd.isna(v))
            for v in [_prior_bb_width_z, _prior_mom_slope, _prior_adx, _prior_rsi]
        )

        # Deltas (current − prior): positive = improving for long call, negative = degrading
        if _have_prior:
            _d_bb_z   = bb_width_z   - float(_prior_bb_width_z)   # more negative = more compression
            _d_slope  = mom_slope    - float(_prior_mom_slope)     # positive = slope recovering
            _d_adx    = adx          - float(_prior_adx)           # positive = trend strengthening
            _d_rsi    = rsi          - float(_prior_rsi)           # positive = recovering from oversold
        else:
            _d_bb_z = _d_slope = _d_adx = _d_rsi = 0.0

        # Watch-level breaches: these tighten urgency within the HOLD/COILING cases.
        #
        # Slope: 2-consecutive-daily-close confirmation rule.
        #   Technical indicators come from daily OHLC bars — identical across all intraday
        #   runs on the same date. A single negative-slope day after a positive day could be
        #   data timing noise. Require 2 distinct calendar days both negative before treating
        #   it as a structural flip. run_all.py injects Prior2_momentum_slope = day N-2 close.
        #   Falls back to single-bar if Prior2 unavailable (position < 2 trading days old).
        #
        # RSI + ADX: absolute level thresholds, single-bar.
        #   Not directional flip detections — RSI < 40 and ADX < 15 are unconditionally
        #   significant regardless of the prior bar. No confirmation needed.
        #
        # bb_width_z releasing downward: single-bar, directional and unambiguous.
        _prior2_slope = row.get('Prior2_momentum_slope')
        _have_prior2  = (
            _prior2_slope is not None
            and not (isinstance(_prior2_slope, float) and pd.isna(_prior2_slope))
        )

        # Slope breach: 2-bar confirmation when prior2 available, else single-bar fallback
        if _have_prior and _have_prior2:
            _slope_breach = (mom_slope < 0 and float(_prior_mom_slope) < 0)
        elif _have_prior:
            _slope_breach = (mom_slope < 0 and float(_prior_mom_slope) >= 0)
        else:
            _slope_breach = False

        # RSI / ADX: absolute level, single-bar
        _rsi_breach = rsi < 40
        _adx_breach = adx < 15

        # Compression releasing downward
        _compression_resolving_down = (
            _have_prior
            and _d_bb_z > 0.15
            and mom_slope < -0.005
        )
        _any_watch_breach = _slope_breach or _rsi_breach or _adx_breach or _compression_resolving_down

        # Build a concise watch-status string surfaced in rationale
        def _watch_status() -> str:
            if not _have_prior:
                return "(no prior run — single-snapshot evaluation)"
            parts = []
            # bb_width_z: more negative = deepening compression (good); rising = decompressing
            _bb_arrow = "↓" if _d_bb_z < -0.05 else ("↑" if _d_bb_z > 0.05 else "→")
            parts.append(f"bb_width_z {bb_width_z:.2f} {_bb_arrow}{_d_bb_z:+.2f}")
            # slope: want ≥ 0
            _sl_arrow = "↑" if _d_slope > 0.002 else ("↓" if _d_slope < -0.002 else "→")
            _sl_flag  = " ⚠️FLIP" if _slope_breach else ""
            parts.append(f"slope {mom_slope:+.3f} {_sl_arrow}{_d_slope:+.3f}{_sl_flag}")
            # ADX: want ≥ 15
            _adx_arrow = "↑" if _d_adx > 1 else ("↓" if _d_adx < -1 else "→")
            _adx_flag  = " ⚠️COLLAPSE" if _adx_breach else ""
            parts.append(f"ADX {adx:.0f} {_adx_arrow}{_d_adx:+.0f}{_adx_flag}")
            # RSI: want ≥ 40
            _rsi_arrow = "↑" if _d_rsi > 1 else ("↓" if _d_rsi < -1 else "→")
            _rsi_flag  = " ⚠️BREAK" if _rsi_breach else ""
            parts.append(f"RSI {rsi:.0f} {_rsi_arrow}{_d_rsi:+.0f}{_rsi_flag}")
            if _compression_resolving_down:
                parts.append("⚠️ compression releasing downward")
            return " | ".join(parts)

        # ── Dimension A: Structure Direction ─────────────────────────────────
        # For calls (bullish): stock falling = broken, stock rising = intact
        # For puts (bearish): stock rising = broken, stock falling = intact
        if _is_put:
            _dir_broken = (roc20 > 8 and mom_slope > 0
                           and trend_state in ('NO_TREND', 'TREND_EXHAUSTED', 'WEAK_TREND')
                           and adx < 20 and rsi > 52)
            _dir_intact = (roc20 < 0 or (mom_slope < 0 and adx > 20)
                           or mom_velocity in ('ACCELERATING', 'TRENDING'))
        else:
            _dir_broken = (roc20 < -8 and mom_slope < 0
                           and trend_state in ('NO_TREND', 'TREND_EXHAUSTED', 'WEAK_TREND')
                           and adx < 20 and rsi < 48)
            _dir_intact = (roc20 > 0 or (mom_slope > 0 and adx > 20)
                           or mom_velocity in ('ACCELERATING', 'TRENDING'))
        _dir_weak   = not _dir_broken and not _dir_intact

        # ── Dimension B: Compression Setup ───────────────────────────────────
        _compressing = (bb_width_z < -0.8
                        or comp_raw in ('EARLY_COMPRESSION', 'MID_COMPRESSION', 'MATURE_COMPRESSION'))
        _expanding   = (bb_width_z > 0.5 and comp_raw not in (
                        'EARLY_COMPRESSION', 'MID_COMPRESSION', 'MATURE_COMPRESSION'))

        # ── Dimension C: Vol Regime ───────────────────────────────────────────
        _vol_hostile = (hv > 0 and iv > 0 and hv > iv * 1.20
                        and iv_surface in ('BACKWARDATION', ''))
        _vol_neutral = not _vol_hostile

        # ── Dimension D: Alignment (hold conditions) ──────────────────────────
        _d1_compression_coiling = (_compressing and mom_slope > -0.015)
        if _is_put:
            # For puts: early acceleration = price falling (roc5 < 0, roc10 < 0)
            _d2_early_accel         = (roc5 < 0 and roc10 < 0 and _compressing)
            # For puts: topping reversal = momentum reversing from overbought
            _d3_bottoming_reversal  = (mom_velocity in ('REVERSING',) and rsi > 52
                                       and mom_slope < 0.01)
        else:
            _d2_early_accel         = (roc5 > 0 and roc10 > 0 and _compressing)
            _d3_bottoming_reversal  = (mom_velocity in ('REVERSING',) and rsi < 48
                                       and mom_slope > -0.01)
        _alignment_pass         = _d1_compression_coiling or _d2_early_accel or _d3_bottoming_reversal

        # ── Decision ─────────────────────────────────────────────────────────
        # Case 1: Structure BROKEN — directional thesis failed regardless of vol
        if _dir_broken:
            _pct_to_strike = (_strike / _ul_last - 1) if _ul_last > 0 else 0
            _gap_str = f"{_pct_to_strike:.1%} away" if _ul_last > 0 else "N/A"
            result.update({
                "Action": "EXIT",
                "Urgency": "HIGH",
                "Rationale": (
                    f"Thesis BROKEN — structural direction failed for a {_option_type_label}. "
                    f"ROC20={roc20:.1f}%, slope={mom_slope:+.3f}, ADX={adx:.0f} (weak), "
                    f"RSI={rsi:.0f}, trend={trend_state}. "
                    f"Stock at ${_ul_last:.2f}, strike ${_strike:.0f}, {_gap_str} against {'uptrend' if _is_put else 'downtrend'}. "
                    + (f"Realized vol (HV={hv:.1%}) is {'upside' if _is_put else 'downside'} vol — HV>IV does NOT help "
                       f"a {_option_type_label} when stock is {'rising' if _is_put else 'falling'} (Natenberg Ch.5: directional alignment required). "
                       if hv > 0 and iv > 0 and hv > iv * 1.05 else "")
                    + f"Rolling buys more time for the same broken thesis. "
                    f"Exit and redeploy when structure recovers "
                    f"(McMillan Ch.4: cut thesis failures, not temporary weakness). "
                    f"Watch: {_watch_status()}"
                ),
                "Doctrine_Source": "McMillan Ch.4 + Natenberg Ch.5: Thesis BROKEN",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result

        # Case 2: Structure WEAK but compression is building — thesis still valid.
        # Urgency escalates to HIGH if any watch metric has breached its threshold,
        # signalling the compression setup is degrading toward a broken thesis.
        elif _dir_weak and _d1_compression_coiling:
            _comp_context = (
                f"bb_width_z={bb_width_z:.2f} (compressing, {abs(bb_width_z):.1f}σ below mean), "
                f"CompressionMaturity={comp_raw}"
                if comp_raw else
                f"bb_width_z={bb_width_z:.2f} (compressing)"
            )
            # Watch breach → escalate: compression holding but one guard metric cracking
            _case2_urgency = "HIGH" if _any_watch_breach else "MEDIUM"
            _breach_note = ""
            if _any_watch_breach:
                _breach_note = (
                    " ⚠️ WATCH BREACH — compression holds but guard metrics breaking: "
                    + (f"slope flipped negative ({mom_slope:+.3f}). " if _slope_breach else "")
                    + (f"RSI broke below 40 ({rsi:.0f}). " if _rsi_breach else "")
                    + (f"ADX collapsed below 15 ({adx:.0f}). " if _adx_breach else "")
                    + ("Compression releasing downward. " if _compression_resolving_down else "")
                    + "Gate 2d will flip to ROLL/EXIT if structure joins the breach."
                )
            result.update({
                "Action": "HOLD",
                "Urgency": _case2_urgency,
                "Rationale": (
                    f"Thesis COILING — price compressing, not broken. "
                    f"Structure direction weak (ROC20={roc20:.1f}%, slope={mom_slope:+.3f}) but "
                    f"{_comp_context}. "
                    f"Choppiness={choppiness:.0f}, MomentumVelocity={mom_velocity}. "
                    f"Compression is a valid hold context for a {_option_type_label} — energy builds toward breakout "
                    f"(Passarelli Ch.5: compression → release; Cohen Ch.4: coiling before expansion). "
                    + (f"Vol context: HV={hv:.1%} vs IV={iv:.1%} [{iv_source}] — "
                       + ("vol drag present, monitor if compression stalls > 10d. "
                          if hv > iv * 1.10 else "vol edge neutral. ")
                       if hv > 0 and iv > 0 else "")
                    + f"Watch: {_watch_status()}"
                    + _breach_note
                ),
                "Doctrine_Source": "Passarelli Ch.5 + Cohen Ch.4: Compression Hold",
                "Decision_State": STATE_NEUTRAL_CONFIDENT,
                "Required_Conditions_Met": True
            })
            return result

        # Case 3: Structure weak, bottoming reversal pattern — watch, not act
        elif _dir_weak and _d3_bottoming_reversal:
            _case3_urgency = "HIGH" if _any_watch_breach else "MEDIUM"
            result.update({
                "Action": "HOLD",
                "Urgency": _case3_urgency,
                "Rationale": (
                    f"Thesis REVERSING — momentum shifting from {'upside' if _is_put else 'downside'}. "
                    f"MomentumVelocity={mom_velocity}, RSI={rsi:.0f} ({'overbought' if _is_put else 'oversold'} territory), "
                    f"slope={mom_slope:+.3f} (turning). "
                    f"ROC20={roc20:.1f}% but 5d/10d trend: ROC5={roc5:+.1f}%, ROC10={roc10:+.1f}%. "
                    f"{'Topping' if _is_put else 'Bottoming'} pattern — {_option_type_label} can recover from here if reversal holds. "
                    + (f"HV={hv:.1%} > IV={iv:.1%} [{iv_source}] — if reversal fails within 5d, vol drag "
                       f"makes rolling expensive. Exit if RSI {'> 65' if _is_put else '< 35'} and slope re-accelerates {'up' if _is_put else 'down'}. "
                       if hv > 0 and iv > 0 and hv > iv * 1.05 else "")
                    + f"Monitor for 5d confirmation before adding (McMillan Ch.4: wait for reversal confirmation). "
                    f"Watch: {_watch_status()}"
                ),
                "Doctrine_Source": "McMillan Ch.4: Reversal Monitoring",
                "Decision_State": STATE_NEUTRAL_CONFIDENT,
                "Required_Conditions_Met": True
            })
            return result

        # Case 4: Structure weak, vol hostile, no alignment pass — roll to reduce vol drag
        elif _dir_weak and _vol_hostile and not _alignment_pass:
            result.update({
                "Action": "ROLL",
                "Urgency": "MEDIUM",
                "Rationale": (
                    f"Thesis WEAKENING with vol headwind. "
                    f"Structure: ROC20={roc20:.1f}%, slope={mom_slope:+.3f}, ADX={adx:.0f}. "
                    f"Vol: HV={hv:.1%} > IV={iv:.1%} [{iv_source}] ({(hv/iv - 1):.0%} above implied) "
                    f"with {iv_surface} surface — vol drag compounding against a weakening thesis. "
                    f"No compression or reversal alignment detected "
                    f"(bb_width_z={bb_width_z:.2f}, MomentumVelocity={mom_velocity}). "
                    f"Roll to a lower-IV further expiry to reduce theta and vol cost while the thesis resets "
                    f"(Natenberg Ch.5: reduce vol edge deficit when direction is unclear)."
                ),
                "Doctrine_Source": "Natenberg Ch.5: Vol Drag Roll",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result

        # Case 5: Structure intact (or early accel confirmed) — HOLD, surface vol context
        elif _dir_intact or _d2_early_accel:
            # Only surface as a gate result if vol is hostile; otherwise fall through to HOLD below
            if _vol_hostile:
                result.update({
                    "Action": "HOLD",
                    "Urgency": "LOW",
                    "Rationale": (
                        f"Thesis INTACT — structure direction supports {_option_type_label}. "
                        f"ROC20={roc20:.1f}%, slope={mom_slope:+.3f}, ADX={adx:.0f}, "
                        f"MomentumVelocity={mom_velocity}. "
                        + (f"Vol note: HV={hv:.1%} > IV={iv:.1%} [{iv_source}] — vol is elevated but "
                           f"direction is in our favour so realized vol is {'downside' if _is_put else 'upside'} vol (Natenberg Ch.5). "
                           if hv > 0 and iv > 0 and hv > iv * 1.05 else "")
                        + f"Hold with current thesis (McMillan Ch.4: stay long while structure supports)."
                    ),
                    "Doctrine_Source": "McMillan Ch.4: Thesis Intact HOLD",
                    "Decision_State": STATE_NEUTRAL_CONFIDENT,
                    "Required_Conditions_Met": True
                })
                return result
            # Direction intact and vol not hostile → fall through to normal HOLD gate below

    # 2.5 Optimum Price / Thesis Satisfaction Check
    # The thesis for a long option has TWO failure modes:
    #   (a) BROKEN thesis — stock moved against thesis direction, structure failed → EXIT (handled above)
    #   (b) SATISFIED thesis — stock reached the measured-move target → TRIM or EXIT
    # This gate catches (b): the stock has moved far enough that the original edge is captured.
    #
    # "Optimum" proxy signals (in order of reliability):
    #   1. Price_Target_Entry field (if stored at scan entry)
    #   2. Measured_Move (breakout pattern target — typically ATR-based)
    #   3. Resistance_Level_1 (nearest overhead supply)
    #   4. Fallback: 1σ × DTE move from entry price (Natenberg Ch.5: expected-value anchor)
    #
    # At optimum: thesis is SATISFIED (not broken). Action = TRIM if multi-contract, EXIT if single.
    # McMillan Ch.4: "Realize profits when the target is reached — don't guess whether it keeps going."
    # Natenberg Ch.11: "Speed and direction — once you've been right on both, the edge is consumed."
    _price_target  = safe_row_float(row, 'Price_Target_Entry')
    _measured_move = safe_row_float(row, 'Measured_Move')
    _resistance_1  = safe_row_float(row, 'Resistance_Level_1')
    # SMA-based structural levels as last-resort fallbacks.
    # For LONG_PUT: SMA20/SMA50 represent support levels; breaking below = bearish thesis hit.
    # For LONG_CALL: SMA20/SMA50 represent intermediate targets.
    # Only use as fallback — they are dynamic levels, not frozen thesis targets.
    _sma20 = safe_row_float(row, 'SMA20')
    _sma50 = safe_row_float(row, 'SMA50')

    # Determine best available target.
    # For puts: prefer frozen IV-implied target, then moving average support levels.
    # For calls: prefer frozen IV-implied target, then resistance levels above price.
    if _is_put:
        # Bearish targets: use SMA20 if below current price, SMA50 if SMA20 not available
        _sma_fallback = 0.0
        if _sma20 > 0 and _ul_last > 0 and _sma20 < _ul_last:
            _sma_fallback = _sma20
        elif _sma50 > 0 and _ul_last > 0 and _sma50 < _ul_last:
            _sma_fallback = _sma50
        _optimum_target = _price_target or _measured_move or _sma_fallback
    else:
        _optimum_target = _price_target or _measured_move or _resistance_1

    if _optimum_target > 0 and _ul_last > 0 and option_gain_pct > 0:
        # Stock has reached or exceeded the thesis price target.
        # For LONG_PUT: target is BELOW current price (bearish thesis) — satisfied when stock drops to/below target.
        # For LONG_CALL: target is ABOVE current price (bullish thesis) — satisfied when stock rises to/above target.
        if _is_put:
            _at_or_beyond_target = _ul_last <= _optimum_target
            _approaching_target  = _ul_last <= _optimum_target * 1.02   # within 2% above put target
        else:
            _at_or_beyond_target = _ul_last >= _optimum_target
            _approaching_target  = _ul_last >= _optimum_target * 0.98   # within 2% below call target

        # Determine target source label for rationale
        def _target_src_label():
            if _price_target:    return "IV-implied 1σ target (frozen at entry)"
            if _measured_move:   return "Measured_Move"
            if not _is_put and _resistance_1: return "Resistance_Level_1"
            if _sma20 and _optimum_target == _sma20: return "SMA20 support"
            if _sma50 and _optimum_target == _sma50: return "SMA50 support"
            return "price target"

        if _at_or_beyond_target and option_gain_pct >= 0.30:
            _target_source = _target_src_label()
            _qty_int = abs(safe_row_float(row, 'Quantity', default=1.0))
            _thesis_satisfied_action = "TRIM" if _qty_int > 1 else "EXIT"
            _thesis_satisfied_urgency = "MEDIUM"
            result.update({
                "Action": _thesis_satisfied_action,
                "Urgency": _thesis_satisfied_urgency,
                "Rationale": (
                    f"Thesis SATISFIED: underlying ${_ul_last:.2f} reached target ${_optimum_target:.2f} "
                    f"({_target_source}). Option up {option_gain_pct:.0%} — edge is captured. "
                    + (
                        f"Trim to 50% size to lock partial profits while staying long "
                        f"for any extension (McMillan Ch.4: Realize targets, stay optionally long)."
                        if _qty_int > 1
                        else
                        f"Take full profit — thesis complete, not broken. "
                        f"Natenberg Ch.11: Once right on speed AND direction, realize the edge."
                    )
                ),
                "Doctrine_Source": f"McMillan Ch.4 + Natenberg Ch.11: Thesis Satisfied ({_target_source})",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result

        elif _approaching_target and option_gain_pct >= 0.25:
            # Approaching target — escalate urgency on HOLD, surface thesis satisfaction risk
            result['_approaching_optimum'] = True   # signal for display layer
            result['_optimum_target'] = _optimum_target
            result['_optimum_source'] = _target_src_label()
            # Don't return — let HOLD gate add this context to rationale below

    # 2c-mfe. Directional Profit Capture Gate
    # ─────────────────────────────────────────────────────────────────────────────
    # Directional long options that reach +30% gain should lock profits.
    # Without this gate, positions round-trip: MSFT put Feb-2026 hit +48.4%
    # then bled back to -55%. Theta + direction reversal = double destruction.
    # At +50%: EXIT for single-contract, TRIM for multi-contract.
    # At +30%: EXIT MEDIUM for weak-entry, HOLD HIGH (alert) for strong-entry.
    # LEAPs exempt (longer thesis runway, less theta urgency).
    # McMillan Ch.4: "Realize profits on short-dated directional positions."
    # ─────────────────────────────────────────────────────────────────────────────
    if not is_leap and option_gain_pct >= 0.50:
        _qty_mfe = abs(safe_row_float(row, 'Quantity', default=1.0))
        _mfe_action = "TRIM" if _qty_mfe > 1 else "EXIT"
        result.update({
            "Action": _mfe_action,
            "Urgency": "MEDIUM",
            "Rationale": (
                f"PROFIT_CAPTURE: option up {option_gain_pct:.0%} (≥50%). "
                + (f"Trim to {max(1, int(_qty_mfe) // 2)} contracts to lock partial profit. "
                   if _qty_mfe > 1
                   else "Take full profit — directional edge captured. ")
                + f"P&L=${pnl_pct:.0%}. "
                f"(McMillan Ch.4: realize profits; avoid round-tripping.)"
            ),
            "Doctrine_Source": "McMillan Ch.4: Directional Profit Capture",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True
        })
        return result

    if not is_leap and option_gain_pct >= 0.30 and _entry_quality == 'WEAK':
        result.update({
            "Action": "EXIT",
            "Urgency": "MEDIUM",
            "Rationale": (
                f"PROFIT_CAPTURE: option up {option_gain_pct:.0%} (≥30%) on a WEAK entry "
                f"(entry trend: {_entry_ti}, structure: {_entry_ps}). "
                f"Weak entries get shorter profit leash — don't let a +30% gain become a loss. "
                f"(McMillan Ch.4: realize early on weak setups.)"
            ),
            "Doctrine_Source": "McMillan Ch.4: Weak Entry Profit Capture",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True
        })
        return result

    # 3-pre. Time stop — runs BEFORE winner management gates.
    # McMillan Ch.4 + Passarelli Ch.2/Ch.8: "Theta acceleration near expiry makes rolling
    # expensive and exit the preferred action regardless of P&L."
    #
    # Key insight (from simulation audit): if time stop fires AFTER winner gates, a
    # profitable position at DTE=10 gets routed to a roll-winner gate (ROLL LOW/MEDIUM)
    # instead of EXIT. But at DTE=10 with theta non-linear, rolling a winner is usually
    # wrong — the right action is to exit and capture the gain before theta destroys it.
    #
    # Exception: if the position has captured ≥100% gain AND momentum is still strong,
    # the winner gate (Gate 3a) still makes sense even inside the time stop zone — rolling
    # to a higher strike with confirmed momentum is valid at 10–15 DTE.
    # So: time stop fires UNLESS option_gain_pct ≥ 1.0 (handled by Gate 3a below).
    #
    # Doctrine: McMillan Ch.4: "Don't roll a winner at expiry — take the gain."
    #           Passarelli Ch.2: "Theta is non-linear — DTE≤7 = every day is expensive."
    _time_stop_dte = 90 if is_leap else 21
    if dte <= _time_stop_dte and option_gain_pct < 1.0:
        # Determine the best action: profitable → EXIT MEDIUM; losing → EXIT HIGH
        _ts_urgency  = "HIGH" if option_gain_pct <= 0 else "MEDIUM"
        _ts_pnl_note = (
            f"up {option_gain_pct:.0%}" if option_gain_pct > 0
            else f"down {abs(option_gain_pct):.0%}"
        )
        result.update({
            "Action": "EXIT",
            "Urgency": _ts_urgency,
            "Rationale": (
                f"Time stop: DTE={dte:.0f} ≤ {_time_stop_dte} "
                f"({'LEAP vega decay' if is_leap else 'theta acceleration zone'}). "
                f"Option {_ts_pnl_note} — "
                f"{'realize gains before theta erodes them' if option_gain_pct > 0 else 'cut losses before theta accelerates further'}. "
                f"(Passarelli {'Ch.8' if is_leap else 'Ch.2'}: time stop.)"
                + (_cal_bleed_note if _cal_bleed_note else "")
            ),
            "Doctrine_Source": f"Passarelli {'Ch.8' if is_leap else 'Ch.2'}: Time Stop",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True
        })
        return result

    # 3. Winner management — profit targets and ITM time-value decay
    # McMillan Ch.4: "Long options rarely deserve to run past doubling — theta decay
    # accelerates as the position becomes deeply in-the-money."
    # Passarelli Ch.5: "When deeply ITM, most of your value is intrinsic — you're paying
    # theta to protect gains you could realize today."

    # Pre-compute theta dollar cost (used in 3a and 3b rationale strings)
    _theta_day_dollar = abs(safe_row_float(row, 'Theta', default=0.0)) * 100 * abs(safe_row_float(row, 'Quantity', default=1.0))

    # Gate 3a: ≥100% gain on option price → EXIT or roll (full profit target)
    # Lifecycle-scaled maturity guard: a position must have consumed ≥10% of its intended
    # life before harvest gates fire. This prevents pricing artifacts and data-lag from
    # triggering restructuring on fresh entries while still allowing violent short-DTE
    # winners to be recognized quickly.
    #   DTE=21 income → min 2.1d (rounds to max(2,2)=2) — 2-day violent move valid
    #   DTE=45 swing  → min 4.5d (rounds to max(2,4.5)≈5) — similar to old flat guard
    #   DTE=475 LEAP  → min 47.5d — same-day XOM artifact correctly blocked
    # McMillan Ch.4: "Roll Winners" applies to positions that have fully developed
    # their thesis, not same-day noise. original_dte = dte + days_held (line 2062).
    _min_days_held = max(2, original_dte * 0.10)
    if option_gain_pct >= 1.0 and days_held >= _min_days_held:
        # LATE_CYCLE explicitly excluded from rolling: RSI divergence + decelerating ROC means
        # the easy move is behind us — rolling risks buying at exhaustion.
        # ACCELERATING/TRENDING → rolling still makes sense if DTE allows.
        #
        # LONG_PUT: roll DOWN to lower strike to lock gains and stay bearish.
        # LONG_CALL: roll UP to higher strike to lock gains and stay bullish.
        _mom_strong = mom_state in ('ACCELERATING', 'TRENDING')
        _trend_state = str(row.get('TrendIntegrity_State', '') or '').split('.')[-1].upper()
        if _is_put:
            # For a put, continuation means bearish trend still intact (downtrend)
            _trend_confirms = _trend_state in ('STRONG_TREND', 'WEAK_TREND')
            _roll_direction = "lower strike"
            _trend_desc = "bearish"
        else:
            _trend_confirms = _trend_state in ('STRONG_TREND', 'WEAK_TREND')
            _roll_direction = "higher strike"
            _trend_desc = "bullish"
        if _mom_strong and _trend_confirms and dte > 30:
            result.update({
                "Action": "ROLL",
                "Urgency": "MEDIUM",
                "Rationale": (
                    f"Profit target: option up {option_gain_pct:.0%} (${_entry_price:.2f}→${_last_price:.2f}). "
                    f"Momentum {mom_state} + trend {_trend_state} still {_trend_desc} — "
                    f"roll to {_roll_direction} to lock in gains and reduce premium-at-risk while staying directional "
                    f"(McMillan Ch.4: Roll Winners). "
                    f"Time value remaining: ${_time_val:.2f} ({_tv_pct:.0%} of price)."
                ),
                "Doctrine_Source": "McMillan Ch.4: Roll Winners",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
        else:
            # Build layered rationale: profit target + momentum state + time value urgency
            _exit_reasons = [
                f"Option up {option_gain_pct:.0%} (\\${_entry_price:.2f}→\\${_last_price:.2f}) — profit target reached"
            ]
            if mom_state == 'LATE_CYCLE':
                _exit_reasons.append(
                    f"momentum LATE_CYCLE (RSI diverging, ROC decelerating) — "
                    f"roll-up would convert realised gains back into time value at exhaustion point"
                )
            if _tv_pct > 0.40:
                _exit_reasons.append(
                    f"${_time_val:.2f}/share ({_tv_pct:.0%}) is pure time value decaying at "
                    f"\\${_theta_day_dollar:.0f}/day — exits now capture it; holding gives it back to the market"
                )
            elif _tv_pct > 0:
                _exit_reasons.append(
                    f"time value remaining: \\${_time_val:.2f} ({_tv_pct:.0%})"
                )
            result.update({
                "Action": "EXIT",
                "Urgency": "MEDIUM" if mom_state != 'LATE_CYCLE' else "HIGH",
                "Rationale": (
                    "Profit target: " + ". ".join(_exit_reasons) + " (McMillan Ch.4: Profit Target)."
                ),
                "Doctrine_Source": "McMillan Ch.4: Profit Target",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
        return result

    # Gate 3b-single: Single-contract trim-via-roll at 50%+ option gain
    # McMillan Ch.4: "With one contract you can't sell half — instead roll to a strike closer
    # to the money to extract intrinsic value while staying in the directional trade."
    # Passarelli Ch.6: "Rolling a winner locks the gain in the spread (sell current, buy closer
    # to ATM) — the debit paid is the cost of staying long; the credit received is partial harvest."
    #
    # Mechanics:
    #   LONG_PUT winner: roll DOWN to a lower strike (closer to ATM from below).
    #     - Close the deep ITM put (high intrinsic, expensive), buy a cheaper strike.
    #     - Net credit = intrinsic harvested. You keep directional exposure at lower cost basis.
    #   LONG_CALL winner: roll UP to a higher strike (closer to ATM from above).
    #     - Same logic in reverse.
    #
    # Only fires when deeply ITM (intrinsic > 60% of price) so rolling has real economic benefit —
    # if mostly time value, the roll credit is negligible and Gate 3b handles it instead.
    _qty_for_trim = abs(safe_row_float(row, 'Quantity', default=1.0))
    if (option_gain_pct >= 0.50
            and days_held >= _min_days_held  # lifecycle-scaled: ≥10% of intended life consumed
            and _qty_for_trim == 1           # single contract only (multi handled below)
            and _tv_pct < 0.40               # deeply ITM: intrinsic > 60% of price
            and not is_leap                  # LEAPs: rolling is rarely worth the friction
            and dte > 7):                    # don't roll within final week — just exit
        _roll_dir = "lower" if _is_put else "higher"

        # ── Urgency is momentum-aware; eligibility remains structural ─────────
        # Convexity decay (intrinsic > 60%) is a time/intrinsic function, not
        # velocity-dependent — so the gate fires regardless of momentum state.
        # But WHEN to act is informed by momentum:
        #   STALLING / REVERSING  → HIGH   — reversal risk; harvest intrinsic NOW
        #                                    before the position gives it back
        #   DECELERATING          → MEDIUM  — move slowing; harvest window narrowing
        #   TRENDING              → LOW     — sustained drift, no urgency; wait for
        #                                    better strike / liquidity conditions
        #   ACCELERATING          → LOW     — flush still developing; let it run,
        #                                    roll opportunistically at better price
        #   (unknown)             → LOW     — default conservative
        if mom_state in ('STALLING', 'REVERSING'):
            _3b_urgency   = 'HIGH'
            _mom_note     = (f"momentum {mom_state} — reversal risk elevated; "
                             f"harvest intrinsic now before position gives it back")
        elif mom_state == 'DECELERATING':
            _3b_urgency   = 'MEDIUM'
            _mom_note     = (f"momentum DECELERATING — move slowing; "
                             f"roll harvest window is narrowing")
        elif mom_state == 'ACCELERATING':
            _3b_urgency   = 'LOW'
            _mom_note     = (f"momentum ACCELERATING — flush still developing; "
                             f"roll timing flexible, let move mature for better strike")
        else:  # TRENDING or unknown
            _3b_urgency   = 'LOW'
            _mom_note     = (f"momentum {mom_state or 'TRENDING'} — sustained drift, "
                             f"no urgency; wait for better conditions (OI, spread, strike)")

        result.update({
            "Action": "ROLL",
            "Urgency": _3b_urgency,
            "Rationale": (
                f"Single-contract winner: option up {option_gain_pct:.0%} "
                f"(${_entry_price:.2f}→${_last_price:.2f}), intrinsic ${_intrinsic:.2f} "
                f"({1-_tv_pct:.0%} of price). "
                f"Roll to a {_roll_dir} strike to harvest intrinsic and reduce cost basis — "
                f"equivalent to a partial trim. "
                f"You cannot sell half a contract, but rolling extracts the same economic benefit: "
                f"lock most of the gain, stay directional at lower premium-at-risk "
                f"(McMillan Ch.4: Single-Contract Winner Management). "
                f"Urgency: {_mom_note}."
            ),
            "Doctrine_Source": "McMillan Ch.4 + Passarelli Ch.6: Single-Contract Trim via Roll",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True
        })
        return result

    # Gate 3b-pre: Multi-contract TRIM at 50%+ option gain
    # McMillan Ch.4: "For multi-contract positions, take partial profits at 50% gain —
    # reduces risk while keeping the remaining position open for the full move."
    # Passarelli Ch.6: "50% of max profit for income; ~50% of option gain for directional longs
    # is the natural half-way point to bank gains and let the rest run."
    # Anti-chasing corollary: locking partial profits is NOT chasing — it resets your cost basis
    # on the remaining contracts so you can survive a pullback without giving back all gains.
    if (option_gain_pct >= 0.50
            and days_held >= _min_days_held  # lifecycle-scaled: ≥10% of intended life consumed
            and _qty_for_trim > 1
            and not is_leap):   # LEAPs: different calculus — partial trim only if >90 DTE remains
        _trim_qty = max(1, int(_qty_for_trim / 2))
        _keep_qty = int(_qty_for_trim) - _trim_qty
        result.update({
            "Action": "TRIM",
            "Urgency": "MEDIUM",
            "Rationale": (
                f"Multi-contract winner: option up {option_gain_pct:.0%} "
                f"(${_entry_price:.2f}→${_last_price:.2f}) on {_qty_for_trim:.0f} contracts. "
                f"Bank gains on {_trim_qty} contract(s), keep {_keep_qty} contract(s) open. "
                f"Partial exit locks profit without abandoning the thesis. "
                f"Remaining position: breakeven-protected — can survive a pullback "
                f"(McMillan Ch.4: Partial Profit on Multi-Contract)."
            ),
            "Doctrine_Source": "McMillan Ch.4 + Passarelli Ch.6: Multi-Contract Partial Profit",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True
        })
        return result

    # Gate 3b: 50–99% gain + deeply ITM (time value < 40% of option price) + theta > $25/day
    # Passarelli Ch.5: holding deeply ITM option = paying theta to insure intrinsic gains.
    # At this point: sell to capture intrinsic, or roll up to a cheaper-to-carry strike.
    if (option_gain_pct >= 0.50
            and days_held >= _min_days_held  # lifecycle-scaled: ≥10% of intended life consumed
            and _tv_pct < 0.40          # time value < 40% of option price → mostly intrinsic
            and _theta_day_dollar > 25  # paying material theta to hold gains
            and not is_leap):           # LEAPs have more time, different calculus
        result.update({
            "Action": "ROLL",
            "Urgency": "MEDIUM",
            "Rationale": (
                f"Winner management: option up {option_gain_pct:.0%} (${_entry_price:.2f}→${_last_price:.2f}), "
                f"deeply ITM (intrinsic ${_intrinsic:.2f} / time value ${_time_val:.2f} = {_tv_pct:.0%} time). "
                f"Paying ${_theta_day_dollar:.0f}/day theta to hold gains that could be realized now. "
                f"Options: (1) EXIT — lock in profit. "
                f"(2) ROLL UP — buy back, sell higher strike to reduce premium-at-risk and reset carry. "
                f"(Passarelli Ch.5: Don't pay theta to protect intrinsic gains.)"
            ),
            "Doctrine_Source": "Passarelli Ch.5: Winner Carry Management",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True
        })
        return result

    # Gate 3b-theta: Theta Efficiency Exit — catches winners where theta will consume
    # ≥75% of remaining time value before expiry, even if TV% hasn't crossed the
    # hard 40% threshold used by Gate 3b-single and 3b.
    #
    # RAG grounding (all verified ≥ 0.68):
    #   Augen, Volatility Edge (0.769): "Close a winning long position with a modest profit"
    #   Given, No-Hype Options (0.756): "Minimum profit at 25%... close half on pullbacks"
    #   Jabbour, Option Trader Handbook (0.739): "Time decay greatest in last 30 days"
    #   Jabbour, Option Trader Handbook (0.730): Theta as % of premium is the key metric
    #   Passarelli, Trading Greeks (0.681): "Only pay theta as long as necessary"
    #
    # Why this gate exists:
    #   Gate 3b-single/3b use a hard TV < 40% cutoff. A position at TV=41% with theta
    #   that will consume all TV before expiry gets HOLD instead of EXIT — a 1% gap
    #   in TV% causes the engine to miss a structurally identical scenario.
    #   This gate uses a rate-based check (theta × DTE vs TV) instead of a level check.
    _te_theta_per_share = abs(safe_row_float(row, 'Theta', default=0.0))
    _te_theta_consumes_tv = (
        _te_theta_per_share > 0 and _time_val > 0 and dte > 0
        and _te_theta_per_share * dte >= _time_val * 0.75
    )
    _te_bleed_pct = (_te_theta_per_share / _last_price * 100) if _last_price > 0 else 0.0
    _te_tv_days = _time_val / _te_theta_per_share if _te_theta_per_share > 0 else float('inf')

    if (option_gain_pct >= 0.30
            and days_held >= _min_days_held
            and _te_theta_consumes_tv
            and _te_bleed_pct > 1.0
            and _tv_pct >= 0.40       # only fires above 3b threshold (below 40% → 3b handles)
            and not is_leap):
        result.update({
            "Action": "EXIT",
            "Urgency": "MEDIUM",
            "Rationale": (
                f"Theta efficiency exit: option up {option_gain_pct:.0%} "
                f"(${_entry_price:.2f}→${_last_price:.2f}). "
                f"Theta bleed {_te_bleed_pct:.1f}%/day will consume ≥75% of "
                f"remaining time value (${_time_val:.2f}/share) in {_te_tv_days:.0f} days — "
                f"before expiry at DTE {dte:.0f}. "
                f"Continuing to hold pays theta to insure gains you could realize now. "
                f"(Augen: close winning longs + "
                f"Jabbour: last-30-day theta acceleration + "
                f"Given: exit at 25%+ gain.)"
            ),
            "Doctrine_Source": "Augen (Volatility Edge) + Given (No-Hype) + Jabbour (Option Trader Handbook): Theta Efficiency",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True
        })
        return result

    # Gate 3b-theta-warn: Soft escalation for near-threshold winners (25–29% gain).
    # All theta efficiency conditions met but gain is 1–5% below the 30% EXIT threshold.
    # Escalate HOLD LOW → HOLD MEDIUM so the user sees urgency without a premature exit.
    # RAG: Given (0.756): "Minimum profit at 25%... close half on pullbacks"
    if (option_gain_pct >= 0.25
            and option_gain_pct < 0.30
            and days_held >= _min_days_held
            and _te_theta_consumes_tv
            and _te_bleed_pct > 1.0
            and _tv_pct >= 0.40
            and not is_leap):
        result.update({
            "Action": "HOLD",
            "Urgency": "MEDIUM",
            "Rationale": (
                f"Theta efficiency warning: option up {option_gain_pct:.0%} "
                f"(${_entry_price:.2f}→${_last_price:.2f}), approaching 30% harvest zone. "
                f"Theta bleed {_te_bleed_pct:.1f}%/day will consume ≥75% of "
                f"remaining time value (${_time_val:.2f}/share) in {_te_tv_days:.0f} days — "
                f"before expiry at DTE {dte:.0f}. "
                f"Not yet at EXIT threshold but carry cost is material — "
                f"monitor closely for exit or roll. "
                f"(Given: exit consideration at 25%+ gain + "
                f"Jabbour: theta as % of premium is the key metric.)"
            ),
            "Doctrine_Source": "Given (No-Hype Options) + Jabbour (Option Trader Handbook): Theta Efficiency Warning",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True
        })
        return result

    # 4. Time stop — catch-all for positions that ran through winner gates (option_gain_pct ≥ 1.0)
    # but STILL haven't returned (e.g., roll was triggered but DTE is now critical).
    # The pre-winner time stop (3-pre) already handled option_gain_pct < 1.0.
    # This gate catches the ≥100% gain case that fell through Gate 3a (no trend continuation).
    time_stop_dte = 90 if is_leap else 21
    if dte <= time_stop_dte:
        result.update({
            "Action": "EXIT",
            "Urgency": "HIGH",
            "Rationale": (
                f"Time stop: DTE={dte:.0f} ≤ {time_stop_dte} "
                f"({'LEAP vega decay' if is_leap else 'theta acceleration'}). "
                f"Option up {option_gain_pct:.0%} — take profits before theta destroys remaining value. "
                f"(Passarelli {'Ch.8' if is_leap else 'Ch.2'}: time stop.)"
            ),
            "Doctrine_Source": f"Passarelli {'Ch.8' if is_leap else 'Ch.2'}: Time Stop",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True
        })
        return result

    # 5. Delta floor: option now non-responsive to price moves
    # McMillan Ch.4: delta < 0.10 means option is effectively worthless for directional use
    if delta_now < 0.10:
        result.update({
            "Action": "EXIT",
            "Urgency": "MEDIUM",
            "Rationale": f"Delta floor breached ({delta_now:.2f} < 0.10) — contract non-responsive. Cut and redeploy (McMillan Ch.4).",
            "Doctrine_Source": "McMillan Ch.4: Delta Minimums",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True
        })
        return result

    # 6. Thesis regime degradation: entry setup no longer exists
    thesis = check_thesis_degradation(row)
    if thesis:
        # Urgency calibration:
        # - Deep ITM positions (intrinsic > 50% of price): vol compression has limited
        #   impact on intrinsic value. Downgrade urgency to LOW — the position is
        #   protected by intrinsic; the regime shift is an environment note, not a crisis.
        #   Natenberg Ch.11: "Vol compression damages OTM options most; deep ITM options
        #   retain intrinsic regardless of vol regime."
        # - Fresh positions (Days_In_Trade < 5): regime readings on <5-day positions are
        #   noise — the regime may not have stabilized yet. Downgrade to LOW.
        _g6_intrinsic = safe_row_float(row, 'Intrinsic_Val', default=0.0)
        _g6_last      = safe_row_float(row, 'Last', 'Premium_Entry', default=0.0)
        _g6_days_held = safe_row_float(row, 'Days_In_Trade', default=99.0)
        _g6_itm_deep  = (_g6_intrinsic > 0 and _g6_last > 0
                         and _g6_intrinsic / _g6_last >= 0.50)
        _g6_fresh     = _g6_days_held < 5
        if _g6_itm_deep:
            _g6_urgency = "LOW"
            _g6_note    = (f" [Deep ITM: intrinsic ${_g6_intrinsic:.2f} = "
                           f"{_g6_intrinsic/_g6_last:.0%} of price — vol compression "
                           f"does not threaten intrinsic; monitor for directional reversal.]")
        elif _g6_fresh:
            _g6_urgency = "LOW"
            _g6_note    = (f" [Fresh position ({_g6_days_held:.0f}d old) — "
                           f"regime reading may be noise; reassess after 5+ days.]")
        else:
            _g6_urgency = "MEDIUM"
            _g6_note    = ""

        # ── Gate Conflict Resolver ────────────────────────────────────────
        # Gate 6 fires ROLL on regime shift. Before returning, run the action
        # comparator and check whether HOLD dominates.
        #
        # If EV + MC both say HOLD, the regime degradation is real but the
        # *timing* of the roll is wrong. Output HOLD_PREPARE instead of ROLL:
        # "Setup degraded, prepare to roll, execute when MC shifts or DTE < 14."
        #
        # Arbitration rules (ALL must be true to downgrade ROLL → HOLD_PREPARE):
        #   R1: EV comparator says HOLD wins (not a statistical tie)
        #   R2: MC_Wait_Verdict is WAIT or HOLD (not ACT_NOW)
        #   R3: Roll is a debit (costs capital, not neutral/credit)
        #   R4: Vol confidence ≥ 0.60 (MC estimate is reliable enough to trust)
        #   R5: DTE > 14 (time pressure not yet critical — still have runway)
        #   R6: Position not at stop (pnl_pct > -0.80 — not in rescue mode)
        #
        # Capital impact check (R3 extension):
        #   If roll_cost > current_loss_amount → rolling doubles the loss exposure.
        #   Flag this even if arbitration doesn't fully downgrade.
        try:
            _arb = compare_actions_long_option(row, dte, pnl_pct)
            _arb_winner    = _arb["ev_winner"]
            _arb_margin    = _arb["ev_margin"]
            _arb_vol_conf  = _arb["vol_confidence"]
            _arb_cap_imp   = _arb["capital_impact"]   # $ debit to roll
            _arb_ev_sum    = _arb["ev_summary"]
            _arb_mc_used   = _arb["mc_used"]

            # MC verdict from row (populated by mc_management.py before engine runs)
            _mc_wait_v = str(row.get('MC_Wait_Verdict', '') or '').upper()
            _mc_hold_v = str(row.get('MC_Hold_Verdict', '') or '').upper()
            _mc_says_wait = _mc_wait_v in ('WAIT', 'HOLD') or _mc_hold_v == 'HOLD_JUSTIFIED'
            _mc_says_act  = _mc_wait_v == 'ACT_NOW' or _mc_hold_v == 'EXIT_NOW'

            # Current loss in dollars (for capital impact comparison)
            _pos_pnl_dollars = safe_row_float(row, 'PnL_Total', 'Total_GL_Dollar')
            _roll_exceeds_loss = (_arb_cap_imp > 0 and _pos_pnl_dollars < 0
                                  and _arb_cap_imp > abs(_pos_pnl_dollars))

            # Check all arbitration rules
            _r1_ev_hold_wins = (_arb_winner == 'HOLD' and _arb_margin >= 75.0)
            _r2_mc_wait      = _mc_says_wait and not _mc_says_act
            _r3_debit_roll   = _arb_cap_imp > 0
            _r4_vol_reliable = _arb_vol_conf >= 0.60
            _r5_dte_ok       = float(dte) > 14
            _r6_not_rescue   = pnl_pct > -0.80

            _arbitration_says_wait = (
                _r1_ev_hold_wins and _r2_mc_wait and _r3_debit_roll
                and _r4_vol_reliable and _r5_dte_ok and _r6_not_rescue
            )

            # Capital impact warning (fires independently of arbitration decision)
            _cap_impact_note = ""
            if _roll_exceeds_loss and _r3_debit_roll:
                _cap_impact_note = (
                    f" ⚠️ Capital impact: rolling costs ${_arb_cap_imp:,.0f} — "
                    f"exceeds current loss of ${abs(_pos_pnl_dollars):,.0f}. "
                    f"Rolling doubles total loss exposure."
                )

            # Build arbitration metadata for output columns
            _arb_rules_fired = []
            if not _r1_ev_hold_wins: _arb_rules_fired.append("EV→ROLL" if _arb_winner == 'ROLL' else f"EV→{_arb_winner}")
            if not _r2_mc_wait:      _arb_rules_fired.append("MC→ACT_NOW")
            if not _r3_debit_roll:   _arb_rules_fired.append("credit-roll")
            if not _r4_vol_reliable: _arb_rules_fired.append(f"vol-conf-low({_arb_vol_conf:.0%})")
            if not _r5_dte_ok:       _arb_rules_fired.append(f"DTE≤14({dte:.0f}d)")
            _arb_override_reason = "; ".join(_arb_rules_fired) if _arb_rules_fired else "all-clear"

            if _arbitration_says_wait:
                # Downgrade ROLL → HOLD_PREPARE
                # "The setup degraded but rolling NOW is not the right timing."
                _arb_rationale = (
                    f"Entry thesis regime degraded: {thesis['text']}. "
                    f"However, action arbitration says HOLD dominates ROLL: {_arb_ev_sum} "
                    f"MC confirms: {_mc_wait_v or 'WAIT'}. "
                    f"Roll cost ${_arb_cap_imp:,.0f} not yet justified. "
                    f"PREPARE to roll — execute when MC shifts to ACT_NOW or DTE ≤ 14d. "
                    f"(McMillan Ch.4: thesis degraded but timing matters; "
                    f"Passarelli Ch.6: roll when price edge, not just when setup changes.)"
                    f"{_cap_impact_note}"
                )
                result.update({
                    "Action":                  "ROLL",         # keep ROLL for UI routing
                    "Urgency":                 "LOW",          # downgrade: wait, don't act yet
                    "Rationale":               _arb_rationale,
                    "Doctrine_Source":         "McMillan Ch.4: Thesis Persistence + Arbitration Override",
                    "Decision_State":          STATE_ACTIONABLE,
                    "Required_Conditions_Met": True,
                    # ── Arbitration output fields ─────────────────────────
                    "Arbitration_Gate":        "G6_THESIS_DEGRADATION",
                    "Arbitration_Gate_Action": "ROLL",
                    "Arbitration_EV_Winner":   _arb_winner,
                    "Arbitration_EV_Margin":   round(_arb_margin, 0),
                    "Arbitration_Override":    "HOLD_PREPARE",
                    "Arbitration_Override_Reason": _arb_override_reason,
                    "Arbitration_Vol_Confidence": round(_arb_vol_conf, 2),
                    "Arbitration_Capital_Impact": round(_arb_cap_imp, 0),
                    "Arbitration_MC_Used":     _arb_mc_used,
                    "Arbitration_Summary":     _arb_ev_sum,
                    "Action_EV_Ranking":       ">".join(_arb["ranked_actions"]),
                })
                return result
            else:
                # ROLL stands — arbitration did not override
                # Still attach arbitration metadata so the UI can show the reasoning
                _g6_note += _cap_impact_note
                result.update({
                    "Action":                  "ROLL",
                    "Urgency":                 _g6_urgency,
                    "Rationale":               (
                        f"Entry thesis regime degraded: {thesis['text']}. "
                        f"Original setup no longer intact — reassess or roll "
                        f"(McMillan Ch.4: Thesis Persistence).{_g6_note} "
                        f"Arbitration confirms ROLL: {_arb_ev_sum}"
                    ),
                    "Doctrine_Source":         "McMillan Ch.4: Thesis Persistence",
                    "Decision_State":          STATE_ACTIONABLE,
                    "Required_Conditions_Met": True,
                    "Arbitration_Gate":        "G6_THESIS_DEGRADATION",
                    "Arbitration_Gate_Action": "ROLL",
                    "Arbitration_EV_Winner":   _arb_winner,
                    "Arbitration_EV_Margin":   round(_arb_margin, 0),
                    "Arbitration_Override":    "NONE",
                    "Arbitration_Override_Reason": _arb_override_reason,
                    "Arbitration_Vol_Confidence": round(_arb_vol_conf, 2),
                    "Arbitration_Capital_Impact": round(_arb_cap_imp, 0),
                    "Arbitration_MC_Used":     _arb_mc_used,
                    "Arbitration_Summary":     _arb_ev_sum,
                    "Action_EV_Ranking":       ">".join(_arb["ranked_actions"]),
                })
                return result

        except Exception as _arb_err:
            # Arbitration failed — fall through to original ROLL output, log warning
            logger.warning(f"[Arbitration] Gate 6 comparator failed: {_arb_err}")
            result.update({
                "Action": "ROLL",
                "Urgency": _g6_urgency,
                "Rationale": (
                    f"Entry thesis regime degraded: {thesis['text']}. "
                    f"Original setup no longer intact — reassess or roll "
                    f"(McMillan Ch.4: Thesis Persistence).{_g6_note}"
                ),
                "Doctrine_Source": "McMillan Ch.4: Thesis Persistence",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result

    # 7. Recovery infeasibility: mathematically cannot recover in remaining DTE
    # Natenberg Ch.5: required daily move vs HV-implied 1-sigma move
    #
    # GAP FIX: Gate previously fired regardless of price direction.
    # Simulation identified: if stock is already drifting UP (recovering), the
    # "mathematically impossible" label is based on HV-implied static analysis —
    # it doesn't account for an active catalyst or directional momentum already in play.
    # Suppressing the gate when Drift_Direction='Up' prevents premature exits on
    # positions that are actively recovering (Natenberg Ch.5: forward prob > static math).
    _rf_drift_dir = str(row.get('Drift_Direction', '') or '').upper()
    # Direction fix: "recovering" means stock moving TOWARD the option's thesis direction
    if _is_put:
        _recovering = _rf_drift_dir in ('DOWN', 'DOWNWARD')
    else:
        _recovering = _rf_drift_dir in ('UP', 'UPWARD')
    recovery = str(row.get('Recovery_Feasibility', '') or '').upper()
    if recovery in ('IMPOSSIBLE', 'UNLIKELY') and pnl_pct < -0.20 and not _recovering:
        hv_daily  = safe_row_float(row, 'HV_Daily_Move_1Sigma')
        req_daily = safe_row_float(row, 'Recovery_Move_Per_Day')
        if recovery == 'IMPOSSIBLE' and pnl_pct < -0.30:
            result.update({
                "Action": "EXIT",
                "Urgency": "HIGH",
                "Rationale": (
                    f"Recovery infeasible: needs ${req_daily:.2f}/day vs "
                    f"HV-implied ${hv_daily:.2f}/day ({req_daily/max(hv_daily, 0.01):.1f}× sigma). "
                    f"Cut losses — insufficient time and vol to recover (Natenberg Ch.5)."
                ),
                "Doctrine_Source": "Natenberg Ch.5: Recovery Infeasibility",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result
        elif recovery == 'UNLIKELY' and result.get('Urgency', 'LOW') == 'LOW':
            result['Urgency'] = 'MEDIUM'
            result['Rationale'] = (
                f"Position intact — Delta={delta_now:.2f}, DTE={dte:.0f}, structure={price_state.split('.')[-1]}. "
                f"Recovery UNLIKELY: needs ${req_daily:.2f}/day vs HV ${hv_daily:.2f}/day. "
                f"Monitor closely (Natenberg Ch.5)."
            )
            result['Doctrine_Source'] = "Natenberg Ch.5: Recovery Unlikely"
            result['Decision_State'] = STATE_NEUTRAL_CONFIDENT
            result['Required_Conditions_Met'] = True
            return result

    # ── Signal Hub: institutional signal annotations (collected, appended after HOLD) ──
    _signal_hub_notes = ""
    _macd_div_lo = str(row.get('MACD_Divergence', 'None') or 'None')
    _is_put_lo = any(s in strategy for s in ('LONG_PUT', 'BUY_PUT', 'LEAPS_PUT'))
    if _macd_div_lo == 'Bearish_Divergence' and not _is_put_lo:
        _signal_hub_notes += (
            " MACD bearish divergence (Murphy 0.691): price HH but MACD LH"
            " — near-term upside may be exhausting."
        )
    elif _macd_div_lo == 'Bullish_Divergence' and _is_put_lo:
        _signal_hub_notes += (
            " MACD bullish divergence (Murphy 0.691): price LL but MACD HL"
            " — bearish thesis may be losing momentum."
        )
    _rs_spy_lo = safe_row_float(row, 'RS_vs_SPY_20d')
    if abs(_rs_spy_lo) > 5:
        _rs_dir = "outperforming" if _rs_spy_lo > 0 else "underperforming"
        _rs_aligned = ((_rs_spy_lo > 0 and not _is_put_lo) or (_rs_spy_lo < 0 and _is_put_lo))
        _signal_hub_notes += (
            f" RS vs SPY: {_rs_spy_lo:+.1f}% ({_rs_dir} over 20d). "
            f"{'Supports conviction.' if _rs_aligned else 'Counter-thesis — verify catalyst.'}"
            f" (Murphy 0.740)"
        )

    # Default HOLD — enrich rationale with carry cost and scale context
    theta_day = abs(safe_row_float(row, 'Theta', default=0.0))
    last_price = safe_row_float(row, 'Last')
    qty = abs(safe_row_float(row, 'Quantity', default=1.0))

    # Carry cost: theta decay + Fidelity 10.375% margin interest on option premium paid.
    # Natenberg Ch.5: total holding cost = theta decay + financing cost on capital deployed.
    # For a long option on margin, the daily bleed has TWO components:
    #   1. Theta decay (time value eroding)
    #   2. Margin interest: 10.375%/yr on the option's market value * number of contracts * 100
    carry_note = ""
    if theta_day > 0 and last_price > 0:
        option_market_value = last_price * 100 * qty  # total dollar value of position
        daily_margin_interest = option_market_value * FIDELITY_MARGIN_RATE_DAILY
        total_daily_carry = theta_day * 100 * qty + daily_margin_interest
        # Long options: theta is a COST (bleed), not income — label as daily bleed, not yield.
        # X%/yr framing is misleading (sounds like income yield; it's the opposite).
        carry_note = (
            f" Carry: ${theta_day*100*qty:.2f}/day theta"
            f" + ${daily_margin_interest:.2f}/day margin interest (10.375% × ${option_market_value:,.0f})"
            f" = ${total_daily_carry:.2f}/day total hold cost."
        )

    # Scale signal — doctrine-aligned:
    #   Scale UP:   McMillan Ch.4: only add to a WINNING position when thesis confirms.
    #               Signal strength gating: require both profit AND positive momentum.
    #   Scale DOWN: Natenberg Ch.11: when holding multiple contracts and adverse move is
    #               within recovery range but carry is expensive, reduce to minimum size
    #               to keep the thesis alive without compounding carry cost risk.
    scale_note = ""

    # Scale up: only when profitable + momentum confirming + NOT chasing a fresh breakout
    # McMillan Ch.4: "Pyramid on strength — but only on a PULLBACK, never buy into a runaway move."
    # Natenberg Ch.11: "Speed and direction — if speed is at peak (ACCELERATING), the move is
    # consuming itself; wait for deceleration then re-acceleration on a retest."
    # Anti-chasing gates:
    #   1. Must already be profitable (option_gain_pct, not position P&L)
    #   2. Momentum TRENDING (sustained), NOT ACCELERATING (parabolic = chasing risk)
    #   3. RSI must be < 70 (not overbought — buying overbought = chasing)
    #   4. Price must be in a constructive structure (not EXTENDED)
    #   5. DTE > 21 (McMillan's actual rule: don't add within 3 weeks of expiry)
    #   6. Gamma_ROC_3D > 0 AND Gamma > 0.02 — convexity gate
    #      Gamma_ROC_3D > 0: gamma is EXPANDING (acceleration increasing = impulse phase)
    #      Gamma > 0.02:     gamma is SUBSTANTIAL (not noise — real convexity exists)
    #      Together: pyramiding into an impulse, not into late-trend exhaustion.
    #      McMillan Ch.4: "Don't add when delta acceleration is decelerating — the leverage
    #      you're paying for no longer exists." Flat/falling gamma = move is maturing.
    _rsi_for_scale  = safe_row_float(row, 'rsi_14', default=50.0)
    _gamma_now      = abs(safe_row_float(row, 'Gamma', default=0.0))
    _gamma_roc_3d   = safe_row_float(row, 'Gamma_ROC_3D')
    _price_state_scale = price_state.split('.')[-1].upper()
    _is_extended = 'EXTENDED' in _price_state_scale or 'BREAKOUT' in _price_state_scale
    _wave_phase = str(row.get('WavePhase_State', '') or '').upper()
    _scale_up_ready = (
        option_gain_pct >= 0.25               # already a winner
        and mom_state == 'TRENDING'            # sustained — NOT parabolic ACCELERATING
        and _wave_phase in ('BUILDING', '')    # wave phase: only scale in BUILDING window
        and _rsi_for_scale < 70               # not overbought (no chasing)
        and not _is_extended                  # structure not extended/runaway
        and qty >= 1
        and dte > 21                          # don't add within final 3 weeks
        and _gamma_roc_3d > 0                 # gamma expanding: impulse phase, not exhaustion
        and _gamma_now > 0.02                 # gamma substantial: real convexity, not noise
        and _conv_status in ('STABLE', 'STRENGTHENING')  # conviction not deteriorating
        and _pyramid_tier < 2                 # Murphy: max 2 add-on tiers (0→1, 1→2; tier 2 = full)
        and _winner_lifecycle not in ('THESIS_EXHAUSTING', 'FULL_POSITION')  # lifecycle not exhausted
    )
    _scale_up_watch = (
        option_gain_pct >= 0.25
        and mom_state == 'ACCELERATING'       # was: also LATE_CYCLE — but LATE_CYCLE = FADING wave, don't chase
        and _wave_phase not in ('FADING', 'EXHAUSTED', 'PEAKING')  # never watch fading waves
        and qty >= 1
    )

    # Pullback target: the specific price level to wait for before adding.
    # McMillan Ch.4: "Add on a retest of a prior support/resistance level — not in mid-air."
    # Priority: EMA9 (fastest dynamic level) → SMA20 (medium-term) → Bollinger Band (2σ)
    # For puts (bearish): price rallying back up to EMA/resistance = pullback = good add entry.
    # For calls (bullish): price dipping back to EMA/support = pullback = good add entry.
    _ema9       = safe_row_float(row, 'EMA9', 'ema9')
    _sma20_sc   = safe_row_float(row, 'SMA20')
    _lower_band = safe_row_float(row, 'LowerBand_20')
    _upper_band = safe_row_float(row, 'UpperBand_20')
    _ul_for_scale = safe_row_float(row, 'UL Last')

    # Choose best pullback level: prefer EMA9 if within 5% of price, else SMA20, else band
    # For puts: last-resort band = UpperBand_20 (resistance); for calls: LowerBand_20 (support)
    _pullback_level = 0.0
    _pullback_label = ""
    if _ema9 > 0 and _ul_for_scale > 0 and abs(_ema9 - _ul_for_scale) / _ul_for_scale < 0.05:
        _pullback_level = _ema9
        _pullback_label = "EMA9"
    elif _sma20_sc > 0:
        _pullback_level = _sma20_sc
        _pullback_label = "SMA20"
    elif _is_put and _upper_band > 0:
        _pullback_level = _upper_band
        _pullback_label = "Upper BB"
    elif not _is_put and _lower_band > 0:
        _pullback_level = _lower_band
        _pullback_label = "Lower BB"

    _pullback_str = (
        f"${_pullback_level:.2f} ({_pullback_label})"
        if _pullback_level > 0
        else "prior support/resistance level"
    )

    if _scale_up_ready:
        # ── Deterministic add-on sizing (McMillan Ch.4 + Natenberg Ch.12 + Murphy) ──────
        # Principle: decreasing size per tier, further capped by EWMA-CVaR risk budget.
        # Murphy (0.724): "Each add smaller than the last — ½ then ¼."
        # No GBM / no Monte Carlo path sampling — all inputs are point estimates.
        #
        # Step 1: Tier-aware baseline (Murphy: pyramid at decreasing size per tier)
        if _pyramid_tier == 0:
            _tier_size = max(1, int(qty / 2))    # Tier 0→1: ½-size (first add)
            _tier_label = "1/2-size (Tier 0→1)"
        elif _pyramid_tier == 1:
            _tier_size = max(1, int(qty / 4))    # Tier 1→2: ¼-size (each add SMALLER)
            _tier_label = "1/4-size (Tier 1→2)"
        else:
            _tier_size = 0  # Should not reach here due to gate (_pyramid_tier < 2)
            _tier_label = "BLOCKED (max tier)"
        #
        # Step 2: EWMA-based 1-day vol (λ=0.94, deterministic recursive formula)
        #   σ²_t = λ·σ²_{t-1} + (1-λ)·r²_t
        # Approximated from ATR_14 / (UL_price × 1.414) — same point estimate,
        # no distributional simulation.
        _atr_for_sz  = safe_row_float(row, 'atr_14')
        _ul_sz       = safe_row_float(row, 'UL Last', default=1.0)
        _ewma_1d_vol = (_atr_for_sz / (_ul_sz * 1.414)) if (_atr_for_sz > 0 and _ul_sz > 0) else 0.0
        #
        # Step 3: CVaR-implied max contracts (1.65σ adverse 1-day move, Natenberg Ch.12)
        #   dollar_CVaR_per_contract = delta_now × 100 × UL_price × 1.65 × ewma_1d_vol
        #   max_by_cvar = floor(account_2pct / dollar_CVaR_per_contract)
        # Account 2% risk proxy: use Last × 100 × qty × 0.02 (self-contained, no external lookup)
        _account_2pct    = abs(_last_price) * 100 * qty * 0.02  # 2% of current position notional
        _delta_abs       = abs(delta_now)
        _cvar_per_c      = (_delta_abs * 100 * _ul_sz * 1.65 * _ewma_1d_vol) if (_delta_abs > 0 and _ewma_1d_vol > 0) else 0.0
        _max_by_cvar     = max(1, int(_account_2pct / _cvar_per_c)) if _cvar_per_c > 0 else _tier_size
        #
        # Step 4: Portfolio delta utilization cap (McMillan Ch.3: single-ticker <15%)
        _delta_util_sc   = safe_row_float(row, 'Portfolio_Delta_Utilization_Pct')
        # Headroom above 15% cap: if already at 12%, max add = floor((15%-12%)/delta_per_c)
        # Simplified: if util≥12% → cap at 1 contract; if util≥10% → cap at tier-size
        _delta_cap = (
            1          if _delta_util_sc >= 12.0 else
            _tier_size if _delta_util_sc >= 10.0 else
            _tier_size
        )
        #
        # Step 5: Final add-on = min(all constraints), always ≥ 1
        _add_contracts = max(1, min(_tier_size, _max_by_cvar, _delta_cap))
        #
        # Sizing rationale string (surfaced in manage_view Scale Plan expander)
        _sizing_method = "EWMA-CVaR" if _cvar_per_c > 0 else "TIER-FALLBACK"
        _sizing_note = (
            f"Add-on: {_add_contracts}c "
            f"[{_tier_label}={_tier_size}c | CVaR-cap={_max_by_cvar}c | δ-util={_delta_util_sc:.1f}%] "
            f"method={_sizing_method} | Pyramid: Tier {_pyramid_tier}→{_pyramid_tier+1}"
        )

        # ── Emit SCALE_UP as a first-class doctrine action ───────────────────────────────
        # Persist Scale_Trigger_Price and Scale_Add_Contracts so the NEXT run can detect
        # when UL Last touches the trigger and fire SCALE_UP with Urgency=HIGH.
        result.update({
            "Action": "SCALE_UP",
            "Urgency": "MEDIUM",
            "Scale_Trigger_Price": round(_pullback_level, 2) if _pullback_level > 0 else None,
            "Scale_Add_Contracts": int(_add_contracts),
            "Pyramid_Tier": _pyramid_tier + 1,  # tier AFTER this add
            "Winner_Lifecycle": _winner_lifecycle,
            "Rationale": (
                f"⬆️ Scale-up (Tier {_pyramid_tier}→{_pyramid_tier+1}): "
                f"option up {option_gain_pct:.0%}, momentum {mom_state} "
                f"(RSI={_rsi_for_scale:.0f}, DTE={dte:.0f}d). "
                f"Gamma={_gamma_now:.3f} expanding (ROC_3D={_gamma_roc_3d:+.2f}) — "
                f"convexity confirms impulse phase, not exhaustion. "
                f"Conviction {_conv_status}. "
                f"Add {_add_contracts} contract(s) on pullback to {_pullback_str} — "
                f"NOT at current price. {_tier_label} preserves pyramid discipline "
                f"(Murphy: each add smaller than the last). "
                f"{_sizing_note}. "
                f"(McMillan Ch.4: Pyramid on Strength + Murphy + Natenberg Ch.12)"
                f"{carry_note}{winner_note}{iv_slope_note}{_cal_bleed_note}"
            ),
            "Doctrine_Source": "McMillan Ch.4 + Murphy: Pyramid on Strength (Tier-Aware)",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True,
        })
        return result

    # ── Trailing Protection Mode (Murphy + Nison: protect accumulated pyramid gains) ─────
    # Once a position reaches FULL_POSITION or THESIS_EXHAUSTING, no more adds.
    # Murphy (0.724): "A fully pyramided position needs only one thing: protection."
    # Nison (0.770):  "Trailing stops protect accumulated gains — don't let a winner become a loser."
    elif _winner_lifecycle == 'THESIS_EXHAUSTING' and option_gain_pct >= 0.25:
        _exhaust_urgency = "HIGH" if _conv_status == 'REVERSING' else "MEDIUM"
        result.update({
            "Action": "EXIT",
            "Urgency": _exhaust_urgency,
            "Pyramid_Tier": _pyramid_tier,
            "Winner_Lifecycle": _winner_lifecycle,
            "Scale_Trigger_Price": None,
            "Scale_Add_Contracts": 0,
            "Rationale": (
                f"Thesis exhausting: Pyramid Tier {_pyramid_tier}, momentum {mom_state}, "
                f"conviction {_conv_status}. Winner lifecycle = THESIS_EXHAUSTING. "
                f"Option up {option_gain_pct:.0%} — protect accumulated gains. "
                f"Murphy: 'Once the thesis is consumed, the pyramid must be collapsed.' "
                f"Nison: 'Trailing protection — don't let a winner become a loser.'"
                f"{carry_note}{winner_note}"
            ),
            "Doctrine_Source": "Murphy + Nison: Trailing Protection Mode",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True,
        })
        return result

    elif _winner_lifecycle == 'FULL_POSITION' and option_gain_pct >= 0.25:
        # No more scale-ups; hold with trailing protection note
        scale_note = (
            f" Pyramid complete: Tier {_pyramid_tier}/3 — no further adds. "
            f"Winner lifecycle = FULL_POSITION, conviction {_conv_status}. "
            f"Monitor for momentum shift to LATE_CYCLE/REVERSING → EXIT. "
            f"Murphy: 'A fully pyramided position needs only one thing: protection.'"
        )

    elif _scale_up_watch:
        if mom_state == 'LATE_CYCLE':
            scale_note = (
                f" ⚠️ Winner at risk: option up {option_gain_pct:.0%}, momentum LATE_CYCLE — "
                f"RSI diverging from price, ROC decelerating. "
                f"Do NOT add here. Consider trimming or preparing exit. "
                f"Natenberg Ch.11: Late-cycle divergence = edge is being consumed."
            )
        else:
            scale_note = (
                f" Winner watch: option up {option_gain_pct:.0%}, momentum ACCELERATING (parabolic). "
                f"Do NOT add — wait for momentum to settle to TRENDING and RSI to reset below 70, "
                f"then add on pullback to {_pullback_str}. "
                f"Natenberg Ch.11: Parabolic moves self-consume — adding at peak is chasing."
            )

    # Gamma-blocked scale-up: all conditions met EXCEPT convexity gate.
    # Surface this explicitly so the trader knows exactly what's missing.
    # Fires when: profitable + TRENDING + RSI<70 + structure OK + DTE>21
    #             BUT gamma is flat/contracting or too small to matter.
    # This is the most common reason a scale-up is withheld — late-trend appearance.
    elif (
        option_gain_pct >= 0.25
        and mom_state == 'TRENDING'
        and _rsi_for_scale < 70
        and not _is_extended
        and qty >= 1
        and dte > 21
        and not (_gamma_roc_3d > 0 and _gamma_now > 0.02)   # only the gamma gate failing
    ):
        _gamma_block_reason = (
            f"Gamma={_gamma_now:.3f} flat/contracting (ROC_3D={_gamma_roc_3d:+.2f})"
            if _gamma_roc_3d <= 0
            else f"Gamma={_gamma_now:.3f} too small (<0.02 threshold)"
        )
        scale_note = (
            f" Scale-up pending convexity: option up {option_gain_pct:.0%}, momentum {mom_state} "
            f"(RSI={_rsi_for_scale:.0f}) — all conditions met except gamma gate. "
            f"{_gamma_block_reason}. "
            f"Move may be maturing — pyramiding here risks buying exhaustion, not impulse. "
            f"Wait for Gamma_ROC_3D > 0 and Gamma > 0.02 before adding "
            f"(McMillan Ch.4: Convexity Confirms Impulse)."
        )

    # Scale down: multi-contract position, thesis still possible, but adverse + expensive carry
    # Only fire when: >1 contract AND losing AND carry is high AND NOT already near delta floor
    elif (qty > 1
          and pnl_pct < -0.15
          and theta_day > 0 and last_price > 0
          and (theta_day * 365) / last_price > 0.30  # carry > 30%/yr
          and delta_now > 0.20):  # still has real delta — not lottery ticket yet
        contracts_to_keep = max(1, int(qty / 2))
        scale_note = (
            f" Scale-down: {qty:.0f} contracts losing at {pnl_pct:.0%} with high carry "
            f"({(theta_day * 365) / last_price:.0%}/yr). "
            f"Cut to {contracts_to_keep} contract(s) — keeps thesis alive at minimum cost. "
            f"Re-size back up if {_pullback_str} holds and momentum recovers "
            f"(Natenberg Ch.11: Position Defense)."
        )

    # IV term structure context (Natenberg Ch.5/11)
    # BACKWARDATION: near-term IV > far-term IV — elevated short-dated vol hurts long option buyers
    # CONTANGO: normal market — confirms LEAP "buy cheap end of curve" thesis
    iv_shape = str(row.get('iv_surface_shape', '') or '').upper()
    iv_slope_note = ""
    if iv_shape == 'BACKWARDATION':
        iv_slope_30_90 = safe_row_float(row, 'iv_ts_slope_30_90')
        if is_leap:
            iv_slope_note = (
                f" IV curve BACKWARDATED ({iv_slope_30_90:+.1f}pt 30-90d slope): "
                f"near-term fear spike — LEAP thesis intact but monitor for vol normalization (Natenberg Ch.5)."
            )
        else:
            iv_slope_note = (
                f" IV curve BACKWARDATED ({iv_slope_30_90:+.1f}pt slope): "
                f"elevated near-term IV accelerates theta decay — reassess holding cost (Natenberg Ch.5)."
            )
    elif iv_shape == 'CONTANGO' and is_leap:
        iv_slope_30_90 = safe_row_float(row, 'iv_ts_slope_30_90')
        iv_slope_note = (
            f" IV in normal CONTANGO (+{iv_slope_30_90:.1f}pt 30-90d): "
            f"LEAP positioned at lower end of vol curve — favorable carry structure (Natenberg Ch.11)."
        )

    price_state_display = price_state.split('.')[-1]  # strip enum prefix if present

    # Winner context for profitable positions not yet at action gates
    winner_note = ""
    if option_gain_pct >= 0.25:
        _approaching_optimum = result.pop('_approaching_optimum', False)
        _opt_target = result.pop('_optimum_target', 0)
        _opt_src = result.pop('_optimum_source', '')
        _approach_str = ""
        if _approaching_optimum and _opt_target > 0:
            _pct_to_target = (_opt_target - _ul_last) / _ul_last
            _approach_str = (
                f" ⚠️ Approaching thesis target: stock ${_ul_last:.2f} → target ${_opt_target:.2f} "
                f"({_pct_to_target:.1%} away, {_opt_src}). "
                f"Prepare exit or partial trim — thesis satisfaction is distinct from breakdown "
                f"(McMillan Ch.4: Realize targets)."
            )
        winner_note = (
            f" ✅ Winner: option up {option_gain_pct:.0%} (${_entry_price:.2f}→${_last_price:.2f}). "
            f"Intrinsic ${_intrinsic:.2f} / time value ${_time_val:.2f} ({_tv_pct:.0%} time remaining). "
            f"Watch: if time value falls below 40% of price AND theta > $25/day → roll-up or exit "
            f"(Passarelli Ch.5).{_approach_str}"
        )

    # Pre-holiday expiry escalation: ≤7 DTE into a long weekend — upgrade urgency
    _final_urgency = "LOW"
    _final_action  = "HOLD"
    if _pre_holiday_expiry_escalate:
        _final_urgency = "MEDIUM"
        _final_action  = "ROLL"
        result.update({
            "Action": "ROLL",
            "Urgency": "MEDIUM",
            "Rationale": (
                f"Pre-holiday expiry risk: {dte:.0f} DTE into extended weekend. "
                f"Theta acceleration + multi-day non-trading gap = double bleed. "
                f"Roll before close today to avoid carrying through the break. "
                f"Hull Ch.18 + Passarelli Ch.6: pre-holiday theta is structurally costly for long premium."
                f"{_cal_bleed_note}"
            ),
            "Doctrine_Source": "Hull Ch.18 + Passarelli Ch.6: Pre-Holiday Expiry",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True
        })
        return result

    result.update({
        "Action": "HOLD",
        "Urgency": "LOW",
        "Pyramid_Tier": _pyramid_tier,
        "Winner_Lifecycle": _winner_lifecycle,
        "Rationale": (
            f"Position intact — Delta={delta_now:.2f}, DTE={dte:.0f}, structure={price_state_display}."
            f"{carry_note}{winner_note}{iv_slope_note}{scale_note}{_cal_bleed_note}{_signal_hub_notes}"
            f"{f' Wave: {_wave_phase}.' if _wave_phase and _wave_phase not in ('', 'UNKNOWN', 'NOT_APPLICABLE') else ''}"
            f" Thesis active (McMillan Ch.4)."
        ),
        "Doctrine_Source": "McMillan Ch.4: Neutral Maintenance",
        "Decision_State": STATE_NEUTRAL_CONFIDENT,
        "Required_Conditions_Met": True
    })
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# V2: Proposal-based long option evaluation
# ═══════════════════════════════════════════════════════════════════════════════

from ..proposal import ProposalCollector, propose_gate
from ..comparators.action_ev_long import (
    compare_actions_long_option,
    resolve_directional_proposals,
)


def long_option_doctrine_v2(row: pd.Series, result: Dict) -> Dict:
    """Proposal-based LONG_OPTION / LEAPS evaluation.

    All gates propose actions into a ProposalCollector instead of returning
    immediately.  A resolver picks the best action using deterministic EV
    and MC evidence.

    Original ``long_option_doctrine()`` is preserved unchanged for A/B testing.
    """
    collector = ProposalCollector()

    # ── Extract fields (same as v1) ──────────────────────────────────────
    price_state = str(row.get('PriceStructure_State', 'UNKNOWN') or 'UNKNOWN').upper()
    price_drift = safe_row_float(row, 'Price_Drift_Pct')
    delta_entry = safe_row_float(row, 'Delta_Entry')
    delta_now = abs(safe_row_float(row, 'Delta'))
    dte = row.get('DTE', 999)
    dte = dte if dte is not None and not (isinstance(dte, float) and pd.isna(dte)) else 999
    strategy = str(row.get('Strategy', '') or '').upper()
    is_leap = 'LEAP' in strategy or dte >= 180
    ticker_net_delta = safe_row_float(row, '_Ticker_Net_Delta')
    ticker_has_stock = bool(row.get('_Ticker_Has_Stock', False))

    # Pyramid tier + winner lifecycle defaults
    result["Pyramid_Tier"] = int(row.get('Pyramid_Tier', 0) or 0)
    result["Winner_Lifecycle"] = str(row.get('Winner_Lifecycle', 'THESIS_UNPROVEN') or 'THESIS_UNPROVEN')

    # ── Calendar context ────────────────────────────────────────────────
    _pre_holiday_expiry_escalate = False
    _cal_bleed_note = ""
    try:
        from scan_engine.calendar_context import expiry_proximity_flag, get_calendar_context
        _ul_last_cal = safe_row_float(row, 'UL Last')
        _strike_cal = safe_row_float(row, 'Strike')
        _exp_flag, _exp_note = expiry_proximity_flag(
            dte=dte, strategy=strategy,
            ul_last=_ul_last_cal, strike=_strike_cal,
        )
        if _exp_flag == 'PIN_RISK':
            propose_gate(
                collector, "pin_risk",
                action="EXIT", urgency="CRITICAL",
                rationale=_exp_note,
                doctrine_source="McMillan Ch.7 + Natenberg Ch.15: Pin Risk",
                priority=1, is_hard_veto=True, exit_trigger_type="GAMMA",
            )
        elif _exp_flag == 'GAMMA_CRITICAL':
            propose_gate(
                collector, "gamma_critical",
                action="ROLL", urgency="HIGH",
                rationale=_exp_note,
                doctrine_source="Natenberg Ch.15: Gamma Critical — Force Decision",
                priority=5,
            )
        elif _exp_flag == 'PRE_HOLIDAY_EXPIRY':
            _pre_holiday_expiry_escalate = True

        _cal_ctx = get_calendar_context()
        if _cal_ctx.is_pre_long_weekend and not is_leap and dte <= 21:
            _cal_bleed_note = (
                f" ⚠️ Calendar: {_cal_ctx.theta_bleed_days} non-trading days ahead — "
                f"long premium bleeds theta with no stock movement. "
                f"Passarelli Ch.6: pre-holiday hold cost is {_cal_ctx.theta_bleed_days}× daily theta."
            )
    except Exception as _cal_err:
        logger.debug(f"Calendar context skipped for long option v2: {_cal_err}")

    # ── Scale_Trigger_Price re-check ────────────────────────────────────
    _prior_trigger = row.get('Scale_Trigger_Price')
    _prior_add_c = row.get('Scale_Add_Contracts')
    _ul_now_sc = safe_row_float(row, 'UL Last')
    if (
        pd.notna(_prior_trigger)
        and float(_prior_trigger or 0) > 0
        and _ul_now_sc > 0
    ):
        _prior_trigger_f = float(_prior_trigger)
        _prior_add_c_i = int(_prior_add_c or 1)
        _strat_sc = str(row.get('Strategy', '') or '').upper()
        _is_long_put_sc = any(s in _strat_sc for s in ('LONG_PUT', 'BUY_PUT', 'LEAPS_PUT'))
        _is_long_call_sc = any(s in _strat_sc for s in ('LONG_CALL', 'BUY_CALL', 'LEAPS_CALL'))
        _trigger_touched = (
            (_is_long_call_sc and _ul_now_sc <= _prior_trigger_f * 1.005) or
            (_is_long_put_sc and _ul_now_sc >= _prior_trigger_f * 0.995) or
            (not _is_long_call_sc and not _is_long_put_sc and abs(_ul_now_sc - _prior_trigger_f) / _prior_trigger_f <= 0.005)
        )
        if _trigger_touched:
            propose_gate(
                collector, "scale_trigger",
                action="SCALE_UP", urgency="HIGH",
                rationale=(
                    f"⬆️🎯 Scale trigger reached: UL=${_ul_now_sc:.2f} touched pullback level "
                    f"${_prior_trigger_f:.2f}. "
                    f"Add {_prior_add_c_i} contract(s) now — pullback-to-support entry "
                    f"confirmed (McMillan Ch.4: Pyramid on Strength, act on the pullback)."
                ),
                doctrine_source="McMillan Ch.4: Scale Trigger Activated",
                priority=8,
                Scale_Trigger_Price=round(_prior_trigger_f, 2),
                Scale_Add_Contracts=_prior_add_c_i,
            )

    # ── Portfolio delta redundancy ──────────────────────────────────────
    _pnl_quick = safe_pnl_pct(row) or 0.0
    _delta_redundancy_threshold = -0.35 if is_leap else -0.15
    _qty_for_redundancy = abs(safe_row_float(row, 'Quantity', default=1.0))
    if ticker_has_stock and ticker_net_delta > 0.8 and _pnl_quick < _delta_redundancy_threshold:
        if _qty_for_redundancy <= 1:
            propose_gate(
                collector, "portfolio_delta_redundancy",
                action="EXIT", urgency="LOW",
                rationale=(
                    f"Ticker net delta={ticker_net_delta:.2f} already long from stock. "
                    f"Long call adds redundant directional exposure at a loss — close this position. "
                    f"Single contract: no further trimming possible; EXIT to remove overlap "
                    f"(McMillan Ch.4: Portfolio Delta Management)."
                ),
                doctrine_source="McMillan Ch.4: Portfolio Delta Management",
                priority=10,
            )
        else:
            propose_gate(
                collector, "portfolio_delta_redundancy",
                action="TRIM", urgency="LOW",
                rationale=(
                    f"Ticker net delta={ticker_net_delta:.2f} already long from stock. "
                    f"Long call adds redundant directional exposure at a loss — consider trimming "
                    f"(McMillan Ch.4: Portfolio Delta Management)."
                ),
                doctrine_source="McMillan Ch.4: Portfolio Delta Management",
                priority=10,
            )

    # ── Roll cooldown ───────────────────────────────────────────────────
    _COOLDOWN_DAYS_LO = 1
    _days_since_roll_lo = row.get('Days_Since_Last_Roll')
    _thesis_for_cooldown_lo = str(row.get('Thesis_State', '') or '').upper()
    if (
        pd.notna(_days_since_roll_lo)
        and float(_days_since_roll_lo) < _COOLDOWN_DAYS_LO
        and _thesis_for_cooldown_lo in ('INTACT', 'UNKNOWN', '')
    ):
        propose_gate(
            collector, "roll_cooldown",
            action="HOLD", urgency="LOW",
            rationale=(
                f"Recently-rolled cooldown: current leg opened {int(_days_since_roll_lo)}d ago "
                f"(< {_COOLDOWN_DAYS_LO}d window). Thesis is {_thesis_for_cooldown_lo or 'UNKNOWN'} — "
                f"suppressing discretionary ROLL to prevent same-day flip-flop. "
                f"Natenberg Ch.7: 'Frequent adjustments cost more than the risk they mitigate.' "
                f"Jabbour Ch.8: 'Repair is a dangerous misnomer for overtrading.'"
            ),
            doctrine_source="Natenberg Ch.7 + Jabbour Ch.8: Recently-Rolled Cooldown",
            priority=25,
        )

    # ── Structure broken ────────────────────────────────────────────────
    _theta_bleed = safe_row_float(row, 'Theta_Bleed_Daily_Pct')
    if "STRUCTURE_BROKEN" in price_state and price_drift < -0.05:
        if is_leap:
            propose_gate(
                collector, "structure_broken_leap",
                action="EXIT", urgency="HIGH",
                rationale=(
                    f"Technical structure broken ({price_state}) with {price_drift:.1%} adverse drift. "
                    f"BUT DTE={dte:.0f} — LEAP has time for structure repair. "
                    f"Theta bleed {_theta_bleed:.1%}/day is manageable. "
                    f"Revalidate thesis: if conviction lost, exit on next bounce. "
                    f"If thesis intact, structure may rebuild (McMillan Ch.4: LEAPs tolerate "
                    f"structural breaks that short-dated options cannot)."
                ),
                doctrine_source="McMillan Ch.4: Structural Exit",
                priority=12,
            )
        else:
            propose_gate(
                collector, "structure_broken",
                action="EXIT", urgency="CRITICAL",
                rationale=(
                    f"Technical structure broken ({price_state}) with {price_drift:.1%} adverse drift. "
                    f"Directional thesis invalidated (McMillan Ch.4)."
                ),
                doctrine_source="McMillan Ch.4: Structural Exit",
                priority=3, is_hard_veto=True, exit_trigger_type="CAPITAL",
            )

    # ── Delta collapse ──────────────────────────────────────────────────
    if delta_entry > 0:
        sensitivity_ratio = delta_now / delta_entry
        if sensitivity_ratio < 0.40:
            propose_gate(
                collector, "delta_collapse",
                action="EXIT", urgency="HIGH",
                rationale=(
                    f"Delta collapsed to {sensitivity_ratio:.0%} of entry "
                    f"({delta_entry:.2f}→{delta_now:.2f}). Position now lottery-ticket risk "
                    f"(Passarelli Ch.2)."
                ),
                doctrine_source="Passarelli Ch.2: Greek Drift",
                priority=15,
            )

    # ── Shared computed values ──────────────────────────────────────────
    pnl_pct = safe_pnl_pct(row) or 0.0

    _last_raw = safe_row_float(row, 'Last')
    _bid_raw = safe_row_float(row, 'Bid')
    _last_price = max(_last_raw, _bid_raw) if _bid_raw > 0 else _last_raw
    _entry_price = abs(safe_row_float(row, 'Premium_Entry', default=0.0))
    option_gain_pct = (_last_price - _entry_price) / _entry_price if _entry_price > 0 else 0.0

    _pyramid_tier = int(row.get('Pyramid_Tier', 0) or 0)
    _winner_lifecycle = str(row.get('Winner_Lifecycle', 'THESIS_UNPROVEN') or 'THESIS_UNPROVEN').upper()

    _ul_last = safe_row_float(row, 'UL Last', 'Underlying_Price_Entry')
    _strike = safe_row_float(row, 'Strike')
    _cp = str(row.get('Call/Put', '') or '').upper()
    _is_put = 'P' in _cp
    _option_type_label = "long put" if _is_put else "long call"
    _intrinsic = max(0.0, _strike - _ul_last) if _is_put else max(0.0, _ul_last - _strike)
    _time_val = max(0.0, _last_price - _intrinsic)
    _tv_pct = (_time_val / _last_price) if _last_price > 0 else 0.0

    # Direction-awareness — sigma-normalized (same helper as v1)
    _drift_raw_da = row.get('Drift_Direction', '') or ''
    drift_dir = (getattr(_drift_raw_da, 'value', None) or str(_drift_raw_da).split('.')[-1]).upper()
    _roc5 = safe_row_float(row, 'roc_5')
    _roc10 = safe_row_float(row, 'roc_10')
    _hv_20d = safe_row_float(row, 'HV_20D') if pd.notna(row.get('HV_20D')) else 0.0
    _adverse_drift_dir = 'UP' if _is_put else 'DOWN'
    from core.management.cycle3.doctrine.helpers import compute_direction_adverse_signals
    _roc5_adverse, _drift_is_adverse, _roc5_z, _drift_z, _used_sigma = (
        compute_direction_adverse_signals(_roc5, price_drift, _hv_20d, _is_put)
    )
    # Persist z-scores for audit trail (queryable in management_recommendations)
    result["ROC5_Z"] = _roc5_z
    result["Drift_Z"] = _drift_z
    result["Sigma_Mode"] = _used_sigma
    if _used_sigma:
        _sigma_tag = (f" [σ-mode: roc5_z={_roc5_z:+.1f}, drift_z={_drift_z:+.1f}, "
                      f"HV={_hv_20d:.0%}]")
    else:
        _sigma_tag = " [⚠ HV_20D missing — direction signals indeterminate]"

    # Entry quality
    _entry_ps = str(row.get('Entry_Chart_State_PriceStructure', '') or '').upper()
    _entry_ti = str(row.get('Entry_Chart_State_TrendIntegrity', '') or '').upper()
    if _is_put:
        _strong_entry = _entry_ps in ('STRUCTURAL_DOWN',) and _entry_ti in ('STRONG_TREND', 'WEAK_TREND')
    else:
        _strong_entry = _entry_ps in ('STRUCTURAL_UP',) and _entry_ti in ('STRONG_TREND', 'WEAK_TREND')
    _weak_entry = _entry_ps in ('RANGE_BOUND', 'NO_TREND', 'UNKNOWN', '') or _entry_ti in ('NO_TREND', 'UNKNOWN', '')

    if _strong_entry:
        _entry_quality = 'STRONG'; _pnl_threshold_da = -0.25
    elif _weak_entry:
        _entry_quality = 'WEAK'; _pnl_threshold_da = -0.10
    else:
        _entry_quality = 'NEUTRAL'; _pnl_threshold_da = -0.15

    days_held = safe_row_float(row, 'Days_In_Trade')
    original_dte = dte + days_held

    # ── Thesis staleness ────────────────────────────────────────────────
    _time_fraction = 2 if _entry_quality == 'STRONG' else (4 if _entry_quality == 'WEAK' else 3)
    from core.management.cycle3.doctrine.thresholds import SIGMA_DRIFT_STALENESS_Z
    _drift_is_flat_v2 = (abs(_drift_z) < SIGMA_DRIFT_STALENESS_Z if _used_sigma
                         else False)  # HV missing: indeterminate — don't gate
    if (original_dte > 0
            and days_held >= original_dte / _time_fraction
            and _drift_is_flat_v2
            and pnl_pct < -0.30):
        propose_gate(
            collector, "thesis_staleness",
            action="ROLL", urgency="MEDIUM",
            rationale=(
                f"Thesis not confirming: {days_held:.0f}d held ({days_held/original_dte:.0%} of life), "
                f"drift only {price_drift:+.1%}, P&L={pnl_pct:.0%}. "
                f"Roll to later expiry or exit if conviction lost (McMillan Ch.4: Time-to-be-Right)."
            ),
            doctrine_source="McMillan Ch.4: Time-to-be-Right",
            priority=18,
        )

    # ── Entry-trend invalidation ────────────────────────────────────────
    _current_ti = str(row.get('TrendIntegrity_State', '') or '').upper()
    if (not is_leap
            and _entry_ti in ('STRONG_TREND', 'WEAK_TREND')
            and _current_ti in ('NO_TREND', 'TREND_EXHAUSTED', '')
            and pnl_pct < 0):
        propose_gate(
            collector, "entry_trend_invalidation",
            action="EXIT", urgency="HIGH",
            rationale=(
                f"TREND_INVALIDATED — entry trend was {_entry_ti}, now {_current_ti or 'UNKNOWN'}. "
                f"Directional thesis is structurally broken (not degraded). "
                f"P&L={pnl_pct:.0%}, DTE={dte}. "
                f"Entry quality: {_entry_quality} (leash: {_pnl_threshold_da:.0%}). "
                f"(Natenberg Ch.5: position no longer sensible under new conditions.)"
            ),
            doctrine_source="Natenberg Ch.5 + Audit Feb-2026",
            priority=20,
        )

    # ── Direction-adverse thesis confirmation ───────────────────────────
    _any_adverse_signal = (_roc5_adverse or _drift_is_adverse)
    _dir_adverse_detected = (
        _any_adverse_signal
        and dte < 45
        and pnl_pct < _pnl_threshold_da
    )
    _recovery_raw_da = str(row.get('Recovery_Feasibility', '') or '').upper()
    _already_impossible = _recovery_raw_da == 'IMPOSSIBLE'

    if _dir_adverse_detected and not _already_impossible and not is_leap:
        # New-position grace: scan engine just recommended this trade.
        # Priority 10 beats direction_adverse_exit (priority 22) in the resolver.
        # Not a hard veto — catalyst hold and SRS gates can still propose at
        # their own priorities. Only applies when loss is within reason — a -40%+
        # gap move on day 0 means something catastrophic happened, no grace.
        if days_held < 2 and (pnl_pct is None or pnl_pct > -0.40):
            propose_gate(
                collector, "direction_adverse_new_position_grace",
                action="HOLD", urgency="MEDIUM",
                is_hard_veto=True,
                rationale=(
                    f"Direction ADVERSE for {_option_type_label}: "
                    f"stock {'rallying' if _is_put else 'falling'} (ROC5={_roc5:+.1f}%, "
                    f"Drift={drift_dir}, Price_Drift={price_drift:+.1%}), P&L={pnl_pct:.0%}. "
                    f"However: position opened {days_held:.0f}d ago — scan engine evaluated this "
                    f"price action and recommended entry. Grace period: reassess after day 2. "
                    f"Entry quality: {_entry_quality} (leash: {_pnl_threshold_da:.0%}). "
                    f"(McMillan Ch.4: new positions need time to develop thesis.)"
                ),
                doctrine_source="McMillan Ch.4: Direction Adverse — New Position Grace",
                priority=10,
            )

        _thesis_state_da = str(row.get('Thesis_State', '') or '').upper()
        _conv_status_da = str(row.get('Conviction_Status', '') or '').upper()

        _has_catalyst = False
        _catalyst_label = ""
        try:
            _earn_date = row.get('Earnings_Date')
            _snap_ts = row.get('Snapshot_TS')
            if _earn_date and _snap_ts:
                _earn_dt = pd.to_datetime(_earn_date)
                _snap_dt = pd.to_datetime(_snap_ts)
                _days_to_earn = (_earn_dt.normalize() - _snap_dt.normalize()).days
                if 0 < _days_to_earn <= 14:
                    _has_catalyst = True
                    _catalyst_label = f"earnings in {_days_to_earn}d"
        except Exception:
            pass

        if not _has_catalyst:
            try:
                _macro_days = safe_row_float(row, 'Days_To_Macro', default=99.0)
                _macro_impact = str(row.get('Macro_Impact', '') or '').upper()
                _macro_type = str(row.get('Macro_Next_Type', '') or '')
                if _macro_impact == 'HIGH' and 0 <= _macro_days <= 5:
                    _has_catalyst = True
                    _catalyst_label = f"{_macro_type} in {_macro_days:.0f}d"
            except Exception:
                pass

        _escape = (
            _thesis_state_da == 'INTACT'
            and _conv_status_da in ('STABLE', 'STRENGTHENING')
            and _has_catalyst
        )

        if _escape:
            propose_gate(
                collector, "direction_adverse_catalyst_hold",
                action="HOLD", urgency="HIGH",
                rationale=(
                    f"Direction ADVERSE for {_option_type_label}: "
                    f"stock {'rallying' if _is_put else 'falling'} (ROC5={_roc5:+.1f}%, "
                    f"Drift={drift_dir}, Price_Drift={price_drift:+.1%}). "
                    f"However: Thesis={_thesis_state_da}, Conviction={_conv_status_da}, "
                    f"{_catalyst_label} — potential reversal catalyst. "
                    f"HOLD with tight review — exit if catalyst fails to reverse direction. "
                    f"Entry quality: {_entry_quality} (leash: {_pnl_threshold_da:.0%}). "
                    f"(Given Ch.9: time stop with catalyst exception; "
                    f"Natenberg Ch.5: forward probability shifts with catalyst.)"
                ),
                doctrine_source="Given Ch.9 + Natenberg Ch.5: Direction Adverse — Catalyst Hold",
                priority=10,
            )
        else:
            # SRS modulation
            _srs_raw = str(row.get('Sector_Relative_Strength', '') or '').upper()
            _srs_z = safe_row_float(row, 'Sector_RS_ZScore') if pd.notna(row.get('Sector_RS_ZScore')) else 0.0
            _srs_bench = str(row.get('Sector_Benchmark', 'SPY') or 'SPY')
            _prior_action_da = str(row.get('Prior_Action', '') or '').upper()
            _exp_current = str(row.get('Expiration', '') or '')
            _exp_entry = str(row.get('Expiration_Entry', '') or '')
            _already_rolled = (_exp_current != _exp_entry and _exp_entry != '' and _exp_current != '')
            _srs_override = (pnl_pct < -0.40 or _already_rolled or dte < 10)
            _srs_favorable = _srs_raw in ('UNDERPERFORMING', 'MICRO_BREAKDOWN', 'BROKEN')

            if _srs_favorable and not _srs_override:
                if _srs_raw in ('MICRO_BREAKDOWN', 'BROKEN'):
                    propose_gate(
                        collector, "direction_adverse_srs_intact",
                        action="HOLD", urgency="HIGH",
                        rationale=(
                            f"Direction ADVERSE for {_option_type_label}: "
                            f"stock {'rallying' if _is_put else 'falling'} (ROC5={_roc5:+.1f}%, "
                            f"Price_Drift={price_drift:+.1%}), P&L={pnl_pct:.0%}. "
                            f"BUT sector-relative thesis REINFORCED: "
                            f"SRS={_srs_raw} (z={_srs_z:+.1f}) vs {_srs_bench}. "
                            f"Stock significantly underperforming sector — adverse move is "
                            f"market-driven, not thesis failure. "
                            f"Entry quality: {_entry_quality}. "
                            f"(Sinclair: exit when wrong, not when losing; "
                            f"Natenberg: sector context modulates risk assessment.)"
                        ),
                        doctrine_source="Sinclair + Natenberg: Direction Adverse — SRS Thesis Intact",
                        priority=22,
                    )
                else:
                    _iv_pctile_da = safe_row_float(row, 'IV_Percentile', default=50.0) if pd.notna(row.get('IV_Percentile')) else 50.0
                    _iv_depth_da = int(safe_row_float(row, 'IV_Percentile_Depth')) if pd.notna(row.get('IV_Percentile_Depth')) else 0
                    _iv_pctile_reliable = _iv_depth_da >= 45
                    _iv_gap_srs = row.get('IV_vs_HV_Gap')
                    _iv_gap_srs_valid = pd.notna(_iv_gap_srs)
                    _iv_gap_srs = float(_iv_gap_srs) if _iv_gap_srs_valid else 0.0
                    if _iv_pctile_reliable:
                        _iv_srs_affordable = _iv_pctile_da <= 50
                    else:
                        _iv_srs_affordable = (not _iv_gap_srs_valid) or (_iv_gap_srs <= 0)
                    _roll_affordable = _iv_srs_affordable and dte <= 30
                    if _roll_affordable:
                        propose_gate(
                            collector, "direction_adverse_srs_roll",
                            action="ROLL", urgency="MEDIUM",
                            rationale=(
                                f"Direction ADVERSE for {_option_type_label}: "
                                f"stock {'rallying' if _is_put else 'falling'} (ROC5={_roc5:+.1f}%, "
                                f"Price_Drift={price_drift:+.1%}), P&L={pnl_pct:.0%}. "
                                f"Sector-relative thesis intact: SRS={_srs_raw} "
                                f"(z={_srs_z:+.1f}) vs {_srs_bench}. "
                                f"Roll to 60+ DTE. Entry quality: {_entry_quality}. "
                                f"(Bennett: extend thesis runway when thesis intact.)"
                            ),
                            doctrine_source="Bennett + Sinclair: Direction Adverse — SRS Roll-for-Time",
                            priority=22,
                        )
                    else:
                        propose_gate(
                            collector, "direction_adverse_srs_hold_unavail",
                            action="HOLD", urgency="HIGH",
                            rationale=(
                                f"Direction ADVERSE for {_option_type_label}: "
                                f"stock {'rallying' if _is_put else 'falling'} (ROC5={_roc5:+.1f}%, "
                                f"Price_Drift={price_drift:+.1%}), P&L={pnl_pct:.0%}. "
                                f"Sector-relative thesis intact: SRS={_srs_raw} "
                                f"(z={_srs_z:+.1f}) vs {_srs_bench}. "
                                f"Roll not affordable. HOLD with tight review. "
                                f"Entry quality: {_entry_quality}. "
                                f"(Sinclair: relative weakness is not thesis failure.)"
                            ),
                            doctrine_source="Sinclair: Direction Adverse — SRS HOLD (roll unavailable)",
                            priority=22,
                        )
            else:
                # SRS = OUTPERFORMING or NEUTRAL or override
                _mom_slope_da = safe_row_float(row, 'momentum_slope')
                _confirming_signal = (_mom_slope_da < 0 if _is_put else _mom_slope_da > 0)
                _ev_ratio_da = safe_row_float(row, 'EV_Feasibility_Ratio') if pd.notna(row.get('EV_Feasibility_Ratio')) else float('nan')
                _iv_pctile_da = safe_row_float(row, 'IV_Percentile', default=50.0) if pd.notna(row.get('IV_Percentile')) else 50.0
                _iv_depth_da = int(safe_row_float(row, 'IV_Percentile_Depth')) if pd.notna(row.get('IV_Percentile_Depth')) else 0
                _iv_pctile_reliable = _iv_depth_da >= 45
                _iv_gap_for_roll = row.get('IV_vs_HV_Gap')
                _iv_gap_valid = pd.notna(_iv_gap_for_roll)
                _iv_gap_for_roll = float(_iv_gap_for_roll) if _iv_gap_valid else 0.0
                if _iv_pctile_reliable:
                    _iv_roll_affordable = _iv_pctile_da <= 50
                else:
                    _iv_roll_affordable = (not _iv_gap_valid) or (_iv_gap_for_roll <= 0)

                _roll_conditions = (
                    _thesis_state_da == 'INTACT'
                    and _confirming_signal
                    and (not pd.isna(_ev_ratio_da) and _ev_ratio_da < 1.0)
                    and _iv_roll_affordable
                    and dte <= 30
                    and not _already_rolled
                )

                _srs_note = f" SRS={_srs_raw} (z={_srs_z:+.1f}) vs {_srs_bench} — no relative edge." if _srs_raw else ""

                if _roll_conditions:
                    propose_gate(
                        collector, "direction_adverse_roll",
                        action="ROLL", urgency="MEDIUM",
                        rationale=(
                            f"Direction ADVERSE for {_option_type_label}: "
                            f"stock {'rallying' if _is_put else 'falling'} (ROC5={_roc5:+.1f}%, "
                            f"Drift={drift_dir}, Price_Drift={price_drift:+.1%}), P&L={pnl_pct:.0%}. "
                            f"Thesis INTACT with confirming signal (slope={_mom_slope_da:+.3f}).{_srs_note} "
                            f"Roll to 60+ DTE. Entry quality: {_entry_quality}. "
                            f"(Bennett + Lopez de Prado: extend thesis runway.)"
                        ),
                        doctrine_source="Bennett + Lopez de Prado: Direction Adverse Roll-for-Time",
                        priority=22,
                    )
                else:
                    # EV Feasibility Escape check
                    _both_adverse = (_roc5_adverse and _drift_is_adverse)
                    _original_dte = safe_row_float(row, 'DTE_Entry')
                    _time_remaining_pct = (dte / _original_dte) if _original_dte > 0 else 0.0
                    _ev_feasible = (not pd.isna(_ev_ratio_da) and _ev_ratio_da < 0.50)
                    _time_has_room = _time_remaining_pct >= 0.50

                    if (not _both_adverse and _ev_feasible and _time_has_room and not _srs_override):
                        propose_gate(
                            collector, "direction_adverse_ev_feasible_hold",
                            action="HOLD", urgency="MEDIUM",
                            rationale=(
                                f"Direction ADVERSE (marginal) for {_option_type_label}: "
                                f"stock {'rallying' if _is_put else 'falling'} (ROC5={_roc5:+.1f}%, "
                                f"Price_Drift={price_drift:+.1%}), P&L={pnl_pct:.0%}. "
                                f"BUT breakeven feasible: EV_Ratio={_ev_ratio_da:.2f}× "
                                f"(< 0.50× expected move). Time remaining: {_time_remaining_pct:.0%}. "
                                f"HOLD with TIME STOP.{_srs_note} "
                                f"Entry quality: {_entry_quality}. "
                                f"(Nison + Chan + Given: EV Feasible Hold.)"
                            ),
                            doctrine_source="Nison + Chan + Given: EV Feasible — Hold with Time Stop",
                            priority=22,
                        )
                    else:
                        # Standard EXIT — roll blocked
                        _fail_reasons = []
                        if _thesis_state_da != 'INTACT':
                            _fail_reasons.append(f"Thesis={_thesis_state_da}")
                        if not _confirming_signal:
                            _fail_reasons.append(f"no confirming momentum (slope={_mom_slope_da:+.3f})")
                        if pd.isna(_ev_ratio_da) or _ev_ratio_da >= 1.0:
                            _fail_reasons.append(f"EV_Ratio={'N/A' if pd.isna(_ev_ratio_da) else f'{_ev_ratio_da:.2f}'}")
                        if not _iv_roll_affordable:
                            if _iv_pctile_reliable:
                                _fail_reasons.append(f"IV_Pctile={_iv_pctile_da:.0f}% (expensive to roll)")
                            elif _iv_gap_valid:
                                _fail_reasons.append(
                                    f"IV gap={_iv_gap_for_roll:+.1f}% with {_iv_depth_da}d history — "
                                    f"no vol edge for buyer (Jabbour: reevaluate as fresh entry)"
                                )
                            else:
                                _fail_reasons.append("IV data insufficient to confirm roll affordability")
                        if dte > 30:
                            _fail_reasons.append(f"DTE={dte:.0f} (not in theta acceleration zone)")
                        if _already_rolled:
                            _fail_reasons.append("already rolled once")
                        if _both_adverse:
                            _fail_reasons.append("BOTH adverse signals confirm direction")

                        propose_gate(
                            collector, "direction_adverse_exit",
                            action="EXIT", urgency="MEDIUM",
                            rationale=(
                                f"Direction ADVERSE for {_option_type_label}: "
                                f"stock {'rallying' if _is_put else 'falling'} (ROC5={_roc5:+.1f}%, "
                                f"Drift={drift_dir}, Price_Drift={price_drift:+.1%}), P&L={pnl_pct:.0%}.{_sigma_tag} "
                                f"Roll blocked: {'; '.join(_fail_reasons)}.{_srs_note} "
                                f"Entry quality: {_entry_quality} (leash: {_pnl_threshold_da:.0%}). "
                                f"(Natenberg Ch.5 + Jabbour Ch.7: Direction Adverse EXIT.)"
                            ),
                            doctrine_source="Natenberg Ch.5 + Jabbour Ch.7: Direction Adverse EXIT",
                            priority=22,
                        )

    # ── Theta dominance ─────────────────────────────────────────────────
    _greek_raw = row.get('GreekDominance_State', '') or ''
    greek_dom = (getattr(_greek_raw, 'value', None) or str(_greek_raw).split('.')[-1]).upper()
    _mom_raw = row.get('MomentumVelocity_State', '') or ''
    mom_state = (getattr(_mom_raw, 'value', None) or str(_mom_raw).split('.')[-1]).upper()
    if (greek_dom == 'THETA_DOMINANT'
            and drift_dir in ('FLAT', _adverse_drift_dir)
            and mom_state in ('STALLING', 'REVERSING')
            and dte <= 60
            and pnl_pct < -0.20):
        _tc_trade_count = int(row.get('_Ticker_Trade_Count', 1) or 1)
        _tc_structure = str(row.get('_Ticker_Structure_Class', '') or '')
        _income_structures = {"CALL_DIAGONAL", "PUT_DIAGONAL", "INCOME_WITH_LEGS", "SINGLE_LEG"}
        _is_income_leg = _tc_structure in _income_structures
        _leg_theta = safe_row_float(row, 'Theta')

        if _tc_trade_count > 1 and not is_leap and not _is_income_leg:
            _leg_daily_cost = abs(_leg_theta) * 100
            _tc_net_theta = safe_row_float(row, '_Ticker_Net_Theta')
            _theta_daily_cost = abs(_tc_net_theta) * 100
            _tc_ticker = str(row.get('Underlying_Ticker', '') or '')
            propose_gate(
                collector, "theta_dominance_multileg_exit",
                action="EXIT", urgency="MEDIUM",
                rationale=(
                    f"THETA_DOMINANT at DTE={dte:.0f} with {mom_state} momentum — no directional payoff. "
                    f"This leg costs ${_leg_daily_cost:.0f}/day theta. "
                    f"Collective {_tc_ticker} structure ({_tc_structure}) bleeds "
                    f"${_theta_daily_cost:.0f}/day. "
                    f"Exit THIS leg to stop the short-dated theta leak. "
                    f"(Natenberg Ch.4: leg-level theta management.)"
                ),
                doctrine_source="Natenberg Ch.4: Multi-Leg Theta Management",
                priority=30,
            )
        else:
            propose_gate(
                collector, "theta_dominance",
                action="HOLD", urgency="MEDIUM",
                rationale=(
                    f"THETA_DOMINANT with flat price ({drift_dir}) and {mom_state} momentum at DTE={dte:.0f}. "
                    f"Theta consuming remaining premium without directional payoff — monitor closely. "
                    f"Exit if no catalyst within 10d (Passarelli Ch.2: Theta Awareness)."
                ),
                doctrine_source="Passarelli Ch.2: Theta Awareness",
                priority=30,
            )

    # ── Time-to-impulse ─────────────────────────────────────────────────
    _tti_dte_raw = row.get('DTE', 999) or 999
    try:
        _tti_dte = float(_tti_dte_raw)
    except (TypeError, ValueError):
        _tti_dte = 999.0

    _tti_greek_raw = row.get('GreekDominance_State', '') or ''
    _tti_greek = (getattr(_tti_greek_raw, 'value', None) or str(_tti_greek_raw).split('.')[-1]).upper()
    _tti_theta_dominant = (_tti_greek == 'THETA_DOMINANT')

    _tti_comp_raw = str(row.get('CompressionMaturity', '') or '').split('.')[-1].upper()
    _tti_bb_z = safe_row_float(row, 'bb_width_z')
    _tti_compressing = (
        _tti_bb_z < -0.5
        or _tti_comp_raw in ('EARLY_COMPRESSION', 'MID_COMPRESSION', 'MATURE_COMPRESSION')
    )

    _tti_roc5 = safe_row_float(row, 'roc_5')
    _tti_roc10 = safe_row_float(row, 'roc_10')
    _tti_no_momentum = (_tti_roc5 <= 0 and _tti_roc10 <= 0)

    _tti_chop = safe_row_float(row, 'choppiness_index', default=50.0)
    _tti_adx = safe_row_float(row, 'adx_14', default=25.0)
    _tti_range_bound = (_tti_chop > 55 or _tti_adx < 18)

    _tti_mom_vel_raw = str(row.get('MomentumVelocity_State', '') or '').split('.')[-1].upper()
    _tti_bottoming = (_tti_mom_vel_raw == 'REVERSING' and safe_row_float(row, 'rsi_14', default=50.0) < 42)

    _tti_last_raw = safe_row_float(row, 'Last')
    _tti_bid_raw = safe_row_float(row, 'Bid')
    _tti_last_p = max(_tti_last_raw, _tti_bid_raw) if _tti_bid_raw > 0 else _tti_last_raw
    _tti_entry_p = abs(safe_row_float(row, 'Premium_Entry', default=0.0))
    _tti_option_gain = (_tti_last_p - _tti_entry_p) / _tti_entry_p if _tti_entry_p > 0 else 0.0

    _tti_is_leap_local = 'LEAP' in str(row.get('Strategy', '') or '').upper() or _tti_dte >= 180
    _tti_dte_in_zone = (
        (_tti_dte <= 60 and not _tti_is_leap_local)
        or (_tti_dte <= 180 and _tti_is_leap_local)
    )

    _tti_compression_direction_ok = (_tti_compressing and not _drift_is_adverse)
    _tti_exempt = (
        _tti_option_gain > 0.15
        or _tti_compression_direction_ok
        or _tti_bottoming
        or (pnl_pct < -0.20 and _tti_theta_dominant)
    )

    if (not _tti_exempt
            and _tti_dte_in_zone
            and _tti_theta_dominant
            and _tti_no_momentum
            and _tti_range_bound):
        _tti_theta_per_day = abs(safe_row_float(row, 'Theta', default=0.0))
        if _tti_theta_per_day > 0 and _tti_last_p > 0:
            _tti_20pct_budget = (_tti_last_p * 0.20) / _tti_theta_per_day
            _tti_budget_str = f"{int(_tti_20pct_budget)}d"
        else:
            _tti_budget_str = f"{max(5, int(_tti_dte // 4))}d"

        _tti_urgency = "HIGH" if _tti_dte <= 30 else "MEDIUM"
        _tti_trade_count = int(row.get('_Ticker_Trade_Count', 1) or 1)
        _tti_structure = str(row.get('_Ticker_Structure_Class', '') or '')
        _tti_income_structures = {"CALL_DIAGONAL", "PUT_DIAGONAL", "INCOME_WITH_LEGS", "SINGLE_LEG"}
        _tti_is_income_leg = _tti_structure in _tti_income_structures

        if _tti_trade_count > 1 and not _tti_is_leap_local and not _tti_is_income_leg:
            propose_gate(
                collector, "time_to_impulse_multileg_exit",
                action="EXIT", urgency=_tti_urgency,
                rationale=(
                    f"⚠️ RANGE_BOUND with no catalyst: ADX={_tti_adx:.0f}, "
                    f"choppiness={_tti_chop:.0f}, ROC5={_tti_roc5:+.1f}%. "
                    f"No breakout within ~{_tti_budget_str} = exit THIS leg. "
                    f"(Natenberg Ch.4 + McMillan Ch.4: no catalyst = no thesis.)"
                ),
                doctrine_source="Natenberg Ch.4: Multi-Leg Theta Management",
                priority=35,
            )
        else:
            propose_gate(
                collector, "time_to_impulse",
                action="HOLD", urgency=_tti_urgency,
                rationale=(
                    f"⚠️ WATCH — breakout required within ~{_tti_budget_str}. "
                    f"Position is RANGE_BOUND with no momentum catalyst: "
                    f"ADX={_tti_adx:.0f}, choppiness={_tti_chop:.0f}, "
                    f"ROC5={_tti_roc5:+.1f}%, ROC10={_tti_roc10:+.1f}%. "
                    f"GreekDominance=THETA_DOMINANT with {_tti_dte:.0f} DTE. "
                    f"(McMillan Ch.4 + Passarelli Ch.2: Time-to-Impulse.)"
                ),
                doctrine_source="McMillan Ch.4 + Passarelli Ch.2: Time-to-Impulse",
                priority=35,
            )

    # ── Forward expectancy ──────────────────────────────────────────────
    _ev_ratio = safe_row_float(row, 'EV_Feasibility_Ratio') if pd.notna(row.get('EV_Feasibility_Ratio')) else float('nan')
    _ev_50_ratio = safe_row_float(row, 'EV_50pct_Feasibility_Ratio') if pd.notna(row.get('EV_50pct_Feasibility_Ratio')) else float('nan')
    _req_move_be = safe_row_float(row, 'Required_Move_Breakeven')
    _em_10 = safe_row_float(row, 'Expected_Move_10D')
    _conv_status = str(row.get('Conviction_Status', '') or '').upper()
    _det_streak_raw = row.get('Delta_Deterioration_Streak', 0)
    _det_streak = int(_det_streak_raw) if pd.notna(_det_streak_raw) and _det_streak_raw else 0

    if (not pd.isna(_ev_ratio) and _ev_ratio > 1.5 and dte < 45 and pnl_pct < -0.20):
        propose_gate(
            collector, "forward_expectancy",
            action="ROLL", urgency="MEDIUM",
            rationale=(
                f"Forward expectancy gate: required move to breakeven "
                f"(${_req_move_be:.1f}) is {_ev_ratio:.1f}× the 10-day "
                f"expected move (${_em_10:.1f}, IV-based). "
                f"With DTE={dte:.0f}d remaining, structural recovery probability is low. "
                f"Roll to extend time or widen strike (McMillan Ch.4: Forward Expectancy)."
            ),
            doctrine_source="McMillan Ch.4: Forward Expectancy",
            priority=38,
        )

    # ── Conviction decay ────────────────────────────────────────────────
    if (_conv_status == 'REVERSING' and _det_streak >= 3 and dte < 45 and pnl_pct < -0.20):
        propose_gate(
            collector, "conviction_decay",
            action="ROLL", urgency="MEDIUM",
            rationale=(
                f"Conviction decay: delta deteriorating for {_det_streak} "
                f"consecutive cycles (Conviction={_conv_status}). "
                f"Roll or exit before theta accelerates at DTE={dte:.0f}d "
                f"(Passarelli Ch.2: Conviction Decay)."
            ),
            doctrine_source="Passarelli Ch.2: Conviction Decay",
            priority=40,
        )

    # ── Four-dimension thesis health (2g) ───────────────────────────────
    hv = safe_row_float(row, 'HV_20D')
    iv_live = row.get('IV_Now')
    iv = float(iv_live) if iv_live is not None and not pd.isna(iv_live) and float(iv_live or 0) > 0 \
         else safe_row_float(row, 'IV_30D')
    iv_source = "live" if (iv_live is not None and not pd.isna(iv_live) and float(iv_live or 0) > 0) else "daily"

    if pnl_pct < -0.15:
        roc5 = safe_row_float(row, 'roc_5')
        roc10 = safe_row_float(row, 'roc_10')
        roc20 = safe_row_float(row, 'roc_20')
        mom_slope = safe_row_float(row, 'momentum_slope')
        adx = safe_row_float(row, 'adx_14')
        rsi = safe_row_float(row, 'rsi_14', default=50.0)
        bb_width_z = safe_row_float(row, 'bb_width_z')
        choppiness = safe_row_float(row, 'choppiness_index', default=50.0)
        trend_state = str(row.get('TrendIntegrity_State', '') or '').split('.')[-1].upper()
        mom_velocity = str(row.get('MomentumVelocity_State', '') or '').split('.')[-1].upper()
        comp_raw = str(row.get('CompressionMaturity', '') or '').split('.')[-1].upper()
        iv_surface = str(row.get('iv_surface_shape', '') or '').split('.')[-1].upper()

        # Dimension A: Structure Direction
        if _is_put:
            _dir_broken = (roc20 > 8 and mom_slope > 0
                           and trend_state in ('NO_TREND', 'TREND_EXHAUSTED', 'WEAK_TREND')
                           and adx < 20 and rsi > 52)
            _dir_intact = (roc20 < 0 or (mom_slope < 0 and adx > 20)
                           or mom_velocity in ('ACCELERATING', 'TRENDING'))
        else:
            _dir_broken = (roc20 < -8 and mom_slope < 0
                           and trend_state in ('NO_TREND', 'TREND_EXHAUSTED', 'WEAK_TREND')
                           and adx < 20 and rsi < 48)
            _dir_intact = (roc20 > 0 or (mom_slope > 0 and adx > 20)
                           or mom_velocity in ('ACCELERATING', 'TRENDING'))
        _dir_weak = not _dir_broken and not _dir_intact

        # Dimension B: Compression
        _compressing = (bb_width_z < -0.8
                        or comp_raw in ('EARLY_COMPRESSION', 'MID_COMPRESSION', 'MATURE_COMPRESSION'))
        _expanding = (bb_width_z > 0.5 and comp_raw not in (
                      'EARLY_COMPRESSION', 'MID_COMPRESSION', 'MATURE_COMPRESSION'))

        # Dimension C: Vol Regime
        _vol_hostile = (hv > 0 and iv > 0 and hv > iv * 1.20 and iv_surface in ('BACKWARDATION', ''))
        _vol_neutral = not _vol_hostile

        # Dimension D: Alignment
        _d1_compression_coiling = (_compressing and mom_slope > -0.015)
        if _is_put:
            _d2_early_accel = (roc5 < 0 and roc10 < 0 and _compressing)
            _d3_bottoming_reversal = (mom_velocity in ('REVERSING',) and rsi > 52 and mom_slope < 0.01)
        else:
            _d2_early_accel = (roc5 > 0 and roc10 > 0 and _compressing)
            _d3_bottoming_reversal = (mom_velocity in ('REVERSING',) and rsi < 48 and mom_slope > -0.01)
        _alignment_pass = _d1_compression_coiling or _d2_early_accel or _d3_bottoming_reversal

        if _dir_broken:
            _pct_to_strike = (_strike / _ul_last - 1) if _ul_last > 0 else 0
            _gap_str = f"{_pct_to_strike:.1%} away" if _ul_last > 0 else "N/A"
            propose_gate(
                collector, "thesis_regime_degradation_broken",
                action="EXIT", urgency="HIGH",
                rationale=(
                    f"Thesis BROKEN — structural direction failed for a {_option_type_label}. "
                    f"ROC20={roc20:.1f}%, slope={mom_slope:+.3f}, ADX={adx:.0f} (weak), "
                    f"RSI={rsi:.0f}, trend={trend_state}. "
                    f"Stock at ${_ul_last:.2f}, strike ${_strike:.0f}, {_gap_str}. "
                    f"Exit and redeploy when structure recovers "
                    f"(McMillan Ch.4: cut thesis failures)."
                ),
                doctrine_source="McMillan Ch.4 + Natenberg Ch.5: Thesis BROKEN",
                priority=45,
            )
        elif _dir_weak and _d1_compression_coiling:
            propose_gate(
                collector, "thesis_regime_degradation_coiling",
                action="HOLD", urgency="MEDIUM",
                rationale=(
                    f"Thesis COILING — price compressing, not broken. "
                    f"Structure direction weak (ROC20={roc20:.1f}%, slope={mom_slope:+.3f}) but "
                    f"bb_width_z={bb_width_z:.2f} (compressing). "
                    f"Choppiness={choppiness:.0f}, MomentumVelocity={mom_velocity}. "
                    f"(Passarelli Ch.5: compression → release.)"
                ),
                doctrine_source="Passarelli Ch.5 + Cohen Ch.4: Compression Hold",
                priority=45,
            )
        elif _dir_weak and _d3_bottoming_reversal:
            propose_gate(
                collector, "thesis_regime_degradation_reversing",
                action="HOLD", urgency="MEDIUM",
                rationale=(
                    f"Thesis REVERSING — momentum shifting. "
                    f"MomentumVelocity={mom_velocity}, RSI={rsi:.0f}, "
                    f"slope={mom_slope:+.3f}. "
                    f"Monitor for 5d confirmation (McMillan Ch.4: Reversal Monitoring)."
                ),
                doctrine_source="McMillan Ch.4: Reversal Monitoring",
                priority=45,
            )
        elif _dir_weak and _vol_hostile and not _alignment_pass:
            propose_gate(
                collector, "thesis_regime_degradation_vol_drag",
                action="ROLL", urgency="MEDIUM",
                rationale=(
                    f"Thesis WEAKENING with vol headwind. "
                    f"Structure: ROC20={roc20:.1f}%, slope={mom_slope:+.3f}, ADX={adx:.0f}. "
                    f"Vol: HV={hv:.1%} > IV={iv:.1%} [{iv_source}]. "
                    f"Roll to lower-IV further expiry (Natenberg Ch.5: Vol Drag Roll)."
                ),
                doctrine_source="Natenberg Ch.5: Vol Drag Roll",
                priority=45,
            )
        elif (_dir_intact or _d2_early_accel) and _vol_hostile:
            propose_gate(
                collector, "thesis_regime_degradation_intact_vol",
                action="HOLD", urgency="LOW",
                rationale=(
                    f"Thesis INTACT — structure supports {_option_type_label}. "
                    f"ROC20={roc20:.1f}%, slope={mom_slope:+.3f}, ADX={adx:.0f}. "
                    f"Vol note: HV={hv:.1%} > IV={iv:.1%} [{iv_source}]. "
                    f"Hold with current thesis (McMillan Ch.4: stay long while structure supports)."
                ),
                doctrine_source="McMillan Ch.4: Thesis Intact HOLD",
                priority=45,
            )

    # ── Thesis satisfaction (2.5) — pre-target profit capture ───────────
    _price_target = safe_row_float(row, 'Price_Target_Entry')
    _measured_move = safe_row_float(row, 'Measured_Move')
    _resistance_1 = safe_row_float(row, 'Resistance_Level_1')
    _sma20 = safe_row_float(row, 'SMA20')
    _sma50 = safe_row_float(row, 'SMA50')

    if _is_put:
        _sma_fallback = 0.0
        if _sma20 > 0 and _ul_last > 0 and _sma20 < _ul_last:
            _sma_fallback = _sma20
        elif _sma50 > 0 and _ul_last > 0 and _sma50 < _ul_last:
            _sma_fallback = _sma50
        _optimum_target = _price_target or _measured_move or _sma_fallback
    else:
        _optimum_target = _price_target or _measured_move or _resistance_1

    if _optimum_target > 0 and _ul_last > 0 and option_gain_pct > 0:
        if _is_put:
            _at_or_beyond_target = _ul_last <= _optimum_target
        else:
            _at_or_beyond_target = _ul_last >= _optimum_target

        def _target_src_label_v2():
            if _price_target:    return "IV-implied 1σ target (frozen at entry)"
            if _measured_move:   return "Measured_Move"
            if not _is_put and _resistance_1: return "Resistance_Level_1"
            if _sma20 and _optimum_target == _sma20: return "SMA20 support"
            if _sma50 and _optimum_target == _sma50: return "SMA50 support"
            return "price target"

        if _at_or_beyond_target and option_gain_pct >= 0.30:
            _target_source = _target_src_label_v2()
            _qty_int = abs(safe_row_float(row, 'Quantity', default=1.0))
            _thesis_satisfied_action = "TRIM" if _qty_int > 1 else "EXIT"
            propose_gate(
                collector, "pre_target_profit_capture",
                action=_thesis_satisfied_action, urgency="MEDIUM",
                rationale=(
                    f"Thesis SATISFIED: underlying ${_ul_last:.2f} reached target ${_optimum_target:.2f} "
                    f"({_target_source}). Option up {option_gain_pct:.0%} — edge is captured. "
                    + (
                        f"Trim to 50% size to lock partial profits. "
                        if _qty_int > 1 else
                        f"Take full profit — thesis complete. "
                    )
                    + f"(McMillan Ch.4 + Natenberg Ch.11: Thesis Satisfied.)"
                ),
                doctrine_source=f"McMillan Ch.4 + Natenberg Ch.11: Thesis Satisfied ({_target_source})",
                priority=50,
            )

    # ── Time value exhausted (C4) ───────────────────────────────────────
    _c4_pnl = safe_pnl_pct(row) or 0.0
    if (
        not is_leap
        and _last_price > 0
        and _tv_pct < 0.10
        and _intrinsic > 0
        and _c4_pnl > 0.05
    ):
        propose_gate(
            collector, "time_value_exhausted",
            action="EXIT", urgency="MEDIUM",
            rationale=(
                f"⏰ Time value exhausted: ${_time_val:.2f} ({_tv_pct:.0%} of ${_last_price:.2f}) — "
                f"only {_tv_pct:.0%} extrinsic remaining. "
                f"Option is {1-_tv_pct:.0%} intrinsic (${_intrinsic:.2f}/share). "
                f"Close to capture intrinsic gain cleanly. "
                f"(Natenberg Ch.7 + Cohen Ch.3: Time Value Exhausted Exit)"
            ),
            doctrine_source="Natenberg Ch.7 + Cohen Ch.3: Time Value Exhausted Exit (C4)",
            priority=30,  # same priority bucket as theta dominance
        )

    # ── Directional profit cap (MFE) ───────────────────────────────────
    if not is_leap and option_gain_pct >= 0.50:
        _qty_mfe = abs(safe_row_float(row, 'Quantity', default=1.0))
        _mfe_action = "TRIM" if _qty_mfe > 1 else "EXIT"
        propose_gate(
            collector, "directional_profit_cap",
            action=_mfe_action, urgency="MEDIUM",
            rationale=(
                f"PROFIT_CAPTURE: option up {option_gain_pct:.0%} (≥50%). "
                + (f"Trim to {max(1, int(_qty_mfe) // 2)} contracts to lock partial profit. "
                   if _qty_mfe > 1
                   else "Take full profit — directional edge captured. ")
                + f"(McMillan Ch.4: realize profits; avoid round-tripping.)"
            ),
            doctrine_source="McMillan Ch.4: Directional Profit Capture",
            priority=52,
        )

    # ── Weak entry quality profit capture ───────────────────────────────
    if not is_leap and option_gain_pct >= 0.30 and _entry_quality == 'WEAK':
        propose_gate(
            collector, "weak_entry_profit_capture",
            action="EXIT", urgency="MEDIUM",
            rationale=(
                f"PROFIT_CAPTURE: option up {option_gain_pct:.0%} (≥30%) on a WEAK entry "
                f"(entry trend: {_entry_ti}, structure: {_entry_ps}). "
                f"Weak entries get shorter profit leash. "
                f"(McMillan Ch.4: realize early on weak setups.)"
            ),
            doctrine_source="McMillan Ch.4: Weak Entry Profit Capture",
            priority=54,
        )

    # ── Time stop (pre-winner: option_gain_pct < 1.0) ──────────────────
    _time_stop_dte = 90 if is_leap else 21
    if dte <= _time_stop_dte and option_gain_pct < 1.0:
        _ts_urgency = "HIGH" if option_gain_pct <= 0 else "MEDIUM"
        _ts_pnl_note = (
            f"up {option_gain_pct:.0%}" if option_gain_pct > 0
            else f"down {abs(option_gain_pct):.0%}"
        )
        propose_gate(
            collector, "time_stop",
            action="EXIT", urgency=_ts_urgency,
            rationale=(
                f"Time stop: DTE={dte:.0f} ≤ {_time_stop_dte} "
                f"({'LEAP vega decay' if is_leap else 'theta acceleration zone'}). "
                f"Option {_ts_pnl_note} — "
                f"{'realize gains before theta erodes them' if option_gain_pct > 0 else 'cut losses before theta accelerates further'}. "
                f"(Passarelli {'Ch.8' if is_leap else 'Ch.2'}: time stop.)"
                + (_cal_bleed_note if _cal_bleed_note else "")
            ),
            doctrine_source=f"Passarelli {'Ch.8' if is_leap else 'Ch.2'}: Time Stop",
            priority=55,
        )

    # ── 100%+ gain target (Gate 3a) ─────────────────────────────────────
    _theta_day_dollar = abs(safe_row_float(row, 'Theta', default=0.0)) * 100 * abs(safe_row_float(row, 'Quantity', default=1.0))
    _min_days_held = max(2, original_dte * 0.10)
    if option_gain_pct >= 1.0 and days_held >= _min_days_held:
        _mom_strong = mom_state in ('ACCELERATING', 'TRENDING')
        _trend_state = str(row.get('TrendIntegrity_State', '') or '').split('.')[-1].upper()
        if _is_put:
            _trend_confirms = _trend_state in ('STRONG_TREND', 'WEAK_TREND')
            _roll_direction = "lower strike"
        else:
            _trend_confirms = _trend_state in ('STRONG_TREND', 'WEAK_TREND')
            _roll_direction = "higher strike"
        if _mom_strong and _trend_confirms and dte > 30:
            propose_gate(
                collector, "hundred_pct_gain_roll",
                action="ROLL", urgency="MEDIUM",
                rationale=(
                    f"Profit target: option up {option_gain_pct:.0%} (${_entry_price:.2f}→${_last_price:.2f}). "
                    f"Momentum {mom_state} + trend {_trend_state} — "
                    f"roll to {_roll_direction} to lock gains and stay directional "
                    f"(McMillan Ch.4: Roll Winners)."
                ),
                doctrine_source="McMillan Ch.4: Roll Winners",
                priority=58,
            )
        else:
            propose_gate(
                collector, "hundred_pct_gain_exit",
                action="EXIT", urgency="HIGH" if mom_state == 'LATE_CYCLE' else "MEDIUM",
                rationale=(
                    f"Profit target: option up {option_gain_pct:.0%} "
                    f"(${_entry_price:.2f}→${_last_price:.2f}) — take profits "
                    f"(McMillan Ch.4: Profit Target)."
                ),
                doctrine_source="McMillan Ch.4: Profit Target",
                priority=58,
            )

    # ── Single-contract trim-via-roll (Gate 3b-single) ──────────────────
    _qty_for_trim = abs(safe_row_float(row, 'Quantity', default=1.0))
    if (option_gain_pct >= 0.50
            and days_held >= _min_days_held
            and _qty_for_trim == 1
            and _tv_pct < 0.40
            and not is_leap
            and dte > 7):
        _roll_dir = "lower" if _is_put else "higher"
        if mom_state in ('STALLING', 'REVERSING'):
            _3b_urgency = 'HIGH'
        elif mom_state == 'DECELERATING':
            _3b_urgency = 'MEDIUM'
        else:
            _3b_urgency = 'LOW'
        propose_gate(
            collector, "single_contract_trim_via_roll",
            action="ROLL", urgency=_3b_urgency,
            rationale=(
                f"Single-contract winner: option up {option_gain_pct:.0%} "
                f"(${_entry_price:.2f}→${_last_price:.2f}), intrinsic ${_intrinsic:.2f} "
                f"({1-_tv_pct:.0%} of price). "
                f"Roll to a {_roll_dir} strike to harvest intrinsic "
                f"(McMillan Ch.4 + Passarelli Ch.6: Single-Contract Trim via Roll)."
            ),
            doctrine_source="McMillan Ch.4 + Passarelli Ch.6: Single-Contract Trim via Roll",
            priority=60,
        )

    # ── Multi-contract TRIM (Gate 3b-pre) ───────────────────────────────
    if (option_gain_pct >= 0.50
            and days_held >= _min_days_held
            and _qty_for_trim > 1
            and not is_leap):
        _trim_qty = max(1, int(_qty_for_trim / 2))
        _keep_qty = int(_qty_for_trim) - _trim_qty
        propose_gate(
            collector, "multi_contract_trim",
            action="TRIM", urgency="MEDIUM",
            rationale=(
                f"Multi-contract winner: option up {option_gain_pct:.0%} "
                f"(${_entry_price:.2f}→${_last_price:.2f}) on {_qty_for_trim:.0f} contracts. "
                f"Bank gains on {_trim_qty} contract(s), keep {_keep_qty} contract(s) open. "
                f"(McMillan Ch.4 + Passarelli Ch.6: Multi-Contract Partial Profit.)"
            ),
            doctrine_source="McMillan Ch.4 + Passarelli Ch.6: Multi-Contract Partial Profit",
            priority=62,
        )

    # ── Theta efficiency exit (Gate 3b) ─────────────────────────────────
    if (option_gain_pct >= 0.50
            and days_held >= _min_days_held
            and _tv_pct < 0.40
            and _theta_day_dollar > 25
            and not is_leap):
        propose_gate(
            collector, "theta_efficiency_exit_deep_itm",
            action="ROLL", urgency="MEDIUM",
            rationale=(
                f"Winner management: option up {option_gain_pct:.0%} "
                f"(${_entry_price:.2f}→${_last_price:.2f}), deeply ITM. "
                f"Paying ${_theta_day_dollar:.0f}/day theta. "
                f"Roll or exit (Passarelli Ch.5: Winner Carry Management)."
            ),
            doctrine_source="Passarelli Ch.5: Winner Carry Management",
            priority=65,
        )

    # ── Theta efficiency exit (rate-based, Gate 3b-theta) ───────────────
    _te_theta_per_share = abs(safe_row_float(row, 'Theta', default=0.0))
    _te_theta_consumes_tv = (
        _te_theta_per_share > 0 and _time_val > 0 and dte > 0
        and _te_theta_per_share * dte >= _time_val * 0.75
    )
    _te_bleed_pct = (_te_theta_per_share / _last_price * 100) if _last_price > 0 else 0.0
    _te_tv_days = _time_val / _te_theta_per_share if _te_theta_per_share > 0 else float('inf')

    if (option_gain_pct >= 0.30
            and days_held >= _min_days_held
            and _te_theta_consumes_tv
            and _te_bleed_pct > 1.0
            and _tv_pct >= 0.40
            and not is_leap):
        propose_gate(
            collector, "theta_efficiency_exit",
            action="EXIT", urgency="MEDIUM",
            rationale=(
                f"Theta efficiency exit: option up {option_gain_pct:.0%} "
                f"(${_entry_price:.2f}→${_last_price:.2f}). "
                f"Theta bleed {_te_bleed_pct:.1f}%/day will consume ≥75% of "
                f"remaining time value (${_time_val:.2f}/share) in {_te_tv_days:.0f} days. "
                f"(Augen + Given + Jabbour: Theta Efficiency.)"
            ),
            doctrine_source="Augen + Given + Jabbour: Theta Efficiency",
            priority=65,
        )

    # ── Near-threshold winners (Gate 3b-theta-warn) ─────────────────────
    if (option_gain_pct >= 0.25
            and option_gain_pct < 0.30
            and days_held >= _min_days_held
            and _te_theta_consumes_tv
            and _te_bleed_pct > 1.0
            and _tv_pct >= 0.40
            and not is_leap):
        propose_gate(
            collector, "near_threshold_winners",
            action="HOLD", urgency="MEDIUM",
            rationale=(
                f"Theta efficiency warning: option up {option_gain_pct:.0%} "
                f"(${_entry_price:.2f}→${_last_price:.2f}), approaching 30% harvest zone. "
                f"Theta bleed {_te_bleed_pct:.1f}%/day — monitor closely. "
                f"(Given + Jabbour: Theta Efficiency Warning.)"
            ),
            doctrine_source="Given + Jabbour: Theta Efficiency Warning",
            priority=68,
        )

    # ── Time stop (catch-all for ≥100% gain, Gate 4) ────────────────────
    time_stop_dte = 90 if is_leap else 21
    if dte <= time_stop_dte and option_gain_pct >= 1.0:
        propose_gate(
            collector, "time_stop_winner",
            action="EXIT", urgency="HIGH",
            rationale=(
                f"Time stop: DTE={dte:.0f} ≤ {time_stop_dte} "
                f"({'LEAP vega decay' if is_leap else 'theta acceleration'}). "
                f"Option up {option_gain_pct:.0%} — take profits before theta destroys remaining value. "
                f"(Passarelli {'Ch.8' if is_leap else 'Ch.2'}: time stop.)"
            ),
            doctrine_source=f"Passarelli {'Ch.8' if is_leap else 'Ch.2'}: Time Stop",
            priority=55,  # same priority as pre-winner time stop
        )

    # ── Delta floor (Gate 5) ────────────────────────────────────────────
    if delta_now < 0.10:
        propose_gate(
            collector, "delta_floor",
            action="EXIT", urgency="MEDIUM",
            rationale=(
                f"Delta floor breached ({delta_now:.2f} < 0.10) — contract non-responsive. "
                f"Cut and redeploy (McMillan Ch.4)."
            ),
            doctrine_source="McMillan Ch.4: Delta Minimums",
            priority=14,
        )

    # ── Thesis regime degradation (Gate 6) ──────────────────────────────
    thesis = check_thesis_degradation(row)
    if thesis:
        propose_gate(
            collector, "thesis_regime_degradation",
            action="ROLL", urgency="MEDIUM",
            rationale=(
                f"Entry thesis regime degraded: {thesis['text']}. "
                f"Original setup no longer intact — reassess or roll "
                f"(McMillan Ch.4: Thesis Persistence)."
            ),
            doctrine_source="McMillan Ch.4: Thesis Persistence",
            priority=45,
        )

    # ── Recovery infeasibility (Gate 7) ─────────────────────────────────
    _rf_drift_dir = str(row.get('Drift_Direction', '') or '').upper()
    if _is_put:
        _recovering = _rf_drift_dir in ('DOWN', 'DOWNWARD')
    else:
        _recovering = _rf_drift_dir in ('UP', 'UPWARD')
    recovery = str(row.get('Recovery_Feasibility', '') or '').upper()
    if recovery in ('IMPOSSIBLE', 'UNLIKELY') and pnl_pct < -0.20 and not _recovering:
        hv_daily = safe_row_float(row, 'HV_Daily_Move_1Sigma')
        req_daily = safe_row_float(row, 'Recovery_Move_Per_Day')
        if recovery == 'IMPOSSIBLE' and pnl_pct < -0.30:
            propose_gate(
                collector, "recovery_infeasibility",
                action="EXIT", urgency="HIGH",
                rationale=(
                    f"Recovery infeasible: needs ${req_daily:.2f}/day vs "
                    f"HV-implied ${hv_daily:.2f}/day ({req_daily/max(hv_daily, 0.01):.1f}× sigma). "
                    f"Cut losses (Natenberg Ch.5)."
                ),
                doctrine_source="Natenberg Ch.5: Recovery Infeasibility",
                priority=42,
            )
        elif recovery == 'UNLIKELY':
            propose_gate(
                collector, "recovery_unlikely",
                action="HOLD", urgency="MEDIUM",
                rationale=(
                    f"Recovery UNLIKELY: needs ${req_daily:.2f}/day vs HV ${hv_daily:.2f}/day. "
                    f"Monitor closely (Natenberg Ch.5)."
                ),
                doctrine_source="Natenberg Ch.5: Recovery Unlikely",
                priority=42,
            )

    # ── Trailing protection (Murphy + Nison) ────────────────────────────
    if _winner_lifecycle == 'THESIS_EXHAUSTING' and option_gain_pct >= 0.25:
        _exhaust_urgency = "HIGH" if _conv_status == 'REVERSING' else "MEDIUM"
        propose_gate(
            collector, "trailing_protection",
            action="EXIT", urgency=_exhaust_urgency,
            rationale=(
                f"Thesis exhausting: Pyramid Tier {_pyramid_tier}, momentum {mom_state}, "
                f"conviction {_conv_status}. Option up {option_gain_pct:.0%} — protect accumulated gains. "
                f"Murphy: 'Once thesis consumed, collapse the pyramid.' "
                f"Nison: 'Don't let a winner become a loser.'"
            ),
            doctrine_source="Murphy + Nison: Trailing Protection Mode",
            priority=56,
            Pyramid_Tier=_pyramid_tier,
            Winner_Lifecycle=_winner_lifecycle,
        )

    # ── Pre-holiday expiry escalation ───────────────────────────────────
    if _pre_holiday_expiry_escalate:
        propose_gate(
            collector, "pre_holiday_expiry",
            action="ROLL", urgency="MEDIUM",
            rationale=(
                f"Pre-holiday expiry risk: {dte:.0f} DTE into extended weekend. "
                f"Theta acceleration + multi-day non-trading gap = double bleed. "
                f"Roll before close today. "
                f"(Hull Ch.18 + Passarelli Ch.6: pre-holiday theta.)"
                f"{_cal_bleed_note}"
            ),
            doctrine_source="Hull Ch.18 + Passarelli Ch.6: Pre-Holiday Expiry",
            priority=55,
        )

    # ── Default HOLD (always present) ───────────────────────────────────
    theta_day = abs(safe_row_float(row, 'Theta', default=0.0))
    last_price_default = safe_row_float(row, 'Last')
    qty = abs(safe_row_float(row, 'Quantity', default=1.0))
    price_state_display = price_state.split('.')[-1]

    carry_note = ""
    if theta_day > 0 and last_price_default > 0:
        option_market_value = last_price_default * 100 * qty
        daily_margin_interest = option_market_value * FIDELITY_MARGIN_RATE_DAILY
        total_daily_carry = theta_day * 100 * qty + daily_margin_interest
        carry_note = (
            f" Carry: ${theta_day*100*qty:.2f}/day theta"
            f" + ${daily_margin_interest:.2f}/day margin"
            f" = ${total_daily_carry:.2f}/day total."
        )

    # Signal hub annotations (non-blocking)
    _signal_hub_notes = ""
    _macd_div_lo = str(row.get('MACD_Divergence', 'None') or 'None')
    _is_put_lo = any(s in strategy for s in ('LONG_PUT', 'BUY_PUT', 'LEAPS_PUT'))
    if _macd_div_lo == 'Bearish_Divergence' and not _is_put_lo:
        _signal_hub_notes += " MACD bearish divergence (Murphy 0.691)."
    elif _macd_div_lo == 'Bullish_Divergence' and _is_put_lo:
        _signal_hub_notes += " MACD bullish divergence (Murphy 0.691)."
    _rs_spy_lo = safe_row_float(row, 'RS_vs_SPY_20d')
    if abs(_rs_spy_lo) > 5:
        _rs_dir = "outperforming" if _rs_spy_lo > 0 else "underperforming"
        _signal_hub_notes += f" RS vs SPY: {_rs_spy_lo:+.1f}% ({_rs_dir} 20d)."

    # ── Cross-gate prior-EXIT persistence (McMillan Ch.4) ───────────────
    # If prior day said EXIT from any gate and conditions haven't materially
    # improved, inject a high-priority EXIT proposal. This prevents one-day
    # EXIT→HOLD flip-flops that erode trust. Macro catalyst exception allows
    # clearing when FOMC/CPI/NFP is imminent (vol catalyst for long premium).
    from core.management.cycle3.doctrine.helpers import check_prior_exit_persistence
    _macro_days_v2 = safe_row_float(row, 'Days_To_Macro', default=999.0) if pd.notna(row.get('Days_To_Macro')) else None
    _macro_type_v2 = str(row.get('Macro_Next_Type', '') or '') or None
    _persist_exit_v2, _persist_reason_v2, _macro_catalyst_v2 = check_prior_exit_persistence(
        row, _is_put,
        macro_days=_macro_days_v2,
        macro_type=_macro_type_v2,
    )
    if _persist_exit_v2:
        propose_gate(
            collector, "prior_exit_persistence",
            action="EXIT", urgency="MEDIUM",
            rationale=f"{_persist_reason_v2}{_sigma_tag}",
            doctrine_source="McMillan Ch.4: Prior EXIT Persistence",
            priority=8,  # high priority — below hard vetoes (1-5) but above soft gates
        )

    # Macro catalyst annotation for default HOLD
    _macro_note_v2 = ""
    if _macro_catalyst_v2:
        result["Macro_Catalyst_Protected"] = True
        _macro_note_v2 = f" 📅 {_persist_reason_v2}"

    propose_gate(
        collector, "default_hold",
        action="HOLD", urgency="LOW",
        rationale=(
            f"Position intact — Delta={delta_now:.2f}, DTE={dte:.0f}, structure={price_state_display}."
            f"{carry_note}{_signal_hub_notes}{_cal_bleed_note}{_macro_note_v2} Thesis active (McMillan Ch.4)."
        ),
        doctrine_source="McMillan Ch.4: Neutral Maintenance",
        priority=100,
    )

    # ── EV Comparator (always evaluates) ────────────────────────────────
    ev_result = None
    try:
        ev_result = compare_actions_long_option(row, float(max(dte, 1)), pnl_pct)
        result["Action_EV_Ranking"] = ">".join(ev_result["ranked_actions"])
        result["Action_EV_Winner"] = ev_result["ev_winner"]
        result["Action_EV_Margin"] = ev_result["ev_margin"]
        result["Action_EV_Hold"] = ev_result["ev_hold"]
        result["Action_EV_Roll"] = ev_result["ev_roll"]
        result["Action_EV_Close"] = ev_result["ev_close"]
        result["Vol_Confidence"] = ev_result["vol_confidence"]
        result["EV_Capital_Impact"] = ev_result["capital_impact"]
        result["MC_Used"] = ev_result["mc_used"]
    except Exception as ev_err:
        logger.debug(f"Long option v2 EV comparator error (non-fatal): {ev_err}")

    # ── Resolution ──────────────────────────────────────────────────────
    logger.debug(f"[LO_v2] {collector.summary()}")

    # Hard vetoes win immediately
    if collector.has_hard_veto():
        winner = collector.get_veto()
        return collector.to_result(winner, result, resolution_method="HARD_VETO")

    # Resolve via EV + MC
    if ev_result is not None:
        winner = resolve_directional_proposals(collector, ev_result, row)
        resolved = collector.to_result(winner, result, resolution_method="EV_COMPARISON")
    else:
        # No EV data — fall back to highest urgency, lowest priority
        proposals_sorted = sorted(
            collector.proposals,
            key=lambda p: (-p.urgency_rank, p.priority),
        )
        winner = proposals_sorted[0]
        resolved = collector.to_result(winner, result, resolution_method="PRIORITY_FALLBACK")

    return resolved
