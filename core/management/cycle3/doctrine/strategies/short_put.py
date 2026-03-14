"""
Short put (CSP) doctrine — wheel assessment, assignment management, income gates.

Extracted from DoctrineAuthority._short_put_doctrine (engine.py lines 6032-6689).
"""

import logging
from typing import Dict

import pandas as pd

from core.management.cycle3.doctrine.gate_result import (
    STATE_ACTIONABLE,
    STATE_NEUTRAL_CONFIDENT,
)
from core.management.cycle3.doctrine.helpers import check_thesis_degradation
from ..proposal import ProposalCollector, propose_gate
from ..comparators.action_ev_bw import compare_actions_bw, resolve_income_proposals
from ..shared_income_gates import gate_consecutive_debit_roll_stop, gate_fading_winner
from ..helpers import safe_pnl_pct, safe_row_float

logger = logging.getLogger(__name__)


def short_put_doctrine(row: pd.Series, result: Dict) -> Dict:
    # ── Wheel Assessment (runs first — persists to result regardless of gate path) ─
    _spot_for_wheel     = safe_row_float(row, 'UL Last')
    _strike_for_wheel   = safe_row_float(row, 'Strike')
    _premium_entry_w    = abs(safe_row_float(row, 'Premium_Entry'))
    _net_cost_basis_w   = safe_row_float(row, 'Net_Cost_Basis_Per_Share')
    _broker_basis_w     = abs(safe_row_float(row, 'Basis'))
    _qty_w              = abs(safe_row_float(row, 'Quantity', default=1.0))
    _dte_for_wheel      = safe_row_float(row, 'DTE', default=999.0)
    _iv_now_w_raw       = safe_row_float(row, 'IV_Now', 'IV_30D')
    _hv_20d_w           = safe_row_float(row, 'HV_20D')
    if _dte_for_wheel <= 2 and _hv_20d_w > 0:
        _iv_now_w       = _hv_20d_w
        _iv_source_w    = "HV_20D (near-expiry — option IV unreliable)"
    else:
        _iv_now_w       = _iv_now_w_raw
        _iv_source_w    = "IV_Now/IV_30D"
    _delta_util_w       = safe_row_float(row, 'Portfolio_Delta_Utilization_Pct')
    _trend_w            = str(row.get('TrendIntegrity_State', '') or '').split('.')[-1].upper()
    _price_struct_w     = str(row.get('PriceStructure_State', '') or '').split('.')[-1].upper()
    _mc_assign_raw = row.get('MC_Assign_P_Expiry')
    if pd.notna(_mc_assign_raw) and _mc_assign_raw not in (None, '', 0):
        _mc_assign_p_w = float(_mc_assign_raw)
    else:
        # Delta-based fallback when MC didn't run (API timeout, insufficient history).
        # |delta| ≈ N(d1) is a rough proxy for P(finish ITM). Scale by 1.2 to approximate
        # P(touch) > P(finish ITM), then clamp to [0, 1].
        _delta_abs = abs(safe_row_float(row, 'Delta'))
        _mc_assign_p_w = min(1.0, _delta_abs * 1.2) if _delta_abs > 0.05 else 0.0

    # Effective basis per share
    _broker_cost_per_share_w = (
        (_broker_basis_w / _qty_w / 100.0)
        if (_qty_w > 0 and _broker_basis_w > 0)
        else 0.0
    )
    if _net_cost_basis_w > 0:
        _effective_basis_w = _net_cost_basis_w
    elif _strike_for_wheel > 0 and _premium_entry_w > 0:
        _effective_basis_w = _strike_for_wheel - _premium_entry_w
    elif _broker_cost_per_share_w > 0:
        _effective_basis_w = _broker_cost_per_share_w
    else:
        _effective_basis_w = 0.0

    _wheel_basis_ok = (
        _effective_basis_w > 0
        and _spot_for_wheel > 0
        and _effective_basis_w <= _spot_for_wheel * 0.97
    )
    _market_structure_w = str(row.get('Market_Structure', 'Unknown') or 'Unknown')
    _wheel_chart_ok = (
        'BROKEN' not in _trend_w
        and 'BROKEN' not in _price_struct_w
        and _market_structure_w != 'Downtrend'
    )
    from core.shared.finance_utils import normalize_iv as _normalize_iv
    _iv_now_w_norm = _normalize_iv(_iv_now_w) or 0.0
    _wheel_iv_ok = _iv_now_w_norm >= 0.25
    _wheel_capital_ok = _delta_util_w < 15.0

    _wheel_ready = _wheel_basis_ok and _wheel_chart_ok and _wheel_iv_ok and _wheel_capital_ok

    _wheel_fails = []
    if not _wheel_basis_ok:
        _basis_note = f"${_effective_basis_w:.2f}" if _effective_basis_w > 0 else "unknown"
        _wheel_fails.append(f"basis {_basis_note} not at ≥3% discount (spot=${_spot_for_wheel:.2f})")
    if not _wheel_chart_ok:
        _fail_parts = []
        if 'BROKEN' in _trend_w or 'BROKEN' in _price_struct_w:
            _fail_parts.append(f"structure broken (Trend={_trend_w}, Price={_price_struct_w})")
        if _market_structure_w == 'Downtrend':
            _fail_parts.append("swing structure downtrend (Murphy Ch.4: LH/LL pattern)")
        _wheel_fails.append("; ".join(_fail_parts) if _fail_parts else f"chart check failed")
    if not _wheel_iv_ok:
        _wheel_fails.append(f"IV {_iv_now_w_norm:.0%} < 25% — CC premium too thin ({_iv_source_w})")
    if not _wheel_capital_ok:
        _wheel_fails.append(f"delta utilization {_delta_util_w:.1f}% ≥ 15% — overconcentrated")

    if _wheel_ready:
        _wheel_note = (
            f"✅ Wheel Ready — assignment is a FEATURE: "
            f"effective basis ${_effective_basis_w:.2f} vs spot ${_spot_for_wheel:.2f} "
            f"({(_spot_for_wheel - _effective_basis_w)/_spot_for_wheel:.1%} discount). "
            f"IV={_iv_now_w_norm:.0%} ({_iv_source_w}) supports CC entry. "
            f"Chart intact. Delta util={_delta_util_w:.1f}%. "
            f"Passarelli Ch.1: 'The effective purchase price is strike minus premium — "
            f"assignment at a discount is the intended outcome of a CSP.' "
            f"Next: accept stock, sell covered call at/above basis."
        )
    else:
        _wheel_note = (
            f"⚠️ Wheel NOT Ready — "
            + "; ".join(_wheel_fails)
            + f". Standard assignment defense applies."
        )

    # Signal Hub: OBV distribution warning (Murphy Ch.7)
    _obv_slope_sp = safe_row_float(row, 'OBV_Slope')
    if _obv_slope_sp < -10:
        _wheel_note += (
            f" OBV distributing ({_obv_slope_sp:.1f}%) — smart money "
            f"may be exiting (Murphy Ch.7). Assignment into distribution "
            f"trend increases basis recovery difficulty."
        )

    result['Wheel_Ready']  = _wheel_ready
    result['Wheel_Note']   = _wheel_note
    result['Wheel_Basis']  = round(_effective_basis_w, 2) if _effective_basis_w > 0 else None
    result['Wheel_IV_Ok']  = _wheel_iv_ok
    result['Wheel_Chart_Ok'] = _wheel_chart_ok
    result['Wheel_Capital_Ok'] = _wheel_capital_ok

    # MC_Assign_P_Expiry gate
    result['MC_Assign_P_Expiry_Used'] = round(_mc_assign_p_w, 3) if _mc_assign_p_w > 0 else None
    if _mc_assign_p_w > 0.75 and not _wheel_ready:
        _current_urgency = str(result.get('Urgency', 'LOW')).upper()
        if _current_urgency not in ('HIGH', 'CRITICAL'):
            result['Urgency'] = 'HIGH'
            result['Rationale'] = (
                (result.get('Rationale') or '') +
                f" | ⚡ MC: P(assign by expiry)={_mc_assign_p_w:.0%} — assignment is "
                f"statistically probable (>75%) and Wheel not Ready. Roll to defend "
                f"or close position before assignment probability locks in "
                f"(McMillan Ch.7 + Natenberg Ch.19: quantitative assignment directive)."
            )

    # ── Earnings Lockdown Guard ──────────────────────────────────────────
    _earn_date_sp = row.get('Earnings_Date')
    _days_to_earn_sp = None
    if _earn_date_sp not in (None, '', 'nan', 'N/A') and not (
        isinstance(_earn_date_sp, float) and pd.isna(_earn_date_sp)
    ):
        try:
            _ed_sp = pd.to_datetime(str(_earn_date_sp), errors='coerce')
            if pd.notna(_ed_sp):
                _snap_sp = pd.to_datetime(row.get('Snapshot_TS') or pd.Timestamp.now())
                _days_to_earn_sp = (_ed_sp.normalize() - _snap_sp.normalize()).days
        except Exception as _sp_earn_err:
            logger.debug(f"Short put earnings date parse skipped: {_sp_earn_err}")
    if _days_to_earn_sp is not None and 0 <= _days_to_earn_sp <= 2:
        _earn_itm_sp = row.get('Moneyness_Label') == 'ITM'
        _earn_wheel_ok = _wheel_ready and not _earn_itm_sp
        if not _earn_wheel_ok:
            result.update({
                "Action": "EXIT",
                "Urgency": "HIGH",
                "Rationale": (
                    f"Earnings lockdown: earnings in {_days_to_earn_sp}d. "
                    f"Rolling into a binary event is structurally invalid — "
                    f"gap risk cannot be delta-hedged. "
                    f"Exit before announcement: close the short put to eliminate "
                    f"assignment gap exposure. "
                    f"(Jabbour Ch.8: never roll into earnings; Given Ch.4: close before event.)"
                ),
                "Doctrine_Source": "Jabbour Ch.8 + Given Ch.4: Earnings Lockdown",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True,
            })
            return result

    # ── Calendar gates ───────────────────────────────────────────────────
    try:
        from scan_engine.calendar_context import expiry_proximity_flag
        _dte_sp_cal   = safe_row_float(row, 'DTE', default=999.0)
        _ul_sp_cal    = safe_row_float(row, 'UL Last')
        _strike_sp_cal = safe_row_float(row, 'Strike')
        _strat_sp_cal  = str(row.get('Strategy', '') or '').upper()
        _exp_flag_sp, _exp_note_sp = expiry_proximity_flag(
            dte=_dte_sp_cal, strategy=_strat_sp_cal,
            ul_last=_ul_sp_cal, strike=_strike_sp_cal,
        )
        if _exp_flag_sp == 'PIN_RISK':
            result.update({
                "Action": "EXIT",
                "Urgency": "CRITICAL",
                "Exit_Trigger_Type": "GAMMA",
                "Rationale": _exp_note_sp,
                "Doctrine_Source": "McMillan Ch.7 + Natenberg Ch.15: Pin Risk",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result
        elif _exp_flag_sp == 'GAMMA_CRITICAL':
            result.update({
                "Action": "ROLL",
                "Urgency": "HIGH",
                "Rationale": _exp_note_sp,
                "Doctrine_Source": "Natenberg Ch.15: Gamma Critical — Force Decision",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result
        elif _exp_flag_sp == 'PRE_HOLIDAY_EXPIRY':
            if _wheel_ready:
                result.update({
                    "Action": "HOLD",
                    "Urgency": "LOW",
                    "Rationale": (
                        f"Pre-holiday expiry — BUT Wheel Ready: {_wheel_note} "
                        f"Accepting assignment here is preferable to rolling into "
                        f"holiday-thin markets. After assignment, sell CC when liquidity returns."
                    ),
                    "Doctrine_Source": "Passarelli Ch.1 (Wheel Override): Pre-Holiday Assignment Acceptance",
                    "Decision_State": STATE_NEUTRAL_CONFIDENT,
                    "Required_Conditions_Met": True
                })
            else:
                result.update({
                    "Action": "ROLL",
                    "Urgency": "HIGH",
                    "Rationale": _exp_note_sp,
                    "Doctrine_Source": "Hull Ch.18 + Passarelli Ch.6: Pre-Holiday Expiry",
                    "Decision_State": STATE_ACTIONABLE,
                    "Required_Conditions_Met": True
                })
            return result
    except Exception as _sp_exp_err:
        logger.debug(f"Short put expiration/calendar gate skipped: {_sp_exp_err}")

    # 1. Tactical Maintenance: Expiration/Assignment Risk
    is_late = row.get('Lifecycle_Phase') == 'TERMINAL'
    is_itm = row.get('Moneyness_Label') == 'ITM'
    assignment_acceptable = row.get('Assignment_Acceptable', True)

    # ── 0. Deep Underwater Gate ────────────────────────────────────────────
    # When stock has collapsed far below strike AND there's no time value left,
    # the option is pure intrinsic — holding bleeds nothing (theta=$0) but
    # assignment creates a position at basis >> market price.
    #
    # Recovery path: if thesis INTACT and IV supports CC premium, accepting
    # assignment and wheeling (sell CCs to reduce basis) may be better than
    # locking in a permanent loss.  The decision: exit for defined loss vs
    # accept assignment → wheel conversion → CC income recovery.
    # Jabbour Ch.8: "Know when assignment is a liability, not an opportunity."
    # Passarelli Ch.1: "Wheel conversion — assignment at a discount is intentional."
    _sp_dte = safe_row_float(row, 'DTE', default=999.0)
    _sp_spot_uw = safe_row_float(row, 'UL Last')
    _sp_strike_uw = safe_row_float(row, 'Strike')
    _sp_theta_uw = abs(safe_row_float(row, 'Theta'))
    if (
        _sp_strike_uw > 0
        and _sp_spot_uw > 0
        and _sp_spot_uw < _sp_strike_uw * 0.60  # stock > 40% below strike
        and _sp_theta_uw < 0.01                  # no time value left
        and not _wheel_ready                     # wheel can't rescue this
    ):
        _uw_deficit_pct = 1.0 - (_sp_spot_uw / _sp_strike_uw)
        _uw_basis_note = f" Effective basis ${_effective_basis_w:.2f}" if _effective_basis_w > 0 else ""

        # ── CSP Recovery Check: can assignment → wheel work? ───────────
        # Wheel_Ready failed on basis (basis >> spot), but if IV can support
        # CC premium and thesis is intact, accepting assignment and selling
        # covered calls IS the recovery path.
        _csp_thesis = str(row.get('Thesis_State', '') or '').upper()
        _csp_iv_for_recovery = _iv_now_w_norm  # already normalized above
        _csp_iv_viable = _csp_iv_for_recovery > 0.15  # 15% floor for CC income
        _csp_recovery_viable = (
            _csp_thesis != 'BROKEN'
            and _csp_iv_viable
            and _wheel_capital_ok  # not overconcentrated
        )

        if _csp_recovery_viable:
            # Estimate wheel recovery economics
            import math
            _csp_basis = _effective_basis_w if _effective_basis_w > 0 else _sp_strike_uw
            _csp_gap = _csp_basis - _sp_spot_uw
            # Monthly CC premium estimate: BS ATM approximation
            _csp_monthly_cc = _sp_spot_uw * _csp_iv_for_recovery * math.sqrt(1.0 / 12.0) * 0.4
            _csp_months_be = _csp_gap / _csp_monthly_cc if _csp_monthly_cc > 0 else float('inf')
            _csp_months_str = (
                f" Est. {_csp_months_be:.0f} months of CC cycles to breakeven."
                if _csp_months_be < 999 else ""
            )

            # Catalyst context
            _csp_catalyst = []
            _csp_days_earn = safe_row_float(row, 'days_to_earnings', default=999.0)
            _csp_beat_rate = safe_row_float(row, 'Earnings_Beat_Rate')
            if _csp_days_earn < 45:
                _earn_note = f"Earnings in {_csp_days_earn:.0f}d"
                if _csp_beat_rate > 0:
                    _earn_note += f" (beat rate {_csp_beat_rate:.0%})"
                _csp_catalyst.append(_earn_note)
            _csp_days_macro = safe_row_float(row, 'Days_To_Macro', default=999.0)
            _csp_macro_evt = str(row.get('Macro_Next_Event', '') or '')
            if _csp_days_macro < 10 and _csp_macro_evt:
                _csp_catalyst.append(f"{_csp_macro_evt} in {_csp_days_macro:.0f}d")
            _csp_adx = safe_row_float(row, 'adx_14', default=25.0)
            _csp_trend = str(row.get('TrendIntegrity_State', '') or '').upper()
            if _csp_adx < 20 and _csp_trend in ('NO_TREND', 'TREND_EXHAUSTED', ''):
                _csp_catalyst.append(f"stock basing (ADX {_csp_adx:.0f})")
            _csp_catalyst_str = (
                f" Next catalyst: {'; '.join(_csp_catalyst)}."
                if _csp_catalyst else ""
            )

            result.update({
                "Action": "HOLD",
                "Urgency": "MEDIUM",
                "Rationale": (
                    f"Deep underwater CSP recovery: stock ${_sp_spot_uw:.2f} is "
                    f"{_uw_deficit_pct:.0%} below strike ${_sp_strike_uw:.2f}."
                    f"{_uw_basis_note} "
                    f"EXIT locks in ${_csp_gap:.2f}/sh permanent loss. "
                    f"Assignment → wheel conversion: accept stock at basis "
                    f"${_csp_basis:.2f}, sell CCs at IV {_csp_iv_for_recovery:.0%} "
                    f"(~${_csp_monthly_cc:.2f}/sh/mo).{_csp_months_str}"
                    f"{_csp_catalyst_str} "
                    f"Thesis {_csp_thesis} — hold through assignment, start CC income cycle. "
                    f"(Passarelli Ch.1: wheel conversion; Jabbour Ch.4: repair strategy; "
                    f"McMillan Ch.3: basis reduction via covered writing.)"
                ),
                "Doctrine_Source": "Passarelli Ch.1 + Jabbour Ch.4: CSP Recovery — Wheel Conversion",
                "Doctrine_State": "RECOVERY_LADDER",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True,
            })
            return result

        # No recovery path — standard EXIT
        result.update({
            "Action": "EXIT",
            "Urgency": "HIGH",
            "Exit_Trigger_Type": "CAPITAL",
            "Rationale": (
                f"Deep underwater: stock ${_sp_spot_uw:.2f} is {_uw_deficit_pct:.0%} below "
                f"strike ${_sp_strike_uw:.2f}.{_uw_basis_note} vs spot ${_sp_spot_uw:.2f}. "
                f"Option is pure intrinsic (θ=${_sp_theta_uw:.2f}/day, no time value to collect). "
                f"Assignment creates shares at {_uw_deficit_pct:.0%} premium to market — "
                f"not a strategic entry, a capital trap. Wheel not ready: {_wheel_note} "
                f"Recovery not viable: "
                + (f"thesis {_csp_thesis}" if _csp_thesis == 'BROKEN'
                   else f"IV {_csp_iv_for_recovery:.0%} too low for CC income"
                   if not _csp_iv_viable else "overconcentrated")
                + ". Close for defined loss. "
                f"(Jabbour Ch.8: assignment is a liability when basis >> market price; "
                f"Natenberg Ch.15: no EV in holding pure-intrinsic short option.)"
            ),
            "Doctrine_Source": "Jabbour Ch.8 + Natenberg Ch.15: Deep Underwater Exit",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True,
        })
        return result

    # Assignment Risk Auto-Close Terminal Case
    _sp_delta_abs = abs(safe_row_float(row, 'Delta'))
    if _sp_dte <= 7 and _sp_delta_abs > 0.90 and not _wheel_ready:
        _sp_spot_atc = safe_row_float(row, 'UL Last')
        _sp_strike_atc = safe_row_float(row, 'Strike')
        _sp_intrinsic = max(0.0, _sp_strike_atc - _sp_spot_atc) if _sp_strike_atc > 0 and _sp_spot_atc > 0 else 0.0

        # CSP recovery: if thesis INTACT + IV viable, accept assignment → wheel
        _atc_thesis_v1 = str(row.get('Thesis_State', '') or '').upper()
        _atc_iv_v1 = _iv_now_w_norm
        _atc_recovery_v1 = _atc_thesis_v1 != 'BROKEN' and _atc_iv_v1 > 0.15 and _wheel_capital_ok

        if _atc_recovery_v1:
            import math
            _atc_basis_v1 = _effective_basis_w if _effective_basis_w > 0 else _sp_strike_atc
            _atc_gap_v1 = _atc_basis_v1 - _sp_spot_atc
            _atc_cc_v1 = _sp_spot_atc * _atc_iv_v1 * math.sqrt(1.0 / 12.0) * 0.4
            _atc_months_v1 = _atc_gap_v1 / _atc_cc_v1 if _atc_cc_v1 > 0 else float('inf')
            result.update({
                "Action": "HOLD",
                "Urgency": "MEDIUM",
                "Rationale": (
                    f"Assignment terminal (DTE={_sp_dte:.0f}, Δ={_sp_delta_abs:.2f}): "
                    f"deep ITM (intrinsic ${_sp_intrinsic:.2f}/sh). "
                    f"Rolling is uneconomical — accept assignment at basis "
                    f"${_atc_basis_v1:.2f} and start wheel. "
                    f"IV {_atc_iv_v1:.0%} supports CC premium (~${_atc_cc_v1:.2f}/sh/mo). "
                    + (f"Est. {_atc_months_v1:.0f} months to breakeven. " if _atc_months_v1 < 999 else "")
                    + f"Thesis {_atc_thesis_v1}. "
                    f"(Passarelli Ch.1: wheel conversion; Jabbour Ch.4: repair strategy.)"
                ),
                "Doctrine_Source": "Passarelli Ch.1 + Jabbour Ch.4: Terminal Assignment — Wheel Recovery",
                "Doctrine_State": "RECOVERY_LADDER",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True,
            })
            return result

        result.update({
            "Action": "EXIT",
            "Urgency": "HIGH",
            "Rationale": (
                f"Assignment terminal: DTE={_sp_dte:.0f} ≤ 7, Delta={_sp_delta_abs:.2f} > 0.90. "
                f"Short put is deep ITM (intrinsic ≈${_sp_intrinsic:.2f}/share). "
                f"Roll credit is structurally insufficient to offset the intrinsic loss. "
                f"Stop rolling — either CLOSE for defined loss or accept assignment at effective basis. "
                f"(Jabbour Ch.8: terminal assignment case; Natenberg Ch.15: no EV in deep-ITM roll.)"
            ),
            "Doctrine_Source": "Jabbour Ch.8 + Natenberg Ch.15: Assignment Terminal Case",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True,
        })
        return result

    # 1a. DTE<7 hard gate
    if _sp_dte < 7 and is_itm:
        if _wheel_ready:
            result.update({
                "Action": "HOLD",
                "Urgency": "LOW",
                "Rationale": (
                    f"DTE={_sp_dte:.0f} < 7, ITM — assignment imminent. "
                    f"Wheel Ready: {_wheel_note} "
                    f"Prepare to accept stock and sell covered call at/above basis. "
                    f"Do NOT roll — rolling here locks in a loss and defers the wheel entry "
                    f"(Passarelli Ch.1: effective basis = strike - premium = planned entry price)."
                ),
                "Doctrine_Source": "Passarelli Ch.1: Wheel Assignment — Intentional Acquisition",
                "Decision_State": STATE_NEUTRAL_CONFIDENT,
                "Required_Conditions_Met": True
            })
            return result
        elif not assignment_acceptable:
            result.update({
                "Action": "ROLL",
                "Urgency": "HIGH",
                "Rationale": (
                    f"DTE={_sp_dte:.0f} < 7 with ITM put and assignment unacceptable — "
                    f"gamma and pin risk accelerating. Roll down/out now to defend assignment. "
                    f"{_wheel_note} "
                    f"(McMillan Ch.7: expiration management, short DTE ITM = urgent)."
                ),
                "Doctrine_Source": "McMillan Ch.7: Short DTE ITM Defense",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result

    if is_late and is_itm:
        if _wheel_ready:
            result.update({
                "Action": "HOLD",
                "Urgency": "LOW",
                "Rationale": (
                    f"Late lifecycle + ITM — assignment approaching. "
                    f"Wheel Ready: {_wheel_note} "
                    f"Accept stock at effective basis ${_effective_basis_w:.2f}. "
                    f"After assignment, sell covered call to continue income cycle."
                ),
                "Doctrine_Source": "Passarelli Ch.1 + McMillan: Wheel — Accept Assignment",
                "Decision_State": STATE_NEUTRAL_CONFIDENT,
                "Required_Conditions_Met": True
            })
            return result
        elif not assignment_acceptable:
            result.update({
                "Action": "ROLL",
                "Urgency": "HIGH",
                "Rationale": (
                    f"Assignment undesirable (weak ticker health) + Expiration proximity. "
                    f"{_wheel_note} "
                    f"Roll to defend."
                ),
                "Doctrine_Source": "McMillan: Expiration Management",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result
        else:
            result.update({
                "Action": "HOLD",
                "Urgency": "LOW",
                "Rationale": (
                    f"Assignment acceptable (deferred entry path valid). Holding for strategic entry. "
                    f"{_wheel_note}"
                ),
                "Doctrine_Source": "RAG: Strategic Assignment",
                "Decision_State": STATE_NEUTRAL_CONFIDENT,
                "Required_Conditions_Met": True
            })
            return result

    # ── Recently-Rolled Cooldown gate (Signal Coherence Gate 1) ─────────
    # Income strategies (CSP): 3 trading-day cooldown.
    # Suppresses discretionary ROLL when the current leg was just opened,
    # preventing self-contradicting roll chains (Natenberg Ch.7, Jabbour Ch.8).
    _COOLDOWN_DAYS_SP = 3
    _days_since_roll_sp = row.get('Days_Since_Last_Roll')
    _thesis_for_cooldown_sp = str(row.get('Thesis_State', '') or '').upper()
    if (
        pd.notna(_days_since_roll_sp)
        and float(_days_since_roll_sp) < _COOLDOWN_DAYS_SP
        and _thesis_for_cooldown_sp in ('INTACT', 'UNKNOWN', '')
    ):
        result.update({
            "Action": "HOLD",
            "Urgency": "LOW",
            "Rationale": (
                f"Recently-rolled cooldown: current leg opened {int(_days_since_roll_sp)}d ago "
                f"(< {_COOLDOWN_DAYS_SP}d window). Thesis is {_thesis_for_cooldown_sp or 'UNKNOWN'} — "
                f"suppressing discretionary ROLL to prevent self-contradicting roll chains. "
                f"Natenberg Ch.7: 'Frequent adjustments cost more than the risk they mitigate.' "
                f"Jabbour Ch.8: 'Repair is a dangerous misnomer for overtrading.'"
            ),
            "Doctrine_Source": "Natenberg Ch.7 + Jabbour Ch.8: Recently-Rolled Cooldown",
            "Decision_State": STATE_NEUTRAL_CONFIDENT,
            "Required_Conditions_Met": True,
        })
        return result

    # 1b. Hard 21-DTE exit gate for income strategies (short put)
    _sp_50_pct_gate_dte = safe_row_float(row, 'DTE', default=999.0)
    _sp_premium_entry_21 = abs(safe_row_float(row, 'Premium_Entry', 'Short_Call_Premium'))
    _sp_last_21 = abs(safe_row_float(row, 'Last'))
    _sp_profit_captured = (
        (_sp_premium_entry_21 - _sp_last_21) / _sp_premium_entry_21
        if _sp_premium_entry_21 > 0 else 0.0
    )
    if (
        _sp_50_pct_gate_dte <= 21
        and _sp_50_pct_gate_dte >= 7
        and _sp_profit_captured < 0.50
        and not (_wheel_ready and not is_itm)
    ):
        # ── Roll-vs-Assignment Cost Gate ──────────────────────────────────
        # When the put's market value exceeds 50% of the stock price, rolling
        # is structurally uneconomical — the buyback cost dwarfs any roll credit.
        # Assignment + covered calls (or close for defined loss) is better.
        _sp_intrinsic_rvs = max(0.0, _strike_for_wheel - _spot_for_wheel) if (_strike_for_wheel > 0 and _spot_for_wheel > 0) else 0.0
        _sp_roll_cost_proxy = max(_sp_last_21, _sp_intrinsic_rvs)
        if _sp_roll_cost_proxy > 0 and _spot_for_wheel > 0 and _sp_roll_cost_proxy > 0.50 * _spot_for_wheel:
            # Roll cost exceeds 50% of stock — stop rolling, take assignment or close
            _rvs_roll_cost_total = _sp_roll_cost_proxy * _qty_w * 100.0
            _rvs_assign_cost = _strike_for_wheel * _qty_w * 100.0
            _rvs_cc_target = _effective_basis_w if _effective_basis_w > 0 else _strike_for_wheel
            if _wheel_ready:
                # Wheel path: accept assignment, sell CC at/above basis
                result.update({
                    "Action": "HOLD",
                    "Urgency": "LOW",
                    "Rationale": (
                        f"Roll-vs-assignment cost gate: put value ${_sp_roll_cost_proxy:.2f} > "
                        f"50% of stock ${_spot_for_wheel:.2f} "
                        f"(intrinsic ${_sp_intrinsic_rvs:.2f} = "
                        f"{_sp_intrinsic_rvs/_spot_for_wheel:.0%} of stock price). "
                        f"Rolling costs ~${_rvs_roll_cost_total:,.0f} to defer — "
                        f"assignment costs ${_rvs_assign_cost:,.0f} to own the stock. "
                        f"✅ Wheel Ready: LET ASSIGN → sell CC at/above ${_rvs_cc_target:.2f} "
                        f"(effective basis). Assignment is the intended outcome — "
                        f"rolling here pays to avoid what you planned for. "
                        f"(Passarelli Ch.1: 'effective purchase price = strike - premium'; "
                        f"Jabbour Ch.8: roll EV negative when intrinsic > net credit.)"
                    ),
                    "Doctrine_Source": "Passarelli Ch.1 + Jabbour Ch.8: Accept Assignment — Wheel Conversion",
                    "Decision_State": STATE_NEUTRAL_CONFIDENT,
                    "Required_Conditions_Met": True,
                })
            else:
                # Not wheel-ready: close for defined loss or accept assignment with warning
                result.update({
                    "Action": "EXIT",
                    "Urgency": "HIGH",
                    "Rationale": (
                        f"Roll-vs-assignment cost gate: put value ${_sp_roll_cost_proxy:.2f} > "
                        f"50% of stock ${_spot_for_wheel:.2f} "
                        f"(intrinsic ${_sp_intrinsic_rvs:.2f} = "
                        f"{_sp_intrinsic_rvs/_spot_for_wheel:.0%} of stock price). "
                        f"Rolling costs ~${_rvs_roll_cost_total:,.0f} to defer — "
                        f"no achievable roll credit offsets the buyback. "
                        f"{_wheel_note} "
                        f"Options: (A) close put for defined loss (${_sp_roll_cost_proxy:.2f}/share), "
                        f"(B) accept assignment at ${_strike_for_wheel:.2f} and sell CC to recover "
                        f"(but basis ${_rvs_cc_target:.2f} is {(_rvs_cc_target - _spot_for_wheel)/_spot_for_wheel:+.0%} "
                        f"above current price — recovery may take multiple CC cycles). "
                        f"(Jabbour Ch.8: roll EV turns negative when intrinsic exceeds net credit; "
                        f"Natenberg Ch.15: deep ITM roll = paying to defer, not to profit.)"
                    ),
                    "Doctrine_Source": "Jabbour Ch.8 + Natenberg Ch.15: Roll-vs-Assignment Cost Gate",
                    "Decision_State": STATE_ACTIONABLE,
                    "Required_Conditions_Met": True,
                })
            return result

        _sp_21_urgency = 'MEDIUM' if _sp_profit_captured >= 0 else 'HIGH'

        # Strategy-aware IV regime check (Chan 0.786)
        _sp_iv_entry_21 = safe_row_float(row, 'IV_Entry')
        _sp_iv_now_21   = safe_row_float(row, 'IV_30D', 'IV_Now')
        _sp_iv_pctile_21 = safe_row_float(row, 'IV_Percentile', default=50.0)
        _sp_iv_gap_21   = safe_row_float(row, 'IV_vs_HV_Gap')

        _sp_iv_collapsed_21 = (
            _sp_iv_entry_21 > 0 and _sp_iv_now_21 > 0
            and (_sp_iv_now_21 / _sp_iv_entry_21) < 0.70
            and _sp_iv_pctile_21 < 25
            and _sp_iv_gap_21 <= 0
        )

        if _sp_iv_collapsed_21:
            result.update({
                "Action": "HOLD",
                "Urgency": "LOW",
                "Rationale": (
                    f"21-DTE income gate: DTE={_sp_50_pct_gate_dte:.0f}, "
                    f"{_sp_profit_captured:.0%} captured. "
                    f"Vol regime shift: IV contracted "
                    f"{(1 - _sp_iv_now_21/_sp_iv_entry_21):.0%} from entry "
                    f"({_sp_iv_entry_21:.1%} -> {_sp_iv_now_21:.1%}), "
                    f"IV_Percentile={_sp_iv_pctile_21:.0f}, "
                    f"IV-HV gap={_sp_iv_gap_21:+.1%}. "
                    f"Rolling into a low-IV environment yields thin premium. "
                    f"Let current put decay (remaining TV = ${_sp_last_21:.2f}). "
                    f"Chan: 'Mean-reversion exit when regime shifts.' "
                    f"Natenberg Ch.8: 'Selling premium below HV = negative EV.'"
                ),
                "Doctrine_Source": "Chan: Strategy-aware exit — Vol regime shift (CSP)",
                "Decision_State": STATE_NEUTRAL_CONFIDENT,
                "Required_Conditions_Met": True,
            })
            return result

        _sp_21_pnl = (
            f"up {_sp_profit_captured:.0%}" if _sp_profit_captured > 0
            else f"down {abs(_sp_profit_captured):.0%}" if _sp_profit_captured < 0
            else "flat"
        )
        # Position Trajectory context
        _sp_regime_21 = str(row.get('Position_Regime', 'NEUTRAL') or 'NEUTRAL')
        _sp_consec_debits_21 = int(safe_row_float(row, 'Trajectory_Consecutive_Debit_Rolls'))
        _sp_stock_ret_21 = safe_row_float(row, 'Trajectory_Stock_Return')
        if _sp_regime_21 == 'TRENDING_CHASE':
            _sp_21_urgency = 'HIGH'
            result.update({
                "Action": "ROLL",
                "Urgency": _sp_21_urgency,
                "Rationale": (
                    f"21-DTE income gate: DTE={_sp_50_pct_gate_dte:.0f} ≤ 21 with {_sp_profit_captured:.0%} captured. "
                    f"⚠️ TRENDING_CHASE: stock has moved {_sp_stock_ret_21:+.0%} since entry "
                    f"with {_sp_consec_debits_21} consecutive debit roll(s). "
                    f"Stock is structurally declining through put strikes — this is NOT a temporary dip. "
                    f"Consider: (A) accept assignment if wheel-ready at this strike, "
                    f"(B) buy back put and wait for stabilization, or "
                    f"(C) roll to a much lower strike if premium justifies risk. "
                    f"Rolling to the next monthly repeats the chase cycle. "
                    f"(McMillan Ch.3: strike-chase recognition; Given Ch.6: 21-DTE gate)"
                ),
                "Doctrine_Source": "McMillan Ch.3 + Position Trajectory: Strike Chase at 21-DTE",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True,
            })
        else:
            _sp_regime_note_21 = ""
            if _sp_regime_21 in ('SIDEWAYS_INCOME', 'MEAN_REVERSION'):
                _sp_regime_note_21 = (
                    f" {_sp_regime_21}: roll OUT at same strike for credit — "
                    "do NOT roll DOWN to a lower strike (debit). "
                    "Stock is range-bound; income cycle is working. "
                    "If assigned at current strike, that's the wheel entry "
                    "(McMillan Ch.3: same-strike credit rolls in range-bound regimes)."
                )
            result.update({
                "Action": "ROLL",
                "Urgency": _sp_21_urgency,
                "Rationale": (
                    f"21-DTE income gate: DTE={_sp_50_pct_gate_dte:.0f} ≤ 21 with only "
                    f"{_sp_profit_captured:.0%} profit captured (need ≥50%). "
                    f"Position {_sp_21_pnl} — gamma-theta ratio has degraded; "
                    f"short put edge is structurally exhausted. "
                    f"Buy back current put and roll out 30-45 DTE to reset theta efficiency. "
                    f"(Given Ch.6: 21-DTE income roll; Passarelli Ch.2: gamma-theta degradation.)"
                    f"{_sp_regime_note_21}"
                ),
                "Doctrine_Source": "Given Ch.6 + Passarelli Ch.2: 21-DTE Income Roll Gate",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True,
            })
        return result

    # 2. Expectancy Preservation Logic (Reversion-Aware)
    if row.get('Drift_Direction') == 'Up':
        struct_state = str(row.get('PriceStructure_State', 'STABLE')).upper()
        mom_state = str(row.get('MomentumVelocity_State', 'UNKNOWN')).upper()
        vol_state = str(row.get('VolatilityState_State', 'NORMAL')).upper()

        reversion_prob_collapse = ("STRUCTURE_BROKEN" in struct_state or "REVERSING" in mom_state)
        low_continuation_value = row.get('Drift_Magnitude') == 'High'
        vol_expansion = vol_state in ["EXPANDING", "EXTREME"]

        if reversion_prob_collapse and low_continuation_value and vol_expansion:
            result.update({
                "Action": "TRIM",
                "Urgency": "MEDIUM",
                "Rationale": "Triple-gate met: Reversion edge collapsed + Low continuation value + Vol expansion.",
                "Doctrine_Source": "RAG: Expectancy Preservation",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result

        if low_continuation_value and not reversion_prob_collapse:
            result.update({
                "Action": "HOLD_FOR_REVERSION",
                "Urgency": "LOW",
                "Rationale": "Extended success with intact structure; holding for premium regeneration.",
                "Doctrine_Source": "RAG: Expectancy Preservation",
                "Decision_State": STATE_NEUTRAL_CONFIDENT,
                "Required_Conditions_Met": True
            })
            return result

    # 3. Default Neutrality
    rationale = "Position is OTM or has sufficient time remaining. No action required."
    if is_itm:
        rationale = "Position is ITM but has sufficient time remaining. Assignment acceptable." if assignment_acceptable else "Position is ITM; monitoring for defense."

    result.update({
        "Action": "HOLD",
        "Urgency": "LOW",
        "Rationale": rationale,
        "Doctrine_Source": "McMillan: Neutral Maintenance",
        "Decision_State": STATE_NEUTRAL_CONFIDENT,
        "Required_Conditions_Met": True
    })

    # 3a. Thesis regime degradation (short put)
    _vol_state_sp = str(row.get('VolatilityState_State', '') or '').split('.')[-1].upper()
    _iv_entry_sp  = safe_row_float(row, 'IV_Entry')
    _iv_now_sp    = safe_row_float(row, 'IV_Now', 'IV_30D')
    _iv_entry_low = _iv_entry_sp < 0.25 if _iv_entry_sp > 0 else False
    _iv_expanded  = _iv_now_sp > _iv_entry_sp * 1.5 if (_iv_entry_sp > 0 and _iv_now_sp > 0) else False

    thesis = check_thesis_degradation(row)
    if _vol_state_sp == 'EXTREME' and _iv_expanded and not is_itm:
        result.update({
            "Action": "ROLL",
            "Urgency": "HIGH",
            "Rationale": (
                f"Vol regime EXTREME (IV now {_iv_now_sp:.1%} vs entry {_iv_entry_sp:.1%}, "
                f"+{(_iv_now_sp - _iv_entry_sp):.1%}). "
                f"Short put sold into low-vol environment is now exposed to a regime where "
                f"IV expansion dwarfs the premium collected. "
                f"Roll down-and-out or buy protective put to reduce delta exposure "
                f"(Passarelli Ch.6: vol regime flip = edge reversal)."
            ),
            "Doctrine_Source": "Passarelli Ch.6: Vol Regime Flip — Edge Reversal",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True,
        })
        return result
    elif thesis:
        result['Urgency'] = 'MEDIUM'
        result['Rationale'] = (
            f"Entry regime degraded: {thesis['text']}. " + result['Rationale']
        )

    # 3b. IV term structure note (Natenberg Ch.11)
    iv_shape_sp = str(row.get('iv_surface_shape', '') or '').upper()
    if iv_shape_sp == 'BACKWARDATION':
        slope_sp = safe_row_float(row, 'iv_ts_slope_30_90')
        result['Rationale'] += (
            f" IV BACKWARDATED ({slope_sp:+.1f}pt 30-90d): short put collecting "
            f"above-normal near-term IV — premium favorable (Natenberg Ch.11)."
        )

    # 3c. Forward expectancy note
    _ev_ratio_sp = safe_row_float(row, 'EV_Feasibility_Ratio') if pd.notna(row.get('EV_Feasibility_Ratio')) else float('nan')
    _em_10_sp    = safe_row_float(row, 'Expected_Move_10D')
    _req_sp      = safe_row_float(row, 'Required_Move_Breakeven', 'Required_Move')
    if not pd.isna(_ev_ratio_sp) and _ev_ratio_sp > 0 and _em_10_sp > 0:
        _sp_context = (
            f" Expected 10D move: ${_em_10_sp:.1f}. "
            f"Required move to breakeven: ${_req_sp:.1f} "
            f"({_ev_ratio_sp:.2f}× expected). "
        )
        if _ev_ratio_sp > 1.5:
            _sp_context += (
                f"Stock needs to move {_ev_ratio_sp:.1f}× the 10D expected move to reach breakeven — "
                f"assignment is statistically likely within this DTE window."
            )
        else:
            _sp_context += f"Within expected 10D range — breakeven is statistically reachable."
        result['Rationale'] += _sp_context

    return result


# ── v2: Proposal-based evaluation ────────────────────────────────────────────

def short_put_doctrine_v2(row: pd.Series, result: Dict) -> Dict:
    """Proposal-based SHORT_PUT (CSP) evaluation.

    All gates propose actions into a ProposalCollector instead of returning
    immediately. A resolver picks the best action using deterministic EV
    and MC evidence.

    Original ``short_put_doctrine()`` is preserved unchanged for A/B testing.
    """
    collector = ProposalCollector()

    # ── Wheel Assessment (runs first — persists to result regardless of gate path) ─
    _spot_for_wheel     = safe_row_float(row, 'UL Last')
    _strike_for_wheel   = safe_row_float(row, 'Strike')
    _premium_entry_w    = abs(safe_row_float(row, 'Premium_Entry'))
    _net_cost_basis_w   = safe_row_float(row, 'Net_Cost_Basis_Per_Share')
    _broker_basis_w     = abs(safe_row_float(row, 'Basis'))
    _qty_w              = abs(safe_row_float(row, 'Quantity', default=1.0))
    _dte_for_wheel      = safe_row_float(row, 'DTE', default=999.0)
    _iv_now_w_raw       = safe_row_float(row, 'IV_Now', 'IV_30D')
    _hv_20d_w           = safe_row_float(row, 'HV_20D')
    if _dte_for_wheel <= 2 and _hv_20d_w > 0:
        _iv_now_w       = _hv_20d_w
        _iv_source_w    = "HV_20D (near-expiry — option IV unreliable)"
    else:
        _iv_now_w       = _iv_now_w_raw
        _iv_source_w    = "IV_Now/IV_30D"
    _delta_util_w       = safe_row_float(row, 'Portfolio_Delta_Utilization_Pct')
    _trend_w            = str(row.get('TrendIntegrity_State', '') or '').split('.')[-1].upper()
    _price_struct_w     = str(row.get('PriceStructure_State', '') or '').split('.')[-1].upper()
    _mc_assign_raw = row.get('MC_Assign_P_Expiry')
    if pd.notna(_mc_assign_raw) and _mc_assign_raw not in (None, '', 0):
        _mc_assign_p_w = float(_mc_assign_raw)
    else:
        _delta_abs = abs(safe_row_float(row, 'Delta'))
        _mc_assign_p_w = min(1.0, _delta_abs * 1.2) if _delta_abs > 0.05 else 0.0

    # Effective basis per share
    _broker_cost_per_share_w = (
        (_broker_basis_w / _qty_w / 100.0)
        if (_qty_w > 0 and _broker_basis_w > 0)
        else 0.0
    )
    if _net_cost_basis_w > 0:
        _effective_basis_w = _net_cost_basis_w
    elif _strike_for_wheel > 0 and _premium_entry_w > 0:
        _effective_basis_w = _strike_for_wheel - _premium_entry_w
    elif _broker_cost_per_share_w > 0:
        _effective_basis_w = _broker_cost_per_share_w
    else:
        _effective_basis_w = 0.0

    _wheel_basis_ok = (
        _effective_basis_w > 0
        and _spot_for_wheel > 0
        and _effective_basis_w <= _spot_for_wheel * 0.97
    )
    _market_structure_w = str(row.get('Market_Structure', 'Unknown') or 'Unknown')
    _wheel_chart_ok = (
        'BROKEN' not in _trend_w
        and 'BROKEN' not in _price_struct_w
        and _market_structure_w != 'Downtrend'
    )
    from core.shared.finance_utils import normalize_iv as _normalize_iv
    _iv_now_w_norm = _normalize_iv(_iv_now_w) or 0.0
    _wheel_iv_ok = _iv_now_w_norm >= 0.25
    _wheel_capital_ok = _delta_util_w < 15.0

    _wheel_ready = _wheel_basis_ok and _wheel_chart_ok and _wheel_iv_ok and _wheel_capital_ok

    _wheel_fails = []
    if not _wheel_basis_ok:
        _basis_note = f"${_effective_basis_w:.2f}" if _effective_basis_w > 0 else "unknown"
        _wheel_fails.append(f"basis {_basis_note} not at ≥3% discount (spot=${_spot_for_wheel:.2f})")
    if not _wheel_chart_ok:
        _fail_parts = []
        if 'BROKEN' in _trend_w or 'BROKEN' in _price_struct_w:
            _fail_parts.append(f"structure broken (Trend={_trend_w}, Price={_price_struct_w})")
        if _market_structure_w == 'Downtrend':
            _fail_parts.append("swing structure downtrend (Murphy Ch.4: LH/LL pattern)")
        _wheel_fails.append("; ".join(_fail_parts) if _fail_parts else f"chart check failed")
    if not _wheel_iv_ok:
        _wheel_fails.append(f"IV {_iv_now_w_norm:.0%} < 25% — CC premium too thin ({_iv_source_w})")
    if not _wheel_capital_ok:
        _wheel_fails.append(f"delta utilization {_delta_util_w:.1f}% ≥ 15% — overconcentrated")

    if _wheel_ready:
        _wheel_note = (
            f"✅ Wheel Ready — assignment is a FEATURE: "
            f"effective basis ${_effective_basis_w:.2f} vs spot ${_spot_for_wheel:.2f} "
            f"({(_spot_for_wheel - _effective_basis_w)/_spot_for_wheel:.1%} discount). "
            f"IV={_iv_now_w_norm:.0%} ({_iv_source_w}) supports CC entry. "
            f"Chart intact. Delta util={_delta_util_w:.1f}%. "
            f"Passarelli Ch.1: 'The effective purchase price is strike minus premium — "
            f"assignment at a discount is the intended outcome of a CSP.' "
            f"Next: accept stock, sell covered call at/above basis."
        )
    else:
        _wheel_note = (
            f"⚠️ Wheel NOT Ready — "
            + "; ".join(_wheel_fails)
            + f". Standard assignment defense applies."
        )

    # Signal Hub: OBV distribution warning (Murphy Ch.7)
    _obv_slope_sp = safe_row_float(row, 'OBV_Slope')
    if _obv_slope_sp < -10:
        _wheel_note += (
            f" OBV distributing ({_obv_slope_sp:.1f}%) — smart money "
            f"may be exiting (Murphy Ch.7). Assignment into distribution "
            f"trend increases basis recovery difficulty."
        )

    result['Wheel_Ready']  = _wheel_ready
    result['Wheel_Note']   = _wheel_note
    result['Wheel_Basis']  = round(_effective_basis_w, 2) if _effective_basis_w > 0 else None
    result['Wheel_IV_Ok']  = _wheel_iv_ok
    result['Wheel_Chart_Ok'] = _wheel_chart_ok
    result['Wheel_Capital_Ok'] = _wheel_capital_ok

    # MC_Assign_P_Expiry gate (annotation — urgency escalation, not a decision gate)
    result['MC_Assign_P_Expiry_Used'] = round(_mc_assign_p_w, 3) if _mc_assign_p_w > 0 else None
    if _mc_assign_p_w > 0.75 and not _wheel_ready:
        _current_urgency = str(result.get('Urgency', 'LOW')).upper()
        if _current_urgency not in ('HIGH', 'CRITICAL'):
            result['Urgency'] = 'HIGH'
            result['Rationale'] = (
                (result.get('Rationale') or '') +
                f" | ⚡ MC: P(assign by expiry)={_mc_assign_p_w:.0%} — assignment is "
                f"statistically probable (>75%) and Wheel not Ready. Roll to defend "
                f"or close position before assignment probability locks in "
                f"(McMillan Ch.7 + Natenberg Ch.19: quantitative assignment directive)."
            )

    # ── Consecutive debit roll hard stop ─────────────────────────────────
    _r_cdr_sp = result.copy()
    _fired_cdr_sp, _r_cdr_sp = gate_consecutive_debit_roll_stop(
        row=row, result=_r_cdr_sp, strategy_label="CSP",
    )
    if _fired_cdr_sp:
        propose_gate(
            collector, "consecutive_debit_roll_stop",
            action=_r_cdr_sp.get("Action", "EXIT"),
            urgency=_r_cdr_sp.get("Urgency", "HIGH"),
            rationale=_r_cdr_sp.get("Rationale", ""),
            doctrine_source=_r_cdr_sp.get("Doctrine_Source", ""),
            priority=2, is_hard_veto=True,
            exit_trigger_type="INCOME",
        )

    # ── Fading Winner trailing protection ─────────────────────────────────
    _pnl_fw_sp = safe_pnl_pct(row)
    _r_fw_sp = result.copy()
    _fired_fw_sp, _r_fw_sp = gate_fading_winner(
        row=row, pnl_pct=_pnl_fw_sp, result=_r_fw_sp, strategy_label="CSP",
    )
    if _fired_fw_sp:
        propose_gate(
            collector, "fading_winner",
            action=_r_fw_sp.get("Action", "EXIT"),
            urgency=_r_fw_sp.get("Urgency", "MEDIUM"),
            rationale=_r_fw_sp.get("Rationale", ""),
            doctrine_source=_r_fw_sp.get("Doctrine_Source", ""),
            priority=3,
            exit_trigger_type="INCOME",
        )

    # ── Earnings Lockdown Guard ──────────────────────────────────────────
    _earn_date_sp = row.get('Earnings_Date')
    _days_to_earn_sp = None
    if _earn_date_sp not in (None, '', 'nan', 'N/A') and not (
        isinstance(_earn_date_sp, float) and pd.isna(_earn_date_sp)
    ):
        try:
            _ed_sp = pd.to_datetime(str(_earn_date_sp), errors='coerce')
            if pd.notna(_ed_sp):
                _snap_sp = pd.to_datetime(row.get('Snapshot_TS') or pd.Timestamp.now())
                _days_to_earn_sp = (_ed_sp.normalize() - _snap_sp.normalize()).days
        except Exception as _sp_earn_err:
            logger.debug(f"Short put v2 earnings date parse skipped: {_sp_earn_err}")
    if _days_to_earn_sp is not None and 0 <= _days_to_earn_sp <= 2:
        _earn_itm_sp = row.get('Moneyness_Label') == 'ITM'
        _earn_wheel_ok = _wheel_ready and not _earn_itm_sp
        if not _earn_wheel_ok:
            propose_gate(
                collector, "earnings_lockdown",
                action="EXIT", urgency="HIGH",
                rationale=(
                    f"Earnings lockdown: earnings in {_days_to_earn_sp}d. "
                    f"Rolling into a binary event is structurally invalid — "
                    f"gap risk cannot be delta-hedged. "
                    f"Exit before announcement: close the short put to eliminate "
                    f"assignment gap exposure. "
                    f"(Jabbour Ch.8: never roll into earnings; Given Ch.4: close before event.)"
                ),
                doctrine_source="Jabbour Ch.8 + Given Ch.4: Earnings Lockdown",
                priority=8, exit_trigger_type="CAPITAL",
            )

    # ── Calendar gates ───────────────────────────────────────────────────
    try:
        from scan_engine.calendar_context import expiry_proximity_flag
        _dte_sp_cal   = safe_row_float(row, 'DTE', default=999.0)
        _ul_sp_cal    = safe_row_float(row, 'UL Last')
        _strike_sp_cal = safe_row_float(row, 'Strike')
        _strat_sp_cal  = str(row.get('Strategy', '') or '').upper()
        _exp_flag_sp, _exp_note_sp = expiry_proximity_flag(
            dte=_dte_sp_cal, strategy=_strat_sp_cal,
            ul_last=_ul_sp_cal, strike=_strike_sp_cal,
        )
        if _exp_flag_sp == 'PIN_RISK':
            propose_gate(
                collector, "pin_risk",
                action="EXIT", urgency="CRITICAL",
                rationale=_exp_note_sp,
                doctrine_source="McMillan Ch.7 + Natenberg Ch.15: Pin Risk",
                priority=1, is_hard_veto=True, exit_trigger_type="GAMMA",
            )
        elif _exp_flag_sp == 'GAMMA_CRITICAL':
            propose_gate(
                collector, "gamma_critical",
                action="ROLL", urgency="HIGH",
                rationale=_exp_note_sp,
                doctrine_source="Natenberg Ch.15: Gamma Critical — Force Decision",
                priority=5,
            )
        elif _exp_flag_sp == 'PRE_HOLIDAY_EXPIRY':
            if _wheel_ready:
                propose_gate(
                    collector, "pre_holiday_expiry",
                    action="HOLD", urgency="LOW",
                    rationale=(
                        f"Pre-holiday expiry — BUT Wheel Ready: {_wheel_note} "
                        f"Accepting assignment here is preferable to rolling into "
                        f"holiday-thin markets. After assignment, sell CC when liquidity returns."
                    ),
                    doctrine_source="Passarelli Ch.1 (Wheel Override): Pre-Holiday Assignment Acceptance",
                    priority=12,
                )
            else:
                propose_gate(
                    collector, "pre_holiday_expiry",
                    action="ROLL", urgency="HIGH",
                    rationale=_exp_note_sp,
                    doctrine_source="Hull Ch.18 + Passarelli Ch.6: Pre-Holiday Expiry",
                    priority=12,
                )
    except Exception as _sp_exp_err:
        logger.debug(f"Short put v2 expiration/calendar gate skipped: {_sp_exp_err}")

    # ── Extract common fields ────────────────────────────────────────────
    is_late = row.get('Lifecycle_Phase') == 'TERMINAL'
    is_itm = row.get('Moneyness_Label') == 'ITM'
    assignment_acceptable = row.get('Assignment_Acceptable', True)
    _sp_dte = safe_row_float(row, 'DTE', default=999.0)

    # ── 0. Deep Underwater Gate ────────────────────────────────────────────
    # Recovery path: if thesis INTACT and IV supports CC premium, accepting
    # assignment and wheeling is a recovery strategy.  The hard veto only
    # fires when recovery is NOT viable (thesis BROKEN, IV crushed, etc.).
    _sp_spot_uw = safe_row_float(row, 'UL Last')
    _sp_strike_uw = safe_row_float(row, 'Strike')
    _sp_theta_uw = abs(safe_row_float(row, 'Theta'))
    if (
        _sp_strike_uw > 0
        and _sp_spot_uw > 0
        and _sp_spot_uw < _sp_strike_uw * 0.60
        and _sp_theta_uw < 0.01
        and not _wheel_ready
    ):
        _uw_deficit_pct = 1.0 - (_sp_spot_uw / _sp_strike_uw)
        _uw_basis_note = f" Effective basis ${_effective_basis_w:.2f}" if _effective_basis_w > 0 else ""

        # CSP recovery viability check
        _csp_thesis = str(row.get('Thesis_State', '') or '').upper()
        _csp_iv_recov = _iv_now_w_norm  # normalized above
        _csp_iv_ok = _csp_iv_recov > 0.15
        _csp_recovery_ok = _csp_thesis != 'BROKEN' and _csp_iv_ok and _wheel_capital_ok

        if _csp_recovery_ok:
            # Wheel recovery economics
            import math
            _csp_basis = _effective_basis_w if _effective_basis_w > 0 else _sp_strike_uw
            _csp_gap = _csp_basis - _sp_spot_uw
            _csp_monthly_cc = _sp_spot_uw * _csp_iv_recov * math.sqrt(1.0 / 12.0) * 0.4
            _csp_months_be = _csp_gap / _csp_monthly_cc if _csp_monthly_cc > 0 else float('inf')
            _csp_months_str = (
                f" Est. {_csp_months_be:.0f} months of CC cycles to breakeven."
                if _csp_months_be < 999 else ""
            )
            # Catalyst context
            _csp_catalyst = []
            _csp_days_earn = safe_row_float(row, 'days_to_earnings', default=999.0)
            _csp_beat_rate = safe_row_float(row, 'Earnings_Beat_Rate')
            if _csp_days_earn < 45:
                _e_note = f"Earnings in {_csp_days_earn:.0f}d"
                if _csp_beat_rate > 0:
                    _e_note += f" (beat rate {_csp_beat_rate:.0%})"
                _csp_catalyst.append(_e_note)
            _csp_days_macro = safe_row_float(row, 'Days_To_Macro', default=999.0)
            _csp_macro_evt = str(row.get('Macro_Next_Event', '') or '')
            if _csp_days_macro < 10 and _csp_macro_evt:
                _csp_catalyst.append(f"{_csp_macro_evt} in {_csp_days_macro:.0f}d")
            _csp_adx = safe_row_float(row, 'adx_14', default=25.0)
            _csp_trend = str(row.get('TrendIntegrity_State', '') or '').upper()
            if _csp_adx < 20 and _csp_trend in ('NO_TREND', 'TREND_EXHAUSTED', ''):
                _csp_catalyst.append(f"stock basing (ADX {_csp_adx:.0f})")
            _csp_catalyst_str = (
                f" Next catalyst: {'; '.join(_csp_catalyst)}."
                if _csp_catalyst else ""
            )

            # Propose recovery HOLD (accept assignment → wheel)
            propose_gate(
                collector, "deep_underwater_recovery_wheel",
                action="HOLD", urgency="MEDIUM",
                rationale=(
                    f"Deep underwater CSP recovery: stock ${_sp_spot_uw:.2f} is "
                    f"{_uw_deficit_pct:.0%} below strike ${_sp_strike_uw:.2f}."
                    f"{_uw_basis_note} "
                    f"EXIT locks in ${_csp_gap:.2f}/sh permanent loss. "
                    f"Assignment → wheel conversion: accept stock at basis "
                    f"${_csp_basis:.2f}, sell CCs at IV {_csp_iv_recov:.0%} "
                    f"(~${_csp_monthly_cc:.2f}/sh/mo).{_csp_months_str}"
                    f"{_csp_catalyst_str} "
                    f"Thesis {_csp_thesis} — hold through assignment, start CC income cycle. "
                    f"(Passarelli Ch.1: wheel conversion; Jabbour Ch.4: repair strategy; "
                    f"McMillan Ch.3: basis reduction via covered writing.)"
                ),
                doctrine_source="Passarelli Ch.1 + Jabbour Ch.4: CSP Recovery — Wheel Conversion",
                priority=2,
            )
            # No hard veto EXIT — recovery is viable, let resolver compare
        else:
            # Recovery not viable — hard veto EXIT
            _csp_block = (
                f"thesis {_csp_thesis}" if _csp_thesis == 'BROKEN'
                else f"IV {_csp_iv_recov:.0%} too low for CC income"
                if not _csp_iv_ok else "overconcentrated"
            )
            propose_gate(
                collector, "deep_underwater",
                action="EXIT", urgency="HIGH",
                rationale=(
                    f"Deep underwater: stock ${_sp_spot_uw:.2f} is {_uw_deficit_pct:.0%} below "
                    f"strike ${_sp_strike_uw:.2f}.{_uw_basis_note} vs spot ${_sp_spot_uw:.2f}. "
                    f"Option is pure intrinsic (θ=${_sp_theta_uw:.2f}/day, no time value to collect). "
                    f"Recovery not viable: {_csp_block}. "
                    f"Assignment creates shares at {_uw_deficit_pct:.0%} premium to market — "
                    f"capital trap. Close for defined loss. "
                    f"(Jabbour Ch.8: assignment is a liability when basis >> market price; "
                    f"Natenberg Ch.15: no EV in holding pure-intrinsic short option.)"
                ),
                doctrine_source="Jabbour Ch.8 + Natenberg Ch.15: Deep Underwater Exit",
                priority=2, is_hard_veto=True, exit_trigger_type="CAPITAL",
            )

    # ── Assignment Risk Auto-Close Terminal Case ───────────────────────────
    # Same recovery logic: if thesis INTACT + IV viable, let assignment
    # happen and start wheel.  Otherwise hard veto EXIT.
    _sp_delta_abs = abs(safe_row_float(row, 'Delta'))
    if _sp_dte <= 7 and _sp_delta_abs > 0.90 and not _wheel_ready:
        _sp_spot_atc = safe_row_float(row, 'UL Last')
        _sp_strike_atc = safe_row_float(row, 'Strike')
        _sp_intrinsic = max(0.0, _sp_strike_atc - _sp_spot_atc) if _sp_strike_atc > 0 and _sp_spot_atc > 0 else 0.0

        _atc_thesis = str(row.get('Thesis_State', '') or '').upper()
        _atc_iv = _iv_now_w_norm
        _atc_recovery_ok = _atc_thesis != 'BROKEN' and _atc_iv > 0.15 and _wheel_capital_ok

        if _atc_recovery_ok:
            import math
            _atc_basis = _effective_basis_w if _effective_basis_w > 0 else _sp_strike_atc
            _atc_gap = _atc_basis - _sp_spot_atc
            _atc_monthly_cc = _sp_spot_atc * _atc_iv * math.sqrt(1.0 / 12.0) * 0.4
            _atc_months = _atc_gap / _atc_monthly_cc if _atc_monthly_cc > 0 else float('inf')
            propose_gate(
                collector, "assignment_terminal_recovery_wheel",
                action="HOLD", urgency="MEDIUM",
                rationale=(
                    f"Assignment terminal (DTE={_sp_dte:.0f}, Δ={_sp_delta_abs:.2f}): "
                    f"deep ITM (intrinsic ${_sp_intrinsic:.2f}/sh). "
                    f"Rolling is uneconomical — accept assignment at basis "
                    f"${_atc_basis:.2f} and start wheel. "
                    f"IV {_atc_iv:.0%} supports CC premium (~${_atc_monthly_cc:.2f}/sh/mo). "
                    + (f"Est. {_atc_months:.0f} months to breakeven. " if _atc_months < 999 else "")
                    + f"Thesis {_atc_thesis}. "
                    f"(Passarelli Ch.1: wheel conversion; Jabbour Ch.4: repair strategy.)"
                ),
                doctrine_source="Passarelli Ch.1 + Jabbour Ch.4: Terminal Assignment — Wheel Recovery",
                priority=3,
            )
        else:
            propose_gate(
                collector, "assignment_terminal",
                action="EXIT", urgency="HIGH",
                rationale=(
                    f"Assignment terminal: DTE={_sp_dte:.0f} ≤ 7, Delta={_sp_delta_abs:.2f} > 0.90. "
                    f"Short put is deep ITM (intrinsic ≈${_sp_intrinsic:.2f}/share). "
                    f"Roll credit is structurally insufficient to offset the intrinsic loss. "
                    f"Stop rolling — either CLOSE for defined loss or accept assignment at effective basis. "
                    f"(Jabbour Ch.8: terminal assignment case; Natenberg Ch.15: no EV in deep-ITM roll.)"
                ),
                doctrine_source="Jabbour Ch.8 + Natenberg Ch.15: Assignment Terminal Case",
                priority=3, is_hard_veto=True, exit_trigger_type="CAPITAL",
            )

    # ── DTE<7 hard gate ──────────────────────────────────────────────────
    if _sp_dte < 7 and is_itm:
        if _wheel_ready:
            propose_gate(
                collector, "dte7_wheel_accept",
                action="HOLD", urgency="LOW",
                rationale=(
                    f"DTE={_sp_dte:.0f} < 7, ITM — assignment imminent. "
                    f"Wheel Ready: {_wheel_note} "
                    f"Prepare to accept stock and sell covered call at/above basis. "
                    f"Do NOT roll — rolling here locks in a loss and defers the wheel entry "
                    f"(Passarelli Ch.1: effective basis = strike - premium = planned entry price)."
                ),
                doctrine_source="Passarelli Ch.1: Wheel Assignment — Intentional Acquisition",
                priority=15,
            )
        elif not assignment_acceptable:
            propose_gate(
                collector, "dte7_itm_defense",
                action="ROLL", urgency="HIGH",
                rationale=(
                    f"DTE={_sp_dte:.0f} < 7 with ITM put and assignment unacceptable — "
                    f"gamma and pin risk accelerating. Roll down/out now to defend assignment. "
                    f"{_wheel_note} "
                    f"(McMillan Ch.7: expiration management, short DTE ITM = urgent)."
                ),
                doctrine_source="McMillan Ch.7: Short DTE ITM Defense",
                priority=15,
            )

    # ── Late lifecycle ITM ───────────────────────────────────────────────
    if is_late and is_itm:
        if _wheel_ready:
            propose_gate(
                collector, "late_itm_wheel_accept",
                action="HOLD", urgency="LOW",
                rationale=(
                    f"Late lifecycle + ITM — assignment approaching. "
                    f"Wheel Ready: {_wheel_note} "
                    f"Accept stock at effective basis ${_effective_basis_w:.2f}. "
                    f"After assignment, sell covered call to continue income cycle."
                ),
                doctrine_source="Passarelli Ch.1 + McMillan: Wheel — Accept Assignment",
                priority=20,
            )
        elif not assignment_acceptable:
            propose_gate(
                collector, "late_itm_defense",
                action="ROLL", urgency="HIGH",
                rationale=(
                    f"Assignment undesirable (weak ticker health) + Expiration proximity. "
                    f"{_wheel_note} "
                    f"Roll to defend."
                ),
                doctrine_source="McMillan: Expiration Management",
                priority=20,
            )
        else:
            propose_gate(
                collector, "late_itm_strategic_hold",
                action="HOLD", urgency="LOW",
                rationale=(
                    f"Assignment acceptable (deferred entry path valid). Holding for strategic entry. "
                    f"{_wheel_note}"
                ),
                doctrine_source="RAG: Strategic Assignment",
                priority=20,
            )

    # ── Recently-Rolled Cooldown gate (Signal Coherence Gate 1) ──────────
    _COOLDOWN_DAYS_SP = 3
    _days_since_roll_sp = row.get('Days_Since_Last_Roll')
    _thesis_for_cooldown_sp = str(row.get('Thesis_State', '') or '').upper()
    if (
        pd.notna(_days_since_roll_sp)
        and float(_days_since_roll_sp) < _COOLDOWN_DAYS_SP
        and _thesis_for_cooldown_sp in ('INTACT', 'UNKNOWN', '')
    ):
        propose_gate(
            collector, "roll_cooldown",
            action="HOLD", urgency="LOW",
            rationale=(
                f"Recently-rolled cooldown: current leg opened {int(_days_since_roll_sp)}d ago "
                f"(< {_COOLDOWN_DAYS_SP}d window). Thesis is {_thesis_for_cooldown_sp or 'UNKNOWN'} — "
                f"suppressing discretionary ROLL to prevent self-contradicting roll chains. "
                f"Natenberg Ch.7: 'Frequent adjustments cost more than the risk they mitigate.' "
                f"Jabbour Ch.8: 'Repair is a dangerous misnomer for overtrading.'"
            ),
            doctrine_source="Natenberg Ch.7 + Jabbour Ch.8: Recently-Rolled Cooldown",
            priority=25,
        )

    # ── 21-DTE income gate ───────────────────────────────────────────────
    _sp_50_pct_gate_dte = safe_row_float(row, 'DTE', default=999.0)
    _sp_premium_entry_21 = abs(safe_row_float(row, 'Premium_Entry', 'Short_Call_Premium'))
    _sp_last_21 = abs(safe_row_float(row, 'Last'))
    _sp_profit_captured = (
        (_sp_premium_entry_21 - _sp_last_21) / _sp_premium_entry_21
        if _sp_premium_entry_21 > 0 else 0.0
    )
    if (
        _sp_50_pct_gate_dte <= 21
        and _sp_50_pct_gate_dte >= 7
        and _sp_profit_captured < 0.50
        and not (_wheel_ready and not is_itm)
    ):
        # ── Roll-vs-Assignment Cost Gate ─────────────────────────────────
        _sp_intrinsic_rvs = max(0.0, _strike_for_wheel - _spot_for_wheel) if (_strike_for_wheel > 0 and _spot_for_wheel > 0) else 0.0
        _sp_roll_cost_proxy = max(_sp_last_21, _sp_intrinsic_rvs)
        if _sp_roll_cost_proxy > 0 and _spot_for_wheel > 0 and _sp_roll_cost_proxy > 0.50 * _spot_for_wheel:
            _rvs_roll_cost_total = _sp_roll_cost_proxy * _qty_w * 100.0
            _rvs_assign_cost = _strike_for_wheel * _qty_w * 100.0
            _rvs_cc_target = _effective_basis_w if _effective_basis_w > 0 else _strike_for_wheel
            if _wheel_ready:
                propose_gate(
                    collector, "roll_vs_assignment_cost_wheel",
                    action="HOLD", urgency="LOW",
                    rationale=(
                        f"Roll-vs-assignment cost gate: put value ${_sp_roll_cost_proxy:.2f} > "
                        f"50% of stock ${_spot_for_wheel:.2f} "
                        f"(intrinsic ${_sp_intrinsic_rvs:.2f} = "
                        f"{_sp_intrinsic_rvs/_spot_for_wheel:.0%} of stock price). "
                        f"Rolling costs ~${_rvs_roll_cost_total:,.0f} to defer — "
                        f"assignment costs ${_rvs_assign_cost:,.0f} to own the stock. "
                        f"✅ Wheel Ready: LET ASSIGN → sell CC at/above ${_rvs_cc_target:.2f} "
                        f"(effective basis). Assignment is the intended outcome — "
                        f"rolling here pays to avoid what you planned for. "
                        f"(Passarelli Ch.1: 'effective purchase price = strike - premium'; "
                        f"Jabbour Ch.8: roll EV negative when intrinsic > net credit.)"
                    ),
                    doctrine_source="Passarelli Ch.1 + Jabbour Ch.8: Accept Assignment — Wheel Conversion",
                    priority=28,
                )
            else:
                # CSP recovery check: if thesis intact + IV viable, propose
                # assignment → wheel as a recovery path instead of just EXIT.
                _rvs_thesis = str(row.get('Thesis_State', '') or '').upper()
                _rvs_iv_recov = _iv_now_w_norm
                _rvs_recovery_ok = (
                    _rvs_thesis != 'BROKEN'
                    and _rvs_iv_recov > 0.15
                    and _wheel_capital_ok
                )
                if _rvs_recovery_ok:
                    import math
                    _rvs_gap = _rvs_cc_target - _spot_for_wheel
                    _rvs_monthly_cc = _spot_for_wheel * _rvs_iv_recov * math.sqrt(1.0 / 12.0) * 0.4
                    _rvs_months = _rvs_gap / _rvs_monthly_cc if _rvs_monthly_cc > 0 else float('inf')
                    propose_gate(
                        collector, "roll_vs_assignment_cost_recovery_wheel",
                        action="HOLD", urgency="MEDIUM",
                        rationale=(
                            f"Roll-vs-assignment cost gate: put value ${_sp_roll_cost_proxy:.2f} > "
                            f"50% of stock ${_spot_for_wheel:.2f}. "
                            f"Rolling costs ~${_rvs_roll_cost_total:,.0f} — uneconomical. "
                            f"Accept assignment at basis ${_rvs_cc_target:.2f} → wheel conversion. "
                            f"IV {_rvs_iv_recov:.0%} supports CC premium (~${_rvs_monthly_cc:.2f}/sh/mo). "
                            + (f"Est. {_rvs_months:.0f} months to breakeven. " if _rvs_months < 999 else "")
                            + f"Thesis {_rvs_thesis}. "
                            f"(Passarelli Ch.1: wheel conversion; Jabbour Ch.4: repair strategy.)"
                        ),
                        doctrine_source="Passarelli Ch.1 + Jabbour Ch.4: Roll Cost Gate — Wheel Recovery",
                        priority=28,
                    )
                else:
                    propose_gate(
                        collector, "roll_vs_assignment_cost_exit",
                        action="EXIT", urgency="HIGH",
                        exit_trigger_type="CAPITAL",
                        rationale=(
                            f"Roll-vs-assignment cost gate: put value ${_sp_roll_cost_proxy:.2f} > "
                            f"50% of stock ${_spot_for_wheel:.2f} "
                            f"(intrinsic ${_sp_intrinsic_rvs:.2f} = "
                            f"{_sp_intrinsic_rvs/_spot_for_wheel:.0%} of stock price). "
                            f"Rolling costs ~${_rvs_roll_cost_total:,.0f} to defer — "
                            f"no achievable roll credit offsets the buyback. "
                            f"{_wheel_note} "
                            f"Recovery not viable: "
                            + (f"thesis {_rvs_thesis}" if _rvs_thesis == 'BROKEN'
                               else f"IV {_rvs_iv_recov:.0%} too low"
                               if not (_rvs_iv_recov > 0.15) else "overconcentrated")
                            + ". Close for defined loss. "
                            f"(Jabbour Ch.8: roll EV turns negative when intrinsic exceeds net credit; "
                            f"Natenberg Ch.15: deep ITM roll = paying to defer, not to profit.)"
                        ),
                        doctrine_source="Jabbour Ch.8 + Natenberg Ch.15: Roll-vs-Assignment Cost Gate",
                        priority=28,
                    )
        else:
            # Standard 21-DTE income roll logic
            _sp_21_urgency = 'MEDIUM' if _sp_profit_captured >= 0 else 'HIGH'
            _sp_21_pnl = (
                f"up {_sp_profit_captured:.0%}" if _sp_profit_captured > 0
                else f"down {abs(_sp_profit_captured):.0%}" if _sp_profit_captured < 0
                else "flat"
            )

            # Strategy-aware IV regime check (Chan 0.786)
            _sp_iv_entry_21 = safe_row_float(row, 'IV_Entry')
            _sp_iv_now_21   = safe_row_float(row, 'IV_30D', 'IV_Now')
            _sp_iv_pctile_21 = safe_row_float(row, 'IV_Percentile', default=50.0)
            _sp_iv_gap_21   = safe_row_float(row, 'IV_vs_HV_Gap')

            _sp_iv_collapsed_21 = (
                _sp_iv_entry_21 > 0 and _sp_iv_now_21 > 0
                and (_sp_iv_now_21 / _sp_iv_entry_21) < 0.70
                and _sp_iv_pctile_21 < 25
                and _sp_iv_gap_21 <= 0
            )

            if _sp_iv_collapsed_21:
                propose_gate(
                    collector, "income_gate_21dte_iv_collapsed",
                    action="HOLD", urgency="LOW",
                    rationale=(
                        f"21-DTE income gate: DTE={_sp_50_pct_gate_dte:.0f}, "
                        f"{_sp_profit_captured:.0%} captured. "
                        f"Vol regime shift: IV contracted "
                        f"{(1 - _sp_iv_now_21/_sp_iv_entry_21):.0%} from entry "
                        f"({_sp_iv_entry_21:.1%} -> {_sp_iv_now_21:.1%}), "
                        f"IV_Percentile={_sp_iv_pctile_21:.0f}, "
                        f"IV-HV gap={_sp_iv_gap_21:+.1%}. "
                        f"Rolling into a low-IV environment yields thin premium. "
                        f"Let current put decay (remaining TV = ${_sp_last_21:.2f}). "
                        f"Chan: 'Mean-reversion exit when regime shifts.' "
                        f"Natenberg Ch.8: 'Selling premium below HV = negative EV.'"
                    ),
                    doctrine_source="Chan: Strategy-aware exit — Vol regime shift (CSP)",
                    priority=30,
                )
            else:
                # Position Trajectory context
                _sp_regime_21 = str(row.get('Position_Regime', 'NEUTRAL') or 'NEUTRAL')
                _sp_consec_debits_21 = int(safe_row_float(row, 'Trajectory_Consecutive_Debit_Rolls'))
                _sp_stock_ret_21 = safe_row_float(row, 'Trajectory_Stock_Return')
                if _sp_regime_21 == 'TRENDING_CHASE':
                    propose_gate(
                        collector, "income_gate_21dte_trending_chase",
                        action="ROLL", urgency="HIGH",
                        rationale=(
                            f"21-DTE income gate: DTE={_sp_50_pct_gate_dte:.0f} ≤ 21 with {_sp_profit_captured:.0%} captured. "
                            f"⚠️ TRENDING_CHASE: stock has moved {_sp_stock_ret_21:+.0%} since entry "
                            f"with {_sp_consec_debits_21} consecutive debit roll(s). "
                            f"Stock is structurally declining through put strikes — this is NOT a temporary dip. "
                            f"Consider: (A) accept assignment if wheel-ready at this strike, "
                            f"(B) buy back put and wait for stabilization, or "
                            f"(C) roll to a much lower strike if premium justifies risk. "
                            f"Rolling to the next monthly repeats the chase cycle. "
                            f"(McMillan Ch.3: strike-chase recognition; Given Ch.6: 21-DTE gate)"
                        ),
                        doctrine_source="McMillan Ch.3 + Position Trajectory: Strike Chase at 21-DTE",
                        priority=30,
                    )
                else:
                    _sp_regime_note_21 = ""
                    if _sp_regime_21 in ('SIDEWAYS_INCOME', 'MEAN_REVERSION'):
                        _sp_regime_note_21 = (
                            f" {_sp_regime_21}: roll OUT at same strike for credit — "
                            "do NOT roll DOWN to a lower strike (debit). "
                            "Stock is range-bound; income cycle is working. "
                            "If assigned at current strike, that's the wheel entry "
                            "(McMillan Ch.3: same-strike credit rolls in range-bound regimes)."
                        )
                    propose_gate(
                        collector, "income_gate_21dte_standard",
                        action="ROLL", urgency=_sp_21_urgency,
                        rationale=(
                            f"21-DTE income gate: DTE={_sp_50_pct_gate_dte:.0f} ≤ 21 with only "
                            f"{_sp_profit_captured:.0%} profit captured (need ≥50%). "
                            f"Position {_sp_21_pnl} — gamma-theta ratio has degraded; "
                            f"short put edge is structurally exhausted. "
                            f"Buy back current put and roll out 30-45 DTE to reset theta efficiency. "
                            f"(Given Ch.6: 21-DTE income roll; Passarelli Ch.2: gamma-theta degradation.)"
                            f"{_sp_regime_note_21}"
                        ),
                        doctrine_source="Given Ch.6 + Passarelli Ch.2: 21-DTE Income Roll Gate",
                        priority=30,
                    )

    # ── Expectancy Preservation Logic (Reversion-Aware) ──────────────────
    if row.get('Drift_Direction') == 'Up':
        struct_state = str(row.get('PriceStructure_State', 'STABLE')).upper()
        mom_state = str(row.get('MomentumVelocity_State', 'UNKNOWN')).upper()
        vol_state = str(row.get('VolatilityState_State', 'NORMAL')).upper()

        reversion_prob_collapse = ("STRUCTURE_BROKEN" in struct_state or "REVERSING" in mom_state)
        low_continuation_value = row.get('Drift_Magnitude') == 'High'
        vol_expansion = vol_state in ["EXPANDING", "EXTREME"]

        if reversion_prob_collapse and low_continuation_value and vol_expansion:
            propose_gate(
                collector, "expectancy_preservation_trim",
                action="TRIM", urgency="MEDIUM",
                rationale="Triple-gate met: Reversion edge collapsed + Low continuation value + Vol expansion.",
                doctrine_source="RAG: Expectancy Preservation",
                priority=40,
            )
        elif low_continuation_value and not reversion_prob_collapse:
            propose_gate(
                collector, "expectancy_preservation_hold",
                action="HOLD_FOR_REVERSION", urgency="LOW",
                rationale="Extended success with intact structure; holding for premium regeneration.",
                doctrine_source="RAG: Expectancy Preservation",
                priority=40,
            )

    # ── Vol regime flip ──────────────────────────────────────────────────
    _vol_state_sp = str(row.get('VolatilityState_State', '') or '').split('.')[-1].upper()
    _iv_entry_sp  = safe_row_float(row, 'IV_Entry')
    _iv_now_sp    = safe_row_float(row, 'IV_Now', 'IV_30D')
    _iv_entry_low = _iv_entry_sp < 0.25 if _iv_entry_sp > 0 else False
    _iv_expanded  = _iv_now_sp > _iv_entry_sp * 1.5 if (_iv_entry_sp > 0 and _iv_now_sp > 0) else False

    if _vol_state_sp == 'EXTREME' and _iv_expanded and not is_itm:
        propose_gate(
            collector, "vol_regime_flip",
            action="ROLL", urgency="HIGH",
            rationale=(
                f"Vol regime EXTREME (IV now {_iv_now_sp:.1%} vs entry {_iv_entry_sp:.1%}, "
                f"+{(_iv_now_sp - _iv_entry_sp):.1%}). "
                f"Short put sold into low-vol environment is now exposed to a regime where "
                f"IV expansion dwarfs the premium collected. "
                f"Roll down-and-out or buy protective put to reduce delta exposure "
                f"(Passarelli Ch.6: vol regime flip = edge reversal)."
            ),
            doctrine_source="Passarelli Ch.6: Vol Regime Flip — Edge Reversal",
            priority=45,
        )

    # ── Thesis degradation ───────────────────────────────────────────────
    thesis = check_thesis_degradation(row)
    if thesis:
        propose_gate(
            collector, "thesis_degradation",
            action="HOLD", urgency="MEDIUM",
            rationale=(
                f"Entry regime degraded: {thesis['text']}. "
                f"Monitoring position — original setup no longer intact "
                f"(McMillan Ch.2: Thesis Persistence)."
            ),
            doctrine_source="McMillan Ch.2: Thesis Persistence",
            priority=50,
        )

    # ── EV comparator (always evaluates) ─────────────────────────────────
    ev_result = None
    try:
        qty_ev = abs(safe_row_float(row, 'Quantity', default=1.0))
        dte_ev = max(float(_sp_dte), 1)
        # Use same cost hierarchy as BW: Net_Cost_Basis_Per_Share first
        net_cost_ev = safe_row_float(row, 'Net_Cost_Basis_Per_Share')
        broker_basis = abs(safe_row_float(row, 'Basis'))
        qty_abs = abs(safe_row_float(row, 'Quantity', default=1.0))
        broker_cost_per_share = (broker_basis / qty_abs) if qty_abs > 0 and broker_basis > 0 else 0.0
        if net_cost_ev > 0:
            effective_cost_ev = net_cost_ev
        elif broker_cost_per_share > 0:
            effective_cost_ev = broker_cost_per_share
        else:
            effective_cost_ev = safe_row_float(row, 'Underlying_Price_Entry') or _spot_for_wheel

        ev_result = compare_actions_bw(
            row, spot=_spot_for_wheel, strike=_strike_for_wheel,
            effective_cost=effective_cost_ev, qty=qty_ev, dte=dte_ev,
        )
        # Store EV fields in result for downstream display
        result["Action_EV_Ranking"] = ev_result["ranked_actions"]
        result["Action_EV_Winner"] = ev_result["ev_winner"]
        result["Action_EV_Margin"] = ev_result["ev_margin"]
        result["Action_EV_Hold"] = ev_result["ev_hold"]
        result["Action_EV_Roll"] = ev_result["ev_roll"]
        result["Action_EV_Assign"] = ev_result["ev_assign"]
        result["Action_EV_Buyback"] = ev_result["ev_buyback"]
        result["EV_Buyback_Trigger"] = ev_result["ev_buyback_trigger"]
        result["Gamma_Drag_Daily"] = ev_result["gamma_drag_daily"]

        # ── Timing quality adjustments ────────────────────────────────────
        _timing_roll_adj = 1.0
        _timing_hold_bonus = 0.0
        _timing_notes = []

        _tq_iv_now = safe_row_float(row, 'IV_Now', 'IV_30D')
        _tq_hv = safe_row_float(row, 'HV_20D')
        if _tq_hv >= 5.0:
            _tq_hv /= 100.0
        if _tq_iv_now >= 5.0:
            _tq_iv_now /= 100.0
        _tq_iv_hv_ratio = _tq_iv_now / _tq_hv if _tq_hv > 0.01 else 1.0
        if _tq_iv_hv_ratio < 0.95:
            _iv_discount = max(0.50, _tq_iv_hv_ratio)
            _timing_roll_adj *= _iv_discount
            _timing_notes.append(
                f"IV depressed ({_tq_iv_now:.0%}/{_tq_hv:.0%}={_tq_iv_hv_ratio:.2f}): "
                f"ROLL EV ×{_iv_discount:.2f}"
            )

        _tq_days_macro = safe_row_float(row, 'Days_To_Macro', default=999.0)
        _tq_macro_type = str(row.get('Macro_Next_Type', '') or '').upper()
        _tq_macro_event = str(row.get('Macro_Next_Event', '') or '')
        _tq_macro_high = _tq_macro_type in ('FOMC', 'CPI', 'NFP')
        if _tq_macro_high and _tq_days_macro <= 3:
            _timing_roll_adj *= 0.70
            _timing_hold_bonus = abs(ev_result.get('ev_roll', 0)) * 0.20
            _timing_notes.append(
                f"Macro {_tq_macro_event} in {_tq_days_macro:.0f}d: "
                f"ROLL EV ×0.70 + HOLD bonus ${_timing_hold_bonus:,.0f}"
            )
        elif _tq_macro_high and _tq_days_macro <= 5:
            _timing_roll_adj *= 0.85
            _timing_notes.append(f"Macro {_tq_macro_event} in {_tq_days_macro:.0f}d: ROLL EV ×0.85")

        _tq_squeeze = bool(row.get('Keltner_Squeeze_On', False))
        if _tq_squeeze:
            _timing_roll_adj *= 0.85
            _timing_notes.append("Keltner squeeze ON: ROLL EV ×0.85")

        # (d) Debit roll history: consecutive debit rolls erode income edge.
        _tq_consec_debits = int(safe_row_float(row, 'Trajectory_Consecutive_Debit_Rolls'))
        _tq_emergency = delta > 0.55 or dte < 14
        if _tq_consec_debits >= 2 and not _tq_emergency:
            _debit_discount = 0.80 if _tq_consec_debits >= 3 else 0.90
            _timing_roll_adj *= _debit_discount
            _timing_notes.append(
                f"{_tq_consec_debits} consecutive debit roll(s): ROLL EV "
                f"discounted ×{_debit_discount:.2f} — income edge eroding "
                f"(McMillan Ch.3: debit rolls signal structural headwind)"
            )

        if _timing_roll_adj < 1.0:
            ev_result['ev_roll'] = ev_result.get('ev_roll', 0) * _timing_roll_adj
            ev_result['ev_buyback'] = ev_result.get('ev_buyback', 0) * max(_timing_roll_adj, 0.90)
            if _timing_hold_bonus > 0:
                ev_result['ev_hold'] = ev_result.get('ev_hold', 0) + _timing_hold_bonus
            _adj_evs = {
                'HOLD': ev_result.get('ev_hold', 0),
                'ROLL': ev_result.get('ev_roll', 0),
                'ASSIGN': ev_result.get('ev_assign', 0),
                'BUYBACK': ev_result.get('ev_buyback', 0),
            }
            ev_result['ev_winner'] = max(_adj_evs, key=_adj_evs.get)
            ev_result['ev_summary'] = (
                f"Action EV (over {dte_ev:.0f}d): "
                + " | ".join(f"{a} {'+' if v >= 0 else ''}{v:,.0f}"
                             for a, v in [('HOLD', ev_result['ev_hold']),
                                          ('ROLL', ev_result['ev_roll']),
                                          ('ASSIGN', ev_result['ev_assign']),
                                          ('BUYBACK', ev_result['ev_buyback'])])
                + f" → **{ev_result['ev_winner']}** wins"
                + (f" [Timing: {'; '.join(_timing_notes)}]" if _timing_notes else "")
            )
            logger.debug(f"[CSP_v2] Timing quality: roll_adj={_timing_roll_adj:.2f}, new_winner={ev_result['ev_winner']}")

        # Add EV-backed proposals for each action
        _ev_winner_action = ev_result.get("ev_winner", "")

        # ASSIGN guard: don't propose ASSIGN when assignment is improbable
        _p_assign_ev = abs(safe_row_float(row, 'MC_Assign_P_Expiry', 'Delta'))
        if _ev_winner_action == "ASSIGN" and _p_assign_ev < 0.20:
            _ev_winner_action = "HOLD"
            ev_result["ev_winner"] = "HOLD"

        # Map internal ASSIGN to user-facing label based on moneyness.
        # CSP: ACCEPT_SHARE_ASSIGNMENT = shares put to you at strike (ITM, |delta| >= 0.50).
        #      LET_EXPIRE = put expires worthless, keep premium (OTM, |delta| < 0.50).
        if _ev_winner_action == "ASSIGN":
            if _sp_delta_abs >= 0.50:
                _ev_winner_action = "ACCEPT_SHARE_ASSIGNMENT"
            else:
                _ev_winner_action = "LET_EXPIRE"
            ev_result["ev_winner"] = _ev_winner_action
            # Update summary text so user sees the mapped label, not internal "ASSIGN"
            ev_result["ev_summary"] = ev_result.get("ev_summary", "").replace("**ASSIGN**", f"**{_ev_winner_action}**").replace("ASSIGN ", f"{_ev_winner_action} ")

        # ROLL guard: don't propose ROLL when thesis blocks it
        if _ev_winner_action == "ROLL" and result.get('_thesis_blocks_roll', False):
            _ev_winner_action = "HOLD"
            ev_result["ev_winner"] = "HOLD"

        _ASSIGN_LABELS = ("LET_EXPIRE", "ACCEPT_SHARE_ASSIGNMENT")
        for action_name, ev_key in [("HOLD", "ev_hold"), ("ROLL", "ev_roll"),
                                     ("ASSIGN", "ev_assign"), ("BUYBACK", "ev_buyback")]:
            ev_val = ev_result.get(ev_key, 0.0)
            if action_name == "ASSIGN" and _ev_winner_action in _ASSIGN_LABELS:
                propose_gate(
                    collector, f"ev_comparator_{_ev_winner_action.lower()}",
                    action=_ev_winner_action, urgency="MEDIUM",
                    rationale=(
                        f"EV decision: {_ev_winner_action} wins with ${ev_val:,.0f} over "
                        f"{dte_ev:.0f}d. {ev_result['ev_summary']}"
                    ),
                    doctrine_source=f"ActionEV: {_ev_winner_action} winner (CSP)",
                    priority=70, ev_estimate=ev_val,
                )
            elif action_name == _ev_winner_action:
                propose_gate(
                    collector, f"ev_comparator_{action_name.lower()}",
                    action=action_name, urgency="MEDIUM",
                    rationale=(
                        f"EV decision: {action_name} wins with ${ev_val:,.0f} over "
                        f"{dte_ev:.0f}d. {ev_result['ev_summary']}"
                    ),
                    doctrine_source=f"ActionEV: {action_name} winner (CSP)",
                    priority=70, ev_estimate=ev_val,
                )
    except Exception as ev_err:
        logger.debug(f"CSP v2 EV comparator error (non-fatal): {ev_err}")

    # ── Carry inversion (pre-computed from MarginCarryCalculator) ─────────
    from core.management.cycle3.doctrine.shared_income_gates import gate_carry_inversion_roll
    _r_ci = result.copy()
    _ci_fired, _r_ci = gate_carry_inversion_roll(row=row, result=_r_ci)
    if _ci_fired:
        propose_gate(
            collector, "carry_inversion_roll",
            action=_r_ci.get("Action", "ROLL"),
            urgency=_r_ci.get("Urgency", "MEDIUM"),
            rationale=_r_ci.get("Rationale", ""),
            doctrine_source=_r_ci.get("Doctrine_Source", ""),
            priority=48,
        )

    # ── Default HOLD LOW (always present) ────────────────────────────────
    rationale = "Position is OTM or has sufficient time remaining. No action required."
    if is_itm:
        rationale = (
            "Position is ITM but has sufficient time remaining. Assignment acceptable."
            if assignment_acceptable
            else "Position is ITM; monitoring for defense."
        )

    # IV term structure note (Natenberg Ch.11)
    iv_shape_sp = str(row.get('iv_surface_shape', '') or '').upper()
    if iv_shape_sp == 'BACKWARDATION':
        slope_sp = safe_row_float(row, 'iv_ts_slope_30_90')
        rationale += (
            f" IV BACKWARDATED ({slope_sp:+.1f}pt 30-90d): short put collecting "
            f"above-normal near-term IV — premium favorable (Natenberg Ch.11)."
        )

    # Forward expectancy note
    _ev_ratio_sp = safe_row_float(row, 'EV_Feasibility_Ratio') if pd.notna(row.get('EV_Feasibility_Ratio')) else float('nan')
    _em_10_sp    = safe_row_float(row, 'Expected_Move_10D')
    _req_sp      = safe_row_float(row, 'Required_Move_Breakeven', 'Required_Move')
    if not pd.isna(_ev_ratio_sp) and _ev_ratio_sp > 0 and _em_10_sp > 0:
        _sp_context = (
            f" Expected 10D move: ${_em_10_sp:.1f}. "
            f"Required move to breakeven: ${_req_sp:.1f} "
            f"({_ev_ratio_sp:.2f}× expected). "
        )
        if _ev_ratio_sp > 1.5:
            _sp_context += (
                f"Stock needs to move {_ev_ratio_sp:.1f}× the 10D expected move to reach breakeven — "
                f"assignment is statistically likely within this DTE window."
            )
        else:
            _sp_context += f"Within expected 10D range — breakeven is statistically reachable."
        rationale += _sp_context

    propose_gate(
        collector, "default_hold",
        action="HOLD", urgency="LOW",
        rationale=rationale,
        doctrine_source="McMillan: Neutral Maintenance",
        priority=100,
    )

    # ── Resolution ───────────────────────────────────────────────────────
    logger.debug(f"[CSP_v2] {collector.summary()}")

    if collector.has_hard_veto():
        winner = collector.get_veto()
        return collector.to_result(winner, result, resolution_method="HARD_VETO")

    # Resolve via EV + MC
    if ev_result is not None:
        winner = resolve_income_proposals(collector, ev_result, row)
        resolved = collector.to_result(winner, result, resolution_method="EV_COMPARISON")
    else:
        # No EV data — fall back to highest urgency, lowest priority
        proposals_sorted = sorted(
            collector.proposals,
            key=lambda p: (-p.urgency_rank, p.priority),
        )
        winner = proposals_sorted[0]
        resolved = collector.to_result(winner, result, resolution_method="PRIORITY_FALLBACK")

    # Tag recovery ladder state so MC EXIT_NOW guard in run_all.py respects it
    _recovery_gates = (
        "deep_underwater_recovery_wheel",
        "assignment_terminal_recovery_wheel",
        "roll_vs_assignment_cost_recovery_wheel",
    )
    if resolved.get("Winning_Gate") in _recovery_gates:
        resolved["Doctrine_State"] = "RECOVERY_LADDER"
        resolved["Resolution_Method"] = "RECOVERY_LADDER"

    return resolved
