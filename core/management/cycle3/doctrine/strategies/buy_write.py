"""
BUY_WRITE strategy doctrine — extracted from engine.py.

Gate order (first match → early return):
  1.  Hard stop (-20% from net cost basis) with recovery ladder guard
  1a. Approaching hard stop (-15% to -20%)
  1b. Earnings event risk (binary event inside DTE)
  1b-GAP2. Earnings lockdown (upgrade ROLL→EXIT when ≤2 days)
  1b-LEAPS. LEAPS earnings routine note
  1c. Underlying health story check (BROKEN/DEGRADED)
  2.  ITM defense (delta > 0.70 with cost-basis context)
  3.  Expiration proximity (DTE < 7)
  Pre-ITM drift warning (0.55-0.70 delta with extrinsic analysis)
  Roll timing intelligence
  Thesis block interceptor
  Post-BUYBACK sticky gate
  Gamma danger zone
  Equity integrity gate (BROKEN with gamma dominance/conviction/no-conviction)
  WEAKENING annotation
  3a-LEAPS. Carry inversion (non-BROKEN)
  3b. Dividend assignment
  3b-GAP1. 21-DTE income gate with IV regime + position trajectory
  4.  50% premium capture with timing gate
  5.  Negative carry (yield below margin rate)
  5b. Debit roll efficiency / cadence switch
  6.  Persistence escalation
  7.  Thesis regime degradation
  8.  IV term structure note
  9.  EV comparator with 5 override paths + cadence note recovery
"""

import math
import json
import logging
from typing import Dict, Any

import pandas as pd

from core.management.cycle1.identity.constants import (
    FIDELITY_MARGIN_RATE,
    FIDELITY_MARGIN_RATE_DAILY,
)
from core.management.cycle3.doctrine.gate_result import (
    STATE_ACTIONABLE,
    STATE_NEUTRAL_CONFIDENT,
    STATE_UNCERTAIN,
    fire_gate,
)
from core.management.cycle3.doctrine.thresholds import (
    CARRY_INVERSION_SEVERE,
    DELTA_DIVIDEND_ASSIGNMENT,
    DELTA_FAR_OTM,
    DELTA_ITM_EMERGENCY,
    DELTA_PRE_ITM_WARNING,
    DTE_CADENCE_THRESHOLD,
    DTE_CUSHION_WINDOW,
    DTE_EMERGENCY_ROLL,
    DTE_INCOME_GATE,
    DTE_LEAPS_THRESHOLD,
    EARNINGS_NOTE_WINDOW,
    EV_NOISE_FLOOR_INCOME,
    EXTRINSIC_CREDIT_STRONG,
    EXTRINSIC_CREDIT_VIABLE,
    EXTRINSIC_THETA_EXHAUSTED,
    GAMMA_ATM_PROXIMITY,
    GAMMA_DANGER_RATIO,
    GAMMA_DOMINANCE_RATIO,
    GAMMA_MONEYNESS_GUARD,
    PNL_APPROACHING_HARD_STOP,
    PNL_DRIFT_STRUCTURE_BROKEN,
    PNL_HARD_STOP_BW,
    PNL_POST_EARNINGS_DROP,
    PREMIUM_CAPTURE_TARGET,
    STRIKE_PROXIMITY_EARNINGS,
    STRIKE_PROXIMITY_NARROW,
    YIELD_ESCALATION_THRESHOLD,
    BREAKOUT_THROUGH_STRIKE,
    STANDARD_ROLL_DTE,
)
from core.management.cycle3.doctrine.helpers import (
    classify_roll_timing,
    check_thesis_degradation,
    detect_recovery_state,
    detect_moderate_recovery_state,
    safe_row_float,
)
from core.management.cycle3.doctrine.comparators.action_ev_bw import compare_actions_bw, resolve_income_proposals
from core.management.cycle3.doctrine.proposal import ProposalCollector, propose_gate
from core.management.cycle3.doctrine.shared_income_gates import gate_consecutive_debit_roll_stop, gate_fading_winner

logger = logging.getLogger(__name__)


def buy_write_doctrine(row: pd.Series, result: Dict) -> Dict:
    """Full BUY_WRITE doctrine — first-match gate cascade."""

    _signal_hub_bw_notes = ""  # Collected at gate 7b, appended at end

    spot = safe_row_float(row, 'UL Last')
    delta = abs(safe_row_float(row, 'Short_Call_Delta', 'Delta'))
    # NaN-safe DTE: NaN is truthy → `NaN or 999` returns NaN, not 999.
    _dte_raw = row.get('Short_Call_DTE')
    if not pd.notna(_dte_raw):
        _dte_raw = row.get('DTE')
    if not pd.notna(_dte_raw):
        _dte_raw = 999
    dte = float(_dte_raw)

    # ── Net cost basis ──────────────────────────────────────────────────
    from core.shared.finance_utils import effective_cost_per_share as _ecp
    cum_premium = safe_row_float(row, 'Cumulative_Premium_Collected')
    strike = safe_row_float(row, 'Short_Call_Strike', 'Strike')

    effective_cost, broker_cost_per_share, _cost_tier = _ecp(row, spot_fallback=spot)

    # Drift from net cost
    drift_from_net = (spot - effective_cost) / effective_cost if effective_cost > 0 else 0.0
    stock_basis_raw = broker_cost_per_share
    total_loss_dollars = (spot - stock_basis_raw) * abs(safe_row_float(row, 'Quantity')) if stock_basis_raw > 0 else 0.0

    # ── 0. New-position grace period ───────────────────────────────────
    # Scan engine just recommended this BUY_WRITE — don't ROLL/EXIT a call
    # you sold today.  Give the position at least 1 full trading day to
    # develop. Exception: catastrophic gap move (>-25% on day 0).
    _bw_days_in_trade = safe_row_float(row, 'Days_In_Trade')
    if _bw_days_in_trade < 2 and drift_from_net > -0.25:
        return fire_gate(
            result,
            action="HOLD", urgency="LOW",
            rationale=(
                f"New position grace: BUY_WRITE opened {_bw_days_in_trade:.0f}d ago. "
                f"Scan engine approved this trade — allow at least 1 full "
                f"trading day before considering ROLL or EXIT. "
                f"Current drift {drift_from_net:+.1%} from net cost "
                f"${effective_cost:.2f} (McMillan Ch.4: give new positions "
                f"time to develop)."
            ),
            doctrine_source="McMillan Ch.4: New Position Grace Period",
        )

    # ── 1. Hard Stop ────────────────────────────────────────────────────
    if effective_cost > 0 and drift_from_net <= PNL_HARD_STOP_BW:
        # Recovery Ladder Guard
        _rl_cycle_ct = int(row.get('_cycle_count', 1) or 1)
        _rl_thesis = str(row.get('Thesis_State', '') or '').upper()

        if _rl_cycle_ct >= 2 and cum_premium > 0 and _rl_thesis not in ('BROKEN',):
            _rl_last_prem = safe_row_float(row, 'Premium_Entry')
            _rl_monthly = (_rl_last_prem / max(dte, 1)) * 30 if _rl_last_prem > 0 else 0
            _rl_gap = stock_basis_raw - spot if stock_basis_raw > 0 else effective_cost - spot
            _rl_months_to_close = (_rl_gap / _rl_monthly) if _rl_monthly > 0 else float('inf')
            _rl_months_str = (
                f" At ~${_rl_monthly:.2f}/mo income rate, ~{_rl_months_to_close:.0f} months"
                f" to close the gap via premium alone."
                if _rl_monthly > 0 and _rl_months_to_close < 999
                else ""
            )

            result.update({
                "Action": "HOLD",
                "Urgency": "MEDIUM",
                "Rationale": (
                    f"Recovery ladder active: hard stop breached (${spot:.2f} vs "
                    f"net cost ${effective_cost:.2f}, {drift_from_net:.1%}) but "
                    f"{_rl_cycle_ct} cycles of premium collection have reduced basis "
                    f"from ${stock_basis_raw:.2f} → ${effective_cost:.2f} "
                    f"(${cum_premium:.2f}/sh collected).{_rl_months_str} "
                    f"Hold short call to expiration. At expiry, reassess stock "
                    f"thesis before opening next cycle. "
                    f"⚠️ Stock leg remains at risk — evaluate separately whether "
                    f"to continue holding equity "
                    f"(Jabbour Ch.4: Repair Strategies; McMillan Ch.3: Basis Reduction)."
                ),
                "Doctrine_Source": "Jabbour Ch.4 / McMillan Ch.3: Recovery Ladder",
                "Doctrine_State": "RECOVERY_LADDER",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result

        # ── Forward-Economics Guard (1st-cycle positions) ──────────────
        # Sunk loss alone should not drive exit. If IV supports future
        # income cycles and recovery is feasible within a reasonable
        # horizon, downgrade EXIT to HOLD with elevated monitoring.
        # (McMillan Ch.3: forward EV supersedes sunk-loss exit)
        _rl_thesis_fe = str(row.get('Thesis_State', '') or '').upper()
        if _rl_thesis_fe not in ('BROKEN',):
            try:
                from core.management.cycle3.doctrine.helpers import compute_forward_income_economics
                from core.management.cycle3.doctrine.thresholds import FORWARD_ECON_MONTHS_BW_HARD_STOP
                _fe = compute_forward_income_economics(row, spot, effective_cost)
                if (_fe["viable"]
                        and _fe["months_to_breakeven"] < FORWARD_ECON_MONTHS_BW_HARD_STOP):
                    _fe_note = (
                        f" At ~${_fe['net_monthly']:.2f}/mo net income, "
                        f"~{_fe['months_to_breakeven']:.0f} months to close gap."
                        if _fe["net_monthly"] > 0 else ""
                    )
                    result.update({
                        "Action": "HOLD",
                        "Urgency": "HIGH",
                        "Rationale": (
                            f"Forward-economics override: hard stop breached "
                            f"({drift_from_net:.1%}) but forward income path viable."
                            f"{_fe_note} IV {_fe['iv_now']:.0%} "
                            f"(Rank {_fe['iv_rank']:.0f}) supports premium generation. "
                            f"Hold through current call expiry, then write next cycle "
                            f"to begin basis reduction. "
                            f"(McMillan Ch.3: forward EV supersedes sunk-loss exit; "
                            f"Jabbour Ch.4: repair when forward income covers gap "
                            f"within {FORWARD_ECON_MONTHS_BW_HARD_STOP}mo)"
                        ),
                        "Doctrine_Source": "McMillan Ch.3 + Jabbour Ch.4: Forward-Economics Override",
                        "Doctrine_State": "FORWARD_ECON_HOLD",
                        "Decision_State": STATE_ACTIONABLE,
                        "Required_Conditions_Met": True,
                    })
                    return result
            except Exception:
                pass  # Graceful fallback: continue to original hard stop

        # Original Hard Stop EXIT
        if cum_premium > 0:
            cushion_note = (
                f" ${cum_premium:.2f}/share collected across {int(row.get('_cycle_count', 1))} cycles"
                f" reduced basis from ${stock_basis_raw:.2f} to ${effective_cost:.2f}."
            )
        else:
            cushion_note = " No premium history recorded — run backfill if cycles are missing."

        total_loss_note = (
            f" Combined P&L ≈ ${total_loss_dollars:+,.0f}."
            if total_loss_dollars != 0 else ""
        )

        _cost_provenance = ""
        _hard_stop_urgency = "CRITICAL"
        if _cost_tier == 3 and cum_premium == 0:
            _cost_provenance = (
                f" ⚠️ UNVERIFIED COST BASIS: ${effective_cost:.2f} is the spot price "
                f"when the position was first observed — NOT confirmed purchase price. "
                f"Verify broker Basis before acting."
            )
            _hard_stop_urgency = "HIGH"

        result.update({
            "Action": "EXIT",
            "Urgency": _hard_stop_urgency,
            "Exit_Trigger_Type": "CAPITAL",
            "Rationale": (
                f"Hard stop breached: stock at ${spot:.2f} vs net cost ${effective_cost:.2f}/share "
                f"({drift_from_net:.1%}).{cushion_note}{total_loss_note}"
                f"{_cost_provenance} "
                f"Continue rolling only if thesis intact — otherwise exit stock + buy back call "
                f"(McMillan Ch.3: Hard Stop)."
            ),
            "Doctrine_Source": "McMillan Ch.3: Hard Stop",
            "Doctrine_State": "EXIT_REQUIRED",
            "Decision_State": STATE_ACTIONABLE if _cost_tier <= 2 else STATE_UNCERTAIN,
            "Required_Conditions_Met": _cost_tier <= 2
        })
        return result

    # ── 1a. Approaching hard stop ───────────────────────────────────────
    # Moderate recovery: catch -10% to -25% drawdowns early.
    _v1_moderate = detect_moderate_recovery_state(row, spot=spot, effective_cost=effective_cost)
    _v1_moderate_recovery = _v1_moderate["is_moderate_recovery"]

    if effective_cost > 0 and PNL_HARD_STOP_BW < drift_from_net <= PNL_APPROACHING_HARD_STOP:
        hard_stop_price = effective_cost * (1 + PNL_HARD_STOP_BW)
        gap_to_stop = spot - hard_stop_price
        premium_note = f" ${cum_premium:.2f}/share collected to date." if cum_premium > 0 else ""

        # Thesis-aware override
        _thesis_hs = str(row.get('Thesis_State', '') or '').upper()
        _gap_pct = gap_to_stop / spot if spot and spot > 0 else 1.0
        if _gap_pct < 0.03 and _thesis_hs in ('DEGRADED', 'BROKEN'):
            # Moderate recovery: DEGRADED thesis with income path → ROLL
            # instead of EXIT.  BROKEN thesis always exits.
            if _v1_moderate_recovery and _thesis_hs != 'BROKEN':
                _mr_v1 = _v1_moderate["context"]
                _mr_v1_months = _mr_v1.get("months_to_breakeven", float('inf'))
                _mr_v1_net = _mr_v1.get("net_monthly", 0)
                _mr_v1_months_str = (
                    f" At ~${_mr_v1_net:.2f}/mo, ~{_mr_v1_months:.0f} months to close gap."
                    if _mr_v1_net > 0 and _mr_v1_months < 999
                    else ""
                )
                result.update({
                    "Action": "ROLL",
                    "Urgency": "HIGH",
                    "Rationale": (
                        f"Moderate recovery: hard stop approaching (${spot:.2f} vs "
                        f"${hard_stop_price:.2f}, {_gap_pct:.1%} cushion). "
                        f"Thesis {_thesis_hs} but income path active — "
                        f"${cum_premium:.2f}/share collected.{_mr_v1_months_str} "
                        f"Roll call down aggressively to reduce basis before "
                        f"drawdown deepens (McMillan Ch.3: Early Basis Reduction; "
                        f"Jabbour Ch.4: repair when damage is manageable)."
                    ),
                    "Doctrine_Source": "McMillan Ch.3: Early Basis Reduction + Jabbour Ch.4: Manageable Repair",
                    "Decision_State": STATE_ACTIONABLE,
                    "Required_Conditions_Met": True
                })
                return result

            # Forward-economics guard for DEGRADED thesis near hard stop:
            # if forward income can still close the gap, ROLL instead of EXIT.
            if _thesis_hs != 'BROKEN':
                try:
                    from core.management.cycle3.doctrine.helpers import compute_forward_income_economics
                    from core.management.cycle3.doctrine.thresholds import FORWARD_ECON_MONTHS_BW_APPROACHING
                    _fe_app = compute_forward_income_economics(row, spot, effective_cost)
                    if (_fe_app["viable"]
                            and _fe_app["months_to_breakeven"] < FORWARD_ECON_MONTHS_BW_APPROACHING):
                        result.update({
                            "Action": "ROLL",
                            "Urgency": "HIGH",
                            "Rationale": (
                                f"Forward-economics override: hard stop approaching "
                                f"({_gap_pct:.1%} cushion), thesis {_thesis_hs}, but "
                                f"forward income can close gap in "
                                f"~{_fe_app['months_to_breakeven']:.0f} months "
                                f"(${_fe_app['net_monthly']:.2f}/mo net). "
                                f"Roll call down aggressively to accelerate basis "
                                f"reduction (McMillan Ch.3 + Jabbour Ch.4: forward "
                                f"EV check before panic exit on degraded thesis)."
                            ),
                            "Doctrine_Source": "McMillan Ch.3 + Jabbour Ch.4: Forward-Econ Approaching Stop",
                            "Decision_State": STATE_ACTIONABLE,
                            "Required_Conditions_Met": True,
                        })
                        return result
                except Exception:
                    pass  # Fall through to original EXIT

            result.update({
                "Action": "EXIT",
                "Urgency": "CRITICAL",
                "Exit_Trigger_Type": "CAPITAL",
                "Rationale": (
                    f"Hard stop imminent: ${spot:.2f} vs hard stop ${hard_stop_price:.2f} "
                    f"({_gap_pct:.1%} cushion). Thesis is {_thesis_hs} — rolling into further "
                    f"debit would compound a failed thesis. Exit stock + buy back call now "
                    f"(McMillan Ch.3: Hard Stop — thesis degraded, exit before breach)."
                ),
                "Doctrine_Source": "McMillan Ch.3: Hard Stop — thesis degraded, exit before breach",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result

        result.update({
            "Action": "ROLL",
            "Urgency": "HIGH",
            "Rationale": (
                f"Approaching hard stop: ${spot:.2f} vs net cost ${effective_cost:.2f} "
                f"({drift_from_net:.1%}).{premium_note} "
                f"Hard stop at ${hard_stop_price:.2f} — only ${gap_to_stop:.2f}/share cushion. "
                f"Roll call down aggressively to collect more premium and lower net cost "
                f"(McMillan Ch.3: Basis Reduction Under Pressure)."
            ),
            "Doctrine_Source": "McMillan Ch.3: Basis Reduction Under Pressure",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True
        })
        return result

    # ── Recently-Rolled Cooldown gate (Signal Coherence Gate 1) ─────────
    # Income strategies (BUY_WRITE/COVERED_CALL): 3 trading-day cooldown.
    # Fires AFTER hard stops (genuine emergencies bypass) but BEFORE all
    # discretionary gates (earnings, pre-ITM, gamma, income, EV).
    # Prevents self-contradicting roll chains (Natenberg Ch.7: adjustment
    # frequency; Jabbour Ch.8: repair = overtrading).
    _COOLDOWN_DAYS_BW = 3
    _days_since_roll = row.get('Days_Since_Last_Roll')
    _thesis_for_cooldown = str(row.get('Thesis_State', '') or '').upper()
    if (
        pd.notna(_days_since_roll)
        and float(_days_since_roll) < _COOLDOWN_DAYS_BW
        and _thesis_for_cooldown in ('INTACT', 'UNKNOWN', '')
    ):
        result.update({
            "Action": "HOLD",
            "Urgency": "LOW",
            "Rationale": (
                f"Recently-rolled cooldown: current leg opened {int(_days_since_roll)}d ago "
                f"(< {_COOLDOWN_DAYS_BW}d window). Thesis is {_thesis_for_cooldown or 'UNKNOWN'} — "
                f"suppressing discretionary ROLL to prevent self-contradicting roll chains. "
                f"Natenberg Ch.7: 'Frequent adjustments cost more than the risk they mitigate.' "
                f"Jabbour Ch.8: 'Repair is a dangerous misnomer for overtrading.'"
            ),
            "Doctrine_Source": "Natenberg Ch.7 + Jabbour Ch.8: Recently-Rolled Cooldown",
            "Decision_State": STATE_NEUTRAL_CONFIDENT,
            "Required_Conditions_Met": True,
        })
        return result

    # ── 1b. Earnings Event Risk ─────────────────────────────────────────
    earnings_date = row.get('Earnings_Date')
    _is_leaps_dte = dte > DTE_LEAPS_THRESHOLD
    _pct_to_strike = abs(spot - strike) / strike if strike > 0 and spot > 0 else 1.0
    _near_strike = _pct_to_strike <= STRIKE_PROXIMITY_EARNINGS

    if earnings_date is not None and not _is_leaps_dte and _near_strike:
        try:
            snap_ts = pd.to_datetime(row.get('Snapshot_TS') or pd.Timestamp.now())
            earn_dt = pd.to_datetime(earnings_date)
            days_to_earnings = (earn_dt.normalize() - snap_ts.normalize()).days
            if 0 <= days_to_earnings <= max(int(dte), 7):
                _earn_urgency = "CRITICAL" if (drift_from_net <= -0.10 and days_to_earnings <= 5) else "HIGH"
                _loss_note = (
                    f" Position already at {drift_from_net:.1%} from net cost — "
                    f"earnings gap could breach hard stop in one session."
                    if drift_from_net < PNL_DRIFT_STRUCTURE_BROKEN else ""
                )
                _premium_note = f" ${cum_premium:.2f}/share collected to date." if cum_premium > 0 else ""
                # Append historical crush + move context if available
                _hist_note = ""
                _track_q = row.get('Earnings_Track_Quarters')
                if _track_q and pd.notna(_track_q) and int(_track_q) > 0:
                    _crush = row.get('Earnings_Avg_IV_Crush_Pct')
                    _gap = row.get('Earnings_Avg_Gap_Pct')
                    _ratio = row.get('Earnings_Avg_Move_Ratio')
                    _parts = []
                    if pd.notna(_crush):
                        _parts.append(f"avg IV crush {_crush*100:.0f}%")
                    if pd.notna(_gap):
                        _parts.append(f"avg |gap| {_gap*100:.1f}%")
                    if pd.notna(_ratio):
                        _label = "underpriced" if _ratio > 1.0 else "overpriced"
                        _parts.append(f"move ratio {_ratio:.2f} ({_label})")
                    if _parts:
                        _hist_note = f" Historical ({int(_track_q)}Q): {', '.join(_parts)}."
                result.update({
                    "Action": "ROLL",
                    "Urgency": _earn_urgency,
                    "Rationale": (
                        f"Earnings in {days_to_earnings}d (within {dte:.0f}d DTE window).{_loss_note}"
                        f"{_premium_note}{_hist_note} Roll call before event: move to post-earnings expiry and "
                        f"consider wider strike to absorb gap risk "
                        f"(Natenberg Ch.12: Event Gap Risk — delta cannot protect against discontinuous moves)."
                    ),
                    "Doctrine_Source": "Natenberg Ch.12: Earnings Event Risk",
                    "Decision_State": STATE_ACTIONABLE,
                    "Required_Conditions_Met": True
                })
                return result
            elif -7 <= days_to_earnings < 0:
                price_change = drift_from_net
                if price_change < PNL_POST_EARNINGS_DROP:
                    _post_hist = ""
                    _track_q = row.get('Earnings_Track_Quarters')
                    if _track_q and pd.notna(_track_q) and int(_track_q) > 0:
                        _beat = row.get('Earnings_Beat_Rate')
                        _drift = row.get('Earnings_Last_Surprise_Pct')
                        _parts = []
                        if pd.notna(_beat):
                            _parts.append(f"beat rate {_beat*100:.0f}%")
                        if pd.notna(_drift):
                            _parts.append(f"last surprise {_drift:.1f}%")
                        if _parts:
                            _post_hist = f" Track record ({int(_track_q)}Q): {', '.join(_parts)}."
                    result.update({
                        "Action": "HOLD",
                        "Urgency": "MEDIUM",
                        "Rationale": (
                            f"Earnings passed {abs(days_to_earnings)}d ago. Post-earnings drop of "
                            f"{price_change:.1%} from net cost.{_post_hist} Assess: was this guidance-driven "
                            f"(structural — consider exit) or beat/miss reaction "
                            f"(temporary — continue collecting). Check HV normalization: "
                            f"if IV crushes back, thesis may be intact "
                            f"(Natenberg Ch.12: Post-Event Assessment)."
                        ),
                        "Doctrine_Source": "Natenberg Ch.12: Post-Event Assessment",
                        "Decision_State": STATE_NEUTRAL_CONFIDENT,
                        "Required_Conditions_Met": True
                    })
                    return result
        except Exception as _bw_post_earn_err:
            logger.debug(f"BW post-earnings assessment skipped: {_bw_post_earn_err}")

    # ── 1b-GAP2: Earnings Lockdown ──────────────────────────────────────
    if (
        earnings_date is not None
        and not _is_leaps_dte
        and result.get('Action') == 'ROLL'
    ):
        try:
            snap_ts_lk = pd.to_datetime(row.get('Snapshot_TS') or pd.Timestamp.now())
            earn_dt_lk = pd.to_datetime(earnings_date)
            days_to_earn_lk = (earn_dt_lk.normalize() - snap_ts_lk.normalize()).days
            if 0 <= days_to_earn_lk <= 2:
                result.update({
                    "Action": "EXIT",
                    "Urgency": "HIGH",
                    "Rationale": (
                        f"Earnings lockdown: earnings in {days_to_earn_lk}d. "
                        f"Rolling a short call into a binary event extends assignment exposure — "
                        f"gap risk cannot be managed by delta. "
                        f"Exit the short call before announcement. "
                        f"(Jabbour Ch.8: never roll into earnings; Given Ch.4: close before event.)"
                    ),
                    "Doctrine_Source": "Jabbour Ch.8 + Given Ch.4: Earnings Lockdown",
                    "Decision_State": STATE_ACTIONABLE,
                    "Required_Conditions_Met": True,
                })
                return result
        except Exception as _bw_lockdown_err:
            logger.debug(f"BW earnings lockdown gate skipped: {_bw_lockdown_err}")

    # ── 1b-LEAPS: Earnings routine note (DTE > 90) ─────────────────────
    if earnings_date is not None and _is_leaps_dte:
        try:
            snap_ts = pd.to_datetime(row.get('Snapshot_TS') or pd.Timestamp.now())
            earn_dt = pd.to_datetime(earnings_date)
            days_to_earnings = (earn_dt.normalize() - snap_ts.normalize()).days
            if 0 <= days_to_earnings <= EARNINGS_NOTE_WINDOW:
                _earn_note = (
                    f" [Earnings in {days_to_earnings}d — LEAPS call at "
                    f"${strike:.0f} is ${abs(spot-strike):.0f} OTM ({_pct_to_strike:.0%} from strike). "
                    f"No roll needed; event risk is priced into long-dated IV. "
                    f"Monitor if stock gaps toward strike post-earnings.]"
                )
                result['Rationale'] = result.get('Rationale', '') + _earn_note
        except Exception as _bw_leaps_err:
            logger.debug(f"BW LEAPS earnings note skipped: {_bw_leaps_err}")

    # ── 1c. Underlying Health Story Check ───────────────────────────────
    _price_struct = str(row.get('PriceStructure_State', '') or '').upper()
    _trend_integ = str(row.get('TrendIntegrity_State', '') or '').upper()
    _ema50_slope = safe_row_float(row, 'ema50_slope')
    _hv_percentile = safe_row_float(row, 'hv_20d_percentile', default=50.0)

    if 'STRUCTURE_BROKEN' in _price_struct and _trend_integ in ('NO_TREND', 'TREND_EXHAUSTED'):
        _struct_note = (
            f"Price structure BROKEN + trend exhausted. "
            f"This is not a timing problem — the underlying thesis is structurally invalid. "
        )
        if effective_cost > 0 and drift_from_net < PNL_DRIFT_STRUCTURE_BROKEN:
            result.update({
                "Action": "EXIT",
                "Urgency": "HIGH",
                "Rationale": (
                    f"Underlying story BROKEN: {_struct_note}"
                    f"Stock at ${spot:.2f} vs net cost ${effective_cost:.2f} ({drift_from_net:.1%}). "
                    f"${cum_premium:.2f}/share collected, but continued rolling locks in deeper losses. "
                    f"Exit stock + buy back call. Do not roll a broken thesis "
                    f"(Passarelli Ch.2: Story Check — thesis must be intact to continue managing)."
                ),
                "Doctrine_Source": "Passarelli Ch.2: Story Check",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result
        else:
            result['Rationale'] = (
                f"⚠️ Story Check: {_struct_note}"
                f"Verify thesis before next roll (Passarelli Ch.2). "
            ) + result.get('Rationale', '')

    elif _ema50_slope < -0.02 and _hv_percentile > 70 and drift_from_net < PNL_DRIFT_STRUCTURE_BROKEN:
        _degrade_note = (
            f"EMA50 slope={_ema50_slope:.3f} (declining) + HV at {_hv_percentile:.0f}th percentile "
            f"(elevated fear). Underlying trending against position under high volatility. "
        )
        if result.get('Urgency', 'LOW') == 'LOW':
            result['Urgency'] = 'MEDIUM'
        result['Rationale'] = (
            f"⚠️ Underlying weakening: {_degrade_note}"
        ) + result.get('Rationale', '')

    # ── 2. ITM Defense (BW-specific with cost-basis context) ────────────
    if delta > DELTA_ITM_EMERGENCY:
        if strike > 0 and effective_cost > 0 and strike < effective_cost:
            loss_per_share = effective_cost - strike
            assignment_outcome = (
                f"Assignment at ${strike:.2f} BELOW net cost ${effective_cost:.2f} "
                f"(−${loss_per_share:.2f}/share loss despite ${cum_premium:.2f}/share collected). "
                f"Roll up to strike above ${effective_cost:.2f} to preserve breakeven."
            )
            urgency = "CRITICAL"
        elif strike > 0 and effective_cost > 0 and strike >= effective_cost:
            profit_per_share = strike - effective_cost
            assignment_outcome = (
                f"Assignment at ${strike:.2f} above net cost ${effective_cost:.2f} "
                f"(+${profit_per_share:.2f}/share profit including ${cum_premium:.2f}/share collected). "
                f"Roll up/out to capture more upside, or allow assignment."
            )
            urgency = "HIGH"
        else:
            assignment_outcome = f"Assignment risk — roll up/out to maintain income."
            urgency = "HIGH"

        result.update({
            "Action": "ROLL",
            "Urgency": urgency,
            "Rationale": f"Call deep ITM (Delta={delta:.2f} > 0.70). {assignment_outcome} (McMillan Ch.3: ITM Defense).",
            "Doctrine_Source": "McMillan Ch.3: ITM Defense",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True
        })
        return result

    # ── 3. Expiration proximity: DTE < 7 ────────────────────────────────
    if dte < DTE_EMERGENCY_ROLL:
        result.update({
            "Action": "ROLL",
            "Urgency": "HIGH",
            "Rationale": f"DTE={dte:.0f} < 7 — pin risk and gamma acceleration. Roll to next cycle (McMillan Ch.3: Expiration Management).",
            "Doctrine_Source": "McMillan Ch.3: Expiration Management",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True
        })
        return result

    # ── Pre-ITM Drift Warning: 0.55 < delta ≤ 0.70 ─────────────────────
    _call_last_pre = abs(safe_row_float(row, 'Short_Call_Last', 'Last'))
    _intrinsic_pre = max(0.0, spot - strike) if strike > 0 else 0.0
    _extrinsic_pre = max(0.0, _call_last_pre - _intrinsic_pre) if _call_last_pre > 0 else 0.0
    _extrinsic_pct = (_extrinsic_pre / _call_last_pre) if _call_last_pre > 0 else 0.0
    _strike_prox = ((spot - strike) / strike) if strike > 0 else 0.0

    _pre_itm_delta_warn = (DELTA_PRE_ITM_WARNING < delta <= DELTA_ITM_EMERGENCY)
    _pre_itm_strike_prox = (strike > 0 and 0 <= _strike_prox <= STRIKE_PROXIMITY_NARROW)
    _credit_still_viable = (_extrinsic_pct >= EXTRINSIC_CREDIT_VIABLE)
    _credit_strong = (_extrinsic_pct >= EXTRINSIC_CREDIT_STRONG)

    if _pre_itm_delta_warn or _pre_itm_strike_prox:
        _delta_gap_to_emergency = max(0.0, DELTA_ITM_EMERGENCY - delta)
        _strike_below_cost = (effective_cost > 0 and strike > 0 and strike < effective_cost)
        _rescue_debit_est = _call_last_pre

        if _credit_strong:
            if _strike_below_cost:
                _credit_label = (
                    f"Extrinsic = ${_extrinsic_pre:.2f} ({_extrinsic_pct:.0%} of option price). "
                    f"Two paths: (A) same-strike rollout = small credit (preserves ${_extrinsic_pre:.2f} extrinsic); "
                    f"(B) rescue roll above ${effective_cost:.2f} = debit roll required "
                    f"(buying back ${_call_last_pre:.2f} intrinsic+extrinsic, selling cheaper OTM premium). "
                    f"Debit is cheapest NOW — grows as delta rises toward 0.70."
                )
            else:
                _credit_label = f"Extrinsic = ${_extrinsic_pre:.2f} ({_extrinsic_pct:.0%} of option price) — CREDIT ROLL VIABLE"
            _window_quality = "optimal"
            _urgency_pre = "MEDIUM"
        elif _credit_still_viable:
            if _strike_below_cost:
                _credit_label = (
                    f"Extrinsic = ${_extrinsic_pre:.2f} ({_extrinsic_pct:.0%} of option price) — credit thin. "
                    f"Rescue roll above ${effective_cost:.2f} requires debit; debit cost grows daily. Act soon."
                )
            else:
                _credit_label = f"Extrinsic = ${_extrinsic_pre:.2f} ({_extrinsic_pct:.0%} of option price) — credit thin, act soon"
            _window_quality = "closing"
            _urgency_pre = "HIGH"
        else:
            _credit_label = f"Extrinsic = ${_extrinsic_pre:.2f} ({_extrinsic_pct:.0%}) — mostly intrinsic, debit roll required"
            _window_quality = "closing fast"
            _urgency_pre = "HIGH"

        _basis_note = (
            f" Net cost: ${effective_cost:.2f}. Strike ${strike:.2f} is "
            f"{'ABOVE' if strike >= effective_cost else 'BELOW'} net cost "
            f"({'protected' if strike >= effective_cost else f'roll above ${effective_cost:.2f} to protect breakeven'})."
        ) if effective_cost > 0 and strike > 0 else ""

        result.update({
            "Action": "ROLL",
            "Urgency": _urgency_pre,
            "Rationale": (
                f"⚠️ ROLL WINDOW {'OPTIMAL' if _window_quality == 'optimal' else 'CLOSING'} — "
                f"Delta={delta:.2f} entering ITM defense zone (0.55–0.70). "
                f"{_credit_label}.{_basis_note} "
                f"Δ gap to emergency gate: {_delta_gap_to_emergency:.2f}. "
                f"Act now — debit cost rises as delta climbs toward 0.70. "
                f"(Passarelli Ch.5: pre-ITM roll timing / McMillan Ch.3: anticipatory defense)"
            ),
            "Doctrine_Source": "Passarelli Ch.5: Pre-ITM Roll Timing",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True,
        })
        # Do NOT return — fall through to roll timing classification
        _pos_regime_pre = str(row.get('Position_Regime', 'NEUTRAL') or 'NEUTRAL')
        if _pos_regime_pre == 'TRENDING_CHASE':
            result['Rationale'] += (
                " ⚠️ TRENDING_CHASE: stock is trending through strikes. "
                "This ITM event is structural, not temporary — assignment may be the correct outcome."
            )

    # ── Roll Timing Intelligence ────────────────────────────────────────
    _timing = classify_roll_timing(row)

    # ── Thesis block interceptor ────────────────────────────────────────
    if result.get('_thesis_blocks_roll'):
        _thesis_sum = str(row.get('Thesis_Summary', '') or '')
        result.update({
            "Action": "HOLD",
            "Urgency": "HIGH",
            "Rationale": (
                f"🚫 Thesis BROKEN — discretionary roll blocked. {_thesis_sum} "
                f"Evaluate: is this STRUCTURAL (exit) or TEMPORARY (hold)? "
                f"If structural, exit stock + buy back call. "
                f"If temporary, hold and reassess after recovery signals confirm "
                f"(McMillan Ch.3, Passarelli Ch.2: story check)."
            ),
            "Doctrine_Source": "ThesisEngine: BROKEN story gate",
            "Decision_State": STATE_UNCERTAIN,
            "Required_Conditions_Met": False,
        })
        return result

    # ── Post-BUYBACK sticky gate ────────────────────────────────────────
    _prior_action = str(row.get('Prior_Action', '') or '').upper()
    _ei_state_pre = str(row.get('Equity_Integrity_State', '') or '').strip()

    if _prior_action == "BUYBACK" and _ei_state_pre != "INTACT":
        result.update({
            "Action": "HOLD",
            "Urgency": "HIGH",
            "Rationale": (
                f"Post-BUYBACK hold — short call was removed, stock held unencumbered. "
                f"Equity Integrity is {_ei_state_pre or 'UNKNOWN'} (not yet INTACT). "
                f"Do NOT re-sell premium until structure confirms recovery. "
                f"Stock carries ${effective_cost * FIDELITY_MARGIN_RATE_DAILY * 100:.2f}/day margin cost "
                f"with zero theta offset — accept this cost as the price of decoupling. "
                f"(Jabbour Ch.11: re-evaluate only after structure resolves; "
                f"McMillan Ch.3: stock and call decisions are independent)"
            ),
            "Doctrine_Source": "Post-BUYBACK: Equity not INTACT — hold unencumbered",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True,
        })
        return result


    # ── Gamma Danger Zone gate ──────────────────────────────────────────
    _gdz_strike = safe_row_float(row, 'Short_Call_Strike', 'Strike')
    _gdz_theta = abs(safe_row_float(row, 'Theta'))
    _gdz_gamma = abs(safe_row_float(row, 'Gamma'))
    _gdz_hv = safe_row_float(row, 'HV_20D', default=0.20)
    if _gdz_hv >= 1.0:
        _gdz_hv /= 100.0
    _gdz_sigma = spot * (_gdz_hv / math.sqrt(252)) if spot > 0 else 0.0
    _gdz_drag = 0.5 * _gdz_gamma * (_gdz_sigma ** 2)
    _gdz_roc3 = safe_row_float(row, 'Gamma_ROC_3D')
    _gdz_atm_pct = abs(spot - _gdz_strike) / spot if spot > 0 and _gdz_strike > 0 else 1.0
    _gdz_ratio = _gdz_drag / _gdz_theta if _gdz_theta > 0 else 0.0
    _gdz_ei = str(row.get('Equity_Integrity_State', '') or '').strip()

    _gdz_fires = (
        _gdz_atm_pct < GAMMA_ATM_PROXIMITY
        and DTE_EMERGENCY_ROLL < dte <= DTE_INCOME_GATE
        and _gdz_ratio > GAMMA_DANGER_RATIO
        and _gdz_ei != 'BROKEN'
    )

    if _gdz_fires:
        _gdz_urgency = "HIGH" if _gdz_roc3 > 0 else "MEDIUM"
        _gdz_roc_note = (
            f"Gamma_ROC_3D={_gdz_roc3:+.4f} (accelerating — urgency escalated). "
            if _gdz_roc3 > 0 else
            f"Gamma_ROC_3D={_gdz_roc3:+.4f} (stable/declining). "
        )

        # ── Assignment-vs-Roll economics check ────────────────────────────
        # When stock is ITM or ATM on a buy-write AND carry is negative,
        # assignment may be better than paying to roll. Assignment at strike
        # = defined exit. Roll = pay debit to extend a negative-carry position.
        # McMillan Ch.3: "Assignment on a buy-write is not a failure — it's
        # the intended profit-taking mechanism when the call is ITM."
        _gdz_call_last = abs(safe_row_float(row, 'Last', 'Short_Call_Last'))
        _gdz_intrinsic = max(0.0, spot - _gdz_strike) if spot > 0 and _gdz_strike > 0 else 0.0
        _gdz_extrinsic = max(0.0, _gdz_call_last - _gdz_intrinsic)
        _gdz_carry_negative = _gdz_drag > _gdz_theta  # gamma drag > theta income
        _gdz_below_cost = drift_from_net < 0  # stock below net cost basis
        _gdz_assignment_loss = (_gdz_strike - effective_cost) if effective_cost > 0 else 0.0

        # Assignment-vs-roll threshold: extrinsic as % of option price.
        # When < 15%, option is mostly intrinsic — roll buyback costs more than
        # the new premium can cover. When extrinsic is substantial (like ATM at
        # 11 DTE with high IV), same-strike rollout captures that time value as credit.
        _gdz_extrinsic_pct = (_gdz_extrinsic / _gdz_call_last) if _gdz_call_last > 0 else 0.0
        if (
            _gdz_carry_negative
            and _gdz_below_cost
            and _gdz_strike > 0
            and effective_cost > 0
            and _gdz_extrinsic_pct < 0.15  # mostly intrinsic — debit roll likely
        ):
            result.update({
                "Action": "HOLD",
                "Urgency": "HIGH",
                "Rationale": (
                    f"Gamma Danger Zone: near-ATM ({_gdz_atm_pct:.1%} from strike "
                    f"${_gdz_strike:.2f}), DTE={dte:.0f}, gamma/theta ratio={_gdz_ratio:.1f}x. "
                    f"{_gdz_roc_note}"
                    f"BUT roll economics unfavorable: carry is negative "
                    f"(gamma drag ${_gdz_drag*100:.2f}c > theta ${_gdz_theta*100:.2f}c/day), "
                    f"stock {drift_from_net:.1%} below net cost ${effective_cost:.2f}, "
                    f"extrinsic only ${_gdz_extrinsic:.2f}. "
                    f"Assignment at ${_gdz_strike:.2f} realizes "
                    f"${_gdz_assignment_loss:+.2f}/share vs cost basis — "
                    f"{'a defined loss but stops the bleed' if _gdz_assignment_loss < 0 else 'a gain'}. "
                    f"Consider: (A) let assignment happen at ${_gdz_strike:.2f}, "
                    f"(B) buy back call for ${_gdz_call_last:.2f} and hold stock for recovery, "
                    f"(C) roll ONLY if credit available at higher strike. "
                    f"Do NOT pay debit to extend negative carry. "
                    f"(McMillan Ch.3: assignment is the buy-write profit mechanism; "
                    f"Natenberg Ch.7: don't roll when gamma > theta.)"
                ),
                "Doctrine_Source": "McMillan Ch.3: Assignment economics + Natenberg Ch.7: Gamma danger",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True,
            })
            return result

        result.update({
            "Action": "ROLL",
            "Urgency": _gdz_urgency,
            "Rationale": (
                f"Gamma Danger Zone: near-ATM ({_gdz_atm_pct:.1%} from strike "
                f"${_gdz_strike:.2f}), DTE={dte:.0f}, gamma/theta ratio={_gdz_ratio:.1f}x. "
                f"{_gdz_roc_note}"
                f"Gamma drag ${_gdz_drag*100:.2f}c/contract/day approaching theta "
                f"${_gdz_theta*100:.2f}c/contract/day — short gamma accelerating toward dominance. "
                f"Roll to extend DTE (30-45d target reduces gamma ~40%) or move strike OTM. "
                f"Natenberg Ch.7: 'ATM + low DTE is the maximum-risk configuration for short gamma.' "
                f"Passarelli Ch.6: 'Pre-emptive roll before gamma overwhelms theta.'"
            ),
            "Doctrine_Source": "Natenberg Ch.7: Gamma danger zone + Passarelli Ch.6: pre-emptive roll",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True,
        })
        return result

    # ── Equity Integrity gate ───────────────────────────────────────────
    _ei_state = str(row.get('Equity_Integrity_State', '') or '').strip()
    _ei_reason = str(row.get('Equity_Integrity_Reason', '') or '').strip()
    if _ei_state == 'BROKEN' and result.get('Action') not in ('EXIT', 'HARD_HALT'):
        _ei_theta = abs(safe_row_float(row, 'Theta'))
        _ei_gamma = abs(safe_row_float(row, 'Gamma'))
        _ei_hv = safe_row_float(row, 'HV_20D', default=0.20)
        if _ei_hv >= 1.0:
            _ei_hv /= 100.0
        _ei_sigma = spot * (_ei_hv / math.sqrt(252))
        _ei_gamma_drag = 0.5 * _ei_gamma * (_ei_sigma ** 2)

        _ei_short_strike = safe_row_float(row, 'Short_Call_Strike', 'Strike')
        _ei_otm_pct = (
            abs(_ei_short_strike - spot) / spot
            if _ei_short_strike > 0 and spot and spot > 0 else 0.0
        )
        _ei_gamma_dominant = (
            _ei_theta > 0
            and _ei_gamma_drag > _ei_theta * GAMMA_DOMINANCE_RATIO
            and _ei_otm_pct <= GAMMA_MONEYNESS_GUARD
        )

        if _ei_gamma_dominant:
            _ei_ratio = _ei_gamma_drag / _ei_theta if _ei_theta > 0 else float('inf')
            _ei_call_last = abs(safe_row_float(row, 'Short_Call_Last', 'Last'))
            _ei_entry = abs(safe_row_float(row, 'Short_Call_Premium', 'Premium_Entry'))

            _ei_intrinsic = max(0.0, spot - _ei_short_strike) if spot > 0 and _ei_short_strike > 0 else 0.0
            _ei_extrinsic_val = max(0.0, _ei_call_last - _ei_intrinsic)

            _ei_adx = safe_row_float(row, 'adx_14')
            _ei_roc = safe_row_float(row, 'roc_20')

            _ei_buyback_conviction = (
                dte <= DTE_EMERGENCY_ROLL
                or (spot > _ei_short_strike * BREAKOUT_THROUGH_STRIKE and _ei_roc > 0 and _ei_adx > 25)
                or _ei_extrinsic_val < EXTRINSIC_THETA_EXHAUSTED
            )

            if _ei_buyback_conviction:
                _ei_close_note = (
                    f" Current call at ${_ei_call_last:.2f} vs ${_ei_entry:.2f} entry "
                    f"({'profit' if _ei_call_last < _ei_entry else 'debit'} to close)."
                    if _ei_call_last > 0 and _ei_entry > 0 else ""
                )
                _ei_conv_reason = (
                    "DTE ≤ 7 (expiration week)" if dte <= DTE_EMERGENCY_ROLL else
                    f"stock ${spot:.2f} above strike ${_ei_short_strike:.2f} + momentum (ROC={_ei_roc:.1f}, ADX={_ei_adx:.0f})"
                    if spot > _ei_short_strike * BREAKOUT_THROUGH_STRIKE else
                    f"extrinsic only ${_ei_extrinsic_val:.2f} (< $0.20 — no theta left)"
                )
                result.update({
                    "Action": "ROLL",
                    "Urgency": "HIGH",
                    "Rationale": (
                        f"⚡ Equity BROKEN + gamma dominant ({_ei_ratio:.1f}× theta) + "
                        f"buyback conviction ({_ei_conv_reason}): "
                        f"gamma drag ${_ei_gamma_drag*100:.2f}/contract/day vs theta "
                        f"${_ei_theta*100:.2f}/contract/day — HOLD bleeds ${(_ei_gamma_drag - _ei_theta)*100:.2f}/contract/day. "
                        f"{_ei_close_note} "
                        f"BUY BACK the short call to stop the gamma bleed and decouple from the stock decision. "
                        f"Then evaluate the stock independently: if thesis broken → sell stock; "
                        f"if temporary → re-sell a 30–45 DTE near-ATM call for better theta efficiency. "
                        f"(Passarelli Ch.6: close short premium in expiration week; "
                        f"Natenberg Ch.7: short gamma at {_ei_ratio:.1f}× theta is structurally unprofitable)"
                    ),
                    "Doctrine_Source": "Passarelli Ch.6: Expiration week close + Natenberg Ch.7: gamma/theta ratio",
                    "Decision_State": STATE_ACTIONABLE,
                    "Required_Conditions_Met": True,
                })
                _cn = result.pop('_cadence_note', None)
                if _cn:
                    result['Rationale'] += _cn
                return result
            else:
                # Gamma dominant but no conviction — check carry
                _gd_margin_daily = effective_cost * FIDELITY_MARGIN_RATE_DAILY if effective_cost > 0 else 0.0
                _gd_net_carry = _ei_theta - _gd_margin_daily - _ei_gamma_drag

                # Cost-basis cushion guard
                _gd_above_net_cost = (
                    effective_cost > 0
                    and spot > effective_cost
                    and dte > DTE_CUSHION_WINDOW
                )
                _gd_cushion_pct = (
                    (spot - effective_cost) / effective_cost
                    if effective_cost > 0 and spot > effective_cost else 0.0
                )

                if _gd_net_carry < 0 and not _gd_above_net_cost:
                    _gd_bleed_contract = abs(_gd_net_carry) * 100
                    _gd_bleed_to_exp = _gd_bleed_contract * dte
                    result.update({
                        "Action": "EXIT",
                        "Urgency": "MEDIUM",
                        "Rationale": (
                            f"⚠️ Equity BROKEN ({_ei_reason}) + negative carry: "
                            f"θ ${_ei_theta*100:.2f}/day − margin ${_gd_margin_daily*100:.2f}/day "
                            f"− γ-drag ${_ei_gamma_drag*100:.2f}/day = "
                            f"net bleed ${_gd_bleed_contract:.2f}/contract/day "
                            f"(${_gd_bleed_to_exp:.0f} to expiry). "
                            f"Stock ${spot:.2f} is BELOW net cost basis ${effective_cost:.2f} — "
                            f"no premium cushion to absorb the bleed. "
                            f"Gamma dominance ({_ei_ratio:.1f}× theta) confirms drag exceeds income. "
                            f"Close the position or buy back the call "
                            f"and evaluate the stock independently. "
                            f"(McMillan Ch.3: don't carry a broken structure at negative EV; "
                            f"Natenberg Ch.7: negative carry + broken equity = structural loss)"
                        ),
                        "Doctrine_Source": "EquityIntegrity: BROKEN + Negative Carry → EXIT",
                        "Decision_State": STATE_ACTIONABLE,
                        "Required_Conditions_Met": True,
                    })
                    return result

                if _gd_net_carry < 0 and _gd_above_net_cost:
                    _gd_bleed_contract = abs(_gd_net_carry) * 100
                    _gd_cushion_days = int(
                        (spot - effective_cost) * 100 / _gd_bleed_contract
                    ) if _gd_bleed_contract > 0 else 999
                    result.update({
                        "Action": "HOLD",
                        "Urgency": "MEDIUM",
                        "Rationale": (
                            f"⚠️ Equity BROKEN ({_ei_reason}) + negative daily carry: "
                            f"θ ${_ei_theta*100:.2f}/day − margin ${_gd_margin_daily*100:.2f}/day "
                            f"− γ-drag ${_ei_gamma_drag*100:.2f}/day = "
                            f"net bleed ${_gd_bleed_contract:.2f}/contract/day. "
                            f"BUT stock ${spot:.2f} is {_gd_cushion_pct:.1%} ABOVE net cost basis "
                            f"${effective_cost:.2f} (cushion from ${cum_premium:.2f}/share collected). "
                            f"Premium cushion absorbs ~{_gd_cushion_days}d of bleed before breakeven is threatened. "
                            f"Monitor for: (A) stock approaching net cost basis → upgrade to EXIT, "
                            f"(B) HV mean-reversion reducing gamma drag, "
                            f"(C) DTE ≤ 14 → roll or close. "
                            f"(McMillan Ch.3: cumulative premium income IS the BUY_WRITE edge — "
                            f"don't abandon accumulated cost reduction on a single cycle's carry metric)"
                        ),
                        "Doctrine_Source": "McMillan Ch.3: BUY_WRITE cost-basis cushion — HOLD with carry warning",
                        "Decision_State": STATE_ACTIONABLE,
                        "Required_Conditions_Met": True,
                    })
                    return result

                # Positive carry despite gamma dominance
                result.update({
                    "Action": "HOLD",
                    "Urgency": "MEDIUM",
                    "Rationale": (
                        f"⚠️ Equity BROKEN + gamma elevated ({_ei_ratio:.1f}× theta) — "
                        f"structurally expected at DTE {dte:.0f} near ATM (δ={delta:.2f}). "
                        f"Gamma drag ${_ei_gamma_drag*100:.2f}/contract/day vs theta "
                        f"${_ei_theta*100:.2f}/contract/day. "
                        f"Carry still positive (${_gd_net_carry*100:.2f}/day net) — "
                        f"theta income covers costs. Hold unless conviction develops: "
                        f"(A) stock breaks above ${_ei_short_strike:.2f} with momentum → buy back call, "
                        f"(B) DTE approaches expiration week (≤7d) → close or roll. "
                        f"(Passarelli Ch.6: near-ATM gamma at 2–3 weeks is structural, "
                        f"not an emergency — close short premium in expiration week, not before. "
                        f"Given: 'large gamma values are the reason ATM positions change value "
                        f"rapidly during expiration week' — this is expected behaviour at DTE {dte:.0f})"
                    ),
                    "Doctrine_Source": "Passarelli Ch.6: Gamma awareness — expiration week rule",
                    "Decision_State": STATE_ACTIONABLE,
                    "Required_Conditions_Met": True,
                })
                return result
        else:
            # Standard BROKEN gate: no gamma dominance — check LEAPS carry inversion
            _ei_is_leaps = dte > DTE_LEAPS_THRESHOLD
            if _ei_is_leaps:
                # Prefer pre-computed Daily_Margin_Cost (correct: borrowed portion only)
                _ci_pre_v1 = float(row.get('Daily_Margin_Cost') or 0.0)
                _ci_margin_daily = (_ci_pre_v1 / 100.0) if _ci_pre_v1 > 0 else (
                    effective_cost * FIDELITY_MARGIN_RATE_DAILY if effective_cost > 0 else 0.0
                )
                _ci_theta_daily = abs(safe_row_float(row, 'Theta'))
                _ci_strike = safe_row_float(row, 'Short_Call_Strike', 'Strike')
                _ci_pct_otm = (
                    abs(spot - _ci_strike) / spot
                    if spot > 0 and _ci_strike > 0 else 0.0
                )

                if _ci_margin_daily > 0 and _ci_theta_daily > 0 and _ci_margin_daily >= _ci_theta_daily:
                    _ci_ratio = _ci_margin_daily / _ci_theta_daily

                    if _ci_ratio >= CARRY_INVERSION_SEVERE:
                        result.update({
                            "Action": "BUYBACK",
                            "Urgency": "HIGH",
                            "Rationale": (
                                f"⚠️ Equity BROKEN + LEAPS carry severely inverted: "
                                f"margin cost ${_ci_margin_daily*100:.2f}/contract/day vs theta income "
                                f"${_ci_theta_daily*100:.2f}/contract/day ({_ci_ratio:.1f}× theta). "
                                f"Strike ${_ci_strike:.0f} is {_ci_pct_otm:.0%} OTM — theta too weak "
                                f"to cover financing. Buy back the short call. "
                                f"Do NOT re-sell while equity is BROKEN ({_ei_reason}) — hold stock "
                                f"unencumbered until structural deterioration resolves. "
                                f"(Given Ch.6: 'sell calls within one strike of ATM'; "
                                f"Jabbour Ch.11: 'close and re-evaluate rather than rolling a losing structure')"
                            ),
                            "Doctrine_Source": "Given Ch.6: LEAPS Carry Inversion (severe) + EquityIntegrity: BROKEN",
                            "Decision_State": STATE_ACTIONABLE,
                            "Required_Conditions_Met": True,
                        })
                        return result
                    else:
                        _ci_net_bleed = (_ci_margin_daily - _ci_theta_daily) * 100
                        result.update({
                            "Action": "HOLD",
                            "Urgency": "HIGH",
                            "Rationale": (
                                f"⚠️ Equity BROKEN + LEAPS carry mildly inverted: "
                                f"margin ${_ci_margin_daily*100:.2f}/day vs theta "
                                f"${_ci_theta_daily*100:.2f}/day ({_ci_ratio:.1f}× theta, "
                                f"net bleed ${_ci_net_bleed:.2f}/day). "
                                f"Buying back the call would increase bleed to "
                                f"${_ci_margin_daily*100:.2f}/day with zero income. "
                                f"HOLD — the short call still offsets most margin cost. "
                                f"Monitor: if ratio exceeds 1.5× or equity recovers to re-sell "
                                f"closer to ATM (Given Ch.6). Strike ${_ci_strike:.0f} "
                                f"is {_ci_pct_otm:.0%} OTM."
                            ),
                            "Doctrine_Source": "Given Ch.6: LEAPS Carry Mild Inversion + EquityIntegrity: BROKEN",
                            "Decision_State": STATE_ACTIONABLE,
                            "Required_Conditions_Met": True,
                        })
                        return result

            # Standard BROKEN gate: check net carry
            _ei_margin_daily = effective_cost * FIDELITY_MARGIN_RATE_DAILY if effective_cost > 0 else 0.0
            _ei_net_carry = _ei_theta - _ei_margin_daily - _ei_gamma_drag

            if _ei_net_carry < 0:
                _ei_bleed_contract = abs(_ei_net_carry) * 100
                _ei_bleed_to_exp = _ei_bleed_contract * dte

                # Cost-basis cushion guard: stock above net cost = accumulated premium cushion
                _ei_above_net_cost = (
                    effective_cost > 0
                    and spot > effective_cost
                    and dte > DTE_CUSHION_WINDOW
                )
                _ei_cushion_pct = (
                    (spot - effective_cost) / effective_cost
                    if effective_cost > 0 and spot > effective_cost else 0.0
                )

                if _ei_above_net_cost:
                    _ei_cushion_days = int(
                        (spot - effective_cost) * 100 / _ei_bleed_contract
                    ) if _ei_bleed_contract > 0 else 999

                    # Leg decomposition: when call is OTM, legs are independent decisions
                    _ei_sc_delta = abs(safe_row_float(row, 'Short_Call_Delta'))
                    _ei_sc_strike = safe_row_float(row, 'Short_Call_Strike', 'Strike')
                    _ei_sc_last = abs(safe_row_float(row, 'Short_Call_Last', 'Last'))
                    _ei_sc_pct_otm = (
                        (_ei_sc_strike - spot) / spot
                        if _ei_sc_strike > 0 and spot > 0 and _ei_sc_strike > spot else 0.0
                    )
                    _ei_stock_gain = (spot - effective_cost) * 100  # $ gain on 100 shares

                    if _ei_sc_delta < 0.30 and _ei_sc_pct_otm > 0.03:
                        # Call is sufficiently OTM — close both legs (shares are collateral)
                        _ei_buyback_cost = _ei_sc_last * 100  # cost to close the call
                        _ei_net_after_close = _ei_stock_gain - _ei_buyback_cost
                        result.update({
                            "Action": "EXIT",
                            "Urgency": "MEDIUM",
                            "Rationale": (
                                f"🟡 Close position — buy back call, then sell stock. "
                                f"Equity BROKEN ({_ei_reason}) + negative carry "
                                f"${_ei_bleed_contract:.2f}/contract/day. "
                                f"Step 1: buy back ${_ei_sc_strike:.0f} call at ${_ei_sc_last:.2f} "
                                f"(${_ei_buyback_cost:.0f} cost) — {_ei_sc_pct_otm:.0%} OTM, "
                                f"Δ {_ei_sc_delta:.3f}, cheap to close. "
                                f"Step 2: sell 100 shares at ${spot:.2f} — locks "
                                f"${_ei_stock_gain:.0f} gain above net cost ${effective_cost:.2f} "
                                f"({_ei_cushion_pct:.1%} cushion from ${cum_premium:.2f}/share "
                                f"collected across prior cycles). "
                                f"Net after closing both legs: ${_ei_net_after_close:+,.0f}. "
                                f"(McMillan Ch.3: lock the cost-basis gain, don't let bleed erode it; "
                                f"Passarelli Ch.6: call is {_ei_sc_pct_otm:.0%} OTM — "
                                f"buyback cost ${_ei_buyback_cost:.0f} is small vs "
                                f"${_ei_stock_gain:.0f} stock gain)"
                            ),
                            "Doctrine_Source": "McMillan Ch.3: lock cost-basis gain + Passarelli Ch.6: OTM call cheap to close",
                            "Decision_State": STATE_ACTIONABLE,
                            "Required_Conditions_Met": True,
                        })
                        return result

                    # Call near ATM — legs coupled, generic cushion HOLD
                    result.update({
                        "Action": "HOLD",
                        "Urgency": "MEDIUM",
                        "Rationale": (
                            f"⚠️ Equity BROKEN ({_ei_reason}) + negative daily carry: "
                            f"θ ${_ei_theta*100:.2f}/day − margin ${_ei_margin_daily*100:.2f}/day "
                            f"− γ-drag ${_ei_gamma_drag*100:.2f}/day = "
                            f"net bleed ${_ei_bleed_contract:.2f}/contract/day. "
                            f"BUT stock ${spot:.2f} is {_ei_cushion_pct:.1%} ABOVE net cost basis "
                            f"${effective_cost:.2f} (cushion from ${cum_premium:.2f}/share collected). "
                            f"Premium cushion absorbs ~{_ei_cushion_days}d of bleed before breakeven is threatened. "
                            f"Short call Δ {_ei_sc_delta:.2f} — too close to ATM to treat legs independently. "
                            f"Monitor for: (A) stock approaching net cost basis → upgrade to EXIT, "
                            f"(B) equity integrity recovery (BROKEN → WEAKENING), "
                            f"(C) DTE ≤ 14 → roll or close. "
                            f"(McMillan Ch.3: cumulative premium income IS the BUY_WRITE edge — "
                            f"don't abandon accumulated cost reduction on a single cycle's carry metric)"
                        ),
                        "Doctrine_Source": "McMillan Ch.3: BUY_WRITE cost-basis cushion — HOLD with carry warning",
                        "Decision_State": STATE_ACTIONABLE,
                        "Required_Conditions_Met": True,
                    })
                    return result

                result.update({
                    "Action": "EXIT",
                    "Urgency": "MEDIUM",
                    "Rationale": (
                        f"⚠️ Equity BROKEN ({_ei_reason}) + negative carry: "
                        f"θ ${_ei_theta*100:.2f}/day − margin ${_ei_margin_daily*100:.2f}/day "
                        f"− γ-drag ${_ei_gamma_drag*100:.2f}/day = "
                        f"net bleed ${_ei_bleed_contract:.2f}/contract/day "
                        f"(${_ei_bleed_to_exp:.0f} to expiry). "
                        f"Stock ${spot:.2f} is BELOW net cost basis ${effective_cost:.2f} — "
                        f"no premium cushion to absorb the bleed. "
                        f"Holding a structurally declining stock while paying to hold "
                        f"is a compounding loss. Close the position or buy back the call "
                        f"and evaluate the stock independently. "
                        f"(McMillan Ch.3: don't carry a broken structure at negative EV; "
                        f"Natenberg Ch.7: negative carry + broken equity = structural loss)"
                    ),
                    "Doctrine_Source": "EquityIntegrity: BROKEN + Negative Carry → EXIT",
                    "Decision_State": STATE_ACTIONABLE,
                    "Required_Conditions_Met": True,
                })
                return result

            # Positive carry: theta covers costs
            result.update({
                "Action": "HOLD",
                "Urgency": "HIGH",
                "Rationale": (
                    f"⚠️ Equity Integrity BROKEN — structural deterioration detected "
                    f"({_ei_reason}). "
                    f"Carry still positive: θ ${_ei_theta*100:.2f}/day > costs "
                    f"${(_ei_margin_daily + _ei_gamma_drag)*100:.2f}/day — "
                    f"theta income justifies patience. "
                    f"Rolling locks in deeper commitment to a structurally declining stock. "
                    f"Hold and reassess: confirm if breakdown is temporary or structural "
                    f"before next roll (McMillan Ch.1: trend context first)."
                ),
                "Doctrine_Source": "EquityIntegrity: BROKEN structural gate",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True,
            })
            return result

    if _ei_state == 'WEAKENING' and _ei_reason:
        result['Rationale'] = (
            result.get('Rationale', '') +
            f"  [⚠️ Equity WEAKENING: {_ei_reason} — monitor.]"
        )

    # ── 3a-LEAPS. Carry Inversion (non-BROKEN) ─────────────────────────
    _ci_is_leaps = dte > DTE_LEAPS_THRESHOLD
    if _ci_is_leaps:
        # Prefer pre-computed Daily_Margin_Cost (correct: borrowed portion only)
        _ci_pre_v1_nb = float(row.get('Daily_Margin_Cost') or 0.0)
        _ci_margin_daily = (_ci_pre_v1_nb / 100.0) if _ci_pre_v1_nb > 0 else (
            effective_cost * FIDELITY_MARGIN_RATE_DAILY if effective_cost > 0 else 0.0
        )
        _ci_theta_daily = abs(safe_row_float(row, 'Theta'))
        _ci_strike = safe_row_float(row, 'Short_Call_Strike', 'Strike')
        _ci_pct_otm = (
            abs(spot - _ci_strike) / spot
            if spot > 0 and _ci_strike > 0 else 0.0
        )

        if _ci_margin_daily > 0 and _ci_theta_daily > 0 and _ci_margin_daily >= _ci_theta_daily:
            _ci_ratio = _ci_margin_daily / _ci_theta_daily
            result.update({
                "Action": "ROLL",
                "Urgency": "MEDIUM",
                "Rationale": (
                    f"📊 LEAPS carry inverted: margin ${_ci_margin_daily*100:.2f}/contract/day ≥ "
                    f"theta ${_ci_theta_daily*100:.2f}/contract/day ({_ci_ratio:.1f}× theta). "
                    f"Strike ${_ci_strike:.0f} is {_ci_pct_otm:.0%} OTM — theta decays too slowly "
                    f"at this distance to cover financing at 10.375%/yr. "
                    f"Buy back and re-sell 30–45 DTE closer to ATM for efficient carry "
                    f"(Given Ch.6: 'one strike from ATM'; "
                    f"Augen: 'roll when the new position has similar dynamics')."
                ),
                "Doctrine_Source": "Given Ch.6: LEAPS Carry Inversion — re-sell closer",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True,
            })
            return result

    # ── 3b. Dividend assignment gate ────────────────────────────────────
    _bw_days_div = safe_row_float(row, 'Days_To_Dividend', default=9999.0)
    _bw_div_amt = safe_row_float(row, 'Dividend_Amount')
    if delta > DELTA_DIVIDEND_ASSIGNMENT and _bw_days_div < 5 and _bw_div_amt > 0:
        _bw_div_urgency = "CRITICAL" if _bw_days_div < 2 else "HIGH"
        result.update({
            "Action": "ROLL",
            "Urgency": _bw_div_urgency,
            "Rationale": (
                f"⚠️ Dividend assignment risk: ex-dividend in {_bw_days_div:.0f} day(s) "
                f"(${_bw_div_amt:.2f}/share), call delta={delta:.2f} > 0.50. "
                f"Call owners will exercise early to capture the dividend — "
                f"forced assignment before expiry is highly probable. "
                f"Roll up/out NOW to avoid early assignment: "
                f"close the short call and re-sell a further-OTM strike "
                f"(McMillan Ch.2: dividend-driven early exercise is the primary risk "
                f"of short calls near ex-date with delta > 0.50)."
            ),
            "Doctrine_Source": "McMillan Ch.2: Dividend Assignment Risk — BUY_WRITE (M1)",
            "Decision_State": STATE_ACTIONABLE,
            "Required_Conditions_Met": True,
        })
        return result

    # ── 3b-GAP1: 21-DTE income gate ────────────────────────────────────
    premium_collected_21 = abs(safe_row_float(row, 'Short_Call_Premium', 'Premium_Entry'))
    current_close_cost_21 = abs(safe_row_float(row, 'Short_Call_Last', 'Last'))
    pct_captured_21 = (
        (premium_collected_21 - current_close_cost_21) / premium_collected_21
        if premium_collected_21 > 0 else 0.0
    )
    _bw_moneyness = str(row.get('Moneyness_Label') or row.get('Short_Call_Moneyness') or 'OTM')

    # Far-OTM exemption (v1): same logic as v2 — don't roll far-OTM calls near expiry
    _mc_p_assign_v1 = safe_row_float(row, 'MC_Assign_P_Expiry')
    _far_otm_v1 = (
        delta < 0.30
        and (_mc_p_assign_v1 < 0.05 if not (isinstance(_mc_p_assign_v1, float) and _mc_p_assign_v1 != _mc_p_assign_v1) else delta < 0.25)
    )

    if (
        dte <= DTE_INCOME_GATE
        and dte >= DTE_EMERGENCY_ROLL
        and pct_captured_21 < PREMIUM_CAPTURE_TARGET
        and _bw_moneyness != 'ITM'
        and not _far_otm_v1
    ):
        _cc_21_urgency = 'MEDIUM' if pct_captured_21 >= 0 else 'HIGH'

        # Strategy-aware IV regime check
        _iv_entry_21 = safe_row_float(row, 'IV_Entry')
        _iv_now_21 = safe_row_float(row, 'IV_30D', 'IV_Now')
        _iv_pctile_21 = safe_row_float(row, 'IV_Percentile', default=50.0)
        _iv_gap_21 = safe_row_float(row, 'IV_vs_HV_Gap')

        _iv_collapsed_21 = (
            _iv_entry_21 > 0 and _iv_now_21 > 0
            and (_iv_now_21 / _iv_entry_21) < 0.70
            and _iv_pctile_21 < 25
            and _iv_gap_21 <= 0
        )

        if _iv_collapsed_21:
            result.update({
                "Action": "HOLD",
                "Urgency": "LOW",
                "Rationale": (
                    f"21-DTE income gate: DTE={dte:.0f}, {pct_captured_21:.0%} captured. "
                    f"Vol regime shift: IV contracted {(1 - _iv_now_21/_iv_entry_21):.0%} from entry "
                    f"({_iv_entry_21:.1%} -> {_iv_now_21:.1%}), IV_Percentile={_iv_pctile_21:.0f}, "
                    f"IV-HV gap={_iv_gap_21:+.1%}. "
                    f"Rolling into a low-IV environment yields thin premium — mean-reversion edge exhausted. "
                    f"Let current premium decay (remaining time value = ${current_close_cost_21:.2f}). "
                    f"Chan: 'Mean-reversion exit when regime shifts — don't repeat a trade whose edge is gone.' "
                    f"Natenberg Ch.8: 'Selling premium below HV = negative expected value.'"
                ),
                "Doctrine_Source": "Chan: Strategy-aware exit — Vol regime shift (BW)",
                "Decision_State": STATE_NEUTRAL_CONFIDENT,
                "Required_Conditions_Met": True,
            })
            return result

        # Position Trajectory context
        _pos_regime_21 = str(row.get('Position_Regime', 'NEUTRAL') or 'NEUTRAL')
        _consec_debits_21 = int(safe_row_float(row, 'Trajectory_Consecutive_Debit_Rolls'))
        _stock_ret_21 = safe_row_float(row, 'Trajectory_Stock_Return')
        if _pos_regime_21 == 'TRENDING_CHASE':
            _cc_21_urgency = 'HIGH'
            result.update({
                "Action": "ROLL",
                "Urgency": _cc_21_urgency,
                "Rationale": (
                    f"21-DTE income gate: DTE={dte:.0f} ≤ 21 with {pct_captured_21:.0%} captured. "
                    f"⚠️ TRENDING_CHASE: stock has moved {_stock_ret_21:+.0%} since entry "
                    f"with {_consec_debits_21} consecutive debit roll(s). "
                    f"Stock is structurally outrunning the covered call — this is NOT a temporary ITM event. "
                    f"Consider: (A) accept assignment and redeploy capital at higher basis, "
                    f"(B) buy back call and hold stock unencumbered for the trend, or "
                    f"(C) widen to a much higher strike if premium justifies carry. "
                    f"Rolling to the next monthly repeats the chase cycle. "
                    f"(McMillan Ch.3: strike-chase recognition; Given Ch.6: 21-DTE gate)"
                ),
                "Doctrine_Source": "McMillan Ch.3 + Position Trajectory: Strike Chase at 21-DTE",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True,
            })
        else:
            _regime_note_21 = ""
            if _pos_regime_21 in ('SIDEWAYS_INCOME', 'MEAN_REVERSION'):
                _regime_note_21 = (
                    f" {_pos_regime_21}: roll OUT at same strike for credit — "
                    "do NOT roll UP to a higher strike (debit). "
                    "Stock is range-bound; income cycle is working. "
                    "If assigned at current strike, that's a profitable exit "
                    "(McMillan Ch.3: same-strike credit rolls in range-bound regimes)."
                )
            result.update({
                "Action": "ROLL",
                "Urgency": _cc_21_urgency,
                "Rationale": (
                    f"21-DTE income gate: DTE={dte:.0f} ≤ 21 with only "
                    f"{pct_captured_21:.0%} profit captured (need ≥50%). "
                    f"Gamma-theta ratio has degraded — short call edge exhausted. "
                    f"Buy back current call and roll out to 30-45 DTE to reset cycle. "
                    f"(Given Ch.6: 21-DTE income roll; Passarelli Ch.2: theta/gamma degradation.)"
                    f"{_regime_note_21}"
                ),
                "Doctrine_Source": "Given Ch.6 + Passarelli Ch.2: 21-DTE Income Roll Gate",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True,
            })
        return result

    # ── 4. 50% premium capture with timing gate ────────────────────────
    premium_collected = abs(safe_row_float(row, 'Short_Call_Premium', 'Premium_Entry'))
    current_close_cost = abs(safe_row_float(row, 'Short_Call_Last', 'Last'))
    if premium_collected > 0 and current_close_cost <= premium_collected * PREMIUM_CAPTURE_TARGET and dte > DTE_INCOME_GATE:
        pct_captured = 1 - (current_close_cost / premium_collected)

        if _timing['action_mod'] == 'WAIT':
            result.update({
                "Action": "HOLD",
                "Urgency": "LOW",
                "Rationale": (
                    f"50% premium captured ({pct_captured:.0%}) but market timing unfavorable — "
                    f"{_timing['reason']} "
                    f"Hold and monitor; roll when market shows directional clarity "
                    f"(Passarelli Ch.6: 50% Rule + Roll Timing)."
                ),
                "Doctrine_Source": "Passarelli Ch.6: 50% Rule + Timing Gate",
                "Decision_State": STATE_NEUTRAL_CONFIDENT,
                "Required_Conditions_Met": True
            })
        else:
            urgency = _timing['urgency_mod'] if _timing['action_mod'] == 'ROLL_NOW' else "MEDIUM"
            timing_note = f" {_timing['reason']}" if _timing['reason'] else ""
            result.update({
                "Action": "ROLL",
                "Urgency": urgency,
                "Rationale": (
                    f"50% premium captured ({pct_captured:.0%} of ${premium_collected:.2f} entry credit) "
                    f"with {dte:.0f} DTE remaining — redeploy into next cycle.{timing_note} "
                    f"(Passarelli Ch.6: 50% Rule)."
                ),
                "Doctrine_Source": "Passarelli Ch.6: 50% Rule",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True,
                "Intraday_Advisory_JSON": (
                    json.dumps(_timing['intraday_advisory'])
                    if _timing.get('intraday_advisory') else ""
                ),
            })
        # Position Trajectory context for 50% profit gate
        _pos_regime_50 = str(row.get('Position_Regime', 'NEUTRAL') or 'NEUTRAL')
        _stock_ret_50 = safe_row_float(row, 'Trajectory_Stock_Return')
        if _pos_regime_50 in ('SIDEWAYS_INCOME', 'MEAN_REVERSION'):
            result['Rationale'] += (
                f" Position regime: {_pos_regime_50} — healthy cycle complete, roll to continue."
            )
        elif _pos_regime_50 == 'TRENDING_CHASE':
            result['Rationale'] += (
                f" ⚠️ TRENDING_CHASE: 50% captured but stock trending {_stock_ret_50:+.0%} since entry. "
                f"Next roll will likely face the same chase — consider accepting assignment or restructuring."
            )
        return result

    # ── 5. Negative carry ───────────────────────────────────────────────
    from core.shared.finance_utils import annualized_yield as _ann_yield
    dte_val = max(dte, 1)
    capital_at_risk = effective_cost if effective_cost > 0 else abs(float(spot or 0))
    premium = abs(safe_row_float(row, 'Short_Call_Premium', 'Premium_Entry'))
    if premium > 0 and capital_at_risk > 0:
        annualized_yield = _ann_yield(premium, capital_at_risk, dte_val)
        # Prefer pre-computed Daily_Margin_Cost (correct: borrowed portion only)
        _nc_pre_v1 = float(row.get('Daily_Margin_Cost') or 0.0)
        daily_margin_bleed = (_nc_pre_v1 / 100.0) if _nc_pre_v1 > 0 else (
            capital_at_risk * FIDELITY_MARGIN_RATE_DAILY
        )
        if annualized_yield < FIDELITY_MARGIN_RATE:
            cost_note = (
                f" (net cost ${effective_cost:.2f}/share after ${cum_premium:.2f} collected)"
                if cum_premium > 0 else ""
            )
            bleed_note = (
                f" Fidelity margin at 10.375%/yr costs ${daily_margin_bleed:.3f}/share/day "
                f"(${daily_margin_bleed * 100:.2f}/contract/day) — premium is not covering carry."
            )

            if _timing['action_mod'] == 'WAIT':
                _nc_urgency = "LOW"
                if dte < DTE_CUSHION_WINDOW and annualized_yield < YIELD_ESCALATION_THRESHOLD:
                    _nc_urgency = "MEDIUM"
                result.update({
                    "Action": "HOLD",
                    "Urgency": _nc_urgency,
                    "Rationale": (
                        f"Yield {annualized_yield:.1%} < Fidelity margin 10.375%{cost_note}.{bleed_note} "
                        f"Roll warranted but market timing unfavorable — "
                        f"{_timing['reason']} "
                        f"Monitor; re-evaluate when directional clarity appears "
                        f"(McMillan Ch.3: Yield Maintenance + Roll Timing)."
                        + (f" ⚠️ DTE={dte:.0f} short — carry deficit growing daily." if _nc_urgency == "MEDIUM" else "")
                    ),
                    "Doctrine_Source": "McMillan Ch.3: Yield Maintenance + Timing Gate",
                    "Decision_State": STATE_NEUTRAL_CONFIDENT,
                    "Required_Conditions_Met": True
                })
            else:
                urgency = _timing['urgency_mod'] if _timing['action_mod'] == 'ROLL_NOW' else "MEDIUM"
                timing_note = f" {_timing['reason']}" if _timing['reason'] else ""
                result.update({
                    "Action": "ROLL",
                    "Urgency": urgency,
                    "Rationale": (
                        f"Negative carry: yield {annualized_yield:.1%} < Fidelity margin 10.375%"
                        f"{cost_note}.{bleed_note}{timing_note} "
                        f"Roll to restore yield above carry cost (McMillan Ch.3: Yield Maintenance)."
                    ),
                    "Doctrine_Source": "McMillan Ch.3: Yield Maintenance",
                    "Intraday_Advisory_JSON": (
                        json.dumps(_timing['intraday_advisory'])
                        if _timing.get('intraday_advisory') else ""
                    ),
                    "Decision_State": STATE_ACTIONABLE,
                    "Required_Conditions_Met": True
                })
            return result

    # ── 5b. Debit Roll Efficiency / Cadence Switch ──────────────────────
    _has_debit_rolls = bool(row.get('Has_Debit_Rolls', False))
    _gross_prem = safe_row_float(row, 'Gross_Premium_Collected')
    _total_close_cost = safe_row_float(row, 'Total_Close_Cost')
    _is_emergency_zone = delta > DELTA_PRE_ITM_WARNING or dte < DTE_CUSHION_WINDOW

    if (_has_debit_rolls
            and _gross_prem > 0
            and not _is_emergency_zone
            and result.get('Action') not in ('EXIT',)):

        _buyback_ratio = _total_close_cost / _gross_prem

        if _buyback_ratio > 0.40:
            _cycle_yield_note = ""
            if premium > 0 and capital_at_risk > 0:
                _ann_yield = (premium / capital_at_risk) * (365 / max(dte, 1))
                _cycle_yield_note = f" Current call yield: {_ann_yield:.1%}/yr annualized."

            _net_collected = _gross_prem - _total_close_cost
            _debit_note = (
                f"Gross collected: ${_gross_prem:.2f}/share, "
                f"buyback costs: ${_total_close_cost:.2f}/share ({_buyback_ratio:.0%} of gross), "
                f"net kept: ${_net_collected:.2f}/share."
            )

            _exit_credit_note = (
                f" Current call has ${_extrinsic_pre:.2f} extrinsic — "
                f"buying back now at ${_call_last_pre:.2f} vs original ${premium_collected:.2f} entry."
                if _call_last_pre > 0 and premium_collected > 0 else ""
            )

            _far_otm = delta < DELTA_FAR_OTM
            _long_dte = dte > DTE_CADENCE_THRESHOLD
            _cadence_urgency = "MEDIUM" if (_far_otm and _long_dte) else "LOW"

            _cadence_note = (
                "Consider buying back this call and switching to monthly (30–45 DTE) cycles: "
                "near-dated calls have faster theta decay, allow tighter strikes near current price, "
                "and reduce the size of any future buyback if stock runs again."
                if _long_dte else
                "When rolling at expiry, consider tighter near-dated strikes rather than "
                "extending further OTM — the debit roll history suggests the far-OTM cadence "
                "is not capturing enough premium relative to buyback risk."
            )

            _prior_urgency_rank = {'LOW': 0, 'MEDIUM': 1, 'HIGH': 2, 'CRITICAL': 3}
            _cur_urgency = result.get('Urgency', 'LOW')
            if _prior_urgency_rank.get(_cadence_urgency, 0) > _prior_urgency_rank.get(_cur_urgency, 0):
                result['Urgency'] = _cadence_urgency

            _cadence_text = (
                f"  ⚠️ Cadence review: {_debit_note}{_exit_credit_note}{_cycle_yield_note} "
                f"{_cadence_note} "
                f"(Natenberg Ch.8: strike/cycle selection; McMillan Ch.3: net premium efficiency)"
            )
            result['Rationale'] = result.get('Rationale', '') + _cadence_text
            result['_cadence_note'] = _cadence_text

    # ── 6. Persistence escalation ───────────────────────────────────────
    if row.get('Drift_Persistence') == 'Sustained' and result['Action'] != 'HOLD':
        # Check if stock is still above net cost basis — if cushion remains,
        # escalate to HIGH (plan exit) not CRITICAL (act now).
        if drift_from_net > 0:
            # Stock above cost basis — negative carry but no capital loss yet
            result['Urgency'] = 'HIGH'
            result['Exit_Trigger_Type'] = 'INCOME'
            result['Rationale'] += (
                f" Sustained drift confirms trend, but stock still {drift_from_net:.1%} above net cost"
                f" — plan exit within days, not panic (Passarelli Ch.5)."
            )
        else:
            # DTE guard: OTM call with ample time doesn't warrant CRITICAL —
            # theta is still extracting, no assignment risk.  Cap at HIGH.
            # CRITICAL reserved for DTE ≤ 21 or ITM (delta ≥ 0.50) where
            # delay causes material harm (McMillan Ch.3; Natenberg Ch.8).
            if dte > 21 and delta < 0.50:
                result['Urgency'] = 'HIGH'
                result['Rationale'] += (
                    " Sustained drift confirms trend — plan roll within this cycle"
                    " (Passarelli Ch.5). DTE and delta not yet urgent."
                )
            else:
                result['Urgency'] = 'CRITICAL'
                result['Rationale'] += " Sustained drift confirms trend; immediate action required (Passarelli Ch.5)."
            result['Exit_Trigger_Type'] = 'CAPITAL'

    # ── 7. Thesis regime degradation ────────────────────────────────────
    thesis = check_thesis_degradation(row)
    if thesis:
        if result.get('Urgency', 'LOW') == 'LOW':
            result['Urgency'] = 'MEDIUM'
        result['Rationale'] += f" Entry regime degraded: {thesis['text']} (McMillan Ch.4: Thesis Persistence)."

    # ── 7b. Signal Hub: institutional signal context (collected, appended at end) ──
    _signal_hub_bw_notes = ""
    _weekly_bias_bw = str(row.get('Weekly_Trend_Bias', 'Unknown') or 'Unknown')
    if _weekly_bias_bw == 'CONFLICTING':
        _signal_hub_bw_notes += (
            " Weekly trend CONFLICTS with daily (Murphy 0.634): "
            "CC roll strike selection should favor defensive until weekly clarifies."
        )
    _squeeze_on_bw = bool(row.get('Keltner_Squeeze_On', False))
    if _squeeze_on_bw:
        _signal_hub_bw_notes += (
            " Keltner squeeze ON — volatility compressing. "
            "CC premium may be thin; consider waiting for squeeze release (Raschke/Murphy 0.739)."
        )

    # ── 8. IV term structure note ───────────────────────────────────────
    iv_shape = str(row.get('iv_surface_shape', '') or '').upper()
    if iv_shape == 'BACKWARDATION':
        slope = safe_row_float(row, 'iv_ts_slope_30_90')
        result['Rationale'] += (
            f" IV BACKWARDATED ({slope:+.1f}pt 30-90d): collecting elevated near-term IV "
            f"— premium receipt above normal; favorable short-vol entry (Natenberg Ch.11)."
        )

    # ── 9. Action EV Comparator ─────────────────────────────────────────
    try:
        _ev = compare_actions_bw(
            row,
            spot=spot,
            strike=strike,
            effective_cost=effective_cost,
            qty=abs(safe_row_float(row, 'Quantity', default=1.0)),
            dte=max(dte, 1),
        )
        result["Action_EV_Ranking"] = _ev["ranked_actions"]
        result["Action_EV_Winner"] = _ev["ev_winner"]
        result["Action_EV_Margin"] = _ev["ev_margin"]
        result["Action_EV_Hold"] = _ev["ev_hold"]
        result["Action_EV_Roll"] = _ev["ev_roll"]
        result["Action_EV_Assign"] = _ev["ev_assign"]
        result["Action_EV_Buyback"] = _ev["ev_buyback"]
        result["EV_Buyback_Trigger"] = _ev["ev_buyback_trigger"]
        result["Gamma_Drag_Daily"] = _ev["gamma_drag_daily"]

        _ev_winner = _ev["ev_winner"]
        _ev_margin = _ev["ev_margin"]
        _ev_summary = _ev["ev_summary"]
        _prior_action = result.get("Action", "HOLD")
        _thesis_ok = not result.get("_thesis_blocks_roll", False)
        _EV_NOISE_FLOOR = EV_NOISE_FLOOR_INCOME

        _ev_overrode = False

        if _thesis_ok and _ev_margin >= _EV_NOISE_FLOOR:

            if _ev_winner == "HOLD" and _prior_action != "HOLD":
                result.update({
                    "Action": "HOLD",
                    "Urgency": "LOW",
                    "Rationale": (
                        f"EV decision: HOLD dominates ROLL by ${_ev_margin:,.0f} "
                        f"over {dte:.0f}d horizon. "
                        f"Theta carry exceeds roll cost + new carry reset. "
                        f"Prior gate said ROLL ({_prior_action}) — overridden by EV. "
                        f"{_ev_summary}  "
                        f"(Passarelli Ch.6: roll only when it maximises holding-period return)"
                    ),
                    "Doctrine_Source": "ActionEV: HOLD > ROLL",
                    "Decision_State": STATE_NEUTRAL_CONFIDENT,
                    "Required_Conditions_Met": True,
                })
                _ev_overrode = True

            elif _ev_winner == "ROLL" and _prior_action == "HOLD":
                _iv_gap = pd.to_numeric(row.get('IV_vs_HV_Gap'), errors='coerce')
                _iv_pctile = pd.to_numeric(row.get('IV_Percentile'), errors='coerce')
                _timing_is_wait = _timing['action_mod'] == 'WAIT'
                _dte_urgent = dte <= 30

                _rc1_raw = row.get('Roll_Candidate_1')
                _has_live_chain = (
                    _rc1_raw is not None
                    and str(_rc1_raw) not in ('', 'nan', 'None')
                    and not (isinstance(_rc1_raw, float) and pd.isna(_rc1_raw))
                )

                _credit_soft_fail = (
                    not _dte_urgent
                    and pd.notna(_iv_gap)
                    and _iv_gap < 0
                    and pd.notna(_iv_pctile)
                    and _iv_pctile < 70
                )
                _roll_wait = (
                    (_timing_is_wait or _credit_soft_fail or not _has_live_chain)
                    and not _dte_urgent
                )

                if _roll_wait:
                    _wait_reasons = []
                    if _timing_is_wait:
                        _wait_reasons.append(f"market timing: {_timing['reason']}")
                    if _credit_soft_fail:
                        _wait_reasons.append(
                            f"IV below HV by {abs(_iv_gap):.1%} and at "
                            f"{_iv_pctile:.0f}th percentile of recent range — "
                            f"better credit likely if IV expands toward HV"
                        )
                    if not _has_live_chain:
                        _wait_reasons.append(
                            "no live chain data — EV credit estimate is model-only "
                            "(run pipeline during market hours to verify executable credit)"
                        )
                    result.update({
                        "Action": "ROLL_WAIT",
                        "Urgency": "LOW",
                        "Rationale": (
                            f"EV favors ROLL by ${_ev_margin:,.0f} over {dte:.0f}d "
                            f"but timing/credit gates not met: "
                            f"{'; '.join(_wait_reasons)}.  "
                            f"Structure is roll-ready — monitor for IV expansion or "
                            f"directional clarity before executing.  "
                            f"{_ev_summary}  "
                            f"(Passarelli Ch.6: roll when EV favors AND credit/timing align)"
                        ),
                        "Doctrine_Source": "ActionEV: ROLL > HOLD — WAIT (timing/credit)",
                        "Decision_State": STATE_NEUTRAL_CONFIDENT,
                        "Required_Conditions_Met": False,
                    })
                else:
                    result.update({
                        "Action": "ROLL",
                        "Urgency": "MEDIUM",
                        "Rationale": (
                            f"EV decision: ROLL dominates HOLD by ${_ev_margin:,.0f} "
                            f"over {dte:.0f}d horizon. "
                            f"Extrinsic credit exceeds carry reset cost; holding erodes more. "
                            f"Prior gate said HOLD — overridden by EV. "
                            f"{_ev_summary}  "
                            f"(Passarelli Ch.6: roll when EV of new cycle exceeds hold EV)"
                        ),
                        "Doctrine_Source": "ActionEV: ROLL > HOLD",
                        "Decision_State": STATE_ACTIONABLE,
                        "Required_Conditions_Met": True,
                    })
                _ev_overrode = True

            elif _ev_winner == "ASSIGN":
                _assign_profit = _ev["ev_assign"]
                _assign_label = "ACCEPT_CALL_AWAY" if delta >= 0.50 else "LET_EXPIRE"
                result.update({
                    "Action": _assign_label,
                    "Urgency": "LOW",
                    "Rationale": (
                        f"EV decision: {_assign_label} is optimal (+${_assign_profit:,.0f} certain proceeds). "
                        f"Strike ${strike:.2f} ≥ net cost ${effective_cost:.2f} — "
                        f"assignment locks in a profit. No roll needed; let expiry proceed. "
                        f"{_ev_summary}  "
                        f"(McMillan Ch.3: 'When assignment is profitable, rolling is optional — "
                        f"only roll if you want to defer the gain or capture more upside')"
                    ),
                    "Doctrine_Source": f"ActionEV: {_assign_label} optimal",
                    "Decision_State": STATE_NEUTRAL_CONFIDENT,
                    "Required_Conditions_Met": True,
                })
                _ev_overrode = True

            elif _ev_winner == "BUYBACK" and _ev["ev_buyback_trigger"]:
                result.update({
                    "Action": "ROLL",
                    "Urgency": "HIGH",
                    "Rationale": (
                        f"⚡ EV decision: BUYBACK dominates by ${_ev_margin:,.0f}. "
                        f"Gamma drag ${_ev['gamma_drag_daily']:.2f}/contract/day exceeding theta. "
                        f"Breakout confirmed (ADX rising, ROC expanding). IV cheap to close. "
                        f"Buy back short call NOW to capture uncapped stock upside. "
                        f"{_ev_summary}  "
                        f"(Passarelli Ch.6: close short premium when breakout confirmed; Natenberg Ch.7: gamma/theta ratio)"
                    ),
                    "Doctrine_Source": "ActionEV: BUYBACK — gamma breakout",
                    "Decision_State": STATE_ACTIONABLE,
                    "Required_Conditions_Met": True,
                })
                _ev_overrode = True

        if not _ev_overrode:
            result["Rationale"] += f"  ·  {_ev_summary}"

        # Re-append cadence note if EV override replaced rationale
        _cadence_note_saved = result.pop('_cadence_note', None)
        if _cadence_note_saved and _cadence_note_saved not in result.get('Rationale', ''):
            result['Rationale'] += _cadence_note_saved

    except Exception as _ev_err:
        logger.debug(f"[DoctrineEngine] Action EV comparator skipped: {_ev_err}")

    # Final cadence note recovery
    _cadence_note_saved = result.pop('_cadence_note', None)
    if _cadence_note_saved and _cadence_note_saved not in result.get('Rationale', ''):
        result['Rationale'] += _cadence_note_saved

    # Signal Hub annotations (appended after all gates to avoid overwrite)
    if _signal_hub_bw_notes:
        result['Rationale'] = result.get('Rationale', '') + _signal_hub_bw_notes

    return result


# ────────────────────────────────────────────────────────────────────────────
# buy_write_doctrine_v2 — proposal-based (parallel to covered_call_doctrine_v2)
# ────────────────────────────────────────────────────────────────────────────

def buy_write_doctrine_v2(row: pd.Series, result: Dict) -> Dict:
    """Proposal-based BUY_WRITE evaluation.

    All gates propose actions into a ProposalCollector instead of returning
    immediately. A resolver picks the best action using deterministic EV
    and MC evidence.

    Original ``buy_write_doctrine()`` is preserved unchanged for A/B testing.
    """
    collector = ProposalCollector()

    # ── Extract fields (same as v1) ───────────────────────────────────────
    spot = safe_row_float(row, 'UL Last')
    delta = abs(safe_row_float(row, 'Short_Call_Delta', 'Delta'))
    # NaN-safe DTE: NaN is truthy → `NaN or 999` returns NaN, not 999.
    _dte_raw = row.get('Short_Call_DTE')
    if not pd.notna(_dte_raw):
        _dte_raw = row.get('DTE')
    if not pd.notna(_dte_raw):
        _dte_raw = 999
    dte = float(_dte_raw)

    # ── Net cost basis ────────────────────────────────────────────────────
    from core.shared.finance_utils import effective_cost_per_share as _ecp
    cum_premium = safe_row_float(row, 'Cumulative_Premium_Collected')
    strike = safe_row_float(row, 'Short_Call_Strike', 'Strike')

    effective_cost, broker_cost_per_share, _cost_tier = _ecp(row, spot_fallback=spot)

    drift_from_net = (spot - effective_cost) / effective_cost if effective_cost > 0 else 0.0
    stock_basis_raw = broker_cost_per_share
    total_loss_dollars = (spot - stock_basis_raw) * abs(safe_row_float(row, 'Quantity')) if stock_basis_raw > 0 else 0.0

    # ── 1. Hard Stop ──────────────────────────────────────────────────────
    # Detect recovery state: position deeply underwater but income repair viable?
    _recovery = detect_recovery_state(row, spot=spot, effective_cost=effective_cost)
    _recovery_mode = _recovery["is_recovery"]

    # Moderate recovery: catch -10% to -25% drawdowns early, before they
    # become deep recovery emergencies.  Enables roll-down proposals that
    # compete on EV instead of being overridden by CAPITAL EXIT.
    _moderate = detect_moderate_recovery_state(row, spot=spot, effective_cost=effective_cost)
    _moderate_recovery = _moderate["is_moderate_recovery"]

    # Income path active: position has established an income repair strategy
    # (2+ cycles, premium collected, thesis not dead).  When this is True,
    # equity_broken EXIT gates lose their CAPITAL auto-win and must compete
    # on EV — preventing forced exits on recoverable positions like DKNG
    # at -3.46% with 7 cycles of premium and INTACT thesis.
    _cycle_count = int(row.get('_cycle_count', 1) or 1)
    _thesis_for_income = str(row.get('Thesis_State', '') or '').upper()
    # IV viability: need IV above 15% to generate meaningful premium
    from core.shared.finance_utils import normalize_iv as _normalize_iv
    _iv_for_income = _normalize_iv(safe_row_float(row, 'IV_Now', 'IV_30D')) or 0.0
    _income_path_active = (
        cum_premium > 0
        and _cycle_count >= 2
        and _thesis_for_income != 'BROKEN'
        and _iv_for_income > 0.15
    )

    # ── 0. New-position grace period ───────────────────────────────────
    # Scan engine just recommended this BUY_WRITE — don't ROLL/EXIT a call
    # you sold today.  Hard veto so EV scoring can't override.
    # Exception: catastrophic gap move (>-25% on day 0).
    _bw_days_in_trade = safe_row_float(row, 'Days_In_Trade')
    if _bw_days_in_trade < 2 and drift_from_net > -0.25:
        propose_gate(
            collector, "new_position_grace",
            action="HOLD", urgency="LOW",
            is_hard_veto=True,
            rationale=(
                f"New position grace: BUY_WRITE opened {_bw_days_in_trade:.0f}d ago. "
                f"Scan engine approved this trade — allow at least 1 full "
                f"trading day before considering ROLL or EXIT. "
                f"Current drift {drift_from_net:+.1%} from net cost "
                f"${effective_cost:.2f} (McMillan Ch.4: give new positions "
                f"time to develop)."
            ),
            doctrine_source="McMillan Ch.4: New Position Grace Period",
            priority=1,
            exit_trigger_type="",
        )
        # Hard veto — skip all other gates and go straight to resolution
        return collector.to_result(collector.get_veto(), result, "HARD_VETO")

    # ── 0b. Consecutive debit roll hard stop ────────────────────────────
    _r0b_bw = result.copy()
    _fired_0b_bw, _r0b_bw = gate_consecutive_debit_roll_stop(
        row=row, result=_r0b_bw, strategy_label="BW",
    )
    if _fired_0b_bw:
        propose_gate(
            collector, "consecutive_debit_roll_stop",
            action=_r0b_bw.get("Action", "EXIT"),
            urgency=_r0b_bw.get("Urgency", "HIGH"),
            rationale=_r0b_bw.get("Rationale", ""),
            doctrine_source=_r0b_bw.get("Doctrine_Source", ""),
            priority=2, is_hard_veto=True,
            exit_trigger_type="INCOME",
        )

    # ── 0c. Fading winner trailing protection ─────────────────────────
    from core.management.cycle3.doctrine.helpers import safe_pnl_pct
    _bw_pnl = safe_pnl_pct(row)
    _r0c_bw = result.copy()
    _fired_0c_bw, _r0c_bw = gate_fading_winner(
        row=row, pnl_pct=_bw_pnl if _bw_pnl is not None else 0.0,
        result=_r0c_bw, strategy_label="BW",
    )
    if _fired_0c_bw:
        propose_gate(
            collector, "fading_winner",
            action=_r0c_bw.get("Action", "EXIT"),
            urgency=_r0c_bw.get("Urgency", "MEDIUM"),
            rationale=_r0c_bw.get("Rationale", ""),
            doctrine_source=_r0c_bw.get("Doctrine_Source", ""),
            priority=3,
            exit_trigger_type="INCOME",
        )

    # ── 0d. Income TRIM — wave-phase partial close ──────────────────────
    from core.management.cycle3.doctrine.shared_income_gates import gate_income_trim
    _wave_bw = str(row.get('WavePhase_State', '') or '').upper()
    _conv_bw = str(row.get('Conviction_Status', '') or '').upper()
    # Normalize to contract units (Base_Quantity frozen at entry, fallback Quantity)
    _qty_bw = abs(int(float(
        row.get('Base_Quantity') or row.get('Entry_Quantity') or row.get('Quantity', 1)
    )))
    _r0d_bw = result.copy()
    _fired_0d_bw, _r0d_bw = gate_income_trim(
        row=row, pnl_pct=_bw_pnl if _bw_pnl is not None else 0.0,
        wave_phase=_wave_bw, conviction_status=_conv_bw,
        quantity=_qty_bw, result=_r0d_bw, strategy_label="BW",
    )
    if _fired_0d_bw:
        propose_gate(
            collector, "income_trim",
            action=_r0d_bw.get("Action", "TRIM"),
            urgency=_r0d_bw.get("Urgency", "MEDIUM"),
            rationale=_r0d_bw.get("Rationale", ""),
            doctrine_source=_r0d_bw.get("Doctrine_Source", ""),
            priority=4,
            exit_trigger_type="INCOME",
            Trim_Contracts=_r0d_bw.get("Trim_Contracts", 0),
            Trim_Pct=_r0d_bw.get("Trim_Pct", 0.0),
        )

    if effective_cost > 0 and drift_from_net <= PNL_HARD_STOP_BW:
        if _recovery_mode:
            # ── Recovery Mode: shift from "exit" to "optimize repair" ────
            # Hard stop is acknowledged but NOT a hard veto — recovery HOLD
            # competes via EV/proposal resolution. The position's economic
            # reality is: exit locks in permanent loss; premium collection
            # is the rational repair path (Jabbour Ch.4, McMillan Ch.3).
            _rc = _recovery["context"]
            _rc_months = _rc.get("months_to_breakeven", float('inf'))
            _rc_net_mo = _rc.get("net_monthly", 0)
            _rc_cycles = _rc.get("cycles_completed", 1)
            _rc_prem = _rc.get("premium_collected_per_share", 0)

            _rc_months_str = (
                f" Net income ~${_rc_net_mo:.2f}/mo → ~{_rc_months:.0f} months to close gap."
                if _rc_net_mo > 0 and _rc_months < 999
                else ""
            )

            # Recovery catalyst context
            _rc_catalyst = ""
            _rc_days_earn = _rc.get("days_to_earnings", 999)
            _rc_beat = _rc.get("earnings_beat_rate", 0)
            _rc_crush = _rc.get("earnings_crush_pct", 0)
            _rc_move_ratio = _rc.get("earnings_move_ratio", 0)
            _rc_days_macro = _rc.get("days_to_macro", 999)
            _rc_macro_event = _rc.get("macro_event", "")
            _rc_basing = _rc.get("stock_basing", False)

            _catalyst_parts = []
            if _rc_days_earn < 60:
                _earn_note = f"Earnings in {_rc_days_earn:.0f}d"
                if _rc_beat > 0:
                    _earn_note += f" (beat rate {_rc_beat:.0%}"
                    if _rc_crush > 0:
                        _earn_note += f", avg crush {_rc_crush:.0f}%"
                    if _rc_move_ratio > 0:
                        _earn_note += f", move ratio {_rc_move_ratio:.1f}×"
                    _earn_note += ")"
                _catalyst_parts.append(_earn_note)
            if _rc_days_macro < 10 and _rc_macro_event:
                _catalyst_parts.append(f"{_rc_macro_event} in {_rc_days_macro:.0f}d")
            if _rc_basing:
                _catalyst_parts.append(f"stock basing (ADX {_rc.get('adx', 0):.0f})")

            if _catalyst_parts:
                _rc_catalyst = f" Next catalyst: {'; '.join(_catalyst_parts)}."

            propose_gate(
                collector, "hard_stop_recovery_ladder",
                action="HOLD", urgency="MEDIUM",
                rationale=(
                    f"Recovery ladder active: hard stop breached (${spot:.2f} vs "
                    f"net cost ${effective_cost:.2f}, {drift_from_net:.1%}) but "
                    f"{_rc_cycles} cycles of premium collection have reduced basis "
                    f"from ${stock_basis_raw:.2f} → ${effective_cost:.2f} "
                    f"(${_rc_prem:.2f}/sh collected).{_rc_months_str} "
                    f"Hold short call to expiration. At expiry, reassess stock "
                    f"thesis before opening next cycle."
                    f"{_rc_catalyst} "
                    f"⚠️ Stock leg remains at risk — evaluate separately whether "
                    f"to continue holding equity "
                    f"(Jabbour Ch.4: Repair Strategies; McMillan Ch.3: Basis Reduction)."
                ),
                doctrine_source="Jabbour Ch.4 / McMillan Ch.3: Recovery Ladder",
                priority=2,
            )
            # No soft EXIT proposed — recovery guardrails already validated
            # that income repair is viable. Underlying collapse (separate gate)
            # remains a hard veto for genuine structural emergencies.
        else:
            # ── Prevention Mode: standard hard stop EXIT (hard veto) ─────
            _exit_recovery_reason = _recovery.get("exit_recovery", "")
            if cum_premium > 0:
                cushion_note = (
                    f" ${cum_premium:.2f}/share collected across {int(row.get('_cycle_count', 1))} cycles"
                    f" reduced basis from ${stock_basis_raw:.2f} to ${effective_cost:.2f}."
                )
            else:
                cushion_note = " No premium history recorded — run backfill if cycles are missing."

            total_loss_note = (
                f" Combined P&L ≈ ${total_loss_dollars:+,.0f}."
                if total_loss_dollars != 0 else ""
            )

            _cost_provenance = ""
            _hard_stop_urgency = "CRITICAL"
            if _cost_tier == 3 and cum_premium == 0:
                _cost_provenance = (
                    f" ⚠️ UNVERIFIED COST BASIS: ${effective_cost:.2f} is the spot price "
                    f"when the position was first observed — NOT confirmed purchase price. "
                    f"Verify broker Basis before acting."
                )
                _hard_stop_urgency = "HIGH"

            _recovery_guardrail_note = (
                f" Recovery not viable: {_exit_recovery_reason}"
                if _exit_recovery_reason else ""
            )

            propose_gate(
                collector, "hard_stop_exit",
                action="EXIT", urgency=_hard_stop_urgency,
                rationale=(
                    f"Hard stop breached: stock at ${spot:.2f} vs net cost ${effective_cost:.2f}/share "
                    f"({drift_from_net:.1%}).{cushion_note}{total_loss_note}"
                    f"{_cost_provenance}{_recovery_guardrail_note} "
                    f"Continue rolling only if thesis intact — otherwise exit stock + buy back call "
                    f"(McMillan Ch.3: Hard Stop)."
                ),
                doctrine_source="McMillan Ch.3: Hard Stop",
                priority=1, is_hard_veto=True, exit_trigger_type="CAPITAL",
            )

    # ── 1a. Approaching hard stop ─────────────────────────────────────────
    if effective_cost > 0 and PNL_HARD_STOP_BW < drift_from_net <= PNL_APPROACHING_HARD_STOP:
        hard_stop_price = effective_cost * (1 + PNL_HARD_STOP_BW)
        gap_to_stop = spot - hard_stop_price
        premium_note = f" ${cum_premium:.2f}/share collected to date." if cum_premium > 0 else ""

        _thesis_hs = str(row.get('Thesis_State', '') or '').upper()
        _gap_pct = gap_to_stop / spot if spot and spot > 0 else 1.0
        if _gap_pct < 0.03 and _thesis_hs in ('DEGRADED', 'BROKEN'):
            # Income path active: EXIT competes on EV instead of
            # auto-winning via CAPITAL.  The income path may still beat exit.
            # BROKEN thesis always keeps CAPITAL (already excluded by
            # _income_path_active check).
            _hs_exit_trigger = "" if _income_path_active else "CAPITAL"

            propose_gate(
                collector, "approaching_hard_stop_exit",
                action="EXIT", urgency="CRITICAL",
                rationale=(
                    f"Hard stop imminent: ${spot:.2f} vs hard stop ${hard_stop_price:.2f} "
                    f"({_gap_pct:.1%} cushion). Thesis is {_thesis_hs} — rolling into further "
                    f"debit would compound a failed thesis. Exit stock + buy back call now "
                    f"(McMillan Ch.3: Hard Stop — thesis degraded, exit before breach)."
                ),
                doctrine_source="McMillan Ch.3: Hard Stop — thesis degraded, exit before breach",
                priority=3, exit_trigger_type=_hs_exit_trigger,
            )
        else:
            propose_gate(
                collector, "approaching_hard_stop_roll",
                action="ROLL", urgency="HIGH",
                rationale=(
                    f"Approaching hard stop: ${spot:.2f} vs net cost ${effective_cost:.2f} "
                    f"({drift_from_net:.1%}).{premium_note} "
                    f"Hard stop at ${hard_stop_price:.2f} — only ${gap_to_stop:.2f}/share cushion. "
                    f"Roll call down aggressively to collect more premium and lower net cost "
                    f"(McMillan Ch.3: Basis Reduction Under Pressure)."
                ),
                doctrine_source="McMillan Ch.3: Basis Reduction Under Pressure",
                priority=5,
            )

        # ── 1a-TRIM. Defensive trim at approaching stop ────────────────
        # For multi-contract positions: reduce exposure by 30% while
        # continuing to manage the remaining core position.
        # Sinclair Ch.7: "Reduce size as edge degrades" (fractional Kelly).
        # Chan Ch.4: "Partial exits preserve optionality."
        # Competes with ROLL via EV — resolver picks best action.
        from core.management.cycle3.doctrine.thresholds import DEFENSIVE_TRIM_PCT, DEFENSIVE_TRIM_MIN_QUANTITY
        if _qty_bw >= DEFENSIVE_TRIM_MIN_QUANTITY:
            _def_trim_n = max(1, round(_qty_bw * DEFENSIVE_TRIM_PCT))
            _def_trim_n = min(_def_trim_n, _qty_bw - 1)  # never trim to 0
            propose_gate(
                collector, "defensive_trim_approaching_stop",
                action="TRIM", urgency="HIGH",
                rationale=(
                    f"Defensive trim: approaching hard stop ({drift_from_net:.1%}) "
                    f"with {_qty_bw} contracts. Trim {_def_trim_n} contracts "
                    f"({DEFENSIVE_TRIM_PCT:.0%}) to reduce exposure while preserving "
                    f"recovery optionality on remaining {_qty_bw - _def_trim_n}. "
                    f"Hard stop at ${hard_stop_price:.2f} — ${gap_to_stop:.2f}/share cushion. "
                    f"(Sinclair Ch.7: fractional Kelly — reduce size as edge degrades; "
                    f"Chan Ch.4: partial exits on momentum strategies)"
                ),
                doctrine_source="Sinclair Ch.7 + Chan Ch.4: Defensive Trim",
                priority=5,
                exit_trigger_type="CAPITAL",
                Trim_Contracts=_def_trim_n,
                Trim_Pct=DEFENSIVE_TRIM_PCT,
            )

    # ── 1b-MOD. Moderate Recovery Roll-Down ─────────────────────────────
    # For positions at -10% to -25% with active income strategy: propose
    # rolling call down to capture more premium and reduce cost basis.
    # This gate fires regardless of approaching_hard_stop — it bridges
    # the gap for positions at -10% to -15% that don't yet trigger the
    # approaching_hard_stop section.
    if _moderate_recovery:
        _mr = _moderate["context"]
        _mr_months = _mr.get("months_to_breakeven", float('inf'))
        _mr_net_mo = _mr.get("net_monthly", 0)
        _mr_gap = _mr.get("gap_to_breakeven", 0)
        _mr_prem = _mr.get("premium_collected_per_share", 0)
        _mr_loss = _mr.get("loss_pct", 0)
        _mr_cycles = _mr.get("cycles_completed", 1)
        _mr_iv = _mr.get("iv_now", 0)
        _mr_thesis = _mr.get("thesis", "UNKNOWN")

        _mr_months_str = (
            f" At current rate ~${_mr_net_mo:.2f}/mo, ~{_mr_months:.0f} months to close gap."
            if _mr_net_mo > 0 and _mr_months < 999
            else ""
        )

        _mr_urgency = "HIGH" if _mr_loss <= -0.15 else "MEDIUM"

        propose_gate(
            collector, "moderate_recovery_roll_down",
            action="ROLL", urgency=_mr_urgency,
            rationale=(
                f"Moderate recovery: stock at ${spot:.2f} vs net cost "
                f"${effective_cost:.2f} ({_mr_loss:.1%}). "
                f"{_mr_cycles} cycle(s) collected ${_mr_prem:.2f}/share so far. "
                f"Gap to breakeven ${_mr_gap:.2f}/share.{_mr_months_str} "
                f"IV at {_mr_iv:.0%} (rank {_mr.get('iv_rank', 0):.0f}) — "
                f"roll call down aggressively to capture more premium and "
                f"reduce cost basis before drawdown deepens. "
                f"Thesis {_mr_thesis} — income repair viable. "
                f"(McMillan Ch.3: reduce basis early; "
                f"Jabbour Ch.4: repair when damage is manageable)"
            ),
            doctrine_source="McMillan Ch.3: Early Basis Reduction + Jabbour Ch.4: Manageable Repair",
            priority=8,
        )

    # ── Recently-Rolled Cooldown gate ─────────────────────────────────────
    _COOLDOWN_DAYS_BW = 3
    _days_since_roll = row.get('Days_Since_Last_Roll')
    _thesis_for_cooldown = str(row.get('Thesis_State', '') or '').upper()
    if (
        pd.notna(_days_since_roll)
        and float(_days_since_roll) < _COOLDOWN_DAYS_BW
        and _thesis_for_cooldown in ('INTACT', 'UNKNOWN', '')
    ):
        propose_gate(
            collector, "roll_cooldown",
            action="HOLD", urgency="LOW",
            rationale=(
                f"Recently-rolled cooldown: current leg opened {int(_days_since_roll)}d ago "
                f"(< {_COOLDOWN_DAYS_BW}d window). Thesis is {_thesis_for_cooldown or 'UNKNOWN'} — "
                f"suppressing discretionary ROLL to prevent self-contradicting roll chains. "
                f"Natenberg Ch.7: 'Frequent adjustments cost more than the risk they mitigate.' "
                f"Jabbour Ch.8: 'Repair is a dangerous misnomer for overtrading.'"
            ),
            doctrine_source="Natenberg Ch.7 + Jabbour Ch.8: Recently-Rolled Cooldown",
            priority=25,
        )

    # ── 1b. Earnings Event Risk ───────────────────────────────────────────
    earnings_date = row.get('Earnings_Date')
    _is_leaps_dte = dte > DTE_LEAPS_THRESHOLD
    _pct_to_strike = abs(spot - strike) / strike if strike > 0 and spot > 0 else 1.0
    _near_strike = _pct_to_strike <= STRIKE_PROXIMITY_EARNINGS

    # Track whether thesis blocks ROLL proposals
    _thesis_blocks_roll = bool(result.get('_thesis_blocks_roll', False))

    if earnings_date is not None and not _is_leaps_dte and _near_strike:
        try:
            snap_ts = pd.to_datetime(row.get('Snapshot_TS') or pd.Timestamp.now())
            earn_dt = pd.to_datetime(earnings_date)
            days_to_earnings = (earn_dt.normalize() - snap_ts.normalize()).days
            if 0 <= days_to_earnings <= max(int(dte), 7):
                _earn_urgency = "CRITICAL" if (drift_from_net <= -0.10 and days_to_earnings <= 5) else "HIGH"
                _loss_note = (
                    f" Position already at {drift_from_net:.1%} from net cost — "
                    f"earnings gap could breach hard stop in one session."
                    if drift_from_net < PNL_DRIFT_STRUCTURE_BROKEN else ""
                )
                _premium_note = f" ${cum_premium:.2f}/share collected to date." if cum_premium > 0 else ""
                _hist_note = ""
                _track_q = row.get('Earnings_Track_Quarters')
                if _track_q and pd.notna(_track_q) and int(_track_q) > 0:
                    _crush = row.get('Earnings_Avg_IV_Crush_Pct')
                    _gap = row.get('Earnings_Avg_Gap_Pct')
                    _ratio = row.get('Earnings_Avg_Move_Ratio')
                    _parts = []
                    if pd.notna(_crush):
                        _parts.append(f"avg IV crush {_crush*100:.0f}%")
                    if pd.notna(_gap):
                        _parts.append(f"avg |gap| {_gap*100:.1f}%")
                    if pd.notna(_ratio):
                        _label = "underpriced" if _ratio > 1.0 else "overpriced"
                        _parts.append(f"move ratio {_ratio:.2f} ({_label})")
                    if _parts:
                        _hist_note = f" Historical ({int(_track_q)}Q): {', '.join(_parts)}."
                propose_gate(
                    collector, "earnings_event_roll",
                    action="ROLL", urgency=_earn_urgency,
                    rationale=(
                        f"Earnings in {days_to_earnings}d (within {dte:.0f}d DTE window).{_loss_note}"
                        f"{_premium_note}{_hist_note} Roll call before event: move to post-earnings expiry and "
                        f"consider wider strike to absorb gap risk "
                        f"(Natenberg Ch.12: Event Gap Risk — delta cannot protect against discontinuous moves)."
                    ),
                    doctrine_source="Natenberg Ch.12: Earnings Event Risk",
                    priority=10,
                )
            elif -7 <= days_to_earnings < 0:
                price_change = drift_from_net
                if price_change < PNL_POST_EARNINGS_DROP:
                    _post_hist = ""
                    _track_q = row.get('Earnings_Track_Quarters')
                    if _track_q and pd.notna(_track_q) and int(_track_q) > 0:
                        _beat = row.get('Earnings_Beat_Rate')
                        _drift = row.get('Earnings_Last_Surprise_Pct')
                        _parts = []
                        if pd.notna(_beat):
                            _parts.append(f"beat rate {_beat*100:.0f}%")
                        if pd.notna(_drift):
                            _parts.append(f"last surprise {_drift:.1f}%")
                        if _parts:
                            _post_hist = f" Track record ({int(_track_q)}Q): {', '.join(_parts)}."
                    propose_gate(
                        collector, "post_earnings_drop_hold",
                        action="HOLD", urgency="MEDIUM",
                        rationale=(
                            f"Earnings passed {abs(days_to_earnings)}d ago. Post-earnings drop of "
                            f"{price_change:.1%} from net cost.{_post_hist} Assess: was this guidance-driven "
                            f"(structural — consider exit) or beat/miss reaction "
                            f"(temporary — continue collecting). Check HV normalization: "
                            f"if IV crushes back, thesis may be intact "
                            f"(Natenberg Ch.12: Post-Event Assessment)."
                        ),
                        doctrine_source="Natenberg Ch.12: Post-Event Assessment",
                        priority=12,
                    )
        except Exception as _bw_post_earn_err:
            logger.debug(f"BW v2 post-earnings assessment skipped: {_bw_post_earn_err}")

    # ── 1b-GAP2: Earnings Lockdown ────────────────────────────────────────
    # Only CAPITAL-tag near-strike positions where gap risk is real.
    # Far-OTM calls (>STRIKE_PROXIMITY_EARNINGS away) compete on EV instead.
    if earnings_date is not None and not _is_leaps_dte:
        try:
            snap_ts_lk = pd.to_datetime(row.get('Snapshot_TS') or pd.Timestamp.now())
            earn_dt_lk = pd.to_datetime(earnings_date)
            days_to_earn_lk = (earn_dt_lk.normalize() - snap_ts_lk.normalize()).days
            if 0 <= days_to_earn_lk <= 2:
                _lockdown_trigger = "CAPITAL" if _near_strike else ""
                propose_gate(
                    collector, "earnings_lockdown_exit",
                    action="EXIT", urgency="HIGH",
                    rationale=(
                        f"Earnings lockdown: earnings in {days_to_earn_lk}d. "
                        f"Rolling a short call into a binary event extends assignment exposure — "
                        f"gap risk cannot be managed by delta. "
                        f"Exit the short call before announcement. "
                        f"(Jabbour Ch.8: never roll into earnings; Given Ch.4: close before event.)"
                    ),
                    doctrine_source="Jabbour Ch.8 + Given Ch.4: Earnings Lockdown",
                    priority=8, exit_trigger_type=_lockdown_trigger,
                )
        except Exception as _bw_lockdown_err:
            logger.debug(f"BW v2 earnings lockdown gate skipped: {_bw_lockdown_err}")

    # ── 1b-LEAPS: Earnings routine note (annotation only) ─────────────────
    _leaps_earnings_note = ""
    if earnings_date is not None and _is_leaps_dte:
        try:
            snap_ts = pd.to_datetime(row.get('Snapshot_TS') or pd.Timestamp.now())
            earn_dt = pd.to_datetime(earnings_date)
            days_to_earnings = (earn_dt.normalize() - snap_ts.normalize()).days
            if 0 <= days_to_earnings <= EARNINGS_NOTE_WINDOW:
                _leaps_earnings_note = (
                    f" [Earnings in {days_to_earnings}d — LEAPS call at "
                    f"${strike:.0f} is ${abs(spot-strike):.0f} OTM ({_pct_to_strike:.0%} from strike). "
                    f"No roll needed; event risk is priced into long-dated IV. "
                    f"Monitor if stock gaps toward strike post-earnings.]"
                )
        except Exception as _bw_leaps_err:
            logger.debug(f"BW v2 LEAPS earnings note skipped: {_bw_leaps_err}")

    # ── 1c. Underlying Health Story Check ─────────────────────────────────
    _price_struct = str(row.get('PriceStructure_State', '') or '').upper()
    _trend_integ = str(row.get('TrendIntegrity_State', '') or '').upper()
    _ema50_slope = safe_row_float(row, 'ema50_slope')
    _hv_percentile = safe_row_float(row, 'hv_20d_percentile', default=50.0)

    _underlying_health_annotation = ""
    if 'STRUCTURE_BROKEN' in _price_struct and _trend_integ in ('NO_TREND', 'TREND_EXHAUSTED'):
        if effective_cost > 0 and drift_from_net < PNL_DRIFT_STRUCTURE_BROKEN:
            # CAPITAL only when income path is NOT active and drift is near hard stop.
            # Active income positions compete on EV — premium collection may justify
            # continuing despite structural breakdown (same principle as equity_broken).
            _ub_capital_danger = (
                not _income_path_active
                and drift_from_net <= PNL_APPROACHING_HARD_STOP
            )
            _ub_exit_trigger = "CAPITAL" if _ub_capital_danger else ""
            propose_gate(
                collector, "underlying_broken_exit",
                action="EXIT", urgency="HIGH",
                rationale=(
                    f"Underlying story BROKEN: Price structure BROKEN + trend exhausted. "
                    f"This is not a timing problem — the underlying thesis is structurally invalid. "
                    f"Stock at ${spot:.2f} vs net cost ${effective_cost:.2f} ({drift_from_net:.1%}). "
                    f"${cum_premium:.2f}/share collected, but continued rolling locks in deeper losses. "
                    f"Exit stock + buy back call. Do not roll a broken thesis "
                    f"(Passarelli Ch.2: Story Check — thesis must be intact to continue managing)."
                ),
                doctrine_source="Passarelli Ch.2: Story Check",
                priority=6, exit_trigger_type=_ub_exit_trigger,
            )
        else:
            _underlying_health_annotation = (
                f"⚠️ Story Check: Price structure BROKEN + trend exhausted. "
                f"This is not a timing problem — the underlying thesis is structurally invalid. "
                f"Verify thesis before next roll (Passarelli Ch.2). "
            )
    elif _ema50_slope < -0.02 and _hv_percentile > 70 and drift_from_net < PNL_DRIFT_STRUCTURE_BROKEN:
        _underlying_health_annotation = (
            f"⚠️ Underlying weakening: "
            f"EMA50 slope={_ema50_slope:.3f} (declining) + HV at {_hv_percentile:.0f}th percentile "
            f"(elevated fear). Underlying trending against position under high volatility. "
        )

    # ── 2. ITM Defense (BW-specific with cost-basis context) ──────────────
    if delta > DELTA_ITM_EMERGENCY:
        if strike > 0 and effective_cost > 0 and strike < effective_cost:
            loss_per_share = effective_cost - strike
            assignment_outcome = (
                f"Assignment at ${strike:.2f} BELOW net cost ${effective_cost:.2f} "
                f"(−${loss_per_share:.2f}/share loss despite ${cum_premium:.2f}/share collected). "
                f"Roll up to strike above ${effective_cost:.2f} to preserve breakeven."
            )
            urgency = "CRITICAL"
        elif strike > 0 and effective_cost > 0 and strike >= effective_cost:
            profit_per_share = strike - effective_cost
            assignment_outcome = (
                f"Assignment at ${strike:.2f} above net cost ${effective_cost:.2f} "
                f"(+${profit_per_share:.2f}/share profit including ${cum_premium:.2f}/share collected). "
                f"Roll up/out to capture more upside, or allow assignment."
            )
            urgency = "HIGH"
        else:
            assignment_outcome = f"Assignment risk — roll up/out to maintain income."
            urgency = "HIGH"

        if not _thesis_blocks_roll:
            propose_gate(
                collector, "itm_defense_roll",
                action="ROLL", urgency=urgency,
                rationale=(
                    f"Call deep ITM (Delta={delta:.2f} > 0.70). {assignment_outcome} "
                    f"(McMillan Ch.3: ITM Defense)."
                ),
                doctrine_source="McMillan Ch.3: ITM Defense",
                priority=15,
            )

    # ── 3. Expiration proximity: DTE < 7 ──────────────────────────────────
    if dte < DTE_EMERGENCY_ROLL:
        if not _thesis_blocks_roll:
            propose_gate(
                collector, "expiration_proximity_roll",
                action="ROLL", urgency="HIGH",
                rationale=(
                    f"DTE={dte:.0f} < 7 — pin risk and gamma acceleration. "
                    f"Roll to next cycle (McMillan Ch.3: Expiration Management)."
                ),
                doctrine_source="McMillan Ch.3: Expiration Management",
                priority=18,
            )

    # ── Pre-ITM Drift Warning: 0.55 < delta ≤ 0.70 ───────────────────────
    _call_last_pre = abs(safe_row_float(row, 'Short_Call_Last', 'Last'))
    _intrinsic_pre = max(0.0, spot - strike) if strike > 0 else 0.0
    _extrinsic_pre = max(0.0, _call_last_pre - _intrinsic_pre) if _call_last_pre > 0 else 0.0
    _extrinsic_pct = (_extrinsic_pre / _call_last_pre) if _call_last_pre > 0 else 0.0
    _strike_prox = ((spot - strike) / strike) if strike > 0 else 0.0

    _pre_itm_delta_warn = (DELTA_PRE_ITM_WARNING < delta <= DELTA_ITM_EMERGENCY)
    _pre_itm_strike_prox = (strike > 0 and 0 <= _strike_prox <= STRIKE_PROXIMITY_NARROW)
    _credit_still_viable = (_extrinsic_pct >= EXTRINSIC_CREDIT_VIABLE)
    _credit_strong = (_extrinsic_pct >= EXTRINSIC_CREDIT_STRONG)

    if (_pre_itm_delta_warn or _pre_itm_strike_prox) and not _thesis_blocks_roll:
        _delta_gap_to_emergency = max(0.0, DELTA_ITM_EMERGENCY - delta)
        _strike_below_cost = (effective_cost > 0 and strike > 0 and strike < effective_cost)
        _rescue_debit_est = _call_last_pre

        if _credit_strong:
            if _strike_below_cost:
                _credit_label = (
                    f"Extrinsic = ${_extrinsic_pre:.2f} ({_extrinsic_pct:.0%} of option price). "
                    f"Two paths: (A) same-strike rollout = small credit (preserves ${_extrinsic_pre:.2f} extrinsic); "
                    f"(B) rescue roll above ${effective_cost:.2f} = debit roll required "
                    f"(buying back ${_call_last_pre:.2f} intrinsic+extrinsic, selling cheaper OTM premium). "
                    f"Debit is cheapest NOW — grows as delta rises toward 0.70."
                )
            else:
                _credit_label = f"Extrinsic = ${_extrinsic_pre:.2f} ({_extrinsic_pct:.0%} of option price) — CREDIT ROLL VIABLE"
            _urgency_pre = "MEDIUM"
        elif _credit_still_viable:
            if _strike_below_cost:
                _credit_label = (
                    f"Extrinsic = ${_extrinsic_pre:.2f} ({_extrinsic_pct:.0%} of option price) — credit thin. "
                    f"Rescue roll above ${effective_cost:.2f} requires debit; debit cost grows daily. Act soon."
                )
            else:
                _credit_label = f"Extrinsic = ${_extrinsic_pre:.2f} ({_extrinsic_pct:.0%} of option price) — credit thin, act soon"
            _urgency_pre = "HIGH"
        else:
            _credit_label = f"Extrinsic = ${_extrinsic_pre:.2f} ({_extrinsic_pct:.0%}) — mostly intrinsic, debit roll required"
            _urgency_pre = "HIGH"

        _basis_note = (
            f" Net cost: ${effective_cost:.2f}. Strike ${strike:.2f} is "
            f"{'ABOVE' if strike >= effective_cost else 'BELOW'} net cost "
            f"({'protected' if strike >= effective_cost else f'roll above ${effective_cost:.2f} to protect breakeven'})."
        ) if effective_cost > 0 and strike > 0 else ""

        _pre_itm_rationale = (
            f"⚠️ ROLL WINDOW — "
            f"Delta={delta:.2f} entering ITM defense zone (0.55–0.70). "
            f"{_credit_label}.{_basis_note} "
            f"Δ gap to emergency gate: {_delta_gap_to_emergency:.2f}. "
            f"Act now — debit cost rises as delta climbs toward 0.70. "
            f"(Passarelli Ch.5: pre-ITM roll timing / McMillan Ch.3: anticipatory defense)"
        )
        _pos_regime_pre = str(row.get('Position_Regime', 'NEUTRAL') or 'NEUTRAL')
        if _pos_regime_pre == 'TRENDING_CHASE':
            _pre_itm_rationale += (
                " ⚠️ TRENDING_CHASE: stock is trending through strikes. "
                "This ITM event is structural, not temporary — assignment may be the correct outcome."
            )
        propose_gate(
            collector, "pre_itm_drift_roll",
            action="ROLL", urgency=_urgency_pre,
            rationale=_pre_itm_rationale,
            doctrine_source="Passarelli Ch.5: Pre-ITM Roll Timing",
            priority=15,
        )

    # ── Roll Timing Intelligence ──────────────────────────────────────────
    _timing = classify_roll_timing(row)

    # ── Thesis block interceptor ──────────────────────────────────────────
    # In v2, we do NOT block all subsequent gates. Instead, gates that produce
    # ROLL proposals check _thesis_blocks_roll and skip themselves. We still
    # collect a thesis-blocked HOLD proposal for the resolver.
    if result.get('_thesis_blocks_roll'):
        _thesis_sum = str(row.get('Thesis_Summary', '') or '')
        propose_gate(
            collector, "thesis_blocked_hold",
            action="HOLD", urgency="HIGH",
            rationale=(
                f"🚫 Thesis BROKEN — discretionary roll blocked. {_thesis_sum} "
                f"Evaluate: is this STRUCTURAL (exit) or TEMPORARY (hold)? "
                f"If structural, exit stock + buy back call. "
                f"If temporary, hold and reassess after recovery signals confirm "
                f"(McMillan Ch.3, Passarelli Ch.2: story check)."
            ),
            doctrine_source="ThesisEngine: BROKEN story gate",
            priority=22,
        )

    # ── Post-BUYBACK sticky gate ──────────────────────────────────────────
    _prior_action = str(row.get('Prior_Action', '') or '').upper()
    _ei_state_pre = str(row.get('Equity_Integrity_State', '') or '').strip()

    if _prior_action == "BUYBACK" and _ei_state_pre != "INTACT":
        propose_gate(
            collector, "post_buyback_sticky",
            action="HOLD", urgency="HIGH",
            is_hard_veto=True,
            rationale=(
                f"Post-BUYBACK hold — short call was removed, stock held unencumbered. "
                f"Equity Integrity is {_ei_state_pre or 'UNKNOWN'} (not yet INTACT). "
                f"Do NOT re-sell premium until structure confirms recovery. "
                f"Stock carries ${effective_cost * FIDELITY_MARGIN_RATE_DAILY * 100:.2f}/day margin cost "
                f"with zero theta offset — accept this cost as the price of decoupling. "
                f"(Jabbour Ch.11: re-evaluate only after structure resolves; "
                f"McMillan Ch.3: stock and call decisions are independent)"
            ),
            doctrine_source="Post-BUYBACK: Equity not INTACT — hold unencumbered",
            priority=22,
        )
        # Hard veto — no short call exists post-BUYBACK, so carry/gamma gates
        # fire on artificial data (Theta=0, no strike). Skip all subsequent gates.
        return collector.to_result(collector.get_veto(), result, "HARD_VETO")

    # ── Gamma Danger Zone gate ────────────────────────────────────────────
    _gdz_strike = safe_row_float(row, 'Short_Call_Strike', 'Strike')
    _gdz_theta = abs(safe_row_float(row, 'Theta'))
    _gdz_gamma = abs(safe_row_float(row, 'Gamma'))
    _gdz_hv = safe_row_float(row, 'HV_20D', default=0.20)
    if _gdz_hv >= 1.0:
        _gdz_hv /= 100.0
    _gdz_sigma = spot * (_gdz_hv / math.sqrt(252)) if spot > 0 else 0.0
    _gdz_drag = 0.5 * _gdz_gamma * (_gdz_sigma ** 2)
    _gdz_roc3 = safe_row_float(row, 'Gamma_ROC_3D')
    _gdz_atm_pct = abs(spot - _gdz_strike) / spot if spot > 0 and _gdz_strike > 0 else 1.0
    _gdz_ratio = _gdz_drag / _gdz_theta if _gdz_theta > 0 else 0.0
    _gdz_ei = str(row.get('Equity_Integrity_State', '') or '').strip()

    _gdz_fires = (
        _gdz_atm_pct < GAMMA_ATM_PROXIMITY
        and DTE_EMERGENCY_ROLL < dte <= DTE_INCOME_GATE
        and _gdz_ratio > GAMMA_DANGER_RATIO
        and _gdz_ei != 'BROKEN'
    )

    if _gdz_fires:
        _gdz_urgency = "HIGH" if _gdz_roc3 > 0 else "MEDIUM"
        _gdz_roc_note = (
            f"Gamma_ROC_3D={_gdz_roc3:+.4f} (accelerating — urgency escalated). "
            if _gdz_roc3 > 0 else
            f"Gamma_ROC_3D={_gdz_roc3:+.4f} (stable/declining). "
        )

        _gdz_call_last = abs(safe_row_float(row, 'Last', 'Short_Call_Last'))
        _gdz_intrinsic = max(0.0, spot - _gdz_strike) if spot > 0 and _gdz_strike > 0 else 0.0
        _gdz_extrinsic = max(0.0, _gdz_call_last - _gdz_intrinsic)
        _gdz_carry_negative = _gdz_drag > _gdz_theta
        _gdz_below_cost = drift_from_net < 0
        _gdz_assignment_loss = (_gdz_strike - effective_cost) if effective_cost > 0 else 0.0
        _gdz_extrinsic_pct = (_gdz_extrinsic / _gdz_call_last) if _gdz_call_last > 0 else 0.0

        if (
            _gdz_carry_negative
            and _gdz_below_cost
            and _gdz_strike > 0
            and effective_cost > 0
            and _gdz_extrinsic_pct < 0.15
        ):
            propose_gate(
                collector, "gamma_danger_assignment_economics",
                action="HOLD", urgency="HIGH",
                rationale=(
                    f"Gamma Danger Zone: near-ATM ({_gdz_atm_pct:.1%} from strike "
                    f"${_gdz_strike:.2f}), DTE={dte:.0f}, gamma/theta ratio={_gdz_ratio:.1f}x. "
                    f"{_gdz_roc_note}"
                    f"BUT roll economics unfavorable: carry is negative "
                    f"(gamma drag ${_gdz_drag*100:.2f}c > theta ${_gdz_theta*100:.2f}c/day), "
                    f"stock {drift_from_net:.1%} below net cost ${effective_cost:.2f}, "
                    f"extrinsic only ${_gdz_extrinsic:.2f}. "
                    f"Assignment at ${_gdz_strike:.2f} realizes "
                    f"${_gdz_assignment_loss:+.2f}/share vs cost basis — "
                    f"{'a defined loss but stops the bleed' if _gdz_assignment_loss < 0 else 'a gain'}. "
                    f"Consider: (A) let assignment happen at ${_gdz_strike:.2f}, "
                    f"(B) buy back call for ${_gdz_call_last:.2f} and hold stock for recovery, "
                    f"(C) roll ONLY if credit available at higher strike. "
                    f"Do NOT pay debit to extend negative carry. "
                    f"(McMillan Ch.3: assignment is the buy-write profit mechanism; "
                    f"Natenberg Ch.7: don't roll when gamma > theta.)"
                ),
                doctrine_source="McMillan Ch.3: Assignment economics + Natenberg Ch.7: Gamma danger",
                priority=20,
            )
        elif not _thesis_blocks_roll:
            propose_gate(
                collector, "gamma_danger_roll",
                action="ROLL", urgency=_gdz_urgency,
                rationale=(
                    f"Gamma Danger Zone: near-ATM ({_gdz_atm_pct:.1%} from strike "
                    f"${_gdz_strike:.2f}), DTE={dte:.0f}, gamma/theta ratio={_gdz_ratio:.1f}x. "
                    f"{_gdz_roc_note}"
                    f"Gamma drag ${_gdz_drag*100:.2f}c/contract/day approaching theta "
                    f"${_gdz_theta*100:.2f}c/contract/day — short gamma accelerating toward dominance. "
                    f"Roll to extend DTE (30-45d target reduces gamma ~40%) or move strike OTM. "
                    f"Natenberg Ch.7: 'ATM + low DTE is the maximum-risk configuration for short gamma.' "
                    f"Passarelli Ch.6: 'Pre-emptive roll before gamma overwhelms theta.'"
                ),
                doctrine_source="Natenberg Ch.7: Gamma danger zone + Passarelli Ch.6: pre-emptive roll",
                priority=20,
            )

    # ── Equity Integrity gate ─────────────────────────────────────────────
    _ei_state = str(row.get('Equity_Integrity_State', '') or '').strip()
    _ei_reason = str(row.get('Equity_Integrity_Reason', '') or '').strip()
    if _ei_state == 'BROKEN':
        _ei_theta = abs(safe_row_float(row, 'Theta'))
        _ei_gamma = abs(safe_row_float(row, 'Gamma'))
        _ei_hv = safe_row_float(row, 'HV_20D', default=0.20)
        if _ei_hv >= 1.0:
            _ei_hv /= 100.0
        _ei_sigma = spot * (_ei_hv / math.sqrt(252))
        _ei_gamma_drag = 0.5 * _ei_gamma * (_ei_sigma ** 2)

        _ei_short_strike = safe_row_float(row, 'Short_Call_Strike', 'Strike')
        _ei_otm_pct = (
            abs(_ei_short_strike - spot) / spot
            if _ei_short_strike > 0 and spot and spot > 0 else 0.0
        )
        _ei_gamma_dominant = (
            _ei_theta > 0
            and _ei_gamma_drag > _ei_theta * GAMMA_DOMINANCE_RATIO
            and _ei_otm_pct <= GAMMA_MONEYNESS_GUARD
        )

        if _ei_gamma_dominant:
            _ei_ratio = _ei_gamma_drag / _ei_theta if _ei_theta > 0 else float('inf')
            _ei_call_last = abs(safe_row_float(row, 'Short_Call_Last', 'Last'))
            _ei_entry = abs(safe_row_float(row, 'Short_Call_Premium', 'Premium_Entry'))

            _ei_intrinsic = max(0.0, spot - _ei_short_strike) if spot > 0 and _ei_short_strike > 0 else 0.0
            _ei_extrinsic_val = max(0.0, _ei_call_last - _ei_intrinsic)

            _ei_adx = safe_row_float(row, 'adx_14')
            _ei_roc = safe_row_float(row, 'roc_20')

            _ei_buyback_conviction = (
                dte <= DTE_EMERGENCY_ROLL
                or (spot > _ei_short_strike * BREAKOUT_THROUGH_STRIKE and _ei_roc > 0 and _ei_adx > 25)
                or _ei_extrinsic_val < EXTRINSIC_THETA_EXHAUSTED
            )

            if _ei_buyback_conviction:
                _ei_close_note = (
                    f" Current call at ${_ei_call_last:.2f} vs ${_ei_entry:.2f} entry "
                    f"({'profit' if _ei_call_last < _ei_entry else 'debit'} to close)."
                    if _ei_call_last > 0 and _ei_entry > 0 else ""
                )
                _ei_conv_reason = (
                    "DTE ≤ 7 (expiration week)" if dte <= DTE_EMERGENCY_ROLL else
                    f"stock ${spot:.2f} above strike ${_ei_short_strike:.2f} + momentum (ROC={_ei_roc:.1f}, ADX={_ei_adx:.0f})"
                    if spot > _ei_short_strike * BREAKOUT_THROUGH_STRIKE else
                    f"extrinsic only ${_ei_extrinsic_val:.2f} (< $0.20 — no theta left)"
                )
                propose_gate(
                    collector, "equity_broken_gamma_conviction_roll",
                    action="ROLL", urgency="HIGH",
                    rationale=(
                        f"⚡ Equity BROKEN + gamma dominant ({_ei_ratio:.1f}× theta) + "
                        f"buyback conviction ({_ei_conv_reason}): "
                        f"gamma drag ${_ei_gamma_drag*100:.2f}/contract/day vs theta "
                        f"${_ei_theta*100:.2f}/contract/day — HOLD bleeds ${(_ei_gamma_drag - _ei_theta)*100:.2f}/contract/day. "
                        f"{_ei_close_note} "
                        f"BUY BACK the short call to stop the gamma bleed and decouple from the stock decision. "
                        f"Then evaluate the stock independently: if thesis broken → sell stock; "
                        f"if temporary → re-sell a 30–45 DTE near-ATM call for better theta efficiency. "
                        f"(Passarelli Ch.6: close short premium in expiration week; "
                        f"Natenberg Ch.7: short gamma at {_ei_ratio:.1f}× theta is structurally unprofitable)"
                    ),
                    doctrine_source="Passarelli Ch.6: Expiration week close + Natenberg Ch.7: gamma/theta ratio",
                    priority=30,
                )
            else:
                # Gamma dominant but no conviction — check carry
                _gd_margin_daily = effective_cost * FIDELITY_MARGIN_RATE_DAILY if effective_cost > 0 else 0.0
                _gd_net_carry = _ei_theta - _gd_margin_daily - _ei_gamma_drag

                _gd_above_net_cost = (
                    effective_cost > 0
                    and spot > effective_cost
                    and dte > DTE_CUSHION_WINDOW
                )
                _gd_cushion_pct = (
                    (spot - effective_cost) / effective_cost
                    if effective_cost > 0 and spot > effective_cost else 0.0
                )

                if _gd_net_carry < 0 and not _gd_above_net_cost:
                    _gd_bleed_contract = abs(_gd_net_carry) * 100
                    _gd_bleed_to_exp = _gd_bleed_contract * dte

                    # Far-OTM theta override: if the call is far OTM and remaining
                    # theta income covers the gap to breakeven, the position is
                    # structurally sound despite negative carry — don't fire CAPITAL EXIT.
                    # (McMillan Ch.3: assignment is the profit mechanism; if call expires
                    # worthless and theta covers the gap, the structure is working.)
                    _gd_gap_per_share = max(0.0, effective_cost - spot) if effective_cost > 0 else 0.0
                    _gd_theta_remaining = _ei_theta * 100 * dte  # total θ income to expiry
                    _gd_call_far_otm = delta < 0.30 and dte <= DTE_CUSHION_WINDOW
                    _gd_theta_covers_gap = _gd_theta_remaining > _gd_gap_per_share * 100

                    if _gd_call_far_otm and _gd_theta_covers_gap:
                        propose_gate(
                            collector, "equity_broken_theta_covers_gap_hold",
                            action="HOLD", urgency="MEDIUM",
                            rationale=(
                                f"⚠️ Equity BROKEN ({_ei_reason}) + negative carry, "
                                f"BUT call is far OTM (Δ {delta:.3f}) with {dte}d to expiry. "
                                f"Remaining θ income ${_gd_theta_remaining:.0f} covers gap to "
                                f"breakeven ${_gd_gap_per_share:.2f}/share (${_gd_gap_per_share*100:.0f}). "
                                f"Assignment risk negligible — let call expire worthless and collect "
                                f"full premium. Stock below cost basis by ${_gd_gap_per_share:.2f}/share "
                                f"is offset by ${cum_premium:.2f}/share collected. "
                                f"Monitor: if stock breaks hard stop ${effective_cost * (1 + PNL_HARD_STOP_BW):.2f} → EXIT. "
                                f"(McMillan Ch.3: near-expiry far-OTM short call is pure income — "
                                f"structural damage doesn't override when θ covers the cost basis gap)"
                            ),
                            doctrine_source="McMillan Ch.3: Theta-covers-gap override — HOLD near expiry",
                            priority=31,
                        )
                    else:
                        # CAPITAL = auto-win over EV comparator. Reserve for
                        # genuine capital danger: approaching hard stop OR
                        # income path established and actively generating.
                        # When above hard stop cushion, let EXIT compete on EV
                        # so ROLL/LET_EXPIRE can win if they're higher EV.
                        _gnc_capital_danger = (
                            not _income_path_active
                            and drift_from_net <= PNL_APPROACHING_HARD_STOP
                        )
                        _gnc_exit_trigger = "CAPITAL" if _gnc_capital_danger else ""
                        propose_gate(
                            collector, "equity_broken_gamma_no_conv_exit",
                            action="EXIT", urgency="MEDIUM",
                            rationale=(
                                f"⚠️ Equity BROKEN ({_ei_reason}) + negative carry: "
                                f"θ ${_ei_theta*100:.2f}/day − margin ${_gd_margin_daily*100:.2f}/day "
                                f"− γ-drag ${_ei_gamma_drag*100:.2f}/day = "
                                f"net bleed ${_gd_bleed_contract:.2f}/contract/day "
                                f"(${_gd_bleed_to_exp:.0f} to expiry). "
                                f"Stock ${spot:.2f} is BELOW net cost basis ${effective_cost:.2f} — "
                                f"no premium cushion to absorb the bleed. "
                                f"Gamma dominance ({_ei_ratio:.1f}× theta) confirms drag exceeds income. "
                                f"Close the position or buy back the call "
                                f"and evaluate the stock independently. "
                                f"(McMillan Ch.3: don't carry a broken structure at negative EV; "
                                f"Natenberg Ch.7: negative carry + broken equity = structural loss)"
                            ),
                            doctrine_source="EquityIntegrity: BROKEN + Negative Carry → EXIT",
                            priority=30, exit_trigger_type=_gnc_exit_trigger,
                        )

                elif _gd_net_carry < 0 and _gd_above_net_cost:
                    _gd_bleed_contract = abs(_gd_net_carry) * 100
                    _gd_cushion_days = int(
                        (spot - effective_cost) * 100 / _gd_bleed_contract
                    ) if _gd_bleed_contract > 0 else 999
                    propose_gate(
                        collector, "equity_broken_gamma_no_conv_cushion_hold",
                        action="HOLD", urgency="MEDIUM",
                        rationale=(
                            f"⚠️ Equity BROKEN ({_ei_reason}) + negative daily carry: "
                            f"θ ${_ei_theta*100:.2f}/day − margin ${_gd_margin_daily*100:.2f}/day "
                            f"− γ-drag ${_ei_gamma_drag*100:.2f}/day = "
                            f"net bleed ${_gd_bleed_contract:.2f}/contract/day. "
                            f"BUT stock ${spot:.2f} is {_gd_cushion_pct:.1%} ABOVE net cost basis "
                            f"${effective_cost:.2f} (cushion from ${cum_premium:.2f}/share collected). "
                            f"Premium cushion absorbs ~{_gd_cushion_days}d of bleed before breakeven is threatened. "
                            f"Monitor for: (A) stock approaching net cost basis → upgrade to EXIT, "
                            f"(B) HV mean-reversion reducing gamma drag, "
                            f"(C) DTE ≤ 14 → roll or close. "
                            f"(McMillan Ch.3: cumulative premium income IS the BUY_WRITE edge — "
                            f"don't abandon accumulated cost reduction on a single cycle's carry metric)"
                        ),
                        doctrine_source="McMillan Ch.3: BUY_WRITE cost-basis cushion — HOLD with carry warning",
                        priority=32,
                    )
                else:
                    # Positive carry despite gamma dominance
                    propose_gate(
                        collector, "equity_broken_gamma_positive_carry_hold",
                        action="HOLD", urgency="MEDIUM",
                        rationale=(
                            f"⚠️ Equity BROKEN + gamma elevated ({_ei_ratio:.1f}× theta) — "
                            f"structurally expected at DTE {dte:.0f} near ATM (δ={delta:.2f}). "
                            f"Gamma drag ${_ei_gamma_drag*100:.2f}/contract/day vs theta "
                            f"${_ei_theta*100:.2f}/contract/day. "
                            f"Carry still positive (${_gd_net_carry*100:.2f}/day net) — "
                            f"theta income covers costs. Hold unless conviction develops: "
                            f"(A) stock breaks above ${_ei_short_strike:.2f} with momentum → buy back call, "
                            f"(B) DTE approaches expiration week (≤7d) → close or roll. "
                            f"(Passarelli Ch.6: near-ATM gamma at 2–3 weeks is structural, "
                            f"not an emergency — close short premium in expiration week, not before. "
                            f"Given: 'large gamma values are the reason ATM positions change value "
                            f"rapidly during expiration week' — this is expected behaviour at DTE {dte:.0f})"
                        ),
                        doctrine_source="Passarelli Ch.6: Gamma awareness — expiration week rule",
                        priority=34,
                    )
        else:
            # Standard BROKEN gate: no gamma dominance — check LEAPS carry inversion
            _ei_is_leaps = dte > DTE_LEAPS_THRESHOLD
            if _ei_is_leaps:
                # Prefer pre-computed Daily_Margin_Cost (correct: borrowed portion only)
                _ci_pre_v2_brk = float(row.get('Daily_Margin_Cost') or 0.0)
                _ci_margin_daily = (_ci_pre_v2_brk / 100.0) if _ci_pre_v2_brk > 0 else (
                    effective_cost * FIDELITY_MARGIN_RATE_DAILY if effective_cost > 0 else 0.0
                )
                _ci_theta_daily = abs(safe_row_float(row, 'Theta'))
                _ci_strike = safe_row_float(row, 'Short_Call_Strike', 'Strike')
                _ci_pct_otm = (
                    abs(spot - _ci_strike) / spot
                    if spot > 0 and _ci_strike > 0 else 0.0
                )

                if _ci_margin_daily > 0 and _ci_theta_daily > 0 and _ci_margin_daily >= _ci_theta_daily:
                    _ci_ratio = _ci_margin_daily / _ci_theta_daily

                    if _ci_ratio >= CARRY_INVERSION_SEVERE:
                        propose_gate(
                            collector, "equity_broken_leaps_carry_severe",
                            action="BUYBACK", urgency="HIGH",
                            is_hard_veto=True,
                            rationale=(
                                f"⚠️ Equity BROKEN + LEAPS carry severely inverted: "
                                f"margin cost ${_ci_margin_daily*100:.2f}/contract/day vs theta income "
                                f"${_ci_theta_daily*100:.2f}/contract/day ({_ci_ratio:.1f}× theta). "
                                f"Strike ${_ci_strike:.0f} is {_ci_pct_otm:.0%} OTM — theta too weak "
                                f"to cover financing. Buy back the short call. "
                                f"Do NOT re-sell while equity is BROKEN ({_ei_reason}) — hold stock "
                                f"unencumbered until structural deterioration resolves. "
                                f"(Given Ch.6: 'sell calls within one strike of ATM'; "
                                f"Jabbour Ch.11: 'close and re-evaluate rather than rolling a losing structure')"
                            ),
                            doctrine_source="Given Ch.6: LEAPS Carry Inversion (severe) + EquityIntegrity: BROKEN",
                            priority=35,
                        )
                    else:
                        _ci_net_bleed = (_ci_margin_daily - _ci_theta_daily) * 100
                        propose_gate(
                            collector, "equity_broken_leaps_carry_mild",
                            action="HOLD", urgency="HIGH",
                            rationale=(
                                f"⚠️ Equity BROKEN + LEAPS carry mildly inverted: "
                                f"margin ${_ci_margin_daily*100:.2f}/day vs theta "
                                f"${_ci_theta_daily*100:.2f}/day ({_ci_ratio:.1f}× theta, "
                                f"net bleed ${_ci_net_bleed:.2f}/day). "
                                f"Buying back the call would increase bleed to "
                                f"${_ci_margin_daily*100:.2f}/day with zero income. "
                                f"HOLD — the short call still offsets most margin cost. "
                                f"Monitor: if ratio exceeds 1.5× or equity recovers to re-sell "
                                f"closer to ATM (Given Ch.6). Strike ${_ci_strike:.0f} "
                                f"is {_ci_pct_otm:.0%} OTM."
                            ),
                            doctrine_source="Given Ch.6: LEAPS Carry Mild Inversion + EquityIntegrity: BROKEN",
                            priority=36,
                        )

            # Standard BROKEN gate: check net carry
            _ei_margin_daily = effective_cost * FIDELITY_MARGIN_RATE_DAILY if effective_cost > 0 else 0.0
            _ei_net_carry = _ei_theta - _ei_margin_daily - _ei_gamma_drag

            if _ei_net_carry < 0:
                _ei_bleed_contract = abs(_ei_net_carry) * 100
                _ei_bleed_to_exp = _ei_bleed_contract * dte

                _ei_above_net_cost = (
                    effective_cost > 0
                    and spot > effective_cost
                    and dte > DTE_CUSHION_WINDOW
                )
                _ei_cushion_pct = (
                    (spot - effective_cost) / effective_cost
                    if effective_cost > 0 and spot > effective_cost else 0.0
                )

                if _ei_above_net_cost:
                    _ei_cushion_days = int(
                        (spot - effective_cost) * 100 / _ei_bleed_contract
                    ) if _ei_bleed_contract > 0 else 999

                    _ei_sc_delta = abs(safe_row_float(row, 'Short_Call_Delta'))
                    _ei_sc_strike = safe_row_float(row, 'Short_Call_Strike', 'Strike')
                    _ei_sc_last = abs(safe_row_float(row, 'Short_Call_Last', 'Last'))
                    _ei_sc_pct_otm = (
                        (_ei_sc_strike - spot) / spot
                        if _ei_sc_strike > 0 and spot > 0 and _ei_sc_strike > spot else 0.0
                    )
                    _ei_stock_gain = (spot - effective_cost) * 100

                    if _ei_sc_delta < 0.30 and _ei_sc_pct_otm > 0.03:
                        _ei_buyback_cost = _ei_sc_last * 100
                        _ei_net_after_close = _ei_stock_gain - _ei_buyback_cost
                        propose_gate(
                            collector, "equity_broken_otm_call_exit",
                            action="EXIT", urgency="MEDIUM",
                            rationale=(
                                f"🟡 Close position — buy back call, then sell stock. "
                                f"Equity BROKEN ({_ei_reason}) + negative carry "
                                f"${_ei_bleed_contract:.2f}/contract/day. "
                                f"Step 1: buy back ${_ei_sc_strike:.0f} call at ${_ei_sc_last:.2f} "
                                f"(${_ei_buyback_cost:.0f} cost) — {_ei_sc_pct_otm:.0%} OTM, "
                                f"Δ {_ei_sc_delta:.3f}, cheap to close. "
                                f"Step 2: sell 100 shares at ${spot:.2f} — locks "
                                f"${_ei_stock_gain:.0f} gain above net cost ${effective_cost:.2f} "
                                f"({_ei_cushion_pct:.1%} cushion from ${cum_premium:.2f}/share "
                                f"collected across prior cycles). "
                                f"Net after closing both legs: ${_ei_net_after_close:+,.0f}. "
                                f"(McMillan Ch.3: lock the cost-basis gain, don't let bleed erode it; "
                                f"Passarelli Ch.6: call is {_ei_sc_pct_otm:.0%} OTM — "
                                f"buyback cost ${_ei_buyback_cost:.0f} is small vs "
                                f"${_ei_stock_gain:.0f} stock gain)"
                            ),
                            doctrine_source="McMillan Ch.3: lock cost-basis gain + Passarelli Ch.6: OTM call cheap to close",
                            priority=34,
                        )
                    else:
                        # Call near ATM — legs coupled, generic cushion HOLD
                        propose_gate(
                            collector, "equity_broken_cushion_hold",
                            action="HOLD", urgency="MEDIUM",
                            rationale=(
                                f"⚠️ Equity BROKEN ({_ei_reason}) + negative daily carry: "
                                f"θ ${_ei_theta*100:.2f}/day − margin ${_ei_margin_daily*100:.2f}/day "
                                f"− γ-drag ${_ei_gamma_drag*100:.2f}/day = "
                                f"net bleed ${_ei_bleed_contract:.2f}/contract/day. "
                                f"BUT stock ${spot:.2f} is {_ei_cushion_pct:.1%} ABOVE net cost basis "
                                f"${effective_cost:.2f} (cushion from ${cum_premium:.2f}/share collected). "
                                f"Premium cushion absorbs ~{_ei_cushion_days}d of bleed before breakeven is threatened. "
                                f"Short call Δ {_ei_sc_delta:.2f} — too close to ATM to treat legs independently. "
                                f"Monitor for: (A) stock approaching net cost basis → upgrade to EXIT, "
                                f"(B) equity integrity recovery (BROKEN → WEAKENING), "
                                f"(C) DTE ≤ 14 → roll or close. "
                                f"(McMillan Ch.3: cumulative premium income IS the BUY_WRITE edge — "
                                f"don't abandon accumulated cost reduction on a single cycle's carry metric)"
                            ),
                            doctrine_source="McMillan Ch.3: BUY_WRITE cost-basis cushion — HOLD with carry warning",
                            priority=36,
                        )
                else:
                    # Far-OTM theta override: same logic as gamma-dominant path.
                    # If call is far OTM and remaining theta covers the gap to
                    # breakeven, the position is structurally sound — HOLD.
                    _nc_gap_per_share = max(0.0, effective_cost - spot) if effective_cost > 0 else 0.0
                    _nc_theta_remaining = _ei_theta * 100 * dte
                    _nc_sc_delta = abs(safe_row_float(row, 'Short_Call_Delta', 'Delta'))
                    _nc_call_far_otm = _nc_sc_delta < 0.30
                    _nc_theta_covers_gap = _nc_theta_remaining > _nc_gap_per_share * 100

                    if _nc_call_far_otm and _nc_theta_covers_gap:
                        propose_gate(
                            collector, "equity_broken_neg_carry_theta_hold",
                            action="HOLD", urgency="MEDIUM",
                            rationale=(
                                f"⚠️ Equity BROKEN ({_ei_reason}) + negative carry "
                                f"${_ei_bleed_contract:.2f}/contract/day, BUT call is far OTM "
                                f"(Δ {_nc_sc_delta:.3f}) — assignment risk negligible. "
                                f"Remaining θ income ${_nc_theta_remaining:.0f} covers gap to "
                                f"breakeven ${_nc_gap_per_share:.2f}/share (${_nc_gap_per_share*100:.0f}). "
                                f"Stock ${spot:.2f} is below cost basis ${effective_cost:.2f} by "
                                f"${_nc_gap_per_share:.2f}/share, but ${cum_premium:.2f}/share collected "
                                f"across prior cycles offsets. Let call expire worthless and collect "
                                f"full premium. Monitor: hard stop ${effective_cost * (1 + PNL_HARD_STOP_BW):.2f}. "
                                f"(McMillan Ch.3: near-expiry far-OTM short call is pure income — "
                                f"don't exit when θ covers the cost basis gap)"
                            ),
                            doctrine_source="McMillan Ch.3: Theta-covers-gap override — neg carry HOLD",
                            priority=35,
                        )
                    else:
                        # CAPITAL only when approaching hard stop — otherwise
                        # let EXIT compete on EV so ROLL/LET_EXPIRE can win.
                        _nc_capital_danger = (
                            not _income_path_active
                            and drift_from_net <= PNL_APPROACHING_HARD_STOP
                        )
                        _nc_exit_trigger = "CAPITAL" if _nc_capital_danger else ""
                        propose_gate(
                            collector, "equity_broken_neg_carry_exit",
                            action="EXIT", urgency="MEDIUM",
                            rationale=(
                                f"⚠️ Equity BROKEN ({_ei_reason}) + negative carry: "
                                f"θ ${_ei_theta*100:.2f}/day − margin ${_ei_margin_daily*100:.2f}/day "
                                f"− γ-drag ${_ei_gamma_drag*100:.2f}/day = "
                                f"net bleed ${_ei_bleed_contract:.2f}/contract/day "
                                f"(${_ei_bleed_to_exp:.0f} to expiry). "
                                f"Stock ${spot:.2f} is BELOW net cost basis ${effective_cost:.2f} — "
                                f"no premium cushion to absorb the bleed. "
                                f"Call Δ {_nc_sc_delta:.3f} — not far enough OTM to ignore. "
                                f"Holding a structurally declining stock while paying to hold "
                                f"is a compounding loss. Close the position or buy back the call "
                                f"and evaluate the stock independently. "
                                f"(McMillan Ch.3: don't carry a broken structure at negative EV; "
                                f"Natenberg Ch.7: negative carry + broken equity = structural loss)"
                            ),
                            doctrine_source="EquityIntegrity: BROKEN + Negative Carry → EXIT",
                            priority=36, exit_trigger_type=_nc_exit_trigger,
                        )
            else:
                # Positive carry: theta covers costs
                propose_gate(
                    collector, "equity_broken_positive_carry_hold",
                    action="HOLD", urgency="HIGH",
                    rationale=(
                        f"⚠️ Equity Integrity BROKEN — structural deterioration detected "
                        f"({_ei_reason}). "
                        f"Carry still positive: θ ${_ei_theta*100:.2f}/day > costs "
                        f"${(_ei_margin_daily + _ei_gamma_drag)*100:.2f}/day — "
                        f"theta income justifies patience. "
                        f"Rolling locks in deeper commitment to a structurally declining stock. "
                        f"Hold and reassess: confirm if breakdown is temporary or structural "
                        f"before next roll (McMillan Ch.1: trend context first)."
                    ),
                    doctrine_source="EquityIntegrity: BROKEN structural gate",
                    priority=38,
                )

    # ── 3a-LEAPS. Carry Inversion (non-BROKEN) ───────────────────────────
    _ci_is_leaps = dte > DTE_LEAPS_THRESHOLD
    if _ci_is_leaps and _ei_state != 'BROKEN':
        # Prefer pre-computed Daily_Margin_Cost (correct: borrowed portion only)
        _ci_precomputed = float(row.get('Daily_Margin_Cost') or 0.0)
        _ci_margin_daily = (_ci_precomputed / 100.0) if _ci_precomputed > 0 else (
            effective_cost * FIDELITY_MARGIN_RATE_DAILY if effective_cost > 0 else 0.0
        )
        _ci_theta_daily = abs(safe_row_float(row, 'Theta'))
        _ci_strike = safe_row_float(row, 'Short_Call_Strike', 'Strike')
        _ci_pct_otm = (
            abs(spot - _ci_strike) / spot
            if spot > 0 and _ci_strike > 0 else 0.0
        )

        if _ci_margin_daily > 0 and _ci_theta_daily > 0 and _ci_margin_daily >= _ci_theta_daily:
            _ci_ratio = _ci_margin_daily / _ci_theta_daily
            if not _thesis_blocks_roll:
                propose_gate(
                    collector, "leaps_carry_inversion_roll",
                    action="ROLL", urgency="MEDIUM",
                    rationale=(
                        f"📊 LEAPS carry inverted: margin ${_ci_margin_daily*100:.2f}/contract/day ≥ "
                        f"theta ${_ci_theta_daily*100:.2f}/contract/day ({_ci_ratio:.1f}× theta). "
                        f"Strike ${_ci_strike:.0f} is {_ci_pct_otm:.0%} OTM — theta decays too slowly "
                        f"at this distance to cover financing at 10.375%/yr. "
                        f"Buy back and re-sell 30–45 DTE closer to ATM for efficient carry "
                        f"(Given Ch.6: 'one strike from ATM'; "
                        f"Augen: 'roll when the new position has similar dynamics')."
                    ),
                    doctrine_source="Given Ch.6: LEAPS Carry Inversion — re-sell closer",
                    priority=35,
                )

    # ── 3a-ALL. Carry inversion (all DTE, pre-computed) ───────────────────
    from core.management.cycle3.doctrine.shared_income_gates import gate_carry_inversion_roll
    _r_ci_all = result.copy()
    _ci_fired, _r_ci_all = gate_carry_inversion_roll(row=row, result=_r_ci_all)
    if _ci_fired:
        _shared_gate_to_proposal(collector, "carry_inversion_roll", _ci_fired, _r_ci_all, priority=36)

    # ── 3b. Dividend assignment gate ──────────────────────────────────────
    _bw_days_div = safe_row_float(row, 'Days_To_Dividend', default=9999.0)
    _bw_div_amt = safe_row_float(row, 'Dividend_Amount')
    if delta > DELTA_DIVIDEND_ASSIGNMENT and _bw_days_div < 5 and _bw_div_amt > 0:
        _bw_div_urgency = "CRITICAL" if _bw_days_div < 2 else "HIGH"
        if not _thesis_blocks_roll:
            propose_gate(
                collector, "dividend_assignment_roll",
                action="ROLL", urgency=_bw_div_urgency,
                rationale=(
                    f"⚠️ Dividend assignment risk: ex-dividend in {_bw_days_div:.0f} day(s) "
                    f"(${_bw_div_amt:.2f}/share), call delta={delta:.2f} > 0.50. "
                    f"Call owners will exercise early to capture the dividend — "
                    f"forced assignment before expiry is highly probable. "
                    f"Roll up/out NOW to avoid early assignment: "
                    f"close the short call and re-sell a further-OTM strike "
                    f"(McMillan Ch.2: dividend-driven early exercise is the primary risk "
                    f"of short calls near ex-date with delta > 0.50)."
                ),
                doctrine_source="McMillan Ch.2: Dividend Assignment Risk — BUY_WRITE (M1)",
                priority=16,
            )

    # ── 3b-GAP1: 21-DTE income gate ──────────────────────────────────────
    premium_collected_21 = abs(safe_row_float(row, 'Short_Call_Premium', 'Premium_Entry'))
    current_close_cost_21 = abs(safe_row_float(row, 'Short_Call_Last', 'Last'))
    pct_captured_21 = (
        (premium_collected_21 - current_close_cost_21) / premium_collected_21
        if premium_collected_21 > 0 else 0.0
    )
    _bw_moneyness = str(row.get('Moneyness_Label') or row.get('Short_Call_Moneyness') or 'OTM')
    # Far-OTM exemption: when the call is far OTM (delta < 0.30) and assignment
    # risk is negligible, the 21-DTE gate's "gamma-theta degradation" rationale
    # doesn't apply — theta IS the income, and it's working. Rolling would cost
    # the buyback spread for minimal net credit improvement.
    # (McMillan Ch.3: "Let far-OTM short calls expire — rolling adds friction
    # with no structural benefit when assignment risk is zero.")
    _mc_p_assign_21 = safe_row_float(row, 'MC_Assign_P_Expiry')
    _far_otm_exempt = (
        delta < 0.30
        and (_mc_p_assign_21 < 0.05 if not (isinstance(_mc_p_assign_21, float) and _mc_p_assign_21 != _mc_p_assign_21) else delta < 0.25)
    )

    if (
        dte <= DTE_INCOME_GATE
        and dte >= DTE_EMERGENCY_ROLL
        and pct_captured_21 < PREMIUM_CAPTURE_TARGET
        and _bw_moneyness != 'ITM'
        and not _thesis_blocks_roll
        and not _far_otm_exempt
    ):
        _cc_21_urgency = 'MEDIUM' if pct_captured_21 >= 0 else 'HIGH'

        # Strategy-aware IV regime check
        _iv_entry_21 = safe_row_float(row, 'IV_Entry')
        _iv_now_21 = safe_row_float(row, 'IV_30D', 'IV_Now')
        _iv_pctile_21 = safe_row_float(row, 'IV_Percentile', default=50.0)
        _iv_gap_21 = safe_row_float(row, 'IV_vs_HV_Gap')

        _iv_collapsed_21 = (
            _iv_entry_21 > 0 and _iv_now_21 > 0
            and (_iv_now_21 / _iv_entry_21) < 0.70
            and _iv_pctile_21 < 25
            and _iv_gap_21 <= 0
        )

        if _iv_collapsed_21:
            propose_gate(
                collector, "income_gate_21dte_iv_collapsed_hold",
                action="HOLD", urgency="LOW",
                rationale=(
                    f"21-DTE income gate: DTE={dte:.0f}, {pct_captured_21:.0%} captured. "
                    f"Vol regime shift: IV contracted {(1 - _iv_now_21/_iv_entry_21):.0%} from entry "
                    f"({_iv_entry_21:.1%} -> {_iv_now_21:.1%}), IV_Percentile={_iv_pctile_21:.0f}, "
                    f"IV-HV gap={_iv_gap_21:+.1%}. "
                    f"Rolling into a low-IV environment yields thin premium — mean-reversion edge exhausted. "
                    f"Let current premium decay (remaining time value = ${current_close_cost_21:.2f}). "
                    f"Chan: 'Mean-reversion exit when regime shifts — don't repeat a trade whose edge is gone.' "
                    f"Natenberg Ch.8: 'Selling premium below HV = negative expected value.'"
                ),
                doctrine_source="Chan: Strategy-aware exit — Vol regime shift (BW)",
                priority=40,
            )
        else:
            # Position Trajectory context
            _pos_regime_21 = str(row.get('Position_Regime', 'NEUTRAL') or 'NEUTRAL')
            _consec_debits_21 = int(safe_row_float(row, 'Trajectory_Consecutive_Debit_Rolls'))
            _stock_ret_21 = safe_row_float(row, 'Trajectory_Stock_Return')
            if _pos_regime_21 == 'TRENDING_CHASE':
                _cc_21_urgency = 'HIGH'
                propose_gate(
                    collector, "income_gate_21dte_trending_chase",
                    action="ROLL", urgency=_cc_21_urgency,
                    rationale=(
                        f"21-DTE income gate: DTE={dte:.0f} ≤ 21 with {pct_captured_21:.0%} captured. "
                        f"⚠️ TRENDING_CHASE: stock has moved {_stock_ret_21:+.0%} since entry "
                        f"with {_consec_debits_21} consecutive debit roll(s). "
                        f"Stock is structurally outrunning the covered call — this is NOT a temporary ITM event. "
                        f"Consider: (A) accept assignment and redeploy capital at higher basis, "
                        f"(B) buy back call and hold stock unencumbered for the trend, or "
                        f"(C) widen to a much higher strike if premium justifies carry. "
                        f"Rolling to the next monthly repeats the chase cycle. "
                        f"(McMillan Ch.3: strike-chase recognition; Given Ch.6: 21-DTE gate)"
                    ),
                    doctrine_source="McMillan Ch.3 + Position Trajectory: Strike Chase at 21-DTE",
                    priority=40,
                )
            else:
                _regime_note_21 = ""
                if _pos_regime_21 in ('SIDEWAYS_INCOME', 'MEAN_REVERSION'):
                    _regime_note_21 = (
                        f" {_pos_regime_21}: roll OUT at same strike for credit — "
                        "do NOT roll UP to a higher strike (debit). "
                        "Stock is range-bound; income cycle is working. "
                        "If assigned at current strike, that's a profitable exit "
                        "(McMillan Ch.3: same-strike credit rolls in range-bound regimes)."
                    )
                propose_gate(
                    collector, "income_gate_21dte_roll",
                    action="ROLL", urgency=_cc_21_urgency,
                    rationale=(
                        f"21-DTE income gate: DTE={dte:.0f} ≤ 21 with only "
                        f"{pct_captured_21:.0%} profit captured (need ≥50%). "
                        f"Gamma-theta ratio has degraded — short call edge exhausted. "
                        f"Buy back current call and roll out to 30-45 DTE to reset cycle. "
                        f"(Given Ch.6: 21-DTE income roll; Passarelli Ch.2: theta/gamma degradation.)"
                        f"{_regime_note_21}"
                    ),
                    doctrine_source="Given Ch.6 + Passarelli Ch.2: 21-DTE Income Roll Gate",
                    priority=40,
                )

    # Far-OTM hold: when the 21-DTE gate was skipped due to far-OTM exemption,
    # explicitly propose HOLD so the resolver sees the rationale.
    if (
        _far_otm_exempt
        and dte <= DTE_INCOME_GATE
        and dte >= DTE_EMERGENCY_ROLL
        and pct_captured_21 < PREMIUM_CAPTURE_TARGET
        and _bw_moneyness != 'ITM'
    ):
        _remaining_theta_21 = abs(safe_row_float(row, 'Theta')) * 100 * dte
        propose_gate(
            collector, "income_gate_21dte_far_otm_hold",
            action="HOLD", urgency="LOW",
            rationale=(
                f"21-DTE income gate: DTE={dte:.0f}, {pct_captured_21:.0%} captured, "
                f"but call is far OTM (Δ {delta:.3f}, P(assign)={_mc_p_assign_21:.0%}). "
                f"Rolling costs ${current_close_cost_21:.2f} buyback for marginal net credit improvement. "
                f"Remaining θ income ${_remaining_theta_21:.0f} to expiry — "
                f"let call expire worthless and sell new cycle after expiry. "
                f"(McMillan Ch.3: far-OTM expiry is the intended profit mechanism — "
                f"rolling adds spread friction with no assignment benefit.)"
            ),
            doctrine_source="McMillan Ch.3: Far-OTM expiry — let theta work",
            priority=40,
        )

    # ── 4. 50% premium capture with timing gate ──────────────────────────
    premium_collected = abs(safe_row_float(row, 'Short_Call_Premium', 'Premium_Entry'))
    current_close_cost = abs(safe_row_float(row, 'Short_Call_Last', 'Last'))
    if (
        premium_collected > 0
        and current_close_cost <= premium_collected * PREMIUM_CAPTURE_TARGET
        and dte > DTE_INCOME_GATE
        and not _thesis_blocks_roll
    ):
        pct_captured = 1 - (current_close_cost / premium_collected)

        if _timing['action_mod'] == 'WAIT':
            propose_gate(
                collector, "premium_capture_wait_hold",
                action="HOLD", urgency="LOW",
                rationale=(
                    f"50% premium captured ({pct_captured:.0%}) but market timing unfavorable — "
                    f"{_timing['reason']} "
                    f"Hold and monitor; roll when market shows directional clarity "
                    f"(Passarelli Ch.6: 50% Rule + Roll Timing)."
                ),
                doctrine_source="Passarelli Ch.6: 50% Rule + Timing Gate",
                priority=45,
            )
        else:
            _50_urgency = _timing['urgency_mod'] if _timing['action_mod'] == 'ROLL_NOW' else "MEDIUM"
            timing_note = f" {_timing['reason']}" if _timing['reason'] else ""
            _50_rationale = (
                f"50% premium captured ({pct_captured:.0%} of ${premium_collected:.2f} entry credit) "
                f"with {dte:.0f} DTE remaining — redeploy into next cycle.{timing_note} "
                f"(Passarelli Ch.6: 50% Rule)."
            )
            # Position Trajectory context for 50% profit gate
            _pos_regime_50 = str(row.get('Position_Regime', 'NEUTRAL') or 'NEUTRAL')
            _stock_ret_50 = safe_row_float(row, 'Trajectory_Stock_Return')
            if _pos_regime_50 in ('SIDEWAYS_INCOME', 'MEAN_REVERSION'):
                _50_rationale += (
                    f" Position regime: {_pos_regime_50} — healthy cycle complete, roll to continue."
                )
            elif _pos_regime_50 == 'TRENDING_CHASE':
                _50_rationale += (
                    f" ⚠️ TRENDING_CHASE: 50% captured but stock trending {_stock_ret_50:+.0%} since entry. "
                    f"Next roll will likely face the same chase — consider accepting assignment or restructuring."
                )
            propose_gate(
                collector, "premium_capture_roll",
                action="ROLL", urgency=_50_urgency,
                rationale=_50_rationale,
                doctrine_source="Passarelli Ch.6: 50% Rule",
                priority=45,
            )

    # ── 5. Negative carry ─────────────────────────────────────────────────
    from core.shared.finance_utils import annualized_yield as _ann_yield
    dte_val = max(dte, 1)
    capital_at_risk = effective_cost if effective_cost > 0 else abs(float(spot or 0))
    premium = abs(safe_row_float(row, 'Short_Call_Premium', 'Premium_Entry'))
    if premium > 0 and capital_at_risk > 0 and not _thesis_blocks_roll:
        annualized_yield = _ann_yield(premium, capital_at_risk, dte_val)
        # Prefer pre-computed Daily_Margin_Cost (correct: borrowed portion only)
        _nc_pre_v2 = float(row.get('Daily_Margin_Cost') or 0.0)
        daily_margin_bleed = (_nc_pre_v2 / 100.0) if _nc_pre_v2 > 0 else (
            capital_at_risk * FIDELITY_MARGIN_RATE_DAILY
        )
        if annualized_yield < FIDELITY_MARGIN_RATE:
            cost_note = (
                f" (net cost ${effective_cost:.2f}/share after ${cum_premium:.2f} collected)"
                if cum_premium > 0 else ""
            )
            bleed_note = (
                f" Fidelity margin at 10.375%/yr costs ${daily_margin_bleed:.3f}/share/day "
                f"(${daily_margin_bleed * 100:.2f}/contract/day) — premium is not covering carry."
            )

            if _timing['action_mod'] == 'WAIT':
                _nc_urgency = "LOW"
                if dte < DTE_CUSHION_WINDOW and annualized_yield < YIELD_ESCALATION_THRESHOLD:
                    _nc_urgency = "MEDIUM"
                propose_gate(
                    collector, "negative_carry_wait_hold",
                    action="HOLD", urgency=_nc_urgency,
                    rationale=(
                        f"Yield {annualized_yield:.1%} < Fidelity margin 10.375%{cost_note}.{bleed_note} "
                        f"Roll warranted but market timing unfavorable — "
                        f"{_timing['reason']} "
                        f"Monitor; re-evaluate when directional clarity appears "
                        f"(McMillan Ch.3: Yield Maintenance + Roll Timing)."
                        + (f" ⚠️ DTE={dte:.0f} short — carry deficit growing daily." if _nc_urgency == "MEDIUM" else "")
                    ),
                    doctrine_source="McMillan Ch.3: Yield Maintenance + Timing Gate",
                    priority=48,
                )
            else:
                _nc_roll_urgency = _timing['urgency_mod'] if _timing['action_mod'] == 'ROLL_NOW' else "MEDIUM"
                timing_note = f" {_timing['reason']}" if _timing['reason'] else ""
                propose_gate(
                    collector, "negative_carry_roll",
                    action="ROLL", urgency=_nc_roll_urgency,
                    rationale=(
                        f"Negative carry: yield {annualized_yield:.1%} < Fidelity margin 10.375%"
                        f"{cost_note}.{bleed_note}{timing_note} "
                        f"Roll to restore yield above carry cost (McMillan Ch.3: Yield Maintenance)."
                    ),
                    doctrine_source="McMillan Ch.3: Yield Maintenance",
                    priority=48,
                )

    # ── 5b. Debit Roll Efficiency / Cadence Switch (annotation) ───────────
    _cadence_annotation = ""
    _has_debit_rolls = bool(row.get('Has_Debit_Rolls', False))
    _gross_prem = safe_row_float(row, 'Gross_Premium_Collected')
    _total_close_cost = safe_row_float(row, 'Total_Close_Cost')
    _is_emergency_zone = delta > DELTA_PRE_ITM_WARNING or dte < DTE_CUSHION_WINDOW

    if _has_debit_rolls and _gross_prem > 0 and not _is_emergency_zone:
        _buyback_ratio = _total_close_cost / _gross_prem

        if _buyback_ratio > 0.40:
            _cycle_yield_note = ""
            if premium > 0 and capital_at_risk > 0:
                _ann_yield = (premium / capital_at_risk) * (365 / max(dte, 1))
                _cycle_yield_note = f" Current call yield: {_ann_yield:.1%}/yr annualized."

            _net_collected = _gross_prem - _total_close_cost
            _debit_note = (
                f"Gross collected: ${_gross_prem:.2f}/share, "
                f"buyback costs: ${_total_close_cost:.2f}/share ({_buyback_ratio:.0%} of gross), "
                f"net kept: ${_net_collected:.2f}/share."
            )

            _exit_credit_note = (
                f" Current call has ${_extrinsic_pre:.2f} extrinsic — "
                f"buying back now at ${_call_last_pre:.2f} vs original ${premium_collected:.2f} entry."
                if _call_last_pre > 0 and premium_collected > 0 else ""
            )

            _far_otm = delta < DELTA_FAR_OTM
            _long_dte = dte > DTE_CADENCE_THRESHOLD

            _cadence_note_text = (
                "Consider buying back this call and switching to monthly (30–45 DTE) cycles: "
                "near-dated calls have faster theta decay, allow tighter strikes near current price, "
                "and reduce the size of any future buyback if stock runs again."
                if _long_dte else
                "When rolling at expiry, consider tighter near-dated strikes rather than "
                "extending further OTM — the debit roll history suggests the far-OTM cadence "
                "is not capturing enough premium relative to buyback risk."
            )

            _cadence_annotation = (
                f"  ⚠️ Cadence review: {_debit_note}{_exit_credit_note}{_cycle_yield_note} "
                f"{_cadence_note_text} "
                f"(Natenberg Ch.8: strike/cycle selection; McMillan Ch.3: net premium efficiency)"
            )

    # ── 6. Persistence escalation (annotation) ───────────────────────────
    _persistence_annotation = ""
    if row.get('Drift_Persistence') == 'Sustained':
        if drift_from_net > 0:
            _persistence_annotation = (
                f" Sustained drift confirms trend, but stock still {drift_from_net:.1%} above net cost"
                f" — plan exit within days, not panic (Passarelli Ch.5)."
            )
        elif dte > 21 and delta < 0.50:
            # DTE guard: OTM call with ample time → text matches HIGH urgency cap.
            _persistence_annotation = (
                " Sustained drift confirms trend — plan roll within this cycle"
                " (Passarelli Ch.5). DTE and delta not yet urgent."
            )
        else:
            _persistence_annotation = " Sustained drift confirms trend; immediate action required (Passarelli Ch.5)."

    # ── 7. Thesis regime degradation (annotation) ─────────────────────────
    _thesis_annotation = ""
    thesis = check_thesis_degradation(row)
    if thesis:
        _thesis_annotation = f" Entry regime degraded: {thesis['text']} (McMillan Ch.4: Thesis Persistence)."

    # ── 7b. Signal Hub: institutional signal context (annotation) ─────────
    _signal_hub_bw_notes = ""
    _weekly_bias_bw = str(row.get('Weekly_Trend_Bias', 'Unknown') or 'Unknown')
    if _weekly_bias_bw == 'CONFLICTING':
        _signal_hub_bw_notes += (
            " Weekly trend CONFLICTS with daily (Murphy 0.634): "
            "CC roll strike selection should favor defensive until weekly clarifies."
        )
    _squeeze_on_bw = bool(row.get('Keltner_Squeeze_On', False))
    if _squeeze_on_bw:
        _signal_hub_bw_notes += (
            " Keltner squeeze ON — volatility compressing. "
            "ROLL EV adjusted for thin premiums (Raschke/Murphy 0.739)."
        )

    # ── 8. IV term structure note (annotation) ────────────────────────────
    _iv_annotation = ""
    iv_shape = str(row.get('iv_surface_shape', '') or '').upper()
    if iv_shape == 'BACKWARDATION':
        slope = safe_row_float(row, 'iv_ts_slope_30_90')
        _iv_annotation = (
            f" IV BACKWARDATED ({slope:+.1f}pt 30-90d): collecting elevated near-term IV "
            f"— premium receipt above normal; favorable short-vol entry (Natenberg Ch.11)."
        )

    # ── 9. EV comparator (always evaluates) ──────────────────────────────
    ev_result = None
    try:
        _ev = compare_actions_bw(
            row,
            spot=spot,
            strike=strike,
            effective_cost=effective_cost,
            qty=abs(safe_row_float(row, 'Quantity', default=1.0)),
            dte=max(dte, 1),
        )
        ev_result = _ev
        result["Action_EV_Ranking"] = _ev["ranked_actions"]
        result["Action_EV_Winner"] = _ev["ev_winner"]
        result["Action_EV_Margin"] = _ev["ev_margin"]
        result["Action_EV_Hold"] = _ev["ev_hold"]
        result["Action_EV_Roll"] = _ev["ev_roll"]
        result["Action_EV_Assign"] = _ev["ev_assign"]
        result["Action_EV_Buyback"] = _ev["ev_buyback"]
        result["EV_Buyback_Trigger"] = _ev["ev_buyback_trigger"]
        result["Gamma_Drag_Daily"] = _ev["gamma_drag_daily"]

        # ── Timing quality adjustments ────────────────────────────────────
        # Adjust raw EV estimates for market timing conditions that the
        # deterministic EV model doesn't capture. This makes the resolver
        # holistically weigh timing signals instead of treating them as
        # post-resolution annotations.
        _timing_roll_adj = 1.0
        _timing_hold_bonus = 0.0
        _timing_notes = []

        # (a) IV depressed: IV/HV < 1.0 → roll credit will be thinner than
        #     the EV model assumes. Discount ROLL EV proportionally.
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
                f"IV depressed ({_tq_iv_now:.0%} vs HV {_tq_hv:.0%}, "
                f"ratio {_tq_iv_hv_ratio:.2f}): ROLL EV discounted "
                f"×{_iv_discount:.2f} — credit will be thinner than modeled"
            )

        # (b) Macro proximity: HIGH event ≤ 3d → IV expansion expected.
        #     ROLL now = selling cheap; waiting = selling into elevated IV.
        _tq_days_macro = safe_row_float(row, 'Days_To_Macro', default=999.0)
        _tq_macro_type = str(row.get('Macro_Next_Type', '') or '').upper()
        _tq_macro_event = str(row.get('Macro_Next_Event', '') or '')
        _tq_macro_high = _tq_macro_type in ('FOMC', 'CPI', 'NFP')
        if _tq_macro_high and _tq_days_macro <= 3:
            _timing_roll_adj *= 0.70
            _timing_hold_bonus = abs(_ev.get('ev_roll', 0)) * 0.20
            _timing_notes.append(
                f"Macro {_tq_macro_event} in {_tq_days_macro:.0f}d: "
                f"ROLL EV discounted ×0.70 + HOLD bonus "
                f"${_timing_hold_bonus:,.0f} — IV expansion expected, "
                f"wait for better credit (Passarelli Ch.6)"
            )
        elif _tq_macro_high and _tq_days_macro <= 5:
            _timing_roll_adj *= 0.85
            _timing_notes.append(
                f"Macro {_tq_macro_event} in {_tq_days_macro:.0f}d: "
                f"ROLL EV discounted ×0.85 — event proximity"
            )

        # (c) Keltner squeeze: compressed vol → thin premiums.
        _tq_squeeze = bool(row.get('Keltner_Squeeze_On', False))
        if _tq_squeeze:
            _timing_roll_adj *= 0.85
            _timing_notes.append(
                "Keltner squeeze ON: ROLL EV discounted ×0.85 — "
                "vol compressed, premiums thin (Raschke/Murphy 0.739)"
            )

        # (d) Debit roll history: consecutive debit rolls erode income edge.
        #     1 debit roll = normal, 2 = caution (×0.90), 3+ = strong (×0.80).
        #     Only discount when position is NOT in emergency (deep ITM/low DTE).
        _tq_consec_debits = int(safe_row_float(row, 'Trajectory_Consecutive_Debit_Rolls'))
        _tq_emergency = delta > DELTA_PRE_ITM_WARNING or dte < DTE_CUSHION_WINDOW
        if _tq_consec_debits >= 2 and not _tq_emergency:
            _debit_discount = 0.80 if _tq_consec_debits >= 3 else 0.90
            _timing_roll_adj *= _debit_discount
            _timing_notes.append(
                f"{_tq_consec_debits} consecutive debit roll(s): ROLL EV "
                f"discounted ×{_debit_discount:.2f} — income edge eroding "
                f"(McMillan Ch.3: debit rolls signal structural headwind)"
            )

        # Apply adjustments to EV values
        if _timing_roll_adj < 1.0:
            _ev['ev_roll'] = _ev.get('ev_roll', 0) * _timing_roll_adj
            _ev['ev_buyback'] = _ev.get('ev_buyback', 0) * max(_timing_roll_adj, 0.90)
            if _timing_hold_bonus > 0:
                _ev['ev_hold'] = _ev.get('ev_hold', 0) + _timing_hold_bonus
            # Recompute winner after adjustments
            _adj_evs = {
                'HOLD': _ev.get('ev_hold', 0),
                'ROLL': _ev.get('ev_roll', 0),
                'ASSIGN': _ev.get('ev_assign', 0),
                'BUYBACK': _ev.get('ev_buyback', 0),
            }
            _ev['ev_winner'] = max(_adj_evs, key=_adj_evs.get)
            _ev['ev_summary'] = (
                f"Action EV (over {max(dte,1):.0f}d): "
                + " | ".join(f"{a} {'+' if v >= 0 else ''}{v:,.0f}"
                             for a, v in [('HOLD', _ev['ev_hold']),
                                          ('ROLL', _ev['ev_roll']),
                                          ('ASSIGN', _ev['ev_assign']),
                                          ('BUYBACK', _ev['ev_buyback'])])
                + f" → **{_ev['ev_winner']}** wins"
                + (f" [Timing adj: {'; '.join(_timing_notes)}]"
                   if _timing_notes else "")
            )
            logger.debug(
                f"[BW_v2] Timing quality: roll_adj={_timing_roll_adj:.2f}, "
                f"hold_bonus=${_timing_hold_bonus:,.0f}, "
                f"new_winner={_ev['ev_winner']}"
            )

        # Add EV-backed proposals for each action
        _ev_winner_action = _ev.get("ev_winner", "")

        # ASSIGN guard: don't propose ASSIGN when assignment is improbable.
        # At P(assign) < 20%, assignment is a fantasy outcome
        # and ASSIGN EV inflates from the large (strike - cost) gap.
        # Demote to HOLD: "let the call expire worthless, collect full premium."
        _p_assign_ev = abs(safe_row_float(row, 'MC_Assign_P_Expiry', 'Short_Call_Delta', 'Delta'))
        if _ev_winner_action == "ASSIGN" and _p_assign_ev < 0.20:
            _ev_winner_action = "HOLD"
            _ev["ev_winner"] = "HOLD"

        # Map internal ASSIGN label to user-facing label based on moneyness.
        # ASSIGN is an EV calculation scenario; the display label should tell the
        # user what to actually do.
        if _ev_winner_action == "ASSIGN":
            if delta >= 0.50:
                _ev_winner_action = "ACCEPT_CALL_AWAY"
            else:
                _ev_winner_action = "LET_EXPIRE"
            _ev["ev_winner"] = _ev_winner_action
            # Update summary text so user sees the mapped label, not internal "ASSIGN"
            _ev["ev_summary"] = _ev["ev_summary"].replace("**ASSIGN**", f"**{_ev_winner_action}**").replace("ASSIGN ", f"{_ev_winner_action} ")

        # ROLL guard: don't propose ROLL when thesis blocks it.
        # McMillan Ch.3: "Don't roll a broken thesis — rolling amplifies the loss."
        # Exception: deep ITM (delta >= 0.70) — ROLL is defensive (preventing
        # assignment), not speculative. Thesis degradation doesn't invalidate
        # the need to defend against assignment.
        if _ev_winner_action == "ROLL" and _thesis_blocks_roll and delta < DELTA_ITM_EMERGENCY:
            _ev_winner_action = "HOLD"
            _ev["ev_winner"] = "HOLD"

        _ASSIGN_LABELS = ("LET_EXPIRE", "ACCEPT_CALL_AWAY")
        for action_name, ev_key in [("HOLD", "ev_hold"), ("ROLL", "ev_roll"),
                                     ("ASSIGN", "ev_assign"), ("BUYBACK", "ev_buyback")]:
            ev_val = _ev.get(ev_key, 0.0)
            # Map ASSIGN to user-facing label for the winning proposal
            if action_name == "ASSIGN" and _ev_winner_action in _ASSIGN_LABELS:
                propose_gate(
                    collector, f"ev_comparator_{_ev_winner_action.lower()}",
                    action=_ev_winner_action, urgency="MEDIUM",
                    rationale=(
                        f"EV decision: {_ev_winner_action} wins with ${ev_val:,.0f} over "
                        f"{max(dte, 1):.0f}d. {_ev['ev_summary']}"
                    ),
                    doctrine_source=f"ActionEV: {_ev_winner_action} winner (BW)",
                    priority=70, ev_estimate=ev_val,
                )
            elif action_name == _ev_winner_action:
                propose_gate(
                    collector, f"ev_comparator_{action_name.lower()}",
                    action=action_name, urgency="MEDIUM",
                    rationale=(
                        f"EV decision: {action_name} wins with ${ev_val:,.0f} over "
                        f"{max(dte, 1):.0f}d. {_ev['ev_summary']}"
                    ),
                    doctrine_source=f"ActionEV: {action_name} winner (BW)",
                    priority=70, ev_estimate=ev_val,
                )
    except Exception as _ev_err:
        logger.debug(f"[BW_v2] Action EV comparator skipped: {_ev_err}")

    # ── Default HOLD LOW (always present) ─────────────────────────────────
    propose_gate(
        collector, "default_hold",
        action="HOLD", urgency="LOW",
        rationale=(
            f"Call OTM, DTE={dte:.0f} — theta working as intended."
            f"{_iv_annotation} No action required (McMillan Ch.2)."
        ),
        doctrine_source="McMillan Ch.2: Neutral Maintenance",
        priority=100,
    )

    # ── Resolution ────────────────────────────────────────────────────────
    logger.debug(f"[BW_v2] {collector.summary()}")

    if collector.has_hard_veto():
        winner = collector.get_veto()
        resolved = collector.to_result(winner, result, resolution_method="HARD_VETO")
    elif ev_result is not None:
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

    # ── Post-resolution far-OTM guard ──────────────────────────────────────
    # When the short call is far OTM with negligible assignment probability,
    # the position is working as designed (collecting theta). Don't let the
    # EV comparator or individual gates override this with ROLL/EXIT.
    # Only a CAPITAL EXIT (hard stop, underlying collapse) can override.
    _post_delta = delta
    _post_p_assign = abs(safe_row_float(row, 'MC_Assign_P_Expiry'))
    _post_p_assign_valid = not (isinstance(_post_p_assign, float) and _post_p_assign != _post_p_assign)
    # Far-OTM only applies when:
    # - equity structure is intact (BROKEN = compromised, EXIT/ROLL warranted)
    # - DTE > 21 (at ≤21 DTE, income management needed regardless of delta)
    _post_far_otm = (
        _post_delta < 0.30
        and (_post_p_assign < 0.05 if _post_p_assign_valid else _post_delta < 0.25)
        and _ei_state != 'BROKEN'
        and dte > DTE_INCOME_GATE
    )

    if _post_far_otm and resolved.get('Action') not in ('HOLD', 'EXIT'):
        resolved['Action'] = 'HOLD'
        resolved['Urgency'] = 'LOW'
        resolved['Rationale'] = (
            f"Far-OTM override: short call Δ {_post_delta:.3f} with P(assign) "
            f"{'< 5%' if _post_p_assign_valid else 'negligible (delta proxy)'}. "
            f"Position is collecting θ as designed — no action needed. "
            f"Original proposal ({winner.action} {winner.urgency}) suppressed. "
            f"(McMillan Ch.3: far-OTM covered write is pure income collection; "
            f"only structural capital damage warrants intervention.)"
        )
        resolved['Resolution_Method'] = 'FAR_OTM_OVERRIDE'
    elif _post_far_otm and resolved.get('Action') == 'EXIT' and resolved.get('Exit_Trigger_Type') != 'CAPITAL':
        resolved['Action'] = 'HOLD'
        resolved['Urgency'] = 'LOW'
        resolved['Rationale'] = (
            f"Far-OTM override: EXIT suppressed — call Δ {_post_delta:.3f}, "
            f"P(assign) negligible. Non-CAPITAL EXIT not justified when "
            f"position is structurally sound. (McMillan Ch.3)"
        )
        resolved['Resolution_Method'] = 'FAR_OTM_OVERRIDE'

    # ── Recovery state tagging ──────────────────────────────────────────────
    # If recovery mode is active, tag the position regardless of which HOLD
    # gate won resolution. The resolver may pick equity_broken or theta-covers-gap
    # over the recovery ladder (higher urgency), but the position is still in
    # recovery — MC EXIT_NOW guard in run_all.py must respect this.
    _has_recovery_proposal = any(
        p.gate_name == "hard_stop_recovery_ladder" for p in collector.proposals
    )
    if _has_recovery_proposal and resolved.get('Action') == 'HOLD':
        resolved['Doctrine_State'] = 'RECOVERY_LADDER'
        if resolved.get('Resolution_Method') not in ('HARD_VETO',):
            resolved['Resolution_Method'] = 'RECOVERY_LADDER'

    # ── Non-blocking annotations ──────────────────────────────────────────
    rationale = resolved.get('Rationale', '')

    if _underlying_health_annotation:
        rationale = _underlying_health_annotation + rationale

    if _leaps_earnings_note:
        rationale += _leaps_earnings_note

    if _cadence_annotation:
        rationale += _cadence_annotation

    if _persistence_annotation:
        rationale += _persistence_annotation
        # Persistence escalation: adjust urgency for sustained adverse drift
        # Exempt far-OTM positions — sustained drift below cost basis doesn't
        # threaten the short call when delta < 0.30 (McMillan Ch.3).
        # Use delta-only check (no DTE gate) — even near expiration, a far-OTM
        # call doesn't warrant CRITICAL urgency from drift alone.
        _persist_far_otm = (
            _post_delta < 0.30
            and (_post_p_assign < 0.05 if _post_p_assign_valid else _post_delta < 0.25)
        )
        if (row.get('Drift_Persistence') == 'Sustained'
                and resolved.get('Action') not in ('HOLD', 'ASSIGN', 'LET_EXPIRE', 'ACCEPT_CALL_AWAY')
                and not _persist_far_otm):
            if drift_from_net > 0:
                resolved['Urgency'] = 'HIGH'
                resolved['Exit_Trigger_Type'] = 'INCOME'
            else:
                # DTE guard: OTM call with ample time → cap at HIGH.
                # CRITICAL only when DTE ≤ 21 or ITM (McMillan Ch.3).
                # Note: Do NOT inject CAPITAL here — the EV comparator already
                # resolved the best action. Persistence escalates urgency only;
                # CAPITAL bypass would override the EV authority principle.
                if dte > 21 and _post_delta < 0.50:
                    resolved['Urgency'] = 'HIGH'
                else:
                    resolved['Urgency'] = 'CRITICAL'

    if _thesis_annotation:
        rationale += _thesis_annotation
        if resolved.get('Urgency', 'LOW') == 'LOW':
            resolved['Urgency'] = 'MEDIUM'

    if _signal_hub_bw_notes:
        rationale += _signal_hub_bw_notes

    if _ei_state == 'WEAKENING' and _ei_reason:
        rationale += f"  [⚠️ Equity WEAKENING: {_ei_reason} — monitor.]"

    # Forward expectancy context annotation
    _ev_ratio = safe_row_float(row, 'EV_Feasibility_Ratio') if pd.notna(row.get('EV_Feasibility_Ratio')) else float('nan')
    _em = safe_row_float(row, 'Expected_Move_10D')
    _req = safe_row_float(row, 'Required_Move_Breakeven', 'Required_Move')
    if not pd.isna(_ev_ratio) and _ev_ratio > 0 and _em > 0:
        if _ev_ratio < 0.5:
            rationale += (
                f" ⚠️ Strike proximity: stock only ${_req:.1f} away from strike "
                f"({_ev_ratio:.2f}× 10D expected move ${_em:.1f}). "
                f"Assignment risk is elevated — consider rolling up/out proactively "
                f"(Passarelli Ch.5: roll before ITM, not after)."
            )

    resolved['Rationale'] = rationale

    return resolved
