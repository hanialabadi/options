import pandas as pd
import numpy as np
import logging
import os
from typing import Dict, Any
from core.shared.data_layer.technical_data_repository import get_latest_technical_indicators
from core.shared.data_layer.market_stress_detector import classify_market_stress, should_halt_trades, get_halt_reason
from scan_engine.loaders.schwab_api_client import SchwabClient # Assuming SchwabClient is available

from core.management.cycle1.identity.constants import (
    STRATEGY_COVERED_CALL,
    STRATEGY_BUY_WRITE,
    STRATEGY_BUY_CALL,
    STRATEGY_BUY_PUT,
    STRATEGY_LEAPS_CALL,
    STRATEGY_LEAPS_PUT,
    STRATEGY_CSP,
    STRATEGY_LONG_STRADDLE,
    STRATEGY_LONG_STRANGLE,
    STRATEGY_UNKNOWN,
    STRATEGY_STOCK,
    FIDELITY_MARGIN_RATE,
    FIDELITY_MARGIN_RATE_DAILY,
)
from core.management.cycle3.decision.resolver import StrategyResolver

logger = logging.getLogger(__name__)

# Decision States
STATE_ACTIONABLE = "ACTIONABLE"
STATE_NEUTRAL_CONFIDENT = "NEUTRAL_CONFIDENT"
STATE_UNCERTAIN = "UNCERTAIN"
STATE_BLOCKED_GOVERNANCE = "BLOCKED_GOVERNANCE"
STATE_UNRESOLVED_IDENTITY = "UNRESOLVED_IDENTITY"

# Uncertainty Reasons
REASON_ATTRIBUTION_QUALITY_LOW = "ATTRIBUTION_QUALITY_LOW"
REASON_IV_AUTHORITY_MISSING = "IV_AUTHORITY_MISSING"
REASON_SCHWAB_IV_EXPIRED = "SCHWAB_IV_EXPIRED"
REASON_DELTA_GAMMA_INCOMPLETE = "DELTA_GAMMA_INCOMPLETE"
REASON_STOCK_LEG_NOT_AVAILABLE = "STOCK_LEG_NOT_AVAILABLE"
REASON_CYCLE2_SIGNAL_INCOMPLETE = "CYCLE2_SIGNAL_INCOMPLETE"
REASON_STOCK_AUTHORITY_VIOLATION = "STOCK_AUTHORITY_VIOLATION"
REASON_STRUCTURAL_DATA_INCOMPLETE = "STRUCTURAL_DATA_INCOMPLETE"

# Epistemic Authority Levels
AUTHORITY_REQUIRED = "REQUIRED"
AUTHORITY_CONTEXTUAL = "CONTEXTUAL"
AUTHORITY_SUPPORTIVE = "SUPPORTIVE"
AUTHORITY_NON_AUTHORITATIVE = "NON_AUTHORITATIVE"

# Strategy -> Stock Authority Mapping
STOCK_AUTHORITY_MAP = {
    'BUY_WRITE': AUTHORITY_REQUIRED,
    'COVERED_CALL': AUTHORITY_CONTEXTUAL,
    'CSP': AUTHORITY_NON_AUTHORITATIVE,
    'BUY_CALL': AUTHORITY_NON_AUTHORITATIVE,
    'BUY_PUT': AUTHORITY_NON_AUTHORITATIVE,
    'LONG_CALL': AUTHORITY_NON_AUTHORITATIVE,
    'LONG_PUT': AUTHORITY_NON_AUTHORITATIVE,
    'LEAPS_CALL': AUTHORITY_NON_AUTHORITATIVE,
    'LEAPS_PUT': AUTHORITY_NON_AUTHORITATIVE,
    'STRADDLE': AUTHORITY_NON_AUTHORITATIVE,
    'STRANGLE': AUTHORITY_NON_AUTHORITATIVE,
    'STOCK_ONLY': AUTHORITY_REQUIRED,
    'STOCK_ONLY_IDLE': AUTHORITY_REQUIRED,   # idle stock: no option leg yet
    'UNKNOWN': AUTHORITY_NON_AUTHORITATIVE
}

# Strategy -> IV Authority Mapping
IV_AUTHORITY_MAP = {
    'BUY_WRITE': AUTHORITY_SUPPORTIVE,
    'COVERED_CALL': AUTHORITY_SUPPORTIVE,
    'CSP': AUTHORITY_SUPPORTIVE,
    'BUY_CALL': AUTHORITY_SUPPORTIVE,
    'BUY_PUT': AUTHORITY_SUPPORTIVE,
    'LONG_CALL': AUTHORITY_SUPPORTIVE,
    'LONG_PUT': AUTHORITY_SUPPORTIVE,
    'LEAPS_CALL': AUTHORITY_SUPPORTIVE,
    'LEAPS_PUT': AUTHORITY_SUPPORTIVE,
    'STRADDLE': AUTHORITY_SUPPORTIVE,
    'STRANGLE': AUTHORITY_SUPPORTIVE,
    'STOCK_ONLY': AUTHORITY_NON_AUTHORITATIVE,
    'STOCK_ONLY_IDLE': AUTHORITY_NON_AUTHORITATIVE,
    'UNKNOWN': AUTHORITY_NON_AUTHORITATIVE
}

def _build_journey_note(row: pd.Series, current_action: str, ul_last: float) -> str:
    """
    Build a compact trade journey note for inclusion in any rationale string.

    Reads the Prior_* columns injected by run_all.py (2.95 Journey Context block)
    and returns a one-liner that tells the continuous story of the trade so far,
    with RAG book citations for each transition type.

    Returns empty string if no prior context is available (first run or DB failure).

    RAG grounding:
      EXIT→HOLD  — McMillan Ch.4: "If price pulls back after a profit-take signal,
                   re-evaluate: is the pullback a retracement or a new downtrend?"
      ROLL→HOLD  — Passarelli Ch.6: "Don't force a roll into a choppy market —
                   pre-stage and wait for directional clarity."
      HOLD n-day — Passarelli Ch.5: "Patience while theta works is a position,
                   not a lapse in management."
      HOLD→EXIT  — Natenberg Ch.11: "Sustained LATE_CYCLE + rising hold cost =
                   edge is being consumed. Escalate."
    """
    prior_action  = str(row.get("Prior_Action")  or "").strip().upper()
    prior_price   = row.get("Prior_UL_Last")
    prior_ts      = row.get("Prior_Snapshot_TS")
    days_ago      = row.get("Prior_Days_Ago")

    if not prior_action or prior_action in ("", "NONE", "NAN"):
        return ""   # first run — no history

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
        _dir = "↑" if _delta_pct > 0 else "↓"
        _price_note = f" (stock {_dir}{abs(_delta_pct):.1%} since then: ${_pp:.2f} → ${ul_last:.2f})"
    except (TypeError, ValueError):
        pass

    # Transition-aware citation
    flip = f"{prior_action}→{current_action}"
    if prior_action == "EXIT" and current_action == "HOLD":
        cite = (
            "Exit signal not acted on — stock retraced. "
            "Re-evaluate: retracement or new downtrend? "
            "(McMillan Ch.4: re-entry after failed exit)"
        )
    elif prior_action == "EXIT" and current_action == "EXIT":
        cite = "Exit signal persists — urgency confirmed (McMillan Ch.4)."
    elif prior_action == "ROLL" and current_action == "HOLD":
        cite = (
            "Roll blocked by timing gate — pre-staged candidates ready when market clarifies "
            "(Passarelli Ch.6: don't force a roll into choppy market)."
        )
    elif prior_action == "HOLD" and current_action == "HOLD":
        try:
            _held_days = float(days_ago) if days_ago else 0
            # Find total hold streak by checking if prior was also HOLD — approximated by days_ago
            cite = f"Holding {_days_str} — thesis monitoring continues (Passarelli Ch.5: patience while theta works)."
        except (TypeError, ValueError):
            cite = "Holding — thesis monitoring continues (Passarelli Ch.5)."
    elif prior_action == "HOLD" and current_action == "EXIT":
        cite = "Condition deteriorated since last HOLD — escalating to EXIT (Natenberg Ch.11: edge consumed)."
    elif prior_action == "HOLD" and current_action == "ROLL":
        cite = "Timing gate cleared — executing pre-staged roll (Passarelli Ch.6)."
    elif prior_action == "TRIM" and current_action == "HOLD":
        cite = "Partial trim executed — holding remaining position (McMillan Ch.4: scale out, not all-or-nothing)."
    elif current_action == "BUYBACK":
        cite = "Carry inversion detected — buy back short call, hold stock unencumbered (Given Ch.6 + Jabbour Ch.11)."
    elif prior_action == "BUYBACK" and current_action == "HOLD":
        cite = "Short call bought back — holding stock only until structure resolves (McMillan Ch.3: uncap position)."
    else:
        cite = f"Prior: {prior_action} → Now: {current_action}."

    return f"📖 Journey ({_ts_str}, {_days_str}): Prior signal was **{prior_action}**{_price_note}. {cite}"


class DoctrineAuthority:
    """
    Cycle 3: Doctrine Authority Layer.
    """
    
    _REGISTERED_DOCTRINES = [
        'BUY_WRITE', 'COVERED_CALL', 'BUY_CALL', 'BUY_PUT', 'LONG_CALL', 'LONG_PUT',
        'CSP', 'STRADDLE', 'STRANGLE', 'STOCK_ONLY', 'STOCK_ONLY_IDLE',
        'LEAPS_CALL', 'LEAPS_PUT'
    ]

    @staticmethod
    def evaluate(row: pd.Series) -> Dict[str, Any]:
        # RAG: Strategy Normalization. Ensure we use uppercase for all comparisons.
        strategy_raw = row.get('Strategy', 'UNKNOWN')
        strategy = str(strategy_raw).upper().replace(' ', '_')
        
        stock_auth = STOCK_AUTHORITY_MAP.get(strategy, AUTHORITY_NON_AUTHORITATIVE)
        iv_auth = IV_AUTHORITY_MAP.get(strategy, AUTHORITY_NON_AUTHORITATIVE)

        # Default State (Neutral Confidence)
        result = {
            "Action": "HOLD",
            "Urgency": "LOW",
            "Rationale": "Position within sensitivity envelope. No doctrinal triggers met.",
            "Doctrine_Source": "McMillan: Neutrality",
            "Authoritative_Strategy": strategy,
            "Stock_Authority": stock_auth,
            "IV_Authority": iv_auth,
            "Doctrine_State": "VALID_HOLD",
            "Decision_State": STATE_NEUTRAL_CONFIDENT,
            "Uncertainty_Reasons": [],
            "Missing_Data_Fields": [],
            "Required_Conditions_Met": True,
            "Doctrine_Trace": f"Strategy: {strategy} | Rule: Default Neutrality",
            "Pyramid_Tier": int(row.get('Pyramid_Tier', 0) or 0),
            "Winner_Lifecycle": str(row.get('Winner_Lifecycle', 'THESIS_UNPROVEN') or 'THESIS_UNPROVEN'),
        }

        # Guard: Expired option — data hasn't been refreshed yet by broker.
        # DTE=0 with AssetType=OPTION means the position expired yesterday (or today at close).
        # Doctrine cannot evaluate a dead leg — surface a clear status instead.
        asset_type = str(row.get('AssetType', '') or '').upper()
        dte_raw = row.get('DTE')
        dte_val = float(dte_raw) if pd.notna(dte_raw) else 999.0
        if asset_type == 'OPTION' and dte_val <= 0:
            _exp_strategy = str(row.get('Strategy', '') or '').upper()
            _is_csp_expired = _exp_strategy in ('CSP', 'SHORT_PUT', 'CASH_SECURED_PUT')
            # For expired CSPs: run wheel assessment before returning so the display
            # layer can show Wheel_Ready state and next-step guidance (sell covered call).
            # Passarelli Ch.1: the wheel cycle is CSP → assignment → CC → repeat.
            if _is_csp_expired:
                result = DoctrineAuthority._short_put_doctrine(row, result)
                # Overwrite the action/rationale (doctrine may have set ROLL/EXIT)
                # with the settled state — but preserve the wheel columns it computed.
                _wheel_note_exp = str(result.get('Wheel_Note', '') or '')
                _wheel_ready_exp = result.get('Wheel_Ready', False)
                _next_step = (
                    "Wheel Ready — on assignment, sell covered call at/above basis. "
                    if _wheel_ready_exp else
                    "Not wheel-ready — evaluate assignment vs. close after settlement."
                )
                result.update({
                    "Action": "AWAITING_SETTLEMENT",
                    "Urgency": "LOW",
                    "Rationale": (
                        f"Option expired (DTE={dte_val:.0f}). Awaiting broker settlement — "
                        f"position will be removed or assigned in next data refresh. "
                        f"{_next_step}"
                    ),
                    "Doctrine_Source": "System: Expiration Settlement",
                    "Decision_State": STATE_NEUTRAL_CONFIDENT,
                    "Required_Conditions_Met": True,
                })
            else:
                result.update({
                    "Action": "AWAITING_SETTLEMENT",
                    "Urgency": "LOW",
                    "Rationale": (
                        f"Option expired (DTE={dte_val:.0f}). Awaiting broker settlement — "
                        f"position will be removed or assigned in next data refresh."
                    ),
                    "Doctrine_Source": "System: Expiration Settlement",
                    "Decision_State": STATE_NEUTRAL_CONFIDENT,
                    "Required_Conditions_Met": True,
                })
            return result

        # RAG: Epistemic Strictness.
        # If critical data is missing, we cannot evaluate doctrine.
        hv = row.get('HV_20D')
        if pd.isna(hv) or hv == 0:
            result.update({
                "Rationale": "Action refused: Missing critical volatility data (HV_20D).",
                "Decision_State": STATE_UNCERTAIN,
                "Uncertainty_Reasons": ["MISSING_HV_20D"],
                "Required_Conditions_Met": False
            })
            return result

        if strategy == "UNKNOWN":
            result.update({
                "Rationale": "Doctrine Not Bound: Unknown strategy type.",
                "Decision_State": STATE_UNRESOLVED_IDENTITY,
                "Required_Conditions_Met": False
            })
            return result

        # ── Condition Monitor integration ────────────────────────────────────
        # _Condition_Resolved / _Active_Conditions are written by ConditionMonitor
        # before doctrine runs. Two authority levels:
        #
        # (A) RESOLVED conditions: a condition just cleared this run → override to
        #     resolved Action (e.g., dead_cat_bounce resolved → unlock ROLL).
        #     Strategy doctrine still runs after, but only emergency triggers can
        #     override a resolved ROLL back to something else.
        #
        # (B) ACTIVE conditions: ongoing monitoring state. If a blocking condition
        #     type is active (dead_cat_bounce, iv_depressed), non-emergency ROLL
        #     triggers inside strategy doctrine must downgrade to HOLD.
        #     Emergency triggers (hard stop, DTE<7, delta>0.70) still bypass this.
        #
        # This is the "authoritative" layer: conditions gate Action, not just annotate.
        _resolved_note   = str(row.get('_Condition_Resolved', '') or '')
        _resolved_action = str(row.get('_Resolved_Action', '') or '')
        _resolved_urgency = str(row.get('_Resolved_Urgency', '') or '')
        _active_note     = str(row.get('_Active_Conditions', '') or '')

        # Parse active condition types for fast lookup
        _blocking_active = set()
        for _part in _active_note.split(' | '):
            _ctype = _part.split('[')[0].strip()
            if _ctype:
                _blocking_active.add(_ctype.lower())

        # Blocking condition types: prevent non-emergency discretionary rolls
        _ROLL_BLOCKING_CONDITIONS = {'dead_cat_bounce', 'iv_depressed'}
        _condition_blocks_roll = bool(_blocking_active & _ROLL_BLOCKING_CONDITIONS)

        if _resolved_note:
            result['Rationale'] = f"[Monitor resolved] {_resolved_note}"
            if _resolved_action:
                result['Action'] = _resolved_action
                result['Urgency'] = _resolved_urgency or 'LOW'
                result['Decision_State'] = STATE_ACTIONABLE if _resolved_action != 'HOLD' else STATE_NEUTRAL_CONFIDENT

        if _active_note and not _resolved_note:
            _block_tag = " ⛔ ROLL blocked by active condition." if _condition_blocks_roll else ""
            result['Rationale'] = f"Monitoring: {_active_note}{_block_tag}"

        # Stamp blocking flag and resolution origin onto result dict.
        # _resolved_by_condition = True means the initial Action came from a condition
        # resolution, not from doctrine's own structural evaluation.  Strategy doctrine
        # MUST still run all structural gates — emergency exits can override — but the
        # final action (if still ROLL when no gate fired independently) must be passed
        # through the timing gate before returning.  This prevents condition resolution
        # from short-cutting to ROLL in a choppy/still-declining market.
        result['_condition_blocks_roll'] = _condition_blocks_roll
        result['_resolved_by_condition'] = bool(_resolved_note and _resolved_action == 'ROLL')

        # ── Thesis State Gate ────────────────────────────────────────────────
        # Layer 0: before any strategy doctrine runs, check if the underlying
        # company thesis is still valid.
        #
        # BROKEN  → block all discretionary rolls; escalate to EXIT consideration.
        #           Hard stop, DTE<7, and delta>0.70 emergencies still override
        #           by returning early from within strategy doctrine.
        # DEGRADED → roll with caution; downgrade urgency; annotate rationale.
        # INTACT / UNKNOWN → no intervention.
        #
        # Passarelli Ch.2: "The story check is the first question, not the last."
        # McMillan Ch.3: "Don't roll a broken thesis — rolling amplifies the loss."
        _thesis_state    = str(row.get('Thesis_State', '') or '').upper()
        _thesis_summary  = str(row.get('Thesis_Summary', '') or '')
        _drawdown_type   = str(row.get('Thesis_Drawdown_Type', '') or '')

        # ── Sector Relative Strength modifier ─────────────────────────────────
        # If stock is underperforming its sector benchmark by ≥2σ (MICRO_BREAKDOWN
        # or BROKEN) while Thesis is INTACT, downgrade to DEGRADED.
        # Natenberg Ch.8: sector divergence at 2σ+ signals structural misalignment.
        # McMillan Ch.1: relative strength context prevents rolling into sector headwind.
        _srs = str(row.get('Sector_Relative_Strength', '') or '').upper()
        if _thesis_state == 'INTACT' and _srs in ('MICRO_BREAKDOWN', 'BROKEN'):
            _srs_bench = str(row.get('Sector_Benchmark', 'SPY') or 'SPY')
            _srs_z     = float(row.get('Sector_RS_ZScore', 0) or 0)
            _thesis_state   = 'DEGRADED'
            _drawdown_type  = _drawdown_type or 'MACRO'
            _thesis_summary = (
                f"Sector RS={_srs} (z={_srs_z:.2f} vs {_srs_bench}) — "
                f"stock underperforming sector by >{abs(_srs_z):.1f}σ. "
                f"Natenberg Ch.8: sector divergence at 2σ+ warrants caution."
            )

        if _thesis_state == 'BROKEN':
            result['Thesis_Gate'] = 'BLOCKED'
            result['Rationale'] = (
                f"⚠️ THESIS BROKEN — {_thesis_summary} "
                f"[Drawdown: {_drawdown_type}] "
                f"Rolls blocked. Evaluate exit vs thesis repair "
                f"(Passarelli Ch.2: story check fails)."
            )
            result['Action']         = 'HOLD'   # placeholder — strategy doctrine may escalate to EXIT
            result['Urgency']        = 'HIGH'
            result['Decision_State'] = STATE_UNCERTAIN
            # Let strategy doctrine still run — emergency gates can escalate to EXIT
            # We stamp the thesis block so doctrine knows to skip discretionary rolls
            result['_thesis_blocks_roll'] = True

        elif _thesis_state == 'DEGRADED':
            result['Thesis_Gate'] = 'CAUTION'
            result['_thesis_blocks_roll'] = False
            # Annotate but don't override — strategy doctrine decides action
            result['Rationale'] = (
                f"⚠️ Thesis DEGRADED ({_drawdown_type}): {_thesis_summary} "
            ) + result.get('Rationale', '')
        else:
            result['Thesis_Gate']       = 'PASS'
            result['_thesis_blocks_roll'] = False

        # ── OI Deterioration gate (Murphy 0.704) ──────────────────────────
        # "Declining OI in a held contract means the crowd is leaving;
        #  you may be the last one at the party."
        # Fires for ALL option strategies before strategy-specific doctrine.
        # Hard exits (OI < 25 or ratio < 0.25) skip strategy routing entirely
        # because rolling/holding is pointless without exit liquidity.
        _oi_current = float(row.get('Open_Int', 0) or 0)
        _oi_entry   = float(row.get('OI_Entry', 0) or 0)
        _oi_ratio   = _oi_current / _oi_entry if _oi_entry > 0 else 1.0
        _is_option_leg = str(row.get('AssetType', '') or '').upper() in ('OPTION', 'CALL', 'PUT')
        _oi_hard_exit = False

        if _is_option_leg and _oi_entry > 0:
            if _oi_current < 25:
                # Absolute floor: <25 OI = no viable exit market
                result.update({
                    "Action": "EXIT", "Urgency": "HIGH",
                    "Rationale": (
                        f"OI liquidity trap: current OI={_oi_current:.0f} < 25 "
                        f"(entry OI was {_oi_entry:.0f}). No viable exit market. "
                        f"Murphy: 'Declining OI = crowd leaving the position'"
                    ),
                    "Doctrine_Source": "Murphy: OI Deterioration — Absolute Floor",
                    "Decision_State": STATE_ACTIONABLE,
                    "Required_Conditions_Met": True,
                })
                _oi_hard_exit = True
            elif _oi_ratio < 0.25:
                # Severe: OI dropped >75% from entry
                result.update({
                    "Action": "EXIT", "Urgency": "MEDIUM",
                    "Rationale": (
                        f"OI deterioration severe: {_oi_current:.0f}/{_oi_entry:.0f} = "
                        f"{_oi_ratio:.0%} of entry OI. Liquidity draining — exit cost rising. "
                        f"Murphy: 'Volume precedes price; OI confirms commitment'"
                    ),
                    "Doctrine_Source": "Murphy: OI Deterioration — Severe (>75% decline)",
                    "Decision_State": STATE_ACTIONABLE,
                    "Required_Conditions_Met": True,
                })
                _oi_hard_exit = True
            elif _oi_ratio < 0.50:
                # Warning: OI halved — annotate but don't override
                result['OI_Deterioration_Warning'] = (
                    f"OI halved: {_oi_current:.0f}/{_oi_entry:.0f} = {_oi_ratio:.0%}. "
                    f"Monitor exit liquidity."
                )

        # ── Vol Stop gate (Given 0.677 / Bennett 0.719) ─────────────────
        # "If IV rises >50% from your entry IV, the trade's risk profile has
        #  fundamentally changed — the market is repricing risk against you."
        # Fires for SHORT premium strategies only (CSP, BUY_WRITE, COVERED_CALL).
        # Long-vol benefits from IV rise, so the gate is direction-aware.
        # AUDIT FIX: use IV_Now only (per-contract IV). Previous fallback to IV_30D
        # (underlying ATM IV) mixed per-contract and underlying IV — apples to oranges.
        # IV_Entry is also per-contract (frozen from IV_Now at entry).
        _iv_now_raw  = float(row.get('IV_Now', 0) or 0)
        _iv_entry_raw = float(row.get('IV_Entry', 0) or 0)
        _vol_stop_exit = False
        _SHORT_VOL_STRATEGIES = {'BUY_WRITE', 'COVERED_CALL', 'CSP'}

        if (_is_option_leg and _iv_entry_raw > 0 and _iv_now_raw > 0
                and strategy in _SHORT_VOL_STRATEGIES):
            _iv_rise_pct = (_iv_now_raw - _iv_entry_raw) / _iv_entry_raw
            if _iv_rise_pct > 0.50:
                result['Vol_Stop_Warning'] = (
                    f"IV rose {_iv_rise_pct:.0%} from entry "
                    f"({_iv_entry_raw:.2%}→{_iv_now_raw:.2%}). "
                    f"Given: 'vol stop triggered — risk profile fundamentally changed'"
                )
                # Annotate but let strategy doctrine decide final action.
                # Vol stop fires as HIGH-urgency advisory, not hard exit,
                # because the short premium may still be profitable (theta vs vega).
                result['Urgency'] = 'HIGH'
                result['Doctrine_Source'] = result.get('Doctrine_Source', '') or 'Given: Vol Stop (IV +50%)'

        # ── VRP Drift annotation (Bennett 0.719) ─────────────────────────
        # Volatility Risk Premium = IV - HV.  Track drift from entry baseline.
        _iv_30d   = float(row.get('IV_30D', 0) or 0)
        _hv_20d   = float(row.get('HV_20D', 0) or 0)
        _iv_30d_e = float(row.get('IV_30D_Entry', 0) or 0)
        _hv_20d_e = float(row.get('HV_20D_Entry', 0) or 0)

        if _iv_30d > 0 and _hv_20d > 0 and _iv_30d_e > 0 and _hv_20d_e > 0:
            _vrp_now   = _iv_30d - _hv_20d
            _vrp_entry = _iv_30d_e - _hv_20d_e
            _vrp_drift = _vrp_now - _vrp_entry
            result['VRP_Entry']  = round(_vrp_entry, 4)
            result['VRP_Now']    = round(_vrp_now, 4)
            result['VRP_Drift']  = round(_vrp_drift, 4)

        # Route to strategy-specific doctrine (skip if OI hard exit)
        if _oi_hard_exit:
            pass  # OI exit overrides all strategy doctrine
        elif strategy in ["BUY_WRITE"]:
            result = DoctrineAuthority._buy_write_doctrine(row, result)
        elif strategy in ["COVERED_CALL"]:
            result = DoctrineAuthority._covered_call_doctrine(row, result)
        elif strategy in ["BUY_CALL", "BUY_PUT", "LEAPS_CALL", "LEAPS_PUT", "LONG_CALL", "LONG_PUT"]:
            result = DoctrineAuthority._long_option_doctrine(row, result)
        elif strategy in ["CSP"]:
            result = DoctrineAuthority._short_put_doctrine(row, result)
        elif strategy in ["STRADDLE", "STRANGLE"]:
            result = DoctrineAuthority._multi_leg_doctrine(row, result)
        elif strategy in ["STOCK_ONLY"]:
            result = DoctrineAuthority._stock_only_doctrine(row, result)
        elif strategy in ["STOCK_ONLY_IDLE"]:
            result = DoctrineAuthority._stock_only_idle_doctrine(row, result)
        else:
            # Unrecognized strategy — no doctrine binding available
            result['Rationale'] = (
                f"Unrecognized strategy '{strategy}' — no doctrine binding. "
                f"Defaulting to HOLD with diagnostic annotation. "
                f"Review strategy classification for ticker={row.get('Underlying_Ticker')}."
            )
            result['Uncertainty_Reasons'] = result.get('Uncertainty_Reasons', []) + [
                f"UNRECOGNIZED_STRATEGY:{strategy}"
            ]
            result['Decision_State'] = STATE_UNRESOLVED_IDENTITY
            logger.warning(
                f"[DoctrineAuthority] No doctrine for strategy='{strategy}', "
                f"ticker={row.get('Underlying_Ticker')}, tradeID={row.get('TradeID')}"
            )

        # ── Post-doctrine timing gate for condition-resolved ROLLs ────────────
        # When a condition resolves (dead_cat_bounce cleared, iv_depressed lifted)
        # and the resolution suggested ROLL, strategy doctrine has now run ALL its
        # structural gates — emergency exits (hard stop, DTE<7, delta>0.70) have
        # already returned early and bypassed this block.
        #
        # What remains: a ROLL that survived all structural checks but originated
        # from a condition resolution, not from an independent doctrinal trigger.
        # We must still pass it through the timing gate:
        #   - Structural recovery cleared the bounce watch → regime shifted
        #   - But "regime shifted" ≠ "roll now" — timing still matters
        #   - If market is still choppy (WAIT), hold with candidates pre-staged
        #   - If timing is favorable, confirm the ROLL with urgency from timing
        #
        # Doctrine-triggered ROLLs (50% capture, ITM, DTE<14) already applied
        # timing in-line; only resolution-originated ROLLs reach here un-gated.
        if result.get('_resolved_by_condition') and result.get('Action') == 'ROLL':
            _post_timing = DoctrineAuthority._classify_roll_timing(row)
            if _post_timing['action_mod'] == 'WAIT':
                result.update({
                    "Action": "HOLD",
                    "Urgency": "LOW",
                    "Rationale": (
                        result.get('Rationale', '') +
                        f" ⚠️ Condition resolved but market timing still unfavorable — "
                        f"{_post_timing['reason']} "
                        f"Roll candidates pre-staged; execute when directional clarity confirmed "
                        f"(Passarelli Ch.6: resolution unlocks roll, timing authorizes it)."
                    ),
                    "Decision_State": STATE_NEUTRAL_CONFIDENT,
                })
            elif _post_timing['action_mod'] == 'ROLL_NOW':
                # Timing confirms — upgrade urgency and note the signal
                result['Urgency'] = _post_timing['urgency_mod']
                result['Rationale'] = (
                    result.get('Rationale', '') + f" {_post_timing['reason']}"
                )
                _adv = _post_timing.get('intraday_advisory')
                result['Intraday_Advisory_JSON'] = (
                    __import__('json').dumps(_adv) if _adv else ""
                )

        return result

    @staticmethod
    def _safe_pnl_pct(row: pd.Series):
        """Read P&L % with fallback: Total_GL_Decimal → PnL_Total/Basis → None.
        Returns None when P&L data is truly unavailable, allowing callers to
        distinguish 'no data' from 'breakeven' and skip gates accordingly."""
        gl = row.get('Total_GL_Decimal')
        if pd.notna(gl):
            return float(gl)
        pnl_total = row.get('PnL_Total')
        basis = row.get('Basis')
        if pd.notna(pnl_total) and pd.notna(basis) and abs(float(basis or 0)) > 0:
            return float(pnl_total) / abs(float(basis))
        return None

    @staticmethod
    def _stock_only_doctrine(row: pd.Series, result: Dict) -> Dict:
        """
        Doctrine for STOCK_ONLY positions — shares held with no option overlay.

        Gate order (first match returns):
          1. Equity BROKEN → EXIT HIGH (structural breakdown — Natenberg Ch.8)
          2. Deep Loss (≤-50%) → EXIT HIGH (capital preservation — McMillan Ch.1)
          3. Significant Loss (≤-25%) → HOLD HIGH (pure directional risk — Passarelli Ch.6)
          4. WEAKENING + Loss (<-10%) → HOLD MEDIUM (early deterioration — Natenberg Ch.8)
          5. CC Opportunity (≥100 shares, not BROKEN, loss < 25%) → HOLD LOW + CC note
          6. Default → HOLD LOW
        """
        ticker = str(row.get("Underlying_Ticker") or row.get("Symbol") or "ticker")
        qty = float(row.get("Quantity") or row.get("Qty") or 0)
        shares_label = f"{int(qty):,} shares" if qty > 0 else "shares"

        pnl_pct = DoctrineAuthority._safe_pnl_pct(row)
        pnl_dollars = float(row.get("PnL_Total", 0) or 0) if pd.notna(row.get("PnL_Total")) else None
        ei_state = str(row.get("Equity_Integrity_State", "") or "").strip()
        ei_reason = str(row.get("Equity_Integrity_Reason", "") or "").strip()

        pnl_str = f"{pnl_pct:+.1%}" if pnl_pct is not None else "N/A"
        pnl_dollar_str = f"${pnl_dollars:+,.0f}" if pnl_dollars is not None else ""

        # Gate 1: Equity BROKEN — structural breakdown is more fundamental than any P&L threshold
        if ei_state == "BROKEN":
            result.update({
                "Action": "EXIT",
                "Urgency": "HIGH",
                "Rationale": (
                    f"🔴 Stock {ticker} ({shares_label}) — Equity Integrity BROKEN: {ei_reason}. "
                    f"P&L: {pnl_str} {pnl_dollar_str}. "
                    f"Structural breakdown on stock with no option hedge — full downside exposure. "
                    f"(Natenberg Ch.8: structural breakdown is not cyclical; "
                    f"McMillan Ch.1: capital preservation supersedes recovery hope)"
                ),
                "Doctrine_Source": "Natenberg Ch.8 + McMillan Ch.1: BROKEN Equity — EXIT",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True,
            })
            return result

        # Gate 2: Deep loss stop — capital preservation
        if pnl_pct is not None and pnl_pct <= -0.50:
            result.update({
                "Action": "EXIT",
                "Urgency": "HIGH",
                "Rationale": (
                    f"🔴 Stock {ticker} ({shares_label}) — deep loss {pnl_str} {pnl_dollar_str}. "
                    f"Equity state: {ei_state or 'UNKNOWN'}. "
                    f"No theta cushion, no hedge — pure directional risk at >50% drawdown. "
                    f"(McMillan Ch.1: capital preservation supersedes recovery hope; "
                    f"Passarelli Ch.6: unhedged stock at deep loss = sunk cost trap)"
                ),
                "Doctrine_Source": "McMillan Ch.1: Deep Loss Stop — EXIT",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True,
            })
            return result

        # Gate 3: Significant loss — elevated monitoring
        if pnl_pct is not None and pnl_pct <= -0.25:
            result.update({
                "Action": "HOLD",
                "Urgency": "HIGH",
                "Rationale": (
                    f"⚠️ Stock {ticker} ({shares_label}) — significant loss {pnl_str} {pnl_dollar_str}. "
                    f"Equity state: {ei_state or 'UNKNOWN'}. "
                    f"No theta cushion = pure directional risk. Monitor for further deterioration. "
                    f"(Passarelli Ch.6: unhedged stock beyond -25% needs active review)"
                ),
                "Doctrine_Source": "Passarelli Ch.6: Significant Loss — HOLD HIGH",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True,
            })
            return result

        # Gate 4: WEAKENING equity + moderate loss
        if ei_state == "WEAKENING" and pnl_pct is not None and pnl_pct < -0.10:
            result.update({
                "Action": "HOLD",
                "Urgency": "MEDIUM",
                "Rationale": (
                    f"⚠️ Stock {ticker} ({shares_label}) — WEAKENING equity at {pnl_str} {pnl_dollar_str}. "
                    f"Reason: {ei_reason}. "
                    f"Early deterioration signals — watch for further breakdown. "
                    f"(Natenberg Ch.8: WEAKENING = early structural warning)"
                ),
                "Doctrine_Source": "Natenberg Ch.8: WEAKENING + Loss — HOLD MEDIUM",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True,
            })
            return result

        # Gate 5: CC opportunity — idle stock earns zero theta
        if qty >= 100 and ei_state != "BROKEN" and (pnl_pct is None or pnl_pct > -0.25):
            result.update({
                "Action": "HOLD",
                "Urgency": "LOW",
                "Rationale": (
                    f"📦 Stock {ticker} ({shares_label}) — P&L: {pnl_str}. "
                    f"Eligible for covered call overlay (≥100 shares, equity {ei_state or 'UNKNOWN'}). "
                    f"Idle stock earns zero theta — consider writing calls to generate income. "
                    f"(McMillan Ch.3: CC converts holding cost into income)"
                ),
                "Doctrine_Source": "McMillan Ch.3: CC Opportunity — HOLD LOW",
                "Decision_State": STATE_NEUTRAL_CONFIDENT,
                "Required_Conditions_Met": True,
            })
            return result

        # Gate 6: Default — no actionable signal
        result.update({
            "Action": "HOLD",
            "Urgency": "LOW",
            "Rationale": (
                f"📦 Stock {ticker} ({shares_label}) — P&L: {pnl_str}. "
                f"Equity state: {ei_state or 'UNKNOWN'}. "
                f"No doctrinal triggers. "
                f"(McMillan Ch.1: stock position within normal parameters)"
            ),
            "Doctrine_Source": "McMillan Ch.1: Neutrality",
            "Decision_State": STATE_NEUTRAL_CONFIDENT,
            "Required_Conditions_Met": True,
        })
        return result

    @staticmethod
    def _stock_only_idle_doctrine(row: pd.Series, result: Dict) -> Dict:
        """
        Doctrine for idle stock positions (STOCK_ONLY_IDLE) — shares held with no
        covered call written against them.

        The CC opportunity engine (cc_opportunity_engine.py) runs post-doctrine and
        writes CC_Proposal_Status / CC_Proposal_Verdict / CC_Candidate_* columns.
        Doctrine here sets the baseline action and urgency; the CC panel in
        manage_view.py surfaces the opportunity details.

        Logic:
          - If CC_Proposal_Status already set (engine ran before doctrine — unlikely
            ordering, but defensive): use it to calibrate urgency.
          - Otherwise: set HOLD/MEDIUM with a forward-looking rationale that tells
            the user the CC evaluation is pending (will be filled post-doctrine).

        McMillan Ch.3: idle long stock is uncapped upside — only write calls when
        the income opportunity clearly justifies the cap risk.
        """
        ticker = str(row.get("Underlying_Ticker") or row.get("Symbol") or "ticker")
        qty    = float(row.get("Quantity") or row.get("Qty") or 0)
        shares_label = f"{int(qty):,} shares" if qty > 0 else "shares"

        # C3 audit fix: Equity_Integrity BROKEN guard — block CC proposals on structurally broken stock.
        # Natenberg Ch.8: "Never sell calls against a stock in structural breakdown — you cap upside
        # on a bounce while leaving full downside exposure." McMillan Ch.3: "CC thesis requires the
        # stock to be stable or range-bound — a BROKEN structure invalidates the premise."
        # If the stock has 3+ signals broken (MA bear, momentum, drawdown, vol regime), selling a CC
        # against it is not income generation — it's selling a cap on a falling asset.
        _idle_ei_state  = str(row.get('Equity_Integrity_State', '') or '').strip()
        _idle_ei_reason = str(row.get('Equity_Integrity_Reason', '') or '').strip()
        if _idle_ei_state == 'BROKEN':
            result.update({
                "Action": "HOLD",
                "Urgency": "HIGH",
                "Rationale": (
                    f"📦 Idle stock: {ticker} ({shares_label}) — BLOCKED from CC evaluation. "
                    f"Equity Integrity BROKEN: {_idle_ei_reason}. "
                    f"Do NOT sell covered calls against a structurally declining stock — "
                    f"you cap recovery upside while retaining full downside exposure. "
                    f"Wait for structure to recover (price reclaim 20D MA + momentum inflection) "
                    f"before writing calls. "
                    f"(Natenberg Ch.8: CC premise requires stable/range-bound stock; "
                    f"McMillan Ch.3: broken structure = CC income thesis invalid)"
                ),
                "Doctrine_Source": "EquityIntegrity: BROKEN — CC blocked for idle stock",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True,
            })
            return result

        # Check if CC engine already ran (post-doctrine ordering — defensive path)
        cc_status  = str(row.get("CC_Proposal_Status") or "")
        cc_verdict = str(row.get("CC_Proposal_Verdict") or "")

        if cc_status == "FAVORABLE":
            action   = "HOLD"
            urgency  = "MEDIUM"
            rationale = (
                f"📦 Idle stock: {ticker} ({shares_label}) — no call written. "
                f"CC opportunity detected: {cc_verdict}. "
                f"Review CC candidates below before next session. "
                f"(McMillan Ch.3: idle stock earns zero theta — covered call converts "
                f"holding cost into income when IV conditions are right)"
            )
            state = STATE_ACTIONABLE
        elif cc_status == "UNFAVORABLE":
            action   = "HOLD"
            urgency  = "LOW"
            rationale = (
                f"📦 Idle stock: {ticker} ({shares_label}) — no call written. "
                f"CC not advisable now: {cc_verdict}. "
                f"Watch for: {row.get('CC_Watch_Signal', 'improved IV conditions')}. "
                f"(Natenberg Ch.8: sell calls only when IV_Rank > 20% — "
                f"selling in compressed vol gives away upside for thin premium)"
            )
            state = STATE_NEUTRAL_CONFIDENT
        else:
            # CC engine hasn't run yet (normal case — runs post-doctrine)
            action   = "HOLD"
            urgency  = "LOW"
            rationale = (
                f"📦 Idle stock: {ticker} ({shares_label}) — no covered call written. "
                f"CC opportunity assessment pending (evaluating scan engine output). "
                f"(McMillan Ch.3: {shares_label} of idle stock earns zero theta; "
                f"a covered call converts holding cost into income when conditions are right)"
            )
            state = STATE_NEUTRAL_CONFIDENT

        result.update({
            "Action":                 action,
            "Urgency":                urgency,
            "Rationale":              rationale,
            "Doctrine_Source":        "McMillan Ch.3: Idle Stock — CC Opportunity Assessment",
            "Decision_State":         state,
            "Required_Conditions_Met": True,
        })
        return result

    @staticmethod
    def _buy_write_doctrine(row: pd.Series, result: Dict) -> Dict:
        spot   = float(row.get('UL Last', 0) or 0)
        # Short_Call_* columns are pre-computed by TradeLegEnrichment (Cycle 2.9) and
        # broadcast onto ALL leg rows of each trade (including the STOCK leg).
        # Fallback to raw column names for backwards-compatibility (option-leg rows still
        # carry the raw values; enriched columns are just authoritative cross-leg aliases).
        delta  = abs(float(row.get('Short_Call_Delta') or row.get('Delta') or 0))
        dte    = float(row.get('Short_Call_DTE') or row.get('DTE') or 999)

        # Net cost basis (after cumulative premiums collected across all cycles).
        # McMillan Ch.3: "Each successive call further reduces the effective cost of the shares."
        # Passarelli Ch.6: net cost basis is the real breakeven — not the stock purchase price.
        net_cost_basis  = float(row.get('Net_Cost_Basis_Per_Share', 0) or 0)
        cum_premium     = float(row.get('Cumulative_Premium_Collected', 0) or 0)
        strike          = float(row.get('Short_Call_Strike') or row.get('Strike') or 0)

        # Authoritative stock cost per share: Fidelity Basis ÷ Quantity.
        # CRITICAL: Fidelity's 'Basis' for a stock = total dollars paid (purchase price × shares).
        # This is the REAL cost of the position — not Underlying_Price_Entry (which is the spot
        # price when the position was FIRST OBSERVED, NOT what was paid for the stock).
        # Using Underlying_Price_Entry as fallback causes false hard stop triggers when the
        # stock was bought at a price different from the first-observed spot (e.g., older positions).
        broker_basis_total = abs(float(row.get('Basis', 0) or 0))
        qty_abs            = abs(float(row.get('Quantity', 1) or 1))
        broker_cost_per_share = (broker_basis_total / qty_abs) if qty_abs > 0 and broker_basis_total > 0 else 0.0

        # Effective cost hierarchy:
        # 1. Net_Cost_Basis_Per_Share (from BuyWriteLedger — broker cost minus premiums collected) — most accurate
        # 2. Broker Basis / Quantity  (Fidelity's actual cost basis field) — correct stock purchase price
        # 3. Underlying_Price_Entry   (spot at first observation) — LAST resort only; can be wrong
        anchor = row.get('Underlying_Price_Entry', 0)
        if net_cost_basis > 0:
            effective_cost = net_cost_basis
            _cost_tier = 1  # Ledger net cost (authoritative)
        elif broker_cost_per_share > 0:
            effective_cost = broker_cost_per_share
            _cost_tier = 2  # Broker Basis (reliable)
        else:
            effective_cost = float(anchor or 0)
            _cost_tier = 3  # Underlying_Price_Entry (unverified — spot at first observation)

        # 1. Hard Stop: structural drawdown measured from NET cost basis, not raw stock price.
        # McMillan Ch.3 — buy-write risk is stock-side; collected premiums partially cushion.
        if effective_cost > 0:
            drift_from_net = (spot - effective_cost) / effective_cost
            stock_basis_raw = broker_cost_per_share  # original purchase price before premiums
            total_loss_dollars = (spot - stock_basis_raw) * abs(float(row.get('Quantity', 0) or 0))

            if drift_from_net <= -0.20:
                # ── Recovery Ladder Guard ────────────────────────────────────
                # When _cycle_count >= 2 AND thesis is not BROKEN, the trader
                # has consciously sold calls through at least one roll cycle on
                # a stock already below the hard stop.  This is deliberate
                # recovery premium harvesting (Jabbour Ch.4: repair strategies;
                # McMillan Ch.3: progressive basis reduction).
                # Suppress EXIT → HOLD MEDIUM for the call leg.  The stock leg
                # decision is separate — the trader may still choose to exit
                # the equity position independently.
                _rl_cycle_ct = int(row.get('_cycle_count', 1) or 1)
                _rl_thesis   = str(row.get('Thesis_State', '') or '').upper()

                if _rl_cycle_ct >= 2 and cum_premium > 0 and _rl_thesis not in ('BROKEN',):
                    # Monthly income estimate (rough: last premium ÷ last DTE × 30)
                    _rl_last_prem = float(row.get('Premium_Entry', 0) or 0)
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

                # ── Original Hard Stop EXIT ──────────────────────────────────
                # Full context: show original cost, premiums collected, net cost, and total loss
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

                # Epistemic guard: when cost basis is from tier 3 (Underlying_Price_Entry)
                # with no premium history, the drawdown percentage is unverified.  The
                # $112.33 might be first-observed spot, not what was paid for the stock.
                # Downgrade to HIGH (not CRITICAL) and flag the provenance so the trader
                # verifies before acting.  EXIT still fires — position appears deeply
                # underwater — but urgency reflects data confidence.
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

            # 1a. Approaching hard stop: warn when within 5% of the threshold (i.e. -15% to -20%)
            if -0.20 < drift_from_net <= -0.15:
                hard_stop_price = effective_cost * 0.80
                gap_to_stop = spot - hard_stop_price
                premium_note = f" ${cum_premium:.2f}/share collected to date." if cum_premium > 0 else ""

                # Thesis-aware override: within 3% of hard stop + DEGRADED/BROKEN thesis → EXIT
                # Do not roll into further debit when the investment thesis has already failed.
                _thesis_hs = str(row.get('Thesis_State', '') or '').upper()
                _gap_pct = gap_to_stop / spot if spot and spot > 0 else 1.0
                if _gap_pct < 0.03 and _thesis_hs in ('DEGRADED', 'BROKEN'):
                    result.update({
                        "Action": "EXIT",
                        "Urgency": "CRITICAL",
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

        # 1b. Earnings Event Risk: binary event inside option DTE → gap risk unmanageable by delta
        # Natenberg Ch.12: "Event-driven moves are discontinuous — delta hedging cannot protect."
        # McMillan Ch.3: close or roll BEFORE earnings if stock is near cost basis under pressure.
        #
        # Guard: only applies when DTE ≤ 90 (short-dated cycle straddles the event).
        # For LEAPS (DTE > 90) the call survives many earnings events by design — the risk
        # is baked into the long-dated premium. The relevant risk is assignment, not event gap,
        # since the call is typically far OTM relative to the long time horizon.
        # Moneyness guard: only flag if stock is within 20% of strike (meaningful delta exposure).
        earnings_date = row.get('Earnings Date') or row.get('Earnings_Date')
        _is_leaps_dte = dte > 90
        _pct_to_strike = abs(spot - strike) / strike if strike > 0 and spot > 0 else 1.0
        _near_strike = _pct_to_strike <= 0.20  # within 20% of strike
        if earnings_date is not None and not _is_leaps_dte and _near_strike:
            try:
                snap_ts = pd.to_datetime(row.get('Snapshot_TS') or pd.Timestamp.now())
                earn_dt = pd.to_datetime(earnings_date)
                days_to_earnings = (earn_dt.normalize() - snap_ts.normalize()).days
                # Event risk window: earnings within current option cycle
                if 0 <= days_to_earnings <= max(int(dte), 7):
                    _earn_urgency = "CRITICAL" if (drift_from_net <= -0.10 and days_to_earnings <= 5) else "HIGH"
                    _loss_note = (
                        f" Position already at {drift_from_net:.1%} from net cost — "
                        f"earnings gap could breach hard stop in one session."
                        if drift_from_net < -0.05 else ""
                    )
                    _premium_note = f" ${cum_premium:.2f}/share collected to date." if cum_premium > 0 else ""
                    result.update({
                        "Action": "ROLL",
                        "Urgency": _earn_urgency,
                        "Rationale": (
                            f"Earnings in {days_to_earnings}d (within {dte:.0f}d DTE window).{_loss_note}"
                            f"{_premium_note} Roll call before event: move to post-earnings expiry and "
                            f"consider wider strike to absorb gap risk "
                            f"(Natenberg Ch.12: Event Gap Risk — delta cannot protect against discontinuous moves)."
                        ),
                        "Doctrine_Source": "Natenberg Ch.12: Earnings Event Risk",
                        "Decision_State": STATE_ACTIONABLE,
                        "Required_Conditions_Met": True
                    })
                    return result
                elif -7 <= days_to_earnings < 0:
                    # Earnings just passed — assess if move was structural or temporary
                    price_change = drift_from_net  # using cost-basis drift as proxy
                    if price_change < -0.08:
                        result.update({
                            "Action": "HOLD",
                            "Urgency": "MEDIUM",
                            "Rationale": (
                                f"Earnings passed {abs(days_to_earnings)}d ago. Post-earnings drop of "
                                f"{price_change:.1%} from net cost. Assess: was this guidance-driven "
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
            except Exception:
                pass  # Earnings date parse failure is non-fatal

        # 1b-GAP2 FIX: Earnings Lockdown — upgrade ROLL to EXIT when earnings < 3 days.
        # Jabbour (Option Trader Handbook Ch.8) + Given (No-Hype Options Trading Ch.4):
        # "Never roll INTO an earnings event — binary risk invalidates the spread thesis."
        # When the prior earnings gate (1b above) set Action=ROLL but earnings is now
        # ≤ 2 days away, rolling creates a NEW position inside the binary window. That is
        # structurally worse than exiting the old one. Upgrade ROLL → EXIT.
        # Does NOT apply to LEAPS (_is_leaps_dte) — they span earnings by design.
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
            except Exception:
                pass

        # 1b-LEAPS: For long-dated positions (DTE > 90), earnings is routine — the call spans
        # many events by design. Inject a soft note when earnings is within 30 days so the
        # holder is aware, but do NOT escalate to ROLL (that would be incorrect doctrine).
        if earnings_date is not None and _is_leaps_dte:
            try:
                snap_ts = pd.to_datetime(row.get('Snapshot_TS') or pd.Timestamp.now())
                earn_dt = pd.to_datetime(earnings_date)
                days_to_earnings = (earn_dt.normalize() - snap_ts.normalize()).days
                if 0 <= days_to_earnings <= 30:
                    _earn_note = (
                        f" [Earnings in {days_to_earnings}d — LEAPS call at "
                        f"${strike:.0f} is ${abs(spot-strike):.0f} OTM ({_pct_to_strike:.0%} from strike). "
                        f"No roll needed; event risk is priced into long-dated IV. "
                        f"Monitor if stock gaps toward strike post-earnings.]"
                    )
                    result['Rationale'] = result.get('Rationale', '') + _earn_note
            except Exception:
                pass

        # 1c. Underlying Health Story Check (Passarelli Ch.2: "Monitor whether the story is still intact.")
        # NOT a scorecard. Three gating states derived from chart primitives already computed:
        #   BROKEN  → price structure destroyed; thesis invalid → escalate doctrine action
        #   DEGRADED → trend deteriorating but recoverable; add warning to rationale
        #   INTACT  → story unchanged; no impact
        # This replaces the urge to add beta/P/E noise. The chart states ARE the story.
        _price_struct  = str(row.get('PriceStructure_State', '') or '').upper()
        _trend_integ   = str(row.get('TrendIntegrity_State', '') or '').upper()
        _ema50_slope   = float(row.get('ema50_slope', 0) or 0)
        _hv_percentile = float(row.get('hv_20d_percentile', 50) or 50)

        # BROKEN: structure destroyed AND trend also gone — thesis is invalid
        if 'STRUCTURE_BROKEN' in _price_struct and _trend_integ in ('NO_TREND', 'TREND_EXHAUSTED'):
            _struct_note = (
                f"Price structure BROKEN + trend exhausted. "
                f"This is not a timing problem — the underlying thesis is structurally invalid. "
            )
            if effective_cost > 0 and drift_from_net < -0.05:
                # Under pressure + story broken → escalate to exit consideration
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
                # Structure broken but not yet losing → warn and degrade urgency
                result['Rationale'] = (
                    f"⚠️ Story Check: {_struct_note}"
                    f"Verify thesis before next roll (Passarelli Ch.2). "
                ) + result.get('Rationale', '')

        # DEGRADED: EMA50 declining + HV elevated → trend weakening but not broken
        elif _ema50_slope < -0.02 and _hv_percentile > 70 and drift_from_net < -0.05:
            _degrade_note = (
                f"EMA50 slope={_ema50_slope:.3f} (declining) + HV at {_hv_percentile:.0f}th percentile "
                f"(elevated fear). Underlying trending against position under high volatility. "
            )
            # Don't override action — add to rationale as a warning modifier
            if result.get('Urgency', 'LOW') == 'LOW':
                result['Urgency'] = 'MEDIUM'
            result['Rationale'] = (
                f"⚠️ Underlying weakening: {_degrade_note}"
            ) + result.get('Rationale', '')

        # 2. ITM Defense: Delta > 0.70 → assignment imminent.
        # McMillan Ch.3: roll when call goes deep ITM to avoid forced assignment.
        # CRITICAL: Compare strike to NET cost basis — assignment at a strike BELOW net cost
        # locks in a real loss even after collected premiums. Surface this explicitly.
        if delta > 0.70:
            if strike > 0 and effective_cost > 0 and strike < effective_cost:
                # Assignment would realize a loss even after all premiums collected
                loss_per_share = effective_cost - strike
                assignment_outcome = (
                    f"Assignment at ${strike:.2f} BELOW net cost ${effective_cost:.2f} "
                    f"(−${loss_per_share:.2f}/share loss despite ${cum_premium:.2f}/share collected). "
                    f"Roll up to strike above ${effective_cost:.2f} to preserve breakeven."
                )
                urgency = "CRITICAL"
            elif strike > 0 and effective_cost > 0 and strike >= effective_cost:
                # Assignment profitable — premiums have reduced cost enough
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

        # 3. Expiration proximity: DTE < 7 → roll to next cycle before pin risk
        # McMillan Ch.3: don't let covered calls expire in-the-money — roll with 7–14 DTE
        if dte < 7:
            result.update({
                "Action": "ROLL",
                "Urgency": "HIGH",
                "Rationale": f"DTE={dte:.0f} < 7 — pin risk and gamma acceleration. Roll to next cycle (McMillan Ch.3: Expiration Management).",
                "Doctrine_Source": "McMillan Ch.3: Expiration Management",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result

        # ── Pre-ITM Drift Warning: 0.55 < delta ≤ 0.70 ─────────────────────────
        # This is the "Roll Optimal Window" — the gate that fires BEFORE the emergency.
        # At this delta range: intrinsic is growing but extrinsic (credit) still exists.
        # Once delta > 0.70 that credit is mostly gone and any roll requires a debit.
        #
        # Passarelli Ch.5 / McMillan Ch.3: "The best time to roll a covered call is when
        # you still have extrinsic value to sell — not after it's been crushed by deep ITM."
        #
        # Three sub-conditions are checked:
        #   A) Delta in warning zone (0.55–0.70) — approaching critical
        #   B) Strike proximity — stock within 2% of strike (even at lower delta)
        #   C) Both A and B — strongest pre-warning signal
        _call_last_pre  = abs(float(row.get('Short_Call_Last') or row.get('Last') or 0))
        _intrinsic_pre  = max(0.0, spot - strike) if strike > 0 else 0.0
        _extrinsic_pre  = max(0.0, _call_last_pre - _intrinsic_pre) if _call_last_pre > 0 else 0.0
        _extrinsic_pct  = (_extrinsic_pre / _call_last_pre) if _call_last_pre > 0 else 0.0
        _strike_prox    = ((spot - strike) / strike) if strike > 0 else 0.0  # positive = ITM

        _pre_itm_delta_warn   = (0.55 < delta <= 0.70)
        _pre_itm_strike_prox  = (strike > 0 and 0 <= _strike_prox <= 0.03)  # within 3% of strike
        _credit_still_viable  = (_extrinsic_pct >= 0.25)  # ≥25% extrinsic = roll for credit possible
        _credit_strong        = (_extrinsic_pct >= 0.40)  # ≥40% = good credit window

        if _pre_itm_delta_warn or _pre_itm_strike_prox:
            # Compute how much runway remains before hitting the 0.70 emergency gate
            _delta_gap_to_emergency = max(0.0, 0.70 - delta)

            # Strike-below-cost flag: when the current strike is under net cost basis,
            # a same-strike rollout collects a credit but doesn't rescue the breakeven.
            # A rescue roll (to a strike ABOVE net cost) is always a debit — the buyback
            # cost of intrinsic + extrinsic exceeds the new OTM premium received.
            # These are two distinct roll paths with different cost structures.
            _strike_below_cost = (effective_cost > 0 and strike > 0 and strike < effective_cost)
            _rescue_debit_est  = _call_last_pre  # cost to buy back = at minimum the current option price

            if _credit_strong:
                if _strike_below_cost:
                    # Credit exists on the current option, but a rescue roll costs a debit.
                    # Distinguish clearly: the extrinsic is the MINIMUM you save vs waiting,
                    # not a credit you receive.
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
                _urgency_pre    = "MEDIUM"
            elif _credit_still_viable:
                if _strike_below_cost:
                    _credit_label = (
                        f"Extrinsic = ${_extrinsic_pre:.2f} ({_extrinsic_pct:.0%} of option price) — credit thin. "
                        f"Rescue roll above ${effective_cost:.2f} requires debit; debit cost grows daily. Act soon."
                    )
                else:
                    _credit_label = f"Extrinsic = ${_extrinsic_pre:.2f} ({_extrinsic_pct:.0%} of option price) — credit thin, act soon"
                _window_quality = "closing"
                _urgency_pre    = "HIGH"
            else:
                # Extrinsic nearly gone — debit roll likely already needed, but delta not yet at 0.70
                _credit_label   = f"Extrinsic = ${_extrinsic_pre:.2f} ({_extrinsic_pct:.0%}) — mostly intrinsic, debit roll required"
                _window_quality = "closing fast"
                _urgency_pre    = "HIGH"

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
            # Do NOT return here — fall through to roll timing classification
            # so the candidate strike selection and timing logic still applies.
            # Position Trajectory context for pre-ITM
            _pos_regime_pre = str(row.get('Position_Regime', 'NEUTRAL') or 'NEUTRAL')
            if _pos_regime_pre == 'TRENDING_CHASE':
                result['Rationale'] += (
                    " ⚠️ TRENDING_CHASE: stock is trending through strikes. "
                    "This ITM event is structural, not temporary — assignment may be the correct outcome."
                )

        # ── Roll Timing Intelligence (applies to all non-emergency rolls below) ──
        # Classify market conditions ONCE before checking discretionary roll triggers.
        # Emergency triggers (DTE < 7, delta > 0.70) bypass timing — they always fire.
        # McMillan Ch.3 / Passarelli Ch.6 / Natenberg Ch.8: roll timing matters as much
        # as the roll decision itself. Never roll into choppy range — collect thin premium,
        # get stuck with wrong strike. Always roll immediately into a confirmed breakout.
        _timing = DoctrineAuthority._classify_roll_timing(row)

        # ── Thesis block: intercept all discretionary rolls ──────────────────
        # Emergency exits (hard stop, delta>0.70, DTE<7) returned early above.
        # If thesis is BROKEN, the only valid discretionary action is HOLD pending
        # EXIT evaluation — no roll optimizes a broken thesis, it just increases
        # commitment to a failing position.
        # McMillan Ch.3: "Rolling amplifies the loss when the thesis is broken."
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

        # ── Post-BUYBACK sticky gate ─────────────────────────────────────────
        # When prior action was BUYBACK, the short call has been removed. The engine
        # must NOT suggest re-selling (ROLL) until equity structure fully recovers.
        # Rationale: the stock still carries margin cost — but re-selling into a
        # BROKEN or WEAKENING structure repeats the same losing carry trade.
        # Jabbour Ch.11: "close and re-evaluate" means wait for confirmation.
        # Only release to ROLL when Equity_Integrity = INTACT.
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
        # Natenberg Ch.7 (0.744): "ATM + low DTE + low vol is the maximum-risk
        # configuration for short gamma. Gamma peaks at this point on the
        # volatility surface — the writer's exposure to sudden moves is maximal."
        #
        # Pre-emptive detection: fires BEFORE equity breaks, when the position's
        # gamma profile is entering the danger zone. The existing gamma dominance
        # gate (below) only fires when equity is already BROKEN — by then, the
        # damage is structural and buyback is reactive.
        #
        # Conditions (ALL must be true):
        #   1. Near-ATM: |spot - strike| / spot < 0.05 (within 5% of strike)
        #   2. DTE 7-21: gamma acceleration zone (< 7 handled by expiration gate)
        #   3. Gamma/theta ratio > 1.5: gamma starting to dominate theta
        #   4. Equity NOT BROKEN: BROKEN handled by existing gamma dominance gate
        #
        # Escalation: Gamma_ROC_3D > 0 → urgency HIGH (gamma accelerating)
        #
        # Action: ROLL → extend DTE to reduce gamma, or move strike OTM.
        # Passarelli Ch.6: "Pre-emptive roll before gamma overwhelms theta."
        import math as _math_gdz
        _gdz_strike = float(row.get('Short_Call_Strike') or row.get('Strike') or 0)
        _gdz_theta  = abs(float(row.get('Theta', 0) or 0))
        _gdz_gamma  = abs(float(row.get('Gamma', 0) or 0))
        _gdz_hv     = float(row.get('HV_20D', 0.20) or 0.20)
        if _gdz_hv >= 1.0: _gdz_hv /= 100.0
        _gdz_sigma  = spot * (_gdz_hv / _math_gdz.sqrt(252)) if spot > 0 else 0.0
        _gdz_drag   = 0.5 * _gdz_gamma * (_gdz_sigma ** 2)
        _gdz_roc3   = float(row.get('Gamma_ROC_3D', 0) or 0)
        _gdz_atm_pct = abs(spot - _gdz_strike) / spot if spot > 0 and _gdz_strike > 0 else 1.0
        _gdz_ratio  = _gdz_drag / _gdz_theta if _gdz_theta > 0 else 0.0
        _gdz_ei     = str(row.get('Equity_Integrity_State', '') or '').strip()

        _gdz_fires = (
            _gdz_atm_pct < 0.05       # within 5% of strike
            and 7 < dte <= 21          # gamma acceleration zone
            and _gdz_ratio > 1.5      # gamma starting to dominate
            and _gdz_ei != 'BROKEN'   # not already handled by gamma dominance gate
        )

        if _gdz_fires:
            _gdz_urgency = "HIGH" if _gdz_roc3 > 0 else "MEDIUM"
            _gdz_roc_note = (
                f"Gamma_ROC_3D={_gdz_roc3:+.4f} (accelerating — urgency escalated). "
                if _gdz_roc3 > 0 else
                f"Gamma_ROC_3D={_gdz_roc3:+.4f} (stable/declining). "
            )
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

        # ── Equity Integrity gate ─────────────────────────────────────────────
        # BROKEN:    3+ structural signals (MA bear, momentum, drawdown, vol regime).
        #            Default: blocks discretionary roll — don't roll into a broken structure.
        #            McMillan Ch.1: don't roll into a broken stock structure.
        #
        # EXCEPTION — Gamma Dominance Buyback:
        #   When equity is BROKEN, the instinct to HOLD the current short call is wrong if
        #   gamma drag > 2× theta carry. In that case HOLD is mathematically losing money
        #   every day AND the stock is falling. The rational action is to SEPARATE the two
        #   decisions: buy back the call (stop the gamma bleed), then evaluate the stock
        #   independently. This is McMillan Ch.3's core teaching: "The stock decision and
        #   the call decision are independent. When structure breaks, uncap the position."
        #
        # WEAKENING: 1–2 signals. Non-blocking — annotate rationale as early warning.
        _ei_state  = str(row.get('Equity_Integrity_State', '') or '').strip()
        _ei_reason = str(row.get('Equity_Integrity_Reason', '') or '').strip()
        if _ei_state == 'BROKEN' and result.get('Action') not in ('EXIT', 'HARD_HALT'):
            # Compute inline gamma dominance to determine if HOLD is mathematically valid
            import math as _math_ei
            _ei_theta   = abs(float(row.get('Theta', 0) or 0))          # per share per day
            _ei_gamma   = abs(float(row.get('Gamma', 0) or 0))          # per share per $1
            _ei_hv      = float(row.get('HV_20D', 0.20) or 0.20)
            if _ei_hv >= 1.0: _ei_hv /= 100.0
            _ei_sigma   = spot * (_ei_hv / _math_ei.sqrt(252))          # daily $ move 1σ
            _ei_gamma_drag = 0.5 * _ei_gamma * (_ei_sigma ** 2)         # $ per share per day

            # Moneyness sanity guard: gamma dominance is only physically meaningful when
            # the short strike is within ~30% of spot. A call/put that is >30% OTM has
            # near-zero gamma by Black-Scholes — any reported Gamma at that distance is
            # data noise, stale Greeks, or net-position artifact (not option gamma).
            # Firing the buyback doctrine on a deeply OTM call would be wrong for any
            # position where the strike has drifted far away over time.
            _ei_short_strike = float(
                row.get('Short_Call_Strike') or row.get('Strike') or 0
            ) or 0.0
            _ei_otm_pct = (
                abs(_ei_short_strike - spot) / spot
                if _ei_short_strike > 0 and spot and spot > 0 else 0.0
            )
            _ei_gamma_dominant = (
                _ei_theta > 0
                and _ei_gamma_drag > _ei_theta * 2.0
                and _ei_otm_pct <= 0.30   # strike must be within 30% of spot
            )

            if _ei_gamma_dominant:
                # Gamma drag > 2× theta — mathematically correct measurement.
                # But near-ATM gamma is STRUCTURALLY HIGH at 2-3 weeks DTE (Passarelli Ch.6,
                # Given Ch.X). Buyback requires CONVICTION — not just gamma math.
                #
                # Conviction gate (any one fires → buyback):
                #   A. DTE ≤ 7: expiration week — Passarelli Ch.6: "close short premium
                #      before expiration week"; Given: "trading rules call for closing
                #      positions on the Friday before expiration week."
                #   B. Stock above strike + upward momentum: breakout through strike.
                #      The short call caps upside that the stock is actively realising.
                #   C. Extrinsic < $0.20: no theta left to collect. The call is all
                #      intrinsic risk with zero income benefit.
                #
                # Without conviction → HOLD with gamma warning (informational).
                # Theta income continues; trader is alerted to monitor.
                _ei_ratio = _ei_gamma_drag / _ei_theta if _ei_theta > 0 else float('inf')
                _ei_call_last = abs(float(row.get('Short_Call_Last') or row.get('Last') or 0))
                _ei_entry     = abs(float(row.get('Short_Call_Premium') or row.get('Premium_Entry') or 0))

                # Compute extrinsic value: call price minus intrinsic (max(0, spot - strike))
                _ei_intrinsic = max(0.0, spot - _ei_short_strike) if spot > 0 and _ei_short_strike > 0 else 0.0
                _ei_extrinsic_val = max(0.0, _ei_call_last - _ei_intrinsic)

                # Direction/momentum from chart primitives (already in row)
                _ei_adx = float(row.get('adx_14', 0) or 0)
                _ei_roc = float(row.get('roc_20', 0) or 0)

                _ei_buyback_conviction = (
                    dte <= 7                                                                    # A: expiration week
                    or (spot > _ei_short_strike * 1.01 and _ei_roc > 0 and _ei_adx > 25)       # B: breakout through strike
                    or _ei_extrinsic_val < 0.20                                                 # C: no theta left
                )

                if _ei_buyback_conviction:
                    _ei_close_note = (
                        f" Current call at ${_ei_call_last:.2f} vs ${_ei_entry:.2f} entry "
                        f"({'profit' if _ei_call_last < _ei_entry else 'debit'} to close)."
                        if _ei_call_last > 0 and _ei_entry > 0 else ""
                    )
                    # Which conviction signal fired?
                    _ei_conv_reason = (
                        "DTE ≤ 7 (expiration week)" if dte <= 7 else
                        f"stock ${spot:.2f} above strike ${_ei_short_strike:.2f} + momentum (ROC={_ei_roc:.1f}, ADX={_ei_adx:.0f})"
                        if spot > _ei_short_strike * 1.01 else
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
                    # Gamma dominant but no conviction. Before defaulting to HOLD, check
                    # if carry is actually negative — if gamma + margin > theta, the
                    # "theta income still collecting" premise is FALSE.
                    _gd_margin_daily = effective_cost * FIDELITY_MARGIN_RATE_DAILY if effective_cost > 0 else 0.0
                    _gd_net_carry = _ei_theta - _gd_margin_daily - _ei_gamma_drag

                    # Cost-basis cushion guard: BUY_WRITE is an income strategy where
                    # cumulative premiums reduce the effective cost basis. When spot is
                    # ABOVE net cost basis, the position has a profit cushion that absorbs
                    # negative daily carry. EXIT is premature — the "compounding loss" is
                    # being cushioned by prior cycles' income. Only EXIT when the cushion
                    # is consumed (spot drops below net cost) or DTE is very short.
                    # McMillan Ch.3: "Each successive call further reduces the effective
                    # cost of the shares" — don't abandon that advantage on a single
                    # cycle's daily carry metric.
                    _gd_above_net_cost = (
                        effective_cost > 0
                        and spot > effective_cost
                        and dte > 14  # if DTE ≤ 14, expiration risk overrides cushion
                    )
                    _gd_cushion_pct = (
                        (spot - effective_cost) / effective_cost
                        if effective_cost > 0 and spot > effective_cost else 0.0
                    )

                    if _gd_net_carry < 0 and not _gd_above_net_cost:
                        # Gamma dominant + negative carry + no cost-basis cushion → EXIT MEDIUM
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
                        # Gamma dominant + negative carry BUT stock above net cost basis.
                        # The position has a profit cushion from cumulative premiums.
                        # HOLD with carry warning — don't EXIT a cushioned income position.
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

                    # Positive carry despite gamma dominance: structurally expected near-ATM
                    # at this DTE. Theta income IS covering costs — hold.
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
                # Standard BROKEN gate: no gamma dominance.
                # Before returning generic BROKEN HOLD, check for LEAPS carry inversion:
                # when DTE > 90 and margin cost >= theta income, the financing cost exceeds
                # what the short call earns in decay — the carry thesis has structurally inverted.
                # Given Ch.6 (0.764): "sell calls within one strike of ATM only"
                # Jabbour Ch.11 (0.692): "close and re-evaluate rather than rolling"
                _ei_is_leaps = dte > 90
                if _ei_is_leaps:
                    _ci_margin_daily = effective_cost * FIDELITY_MARGIN_RATE_DAILY if effective_cost > 0 else 0.0
                    _ci_theta_daily  = abs(float(row.get('Theta', 0) or 0))
                    _ci_strike = float(
                        row.get('Short_Call_Strike') or row.get('Strike') or 0
                    ) or 0.0
                    _ci_pct_otm = (
                        abs(spot - _ci_strike) / spot
                        if spot > 0 and _ci_strike > 0 else 0.0
                    )

                    if _ci_margin_daily > 0 and _ci_theta_daily > 0 and _ci_margin_daily >= _ci_theta_daily:
                        _ci_ratio = _ci_margin_daily / _ci_theta_daily

                        # Severity-gated BUYBACK: only recommend buying back the call when
                        # the inversion is severe enough to justify losing ALL theta income.
                        # At 1.04× the net bleed is $0.14/day — BUYBACK makes it $3.65/day.
                        # BUYBACK only makes sense when the carry bleed is substantial.
                        # Given Ch.6: "sell calls within one strike of ATM"
                        # Jabbour Ch.11: "close and re-evaluate rather than rolling"
                        _CI_SEVERE_RATIO = 1.5  # margin >= 1.5× theta → severe inversion

                        if _ci_ratio >= _CI_SEVERE_RATIO:
                            # Severe inversion: theta is negligible vs margin cost.
                            # BUYBACK justified — the call isn't earning enough to justify holding.
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
                            # Mild inversion (1.0-1.5×): net bleed is small.
                            # BUYBACK would remove ALL theta income while margin persists.
                            # HOLD is less damaging than BUYBACK at this ratio.
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

                # Standard BROKEN gate: check if holding is net-negative carry.
                # Components: theta income - margin cost - gamma drag (all per share per day).
                # Variables _ei_theta, _ei_gamma_drag already computed (line 1281-1286);
                # effective_cost and FIDELITY_MARGIN_RATE_DAILY already in scope.
                # (Audit: DKNG Feb-2026 — gamma $7.39/day vs theta $3.66/day + margin $0.77/day
                #  = net bleed $4.50/contract/day while BROKEN, system defaulted to HOLD HIGH)
                _ei_margin_daily = effective_cost * FIDELITY_MARGIN_RATE_DAILY if effective_cost > 0 else 0.0
                _ei_net_carry = _ei_theta - _ei_margin_daily - _ei_gamma_drag  # per share per day

                if _ei_net_carry < 0:
                    # BROKEN equity + negative carry = compounding loss → EXIT MEDIUM.
                    # McMillan Ch.3: "don't carry a broken structure at negative EV."
                    # Natenberg Ch.7: "negative carry + broken equity = structural loss."
                    _ei_bleed_contract = abs(_ei_net_carry) * 100  # per contract per day
                    _ei_bleed_to_exp = _ei_bleed_contract * dte
                    result.update({
                        "Action": "EXIT",
                        "Urgency": "MEDIUM",
                        "Rationale": (
                            f"⚠️ Equity BROKEN ({_ei_reason}) + negative carry: "
                            f"θ ${_ei_theta*100:.2f}/day − margin ${_ei_margin_daily*100:.2f}/day "
                            f"− γ-drag ${_ei_gamma_drag*100:.2f}/day = "
                            f"net bleed ${_ei_bleed_contract:.2f}/contract/day "
                            f"(${_ei_bleed_to_exp:.0f} to expiry). "
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

                # Positive carry: theta earns more than costs. HOLD is justified — the
                # short call IS providing income despite broken equity structure.
                # McMillan Ch.1: "Confirm trend context before committing further capital."
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

        # 3a-LEAPS. Carry Inversion Gate — margin exceeds theta on long-dated positions.
        # Given Ch.6 (0.764): "sell calls within one strike of ATM only" — deep OTM LEAPS
        # generate insufficient theta to cover daily financing cost.
        # Jabbour Ch.11 (0.692): "close and re-evaluate rather than rolling a losing structure."
        # Augen (0.704): "roll forward only when the new position has similar dynamics."
        # This gate is distinct from the negative carry gate (gate 5, line ~1437) which checks
        # annualized yield vs margin rate. This checks daily cash flow: theta vs margin cost.
        _ci_is_leaps = dte > 90
        if _ci_is_leaps:
            _ci_margin_daily = effective_cost * FIDELITY_MARGIN_RATE_DAILY if effective_cost > 0 else 0.0
            _ci_theta_daily  = abs(float(row.get('Theta', 0) or 0))
            _ci_strike = float(
                row.get('Short_Call_Strike') or row.get('Strike') or 0
            ) or 0.0
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

        # 3b. Dividend assignment gate — M1 audit fix (BUY_WRITE side).
        # Short call with delta > 0.50 when ex-dividend is within 5 days: call owners will
        # exercise early to capture the dividend. Forced assignment can occur ANY day before
        # expiry — not just expiration week. McMillan Ch.2: "American-style calls are
        # exercised early when the dividend exceeds the remaining time value of the call.
        # Delta > 0.50 ITM calls typically have time value < dividend amount — prime exercise candidates."
        # Urgency: CRITICAL if < 2 days (may already be too late), HIGH if 2-4 days.
        _bw_days_div = float(row.get('Days_To_Dividend', 9999) or 9999)
        _bw_div_amt  = float(row.get('Dividend_Amount', 0) or 0)
        if delta > 0.50 and _bw_days_div < 5 and _bw_div_amt > 0:
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

        # 3b. GAP 1 FIX: Hard 21-DTE exit gate for income (covered call / buy-write).
        # Given (No-Hype Options Trading Ch.6) + Passarelli Ch.2:
        # When DTE ≤ 21 and 50% profit NOT yet captured, the gamma-theta ratio has degraded —
        # the premium-seller edge is structurally exhausted. Hold-and-hope at this point
        # only exposes to gamma acceleration risk with little remaining theta reward.
        # Exceptions: ITM call (assignment risk = separate handling above); call already
        # deeply profitable (>50% — handled by early-roll gate below).
        premium_collected_21 = abs(float(row.get('Short_Call_Premium') or row.get('Premium_Entry') or 0))
        current_close_cost_21 = abs(float(row.get('Short_Call_Last') or row.get('Last') or 0))
        pct_captured_21 = (
            (premium_collected_21 - current_close_cost_21) / premium_collected_21
            if premium_collected_21 > 0 else 0.0
        )
        _bw_moneyness = str(row.get('Moneyness_Label') or row.get('Short_Call_Moneyness') or 'OTM')
        if (
            dte <= 21
            and dte >= 7                       # DTE<7 handled by emergency gates above
            and pct_captured_21 < 0.50         # 50% NOT yet captured
            and _bw_moneyness != 'ITM'         # ITM calls = separate assignment gates above
        ):
            _cc_21_urgency = 'MEDIUM' if pct_captured_21 >= 0 else 'HIGH'

            # ── Strategy-aware IV regime check (Chan 0.786) ─────────────────
            # Chan: "exit logic must differ: mean-reversion (income) vs momentum."
            # For income strategies, the edge = IV > HV (selling overpriced premium).
            # When IV has contracted significantly AND is below the selling threshold,
            # rolling starts a new short position with negative EV. Better to let
            # current premium decay to zero than to ROLL into thin premium.
            # Natenberg Ch.8: "Selling premium below HV = negative expected value."
            _iv_entry_21 = float(row.get('IV_Entry', 0) or 0)
            _iv_now_21   = float(row.get('IV_30D', 0) or row.get('IV_Now', 0) or 0)
            _iv_pctile_21 = float(row.get('IV_Percentile', 50) or 50)
            _iv_gap_21   = float(row.get('IV_vs_HV_Gap', 0) or 0)

            _iv_collapsed_21 = (
                _iv_entry_21 > 0 and _iv_now_21 > 0
                and (_iv_now_21 / _iv_entry_21) < 0.70   # >30% IV contraction
                and _iv_pctile_21 < 25                    # bottom quartile
                and _iv_gap_21 <= 0                       # selling edge gone
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
            _consec_debits_21 = int(float(row.get('Trajectory_Consecutive_Debit_Rolls', 0) or 0))
            _stock_ret_21 = float(row.get('Trajectory_Stock_Return', 0) or 0)
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

        # 4. Early roll opportunity: 50% premium captured with >21 DTE remaining
        # Passarelli Ch.6: take 50% of max profit early to redeploy capital efficiently.
        # For a SHORT call: premium_collected = Premium_Entry (positive when short).
        # Current cost-to-close = Last (current option price) × 100.
        # Profit captured = premium_collected - current_close_cost (per share).
        # Short_Call_Premium / Short_Call_Last: authoritative from short-call leg (Cycle 2.9).
        premium_collected = abs(float(row.get('Short_Call_Premium') or row.get('Premium_Entry') or 0))
        current_close_cost = abs(float(row.get('Short_Call_Last') or row.get('Last') or 0))  # current option bid ~= Last
        if premium_collected > 0 and current_close_cost <= premium_collected * 0.50 and dte > 21:
            pct_captured = 1 - (current_close_cost / premium_collected)

            if _timing['action_mod'] == 'WAIT':
                # Market is choppy or compression releasing — downgrade to HOLD and explain why
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
                        __import__('json').dumps(_timing['intraday_advisory'])
                        if _timing.get('intraday_advisory') else ""
                    ),
                })
            # Position Trajectory context for 50% profit gate
            _pos_regime_50 = str(row.get('Position_Regime', 'NEUTRAL') or 'NEUTRAL')
            _stock_ret_50 = float(row.get('Trajectory_Stock_Return', 0) or 0)
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

        # 5. Negative carry: annualized yield below Fidelity margin rate (10.375%)
        # McMillan Ch.3: "The covered writer must earn at least the cost of carrying the stock."
        # Passarelli Ch.6: "Negative carry — yield below financing rate — is a ROLL signal."
        # Capital at risk is the net cost basis (stock cost after all premiums collected).
        dte_val = max(dte, 1)
        capital_at_risk = effective_cost if effective_cost > 0 else abs(float(anchor or 0))
        premium = abs(float(row.get('Short_Call_Premium') or row.get('Premium_Entry') or 0))
        if premium > 0 and capital_at_risk > 0:
            # Per-share annualized yield: premium / net stock cost per annum.
            annualized_yield = (premium / capital_at_risk) * (365 / dte_val)
            # Daily margin bleed in dollars per share at Fidelity's 10.375% rate
            daily_margin_bleed = capital_at_risk * FIDELITY_MARGIN_RATE_DAILY
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
                    # Choppy: flag yield issue but don't force a roll into noise.
                    # GAP FIX: if DTE < 14 AND carry deficit is material (yield < 5%),
                    # a WAIT/HOLD at LOW urgency is dangerous — the position expires in 2 weeks
                    # and is still losing carry every day. Escalate to MEDIUM.
                    # Passarelli Ch.6: "Waiting for timing clarity is wise; ignoring DTE is not."
                    _nc_urgency = "LOW"
                    if dte < 14 and annualized_yield < 0.05:
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
                            __import__('json').dumps(_timing['intraday_advisory'])
                            if _timing.get('intraday_advisory') else ""
                        ),
                        "Decision_State": STATE_ACTIONABLE,
                        "Required_Conditions_Met": True
                    })
                return result

        # 5b. Debit Roll Efficiency Gate — cadence switch evaluation
        # When a position has accumulated debit rolls (buybacks > 40% of gross collected),
        # the current long-dated / far-OTM strike cadence is not working efficiently.
        # The system can now answer: "Should we buy back and switch to weeklies/monthlies?"
        #
        # Conditions that trigger the cadence switch flag:
        #   A) Has_Debit_Rolls = True — at least one cycle was a net debit
        #   B) buyback efficiency ratio > 0.40 (buybacks consumed 40%+ of gross premium)
        #   C) Current call has meaningful extrinsic left (not already in emergency)
        #   D) Call is not currently ITM with high delta (emergency gates handle that above)
        #
        # Natenberg Ch.8: "Strike selection and cycle length are the two primary levers for
        # buy-write efficiency. A far-OTM LEAPS call collects little relative to its time;
        # switching to near-dated calls on the same shares redeploys theta more efficiently."
        # McMillan Ch.3: "Rolling is only beneficial when the new cycle's net premium improves
        # the total position yield. Repeated debit rolls signal the wrong strike cadence."
        _has_debit_rolls   = bool(row.get('Has_Debit_Rolls', False))
        _gross_prem        = float(row.get('Gross_Premium_Collected', 0) or 0)
        _total_close_cost  = float(row.get('Total_Close_Cost', 0) or 0)
        _is_emergency_zone = delta > 0.55 or dte < 14  # emergency gates already handled above

        if (_has_debit_rolls
                and _gross_prem > 0
                and not _is_emergency_zone
                and result.get('Action') not in ('EXIT',)):

            _buyback_ratio = _total_close_cost / _gross_prem  # fraction of gross eaten by buybacks

            if _buyback_ratio > 0.40:
                # Debit rolls have consumed significant premium — evaluate cadence switch
                # Compute current cycle's annualized yield for comparison context
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

                # Is there still extrinsic in the current call to sell out of?
                _exit_credit_note = (
                    f" Current call has ${_extrinsic_pre:.2f} extrinsic — "
                    f"buying back now at ${_call_last_pre:.2f} vs original ${premium_collected:.2f} entry."
                    if _call_last_pre > 0 and premium_collected > 0 else ""
                )

                # Determine urgency: if current call is far OTM (low delta) + still long DTE
                # → higher urgency to switch (theta decay is slow, better cadence available)
                _far_otm = delta < 0.20
                _long_dte = dte > 30
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

                # Only upgrade urgency if not already at a higher urgency from a prior gate
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
                # Persist across EV override — EV block replaces Rationale wholesale,
                # but cadence context should always be visible since it's actionable regardless
                # of what the EV comparator decides.
                result['_cadence_note'] = _cadence_text

        # 6. Persistence escalation (Passarelli Ch.5)
        if row.get('Drift_Persistence') == 'Sustained' and result['Action'] != 'HOLD':
            result['Urgency'] = 'CRITICAL'
            result['Rationale'] += " Sustained drift confirms trend; immediate action required (Passarelli Ch.5)."

        # 7. Thesis regime degradation (buy-write — escalate if entry setup gone)
        thesis = DoctrineAuthority._check_thesis_degradation(row)
        if thesis:
            if result.get('Urgency', 'LOW') == 'LOW':
                result['Urgency'] = 'MEDIUM'
            result['Rationale'] += f" Entry regime degraded: {thesis['text']} (McMillan Ch.4: Thesis Persistence)."

        # 8. IV term structure note (Natenberg Ch.11)
        # BACKWARDATION: collecting inflated near-term vol — favorable for short premium
        iv_shape = str(row.get('iv_surface_shape', '') or '').upper()
        if iv_shape == 'BACKWARDATION':
            slope = float(row.get('iv_ts_slope_30_90', 0) or 0)
            result['Rationale'] += (
                f" IV BACKWARDATED ({slope:+.1f}pt 30-90d): collecting elevated near-term IV "
                f"— premium receipt above normal; favorable short-vol entry (Natenberg Ch.11)."
            )

        # 9. Action EV Comparator — compute and DECIDE.
        # At this point all emergency gates (hard-stop / DTE<7 / delta>0.70) have already
        # returned. Thesis-broken gate has returned. What remains is the discretionary
        # zone where pure gate logic (50% capture, negative carry) may disagree with EV.
        #
        # Contract:
        #   - EV winner OVERRIDES the current Action/Urgency when margin > $50 (noise floor)
        #     AND thesis is not BROKEN AND the override is directionally coherent.
        #   - ASSIGN winner → HOLD (let it expire/assign naturally; no active roll needed)
        #   - BUYBACK winner → only overrides when ev_buyback_trigger is also True
        #     (requires gamma_dominant + breakout + IV cheap — not just a math artifact)
        #   - Override is explicit in Rationale with the EV table so it's auditable.
        #
        # Passarelli Ch.6: "The roll decision is optimal only when it maximizes risk-adjusted
        # return over the remaining holding period — not when a threshold is crossed."
        try:
            _ev = DoctrineAuthority._compare_actions_bw(
                row            = row,
                spot           = spot,
                strike         = strike,
                effective_cost = effective_cost,
                qty            = abs(float(row.get('Quantity', 1) or 1)),
                dte            = max(dte, 1),
            )
            # Store all EV fields regardless of override
            result["Action_EV_Ranking"] = _ev["ranked_actions"]
            result["Action_EV_Winner"]  = _ev["ev_winner"]
            result["Action_EV_Margin"]  = _ev["ev_margin"]
            result["Action_EV_Hold"]    = _ev["ev_hold"]
            result["Action_EV_Roll"]    = _ev["ev_roll"]
            result["Action_EV_Assign"]  = _ev["ev_assign"]
            result["Action_EV_Buyback"] = _ev["ev_buyback"]
            result["EV_Buyback_Trigger"]= _ev["ev_buyback_trigger"]
            result["Gamma_Drag_Daily"]  = _ev["gamma_drag_daily"]

            _ev_winner      = _ev["ev_winner"]
            _ev_margin      = _ev["ev_margin"]
            _ev_summary     = _ev["ev_summary"]
            _prior_action   = result.get("Action", "HOLD")
            _thesis_ok      = not result.get("_thesis_blocks_roll", False)
            _EV_NOISE_FLOOR = 50.0   # $ — below this, EV difference is noise, keep prior action

            # ── Decision override logic ───────────────────────────────────────
            _ev_overrode = False

            if _thesis_ok and _ev_margin >= _EV_NOISE_FLOOR:

                if _ev_winner == "HOLD" and _prior_action != "HOLD":
                    # EV says holding is better than rolling — downgrade ROLL → HOLD
                    result.update({
                        "Action":   "HOLD",
                        "Urgency":  "LOW",
                        "Rationale": (
                            f"EV decision: HOLD dominates ROLL by ${_ev_margin:,.0f} "
                            f"over {dte:.0f}d horizon. "
                            f"Theta carry exceeds roll cost + new carry reset. "
                            f"Prior gate said ROLL ({_prior_action}) — overridden by EV. "
                            f"{_ev_summary}  "
                            f"(Passarelli Ch.6: roll only when it maximises holding-period return)"
                        ),
                        "Doctrine_Source": "ActionEV: HOLD > ROLL",
                        "Decision_State":  STATE_NEUTRAL_CONFIDENT,
                        "Required_Conditions_Met": True,
                    })
                    _ev_overrode = True

                elif _ev_winner == "ROLL" and _prior_action == "HOLD":
                    # EV says rolling is better than holding — but check timing + credit before
                    # confirming ROLL vs ROLL_WAIT.
                    # Gate 4: credit executable? IV_vs_HV_Gap >= 0 means IV ≥ HV (good credit).
                    #         IV_Percentile >= 70 means this is near the recent-range peak (good timing).
                    # Gate 5: DTE urgency? If DTE ≤ 30, roll now regardless of timing softness.
                    _iv_gap   = pd.to_numeric(row.get('IV_vs_HV_Gap'),   errors='coerce')   # IV - HV
                    _iv_pctile= pd.to_numeric(row.get('IV_Percentile'),  errors='coerce')   # 0-100
                    _timing_is_wait = _timing['action_mod'] == 'WAIT'
                    _dte_urgent = dte <= 30

                    # Gate 4b: live chain data present?
                    # Roll_Candidate_1 is populated only when Schwab chain was fetched during
                    # market hours. Absent = EV roll estimate is model-only (uses call_last from
                    # snapshot), not a verified executable credit. Without it we can't confirm
                    # condition 4 ("credit is executable at current IV").
                    _rc1_raw = row.get('Roll_Candidate_1')
                    _has_live_chain = (
                        _rc1_raw is not None
                        and str(_rc1_raw) not in ('', 'nan', 'None')
                        and not (isinstance(_rc1_raw, float) and pd.isna(_rc1_raw))
                    )

                    # Credit soft failure: IV below HV AND not at recent-range high AND no DTE urgency
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
                            "Action":   "ROLL_WAIT",
                            "Urgency":  "LOW",
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
                            "Decision_State":  STATE_NEUTRAL_CONFIDENT,
                            "Required_Conditions_Met": False,
                        })
                    else:
                        result.update({
                            "Action":   "ROLL",
                            "Urgency":  "MEDIUM",
                            "Rationale": (
                                f"EV decision: ROLL dominates HOLD by ${_ev_margin:,.0f} "
                                f"over {dte:.0f}d horizon. "
                                f"Extrinsic credit exceeds carry reset cost; holding erodes more. "
                                f"Prior gate said HOLD — overridden by EV. "
                                f"{_ev_summary}  "
                                f"(Passarelli Ch.6: roll when EV of new cycle exceeds hold EV)"
                            ),
                            "Doctrine_Source": "ActionEV: ROLL > HOLD",
                            "Decision_State":  STATE_ACTIONABLE,
                            "Required_Conditions_Met": True,
                        })
                    _ev_overrode = True

                elif _ev_winner == "ASSIGN":
                    # Assignment is the highest-EV outcome — no roll needed, let it happen
                    _assign_profit = _ev["ev_assign"]
                    result.update({
                        "Action":   "HOLD",
                        "Urgency":  "LOW",
                        "Rationale": (
                            f"EV decision: ASSIGN is optimal (+${_assign_profit:,.0f} certain proceeds). "
                            f"Strike ${strike:.2f} ≥ net cost ${effective_cost:.2f} — "
                            f"assignment locks in a profit. No roll needed; let expiry proceed. "
                            f"{_ev_summary}  "
                            f"(McMillan Ch.3: 'When assignment is profitable, rolling is optional — "
                            f"only roll if you want to defer the gain or capture more upside')"
                        ),
                        "Doctrine_Source": "ActionEV: ASSIGN optimal",
                        "Decision_State":  STATE_NEUTRAL_CONFIDENT,
                        "Required_Conditions_Met": True,
                    })
                    _ev_overrode = True

                elif _ev_winner == "BUYBACK" and _ev["ev_buyback_trigger"]:
                    # Buyback is dominant AND gamma/breakout conditions confirmed
                    # → surface as ROLL (user needs to execute: buy back call, then decide on stock)
                    result.update({
                        "Action":   "ROLL",
                        "Urgency":  "HIGH",
                        "Rationale": (
                            f"⚡ EV decision: BUYBACK dominates by ${_ev_margin:,.0f}. "
                            f"Gamma drag ${_ev['gamma_drag_daily']:.2f}/contract/day exceeding theta. "
                            f"Breakout confirmed (ADX rising, ROC expanding). IV cheap to close. "
                            f"Buy back short call NOW to capture uncapped stock upside. "
                            f"{_ev_summary}  "
                            f"(Passarelli Ch.6: close short premium when breakout confirmed; Natenberg Ch.7: gamma/theta ratio)"
                        ),
                        "Doctrine_Source": "ActionEV: BUYBACK — gamma breakout",
                        "Decision_State":  STATE_ACTIONABLE,
                        "Required_Conditions_Met": True,
                    })
                    _ev_overrode = True

            # If no override, annotate the existing rationale with the EV table
            if not _ev_overrode:
                result["Rationale"] += f"  ·  {_ev_summary}"

            # Re-append cadence note if EV override replaced the rationale
            # (EV block uses result.update() which overwrites Rationale wholesale)
            _cadence_note_saved = result.pop('_cadence_note', None)
            if _cadence_note_saved and _cadence_note_saved not in result.get('Rationale', ''):
                result['Rationale'] += _cadence_note_saved

        except Exception as _ev_err:
            logger.debug(f"[DoctrineEngine] Action EV comparator skipped: {_ev_err}")

        # Final cadence note recovery (if EV block raised an exception or was skipped)
        _cadence_note_saved = result.pop('_cadence_note', None)
        if _cadence_note_saved and _cadence_note_saved not in result.get('Rationale', ''):
            result['Rationale'] += _cadence_note_saved

        return result

    @staticmethod
    def _compare_actions_bw(row: pd.Series, spot: float, strike: float,
                             effective_cost: float, qty: float, dte: float) -> Dict[str, Any]:
        """
        Action Comparator for BUY_WRITE / COVERED_CALL positions.

        Computes a deterministic expected value for four competing actions:
          HOLD      — collect remaining theta, pay carry cost, keep position
          ROLL      — estimated credit from extrinsic value, reset DTE cycle
          ASSIGN    — certain: stock called away at strike, P&L is fixed now
          BUYBACK   — certain cost: close short call, hold stock naked

        This is NOT a full probability-weighted EV model. It uses:
          - Theta carry as the certain income stream for HOLD
          - Gamma drag estimate (Natenberg Ch.7: ½ × Γ × σ² × S²) as HOLD cost
          - Extrinsic remaining as ROLL credit proxy (lower-bound estimate)
          - Assignment outcome as certain proceeds (no probability needed)
          - Buyback cost from current ask as certain exit cost

        All EVs are in dollars over the remaining DTE horizon.
        The winner is the action with the highest EV (or least-negative).

        Returns a dict with:
          ev_hold, ev_roll, ev_assign, ev_buyback  (floats, $ over DTE)
          ranked_actions                            (list[str], highest to lowest)
          ev_winner                                 (str)
          ev_margin                                 (#1 minus #2, $ — confidence gap)
          ev_summary                                (one-line readable string)
          ev_buyback_trigger                        (bool — gamma conditions favor buyback)
          gamma_drag_daily                          ($ per day)
        """
        import math as _math

        n_contracts = max(1, int(abs(qty)))
        multiplier  = 100 * n_contracts  # per-contract → total dollars

        # ── Raw inputs ────────────────────────────────────────────────────────
        theta_raw   = abs(float(row.get('Theta', 0) or 0))          # per share per day
        gamma_raw   = abs(float(row.get('Gamma', 0) or 0))          # per share per $1 move
        hv_daily    = float(row.get('HV_20D', 0.20) or 0.20)        # annualised; convert below
        if hv_daily >= 1.0:
            hv_daily = hv_daily / 100.0                             # normalise 46.0 → 0.46
        hv_daily_frac = hv_daily / _math.sqrt(252)                  # daily σ fraction
        daily_sigma_dollars = spot * hv_daily_frac                  # expected daily $ move

        # Option price (to close)
        call_last  = abs(float(row.get('Short_Call_Last') or row.get('Last') or 0))
        call_ask   = abs(float(row.get('Ask', call_last * 1.02) or call_last * 1.02))
        call_bid   = abs(float(row.get('Bid', call_last * 0.98) or call_last * 0.98))
        premium_entry = abs(float(row.get('Short_Call_Premium') or row.get('Premium_Entry') or 0))

        # Carry cost
        capital     = effective_cost if effective_cost > 0 else spot
        daily_carry = capital * FIDELITY_MARGIN_RATE_DAILY          # $ per share per day

        # ── EV_HOLD ───────────────────────────────────────────────────────────
        # = theta income over DTE − carry cost − gamma drag over DTE
        # Gamma drag: Natenberg Ch.7 — short gamma costs ½Γσ²S² per day
        # For short call: we pay gamma drag (stock moves hurt us when deep ITM)
        #
        # Unit chain:
        #   theta_raw       = $ per share per day  (broker-reported)
        #   multiplier      = 100 × n_contracts    (converts per-share to total dollars)
        #   theta_income    = theta_raw × dte × multiplier
        #                   = ($/share/day) × days × (100shares/contract × n_contracts)
        #                   = $ total over horizon  ✅
        # Do NOT divide by 100 again — multiplier already contains the 100.
        gamma_drag_daily = 0.5 * gamma_raw * (daily_sigma_dollars ** 2)  # $ per share per day
        theta_income_total = theta_raw    * dte * multiplier
        carry_cost_total   = daily_carry  * dte * multiplier
        gamma_drag_total   = gamma_drag_daily * dte * multiplier
        ev_hold = theta_income_total - carry_cost_total - gamma_drag_total

        # ── EV_ROLL ───────────────────────────────────────────────────────────
        # Proxy: extrinsic value remaining = credit we could collect by rolling NOW.
        # Then assume rolling resets carry and gamma costs for a new 45-DTE cycle.
        # EV_roll = extrinsic_credit_collected − roll_slippage − new_carry_over_new_DTE
        intrinsic_val  = max(0.0, spot - strike)                     # per share
        extrinsic_val  = max(0.0, call_last - intrinsic_val)         # per share
        roll_slippage  = (call_ask - call_bid) / 2.0                 # half-spread cost per share
        new_dte_est    = 45.0                                         # assume standard 45-DTE roll
        new_carry      = daily_carry * new_dte_est * multiplier
        # Rolling captures extrinsic + resets carry; we don't model new theta (uncertain)
        ev_roll = (extrinsic_val * multiplier) - (roll_slippage * multiplier) - new_carry

        # ── EV_ASSIGN ─────────────────────────────────────────────────────────
        # Certain outcome: stock called at strike. Net = (strike − effective_cost) × shares.
        # Quantity on the OPTION leg = number of contracts (e.g. 2), not shares.
        # Assignment delivers 100 shares per contract → multiply by 100.
        # multiplier = 100 × n_contracts already encodes this for option-side calcs,
        # so EV_ASSIGN = assign_per_share × (n_contracts × 100) = assign_per_share × multiplier.
        assign_per_share = strike - effective_cost
        ev_assign = assign_per_share * multiplier   # multiplier = 100 × n_contracts = total shares

        # ── EV_BUYBACK ────────────────────────────────────────────────────────
        # Close short call at ask (certain cost), hold stock naked.
        # EV of the stock-only position over DTE is uncertain — we model it
        # as zero (neutral) because we don't know direction. The buyback cost
        # is certain; upside is thesis-dependent. This makes BUYBACK look conservative.
        buyback_cost = call_ask * multiplier                         # dollars to close
        ev_buyback   = -buyback_cost                                  # certain cost, uncertain upside

        # ── Gamma/breakout buyback trigger ────────────────────────────────────
        # McMillan Ch.3 / Natenberg Ch.7: buying back is correct when gamma drag
        # exceeds theta income AND a breakout is underway (you want uncapped upside).
        adx          = float(row.get('adx_14', 25) or 25)
        roc_5        = float(row.get('roc_5', 0) or 0)
        _greek_raw   = row.get('GreekDominance_State', '') or ''
        greek_dom    = (getattr(_greek_raw, 'value', None) or str(_greek_raw).split('.')[-1]).upper()
        iv_norm      = float(row.get('IV_30D', row.get('IV_Now', 0.30)) or 0.30)
        if iv_norm >= 5.0:
            iv_norm = iv_norm / 100.0
        ev_buyback_trigger = (
            greek_dom == 'GAMMA_DOMINANT'
            and adx > 28                                              # trending
            and roc_5 > 2.5                                          # accelerating up
            and iv_norm < 0.35                                       # IV low → cheap to close
            and gamma_drag_daily > theta_raw * 0.80                  # gamma eating theta
        )

        # ── Rank ──────────────────────────────────────────────────────────────
        ev_map = {
            "HOLD":    ev_hold,
            "ROLL":    ev_roll,
            "ASSIGN":  ev_assign,
            "BUYBACK": ev_buyback,
        }
        ranked = sorted(ev_map.items(), key=lambda x: x[1], reverse=True)
        ranked_actions = [a for a, _ in ranked]
        ev_winner      = ranked_actions[0]
        ev_margin      = ranked[0][1] - ranked[1][1]   # gap between #1 and #2

        # ── Summary line ──────────────────────────────────────────────────────
        def _fmt(v: float) -> str:
            return f"+${v:,.0f}" if v >= 0 else f"-${abs(v):,.0f}"

        # Noise-floor aware summary: when margin < $50, the difference is rounding noise.
        # Displaying "ASSIGN wins by +$1" would mislead — both ASSIGN and ROLL are tied.
        # Match the decision override logic (which also uses a $50 noise floor at line 1389).
        _EV_NOISE_FLOOR_SUMMARY = 50.0
        if ev_margin < _EV_NOISE_FLOOR_SUMMARY:
            ev_summary = (
                f"Action EV (over {dte:.0f}d): "
                f"HOLD {_fmt(ev_hold)} | ROLL {_fmt(ev_roll)} | "
                f"ASSIGN {_fmt(ev_assign)} | BUYBACK {_fmt(ev_buyback)} "
                f"→ statistical tie ({ev_winner}/{ranked_actions[1]} within ${ev_margin:.0f} noise floor) — "
                f"doctrine gates take precedence"
            )
        else:
            ev_summary = (
                f"Action EV (over {dte:.0f}d): "
                f"HOLD {_fmt(ev_hold)} | ROLL {_fmt(ev_roll)} | "
                f"ASSIGN {_fmt(ev_assign)} | BUYBACK {_fmt(ev_buyback)} "
                f"→ **{ev_winner}** wins by {_fmt(ev_margin)}"
            )

        if ev_buyback_trigger:
            ev_summary += (
                f" ⚡ Buyback trigger active: gamma drag ${gamma_drag_daily*100:.2f}/day "
                f"vs theta ${theta_raw*100:.2f}/day — breakout underway, IV cheap to close."
            )

        return {
            "ev_hold":             round(ev_hold,    2),
            "ev_roll":             round(ev_roll,    2),
            "ev_assign":           round(ev_assign,  2),
            "ev_buyback":          round(ev_buyback, 2),
            "ranked_actions":      ranked_actions,
            "ev_winner":           ev_winner,
            "ev_margin":           round(ev_margin,  2),
            "ev_summary":          ev_summary,
            "ev_buyback_trigger":  ev_buyback_trigger,
            "gamma_drag_daily":    round(gamma_drag_daily * 100, 4),  # per contract per day
        }

    @staticmethod
    def _covered_call_doctrine(row: pd.Series, result: Dict) -> Dict:
        # Use Short_Call_* enriched columns (Cycle 2.9) with raw fallback —
        # matches BUY_WRITE pattern.  On multi-leg CC rows, the STOCK leg carries
        # Short_Call_* via broadcast; the raw fallback handles single-leg rows.
        delta = abs(float(row.get('Short_Call_Delta') or row.get('Delta') or 0))
        dte = float(row.get('Short_Call_DTE') or row.get('DTE') or 999)
        moneyness = row.get('Moneyness_Label', 'OTM')
        lifecycle = row.get('Lifecycle_Phase', 'Early')
        drift_dir = row.get('Drift_Direction', '')
        drift_mag = row.get('Drift_Magnitude', '')

        # 1. Underlying collapse: stock fell hard — call premium doesn't cover stock loss
        # McMillan Ch.2: CC hard stop when underlying breaks support structurally
        if drift_mag == 'High' and drift_dir == 'Down':
            result.update({
                "Action": "EXIT",
                "Urgency": "CRITICAL",
                "Rationale": "Underlying price collapse — call premium insufficient to offset stock loss. Exit both legs (McMillan Ch.2: Hard Stop).",
                "Doctrine_Source": "McMillan Ch.2: Hard Stop",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result

        # 1a-post. Post-BUYBACK sticky gate (CC — mirrors BUY_WRITE logic)
        _cc_prior_action = str(row.get('Prior_Action', '') or '').upper()
        _cc_spot = abs(float(row.get('UL Last', 0) or row.get('Spot', 0) or 0))
        _cc_ei_state  = str(row.get('Equity_Integrity_State', '') or '').strip()
        _cc_ei_reason = str(row.get('Equity_Integrity_Reason', '') or '').strip()

        if _cc_prior_action == "BUYBACK" and _cc_ei_state != "INTACT":
            _cc_margin_cost = _cc_spot * FIDELITY_MARGIN_RATE_DAILY * 100 if _cc_spot > 0 else 0.0
            result.update({
                "Action": "HOLD",
                "Urgency": "HIGH",
                "Rationale": (
                    f"Post-BUYBACK hold — short call was removed, stock held unencumbered. "
                    f"Equity Integrity is {_cc_ei_state or 'UNKNOWN'} (not yet INTACT). "
                    f"Do NOT re-sell premium until structure confirms recovery. "
                    f"Stock carries ${_cc_margin_cost:.2f}/day margin cost "
                    f"with zero theta offset — accept this cost as the price of decoupling. "
                    f"(Jabbour Ch.11: re-evaluate only after structure resolves; "
                    f"McMillan Ch.3: stock and call decisions are independent)"
                ),
                "Doctrine_Source": "Post-BUYBACK: Equity not INTACT — hold unencumbered (CC)",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True,
            })
            return result

        # ── Gamma Danger Zone gate (CC — mirrors BUY_WRITE) ─────────────────
        # Natenberg Ch.7: near-ATM + low DTE = max gamma risk for short call.
        # Pre-emptive: fires BEFORE equity breaks.
        import math as _math_gdz_cc
        _gdz_cc_strike = float(row.get('Strike', 0) or 0)
        _gdz_cc_theta  = abs(float(row.get('Theta', 0) or 0))
        _gdz_cc_gamma  = abs(float(row.get('Gamma', 0) or 0))
        _gdz_cc_hv     = float(row.get('HV_20D', 0.20) or 0.20)
        if _gdz_cc_hv >= 1.0: _gdz_cc_hv /= 100.0
        _gdz_cc_sigma  = _cc_spot * (_gdz_cc_hv / _math_gdz_cc.sqrt(252)) if _cc_spot > 0 else 0.0
        _gdz_cc_drag   = 0.5 * _gdz_cc_gamma * (_gdz_cc_sigma ** 2)
        _gdz_cc_roc3   = float(row.get('Gamma_ROC_3D', 0) or 0)
        _gdz_cc_atm_pct = abs(_cc_spot - _gdz_cc_strike) / _cc_spot if _cc_spot > 0 and _gdz_cc_strike > 0 else 1.0
        _gdz_cc_ratio  = _gdz_cc_drag / _gdz_cc_theta if _gdz_cc_theta > 0 else 0.0

        _gdz_cc_fires = (
            _gdz_cc_atm_pct < 0.05
            and 7 < dte <= 21
            and _gdz_cc_ratio > 1.5
            and _cc_ei_state != 'BROKEN'
        )

        if _gdz_cc_fires:
            _gdz_cc_urgency = "HIGH" if _gdz_cc_roc3 > 0 else "MEDIUM"
            _gdz_cc_roc_note = (
                f"Gamma_ROC_3D={_gdz_cc_roc3:+.4f} (accelerating). "
                if _gdz_cc_roc3 > 0 else
                f"Gamma_ROC_3D={_gdz_cc_roc3:+.4f} (stable/declining). "
            )
            result.update({
                "Action": "ROLL",
                "Urgency": _gdz_cc_urgency,
                "Rationale": (
                    f"Gamma Danger Zone: near-ATM ({_gdz_cc_atm_pct:.1%} from strike "
                    f"${_gdz_cc_strike:.2f}), DTE={dte:.0f}, gamma/theta ratio={_gdz_cc_ratio:.1f}x. "
                    f"{_gdz_cc_roc_note}"
                    f"Gamma drag ${_gdz_cc_drag*100:.2f}c/contract/day approaching theta "
                    f"${_gdz_cc_theta*100:.2f}c/contract/day — short gamma accelerating toward dominance. "
                    f"Roll to extend DTE (30-45d target) or move strike OTM. "
                    f"Natenberg Ch.7: 'ATM + low DTE = max gamma risk for short gamma.' "
                    f"Passarelli Ch.6: 'Pre-emptive roll before gamma overwhelms theta.'"
                ),
                "Doctrine_Source": "Natenberg Ch.7: Gamma danger zone + Passarelli Ch.6: pre-emptive roll (CC)",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True,
            })
            return result

        # 1b. Equity Integrity gate (mirrors BUY_WRITE logic — C2 audit fix)
        # BROKEN:   3+ structural signals → don't roll into a structurally declining stock.
        # EXCEPTION — Gamma Dominance Buyback: when equity is BROKEN and gamma drag >
        # 2× theta carry, HOLD is mathematically losing money every day — but near-ATM
        # gamma is STRUCTURALLY HIGH at 2-3 weeks DTE (Passarelli Ch.6, Given).
        # Buyback requires CONVICTION: DTE ≤ 7, breakout through strike, or extrinsic < $0.20.
        # Without conviction → HOLD with informational gamma warning.
        # Moneyness guard: gamma dominance is only physically meaningful when the strike is
        # within 30% of spot (Black-Scholes: gamma is near-zero beyond this distance).
        # WEAKENING: annotated below (non-blocking).
        if _cc_ei_state == 'BROKEN' and result.get('Action') not in ('EXIT', 'HARD_HALT'):
            import math as _math_cc
            _cc_theta       = abs(float(row.get('Theta', 0) or 0))
            _cc_gamma       = abs(float(row.get('Gamma', 0) or 0))
            _cc_hv          = float(row.get('HV_20D', 0.20) or 0.20)
            if _cc_hv >= 1.0: _cc_hv /= 100.0
            _cc_sigma       = _cc_spot * (_cc_hv / _math_cc.sqrt(252)) if _cc_spot > 0 else 0.0
            _cc_gamma_drag  = 0.5 * _cc_gamma * (_cc_sigma ** 2)
            _cc_strike      = float(row.get('Strike', 0) or 0)
            _cc_otm_pct     = (
                abs(_cc_strike - _cc_spot) / _cc_spot
                if _cc_strike > 0 and _cc_spot > 0 else 0.0
            )
            _cc_gamma_dominant = (
                _cc_theta > 0
                and _cc_gamma_drag > _cc_theta * 2.0
                and _cc_otm_pct <= 0.30
            )
            if _cc_gamma_dominant:
                # Gamma drag > 2× theta — mathematically correct measurement.
                # But near-ATM gamma is STRUCTURALLY HIGH at 2-3 weeks DTE (Passarelli Ch.6,
                # Given Ch.X). Buyback requires CONVICTION — not just gamma math.
                #
                # Conviction gate (any one fires → buyback):
                #   A. DTE ≤ 7: expiration week — Passarelli Ch.6
                #   B. Stock above strike + upward momentum: breakout through strike
                #   C. Extrinsic < $0.20: no theta left to collect
                #
                # Without conviction → HOLD with gamma warning (informational).
                _cc_ratio     = _cc_gamma_drag / _cc_theta if _cc_theta > 0 else float('inf')
                _cc_call_last = abs(float(row.get('Last', 0) or 0))
                _cc_entry     = abs(float(row.get('Premium_Entry', 0) or row.get('Premium', 0) or 0))
                _cc_close_note = (
                    f" Call at ${_cc_call_last:.2f} vs ${_cc_entry:.2f} entry "
                    f"({'profit' if _cc_call_last < _cc_entry else 'debit'} to close)."
                    if _cc_call_last > 0 and _cc_entry > 0 else ""
                )

                # Compute extrinsic value: call price minus intrinsic (max(0, spot - strike))
                _cc_intrinsic = max(0.0, _cc_spot - _cc_strike) if _cc_spot > 0 and _cc_strike > 0 else 0.0
                _cc_extrinsic_val = max(0.0, _cc_call_last - _cc_intrinsic)

                # Direction/momentum from chart primitives
                _cc_adx = float(row.get('adx_14', 0) or 0)
                _cc_roc = float(row.get('roc_20', 0) or 0)

                _cc_buyback_conviction = (
                    dte <= 7                                                                    # A: expiration week
                    or (_cc_spot > _cc_strike * 1.01 and _cc_roc > 0 and _cc_adx > 25)         # B: breakout through strike
                    or _cc_extrinsic_val < 0.20                                                  # C: no theta left
                )

                if _cc_buyback_conviction:
                    # Which conviction signal fired?
                    _cc_conv_reason = (
                        "DTE ≤ 7 (expiration week)" if dte <= 7 else
                        f"stock ${_cc_spot:.2f} above strike ${_cc_strike:.2f} + momentum (ROC={_cc_roc:.1f}, ADX={_cc_adx:.0f})"
                        if _cc_spot > _cc_strike * 1.01 else
                        f"extrinsic only ${_cc_extrinsic_val:.2f} (< $0.20 — no theta left)"
                    )
                    result.update({
                        "Action": "ROLL",
                        "Urgency": "HIGH",
                        "Rationale": (
                            f"⚡ Equity BROKEN + gamma dominant ({_cc_ratio:.1f}× theta) + "
                            f"buyback conviction ({_cc_conv_reason}): "
                            f"gamma drag ${_cc_gamma_drag*100:.2f}/contract/day vs theta "
                            f"${_cc_theta*100:.2f}/contract/day — HOLD bleeds ${(_cc_gamma_drag - _cc_theta)*100:.2f}/contract/day. "
                            f"{_cc_close_note} "
                            f"BUY BACK the short call to stop gamma bleed and decouple from stock decision. "
                            f"Then evaluate stock independently: if thesis broken → sell; "
                            f"if temporary → re-sell 30–45 DTE call for better theta efficiency. "
                            f"(Passarelli Ch.6: close short premium in expiration week; "
                            f"Natenberg Ch.7: short gamma at {_cc_ratio:.1f}× theta is structurally unprofitable)"
                        ),
                        "Doctrine_Source": "Passarelli Ch.6: Expiration week close + Natenberg Ch.7: gamma/theta ratio (CC)",
                        "Decision_State": STATE_ACTIONABLE,
                        "Required_Conditions_Met": True,
                    })
                    return result
                else:
                    # Gamma dominant but no conviction: structurally expected near-ATM at
                    # this DTE. Preserve theta income; inform trader of gamma exposure.
                    result.update({
                        "Action": "HOLD",
                        "Urgency": "MEDIUM",
                        "Rationale": (
                            f"⚠️ Equity BROKEN + gamma elevated ({_cc_ratio:.1f}× theta) — "
                            f"structurally expected at DTE {dte:.0f} near ATM (δ={delta:.2f}). "
                            f"Gamma drag ${_cc_gamma_drag*100:.2f}/contract/day vs theta "
                            f"${_cc_theta*100:.2f}/contract/day. "
                            f"Theta income still collecting — hold unless conviction develops: "
                            f"(A) stock breaks above ${_cc_strike:.2f} with momentum → buy back call, "
                            f"(B) DTE approaches expiration week (≤7d) → close or roll. "
                            f"(Passarelli Ch.6: near-ATM gamma at 2–3 weeks is structural, "
                            f"not an emergency — close short premium in expiration week, not before. "
                            f"Given: 'large gamma values are the reason ATM positions change value "
                            f"rapidly during expiration week' — this is expected behaviour at DTE {dte:.0f})"
                        ),
                        "Doctrine_Source": "Passarelli Ch.6: Gamma awareness — expiration week rule (CC)",
                        "Decision_State": STATE_ACTIONABLE,
                        "Required_Conditions_Met": True,
                    })
                    return result
            else:
                # Standard BROKEN (CC): check for LEAPS carry inversion first.
                _cci_is_leaps = dte > 90
                if _cci_is_leaps:
                    _cci_margin_daily = _cc_spot * FIDELITY_MARGIN_RATE_DAILY if _cc_spot > 0 else 0.0
                    _cci_theta_daily  = abs(float(row.get('Theta', 0) or 0))
                    _cci_strike = float(row.get('Strike', 0) or 0) or 0.0
                    _cci_pct_otm = (
                        abs(_cc_spot - _cci_strike) / _cc_spot
                        if _cc_spot > 0 and _cci_strike > 0 else 0.0
                    )

                    if _cci_margin_daily > 0 and _cci_theta_daily > 0 and _cci_margin_daily >= _cci_theta_daily:
                        _cci_ratio = _cci_margin_daily / _cci_theta_daily
                        _CCI_SEVERE_RATIO = 1.5

                        if _cci_ratio >= _CCI_SEVERE_RATIO:
                            result.update({
                                "Action": "BUYBACK",
                                "Urgency": "HIGH",
                                "Rationale": (
                                    f"⚠️ Equity BROKEN + LEAPS carry severely inverted: "
                                    f"margin cost ${_cci_margin_daily*100:.2f}/contract/day vs theta income "
                                    f"${_cci_theta_daily*100:.2f}/contract/day ({_cci_ratio:.1f}× theta). "
                                    f"Strike ${_cci_strike:.0f} is {_cci_pct_otm:.0%} OTM — theta too weak "
                                    f"to cover financing. Buy back the short call. "
                                    f"Do NOT re-sell while equity is BROKEN ({_cc_ei_reason}) — hold stock "
                                    f"unencumbered until structural deterioration resolves. "
                                    f"(Given Ch.6: 'sell calls within one strike of ATM'; "
                                    f"Jabbour Ch.11: 'close and re-evaluate rather than rolling a losing structure')"
                                ),
                                "Doctrine_Source": "Given Ch.6: LEAPS Carry Inversion (severe) + EquityIntegrity: BROKEN (CC)",
                                "Decision_State": STATE_ACTIONABLE,
                                "Required_Conditions_Met": True,
                            })
                            return result
                        else:
                            _cci_net_bleed = (_cci_margin_daily - _cci_theta_daily) * 100
                            result.update({
                                "Action": "HOLD",
                                "Urgency": "HIGH",
                                "Rationale": (
                                    f"⚠️ Equity BROKEN + LEAPS carry mildly inverted: "
                                    f"margin ${_cci_margin_daily*100:.2f}/day vs theta "
                                    f"${_cci_theta_daily*100:.2f}/day ({_cci_ratio:.1f}× theta, "
                                    f"net bleed ${_cci_net_bleed:.2f}/day). "
                                    f"Buying back the call would increase bleed to "
                                    f"${_cci_margin_daily*100:.2f}/day with zero income. "
                                    f"HOLD — the short call still offsets most margin cost. "
                                    f"Monitor: if ratio exceeds 1.5× or equity recovers to re-sell "
                                    f"closer to ATM (Given Ch.6). Strike ${_cci_strike:.0f} "
                                    f"is {_cci_pct_otm:.0%} OTM."
                                ),
                                "Doctrine_Source": "Given Ch.6: LEAPS Carry Mild Inversion + EquityIntegrity: BROKEN (CC)",
                                "Decision_State": STATE_ACTIONABLE,
                                "Required_Conditions_Met": True,
                            })
                            return result

                result.update({
                    "Action": "HOLD",
                    "Urgency": "HIGH",
                    "Rationale": (
                        f"⚠️ Equity Integrity BROKEN — structural deterioration detected "
                        f"({_cc_ei_reason}). "
                        f"Rolling locks in deeper commitment to a structurally declining stock. "
                        f"Hold and reassess before next roll "
                        f"(McMillan Ch.1: confirm trend context before committing further capital)."
                    ),
                    "Doctrine_Source": "EquityIntegrity: BROKEN structural gate (CC)",
                    "Decision_State": STATE_ACTIONABLE,
                    "Required_Conditions_Met": True,
                })
                return result

        # 1b-LEAPS. Carry Inversion Gate (CC, non-BROKEN) — margin exceeds theta.
        # Given Ch.6 (0.764): "sell calls within one strike of ATM only"
        # Augen (0.704): "roll forward only when the new position has similar dynamics"
        _cci_is_leaps_nb = dte > 90
        if _cci_is_leaps_nb:
            _cci_margin_daily_nb = _cc_spot * FIDELITY_MARGIN_RATE_DAILY if _cc_spot > 0 else 0.0
            _cci_theta_daily_nb  = abs(float(row.get('Theta', 0) or 0))
            _cci_strike_nb = float(row.get('Strike', 0) or 0) or 0.0
            _cci_pct_otm_nb = (
                abs(_cc_spot - _cci_strike_nb) / _cc_spot
                if _cc_spot > 0 and _cci_strike_nb > 0 else 0.0
            )

            if _cci_margin_daily_nb > 0 and _cci_theta_daily_nb > 0 and _cci_margin_daily_nb >= _cci_theta_daily_nb:
                _cci_ratio_nb = _cci_margin_daily_nb / _cci_theta_daily_nb
                result.update({
                    "Action": "ROLL",
                    "Urgency": "MEDIUM",
                    "Rationale": (
                        f"📊 LEAPS carry inverted: margin ${_cci_margin_daily_nb*100:.2f}/contract/day ≥ "
                        f"theta ${_cci_theta_daily_nb*100:.2f}/contract/day ({_cci_ratio_nb:.1f}× theta). "
                        f"Strike ${_cci_strike_nb:.0f} is {_cci_pct_otm_nb:.0%} OTM — theta decays too slowly "
                        f"at this distance to cover financing at 10.375%/yr. "
                        f"Buy back and re-sell 30–45 DTE closer to ATM for efficient carry "
                        f"(Given Ch.6: 'one strike from ATM'; "
                        f"Augen: 'roll when the new position has similar dynamics')."
                    ),
                    "Doctrine_Source": "Given Ch.6: LEAPS Carry Inversion — re-sell closer (CC)",
                    "Decision_State": STATE_ACTIONABLE,
                    "Required_Conditions_Met": True,
                })
                return result

        # 1c. Dividend assignment gate — M1 audit fix (CC side).
        # When a stock pays a dividend and the short call is ITM/near-ITM (delta > 0.50),
        # call owners will exercise early to capture the dividend. This creates assignment risk
        # that is CALENDAR-DRIVEN, not delta-driven — it can happen even before expiry.
        # McMillan Ch.2: "Any ITM call with delta > 0.50 the day before ex-dividend is
        # subject to early exercise by the call holder who wants to capture the dividend."
        # Action: roll up/out BEFORE the ex-dividend date to avoid forced assignment.
        # Urgency escalation: CRITICAL if < 2 days, HIGH if 2-4 days.
        _cc_days_div   = float(row.get('Days_To_Dividend', 9999) or 9999)
        _cc_div_amt    = float(row.get('Dividend_Amount', 0) or 0)
        if delta > 0.50 and _cc_days_div < 5 and _cc_div_amt > 0:
            _cc_div_urgency = "CRITICAL" if _cc_days_div < 2 else "HIGH"
            result.update({
                "Action": "ROLL",
                "Urgency": _cc_div_urgency,
                "Rationale": (
                    f"⚠️ Dividend assignment risk: ex-dividend in {_cc_days_div:.0f} day(s) "
                    f"(${_cc_div_amt:.2f}/share), call delta={delta:.2f} > 0.50. "
                    f"Call owners will exercise early to capture the dividend — "
                    f"forced assignment before expiry. Roll up/out NOW to avoid early assignment "
                    f"(McMillan Ch.2: dividend-driven early exercise; "
                    f"Passarelli Ch.5: roll before ex-date when delta > 0.50)."
                ),
                "Doctrine_Source": "McMillan Ch.2: Dividend Assignment Risk (M1)",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True,
            })
            return result

        # 2. ITM/ATM-with-high-delta + Late: assignment imminent — roll up/out or let assign
        # McMillan Ch.2: if stock above strike at expiry, roll or accept assignment.
        # Also catches ATM-labeled positions where delta > 0.60 — the 5% ATM band can
        # include calls that are behaviorally ITM (e.g. $260 strike, stock at $271 = 4.6%
        # above strike but delta=0.75 → assignment risk is real despite ATM label).
        _is_behaviorally_itm = (moneyness == 'ITM') or (moneyness == 'ATM' and delta > 0.60)
        if lifecycle == 'Late' and _is_behaviorally_itm:
            result.update({
                "Action": "ROLL",
                "Urgency": "HIGH",
                "Rationale": f"Call ITM/near-ITM (Delta={delta:.2f}) with DTE={dte:.0f} — assignment risk imminent. Roll up/out or accept assignment if stock thesis intact (McMillan Ch.2).",
                "Doctrine_Source": "McMillan Ch.2: Expiration Management",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result

        # 3. ITM/near-ITM before expiry — dual-stage delta guard (H1 audit fix: match BUY_WRITE).
        # BUY_WRITE has two-stage: 0.55 (early warning) → 0.70 (emergency roll).
        # CC previously had a single gate at 0.65. Aligned to dual-stage for consistency.
        #
        # Stage A — Delta > 0.70: Emergency — call moving dollar-for-dollar with stock.
        # At delta ≥ 0.70 the income premise of a covered call is gone: you've effectively
        # capped the stock's upside entirely. Buy back immediately and roll up/out.
        # Passarelli Ch.5: "When the short call's delta exceeds 0.70, the income strategy
        # has become a pure short-stock synthetic — the call decision must be made NOW."
        #
        # Stage B — Delta 0.55–0.70: Early warning — call starting to cap upside materially.
        # This is NOT yet an emergency, but the trend toward assignment is clear. Surface as
        # advisory ROLL with MEDIUM urgency: "Consider rolling up before assignment accelerates."
        # McMillan Ch.2: "The time to roll a covered call is before it goes deeply ITM,
        # not after — early action preserves more premium in the roll."
        if delta > 0.70:
            result.update({
                "Action": "ROLL",
                "Urgency": "HIGH",
                "Rationale": (
                    f"📛 Call delta={delta:.2f} > 0.70 — moving nearly dollar-for-dollar with stock. "
                    f"Income purpose of covered call is eliminated; assignment highly probable. "
                    f"BUY BACK the call and roll up/out immediately to restore income structure "
                    f"(Passarelli Ch.5: Uncap Upside; McMillan Ch.2: delta > 0.70 = emergency roll)."
                ),
                "Doctrine_Source": "Passarelli Ch.5: Uncap Upside (delta > 0.70 — emergency)",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result
        if delta > 0.55:
            result.update({
                "Action": "ROLL",
                "Urgency": "MEDIUM",
                "Rationale": (
                    f"⚠️ Call delta={delta:.2f} > 0.55 — upside cap becoming material. "
                    f"Early warning: roll up/out now while the debit-to-close is still manageable "
                    f"and the roll premium is favorable. Acting at 0.55 rather than 0.70 "
                    f"preserves more premium in the new cycle "
                    f"(McMillan Ch.2: early roll before deeply ITM; Passarelli Ch.5: dual-stage delta gate)."
                ),
                "Doctrine_Source": "Passarelli Ch.5 + McMillan Ch.2: Early Warning (delta 0.55–0.70)",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result

        # 4a. Pin risk / expiration urgency — DTE ≤ 7 (fires before thesis check)
        # McMillan Ch.3: "At expiration week, gamma accelerates — small moves cause
        # large delta shifts. Roll before pin risk crystallizes regardless of thesis."
        # This gate supersedes thesis persistence because operational urgency
        # (imminent expiration) overrides analytical context at ≤ 7 DTE.
        try:
            _dte_float = float(dte)
        except (TypeError, ValueError):
            _dte_float = 999.0
        if _dte_float <= 7:
            _thesis_addendum = ""
            _thesis_check = DoctrineAuthority._check_thesis_degradation(row)
            if _thesis_check:
                _thesis_addendum = f" Note: {_thesis_check['text']}."
            result.update({
                "Action": "ROLL",
                "Urgency": "HIGH",
                "Rationale": (
                    f"DTE={_dte_float:.0f} — expiration imminent. "
                    f"Pin risk and gamma acceleration require rolling now. "
                    f"Roll to next cycle before time value collapses entirely."
                    + _thesis_addendum
                    + " (McMillan Ch.3: Expiration Management)."
                ),
                "Doctrine_Source": "McMillan Ch.3: Expiration Management",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result

        # 4a-21. AUDIT FIX: 21-DTE income gate (was missing from CC — BW and CSP both have it).
        # Given (No-Hype Options Trading Ch.6) + Passarelli Ch.2:
        # When DTE ≤ 21 and 50% profit NOT yet captured, the gamma-theta ratio has degraded.
        # The premium-seller edge is structurally exhausted. Roll to reset.
        _cc_pe_21 = abs(float(row.get('Premium_Entry', 0) or row.get('Premium', 0) or 0))
        _cc_last_21 = abs(float(row.get('Last', 0) or 0))
        _cc_captured_21 = (
            (_cc_pe_21 - _cc_last_21) / _cc_pe_21
            if _cc_pe_21 > 0 else 0.0
        )
        _cc_moneyness_21 = str(row.get('Moneyness_Label') or row.get('Short_Call_Moneyness') or 'OTM')
        if (
            dte <= 21
            and dte >= 7              # DTE<7 handled by emergency gate above
            and _cc_captured_21 < 0.50  # 50% NOT yet captured
            and _cc_moneyness_21 != 'ITM'  # ITM = assignment gate above
        ):
            _cc_21_urgency = 'MEDIUM' if _cc_captured_21 >= 0 else 'HIGH'
            result.update({
                "Action": "ROLL",
                "Urgency": _cc_21_urgency,
                "Rationale": (
                    f"21-DTE income gate: DTE={dte:.0f} ≤ 21 with only "
                    f"{_cc_captured_21:.0%} profit captured (need ≥50%). "
                    f"Gamma-theta ratio has degraded — short call edge exhausted. "
                    f"Buy back current call and roll out to 30-45 DTE to reset cycle. "
                    f"(Given Ch.6: 21-DTE income roll; Passarelli Ch.2: theta/gamma degradation.)"
                ),
                "Doctrine_Source": "Given Ch.6 + Passarelli Ch.2: 21-DTE Income Roll Gate (CC)",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True,
            })
            return result

        # 4b. 50% premium captured with time remaining — redeploy
        # Passarelli Ch.6: standard 50% profit-take for income strategies
        # AUDIT FIX: was comparing per-share Premium to total-dollar Current_Value
        # (100× units mismatch). Now uses per-share Premium_Entry vs per-share Last,
        # matching BUY_WRITE's Short_Call_Premium / Short_Call_Last pattern.
        _cc_premium_entry = abs(float(row.get('Premium_Entry', 0) or row.get('Premium', 0) or 0))
        _cc_current_cost  = abs(float(row.get('Last', 0) or 0))
        if _cc_premium_entry > 0 and _cc_current_cost <= _cc_premium_entry * 0.50 and dte > 21:
            _cc_pct_captured = 1 - (_cc_current_cost / _cc_premium_entry)
            result.update({
                "Action": "ROLL",
                "Urgency": "MEDIUM",
                "Rationale": (
                    f"50% premium captured ({_cc_pct_captured:.0%} of ${_cc_premium_entry:.2f} entry credit) "
                    f"with {dte:.0f} DTE remaining — close and redeploy into next cycle "
                    f"(Passarelli Ch.6: 50% Rule)."
                ),
                "Doctrine_Source": "Passarelli Ch.6: 50% Rule",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result

        # 5a. Thesis regime degradation (covered call — regime shifted since entry)
        thesis = DoctrineAuthority._check_thesis_degradation(row)
        if thesis:
            result.update({
                "Action": "ROLL",
                "Urgency": "MEDIUM",
                "Rationale": (
                    f"Entry regime degraded: {thesis['text']}. "
                    f"Reassess strike/expiry — original setup no longer intact (McMillan Ch.2: Thesis Persistence)."
                ),
                "Doctrine_Source": "McMillan Ch.2: Thesis Persistence",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result

        # 5. Default: OTM or sufficient time — let theta work
        # Add IV term structure note for short-premium context
        iv_shape_cc = str(row.get('iv_surface_shape', '') or '').upper()
        iv_note_cc = ""
        if iv_shape_cc == 'BACKWARDATION':
            slope_cc = float(row.get('iv_ts_slope_30_90', 0) or 0)
            iv_note_cc = (
                f" IV BACKWARDATED ({slope_cc:+.1f}pt): collecting elevated near-term IV "
                f"— premium received is above-normal (Natenberg Ch.11)."
            )

        # GAP 6 FIX — Forward expectancy context for covered calls.
        # For CCs, the profit thesis is: stock stays BELOW the strike, call expires worthless.
        # EV_Feasibility_Ratio here measures how far the stock needs to rally to hit the strike
        # vs the 10D expected move. A HIGH ratio = stock very unlikely to be called away.
        # A LOW ratio = stock is already near the strike and could move through it easily.
        # Surface this so the trader can see assignment risk in expected-move terms.
        _ev_cc = float(row.get('EV_Feasibility_Ratio', 0) or 0) if pd.notna(row.get('EV_Feasibility_Ratio')) else float('nan')
        _em_cc = float(row.get('Expected_Move_10D', 0) or 0)
        _req_cc = float(row.get('Required_Move_Breakeven', row.get('Required_Move', 0) or 0) or 0)
        _ev_cc_note = ""
        if not pd.isna(_ev_cc) and _ev_cc > 0 and _em_cc > 0:
            if _ev_cc < 0.5:
                _ev_cc_note = (
                    f" ⚠️ Strike proximity: stock only ${_req_cc:.1f} away from strike "
                    f"({_ev_cc:.2f}× 10D expected move ${_em_cc:.1f}). "
                    f"Assignment risk is elevated — consider rolling up/out proactively "
                    f"(Passarelli Ch.5: roll before ITM, not after)."
                )
            else:
                _ev_cc_note = (
                    f" Strike ${_req_cc:.1f} away ({_ev_cc:.2f}× 10D expected ${_em_cc:.1f}) "
                    f"— low assignment probability, theta working as planned."
                )

        # ── EV Comparator (mirrors BUY_WRITE — CC is economically identical) ──
        # _compare_actions_bw is designed for both BW and CC (same docstring).
        # Compute EV ranking and allow override when ROLL dominates HOLD by
        # margin > noise floor. Emergency gates (returned above) bypass this.
        _cc_ev_override = False
        try:
            _cc_strike = float(row.get('Strike', 0) or 0)
            _cc_qty = abs(float(row.get('Quantity', 1) or 1))
            _cc_dte_ev = max(float(dte), 1)
            # Effective cost: broker basis per share (same as BW)
            _cc_broker_basis = abs(float(row.get('Basis', 0) or 0))
            _cc_qty_abs = abs(float(row.get('Quantity', 1) or 1))
            _cc_cost_per_share = (_cc_broker_basis / _cc_qty_abs) if _cc_qty_abs > 0 and _cc_broker_basis > 0 else 0.0
            _cc_effective_cost = _cc_cost_per_share or float(row.get('Underlying_Price_Entry', 0) or 0) or _cc_spot

            _cc_ev = DoctrineAuthority._compare_actions_bw(
                row=row, spot=_cc_spot, strike=_cc_strike,
                effective_cost=_cc_effective_cost, qty=_cc_qty, dte=_cc_dte_ev,
            )
            # Store EV fields regardless of override
            result["Action_EV_Ranking"] = _cc_ev["ranked_actions"]
            result["Action_EV_Winner"]  = _cc_ev["ev_winner"]
            result["Action_EV_Margin"]  = _cc_ev["ev_margin"]
            result["Action_EV_Hold"]    = _cc_ev["ev_hold"]
            result["Action_EV_Roll"]    = _cc_ev["ev_roll"]
            result["Action_EV_Assign"]  = _cc_ev["ev_assign"]
            result["Action_EV_Buyback"] = _cc_ev["ev_buyback"]
            result["EV_Buyback_Trigger"]= _cc_ev["ev_buyback_trigger"]
            result["Gamma_Drag_Daily"]  = _cc_ev["gamma_drag_daily"]

            _cc_ev_winner = _cc_ev["ev_winner"]
            _cc_ev_margin = _cc_ev["ev_margin"]
            _cc_ev_summary = _cc_ev["ev_summary"]
            _cc_thesis_ok = not result.get("_thesis_blocks_roll", False)
            _CC_EV_NOISE = 50.0

            if _cc_thesis_ok and _cc_ev_margin >= _CC_EV_NOISE:
                if _cc_ev_winner == "ROLL":
                    result.update({
                        "Action": "ROLL",
                        "Urgency": "MEDIUM",
                        "Rationale": (
                            f"EV decision: ROLL dominates HOLD by ${_cc_ev_margin:,.0f} "
                            f"over {_cc_dte_ev:.0f}d horizon. "
                            f"Theta carry insufficient vs roll credit opportunity. "
                            f"{_cc_ev_summary}  "
                            f"(Passarelli Ch.6: roll when EV favors)"
                        ),
                        "Doctrine_Source": "ActionEV: ROLL > HOLD (CC)",
                        "Decision_State": STATE_ACTIONABLE,
                        "Required_Conditions_Met": True,
                    })
                    _cc_ev_override = True
        except Exception as _cc_ev_err:
            logger.debug(f"CC EV comparator error (non-fatal): {_cc_ev_err}")

        if not _cc_ev_override:
            result.update({
                "Action": "HOLD",
                "Urgency": "LOW",
                "Rationale": f"Call OTM ({moneyness}), DTE={dte:.0f} — theta working as intended.{iv_note_cc}{_ev_cc_note} No action required (McMillan Ch.2).",
                "Doctrine_Source": "McMillan Ch.2: Neutral Maintenance",
                "Decision_State": STATE_NEUTRAL_CONFIDENT,
                "Required_Conditions_Met": True
            })

        # H2: WEAKENING equity annotation — non-blocking, advisory only.
        # 1–2 structural signals: not broken enough to halt, but worth monitoring.
        # McMillan Ch.1: "Weakening structure is an early warning — don't wait for BROKEN."
        if _cc_ei_state == 'WEAKENING' and _cc_ei_reason:
            result['Rationale'] = (
                result.get('Rationale', '') +
                f"  [⚠️ Equity WEAKENING: {_cc_ei_reason} — monitor closely before next roll.]"
            )

        return result

    @staticmethod
    def _classify_roll_timing(row: pd.Series) -> dict:
        """
        Market timing intelligence for roll decisions.

        Classifies current market conditions as BREAKOUT, CHOPPY, or NEUTRAL to
        determine whether to roll immediately, wait for a better entry, or proceed
        normally. This prevents rolling into expensive, mean-reverting noise while
        ensuring breakouts are acted on promptly.

        McMillan Ch.3: "The most costly mistake in a buy-write is rolling the call
          into a choppy market — you accept a premium that's too thin and a strike
          that's instantly wrong as the stock oscillates."
        Passarelli Ch.6: "Wait for directional clarity before redeploying premium."
        Natenberg Ch.8: Range efficiency and trend confirmation as roll timing gates.

        Returns dict with keys:
          timing        : "BREAKOUT_UP" | "BREAKOUT_DOWN" | "CHOPPY" | "NEUTRAL"
          urgency_mod   : "CRITICAL" | "HIGH" | "MEDIUM" | "LOW" (suggested urgency)
          action_mod    : "ROLL_NOW" | "WAIT" | "PROCEED"
          reason        : human-readable explanation (appended to rationale)
        """
        # ── Dead-cat bounce suppression gate ────────────────────────────────
        # RecoveryQuality_State is computed by chart_state_engine and distinguishes
        # a 1–2 day uptick in a broken trend from a genuine structural recovery.
        # A DEAD_CAT_BOUNCE must not trigger discretionary rolls — the user would
        # be adapting strike/expiry to noise, not signal.
        # Passarelli Ch.6 / McMillan Ch.3: "Don't adapt to what hasn't changed."
        recovery_state = (row.get('RecoveryQuality_State') or '').upper()
        if 'DEAD_CAT' in recovery_state or recovery_state == 'DEAD_CAT_BOUNCE':
            resolution = row.get('RecoveryQuality_Resolution_Reason', '')
            return {
                "timing": "DEAD_CAT_BOUNCE",
                "urgency_mod": "LOW",
                "action_mod": "WAIT",
                "reason": (
                    f"Dead-cat bounce detected — structure has NOT changed. "
                    f"{resolution}. "
                    f"Wait for: higher low + break above prior swing high + ROC10 > 0 + EMA20 turning up. "
                    f"Passarelli Ch.6: don't adapt the roll to noise."
                )
            }

        # ── Read all relevant signal columns ────────────────────────────────
        chop       = float(row.get('choppiness_index', 50) or 50)
        ker        = float(row.get('kaufman_efficiency_ratio', 0.5) or 0.5)
        adx        = float(row.get('adx_14', 25) or 25)
        bb_z       = float(row.get('bb_width_z', 0) or 0)
        roc_5      = float(row.get('roc_5', 0) or 0)
        roc_10     = float(row.get('roc_10', 0) or 0)
        roc_20     = float(row.get('roc_20', 0) or 0)
        mom_slope  = float(row.get('momentum_slope', 0) or 0)

        def _sn(col):
            v = row.get(col, '') or ''
            return (getattr(v, 'value', None) or str(v).split('.')[-1]).upper()

        range_eff  = _sn('RangeEfficiency_State')   # EFFICIENT_TREND | INEFFICIENT_RANGE | NOISY | FAKE_BREAK
        trend_int  = _sn('TrendIntegrity_State')     # STRONG_TREND | WEAK_TREND | TREND_EXHAUSTED | NO_TREND
        mom_vel    = _sn('MomentumVelocity_State')   # ACCELERATING | DECELERATING | STALLING | REVERSING
        dir_bal    = _sn('DirectionalBalance_State') # BUYER_DOMINANT | SELLER_DOMINANT | BALANCED | CONTESTED
        tf_agree   = _sn('TimeframeAgreement_State') # ALIGNED | PARTIAL | DIVERGENT
        comp_mat   = _sn('CompressionMaturity_State') # EARLY_COMPRESSION | MATURE_COMPRESSION | RELEASING | POST_EXPANSION

        # ── Classify ─────────────────────────────────────────────────────────

        # BREAKOUT_UP: stock breaking above call strike territory — act NOW before
        # gamma explodes and the roll becomes very expensive (Natenberg Ch.8).
        #
        # GAP FIX: Prior version required all 6 conditions simultaneously — too strict,
        # rarely fired. Simulation showed most real breakouts classified as NEUTRAL.
        #
        # New logic (OR-based with two sufficient conditions):
        #   Primary signal:  Strong trend structure + buyers in control (most reliable)
        #   Secondary signal: High KER (efficient directional move) + short-term momentum
        # Either branch is sufficient to classify as BREAKOUT_UP and trigger ROLL_NOW.
        # McMillan Ch.3: "Roll before the stock runs through the strike, not after."
        _bu_primary   = (trend_int == 'STRONG_TREND'
                         and dir_bal == 'BUYER_DOMINANT'
                         and mom_vel in ('ACCELERATING', 'TRENDING'))
        _bu_secondary = (ker > 0.55
                         and roc_5 > 2.0
                         and chop < 50
                         and mom_vel in ('ACCELERATING', 'TRENDING'))
        is_breakout_up = _bu_primary or _bu_secondary

        # BREAKOUT_DOWN: stock collapsing — call loses value fast, roll is cheap NOW
        # and the stock may fall through the cost basis. Immediate defensive roll needed.
        is_breakout_down = (
            trend_int in ('STRONG_TREND', 'WEAK_TREND')
            and dir_bal == 'SELLER_DOMINANT'
            and mom_vel in ('ACCELERATING', 'TRENDING')
            and roc_5 < -2.0
            and roc_10 < -4.0
            and chop < 55
        )

        # CHOPPY: high choppiness + range-bound + no directional conviction
        # Rolling here = paying expensive theta for a strike that's likely wrong by next week.
        # McMillan Ch.3: "Wait for the market to show its hand before redeploying."
        is_choppy = (
            chop > 61.8                               # Classic choppiness threshold (Fibonacci)
            and ker < 0.35                            # Very inefficient price movement
            and range_eff in ('INEFFICIENT_RANGE', 'NOISY')
            and trend_int in ('NO_TREND', 'TREND_EXHAUSTED')
            and adx < 20                              # Weak trend strength
            and abs(roc_5) < 2.0                      # No short-term momentum
        )

        # COMPRESSION_RELEASING: BB width expanding from squeeze → breakout imminent
        # Wait 1-2 bars for direction to confirm rather than rolling into the ambiguity.
        is_releasing = (
            comp_mat in ('RELEASING', 'POST_EXPANSION')
            and bb_z > 0.5                            # Width expanding above recent mean
            and adx < 25                              # Trend not yet established
        )

        # ── Build response ────────────────────────────────────────────────────

        if is_breakout_up:
            _advisory = DoctrineAuthority._build_intraday_roll_advisory(row, "BREAKOUT_UP")
            return {
                "timing": "BREAKOUT_UP",
                "urgency_mod": "CRITICAL",
                "action_mod": "ROLL_NOW",
                "reason": (
                    f"BREAKOUT upward confirmed: chop={chop:.0f}, KER={ker:.2f}, "
                    f"ADX={adx:.0f}, ROC5=+{roc_5:.1f}%. "
                    f"Roll immediately — gamma acceleration will make this expensive to delay "
                    f"(McMillan Ch.3: Roll Timing / Natenberg Ch.8)."
                ),
                "intraday_advisory": _advisory,
            }

        if is_breakout_down:
            _advisory = DoctrineAuthority._build_intraday_roll_advisory(row, "BREAKOUT_DOWN")
            return {
                "timing": "BREAKOUT_DOWN",
                "urgency_mod": "HIGH",
                "action_mod": "ROLL_NOW",
                "reason": (
                    f"BREAKDOWN confirmed: chop={chop:.0f}, ROC5={roc_5:.1f}%, ROC10={roc_10:.1f}%. "
                    f"Roll call down/out now — premium cheap and stock falling toward cost basis "
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
                    f"Market CHOPPY: chop={chop:.0f} (>{61.8:.0f}), KER={ker:.2f} (<0.35), "
                    f"ADX={adx:.0f} (<20). "
                    f"Rolling now risks collecting thin premium into a whipsawing market — "
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
                    f"breakout direction not yet confirmed — wait 1-2 sessions for clarity "
                    f"before rolling to avoid picking the wrong strike (Natenberg Ch.8)."
                ),
                "intraday_advisory": None,
            }

        return {
            "timing": "NEUTRAL",
            "urgency_mod": None,   # caller keeps its own urgency
            "action_mod": "PROCEED",
            "reason": "",
            "intraday_advisory": None,
        }

    @staticmethod
    def _build_intraday_roll_advisory(row: pd.Series, timing: str) -> dict:
        """
        Intraday timing advisory for CRITICAL/HIGH urgency ROLL decisions.

        The system uses end-of-day price history — it cannot see intraday candles,
        VWAP, or real-time volume. This advisory surfaces:
          1. Live proxy signals derived from available data (UL Last, Delta, IV, ATR)
          2. Manual verification checklist — signals the trader must check in their
             broker chart before executing the roll.

        This is NOT a gate — the ROLL decision stands. The advisory answers:
        "Is right now within today's session the ideal execution window?"

        Passarelli Ch.6: "The right decision at the wrong intraday moment = worse fill,
          wider spread, larger slippage than the model assumed."
        McMillan Ch.3: "Roll timing within a session matters — the first 30 minutes
          and last 30 minutes carry meaningfully wider effective spreads."
        """
        import math

        # ── Live proxy signals ───────────────────────────────────────────────
        ul_last   = float(row.get('UL Last', 0) or 0)
        ul_prev   = float(row.get('UL_Prev_Close', 0) or 0)
        delta_now = abs(float(row.get('Delta', 0) or 0))
        delta_ent = abs(float(row.get('Delta_Entry', 0) or 0))
        iv_now    = float(row.get('IV_30D', 0) or 0)
        iv_entry  = float(row.get('IV_30D_Entry', 0) or 0)
        atr_14    = float(row.get('ATR_14', 0) or 0)
        roc_5     = float(row.get('roc_5', 0) or 0)
        bb_z      = float(row.get('bb_width_z', 0) or 0)
        adx       = float(row.get('adx_14', 25) or 25)

        signals = {}
        notes   = []

        # 1. Intraday momentum proxy: UL Last vs prev close
        intraday_chg_pct = 0.0
        if ul_prev > 0 and ul_last > 0:
            intraday_chg_pct = (ul_last - ul_prev) / ul_prev * 100
            signals['intraday_chg_pct'] = round(intraday_chg_pct, 2)
            if timing == 'BREAKOUT_UP' and intraday_chg_pct > 1.5:
                notes.append(
                    f"Stock +{intraday_chg_pct:.1f}% today — momentum running HOT. "
                    f"Verify on 5-min chart: is price still in impulse wave or showing "
                    f"deceleration candles (decreasing bar size, wicks forming)? "
                    f"Roll at next intraday pullback to EMA5/VWAP for tighter fill."
                )
            elif timing == 'BREAKOUT_UP' and intraday_chg_pct < 0.3:
                notes.append(
                    f"Daily trend BREAKOUT_UP but today's move is only +{intraday_chg_pct:.1f}%. "
                    f"Momentum may be pausing — check if price is consolidating above prior "
                    f"resistance or pulling back. Present moment may be ideal roll window."
                )
            elif timing == 'BREAKOUT_DOWN' and intraday_chg_pct < -1.5:
                notes.append(
                    f"Stock {intraday_chg_pct:.1f}% today — breakdown running HOT. "
                    f"Call premium is deflating fast. Execute roll before further drop "
                    f"makes the buyback cheaper but the new sale premium thinner."
                )

        # 2. Delta acceleration proxy: Delta now vs entry delta
        if delta_ent > 0 and delta_now > 0:
            delta_drift = (delta_now - delta_ent) / delta_ent * 100
            signals['delta_drift_pct'] = round(delta_drift, 1)
            if abs(delta_drift) > 20:
                direction = "risen" if delta_drift > 0 else "fallen"
                notes.append(
                    f"Delta has {direction} {abs(delta_drift):.0f}% from entry "
                    f"(entry: {delta_ent:.2f} → now: {delta_now:.2f}). "
                    f"{'Gamma is accelerating — roll delay = more expensive buyback.' if delta_drift > 0 else 'Gamma decelerating — premium thinning on new sale.'}"
                )

        # 3. IV expansion proxy (vol expansion on the move)
        if iv_entry > 0 and iv_now > 0:
            iv_drift_pct = (iv_now - iv_entry) / iv_entry * 100
            signals['iv_drift_pct'] = round(iv_drift_pct, 1)
            if iv_drift_pct > 15:
                notes.append(
                    f"IV has expanded {iv_drift_pct:.0f}% since entry (entry: {iv_entry:.1%} → now: {iv_now:.1%}). "
                    f"Volatility expansion on the move — new call premium will be richer than typical. "
                    f"Good timing for the sell leg. Verify VVIX not spiking (fear spike = bid/ask widening)."
                )
            elif iv_drift_pct < -15:
                notes.append(
                    f"IV has contracted {abs(iv_drift_pct):.0f}% since entry — premium is cheaper than expected. "
                    f"New call sale will collect less. Consider rolling to a closer strike to "
                    f"compensate for vol compression (Natenberg Ch.8: vol crush tradeoff)."
                )

        # 4. ATR context: how many ATRs is today's move?
        if atr_14 > 0 and ul_last > 0 and ul_prev > 0:
            intraday_abs = abs(ul_last - ul_prev)
            atr_multiple = intraday_abs / atr_14
            signals['atr_multiple'] = round(atr_multiple, 2)
            if atr_multiple > 1.5:
                notes.append(
                    f"Today's move ({intraday_abs:.2f}) = {atr_multiple:.1f}× ATR_14 ({atr_14:.2f}). "
                    f"This is an extended intraday move — spreads likely wider than normal. "
                    f"Use limit orders only; expect 10-20% wider effective spread than model assumes."
                )
            elif atr_multiple < 0.3:
                notes.append(
                    f"Today's move is only {atr_multiple:.1f}× ATR — stock is quiet intraday. "
                    f"Spreads should be tight. Normal limit order execution expected."
                )

        # 5. ADX trend strength context
        if adx > 30:
            signals['adx_strength'] = 'STRONG'
            notes.append(
                f"ADX={adx:.0f} (strong trend). The daily trend has directional conviction — "
                f"less risk that intraday consolidation reverses the move."
            )
        elif adx < 18:
            signals['adx_strength'] = 'WEAK'
            notes.append(
                f"ADX={adx:.0f} (weak trend). Despite timing signal, directional conviction is low. "
                f"More likely to see intraday reversals — verify on 15-min chart before executing."
            )

        # ── Manual verification checklist ───────────────────────────────────
        # These 6 signals cannot be derived from daily bars. Trader MUST check.
        checklist = []

        if timing in ('BREAKOUT_UP', 'BREAKOUT_DOWN'):
            checklist.append({
                "item": "Momentum deceleration",
                "description": (
                    "On 5-min chart: are bars getting smaller near current price level? "
                    "Wicks forming on the leading edge? RSI(5) diverging from price? "
                    "If yes → wait for one pullback candle before rolling."
                )
            })
            checklist.append({
                "item": "VWAP position",
                "description": (
                    "Is price above VWAP (bullish) or below (bearish)? "
                    "A BREAKOUT_UP roll with price below VWAP = weak breakout — "
                    "may snap back to VWAP before continuing. "
                    "Ideal: price extended above VWAP + first pullback to VWAP = "
                    "tightest spread window for the roll."
                )
            })
            checklist.append({
                "item": "EMA5/EMA8 angle",
                "description": (
                    "Are the fast EMAs (5/8 period on 5-min) still pointing in the "
                    "breakout direction, or have they started to flatten/curl back? "
                    "Flattening fast EMAs = momentum transfer from impulse to consolidation. "
                    "Good: execute now. Curling back: wait 1 bar."
                )
            })
            checklist.append({
                "item": "Reversal candle structure",
                "description": (
                    "Check the last 3 candles on the 5-min chart for: doji at HOD/LOD, "
                    "bearish engulfing (BREAKOUT_UP), hammer/reversal wick (BREAKOUT_DOWN). "
                    "Any reversal structure at the current price level = pause execution, "
                    "wait for the next directional candle to confirm continuation."
                )
            })
            checklist.append({
                "item": "Volume on the move",
                "description": (
                    "Is intraday volume tracking ABOVE average for this time of day? "
                    "Breakout on low volume = institutional absence = false breakout risk. "
                    "Breakout on high volume = confirmed demand/supply shift. "
                    "Rule: if volume < 70% of typical by 11am ET → treat as LOW conviction."
                )
            })
            checklist.append({
                "item": "Bid/ask spread on the call",
                "description": (
                    "Pull up the specific call option chain right now (not the last saved quote). "
                    "During fast moves, MM spreads widen 2-4×. "
                    "If spread > 3% of mid → use limit at mid; never lift the ask during "
                    "an active move (you are selling, not buying — post the offer)."
                )
            })
        else:
            # BREAKOUT_DOWN specific
            checklist.append({
                "item": "Support level test",
                "description": (
                    "Is stock near a known support (50-day MA, prior swing low, round number)? "
                    "Rolling into a support test = possible bounce that makes your new lower "
                    "strike immediately ITM. Confirm break of support before rolling down."
                )
            })

        # ── Overall intraday confidence ──────────────────────────────────────
        proxy_confirm_count = sum([
            intraday_chg_pct > 1.5 if timing == 'BREAKOUT_UP' else intraday_chg_pct < -1.5,
            delta_now > delta_ent * 1.1,
            iv_now > iv_entry if timing == 'BREAKOUT_UP' else iv_now < iv_entry,
            atr_multiple > 0.5 if atr_14 > 0 else False,
        ])
        if proxy_confirm_count >= 3:
            proxy_verdict = "EXECUTE_NOW"
            proxy_color   = "red"
            proxy_summary = (
                f"{proxy_confirm_count}/4 live proxies confirm the breakout. "
                f"Execute roll during current window — do not wait for next session."
            )
        elif proxy_confirm_count >= 2:
            proxy_verdict = "FAVORABLE_WINDOW"
            proxy_color   = "orange"
            proxy_summary = (
                f"{proxy_confirm_count}/4 live proxies align. "
                f"Good execution window — verify the 6-item checklist above before sending order."
            )
        else:
            proxy_verdict = "VERIFY_FIRST"
            proxy_color   = "blue"
            proxy_summary = (
                f"Only {proxy_confirm_count}/4 live proxies confirm. "
                f"Daily timing signal fired but intraday proxies are mixed. "
                f"Verify manual checklist before executing — consider waiting 30-60 min."
            )

        return {
            "proxy_verdict":  proxy_verdict,
            "proxy_color":    proxy_color,
            "proxy_summary":  proxy_summary,
            "signals":        signals,
            "notes":          notes,
            "checklist":      checklist,
        }

    @staticmethod
    def _check_thesis_degradation(row: pd.Series) -> dict | None:
        """
        Cross-temporal thesis check: compare frozen entry chart states vs current states.
        Returns an escalation dict if the regime that justified the trade has structurally
        shifted, else None.
        McMillan Ch.4 / Passarelli Ch.2: position management requires thesis persistence.

        CRITICAL — vol regime check is direction-aware:
          Long vol  (LONG_CALL, LONG_PUT, LEAP): COMPRESSED→EXTREME is thesis CONFIRMING.
            Natenberg Ch.11: the correct entry for long vol is during compression;
            expansion is the payoff. Flagging this as degradation is wrong.
          Short vol (COVERED_CALL, BUY_WRITE, SHORT_PUT): COMPRESSED→EXTREME is thesis
            BREAKING — sold premium into low vol, now vol spikes against the position.
        """
        def _sn(val):
            """Normalize enum objects and plain strings to the bare uppercase name."""
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return None
            s = str(val).strip()
            if not s or s in ('nan', 'None', 'N/A'):
                return None
            return (getattr(val, 'value', None) or s.split('.')[-1]).upper()

        # Minimum position age before thesis degradation can fire.
        # The entry-backfill classifier and the cycle-2 state engine use different
        # formulas — they can disagree on the same bar. Requiring ≥2 calendar days
        # ensures at least one full overnight session has separated entry from current state.
        _entry_ts = row.get('Entry_Snapshot_TS') or row.get('Snapshot_TS')
        _snap_ts  = row.get('Snapshot_TS')
        try:
            _entry_dt = pd.to_datetime(_entry_ts)
            _snap_dt  = pd.to_datetime(_snap_ts)
            _age_days = (_snap_dt - _entry_dt).total_seconds() / 86400
        except Exception:
            _age_days = 999   # unknown age — allow checks
        if _age_days < 2:
            # Position opened today or yesterday — classifier disagreement is expected.
            # Don't fire thesis degradation on intra-day or same-session data.
            return None

        entry_trend    = _sn(row.get('Entry_Chart_State_TrendIntegrity'))
        current_trend  = _sn(row.get('TrendIntegrity_State'))
        entry_vol      = _sn(row.get('Entry_Chart_State_VolatilityState'))
        current_vol    = _sn(row.get('VolatilityState_State'))
        entry_struct   = _sn(row.get('Entry_Chart_State_PriceStructure'))
        current_struct = _sn(row.get('PriceStructure_State'))

        # Determine if this is a long-vol or short-vol position
        strategy = str(row.get('Strategy', '') or '').upper()
        qty       = float(row.get('Quantity', 1) or 1)
        is_long_vol = (
            'LONG_CALL' in strategy or
            'LONG_PUT'  in strategy or
            ('LEAP' in strategy and qty > 0)
        )

        degradations = []

        # Trend collapse: entered in strong trend, now exhausted or absent.
        # Direction-aware for long puts: if the entry structure was already bearish
        # (STRUCTURE_BROKEN or STRUCTURAL_DOWN), STRONG_TREND referred to a downtrend.
        # A STRONG_TREND→NO_TREND transition in that context is momentum deceleration,
        # not thesis collapse — the bearish thesis persists as long as ROC/ADX/RSI remain bearish.
        if entry_trend == 'STRONG_TREND' and current_trend in ('TREND_EXHAUSTED', 'NO_TREND'):
            _entry_bearish_context = entry_struct in ('STRUCTURE_BROKEN', 'STRUCTURAL_DOWN')
            _current_bearish_signals = (
                float(row.get('roc_20', 0) or 0) < -5           # price falling
                or float(row.get('adx_14', 0) or 0) > 25        # trend still strong
                or float(row.get('rsi_14', 50) or 50) < 40      # momentum bearish
            )
            if is_long_vol and _entry_bearish_context and _current_bearish_signals:
                # Bearish downtrend decelerating — not a put thesis break, just momentum stalling.
                # Don't flag as degradation.
                pass
            else:
                degradations.append(f"trend collapsed ({entry_trend}→{current_trend})")

        # Vol regime flip — direction-aware
        if entry_vol == 'COMPRESSED' and current_vol in ('EXPANDING', 'EXTREME'):
            if is_long_vol:
                # COMPRESSED→EXTREME for long vol = thesis CONFIRMING, not degrading.
                # Natenberg Ch.11: this is exactly the payoff scenario.
                # Do NOT flag as degradation.
                pass
            else:
                # Short vol (CC, BW, CSP): sold into low vol, now vol expanded against us.
                degradations.append(f"vol regime shifted ({entry_vol}→{current_vol})")
        elif entry_vol in ('EXPANDING', 'EXTREME') and current_vol == 'COMPRESSED':
            if is_long_vol:
                # Long vol entered into high vol, now vol crushed — this IS degradation
                degradations.append(f"vol regime shifted ({entry_vol}→{current_vol})")
            # Short vol: EXPANDING→COMPRESSED is thesis confirming (sold high, IV decayed)

        # Structure broken: entered in directional structure, now structure broken
        if entry_struct in ('STRUCTURAL_UP', 'STRUCTURAL_DOWN') and current_struct == 'STRUCTURE_BROKEN':
            degradations.append(f"price structure broken ({entry_struct}→{current_struct})")

        if not degradations:
            return None
        return {"text": "; ".join(degradations)}

    @staticmethod
    def _compare_actions_long_option(row: pd.Series, dte: float, pnl_pct: float) -> Dict[str, Any]:
        """
        Action comparator for directional long options (LONG_PUT, LONG_CALL, LEAPS).

        Mirrors _compare_actions_bw() but for the buyer side:
          HOLD  — keep position, pay theta, ride directional move
          ROLL  — pay debit (or collect credit) to extend/adjust
          CLOSE — sell to close at current bid (certain exit)

        EV inputs:
          - MC_Hold_EV / MC_Hold_P50 from forward simulation (primary if available)
          - Static theta × DTE as fallback (deterministic lower bound)
          - roll_cost from MC_Wait_Credit_Delta or row roll candidate data
          - IV/HV ratio as vol confidence weight on MC estimates

        Returns:
          ev_hold, ev_roll, ev_close  (floats, $ per contract)
          ranked_actions              (list[str])
          ev_winner                   (str)
          ev_margin                   ($, gap between #1 and #2)
          vol_confidence              (float 0-1, reliability weight on MC)
          capital_impact              ($ additional capital if ROLL)
          ev_summary                  (one-line string)
          mc_used                     (bool — True if MC fields populated)
        """
        import math as _m

        qty     = max(1, int(abs(float(row.get('Quantity', 1) or 1))))
        mult    = 100 * qty

        # ── Vol confidence weight ─────────────────────────────────────────────
        # IV/HV ratio: when IV ≈ HV (ratio 0.9-1.1), MC σ estimate is calibrated.
        # When IV << HV (ratio < 0.8), EWMA under-prices realized moves → MC optimistic.
        # When IV >> HV (ratio > 1.3), market pricing fear premium → MC may over-price.
        iv_now  = float(row.get('IV_30D', row.get('IV_Now', 0)) or 0)
        hv_20   = float(row.get('HV_20D', row.get('HV_20', 0)) or 0)
        if iv_now >= 5.0: iv_now /= 100.0
        if hv_20  >= 5.0: hv_20  /= 100.0
        if hv_20 > 0 and iv_now > 0:
            _iv_hv_ratio = iv_now / hv_20
            # Confidence highest when ratio 0.85-1.15 (vol fairly priced)
            if 0.85 <= _iv_hv_ratio <= 1.15:
                vol_confidence = 0.85
            elif 0.70 <= _iv_hv_ratio < 0.85 or 1.15 < _iv_hv_ratio <= 1.30:
                vol_confidence = 0.65
            else:
                vol_confidence = 0.45   # significant mispricing — MC less reliable
        else:
            vol_confidence = 0.50       # unknown — neutral weight

        # ── EV_HOLD ──────────────────────────────────────────────────────────
        # Primary: MC_Hold_EV (forward simulation P&L per contract)
        # Fallback: static theta × DTE (certain decay cost only — no directional component)
        mc_hold_ev  = row.get('MC_Hold_EV')
        mc_hold_p50 = row.get('MC_Hold_P50')
        mc_used     = False
        theta_raw   = abs(float(row.get('Theta', 0) or 0))

        if mc_hold_ev is not None and pd.notna(mc_hold_ev):
            _mc_ev = float(mc_hold_ev)
            # Weight by vol confidence — if IV/HV badly misaligned, discount MC
            ev_hold = _mc_ev * vol_confidence + (-theta_raw * dte * mult) * (1 - vol_confidence)
            mc_used = True
        else:
            # Deterministic fallback: expected theta bleed only (no directional estimate)
            ev_hold = -theta_raw * dte * mult

        # ── EV_ROLL ───────────────────────────────────────────────────────────
        # Roll cost = capital outlay (negative = debit, positive = credit collected)
        # Source priority: MC_Wait_Credit_Delta → Roll_Candidate_1 → zero
        mc_credit_delta = row.get('MC_Wait_Credit_Delta')
        roll_cost_per_contract = 0.0

        if mc_credit_delta is not None and pd.notna(mc_credit_delta):
            # MC_Wait_Credit_Delta = median change in option value if we wait wait_days
            # Negative = option will be worth less → rolling now is cheaper
            roll_cost_per_contract = float(mc_credit_delta)
        else:
            # Try Roll_Candidate_1 cost_to_roll
            _rc1 = row.get('Roll_Candidate_1')
            if _rc1 and str(_rc1) not in ('', 'nan', 'None'):
                try:
                    import json as _jrc
                    _cd = _jrc.loads(str(_rc1)) if isinstance(_rc1, str) else _rc1
                    _ctr = _cd.get('cost_to_roll', {})
                    if isinstance(_ctr, str):
                        _ctr = _jrc.loads(_ctr)
                    _npc = float(_ctr.get('net_per_contract', 0) or 0)
                    roll_cost_per_contract = _npc   # negative = debit
                except Exception:
                    pass

        # EV_ROLL: the net P&L impact of executing the roll now
        # If we roll for a debit, that's immediate capital destruction
        # If we roll for a credit, that's immediate income
        # After rolling, assume EV is approximately zero (new position neutral — we don't model new leg)
        ev_roll = roll_cost_per_contract * qty   # total dollars across all contracts

        # Capital impact: additional capital committed by rolling (debit = positive impact)
        capital_impact = max(0.0, -roll_cost_per_contract) * qty   # $ out of pocket if debit

        # ── EV_CLOSE ─────────────────────────────────────────────────────────
        # Certain: sell at current bid
        last_price = float(row.get('Last', row.get('Mark', 0)) or 0)
        bid_price  = float(row.get('Bid', last_price * 0.98) or last_price * 0.98)
        # Current P&L on close: (bid - entry_premium) per share × mult
        # Premium_Entry is the authoritative entry cost (frozen at inception).
        # Fallback to last_price when missing — makes ev_close ≈ 0 (neutral proxy).
        _raw_entry_prem = row.get('Premium_Entry')
        _entry_prem_missing = pd.isna(_raw_entry_prem)
        entry_prem = float(_raw_entry_prem or last_price) if not _entry_prem_missing else last_price
        ev_close   = (bid_price - entry_prem) * mult

        # ── Rank ─────────────────────────────────────────────────────────────
        ev_map = {"HOLD": ev_hold, "ROLL": ev_roll, "CLOSE": ev_close}
        ranked = sorted(ev_map.items(), key=lambda x: x[1], reverse=True)
        ranked_actions = [a for a, _ in ranked]
        ev_winner      = ranked_actions[0]
        ev_margin      = ranked[0][1] - ranked[1][1]

        def _fmt(v: float) -> str:
            return f"+${v:,.0f}" if v >= 0 else f"-${abs(v):,.0f}"

        _NOISE = 75.0   # below this margin, treat as statistical tie
        if ev_margin < _NOISE:
            ev_summary = (
                f"Statistical tie: {ev_winner}≈{ranked_actions[1]} "
                f"(margin ${ev_margin:.0f} < ${_NOISE:.0f} noise floor). "
                f"Vol confidence: {vol_confidence:.0%}. "
                f"{'MC-weighted.' if mc_used else 'Static theta fallback — no MC.'}"
            )
        else:
            ev_summary = (
                f"{ev_winner} dominates: {_fmt(ranked[0][1])} vs "
                f"{ranked_actions[1]}: {_fmt(ranked[1][1])} "
                f"(margin {_fmt(ev_margin)}). "
                f"Vol confidence: {vol_confidence:.0%}. "
                f"{'MC-weighted.' if mc_used else 'Static theta fallback.'}"
            )

        if _entry_prem_missing:
            ev_summary += (
                " [WARNING: Premium_Entry missing — ev_close uses Last as proxy, "
                "making CLOSE appear ~neutral. Treat CLOSE EV with low confidence.]"
            )

        return {
            "ev_hold":          ev_hold,
            "ev_roll":          ev_roll,
            "ev_close":         ev_close,
            "ranked_actions":   ranked_actions,
            "ev_winner":        ev_winner,
            "ev_margin":        ev_margin,
            "vol_confidence":   vol_confidence,
            "capital_impact":   capital_impact,
            "ev_summary":       ev_summary,
            "mc_used":          mc_used,
        }

    @staticmethod
    def _long_option_doctrine(row: pd.Series, result: Dict) -> Dict:
        price_state = str(row.get('PriceStructure_State', 'UNKNOWN') or 'UNKNOWN').upper()
        price_drift = float(row.get('Price_Drift_Pct', 0) if pd.notna(row.get('Price_Drift_Pct')) else 0)
        delta_entry = float(row.get('Delta_Entry', 0) if pd.notna(row.get('Delta_Entry')) else 0)
        delta_now = abs(float(row.get('Delta', 0) if pd.notna(row.get('Delta')) else 0))
        dte = row.get('DTE', 999) or 999
        strategy = str(row.get('Strategy', '') or '').upper()
        is_leap = 'LEAP' in strategy or dte >= 180
        ticker_net_delta = float(row.get('_Ticker_Net_Delta', 0) or 0)
        ticker_has_stock = bool(row.get('_Ticker_Has_Stock', False))

        # Pyramid tier + winner lifecycle defaults (always present in result)
        result["Pyramid_Tier"] = int(row.get('Pyramid_Tier', 0) or 0)
        result["Winner_Lifecycle"] = str(row.get('Winner_Lifecycle', 'THESIS_UNPROVEN') or 'THESIS_UNPROVEN')

        # ── Calendar gates (evaluated first — expiration overrides all other holds) ────
        # Natenberg Ch.15: at ≤3 DTE, gamma → ∞ near strike. McMillan Ch.7: pin risk.
        # Hull Ch.18: theta is non-linear in the final week.
        try:
            from scan_engine.calendar_context import expiry_proximity_flag, get_calendar_context
            _ul_last_cal  = float(row.get('UL Last', 0) or 0)
            _strike_cal   = float(row.get('Strike', 0) or 0)
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
        except Exception:
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
        _ul_now_sc     = float(row.get('UL Last', 0) or 0)
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
        _pnl_quick = DoctrineAuthority._safe_pnl_pct(row) or 0.0
        _delta_redundancy_threshold = -0.35 if is_leap else -0.15
        _qty_for_redundancy = abs(float(row.get('Quantity', 1) or 1))
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

        # 1. Thesis invalidation: structure broken + price moved against position
        # McMillan Ch.4: cut long options when directional thesis is structurally broken
        if "STRUCTURE_BROKEN" in price_state and price_drift < -0.05:
            result.update({
                "Action": "EXIT",
                "Urgency": "CRITICAL",
                "Rationale": f"Technical structure broken ({price_state}) with {price_drift:.1%} adverse drift. Directional thesis invalidated (McMillan Ch.4).",
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
        pnl_pct = DoctrineAuthority._safe_pnl_pct(row) or 0.0

        # Compute option-level gain % from price: (Last - |Premium_Entry|) / |Premium_Entry|
        # This is the correct denominator for profit targets on long options.
        # Total_GL_Decimal uses cost-basis which includes broker charges and may be diluted
        # by multi-leg trades — option price gain is the canonical measure (McMillan Ch.4).
        _last_raw    = float(row.get('Last', 0) or 0)
        _bid_raw     = float(row.get('Bid', 0) or 0)
        # Use max(Last, Bid) as the realizable price floor.  For deeply ITM options,
        # Last can be a stale EOD trade price BELOW intrinsic (arbitrage floor), which
        # makes _time_val = 0 and triggers a false TV-exhausted EXIT.  The bid is the
        # actual price the market will pay right now — it cannot be below intrinsic
        # (market makers enforce this).  Using max(Last, Bid) prevents stale-Last
        # from creating phantom 0% TV readings.
        _last_price  = max(_last_raw, _bid_raw) if _bid_raw > 0 else _last_raw
        _entry_price = abs(float(row.get('Premium_Entry', 0) or 0))
        option_gain_pct = (_last_price - _entry_price) / _entry_price if _entry_price > 0 else 0.0

        # Pyramid tier and winner lifecycle (Cycle 2.96 — Murphy pyramid rules)
        _pyramid_tier = int(row.get('Pyramid_Tier', 0) or 0)
        _winner_lifecycle = str(row.get('Winner_Lifecycle', 'THESIS_UNPROVEN') or 'THESIS_UNPROVEN').upper()

        # Deep-ITM intrinsic metrics — used for winner management gates below
        _ul_last  = float(row.get('UL Last', row.get('Underlying_Price_Entry', 0)) or 0)
        _strike   = float(row.get('Strike', 0) or 0)
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
        _roc5  = float(row.get('roc_5',  0) or 0)
        _roc10 = float(row.get('roc_10', 0) or 0)

        _adverse_drift_dir = 'UP' if _is_put else 'DOWN'
        # Magnitude thresholds: filter noise, only trigger on meaningful adverse moves.
        # ROC5 > 1.5%: exceeds typical daily σ (1-1.5% for most stocks)
        # Price_Drift > 2%: accumulated adverse drift exceeds noise floor
        # AUDIT FIX: use >= so boundary values fire consistently with thesis_engine.
        _roc5_adverse = (_roc5 >= 1.5 if _is_put else _roc5 <= -1.5)
        _drift_is_adverse = (price_drift >= 0.02 if _is_put else price_drift <= -0.02)
        _theta_bleed = float(row.get('Theta_Bleed_Daily_Pct', 0) or 0)

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
        _c4_pnl = DoctrineAuthority._safe_pnl_pct(row) or 0.0
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
        days_held = float(row.get('Days_In_Trade', 0) or 0)
        original_dte = dte + days_held  # approximate original DTE at entry
        _time_fraction = 2 if _entry_quality == 'STRONG' else (4 if _entry_quality == 'WEAK' else 3)
        if (original_dte > 0
                and days_held >= original_dte / _time_fraction
                and abs(price_drift) < 0.03
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

        _recovery_raw_da = str(row.get('Recovery_Feasibility', '') or '').upper()
        _already_impossible = _recovery_raw_da == 'IMPOSSIBLE'

        if _dir_adverse_detected and not _already_impossible and not is_leap:
            _thesis_state_da = str(row.get('Thesis_State', '') or '').upper()
            _conv_status_da  = str(row.get('Conviction_Status', '') or '').upper()

            # Catalyst check: earnings within 14 days as a potential reversal trigger
            _has_catalyst = False
            try:
                _earn_date = row.get('Earnings Date') or row.get('Earnings_Date')
                _snap_ts = row.get('Snapshot_TS')
                if _earn_date and _snap_ts:
                    _earn_dt = pd.to_datetime(_earn_date)
                    _snap_dt = pd.to_datetime(_snap_ts)
                    _days_to_earn = (_earn_dt.normalize() - _snap_dt.normalize()).days
                    _has_catalyst = 0 < _days_to_earn <= 14
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
                        f"earnings catalyst within 14d. "
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
                _srs_z   = float(row.get('Sector_RS_ZScore', 0) or 0) if pd.notna(row.get('Sector_RS_ZScore')) else 0.0
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
                        _iv_pctile_da = float(row.get('IV_Percentile', 50) or 50) if pd.notna(row.get('IV_Percentile')) else 50.0
                        _iv_depth_da = int(row.get('IV_Percentile_Depth', 0) or 0)
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
                _mom_slope_da = float(row.get('momentum_slope', 0) or 0)
                _confirming_signal = (_mom_slope_da < 0 if _is_put else _mom_slope_da > 0)
                _ev_ratio_da = float(row.get('EV_Feasibility_Ratio', 0) or 0) if pd.notna(row.get('EV_Feasibility_Ratio')) else float('nan')
                _iv_pctile_da = float(row.get('IV_Percentile', 50) or 50) if pd.notna(row.get('IV_Percentile')) else 50.0
                _iv_depth_da = int(row.get('IV_Percentile_Depth', 0) or 0)
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
                    _original_dte = float(row.get('DTE_Entry', 0) or 0)
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
                            f"Drift={drift_dir}, Price_Drift={price_drift:+.1%}), P&L={pnl_pct:.0%}. "
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
            _tc_net_theta    = float(row.get('_Ticker_Net_Theta', 0) or 0)
            _tc_net_vega     = float(row.get('_Ticker_Net_Vega', 0) or 0)
            _tc_structure    = str(row.get('_Ticker_Structure_Class', '') or '')
            _tc_strategy_mix = str(row.get('_Ticker_Strategy_Mix', '') or '')
            _tc_ticker       = str(row.get('Underlying_Ticker', '') or '')
            _leg_theta       = float(row.get('Theta', 0) or 0)

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
        _tti_bb_z = float(row.get('bb_width_z', 0) or 0)
        _tti_compressing = (
            _tti_bb_z < -0.5
            or _tti_comp_raw in ('EARLY_COMPRESSION', 'MID_COMPRESSION', 'MATURE_COMPRESSION')
        )

        _tti_roc5  = float(row.get('roc_5',  0) or 0)
        _tti_roc10 = float(row.get('roc_10', 0) or 0)
        _tti_no_momentum = (_tti_roc5 <= 0 and _tti_roc10 <= 0)

        _tti_chop  = float(row.get('choppiness_index', 50) or 50)
        _tti_adx   = float(row.get('adx_14', 25) or 25)
        _tti_range_bound = (_tti_chop > 55 or _tti_adx < 18)

        _tti_mom_vel_raw = str(row.get('MomentumVelocity_State', '') or '').split('.')[-1].upper()
        _tti_bottoming = (_tti_mom_vel_raw == 'REVERSING'
                          and float(row.get('rsi_14', 50) or 50) < 42)

        # Compute option gain inline — option_gain_pct is not yet defined at this gate.
        # Use max(Last, Bid) for same stale-Last protection as the main TV calculation.
        _tti_last_raw  = float(row.get('Last', 0) or 0)
        _tti_bid_raw   = float(row.get('Bid', 0) or 0)
        _tti_last_p    = max(_tti_last_raw, _tti_bid_raw) if _tti_bid_raw > 0 else _tti_last_raw
        _tti_entry_p = abs(float(row.get('Premium_Entry', 0) or 0))
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
            _tti_theta_per_day = abs(float(row.get('Theta', 0) or 0))
            if _tti_theta_per_day > 0 and _tti_last_p > 0:
                _tti_20pct_budget = (_tti_last_p * 0.20) / _tti_theta_per_day
                _tti_budget_str = f"{int(_tti_20pct_budget)}d"
            else:
                _tti_budget_str = f"{max(5, int(_tti_dte // 4))}d"  # fallback: quarter of remaining DTE

            _tti_urgency = "HIGH" if _tti_dte <= 30 else "MEDIUM"

            # Ticker-context branch: short-dated leg inside a multi-leg structure
            _tti_trade_count  = int(row.get('_Ticker_Trade_Count', 1) or 1)
            _tti_net_theta    = float(row.get('_Ticker_Net_Theta', 0) or 0)
            _tti_structure    = str(row.get('_Ticker_Structure_Class', '') or '')
            _tti_strategy_mix = str(row.get('_Ticker_Strategy_Mix', '') or '')
            _tti_ticker       = str(row.get('Underlying_Ticker', '') or '')
            _tti_leg_theta_d  = abs(float(row.get('Theta', 0) or 0)) * 100

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

        _ev_ratio      = float(row.get('EV_Feasibility_Ratio', 0) or 0) if pd.notna(row.get('EV_Feasibility_Ratio')) else float('nan')
        _ev_50_ratio   = float(row.get('EV_50pct_Feasibility_Ratio', 0) or 0) if pd.notna(row.get('EV_50pct_Feasibility_Ratio')) else float('nan')
        _theta_bleed   = float(row.get('Theta_Bleed_Daily_Pct', 0) or 0)
        _req_move_be   = float(row.get('Required_Move_Breakeven', 0) or 0)
        _req_move_50   = float(row.get('Required_Move_50pct', 0) or 0)
        _em_10         = float(row.get('Expected_Move_10D', 0) or 0)
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

        hv = float(row.get('HV_20D', 0) or 0)
        iv_live = row.get('IV_Now')
        iv = float(iv_live) if iv_live is not None and not pd.isna(iv_live) and float(iv_live or 0) > 0 \
             else float(row.get('IV_30D', 0) or 0)
        iv_source = "live" if (iv_live is not None and not pd.isna(iv_live) and float(iv_live or 0) > 0) else "daily"

        # Only evaluate thesis health when the position is losing meaningfully.
        # Positions in (-15%, 0%) fall through intentionally: a mildly losing position
        # without deep structural breakdown doesn't warrant thesis-level intervention.
        # Those are still covered by Gate 1 (drift), Gate 2.5 (thesis satisfaction),
        # and calendar gates (DTE/pin risk) evaluated above.
        if pnl_pct < -0.15:
            roc5         = float(row.get('roc_5',  0) or 0)
            roc10        = float(row.get('roc_10', 0) or 0)
            roc20        = float(row.get('roc_20', 0) or 0)
            mom_slope    = float(row.get('momentum_slope', 0) or 0)
            adx          = float(row.get('adx_14', 0) or 0)
            rsi          = float(row.get('rsi_14', 50) or 50)
            bb_width_z   = float(row.get('bb_width_z', 0) or 0)
            choppiness   = float(row.get('choppiness_index', 50) or 50)
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
        _price_target  = float(row.get('Price_Target_Entry', 0) or 0)
        _measured_move = float(row.get('Measured_Move', 0) or 0)
        _resistance_1  = float(row.get('Resistance_Level_1', 0) or 0)
        # SMA-based structural levels as last-resort fallbacks.
        # For LONG_PUT: SMA20/SMA50 represent support levels; breaking below = bearish thesis hit.
        # For LONG_CALL: SMA20/SMA50 represent intermediate targets.
        # Only use as fallback — they are dynamic levels, not frozen thesis targets.
        _sma20 = float(row.get('SMA20', 0) or 0)
        _sma50 = float(row.get('SMA50', 0) or 0)

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
                _qty_int = abs(float(row.get('Quantity', 1) or 1))
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
            _qty_mfe = abs(float(row.get('Quantity', 1) or 1))
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
        _theta_day_dollar = abs(float(row.get('Theta', 0) or 0)) * 100 * abs(float(row.get('Quantity', 1) or 1))

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
        _qty_for_trim = abs(float(row.get('Quantity', 1) or 1))
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
        _te_theta_per_share = abs(float(row.get('Theta', 0) or 0))
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
        thesis = DoctrineAuthority._check_thesis_degradation(row)
        if thesis:
            # Urgency calibration:
            # - Deep ITM positions (intrinsic > 50% of price): vol compression has limited
            #   impact on intrinsic value. Downgrade urgency to LOW — the position is
            #   protected by intrinsic; the regime shift is an environment note, not a crisis.
            #   Natenberg Ch.11: "Vol compression damages OTM options most; deep ITM options
            #   retain intrinsic regardless of vol regime."
            # - Fresh positions (Days_In_Trade < 5): regime readings on <5-day positions are
            #   noise — the regime may not have stabilized yet. Downgrade to LOW.
            _g6_intrinsic = float(row.get('Intrinsic_Val', 0) or 0)
            _g6_last      = float(row.get('Last', row.get('Premium_Entry', 0)) or 0)
            _g6_days_held = float(row.get('Days_In_Trade', 99) or 99)
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
                _arb = DoctrineAuthority._compare_actions_long_option(row, dte, pnl_pct)
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
                _pos_pnl_dollars = float(row.get('PnL_Total', row.get('Total_GL_Dollar', 0)) or 0)
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
            hv_daily  = float(row.get('HV_Daily_Move_1Sigma', 0) or 0)
            req_daily = float(row.get('Recovery_Move_Per_Day', 0) or 0)
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

        # Default HOLD — enrich rationale with carry cost and scale context
        theta_day = abs(float(row.get('Theta', 0) or 0))
        last_price = float(row.get('Last', 0) or 0)
        qty = abs(float(row.get('Quantity', 1) or 1))

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
        _rsi_for_scale  = float(row.get('rsi_14', 50) or 50)
        _gamma_now      = abs(float(row.get('Gamma', 0) or 0))
        _gamma_roc_3d   = float(row.get('Gamma_ROC_3D', 0) or 0)
        _price_state_scale = price_state.split('.')[-1].upper()
        _is_extended = 'EXTENDED' in _price_state_scale or 'BREAKOUT' in _price_state_scale
        _scale_up_ready = (
            option_gain_pct >= 0.25               # already a winner
            and mom_state == 'TRENDING'            # sustained — NOT parabolic ACCELERATING
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
            and mom_state in ('ACCELERATING', 'LATE_CYCLE')
            and qty >= 1
        )

        # Pullback target: the specific price level to wait for before adding.
        # McMillan Ch.4: "Add on a retest of a prior support/resistance level — not in mid-air."
        # Priority: EMA9 (fastest dynamic level) → SMA20 (medium-term) → Bollinger Band (2σ)
        # For puts (bearish): price rallying back up to EMA/resistance = pullback = good add entry.
        # For calls (bullish): price dipping back to EMA/support = pullback = good add entry.
        _ema9       = float(row.get('EMA9', 0) or row.get('ema9', 0) or 0)
        _sma20_sc   = float(row.get('SMA20', 0) or 0)
        _lower_band = float(row.get('LowerBand_20', 0) or 0)
        _upper_band = float(row.get('UpperBand_20', 0) or 0)
        _ul_for_scale = float(row.get('UL Last', 0) or 0)

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
            _atr_for_sz  = float(row.get('atr_14', 0) or 0)
            _ul_sz       = float(row.get('UL Last', 1) or 1)
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
            _delta_util_sc   = float(row.get('Portfolio_Delta_Utilization_Pct', 0) or 0)
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
            iv_slope_30_90 = float(row.get('iv_ts_slope_30_90', 0) or 0)
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
            iv_slope_30_90 = float(row.get('iv_ts_slope_30_90', 0) or 0)
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
                f"{carry_note}{winner_note}{iv_slope_note}{scale_note}{_cal_bleed_note} Thesis active (McMillan Ch.4)."
            ),
            "Doctrine_Source": "McMillan Ch.4: Neutral Maintenance",
            "Decision_State": STATE_NEUTRAL_CONFIDENT,
            "Required_Conditions_Met": True
        })
        return result

    @staticmethod
    def _short_put_doctrine(row: pd.Series, result: Dict) -> Dict:
        # ── Weighting Wheel Assessment (runs first — persists to result regardless of gate path) ─
        # Passarelli Ch.1: intentional assignment via CSP → CC cycle.
        # Wheel columns are injected into result here so they're present even when an
        # early-returning gate (PIN_RISK, GAMMA_CRITICAL, PRE_HOLIDAY_EXPIRY) fires.
        # Before any assignment risk escalation, evaluate whether assignment is
        # INTENTIONAL — a feature of the income wheel cycle, not a failure.
        #
        # The "wheel" = CSP → assignment → covered call → repeat.
        # Passarelli Ch.1: "The short put allows the trader to establish a target
        #   price at which they would buy the stock — effective purchase price is
        #   strike minus premium collected."
        # McMillan: "When assignment is at a favorable basis and IV supports a CC,
        #   rolling the put is suboptimal — accepting stock and selling calls extracts
        #   more total income than perpetually rolling."
        #
        # Four conditions must ALL be true for Wheel Ready:
        #   1. Net Basis attractive: effective_basis < current_spot × 0.97
        #      (getting stock at ≥3% discount vs current market)
        #   2. Chart supportive: TrendIntegrity not BROKEN + PriceStructure not BROKEN
        #      (not catching a falling knife — structure still exists)
        #   3. IV high enough to sell calls: IV_Now >= 0.25 (25% annualized)
        #      (Natenberg: selling calls at <25% IV = thin premium, poor risk/reward)
        #   4. Capital concentration acceptable: Portfolio_Delta_Utilization_Pct < 15%
        #      (McMillan Ch.3: never let one ticker consume >15% of portfolio delta capacity)
        #
        # If all four pass → Wheel_Ready=True: assignment becomes a strategic path,
        #   not a failure. Urgency on roll gates is REDUCED. Rationale explains the wheel.
        # If any fail → Wheel_Ready=False: standard assignment defense logic runs.
        # ─────────────────────────────────────────────────────────────────────────────
        _spot_for_wheel     = float(row.get('UL Last', 0) or 0)
        _strike_for_wheel   = float(row.get('Strike', 0) or 0)
        _premium_entry_w    = abs(float(row.get('Premium_Entry', 0) or 0))
        _net_cost_basis_w   = float(row.get('Net_Cost_Basis_Per_Share', 0) or 0)
        _broker_basis_w     = abs(float(row.get('Basis', 0) or 0))
        _qty_w              = abs(float(row.get('Quantity', 1) or 1))
        # For wheel IV check, we need the UNDERLYING's vol — not the option's own IV.
        # When DTE=0 (expired option), IV_Now and IV_30D are garbage data from the
        # expired option chain (can be 300%+ for deep ITM/ATM at expiry).
        # Use HV_20D as the proxy for what the underlying's vol environment looks like
        # for future covered call premium — this is what matters for the wheel decision.
        # Natenberg Ch.8: "Historical volatility is the best predictor of near-term
        #   realized vol when IV is distorted by proximity to expiry."
        _dte_for_wheel      = float(row.get('DTE', 999) or 999)
        _iv_now_w_raw       = float(row.get('IV_Now', 0) or row.get('IV_30D', 0) or 0)
        _hv_20d_w           = float(row.get('HV_20D', 0) or 0)
        if _dte_for_wheel <= 2 and _hv_20d_w > 0:
            # Expiring/expired option (DTE ≤ 2): IV_Now is garbage (option pinning,
            # extreme skew near expiry). Substitute HV_20D as IV proxy for CC viability.
            # This includes same-day expiry (DTE=0) and weekend-artifact DTE=1-2.
            _iv_now_w       = _hv_20d_w
            _iv_source_w    = "HV_20D (near-expiry — option IV unreliable)"
        else:
            _iv_now_w       = _iv_now_w_raw
            _iv_source_w    = "IV_Now/IV_30D"
        _delta_util_w       = float(row.get('Portfolio_Delta_Utilization_Pct', 0) or 0)
        _trend_w            = str(row.get('TrendIntegrity_State', '') or '').split('.')[-1].upper()
        _price_struct_w     = str(row.get('PriceStructure_State', '') or '').split('.')[-1].upper()
        _mc_assign_p_w      = float(row.get('MC_Assign_P_Expiry', 0) or 0) if pd.notna(row.get('MC_Assign_P_Expiry')) else 0.0

        # Effective basis per share = strike - premium collected per share.
        # Passarelli Ch.1: "Effective purchase price = strike minus premium collected."
        #
        # Priority:
        #   1. Net_Cost_Basis_Per_Share (enriched column, already correct per-share)
        #   2. Premium_Entry: strike - abs(premium_entry) — most reliable direct calculation
        #   3. Broker Basis fallback: basis / qty / 100 — broker stores TOTAL cost for all
        #      contracts; must divide by qty (contracts) AND 100 (shares/contract) to get
        #      per-share. Without /100 this overstates basis by 100×.
        _broker_cost_per_share_w = (
            (_broker_basis_w / _qty_w / 100.0)
            if (_qty_w > 0 and _broker_basis_w > 0)
            else 0.0
        )
        if _net_cost_basis_w > 0:
            _effective_basis_w = _net_cost_basis_w
        elif _strike_for_wheel > 0 and _premium_entry_w > 0:
            _effective_basis_w = _strike_for_wheel - _premium_entry_w   # Passarelli: effective purchase price
        elif _broker_cost_per_share_w > 0:
            _effective_basis_w = _broker_cost_per_share_w
        else:
            _effective_basis_w = 0.0

        # Condition 1: Net basis is at a ≥3% discount to current spot
        _wheel_basis_ok = (
            _effective_basis_w > 0
            and _spot_for_wheel > 0
            and _effective_basis_w <= _spot_for_wheel * 0.97
        )

        # Condition 2: Chart structure is not broken (not catching a falling knife)
        _wheel_chart_ok = (
            'BROKEN' not in _trend_w
            and 'BROKEN' not in _price_struct_w
        )

        # Condition 3: IV high enough to sell covered calls after assignment
        # Natenberg Ch.12: selling calls below 25% IV = poor premium-to-risk ratio
        # IV fields in this pipeline are stored as decimals (0.35 = 35%, 1.20 = 120%).
        # No normalization needed. Only divide if the value looks like it was stored as
        # a plain percentage integer (> 10.0 is a safe threshold since 1000% IV is
        # essentially impossible in practice).
        _iv_now_w_norm = _iv_now_w / 100.0 if _iv_now_w > 10.0 else _iv_now_w
        _wheel_iv_ok = _iv_now_w_norm >= 0.25

        # Condition 4: Capital concentration acceptable
        # McMillan Ch.3: single-ticker delta utilization > 15% = overconcentration
        _wheel_capital_ok = _delta_util_w < 15.0

        _wheel_ready = _wheel_basis_ok and _wheel_chart_ok and _wheel_iv_ok and _wheel_capital_ok

        # Build wheel assessment note regardless (surfaces failing conditions even if not ready)
        _wheel_fails = []
        if not _wheel_basis_ok:
            _basis_note = f"${_effective_basis_w:.2f}" if _effective_basis_w > 0 else "unknown"
            _wheel_fails.append(f"basis {_basis_note} not at ≥3% discount (spot=${_spot_for_wheel:.2f})")
        if not _wheel_chart_ok:
            _wheel_fails.append(f"structure broken (Trend={_trend_w}, Price={_price_struct_w})")
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

        # Inject wheel assessment into result for display layer and downstream reconciliation
        result['Wheel_Ready']  = _wheel_ready
        result['Wheel_Note']   = _wheel_note
        result['Wheel_Basis']  = round(_effective_basis_w, 2) if _effective_basis_w > 0 else None
        result['Wheel_IV_Ok']  = _wheel_iv_ok
        result['Wheel_Chart_Ok'] = _wheel_chart_ok
        result['Wheel_Capital_Ok'] = _wheel_capital_ok

        # C1 audit fix: MC_Assign_P_Expiry gate — wire the MC assignment probability into urgency.
        # `_mc_assign_p_w` was computed above (line 4020) but never acted upon — audit confirmed.
        # When the forward simulation says P(assignment by expiry) > 0.75, the short put has a
        # 75%+ chance of being assigned. If Wheel_Ready=False (assignment is NOT the intended path),
        # this is a critical warning requiring urgency escalation to HIGH.
        # If Wheel_Ready=True, high P(assign) is a FEATURE — no escalation needed (expected outcome).
        #
        # McMillan Ch.7: "When a short put has >75% probability of assignment and the wheel is
        # not the intended outcome, the correct action is to roll before the probability locks in."
        # Natenberg Ch.19: "Model-confirmed assignment risk at 75%+ is a quantitative directive
        # to act — not a suggestion."
        #
        # One-way: this sets urgency HIGH if not already CRITICAL. Never demotes.
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

        # ── GAP 2 FIX: Earnings Lockdown Guard ───────────────────────────────────
        # Jabbour (Option Trader Handbook Ch.8):
        # "Never roll into an earnings event — binary risk invalidates the spread thesis.
        # Rolling a short put WITH earnings < 3 days extends the position into a binary event
        # where a gap move cannot be delta-hedged. The roll credit does not compensate for
        # the potential assignment gap risk."
        # Given (No-Hype Options Trading Ch.4): "Earnings risk is different from market risk —
        # no amount of spread management eliminates it. Close before the event."
        #
        # Action: when earnings < 3 days, block ROLL and escalate to EXIT evaluation.
        # Exception: if position is OTM AND wheel-ready (assignment intended = acceptable).
        _earn_date_sp = row.get('Earnings Date') or row.get('Earnings_Date')
        _days_to_earn_sp = None
        if _earn_date_sp not in (None, '', 'nan', 'N/A') and not (
            isinstance(_earn_date_sp, float) and pd.isna(_earn_date_sp)
        ):
            try:
                _ed_sp = pd.to_datetime(str(_earn_date_sp), errors='coerce')
                if pd.notna(_ed_sp):
                    _snap_sp = pd.to_datetime(row.get('Snapshot_TS') or pd.Timestamp.now())
                    _days_to_earn_sp = (_ed_sp.normalize() - _snap_sp.normalize()).days
            except Exception:
                pass
        if _days_to_earn_sp is not None and 0 <= _days_to_earn_sp <= 2:
            # Earnings in ≤2 days → lockdown: no rolls, evaluate exit
            _earn_itm_sp = row.get('Moneyness_Label') == 'ITM'
            _earn_wheel_ok = _wheel_ready and not _earn_itm_sp  # wheel + OTM = intentional path
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

        # ── Calendar gates (apply after wheel assessment so wheel columns persist) ─
        # McMillan Ch.7: short premium near strike at ≤3 DTE = pin risk.
        # Hull Ch.18: theta non-linear in final week.
        # NOTE: wheel override may soften calendar gate urgency (wheel_ready + PRE_HOLIDAY
        # could mean: accept assignment rather than rolling into holiday thin markets).
        try:
            from scan_engine.calendar_context import expiry_proximity_flag
            _dte_sp_cal   = float(row.get('DTE', 999) or 999)
            _ul_sp_cal    = float(row.get('UL Last', 0) or 0)
            _strike_sp_cal = float(row.get('Strike', 0) or 0)
            _strat_sp_cal  = str(row.get('Strategy', '') or '').upper()
            _exp_flag_sp, _exp_note_sp = expiry_proximity_flag(
                dte=_dte_sp_cal, strategy=_strat_sp_cal,
                ul_last=_ul_sp_cal, strike=_strike_sp_cal,
            )
            if _exp_flag_sp == 'PIN_RISK':
                result.update({
                    "Action": "EXIT",
                    "Urgency": "CRITICAL",
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
                    # Wheel override: pre-holiday with wheel ready = accept assignment
                    # rather than rolling into holiday thin markets (poor liquidity for CC)
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
        except Exception:
            pass

        # 1. Tactical Maintenance: Expiration/Assignment Risk
        # RAG: Strategic Assignment. If assignment is acceptable, we are less aggressive with rolling.
        is_late = row.get('Lifecycle_Phase') == 'Late'
        is_itm = row.get('Moneyness_Label') == 'ITM'
        assignment_acceptable = row.get('Assignment_Acceptable', True)

        # GAP 3 FIX: Assignment Risk Auto-Close Terminal Case.
        # Jabbour (Option Trader Handbook Ch.8):
        # "When a short put is deep ITM (delta > 0.90), the roll credit is structurally
        # insufficient to offset the intrinsic loss. Every $1 the stock drops adds ~$1 to the
        # put's value — rolling only delays the loss and adds transaction costs. At this point,
        # the rational outcome is to stop rolling, accept assignment at the effective basis,
        # or close for a defined loss. The roll is no longer profitable."
        # Natenberg Ch.15: "When time value is near-zero, the roll adds no EV — you are
        # buying back near-intrinsic and selling near-intrinsic at a different strike."
        #
        # Conditions: delta > 0.90 AND DTE ≤ 7 AND NOT wheel-ready (wheel manages intentional assignment).
        _sp_dte = float(row.get('DTE', 999) or 999)
        _sp_delta_abs = abs(float(row.get('Delta', 0) or 0))
        if _sp_dte <= 7 and _sp_delta_abs > 0.90 and not _wheel_ready:
            _sp_spot_atc = float(row.get('UL Last', 0) or 0)
            _sp_strike_atc = float(row.get('Strike', 0) or 0)
            _sp_intrinsic = max(0.0, _sp_strike_atc - _sp_spot_atc) if _sp_strike_atc > 0 and _sp_spot_atc > 0 else 0.0
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

        # 1a. DTE<7 hard gate — independent of Lifecycle_Phase field.
        # Simulation found: Lifecycle_Phase=='Late' may not fire when DTE=5 if the field
        # was computed on entry (e.g., entry at DTE=30, lifecycle='Mid' never updated).
        # McMillan Ch.7: "Within 7 days, the put behaves discontinuously near the strike.
        # Don't rely on lifecycle labels — use raw DTE as the hard guard."
        #
        # Wheel override: if Wheel_Ready, even at DTE<7 + ITM, the action is
        # HOLD_ACCEPT (prepare to receive stock) rather than a panic roll.
        # The only hard override is capital concentration failure — you can't
        # accept stock you can't afford to carry.
        if _sp_dte < 7 and is_itm:
            if _wheel_ready:
                # Wheel path: assignment is intentional — prepare to accept stock
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
                # Wheel path: late + ITM + wheel conditions met = accept assignment gracefully
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

        # 1b. GAP 1 FIX: Hard 21-DTE exit gate for income strategies (short put).
        # Given (No-Hype Options Trading Ch.6) + Passarelli Ch.2:
        # "At 21 DTE, the gamma-theta ratio has degraded — the premium seller's edge is gone.
        # If 50% profit has NOT been captured by 21 DTE, the remaining time value is theta-
        # negative: gamma acceleration now works against the short position. The correct
        # action is EXIT or force-roll, not hold and hope."
        # Exceptions:
        #   - Wheel_Ready + OTM: intentional assignment path, don't exit
        #   - Already handled DTE<7 above (more urgent gate)
        #   - 50% already captured → earlier gate (roll) already fired
        _sp_50_pct_gate_dte = float(row.get('DTE', 999) or 999)
        _sp_premium_entry_21 = abs(float(row.get('Premium_Entry', 0) or row.get('Short_Call_Premium', 0) or 0))
        _sp_last_21 = abs(float(row.get('Last', 0) or 0))
        _sp_profit_captured = (
            (_sp_premium_entry_21 - _sp_last_21) / _sp_premium_entry_21
            if _sp_premium_entry_21 > 0 else 0.0
        )
        if (
            _sp_50_pct_gate_dte <= 21
            and _sp_50_pct_gate_dte >= 7   # DTE<7 handled by earlier hard gate
            and _sp_profit_captured < 0.50  # 50% NOT captured
            and not (_wheel_ready and not is_itm)  # not intentional wheel assignment approaching
        ):
            _sp_21_urgency = 'MEDIUM' if _sp_profit_captured >= 0 else 'HIGH'

            # ── Strategy-aware IV regime check (Chan 0.786) ─────────────────
            # Same logic as BW 21-DTE gate: when IV has contracted >30% from
            # entry, is in bottom quartile, and selling edge (IV-HV) is gone,
            # rolling into thin premium has negative EV. Let current put expire.
            _sp_iv_entry_21 = float(row.get('IV_Entry', 0) or 0)
            _sp_iv_now_21   = float(row.get('IV_30D', 0) or row.get('IV_Now', 0) or 0)
            _sp_iv_pctile_21 = float(row.get('IV_Percentile', 50) or 50)
            _sp_iv_gap_21   = float(row.get('IV_vs_HV_Gap', 0) or 0)

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
            # Position Trajectory context for SHORT_PUT 21-DTE gate
            _sp_regime_21 = str(row.get('Position_Regime', 'NEUTRAL') or 'NEUTRAL')
            _sp_consec_debits_21 = int(float(row.get('Trajectory_Consecutive_Debit_Rolls', 0) or 0))
            _sp_stock_ret_21 = float(row.get('Trajectory_Stock_Return', 0) or 0)
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
        # Upward drift is success, not risk.
        if row.get('Drift_Direction') == 'Up':
            # Primitives for Triple-Gate TRIM
            # Reversion probability: Collapses if structure is broken or momentum is exhausted
            struct_state = str(row.get('PriceStructure_State', 'STABLE')).upper()
            mom_state = str(row.get('MomentumVelocity_State', 'UNKNOWN')).upper()
            vol_state = str(row.get('VolatilityState_State', 'NORMAL')).upper()
            
            reversion_prob_collapse = ("STRUCTURE_BROKEN" in struct_state or "REVERSING" in mom_state)
            low_continuation_value = row.get('Drift_Magnitude') == 'High' # Proxy for premium capture
            vol_expansion = vol_state in ["EXPANDING", "EXTREME"]
            
            # TRIPLE GATE: Only trim if reversion edge is gone AND premium is low AND vol is expanding
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
            
            # Resolve to HOLD_FOR_REVERSION if success is high but structure is intact
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
        # RAG: UI Honesty. Correct rationale if ITM but not late.
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

        # 3a. Thesis regime degradation (short put — entry vol setup shifted)
        # For short puts: if vol was compressed at entry (premium was thin) and is now expanding,
        # the risk profile has worsened.
        # GAP 4 FIX: escalate more aggressively when vol regime flips to EXTREME/HIGH.
        # A short put entered at LOW vol that now faces EXTREME vol has fundamentally
        # changed risk profile — the premium collected is now dwarfed by potential assignment loss.
        # Passarelli Ch.6: "The edge in selling premium is only real when IV > HV. If the regime
        # flips to extreme, you are now selling cheap and exposed to directional risk."
        _vol_state_sp = str(row.get('VolatilityState_State', '') or '').split('.')[-1].upper()
        _iv_entry_sp  = float(row.get('IV_Entry', 0) or 0)
        _iv_now_sp    = float(row.get('IV_Now', 0) or row.get('IV_30D', 0) or 0)
        _iv_entry_low = _iv_entry_sp < 0.25 if _iv_entry_sp > 0 else False  # entry was low vol
        _iv_expanded  = _iv_now_sp > _iv_entry_sp * 1.5 if (_iv_entry_sp > 0 and _iv_now_sp > 0) else False

        thesis = DoctrineAuthority._check_thesis_degradation(row)
        if _vol_state_sp == 'EXTREME' and _iv_expanded and not is_itm:
            # Vol exploded from a low-vol entry — short put edge is structurally gone
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
        # BACKWARDATION: near-term IV elevated — short put is collecting above-normal premium (favorable)
        # CONTANGO: normal — premium is fair value
        iv_shape_sp = str(row.get('iv_surface_shape', '') or '').upper()
        if iv_shape_sp == 'BACKWARDATION':
            slope_sp = float(row.get('iv_ts_slope_30_90', 0) or 0)
            result['Rationale'] += (
                f" IV BACKWARDATED ({slope_sp:+.1f}pt 30-90d): short put collecting "
                f"above-normal near-term IV — premium favorable (Natenberg Ch.11)."
            )

        # 3c. Forward expectancy note (GAP 1 FIX)
        # For short puts, the put is profitable when stock stays ABOVE the breakeven.
        # If IV has expanded significantly and the stock is close to or below the put breakeven,
        # surface the expected-range context so the trader can see if assignment is now statistically likely.
        _ev_ratio_sp = float(row.get('EV_Feasibility_Ratio', 0) or 0) if pd.notna(row.get('EV_Feasibility_Ratio')) else float('nan')
        _em_10_sp    = float(row.get('Expected_Move_10D', 0) or 0)
        _req_sp      = float(row.get('Required_Move_Breakeven', 0) or row.get('Required_Move', 0) or 0)
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

    @staticmethod
    def _multi_leg_doctrine(row: pd.Series, result: Dict) -> Dict:
        # Straddles and strangles are long-volatility plays.
        # The enemy is theta; the edge is a realized move > premium paid.
        # Natenberg Ch.11: straddle profits if |move| > total premium paid.
        # Passarelli Ch.3: straddles should be exited when vol spike is complete.

        dte = row.get('DTE', 999) or 999
        drift_mag = row.get('Drift_Magnitude', '')
        vol_state = str(row.get('VolatilityState_State', 'NORMAL') or 'NORMAL').upper()
        pnl_pct = float(row.get('Total_GL_Decimal', 0) if pd.notna(row.get('Total_GL_Decimal')) else 0)

        # ── Calendar gates (GAP 2 FIX — now wired into multi-leg like all other doctrines) ──
        # Long vol structures (straddle/strangle) are especially sensitive to pre-holiday calendar:
        # the non-trading days are theta-negative (option bleeds theta over the gap with no
        # offsetting stock movement). McMillan Ch.7 + Passarelli Ch.6.
        _ml_cal_note = ""
        try:
            from scan_engine.calendar_context import expiry_proximity_flag, get_calendar_context
            _ul_ml   = float(row.get('UL Last', 0) or 0)
            _str_ml  = float(row.get('Strike', 0) or 0)
            _strat_ml = str(row.get('Strategy', '') or '').upper()
            _exp_flag_ml, _exp_note_ml = expiry_proximity_flag(
                dte=dte, strategy=_strat_ml,
                ul_last=_ul_ml, strike=_str_ml,
            )
            if _exp_flag_ml == 'PIN_RISK':
                result.update({
                    "Action": "EXIT",
                    "Urgency": "CRITICAL",
                    "Rationale": _exp_note_ml,
                    "Doctrine_Source": "McMillan Ch.7 + Natenberg Ch.15: Pin Risk",
                    "Decision_State": STATE_ACTIONABLE,
                    "Required_Conditions_Met": True
                })
                return result
            elif _exp_flag_ml == 'GAMMA_CRITICAL':
                result.update({
                    "Action": "EXIT",
                    "Urgency": "HIGH",
                    "Rationale": (
                        _exp_note_ml +
                        " For long vol: exit rather than roll — gamma spike benefits the short side, "
                        "not longs (Passarelli Ch.2: Theta Acceleration)."
                    ),
                    "Doctrine_Source": "Natenberg Ch.15 + Passarelli Ch.2: Gamma Critical (Long Vol)",
                    "Decision_State": STATE_ACTIONABLE,
                    "Required_Conditions_Met": True
                })
                return result
            elif _exp_flag_ml in ('PRE_HOLIDAY_EXPIRY', 'THETA_ACCELERATING'):
                _cal_ctx_ml = get_calendar_context()
                _gap_days   = getattr(_cal_ctx_ml, 'theta_bleed_days', 1)
                _ml_cal_note = (
                    f" ⚠️ Calendar: {_gap_days} non-trading day(s) ahead — "
                    f"long vol (straddle/strangle) bleeds theta with no stock movement. "
                    f"Passarelli Ch.6: pre-holiday hold cost is {_gap_days}× daily theta."
                )
            elif getattr(get_calendar_context(), 'is_pre_long_weekend', False) and dte <= 21:
                _gap_days = getattr(get_calendar_context(), 'theta_bleed_days', 2)
                _ml_cal_note = (
                    f" ⚠️ Pre-long-weekend: {_gap_days} theta days lost with no offsetting move. "
                    f"Consider closing before weekend if no catalyst expected."
                )
        except Exception:
            pass

        # ── Thesis-aware routing (Capital Survival Audit, Phase 3) ──────────
        # When the scan engine's entry thesis is available, use it to determine
        # whether current market state is thesis-confirming or thesis-violating.
        # A straddle entered for vol expansion should NOT be exited when vol expands.
        scan_thesis = str(row.get('Scan_Thesis', '') or '').lower()
        scan_bias = str(row.get('Scan_Trade_Bias', '') or '').upper()
        entered_for_vol_expansion = any(k in scan_thesis for k in
            ('vol expansion', 'expansion', 'bidirectional', 'volatility play',
             'cheap iv', 'iv cheap', 'gap_180d'))

        # ── Additional Greeks for expanded doctrine (Phase 5) ──────────────
        delta = float(row.get('Delta', 0) if pd.notna(row.get('Delta')) else 0)
        theta = float(row.get('Theta', 0) if pd.notna(row.get('Theta')) else 0)
        last_price = float(row.get('Last', 0) if pd.notna(row.get('Last')) else 0)
        iv_roc_3d = float(row.get('IV_ROC_3D', 0) if pd.notna(row.get('IV_ROC_3D')) else 0)

        # 1. Vol spike / big price move — THESIS-AWARE
        # If entered for vol expansion, EXPANDING/EXTREME is the GOAL, not the exit trigger.
        # Exit only when the gain has been captured or vol has peaked and is reversing.
        if drift_mag == 'High' or vol_state in ['EXPANDING', 'EXTREME']:
            if entered_for_vol_expansion:
                # Vol expansion IS the thesis. Check if profits should be taken.
                if pnl_pct >= 0.50:
                    result.update({
                        "Action": "EXIT",
                        "Urgency": "MEDIUM",
                        "Rationale": (
                            f"[Entry thesis: vol expansion] Vol expanded as expected "
                            f"(Vol={vol_state}) AND profit target reached ({pnl_pct:.0%}). "
                            f"Take gains — Passarelli Ch.3: vol mean-reverts."
                        ),
                        "Doctrine_Source": "Passarelli Ch.3: Thesis-Confirmed Profit Exit",
                        "Decision_State": STATE_ACTIONABLE,
                        "Required_Conditions_Met": True
                    })
                    return result
                elif iv_roc_3d < -0.10 and vol_state == 'EXPANDING':
                    # Vol was expanding but is now sharply reversing
                    result.update({
                        "Action": "EXIT",
                        "Urgency": "HIGH",
                        "Rationale": (
                            f"[Entry thesis: vol expansion] Vol peak detected — "
                            f"expansion is reversing (IV_ROC_3D={iv_roc_3d:.2f}). "
                            f"Exit before mean-reversion erodes gains (Natenberg Ch.11)."
                        ),
                        "Doctrine_Source": "Natenberg Ch.11: Vol Mean-Reversion",
                        "Decision_State": STATE_ACTIONABLE,
                        "Required_Conditions_Met": True
                    })
                    return result
                # else: vol is still expanding, thesis is working → fall through to HOLD
            else:
                # No vol-expansion thesis — original behavior: exit on vol spike
                result.update({
                    "Action": "EXIT",
                    "Urgency": "MEDIUM",
                    "Rationale": f"Volatility/price move target reached (Drift={drift_mag}, Vol={vol_state}) — close before theta erodes the gain (Passarelli Ch.3: Vol Spike Exit).",
                    "Doctrine_Source": "Passarelli Ch.3: Vol Spike Exit",
                    "Decision_State": STATE_ACTIONABLE,
                    "Required_Conditions_Met": True
                })
                return result

        # 2. Profit target: position up 50%+ of max theoretical gain
        # Natenberg Ch.11: don't hold long vol past 50% — vol mean-reverts
        if pnl_pct >= 0.50:
            result.update({
                "Action": "EXIT",
                "Urgency": "MEDIUM",
                "Rationale": f"Profit target {pnl_pct:.0%} — close straddle/strangle; vol mean-reversion will erode remaining value (Natenberg Ch.11).",
                "Doctrine_Source": "Natenberg Ch.11: Profit Target",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result

        # 3. Theta bleed rate monitoring (Phase 5 expansion)
        # Passarelli Ch.2: when daily theta exceeds 3% of remaining premium,
        # the position is silently destroying value regardless of other conditions.
        if last_price > 0 and abs(theta) > 0:
            theta_bleed_pct = abs(theta) / last_price
            if theta_bleed_pct > 0.03 and dte > 21:
                result.update({
                    "Action": "EXIT",
                    "Urgency": "MEDIUM",
                    "Rationale": (
                        f"Theta bleed {theta_bleed_pct:.1%}/day exceeds 3% threshold "
                        f"(Theta=${theta:.2f}, Last=${last_price:.2f}) — "
                        f"silent value destruction. Passarelli Ch.2: Theta Monitoring."
                    ),
                    "Doctrine_Source": "Passarelli Ch.2: Theta Bleed Rate",
                    "Decision_State": STATE_ACTIONABLE,
                    "Required_Conditions_Met": True
                })
                return result

        # 4. Asymmetric leg management (Phase 5 expansion)
        # Natenberg Ch.11: when one side of the straddle dominates (|delta|>0.40),
        # the position has effectively become a directional trade, not a vol trade.
        if abs(delta) > 0.40:
            dominant_leg = "call" if delta > 0 else "put"
            result.update({
                "Action": "TRIM",
                "Urgency": "LOW",
                "Rationale": (
                    f"Delta={delta:.2f} — {dominant_leg} leg dominates. "
                    f"Position has become directional, not vol. "
                    f"Consider closing profitable leg to lock gains. "
                    f"Natenberg Ch.11: asymmetric leg management."
                ),
                "Doctrine_Source": "Natenberg Ch.11: Asymmetric Leg Management",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result

        # 5. Time stop: inside 21 DTE theta acceleration kills long vol
        # Passarelli Ch.2: gamma spike near expiry benefits shorts, not longs
        if dte <= 21:
            result.update({
                "Action": "EXIT",
                "Urgency": "HIGH",
                "Rationale": f"DTE={dte:.0f} ≤ 21 — theta acceleration destroys long vol value. Exit before gamma spike benefits the short side (Passarelli Ch.2).",
                "Doctrine_Source": "Passarelli Ch.2: Theta Acceleration",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result

        # 6. Vol mean-reversion detection (Phase 5 expansion)
        # Natenberg Ch.11: when IV was expanding but ROC turns sharply negative,
        # the vol expansion cycle is likely over — theta will dominate from here.
        if vol_state == 'NORMAL' and iv_roc_3d < -0.10:
            result.update({
                "Action": "EXIT",
                "Urgency": "HIGH",
                "Rationale": (
                    f"Vol mean-reversion detected (IV_ROC_3D={iv_roc_3d:.2f}) — "
                    f"expansion thesis exhausted. Natenberg Ch.11: vol reverts to mean."
                ),
                "Doctrine_Source": "Natenberg Ch.11: Vol Mean-Reversion",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result

        # 7. Vol collapsed back: position lost its edge
        # Natenberg Ch.11: if vol compresses with no move, the thesis is dead
        if vol_state in ['COMPRESSING', 'LOW'] and drift_mag not in ['High', 'Medium']:
            result.update({
                "Action": "EXIT",
                "Urgency": "MEDIUM",
                "Rationale": f"Vol compressed ({vol_state}) with no meaningful price move — long vol edge evaporated. Cut theta losses (Natenberg Ch.11).",
                "Doctrine_Source": "Natenberg Ch.11: Vol Collapse",
                "Decision_State": STATE_ACTIONABLE,
                "Required_Conditions_Met": True
            })
            return result

        # Default: HOLD — awaiting vol expansion
        _thesis_note = ""
        if entered_for_vol_expansion and vol_state in ['EXPANDING', 'EXTREME']:
            _thesis_note = " [Entry thesis: vol expansion — thesis is WORKING, HOLD per Natenberg Ch.11.]"
        result.update({
            "Action": "HOLD",
            "Urgency": "LOW",
            "Rationale": (
                f"Position awaiting vol expansion — DTE={dte:.0f}, Vol={vol_state}. "
                f"Within expected envelope (Natenberg Ch.11)."
                + _thesis_note + _ml_cal_note
            ),
            "Doctrine_Source": "Natenberg Ch.11: Neutral Maintenance",
            "Decision_State": STATE_NEUTRAL_CONFIDENT,
            "Required_Conditions_Met": True
        })
        return result

def generate_recommendations(df_signals: pd.DataFrame) -> pd.DataFrame:
    if df_signals.empty:
        return df_signals
    
    if df_signals.columns.duplicated().any():
        dupes = df_signals.columns[df_signals.columns.duplicated()].unique().tolist()
        raise ValueError(f"Cycle-3 Contract Violation: Duplicate columns detected: {dupes}")

    df = StrategyResolver.resolve(df_signals)

    # Initialize Schwab client (optional, for live data)
    schwab_client = None
    try:
        client_id = os.getenv("SCHWAB_APP_KEY")
        client_secret = os.getenv("SCHWAB_APP_SECRET")
        if client_id and client_secret:
            schwab_client = SchwabClient(client_id, client_secret)
            logger.info("✅ Schwab client initialized for market stress detection.")
    except Exception as e:
        logger.warning(f"⚠️ Schwab client initialization failed for market stress: {e}")

    # Classify market stress once per run
    market_stress_level, market_stress_metric, market_stress_basis = classify_market_stress(client=schwab_client)
    logger.info(f"📊 Global Market Stress: {market_stress_level} (Metric: {market_stress_basis}={market_stress_metric:.2f})")

    # Fetch technical indicators from the repository
    if 'Underlying_Ticker' in df.columns:
        underlying_tickers = df['Underlying_Ticker'].dropna().unique().tolist()
        if underlying_tickers:
            df_tech_indicators = get_latest_technical_indicators(underlying_tickers)
            if df_tech_indicators is not None and not df_tech_indicators.empty:
                # Ensure Snapshot_TS is datetime for proper merging
                df['Snapshot_TS'] = pd.to_datetime(df['Snapshot_TS'])
                df_tech_indicators['Snapshot_TS'] = pd.to_datetime(df_tech_indicators['Snapshot_TS'])

                # Rename Ticker → Underlying_Ticker if needed before merging
                if 'Ticker' in df_tech_indicators.columns and 'Underlying_Ticker' not in df_tech_indicators.columns:
                    df_tech_indicators = df_tech_indicators.rename(columns={'Ticker': 'Underlying_Ticker'})
                merge_keys = [k for k in ['Underlying_Ticker', 'Snapshot_TS'] if k in df_tech_indicators.columns]
                # Merge technical indicators into the main DataFrame, aligning by Ticker and Snapshot_TS
                df = pd.merge(df, df_tech_indicators, on=merge_keys, how='left', suffixes=('', '_Tech'))
                logger.info(f"✅ Merged {len(df_tech_indicators)} technical indicator records.")
            else:
                logger.warning("⚠️ No technical indicators found in repository for current tickers.")
        else:
            logger.warning("⚠️ No underlying tickers found to fetch technical indicators.")
    else:
        logger.warning("⚠️ 'Underlying_Ticker' column missing, cannot fetch technical indicators.")

    # === Ticker-Level Holistic Analysis (RAG: Strategic Context) ===
    # We calculate holistic health per ticker to inform individual leg decisions.
    # This addresses the "CSP evaluated in isolation" problem.
    if 'Underlying_Ticker' in df.columns and 'PCS' in df.columns:
        ticker_health = df.groupby('Underlying_Ticker')['PCS'].transform('mean')
        df['Ticker_Holistic_Health'] = ticker_health
        # Assignment is acceptable if holistic ticker health is strong
        df['Assignment_Acceptable'] = ticker_health > 70
    else:
        logger.warning("⚠️ Underlying_Ticker or PCS missing — using default holistic health")
        df['Ticker_Holistic_Health'] = 75.0
        df['Assignment_Acceptable'] = True

    # === Per-Ticker Context (for multi-leg coherence in doctrine) ===
    # Computes net delta and stock presence per underlying, injected as _Ticker_* columns
    # so doctrine functions can reason about portfolio coherence without signature changes.

    def _classify_ticker_structure(strategies: list, net_delta: float, net_vega: float) -> str:
        """Detect collective structure type for multi-trade tickers."""
        s = set(strategies)
        has_bw     = bool(s & {"BUY_WRITE", "COVERED_CALL"})
        has_call   = bool(s & {"BUY_CALL", "LONG_CALL"})
        has_put    = bool(s & {"BUY_PUT",  "LONG_PUT"})
        has_leap_c = "LEAPS_CALL" in s
        has_leap_p = "LEAPS_PUT"  in s

        if has_bw:                                      return "INCOME_WITH_LEGS"
        if has_call and has_put and has_leap_c:         return "BULL_VOL_LEVERED"
        if has_call and has_put and has_leap_p:         return "BEAR_VOL_LEVERED"
        if has_call and has_put:
            if net_delta > 0.3:                         return "STRADDLE_BULLISH_TILT"
            if net_delta < -0.3:                        return "STRADDLE_BEARISH_TILT"
            return "STRADDLE_SYNTHETIC"
        if has_leap_c and has_call and not has_put:     return "CALL_DIAGONAL"
        if has_leap_c and has_put  and not has_call:    return "BULL_HEDGE"
        if has_leap_p and has_call and not has_put:     return "BEAR_HEDGE"
        if has_leap_p and has_put  and not has_call:    return "PUT_DIAGONAL"
        if len(s) == 1:                                 return "SINGLE_LEG"
        return "MULTI_LEG_MIXED"

    ticker_ctx: dict = {}
    if 'Underlying_Ticker' in df.columns:
        for ticker, grp in df.groupby('Underlying_Ticker'):
            # Exclude expired options (DTE <= 0) from delta sum — stale data, settlement pending
            if 'DTE' in grp.columns and 'AssetType' in grp.columns:
                live_mask = ~((grp['AssetType'].str.upper() == 'OPTION') & (grp['DTE'].fillna(999) <= 0))
                live_grp = grp[live_mask]
            else:
                live_grp = grp
            delta_sum = float(live_grp['Delta'].fillna(0).sum()) if 'Delta' in live_grp.columns else 0.0
            has_stock = bool((grp.get('AssetType', pd.Series(dtype=str)) == 'STOCK').any()) \
                if 'AssetType' in grp.columns else False
            net_vega  = float(live_grp['Vega'].fillna(0).sum())  if 'Vega'  in live_grp.columns else 0.0
            net_theta = float(live_grp['Theta'].fillna(0).sum()) if 'Theta' in live_grp.columns else 0.0
            strat_list = live_grp['Strategy'].dropna().tolist() if 'Strategy' in live_grp.columns else []
            ticker_ctx[str(ticker)] = {
                'net_delta':       delta_sum,
                'has_stock':       has_stock,
                'net_vega':        net_vega,
                'net_theta':       net_theta,
                'trade_count':     live_grp['TradeID'].nunique() if 'TradeID' in live_grp.columns else 1,
                'strategy_mix':    ",".join(sorted(set(strat_list))),
                'structure_class': _classify_ticker_structure(strat_list, delta_sum, net_vega),
            }

    if ticker_ctx and 'Underlying_Ticker' in df.columns:
        df['_Ticker_Net_Delta'] = df['Underlying_Ticker'].map(
            lambda t: ticker_ctx.get(str(t), {}).get('net_delta', 0.0)
        ).fillna(0.0)
        df['_Ticker_Has_Stock'] = df['Underlying_Ticker'].map(
            lambda t: ticker_ctx.get(str(t), {}).get('has_stock', False)
        ).fillna(False)
        for _col_key, _col_name in [
            ('net_vega',        '_Ticker_Net_Vega'),
            ('net_theta',       '_Ticker_Net_Theta'),
            ('trade_count',     '_Ticker_Trade_Count'),
            ('strategy_mix',    '_Ticker_Strategy_Mix'),
            ('structure_class', '_Ticker_Structure_Class'),
        ]:
            df[_col_name] = df['Underlying_Ticker'].map(
                lambda t, k=_col_key: ticker_ctx.get(str(t), {}).get(k, None)
            )
    else:
        df['_Ticker_Net_Delta'] = 0.0
        df['_Ticker_Has_Stock'] = False
        df['_Ticker_Net_Vega']       = 0.0
        df['_Ticker_Net_Theta']      = 0.0
        df['_Ticker_Trade_Count']    = 1
        df['_Ticker_Strategy_Mix']   = ""
        df['_Ticker_Structure_Class'] = "SINGLE_LEG"

    # === Structural Decay Regime Classifier ===
    # Synthesises cross-signal convergence for long-vol structures.
    # Individual signals (IV_ROC, Delta_ROC, chop, theta) are component-aware.
    # This pass makes the engine regime-aware: it detects when those components
    # align simultaneously into "chop + IV compression + long vol = silent bleed."
    #
    # Output column: _Structural_Decay_Regime
    #   STRUCTURAL_DECAY  — 3+ unfavorable signals converging; escalate Signal_State
    #   DECAY_RISK        — 2 signals converging; warn but don't escalate
    #   NONE              — insufficient convergence or not a long-vol structure

    _LONG_VOL_STRUCTURES = {
        "BULL_VOL_LEVERED", "BEAR_VOL_LEVERED",
        "STRADDLE_SYNTHETIC", "STRADDLE_BULLISH_TILT", "STRADDLE_BEARISH_TILT",
        "CALL_DIAGONAL", "PUT_DIAGONAL", "BULL_HEDGE", "BEAR_HEDGE",
        "MULTI_LEG_MIXED",  # include — needs manual review
    }

    def _score_structural_decay(row) -> str:
        structure = str(row.get('_Ticker_Structure_Class', '') or '')
        net_vega  = float(row.get('_Ticker_Net_Vega',  0) or 0)
        net_theta = float(row.get('_Ticker_Net_Theta', 0) or 0)

        # Only classify long-vol structures that are net-vega-long and theta-negative
        if structure not in _LONG_VOL_STRUCTURES:
            return "NONE"
        if net_vega <= 0:
            return "NONE"
        if net_theta >= -5:          # less than $5/day theta drag — not material
            return "NONE"

        score = 0

        # Signal 1: IV actively compressing
        # Threshold is IV-level-relative, not absolute.
        # Natenberg Ch.8: "meaningful moves in volatility-normalized terms."
        # A -0.08 absolute drop on a 25% IV stock = 32% of its level (extreme).
        # The same drop on a 120% IV stock (MSTR/COIN) = 7% of its level (noise).
        # Formula: threshold = max(0.08, IV_Now × 0.20) — 20% of current IV level.
        # Floor of 0.08 prevents the threshold collapsing on low-vol names.
        # Severe: 2× the base threshold (regime-scaled).
        iv_roc  = pd.to_numeric(row.get('IV_ROC_3D'), errors='coerce')
        iv_now  = pd.to_numeric(row.get('IV_Now'),    errors='coerce')
        _iv_lvl = float(iv_now) if pd.notna(iv_now) and iv_now > 0 else float('nan')
        # Normalise: stored as decimal (0.33) or percentage (33.0)
        if pd.notna(_iv_lvl) and _iv_lvl >= 5:
            _iv_lvl = _iv_lvl / 100.0
        _iv_compress_thresh = max(0.08, _iv_lvl * 0.20) if pd.notna(_iv_lvl) else 0.08
        _iv_severe_thresh   = _iv_compress_thresh * 2.0

        if pd.notna(iv_roc) and iv_roc < -_iv_compress_thresh:
            score += 1
            if iv_roc < -_iv_severe_thresh:      # severe compression (regime-scaled)
                score += 1

        # Signal 2: Range-bound / choppy price action (realized vol not helping)
        chop = float(row.get('choppiness_index', 50) or 50)
        adx  = float(row.get('adx_14', 25) or 25)
        if chop > 55 or adx < 22:
            score += 1

        # Signal 3: No directional move to rescue long delta/gamma
        d_roc = pd.to_numeric(row.get('Delta_ROC_3D'), errors='coerce')
        if pd.notna(d_roc) and abs(d_roc) < 0.08:
            score += 1

        # Signal 4: IV below HV (selling premium into you is cheap — you're losing edge)
        iv_gap = pd.to_numeric(row.get('IV_vs_HV_Gap'), errors='coerce')
        if pd.notna(iv_gap) and iv_gap < -0.05:
            score += 1

        if score >= 3:
            return "STRUCTURAL_DECAY"
        if score >= 2:
            return "DECAY_RISK"
        return "NONE"

    df['_Structural_Decay_Regime'] = df.apply(_score_structural_decay, axis=1)

    STRATEGY_NORMALIZATION_MAP = {
        'Covered_Call': 'COVERED_CALL',
        'Cash_Secured_Put': 'CSP',
        'Buy_Call': 'BUY_CALL',
        'Buy_Put': 'BUY_PUT',
        'LEAPS_Call': 'LEAPS_CALL',
        'LEAPS_Put': 'LEAPS_PUT',
        'Buy_Write': 'BUY_WRITE',
        'Long_Straddle': 'STRADDLE',
        'Long_Strangle': 'STRANGLE',
        'STOCK_ONLY': 'STOCK_ONLY',
        'STOCK_ONLY_IDLE': 'STOCK_ONLY_IDLE',
        'LEAPS_CALL': 'LEAPS_CALL',
        'LEAPS_PUT': 'LEAPS_PUT',
        'Unknown': 'UNKNOWN'
    }

    if 'Strategy' in df.columns:
        df['Strategy'] = df['Strategy'].map(lambda x: STRATEGY_NORMALIZATION_MAP.get(x, x))
    else:
        logger.warning("⚠️ Strategy column missing in generate_recommendations")

    # RAG: Ensure all required columns for dashboard exist
    for col in ['Uncertainty_Reasons', 'Missing_Data_Fields']:
        if col not in df.columns:
            df[col] = [[] for _ in range(len(df))]

    def apply_guards(row):
        reasons = []
        missing_fields = []
        strategy = row.get('Strategy', 'UNKNOWN')
        
        # Global Market Stress Guard (Tier 1)
        if should_halt_trades(market_stress_level):
            return {
                "Action": "HALT",
                "Urgency": "CRITICAL",
                "Rationale": get_halt_reason(market_stress_level, market_stress_metric, market_stress_basis),
                "Decision_State": STATE_BLOCKED_GOVERNANCE,
                "Uncertainty_Reasons": ["MARKET_STRESS_HALT"],
                "Missing_Data_Fields": [],
                "Doctrine_Source": "System: Market Stress Guard",
                "Required_Conditions_Met": False
            }

        # Structural_Data_Complete reflects PriceStructure swing metrics (swing_hh_count,
        # break_of_structure, etc.).  These are supplementary context — NOT required for
        # core doctrine evaluation.  The doctrine engine uses equity integrity, Greeks,
        # IV/HV, DTE, and cost basis, all of which are independent of swing metrics.
        # Blocking doctrine evaluation here caused ALL positions to get HOLD with
        # "Structure unresolved" (PriceStructure metrics never populated).
        if not row.get('Structural_Data_Complete', True):
            logger.debug(f"PriceStructure incomplete for {row.get('Symbol')} — doctrine will evaluate with available data")

        # RAG: Epistemic Strictness
        # IV is only required for options. Stocks do not have IV.
        is_option_leg = row.get('AssetType') == 'OPTION'
        if is_option_leg and pd.isna(row.get('IV_30D')):
            logger.debug(f"DEBUG: IV_AUTHORITY_MISSING triggered for LegID: {row.get('LegID')}, AssetType: {row.get('AssetType')}, IV_30D: {row.get('IV_30D')}")
            reasons.append(REASON_IV_AUTHORITY_MISSING)
            missing_fields.append('IV_30D')
        # Removed HV_DATA_MISSING check as market stress is now handled globally

        # Check for chart state completeness (distilled from raw indicators by ChartStateEngine)
        # Raw indicator columns (RSI_14, ADX_14, etc.) are not required — chart states are sufficient.
        required_chart_states = ['TrendIntegrity_State', 'VolatilityState_State', 'AssignmentRisk_State']
        chart_states_missing = [s for s in required_chart_states if pd.isna(row.get(s)) or str(row.get(s, '')).strip() == '']
        if chart_states_missing:
            reasons.append(f"MISSING_CHART_STATES: {', '.join(chart_states_missing)}")
            missing_fields.extend(chart_states_missing)

        if strategy == 'BUY_WRITE':
            if pd.isna(row.get('Underlying_Price_Entry')):
                reasons.append(REASON_STOCK_LEG_NOT_AVAILABLE)
                missing_fields.append('Underlying_Price_Entry')
        
        if reasons:
            return {
                "Action": "HOLD",
                "Urgency": "LOW",
                "Rationale": f"Action refused due to uncertainty: {', '.join(reasons)}",
                "Decision_State": STATE_UNCERTAIN,
                "Uncertainty_Reasons": reasons,
                "Missing_Data_Fields": missing_fields,
                "Doctrine_Source": "System: Uncertainty Guard",
                "Required_Conditions_Met": False
            }
        return None
    
    guards = df.apply(apply_guards, axis=1)
    
    def evaluate_with_guard(row):
        guard_result = guards.loc[row.name]
        result = guard_result if guard_result else DoctrineAuthority.evaluate(row)

        # Prepend trade journey context to every rationale.
        # Reads Prior_* columns injected by run_all.py step 2.95.
        # Guards (HALT, data incomplete) are excluded — they fire before doctrine and
        # their rationale is system-generated, not trader-narrative.
        _action = result.get("Action", "")
        _decision_state = result.get("Decision_State", "")
        if _action not in ("HALT",) and _decision_state != STATE_BLOCKED_GOVERNANCE:
            try:
                _ul = float(row.get("UL Last") or row.get("Last") or 0)
                _prior_action = str(row.get("Prior_Action") or "").strip().upper()
                _jnote = _build_journey_note(row, _action, _ul)
                if _jnote:
                    result["Rationale"] = _jnote + "\n" + result.get("Rationale", "")

                # Action-flip urgency escalation:
                # EXIT→HOLD means we had a profit-take signal but didn't act. The stock
                # retraced. This is not a clean HOLD — it needs fresh eyes with MEDIUM urgency.
                # (McMillan Ch.4: re-evaluate after a missed exit before the edge erodes further.)
                if _prior_action == "EXIT" and _action == "HOLD":
                    if result.get("Urgency", "LOW") == "LOW":
                        result["Urgency"] = "MEDIUM"
                        result["Rationale"] = result.get("Rationale", "") + (
                            " ⚠️ Urgency elevated: prior EXIT signal not acted on — "
                            "re-evaluate whether this pullback confirms hold or whether "
                            "the exit thesis still stands (McMillan Ch.4)."
                        )

                # Scan signal feedback — cross-system directional conflict detection.
                # run_all.py step 2.96 injects Scan_Current_Bias (BULLISH/BEARISH/NEUTRAL/MIXED)
                # from the latest Step12 acceptance CSV.  When an open position runs against
                # the current scan signal, surface a conflict note so the trader knows the
                # two systems disagree.
                #
                # Direction map:
                #   LONG_CALL / LEAP / BUY_WRITE → position is BULLISH
                #   LONG_PUT                      → position is BEARISH
                #   short legs (COVERED_CALL, etc.) with stock → BULLISH underlying exposure
                #
                # Conflict rules (directionally aware):
                #   BULLISH position + BEARISH scan  → conflict — scan disagrees with holding
                #   BEARISH position + BULLISH scan  → conflict — scan disagrees with holding
                #   Any position + MIXED scan        → note only (scan has both sides)
                #
                # Urgency escalation: HOLD + conflict → bump to MEDIUM; EXIT/ROLL unchanged.
                _scan_bias = str(row.get("Scan_Current_Bias") or "").strip().upper()
                if _scan_bias and _scan_bias not in ("", "NONE", "NAN", "NEUTRAL"):
                    _pos_strat  = str(row.get("Strategy", "") or "").upper()
                    _pos_qty    = float(row.get("Quantity", 1) or 1)
                    _pos_is_bullish = (
                        "LONG_CALL"     in _pos_strat
                        or "LEAP"       in _pos_strat
                        or "BUY_WRITE"  in _pos_strat
                        or "COVERED_CALL" in _pos_strat
                        or ("LONG_CALL" not in _pos_strat and "LONG_PUT" not in _pos_strat
                            and _pos_qty > 0 and "CALL" in _pos_strat)
                    )
                    _pos_is_bearish = (
                        "LONG_PUT" in _pos_strat
                        or ("PUT" in _pos_strat and "CASH_SECURED" not in _pos_strat
                            and "SHORT_PUT" not in _pos_strat and _pos_qty > 0)
                    )

                    _conflict_note = ""
                    if _scan_bias == "MIXED":
                        _conflict_note = (
                            " 📡 **Scan note:** scan engine has BOTH bullish and bearish "
                            "candidates on this ticker simultaneously — cross-signal ambiguity. "
                            "Verify position direction aligns with your intended thesis before adding exposure."
                        )
                    elif _pos_is_bullish and _scan_bias == "BEARISH":
                        _conflict_note = (
                            " ⚡ **Scan conflict:** scan engine is currently **BEARISH** on "
                            f"{row.get('Underlying_Ticker', 'this ticker')} while this position is "
                            "long directional. The two systems disagree — the scan signal suggests "
                            "the setup that justified entry has reversed. "
                            "Verify thesis is still intact before holding "
                            "(McMillan Ch.4: don't hold a directional position against the trend)."
                        )
                        # Escalate HOLD to MEDIUM when scan conflicts — silent HOLD is dangerous here
                        if _action == "HOLD" and result.get("Urgency", "LOW") == "LOW":
                            result["Urgency"] = "MEDIUM"
                    elif _pos_is_bearish and _scan_bias == "BULLISH":
                        _conflict_note = (
                            " ⚡ **Scan conflict:** scan engine is currently **BULLISH** on "
                            f"{row.get('Underlying_Ticker', 'this ticker')} while this position is "
                            "bearish directional. The two systems disagree — the scan signal suggests "
                            "the bearish setup has reversed to bullish. "
                            "Verify put thesis is still intact "
                            "(McMillan Ch.4: don't hold a directional position against the trend)."
                        )
                        if _action == "HOLD" and result.get("Urgency", "LOW") == "LOW":
                            result["Urgency"] = "MEDIUM"

                    if _conflict_note:
                        result["Rationale"] = result.get("Rationale", "") + _conflict_note
                        result["Scan_Conflict"] = _scan_bias   # surface for display layer

            except Exception:
                pass  # journey note is always non-blocking

        # ── Structural Decay Regime annotation ───────────────────────────────
        # Reads _Structural_Decay_Regime (computed vectorially above).
        # Non-blocking: never overrides Action, only annotates Signal_State
        # and appends to Rationale so the dashboard drift strip reflects it.
        try:
            _sdr = str(row.get('_Structural_Decay_Regime', 'NONE') or 'NONE')
            if _sdr in ('STRUCTURAL_DECAY', 'DECAY_RISK'):
                _net_t = float(row.get('_Ticker_Net_Theta', 0) or 0)
                _net_v = float(row.get('_Ticker_Net_Vega',  0) or 0)
                _iv_r  = pd.to_numeric(row.get('IV_ROC_3D'),   errors='coerce')
                _chop  = float(row.get('choppiness_index', 50) or 50)
                _adx   = float(row.get('adx_14', 25) or 25)
                _iv_gap= pd.to_numeric(row.get('IV_vs_HV_Gap'), errors='coerce')

                # Reconstruct the scaled IV threshold for display (mirrors _score_structural_decay)
                _ann_iv_now  = pd.to_numeric(row.get('IV_Now'), errors='coerce')
                _ann_iv_lvl  = float(_ann_iv_now) if pd.notna(_ann_iv_now) and _ann_iv_now > 0 else float('nan')
                if pd.notna(_ann_iv_lvl) and _ann_iv_lvl >= 5:
                    _ann_iv_lvl = _ann_iv_lvl / 100.0
                _ann_thresh  = max(0.08, _ann_iv_lvl * 0.20) if pd.notna(_ann_iv_lvl) else 0.08

                _decay_detail = (
                    f"θ/day={_net_t:+.1f}, ν={_net_v:+.0f}"
                    + (f", IV ROC 3D={_iv_r:+.2f} (thresh={_ann_thresh:.2f})" if pd.notna(_iv_r) else "")
                    + f", chop={_chop:.0f}, ADX={_adx:.0f}"
                    + (f", IV vs HV={_iv_gap:+.1%}" if pd.notna(_iv_gap) else "")
                )

                if _sdr == 'STRUCTURAL_DECAY':
                    # ── Directional Confirmation Branch ───────────────────────────────
                    # The structural decay regime was designed for STAGNANT long-vol
                    # positions (range-bound stock, falling IV, theta/vega bleeding with
                    # no directional offset). If direction IS working — stock moving toward
                    # the strike, delta growing in the thesis direction, momentum sustained —
                    # then theta cost is being offset by intrinsic gain and the bleed
                    # characterisation is wrong. Suppress the DEGRADED escalation.
                    #
                    # Thesis-aware directional check:
                    #   LONG_PUT: price must be falling (Price_Drift_Pct < -2%) AND
                    #             delta growing more negative (Delta_ROC_3D < -0.05, i.e.
                    #             option gaining sensitivity as stock moves toward strike) AND
                    #             momentum not stalling/reversing
                    #   LONG_CALL: mirror — price rising, delta growing more positive
                    #
                    # The Delta_ROC_3D signal is the decisive one: the scorer penalises
                    # abs(d_roc) < 0.08 as "no directional move". For a LONG_PUT with the
                    # stock falling, d_roc will be < -0.08 (delta going more negative = put
                    # gaining intrinsic). That same number suppresses the false DEGRADED here.
                    #
                    # Doctrine: McMillan Ch.4: "A long put that is working — stock declining,
                    #   delta increasing, momentum sustained — is not in decay. It is in
                    #   theta-offset directional gain. Decay only applies when there is NO
                    #   directional component to counterbalance the carry cost."
                    # ─────────────────────────────────────────────────────────────────────
                    _strat_sdr = str(row.get('Strategy', '') or '').upper()
                    _is_long_put_sdr  = any(s in _strat_sdr for s in ('LONG_PUT', 'BUY_PUT', 'LEAPS_PUT'))
                    _is_long_call_sdr = any(s in _strat_sdr for s in ('LONG_CALL', 'BUY_CALL', 'LEAPS_CALL'))

                    _price_drift_sdr = float(row.get('Price_Drift_Pct', 0) or 0)
                    _d_roc_sdr       = pd.to_numeric(row.get('Delta_ROC_3D'), errors='coerce')
                    _mom_sdr_raw     = str(row.get('MomentumVelocity_State', '') or '').split('.')[-1].upper()
                    _mom_sustained   = _mom_sdr_raw in ('TRENDING', 'SUSTAINED', 'ACCELERATING')

                    # Direction working: price moved ≥2% toward strike AND delta growing
                    # in thesis direction AND momentum not stalling/reversing
                    if _is_long_put_sdr:
                        _direction_working = (
                            _price_drift_sdr < -0.02                       # stock falling ≥2%
                            and pd.notna(_d_roc_sdr) and _d_roc_sdr < -0.05  # put gaining sensitivity
                            and _mom_sustained                              # not stalling
                        )
                    elif _is_long_call_sdr:
                        _direction_working = (
                            _price_drift_sdr > 0.02                        # stock rising ≥2%
                            and pd.notna(_d_roc_sdr) and _d_roc_sdr > 0.05   # call gaining sensitivity
                            and _mom_sustained                              # not stalling
                        )
                    else:
                        _direction_working = False

                    if _direction_working:
                        # Structural decay signals present but direction is offsetting.
                        # Downgrade to DECAY_RISK note (warn, don't escalate to DEGRADED).
                        _decay_note = (
                            f"  ⚠️ **Decay signals present but direction working** — "
                            f"stock moving toward strike ({_price_drift_sdr:+.1%} drift, "
                            f"Δ ROC 3D={float(_d_roc_sdr):+.2f}, {_mom_sdr_raw} momentum). "
                            f"Theta cost is being offset by intrinsic gain. "
                            f"Monitor: if momentum stalls or drift reverses, decay escalates. "
                            f"({_decay_detail})"
                        )
                        # Do NOT escalate Signal_State — direction is working
                    else:
                        _decay_note = (
                            f"  🔴 **Structural Decay Regime:** long-vol structure in "
                            f"chop + IV compression — bleeding theta AND vega simultaneously. "
                            f"Low realized movement + falling IV = silent bleed. "
                            f"({_decay_detail})  "
                            f"Review whether the vol expansion thesis still has a near-term catalyst."
                        )
                        # Escalate Signal_State to DEGRADED if not already VIOLATED
                        _cur_ss = result.get('Signal_State', 'VALID')
                        if _cur_ss not in ('VIOLATED', 'DEGRADED'):
                            result['Signal_State'] = 'DEGRADED'
                    result['Drift_Action'] = result.get('Drift_Action', 'NO_ACTION')
                else:  # DECAY_RISK
                    _decay_note = (
                        f"  ⚠️ **Decay Risk:** 2 structural decay signals converging "
                        f"(chop + IV compression or delta flatline). "
                        f"Not yet critical but monitor — if realized vol stays low "
                        f"and IV continues to compress, this becomes active bleed. "
                        f"({_decay_detail})"
                    )

                result['Rationale'] = result.get('Rationale', '') + _decay_note
                result['_Structural_Decay_Regime'] = _sdr
        except Exception:
            pass  # structural decay annotation is always non-blocking

        return result

    decisions = df.apply(evaluate_with_guard, axis=1)
    decision_df = pd.DataFrame(decisions.tolist(), index=df.index)

    # Preserve existing columns from df_signals that are not overwritten by decision_df
    # This includes PnL_Total, PnL_Unexplained, etc.
    for col in df_signals.columns:
        if col not in df.columns: # Only add if not already present (e.g., from StrategyResolver)
            df[col] = df_signals[col]

    for col in decision_df.columns:
        if col == 'Strategy': continue # Strategy is already handled
        df[col] = decision_df[col]

    df['RAG_Citation'] = df['Doctrine_Source']

    # NOTE: _apply_execution_readiness() is intentionally called in run_all.py
    # AFTER all post-processing (drift overrides, Action mutations, schema enforcement)
    # so it sees the final resolved Action column — not the pre-drift version.

    return df


def _apply_execution_readiness(df: pd.DataFrame) -> pd.DataFrame:
    """
    Layer 2 — Execution Readiness (backend component).

    Runs after doctrine + drift filter to classify each row as:
      EXECUTE_NOW      — act on current run; structural urgency or risk override
      WAIT_FOR_WINDOW  — action is valid but execution conditions are poor;
                         wait for liquidity/spread to improve
      STAGE_AND_RECHECK — signal is marginal or conditions mixed;
                          pre-stage candidates, monitor next run

    Inputs used (all from pipeline data — no live chain call):
      Action, Urgency: from doctrine engine
      DTE: option time-to-expiry
      Delta: for gamma/convexity proximity detection
      Roll_Candidate_1: spread_pct from pre-staged candidates
      Earnings Date: broker CSV earnings proximity
      IV_vs_HV_Gap: spread environment quality proxy

    Rules (priority order — first match wins):

    EXECUTE_NOW forced:
      • Action=EXIT (any urgency)          — thesis broken, delay compounds loss
      • Action=ROLL/TRIM + Urgency=CRITICAL — time-critical, don't wait
      • DTE ≤ 3                            — pin risk, theta zero, must act
      • |Delta| ≥ 0.70 (ITM deep)         — gamma dominance, rapid value decay
      • Earnings within 1 day             — IV event imminent

    WAIT_FOR_WINDOW:
      • Action=HOLD                        — no structural trigger, time is available
      • Action=ROLL_WAIT                   — doctrine already gated this
      • Spread ≥ 12% on Roll_Candidate_1  — execution cost destroys credit
      • IV_vs_HV_Gap ≤ -5 (IV crushed)   — vol too low to sell into for credit roll

    STAGE_AND_RECHECK (default for ROLL/TRIM with no forcing condition):
      • Action=ROLL + Urgency=LOW/MEDIUM  — valid but not urgent; pre-stage, wait for window
      • Spread 8–12% on candidate         — marginal execution, check later

    EXECUTE_NOW is the default for any unclassified ROLL/EXIT/TRIM with MEDIUM+ urgency.

    Passarelli Ch.6: "Decouple the decision to roll from the moment of execution.
    The decision is structural; the execution is tactical."
    McMillan Ch.3: "Never execute a roll into a wide spread — the credit is theoretical."
    """
    import json as _json

    def _readiness(row: pd.Series):
        action   = str(row.get('Action', '')  or '').upper()
        urgency  = str(row.get('Urgency', '') or '').upper()
        dte      = pd.to_numeric(row.get('DTE'), errors='coerce')
        delta    = abs(pd.to_numeric(row.get('Delta', 0), errors='coerce') or 0)

        # Parse spread from Roll_Candidate_1 if available
        spread_pct = None
        _rc1_raw = row.get('Roll_Candidate_1')
        if _rc1_raw and _rc1_raw not in ('', 'nan'):
            try:
                _rc1 = _json.loads(str(_rc1_raw)) if isinstance(_rc1_raw, str) else _rc1_raw
                if isinstance(_rc1, dict):
                    spread_pct = float(_rc1.get('spread_pct', 0) or 0) or None
            except Exception:
                pass

        iv_hv_gap = pd.to_numeric(row.get('IV_vs_HV_Gap'), errors='coerce')

        # Parse earnings proximity
        days_to_earn = None
        _earn_raw = row.get('Earnings Date')
        if _earn_raw not in (None, '', 'nan', 'N/A') and not (
            isinstance(_earn_raw, float) and pd.isna(_earn_raw)
        ):
            try:
                _ed = pd.to_datetime(str(_earn_raw), errors='coerce')
                if pd.notna(_ed):
                    days_to_earn = (_ed.normalize() - pd.Timestamp.now().normalize()).days
            except Exception:
                pass

        reasons = []

        # ── EXECUTE_NOW forcing conditions ────────────────────────────────────
        if action == 'EXIT':
            return 'EXECUTE_NOW', 'EXIT action — thesis broken; delay compounds loss'

        if action in ('ROLL', 'TRIM', 'HALT') and urgency == 'CRITICAL':
            return 'EXECUTE_NOW', f'{action} + CRITICAL urgency — time-sensitive, act immediately'

        if pd.notna(dte) and dte <= 3:
            return 'EXECUTE_NOW', f'DTE={int(dte)}d — pin risk active, theta near zero; act today'

        if delta >= 0.70:
            return 'EXECUTE_NOW', (
                f'|Delta|={delta:.2f} ≥ 0.70 — deep ITM gamma dominance; '
                'intrinsic decaying, roll before extrinsic gone'
            )

        if days_to_earn is not None and 0 <= days_to_earn <= 1:
            return 'EXECUTE_NOW', (
                f'Earnings in {days_to_earn}d — IV event imminent; '
                'execute before vol crush or IV spike (Natenberg Ch.8)'
            )

        # ── WAIT_FOR_WINDOW conditions ────────────────────────────────────────
        if action in ('HOLD', 'HOLD_FOR_REVERSION'):
            return 'WAIT_FOR_WINDOW', 'HOLD — no structural trigger; collect theta, wait for setup'

        if action == 'ROLL_WAIT':
            return 'WAIT_FOR_WINDOW', 'ROLL_WAIT — doctrine gated this; conditions not yet met'

        if spread_pct is not None and spread_pct >= 12.0:
            reasons.append(f'spread={spread_pct:.1f}% ≥ 12% — credit theoretical at this width')

        if pd.notna(iv_hv_gap) and iv_hv_gap <= -5.0:
            reasons.append(f'IV/HV gap={iv_hv_gap:+.1f}pt — IV crushed vs realized; credit environment poor')

        if reasons and action in ('ROLL', 'TRIM'):
            return 'WAIT_FOR_WINDOW', '; '.join(reasons) + ' (McMillan Ch.3: wait for spread to tighten)'

        # ── STAGE_AND_RECHECK ─────────────────────────────────────────────────
        if action in ('ROLL', 'TRIM') and urgency in ('LOW', 'MEDIUM'):
            _stage_reason = f'{action} + {urgency} urgency — valid signal, not urgent'
            if spread_pct is not None and 8.0 <= spread_pct < 12.0:
                _stage_reason += f'; spread={spread_pct:.1f}% marginal — wait for tighter window'
            return 'STAGE_AND_RECHECK', _stage_reason + ' (Passarelli Ch.6: decouple decision from execution)'

        # ── Default: EXECUTE_NOW for any remaining active action ─────────────
        return 'EXECUTE_NOW', f'{action} + {urgency} urgency — proceed in next good window'

    results = df.apply(_readiness, axis=1)
    df['Execution_Readiness']        = results.apply(lambda x: x[0])
    df['Execution_Readiness_Reason'] = results.apply(lambda x: x[1])

    # ── Scan Feedback Integration (Capital Survival Audit, Phase 4) ──────────
    # Scan_DQS_Score and Scan_Confidence are injected by run_all.py from the
    # latest Step12 output.  When the scan engine sees the current setup as LOW
    # quality (DQS < 50), it de-risks two specific management decisions:
    #   1. ROLL → HOLD: a low-quality entry setup means the roll target is also
    #      questionable; wait for setup to improve before extending exposure.
    #   2. Scale-up (HOLD with scale intent): DQS < 50 blocks scale-up entirely
    #      per Vince (f-fraction scaling requires same edge quality as original entry).
    # NEVER blocks EXIT — exits are driven by position state, not entry quality.
    # Doctrine: Chan (Quantitative Trading Ch.3), Vince (Mathematics of Money Mgmt).
    if 'Scan_DQS_Score' in df.columns:
        def _apply_scan_feedback(row):
            dqs_raw = row.get('Scan_DQS_Score')
            action  = str(row.get('Action', '') or '').upper()
            try:
                dqs = float(dqs_raw) if dqs_raw is not None and pd.notna(dqs_raw) else None
            except (TypeError, ValueError):
                dqs = None

            if dqs is None or dqs >= 50:
                return row  # Insufficient data or acceptable quality — no change

            # DQS < 50: weak scan quality for this ticker's current setup
            if action == 'ROLL':
                # Emergency gates bypass scan feedback — never downgrade a structural emergency
                _dte_sf = pd.to_numeric(row.get('DTE'), errors='coerce')
                _delta_sf = abs(pd.to_numeric(row.get('Delta', 0), errors='coerce') or 0)
                _urgency_sf = str(row.get('Urgency', '') or '').upper()
                if (pd.notna(_dte_sf) and _dte_sf < 7) or _delta_sf >= 0.70 or _urgency_sf == 'CRITICAL':
                    return row  # Emergency ROLL — do not override with scan feedback
                row = row.copy()
                row['Action']   = 'HOLD'
                row['Urgency']  = 'LOW'
                row['Rationale'] = (
                    f"[ScanFeedback] Scan_DQS={dqs:.0f}<50 — setup quality degraded. "
                    f"ROLL target is also weak. Hold, wait for DQS recovery. "
                    f"Original: {row.get('Rationale', '')} | "
                    f"Chan Ch.3: do not extend exposure into a low-edge environment."
                )
                row['Doctrine_Source'] = 'ScanFeedback_DQS_Low: ROLL→HOLD (Chan Ch.3)'
            return row

        df = df.apply(_apply_scan_feedback, axis=1)
        low_dqs_rolls = (
            (df.get('Scan_DQS_Score', pd.Series(dtype=float)).fillna(100) < 50) &
            (df.get('Action', pd.Series(dtype=str)) == 'HOLD') &
            (df.get('Doctrine_Source', pd.Series(dtype=str)).str.startswith('ScanFeedback', na=False))
        ).sum()
        if low_dqs_rolls > 0:
            logger.info(f"[ScanFeedback] {low_dqs_rolls} ROLL→HOLD overrides applied (Scan_DQS_Score < 50)")

    return df
