"""
Doctrine Authority — core evaluation engine for management decisions.

Extracted from engine.py. Contains:
- Decision state constants
- Epistemic authority levels and mappings
- DoctrineAuthority class with evaluate() + roll timing intelligence

Public API:
  DoctrineAuthority.evaluate(row) -> Dict[str, Any]
"""
import json
import logging
import math
from typing import Dict, Any

import numpy as np
import pandas as pd

from core.management.cycle1.identity.constants import (
    FIDELITY_MARGIN_RATE,
    FIDELITY_MARGIN_RATE_DAILY,
)

# Strategy-specific doctrine functions (extracted to separate modules)
from core.management.cycle3.doctrine.strategies.stock_only import stock_only_doctrine
from core.management.cycle3.doctrine.strategies.stock_only_idle import stock_only_idle_doctrine
from core.management.cycle3.doctrine.strategies.covered_call import covered_call_doctrine, covered_call_doctrine_v2
from core.management.cycle3.doctrine.strategies.buy_write import buy_write_doctrine, buy_write_doctrine_v2
from core.management.cycle3.doctrine.strategies.recovery_premium import recovery_premium_doctrine
from core.management.cycle3.doctrine.strategies.long_option import long_option_doctrine, long_option_doctrine_v2
from core.management.cycle3.doctrine.strategies.short_put import short_put_doctrine, short_put_doctrine_v2
from core.management.cycle3.doctrine.strategies.multi_leg import multi_leg_doctrine, multi_leg_doctrine_v2
from core.management.cycle3.doctrine.strategies.pmcc import pmcc_doctrine, pmcc_doctrine_v2

# Shadow mode: v1 function lookup for rollback comparison (v2 is now production)
_V1_SHADOW_DISPATCH = {
    "BUY_WRITE": buy_write_doctrine,
    "COVERED_CALL": covered_call_doctrine,
    "BUY_CALL": long_option_doctrine,
    "BUY_PUT": long_option_doctrine,
    "LEAPS_CALL": long_option_doctrine,
    "LEAPS_PUT": long_option_doctrine,
    "LONG_CALL": long_option_doctrine,
    "LONG_PUT": long_option_doctrine,
    "CSP": short_put_doctrine,
    "STRADDLE": multi_leg_doctrine,
    "STRANGLE": multi_leg_doctrine,
    "PMCC": pmcc_doctrine,
}

_V2_SHADOW_KEYS = [
    "Action", "Urgency", "Resolution_Method",
    "Proposals_Considered", "Proposals_Summary", "Winning_Gate",
]

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
    'PMCC': AUTHORITY_CONTEXTUAL,  # No stock, but LEAP acts as synthetic
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
    'PMCC': AUTHORITY_SUPPORTIVE,
    'STOCK_ONLY': AUTHORITY_NON_AUTHORITATIVE,
    'STOCK_ONLY_IDLE': AUTHORITY_NON_AUTHORITATIVE,
    'UNKNOWN': AUTHORITY_NON_AUTHORITATIVE
}

class DoctrineAuthority:
    """
    Cycle 3: Doctrine Authority Layer.
    """
    
    _REGISTERED_DOCTRINES = [
        'BUY_WRITE', 'COVERED_CALL', 'BUY_CALL', 'BUY_PUT', 'LONG_CALL', 'LONG_PUT',
        'CSP', 'STRADDLE', 'STRANGLE', 'STOCK_ONLY', 'STOCK_ONLY_IDLE',
        'LEAPS_CALL', 'LEAPS_PUT', 'PMCC'
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
                result = short_put_doctrine(row, result)
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

        # Guard: OI=0 on a contract that had substantial entry OI (>100) is almost
        # certainly an API data miss (Schwab returns 0 for deep ITM LEAPs after hours).
        # Don't trigger a hard exit on phantom data. Annotate instead.
        _oi_likely_data_miss = (_oi_current == 0 and _oi_entry > 100)
        _dte_raw = pd.to_numeric(row.get('DTE'), errors='coerce')

        if _is_option_leg and _oi_entry > 0 and not _oi_likely_data_miss:
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

        if _oi_likely_data_miss and _is_option_leg:
            result['OI_Deterioration_Warning'] = (
                f"OI data suspect: API returned 0 but entry OI was {_oi_entry:.0f}. "
                f"Likely Schwab API miss on deep ITM/LEAP contract. Verify manually."
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
            # Recovery Premium Mode: check if this damaged BW should switch
            # from trade management to multi-cycle basis reduction optimization.
            from core.management.cycle3.doctrine.helpers import should_enter_recovery_premium_mode
            from core.shared.finance_utils import safe_row_float, effective_cost_per_share as _ecp
            _rp_spot = safe_row_float(row, 'UL Last')
            _rp_cost, _, _ = _ecp(row, spot_fallback=_rp_spot)
            _rpm = should_enter_recovery_premium_mode(row, spot=_rp_spot, effective_cost=_rp_cost)
            if _rpm["should_activate"]:
                result = recovery_premium_doctrine(row, result, _rpm["context"])
            else:
                result = buy_write_doctrine_v2(row, result)
        elif strategy in ["COVERED_CALL"]:
            result = covered_call_doctrine_v2(row, result)
        elif strategy in ["BUY_CALL", "BUY_PUT", "LEAPS_CALL", "LEAPS_PUT", "LONG_CALL", "LONG_PUT"]:
            result = long_option_doctrine_v2(row, result)
        elif strategy in ["CSP"]:
            result = short_put_doctrine_v2(row, result)
        elif strategy in ["STRADDLE", "STRANGLE"]:
            result = multi_leg_doctrine_v2(row, result)
        elif strategy in ["PMCC"]:
            result = pmcc_doctrine_v2(row, result)
        elif strategy in ["STOCK_ONLY"]:
            result = stock_only_doctrine(row, result)
        elif strategy in ["STOCK_ONLY_IDLE"]:
            result = stock_only_idle_doctrine(row, result)
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

        # ── Shadow v1 (legacy gate cascade) ──────────────────────────────
        # Runs v1 alongside v2 production. v1 results stored as v1_* shadow
        # columns for rollback comparison. v1 failures never affect v2 output.
        _v1_fn = _V1_SHADOW_DISPATCH.get(strategy)
        if _v1_fn and not _oi_hard_exit:
            try:
                _v1_base = {
                    "Action": "HOLD", "Urgency": "LOW",
                    "Rationale": "default", "Doctrine_Source": "McMillan: Neutrality",
                    "Decision_State": STATE_NEUTRAL_CONFIDENT,
                }
                _v1_result = _v1_fn(row, _v1_base)
                for _k in _V2_SHADOW_KEYS:
                    result[f"v1_{_k}"] = _v1_result.get(_k, "")
            except Exception as _v1_err:
                logger.debug(f"[v1 shadow] {strategy} failed for {row.get('TradeID', '?')}: {_v1_err}")
                for _k in _V2_SHADOW_KEYS:
                    result[f"v1_{_k}"] = ""

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
        """Backward-compat wrapper — delegates to doctrine.strategies.stock_only."""
        return stock_only_doctrine(row, result)

    @staticmethod
    def _stock_only_idle_doctrine(row: pd.Series, result: Dict) -> Dict:
        """Backward-compat wrapper — delegates to doctrine.strategies.stock_only_idle."""
        return stock_only_idle_doctrine(row, result)

    @staticmethod
    def _buy_write_doctrine(row: pd.Series, result: Dict) -> Dict:
        """Backward-compat wrapper — delegates to doctrine.strategies.buy_write."""
        return buy_write_doctrine(row, result)
