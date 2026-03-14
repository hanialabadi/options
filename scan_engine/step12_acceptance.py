"""
Step 12: Acceptance Logic - Execution Gate

PURPOSE:
    Convert Phase 1 + Phase 2 enrichment into actionable execution decisions.
    This module acts as the final Execution Gate, applying deterministic rules
    to assign a final Execution_Status (READY, CONDITIONAL, BLOCKED) and a
    corresponding Block_Reason. It strictly enforces data quality, IV maturity,
    and strategy-specific requirements to prevent biased or unsafe trades.
    
ARCHITECTURAL PRINCIPLES:
    - Phase 1 inputs are REQUIRED and PRIMARY
    - Phase 2 inputs are OPTIONAL and SECONDARY
    - UNKNOWN in Phase 2 = neutral (not negative)
    - All rules are deterministic and explainable
    - No acceptance rule requires Phase 2 data to function
    - **Strictly enforces Execution Gate rules (READY, CONDITIONAL, BLOCKED)**
    
INPUTS:
    - Strategy_Type: 'Volatility-Selling' | 'Directional' | 'UNKNOWN'
    - IV_Maturity_Level: 1-5 (from IVEngine: 1=<20d, 2=20-60d, 3=60-120d, 4=120-180d, 5=180+d)
    - IV_Maturity_State: 'MATURE' | 'DEVELOPING' | 'IMMATURE' | 'MISSING' (mapped from IV_Maturity_Level)
    - IV_Source: 'Schwab' (Schwab-only IV via IVEngine)
    - IV_Rank: numerical (0-100) (from IVEngine rolling rank)
    - IV_Trend_7D: 'Rising' | 'Falling' | 'Stable' (from Discovery IV - Schwab)
    - IVHV_gap_30D: numerical (0-100) (from Discovery IV - Schwab)
    - Liquidity_Grade: 'Excellent' | 'Good' | 'Acceptable' | 'Thin' | 'Illiquid'
    - Signal_Strength: 'Strong Bullish' | 'Moderate Bearish' | 'Bidirectional' | 'NEUTRAL'
    - Scraper_Status: 'OK' | 'API_FAILURE' | 'PARTIAL_DATA' | 'STALE_DATA' | 'NOT_INVOKED'
    - Data_Completeness_Overall: 'Complete' | 'Partial' | 'Missing'
    - acceptance_reason: Human-readable explanation (from prior steps, will be overwritten by Block_Reason)
    
    Phase 1 (always available):
        - compression_tag: COMPRESSION | NORMAL | EXPANSION
        - gap_tag: NO_GAP | GAP_UP | GAP_DOWN
        - intraday_position_tag: NEAR_LOW | MID_RANGE | NEAR_HIGH
        - 52w_regime_tag: NEAR_52W_LOW | MID_RANGE | NEAR_52W_HIGH
        - momentum_tag: STRONG_DOWN_DAY | FLAT_DAY | NORMAL | STRONG_UP_DAY
        - entry_timing_context: EARLY_LONG | MODERATE | LATE_LONG | EARLY_SHORT | LATE_SHORT
        
    Phase 2 (optional):
        - execution_quality: EXCELLENT | GOOD | FAIR | POOR | UNKNOWN
        - balance_tag: BALANCED | MODERATE_IMBALANCE | IMBALANCED | UNKNOWN
        - dividend_risk: HIGH | MODERATE | LOW | UNKNOWN
        
OUTPUTS:
    - Execution_Status: READY | CONDITIONAL | BLOCKED
    - Block_Reason: Human-readable explanation for decision
    - confidence_band: HIGH | MEDIUM | LOW
    - directional_bias: BULLISH_STRONG | BULLISH_MODERATE | BEARISH_STRONG | BEARISH_MODERATE | NEUTRAL
    - structure_bias: TRENDING | RANGE_BOUND | BREAKOUT_SETUP | BREAKOUT_TRIGGERED | UNCLEAR
    - timing_quality: EXCELLENT | GOOD | FAIR | POOR | MODERATE
    - execution_adjustment: SIZE_UP | NORMAL | SIZE_DOWN | CAUTION
    
INTEGRATION:
    Called after Step 9B (contract fetching) to filter and prioritize contracts.
    
    df_step9b = fetch_and_select_contracts_schwab(df_step11, df_step9a)
    df_step12 = apply_acceptance_logic(df_step9b)
    df_ready = df_step12[df_step12['Execution_Status'] == 'READY'] # Updated to Execution_Status
"""

import pandas as pd
import numpy as np
import os
import uuid
from typing import Dict, Optional, Tuple
import logging
from .debug.debug_mode import get_debug_manager
from core.shared.data_layer.market_stress_detector import check_market_stress, get_halt_reason
from core.shared.data_layer.duckdb_utils import get_domain_connection, get_domain_write_connection, DbDomain
from scan_engine.feedback_calibration import get_feedback_calibration
from scan_engine.calendar_context import calendar_risk_flag

# Layer 1 pure helpers — extracted to scan_engine/scoring/
from .scoring.classifiers import (
    operating_mode as _operating_mode,
    dqs_confidence_band as _dqs_confidence_band,
    classify_strategy_type,
    assign_capital_bucket as _assign_capital_bucket,
)
from .scoring.bias_detectors import (
    detect_directional_bias,
    detect_structure_bias,
    evaluate_timing_quality,
)
from .scoring.income_gates import check_income_eligibility
from .scoring.filters import (
    REGIME_STRATEGY_MATRIX,
    lookup_regime_fit,
    filter_ready_contracts,
    sort_by_confidence,
)
from .scoring.thesis_quality import check_thesis_quality

logger = logging.getLogger(__name__)


# ============================================================
# R4.2–R4.5: Post-gate demotion sweep (runs AFTER per-row gate)
# ============================================================
# Extracted as a standalone function so pipeline.py can call it
# regardless of whether it uses apply_acceptance_logic() or the
# direct .apply(apply_execution_gate, ...) path.

def apply_post_gate_demotions(df_result: pd.DataFrame) -> pd.DataFrame:
    """
    Apply R4.2–R4.5 demotion gates on a DataFrame that already has
    Execution_Status assigned by the per-row execution gate.

    These catch structural issues that individual row evaluation cannot see
    (e.g., evaluator verdict overrides, DQS floor, regime conflicts).

    Modifies df_result in-place and returns it.
    """
    # ── R4.2: Evaluator Verdict Gate ──
    _eval_demoted = 0
    _ready_with_eval = df_result[
        (df_result['Execution_Status'] == 'READY') &
        df_result['Evaluation_Notes'].fillna('').str.contains('Fails', case=False, na=False)
    ].index if 'Evaluation_Notes' in df_result.columns else []

    for idx in _ready_with_eval:
        _eval_notes = str(df_result.at[idx, 'Evaluation_Notes'] or '')
        _orig_gate = str(df_result.at[idx, 'Gate_Reason'] or '')
        df_result.at[idx, 'Execution_Status'] = 'CONDITIONAL'
        df_result.at[idx, 'Gate_Reason'] = (
            f'R4.2: Evaluator verdict — {_eval_notes[:120]} '
            f'[original: {_orig_gate}]'
        )
        _eval_demoted += 1
    if _eval_demoted > 0:
        logger.info(f"[R4.2] Evaluator verdict gate: demoted {_eval_demoted} READY→CONDITIONAL")

    # ── R4.3: Effective DQS Floor ──
    _DQS_READY_FLOOR = 45.0
    _dqs_demoted = 0
    if 'DQS_Score' in df_result.columns:
        _ready_with_dqs = df_result[
            (df_result['Execution_Status'] == 'READY') &
            (df_result['DQS_Score'].fillna(0.0) < _DQS_READY_FLOOR)
        ].index
        for idx in _ready_with_dqs:
            _dqs_val = float(df_result.at[idx, 'DQS_Score'])
            _mult_val = float(df_result.at[idx, 'DQS_Combined_Multiplier']) if pd.notna(df_result.at[idx, 'DQS_Combined_Multiplier']) else 1.0
            _orig_gate = str(df_result.at[idx, 'Gate_Reason'] or '')
            df_result.at[idx, 'Execution_Status'] = 'CONDITIONAL'
            df_result.at[idx, 'Gate_Reason'] = (
                f'R4.3: DQS={_dqs_val:.0f} below floor {_DQS_READY_FLOOR:.0f} '
                f'(combined mult={_mult_val:.2f}) — quality eroded below READY threshold '
                f'[original: {_orig_gate}]'
            )
            _dqs_demoted += 1
        if _dqs_demoted > 0:
            logger.info(f"[R4.3] Effective DQS floor: demoted {_dqs_demoted} READY→CONDITIONAL")

    # ── R4.4: Income in Downtrend ──
    _income_trend_demoted = 0
    _ready_income = df_result[
        (df_result['Execution_Status'] == 'READY') &
        (df_result['Strategy_Type'].fillna('').str.upper() == 'INCOME') &
        (df_result['Market_Structure'].fillna('') == 'Downtrend')
    ].index if ('Strategy_Type' in df_result.columns and 'Market_Structure' in df_result.columns) else []

    for idx in _ready_income:
        _orig_gate = str(df_result.at[idx, 'Gate_Reason'] or '')
        df_result.at[idx, 'Execution_Status'] = 'CONDITIONAL'
        df_result.at[idx, 'Gate_Reason'] = (
            f'R4.4: Income strategy in Downtrend — selling premium against the trend '
            f'increases assignment risk (Murphy Ch.4) [original: {_orig_gate}]'
        )
        _income_trend_demoted += 1
    if _income_trend_demoted > 0:
        logger.info(f"[R4.4] Income in Downtrend: demoted {_income_trend_demoted} READY→CONDITIONAL")

    # ── R4.5: Oversold Chasing Gate ──
    _chase_demoted = 0
    _ready_bearish = df_result[
        (df_result['Execution_Status'] == 'READY') &
        (df_result['Trade_Bias'].fillna('').str.upper().str.contains('BEAR', na=False))
    ].index if 'Trade_Bias' in df_result.columns else []

    for idx in _ready_bearish:
        _rsi = pd.to_numeric(df_result.at[idx, 'RSI'] if 'RSI' in df_result.columns else None, errors='coerce')
        _bb = pd.to_numeric(df_result.at[idx, 'BB_Position'] if 'BB_Position' in df_result.columns else None, errors='coerce')
        _rsi_div = str(df_result.at[idx, 'RSI_Divergence'] if 'RSI_Divergence' in df_result.columns else '')

        if (pd.notna(_rsi) and _rsi < 30
                and pd.notna(_bb) and _bb < 15
                and 'bullish' in _rsi_div.lower()):
            _orig_gate = str(df_result.at[idx, 'Gate_Reason'] or '')
            df_result.at[idx, 'Execution_Status'] = 'CONDITIONAL'
            df_result.at[idx, 'Gate_Reason'] = (
                f'R4.5: Oversold chasing — RSI={_rsi:.0f}, BB={_bb:.0f}% with '
                f'bullish divergence signals mean-reversion risk (Murphy Ch.9) '
                f'[original: {_orig_gate}]'
            )
            _chase_demoted += 1
    if _chase_demoted > 0:
        logger.info(f"[R4.5] Oversold chasing: demoted {_chase_demoted} READY→CONDITIONAL")

    return df_result


# ============================================================
# PORTFOLIO GATE — underwater position suppressor
# ============================================================

def _check_portfolio_gate(ticker: str, strategy_name: str) -> dict:
    """
    Check whether an existing SAME-DIRECTION position on this ticker is deeply
    underwater.  When it is, READY should become AWAIT_CONFIRMATION with gate
    code R3.2.PORTFOLIO — McMillan Ch.4: never average into a losing directional
    long until the original thesis either recovers or is explicitly closed.

    Returns:
        {
            'blocks': bool,                # True → demote to AWAIT_CONFIRMATION
            'existing_symbol': str,        # Option symbol that is underwater
            'gl_pct': float,               # Total_GL_Decimal of that position
            'basis': float,                # Original cost paid
        }
    """
    result = {'blocks': False, 'existing_symbol': None, 'gl_pct': None, 'basis': None}

    # Determine direction from strategy name
    sn_lower = (strategy_name or '').lower()
    is_bullish = any(k in sn_lower for k in ('long call', 'leap call', 'call debit'))
    is_bearish = any(k in sn_lower for k in ('long put', 'leap put', 'put debit'))
    if not (is_bullish or is_bearish):
        return result  # Not a directional long — Portfolio_Gate only applies to directional longs

    try:
        con = get_domain_connection(DbDomain.MANAGEMENT, read_only=True)

        # Find option positions for this underlying that match direction
        # Symbol convention: e.g. APH260417C160 (Call=bullish, Put=bearish)
        direction_char = 'C' if is_bullish else 'P'
        rows = con.execute("""
            SELECT Symbol, Total_GL_Decimal, Basis, Last
            FROM entry_anchors
            WHERE Symbol LIKE ? AND Symbol LIKE ?
            ORDER BY Total_GL_Decimal ASC
        """, [f'{ticker}%', f'%{direction_char}%']).fetchall()
        con.close()

        if not rows:
            return result

        # Take the most underwater position
        worst_sym, worst_gl, worst_basis, worst_last = rows[0]

        # Gate triggers when unrealized loss exceeds 25% of premium paid
        _PORTFOLIO_GATE_THRESHOLD = -0.25
        if worst_gl is not None and float(worst_gl) < _PORTFOLIO_GATE_THRESHOLD:
            result.update({
                'blocks': True,
                'existing_symbol': worst_sym,
                'gl_pct': float(worst_gl),
                'basis': float(worst_basis) if worst_basis else None,
            })

    except Exception as e:
        logger.debug(f"[PortfolioGate] DB lookup failed for {ticker} ({e}) — skipping gate")

    return result


# ============================================================
# STRATEGY TYPE CLASSIFICATION + SIGNAL DETECTION + INCOME GATES
# Extracted to scan_engine/scoring/ (Layer 1 pure helpers).
# Imported at top of file. Original functions preserved as imports.
# ============================================================


# ============================================================
# DQS MULTIPLIER CHAIN — applied to all non-BLOCKED decisions
# ============================================================

def _apply_dqs_multiplier_chain(row: pd.Series, decision: Dict, iv_data_stale: bool = True) -> pd.Series:
    """
    Apply Calendar DQS, Signal Trajectory, and Multiplier Clamp to any decision.

    Modifies ``decision`` in place and returns the (potentially updated) ``row``.
    Safe to call from any gate path — reads all inputs from row/decision.
    """
    # ── Calendar DQS Multiplier ──────────────────────────────────────────────
    _strat_name = str(row.get('Strategy_Name', '') or row.get('Strategy', '') or '').upper().replace('_', ' ')
    _cal_flag, _cal_note = calendar_risk_flag(_strat_name)

    _CAL_BASE_EFFECT = {
        'HIGH_BLEED': -0.15,
        'ELEVATED_BLEED': -0.10,
        'PRE_HOLIDAY_EDGE': 0.08,
        'ADVANTAGEOUS': 0.05,
    }
    _cal_base = _CAL_BASE_EFFECT.get(_cal_flag, 0.0)
    _cal_dte_raw = pd.to_numeric(row.get('Actual_DTE') or row.get('DTE'), errors='coerce')
    _cal_dte = float(_cal_dte_raw) if pd.notna(_cal_dte_raw) and _cal_dte_raw > 0 else 0.0
    _cal_theta_factor = min(1.0, 45.0 / _cal_dte) if _cal_dte > 0 else 1.0
    _cal_dqs_multiplier = 1.0 + (_cal_base * _cal_theta_factor)

    if _cal_dqs_multiplier != 1.0:
        _pre_cal_dqs = row.get('DQS_Score')
        if pd.notna(_pre_cal_dqs):
            row = row.copy() if not isinstance(row, dict) else row.copy()
            row['DQS_Score'] = float(_pre_cal_dqs) * _cal_dqs_multiplier
            logger.debug(
                f"  [CalendarDQS] {_strat_name}: DQS {float(_pre_cal_dqs):.0f} "
                f"→ {float(row['DQS_Score']):.0f} (×{_cal_dqs_multiplier:.3f}, "
                f"{_cal_flag}, θ_factor={_cal_theta_factor:.2f}, DTE={_cal_dte:.0f})"
            )
    decision['Calendar_DQS_Multiplier'] = round(_cal_dqs_multiplier, 4)
    decision['Calendar_Theta_Factor'] = round(_cal_theta_factor, 2)
    decision['Calendar_Risk_Flag'] = _cal_flag
    decision['Calendar_Risk_Note'] = _cal_note

    # Calendar risk confidence cap (belt + suspenders alongside multiplier)
    if _cal_flag in ('HIGH_BLEED',) and decision.get('confidence_band') == 'HIGH':
        decision['confidence_band'] = 'MEDIUM'
        _old_gr = decision.get('Gate_Reason', '')
        decision['Gate_Reason'] = _old_gr + ' [calendar-capped: pre-holiday long premium entry]'

    # ── Signal Trajectory Multiplier ─────────────────────────────────────────
    _traj_mult = 1.0
    _traj_label = str(row.get('Signal_Trajectory') or 'UNKNOWN')
    _traj_raw = row.get('Trajectory_Multiplier')
    if _traj_raw is not None and pd.notna(_traj_raw):
        try:
            _traj_mult = float(_traj_raw)
        except (TypeError, ValueError):
            _traj_mult = 1.0

    if _traj_mult != 1.0:
        _pre_traj_dqs = row.get('DQS_Score')
        if pd.notna(_pre_traj_dqs):
            row = row.copy() if not isinstance(row, dict) else row.copy()
            row['DQS_Score'] = float(_pre_traj_dqs) * _traj_mult
            logger.debug(
                f"  [Trajectory] {row.get('Ticker')}: DQS {float(_pre_traj_dqs):.0f} "
                f"→ {float(row['DQS_Score']):.0f} (×{_traj_mult:.2f}, {_traj_label})"
            )
    decision['Signal_Trajectory'] = _traj_label
    decision['Trajectory_Multiplier'] = round(_traj_mult, 4)
    decision['Score_Acceleration'] = row.get('Score_Acceleration', 0.0)

    # ── IV Headwind Multiplier (long vega only) ───────────────────────────────
    # Natenberg: "If IV is high, look for negative vega spreads"
    # Passarelli: "Buy with IV in the lower third of its range"
    # Jabbour: "significant IV Crush" risk when buying at peak
    #
    # Macro-cluster attenuation (Sinclair: structural vol vs event premium):
    # When macro_density ≥ 2 (e.g., CPI + FOMC within 14d), IV elevation is
    # structural — crush is less likely because uncertainty persists across
    # sequential events.  Attenuate penalty, don't remove it: IV is still
    # expensive, but the floor is higher than post-earnings collapse.
    _iv_hw_mult = 1.0
    _iv_hw_note = ''
    _long_vega_strats = ('LONG CALL', 'LONG PUT', 'LONG STRADDLE', 'LONG STRANGLE',
                         'LONG CALL LEAP', 'LONG PUT LEAP')
    _short_vega_strats = ('CASH SECURED PUT', 'COVERED CALL', 'BUY-WRITE',
                          'SHORT PUT', 'SHORT CALL', 'IRON CONDOR')
    if _strat_name in _long_vega_strats:
        _ivr_raw = pd.to_numeric(row.get('IV_Rank_20D'), errors='coerce')
        _gap_raw = pd.to_numeric(row.get('IVHV_gap_30D'), errors='coerce')
        _surf    = str(row.get('Surface_Shape', '') or '').upper()
        _ivr = float(_ivr_raw) if pd.notna(_ivr_raw) else None
        _gap = float(_gap_raw) if pd.notna(_gap_raw) else 0.0

        # Macro density: structural vol regime detection
        # Use snapshot date from the row (not today) so tests are deterministic.
        _macro_cluster = False
        try:
            from config.macro_calendar import get_macro_proximity
            from datetime import date as _date_cls
            _snap_raw = None
            for _ts_col in ('snapshot_ts', 'Snapshot_TS', 'snapshot_date'):
                _v = row.get(_ts_col)
                if _v is not None and not (isinstance(_v, float) and pd.isna(_v)):
                    _snap_raw = _v
                    break
            _snap_date = pd.to_datetime(_snap_raw).date() if _snap_raw is not None and pd.notna(_snap_raw) else _date_cls.today()
            _macro_prox = get_macro_proximity(_snap_date)
            _macro_cluster = _macro_prox.macro_density >= 2
        except Exception:
            pass

        # Penalty: buying long vol at IV_Rank > 80 AND positive IV/HV gap
        # Attenuated when macro clustering creates a structural vol floor
        if _ivr is not None and _ivr > 80 and _gap > 5:
            if _macro_cluster:
                _iv_hw_mult *= 0.90
                _iv_hw_note = f'IV_Rank {_ivr:.0f} + gap +{_gap:.1f}% (expensive vol, macro cluster → attenuated)'
            else:
                _iv_hw_mult *= 0.85
                _iv_hw_note = f'IV_Rank {_ivr:.0f} + gap +{_gap:.1f}% (buying expensive vol)'
        # Additional penalty: INVERTED surface = short-term fear spike
        # Macro cluster attenuates: inversion may reflect event sequencing, not panic
        if _ivr is not None and _ivr > 80 and _surf == 'INVERTED':
            if _macro_cluster:
                _iv_hw_mult *= 0.95
                _iv_hw_note += ('; ' if _iv_hw_note else '') + 'INVERTED surface (macro cluster → attenuated)'
            else:
                _iv_hw_mult *= 0.90
                _iv_hw_note += ('; ' if _iv_hw_note else '') + 'INVERTED surface (fear premium spike)'
        # LEAP amplifier: LEAPs carry 2-3x vega — IV mean-reversion exposure amplified
        # Natenberg: "The vega of a LEAP makes IV rank the single most important entry criterion"
        if 'LEAP' in _strat_name and _ivr is not None and _ivr > 80:
            _iv_hw_mult *= 0.90
            _iv_hw_note += ('; ' if _iv_hw_note else '') + f'LEAP at IV_Rank {_ivr:.0f} (amplified vega risk)'

        # Inverted surface LEAP bonus: buying cheap back-month vol on inverted term structure.
        # Natenberg: inverted surface means front-month IV >> back-month IV. LEAP buyers purchase
        # the cheap end of the curve — structural edge when surface normalizes.
        # Narrowly scoped: only LEAP buyers, meaningful inversion, IV_Rank ≤ 60 (not expensive).
        if 'LEAP' in _strat_name and _surf == 'INVERTED' and _ivr is not None and _ivr <= 60:
            _iv_hw_mult *= 1.05
            _iv_hw_note += ('; ' if _iv_hw_note else '') + f'INVERTED surface LEAP bonus (back-month IV cheap, IV_Rank {_ivr:.0f})'

        if _iv_hw_mult != 1.0:
            _pre_hw_dqs = row.get('DQS_Score')
            if pd.notna(_pre_hw_dqs):
                row = row.copy() if not isinstance(row, dict) else row.copy()
                row['DQS_Score'] = float(_pre_hw_dqs) * _iv_hw_mult
                logger.debug(
                    f"  [IV_Headwind] {row.get('Ticker')}: DQS {float(_pre_hw_dqs):.0f} "
                    f"→ {float(row['DQS_Score']):.0f} (×{_iv_hw_mult:.2f}, {_iv_hw_note})"
                )
    elif _strat_name in _short_vega_strats:
        # Tailwind: selling vol at high IV is the sweet spot — no penalty (informational only)
        _ivr_raw = pd.to_numeric(row.get('IV_Rank_20D'), errors='coerce')
        _ivr = float(_ivr_raw) if pd.notna(_ivr_raw) else None
        if _ivr is not None and _ivr > 80:
            _iv_hw_note = f'IV_Rank {_ivr:.0f} — selling at premium (tailwind)'

    decision['IV_Headwind_Multiplier'] = round(_iv_hw_mult, 4)
    decision['IV_Headwind_Note'] = _iv_hw_note

    # ── Blind-Spot Multiplier (divergence, BB extremes, OBV conflict) ────────
    # Penalizes directional trades where chart signals contradict the thesis.
    # Annotation-only for Keltner squeeze (direction unknown until fired).
    _bs_mult = 1.0
    _bs_notes = []
    _is_bullish_strat = _strat_name in ('LONG CALL', 'LONG CALL LEAP')
    _is_bearish_strat = _strat_name in ('LONG PUT', 'LONG PUT LEAP')

    if _is_bullish_strat or _is_bearish_strat:
        _rsi_div = str(row.get('RSI_Divergence', '') or '')
        _macd_div = str(row.get('MACD_Divergence', '') or '')

        # Divergence opposing direction (Murphy Ch.10)
        _div_count = 0
        if _is_bullish_strat and _rsi_div == 'Bearish_Divergence':
            _div_count += 1
        if _is_bullish_strat and _macd_div == 'Bearish_Divergence':
            _div_count += 1
        if _is_bearish_strat and _rsi_div == 'Bullish_Divergence':
            _div_count += 1
        if _is_bearish_strat and _macd_div == 'Bullish_Divergence':
            _div_count += 1

        if _div_count >= 2:
            _bs_mult *= 0.90
            _bs_notes.append(f'Double divergence opposes direction (Murphy Ch.10: serious warning)')
        elif _div_count == 1:
            _bs_mult *= 0.95
            _bs_notes.append(f'Divergence opposes direction (Murphy Ch.10)')

        # BB extremes on directional entries (Murphy: overextended band touch)
        # Trend-adjusted: ADX ≥ 40 = band-walking expected (Bollinger), skip penalty
        #                 ADX 30-39 = trending, raise threshold to 90/10
        #                 ADX < 30 = ranging, standard threshold 85/15
        _bb = pd.to_numeric(row.get('BB_Position'), errors='coerce')
        _adx_val = pd.to_numeric(row.get('ADX'), errors='coerce')
        if pd.notna(_bb):
            _bb_skip = pd.notna(_adx_val) and _adx_val >= 40  # Strong trend: band-walk
            _bb_bull_thresh = 90 if (pd.notna(_adx_val) and _adx_val >= 30) else 85
            _bb_bear_thresh = 10 if (pd.notna(_adx_val) and _adx_val >= 30) else 15

            if _bb_skip:
                if (_is_bullish_strat and _bb > 90) or (_is_bearish_strat and _bb < 10):
                    _bs_notes.append(f'BB={_bb:.0f}% in strong trend (ADX={_adx_val:.0f}) — band-walk, annotation only')
            elif _is_bullish_strat and _bb > _bb_bull_thresh:
                _bs_mult *= 0.95
                _bs_notes.append(f'BB={_bb:.0f}% — buying calls at upper band extreme (thresh={_bb_bull_thresh})')
            elif _is_bearish_strat and _bb < _bb_bear_thresh:
                _bs_mult *= 0.95
                _bs_notes.append(f'BB={_bb:.0f}% — buying puts at lower band extreme (thresh={_bb_bear_thresh})')

        # OBV slope conflicts with direction (Murphy Ch.7: smart money flow)
        _obv = pd.to_numeric(row.get('OBV_Slope'), errors='coerce')
        if pd.notna(_obv):
            if _is_bullish_strat and _obv < -15:
                _bs_mult *= 0.95
                _bs_notes.append(f'OBV={_obv:+.0f}% distribution contradicts bullish (Murphy Ch.7)')
            elif _is_bearish_strat and _obv > 15:
                _bs_mult *= 0.95
                _bs_notes.append(f'OBV={_obv:+.0f}% accumulation contradicts bearish (Murphy Ch.7)')

        # Structure conflict: directional trade fighting the swing structure (Murphy Ch.4)
        # Long Call on Downtrend or Long Put on Uptrend = buying against prevailing HH/HL or LH/LL
        _mkt_struct = str(row.get('Market_Structure', '') or '').strip()
        if (_is_bullish_strat and _mkt_struct == 'Downtrend') or \
           (_is_bearish_strat and _mkt_struct == 'Uptrend'):
            _bs_mult *= 0.90
            _direction = 'calls' if _is_bullish_strat else 'puts'
            _bs_notes.append(f'Structure conflict: buying {_direction} against {_mkt_struct} swings (Murphy Ch.4)')

        # Weekly trend conflict (Murphy: "weekly signals filter daily noise")
        # Applies to ALL directionals — weekly timeframe disagreeing with daily
        # weakens conviction. LEAPs get heavier penalty (longer horizon = weekly matters more).
        _is_leap = 'LEAP' in _strat_name
        _weekly_bias = str(row.get('Weekly_Trend_Bias', '') or '').strip()
        if _weekly_bias == 'CONFLICTING':
            if _is_leap:
                _bs_mult *= 0.95
                _bs_notes.append('Weekly trend conflicts with LEAP direction (Murphy: weekly filters daily)')
            else:
                _bs_mult *= 0.95
                _bs_notes.append('Weekly trend CONFLICTING — higher timeframe does not confirm directional thesis (Murphy)')

        # ADX conviction gate (Murphy 0.764: "trade markets with highest trend ratings")
        # Short-dated directionals in flat markets burn theta with no price movement.
        # ADX < 15: no trend exists (hard penalty). ADX 15–19: trend unconfirmed (soft penalty).
        if not _is_leap:  # LEAPs have longer horizon, less sensitive to short-term ADX
            if pd.notna(_adx_val) and _adx_val < 15:
                _bs_mult *= 0.90
                _bs_notes.append(f'ADX={_adx_val:.0f} — no trend conviction for directional trade (Murphy: nontrending)')
            elif pd.notna(_adx_val) and _adx_val < 20:
                _bs_mult *= 0.95
                _bs_notes.append(f'ADX={_adx_val:.0f} — weak trend, directional risk elevated (Murphy Ch.14)')

        # Earnings IV crush on short-dated directionals (Augen 0.754)
        # Buying non-LEAP options near earnings = paying inflated IV that collapses post-announcement
        # Track record context: beat_rate, avg_iv_crush, avg_move_ratio from earnings_stats
        if not _is_leap:
            _dte_earn = pd.to_numeric(row.get('days_to_earnings'), errors='coerce')
            if pd.notna(_dte_earn) and _dte_earn <= 5 and _dte_earn >= 0:
                _bs_mult *= 0.90
                _earn_ctx_parts = [f'Earnings in {int(_dte_earn)}d — IV crush risk on short-dated directional (Augen)']

                # Enrich with track-record context from earnings_stats replay data
                _e_move_ratio = pd.to_numeric(row.get('Earnings_Move_Ratio'), errors='coerce')
                _e_beat_rate = pd.to_numeric(row.get('Earnings_Beat_Rate'), errors='coerce')
                _e_avg_crush = pd.to_numeric(row.get('Earnings_Avg_IV_Crush'), errors='coerce')
                _e_consec_beats = pd.to_numeric(row.get('Earnings_Consecutive_Beats'), errors='coerce')
                _e_consec_misses = pd.to_numeric(row.get('Earnings_Consecutive_Misses'), errors='coerce')

                _track_parts = []
                if pd.notna(_e_beat_rate):
                    _track_parts.append(f'beat_rate={_e_beat_rate:.0f}%')
                if pd.notna(_e_avg_crush):
                    # DB stores as decimal fraction (0.30 = 30%); display as whole %
                    _crush_display = _e_avg_crush * 100 if _e_avg_crush <= 1.0 else _e_avg_crush
                    _track_parts.append(f'avg_crush={_crush_display:.0f}%')
                if pd.notna(_e_consec_beats) and _e_consec_beats >= 3:
                    _track_parts.append(f'{int(_e_consec_beats)} consecutive beats')
                if pd.notna(_e_consec_misses) and _e_consec_misses >= 2:
                    _track_parts.append(f'{int(_e_consec_misses)} consecutive misses')

                if _track_parts:
                    _earn_ctx_parts.append(f'Track record: {", ".join(_track_parts)}')

                # Stacking: severe IV crush history + market overpricing = extra penalty
                if pd.notna(_e_move_ratio) and _e_move_ratio < 0.6:
                    _bs_mult *= 0.95
                    _earn_ctx_parts.append(f'Move_Ratio={_e_move_ratio:.2f} — market overprices moves (×0.95 stacked)')
                elif pd.notna(_e_move_ratio) and _e_move_ratio > 1.2:
                    _earn_ctx_parts.append(f'Move_Ratio={_e_move_ratio:.2f} — stock moves more than implied (favorable)')

                _bs_notes.append('; '.join(_earn_ctx_parts))

        # Keltner squeeze — annotation only (direction unknown until fired)
        _squeeze_on = row.get('Keltner_Squeeze_On', False)
        _squeeze_fired = row.get('Keltner_Squeeze_Fired', False)
        if _squeeze_on and not _squeeze_fired:
            _bs_notes.append('Keltner squeeze active — breakout direction unconfirmed (Raschke)')

    if _bs_mult != 1.0:
        _pre_bs_dqs = row.get('DQS_Score')
        if pd.notna(_pre_bs_dqs):
            row = row.copy() if not isinstance(row, dict) else row.copy()
            row['DQS_Score'] = float(_pre_bs_dqs) * _bs_mult
            logger.debug(
                f"  [BlindSpot] {row.get('Ticker')}: DQS "
                f"→ {float(row['DQS_Score']):.0f} (×{_bs_mult:.2f})"
            )

    decision['Blind_Spot_Multiplier'] = round(_bs_mult, 4)
    decision['Blind_Spot_Notes'] = '; '.join(_bs_notes) if _bs_notes else ''

    # ── Feedback calibration (read-only from closed-trade outcomes) ───────────
    _fb_strategy = _strat_name
    _timing_ctx = str(row.get('entry_timing_context', '') or '').upper()
    _fb_multiplier, _fb_meta = get_feedback_calibration(
        strategy=_fb_strategy,
        entry_timing_context=_timing_ctx,
    )
    if _fb_multiplier != 1.0:
        _pre_fb_dqs = row.get('DQS_Score')
        if pd.notna(_pre_fb_dqs):
            row = row.copy() if not isinstance(row, dict) else row.copy()
            row['DQS_Score'] = float(_pre_fb_dqs) * _fb_multiplier
    decision['Calibrated_Confidence'] = _fb_meta.get('win_rate')
    decision['Feedback_Win_Rate'] = _fb_meta.get('win_rate')
    decision['Feedback_Sample_N'] = _fb_meta.get('sample_n', 0)
    decision['Feedback_Action'] = _fb_meta.get('suggested_action', 'INSUFFICIENT_SAMPLE')
    decision['Feedback_Note'] = _fb_meta.get('note', '')

    # ── Behavioral Memory Multiplier (YTD scan arc + v2 intelligence) ────────
    # Base: score-driven (Murphy Ch.1: "the trend is your friend").
    # v2 overlays: contradiction flags, data maturity, event proximity.
    # Sinclair (Volatility Trading): high contradiction = hold to higher standard.
    # Chan/Harris: thin data = don't trust the signal.
    _bm_mult = 1.0
    _bm_score = row.get('Behavioral_Score')
    _bm_note_parts = []
    if _bm_score is not None and pd.notna(_bm_score):
        _bm_score = float(_bm_score)
        if _bm_score >= 70:
            _bm_mult = 1.05   # strong behavioral arc — boost
        elif _bm_score < 40:
            _bm_mult = 0.90   # hostile behavioral arc — stronger penalty

        # v2: Contradiction dampening — if market evidence conflicts with
        # portfolio evidence, reduce trust in the base score.
        _bm_contras = str(row.get('Contradiction_Flags', '') or '')
        if _bm_contras and _bm_contras not in ('', 'nan'):
            _contra_count = len([c for c in _bm_contras.split(',') if c.strip()])
            if _contra_count >= 2:
                _bm_mult *= 0.95  # multiple contradictions — significant concern
                _bm_note_parts.append(f'{_contra_count} contradictions')
            elif _contra_count == 1:
                _bm_mult *= 0.97  # single contradiction — minor concern
                _bm_note_parts.append('1 contradiction')

        # v2: Data maturity guard — don't boost thin-data tickers.
        # A NEW_TICKER with score 75 shouldn't get the same boost as a
        # MATURE ticker with score 75.
        _bm_maturity = str(row.get('Data_Maturity', '') or '')
        if _bm_maturity == 'NEW_TICKER' and _bm_mult > 1.0:
            _bm_mult = 1.0  # suppress boost for new tickers
            _bm_note_parts.append('new ticker — boost suppressed')
        elif _bm_maturity == 'DEVELOPING' and _bm_mult > 1.0:
            _bm_mult = 1.0 + (_bm_mult - 1.0) * 0.5  # halve the boost
            _bm_note_parts.append('developing — boost halved')

        # v2: Worst-event proximity check — if ticker's worst macro event
        # is within 3 days, apply caution penalty.
        _bm_worst_evt = str(row.get('Worst_Event_Type', '') or '')
        if _bm_worst_evt and _bm_worst_evt not in ('', 'nan'):
            try:
                from config.macro_calendar import get_macro_proximity
                from datetime import date
                _today = date.today()
                _macro_prox = get_macro_proximity(_today)
                if _macro_prox.events_within_5d:
                    for _evt in _macro_prox.events_within_5d:
                        if _evt.event_type == _bm_worst_evt:
                            _days_to = (_evt.event_date - _today).days
                            if 0 <= _days_to <= 3:
                                _bm_mult *= 0.97
                                _bm_note_parts.append(
                                    f'{_bm_worst_evt} in {_days_to}d (worst event)')
                                break
            except Exception:
                pass

    if _bm_mult != 1.0:
        _pre_bm_dqs = row.get('DQS_Score')
        if pd.notna(_pre_bm_dqs):
            row = row.copy() if not isinstance(row, dict) else row.copy()
            row['DQS_Score'] = float(_pre_bm_dqs) * _bm_mult
    decision['Behavioral_Multiplier'] = round(_bm_mult, 4)
    decision['Behavioral_Score'] = round(float(_bm_score), 0) if _bm_score is not None and pd.notna(_bm_score) else 50
    decision['Behavioral_Note'] = '; '.join(_bm_note_parts) if _bm_note_parts else ''

    # ── Market Regime Multiplier ─────────────────────────────────────────────
    # Modest DQS penalty for elevated regimes. Non-blocking: influences ranking,
    # never vetoes. CRISIS ×0.85, RISK_OFF ×0.90, CAUTIOUS ×0.95, else ×1.00.
    _mkt_regime = str(row.get('Market_Regime', 'UNKNOWN') or 'UNKNOWN')
    _MKT_REGIME_MULT = {
        'CRISIS': 0.85, 'RISK_OFF': 0.90, 'CAUTIOUS': 0.95,
        'NORMAL': 1.0, 'RISK_ON': 1.0, 'UNKNOWN': 1.0,
    }
    _mkt_mult = _MKT_REGIME_MULT.get(_mkt_regime, 1.0)

    # Regime-strategy annotations (non-blocking, informational)
    _regime_notes = []
    _mkt_term = str(row.get('Market_Term_Structure', '') or '')
    _mkt_breadth = str(row.get('Market_Breadth_State', '') or '')
    _is_income = _strat_name in ('COVERED CALL', 'BUY-WRITE', 'CASH SECURED PUT',
                                  'SHORT PUT', 'SHORT CALL', 'IRON CONDOR', 'PMCC')
    _is_directional_long = _strat_name in ('LONG CALL', 'LONG PUT',
                                            'LONG CALL LEAP', 'LONG PUT LEAP')

    if _mkt_term == 'BACKWARDATION' and _is_income:
        _regime_notes.append('CAUTION: Backwardation — front-month IV elevated, rolls expensive')
    if _mkt_breadth == 'NARROW' and _is_directional_long:
        _regime_notes.append('CAUTION: Narrow breadth — directional conviction reduced')
    if _mkt_regime == 'CRISIS':
        _regime_notes.append('CAUTION: CRISIS regime — elevated risk across all strategies')

    if _mkt_mult != 1.0:
        _pre_mkt_dqs = row.get('DQS_Score')
        if pd.notna(_pre_mkt_dqs):
            row = row.copy() if not isinstance(row, dict) else row.copy()
            row['DQS_Score'] = float(_pre_mkt_dqs) * _mkt_mult
            logger.debug(
                f"  [MarketRegime] {row.get('Ticker')}: DQS {float(_pre_mkt_dqs):.0f} "
                f"→ {float(row['DQS_Score']):.0f} (×{_mkt_mult:.2f}, {_mkt_regime})"
            )
    decision['Market_Regime_Multiplier'] = round(_mkt_mult, 4)
    decision['Regime_Strategy_Note'] = '; '.join(_regime_notes) if _regime_notes else ''

    # ── CBOE SKEW Multiplier ───────────────────────────────────────────────
    # Direct strategy-aware SKEW signal. Elevated SKEW = market pricing tail
    # risk via OTM puts. Penalises long-vega (buying into expensive tails),
    # rewards income sellers (collecting richer premium).
    # Complements the composite regime multiplier which only weighs SKEW at 5%.
    _skew_mult = 1.0
    _skew_val = row.get('CBOE_SKEW')
    _skew_note = ''
    if _skew_val is not None and pd.notna(_skew_val):
        _skew_f = float(_skew_val)
        from config.indicator_settings import MARKET_REGIME_THRESHOLDS as _SKEW_THRESHOLDS
        _skew_long_thresh = _SKEW_THRESHOLDS.get('SKEW_DQS_LONG_PENALTY_THRESHOLD', 140)
        _skew_long_mild = _SKEW_THRESHOLDS.get('SKEW_DQS_LONG_PENALTY_MILD', 0.97)
        _skew_long_severe = _SKEW_THRESHOLDS.get('SKEW_DQS_LONG_PENALTY_SEVERE', 0.93)
        _skew_income_thresh = _SKEW_THRESHOLDS.get('SKEW_DQS_INCOME_BOOST_THRESHOLD', 135)
        _skew_income_boost = _SKEW_THRESHOLDS.get('SKEW_DQS_INCOME_BOOST', 1.03)

        if _is_directional_long and _skew_f >= _skew_long_thresh:
            if _skew_f >= 150:
                _skew_mult = _skew_long_severe
                _skew_note = f'SKEW {_skew_f:.0f} ≥ 150 — elevated tail risk, long-vega penalised ×{_skew_long_severe}'
            else:
                _skew_mult = _skew_long_mild
                _skew_note = f'SKEW {_skew_f:.0f} ≥ {_skew_long_thresh} — mild tail risk, long-vega ×{_skew_long_mild}'
        elif _is_income and _skew_f >= _skew_income_thresh:
            _skew_mult = _skew_income_boost
            _skew_note = f'SKEW {_skew_f:.0f} ≥ {_skew_income_thresh} — richer premium for income ×{_skew_income_boost}'

        if _skew_mult != 1.0:
            _pre_skew_dqs = row.get('DQS_Score')
            if pd.notna(_pre_skew_dqs):
                row = row.copy() if not isinstance(row, dict) else row.copy()
                row['DQS_Score'] = float(_pre_skew_dqs) * _skew_mult
                logger.debug(
                    f"  [SKEW] {row.get('Ticker')}: DQS {float(_pre_skew_dqs):.0f} "
                    f"→ {float(row['DQS_Score']):.0f} (×{_skew_mult:.2f}, SKEW={_skew_f:.0f})"
                )
    decision['SKEW_Multiplier'] = round(_skew_mult, 4)
    decision['SKEW_Note'] = _skew_note

    # ── Multiplier Clamp ─────────────────────────────────────────────────────
    _raw_dqs = row.get('DQS_Score')
    _timing_mult = 0.85 if (_timing_ctx in ('LATE_SHORT', 'LATE_LONG') and pd.notna(_raw_dqs)) else 1.0
    _staleness_mult = 0.85 if iv_data_stale else 1.0
    _combined_mult = _timing_mult * _staleness_mult * _fb_multiplier * _cal_dqs_multiplier * _traj_mult * _iv_hw_mult * _bs_mult * _bm_mult * _mkt_mult * _skew_mult
    _MULT_FLOOR = 0.40
    _MULT_CEILING = 1.35
    _was_clamped = False

    if _combined_mult < _MULT_FLOOR or _combined_mult > _MULT_CEILING:
        _clamped_mult = max(_MULT_FLOOR, min(_MULT_CEILING, _combined_mult))
        _was_clamped = True
        _combined_mult = _clamped_mult

    decision['DQS_Combined_Multiplier'] = round(_combined_mult, 4)
    decision['DQS_Multiplier_Clamped'] = _was_clamped

    return row


# ============================================================
# EXECUTION GATE - FINAL DECISION LOGIC
# ============================================================

def apply_execution_gate(
    row: pd.Series,
    strategy_type: str,
    iv_maturity_state: str,
    iv_source: str,
    iv_rank: float,
    iv_trend_7d: str,
    ivhv_gap_30d: float,
    liquidity_grade: str,
    signal_strength: str,
    scraper_status: str,
    data_completeness_overall: str,
    compression: str,
    regime_52w: str,
    momentum: str,
    gap: str,
    timing: str,
    directional_bias: str,
    structure_bias: str,
    timing_quality: str,
    actual_dte: float,
    strategy_name: str,
    exec_quality: str,
    balance: str,
    div_risk: str,
    history_depth_ok: bool,
    iv_data_stale: bool,
    regime_confidence: float,
    is_initial_pass: bool = False,
    iv_maturity_level: int = 1,
) -> Dict:
    """
    Execution Gate: Converts enriched data state into a final execution decision.
    Applies deterministic rules to assign Execution_Status (READY, CONDITIONAL, BLOCKED)
    and a corresponding Gate_Reason.

    Uses IV_Maturity_Level (1-5) from IVEngine instead of Fidelity maturity.
    """

    decision = {
        'Execution_Status': 'BLOCKED',
        'Gate_Reason': 'Default: No rules met',
        'confidence_band': 'LOW',
        'directional_bias': directional_bias,
        'structure_bias': structure_bias,
        'timing_quality': timing_quality,
        'execution_adjustment': 'AVOID_SIZE',
    }

    # Normalize strategy_type to uppercase — step6 uses 'Directional', we need 'DIRECTIONAL'
    strategy_type = strategy_type.upper() if isinstance(strategy_type, str) else strategy_type

    logger.info(f"[GATE_ENTRY] {row.get('Ticker', 'N/A')} {row.get('Strategy_Name', 'N/A')} - Execution gate entered (Pass: {'initial' if is_initial_pass else 'final'})")
    logger.debug(f"--- apply_execution_gate for {row.get('Ticker', 'N/A')} {row.get('Strategy_Name', 'N/A')} ---")
    logger.debug(f"  Strategy Type: '{strategy_type}', IV Maturity Level: {iv_maturity_level}, Liquidity: {liquidity_grade}")
    logger.debug(f"  Data Completeness: {data_completeness_overall}, IV Maturity State: {iv_maturity_state}")

    # --- Initial Pass Rules ---
    if is_initial_pass:
        # R0.1: Critical Data Missing
        if data_completeness_overall == 'Missing':
            decision.update({
                'Execution_Status': 'BLOCKED',
                'Gate_Reason': 'R0.1: Critical non-IV data missing (e.g., price, basic Greeks)',
                'confidence_band': 'LOW',
                'execution_adjustment': 'AVOID_SIZE'
            })
            return decision

        # R0.2: Illiquid Contract
        if liquidity_grade == 'Illiquid':
            decision.update({
                'Execution_Status': 'BLOCKED',
                'Gate_Reason': f'R0.2: Illiquid contract ({liquidity_grade})',
                'confidence_band': 'LOW',
                'execution_adjustment': 'AVOID_SIZE'
            })
            return decision

        # R0.CC: Covered Call requires stock ownership — system has no portfolio tracker,
        # so it can never auto-confirm ownership. Route to informational panel only.
        if strategy_name == 'Covered Call':
            decision.update({
                'Execution_Status': 'CONDITIONAL',
                'Gate_Reason': 'R0.CC: Covered Call requires existing stock ownership — confirm in CC Opportunities panel',
                'confidence_band': 'LOW',
                'execution_adjustment': 'OWNERSHIP_REQUIRED',
            })
            return decision

        # R0.3: Income strategies need a demonstrated volatility edge.
        # When IV history is immature (Level 1-3), IV_Rank and IVHV_gap are unavailable —
        # selling premium without vol context violates Natenberg Ch.6 and Sinclair Ch.2.
        # DOCTRINE DECISION: income is explicitly deferred (not silently absent) until
        # IV_Maturity_Level >= 4. No income fallback at lower levels — McMillan and
        # Natenberg both require knowing whether you're selling rich or cheap premium.
        if strategy_type == 'INCOME':
            eligible, elig_reason = check_income_eligibility(row, actual_dte)
            if not eligible:
                _op_mode = _operating_mode(iv_maturity_level)
                _income_note = (
                    f'INCOME_DEFERRED_{_op_mode.split("(")[0].strip()}'
                    if iv_maturity_level < 4
                    else 'INSUFFICIENT_VOL_EDGE'
                )
                decision.update({
                    'Execution_Status': 'AWAIT_CONFIRMATION',
                    'Gate_Reason': (
                        f'R0.3: Income strategy lacks volatility edge — {elig_reason}. '
                        f'Operating mode: {_op_mode}. '
                        f'Doctrine: income requires IV_Maturity_Level >= 4 (currently {iv_maturity_level}). '
                        f'Natenberg Ch.6 / Sinclair Ch.2: do not sell premium without vol context.'
                    ),
                    'confidence_band': 'LOW',
                    'Operating_Mode': _op_mode,
                    'execution_adjustment': 'CAUTION',
                    'Income_Defer_Reason': _income_note,
                })
                logger.info(f"[ESCALATION_RULE] R0.3 triggered for {row.get('Ticker', 'N/A')} - {elig_reason} [{_income_note}]")
                return decision

        # R0.4: Directional with sufficient IV maturity
        if strategy_type == 'DIRECTIONAL' and iv_maturity_level >= 2:
            decision.update({
                'Execution_Status': 'AWAIT_CONFIRMATION',
                'Gate_Reason': f'R0.4: Directional strategy with IV Maturity Level {iv_maturity_level}, awaiting final gate',
                'confidence_band': 'LOW',
                'execution_adjustment': 'NORMAL',
            })
            logger.info(f"[ESCALATION_RULE] R0.4 triggered for {row.get('Ticker', 'N/A')} - DIRECTIONAL + Level {iv_maturity_level}")
            return decision

        # R0.5: Default initial pass
        decision.update({
            'Execution_Status': 'AWAIT_CONFIRMATION',
            'Gate_Reason': f'R0.5: Passed initial gates (IV Maturity Level: {iv_maturity_level}), awaiting final decision',
            'confidence_band': 'LOW',
            'execution_adjustment': 'CAUTION',
        })
        return decision

    # --- Final Pass Rules ---

    # R1.1: Critical Data Missing
    if data_completeness_overall == 'Missing':
        decision.update({
            'Execution_Status': 'BLOCKED',
            'Gate_Reason': 'R1.1: Critical non-IV data missing (e.g., price, basic Greeks)',
            'confidence_band': 'LOW',
            'execution_adjustment': 'AVOID_SIZE'
        })
        return decision

    # R1.2: Illiquid Contract
    if liquidity_grade == 'Illiquid':
        decision.update({
            'Execution_Status': 'BLOCKED',
            'Gate_Reason': f'R1.2: Illiquid contract ({liquidity_grade})',
            'confidence_band': 'LOW',
            'execution_adjustment': 'AVOID_SIZE'
        })
        return decision

    # R1.4: Income lacks volatility edge
    if strategy_type == 'INCOME':
        eligible, elig_reason = check_income_eligibility(row, actual_dte)
        if not eligible:
            decision.update({
                'Execution_Status': 'BLOCKED',
                'Gate_Reason': f'R1.4: Income strategy lacks volatility edge — {elig_reason}',
                'confidence_band': 'LOW',
                'execution_adjustment': 'AVOID_SIZE'
            })
            return decision

    # R1.5: Directional with very immature IV — note only, not a block.
    # Natenberg Ch.3 / Passarelli: directional thesis is chart + delta driven.
    # No minimum IV history required for Long Call/Put entry. IV context improves
    # edge but absence of history does not invalidate the trade.
    # (Gate removed — handled as confidence_band downgrade in R3.2)

    # R1.6: Long-term directional (LEAP) with immature IV — downgrade confidence only.
    # Hull / Passarelli Ch.8: LEAP selection is structural (delta, DTE fit) not IV-rank
    # dependent. IV rank improves timing but is not a prerequisite for LEAP entry.
    # Low maturity → confidence_band = LOW in R3.2 rather than outright block.

    # --- Rule Set 2: CONDITIONAL ---

    # R2.2: Directional with low maturity, no relative IV claim
    if strategy_type == 'DIRECTIONAL' and iv_maturity_level < 2 and pd.isna(iv_rank):
        decision.update({
            'Execution_Status': 'CONDITIONAL',
            'Gate_Reason': f'R2.2: Directional strategy with IV Maturity Level {iv_maturity_level} (no relative claim made)',
            'confidence_band': 'LOW',
            'execution_adjustment': 'CAUTION'
        })
        return decision

    # R2.2c: Long options (vol-buying) with very immature IV — CONDITIONAL until IMMATURE.
    # Long Call/Put BUY volatility: entry without any IV_Rank means you can't
    # assess whether vol is cheap or expensive. At Level 2 (20d+), IV_Rank_20D
    # exists and provides a basic ranking signal.
    # Lowered from Level 3 (60d) → Level 2 (20d) per Mar-2026 audit:
    # Jabbour & Budwick (0.769): "IV is an initial screen, not the only gate."
    # Given (0.717): "Trend judgment matters more than IV maturity for directionals."
    # Scan should surface all eligible strategies; decision layer ranks them.
    if strategy_type == 'DIRECTIONAL' and iv_maturity_level < 2:
        _sn_r22c = str(row.get('Strategy_Name', '') or '').lower()
        if any(k in _sn_r22c for k in ('long call', 'long put')):
            decision.update({
                'Execution_Status': 'CONDITIONAL',
                'Gate_Reason': (
                    f'R2.2c: Long option ({row.get("Strategy_Name")}) requires '
                    f'IMMATURE IV (Level 2+, 20d history). '
                    f'Current: Level {iv_maturity_level}. '
                    'No IV_Rank available — cannot assess vol cheapness.'
                ),
                'confidence_band': 'LOW',
                'execution_adjustment': 'CAUTION'
            })
            return decision

    # R2.2d: Deep ITM LEAP guard — block when essentially synthetic stock.
    # A deep ITM LEAP (|delta| > 0.70, time value < 5% of premium) is capital-inefficient:
    # you're paying $20,000+ of intrinsic value for stock-like exposure with no optionality.
    # Hull Ch.10: LEAPS provide leverage through time value, not intrinsic.
    # If time_value / premium < 5%, the option is just expensive synthetic stock.
    _sn_r22d = str(row.get('Strategy_Name', '') or '').upper()
    if 'LEAP' in _sn_r22d:
        try:
            _delta_raw = pd.to_numeric(row.get('Delta'), errors='coerce')
            _delta_abs = abs(float(_delta_raw)) if pd.notna(_delta_raw) else 0.0
            _strike_r = float(row.get('Strike', 0) or 0)
            _ul_price = float(row.get('UL_Price', 0) or row.get('Stock_Price', 0) or 0)
            _mid_r22d = float(row.get('Mid_Price', 0) or 0)

            if _delta_abs > 0.70 and _strike_r > 0 and _ul_price > 0 and _mid_r22d > 0:
                # Compute intrinsic value
                if 'PUT' in _sn_r22d:
                    _intrinsic = max(0.0, _strike_r - _ul_price)
                else:
                    _intrinsic = max(0.0, _ul_price - _strike_r)
                _time_value = _mid_r22d - _intrinsic
                _tv_pct = _time_value / _mid_r22d if _mid_r22d > 0 else 0.0

                if _tv_pct < 0.05 and _intrinsic > 0:
                    decision.update({
                        'Execution_Status': 'CONDITIONAL',
                        'Gate_Reason': (
                            f'R2.2d: Deep ITM LEAP — |delta|={_delta_abs:.2f}, '
                            f'time value ${_time_value:.2f} ({_tv_pct:.1%} of premium). '
                            f'Capital ${_mid_r22d*100:,.0f} buys synthetic stock, not optionality. '
                            f'Hull Ch.10: use ATM/OTM strike for leverage.'
                        ),
                        'confidence_band': 'LOW',
                        'execution_adjustment': 'WRONG_STRIKE',
                    })
                    return decision
        except (TypeError, ValueError):
            pass

    # R2.2b: Critically low absolute OI — exit liquidity trap
    # Natenberg Ch.9 / Passarelli: spread-to-premium ratio alone is insufficient when OI is
    # critically low. With OI < 10, you may be the entire market on exit, especially for
    # high-premium options where each contract represents significant capital.
    # Hard-block when OI < 10 AND premium > $5 (non-trivial capital at risk with no exit depth).
    try:
        _oi_val  = int(float(row.get('Open_Interest', 0) or 0))
        _mid_val = float(row.get('Mid_Price', 0) or 0)
        if _oi_val < 10 and _mid_val > 5.0:
            decision.update({
                'Execution_Status': 'BLOCKED',
                'Gate_Reason': (
                    f'R2.2b: OI={_oi_val} contracts — exit liquidity trap. '
                    f'Natenberg: cannot exit ${_mid_val:.2f} premium option with <10 OI without moving the market.'
                ),
                'confidence_band': 'LOW',
                'execution_adjustment': 'AVOID_SIZE'
            })
            return decision
    except (TypeError, ValueError):
        pass

    # R2.3: Acceptable/Thin Liquidity — evaluated by spread-cost-to-premium ratio
    # RAG: Natenberg (<5% institutional liquid), Sinclair (5-15% retail normal),
    #      Cohen (income sellers need excellent liq for rolling), Passarelli (each leg justifies itself)
    if liquidity_grade in ['Acceptable', 'Thin']:
        try:
            bid_v = float(row.get('Bid', 0) or 0)
            ask_v = float(row.get('Ask', 0) or 0)
            mid_v = float(row.get('Mid_Price', 0) or 0)
            spread_cost_ratio = (ask_v - bid_v) / mid_v if mid_v > 0 else 1.0
        except (TypeError, ValueError, ZeroDivisionError):
            spread_cost_ratio = 1.0

        # R2.3-OI: Hard OI floor scaled by contract dollar size.
        # OI=11 on a $20,000 contract = no real market depth regardless of spread ratio.
        # Minimum: OI ≥ 50 for contracts ≤ $5,000; OI ≥ 100 for contracts > $5,000.
        # LEAP exception (DTE ≥ 270): OI ≥ 25 flat. LEAPs structurally have lower OI
        # than near-term contracts — they're held-to-maturity instruments, not day-traded.
        # The spread gate (R2.3b/c) already catches genuinely illiquid LEAPs.
        _oi_raw = pd.to_numeric(row.get('Open_Interest'), errors='coerce')
        _oi_val = float(_oi_raw) if pd.notna(_oi_raw) else 0.0
        _oi_dte_raw = pd.to_numeric(row.get('Actual_DTE') or row.get('DTE'), errors='coerce')
        _oi_dte = float(_oi_dte_raw) if pd.notna(_oi_dte_raw) else 0.0
        _is_leap_oi = _oi_dte >= 270
        _oi_floor = 25 if _is_leap_oi else (100 if mid_v > 50.0 else 50)
        if _oi_val < _oi_floor:
            decision.update({
                'Execution_Status': 'CONDITIONAL',
                'Gate_Reason': (
                    f'R2.3-OI: OI={int(_oi_val)} below minimum {_oi_floor} for '
                    f'${mid_v*100:,.0f} contract — insufficient market depth for reliable fills'
                ),
                'confidence_band': 'LOW',
                'execution_adjustment': 'AVOID_THIN_OI',
            })
            row = _apply_dqs_multiplier_chain(row, decision, iv_data_stale)
            return decision

        if spread_cost_ratio <= 0.05 and strategy_type in ('DIRECTIONAL', 'VOLATILITY'):
            # Natenberg: <5% spread = institutional standard. Directional and
            # volatility trades don't require rolling — entry cost is the only
            # concern. READY but enforce limit-order-only discipline.
            # Confidence is DQS-driven (capped at MEDIUM for Acceptable/Thin liq):
            #   DQS >= 75 (Strong) → MEDIUM  |  DQS 50-74 (Eligible) → LOW
            _r2_3a_gate_reason = (
                f'R2.3a: {liquidity_grade} liquidity but spread cost {spread_cost_ratio*100:.1f}% '
                f'of premium — Natenberg: tradable, use limit order at mid only'
            )
            _r2_3a_band = _dqs_confidence_band(row.get('DQS_Score'), max_band='MEDIUM')

            # R4.1 inline: thesis quality check before admitting directional trades.
            # Without this, R2.3a returns READY before R3.2 (which has its own R4.1).
            if strategy_type == 'DIRECTIONAL':
                try:
                    _tq_pass_23a, _tq_issues_23a, _tq_conds_23a = check_thesis_quality(row)
                    if not _tq_pass_23a:
                        _tq_reason_23a = '; '.join(_tq_issues_23a)
                        decision.update({
                            'Execution_Status': 'CONDITIONAL',
                            'Gate_Reason': f'R4.1: Thesis quality — {_tq_reason_23a} [original: {_r2_3a_gate_reason}]',
                            'confidence_band': _r2_3a_band,
                            'Operating_Mode': _operating_mode(iv_maturity_level),
                            'execution_adjustment': 'LIMIT_ORDER_ONLY',
                        })
                        row = _apply_dqs_multiplier_chain(row, decision, iv_data_stale)
                        return decision
                except Exception as _tq_23a_err:
                    logger.debug(f"[R4.1-R2.3a] Thesis quality check failed: {_tq_23a_err}")

            decision.update({
                'Execution_Status': 'READY',
                'Gate_Reason': _r2_3a_gate_reason,
                'confidence_band': _r2_3a_band,
                'Operating_Mode': _operating_mode(iv_maturity_level),
                'execution_adjustment': 'LIMIT_ORDER_ONLY'
            })
        elif spread_cost_ratio <= 0.08:
            # Sinclair: 5-8% is retail-normal. Tradable with awareness.
            decision.update({
                'Execution_Status': 'CONDITIONAL',
                'Gate_Reason': (
                    f'R2.3b: {liquidity_grade} liquidity, spread cost {spread_cost_ratio*100:.1f}% '
                    f'of premium — Sinclair: retail-normal, use limit at mid, size down'
                ),
                'confidence_band': 'LOW',
                'execution_adjustment': 'SIZE_DOWN'
            })
        else:
            # Spread consumes >8% of premium — slippage materially erodes edge.
            # Cohen: income sellers specifically need better than this for rolling.
            rolling_note = ' Rolling risk: spread may widen at expiry.' if strategy_type == 'INCOME' else ''
            decision.update({
                'Execution_Status': 'CONDITIONAL',
                'Gate_Reason': (
                    f'R2.3c: {liquidity_grade} liquidity, spread cost {spread_cost_ratio*100:.1f}% '
                    f'of premium — slippage erodes edge.{rolling_note} Avoid market orders.'
                ),
                'confidence_band': 'LOW',
                'execution_adjustment': 'AVOID_MARKET_ORDER'
            })
        row = _apply_dqs_multiplier_chain(row, decision, iv_data_stale)
        return decision

    # R2.4: Minor Data Gaps
    if data_completeness_overall == 'Partial':
        decision.update({
            'Execution_Status': 'CONDITIONAL',
            'Gate_Reason': 'R2.4: Minor data gaps detected (overall completeness Partial)',
            'confidence_band': 'LOW',
            'execution_adjustment': 'CAUTION'
        })
        return decision

    # --- Rule Set 3: READY ---

    # R3.0p: PMCC LEAP leg validation — must have found a liquid LEAP before READY
    _strategy_name_r3 = str(row.get('Strategy_Name', '') or '').upper()
    if _strategy_name_r3 == 'PMCC':
        _pmcc_leap_status = str(row.get('PMCC_LEAP_Status', '') or '').upper()
        if _pmcc_leap_status != 'OK':
            decision.update({
                'Execution_Status': 'BLOCKED',
                'Gate_Reason': (
                    f'R3.0p: PMCC requires liquid LEAP leg (PMCC_LEAP_Status={_pmcc_leap_status}). '
                    'No LEAP with DTE≥270 and delta 0.70-0.85 found with OI≥10. '
                    'Hull Ch.10: diagonal requires both legs to have structural validity.'
                ),
                'confidence_band': 'LOW',
                'execution_adjustment': 'AVOID_SIZE',
            })
            return decision

    # R3.1: Income with demonstrated volatility edge and good liquidity
    if strategy_type == 'INCOME' and liquidity_grade in ['Good', 'Excellent']:
        eligible, elig_reason = check_income_eligibility(row, actual_dte)
        if eligible:
            decision.update({
                'Execution_Status': 'READY',
                'Gate_Reason': f'R3.1: Income strategy with volatility edge and {liquidity_grade} liquidity ({elig_reason})',
                'confidence_band': 'HIGH',
                'Operating_Mode': _operating_mode(iv_maturity_level),
                'execution_adjustment': 'NORMAL'
            })
            row = _apply_dqs_multiplier_chain(row, decision, iv_data_stale)
            return decision

    # R3.2: Directional with sufficient IV and good liquidity
    if (strategy_type == 'DIRECTIONAL' and iv_maturity_level >= 2 and
        liquidity_grade in ['Good', 'Excellent']):
        # Timing penalty: LATE_SHORT/LATE_LONG entries reduce directional conviction.
        # A stock already down >2% today (LATE_SHORT) or up >2% (LATE_LONG) signals
        # the move may be exhausted. Penalize DQS ×0.85 before the threshold check
        # so that a "strong" setup on a late day becomes "medium" conviction.
        # Does NOT block READY — only reduces confidence_band from HIGH to MEDIUM.
        _timing_ctx = str(row.get('entry_timing_context', '') or '').upper()
        _strat_name = str(row.get('Strategy_Name', '') or '').lower()
        if _timing_ctx in ('LATE_SHORT', 'LATE_LONG'):
            # R3.2a: Direction-exhaustion block — buying a put after a big drop
            # (LATE_SHORT) or a call after a big rally (LATE_LONG) is chasing.
            # The move is partially exhausted; long option entry is structurally
            # negative EV. Block outright instead of mild penalty.
            # (Audit: AMZN/MSFT/META Feb 2026 — all LATE_SHORT long puts, all -50%+)
            _is_long_chasing = (
                (_timing_ctx == 'LATE_SHORT' and ('long put' in _strat_name or _strat_name == 'put debit spread'))
                or (_timing_ctx == 'LATE_LONG' and ('long call' in _strat_name or _strat_name == 'call debit spread'))
            )
            if _is_long_chasing:
                decision.update({
                    'Execution_Status': 'BLOCKED',
                    'Gate_Reason': (
                        f"R3.2a: DIRECTION_EXHAUST — {_timing_ctx} + {row.get('Strategy_Name', '')}: "
                        "move already >2% in thesis direction today. Buying here is chasing "
                        "an exhausted move. Wait for pullback/consolidation before entry. "
                        "(Audit lesson: AMZN/MSFT/META Feb-2026)"
                    ),
                    'confidence_band': 'LOW',
                    'Operating_Mode': _operating_mode(iv_maturity_level),
                    'execution_adjustment': 'AVOID_SIZE',
                })
                return decision

            _raw_dqs = row.get('DQS_Score')
            if pd.notna(_raw_dqs):
                row = row.copy()
                _penalized_dqs = float(_raw_dqs) * 0.85
                row['DQS_Score'] = _penalized_dqs
                logger.debug(
                    f"  [TimingPenalty] {_timing_ctx}: DQS {float(_raw_dqs):.0f} "
                    f"→ {_penalized_dqs:.0f} (×0.85 late-entry penalty)"
                )

        # Data staleness penalty (Capital Survival Audit, Phase 4):
        # Morning scans use prior-close IV (14-18h old). Penalize DQS to reflect
        # reduced conviction when acting on stale data. Does NOT block READY.
        # Reference: Sinclair Ch.4 — information decay in volatility estimation.
        if iv_data_stale:
            _stale_dqs = row.get('DQS_Score')
            if pd.notna(_stale_dqs):
                row = row.copy() if not isinstance(row, dict) else row.copy()
                _stale_penalty = 0.85  # 15% reduction for known-stale IV data
                _stale_penalized = float(_stale_dqs) * _stale_penalty
                row['DQS_Score'] = _stale_penalized
                decision['Data_Staleness_Penalty'] = _stale_penalty
                logger.debug(
                    f"  [StalenessPenalty] iv_data_stale=True: DQS {float(_stale_dqs):.0f} "
                    f"→ {_stale_penalized:.0f} (×{_stale_penalty} staleness penalty)"
                )
            else:
                decision['Data_Staleness_Penalty'] = 1.0
        else:
            decision['Data_Staleness_Penalty'] = 1.0

        # Apply DQS multiplier chain (feedback, calendar, trajectory, clamp)
        row = _apply_dqs_multiplier_chain(row, decision, iv_data_stale)

        # Confidence tier: IV maturity >= 4 always HIGH (full 120d history).
        # When IV history is short (levels 2-3), compensate with signal conviction:
        #   - DQS_Score >= 85 (Strong setup)
        #   - STRONG directional bias (not just Moderate)
        #   - ADX >= 20 (confirmed trend, not ranging noise)
        # All three required: one weak signal can't override the others.
        if iv_maturity_level >= 4:
            confidence = 'HIGH'
            iv_note = f'IV Maturity Level {iv_maturity_level}'
        else:
            dqs_score      = row.get('DQS_Score')
            adx_val        = row.get('ADX')
            dir_bias       = str(row.get('directional_bias', '') or '').upper()
            dqs_strong     = pd.notna(dqs_score) and float(dqs_score) >= 85
            adx_trending   = pd.notna(adx_val) and float(adx_val) >= 20
            bias_strong    = dir_bias in ('BULLISH_STRONG', 'BEARISH_STRONG')
            if dqs_strong and adx_trending and bias_strong:
                confidence = 'HIGH'
                iv_note = (f'IV Maturity Level {iv_maturity_level} (short-window IV rank) '
                           f'+ DQS={float(dqs_score):.0f} + ADX={float(adx_val):.0f} + {dir_bias}')
            else:
                confidence = 'MEDIUM'
                reasons = []
                if not dqs_strong:
                    reasons.append(f'DQS={float(dqs_score):.0f}<85' if pd.notna(dqs_score) else 'DQS missing')
                if not adx_trending:
                    reasons.append(f'ADX={float(adx_val):.0f}<20' if pd.notna(adx_val) else 'ADX missing')
                if not bias_strong:
                    reasons.append(f'bias={dir_bias} (need STRONG)')
                iv_note = f'IV Maturity Level {iv_maturity_level}; signal gap: {", ".join(reasons)}'
        # NEAR_LEAP_FALLBACK confidence cap: Hull Ch.10 — 180-269 DTE lacks full LEAP-like
        # convexity and vega properties. Cap at MEDIUM regardless of IV maturity.
        _contract_status = str(row.get('Contract_Status', '') or '')
        if _contract_status == 'NEAR_LEAP_FALLBACK' and confidence == 'HIGH':
            confidence = 'MEDIUM'
            iv_note = iv_note + ' [NEAR_LEAP_FALLBACK: 180-269 DTE has reduced LEAP-like properties — Hull Ch.10]'

        # Apply feedback confidence cap/promotion (only when bucket has sufficient sample)
        # Feedback columns already set by _apply_dqs_multiplier_chain.
        _fb_action = decision.get('Feedback_Action', '')
        if _fb_action == 'TIGHTEN' and confidence == 'HIGH':
            confidence = 'MEDIUM'
            iv_note = iv_note + f' [feedback-capped: {_fb_action}]'
        elif _fb_action == 'WIDEN' and confidence == 'MEDIUM':
            confidence = 'HIGH'
            iv_note = iv_note + f' [feedback-promoted: {_fb_action}]'

        # Calendar risk confidence cap (belt + suspenders — helper already sets Calendar_Risk_Flag).
        if decision.get('Calendar_Risk_Flag') in ('HIGH_BLEED',) and confidence == 'HIGH':
            confidence = 'MEDIUM'
            iv_note = iv_note + ' [calendar-capped: pre-holiday long premium entry]'

        decision['Calibrated_Confidence'] = confidence

        # ── GAP 6 FIX: LEAP rho sensitivity flag ─────────────────────────────
        # Hull (Options, Futures, & Other Derivatives Ch.10):
        # For LEAP strategies (DTE ≥ 270), rho is material — each Fed meeting shifts
        # theoretical value meaningfully. Surface as LEAP_Rate_Sensitivity column for
        # dashboard visibility. Not a block — informational note only.
        # Rho is stored per-share per-1% rate move (e.g., 0.05 = $0.05/contract/+1%).
        _leap_strat_check = str(row.get('Strategy_Name', '') or row.get('Strategy', '') or '').lower()
        _dte_for_rho = float(row.get('DTE', 0) or 0)
        if 'leap' in _leap_strat_check or _dte_for_rho >= 270:
            try:
                _rho_raw = row.get('Rho')
                _rho_val = float(_rho_raw) if _rho_raw is not None and pd.notna(_rho_raw) else 0.0
            except (TypeError, ValueError):
                _rho_val = 0.0
            if abs(_rho_val) >= 0.05:
                decision['LEAP_Rate_Sensitivity'] = (
                    f"HIGH (Rho={_rho_val:+.3f}/contract/+1% rate; "
                    f"DTE={_dte_for_rho:.0f} — Hull Ch.10: rate risk material for LEAP)"
                )
            elif abs(_rho_val) > 0:
                decision['LEAP_Rate_Sensitivity'] = f"LOW (Rho={_rho_val:+.3f})"
            else:
                decision['LEAP_Rate_Sensitivity'] = 'UNKNOWN (no rho data)'
        else:
            decision['LEAP_Rate_Sensitivity'] = None

        # ── Timing_Gate: TQS-driven entry timing assessment ──────────────────
        # TQS ≥ 60 → EXECUTE (timing acceptable)
        # TQS 40-59 → WAIT_PULLBACK (thesis valid, entry extended — wait for RSI/price mean-reversion)
        # TQS < 40  → DEFER (poor timing — wait for RSI/price recovery)
        # Murphy Ch.4: "Wait for the pullback"; Bulkowski: chasing >5% intraday = statistical loss
        _tqs_val = row.get('TQS_Score')
        if pd.notna(_tqs_val):
            _tqs_f = float(_tqs_val)
            if _tqs_f >= 60:
                _timing_gate = 'EXECUTE'
            elif _tqs_f >= 40:
                _timing_gate = 'WAIT_PULLBACK'
            else:
                _timing_gate = 'DEFER'
        else:
            _timing_gate = 'WAIT_PULLBACK'   # no TQS → conservative; missing timing ≠ safe timing
        decision['Timing_Gate']      = _timing_gate
        decision['Timing_Gate_TQS']  = float(_tqs_val) if pd.notna(_tqs_val) else None

        # ── Price_Gate: BS fair-value overpay gate ────────────────────────────
        # AT_FAIR_VALUE  → mid within BS band (Premium_vs_FairValue_Pct ≤ +0.5%)
        # SLIGHT_PREMIUM → paying 0.5–3% above fair value (acceptable, use limit at mid)
        # WAIT_PRICE     → paying >3% above fair value — defer and target band entry
        # Natenberg Ch.8: "Price discipline is as important as direction selection"
        _fv_pct = row.get('Premium_vs_FairValue_Pct')
        _bs_lower = row.get('Entry_Band_Lower')
        _bs_upper = row.get('Entry_Band_Upper')
        if pd.notna(_fv_pct):
            _fv_f = float(_fv_pct)
            if _fv_f <= 0.5:
                _price_gate = 'AT_FAIR_VALUE'
            elif _fv_f <= 3.0:
                _price_gate = 'SLIGHT_PREMIUM'
            else:
                _price_gate = 'WAIT_PRICE'
        else:
            _price_gate = 'UNKNOWN_PRICING'  # no BS data → flag for review, missing ≠ fair
        decision['Price_Gate']              = _price_gate
        decision['Price_Gate_FV_Pct']       = float(_fv_pct) if pd.notna(_fv_pct) else None
        decision['Price_Gate_Band_Lower']   = float(_bs_lower) if pd.notna(_bs_lower) else None
        decision['Price_Gate_Band_Upper']   = float(_bs_upper) if pd.notna(_bs_upper) else None

        # ── Timing or Price gate blocks promotion to READY ────────────────────
        # WAIT_PULLBACK + WAIT_PRICE → AWAIT_CONFIRMATION, routed to wait loop
        # DEFER → AWAIT_CONFIRMATION with stronger note
        # Murphy Ch.4, Bulkowski, Natenberg Ch.8
        _timing_blocks = _timing_gate in ('WAIT_PULLBACK', 'DEFER')
        _price_blocks  = _price_gate == 'WAIT_PRICE'

        if _timing_blocks or _price_blocks:
            _block_parts = []
            if _timing_gate == 'DEFER':
                _block_parts.append(
                    f'TQS={_tqs_f:.0f}<40 — poor timing (extended, momentum exhausted). '
                    f'Murphy Ch.4: wait for mean reversion before entry.'
                )
            elif _timing_gate == 'WAIT_PULLBACK':
                if pd.notna(_tqs_val):
                    _block_parts.append(
                        f'TQS={float(_tqs_val):.0f} (40-59) — entry extended. '
                        f'Bulkowski: chasing extended move reduces edge. Wait for RSI/price pullback.'
                    )
                else:
                    _block_parts.append(
                        'TQS missing — timing unknown. '
                        'Conservative: wait for pullback confirmation before entry.'
                    )
            if _price_gate == 'WAIT_PRICE':
                _block_parts.append(
                    f'Paying {_fv_f:.1f}% above BS fair value '
                    f'(band: {_bs_lower:.2f}–{_bs_upper:.2f}). '
                    f'Natenberg Ch.8: target limit at mid-band entry.'
                )
            _gate_suffix = ' | '.join(_block_parts)
            _gate_code = 'R3.2.TIMING' if _timing_blocks else 'R3.2.PRICE'
            if _timing_blocks and _price_blocks:
                _gate_code = 'R3.2.TIMING_AND_PRICE'

            decision.update({
                'Execution_Status': 'AWAIT_CONFIRMATION',
                'Gate_Reason': f'{_gate_code}: Directional thesis valid — {iv_note} — waiting for entry conditions: {_gate_suffix}',
                'confidence_band': confidence,
                'execution_adjustment': 'WAIT_FOR_ENTRY',
            })
            return decision

        # ── Portfolio_Gate: don't surface READY when existing same-direction
        # position is deeply underwater (>25% loss on premium paid).
        # McMillan Ch.4: "Never average into a losing directional long until
        # the original thesis recovers or the position is explicitly closed."
        _ticker_val = row.get('Ticker', '')
        _pg = _check_portfolio_gate(_ticker_val, strategy_name)
        if _pg['blocks']:
            _gl_pct_str = f"{_pg['gl_pct']*100:.1f}%" if _pg['gl_pct'] is not None else 'unknown'
            decision.update({
                'Execution_Status': 'AWAIT_CONFIRMATION',
                'Gate_Reason': (
                    f"R3.2.PORTFOLIO: Existing {_pg['existing_symbol']} is {_gl_pct_str} "
                    f"unrealized — McMillan Ch.4: do not SIZE_UP into a losing directional "
                    f"long. Wait for existing position to recover above -15%, or close it first."
                ),
                'confidence_band': confidence,
                'execution_adjustment': 'WAIT_PORTFOLIO_RECOVERY',
            })
            logger.info(
                f"[PortfolioGate] 🟡 {_ticker_val} {strategy_name}: "
                f"AWAIT_CONFIRMATION — existing {_pg['existing_symbol']} at "
                f"{_gl_pct_str} P&L (threshold -25%)"
            )
            return decision

        # Earnings formation context (informational, not blocking)
        _earnings_phase_d = str(row.get('Earnings_Formation_Phase', '') or '').upper()
        _move_ratio_d = row.get('Earnings_Move_Ratio')
        _earnings_ctx_d = ''

        if _earnings_phase_d in ('EARLY_POSITIONING', 'LATE_POSITIONING') and _move_ratio_d is not None and pd.notna(_move_ratio_d):
            _mr_d = float(_move_ratio_d)
            _dte_d = row.get('days_to_earnings')
            _dte_str = f'D-{int(float(_dte_d))}' if _dte_d is not None and pd.notna(_dte_d) else 'D-?'
            if _mr_d < 0.6:
                _earnings_ctx_d = (
                    f' [EARNINGS: Move_Ratio={_mr_d:.2f} — market consistently overprices '
                    f'earnings moves ({_dte_str}, {_earnings_phase_d}). Consider reduced size.]'
                )
            elif _mr_d > 1.2:
                _earnings_ctx_d = (
                    f' [EARNINGS: Move_Ratio={_mr_d:.2f} — market underprices '
                    f'earnings moves ({_dte_str}, {_earnings_phase_d}). Stock tends to move more than implied.]'
                )

        # MC Earnings Event Simulation (Augen 0.754)
        # If near earnings, simulate hold-through vs close-before to give trader EV context
        _mc_earn_ctx = ''
        _dte_earn_mc = pd.to_numeric(row.get('days_to_earnings'), errors='coerce')
        if pd.notna(_dte_earn_mc) and 0 <= _dte_earn_mc <= 10:
            try:
                from scan_engine.mc_earnings_event import mc_earnings_event
                _mc_earn = mc_earnings_event(row)
                if _mc_earn.get('MC_Earn_Verdict') != 'SKIP':
                    decision.update(_mc_earn)
                    _mc_earn_ctx = f' [MC_EARNINGS: {_mc_earn.get("MC_Earn_Note", "")}]'
            except Exception as _mc_e:
                logger.debug(f"MC earnings sim skipped: {_mc_e}")

        # MC Variance Premium Scoring (Sinclair 0.738)
        # Score whether the option buyer is overpaying relative to realized vol
        try:
            from scan_engine.mc_variance_premium import mc_variance_premium
            _mc_vp = mc_variance_premium(row)
            if _mc_vp.get('MC_VP_Verdict') != 'SKIP':
                decision.update(_mc_vp)
                if _mc_vp.get('MC_VP_Verdict') == 'EXPENSIVE':
                    _mc_earn_ctx += f' [VP: {_mc_vp.get("MC_VP_Note", "")}]'
        except Exception as _vp_e:
            logger.debug(f"MC variance premium skipped: {_vp_e}")

        # ── MC Verdict → DQS Integration (Sinclair Ch.3, Augen Ch.7) ────────
        # Penalize DQS when MC evidence conflicts with directional entry:
        #   VP EXPENSIVE = option buyer overpaying relative to realized vol
        #   CLOSE_BEFORE = hold-through EV worse than closing before earnings
        _mc_vp_verdict = decision.get('MC_VP_Verdict', 'SKIP')
        _mc_earn_verdict = decision.get('MC_Earn_Verdict', 'SKIP')
        _mc_dqs_adj = 1.0
        if _mc_vp_verdict == 'EXPENSIVE':
            _mc_dqs_adj *= 0.95
            _mc_earn_ctx += ' [VP_GATE: ×0.95 — buying expensive vol (Sinclair Ch.3)]'
        if _mc_earn_verdict == 'CLOSE_BEFORE':
            _mc_dqs_adj *= 0.95
            _mc_earn_ctx += ' [EARN_GATE: ×0.95 — close-before EV > hold-through (Augen Ch.7)]'
        if _mc_dqs_adj < 1.0:
            _pre_mc_dqs = row.get('DQS_Score')
            if pd.notna(_pre_mc_dqs):
                row = row.copy() if not isinstance(row, dict) else row.copy()
                row['DQS_Score'] = float(_pre_mc_dqs) * _mc_dqs_adj
                logger.debug(
                    f"  [MC_Verdict] {row.get('Ticker')}: DQS "
                    f"{float(_pre_mc_dqs):.0f} → {float(row['DQS_Score']):.0f} "
                    f"(×{_mc_dqs_adj:.2f})"
                )
            decision['MC_Verdict_DQS_Adj'] = round(_mc_dqs_adj, 4)

        # R4.1 inline: thesis quality check BEFORE assigning READY.
        # Prevents surfacing directional trades with structural signal conflicts
        # that would be immediately contradicted by management doctrine.
        # (Post-loop R4.1 at line ~1461 remains as safety net for edge cases.)
        _r32_gate_reason = f'R3.2: Directional strategy — {iv_note} — {liquidity_grade} liquidity{_earnings_ctx_d}{_mc_earn_ctx}'
        try:
            _tq_pass, _tq_issues, _tq_conds = check_thesis_quality(row)
            if not _tq_pass:
                _tq_reason = '; '.join(_tq_issues)
                decision.update({
                    'Execution_Status': 'CONDITIONAL',
                    'Gate_Reason': f'R4.1: Thesis quality — {_tq_reason} [original: {_r32_gate_reason}]',
                    'confidence_band': confidence,
                    'Operating_Mode': _operating_mode(iv_maturity_level),
                    'execution_adjustment': 'NORMAL',
                    '_thesis_wait_conditions': _tq_conds,
                })
                return decision
        except Exception as _tq_err:
            logger.debug(f"Inline thesis quality check skipped: {_tq_err}")

        decision.update({
            'Execution_Status': 'READY',
            'Gate_Reason': _r32_gate_reason,
            'confidence_band': confidence,
            'Operating_Mode': _operating_mode(iv_maturity_level),
            'execution_adjustment': 'NORMAL'
        })
        return decision

    # R3.3: Volatility strategy (Straddle / Strangle) — theory verdict from Step 8
    # Step 8 evaluates vol strategies using 5-book RAG requirements:
    #   Passarelli Ch.8/2: vega ≥ 0.40, gamma ≥ 0.04/0.06, gamma/theta ≥ 0.5
    #   Natenberg Ch.11/16: delta-neutral (|Δ| < 0.15), skew < 1.20 (HARD GATE)
    #   Sinclair Ch.2-4: regime (Compression/Low-Vol), no recent vol spike, VVIX < 130
    #   Hull Ch.20: RV/IV < 1.15 (not buying expensive vol)
    #
    # Step 12 trusts that verdict rather than re-deriving it.
    # Confidence tier:
    #   compliance ≥ 80 + Good/Excellent liquidity → HIGH
    #   compliance ≥ 70                            → MEDIUM
    #   compliance ≥ 50 (Watch)                    → CONDITIONAL (needs user review)
    #   Reject / Incomplete_Data / score < 50      → BLOCKED
    if strategy_type == 'VOLATILITY':
        vol_status     = str(row.get('Validation_Status', '') or '').strip()
        vol_compliance = float(row.get('Theory_Compliance_Score', 0) or 0)
        vol_notes      = str(row.get('Evaluation_Notes', '') or '')

        if vol_status in ('Reject', 'Incomplete_Data') or vol_compliance < 50:
            decision.update({
                'Execution_Status': 'BLOCKED',
                'Gate_Reason': (
                    f'R3.3: Vol strategy failed theory requirements '
                    f'(status={vol_status}, score={vol_compliance:.0f}). '
                    f'{vol_notes[:120] if vol_notes else "See Evaluation_Notes"}'
                ),
                'confidence_band': 'LOW',
                'execution_adjustment': 'AVOID_SIZE',
            })
            return decision

        if vol_status == 'Watch' or vol_compliance < 70:
            decision.update({
                'Execution_Status': 'CONDITIONAL',
                'Gate_Reason': (
                    f'R3.3: Vol strategy marginal (status={vol_status}, score={vol_compliance:.0f}). '
                    f'Tradeable with caution — review gamma/theta ratio and regime before entry. '
                    f'{vol_notes[:120] if vol_notes else ""}'
                ),
                'confidence_band': 'LOW',
                'execution_adjustment': 'SIZE_DOWN',
            })
            return decision

        # Valid (compliance ≥ 70) — assign confidence by score + liquidity
        _vol_conf = 'HIGH' if (vol_compliance >= 80 and liquidity_grade in ('Good', 'Excellent')) else 'MEDIUM'
        decision.update({
            'Execution_Status': 'READY',
            'Gate_Reason': (
                f'R3.3: Vol strategy — score={vol_compliance:.0f}/100, '
                f'{liquidity_grade} liquidity. '
                f'{vol_notes[:120] if vol_notes else ""}'
            ),
            'confidence_band': _vol_conf,
            'execution_adjustment': 'NORMAL',
        })
        row = _apply_dqs_multiplier_chain(row, decision, iv_data_stale)
        return decision

    # R3.4: Neutral / Watch — no actionable signal from Step 6.
    # These are tickers with insufficient directional/vol/income conviction.
    if strategy_type == 'NEUTRAL':
        decision.update({
            'Execution_Status': 'BLOCKED',
            'Gate_Reason': 'R3.4: Neutral/Watch — no strong signals detected, awaiting clearer setup',
            'confidence_band': 'LOW',
            'execution_adjustment': 'AVOID_SIZE',
        })
        return decision

    # --- Default Fallback ---
    decision.update({
        'Execution_Status': 'BLOCKED',
        'Gate_Reason': f'Default Fallback: Unrecognized strategy type ({strategy_type}) — BLOCKED for safety',
        'confidence_band': 'LOW',
        'execution_adjustment': 'AVOID_SIZE'
    })
    return decision


# ============================================================
# PIPELINE INTEGRATION
# ============================================================

def apply_acceptance_logic(df: pd.DataFrame, expiry_intent: str = 'ANY', is_initial_pass: bool = False, run_id: Optional[str] = None) -> pd.DataFrame: # Added is_initial_pass
    """
    Apply the Execution Gate logic to all contracts in DataFrame.

    This is the main entry point for Step 12 integration.

    Args:
        df: DataFrame from Step 9B (with Phase 1 + Phase 2 + Execution IV enrichment)
        expiry_intent: THIS_WEEK | NEXT_WEEK | ANY
        is_initial_pass: True if this is the first pass of the gate (Stage 0-1)
        run_id: Optional short run identifier for scan_candidates deduplication.
                If None, generates a new UUID4 short hash for this run.
                Pass the same run_id from pipeline.py to correlate all Step 12 rows.

    Returns:
        DataFrame with Execution_Status and Gate_Reason columns added
    """
    # GUARDRAIL 5: Stable run_id ensures scan_candidates rows are grouped by pipeline run,
    # not by Scan_TS (which could collide on same-second reruns or parallel debug runs).
    _run_id = run_id if run_id else str(uuid.uuid4())[:8]
    logger.info(f"🎯 Step 12: Applying Execution Gate logic (Initial Pass: {is_initial_pass})...")
    
    # ACTION 8: Market Stress Hard Gate
    stress_level, median_iv, stress_basis = check_market_stress()
    is_halted = (stress_level == 'RED')
    
    if df.empty:
        logger.warning("Empty DataFrame - no contracts to evaluate")
        return df

    df_result = df.copy()

    # PATCH 3: Pre-acceptance IV metadata diagnostic
    logger.info("📊 [DIAGNOSTIC] IV metadata check before acceptance filtering:")
    if 'IV_Maturity_State' in df_result.columns:
        maturity_counts = df_result['IV_Maturity_State'].value_counts().to_dict()
        logger.info(f"   IV_Maturity_State distribution: {maturity_counts}")
    else:
        logger.error("   ❌ CRITICAL: IV_Maturity_State column MISSING!")

    if 'iv_history_days' in df_result.columns:
        has_history = (~df_result['iv_history_days'].isna()).sum()
        logger.info(f"   iv_history_days: {has_history}/{len(df_result)} strategies have data")
        if has_history > 0:
            avg_days = df_result['iv_history_days'].mean()
            logger.info(f"   Average iv_history_days: {avg_days:.0f} days")
    else:
        logger.warning("   ⚠️ iv_history_days column not found (may not be critical)")

    if 'IV_Rank_30D' in df_result.columns:
        has_rank = (~df_result['IV_Rank_30D'].isna()).sum()
        logger.info(f"   IV_Rank_30D: {has_rank}/{len(df_result)} strategies have data")
    else:
        logger.warning("   ⚠️ IV_Rank_30D column not found")

    # PRE-FILTER: Only evaluate contracts with successful Contract_Status
    # DEBUG MODE DOCTRINE: Debug mode uses real API calls, so no special status handling needed.
    # The same execution gating must run in both debug and production modes.
    if 'Contract_Status' in df_result.columns:
        successful_statuses = ['OK', 'LEAP_FALLBACK', 'NEAR_LEAP_FALLBACK']

        failed_contracts = ~df_result['Contract_Status'].isin(successful_statuses)
        failed_count = failed_contracts.sum()
        
        if failed_count > 0:
            # ESCALATION ELIGIBILITY TRANSPARENCY (Enhancement - No Behavior Change)
            # RAG Source: Implementation request for liquidity gating transparency
            logger.info(f"🔍 Pre-filter: {failed_count} contracts have failed Contract_Status (will skip Execution Gate evaluation)")
            logger.info(f"   [ESCALATION_IMPACT] These {failed_count} contracts cannot reach AWAIT_CONFIRMATION")

            # Mark failed contracts as BLOCKED before evaluation
            df_result.loc[failed_contracts, 'Execution_Status'] = 'BLOCKED' # Renamed
            df_result.loc[failed_contracts, 'Gate_Reason'] = 'Contract validation failed (Step 9B)' # Renamed
            df_result.loc[failed_contracts, 'confidence_band'] = 'LOW'
            df_result.loc[failed_contracts, 'execution_adjustment'] = 'AVOID_SIZE'

            # Log breakdown of failure reasons
            failed_breakdown = df_result[failed_contracts]['Contract_Status'].value_counts().to_dict()
            logger.info(f"   Failure breakdown: {failed_breakdown}")

            # DIAGNOSTIC: Show which stage blocked each contract type
            if 'FAILED_LIQUIDITY_FILTER' in failed_breakdown:
                liquidity_failed = failed_breakdown['FAILED_LIQUIDITY_FILTER']
                logger.info(f"   [STAGE_DIAGNOSTIC] {liquidity_failed} contracts blocked at Step 9B liquidity filter (pre-gate)")

    
    # Initialize Execution Gate columns for successful contracts
    execution_gate_cols = [
        'Execution_Status', 'Gate_Reason', 'confidence_band',
        'directional_bias', 'structure_bias', 'timing_quality', 'execution_adjustment',
    ]
    
    for col in execution_gate_cols:
        if col not in df_result.columns:
            df_result[col] = 'UNKNOWN' # Default to UNKNOWN before gate evaluation
    
    # Apply Execution Gate logic only to contracts that passed pre-filter
    successful_mask = df_result['Contract_Status'].isin(successful_statuses) if 'Contract_Status' in df_result.columns else pd.Series([True] * len(df_result))
    
    debug_manager = get_debug_manager()
    for idx in df_result[successful_mask].index:
        row = df_result.loc[idx]
        
        # Handle Market Stress Halt (R0: Pre-Gate Hard Halt)
        if is_halted:
            df_result.at[idx, 'Execution_Status'] = 'BLOCKED' # Renamed
            df_result.at[idx, 'Gate_Reason'] = get_halt_reason(median_iv) # Renamed
            df_result.at[idx, 'confidence_band'] = 'LOW'
            df_result.at[idx, 'execution_adjustment'] = 'AVOID_SIZE'
            continue

        try:
            # Extract all inputs for the Execution Gate
            ticker = row.get('Ticker', 'UNKNOWN')
            strategy_name = row.get('Strategy_Name', 'UNKNOWN')
            strategy_type = classify_strategy_type(strategy_name)
            capital_bucket = _assign_capital_bucket(strategy_type, row.get('DTE'), strategy_name)
            iv_maturity_state = row.get('IV_Maturity_State', 'IMMATURE')
            iv_source = row.get('IV_Source', 'None')

            # IV_Rank is not a direct pipeline column — IVEngine writes IV_Rank_20D/30D/etc.
            # Fall back gracefully: prefer 30D rank, then 20D, then NaN (handled by R2.2).
            iv_rank = row.get('IV_Rank')
            if iv_rank is None or (isinstance(iv_rank, float) and pd.isna(iv_rank)):
                iv_rank = row.get('IV_Rank_30D', row.get('IV_Rank_20D', np.nan))

            liquidity_grade = row.get('Liquidity_Grade')
            if liquidity_grade is None or pd.isna(liquidity_grade):
                logger.error(f"❌ Missing Liquidity_Grade for {ticker}. Cannot apply execution gate.")
                raise ValueError(f"Required field Liquidity_Grade missing for {ticker}")

            # Scraper_Status: populated by step10 fillna('OK'). Soft-default rather than raise —
            # the value is never used in any gate rule; raising here routes to the exception
            # handler with an obscure error rather than applying R1.1 correctly.
            scraper_status = row.get('Scraper_Status') or 'NOT_INVOKED'

            # Data_Completeness_Overall: soft-default to 'Missing' so R1.1 fires with a
            # correct Gate_Reason rather than an opaque exception message.
            data_completeness_overall = row.get('Data_Completeness_Overall') or 'Missing'

            # Non-critical fields (can have defaults)
            iv_trend_7d = row.get('IV_Trend_7D', 'UNKNOWN')
            ivhv_gap_30d = row.get('IVHV_gap_30D', np.nan)
            signal_strength = row.get('Signal_Type', 'NEUTRAL') # Assuming Signal_Type maps to Signal_Strength

            # Existing Phase 1/2/3 inputs for context
            compression = row.get('compression_tag', 'UNKNOWN')
            regime_52w = row.get('52w_regime_tag', 'UNKNOWN')
            momentum = row.get('momentum_tag', 'UNKNOWN')
            gap = row.get('gap_tag', 'UNKNOWN')
            timing = row.get('entry_timing_context', 'UNKNOWN')
            actual_dte = row.get('Actual_DTE', np.nan)
            exec_quality = row.get('execution_quality', 'UNKNOWN')
            balance = row.get('balance_tag', 'UNKNOWN')
            div_risk = row.get('dividend_risk', 'UNKNOWN')
            history_depth_ok = row.get('history_depth_ok', False)
            iv_data_stale = row.get('iv_data_stale', True)
            regime_confidence = row.get('regime_confidence', 0.0)

            # Extract IV Maturity Level from IVEngine
            iv_maturity_level = row.get('IV_Maturity_Level', 1)
            if pd.isna(iv_maturity_level):
                iv_maturity_level = 1
            iv_maturity_level = int(iv_maturity_level)

            # Apply the Execution Gate
            decision = apply_execution_gate(
                row,
                strategy_type, iv_maturity_state, iv_source, iv_rank, iv_trend_7d,
                ivhv_gap_30d, liquidity_grade, signal_strength, scraper_status,
                data_completeness_overall,
                compression, regime_52w, momentum, gap, timing,
                row.get('directional_bias', 'UNKNOWN'),
                row.get('structure_bias', 'UNKNOWN'),
                row.get('timing_quality', 'UNKNOWN'),
                actual_dte, strategy_name,
                exec_quality, balance, div_risk, history_depth_ok, iv_data_stale,
                regime_confidence,
                is_initial_pass,
                iv_maturity_level,
            )
            
            for key, val in decision.items():
                df_result.at[idx, key] = val
            df_result.at[idx, 'Capital_Bucket'] = capital_bucket
        except Exception as e:
            if debug_manager.enabled:
                debug_manager.log_exception(
                    step="step12_execution_gate",
                    exception=e,
                    recovery_action="Skipping Execution Gate evaluation",
                    context={"ticker": row.get('Ticker'), "strategy": row.get('Strategy_Name')}
                )
            df_result.at[idx, 'Execution_Status'] = 'BLOCKED' # Renamed
            df_result.at[idx, 'Gate_Reason'] = f'Execution Gate internal error: {str(e)}' # Renamed
            df_result.at[idx, 'confidence_band'] = 'LOW'
            df_result.at[idx, 'execution_adjustment'] = 'AVOID_SIZE'
    
    # ── R4.1: Thesis Quality Gate — demote READY with structural conflicts ──
    # Catches cases where execution gates passed but thesis signals conflict
    # (e.g., Long Put with ADX=9 and Uptrend structure). Demotes to CONDITIONAL
    # so the wait loop can track until conditions clear.
    _thesis_demoted = 0
    _ready_directional = df_result[
        (df_result['Execution_Status'] == 'READY') &
        (df_result['Strategy_Type'].fillna('').str.upper() == 'DIRECTIONAL')
    ].index if 'Strategy_Type' in df_result.columns else []

    for idx in _ready_directional:
        _row_dict = df_result.loc[idx].to_dict()
        _tq_passed, _tq_issues, _tq_conditions = check_thesis_quality(_row_dict)
        if not _tq_passed:
            _tq_reason = '; '.join(_tq_issues)
            _orig_gate = str(df_result.at[idx, 'Gate_Reason'] or '')
            df_result.at[idx, 'Execution_Status'] = 'CONDITIONAL'
            df_result.at[idx, 'Gate_Reason'] = (
                f'R4.1: Thesis quality — {_tq_reason} '
                f'[original: {_orig_gate}]'
            )
            # Store conditions for wait_condition_generator to pick up
            df_result.at[idx, '_thesis_wait_conditions'] = _tq_conditions
            _thesis_demoted += 1
            logger.info(
                f"[R4.1] {_row_dict.get('Ticker')} {_row_dict.get('Strategy_Name')}: "
                f"demoted READY→CONDITIONAL — {_tq_reason}"
            )

    if _thesis_demoted > 0:
        logger.info(f"[R4.1] Thesis quality gate: demoted {_thesis_demoted} READY→CONDITIONAL")

    # ── R4.2: Evaluator Verdict Gate — demote READY that evaluator explicitly fails ──
    # Step 8/11 evaluator sets Evaluation_Notes with "Fails directional/income requirements"
    # when the strategy doesn't meet its own criteria. R2.3a admits purely on liquidity
    # without checking this verdict. This gate enforces the evaluator's assessment.
    _eval_demoted = 0
    _ready_with_eval = df_result[
        (df_result['Execution_Status'] == 'READY') &
        df_result['Evaluation_Notes'].fillna('').str.contains('Fails', case=False, na=False)
    ].index if 'Evaluation_Notes' in df_result.columns else []

    for idx in _ready_with_eval:
        _eval_notes = str(df_result.at[idx, 'Evaluation_Notes'] or '')
        _orig_gate = str(df_result.at[idx, 'Gate_Reason'] or '')
        df_result.at[idx, 'Execution_Status'] = 'CONDITIONAL'
        df_result.at[idx, 'Gate_Reason'] = (
            f'R4.2: Evaluator verdict — {_eval_notes[:120]} '
            f'[original: {_orig_gate}]'
        )
        _eval_demoted += 1
        logger.info(
            f"[R4.2] {df_result.at[idx, 'Ticker']} {df_result.at[idx, 'Strategy_Name']}: "
            f"demoted READY→CONDITIONAL — evaluator says fails requirements"
        )

    if _eval_demoted > 0:
        logger.info(f"[R4.2] Evaluator verdict gate: demoted {_eval_demoted} READY→CONDITIONAL")

    # ── R4.3: Effective DQS Floor — demote READY when multiplier-eroded DQS too low ──
    # After all multipliers (timing, staleness, blind-spot, trajectory, feedback),
    # a DQS below 45 means the quality chain has eroded the score beyond viability.
    # Natenberg Ch.12: "Don't enter a trade that your own scoring system rejects."
    _DQS_READY_FLOOR = 45.0
    _dqs_demoted = 0
    if 'DQS_Score' in df_result.columns:
        _ready_with_dqs = df_result[
            (df_result['Execution_Status'] == 'READY') &
            (df_result['DQS_Score'].fillna(0.0) < _DQS_READY_FLOOR)
        ].index

        for idx in _ready_with_dqs:
            _dqs_val = float(df_result.at[idx, 'DQS_Score'])
            _mult_val = float(df_result.at[idx, 'DQS_Combined_Multiplier']) if pd.notna(df_result.at[idx, 'DQS_Combined_Multiplier']) else 1.0
            _orig_gate = str(df_result.at[idx, 'Gate_Reason'] or '')
            df_result.at[idx, 'Execution_Status'] = 'CONDITIONAL'
            df_result.at[idx, 'Gate_Reason'] = (
                f'R4.3: DQS={_dqs_val:.0f} below floor {_DQS_READY_FLOOR:.0f} '
                f'(combined mult={_mult_val:.2f}) — quality eroded below READY threshold '
                f'[original: {_orig_gate}]'
            )
            _dqs_demoted += 1
            logger.info(
                f"[R4.3] {df_result.at[idx, 'Ticker']} {df_result.at[idx, 'Strategy_Name']}: "
                f"demoted READY→CONDITIONAL — DQS={_dqs_val:.0f} < {_DQS_READY_FLOOR:.0f}"
            )

        if _dqs_demoted > 0:
            logger.info(f"[R4.3] Effective DQS floor: demoted {_dqs_demoted} READY→CONDITIONAL")

    # ── R4.4: Income in Downtrend — demote income strategies in hostile structure ──
    # Selling premium (CSP, CC, BW) in a Downtrend exposes the seller to accelerated
    # assignment risk and cost-basis erosion. Murphy Ch.4: don't sell premium against the trend.
    _income_trend_demoted = 0
    _ready_income = df_result[
        (df_result['Execution_Status'] == 'READY') &
        (df_result['Strategy_Type'].fillna('').str.upper() == 'INCOME') &
        (df_result['Market_Structure'].fillna('') == 'Downtrend')
    ].index if ('Strategy_Type' in df_result.columns and 'Market_Structure' in df_result.columns) else []

    for idx in _ready_income:
        _orig_gate = str(df_result.at[idx, 'Gate_Reason'] or '')
        df_result.at[idx, 'Execution_Status'] = 'CONDITIONAL'
        df_result.at[idx, 'Gate_Reason'] = (
            f'R4.4: Income strategy in Downtrend — selling premium against the trend '
            f'increases assignment risk (Murphy Ch.4) [original: {_orig_gate}]'
        )
        _income_trend_demoted += 1
        logger.info(
            f"[R4.4] {df_result.at[idx, 'Ticker']} {df_result.at[idx, 'Strategy_Name']}: "
            f"demoted READY→CONDITIONAL — income in Downtrend"
        )

    if _income_trend_demoted > 0:
        logger.info(f"[R4.4] Income in Downtrend: demoted {_income_trend_demoted} READY→CONDITIONAL")

    # ── R4.5: Oversold Chasing Gate — demote bearish directional at exhaustion ──
    # Buying puts on already oversold stocks (RSI<30, BB<15) with bullish divergence
    # is chasing the move. Murphy Ch.9: divergence at extremes is the strongest
    # reversal signal — entering WITH the trend at that point is a mean-reversion trap.
    _chase_demoted = 0
    _ready_bearish = df_result[
        (df_result['Execution_Status'] == 'READY') &
        (df_result['Trade_Bias'].fillna('').str.upper().str.contains('BEAR', na=False))
    ].index if 'Trade_Bias' in df_result.columns else []

    for idx in _ready_bearish:
        _rsi = pd.to_numeric(df_result.at[idx, 'RSI'] if 'RSI' in df_result.columns else None, errors='coerce')
        _bb = pd.to_numeric(df_result.at[idx, 'BB_Position'] if 'BB_Position' in df_result.columns else None, errors='coerce')
        _rsi_div = str(df_result.at[idx, 'RSI_Divergence'] if 'RSI_Divergence' in df_result.columns else '')

        if (pd.notna(_rsi) and _rsi < 30
                and pd.notna(_bb) and _bb < 15
                and 'bullish' in _rsi_div.lower()):
            _orig_gate = str(df_result.at[idx, 'Gate_Reason'] or '')
            df_result.at[idx, 'Execution_Status'] = 'CONDITIONAL'
            df_result.at[idx, 'Gate_Reason'] = (
                f'R4.5: Oversold chasing — RSI={_rsi:.0f}, BB={_bb:.0f}% with '
                f'bullish divergence signals mean-reversion risk (Murphy Ch.9) '
                f'[original: {_orig_gate}]'
            )
            _chase_demoted += 1
            logger.info(
                f"[R4.5] {df_result.at[idx, 'Ticker']} {df_result.at[idx, 'Strategy_Name']}: "
                f"demoted READY→CONDITIONAL — oversold chasing (RSI={_rsi:.0f})"
            )

    if _chase_demoted > 0:
        logger.info(f"[R4.5] Oversold chasing: demoted {_chase_demoted} READY→CONDITIONAL")

    # ── R5.0: Theory Compliance Floor — directional strategy quality gate ────
    # Step 8 evaluates directional theory support (delta conviction, gamma floor,
    # MACD/volume/candle confirmation). A score below 50 means multiple chart
    # signals contradict the directional thesis. Demote to CONDITIONAL and let
    # the wait loop monitor until signals align — zero capital risk while waiting.
    #
    # Natenberg Ch.5: "A position that initially seemed sensible may under new
    # conditions represent a losing strategy." The evaluator found contradictions
    # BEFORE entry — don't accept on liquidity alone.
    #
    # This is the gap that surfaced AMD LONG_PUT as READY with theory=42/100,
    # Bullish Engulfing candle, positive MACD, weak volume, and ADX=18.
    _THEORY_FLOOR_DIRECTIONAL = 50
    _theory_demoted = 0
    _ready_directional_theory = df_result[
        (df_result['Execution_Status'] == 'READY') &
        (df_result['Strategy_Type'].fillna('').str.upper() == 'DIRECTIONAL') &
        (df_result['Theory_Compliance_Score'].fillna(999) < _THEORY_FLOOR_DIRECTIONAL) &
        (df_result['Theory_Compliance_Score'].notna())
    ].index if ('Strategy_Type' in df_result.columns
                and 'Theory_Compliance_Score' in df_result.columns) else []

    for idx in _ready_directional_theory:
        _tc_score = float(df_result.at[idx, 'Theory_Compliance_Score'])
        _tc_notes = str(df_result.at[idx, 'Evaluation_Notes'] or '')[:200]
        _orig_gate = str(df_result.at[idx, 'Gate_Reason'] or '')
        _tc_bias = str(df_result.at[idx, 'Trade_Bias'] or '').upper()
        _tc_is_bearish = 'BEAR' in _tc_bias

        # Build signal-specific wait conditions
        _tc_conditions = []

        # 1. Theory score must improve
        _tc_conditions.append({
            'condition_id': f'theory_score_{str(__import__("uuid").uuid4())[:8]}',
            'type': 'technical',
            'description': (
                f'Theory compliance must reach ≥{_THEORY_FLOOR_DIRECTIONAL} '
                f'(currently {_tc_score:.0f}/100) — Natenberg Ch.5'
            ),
            'config': {
                'metric': 'Theory_Compliance_Score',
                'operator': 'greater_than',
                'threshold': _THEORY_FLOOR_DIRECTIONAL,
            },
        })

        # 2. MACD must align with direction
        _tc_macd = pd.to_numeric(
            df_result.at[idx, 'MACD_Histogram'] if 'MACD_Histogram' in df_result.columns else None,
            errors='coerce',
        )
        if pd.notna(_tc_macd):
            _macd_contradicts = (_tc_is_bearish and _tc_macd > 0) or (not _tc_is_bearish and _tc_macd < 0)
            if _macd_contradicts:
                _tc_conditions.append({
                    'condition_id': f'theory_macd_{str(__import__("uuid").uuid4())[:8]}',
                    'type': 'technical',
                    'description': (
                        f'MACD histogram must align with {_tc_bias} direction '
                        f'(currently {"+" if _tc_macd > 0 else ""}{_tc_macd:.3f}) — Murphy Ch.10'
                    ),
                    'config': {
                        'metric': 'MACD_Histogram',
                        'operator': 'less_than' if _tc_is_bearish else 'greater_than',
                        'threshold': 0,
                    },
                })

        # 3. ADX must show trend
        _tc_adx = pd.to_numeric(
            df_result.at[idx, 'ADX'] if 'ADX' in df_result.columns else None,
            errors='coerce',
        )
        if pd.notna(_tc_adx) and _tc_adx < 20:
            _tc_conditions.append({
                'condition_id': f'theory_adx_{str(__import__("uuid").uuid4())[:8]}',
                'type': 'technical',
                'description': (
                    f'ADX must rise above 20 confirming trend '
                    f'(currently {_tc_adx:.0f}) — Murphy Ch.14'
                ),
                'config': {
                    'metric': 'ADX',
                    'operator': 'greater_than',
                    'threshold': 20,
                },
            })

        df_result.at[idx, 'Execution_Status'] = 'CONDITIONAL'
        df_result.at[idx, 'Gate_Reason'] = (
            f'R5.0: Theory compliance {_tc_score:.0f}/100 below floor '
            f'{_THEORY_FLOOR_DIRECTIONAL} — chart signals contradict '
            f'{_tc_bias} thesis. Monitoring via wait loop until signals '
            f'align. {_tc_notes[:100]} [original: {_orig_gate}]'
        )
        df_result.at[idx, '_theory_compliance_conditions'] = _tc_conditions
        _theory_demoted += 1
        logger.info(
            f"[R5.0] {df_result.at[idx, 'Ticker']} {df_result.at[idx, 'Strategy_Name']}: "
            f"demoted READY→CONDITIONAL — theory={_tc_score:.0f} < {_THEORY_FLOOR_DIRECTIONAL}"
        )

    if _theory_demoted > 0:
        logger.info(
            f"[R5.0] Theory compliance floor: demoted {_theory_demoted} "
            f"READY→CONDITIONAL (floor={_THEORY_FLOOR_DIRECTIONAL})"
        )

    # ── BUG 4 FIX: Feedback_Calibration_Applied column ──────────────────────
    # Indicates whether feedback calibration was active for each row.
    # True  = doctrine_feedback table existed AND N >= _MIN_SAMPLE AND multiplier != 1.0
    # False = table missing, fresh install, or insufficient sample (expected on fresh install)
    if 'Feedback_Action' in df_result.columns:
        df_result['Feedback_Calibration_Applied'] = (
            df_result['Feedback_Action'].notna() &
            ~df_result['Feedback_Action'].isin(['INSUFFICIENT_SAMPLE', '', 'UNKNOWN'])
        )
    else:
        df_result['Feedback_Calibration_Applied'] = False

    # ── IMP 5: Regime_Strategy_Fit column ────────────────────────────────────
    # Matrix + lookup extracted to scan_engine/scoring/filters.py
    if 'Capital_Bucket' in df_result.columns:
        df_result['Regime_Strategy_Fit'] = df_result.apply(lookup_regime_fit, axis=1)
    else:
        df_result['Regime_Strategy_Fit'] = 'UNKNOWN'

    # ── DQS Multiplier Audit — drift monitoring ─────────────────────────────
    # Persist per-candidate multiplier breakdown so we can detect long-term
    # drift (e.g., a strategy consistently clamped).  Non-fatal on failure.
    _audit_cols = ['Ticker', 'Strategy_Name', 'DQS_Combined_Multiplier',
                   'DQS_Multiplier_Clamped', 'Calendar_DQS_Multiplier',
                   'Calendar_Theta_Factor', 'Feedback_Action', 'DQS_Score']
    _has_audit_cols = all(c in df_result.columns for c in _audit_cols)
    if _has_audit_cols and not df_result.empty:
        try:
            from datetime import datetime as _audit_dt
            _acon = get_domain_write_connection(DbDomain.SCAN)
            _acon.execute("""
                CREATE TABLE IF NOT EXISTS dqs_multiplier_audit (
                    ticker              VARCHAR,
                    strategy            VARCHAR,
                    run_id              VARCHAR,
                    scan_ts             TIMESTAMP,
                    raw_dqs             DOUBLE,
                    final_dqs           DOUBLE,
                    timing_mult         DOUBLE,
                    staleness_mult      DOUBLE,
                    feedback_mult       DOUBLE,
                    calendar_mult       DOUBLE,
                    trajectory_mult     DOUBLE,
                    combined_unclamped  DOUBLE,
                    combined_clamped    DOUBLE,
                    was_clamped         BOOLEAN
                )
            """)
            _audit_ts = _audit_dt.utcnow()
            for _, _ar in df_result.iterrows():
                try:
                    _acon.execute("""
                        INSERT INTO dqs_multiplier_audit
                        (ticker, strategy, run_id, scan_ts, raw_dqs, final_dqs,
                         timing_mult, staleness_mult, feedback_mult, calendar_mult,
                         trajectory_mult, combined_unclamped, combined_clamped, was_clamped)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        str(_ar.get('Ticker', '')),
                        str(_ar.get('Strategy_Name', '') or _ar.get('Strategy', '')),
                        _run_id,
                        _audit_ts,
                        float(_ar['DQS_Score']) if pd.notna(_ar.get('DQS_Score')) else None,
                        float(_ar['DQS_Score']) if pd.notna(_ar.get('DQS_Score')) else None,
                        None,  # timing_mult — per-row not stored yet, reserved
                        float(_ar['Data_Staleness_Penalty']) if pd.notna(_ar.get('Data_Staleness_Penalty')) else 1.0,
                        None,  # feedback_mult — per-row not stored yet, reserved
                        float(_ar['Calendar_DQS_Multiplier']) if pd.notna(_ar.get('Calendar_DQS_Multiplier')) else 1.0,
                        float(_ar['Trajectory_Multiplier']) if pd.notna(_ar.get('Trajectory_Multiplier')) else 1.0,
                        float(_ar['DQS_Combined_Multiplier']) if pd.notna(_ar.get('DQS_Combined_Multiplier')) else 1.0,
                        float(_ar['DQS_Combined_Multiplier']) if pd.notna(_ar.get('DQS_Combined_Multiplier')) else 1.0,
                        bool(_ar.get('DQS_Multiplier_Clamped', False)),
                    ])
                except Exception:
                    pass  # per-row failures are non-critical
            _acon.close()
            logger.info(f"[MultiplierAudit] Persisted {len(df_result)} rows to dqs_multiplier_audit")
        except Exception as _audit_err:
            logger.debug(f"[MultiplierAudit] Write failed (non-critical): {_audit_err}")

    # ── GAP 7: Persist READY candidates to scan_candidates table ─────────────
    # Provides scan→management handshake: management can JOIN this table at entry
    # to enrich position records with scan-origin quality scores.
    # Written only when Execution_Status == 'READY'.
    _df_ready_candidates = df_result[df_result['Execution_Status'] == 'READY']
    if not _df_ready_candidates.empty:
        try:
            from datetime import datetime
            _con = get_domain_write_connection(DbDomain.SCAN)
            # GUARDRAIL 5: Use Run_ID (not Scan_TS) as part of PRIMARY KEY.
            # Scan_TS collides when two pipeline runs complete within the same second.
            # Run_ID is stable per pipeline invocation — unique rows guaranteed.
            _con.execute("""
                CREATE TABLE IF NOT EXISTS scan_candidates (
                    Ticker              VARCHAR NOT NULL,
                    Strategy_Name       VARCHAR,
                    Run_ID              VARCHAR NOT NULL,
                    Scan_TS             TIMESTAMP,
                    Execution_Status    VARCHAR,
                    DQS_Score           DOUBLE,
                    TQS_Score           DOUBLE,
                    PCS_Score           DOUBLE,
                    Confidence_Band     VARCHAR,
                    Gate_Reason         VARCHAR,
                    IV_Maturity_State   VARCHAR,
                    Regime              VARCHAR,
                    Signal_Type         VARCHAR,
                    PRIMARY KEY (Ticker, Strategy_Name, Run_ID)
                )
            """)
            _scan_ts = datetime.utcnow()
            for _, _row in _df_ready_candidates.iterrows():
                try:
                    _con.execute("""
                        INSERT OR IGNORE INTO scan_candidates
                        (Ticker, Strategy_Name, Run_ID, Scan_TS, Execution_Status, DQS_Score,
                         TQS_Score, PCS_Score, Confidence_Band, Gate_Reason,
                         IV_Maturity_State, Regime, Signal_Type)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        str(_row.get('Ticker') or ''),
                        str(_row.get('Strategy_Name') or _row.get('Strategy') or ''),
                        _run_id,
                        _scan_ts,
                        'READY',
                        float(_row['DQS_Score']) if pd.notna(_row.get('DQS_Score')) else None,
                        float(_row['TQS_Score']) if pd.notna(_row.get('TQS_Score')) else None,
                        float(_row['PCS_Score']) if pd.notna(_row.get('PCS_Score')) else None,
                        str(_row.get('confidence_band') or ''),
                        str(_row.get('Gate_Reason') or ''),
                        str(_row.get('IV_Maturity_State') or ''),
                        str(_row.get('Regime') or ''),
                        str(_row.get('Signal_Type') or ''),
                    ])
                except Exception as _row_err:
                    logger.debug(f"[scan_candidates] Row insert failed for {_row.get('Ticker')}: {_row_err}")
            _con.close()
            logger.info(f"[GAP7] scan_candidates: persisted {len(_df_ready_candidates)} READY rows to scan.duckdb")
        except Exception as _db_err:
            logger.warning(f"[GAP7] scan_candidates write failed (non-critical): {_db_err}")

    # Log summary
    status_counts = df_result['Execution_Status'].value_counts().to_dict() # Renamed
    confidence_counts = df_result['confidence_band'].value_counts().to_dict()
    
    logger.info("✅ Execution Gate logic complete:")
    logger.info(f"   Status: {status_counts}")
    logger.info(f"   Confidence: {confidence_counts}")
    
    ready_count = status_counts.get('READY', 0)
    conditional_count = status_counts.get('CONDITIONAL', 0)
    blocked_count = status_counts.get('BLOCKED', 0)
    await_confirmation_count = status_counts.get('AWAIT_CONFIRMATION', 0) # New status
    
    logger.info(f"\n📊 Execution Gate Summary (Pass: {is_initial_pass}):")
    logger.info(f"   ✅ READY: {ready_count} ({ready_count/len(df_result)*100:.1f}%)")
    logger.info(f"   ⏸️  CONDITIONAL: {conditional_count} ({conditional_count/len(df_result)*100:.1f}%)")
    logger.info(f"   ⏳ AWAIT_CONFIRMATION: {await_confirmation_count} ({await_confirmation_count/len(df_result)*100:.1f}%)")
    logger.info(f"   ❌ BLOCKED: {blocked_count} ({blocked_count/len(df_result)*100:.1f}%)")
    
    return df_result


# ============================================================
# FILTERING UTILITIES — extracted to scan_engine/scoring/filters.py
# filter_ready_contracts and sort_by_confidence imported at top.
# ============================================================


# ============================================================
# SMART WAIT LOOP INTEGRATION
# ============================================================

def persist_to_wait_list(df: pd.DataFrame, con) -> Dict[str, int]:
    """
    Persist CONDITIONAL and AWAIT_CONFIRMATION trades to wait_list table.

    This function implements the Smart WAIT Loop by:
    1. Converting CONDITIONAL → AWAIT_CONFIRMATION (with wait conditions) or REJECTED
    2. Generating explicit, testable wait conditions
    3. Persisting to wait_list table for re-evaluation

    Args:
        df: DataFrame from apply_acceptance_logic
        con: DuckDB connection

    Returns:
        Dict with counts: {'await_confirmation': N, 'rejected': M}

    RAG Source: docs/SMART_WAIT_DESIGN.md
    """
    try:
        from core.wait_loop.schema import WaitListEntry, TradeStatus, extract_contract_quality
        from core.wait_loop.persistence import save_wait_entry
        from core.wait_loop.ttl import calculate_expiry_deadline, get_ttl_config
        from .wait_condition_generator import (
            generate_wait_conditions_for_gate,
            should_reject_permanently
        )
        import uuid
        from datetime import datetime
    except ImportError as e:
        logger.warning(f"[WAIT_LOOP] Could not import wait_loop modules: {e}")
        logger.warning("[WAIT_LOOP] Skipping wait list persistence")
        return {'await_confirmation': 0, 'rejected': 0}

    # Filter for CONDITIONAL and AWAIT_CONFIRMATION trades
    candidates = df[df['Execution_Status'].isin(['CONDITIONAL', 'AWAIT_CONFIRMATION'])].copy()

    if candidates.empty:
        logger.info("[WAIT_LOOP] No CONDITIONAL or AWAIT_CONFIRMATION trades to persist")
        return {'await_confirmation': 0, 'rejected': 0}

    logger.info(f"[WAIT_LOOP] Processing {len(candidates)} candidates for wait list")

    await_confirmation_count = 0
    rejected_count = 0
    now = datetime.now()

    for idx, row in candidates.iterrows():
        ticker = row.get('Ticker', 'UNKNOWN')
        strategy_name = row.get('Strategy_Name', 'UNKNOWN')
        strategy_type = row.get('Strategy_Type', 'UNKNOWN')
        gate_reason = row.get('Gate_Reason', '')

        # Build row context for condition generation
        row_context = {
            'ticker': ticker,
            'strategy_name': strategy_name,
            'strategy_type': strategy_type,
            'last_price': row.get('last_price'),
            'iv_30d': row.get('iv_30d'),
            'hv_30': row.get('hv_30'),
            'bid': row.get('Bid'),  # Fixed: Use capital B (actual column name)
            'ask': row.get('Ask'),  # Fixed: Use capital A (actual column name)
            'Liquidity_Grade': row.get('Liquidity_Grade'),
            'Data_Completeness_Overall': row.get('Data_Completeness_Overall'),
            'chart_signal': row.get('Signal_Strength'),
            'pcs_score': row.get('PCS_Score'),
            # R4.1 thesis quality: pre-generated conditions (if present)
            '_thesis_wait_conditions': row.get('_thesis_wait_conditions'),
            # Technical fields for R3.2 and R4.1 re-evaluation
            'RSI': row.get('RSI'),
            'SMA20': row.get('SMA20'),
            'Last': row.get('Last') or row.get('last_price'),
            'TQS_Score': row.get('TQS_Score'),
            'Mid_Price': row.get('Mid_Price'),
            'Entry_Band_Upper': row.get('Entry_Band_Upper'),
            'Premium_vs_FairValue_Pct': row.get('Premium_vs_FairValue_Pct'),
        }

        # Check if should be permanently rejected
        if should_reject_permanently(gate_reason, row_context):
            logger.info(f"[WAIT_LOOP] {ticker} {strategy_name}: Permanently REJECTED ({gate_reason})")
            # Update Execution_Status in DataFrame
            df.at[idx, 'Execution_Status'] = 'REJECTED'
            df.at[idx, 'Gate_Reason'] = f"REJECTED: {gate_reason}"
            rejected_count += 1
            continue

        # Generate wait conditions
        try:
            wait_conditions = generate_wait_conditions_for_gate(gate_reason, row_context)
        except Exception as e:
            logger.error(f"[WAIT_LOOP] Error generating conditions for {ticker}: {e}")
            # Default to simple time delay
            wait_conditions = [{
                "condition_id": f"default_time_{uuid.uuid4().hex[:8]}",
                "type": "time_delay",
                "description": "Wait 24 hours and recheck",
                "config": {"delay_hours": 24}
            }]

        # Calculate TTL — LEAPs get LEAP TTL even though gated as DIRECTIONAL
        _ttl_type = 'LEAP' if 'leap' in strategy_name.lower() else strategy_type
        ttl_config = get_ttl_config(_ttl_type)
        wait_expires_at = calculate_expiry_deadline(now, _ttl_type)

        # Create WaitListEntry
        # Strike may be a list [short, long] for spreads — DuckDB DOUBLE needs a scalar.
        # When loaded from CSV it arrives as the string '[350.0, 420.0]', not an actual list.
        raw_strike = row.get('Strike')
        if isinstance(raw_strike, (list, tuple)):
            proposed_strike = float(raw_strike[0]) if raw_strike else None
        elif isinstance(raw_strike, str) and raw_strike.strip().startswith('['):
            import ast
            try:
                parsed = ast.literal_eval(raw_strike)
                proposed_strike = float(parsed[0]) if parsed else None
            except Exception:
                proposed_strike = None
        else:
            proposed_strike = float(raw_strike) if raw_strike is not None else None

        wait_entry = WaitListEntry(
            wait_id=str(uuid.uuid4()),
            ticker=ticker,
            strategy_name=strategy_name,
            strategy_type=strategy_type,
            proposed_strike=proposed_strike,
            proposed_expiration=row.get('Expiration'),
            contract_symbol=row.get('Contract_Symbol'),
            wait_started_at=now,
            wait_expires_at=wait_expires_at,
            last_evaluated_at=now,
            evaluation_count=1,
            wait_conditions=wait_conditions,  # Will be converted to ConfirmationCondition objects in save
            conditions_met=[],
            wait_progress=0.0,
            entry_price=row.get('last_price', 0.0),
            entry_iv_30d=row.get('iv_30d'),
            entry_hv_30=row.get('hv_30'),
            entry_chart_signal=row.get('Signal_Strength'),
            entry_pcs_score=row.get('PCS_Score'),
            current_price=row.get('last_price'),
            current_iv_30d=row.get('iv_30d'),
            current_chart_signal=row.get('Signal_Strength'),
            price_change_pct=0.0,
            invalidation_price=row.get('last_price', 0.0) * 0.90 if row.get('last_price') else None,  # 10% stop loss
            max_sessions_wait=ttl_config['max_sessions_wait'],
            max_days_wait=ttl_config['max_days_wait'],
            contract_quality=extract_contract_quality(row),
            status=TradeStatus.ACTIVE,
            rejection_reason=None
        )

        # Save to wait_list
        try:
            # Convert wait_conditions list to ConfirmationCondition objects
            from core.wait_loop.schema import ConfirmationCondition, ConditionType
            wait_entry.wait_conditions = [
                ConfirmationCondition(
                    condition_id=c['condition_id'],
                    condition_type=ConditionType(c['type']),
                    description=c['description'],
                    config=c['config'],
                    is_met=False
                )
                for c in wait_conditions
            ]

            save_wait_entry(con, wait_entry)

            # Update Execution_Status in DataFrame
            df.at[idx, 'Execution_Status'] = 'AWAIT_CONFIRMATION'
            df.at[idx, 'Gate_Reason'] = f"AWAIT_CONFIRMATION: {gate_reason} (added to wait list)"

            await_confirmation_count += 1
            logger.info(
                f"[WAIT_LOOP] {ticker} {strategy_name}: "
                f"Saved to wait list (TTL: {ttl_config['max_days_wait']}d, "
                f"{len(wait_conditions)} conditions)"
            )
        except Exception as e:
            logger.error(f"[WAIT_LOOP] Error saving {ticker} to wait list: {e}")
            # Fallback: keep as CONDITIONAL
            continue

    logger.info(
        f"[WAIT_LOOP] Persist complete: "
        f"{await_confirmation_count} → wait list, {rejected_count} → rejected"
    )

    return {
        'await_confirmation': await_confirmation_count,
        'rejected': rejected_count
    }


def persist_verdict_skips_to_wait_list(df: pd.DataFrame, con) -> Dict[str, int]:
    """
    Persist verdict-SKIP candidates to the wait list with clearance conditions.

    These are READY candidates that passed all step12 gates but were SKIP'd
    by the execution verdict engine (interpreter weak, RSI overextended, etc.).
    They get routed to the Smart WAIT Loop so the system monitors when their
    blocking conditions clear.

    Args:
        df: DataFrame with Execution_Verdict == 'SKIP' rows
        con: DuckDB connection

    Returns:
        Dict with counts: {'verdict_await': N}
    """
    try:
        from core.wait_loop.schema import (
            WaitListEntry, TradeStatus, ConfirmationCondition,
            ConditionType, extract_contract_quality,
        )
        from core.wait_loop.persistence import save_wait_entry
        from core.wait_loop.ttl import calculate_expiry_deadline, get_ttl_config
        from scan_engine.execution_verdict import generate_verdict_wait_conditions
        import uuid
        from datetime import datetime
    except ImportError as e:
        logger.warning(f"[VERDICT_WAIT] Could not import modules: {e}")
        return {'verdict_await': 0}

    skip_df = df[df.get('Execution_Verdict', pd.Series(dtype=str)) == 'SKIP'].copy()
    if skip_df.empty:
        return {'verdict_await': 0}

    logger.info(f"[VERDICT_WAIT] Processing {len(skip_df)} verdict-SKIP candidates for wait list")

    verdict_await_count = 0
    now = datetime.now()

    for idx, row in skip_df.iterrows():
        ticker = row.get('Ticker', 'UNKNOWN')
        strategy_name = row.get('Strategy_Name', 'UNKNOWN')
        strategy_type = row.get('Strategy_Type', 'UNKNOWN')
        verdict_reason = row.get('Verdict_Reason', '')

        # Generate clearance conditions from the verdict reason
        try:
            wait_conditions = generate_verdict_wait_conditions(verdict_reason, row)
        except Exception as e:
            logger.error(f"[VERDICT_WAIT] Error generating conditions for {ticker}: {e}")
            wait_conditions = [{
                "condition_id": f"verdict_time_{uuid.uuid4().hex[:8]}",
                "type": "time_delay",
                "description": "Wait for next scan to re-evaluate verdict",
                "config": {"next_session": True}
            }]

        # TTL: LEAPs get LEAP TTL; directional gets shorter TTL than income
        _ttl_type = 'LEAP' if 'leap' in strategy_name.lower() else strategy_type
        ttl_config = get_ttl_config(_ttl_type)
        wait_expires_at = calculate_expiry_deadline(now, _ttl_type)

        # Handle strike serialization (same logic as persist_to_wait_list)
        raw_strike = row.get('Strike')
        if isinstance(raw_strike, (list, tuple)):
            proposed_strike = float(raw_strike[0]) if raw_strike else None
        elif isinstance(raw_strike, str) and raw_strike.strip().startswith('['):
            import ast
            try:
                parsed = ast.literal_eval(raw_strike)
                proposed_strike = float(parsed[0]) if parsed else None
            except Exception:
                proposed_strike = None
        else:
            proposed_strike = float(raw_strike) if raw_strike is not None else None

        # Build WaitListEntry — gate_reason prefixed with VERDICT_SKIP for traceability
        wait_entry = WaitListEntry(
            wait_id=str(uuid.uuid4()),
            ticker=ticker,
            strategy_name=strategy_name,
            strategy_type=strategy_type,
            proposed_strike=proposed_strike,
            proposed_expiration=row.get('Expiration'),
            contract_symbol=row.get('Contract_Symbol'),
            wait_started_at=now,
            wait_expires_at=wait_expires_at,
            last_evaluated_at=now,
            evaluation_count=1,
            wait_conditions=[],  # populated below
            conditions_met=[],
            wait_progress=0.0,
            entry_price=row.get('last_price', 0.0),
            entry_iv_30d=row.get('iv_30d'),
            entry_hv_30=row.get('hv_30'),
            entry_chart_signal=row.get('Signal_Strength'),
            entry_pcs_score=row.get('PCS_Score'),
            current_price=row.get('last_price'),
            current_iv_30d=row.get('iv_30d'),
            current_chart_signal=row.get('Signal_Strength'),
            price_change_pct=0.0,
            invalidation_price=(
                row.get('last_price', 0.0) * 0.90
                if row.get('last_price') else None
            ),
            max_sessions_wait=ttl_config['max_sessions_wait'],
            max_days_wait=ttl_config['max_days_wait'],
            contract_quality=extract_contract_quality(row),
            status=TradeStatus.ACTIVE,
            rejection_reason=f"VERDICT_SKIP: {verdict_reason}"
        )

        # Convert condition dicts to ConfirmationCondition objects
        try:
            wait_entry.wait_conditions = [
                ConfirmationCondition(
                    condition_id=c['condition_id'],
                    condition_type=ConditionType(c['type']),
                    description=c['description'],
                    config=c['config'],
                    is_met=False
                )
                for c in wait_conditions
            ]

            save_wait_entry(con, wait_entry)

            # Update status in source DataFrame for dashboard visibility
            df.at[idx, 'Execution_Status'] = 'AWAIT_VERDICT_CLEARANCE'
            df.at[idx, 'Gate_Reason'] = f"VERDICT_SKIP: {verdict_reason} (monitoring {len(wait_conditions)} conditions)"

            verdict_await_count += 1
            logger.info(
                f"[VERDICT_WAIT] {ticker} {strategy_name}: "
                f"Saved to wait list ({len(wait_conditions)} conditions, "
                f"TTL: {ttl_config['max_days_wait']}d)"
            )
        except Exception as e:
            logger.error(f"[VERDICT_WAIT] Error saving {ticker} to wait list: {e}")
            continue

    logger.info(
        f"[VERDICT_WAIT] Complete: {verdict_await_count} verdict-SKIP → wait list"
    )
    return {'verdict_await': verdict_await_count}
