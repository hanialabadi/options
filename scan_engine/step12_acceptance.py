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
from scan_engine.feedback_calibration import get_feedback_calibration
from scan_engine.calendar_context import calendar_risk_flag

logger = logging.getLogger(__name__)


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
        import duckdb
        _pipeline_db = os.path.join(os.path.dirname(__file__), '..', 'data', 'pipeline.duckdb')
        _pipeline_db = os.path.abspath(_pipeline_db)
        con = duckdb.connect(_pipeline_db, read_only=True)

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
# STRATEGY TYPE CLASSIFICATION
# ============================================================

def _operating_mode(iv_maturity_level: int) -> str:
    """
    Returns a human-readable Operating_Mode tag that makes the IV data context
    explicit in every READY/CONDITIONAL row.  Prevents the implicit 'CHART_DRIVEN'
    fallback from being invisible in the output.

    Level 1 (<20d): CHART_DRIVEN — no vol edge measurement possible
    Level 2 (20-60d): CHART_DRIVEN — early IV, relative rank unreliable
    Level 3 (60-120d): CHART_ASSISTED — partial IV context, rank directional only
    Level 4 (120-180d): VOL_INFORMED — sufficient history for IV_Rank signals
    Level 5 (180d+):  FULL_CONTEXT — mature surface, regime and rank trustworthy
    """
    _map = {
        1: "CHART_DRIVEN (IMMATURE: <20d IV history — no vol edge measurement)",
        2: "CHART_DRIVEN (EARLY: 20-60d IV history — chart signals primary)",
        3: "CHART_ASSISTED (PARTIAL: 60-120d IV history — IV rank directional only)",
        4: "VOL_INFORMED (DEVELOPING: 120-180d IV history — IV_Rank valid)",
        5: "FULL_CONTEXT (MATURE: 180d+ IV history — full vol surface available)",
    }
    return _map.get(int(iv_maturity_level) if iv_maturity_level else 1,
                    "CHART_DRIVEN (IMMATURE: <20d IV history — no vol edge measurement)")


def _dqs_confidence_band(dqs_score, max_band: str = 'MEDIUM') -> str:
    """
    Translate a DQS_Score (0-100) into a confidence_band, capped at max_band.

    Tiers mirror the DQS_Status thresholds:
        DQS >= 75 (Strong)   → MEDIUM  (or HIGH if max_band allows)
        DQS 50-74 (Eligible) → LOW
        DQS < 50 (Weak)      → LOW

    max_band: enforced ceiling — R2.3a (Acceptable liq) caps at MEDIUM,
              R3.1/R3.2 (Good/Excellent liq) may allow HIGH.
    """
    try:
        dqs = float(dqs_score) if dqs_score is not None and pd.notna(dqs_score) else 0.0
    except (TypeError, ValueError):
        dqs = 0.0

    if dqs >= 75:
        raw = 'HIGH'   # Strong setup — ceiling applied by max_band
    else:
        raw = 'LOW'    # Eligible (50-74) or Weak (<50) → LOW before ceiling

    # Apply ceiling
    _order = {'LOW': 0, 'MEDIUM': 1, 'HIGH': 2}
    cap = _order.get(max_band, 2)
    if _order.get(raw, 0) > cap:
        return max_band
    return raw


def classify_strategy_type(strategy_name: str) -> str:
    """
    Classify strategy into DIRECTIONAL, INCOME, or VOLATILITY.
    
    Args:
        strategy_name: Strategy name from Step 11
        
    Returns:
        'DIRECTIONAL' | 'INCOME' | 'VOLATILITY' | 'UNKNOWN'
    """
    strategy_name_lower = strategy_name.lower()
    
    # Import re for regular expression matching
    import re

    # Income strategies (premium collection) - Prioritize these first
    # Use word boundaries (\b) to ensure whole word matching
    income_keywords = [r'\bcovered call\b', r'\bnaked put\b', r'\bcsp\b',
                      r'\bbull put spread\b', r'\bbear call spread\b', r'\bcredit spread\b', r'\biron condor\b', r'\bbuy-write\b', r'\bcash-secured put\b']
    
    # Directional strategies (long/short bias)
    directional_keywords = [r'\blong call\b', r'\blong put\b', r'\bleap call\b', r'\bleap put\b',
                           r'\bbull call spread\b', r'\bbear put spread\b',
                           r'\bcall debit spread\b', r'\bput debit spread\b', r'\bvertical spread\b']
    
    # Volatility strategies (non-directional)
    volatility_keywords = [r'\bstraddle\b', r'\bstrangle\b', r'\bbutterfly\b', r'\bcondor\b']
    
    # Prioritize more specific classifications first, or ensure keywords are distinct
    for keyword_regex in income_keywords:
        if re.search(keyword_regex, strategy_name_lower):
            return 'INCOME'
            
    for keyword_regex in directional_keywords:
        if re.search(keyword_regex, strategy_name_lower):
            return 'DIRECTIONAL'
    
    for keyword_regex in volatility_keywords:
        if re.search(keyword_regex, strategy_name_lower):
            return 'VOLATILITY'
    
    return 'UNKNOWN'


def _assign_capital_bucket(strategy_type: str, dte, strategy_name: str) -> str:
    """
    Assign Capital_Bucket based on time horizon + structure type.

    TACTICAL:  short-dated directional (DTE <= 60) or volatility strategies
    STRATEGIC: LEAPS or long-dated directional (DTE > 60)
    DEFENSIVE: income strategies (CSP, BW, CC, credit spreads)
    """
    import math
    sn = (strategy_name or "").lower()
    # LEAP keyword overrides DTE — always STRATEGIC
    if "leap" in sn:
        return "STRATEGIC"
    if strategy_type == "DIRECTIONAL":
        try:
            dte_f = float(dte)
            if math.isnan(dte_f):
                dte_f = 45.0
        except (TypeError, ValueError):
            dte_f = 45.0
        return "STRATEGIC" if dte_f > 60 else "TACTICAL"
    if strategy_type == "INCOME":
        return "DEFENSIVE"
    if strategy_type == "VOLATILITY":
        return "TACTICAL"
    return "TACTICAL"  # fallback


# ============================================================
# PHASE 1 SIGNAL DETECTION
# ============================================================

def detect_directional_bias(momentum: str, regime_52w: str, gap: str, timing: str,
                             ema_signal: str = 'UNKNOWN', trend_state: str = 'UNKNOWN',
                             rsi: float = None, macd: float = None) -> str:
    """
    Detect bullish/bearish/neutral bias from chart + Phase 1 signals.

    Primary signals (chart): EMA signal, Trend_State, RSI, MACD
    Secondary signals (price structure): momentum_tag, 52w_regime

    Returns:
        'BULLISH_STRONG' | 'BULLISH_MODERATE' | 'BEARISH_STRONG' |
        'BEARISH_MODERATE' | 'NEUTRAL'
    """
    # Normalise
    ema_upper   = str(ema_signal).upper()
    trend_upper = str(trend_state).upper()

    bearish_chart = ema_upper in ('BEARISH', 'BEARISH_CROSS') or trend_upper == 'BEARISH'
    bullish_chart = ema_upper in ('BULLISH', 'BULLISH_CROSS') or trend_upper == 'BULLISH'

    # RSI & MACD directional tilt
    rsi_bearish = rsi is not None and not pd.isna(rsi) and float(rsi) < 45
    rsi_bullish = rsi is not None and not pd.isna(rsi) and float(rsi) > 55
    macd_bearish = macd is not None and not pd.isna(macd) and float(macd) < 0
    macd_bullish = macd is not None and not pd.isna(macd) and float(macd) > 0

    bearish_score = sum([bearish_chart, rsi_bearish, macd_bearish,
                         momentum == 'STRONG_DOWN_DAY'])
    bullish_score = sum([bullish_chart, rsi_bullish, macd_bullish,
                         momentum == 'STRONG_UP_DAY'])

    # Strong signals: chart + 2 confirmations
    if bearish_chart and bearish_score >= 3:
        return 'BEARISH_STRONG'
    if bullish_chart and bullish_score >= 3:
        return 'BULLISH_STRONG'

    # Moderate: chart signal present
    if bearish_chart and bearish_score >= 1:
        return 'BEARISH_MODERATE'
    if bullish_chart and bullish_score >= 1:
        return 'BULLISH_MODERATE'

    # Fallback: price-structure only (no clear chart signal)
    if momentum == 'STRONG_DOWN_DAY' and regime_52w in ['NEAR_52W_HIGH', 'MID_RANGE']:
        return 'BEARISH_MODERATE'
    if momentum == 'STRONG_UP_DAY' and regime_52w in ['NEAR_52W_LOW', 'MID_RANGE']:
        return 'BULLISH_MODERATE'

    return 'NEUTRAL'


def detect_structure_bias(compression: str, regime_52w: str, momentum: str) -> str:
    """
    Detect range-bound vs trending vs breakout structure.
    
    Returns:
        'RANGE_BOUND' | 'TRENDING' | 'BREAKOUT_SETUP' | 'BREAKOUT_TRIGGERED' | 'UNCLEAR'
    """
    # Range-bound (compression with low momentum)
    if (compression in ['COMPRESSION', 'NORMAL'] and
        regime_52w == 'MID_RANGE' and
        momentum in ['FLAT_DAY', 'NORMAL']):
        return 'RANGE_BOUND'
    
    # Trending (strong momentum with normal/expansion)
    elif (momentum in ['STRONG_UP_DAY', 'STRONG_DOWN_DAY'] and
          compression in ['NORMAL', 'EXPANSION']):
        return 'TRENDING'
    
    # Breakout setup (compressed + flat, waiting for catalyst)
    elif compression == 'COMPRESSION' and momentum in ['NORMAL', 'FLAT_DAY']:
        return 'BREAKOUT_SETUP'
    
    # Breakout triggered (compressed + strong move)
    elif compression == 'COMPRESSION' and momentum in ['STRONG_UP_DAY', 'STRONG_DOWN_DAY']:
        return 'BREAKOUT_TRIGGERED'
    
    else:
        return 'UNCLEAR'


def evaluate_timing_quality(timing: str, intraday_pos: str, gap: str, momentum: str) -> str:
    """
    Evaluate entry timing quality.
    
    Returns:
        'EXCELLENT' | 'GOOD' | 'FAIR' | 'POOR' | 'MODERATE'
    """
    # Excellent timing (early entry, no gap, pullback)
    if timing in ['EARLY_LONG', 'EARLY_SHORT'] and gap == 'NO_GAP':
        return 'EXCELLENT'
    
    # Good timing (moderate entry, mid-range)
    elif timing == 'MODERATE' and intraday_pos == 'MID_RANGE':
        return 'GOOD'
    
    # Fair timing (late entry but normal momentum)
    elif timing in ['LATE_LONG', 'LATE_SHORT'] and momentum == 'NORMAL':
        return 'FAIR'
    
    # Poor timing (late entry + gap + strong momentum = extended)
    elif timing in ['LATE_LONG', 'LATE_SHORT'] and gap in ['GAP_UP', 'GAP_DOWN']:
        return 'POOR'
    
    else:
        return 'MODERATE'


# ============================================================
# INCOME ELIGIBILITY — VOLATILITY EDGE CHECKLIST
# ============================================================

def check_income_eligibility(row: pd.Series, actual_dte: float) -> Tuple[bool, str]:
    """
    Determine whether an income strategy (CSP / Covered Call / Buy-Write) has
    a genuine volatility edge to sell premium.

    Does NOT require 120+ days of IV history. Requires:
      1. IV > HV  — there is premium to sell (gap_30d > 0)
      2. IV not at the bottom of its recent distribution — rank > 30 OR gap large enough
      3. Term structure supports near-term selling — Surface_Shape not inverted,
         OR inverted with a large gap (>10 pts) as compensation
      4. No earnings event inside the trade window

    Returns:
        (eligible: bool, reason: str)
    """
    gap_30d       = row.get('IVHV_gap_30D', None)
    iv_rank_20d   = row.get('IV_Rank_20D', None)
    iv_rank_30d   = row.get('IV_Rank_30D', None)   # BUG 2 FIX: also check 30D rank
    surface_shape = str(row.get('Surface_Shape', '') or '').upper()
    days_to_earn  = row.get('days_to_earnings', None)
    iv_hist       = row.get('IV_History_Count', 0) or 0

    # ── Condition 1: IV > HV (gap must be positive to sell premium) ──────────
    try:
        gap = float(gap_30d)
    except (TypeError, ValueError):
        return False, 'BLOCK: IV vs HV gap unavailable — cannot confirm premium edge'

    if gap <= 0:
        return False, f'BLOCK: IV not elevated vs HV (gap_30d={gap:.1f}) — no premium edge'

    # ── Condition 2: IV not at the floor of its distribution ────────────────
    # BUG 2 FIX: Explicit NaN guards before every threshold comparison.
    # Priority: IV_Rank_20D → IV_Rank_30D → gap fallback (if both NaN).
    # In pandas, NaN > 25 = False, which would incorrectly block valid setups.
    # Use the best available rank; fall back to gap proxy when both are NaN.
    rank_20d_ok = iv_rank_20d is not None and pd.notna(iv_rank_20d)
    rank_30d_ok = iv_rank_30d is not None and pd.notna(iv_rank_30d)

    if rank_20d_ok:
        rank = float(iv_rank_20d)
        rank_src = 'IV_Rank_20D'
        if rank < 25:
            return False, f'BLOCK: IV Rank too low ({rank:.0f}/100) — IV near 20d floor, no edge'
    elif rank_30d_ok:
        rank = float(iv_rank_30d)
        rank_src = 'IV_Rank_30D'
        if rank < 25:
            return False, f'BLOCK: IV Rank too low ({rank:.0f}/100, 30D) — IV near floor, no edge'
    else:
        # Both ranks are NaN (< ~20 days history) — require a larger raw gap as proxy.
        # BUG 2: gap >= 8 is the explicit fallback condition that was previously unreachable
        # when NaN comparisons silently evaluated to False.
        if gap < 8:
            return False, (
                f'BLOCK: IV Rank unavailable ({int(iv_hist)}d history) and gap_30d too small '
                f'({gap:.1f} pts) — insufficient evidence of elevation'
            )
        rank = None
        rank_src = 'gap_proxy'

    # ── Condition 3: Term structure ───────────────────────────────────────────
    # INVERTED surface = short-term IV spike (front > back). Bad for selling
    # near-term premium — you'd be selling into an already-elevated front month.
    # Waive if gap is very large (>10 pts): large absolute spread overrides shape concern.
    if surface_shape == 'INVERTED' and gap <= 10:
        return False, (
            f'BLOCK: Inverted term structure (Surface_Shape=INVERTED) with moderate gap '
            f'({gap:.1f} pts) — front IV spike makes premium selling risky'
        )

    # ── Condition 4: Earnings inside the trade window ────────────────────────
    try:
        dte_earn = float(days_to_earn)
        if dte_earn > 0:
            # NaN actual_dte: cannot confirm earnings are outside window — block conservatively.
            # NaN > 0 evaluates False in Python; explicit guard is required.
            if actual_dte is None or (isinstance(actual_dte, float) and pd.isna(actual_dte)):
                return False, (
                    f'BLOCK: Earnings in {dte_earn:.0f}d but Actual_DTE unknown — '
                    f'cannot confirm earnings are outside trade window'
                )
            if dte_earn <= float(actual_dte):
                return False, f'BLOCK: Earnings in {dte_earn:.0f}d inside trade window ({actual_dte:.0f} DTE) — binary risk'
    except (TypeError, ValueError):
        pass  # Unknown earnings date → proceed (don't block on missing data)

    # ── All conditions met ───────────────────────────────────────────────────
    if rank is not None:
        rank_str = f'{rank_src}={rank:.0f}'
    else:
        rank_str = f'gap_proxy={gap:.1f}pts'
    shape_str = surface_shape if surface_shape else 'UNKNOWN'
    return True, f'OK: gap_30d={gap:.1f}, {rank_str}, shape={shape_str}'


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

        if spread_cost_ratio <= 0.05 and strategy_type in ('DIRECTIONAL', 'VOLATILITY'):
            # Natenberg: <5% spread = institutional standard. Directional and
            # volatility trades don't require rolling — entry cost is the only
            # concern. READY but enforce limit-order-only discipline.
            # Confidence is DQS-driven (capped at MEDIUM for Acceptable/Thin liq):
            #   DQS >= 75 (Strong) → MEDIUM  |  DQS 50-74 (Eligible) → LOW
            _r2_3a_band = _dqs_confidence_band(row.get('DQS_Score'), max_band='MEDIUM')
            decision.update({
                'Execution_Status': 'READY',
                'Gate_Reason': (
                    f'R2.3a: {liquidity_grade} liquidity but spread cost {spread_cost_ratio*100:.1f}% '
                    f'of premium — Natenberg: tradable, use limit order at mid only'
                ),
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

        # Feedback calibration: adjust DQS using closed-trade outcomes from doctrine_feedback.
        # READ-ONLY — only fires when N >= 15 in the (strategy, momentum_state) bucket.
        # Graceful: any DB error → multiplier=1.0 (neutral), no log noise.
        _fb_strategy = str(row.get('Strategy_Name', '') or row.get('Strategy', '') or '').upper()
        _fb_multiplier, _fb_meta = get_feedback_calibration(
            strategy=_fb_strategy,
            entry_timing_context=_timing_ctx,
        )
        if _fb_multiplier != 1.0:
            _pre_fb_dqs = row.get('DQS_Score')
            if pd.notna(_pre_fb_dqs):
                row = row.copy() if not isinstance(row, dict) else row.copy()
                row['DQS_Score'] = float(_pre_fb_dqs) * _fb_multiplier
                logger.debug(
                    f"  [FeedbackCalibration] {_fb_strategy}: DQS {float(_pre_fb_dqs):.0f} "
                    f"→ {float(row['DQS_Score']):.0f} (×{_fb_multiplier:.2f}, "
                    f"{_fb_meta.get('suggested_action','')})"
                )
        # Always write feedback columns (even when neutral — audit trail)
        decision['Feedback_Win_Rate']  = _fb_meta.get('win_rate')
        decision['Feedback_Sample_N']  = _fb_meta.get('sample_n', 0)
        decision['Feedback_Action']    = _fb_meta.get('suggested_action', 'INSUFFICIENT_SAMPLE')
        decision['Feedback_Note']      = _fb_meta.get('note', '')

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
        _conf_adj = _fb_meta.get('confidence_adjustment')
        if _conf_adj == "MEDIUM" and confidence == "HIGH":
            confidence = "MEDIUM"
            iv_note = iv_note + f" [feedback-capped: {_fb_meta.get('suggested_action','')}]"
        elif _conf_adj == "HIGH" and confidence == "MEDIUM":
            confidence = "HIGH"
            iv_note = iv_note + f" [feedback-promoted: {_fb_meta.get('suggested_action','')}]"

        # Calendar risk flag: long premium on Friday/pre-holiday bleeds theta over weekend
        # with no offsetting stock movement. Short premium on same days collects extra theta.
        # Passarelli Ch.6, Natenberg Ch.11. Does NOT block READY — conviction can override.
        _strat_name = str(row.get('Strategy_Name', '') or row.get('Strategy', '') or '').upper()
        _cal_flag, _cal_note = calendar_risk_flag(_strat_name)
        decision['Calendar_Risk_Flag'] = _cal_flag
        decision['Calendar_Risk_Note'] = _cal_note
        if _cal_flag in ('HIGH_BLEED',) and confidence == 'HIGH':
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
            _timing_gate = 'EXECUTE'   # no TQS → don't block, data gap
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
            _price_gate = 'AT_FAIR_VALUE'  # no BS data → don't block, data gap
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
                _block_parts.append(
                    f'TQS={_tqs_f:.0f} (40-59) — entry extended. '
                    f'Bulkowski: chasing extended move reduces edge. Wait for RSI/price pullback.'
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

        decision.update({
            'Execution_Status': 'READY',
            'Gate_Reason': f'R3.2: Directional strategy — {iv_note} — {liquidity_grade} liquidity',
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
    # Natenberg Ch.19 / McMillan Ch.1 / Passarelli Ch.2 matrix lookup.
    # Surfaces (Regime × Stress) → fit/caution/mismatch for the strategy's Capital_Bucket.
    # Read-only derivation from existing columns — no logic change.
    _REGIME_MATRIX_LOCAL = {
        ('High Vol',    'CRISIS'):    {'fit': ['DEFENSIVE'],                          'caution': ['STRATEGIC'], 'mismatch': ['TACTICAL']},
        ('High Vol',    'ELEVATED'):  {'fit': ['DEFENSIVE', 'STRATEGIC'],             'caution': ['TACTICAL'],  'mismatch': []},
        ('High Vol',    'NORMAL'):    {'fit': ['DEFENSIVE', 'STRATEGIC'],             'caution': ['TACTICAL'],  'mismatch': []},
        ('High Vol',    'LOW'):       {'fit': ['DEFENSIVE', 'STRATEGIC', 'TACTICAL'], 'caution': [],            'mismatch': []},
        ('Compression', 'CRISIS'):    {'fit': ['DEFENSIVE'],                          'caution': ['STRATEGIC'], 'mismatch': ['TACTICAL']},
        ('Compression', 'ELEVATED'):  {'fit': ['DEFENSIVE'],                          'caution': ['STRATEGIC'], 'mismatch': ['TACTICAL']},
        ('Compression', 'NORMAL'):    {'fit': ['DEFENSIVE', 'STRATEGIC'],             'caution': ['TACTICAL'],  'mismatch': []},
        ('Compression', 'LOW'):       {'fit': ['DEFENSIVE', 'STRATEGIC', 'TACTICAL'], 'caution': [],            'mismatch': []},
        ('Low Vol',     'CRISIS'):    {'fit': ['DEFENSIVE'],                          'caution': ['STRATEGIC'], 'mismatch': ['TACTICAL']},
        ('Low Vol',     'ELEVATED'):  {'fit': ['DEFENSIVE'],                          'caution': ['STRATEGIC'], 'mismatch': ['TACTICAL']},
        ('Low Vol',     'NORMAL'):    {'fit': ['STRATEGIC', 'TACTICAL'],              'caution': ['DEFENSIVE'], 'mismatch': []},
        ('Low Vol',     'LOW'):       {'fit': ['STRATEGIC', 'TACTICAL'],              'caution': ['DEFENSIVE'], 'mismatch': []},
        ('Unknown',     'CRISIS'):    {'fit': ['DEFENSIVE'],                          'caution': [],            'mismatch': ['TACTICAL']},
        ('Unknown',     'ELEVATED'):  {'fit': ['DEFENSIVE', 'STRATEGIC'],             'caution': ['TACTICAL'],  'mismatch': []},
        ('Unknown',     'NORMAL'):    {'fit': ['DEFENSIVE', 'STRATEGIC', 'TACTICAL'], 'caution': [],            'mismatch': []},
        ('Unknown',     'LOW'):       {'fit': ['DEFENSIVE', 'STRATEGIC', 'TACTICAL'], 'caution': [],            'mismatch': []},
        # GAP 1: VVIX-driven regime overrides (step2 can produce these)
        # 'Expansion'  = VVIX > 130  → treat like High Vol (defensive income only in crisis)
        ('Expansion',   'CRISIS'):    {'fit': ['DEFENSIVE'],                          'caution': ['STRATEGIC'], 'mismatch': ['TACTICAL']},
        ('Expansion',   'ELEVATED'):  {'fit': ['DEFENSIVE', 'STRATEGIC'],             'caution': ['TACTICAL'],  'mismatch': []},
        ('Expansion',   'NORMAL'):    {'fit': ['DEFENSIVE', 'STRATEGIC'],             'caution': ['TACTICAL'],  'mismatch': []},
        ('Expansion',   'LOW'):       {'fit': ['DEFENSIVE', 'STRATEGIC', 'TACTICAL'], 'caution': [],            'mismatch': []},
        # 'Uncertain'  = VVIX > 100 + Compression → treat like Unknown (permissive but cautious)
        ('Uncertain',   'CRISIS'):    {'fit': ['DEFENSIVE'],                          'caution': [],            'mismatch': ['TACTICAL']},
        ('Uncertain',   'ELEVATED'):  {'fit': ['DEFENSIVE', 'STRATEGIC'],             'caution': ['TACTICAL'],  'mismatch': []},
        ('Uncertain',   'NORMAL'):    {'fit': ['DEFENSIVE', 'STRATEGIC', 'TACTICAL'], 'caution': [],            'mismatch': []},
        ('Uncertain',   'LOW'):       {'fit': ['DEFENSIVE', 'STRATEGIC', 'TACTICAL'], 'caution': [],            'mismatch': []},
    }

    def _lookup_regime_fit(row: pd.Series) -> str:
        regime  = str(row.get('Regime') or 'Unknown')
        # market_stress may be in different column names
        stress  = str(row.get('market_stress') or row.get('Market_Stress') or 'NORMAL').upper()
        bucket  = str(row.get('Capital_Bucket') or '').upper()
        if not bucket:
            return 'UNKNOWN'
        entry = _REGIME_MATRIX_LOCAL.get((regime, stress))
        if entry is None:
            # Try normalizing stress to known values
            for known_stress in ('CRISIS', 'ELEVATED', 'NORMAL', 'LOW'):
                if known_stress in stress:
                    entry = _REGIME_MATRIX_LOCAL.get((regime, known_stress))
                    break
        if entry is None:
            return 'UNKNOWN'
        if bucket in entry.get('fit', []):
            return 'FIT'
        if bucket in entry.get('caution', []):
            return 'CAUTION'
        if bucket in entry.get('mismatch', []):
            return 'MISMATCH'
        return 'UNKNOWN'

    if 'Capital_Bucket' in df_result.columns:
        df_result['Regime_Strategy_Fit'] = df_result.apply(_lookup_regime_fit, axis=1)
    else:
        df_result['Regime_Strategy_Fit'] = 'UNKNOWN'

    # ── GAP 7: Persist READY candidates to scan_candidates table ─────────────
    # Provides scan→management handshake: management can JOIN this table at entry
    # to enrich position records with scan-origin quality scores.
    # Written only when Execution_Status == 'READY'.
    _df_ready_candidates = df_result[df_result['Execution_Status'] == 'READY']
    if not _df_ready_candidates.empty:
        try:
            import duckdb
            from datetime import datetime
            _pipeline_db = os.path.join(os.path.dirname(__file__), '..', 'data', 'pipeline.duckdb')
            _pipeline_db = os.path.abspath(_pipeline_db)
            _con = duckdb.connect(_pipeline_db)
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
            logger.info(f"[GAP7] scan_candidates: persisted {len(_df_ready_candidates)} READY rows to pipeline.duckdb")
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
# FILTERING UTILITIES
# ============================================================

def filter_ready_contracts(df: pd.DataFrame, min_confidence: str = 'LOW') -> pd.DataFrame:
    """
    Filter for READY contracts.
    
    Args:
        df: DataFrame from apply_acceptance_logic (now apply_execution_gate)
        min_confidence: 'LOW' | 'MEDIUM' | 'HIGH'
    
    Returns:
        Filtered DataFrame
    """
    confidence_hierarchy = {'LOW': 1, 'MEDIUM': 2, 'HIGH': 3}
    min_level = confidence_hierarchy.get(min_confidence, 1)
    
    # Filter for READY only (excludes CONDITIONAL, BLOCKED, AWAIT_CONFIRMATION)
    df_ready = df[df['Execution_Status'] == 'READY'].copy() # Renamed
    
    if not df_ready.empty:
        df_ready['_confidence_level'] = df_ready['confidence_band'].map(confidence_hierarchy)
        df_ready = df_ready[df_ready['_confidence_level'] >= min_level]
        df_ready.drop(columns=['_confidence_level'], inplace=True)
    
    logger.info(f"🔍 Filtered for READY with {min_confidence}+ confidence: {len(df_ready)} contracts")
    
    return df_ready


def sort_by_confidence(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sort contracts by confidence band (HIGH → MEDIUM → LOW).

    Args:
        df: DataFrame from apply_acceptance_logic (now apply_execution_gate)

    Returns:
        Sorted DataFrame
    """
    confidence_order = {'HIGH': 1, 'MEDIUM': 2, 'LOW': 3, 'UNKNOWN': 4}
    df_sorted = df.copy()
    df_sorted['_confidence_sort'] = df_sorted['confidence_band'].map(confidence_order)
    df_sorted = df_sorted.sort_values('_confidence_sort')
    df_sorted.drop(columns=['_confidence_sort'], inplace=True)

    return df_sorted


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
        from core.wait_loop.schema import WaitListEntry, TradeStatus
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
            'pcs_score': row.get('PCS_Score')
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

        # Calculate TTL
        ttl_config = get_ttl_config(strategy_type)
        wait_expires_at = calculate_expiry_deadline(now, strategy_type)

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
