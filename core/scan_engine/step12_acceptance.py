"""
Step 12: Acceptance Logic

PURPOSE:
    Convert Phase 1 + Phase 2 enrichment into actionable acceptance decisions.
    
    Phase 1 (Entry Quality) drives all decisions.
    Phase 2 (Execution Quality) refines confidence and sizing guidance.
    
ARCHITECTURAL PRINCIPLES:
    - Phase 1 inputs are REQUIRED and PRIMARY
    - Phase 2 inputs are OPTIONAL and SECONDARY
    - UNKNOWN in Phase 2 = neutral (not negative)
    - All rules are deterministic and explainable
    - No acceptance rule requires Phase 2 data to function
    
INPUTS:
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
    - acceptance_status: READY_NOW | WAIT | AVOID
    - acceptance_reason: Human-readable explanation
    - confidence_band: HIGH | MEDIUM | LOW
    - directional_bias: BULLISH_STRONG | BULLISH_MODERATE | BEARISH_STRONG | BEARISH_MODERATE | NEUTRAL
    - structure_bias: TRENDING | RANGE_BOUND | BREAKOUT_SETUP | BREAKOUT_TRIGGERED | UNCLEAR
    - timing_quality: EXCELLENT | GOOD | FAIR | POOR | MODERATE
    - execution_adjustment: SIZE_UP | NORMAL | SIZE_DOWN | CAUTION
    
INTEGRATION:
    Called after Step 9B (contract fetching) to filter and prioritize contracts.
    
    df_step9b = fetch_and_select_contracts_schwab(df_step11, df_step9a)
    df_step12 = apply_acceptance_logic(df_step9b)
    df_ready = df_step12[df_step12['acceptance_status'] == 'READY_NOW']
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional
import logging
from .debug_mode import get_debug_manager
from core.data_layer.market_stress_detector import check_market_stress, get_halt_reason

logger = logging.getLogger(__name__)


# ============================================================
# STRATEGY TYPE CLASSIFICATION
# ============================================================

def classify_strategy_type(strategy_name: str) -> str:
    """
    Classify strategy into DIRECTIONAL, INCOME, or VOLATILITY.
    
    Args:
        strategy_name: Strategy name from Step 11
        
    Returns:
        'DIRECTIONAL' | 'INCOME' | 'VOLATILITY' | 'UNKNOWN'
    """
    strategy_name_lower = strategy_name.lower()
    
    # Directional strategies (long/short bias)
    directional_keywords = ['long call', 'long put', 'leap', 'bull call', 'bear put', 
                           'call debit', 'put debit', 'vertical spread']
    
    # Income strategies (premium collection)
    income_keywords = ['covered call', 'cash-secured put', 'naked put', 'csp',
                      'bull put', 'bear call', 'credit spread', 'iron condor']
    
    # Volatility strategies (non-directional)
    volatility_keywords = ['straddle', 'strangle', 'butterfly', 'condor']
    
    for keyword in directional_keywords:
        if keyword in strategy_name_lower:
            return 'DIRECTIONAL'
    
    for keyword in income_keywords:
        if keyword in strategy_name_lower:
            return 'INCOME'
    
    for keyword in volatility_keywords:
        if keyword in strategy_name_lower:
            return 'VOLATILITY'
    
    return 'UNKNOWN'


# ============================================================
# PHASE 1 SIGNAL DETECTION
# ============================================================

def detect_directional_bias(momentum: str, regime_52w: str, gap: str, timing: str) -> str:
    """
    Detect bullish/bearish/neutral bias from Phase 1 signals.
    
    Returns:
        'BULLISH_STRONG' | 'BULLISH_MODERATE' | 'BEARISH_STRONG' | 
        'BEARISH_MODERATE' | 'NEUTRAL'
    """
    # Strong bullish
    if (momentum == 'STRONG_UP_DAY' and 
        regime_52w in ['NEAR_52W_LOW', 'MID_RANGE'] and
        timing in ['EARLY_LONG', 'MODERATE']):
        return 'BULLISH_STRONG'
    
    # Strong bearish
    elif (momentum == 'STRONG_DOWN_DAY' and 
          regime_52w in ['NEAR_52W_HIGH', 'MID_RANGE'] and
          timing in ['EARLY_SHORT', 'MODERATE']):
        return 'BEARISH_STRONG'
    
    # Moderate bullish
    elif momentum in ['NORMAL', 'STRONG_UP_DAY'] and regime_52w == 'MID_RANGE':
        return 'BULLISH_MODERATE'
    
    # Moderate bearish
    elif momentum in ['NORMAL', 'STRONG_DOWN_DAY'] and regime_52w == 'MID_RANGE':
        return 'BEARISH_MODERATE'
    
    # Neutral (flat or conflicting signals)
    else:
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
# STRATEGY-SPECIFIC ACCEPTANCE RULES
# ============================================================

def apply_directional_rules(compression: str, regime_52w: str, momentum: str, 
                           gap: str, timing: str, directional_bias: str, 
                           structure_bias: str, timing_quality: str) -> Dict:
    """
    Apply acceptance rules for directional strategies (Long Call, Long Put, Debit Spreads).
    
    READY_NOW: Strong momentum + favorable timing
    WAIT: Unclear setup or late timing
    AVOID: Overextended (high reversal risk)
    """
    
    # AVOID: Overextended on all timeframes
    if ((regime_52w == 'NEAR_52W_HIGH' and momentum == 'STRONG_UP_DAY' and timing == 'LATE_LONG') or
        (regime_52w == 'NEAR_52W_LOW' and momentum == 'STRONG_DOWN_DAY' and timing == 'LATE_SHORT')):
        return {
            'acceptance_status': 'AVOID',
            'acceptance_reason': 'Overextended on all timeframes - high reversal risk',
            'confidence_band': 'LOW',
            'directional_bias': directional_bias,
            'structure_bias': structure_bias,
            'timing_quality': 'POOR'
        }
    
    # READY_NOW: Strong momentum with favorable timing
    if (momentum in ['STRONG_UP_DAY', 'STRONG_DOWN_DAY'] and
        timing in ['EARLY_LONG', 'EARLY_SHORT', 'MODERATE']):
        confidence = 'HIGH' if timing in ['EARLY_LONG', 'EARLY_SHORT'] else 'MEDIUM'
        return {
            'acceptance_status': 'READY_NOW',
            'acceptance_reason': f'{directional_bias} momentum with favorable timing',
            'confidence_band': confidence,
            'directional_bias': directional_bias,
            'structure_bias': structure_bias,
            'timing_quality': timing_quality
        }
    
    # READY_NOW: Moderate directional with good structure
    elif (directional_bias in ['BULLISH_MODERATE', 'BEARISH_MODERATE'] and
          structure_bias in ['TRENDING', 'RANGE_BOUND'] and
          timing != 'LATE_LONG' and timing != 'LATE_SHORT'):
        return {
            'acceptance_status': 'READY_NOW',
            'acceptance_reason': f'{directional_bias} setup with {structure_bias.lower()} structure',
            'confidence_band': 'MEDIUM',
            'directional_bias': directional_bias,
            'structure_bias': structure_bias,
            'timing_quality': timing_quality
        }
    
    # WAIT: Unclear or late setup
    else:
        return {
            'acceptance_status': 'WAIT',
            'acceptance_reason': 'Wait for clearer directional setup or better timing',
            'confidence_band': 'LOW',
            'directional_bias': directional_bias,
            'structure_bias': structure_bias,
            'timing_quality': timing_quality
        }


def apply_income_rules(compression: str, regime_52w: str, momentum: str, 
                      gap: str, timing: str, directional_bias: str, 
                      structure_bias: str, timing_quality: str) -> Dict:
    """
    Apply acceptance rules for income strategies (Covered Call, CSP, Credit Spreads).
    
    READY_NOW: Range-bound + compression
    WAIT: Moderate volatility
    AVOID: Strong trending environment
    """
    
    # AVOID: Strong trending environment
    if (compression == 'EXPANSION' and
        momentum in ['STRONG_UP_DAY', 'STRONG_DOWN_DAY'] and
        gap in ['GAP_UP', 'GAP_DOWN']):
        return {
            'acceptance_status': 'AVOID',
            'acceptance_reason': 'Strong trend with expansion - poor environment for income strategies',
            'confidence_band': 'LOW',
            'directional_bias': directional_bias,
            'structure_bias': structure_bias,
            'timing_quality': 'POOR'
        }
    
    # READY_NOW: Ideal range-bound setup
    if (compression in ['COMPRESSION', 'NORMAL'] and
        regime_52w == 'MID_RANGE' and
        momentum in ['FLAT_DAY', 'NORMAL']):
        confidence = 'HIGH' if compression == 'COMPRESSION' else 'MEDIUM'
        return {
            'acceptance_status': 'READY_NOW',
            'acceptance_reason': f'{compression} range in {regime_52w} - ideal for income strategies',
            'confidence_band': confidence,
            'directional_bias': 'NEUTRAL',
            'structure_bias': structure_bias,
            'timing_quality': timing_quality
        }
    
    # READY_NOW: Moderate range-bound
    elif (compression == 'NORMAL' and
          momentum in ['NORMAL', 'FLAT_DAY']):
        return {
            'acceptance_status': 'READY_NOW',
            'acceptance_reason': 'Stable range - good for income collection',
            'confidence_band': 'MEDIUM',
            'directional_bias': directional_bias,
            'structure_bias': structure_bias,
            'timing_quality': timing_quality
        }
    
    # WAIT: Moderate volatility
    else:
        return {
            'acceptance_status': 'WAIT',
            'acceptance_reason': 'Wait for range compression or lower volatility',
            'confidence_band': 'LOW',
            'directional_bias': directional_bias,
            'structure_bias': structure_bias,
            'timing_quality': timing_quality
        }


def apply_volatility_rules(compression: str, regime_52w: str, momentum: str, 
                          gap: str, timing: str, directional_bias: str, 
                          structure_bias: str, timing_quality: str) -> Dict:
    """
    Apply acceptance rules for volatility strategies (Long Straddle, Long Strangle).
    
    READY_NOW: Compressed + flat (breakout setup)
    WAIT: Normal volatility
    AVOID: Already volatile
    """
    
    # AVOID: Already volatile (poor risk/reward)
    if (compression == 'EXPANSION' or
        momentum in ['STRONG_UP_DAY', 'STRONG_DOWN_DAY']):
        return {
            'acceptance_status': 'AVOID',
            'acceptance_reason': 'Already volatile - poor risk/reward for volatility strategies',
            'confidence_band': 'LOW',
            'directional_bias': directional_bias,
            'structure_bias': structure_bias,
            'timing_quality': 'POOR'
        }
    
    # READY_NOW: Compressed breakout setup
    if (compression == 'COMPRESSION' and
        momentum in ['FLAT_DAY', 'NORMAL'] and
        regime_52w == 'MID_RANGE'):
        return {
            'acceptance_status': 'READY_NOW',
            'acceptance_reason': 'Compressed range in mid-range - setup for breakout',
            'confidence_band': 'HIGH',
            'directional_bias': 'NEUTRAL',
            'structure_bias': 'BREAKOUT_SETUP',
            'timing_quality': 'EXCELLENT'
        }
    
    # READY_NOW: Near extremes with compression
    elif (compression == 'COMPRESSION' and
          regime_52w in ['NEAR_52W_HIGH', 'NEAR_52W_LOW']):
        return {
            'acceptance_status': 'READY_NOW',
            'acceptance_reason': f'Compressed at {regime_52w} - potential reversal or breakout',
            'confidence_band': 'MEDIUM',
            'directional_bias': 'NEUTRAL',
            'structure_bias': 'BREAKOUT_SETUP',
            'timing_quality': timing_quality
        }
    
    # WAIT: No clear catalyst
    else:
        return {
            'acceptance_status': 'WAIT',
            'acceptance_reason': 'Wait for compression or clear catalyst',
            'confidence_band': 'LOW',
            'directional_bias': 'NEUTRAL',
            'structure_bias': structure_bias,
            'timing_quality': timing_quality
        }


# ============================================================
# PHASE 2 MODIFIERS
# ============================================================

def apply_phase3_history_modifiers(base_decision: Dict, history_depth_ok: bool, 
                                  iv_data_stale: bool, regime_confidence: float) -> Dict:
    """
    Apply Phase 3 History modifiers (Volatility Identity Card).
    
    Rules:
    - history_depth_ok: If False, cap confidence at LOW.
    - iv_data_stale: If True, reduce execution adjustment (caution) and add warning.
    - regime_confidence: Informational (used in PCS weighting).
    
    CRITICAL: These flags do NOT change acceptance_status (READY_NOW/WAIT).
    They only refine confidence and sizing guidance.
    """
    result = base_decision.copy()
    
    # 1. History Depth Cap
    if not history_depth_ok:
        result['confidence_band'] = 'LOW'
        result['acceptance_reason'] += " (Low history depth - capping confidence)"
        
    # 2. Data Staleness Modifier
    if iv_data_stale:
        result['execution_adjustment'] = 'CAUTION'
        result['acceptance_reason'] += " (STALE IV HISTORY - use caution)"
        
    return result


def apply_phase2_modifiers(base_decision: Dict, exec_quality: str, balance: str, 
                          div_risk: str, strategy_type: str) -> Dict:
    """
    Apply Phase 2 modifiers to refine confidence and execution guidance.
    
    Phase 2 modifiers:
    - Upgrade/downgrade confidence based on execution quality
    - Adjust sizing guidance based on book balance
    - Override READY_NOW → WAIT for HIGH dividend risk on income strategies
    
    CRITICAL: Phase 2 NEVER promotes WAIT → READY_NOW
    CRITICAL: UNKNOWN in Phase 2 = neutral (no impact)
    """
    result = base_decision.copy()
    
    # Initialize execution_adjustment if not present
    if 'execution_adjustment' not in result:
        result['execution_adjustment'] = 'NORMAL'
    
    # Only apply modifications if we have Phase 2 data (not UNKNOWN)
    # UNKNOWN = neutral = no modification
    
    # Execution quality modifier
    if exec_quality == 'EXCELLENT':
        # Upgrade confidence
        if result['confidence_band'] == 'MEDIUM':
            result['confidence_band'] = 'HIGH'
        result['execution_adjustment'] = 'SIZE_UP'
        result['acceptance_reason'] += ' + excellent execution quality'
    
    elif exec_quality == 'GOOD':
        result['execution_adjustment'] = 'NORMAL'
        result['acceptance_reason'] += ' + good execution'
    
    elif exec_quality == 'FAIR':
        result['execution_adjustment'] = 'SIZE_DOWN'
        result['acceptance_reason'] += ' + fair execution (reduce size)'
    
    elif exec_quality == 'POOR':
        # Downgrade confidence
        if result['confidence_band'] == 'HIGH':
            result['confidence_band'] = 'MEDIUM'
        result['execution_adjustment'] = 'CAUTION'
        result['acceptance_reason'] += ' + poor execution (caution advised)'
    
    # UNKNOWN: no modification (neutral)
    
    # Balance tag modifier
    if balance == 'IMBALANCED' and result['acceptance_status'] == 'READY_NOW':
        if result['execution_adjustment'] != 'CAUTION':
            result['execution_adjustment'] = 'SIZE_DOWN'
        result['acceptance_reason'] += ' (imbalanced book - reduce size)'
    
    elif balance == 'BALANCED' and result['confidence_band'] == 'HIGH':
        result['acceptance_reason'] += ' (balanced book confirms)'
    
    # UNKNOWN: no modification (neutral)
    
    # Dividend risk override (can downgrade READY_NOW → WAIT for income strategies)
    if div_risk == 'HIGH':
        # High dividend risk impacts short option strategies
        if strategy_type == 'INCOME' and result['acceptance_status'] == 'READY_NOW':
            result['acceptance_status'] = 'WAIT'
            result['acceptance_reason'] = 'HIGH dividend risk - wait until after ex-dividend date'
            result['confidence_band'] = 'LOW'
            result['execution_adjustment'] = 'CAUTION'
    
    elif div_risk == 'MODERATE':
        if strategy_type == 'INCOME':
            result['acceptance_reason'] += ' (moderate dividend risk - monitor ex-div date)'
    
    # UNKNOWN or LOW: no modification
    
    return result


# ============================================================
# MAIN EVALUATION FUNCTION
# ============================================================

def evaluate_acceptance(row: pd.Series) -> Dict:
    """
    Phase 3: Acceptance Logic
    
    Converts Phase 1 + Phase 2 enrichment into actionable acceptance decision.
    
    Args:
        row: Contract row with Phase 1 (required) and Phase 2 (optional) enrichment
    
    Returns:
        Dict with acceptance_status, acceptance_reason, confidence_band, 
        directional_bias, structure_bias, timing_quality, execution_adjustment
    """
    # ACTION 7: Step 12 is the SOVEREIGN decision authority.
    # It MUST NOT consume or be influenced by Step 11's 'Validation_Status' or 'Theory_Compliance_Score'.
    # We explicitly ignore these fields to ensure semantic isolation.
    
    # FIX 5: Explain Immature IV
    # If IV is immature, we allow execution but cap confidence and explain why.
    iv_maturity = row.get('IV_Maturity_State', 'MATURE')
    
    # Extract Phase 1 inputs (always available)
    compression = row.get('compression_tag', 'UNKNOWN')
    gap = row.get('gap_tag', 'UNKNOWN')
    intraday_pos = row.get('intraday_position_tag', 'UNKNOWN')
    regime_52w = row.get('52w_regime_tag', 'UNKNOWN')
    momentum = row.get('momentum_tag', 'UNKNOWN')
    timing = row.get('entry_timing_context', 'UNKNOWN')
    
    # Extract Phase 2 inputs (optional, default to UNKNOWN)
    exec_quality = row.get('execution_quality', 'UNKNOWN')
    balance = row.get('balance_tag', 'UNKNOWN')
    div_risk = row.get('dividend_risk', 'UNKNOWN')
    
    # Extract Phase 3 History inputs (Volatility Identity Card)
    history_depth_ok = row.get('history_depth_ok', False)
    iv_data_stale = row.get('iv_data_stale', True)
    regime_confidence = row.get('regime_confidence', 0.0)
    
    # BOOTSTRAP OVERRIDE: If history is accumulating but not yet mature,
    # we allow READY_NOW but mark it as STRUCTURALLY_READY for shadow mode tracking.
    # This prevents "forced" trades while history is bootstrapping.
    is_bootstrapping = not history_depth_ok
    
    # Extract strategy info
    strategy_name = row.get('Strategy_Name', 'UNKNOWN')
    strategy_type = classify_strategy_type(strategy_name)
    
    # Check if Phase 1 data is available
    if compression == 'UNKNOWN' or regime_52w == 'UNKNOWN' or momentum == 'UNKNOWN':
        return {
            'acceptance_status': 'WAIT',
            'acceptance_reason': 'Insufficient Phase 1 enrichment data',
            'confidence_band': 'LOW',
            'directional_bias': 'NEUTRAL',
            'structure_bias': 'UNCLEAR',
            'timing_quality': 'MODERATE',
            'execution_adjustment': 'NORMAL'
        }
    
    # Step 1: Detect directional bias
    directional_bias = detect_directional_bias(momentum, regime_52w, gap, timing)
    
    # Step 2: Detect structure bias
    structure_bias = detect_structure_bias(compression, regime_52w, momentum)
    
    # Step 3: Evaluate timing quality
    timing_quality = evaluate_timing_quality(timing, intraday_pos, gap, momentum)
    
    # Step 4: Apply strategy-specific rules (Phase 1 only)
    if strategy_type == 'DIRECTIONAL':
        base_decision = apply_directional_rules(
            compression, regime_52w, momentum, gap, timing,
            directional_bias, structure_bias, timing_quality
        )
    
    elif strategy_type == 'INCOME':
        base_decision = apply_income_rules(
            compression, regime_52w, momentum, gap, timing,
            directional_bias, structure_bias, timing_quality
        )
    
    elif strategy_type == 'VOLATILITY':
        base_decision = apply_volatility_rules(
            compression, regime_52w, momentum, gap, timing,
            directional_bias, structure_bias, timing_quality
        )
    
    else:
        # Unknown strategy type - conservative default
        base_decision = {
            'acceptance_status': 'WAIT',
            'acceptance_reason': 'Unknown strategy type - manual review required',
            'confidence_band': 'LOW',
            'directional_bias': directional_bias,
            'structure_bias': structure_bias,
            'timing_quality': timing_quality
        }
    
    # Step 5: Apply Phase 2 modifiers (if available)
    final_decision = apply_phase2_modifiers(
        base_decision, exec_quality, balance, div_risk, strategy_type
    )
    
    # Step 6: Apply Phase 3 History modifiers (Volatility Identity Card)
    final_decision = apply_phase3_history_modifiers(
        final_decision, history_depth_ok, iv_data_stale, regime_confidence
    )

    # FIX 5: Explain Immature IV (Continued)
    if iv_maturity != 'MATURE' and final_decision['acceptance_status'] == 'READY_NOW':
        final_decision['confidence_band'] = 'LOW'
        final_decision['acceptance_reason'] += f" (IV data {iv_maturity.lower()}; execution allowed but volatility context limited)"

    # Step 7: Strategy-Aware IV Gating (Demand-Driven Architecture)
    # Rules:
    # - Volatility: Hard Gate (No IV -> WAIT)
    # - Directional: Optional (No IV -> READY_NOW with confidence cap)
    # - Income: Not Required (No IV -> READY_NOW)
    has_iv = pd.notna(row.get('IV_Rank_30D')) or pd.notna(row.get('IV_Rank_XS'))
    
    if not has_iv:
        if strategy_type == 'VOLATILITY':
            final_decision['acceptance_status'] = 'WAIT'
            final_decision['acceptance_reason'] = 'MISSING_IV_HARD_GATE: Volatility strategies require historical IV context'
            final_decision['confidence_band'] = 'LOW'
        elif strategy_type == 'DIRECTIONAL':
            # Allowed but capped
            if final_decision['confidence_band'] == 'HIGH':
                final_decision['confidence_band'] = 'MEDIUM'
            final_decision['acceptance_reason'] += " (No IV context - confidence capped)"
        elif strategy_type == 'INCOME':
            # No change needed - income strategies are IV-independent for discovery
            pass
    
    # SEMANTIC FIX: "READY_NOW means READY_NOW"
    # We no longer downgrade to STRUCTURALLY_READY during bootstrap.
    # Instead, we allow them to remain READY_NOW with LOW confidence.
    # This ensures they are visible in the primary dashboard tab.
    # STRUCTURALLY_READY is deprecated for execution paths.
    
    return final_decision


# ============================================================
# PIPELINE INTEGRATION
# ============================================================

def apply_acceptance_logic(df: pd.DataFrame, expiry_intent: str = 'ANY') -> pd.DataFrame:
    """
    Apply acceptance logic to all contracts in DataFrame.
    
    This is the main entry point for Step 12 integration.
    
    Args:
        df: DataFrame from Step 9B (with Phase 1 + Phase 2 enrichment)
        expiry_intent: THIS_WEEK | NEXT_WEEK | ANY
    
    Returns:
        DataFrame with acceptance columns added
    """
    logger.info("🎯 Step 12: Applying acceptance logic...")
    
    # ACTION 8: Market Stress Hard Gate
    # UNKNOWN market stress must NEVER block execution (informational only).
    # Only RED status triggers a HARD HALT.
    # DESIGN INVARIANT: UNKNOWN is informational by design. Do not gate.
    stress_level, median_iv, stress_basis = check_market_stress()
    is_halted = (stress_level == 'RED')
    
    if df.empty:
        logger.warning("Empty DataFrame - no contracts to evaluate")
        return df
    
    df_result = df.copy()
    
    # PRE-FILTER: Only evaluate contracts with successful Contract_Status
    # Reject contracts that failed Step 9B validation (liquidity, DTE, etc.)
    if 'Contract_Status' in df_result.columns:
        successful_statuses = ['OK', 'LEAP_FALLBACK']
        failed_contracts = ~df_result['Contract_Status'].isin(successful_statuses)
        failed_count = failed_contracts.sum()
        
        if failed_count > 0:
            logger.info(f"🔍 Pre-filter: {failed_count} contracts have failed Contract_Status (will skip acceptance evaluation)")
            
            # Mark failed contracts as INCOMPLETE before evaluation
            df_result.loc[failed_contracts, 'acceptance_status'] = 'INCOMPLETE'
            df_result.loc[failed_contracts, 'acceptance_reason'] = 'Contract validation failed (Step 9B)'
            df_result.loc[failed_contracts, 'confidence_band'] = 'LOW'
            
            # Log breakdown of failure reasons
            failed_breakdown = df_result[failed_contracts]['Contract_Status'].value_counts().to_dict()
            logger.info(f"   Failure breakdown: {failed_breakdown}")
    
    # Initialize acceptance columns for successful contracts
    acceptance_cols = [
        'acceptance_status', 'acceptance_reason', 'confidence_band',
        'directional_bias', 'structure_bias', 'timing_quality', 'execution_adjustment'
    ]
    
    for col in acceptance_cols:
        if col not in df_result.columns:
            df_result[col] = 'UNKNOWN'
    
    # Apply acceptance logic only to contracts with successful Contract_Status
    successful_mask = df_result['Contract_Status'].isin(['OK', 'LEAP_FALLBACK']) if 'Contract_Status' in df_result.columns else pd.Series([True] * len(df_result))
    
    debug_manager = get_debug_manager()
    for idx in df_result[successful_mask].index:
        row = df_result.loc[idx]
        
        # Handle Market Stress Halt
        if is_halted:
            df_result.at[idx, 'acceptance_status'] = 'HALTED_MARKET_STRESS'
            df_result.at[idx, 'acceptance_reason'] = get_halt_reason(median_iv)
            df_result.at[idx, 'confidence_band'] = 'LOW'
            continue

        try:
            decision = evaluate_acceptance(row)
            for key, val in decision.items():
                df_result.at[idx, key] = val
        except Exception as e:
            if debug_manager.enabled:
                debug_manager.log_exception(
                    step="step12",
                    exception=e,
                    recovery_action="Skipping contract evaluation",
                    context={"ticker": row.get('Ticker'), "strategy": row.get('Strategy_Name')}
                )
    
    # Log summary
    status_counts = df_result['acceptance_status'].value_counts().to_dict()
    confidence_counts = df_result['confidence_band'].value_counts().to_dict()
    
    logger.info("✅ Acceptance logic complete:")
    logger.info(f"   Status: {status_counts}")
    logger.info(f"   Confidence: {confidence_counts}")
    
    ready_count = status_counts.get('READY_NOW', 0)
    wait_count = status_counts.get('WAIT', 0)
    avoid_count = status_counts.get('AVOID', 0)
    incomplete_count = status_counts.get('INCOMPLETE', 0)
    
    logger.info(f"\n📊 Acceptance Summary:")
    logger.info(f"   ✅ READY_NOW: {ready_count} ({ready_count/len(df_result)*100:.1f}%)")
    logger.info(f"   ⏸️  WAIT: {wait_count} ({wait_count/len(df_result)*100:.1f}%)")
    logger.info(f"   ❌ AVOID: {avoid_count} ({avoid_count/len(df_result)*100:.1f}%)")
    if incomplete_count > 0:
        logger.info(f"   ⚠️  INCOMPLETE: {incomplete_count} ({incomplete_count/len(df_result)*100:.1f}%)")
    
    return df_result


# ============================================================
# FILTERING UTILITIES
# ============================================================

def filter_ready_contracts(df: pd.DataFrame, min_confidence: str = 'LOW') -> pd.DataFrame:
    """
    Filter for READY_NOW contracts.
    
    SEMANTIC FIX: We now default to 'LOW' to allow all READY_NOW trades 
    regardless of confidence level (critical for bootstrap).
    
    Args:
        df: DataFrame from apply_acceptance_logic
        min_confidence: 'LOW' | 'MEDIUM' | 'HIGH'
    
    Returns:
        Filtered DataFrame
    """
    confidence_hierarchy = {'LOW': 1, 'MEDIUM': 2, 'HIGH': 3}
    min_level = confidence_hierarchy.get(min_confidence, 1)
    
    # Filter for READY_NOW only (excludes WAIT, AVOID, INCOMPLETE)
    df_ready = df[df['acceptance_status'] == 'READY_NOW'].copy()
    
    if not df_ready.empty:
        df_ready['_confidence_level'] = df_ready['confidence_band'].map(confidence_hierarchy)
        df_ready = df_ready[df_ready['_confidence_level'] >= min_level]
        df_ready.drop(columns=['_confidence_level'], inplace=True)
    
    logger.info(f"🔍 Filtered for READY_NOW with {min_confidence}+ confidence: {len(df_ready)} contracts")
    
    return df_ready


def sort_by_confidence(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sort contracts by confidence band (HIGH → MEDIUM → LOW).
    
    Args:
        df: DataFrame from apply_acceptance_logic
    
    Returns:
        Sorted DataFrame
    """
    confidence_order = {'HIGH': 1, 'MEDIUM': 2, 'LOW': 3, 'UNKNOWN': 4}
    df_sorted = df.copy()
    df_sorted['_confidence_sort'] = df_sorted['confidence_band'].map(confidence_order)
    df_sorted = df_sorted.sort_values('_confidence_sort')
    df_sorted.drop(columns=['_confidence_sort'], inplace=True)
    
    return df_sorted
