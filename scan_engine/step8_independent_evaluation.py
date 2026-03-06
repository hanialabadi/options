"""
Step 11: Strategy Independent Evaluation (STRATEGY ISOLATION)

PURPOSE:
    Evaluate each strategy INDEPENDENTLY against its own requirements.
    NO cross-strategy ranking or competition.
    Each strategy passes/fails on its own merits (theory-driven gates).
    
# AGENT SAFETY: This file is the ONLY valid strategy validation layer.
# No other file or agent is permitted to perform strategy ranking or comparison.
# This prevents agents from "helpfully" resurrecting invalid logic or bypassing architectural boundaries.

CRITICAL ARCHITECTURAL PRINCIPLE (RAG-ALIGNED):
    - Strategies do NOT compete with each other
    - Each strategy family evaluated using its own required data
    - PCS is within-family only (best straddle, best call, best CSP)
    - Portfolio layer (future) handles allocation based on user goal
    - Missing data = REJECT or INCOMPLETE, not low score

DESIGN PRINCIPLES:
    - Strategy isolation: Directionals vs Volatility vs Income = independent
    - Hard gates: Reject early (Tier 2/3), don't compensate later (Tier 4)
    - Data completeness: Missing required data → status INCOMPLETE_DATA
    - Theory-grounded: ALL 8 RAG books leveraged (comprehensive coverage)

RAG SOURCES (COMPLETE COVERAGE):
    1. Natenberg - Volatility & Pricing: IV/RV edge, skew, Vega limits
    2. Passarelli - Trading Greeks: Delta+Gamma conviction, strategy eligibility
    3. Hull - Options, Futures, Derivatives: Volatility smile, term structure
    4. Cohen - Bible of Options: Income strategies, POP, tail risk
    5. Sinclair - Volatility Trading: Regime gating, vol clustering, when NOT to trade
    6. Bulkowski - Chart Patterns: Pattern validity, statistical edge
    7. Murphy - Technical Analysis: Trend alignment, momentum, volume
    8. Nison - Candlestick Charting: Entry timing, reversal detection
    
EVALUATION METRICS (PER STRATEGY FAMILY):
    - Validation_Status: Valid, Watch, Reject, Incomplete_Data
    - Data_Completeness_Pct: 0-100% (all required data present?)
    - Strategy_Family_Rank: Rank within family (1=best directional, 1=best straddle, etc.)
    - Theory_Compliance_Score: 0-100 (how well meets RAG requirements)
    - Evaluation_Notes: Why this validation status assigned

STRATEGY ISOLATION EXAMPLE:
    Input: AAPL with 3 strategies
      AAPL | Long Call     | Delta: 0.50, Gamma: 0.03 | PCS: 85
      AAPL | Long Straddle | Vega: 0.80, Skew: 1.35  | PCS: 90
      AAPL | Buy-Write     | IV > HV: Yes            | PCS: 78
    
    Output: Independent evaluations
      AAPL | Long Call     | Status: Valid | Family Rank: 1 (best directional)
      AAPL | Long Straddle | Status: Reject | Reason: Skew >1.20 (RAG violation)
      AAPL | Buy-Write     | Status: Valid | Family Rank: 1 (best income)
    
    Result: 2 valid strategies (Call + Buy-Write), both can be executed
    Portfolio layer (future) decides allocation: 60% Call, 40% Buy-Write
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Tuple, List
import json
from datetime import datetime

logger = logging.getLogger(__name__)


# Strategy family classification
DIRECTIONAL_STRATEGIES = [
    'Long Call', 'Long Put', 'Long Call LEAP', 'Long Put LEAP',
    'Bull Call Spread', 'Bear Put Spread', 'Call Debit Spread', 'Put Debit Spread'
]

VOLATILITY_STRATEGIES = [
    'Long Straddle', 'Long Strangle',
    'Straddle', 'Strangle',  # Step 6 emits these names
]

INCOME_STRATEGIES = [
    'Cash-Secured Put', 'Covered Call', 'Buy-Write',
    'Short Iron Condor', 'Credit Spread'
]


def evaluate_strategies_independently(
    df: pd.DataFrame,
    user_goal: str = 'income',  # For portfolio layer (future), not scoring
    account_size: float = 10000.0,
    risk_tolerance: str = 'moderate'
) -> pd.DataFrame:
    """
    Evaluate each strategy independently against its own requirements.
    """
    # Ensure 'Strategy' column exists (Step 7 uses 'Strategy_Name')
    if 'Strategy' not in df.columns and 'Strategy_Name' in df.columns:
        df['Strategy'] = df['Strategy_Name']
        logger.info("ℹ️ Aliased Strategy_Name -> Strategy for Step 11 compatibility")

    """
    Evaluate each strategy independently against its own requirements.
    
    NO cross-strategy competition. Each strategy passes/fails on its own merits.
    
    HARD RULES (from Authoritative Contract):
    - Strategies are evaluated independently.
    - No ranking.
    - No forced distribution.
    - System is allowed to return zero valid strategies.
    
    Args:
        df (pd.DataFrame): Step 9B/10 output with contracts and Greeks
        user_goal (str): 'income', 'growth', 'volatility' - for portfolio layer only
        account_size (float): Account size for position sizing (future)
        risk_tolerance (str): 'conservative', 'moderate', 'aggressive' - for portfolio layer
    
    Returns:
        pd.DataFrame: All strategies with independent evaluation status
        
    Row Preservation:
        Input rows == Output rows (no strategies dropped)
        
    Example:
        >>> df_evaluated = evaluate_strategies_independently(df_contracts)
        >>> # Result: Each strategy has Validation_Status (Valid/Watch/Reject/Incomplete)
        >>> # Multiple strategies can be Valid simultaneously
        >>> # Portfolio layer (future) decides which to execute based on goal
    """
    
    if df.empty:
        logger.warning("⚠️ Empty DataFrame passed to Step 11")
        return df
    
    # --- Runtime Assertion: Ensure critical columns from upstream steps exist and are non-null ---
    required_upstream_cols = ['Signal_Type', 'Regime']
    for col in required_upstream_cols:
        if col not in df.columns:
            error_msg = f"❌ Step 11 Input Error: Missing required column '{col}' from upstream pipeline. Aborting evaluation."
            logger.error(error_msg)
            raise ValueError(error_msg)
        if df[col].isnull().any():
            error_msg = f"❌ Step 11 Input Error: Column '{col}' contains null values. Upstream steps must ensure non-null data. Aborting evaluation."
            logger.error(error_msg)
            raise ValueError(error_msg)
    logger.info(f"✅ Step 11 Input Assertion Passed: Required columns {required_upstream_cols} exist and are non-null.")

    input_row_count = len(df)
    logger.info(f"🎯 Step 11: Evaluating {input_row_count} strategies independently")
    logger.info(f"   Mode: STRATEGY ISOLATION (no cross-strategy competition)")
    logger.info(f"   User Goal: {user_goal} (for portfolio layer, not scoring)")
    
    # Initialize evaluation columns
    df_evaluated = df.copy()
    df_evaluated['Validation_Status'] = 'Pending'
    df_evaluated['Data_Completeness_Pct'] = 0.0
    df_evaluated['Missing_Required_Data'] = ''
    df_evaluated['Theory_Compliance_Score'] = 0.0
    df_evaluated['Evaluation_Notes'] = ''
    
    # Evaluate each strategy independently
    iv_maturity = df_evaluated.get('IV_Maturity_State', pd.Series(['MATURE'] * len(df_evaluated)))
    
    for idx, row in df_evaluated.iterrows():
        status, completeness, missing, compliance, notes = _evaluate_single_strategy(row)
        
        # FIX 2: Never penalize IMMATURE data
        # If IV is immature, we demote the status to DATA_NOT_MATURE instead of Reject/Incomplete
        maturity = iv_maturity.get(idx, 'MATURE')
        if maturity == 'IMMATURE' and status in ['Reject', 'Incomplete_Data']:
            status = 'DATA_NOT_MATURE'
            notes = f"Diagnostic: IV context still forming ({maturity}). " + notes
        
        df_evaluated.at[idx, 'Validation_Status'] = status
        df_evaluated.at[idx, 'Data_Completeness_Pct'] = completeness
        df_evaluated.at[idx, 'Missing_Required_Data'] = missing
        df_evaluated.at[idx, 'Theory_Compliance_Score'] = compliance
        df_evaluated.at[idx, 'Evaluation_Notes'] = notes
    
    # ACTION 6: Step 11 is DIAGNOSTIC ONLY.
    # It must NOT emit execution-triggering labels like 'EXECUTE_NOW' or 'acceptance_status'.
    # All decision authority resides in Step 12.
    
    # Rank within strategy families (NOT cross-strategy)
    df_evaluated = _rank_within_families(df_evaluated)

    # BACKWARD COMPATIBILITY: Add acceptance_status for Dashboard
    if "acceptance_status" not in df_evaluated.columns:
        if "Validation_Status" in df_evaluated.columns:
            df_evaluated["acceptance_status"] = df_evaluated["Validation_Status"]
        else:
            df_evaluated["acceptance_status"] = "UNKNOWN"
    
    # Audit evaluation results
    _audit_independent_evaluation(df_evaluated)
    
    # Verify row preservation
    output_row_count = len(df_evaluated)
    assert output_row_count == input_row_count, (
        f"❌ Row count mismatch: {output_row_count} != {input_row_count}. "
        f"Step 11 must preserve all strategies."
    )
    
    logger.info(f"✅ Step 11 Complete: {len(df_evaluated)} strategies independently evaluated")
    _log_evaluation_summary(df_evaluated, user_goal)
    
    # ====================
    # ENTRY QUALITY ENRICHMENT (NEW - Entry Readiness Scoring) - TEMPORARILY DISABLED
    # ====================
    # try:
    #     from .loaders.entry_quality_enhancements import enrich_evaluation_with_entry_readiness
    #     df_evaluated = enrich_evaluation_with_entry_readiness(df_evaluated)
    #     logger.info("✅ Entry readiness scoring complete (scan-time enhancement)")
    # except Exception as e:
    #     logger.warning(f"⚠️ Entry readiness enrichment failed (non-critical): {e}")
    
    return df_evaluated


def _evaluate_single_strategy(row: pd.Series) -> Tuple[str, float, str, float, str]:
    """
    Evaluate a single strategy against its own requirements.
    
    Contract Status Decision Tree (Step 9B semantics):
    - OK → Proceed to full evaluation
    - LEAP_FALLBACK → Proceed but reduce confidence score
    - NO_EXPIRATIONS_IN_WINDOW → Deferred (not rejected - can retry)
    - FAILED_LIQUIDITY_FILTER → Deferred if market closed, reject if market open
    - FAILED_GREEKS_FILTER → Reject (invalid contract)
    - FAILED_IV_FILTER → Reject (invalid contract)
    - NO_CHAIN_RETURNED → Reject (ticker unavailable)
    - NO_CALLS_AVAILABLE / NO_PUTS_AVAILABLE → Reject (structure missing)
    
    Returns:
        (validation_status, data_completeness_pct, missing_data, theory_compliance_score, notes)
    """
    
    strategy = next(
        (v for v in (row.get('Strategy_Name'), row.get('Strategy'), row.get('Primary_Strategy'))
         if pd.notna(v) and v), ''
    )
    strategy_type = row.get('Strategy_Type', '')
    
    # Check Step 9B Contract_Status first (new semantic-aware logic)
    contract_status = row.get('Contract_Status', None)
    failure_reason = row.get('Failure_Reason', '')
    market_open = row.get('is_market_open', True)
    
    # Handle new Contract_Status enum from Step 9B
    if contract_status == 'OK':
        # ✅ Contract available and meets criteria - proceed to full evaluation
        pass
    
    elif contract_status == 'LEAP_FALLBACK':
        # ⚠️ LEAP using shorter expiration - eligible but with reduced confidence
        # Will proceed to evaluation but note the fallback
        pass
    
    elif contract_status == 'NO_EXPIRATIONS_IN_WINDOW':
        # ⏸️ Deferred - market structure doesn't match strategy DTE needs right now
        return ('Deferred_DTE', 75.0, 'No expirations in DTE window', 50.0,
                f"Deferred: {failure_reason}. Not rejected - can retry with different DTE window or wait for new expirations.")
    
    elif contract_status == 'FAILED_LIQUIDITY_FILTER':
        # ⏸️ Deferred if market closed (thin tape), Reject if market open (structurally illiquid)
        if not market_open:
            return ('Deferred_Liquidity', 60.0, 'Thin liquidity (off-hours)', 50.0,
                    f"Deferred: {failure_reason}. Off-hours thin tape - retry during market hours.")
        else:
            return ('Reject', 40.0, 'Structurally illiquid', 0.0,
                    f"Rejected: {failure_reason}. Illiquid during market hours - not tradable.")
    
    elif contract_status == 'FAILED_GREEKS_FILTER':
        # ❌ Reject - contract has no Greeks or invalid Greeks
        return ('Reject', 30.0, 'Missing or invalid Greeks', 0.0,
                f"Rejected: {failure_reason}. Contract cannot be properly risk-managed without Greeks.")
    
    elif contract_status == 'FAILED_IV_FILTER':
        # ❌ Reject - missing IV prevents volatility analysis
        return ('Reject', 30.0, 'Missing implied volatility', 0.0,
                f"Rejected: {failure_reason}. Volatility strategies require IV data.")
    
    elif contract_status == 'NO_CHAIN_RETURNED':
        # ❌ Reject - Schwab returned no chain data at all
        return ('Reject', 0.0, 'No option chains available', 0.0,
                f"Rejected: {failure_reason}. Ticker not optionable or API failure.")
    
    elif contract_status == 'NO_CALLS_AVAILABLE':
        # ❌ Reject - strategy needs calls but none exist
        return ('Reject', 0.0, 'No call options available', 0.0,
                f"Rejected: {failure_reason}. Strategy requires calls but chain has none.")
    
    elif contract_status == 'NO_PUTS_AVAILABLE':
        # ❌ Reject - strategy needs puts but none exist
        return ('Reject', 0.0, 'No put options available', 0.0,
                f"Rejected: {failure_reason}. Strategy requires puts but chain has none.")
    
    elif contract_status is None or contract_status == '':
        # Legacy path: Step 9B not yet run (backward compatibility)
        legacy_status = row.get('Contract_Selection_Status', 'Pending')
        if legacy_status == 'Pending' or legacy_status == 'No_Chains_Available':
            return ('Pending_Greeks', 50.0, 'Contract selection not yet run (Step 9B pending)', 50.0,
                    'Pre-contract evaluation - awaiting Step 9B')
        elif legacy_status == 'No_Expirations_In_DTE_Window':
            return ('Deferred_DTE', 75.0, 'No expirations in DTE window', 50.0,
                    'Deferred: No matching expirations. Can retry with adjusted DTE.')
        elif legacy_status != 'Contracts_Available':
            return ('Reject', 0.0, 'No valid contracts selected', 0.0,
                    f"Rejected: Contract selection failed ({legacy_status})")
    
    else:
        # Unknown contract status - treat as error
        return ('Reject', 0.0, f'Unknown contract status: {contract_status}', 0.0,
                f"Rejected: Unrecognized Contract_Status value: {contract_status}")
    
    # Route to family-specific evaluation (contract available)
    if strategy in DIRECTIONAL_STRATEGIES:
        return _evaluate_directional_strategy(row)
    elif strategy in VOLATILITY_STRATEGIES:
        return _evaluate_volatility_strategy(row)
    elif strategy in INCOME_STRATEGIES:
        return _evaluate_income_strategy(row)
    else:
        # Unknown strategy family
        return ('Watch', 50.0, 'Strategy family not classified', 50.0,
                f"Strategy '{strategy}' not in known families")


def _evaluate_directional_strategy(row: pd.Series) -> Tuple[str, float, str, float, str]:
    """
    Evaluate directional strategy (Long Call/Put, LEAPs).
    
    RAG Requirements (COMPLETE - 5 Books):
    
    Greek Conviction (Passarelli Ch.4, Natenberg Ch.3):
    - Delta ≥ 0.45 (strong directional conviction)
    - Gamma ≥ 0.03 (convexity support, not optional)
    - Vega ≥ 0.18 (adjustment potential)
    
    Trend Alignment (Murphy Ch.4-6):
    - Price above SMA20 (bullish) or below (bearish)
    - Momentum confirmation (ADX, RSI)
    - Volume supporting direction
    
    Pattern Validity (Bulkowski):
    - Recognizable chart pattern (if available)
    - Statistical edge from pattern
    - Avoiding random breakouts
    
    Entry Timing (Nison Ch.5-8 - for short-term only):
    - Candlestick reversal confirmation
    - Avoiding premature entries
    
    Volatility Edge (Natenberg Ch.3):
    - Cheap IV (IV < HV preferred)
    - Not buying elevated vol
    
    Returns:
        (validation_status, data_completeness_pct, missing_data, compliance_score, notes)
    """
    
    strategy = next(
        (v for v in (row.get('Strategy_Name'), row.get('Strategy'), row.get('Primary_Strategy'))
         if pd.notna(v) and v), ''
    )
    contract_status = row.get('Contract_Status', 'OK')
    is_leap_fallback = (contract_status == 'LEAP_FALLBACK')
    
    delta = row.get('Delta')
    gamma = row.get('Gamma')
    vega = row.get('Vega')
    pcs_score = row.get('PCS_Final', row.get('PCS_Score_V2', 50))
    actual_dte = row.get('Actual_DTE', row.get('DTE', 45))
    
    # Trend/momentum data (Murphy)
    price_vs_sma20 = row.get('Price_vs_SMA20')
    price_vs_sma50 = row.get('Price_vs_SMA50')
    trend = row.get('Trend') or row.get('Signal_Type')
    volume_trend = row.get('Volume_Trend')
    
    # Pattern data (Bulkowski)
    chart_pattern = row.get('Chart_Pattern')
    pattern_confidence = row.get('Pattern_Confidence')
    
    # Entry timing data (Nison)
    candlestick_pattern = row.get('Candlestick_Pattern')
    entry_timing = row.get('Entry_Timing_Quality')
    reversal_confirmation = row.get('Reversal_Confirmation')
    
    missing = []
    notes = []
    
    # 1. Data completeness check
    if pd.isna(delta):
        missing.append('Delta')
    if pd.isna(gamma):
        missing.append('Gamma')
    if pd.isna(vega):
        missing.append('Vega')
    
    if missing:
        return ('Incomplete_Data', 33.0, ', '.join(missing), 0.0,
                f"Missing Greeks: {', '.join(missing)} (REQUIRED for directional)")
    
    data_completeness = 100.0
    
    # 2. Theory compliance check (RAG requirements)
    compliance_score = 100.0
    abs_delta = abs(delta)
    
    # RAG: Passarelli - "Delta without Gamma is noise"
    if abs_delta < 0.45:
        compliance_score -= 30
        notes.append(f"Weak Delta ({abs_delta:.2f} < 0.45)")

    # Gamma threshold: LEAPs at 180+ DTE naturally have low gamma (vega dominates).
    # Passarelli: ATM gamma ≈ 0.092 at 92 DTE → falls to ~0.01-0.02 at 365 DTE.
    # Do NOT penalise LEAPs for having low gamma — it's a structural feature, not a flaw.
    is_leap = 'leap' in strategy.lower() or (pd.notna(actual_dte) and actual_dte >= 180)
    if is_leap:
        gamma_floor = 0.008  # LEAPs: vega play, gamma is intentionally low
        gamma_label = "LEAP"
    else:
        gamma_floor = 0.03   # Short-dated directionals: need convexity (Passarelli Ch.4)
        gamma_label = "directional"
    if gamma < gamma_floor:
        compliance_score -= 30
        notes.append(f"Low Gamma ({gamma:.3f} < {gamma_floor} - insufficient convexity for {gamma_label}; Passarelli Ch.4)")
    else:
        notes.append(f"✅ Gamma {gamma:.3f} ≥ {gamma_floor} (convexity adequate for {gamma_label})")

    # Check for weak conviction (both conditions) — applies to all directionals including LEAPs
    weak_gamma = 0.004 if is_leap else 0.02
    if abs_delta < 0.30 and gamma < weak_gamma:
        compliance_score -= 20
        notes.append("Weak conviction (low Delta + low Gamma = coin flip)")
    
    # Vega check (need adjustment potential)
    if vega < 0.18:
        compliance_score -= 10
        notes.append(f"Low Vega ({vega:.2f} - limited adjustment potential)")
    
    # Murphy: Trend alignment check
    if pd.notna(trend):
        if strategy in ['Long Call', 'Bull Call Spread']:
            if trend not in ['Bullish', 'Sustained Bullish']:
                compliance_score -= 25
                notes.append(f"Trend misalignment ({trend} - RAG: Murphy Ch.4)")
            else:
                notes.append(f"✅ Trend aligned ({trend} - Murphy)")
        elif strategy in ['Long Put', 'Bear Put Spread']:
            if trend not in ['Bearish']:
                compliance_score -= 25
                notes.append(f"Trend misalignment ({trend} - RAG: Murphy Ch.4)")
            else:
                notes.append(f"✅ Trend aligned ({trend} - Murphy)")
    else:
        compliance_score -= 15
        notes.append("Missing trend data (Murphy Ch.4 - trend confirmation required)")
    
    # Murphy: Price structure check
    if pd.notna(price_vs_sma20):
        if strategy in ['Long Call', 'Bull Call Spread']:
            if price_vs_sma20 < 0:  # Price below SMA20 (bearish structure)
                compliance_score -= 20
                notes.append(f"Price below SMA20 ({price_vs_sma20:.2f} - Murphy: bearish structure)")
        elif strategy in ['Long Put', 'Bear Put Spread']:
            if price_vs_sma20 > 0:  # Price above SMA20 (bullish structure)
                compliance_score -= 20
                notes.append(f"Price above SMA20 ({price_vs_sma20:.2f} - Murphy: bullish structure)")
    
    # Murphy Ch.6: Volume confirmation (CRITICAL for directional strategies)
    if pd.notna(volume_trend):
        if strategy in ['Long Call', 'Bull Call Spread', 'Long Call LEAP']:
            if volume_trend in ['Rising', 'High', 'Increasing']:
                notes.append(f"✅ Volume confirms uptrend ({volume_trend} - Murphy Ch.6)")
            elif volume_trend in ['Falling', 'Low', 'Decreasing']:
                compliance_score -= 20
                notes.append(f"❌ Volume not supporting ({volume_trend} - Murphy Ch.6: weak breakout)")
            else:
                compliance_score -= 10
                notes.append(f"⚠️ Neutral volume ({volume_trend} - Murphy: breakout unconfirmed)")
        
        elif strategy in ['Long Put', 'Bear Put Spread', 'Long Put LEAP']:
            if volume_trend in ['Rising', 'High', 'Increasing']:
                notes.append(f"✅ Volume confirms downtrend ({volume_trend} - Murphy Ch.6)")
            else:
                compliance_score -= 15
                notes.append(f"⚠️ Volume weak ({volume_trend} - Murphy: sell-off unconvincing)")
    else:
        compliance_score -= 10
        notes.append("Volume data missing (Murphy Ch.6: volume confirmation REQUIRED for directional)")
    
    # Bulkowski: Pattern validation (statistical edge)
    if pd.notna(chart_pattern):
        if pd.notna(pattern_confidence) and pattern_confidence >= 70:
            compliance_score += 10  # Bonus for high-probability pattern
            notes.append(f"✅ Pattern confirmed: {chart_pattern} (Bulkowski: {pattern_confidence:.0f}% success rate)")
        elif pd.notna(pattern_confidence) and pattern_confidence >= 60:
            compliance_score += 5
            notes.append(f"✅ Pattern detected: {chart_pattern} (Bulkowski: {pattern_confidence:.0f}% success rate)")
        elif pd.notna(pattern_confidence) and pattern_confidence < 50:
            compliance_score -= 10
            notes.append(f"⚠️ Weak pattern ({chart_pattern}, {pattern_confidence:.0f}% - Bulkowski: low success rate)")
        else:
            notes.append(f"Pattern detected: {chart_pattern} (Bulkowski)")
    
    # Nison: Entry timing validation (for short-term strategies <30 DTE)
    is_short_term = pd.notna(actual_dte) and actual_dte < 30
    
    if is_short_term:
        # Short-term directionals require entry timing confirmation
        if pd.notna(candlestick_pattern):
            if entry_timing == 'Strong':
                compliance_score += 10
                notes.append(f"✅ Entry timing confirmed: {candlestick_pattern} (Nison: Strong reversal signal)")
            elif entry_timing == 'Moderate':
                compliance_score += 5
                notes.append(f"✅ Entry signal: {candlestick_pattern} (Nison: Moderate confirmation)")
            elif entry_timing == 'Weak':
                compliance_score -= 5
                notes.append(f"⚠️ Weak entry signal: {candlestick_pattern} (Nison: low confidence)")
        else:
            # Missing entry timing for short-term = risk
            compliance_score -= 10
            notes.append("⚠️ No candlestick confirmation (Nison: short-term entries need timing validation)")
    else:
        # Long-term strategies (LEAPs): timing less critical
        if pd.notna(candlestick_pattern) and entry_timing == 'Strong':
            compliance_score += 5
            notes.append(f"✅ Entry confirmed: {candlestick_pattern} (Nison: bonus confirmation)")
    
    # LEAP_FALLBACK penalty: reduce confidence for shorter-than-requested expiration
    if is_leap_fallback:
        requested_dte = row.get('Min_DTE', 365)
        fallback_penalty = 15  # Base penalty for using fallback
        if actual_dte < requested_dte * 0.5:  # Less than half requested
            fallback_penalty = 20
        compliance_score -= fallback_penalty
        notes.append(f"⚠️ LEAP_FALLBACK: Requested {requested_dte}+ DTE, using {actual_dte} DTE (confidence reduced by {fallback_penalty})")
    
    # 3. Determine validation status
    if compliance_score >= 70:
        status = 'Valid'
        notes.append(f"✅ Meets directional requirements (Delta={abs_delta:.2f}, Gamma={gamma:.3f})")
    elif compliance_score >= 50:
        status = 'Watch'
        notes.append(f"⚠️ Marginal directional setup (consider stronger conviction)")
    else:
        status = 'Reject'
        notes.append(f"❌ Fails directional requirements (RAG: Delta ≥0.45, Gamma ≥0.03)")
    
    return (status, data_completeness, '', compliance_score, ' | '.join(notes))


def _evaluate_volatility_strategy(row: pd.Series) -> Tuple[str, float, str, float, str]:
    """
    Evaluate volatility strategy (Straddle/Strangle).

    RAG Requirements (COMPLETE - 5 Books):

    Greek Requirements (Passarelli Ch.8, Natenberg Ch.15):
    - Vega ≥ 0.40 (high vol sensitivity, MANDATORY)
    - Gamma ≥ 0.06 combined (both legs long → positive gamma REQUIRED, Natenberg Ch.11)
    - Gamma/Theta ratio ≥ 0.5 (convexity reward justifies decay cost, Passarelli Ch.2)
    - Delta-neutral (|Delta| < 0.15, not directional bet)
    
    Skew & Smile (Hull Ch.20, Natenberg Ch.14):
    - Skew < 1.20 (HARD GATE - puts not overpriced)
    - ATM not systematically expensive vs wings
    
    Volatility Edge (Natenberg Ch.16, Sinclair Ch.3):
    - RV/IV ratio < 0.90 (buying cheap vol, statistical edge)
    - IV percentile 30-60 (expansion potential)
    - NOT buying elevated vol (IV > 70th percentile)
    
    Regime Gating (Sinclair Ch.2-4):
    - Volatility regime: Must be Compression or Low-Vol
    - NOT Expansion regime (already elevated)
    - Vol clustering risk: No recent vol spikes
    - Catalyst justification (earnings, event)
    
    Execution Realism (Hull Ch.19):
    - Liquidity adequate for both legs
    - Spread cost reasonable
    
    Returns:
        (validation_status, data_completeness_pct, missing_data, compliance_score, notes)
    """
    
    strategy = next(
        (v for v in (row.get('Strategy_Name'), row.get('Strategy'), row.get('Primary_Strategy'))
         if pd.notna(v) and v), ''
    )
    delta = row.get('Delta')
    gamma = row.get('Gamma')
    vega = row.get('Vega')
    theta = row.get('Theta')
    # NaN is truthy in Python — can't use `or` chains for fallback.
    # Use explicit pd.notna() checks to resolve the first available percentile.
    iv_percentile = next(
        (v for v in (row.get('IV_Percentile'), row.get('IV_Rank'),
                     row.get('IV_Rank_30D'), row.get('IV_Rank_XS'))
         if pd.notna(v)), None
    )
    # FIXED: Use exact column names from Step 9B implementation
    skew = row.get('Put_Call_Skew')
    rv_iv_ratio = row.get('RV_IV_Ratio')
    catalyst = next(
        (v for v in (row.get('Earnings_Days_Away'), row.get('Event_Risk'))
         if pd.notna(v)), None
    )

    # Sinclair: Regime data
    # Try IV-based regime first, then fall back to Step 0's HV-based volatility_regime
    vol_regime = next((v for v in (row.get('Volatility_Regime'), row.get('Regime')) if pd.notna(v) and v), None)
    if not vol_regime or vol_regime == 'Unknown':
        _hv_regime = str(row.get('volatility_regime') or '')
        # Map Step 0 HV-based regime names to canonical set
        _HV_REGIME_MAP = {
            'High_Expansion': 'Expansion', 'Normal_Expansion': 'Expansion',
            'High_Contraction': 'High Vol', 'High': 'High Vol',
            'Normal_Compression': 'Compression', 'High_Compression': 'Compression',
            'Low_Compression': 'Low Vol',
            'Normal_Contraction': 'Compression', 'Normal': 'Compression',
        }
        vol_regime = _HV_REGIME_MAP.get(_hv_regime) or vol_regime
    vvix = next((v for v in (row.get('VVIX'), row.get('Vol_of_Vol')) if pd.notna(v)), None)
    recent_vol_spike = row.get('Recent_Vol_Spike')  # Boolean: vol spike in last 5 days
    iv_term_structure = next((v for v in (row.get('Surface_Shape'), row.get('IV_Term_Structure')) if pd.notna(v)), None)

    missing = []
    notes = []

    # 1. Data completeness check
    required_data = {
        'Vega': vega,
        'Delta': delta,
        'Skew': skew,
        'IV_Percentile': iv_percentile
    }

    for name, value in required_data.items():
        if pd.isna(value):
            missing.append(name)

    # Calculate data completeness percentage
    data_completeness = ((len(required_data) - len(missing)) / len(required_data)) * 100

    # Vega is truly critical — can't evaluate a vol strategy without it
    if 'Vega' in missing:
        return ('Incomplete_Data', data_completeness, ', '.join(missing), 0.0,
                f"❌ CRITICAL data missing: {', '.join(missing)} (Vega REQUIRED for vol strategies)")
    # Skew missing → penalise compliance (not a hard block — skew data not always available)
    # When skew IS available, the hard gate at line 628 still rejects skew > 1.20
    
    # 2. Theory compliance check (RAG requirements)
    compliance_score = 100.0
    abs_delta = abs(delta) if pd.notna(delta) else 0.5
    
    # RAG CRITICAL: Skew check (HARD GATE when available)
    if pd.notna(skew) and skew > 1.20:
        # REJECT immediately - non-negotiable
        return ('Reject', data_completeness, '', 0.0,
                f"❌ SKEW VIOLATION: {skew:.2f} > 1.20 (puts overpriced - RAG: Passarelli Ch.8)")
    elif 'Skew' in missing:
        compliance_score -= 15
        notes.append("Skew data unavailable — cannot verify put/call parity (RAG: Passarelli Ch.8)")

    # Vega requirement
    if pd.notna(vega):
        if vega < 0.40:
            compliance_score -= 40
            notes.append(f"Low Vega ({vega:.2f} < 0.40 - weak vol sensitivity)")
    else:
        compliance_score -= 40
        notes.append("Missing Vega (cannot validate vol strategy)")

    # Gamma requirement (Natenberg Ch.11: straddles/strangles MUST have positive combined gamma)
    # Both legs are long, so gamma = call_gamma + put_gamma (Step10 computes this correctly).
    # A deep OTM strangle with near-zero gamma has no convexity — it won't profit from moves.
    #
    # Gamma is per-share — naturally scales inversely with stock price.
    # A $3700 stock (AZO) with Gamma=0.002 has dollar-gamma = $7.40, which is adequate.
    # Normalize: use dollar-gamma (Gamma × Stock_Price) with floor at $2.00.
    # Fallback to per-share floor if stock price unavailable.
    is_strangle = 'strangle' in strategy.lower()
    _stock_px = row.get('Stock_Price') or row.get('last_price') or row.get('closePrice')
    _stock_px_f = float(_stock_px) if pd.notna(_stock_px) and _stock_px else None
    if _stock_px_f and _stock_px_f > 0:
        # Dollar-gamma floor: $2.00 strangle, $3.00 straddle
        _dgamma_floor = 2.0 if is_strangle else 3.0
        _dollar_gamma = gamma * _stock_px_f if pd.notna(gamma) else 0
        gamma_floor_label = f"${_dgamma_floor:.0f} dollar-gamma"
    else:
        # Fallback to per-share floors (calibrated for ~$100 stocks)
        _dgamma_floor = None
        _dollar_gamma = None
        gamma_floor_label = None
    _per_share_floor = 0.04 if is_strangle else 0.06
    if pd.notna(gamma):
        if gamma <= 0:
            # Negative gamma on a long vol strategy is structurally wrong
            compliance_score -= 50
            notes.append(f"❌ NEGATIVE Gamma ({gamma:.3f}) on long vol strategy — structure incorrect (Natenberg Ch.11)")
        elif _dgamma_floor is not None:
            # Price-normalized check
            if _dollar_gamma < _dgamma_floor:
                compliance_score -= 20
                notes.append(f"Low dollar-Gamma (${_dollar_gamma:.2f} < ${_dgamma_floor:.0f}; Γ={gamma:.4f} × ${_stock_px_f:.0f})")
            else:
                notes.append(f"✅ Gamma adequate (${_dollar_gamma:.2f} dollar-gamma ≥ ${_dgamma_floor:.0f})")
        elif gamma < _per_share_floor:
            compliance_score -= 20
            notes.append(f"Low Gamma ({gamma:.3f} < {_per_share_floor}; stock price unknown, using per-share floor)")
        else:
            notes.append(f"✅ Gamma adequate ({gamma:.3f} ≥ {_per_share_floor})")
    else:
        compliance_score -= 20
        notes.append("Missing Gamma (cannot validate convexity for long vol strategy)")

    # Gamma/Theta ratio (Passarelli Ch.2: maximize gamma/theta for long vol — convexity per unit of decay)
    # Use dollar-gamma / |theta| when stock price available — raw gamma/theta is misleading
    # for high-priced stocks (AZO Γ=0.002 raw ratio=0.0006, but dollar-adjusted=2.34).
    if pd.notna(gamma) and pd.notna(theta) and theta != 0:
        abs_theta = abs(theta)
        if _stock_px_f and _stock_px_f > 0:
            # Dollar-gamma / dollar-theta ratio (price-normalized)
            gamma_theta_ratio = (gamma * _stock_px_f) / abs_theta if abs_theta > 1e-9 else 99.0
            _gt_label = f"dollar-gamma/theta {gamma_theta_ratio:.2f}"
            _gt_floor = 1.0  # $-gamma per $1 of daily theta
        else:
            gamma_theta_ratio = gamma / abs_theta if abs_theta > 1e-9 else 99.0
            _gt_label = f"gamma/theta {gamma_theta_ratio:.2f}"
            _gt_floor = 0.5
        if gamma_theta_ratio < _gt_floor:
            compliance_score -= 15
            notes.append(f"⚠️ Low {_gt_label} < {_gt_floor} — decay cost outweighs convexity (Passarelli Ch.2)")
        else:
            notes.append(f"✅ {_gt_label} ≥ {_gt_floor} (Passarelli: convexity justifies decay cost)")

    # Delta-neutral check
    if abs_delta > 0.15:
        compliance_score -= 20
        notes.append(f"Directional bias (|Delta|={abs_delta:.2f} > 0.15 - not neutral)")
    
    # IV edge check for long vol strategies
    # Low IV rank = cheap vol = good for buying. High IV rank = expensive = bad.
    # Sinclair Ch.3: sweet spot 20-60 (room to expand, not already elevated)
    if pd.notna(iv_percentile):
        if iv_percentile > 80:
            compliance_score -= 25
            notes.append(f"⚠️ Very high IV (IV%ile={iv_percentile:.0f} > 80 - buying expensive vol, limited expansion room)")
        elif iv_percentile > 60:
            compliance_score -= 10
            notes.append(f"Elevated IV (IV%ile={iv_percentile:.0f} > 60 - moderate expansion risk)")
        elif iv_percentile < 15:
            compliance_score -= 10
            notes.append(f"Very low IV (IV%ile={iv_percentile:.0f} < 15 - cheap but may stay compressed; needs catalyst)")
        else:
            notes.append(f"✅ IV in expansion zone (IV%ile={iv_percentile:.0f} — favorable for long vol)")
    else:
        compliance_score -= 10
        notes.append("Missing IV percentile (vol edge unvalidated)")
    
    # RV/IV ratio check (CRITICAL - Natenberg Ch.10)
    if pd.notna(rv_iv_ratio):
        if rv_iv_ratio > 1.15:
            # HARD GATE: No vol edge, expensive volatility
            return ('Reject', data_completeness, '', 0.0,
                    f"❌ NO VOL EDGE: RV/IV={rv_iv_ratio:.2f} > 1.15 (RAG: Natenberg Ch.10 - buying expensive vol)")
        elif rv_iv_ratio > 0.90:
            compliance_score -= 25
            notes.append(f"⚠️ Marginal vol edge (RV/IV={rv_iv_ratio:.2f} > 0.90 - barely favorable)")
        else:
            notes.append(f"✅ Strong vol edge (RV/IV={rv_iv_ratio:.2f} < 0.90 - Natenberg: IV significantly > RV)")
    else:
        compliance_score -= 15
        notes.append("Missing RV/IV ratio (vol edge unvalidated - non-critical for straddles)")
    
    # Catalyst check — single penalty (Sinclair Ch.3 below handles straddle/strangle specifics)
    _is_straddle_strangle = strategy in ['Long Straddle', 'Long Strangle', 'Straddle', 'Strangle']
    if pd.isna(catalyst) and not _is_straddle_strangle:
        # Generic vol strategies without catalyst: mild penalty
        compliance_score -= 15
        notes.append("No catalyst identified (generic vol bet)")
    elif pd.notna(catalyst) and not _is_straddle_strangle:
        notes.append(f"✅ Catalyst present: {catalyst}")
    
    # Sinclair: Regime gating (CRITICAL)
    if pd.notna(vol_regime):
        if vol_regime in ['Expansion', 'High Vol']:
            compliance_score -= 30
            notes.append(f"❌ Wrong regime ({vol_regime} - Sinclair: don't buy elevated vol)")
        elif vol_regime in ['Compression', 'Low Vol']:
            notes.append(f"✅ Favorable regime ({vol_regime} - Sinclair Ch.3)")
        else:
            compliance_score -= 10
            notes.append(f"⚠️ Neutral regime ({vol_regime})")
    else:
        compliance_score -= 20
        notes.append("Missing vol regime (Sinclair Ch.2 - regime classification required)")
    
    # Sinclair Ch.4: Vol clustering risk (HARD GATE - enhanced implementation)
    if pd.notna(recent_vol_spike):
        if recent_vol_spike:
            # HARD GATE: Recent spike detected
            days_since = row.get('Days_Since_Vol_Spike', 0)
            if pd.notna(days_since) and days_since < 5:
                return ('Reject', data_completeness, '', 0.0,
                        f"❌ RECENT VOL SPIKE: {days_since:.0f} days ago (Sinclair Ch.4: wait for mean reversion)")
            elif pd.notna(days_since):
                compliance_score -= 15
                notes.append(f"⚠️ Vol spike {days_since:.0f} days ago (Sinclair: monitor for clustering)")
            else:
                # days_since not available but spike flag is True
                compliance_score -= 25
                notes.append("❌ Recent vol spike detected (Sinclair: clustering risk - wait for mean reversion)")
    
    # Sinclair Ch.3: VVIX check (vol-of-vol uncertainty)
    if pd.notna(vvix):
        if vvix > 130:
            return ('Reject', data_completeness, '', 0.0,
                    f"❌ HIGH VVIX: {vvix:.0f} > 130 (Sinclair Ch.3: vol-of-vol too elevated, unpredictable)")
        elif vvix > 100:
            compliance_score -= 10
            notes.append(f"⚠️ Elevated VVIX ({vvix:.0f} - Sinclair: moderate vol uncertainty)")
        else:
            notes.append(f"✅ Normal VVIX ({vvix:.0f} - Sinclair: vol predictable)")
    
    # Sinclair Ch.3: Catalyst requirement (not optional for long vol strategies)
    # Single catalyst check for straddle/strangle — replaces generic check above
    if _is_straddle_strangle:
        if pd.isna(catalyst) or (pd.notna(catalyst) and catalyst > 30):
            compliance_score -= 20
            notes.append("⚠️ No near-term catalyst (Sinclair Ch.3: long vol benefits from event justification)")
        elif pd.notna(catalyst) and catalyst <= 30:
            notes.append(f"✅ Catalyst present: {catalyst:.0f} days (Sinclair: justified vol purchase)")
    
    # Sinclair: Term structure check
    if pd.notna(iv_term_structure):
        ts_upper = str(iv_term_structure).upper()
        if ts_upper == 'INVERTED':
            compliance_score -= 10
            notes.append("⚠️ Inverted term structure (Sinclair: front vol overpriced, monitor)")
        elif ts_upper == 'CONTANGO':
            notes.append("✅ Normal term structure (Sinclair: favorable for long vol)")
    
    # 3. Determine validation status
    gamma_str = f", Gamma={gamma:.3f}" if pd.notna(gamma) else ""
    if compliance_score >= 70:
        status = 'Valid'
        notes.insert(0, f"✅ Meets vol strategy requirements (Vega={vega:.2f}, Skew={skew:.2f}{gamma_str})")
    elif compliance_score >= 50:
        status = 'Watch'
        notes.insert(0, f"⚠️ Marginal vol setup (consider stronger edge)")
    else:
        status = 'Reject'
        notes.insert(0, f"❌ Fails vol strategy requirements (RAG violations)")

    return (status, data_completeness, ', '.join(missing) if missing else '',
            compliance_score, ' | '.join(notes))


def _evaluate_income_strategy(row: pd.Series) -> Tuple[str, float, str, float, str]:
    """
    Evaluate income strategy (CSP, Covered Call, Buy-Write).

    RAG Requirements (COMPLETE - 4 Books):

    Premium Collection Edge (Cohen Ch.28, Natenberg Ch.16):
    - IV > RV (selling expensive volatility, statistical edge)
    - IV percentile > 50 (elevated premium)
    - NOT selling during compression (premium too cheap)

    Greek Profile (Passarelli, Natenberg):
    - Theta > 0 (positive — time decay works FOR seller)
    - Gamma ≤ 0 (negative — short option creates negative gamma; Natenberg Ch.7)
    - Short-DTE warning: gamma spikes under 21 DTE → gap risk (Passarelli Ch.2)
    - Defined risk profile

    Probability Realism (Cohen Ch.28):
    - POP ≥ 65% (probability of profit, not 50/50)
    - Tail risk acceptable (max loss < 20× premium)
    - Win rate awareness (10 wins can't be wiped by 1 loss)

    Market Structure (Murphy Ch.4 - for directional income):
    - CSP: Bullish structure (price above SMA20)
    - Covered Call: Neutral-to-bullish structure
    - Buy-Write: Entry price reasonable (not chasing)

    Returns:
        (validation_status, data_completeness_pct, missing_data, compliance_score, notes)
    """

    strategy = next(
        (v for v in (row.get('Strategy_Name'), row.get('Strategy'), row.get('Primary_Strategy'))
         if pd.notna(v) and v), ''
    )
    theta = row.get('Theta')
    vega = row.get('Vega')
    gamma = row.get('Gamma')
    iv_hv_gap = row.get('IVHV_gap_30D') or row.get('IV_HV_Gap')
    # FIXED: Use exact column name from Step 9B implementation
    pop = row.get('Probability_Of_Profit')
    pcs_score = row.get('PCS_Final', row.get('PCS_Score_V2', 50))
    # NEW: RV/IV ratio for premium selling validation
    rv_iv_ratio = row.get('RV_IV_Ratio')
    iv_percentile = next(
        (v for v in (row.get('IV_Percentile'), row.get('IV_Rank'),
                     row.get('IV_Rank_30D'), row.get('IV_Rank_XS'))
         if pd.notna(v)), None
    )

    actual_dte = row.get('Actual_DTE', row.get('DTE'))

    # Murphy: Market structure (for directional income strategies)
    price_vs_sma20 = row.get('Price_vs_SMA20')
    trend = row.get('Trend') or row.get('Signal_Type')
    volume_trend = row.get('Volume_Trend')
    
    missing = []
    notes = []
    
    # 1. Data completeness check
    if pd.isna(theta):
        missing.append('Theta')
    if pd.isna(vega):
        missing.append('Vega')
    if pd.isna(iv_hv_gap):
        missing.append('IV_HV_Gap')
    
    if missing:
        data_completeness = ((3 - len(missing)) / 3) * 100
        return ('Incomplete_Data', data_completeness, ', '.join(missing), 0.0,
                f"Missing required data: {', '.join(missing)}")
    
    data_completeness = 100.0
    
    # 2. Theory compliance check
    compliance_score = 100.0
    abs_theta = abs(theta)
    
    # RV/IV ratio check (CRITICAL for premium selling - Cohen Ch.28, Natenberg Ch.16, Sinclair)
    # RV/IV = HV / IV. Selling edge = IV > RV = RV/IV < 1.0 (premium is expensive).
    # Sinclair: "Sell vol when IV > RV" → RV/IV < 1.0 is the signal to SELL.
    # Hard block only when RV >> IV (HV much greater than IV = premium is CHEAP, no edge).
    if pd.notna(rv_iv_ratio):
        if rv_iv_ratio > 1.10:
            # HARD GATE: HV > IV by 10%+ — selling cheap premium, no statistical edge
            return ('Reject', data_completeness, '', 0.0,
                    f"❌ NO EDGE: RV/IV={rv_iv_ratio:.2f} > 1.10 (Sinclair: HV > IV — premium is cheap, don't sell)")
        elif rv_iv_ratio > 1.00:
            compliance_score -= 20
            notes.append(f"⚠️ Marginal edge (RV/IV={rv_iv_ratio:.2f} > 1.0 — HV slightly above IV)")
        elif rv_iv_ratio < 0.80:
            notes.append(f"✅ Strong premium edge (RV/IV={rv_iv_ratio:.2f} — IV significantly elevated vs HV, Sinclair: optimal sell zone)")
        else:
            notes.append(f"✅ Premium edge confirmed (RV/IV={rv_iv_ratio:.2f} — IV > HV, Natenberg: sell vol)")
    elif iv_hv_gap is not None:
        # Fallback to IV/HV gap if RV/IV not available
        if iv_hv_gap <= 0:
            compliance_score -= 30
            notes.append(f"IV ≤ RV (gap={iv_hv_gap:.1f} - not selling rich premium)")
        else:
            notes.append(f"✅ IV > RV (gap={iv_hv_gap:.1f} - premium collection justified)")
    else:
        compliance_score -= 25
        notes.append("Missing RV/IV data (cannot validate premium selling edge - CRITICAL)")
    
    # Theta/Vega check for income position
    # For short calls/puts, the position is SHORT vega and LONG theta (contract greeks are reversed).
    # Contract-level |theta| < vega is normal for 30-60 DTE options and not penalized for income sellers.
    # Only penalize if vega >> theta (>3×), indicating extreme vol sensitivity even for a short position.
    if abs_theta > 0 and vega / abs_theta > 3.0:
        compliance_score -= 10
        notes.append(f"High vega/theta ratio ({vega/abs_theta:.1f}×) — short position has elevated vol exposure")
    else:
        notes.append(f"✅ Acceptable greek profile for income: θ={abs_theta:.3f}, V={vega:.3f} (position is short call)")

    # Gamma sign check (Natenberg Ch.7: short options MUST have negative gamma)
    # Buy-Write has stock (delta=1.0, gamma=0) + short call (negative gamma) → net negative gamma.
    # A positive gamma reading on an income position indicates the contract is structured as a long
    # option (e.g. data inversion or wrong leg sign) — flag it.
    if pd.notna(gamma):
        if gamma > 0.05:
            # Clearly positive gamma on what should be a short-option structure
            compliance_score -= 20
            notes.append(f"⚠️ Positive Gamma ({gamma:.3f}) on short premium position — verify contract structure (Natenberg Ch.7: income sellers have negative gamma)")
        elif gamma > 0:
            # Marginal positive — could be rounding or near-ATM imprecision, soft warning only
            notes.append(f"ℹ️ Near-zero positive Gamma ({gamma:.3f}) — confirm short-option leg (income sellers expect γ < 0)")
        else:
            notes.append(f"✅ Negative Gamma ({gamma:.3f}) — confirms short premium structure (Natenberg Ch.7)")

        # Short-DTE gamma spike warning (Passarelli Ch.2: under 21 DTE, short-option gamma spikes)
        # This creates accelerating delta exposure — a gap move can blow through the short strike rapidly.
        if pd.notna(actual_dte) and actual_dte < 21 and gamma < -0.05:
            compliance_score -= 15
            notes.append(f"⚠️ Short DTE ({actual_dte:.0f}d) + high short gamma ({gamma:.3f}) — gap risk elevated (Passarelli Ch.2: gamma spikes <21 DTE)")
        elif pd.notna(actual_dte) and actual_dte < 21:
            notes.append(f"ℹ️ DTE {actual_dte:.0f}d < 21 — monitor short gamma exposure as expiry approaches")
    
    # POP check (CRITICAL - Cohen Ch.28: "Without POP, you're selling insurance without actuarial tables")
    if pd.notna(pop):
        if pop < 65:
            # HARD GATE: Low probability of profit
            return ('Reject', data_completeness, '', 0.0,
                    f"❌ LOW POP: {pop:.1f}% < 65% (RAG: Cohen Ch.28 - income strategies require ≥65% win rate)")
        elif pop < 70:
            compliance_score -= 10
            notes.append(f"⚠️ Marginal POP ({pop:.1f}% - Cohen: acceptable but low)")
        else:
            notes.append(f"✅ Strong POP ({pop:.1f}% ≥ 70% - Cohen: high-probability income trade)")
    else:
        # POP missing: reduced penalty for covered calls (undefined risk, POP model not applicable)
        # Cohen Ch.28 POP framework applies to defined-risk income trades (CSP, iron condor).
        # Covered calls have stock ownership as the hedge — POP not computed but acceptable.
        if strategy in ['Covered Call', 'Buy-Write']:
            compliance_score -= 5
            notes.append("POP not calculated for covered call (stock ownership is the hedge — Cohen: acceptable)")
        else:
            compliance_score -= 25
            notes.append("❌ POP not calculated (Cohen Ch.28: win rate validation REQUIRED for premium selling)")
    
    # Murphy: Market structure alignment (for directional income)
    if strategy in ['Cash-Secured Put', 'CSP']:
        # CSP = bullish structure required
        if pd.notna(trend):
            if trend not in ['Bullish', 'Sustained Bullish']:
                compliance_score -= 20
                notes.append(f"CSP in {trend} trend (Murphy: requires bullish structure)")
        if pd.notna(price_vs_sma20) and price_vs_sma20 < 0:
            compliance_score -= 15
            notes.append("CSP: price below SMA20 (Murphy: weak structure)")
    
    elif strategy in ['Covered Call', 'Buy-Write']:
        # Covered Call = neutral-to-bullish structure
        if pd.notna(trend) and trend == 'Bearish':
            compliance_score -= 25
            notes.append(f"Covered Call in bearish trend (Murphy: structural risk)")
    
    # 3. Determine validation status
    if compliance_score >= 70:
        status = 'Valid'
        notes.insert(0, f"✅ Meets income strategy requirements")
    elif compliance_score >= 50:
        status = 'Watch'
        notes.insert(0, f"⚠️ Marginal income setup")
    else:
        status = 'Reject'
        notes.insert(0, f"❌ Fails income strategy requirements")
    
    return (status, data_completeness, '', compliance_score, ' | '.join(notes))


def _rank_within_families(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rank strategies within their own families (NOT cross-family).
    
    Each strategy family gets independent ranking:
    - Best directional (highest theory compliance among directionals)
    - Best volatility strategy (highest theory compliance among vol)
    - Best income strategy (highest theory compliance among income)
    
    Args:
        df: Evaluated strategies
    
    Returns:
        DataFrame with Strategy_Family_Rank column
    """
    
    df_ranked = df.copy()
    df_ranked['Strategy_Family'] = ''
    df_ranked['Strategy_Family_Rank'] = 0
    
    # Classify strategies into families
    for idx, row in df_ranked.iterrows():
        strategy = next(
        (v for v in (row.get('Strategy_Name'), row.get('Strategy'), row.get('Primary_Strategy'))
         if pd.notna(v) and v), ''
    )
        
        if strategy in DIRECTIONAL_STRATEGIES:
            df_ranked.at[idx, 'Strategy_Family'] = 'Directional'
        elif strategy in VOLATILITY_STRATEGIES:
            df_ranked.at[idx, 'Strategy_Family'] = 'Volatility'
        elif strategy in INCOME_STRATEGIES:
            df_ranked.at[idx, 'Strategy_Family'] = 'Income'
        else:
            df_ranked.at[idx, 'Strategy_Family'] = 'Other'
    
    # Rank within each family (by Theory_Compliance_Score)
    for family in ['Directional', 'Volatility', 'Income', 'Other']:
        family_mask = df_ranked['Strategy_Family'] == family
        if family_mask.any():
            df_ranked.loc[family_mask, 'Strategy_Family_Rank'] = (
                df_ranked.loc[family_mask, 'Theory_Compliance_Score']
                .rank(method='dense', ascending=False)
                .astype(int)
            )
    
    logger.info(f"   📊 Ranked within families:")
    for family in ['Directional', 'Volatility', 'Income']:
        family_df = df_ranked[df_ranked['Strategy_Family'] == family]
        if not family_df.empty:
            rank1_count = len(family_df[family_df['Strategy_Family_Rank'] == 1])
            logger.info(f"      {family}: {len(family_df)} strategies, {rank1_count} top-ranked")
    
    return df_ranked


def _audit_independent_evaluation(df: pd.DataFrame) -> None:
    """
    Audit independent evaluation results.
    
    Args:
        df: Evaluated strategies
    """
    
    logger.info(f"    📊 Independent Evaluation Audit:")
    
    # Count by validation status (including new statuses)
    status_counts = df['Validation_Status'].value_counts().to_dict()
    total = len(df)
    
    for status in ['Valid', 'Watch', 'Deferred_DTE', 'Deferred_Liquidity', 'Pending_Greeks', 
                   'Blocked_No_IV', 'Blocked_No_Contracts', 'Reject', 'Incomplete_Data']:
        count = status_counts.get(status, 0)
        pct = (count / total * 100) if total > 0 else 0
        if count > 0:  # Only log statuses that exist
            logger.info(f"       {status}: {count} ({pct:.1f}%)")
    
    # Count LEAP_FALLBACK contracts separately
    if 'Contract_Status' in df.columns:
        leap_fallback = (df['Contract_Status'] == 'LEAP_FALLBACK').sum()
        if leap_fallback > 0:
            logger.info(f"       📌 LEAP_FALLBACK used: {leap_fallback} strategies")
    
    # Log rejection/deferral reasons (top 5)
    if 'Evaluation_Notes' in df.columns:
        blocked_statuses = ['Reject', 'Blocked_No_IV', 'Blocked_No_Contracts', 'Deferred_DTE', 'Deferred_Liquidity']
        blocked_df = df[df['Validation_Status'].isin(blocked_statuses)]
        if len(blocked_df) > 0:
            rejection_reasons = blocked_df['Evaluation_Notes'].str.split(' | ').str[0].value_counts().head(5)
            if len(rejection_reasons) > 0:
                logger.info("    📋 Top rejection/deferral reasons:")
                for reason, count in rejection_reasons.items():
                    reason_short = reason[:80] + '...' if len(reason) > 80 else reason
                    logger.info(f"       • {reason_short}: {count}")
    
    # Count by strategy family
    if 'Strategy_Family' in df.columns:
        family_counts = df['Strategy_Family'].value_counts()
        logger.info(f"    📊 By Strategy Family:")
        for family, count in family_counts.items():
            valid_count = len(df[(df['Strategy_Family'] == family) & (df['Validation_Status'] == 'Valid')])
            logger.info(f"       {family}: {count} total, {valid_count} valid")
    else:
        # Use Strategy column if Strategy_Family not available
        if 'Strategy' in df.columns:
            strategy_counts = df['Strategy'].value_counts().head(10)
            logger.info(f"    📊 By Strategy (top 10):")
            for strategy, count in strategy_counts.items():
                valid_count = len(df[(df['Strategy'] == strategy) & (df['Validation_Status'] == 'Valid')])
                logger.info(f"       {strategy}: {count} total, {valid_count} valid")
    
    # Average data completeness
    avg_completeness = df['Data_Completeness_Pct'].mean()
    logger.info(f"    📊 Avg Data Completeness: {avg_completeness:.1f}%")
    
    # Count IV dependency
    if 'IV_30_D_Call' in df.columns:
        has_iv = df['IV_30_D_Call'].notna().sum()
        missing_iv = df['IV_30_D_Call'].isna().sum()
        missing_iv_pct = (missing_iv / total * 100) if total > 0 else 0
        logger.info(f"    📊 IV Status: {has_iv} have IV, {missing_iv} missing ({missing_iv_pct:.1f}%)")
        
        # Count how many are blocked due to missing IV
        blocked_no_iv = (df['Validation_Status'] == 'Blocked_No_IV').sum()
        if blocked_no_iv > 0:
            blocked_pct = (blocked_no_iv / total * 100)
            logger.info(f"    ⚠️  Blocked by missing IV: {blocked_no_iv} ({blocked_pct:.1f}%)")
    
    # Average theory compliance (for valid/watch only)
    valid_watch = df[df['Validation_Status'].isin(['Valid', 'Watch'])]
    if not valid_watch.empty:
        avg_compliance = valid_watch['Theory_Compliance_Score'].mean()
        logger.info(f"    📊 Avg Theory Compliance: {avg_compliance:.1f} (valid/watch only)")


def _log_evaluation_summary(df: pd.DataFrame, user_goal: str) -> None:
    """
    Log summary of evaluation results.
    
    Args:
        df: Evaluated strategies
        user_goal: User's stated goal (for context, not used in scoring)
    """
    
    logger.info(f"   📊 Evaluation Summary:")
    
    # Valid strategies by family
    valid_strategies = df[df['Validation_Status'] == 'Valid']
    logger.info(f"      Valid Strategies: {len(valid_strategies)}")
    
    if not valid_strategies.empty:
        for family in ['Directional', 'Volatility', 'Income']:
            family_valid = valid_strategies[valid_strategies['Strategy_Family'] == family]
            if not family_valid.empty:
                top_strategy = family_valid.nsmallest(1, 'Strategy_Family_Rank').iloc[0]
                strategy_name = top_strategy.get('Strategy') or top_strategy.get('Primary_Strategy', '')
                ticker = top_strategy.get('Ticker', '')
                compliance = top_strategy['Theory_Compliance_Score']
                logger.info(f"      Best {family}: {ticker} {strategy_name} (compliance: {compliance:.0f})")
    
    # Data quality issues
    incomplete = df[df['Validation_Status'] == 'Incomplete_Data']
    if not incomplete.empty:
        logger.info(f"      ⚠️ Incomplete Data: {len(incomplete)} strategies")
        missing_data_summary = incomplete['Missing_Required_Data'].value_counts().head(3)
        for data, count in missing_data_summary.items():
            logger.info(f"         {data}: {count} occurrences")
    
    # User goal context (informational only)
    logger.info(f"   💡 User Goal: {user_goal} (will guide portfolio allocation, not scoring)")
    logger.info(f"      All valid strategies available regardless of goal")
    logger.info(f"      Portfolio layer (future) will allocate based on goal + risk tolerance")


# ============================================================
# BACKWARD COMPATIBILITY
# ============================================================

def compare_and_rank_strategies(
    df: pd.DataFrame,
    user_goal: str = 'income',
    account_size: float = 10000.0,
    risk_tolerance: str = 'moderate'
) -> pd.DataFrame:
    """
    DEPRECATED: Legacy function name for backward compatibility.
    
    Now calls evaluate_strategies_independently() with strategy isolation.
    
    This function redirects to the new independent evaluation model.
    Old code expecting Comparison_Score and Strategy_Rank will break
    (intentionally - those concepts violate RAG principles).
    """
    
    logger.warning("⚠️ compare_and_rank_strategies() is DEPRECATED")
    logger.warning("   Redirecting to evaluate_strategies_independently()")
    logger.warning("   Comparison_Score and Strategy_Rank columns removed (RAG violation)")
    
    return evaluate_strategies_independently(
        df,
        user_goal=user_goal,
        account_size=account_size,
        risk_tolerance=risk_tolerance
    )
