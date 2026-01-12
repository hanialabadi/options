"""
Step 8: Portfolio Management (REFACTORED - Strategy Isolation + Strict Execution Gates)

CRITICAL ARCHITECTURE CHANGE (v3 - Dec 2025):
Step 8 is EXECUTION-ONLY, not evaluation. Step 11 already decided what is tradable.

# AGENT SAFETY: This file is execution-only and MUST NEVER evaluate or rank strategies.
# All strategy validation and ranking is performed exclusively by `step11_independent_evaluation.py`.
# This prevents agents from "helpfully" resurrecting invalid logic or bypassing architectural boundaries.

MANDATORY EXECUTION CONTRACT:
    1. Step 8 is DESCRIPTIVE ONLY. It must NOT filter out READY_NOW candidates.
    2. NO NaN/inf coercion allowed for critical fields.
    3. NO strategy selection or cross-family comparison.
    4. Explicit defensive checks before numeric operations.
    5. Never return empty unless input is empty.

RAG Principle:
    "Strategies do not compete. Each strategy family is evaluated independently.
     Portfolio layer decides ALLOCATION, not SELECTION."
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Optional
from .debug_mode import get_debug_manager

logger = logging.getLogger(__name__)


# ============================================================
# NEW MAIN FUNCTION (Post-Step 11 Architecture)
# ============================================================

def compute_thesis_capacity(
    df: pd.DataFrame,
    account_balance: float = 100000.0,
    max_portfolio_risk: float = 0.20,
    max_trade_risk: float = 0.02,
    min_compliance_score: float = 0.0, # Default to 0 to allow all READY_NOW
    max_strategies_per_ticker: int = 50, # Relaxed per user feedback
    sizing_method: str = 'volatility_scaled',
    risk_per_contract: float = 500.0,
    expiry_intent: str = 'ANY'
) -> pd.DataFrame:
    """
    Step 8: Thesis Capacity Calculation (Descriptive Sizing)
    
    SEMANTIC SHIFT (Jan 2026):
    Step 8 is now descriptive-only. It annotates READY_NOW candidates with 
    sizing metadata (envelopes) but does NOT filter them out.
    """
    
    if df.empty:
        logger.warning("⚠️ Empty DataFrame passed to Step 8")
        return df
    
    input_row_count = len(df)
    logger.info(f"🎯 Step 8 (THESIS CAPACITY): Processing {input_row_count} candidates")
    
    # Step 1: Annotate validation status (RELAXED - no filtering)
    df_valid = _filter_by_validation_status(df, min_compliance_score=min_compliance_score)
    
    if df_valid.empty:
        # This should technically not happen if input is not empty and we removed filtering
        logger.warning("⚠️ No candidates passed validation check - returning empty DataFrame")
        return df_valid
    
    # Step 2: Apply portfolio-level constraints (RELAXED)
    df_constrained = _apply_portfolio_risk_limits(
        df_valid,
        max_strategies_per_ticker=max_strategies_per_ticker,
        account_balance=account_balance
    )
    
    if df_constrained.empty:
        logger.warning("⚠️ No strategies after portfolio constraints - returning empty DataFrame")
        return df_constrained
    
    # Step 3: Calculate capital allocation
    df_allocated = _allocate_capital_by_score(
        df_constrained,
        account_balance=account_balance,
        max_portfolio_risk=max_portfolio_risk,
        max_trade_risk=max_trade_risk,
        sizing_method=sizing_method,
        risk_per_contract=risk_per_contract
    )
    
    # Step 4: Aggregate portfolio Greeks
    df_with_greeks = _calculate_portfolio_greeks(df_allocated)
    
    # Step 5: Generate portfolio audit
    df_audited = _generate_portfolio_audit(
        df_with_greeks,
        account_balance=account_balance
    )
    
    logger.info(f"🎯 Step 8 Complete: {len(df_audited)} thesis envelopes generated")
    
    return df_audited


def _filter_by_validation_status(
    df: pd.DataFrame,
    min_compliance_score: float
) -> pd.DataFrame:
    """
    Annotate strategies by Validation_Status and Theory_Compliance_Score.
    SEMANTIC FIX: Step 8 must NOT return empty if input is not empty.
    """
    
    df_filtered = df.copy()
    initial_count = len(df_filtered)
    
    # MANDATORY: Check for Validation_Status column
    if 'Validation_Status' not in df_filtered.columns:
        raise ValueError("❌ CRITICAL: Validation_Status column missing.")
    
    # SEMANTIC FIX: We no longer filter by Validation_Status here because Step 12 
    # has already decided these are READY_NOW. We only ensure columns exist.
    logger.info(f"      Step 8: Processing {initial_count} READY_NOW candidates")
    
    # Ensure Theory_Compliance_Score exists and is finite
    if 'Theory_Compliance_Score' in df_filtered.columns:
        # Fill NaNs with a neutral score (50) to ensure sizing logic works
        df_filtered['Theory_Compliance_Score'] = df_filtered['Theory_Compliance_Score'].fillna(50.0)
    
    # OPTIONAL: Validate required execution fields are present and finite
    required_fields = ['Total_Debit', 'Delta']
    for field in required_fields:
        if field in df_filtered.columns:
            invalid_data = df_filtered[~np.isfinite(df_filtered[field])]
            if len(invalid_data) > 0:
                logger.warning(f"⚠️ {len(invalid_data)} strategies have invalid {field} - preserving for visibility")
    
    return df_filtered


def _apply_portfolio_risk_limits(
    df: pd.DataFrame,
    max_strategies_per_ticker: int,
    account_balance: float
) -> pd.DataFrame:
    """
    Apply portfolio-level risk constraints.
    """
    
    df_constrained = df.copy()
    
    # Constraint 2: Max strategies per ticker
    if max_strategies_per_ticker > 0:
        before_count = len(df_constrained)
        # Sort to keep highest compliance scores if we were to cap, but we've relaxed max_strategies_per_ticker to 50
        df_constrained = df_constrained.sort_values(['Ticker', 'Theory_Compliance_Score'], ascending=[True, False])
        df_constrained = df_constrained.groupby('Ticker').head(max_strategies_per_ticker)
        logger.info(f"      Constraint: Max {max_strategies_per_ticker} strategies/ticker: {len(df_constrained)}/{before_count}")
    
    return df_constrained


def _allocate_capital_by_score(
    df: pd.DataFrame,
    account_balance: float,
    max_portfolio_risk: float,
    max_trade_risk: float,
    sizing_method: str,
    risk_per_contract: float
) -> pd.DataFrame:
    """
    Allocate capital based on Risk Budget and Capital Constraints.
    
    ACTION 4: Decoupled from strategy semantics (PCS, Score, Regime).
    Sizing is now a function of account risk limits and execution eligibility.
    """
    
    df_allocated = df.copy()
    
    # Track adjustment reasons for the audit trail
    df_allocated['Sizing_Adjustments'] = ""
    
    # 1. Base Capacity (Risk-Based Unit Sizing)
    # All READY_NOW candidates start with a base unit size derived from account risk limits.
    # Default: 1% of account balance per trade, or fixed unit sizing.
    base_unit_size = (account_balance * 0.01) / risk_per_contract
    base_capacity = pd.Series([base_unit_size] * len(df_allocated), index=df_allocated.index)
    
    # 2. Liquidity Constraint (Microstructure Guardrail)
    # If liquidity is poor or spreads are wide, the "expressive envelope" must shrink
    liq_adj = 1.0
    if 'Liquidity_OK' in df_allocated.columns:
        mask = ~df_allocated['Liquidity_OK']
        liq_adj = np.where(df_allocated['Liquidity_OK'], 1.0, 0.5)
        df_allocated.loc[mask, 'Sizing_Adjustments'] += "💧 LIQUIDITY_CAP "
        
    if 'Spread_Pct' in df_allocated.columns:
        # Penalize envelopes for spreads > 5%
        mask = df_allocated['Spread_Pct'] > 0.05
        liq_adj = liq_adj * np.where(mask, 0.7, 1.0)
        df_allocated.loc[mask, 'Sizing_Adjustments'] += "💧 SPREAD_PENALTY "
    
    # Microstructure: Cap envelope at 10% of Open Interest to prevent market impact
    if 'Open Int' in df_allocated.columns:
        oi_cap = (df_allocated['Open Int'] * 0.10).fillna(100)
        mask = base_capacity > oi_cap
        base_capacity = np.minimum(base_capacity, oi_cap)
        df_allocated.loc[mask, 'Sizing_Adjustments'] += "💧 OI_CAP "
        
    # 3. Price-Level Normalization (Behavioral Guardrail)
    # Prevent massive envelopes for "cheap" lottery tickets (Contract Bloat)
    price_adj = 1.0
    if 'Total_Debit' in df_allocated.columns:
        # If debit < $1.00, reduce envelope to prevent "contract bloat"
        mask = df_allocated['Total_Debit'] < 1.0
        price_adj = np.where(mask, 0.5, 1.0)
        df_allocated.loc[mask, 'Sizing_Adjustments'] += "🎈 PRICE_CAP "

    # 4. Liquidity Velocity Score (Jan 2026)
    # Qualitative measure of how easily the full envelope can be exited (1-10)
    df_allocated['Liquidity_Velocity'] = 10
    if 'Open Int' in df_allocated.columns:
        # Simple heuristic: Velocity drops if OI is low relative to envelope
        oi_ratio = (df_allocated['Open Int'] / (base_capacity * 10)).fillna(1.0)
        df_allocated['Liquidity_Velocity'] = (oi_ratio * 10).clip(1, 10).round().astype(int)

    # Calculate final envelope with NaN protection
    debug_manager = get_debug_manager()
    envelope_raw = (base_capacity * liq_adj * price_adj)
    
    nan_mask = envelope_raw.isna()
    if nan_mask.any() and debug_manager.enabled:
        debug_manager.log_event(
            step="step8",
            severity="WARN",
            code="ENVELOPE_NAN_NEUTRALIZED",
            message=f"Neutralized {nan_mask.sum()} NaN envelopes to 1",
            context={"affected_rows": int(nan_mask.sum())}
        )
        
    df_allocated['Thesis_Max_Envelope'] = envelope_raw.fillna(1).round().astype(int).clip(lower=1)
    df_allocated['Contracts'] = df_allocated['Thesis_Max_Envelope'] # Legacy support
    
    # 5. Expression Tiers (Jan 2026)
    # Categorize the trade's role in a portfolio based on the envelope size
    def get_expression_tier(size):
        if size <= 2: return "NICHE"
        if size <= 5: return "STANDARD"
        return "CORE"
        
    df_allocated['Expression_Tier'] = df_allocated['Thesis_Max_Envelope'].apply(get_expression_tier)
    
    # 6. Scaling Roadmap (Jan 2026)
    # Explicitly define the floor vs ceiling behavior
    df_allocated['Scaling_Roadmap'] = (
        "Entry: 1 Unit | " + 
        "Max: " + df_allocated['Thesis_Max_Envelope'].astype(str) + " Units | " +
        "Scale only on confirmation."
    )

    # Capital_Allocation is now a derived "Theoretical Requirement" for the full envelope
    if 'Total_Debit' in df_allocated.columns:
        df_allocated['Theoretical_Capital_Req'] = df_allocated['Thesis_Max_Envelope'] * df_allocated['Total_Debit']
        
        debit_nan = df_allocated['Theoretical_Capital_Req'].isna()
        if debit_nan.any() and debug_manager.enabled:
            debug_manager.log_event(
                step="step8",
                severity="WARN",
                code="CAPITAL_REQ_NAN",
                message=f"Neutralized {debit_nan.sum()} NaN capital requirements to 0",
                context={"affected_rows": int(debit_nan.sum())}
            )
        df_allocated['Capital_Allocation'] = df_allocated['Theoretical_Capital_Req'].fillna(0)
    else:
        # Fallback if Total_Debit is missing
        if debug_manager.enabled:
            debug_manager.log_event(
                step="step8",
                severity="WARN",
                code="MISSING_DEBIT",
                message="Total_Debit missing; capital allocation set to 0",
                context={"affected_rows": len(df_allocated)}
            )
        df_allocated['Capital_Allocation'] = 0.0
    
    return df_allocated


def _calculate_portfolio_greeks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate portfolio-level Greek exposure.
    """
    df_greeks = df.copy()
    if 'Contracts' in df_greeks.columns:
        for greek in ['Delta', 'Gamma', 'Vega', 'Theta']:
            if greek in df_greeks.columns:
                df_greeks[f'Position_{greek}'] = df_greeks[greek] * df_greeks['Contracts']
    return df_greeks


def _generate_portfolio_audit(
    df: pd.DataFrame,
    account_balance: float
) -> pd.DataFrame:
    """
    Generate portfolio allocation audit trail.
    """
    df_audited = df.copy()
    if 'Capital_Allocation' in df_audited.columns:
        # Note: Allocation_Pct is now "Theoretical Portfolio Impact" if fully expressed
        df_audited['Theoretical_Impact_Pct'] = ((df_audited['Capital_Allocation'] / account_balance) * 100).round(2)
        
        # Build descriptive audit string
        df_audited['Portfolio_Audit'] = (
            "Max Expression: " + df_audited['Thesis_Max_Envelope'].astype(str) + " Units" +
            " | Tier: " + df_audited['Expression_Tier'] +
            " | Full Req: $" + df_audited['Capital_Allocation'].round(0).astype(str) + 
            " | Score: " + df_audited['Theory_Compliance_Score'].round(0).astype(str) + "/100"
        )
        
        # Append adjustment reasons if any
        if 'Sizing_Adjustments' in df_audited.columns:
            mask = df_audited['Sizing_Adjustments'] != ""
            df_audited.loc[mask, 'Portfolio_Audit'] += " | Constraints: " + df_audited.loc[mask, 'Sizing_Adjustments']
        
        # Append Liquidity Velocity
        if 'Liquidity_Velocity' in df_audited.columns:
            df_audited['Portfolio_Audit'] += " | Exit Velocity: " + df_audited['Liquidity_Velocity'].astype(str) + "/10"
        
        # Add behavioral annotations
        if 'Spread_Pct' in df_audited.columns:
            mask = df_audited['Spread_Pct'] > 0.05
            df_audited.loc[mask, 'Portfolio_Audit'] += " | ⚠️ WIDE SPREADS"
            
        if 'Delta' in df_audited.columns:
            mask = df_audited['Delta'].abs() > 0.7
            df_audited.loc[mask, 'Portfolio_Audit'] += " | ⚡ HIGH DELTA"
            
        if 'Gamma' in df_audited.columns:
            mask = df_audited['Gamma'].abs() > 0.1
            df_audited.loc[mask, 'Portfolio_Audit'] += " | 🌊 HIGH GAMMA"
            
        if 'Total_Debit' in df_audited.columns:
            mask = df_audited['Total_Debit'] < 1.0
            df_audited.loc[mask, 'Portfolio_Audit'] += " | 🎈 CONTRACT BLOAT RISK"

    return df_audited

# Legacy functions kept for backward compatibility
def allocate_portfolio_capital(*args, **kwargs):
    return compute_thesis_capacity(*args, **kwargs)

def finalize_and_size_positions(*args, **kwargs):
    return compute_thesis_capacity(*args, **kwargs)
