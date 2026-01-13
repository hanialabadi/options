"""
LEGACY FILE: Step 7: Strategy Recommendation Engine (DEPRECATED)

⚠️  THIS FILE IS DEPRECATED AND SHOULD NOT BE USED.
    The authoritative strategy evaluation is now performed by `step11_independent_evaluation.py`.
    This file is preserved for historical context only.
    
    CANONICAL RULES: See STEP7_CANONICAL_RULES.md for authoritative system contract.
    This is a PRESCRIPTIVE step ("worth evaluating"), NOT EXECUTION ("execute now").

NOTE:
This is where PRESCRIPTIVE logic begins.
Steps 2-6 were purely descriptive. Step 7+ applies strategy logic,
trade recommendations, scoring, and actionable advice.

Purpose:
    Takes validated, descriptive market data from Steps 2-6 and applies
    strategy-specific logic to generate trade recommendations.
    
    This step:
    - Applies strategy selection rules (directional vs neutral)
    - Assigns confidence scores and success probabilities
    - Tags specific strategies (PCS, CCS, straddles, etc.)
    - Provides actionable trade recommendations
    
Design:
    Strategy logic is contained in Step 7+, keeping Steps 2-6 reusable
    for any strategy framework. This separation allows:
    - Testing strategies against same descriptive data
    - Swapping strategy engines without changing data pipeline
    - Clear boundary between observation and action
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)


def _validate_arrow_compatible_dtypes(df: pd.DataFrame, cols: List[str]) -> None:
    """
    Validate that DataFrame columns have Arrow-compatible dtypes.
    
    Prevents Streamlit Arrow serialization failures caused by object dtype corruption.
    """
    for col in cols:
        if col not in df.columns:
            continue
        dtype = df[col].dtype
        if dtype == 'object':
            raise TypeError(
                f"❌ DTYPE CORRUPTION DETECTED: Column '{col}' has object dtype. "
                f"This will cause Arrow serialization failure in Streamlit. "
                f"Expected: float64 or string. "
                f"Sample values: {df[col].head(3).tolist()}"
            )
        if col in ['Confidence', 'Success_Probability']:
            if dtype != 'float64':
                logger.warning(f"⚠️ Column '{col}' has dtype {dtype}, expected float64")
        else:
            if dtype != 'string':
                logger.warning(f"⚠️ Column '{col}' has dtype {dtype}, expected string")
    
    logger.info(f"✅ Arrow compatibility validated for {len(cols)} columns")



def recommend_strategies(
    df: pd.DataFrame,
    min_iv_rank: float = 50.0,
    min_ivhv_gap: float = 3.5,
    enable_directional: bool = True,
    enable_neutral: bool = True,
    enable_volatility: bool = True,
    tier_filter: str = 'tier1_only',
    exploration_mode: bool = False
) -> pd.DataFrame:
    # AGENT SAFETY: This function is DEPRECATED.
    # It performs legacy strategy ranking and comparison, which violates the new architecture.
    # All strategy evaluation is now handled by `step11_independent_evaluation.py`.
    raise RuntimeError(
        "❌ DEPRECATED FUNCTION CALL: `step7_strategy_recommendation_OLD.recommend_strategies()` "
        "is no longer valid. Refer to LEGACY.md for details. "
        "Use `step11_independent_evaluation.evaluate_strategies_independently()` instead."
    )
    """
    Generate strategy recommendations based on market context.
    
    This is the PRIMARY prescriptive step. All prior steps (2-6) were descriptive.
    
    🚨 SAFETY GATE: By default, outputs ONLY Tier-1 (executable) strategies.
    
    Args:
        df (pd.DataFrame): Validated data from Step 6
            Required columns: ['Ticker', 'IVHV_gap_30D', 'IV_Rank_30D', 
                              'Signal_Type', 'Regime', 'IV_Rich', 'IV_Cheap',
                              'MeanReversion_Setup', 'Expansion_Setup',
                              'Crossover_Age_Bucket', 'Data_Complete']
        min_iv_rank (float): Minimum IV rank for premium selling strategies (default: 50)
        min_ivhv_gap (float): Minimum IV-HV gap for volatility plays (default: 3.5)
        enable_directional (bool): Enable directional strategies (verticals, diagonals)
        enable_neutral (bool): Enable neutral strategies (IC, strangles, BWB)
        enable_volatility (bool): Enable volatility strategies (calendars, ratio spreads)
        tier_filter (str): 'tier1_only' (default, executable) | 'include_tier2' | 'all_tiers'
        exploration_mode (bool): If True, include all tiers with NON_EXECUTABLE flag (educational)
    
    Returns:
        pd.DataFrame: Original data with added strategy columns:
            - Primary_Strategy: Main recommended strategy
            - Secondary_Strategy: Alternative strategy
            - Strategy_Type: 'Directional', 'Neutral', 'Volatility', 'Mixed'
            - Confidence: 0-100 score (higher = stronger setup)
            - Success_Probability: Estimated probability of profit (0-1)
            - Trade_Bias: 'Bullish', 'Bearish', 'Neutral', 'Bidirectional'
            - Entry_Priority: 'High', 'Medium', 'Low'
            - Risk_Level: 'Low', 'Medium', 'High'
            - Strategy_Tier: 1 (executable), 2 (broker-blocked), 3 (logic-blocked)
            - EXECUTABLE: True/False (whether strategy can be executed)
    
    Theory-Backed Strategy Selection (RAG References):
        **Tier-1 Directional Strategies:**
        - Long Call/Put: Natenberg Ch.3 - Directional bias + HV > IV (cheap)
        - Cash-Secured Put: Passarelli - Bullish + IV > HV (rich premium)
        - Covered Call: Cohen - Bearish/Neutral + IV > HV (income on stock)
        - Buy-Write: Cohen Ch.7 - Bullish entry + Rich IV (reduce cost basis)
        
        **Tier-1 Volatility Strategies:**
        - Long Straddle: Natenberg Ch.9 - Expansion expected + IV_Rank < 30
        - Long Strangle: Natenberg Ch.9 - Expansion expected + IV_Rank < 40 (cheaper)
    
    Strategy Selection Logic:
        1. **Premium Selling (High IV):**
           - IV_Rank > 70 + IV_Rich → Naked puts, credit spreads, strangles
           - MeanReversion_Setup → Calendar spreads, ratio spreads
        
        2. **Premium Buying (Low IV):**
           - IV_Rank < 30 + IV_Cheap → Long calls/puts, debit spreads
           - Expansion_Setup → Straddles, long volatility
        
        3. **Directional (Trend + IV Context):**
           - Bullish signal + Age_0_5 + IV context → Call spreads, diagonals
           - Bearish signal + Age_0_5 + IV context → Put spreads, diagonals
        
        4. **Neutral (Range-bound + High IV):**
           - Regime='Ranging' + IV_Rich → Iron Condors, Butterflies
           - Base/Neutral signal + IV_Rank>60 → Short strangles, BWB
    
    Confidence Scoring:
        - High (80-100): Multiple aligned signals, strong IV edge, fresh crossover
        - Medium (60-79): Some alignment, moderate IV edge, aging signal
        - Low (40-59): Weak alignment, marginal IV edge, stale signal
    
    Example:
        >>> df_recommended = recommend_strategies(df_validated)
        >>> high_conf = df_recommended[df_recommended['Confidence'] >= 80]
        >>> print(f"High confidence setups: {len(high_conf)}")
    
    Usage Notes:
        - This is where strategy assumptions and trade intent are introduced
        - Confidence scores are subjective and should be backtested
        - Success probabilities are estimates, not guarantees
        - Users should apply their own risk management and position sizing
    """
    from .utils import validate_input
    
    # Validate required columns
    required_cols = [
        'Ticker', 'IVHV_gap_30D', 'IV_Rank_30D', 'Signal_Type', 
        'Regime', 'Crossover_Age_Bucket', 'Data_Complete'
    ]
    validate_input(df, required_cols, 'Step 7')
    
    # Work on complete data only
    df_complete = df[df['Data_Complete'] == True].copy()
    logger.info(f"🎯 Step 7: Generating strategies for {len(df_complete)}/{len(df)} tickers with complete data")
    
    if df_complete.empty:
        logger.warning("⚠️ No tickers with complete data for strategy recommendation")
        return df
    
    # Initialize strategy columns WITH PROPER DTYPES (prevents Arrow serialization failure)
    # CRITICAL: Use explicit dtypes to prevent pandas from upcasting to object dtype
    df_complete['Primary_Strategy'] = pd.Series('None', index=df_complete.index, dtype='string')
    df_complete['Secondary_Strategy'] = pd.Series('None', index=df_complete.index, dtype='string')
    df_complete['Strategy_Type'] = pd.Series('None', index=df_complete.index, dtype='string')
    df_complete['Confidence'] = pd.Series(0.0, index=df_complete.index, dtype='float64')
    df_complete['Success_Probability'] = pd.Series(0.0, index=df_complete.index, dtype='float64')
    df_complete['Trade_Bias'] = pd.Series('Neutral', index=df_complete.index, dtype='string')
    df_complete['Entry_Priority'] = pd.Series('Low', index=df_complete.index, dtype='string')
    df_complete['Risk_Level'] = pd.Series('Medium', index=df_complete.index, dtype='string')
    
    # Apply strategy selection logic
    for idx, row in df_complete.iterrows():
        strategy_result = _select_strategy(
            row,
            min_iv_rank=min_iv_rank,
            min_ivhv_gap=min_ivhv_gap,
            enable_directional=enable_directional,
            enable_neutral=enable_neutral,
            enable_volatility=enable_volatility
        )
        
        # Update row with recommendations
        for col, value in strategy_result.items():
            df_complete.at[idx, col] = value
    
    # Calculate success probabilities
    df_complete = _estimate_success_probability(df_complete)
    
    # Log summary
    _log_strategy_summary(df_complete)
    
    # 🚨 TIER-1 ENFORCEMENT (SAFETY GATE)
    # Determine strategy tier for each ticker (requires strategy tier lookup)
    from core.strategy_tiers import get_strategy_tier
    df_complete['Strategy_Tier'] = df_complete['Primary_Strategy'].apply(
        lambda s: get_strategy_tier(s).get('tier', 999) if s != 'None' else 999
    )
    
    # Check if any strategies were generated
    strategies_generated = (df_complete['Primary_Strategy'] != 'None').sum()
    if strategies_generated == 0:
        logger.warning("⚠️ No strategies generated! All Primary_Strategy values are 'None'")
        logger.warning("   Possible causes:")
        logger.warning("   - Input data missing required fields (Signal, chart_signal, IV data)")
        logger.warning("   - Signal strength too weak (all 'Weak' signals)")
        logger.warning("   - Filtering removed all strategies")
        logger.warning("   Returning empty DataFrame - check Step 6 GEM output")
    
    # Apply tier filtering based on mode
    if tier_filter == 'tier1_only' and not exploration_mode:
        # DEFAULT SAFETY MODE: Output only Tier-1 strategies
        total_count = len(df_complete)
        tier1_count = (df_complete['Strategy_Tier'] == 1).sum()
        df_complete = df_complete[df_complete['Strategy_Tier'] == 1].copy()
        logger.info(f"🔒 TIER-1 FILTER: {tier1_count}/{total_count} strategies are Tier-1 (executable)")
        logger.info(f"   Non-Tier-1 strategies excluded for safety (use exploration_mode=True to see all)")
    elif tier_filter == 'include_tier2':
        # Include Tier 1 and Tier 2
        df_complete = df_complete[df_complete['Strategy_Tier'].isin([1, 2])].copy()
        logger.info(f"📋 TIER-1+2 FILTER: Including Tier-1 and Tier-2 strategies")
    elif tier_filter == 'all_tiers' or exploration_mode:
        # Educational mode: include all tiers
        logger.info(f"📚 EXPLORATION MODE: Including all strategy tiers (educational only)")
    
    # Tag exploration data as NON-EXECUTABLE
    if exploration_mode or tier_filter != 'tier1_only':
        df_complete['EXECUTABLE'] = (df_complete['Strategy_Tier'] == 1).astype('bool')
        non_exec_count = (~df_complete['EXECUTABLE']).sum()
        logger.warning(f"⚠️ {non_exec_count} strategies tagged NON_EXECUTABLE (Tier 2/3)")
    else:
        # All remaining strategies are Tier-1, thus executable
        df_complete['EXECUTABLE'] = pd.Series(True, index=df_complete.index, dtype='bool')
    
    # Initialize strategy columns in main df WITH PROPER DTYPES (prevents Arrow serialization failure)
    # CRITICAL: Must initialize BEFORE merge to establish dtype contract
    strategy_cols = ['Primary_Strategy', 'Secondary_Strategy', 'Strategy_Type', 
                     'Confidence', 'Success_Probability', 'Trade_Bias', 
                     'Entry_Priority', 'Risk_Level', 'Strategy_Tier', 'EXECUTABLE']
    
    for col in strategy_cols:
        if col not in df.columns:
            if col in ['Confidence', 'Success_Probability']:
                df[col] = pd.Series(0.0, index=df.index, dtype='float64')
            elif col == 'Strategy_Tier':
                df[col] = pd.Series(999, index=df.index, dtype='int64')  # Initialize as int64, not string
            elif col == 'EXECUTABLE':
                df[col] = pd.Series(False, index=df.index, dtype='bool')
            else:
                df[col] = pd.Series('None', index=df.index, dtype='string')
    
    # Merge back with original dataframe using explicit merge (NOT df.update)
    # df.update() causes dtype corruption when mixing types - use merge instead
    df_result = df.drop(columns=strategy_cols, errors='ignore').merge(
        df_complete[['Ticker'] + strategy_cols],
        on='Ticker',
        how='left'
    )
    
    # Fill NaN values for incomplete data rows (maintain dtype integrity)
    for col in strategy_cols:
        if col in ['Confidence', 'Success_Probability']:
            df_result[col] = df_result[col].fillna(0.0).astype('float64')
        elif col == 'Strategy_Tier':
            df_result[col] = df_result[col].fillna(999).astype('int64')  # 999 = no tier assigned
        elif col == 'EXECUTABLE':
            df_result[col] = df_result[col].fillna(False).astype('bool')
        else:
            df_result[col] = df_result[col].fillna('None').astype('string')
    
    # FINAL DTYPE ENFORCEMENT: Ensure Arrow-compatible types before returning
    # This prevents Streamlit Arrow serialization errors
    df_result['Confidence'] = df_result['Confidence'].astype('float64')
    df_result['Success_Probability'] = df_result['Success_Probability'].astype('float64')
    df_result['Strategy_Tier'] = df_result['Strategy_Tier'].astype('int64')
    df_result['EXECUTABLE'] = df_result['EXECUTABLE'].astype('bool')
    for col in ['Primary_Strategy', 'Secondary_Strategy', 'Strategy_Type', 
                'Trade_Bias', 'Entry_Priority', 'Risk_Level']:
        df_result[col] = df_result[col].astype('string')
    
    logger.info(f"✅ Step 7: Final dtypes - {df_result[strategy_cols].dtypes.to_dict()}")
    
    # Validate Arrow compatibility (raises TypeError if object dtype detected)
    _validate_arrow_compatible_dtypes(df_result, strategy_cols)
    
    return df_result


# ==========================================
# INDEPENDENT STRATEGY VALIDATORS
# (Multi-Strategy Ledger Architecture)
# ==========================================
# Each validator returns None (invalid) or strategy dict (valid)
# NO if/elif chains - validators can run in any order
# Theory-explicit: Valid_Reason + Theory_Source per strategy

def _validate_long_call(ticker: str, row: pd.Series) -> Optional[Dict]:
    """
    Validate Long Call strategy.
    
    Theory: Natenberg Ch.3 - Directional with positive vega.
    Entry: Bullish signal + Cheap IV (gap < 0).
    """
    signal = row.get('Signal_Type', '')
    gap_180d = row.get('IVHV_gap_180D', 0)
    gap_60d = row.get('IVHV_gap_60D', 0)
    
    # Rejection criteria
    if signal not in ['Bullish', 'Sustained Bullish']:
        return None
    if gap_180d >= 0 and gap_60d >= 0:
        return None  # IV not cheap
    
    # Valid - return strategy
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Long Call',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Bullish + Cheap IV (gap_180d={gap_180d:.1f})",
        'Theory_Source': 'Natenberg Ch.3 - Directional with positive vega',
        'Regime_Context': signal,
        'IV_Context': f"gap_60d={gap_60d:.1f}, gap_180d={gap_180d:.1f}",
        'Capital_Requirement': 500,  # Approximate for 1 contract
        'Risk_Profile': 'Defined (max loss = premium paid)',
        'Greeks_Exposure': 'Long Delta, Long Vega, Short Theta',
        'Execution_Ready': True,
        'Confidence': 65,
        'Trade_Bias': 'Bullish',
        'Strategy_Type': 'Directional',
    }


def _validate_long_put(ticker: str, row: pd.Series) -> Optional[Dict]:
    """
    Validate Long Put strategy.
    
    Theory: Natenberg Ch.3 - Directional with positive vega.
    Entry: Bearish signal + Cheap IV (gap < 0).
    """
    signal = row.get('Signal_Type', '')
    gap_180d = row.get('IVHV_gap_180D', 0)
    gap_60d = row.get('IVHV_gap_60D', 0)
    
    # Rejection criteria
    if signal not in ['Bearish']:
        return None
    if gap_180d >= 0 and gap_60d >= 0:
        return None
    
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Long Put',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Bearish + Cheap IV (gap_180d={gap_180d:.1f})",
        'Theory_Source': 'Natenberg Ch.3 - Directional with negative delta',
        'Regime_Context': signal,
        'IV_Context': f"gap_60d={gap_60d:.1f}, gap_180d={gap_180d:.1f}",
        'Capital_Requirement': 500,
        'Risk_Profile': 'Defined (max loss = premium paid)',
        'Greeks_Exposure': 'Short Delta, Long Vega, Short Theta',
        'Execution_Ready': True,
        'Confidence': 65,
        'Trade_Bias': 'Bearish',
        'Strategy_Type': 'Directional',
    }


def _validate_csp(ticker: str, row: pd.Series) -> Optional[Dict]:
    """
    Validate Cash-Secured Put strategy.
    
    Theory: Passarelli - Premium collection when IV > HV.
    Entry: Bullish signal + Rich IV (gap > 0) + Moderate IV_Rank (≤70).
    """
    signal = row.get('Signal_Type', '')
    gap_30d = row.get('IVHV_gap_30D', 0)
    iv_rank = row.get('IV_Rank_30D', 50)
    
    # Rejection criteria
    if signal not in ['Bullish', 'Sustained Bullish']:
        return None
    if gap_30d <= 0:
        return None  # IV not rich
    if iv_rank > 70:
        return None  # Prefer Buy-Write when IV very rich
    
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Cash-Secured Put',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Bullish + Rich IV (gap_30d={gap_30d:.1f}, IV_Rank={iv_rank:.0f})",
        'Theory_Source': 'Passarelli - Premium collection when IV > HV',
        'Regime_Context': signal,
        'IV_Context': f"gap_30d={gap_30d:.1f}, IV_Rank={iv_rank:.0f}",
        'Capital_Requirement': 15000,  # Approximate for $150 stock
        'Risk_Profile': 'Obligation (max loss = strike - premium)',
        'Greeks_Exposure': 'Long Delta, Short Vega, Long Theta',
        'Execution_Ready': True,
        'Confidence': 70,
        'Trade_Bias': 'Bullish',
        'Strategy_Type': 'Directional',
    }


def _validate_covered_call(ticker: str, row: pd.Series) -> Optional[Dict]:
    """
    Validate Covered Call strategy.
    
    Theory: Passarelli - Premium collection on held stock.
    Entry: Bearish signal + Rich IV (gap > 0) + Stock ownership.
    
    NOTE: Returns valid strategy but marks Execution_Ready=False
    (requires manual confirmation of stock ownership).
    """
    signal = row.get('Signal_Type', '')
    gap_30d = row.get('IVHV_gap_30D', 0)
    
    # Rejection criteria
    if signal not in ['Bearish']:
        return None
    if gap_30d <= 0:
        return None
    
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Covered Call',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Bearish + Rich IV (gap_30d={gap_30d:.1f}) [requires stock ownership]",
        'Theory_Source': 'Passarelli - Premium collection on held stock',
        'Regime_Context': signal,
        'IV_Context': f"gap_30d={gap_30d:.1f}",
        'Capital_Requirement': 0,  # Assumes stock already held
        'Risk_Profile': 'Unlimited downside (stock ownership)',
        'Greeks_Exposure': 'Long Delta (from stock), Short Vega, Long Theta',
        'Execution_Ready': False,  # Requires stock ownership confirmation
        'Confidence': 70,
        'Trade_Bias': 'Bearish',
        'Strategy_Type': 'Directional',
    }


def _validate_buy_write(ticker: str, row: pd.Series) -> Optional[Dict]:
    """
    Validate Buy-Write strategy (stock + short call).
    
    Theory: Cohen Ch.7 - Buy stock + sell call when IV very rich.
    Entry: Bullish signal + Very Rich IV (IV_Rank > 70).
    """
    signal = row.get('Signal_Type', '')
    gap_30d = row.get('IVHV_gap_30D', 0)
    iv_rank = row.get('IV_Rank_30D', 50)
    
    # Rejection criteria
    if signal not in ['Bullish', 'Sustained Bullish']:
        return None
    if gap_30d <= 0:
        return None
    if iv_rank <= 70:
        return None  # Prefer CSP when IV moderately rich
    
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Buy-Write',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Bullish + Very Rich IV (IV_Rank={iv_rank:.0f})",
        'Theory_Source': 'Cohen Ch.7 - Reduces cost basis via call premium',
        'Regime_Context': signal,
        'IV_Context': f"gap_30d={gap_30d:.1f}, IV_Rank={iv_rank:.0f}",
        'Capital_Requirement': 50000,  # Approximate for $500 stock
        'Risk_Profile': 'Stock downside risk offset by call premium',
        'Greeks_Exposure': 'Long Delta (from stock), Short Vega, Long Theta',
        'Execution_Ready': True,
        'Confidence': 75,
        'Trade_Bias': 'Bullish',
        'Strategy_Type': 'Directional',
    }


def _validate_long_straddle(ticker: str, row: pd.Series) -> Optional[Dict]:
    """
    Validate Long Straddle strategy.
    
    Theory: Natenberg Ch.9 - Volatility buying when expecting expansion.
    Entry: Expansion setup + Very Cheap IV (IV_Rank < 35 OR gap_180d < -15).
    """
    expansion = row.get('Expansion_Setup', False)
    signal = row.get('Signal_Type', '')
    gap_180d = row.get('IVHV_gap_180D', 0)
    gap_60d = row.get('IVHV_gap_60D', 0)
    iv_rank = row.get('IV_Rank_30D', 50)
    
    # Rejection criteria
    if not expansion and signal != 'Bidirectional':
        return None
    if gap_180d >= 0 and gap_60d >= 0:
        return None  # IV not cheap
    if iv_rank >= 35 and gap_180d >= -15:
        return None  # Not very cheap
    
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Long Straddle',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Expansion + Very Cheap IV (IV_Rank={iv_rank:.0f}, gap_180d={gap_180d:.1f})",
        'Theory_Source': 'Natenberg Ch.9 - ATM volatility play',
        'Regime_Context': signal if signal == 'Bidirectional' else 'Expansion',
        'IV_Context': f"gap_60d={gap_60d:.1f}, gap_180d={gap_180d:.1f}, IV_Rank={iv_rank:.0f}",
        'Capital_Requirement': 8000,  # Approximate for ATM straddle
        'Risk_Profile': 'Defined (max loss = total premium)',
        'Greeks_Exposure': 'Delta-neutral, Long Vega, Short Theta',
        'Execution_Ready': True,
        'Confidence': 72,
        'Trade_Bias': 'Bidirectional',
        'Strategy_Type': 'Volatility',
    }


def _validate_long_strangle(ticker: str, row: pd.Series) -> Optional[Dict]:
    """
    Validate Long Strangle strategy.
    
    Theory: Natenberg Ch.9 - OTM volatility play (cheaper than straddle).
    Entry: Expansion setup + Moderately Cheap IV (35 ≤ IV_Rank < 50).
    """
    expansion = row.get('Expansion_Setup', False)
    signal = row.get('Signal_Type', '')
    gap_180d = row.get('IVHV_gap_180D', 0)
    gap_60d = row.get('IVHV_gap_60D', 0)
    iv_rank = row.get('IV_Rank_30D', 50)
    
    # Rejection criteria
    if not expansion and signal != 'Bidirectional':
        return None
    if gap_180d >= 0 and gap_60d >= 0:
        return None
    if iv_rank < 35:
        return None  # Prefer Straddle when very cheap
    if iv_rank >= 50:
        return None  # Not cheap enough
    
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Long Strangle',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Expansion + Moderately Cheap IV (IV_Rank={iv_rank:.0f})",
        'Theory_Source': 'Natenberg Ch.9 - OTM volatility (cheaper, needs bigger move)',
        'Regime_Context': signal if signal == 'Bidirectional' else 'Expansion',
        'IV_Context': f"gap_60d={gap_60d:.1f}, gap_180d={gap_180d:.1f}, IV_Rank={iv_rank:.0f}",
        'Capital_Requirement': 5000,  # Cheaper than straddle
        'Risk_Profile': 'Defined (max loss = total premium)',
        'Greeks_Exposure': 'Delta-neutral, Long Vega, Short Theta',
        'Execution_Ready': True,
        'Confidence': 68,
        'Trade_Bias': 'Bidirectional',
        'Strategy_Type': 'Volatility',
    }


def _select_strategy(
    row: pd.Series,
    min_iv_rank: float,
    min_ivhv_gap: float,
    enable_directional: bool,
    enable_neutral: bool,
    enable_volatility: bool
) -> Dict:
    """
    DEPRECATED: Legacy single-strategy selector.
    
    This function is preserved for backward compatibility but should
    not be used in new code. Use independent validators instead.
    
    Returns dict with strategy recommendations and metadata.
    """
    result = {
        'Primary_Strategy': 'None',
        'Secondary_Strategy': 'None',
        'Strategy_Type': 'None',
        'Confidence': 0,
        'Trade_Bias': 'Neutral',
        'Entry_Priority': 'Low',
        'Risk_Level': 'Medium'
    }
    
    # Legacy 30D metrics (for backward compatibility)
    iv_rank = row.get('IV_Rank_30D', 0)
    
    # Step 3 multi-timeframe edge flags (NEW)
    short_term_edge = row.get('ShortTerm_IV_Edge', False)
    medium_term_edge = row.get('MediumTerm_IV_Edge', False)
    leap_edge = row.get('LEAP_IV_Edge', False)
    ultra_leap_edge = row.get('UltraLEAP_IV_Edge', False)
    
    # Step 3 multi-timeframe gaps WITH SIGN (NEW)
    gap_30d = row.get('IVHV_gap_30D', 0)
    gap_60d = row.get('IVHV_gap_60D', 0)
    gap_180d = row.get('IVHV_gap_180D', 0)
    gap_360d = row.get('IVHV_gap_360D', 0)
    
    # Context signals
    signal = row.get('Signal_Type', '')
    regime = row.get('Regime', '')
    crossover_age = row.get('Crossover_Age_Bucket', 'None')
    
    # Legacy regime flags (fallback if Step 3 flags unavailable)
    iv_rich = row.get('IV_Rich', False)
    iv_cheap = row.get('IV_Cheap', False)
    mean_reversion = row.get('MeanReversion_Setup', False)
    expansion = row.get('Expansion_Setup', False)
    
    # Base confidence (no crossover age bias yet)
    base_confidence = 0
    
    # === TIER-1 STRATEGIES (Simple, Broker-Approved) ===
    # Prioritize simple strategies that don't require spreads
    
    # BULLISH TIER-1: Long Call, Cash-Secured Put, or Buy-Write
    if signal in ['Bullish', 'Sustained Bullish'] and enable_directional:
        iv_rank = row.get('IV_Rank_30D', 50)
        
        if gap_180d < 0 or gap_60d < 0:
            # Cheap IV → Buy long call (standard directional play)
            result['Primary_Strategy'] = 'Long Call'
            result['Secondary_Strategy'] = 'Cash-Secured Put'
            result['Strategy_Type'] = 'Directional'
            result['Trade_Bias'] = 'Bullish'
            base_confidence = 65
            
        elif gap_30d > 0:
            # Rich IV decision: Buy-Write vs CSP
            # Theory: Buy-Write = stock + call (Cohen Ch.7), CSP = naked put (Passarelli)
            
            if iv_rank > 70:
                # Very rich IV (>70th percentile) → Buy-Write preferred
                # Cohen: "Buy-Write reduces cost basis more aggressively than CSP"
                # Passarelli: "When IV extremely rich, owning stock + selling calls superior"
                result['Primary_Strategy'] = 'Buy-Write'
                result['Secondary_Strategy'] = 'Cash-Secured Put'
                result['Strategy_Type'] = 'Directional'
                result['Trade_Bias'] = 'Bullish'
                base_confidence = 75  # High confidence: strong directional + very rich premium
                
            else:
                # Moderately rich IV (30D gap > 0 but IV_Rank <= 70) → CSP
                # Passarelli: "CSP simpler execution, same risk profile as covered call"
                result['Primary_Strategy'] = 'Cash-Secured Put'
                result['Secondary_Strategy'] = 'Long Call'
                result['Strategy_Type'] = 'Directional'
                result['Trade_Bias'] = 'Bullish'
                base_confidence = 70
    
    # BEARISH TIER-1: Long Put or Covered Call
    elif signal in ['Bearish'] and enable_directional:
        if gap_180d < 0 or gap_60d < 0:
            # Cheap IV → Buy long put
            result['Primary_Strategy'] = 'Long Put'
            result['Secondary_Strategy'] = 'Covered Call (if holding stock)'
            result['Strategy_Type'] = 'Directional'
            result['Trade_Bias'] = 'Bearish'
            base_confidence = 65
        elif gap_30d > 0:
            # Rich IV → Sell covered call (if stock held)
            result['Primary_Strategy'] = 'Covered Call (if holding stock)'
            result['Secondary_Strategy'] = 'Long Put'
            result['Strategy_Type'] = 'Directional'
            result['Trade_Bias'] = 'Bearish'
            base_confidence = 70
    
    # VOLATILITY TIER-1: Long Straddle or Long Strangle (parallel to directional, not mutually exclusive)
    # Theory: Natenberg Ch.9 - Volatility buying when expecting expansion
    if enable_volatility and (expansion or signal == 'Bidirectional'):
        if gap_180d < 0 or gap_60d < 0:
            # Cheap IV → Long volatility strategies
            iv_rank = row.get('IV_Rank_30D', 50)
            
            # Decision tree based on IV_Rank to differentiate strategies
            
            # Long Straddle: Best when IV is VERY cheap (bottom 35%)
            # Natenberg: "Straddle profits from volatility increase OR directional move"
            # Threshold: IV_Rank < 35 OR extremely negative gap
            if iv_rank < 35 or gap_180d < -15:
                result['Primary_Strategy'] = 'Long Straddle'
                result['Secondary_Strategy'] = 'Long Strangle'
                result['Strategy_Type'] = 'Volatility'
                result['Trade_Bias'] = 'Bidirectional'
                base_confidence = 72  # Higher confidence when IV extremely cheap
                
            # Long Strangle: Suitable when IV moderately cheap (35-50%)
            # Natenberg: "Strangle cheaper than straddle but needs bigger directional move"
            # Theory: OTM options → lower cost, requires larger move for profit
            # Threshold: 35 <= IV_Rank < 50 (middle range of cheap IV)
            else:
                # This captures expansion cases where IV not extremely cheap
                result['Primary_Strategy'] = 'Long Strangle'
                result['Secondary_Strategy'] = 'Long Straddle'
                result['Strategy_Type'] = 'Volatility'
                result['Trade_Bias'] = 'Bidirectional'
                base_confidence = 68
    
    # === HIGH IV STRATEGIES (Premium Selling - TIER 2) ===
    # Use Step 3 edge flags + positive gap requirement
    if result['Primary_Strategy'] == 'None' and short_term_edge and gap_30d > 0:
        if signal in ['Bullish', 'Sustained Bullish'] and enable_directional:
            # Positive 30D gap + Bullish = Sell put spreads / CSP
            result['Primary_Strategy'] = 'Put Credit Spread'
            result['Secondary_Strategy'] = 'Cash-Secured Put'
            result['Strategy_Type'] = 'Directional'
            result['Trade_Bias'] = 'Bullish'
            base_confidence = 70  # Step 3 edge flag is pre-validated
            
        elif signal in ['Bearish'] and enable_directional:
            # Positive 30D gap + Bearish = Sell call spreads
            result['Primary_Strategy'] = 'Call Credit Spread'
            result['Secondary_Strategy'] = 'Covered Call (if holding stock)'
            result['Strategy_Type'] = 'Directional'
            result['Trade_Bias'] = 'Bearish'
            base_confidence = 70
            
        elif signal in ['Base', 'Neutral'] and enable_neutral:
            # Positive 30D gap + Neutral = Income strategies (crossover age irrelevant)
            result['Primary_Strategy'] = 'Iron Condor'
            result['Secondary_Strategy'] = 'Short Strangle'
            result['Strategy_Type'] = 'Neutral'
            result['Trade_Bias'] = 'Neutral'
            # Neutral strategies: regime matters more than crossover
            base_confidence = 75 if regime == 'Ranging' else 65
            
        if mean_reversion and enable_volatility:
            # Mean reversion setup = Volatility plays (extended trends preferred)
            result['Secondary_Strategy'] = 'Calendar Spread'
            result['Strategy_Type'] = 'Volatility' if result['Strategy_Type'] == 'None' else 'Mixed'
            base_confidence += 10  # IV reversion, not momentum
    
    # === MODERATE IV + POSITIVE GAP (Debit Spreads - TIER 2) ===
    # Debit spreads SELL short leg → need positive gap for premium capture
    elif result['Primary_Strategy'] == 'None' and medium_term_edge and gap_60d > 0:
        if signal in ['Bullish', 'Sustained Bullish'] and enable_directional:
            # Positive 60D gap + Bullish = Call debit spread (sell short call at high IV)
            result['Primary_Strategy'] = 'Call Debit Spread'
            result['Secondary_Strategy'] = 'Diagonal Spread (Bullish)'
            result['Strategy_Type'] = 'Directional'
            result['Trade_Bias'] = 'Bullish'
            base_confidence = 65
            
        elif signal in ['Bearish'] and enable_directional:
            # Positive 60D gap + Bearish = Put debit spread (sell short put at high IV)
            result['Primary_Strategy'] = 'Put Debit Spread'
            result['Secondary_Strategy'] = 'Diagonal Spread (Bearish)'
            result['Strategy_Type'] = 'Directional'
            result['Trade_Bias'] = 'Bearish'
            base_confidence = 65
    
    # === NEGATIVE GAP (Pure Volatility Buying - TIER 3) ===
    # Naked long options + LEAPs → need NEGATIVE gap (cheap volatility)
    elif result['Primary_Strategy'] == 'None' and leap_edge and gap_180d < 0:
        if signal in ['Bullish', 'Sustained Bullish'] and enable_directional:
            # Negative 180D gap + Bullish = Buy naked LEAP call (cheap IV)
            result['Primary_Strategy'] = 'LEAP Call (Buy to Open)'
            result['Secondary_Strategy'] = 'Long Call'
            result['Strategy_Type'] = 'Directional'
            result['Trade_Bias'] = 'Bullish'
            base_confidence = 60
            
        elif signal in ['Bearish'] and enable_directional:
            # Negative 180D gap + Bearish = Buy naked LEAP put (cheap IV)
            result['Primary_Strategy'] = 'LEAP Put (Buy to Open)'
            result['Secondary_Strategy'] = 'Long Put'
            result['Strategy_Type'] = 'Directional'
            result['Trade_Bias'] = 'Bearish'
            base_confidence = 60
            
        if expansion and enable_volatility:
            # IV expansion + cheap volatility = Long straddles (crossover age irrelevant)
            result['Primary_Strategy'] = 'Long Straddle'
            result['Secondary_Strategy'] = 'Long Strangle'
            result['Strategy_Type'] = 'Volatility'
            result['Trade_Bias'] = 'Bidirectional'
            base_confidence = 65  # IV expansion matters, not momentum
    
    # === FALLBACK: No strong edge detected ===
    # If Step 3 flags are unavailable, use legacy IV_Rank logic
    elif result['Primary_Strategy'] == 'None' and not short_term_edge and not medium_term_edge and not leap_edge:
        if iv_rank >= 60 and gap_30d > 0:
            # Legacy high IV logic
            result['Primary_Strategy'] = 'Iron Condor' if signal in ['Base', 'Neutral'] else 'Put Credit Spread'
            result['Strategy_Type'] = 'Neutral' if signal in ['Base', 'Neutral'] else 'Directional'
            result['Trade_Bias'] = 'Neutral' if signal in ['Base', 'Neutral'] else 'Bullish'
            base_confidence = 50
    
    # === APPLY CROSSOVER AGE ADJUSTMENT (ONLY FOR DIRECTIONAL) ===
    # NOTE: This is the KEY fix - momentum freshness only matters for directional trades
    confidence = base_confidence
    
    if result['Strategy_Type'] == 'Directional':
        # Directional momentum: Recent crossovers are better
        if crossover_age == 'Age_0_5':
            confidence += 15  # Fresh momentum
        elif crossover_age == 'Age_6_15':
            confidence += 5   # Moderate momentum
        # Age_16_plus: no adjustment (base confidence)
    elif result['Strategy_Type'] in ['Neutral', 'Volatility', 'Mixed']:
        # Neutral/Volatility: Crossover age is neutral or beneficial
        # Extended trends provide stability for income/volatility strategies
        # No penalty for Age_16_plus
        pass  # Keep base confidence
    
    # === RISK AND PRIORITY ASSIGNMENT ===
    if confidence >= 70:
        result['Entry_Priority'] = 'High'
        result['Risk_Level'] = 'Low' if result['Strategy_Type'] in ['Neutral', 'Volatility'] else 'Medium'
    elif confidence >= 55:
        result['Entry_Priority'] = 'Medium'
        result['Risk_Level'] = 'Medium'
    else:
        result['Entry_Priority'] = 'Low'
        result['Risk_Level'] = 'High' if result['Trade_Bias'] != 'Neutral' else 'Medium'
    
    # Cap confidence at 100
    result['Confidence'] = min(confidence, 100)
    
    return result


def _estimate_success_probability(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estimate probability of profit for each strategy.
    
    NOTE: Crossover age adjustments are ONLY applied to directional strategies.
    Neutral, income, and volatility strategies should NOT be penalized for
    extended trends - they often prefer stability over momentum freshness.
    
    This is a heuristic model. For production, use backtested probabilities
    or machine learning models trained on historical outcomes.
    """
    df = df.copy()
    
    # Base probability from confidence
    df['Success_Probability'] = df['Confidence'] / 100.0
    
    # Adjust for strategy type (neutral strategies higher base POP)
    neutral_mask = df['Strategy_Type'] == 'Neutral'
    df.loc[neutral_mask, 'Success_Probability'] *= 1.1  # 10% boost
    
    # Adjust for IV rank extremes (mean reversion potential)
    high_iv_mask = df['IV_Rank_30D'] >= 80
    low_iv_mask = df['IV_Rank_30D'] <= 20
    df.loc[high_iv_mask, 'Success_Probability'] *= 1.05
    df.loc[low_iv_mask, 'Success_Probability'] *= 0.95
    
    # === CROSSOVER AGE ADJUSTMENTS (DIRECTIONAL ONLY) ===
    # This is the KEY fix - only directional strategies care about momentum freshness
    directional_mask = df['Strategy_Type'] == 'Directional'
    
    # For directional: fresh signals = higher probability
    directional_recent = directional_mask & (df['Crossover_Age_Bucket'] == 'Age_0_5')
    directional_extended = directional_mask & (df['Crossover_Age_Bucket'] == 'Age_16_plus')
    
    df.loc[directional_recent, 'Success_Probability'] *= 1.05
    df.loc[directional_extended, 'Success_Probability'] *= 0.95
    
    # For neutral/volatility: crossover age is neutral (no adjustment)
    # Extended trends can be BENEFICIAL for income and volatility strategies
    
    # Cap between 0.3 and 0.85 (no trade is guaranteed or hopeless)
    df['Success_Probability'] = df['Success_Probability'].clip(0.3, 0.85)
    
    return df


def _log_strategy_summary(df: pd.DataFrame) -> None:
    """Log summary statistics for recommended strategies."""
    total = len(df)
    
    # Count by strategy type
    strategy_counts = df['Strategy_Type'].value_counts()
    logger.info(f"📊 Strategy distribution: {strategy_counts.to_dict()}")
    
    # Count by bias
    bias_counts = df['Trade_Bias'].value_counts()
    logger.info(f"🎯 Trade bias: {bias_counts.to_dict()}")
    
    # High priority setups
    high_priority = len(df[df['Entry_Priority'] == 'High'])
    logger.info(f"⭐ High priority entries: {high_priority}/{total}")
    
    # Confidence distribution
    high_conf = len(df[df['Confidence'] >= 70])
    med_conf = len(df[(df['Confidence'] >= 55) & (df['Confidence'] < 70)])
    low_conf = len(df[df['Confidence'] < 55])
    logger.info(f"🎚️ Confidence: High={high_conf}, Medium={med_conf}, Low={low_conf}")
    
    # Average success probability
    avg_prob = df['Success_Probability'].mean()
    logger.info(f"📈 Average success probability: {avg_prob:.2%}")
