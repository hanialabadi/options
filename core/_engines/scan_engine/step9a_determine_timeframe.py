"""
Step 9A: Determine Optimal Option Timeframe (STRATEGY-AWARE)

🔄 ARCHITECTURAL REDESIGN (Post-Audit):
    Previously: Processed one strategy per ticker (ticker-level DTE)
    Now: Processes EACH (Ticker, Strategy) pair independently

PURPOSE:
    Assign optimal DTE ranges to each strategy in the multi-strategy ledger.
    Different strategies on the SAME ticker need DIFFERENT DTE windows.
    
    Example: AAPL at $185
        - Long Call (Directional):    30-45 DTE (theta efficient, gamma exposure)
        - Long Straddle (Volatility): 45-60 DTE (volatility expansion runway)
        - Buy-Write (Income):         30-45 DTE (premium collection cycles)
    
    Each strategy fetches its OWN option chain in Step 9B.

DESIGN PRINCIPLE (RAG-Informed):
    From Natenberg & Passarelli:
    - Theta decay accelerates in final 30-45 days (non-linear)
    - Short-term options have higher gamma (more sensitive to moves)
    - Longer-term options have lower theta but higher vega (volatility exposure)
    - Different strategies require different time horizons for optimal execution

CRITICAL RULE:
    This step operates on the STRATEGY LEDGER from Step 7.
    Input: Multiple rows per ticker (one per strategy)
    Output: Same number of rows (DTE columns added, NO ROWS DROPPED)
    
    ⚠️ DO NOT group by ticker or select "best" strategy
    ⚠️ Each (Ticker, Strategy_Name) pair is independent

STRATEGY-SPECIFIC DTE RANGES:

    Directional Strategies (30-45 DTE):
        - Long Call, Long Put
        - Rationale: Balance theta decay vs directional exposure
        - Need movement within 1-2 months
    
    Volatility Strategies (45-60 DTE):
        - Long Straddle, Long Strangle
        - Rationale: More time for volatility expansion
        - Avoid rapid theta decay on both legs
    
    Income Strategies (30-45 DTE):
        - Cash-Secured Put, Covered Call, Buy-Write
        - Rationale: Faster premium collection, more cycles/year
        - Sweet spot for theta decay efficiency
    
    Spread Strategies (30-60 DTE):
        - Credit spreads, debit spreads
        - Rationale: Varies by width and intent

INPUTS (from Step 7):
    - Ticker: Stock symbol
    - Strategy_Name: Long Call | Long Put | Long Straddle | CSP | etc.
    - Strategy_Type: Directional | Volatility | Income | Mixed
    - Trade_Bias: Bullish | Bearish | Neutral | Expansion
    - Confidence: 0-100 score (optional, for fine-tuning)
    - IV_Rank_30D: Percentile rank (optional)

OUTPUTS:
    Original ledger + new columns:
    - Min_DTE: Minimum days to expiration
    - Max_DTE: Maximum days to expiration
    - Target_DTE: Preferred DTE (midpoint)
    - Timeframe_Label: Short/Medium/Long
    - DTE_Rationale: Why this DTE range (strategy-specific)
    - Expiration_Count_Target: How many expirations to fetch (2-3)

GUARDRAIL:
    This step does NOT fetch options. It only determines WHICH expirations to query.
    Actual chain fetching happens in Step 9B using these DTE bounds per strategy.
"""

import pandas as pd
import numpy as np
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def determine_timeframe(df: pd.DataFrame, expiry_intent: str = 'ANY') -> pd.DataFrame:
    """
    Determine optimal DTE range for EACH (Ticker, Strategy) pair independently.
    
    🔄 CRITICAL CHANGE: This function now operates on the MULTI-STRATEGY LEDGER.
    - Input: Multiple strategies per ticker (from Step 7)
    - Output: Same strategies with DTE columns added (NO ROWS DROPPED)
    - Each strategy gets its own optimal DTE range
    
    Example Input:
        Ticker  Strategy_Name    Strategy_Type
        AAPL    Long Call        Directional
        AAPL    Long Straddle    Volatility
        AAPL    Buy-Write        Income
    
    Example Output:
        Ticker  Strategy_Name    Min_DTE  Max_DTE  Target_DTE
        AAPL    Long Call        30       45       37
        AAPL    Long Straddle    45       60       52
        AAPL    Buy-Write        30       45       37
    
    Args:
        df (pd.DataFrame): Strategy ledger from Step 7 with columns:
            REQUIRED:
            - Ticker (str): Stock symbol
            - Strategy_Name (str): Long Call, Long Put, Long Straddle, etc.
            - Strategy_Type (str): Directional, Volatility, Income, Mixed
            
            OPTIONAL (for fine-tuning):
            - Confidence (int): 0-100 score
            - IV_Rank_30D (float): 0-100 percentile
            - Trade_Bias (str): Bullish, Bearish, Neutral, Expansion
    
    Returns:
        pd.DataFrame: Original ledger with DTE columns added:
            - Min_DTE (int): Minimum days to expiration
            - Max_DTE (int): Maximum days to expiration
            - Target_DTE (int): Preferred DTE (midpoint)
            - Timeframe_Label (str): Short/Medium/Long
            - DTE_Rationale (str): Strategy-specific explanation
            - Expiration_Count_Target (int): Number of expirations to fetch
    
    Raises:
        ValueError: If required columns are missing
        AssertionError: If row count changes (architectural violation)
    
    Example:
        >>> # Multi-strategy ledger from Step 7
        >>> df = pd.DataFrame({
        ...     'Ticker': ['AAPL', 'AAPL', 'AAPL'],
        ...     'Strategy_Name': ['Long Call', 'Long Straddle', 'Buy-Write'],
        ...     'Strategy_Type': ['Directional', 'Volatility', 'Income'],
        ...     'Confidence': [75, 65, 70]
        ... })
        >>> result = determine_option_timeframe(df)
        >>> print(result[['Ticker', 'Strategy_Name', 'Min_DTE', 'Max_DTE']])
    """
    
    # Validate inputs
    required_cols = ['Ticker', 'Strategy_Name', 'Strategy_Type']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    logger.info("⏱️ Step 9A: Entering determine_timeframe function.")
    if df.empty:
        logger.warning("⚠️ Empty DataFrame passed to Step 9A. Returning empty DataFrame.")
        logger.info("⏱️ Step 9A: Exiting determine_timeframe function (empty input).")
        return df
    
    input_row_count = len(df)
    logger.info(f"✅ Step 9A: Processing {input_row_count} strategies (may include multiple per ticker).")
    
    # Create output columns
    df = df.copy()
    
    # Ensure thesis column is preserved
    if 'thesis' not in df.columns:
        logger.warning("⚠️ 'thesis' column missing in Step 9A input")
        
    df['Min_DTE'] = 0
    df['Max_DTE'] = 0
    df['Target_DTE'] = 0
    df['Timeframe_Label'] = ''
    df['DTE_Rationale'] = ''
    df['Expiration_Count_Target'] = 2  # Default: fetch 2 expirations per strategy
    
    logger.info(f"🎯 Step 9A (STRATEGY-AWARE): Processing {input_row_count} (Ticker, Strategy) pairs")
    
    # Process each strategy independently
    for idx, row in df.iterrows():
        ticker = row['Ticker']
        strategy_name = row['Strategy_Name']
        strategy_type = row['Strategy_Type']
        confidence = row.get('Confidence', 70)  # Default to moderate
        iv_rank = row.get('IV_Rank_30D', 50)    # Default to neutral
        
        # Apply Expiry Intent Overrides
        if expiry_intent == 'THIS_WEEK':
            min_dte, max_dte = 0, 7
            label = 'Weekly'
            rationale = "Operator Intent: THIS_WEEK (0-7 DTE)"
        elif expiry_intent == 'NEXT_WEEK':
            min_dte, max_dte = 8, 14
            label = 'Next-Week'
            rationale = "Operator Intent: NEXT_WEEK (8-14 DTE)"
        else:
            # Standard strategy-aware logic
            min_dte, max_dte, label, rationale = _calculate_dte_range_by_strategy(
                strategy_name, strategy_type, confidence, iv_rank
            )
        
        df.at[idx, 'Min_DTE'] = min_dte
        df.at[idx, 'Max_DTE'] = max_dte
        df.at[idx, 'Target_DTE'] = (min_dte + max_dte) // 2
        df.at[idx, 'Timeframe_Label'] = label
        df.at[idx, 'DTE_Rationale'] = rationale
    
    # CRITICAL: Verify no rows dropped
    output_row_count = len(df)
    if output_row_count != input_row_count:
        raise AssertionError(
            f"Step 9A architectural violation: "
            f"Input {input_row_count} rows → Output {output_row_count} rows. "
            f"This step must preserve all strategies!"
        )
    
    # Log distribution
    _log_timeframe_summary(df)
    
    # Audit: Check for multi-DTE tickers
    _audit_multi_dte_tickers(df)
    
    logger.info("⏱️ Step 9A: Exiting determine_timeframe function successfully.")
    return df


def _calculate_dte_range_by_strategy(
    strategy_name: str,
    strategy_type: str,
    confidence: int,
    iv_rank: float
) -> tuple[int, int, str, str]:
    """
    Calculate DTE range based on STRATEGY NAME (not just type).
    
    Strategy-Specific Rules:
        Directional (30-45 DTE):
            - Long Call, Long Put
            - Need directional movement within 1-2 months
            
        Volatility (45-60 DTE):
            - Long Straddle, Long Strangle
            - Need time for volatility expansion
            
        Income (30-45 DTE):
            - Cash-Secured Put, Covered Call, Buy-Write
            - Optimize for premium collection cycles
            
        Spreads (varies):
            - Credit/Debit spreads: 30-60 DTE based on width
    
    Args:
        strategy_name: Specific strategy (e.g., "Long Call")
        strategy_type: General category (e.g., "Directional")
        confidence: 0-100 conviction score (for fine-tuning)
        iv_rank: 0-100 IV percentile (for vol strategies)
    
    Returns:
        tuple: (min_dte, max_dte, label, rationale)
    """
    
    # LEAP STRATEGIES (Long Call LEAP, Long Put LEAP)
    if strategy_name in ['Long Call LEAP', 'Long Put LEAP']:
        # LEAPs: 365-730 DTE for multi-year thesis
        return (365, 730, 'LEAP', 
               f"LEAP strategy ({strategy_name}): 365-730 DTE for multi-year structural thesis")
    
    # DIRECTIONAL STRATEGIES (Long Call, Long Put)
    elif strategy_name in ['Long Call', 'Long Put']:
        # Standard: 30-45 DTE for theta efficiency
        # High confidence can use shorter end, low confidence longer end
        if confidence >= 75:
            return (30, 42, 'Short', 
                   f"Directional ({strategy_name}) with high confidence ({confidence}): 30-42 DTE for theta efficiency")
        else:
            return (35, 50, 'Short-Medium', 
                   f"Directional ({strategy_name}) with moderate confidence ({confidence}): 35-50 DTE for directional exposure")
    
    # VOLATILITY STRATEGIES (Long Straddle, Long Strangle)
    elif strategy_name in ['Long Straddle', 'Long Strangle']:
        # Need more time for volatility expansion
        # Adjust based on current IV rank
        if iv_rank >= 70:
            # High IV → expect mean reversion → shorter
            return (35, 50, 'Short-Medium', 
                   f"Volatility ({strategy_name}) with elevated IV ({iv_rank:.0f}): 35-50 DTE for vol contraction play")
        elif iv_rank <= 30:
            # Low IV → wait for expansion → longer
            return (50, 65, 'Medium', 
                   f"Volatility ({strategy_name}) with depressed IV ({iv_rank:.0f}): 50-65 DTE for vol expansion")
        else:
            # Neutral IV → standard range
            return (45, 60, 'Medium', 
                   f"Volatility ({strategy_name}) with neutral IV ({iv_rank:.0f}): 45-60 DTE for volatility play")
    
    # INCOME STRATEGIES (CSP, Covered Call, Buy-Write)
    elif strategy_name in ['Cash-Secured Put', 'Covered Call', 'Buy-Write']:
        # Optimize for premium collection cycles
        return (30, 45, 'Short', 
               f"Income ({strategy_name}): 30-45 DTE for premium collection efficiency")
    
    # CREDIT SPREADS (Iron Condor, Put Credit Spread, Call Credit Spread)
    elif any(kw in strategy_name for kw in ['Credit Spread', 'Iron Condor', 'Iron Butterfly']):
        if confidence >= 75:
            return (30, 45, 'Short', 
                   f"Credit strategy ({strategy_name}) with high confidence ({confidence}): 30-45 DTE for theta decay")
        else:
            return (40, 55, 'Medium', 
                   f"Credit strategy ({strategy_name}) with moderate confidence ({confidence}): 40-55 DTE for adjustment time")
    
    # DEBIT SPREADS (Bull Call, Bear Put, etc.)
    elif any(kw in strategy_name for kw in ['Debit Spread', 'Bull', 'Bear']):
        return (30, 50, 'Short-Medium', 
               f"Debit spread ({strategy_name}): 30-50 DTE for directional play")
    
    # CALENDAR/DIAGONAL SPREADS
    elif any(kw in strategy_name for kw in ['Calendar', 'Diagonal']):
        return (45, 75, 'Medium', 
               f"Calendar/Diagonal ({strategy_name}): 45-75 DTE for time decay differential")
    
    # FALLBACK BY STRATEGY TYPE
    elif strategy_type == 'Directional':
        return (30, 50, 'Short-Medium', 
               f"Directional strategy type: 30-50 DTE (generic directional)")
    elif strategy_type == 'Volatility':
        return (45, 60, 'Medium', 
               f"Volatility strategy type: 45-60 DTE (generic volatility)")
    elif strategy_type == 'Income':
        return (30, 45, 'Short', 
               f"Income strategy type: 30-45 DTE (generic income)")
    elif strategy_type in ['Mixed', 'Neutral']:
        return (35, 55, 'Medium', 
               f"{strategy_type} strategy type: 35-55 DTE (balanced Greeks)")
    
    # ULTIMATE FALLBACK
    else:
        logger.warning(f"⚠️ Unknown strategy '{strategy_name}' (type: {strategy_type}), using default DTE")
        return (35, 55, 'Medium', 
               f"Default timeframe for {strategy_name}: 35-55 DTE")


def _audit_multi_dte_tickers(df: pd.DataFrame):
    """
    Audit and log tickers with multiple DTE windows (proves strategy independence).
    
    This function verifies that the same ticker can have different DTE ranges
    for different strategies, confirming the architecture is working correctly.
    """
    if 'Ticker' not in df.columns or 'Target_DTE' not in df.columns:
        return
    
    # Group by ticker and count unique DTE targets
    dte_variety = df.groupby('Ticker')['Target_DTE'].nunique()
    multi_dte_tickers = dte_variety[dte_variety > 1]
    
    if len(multi_dte_tickers) > 0:
        logger.info(f"")
        logger.info(f"✅ MULTI-DTE VALIDATION: {len(multi_dte_tickers)} tickers have multiple DTE windows")
        logger.info(f"   (This confirms strategy-aware DTE assignment is working)")
        
        # Show examples
        sample_tickers = multi_dte_tickers.head(3).index
        for ticker in sample_tickers:
            ticker_strategies = df[df['Ticker'] == ticker][['Strategy_Name', 'Min_DTE', 'Max_DTE', 'Target_DTE']]
            logger.info(f"")
            logger.info(f"   {ticker}: {len(ticker_strategies)} strategies with different DTE windows")
            for _, row in ticker_strategies.iterrows():
                logger.info(f"      • {row['Strategy_Name']}: {row['Min_DTE']}-{row['Max_DTE']} DTE (target: {row['Target_DTE']})")
    else:
        logger.info(f"")
        logger.info(f"ℹ️ No tickers with multiple DTE windows (all tickers have single strategy)")


def _calculate_dte_range(
    strategy: str,
    strategy_type: str,
    confidence: int,
    iv_rank: float
) -> tuple[int, int, str, str]:
    """
    DEPRECATED: Old ticker-level DTE calculation (kept for backward compatibility).
    
    Use _calculate_dte_range_by_strategy() instead for strategy-aware DTE.
    
    This function is preserved for any legacy code that might call it,
    but new code should use the strategy-aware version.
    """
    logger.warning(f"⚠️ DEPRECATED: _calculate_dte_range() called. Use _calculate_dte_range_by_strategy() instead.")
    return _calculate_dte_range_by_strategy(strategy, strategy_type, confidence, iv_rank)


def _log_timeframe_summary(df: pd.DataFrame):
    """Log distribution of timeframe selections."""
    
    label_dist = df['Timeframe_Label'].value_counts().to_dict()
    avg_min = df['Min_DTE'].mean()
    avg_max = df['Max_DTE'].mean()
    avg_target = df['Target_DTE'].mean()
    
    logger.info(f"📊 Step 9A Summary:")
    logger.info(f"   Timeframe distribution: {label_dist}")
    logger.info(f"   Average DTE range: {avg_min:.0f}-{avg_max:.0f} days (target: {avg_target:.0f})")
    logger.info(f"   Shortest window: {df['Min_DTE'].min()}-{df['Max_DTE'].min()} days")
    logger.info(f"   Longest window: {df['Min_DTE'].max()}-{df['Max_DTE'].max()} days")


# Additional validation helper
def validate_dte_bounds(df: pd.DataFrame, warn_only: bool = True) -> pd.DataFrame:
    """
    Validate that DTE ranges are reasonable and flag potential issues.
    
    Args:
        df: DataFrame with Min_DTE, Max_DTE columns
        warn_only: If True, log warnings but don't modify data
    
    Returns:
        DataFrame with validation results
    """
    issues = []
    
    for idx, row in df.iterrows():
        ticker = row.get('Ticker', f"Row_{idx}")
        min_dte = row['Min_DTE']
        max_dte = row['Max_DTE']
        
        if min_dte < 7:
            issues.append(f"{ticker}: Min_DTE={min_dte} too short (< 7 days, high gamma risk)")
        
        if max_dte > 365:
            issues.append(f"{ticker}: Max_DTE={max_dte} very long (> 1 year, LEAPS territory)")
        
        if max_dte - min_dte < 10:
            issues.append(f"{ticker}: DTE range too narrow ({min_dte}-{max_dte}, may miss expirations)")
    
    if issues:
        logger.warning(f"⚠️ DTE validation found {len(issues)} potential issues:")
        for issue in issues[:5]:  # Show first 5
            logger.warning(f"   - {issue}")
        if len(issues) > 5:
            logger.warning(f"   ... and {len(issues) - 5} more")
    else:
        logger.info("✅ All DTE ranges validated successfully")
    
    return df


if __name__ == "__main__":
    # Test with MULTI-STRATEGY ledger (same ticker, multiple strategies)
    import sys
    logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
    
    test_df = pd.DataFrame({
        # Multiple strategies for AAPL
        'Ticker': ['AAPL', 'AAPL', 'AAPL', 
                   'MSFT', 'MSFT',
                   'GOOGL', 'TSLA', 'NVDA'],
        'Strategy_Name': [
            'Long Call', 'Long Straddle', 'Buy-Write',  # AAPL: 3 strategies
            'Long Put', 'Cash-Secured Put',             # MSFT: 2 strategies
            'Long Call',                                 # GOOGL: 1 strategy
            'Long Straddle',                            # TSLA: 1 strategy
            'Long Call'                                  # NVDA: 1 strategy
        ],
        'Strategy_Type': [
            'Directional', 'Volatility', 'Income',      # AAPL
            'Directional', 'Income',                     # MSFT
            'Directional', 'Volatility', 'Directional'  # GOOGL, TSLA, NVDA
        ],
        'Trade_Bias': [
            'Bullish', 'Expansion', 'Bullish',
            'Bearish', 'Bullish',
            'Bullish', 'Expansion', 'Bullish'
        ],
        'Confidence': [80, 65, 70, 75, 72, 85, 60, 78],
        'IV_Rank_30D': [60, 25, 60, 85, 80, 55, 30, 45],
        'Strategy_Tier': [1, 1, 1, 1, 1, 1, 1, 1]
    })
    
    print("=" * 80)
    print("Step 9A Test: Strategy-Aware DTE Determination")
    print("=" * 80)
    print(f"\nInput: {len(test_df)} strategies across {test_df['Ticker'].nunique()} tickers")
    print(f"Multi-strategy tickers: {test_df['Ticker'].value_counts()[test_df['Ticker'].value_counts() > 1].to_dict()}")
    print("\n" + "=" * 80)
    
    result = determine_option_timeframe(test_df)
    
    print("\n" + "=" * 80)
    print("Results: DTE Ranges per Strategy")
    print("=" * 80)
    print(result[['Ticker', 'Strategy_Name', 'Min_DTE', 'Max_DTE', 
                  'Target_DTE', 'Timeframe_Label']].to_string(index=False))
    
    print("\n" + "=" * 80)
    print("Rationale (Strategy-Specific)")
    print("=" * 80)
    for _, row in result.iterrows():
        print(f"\n{row['Ticker']:6s} | {row['Strategy_Name']:18s}")
        print(f"       → {row['DTE_Rationale']}")
    
    print("\n" + "=" * 80)
    print("Multi-DTE Verification")
    print("=" * 80)
    print("\nTickers with multiple DTE windows (different strategies):")
    for ticker in ['AAPL', 'MSFT']:
        ticker_data = result[result['Ticker'] == ticker]
        if len(ticker_data) > 1:
            print(f"\n{ticker}:")
            for _, row in ticker_data.iterrows():
                print(f"  • {row['Strategy_Name']:20s} {row['Min_DTE']}-{row['Max_DTE']} DTE")
    
    print("\n" + "=" * 80)
    print("Validation")
    print("=" * 80)
    validate_dte_bounds(result)
    
    # Verify row count preservation
    assert len(result) == len(test_df), "Row count changed!"
    print(f"\n✅ Row count preserved: {len(test_df)} → {len(result)}")
    print(f"✅ All strategies processed independently")
