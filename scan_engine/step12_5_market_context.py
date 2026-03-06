"""
Step 12.5: Market Context Validation

PURPOSE:
    Validate if market conditions support this trade type.
    Expert traders don't fight the market - they wait for favorable conditions.

DESIGN PRINCIPLE:
    - VIX regime check (high VIX = sell premium, low VIX = buy premium)
    - Market stress gates (SPY down >2% = pause new long entries)
    - Sector strength validation (only trade stocks in strong sectors)
    - Correlation awareness (don't over-concentrate in single sector)

RATIONALE:
    Market context is critical - best setup in wrong market = loss.
    This enforces the discipline to wait for favorable conditions.

INPUTS:
    - strategy_type: Directional/Income/Volatility
    - ticker: Stock symbol
    - market_data: {vix, spy_change_pct, sector_strength, ...}

OUTPUTS (added columns):
    - Market_Context: 'FAVORABLE' | 'NEUTRAL' | 'UNFAVORABLE'
    - Market_Flags: List of market condition issues
    - Market_Proceed: True/False (should trade proceed?)

INTEGRATION:
    Called in Step 12 (Execution Gate) after all other validation.
    Final gate before marking as READY.

Updated: 2026-02-03 (Execution Readiness - Phase 1)
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Tuple, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


# Market context thresholds
# VIX bands calibrated to Sinclair 2020 Ch.4 (Positional Option Trading) S&P 500 quintile analysis:
#   VIX < 13.02  → thin premium (mean VP = 2.61 pts) — income edge minimal, reduce size
#   13.02–15.89  → below-average premium (VP = 3.37 pts)
#   15.89–19.42  → OPTIMAL harvest band (VP = 4.35 pts, best Sharpe) — full size allowed
#   19.42–24.15  → above-average (VP = 4.19 pts) — good but vol starting to spike
#   VIX > 24.15  → extreme (VP = 5.87 pts mean but SD explodes) — reduce size, tighten stops
# Operational thresholds round to nearest whole number for robustness.
VIX_HIGH = 25.0        # >25 = extreme vol regime (Sinclair 2020 Ch.4: > 24.15 quintile)
VIX_LOW = 13.0         # <13 = thin premium regime (Sinclair 2020 Ch.4: < 13.02 quintile)
VIX_OPTIMAL_LOW  = 16.0  # lower bound of optimal harvest band (~15.89)
VIX_OPTIMAL_HIGH = 19.0  # upper bound of optimal harvest band (~19.42)
SPY_STRESS = 2.0       # >2% move = market stress
SECTOR_WEAK = 30.0     # <30/100 = weak sector (rotation away)


def validate_market_context(
    df: pd.DataFrame,
    market_data: Optional[Dict] = None
) -> pd.DataFrame:
    """
    Validate market context for all tickers in DataFrame.

    Args:
        df: DataFrame from Step 12 with execution candidates
        market_data: Dictionary with market conditions {vix, spy_change_pct, sector_data}

    Returns:
        DataFrame with Market_Context, Market_Flags, Market_Proceed columns
    """

    if df.empty:
        logger.warning("⚠️ Empty DataFrame passed to Step 12.5")
        return df

    logger.info(f"🌍 Step 12.5: Market Context Validation for {len(df)} candidates")

    # Get market data (fetch if not provided)
    if market_data is None:
        market_data = _get_current_market_data()

    vix = market_data.get('vix', 15.0)
    logger.info(f"📊 Market Conditions: VIX={vix:.1f}, SPY={market_data.get('spy_change_pct', 0):+.1f}%")

    # Sinclair 2020 Ch.4: classify VIX band for sizing guidance (audit trail only — Step 8 sizes)
    if vix < VIX_LOW:
        _vix_band = 'THIN'
        _vix_note = f"VIX={vix:.1f} below optimal band — premium thin (Sinclair 2020 Ch.4: mean VP 2.61 pts). Reduce income size."
    elif VIX_OPTIMAL_LOW <= vix <= VIX_OPTIMAL_HIGH:
        _vix_band = 'OPTIMAL'
        _vix_note = f"VIX={vix:.1f} in optimal harvest band — strongest Sharpe for income (Sinclair 2020 Ch.4: mean VP 4.35 pts)."
    elif vix > VIX_HIGH:
        _vix_band = 'EXTREME'
        _vix_note = f"VIX={vix:.1f} extreme regime — premium large but SD explodes (Sinclair 2020 Ch.4: reduce size, tighten stops)."
    else:
        _vix_band = 'NORMAL'
        _vix_note = f"VIX={vix:.1f} normal range — income edge present."

    # Initialize columns
    df['Market_Context'] = 'NEUTRAL'
    df['Market_Flags'] = ''
    df['Market_Proceed'] = True
    df['VIX_Band'] = _vix_band
    df['VIX_Sizing_Note'] = _vix_note

    # Process each row
    for idx, row in df.iterrows():
        context, flags, proceed = _evaluate_market_context(row, market_data)

        df.at[idx, 'Market_Context'] = context
        df.at[idx, 'Market_Flags'] = ' | '.join(flags) if flags else 'None'
        df.at[idx, 'Market_Proceed'] = proceed

    # Log summary
    _log_market_context_summary(df, market_data)

    return df


def _evaluate_market_context(
    row: pd.Series,
    market_data: Dict
) -> Tuple[str, List[str], bool]:
    """
    Evaluate market context for a single ticker.

    Returns:
        (context, flags, proceed)
    """

    flags = []
    proceed = True

    ticker = row.get('Ticker', 'N/A')
    strategy_type = row.get('Strategy_Type', 'Unknown')

    vix = market_data.get('vix', 15.0)
    spy_change = market_data.get('spy_change_pct', 0.0)

    # 1. VIX Regime Check
    if vix > VIX_HIGH:
        # High VIX = high volatility environment
        if strategy_type in ['Directional', 'DIRECTIONAL']:
            # Buying premium in high VIX = overpaying
            flags.append(f'High VIX ({vix:.1f}) - premium buyers disadvantaged')
            proceed = False
        else:
            # Selling premium in high VIX = good
            flags.append(f'High VIX ({vix:.1f}) - favorable for premium sellers')

    elif vix < VIX_LOW:
        # Low VIX = low volatility environment
        if strategy_type in ['Income', 'INCOME']:
            # Selling premium in low VIX = cheap premiums
            flags.append(f'Low VIX ({vix:.1f}) - premium sellers collect less')
            # Don't block, but warn

    # 2. Market Stress Check
    if abs(spy_change) > SPY_STRESS:
        # Market moving >2% = stress event
        if spy_change < -SPY_STRESS and strategy_type in ['Directional', 'DIRECTIONAL']:
            # Market down >2%, going long = catching falling knife
            flags.append(f'Market stress (SPY {spy_change:+.1f}%) - avoid new long directional')
            proceed = False

        elif abs(spy_change) > SPY_STRESS:
            # Any >2% move = elevated risk
            flags.append(f'Market volatility (SPY {spy_change:+.1f}%) - heightened risk')

    # 3. Sector Strength Check (if available)
    sector = market_data.get('sectors', {}).get(ticker, {})
    sector_name = sector.get('name', 'Unknown')
    sector_strength = sector.get('strength', 50)  # 0-100 scale

    if sector_strength < SECTOR_WEAK:
        if strategy_type in ['Directional', 'DIRECTIONAL']:
            # Weak sector + going long = fighting rotation
            flags.append(f'{sector_name} sector weak ({sector_strength:.0f}/100) - rotation away')
            proceed = False

    # 4. Market Hours Check (if available)
    market_hours = market_data.get('market_hours', True)
    if not market_hours:
        flags.append('After hours - wider spreads, lower liquidity')
        # Don't block, but warn

    # 5. Earnings Season Check (broad market)
    earnings_season = market_data.get('earnings_season', False)
    if earnings_season:
        flags.append('Earnings season - elevated IV, higher volatility')
        # Don't block, but note

    # Classify context
    if not proceed:
        context = 'UNFAVORABLE'
    elif len(flags) == 0:
        context = 'FAVORABLE'
    else:
        context = 'NEUTRAL'

    return context, flags, proceed


def _get_current_market_data() -> Dict:
    """
    Get current market conditions (VIX, SPY, sector data).

    In production, this would fetch live data from APIs.
    For now, returns default/mock data.

    Returns:
        Dictionary with market conditions
    """

    try:
        # Try to get real market data from existing detector
        from core.shared.data_layer.market_stress_detector import check_market_stress, get_halt_reason

        stress_result = check_market_stress()

        market_data = {
            'vix': stress_result.get('vix', 15.0),
            'spy_change_pct': stress_result.get('spy_change_pct', 0.0),
            'market_stress': stress_result.get('is_stressed', False),
            'stress_reason': stress_result.get('reason', 'None'),
            'market_hours': True,  # Default
            'earnings_season': False,  # Default
            'sectors': {}  # Empty for now
        }

        logger.debug(f"📊 Fetched live market data: VIX={market_data['vix']:.1f}, SPY={market_data['spy_change_pct']:+.1f}%")

        return market_data

    except Exception as e:
        logger.warning(f"⚠️ Could not fetch live market data: {e}. Using defaults.")

        # Fallback to safe defaults
        return {
            'vix': 17.0,  # Neutral VIX
            'spy_change_pct': 0.0,  # Neutral market
            'market_stress': False,
            'stress_reason': 'None',
            'market_hours': True,
            'earnings_season': False,
            'sectors': {}
        }


def _log_market_context_summary(df: pd.DataFrame, market_data: Dict):
    """Log summary of market context validation."""

    context_counts = df['Market_Context'].value_counts().to_dict()
    total = len(df)

    favorable = context_counts.get('FAVORABLE', 0)
    neutral = context_counts.get('NEUTRAL', 0)
    unfavorable = context_counts.get('UNFAVORABLE', 0)

    logger.info(f"\n📊 Step 12.5 Market Context Summary:")
    logger.info(f"   Market Conditions: VIX={market_data.get('vix', 'N/A'):.1f}, SPY={market_data.get('spy_change_pct', 0):+.1f}%")
    logger.info(f"   ✅ Favorable: {favorable}/{total} ({favorable/total*100:.1f}%)")
    logger.info(f"   ⚠️  Neutral: {neutral}/{total} ({neutral/total*100:.1f}%)")
    logger.info(f"   ❌ Unfavorable: {unfavorable}/{total} ({unfavorable/total*100:.1f}%)")

    # Log blocked count
    blocked = (~df['Market_Proceed']).sum()
    if blocked > 0:
        logger.warning(f"   🚫 {blocked} trades blocked due to unfavorable market conditions")

        # Log top block reasons
        blocked_df = df[~df['Market_Proceed']]
        all_flags = blocked_df['Market_Flags']
        flag_list = []
        for flags_str in all_flags:
            if flags_str and flags_str != 'None':
                flag_list.extend(flags_str.split(' | '))

        if flag_list:
            from collections import Counter
            top_reasons = Counter(flag_list).most_common(3)
            logger.info(f"\n   Top Block Reasons:")
            for reason, count in top_reasons:
                logger.info(f"     • {reason}: {count}")


def filter_favorable_context(
    df: pd.DataFrame,
    allow_neutral: bool = True
) -> pd.DataFrame:
    """
    Filter DataFrame to only favorable/neutral market contexts.

    Args:
        df: DataFrame with Market_Context columns
        allow_neutral: If True, allow NEUTRAL context (default True)

    Returns:
        Filtered DataFrame
    """

    initial_count = len(df)

    if allow_neutral:
        df_filtered = df[df['Market_Context'].isin(['FAVORABLE', 'NEUTRAL'])].copy()
    else:
        df_filtered = df[df['Market_Context'] == 'FAVORABLE'].copy()

    filtered_count = len(df_filtered)
    removed_count = initial_count - filtered_count

    logger.info(f"🌍 Market Context Filter: {filtered_count}/{initial_count} passed ({removed_count} removed as unfavorable)")

    return df_filtered


def get_market_context_metrics(df: pd.DataFrame) -> Dict:
    """
    Get market context metrics for monitoring/analysis.

    Returns:
        Dictionary with context distribution metrics
    """

    metrics = {
        'total_tickers': len(df),
        'favorable_count': (df['Market_Context'] == 'FAVORABLE').sum(),
        'neutral_count': (df['Market_Context'] == 'NEUTRAL').sum(),
        'unfavorable_count': (df['Market_Context'] == 'UNFAVORABLE').sum(),
        'blocked_count': (~df['Market_Proceed']).sum(),
        'proceed_pct': (df['Market_Proceed'].sum() / len(df) * 100) if len(df) > 0 else 0
    }

    return metrics
