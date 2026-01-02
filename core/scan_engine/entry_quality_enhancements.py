"""
Scan-Time Entry Quality Enhancements

PURPOSE:
    Provide entry timing, execution quality, and assignment risk context
    using ONLY Schwab-provided data or locally computed derivatives.
    
    All functions are DESCRIPTIVE and NON-BLOCKING.
    They score and tag setups but never reject trades.
    
SCOPE:
    - Scan-time only (NO trade management)
    - No synthetic data (IV Rank, earnings dates not from Schwab)
    - No mutation of historical snapshots
    - No execution logic (informational only)
    
DATA SOURCES:
    - Schwab /quotes endpoint fields
    - Schwab /chains endpoint fields
    - Local HV calculations from price history
    
ARCHITECTURAL PRINCIPLES:
    - Enhancements are additive (never replace existing logic)
    - Missing data → neutral scoring (not rejection)
    - All tags/scores are for UI/dashboard, not automation
"""

import pandas as pd
import numpy as np
from typing import Dict, Tuple, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


# ============================================================
# 1. INTRADAY RANGE & COMPRESSION (ENTRY TIMING)
# ============================================================

def calculate_intraday_metrics(row: pd.Series) -> Dict[str, any]:
    """
    Calculate intraday range and gap metrics from quote data.
    
    Data Source: Schwab /quotes endpoint
    Fields Used: highPrice, lowPrice, lastPrice, openPrice, closePrice (previous)
    
    Metrics:
    - intraday_range_pct: (high - low) / last * 100
    - gap_pct: |open - prev_close| / prev_close * 100
    - intraday_position_pct: (last - low) / (high - low) * 100
    
    Tags:
    - COMPRESSION: range < 1% (breakout setup)
    - EXPANSION: range > 5% (already moving)
    - GAP_UP/GAP_DOWN: gap > 2% (momentum signal)
    - EARLY_DAY: position < 30% (still near low)
    - LATE_DAY: position > 70% (near high)
    
    Args:
        row: DataFrame row with quote fields
    
    Returns:
        Dict with metrics and tags
    """
    result = {
        'intraday_range_pct': np.nan,
        'gap_pct': np.nan,
        'intraday_position_pct': np.nan,
        'compression_tag': 'UNKNOWN',
        'gap_tag': 'NONE',
        'intraday_position_tag': 'UNKNOWN'
    }
    
    try:
        high = row.get('highPrice') or row.get('high_price')
        low = row.get('lowPrice') or row.get('low_price')
        last = row.get('lastPrice') or row.get('last_price')
        open_price = row.get('openPrice') or row.get('open_price')
        close_price = row.get('closePrice') or row.get('close_price')
        
        # Validate required fields
        if pd.isna(high) or pd.isna(low) or pd.isna(last):
            return result
        
        if high <= 0 or low <= 0 or last <= 0:
            return result
        
        # Calculate intraday range
        intraday_range = high - low
        range_pct = (intraday_range / last) * 100
        result['intraday_range_pct'] = range_pct
        
        # Tag compression/expansion
        if range_pct < 1.0:
            result['compression_tag'] = 'COMPRESSION'  # Breakout setup
        elif range_pct > 5.0:
            result['compression_tag'] = 'EXPANSION'  # Already moving
        else:
            result['compression_tag'] = 'NORMAL'
        
        # Calculate gap (if open and prev close available)
        if not pd.isna(open_price) and not pd.isna(close_price) and close_price > 0:
            gap = abs(open_price - close_price)
            gap_pct = (gap / close_price) * 100
            result['gap_pct'] = gap_pct
            
            if gap_pct > 2.0:
                if open_price > close_price:
                    result['gap_tag'] = 'GAP_UP'
                else:
                    result['gap_tag'] = 'GAP_DOWN'
            else:
                result['gap_tag'] = 'NO_GAP'
        
        # Calculate intraday position (where price is within range)
        if intraday_range > 0:
            position = (last - low) / intraday_range
            position_pct = position * 100
            result['intraday_position_pct'] = position_pct
            
            if position_pct < 30:
                result['intraday_position_tag'] = 'NEAR_LOW'  # Early entry opportunity
            elif position_pct > 70:
                result['intraday_position_tag'] = 'NEAR_HIGH'  # Late entry (chase)
            else:
                result['intraday_position_tag'] = 'MID_RANGE'
        
    except Exception as e:
        logger.debug(f"Intraday metrics calculation failed: {e}")
    
    return result


# ============================================================
# 2. 52-WEEK CONTEXT (REGIME FILTER)
# ============================================================

def calculate_52w_context(row: pd.Series) -> Dict[str, any]:
    """
    Calculate 52-week positioning context.
    
    Data Source: Schwab /quotes endpoint
    Fields Used: 52WeekHigh, 52WeekLow, lastPrice
    
    Metrics:
    - pct_from_52w_high: (52W_high - price) / 52W_high * 100
    - pct_from_52w_low: (price - 52W_low) / 52W_low * 100
    - range_position: Where price sits in 52W range (0-100%)
    
    Tags:
    - NEAR_52W_HIGH: < 2% from high (momentum)
    - NEAR_52W_LOW: < 10% from low (contrarian)
    - MID_RANGE: Between extremes
    
    Strategy Context:
    - NEAR_HIGH → Favor momentum (long calls, bull spreads)
    - NEAR_LOW → Favor contrarian (CSP, straddles)
    - MID_RANGE → Neutral (any strategy valid)
    
    Args:
        row: DataFrame row with quote fields
    
    Returns:
        Dict with metrics and tags
    """
    result = {
        'pct_from_52w_high': np.nan,
        'pct_from_52w_low': np.nan,
        '52w_range_position': np.nan,
        '52w_regime_tag': 'UNKNOWN',
        '52w_strategy_context': 'NEUTRAL'
    }
    
    try:
        high_52w = row.get('52WeekHigh') or row.get('week52_high')
        low_52w = row.get('52WeekLow') or row.get('week52_low')
        last = row.get('lastPrice') or row.get('last_price')
        
        # Validate required fields
        if pd.isna(high_52w) or pd.isna(low_52w) or pd.isna(last):
            return result
        
        if high_52w <= 0 or low_52w <= 0 or last <= 0 or high_52w < low_52w:
            return result
        
        # Calculate distance from highs/lows
        pct_from_high = ((high_52w - last) / high_52w) * 100
        pct_from_low = ((last - low_52w) / low_52w) * 100
        
        result['pct_from_52w_high'] = pct_from_high
        result['pct_from_52w_low'] = pct_from_low
        
        # Calculate position in 52W range
        range_52w = high_52w - low_52w
        if range_52w > 0:
            position = (last - low_52w) / range_52w
            result['52w_range_position'] = position * 100
        
        # Tag regime
        if pct_from_high < 2.0:
            result['52w_regime_tag'] = 'NEAR_52W_HIGH'
            result['52w_strategy_context'] = 'MOMENTUM'
        elif pct_from_low < 10.0:
            result['52w_regime_tag'] = 'NEAR_52W_LOW'
            result['52w_strategy_context'] = 'CONTRARIAN'
        else:
            result['52w_regime_tag'] = 'MID_RANGE'
            result['52w_strategy_context'] = 'NEUTRAL'
        
    except Exception as e:
        logger.debug(f"52W context calculation failed: {e}")
    
    return result


# ============================================================
# 3. DAILY MOMENTUM FILTER (TRADE READINESS)
# ============================================================

def calculate_daily_momentum(row: pd.Series) -> Dict[str, any]:
    """
    Calculate daily momentum context.
    
    Data Source: Schwab /quotes endpoint
    Fields Used: netChange, netPercentChange
    
    Metrics:
    - net_change: $ change from previous close
    - net_percent_change: % change from previous close
    
    Tags:
    - STRONG_UP_DAY: +2% or more
    - STRONG_DOWN_DAY: -2% or more
    - FLAT_DAY: < 0.5% move
    - NORMAL: 0.5-2% move
    
    Entry Quality Context:
    - STRONG_UP → May be late for longs (wait for pullback)
    - STRONG_DOWN → May be late for shorts (wait for bounce)
    - FLAT → Early entry opportunity (before expansion)
    
    Args:
        row: DataFrame row with quote fields
    
    Returns:
        Dict with metrics and tags
    """
    result = {
        'net_change': np.nan,
        'net_percent_change': np.nan,
        'momentum_tag': 'UNKNOWN',
        'entry_timing_context': 'UNKNOWN'
    }
    
    try:
        net_change = row.get('netChange') or row.get('net_change')
        net_pct = row.get('netPercentChange') or row.get('net_percent_change')
        
        if not pd.isna(net_change):
            result['net_change'] = net_change
        
        if not pd.isna(net_pct):
            result['net_percent_change'] = net_pct
            
            # Tag momentum
            if net_pct >= 2.0:
                result['momentum_tag'] = 'STRONG_UP_DAY'
                result['entry_timing_context'] = 'LATE_LONG'  # May have missed entry
            elif net_pct <= -2.0:
                result['momentum_tag'] = 'STRONG_DOWN_DAY'
                result['entry_timing_context'] = 'LATE_SHORT'
            elif abs(net_pct) < 0.5:
                result['momentum_tag'] = 'FLAT_DAY'
                result['entry_timing_context'] = 'EARLY'  # Good entry timing
            else:
                result['momentum_tag'] = 'NORMAL'
                result['entry_timing_context'] = 'MODERATE'
        
    except Exception as e:
        logger.debug(f"Daily momentum calculation failed: {e}")
    
    return result


# ============================================================
# 4. DIVIDEND ASSIGNMENT RISK (SHORT OPTIONS)
# ============================================================

def calculate_dividend_risk(
    dividend_date: any,
    dividend_yield: float,
    option_dte: int,
    strategy_name: str
) -> Dict[str, any]:
    """
    Calculate dividend assignment risk for short option strategies.
    
    Data Source: Schwab /quotes endpoint
    Fields Used: dividendDate, dividendYield
    
    Risk Assessment:
    - Ex-div within option window + ITM short → HIGH_RISK
    - Ex-div within option window + OTM short → MODERATE_RISK
    - No ex-div in window → LOW_RISK
    - No dividend data → UNKNOWN
    
    Affected Strategies:
    - Covered Call (short call risk)
    - Cash-Secured Put (short put risk)
    - Credit Spreads (short leg risk)
    - Iron Condors (short legs risk)
    
    Args:
        dividend_date: Ex-dividend date (string or datetime)
        dividend_yield: Annual dividend yield %
        option_dte: Days to expiration
        strategy_name: Strategy name (to determine short legs)
    
    Returns:
        Dict with risk assessment
    """
    result = {
        'dividend_risk': 'UNKNOWN',
        'days_to_dividend': np.nan,
        'dividend_yield': np.nan,
        'dividend_notes': ''
    }
    
    try:
        # Check if strategy has short legs
        short_leg_strategies = [
            'Covered Call', 'Buy-Write', 'Cash-Secured Put', 'CSP',
            'Credit Spread', 'Short Iron Condor', 'Iron Condor'
        ]
        
        if strategy_name not in short_leg_strategies:
            result['dividend_risk'] = 'N/A'
            result['dividend_notes'] = 'No short legs'
            return result
        
        # Parse dividend date
        if pd.isna(dividend_date) or dividend_date == '':
            result['dividend_risk'] = 'UNKNOWN'
            result['dividend_notes'] = 'No dividend date available'
            return result
        
        # Convert to datetime if string
        if isinstance(dividend_date, str):
            try:
                div_dt = pd.to_datetime(dividend_date)
            except:
                result['dividend_risk'] = 'UNKNOWN'
                result['dividend_notes'] = f'Invalid date format: {dividend_date}'
                return result
        else:
            div_dt = pd.to_datetime(dividend_date)
        
        # Calculate days to dividend
        today = pd.Timestamp.now()
        days_to_div = (div_dt - today).days
        result['days_to_dividend'] = days_to_div
        
        if not pd.isna(dividend_yield):
            result['dividend_yield'] = dividend_yield
        
        # Assess risk
        if days_to_div < 0:
            # Dividend already passed
            result['dividend_risk'] = 'LOW'
            result['dividend_notes'] = 'Ex-div date already passed'
        elif 0 <= days_to_div < option_dte:
            # Ex-div within option window
            if dividend_yield > 2.0:
                result['dividend_risk'] = 'HIGH'
                result['dividend_notes'] = f'Ex-div in {days_to_div} days (yield {dividend_yield:.2f}%)'
            else:
                result['dividend_risk'] = 'MODERATE'
                result['dividend_notes'] = f'Ex-div in {days_to_div} days (low yield)'
        else:
            # Ex-div after expiration
            result['dividend_risk'] = 'LOW'
            result['dividend_notes'] = f'Ex-div after expiration ({days_to_div} days)'
        
    except Exception as e:
        logger.debug(f"Dividend risk calculation failed: {e}")
        result['dividend_notes'] = f'Calculation error: {str(e)}'
    
    return result


# ============================================================
# 5. EXECUTION QUALITY: BID/ASK DEPTH
# ============================================================

def calculate_depth_quality(row: pd.Series) -> Dict[str, any]:
    """
    Calculate bid/ask depth and balance for execution quality.
    
    Data Source: Schwab /chains endpoint (per-contract)
    Fields Used: bidSize, askSize, openInterest, bid, ask
    
    Metrics:
    - total_depth: bidSize + askSize (contracts)
    - depth_imbalance: |bidSize - askSize| / total_depth (0-1)
    - depth_to_oi_ratio: total_depth / openInterest
    
    Tags:
    - DEEP_BOOK: depth >= 50 contracts
    - BALANCED_BOOK: imbalance < 30%
    - THIN_BOOK: depth < 20
    - IMBALANCED_BOOK: imbalance > 50%
    
    Execution Context:
    - DEEP + BALANCED → Excellent fill quality
    - DEEP + IMBALANCED → Good size, but may move price
    - THIN + BALANCED → Fair fill, limited size
    - THIN + IMBALANCED → Poor fill quality
    
    Args:
        row: DataFrame row with contract fields
    
    Returns:
        Dict with depth metrics and tags
    """
    result = {
        'bid_size': np.nan,
        'ask_size': np.nan,
        'total_depth': np.nan,
        'depth_imbalance': np.nan,
        'depth_to_oi_ratio': np.nan,
        'depth_tag': 'UNKNOWN',
        'balance_tag': 'UNKNOWN',
        'execution_quality': 'UNKNOWN'
    }
    
    try:
        bid_size = row.get('bid_size') or row.get('bidSize') or row.get('Bid_Size')
        ask_size = row.get('ask_size') or row.get('askSize') or row.get('Ask_Size')
        oi = row.get('Open_Interest') or row.get('openInterest') or row.get('open_interest')
        
        # Validate
        if pd.isna(bid_size) or pd.isna(ask_size):
            return result
        
        if bid_size < 0 or ask_size < 0:
            return result
        
        result['bid_size'] = bid_size
        result['ask_size'] = ask_size
        
        # Calculate total depth
        total_depth = bid_size + ask_size
        result['total_depth'] = total_depth
        
        # Calculate imbalance
        if total_depth > 0:
            imbalance = abs(bid_size - ask_size) / total_depth
            result['depth_imbalance'] = imbalance
            
            # Tag balance
            if imbalance < 0.30:
                result['balance_tag'] = 'BALANCED'
            elif imbalance < 0.50:
                result['balance_tag'] = 'MODERATE_IMBALANCE'
            else:
                result['balance_tag'] = 'IMBALANCED'
        
        # Tag depth
        if total_depth >= 50:
            result['depth_tag'] = 'DEEP_BOOK'
        elif total_depth >= 20:
            result['depth_tag'] = 'ADEQUATE_BOOK'
        else:
            result['depth_tag'] = 'THIN_BOOK'
        
        # Calculate depth to OI ratio
        if not pd.isna(oi) and oi > 0:
            result['depth_to_oi_ratio'] = total_depth / oi
        
        # Overall execution quality
        if result['depth_tag'] == 'DEEP_BOOK' and result['balance_tag'] == 'BALANCED':
            result['execution_quality'] = 'EXCELLENT'
        elif result['depth_tag'] in ['DEEP_BOOK', 'ADEQUATE_BOOK'] and result['balance_tag'] != 'IMBALANCED':
            result['execution_quality'] = 'GOOD'
        elif result['depth_tag'] == 'THIN_BOOK' or result['balance_tag'] == 'IMBALANCED':
            result['execution_quality'] = 'FAIR'
        else:
            result['execution_quality'] = 'POOR'
        
    except Exception as e:
        logger.debug(f"Depth quality calculation failed: {e}")
    
    return result


# ============================================================
# 6. ENTRY READINESS SCORE (COMPOSITE)
# ============================================================

def calculate_entry_readiness(row: pd.Series) -> Dict[str, any]:
    """
    Calculate composite entry readiness score.
    
    Combines:
    - Intraday compression/expansion (timing)
    - Daily momentum (avoid late entries)
    - 52W context (regime alignment)
    - Execution depth (fill quality)
    
    Readiness Levels:
    - READY_NOW: Compression + early day + good depth
    - EARLY: Setup forming, wait for confirmation
    - LATE: Already moved, missed optimal entry
    - WAIT: Poor timing or execution conditions
    
    Scoring (0-100):
    - Timing: 40 points (compression + position + momentum)
    - Execution: 30 points (depth + balance + spread)
    - Regime: 30 points (52W context + strategy alignment)
    
    Args:
        row: DataFrame row with all enhancement fields
    
    Returns:
        Dict with readiness score and assessment
    """
    result = {
        'entry_readiness_score': 0,
        'entry_readiness': 'UNKNOWN',
        'readiness_notes': ''
    }
    
    notes = []
    score = 0
    
    try:
        # --- Timing Component (40 points) ---
        timing_score = 0
        
        # Compression/expansion (15 points)
        compression = row.get('compression_tag')
        if compression == 'COMPRESSION':
            timing_score += 15
            notes.append('Compression setup')
        elif compression == 'NORMAL':
            timing_score += 10
        elif compression == 'EXPANSION':
            timing_score += 5
            notes.append('Already expanded')
        
        # Intraday position (10 points)
        position = row.get('intraday_position_tag')
        if position == 'NEAR_LOW':
            timing_score += 10
            notes.append('Early entry')
        elif position == 'MID_RANGE':
            timing_score += 7
        elif position == 'NEAR_HIGH':
            timing_score += 3
            notes.append('Late entry (chase)')
        
        # Daily momentum (15 points)
        momentum = row.get('entry_timing_context')
        if momentum == 'EARLY':
            timing_score += 15
        elif momentum == 'MODERATE':
            timing_score += 10
        elif momentum in ['LATE_LONG', 'LATE_SHORT']:
            timing_score += 3
            notes.append('Late entry')
        
        score += timing_score
        
        # --- Execution Component (30 points) ---
        execution_score = 0
        
        # Execution quality (20 points)
        exec_qual = row.get('execution_quality')
        if exec_qual == 'EXCELLENT':
            execution_score += 20
        elif exec_qual == 'GOOD':
            execution_score += 15
        elif exec_qual == 'FAIR':
            execution_score += 10
        elif exec_qual == 'POOR':
            execution_score += 3
            notes.append('Poor execution quality')
        
        # Spread quality (10 points) - from existing liquidity_grade
        liq_grade = row.get('Liquidity_Grade')
        if liq_grade == 'Excellent':
            execution_score += 10
        elif liq_grade == 'Good':
            execution_score += 7
        elif liq_grade == 'Acceptable':
            execution_score += 5
        elif liq_grade == 'Thin':
            execution_score += 2
            notes.append('Thin liquidity')
        
        score += execution_score
        
        # --- Regime Component (30 points) ---
        regime_score = 0
        
        # 52W context (15 points)
        regime_52w = row.get('52w_regime_tag')
        if regime_52w in ['NEAR_52W_HIGH', 'NEAR_52W_LOW']:
            regime_score += 15
            notes.append(f'{regime_52w}')
        elif regime_52w == 'MID_RANGE':
            regime_score += 10
        
        # Strategy alignment with regime (15 points)
        strategy_context = row.get('52w_strategy_context')
        strategy_name = row.get('Strategy_Name', '')
        
        # Check if strategy aligns with regime
        if strategy_context == 'MOMENTUM' and any(s in strategy_name for s in ['Call', 'Bull']):
            regime_score += 15
            notes.append('Momentum alignment')
        elif strategy_context == 'CONTRARIAN' and any(s in strategy_name for s in ['Put', 'Straddle']):
            regime_score += 15
            notes.append('Contrarian alignment')
        elif strategy_context == 'NEUTRAL':
            regime_score += 10
        else:
            regime_score += 5
        
        score += regime_score
        
        # --- Final Assessment ---
        result['entry_readiness_score'] = min(100, score)
        
        if score >= 75:
            result['entry_readiness'] = 'READY_NOW'
        elif score >= 60:
            result['entry_readiness'] = 'EARLY'
        elif score >= 40:
            result['entry_readiness'] = 'LATE'
        else:
            result['entry_readiness'] = 'WAIT'
        
        result['readiness_notes'] = '; '.join(notes) if notes else 'No specific timing signals'
        
    except Exception as e:
        logger.debug(f"Entry readiness calculation failed: {e}")
        result['readiness_notes'] = f'Calculation error: {str(e)}'
    
    return result


# ============================================================
# BATCH ENRICHMENT FUNCTIONS (for pipeline integration)
# ============================================================

def enrich_snapshot_with_entry_quality(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich snapshot DataFrame with all entry quality metrics.
    
    Applied in Step 2 (load_snapshot).
    
    Adds fields:
    - Intraday metrics (range, gap, position)
    - 52W context (regime, strategy alignment)
    - Daily momentum (timing context)
    
    Args:
        df: Snapshot DataFrame from Step 0
    
    Returns:
        Enriched DataFrame with new columns
    """
    logger.info("🔍 Enriching snapshot with entry quality metrics...")
    
    if df.empty:
        logger.warning("Empty DataFrame, skipping entry quality enrichment")
        return df
    
    df_enriched = df.copy()
    
    # Initialize columns
    intraday_cols = [
        'intraday_range_pct', 'gap_pct', 'intraday_position_pct',
        'compression_tag', 'gap_tag', 'intraday_position_tag'
    ]
    week52_cols = [
        'pct_from_52w_high', 'pct_from_52w_low', '52w_range_position',
        '52w_regime_tag', '52w_strategy_context'
    ]
    momentum_cols = [
        'net_change', 'net_percent_change', 'momentum_tag', 'entry_timing_context'
    ]
    
    for col in intraday_cols + week52_cols + momentum_cols:
        if col not in df_enriched.columns:
            df_enriched[col] = np.nan if 'pct' in col or 'change' in col else 'UNKNOWN'
    
    # Apply enrichment functions
    for idx, row in df_enriched.iterrows():
        # Intraday metrics
        intraday = calculate_intraday_metrics(row)
        for key, val in intraday.items():
            df_enriched.at[idx, key] = val
        
        # 52W context
        week52 = calculate_52w_context(row)
        for key, val in week52.items():
            df_enriched.at[idx, key] = val
        
        # Daily momentum
        momentum = calculate_daily_momentum(row)
        for key, val in momentum.items():
            df_enriched.at[idx, key] = val
    
    # Log summary
    compression_count = (df_enriched['compression_tag'] == 'COMPRESSION').sum()
    near_high_count = (df_enriched['52w_regime_tag'] == 'NEAR_52W_HIGH').sum()
    early_count = (df_enriched['entry_timing_context'] == 'EARLY').sum()
    
    logger.info(f"✅ Entry quality enrichment complete:")
    logger.info(f"   - Compression setups: {compression_count}")
    logger.info(f"   - Near 52W high: {near_high_count}")
    logger.info(f"   - Early entry timing: {early_count}")
    
    return df_enriched


def enrich_contracts_with_execution_quality(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich contract DataFrame with execution quality metrics.
    
    Applied in Step 9B (fetch_contracts_schwab).
    
    Adds fields:
    - Depth metrics (bid/ask size, imbalance)
    - Execution quality grade
    - Dividend assignment risk
    
    Args:
        df: Contract DataFrame from Step 9B
    
    Returns:
        Enriched DataFrame with new columns
    """
    logger.info("🔍 Enriching contracts with execution quality metrics...")
    
    if df.empty:
        logger.warning("Empty DataFrame, skipping execution quality enrichment")
        return df
    
    df_enriched = df.copy()
    
    # Initialize columns
    depth_cols = [
        'bid_size', 'ask_size', 'total_depth', 'depth_imbalance',
        'depth_to_oi_ratio', 'depth_tag', 'balance_tag', 'execution_quality'
    ]
    dividend_cols = [
        'dividend_risk', 'days_to_dividend', 'dividend_yield', 'dividend_notes'
    ]
    
    for col in depth_cols:
        if col not in df_enriched.columns:
            df_enriched[col] = np.nan if 'size' in col or 'depth' in col or 'ratio' in col else 'UNKNOWN'
    
    for col in dividend_cols:
        if col not in df_enriched.columns:
            df_enriched[col] = np.nan if 'days' in col or 'yield' in col else 'UNKNOWN'
    
    # Apply enrichment functions
    for idx, row in df_enriched.iterrows():
        # Depth quality
        depth = calculate_depth_quality(row)
        for key, val in depth.items():
            df_enriched.at[idx, key] = val
        
        # Dividend risk (if applicable)
        div_date = row.get('dividendDate') or row.get('dividend_date')
        div_yield = row.get('dividendYield') or row.get('dividend_yield')
        dte = row.get('Actual_DTE') or row.get('actual_dte')
        strategy = row.get('Strategy_Name') or row.get('strategy_name')
        
        if not pd.isna(dte) and not pd.isna(strategy):
            div_risk = calculate_dividend_risk(div_date, div_yield, dte, strategy)
            for key, val in div_risk.items():
                df_enriched.at[idx, key] = val
    
    # Log summary
    deep_book_count = (df_enriched['depth_tag'] == 'DEEP_BOOK').sum()
    excellent_exec_count = (df_enriched['execution_quality'] == 'EXCELLENT').sum()
    high_div_risk_count = (df_enriched['dividend_risk'] == 'HIGH').sum()
    
    logger.info(f"✅ Execution quality enrichment complete:")
    logger.info(f"   - Deep book contracts: {deep_book_count}")
    logger.info(f"   - Excellent execution: {excellent_exec_count}")
    logger.info(f"   - High dividend risk: {high_div_risk_count}")
    
    return df_enriched


def enrich_evaluation_with_entry_readiness(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich evaluation DataFrame with entry readiness scoring.
    
    Applied in Step 11 (independent_evaluation).
    
    Adds fields:
    - Entry readiness score (0-100)
    - Entry readiness assessment (READY_NOW/EARLY/LATE/WAIT)
    - Readiness notes (explanation)
    
    Args:
        df: Evaluation DataFrame from Step 11
    
    Returns:
        Enriched DataFrame with readiness scoring
    """
    logger.info("🔍 Calculating entry readiness scores...")
    
    if df.empty:
        logger.warning("Empty DataFrame, skipping entry readiness scoring")
        return df
    
    df_enriched = df.copy()
    
    # Initialize columns
    readiness_cols = ['entry_readiness_score', 'entry_readiness', 'readiness_notes']
    for col in readiness_cols:
        if col not in df_enriched.columns:
            df_enriched[col] = 0 if 'score' in col else 'UNKNOWN'
    
    # Calculate readiness for each row
    for idx, row in df_enriched.iterrows():
        readiness = calculate_entry_readiness(row)
        for key, val in readiness.items():
            df_enriched.at[idx, key] = val
    
    # Log summary
    ready_now_count = (df_enriched['entry_readiness'] == 'READY_NOW').sum()
    early_count = (df_enriched['entry_readiness'] == 'EARLY').sum()
    late_count = (df_enriched['entry_readiness'] == 'LATE').sum()
    wait_count = (df_enriched['entry_readiness'] == 'WAIT').sum()
    
    avg_score = df_enriched['entry_readiness_score'].mean()
    
    logger.info(f"✅ Entry readiness scoring complete:")
    logger.info(f"   - READY_NOW: {ready_now_count}")
    logger.info(f"   - EARLY: {early_count}")
    logger.info(f"   - LATE: {late_count}")
    logger.info(f"   - WAIT: {wait_count}")
    logger.info(f"   - Average score: {avg_score:.1f}/100")
    
    return df_enriched
