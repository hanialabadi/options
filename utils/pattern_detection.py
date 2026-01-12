"""
Pattern Detection Utilities

Implements Bulkowski chart patterns and Nison candlestick patterns
using pandas-based detection (no TA-Lib dependency required).

RAG Sources:
- Bulkowski: Encyclopedia of Chart Patterns (statistical edge)
- Nison: Japanese Candlestick Charting Techniques (entry timing)
"""

import pandas as pd
import numpy as np
import logging
from typing import Tuple, Optional
from core.scan_engine.price_history_loader import load_price_history

logger = logging.getLogger(__name__)


def detect_bulkowski_patterns(ticker: str, df_price: pd.DataFrame = None) -> Tuple[Optional[str], float]:
    """
    Detect high-probability chart patterns per Bulkowski's statistical analysis.
    
    Bulkowski patterns with >60% success rate (Encyclopedia of Chart Patterns):
    - Bull Flag: 70% success, uptrend continuation
    - Ascending Triangle: 63% success, bullish breakout
    - Cup and Handle: 65% success, bullish continuation
    - Double Bottom: 70% success, bullish reversal
    - Bear Flag: 70% success, downtrend continuation
    - Descending Triangle: 64% success, bearish breakout
    
    Args:
        ticker: Stock symbol
        df_price: Price dataframe with OHLC data (optional, will fetch if None)
    
    Returns:
        (pattern_name, confidence): Pattern name and Bulkowski's success rate (0-100)
        Returns (None, 0.0) if no pattern detected
    
    Theory (Bulkowski):
        "Chart patterns are not magic - they're statistical edges from
        recurring market structure. Success rate >60% = tradeable edge."
    
    Example:
        >>> pattern, confidence = detect_bulkowski_patterns('AAPL')
        >>> if pattern == 'Bull Flag' and confidence > 70:
        ...     print("Strong bullish continuation pattern")
    """
    try:
        # Fetch price data if not provided
        if df_price is None or df_price.empty:
            df_price, source = load_price_history(ticker, days=90)
        
        if df_price.empty or len(df_price) < 20:
            return (None, 0.0)
        
        # Ensure we have required columns
        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        if not all(col in df_price.columns for col in required_cols):
            logger.debug(f"{ticker}: Missing OHLC columns for pattern detection (have: {df_price.columns.tolist()})")
            return (None, 0.0)
        
        # Calculate indicators for pattern detection
        df = df_price.copy()
        df['SMA20'] = df['Close'].rolling(20).mean()
        df['SMA50'] = df['Close'].rolling(50).mean()
        df['High_20'] = df['High'].rolling(20).max()
        df['Low_20'] = df['Low'].rolling(20).min()
        
        # Get recent data (last 20 days for pattern analysis)
        recent = df.tail(20)
        if len(recent) < 20:
            return (None, 0.0)
        
        close_prices = recent['Close'].values
        high_prices = recent['High'].values
        low_prices = recent['Low'].values
        
        # 1. BULL FLAG (Bulkowski: 70% success rate)
        # Criteria: Strong uptrend + consolidation + upside breakout
        if len(df) >= 30:
            prev_30 = df.tail(30)
            # Check for prior uptrend (20%+ gain in last 30 days)
            price_change = (prev_30['Close'].iloc[-1] / prev_30['Close'].iloc[0] - 1) * 100
            if price_change > 15:  # Strong prior uptrend
                # Check for consolidation (range-bound last 10 days)
                last_10 = recent.tail(10)
                high_low_range = (last_10['High'].max() - last_10['Low'].min()) / last_10['Close'].mean() * 100
                if 2 < high_low_range < 8:  # Tight consolidation (2-8%)
                    # Check for breakout
                    if recent['Close'].iloc[-1] > recent['High'].iloc[-2]:
                        return ('Bull Flag', 70.0)
        
        # 2. ASCENDING TRIANGLE (Bulkowski: 63% success rate)
        # Criteria: Flat resistance + rising support + bullish breakout
        if len(recent) >= 15:
            # Check for flat resistance (highs at similar level)
            recent_highs = high_prices[-10:]
            resistance_line = np.mean(recent_highs[-5:])
            resistance_std = np.std(recent_highs[-5:])
            
            # Check for rising support (higher lows)
            lows = low_prices[-10:]
            if len(lows) >= 3:
                if lows[-1] > lows[-3] > lows[-5]:  # Rising lows
                    if resistance_std < resistance_line * 0.02:  # Flat top
                        if close_prices[-1] > resistance_line:  # Breakout
                            return ('Ascending Triangle', 63.0)
        
        # 3. DOUBLE BOTTOM (Bulkowski: 70% success rate)
        # Criteria: Two lows at similar level + breakout above middle high
        if len(recent) >= 20:
            lows = low_prices
            # Find two lowest points
            low_indices = np.argsort(lows)[:2]
            if len(low_indices) == 2:
                low1_idx, low2_idx = sorted(low_indices)
                low1, low2 = lows[low1_idx], lows[low2_idx]
                
                # Check lows are similar (within 3%)
                if abs(low1 - low2) / low1 < 0.03:
                    # Check there's a middle high between them
                    if low2_idx > low1_idx + 2:
                        middle_high = np.max(high_prices[low1_idx:low2_idx])
                        # Check for breakout above middle high
                        if close_prices[-1] > middle_high:
                            return ('Double Bottom', 70.0)
        
        # 4. CUP AND HANDLE (Bulkowski: 65% success rate)
        # Criteria: U-shaped bottom + consolidation handle + breakout
        if len(df) >= 60:
            last_60 = df.tail(60)
            # Check for U-shape (gradual decline then rise)
            first_third = last_60['Close'].iloc[:20].mean()
            middle_third = last_60['Close'].iloc[20:40].mean()
            last_third = last_60['Close'].iloc[40:].mean()
            
            if middle_third < first_third * 0.95 and last_third > middle_third:
                # Check for handle (small consolidation at top)
                last_10 = recent.tail(10)
                handle_range = (last_10['High'].max() - last_10['Low'].min()) / last_10['Close'].mean() * 100
                if 2 < handle_range < 5:
                    # Check for breakout
                    if close_prices[-1] > recent['High'].iloc[-2]:
                        return ('Cup and Handle', 65.0)
        
        # 5. BEAR FLAG (Bulkowski: 70% success rate) - for put strategies
        if len(df) >= 30:
            prev_30 = df.tail(30)
            # Check for prior downtrend (15%+ decline)
            price_change = (prev_30['Close'].iloc[-1] / prev_30['Close'].iloc[0] - 1) * 100
            if price_change < -10:  # Strong prior downtrend
                # Check for upward consolidation (counter-trend bounce)
                last_10 = recent.tail(10)
                if last_10['Close'].iloc[-1] > last_10['Close'].iloc[0]:
                    # Check for breakdown
                    if recent['Close'].iloc[-1] < recent['Low'].iloc[-2]:
                        return ('Bear Flag', 70.0)
        
        # 6. DESCENDING TRIANGLE (Bulkowski: 64% success rate)
        # Criteria: Flat support + declining resistance + bearish breakdown
        if len(recent) >= 15:
            recent_lows = low_prices[-10:]
            support_line = np.mean(recent_lows[-5:])
            support_std = np.std(recent_lows[-5:])
            
            # Check for declining resistance (lower highs)
            highs = high_prices[-10:]
            if len(highs) >= 3:
                if highs[-1] < highs[-3] < highs[-5]:  # Declining highs
                    if support_std < support_line * 0.02:  # Flat bottom
                        if close_prices[-1] < support_line:  # Breakdown
                            return ('Descending Triangle', 64.0)
        
        # No pattern detected
        return (None, 0.0)
    
    except Exception as e:
        logger.debug(f"{ticker}: Pattern detection failed: {e}")
        return (None, 0.0)


def detect_nison_candlestick(ticker: str, df_price: pd.DataFrame = None) -> Tuple[Optional[str], str]:
    """
    Detect Nison candlestick reversal patterns for entry timing.
    
    Nison high-reliability patterns (Japanese Candlestick Charting):
    Bullish:
    - Hammer (at support): Strong reversal signal
    - Bullish Engulfing: Momentum shift
    - Morning Star: Three-candle reversal
    - Piercing Line: Strong buying pressure
    
    Bearish:
    - Shooting Star (at resistance): Reversal warning
    - Bearish Engulfing: Momentum shift
    - Evening Star: Three-candle reversal
    - Dark Cloud Cover: Selling pressure
    
    Args:
        ticker: Stock symbol
        df_price: Price dataframe with OHLC (optional, will fetch if None)
    
    Returns:
        (pattern_name, entry_timing_quality): Pattern name and timing quality
        entry_timing_quality: "Strong", "Moderate", "Weak", or None
    
    Theory (Nison):
        "Candlestick patterns reveal the psychology of market participants.
        Reversal patterns at key levels = high-probability entry timing."
    
    Example:
        >>> pattern, timing = detect_nison_candlestick('AAPL')
        >>> if pattern == 'Bullish Engulfing' and timing == 'Strong':
        ...     print("Strong bullish entry signal")
    """
    try:
        # Fetch price data if not provided
        if df_price is None or df_price.empty:
            df_price, source = load_price_history(ticker, days=30)
        
        if df_price.empty or len(df_price) < 5:
            return (None, None)
        
        # Ensure we have OHLC
        required_cols = ['Open', 'High', 'Low', 'Close']
        if not all(col in df_price.columns for col in required_cols):
            logger.debug(f"{ticker}: Missing OHLC columns for candlestick (have: {df_price.columns.tolist()})")
            return (None, None)
        
        df = df_price.copy()
        
        # Get last 5 candles for pattern detection
        if len(df) < 5:
            return (None, None)
        
        recent = df.tail(5)
        
        # Extract last 3 candles for analysis
        candle_0 = recent.iloc[-3]  # Two days ago
        candle_1 = recent.iloc[-2]  # Yesterday
        candle_2 = recent.iloc[-1]  # Today (most recent)
        
        # Calculate body and shadow sizes
        body_2 = abs(candle_2['Close'] - candle_2['Open'])
        upper_shadow_2 = candle_2['High'] - max(candle_2['Open'], candle_2['Close'])
        lower_shadow_2 = min(candle_2['Open'], candle_2['Close']) - candle_2['Low']
        range_2 = candle_2['High'] - candle_2['Low']
        
        body_1 = abs(candle_1['Close'] - candle_1['Open'])
        range_1 = candle_1['High'] - candle_1['Low']
        
        # Calculate support/resistance context (SMA20 as reference)
        if len(df) >= 20:
            sma20 = df['Close'].rolling(20).mean().iloc[-1]
            at_support = candle_2['Close'] < sma20 * 1.02  # Within 2% of SMA20
            at_resistance = candle_2['Close'] > sma20 * 0.98
        else:
            at_support = True
            at_resistance = True
        
        # 1. HAMMER (Nison: Strong bullish reversal at support)
        # Criteria: Small body, long lower shadow (2x body), tiny upper shadow
        if lower_shadow_2 > body_2 * 2 and upper_shadow_2 < body_2 * 0.3:
            if candle_2['Close'] > candle_2['Open']:  # Bullish close
                timing = "Strong" if at_support else "Moderate"
                return ('Hammer', timing)
        
        # 2. BULLISH ENGULFING (Nison: Strong momentum shift)
        # Criteria: Bearish candle followed by larger bullish candle that engulfs it
        if candle_1['Close'] < candle_1['Open']:  # Yesterday bearish
            if candle_2['Close'] > candle_2['Open']:  # Today bullish
                if candle_2['Close'] > candle_1['Open'] and candle_2['Open'] < candle_1['Close']:
                    # Today engulfs yesterday
                    timing = "Strong" if body_2 > body_1 * 1.5 else "Moderate"
                    return ('Bullish Engulfing', timing)
        
        # 3. MORNING STAR (Nison: Three-candle bullish reversal)
        # Criteria: Large bearish + small body + large bullish
        if len(recent) >= 3:
            body_0 = abs(candle_0['Close'] - candle_0['Open'])
            
            # Day 1: Large bearish
            if candle_0['Close'] < candle_0['Open'] and body_0 > range_1 * 0.6:
                # Day 2: Small body (star)
                if body_1 < body_0 * 0.3:
                    # Day 3: Large bullish
                    if candle_2['Close'] > candle_2['Open'] and body_2 > body_0 * 0.6:
                        if candle_2['Close'] > (candle_0['Close'] + candle_0['Open']) / 2:
                            return ('Morning Star', 'Strong')
        
        # 4. PIERCING LINE (Nison: Strong bullish reversal)
        # Criteria: Bearish candle + bullish candle closing >50% into prior body
        if candle_1['Close'] < candle_1['Open']:  # Yesterday bearish
            if candle_2['Close'] > candle_2['Open']:  # Today bullish
                midpoint = (candle_1['Open'] + candle_1['Close']) / 2
                if candle_2['Close'] > midpoint and candle_2['Open'] < candle_1['Close']:
                    return ('Piercing Line', 'Moderate')
        
        # 5. SHOOTING STAR (Nison: Bearish reversal at resistance)
        # Criteria: Small body, long upper shadow (2x body), tiny lower shadow
        if upper_shadow_2 > body_2 * 2 and lower_shadow_2 < body_2 * 0.3:
            if candle_2['Open'] > candle_2['Close']:  # Bearish close
                timing = "Strong" if at_resistance else "Moderate"
                return ('Shooting Star', timing)
        
        # 6. BEARISH ENGULFING (Nison: Strong momentum shift down)
        # Criteria: Bullish candle followed by larger bearish candle
        if candle_1['Close'] > candle_1['Open']:  # Yesterday bullish
            if candle_2['Close'] < candle_2['Open']:  # Today bearish
                if candle_2['Open'] > candle_1['Close'] and candle_2['Close'] < candle_1['Open']:
                    timing = "Strong" if body_2 > body_1 * 1.5 else "Moderate"
                    return ('Bearish Engulfing', timing)
        
        # 7. EVENING STAR (Nison: Three-candle bearish reversal)
        if len(recent) >= 3:
            body_0 = abs(candle_0['Close'] - candle_0['Open'])
            
            # Day 1: Large bullish
            if candle_0['Close'] > candle_0['Open'] and body_0 > range_1 * 0.6:
                # Day 2: Small body (star)
                if body_1 < body_0 * 0.3:
                    # Day 3: Large bearish
                    if candle_2['Close'] < candle_2['Open'] and body_2 > body_0 * 0.6:
                        if candle_2['Close'] < (candle_0['Close'] + candle_0['Open']) / 2:
                            return ('Evening Star', 'Strong')
        
        # 8. DARK CLOUD COVER (Nison: Bearish reversal)
        # Criteria: Bullish candle + bearish candle closing <50% into prior body
        if candle_1['Close'] > candle_1['Open']:  # Yesterday bullish
            if candle_2['Close'] < candle_2['Open']:  # Today bearish
                midpoint = (candle_1['Open'] + candle_1['Close']) / 2
                if candle_2['Close'] < midpoint and candle_2['Open'] > candle_1['Close']:
                    return ('Dark Cloud Cover', 'Moderate')
        
        # No pattern detected
        return (None, None)
    
    except Exception as e:
        logger.debug(f"{ticker}: Candlestick detection failed: {e}")
        return (None, None)


def get_reversal_confirmation(pattern_name: str, entry_timing: str) -> bool:
    """
    Determine if candlestick pattern provides strong reversal confirmation.
    
    Used by Step 11 for short-term directional strategies requiring entry timing.
    
    Args:
        pattern_name: Nison pattern name
        entry_timing: "Strong", "Moderate", or "Weak"
    
    Returns:
        True if strong reversal confirmation present
    """
    if pattern_name is None or entry_timing is None:
        return False
    
    # Strong reversal patterns at key levels
    strong_patterns = [
        'Hammer', 'Bullish Engulfing', 'Morning Star',
        'Shooting Star', 'Bearish Engulfing', 'Evening Star'
    ]
    
    return pattern_name in strong_patterns and entry_timing == 'Strong'
