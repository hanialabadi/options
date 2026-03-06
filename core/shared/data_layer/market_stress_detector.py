"""
Market Stress Detector - P1 Guardrail

Detects extreme market volatility conditions and enables HARD HALT of all trade execution.

Philosophy:
    - Trust-first guardrail (not optimization)
    - Hard halt (no sizing, no throttling, no fallbacks)
    - Explicit diagnostics (obvious and auditable)
    - Conservative thresholds

Usage:
    from core.shared.data_layer.market_stress_detector import classify_market_stress, get_stress_diagnostic
    
    stress_level, primary_metric_value, stress_basis = classify_market_stress()
    
    if stress_level == 'CRISIS':
        # Halt all trades
        diagnostic = get_stress_diagnostic(stress_level, primary_metric_value, stress_basis)
"""

import pandas as pd
import numpy as np
import logging
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Tuple, Optional
from core.shared.data_contracts.config import MANAGEMENT_SAFE_MODE
from core.shared.data_layer.price_history_loader import load_price_history
from utils.ta_lib_utils import calculate_atr, calculate_ema, calculate_sma
from config.indicator_settings import (
    MARKET_STRESS_THRESHOLDS, ATR_SETTINGS, EMA_SETTINGS, SMA_SETTINGS
)

logger = logging.getLogger(__name__)

def classify_market_stress(
    market_ticker: str = "SPY",
    days_history: int = 60,
    client=None
) -> Tuple[str, float, str]:
    """
    Classifies the overall market stress level using market-level proxies (e.g., SPY).
    
    Args:
        market_ticker: The ticker symbol for the market proxy (e.g., 'SPY', 'QQQ').
        days_history: Number of days of historical data to fetch.
        client: SchwabClient instance for fetching data.
        
    Returns:
        Tuple of (stress_level, primary_metric_value, stress_basis):
            stress_level: 'LOW', 'NORMAL', 'ELEVATED', 'CRISIS'
            primary_metric_value: The value of the primary metric used for classification (e.g., SPY ATR %).
            stress_basis: A string indicating the primary metric used (e.g., 'SPY_ATR_PCT').
    """
    try:
        hist, source = load_price_history(market_ticker, days=days_history, client=client)
        
        min_bars = 20
        if hist is None or hist.empty or len(hist) < min_bars:
            logger.warning(f"⚠️ Insufficient history for {market_ticker} to classify market stress. Need ≥{min_bars} bars.")
            from scan_engine.debug.debug_mode import get_debug_manager
            get_debug_manager().log_event(
                "market_stress",
                "WARN",
                "INSUFFICIENT_HISTORY",
                f"Market regime proxy unavailable for {market_ticker}",
                {"bars": 0 if hist is None else len(hist), "required": min_bars}
            )
            return 'UNKNOWN', 0.0, 'INSUFFICIENT_DATA'

        close_prices = hist['Close']
        high_prices = hist['High']
        low_prices = hist['Low']

        # Compute SPY ATR %
        atr_series = calculate_atr(high_prices, low_prices, close_prices, timeperiod=ATR_SETTINGS["timeperiod"])
        spy_atr_pct = (atr_series.iloc[-1] / close_prices.iloc[-1]) * 100 if close_prices.iloc[-1] != 0 else np.nan
        
        # Compute SPY Trend State (EMA21 / SMA50)
        ema21_series = calculate_ema(close_prices, timeperiod=EMA_SETTINGS["timeperiod_21"])
        sma50_series = calculate_sma(close_prices, timeperiod=SMA_SETTINGS["timeperiod_50"])
        
        spy_trend_state = 'UNKNOWN'
        if pd.notna(ema21_series.iloc[-1]) and pd.notna(sma50_series.iloc[-1]):
            if ema21_series.iloc[-1] > sma50_series.iloc[-1]:
                spy_trend_state = 'UPTREND'
            elif ema21_series.iloc[-1] < sma50_series.iloc[-1]:
                spy_trend_state = 'DOWNTREND'
            else:
                spy_trend_state = 'RANGE'

        # Classify stress based on thresholds
        stress_level = 'NORMAL'
        stress_basis = 'SPY_ATR_PCT'
        primary_metric_value = spy_atr_pct

        if pd.notna(spy_atr_pct):
            if spy_atr_pct >= MARKET_STRESS_THRESHOLDS["ATR_CRISIS"]:
                stress_level = 'CRISIS'
            elif spy_atr_pct >= MARKET_STRESS_THRESHOLDS["ATR_ELEVATED"]:
                stress_level = 'ELEVATED'
            elif spy_atr_pct <= MARKET_STRESS_THRESHOLDS["ATR_LOW"]:
                stress_level = 'LOW'
        
        # Further refine based on trend state for CRISIS/ELEVATED
        if stress_level in ['ELEVATED', 'CRISIS'] and spy_trend_state == 'DOWNTREND':
            stress_level = 'CRISIS' # Downgrade to crisis if elevated vol and downtrend
        elif stress_level == 'NORMAL' and spy_trend_state == 'DOWNTREND':
            stress_level = 'ELEVATED' # Upgrade to elevated if normal vol but downtrend

        logger.info(f"📊 Market Stress for {market_ticker}: {stress_level} (ATR: {spy_atr_pct:.2f}%, Trend: {spy_trend_state})")
        return stress_level, primary_metric_value, stress_basis

    except Exception as e:
        logger.error(f"❌ Error classifying market stress for {market_ticker}: {e}")
        return 'UNKNOWN', 0.0, 'ERROR'

# Original check_market_stress (now deprecated or repurposed)
def check_market_stress(
    market_ticker: str = "SPY", # Added market_ticker to match classify_market_stress
    days_history: int = 60, # Added days_history to match classify_market_stress
    client=None # Added client to match classify_market_stress
) -> Tuple[str, float, str]:
    """
    DEPRECATED: Use classify_market_stress for market-level proxy.
    This function now serves as a placeholder or can be adapted for ticker-level IV stress.
    """
    logger.warning("⚠️ Using deprecated `check_market_stress`. Please use `classify_market_stress` for market-level proxy.")
    return classify_market_stress(market_ticker, days_history, client) # Call the new function


def get_stress_diagnostic(stress_level: str, primary_metric_value: float, stress_basis: str) -> str:
    """
    Generate diagnostic message for market stress level.
    """
    if stress_level == 'CRISIS':
        return f"Market Stress: CRISIS (Primary Metric: {stress_basis} = {primary_metric_value:.1f})"
    elif stress_level == 'ELEVATED':
        return f"Market Stress: ELEVATED (Primary Metric: {stress_basis} = {primary_metric_value:.1f})"
    elif stress_level == 'LOW':
        return f"Market Stress: LOW (Primary Metric: {stress_basis} = {primary_metric_value:.1f})"
    elif stress_level == 'UNKNOWN':
        return "Market Stress: UNKNOWN (Insufficient data or error)"
    else:
        return f"Market Stress: NORMAL (Primary Metric: {stress_basis} = {primary_metric_value:.1f})"


def get_halt_reason(stress_level: str, primary_metric_value: float, stress_basis: str) -> str:
    """
    Generate acceptance_reason for HALTED_MARKET_STRESS status.
    """
    if stress_level == 'CRISIS':
        return f"Market Stress Mode active (CRISIS: {stress_basis} = {primary_metric_value:.1f})"
    elif stress_level == 'ELEVATED':
        return f"Elevated Market Volatility (ELEVATED: {stress_basis} = {primary_metric_value:.1f})"
    else:
        return "Market conditions do not warrant a halt."


def should_halt_trades(stress_level: str) -> bool:
    """
    Determine if trades should be halted based on stress level.
    """
    return stress_level == 'CRISIS'


def get_market_stress_summary(stress_level: str, primary_metric_value: float, stress_basis: str, ticker_count: int = 0) -> str:
    """
    Generate summary banner for CLI/dashboard display.
    """
    ticker_info = f" (from {ticker_count} tickers)" if ticker_count > 0 else ""
    
    if stress_level == 'CRISIS':
        return f"🛑 MARKET STRESS MODE ACTIVE - ALL TRADES HALTED\n   Stress Level: {stress_level}, Metric: {stress_basis} = {primary_metric_value:.1f}{ticker_info}"
    elif stress_level == 'ELEVATED':
        return f"⚠️ ELEVATED VOLATILITY - CAUTION ADVISED\n   Stress Level: {stress_level}, Metric: {stress_basis} = {primary_metric_value:.1f}{ticker_info}"
    elif stress_level == 'UNKNOWN':
        return f"❓ MARKET STRESS UNKNOWN - NO DATA AVAILABLE\n   Insufficient data to determine regime{ticker_info}"
    elif stress_level == 'LOW':
        return f"✅ LOW Market Volatility\n   Stress Level: {stress_level}, Metric: {stress_basis} = {primary_metric_value:.1f}{ticker_info}"
    else:
        return f"✅ Normal Market Conditions\n   Stress Level: {stress_level}, Metric: {stress_basis} = {primary_metric_value:.1f}{ticker_info}"


# Example usage
if __name__ == "__main__":
    import sys
    from scan_engine.loaders.schwab_api_client import SchwabClient # Assuming SchwabClient is available

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s'
    )
    
    print("="*70)
    print("MARKET STRESS DETECTOR - P1 GUARDRAIL")
    print("="*70)
    
    # Initialize Schwab client (optional, for live data)
    schwab_client = None
    try:
        client_id = os.getenv("SCHWAB_APP_KEY")
        client_secret = os.getenv("SCHWAB_APP_SECRET")
        if client_id and client_secret:
            schwab_client = SchwabClient(client_id, client_secret)
            logger.info("✅ Schwab client initialized for market stress detection.")
    except Exception as e:
        logger.warning(f"⚠️ Schwab client initialization failed for market stress: {e}")

    # Classify market stress
    stress_level, primary_metric_value, stress_basis = classify_market_stress(client=schwab_client)
    
    print(f"\n📊 Stress Level: {stress_level}")
    print(f"📊 Primary Metric Value: {primary_metric_value:.2f}")
    print(f"📊 Stress Basis: {stress_basis}")
    
    print(f"\n🔍 Diagnostics:")
    print(f"   {get_stress_diagnostic(stress_level, primary_metric_value, stress_basis)}")
    
    print(f"\n🚦 Trade Execution:")
    if should_halt_trades(stress_level):
        print(f"   🛑 HALT - No trades allowed")
        print(f"   Reason: {get_halt_reason(stress_level, primary_metric_value, stress_basis)}")
    else:
        print(f"   ✅ PROCEED - Trades allowed")
    
    print(f"\n📢 Summary Banner:")
    print(get_market_stress_summary(stress_level, primary_metric_value, stress_basis, ticker_count=177))
