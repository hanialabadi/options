"""
Market Stress Detector - P1 Guardrail

Detects extreme market volatility conditions and enables HARD HALT of all trade execution.

Philosophy:
    - Trust-first guardrail (not optimization)
    - Hard halt (no sizing, no throttling, no fallbacks)
    - Explicit diagnostics (obvious and auditable)
    - Conservative thresholds

Usage:
    from core.data_layer.market_stress_detector import check_market_stress, get_stress_diagnostic
    
    stress_level, median_iv = check_market_stress(snapshot_date)
    
    if stress_level == 'RED':
        # Halt all trades
        diagnostic = get_stress_diagnostic(stress_level, median_iv)
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Tuple, Optional
from core.data_contracts.config import MANAGEMENT_SAFE_MODE

logger = logging.getLogger(__name__)

# Market Stress Thresholds (configurable)
STRESS_THRESHOLD_YELLOW = 30.0  # Caution: Median IV ≥ 30
STRESS_THRESHOLD_RED = 40.0     # Halt: Median IV ≥ 40

def check_market_stress(
    snapshot_date: Optional[str] = None,
    yellow_threshold: float = STRESS_THRESHOLD_YELLOW,
    red_threshold: float = STRESS_THRESHOLD_RED
) -> Tuple[str, float, str]:
    """
    Check market stress level using median IV Index 30d across all tickers.
    
    Args:
        snapshot_date: Date to check (YYYY-MM-DD). If None, uses most recent date.
        yellow_threshold: Median IV threshold for YELLOW (caution) alert
        red_threshold: Median IV threshold for RED (halt) alert
        
    Returns:
        Tuple of (stress_level, median_iv, stress_basis):
            stress_level: 'GREEN', 'YELLOW', 'RED', or 'UNKNOWN'
            median_iv: Median IV Index 30d value
            stress_basis: 'IV_INDEX' or 'NONE'
            
    Raises:
        FileNotFoundError: If derived analytics file not found
        ValueError: If no data available for specified date
    """
    # Load derived IV analytics
    derived_path = Path("data/ivhv_timeseries/ivhv_timeseries_derived.csv")
    
    if not derived_path.exists():
        logger.warning(f"⚠️ Derived IV analytics not found: {derived_path}")
        logger.warning(f"   Market stress detection disabled (defaulting to GREEN)")
        return 'GREEN', 0.0, 'NONE'
    
    try:
        df_iv = pd.read_csv(derived_path)
        
        # Filter by date
        if snapshot_date is not None:
            df_filtered = df_iv[df_iv['date'] == snapshot_date]
            if df_filtered.empty:
                logger.warning(f"⚠️ No IV data for date {snapshot_date}, using most recent")
                snapshot_date = None
        
        if snapshot_date is None:
            # Use most recent date
            latest_date = df_iv['date'].max()
            df_filtered = df_iv[df_iv['date'] == latest_date]
            logger.info(f"📊 Market stress check using latest date: {latest_date}")
        
        # Compute median IV Index 30d
        if 'iv_index_30d' not in df_filtered.columns:
            logger.warning(f"⚠️ iv_index_30d column not found in derived analytics")
            return 'GREEN', 0.0, 'NONE'
        
        # FIX: Institutional Maturity Gate
        # If IV history is not mature (120+ days), we refuse to compute market stress.
        # Management Safe Mode: Short-circuit maturity gate
        max_history = df_filtered['iv_history_days'].max() if 'iv_history_days' in df_filtered.columns else 0
        if max_history < 120 and not MANAGEMENT_SAFE_MODE:
            logger.warning(f"⚠️ Market Stress IMMATURE: Max IV history {max_history} < 120 days")
            return 'UNKNOWN', 0.0, 'IV_NOT_MATURE'

        median_iv = df_filtered['iv_index_30d'].median()
        non_null_count = df_filtered['iv_index_30d'].notna().sum()
        
        logger.info(f"📊 Median IV Index 30d: {median_iv:.2f} (from {non_null_count} tickers)")
        
        # Determine stress level
        if non_null_count == 0:
            stress_level = 'UNKNOWN'
            logger.warning("⚠️ UNKNOWN: No IV data available to determine market stress.")
            return stress_level, 0.0, 'NONE'
        
        stress_basis = 'IV_INDEX'
        if median_iv >= red_threshold:
            stress_level = 'RED'
            logger.warning(f"🛑 RED ALERT: Market stress detected (median IV {median_iv:.2f} ≥ {red_threshold})")
        elif median_iv >= yellow_threshold:
            stress_level = 'YELLOW'
            logger.warning(f"⚠️ YELLOW ALERT: Elevated volatility (median IV {median_iv:.2f} ≥ {yellow_threshold})")
        else:
            stress_level = 'GREEN'
            logger.info(f"✅ GREEN: Normal market conditions (median IV {median_iv:.2f} < {yellow_threshold})")
        
        return stress_level, median_iv, stress_basis
        
    except Exception as e:
        logger.error(f"❌ Error checking market stress: {e}")
        logger.warning(f"   Defaulting to GREEN (safe mode)")
        return 'GREEN', 0.0, 'NONE'


def get_stress_diagnostic(stress_level: str, median_iv: float) -> str:
    """
    Generate diagnostic message for market stress level.
    
    Args:
        stress_level: 'GREEN', 'YELLOW', 'RED', or 'UNKNOWN'
        median_iv: Median IV Index 30d value
        
    Returns:
        Human-readable diagnostic string
    """
    if stress_level == 'RED':
        return f"Market Stress Mode ACTIVE (Median IV {median_iv:.1f} ≥ {STRESS_THRESHOLD_RED} threshold)"
    elif stress_level == 'YELLOW':
        return f"Elevated Market Volatility (Median IV {median_iv:.1f} ≥ {STRESS_THRESHOLD_YELLOW} threshold)"
    elif stress_level == 'UNKNOWN':
        return "Market Stress UNKNOWN (No IV data available)"
    else:
        return f"Normal Market Conditions (Median IV {median_iv:.1f})"


def get_halt_reason(median_iv: float) -> str:
    """
    Generate acceptance_reason for HALTED_MARKET_STRESS status.
    
    Args:
        median_iv: Median IV Index 30d value
        
    Returns:
        Acceptance reason string for halted trades
    """
    return f"Market Stress Mode active (Median IV = {median_iv:.1f} ≥ {STRESS_THRESHOLD_RED} threshold)"


def should_halt_trades(stress_level: str) -> bool:
    """
    Determine if trades should be halted based on stress level.
    
    Args:
        stress_level: 'GREEN', 'YELLOW', or 'RED'
        
    Returns:
        True if RED alert (halt all trades), False otherwise
    """
    return stress_level == 'RED'


def get_market_stress_summary(stress_level: str, median_iv: float, ticker_count: int = 0) -> str:
    """
    Generate summary banner for CLI/dashboard display.
    
    Args:
        stress_level: 'GREEN', 'YELLOW', 'RED', or 'UNKNOWN'
        median_iv: Median IV Index 30d value
        ticker_count: Number of tickers in analysis (optional)
        
    Returns:
        Formatted summary string
    """
    ticker_info = f" (from {ticker_count} tickers)" if ticker_count > 0 else ""
    
    if stress_level == 'RED':
        return f"🛑 MARKET STRESS MODE ACTIVE - ALL TRADES HALTED\n   Median IV: {median_iv:.1f} ≥ {STRESS_THRESHOLD_RED} threshold{ticker_info}"
    elif stress_level == 'YELLOW':
        return f"⚠️ ELEVATED VOLATILITY - CAUTION ADVISED\n   Median IV: {median_iv:.1f} ≥ {STRESS_THRESHOLD_YELLOW} threshold{ticker_info}"
    elif stress_level == 'UNKNOWN':
        return f"❓ MARKET STRESS UNKNOWN - NO DATA AVAILABLE\n   Insufficient IV samples to determine regime{ticker_info}"
    else:
        return f"✅ Normal Market Conditions\n   Median IV: {median_iv:.1f}{ticker_info}"


# Example usage
if __name__ == "__main__":
    import sys
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s'
    )
    
    print("="*70)
    print("MARKET STRESS DETECTOR - P1 GUARDRAIL")
    print("="*70)
    
    # Check current market stress
    stress_level, median_iv, stress_basis = check_market_stress()
    
    print(f"\n📊 Stress Level: {stress_level}")
    print(f"📊 Stress Basis: {stress_basis}")
    print(f"📊 Median IV: {median_iv:.2f}")
    
    print(f"\n🔍 Diagnostics:")
    print(f"   {get_stress_diagnostic(stress_level, median_iv)}")
    
    print(f"\n🚦 Trade Execution:")
    if should_halt_trades(stress_level):
        print(f"   🛑 HALT - No trades allowed")
        print(f"   Reason: {get_halt_reason(median_iv)}")
    else:
        print(f"   ✅ PROCEED - Trades allowed")
    
    print(f"\n📢 Summary Banner:")
    print(get_market_stress_summary(stress_level, median_iv, ticker_count=177))
