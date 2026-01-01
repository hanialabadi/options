"""
Step 7: Strategy Recommendation Engine (Multi-Strategy Ledger Architecture)

🚨 ARCHITECTURAL CHANGE (2025-01-XX):
Moved from single-strategy-per-ticker to Strategy Ledger pattern.

**Strategy Ledger Pattern**:
- Each row = (Ticker × Strategy) pairing
- Independent validators (no if/elif chains)
- Additive logic (append all valid strategies)
- Theory-explicit (Valid_Reason + Theory_Source)

**Theory Compliance**:
- Multiple strategies can coexist for same ticker (Hull)
- Bullish ticker can have: Long Call + CSP + Buy-Write (capital/risk-dependent)
- Expansion ticker can have: Long Straddle + Long Strangle (budget-dependent)
- Strategy discovery ≠ execution filtering (Step 7 vs Step 9B)

Purpose:
    Takes validated market data from Steps 2-6 and generates
    MULTIPLE strategy recommendations per ticker (when theory allows).
    
Design:
    Independent validators ensure order-independence and theory compliance.
    No mutual exclusion - all valid strategies are discovered.
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


# ==========================================
# HELPER FUNCTIONS
# ==========================================

def _calculate_approx_stock_price(row: pd.Series) -> float:
    """
    Calculate approximate stock price using Step 5 derived data.
    Assumes SMA20 and Price_vs_SMA20 are available.
    """
    sma20 = row.get('SMA20', 0)
    price_vs_sma20 = row.get('Price_vs_SMA20', 0)
    
    if sma20 and price_vs_sma20:
        return sma20 + price_vs_sma20
    elif 'Close' in row: # Fallback to Close price if SMA data not available
        return row['Close']
    return 0.0 # Default to 0 if no price data

# ==========================================
# INDEPENDENT STRATEGY VALIDATORS
# (Multi-Strategy Ledger Architecture)
# ==========================================

def _validate_long_call(ticker: str, row: pd.Series) -> Optional[Dict]:
    """
    Validate Long Call strategy.
    
    Theory: Natenberg Ch.3 - Directional with positive vega.
    Entry: Bullish signal + Cheap IV (gap < 0).
    """
    stock_price = _calculate_approx_stock_price(row)
    if stock_price == 0:
        return None # Cannot determine capital without stock price
    
    # Approximate capital for 1 contract (e.g., $5 premium)
    capital_req = 5 * 100 # $500 for a typical call option
    
    signal = row.get('Signal_Type', '')
    gap_30d = row.get('IVHV_gap_30D', 0)
    
    # Calculate longer-term gaps if not present
    iv_180 = row.get('IV_180_D_Call', 0)
    hv_180 = row.get('HV_180_D_Cur', 0)
    gap_180d = (iv_180 - hv_180) if (iv_180 and hv_180) else 0
    
    iv_60 = row.get('IV_60_D_Call', 0)
    hv_60 = row.get('HV_60_D_Cur', 0)
    gap_60d = (iv_60 - hv_60) if (iv_60 and hv_60) else 0
    
    # Rejection criteria
    if signal not in ['Bullish', 'Sustained Bullish']:
        return None
    if gap_180d >= 0 and gap_60d >= 0 and gap_30d >= 0:
        return None  # IV not cheap on any timeframe
    
    # Valid - return strategy
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Long Call',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Bullish + Cheap IV (gap_180d={gap_180d:.1f})",
        'Theory_Source': 'Natenberg Ch.3 - Directional with positive vega',
        'Regime_Context': signal,
        'IV_Context': f"gap_30d={gap_30d:.1f}, gap_60d={gap_60d:.1f}, gap_180d={gap_180d:.1f}",
        'Capital_Requirement': capital_req,
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
    stock_price = _calculate_approx_stock_price(row)
    if stock_price == 0:
        return None
    
    capital_req = 5 * 100 # $500 for a typical put option
    
    signal = row.get('Signal_Type', '')
    gap_30d = row.get('IVHV_gap_30D', 0)
    
    # Calculate longer-term gaps
    iv_180 = row.get('IV_180_D_Put', 0)
    hv_180 = row.get('HV_180_D_Cur', 0)
    gap_180d = (iv_180 - hv_180) if (iv_180 and hv_180) else 0
    
    iv_60 = row.get('IV_60_D_Put', 0)
    hv_60 = row.get('HV_60_D_Cur', 0)
    gap_60d = (iv_60 - hv_60) if (iv_60 and hv_60) else 0
    
    # Rejection criteria
    if signal not in ['Bearish']:
        return None
    if gap_180d >= 0 and gap_60d >= 0 and gap_30d >= 0:
        return None
    
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Long Put',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Bearish + Cheap IV (gap_180d={gap_180d:.1f})",
        'Theory_Source': 'Natenberg Ch.3 - Directional with negative delta',
        'Regime_Context': signal,
        'IV_Context': f"gap_30d={gap_30d:.1f}, gap_60d={gap_60d:.1f}, gap_180d={gap_180d:.1f}",
        'Capital_Requirement': capital_req,
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
    stock_price = _calculate_approx_stock_price(row)
    if stock_price == 0:
        return None
    
    # Capital for 100 shares (approximate)
    capital_req = stock_price * 100
    
    signal = row.get('Signal_Type', '')
    gap_30d = row.get('IVHV_gap_30D', 0)
    iv_rank = row.get('IV_Rank_XS', 50)
    
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
        'Capital_Requirement': capital_req,
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
    """
    stock_price = _calculate_approx_stock_price(row)
    if stock_price == 0:
        return None
    
    # Capital requirement is 0 as stock is assumed to be held
    capital_req = 0
    
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
        'Capital_Requirement': capital_req,
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
    stock_price = _calculate_approx_stock_price(row)
    if stock_price == 0:
        return None
    
    # Capital calculation as per architecture:
    # stock_cost = stock_price * 100
    # call_premium_est = stock_price * 0.01 * 100  # ~1% OTM estimate
    # net_capital = stock_cost - call_premium_est
    # For simplicity, let's use stock_cost as the primary capital requirement for now
    # The architecture implies this is an approximate estimate.
    capital_req = stock_price * 100 # Cost of 100 shares
    
    signal = row.get('Signal_Type', '')
    gap_30d = row.get('IVHV_gap_30D', 0)
    iv_rank = row.get('IV_Rank_XS', 50)
    
    # Rejection criteria
    if signal not in ['Bullish', 'Sustained Bullish']:
        return None
    if gap_30d <= 0:
        return None
    if iv_rank <= 50: # Relaxed to match audit's 'Positive gap or IV_Rank > 50'
        return None  # Prefer CSP when IV moderately rich
    
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Buy-Write',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Bullish + Very Rich IV (IV_Rank={iv_rank:.0f})",
        'Theory_Source': 'Cohen Ch.7 - Reduces cost basis via call premium',
        'Regime_Context': signal,
        'IV_Context': f"gap_30d={gap_30d:.1f}, IV_Rank={iv_rank:.0f}",
        'Capital_Requirement': capital_req,
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
    stock_price = _calculate_approx_stock_price(row)
    if stock_price == 0:
        return None
    
    # Approximate capital for straddle (e.g., 8% of stock price for ATM options)
    capital_req = stock_price * 0.08 * 100
    
    # Infer expansion from regime and signal patterns
    regime = row.get('Regime', '')
    signal = row.get('Signal_Type', '')
    gap_30d = row.get('IVHV_gap_30D', 0)
    iv_rank = row.get('IV_Rank_XS', 50)
    
    # Calculate longer-term gaps
    iv_180 = row.get('IV_180_D_Call', 0)
    hv_180 = row.get('HV_180_D_Cur', 0)
    gap_180d = (iv_180 - hv_180) if (iv_180 and hv_180) else 0
    
    iv_60 = row.get('IV_60_D_Call', 0)
    hv_60 = row.get('HV_60_D_Cur', 0)
    gap_60d = (iv_60 - hv_60) if (iv_60 and hv_60) else 0
    
    # Expansion proxy: Low IV rank + negative gap
    expansion = (iv_rank < 40 and (gap_180d < 0 or gap_60d < 0))
    
    # Rejection criteria
    if not expansion and signal != 'Bidirectional':
        return None
    if gap_180d >= 0 and gap_60d >= 0:
        return None  # IV not cheap
    if iv_rank >= 40: # Adjusted to match audit's 'IV_Rank < 40'
        return None  # Not cheap enough
    
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Long Straddle',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Expansion + Very Cheap IV (IV_Rank={iv_rank:.0f}, gap_180d={gap_180d:.1f})",
        'Theory_Source': 'Natenberg Ch.9 - ATM volatility play',
        'Regime_Context': signal if signal == 'Bidirectional' else 'Expansion',
        'IV_Context': f"gap_30d={gap_30d:.1f}, gap_60d={gap_60d:.1f}, gap_180d={gap_180d:.1f}, IV_Rank={iv_rank:.0f}",
        'Capital_Requirement': capital_req,
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
    stock_price = _calculate_approx_stock_price(row)
    if stock_price == 0:
        return None
    
    # Approximate capital for strangle (e.g., 5% of stock price for OTM options)
    capital_req = stock_price * 0.05 * 100
    
    regime = row.get('Regime', '')
    signal = row.get('Signal_Type', '')
    gap_30d = row.get('IVHV_gap_30D', 0)
    iv_rank = row.get('IV_Rank_XS', 50)
    
    # Calculate longer-term gaps
    iv_180 = row.get('IV_180_D_Call', 0)
    hv_180 = row.get('HV_180_D_Cur', 0)
    gap_180d = (iv_180 - hv_180) if (iv_180 and hv_180) else 0
    
    iv_60 = row.get('IV_60_D_Call', 0)
    hv_60 = row.get('HV_60_D_Cur', 0)
    gap_60d = (iv_60 - hv_60) if (iv_60 and hv_60) else 0
    
    # Expansion proxy
    expansion = (iv_rank < 50 and (gap_180d < 0 or gap_60d < 0))
    
    # Rejection criteria
    if not expansion and signal != 'Bidirectional':
        return None
    if gap_180d >= 0 and gap_60d >= 0:
        return None
    if iv_rank >= 40: # Adjusted to match audit's 'IV_Rank < 40'
        return None  # Not cheap enough
    
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Long Strangle',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Expansion + Moderately Cheap IV (IV_Rank={iv_rank:.0f})",
        'Theory_Source': 'Natenberg Ch.9 - OTM volatility (cheaper, needs bigger move)',
        'Regime_Context': signal if signal == 'Bidirectional' else 'Expansion',
        'IV_Context': f"gap_30d={gap_30d:.1f}, gap_60d={gap_60d:.1f}, gap_180d={gap_180d:.1f}, IV_Rank={iv_rank:.0f}",
        'Capital_Requirement': capital_req,
        'Risk_Profile': 'Defined (max loss = total premium)',
        'Greeks_Exposure': 'Delta-neutral, Long Vega, Short Theta',
        'Execution_Ready': True,
        'Confidence': 68,
        'Trade_Bias': 'Bidirectional',
        'Strategy_Type': 'Volatility',
    }


def _validate_long_call_leap(ticker: str, row: pd.Series) -> Optional[Dict]:
    """
    Validate Long Call LEAP strategy.
    
    Theory: Hull Ch.10 - Multi-year directional thesis with structural conviction.
    Entry: Sustained bullish signal + Low IV + Cheap long-term IV.
    
    LEAP-Specific Criteria (distinguish from short-term Long Call):
    - Sustained bullish signal (not just short-term momentum)
    - IV_Rank < 40 (prefer buying when IV suppressed for long term)
    - gap_180d < -5 (want cheap long-term IV)
    - Capital-heavy but defined risk (typical $2000-$5000 per contract)
    """
    stock_price = _calculate_approx_stock_price(row)
    if stock_price == 0:
        return None
    
    # Approximate capital for LEAP call (e.g., 20% of stock price for ITM options)
    capital_req = stock_price * 0.20 * 100
    
    signal = row.get('Signal_Type', '')
    gap_30d = row.get('IVHV_gap_30D', 0)
    iv_rank = row.get('IV_Rank_XS', 50)
    
    # Calculate longer-term gaps
    iv_180 = row.get('IV_180_D_Call', 0)
    hv_180 = row.get('HV_180_D_Cur', 0)
    gap_180d = (iv_180 - hv_180) if (iv_180 and hv_180) else 0
    
    iv_60 = row.get('IV_60_D_Call', 0)
    hv_60 = row.get('HV_60_D_Cur', 0)
    gap_60d = (iv_60 - hv_60) if (iv_60 and hv_60) else 0
    
    # LEAP criteria (stricter than short-term)
    # Note: 'Sustained Bullish' is stricter; fallback to 'Bullish' if not available
    if signal not in ['Sustained Bullish', 'Bullish']:
        return None
    if gap_180d >= 0:  # Want cheap long-term IV (negative gap)
        return None
    
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Long Call LEAP',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Bullish + Structural thesis + Cheap long-term IV (IV_Rank={iv_rank:.0f}, gap_180d={gap_180d:.1f})",
        'Theory_Source': 'Hull Ch.10 - Multi-year directional with defined risk',
        'Regime_Context': signal,
        'IV_Context': f"gap_30d={gap_30d:.1f}, gap_60d={gap_60d:.1f}, gap_180d={gap_180d:.1f}, IV_Rank={iv_rank:.0f}",
        'Capital_Requirement': capital_req,
        'Risk_Profile': 'Defined (max loss = premium, typically $2000-$5000)',
        'Greeks_Exposure': 'Long Delta (lower), Long Vega, Short Theta (minimal)',
        'Execution_Ready': True,
        'Confidence': 70,
        'Trade_Bias': 'Bullish',
        'Strategy_Type': 'Directional',
    }


def _validate_long_put_leap(ticker: str, row: pd.Series) -> Optional[Dict]:
    """
    Validate Long Put LEAP strategy.
    
    Theory: Hull Ch.10 - Multi-year hedging or structural bearish thesis.
    Entry: Sustained bearish signal + Low IV + Cheap long-term IV.
    
    LEAP-Specific Criteria:
    - Sustained bearish signal or hedge rationale
    - IV_Rank < 40 (don't overpay for long-term protection)
    - gap_180d < -5 (cheap long-term IV)
    """
    stock_price = _calculate_approx_stock_price(row)
    if stock_price == 0:
        return None
    
    # Approximate capital for LEAP put (e.g., 20% of stock price for ITM options)
    capital_req = stock_price * 0.20 * 100
    
    signal = row.get('Signal_Type', '')
    gap_30d = row.get('IVHV_gap_30D', 0)
    iv_rank = row.get('IV_Rank_XS', 50)
    
    # Calculate longer-term gaps
    iv_180 = row.get('IV_180_D_Put', 0)
    hv_180 = row.get('HV_180_D_Cur', 0)
    gap_180d = (iv_180 - hv_180) if (iv_180 and hv_180) else 0
    
    iv_60 = row.get('IV_60_D_Put', 0)
    hv_60 = row.get('HV_60_D_Cur', 0)
    gap_60d = (iv_60 - hv_60) if (iv_60 and hv_60) else 0
    
    # LEAP criteria
    if signal not in ['Sustained Bearish', 'Bearish']:
        return None
    if gap_180d >= 0:
        return None
    
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Long Put LEAP',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Bearish + Structural thesis + Cheap long-term IV (IV_Rank={iv_rank:.0f}, gap_180d={gap_180d:.1f})",
        'Theory_Source': 'Hull Ch.10 - Multi-year protective or directional',
        'Regime_Context': signal,
        'IV_Context': f"gap_30d={gap_30d:.1f}, gap_60d={gap_60d:.1f}, gap_180d={gap_180d:.1f}, IV_Rank={iv_rank:.0f}",
        'Capital_Requirement': capital_req,
        'Risk_Profile': 'Defined (max loss = premium, typically $2000-$5000)',
        'Greeks_Exposure': 'Short Delta (lower), Long Vega, Short Theta (minimal)',
        'Execution_Ready': True,
        'Confidence': 70,
        'Trade_Bias': 'Bearish',
        'Strategy_Type': 'Directional',
    }
    signal = row.get('Signal_Type', '')
    gap_30d = row.get('IVHV_gap_30D', 0)
    
    # Calculate longer-term gaps if not present
    iv_180 = row.get('IV_180_D_Call', 0)
    hv_180 = row.get('HV_180_D_Cur', 0)
    gap_180d = (iv_180 - hv_180) if (iv_180 and hv_180) else 0
    
    iv_60 = row.get('IV_60_D_Call', 0)
    hv_60 = row.get('HV_60_D_Cur', 0)
    gap_60d = (iv_60 - hv_60) if (iv_60 and hv_60) else 0
    
    # Rejection criteria
    if signal not in ['Bullish', 'Sustained Bullish']:
        return None
    if gap_180d >= 0 and gap_60d >= 0 and gap_30d >= 0:
        return None  # IV not cheap on any timeframe
    
    # Valid - return strategy
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Long Call',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Bullish + Cheap IV (gap_180d={gap_180d:.1f})",
        'Theory_Source': 'Natenberg Ch.3 - Directional with positive vega',
        'Regime_Context': signal,
        'IV_Context': f"gap_30d={gap_30d:.1f}, gap_60d={gap_60d:.1f}, gap_180d={gap_180d:.1f}",
        'Capital_Requirement': 500,
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
    gap_30d = row.get('IVHV_gap_30D', 0)
    
    # Calculate longer-term gaps
    iv_180 = row.get('IV_180_D_Put', 0)
    hv_180 = row.get('HV_180_D_Cur', 0)
    gap_180d = (iv_180 - hv_180) if (iv_180 and hv_180) else 0
    
    iv_60 = row.get('IV_60_D_Put', 0)
    hv_60 = row.get('HV_60_D_Cur', 0)
    gap_60d = (iv_60 - hv_60) if (iv_60 and hv_60) else 0
    
    # Rejection criteria
    if signal not in ['Bearish']:
        return None
    if gap_180d >= 0 and gap_60d >= 0 and gap_30d >= 0:
        return None
    
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Long Put',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Bearish + Cheap IV (gap_180d={gap_180d:.1f})",
        'Theory_Source': 'Natenberg Ch.3 - Directional with negative delta',
        'Regime_Context': signal,
        'IV_Context': f"gap_30d={gap_30d:.1f}, gap_60d={gap_60d:.1f}, gap_180d={gap_180d:.1f}",
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
    iv_rank = row.get('IV_Rank_XS', 50)
    
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
        'Capital_Requirement': 15000,
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
        'Capital_Requirement': 0,
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
    iv_rank = row.get('IV_Rank_XS', 50)
    
    # Rejection criteria
    if signal not in ['Bullish', 'Sustained Bullish']:
        return None
    if gap_30d <= 0:
        return None
    if iv_rank <= 50: # Relaxed to match audit's 'Positive gap or IV_Rank > 50'
        return None  # Prefer CSP when IV moderately rich
    
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Buy-Write',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Bullish + Very Rich IV (IV_Rank={iv_rank:.0f})",
        'Theory_Source': 'Cohen Ch.7 - Reduces cost basis via call premium',
        'Regime_Context': signal,
        'IV_Context': f"gap_30d={gap_30d:.1f}, IV_Rank={iv_rank:.0f}",
        'Capital_Requirement': 50000,
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
    # Infer expansion from regime and signal patterns
    regime = row.get('Regime', '')
    signal = row.get('Signal_Type', '')
    gap_30d = row.get('IVHV_gap_30D', 0)
    iv_rank = row.get('IV_Rank_XS', 50)
    
    # Calculate longer-term gaps
    iv_180 = row.get('IV_180_D_Call', 0)
    hv_180 = row.get('HV_180_D_Cur', 0)
    gap_180d = (iv_180 - hv_180) if (iv_180 and hv_180) else 0
    
    iv_60 = row.get('IV_60_D_Call', 0)
    hv_60 = row.get('HV_60_D_Cur', 0)
    gap_60d = (iv_60 - hv_60) if (iv_60 and hv_60) else 0
    
    # Expansion proxy: Low IV rank + negative gap
    expansion = (iv_rank < 40 and (gap_180d < 0 or gap_60d < 0))
    
    # Rejection criteria
    if not expansion and signal != 'Bidirectional':
        return None
    if gap_180d >= 0 and gap_60d >= 0:
        return None  # IV not cheap
    if iv_rank >= 40: # Adjusted to match audit's 'IV_Rank < 40'
        return None  # Not cheap enough
    
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Long Straddle',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Expansion + Very Cheap IV (IV_Rank={iv_rank:.0f}, gap_180d={gap_180d:.1f})",
        'Theory_Source': 'Natenberg Ch.9 - ATM volatility play',
        'Regime_Context': signal if signal == 'Bidirectional' else 'Expansion',
        'IV_Context': f"gap_30d={gap_30d:.1f}, gap_60d={gap_60d:.1f}, gap_180d={gap_180d:.1f}, IV_Rank={iv_rank:.0f}",
        'Capital_Requirement': 8000,
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
    regime = row.get('Regime', '')
    signal = row.get('Signal_Type', '')
    gap_30d = row.get('IVHV_gap_30D', 0)
    iv_rank = row.get('IV_Rank_XS', 50)
    
    # Calculate longer-term gaps
    iv_180 = row.get('IV_180_D_Call', 0)
    hv_180 = row.get('HV_180_D_Cur', 0)
    gap_180d = (iv_180 - hv_180) if (iv_180 and hv_180) else 0
    
    iv_60 = row.get('IV_60_D_Call', 0)
    hv_60 = row.get('HV_60_D_Cur', 0)
    gap_60d = (iv_60 - hv_60) if (iv_60 and hv_60) else 0
    
    # Expansion proxy
    expansion = (iv_rank < 50 and (gap_180d < 0 or gap_60d < 0))
    
    # Rejection criteria
    if not expansion and signal != 'Bidirectional':
        return None
    if gap_180d >= 0 and gap_60d >= 0:
        return None
    if iv_rank >= 40: # Adjusted to match audit's 'IV_Rank < 40'
        return None  # Not cheap enough
    
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Long Strangle',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Expansion + Moderately Cheap IV (IV_Rank={iv_rank:.0f})",
        'Theory_Source': 'Natenberg Ch.9 - OTM volatility (cheaper, needs bigger move)',
        'Regime_Context': signal if signal == 'Bidirectional' else 'Expansion',
        'IV_Context': f"gap_30d={gap_30d:.1f}, gap_60d={gap_60d:.1f}, gap_180d={gap_180d:.1f}, IV_Rank={iv_rank:.0f}",
        'Capital_Requirement': 5000,
        'Risk_Profile': 'Defined (max loss = total premium)',
        'Greeks_Exposure': 'Delta-neutral, Long Vega, Short Theta',
        'Execution_Ready': True,
        'Confidence': 68,
        'Trade_Bias': 'Bidirectional',
        'Strategy_Type': 'Volatility',
    }


def _validate_long_call_leap(ticker: str, row: pd.Series) -> Optional[Dict]:
    """
    Validate Long Call LEAP strategy.
    
    Theory: Hull Ch.10 - Multi-year directional thesis with structural conviction.
    Entry: Sustained bullish signal + Low IV + Cheap long-term IV.
    
    LEAP-Specific Criteria (distinguish from short-term Long Call):
    - Sustained bullish signal (not just short-term momentum)
    - IV_Rank < 40 (prefer buying when IV suppressed for long term)
    - gap_180d < -5 (want cheap long-term IV)
    - Capital-heavy but defined risk (typical $2000-$5000 per contract)
    """
    signal = row.get('Signal_Type', '')
    gap_30d = row.get('IVHV_gap_30D', 0)
    iv_rank = row.get('IV_Rank_XS', 50)
    
    # Calculate longer-term gaps
    iv_180 = row.get('IV_180_D_Call', 0)
    hv_180 = row.get('HV_180_D_Cur', 0)
    gap_180d = (iv_180 - hv_180) if (iv_180 and hv_180) else 0
    
    iv_60 = row.get('IV_60_D_Call', 0)
    hv_60 = row.get('HV_60_D_Cur', 0)
    gap_60d = (iv_60 - hv_60) if (iv_60 and hv_60) else 0
    
    # LEAP criteria (stricter than short-term)
    # Note: 'Sustained Bullish' is stricter; fallback to 'Bullish' if not available
    if signal not in ['Sustained Bullish', 'Bullish']:
        return None
    if gap_180d >= 0:  # Want cheap long-term IV (negative gap)
        return None
    
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Long Call LEAP',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Bullish + Structural thesis + Cheap long-term IV (IV_Rank={iv_rank:.0f}, gap_180d={gap_180d:.1f})",
        'Theory_Source': 'Hull Ch.10 - Multi-year directional with defined risk',
        'Regime_Context': signal,
        'IV_Context': f"gap_30d={gap_30d:.1f}, gap_60d={gap_60d:.1f}, gap_180d={gap_180d:.1f}, IV_Rank={iv_rank:.0f}",
        'Capital_Requirement': 3000,
        'Risk_Profile': 'Defined (max loss = premium, typically $2000-$5000)',
        'Greeks_Exposure': 'Long Delta (lower), Long Vega, Short Theta (minimal)',
        'Execution_Ready': True,
        'Confidence': 70,
        'Trade_Bias': 'Bullish',
        'Strategy_Type': 'Directional',
    }


def _validate_long_put_leap(ticker: str, row: pd.Series) -> Optional[Dict]:
    """
    Validate Long Put LEAP strategy.
    
    Theory: Hull Ch.10 - Multi-year hedging or structural bearish thesis.
    Entry: Sustained bearish signal + Low IV + Cheap long-term IV.
    
    LEAP-Specific Criteria:
    - Sustained bearish signal or hedge rationale
    - IV_Rank < 40 (don't overpay for long-term protection)
    - gap_180d < -5 (cheap long-term IV)
    """
    signal = row.get('Signal_Type', '')
    gap_30d = row.get('IVHV_gap_30D', 0)
    iv_rank = row.get('IV_Rank_XS', 50)
    
    # Calculate longer-term gaps
    iv_180 = row.get('IV_180_D_Put', 0)
    hv_180 = row.get('HV_180_D_Cur', 0)
    gap_180d = (iv_180 - hv_180) if (iv_180 and hv_180) else 0
    
    iv_60 = row.get('IV_60_D_Put', 0)
    hv_60 = row.get('HV_60_D_Cur', 0)
    gap_60d = (iv_60 - hv_60) if (iv_60 and hv_60) else 0
    
    # LEAP criteria
    if signal not in ['Sustained Bearish', 'Bearish']:
        return None
    if gap_180d >= 0:
        return None
    
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Long Put LEAP',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Bearish + Structural thesis + Cheap long-term IV (IV_Rank={iv_rank:.0f}, gap_180d={gap_180d:.1f})",
        'Theory_Source': 'Hull Ch.10 - Multi-year protective or directional',
        'Regime_Context': signal,
        'IV_Context': f"gap_30d={gap_30d:.1f}, gap_60d={gap_60d:.1f}, gap_180d={gap_180d:.1f}, IV_Rank={iv_rank:.0f}",
        'Capital_Requirement': 3000,
        'Risk_Profile': 'Defined (max loss = premium, typically $2000-$5000)',
        'Greeks_Exposure': 'Short Delta (lower), Long Vega, Short Theta (minimal)',
        'Execution_Ready': True,
        'Confidence': 70,
        'Trade_Bias': 'Bearish',
        'Strategy_Type': 'Directional',
    }


def _validate_call_debit_spread(ticker: str, row: pd.Series) -> Optional[Dict]:
    """
    Validate Call Debit Spread strategy.
    
    Theory: Natenberg Ch.5 - Directional bullish with defined risk/reward.
    Entry: Bullish signal + Any IV regime.
    """
    stock_price = _calculate_approx_stock_price(row)
    if stock_price == 0:
        return None
    
    # Approximate capital for a debit spread (e.g., 2% of stock price for spread width)
    capital_req = stock_price * 0.02 * 100
    
    signal = row.get('Signal_Type', '')
    
    # Rejection criteria
    if signal not in ['Bullish', 'Sustained Bullish']:
        return None
    
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Call Debit Spread',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Bullish + Defined Risk/Reward",
        'Theory_Source': 'Natenberg Ch.5 - Directional with limited risk',
        'Regime_Context': signal,
        'IV_Context': 'Any IV regime',
        'Capital_Requirement': capital_req,
        'Risk_Profile': 'Defined (max loss = debit paid)',
        'Greeks_Exposure': 'Long Delta, Short Vega, Short Theta',
        'Execution_Ready': True,
        'Confidence': 60,
        'Trade_Bias': 'Bullish',
        'Strategy_Type': 'Directional',
    }


def _validate_put_debit_spread(ticker: str, row: pd.Series) -> Optional[Dict]:
    """
    Validate Put Debit Spread strategy.
    
    Theory: Natenberg Ch.5 - Directional bearish with defined risk/reward.
    Entry: Bearish signal + Any IV regime.
    """
    stock_price = _calculate_approx_stock_price(row)
    if stock_price == 0:
        return None
    
    # Approximate capital for a debit spread (e.g., 2% of stock price for spread width)
    capital_req = stock_price * 0.02 * 100
    
    signal = row.get('Signal_Type', '')
    
    # Rejection criteria
    if signal not in ['Bearish']:
        return None
    
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Put Debit Spread',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Bearish + Defined Risk/Reward",
        'Theory_Source': 'Natenberg Ch.5 - Directional with limited risk',
        'Regime_Context': signal,
        'IV_Context': 'Any IV regime',
        'Capital_Requirement': capital_req,
        'Risk_Profile': 'Defined (max loss = debit paid)',
        'Greeks_Exposure': 'Short Delta, Short Vega, Short Theta',
        'Execution_Ready': True,
        'Confidence': 60,
        'Trade_Bias': 'Bearish',
        'Strategy_Type': 'Directional',
    }


# ==========================================
# MAIN RECOMMENDATION FUNCTION
# ==========================================

def recommend_strategies(
    df: pd.DataFrame,
    min_iv_rank: float = 60,
    min_ivhv_gap: float = 5.0,
    enable_directional: bool = True,
    enable_neutral: bool = True,
    enable_volatility: bool = True,
    tier_filter: str = 'tier1_only',
    exploration_mode: bool = False
) -> pd.DataFrame:
    """
    Generate multi-strategy recommendations using Strategy Ledger architecture.
    
    🚨 ARCHITECTURAL CHANGE (2025-01-XX):
    Moved from single-strategy-per-ticker to Strategy Ledger pattern.
    Each ticker may generate MULTIPLE strategies simultaneously.
    
    **Strategy Ledger Pattern**:
    - Each row = (Ticker × Strategy) pairing
    - Independent validators (no if/elif chains)
    - Additive logic (append all valid strategies)
    - Theory-explicit (Valid_Reason + Theory_Source)
    
    **Theory Compliance**:
    - Multiple strategies can coexist for same ticker (Hull)
    - Bullish ticker can have: Long Call + CSP + Buy-Write (capital/risk-dependent)
    - Expansion ticker can have: Long Straddle + Long Strangle (budget-dependent)
    - Strategy discovery ≠ execution filtering (Step 7 vs Step 9B)
    
    Returns:
        DataFrame with Strategy Ledger format:
        - Multiple rows per ticker (if multiple strategies valid)
        - Columns: Ticker, Strategy_Name, Valid_Reason, Theory_Source, etc.
        - No Primary_Strategy (deprecated single-strategy schema)
    
    Example Output:
        | Ticker | Strategy_Name      | Valid_Reason                          | Capital_Requirement |
        |--------|--------------------|---------------------------------------|---------------------|
        | AAPL   | Long Call          | Bullish + Cheap IV (gap_180d=-12.3)  | 500                 |
        | AAPL   | Cash-Secured Put   | Bullish + Rich IV (IV_Rank=65)       | 15000               |
        | MELI   | Long Straddle      | Expansion + Very Cheap IV (rank=28)  | 8000                |
        | MELI   | Long Strangle      | Expansion + Moderately Cheap IV      | 5000                |
    
    Strategy Selection Logic:
        **TIER-1 STRATEGIES (Broker-Approved)**:
        1. **Long Call**: Bullish + Cheap IV (gap < 0)
        2. **Long Put**: Bearish + Cheap IV (gap < 0)
        3. **Cash-Secured Put**: Bullish + Rich IV (gap > 0, IV_Rank ≤ 70)
        4. **Covered Call**: Bearish + Rich IV (requires stock ownership)
        5. **Buy-Write**: Bullish + Very Rich IV (IV_Rank > 70)
        6. **Long Straddle**: Expansion + Very Cheap IV (IV_Rank < 35)
        7. **Long Strangle**: Expansion + Moderately Cheap IV (35 ≤ IV_Rank < 50)
    
    Usage Notes:
        - Step 7 = DISCOVERY ONLY (no execution filtering)
        - Step 9B = EXECUTION VALIDATION (liquidity, strikes, capital, Greeks)
        - User chooses from multiple strategies based on capital/risk preference
    """
    from .utils import validate_input
    
    # Create working copy
    df = df.copy()
    
    # Validate required columns (flexible IV_Rank column name)
    required_cols = [
        'Ticker', 'IVHV_gap_30D', 'Signal_Type', 'Regime'
    ]
    validate_input(df, required_cols, 'Step 7')
    
    # Handle IV_Rank column flexibility
    if 'IV_Rank_XS' not in df.columns:
        if 'IV_Rank_30D' in df.columns:
            df['IV_Rank_XS'] = df['IV_Rank_30D']
            logger.info(f"ℹ️ Using IV_Rank_30D as IV_Rank_XS")
        elif 'IV30_Call' in df.columns and 'HV30' in df.columns:
            # Calculate IV_Rank from IV30 if neither rank column exists
            logger.info(f"ℹ️ Calculating IV_Rank_XS from IV30_Call")
            df['IV_Rank_XS'] = 50.0  # Default to neutral rank
            # Per-ticker percentile calculation would go here if we had historical data
        else:
            # Last resort: use constant neutral value
            logger.warning(f"⚠️ No IV_Rank column found, using neutral value (50.0)")
            df['IV_Rank_XS'] = 50.0
    
    # Work on all data (no Data_Complete filtering since Step 6 already validates)
    df_complete = df.copy()
    logger.info(f"🎯 Step 7 (MULTI-STRATEGY): Processing {len(df_complete)} tickers")
    
    if df_complete.empty:
        logger.warning("⚠️ No tickers with complete data")
        return df
    
    # === MULTI-STRATEGY LEDGER GENERATION ===
    # Additive logic: append all valid strategies (no if/elif chains)
    strategies = []
    
    # Define independent validators (order-independent)
    validators = []
    if enable_directional:
        validators.extend([
            _validate_long_call,
            _validate_long_put,
            _validate_long_call_leap,  # LEAP variant
            _validate_long_put_leap,   # LEAP variant
            _validate_csp,
            _validate_covered_call,
            _validate_buy_write,
            _validate_call_debit_spread, # Added Call Debit Spread
            _validate_put_debit_spread,  # Added Put Debit Spread
        ])
    if enable_volatility:
        validators.extend([
            _validate_long_straddle,
            _validate_long_strangle,
        ])
    
    # Apply validators additively
    for idx, row in df_complete.iterrows():
        ticker = row['Ticker']
        
        # Run all validators (independent, no mutual exclusion)
        for validator in validators:
            strategy = validator(ticker, row)
            if strategy:  # If valid, append
                # Copy all original row data for Step 9B
                strategy_with_context = {**row.to_dict(), **strategy}
                strategies.append(strategy_with_context)
    
    # Convert to DataFrame (Strategy Ledger)
    if not strategies:
        logger.warning("⚠️ No strategies generated! Possible causes:")
        logger.warning("   - Input data missing required fields")
        logger.warning("   - All signals too weak")
        logger.warning("   - IV context not favorable")
        return df
    
    df_ledger = pd.DataFrame(strategies)
    
    # Log multi-strategy stats
    strategies_per_ticker = df_ledger.groupby('Ticker').size()
    avg_strategies = strategies_per_ticker.mean()
    max_strategies = strategies_per_ticker.max()
    
    logger.info(f"📊 STRATEGY LEDGER STATS:")
    logger.info(f"   Total strategies: {len(df_ledger)}")
    logger.info(f"   Unique tickers: {df_ledger['Ticker'].nunique()}")
    logger.info(f"   Avg strategies/ticker: {avg_strategies:.2f}")
    logger.info(f"   Max strategies/ticker: {max_strategies}")
    
    # Strategy breakdown
    strategy_counts = df_ledger['Strategy_Name'].value_counts()
    logger.info(f"   Strategy breakdown:")
    for strategy, count in strategy_counts.items():
        logger.info(f"      {strategy}: {count}")
    
    # === TIER-1 ENFORCEMENT ===
    # Apply tier filtering if requested
    if tier_filter == 'tier1_only' and not exploration_mode:
        total_count = len(df_ledger)
        tier1_count = (df_ledger['Strategy_Tier'] == 1).sum()
        df_ledger = df_ledger[df_ledger['Strategy_Tier'] == 1].copy()
        logger.info(f"🔒 TIER-1 FILTER: {tier1_count}/{total_count} strategies are Tier-1")
    elif tier_filter == 'include_tier2':
        df_ledger = df_ledger[df_ledger['Strategy_Tier'].isin([1, 2])].copy()
        logger.info(f"📋 TIER-1+2 FILTER: Including Tier-1 and Tier-2")
    elif tier_filter == 'all_tiers' or exploration_mode:
        logger.info(f"📚 EXPLORATION MODE: Including all tiers")
    
    # Tag execution readiness
    if exploration_mode or tier_filter != 'tier1_only':
        df_ledger['EXECUTABLE'] = (df_ledger['Strategy_Tier'] == 1).astype('bool')
        non_exec_count = (~df_ledger['EXECUTABLE']).sum()
        logger.warning(f"⚠️ {non_exec_count} strategies tagged NON_EXECUTABLE")
    else:
        df_ledger['EXECUTABLE'] = True
    
    # 🚨 HARD RULE: No ranking or single-strategy selection in Step 7.
    # The multi-strategy ledger is the authoritative output.
    # Removed _add_legacy_columns to enforce this.
    
    return df_ledger
