"""
Step 9B: Fetch Option Contracts from Schwab API

PURPOSE:
    Fetch real option chains from Schwab Trader API for each (Ticker, Strategy, Timeframe)
    combination. Respect DTE windows from Step 9A. Attach Greeks, IV, bid/ask, OI, spreads.
    
    This step answers:
    - "What contracts are structurally available?"
    - "What are the Greeks and pricing?"
    - "Is the chain liquid enough to trade?"
    
ARCHITECTURAL PRINCIPLE: EXPLORATION MODE
    ✅ Fetch chains and annotate quality (don't reject prematurely)
    ✅ Log rejection reasons at chain level
    ✅ Preserve multiple contracts per strategy when appropriate
    ✅ Strategy-aware chain selection (calls vs puts vs straddles)
    
INPUTS (from Step 9A):
    - Ticker: Symbol
    - Strategy_Name: Strategy name
    - Strategy_Type: Directional/Neutral/Volatility/Mixed
    - Trade_Bias: Bullish/Bearish/Neutral/Bidirectional
    - Min_DTE: Minimum days to expiration
    - Max_DTE: Maximum days to expiration
    - Target_DTE: Preferred days to expiration
    - last_price: Underlying price from snapshot
    
OUTPUTS:
    **Contract Status:**
    - Contract_Selection_Status: 'Contracts_Available' / 'No_Chains_Available' / 'No_Expirations_In_DTE_Window'
    
    **Contract Details (when available):**
    - Selected_Expiration: Expiration date (YYYY-MM-DD)
    - Actual_DTE: Days to expiration
    - Selected_Strike: Strike price (single) or JSON array for multi-leg
    - Contract_Symbol: OCC symbol(s)
    - Option_Type: 'call' / 'put' / 'straddle' / 'strangle'
    
    **Greeks (per contract):**
    - Delta, Gamma, Vega, Theta, Rho
    - Delta_Total: Sum of deltas for multi-leg
    - Theta_Total: Sum of thetas for multi-leg
    - Vega_Total: Sum of vegas for multi-leg
    
    **Pricing:**
    - Bid, Ask, Mid, Last
    - Bid_Ask_Spread_Pct: (ask - bid) / mid * 100
    - Total_Debit: For debit spreads
    - Total_Credit: For credit spreads
    
    **Liquidity:**
    - Open_Interest: Total OI
    - Volume: Daily volume
    - Liquidity_Grade: 'Excellent' / 'Good' / 'Acceptable' / 'Thin'
    - Liquidity_Score: 0-100
    - Liquidity_Reason: Human explanation
    
    **Rejection Tracking:**
    - Chain_Rejection_Reason: Why no contracts (if applicable)
    - Expirations_Checked: How many dates were evaluated
    - Strikes_Available: Strike count in selected expiration
"""

import pandas as pd
import numpy as np
import os
import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import json
import time

# Add project root to path
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.scan_engine.schwab_api_client import SchwabClient

logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION
# ============================================================

# Schwab API throttling
CHAIN_FETCH_DELAY = 0.5  # 500ms between chain fetches (2 req/sec)

# Strike selection (for single-leg strategies)
STRIKE_RANGE_PERCENT = 0.15  # Look at strikes within ±15% of current price

# Liquidity thresholds (market hours)
OI_EXCELLENT = 500
OI_GOOD = 100
OI_ACCEPTABLE = 25
OI_THIN = 10

SPREAD_EXCELLENT = 0.03  # < 3%
SPREAD_GOOD = 0.05       # < 5%
SPREAD_ACCEPTABLE = 0.10 # < 10%
SPREAD_WIDE = 0.20       # < 20%

# Relaxed thresholds for off-hours / closed market
OI_ACCEPTABLE_OFFHOURS = 10  # Lower OI acceptable when market closed
SPREAD_ACCEPTABLE_OFFHOURS = 0.20  # Wider spread acceptable when market closed

# Delta targeting for different strategies
DELTA_TARGETS = {
    'Long Call': (0.30, 0.70),        # Balanced calls
    'Long Put': (-0.70, -0.30),       # Balanced puts
    'Long Call LEAP': (0.60, 0.90),   # ITM LEAPs
    'Long Put LEAP': (-0.90, -0.60),  # ITM LEAPs
    'CSP': (-0.30, -0.15),            # OTM puts
    'Cash-Secured Put': (-0.30, -0.15),
    'Covered Call': (0.20, 0.40),     # OTM calls
    'Buy-Write': (0.20, 0.40),        # OTM calls
    'Straddle': None,                 # ATM (closest to 0.50 delta)
    'Strangle': None,                 # OTM on both sides
}

# Contract status enum
CONTRACT_STATUS_OK = 'OK'
CONTRACT_STATUS_NO_CHAIN = 'NO_CHAIN_RETURNED'
CONTRACT_STATUS_NO_EXPIRATIONS = 'NO_EXPIRATIONS_IN_WINDOW'
CONTRACT_STATUS_NO_CALLS = 'NO_CALLS_AVAILABLE'
CONTRACT_STATUS_NO_PUTS = 'NO_PUTS_AVAILABLE'
CONTRACT_STATUS_LIQUIDITY_FAIL = 'FAILED_LIQUIDITY_FILTER'
CONTRACT_STATUS_GREEKS_FAIL = 'FAILED_GREEKS_FILTER'
CONTRACT_STATUS_IV_FAIL = 'FAILED_IV_FILTER'
CONTRACT_STATUS_LEAP_FALLBACK = 'LEAP_FALLBACK'

# LEAP strategy names (eligible for fallback)
LEAP_STRATEGIES = {'Long Call LEAP', 'Long Put LEAP'}

# Minimum LEAP DTE threshold
LEAP_MIN_DTE = 365

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def calculate_dte(expiration_str: str) -> int:
    """Calculate days to expiration from expiration date string."""
    try:
        exp_date = datetime.strptime(expiration_str, '%Y-%m-%d')
        today = datetime.now()
        return (exp_date - today).days
    except:
        return -1

def find_best_expiration(expirations: List[str], min_dte: int, max_dte: int, target_dte: int) -> Optional[str]:
    """
    Find the best expiration within DTE window.
    Prioritize: closest to target, then closest to min if target not available.
    """
    valid_exps = []
    
    for exp in expirations:
        dte = calculate_dte(exp)
        if min_dte <= dte <= max_dte:
            valid_exps.append((exp, dte, abs(dte - target_dte)))
    
    if not valid_exps:
        return None
    
    # Sort by distance from target
    valid_exps.sort(key=lambda x: x[2])
    return valid_exps[0][0]  # Return expiration with smallest distance to target

def find_best_expiration_with_fallback(expirations: List[str], min_dte: int, max_dte: int, 
                                       target_dte: int, is_leap: bool) -> Tuple[Optional[str], str]:
    """
    Find the best expiration within DTE window, with LEAP fallback.
    
    Returns:
        (expiration, status) where status is 'OK' or 'LEAP_FALLBACK'
    """
    # First try exact match
    best_exp = find_best_expiration(expirations, min_dte, max_dte, target_dte)
    if best_exp:
        return best_exp, CONTRACT_STATUS_OK
    
    # LEAP fallback: if no expirations in LEAP window, use longest available
    if is_leap and min_dte >= LEAP_MIN_DTE:
        all_dtes = [(exp, calculate_dte(exp)) for exp in expirations]
        if all_dtes:
            # Sort by DTE descending, take longest
            all_dtes.sort(key=lambda x: x[1], reverse=True)
            longest_exp, longest_dte = all_dtes[0]
            
            # Only use fallback if longest is at least 180 days (reasonable)
            if longest_dte >= 180:
                logger.warning(f"LEAP fallback: requested {min_dte}-{max_dte} DTE, using longest available: {longest_dte} DTE")
                return longest_exp, CONTRACT_STATUS_LEAP_FALLBACK
    
    return None, CONTRACT_STATUS_NO_EXPIRATIONS

def find_strike_by_delta(chain_df: pd.DataFrame, target_delta_range: Tuple[float, float], 
                         underlying_price: float, option_type: str) -> Optional[Dict]:
    """
    Find the best strike within target delta range.
    
    Args:
        chain_df: Option chain DataFrame
        target_delta_range: (min_delta, max_delta)
        underlying_price: Current stock price
        option_type: 'call' or 'put'
    
    Returns:
        Dict with contract details or None
    """
    if chain_df.empty:
        return None
    
    # Filter by option type
    type_chain = chain_df[chain_df['putCall'] == option_type.upper()].copy()
    if type_chain.empty:
        return None
    
    # Filter by delta range
    min_delta, max_delta = target_delta_range
    
    if option_type == 'call':
        valid = type_chain[(type_chain['delta'] >= min_delta) & (type_chain['delta'] <= max_delta)]
    else:  # put
        valid = type_chain[(type_chain['delta'] <= min_delta) & (type_chain['delta'] >= max_delta)]
    
    if valid.empty:
        # Fallback: find closest to midpoint of range
        mid_delta = (min_delta + max_delta) / 2
        type_chain['delta_dist'] = abs(type_chain['delta'] - mid_delta)
        best = type_chain.nsmallest(1, 'delta_dist')
    else:
        # Find the one closest to middle of range
        mid_delta = (min_delta + max_delta) / 2
        valid['delta_dist'] = abs(valid['delta'] - mid_delta)
        best = valid.nsmallest(1, 'delta_dist')
    
    if best.empty:
        return None
    
    row = best.iloc[0]
    return {
        'strike': row['strikePrice'],
        'symbol': row['symbol'],
        'delta': row['delta'],
        'gamma': row['gamma'],
        'vega': row['vega'],
        'theta': row['theta'],
        'rho': row.get('rho', 0),
        'bid': row['bid'],
        'ask': row['ask'],
        'last': row['last'],
        'mark': row['mark'],
        'bid_size': row.get('bidSize', 0),
        'ask_size': row.get('askSize', 0),
        'volume': row.get('totalVolume', 0),
        'open_interest': row.get('openInterest', 0),
        'implied_volatility': row.get('volatility', 0) * 100,  # Convert to percentage
    }

def find_atm_strike(chain_df: pd.DataFrame, underlying_price: float, option_type: str) -> Optional[Dict]:
    """
    Find ATM strike (closest to underlying price).
    """
    if chain_df.empty:
        return None
    
    type_chain = chain_df[chain_df['putCall'] == option_type.upper()].copy()
    if type_chain.empty:
        return None
    
    # Find strike closest to underlying price
    type_chain['strike_dist'] = abs(type_chain['strikePrice'] - underlying_price)
    best = type_chain.nsmallest(1, 'strike_dist')
    
    if best.empty:
        return None
    
    row = best.iloc[0]
    return {
        'strike': row['strikePrice'],
        'symbol': row['symbol'],
        'delta': row['delta'],
        'gamma': row['gamma'],
        'vega': row['vega'],
        'theta': row['theta'],
        'rho': row.get('rho', 0),
        'bid': row['bid'],
        'ask': row['ask'],
        'last': row['last'],
        'mark': row['mark'],
        'bid_size': row.get('bidSize', 0),
        'ask_size': row.get('askSize', 0),
        'volume': row.get('totalVolume', 0),
        'open_interest': row.get('openInterest', 0),
        'implied_volatility': row.get('volatility', 0) * 100,
    }

def grade_liquidity(bid: float, ask: float, mid: float, oi: int, volume: int, 
                    market_open: bool = True) -> Tuple[str, int, str]:
    """
    Grade contract liquidity with market-hours awareness.
    
    Args:
        bid, ask, mid: Pricing
        oi: Open interest
        volume: Daily volume
        market_open: Whether market is currently open (relaxes thresholds)
    
    Returns:
        (grade, score, reason)
    """
    if mid == 0:
        spread_pct = 100.0
    else:
        spread_pct = (ask - bid) / mid * 100
    
    # Adjust thresholds based on market hours
    oi_acceptable = OI_ACCEPTABLE if market_open else OI_ACCEPTABLE_OFFHOURS
    spread_acceptable = SPREAD_ACCEPTABLE if market_open else SPREAD_ACCEPTABLE_OFFHOURS
    
    # Calculate liquidity score (0-100)
    oi_score = min(100, (oi / OI_EXCELLENT) * 50)
    spread_score = max(0, 50 - (spread_pct / spread_acceptable) * 50)
    score = int(oi_score + spread_score)
    
    # Determine grade
    if oi >= OI_EXCELLENT and spread_pct <= SPREAD_EXCELLENT:
        grade = 'Excellent'
        reason = f'Deep OI ({oi}), tight spread ({spread_pct:.1f}%)'
    elif oi >= OI_GOOD and spread_pct <= SPREAD_GOOD:
        grade = 'Good'
        reason = f'Adequate OI ({oi}), reasonable spread ({spread_pct:.1f}%)'
    elif oi >= oi_acceptable and spread_pct <= spread_acceptable:
        grade = 'Acceptable'
        suffix = ' (off-hours)' if not market_open else ''
        reason = f'Moderate OI ({oi}), acceptable spread ({spread_pct:.1f}%){suffix}'
    elif oi >= OI_THIN:
        grade = 'Thin'
        reason = f'Light OI ({oi}), wide spread ({spread_pct:.1f}%)'
    else:
        grade = 'Illiquid'
        reason = f'Very low OI ({oi}), very wide spread ({spread_pct:.1f}%)'
    
    return grade, score, reason

def parse_schwab_chain_to_dataframe(chain_data: Dict, expiration: str) -> pd.DataFrame:
    """
    Parse Schwab API chain response into DataFrame.
    
    Args:
        chain_data: Raw Schwab API response
        expiration: Specific expiration to extract (YYYY-MM-DD format)
    
    Returns:
        DataFrame with columns: strike, putCall, delta, gamma, vega, theta, rho,
                               bid, ask, last, mark, volume, openInterest, volatility, symbol
    """
    rows = []
    
    # Schwab returns chains as: {callExpDateMap: {...}, putExpDateMap: {...}}
    for option_type in ['call', 'put']:
        exp_map_key = f'{option_type}ExpDateMap'
        if exp_map_key not in chain_data:
            continue
        
        exp_map = chain_data[exp_map_key]
        
        # Find the expiration key (format: "2025-02-14:10" where 10 is DTE)
        exp_key = None
        for key in exp_map.keys():
            if key.startswith(expiration):
                exp_key = key
                break
        
        if not exp_key:
            continue
        
        strikes = exp_map[exp_key]
        for strike_price, contracts in strikes.items():
            # Schwab returns array of contracts per strike (usually just 1)
            for contract in contracts:
                rows.append({
                    'strikePrice': float(strike_price),
                    'putCall': option_type.upper(),
                    'delta': contract.get('delta', 0),
                    'gamma': contract.get('gamma', 0),
                    'vega': contract.get('vega', 0),
                    'theta': contract.get('theta', 0),
                    'rho': contract.get('rho', 0),
                    'bid': contract.get('bid', 0),
                    'ask': contract.get('ask', 0),
                    'last': contract.get('last', 0),
                    'mark': contract.get('mark', 0),
                    'bidSize': contract.get('bidSize', 0),
                    'askSize': contract.get('askSize', 0),
                    'totalVolume': contract.get('totalVolume', 0),
                    'openInterest': contract.get('openInterest', 0),
                    'volatility': contract.get('volatility', 0),
                    'symbol': contract.get('symbol', ''),
                })
    
    return pd.DataFrame(rows)

# ============================================================
# STRATEGY-SPECIFIC CONTRACT SELECTION
# ============================================================

def select_long_call_contract(chain_df: pd.DataFrame, underlying_price: float, 
                               strategy_name: str) -> Optional[Dict]:
    """Select contract for Long Call or Long Call LEAP."""
    delta_range = DELTA_TARGETS.get(strategy_name, (0.30, 0.70))
    return find_strike_by_delta(chain_df, delta_range, underlying_price, 'call')

def select_long_put_contract(chain_df: pd.DataFrame, underlying_price: float,
                              strategy_name: str) -> Optional[Dict]:
    """Select contract for Long Put or Long Put LEAP."""
    delta_range = DELTA_TARGETS.get(strategy_name, (-0.70, -0.30))
    return find_strike_by_delta(chain_df, delta_range, underlying_price, 'put')

def select_csp_contract(chain_df: pd.DataFrame, underlying_price: float) -> Optional[Dict]:
    """Select contract for Cash-Secured Put."""
    delta_range = (-0.30, -0.15)
    return find_strike_by_delta(chain_df, delta_range, underlying_price, 'put')

def select_covered_call_contract(chain_df: pd.DataFrame, underlying_price: float) -> Optional[Dict]:
    """Select contract for Covered Call or Buy-Write."""
    delta_range = (0.20, 0.40)
    return find_strike_by_delta(chain_df, delta_range, underlying_price, 'call')

def select_straddle_contracts(chain_df: pd.DataFrame, underlying_price: float) -> Optional[Dict]:
    """
    Select contracts for Long Straddle (ATM call + ATM put).
    
    Returns:
        Dict with combined contract details
    """
    call_contract = find_atm_strike(chain_df, underlying_price, 'call')
    put_contract = find_atm_strike(chain_df, underlying_price, 'put')
    
    if not call_contract or not put_contract:
        return None
    
    # Combine into multi-leg structure
    return {
        'strike': call_contract['strike'],  # Should be same for both
        'symbol': json.dumps([call_contract['symbol'], put_contract['symbol']]),
        'delta': call_contract['delta'] + put_contract['delta'],
        'gamma': call_contract['gamma'] + put_contract['gamma'],
        'vega': call_contract['vega'] + put_contract['vega'],
        'theta': call_contract['theta'] + put_contract['theta'],
        'rho': call_contract.get('rho', 0) + put_contract.get('rho', 0),
        'bid': call_contract['bid'] + put_contract['bid'],
        'ask': call_contract['ask'] + put_contract['ask'],
        'last': call_contract['last'] + put_contract['last'],
        'mark': call_contract['mark'] + put_contract['mark'],
        'volume': call_contract['volume'] + put_contract['volume'],
        'open_interest': call_contract['open_interest'] + put_contract['open_interest'],
        'implied_volatility': (call_contract['implied_volatility'] + put_contract['implied_volatility']) / 2,
        'option_type': 'straddle',
    }

def select_strangle_contracts(chain_df: pd.DataFrame, underlying_price: float) -> Optional[Dict]:
    """
    Select contracts for Long Strangle (OTM call + OTM put).
    """
    # Target deltas: call ~0.30, put ~-0.30
    call_contract = find_strike_by_delta(chain_df, (0.25, 0.35), underlying_price, 'call')
    put_contract = find_strike_by_delta(chain_df, (-0.35, -0.25), underlying_price, 'put')
    
    if not call_contract or not put_contract:
        return None
    
    return {
        'strike': json.dumps([put_contract['strike'], call_contract['strike']]),
        'symbol': json.dumps([call_contract['symbol'], put_contract['symbol']]),
        'delta': call_contract['delta'] + put_contract['delta'],
        'gamma': call_contract['gamma'] + put_contract['gamma'],
        'vega': call_contract['vega'] + put_contract['vega'],
        'theta': call_contract['theta'] + put_contract['theta'],
        'rho': call_contract.get('rho', 0) + put_contract.get('rho', 0),
        'bid': call_contract['bid'] + put_contract['bid'],
        'ask': call_contract['ask'] + put_contract['ask'],
        'last': call_contract['last'] + put_contract['last'],
        'mark': call_contract['mark'] + put_contract['mark'],
        'volume': call_contract['volume'] + put_contract['volume'],
        'open_interest': call_contract['open_interest'] + put_contract['open_interest'],
        'implied_volatility': (call_contract['implied_volatility'] + put_contract['implied_volatility']) / 2,
        'option_type': 'strangle',
    }

# ============================================================
# MAIN CONTRACT FETCHING LOGIC
# ============================================================

# ============================================================
# MAIN CONTRACT FETCHING LOGIC
# ============================================================

def fetch_contracts_for_strategy(chain_data: Dict, ticker: str, strategy_name: str,
                                 trade_bias: str, min_dte: int, max_dte: int, 
                                 target_dte: int, underlying_price: float,
                                 market_open: bool) -> Dict:
    """
    Fetch option contracts for a single strategy from CACHED chain data.
    
    Args:
        chain_data: Pre-fetched Schwab chain data (shared across strategies)
        ticker: Stock symbol
        strategy_name: Strategy name
        trade_bias: Bullish/Bearish/Neutral/Bidirectional
        min_dte, max_dte, target_dte: DTE windows
        underlying_price: Current stock price
        market_open: Whether market is currently open
    
    Returns:
        Dict with contract details and status
    """
    result = {
        'Contract_Status': CONTRACT_STATUS_NO_CHAIN,
        'Contract_Selection_Status': 'No_Chains_Available',  # Legacy field
        'Failure_Reason': None,
        'Selected_Expiration': None,
        'Actual_DTE': None,
        'Selected_Strike': None,
        'Contract_Symbol': None,
        'Option_Type': None,
        'Delta': None,
        'Gamma': None,
        'Vega': None,
        'Theta': None,
        'Rho': None,
        'Bid': None,
        'Ask': None,
        'Mid': None,
        'Last': None,
        'Bid_Ask_Spread_Pct': None,
        'Open_Interest': None,
        'Volume': None,
        'Implied_Volatility': None,
        'Liquidity_Grade': None,
        'Liquidity_Score': None,
        'Liquidity_Reason': None,
        'Chain_Rejection_Reason': None,  # Legacy field
        'Expirations_Checked': 0,
        'Strikes_Available': 0,
    }
    
    try:
        if not chain_data or 'callExpDateMap' not in chain_data:
            result['Failure_Reason'] = 'No option chains returned from API'
            result['Chain_Rejection_Reason'] = result['Failure_Reason']
            return result
        
        # Extract available expirations
        expirations = []
        if 'callExpDateMap' in chain_data:
            for key in chain_data['callExpDateMap'].keys():
                exp_date = key.split(':')[0]  # Format: "2025-02-14:10"
                if exp_date not in expirations:
                    expirations.append(exp_date)
        
        if not expirations:
            result['Contract_Status'] = CONTRACT_STATUS_NO_EXPIRATIONS
            result['Failure_Reason'] = 'No expirations found in chain data'
            result['Chain_Rejection_Reason'] = result['Failure_Reason']
            return result
        
        result['Expirations_Checked'] = len(expirations)
        
        # Find best expiration with LEAP fallback
        is_leap = strategy_name in LEAP_STRATEGIES
        best_exp, status = find_best_expiration_with_fallback(
            expirations, min_dte, max_dte, target_dte, is_leap
        )
        
        if not best_exp:
            result['Contract_Status'] = CONTRACT_STATUS_NO_EXPIRATIONS
            result['Contract_Selection_Status'] = 'No_Expirations_In_DTE_Window'
            result['Failure_Reason'] = f'No expirations in DTE range {min_dte}-{max_dte} (target: {target_dte})'
            result['Chain_Rejection_Reason'] = result['Failure_Reason']
            return result
        
        actual_dte = calculate_dte(best_exp)
        result['Selected_Expiration'] = best_exp
        result['Actual_DTE'] = actual_dte
        result['Contract_Status'] = status  # OK or LEAP_FALLBACK
        
        # Parse chain for this expiration
        chain_df = parse_schwab_chain_to_dataframe(chain_data, best_exp)
        
        if chain_df.empty:
            result['Contract_Status'] = CONTRACT_STATUS_NO_CHAIN
            result['Failure_Reason'] = f'No contracts found for expiration {best_exp}'
            result['Chain_Rejection_Reason'] = result['Failure_Reason']
            return result
        
        result['Strikes_Available'] = len(chain_df['strikePrice'].unique())
        
        # Select contract based on strategy
        contract = None
        failure_reason = None
        
        if 'Long Call' in strategy_name:
            calls = chain_df[chain_df['putCall'] == 'CALL']
            if calls.empty:
                result['Contract_Status'] = CONTRACT_STATUS_NO_CALLS
                result['Failure_Reason'] = 'No call options available'
                result['Chain_Rejection_Reason'] = result['Failure_Reason']
                return result
            contract = select_long_call_contract(chain_df, underlying_price, strategy_name)
            if contract:
                result['Option_Type'] = 'call'
            else:
                failure_reason = 'No calls match delta target'
        
        elif 'Long Put' in strategy_name:
            puts = chain_df[chain_df['putCall'] == 'PUT']
            if puts.empty:
                result['Contract_Status'] = CONTRACT_STATUS_NO_PUTS
                result['Failure_Reason'] = 'No put options available'
                result['Chain_Rejection_Reason'] = result['Failure_Reason']
                return result
            contract = select_long_put_contract(chain_df, underlying_price, strategy_name)
            if contract:
                result['Option_Type'] = 'put'
            else:
                failure_reason = 'No puts match delta target'
        
        elif 'CSP' in strategy_name or 'Cash-Secured Put' in strategy_name:
            puts = chain_df[chain_df['putCall'] == 'PUT']
            if puts.empty:
                result['Contract_Status'] = CONTRACT_STATUS_NO_PUTS
                result['Failure_Reason'] = 'No put options available'
                result['Chain_Rejection_Reason'] = result['Failure_Reason']
                return result
            contract = select_csp_contract(chain_df, underlying_price)
            if contract:
                result['Option_Type'] = 'put'
            else:
                failure_reason = 'No puts match delta target for CSP'
        
        elif 'Covered Call' in strategy_name or 'Buy-Write' in strategy_name:
            calls = chain_df[chain_df['putCall'] == 'CALL']
            if calls.empty:
                result['Contract_Status'] = CONTRACT_STATUS_NO_CALLS
                result['Failure_Reason'] = 'No call options available'
                result['Chain_Rejection_Reason'] = result['Failure_Reason']
                return result
            contract = select_covered_call_contract(chain_df, underlying_price)
            if contract:
                result['Option_Type'] = 'call'
            else:
                failure_reason = 'No calls match delta target for covered call'
        
        elif 'Straddle' in strategy_name:
            calls = chain_df[chain_df['putCall'] == 'CALL']
            puts = chain_df[chain_df['putCall'] == 'PUT']
            if calls.empty:
                result['Contract_Status'] = CONTRACT_STATUS_NO_CALLS
                result['Failure_Reason'] = 'No call options for straddle'
                result['Chain_Rejection_Reason'] = result['Failure_Reason']
                return result
            if puts.empty:
                result['Contract_Status'] = CONTRACT_STATUS_NO_PUTS
                result['Failure_Reason'] = 'No put options for straddle'
                result['Chain_Rejection_Reason'] = result['Failure_Reason']
                return result
            contract = select_straddle_contracts(chain_df, underlying_price)
            if contract:
                result['Option_Type'] = 'straddle'
            else:
                failure_reason = 'Could not find matching ATM call+put for straddle'
        
        elif 'Strangle' in strategy_name:
            calls = chain_df[chain_df['putCall'] == 'CALL']
            puts = chain_df[chain_df['putCall'] == 'PUT']
            if calls.empty:
                result['Contract_Status'] = CONTRACT_STATUS_NO_CALLS
                result['Failure_Reason'] = 'No call options for strangle'
                result['Chain_Rejection_Reason'] = result['Failure_Reason']
                return result
            if puts.empty:
                result['Contract_Status'] = CONTRACT_STATUS_NO_PUTS
                result['Failure_Reason'] = 'No put options for strangle'
                result['Chain_Rejection_Reason'] = result['Failure_Reason']
                return result
            contract = select_strangle_contracts(chain_df, underlying_price)
            if contract:
                result['Option_Type'] = 'strangle'
            else:
                failure_reason = 'Could not find matching OTM call+put for strangle'
        
        else:
            result['Failure_Reason'] = f'Unknown strategy type: {strategy_name}'
            result['Chain_Rejection_Reason'] = result['Failure_Reason']
            return result
        
        if not contract:
            # Contract selection failed - determine why
            if failure_reason:
                result['Contract_Status'] = CONTRACT_STATUS_GREEKS_FAIL
                result['Failure_Reason'] = failure_reason
            else:
                result['Contract_Status'] = CONTRACT_STATUS_GREEKS_FAIL
                result['Failure_Reason'] = 'No suitable contracts found matching strategy criteria'
            result['Chain_Rejection_Reason'] = result['Failure_Reason']
            return result
        
        # Check for valid Greeks
        if contract.get('delta') == 0 and contract.get('gamma') == 0:
            result['Contract_Status'] = CONTRACT_STATUS_GREEKS_FAIL
            result['Failure_Reason'] = 'Contract has no Greeks (off-hours or illiquid)'
            result['Chain_Rejection_Reason'] = result['Failure_Reason']
            # Still populate contract details for transparency
        
        # Check for valid IV
        if contract.get('implied_volatility', 0) == 0:
            if result['Contract_Status'] == CONTRACT_STATUS_GREEKS_FAIL:
                result['Failure_Reason'] += ' + missing IV'
            else:
                result['Contract_Status'] = CONTRACT_STATUS_IV_FAIL
                result['Failure_Reason'] = 'Contract missing implied volatility'
        
        # Populate result with contract details
        if result['Contract_Status'] in [CONTRACT_STATUS_OK, CONTRACT_STATUS_LEAP_FALLBACK]:
            result['Contract_Selection_Status'] = 'Contracts_Available'
        
        result['Selected_Strike'] = contract['strike']
        result['Contract_Symbol'] = contract['symbol']
        result['Delta'] = contract['delta']
        result['Gamma'] = contract['gamma']
        result['Vega'] = contract['vega']
        result['Theta'] = contract['theta']
        result['Rho'] = contract.get('rho', 0)
        result['Bid'] = contract['bid']
        result['Ask'] = contract['ask']
        result['Last'] = contract['last']
        result['Mid'] = contract['mark']
        result['Volume'] = contract['volume']
        result['Open_Interest'] = contract['open_interest']
        result['Implied_Volatility'] = contract['implied_volatility']
        
        # Calculate spread
        mid = contract['mark']
        if mid > 0:
            spread_pct = (contract['ask'] - contract['bid']) / mid * 100
        else:
            spread_pct = 100.0
        result['Bid_Ask_Spread_Pct'] = spread_pct
        
        # Grade liquidity (market-aware)
        grade, score, reason = grade_liquidity(
            contract['bid'], contract['ask'], mid,
            contract['open_interest'], contract['volume'],
            market_open=market_open
        )
        result['Liquidity_Grade'] = grade
        result['Liquidity_Score'] = score
        result['Liquidity_Reason'] = reason
        
        # Check liquidity filter
        if grade == 'Illiquid' and market_open:
            # Only hard-fail on illiquid if market is open
            if result['Contract_Status'] in [CONTRACT_STATUS_OK, CONTRACT_STATUS_LEAP_FALLBACK]:
                result['Contract_Status'] = CONTRACT_STATUS_LIQUIDITY_FAIL
                result['Failure_Reason'] = f'Failed liquidity filter: {reason}'
        
        # Clear legacy rejection reason on success
        if result['Contract_Status'] in [CONTRACT_STATUS_OK, CONTRACT_STATUS_LEAP_FALLBACK]:
            result['Chain_Rejection_Reason'] = None
        else:
            result['Chain_Rejection_Reason'] = result['Failure_Reason']
        
    except Exception as e:
        logger.error(f"Error fetching contracts for {ticker} {strategy_name}: {e}")
        result['Contract_Status'] = CONTRACT_STATUS_NO_CHAIN
        result['Failure_Reason'] = f'API error: {str(e)}'
        result['Chain_Rejection_Reason'] = result['Failure_Reason']
    
    return result
    """
    Fetch option contracts for a single strategy.
    
    Returns:
        Dict with contract details and status
    """
    result = {
        'Contract_Selection_Status': 'No_Chains_Available',
        'Selected_Expiration': None,
        'Actual_DTE': None,
        'Selected_Strike': None,
        'Contract_Symbol': None,
        'Option_Type': None,
        'Delta': None,
        'Gamma': None,
        'Vega': None,
        'Theta': None,
        'Rho': None,
        'Bid': None,
        'Ask': None,
        'Mid': None,
        'Last': None,
        'Bid_Ask_Spread_Pct': None,
        'Open_Interest': None,
        'Volume': None,
        'Implied_Volatility': None,
        'Liquidity_Grade': None,
        'Liquidity_Score': None,
        'Liquidity_Reason': None,
        'Chain_Rejection_Reason': None,
        'Expirations_Checked': 0,
        'Strikes_Available': 0,
    }
    
    try:
        # Fetch option chain from Schwab
        # Parameters: strikeCount controls how many strikes around ATM
        # range: 'ALL' gives all strikes
        # strategy: 'SINGLE' for standard chains
        chain_data = client.get_chains(
            symbol=ticker,
            strikeCount=50,  # Get 50 strikes around ATM
            range='ALL',
            strategy='SINGLE'
        )
        
        if not chain_data or 'callExpDateMap' not in chain_data:
            result['Chain_Rejection_Reason'] = 'No option chains returned from API'
            return result
        
        # Extract available expirations
        expirations = []
        if 'callExpDateMap' in chain_data:
            for key in chain_data['callExpDateMap'].keys():
                exp_date = key.split(':')[0]  # Format: "2025-02-14:10"
                if exp_date not in expirations:
                    expirations.append(exp_date)
        
        if not expirations:
            result['Chain_Rejection_Reason'] = 'No expirations found in chain data'
            return result
        
        result['Expirations_Checked'] = len(expirations)
        
        # Find best expiration within DTE window
        best_exp = find_best_expiration(expirations, min_dte, max_dte, target_dte)
        
        if not best_exp:
            result['Contract_Selection_Status'] = 'No_Expirations_In_DTE_Window'
            result['Chain_Rejection_Reason'] = f'No expirations in DTE range {min_dte}-{max_dte} (target: {target_dte})'
            return result
        
        actual_dte = calculate_dte(best_exp)
        result['Selected_Expiration'] = best_exp
        result['Actual_DTE'] = actual_dte
        
        # Parse chain for this expiration
        chain_df = parse_schwab_chain_to_dataframe(chain_data, best_exp)
        
        if chain_df.empty:
            result['Chain_Rejection_Reason'] = f'No contracts found for expiration {best_exp}'
            return result
        
        result['Strikes_Available'] = len(chain_df['strikePrice'].unique())
        
        # Select contract based on strategy
        contract = None
        
        if 'Long Call' in strategy_name:
            contract = select_long_call_contract(chain_df, underlying_price, strategy_name)
            if contract:
                result['Option_Type'] = 'call'
        
        elif 'Long Put' in strategy_name:
            contract = select_long_put_contract(chain_df, underlying_price, strategy_name)
            if contract:
                result['Option_Type'] = 'put'
        
        elif 'CSP' in strategy_name or 'Cash-Secured Put' in strategy_name:
            contract = select_csp_contract(chain_df, underlying_price)
            if contract:
                result['Option_Type'] = 'put'
        
        elif 'Covered Call' in strategy_name or 'Buy-Write' in strategy_name:
            contract = select_covered_call_contract(chain_df, underlying_price)
            if contract:
                result['Option_Type'] = 'call'
        
        elif 'Straddle' in strategy_name:
            contract = select_straddle_contracts(chain_df, underlying_price)
            if contract:
                result['Option_Type'] = 'straddle'
        
        elif 'Strangle' in strategy_name:
            contract = select_strangle_contracts(chain_df, underlying_price)
            if contract:
                result['Option_Type'] = 'strangle'
        
        else:
            result['Chain_Rejection_Reason'] = f'Unknown strategy type: {strategy_name}'
            return result
        
        if not contract:
            result['Chain_Rejection_Reason'] = f'No suitable contracts found matching strategy criteria'
            return result
        
        # Populate result with contract details
        result['Contract_Selection_Status'] = 'Contracts_Available'
        result['Selected_Strike'] = contract['strike']
        result['Contract_Symbol'] = contract['symbol']
        result['Delta'] = contract['delta']
        result['Gamma'] = contract['gamma']
        result['Vega'] = contract['vega']
        result['Theta'] = contract['theta']
        result['Rho'] = contract.get('rho', 0)
        result['Bid'] = contract['bid']
        result['Ask'] = contract['ask']
        result['Last'] = contract['last']
        result['Mid'] = contract['mark']
        result['Volume'] = contract['volume']
        result['Open_Interest'] = contract['open_interest']
        result['Implied_Volatility'] = contract['implied_volatility']
        
        # Calculate spread
        mid = contract['mark']
        if mid > 0:
            spread_pct = (contract['ask'] - contract['bid']) / mid * 100
        else:
            spread_pct = 100.0
        result['Bid_Ask_Spread_Pct'] = spread_pct
        
        # Grade liquidity
        grade, score, reason = grade_liquidity(
            contract['bid'], contract['ask'], mid,
            contract['open_interest'], contract['volume']
        )
        result['Liquidity_Grade'] = grade
        result['Liquidity_Score'] = score
        result['Liquidity_Reason'] = reason
        
        # Clear rejection reason on success
        result['Chain_Rejection_Reason'] = None
        
    except Exception as e:
        logger.error(f"Error fetching contracts for {ticker} {strategy_name}: {e}")
        result['Chain_Rejection_Reason'] = f'API error: {str(e)}'
    
    return result

def fetch_contracts(df: pd.DataFrame, client: SchwabClient) -> pd.DataFrame:
    """
    Fetch option contracts for all strategies in DataFrame.
    Uses ticker-level chain caching (fetch once per ticker, reuse for all strategies).
    
    Args:
        df: Step 9A output DataFrame
        client: Schwab API client
    
    Returns:
        DataFrame with contract details added
    """
    logger.info(f"🔍 Fetching option contracts for {len(df)} strategies...")
    
    # Check if we have market status info
    market_open = df.iloc[0].get('is_market_open', True) if len(df) > 0 else True
    if 'is_market_open' in df.columns:
        logger.info(f"📊 Market status: {'OPEN' if market_open else 'CLOSED'} (filters adjusted accordingly)")
    
    results = []
    
    # Group by ticker to minimize API calls (EFFICIENCY GAIN)
    ticker_groups = df.groupby('Ticker')
    total_tickers = len(ticker_groups)
    
    # Track statistics
    api_calls = 0
    cache_hits = 0
    
    for ticker_idx, (ticker, group) in enumerate(ticker_groups, 1):
        logger.info(f"📊 Processing {ticker} ({ticker_idx}/{total_tickers}): {len(group)} strategies")
        
        # FETCH ONCE PER TICKER (not per strategy)
        try:
            chain_data = client.get_chains(
                symbol=ticker,
                strikeCount=50,
                range='ALL',
                strategy='SINGLE'
            )
            api_calls += 1
            
            if not chain_data or 'callExpDateMap' not in chain_data:
                logger.warning(f"  ⚠️  {ticker}: No chains returned from Schwab")
                chain_data = {}
        
        except Exception as e:
            logger.error(f"  ❌ {ticker}: Chain fetch error: {e}")
            chain_data = {}
        
        # Process all strategies for this ticker using CACHED chain
        ticker_results = []
        for idx, row in group.iterrows():
            strategy_name = row['Strategy_Name']
            trade_bias = row.get('Trade_Bias', 'Neutral')
            min_dte = int(row['Min_DTE'])
            max_dte = int(row['Max_DTE'])
            target_dte = int(row['Target_DTE'])
            underlying_price = float(row['last_price'])
            
            # Get market status from row if available
            row_market_open = row.get('is_market_open', market_open)
            
            logger.debug(f"  • {strategy_name}: DTE {min_dte}-{max_dte} (target: {target_dte})")
            
            # Fetch contracts using CACHED chain data
            contract_data = fetch_contracts_for_strategy(
                chain_data, ticker, strategy_name, trade_bias,
                min_dte, max_dte, target_dte, underlying_price,
                row_market_open
            )
            
            cache_hits += 1  # Using cached chain
            
            # Merge with original row
            result_row = row.to_dict()
            result_row.update(contract_data)
            ticker_results.append(result_row)
        
        results.extend(ticker_results)
        
        # Log ticker summary with detailed status breakdown
        available = sum(1 for r in ticker_results if r['Contract_Status'] in [CONTRACT_STATUS_OK, CONTRACT_STATUS_LEAP_FALLBACK])
        no_exp = sum(1 for r in ticker_results if r['Contract_Status'] == CONTRACT_STATUS_NO_EXPIRATIONS)
        no_calls = sum(1 for r in ticker_results if r['Contract_Status'] == CONTRACT_STATUS_NO_CALLS)
        no_puts = sum(1 for r in ticker_results if r['Contract_Status'] == CONTRACT_STATUS_NO_PUTS)
        greeks_fail = sum(1 for r in ticker_results if r['Contract_Status'] == CONTRACT_STATUS_GREEKS_FAIL)
        liquidity_fail = sum(1 for r in ticker_results if r['Contract_Status'] == CONTRACT_STATUS_LIQUIDITY_FAIL)
        leap_fallback = sum(1 for r in ticker_results if r['Contract_Status'] == CONTRACT_STATUS_LEAP_FALLBACK)
        
        status_parts = [f"{available}/{len(ticker_results)} OK"]
        if leap_fallback > 0:
            status_parts.append(f"{leap_fallback} LEAP_fallback")
        if no_exp > 0:
            status_parts.append(f"{no_exp} no_DTE")
        if no_calls > 0:
            status_parts.append(f"{no_calls} no_calls")
        if no_puts > 0:
            status_parts.append(f"{no_puts} no_puts")
        if greeks_fail > 0:
            status_parts.append(f"{greeks_fail} greeks_fail")
        if liquidity_fail > 0:
            status_parts.append(f"{liquidity_fail} liq_fail")
        
        logger.info(f"  ✅ {ticker}: {', '.join(status_parts)}")
        
        # Throttle API calls (only between tickers, not between strategies)
        if ticker_idx < total_tickers:
            time.sleep(CHAIN_FETCH_DELAY)
    
    result_df = pd.DataFrame(results)
    
    # Efficiency stats
    logger.info(f"\n⚡ Efficiency Stats:")
    logger.info(f"   API calls: {api_calls} (1 per ticker)")
    logger.info(f"   Cache reuse: {cache_hits} (strategies using cached chains)")
    logger.info(f"   Reduction: {cache_hits / api_calls:.1f}x fewer API calls")
    
    # Summary statistics by Contract_Status
    total = len(result_df)
    status_counts = result_df['Contract_Status'].value_counts()
    
    logger.info(f"\n📊 Contract Status Summary:")
    logger.info(f"   Total strategies: {total}")
    
    for status in [CONTRACT_STATUS_OK, CONTRACT_STATUS_LEAP_FALLBACK, CONTRACT_STATUS_NO_EXPIRATIONS,
                   CONTRACT_STATUS_NO_CALLS, CONTRACT_STATUS_NO_PUTS, CONTRACT_STATUS_GREEKS_FAIL,
                   CONTRACT_STATUS_IV_FAIL, CONTRACT_STATUS_LIQUIDITY_FAIL, CONTRACT_STATUS_NO_CHAIN]:
        count = status_counts.get(status, 0)
        if count > 0:
            pct = count / total * 100
            logger.info(f"   {status}: {count} ({pct:.1f}%)")
    
    # Legacy status summary (for backward compatibility)
    available = (result_df['Contract_Selection_Status'] == 'Contracts_Available').sum()
    no_chains = (result_df['Contract_Selection_Status'] == 'No_Chains_Available').sum()
    no_expirations = (result_df['Contract_Selection_Status'] == 'No_Expirations_In_DTE_Window').sum()
    
    logger.info(f"\n📊 Legacy Contract Selection Summary:")
    logger.info(f"   ✅ Contracts available: {available} ({available/total*100:.1f}%)")
    logger.info(f"   ❌ No chains: {no_chains} ({no_chains/total*100:.1f}%)")
    logger.info(f"   ⏰ No expirations in DTE window: {no_expirations} ({no_expirations/total*100:.1f}%)")
    
    # Liquidity breakdown (for available contracts)
    ok_contracts = result_df[result_df['Contract_Status'].isin([CONTRACT_STATUS_OK, CONTRACT_STATUS_LEAP_FALLBACK])]
    if len(ok_contracts) > 0:
        liquidity_counts = ok_contracts['Liquidity_Grade'].value_counts()
        logger.info(f"\n📊 Liquidity Grades (n={len(ok_contracts)}):")
        for grade in ['Excellent', 'Good', 'Acceptable', 'Thin', 'Illiquid']:
            count = liquidity_counts.get(grade, 0)
            if count > 0:
                logger.info(f"   {grade}: {count} ({count/len(ok_contracts)*100:.1f}%)")
    
    return result_df

# ============================================================
# PUBLIC API FOR PIPELINE INTEGRATION
# ============================================================

def fetch_and_select_contracts_schwab(evaluated_strategies_df: pd.DataFrame, 
                                       timeframes_df: pd.DataFrame) -> pd.DataFrame:
    """
    Pipeline-compatible wrapper for Step 9B Schwab contract fetching.
    
    This function is called by pipeline.py to integrate Step 9B into the full pipeline.
    It merges evaluated strategies with timeframes, fetches contracts via Schwab API,
    and returns the enriched DataFrame with contract details.
    
    Args:
        evaluated_strategies_df: Output from Step 11 (independent evaluation)
        timeframes_df: Output from Step 9A (timeframe assignment)
        
    Returns:
        DataFrame with contract details merged from Step 9B
        
    Integration Flow:
        Step 11 (evaluated strategies) → Step 9A (timeframes) → Step 9B (contracts)
        This wrapper merges 11+9A and passes to fetch_contracts()
    """
    logger.info("🔗 Pipeline integration: Merging Step 11 + Step 9A outputs for Step 9B...")
    
    # Merge evaluated strategies with timeframes
    # Step 11 has: Validation_Status, Theory_Compliance_Score, Execution_State, etc.
    # Step 9A has: Min_DTE, Max_DTE, Target_DTE, Timeframe_Label, etc.
    merged = evaluated_strategies_df.merge(
        timeframes_df,
        on=['Ticker', 'Strategy_Name'],  # Common join keys
        how='inner',
        suffixes=('_step11', '_step9a')
    )
    
    logger.info(f"   Merged {len(merged)} strategies (from {len(evaluated_strategies_df)} evaluated, {len(timeframes_df)} with timeframes)")
    
    if merged.empty:
        logger.warning("⚠️ No strategies after merge - check join key alignment")
        return pd.DataFrame()
    
    # Initialize Schwab client
    logger.info("🔑 Initializing Schwab API client for contract fetching...")
    client = SchwabClient()
    
    # Fetch contracts
    result_df = fetch_contracts(merged, client)
    
    logger.info(f"✅ Pipeline Step 9B complete: {len(result_df)} contracts enriched")
    
    return result_df

# ============================================================
# MAIN ENTRY POINT
# ============================================================

def main(input_csv: str = None, output_csv: str = None):
    """
    Main entry point for Step 9B.
    
    Args:
        input_csv: Path to Step 9A output (default: latest Step9A_Timeframes_*.csv)
        output_csv: Path to output file (default: output/Step9B_Contracts_YYYYMMDD_HHMMSS.csv)
    """
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    logger.info("=" * 80)
    logger.info("Step 9B: Fetch Option Contracts from Schwab API")
    logger.info("=" * 80)
    
    # Find input file
    if input_csv is None:
        output_dir = project_root / "output"
        step9a_files = sorted(output_dir.glob("Step9A_Timeframes_*.csv"))
        if not step9a_files:
            logger.error("❌ No Step 9A output files found!")
            return None
        input_csv = step9a_files[-1]
    
    logger.info(f"📂 Input: {input_csv}")
    
    # Load input data
    df = pd.read_csv(input_csv, low_memory=False)
    logger.info(f"📊 Loaded {len(df)} strategies from Step 9A")
    
    # Validate required columns
    required_cols = ['Ticker', 'Strategy_Name', 'Min_DTE', 'Max_DTE', 'Target_DTE', 'last_price']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        logger.error(f"❌ Missing required columns: {missing_cols}")
        return None
    
    # Initialize Schwab client
    logger.info("🔑 Initializing Schwab API client...")
    client = SchwabClient()
    
    # Fetch contracts
    result_df = fetch_contracts(df, client)
    
    # Determine output path
    if output_csv is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_csv = project_root / "output" / f"Step9B_Contracts_{timestamp}.csv"
    
    # Save results
    result_df.to_csv(output_csv, index=False)
    logger.info(f"\n💾 Saved: {output_csv}")
    logger.info(f"   Size: {output_csv.stat().st_size / 1024:.1f} KB")
    
    logger.info("\n" + "=" * 80)
    logger.info("✅ Step 9B Complete")
    logger.info("=" * 80)
    
    return result_df

if __name__ == '__main__':
    import sys
    
    input_file = sys.argv[1] if len(sys.argv) > 1 else None
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    main(input_file, output_file)
