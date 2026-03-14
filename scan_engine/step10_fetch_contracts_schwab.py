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
    - Contract_Status: 'OK' / 'NO_CHAIN_RETURNED' / 'NO_EXPIRATIONS_IN_WINDOW' / etc.
    - Contract_Selection_Status: 'Contracts_Available' / 'No_Chains_Available' / 'No_Expirations_In_DTE_Window'
    
    **Contract Details (when available):**
    - Selected_Expiration: Expiration date (YYYY-MM-DD)
    - Actual_DTE: Days to expiration
    - Selected_Strike: Strike price (single) or JSON array for multi-leg
    - Contract_Symbol: OCC symbol(s)
    - Option_Type: 'call' / 'put' / 'straddle' / 'strangle' / 'vertical'
    
    **Greeks (per contract):**
    - Delta, Gamma, Vega, Theta, Rho
    
    **Pricing:**
    - Bid, Ask, Mid, Last
    - Bid_Ask_Spread_Pct: (ask - bid) / mid * 100
    
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
from core.shared.data_contracts.config import PROJECT_ROOT
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from .loaders.schwab_api_client import SchwabClient
from .chain_cache import SplitChainCache
from .throttled_executor import ThrottledExecutor

logger = logging.getLogger(__name__)

# ============================================================
# MODULE-LEVEL CHAIN CACHE (survives across pipeline re-runs in same process)
#
# SplitChainCache uses two layers:
#   structural (expirations + strikes) → 24h TTL
#   quotes (Greeks, IV, bid/ask)       → 45min TTL
#
# This prevents stale IV from contaminating execution decisions
# while still caching stable geometry for the full trading day.
# ============================================================
_CHAIN_CACHE = SplitChainCache(
    cache_dir=str(PROJECT_ROOT / "data" / "chain_cache"),
    max_memory_size=300,
)

# ============================================================
# CONFIGURATION
# ============================================================

# Schwab API throttling
CHAIN_FETCH_DELAY = 0.5  # 500ms between chain fetches (2 req/sec)

# Layer 3: Parallel fetch configuration (ThrottledExecutor)
# 4 workers × 0.5s per-worker throttle = safe 2 req/sec ceiling (Schwab limit).
# SplitChainCache is thread-safe (threading.RLock) — no modification needed.
# Set to 1 to fall back to sequential mode (useful for debug/rate-limit caution).
CHAIN_FETCH_PARALLEL_WORKERS = 4       # concurrent Schwab chain fetches
CHAIN_FETCH_PARALLEL_RPS    = 2.0      # requests per second ceiling (Schwab safe limit)

# Strike selection (for single-leg strategies)
STRIKE_RANGE_PERCENT = 0.15  # Look at strikes within ±15% of current price

# Liquidity thresholds (market hours)
# NOTE: Spread thresholds are in PERCENTAGE POINTS (e.g., 3.0 = 3%)
#       to match spread_pct calculation: (ask - bid) / mid * 100
OI_EXCELLENT = 500
OI_GOOD = 200       # Aligned with management checklist: OI < 200 = thin
OI_ACCEPTABLE = 25
OI_THIN = 5  # Minimum viable OI

# Spread thresholds in percentage points (matching spread_pct calculation)
SPREAD_EXCELLENT = 3.0   # < 3% (tight spread)
SPREAD_GOOD = 5.0        # < 5% (reasonable spread)
SPREAD_ACCEPTABLE = 10.0 # < 10% (wider but tradable)
SPREAD_WIDE = 20.0       # > 20% triggers warning

# OI thresholds that can compensate for wider spreads
# High OI indicates deep liquidity even if current spread is wide
OI_COMPENSATORY_THRESHOLD = 1000  # OI above this can offset spread concerns
OI_EXCEPTIONAL = 5000             # Exceptionally liquid (any reasonable spread OK)

# Volume thresholds that indicate active trading
VOLUME_ACTIVE = 100               # Active trading today
VOLUME_HIGH = 500                 # High volume day

# Relaxed thresholds for off-hours / closed market
OI_ACCEPTABLE_OFFHOURS = 5        # Lower OI acceptable when market closed
SPREAD_ACCEPTABLE_OFFHOURS = 15.0 # Wider spread acceptable when market closed (in pct points)

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
    'PMCC': (0.25, 0.35),            # OTM short call (LEAP leg uses 0.70-0.85)
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
# BUG 3 FIX: NEAR_LEAP_FALLBACK for 180-269 DTE — qualifies as a substitute
# but does NOT have LEAP-like structural properties (Hull Ch.10: ≥270 required).
# Step 8 assigns reduced confidence for this status.
CONTRACT_STATUS_NEAR_LEAP_FALLBACK = 'NEAR_LEAP_FALLBACK'
CONTRACT_STATUS_OI_FALLBACK = 'OI_FALLBACK'

# Expiration cascade: max additional expirations to try after first liquidity failure
MAX_EXPIRATION_CASCADE = 3

# LEAP strategy names (eligible for fallback)
LEAP_STRATEGIES = {'Long Call LEAP', 'Long Put LEAP'}

# Minimum LEAP DTE threshold
LEAP_MIN_DTE = 365

# Debug logging configuration
DEBUG_CHAIN_DUMP_ENABLED = os.getenv("DEBUG_CHAIN_DUMP", "0") == "1"
DEBUG_CHAIN_DUMP_DIR = PROJECT_ROOT / "logs" / "debug_chain_dumps"

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
                                       target_dte: int, is_leap: bool, ticker: str = None) -> Tuple[Optional[str], str]:
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

            # BUG 3 FIX: Raise minimum to 270 DTE (Hull Ch.10: LEAP-like behavior requires ≥270).
            # 180-269 DTE = NEAR_LEAP_FALLBACK (reduced confidence; Step 8 will flag).
            # <180 DTE = reject (fundamentally different instrument — no LEAP properties).
            if longest_dte >= 270:
                logger.debug(f"LEAP fallback{f' ({ticker})' if ticker else ''}: requested {min_dte}-{max_dte} DTE, using longest available: {longest_dte} DTE (LEAP_FALLBACK)")
                return longest_exp, CONTRACT_STATUS_LEAP_FALLBACK
            elif longest_dte >= 180:
                logger.info(f"NEAR_LEAP_FALLBACK{f' ({ticker})' if ticker else ''}: longest available {longest_dte} DTE < 270 — Hull Ch.10: reduced LEAP-like properties. Confidence will be reduced.")
                return longest_exp, CONTRACT_STATUS_NEAR_LEAP_FALLBACK

    return None, CONTRACT_STATUS_NO_EXPIRATIONS


def _rank_cascade_expirations(
    all_expirations: List[str],
    tried: set,
    target_dte: int,
    min_dte: int,
    is_leap: bool,
) -> List[str]:
    """
    Rank untried expirations for the liquidity cascade.

    LEAPs step DOWN in DTE (nearer = more liquid).
    Non-LEAPs sort by absolute distance from target.
    Respects min_dte floor (never picks an expiration below strategy minimum).
    """
    candidates = []
    for exp in all_expirations:
        if exp in tried:
            continue
        dte = calculate_dte(exp)
        if dte < min_dte:
            continue
        candidates.append((exp, dte))

    if is_leap:
        # Nearer dates first — liquidity clusters at front cycles
        candidates.sort(key=lambda x: x[1])
    else:
        # Closest to target first
        candidates.sort(key=lambda x: abs(x[1] - target_dte))

    return [exp for exp, _ in candidates]


def _select_contract_for_strategy(
    chain_df: pd.DataFrame,
    strategy_name: str,
    underlying_price: float,
    market_open: bool,
) -> Tuple[Optional[Dict], Optional[str], str]:
    """
    Run strategy-specific strike selection on a parsed chain DataFrame.

    Returns:
        (contract_details, failure_reason, option_type)
    """
    contract_details = None
    failure_reason = None
    option_type = 'call'

    if strategy_name in ['Long Call', 'Long Call LEAP']:
        contract_details = select_long_call_contract(chain_df, underlying_price, strategy_name)
        option_type = 'call'
    elif strategy_name in ['Long Put', 'Long Put LEAP']:
        contract_details = select_long_put_contract(chain_df, underlying_price, strategy_name)
        option_type = 'put'
    elif strategy_name in ['CSP', 'Cash-Secured Put']:
        contract_details = select_csp_contract(chain_df, underlying_price)
        option_type = 'put'
    elif strategy_name in ['Covered Call', 'Buy-Write']:
        contract_details = select_covered_call_contract(chain_df, underlying_price)
        option_type = 'call'
    elif strategy_name == 'PMCC':
        # PMCC short-call leg selected here; LEAP leg handled at fetch level
        contract_details = find_strike_by_delta(chain_df, (0.25, 0.35), underlying_price, 'call')
        option_type = 'call'
    elif strategy_name == 'Straddle':
        contract_details = select_straddle_contracts(chain_df, underlying_price)
        option_type = 'straddle'
    elif strategy_name == 'Strangle':
        contract_details = select_strangle_contracts(chain_df, underlying_price)
        option_type = 'strangle'
    elif strategy_name == 'Neutral / Watch':
        contract_details = select_straddle_contracts(chain_df, underlying_price)
        option_type = 'straddle'
    elif 'Vertical' in strategy_name or 'Spread' in strategy_name:
        contract_details, failure_reason = select_vertical_spread_contracts(
            chain_df, underlying_price, strategy_name, market_open
        )
        option_type = 'vertical'
    else:
        failure_reason = f'Unknown strategy: {strategy_name}'
        option_type = ''

    return contract_details, failure_reason, option_type


def _attempt_single_expiration(
    chain_data: Dict,
    expiration: str,
    ticker: str,
    strategy_name: str,
    underlying_price: float,
    market_open: bool,
    expiry_intent: str,
    market_stress: str,
) -> Dict:
    """
    Try one expiration: parse chain → select strike → grade liquidity.

    Returns a partial result dict. Caller merges into the master result.
    The '_cascade_passed' key indicates whether liquidity was acceptable.
    The '_cascade_failure_type' key is 'oi', 'spread', or None.
    """
    attempt = {
        'Selected_Expiration': expiration,
        'Actual_DTE': calculate_dte(expiration),
        '_cascade_passed': False,
        '_cascade_failure_type': None,
    }

    chain_df = parse_schwab_chain_to_dataframe(chain_data, expiration)
    attempt['Strikes_Available'] = len(chain_df)

    if chain_df.empty:
        attempt['Contract_Status'] = CONTRACT_STATUS_NO_CHAIN
        attempt['Contract_Selection_Status'] = 'No_Chains_Available'
        attempt['Chain_Rejection_Reason'] = f'No contracts for expiration {expiration}'
        return attempt

    contract_details, failure_reason, option_type = _select_contract_for_strategy(
        chain_df, strategy_name, underlying_price, market_open,
    )
    attempt['Option_Type'] = option_type

    if not contract_details:
        attempt['Contract_Status'] = (
            CONTRACT_STATUS_NO_CALLS if 'call' in option_type
            else (CONTRACT_STATUS_NO_PUTS if 'put' in option_type else 'NO_CONTRACT_MATCH')
        )
        attempt['Contract_Selection_Status'] = 'No_Contracts_Available'
        attempt['Chain_Rejection_Reason'] = failure_reason or f'No suitable contract for {strategy_name}'
        return attempt

    # Populate contract fields
    attempt['Selected_Strike'] = contract_details['strike']
    attempt['Contract_Symbol'] = contract_details['symbol']
    attempt['Delta'] = contract_details['delta']
    attempt['Gamma'] = contract_details['gamma']
    attempt['Vega'] = contract_details['vega']
    attempt['Theta'] = contract_details['theta']
    attempt['Rho'] = contract_details.get('rho', 0)
    attempt['Bid'] = contract_details['bid']
    attempt['Ask'] = contract_details['ask']
    attempt['Mid'] = contract_details['mark']
    attempt['Last'] = contract_details['last']
    attempt['Open_Interest'] = contract_details['open_interest']
    attempt['Volume'] = contract_details['volume']
    attempt['Implied_Volatility'] = contract_details['implied_volatility']
    if 'put_call_skew' in contract_details:
        attempt['Put_Call_Skew'] = contract_details['put_call_skew']

    mid = attempt['Mid']
    if mid > 0:
        attempt['Bid_Ask_Spread_Pct'] = (attempt['Ask'] - attempt['Bid']) / mid * 100
    else:
        attempt['Bid_Ask_Spread_Pct'] = np.nan

    # Grade liquidity
    grade, score, reason = grade_liquidity(
        attempt['Bid'], attempt['Ask'], mid,
        attempt['Open_Interest'], attempt['Volume'],
        market_open=market_open, expiry_intent=expiry_intent,
        market_stress=market_stress,
    )
    attempt['Liquidity_Grade'] = grade
    attempt['Liquidity_Score'] = score
    attempt['Liquidity_Reason'] = reason
    attempt['Spread_Quality_Adjusted'] = (market_stress.upper() in ('CRISIS', 'ELEVATED'))

    if mid > 0:
        attempt['Total_Debit'] = mid

    if grade in ('Illiquid', 'Thin'):
        attempt['Contract_Status'] = CONTRACT_STATUS_LIQUIDITY_FAIL
        attempt['Contract_Selection_Status'] = 'No_Contracts_Available'
        attempt['Chain_Rejection_Reason'] = f'Failed liquidity filter: {reason}'
        attempt['_cascade_failure_type'] = 'oi' if 'OI' in reason else 'spread'
    else:
        attempt['Contract_Selection_Status'] = 'Contracts_Available'
        attempt['_cascade_passed'] = True

    return attempt


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
        valid = type_chain[(type_chain['delta'] >= min_delta) & (type_chain['delta'] <= max_delta)].copy()
    else:  # put — deltas are negative, e.g. min=-0.70, max=-0.30
        valid = type_chain[(type_chain['delta'] >= min_delta) & (type_chain['delta'] <= max_delta)].copy()
    
    if valid.empty:
        # Fallback: find closest delta to midpoint, preferring strikes with OI > 0
        mid_delta = (min_delta + max_delta) / 2
        temp_type_chain = type_chain.copy()
        temp_type_chain.loc[:, 'delta_dist'] = abs(temp_type_chain['delta'] - mid_delta)
        # Prefer strikes with OI > 0 first
        with_oi = temp_type_chain[temp_type_chain['openInterest'] > 0]
        pool = with_oi if not with_oi.empty else temp_type_chain
        best = pool.nsmallest(1, 'delta_dist')
    else:
        # Within delta range: prefer highest OI (most liquid strike), break ties by delta proximity
        mid_delta = (min_delta + max_delta) / 2
        valid.loc[:, 'delta_dist'] = abs(valid['delta'] - mid_delta)
        with_oi = valid[valid['openInterest'] > 0]
        pool = with_oi if not with_oi.empty else valid
        # Sort by OI desc, then delta_dist asc — take best
        best = pool.sort_values(['openInterest', 'delta_dist'], ascending=[False, True]).head(1)
    
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
        'implied_volatility': row.get('volatility', 0),  # Already a percentage from Schwab (e.g. 55.7 = 55.7%)
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
        'implied_volatility': row.get('volatility', 0),  # Already a percentage from Schwab (e.g. 55.7 = 55.7%)
    }

def grade_liquidity(bid: float, ask: float, mid: float, oi: int, volume: int,
                    market_open: bool = True, expiry_intent: str = 'ANY',
                    market_stress: str = 'NORMAL') -> Tuple[str, int, str]:
    """
    Grade contract liquidity with COMPENSATORY logic.

    A real trader evaluates liquidity holistically:
    - High OI can compensate for wider spreads (deep market)
    - High volume indicates active trading even with wider spreads
    - Spread alone is not a hard gate

    GAP 4 FIX: VIX-adjusted spread thresholds — Sinclair Ch.5: "Evaluate spread quality
    relative to current VIX." During CRISIS, market makers widen spreads 2-3x naturally.
    A 7% spread at VIX=40 = quality execution; same at VIX=15 = poor.
    market_stress: 'CRISIS' → thresholds ×2.0; 'ELEVATED' → ×1.5; 'NORMAL' → ×1.0

    Args:
        bid, ask, mid: Pricing
        oi: Open interest
        volume: Daily volume
        market_open: Whether market is currently open
        expiry_intent: THIS_WEEK | NEXT_WEEK | ANY
        market_stress: CRISIS | ELEVATED | NORMAL | LOW (from market_stress_detector)

    Returns:
        (grade, score, reason)
    """
    if mid == 0:
        spread_pct = 100.0
    else:
        spread_pct = (ask - bid) / mid * 100

    # GAP 4: VIX-adjusted spread multiplier (Sinclair Ch.5)
    _stress = str(market_stress or 'NORMAL').upper()
    if _stress == 'CRISIS':
        _spread_multiplier = 2.0
    elif _stress == 'ELEVATED':
        _spread_multiplier = 1.5
    else:
        _spread_multiplier = 1.0
    _spread_adjusted = _spread_multiplier != 1.0

    # Base thresholds (already in percentage points)
    # Apply VIX-adjusted multiplier to all spread thresholds (Sinclair Ch.5)
    _sp_excellent  = SPREAD_EXCELLENT  * _spread_multiplier
    _sp_good       = SPREAD_GOOD       * _spread_multiplier
    _sp_acceptable = SPREAD_ACCEPTABLE * _spread_multiplier
    _sp_wide       = SPREAD_WIDE       * _spread_multiplier

    if expiry_intent == 'THIS_WEEK':
        # Weekly options need stricter liquidity for slippage control
        oi_excellent_target = 1000
        oi_acceptable = 500
        spread_acceptable = 5.0 * _spread_multiplier  # Apply VIX adjustment here too
    else:
        oi_excellent_target = OI_EXCELLENT
        oi_acceptable = OI_ACCEPTABLE if market_open else OI_ACCEPTABLE_OFFHOURS
        spread_acceptable = (SPREAD_ACCEPTABLE if market_open else SPREAD_ACCEPTABLE_OFFHOURS) * _spread_multiplier

    # Calculate component scores (0-50 each)
    oi_score = min(50, (oi / max(oi_excellent_target, 1)) * 50)
    spread_score = max(0, 50 - (spread_pct / max(spread_acceptable, 0.1)) * 25)
    volume_score = min(20, (volume / max(VOLUME_HIGH, 1)) * 20)  # Bonus for volume

    score = int(oi_score + spread_score + volume_score)

    # ============================================================
    # COMPENSATORY GRADING LOGIC
    # High OI/volume can offset wider spreads (real trader behavior)
    # ============================================================

    # VIX adjustment note suffix
    _vix_note = f' [VIX-adj ×{_spread_multiplier:.1f}]' if _spread_adjusted else ''

    # Tier 1: Excellent - Truly exceptional liquidity
    if oi >= OI_EXCEPTIONAL and spread_pct <= _sp_acceptable:
        # Exceptional OI (5000+) makes almost any reasonable spread tradable
        grade = 'Excellent'
        reason = f'Exceptional OI ({oi:,}), spread OK ({spread_pct:.1f}%){_vix_note}'
        _spread_quality_adjusted = _spread_adjusted

    elif oi >= OI_EXCELLENT and spread_pct <= _sp_excellent:
        grade = 'Excellent'
        reason = f'Deep OI ({oi:,}), tight spread ({spread_pct:.1f}%){_vix_note}'
        _spread_quality_adjusted = _spread_adjusted

    # Tier 2: Good - Solid liquidity with compensation
    elif oi >= OI_COMPENSATORY_THRESHOLD and spread_pct <= _sp_acceptable:
        # High OI (1000+) compensates for spreads up to threshold
        grade = 'Good'
        reason = f'High OI ({oi:,}) compensates for spread ({spread_pct:.1f}%){_vix_note}'
        _spread_quality_adjusted = _spread_adjusted

    elif oi >= OI_GOOD and spread_pct <= _sp_good:
        grade = 'Good'
        reason = f'Adequate OI ({oi:,}), reasonable spread ({spread_pct:.1f}%){_vix_note}'
        _spread_quality_adjusted = _spread_adjusted

    elif oi >= OI_GOOD and volume >= VOLUME_ACTIVE and spread_pct <= _sp_acceptable:
        # Good OI + active trading compensates for wider spread
        grade = 'Good'
        reason = f'OI ({oi:,}) + volume ({volume}) offset spread ({spread_pct:.1f}%){_vix_note}'
        _spread_quality_adjusted = _spread_adjusted

    # Tier 3: Acceptable - Tradable with awareness
    elif oi >= oi_acceptable and spread_pct <= spread_acceptable:
        suffix = ' (off-hours)' if not market_open else ''
        grade = 'Acceptable'
        reason = f'Moderate OI ({oi:,}), acceptable spread ({spread_pct:.1f}%){suffix}{_vix_note}'
        _spread_quality_adjusted = _spread_adjusted

    elif oi >= OI_THIN and volume >= VOLUME_ACTIVE and spread_pct <= _sp_wide:
        # Low OI but active trading today - proceed with caution
        grade = 'Acceptable'
        reason = f'Active trading (vol={volume}) despite low OI ({oi}), spread ({spread_pct:.1f}%){_vix_note}'
        _spread_quality_adjusted = _spread_adjusted

    # Tier 4: Thin - Tradable but with significant slippage risk
    elif oi >= OI_THIN and spread_pct <= _sp_wide:
        grade = 'Thin'
        reason = f'Light OI ({oi}), wide spread ({spread_pct:.1f}%) - slippage risk{_vix_note}'
        _spread_quality_adjusted = _spread_adjusted
        logger.debug(f"   [LIQUIDITY_DIAGNOSTIC] Thin: OI={oi}, Spread={spread_pct:.2f}%")

    # Tier 5: Illiquid - Not tradable
    else:
        grade = 'Illiquid'
        _spread_quality_adjusted = False
        if oi < OI_THIN:
            reason = f'Insufficient OI ({oi} < {OI_THIN})'
        elif spread_pct > _sp_wide:
            reason = f'Spread too wide ({spread_pct:.1f}% > {_sp_wide:.1f}%{_vix_note})'
        else:
            reason = f'Very low OI ({oi}), very wide spread ({spread_pct:.1f}%)'
        logger.debug(f"   [LIQUIDITY_DIAGNOSTIC] Illiquid: OI={oi}, Spread={spread_pct:.2f}%")

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
                def _clean_greek(val):
                    """Return NaN for Schwab sentinel (-999) or missing Greeks."""
                    if val is None:
                        return np.nan
                    try:
                        v = float(val)
                    except (TypeError, ValueError):
                        return np.nan
                    return np.nan if v <= -999 or v >= 999 else v

                rows.append({
                    'strikePrice': float(strike_price),
                    'putCall': option_type.upper(),
                    'delta': _clean_greek(contract.get('delta')),
                    'gamma': _clean_greek(contract.get('gamma')),
                    'vega': _clean_greek(contract.get('vega')),
                    'theta': _clean_greek(contract.get('theta')),
                    'rho': _clean_greek(contract.get('rho')),
                    'bid': contract.get('bid', 0),
                    'ask': contract.get('ask', 0),
                    'last': contract.get('last', 0),
                    'mark': contract.get('mark', 0),
                    'bidSize': contract.get('bidSize', 0),
                    'askSize': contract.get('askSize', 0),
                    'totalVolume': contract.get('totalVolume', 0),
                    'openInterest': contract.get('openInterest', 0),
                    'volatility': _clean_greek(contract.get('volatility')),
                    'symbol': contract.get('symbol', ''),
                })
    
    return pd.DataFrame(rows)

def get_chain_summary(chain_data: Dict) -> str:
    """Generates a concise summary of the option chain data."""
    if not chain_data:
        return "Empty chain data"

    call_exp_map = chain_data.get('callExpDateMap', {})
    put_exp_map = chain_data.get('putExpDateMap', {})

    all_expirations = set()
    total_strikes = 0
    total_contracts = 0

    for exp_map in [call_exp_map, put_exp_map]:
        for exp_key, strikes_data in exp_map.items():
            all_expirations.add(exp_key.split(':')[0]) # Add unique expiration date
            total_strikes += len(strikes_data)
            for strike_price, contracts in strikes_data.items():
                total_contracts += len(contracts)
    
    num_expirations = len(all_expirations)
    
    summary_parts = [
        f"Expirations: {num_expirations}",
        f"Strikes: {total_strikes}",
        f"Contracts: {total_contracts}"
    ]
    
    # Optionally, add first N strikes for a sample
    sample_strikes = []
    for exp_map in [call_exp_map, put_exp_map]:
        for exp_key, strikes_data in exp_map.items():
            sorted_strikes = sorted([float(s) for s in strikes_data.keys()])
            if sorted_strikes:
                sample_strikes.extend(sorted_strikes[:3]) # Take first 3 strikes from each expiration
                break # Only take from first expiration found for brevity
        if sample_strikes:
            break
            
    if sample_strikes:
        summary_parts.append(f"Sample Strikes: {sorted(list(set(sample_strikes)))[:5]}") # Show up to 5 unique sample strikes

    return ", ".join(summary_parts)


# ============================================================
# STRATEGY-SPECIFIC CONTRACT SELECTION
# ============================================================

def fetch_contracts_for_strategy(
    chain_data: Dict,
    ticker: str,
    strategy_name: str,
    trade_bias: str,
    min_dte: int,
    max_dte: int,
    target_dte: int,
    underlying_price: float,
    market_open: bool,
    expiry_intent: str = 'ANY',
    market_stress: str = 'NORMAL'  # GAP 4: VIX-adjusted spread thresholds
) -> Dict:
    """
    Fetches and selects a single option contract (or combination for spreads)
    for a given strategy from the raw Schwab chain data.

    Includes OI-aware expiration cascade: if the best-fit expiration fails
    liquidity, tries up to MAX_EXPIRATION_CASCADE alternative expirations
    (already fetched) before giving up.
    """
    result = {
        'Contract_Status': CONTRACT_STATUS_NO_CHAIN,
        'Contract_Selection_Status': 'No_Chains_Available',
        'Chain_Rejection_Reason': 'No chain data provided' if not chain_data else None,
        'Selected_Expiration': np.nan,
        'Actual_DTE': np.nan,
        'Selected_Strike': np.nan,
        'Contract_Symbol': np.nan,
        'Option_Type': np.nan,
        'Delta': np.nan, 'Gamma': np.nan, 'Vega': np.nan, 'Theta': np.nan, 'Rho': np.nan,
        'Bid': np.nan, 'Ask': np.nan, 'Mid': np.nan, 'Last': np.nan,
        'Bid_Ask_Spread_Pct': np.nan,
        'Open_Interest': np.nan, 'Volume': np.nan,
        'Liquidity_Grade': np.nan, 'Liquidity_Score': np.nan, 'Liquidity_Reason': np.nan,
        'Expirations_Checked': 0,
        'Strikes_Available': 0,
        'Total_Debit': np.nan,  # For spreads
        'Expiration_Fallback_Used': False,
        'Expiration_Attempts': 0,
    }

    if not chain_data:
        return result

    # Extract available expirations
    all_expirations = set()
    if 'callExpDateMap' in chain_data:
        for exp_key in chain_data['callExpDateMap'].keys():
            all_expirations.add(exp_key.split(':')[0])
    if 'putExpDateMap' in chain_data:
        for exp_key in chain_data['putExpDateMap'].keys():
            all_expirations.add(exp_key.split(':')[0])

    all_expirations = sorted(list(all_expirations))
    result['Expirations_Checked'] = len(all_expirations)

    if not all_expirations:
        result['Contract_Status'] = CONTRACT_STATUS_NO_EXPIRATIONS
        result['Contract_Selection_Status'] = 'No_Expirations_In_DTE_Window'
        result['Chain_Rejection_Reason'] = 'No expirations found in chain data'
        return result

    is_leap_strategy = strategy_name in LEAP_STRATEGIES
    selected_expiration, exp_status = find_best_expiration_with_fallback(
        all_expirations, min_dte, max_dte, target_dte, is_leap_strategy, ticker=ticker
    )

    if not selected_expiration:
        result['Contract_Status'] = exp_status  # Will be NO_EXPIRATIONS
        result['Contract_Selection_Status'] = 'No_Expirations_In_DTE_WINDOW'
        result['Chain_Rejection_Reason'] = f'No expirations in DTE window {min_dte}-{max_dte}'
        return result

    # ── Expiration cascade ───────────────────────────────────────────
    # Try the best-fit expiration first.  If it fails liquidity, cascade
    # through alternative expirations (already cached — no new API calls).
    # LEAPs: floor at 180 DTE so cascade doesn't degrade to short-dated.
    # Non-LEAPs: floor at min_dte from Step 9A.
    cascade_min_dte = 180 if is_leap_strategy else max(min_dte, 20)

    tried = set()
    expirations_to_try = [selected_expiration]
    last_attempt = None
    cascade_used = False

    for attempt_idx, exp_to_try in enumerate(expirations_to_try):
        tried.add(exp_to_try)

        attempt = _attempt_single_expiration(
            chain_data, exp_to_try, ticker, strategy_name,
            underlying_price, market_open, expiry_intent, market_stress,
        )

        last_attempt = attempt

        if attempt['_cascade_passed']:
            # Success — merge into result
            if attempt_idx > 0:
                cascade_used = True
                logger.info(
                    f"  🔄 {ticker} {strategy_name}: OI cascade — "
                    f"found liquid contract at {exp_to_try} "
                    f"(attempt {attempt_idx + 1}, DTE={attempt['Actual_DTE']})"
                )
            else:
                logger.debug(
                    f"  ✅ {ticker} {strategy_name}: Contract selected and passed liquidity."
                )
            break

        # First failure — lazily compute alternatives for cascade
        if attempt_idx == 0:
            alternatives = _rank_cascade_expirations(
                all_expirations, tried, target_dte,
                cascade_min_dte, is_leap_strategy,
            )
            expirations_to_try.extend(alternatives[:MAX_EXPIRATION_CASCADE])
            logger.debug(
                f"  🔄 {ticker} {strategy_name}: Liquidity failed at {exp_to_try} "
                f"({attempt.get('_cascade_failure_type', '?')}), "
                f"cascading through {len(alternatives[:MAX_EXPIRATION_CASCADE])} alternatives"
            )
        else:
            logger.debug(
                f"  🔄 {ticker} {strategy_name}: Cascade attempt {attempt_idx + 1} "
                f"failed at {exp_to_try} ({attempt.get('_cascade_failure_type', '?')})"
            )

    # ── Merge winning (or last failed) attempt into result ───────────
    # Copy all non-internal keys from the attempt
    for k, v in last_attempt.items():
        if not k.startswith('_cascade'):
            result[k] = v

    result['Expiration_Attempts'] = len(tried)
    result['Expiration_Fallback_Used'] = cascade_used

    if last_attempt['_cascade_passed']:
        # Preserve original exp_status (OK / LEAP_FALLBACK) unless cascade was used
        if cascade_used:
            result['Contract_Status'] = CONTRACT_STATUS_OI_FALLBACK
        else:
            result['Contract_Status'] = exp_status
    else:
        # All attempts failed — keep the last failure status
        logger.debug(
            f"  ❌ {ticker} {strategy_name}: Cascade exhausted "
            f"({len(tried)} expirations tried), final reject: "
            f"{result.get('Chain_Rejection_Reason', '?')}"
        )

    # ── PMCC: attach LEAP leg from a separate expiration ──────────
    if strategy_name == 'PMCC' and last_attempt.get('_cascade_passed'):
        result = _enrich_pmcc_leap_leg(chain_data, result, underlying_price, ticker)

    return result


def _enrich_pmcc_leap_leg(
    chain_data: Dict,
    result: Dict,
    underlying_price: float,
    ticker: str,
) -> Dict:
    """
    Find the LEAP long-call leg for PMCC from the full chain data.

    Searches expirations ≥270 DTE for a deep-ITM call (delta 0.70-0.85).
    If found, stores LEAP details in PMCC_LEAP_* columns alongside the
    short-call leg already in the standard contract columns.
    """
    PMCC_LEAP_MIN_DTE = 270
    PMCC_LEAP_DELTA = (0.70, 0.85)

    # Collect LEAP-eligible expirations from the call chain
    leap_expirations = []
    if 'callExpDateMap' in chain_data:
        for exp_key in chain_data['callExpDateMap']:
            exp_date = exp_key.split(':')[0]
            dte = calculate_dte(exp_date)
            if dte >= PMCC_LEAP_MIN_DTE:
                leap_expirations.append((exp_date, dte))

    if not leap_expirations:
        result['PMCC_LEAP_Status'] = 'NO_LEAP_EXPIRATION'
        logger.debug(f"  PMCC {ticker}: No expirations with DTE≥{PMCC_LEAP_MIN_DTE}")
        return result

    # Sort by DTE — prefer closest to 365 DTE (sweet spot for PMCC LEAPs)
    leap_expirations.sort(key=lambda x: abs(x[1] - 365))

    for exp_date, dte in leap_expirations:
        chain_df = parse_schwab_chain_to_dataframe(chain_data, exp_date)
        if chain_df.empty:
            continue

        leap_contract = find_strike_by_delta(chain_df, PMCC_LEAP_DELTA, underlying_price, 'call')
        if not leap_contract:
            continue

        # Verify LEAP is deep enough ITM: strike should be below underlying
        if leap_contract['strike'] >= underlying_price:
            continue

        mid = leap_contract['mark']
        if mid <= 0:
            continue

        spread_pct = (leap_contract['ask'] - leap_contract['bid']) / mid * 100 if mid > 0 else 999
        if leap_contract['open_interest'] < 10:
            continue  # Need some liquidity on the LEAP

        result['PMCC_LEAP_Status'] = 'OK'
        result['PMCC_LEAP_Expiration'] = exp_date
        result['PMCC_LEAP_DTE'] = dte
        result['PMCC_LEAP_Strike'] = leap_contract['strike']
        result['PMCC_LEAP_Symbol'] = leap_contract['symbol']
        result['PMCC_LEAP_Delta'] = leap_contract['delta']
        result['PMCC_LEAP_Gamma'] = leap_contract['gamma']
        result['PMCC_LEAP_Vega'] = leap_contract['vega']
        result['PMCC_LEAP_Theta'] = leap_contract['theta']
        result['PMCC_LEAP_Bid'] = leap_contract['bid']
        result['PMCC_LEAP_Ask'] = leap_contract['ask']
        result['PMCC_LEAP_Mid'] = mid
        result['PMCC_LEAP_OI'] = leap_contract['open_interest']
        result['PMCC_LEAP_Spread_Pct'] = spread_pct

        # Combined PMCC cost = LEAP debit - short call credit
        short_mid = result.get('Mid', 0) or 0
        result['PMCC_Net_Debit'] = (mid - short_mid) * 100  # per-contract
        result['PMCC_Max_Loss'] = mid * 100  # LEAP premium is max loss if short expires worthless

        # Reclassify option type to diagonal
        result['Option_Type'] = 'pmcc'
        result['Contract_Symbol'] = json.dumps([
            leap_contract['symbol'],
            result.get('Contract_Symbol', ''),
        ])
        result['Selected_Strike'] = json.dumps([
            leap_contract['strike'],
            result.get('Selected_Strike', 0),
        ])

        logger.info(
            f"  PMCC {ticker}: LEAP {exp_date} (DTE={dte}) "
            f"strike={leap_contract['strike']:.2f} delta={leap_contract['delta']:.2f} "
            f"mid=${mid:.2f} | short-call mid=${short_mid:.2f} | "
            f"net debit=${result['PMCC_Net_Debit']:,.0f}"
        )
        return result

    result['PMCC_LEAP_Status'] = 'NO_LIQUID_LEAP'
    logger.debug(f"  PMCC {ticker}: No liquid deep-ITM LEAP found across {len(leap_expirations)} expirations")
    return result


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

def select_csp_contract(chain_df: pd.DataFrame, underlying_price: float, support_level: Optional[float] = None) -> Optional[Dict]:
    """
    Select contract for Cash-Secured Put.
    Prioritizes strikes near a support level if provided, within the delta range.
    """
    delta_range = (-0.30, -0.15)
    
    # First, filter by delta range
    filtered_by_delta = find_strike_by_delta(chain_df, delta_range, underlying_price, 'put')
    
    if not filtered_by_delta:
        return None
    
    # If a support level is provided, prioritize strikes closest to it
    if support_level is not None:
        # Convert filtered_by_delta (which is a single dict) to a DataFrame for easier manipulation
        temp_df = pd.DataFrame([filtered_by_delta])
        
        temp_df['strike_dist_to_support'] = abs(temp_df['strike'] - support_level)
        best_strike = temp_df.nsmallest(1, 'strike_dist_to_support').iloc[0]
        
        # Return the best strike as a dictionary
        return best_strike.to_dict()
    
    return filtered_by_delta

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
        'delta': np.nansum([call_contract['delta'], put_contract['delta']]),
        'gamma': np.nansum([call_contract['gamma'], put_contract['gamma']]),
        'vega': np.nansum([call_contract['vega'], put_contract['vega']]),
        'theta': np.nansum([call_contract['theta'], put_contract['theta']]),
        'rho': np.nansum([call_contract.get('rho', 0), put_contract.get('rho', 0)]),
        'bid': call_contract['bid'] + put_contract['bid'],
        'ask': call_contract['ask'] + put_contract['ask'],
        'last': call_contract['last'] + put_contract['last'],
        'mark': call_contract['mark'] + put_contract['mark'],
        'volume': call_contract['volume'] + put_contract['volume'],
        'open_interest': call_contract['open_interest'] + put_contract['open_interest'],
        'implied_volatility': np.nanmean([call_contract['implied_volatility'], put_contract['implied_volatility']]),
        'put_call_skew': put_contract['implied_volatility'] / call_contract['implied_volatility'] if call_contract.get('implied_volatility', 0) > 0 else np.nan,
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
        'delta': np.nansum([call_contract['delta'], put_contract['delta']]),
        'gamma': np.nansum([call_contract['gamma'], put_contract['gamma']]),
        'vega': np.nansum([call_contract['vega'], put_contract['vega']]),
        'theta': np.nansum([call_contract['theta'], put_contract['theta']]),
        'rho': np.nansum([call_contract.get('rho', 0), put_contract.get('rho', 0)]),
        'bid': call_contract['bid'] + put_contract['bid'],
        'ask': call_contract['ask'] + put_contract['ask'],
        'last': call_contract['last'] + put_contract['last'],
        'mark': call_contract['mark'] + put_contract['mark'],
        'volume': call_contract['volume'] + put_contract['volume'],
        'open_interest': call_contract['open_interest'] + put_contract['open_interest'],
        'implied_volatility': np.nanmean([call_contract['implied_volatility'], put_contract['implied_volatility']]),
        'put_call_skew': put_contract['implied_volatility'] / call_contract['implied_volatility'] if call_contract.get('implied_volatility', 0) > 0 else np.nan,
        'option_type': 'strangle',
    }

def select_vertical_spread_contracts(chain_df: pd.DataFrame, underlying_price: float, 
                                     strategy_name: str, market_open: bool) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Select contracts for Vertical Spreads (Debit/Credit).
    
    Follows Passarelli Doctrine: Each leg must justify itself.
    
    Returns:
        (contract_dict, failure_reason)
    """
    is_call = 'Call' in strategy_name
    is_debit = 'Debit' in strategy_name
    
    option_type = 'call' if is_call else 'put'
    
    # 1. Select Legs
    if is_debit:
        # Long leg: Target delta ~0.60 for calls, ~-0.60 for puts
        long_target = (0.55, 0.65) if is_call else (-0.65, -0.55)
        long_leg = find_strike_by_delta(chain_df, long_target, underlying_price, option_type)
        
        if not long_leg: return None, "Could not find suitable long leg"
        
        # Short leg: Target delta ~0.40 for calls, ~-0.40 for puts (OTM)
        short_target = (0.35, 0.45) if is_call else (-0.45, -0.35)
        short_leg = find_strike_by_delta(chain_df, short_target, underlying_price, option_type)
        if not short_leg: return None, "Could not find suitable short leg"
    else:
        # Credit Spreads (e.g. Bull Put Spread)
        # Short leg: Target delta ~0.30
        short_target = (0.25, 0.35) if is_call else (-0.35, -0.25)
        short_leg = find_strike_by_delta(chain_df, short_target, underlying_price, option_type)
        
        if not short_leg: return None, "Could not find suitable short leg"
        
        # Long leg (Protection): Target delta ~0.15
        long_target = (0.10, 0.20) if is_call else (-0.20, -0.10)
        long_leg = find_strike_by_delta(chain_df, long_target, underlying_price, option_type)
        if not long_leg: return None, "Could not find suitable long leg"

    # Ensure they are different strikes
    if long_leg['strike'] == short_leg['strike']:
        return None, "Long and short legs selected same strike"

    # Validate each leg independently (Passarelli Doctrine)
    for leg_name, leg in [("Long", long_leg), ("Short", short_leg)]:
        grade, _, reason = grade_liquidity(
            leg['bid'], leg['ask'], leg['mark'],
            leg['open_interest'], leg['volume'],
            market_open=market_open
        )
        if grade == 'Illiquid':
            return None, f"{leg_name} leg failed liquidity: {reason}"

    # Aggregate pricing
    if is_debit:
        net_price = long_leg['mark'] - short_leg['mark']
    else:
        net_price = short_leg['mark'] - long_leg['mark']
        
    if net_price <= 0: 
        return None, f"Invalid net price for spread: {net_price}"

    # Combine details
    return {
        'strike': json.dumps([long_leg['strike'], short_leg['strike']]),
        'symbol': json.dumps([long_leg['symbol'], short_leg['symbol']]),
        'delta': long_leg['delta'] - short_leg['delta'] if is_debit else short_leg['delta'] - long_leg['delta'],
        'gamma': long_leg['gamma'] - short_leg['gamma'],
        'vega': long_leg['vega'] - short_leg['vega'],
        'theta': long_leg['theta'] - short_leg['theta'],
        'rho': long_leg.get('rho', 0) - short_leg.get('rho', 0),
        'bid': long_leg['bid'] - short_leg['ask'] if is_debit else short_leg['bid'] - long_leg['ask'],
        'ask': long_leg['ask'] - short_leg['bid'] if is_debit else short_leg['ask'] - long_leg['bid'],
        'last': long_leg['last'] - short_leg['last'],
        'mark': net_price,
        'volume': min(long_leg['volume'], short_leg['volume']),
        'open_interest': min(long_leg['open_interest'], short_leg['open_interest']),
        'implied_volatility': (long_leg['implied_volatility'] + short_leg['implied_volatility']) / 2,
        'option_type': 'vertical',
        'long_leg': long_leg,
        'short_leg': short_leg
    }, None

# ============================================================
# MAIN CONTRACT FETCHING LOGIC
# ============================================================

def _fetch_ticker_group(
    args: tuple,
) -> list:
    """
    Fetch chain for one ticker and process all its strategy rows.
    Designed as the parallel work unit for ThrottledExecutor.map_parallel.

    Args:
        args: (ticker, group_df, client, expiry_intent, market_open)

    Returns:
        List of result dicts (one per strategy row for this ticker).
    """
    ticker, group, client, expiry_intent, market_open = args

    # ── 1. Fetch chain once per ticker ──────────────────────────────────────
    try:
        chain_data = _CHAIN_CACHE.get(ticker)
        if chain_data is None:
            chain_data = client.get_chains(
                symbol=ticker,
                strikeCount=50,
                range='ALL',
                strategy='SINGLE'
            )
            if chain_data and 'callExpDateMap' in chain_data:
                _CHAIN_CACHE.set(ticker, chain_data)
                logger.debug(f"  💾 {ticker}: Chain cached ({len(chain_data.get('callExpDateMap', {}))} expirations)")
            else:
                logger.warning(f"  ⚠️  {ticker}: No chains returned from Schwab or missing 'callExpDateMap'.")
                chain_data = {}
        else:
            logger.debug(f"  ✅ {ticker}: Chain loaded from cache")
    except Exception as e:
        logger.error(f"  ❌ {ticker}: Chain fetch error: {e}", exc_info=True)
        chain_data = {}

    # ── 2. Process all strategies using cached chain ─────────────────────────
    ticker_results = []
    for _, row in group.iterrows():
        strategy_name   = row['Strategy_Name']
        trade_bias      = row.get('Trade_Bias', 'Neutral')
        try:
            min_dte    = int(row['Min_DTE'])
            max_dte    = int(row['Max_DTE'])
            target_dte = int(row['Target_DTE'])
        except (ValueError, TypeError):
            logger.warning(f"  ⚠️  {ticker}/{strategy_name}: NaN DTE values — skipping row")
            result_row = row.to_dict()
            result_row['Contract_Status'] = 'SKIPPED_NAN_DTE'
            result_row['Chain_Fetch_Parallel'] = True
            ticker_results.append(result_row)
            continue
        underlying_price = float(row['last_price'])
        row_market_open = row.get('is_market_open', market_open)
        _row_market_stress = str(row.get('market_stress') or row.get('Market_Stress') or 'NORMAL')

        contract_data = fetch_contracts_for_strategy(
            chain_data, ticker, strategy_name, trade_bias,
            min_dte, max_dte, target_dte, underlying_price,
            row_market_open, expiry_intent=expiry_intent,
            market_stress=_row_market_stress
        )
        result_row = row.to_dict()
        result_row.update(contract_data)
        result_row['Chain_Fetch_Parallel'] = True
        ticker_results.append(result_row)

    # ── 3. Log ticker summary ────────────────────────────────────────────────
    available   = sum(1 for r in ticker_results if r['Contract_Status'] in [
        CONTRACT_STATUS_OK, CONTRACT_STATUS_LEAP_FALLBACK, CONTRACT_STATUS_NEAR_LEAP_FALLBACK])
    leap_fb     = sum(1 for r in ticker_results if r['Contract_Status'] == CONTRACT_STATUS_LEAP_FALLBACK)
    logger.info(f"  ✅ {ticker}: {available}/{len(ticker_results)} OK" +
                (f", {leap_fb} LEAP_fallback" if leap_fb else ""))

    return ticker_results


def fetch_contracts(df: pd.DataFrame, client: SchwabClient, expiry_intent: str = 'ANY') -> pd.DataFrame:
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

    # Group by ticker — each ticker fetches its chain once and reuses for all strategies.
    # This is the unit of work for the parallel executor.
    ticker_groups = df.groupby('Ticker', sort=False)
    total_tickers = len(ticker_groups)
    logger.info(f"📊 {total_tickers} unique tickers → {len(df)} strategy rows")

    # Layer 3: Parallel chain fetch via ThrottledExecutor
    # 4 workers at 2 req/sec = safe within Schwab's ceiling.
    # SplitChainCache uses threading.RLock → thread-safe, no changes needed.
    # Each work item is (ticker, group_df, client, expiry_intent, market_open).
    work_items = [
        (ticker, group, client, expiry_intent, market_open)
        for ticker, group in ticker_groups
    ]

    executor = ThrottledExecutor(
        max_workers=CHAIN_FETCH_PARALLEL_WORKERS,
        requests_per_second=CHAIN_FETCH_PARALLEL_RPS,
        timeout_seconds=45.0,  # per-ticker timeout (generous for slow chains)
    )
    logger.info(f"⚡ Parallel fetch: {CHAIN_FETCH_PARALLEL_WORKERS} workers, "
                f"{CHAIN_FETCH_PARALLEL_RPS:.1f} req/sec ceiling")

    parallel_results = executor.map_parallel(
        _fetch_ticker_group,
        work_items,
        desc=f"Step 9B parallel chain fetch ({total_tickers} tickers)",
    )
    executor.shutdown(wait=False)

    # Flatten list-of-lists → flat list of result dicts
    results = []
    for ticker_rows in parallel_results:
        if isinstance(ticker_rows, list):
            results.extend(ticker_rows)
        elif isinstance(ticker_rows, dict):
            # Error result from ThrottledExecutor — wrap as single empty row
            results.append(ticker_rows)

    result_df = pd.DataFrame(results)

    # Calculate Data_Completeness_Overall based on critical fields
    # RAG Source: scan_engine/step12_acceptance.py:288-291 (R0.1 gate requirement)
    # Critical fields: price, basic Greeks (Delta, Gamma, Vega, Theta)
    def assess_data_completeness(row):
        """Determine if row has complete foundational data for execution gate."""
        # Check price data
        has_price = pd.notna(row.get('last_price')) and row.get('last_price', 0) > 0

        # Check basic Greeks (required for risk assessment)
        # Reject Schwab sentinels (-999) and implausible values
        def _valid_greek(v):
            return pd.notna(v) and -998 < float(v) < 998
        has_greeks = all(_valid_greek(row.get(greek)) for greek in ['Delta', 'Gamma', 'Vega', 'Theta'])

        # Check contract details
        has_contract = pd.notna(row.get('Contract_Symbol')) and pd.notna(row.get('Mid'))

        if has_price and has_greeks and has_contract:
            return 'Complete'
        elif has_price:  # Has price but missing some Greeks or contract data
            return 'Partial'
        else:
            return 'Missing'

    result_df['Data_Completeness_Overall'] = result_df.apply(assess_data_completeness, axis=1)

    logger.info(f"📊 Data Completeness Assessment:")
    completeness_counts = result_df['Data_Completeness_Overall'].value_counts()
    for status in ['Complete', 'Partial', 'Missing']:
        count = completeness_counts.get(status, 0)
        if count > 0:
            logger.info(f"   {status}: {count} ({count/len(result_df)*100:.1f}%)")

    # Efficiency stats — parallel path: derive from executor stats + cache stats
    cache_stats = _CHAIN_CACHE.stats()
    cache_hits = cache_stats.get('memory_hits', 0) + cache_stats.get('disk_hits', 0)
    api_calls  = total_tickers - cache_hits  # remainder were live fetches
    api_calls  = max(api_calls, 0)
    total_tickers_processed = total_tickers
    logger.info(f"\n⚡ Efficiency Stats (parallel):")
    logger.info(f"   API calls (live fetch): {api_calls}")
    logger.info(f"   Cache hits (memory+disk): {cache_hits}")
    if total_tickers_processed > 0:
        logger.info(f"   Cache hit rate: {cache_hits / total_tickers_processed:.0%}")
    logger.info(f"   Cache state: memory={cache_stats.get('total_memory', cache_stats.get('memory_size', '?'))} disk={cache_stats.get('total_disk_files', cache_stats.get('disk_files', '?'))} files")
    
    # Ensure Contract_Status column exists (error dicts from failed workers may omit it)
    if 'Contract_Status' not in result_df.columns:
        result_df['Contract_Status'] = 'WORKER_ERROR'

    # Summary statistics by Contract_Status
    total = len(result_df)
    status_counts = result_df['Contract_Status'].value_counts()
    
    logger.info(f"\n📊 Contract Status Summary:")
    logger.info(f"   Total strategies: {total}")
    
    for status in [CONTRACT_STATUS_OK, CONTRACT_STATUS_OI_FALLBACK, CONTRACT_STATUS_LEAP_FALLBACK, CONTRACT_STATUS_NEAR_LEAP_FALLBACK,
                   CONTRACT_STATUS_NO_EXPIRATIONS,
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
    ok_contracts = result_df[result_df['Contract_Status'].isin([CONTRACT_STATUS_OK, CONTRACT_STATUS_OI_FALLBACK, CONTRACT_STATUS_LEAP_FALLBACK, CONTRACT_STATUS_NEAR_LEAP_FALLBACK])]
    if len(ok_contracts) > 0:
        liquidity_counts = ok_contracts['Liquidity_Grade'].value_counts()
        logger.info(f"\n📊 Liquidity Grades (n={len(ok_contracts)}):")
        for grade in ['Excellent', 'Good', 'Acceptable', 'Thin', 'Illiquid']:
            count = liquidity_counts.get(grade, 0)
            if count > 0:
                logger.info(f"   {grade}: {count} ({count/len(ok_contracts)*100:.1f}%)")
    
    # ====================
    # ENTRY QUALITY ENRICHMENT - Phase 2: Execution Quality + Dividend Risk
    # ====================
    # Add bid/ask depth, execution quality grades, and dividend assignment risk
    # These are DESCRIPTIVE facts about trade execution conditions, not decisions
    try:
        from .loaders.entry_quality_enhancements import enrich_contracts_with_execution_quality
        # Add dividend date/yield from underlying snapshot to contract rows
        # (These fields come from Step 2 snapshot and propagate through Step 7/9A)
        result_df = enrich_contracts_with_execution_quality(result_df)
        logger.info("✅ Phase 2 enrichment: Execution quality + dividend risk added")
    except Exception as e:
        logger.warning(f"⚠️ Execution quality enrichment failed (non-critical): {e}")
    
    # Legacy Validation_Status logic removed - Step 6/7 refactor uses EXECUTABLE instead
    # EXECUTABLE is set by Step 6/7 and controls whether strategy proceeds to contract fetch

    # ============================================================
    # SCHEMA NORMALIZATION LAYER
    # ============================================================
    # Normalize Schwab raw schema → Canonical schema
    # This prevents downstream fragility in Step 12+
    #
    # Column Lineage Fix:
    # Schwab API: strikePrice → contract_details['strike'] → Selected_Strike
    # Canonical: Selected_Strike (already correct, keep as-is)
    # Greeks: delta/gamma/vega/theta → Delta/Gamma/Vega/Theta
    # ============================================================

    # Rename lowercase/raw columns to canonical PascalCase
    result_df.rename(columns={
        'strike': 'Strike',  # Only for direct strike columns (not Selected_Strike)
        'bid': 'Bid',
        'ask': 'Ask',
        'last': 'Last',
        'delta': 'Delta',
        'gamma': 'Gamma',
        'vega': 'Vega',
        'theta': 'Theta',
        'rho': 'Rho',
        'implied_volatility': 'IV',
        'volume': 'Volume',
        'open_interest': 'Open_Interest',
        'Mid': 'Mid_Price'  # Rename Mid → Mid_Price for consistency
    }, inplace=True, errors='ignore')

    # Compute Mid_Price if not already set and Bid/Ask exist
    if 'Mid_Price' not in result_df.columns or result_df['Mid_Price'].isna().all():
        if 'Bid' in result_df.columns and 'Ask' in result_df.columns:
            result_df['Mid_Price'] = (result_df['Bid'] + result_df['Ask']) / 2

    # Ensure Strike column exists (copy from Selected_Strike if needed)
    if 'Strike' not in result_df.columns and 'Selected_Strike' in result_df.columns:
        result_df['Strike'] = result_df['Selected_Strike']

    # Ensure required fields exist (even if NaN) to prevent downstream KeyErrors
    required_fields = ['Strike', 'Selected_Strike', 'Mid_Price', 'Delta', 'Gamma', 'Vega', 'Theta', 'Bid', 'Ask']
    for col in required_fields:
        if col not in result_df.columns:
            result_df[col] = None

    # Ensure Scraper_Status exists and is valid for Step 12
    # Step 12 requires this field for execution gating
    if 'Scraper_Status' not in result_df.columns:
        result_df['Scraper_Status'] = 'OK'
    else:
        result_df['Scraper_Status'] = result_df['Scraper_Status'].fillna('OK')

    logger.info(f"✅ Schema normalized: {len(result_df)} contracts with canonical columns")

    return result_df

# ============================================================
# PUBLIC API FOR PIPELINE INTEGRATION
# ============================================================

def fetch_and_select_contracts_schwab(evaluated_strategies_df: pd.DataFrame, 
                                       timeframes_df: pd.DataFrame,
                                       expiry_intent: str = 'ANY') -> pd.DataFrame:
    """
    Pipeline-compatible wrapper for Step 9B Schwab contract fetching.
    
    If authentication is missing or expired, this step will be skipped gracefully,
    marking all contracts as AUTH_BLOCKED.
    
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
    # Step 7 has: EXECUTABLE, last_price, Expression_Tier, etc.
    # Step 9A has: Min_DTE, Max_DTE, Target_DTE, Timeframe_Label, etc.

    # ============================================================
    # LEFT-AUTHORITATIVE MERGE PATTERN
    # ============================================================
    # evaluated_strategies_df (Step 7) is the authoritative source for:
    # - EXECUTABLE (boolean gate for contract fetching)
    # - Expression_Tier, last_price, strategy parameters
    #
    # timeframes_df (Step 9A) provides:
    # - Min_DTE, Max_DTE, Target_DTE, Timeframe_Label
    #
    # If a column exists in both, LEFT (evaluated_strategies_df) wins.
    # Explicitly exclude protected columns from right DataFrame before merge.
    # ============================================================

    # ============================================================
    # COLUMN-SAFE MERGE: Explicitly define required timeframe fields only
    # ============================================================
    # DO NOT merge entire timeframes DataFrame - only required timeframe columns
    # This prevents column collisions and preserves Step 7 authoritative fields

    timeframe_cols = [
        "Ticker",
        "Strategy_Name",
        "Min_DTE",
        "Max_DTE",
        "Target_DTE",
        "Timeframe_Label"
    ]

    # Add optional columns if they exist in timeframes
    optional_timeframe_cols = ['DTE_Rationale', 'Expiration_Count_Target']
    for col in optional_timeframe_cols:
        if col in timeframes_df.columns:
            timeframe_cols.append(col)

    # Ensure thesis is preserved if it's in timeframes but not in evaluated
    if 'thesis' in timeframes_df.columns and 'thesis' not in evaluated_strategies_df.columns:
        timeframe_cols.append('thesis')

    # If evaluated_strategies_df already contains all required DTE columns (e.g. when the
    # same pre-screened df is passed as both args), skip the merge to avoid pandas producing
    # Min_DTE_x / Min_DTE_y suffixed columns that break the per-row worker.
    dte_required = {"Min_DTE", "Max_DTE", "Target_DTE", "Timeframe_Label"}
    if dte_required.issubset(set(evaluated_strategies_df.columns)):
        merged = evaluated_strategies_df.copy()
        logger.info(f"   DTE columns already present in evaluated_strategies_df — skipping merge "
                    f"({len(merged)} strategies)")
    else:
        # Build clean subset with only required columns
        timeframes_subset = timeframes_df[timeframe_cols].copy()

        # Perform safe LEFT merge (preserves all strategies from Step 7)
        # LEFT join ensures EXECUTABLE from evaluated_strategies_df is never dropped
        merged = evaluated_strategies_df.merge(
            timeframes_subset,
            on=['Ticker', 'Strategy_Name'],
            how='left',
            validate='one_to_one'
        )

        logger.info(f"   Merged {len(merged)} strategies (from {len(evaluated_strategies_df)} evaluated, {len(timeframes_subset)} with timeframes)")

    if merged.empty:
        logger.warning("⚠️ No strategies after merge - check join key alignment")
        return pd.DataFrame()

    # ============================================================
    # POST-MERGE INTEGRITY GUARD - FAIL FAST
    # ============================================================
    # EXECUTABLE is required gate field from Step 6/7 refactor
    # Immediately verify it survived the merge
    if 'EXECUTABLE' not in merged.columns:
        raise RuntimeError(
            f"Step 9B merge integrity failure. EXECUTABLE column missing after merge.\n"
            f"This column is required from Step 6/7 refactor.\n"
            f"Pre-merge columns (evaluated_strategies): {list(evaluated_strategies_df.columns)}\n"
            f"Pre-merge columns (timeframes_subset): {list(timeframes_subset.columns)}\n"
            f"Post-merge columns: {list(merged.columns)}"
        )

    # Filter to only executable strategies before contract fetching
    # Only process rows where EXECUTABLE == True (Step 6/7 refactor gate)
    before_filter = len(merged)
    merged = merged[merged['EXECUTABLE'] == True].copy()
    after_filter = len(merged)

    filtered_count = before_filter - after_filter
    if filtered_count > 0:
        logger.info(f"   🚫 Filtered out {filtered_count} non-executable strategies (EXECUTABLE=False)")

    if merged.empty:
        logger.warning("⚠️ No executable strategies after filtering - all strategies have EXECUTABLE=False")
        return pd.DataFrame()

    logger.info(f"   ✅ Processing {len(merged)} executable strategies (EXECUTABLE=True)")

    # Additional required columns check
    additional_required = ['last_price', 'Ticker', 'Strategy_Name']
    additional_missing = [col for col in additional_required if col not in merged.columns]
    if additional_missing:
        raise RuntimeError(
            f"Step 9B merge integrity failure. Missing required columns: {additional_missing}"
        )
    
    # Initialize Schwab client with non-invasive auth guard
    logger.info("🔑 Initializing Schwab API client for contract fetching...")
    
    # DEBUG MODE DOCTRINE: Debug mode only filters ticker count, never skips API calls or logic.
    # The same Schwab API, IV resolution, and execution gating must run in both modes.
    # This ensures debug is a truth-preserving miniature of production.
    if os.getenv("DEBUG_TICKER_MODE") == "1":
        logger.info("🧪 DEBUG TICKER MODE ACTIVE - Running real Schwab API with reduced ticker universe")
        # Note: Ticker filtering already happened upstream via DebugManager.restrict_universe()
        # We proceed with real API calls on the filtered set

    try:
        client = SchwabClient()
        client.ensure_valid_token()
        # Fetch contracts
        result_df = fetch_contracts(merged, client, expiry_intent=expiry_intent)
    except Exception as e:
        logger.error(f"❌ CRITICAL: Schwab authentication failed: {e}")
        logger.error("Pipeline cannot continue without contract data (Greeks, prices, liquidity).")
        logger.error("This is a P1 blocking requirement for execution decisions.")
        raise RuntimeError(f"Schwab API authentication failed - pipeline HALTED: {e}") from e
    
    # ============================================================
    # SCHEMA NORMALIZATION LAYER (Public API)
    # ============================================================
    # Ensure canonical schema for downstream steps
    #
    # Column Lineage Fix:
    # Schwab API: strikePrice → contract_details['strike'] → Selected_Strike
    # Canonical: Selected_Strike (already correct, keep as-is)
    # Greeks: delta/gamma/vega/theta → Delta/Gamma/Vega/Theta
    # ============================================================

    # Rename lowercase/raw columns to canonical PascalCase
    result_df.rename(columns={
        'strike': 'Strike',  # Only for direct strike columns (not Selected_Strike)
        'bid': 'Bid',
        'ask': 'Ask',
        'last': 'Last',
        'delta': 'Delta',
        'gamma': 'Gamma',
        'vega': 'Vega',
        'theta': 'Theta',
        'rho': 'Rho',
        'implied_volatility': 'IV',
        'volume': 'Volume',
        'open_interest': 'Open_Interest',
        'Mid': 'Mid_Price'  # Rename Mid → Mid_Price for consistency
    }, inplace=True, errors='ignore')

    # Compute Mid_Price if not already set and Bid/Ask exist
    if 'Mid_Price' not in result_df.columns or result_df['Mid_Price'].isna().all():
        if 'Bid' in result_df.columns and 'Ask' in result_df.columns:
            result_df['Mid_Price'] = (result_df['Bid'] + result_df['Ask']) / 2

    # Ensure Strike column exists (copy from Selected_Strike if needed)
    if 'Strike' not in result_df.columns and 'Selected_Strike' in result_df.columns:
        result_df['Strike'] = result_df['Selected_Strike']

    # Ensure required fields exist (even if NaN) to prevent downstream KeyErrors
    required_fields = ['Strike', 'Selected_Strike', 'Mid_Price', 'Delta', 'Gamma', 'Vega', 'Theta', 'Bid', 'Ask']
    for col in required_fields:
        if col not in result_df.columns:
            result_df[col] = None

    # Ensure Scraper_Status exists and is valid for Step 12
    # Step 12 requires this field for execution gating
    if 'Scraper_Status' not in result_df.columns:
        result_df['Scraper_Status'] = 'OK'
    else:
        result_df['Scraper_Status'] = result_df['Scraper_Status'].fillna('OK')

    logger.info(f"✅ Pipeline Step 9B complete: {len(result_df)} contracts with canonical schema")

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
        from core.shared.data_contracts.config import SCAN_OUTPUT_DIR
        output_dir = SCAN_OUTPUT_DIR
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
        from core.shared.data_contracts.config import SCAN_OUTPUT_DIR
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_csv = SCAN_OUTPUT_DIR / f"Step9B_Contracts_{timestamp}.csv"
    
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
