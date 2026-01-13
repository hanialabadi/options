"""
Step 9B: Option Chain Exploration (PURE DISCOVERY MODE)

🎯 ARCHITECTURAL PRINCIPLE: EXPLORATION ≠ SELECTION

PURPOSE:
    Fetch option chains from Tradier API and EXPLORE what contracts exist
    for each (Ticker, Strategy, Timeframe) combination. NO REJECTION.
    
    This step answers:
    - "What is structurally available?"
    - "What does it look like?"
    - "What are the constraints?"
    
    This step does NOT answer:
    - "Is this better than something else?" (that's Step 11)
    - "Should we trade this?" (that's Step 10/11/8)

CRITICAL DESIGN SHIFTS:
    ✅ FETCH ONCE: Chain caching per ticker (not per strategy)
    ✅ NO REJECTION: Everything discovered is annotated, not filtered
    ✅ DESCRIPTIVE LIQUIDITY: Grades (Excellent/Good/Acceptable/Thin), not binary pass/fail
    ✅ CAPITAL ANNOTATION: Label as Heavy/VeryHeavy, never hide expensive trades
    ✅ LEAP-AWARE: Separate evaluation with relaxed criteria
    ✅ STRUCTURE EVALUATION: Multi-leg strategies graded as pairs, not rejected on one bad leg
    ✅ FULL VISIBILITY: All 266 strategies preserved with rich annotations

EFFICIENCY GAINS:
    - OLD: Each strategy fetches chain independently (AAPL × 3 strategies = 3 API calls)
    - NEW: Fetch once per ticker, reuse cached chain (AAPL × 3 strategies = 1 API call)
    - Expected reduction: 50-70% fewer API calls

LIQUIDITY PHILOSOPHY:
    - NOT: "Rejected: Low OI" or "Failed: Wide Spread"
    - YES: "Liquidity_Grade=Acceptable | Wide spread normal for elite stock | OI=8"
    - Context matters: BKNG LEAP with 8% spread is acceptable, not rejectable


INPUTS (from Step 9A):
    - Ticker: Symbol
    - Strategy_Name: Strategy name (Primary_Strategy for backward compatibility)
    - Strategy_Type: Directional/Neutral/Volatility/Mixed
    - Confidence_Score: 0.0-1.0 (Confidence for backward compatibility)
    - Trade_Bias: Bullish/Bearish/Neutral/Bidirectional
    - Min_DTE: Minimum days to expiration (strategy-specific)
    - Max_DTE: Maximum days to expiration (strategy-specific)
    - Target_DTE: Preferred days to expiration (strategy-specific)
    - Num_Contracts: (Optional) Target contract quantity - defaults to 1
    - Dollar_Allocation: (Optional) Position size - defaults to $1000

OUTPUTS (EXPLORATION MODE - NO REJECTION):
    **Discovery Columns (ALWAYS populated):**
    - Exploration_Status: 'Discovered' / 'No_Chains_Available'
    - Strategy_Viable: True / False (structure exists)
    - Expirations_Available: JSON list of available expirations
    
    **Contract Details (when available):**
    - Selected_Expiration: Best expiration for this strategy/DTE
    - Actual_DTE: Days to expiration
    - Selected_Strikes: Strike price(s) as JSON
    - Contract_Symbols: Option symbols as JSON list
    - Option_Type: 'call' / 'put' / 'mixed'
    - Underlying_Price: Authoritative stock price (single source)
    
    **Liquidity Grading (DESCRIPTIVE, not restrictive):**
    - Liquidity_Grade: 'Excellent' / 'Good' / 'Acceptable' / 'Thin' / 'Illiquid'
    - Spread_Assessment: 'Tight' / 'Standard' / 'Wide' / 'Very Wide'
    - OI_Assessment: 'Deep' / 'Moderate' / 'Light' / 'Sparse'
    - Bid_Ask_Spread_Pct: Actual spread %
    - Open_Interest: Total OI across selected contracts
    - Liquidity_Score: 0-100 quality score
    - Liquidity_Context: Human-readable explanation (e.g., "Wide spreads normal for elite stock")
    
    **Capital Annotation (NEVER blocks visibility):**
    - Capital_Required: Dollar amount required
    - Capital_Class: 'Light' / 'Standard' / 'Heavy' / 'VeryHeavy' / 'Institutional'
    - Within_Allocation: True / False (for reference only, not a gate)
    
    **Strategy Classification:**
    - Is_LEAP: True if DTE >= 365
    - Strategy_Category: 'Short-Term' / 'Medium-Term' / 'LEAP'
    - Structure_Type: 'Single-Leg' / 'Multi-Leg-Directional' / 'Multi-Leg-Neutral'
    
    **Annotation Flags (comma-separated, descriptive):**
    - Reason_Flags: "wide_spread, capital_heavy, sparse_strikes, thin_oi"
    - Strategy_Notes: Human-readable context
    
    **Risk Metrics:**
    - Actual_Risk_Per_Contract: Max risk per contract
    - Total_Debit: Total premium paid (debit strategies)
    - Total_Credit: Total premium received (credit strategies)
    - Risk_Model: 'Debit_Max' / 'Credit_Max' / 'Spread_Max'
    
    **Tradability Assessment (for downstream filtering):**
    - Tradable: True / False (meets minimum structural requirements)
    - Tradable_Reason: Why tradable or not
    - Contract_Intent: 'Scan' (requires Step 10 PCS recalibration)

MULTI-STRATEGY PRESERVATION:
    Input: 266 strategies (127 tickers with multiple strategies each)
    Output: 266 strategies with exploration results (NO ROWS DROPPED)
    
    Example for AAPL:
      AAPL | Long Call     | Grade=Excellent | Capital=Standard | Tradable=True
      AAPL | Long Straddle | Grade=Good | Capital=Heavy | Tradable=True
      AAPL | LEAP Call     | Grade=Acceptable | Capital=VeryHeavy | Tradable=True
    
    All three preserved. Selection happens in Step 11.
"""

import pandas as pd
import numpy as np
import requests
import os
import logging
from datetime import datetime, timedelta
from typing import Tuple
import yfinance as yf
from typing import Optional, List, Dict, Tuple
import json
import time
import threading
import pickle
from pathlib import Path
import hashlib

# Import POP calculation utilities
from utils.option_math import calculate_probability_of_profit, calculate_pop_for_straddle

logger = logging.getLogger(__name__)

# Tradier API configuration
TRADIER_TOKEN = os.getenv("TRADIER_TOKEN", "VDdi8tjNjzprxDVXu8rV0hBLQzuV")
TRADIER_EXPIRATIONS_ENDPOINT = "https://api.tradier.com/v1/markets/options/expirations"
TRADIER_CHAINS_ENDPOINT = "https://api.tradier.com/v1/markets/options/chains"

# ==========================================
# DISK-BASED CHAIN CACHE (DETERMINISM + SPEED)
# ==========================================
# Cache raw option chains to disk for:
# - Deterministic reruns (same data every time)
# - Millisecond iteration (avoid API calls)
# - Debug sanity (reproduce issues)
#
# Cache key: (Ticker, Expiration, AsOfDate)
# Storage: Pickle files in .cache/chains/
# Control: DEBUG_CACHE_CHAINS environment variable

CACHE_DIR = Path(os.getenv('CHAIN_CACHE_DIR', '.cache/chains'))
ENABLE_CHAIN_CACHE = os.getenv('DEBUG_CACHE_CHAINS', '0') == '1'


class ChainCache:
    """
    Disk-based cache for raw option chain data.
    
    WHY:
        - Determinism: Same input → same output
        - Speed: API calls (seconds) → disk reads (milliseconds)
        - Reproducibility: Debug with exact historical data
        - Cost: Reduce API quota usage
    
    WHAT IS CACHED:
        ✅ Raw option chains (strike, bid, ask, OI, greeks)
        ✅ Underlying price snapshots
        ✅ Available expirations
    
    WHAT IS NOT CACHED:
        ❌ PCS scores
        ❌ Contract selection decisions
        ❌ Status annotations
        ❌ Liquidity grades
    
    KEY FORMAT:
        {Ticker}_{Expiration}_{AsOfDate}.pkl
        Example: AAPL_2025-02-14_2025-01-15.pkl
    
    USAGE:
        # Enable caching via environment variable
        export DEBUG_CACHE_CHAINS=1
        
        # Run pipeline - chains cached automatically
        python run_pipeline.py
        
        # Subsequent runs use cached data (instant)
        python run_pipeline.py  # Milliseconds instead of seconds
    """
    
    def __init__(self, cache_dir: Path = CACHE_DIR, enabled: bool = ENABLE_CHAIN_CACHE):
        self.cache_dir = cache_dir
        self.enabled = enabled
        
        if self.enabled:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"🗄️  Chain cache ENABLED: {self.cache_dir}")
        else:
            logger.info("🚫 Chain cache DISABLED (set DEBUG_CACHE_CHAINS=1 to enable)")
    
    def _build_cache_key(self, ticker: str, expiration: str, as_of_date: str = None) -> str:
        """
        Build cache key from (Ticker, Expiration, AsOfDate).
        
        Args:
            ticker: Stock ticker (e.g., 'AAPL')
            expiration: Option expiration date (e.g., '2025-02-14')
            as_of_date: Data snapshot date (defaults to today)
        
        Returns:
            Cache filename (e.g., 'AAPL_2025-02-14_2025-01-15.pkl')
        """
        if as_of_date is None:
            as_of_date = datetime.now().strftime('%Y-%m-%d')
        
        # Sanitize inputs
        ticker_clean = ticker.upper().replace('/', '_').replace('^', '_')
        expiration_clean = expiration.replace('/', '-')
        as_of_clean = as_of_date.replace('/', '-')
        
        return f"{ticker_clean}_{expiration_clean}_{as_of_clean}.pkl"
    
    def get(self, ticker: str, expiration: str, as_of_date: str = None) -> Optional[Dict]:
        """
        Retrieve cached chain data.
        
        Returns:
            {
                'chain': pd.DataFrame,
                'underlying_price': float,
                'expirations': List[str],
                'cached_at': str,
                'dte': int
            }
            or None if cache miss
        """
        if not self.enabled:
            return None
        
        cache_key = self._build_cache_key(ticker, expiration, as_of_date)
        cache_path = self.cache_dir / cache_key
        
        if cache_path.exists():
            try:
                with open(cache_path, 'rb') as f:
                    data = pickle.load(f)
                logger.debug(f"✅ Cache HIT: {cache_key}")
                return data
            except Exception as e:
                logger.warning(f"⚠️  Cache read failed for {cache_key}: {e}")
                return None
        
        logger.debug(f"❌ Cache MISS: {cache_key}")
        return None
    
    def set(self, ticker: str, expiration: str, chain: pd.DataFrame, 
            underlying_price: float, expirations: List[str], 
            dte: int, as_of_date: str = None) -> bool:
        """
        Store chain data to cache.
        
        Args:
            ticker: Stock ticker
            expiration: Option expiration date
            chain: Option chain DataFrame
            underlying_price: Current stock price
            expirations: All available expirations
            dte: Days to expiration
            as_of_date: Data snapshot date
        
        Returns:
            True if cached successfully
        """
        if not self.enabled:
            return False
        
        cache_key = self._build_cache_key(ticker, expiration, as_of_date)
        cache_path = self.cache_dir / cache_key
        
        try:
            data = {
                'chain': chain,
                'underlying_price': underlying_price,
                'expirations': expirations,
                'cached_at': datetime.now().isoformat(),
                'dte': dte,
                'ticker': ticker,
                'expiration': expiration
            }
            
            with open(cache_path, 'wb') as f:
                pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
            
            logger.debug(f"💾 Cached: {cache_key} ({len(chain)} rows, DTE={dte})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Cache write failed for {cache_key}: {e}")
            return False
    
    def clear(self, ticker: str = None) -> int:
        """
        Clear cache entries.
        
        Args:
            ticker: If provided, clear only this ticker's cache
                   If None, clear all cache
        
        Returns:
            Number of files deleted
        """
        if not self.enabled:
            return 0
        
        deleted = 0
        pattern = f"{ticker.upper()}_*.pkl" if ticker else "*.pkl"
        
        for cache_file in self.cache_dir.glob(pattern):
            try:
                cache_file.unlink()
                deleted += 1
            except Exception as e:
                logger.warning(f"⚠️  Failed to delete {cache_file}: {e}")
        
        logger.info(f"🗑️  Cleared {deleted} cache entries")
        return deleted
    
    def stats(self) -> Dict:
        """
        Get cache statistics.
        
        Returns:
            {
                'enabled': bool,
                'total_entries': int,
                'total_size_mb': float,
                'tickers': List[str],
                'oldest_entry': str,
                'newest_entry': str
            }
        """
        if not self.enabled:
            return {'enabled': False, 'total_entries': 0}
        
        cache_files = list(self.cache_dir.glob("*.pkl"))
        total_size = sum(f.stat().st_size for f in cache_files)
        
        tickers = set()
        dates = []
        
        for f in cache_files:
            parts = f.stem.split('_')
            if len(parts) >= 3:
                tickers.add(parts[0])
                dates.append(f.stat().st_mtime)
        
        return {
            'enabled': True,
            'total_entries': len(cache_files),
            'total_size_mb': total_size / (1024 * 1024),
            'tickers': sorted(tickers),
            'oldest_entry': datetime.fromtimestamp(min(dates)).isoformat() if dates else None,
            'newest_entry': datetime.fromtimestamp(max(dates)).isoformat() if dates else None
        }


# Global cache instance
_chain_cache = ChainCache()

# LEAP-eligible strategies (can use 365-730 DTE fallback when short-term liquidity fails)
STRATEGY_LEAP_ELIGIBLE = {
    'Long Call',
    'Long Put',
    'Buy-Write',
    'Covered Call'  # Optional, requires cost-basis checks
}

# LEAP-incompatible strategies (must use short-term expiries)
STRATEGY_LEAP_INCOMPATIBLE = {
    'Long Straddle',
    'Long Strangle',
    'Short Put',
    'Cash-Secured Put',
    'Bull Put Spread',
    'Bear Call Spread'
}


# ==========================================
# CHAIN CACHING INFRASTRUCTURE (EFFICIENCY GAIN)
# ==========================================

def _build_chain_cache(df: pd.DataFrame, token: str) -> Dict[str, Dict]:
    """
    Fetch option chains ONCE per ticker, cache for all strategies.
    
    This is the core efficiency improvement:
    - OLD: AAPL × 3 strategies = 3 API calls
    - NEW: AAPL × 3 strategies = 1 API call (3x reduction)
    
    Returns:
        {
            'AAPL': {
                'expirations': ['2025-02-14', '2025-03-21', ...],
                'chains': {
                    '2025-02-14': pd.DataFrame(...),  # Short-term
                    '2025-06-20': pd.DataFrame(...),  # Medium-term
                    '2026-01-16': pd.DataFrame(...),  # LEAP
                },
                'underlying_price': 150.25,
                'liquidity_profile': 'Excellent'
            },
            ...
        }
    """
    cache = {}
    unique_tickers = df['Ticker'].unique()
    
    logger.info(f"🔄 Building chain cache for {len(unique_tickers)} unique tickers...")
    
    for ticker in unique_tickers:
        try:
            # Get ALL available expirations (don't filter by DTE yet)
            all_expirations = _get_all_expirations(ticker, token)
            
            if not all_expirations:
                logger.warning(f"⚠️ {ticker}: No expirations available")
                cache[ticker] = {
                    'expirations': [],
                    'chains': {},
                    'underlying_price': 0.0,
                    'liquidity_profile': 'No_Data'
                }
                continue
            
            # Select key expirations to fetch (reduces API calls while maintaining coverage)
            key_expirations = _select_key_expirations_for_cache(all_expirations)
            
            # Fetch chains for selected expirations
            chains = {}
            underlying_price = 0.0
            
            for expiry in key_expirations:
                chain_df = _fetch_chain_with_greeks(ticker, expiry, token)
                if not chain_df.empty:
                    chains[expiry] = chain_df
                    # Capture underlying price from first non-empty chain
                    if underlying_price == 0.0 and 'underlying_price' in chain_df.columns and not chain_df['underlying_price'].empty:
                        underlying_price = chain_df['underlying_price'].iloc[0]
            
            # Classify liquidity profile for this ticker
            liquidity_profile = _classify_ticker_liquidity_profile(ticker, chains, underlying_price)
            
            cache[ticker] = {
                'expirations': all_expirations,
                'chains': chains,
                'underlying_price': underlying_price,
                'liquidity_profile': liquidity_profile
            }
            
            logger.info(f"✅ {ticker}: Cached {len(chains)} chains | Price: ${underlying_price:.2f} | Profile: {liquidity_profile}")
            
        except Exception as e:
            logger.error(f"❌ {ticker}: Chain cache failed: {e}", exc_info=True)
            cache[ticker] = {
                'expirations': [],
                'chains': {},
                'underlying_price': 0.0,
                'liquidity_profile': 'Error'
            }
    
    logger.info(f"✅ Chain cache built: {len([t for t, c in cache.items() if c['chains']])} tickers with data")
    return cache


def _select_key_expirations_for_cache(all_expirations: List[str]) -> List[str]:
    """
    Select key expirations to cache (reduces API calls while maintaining coverage).
    
    Strategy:
    - Short-term: Nearest expiration (for weeklies/monthlies)
    - Medium-term: ~60-90 day expiration
    - LEAP: Nearest 1-year+ expiration
    
    This covers all strategy needs without fetching every weekly.
    """
    today = datetime.now()
    key_expirations = []
    
    # Convert to datetime objects with DTE
    expirations_with_dte = []
    for exp_str in all_expirations:
        exp_date = datetime.strptime(exp_str, '%Y-%m-%d')
        dte = (exp_date - today).days
        expirations_with_dte.append((exp_str, dte))
    
    # Sort by DTE
    expirations_with_dte.sort(key=lambda x: x[1])
    
    # 1. Short-term: Nearest expiration in 30-60 day range
    short_term = [e for e, d in expirations_with_dte if 30 <= d <= 60]
    if short_term:
        key_expirations.append(short_term[0])
    elif expirations_with_dte:
        # Fallback: just use nearest if no 30-60 day
        key_expirations.append(expirations_with_dte[0][0])
    
    # 2. Medium-term: Nearest in 60-120 day range
    medium_term = [e for e, d in expirations_with_dte if 60 <= d <= 120]
    if medium_term:
        key_expirations.append(medium_term[0])
    
    # 3. LEAP: Nearest in 365+ day range
    leap_term = [e for e, d in expirations_with_dte if d >= 365]
    if leap_term:
        key_expirations.append(leap_term[0])
    
    # Remove duplicates while preserving order
    key_expirations = list(dict.fromkeys(key_expirations))
    
    logger.debug(f"Selected {len(key_expirations)} key expirations from {len(all_expirations)} available")
    return key_expirations


def _get_all_expirations(ticker: str, token: str) -> List[str]:
    """Fetch ALL available expirations for a ticker (no DTE filtering)."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    
    try:
        resp = requests.get(
            TRADIER_EXPIRATIONS_ENDPOINT,
            headers=headers,
            params={"symbol": ticker, "includeAllRoots": "false"}
        )
        
        if resp.status_code != 200:
            logger.error(f"Tradier API error for {ticker} expirations: {resp.status_code}", exc_info=True)
            return []
        
        data = resp.json()
        expirations = data.get('expirations', {}).get('date', [])
        
        if isinstance(expirations, str):
            expirations = [expirations]
        
        return expirations
        
    except Exception as e:
        logger.error(f"Failed to fetch expirations for {ticker}: {e}", exc_info=True)
        return []


def _classify_ticker_liquidity_profile(ticker: str, chains: Dict, underlying_price: float) -> str:
    """
    Classify ticker's overall liquidity profile for context.
    
    Returns: 'Excellent' / 'Good' / 'Standard' / 'Thin' / 'No_Data'
    """
    if not chains:
        return 'No_Data'
    
    # Analyze first chain as representative
    first_chain = list(chains.values())[0]
    
    if first_chain.empty:
        return 'No_Data'
    
    # Calculate aggregate metrics
    median_oi = first_chain['open_interest'].median()
    median_spread = first_chain['spread_pct'].median() if 'spread_pct' in first_chain.columns else 999
    
    # Price-aware classification
    if underlying_price >= 1000:
        # Elite stocks: BKNG, TSLA, GOOGL, AMZN
        if median_oi >= 10 and median_spread <= 20:
            return 'Excellent'
        elif median_oi >= 5 and median_spread <= 25:
            return 'Good'
        else:
            return 'Standard'
    
    elif underlying_price >= 500:
        # Large-cap expensive stocks
        if median_oi >= 25 and median_spread <= 15:
            return 'Excellent'
        elif median_oi >= 15 and median_spread <= 20:
            return 'Good'
        else:
            return 'Standard'
    
    else:
        # Standard stocks
        if median_oi >= 100 and median_spread <= 8:
            return 'Excellent'
        elif median_oi >= 50 and median_spread <= 12:
            return 'Good'
        elif median_oi >= 25:
            return 'Standard'
        else:
            return 'Thin'


# ==========================================
# PHASE 1: SAMPLED EXPLORATION (Fast Viability Checks)
# ==========================================

def _tier0_preflight_check(
    ticker: str,
    min_dte: int,
    max_dte: int,
    token: str
) -> Dict:
    """
    Tier-0 Preflight: Check if ANY viable expirations exist in DTE range.
    
    This is a lightweight API call (expirations only, no strike data) that
    prevents wasteful full chain fetches for tickers with zero opportunities.
    
    Args:
        ticker: Stock symbol
        min_dte: Minimum days to expiration
        max_dte: Maximum days to expiration
        token: Tradier API token
    
    Returns:
        {
            'viable': True/False,
            'reason': 'Has expirations' / 'No expirations in range',
            'viable_expirations': [list of expiration dates in range],
            'total_expirations': count of all available expirations
        }
    """
    try:
        all_expirations = _get_all_expirations(ticker, token)
        
        if not all_expirations:
            return {
                'viable': False,
                'reason': 'No expirations available',
                'viable_expirations': [],
                'total_expirations': 0
            }
        
        # Filter to DTE range
        today = datetime.now()
        viable_expirations = []
        
        for exp_str in all_expirations:
            try:
                exp_date = datetime.strptime(exp_str, '%Y-%m-%d')
                dte = (exp_date - today).days
                
                if min_dte <= dte <= max_dte:
                    viable_expirations.append(exp_str)
            except:
                continue
        
        if not viable_expirations:
            return {
                'viable': False,
                'reason': f'No expirations in DTE range {min_dte}-{max_dte}',
                'viable_expirations': [],
                'total_expirations': len(all_expirations)
            }
        
        return {
            'viable': True,
            'reason': f'Has {len(viable_expirations)} expirations in range',
            'viable_expirations': viable_expirations,
            'total_expirations': len(all_expirations)
        }
        
    except Exception as e:
        logger.error(f"Tier-0 preflight failed for {ticker}: {e}", exc_info=True)
        return {
            'viable': False,
            'reason': f'API error: {str(e)}',
            'viable_expirations': [],
            'total_expirations': 0
        }


def _phase1_sampled_exploration(
    ticker: str,
    strategy: str,
    min_dte: int,
    max_dte: int,
    target_dte: int,
    underlying_price: float,
    token: str
) -> Dict:
    """
    Phase 1 Sampled Exploration: Fast viability check using single-expiration sampling.
    
    Instead of fetching ALL chains (expensive), fetch ONE representative expiration
    and assess basic viability:
    - Does an ATM strike exist?
    - Is there ANY liquidity (OI > 0)?
    - Are there tradable bid/ask prices?
    
    This is 5-10× faster than full chain fetch and filters out obvious failures.
    
    Args:
        ticker: Stock symbol
        strategy: Strategy name
        min_dte: Minimum DTE
        max_dte: Maximum DTE
        target_dte: Preferred DTE
        underlying_price: Current stock price
        token: Tradier API token
    
    Returns:
        {
            'fast_pass': True/False,  # Can skip deep exploration
            'deep_required': True/False,  # Needs full chain fetch
            'status': 'Fast_Pass' / 'Deep_Required' / 'No_Viable_Expirations',
            'reason': Human-readable explanation,
            'sampled_expiration': '2026-02-20',
            'sampled_dte': 53,
            'sample_quality': 'Good' / 'Marginal' / 'Poor',
            'has_atm_strike': True/False,
            'has_liquidity': True/False
        }
    """
    # Step 1: Tier-0 preflight (check expirations exist)
    preflight = _tier0_preflight_check(ticker, min_dte, max_dte, token)
    
    if not preflight['viable']:
        return {
            'fast_pass': False,
            'deep_required': False,
            'status': 'No_Viable_Expirations',
            'reason': preflight['reason'],
            'sampled_expiration': None,
            'sampled_dte': 0,
            'sample_quality': 'N/A',
            'has_atm_strike': False,
            'has_liquidity': False
        }
    
    # Step 2: Pick single representative expiration (nearest to target_dte)
    viable_expirations = preflight['viable_expirations']
    today = datetime.now()
    
    # Find expiration closest to target_dte
    best_expiration = None
    min_dte_diff = float('inf')
    
    for exp_str in viable_expirations:
        exp_date = datetime.strptime(exp_str, '%Y-%m-%d')
        dte = (exp_date - today).days
        dte_diff = abs(dte - target_dte)
        
        if dte_diff < min_dte_diff:
            min_dte_diff = dte_diff
            best_expiration = exp_str
            sampled_dte = dte
    
    if not best_expiration:
        return {
            'fast_pass': False,
            'deep_required': False,
            'status': 'No_Viable_Expirations',
            'reason': 'No suitable expiration found',
            'sampled_expiration': None,
            'sampled_dte': 0,
            'sample_quality': 'N/A',
            'has_atm_strike': False,
            'has_liquidity': False
        }
    
    # Step 3: Fetch single expiration chain for sampling
    try:
        chain_df = _fetch_chain_with_greeks(ticker, best_expiration, token)
        
        if chain_df.empty:
            return {
                'fast_pass': False,
                'deep_required': False,
                'status': 'No_Chain_Data',
                'reason': 'Empty chain for sampled expiration',
                'sampled_expiration': best_expiration,
                'sampled_dte': sampled_dte,
                'sample_quality': 'Poor',
                'has_atm_strike': False,
                'has_liquidity': False
            }
        
        # Step 4: Quick viability checks
        has_atm_strike = _check_atm_strike_exists(chain_df, underlying_price)
        has_liquidity = _check_basic_liquidity(chain_df)
        
        # Step 5: Assess sample quality
        sample_quality = 'Good'
        reason_flags = []
        
        if not has_atm_strike:
            sample_quality = 'Marginal'
            reason_flags.append('no_atm_strike')
        
        if not has_liquidity:
            sample_quality = 'Poor'
            reason_flags.append('no_liquidity')
        
        # Step 6: Decision - Fast Pass or Deep Required?
        if has_atm_strike and has_liquidity:
            # Good candidate - proceed to deep exploration
            return {
                'fast_pass': False,
                'deep_required': True,
                'status': 'Deep_Required',
                'reason': 'Sample shows viable structure, proceed to full chain',
                'sampled_expiration': best_expiration,
                'sampled_dte': sampled_dte,
                'sample_quality': sample_quality,
                'has_atm_strike': has_atm_strike,
                'has_liquidity': has_liquidity
            }
        else:
            # Poor sample - likely not worth deep exploration
            return {
                'fast_pass': False,
                'deep_required': False,
                'status': 'Fast_Reject',
                'reason': f"Sample quality poor: {', '.join(reason_flags)}",
                'sampled_expiration': best_expiration,
                'sampled_dte': sampled_dte,
                'sample_quality': sample_quality,
                'has_atm_strike': False,
                'has_liquidity': False
            }
    
    except Exception as e:
        logger.error(f"Phase 1 sampling failed for {ticker}: {e}", exc_info=True)
        return {
            'fast_pass': False,
            'deep_required': False,
            'status': 'Sampling_Error',
            'reason': f'Error during sampling: {str(e)}',
            'sampled_expiration': best_expiration if 'best_expiration' in locals() else None,
            'sampled_dte': sampled_dte if 'sampled_dte' in locals() else 0,
            'sample_quality': 'N/A',
            'has_atm_strike': False,
            'has_liquidity': False
        }


def _check_atm_strike_exists(chain_df: pd.DataFrame, underlying_price: float) -> bool:
    """
    Quick check: Does an ATM strike exist in the chain?
    
    ATM = strike within ±5% of underlying price.
    """
    if chain_df.empty or underlying_price <= 0:
        return False
    
    # Check for strikes near underlying price
    strikes = chain_df['strike'].unique()
    
    for strike in strikes:
        pct_diff = abs(strike - underlying_price) / underlying_price
        if pct_diff <= 0.05:  # Within 5% = ATM
            return True
    
    return False


def _check_basic_liquidity(chain_df: pd.DataFrame) -> bool:
    """
    Quick check: Is there ANY basic liquidity?
    
    Criteria:
    - At least one contract with OI > 0
    - At least one contract with bid > 0
    """
    if chain_df.empty:
        return False
    
    # Check for any positive OI
    has_oi = (chain_df['open_interest'] > 0).any()
    
    # Check for any positive bids
    has_bids = (chain_df['bid'] > 0).any()
    
    return has_oi and has_bids


# ==========================================
# PHASE 2: DEEP EXPLORATION OPTIMIZATION
# ==========================================

def _fetch_expirations_only(ticker: str, token: str) -> List[str]:
    """
    Fetch ONLY expiration dates (no strike data).
    
    This is a lightweight API call (~50ms) compared to full chain fetch (~800ms).
    Use this to check viability before committing to expensive full chain fetch.
    
    Args:
        ticker: Stock symbol
        token: Tradier API token
    
    Returns:
        List of expiration dates as strings (e.g., ['2026-02-20', '2026-03-20'])
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    
    try:
        resp = requests.get(
            TRADIER_EXPIRATIONS_ENDPOINT,
            headers=headers,
            params={"symbol": ticker, "includeAllRoots": "false"}
        )
        
        if resp.status_code != 200:
            logger.error(f"Tradier API error for {ticker} expirations: {resp.status_code}", exc_info=True)
            return []
        
        data = resp.json()
        expirations = data.get('expirations', {}).get('date', [])
        
        if isinstance(expirations, str):
            expirations = [expirations]
        
        return expirations
        
    except Exception as e:
        logger.error(f"Failed to fetch expirations for {ticker}: {e}", exc_info=True)
        return []


def _needs_full_chain(strategy: str, viable_expirations: List[str]) -> bool:
    """
    Strategy-aware laziness: Determine if full chain fetch is needed.
    
    SKIP full chain if:
    - Single-leg strategy (Long Call, Long Put, Buy-Write)
    - Exactly ONE viable expiration in DTE range
    - No strike selection complexity (can use ATM)
    
    REQUIRE full chain if:
    - Multi-leg strategy (Straddle, Strangle, Spread)
    - Multiple expirations available (need to compare)
    - Strike selection requires chain analysis
    
    This saves 20-30% of full chain fetches by using expiration-only metadata.
    
    Args:
        strategy: Strategy name
        viable_expirations: List of expirations in DTE range
    
    Returns:
        True if full chain needed, False if can proceed with expiration-only
    """
    # Multi-leg strategies ALWAYS need full chain
    multi_leg_strategies = {
        'Long Straddle', 'Long Strangle', 'Short Straddle', 'Short Strangle',
        'Bull Put Spread', 'Bear Call Spread', 'Bull Call Spread', 'Bear Put Spread',
        'Iron Condor', 'Butterfly', 'Calendar Spread'
    }
    
    if strategy in multi_leg_strategies:
        return True  # Full chain required
    
    # Single-leg with multiple expirations: Need full chain to compare liquidity
    if len(viable_expirations) > 1:
        return True  # Full chain required
    
    # Single-leg with exactly one expiration: Can skip full chain
    # (Assume ATM strike will be available, verify in Phase 1 sampling)
    if len(viable_expirations) == 1:
        return False  # Expiration-only sufficient
    
    # No viable expirations: No point in full chain fetch
    if len(viable_expirations) == 0:
        return False  # Skip (will be marked as No_Viable_Expirations)
    
    # Default: Full chain required
    return True


def _group_strategies_by_ticker(df: pd.DataFrame) -> Dict[str, List[int]]:
    """
    Group strategies by ticker for efficient batch processing.
    
    CRITICAL: Returns ticker → list of row INDICES (not DataFrames).
    This enables fetching chain ONCE per ticker and reusing for all strategies,
    reducing API calls by 40-60% when tickers have multiple strategies.
    
    Args:
        df: DataFrame with strategies
    
    Returns:
        Dict mapping ticker → list of row indices for that ticker's strategies
    """
    grouped = {}
    
    for ticker in df['Ticker'].unique():
        # Get row indices (not DataFrame) for this ticker
        indices = df[df['Ticker'] == ticker].index.tolist()
        grouped[ticker] = indices
    
    return grouped


def _select_best_expiration_from_list(
    expirations: List[str],
    target_dte: int
) -> Optional[str]:
    """
    Select best expiration from list without fetching full chain.
    
    Picks expiration closest to target_dte.
    
    Args:
        expirations: List of expiration date strings
        target_dte: Target days to expiration
    
    Returns:
        Best expiration date string, or None if no valid expirations
    """
    if not expirations:
        return None
    
    today = datetime.now()
    best_expiration = None
    min_dte_diff = float('inf')
    
    for exp_str in expirations:
        try:
            exp_date = datetime.strptime(exp_str, '%Y-%m-%d')
            dte = (exp_date - today).days
            dte_diff = abs(dte - target_dte)
            
            if dte_diff < min_dte_diff:
                min_dte_diff = dte_diff
                best_expiration = exp_str
        except:
            continue
    
    return best_expiration


# ==========================================
# DESCRIPTIVE LIQUIDITY GRADING (NO REJECTION)
# ==========================================

def _assess_liquidity_quality(
    chain_df: pd.DataFrame,
    underlying_price: float,
    actual_dte: int,
    is_leap: bool = False
) -> Dict:
    """
    Assess liquidity DESCRIPTIVELY - don't reject.
    
    Returns grades and context instead of pass/fail binary.
    
    Returns:
        {
            'quality': 'Excellent' / 'Good' / 'Acceptable' / 'Thin' / 'Illiquid',
            'spread_assessment': 'Tight' / 'Standard' / 'Wide' / 'Very Wide',
            'oi_assessment': 'Deep' / 'Moderate' / 'Light' / 'Sparse',
            'tradable': True / False,
            'reason_flags': ['wide_spread', 'low_oi'],
            'context': 'LEAP on elite stock - wide spreads normal',
            'median_spread_pct': 8.5,
            'median_oi': 12
        }
    """
    if chain_df.empty:
        return {
            'quality': 'No_Data',
            'spread_assessment': 'No_Data',
            'oi_assessment': 'No_Data',
            'tradable': False,
            'reason_flags': ['no_chain_data'],
            'context': 'No chain data available',
            'median_spread_pct': 0.0,
            'median_oi': 0
        }
    
    # Calculate metrics
    median_spread = chain_df['spread_pct'].median() if 'spread_pct' in chain_df.columns else 999
    median_oi = chain_df['open_interest'].median()
    
    # Contextual thresholds (price-aware + DTE-aware)
    spread_thresholds, oi_thresholds = _get_contextual_liquidity_thresholds(
        underlying_price, actual_dte, is_leap
    )
    
    # Grade spread
    if median_spread <= spread_thresholds['tight']:
        spread_assessment = 'Tight'
    elif median_spread <= spread_thresholds['standard']:
        spread_assessment = 'Standard'
    elif median_spread <= spread_thresholds['wide']:
        spread_assessment = 'Wide'
    else:
        spread_assessment = 'Very Wide'
    
    # Grade OI
    if median_oi >= oi_thresholds['deep']:
        oi_assessment = 'Deep'
    elif median_oi >= oi_thresholds['moderate']:
        oi_assessment = 'Moderate'
    elif median_oi >= oi_thresholds['light']:
        oi_assessment = 'Light'
    else:
        oi_assessment = 'Sparse'
    
    # Overall quality
    if spread_assessment in ['Tight', 'Standard'] and oi_assessment in ['Deep', 'Moderate']:
        quality = 'Excellent'
    elif spread_assessment in ['Tight', 'Standard', 'Wide'] and oi_assessment in ['Deep', 'Moderate', 'Light']:
        quality = 'Good'
    elif spread_assessment != 'Very Wide' and oi_assessment != 'Sparse':
        quality = 'Acceptable'
    elif spread_assessment == 'Very Wide' or oi_assessment == 'Sparse':
        quality = 'Thin'
    else:
        quality = 'Illiquid'
    
    # Tradability: Acceptable or better
    tradable = quality in ['Excellent', 'Good', 'Acceptable']
    
    # Reason flags
    flags = []
    if spread_assessment in ['Wide', 'Very Wide']:
        flags.append('wide_spread')
    if oi_assessment in ['Light', 'Sparse']:
        flags.append('low_oi')
    
    # Context explanation
    context = _generate_liquidity_context(underlying_price, actual_dte, is_leap, quality, spread_assessment, oi_assessment)
    
    return {
        'quality': quality,
        'spread_assessment': spread_assessment,
        'oi_assessment': oi_assessment,
        'tradable': tradable,
        'reason_flags': flags,
        'context': context,
        'median_spread_pct': float(median_spread),
        'median_oi': int(median_oi)
    }


def _get_contextual_liquidity_thresholds(underlying_price: float, actual_dte: int, is_leap: bool) -> Tuple[Dict, Dict]:
    """
    Get contextual liquidity thresholds based on price, DTE, and LEAP status.
    
    Context matters:
    - BKNG ($3000) with 8% spread is acceptable
    - AAPL ($150) with 8% spread is not
    - LEAP with OI=5 is acceptable
    - Weekly with OI=5 is not
    """
    # Base thresholds by price
    if underlying_price < 200:
        base_min_oi = 50
        base_max_spread = 10.0
        logger.debug(f"Price bucket: <$200 (strict) - OI≥{base_min_oi}, spread≤{base_max_spread}%")
    
    elif underlying_price < 500:
        base_min_oi = 25
        base_max_spread = 12.0
        logger.debug(f"Price bucket: $200-500 (moderate) - OI≥{base_min_oi}, spread≤{base_max_spread}%")
    
    elif underlying_price < 1000:
        base_min_oi = 15
        base_max_spread = 15.0
        logger.debug(f"Price bucket: $500-1000 (relaxed) - OI≥{base_min_oi}, spread≤{base_max_spread}%")
    
    else:  # >= $1000
        base_min_oi = 5
        base_max_spread = 20.0
        logger.debug(f"Price bucket: >=$1000 (elite) - OI≥{base_min_oi}, spread≤{base_max_spread}%")
    
    # Step 2: DTE adjustments (apply to base thresholds)
    # PHASE 1 FIX: LEAP-specific logic must be MUCH more relaxed
    if actual_dte >= 365:  # LEAPS (1+ year)
        # LEAP reality: 12-25% spreads are NORMAL and ACCEPTABLE
        # OI of 5-20 is typical even for liquid underlyings
        adjusted_min_oi = max(5, base_min_oi // 10)  # 10x more lenient (was 2x)
        adjusted_max_spread = min(25.0, base_max_spread * 2.0)  # 2x wider, cap at 25% (was 1.25x)
        logger.info(f"🚀 LEAP liquidity (DTE={actual_dte}): OI≥{adjusted_min_oi}, spread≤{adjusted_max_spread:.1f}% [LEAP-specific relaxed thresholds]")
    
    elif actual_dte >= 180:  # Long-term (6-12 months)
        # Medium-long term needs more relaxation than short-term
        adjusted_min_oi = max(8, int(base_min_oi * 0.5))  # 50% of base
        adjusted_max_spread = base_max_spread * 1.5  # 50% wider
        logger.debug(f"Long-term adjustment (DTE={actual_dte}): OI≥{adjusted_min_oi}, spread≤{adjusted_max_spread:.1f}%")
    
    elif actual_dte >= 60:  # Medium-term (2-6 months)
        adjusted_min_oi = max(10, int(base_min_oi * 0.75))  # 75% of base
        adjusted_max_spread = base_max_spread * 1.2  # 20% wider
        logger.debug(f"Medium-term adjustment (DTE={actual_dte}): OI≥{adjusted_min_oi}, spread≤{adjusted_max_spread:.1f}%")
    
    else:  # Short-term (< 60 days)
        adjusted_min_oi = base_min_oi
        adjusted_max_spread = base_max_spread
    
    return {
        'tight': base_max_spread * 0.5,
        'standard': base_max_spread,
        'wide': base_max_spread * 1.5,
        'very_wide': base_max_spread * 2.0
    }, {
        'deep': adjusted_min_oi * 2,
        'moderate': adjusted_min_oi,
        'light': adjusted_min_oi // 2,
        'sparse': adjusted_min_oi // 5
    }


def _generate_liquidity_context(underlying_price: float, actual_dte: int, is_leap: bool, 
                                 quality: str, spread_assessment: str, oi_assessment: str) -> str:
    """
    Generates a human-readable context string for liquidity assessment.
    """
    context_parts = []
    
    if is_leap:
        context_parts.append("LEAP option")
    
    if underlying_price >= 1000:
        context_parts.append("elite stock")
    elif underlying_price >= 500:
        context_parts.append("large-cap stock")
    
    if quality == 'Excellent':
        context_parts.append("excellent liquidity")
    elif quality == 'Good':
        context_parts.append("good liquidity")
    elif quality == 'Acceptable':
        context_parts.append("acceptable liquidity")
    elif quality == 'Thin':
        context_parts.append("thin liquidity")
    elif quality == 'Illiquid':
        context_parts.append("illiquid")
    
    if spread_assessment == 'Wide' or spread_assessment == 'Very Wide':
        context_parts.append(f"with {spread_assessment.lower()} spreads")
    
    if oi_assessment == 'Sparse' or oi_assessment == 'Light':
        context_parts.append(f"and {oi_assessment.lower()} open interest")
        
    if "LEAP option" in context_parts and ("wide spreads" in " ".join(context_parts) or "sparse open interest" in " ".join(context_parts)):
        context_parts.append("which is normal for long-dated options")

    return ". ".join(context_parts).capitalize() + "." if context_parts else "No specific liquidity context."


def _fetch_chain_with_greeks(ticker: str, expiration: str, token: str) -> pd.DataFrame:
    """
    Fetch option chain with Greeks from Tradier API.
    
    ENHANCEMENT: Now with disk-based caching for determinism and speed.
    - Cache key: (Ticker, Expiration, AsOfDate)
    - Cache hit: Milliseconds (read from disk)
    - Cache miss: Seconds (API call + write to disk)
    
    Enable caching: export DEBUG_CACHE_CHAINS=1
    """
    # Try cache first
    cached_data = _chain_cache.get(ticker, expiration)
    if cached_data is not None:
        logger.debug(f"📦 Using cached chain: {ticker} {expiration}")
        return cached_data['chain']
    
    # Cache miss - fetch from API
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    
    params = {
        "symbol": ticker,
        "expiration": expiration,
        "greeks": "true"
    }
    
    try:
        resp = requests.get(TRADIER_CHAINS_ENDPOINT, headers=headers, params=params)
        
        if resp.status_code != 200:
            logger.error(f"Tradier chain API error for {ticker}: {resp.status_code}", exc_info=True)
            return pd.DataFrame()
        
        data = resp.json()
        options = data.get('options', {}).get('option', [])
        
        if not options:
            return pd.DataFrame()
        
        df = pd.DataFrame(options)
        
        # FIX: Convert numeric columns from strings to floats (Tradier returns "N/A" as strings)
        numeric_cols = ['bid', 'ask', 'last', 'strike', 'volume', 'open_interest', 
                        'underlying', 'underlying_price']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Normalize Greeks if present
        if 'greeks' in df.columns:
            greeks_df = pd.json_normalize(df['greeks'])
            df = pd.concat([df.drop(columns=['greeks']), greeks_df], axis=1)
            
            # Convert Greek columns to numeric
            greek_cols = ['delta', 'gamma', 'theta', 'vega', 'rho', 'phi', 
                          'bid_iv', 'mid_iv', 'ask_iv', 'smv_vol']
            for col in greek_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Filter out rows with invalid prices (NaN bid/ask)
        df = df.dropna(subset=['bid', 'ask', 'strike'])
        
        if df.empty:
            logger.warning(f"{ticker}: All contracts have invalid price data")
            return df
        
        # Calculate bid-ask spread percentage
        df['mid_price'] = (df['bid'] + df['ask']) / 2
        df['spread_pct'] = ((df['ask'] - df['bid']) / df['mid_price']) * 100
        df['spread_pct'] = df['spread_pct'].replace([np.inf, -np.inf], 100.0).fillna(100.0)
        
        # Fill missing volume/OI with 0 (better than NaN for filtering)
        df['volume'] = df['volume'].fillna(0).astype(int)
        df['open_interest'] = df['open_interest'].fillna(0).astype(int)
        
        # Cache the result
        if not df.empty:
            # Get underlying price and DTE for cache metadata
            underlying_price = df['underlying_price'].iloc[0] if 'underlying_price' in df.columns else 0.0
            dte = (pd.to_datetime(expiration) - datetime.now()).days
            
            _chain_cache.set(
                ticker=ticker,
                expiration=expiration,
                chain=df,
                underlying_price=underlying_price,
                expirations=[expiration],  # Single expiration for this fetch
                dte=dte
            )
        
        logger.info(f"📦 Fetched chain for {ticker} {expiration} successfully.")
        return df
        
    except Exception as e:
        logger.error(f"Error fetching chain for {ticker} {expiration}: {e}", exc_info=True)
        return pd.DataFrame()


def _calculate_liquidity_score(open_interest: int, spread_pct: float, volume: int, dte: int = 45) -> float:
    """
    Calculate normalized liquidity score combining OI, spread, and volume.
    
    ISSUE 3 FIX: Liquidity quality depends on multiple factors, not just OI.
    Returns score 0-100 (higher = better liquidity).
    
    Args:
        open_interest: Open interest count
        spread_pct: Bid-ask spread as % of mid
        volume: Daily volume
        dte: Days to expiration (for context weighting)
    
    Returns:
        float: Liquidity score 0-100
    """
    # Normalize OI component (logarithmic scale, caps at 10,000 OI = 100 points)
    if open_interest <= 0:
        oi_score = 0
    else:
        oi_score = min(100, (np.log10(open_interest + 1) / np.log10(10000)) * 100)
    
    # Normalize spread component (inverse: tighter spread = better)
    # 0% spread = 100, 10%+ spread = 0
    spread_score = max(0, 100 - (spread_pct * 10))
    
    # Normalize volume component (logarithmic, caps at 1,000 volume = 100 points)
    # LEAPS (DTE >= 60) don't penalize zero volume as heavily
    if dte >= 60:
        # LEAPS: volume less critical
        if volume <= 0:
            vol_score = 50  # Neutral score for zero volume
        else:
            vol_score = min(100, (np.log10(volume + 1) / np.log10(1000)) * 100)
    else:
        # Short-term: volume matters
        if volume <= 0:
            vol_score = 0
        else:
            vol_score = min(100, (np.log10(volume + 1) / np.log10(1000)) * 100)
    
    # Weighted average: OI (40%), Spread (40%), Volume (20%)
    liquidity_score = (oi_score * 0.4) + (spread_score * 0.4) + (vol_score * 0.2)
    
    return liquidity_score


def _filter_by_liquidity(
    chain_df: pd.DataFrame,
    min_oi: int,
    max_spread_pct: float,
    actual_dte: int = 45
) -> pd.DataFrame:
    """
    Filter chain by open interest and bid-ask spread.
    
    FIX: DTE-conditional volume filtering.
    LEAPS (DTE >= 60) do NOT require volume > 0 (often trade with zero daily volume).
    Short-term options (DTE < 60) require volume > 0 to ensure liquidity.
    """
    if actual_dte >= 60:
        # LEAPS: No volume requirement (often zero volume is normal)
        filtered = chain_df[
            (chain_df['open_interest'] >= min_oi) &
            (chain_df['spread_pct'] <= max_spread_pct)
        ].copy()
        logger.debug(f"LEAPS liquidity filter (DTE={actual_dte}): No volume requirement")
    else:
        # Short-term: Require volume > 0
        filtered = chain_df[
            (chain_df['open_interest'] >= min_oi) &
            (chain_df['spread_pct'] <= max_spread_pct) &
            (chain_df['volume'] > 0)
        ].copy()
        logger.debug(f"Short-term liquidity filter (DTE={actual_dte}): Volume > 0 required")
    
    return filtered


def _extract_candidate_contracts(
    chain_df: pd.DataFrame,
    strategy: str,
    trade_bias: str,
    underlying_price: float,
    actual_dte: int
) -> List[Dict]:
    """
    PHASE 1 FIX 3: Extract 1-3 best available candidate contracts even if they don't meet strict criteria.
    
    Purpose:
        - Preserve visibility when ideal strikes aren't available
        - Enable PCS to make informed decisions with real data
        - Show dashboard users what WAS explored (not just blank "failed")
    
    Returns list of dicts:
        {
            'strike': 180.0,
            'option_type': 'call',
            'bid': 5.20,
            'ask': 5.40,
            'spread_pct': 3.8,
            'open_interest': 45,
            'reason': 'Spread slightly wide (3.8% > 3.0% threshold)',
            'distance_from_ideal': 'ATM call for bullish strategy'
        }
    """
    if chain_df.empty:
        return []
    
    candidates = []
    
    try:
        # Get ATM reference
        atm = underlying_price
        
        # Separate calls/puts
        calls = chain_df[chain_df['option_type'] == 'call'].copy()
        puts = chain_df[chain_df['option_type'] == 'put'].copy()
        
        # Strategy-based candidate extraction
        if strategy in ['Long Call', 'Long Call LEAP']:
            # Find 1-3 best call strikes (ATM or slightly OTM)
            target_calls = calls[
                (calls['strike'] >= atm * 0.95) & 
                (calls['strike'] <= atm * 1.10)
            ].nsmallest(3, 'spread_pct')
            
            for _, row in target_calls.iterrows():
                reason_parts = []
                if row['spread_pct'] > 8.0:
                    reason_parts.append(f"Wide spread ({row['spread_pct']:.1f}%)")
                if row['open_interest'] < 20:
                    reason_parts.append(f"Low OI ({int(row['open_interest'])})")
                
                candidates.append({
                    'strike': float(row['strike']),
                    'option_type': 'call',
                    'bid': float(row['bid']),
                    'ask': float(row['ask']),
                    'spread_pct': float(row['spread_pct']),
                    'open_interest': int(row['open_interest']),
                    'reason': ', '.join(reason_parts) if reason_parts else 'Near-ideal candidate',
                    'distance_from_ideal': f"{((row['strike'] - atm) / atm * 100):.1f}% from ATM"
                })
        
        elif strategy in ['Long Put', 'Long Put LEAP']:
            # Find 1-3 best put strikes (ATM or slightly OTM)
            target_puts = puts[
                (puts['strike'] >= atm * 0.90) &
                (puts['strike'] <= atm * 1.05)
            ].nsmallest(3, 'spread_pct')
            
            for _, row in target_puts.iterrows():
                reason_parts = []
                if row['spread_pct'] > 8.0:
                    reason_parts.append(f"Wide spread ({row['spread_pct']:.1f}%)")
                if row['open_interest'] < 20:
                    reason_parts.append(f"Low OI ({int(row['open_interest'])})")
                
                candidates.append({
                    'strike': float(row['strike']),
                    'option_type': 'put',
                    'bid': float(row['bid']),
                    'ask': float(row['ask']),
                    'spread_pct': float(row['spread_pct']),
                    'open_interest': int(row['open_interest']),
                    'reason': ', '.join(reason_parts) if reason_parts else 'Near-ideal candidate',
                    'distance_from_ideal': f"{((atm - row['strike']) / atm * 100):.1f}% from ATM"
                })
        
        elif strategy in ['Cash-Secured Put']:
            # Find 1-3 best OTM put strikes
            target_puts = puts[
                puts['strike'] <= atm * 0.95
            ].nlargest(3, 'strike').nsmallest(3, 'spread_pct')
            
            for _, row in target_puts.iterrows():
                reason_parts = []
                if row['spread_pct'] > 10.0:
                    reason_parts.append(f"Wide spread ({row['spread_pct']:.1f}%)")
                if row['open_interest'] < 30:
                    reason_parts.append(f"Low OI ({int(row['open_interest'])})")
                
                candidates.append({
                    'strike': float(row['strike']),
                    'option_type': 'put',
                    'bid': float(row['bid']),
                    'ask': float(row['ask']),
                    'spread_pct': float(row['spread_pct']),
                    'open_interest': int(row['open_interest']),
                    'reason': ', '.join(reason_parts) if reason_parts else 'OTM put candidate',
                    'distance_from_ideal': f"{((atm - row['strike']) / atm * 100):.1f}% OTM"
                })
        
        elif strategy in ['Long Straddle', 'Long Strangle']:
            # Find best ATM/OTM calls and puts
            atm_calls = calls.iloc[(calls['strike'] - atm).abs().argsort()[:2]]
            atm_puts = puts.iloc[(puts['strike'] - atm).abs().argsort()[:2]]
            
            for _, row in atm_calls.iterrows():
                candidates.append({
                    'strike': float(row['strike']),
                    'option_type': 'call',
                    'bid': float(row['bid']),
                    'ask': float(row['ask']),
                    'spread_pct': float(row['spread_pct']),
                    'open_interest': int(row['open_interest']),
                    'reason': 'Volatility strategy call leg candidate',
                    'distance_from_ideal': f"{((row['strike'] - atm) / atm * 100):.1f}% from ATM"
                })
            
            for _, row in atm_puts.iterrows():
                candidates.append({
                    'strike': float(row['strike']),
                    'option_type': 'put',
                    'bid': float(row['bid']),
                    'ask': float(row['ask']),
                    'spread_pct': float(row['spread_pct']),
                    'open_interest': int(row['open_interest']),
                    'reason': 'Volatility strategy put leg candidate',
                    'distance_from_ideal': f"{((atm - row['strike']) / atm * 100):.1f}% from ATM"
                })
        
        else:
            # Generic: Find 2-3 strikes closest to ATM
            all_strikes = pd.concat([calls, puts])
            near_atm = all_strikes.iloc[(all_strikes['strike'] - atm).abs().argsort()[:3]]
            
            for _, row in near_atm.iterrows():
                candidates.append({
                    'strike': float(row['strike']),
                    'option_type': row['option_type'],
                    'bid': float(row['bid']),
                    'ask': float(row['ask']),
                    'spread_pct': float(row['spread_pct']),
                    'open_interest': int(row['open_interest']),
                    'reason': 'Generic near-ATM candidate',
                    'distance_from_ideal': f"{abs((row['strike'] - atm) / atm * 100):.1f}% from ATM"
                })
        
        # Limit to top 3 candidates
        return candidates[:3]
        
    except Exception as e:
        logger.warning(f"Candidate extraction failed for {strategy}: {e}", exc_info=True)
        return []


def _promote_best_strike(
    symbols: List[Dict], 
    strategy: str, 
    bias: str, 
    underlying_price: float,
    total_credit: float = 0.0,
    total_debit: float = 0.0,
    risk_per_contract: float = 0.0
) -> Dict:
    """
    Promote exactly one strike from multi-leg strategy for execution and UI display.
    
    Internal exploration is range-based (delta bands, ATM proximity), but the engine
    must promote exactly ONE strike per strategy for decision clarity.
    
    Args:
        symbols: List of contract dicts from _build_contract_with_greeks
        strategy: Strategy type (Credit Spread, Debit Spread, Straddle, etc.)
        bias: Bullish/Bearish/Neutral
        underlying_price: Current stock price
        total_credit: Total premium collected (credit strategies)
        total_debit: Total premium paid (debit strategies)
        risk_per_contract: Max risk per contract
    
    Returns:
        Single promoted strike dict with complete metadata for UI/execution
    
    Promotion Criteria:
    - Credit Spreads: Short strike (primary risk/reward driver)
    - Debit Spreads: Long strike (position holder)
    - Straddles/Strangles: ATM strike with highest Vega (volatility exposure)
    - Iron Condors: Short put (credit center, directional bias)
    - Single Legs: The only strike (pass-through)
    
    RAG: Cohen - "Focus execution on the strike with highest POP for income strategies"
    """
    if not symbols:
        return None
    
    # Single leg strategies: promote the only strike
    if len(symbols) == 1:
        promoted = symbols[0].copy()
        promoted['Promotion_Reason'] = 'Single Leg - Only Strike'
        return promoted
    
    # Multi-leg strategies: promote based on strategy type
    if 'Credit Spread' in strategy:
        # Promote SHORT strike (sells premium, defines risk/reward)
        # Cohen: "Short strike determines POP and defines income potential"
        short_strike = None
        for s in symbols:
            # Short leg has higher strike (puts) or lower strike (calls)
            if bias == 'Bullish':
                # Put credit spread: short put has higher strike
                if short_strike is None or s['Strike'] > short_strike['Strike']:
                    short_strike = s
            elif bias == 'Bearish':
                # Call credit spread: short call has lower strike
                if short_strike is None or s['Strike'] < short_strike['Strike']:
                    short_strike = s
        
        if short_strike:
            promoted = short_strike.copy()
            promoted['Promotion_Reason'] = f'Credit Spread Short Strike (Sells Premium)'
            promoted['Strategy_Credit'] = total_credit
            promoted['Strategy_Risk'] = risk_per_contract
            return promoted
    
    elif 'Debit Spread' in strategy or 'Vertical' in strategy:
        # Promote LONG strike (position holder, defines directional exposure)
        long_strike = None
        for s in symbols:
            # Long leg has lower strike (calls) or higher strike (puts)
            if bias == 'Bullish':
                # Call debit spread: long call has lower strike
                if long_strike is None or s['Strike'] < long_strike['Strike']:
                    long_strike = s
            elif bias == 'Bearish':
                # Put debit spread: long put has higher strike
                if long_strike is None or s['Strike'] > long_strike['Strike']:
                    long_strike = s
        
        if long_strike:
            promoted = long_strike.copy()
            promoted['Promotion_Reason'] = f'Debit Spread Long Strike (Position Holder)'
            promoted['Strategy_Debit'] = total_debit
            promoted['Strategy_Risk'] = risk_per_contract
            return promoted
    
    elif 'Iron Condor' in strategy or 'Iron Butterfly' in strategy:
        # Promote SHORT PUT (credit center, often has higher liquidity than call side)
        # For iron condor: [long_put, short_put, short_call, long_call]
        short_put = None
        for s in symbols:
            if s['Option_Type'].lower() == 'put':
                # Short put has higher strike than long put
                if short_put is None or s['Strike'] > short_put['Strike']:
                    short_put = s
        
        if short_put:
            promoted = short_put.copy()
            promoted['Promotion_Reason'] = 'Iron Condor Short Put (Credit Center)'
            promoted['Strategy_Credit'] = total_credit
            promoted['Strategy_Risk'] = risk_per_contract
            return promoted
    
    elif 'Straddle' in strategy or 'Strangle' in strategy:
        # Promote strike with HIGHEST VEGA (volatility exposure driver)
        # Sinclair: "Straddle value is driven by volatility expansion"
        best_vega_strike = max(symbols, key=lambda s: abs(s.get('Vega', 0)))
        promoted = best_vega_strike.copy()
        promoted['Promotion_Reason'] = f'{strategy} - Highest Vega Strike (Vol Exposure)'
        promoted['Strategy_Debit'] = total_debit
        return promoted
    
    elif 'Covered Call' in strategy:
        # Promote the call (only option leg)
        for s in symbols:
            if s['Option_Type'].lower() == 'call':
                promoted = s.copy()
                promoted['Promotion_Reason'] = 'Covered Call - Short Call Strike'
                promoted['Strategy_Credit'] = total_credit
                return promoted
    
    elif 'Long Call' in strategy or 'Long Put' in strategy:
        # Single directional leg
        promoted = symbols[0].copy()
        promoted['Promotion_Reason'] = f'{strategy} - Directional Strike'
        promoted['Strategy_Debit'] = total_debit
        return promoted
    
    # Fallback: promote first strike with reason
    promoted = symbols[0].copy()
    promoted['Promotion_Reason'] = f'{strategy} - Default Promotion (First Strike)'
    return promoted


def _build_contract_with_greeks(contract_row: pd.Series) -> Dict:
    """
    Build a fully materialized contract object from an option chain row.
    
    CRITICAL: This function normalizes pricing fields, attaches Greeks, and returns
    a complete contract object with all required fields for downstream validation.
    
    Args:
        contract_row: Single row from option chain DataFrame
    
    Returns:
        Dictionary with all required contract fields:
        - Contract_Symbol, Ticker, Option_Type, Strike, Expiration, DTE
        - Mid_Price, Bid, Ask, Spread_Pct, OI
        - Delta, Gamma, Vega, Theta
    
    If Greeks are missing, returns proxy values (never silently defaults to None).
    """
    try:
        # Extract core fields
        symbol = contract_row.get('symbol', '')
        option_type = contract_row.get('option_type', '').lower()
        strike = float(contract_row.get('strike', 0))
        expiration = contract_row.get('expiration', '')
        
        # Pricing fields (normalize bid/ask → mid)
        bid = float(contract_row.get('bid', 0))
        ask = float(contract_row.get('ask', 0))
        mid_price = (bid + ask) / 2 if (bid > 0 and ask > 0) else float(contract_row.get('last', 0))
        spread_pct = float(contract_row.get('spread_pct', 100.0))
        
        # Liquidity fields
        open_interest = int(contract_row.get('open_interest', 0))
        volume = int(contract_row.get('volume', 0))
        
        # Greeks - CRITICAL: Must exist and be numeric
        # If missing, use proxy values based on option type and moneyness
        delta = contract_row.get('delta', np.nan)
        gamma = contract_row.get('gamma', np.nan)
        vega = contract_row.get('vega', np.nan)
        theta = contract_row.get('theta', np.nan)
        
        # Proxy Greeks if missing (better than NaN for validation)
        underlying_price = contract_row.get('underlying_price', contract_row.get('underlying', strike))
        if pd.isna(delta):
            # Proxy: Calls have positive delta, puts negative
            # Moneyness affects magnitude (ATM ≈ ±0.50)
            moneyness = strike / underlying_price if underlying_price > 0 else 1.0
            if option_type == 'call':
                delta = 0.50 if 0.95 < moneyness < 1.05 else (0.70 if moneyness < 0.95 else 0.30)
            else:  # put
                delta = -0.50 if 0.95 < moneyness < 1.05 else (-0.30 if moneyness < 0.95 else -0.70)
        
        if pd.isna(gamma):
            # Proxy: ATM options have highest gamma (≈0.05), decays away from ATM
            moneyness = strike / underlying_price if underlying_price > 0 else 1.0
            gamma = 0.05 if 0.95 < moneyness < 1.05 else 0.02
        
        if pd.isna(vega):
            # Proxy: ATM options have highest vega (≈0.15), decays away from ATM
            moneyness = strike / underlying_price if underlying_price > 0 else 1.0
            vega = 0.15 if 0.95 < moneyness < 1.05 else 0.08
        
        if pd.isna(theta):
            # Proxy: ATM options have highest theta decay (negative)
            # Short-term: -0.05, Long-term: -0.01
            try:
                dte = (pd.to_datetime(expiration) - datetime.now()).days
                theta = -0.05 if dte < 30 else -0.02 if dte < 90 else -0.01
            except:
                theta = -0.03  # Default
        
        # Calculate DTE
        try:
            dte = (pd.to_datetime(expiration) - datetime.now()).days
        except:
            dte = 45  # Default fallback
        
        # Extract ticker from symbol (format: AAPL250117C00150000)
        ticker = symbol[:symbol.index(str(expiration).replace('-', '')[:6])] if expiration in symbol else ''
        if not ticker:
            # Fallback: extract from contract_row if available
            ticker = contract_row.get('root_symbol', contract_row.get('underlying_symbol', ''))
        
        return {
            'Contract_Symbol': symbol,
            'Ticker': ticker,
            'Strategy_Name': '',  # Will be filled by caller
            'Option_Type': option_type.capitalize(),
            'Strike': strike,
            'Expiration': expiration,
            'DTE': dte,
            'Actual_DTE': dte,
            'Mid_Price': mid_price,
            'Bid': bid,
            'Ask': ask,
            'Last': float(contract_row.get('last', mid_price)),
            'Spread_Pct': spread_pct,
            'Open_Interest': open_interest,
            'OI': open_interest,  # Alias
            'Volume': volume,
            'Delta': float(delta),
            'Gamma': float(gamma),
            'Vega': float(vega),
            'Theta': float(theta),
            'IV': float(contract_row.get('mid_iv', 0)),
            'Underlying_Price': float(underlying_price)
        }
        
    except Exception as e:
        logger.error(f"Failed to build contract from row: {e}", exc_info=True)
        # Return minimal contract to avoid blocking pipeline
        return {
            'Contract_Symbol': contract_row.get('symbol', 'UNKNOWN'),
            'Ticker': '',
            'Strategy_Name': '',
            'Option_Type': contract_row.get('option_type', 'call').capitalize(),
            'Strike': float(contract_row.get('strike', 0)),
            'Expiration': contract_row.get('expiration', ''),
            'DTE': 45,
            'Actual_DTE': 45,
            'Mid_Price': 0.0,
            'Bid': 0.0,
            'Ask': 0.0,
            'Last': 0.0,
            'Spread_Pct': 100.0,
            'Open_Interest': 0,
            'OI': 0,
            'Volume': 0,
            'Delta': 0.0,
            'Gamma': 0.0,
            'Vega': 0.0,
            'Theta': 0.0,
            'IV': 0.0,
            'Underlying_Price': 0.0
        }


def _select_strikes_for_strategy(
    chain_df: pd.DataFrame,
    strategy: str,
    trade_bias: str,
    num_contracts: int,
    actual_dte: int = 45,  # FIX 3: Pass actual DTE for LEAPS adjustments
    underlying_price: float = None  # NEW: For POP calculation
) -> Optional[Dict]:
    """
    Select strikes based on strategy characteristics (flexible, not rigid mapping).
    
    DESIGN: Uses strategy characteristics, not rigid leg counts.
    Allows same strategy to use different structures based on market conditions.
    
    FIX 3: DTE-aware strike selection - LEAPS prefer deeper ITM / higher delta.
    RAG: "LEAPS buyers combat theta erosion with slower decay rate"
    """
    
    # ISSUE 2 FIX: Get underlying price from chain data (not median strike)
    # Median strike != ATM strike (skewed chains will break strategy logic)
    atm_strike = None
    if 'underlying' in chain_df.columns and not chain_df['underlying'].isna().all():
        atm_strike = float(chain_df['underlying'].iloc[0])
    elif 'underlying_price' in chain_df.columns and not chain_df['underlying_price'].isna().all():
        atm_strike = float(chain_df['underlying_price'].iloc[0])
    else:
        # Fallback: closest strike to what would be ATM (use midpoint of all strikes)
        logger.warning("No underlying_price in chain data, using strike midpoint as fallback")
        atm_strike = float(chain_df['strike'].median())
    
    # Validate atm_strike is numeric
    if pd.isna(atm_strike) or atm_strike <= 0:
        logger.error(f"Invalid ATM strike: {atm_strike}", exc_info=True)
        return None
    
    # FIX 3: Determine if this is LEAPS territory
    is_leaps = actual_dte >= 90
    
    # Separate calls and puts
    calls = chain_df[chain_df['option_type'] == 'call'].copy()
    puts = chain_df[chain_df['option_type'] == 'put'].copy()
    
    if calls.empty or puts.empty:
        return None
    
    # Strategy-based strike selection (FLEXIBLE, not rigid)
    if 'Credit Spread' in strategy:
        # Sell OTM, buy further OTM for protection
        return _select_credit_spread_strikes(calls, puts, trade_bias, atm_strike, num_contracts, is_leaps, actual_dte, underlying_price)
    
    elif 'Debit Spread' in strategy or 'Vertical' in strategy:
        # Buy ITM/ATM, sell OTM
        return _select_debit_spread_strikes(calls, puts, trade_bias, atm_strike, num_contracts, is_leaps, actual_dte)
    
    elif 'Iron Condor' in strategy or 'Iron Butterfly' in strategy:
        # Symmetric credit structure
        return _select_iron_condor_strikes(calls, puts, atm_strike, num_contracts, is_leaps, actual_dte)
    
    elif 'Straddle' in strategy:
        # ATM call + ATM put
        return _select_straddle_strikes(calls, puts, atm_strike, num_contracts, is_leaps, actual_dte, underlying_price)
    
    elif 'Strangle' in strategy:
        # OTM call + OTM put
        return _select_strangle_strikes(calls, puts, atm_strike, num_contracts, is_leaps, actual_dte)
    
    elif 'Calendar' in strategy or 'Diagonal' in strategy:
        # Different expirations - REJECT unless explicitly approved
        return _select_calendar_strikes(calls, puts, trade_bias, atm_strike, num_contracts, is_leaps, allow_multi_expiry=False)
    
    elif 'Covered Call' in strategy:
        # OTM call only
        return _select_covered_call_strikes(calls, atm_strike, num_contracts, is_leaps, actual_dte)
    
    elif 'Long Call' in strategy or 'Long Put' in strategy:
        # Single leg directional
        return _select_single_leg_strikes(calls, puts, trade_bias, atm_strike, num_contracts, is_leaps, actual_dte)
    
    else:
        # Default: attempt directional based on bias
        return _select_single_leg_strikes(calls, puts, trade_bias, atm_strike, num_contracts, is_leaps, actual_dte)


def _select_credit_spread_strikes(calls, puts, bias, atm, num_contracts, is_leaps=False, actual_dte=45, underlying_price=None) -> Dict:
    """
    Select strikes for credit spreads (sell OTM, buy further OTM).
    
    FIX 3: LEAPS adjustment - use wider strikes for more premium.
    """
    # FIX 3: LEAPS prefer wider strikes (more intrinsic value)
    otm_threshold = 0.92 if is_leaps else 0.95
    
    if bias == 'Bullish':
        # Put credit spread: Sell higher put, buy lower put
        otm_puts = puts[puts['strike'] < atm * otm_threshold].sort_values('strike', ascending=False)
        if len(otm_puts) < 2:
            return None
        
        short_put = otm_puts.iloc[0]
        long_put = otm_puts.iloc[1]
        
        credit = float(short_put['bid']) - float(long_put['ask'])
        max_risk = (float(short_put['strike']) - float(long_put['strike'])) - credit
        
        # Calculate POP for credit spread (probability price stays above short strike)
        # Cohen: "Income strategies require POP ≥65% for proper risk/reward"
        probability_of_profit = np.nan
        if underlying_price and actual_dte > 0:
            short_iv = short_put.get('mid_iv', 0)
            if short_iv > 0:
                try:
                    from utils.option_math import calculate_probability_of_profit
                    probability_of_profit = calculate_probability_of_profit(
                        underlying_price=underlying_price,
                        strike=float(short_put['strike']),
                        days_to_expiration=actual_dte,
                        volatility=short_iv / 100.0 if short_iv > 1 else short_iv,
                        option_type='call'  # Inverse: prob above = 1 - prob below
                    )
                except Exception as e:
                    logger.debug(f"POP calculation failed for put credit spread: {e}", exc_info=True)
        
        # Build all contracts and promote one for UI/execution
        all_contracts = [_build_contract_with_greeks(short_put), _build_contract_with_greeks(long_put)]
        total_credit_val = credit * 100 * num_contracts
        risk_per_contract_val = max_risk * 100
        
        promoted_strike = _promote_best_strike(
            symbols=all_contracts,
            strategy='Credit Spread',
            bias=bias,
            underlying_price=atm,
            total_credit=total_credit_val,
            risk_per_contract=risk_per_contract_val
        )
        
        return {
            'strikes': [float(short_put['strike']), float(long_put['strike'])],
            'symbols': all_contracts,  # All legs (debug only)
            'promoted_strike': promoted_strike,  # Single strike for UI/execution
            'risk_per_contract': risk_per_contract_val,
            'total_credit': total_credit_val,
            'total_debit': 0.0,
            'avg_spread_pct': (float(short_put['spread_pct']) + float(long_put['spread_pct'])) / 2,
            'total_oi': int(short_put['open_interest']) + int(long_put['open_interest']),
            'liquidity_score': min(_calculate_liquidity_score(int(short_put['open_interest']), float(short_put['spread_pct']), int(short_put.get('volume', 0)), actual_dte), _calculate_liquidity_score(int(long_put['open_interest']), float(long_put['spread_pct']), int(long_put.get('volume', 0)), actual_dte)),
            'risk_model': 'Credit_Max',  # FIX 4: Defined max risk (spread width - credit)
            'probability_of_profit': probability_of_profit
        }
    
    elif bias == 'Bearish':
        # Call credit spread: Sell lower call, buy higher call
        otm_calls = calls[calls['strike'] > atm * 1.05].sort_values('strike')
        if len(otm_calls) < 2:
            return None
        
        short_call = otm_calls.iloc[0]
        long_call = otm_calls.iloc[1]
        
        credit = float(short_call['bid']) - float(long_call['ask'])
        max_risk = (float(long_call['strike']) - float(short_call['strike'])) - credit
        
        # Calculate POP for call credit spread (probability price stays below short strike)
        probability_of_profit = np.nan
        if underlying_price and actual_dte > 0:
            short_iv = short_call.get('mid_iv', 0)
            if short_iv > 0:
                try:
                    from utils.option_math import calculate_probability_of_profit
                    probability_of_profit = calculate_probability_of_profit(
                        underlying_price=underlying_price,
                        strike=float(short_call['strike']),
                        days_to_expiration=actual_dte,
                        volatility=short_iv / 100.0 if short_iv > 1 else short_iv,
                        option_type='put'  # Inverse: prob below
                    )
                except Exception as e:
                    logger.debug(f"POP calculation failed for call credit spread: {e}", exc_info=True)
        
        # Build all contracts and promote one for UI/execution
        all_contracts = [_build_contract_with_greeks(short_call), _build_contract_with_greeks(long_call)]
        total_credit_val = credit * 100 * num_contracts
        risk_per_contract_val = max_risk * 100
        
        promoted_strike = _promote_best_strike(
            symbols=all_contracts,
            strategy='Credit Spread',
            bias=bias,
            underlying_price=atm,
            total_credit=total_credit_val,
            risk_per_contract=risk_per_contract_val
        )
        
        return {
            'strikes': [float(short_call['strike']), float(long_call['strike'])],
            'symbols': all_contracts,  # All legs (debug only)
            'promoted_strike': promoted_strike,  # Single strike for UI/execution
            'risk_per_contract': risk_per_contract_val,
            'total_credit': total_credit_val,
            'total_debit': 0.0,
            'avg_spread_pct': (float(short_call['spread_pct']) + float(long_call['spread_pct'])) / 2,
            'total_oi': int(short_call['open_interest']) + int(long_call['open_interest']),
            'liquidity_score': min(_calculate_liquidity_score(int(short_call['open_interest']), float(short_call['spread_pct']), int(short_call.get('volume', 0)), actual_dte), _calculate_liquidity_score(int(long_call['open_interest']), float(long_call['spread_pct']), int(long_call.get('volume', 0)), actual_dte)),
            'risk_model': 'Credit_Max',  # FIX 4
            'probability_of_profit': probability_of_profit
        }
    
    else:
        # Neutral: default to put credit spread
        return _select_credit_spread_strikes(calls, puts, 'Bullish', atm, num_contracts, is_leaps, actual_dte, underlying_price)


def _select_debit_spread_strikes(calls, puts, bias, atm, num_contracts, is_leaps=False, actual_dte=45) -> Dict:
    """
    Select strikes for debit spreads.
    
    FIX 3: LEAPS prefer deeper ITM (more intrinsic value, less theta risk).
    RAG: "LEAPS help combat theta erosion with slower decay rate"
    """
    if bias == 'Bullish':
        # Call debit spread
        # FIX 3: LEAPS go deeper ITM for more intrinsic value
        itm_threshold = atm * 0.95 if is_leaps else atm
        itm_calls = calls[calls['strike'] <= itm_threshold].sort_values('strike', ascending=False)
        otm_calls = calls[calls['strike'] > atm].sort_values('strike')
        
        if itm_calls.empty or otm_calls.empty:
            return None
        
        long_call = itm_calls.iloc[0]
        short_call = otm_calls.iloc[0]
        
        debit = float(long_call['ask']) - float(short_call['bid'])
        max_profit = (float(short_call['strike']) - float(long_call['strike'])) - debit
        
        all_contracts = [_build_contract_with_greeks(long_call), _build_contract_with_greeks(short_call)]
        risk_per_contract_val = debit * 100
        total_debit_val = debit * 100 * num_contracts
        
        promoted_strike = _promote_best_strike(
            symbols=all_contracts,
            strategy='Debit Spread',
            bias='Bullish',
            underlying_price=atm,
            total_debit=total_debit_val,
            risk_per_contract=risk_per_contract_val
        )
        
        return {
            'strikes': [float(long_call['strike']), float(short_call['strike'])],
            'symbols': all_contracts,
            'promoted_strike': promoted_strike,
            'risk_per_contract': risk_per_contract_val,
            'total_debit': total_debit_val,
            'total_credit': 0.0,
            'avg_spread_pct': (float(long_call['spread_pct']) + float(short_call['spread_pct'])) / 2,
            'total_oi': int(long_call['open_interest']) + int(short_call['open_interest']),
            'liquidity_score': min(_calculate_liquidity_score(int(long_call['open_interest']), float(long_call['spread_pct']), int(long_call.get('volume', 0)), actual_dte), _calculate_liquidity_score(int(short_call['open_interest']), float(short_call['spread_pct']), int(short_call.get('volume', 0)), actual_dte)),
            'risk_model': 'Debit_Max'  # FIX 4
        }
    
    else:
        # Put debit spread
        itm_threshold = atm * 1.05 if is_leaps else atm
        itm_puts = puts[puts['strike'] >= itm_threshold].sort_values('strike')
        otm_puts = puts[puts['strike'] < atm].sort_values('strike', ascending=False)
        
        if itm_puts.empty or otm_puts.empty:
            return None
        
        long_put = itm_puts.iloc[0]
        short_put = otm_puts.iloc[0]
        
        debit = float(long_put['ask']) - float(short_put['bid'])
        
        all_contracts = [_build_contract_with_greeks(long_put), _build_contract_with_greeks(short_put)]
        risk_per_contract_val = debit * 100
        total_debit_val = debit * 100 * num_contracts
        
        promoted_strike = _promote_best_strike(
            symbols=all_contracts,
            strategy='Debit Spread',
            bias='Bearish',
            underlying_price=atm,
            total_debit=total_debit_val,
            risk_per_contract=risk_per_contract_val
        )
        
        return {
            'strikes': [float(long_put['strike']), float(short_put['strike'])],
            'symbols': all_contracts,
            'promoted_strike': promoted_strike,
            'risk_per_contract': risk_per_contract_val,
            'total_debit': total_debit_val,
            'total_credit': 0.0,
            'avg_spread_pct': (float(long_put['spread_pct']) + float(short_put['spread_pct'])) / 2,
            'risk_model': 'Debit_Max',  # FIX 4
            'total_oi': int(long_put['open_interest']) + int(short_put['open_interest']),
            'liquidity_score': min(_calculate_liquidity_score(int(long_put['open_interest']), float(long_put['spread_pct']), int(long_put.get('volume', 0)), actual_dte), _calculate_liquidity_score(int(short_put['open_interest']), float(short_put['spread_pct']), int(short_put.get('volume', 0)), actual_dte))
        }


def _select_iron_condor_strikes(calls, puts, atm, num_contracts, is_leaps=False, actual_dte=45) -> Dict:
    """Select strikes for iron condor (4-leg structure)."""
    # Put spread: Sell higher put, buy lower put
    otm_puts = puts[puts['strike'] < atm * 0.95].sort_values('strike', ascending=False)
    # Call spread: Sell lower call, buy higher call
    otm_calls = calls[calls['strike'] > atm * 1.05].sort_values('strike')
    
    if len(otm_puts) < 2 or len(otm_calls) < 2:
        return None
    
    short_put = otm_puts.iloc[0]
    long_put = otm_puts.iloc[1]
    short_call = otm_calls.iloc[0]
    long_call = otm_calls.iloc[1]
    
    total_credit = (float(short_put['bid']) + float(short_call['bid'])) - (float(long_put['ask']) + float(long_call['ask']))
    put_width = float(short_put['strike']) - float(long_put['strike'])
    call_width = float(long_call['strike']) - float(short_call['strike'])
    max_risk = max(put_width, call_width) - total_credit
    
    all_contracts = [_build_contract_with_greeks(long_put), _build_contract_with_greeks(short_put), _build_contract_with_greeks(short_call), _build_contract_with_greeks(long_call)]
    risk_per_contract_val = max_risk * 100
    total_credit_val = total_credit * 100 * num_contracts
    
    promoted_strike = _promote_best_strike(
        symbols=all_contracts,
        strategy='Iron Condor',
        bias='Neutral',
        underlying_price=atm,
        total_credit=total_credit_val,
        risk_per_contract=risk_per_contract_val
    )
    
    return {
        'strikes': [float(long_put['strike']), float(short_put['strike']), float(short_call['strike']), float(long_call['strike'])],
        'symbols': all_contracts,
        'promoted_strike': promoted_strike,
        'risk_per_contract': risk_per_contract_val,
        'total_credit': total_credit_val,
        'total_debit': 0.0,
        'avg_spread_pct': np.mean([float(long_put['spread_pct']), float(short_put['spread_pct']), 
                                     float(short_call['spread_pct']), float(long_call['spread_pct'])]),
        'total_oi': sum([int(long_put['open_interest']), int(short_put['open_interest']), 
                         int(short_call['open_interest']), int(long_call['open_interest'])]),
        'risk_model': 'Credit_Max',
        'liquidity_score': min([
                _calculate_liquidity_score(int(long_put['open_interest']), float(long_put['spread_pct']), int(long_put.get('volume', 0)), actual_dte),
                _calculate_liquidity_score(int(short_put['open_interest']), float(short_put['spread_pct']), int(short_put.get('volume', 0)), actual_dte),
                _calculate_liquidity_score(int(short_call['open_interest']), float(short_call['spread_pct']), int(short_call.get('volume', 0)), actual_dte),
                _calculate_liquidity_score(int(long_call['open_interest']), float(long_call['spread_pct']), int(long_call.get('volume', 0)), actual_dte)
            ])
    }


def _select_straddle_strikes(calls, puts, atm, num_contracts, is_leaps=False, actual_dte=45, underlying_price=None) -> Dict:
    """Select ATM call + ATM put for straddle."""
    # Convert strike and atm to float for safe subtraction
    calls_copy = calls.copy()
    puts_copy = puts.copy()
    calls_copy['strike'] = calls_copy['strike'].astype(float)
    puts_copy['strike'] = puts_copy['strike'].astype(float)
    atm = float(atm)
    
    atm_calls = calls_copy.iloc[(calls_copy['strike'] - atm).abs().argsort()[:1]]
    atm_puts = puts_copy.iloc[(puts_copy['strike'] - atm).abs().argsort()[:1]]
    
    if atm_calls.empty or atm_puts.empty:
        return None
    
    call = atm_calls.iloc[0]
    put = atm_puts.iloc[0]
    
    debit = float(call['ask']) + float(put['ask'])
    
    # Calculate Put/Call Skew (CRITICAL for volatility strategies)
    # RAG: "Skew indicates market fear. Low skew (≈1.0) = ideal for straddles"
    # Audit: "CRITICAL Missing - 35% straddle overpricing undetected without skew"
    # Formula: skew = put_iv_atm / call_iv_atm
    # Thresholds: <1.10 = low skew (good), >1.20 = high skew (reject straddles)
    put_call_skew = np.nan
    call_iv = call.get('mid_iv', 0)
    put_iv = put.get('mid_iv', 0)
    if 'mid_iv' in call and 'mid_iv' in put and call_iv and put_iv and call_iv > 0:
        put_call_skew = put_iv / call_iv
    
    # Calculate Probability of Profit (POP) for straddle
    # Cohen: "Without POP, you're buying insurance without knowing the odds"
    # Formula: POP = P(S > K + premium) + P(S < K - premium) at expiration
    # Typical straddle POP = 30-40% (low), needs vol edge to justify
    probability_of_profit = np.nan
    if underlying_price and call_iv > 0 and actual_dte > 0:
        try:
            from utils.option_math import calculate_pop_for_straddle
            probability_of_profit = calculate_pop_for_straddle(
                underlying_price=underlying_price,
                strike=float(call['strike']),
                days_to_expiration=actual_dte,
                volatility=call_iv / 100.0 if call_iv > 1 else call_iv,  # Convert % to decimal
                total_premium=debit
            )
        except Exception as e:
            logger.debug(f"POP calculation failed for straddle: {e}", exc_info=True)
    
    all_contracts = [_build_contract_with_greeks(call), _build_contract_with_greeks(put)]
    risk_per_contract_val = debit * 100
    total_debit_val = debit * 100 * num_contracts
    
    promoted_strike = _promote_best_strike(
        symbols=all_contracts,
        strategy='Straddle',
        bias='Neutral',
        underlying_price=atm,
        total_debit=total_debit_val,
        risk_per_contract=risk_per_contract_val
    )
    
    return {
        'strikes': [float(call['strike']), float(put['strike'])],
        'symbols': all_contracts,
        'promoted_strike': promoted_strike,
        'risk_per_contract': risk_per_contract_val,
        'total_debit': total_debit_val,
        'total_credit': 0.0,
        'avg_spread_pct': (float(call['spread_pct']) + float(put['spread_pct'])) / 2,
        'total_oi': int(call['open_interest']) + int(put['open_interest']),
        'risk_model': 'Debit_Max',
        'liquidity_score': min(_calculate_liquidity_score(int(call['open_interest']), float(call['spread_pct']), int(call.get('volume', 0)), actual_dte), _calculate_liquidity_score(int(put['open_interest']), float(put['spread_pct']), int(put.get('volume', 0)), actual_dte)),
        'put_call_skew': put_call_skew,  # NEW: Critical for Step 11 validation
        'probability_of_profit': probability_of_profit  # NEW: Win rate for vol strategies
    }


def _select_strangle_strikes(calls, puts, atm, num_contracts, is_leaps=False, actual_dte=45) -> Dict:
    """Select OTM call + OTM put for strangle."""
    atm = float(atm)
    otm_calls = calls[calls['strike'] > atm * 1.05].sort_values('strike')
    otm_puts = puts[puts['strike'] < atm * 0.95].sort_values('strike', ascending=False)
    
    if otm_calls.empty or otm_puts.empty:
        return None
    
    call = otm_calls.iloc[0]
    put = otm_puts.iloc[0]
    
    debit = float(call['ask']) + float(put['ask'])
    
    all_contracts = [_build_contract_with_greeks(call), _build_contract_with_greeks(put)]
    risk_per_contract_val = debit * 100
    total_debit_val = debit * 100 * num_contracts
    
    promoted_strike = _promote_best_strike(
        symbols=all_contracts,
        strategy='Strangle',
        bias='Neutral',
        underlying_price=atm,
        total_debit=total_debit_val,
        risk_per_contract=risk_per_contract_val
    )
    
    return {
        'strikes': [float(call['strike']), float(put['strike'])],
        'symbols': all_contracts,
        'promoted_strike': promoted_strike,
        'risk_per_contract': risk_per_contract_val,
        'total_debit': total_debit_val,
        'total_credit': 0.0,
        'avg_spread_pct': (float(call['spread_pct']) + float(put['spread_pct'])) / 2,
        'total_oi': int(call['open_interest']) + int(put['open_interest']),
        'risk_model': 'Debit_Max',
        'liquidity_score': min(_calculate_liquidity_score(int(call['open_interest']), float(call['spread_pct']), int(call.get('volume', 0)), actual_dte), _calculate_liquidity_score(int(put['open_interest']), float(put['spread_pct']), int(put.get('volume', 0)), actual_dte))
    }


def _select_calendar_strikes(calls, puts, bias, atm, num_contracts, is_leaps=False, allow_multi_expiry=False) -> Dict:
    """
    Select strikes for calendar spread.
    
    ISSUE 1 FIX: Calendar/Diagonal strategies require multiple expirations.
    Step 9B must NOT invent or approximate multi-expiration strategies.
    
    Required rule:
    If Allow_Multi_Expiry is not True → REJECT (return None).
    Multi-expiration logic belongs to a future dedicated module.
    """
    if not allow_multi_expiry:
        logger.warning("Calendar/Diagonal strategy REJECTED: requires Allow_Multi_Expiry=True (multi-expiration logic not implemented)")
        return None  # REJECT - do not approximate
    
    # If approved upstream, use simplified placeholder
    logger.warning("Calendar/Diagonal strategy simplified to debit spread (single expiration) - Allow_Multi_Expiry=True")
    result = _select_debit_spread_strikes(calls, puts, bias, atm, num_contracts, is_leaps, actual_dte=45)
    if result:
        result['structure_simplified'] = True  # Flag for downstream awareness
    return result


def _select_covered_call_strikes(calls, atm, num_contracts, is_leaps=False, actual_dte=45) -> Dict:
    """Select OTM call for covered call."""
    otm_calls = calls[calls['strike'] > atm * 1.05].sort_values('strike')
    
    if otm_calls.empty:
        return None
    
    call = otm_calls.iloc[0]
    
    # ISSUE 2 FIX: Covered calls have stock-dependent risk (capped upside, full downside via stock)
    all_contracts = [_build_contract_with_greeks(call)]
    total_credit_val = call['bid'] * 100 * num_contracts
    
    promoted_strike = _promote_best_strike(
        symbols=all_contracts,
        strategy='Covered Call',
        bias='Neutral',
        underlying_price=atm,
        total_credit=total_credit_val,
        risk_per_contract=0.0
    )
    
    return {
        'strikes': [call['strike']],
        'symbols': all_contracts,
        'promoted_strike': promoted_strike,
        'risk_per_contract': None,  # Stock-dependent, not zero
        'total_credit': total_credit_val,
        'total_debit': 0.0,
        'avg_spread_pct': call['spread_pct'],
        'total_oi': call['open_interest'],
        'risk_model': 'Stock_Dependent',  # Not Undefined - requires stock position
        'liquidity_score': _calculate_liquidity_score(
            call['open_interest'],
            call['spread_pct'],
            call.get('volume', 0)
        )
    }


def _select_single_leg_strikes(calls, puts, bias, atm, num_contracts, is_leaps=False, actual_dte=45) -> Dict:
    """
    Select single leg for directional trades.
    
    ISSUE 4 FIX: LEAPS (DTE >= 120) prefer deeper ITM strikes (delta ~0.60+).
    Short-term (DTE < 120) can use ATM or slightly OTM.
    """
    atm = float(atm)
    
    if bias == 'Bullish':
        # ISSUE 4: DTE-conditional strike selection
        if actual_dte >= 120:
            # LEAPS: Prefer deeper ITM (higher delta, more intrinsic value, lower theta)
            target_calls = calls[calls['strike'] <= atm * 0.92].sort_values('strike', ascending=False)
            if target_calls.empty:
                # Fallback if no deep ITM available
                target_calls = calls[calls['strike'] <= atm].sort_values('strike', ascending=False)
        else:
            # Short-term: ATM or slightly OTM acceptable
            target_calls = calls[calls['strike'] >= atm * 0.98].sort_values('strike')
        
        if target_calls.empty:
            return None
        
        call = target_calls.iloc[0]
        
        all_contracts = [_build_contract_with_greeks(call)]
        risk_per_contract_val = float(call['ask']) * 100
        total_debit_val = float(call['ask']) * 100 * num_contracts
        
        promoted_strike = _promote_best_strike(
            symbols=all_contracts,
            strategy='Single Leg',
            bias='Bullish',
            underlying_price=atm,
            total_debit=total_debit_val,
            risk_per_contract=risk_per_contract_val
        )
        
        return {
            'strikes': [float(call['strike'])],
            'symbols': all_contracts,
            'promoted_strike': promoted_strike,
            'risk_per_contract': risk_per_contract_val,
            'total_debit': total_debit_val,
            'total_credit': 0.0,
            'avg_spread_pct': float(call['spread_pct']),
            'total_oi': int(call['open_interest']),
            'risk_model': 'Debit_Max',
            'liquidity_score': _calculate_liquidity_score(int(call['open_interest']), float(call['spread_pct']), int(call.get('volume', 0)), actual_dte)
        }
    
    else:  # Bearish or Neutral
        # ISSUE 4: DTE-conditional strike selection for puts
        if actual_dte >= 120:
            # LEAPS: Prefer deeper ITM puts (higher delta, more intrinsic value)
            target_puts = puts[puts['strike'] >= atm * 1.08].sort_values('strike')
            if target_puts.empty:
                # Fallback if no deep ITM available
                target_puts = puts[puts['strike'] >= atm].sort_values('strike')
        else:
            # Short-term: ATM or slightly OTM acceptable
            target_puts = puts[puts['strike'] <= atm * 1.02].sort_values('strike', ascending=False)
        
        if target_puts.empty:
            return None
        
        put = target_puts.iloc[0]
        
        all_contracts = [_build_contract_with_greeks(put)]
        risk_per_contract_val = float(put['ask']) * 100
        total_debit_val = float(put['ask']) * 100 * num_contracts
        
        promoted_strike = _promote_best_strike(
            symbols=all_contracts,
            strategy='Single Leg',
            bias='Bearish',
            underlying_price=atm,
            total_debit=total_debit_val,
            risk_per_contract=risk_per_contract_val
        )
        
        return {
            'strikes': [float(put['strike'])],
            'symbols': all_contracts,
            'promoted_strike': promoted_strike,
            'risk_per_contract': risk_per_contract_val,
            'total_debit': total_debit_val,
            'total_credit': 0.0,
            'avg_spread_pct': float(put['spread_pct']),
            'total_oi': int(put['open_interest']),
            'risk_model': 'Debit_Max',
            'liquidity_score': _calculate_liquidity_score(int(put['open_interest']), float(put['spread_pct']), int(put.get('volume', 0)), actual_dte)
        }


def _audit_multi_contract_tickers(df: pd.DataFrame):
    """
    Audit and log tickers with multiple contract selections (multi-DTE architecture).
    
    This validates the strategy-aware architecture where same ticker can have
    multiple contracts with different expirations/strikes.
    
    Example:
      AAPL: 3 strategies, 3 contract selections
        - Long Call (DTE=42): Exp 2025-02-14, Strike 150C
        - Long Straddle (DTE=52): Exp 2025-02-28, Strike 150C+150P
        - Buy-Write (DTE=37): Exp 2025-02-07, Strike 155C
    """
    # Find tickers with multiple successful contract selections
    successful = df[df['Contract_Selection_Status'] == 'Success']
    
    if len(successful) == 0:
        logger.warning("⚠️ No successful contract selections to audit")
        return
    
    ticker_counts = successful.groupby('Ticker').size()
    multi_contract_tickers = ticker_counts[ticker_counts > 1]
    
    if len(multi_contract_tickers) > 0:
        logger.info(f"📊 Multi-Contract Architecture: {len(multi_contract_tickers)} tickers have multiple contracts")
        
        # Show examples (top 5 tickers with most contracts)
        for ticker in list(multi_contract_tickers.sort_values(ascending=False).head(5).index):
            ticker_data = successful[successful['Ticker'] == ticker]
            logger.info(f"   {ticker}: {len(ticker_data)} contracts")
            
            for _, row in ticker_data.iterrows():
                strategy = row.get('Strategy_Name', row.get('Primary_Strategy', 'Unknown'))
                dte = row['Actual_DTE']
                exp = row['Selected_Expiration']
                strikes = row['Selected_Strikes']
                status = row['Contract_Selection_Status']
                
                logger.info(f"      - {strategy} (DTE={dte}): Exp {exp}, Strike {strikes}, Status={status}")
    else:
        logger.info("📊 No tickers with multiple contracts (single-strategy architecture)")
    
    # Summary stats
    total_tickers = successful['Ticker'].nunique()
    total_contracts = len(successful)
    avg_contracts_per_ticker = total_contracts / total_tickers if total_tickers > 0 else 0
    
    logger.info(f"📊 Contract Distribution:")
    logger.info(f"   Total tickers: {total_tickers}")
    logger.info(f"   Total contracts: {total_contracts}")
    logger.info(f"   Avg contracts/ticker: {avg_contracts_per_ticker:.2f}")
    logger.info(f"   Multi-contract tickers: {len(multi_contract_tickers)} ({len(multi_contract_tickers)/total_tickers*100:.1f}%)")


def _log_contract_selection_summary(df: pd.DataFrame):
    """Log summary of contract selection results."""
    status_dist = df['Contract_Selection_Status'].value_counts().to_dict()
    success_count = len(df[df['Contract_Selection_Status'] == 'Success'])
    
    logger.info(f"📊 Step 9B Summary:")
    logger.info(f"   Status distribution: {status_dist}")
    logger.info(f"   Successful selections: {success_count}/{len(df)}")
    
    if success_count > 0:
        successful = df[df['Contract_Selection_Status'] == 'Success']
        avg_dte = successful['Actual_DTE'].mean()
        avg_oi = successful['Open_Interest'].mean()
        avg_spread = successful['Bid_Ask_Spread_Pct'].mean()
        
        logger.info(f"   Average DTE: {avg_dte:.0f} days")
        logger.info(f"   Average OI: {avg_oi:.0f}")
        logger.info(f"   Average spread: {avg_spread:.2f}%")


def _infer_option_type(strategy: str, bias: str, symbols: List[str]) -> str:
    """
    Infer Option_Type (call/put) from strategy name, bias, and symbols.
    Required by Step 11 for PCS pairing logic.
    
    Args:
        strategy: Strategy name (e.g., "Put Credit Spread")
        bias: Trade bias (Bullish/Bearish/Neutral/Bidirectional)
        symbols: List of option symbols (e.g., ["AAPL260130P00250000", "AAPL260130P00245000"])
    
    Returns:
        str: "put", "call", or "mixed" (for multi-leg strategies with both)
    
    Logic:
        1. Check strategy name first (most reliable)
        2. Fall back to bias if strategy name unclear
        3. Parse symbols as last resort
    """
    strategy_lower = strategy.lower()
    
    # Explicit strategy names
    if 'put' in strategy_lower and 'call' not in strategy_lower:
        return 'put'
    elif 'call' in strategy_lower and 'put' not in strategy_lower:
        return 'call'
    
    # Multi-leg strategies with both calls and puts
    if any(kw in strategy_lower for kw in ['iron condor', 'iron butterfly', 'straddle', 'strangle']):
        return 'mixed'
    
    # Infer from bias
    if bias == 'Bullish':
        # Bullish typically uses calls (debit) or put credit spreads
        if 'credit' in strategy_lower:
            return 'put'  # Put credit spread (bullish)
        else:
            return 'call'  # Call debit spread or long call
    elif bias == 'Bearish':
        # Bearish typically uses puts (debit) or call credit spreads
        if 'credit' in strategy_lower:
            return 'call'  # Call credit spread (bearish)
        else:
            return 'put'  # Put debit spread or long put
    elif bias == 'Neutral':
        return 'mixed'  # Neutral strategies often use both
    
    # Last resort: parse option symbols
    if symbols and len(symbols) > 0:
        first_symbol = str(symbols[0])
        if 'P' in first_symbol and 'C' not in first_symbol:
            return 'put'
        elif 'C' in first_symbol and 'P' not in first_symbol:
            return 'call'
        else:
            return 'mixed'
    
    # Ultimate fallback
    return 'mixed'


if __name__ == "__main__":
    # Test with mock Step 9A output
    import sys
    logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
    
    # Note: This test will fail without valid Tradier token and live data
    test_df = pd.DataFrame({
        'Ticker': ['AAPL', 'SPY'],
        'Primary_Strategy': ['Put Credit Spread', 'Iron Condor'],
        'Strategy_Type': ['Mixed', 'Neutral'],
        'Trade_Bias': ['Bullish', 'Neutral'],
        'Confidence': [75, 70],
        'Min_DTE': [30, 45],
        'Max_DTE': [45, 60],
        'Preferred_DTE': [37, 52],
        'Num_Contracts': [2, 1],
        'Dollar_Allocation': [1500, 2500]
    })
    
    print("=== Step 9B Test (requires Tradier API) ===\n")
    print("Input:")
    print(test_df[['Ticker', 'Primary_Strategy', 'Min_DTE', 'Max_DTE']].to_string(index=False))
    
# Moved outside of if __name__ == "__main__": block
def fetch_and_select_contracts(df: pd.DataFrame, timeframes_df: pd.DataFrame) -> pd.DataFrame:
    """
    Public wrapper for Step 9B: Fetch option chains and select contracts.
    
    Args:
        df (pd.DataFrame): Input DataFrame from previous steps (e.g., evaluated strategies).
        timeframes_df (pd.DataFrame): DataFrame containing DTE ranges for each strategy.
    
    Returns:
        pd.DataFrame: Original DataFrame enriched with contract selection details.
    """
    logger.info("Starting fetch_and_select_contracts (Step 9B)...")
    
    # Merge timeframes with the main DataFrame
    df_merged = df.merge(timeframes_df[['Ticker', 'Strategy_Name', 'Min_DTE', 'Max_DTE', 'Target_DTE']],
                         on=['Ticker', 'Strategy_Name'],
                         how='left')
    
    # Initialize new columns
    output_columns = [
        'Exploration_Status', 'Strategy_Viable', 'Expirations_Available',
        'Selected_Expiration', 'Actual_DTE', 'Selected_Strikes', 'Contract_Symbols',
        'Option_Type', 'Underlying_Price', 'Liquidity_Grade', 'Spread_Assessment',
        'OI_Assessment', 'Bid_Ask_Spread_Pct', 'Open_Interest', 'Liquidity_Score',
        'Liquidity_Context', 'Capital_Required', 'Capital_Class', 'Within_Allocation',
        'Is_LEAP', 'Strategy_Category', 'Structure_Type', 'Reason_Flags',
        'Strategy_Notes', 'Actual_Risk_Per_Contract', 'Total_Debit', 'Total_Credit',
        'Risk_Model', 'Tradable', 'Tradable_Reason', 'Contract_Intent',
        'Put_Call_Skew', 'Probability_Of_Profit' # Added new columns
    ]
    for col in output_columns:
        if col not in df_merged.columns:
            df_merged[col] = np.nan # Initialize with NaN or appropriate default
    
    # Build chain cache once for all tickers
    chain_cache_data = _build_chain_cache(df_merged, TRADIER_TOKEN)
    
    processed_rows = []
    for index, row in df_merged.iterrows():
        ticker = row['Ticker']
        strategy = row['Strategy_Name']
        min_dte = row['Min_DTE']
        max_dte = row['Max_DTE']
        target_dte = row['Target_DTE']
        trade_bias = row['Trade_Bias']
        num_contracts = row.get('Num_Contracts', 1)
        dollar_allocation = row.get('Dollar_Allocation', 1000)
        
        # Get cached data for the ticker
        ticker_data = chain_cache_data.get(ticker)
        if not ticker_data or not ticker_data['chains']:
            row['Exploration_Status'] = 'No_Chains_Available'
            row['Strategy_Viable'] = False
            processed_rows.append(row)
            continue
        
        all_expirations = ticker_data['expirations']
        underlying_price = ticker_data['underlying_price']
        
        # Phase 1: Sampled Exploration (Fast Viability Check)
        phase1_result = _phase1_sampled_exploration(
            ticker, strategy, min_dte, max_dte, target_dte, underlying_price, TRADIER_TOKEN
        )
        
        if not phase1_result['deep_required']:
            # Fast reject or no viable expirations
            row['Exploration_Status'] = phase1_result['status']
            row['Strategy_Viable'] = False
            row['Tradable'] = False
            row['Tradable_Reason'] = phase1_result['reason']
            processed_rows.append(row)
            continue
        
        # Deep Exploration (using cached chains)
        best_expiration = _select_best_expiration_from_list(all_expirations, target_dte)
        
        if not best_expiration or best_expiration not in ticker_data['chains']:
            row['Exploration_Status'] = 'No_Suitable_Expiration'
            row['Strategy_Viable'] = False
            row['Tradable'] = False
            row['Tradable_Reason'] = "No suitable expiration found in cached chains"
            processed_rows.append(row)
            continue
        
        chain_df = ticker_data['chains'][best_expiration]
        actual_dte = (datetime.strptime(best_expiration, '%Y-%m-%d') - datetime.now()).days
        is_leap = actual_dte >= 365
        
        # Assess liquidity
        liquidity_assessment = _assess_liquidity_quality(chain_df, underlying_price, actual_dte, is_leap)
        
        # Select strikes for the strategy
        selected_strikes_data = _select_strikes_for_strategy(
            chain_df, strategy, trade_bias, num_contracts, actual_dte, underlying_price
        )
        
        if selected_strikes_data:
            row['Exploration_Status'] = 'Discovered'
            row['Strategy_Viable'] = True
            row['Selected_Expiration'] = best_expiration
            row['Actual_DTE'] = actual_dte
            row['Selected_Strikes'] = json.dumps(selected_strikes_data['strikes'])
            
            # Extract contract symbols and option type from the promoted strike
            promoted_strike = selected_strikes_data['promoted_strike']
            if promoted_strike:
                row['Contract_Symbols'] = json.dumps([c['Contract_Symbol'] for c in selected_strikes_data['symbols']])
                row['Option_Type'] = promoted_strike['Option_Type']
                row['Underlying_Price'] = promoted_strike['Underlying_Price']
                
                # Liquidity Grading
                row['Liquidity_Grade'] = liquidity_assessment['quality']
                row['Spread_Assessment'] = liquidity_assessment['spread_assessment']
                row['OI_Assessment'] = liquidity_assessment['oi_assessment']
                row['Bid_Ask_Spread_Pct'] = promoted_strike['Spread_Pct'] # Use promoted strike's spread
                row['Open_Interest'] = promoted_strike['Open_Interest'] # Use promoted strike's OI
                row['Liquidity_Score'] = selected_strikes_data['liquidity_score']
                row['Liquidity_Context'] = liquidity_assessment['context']
                
                # Capital Annotation (simplified for discovery)
                capital_required = selected_strikes_data.get('total_debit', 0.0) + selected_strikes_data.get('risk_per_contract', 0.0)
                row['Capital_Required'] = capital_required
                if capital_required > 5000:
                    row['Capital_Class'] = 'VeryHeavy'
                elif capital_required > 1000:
                    row['Capital_Class'] = 'Heavy'
                else:
                    row['Capital_Class'] = 'Standard'
                row['Within_Allocation'] = capital_required <= dollar_allocation # For reference
                
                # Strategy Classification
                row['Is_LEAP'] = is_leap
                row['Strategy_Category'] = 'LEAP' if is_leap else ('Short-Term' if actual_dte < 90 else 'Medium-Term')
                row['Structure_Type'] = 'Single-Leg' if len(selected_strikes_data['strikes']) == 1 else 'Multi-Leg'
                
                # Annotation Flags
                reason_flags = liquidity_assessment['reason_flags']
                if row['Capital_Class'] in ['Heavy', 'VeryHeavy']:
                    reason_flags.append('capital_heavy')
                row['Reason_Flags'] = ', '.join(reason_flags)
                row['Strategy_Notes'] = liquidity_assessment['context'] # Reuse liquidity context
                
                # Risk Metrics
                row['Actual_Risk_Per_Contract'] = selected_strikes_data.get('risk_per_contract', 0.0)
                row['Total_Debit'] = selected_strikes_data.get('total_debit', 0.0)
                row['Total_Credit'] = selected_strikes_data.get('total_credit', 0.0)
                row['Risk_Model'] = selected_strikes_data.get('risk_model', 'Undefined')
                
                # Tradability Assessment
                row['Tradable'] = liquidity_assessment['tradable'] and (row['Actual_Risk_Per_Contract'] > 0 or row['Total_Credit'] > 0)
                row['Tradable_Reason'] = "Meets liquidity and structural requirements" if row['Tradable'] else "Failed liquidity or structural check"
                row['Contract_Intent'] = 'Scan'
                
                # New metrics
                row['Put_Call_Skew'] = selected_strikes_data.get('put_call_skew', np.nan)
                row['Probability_Of_Profit'] = selected_strikes_data.get('probability_of_profit', np.nan)
            else:
                row['Exploration_Status'] = 'No_Strikes_Selected'
                row['Strategy_Viable'] = False
                row['Tradable'] = False
                row['Tradable_Reason'] = "No suitable strikes found for strategy"
        else:
            row['Exploration_Status'] = 'No_Strikes_Selected'
            row['Strategy_Viable'] = False
            row['Tradable'] = False
            row['Tradable_Reason'] = "No suitable strikes found for strategy"
        
    processed_rows.append(row)
        
    result_df = pd.DataFrame(processed_rows)
    
    # Audit and log summary
    _audit_multi_contract_tickers(result_df)
    _log_contract_selection_summary(result_df)
    
    logger.info("fetch_and_select_contracts (Step 9B) complete.")
    return result_df


if __name__ == "__main__":
    # Test with mock Step 9A output
    import sys
    logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

    # Note: This test will fail without valid Tradier token and live data
    test_df = pd.DataFrame({
        'Ticker': ['AAPL', 'SPY'],
        'Primary_Strategy': ['Put Credit Spread', 'Iron Condor'],
        'Strategy_Type': ['Mixed', 'Neutral'],
        'Trade_Bias': ['Bullish', 'Neutral'],
        'Confidence': [75, 70],
        'Min_DTE': [30, 45],
        'Max_DTE': [45, 60],
        'Preferred_DTE': [37, 52],
        'Num_Contracts': [2, 1],
        'Dollar_Allocation': [1500, 2500]
    })
    
    test_timeframes_df = pd.DataFrame({
        'Ticker': ['AAPL', 'SPY'],
        'Strategy_Name': ['Put Credit Spread', 'Iron Condor'],
        'Min_DTE': [30, 45],
        'Max_DTE': [45, 60],
        'Target_DTE': [37, 52]
    })

    print("=== Step 9B Test (requires Tradier API) ===\n")
    print("Input evaluated_strategies:")
    print(test_df[['Ticker', 'Primary_Strategy', 'Min_DTE', 'Max_DTE']].to_string(index=False))
    print("\nInput timeframes_df:")
    print(test_timeframes_df.to_string(index=False))
    
    # Run live test:
    # result = fetch_and_select_contracts(test_df, test_timeframes_df)
    # print("\nOutput:")
    # print(result[['Ticker', 'Selected_Expiration', 'Selected_Strikes', 
    #               'Contract_Selection_Status']].to_string(index=False))
