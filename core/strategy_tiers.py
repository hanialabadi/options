"""
Strategy Tier System - Broker-Aware Execution Classification

Purpose:
    Gate option chain scanning to only strategies YOUR BROKER APPROVES.
    Strategy recommendations (Tier 2+) remain visible but don't scan contracts.

Tier 1: BROKER-APPROVED + Executable
    - Your account has approval for these strategies
    - System can scan chains and execute
    - Output: Actionable trades with contracts
    - Examples: Long calls/puts, covered calls, CSPs, straddles/strangles

Tier 2: BROKER-BLOCKED (but technically simple)
    - Single-expiry strategies you CANNOT execute yet
    - Blocked by broker approval level, not logic
    - Output: "Strategy ideas - upgrade account to execute"
    - Examples: Vertical spreads (debit/credit), iron condors
    - Blocker: "Requires spreads approval (Level 2+)"

Tier 3: LOGIC-BLOCKED (multi-expiry or complex)
    - Multi-expiry strategies system cannot execute yet
    - Blocked by execution logic, not broker
    - Output: "Watch list - future capability"
    - Examples: Calendar spreads, diagonals, PMCC, LEAPs
    - Blocker: "Requires multi-expiration logic" or "Requires LEAP filtering"

Guiding Rule:
    Tier 1 = Can execute TODAY (broker + system ready)
    Tier 2 = Broker says no (upgrade account to unlock)
    Tier 3 = System says no (future development to unlock)
"""

# ========================================
# TIER 1: BROKER-APPROVED + EXECUTABLE
# ========================================

TIER_1_STRATEGIES = {
    # Single-Leg Directional (Broker Tier 1)
    'Long Call': {
        'tier': 1,
        'execution_ready': True,
        'broker_approval': 'Tier 1 - Long Options',
        'legs': 1,
        'expirations': 1,
        'description': 'Bullish directional play',
        'blocker': ''
    },
    'Long Put': {
        'tier': 1,
        'execution_ready': True,
        'broker_approval': 'Tier 1 - Long Options',
        'legs': 1,
        'expirations': 1,
        'description': 'Bearish directional play',
        'blocker': ''
    },
    
    # Income Strategies (Broker Tier 1)
    'Covered Call': {
        'tier': 1,
        'execution_ready': True,
        'broker_approval': 'Tier 1 - Covered Strategies',
        'legs': 2,
        'expirations': 1,
        'description': 'Sell OTM call against long stock',
        'blocker': ''
    },
    'Covered Call (if holding stock)': {
        'tier': 1,
        'execution_ready': True,
        'broker_approval': 'Tier 1 - Covered Strategies',
        'legs': 2,
        'expirations': 1,
        'description': 'Alias: Sell OTM call against long stock',
        'blocker': ''
    },
    'Cash-Secured Put': {
        'tier': 1,
        'execution_ready': True,
        'broker_approval': 'Tier 1 - Cash-Secured',
        'legs': 1,
        'expirations': 1,
        'description': 'Sell put with cash collateral',
        'blocker': ''
    },
    'Wheel Strategy (Cash-Secured Puts)': {
        'tier': 1,
        'execution_ready': True,
        'broker_approval': 'Tier 1 - Cash-Secured',
        'legs': 1,
        'expirations': 1,
        'description': 'Alias: Sell put for wheel strategy',
        'blocker': ''
    },
    'Rolling Covered Call': {
        'tier': 1,
        'execution_ready': True,
        'broker_approval': 'Tier 1 - Covered Strategies',
        'legs': 2,
        'expirations': 1,
        'description': 'Close + reopen covered call',
        'blocker': ''
    },
    'Buy-Write': {
        'tier': 1,
        'execution_ready': True,
        'broker_approval': 'Tier 1 - Covered Strategies',
        'legs': 2,
        'expirations': 1,
        'description': 'Buy stock + sell call simultaneously',
        'blocker': ''
    },
    
    # Volatility Strategies (Broker Tier 1)
    'Long Straddle': {
        'tier': 1,
        'execution_ready': True,
        'broker_approval': 'Tier 1 - Long Options',
        'legs': 2,
        'expirations': 1,
        'description': 'Long call + long put at same strike',
        'blocker': ''
    },
    'Long Strangle': {
        'tier': 1,
        'execution_ready': True,
        'broker_approval': 'Tier 1 - Long Options',
        'legs': 2,
        'expirations': 1,
        'description': 'Long call + long put at different strikes',
        'blocker': ''
    },
}

# ========================================
# TIER 2: BROKER-BLOCKED STRATEGIES
# ========================================

TIER_2_STRATEGIES = {
    # Vertical Spreads (Require Level 2+ Approval)
    'Call Debit Spread': {
        'tier': 2,
        'execution_ready': False,
        'broker_approval': 'Level 2+ - Spreads Required',
        'legs': 2,
        'expirations': 1,
        'description': 'Bullish vertical spread',
        'blocker': 'Requires spreads approval (upgrade broker account)'
    },
    'Put Debit Spread': {
        'tier': 2,
        'execution_ready': False,
        'broker_approval': 'Level 2+ - Spreads Required',
        'legs': 2,
        'expirations': 1,
        'description': 'Bearish vertical spread',
        'blocker': 'Requires spreads approval (upgrade broker account)'
    },
    'Call Credit Spread': {
        'tier': 2,
        'execution_ready': False,
        'broker_approval': 'Level 2+ - Spreads Required',
        'legs': 2,
        'expirations': 1,
        'description': 'Bearish vertical spread',
        'blocker': 'Requires spreads approval (upgrade broker account)'
    },
    'Put Credit Spread': {
        'tier': 2,
        'execution_ready': False,
        'broker_approval': 'Level 2+ - Spreads Required',
        'legs': 2,
        'expirations': 1,
        'description': 'Bullish vertical spread',
        'blocker': 'Requires spreads approval (upgrade broker account)'
    },
    
    # Multi-Leg Spreads (Require Level 3+ Approval)
    'Iron Condor': {
        'tier': 2,
        'execution_ready': False,
        'broker_approval': 'Level 3+ - Multi-Leg Spreads',
        'legs': 4,
        'expirations': 1,
        'description': 'Sell OTM put spread + sell OTM call spread',
        'blocker': 'Requires advanced spreads approval (upgrade broker account)'
    },
    'Iron Butterfly': {
        'tier': 2,
        'execution_ready': False,
        'broker_approval': 'Level 3+ - Multi-Leg Spreads',
        'legs': 4,
        'expirations': 1,
        'description': 'Sell ATM straddle + buy OTM strangle',
        'blocker': 'Requires advanced spreads approval (upgrade broker account)'
    },
}

# ========================================
# TIER 3: LOGIC-BLOCKED STRATEGIES
# ========================================

TIER_3_STRATEGIES = {
    # Multi-Expiration Strategies
    'Calendar Spread': {
        'tier': 3,
        'execution_ready': False,
        'broker_approval': 'Level 2+ - Spreads Required',
        'legs': 2,
        'expirations': 2,
        'blocker': 'Requires multi-expiration logic',
        'description': 'Sell near-term, buy far-term (same strike)'
    },
    'Diagonal Spread': {
        'tier': 3,
        'execution_ready': False,
        'broker_approval': 'Level 2+ - Spreads Required',
        'legs': 2,
        'expirations': 2,
        'blocker': 'Requires multi-expiration logic',
        'description': 'Calendar + vertical combined'
    },
    'Diagonal Spread (Bullish)': {
        'tier': 3,
        'execution_ready': False,
        'broker_approval': 'Level 2+ - Spreads Required',
        'legs': 2,
        'expirations': 2,
        'blocker': 'Requires multi-expiration logic',
        'description': 'Calendar + vertical combined (bullish bias)'
    },
    
    # LEAP-Based Strategies
    'LEAP Call': {
        'tier': 3,
        'execution_ready': False,
        'broker_approval': 'Tier 1 - Long Options',
        'legs': 1,
        'expirations': 1,
        'blocker': 'Requires LEAP filtering (180+ DTE)',
        'description': 'Long-dated call (>6 months)'
    },
    'LEAP Call (Buy to Open)': {
        'tier': 1,
        'execution_ready': True,
        'broker_approval': 'Tier 1 - Long Options',
        'legs': 1,
        'expirations': 1,
        'blocker': None,
        'description': 'Buy naked LEAP call (180-365 DTE) when long-term IV cheap'
    },
    'LEAP Call Debit Spread': {
        'tier': 3,
        'execution_ready': False,
        'broker_approval': 'Level 2+ - Spreads Required',
        'legs': 2,
        'expirations': 1,
        'blocker': 'Requires LEAP filtering (180+ DTE)',
        'description': 'Long-dated vertical spread'
    },
    'LEAP Put': {
        'tier': 3,
        'execution_ready': False,
        'broker_approval': 'Tier 1 - Long Options',
        'legs': 1,
        'expirations': 1,
        'blocker': 'Requires LEAP filtering (180+ DTE)',
        'description': 'Long-dated put (>6 months)'
    },
    'LEAP Put (Buy to Open)': {
        'tier': 1,
        'execution_ready': True,
        'broker_approval': 'Tier 1 - Long Options',
        'legs': 1,
        'expirations': 1,
        'blocker': None,
        'description': 'Buy naked LEAP put (180-365 DTE) when long-term IV cheap'
    },
    'LEAP Put Debit Spread': {
        'tier': 3,
        'execution_ready': False,
        'broker_approval': 'Level 2+ - Spreads Required',
        'legs': 2,
        'expirations': 1,
        'blocker': 'Requires LEAP filtering (180+ DTE)',
        'description': 'Long-dated put vertical'
    },
    'Poor Man\'s Covered Call': {
        'tier': 3,
        'execution_ready': False,
        'broker_approval': 'Level 2+ - Spreads Required',
        'legs': 2,
        'expirations': 2,
        'blocker': 'Requires LEAP + multi-expiration logic',
        'description': 'Buy LEAP call, sell near-term call'
    },
    'Poor Man\'s Covered Call (LEAP base)': {
        'tier': 3,
        'execution_ready': False,
        'broker_approval': 'Level 2+ - Spreads Required',
        'legs': 2,
        'expirations': 2,
        'blocker': 'Requires LEAP + multi-expiration logic',
        'description': 'Alias: PMCC'
    },
    'PMCC': {
        'tier': 3,
        'execution_ready': False,
        'broker_approval': 'Level 2+ - Spreads Required',
        'legs': 2,
        'expirations': 2,
        'blocker': 'Requires LEAP + multi-expiration logic',
        'description': 'Alias for Poor Man\'s Covered Call'
    },
    'Ultra-LEAP Call (2-3 year)': {
        'tier': 3,
        'execution_ready': False,
        'broker_approval': 'Tier 1 - Long Options',
        'legs': 1,
        'expirations': 1,
        'blocker': 'Requires ultra-long DTE filtering (450+ days)',
        'description': 'Multi-year long call'
    },
}

# ========================================
# HELPER FUNCTIONS
# ========================================

def get_strategy_tier(strategy_name: str) -> dict:
    """
    Get tier metadata for a strategy name.
    
    Returns:
        dict with keys: tier, execution_ready, broker_approval, blocker, description
    """
    # Check Tier 1 first
    if strategy_name in TIER_1_STRATEGIES:
        return TIER_1_STRATEGIES[strategy_name]
    
    # Check Tier 2
    if strategy_name in TIER_2_STRATEGIES:
        return TIER_2_STRATEGIES[strategy_name]
    
    # Check Tier 3
    if strategy_name in TIER_3_STRATEGIES:
        return TIER_3_STRATEGIES[strategy_name]
    
    # Default: Unknown strategy (treat as Tier 3)
    return {
        'tier': 3,
        'execution_ready': False,
        'broker_approval': 'Unknown',
        'blocker': 'Strategy not in tier map',
        'description': 'Unknown strategy',
        'legs': 0,
        'expirations': 0
    }


def is_execution_ready(strategy_name: str) -> bool:
    """Check if strategy is Tier 1 (execution-ready)."""
    tier_info = get_strategy_tier(strategy_name)
    return tier_info['execution_ready']


def get_all_tier1_strategies() -> list:
    """Get list of all Tier 1 strategy names."""
    return list(TIER_1_STRATEGIES.keys())


def get_all_tier2_strategies() -> list:
    """Get list of all Tier 2 strategy names."""
    return list(TIER_2_STRATEGIES.keys())


def get_all_tier3_strategies() -> list:
    """Get list of all Tier 3 strategy names."""
    return list(TIER_3_STRATEGIES.keys())


def get_execution_blocker(strategy_name: str) -> str:
    """Get the reason why a strategy cannot be executed."""
    tier_info = get_strategy_tier(strategy_name)
    return tier_info.get('blocker', '')
