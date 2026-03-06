"""
Cycle-3 Doctrine: Risk Compatibility Matrix
Defines the design tolerance of each strategy relative to technical states.
This is used by Cycle-3 to determine if a trade is within its operational envelope.

Capital Survival Audit (Phase 5): Expanded to cover ALL Tier 1 strategies.
Unknown strategies now default to INCOMPATIBLE (was: compatible).

Theory references:
  - Natenberg Ch.11: Option strategies have defined volatility regimes
  - Passarelli Ch.2: Assignment risk tolerance is strategy-specific
  - McMillan Ch.7: Greek dominance determines management approach
  - Hull Ch.10: LEAPs are vega-dominant, not gamma-dominant
"""

COMPATIBILITY = {
    # ── Income Strategies (short premium, theta-positive) ──────────────────
    "COVERED_CALL": {
        "AssignmentRisk": ["LOW", "ELEVATED"],
        "GreekDominance": ["THETA_DOMINANT", "BALANCED"],
        "VolatilityState": ["NORMAL", "COMPRESSED", "EXPANDING"],
        "TrendIntegrity": ["STRONG_TREND", "WEAK_TREND", "NO_TREND"]
    },
    "CSP": {
        "AssignmentRisk": ["LOW", "ELEVATED", "IMMINENT"],
        "GreekDominance": ["THETA_DOMINANT", "BALANCED"],
        "VolatilityState": ["NORMAL", "EXPANDING", "EXTREME"],
        "TrendIntegrity": ["STRONG_TREND", "WEAK_TREND", "NO_TREND"]
    },
    "BUY_WRITE": {
        # Buy-write = stock + short call. Owns stock → tolerates assignment.
        # Theta-dominant. Needs mild bullish or neutral trend (Passarelli Ch.6).
        "AssignmentRisk": ["LOW", "ELEVATED"],
        "GreekDominance": ["THETA_DOMINANT", "BALANCED"],
        "VolatilityState": ["NORMAL", "COMPRESSED", "EXPANDING"],
        "TrendIntegrity": ["STRONG_TREND", "WEAK_TREND"]
    },

    # ── Directional Strategies (long premium, gamma/delta-positive) ────────
    "LONG_CALL": {
        # No assignment risk on longs. Needs directional trend (Natenberg Ch.3).
        # Buy when vol is cheap (compressed/normal), not expensive.
        "AssignmentRisk": ["LOW", "ELEVATED", "IMMINENT"],
        "GreekDominance": ["GAMMA_DOMINANT", "BALANCED", "VEGA_DOMINANT"],
        "VolatilityState": ["COMPRESSED", "NORMAL"],
        "TrendIntegrity": ["STRONG_TREND", "WEAK_TREND"]
    },
    "BUY_CALL": {  # Alias for LONG_CALL (Cycle 1 naming)
        "AssignmentRisk": ["LOW", "ELEVATED", "IMMINENT"],
        "GreekDominance": ["GAMMA_DOMINANT", "BALANCED", "VEGA_DOMINANT"],
        "VolatilityState": ["COMPRESSED", "NORMAL"],
        "TrendIntegrity": ["STRONG_TREND", "WEAK_TREND"]
    },
    "LONG_PUT": {
        "AssignmentRisk": ["LOW", "ELEVATED", "IMMINENT"],
        "GreekDominance": ["GAMMA_DOMINANT", "BALANCED", "VEGA_DOMINANT"],
        "VolatilityState": ["COMPRESSED", "NORMAL"],
        "TrendIntegrity": ["STRONG_TREND", "WEAK_TREND"]
    },
    "BUY_PUT": {  # Alias for LONG_PUT (Cycle 1 naming)
        "AssignmentRisk": ["LOW", "ELEVATED", "IMMINENT"],
        "GreekDominance": ["GAMMA_DOMINANT", "BALANCED", "VEGA_DOMINANT"],
        "VolatilityState": ["COMPRESSED", "NORMAL"],
        "TrendIntegrity": ["STRONG_TREND", "WEAK_TREND"]
    },

    # ── LEAP Strategies (long-dated directional, vega-dominant) ────────────
    "LEAPS_CALL": {
        # LEAPs are vega-driven, not gamma-driven (Hull Ch.10).
        # Buy when vol is cheap. Longer horizon → trend less critical.
        "AssignmentRisk": ["LOW", "ELEVATED", "IMMINENT"],
        "GreekDominance": ["VEGA_DOMINANT", "BALANCED"],
        "VolatilityState": ["COMPRESSED", "NORMAL"],
        "TrendIntegrity": ["STRONG_TREND", "WEAK_TREND", "NO_TREND"]
    },
    "LEAPS_PUT": {
        "AssignmentRisk": ["LOW", "ELEVATED", "IMMINENT"],
        "GreekDominance": ["VEGA_DOMINANT", "BALANCED"],
        "VolatilityState": ["COMPRESSED", "NORMAL"],
        "TrendIntegrity": ["STRONG_TREND", "WEAK_TREND", "NO_TREND"]
    },

    # ── Volatility Strategies (long gamma/vega, bidirectional) ─────────────
    "STRADDLE": {
        # Enter when vol is cheap (compressed/normal). Bidirectional = no trend needed.
        # EXPANDING vol is the EXIT condition, not the entry condition.
        # Natenberg Ch.11: long vol positions require compressed entry environment.
        "AssignmentRisk": ["LOW", "ELEVATED"],
        "GreekDominance": ["GAMMA_DOMINANT", "VEGA_DOMINANT"],
        "VolatilityState": ["COMPRESSED", "NORMAL"],
        "TrendIntegrity": ["NO_TREND", "WEAK_TREND"]
    },
    "STRANGLE": {
        "AssignmentRisk": ["LOW", "ELEVATED"],
        "GreekDominance": ["GAMMA_DOMINANT", "VEGA_DOMINANT"],
        "VolatilityState": ["COMPRESSED", "NORMAL"],
        "TrendIntegrity": ["NO_TREND", "WEAK_TREND"]
    },

    # ── Spread Strategies (Tier 2 — not currently executable) ──────────────
    "IRON_CONDOR": {
        "AssignmentRisk": ["LOW"],
        "GreekDominance": ["THETA_DOMINANT"],
        "VolatilityState": ["NORMAL", "COMPRESSED"],
        "TrendIntegrity": ["NO_TREND", "WEAK_TREND"]
    },
    "VERTICAL_SPREAD": {
        "AssignmentRisk": ["LOW", "ELEVATED"],
        "GreekDominance": ["GAMMA_DOMINANT", "BALANCED"],
        "VolatilityState": ["NORMAL", "EXPANDING"],
        "TrendIntegrity": ["STRONG_TREND", "WEAK_TREND"]
    },

    # ── Stock-only (management engine only) ────────────────────────────────
    "STOCK_ONLY": {
        "AssignmentRisk": ["LOW", "ELEVATED", "IMMINENT"],
        "GreekDominance": ["BALANCED"],
        "VolatilityState": ["NORMAL", "COMPRESSED", "EXPANDING", "EXTREME"],
        "TrendIntegrity": ["STRONG_TREND", "WEAK_TREND", "NO_TREND"]
    },
}


def is_compatible(strategy: str, dimension: str, state: str) -> bool:
    """
    Checks if a given state is compatible with a strategy's design tolerance.

    Capital Survival Audit: Unknown strategies now default to INCOMPATIBLE.
    This prevents unclassified positions from silently passing envelope checks.
    """
    if strategy not in COMPATIBILITY:
        return False  # Unknown strategies are NOT compatible by default

    strategy_rules = COMPATIBILITY[strategy]
    if dimension not in strategy_rules:
        return True  # Dimension not constrained for this strategy

    allowed_states = strategy_rules[dimension]
    return state in allowed_states or state == "UNKNOWN"
