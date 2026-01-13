"""
Phase 2: Shared Constants

Canonical definitions for Strategy, Structure, LegType, and AssetType.
All Phase 2 modules MUST import from here to ensure consistency.
"""

# === Strategy Constants ===
STRATEGY_UNKNOWN = "Unknown"
STRATEGY_BUY_CALL = "Buy_Call"
STRATEGY_BUY_PUT = "Buy_Put"
STRATEGY_COVERED_CALL = "Covered_Call"
STRATEGY_CSP = "CSP"
STRATEGY_LONG_STRADDLE = "Straddle"
STRATEGY_LONG_STRANGLE = "Strangle"

# All known strategies (for validation)
ALL_STRATEGIES = {
    STRATEGY_UNKNOWN,
    STRATEGY_BUY_CALL,
    STRATEGY_BUY_PUT,
    STRATEGY_COVERED_CALL,
    STRATEGY_CSP,
    STRATEGY_LONG_STRADDLE,
    STRATEGY_LONG_STRANGLE,
}

# === Structure Constants ===
STRUCTURE_SINGLE_LEG = "Single-leg"
STRUCTURE_MULTI_LEG = "Multi-leg"
STRUCTURE_STOCK_CALL = "STOCK+CALL"

ALL_STRUCTURES = {
    STRUCTURE_SINGLE_LEG,
    STRUCTURE_MULTI_LEG,
    STRUCTURE_STOCK_CALL,
}

# === LegType Constants ===
LEG_TYPE_STOCK = "STOCK"
LEG_TYPE_SHORT_CALL = "SHORT_CALL"
LEG_TYPE_LONG_CALL = "LONG_CALL"
LEG_TYPE_SHORT_PUT = "SHORT_PUT"
LEG_TYPE_LONG_PUT = "LONG_PUT"
LEG_TYPE_UNKNOWN = "UNKNOWN"

ALL_LEG_TYPES = {
    LEG_TYPE_STOCK,
    LEG_TYPE_SHORT_CALL,
    LEG_TYPE_LONG_CALL,
    LEG_TYPE_SHORT_PUT,
    LEG_TYPE_LONG_PUT,
    LEG_TYPE_UNKNOWN,
}

# === AssetType Constants ===
ASSET_TYPE_STOCK = "STOCK"
ASSET_TYPE_OPTION = "OPTION"
ASSET_TYPE_UNKNOWN = "UNKNOWN"

ALL_ASSET_TYPES = {
    ASSET_TYPE_STOCK,
    ASSET_TYPE_OPTION,
    ASSET_TYPE_UNKNOWN,
}

# === OptionType Constants ===
OPTION_TYPE_CALL = "Call"
OPTION_TYPE_PUT = "Put"

ALL_OPTION_TYPES = {
    OPTION_TYPE_CALL,
    OPTION_TYPE_PUT,
    None,  # Stocks have None for OptionType
}

# === Stock Optionability Status ===
STOCK_OPTION_STATUS_NOT_OPTIONABLE = "NOT_OPTIONABLE"
STOCK_OPTION_STATUS_OPTIONABLE = "OPTIONABLE"

# === Option Usage Status ===
OPTION_USAGE_NONE = "NONE"
OPTION_USAGE_ACTIVE = "ACTIVE"

# === Strategy Definitions (Structural Validation) ===
# Used for validating multi-leg structures in Phase 2C and Phase 6
STRATEGY_DEFINITIONS = {
    STRATEGY_LONG_STRADDLE: {
        'leg_count': 2,
        'required_roles': ['Long_Call', 'Long_Put'],
        'strike_constraint': 'all_equal',
        'expiration_constraint': 'all_equal',
        'description': 'Long Call + Long Put at same strike and expiration'
    },
    STRATEGY_LONG_STRANGLE: {
        'leg_count': 2,
        'required_roles': ['Long_Call', 'Long_Put'],
        'strike_constraint': 'different',
        'expiration_constraint': 'all_equal',
        'description': 'Long Call + Long Put at different strikes, same expiration'
    },
    STRATEGY_COVERED_CALL: {
        'leg_count': 2,
        'required_roles': ['Stock_Long', 'Short_Call'],
        'strike_constraint': None,  # Stock has no strike
        'expiration_constraint': None,  # Stock has no expiration
        'description': 'Long Stock (100 shares per contract) + Short Call'
    },
    STRATEGY_CSP: {
        'leg_count': 1,
        'required_roles': ['Short_Put'],
        'strike_constraint': None,
        'expiration_constraint': None,
        'description': 'Cash-Secured Put (single leg)'
    },
    STRATEGY_BUY_CALL: {
        'leg_count': 1,
        'required_roles': ['Long_Call'],
        'strike_constraint': None,
        'expiration_constraint': None,
        'description': 'Long Call (single leg)'
    },
    STRATEGY_BUY_PUT: {
        'leg_count': 1,
        'required_roles': ['Long_Put'],
        'strike_constraint': None,
        'expiration_constraint': None,
        'description': 'Long Put (single leg)'
    },
}
