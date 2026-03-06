"""
Phase 2: Shared Constants

Canonical definitions for Strategy, Structure, LegType, and AssetType.
All Phase 2 modules MUST import from here to ensure consistency.
"""
import re

# === Authoritative Identity Regex (OCC Style) ===
# RAG Authority: McMillan (Identity). 
# Handles Fidelity symbols with internal spaces (e.g. "AAPL  260116C240")
# without mutating the original broker string.
OCC_OPTION_PATTERN = re.compile(
    r"^-?([A-Z]+)\s*(\d{2})(\d{2})(\d{2})([CP])(\d+(\.\d+)?)$"
)

# === Strategy Constants ===
STRATEGY_UNKNOWN = "Unknown"
STRATEGY_BUY_CALL = "Buy_Call"
STRATEGY_BUY_PUT = "Buy_Put"
STRATEGY_LEAPS_CALL = "LEAPS_Call"
STRATEGY_LEAPS_PUT = "LEAPS_Put"
STRATEGY_BUY_WRITE = "Buy_Write"
STRATEGY_COVERED_CALL = "Covered_Call"
STRATEGY_CSP = "Cash_Secured_Put"
STRATEGY_LONG_STRADDLE = "Long_Straddle"
STRATEGY_LONG_STRANGLE = "Long_Strangle"
STRATEGY_STOCK = "STOCK_ONLY"

# All known strategies (for validation)
ALL_STRATEGIES = {
    STRATEGY_UNKNOWN,
    STRATEGY_BUY_CALL,
    STRATEGY_BUY_PUT,
    STRATEGY_LEAPS_CALL,
    STRATEGY_LEAPS_PUT,
    STRATEGY_BUY_WRITE,
    STRATEGY_COVERED_CALL,
    STRATEGY_CSP,
    STRATEGY_LONG_STRADDLE,
    STRATEGY_LONG_STRANGLE,
    STRATEGY_STOCK,
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

# === Exclusion List (Noise Reduction) ===
# Symbols that should be ignored at ingest (e.g., cash equivalents, MMFs)
IGNORED_SYMBOLS = {
    "SPAXX",  # Fidelity Government Money Market
    "FDLXX",  # Fidelity Treasury Only Money Market
    "FZFXX",  # Fidelity Treasury Money Market
    "FCASH",  # Fidelity Cash
    "CORE**", # Fidelity Core Position Placeholder
}

# === Option Usage Status ===
OPTION_USAGE_NONE = "NONE"
OPTION_USAGE_ACTIVE = "ACTIVE"

# === Multipliers ===
OPTIONS_CONTRACT_MULTIPLIER = 100

# === Financing / Carry Cost ===
# Fidelity margin rate as of 2026 (10.375% annualized).
# This is the authoritative hurdle rate for ALL yield maintenance decisions:
#   - BUY_WRITE / COVERED_CALL: annualized premium yield must EXCEED this or the
#     stock financing cost outpaces income → ROLL immediately.
#   - LONG options on margin: daily carry cost bleeds the thesis independently of P&L.
#   - Recovery feasibility: dollar loss from margin bleed is a real daily headwind.
# McMillan Ch.3: "The covered writer must earn at least the cost of carrying the stock."
# Passarelli Ch.6: "Negative carry — yield below financing rate — is a ROLL signal."
FIDELITY_MARGIN_RATE = 0.10375  # 10.375% per annum
FIDELITY_MARGIN_RATE_DAILY = FIDELITY_MARGIN_RATE / 365  # ~0.0284% per day

# === Breakeven Types ===
BREAKEVEN_TYPE_STRADDLE_STRANGLE = "STRADDLE_STRANGLE"
BREAKEVEN_TYPE_PUT = "PUT"
BREAKEVEN_TYPE_CALL = "CALL"
BREAKEVEN_TYPE_COVERED_CALL = "COVERED_CALL"
BREAKEVEN_TYPE_CSP = "CSP"
BREAKEVEN_TYPE_UNKNOWN = "UNKNOWN"

# === Moneyness Constants ===
ATM_THRESHOLD = 0.05
MONEYNESS_ITM = "ITM"
MONEYNESS_ATM = "ATM"
MONEYNESS_OTM = "OTM"

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
    STRATEGY_BUY_WRITE: {
        'leg_count': 2,
        'required_roles': ['Stock_Long', 'Short_Call'],
        'strike_constraint': None,
        'expiration_constraint': None,
        'description': 'Buy-Write (Stock + Short Call)'
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
