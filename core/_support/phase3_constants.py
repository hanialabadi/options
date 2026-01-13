"""
Phase 3 Enrichment Constants

Single source of truth for all Phase 3 magic numbers and thresholds.
Imports Phase 2 constants for strategy/structure validation.
"""

# Import Phase 2 constants for strategy validation
from core.phase2_constants import (
    STRATEGY_COVERED_CALL,
    STRATEGY_CSP,
    STRATEGY_LONG_STRADDLE,
    STRATEGY_LONG_STRANGLE,
    STRATEGY_BUY_CALL,
    STRATEGY_BUY_PUT,
    ALL_STRATEGIES,
    ASSET_TYPE_STOCK,
    ASSET_TYPE_OPTION,
)

# === Moneyness Thresholds ===
ATM_THRESHOLD = 0.05  # ±5% around strike price

# === PCS Scoring Thresholds ===
# Gamma scoring
PCS_GAMMA_MULTIPLIER = 1500
PCS_GAMMA_MAX = 25

# Vega scoring
PCS_VEGA_MULTIPLIER = 5000
PCS_VEGA_MAX = 25

# ROI scoring
PCS_ROI_THRESHOLD_HIGH = 0.30  # 30% ROI → 15 points
PCS_ROI_THRESHOLD_MID = 0.20   # 20% ROI → 10 points
PCS_ROI_SCORE_HIGH = 15
PCS_ROI_SCORE_MID = 10
PCS_ROI_SCORE_LOW = 5

# Profile-based weighting
PCS_WEIGHTS_NEUTRAL_VOL = {"vega": 0.6, "gamma": 0.25, "roi": 0.15}
PCS_WEIGHTS_INCOME = {"roi": 0.5, "vega": 0.3, "gamma": 0.2}
PCS_WEIGHTS_DIRECTIONAL = {"gamma": 0.5, "vega": 0.3, "roi": 0.2}
PCS_WEIGHTS_DEFAULT = {"gamma": 0.4, "vega": 0.4, "roi": 0.2}

# PCS Tier thresholds (consolidated from pcs_score.py and score_confidence_tier.py)
PCS_TIER1_THRESHOLD = 80
PCS_TIER2_THRESHOLD = 70
PCS_TIER3_THRESHOLD = 60  # Below this is Tier 4

# PCS Tier labels (Issue 6: centralized constants)
PCS_TIER1_LABEL = "Tier 1"
PCS_TIER2_LABEL = "Tier 2"
PCS_TIER3_LABEL = "Tier 3"
PCS_TIER4_LABEL = "Tier 4"

# Revalidation thresholds
PCS_REVALIDATION_THRESHOLD = 65
VEGA_REVALIDATION_THRESHOLD = 0.25
GAMMA_REVALIDATION_THRESHOLD = 0.05  # Issue 4: add gamma deterioration check
ROI_REVALIDATION_THRESHOLD = 0.05   # Issue 3: minimum 5% return on capital (works with strategy-aware abs(ROI))

# === Liquidity Thresholds ===
LIQUIDITY_OI_THRESHOLD = 500           # Open Interest minimum
LIQUIDITY_SPREAD_PCT_THRESHOLD = 0.05  # 5% bid-ask spread maximum
LIQUIDITY_MIN_DOLLAR_VOLUME = 5000     # Minimum daily dollar volume
LIQUIDITY_MIN_VEGA_EFFICIENCY = 0.00001  # Minimum vega per spread dollar
LIQUIDITY_WIDE_SPREAD_THRESHOLD = 0.10  # 10% wide spread flag

# Minimum spread for vega efficiency calculation (prevents division by zero)
MIN_SPREAD_FOR_VEGA_EFFICIENCY = 0.01

# === Earnings Event Thresholds ===
EARNINGS_VEGA_THRESHOLD = 2.0        # Minimum vega for event setup
EARNINGS_PROXIMITY_DAYS_MIN = 0      # Minimum days to earnings
EARNINGS_PROXIMITY_DAYS_MAX = 7      # Maximum days to earnings

# === Capital Deployed Validation ===
# Options contract multiplier (standard US options)
OPTIONS_CONTRACT_MULTIPLIER = 100

# === Greek Validation Thresholds ===
MIN_VEGA_FOR_VOL_EDGE = 0.25    # Minimum vega to qualify as "Vol Edge"

# === IV Spread Thresholds ===
IV_SPREAD_LIQUIDITY_RISK_THRESHOLD = 1.0  # IV spread > 1.0 → high liquidity risk

# === Minimum values for safety checks ===
MIN_BASIS_FOR_CALCULATIONS = 1e-3  # Prevent division by zero in ROI, vega efficiency

# === Strategy Profile Mappings (for PCS scoring) ===
# Standardized to uppercase for RAG persona compliance
PROFILE_NEUTRAL_VOL = "NEUTRAL_VOL"
PROFILE_INCOME = "INCOME"
PROFILE_DIRECTIONAL_BULL = "DIRECTIONAL"
PROFILE_DIRECTIONAL_BEAR = "DIRECTIONAL"
PROFILE_OTHER = "Other"

ALL_PROFILES = {
    PROFILE_NEUTRAL_VOL,
    PROFILE_INCOME,
    PROFILE_DIRECTIONAL_BULL,
    PROFILE_DIRECTIONAL_BEAR,
    PROFILE_OTHER,
}

# === Tag Intent Mappings ===
TAG_INTENT_BULLISH_INCOME = "Bullish Income"
TAG_INTENT_NEUTRAL_VOL_EDGE = "Neutral Vol Edge"
TAG_INTENT_DIRECTIONAL_BULLISH = "Directional Bullish"
TAG_INTENT_DIRECTIONAL_BEARISH = "Directional Bearish"
TAG_INTENT_YIELD_CAP = "Yield + Cap"
TAG_INTENT_UNCLASSIFIED = "Unclassified"

# === Exit Style Tags ===
TAG_EXIT_TRAIL = "Trail Exit"
TAG_EXIT_THETA_HOLD = "Theta Hold"
TAG_EXIT_DUAL_LEG = "Dual Leg Exit"
TAG_EXIT_MANUAL = "Manual"

# === Edge Type Tags ===
TAG_EDGE_VOL = "Vol Edge"
TAG_EDGE_THETA = "Theta Edge"
TAG_EDGE_NONE = "No Edge"

# === Moneyness Labels ===
MONEYNESS_ITM = "ITM"
MONEYNESS_ATM = "ATM"
MONEYNESS_OTM = "OTM"

# === Breakeven Type Labels ===
BREAKEVEN_TYPE_STRADDLE_STRANGLE = "Straddle/Strangle"
BREAKEVEN_TYPE_PUT = "Put"
BREAKEVEN_TYPE_CALL = "Call"
BREAKEVEN_TYPE_COVERED_CALL = "Covered_Call"
BREAKEVEN_TYPE_CSP = "CSP"
BREAKEVEN_TYPE_UNKNOWN = "Unknown"
BREAKEVEN_TYPE_ERROR = "Error"
