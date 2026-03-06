from enum import Enum

class PriceStructureState(str, Enum):
    STRUCTURAL_UP = "STRUCTURAL_UP"
    STRUCTURAL_DOWN = "STRUCTURAL_DOWN"
    RANGE_BOUND = "RANGE_BOUND"
    STRUCTURE_BROKEN = "STRUCTURE_BROKEN"
    CHAOTIC = "CHAOTIC"
    UNKNOWN = "UNKNOWN"

class TrendIntegrityState(str, Enum):
    STRONG_TREND = "STRONG_TREND"
    WEAK_TREND = "WEAK_TREND"
    TREND_EXHAUSTED = "TREND_EXHAUSTED"
    NO_TREND = "NO_TREND"
    UNKNOWN = "UNKNOWN"

class VolatilityState(str, Enum):
    COMPRESSED = "COMPRESSED"
    NORMAL = "NORMAL"
    EXPANDING = "EXPANDING"
    EXTREME = "EXTREME"
    UNKNOWN = "UNKNOWN"

class CompressionMaturityState(str, Enum):
    EARLY_COMPRESSION = "EARLY_COMPRESSION"
    MATURE_COMPRESSION = "MATURE_COMPRESSION"
    RELEASING = "RELEASING"
    POST_EXPANSION = "POST_EXPANSION"
    UNKNOWN = "UNKNOWN"

class MomentumVelocityState(str, Enum):
    ACCELERATING = "ACCELERATING"   # rate of change increasing: ROC5 > ROC20, BB expanding, vol up
    TRENDING     = "TRENDING"       # sustained directional move, slope positive, not parabolic
    LATE_CYCLE   = "LATE_CYCLE"     # price still rising but momentum diverging: RSI flat/falling, ROC slowing
    DECELERATING = "DECELERATING"   # momentum falling and slope negative
    STALLING     = "STALLING"       # near-zero slope, no clear direction
    REVERSING    = "REVERSING"      # short-term ROC sign flipped vs medium-term
    UNKNOWN      = "UNKNOWN"        # insufficient primitives to classify

class DirectionalBalanceState(str, Enum):
    BUYER_DOMINANT = "BUYER_DOMINANT"
    SELLER_DOMINANT = "SELLER_DOMINANT"
    BALANCED = "BALANCED"
    CONTESTED = "CONTESTED"
    UNKNOWN = "UNKNOWN"

class RangeEfficiencyState(str, Enum):
    EFFICIENT_TREND = "EFFICIENT_TREND"
    INEFFICIENT_RANGE = "INEFFICIENT_RANGE"
    NOISY = "NOISY"
    FAKE_BREAK = "FAKE_BREAK"
    UNKNOWN = "UNKNOWN"

class TimeframeAgreementState(str, Enum):
    ALIGNED = "ALIGNED"
    PARTIAL = "PARTIAL"
    DIVERGENT = "DIVERGENT"
    UNKNOWN = "UNKNOWN"

class GreekDominanceState(str, Enum):
    THETA_DOMINANT = "THETA_DOMINANT"
    GAMMA_DOMINANT = "GAMMA_DOMINANT"
    BALANCED = "BALANCED"
    UNKNOWN = "UNKNOWN"

class AssignmentRiskState(str, Enum):
    LOW = "LOW"
    ELEVATED = "ELEVATED"
    IMMINENT = "IMMINENT"
    UNKNOWN = "UNKNOWN"

class RegimeStabilityState(str, Enum):
    ESTABLISHED = "ESTABLISHED"
    EMERGING = "EMERGING"
    FRAGMENTING = "FRAGMENTING"
    NOISE = "NOISE"
    UNKNOWN = "UNKNOWN"

class RecoveryQualityState(str, Enum):
    STRUCTURAL_RECOVERY = "STRUCTURAL_RECOVERY"   # Regime has genuinely shifted upward
    DEAD_CAT_BOUNCE = "DEAD_CAT_BOUNCE"           # 1-2 day bounce in a still-broken trend
    STILL_DECLINING = "STILL_DECLINING"           # No bounce — trend continues down
    NOT_IN_RECOVERY = "NOT_IN_RECOVERY"           # Stock not under downside pressure
    UNKNOWN = "UNKNOWN"
