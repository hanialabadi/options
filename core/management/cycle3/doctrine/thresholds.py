"""
Management Engine Doctrine Thresholds — Single Source of Truth.

Every numeric gate that was previously hardcoded inline in engine.py
is extracted here with its RAG citation. The name encodes:
    <CATEGORY>_<GATE>_<DESCRIPTION>

Usage:
    from core.management.cycle3.doctrine.thresholds import (
        DTE_EMERGENCY_ROLL,
        DELTA_ITM_EMERGENCY,
        PNL_HARD_STOP_BW,
        ...
    )

RAG citations reference the original options theory textbooks:
    McMillan  — "Options as a Strategic Investment"
    Natenberg — "Option Volatility and Pricing"
    Passarelli — "Trading Options Greeks"
    Given     — "No-Hype Options Trading"
    Jabbour   — "The Option Trader Handbook"
    Augen     — "Trading Options at Expiration"
    Hull      — "Options, Futures, and Other Derivatives"
    Cohen     — "The Bible of Options Strategies"
    Murphy    — "Technical Analysis of Financial Markets"
    Bennett   — "Trading Volatility"
    Sinclair  — "Volatility Trading"
"""

# =============================================================================
# DTE Gates
# =============================================================================
DTE_EMERGENCY_ROLL = 7          # McMillan Ch.3: pin risk + gamma acceleration
DTE_CUSHION_WINDOW = 14         # BUY_WRITE cost-basis cushion guard
DTE_INCOME_GATE = 21            # Given Ch.6: 21-DTE income roll gate
DTE_CADENCE_THRESHOLD = 30      # Cadence review / roll affordability window
DTE_THETA_ACCELERATION = 45     # Theta acceleration zone for long premium
DTE_THETA_DOMINANCE_WINDOW = 60 # Theta dominance gate for long options
DTE_LEAPS_THRESHOLD = 90        # Hull Ch.10: LEAPs are vega-dominant
DTE_LEAP_CLASSIFICATION = 180   # LEAP classification boundary
DTE_LEAPS_TENDER = 270          # Pre-LEAPS tender: consider rolling

# =============================================================================
# Delta Gates
# =============================================================================
DELTA_FLOOR_WORTHLESS = 0.10    # Option non-responsive: no delta sensitivity
DELTA_FAR_OTM = 0.20            # Far OTM cadence signal
DELTA_DIVIDEND_ASSIGNMENT = 0.50 # McMillan Ch.2: early exercise risk
DELTA_PRE_ITM_WARNING = 0.55    # McMillan Ch.3: early warning — act while credit viable
DELTA_BEHAVIORAL_ITM = 0.60     # ATM behavioral ITM (CC gate 2)
DELTA_ITM_EMERGENCY = 0.70      # Passarelli Ch.5: assignment imminent — roll NOW
DELTA_PORTFOLIO_REDUNDANCY = 0.80 # McMillan Ch.4: portfolio delta redundancy
DELTA_DEEP_ITM_TERMINAL = 0.90  # Jabbour Ch.8: deep ITM — rolling uneconomical

# =============================================================================
# P&L / Drift Gates
# =============================================================================
PNL_DEEP_LOSS_STOP = -0.50      # McMillan Ch.1: capital preservation
PNL_ABSOLUTE_DAMAGE = -0.40     # Absolute damage override threshold
PNL_LEAPS_TRIM = -0.35          # McMillan Ch.4: LEAPS wider loss tolerance
PNL_THESIS_STALENESS = -0.30    # Thesis staleness gate
PNL_SIGNIFICANT_LOSS = -0.25    # Passarelli Ch.6: elevated monitoring
PNL_HARD_STOP_BW = -0.20        # McMillan Ch.3: buy-write hard stop from net cost
PNL_APPROACHING_HARD_STOP = -0.15 # McMillan Ch.3: approaching hard stop warning
PNL_WEAKENING_LOSS = -0.10      # Natenberg Ch.8: early deterioration
PNL_POST_EARNINGS_DROP = -0.08  # Post-earnings gap assessment
PNL_DRIFT_STRUCTURE_BROKEN = -0.05 # Structure broken + drift threshold
PNL_THESIS_STALENESS_DRIFT_FLOOR = -0.03 # Thesis staleness: minimum drift
DRIFT_MAGNITUDE_ADVERSE = 0.02  # Adverse drift magnitude threshold

# =============================================================================
# Premium / Capture Gates
# =============================================================================
PREMIUM_CAPTURE_TARGET = 0.50   # Passarelli Ch.6: 50% profit capture rule
EXTRINSIC_THETA_EXHAUSTED = 0.20 # Extrinsic < $0.20 — no theta value left
EXTRINSIC_CREDIT_VIABLE = 0.25  # 25% extrinsic: viable for credit roll
EXTRINSIC_CREDIT_STRONG = 0.40  # 40% extrinsic: strong credit window

# =============================================================================
# Gamma Gates
# =============================================================================
GAMMA_DANGER_RATIO = 1.5        # Natenberg Ch.7: gamma/theta ratio dominance
GAMMA_DOMINANCE_RATIO = 2.0     # Gamma drag > 2x theta — buyback territory
GAMMA_EATING_THETA = 0.80       # Gamma consuming 80% of theta
GAMMA_CONVEXITY_MINIMUM = 0.02  # Minimum gamma for convexity gate
GAMMA_ATM_PROXIMITY = 0.05      # 5% from strike — gamma danger zone
GAMMA_MONEYNESS_GUARD = 0.30    # Black-Scholes: gamma near-zero beyond 30% OTM

# =============================================================================
# Carry / Margin Gates
# =============================================================================
CARRY_INVERSION_SEVERE = 1.5    # Given Ch.6: severe carry inversion (margin >= 1.5x theta)
CARRY_INVERSION_MILD = 1.0      # Margin cost >= theta — baseline inversion
YIELD_ESCALATION_THRESHOLD = 0.05 # Yield < 5%: DTE < 14 escalation gate

# =============================================================================
# OI Gates (Open Interest)
# =============================================================================
OI_ABSOLUTE_FLOOR = 25          # Murphy (0.704): no viable exit market
OI_DETERIORATION_SEVERE = 0.25  # Murphy: OI dropped > 75% from entry
OI_DETERIORATION_WARNING = 0.50 # Murphy: OI halved — monitor closely

# =============================================================================
# IV / Volatility Gates
# =============================================================================
IV_VOL_STOP_RISE = 0.50         # Given (0.677) + Bennett (0.719): > 50% IV rise from entry
IV_CONTRACTION_THRESHOLD = 0.70 # IV contracted > 30% from entry (ratio = IV_now / IV_entry)
IV_BUYBACK_TRIGGER_CEILING = 0.35 # IV low threshold for buyback trigger
IV_PERCENTILE_BOTTOM_QUARTILE = 25 # IV bottom quartile — selling edge gone
IV_PERCENTILE_RECENT_PEAK = 70  # IV near recent peak — elevated
IV_PERCENTILE_ROLL_AFFORDABLE = 50 # IV <= 50th percentile — roll affordable
IV_WHEEL_MIN = 0.25             # Natenberg Ch.12: minimum IV for CC premium
HV_IV_HOSTILE_RATIO = 1.20      # HV > IV x 1.20: vol hostile for long premium
HV_IV_DRAG_PRESENT = 1.10       # HV > IV x 1.10: vol drag present
HV_IV_DRAG_THRESHOLD = 1.05     # HV > IV x 1.05: mild vol drag

# =============================================================================
# IV/HV Ratio Calibration (Long Option Vol Confidence)
# =============================================================================
VOL_CONFIDENCE_OPTIMAL_LOW = 0.85   # IV/HV ratio calibration zone lower
VOL_CONFIDENCE_OPTIMAL_HIGH = 1.15  # IV/HV ratio calibration zone upper
VOL_CONFIDENCE_SLIGHT_MISPRICING_LOW = 0.70  # Slight mispricing lower
VOL_CONFIDENCE_SLIGHT_MISPRICING_HIGH = 1.30 # Slight mispricing upper
VOL_CONFIDENCE_UNDERPRICED = 0.80   # IV/HV underpriced threshold

# =============================================================================
# EV / Expected Move Gates
# =============================================================================
EV_NOISE_FLOOR_INCOME = 50.0    # Passarelli Ch.6: below this, EV difference is noise
EV_NOISE_FLOOR_DIRECTIONAL = 75.0 # Directional EV noise floor
EV_FEASIBILITY_UNFEASIBLE = 1.5 # EV_Feasibility_Ratio > 1.5: unfeasible forward expectancy
EV_FEASIBILITY_ROLL_CONDITION = 1.0 # Roll condition threshold
EV_FEASIBILITY_ESCAPE = 0.50   # EV_Feasibility < 0.50: escape condition

# =============================================================================
# Trend / ADX / Momentum Gates
# =============================================================================
ADX_STRONG_TREND = 30           # Strong trend confirmation (cf. config/indicator_settings.py adx_emerging=30)
ADX_TRENDING = 25               # McMillan Ch.4: doctrine trending gate (lower bar than scan's adx_trending=40)
ADX_WEAK_TREND = 20             # Weak trend strength (cf. config/indicator_settings.py adx_range_bound=20)
ADX_VERY_WEAK_TREND = 18        # Very weak trend
ADX_COLLAPSE = 15               # Absolute trend collapse signal

# =============================================================================
# RSI Gates
# =============================================================================
RSI_BEARISH_OVERSOLD = 40       # Bearish / oversold signal
RSI_BOTTOMING_REVERSAL = 42     # Bottoming reversal for LEAPS
RSI_OVERSOLD_TERRITORY = 45     # Oversold territory context
RSI_BROKEN_STRUCTURE_CALLS = 48 # Broken structure for call positions
RSI_NEUTRAL = 50                # RSI neutral default
RSI_PUTS_OVERBOUGHT = 52        # Bearish for puts / overbought signal
RSI_REVERSAL_FAILURE_EXIT = 65  # Reversal failure — exit

# =============================================================================
# ROC / Momentum Gates
# =============================================================================
ROC5_ACCELERATING_BUYBACK = 2.5 # ROC5 > 2.5: accelerating up (buyback trigger)
ROC_MOMENTUM_THRESHOLD = 2.0    # ROC / momentum base threshold
ROC5_BREAKOUT_DOWN = -2.0       # ROC5 < -2.0: breakout down
ROC10_BREAKDOWN_ACCELERATION = -4.0 # ROC10 < -4.0: breakdown acceleration
ROC5_ADVERSE = 1.5              # ROC5 >= 1.5: adverse for long calls
ROC5_ADVERSE_PUTS = -1.5        # ROC5 <= -1.5: adverse for long puts

# =============================================================================
# Bollinger Band / Compression Gates
# =============================================================================
BB_Z_COMPRESSION_RELEASING = 0.5  # Compression releasing upward
BB_Z_COMPRESSION_THRESHOLD = -0.5 # No compression signal
BB_Z_DEEP_COMPRESSION = -0.8      # Deep compression
BB_Z_DECOMPRESSION_DELTA = 0.15   # Compression resolving threshold
BB_Z_COMPRESSION_DELTA = -0.05    # Deepening compression delta
BB_Z_DECOMPRESSION_DELTA_UP = 0.05 # Decompression delta threshold

# =============================================================================
# Momentum Slope Gates
# =============================================================================
MOM_SLOPE_COMPRESSION_COILING = -0.015 # Compression coiling threshold
MOM_SLOPE_BOTTOMING = -0.01    # Momentum slope bottoming
MOM_SLOPE_CHANGE_SENSITIVITY = 0.002 # Momentum slope change detection

# =============================================================================
# Choppiness / KER Gates
# =============================================================================
CHOPPINESS_FIBONACCI_HIGH = 61.8 # Fibonacci choppiness threshold
CHOPPINESS_RANGE_BOUND = 55     # Range-bound signal
CHOPPINESS_BASE = 50            # Base choppiness threshold
KER_VERY_LOW = 0.35             # Kaufman Efficiency: very inefficient movement
KER_HIGH = 0.55                 # Kaufman Efficiency: efficient directional move

# =============================================================================
# Strike / Moneyness Proximity Gates
# =============================================================================
PRICE_PROXIMITY_TARGET = 0.02   # Within 2% of price target
STRIKE_PROXIMITY_NARROW = 0.03  # Within 3% of strike
STRIKE_PROXIMITY_ATM = 0.05     # Within 5% ATM
STRIKE_PROXIMITY_EARNINGS = 0.20 # Within 20% of strike (earnings gate)
MONEYNESS_SANITY_GUARD = 0.30   # 30% OTM moneyness sanity limit
BREAKOUT_THROUGH_STRIKE = 1.01  # Stock > strike × 1.01: breakout

# =============================================================================
# Time Value / Intrinsic Gates
# =============================================================================
TIME_VALUE_EXHAUSTED = 0.10     # Time value < 10%: deeply ITM exit
TIME_VALUE_THETA_EFFICIENCY = 0.40 # Time value >= 40%: theta efficiency trigger
INTRINSIC_DEEPLY_ITM = 0.60    # Intrinsic > 60%: deeply ITM roll trigger
THETA_CONSUMPTION_GATE = 0.75   # Theta × DTE >= 75% time value

# =============================================================================
# Option Gain / Profit Gates
# =============================================================================
OPTION_GAIN_DOUBLE = 1.0        # 100% gain: doubling
OPTION_GAIN_FIFTY_PCT = 0.50    # 50% gain
OPTION_GAIN_THIRTY_PCT = 0.30   # 30% gain: profit capture / theta efficiency
OPTION_GAIN_TWENTYFIVE_PCT = 0.25 # 25% gain: theta efficiency warning
OPTION_GAIN_ALREADY_WINNING = 0.15 # 15% gain: winner exemption
OPTION_GAIN_WINNING_THRESHOLD = 0.05 # 5% gain: winning position

# =============================================================================
# Lifecycle / Time Gates
# =============================================================================
LIFECYCLE_MIN_CONSUMPTION_PCT = 0.10 # 10% of original DTE consumed
POSITION_AGE_THESIS_DEGRADATION_MIN = 2 # Days before thesis degradation applies
DAYS_IN_TRADE_MATURITY = 5      # Days for trade maturity assessment
CATALYST_WINDOW = 10            # Catalyst within 10 days
CATALYST_WINDOW_EXTENDED = 14   # Extended catalyst window
EARNINGS_NOTE_WINDOW = 30       # Earnings within 30 days (LEAPS note)
STANDARD_ROLL_DTE = 45          # Standard roll DTE assumption

# =============================================================================
# Theta / Daily Carry Gates
# =============================================================================
THETA_MATERIAL_DAILY_COST = 25  # Theta > $25/day: material carry cost
THETA_BLEED_DAILY_PCT = 3.0     # Theta bleed > 3%/day: long premium flag

# =============================================================================
# Conviction / Streak Gates
# =============================================================================
CONVICTION_DETERIORATION_STREAK = 3 # Delta deterioration streak minimum

# =============================================================================
# Quantity / Contract Gates
# =============================================================================
SHARES_CC_ELIGIBLE = 100        # McMillan Ch.3: shares needed for CC

# =============================================================================
# Dividend Assignment Gates
# =============================================================================
DIVIDEND_DAYS_CRITICAL = 2      # CRITICAL urgency threshold
DIVIDEND_DAYS_WARNING = 5       # HIGH urgency threshold

# =============================================================================
# Wheel Assessment Gates
# =============================================================================
WHEEL_BASIS_DISCOUNT = 0.97     # 3% discount to current spot
WHEEL_DELTA_UTIL_MAX = 15.0     # McMillan Ch.3: single-ticker delta cap

# =============================================================================
# Compression Resolving Gates
# =============================================================================
COMPRESSION_RESOLVING_DOWN = -0.005 # Compression resolving downward threshold

# =============================================================================
# EV Noise Floor Multiplier
# =============================================================================
CONTRACT_MULTIPLIER = 100       # Options contracts: 100 shares per contract

# =============================================================================
# Signal Hub — Institutional Signal Thresholds (Murphy/Raschke/Bulkowski)
# =============================================================================
OBV_SLOPE_DISTRIBUTION_WARN = -10.0     # Murphy Ch.7: significant distribution
OBV_SLOPE_FLAT_THRESHOLD = 3.0          # OBV barely moving — no conviction
RS_SPY_SIGNIFICANT = 5.0                # Murphy 0.740: meaningful relative strength

# =============================================================================
# Consecutive Debit Roll Hard Stop — McMillan Ch.3 / Given Ch.6
# =============================================================================
CONSECUTIVE_DEBIT_ROLLS_HARD_STOP = 3   # 3+ consecutive net-debit rolls = structural chasing

# =============================================================================
# Fading Winner / Trailing Protection — McMillan Ch.4 / Jabbour Ch.11
# =============================================================================
# =============================================================================
# Hysteresis — Direction-Adverse Gate Stabilization
# Prevents EXIT↔HOLD flip-flop on positions near gate thresholds.
# Band: once EXIT triggers, signal must clear by a margin to flip to HOLD.
# =============================================================================
HYSTERESIS_ROC5_EXIT_THRESHOLD = 1.5       # % ROC5 to trigger direction-adverse EXIT (raw fallback)
HYSTERESIS_ROC5_CLEAR_THRESHOLD = 0.5      # % ROC5 to CLEAR a prior EXIT (raw fallback)
HYSTERESIS_PNL_CLEAR_MARGIN = 0.05         # 5pp P&L improvement needed to clear EXIT

# =============================================================================
# Sigma-Normalized Direction-Adverse Thresholds — Natenberg Ch.5 / Hull Ch.2
# Replaces fixed-percent ROC5/drift gates with z-score normalization using
# the stock's own realized volatility. A 1.5% move on a 60% HV stock (z=0.4)
# is noise; the same move on a 15% HV stock (z=1.6) is signal.
# Raw-percent thresholds above are kept as fallback when HV_20D is unavailable.
# =============================================================================
SIGMA_ROC5_Z_ADVERSE = 1.5                 # z-score: ROC5 / (daily_sigma * sqrt(5))
SIGMA_DRIFT_Z_ADVERSE = 2.0                # z-score: Price_Drift_Pct / daily_sigma
SIGMA_ROC5_Z_CLEAR = 0.5                   # z-score to CLEAR hysteresis (1.0σ band)
SIGMA_DAILY_VOL_FLOOR = 0.005              # daily σ floor (~8% annualized) — prevents
                                            #   absurd z-scores on ultra-low-vol names
SIGMA_DRIFT_STALENESS_Z = 3.0              # z-score for thesis staleness drift check
SIGMA_DRIFT_Z_CONFIRMING = 1.0             # z-score: drift in CONFIRMING direction
                                            #   must exceed 1.0σ to count as "direction working"
                                            #   (suppresses false theta-bleed decay escalation)

# =============================================================================
# Cross-Gate Prior-EXIT Persistence — McMillan Ch.4 / Passarelli Ch.2
# When ANY gate produced EXIT yesterday and today's best proposal is HOLD,
# the EXIT persists unless conditions materially improved.
# Prevents one-day EXIT→HOLD flip-flops that erode user trust.
# =============================================================================
PRIOR_EXIT_PNL_RECOVERY_REQUIRED = 0.05   # 5pp P&L improvement to clear a prior EXIT
                                           # (e.g., from -25% to -20%)
PRIOR_EXIT_PRICE_MOVE_CLEAR = 0.02        # 2% favorable price move clears a prior EXIT
                                           # (stock moved strongly in option's direction)

# =============================================================================
# Recovery Premium Mode — Jabbour Ch.4 / McMillan Ch.3
# Activated for damaged buy-writes optimizing multi-cycle basis reduction.
# Success metric: cumulative basis reduction efficiency, not short-term EV.
# =============================================================================
# Entry criteria
RECOVERY_PREMIUM_LOSS_FLOOR = 0.10        # ≥10% loss from effective cost to enter mode
RECOVERY_PREMIUM_MIN_CYCLES = 1           # At least 1 completed income cycle
RECOVERY_PREMIUM_IV_FLOOR = 0.15          # 15% IV minimum for viable premium
RECOVERY_PREMIUM_IV_RANK_FLOOR = 5        # IV_Rank minimum (not at absolute bottom)

# Strike discipline
RECOVERY_STRIKE_COST_BASIS_BUFFER = 0.02  # 2% above cost basis = safe strike floor
RECOVERY_STRIKE_BELOW_BASIS_WARN = 0.05   # >5% below cost basis = strong warning
RECOVERY_STRIKE_NEAR_SPOT_FLOOR = 0.03    # Don't sell strikes within 3% of spot on depressed stock

# Sell timing — IV regime awareness
RECOVERY_IV_RANK_FAVORABLE = 40           # IV_Rank ≥ 40: favorable premium environment
RECOVERY_IV_RANK_POOR = 20                # IV_Rank < 20: poor premium — wait if possible
RECOVERY_RALLY_PCT_TRIGGER = 3.0           # ROC5 ≥ 3.0% → favorable write window (stored as raw pct)

# Basis reduction targets
RECOVERY_ANNUALIZED_YIELD_TARGET = 0.08   # 8% annualized yield from premium collection
RECOVERY_MONTHS_TO_BREAKEVEN_EXIT = 60    # >60 months to breakeven → recovery uneconomical

# Roth / retirement account
RECOVERY_ROTH_STRIKE_BUFFER = 0.05        # Roth: 5% above cost basis preferred (extra caution)
RECOVERY_ROTH_MAX_ASSIGNMENT_LOSS = 0.10  # Roth: max 10% loss on assignment (capital scarce)

# =============================================================================
# Forward-Economics Hard Stop Guards — McMillan Ch.3 / Jabbour Ch.4 / Given Ch.6
# "Sunk loss should not alone drive exit decisions — forward EV is the
# primary lever."  When a hard stop triggers EXIT, check if forward income
# can recover the position within a reasonable horizon before exiting.
# =============================================================================
# ── Write-Off / Micro-Position Thresholds ──────────────────────────────
WRITEOFF_MIN_MARKET_VALUE = 100.0            # Skip doctrine for positions < $100 market value

FORWARD_ECON_MONTHS_BW_HARD_STOP = 18       # BW -20% hard stop: if breakeven <18mo, downgrade to HOLD
FORWARD_ECON_MONTHS_BW_APPROACHING = 24     # BW approaching stop: if breakeven <24mo, reduce urgency
FORWARD_ECON_MONTHS_STOCK_DEEP_LOSS = 36    # Stock -50%: if CC overlay breakeven <36mo, downgrade
FORWARD_ECON_IV_MIN_VIABLE = 0.15           # Minimum IV for viable premium generation
FORWARD_ECON_DRAWDOWN_THETA_OFFSET = 0.02   # Circuit breaker: 2pp drawdown offset for net theta carry
FORWARD_ECON_THETA_ANNUAL_FLOOR = 0.04      # Circuit breaker: 4% annualized theta/equity = meaningful

# =============================================================================
# Macro Catalyst Protection — Bennett / Natenberg Ch.12
# Long-premium positions near HIGH-impact macro events (FOMC/CPI/NFP)
# should NOT be exited for theta-dominant reasons — the macro event IS
# the expected catalyst that justifies holding through theta bleed.
# =============================================================================
MACRO_CATALYST_DAYS_THRESHOLD = 5          # Days to HIGH macro event that triggers protection
MACRO_CATALYST_DTE_MIN = 14                # Minimum DTE for macro protection to apply
                                           # (below 14d, theta acceleration overrides macro hope)

# Extended macro window for event-driven long premium positions.
# When a long option has high IV, low theta bleed, intact thesis, and sufficient DTE,
# a catalyst 6-7 days out is still close enough to override EXIT persistence.
# Avoids false EXIT persistence on boundary cases (e.g., FOMC at 6d with 43 DTE put).
MACRO_CATALYST_DAYS_EXTENDED = 7           # Extended window for qualifying long premium
MACRO_CATALYST_EXTENDED_DTE_MIN = 21       # Minimum DTE for extended window
MACRO_CATALYST_EXTENDED_IV_PCTILE_MIN = 85 # IV percentile floor (vol edge present)
MACRO_CATALYST_EXTENDED_THETA_BLEED_MAX = 0.02  # Max theta bleed %/day of premium

# ── Precedence: weak-entry leash vs sigma-normalized adverse filter ──────────
# Direction-adverse EXIT requires BOTH conditions (AND-gate):
#   1. Sigma adverse:  roc5_z >= 1.5σ  OR  drift_z >= 2.0σ   (move is statistically unusual)
#   2. P&L below leash: pnl < threshold (STRONG=-25%, NEUTRAL=-15%, WEAK=-10%)
#
# Neither alone triggers EXIT.  Consequences:
#   - High-HV stock (AMD, HV=50%): sigma filter is forgiving (3% ROC5 = z=0.42).
#     Even a WEAK entry at -12% P&L holds — the move is noise, not signal.
#   - Low-HV stock (JNJ, HV=12%): sigma filter is strict (3% ROC5 = z=1.77).
#     WEAK entry at -10% triggers EXIT — the move is real for this stock.
#   - Slow grind with no single sigma-adverse day: caught by thesis-not-confirming
#     gate (separate path), not direction-adverse. The leash doesn't help here
#     because the sigma filter never fires.
#
# Design rationale (Natenberg Ch.5 + McMillan Ch.4):
#   Sigma asks "is the move unusual?"  Leash asks "has the move hurt enough?"
#   Both must agree to avoid premature exits on volatile stocks and to give
#   strong-conviction entries more room even on quiet names.

# =============================================================================
# Fading Winner / Trailing Protection — McMillan Ch.4 / Jabbour Ch.11
# =============================================================================
MFE_SIGNIFICANT = 0.20             # 20% peak gain = significant winner
MFE_GIVEBACK_EXIT = 0.50           # gave back 50%+ of MFE → protect remaining (base, vol-scaled)
MFE_GIVEBACK_HV_LOW = 0.15         # Carver: HV below this → tighter giveback (low-vol name)
MFE_GIVEBACK_HV_HIGH = 0.40        # Carver: HV above this → wider giveback (high-vol name)
MFE_GIVEBACK_TIGHT = 0.35          # Low-vol tighter giveback (35% instead of 50%)
MFE_GIVEBACK_WIDE = 0.60           # High-vol wider giveback (60% instead of 50%)
MFE_ROUNDTRIP_PNL = 0.05           # if current P&L < 5% after 20%+ MFE → round-trip
WINNER_SCALE_UP_MFE = 0.15         # 15%+ MFE + thesis intact → eligible for scale-up (income)
WINNER_SCALE_UP_PNL_MIN = 0.10     # must still be >10% in profit to scale (income)

# Directional winner scale-up — Murphy 0.773: stricter than income because
# directional positions have no premium cushion and full downside exposure.
DIRECTIONAL_SCALE_UP_MFE = 0.25    # 25%+ MFE required (vs 15% income)
DIRECTIONAL_SCALE_UP_PNL_MIN = 0.15 # 15%+ P&L required (vs 10% income)
DIRECTIONAL_PYRAMID_MAX_TIER = 0   # Only 1 add allowed (tier 0 only)

# =============================================================================
# Pyramid Sizing — McMillan Ch.4 / Passarelli Ch.6
# Decreasing layers: first add = 60% of base, second = 30%, then full.
# =============================================================================
PYRAMID_TIER_0_RATIO = 0.60        # First scale-up: 60% of frozen base quantity
PYRAMID_TIER_1_RATIO = 0.30        # Second scale-up: 30% of frozen base quantity
PYRAMID_PNL_MIN = 0.05             # 5% P&L minimum for pyramid eligibility

# =============================================================================
# Income TRIM — McMillan Ch.4 / Passarelli Ch.6
# Partial close for multi-contract positions based on wave phase.
# =============================================================================
INCOME_TRIM_PEAK_PCT = 0.25        # PEAKING: trim 25% of contracts
INCOME_TRIM_EXHAUSTION_PCT = 0.50  # EXHAUSTED/FADING: trim 50% of contracts
INCOME_TRIM_MIN_QUANTITY = 2       # Need ≥2 contracts to trim

# =============================================================================
# Defensive TRIM — Sinclair Ch.7 / Chan Ch.4
# Partial close for multi-contract positions approaching hard stop.
# Reduces exposure while preserving recovery optionality.
# Sinclair: "Fractional Kelly — reduce size as edge degrades."
# Chan: "Momentum-based stops benefit from partial exits."
# =============================================================================
DEFENSIVE_TRIM_PCT = 0.30             # Trim 30% of contracts at approaching stop
DEFENSIVE_TRIM_MIN_QUANTITY = 3       # Need ≥3 contracts (trimming 1-of-2 is too coarse)

# =============================================================================
# Thesis Review Scorer — Passarelli Ch.2 / McMillan Ch.4
# Replaces ambiguous REVIEW with scored verdict → concrete executable action.
# =============================================================================
THESIS_REVIEW_REAFFIRMED_FLOOR = 30   # Score >= 30 → REAFFIRMED (thesis intact → HOLD)
THESIS_REVIEW_MONITORING_FLOOR = 10   # Score 10-29 → MONITORING (watch → HOLD_WITH_CAUTION)
THESIS_REVIEW_WEAKENED_FLOOR = -19    # Score -19 to 9 → WEAKENED (deteriorating → TRIM/HOLD_WITH_CAUTION)
# Score <= -20 → DEGRADED (structurally broken → EXIT)
THESIS_REVIEW_CATEGORY_MAX = 20       # Per-category score ceiling
THESIS_REVIEW_CATEGORY_MIN = -20      # Per-category score floor
THESIS_REVIEW_YOUNG_TRADE_DAYS = 5    # Positions ≤5 days get noise protection
THESIS_REVIEW_STALE_TRADE_DAYS = 10   # Positions ≥10 days with no progress penalized
