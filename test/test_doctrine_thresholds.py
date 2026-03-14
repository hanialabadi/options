"""
Threshold lock tests — asserts every constant in doctrine/thresholds.py
matches the original hardcoded value from engine.py.

If a test fails, the constant was changed without updating the engine code
(or vice versa). This is a regression safety net during the refactor.
"""

import pytest
from core.management.cycle3.doctrine.thresholds import (
    # DTE Gates
    DTE_EMERGENCY_ROLL,
    DTE_CUSHION_WINDOW,
    DTE_INCOME_GATE,
    DTE_CADENCE_THRESHOLD,
    DTE_THETA_ACCELERATION,
    DTE_THETA_DOMINANCE_WINDOW,
    DTE_LEAPS_THRESHOLD,
    DTE_LEAP_CLASSIFICATION,
    DTE_LEAPS_TENDER,
    # Delta Gates
    DELTA_FLOOR_WORTHLESS,
    DELTA_FAR_OTM,
    DELTA_DIVIDEND_ASSIGNMENT,
    DELTA_PRE_ITM_WARNING,
    DELTA_BEHAVIORAL_ITM,
    DELTA_ITM_EMERGENCY,
    DELTA_PORTFOLIO_REDUNDANCY,
    DELTA_DEEP_ITM_TERMINAL,
    # P&L / Drift Gates
    PNL_DEEP_LOSS_STOP,
    PNL_ABSOLUTE_DAMAGE,
    PNL_LEAPS_TRIM,
    PNL_THESIS_STALENESS,
    PNL_SIGNIFICANT_LOSS,
    PNL_HARD_STOP_BW,
    PNL_APPROACHING_HARD_STOP,
    PNL_WEAKENING_LOSS,
    PNL_POST_EARNINGS_DROP,
    PNL_DRIFT_STRUCTURE_BROKEN,
    PNL_THESIS_STALENESS_DRIFT_FLOOR,
    DRIFT_MAGNITUDE_ADVERSE,
    # Premium / Capture Gates
    PREMIUM_CAPTURE_TARGET,
    EXTRINSIC_THETA_EXHAUSTED,
    EXTRINSIC_CREDIT_VIABLE,
    EXTRINSIC_CREDIT_STRONG,
    # Gamma Gates
    GAMMA_DANGER_RATIO,
    GAMMA_DOMINANCE_RATIO,
    GAMMA_EATING_THETA,
    GAMMA_CONVEXITY_MINIMUM,
    GAMMA_ATM_PROXIMITY,
    GAMMA_MONEYNESS_GUARD,
    # Carry / Margin Gates
    CARRY_INVERSION_SEVERE,
    CARRY_INVERSION_MILD,
    YIELD_ESCALATION_THRESHOLD,
    # OI Gates
    OI_ABSOLUTE_FLOOR,
    OI_DETERIORATION_SEVERE,
    OI_DETERIORATION_WARNING,
    # IV / Volatility Gates
    IV_VOL_STOP_RISE,
    IV_CONTRACTION_THRESHOLD,
    IV_BUYBACK_TRIGGER_CEILING,
    IV_PERCENTILE_BOTTOM_QUARTILE,
    IV_PERCENTILE_RECENT_PEAK,
    IV_PERCENTILE_ROLL_AFFORDABLE,
    IV_WHEEL_MIN,
    HV_IV_HOSTILE_RATIO,
    HV_IV_DRAG_PRESENT,
    HV_IV_DRAG_THRESHOLD,
    # IV/HV Ratio Calibration
    VOL_CONFIDENCE_OPTIMAL_LOW,
    VOL_CONFIDENCE_OPTIMAL_HIGH,
    VOL_CONFIDENCE_SLIGHT_MISPRICING_LOW,
    VOL_CONFIDENCE_SLIGHT_MISPRICING_HIGH,
    VOL_CONFIDENCE_UNDERPRICED,
    # EV / Expected Move Gates
    EV_NOISE_FLOOR_INCOME,
    EV_NOISE_FLOOR_DIRECTIONAL,
    EV_FEASIBILITY_UNFEASIBLE,
    EV_FEASIBILITY_ROLL_CONDITION,
    EV_FEASIBILITY_ESCAPE,
    # Trend / ADX
    ADX_STRONG_TREND,
    ADX_TRENDING,
    ADX_WEAK_TREND,
    ADX_VERY_WEAK_TREND,
    ADX_COLLAPSE,
    # RSI
    RSI_BEARISH_OVERSOLD,
    RSI_BOTTOMING_REVERSAL,
    RSI_OVERSOLD_TERRITORY,
    RSI_BROKEN_STRUCTURE_CALLS,
    RSI_NEUTRAL,
    RSI_PUTS_OVERBOUGHT,
    RSI_REVERSAL_FAILURE_EXIT,
    # ROC
    ROC5_ACCELERATING_BUYBACK,
    ROC_MOMENTUM_THRESHOLD,
    ROC5_BREAKOUT_DOWN,
    ROC10_BREAKDOWN_ACCELERATION,
    ROC5_ADVERSE,
    ROC5_ADVERSE_PUTS,
    # Bollinger Band
    BB_Z_COMPRESSION_RELEASING,
    BB_Z_COMPRESSION_THRESHOLD,
    BB_Z_DEEP_COMPRESSION,
    BB_Z_DECOMPRESSION_DELTA,
    BB_Z_COMPRESSION_DELTA,
    BB_Z_DECOMPRESSION_DELTA_UP,
    # Momentum Slope
    MOM_SLOPE_COMPRESSION_COILING,
    MOM_SLOPE_BOTTOMING,
    MOM_SLOPE_CHANGE_SENSITIVITY,
    # Choppiness / KER
    CHOPPINESS_FIBONACCI_HIGH,
    CHOPPINESS_RANGE_BOUND,
    CHOPPINESS_BASE,
    KER_VERY_LOW,
    KER_HIGH,
    # Strike / Moneyness Proximity
    PRICE_PROXIMITY_TARGET,
    STRIKE_PROXIMITY_NARROW,
    STRIKE_PROXIMITY_ATM,
    STRIKE_PROXIMITY_EARNINGS,
    MONEYNESS_SANITY_GUARD,
    BREAKOUT_THROUGH_STRIKE,
    # Time Value / Intrinsic
    TIME_VALUE_EXHAUSTED,
    TIME_VALUE_THETA_EFFICIENCY,
    INTRINSIC_DEEPLY_ITM,
    THETA_CONSUMPTION_GATE,
    # Option Gain / Profit
    OPTION_GAIN_DOUBLE,
    OPTION_GAIN_FIFTY_PCT,
    OPTION_GAIN_THIRTY_PCT,
    OPTION_GAIN_TWENTYFIVE_PCT,
    OPTION_GAIN_ALREADY_WINNING,
    OPTION_GAIN_WINNING_THRESHOLD,
    # Lifecycle / Time
    LIFECYCLE_MIN_CONSUMPTION_PCT,
    POSITION_AGE_THESIS_DEGRADATION_MIN,
    DAYS_IN_TRADE_MATURITY,
    CATALYST_WINDOW,
    CATALYST_WINDOW_EXTENDED,
    EARNINGS_NOTE_WINDOW,
    STANDARD_ROLL_DTE,
    # Theta / Daily Carry
    THETA_MATERIAL_DAILY_COST,
    THETA_BLEED_DAILY_PCT,
    # Conviction / Streak
    CONVICTION_DETERIORATION_STREAK,
    # Quantity / Contract
    SHARES_CC_ELIGIBLE,
    # Dividend Assignment
    DIVIDEND_DAYS_CRITICAL,
    DIVIDEND_DAYS_WARNING,
    # Wheel Assessment
    WHEEL_BASIS_DISCOUNT,
    WHEEL_DELTA_UTIL_MAX,
    # Compression Resolving
    COMPRESSION_RESOLVING_DOWN,
    # EV Noise Floor Multiplier
    CONTRACT_MULTIPLIER,
)

from core.management.cycle3.doctrine.gate_result import (
    STATE_ACTIONABLE,
    STATE_NEUTRAL_CONFIDENT,
    STATE_UNCERTAIN,
    STATE_BLOCKED_GOVERNANCE,
    STATE_UNRESOLVED_IDENTITY,
    REASON_ATTRIBUTION_QUALITY_LOW,
    REASON_IV_AUTHORITY_MISSING,
    REASON_SCHWAB_IV_EXPIRED,
    REASON_DELTA_GAMMA_INCOMPLETE,
    REASON_STOCK_LEG_NOT_AVAILABLE,
    REASON_CYCLE2_SIGNAL_INCOMPLETE,
    REASON_STOCK_AUTHORITY_VIOLATION,
    REASON_STRUCTURAL_DATA_INCOMPLETE,
    AUTHORITY_REQUIRED,
    AUTHORITY_CONTEXTUAL,
    AUTHORITY_SUPPORTIVE,
    AUTHORITY_NON_AUTHORITATIVE,
    fire_gate,
    skip_gate,
)


# =====================================================================
# DTE Gates
# =====================================================================

class TestDTEThresholds:
    def test_dte_emergency_roll(self):
        assert DTE_EMERGENCY_ROLL == 7

    def test_dte_cushion_window(self):
        assert DTE_CUSHION_WINDOW == 14

    def test_dte_income_gate(self):
        assert DTE_INCOME_GATE == 21

    def test_dte_cadence_threshold(self):
        assert DTE_CADENCE_THRESHOLD == 30

    def test_dte_theta_acceleration(self):
        assert DTE_THETA_ACCELERATION == 45

    def test_dte_theta_dominance_window(self):
        assert DTE_THETA_DOMINANCE_WINDOW == 60

    def test_dte_leaps_threshold(self):
        assert DTE_LEAPS_THRESHOLD == 90

    def test_dte_leap_classification(self):
        assert DTE_LEAP_CLASSIFICATION == 180

    def test_dte_leaps_tender(self):
        assert DTE_LEAPS_TENDER == 270


# =====================================================================
# Delta Gates
# =====================================================================

class TestDeltaThresholds:
    def test_delta_floor_worthless(self):
        assert DELTA_FLOOR_WORTHLESS == 0.10

    def test_delta_far_otm(self):
        assert DELTA_FAR_OTM == 0.20

    def test_delta_dividend_assignment(self):
        assert DELTA_DIVIDEND_ASSIGNMENT == 0.50

    def test_delta_pre_itm_warning(self):
        assert DELTA_PRE_ITM_WARNING == 0.55

    def test_delta_behavioral_itm(self):
        assert DELTA_BEHAVIORAL_ITM == 0.60

    def test_delta_itm_emergency(self):
        assert DELTA_ITM_EMERGENCY == 0.70

    def test_delta_portfolio_redundancy(self):
        assert DELTA_PORTFOLIO_REDUNDANCY == 0.80

    def test_delta_deep_itm_terminal(self):
        assert DELTA_DEEP_ITM_TERMINAL == 0.90


# =====================================================================
# P&L / Drift Gates
# =====================================================================

class TestPnLThresholds:
    def test_pnl_deep_loss_stop(self):
        assert PNL_DEEP_LOSS_STOP == -0.50

    def test_pnl_absolute_damage(self):
        assert PNL_ABSOLUTE_DAMAGE == -0.40

    def test_pnl_leaps_trim(self):
        assert PNL_LEAPS_TRIM == -0.35

    def test_pnl_thesis_staleness(self):
        assert PNL_THESIS_STALENESS == -0.30

    def test_pnl_significant_loss(self):
        assert PNL_SIGNIFICANT_LOSS == -0.25

    def test_pnl_hard_stop_bw(self):
        assert PNL_HARD_STOP_BW == -0.20

    def test_pnl_approaching_hard_stop(self):
        assert PNL_APPROACHING_HARD_STOP == -0.15

    def test_pnl_weakening_loss(self):
        assert PNL_WEAKENING_LOSS == -0.10

    def test_pnl_post_earnings_drop(self):
        assert PNL_POST_EARNINGS_DROP == -0.08

    def test_pnl_drift_structure_broken(self):
        assert PNL_DRIFT_STRUCTURE_BROKEN == -0.05

    def test_pnl_thesis_staleness_drift_floor(self):
        assert PNL_THESIS_STALENESS_DRIFT_FLOOR == -0.03

    def test_drift_magnitude_adverse(self):
        assert DRIFT_MAGNITUDE_ADVERSE == 0.02


# =====================================================================
# Premium / Capture Gates
# =====================================================================

class TestPremiumThresholds:
    def test_premium_capture_target(self):
        assert PREMIUM_CAPTURE_TARGET == 0.50

    def test_extrinsic_theta_exhausted(self):
        assert EXTRINSIC_THETA_EXHAUSTED == 0.20

    def test_extrinsic_credit_viable(self):
        assert EXTRINSIC_CREDIT_VIABLE == 0.25

    def test_extrinsic_credit_strong(self):
        assert EXTRINSIC_CREDIT_STRONG == 0.40


# =====================================================================
# Gamma Gates
# =====================================================================

class TestGammaThresholds:
    def test_gamma_danger_ratio(self):
        assert GAMMA_DANGER_RATIO == 1.5

    def test_gamma_dominance_ratio(self):
        assert GAMMA_DOMINANCE_RATIO == 2.0

    def test_gamma_eating_theta(self):
        assert GAMMA_EATING_THETA == 0.80

    def test_gamma_convexity_minimum(self):
        assert GAMMA_CONVEXITY_MINIMUM == 0.02

    def test_gamma_atm_proximity(self):
        assert GAMMA_ATM_PROXIMITY == 0.05

    def test_gamma_moneyness_guard(self):
        assert GAMMA_MONEYNESS_GUARD == 0.30


# =====================================================================
# Carry / Margin Gates
# =====================================================================

class TestCarryThresholds:
    def test_carry_inversion_severe(self):
        assert CARRY_INVERSION_SEVERE == 1.5

    def test_carry_inversion_mild(self):
        assert CARRY_INVERSION_MILD == 1.0

    def test_yield_escalation_threshold(self):
        assert YIELD_ESCALATION_THRESHOLD == 0.05


# =====================================================================
# OI Gates
# =====================================================================

class TestOIThresholds:
    def test_oi_absolute_floor(self):
        assert OI_ABSOLUTE_FLOOR == 25

    def test_oi_deterioration_severe(self):
        assert OI_DETERIORATION_SEVERE == 0.25

    def test_oi_deterioration_warning(self):
        assert OI_DETERIORATION_WARNING == 0.50


# =====================================================================
# IV / Volatility Gates
# =====================================================================

class TestIVThresholds:
    def test_iv_vol_stop_rise(self):
        assert IV_VOL_STOP_RISE == 0.50

    def test_iv_contraction_threshold(self):
        assert IV_CONTRACTION_THRESHOLD == 0.70

    def test_iv_buyback_trigger_ceiling(self):
        assert IV_BUYBACK_TRIGGER_CEILING == 0.35

    def test_iv_percentile_bottom_quartile(self):
        assert IV_PERCENTILE_BOTTOM_QUARTILE == 25

    def test_iv_percentile_recent_peak(self):
        assert IV_PERCENTILE_RECENT_PEAK == 70

    def test_iv_percentile_roll_affordable(self):
        assert IV_PERCENTILE_ROLL_AFFORDABLE == 50

    def test_iv_wheel_min(self):
        assert IV_WHEEL_MIN == 0.25

    def test_hv_iv_hostile_ratio(self):
        assert HV_IV_HOSTILE_RATIO == 1.20

    def test_hv_iv_drag_present(self):
        assert HV_IV_DRAG_PRESENT == 1.10

    def test_hv_iv_drag_threshold(self):
        assert HV_IV_DRAG_THRESHOLD == 1.05


# =====================================================================
# IV/HV Ratio Calibration
# =====================================================================

class TestVolConfidenceThresholds:
    def test_vol_confidence_optimal_low(self):
        assert VOL_CONFIDENCE_OPTIMAL_LOW == 0.85

    def test_vol_confidence_optimal_high(self):
        assert VOL_CONFIDENCE_OPTIMAL_HIGH == 1.15

    def test_vol_confidence_slight_mispricing_low(self):
        assert VOL_CONFIDENCE_SLIGHT_MISPRICING_LOW == 0.70

    def test_vol_confidence_slight_mispricing_high(self):
        assert VOL_CONFIDENCE_SLIGHT_MISPRICING_HIGH == 1.30

    def test_vol_confidence_underpriced(self):
        assert VOL_CONFIDENCE_UNDERPRICED == 0.80


# =====================================================================
# EV / Expected Move Gates
# =====================================================================

class TestEVThresholds:
    def test_ev_noise_floor_income(self):
        assert EV_NOISE_FLOOR_INCOME == 50.0

    def test_ev_noise_floor_directional(self):
        assert EV_NOISE_FLOOR_DIRECTIONAL == 75.0

    def test_ev_feasibility_unfeasible(self):
        assert EV_FEASIBILITY_UNFEASIBLE == 1.5

    def test_ev_feasibility_roll_condition(self):
        assert EV_FEASIBILITY_ROLL_CONDITION == 1.0

    def test_ev_feasibility_escape(self):
        assert EV_FEASIBILITY_ESCAPE == 0.50


# =====================================================================
# Trend / ADX / RSI / ROC
# =====================================================================

class TestTechnicalThresholds:
    def test_adx_strong_trend(self):
        assert ADX_STRONG_TREND == 30

    def test_adx_trending(self):
        assert ADX_TRENDING == 25

    def test_adx_weak_trend(self):
        assert ADX_WEAK_TREND == 20

    def test_adx_very_weak_trend(self):
        assert ADX_VERY_WEAK_TREND == 18

    def test_adx_collapse(self):
        assert ADX_COLLAPSE == 15

    def test_rsi_bearish_oversold(self):
        assert RSI_BEARISH_OVERSOLD == 40

    def test_rsi_bottoming_reversal(self):
        assert RSI_BOTTOMING_REVERSAL == 42

    def test_rsi_oversold_territory(self):
        assert RSI_OVERSOLD_TERRITORY == 45

    def test_rsi_broken_structure_calls(self):
        assert RSI_BROKEN_STRUCTURE_CALLS == 48

    def test_rsi_neutral(self):
        assert RSI_NEUTRAL == 50

    def test_rsi_puts_overbought(self):
        assert RSI_PUTS_OVERBOUGHT == 52

    def test_rsi_reversal_failure_exit(self):
        assert RSI_REVERSAL_FAILURE_EXIT == 65

    def test_roc5_accelerating_buyback(self):
        assert ROC5_ACCELERATING_BUYBACK == 2.5

    def test_roc_momentum_threshold(self):
        assert ROC_MOMENTUM_THRESHOLD == 2.0

    def test_roc5_breakout_down(self):
        assert ROC5_BREAKOUT_DOWN == -2.0

    def test_roc10_breakdown_acceleration(self):
        assert ROC10_BREAKDOWN_ACCELERATION == -4.0

    def test_roc5_adverse(self):
        assert ROC5_ADVERSE == 1.5

    def test_roc5_adverse_puts(self):
        assert ROC5_ADVERSE_PUTS == -1.5


# =====================================================================
# Bollinger / Momentum / Choppiness / KER
# =====================================================================

class TestCompressionThresholds:
    def test_bb_z_compression_releasing(self):
        assert BB_Z_COMPRESSION_RELEASING == 0.5

    def test_bb_z_compression_threshold(self):
        assert BB_Z_COMPRESSION_THRESHOLD == -0.5

    def test_bb_z_deep_compression(self):
        assert BB_Z_DEEP_COMPRESSION == -0.8

    def test_bb_z_decompression_delta(self):
        assert BB_Z_DECOMPRESSION_DELTA == 0.15

    def test_bb_z_compression_delta(self):
        assert BB_Z_COMPRESSION_DELTA == -0.05

    def test_bb_z_decompression_delta_up(self):
        assert BB_Z_DECOMPRESSION_DELTA_UP == 0.05

    def test_mom_slope_compression_coiling(self):
        assert MOM_SLOPE_COMPRESSION_COILING == -0.015

    def test_mom_slope_bottoming(self):
        assert MOM_SLOPE_BOTTOMING == -0.01

    def test_mom_slope_change_sensitivity(self):
        assert MOM_SLOPE_CHANGE_SENSITIVITY == 0.002

    def test_choppiness_fibonacci_high(self):
        assert CHOPPINESS_FIBONACCI_HIGH == 61.8

    def test_choppiness_range_bound(self):
        assert CHOPPINESS_RANGE_BOUND == 55

    def test_choppiness_base(self):
        assert CHOPPINESS_BASE == 50

    def test_ker_very_low(self):
        assert KER_VERY_LOW == 0.35

    def test_ker_high(self):
        assert KER_HIGH == 0.55


# =====================================================================
# Strike / Moneyness / Time Value / Gains
# =====================================================================

class TestStrikeAndValueThresholds:
    def test_price_proximity_target(self):
        assert PRICE_PROXIMITY_TARGET == 0.02

    def test_strike_proximity_narrow(self):
        assert STRIKE_PROXIMITY_NARROW == 0.03

    def test_strike_proximity_atm(self):
        assert STRIKE_PROXIMITY_ATM == 0.05

    def test_strike_proximity_earnings(self):
        assert STRIKE_PROXIMITY_EARNINGS == 0.20

    def test_moneyness_sanity_guard(self):
        assert MONEYNESS_SANITY_GUARD == 0.30

    def test_breakout_through_strike(self):
        assert BREAKOUT_THROUGH_STRIKE == 1.01

    def test_time_value_exhausted(self):
        assert TIME_VALUE_EXHAUSTED == 0.10

    def test_time_value_theta_efficiency(self):
        assert TIME_VALUE_THETA_EFFICIENCY == 0.40

    def test_intrinsic_deeply_itm(self):
        assert INTRINSIC_DEEPLY_ITM == 0.60

    def test_theta_consumption_gate(self):
        assert THETA_CONSUMPTION_GATE == 0.75

    def test_option_gain_double(self):
        assert OPTION_GAIN_DOUBLE == 1.0

    def test_option_gain_fifty_pct(self):
        assert OPTION_GAIN_FIFTY_PCT == 0.50

    def test_option_gain_thirty_pct(self):
        assert OPTION_GAIN_THIRTY_PCT == 0.30

    def test_option_gain_twentyfive_pct(self):
        assert OPTION_GAIN_TWENTYFIVE_PCT == 0.25

    def test_option_gain_already_winning(self):
        assert OPTION_GAIN_ALREADY_WINNING == 0.15

    def test_option_gain_winning_threshold(self):
        assert OPTION_GAIN_WINNING_THRESHOLD == 0.05


# =====================================================================
# Lifecycle / Carry / Misc
# =====================================================================

class TestLifecycleThresholds:
    def test_lifecycle_min_consumption_pct(self):
        assert LIFECYCLE_MIN_CONSUMPTION_PCT == 0.10

    def test_position_age_thesis_degradation_min(self):
        assert POSITION_AGE_THESIS_DEGRADATION_MIN == 2

    def test_days_in_trade_maturity(self):
        assert DAYS_IN_TRADE_MATURITY == 5

    def test_catalyst_window(self):
        assert CATALYST_WINDOW == 10

    def test_catalyst_window_extended(self):
        assert CATALYST_WINDOW_EXTENDED == 14

    def test_earnings_note_window(self):
        assert EARNINGS_NOTE_WINDOW == 30

    def test_standard_roll_dte(self):
        assert STANDARD_ROLL_DTE == 45

    def test_theta_material_daily_cost(self):
        assert THETA_MATERIAL_DAILY_COST == 25

    def test_theta_bleed_daily_pct(self):
        assert THETA_BLEED_DAILY_PCT == 3.0

    def test_conviction_deterioration_streak(self):
        assert CONVICTION_DETERIORATION_STREAK == 3

    def test_shares_cc_eligible(self):
        assert SHARES_CC_ELIGIBLE == 100

    def test_dividend_days_critical(self):
        assert DIVIDEND_DAYS_CRITICAL == 2

    def test_dividend_days_warning(self):
        assert DIVIDEND_DAYS_WARNING == 5

    def test_wheel_basis_discount(self):
        assert WHEEL_BASIS_DISCOUNT == 0.97

    def test_wheel_delta_util_max(self):
        assert WHEEL_DELTA_UTIL_MAX == 15.0

    def test_compression_resolving_down(self):
        assert COMPRESSION_RESOLVING_DOWN == -0.005

    def test_contract_multiplier(self):
        assert CONTRACT_MULTIPLIER == 100


# =====================================================================
# gate_result.py — Decision State Constants
# =====================================================================

class TestDecisionStateConstants:
    def test_state_actionable(self):
        assert STATE_ACTIONABLE == "ACTIONABLE"

    def test_state_neutral_confident(self):
        assert STATE_NEUTRAL_CONFIDENT == "NEUTRAL_CONFIDENT"

    def test_state_uncertain(self):
        assert STATE_UNCERTAIN == "UNCERTAIN"

    def test_state_blocked_governance(self):
        assert STATE_BLOCKED_GOVERNANCE == "BLOCKED_GOVERNANCE"

    def test_state_unresolved_identity(self):
        assert STATE_UNRESOLVED_IDENTITY == "UNRESOLVED_IDENTITY"

    def test_authority_levels(self):
        assert AUTHORITY_REQUIRED == "REQUIRED"
        assert AUTHORITY_CONTEXTUAL == "CONTEXTUAL"
        assert AUTHORITY_SUPPORTIVE == "SUPPORTIVE"
        assert AUTHORITY_NON_AUTHORITATIVE == "NON_AUTHORITATIVE"


# =====================================================================
# gate_result.py — fire_gate / skip_gate behavioral tests
# =====================================================================

class TestFireGate:
    def test_fire_gate_returns_true_and_updated_dict(self):
        result = {"Ticker": "AAPL"}
        fired, out = fire_gate(
            result,
            action="EXIT",
            urgency="CRITICAL",
            rationale="Test rationale",
            doctrine_source="McMillan Ch.3",
        )
        assert fired is True
        assert out is result  # same dict, mutated in place
        assert out["Action"] == "EXIT"
        assert out["Urgency"] == "CRITICAL"
        assert out["Rationale"] == "Test rationale"
        assert out["Doctrine_Source"] == "McMillan Ch.3"
        assert out["Decision_State"] == STATE_ACTIONABLE
        assert out["Required_Conditions_Met"] is True
        assert out["Ticker"] == "AAPL"  # original field preserved

    def test_fire_gate_custom_decision_state(self):
        result = {}
        fired, out = fire_gate(
            result,
            action="HOLD",
            urgency="LOW",
            rationale="Uncertain",
            doctrine_source="Passarelli",
            decision_state=STATE_UNCERTAIN,
        )
        assert fired is True
        assert out["Decision_State"] == STATE_UNCERTAIN

    def test_fire_gate_extra_fields(self):
        result = {}
        fired, out = fire_gate(
            result,
            action="ROLL",
            urgency="HIGH",
            rationale="Gamma drag",
            doctrine_source="Natenberg Ch.7",
            Gamma_Drag_Daily=15.5,
            Doctrine_State="BROKEN",
        )
        assert fired is True
        assert out["Gamma_Drag_Daily"] == 15.5
        assert out["Doctrine_State"] == "BROKEN"

    def test_skip_gate_returns_false_unchanged(self):
        result = {"Ticker": "TSLA", "Action": "HOLD"}
        fired, out = skip_gate(result)
        assert fired is False
        assert out is result
        assert out["Action"] == "HOLD"
        assert out["Ticker"] == "TSLA"


# =====================================================================
# helpers.py — safe_pnl_pct tests
# =====================================================================

class TestSafePnlPct:
    def test_total_gl_decimal_available(self):
        import pandas as pd
        from core.management.cycle3.doctrine.helpers import safe_pnl_pct
        row = pd.Series({"Total_GL_Decimal": -0.25, "PnL_Total": None, "Basis": None})
        assert safe_pnl_pct(row) == pytest.approx(-0.25)

    def test_fallback_to_pnl_total_over_basis(self):
        import pandas as pd
        from core.management.cycle3.doctrine.helpers import safe_pnl_pct
        row = pd.Series({"Total_GL_Decimal": None, "PnL_Total": -500.0, "Basis": 2000.0})
        assert safe_pnl_pct(row) == pytest.approx(-0.25)

    def test_returns_none_when_no_data(self):
        import pandas as pd
        from core.management.cycle3.doctrine.helpers import safe_pnl_pct
        row = pd.Series({"Total_GL_Decimal": None, "PnL_Total": None, "Basis": None})
        assert safe_pnl_pct(row) is None

    def test_returns_none_when_basis_zero(self):
        import pandas as pd
        from core.management.cycle3.doctrine.helpers import safe_pnl_pct
        row = pd.Series({"Total_GL_Decimal": None, "PnL_Total": -100.0, "Basis": 0.0})
        assert safe_pnl_pct(row) is None
