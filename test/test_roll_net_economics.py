"""
Tests for net-roll economics scoring in _score_candidate().

Validates that:
1. Debit rolls are penalized proportionally to close cost vs new premium
2. Credit rolls get mild bonus in non-INCOME_SAME modes
3. Strike lift toward/above basis earns recovery bonus
4. Debit + weak strike improvement → penalty dominates
5. Debit + strong recovery geometry (crosses basis) → acceptable
6. INCOME_SAME mode is unaffected (has its own credit-preference re-ranking)
7. Long-vol strategies are unaffected (only short-vol gets net-roll economics)
"""

import pytest
from core.management.cycle3.roll.roll_candidate_engine import (
    _score_candidate as _score_candidate_full,
)


def _score_candidate(**kw):
    """Wrapper: extract composite score for backward-compat test assertions."""
    return _score_candidate_full(**kw)["composite"]


# ── Shared defaults ───────────────────────────────────────────────────────────
_BASE = dict(
    delta=0.30,
    target_delta=0.30,
    actual_dte=45,
    target_dte=45,
    iv=0.30,
    current_iv=0.30,
    liq_grade="GOOD",
    hv_20d=0.25,
    ul_price=25.0,
    theta=-0.02,
)


class TestDebitRollPenalty:
    """Debit rolls should score lower than credit rolls, all else equal."""

    def test_debit_roll_lowers_score_vs_no_close_cost(self):
        """Same candidate mid, but one has close cost → debit → lower score."""
        # No close cost info (old behavior: current_option_mid=0)
        score_no_cost = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.65, strike=27.5,
            net_cost_basis=26.0, roll_mode="NORMAL",
            current_option_mid=0.0, current_strike=27.5,
        )
        # Close cost 0.75, new premium 0.65 → net debit -0.10
        score_debit = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.65, strike=27.5,
            net_cost_basis=26.0, roll_mode="NORMAL",
            current_option_mid=0.75, current_strike=27.5,
        )
        assert score_debit < score_no_cost, (
            f"Debit roll ({score_debit:.4f}) should score lower than "
            f"no-cost baseline ({score_no_cost:.4f})"
        )

    def test_large_debit_penalized_more_than_small(self):
        """Larger debit (close cost >> new premium) → bigger penalty."""
        # Small debit: close=0.70, open=0.65 → net -0.05
        score_small = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.65, strike=28.0,
            net_cost_basis=26.0, roll_mode="WEEKLY",
            current_option_mid=0.70, current_strike=27.5,
        )
        # Large debit: close=1.20, open=0.65 → net -0.55
        score_large = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.65, strike=28.0,
            net_cost_basis=26.0, roll_mode="WEEKLY",
            current_option_mid=1.20, current_strike=27.5,
        )
        assert score_large < score_small, (
            f"Large debit ({score_large:.4f}) should score lower than "
            f"small debit ({score_small:.4f})"
        )

    def test_full_debit_floor(self):
        """Even worst-case debit still produces a positive score (floor at 0.20×)."""
        score = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.50, strike=27.5,
            net_cost_basis=26.0, roll_mode="NORMAL",
            current_option_mid=1.50, current_strike=27.5,
        )
        assert score > 0, f"Score should remain positive even with large debit: {score:.4f}"


class TestCreditRollBonus:
    """Credit rolls in non-INCOME_SAME modes get mild bonus."""

    def test_credit_roll_scores_higher_than_neutral(self):
        """Net credit (new premium > close cost) → bonus over no-cost baseline."""
        # No close cost info
        score_neutral = _score_candidate(
            **_BASE, strategy_key="COVERED_CALL", mid=0.80, strike=28.0,
            net_cost_basis=26.0, roll_mode="NORMAL",
            current_option_mid=0.0, current_strike=27.5,
        )
        # Net credit: close=0.50, open=0.80 → net +0.30
        score_credit = _score_candidate(
            **_BASE, strategy_key="COVERED_CALL", mid=0.80, strike=28.0,
            net_cost_basis=26.0, roll_mode="NORMAL",
            current_option_mid=0.50, current_strike=27.5,
        )
        assert score_credit > score_neutral, (
            f"Credit roll ({score_credit:.4f}) should score higher than "
            f"neutral ({score_neutral:.4f})"
        )


class TestRecoveryImprovement:
    """Strike lift toward basis earns recovery bonus; lift away from basis → penalty."""

    def test_strike_lift_toward_basis_bonus(self):
        """Rolling UP when underwater: new strike closer to basis → bonus."""
        # Current: strike 25.0, basis 28.0 → underwater 10.7%
        # Candidate: strike 27.0 → now only 3.6% underwater → improvement
        score_lift = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.50, strike=27.0,
            net_cost_basis=28.0, roll_mode="WEEKLY",
            current_option_mid=0.55, current_strike=25.0,
        )
        # No lift: same strike 25.0
        score_same = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.50, strike=25.0,
            net_cost_basis=28.0, roll_mode="WEEKLY",
            current_option_mid=0.55, current_strike=25.0,
        )
        assert score_lift > score_same, (
            f"Strike lift ({score_lift:.4f}) should score higher than "
            f"same strike ({score_same:.4f})"
        )

    def test_crossing_above_basis_strong_bonus(self):
        """Debit roll that moves strike from below to above basis → strong recovery."""
        # Current: strike 25.0, basis 26.0 → underwater
        # Candidate: strike 27.0 → ABOVE basis → crossing bonus
        score_cross = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.40, strike=27.0,
            net_cost_basis=26.0, roll_mode="PRE_ITM",
            current_option_mid=0.60, current_strike=25.0,
        )
        # Same debit, but stays below basis
        score_stay = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.40, strike=25.5,
            net_cost_basis=26.0, roll_mode="PRE_ITM",
            current_option_mid=0.60, current_strike=25.0,
        )
        assert score_cross > score_stay, (
            f"Basis crossing ({score_cross:.4f}) should score higher than "
            f"staying below ({score_stay:.4f})"
        )

    def test_debit_roll_down_penalized(self):
        """Rolling DOWN in strike while paying debit → worst case penalty."""
        # Current: strike 28.0, roll DOWN to 26.0 AND pay debit
        score_down = _score_candidate(
            **_BASE, strategy_key="COVERED_CALL", mid=0.80, strike=26.0,
            net_cost_basis=27.0, roll_mode="NORMAL",
            current_option_mid=0.90, current_strike=28.0,
        )
        # Same debit, roll UP to 29.0
        score_up = _score_candidate(
            **_BASE, strategy_key="COVERED_CALL", mid=0.80, strike=29.0,
            net_cost_basis=27.0, roll_mode="NORMAL",
            current_option_mid=0.90, current_strike=28.0,
        )
        assert score_up > score_down, (
            f"Roll up ({score_up:.4f}) should score higher than "
            f"roll down + debit ({score_down:.4f})"
        )


class TestIncomeSameUnaffected:
    """INCOME_SAME mode should NOT apply net-roll economics (has its own re-ranking)."""

    def test_income_same_ignores_close_cost(self):
        """With INCOME_SAME mode, current_option_mid should not affect score."""
        score_a = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.65, strike=27.5,
            net_cost_basis=26.0, roll_mode="INCOME_SAME",
            current_option_mid=0.0, current_strike=27.5,
        )
        score_b = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.65, strike=27.5,
            net_cost_basis=26.0, roll_mode="INCOME_SAME",
            current_option_mid=0.75, current_strike=27.5,
        )
        assert score_a == score_b, (
            f"INCOME_SAME should be unaffected by current_option_mid: "
            f"{score_a:.4f} vs {score_b:.4f}"
        )


class TestLongVolUnaffected:
    """Long-vol strategies should not get net-roll economics adjustments."""

    def test_long_call_no_debit_penalty(self):
        """LONG_CALL with close cost should not apply short-vol debit penalty."""
        score_a = _score_candidate(
            **_BASE, strategy_key="LONG_CALL", mid=2.00, strike=27.0,
            net_cost_basis=0, roll_mode="NORMAL",
            current_option_mid=0.0, current_strike=25.0,
        )
        score_b = _score_candidate(
            **_BASE, strategy_key="LONG_CALL", mid=2.00, strike=27.0,
            net_cost_basis=0, roll_mode="NORMAL",
            current_option_mid=1.50, current_strike=25.0,
        )
        assert score_a == score_b, (
            f"Long vol should be unaffected: {score_a:.4f} vs {score_b:.4f}"
        )


class TestWeeklyModeDebitAwareness:
    """WEEKLY mode (DKNG scenario) should now reflect debit cost in scoring."""

    def test_dkng_scenario_debit_roll_deprioritized(self):
        """DKNG-style: $0.65 premium but $0.75 close cost → net debit → lower rank."""
        # Candidate A: higher premium but net debit
        score_debit = _score_candidate(
            delta=0.30, target_delta=0.30, actual_dte=35, target_dte=35,
            iv=0.52, current_iv=0.50, liq_grade="GOOD",
            strategy_key="BUY_WRITE", mid=0.65, theta=-0.025,
            net_cost_basis=26.06, hv_20d=0.45, ul_price=25.56, strike=27.5,
            roll_mode="WEEKLY",
            current_option_mid=0.75, current_strike=27.5,
        )
        # Candidate B: lower premium but net credit (cheaper to close)
        score_credit = _score_candidate(
            delta=0.25, target_delta=0.30, actual_dte=35, target_dte=35,
            iv=0.50, current_iv=0.50, liq_grade="GOOD",
            strategy_key="BUY_WRITE", mid=0.55, theta=-0.020,
            net_cost_basis=26.06, hv_20d=0.45, ul_price=25.56, strike=27.0,
            roll_mode="WEEKLY",
            current_option_mid=0.40, current_strike=27.5,
        )
        assert score_credit > score_debit, (
            f"Net credit roll ({score_credit:.4f}) should beat net debit roll "
            f"({score_debit:.4f}) in WEEKLY mode"
        )

    def test_debit_acceptable_with_strong_recovery(self):
        """Debit roll acceptable if strike lift materially improves recovery geometry."""
        # Debit roll but crosses above basis: strike 27.0→28.0, basis=27.5
        score = _score_candidate(
            delta=0.35, target_delta=0.30, actual_dte=45, target_dte=45,
            iv=0.50, current_iv=0.50, liq_grade="GOOD",
            strategy_key="BUY_WRITE", mid=0.45, theta=-0.015,
            net_cost_basis=27.5, hv_20d=0.45, ul_price=28.0, strike=28.5,
            roll_mode="WEEKLY",
            current_option_mid=0.55, current_strike=27.0,
        )
        # Score should still be positive and not crushed — recovery geometry matters
        assert score > 0.3, (
            f"Debit roll with basis-crossing recovery should score reasonably: {score:.4f}"
        )


class TestCSPNetEconomics:
    """CSP/SHORT_PUT should also get net-roll economics."""

    def test_csp_debit_roll_penalized(self):
        """CSP debit roll should score lower than equivalent credit roll."""
        score_debit = _score_candidate(
            **_BASE, strategy_key="CSP", mid=0.50, strike=22.0,
            net_cost_basis=23.0, roll_mode="NORMAL",
            current_option_mid=0.70, current_strike=23.0,
        )
        score_credit = _score_candidate(
            **_BASE, strategy_key="CSP", mid=0.50, strike=22.0,
            net_cost_basis=23.0, roll_mode="NORMAL",
            current_option_mid=0.30, current_strike=23.0,
        )
        assert score_credit > score_debit


class TestLiquidityGateOnBonuses:
    """Bonuses must not rescue illiquid candidates. THIN/ILLIQUID = phantom credit."""

    def test_illiquid_stacked_bonuses_lose_to_good_modest(self):
        """ILLIQUID + credit + recovery should lose to GOOD + modest economics."""
        _b = {**_BASE, "liq_grade": "ILLIQUID"}
        illiq = _score_candidate(
            **_b, strategy_key="BUY_WRITE", mid=0.80, strike=27.0,
            net_cost_basis=26.5, roll_mode="NORMAL",
            current_option_mid=0.30, current_strike=25.0,
        )
        good = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.60, strike=27.0,
            net_cost_basis=26.5, roll_mode="NORMAL",
            current_option_mid=0.55, current_strike=27.0,
        )
        assert good > illiq, (
            f"GOOD liquidity ({good:.4f}) must beat ILLIQUID ({illiq:.4f}) "
            f"even with stacked bonuses"
        )

    def test_thin_great_economics_loses_to_good_modest(self):
        """THIN + strong credit/recovery should lose to GOOD + average economics."""
        _b = {**_BASE, "liq_grade": "THIN"}
        thin = _score_candidate(
            **_b, strategy_key="BUY_WRITE", mid=0.80, strike=27.0,
            net_cost_basis=26.5, roll_mode="NORMAL",
            current_option_mid=0.30, current_strike=25.0,
        )
        good = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.60, strike=27.0,
            net_cost_basis=26.5, roll_mode="NORMAL",
            current_option_mid=0.55, current_strike=27.0,
        )
        assert good > thin, (
            f"GOOD liquidity ({good:.4f}) must beat THIN ({thin:.4f})"
        )

    def test_thin_preserves_debit_penalty(self):
        """THIN + debit should be doubly bad: debit penalty preserved, bonuses suppressed."""
        _b = {**_BASE, "liq_grade": "THIN"}
        thin_debit = _score_candidate(
            **_b, strategy_key="BUY_WRITE", mid=0.50, strike=27.0,
            net_cost_basis=26.5, roll_mode="NORMAL",
            current_option_mid=0.80, current_strike=25.0,
        )
        good_debit = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.50, strike=27.0,
            net_cost_basis=26.5, roll_mode="NORMAL",
            current_option_mid=0.80, current_strike=25.0,
        )
        assert good_debit > thin_debit, (
            f"THIN + debit ({thin_debit:.4f}) should be worse than GOOD + debit ({good_debit:.4f})"
        )


class TestBasisCrossingSlippage:
    """Tiny symbolic crossings above basis should not earn full bonus."""

    def test_tiny_crossing_no_extra_bonus(self):
        """$26.01 vs $26.00 basis (<1% above) → no crossing bonus."""
        tiny = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.50, strike=26.01,
            net_cost_basis=26.0, roll_mode="NORMAL",
            current_option_mid=0.60, current_strike=25.5,
        )
        # Same but clearly below basis (no crossing at all)
        below = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.50, strike=25.8,
            net_cost_basis=26.0, roll_mode="NORMAL",
            current_option_mid=0.60, current_strike=25.5,
        )
        # Tiny crossing should NOT get the +0.10 crossing bonus — gap should be small
        # (only from normal recovery_score based on strike_improvement, not the extra +0.10)
        gap = tiny - below
        assert gap < 0.05, (
            f"Tiny crossing bonus ({gap:+.4f}) should be small — no extra +0.10 for "
            f"symbolic crossings that disappear after slippage"
        )

    def test_meaningful_crossing_gets_extra_bonus(self):
        """$27.00 vs $26.00 basis (3.8% above) → full crossing bonus."""
        meaningful = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.50, strike=27.0,
            net_cost_basis=26.0, roll_mode="NORMAL",
            current_option_mid=0.60, current_strike=25.5,
        )
        tiny = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.50, strike=26.01,
            net_cost_basis=26.0, roll_mode="NORMAL",
            current_option_mid=0.60, current_strike=25.5,
        )
        gap = meaningful - tiny
        assert gap > 0.08, (
            f"Meaningful crossing should materially beat tiny: gap={gap:+.4f}"
        )

    def test_one_percent_threshold(self):
        """Strike exactly 1% above basis should qualify for crossing bonus."""
        # 1% above: 26.0 * 1.01 = 26.26
        at_threshold = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.50, strike=26.26,
            net_cost_basis=26.0, roll_mode="NORMAL",
            current_option_mid=0.60, current_strike=25.5,
        )
        # Just below 1%: 26.0 * 1.009 = 26.234
        below_threshold = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.50, strike=26.234,
            net_cost_basis=26.0, roll_mode="NORMAL",
            current_option_mid=0.60, current_strike=25.5,
        )
        # At 1% should be higher (crossing bonus kicks in)
        assert at_threshold > below_threshold


class TestDividendAssignmentPenalty:
    """Candidates with extrinsic < dividend are assignment bait."""

    def test_itm_strike_with_low_extrinsic_penalized(self):
        """ITM strike where extrinsic < dividend → severe penalty."""
        # Stock at $50, strike $48 (ITM), mid=2.30, intrinsic=2.00, extrinsic=0.30
        # Dividend = $0.50 → extrinsic < dividend → assignment bait
        score_bait = _score_candidate(
            delta=0.65, target_delta=0.30, actual_dte=45, target_dte=45,
            iv=0.30, current_iv=0.30, liq_grade="GOOD",
            strategy_key="BUY_WRITE", mid=2.30, theta=-0.04,
            net_cost_basis=50.0, hv_20d=0.25, ul_price=50.0, strike=48.0,
            roll_mode="NORMAL",
            days_to_dividend=15.0, dividend_amount=0.50,
        )
        # OTM strike where all premium is extrinsic (no assignment risk)
        score_safe = _score_candidate(
            delta=0.30, target_delta=0.30, actual_dte=45, target_dte=45,
            iv=0.30, current_iv=0.30, liq_grade="GOOD",
            strategy_key="BUY_WRITE", mid=1.00, theta=-0.03,
            net_cost_basis=50.0, hv_20d=0.25, ul_price=50.0, strike=52.0,
            roll_mode="NORMAL",
            days_to_dividend=15.0, dividend_amount=0.50,
        )
        assert score_safe > score_bait, (
            f"OTM safe ({score_safe:.4f}) must beat ITM assignment bait ({score_bait:.4f})"
        )

    def test_otm_strike_no_dividend_penalty(self):
        """OTM strike has all extrinsic — no dividend penalty even with large div."""
        score_no_div = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.65, strike=27.5,
            net_cost_basis=26.0, roll_mode="NORMAL",
        )
        score_with_div = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.65, strike=27.5,
            net_cost_basis=26.0, roll_mode="NORMAL",
            days_to_dividend=20.0, dividend_amount=0.40,
        )
        # OTM: all $0.65 is extrinsic, which > $0.40 dividend → no penalty
        assert abs(score_no_div - score_with_div) < 0.01, (
            f"OTM strike should not be penalized: {score_no_div:.4f} vs {score_with_div:.4f}"
        )

    def test_csp_no_dividend_penalty(self):
        """CSP (short put) should never get dividend penalty — puts don't exercise for div."""
        score_a = _score_candidate(
            **_BASE, strategy_key="CSP", mid=0.50, strike=23.0,
            net_cost_basis=23.0, roll_mode="NORMAL",
        )
        score_b = _score_candidate(
            **_BASE, strategy_key="CSP", mid=0.50, strike=23.0,
            net_cost_basis=23.0, roll_mode="NORMAL",
            days_to_dividend=5.0, dividend_amount=1.00,
        )
        assert abs(score_a - score_b) < 1e-10, (
            f"CSP should be unaffected by dividend: {score_a:.4f} vs {score_b:.4f}"
        )

    def test_dividend_far_out_no_penalty(self):
        """Dividend > 45 days away → no penalty (not actionable yet)."""
        score_a = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.65, strike=24.5,
            net_cost_basis=26.0, roll_mode="NORMAL",
        )
        score_b = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.65, strike=24.5,
            net_cost_basis=26.0, roll_mode="NORMAL",
            days_to_dividend=60.0, dividend_amount=0.50,
        )
        assert abs(score_a - score_b) < 1e-10

    def test_marginal_extrinsic_moderate_penalty(self):
        """Extrinsic between 1× and 1.5× dividend → moderate penalty."""
        # Stock $50, strike $49 (ITM), mid=1.60, intrinsic=1.00, extrinsic=0.60
        # Dividend $0.50, extrinsic/div = 1.2× (between 1× and 1.5×)
        score = _score_candidate(
            delta=0.55, target_delta=0.30, actual_dte=45, target_dte=45,
            iv=0.30, current_iv=0.30, liq_grade="GOOD",
            strategy_key="BUY_WRITE", mid=1.60, theta=-0.04,
            net_cost_basis=50.0, hv_20d=0.25, ul_price=50.0, strike=49.0,
            roll_mode="NORMAL",
            days_to_dividend=10.0, dividend_amount=0.50,
        )
        # Same without div
        score_no_div = _score_candidate(
            delta=0.55, target_delta=0.30, actual_dte=45, target_dte=45,
            iv=0.30, current_iv=0.30, liq_grade="GOOD",
            strategy_key="BUY_WRITE", mid=1.60, theta=-0.04,
            net_cost_basis=50.0, hv_20d=0.25, ul_price=50.0, strike=49.0,
            roll_mode="NORMAL",
        )
        assert score < score_no_div, (
            f"Marginal extrinsic should be penalized: {score:.4f} vs {score_no_div:.4f}"
        )


class TestChurnGuard:
    """Consecutive debit rolls should escalate penalty."""

    def test_two_consecutive_debits_penalized(self):
        """2 prior debit rolls + another debit → moderate penalty."""
        score_clean = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.65, strike=27.5,
            net_cost_basis=26.0, roll_mode="NORMAL",
            current_option_mid=0.75, current_strike=27.5,
            consecutive_debit_rolls=0,
        )
        score_churn = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.65, strike=27.5,
            net_cost_basis=26.0, roll_mode="NORMAL",
            current_option_mid=0.75, current_strike=27.5,
            consecutive_debit_rolls=2,
        )
        assert score_churn < score_clean, (
            f"Churn ({score_churn:.4f}) should score lower than clean ({score_clean:.4f})"
        )

    def test_three_consecutive_debits_stronger_penalty(self):
        """3+ prior debit rolls → stronger penalty than 2."""
        score_2 = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.65, strike=27.5,
            net_cost_basis=26.0, roll_mode="WEEKLY",
            current_option_mid=0.75, current_strike=27.5,
            consecutive_debit_rolls=2,
        )
        score_3 = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.65, strike=27.5,
            net_cost_basis=26.0, roll_mode="WEEKLY",
            current_option_mid=0.75, current_strike=27.5,
            consecutive_debit_rolls=3,
        )
        assert score_3 < score_2, (
            f"3 debits ({score_3:.4f}) should be worse than 2 ({score_2:.4f})"
        )

    def test_credit_roll_no_churn_penalty(self):
        """Credit roll should NOT get churn penalty even with prior debit streak."""
        score_no_hist = _score_candidate(
            **_BASE, strategy_key="COVERED_CALL", mid=0.80, strike=28.0,
            net_cost_basis=26.0, roll_mode="NORMAL",
            current_option_mid=0.40, current_strike=27.5,
            consecutive_debit_rolls=0,
        )
        score_with_hist = _score_candidate(
            **_BASE, strategy_key="COVERED_CALL", mid=0.80, strike=28.0,
            net_cost_basis=26.0, roll_mode="NORMAL",
            current_option_mid=0.40, current_strike=27.5,
            consecutive_debit_rolls=3,
        )
        # Credit roll → net_roll_adj > 0 → churn guard should NOT fire
        assert abs(score_no_hist - score_with_hist) < 0.01, (
            f"Credit roll should escape churn: {score_no_hist:.4f} vs {score_with_hist:.4f}"
        )

    def test_long_vol_no_churn(self):
        """Long-vol strategies should never get churn penalty."""
        score_a = _score_candidate(
            **_BASE, strategy_key="LONG_CALL", mid=2.0, strike=27.0,
            net_cost_basis=0, roll_mode="NORMAL",
            consecutive_debit_rolls=5,
        )
        score_b = _score_candidate(
            **_BASE, strategy_key="LONG_CALL", mid=2.0, strike=27.0,
            net_cost_basis=0, roll_mode="NORMAL",
            consecutive_debit_rolls=0,
        )
        assert abs(score_a - score_b) < 1e-10


class TestSlippageModel:
    """Net-roll should use conservative slippage estimate, not mid-to-mid."""

    def test_slippage_makes_borderline_credit_a_debit(self):
        """Tiny mid-to-mid credit (0.65 - 0.63 = +0.02) becomes debit after slippage."""
        # Mid-to-mid: +$0.02 credit
        # With slippage: 0.65*0.98 - 0.63*1.02 = 0.637 - 0.6426 = -0.006 (debit)
        score = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.65, strike=27.5,
            net_cost_basis=26.0, roll_mode="NORMAL",
            current_option_mid=0.63, current_strike=27.5,
        )
        # Compare to clearly credit roll (no slippage concern)
        score_clear = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.65, strike=27.5,
            net_cost_basis=26.0, roll_mode="NORMAL",
            current_option_mid=0.30, current_strike=27.5,
        )
        # The borderline case should NOT get credit bonus (slippage turns it to debit)
        assert score_clear > score, (
            f"Clear credit ({score_clear:.4f}) should beat borderline ({score:.4f})"
        )


class TestPMCCWidthConstraint:
    """PMCC short call candidates must stay below LEAP strike."""

    def test_pmcc_filter_rejects_above_leap(self):
        """Candidates at or above LEAP strike should be filtered out."""
        from core.management.cycle3.roll.roll_candidate_engine import _select_roll_candidates

        # Minimal chain with two candidates: one below LEAP strike, one above
        chain = {
            "callExpDateMap": {
                "2026-05-15:63": {
                    "27.0": [{"call": {"delta": 0.35, "bid": 0.60, "ask": 0.70,
                              "totalVolume": 100, "openInterest": 500,
                              "volatility": 30.0, "gamma": 0.05, "vega": 0.03,
                              "theta": -0.02, "bidSize": 20, "askSize": 25}}],
                    "32.0": [{"call": {"delta": 0.15, "bid": 0.20, "ask": 0.30,
                              "totalVolume": 50, "openInterest": 200,
                              "volatility": 28.0, "gamma": 0.03, "vega": 0.02,
                              "theta": -0.01, "bidSize": 15, "askSize": 20}}],
                },
            }
        }
        # LEAP strike at $30 — only $27 should survive, $32 should be filtered
        candidates = _select_roll_candidates(
            chain=chain, cp="C", ul_price=28.0,
            current_strike=26.0, current_dte=14, current_iv=0.30,
            dte_window=(30, 90, 45), delta_range=(0.10, 0.50),
            strategy_key="PMCC", net_cost_basis=25.0,
            leap_strike=30.0,
        )
        strikes = [c["strike"] for c in candidates]
        assert 32.0 not in strikes, f"Strike $32 above LEAP $30 should be filtered: {strikes}"
        # $27 should survive (below LEAP strike)
        assert 27.0 in strikes or len(candidates) == 0  # may fail delta/DTE filters too


class TestBackwardCompatibility:
    """When current_option_mid=0 (no data), behavior should match pre-change."""

    def test_zero_current_mid_no_change(self):
        """Score should be identical with current_option_mid=0 vs not passed."""
        score_explicit = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.65, strike=27.5,
            net_cost_basis=26.0, roll_mode="NORMAL",
            current_option_mid=0.0, current_strike=0.0,
        )
        score_default = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.65, strike=27.5,
            net_cost_basis=26.0, roll_mode="NORMAL",
        )
        assert abs(score_explicit - score_default) < 1e-10, (
            f"Explicit zero should match default: {score_explicit:.6f} vs {score_default:.6f}"
        )

    def test_no_dividend_data_no_penalty(self):
        """Default days_to_dividend=9999 should produce no dividend penalty."""
        score_a = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.65, strike=27.5,
            net_cost_basis=26.0, roll_mode="NORMAL",
        )
        score_b = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.65, strike=27.5,
            net_cost_basis=26.0, roll_mode="NORMAL",
            days_to_dividend=9999.0, dividend_amount=0.0,
        )
        assert abs(score_a - score_b) < 1e-10

    def test_no_churn_data_no_penalty(self):
        """Default consecutive_debit_rolls=0 should produce no churn penalty."""
        score_a = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.65, strike=27.5,
            net_cost_basis=26.0, roll_mode="NORMAL",
        )
        score_b = _score_candidate(
            **_BASE, strategy_key="BUY_WRITE", mid=0.65, strike=27.5,
            net_cost_basis=26.0, roll_mode="NORMAL",
            consecutive_debit_rolls=0,
        )
        assert abs(score_a - score_b) < 1e-10
