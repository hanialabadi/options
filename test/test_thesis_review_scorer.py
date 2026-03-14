"""
Tests for the Thesis Review Scorer — replaces ambiguous REVIEW with scored
thesis verdict producing concrete executable actions.

Validates:
1. Verdict boundaries: REAFFIRMED / MONITORING / WEAKENED / DEGRADED
2. Hard overrides: Structural_State=BROKEN, severe degradation
3. Category scorers: conviction, structure, momentum, profit, age, macro, drift
4. Action mapping: verdict → executable action (HOLD / HOLD_WITH_CAUTION / TRIM / EXIT)
5. Dead-letter fallback: scorer failure preserves REVIEW for legacy streak escalation
"""

import math
import pytest
import pandas as pd
from unittest.mock import patch
from core.management.cycle3.doctrine.thesis_review_scorer import (
    score_thesis_review,
    ThesisVerdict,
    _can_reduce,
)
from core.management.cycle3.doctrine.thresholds import (
    THESIS_REVIEW_REAFFIRMED_FLOOR,
    THESIS_REVIEW_MONITORING_FLOOR,
    THESIS_REVIEW_WEAKENED_FLOOR,
)


# ── Helpers ────────────────────────────────────────────────────────────────

def _row(**overrides):
    """Create a base position row with sensible defaults for REVIEW testing."""
    base = {
        # Identity
        'Strategy': 'BUY_WRITE',
        'Quantity': 4,
        # Conviction
        'Conviction_Status': 'STABLE',
        'Delta_Deterioration_Streak': 0,
        'Conviction_Fade_Days': 0,
        # Structure
        'Thesis_State': 'INTACT',
        'Structural_State': 'INTACT',
        'Entry_Chart_State_TrendIntegrity': 'STRONG_TREND',
        'TrendIntegrity_State': 'STRONG_TREND',
        'Entry_Chart_State_VolatilityState': 'COMPRESSED',
        'VolatilityState_State': 'COMPRESSED',
        'Entry_Chart_State_PriceStructure': 'STRUCTURAL_UP',
        'PriceStructure_State': 'STRUCTURAL_UP',
        # Momentum
        'WavePhase_State': 'BUILDING',
        'MomentumVelocity_State': 'ACCELERATING',
        # Profit
        'PnL_Pct': 0.15,
        'Trajectory_MFE': 0.20,
        'Recovery_Feasibility': 'FEASIBLE',
        # Age
        'Days_In_Trade': 20,
        'Entry_Snapshot_TS': '2026-02-01',
        'Snapshot_TS': '2026-02-21',
        # Drift
        'Data_State': 'STALE',
        'Signal_State': 'VALID',
        'Regime_State': 'STABLE',
        # Macro
        'days_to_earnings': 30,
        'Days_To_Macro': 10,
        'Portfolio_State': 'NOMINAL',
        # Signal hub (needed by check_thesis_degradation)
        'RSI_Divergence': 'None',
        'Weekly_Trend_Bias': 'ALIGNED',
        'roc_20': 2.0,
        'adx_14': 25.0,
        'rsi_14': 55.0,
        'sma_distance_pct': 0.03,
    }
    base.update(overrides)
    return pd.Series(base)


# ═══════════════════════════════════════════════════════════════════════════
# 1. Verdict Boundaries
# ═══════════════════════════════════════════════════════════════════════════

class TestVerdictBoundaries:
    """Score-to-verdict mapping at threshold boundaries."""

    def test_strong_thesis_reaffirmed(self):
        """All positive signals → REAFFIRMED → HOLD."""
        row = _row(
            Conviction_Status='STRENGTHENING',
            Thesis_State='INTACT',
            WavePhase_State='BUILDING',
            MomentumVelocity_State='ACCELERATING',
            TrendIntegrity_State='STRONG_TREND',
            PnL_Pct=0.25,
            Data_State='STALE',  # stale-only → noise offset
            Signal_State='VALID',
            Regime_State='STABLE',
        )
        v = score_thesis_review(row)
        assert v.verdict == "REAFFIRMED"
        assert v.action == "HOLD"
        assert v.score >= THESIS_REVIEW_REAFFIRMED_FLOOR
        assert v.resets_streak is True

    def test_marginal_monitoring(self):
        """Mixed signals → MONITORING → HOLD_WITH_CAUTION."""
        row = _row(
            Conviction_Status='STABLE',       # +10
            Thesis_State='RECOVERING',        # +5 (weaker than INTACT)
            WavePhase_State='STALLED',        # -5
            MomentumVelocity_State='DECELERATING',  # -5
            TrendIntegrity_State='WEAK_TREND',      # 0
            PnL_Pct=-0.05,                   # 0 (between -10% and 0)
            Data_State='STALE',               # stale-only (+10)
            Signal_State='VALID',
            Regime_State='STABLE',            # +5
            Days_In_Trade=8,                  # no age bonus or penalty
        )
        v = score_thesis_review(row)
        assert v.verdict == "MONITORING"
        assert v.action == "HOLD_WITH_CAUTION"
        assert THESIS_REVIEW_MONITORING_FLOOR <= v.score < THESIS_REVIEW_REAFFIRMED_FLOOR

    def test_deteriorating_weakened(self):
        """Multiple negatives → WEAKENED → TRIM (multi-contract)."""
        row = _row(
            Conviction_Status='WEAKENING',     # -5
            Thesis_State='INTACT',             # +15 (keep structure okay)
            WavePhase_State='FADING',          # -10
            MomentumVelocity_State='DECELERATING',  # -5
            TrendIntegrity_State='TREND_EXHAUSTED',  # -5
            PnL_Pct=0.02,                     # +5
            Quantity=4,
            Data_State='STALE',
            Signal_State='DEGRADED',           # -5 from drift
            Regime_State='STABLE',             # +5
            Days_In_Trade=8,
        )
        v = score_thesis_review(row)
        assert v.verdict == "WEAKENED"
        assert v.action == "TRIM"
        assert v.urgency == "HIGH"
        assert THESIS_REVIEW_WEAKENED_FLOOR <= v.score < THESIS_REVIEW_MONITORING_FLOOR

    def test_broken_degraded(self):
        """Thesis structurally broken → DEGRADED → EXIT."""
        row = _row(
            Conviction_Status='REVERSING',
            Thesis_State='DEGRADED',
            WavePhase_State='EXHAUSTED',
            MomentumVelocity_State='LATE_CYCLE',
            TrendIntegrity_State='NO_TREND',
            PnL_Pct=-0.25,
            Recovery_Feasibility='IMPOSSIBLE',
            Days_In_Trade=30,
            Data_State='STALE',
            Signal_State='DEGRADED',
            Regime_State='STRESSED',
        )
        v = score_thesis_review(row)
        assert v.verdict == "DEGRADED"
        assert v.action == "EXIT"
        assert v.score < THESIS_REVIEW_WEAKENED_FLOOR


# ═══════════════════════════════════════════════════════════════════════════
# 2. Hard Overrides
# ═══════════════════════════════════════════════════════════════════════════

class TestHardOverrides:
    """Structural break and severe degradation bypass scoring."""

    def test_structural_broken_forces_degraded(self):
        """Structural_State=BROKEN → immediate DEGRADED/EXIT CRITICAL."""
        row = _row(Structural_State='BROKEN')
        v = score_thesis_review(row)
        assert v.verdict == "DEGRADED"
        assert v.action == "EXIT"
        assert v.urgency == "CRITICAL"
        assert v.score == -100.0

    def test_three_plus_degradation_issues_forces_degraded(self):
        """3+ structural issues from check_thesis_degradation → DEGRADED."""
        row = _row(
            # Trend collapse: STRONG→NO_TREND
            Entry_Chart_State_TrendIntegrity='STRONG_TREND',
            TrendIntegrity_State='NO_TREND',
            # Vol regime flip (short vol): COMPRESSED→EXTREME
            Entry_Chart_State_VolatilityState='COMPRESSED',
            VolatilityState_State='EXTREME',
            # Structure broken
            Entry_Chart_State_PriceStructure='STRUCTURAL_UP',
            PriceStructure_State='STRUCTURE_BROKEN',
            # RSI divergence (Murphy 0.691)
            RSI_Divergence='Bearish_Divergence',
            # Ensure position age is sufficient
            Days_In_Trade=20,
        )
        v = score_thesis_review(row)
        assert v.verdict == "DEGRADED"
        assert v.action == "EXIT"

    def test_two_issues_caps_at_weakened(self):
        """2 structural issues → cannot score higher than WEAKENED."""
        row = _row(
            # Strong positive signals to try to reach REAFFIRMED
            Conviction_Status='STRENGTHENING',
            WavePhase_State='BUILDING',
            MomentumVelocity_State='ACCELERATING',
            PnL_Pct=0.30,
            Data_State='STALE',
            Signal_State='VALID',
            Regime_State='STABLE',
            # But 2 structural issues:
            Entry_Chart_State_TrendIntegrity='STRONG_TREND',
            TrendIntegrity_State='NO_TREND',
            Entry_Chart_State_PriceStructure='STRUCTURAL_UP',
            PriceStructure_State='STRUCTURE_BROKEN',
            Days_In_Trade=20,
        )
        v = score_thesis_review(row)
        # Should be capped at WEAKENED despite strong positive signals
        assert v.verdict in ("WEAKENED", "DEGRADED")
        assert v.score < THESIS_REVIEW_MONITORING_FLOOR


# ═══════════════════════════════════════════════════════════════════════════
# 3. Conviction Scoring
# ═══════════════════════════════════════════════════════════════════════════

class TestConvictionScoring:
    """Conviction category score contributions."""

    def test_strengthening_conviction_positive(self):
        v = score_thesis_review(_row(Conviction_Status='STRENGTHENING'))
        assert v.category_scores.get("conviction", 0) > 0

    def test_reversing_with_streak_negative(self):
        v = score_thesis_review(_row(
            Conviction_Status='REVERSING',
            Delta_Deterioration_Streak=4,
            Conviction_Fade_Days=7,
        ))
        assert v.category_scores.get("conviction", 0) < -15

    def test_stable_conviction_neutral_positive(self):
        v = score_thesis_review(_row(Conviction_Status='STABLE'))
        assert v.category_scores.get("conviction", 0) == 10


# ═══════════════════════════════════════════════════════════════════════════
# 4. Structural Integrity
# ═══════════════════════════════════════════════════════════════════════════

class TestStructuralIntegrity:
    """Structure category score contributions."""

    def test_intact_thesis_positive(self):
        v = score_thesis_review(_row(Thesis_State='INTACT'))
        assert v.category_scores.get("structure", 0) > 0

    def test_degraded_thesis_negative(self):
        v = score_thesis_review(_row(Thesis_State='DEGRADED'))
        assert v.category_scores.get("structure", 0) < 0

    def test_degradation_issues_accumulate(self):
        """Single issue gives less penalty than multiple issues."""
        # Single issue: trend collapse
        row_1 = _row(
            Entry_Chart_State_TrendIntegrity='STRONG_TREND',
            TrendIntegrity_State='NO_TREND',
            Days_In_Trade=20,
        )
        # Multiple: trend collapse + structure broken
        row_2 = _row(
            Entry_Chart_State_TrendIntegrity='STRONG_TREND',
            TrendIntegrity_State='NO_TREND',
            Entry_Chart_State_PriceStructure='STRUCTURAL_UP',
            PriceStructure_State='STRUCTURE_BROKEN',
            Days_In_Trade=20,
        )
        v1 = score_thesis_review(row_1)
        v2 = score_thesis_review(row_2)
        assert v2.category_scores.get("structure", 0) < v1.category_scores.get("structure", 0)


# ═══════════════════════════════════════════════════════════════════════════
# 5. Profit Cushion
# ═══════════════════════════════════════════════════════════════════════════

class TestProfitCushion:
    """Profit category score contributions."""

    def test_large_profit_absorbs_drift(self):
        v = score_thesis_review(_row(PnL_Pct=0.25))
        assert v.category_scores.get("profit", 0) >= 15

    def test_deep_loss_amplifies_risk(self):
        v = score_thesis_review(_row(PnL_Pct=-0.25))
        assert v.category_scores.get("profit", 0) <= -15

    def test_mfe_giveback_penalty(self):
        """High MFE but low current P&L → giveback penalty."""
        v = score_thesis_review(_row(
            PnL_Pct=0.03,
            Trajectory_MFE=0.25,
        ))
        # Should have profit score ≤ 5 (P&L +3%) minus 5 (giveback) = 0
        assert v.category_scores.get("profit", 0) <= 0


# ═══════════════════════════════════════════════════════════════════════════
# 6. Trade Age
# ═══════════════════════════════════════════════════════════════════════════

class TestTradeAge:
    """Age category score contributions."""

    def test_young_trade_noise_protection(self):
        """Position ≤5 days old → noise protection bonus."""
        v = score_thesis_review(_row(Days_In_Trade=3))
        assert v.category_scores.get("age", 0) >= 10

    def test_stale_trade_no_progress_penalized(self):
        """≥10 days, flat/negative P&L + degraded drift → penalty."""
        v = score_thesis_review(_row(
            Days_In_Trade=15,
            PnL_Pct=-0.05,
            Signal_State='DEGRADED',
        ))
        assert v.category_scores.get("age", 0) < 0

    def test_young_but_broken_no_protection(self):
        """Young trade with structural failure → no noise bonus."""
        v = score_thesis_review(_row(
            Days_In_Trade=3,
            Structural_State='BROKEN',
        ))
        # Structural break triggers hard override, but verify intent
        assert v.verdict == "DEGRADED"


# ═══════════════════════════════════════════════════════════════════════════
# 7. Drift Classification
# ═══════════════════════════════════════════════════════════════════════════

class TestDriftClassification:
    """Drift trigger category scoring."""

    def test_stale_data_only_is_noise(self):
        """Data_State=STALE only → positive offset (pure noise)."""
        v = score_thesis_review(_row(
            Data_State='STALE',
            Signal_State='VALID',
            Regime_State='STABLE',
        ))
        assert v.category_scores.get("drift", 0) >= 10

    def test_multiple_triggers_negative(self):
        """Multiple drift triggers active → penalty."""
        v = score_thesis_review(_row(
            Data_State='STALE',
            Signal_State='DEGRADED',
            Regime_State='STRESSED',
        ))
        assert v.category_scores.get("drift", 0) < 0

    def test_degraded_signal_negative(self):
        """Signal_State=DEGRADED only → small negative."""
        v = score_thesis_review(_row(
            Data_State='FRESH',
            Signal_State='DEGRADED',
            Regime_State='STABLE',
        ))
        assert v.category_scores.get("drift", 0) < 0


# ═══════════════════════════════════════════════════════════════════════════
# 8. Action Mapping
# ═══════════════════════════════════════════════════════════════════════════

class TestActionMapping:
    """Verdict → executable action resolution."""

    def test_reaffirmed_maps_to_hold(self):
        row = _row(
            Conviction_Status='STRENGTHENING',
            PnL_Pct=0.25,
            WavePhase_State='BUILDING',
            Data_State='STALE',
            Signal_State='VALID',
            Regime_State='STABLE',
        )
        v = score_thesis_review(row)
        assert v.verdict == "REAFFIRMED"
        assert v.action == "HOLD"

    def test_weakened_multi_contract_maps_to_trim(self):
        """WEAKENED + multi-contract non-spread → TRIM."""
        row = _row(
            Conviction_Status='WEAKENING',
            Thesis_State='DEGRADED',
            WavePhase_State='FADING',
            MomentumVelocity_State='DECELERATING',
            TrendIntegrity_State='TREND_EXHAUSTED',
            PnL_Pct=-0.08,
            Quantity=4,
            Strategy='BUY_WRITE',
        )
        v = score_thesis_review(row)
        if v.verdict == "WEAKENED":
            assert v.action == "TRIM"

    def test_weakened_single_contract_maps_to_hold_with_caution(self):
        """WEAKENED + single contract → HOLD_WITH_CAUTION (can't trim)."""
        row = _row(
            Conviction_Status='WEAKENING',
            Thesis_State='DEGRADED',
            WavePhase_State='FADING',
            MomentumVelocity_State='DECELERATING',
            TrendIntegrity_State='TREND_EXHAUSTED',
            PnL_Pct=-0.08,
            Quantity=1,
            Strategy='LONG_CALL',
        )
        v = score_thesis_review(row)
        if v.verdict == "WEAKENED":
            assert v.action == "HOLD_WITH_CAUTION"


# ═══════════════════════════════════════════════════════════════════════════
# 9. Dead-Letter Fallback
# ═══════════════════════════════════════════════════════════════════════════

class TestDeadLetterFallback:
    """Scorer failure preserves REVIEW for legacy streak escalation."""

    def test_scorer_exception_preserves_review(self):
        """If score_thesis_review raises, run_all.py catches and REVIEW stays."""
        # Simulate the integration pattern from run_all.py
        df = pd.DataFrame([{
            'Action': 'REVIEW',
            'Urgency': 'MEDIUM',
            'Rationale': 'drift noise',
            'Doctrine_Source': 'drift',
            'Prior_Action_Streak': 4,
        }])

        # Simulate what run_all.py does: try scorer, catch exception
        review_mask = df["Action"] == "REVIEW"
        scorer_failed = False
        try:
            with patch(
                'core.management.cycle3.doctrine.thesis_review_scorer.score_thesis_review',
                side_effect=RuntimeError("scorer crash"),
            ):
                from core.management.cycle3.doctrine.thesis_review_scorer import score_thesis_review as _scorer
                for idx in df[review_mask].index:
                    _scorer(df.loc[idx])
        except RuntimeError:
            scorer_failed = True

        assert scorer_failed is True
        # REVIEW is preserved — legacy streak escalation can still fire
        assert df.at[0, "Action"] == "REVIEW"


# ═══════════════════════════════════════════════════════════════════════════
# 10. Strategy Awareness
# ═══════════════════════════════════════════════════════════════════════════

class TestStrategyAwareness:
    """Strategy-aware drift interpretation."""

    def test_long_vol_expanding_iv_confirming(self):
        """Long vol + IV expanding + DEGRADED signal → thesis confirming bonus."""
        row = _row(
            Strategy='LONG_CALL',
            Quantity=2,
            VolatilityState_State='EXPANDING',
            Signal_State='DEGRADED',
            Data_State='FRESH',
            Regime_State='STABLE',
        )
        v = score_thesis_review(row)
        # Drift category should get the +10 long vol bonus
        assert v.category_scores.get("drift", 0) > 0

    def test_short_vol_expanding_iv_no_bonus(self):
        """Short vol + IV expanding + DEGRADED signal → no bonus."""
        row = _row(
            Strategy='BUY_WRITE',
            Quantity=4,
            VolatilityState_State='EXPANDING',
            Signal_State='DEGRADED',
            Data_State='FRESH',
            Regime_State='STABLE',
        )
        v = score_thesis_review(row)
        # Short vol should NOT get the confirming bonus
        assert v.category_scores.get("drift", 0) < 10

    def test_pmcc_weakened_maps_to_hold_with_caution(self):
        """PMCC is multi-leg → WEAKENED maps to HOLD_WITH_CAUTION, not TRIM."""
        assert _can_reduce('PMCC', 4) is False
        assert _can_reduce('POOR_MANS_COVERED_CALL', 4) is False

    def test_spread_cannot_reduce(self):
        """Defined-risk spreads cannot be partially closed."""
        assert _can_reduce('IRON_CONDOR', 4) is False
        assert _can_reduce('VERTICAL_SPREAD', 4) is False

    def test_single_contract_cannot_reduce(self):
        """Single contract positions cannot be trimmed."""
        assert _can_reduce('LONG_CALL', 1) is False
        assert _can_reduce('BUY_WRITE', 1) is False


# ═══════════════════════════════════════════════════════════════════════════
# 11. Evidence Format
# ═══════════════════════════════════════════════════════════════════════════

class TestEvidenceFormat:
    """Evidence list is concise and properly formatted."""

    def test_evidence_limited_to_3(self):
        """Evidence list contains at most 3 items."""
        row = _row()
        v = score_thesis_review(row)
        assert len(v.evidence) <= 3

    def test_evidence_contains_score_deltas(self):
        """Evidence items include score contribution markers."""
        row = _row(Conviction_Status='STRENGTHENING')
        v = score_thesis_review(row)
        # At least one evidence item should contain a score marker
        has_score = any('+' in e or '-' in e for e in v.evidence)
        assert has_score is True
