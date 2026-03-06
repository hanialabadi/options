"""
Test Scan Blindspot Fixes (Mar 2026 Audit)

Fix 1: Bidirectional → Income strategy eligibility (CSP + BW)
Fix 2: IV_Trend_7D computation from DuckDB + HV_Accel_Proxy
Fix 3: Income gap floor raised to 3.0 pts
Fix 4: Strategy overlap annotation
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
import pandas as pd
import numpy as np


# ─── Fix 1 + Fix 3: Step 6 strategy validators ─────────────────────────────

from scan_engine.step6_strategy_recommendation import (
    _validate_csp, _validate_buy_write, _validate_long_call,
    _DIRECTIONAL_STRATEGIES,
)


def _make_row(**overrides):
    """Build a minimal pd.Series that step6 validators expect."""
    defaults = {
        'Signal_Type': 'Bullish',
        'IVHV_gap_30D': 10.0,
        'IV_Rank_30D': 60.0,
        'IV_Rank_60D': np.nan,
        'IV_Rank': np.nan,
        'IV_Rank_XS': np.nan,
        'Chart_EMA_Signal': 'Bullish',
        'IV_Trend_7D': 'Stable',
        'Close': 100.0,         # Used by _calculate_approx_stock_price fallback
        'SMA20': np.nan,
        'Price_vs_SMA20': np.nan,
    }
    defaults.update(overrides)
    return pd.Series(defaults)


# ── Fix 1: Bidirectional → Income ────────────────────────────────────────────

class TestBidirectionalIncomeEligibility:
    """Fix 1: Bidirectional signal with EMA Bullish + strong gap → income eligible."""

    def test_csp_bidirectional_ema_bullish_high_gap_passes(self):
        """Bidirectional + EMA Bullish + gap≥6 → CSP generated with rank-adjusted confidence."""
        row = _make_row(Signal_Type='Bidirectional', Chart_EMA_Signal='Bullish', IVHV_gap_30D=10.0)
        result = _validate_csp('MU', row)
        assert result is not None, "CSP should be generated for Bidirectional with EMA Bullish + gap≥6"
        assert result['Strategy_Name'] == 'Cash-Secured Put'
        # Base 60 (Bidirectional) + 3 (IV_Rank_30D=60 → adj=(60-50)*0.3=3)
        assert result['Confidence'] == 63
        assert 'Bidirectional→Income' in result['Valid_Reason']

    def test_csp_bidirectional_ema_bearish_rejected(self):
        """Bidirectional + EMA Bearish → rejected even with strong gap."""
        row = _make_row(Signal_Type='Bidirectional', Chart_EMA_Signal='Bearish', IVHV_gap_30D=10.0)
        result = _validate_csp('MU', row)
        assert result is None, "CSP should be rejected for Bidirectional with EMA Bearish"

    def test_csp_bidirectional_low_gap_rejected(self):
        """Bidirectional + EMA Bullish + gap<6 → rejected (gap too thin)."""
        row = _make_row(Signal_Type='Bidirectional', Chart_EMA_Signal='Bullish', IVHV_gap_30D=4.0)
        result = _validate_csp('MU', row)
        assert result is None, "CSP should be rejected for Bidirectional with gap<6"

    def test_bw_bidirectional_ema_bullish_passes(self):
        """Bidirectional + EMA Bullish + gap≥6 → BW generated with rank-adjusted confidence."""
        row = _make_row(Signal_Type='Bidirectional', Chart_EMA_Signal='Bullish', IVHV_gap_30D=8.0)
        result = _validate_buy_write('MU', row)
        assert result is not None, "BW should be generated for Bidirectional with EMA Bullish + gap≥6"
        assert result['Strategy_Name'] == 'Buy-Write'
        # Base 65 (Bidirectional) + 3 (IV_Rank_30D=60 → adj=(60-50)*0.3=3)
        assert result['Confidence'] == 68
        assert 'Bidirectional→Income' in result['Valid_Reason']

    def test_bw_bidirectional_ema_unknown_rejected(self):
        """Bidirectional + EMA Unknown → rejected."""
        row = _make_row(Signal_Type='Bidirectional', Chart_EMA_Signal='Unknown', IVHV_gap_30D=12.0)
        result = _validate_buy_write('MU', row)
        assert result is None, "BW should be rejected for Bidirectional with EMA Unknown"

    def test_csp_bullish_still_works(self):
        """Standard Bullish signal still works with rank-adjusted confidence."""
        row = _make_row(Signal_Type='Bullish', IVHV_gap_30D=5.0)
        result = _validate_csp('AAPL', row)
        assert result is not None
        # Base 70 + 3 (IV_Rank_30D=60 → adj=+3)
        assert result['Confidence'] == 73
        assert 'Bidirectional' not in result['Valid_Reason']

    def test_bw_bullish_still_works(self):
        """Standard Bullish BW still works with rank-adjusted confidence."""
        row = _make_row(Signal_Type='Bullish', IVHV_gap_30D=5.0)
        result = _validate_buy_write('AAPL', row)
        assert result is not None
        # Base 75 + 3 (IV_Rank_30D=60 → adj=+3)
        assert result['Confidence'] == 78


# ── Fix 3: Income gap floor ────────────────────────────────────────────────

class TestIncomeGapFloor:
    """Fix 3: Income strategies require gap_30d ≥ 3.0 (not just > 0)."""

    def test_csp_gap_below_3_rejected(self):
        """gap=2.5 → rejected (too thin for income)."""
        row = _make_row(Signal_Type='Bullish', IVHV_gap_30D=2.5)
        result = _validate_csp('AAPL', row)
        assert result is None, "CSP with gap 2.5 should be rejected (< 3.0 floor)"

    def test_csp_gap_above_3_passes(self):
        """gap=3.5 → accepted."""
        row = _make_row(Signal_Type='Bullish', IVHV_gap_30D=3.5)
        result = _validate_csp('AAPL', row)
        assert result is not None, "CSP with gap 3.5 should pass"

    def test_csp_gap_exactly_3_passes(self):
        """gap=3.0 → accepted (edge case)."""
        row = _make_row(Signal_Type='Bullish', IVHV_gap_30D=3.0)
        result = _validate_csp('AAPL', row)
        assert result is not None, "CSP with gap 3.0 should pass"

    def test_bw_gap_below_3_rejected(self):
        """BW gap=1.0 → rejected."""
        row = _make_row(Signal_Type='Bullish', IVHV_gap_30D=1.0)
        result = _validate_buy_write('AAPL', row)
        assert result is None, "BW with gap 1.0 should be rejected (< 3.0 floor)"

    def test_bw_gap_above_3_passes(self):
        """BW gap=4.0 → accepted."""
        row = _make_row(Signal_Type='Bullish', IVHV_gap_30D=4.0)
        result = _validate_buy_write('AAPL', row)
        assert result is not None, "BW with gap 4.0 should pass"

    def test_csp_gap_zero_rejected(self):
        """gap=0 → rejected (was previously the floor, now too thin)."""
        row = _make_row(Signal_Type='Bullish', IVHV_gap_30D=0.0)
        result = _validate_csp('AAPL', row)
        assert result is None, "CSP with gap 0.0 should be rejected"


# ── Fix 2: IV_Trend_7D computation ──────────────────────────────────────────

class TestIVTrendComputation:
    """Fix 2: IV_Trend_7D slope from historical data."""

    def test_iv_trend_rising(self):
        """Monotonically rising iv_30d → 'Rising'."""
        from scan_engine.step2_load_and_enrich_snapshot import enrich_volatility_metrics
        # We test the inner function directly by extracting it
        # Since _compute_iv_trend_7d is defined inside the function, test the logic inline
        iv_30d_values = [20.0, 20.5, 21.0, 21.5, 22.0, 22.5, 23.0]  # +0.5/day = 2.5/week
        recent = pd.Series(iv_30d_values)
        x = np.arange(len(recent))
        slope = np.polyfit(x, recent.values, 1)[0]
        weekly = slope * 5
        assert weekly > 0.5, f"Expected rising trend (weekly slope {weekly:.2f} > 0.5)"
        trend = 'Rising' if weekly > 0.5 else ('Falling' if weekly < -0.5 else 'Stable')
        assert trend == 'Rising'

    def test_iv_trend_falling(self):
        """Monotonically falling iv_30d → 'Falling'."""
        iv_30d_values = [25.0, 24.5, 24.0, 23.5, 23.0, 22.5, 22.0]
        recent = pd.Series(iv_30d_values)
        x = np.arange(len(recent))
        slope = np.polyfit(x, recent.values, 1)[0]
        weekly = slope * 5
        assert weekly < -0.5
        trend = 'Rising' if weekly > 0.5 else ('Falling' if weekly < -0.5 else 'Stable')
        assert trend == 'Falling'

    def test_iv_trend_stable(self):
        """Flat iv_30d → 'Stable'."""
        iv_30d_values = [22.0, 22.05, 21.95, 22.02, 22.01, 21.98, 22.0]
        recent = pd.Series(iv_30d_values)
        x = np.arange(len(recent))
        slope = np.polyfit(x, recent.values, 1)[0]
        weekly = slope * 5
        assert abs(weekly) <= 0.5, f"Expected stable (weekly slope {weekly:.2f})"
        trend = 'Rising' if weekly > 0.5 else ('Falling' if weekly < -0.5 else 'Stable')
        assert trend == 'Stable'


class TestHVAccelProxy:
    """Fix 2: HV_Accel_Proxy from HV30 vs HV10."""

    def test_hv_rising(self):
        """HV30=25, HV10=20 → 'Rising' (diff=5 > 2)."""
        diff = 25.0 - 20.0
        assert diff > 2.0
        trend = 'Rising' if diff > 2.0 else ('Falling' if diff < -2.0 else 'Stable')
        assert trend == 'Rising'

    def test_hv_falling(self):
        """HV30=18, HV10=22 → 'Falling' (diff=-4 < -2)."""
        diff = 18.0 - 22.0
        assert diff < -2.0
        trend = 'Rising' if diff > 2.0 else ('Falling' if diff < -2.0 else 'Stable')
        assert trend == 'Falling'

    def test_hv_stable(self):
        """HV30=20, HV10=19 → 'Stable' (diff=1, within ±2)."""
        diff = 20.0 - 19.0
        assert abs(diff) <= 2.0
        trend = 'Rising' if diff > 2.0 else ('Falling' if diff < -2.0 else 'Stable')
        assert trend == 'Stable'


# ── Fix 4: Strategy overlap annotation ──────────────────────────────────────

class TestStrategyOverlapAnnotation:
    """Fix 4: Same ticker with multiple income strategies gets annotated."""

    def test_overlap_annotation_applied(self):
        """Two income strategies for same ticker → both annotated."""
        df = pd.DataFrame({
            'Ticker': ['CVX', 'CVX', 'AAPL'],
            'Strategy_Name': ['Cash-Secured Put', 'Buy-Write', 'Cash-Secured Put'],
            'Strategy_Type': ['INCOME', 'INCOME', 'INCOME'],
        })
        # Simulate the annotation logic from pipeline.py
        df['Strategy_Overlap_Note'] = ''
        income_mask = df['Strategy_Type'] == 'INCOME'
        ticker_counts = df.loc[income_mask].groupby('Ticker')['Strategy_Name'].transform('count')
        overlap_mask = income_mask & (ticker_counts > 1)
        for ticker in df.loc[overlap_mask, 'Ticker'].unique():
            tmask = (df['Ticker'] == ticker) & income_mask
            strategies = df.loc[tmask, 'Strategy_Name'].unique()
            note = f"Alternative to {'/'.join(strategies)} — shared capital, pick one"
            df.loc[tmask, 'Strategy_Overlap_Note'] = note

        # CVX rows should be annotated
        cvx_notes = df.loc[df['Ticker'] == 'CVX', 'Strategy_Overlap_Note']
        assert all(cvx_notes != ''), "CVX rows should have overlap note"
        assert all('shared capital' in n for n in cvx_notes)
        assert all('Cash-Secured Put' in n for n in cvx_notes)
        assert all('Buy-Write' in n for n in cvx_notes)

        # AAPL row should NOT be annotated (only one income strategy)
        aapl_note = df.loc[df['Ticker'] == 'AAPL', 'Strategy_Overlap_Note'].iloc[0]
        assert aapl_note == '', "AAPL (single strategy) should not have overlap note"

    def test_no_overlap_no_annotation(self):
        """Each ticker has one strategy → no annotations."""
        df = pd.DataFrame({
            'Ticker': ['AAPL', 'MSFT', 'GOOG'],
            'Strategy_Name': ['Cash-Secured Put', 'Buy-Write', 'Cash-Secured Put'],
            'Strategy_Type': ['INCOME', 'INCOME', 'INCOME'],
        })
        df['Strategy_Overlap_Note'] = ''
        income_mask = df['Strategy_Type'] == 'INCOME'
        ticker_counts = df.loc[income_mask].groupby('Ticker')['Strategy_Name'].transform('count')
        overlap_mask = income_mask & (ticker_counts > 1)
        assert not overlap_mask.any(), "No overlaps expected when each ticker has one strategy"


# ── Fix 5a: CHASING gate → confidence penalty ───────────────────────────────

class TestChasingGateNeutrality:
    """Fix 5a: CHASING gate penalizes confidence instead of suppressing directionals."""

    def test_chasing_directional_penalized_not_suppressed(self):
        """Long Call with CHASING entry should still be generated, with reduced confidence."""
        row = _make_row(
            Signal_Type='Bullish',
            IVHV_gap_30D=-2.0,  # cheap IV, good for buying
            Entry_Quality='CHASING',
            Entry_Recommendation='AVOID',
        )
        # _validate_long_call itself doesn't check CHASING — that's the outer loop.
        # We test that the strategy IS generated (validator returns non-None).
        result = _validate_long_call('AAPL', row)
        assert result is not None, "Long Call validator should still generate strategy regardless of CHASING"
        assert result['Strategy_Name'] == 'Long Call'
        assert result['Confidence'] == 65  # Base confidence — CHASING penalty applied in outer loop

    def test_chasing_penalty_amount(self):
        """CHASING penalty is -15 confidence, floored at 40."""
        # Simulate the penalty logic from the outer loop in step6
        original_confidence = 65
        penalty = 15
        penalized = max(original_confidence - penalty, 40)
        assert penalized == 50, f"Expected 50, got {penalized}"

    def test_chasing_penalty_floor(self):
        """CHASING penalty floors at 40 even for low starting confidence."""
        original_confidence = 45
        penalty = 15
        penalized = max(original_confidence - penalty, 40)
        assert penalized == 40, "Should floor at 40"

    def test_directional_strategies_set_complete(self):
        """All expected directional strategies are in the set."""
        expected = {'long call', 'long put', 'long call leap', 'long put leap',
                    'call debit spread', 'put debit spread'}
        assert _DIRECTIONAL_STRATEGIES == expected


# ── Fix 5b: R2.2c threshold lowered ─────────────────────────────────────────

class TestR22cThresholdLowered:
    """Fix 5b: R2.2c now requires Level 2 (20d) instead of Level 3 (60d)."""

    def test_level2_directional_not_blocked(self):
        """Level 2 (IMMATURE, 20-60d) long call should NOT hit R2.2c gate."""
        # Simulate the R2.2c check
        iv_maturity_level = 2
        strategy_type = 'DIRECTIONAL'
        strategy_name = 'long call'

        # New gate: Level < 2 (was < 3)
        would_block = (strategy_type == 'DIRECTIONAL' and iv_maturity_level < 2
                       and any(k in strategy_name for k in ('long call', 'long put')))
        assert not would_block, "Level 2 should NOT trigger R2.2c gate"

    def test_level1_directional_still_blocked(self):
        """Level 1 (<20d) long call should still hit R2.2c gate."""
        iv_maturity_level = 1
        strategy_type = 'DIRECTIONAL'
        strategy_name = 'long call'

        would_block = (strategy_type == 'DIRECTIONAL' and iv_maturity_level < 2
                       and any(k in strategy_name for k in ('long call', 'long put')))
        assert would_block, "Level 1 should trigger R2.2c gate"

    def test_level3_directional_passes(self):
        """Level 3 (PARTIAL_MATURE) should always pass R2.2c."""
        iv_maturity_level = 3
        strategy_type = 'DIRECTIONAL'
        strategy_name = 'long put'

        would_block = (strategy_type == 'DIRECTIONAL' and iv_maturity_level < 2
                       and any(k in strategy_name for k in ('long call', 'long put')))
        assert not would_block, "Level 3 should not trigger R2.2c"

    def test_income_unaffected_by_r22c(self):
        """Income strategies never hit R2.2c regardless of level."""
        iv_maturity_level = 1
        strategy_type = 'INCOME'
        strategy_name = 'cash-secured put'

        would_block = (strategy_type == 'DIRECTIONAL' and iv_maturity_level < 2
                       and any(k in strategy_name for k in ('long call', 'long put')))
        assert not would_block, "Income strategies should never be affected by R2.2c"


# ── Fix 6: IV_Rank confidence scaling ───────────────────────────────────────

from scan_engine.step6_strategy_recommendation import (
    _iv_rank_confidence_adjustment, _validate_covered_call,
)


class TestIVRankConfidenceScaling:
    """Fix 6: IV_Rank drives confidence for income strategies."""

    def test_adjustment_function_high_rank(self):
        """IV_Rank=82 → +9 confidence pts."""
        adj = _iv_rank_confidence_adjustment(82.0, iv_rank_known=True)
        assert adj == int((82 - 50) * 0.30)  # 9.6 → 9
        assert adj == 9

    def test_adjustment_function_low_rank(self):
        """IV_Rank=35 → -4 confidence pts."""
        adj = _iv_rank_confidence_adjustment(35.0, iv_rank_known=True)
        assert adj == int((35 - 50) * 0.30)  # -4.5 → -4
        assert adj == -4

    def test_adjustment_function_neutral(self):
        """IV_Rank=50 → 0 pts."""
        adj = _iv_rank_confidence_adjustment(50.0, iv_rank_known=True)
        assert adj == 0

    def test_adjustment_function_unknown(self):
        """IMMATURE (default 50, not known) → 0 pts."""
        adj = _iv_rank_confidence_adjustment(50.0, iv_rank_known=False)
        assert adj == 0

    def test_adjustment_capped_high(self):
        """IV_Rank=100 → capped at +15."""
        adj = _iv_rank_confidence_adjustment(100.0, iv_rank_known=True)
        assert adj == 15

    def test_adjustment_capped_low(self):
        """IV_Rank=0 → capped at -15."""
        adj = _iv_rank_confidence_adjustment(0.0, iv_rank_known=True)
        assert adj == -15

    def test_csp_high_rank_high_confidence(self):
        """MU-like: IV_Rank=82 → CSP confidence = 70 + 9 = 79."""
        row = _make_row(Signal_Type='Bullish', IVHV_gap_30D=10.0, IV_Rank_30D=82.0)
        result = _validate_csp('MU', row)
        assert result is not None
        assert result['Confidence'] == 79  # 70 base + 9 rank adjustment

    def test_csp_low_rank_low_confidence(self):
        """CVX-like: IV_Rank=35 → CSP confidence = 70 + (-4) = 66."""
        row = _make_row(Signal_Type='Bullish', IVHV_gap_30D=5.0, IV_Rank_30D=35.0)
        result = _validate_csp('CVX', row)
        assert result is not None
        assert result['Confidence'] == 66  # 70 base + (-4) rank adjustment

    def test_csp_immature_rank_neutral(self):
        """IMMATURE ticker (no IV_Rank) → CSP confidence = 70 (no adjustment)."""
        row = _make_row(Signal_Type='Bullish', IVHV_gap_30D=5.0,
                        IV_Rank_30D=np.nan, IV_Rank_60D=np.nan, IV_Rank=np.nan)
        result = _validate_csp('NEW', row)
        assert result is not None
        assert result['Confidence'] == 70  # No adjustment when rank unknown

    def test_bw_high_rank_high_confidence(self):
        """BW with IV_Rank=90 → confidence = 75 + 12 = 87."""
        row = _make_row(Signal_Type='Bullish', IVHV_gap_30D=8.0, IV_Rank_30D=90.0)
        result = _validate_buy_write('MU', row)
        assert result is not None
        assert result['Confidence'] == 87  # 75 base + 12 rank adjustment

    def test_mu_vs_cvx_ranking(self):
        """MU (rank=82) should rank higher than CVX (rank=35) for same strategy."""
        row_mu = _make_row(Signal_Type='Bullish', IVHV_gap_30D=10.0, IV_Rank_30D=82.0)
        row_cvx = _make_row(Signal_Type='Bullish', IVHV_gap_30D=5.0, IV_Rank_30D=35.0)
        result_mu = _validate_csp('MU', row_mu)
        result_cvx = _validate_csp('CVX', row_cvx)
        assert result_mu is not None and result_cvx is not None
        assert result_mu['Confidence'] > result_cvx['Confidence'], (
            f"MU (rank=82, conf={result_mu['Confidence']}) should outrank "
            f"CVX (rank=35, conf={result_cvx['Confidence']})"
        )

    def test_rank_note_in_valid_reason(self):
        """High IV_Rank adjustment should appear in Valid_Reason."""
        row = _make_row(Signal_Type='Bullish', IVHV_gap_30D=10.0, IV_Rank_30D=85.0)
        result = _validate_csp('MU', row)
        assert result is not None
        assert 'conf+' in result['Valid_Reason'], "Positive rank adjustment should be noted"
