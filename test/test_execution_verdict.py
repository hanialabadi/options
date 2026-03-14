"""Tests for scan_engine.execution_verdict — post-step12 triage engine."""

import pandas as pd
import numpy as np
import pytest
from scan_engine.execution_verdict import (
    compute_execution_verdicts,
    generate_verdict_wait_conditions,
)


def _make_ready(**overrides):
    """Factory for a minimal READY candidate row."""
    base = {
        'Ticker': 'AAPL',
        'Strategy_Name': 'Long Call',
        'Execution_Status': 'READY',
        'Trade_Edge_Score': 80.0,
        'Position_Conflict': '',
        'Weekly_Trend_Bias': 'ALIGNED',
        'Blind_Spot_Multiplier': 1.0,
        'IV_Headwind_Multiplier': 1.0,
        'MC_VP_Verdict': 'FAIR',
        'MC_VP_Score': 0.9,
        'MC_CVaR': -500.0,
        'Mid_Price': 5.0,
        'timing_quality': 'MODERATE',
        'DQS_Score': 85.0,
        'confidence_band': 'HIGH',
    }
    base.update(overrides)
    return base


class TestBasicVerdicts:
    def test_clean_candidate_gets_execute(self):
        df = pd.DataFrame([_make_ready()])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'
        assert pd.notna(result.iloc[0]['Execution_Rank'])

    def test_empty_df(self):
        df = pd.DataFrame()
        result = compute_execution_verdicts(df)
        assert result.empty

    def test_size_up_winning_executes_with_note(self):
        """SIZE_UP on non-loser → note only, not SKIP (Murphy: add to winners)."""
        df = pd.DataFrame([_make_ready(
            Position_Conflict='SIZE_UP: already long bullish on AAPL',
            Mgmt_Track_Record='PROVEN_WINNER',
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'
        assert 'pyramiding' in result.iloc[0]['Verdict_Reason'].lower()

    def test_size_up_unknown_track_executes_with_note(self):
        """SIZE_UP with unknown track record → note, not SKIP."""
        df = pd.DataFrame([_make_ready(
            Position_Conflict='SIZE_UP: already long bullish on AAPL',
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'
        assert 'note:' in result.iloc[0]['Verdict_Reason'].lower()

    def test_size_up_proven_loser_skips(self):
        """SIZE_UP on PROVEN_LOSER → SKIP (Murphy: never add to losing position)."""
        df = pd.DataFrame([_make_ready(
            Position_Conflict='SIZE_UP: already long bearish on AAPL',
            Mgmt_Track_Record='PROVEN_LOSER',
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'SKIP'
        assert 'losing' in result.iloc[0]['Verdict_Reason'].lower()
        assert 'Murphy' in result.iloc[0]['Verdict_Reason']

    def test_size_up_with_scale_up_always_executes(self):
        """Explicit SCALE_UP request overrides all overlap checks."""
        df = pd.DataFrame([_make_ready(
            Position_Conflict='SIZE_UP: already long bullish on AAPL',
            Scale_Up_Candidate=True,
            Mgmt_Track_Record='PROVEN_LOSER',
        )])
        result = compute_execution_verdicts(df, scale_up_tickers={'AAPL'})
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'

    def test_opposing_position_always_skips(self):
        df = pd.DataFrame([_make_ready(
            Position_Conflict='CONFLICT: existing bearish position on AAPL'
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'SKIP'
        assert 'opposing' in result.iloc[0]['Verdict_Reason']


class TestSignalConflicts:
    def test_weekly_conflicting_plus_blind_spot_skips(self):
        df = pd.DataFrame([_make_ready(
            Weekly_Trend_Bias='CONFLICTING',
            Blind_Spot_Multiplier=0.86,
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'SKIP'
        assert 'CONFLICTING' in result.iloc[0]['Verdict_Reason']

    def test_weekly_conflicting_alone_skips_directional(self):
        df = pd.DataFrame([_make_ready(
            Weekly_Trend_Bias='CONFLICTING',
            Blind_Spot_Multiplier=1.0,
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'SKIP'

    def test_weekly_conflicting_income_strategy_not_skipped(self):
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Buy-Write',
            Weekly_Trend_Bias='CONFLICTING',
            Blind_Spot_Multiplier=1.0,
        )])
        result = compute_execution_verdicts(df)
        # Income with weekly conflicting + no blind spot → not skipped by signal filter
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'


class TestVariancePremium:
    def test_expensive_vp_skips_directional(self):
        df = pd.DataFrame([_make_ready(
            MC_VP_Verdict='EXPENSIVE',
            MC_VP_Score=0.74,
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'SKIP'
        assert 'EXPENSIVE' in result.iloc[0]['Verdict_Reason']

    def test_expensive_vp_does_not_skip_income(self):
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Cash-Secured Put',
            MC_VP_Verdict='EXPENSIVE',
            MC_VP_Score=0.74,
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'


class TestIVHeadwind:
    def test_severe_headwind_plus_poor_timing_skips(self):
        df = pd.DataFrame([_make_ready(
            IV_Headwind_Multiplier=0.76,
            timing_quality='POOR',
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'SKIP'

    def test_severe_headwind_plus_blind_spot_skips(self):
        df = pd.DataFrame([_make_ready(
            IV_Headwind_Multiplier=0.78,
            Blind_Spot_Multiplier=0.85,
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'SKIP'

    def test_moderate_headwind_good_timing_executes(self):
        df = pd.DataFrame([_make_ready(
            IV_Headwind_Multiplier=0.85,
            timing_quality='GOOD',
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'


class TestPMCCCVaR:
    def test_pmcc_insane_cvar_skips(self):
        df = pd.DataFrame([_make_ready(
            Strategy_Name='PMCC',
            MC_CVaR=-124000.0,
            Mid_Price=1.08,
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'SKIP'
        assert 'miscalculated' in result.iloc[0]['Verdict_Reason']


class TestSameTickerDedup:
    def test_same_ticker_directional_keeps_best(self):
        df = pd.DataFrame([
            _make_ready(Ticker='DVN', Strategy_Name='Long Call', Trade_Edge_Score=80.0),
            _make_ready(Ticker='DVN', Strategy_Name='Long Call LEAP', Trade_Edge_Score=70.0),
        ])
        result = compute_execution_verdicts(df)
        verdicts = result.set_index('Strategy_Name')['Execution_Verdict']
        assert verdicts['Long Call'] == 'EXECUTE'
        assert verdicts['Long Call LEAP'] == 'ALTERNATIVE'

    def test_same_ticker_income_keeps_best(self):
        df = pd.DataFrame([
            _make_ready(Ticker='KMI', Strategy_Name='Cash-Secured Put', Trade_Edge_Score=20.0),
            _make_ready(Ticker='KMI', Strategy_Name='Buy-Write', Trade_Edge_Score=15.0),
        ])
        result = compute_execution_verdicts(df)
        verdicts = result.set_index('Strategy_Name')['Execution_Verdict']
        assert verdicts['Cash-Secured Put'] == 'EXECUTE'
        assert verdicts['Buy-Write'] == 'ALTERNATIVE'

    def test_same_ticker_one_income_one_directional_both_execute(self):
        df = pd.DataFrame([
            _make_ready(Ticker='DVN', Strategy_Name='Long Call', Trade_Edge_Score=80.0),
            _make_ready(Ticker='DVN', Strategy_Name='Buy-Write', Trade_Edge_Score=15.0),
        ])
        result = compute_execution_verdicts(df)
        verdicts = result.set_index('Strategy_Name')['Execution_Verdict']
        assert verdicts['Long Call'] == 'EXECUTE'
        assert verdicts['Buy-Write'] == 'EXECUTE'

    def test_different_tickers_no_dedup(self):
        df = pd.DataFrame([
            _make_ready(Ticker='AAPL', Strategy_Name='Long Call', Trade_Edge_Score=80.0),
            _make_ready(Ticker='MSFT', Strategy_Name='Long Call', Trade_Edge_Score=70.0),
        ])
        result = compute_execution_verdicts(df)
        assert (result['Execution_Verdict'] == 'EXECUTE').all()


class TestIncomePCSFloor:
    """Filter 9: Income PCS floor — PCS < 55 = SKIP (structural)."""

    def test_pcs_below_floor_skips_income(self):
        """CMCSA scenario: PCS 50 Rejected — too weak to sell."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Buy-Write',
            PCS_Score_V2=50.0,
            PCS_Status='Rejected',
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'SKIP'
        assert 'PCS' in result.iloc[0]['Verdict_Reason']
        assert 'below floor' in result.iloc[0]['Verdict_Reason']

    def test_pcs_at_floor_55_executes(self):
        """PCS exactly at 55 = at floor, not below — passes."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Cash-Secured Put',
            PCS_Score_V2=55.0,
            PCS_Status='Watch',
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'

    def test_pcs_valid_tier_executes(self):
        """PCS 85 Valid — clean income candidate."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Buy-Write',
            PCS_Score_V2=85.0,
            PCS_Status='Valid',
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'

    def test_directional_not_affected_by_pcs_floor(self):
        """PCS floor only applies to income strategies."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Long Call',
            PCS_Score_V2=40.0,
            PCS_Status='Rejected',
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'

    def test_pcs_nan_no_skip(self):
        """Missing PCS → graceful degradation, no skip."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Buy-Write',
            PCS_Score_V2=np.nan,
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'

    def test_pcs_fallback_column(self):
        """Uses PCS_Score when PCS_Score_V2 absent."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Cash-Secured Put',
            PCS_Score=48.0,
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'SKIP'
        assert 'PCS' in result.iloc[0]['Verdict_Reason']


class TestIncomePremiumUnderselling:
    """Filter 10: Income premium underselling — selling > 8% below BS fair value → SKIP."""

    def test_underselling_skips_income(self):
        """DVN scenario: selling 8.8% below fair value — wait for better premium."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Buy-Write',
            Premium_vs_FairValue_Pct=-8.8,
            PCS_Score_V2=70.0,
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'SKIP'
        assert 'below BS fair value' in result.iloc[0]['Verdict_Reason']

    def test_severe_underselling_skips(self):
        """CMCSA scenario: selling 10.1% below — stacks with PCS if both fire."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Buy-Write',
            Premium_vs_FairValue_Pct=-10.1,
            PCS_Score_V2=50.0,
            PCS_Status='Rejected',
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'SKIP'
        # Both PCS and underselling should fire
        reason = result.iloc[0]['Verdict_Reason']
        assert 'PCS' in reason
        assert 'below BS fair value' in reason

    def test_at_limit_minus_8_executes(self):
        """Exactly -8.0% = at limit, not below — passes."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Cash-Secured Put',
            Premium_vs_FairValue_Pct=-8.0,
            PCS_Score_V2=70.0,
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'

    def test_selling_above_fair_value_executes(self):
        """KMI scenario: selling at or above fair value — good premium capture."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Cash-Secured Put',
            Premium_vs_FairValue_Pct=2.5,
            PCS_Score_V2=75.0,
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'

    def test_directional_not_affected_by_underselling(self):
        """Premium underselling only applies to income strategies."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Long Call',
            Premium_vs_FairValue_Pct=-12.0,
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'

    def test_nan_premium_no_skip(self):
        """Missing Premium_vs_FairValue_Pct → graceful degradation."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Buy-Write',
            Premium_vs_FairValue_Pct=np.nan,
            PCS_Score_V2=70.0,
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'

    def test_no_column_no_skip(self):
        """Missing column → graceful degradation."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Buy-Write',
            PCS_Score_V2=70.0,
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'


class TestInterpreterFloor:
    """Filter 6a: Interpreter absolute floor — below 60/120 = SKIP regardless of vol edge."""

    def test_below_floor_skips_even_with_favorable_vol(self):
        """AMGN scenario: Interp 55, vol edge FAVORABLE (IV < HV) — still SKIP."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Long Call',
            Interp_Score=55.0,
            Interp_Vol_Edge='FAVORABLE',
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'SKIP'
        assert 'below floor' in result.iloc[0]['Verdict_Reason']

    def test_below_floor_skips_without_vol_edge_column(self):
        """No Interp_Vol_Edge column — floor still catches it."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Long Call',
            Interp_Score=50.0,
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'SKIP'
        assert 'below floor' in result.iloc[0]['Verdict_Reason']

    def test_at_floor_60_not_skipped_by_floor(self):
        """Score exactly at 60 = at floor, not below — floor doesn't fire."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Long Call',
            Interp_Score=60.0,
            Interp_Vol_Edge='FAVORABLE',
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'

    def test_income_not_affected_by_floor(self):
        """Income strategies exempt from interpreter floor."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Cash-Secured Put',
            Interp_Score=40.0,
            Interp_Vol_Edge='UNFAVORABLE',
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'


class TestInterpreterConviction:
    """Filter 6b: Interpreter + Vol edge UNFAVORABLE — graduated (Passarelli Ch.8).

    RAG: Passarelli 0.788 "buying long-term options with IV in the lower third
    of the 12-month range helps improve chances" — preference, not veto.
    60-69 + UNFAVORABLE = SKIP (weak mechanics + wrong vol regime)
    70-79 + UNFAVORABLE = note only (viable thesis, wait for vol to cheapen)
    """

    def test_60_69_unfavorable_vol_skips(self):
        """Interp 60-69 + UNFAVORABLE = hard SKIP (weak mechanics + wrong vol)."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Long Put LEAP',
            Interp_Score=65.0,
            Interp_Vol_Edge='UNFAVORABLE',
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'SKIP'
        assert 'interpreter' in result.iloc[0]['Verdict_Reason'].lower()
        assert 'Passarelli' in result.iloc[0]['Verdict_Reason']

    def test_70_79_unfavorable_vol_executes_with_note(self):
        """Interp 70-79 + UNFAVORABLE = EXECUTE with note (viable thesis, wait for vol).
        RAG: Passarelli says "lower third helps improve chances" — preference, not veto.
        """
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Long Call LEAP',
            Interp_Score=76.0,
            Interp_Vol_Edge='UNFAVORABLE',
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'
        assert 'thesis viable' in result.iloc[0]['Verdict_Reason'].lower()

    def test_boundary_70_unfavorable_vol_executes(self):
        """Score exactly at marginal threshold = note path, not SKIP."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Long Call',
            Interp_Score=70.0,
            Interp_Vol_Edge='UNFAVORABLE',
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'

    def test_boundary_69_unfavorable_vol_skips(self):
        """Score at 69 = still in 60-69 band → SKIP."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Long Put LEAP',
            Interp_Score=69.0,
            Interp_Vol_Edge='UNFAVORABLE',
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'SKIP'

    def test_weak_interp_favorable_vol_executes(self):
        """Interp 65 but vol edge favorable — marginal but allowed."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Long Call LEAP',
            Interp_Score=65.0,
            Interp_Vol_Edge='FAVORABLE',
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'

    def test_strong_interp_unfavorable_vol_executes(self):
        """High interpreter overrides vol edge concern."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Long Call',
            Interp_Score=95.0,
            Interp_Vol_Edge='UNFAVORABLE',
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'

    def test_income_not_affected_by_interp_filter(self):
        """Income strategies are sellers — vol unfavorable doesn't apply."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Cash-Secured Put',
            Interp_Score=65.0,
            Interp_Vol_Edge='UNFAVORABLE',
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'

    def test_boundary_score_80_not_skipped(self):
        """Score exactly at weak threshold = not weak, should pass."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Long Call',
            Interp_Score=80.0,
            Interp_Vol_Edge='UNFAVORABLE',
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'

    def test_no_interp_columns_no_skip(self):
        """Graceful degradation when interpreter columns absent."""
        df = pd.DataFrame([_make_ready()])
        # No Interp_Score or Interp_Vol_Edge columns
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'


class TestIntradayExecution:
    """Filter 7: Intraday execution DEFER."""

    def test_defer_skips(self):
        df = pd.DataFrame([_make_ready(
            Intraday_Readiness='DEFER',
            Intraday_Execution_Score=35.0,
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'SKIP'
        assert 'DEFER' in result.iloc[0]['Verdict_Reason']

    def test_execute_now_passes(self):
        df = pd.DataFrame([_make_ready(
            Intraday_Readiness='EXECUTE_NOW',
            Intraday_Execution_Score=85.0,
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'

    def test_stage_and_wait_passes(self):
        """STAGE_AND_WAIT is not a hard block — monitor for better entry."""
        df = pd.DataFrame([_make_ready(
            Intraday_Readiness='STAGE_AND_WAIT',
            Intraday_Execution_Score=55.0,
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'

    def test_off_hours_passes(self):
        """OFF_HOURS = no data, don't penalize."""
        df = pd.DataFrame([_make_ready(
            Intraday_Readiness='OFF_HOURS',
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'

    def test_no_intraday_column_no_skip(self):
        """Graceful degradation."""
        df = pd.DataFrame([_make_ready()])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'


class TestRSIOverextended:
    """Filter 8: RSI overextended entry (Murphy Ch.10)."""

    def test_put_oversold_rsi_skips(self):
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Long Put LEAP',
            RSI_14=25.0,
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'SKIP'
        assert 'oversold' in result.iloc[0]['Verdict_Reason']
        assert 'Murphy' in result.iloc[0]['Verdict_Reason']

    def test_call_overbought_rsi_skips(self):
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Long Call',
            RSI_14=82.0,
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'SKIP'
        assert 'overbought' in result.iloc[0]['Verdict_Reason']

    def test_put_moderate_rsi_executes(self):
        """RSI 40 — still room to fall, not overextended."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Long Put',
            RSI_14=40.0,
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'

    def test_call_moderate_rsi_executes(self):
        """RSI 60 — still room to rise, not overbought."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Long Call LEAP',
            RSI_14=60.0,
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'

    def test_put_boundary_rsi_30_not_skipped(self):
        """RSI exactly 30 = not below threshold."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Long Put',
            RSI_14=30.0,
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'

    def test_call_boundary_rsi_75_not_skipped(self):
        """RSI exactly 75 = not above threshold."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Long Call',
            RSI_14=75.0,
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'

    def test_income_not_affected_by_rsi(self):
        """Income strategies don't care about RSI overextension."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Cash-Secured Put',
            RSI_14=20.0,
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'

    def test_nan_rsi_no_skip(self):
        """Missing RSI → graceful degradation."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Long Put',
            RSI_14=np.nan,
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'


class TestLeapRhoHeadwind:
    """Filter 9: LEAP put rho headwind — informational note (Passarelli Ch.6).

    Rho is annotation-only, not a SKIP. With exit targets at +100%/-50% and
    DTE≤90 time stop, holding period is 3-6 months. Rho cost is <1% of position.
    """

    def test_leap_put_high_rho_annotated_but_executes(self):
        """HIGH rho on LEAP put = note in Verdict_Reason, but still EXECUTE."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Long Put LEAP',
            LEAP_Rate_Sensitivity='HIGH (Rho=-0.150, ~$15.00/contract per +1% rate)',
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'
        assert 'rho headwind' in result.iloc[0]['Verdict_Reason'].lower()

    def test_leap_put_low_rho_no_note(self):
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Long Put LEAP',
            LEAP_Rate_Sensitivity='LOW (Rho=-0.030)',
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'
        assert 'rho' not in result.iloc[0]['Verdict_Reason'].lower()

    def test_leap_call_high_rho_no_note(self):
        """LEAP calls benefit from rising rates — no headwind note."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Long Call LEAP',
            LEAP_Rate_Sensitivity='HIGH (Rho=+0.150, ~$15.00/contract per +1% rate)',
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'
        assert 'rho' not in result.iloc[0]['Verdict_Reason'].lower()

    def test_non_leap_not_affected(self):
        """Short-dated options: rho is negligible, no annotation."""
        df = pd.DataFrame([_make_ready(
            Strategy_Name='Long Put',
            LEAP_Rate_Sensitivity='HIGH (Rho=-0.150)',
        )])
        result = compute_execution_verdicts(df)
        assert result.iloc[0]['Execution_Verdict'] == 'EXECUTE'
        assert 'rho' not in result.iloc[0]['Verdict_Reason'].lower()


class TestRanking:
    def test_execute_candidates_ranked_by_edge(self):
        df = pd.DataFrame([
            _make_ready(Ticker='AAPL', Trade_Edge_Score=90.0),
            _make_ready(Ticker='MSFT', Trade_Edge_Score=70.0),
            _make_ready(Ticker='GOOG', Trade_Edge_Score=80.0),
        ])
        result = compute_execution_verdicts(df)
        exec_result = result[result['Execution_Verdict'] == 'EXECUTE'].sort_values('Execution_Rank')
        assert list(exec_result['Ticker']) == ['AAPL', 'GOOG', 'MSFT']

    def test_skip_candidates_have_no_rank(self):
        df = pd.DataFrame([_make_ready(
            Weekly_Trend_Bias='CONFLICTING',
            Blind_Spot_Multiplier=0.85,
        )])
        result = compute_execution_verdicts(df)
        assert pd.isna(result.iloc[0]['Execution_Rank'])


# ── Verdict → Wait Condition Tests ──────────────────────────────────────────

class TestVerdictWaitConditions:
    """Tests for generate_verdict_wait_conditions() — SKIP → clearance mapping."""

    def _make_row(self, **overrides):
        """Create a pd.Series resembling a verdict-SKIP candidate."""
        base = _make_ready(**overrides)
        return pd.Series(base)

    def test_weekly_conflicting_generates_trend_condition(self):
        row = self._make_row(Strategy_Name='Long Call')
        conds = generate_verdict_wait_conditions(
            "weekly trend CONFLICTING with directional thesis", row
        )
        assert len(conds) >= 1
        descs = ' '.join(c['description'] for c in conds)
        assert 'Weekly_Trend_Bias' in descs or 'ALIGNED' in descs

    def test_weekly_conflicting_plus_blind_spot_generates_both(self):
        row = self._make_row(Strategy_Name='Long Call', Blind_Spot_Multiplier=0.85)
        conds = generate_verdict_wait_conditions(
            "weekly CONFLICTING + blind spot 0.85", row
        )
        metrics = [c['config'].get('metric', '') for c in conds]
        assert 'Weekly_Trend_Bias' in metrics
        assert 'Blind_Spot_Multiplier' in metrics

    def test_variance_premium_generates_vp_condition(self):
        row = self._make_row(Strategy_Name='Long Call')
        conds = generate_verdict_wait_conditions(
            "variance premium EXPENSIVE (VP=0.60) — overpaying for vol", row
        )
        assert any(c['config'].get('metric') == 'MC_VP_Score' for c in conds)

    def test_iv_headwind_generates_iv_condition(self):
        row = self._make_row(Strategy_Name='Long Put', timing_quality='POOR')
        conds = generate_verdict_wait_conditions(
            "severe IV headwind (0.72) + POOR timing", row
        )
        metrics = [c['config'].get('metric', '') for c in conds]
        assert 'IV_Headwind_Multiplier' in metrics
        assert 'timing_quality' in metrics

    def test_interpreter_weak_generates_iv_rank_and_interp(self):
        row = self._make_row(
            Strategy_Name='Long Put LEAP',
            Interp_Score=69,
            Interp_Vol_Edge='UNFAVORABLE',
            IV_Rank_Pctile=65,
        )
        conds = generate_verdict_wait_conditions(
            "interpreter 69/120 + vol edge UNFAVORABLE — weak conviction (Passarelli Ch.8)",
            row,
        )
        metrics = [c['config'].get('metric', '') for c in conds]
        assert 'IV_Rank' in metrics
        assert 'Interp_Score' in metrics
        # IV_Rank condition threshold should be 40
        iv_cond = [c for c in conds if c['config'].get('metric') == 'IV_Rank'][0]
        assert iv_cond['config']['threshold'] == 40.0

    def test_intraday_defer_generates_session_wait(self):
        row = self._make_row(Strategy_Name='Long Call')
        conds = generate_verdict_wait_conditions(
            "intraday execution DEFER (score 35) — unfavorable market microstructure", row
        )
        assert any(c['type'] == 'time_delay' for c in conds)
        assert any('next_session' in c.get('config', {}) for c in conds)

    def test_rsi_oversold_put_generates_rsi_recovery(self):
        row = self._make_row(Strategy_Name='Long Put LEAP', RSI_14=22.0)
        conds = generate_verdict_wait_conditions(
            "RSI 22 already oversold for put entry — bearish move extended (Murphy Ch.10)",
            row,
        )
        rsi_conds = [c for c in conds if c['config'].get('metric') == 'RSI_14']
        assert len(rsi_conds) == 1
        assert rsi_conds[0]['config']['operator'] == 'greater_than'
        assert rsi_conds[0]['config']['threshold'] == 35.0  # 30 + 5

    def test_rsi_overbought_call_generates_rsi_pullback(self):
        row = self._make_row(Strategy_Name='Long Call', RSI_14=82.0)
        conds = generate_verdict_wait_conditions(
            "RSI 82 overbought for call entry — bullish exhaustion risk (Murphy Ch.10)",
            row,
        )
        rsi_conds = [c for c in conds if c['config'].get('metric') == 'RSI_14']
        assert len(rsi_conds) == 1
        assert rsi_conds[0]['config']['operator'] == 'less_than'
        assert rsi_conds[0]['config']['threshold'] == 70.0  # 75 - 5

    def test_position_overlap_losing_generates_session_wait(self):
        """PROVEN_LOSER overlap → position overlap reason → time delay wait."""
        row = self._make_row(Strategy_Name='Long Call', Mgmt_Track_Record='PROVEN_LOSER')
        conds = generate_verdict_wait_conditions(
            "position overlap on losing ticker (SIZE_UP: already long bearish) — Murphy: never add to a losing position", row
        )
        assert any(c['type'] == 'time_delay' for c in conds)

    def test_notes_ignored(self):
        """Informational notes (rho headwind) should not generate conditions."""
        row = self._make_row(Strategy_Name='Long Put LEAP')
        conds = generate_verdict_wait_conditions(
            "note: LEAP put rho headwind (HIGH)", row
        )
        # Should only have the fallback generic wait
        assert len(conds) == 1
        assert conds[0]['type'] == 'time_delay'

    def test_multiple_reasons_generate_multiple_conditions(self):
        """Stacked reasons produce conditions for each."""
        row = self._make_row(
            Strategy_Name='Long Put LEAP',
            RSI_14=22.0,
            Interp_Score=65,
            Interp_Vol_Edge='UNFAVORABLE',
            IV_Rank_Pctile=70,
        )
        reason = (
            "interpreter 65/120 + vol edge UNFAVORABLE — weak conviction (Passarelli Ch.8); "
            "RSI 22 already oversold for put entry — bearish move extended (Murphy Ch.10)"
        )
        conds = generate_verdict_wait_conditions(reason, row)
        metrics = [c['config'].get('metric', '') for c in conds]
        assert 'IV_Rank' in metrics
        assert 'Interp_Score' in metrics
        assert 'RSI_14' in metrics

    def test_pmcc_cvar_generates_recheck(self):
        row = self._make_row(Strategy_Name='PMCC')
        conds = generate_verdict_wait_conditions(
            "CVaR $124,000 appears miscalculated for PMCC", row
        )
        assert any(c['type'] == 'time_delay' for c in conds)

    def test_interpreter_floor_generates_interp_condition(self):
        """Floor SKIP generates Interp_Score > 60 condition (not IV_Rank)."""
        row = self._make_row(Strategy_Name='Long Call', Interp_Score=55)
        conds = generate_verdict_wait_conditions(
            "interpreter 55/120 below floor (60) — strategy mechanics don't support entry",
            row,
        )
        metrics = [c['config'].get('metric', '') for c in conds]
        assert 'Interp_Score' in metrics
        # Floor condition should NOT require IV_Rank (vol edge is irrelevant)
        assert 'IV_Rank' not in metrics

    def test_pcs_floor_generates_pcs_condition(self):
        """PCS SKIP generates PCS_Score_V2 > 55 condition (structural)."""
        row = self._make_row(Strategy_Name='Buy-Write', PCS_Score_V2=50)
        conds = generate_verdict_wait_conditions(
            "PCS 50 below floor (55) [Rejected] — income quality too weak to sell",
            row,
        )
        pcs_conds = [c for c in conds if c['config'].get('metric') == 'PCS_Score_V2']
        assert len(pcs_conds) == 1
        assert pcs_conds[0]['config']['threshold'] == 55.0

    def test_premium_underselling_generates_fv_condition(self):
        """Underselling SKIP generates Premium_vs_FairValue_Pct > -8 condition (transient)."""
        row = self._make_row(Strategy_Name='Buy-Write', Premium_vs_FairValue_Pct=-8.8)
        conds = generate_verdict_wait_conditions(
            "selling 8.8% below BS fair value (limit 8%) — wait for better premium",
            row,
        )
        fv_conds = [c for c in conds if c['config'].get('metric') == 'Premium_vs_FairValue_Pct']
        assert len(fv_conds) == 1
        assert fv_conds[0]['config']['threshold'] == -8.0

    def test_stacked_income_reasons_generate_both_conditions(self):
        """CMCSA scenario: PCS + underselling both fire → both conditions generated."""
        row = self._make_row(
            Strategy_Name='Buy-Write',
            PCS_Score_V2=50,
            Premium_vs_FairValue_Pct=-10.1,
        )
        reason = (
            "PCS 50 below floor (55) [Rejected] — income quality too weak to sell; "
            "selling 10.1% below BS fair value (limit 8%) — wait for better premium"
        )
        conds = generate_verdict_wait_conditions(reason, row)
        metrics = [c['config'].get('metric', '') for c in conds]
        assert 'PCS_Score_V2' in metrics
        assert 'Premium_vs_FairValue_Pct' in metrics

    def test_empty_reason_generates_fallback(self):
        row = self._make_row(Strategy_Name='Long Call')
        conds = generate_verdict_wait_conditions("", row)
        assert len(conds) >= 1
        assert conds[0]['type'] == 'time_delay'
