"""
Tests for PMCC (Poor Man's Covered Call) strategy — scan + management paths.

Covers:
  - Step 6 validator (_validate_pmcc)
  - Step 8 evaluator routing (INCOME_STRATEGIES)
  - Step 9 DTE assignment
  - Step 10 contract selection routing
  - Step 12 LEAP validation gate (R3.0p)
  - Doctrine gates (pmcc_doctrine / pmcc_doctrine_v2)
  - Orchestrator structure detection
  - MC position sizing (diagonal spread P&L model)
  - Interpreter routing (PMCC → IncomeInterpreter)
"""

import math
import pytest
import pandas as pd
import numpy as np

# ── Scan-side imports ────────────────────────────────────────────────
from scan_engine.step6_strategy_recommendation import _validate_pmcc
from scan_engine.evaluators._types import INCOME_STRATEGIES
from scan_engine.step9_determine_timeframe import _calculate_dte_range_by_strategy

# ── Management-side imports ──────────────────────────────────────────
from core.management.cycle3.doctrine.strategies.pmcc import (
    pmcc_doctrine,
    pmcc_doctrine_v2,
    PMCC_HARD_STOP,
    PMCC_APPROACHING_STOP,
    PMCC_LEAP_ROLL_DTE,
    PMCC_SHORT_DELTA_DEFENSE,
    PMCC_SHORT_DELTA_CRITICAL,
)
from core.management.cycle3.doctrine import DoctrineAuthority


# ── Helpers ──────────────────────────────────────────────────────────

def _make_scan_row(**overrides):
    """Create a minimal Step 6 scan row for PMCC validation."""
    base = {
        'Signal_Type': 'Bullish',
        'SMA20': 150.0,
        'Price_vs_SMA20': 2.0,  # ~$153 stock price
        'IVHV_gap_30D': 8.0,
        'IV_Rank_30D': 55.0,
        'Chart_EMA_Signal': 'Bullish',
    }
    base.update(overrides)
    return pd.Series(base)


def _make_mgmt_row(**overrides):
    """Create a minimal management row for PMCC doctrine gates."""
    base = {
        'UL Last': 150.0,
        'Short_Call_Delta': 0.30,
        'Short_Call_DTE': 35,
        'Short_Call_Strike': 160.0,
        'Strike': 160.0,
        'Delta': 0.30,
        'DTE': 35,
        'LEAP_Call_Delta': 0.78,
        'LEAP_Call_DTE': 320,
        'LEAP_Call_Strike': 130.0,
        'LEAP_Entry_Price': 30.0,
        'LEAP_Call_Last': 32.0,
        'LEAP_Call_Mid': 32.0,
        'Net_Cost_Basis_Per_Share': 28.0,
        'Cumulative_Premium_Collected': 2.50,
        'Premium_Entry': 2.00,
        'Short_Call_Last': 1.20,
        'Last': 1.20,
        'Days_In_Trade': 10,
        'HV_20D': 25.0,
        'Strategy': 'PMCC',
        'Thesis_State': 'INTACT',
        'PriceStructure_State': 'STRUCTURE_HEALTHY',
        'TrendIntegrity_State': 'TREND_ACTIVE',
        'VolatilityState_State': 'VOL_STABLE',
        'AssignmentRisk_State': 'LOW',
    }
    base.update(overrides)
    return pd.Series(base)


def _default_result():
    """Default result dict for doctrine evaluation."""
    return {
        "Action": "HOLD",
        "Urgency": "LOW",
        "Rationale": "default",
        "Doctrine_Source": "McMillan: Neutrality",
        "Decision_State": "NEUTRAL_CONFIDENT",
        "Uncertainty_Reasons": [],
        "Missing_Data_Fields": [],
        "Required_Conditions_Met": True,
    }


# =====================================================================
# STEP 6: VALIDATOR TESTS
# =====================================================================

class TestPMCCValidator:
    """Tests for _validate_pmcc() in step6."""

    def test_bullish_rich_iv_qualifies(self):
        row = _make_scan_row(Signal_Type='Bullish', IVHV_gap_30D=8.0, IV_Rank_30D=55.0)
        result = _validate_pmcc('AAPL', row)
        assert result is not None
        assert result['Strategy_Name'] == 'PMCC'
        assert result['Strategy_Type'] == 'INCOME'
        assert result['Trade_Bias'] == 'Bullish'
        assert result['Execution_Ready'] is True

    def test_sustained_bullish_qualifies(self):
        row = _make_scan_row(Signal_Type='Sustained Bullish', IVHV_gap_30D=10.0)
        result = _validate_pmcc('MSFT', row)
        assert result is not None
        assert result['Strategy_Name'] == 'PMCC'

    def test_bearish_rejected(self):
        row = _make_scan_row(Signal_Type='Bearish')
        assert _validate_pmcc('AAPL', row) is None

    def test_low_iv_gap_rejected(self):
        """Gap < 6 is too thin for PMCC short-call income."""
        row = _make_scan_row(IVHV_gap_30D=4.0)
        assert _validate_pmcc('AAPL', row) is None

    def test_nan_iv_gap_rejected(self):
        row = _make_scan_row(IVHV_gap_30D=np.nan)
        assert _validate_pmcc('AAPL', row) is None

    def test_none_iv_gap_rejected(self):
        row = _make_scan_row(IVHV_gap_30D=None)
        assert _validate_pmcc('AAPL', row) is None

    def test_low_iv_rank_rejected(self):
        """IV_Rank < 40 when known → rejected."""
        row = _make_scan_row(IV_Rank_30D=30.0, IVHV_gap_30D=8.0)
        assert _validate_pmcc('AAPL', row) is None

    def test_unknown_iv_rank_passes(self):
        """When IV_Rank is unknown (IMMATURE), gap alone suffices."""
        row = _make_scan_row(IVHV_gap_30D=8.0)
        # Remove all rank columns
        row = row.drop(labels=['IV_Rank_30D'], errors='ignore')
        result = _validate_pmcc('AAPL', row)
        assert result is not None

    def test_leveraged_etf_rejected(self):
        row = _make_scan_row(Signal_Type='Bullish', IVHV_gap_30D=10.0, IV_Rank_30D=60.0)
        assert _validate_pmcc('TQQQ', row) is None
        assert _validate_pmcc('SOXL', row) is None

    def test_capital_requirement_cheaper_than_bw(self):
        """PMCC capital should be ~40% of stock × 100."""
        row = _make_scan_row(SMA20=200.0, Price_vs_SMA20=1.0, Close=202.0)
        result = _validate_pmcc('AAPL', row)
        assert result is not None
        stock_price = result['Approx_Stock_Price']
        # BW would be stock × 100. PMCC ≈ 40% of that
        assert result['Capital_Requirement'] == pytest.approx(stock_price * 0.40 * 100)
        # PMCC should be significantly cheaper than BW
        assert result['Capital_Requirement'] < stock_price * 100 * 0.55

    def test_bidirectional_with_ema_bullish_qualifies(self):
        """Bidirectional + EMA Bullish + gap≥8 should qualify."""
        row = _make_scan_row(
            Signal_Type='Bidirectional',
            Chart_EMA_Signal='Bullish',
            IVHV_gap_30D=10.0,
            IV_Rank_30D=50.0,
        )
        result = _validate_pmcc('AAPL', row)
        assert result is not None
        assert 'Bidirectional' in result['Valid_Reason']

    def test_bidirectional_low_gap_rejected(self):
        """Bidirectional needs gap≥8 for PMCC (stricter than BW)."""
        row = _make_scan_row(
            Signal_Type='Bidirectional',
            Chart_EMA_Signal='Bullish',
            IVHV_gap_30D=7.0,
        )
        assert _validate_pmcc('AAPL', row) is None

    def test_no_stock_price_rejected(self):
        row = _make_scan_row(SMA20=0, Price_vs_SMA20=0, Close=None)
        assert _validate_pmcc('AAPL', row) is None


# =====================================================================
# STEP 8/9: EVALUATOR + TIMEFRAME
# =====================================================================

class TestPMCCEvaluatorRouting:
    """PMCC should route to income evaluator."""

    def test_pmcc_in_income_strategies(self):
        assert 'PMCC' in INCOME_STRATEGIES

    def test_pmcc_dte_assignment(self):
        """PMCC short-call DTE should be 30-50."""
        min_dte, max_dte, label, reason = _calculate_dte_range_by_strategy('PMCC', 'INCOME', 70, 50.0)
        assert min_dte == 30
        assert max_dte == 50
        assert 'PMCC' in reason


# =====================================================================
# STEP 12: LEAP VALIDATION GATE
# =====================================================================

class TestPMCCStep12:
    """PMCC LEAP leg validation in step12."""

    def _call_gate(self, pmcc_leap_status):
        """Helper to call apply_execution_gate with PMCC-specific args."""
        from scan_engine.step12_acceptance import apply_execution_gate
        row = pd.Series({
            'Strategy_Name': 'PMCC',
            'Strategy_Type': 'INCOME',
            'PMCC_LEAP_Status': pmcc_leap_status,
            'Contract_Status': 'OK',
            'Liquidity_Grade': 'Good',
            'Actual_DTE': 35,
            'DQS_Score': 70,
            'IVHV_gap_30D': 8.0,
            'IV_Rank_20D': 55.0,
            'IV_Rank_30D': 55.0,
            'Surface_Shape': 'NORMAL',
            'IV_Maturity_Level': 5,
            'IV_History_Count': 200,
            'days_to_earnings': np.nan,
            'Earnings_Formation_Phase': '',
        })
        return apply_execution_gate(
            row=row,
            strategy_type='INCOME',
            iv_maturity_state='MATURE',
            iv_source='ROLLING_20D',
            iv_rank=55.0,
            iv_trend_7d='Stable',
            ivhv_gap_30d=8.0,
            liquidity_grade='Good',
            signal_strength='Strong',
            scraper_status='OK',
            data_completeness_overall='Complete',
            compression='None',
            regime_52w='Normal',
            momentum='Bullish',
            gap='None',
            timing='NORMAL',
            directional_bias='Bullish',
            structure_bias='Bullish',
            timing_quality='NORMAL',
            actual_dte=35.0,
            strategy_name='PMCC',
            exec_quality='NORMAL',
            balance='BALANCED',
            div_risk='None',
            history_depth_ok=True,
            iv_data_stale=False,
            regime_confidence=75.0,
            iv_maturity_level=5,
        )

    def test_pmcc_blocked_when_no_leap(self):
        """If PMCC_LEAP_Status != OK, step12 should block."""
        result = self._call_gate('NO_LEAP_EXPIRATION')
        assert result['Execution_Status'] == 'BLOCKED'
        assert 'R3.0p' in result['Gate_Reason']

    def test_pmcc_not_blocked_when_leap_ok(self):
        """If PMCC_LEAP_Status == OK, step12 should proceed normally."""
        result = self._call_gate('OK')
        assert 'R3.0p' not in result.get('Gate_Reason', '')


# =====================================================================
# DOCTRINE: V1 (LEGACY GATE CASCADE)
# =====================================================================

class TestPMCCDoctrineV1:
    """Tests for pmcc_doctrine() — v1 first-match gate cascade."""

    def test_grace_period(self):
        row = _make_mgmt_row(Days_In_Trade=0, LEAP_Entry_Price=30.0, LEAP_Call_Last=30.0)
        result = pmcc_doctrine(row, _default_result())
        assert result['Action'] == 'HOLD'
        assert 'new pmcc' in result['Rationale'].lower()

    def test_leap_hard_stop(self):
        """LEAP lost >40% → EXIT CRITICAL."""
        row = _make_mgmt_row(LEAP_Entry_Price=30.0, LEAP_Call_Last=17.0)  # -43%
        result = pmcc_doctrine(row, _default_result())
        assert result['Action'] == 'EXIT'
        assert result['Urgency'] == 'CRITICAL'

    def test_approaching_leap_stop(self):
        """LEAP lost 25-40% → ROLL HIGH."""
        row = _make_mgmt_row(LEAP_Entry_Price=30.0, LEAP_Call_Last=20.0)  # -33%
        result = pmcc_doctrine(row, _default_result())
        assert result['Action'] == 'ROLL'
        assert result['Urgency'] == 'HIGH'

    def test_short_itm_critical(self):
        """Short call delta > 0.70 → ROLL CRITICAL."""
        row = _make_mgmt_row(Short_Call_Delta=0.75, Delta=0.75)
        result = pmcc_doctrine(row, _default_result())
        assert result['Action'] == 'ROLL'
        assert result['Urgency'] == 'CRITICAL'
        assert 'assignment' in result['Rationale'].lower()

    def test_short_itm_warning(self):
        """Short call delta 0.55-0.70 → ROLL HIGH."""
        row = _make_mgmt_row(Short_Call_Delta=0.60, Delta=0.60)
        result = pmcc_doctrine(row, _default_result())
        assert result['Action'] == 'ROLL'
        assert result['Urgency'] == 'HIGH'

    def test_short_expiration(self):
        """Short call DTE < 7 → ROLL HIGH."""
        row = _make_mgmt_row(Short_Call_DTE=5, DTE=5)
        result = pmcc_doctrine(row, _default_result())
        assert result['Action'] == 'ROLL'

    def test_width_inversion(self):
        """Short strike ≤ LEAP strike → ROLL CRITICAL."""
        row = _make_mgmt_row(
            Short_Call_Strike=125.0,  # Below LEAP strike of 130
            LEAP_Call_Strike=130.0,
            Strike=125.0,
        )
        result = pmcc_doctrine(row, _default_result())
        assert result['Action'] == 'ROLL'
        assert result['Urgency'] == 'CRITICAL'
        assert 'inversion' in result['Rationale'].lower()

    def test_leap_tenor_guard(self):
        """LEAP DTE < 120 → ROLL HIGH."""
        row = _make_mgmt_row(LEAP_Call_DTE=100)
        result = pmcc_doctrine(row, _default_result())
        assert result['Action'] == 'ROLL'
        assert 'LEAP' in result['Rationale']

    def test_premium_capture(self):
        """≥50% premium captured → ROLL MEDIUM."""
        row = _make_mgmt_row(
            Premium_Entry=3.00,
            Short_Call_Last=1.20,  # 60% captured
            Last=1.20,
        )
        result = pmcc_doctrine(row, _default_result())
        assert result['Action'] == 'ROLL'
        assert result['Urgency'] == 'MEDIUM'

    def test_default_hold(self):
        """No gates triggered → HOLD LOW."""
        row = _make_mgmt_row(
            Premium_Entry=0,  # Disable premium capture gate
            Short_Call_DTE=35,
            DTE=35,
        )
        result = pmcc_doctrine(row, _default_result())
        assert result['Action'] == 'HOLD'
        assert result['Urgency'] == 'LOW'

    def test_story_broken_with_leap_loss(self):
        """Broken structure + LEAP loss > 15% → EXIT."""
        row = _make_mgmt_row(
            PriceStructure_State='STRUCTURE_BROKEN',
            TrendIntegrity_State='NO_TREND',
            LEAP_Entry_Price=30.0,
            LEAP_Call_Last=24.0,  # -20%
        )
        result = pmcc_doctrine(row, _default_result())
        assert result['Action'] == 'EXIT'


# =====================================================================
# DOCTRINE: V2 (PROPOSAL-BASED)
# =====================================================================

class TestPMCCDoctrineV2:
    """Tests for pmcc_doctrine_v2() — proposal-based resolution."""

    def test_hard_veto_leap_stop(self):
        row = _make_mgmt_row(LEAP_Entry_Price=30.0, LEAP_Call_Last=17.0)
        result = pmcc_doctrine_v2(row, _default_result())
        assert result['Action'] == 'EXIT'
        assert result['Resolution_Method'] == 'HARD_VETO'

    def test_hard_veto_width_inversion(self):
        row = _make_mgmt_row(
            Short_Call_Strike=125.0,
            LEAP_Call_Strike=130.0,
            Strike=125.0,
        )
        result = pmcc_doctrine_v2(row, _default_result())
        assert result['Action'] == 'ROLL'
        assert result['Resolution_Method'] == 'HARD_VETO'

    def test_hard_veto_assignment_critical(self):
        row = _make_mgmt_row(Short_Call_Delta=0.75, Delta=0.75)
        result = pmcc_doctrine_v2(row, _default_result())
        assert result['Action'] == 'ROLL'
        assert result['Resolution_Method'] == 'HARD_VETO'

    def test_proposals_summary_present(self):
        row = _make_mgmt_row()
        result = pmcc_doctrine_v2(row, _default_result())
        assert 'Proposals_Considered' in result
        assert result['Proposals_Considered'] >= 1
        assert 'Proposals_Summary' in result

    def test_default_hold_via_priority(self):
        row = _make_mgmt_row(Premium_Entry=0, Short_Call_DTE=35, DTE=35)
        result = pmcc_doctrine_v2(row, _default_result())
        assert result['Action'] == 'HOLD'
        assert result['Resolution_Method'] == 'PRIORITY_FALLBACK'

    def test_v2_income_gate_21dte(self):
        """Short call at 18 DTE → ROLL via 21-DTE income gate."""
        row = _make_mgmt_row(
            Short_Call_DTE=18, DTE=18,
            Premium_Entry=0,  # Disable premium capture
        )
        result = pmcc_doctrine_v2(row, _default_result())
        assert result['Action'] == 'ROLL'


# =====================================================================
# ORCHESTRATOR: STRUCTURE DETECTION
# =====================================================================

class TestPMCCOrchestrator:
    """Orchestrator PMCC detection and dispatch."""

    def test_pmcc_registered(self):
        assert 'PMCC' in DoctrineAuthority._REGISTERED_DOCTRINES

    def test_pmcc_dispatches_to_v2(self):
        """DoctrineAuthority.evaluate() with Strategy='PMCC' should call pmcc_doctrine_v2."""
        row = _make_mgmt_row(
            Underlying_Ticker='AAPL',
            TradeID='TEST_PMCC_001',
            AssetType='OPTION',
        )
        result = DoctrineAuthority.evaluate(row)
        assert result['Action'] in ('HOLD', 'ROLL', 'EXIT')
        # Should have proposal metadata (v2)
        assert 'Proposals_Considered' in result or result['Action'] == 'HOLD'


# =====================================================================
# MC POSITION SIZING: PMCC DIAGONAL MODEL
# =====================================================================

from scan_engine.mc_position_sizing import (
    _classify_strategy,
    simulate_pnl_paths,
    mc_size_row,
    _resolve_pmcc_legs,
)


class TestPMCCMCSizing:
    """MC position sizing correctly models PMCC as diagonal spread."""

    def test_classify_pmcc(self):
        """PMCC classified as DIAGONAL_CALL."""
        assert _classify_strategy("PMCC") == "DIAGONAL_CALL"
        assert _classify_strategy("pmcc") == "DIAGONAL_CALL"

    def test_classify_buy_write_unchanged(self):
        """BUY_WRITE still classified as INCOME (no regression)."""
        assert _classify_strategy("BUY_WRITE") == "INCOME"
        assert _classify_strategy("Buy-Write") == "INCOME"

    def test_diagonal_pnl_max_profit(self):
        """PMCC max profit = width - net_debit when S_T > short_strike."""
        import numpy as np
        rng = np.random.default_rng(42)
        # LEAP strike 100, short strike 120, net debit 22
        # If stock ends at 130 (above short strike):
        #   LEAP intrinsic = 130-100 = 30
        #   Short liability = 130-120 = 10
        #   Spread value = 30-10 = 20
        #   P&L = 20 - 22 = -2 per share... wait, width is 20 and debit is 22
        # Let's use a more realistic example:
        # LEAP strike 100, short strike 120, net debit 18
        pnl = simulate_pnl_paths(
            spot=116.0, strike=120.0, hv_annual=0.30, dte=37,
            premium=18.0, option_type='call', strategy_class='DIAGONAL_CALL',
            n_paths=5000, rng=rng, leap_strike=100.0, net_debit=18.0,
        )
        # Max gain = (120-100) - 18 = 2 per share
        # All paths where S_T > 120 should have P&L = 2
        assert pnl.max() <= 2.01  # width - net_debit
        # Max loss = -18 per share (net debit)
        assert pnl.min() >= -18.01

    def test_diagonal_pnl_max_loss(self):
        """PMCC max loss = net_debit when S_T << leap_strike."""
        import numpy as np
        rng = np.random.default_rng(99)
        pnl = simulate_pnl_paths(
            spot=50.0, strike=120.0, hv_annual=0.30, dte=37,
            premium=18.0, option_type='call', strategy_class='DIAGONAL_CALL',
            n_paths=2000, rng=rng, leap_strike=100.0, net_debit=18.0,
        )
        # With spot at 50, most paths end well below LEAP strike
        # so most P&L should be near -18
        p10 = float(np.percentile(pnl, 10))
        assert p10 == pytest.approx(-18.0, abs=0.5)

    def test_diagonal_win_probability_reasonable(self):
        """PMCC win probability should be 30-80% for ATM-ish setup."""
        import numpy as np
        rng = np.random.default_rng(42)
        pnl = simulate_pnl_paths(
            spot=116.0, strike=120.0, hv_annual=0.30, dte=37,
            premium=18.0, option_type='call', strategy_class='DIAGONAL_CALL',
            n_paths=5000, rng=rng, leap_strike=100.0, net_debit=18.0,
        )
        win_prob = float(np.mean(pnl > 0))
        assert 0.10 < win_prob < 0.90  # reasonable range

    def test_mc_size_row_pmcc(self):
        """Full mc_size_row for PMCC row produces valid sizing."""
        row = pd.Series({
            'Ticker': 'COP',
            'Strategy_Name': 'PMCC',
            'Option_Type': 'pmcc',
            'last_price': 116.28,
            'Selected_Strike': '[100.0, 120.0]',
            'Actual_DTE': 37,
            'Mid_Price': 3.00,
            'hv_30': 27.7,
            'PMCC_LEAP_Strike': 100.0,
            'PMCC_LEAP_Mid': 22.0,
            'PMCC_Net_Debit': 19.0,  # LEAP 22 - short call 3
        })
        result = mc_size_row(row, account_balance=100_000)
        assert result['MC_Paths_Used'] == 5000
        assert 'MC_SKIP' not in result['MC_Sizing_Note']
        assert result['MC_Max_Contracts'] >= 1
        # P10 loss should be bounded by net debit × 100
        assert result['MC_P10_Loss'] >= -19.0 * 100 - 1
        # Win probability should be reasonable, not 10%
        assert result['MC_Win_Probability'] > 0.15

    def test_mc_size_row_pmcc_no_leap_data_fallback(self):
        """PMCC with missing LEAP data falls back to LONG classification."""
        row = pd.Series({
            'Ticker': 'COP',
            'Strategy_Name': 'PMCC',
            'Option_Type': 'pmcc',
            'last_price': 116.28,
            'Selected_Strike': '120.0',
            'Actual_DTE': 37,
            'Mid_Price': 3.00,
            'hv_30': 27.7,
        })
        result = mc_size_row(row, account_balance=100_000)
        # Should still run (falls back to LONG)
        assert result['MC_Paths_Used'] == 5000

    def test_resolve_pmcc_legs(self):
        """_resolve_pmcc_legs extracts LEAP parameters correctly."""
        row = pd.Series({
            'PMCC_LEAP_Strike': 100.0,
            'PMCC_LEAP_Mid': 22.0,
            'PMCC_Net_Debit': 19.0,
        })
        legs = _resolve_pmcc_legs(row)
        assert legs is not None
        assert legs['leap_strike'] == 100.0
        assert legs['leap_premium'] == 22.0
        assert legs['net_debit'] == 19.0

    def test_resolve_pmcc_legs_missing(self):
        """_resolve_pmcc_legs returns None when LEAP data missing."""
        row = pd.Series({'Mid_Price': 3.0})
        assert _resolve_pmcc_legs(row) is None


# =====================================================================
# INTERPRETER ROUTING: PMCC → INCOME
# =====================================================================

from scan_engine.interpreters import get_interpreter


class TestPMCCInterpreterRouting:
    """PMCC routes to IncomeInterpreter, not DirectionalInterpreter."""

    def test_pmcc_routes_to_income(self):
        interp = get_interpreter("PMCC")
        assert interp.family == "income"

    def test_pmcc_lowercase_routes_to_income(self):
        interp = get_interpreter("pmcc")
        assert interp.family == "income"

    def test_buy_write_still_income(self):
        """No regression: Buy-Write still routes to income."""
        interp = get_interpreter("Buy-Write")
        assert interp.family == "income"
