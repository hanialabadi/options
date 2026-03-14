"""
Integration tests: full evaluate_strategies_independently() with mixed strategies.
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pytest
import pandas as pd
import numpy as np

from scan_engine.step8_independent_evaluation import evaluate_strategies_independently


def _build_mixed_df():
    """Build a DataFrame with one row per strategy family."""
    rows = []

    # Directional
    rows.append({
        'Strategy_Name': 'Long Call', 'Contract_Status': 'OK',
        'Delta': 0.55, 'Gamma': 0.04, 'Vega': 0.25, 'Theta': -0.02,
        'Actual_DTE': 45, 'Signal_Type': 'Bullish', 'Regime': 'Compression',
        'Trend': 'Bullish', 'Volume_Trend': 'Rising', 'Ticker': 'AAPL',
        'Stock_Price': 150.0, 'RV_IV_Ratio': 1.05,
    })

    # Volatility
    rows.append({
        'Strategy_Name': 'Long Straddle', 'Contract_Status': 'OK',
        'Delta': 0.05, 'Gamma': 0.08, 'Vega': 0.60, 'Theta': -0.05,
        'Actual_DTE': 30, 'Signal_Type': 'Bullish', 'Regime': 'Compression',
        'Put_Call_Skew': 1.05, 'RV_IV_Ratio': 1.10, 'IV_Percentile': 35.0,
        'Ticker': 'MSFT', 'Stock_Price': 300.0, 'Volatility_Regime': 'Compression',
    })

    # Income
    rows.append({
        'Strategy_Name': 'Cash-Secured Put', 'Contract_Status': 'OK',
        'Delta': -0.30, 'Gamma': -0.03, 'Vega': 0.20, 'Theta': 0.05,
        'Actual_DTE': 35, 'Signal_Type': 'Bullish', 'Regime': 'Compression',
        'IVHV_gap_30D': 5.0, 'RV_IV_Ratio': 0.85, 'Probability_Of_Profit': 72.0,
        'Ticker': 'NVDA', 'Stock_Price': 700.0, 'Trend': 'Bullish',
        'Price_vs_SMA20': 2.0,
    })

    # Unknown strategy
    rows.append({
        'Strategy_Name': 'Iron Butterfly', 'Contract_Status': 'OK',
        'Delta': 0.01, 'Gamma': 0.02, 'Vega': 0.15, 'Theta': 0.03,
        'Actual_DTE': 30, 'Signal_Type': 'Bullish', 'Regime': 'Compression',
        'Ticker': 'SPY', 'Stock_Price': 500.0,
    })

    return pd.DataFrame(rows)


class TestIntegration:
    def test_row_count_preserved(self):
        df = _build_mixed_df()
        result = evaluate_strategies_independently(df)
        assert len(result) == len(df)

    def test_output_columns_present(self):
        df = _build_mixed_df()
        result = evaluate_strategies_independently(df)
        required_cols = [
            'Validation_Status', 'Theory_Compliance_Score', 'Evaluation_Notes',
            'Data_Completeness_Pct', 'Missing_Required_Data',
            'Strategy_Family', 'Strategy_Family_Rank', 'acceptance_status',
        ]
        for col in required_cols:
            assert col in result.columns, f"Missing column: {col}"

    def test_family_classification(self):
        df = _build_mixed_df()
        result = evaluate_strategies_independently(df)
        families = result['Strategy_Family'].tolist()
        assert families[0] == 'Directional'
        assert families[1] == 'Volatility'
        assert families[2] == 'Income'
        assert families[3] == 'Other'

    def test_valid_strategies_exist(self):
        df = _build_mixed_df()
        result = evaluate_strategies_independently(df)
        valid = result[result['Validation_Status'] == 'Valid']
        # With good inputs, at least directional and income should be Valid
        assert len(valid) >= 2

    def test_empty_df_returns_empty(self):
        df = pd.DataFrame()
        result = evaluate_strategies_independently(df)
        assert len(result) == 0

    def test_acceptance_status_column(self):
        """Dashboard backward compat: acceptance_status mirrors Validation_Status."""
        df = _build_mixed_df()
        result = evaluate_strategies_independently(df)
        assert (result['acceptance_status'] == result['Validation_Status']).all()

    def test_immature_iv_not_rejected(self):
        """IV_Maturity_State == IMMATURE should demote Reject → DATA_NOT_MATURE."""
        df = _build_mixed_df()
        # Force one row to have bad data + IMMATURE flag
        df.loc[0, 'Delta'] = np.nan
        df.loc[0, 'Gamma'] = np.nan
        df['IV_Maturity_State'] = 'IMMATURE'
        result = evaluate_strategies_independently(df)
        assert result.loc[0, 'Validation_Status'] == 'DATA_NOT_MATURE'

    def test_regression_known_inputs(self):
        """Known good directional row should get Valid with score >= 70."""
        df = pd.DataFrame([{
            'Strategy_Name': 'Long Call', 'Contract_Status': 'OK',
            'Delta': 0.60, 'Gamma': 0.05, 'Vega': 0.30, 'Theta': -0.02,
            'Actual_DTE': 30, 'Signal_Type': 'Bullish', 'Regime': 'Compression',
            'Trend': 'Bullish', 'Volume_Trend': 'Rising', 'Ticker': 'TEST',
            'Stock_Price': 100.0, 'Chart_Pattern': 'Cup and Handle',
            'Pattern_Confidence': 75.0,
        }])
        result = evaluate_strategies_independently(df)
        assert result.iloc[0]['Validation_Status'] == 'Valid'
        assert result.iloc[0]['Theory_Compliance_Score'] >= 70
