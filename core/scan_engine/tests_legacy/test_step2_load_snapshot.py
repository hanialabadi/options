import pandas as pd
import pytest
from core.scan_engine.step2_load_snapshot import load_ivhv_snapshot
import os

# Define the path to the dummy snapshot CSV
DUMMY_SNAPSHOT_PATH = "test/dummy_snapshot.csv"

@pytest.fixture(scope="module")
def dummy_snapshot_df():
    """Fixture to load the dummy snapshot for tests."""
    # Ensure the dummy snapshot file exists
    if not os.path.exists(DUMMY_SNAPSHOT_PATH):
        # This should ideally be created by a setup step or another tool
        # For now, we'll assume it's created by a previous step in the agent's workflow
        pytest.fail(f"Dummy snapshot file not found at {DUMMY_SNAPSHOT_PATH}")
    
    return load_ivhv_snapshot(DUMMY_SNAPSHOT_PATH)

def test_signal_type_and_regime_columns_exist(dummy_snapshot_df):
    """
    Test that 'Signal_Type' and 'Regime' columns exist in the DataFrame
    returned by load_ivhv_snapshot.
    """
    required_columns = ['Signal_Type', 'Regime']
    for col in required_columns:
        assert col in dummy_snapshot_df.columns, f"Column '{col}' missing from DataFrame."

def test_signal_type_and_regime_are_not_null(dummy_snapshot_df):
    """
    Test that 'Signal_Type' and 'Regime' columns do not contain null values
    after load_ivhv_snapshot.
    """
    required_columns = ['Signal_Type', 'Regime']
    for col in required_columns:
        assert dummy_snapshot_df[col].notna().all(), f"Column '{col}' contains null values."

def test_signal_type_and_regime_have_expected_values(dummy_snapshot_df):
    """
    Test that 'Signal_Type' and 'Regime' columns contain expected string values.
    """
    # Assuming 'Trend_State' from Murphy indicators maps to 'Signal_Type'
    # and 'Volatility_Regime' maps to 'Regime'.
    # The dummy data has no specific trend, so 'Neutral' or 'Bidirectional' is expected for Signal_Type.
    # For Regime, 'Unknown' is expected given the limited dummy data.
    
    # Check Signal_Type values
    assert all(st in ['Bullish', 'Bearish', 'Bidirectional'] for st in dummy_snapshot_df['Signal_Type'].unique()), \
        "Signal_Type contains unexpected values."
    
    # Check Regime values
    assert all(r in ['Low Vol', 'Compression', 'Expansion', 'High Vol', 'Unknown'] for r in dummy_snapshot_df['Regime'].unique()), \
        "Regime contains unexpected values."
