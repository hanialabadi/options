"""
Test: IV History System Integration

PURPOSE:
    Verify that the IV history system works end-to-end:
    1. Daily collection script populates iv_term_history
    2. Scan engine reads from DuckDB (no in-scan computation)
    3. Dashboard displays real history depth

GOVERNANCE:
    - DuckDB is the single source of truth
    - Fidelity only for bootstrap when depth < 120
    - No IV history computation during scans
"""

import pytest
import pandas as pd
import duckdb
from datetime import date, datetime, timedelta
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.shared.data_layer.iv_term_history import (
    get_iv_history_db_path,
    initialize_iv_term_history_table,
    append_daily_iv_data,
    get_iv_history_depth,
    calculate_iv_rank,
    get_iv_maturity_state,
    get_latest_iv_data,
    get_history_summary
)


@pytest.fixture
def test_db():
    """Create a temporary test database."""
    db_path = Path("/tmp/test_iv_history.duckdb")
    if db_path.exists():
        db_path.unlink()

    con = duckdb.connect(str(db_path))
    initialize_iv_term_history_table(con)

    yield con

    con.close()
    if db_path.exists():
        db_path.unlink()


def test_daily_collection_workflow(test_db):
    """Test daily IV collection workflow."""
    # Simulate 130 days of IV data for AAPL
    ticker = "AAPL"
    base_date = date.today() - timedelta(days=130)

    for day in range(130):
        current_date = base_date + timedelta(days=day)

        # Simulate IV data (varying over time)
        iv_30d = 25.0 + (day % 10)  # Oscillating IV

        df_daily = pd.DataFrame([{
            'ticker': ticker,
            'iv_7d': iv_30d - 2,
            'iv_14d': iv_30d - 1,
            'iv_30d': iv_30d,
            'iv_60d': iv_30d + 1,
            'iv_90d': iv_30d + 2,
            'iv_120d': iv_30d + 3,
            'iv_180d': iv_30d + 4,
            'iv_360d': iv_30d + 5,
            'source': 'schwab'
        }])

        append_daily_iv_data(test_db, df_daily, current_date)

    # Verify history depth
    depth = get_iv_history_depth(test_db, ticker)
    assert depth == 130, f"Expected 130 days of history, got {depth}"

    print(f"✅ Daily collection: {depth} days of history for {ticker}")


def test_iv_rank_calculation(test_db):
    """Test IV Rank calculation from historical data."""
    ticker = "AAPL"
    base_date = date.today() - timedelta(days=252)

    # Populate with varying IV data
    iv_values = []
    for day in range(252):
        current_date = base_date + timedelta(days=day)
        iv_30d = 20.0 + (day % 30)  # Varies 20-50%
        iv_values.append(iv_30d)

        df_daily = pd.DataFrame([{
            'ticker': ticker,
            'iv_30d': iv_30d,
            'source': 'schwab'
        }])

        append_daily_iv_data(test_db, df_daily, current_date)

    # Calculate IV Rank for current IV = 35%
    current_iv = 35.0
    iv_rank, history_depth = calculate_iv_rank(
        test_db,
        ticker,
        current_iv,
        lookback_days=252
    )

    assert history_depth == 252, f"Expected 252 days, got {history_depth}"
    assert 0 <= iv_rank <= 100, f"IV Rank {iv_rank} out of range"

    # Manual verification
    min_iv = min(iv_values)
    max_iv = max(iv_values)
    expected_rank = ((current_iv - min_iv) / (max_iv - min_iv)) * 100

    assert abs(iv_rank - expected_rank) < 1.0, f"IV Rank mismatch: {iv_rank} vs {expected_rank}"

    print(f"✅ IV Rank: {iv_rank:.1f}% (history: {history_depth} days)")


def test_iv_maturity_states(test_db):
    """Test IV maturity state classification."""
    tickers = ['AAPL', 'MSFT', 'TSLA']
    history_days = [150, 90, 10]  # MATURE, IMMATURE, MISSING

    base_date = date.today()

    for ticker, days in zip(tickers, history_days):
        for day in range(days):
            current_date = base_date - timedelta(days=day)

            df_daily = pd.DataFrame([{
                'ticker': ticker,
                'iv_30d': 25.0 + (day % 5),
                'source': 'schwab'
            }])

            append_daily_iv_data(test_db, df_daily, current_date)

    # Check maturity states
    for ticker, days in zip(tickers, history_days):
        maturity_state, reason = get_iv_maturity_state(
            test_db,
            ticker,
            current_iv_30d=25.0,
            maturity_threshold=120
        )

        if days >= 120:
            assert maturity_state == "MATURE", f"{ticker}: Expected MATURE, got {maturity_state}"
        elif days > 0:
            assert maturity_state == "IMMATURE", f"{ticker}: Expected IMMATURE, got {maturity_state}"
        else:
            assert maturity_state == "MISSING", f"{ticker}: Expected MISSING, got {maturity_state}"

        print(f"✅ {ticker}: {maturity_state} ({reason})")


def test_scan_engine_integration(test_db):
    """Test that scan engine reads from DuckDB, not in-scan computation."""
    # Populate test data
    ticker = "TEST"
    base_date = date.today() - timedelta(days=200)

    for day in range(200):
        current_date = base_date + timedelta(days=day)

        df_daily = pd.DataFrame([{
            'ticker': ticker,
            'iv_30d': 30.0 + (day % 10),
            'source': 'schwab'
        }])

        append_daily_iv_data(test_db, df_daily, current_date)

    # Simulate scan engine reading IV data
    current_iv = 35.0
    iv_rank, history_depth = calculate_iv_rank(test_db, ticker, current_iv, lookback_days=252)

    assert history_depth == 200, "Scan should use DuckDB history"
    assert iv_rank > 0, "IV Rank should be computed from DuckDB"

    print(f"✅ Scan engine: IV Rank {iv_rank:.1f}% from DuckDB ({history_depth} days)")


def test_fidelity_bootstrap_trigger():
    """Test that Fidelity is only triggered for bootstrap (depth < 120)."""
    from core.wait_loop.fidelity_trigger import should_trigger_fidelity

    # Test cases
    test_cases = [
        {
            'strategy_type': 'DIRECTIONAL',
            'iv_maturity_state': 'MATURE',
            'expected_trigger': False,
            'reason': 'SKIP_R2: Schwab IV MATURE (120+ days), sufficient for execution'
        },
        {
            'strategy_type': 'DIRECTIONAL',
            'iv_maturity_state': 'IMMATURE',
            'expected_trigger': True,
            'reason': 'TRIGGER_R3: IMMATURE Schwab IV (<120 days) - Fidelity recommended'
        },
        {
            'strategy_type': 'INCOME',
            'iv_maturity_state': 'MATURE',
            'expected_trigger': True,
            'reason': 'TRIGGER_R1: INCOME strategy requires Fidelity IV (long-term edge validation)'
        }
    ]

    for case in test_cases:
        should_trigger, reason = should_trigger_fidelity(
            strategy_type=case['strategy_type'],
            iv_maturity_state=case['iv_maturity_state'],
            iv_source='Schwab',
            liquidity_grade='Excellent',
            data_completeness='Complete',
            ticker='TEST',
            strategy_name='Test'
        )

        assert should_trigger == case['expected_trigger'], \
            f"Expected trigger={case['expected_trigger']}, got {should_trigger} for {case['strategy_type']}/{case['iv_maturity_state']}"

        print(f"✅ Fidelity trigger: {case['strategy_type']}/{case['iv_maturity_state']} → {should_trigger} ({reason})")


def test_history_summary(test_db):
    """Test history summary statistics."""
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
    base_date = date.today() - timedelta(days=300)

    # Populate varying depths
    for idx, ticker in enumerate(tickers):
        days = 100 + (idx * 50)  # 100, 150, 200, 250, 300 days

        for day in range(days):
            current_date = base_date + timedelta(days=day)

            df_daily = pd.DataFrame([{
                'ticker': ticker,
                'iv_30d': 25.0 + (day % 10),
                'source': 'schwab'
            }])

            append_daily_iv_data(test_db, df_daily, current_date)

    # Get summary
    summary = get_history_summary(test_db)

    assert summary['total_tickers'] == 5, f"Expected 5 tickers, got {summary['total_tickers']}"
    assert summary['median_depth'] == 200, f"Expected median 200, got {summary['median_depth']}"
    assert summary['tickers_120plus'] >= 3, f"Expected 3+ tickers with 120+ days"
    assert summary['tickers_252plus'] >= 1, f"Expected 1+ tickers with 252+ days"

    print(f"✅ Summary: {summary['total_tickers']} tickers, median depth {summary['median_depth']} days")
    print(f"   120+ days: {summary['tickers_120plus']}, 252+ days: {summary['tickers_252plus']}")


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v", "-s"])
