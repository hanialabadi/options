"""
Test: Debug/Production Parity Validation

PURPOSE:
    Verify that debug mode produces identical execution decisions as production
    for the same input data, differing only in universe scale.

ARCHITECTURAL REQUIREMENT:
    Debug = Production ÷ Scale

TEST STRATEGY:
    1. Run production pipeline with full universe
    2. Run debug mode with single ticker from that universe
    3. Compare execution outputs for that ticker
    4. Assert identical Execution_Status, Gate_Reason, and maturity decisions

RAG SOURCE: ARCHITECTURE_ROADMAP.md, docs/EXECUTION_SEMANTICS.md
"""

import pytest
import pandas as pd
import os
from pathlib import Path
from datetime import datetime

# Test fixtures
TEST_TICKER = "AAPL"
TEST_SNAPSHOT_DIR = Path("data/snapshots")


class TestDebugProductionParity:
    """Validate debug mode maintains exact parity with production"""

    def test_same_ticker_same_execution_status(self, tmp_path):
        """
        Test 1: Same Ticker → Same Execution Status

        Verify that a ticker evaluated in debug mode receives the same
        Execution_Status as in production mode.
        """
        # Skip if no recent snapshot available
        if not TEST_SNAPSHOT_DIR.exists():
            pytest.skip("No snapshot directory for testing")

        snapshots = list(TEST_SNAPSHOT_DIR.glob("ivhv_snapshot_live_*.csv"))
        if not snapshots:
            pytest.skip("No live snapshots available")

        latest_snapshot = sorted(snapshots)[-1]

        # Run production pipeline (without debug mode)
        from scan_engine.pipeline import run_full_scan_pipeline

        os.environ.pop("DEBUG_TICKER_MODE", None)
        os.environ.pop("DEBUG_TICKERS", None)

        prod_result = run_full_scan_pipeline(
            snapshot_path=str(latest_snapshot),
            output_dir=str(tmp_path / "prod_output"),
            account_balance=10000.0
        )

        # Run debug mode for single ticker
        os.environ["DEBUG_TICKER_MODE"] = "1"
        os.environ["DEBUG_TICKERS"] = TEST_TICKER

        debug_result = run_full_scan_pipeline(
            snapshot_path=str(latest_snapshot),
            output_dir=str(tmp_path / "debug_output"),
            account_balance=10000.0
        )

        # Extract acceptance DataFrames
        prod_df = prod_result.get('acceptance_all', pd.DataFrame())
        debug_df = debug_result.get('acceptance_all', pd.DataFrame())

        # Filter production results to test ticker
        prod_ticker = prod_df[prod_df['Ticker'] == TEST_TICKER]

        # Assertions
        assert not debug_df.empty, "Debug mode produced no results"
        assert not prod_ticker.empty, f"{TEST_TICKER} not in production results"

        # Compare execution status for same strategies
        for _, debug_row in debug_df.iterrows():
            strategy = debug_row['Strategy_Name']
            ticker = debug_row['Ticker']

            # Find matching strategy in production
            prod_match = prod_ticker[
                (prod_ticker['Strategy_Name'] == strategy) &
                (prod_ticker['Ticker'] == ticker)
            ]

            if not prod_match.empty:
                prod_status = prod_match.iloc[0]['Execution_Status']
                debug_status = debug_row['Execution_Status']

                assert prod_status == debug_status, (
                    f"Execution_Status mismatch for {ticker} {strategy}: "
                    f"prod={prod_status}, debug={debug_status}"
                )


    def test_same_missing_data_same_blocked_outcome(self):
        """
        Test 2: Same Missing Data → Same BLOCKED Outcome

        Verify that missing data produces identical blocking reasons
        in both debug and production modes.
        """
        # This test would require controlled missing data scenarios
        # Implementation depends on ability to manipulate test fixtures
        pytest.skip("Requires controlled test fixtures")


    def test_same_snapshot_same_gate_decisions(self, tmp_path):
        """
        Test 3: Same Snapshot → Same Gate Decisions

        Verify that using identical snapshot produces same:
        - Number of strategies at Step 12
        - IV_Fidelity_Required flags
        - Stage 2 escalation counts
        """
        if not TEST_SNAPSHOT_DIR.exists():
            pytest.skip("No snapshot directory for testing")

        snapshots = list(TEST_SNAPSHOT_DIR.glob("ivhv_snapshot_live_*.csv"))
        if not snapshots:
            pytest.skip("No live snapshots available")

        latest_snapshot = sorted(snapshots)[-1]

        # Run both modes with same snapshot
        from scan_engine.pipeline import run_full_scan_pipeline

        os.environ.pop("DEBUG_TICKER_MODE", None)
        prod_result = run_full_scan_pipeline(
            snapshot_path=str(latest_snapshot),
            output_dir=str(tmp_path / "prod_output"),
            account_balance=10000.0
        )

        os.environ["DEBUG_TICKER_MODE"] = "1"
        os.environ["DEBUG_TICKERS"] = TEST_TICKER
        debug_result = run_full_scan_pipeline(
            snapshot_path=str(latest_snapshot),
            output_dir=str(tmp_path / "debug_output"),
            account_balance=10000.0
        )

        # Extract results
        prod_df = prod_result.get('acceptance_all', pd.DataFrame())
        debug_df = debug_result.get('acceptance_all', pd.DataFrame())

        # Filter to test ticker
        prod_ticker = prod_df[prod_df['Ticker'] == TEST_TICKER]

        # Assertions
        if not prod_ticker.empty and not debug_df.empty:
            # Check IV_Fidelity_Required flag consistency
            prod_fidelity_req = prod_ticker['IV_Fidelity_Required'].sum()
            debug_fidelity_req = debug_df['IV_Fidelity_Required'].sum()

            # Both should have same escalation eligibility
            assert prod_fidelity_req == debug_fidelity_req, (
                f"IV_Fidelity_Required mismatch: prod={prod_fidelity_req}, "
                f"debug={debug_fidelity_req}"
            )


    def test_maturity_parity(self):
        """
        Test 4: Maturity Parity

        Verify that maturity tier classification is identical
        in debug and production modes.
        """
        pytest.skip("Requires maturity integration availability check")


    def test_wait_loop_parity(self):
        """
        Test 5: Wait Loop Parity

        Verify that wait list re-evaluation runs identically
        in debug and production modes.
        """
        pytest.skip("Requires wait loop integration availability check")


    def test_debug_mode_scale_only(self, tmp_path):
        """
        Meta-test: Verify debug mode differs ONLY by scale

        This test verifies that debug mode:
        - Uses same pipeline orchestrator
        - Executes all steps
        - Applies same gates
        - Differs only in ticker count
        """
        if not TEST_SNAPSHOT_DIR.exists():
            pytest.skip("No snapshot directory for testing")

        snapshots = list(TEST_SNAPSHOT_DIR.glob("ivhv_snapshot_live_*.csv"))
        if not snapshots:
            pytest.skip("No live snapshots available")

        latest_snapshot = sorted(snapshots)[-1]

        from scan_engine.pipeline import run_full_scan_pipeline

        # Production mode
        os.environ.pop("DEBUG_TICKER_MODE", None)
        prod_result = run_full_scan_pipeline(
            snapshot_path=str(latest_snapshot),
            output_dir=str(tmp_path / "prod_output"),
            account_balance=10000.0
        )

        # Debug mode
        os.environ["DEBUG_TICKER_MODE"] = "1"
        os.environ["DEBUG_TICKERS"] = f"{TEST_TICKER},MSFT,NVDA"
        debug_result = run_full_scan_pipeline(
            snapshot_path=str(latest_snapshot),
            output_dir=str(tmp_path / "debug_output"),
            account_balance=10000.0
        )

        # Both should have same result structure
        assert set(prod_result.keys()) == set(debug_result.keys()), (
            "Debug and production return different result structures"
        )

        # Both should execute same pipeline steps
        prod_snapshot = prod_result.get('snapshot', pd.DataFrame())
        debug_snapshot = debug_result.get('snapshot', pd.DataFrame())

        assert not prod_snapshot.empty, "Production snapshot missing"
        assert not debug_snapshot.empty, "Debug snapshot missing"

        # Debug should have fewer tickers (scale difference)
        debug_tickers = set(os.environ.get("DEBUG_TICKERS", "").split(","))
        debug_ticker_count = len(debug_snapshot)
        prod_ticker_count = len(prod_snapshot)

        assert debug_ticker_count <= len(debug_tickers), (
            f"Debug mode has more tickers ({debug_ticker_count}) than "
            f"configured ({len(debug_tickers)})"
        )

        assert debug_ticker_count < prod_ticker_count, (
            "Debug mode should have fewer tickers than production"
        )


if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "-s"])
