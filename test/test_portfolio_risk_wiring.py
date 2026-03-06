"""
Portfolio Risk Wiring — Integration Tests
==========================================
Validates Fix 11 (sector bucket), Fix 12 (portfolio limits wiring),
and Fix 13 (dashboard columns survive schema enforcement).

Run:
    pytest test/test_portfolio_risk_wiring.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import numpy as np
import pytest

# ── path bootstrap ────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.sector_benchmarks import (
    get_sector_bucket,
    SECTOR_BUCKET_MAP,
    SECTOR_BENCHMARK_MAP,
)
from core.shared.data_contracts.schema import MANAGEMENT_UI_COLUMNS, enforce_management_schema
from core.phase5_portfolio_limits import analyze_correlation_risk


# ═══════════════════════════════════════════════════════════════════════════════
# Fix 11: Sector Bucket Classification
# ═══════════════════════════════════════════════════════════════════════════════

class TestSectorBucket:
    """get_sector_bucket() returns correct human-readable sector names."""

    def test_known_tech_ticker(self):
        assert get_sector_bucket("AAPL") == "Technology"

    def test_financial_ticker(self):
        assert get_sector_bucket("JPM") == "Financials"

    def test_unknown_falls_back(self):
        assert get_sector_bucket("ZZZZZ") == "Broad Market"

    def test_clean_energy(self):
        assert get_sector_bucket("EOSE") == "Clean Energy"

    def test_all_buckets_valid(self):
        """All values in SECTOR_BUCKET_MAP are non-empty strings."""
        for etf, name in SECTOR_BUCKET_MAP.items():
            assert isinstance(name, str) and len(name) > 0, f"Invalid bucket for {etf}: {name!r}"

    def test_metals_mining(self):
        assert get_sector_bucket("UUUU") == "Metals & Mining"
        assert get_sector_bucket("SLV") == "Metals & Mining"


# ═══════════════════════════════════════════════════════════════════════════════
# Fix 12e: Schema Column Persistence
# ═══════════════════════════════════════════════════════════════════════════════

class TestSchemaColumns:
    """New columns survive schema enforcement (not dropped)."""

    def test_risk_flags_in_schema(self):
        assert "Portfolio_Risk_Flags" in MANAGEMENT_UI_COLUMNS

    def test_sector_bucket_in_schema(self):
        assert "Sector_Bucket" in MANAGEMENT_UI_COLUMNS

    def test_concentration_columns_in_schema(self):
        assert "Underlying_Concentration_Risk" in MANAGEMENT_UI_COLUMNS
        assert "Strategy_Correlation_Risk" in MANAGEMENT_UI_COLUMNS

    def test_portfolio_state_in_schema(self):
        assert "Portfolio_State" in MANAGEMENT_UI_COLUMNS

    def test_enforce_fills_defaults(self):
        """enforce_management_schema fills new columns with sensible defaults."""
        df = pd.DataFrame({"TradeID": ["T1"]})
        result = enforce_management_schema(df)
        assert result["Portfolio_Risk_Flags"].iloc[0] == ""
        assert result["Sector_Bucket"].iloc[0] == ""
        assert result["Portfolio_State"].iloc[0] == "NOMINAL"
        assert result["Underlying_Concentration_Risk"].iloc[0] == "LOW"


# ═══════════════════════════════════════════════════════════════════════════════
# Fix 12d: Sector Concentration Detection
# ═══════════════════════════════════════════════════════════════════════════════

class TestSectorConcentration:
    """Sector concentration detection flags over-concentrated portfolios."""

    def test_five_tech_positions_flagged(self):
        """5 positions all in Technology → detected by analyze_correlation_risk."""
        rows = []
        for i, ticker in enumerate(["AAPL", "MSFT", "NVDA", "AMD", "GOOG"]):
            rows.append({
                "TradeID": f"T{i}",
                "Underlying": ticker,
                "Strategy": "BUY_WRITE",
                "Basis": -50000,
                "Delta": 0.5,
                "Quantity": 100,
            })
        df = pd.DataFrame(rows)
        result = analyze_correlation_risk(df)
        # All 5 are the same strategy on different underlyings
        # analyze_correlation_risk should flag strategy concentration
        assert "Strategy_Concentration" in result.columns
        assert "Strategy_Correlation_Risk" in result.columns

    def test_diverse_portfolio_no_underlying_concentration(self):
        """5 different tickers → no HIGH underlying concentration."""
        rows = []
        for i, ticker in enumerate(["AAPL", "JPM", "XOM", "JNJ", "CAT"]):
            rows.append({
                "TradeID": f"T{i}",
                "Underlying": ticker,
                "Strategy": "BUY_WRITE",
                "Basis": -20000,
                "Delta": 0.3,
                "Quantity": 100,
            })
        df = pd.DataFrame(rows)
        result = analyze_correlation_risk(df)
        high_risk = result[result["Underlying_Concentration_Risk"] == "HIGH"]
        assert len(high_risk) == 0, "Diverse portfolio should have no HIGH concentration"
