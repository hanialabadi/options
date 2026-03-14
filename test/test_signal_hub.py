"""
Signal Hub Integration Tests
=============================
Validates the Signal Hub architecture: scan computes → DuckDB persists → management reads → doctrine uses.

Tests:
  Schema + migration (3): migration adds columns, round-trip insert, partial insert
  Signal_Version (2): insert sets v2, management filters >= 2
  Computed_TS + staleness (3): auto-set on insert, management skips stale, fresh pass
  Indexing (1): indexes exist after initialization
  TTL pruning (2): prune removes old rows, retains recent
  Pipeline persistence (3): new columns in DuckDB, missing cols graceful, get_latest returns them
  Management consumption (5): enrich reads new cols, thesis_degradation with divergence, roll_timing with squeeze/OBV
  Doctrine annotation (6): short_put wheel + OBV, buy_write weekly + squeeze, long_option MACD + RS
  Entry freeze (3): new position freeze, existing not overwritten, missing cols graceful
  Graceful degradation (2): no scan data = identical behavior to today

Run:
    pytest test/test_signal_hub.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def db_con():
    """In-memory DuckDB connection for isolated tests."""
    con = duckdb.connect(":memory:")
    yield con
    con.close()


def _init_table(con):
    """Initialize technical_indicators table in the given connection."""
    from core.shared.data_layer.technical_data_repository import (
        initialize_technical_indicators_table,
    )
    initialize_technical_indicators_table(con=con)


def _make_v2_row(**overrides):
    """Create a DataFrame row with full v2 signal data."""
    defaults = {
        "Ticker": "AAPL",
        "Snapshot_TS": datetime.now(),
        "RSI_14": 55.0,
        "ADX_14": 28.0,
        "SMA_20": 180.0,
        "SMA_50": 175.0,
        "EMA_9": 182.0,
        "EMA_21": 179.0,
        "ATR_14": 3.5,
        "MACD": 1.2,
        "MACD_Signal": 0.8,
        "UpperBand_20": 190.0,
        "MiddleBand_20": 180.0,
        "LowerBand_20": 170.0,
        "SlowK_5_3": 65.0,
        "SlowD_5_3": 60.0,
        "IV_Rank_30D": 45.0,
        "PCS_Score_V2": 72.0,
        # v2 institutional signals
        "Market_Structure": "Uptrend",
        "OBV_Slope": 8.5,
        "Volume_Ratio": 1.2,
        "RSI_Divergence": "None",
        "MACD_Divergence": "None",
        "Weekly_Trend_Bias": "ALIGNED",
        "Keltner_Squeeze_On": False,
        "Keltner_Squeeze_Fired": False,
        "RS_vs_SPY_20d": 3.2,
        "Chart_Regime": "TRENDING",
        "BB_Position": 65.0,
        "ATR_Rank": 40.0,
        "MACD_Histogram": 0.4,
        "Trend_Slope": 0.02,
    }
    defaults.update(overrides)
    return pd.DataFrame([defaults])


def _make_v1_row(**overrides):
    """Create a DataFrame row with only v1 columns (no institutional signals)."""
    defaults = {
        "Ticker": "MSFT",
        "Snapshot_TS": datetime.now(),
        "RSI_14": 50.0,
        "ADX_14": 22.0,
        "SMA_20": 400.0,
        "SMA_50": 395.0,
        "EMA_9": 402.0,
        "EMA_21": 399.0,
        "ATR_14": 5.0,
        "MACD": 0.5,
        "MACD_Signal": 0.3,
        "UpperBand_20": 415.0,
        "MiddleBand_20": 400.0,
        "LowerBand_20": 385.0,
        "SlowK_5_3": 55.0,
        "SlowD_5_3": 50.0,
        "IV_Rank_30D": 35.0,
        "PCS_Score_V2": 60.0,
    }
    defaults.update(overrides)
    return pd.DataFrame([defaults])


# =============================================================================
# Schema + Migration (3 tests)
# =============================================================================

class TestSchemaMigration:
    def test_migration_adds_v2_columns(self, db_con):
        """Migration adds all 14 signal + 2 infrastructure columns."""
        # Create v1-style table first
        db_con.execute("""
            CREATE TABLE technical_indicators (
                Ticker VARCHAR,
                Snapshot_TS TIMESTAMP,
                RSI_14 DOUBLE,
                ADX_14 DOUBLE,
                PRIMARY KEY (Ticker, Snapshot_TS)
            )
        """)
        from core.shared.data_layer.technical_data_repository import _migrate_technical_indicators_v2
        _migrate_technical_indicators_v2(db_con)

        cols = {r[1] for r in db_con.execute("PRAGMA table_info('technical_indicators')").fetchall()}
        for expected in [
            "Market_Structure", "OBV_Slope", "Volume_Ratio", "RSI_Divergence",
            "MACD_Divergence", "Weekly_Trend_Bias", "Keltner_Squeeze_On",
            "Keltner_Squeeze_Fired", "RS_vs_SPY_20d", "Chart_Regime",
            "BB_Position", "ATR_Rank", "MACD_Histogram", "Trend_Slope",
            "Computed_TS", "Signal_Version",
        ]:
            assert expected in cols, f"Missing column: {expected}"

    def test_round_trip_insert_v2(self, db_con):
        """Full v2 row survives insert → read round trip."""
        _init_table(db_con)
        from core.shared.data_layer.technical_data_repository import (
            insert_technical_indicators,
            get_latest_technical_indicators,
        )
        df = _make_v2_row()
        insert_technical_indicators(df, con=db_con)
        result = get_latest_technical_indicators(["AAPL"], con=db_con)
        assert result is not None
        assert len(result) == 1
        assert result.iloc[0]["Market_Structure"] == "Uptrend"
        assert result.iloc[0]["OBV_Slope"] == pytest.approx(8.5)
        assert result.iloc[0]["Keltner_Squeeze_On"] == False

    def test_partial_insert_missing_cols(self, db_con):
        """Insert with subset of columns succeeds (missing cols stay NULL)."""
        _init_table(db_con)
        from core.shared.data_layer.technical_data_repository import (
            insert_technical_indicators,
            get_latest_technical_indicators,
        )
        df = pd.DataFrame([{
            "Ticker": "TSLA",
            "Snapshot_TS": datetime.now(),
            "RSI_14": 60.0,
            "Market_Structure": "Downtrend",
            # Most v2 columns intentionally missing
        }])
        insert_technical_indicators(df, con=db_con)
        result = get_latest_technical_indicators(["TSLA"], con=db_con)
        assert result is not None
        assert len(result) == 1
        assert result.iloc[0]["Market_Structure"] == "Downtrend"
        assert pd.isna(result.iloc[0]["OBV_Slope"])


# =============================================================================
# Signal_Version (2 tests)
# =============================================================================

class TestSignalVersion:
    def test_insert_sets_v2_by_default(self, db_con):
        """Inserted rows get Signal_Version = 2 via DB default."""
        _init_table(db_con)
        from core.shared.data_layer.technical_data_repository import insert_technical_indicators
        df = _make_v2_row()
        insert_technical_indicators(df, con=db_con)
        row = db_con.execute("SELECT Signal_Version FROM technical_indicators WHERE Ticker = 'AAPL'").fetchone()
        assert row[0] == 2

    def test_v1_rows_filtered_by_min_version(self, db_con):
        """Management can filter out v1 rows using min_signal_version."""
        _init_table(db_con)
        from core.shared.data_layer.technical_data_repository import (
            insert_technical_indicators,
            get_latest_technical_indicators,
        )
        # Insert a v2 row
        df_v2 = _make_v2_row(Ticker="GOOG")
        insert_technical_indicators(df_v2, con=db_con)

        # Manually mark it as v1
        db_con.execute("UPDATE technical_indicators SET Signal_Version = 1 WHERE Ticker = 'GOOG'")

        # Query with min_signal_version=2 should return empty
        result = get_latest_technical_indicators(["GOOG"], con=db_con, min_signal_version=2)
        assert result is not None
        assert len(result) == 0

        # Query without version filter should return the row
        result_all = get_latest_technical_indicators(["GOOG"], con=db_con)
        assert len(result_all) == 1


# =============================================================================
# Computed_TS + Staleness (3 tests)
# =============================================================================

class TestComputedTsStaleness:
    def test_computed_ts_auto_set(self, db_con):
        """Computed_TS is auto-populated on insert."""
        _init_table(db_con)
        from core.shared.data_layer.technical_data_repository import insert_technical_indicators
        df = _make_v2_row()
        insert_technical_indicators(df, con=db_con)
        row = db_con.execute("SELECT Computed_TS FROM technical_indicators WHERE Ticker = 'AAPL'").fetchone()
        assert row[0] is not None

    def test_stale_signals_filtered(self, db_con):
        """Signals older than max_age_hours are filtered out."""
        _init_table(db_con)
        from core.shared.data_layer.technical_data_repository import (
            insert_technical_indicators,
            get_latest_technical_indicators,
        )
        df = _make_v2_row(Ticker="META")
        insert_technical_indicators(df, con=db_con)
        # Manually set Computed_TS to 6 hours ago
        db_con.execute("""
            UPDATE technical_indicators
            SET Computed_TS = CURRENT_TIMESTAMP - INTERVAL '6' HOUR
            WHERE Ticker = 'META'
        """)
        result = get_latest_technical_indicators(["META"], con=db_con, max_age_hours=4)
        assert result is not None
        assert len(result) == 0

    def test_fresh_signals_pass(self, db_con):
        """Signals within max_age_hours are returned."""
        _init_table(db_con)
        from core.shared.data_layer.technical_data_repository import (
            insert_technical_indicators,
            get_latest_technical_indicators,
        )
        df = _make_v2_row(Ticker="AMZN")
        insert_technical_indicators(df, con=db_con)
        result = get_latest_technical_indicators(["AMZN"], con=db_con, max_age_hours=4)
        assert result is not None
        assert len(result) == 1


# =============================================================================
# Indexing (1 test)
# =============================================================================

class TestIndexing:
    def test_indexes_created(self, db_con):
        """Both indexes exist after initialization."""
        _init_table(db_con)
        # DuckDB: query pg_indexes or duckdb_indexes
        indexes = db_con.execute("""
            SELECT index_name FROM duckdb_indexes()
            WHERE table_name = 'technical_indicators'
        """).fetchall()
        index_names = {r[0] for r in indexes}
        assert "idx_tech_ticker_ts" in index_names
        assert "idx_tech_computed_ts" in index_names


# =============================================================================
# TTL Pruning (2 tests)
# =============================================================================

class TestTtlPruning:
    def test_prune_removes_old_rows(self, db_con):
        """Rows older than max_age_days are deleted."""
        _init_table(db_con)
        from core.shared.data_layer.technical_data_repository import (
            insert_technical_indicators,
            prune_stale_signals,
        )
        # Insert a row and age it
        df = _make_v2_row(Ticker="OLD")
        insert_technical_indicators(df, con=db_con)
        db_con.execute("""
            UPDATE technical_indicators
            SET Snapshot_TS = CURRENT_TIMESTAMP - INTERVAL '45' DAY
            WHERE Ticker = 'OLD'
        """)
        prune_stale_signals(max_age_days=30, con=db_con)
        count = db_con.execute("SELECT COUNT(*) FROM technical_indicators WHERE Ticker = 'OLD'").fetchone()[0]
        assert count == 0

    def test_prune_retains_recent(self, db_con):
        """Recent rows are not pruned."""
        _init_table(db_con)
        from core.shared.data_layer.technical_data_repository import (
            insert_technical_indicators,
            prune_stale_signals,
        )
        df = _make_v2_row(Ticker="FRESH")
        insert_technical_indicators(df, con=db_con)
        prune_stale_signals(max_age_days=30, con=db_con)
        count = db_con.execute("SELECT COUNT(*) FROM technical_indicators WHERE Ticker = 'FRESH'").fetchone()[0]
        assert count == 1


# =============================================================================
# Pipeline Persistence (3 tests)
# =============================================================================

class TestPipelinePersistence:
    def test_new_columns_in_schema(self, db_con):
        """Full v2 schema has 34 columns (16 v1 + 2 keys + 14 signals + 2 infra)."""
        _init_table(db_con)
        cols = db_con.execute("PRAGMA table_info('technical_indicators')").fetchall()
        col_names = {r[1] for r in cols}
        # 18 original + 14 signal + 2 infrastructure = 34
        assert len(col_names) >= 34, f"Expected >= 34 columns, got {len(col_names)}: {col_names}"

    def test_missing_df_cols_handled(self, db_con):
        """DataFrame with extra columns not in table is handled gracefully."""
        _init_table(db_con)
        from core.shared.data_layer.technical_data_repository import insert_technical_indicators
        df = pd.DataFrame([{
            "Ticker": "NVDA",
            "Snapshot_TS": datetime.now(),
            "RSI_14": 70.0,
            "Extra_Column_Not_In_Schema": 999,
        }])
        # Should NOT raise — extra column silently ignored
        insert_technical_indicators(df, con=db_con)
        count = db_con.execute("SELECT COUNT(*) FROM technical_indicators WHERE Ticker = 'NVDA'").fetchone()[0]
        assert count == 1

    def test_get_latest_returns_v2_cols(self, db_con):
        """get_latest_technical_indicators returns v2 columns."""
        _init_table(db_con)
        from core.shared.data_layer.technical_data_repository import (
            insert_technical_indicators,
            get_latest_technical_indicators,
        )
        df = _make_v2_row(Ticker="COIN", Chart_Regime="CHOPPY", BB_Position=25.0)
        insert_technical_indicators(df, con=db_con)
        result = get_latest_technical_indicators(["COIN"], con=db_con)
        assert result is not None
        assert "Chart_Regime" in result.columns
        assert result.iloc[0]["Chart_Regime"] == "CHOPPY"
        assert result.iloc[0]["BB_Position"] == pytest.approx(25.0)


# =============================================================================
# Management Consumption (5 tests)
# =============================================================================

class TestManagementConsumption:
    def _make_management_row(self, **overrides):
        """Minimal row for management doctrine testing."""
        defaults = {
            "Underlying_Ticker": "AAPL",
            "Ticker": "AAPL260320C260",
            "Strategy": "BUY_WRITE",
            "Quantity": -1,
            "DTE": 30,
            "Delta": -0.35,
            "Call/Put": "Call",
            "AssetType": "OPTION",
            "Total_GL_Decimal": -0.05,
            "Basis": 1500.0,
            "Last": 5.0,
            "UL Last": 260.0,
            "Snapshot_TS": pd.Timestamp.now(),
            "Entry_Snapshot_TS": pd.Timestamp.now() - pd.Timedelta(days=10),
            "Entry_Chart_State_TrendIntegrity": "STRONG_TREND",
            "TrendIntegrity_State": "STRONG_TREND",
            "Entry_Chart_State_VolatilityState": "COMPRESSED",
            "VolatilityState_State": "COMPRESSED",
            "Entry_Chart_State_PriceStructure": "STRUCTURAL_UP",
            "PriceStructure_State": "STRUCTURAL_UP",
            "roc_20": 5.0,
            "adx_14": 25.0,
            "rsi_14": 55.0,
        }
        defaults.update(overrides)
        return pd.Series(defaults)

    def test_thesis_degradation_rsi_divergence(self):
        """RSI bearish divergence on short-vol position triggers degradation."""
        from core.management.cycle3.doctrine.helpers import check_thesis_degradation
        row = self._make_management_row(
            RSI_Divergence="Bearish_Divergence",
            Strategy="BUY_WRITE",
        )
        result = check_thesis_degradation(row)
        assert result is not None
        assert "RSI bearish divergence" in result["text"]

    def test_thesis_degradation_rsi_div_skips_long_vol(self):
        """RSI bearish divergence does NOT trigger on long-vol positions."""
        from core.management.cycle3.doctrine.helpers import check_thesis_degradation
        row = self._make_management_row(
            RSI_Divergence="Bearish_Divergence",
            Strategy="LONG_CALL",
            Quantity=1,
        )
        result = check_thesis_degradation(row)
        # Should be None or not contain RSI divergence
        if result is not None:
            assert "RSI bearish divergence" not in result["text"]

    def test_thesis_degradation_weekly_conflict(self):
        """Weekly trend CONFLICTING triggers degradation."""
        from core.management.cycle3.doctrine.helpers import check_thesis_degradation
        row = self._make_management_row(
            Weekly_Trend_Bias="CONFLICTING",
        )
        result = check_thesis_degradation(row)
        assert result is not None
        assert "weekly trend conflicts" in result["text"]

    def test_roll_timing_squeeze_breakout(self):
        """Keltner squeeze fired + positive ROC triggers BREAKOUT_UP."""
        from core.management.cycle3.doctrine.helpers import classify_roll_timing
        row = self._make_management_row(
            Keltner_Squeeze_Fired=True,
            roc_5=3.0,
            adx_14=20.0,  # > ADX_VERY_WEAK_TREND (18)
            choppiness_index=50,
            kaufman_efficiency_ratio=0.5,
            bb_width_z=0,
            roc_10=2.0,
            RangeEfficiency_State="EFFICIENT",
            TrendIntegrity_State="WEAK_TREND",
            MomentumVelocity_State="TRENDING",
            DirectionalBalance_State="NEUTRAL",
            CompressionMaturity_State="RELEASING",
        )
        result = classify_roll_timing(row)
        assert result["timing"] == "BREAKOUT_UP"

    def test_roll_timing_obv_flat_choppy(self):
        """OBV flat + choppy conditions strengthens CHOPPY classification."""
        from core.management.cycle3.doctrine.helpers import classify_roll_timing
        row = self._make_management_row(
            OBV_Slope=1.0,  # < OBV_SLOPE_FLAT_THRESHOLD (3.0)
            choppiness_index=55,  # > CHOPPINESS_BASE (50)
            adx_14=22.0,  # < ADX_TRENDING (25)
            roc_5=0.5,  # < ROC_MOMENTUM_THRESHOLD (2.0)
            kaufman_efficiency_ratio=0.3,
            bb_width_z=0,
            roc_10=0.3,
            RangeEfficiency_State="NOISY",
            TrendIntegrity_State="NO_TREND",
            MomentumVelocity_State="DECELERATING",
            DirectionalBalance_State="NEUTRAL",
            CompressionMaturity_State="MATURE",
        )
        result = classify_roll_timing(row)
        assert result["timing"] == "CHOPPY"


# =============================================================================
# Doctrine Annotation (6 tests)
# =============================================================================

class TestDoctrineAnnotation:
    def _base_short_put_row(self, **overrides):
        """Minimal row for short_put_doctrine testing."""
        defaults = {
            "Underlying_Ticker": "AAPL",
            "Ticker": "AAPL260320P240",
            "Strategy": "SHORT_PUT",
            "Quantity": -1,
            "DTE": 30,
            "Delta": -0.30,
            "Call/Put": "Put",
            "AssetType": "OPTION",
            "Total_GL_Decimal": 0.10,
            "Basis": 500.0,
            "Last": 3.50,
            "UL Last": 260.0,
            "Strike": 240.0,
            "IV_30D": 0.30,
            "IV_Entry": 0.28,
            "HV_20D": 0.25,
            "Snapshot_TS": pd.Timestamp.now(),
            "Entry_Snapshot_TS": pd.Timestamp.now() - pd.Timedelta(days=10),
            "Entry_Chart_State_TrendIntegrity": "STRONG_TREND",
            "TrendIntegrity_State": "STRONG_TREND",
            "PriceStructure_State": "STRUCTURAL_UP",
            "Market_Structure": "Uptrend",
            "OBV_Slope": 5.0,
            "IV_Source": "Schwab",
            "MC_Assign_P_Expiry": 0.10,
            "Delta_Utilization_Pct": 5.0,
            "Effective_Basis": 237.0,
            "Extrinsic_Ratio": 0.80,
            "roc_5": 1.0,
            "adx_14": 25.0,
            "rsi_14": 55.0,
        }
        defaults.update(overrides)
        return pd.Series(defaults)

    def test_short_put_wheel_downtrend_fails(self):
        """Market_Structure = Downtrend causes wheel chart check to fail."""
        from core.management.cycle3.doctrine.strategies.short_put import short_put_doctrine
        row = self._base_short_put_row(Market_Structure="Downtrend")
        result = {"Action": "HOLD", "Urgency": "LOW", "Rationale": ""}
        result = short_put_doctrine(row, result)
        assert result.get("Wheel_Chart_Ok") == False
        assert "downtrend" in result.get("Wheel_Note", "").lower()

    def test_short_put_obv_distribution_warning(self):
        """OBV_Slope < -10 appends distribution warning to Wheel_Note."""
        from core.management.cycle3.doctrine.strategies.short_put import short_put_doctrine
        row = self._base_short_put_row(OBV_Slope=-15.0)
        result = {"Action": "HOLD", "Urgency": "LOW", "Rationale": ""}
        result = short_put_doctrine(row, result)
        assert "OBV distributing" in result.get("Wheel_Note", "")

    def _base_buy_write_row(self, **overrides):
        """Row that reaches gate 7b (annotations) without early-exit."""
        defaults = {
            "Underlying_Ticker": "AAPL",
            "Ticker": "AAPL260320C260",
            "Strategy": "BUY_WRITE",
            "Quantity": -1,
            "DTE": 30,
            "Short_Call_DTE": 30,
            "Delta": -0.35,
            "Short_Call_Delta": 0.35,
            "Call/Put": "Call",
            "AssetType": "OPTION",
            "Days_In_Trade": 10,
            # Financial: spot above cost → no hard stop; strike far OTM → no assignment
            "UL Last": 260.0,
            "Basis": 250.0,
            "Net_Cost_Basis_Per_Share": 245.0,
            "Cumulative_Premium_Collected": 5.0,
            "Strike": 290.0,
            "Short_Call_Strike": 290.0,
            "Underlying_Price_Entry": 250.0,
            "Premium_Entry": 2.50,
            "Total_GL_Decimal": 0.05,
            "Last": 1.50,
            "Time Val": 1.40,
            "Extrinsic_Ratio": 0.95,
            # Greeks
            "Theta": -0.05,
            "Gamma": 0.02,
            "IV_Now": 0.30,
            "IV_30D": 0.30,
            "IV_Entry": 0.28,
            "IV_30D_Entry": 0.28,
            # Timestamps
            "Snapshot_TS": pd.Timestamp.now(),
            "Entry_Snapshot_TS": pd.Timestamp.now() - pd.Timedelta(days=10),
            # Chart states
            "Entry_Chart_State_TrendIntegrity": "STRONG_TREND",
            "TrendIntegrity_State": "STRONG_TREND",
            "Entry_Chart_State_VolatilityState": "COMPRESSED",
            "VolatilityState_State": "COMPRESSED",
            "Entry_Chart_State_PriceStructure": "STRUCTURAL_UP",
            "PriceStructure_State": "STRUCTURAL_UP",
            # Technicals
            "roc_5": 1.0,
            "roc_20": 3.0,
            "adx_14": 25.0,
            "rsi_14": 55.0,
            # Signal Hub defaults
            "Market_Structure": "Uptrend",
            "OBV_Slope": 5.0,
            "Weekly_Trend_Bias": "ALIGNED",
            "Keltner_Squeeze_On": False,
        }
        defaults.update(overrides)
        return pd.Series(defaults)

    def test_buy_write_weekly_conflict(self):
        """Weekly_Trend_Bias = CONFLICTING annotates buy_write rationale."""
        from core.management.cycle3.doctrine.strategies.buy_write import buy_write_doctrine
        row = self._base_buy_write_row(Weekly_Trend_Bias="CONFLICTING")
        result = {"Action": "HOLD", "Urgency": "LOW", "Rationale": ""}
        ret = buy_write_doctrine(row, result)
        # v1 fire_gate returns (bool, dict); unpack if tuple
        if isinstance(ret, tuple):
            _, ret = ret
        assert "Weekly trend CONFLICTS" in ret.get("Rationale", "")

    def test_buy_write_squeeze_annotation(self):
        """Keltner_Squeeze_On = True annotates premium compression."""
        from core.management.cycle3.doctrine.strategies.buy_write import buy_write_doctrine
        row = self._base_buy_write_row(Keltner_Squeeze_On=True)
        result = {"Action": "HOLD", "Urgency": "LOW", "Rationale": ""}
        ret = buy_write_doctrine(row, result)
        if isinstance(ret, tuple):
            _, ret = ret
        assert "Keltner squeeze ON" in ret.get("Rationale", "")

    def test_long_option_macd_bearish_divergence(self):
        """MACD bearish divergence on long call annotates momentum warning."""
        from core.management.cycle3.doctrine.strategies.long_option import long_option_doctrine
        row = self._base_short_put_row(
            Strategy="LONG_CALL",
            Call__Put="Call",
            Quantity=1,
            DTE=60,
            Delta=0.50,
            MACD_Divergence="Bearish_Divergence",
            RS_vs_SPY_20d=2.0,
            Total_GL_Decimal=0.05,
            Theta=-0.10,
            Gamma=0.03,
            Vega=0.15,
            IV_30D=0.25,
            HV_20D=0.22,
            IV_30D_Entry=0.23,
            HV_20D_Entry=0.20,
            Entry_Chart_State_PriceStructure="STRUCTURAL_UP",
            PriceStructure_State="STRUCTURAL_UP",
            roc_20=3.0,
        )
        row["Call/Put"] = "Call"
        result = {"Action": "HOLD", "Urgency": "LOW", "Rationale": ""}
        result = long_option_doctrine(row, result)
        assert "MACD bearish divergence" in result.get("Rationale", "")

    def test_long_option_rs_spy_context(self):
        """RS_vs_SPY_20d > 5 annotates relative strength context."""
        from core.management.cycle3.doctrine.strategies.long_option import long_option_doctrine
        row = self._base_short_put_row(
            Strategy="LONG_CALL",
            Call__Put="Call",
            Quantity=1,
            DTE=60,
            Delta=0.50,
            MACD_Divergence="None",
            RS_vs_SPY_20d=8.0,
            Total_GL_Decimal=0.05,
            Theta=-0.10,
            Gamma=0.03,
            Vega=0.15,
            IV_30D=0.25,
            HV_20D=0.22,
            IV_30D_Entry=0.23,
            HV_20D_Entry=0.20,
            Entry_Chart_State_PriceStructure="STRUCTURAL_UP",
            PriceStructure_State="STRUCTURAL_UP",
            roc_20=3.0,
        )
        row["Call/Put"] = "Call"
        result = {"Action": "HOLD", "Urgency": "LOW", "Rationale": ""}
        result = long_option_doctrine(row, result)
        assert "RS vs SPY" in result.get("Rationale", "")


# =============================================================================
# Entry Freeze (3 tests)
# =============================================================================

class TestEntryFreeze:
    def _make_freeze_df(self, **overrides):
        """DataFrame with enough columns for freeze_entry_data."""
        defaults = {
            "TradeID": "T001",
            "LegID": "L001",
            "Underlying_Ticker": "AAPL",
            "Ticker": "AAPL260320C260",
            "AssetType": "OPTION",
            "Call/Put": "Call",
            "Quantity": -1,
            "Strategy": "BUY_WRITE",
            "DTE": 30,
            "Delta": -0.35,
            "Gamma": 0.02,
            "Vega": 0.15,
            "Theta": -0.05,
            "Rho": 0.01,
            "IV": 0.28,
            "Last": 5.0,
            "UL Last": 260.0,
            "Basis": 1500.0,
            "Snapshot_TS": pd.Timestamp.now(),
            "Expiration": "2026-04-17",
            # Scan signals
            "Market_Structure": "Uptrend",
            "Weekly_Trend_Bias": "ALIGNED",
            "Keltner_Squeeze_On": True,
            "OBV_Slope": 12.5,
            "RSI_Divergence": "None",
            "RS_vs_SPY_20d": 4.2,
            # Chart states
            "PriceStructure_State": "STRUCTURAL_UP",
            "TrendIntegrity_State": "STRONG_TREND",
            "VolatilityState_State": "COMPRESSED",
            "CompressionMaturity_State": "MATURE",
        }
        defaults.update(overrides)
        return pd.DataFrame([defaults])

    def test_new_position_freezes_scan_signals(self):
        """New position gets 6 scan signals frozen at entry."""
        from core.management.cycle1.snapshot.freeze import freeze_entry_data
        df = self._make_freeze_df()
        result = freeze_entry_data(df, new_trade_ids=["T001"], new_leg_ids={"L001"})

        assert result.iloc[0]["Entry_Market_Structure"] == "Uptrend"
        assert result.iloc[0]["Entry_Weekly_Trend_Bias"] == "ALIGNED"
        assert result.iloc[0]["Entry_Keltner_Squeeze_On"] == True
        assert result.iloc[0]["Entry_OBV_Slope"] == pytest.approx(12.5)
        assert result.iloc[0]["Entry_RSI_Divergence"] == "None"
        assert result.iloc[0]["Entry_RS_vs_SPY_20d"] == pytest.approx(4.2)

    def test_existing_position_not_overwritten(self):
        """Pre-existing frozen values are NOT overwritten."""
        from core.management.cycle1.snapshot.freeze import freeze_entry_data
        df = self._make_freeze_df()
        # First freeze
        df = freeze_entry_data(df, new_trade_ids=["T001"], new_leg_ids={"L001"})
        # Simulate changed live signals
        df["Market_Structure"] = "Downtrend"
        df["OBV_Slope"] = -15.0
        # Second freeze (same trade_id but entry columns already populated)
        df = freeze_entry_data(df, new_trade_ids=["T001"], new_leg_ids={"L001"})
        # Should retain original frozen values
        assert df.iloc[0]["Entry_Market_Structure"] == "Uptrend"
        assert df.iloc[0]["Entry_OBV_Slope"] == pytest.approx(12.5)

    def test_missing_scan_signals_graceful(self):
        """Missing scan signal columns don't crash freeze."""
        from core.management.cycle1.snapshot.freeze import freeze_entry_data
        df = self._make_freeze_df()
        # Remove scan signals
        for col in ["Market_Structure", "Weekly_Trend_Bias", "Keltner_Squeeze_On",
                     "OBV_Slope", "RSI_Divergence", "RS_vs_SPY_20d"]:
            if col in df.columns:
                df = df.drop(columns=[col])
        result = freeze_entry_data(df, new_trade_ids=["T001"], new_leg_ids={"L001"})
        # Should not crash; entry cols should be NaN/None
        assert "Entry_Market_Structure" in result.columns
        assert pd.isna(result.iloc[0]["Entry_Market_Structure"])


# =============================================================================
# Graceful Degradation (2 tests)
# =============================================================================

class TestGracefulDegradation:
    def test_no_scan_data_thesis_same_as_before(self):
        """Without Signal Hub columns, check_thesis_degradation behaves identically."""
        from core.management.cycle3.doctrine.helpers import check_thesis_degradation
        row = pd.Series({
            "Strategy": "BUY_WRITE",
            "Quantity": -1,
            "Snapshot_TS": pd.Timestamp.now(),
            "Entry_Snapshot_TS": pd.Timestamp.now() - pd.Timedelta(days=10),
            "Entry_Chart_State_TrendIntegrity": "STRONG_TREND",
            "TrendIntegrity_State": "STRONG_TREND",
            "Entry_Chart_State_VolatilityState": "COMPRESSED",
            "VolatilityState_State": "COMPRESSED",
            "Entry_Chart_State_PriceStructure": "STRUCTURAL_UP",
            "PriceStructure_State": "STRUCTURAL_UP",
            "roc_20": 5.0,
            "adx_14": 25.0,
            "rsi_14": 55.0,
            # NO Signal Hub columns — should not crash
        })
        result = check_thesis_degradation(row)
        # No degradation — all states unchanged
        assert result is None

    def test_no_scan_data_roll_timing_neutral(self):
        """Without Signal Hub columns, classify_roll_timing falls back gracefully."""
        from core.management.cycle3.doctrine.helpers import classify_roll_timing
        row = pd.Series({
            "choppiness_index": 50,
            "kaufman_efficiency_ratio": 0.5,
            "adx_14": 25.0,
            "bb_width_z": 0.0,
            "roc_5": 0.5,
            "roc_10": 0.3,
            "RangeEfficiency_State": "EFFICIENT",
            "TrendIntegrity_State": "WEAK_TREND",
            "MomentumVelocity_State": "TRENDING",
            "DirectionalBalance_State": "NEUTRAL",
            "CompressionMaturity_State": "MATURE",
            # NO Signal Hub columns — should default to False/0
        })
        result = classify_roll_timing(row)
        assert result["timing"] in ("NEUTRAL", "CHOPPY", "RELEASING")
