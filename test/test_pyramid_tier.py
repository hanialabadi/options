"""
Tests for Pyramid Tier Tracker and Winner Lifecycle state machine.

Covers:
  - DuckDB fallback (no history → tier 0, THESIS_UNPROVEN)
  - Tier counting from management_recommendations
  - Lifecycle derivation from tier + gain + conviction + momentum
  - Tier-aware sizing in SCALE_UP gate
  - Schema inclusion
"""

import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from core.management.cycle2.pyramid_tier_tracker import (
    compute_pyramid_tier,
    _derive_lifecycle,
    LIFECYCLE_UNPROVEN,
    LIFECYCLE_CONFIRMED,
    LIFECYCLE_BUILDING,
    LIFECYCLE_FULL,
    LIFECYCLE_EXHAUSTING,
    _PYRAMID_MAX_TIER,
)


# ── Lifecycle derivation tests ──────────────────────────────────────────────

class TestDeriveLifecycle:
    """Unit tests for the deterministic lifecycle state machine."""

    def test_no_history_tier0_unproven(self):
        """No history → tier 0, THESIS_UNPROVEN."""
        assert _derive_lifecycle(0, 0.10, "STABLE", "TRENDING") == LIFECYCLE_UNPROVEN

    def test_gain_below_threshold_unproven(self):
        """gain < 25% → THESIS_UNPROVEN regardless of tier."""
        assert _derive_lifecycle(0, 0.20, "STRENGTHENING", "TRENDING") == LIFECYCLE_UNPROVEN
        assert _derive_lifecycle(1, 0.20, "STRENGTHENING", "TRENDING") == LIFECYCLE_UNPROVEN

    def test_gain_confirmed_tier0(self):
        """gain ≥ 25%, stable conviction, tier 0 → THESIS_CONFIRMED."""
        assert _derive_lifecycle(0, 0.30, "STABLE", "TRENDING") == LIFECYCLE_CONFIRMED

    def test_gain_confirmed_strengthening(self):
        """gain ≥ 25%, strengthening conviction, tier 0 → THESIS_CONFIRMED."""
        assert _derive_lifecycle(0, 0.50, "STRENGTHENING", "TRENDING") == LIFECYCLE_CONFIRMED

    def test_tier1_favorable_building(self):
        """tier 1, favorable conditions → CONVICTION_BUILDING."""
        assert _derive_lifecycle(1, 0.40, "STABLE", "TRENDING") == LIFECYCLE_BUILDING

    def test_tier1_strengthening_building(self):
        """tier 1, strengthening conviction → CONVICTION_BUILDING."""
        assert _derive_lifecycle(1, 0.35, "STRENGTHENING", "ACCELERATING") == LIFECYCLE_BUILDING

    def test_tier2_full_position(self):
        """tier ≥ 2 → FULL_POSITION."""
        assert _derive_lifecycle(2, 0.50, "STABLE", "TRENDING") == LIFECYCLE_FULL

    def test_tier3_full_position(self):
        """tier 3 (max) → FULL_POSITION."""
        assert _derive_lifecycle(3, 0.60, "STRENGTHENING", "TRENDING") == LIFECYCLE_FULL

    def test_tier2_reversing_conviction_exhausting(self):
        """tier ≥ 2 + REVERSING conviction → THESIS_EXHAUSTING."""
        assert _derive_lifecycle(2, 0.50, "REVERSING", "TRENDING") == LIFECYCLE_EXHAUSTING

    def test_tier2_late_cycle_exhausting(self):
        """tier ≥ 2 + LATE_CYCLE momentum → THESIS_EXHAUSTING."""
        assert _derive_lifecycle(2, 0.50, "STABLE", "LATE_CYCLE") == LIFECYCLE_EXHAUSTING

    def test_tier1_reversing_exhausting(self):
        """tier 1 + REVERSING momentum → THESIS_EXHAUSTING."""
        assert _derive_lifecycle(1, 0.40, "STABLE", "REVERSING") == LIFECYCLE_EXHAUSTING

    def test_weakening_conviction_unproven(self):
        """WEAKENING conviction at tier 0/1 → THESIS_UNPROVEN."""
        assert _derive_lifecycle(0, 0.30, "WEAKENING", "TRENDING") == LIFECYCLE_UNPROVEN
        assert _derive_lifecycle(1, 0.30, "WEAKENING", "TRENDING") == LIFECYCLE_UNPROVEN

    def test_max_tier_cap(self):
        """Verify _PYRAMID_MAX_TIER is 3."""
        assert _PYRAMID_MAX_TIER == 3


# ── DataFrame integration tests (with mocked DuckDB) ────────────────────────

class TestComputePyramidTier:
    """Integration tests for compute_pyramid_tier with mocked DuckDB."""

    def _make_df(self, **kwargs):
        """Create a minimal test DataFrame."""
        defaults = {
            "TradeID": "TEST_TRADE_001",
            "AssetType": "OPTION",
            "Premium_Entry": 5.00,
            "Last": 7.50,  # 50% gain
            "Conviction_Status": "STABLE",
            "MomentumVelocity_State": "TRENDING",
        }
        defaults.update(kwargs)
        return pd.DataFrame([defaults])

    @patch("core.management.cycle2.pyramid_tier_tracker.duckdb")
    def test_no_duckdb_returns_tier0(self, mock_duckdb):
        """When DuckDB is unavailable, returns tier 0 but lifecycle derived from current state."""
        mock_duckdb.connect.side_effect = Exception("DB not found")
        df = self._make_df()  # 50% gain, STABLE conviction → THESIS_CONFIRMED
        result = compute_pyramid_tier(df)
        assert result.iloc[0]["Pyramid_Tier"] == 0
        # Lifecycle is still derived from current metrics (gain 50%, STABLE)
        assert result.iloc[0]["Winner_Lifecycle"] == LIFECYCLE_CONFIRMED

    @patch("core.management.cycle2.pyramid_tier_tracker.duckdb")
    def test_no_duckdb_losing_position_unproven(self, mock_duckdb):
        """When DuckDB unavailable + losing position → THESIS_UNPROVEN."""
        mock_duckdb.connect.side_effect = Exception("DB not found")
        df = self._make_df(Last=4.00)  # -20% loss
        result = compute_pyramid_tier(df)
        assert result.iloc[0]["Pyramid_Tier"] == 0
        assert result.iloc[0]["Winner_Lifecycle"] == LIFECYCLE_UNPROVEN

    @patch("core.management.cycle2.pyramid_tier_tracker.duckdb")
    def test_one_scale_up_high_tier1(self, mock_duckdb):
        """One prior SCALE_UP HIGH → tier 1."""
        mock_con = MagicMock()
        mock_duckdb.connect.return_value = mock_con
        mock_con.execute.return_value.fetchone.side_effect = [
            (1,),   # table exists check
            (1,),   # COUNT(*) for SCALE_UP HIGH
        ]
        df = self._make_df()
        result = compute_pyramid_tier(df)
        assert result.iloc[0]["Pyramid_Tier"] == 1
        assert result.iloc[0]["Winner_Lifecycle"] == LIFECYCLE_BUILDING

    @patch("core.management.cycle2.pyramid_tier_tracker.duckdb")
    def test_five_scale_ups_caps_at_3(self, mock_duckdb):
        """5 SCALE_UP HIGH records → tier 3 (capped)."""
        mock_con = MagicMock()
        mock_duckdb.connect.return_value = mock_con
        mock_con.execute.return_value.fetchone.side_effect = [
            (1,),   # table exists
            (5,),   # COUNT(*) = 5
        ]
        df = self._make_df()
        result = compute_pyramid_tier(df)
        assert result.iloc[0]["Pyramid_Tier"] == 3
        assert result.iloc[0]["Winner_Lifecycle"] == LIFECYCLE_FULL

    @patch("core.management.cycle2.pyramid_tier_tracker.duckdb")
    def test_stock_legs_skipped(self, mock_duckdb):
        """STOCK legs are skipped (tier 0, THESIS_UNPROVEN)."""
        mock_con = MagicMock()
        mock_duckdb.connect.return_value = mock_con
        mock_con.execute.return_value.fetchone.return_value = (1,)
        df = self._make_df(AssetType="STOCK")
        result = compute_pyramid_tier(df)
        assert result.iloc[0]["Pyramid_Tier"] == 0
        assert result.iloc[0]["Winner_Lifecycle"] == LIFECYCLE_UNPROVEN

    @patch("core.management.cycle2.pyramid_tier_tracker.duckdb")
    def test_no_table_returns_tier0(self, mock_duckdb):
        """When management_recommendations table doesn't exist, returns tier 0."""
        mock_con = MagicMock()
        mock_duckdb.connect.return_value = mock_con
        mock_con.execute.return_value.fetchone.return_value = (0,)  # table doesn't exist
        df = self._make_df()
        result = compute_pyramid_tier(df)
        assert result.iloc[0]["Pyramid_Tier"] == 0


# ── Schema tests ────────────────────────────────────────────────────────────

class TestSchemaInclusion:
    """Verify the schema includes new pyramid columns."""

    def test_schema_has_pyramid_tier(self):
        from core.shared.data_contracts.schema import MANAGEMENT_UI_COLUMNS
        assert "Pyramid_Tier" in MANAGEMENT_UI_COLUMNS

    def test_schema_has_winner_lifecycle(self):
        from core.shared.data_contracts.schema import MANAGEMENT_UI_COLUMNS
        assert "Winner_Lifecycle" in MANAGEMENT_UI_COLUMNS
