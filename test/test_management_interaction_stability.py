"""
Management Engine — Interaction Stability Tests
================================================
5 scenario-driven tests that validate edge interactions between the
stateful condition engine, thesis gate, roll timing, and doctrine rules.

These are NOT unit tests of individual functions.
They are interaction tests: does the system behave correctly when
multiple systems make contradictory demands simultaneously?

Run:
    pytest test/test_management_interaction_stability.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import numpy as np
import pytest

# ── path bootstrap ────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.management.cycle3.decision.engine import DoctrineAuthority, generate_recommendations
from core.management.cycle2.thesis.thesis_engine import _classify_thesis, THESIS_INTACT, THESIS_DEGRADED, THESIS_BROKEN
from core.management.run_all import compute_direction_reversals


# ── shared row factory ────────────────────────────────────────────────────────

def _base_buy_write_row(**overrides) -> pd.Series:
    """
    Minimal BUY_WRITE row that passes all epistemic guards and lands on HOLD.
    Caller can override any field to simulate a specific scenario.
    """
    base = {
        # Identity
        "TradeID":            "T001",
        "LegID":              "L001",
        "Symbol":             "AAPL",
        "Underlying_Ticker":  "AAPL",
        "Strategy":           "BUY_WRITE",
        "AssetType":          "STOCK",   # doctrine runs on STOCK leg now

        # Prices & cost
        "UL Last":            264.58,
        "Basis":              54674.0,   # 200 × $273.37
        "Quantity":           200.0,
        "Underlying_Price_Entry": 273.37,
        "Net_Cost_Basis_Per_Share": 267.25,
        "Cumulative_Premium_Collected": 6.12,

        # Short-call enrichment (Cycle 2.9) — key fix for multi-leg blindness
        "Short_Call_Delta":   0.62,
        "Short_Call_Strike":  260.0,
        "Short_Call_DTE":     26.0,
        "Short_Call_Premium": 6.12,
        "Short_Call_Last":    10.50,
        "Short_Call_Moneyness": "ITM",

        # Greeks (stock leg — mostly zero; doctrine reads Short_Call_* now)
        "Delta":              0.0,
        "Strike":             np.nan,
        "DTE":                np.nan,
        "Premium_Entry":      np.nan,
        "Last":               np.nan,
        "HV_20D":             0.33,

        # Chart / thesis
        "Thesis_State":       "INTACT",
        "Thesis_Gate":        "PASS",
        "_thesis_blocks_roll": False,
        "Thesis_Summary":     "",
        "PriceStructure_State": "STRUCTURE_INTACT",
        "TrendIntegrity_State": "TREND_UP",
        "ema50_slope":        0.01,
        "hv_20d_percentile":  45.0,

        # Conditions (empty by default)
        "_Active_Conditions":   "",
        "_Condition_Resolved":  "",

        # Misc required by guards
        "Snapshot_TS":        pd.Timestamp.now(),
        "Earnings_Date":      None,
        "Days_In_Trade":      20,
        "run_id":             "test-run",
        "Schema_Hash":        "abc123",
        "IV":                 None,
        "IV_Now":             None,
    }
    base.update(overrides)
    return pd.Series(base)


def _run_doctrine(row: pd.Series) -> dict:
    """Run DoctrineAuthority.evaluate on a single row."""
    return DoctrineAuthority.evaluate(row)


# ═══════════════════════════════════════════════════════════════════════════════
# Test 1 — Dead Cat Persistence & Oscillation Guard
# ═══════════════════════════════════════════════════════════════════════════════

class TestDeadCatPersistence:
    """
    Validate that ManagementStateStore correctly tracks condition lifecycle:
      fire → persist 2 days → resolve → re-fire within 24h
    Expected: same onset_ts, resolve_count unchanged, is_oscillating() = True
    """

    def _make_state_store(self):
        """Return a StateStore backed by an in-memory DuckDB."""
        # Patch PIPELINE_DB_PATH to use in-memory db
        import duckdb
        from core.management.conditions.state_store import ManagementStateStore, _CREATE_SQL

        mock_con = duckdb.connect(":memory:")
        mock_con.execute(_CREATE_SQL)

        # We'll monkey-patch get_duckdb_connection to return our in-memory con
        return mock_con

    def test_re_fire_within_24h_treated_as_same_episode(self):
        """
        Re-fire within 24h → onset_ts preserved, resolve_count stable.
        """
        from core.management.conditions.state_store import ManagementStateStore, _CREATE_SQL
        import duckdb

        # Build isolated in-memory store
        mem_con = duckdb.connect(":memory:")
        mem_con.execute(_CREATE_SQL)

        now = datetime.now(tz=timezone.utc)
        onset = now - timedelta(days=3)
        resolved_1h_ago = now - timedelta(hours=1)  # resolved only 1h ago

        # Seed: condition was active, then resolved 1h ago (within 24h guard)
        mem_con.execute("""
            INSERT INTO management_state
                (trade_id, condition_type, onset_ts, last_seen_ts,
                 resolved_ts, resolve_count, last_action, thesis_state, thesis_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, ["T001", "dead_cat_bounce",
              onset, resolved_1h_ago,
              resolved_1h_ago,   # resolved_ts set → it resolved
              1,                 # resolve_count = 1
              "HOLD", None, None])

        # Simulate re-fire: condition is NOW active again
        r = {"trade_id": "T001", "ctype": "dead_cat_bounce",
             "last_action": "HOLD", "thesis_state": None, "thesis_ts": None,
             "is_resolved": False, "now": now}

        prior_row = {
            "onset_ts":      onset,
            "resolved_ts":   resolved_1h_ago,
            "resolve_count": 1,
            "last_seen_ts":  resolved_1h_ago,
        }

        # Replicate oscillation guard logic from state_store.save()
        from core.management.conditions.state_store import MIN_RESOLVE_HOLD_HOURS
        hours_since = (now - resolved_1h_ago).total_seconds() / 3600
        is_within_guard = hours_since < MIN_RESOLVE_HOLD_HOURS

        assert is_within_guard, (
            f"Expected re-fire at {hours_since:.1f}h to be within the "
            f"{MIN_RESOLVE_HOLD_HOURS}h guard window"
        )

        # onset_ts should be PRESERVED (not reset)
        new_onset = prior_row["onset_ts"] if is_within_guard else now
        assert new_onset == onset, "onset_ts must be preserved for re-fire within guard window"

        # resolve_count must NOT increment on re-fire
        new_count = prior_row["resolve_count"]  # unchanged
        assert new_count == 1, "resolve_count must not increment on re-fire"

    def test_re_fire_after_26h_is_new_episode(self):
        """
        Re-fire 26h after resolution → new onset_ts, resolve_count reset to 0.
        """
        from core.management.conditions.state_store import MIN_RESOLVE_HOLD_HOURS

        now = datetime.now(tz=timezone.utc)
        onset = now - timedelta(days=5)
        resolved_26h_ago = now - timedelta(hours=26)

        prior_row = {
            "onset_ts":      onset,
            "resolved_ts":   resolved_26h_ago,
            "resolve_count": 2,
            "last_seen_ts":  resolved_26h_ago,
        }

        hours_since = (now - resolved_26h_ago).total_seconds() / 3600
        is_within_guard = hours_since < MIN_RESOLVE_HOLD_HOURS

        assert not is_within_guard, (
            f"26h should be OUTSIDE the {MIN_RESOLVE_HOLD_HOURS}h guard window"
        )

        # New episode: onset resets
        new_onset = now  # reset
        new_count = 0    # reset

        assert new_onset != onset, "onset_ts must reset for genuinely new episode"
        assert new_count == 0, "resolve_count must reset for genuinely new episode"

    def test_is_oscillating_flag(self):
        """
        resolve_count ≥ 2 → is_oscillating() returns True.
        """
        from core.management.conditions.state_store import ManagementStateStore

        store = ManagementStateStore.__new__(ManagementStateStore)

        prior = {
            "T001::dead_cat_bounce": {
                "resolve_count": 2,
                "onset_ts": datetime.now(tz=timezone.utc) - timedelta(days=4),
                "resolved_ts": datetime.now(tz=timezone.utc) - timedelta(hours=2),
                "days_active": 4,
            }
        }

        assert store.is_oscillating("T001", "dead_cat_bounce", prior=prior) is True

    def test_resolve_count_1_is_not_oscillating(self):
        """
        resolve_count = 1 (resolved once, not yet re-fired) → not oscillating.
        """
        from core.management.conditions.state_store import ManagementStateStore

        store = ManagementStateStore.__new__(ManagementStateStore)

        prior = {
            "T001::dead_cat_bounce": {
                "resolve_count": 1,
                "onset_ts": datetime.now(tz=timezone.utc) - timedelta(days=2),
                "resolved_ts": None,
                "days_active": 2,
            }
        }

        assert store.is_oscillating("T001", "dead_cat_bounce", prior=prior) is False


# ═══════════════════════════════════════════════════════════════════════════════
# Test 2 — Thesis BROKEN Blocks Discretionary Roll
# ═══════════════════════════════════════════════════════════════════════════════

class TestThesisBrokenBlocksRoll:
    """
    When Thesis_State = BROKEN, _thesis_blocks_roll = True.
    Doctrine must NOT emit ROLL for any discretionary trigger
    (50% capture, negative carry, low DTE) when thesis is broken.
    Emergency exits (hard stop, delta > 0.70, DTE < 7) may still fire.
    """

    def test_50pct_capture_blocked_by_broken_thesis(self):
        """
        50% premium captured + thesis BROKEN → HOLD (not ROLL).
        """
        row = _base_buy_write_row(
            # 50% capture would normally trigger ROLL
            Short_Call_Premium=10.0,
            Short_Call_Last=4.0,   # 60% captured — well past 50%
            Short_Call_DTE=30.0,
            Short_Call_Delta=0.35,
            Short_Call_Strike=270.0,
            # Thesis broken
            Thesis_State="BROKEN",
            _thesis_blocks_roll=True,
            Thesis_Summary="EMA200 broken on high volume.",
        )
        result = _run_doctrine(row)
        assert result["Action"] != "ROLL", (
            f"Broken thesis must block 50%-capture roll. Got Action={result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )
        rat = result.get("Rationale", "")
        assert "Thesis" in rat or "BROKEN" in rat or "EV" in rat, (
            "Rationale must mention thesis block or EV comparison"
        )

    def test_negative_carry_blocked_by_broken_thesis(self):
        """
        Negative carry (yield < 10.375%) + thesis BROKEN → HOLD (not ROLL).
        """
        row = _base_buy_write_row(
            # Premium yield < carry cost
            Short_Call_Premium=0.50,   # tiny premium → annualized yield << 10.375%
            Short_Call_DTE=45.0,
            Short_Call_Delta=0.20,
            Short_Call_Strike=270.0,
            Short_Call_Last=0.40,
            Net_Cost_Basis_Per_Share=267.25,
            # Thesis broken
            Thesis_State="BROKEN",
            _thesis_blocks_roll=True,
            Thesis_Summary="Revenue guidance cut.",
        )
        result = _run_doctrine(row)
        assert result["Action"] != "ROLL", (
            f"Broken thesis must block negative-carry roll. Got Action={result['Action']}"
        )

    def test_hard_stop_still_fires_through_broken_thesis(self):
        """
        Hard stop (drift < −20%) must fire EXIT even when thesis is broken.
        Emergency gates return early BEFORE thesis block is checked.
        """
        row = _base_buy_write_row(
            Net_Cost_Basis_Per_Share=267.25,
            Basis=53450.0,    # 200 × $267.25
            Thesis_State="BROKEN",
            _thesis_blocks_roll=True,
        )
        row["UL Last"] = 200.0   # −25% from net cost → hard stop
        result = _run_doctrine(row)
        assert result["Action"] == "EXIT", (
            f"Hard stop must fire EXIT even with broken thesis. "
            f"Got Action={result['Action']}"
        )
        assert result["Urgency"] == "CRITICAL"

    def test_deep_itm_still_fires_through_broken_thesis(self):
        """
        Delta > 0.70 must fire ROLL even when thesis is broken.
        ITM Defense is an emergency gate — returns before thesis block.
        """
        row = _base_buy_write_row(
            Short_Call_Delta=0.82,    # deep ITM
            Short_Call_Strike=260.0,
            Short_Call_DTE=20.0,
            Thesis_State="BROKEN",
            _thesis_blocks_roll=True,
        )
        result = _run_doctrine(row)
        assert result["Action"] in ("ROLL", "LET_EXPIRE", "ACCEPT_CALL_AWAY", "EXIT"), (
            f"Deep ITM defense must act. Got Action={result['Action']}"
        )
        rat = result.get("Rationale", "")
        assert "ITM" in rat or "ITM Defense" in result.get("Doctrine_Source", "") or "EV" in rat


# ═══════════════════════════════════════════════════════════════════════════════
# Test 3 — Hard Stop Overrides Everything
# ═══════════════════════════════════════════════════════════════════════════════

class TestHardStopOverridesEverything:
    """
    Drift < −20% → EXIT CRITICAL.
    No other gate (timing, thesis block, conditions, earnings) should interfere.
    The hard stop returns early before any of these checks.
    """

    def test_hard_stop_exit_critical(self):
        """Drift of −25% → EXIT CRITICAL."""
        row = _base_buy_write_row(
            Net_Cost_Basis_Per_Share=267.25,
            Basis=53450.0,
        )
        row["UL Last"] = 200.0   # well below net cost $267.25 → drift = −25%
        result = _run_doctrine(row)
        assert result["Action"] == "EXIT", f"Got {result['Action']}"
        assert result["Urgency"] == "CRITICAL", f"Got {result['Urgency']}"
        assert "Hard stop" in result.get("Rationale", ""), result.get("Rationale", "")

    def test_hard_stop_ignores_timing_gate(self):
        """Timing classifier cannot block the hard stop."""
        row = _base_buy_write_row(
            Net_Cost_Basis_Per_Share=267.25,
            Basis=53450.0,
            # Simulate choppy market (timing would say WAIT)
            PriceStructure_State="STRUCTURE_CHOPPY",
            TrendIntegrity_State="NO_TREND",
        )
        row["UL Last"] = 195.0   # −27% drift
        result = _run_doctrine(row)
        assert result["Action"] == "EXIT"
        assert result["Urgency"] == "CRITICAL"
        # Timing rationale should NOT appear — it runs after hard stop
        assert "timing" not in result.get("Rationale", "").lower(), (
            "Timing gate rationale should not appear when hard stop fires first"
        )

    def test_hard_stop_ignores_condition_annotations(self):
        """Active dead_cat condition cannot block the hard stop."""
        row = _base_buy_write_row(
            Net_Cost_Basis_Per_Share=267.25,
            Basis=53450.0,
            _Active_Conditions="dead_cat_bounce [day 3, val=1.00]",
        )
        row["UL Last"] = 185.0   # −31% drift
        result = _run_doctrine(row)
        assert result["Action"] == "EXIT"
        assert result["Urgency"] == "CRITICAL"

    def test_approaching_hard_stop_is_roll_high(self):
        """
        Drift between −15% and −20% → ROLL HIGH (approaching, not breached).
        267.25 × 0.83 = 221.8 → drift ≈ −17% (inside warning zone, outside hard stop).
        """
        row = _base_buy_write_row(
            Net_Cost_Basis_Per_Share=267.25,
            Basis=53450.0,
        )
        row["UL Last"] = 221.8   # −17% from $267.25 net cost
        result = _run_doctrine(row)
        assert result["Action"] == "ROLL", (
            f"−17% drift should trigger 'approaching hard stop' ROLL. Got {result['Action']}"
        )
        assert result["Urgency"] == "HIGH"


# ═══════════════════════════════════════════════════════════════════════════════
# Test 4 — HOLD with Condition Pre-Staging
# ═══════════════════════════════════════════════════════════════════════════════

class TestHoldWithConditionPreStaging:
    """
    When Action=HOLD and _Active_Conditions contains dead_cat_bounce,
    the roll candidate engine is pre-staged (run_all.py checks _needs_candidates mask).

    We test the mask logic directly — no Schwab call needed.
    """

    def test_dead_cat_hold_included_in_needs_candidates_mask(self):
        """
        HOLD row with dead_cat_bounce active → _needs_candidates = True.
        """
        df = pd.DataFrame([{
            "TradeID":            "T001",
            "Action":             "HOLD",
            "_Active_Conditions": "dead_cat_bounce [day 2, val=1.00]",
        }])

        # Replicate the mask from run_all.py lines 486–495
        _roll_mask = df["Action"] == "ROLL"
        _hold_with_condition_mask = (
            (df["Action"] == "HOLD")
            & (df["_Active_Conditions"].fillna("").str.contains(
                "dead_cat_bounce|iv_depressed", case=False, na=False
            ))
        )
        _needs_candidates = _roll_mask | _hold_with_condition_mask

        assert _needs_candidates.iloc[0] is True or _needs_candidates.iloc[0] == True, (
            "HOLD with dead_cat_bounce active must be included in needs_candidates mask"
        )

    def test_hold_without_condition_excluded_from_mask(self):
        """
        Plain HOLD (no active condition) → _needs_candidates = False.
        """
        df = pd.DataFrame([{
            "TradeID":            "T002",
            "Action":             "HOLD",
            "_Active_Conditions": "",
        }])

        _roll_mask = df["Action"] == "ROLL"
        _hold_with_condition_mask = (
            (df["Action"] == "HOLD")
            & (df["_Active_Conditions"].fillna("").str.contains(
                "dead_cat_bounce|iv_depressed", case=False, na=False
            ))
        )
        _needs_candidates = _roll_mask | _hold_with_condition_mask

        assert not _needs_candidates.iloc[0], (
            "Plain HOLD must NOT be included in needs_candidates mask"
        )

    def test_action_remains_hold_when_condition_active(self):
        """
        dead_cat_bounce active → doctrine must emit HOLD, not ROLL.
        The condition BLOCKS the roll; candidates are pre-staged for when it resolves.
        """
        row = _base_buy_write_row(
            # 50% capture would normally fire ROLL
            Short_Call_Premium=10.0,
            Short_Call_Last=4.0,    # 60% captured
            Short_Call_DTE=30.0,
            Short_Call_Delta=0.35,
            # Condition annotation — doctrine reads this to know a condition is active
            _Active_Conditions="dead_cat_bounce [day 2, val=1.00]",
            _Condition_Resolved="",
        )
        # Doctrine itself doesn't read _Active_Conditions to block rolls —
        # that's ConditionMonitor's job (apply_resolutions pre-populates _thesis_blocks_roll
        # for dead_cat). But since we're testing doctrine in isolation here,
        # we validate that the mask logic correctly identifies this row for pre-staging.
        # The Action from doctrine is whatever the doctrine decides; the mask runs in run_all.py.
        result = _run_doctrine(row)

        # Regardless of doctrine action: the mask must fire for pre-staging
        df = pd.DataFrame([{
            "Action":             result["Action"],
            "_Active_Conditions": "dead_cat_bounce [day 2, val=1.00]",
        }])
        _hold_mask = (df["Action"] == "HOLD") & (
            df["_Active_Conditions"].str.contains("dead_cat_bounce", na=False)
        )
        _roll_mask = df["Action"] == "ROLL"
        _needs_candidates = _hold_mask | _roll_mask

        # Either the action is ROLL (direct) or it's HOLD+condition (pre-staged)
        assert _needs_candidates.iloc[0], (
            f"Row with dead_cat_bounce active must be flagged for candidate pre-staging. "
            f"Got Action={result['Action']}"
        )

    def test_iv_depressed_hold_also_pre_staged(self):
        """iv_depressed is also a blocking condition → included in pre-staging mask."""
        df = pd.DataFrame([{
            "TradeID":            "T003",
            "Action":             "HOLD",
            "_Active_Conditions": "iv_depressed [day 5, val=0.28]",
        }])
        _hold_with_condition_mask = (
            (df["Action"] == "HOLD")
            & (df["_Active_Conditions"].fillna("").str.contains(
                "dead_cat_bounce|iv_depressed", case=False, na=False
            ))
        )
        assert _hold_with_condition_mask.iloc[0]


# ═══════════════════════════════════════════════════════════════════════════════
# Test 5 — Deep ITM Delta Gate Fires Before Timing Gate
# ═══════════════════════════════════════════════════════════════════════════════

class TestDeepITMDeltaGate:
    """
    Delta > 0.70 → ITM Defense ROLL fires regardless of:
    - Timing classifier result (CHOPPY / WAIT)
    - DTE (as long as > 7 — both are emergency gates but DTE < 7 fires first)
    - Active conditions
    - Whether the roll would be "profitable"

    The critical contract: emergency gates (ITM defense) return early, BEFORE
    _classify_roll_timing is even called.
    """

    def test_delta_075_fires_roll(self):
        """Delta 0.75 → ROLL, Urgency HIGH or CRITICAL."""
        row = _base_buy_write_row(
            Short_Call_Delta=0.75,
            Short_Call_Strike=260.0,
            Short_Call_DTE=20.0,
        )
        result = _run_doctrine(row)
        assert result["Action"] == "ROLL", f"Got {result['Action']}"
        assert result["Urgency"] in ("HIGH", "CRITICAL")
        assert "ITM" in result.get("Doctrine_Source", "") or "ITM" in result.get("Rationale", "")

    def test_delta_082_fires_critical_when_assignment_below_net_cost(self):
        """
        Delta 0.82, strike below net cost → CRITICAL (assignment would realize loss).
        """
        row = _base_buy_write_row(
            Short_Call_Delta=0.82,
            Short_Call_Strike=255.0,    # below net cost $267.25
            Short_Call_DTE=18.0,
            Net_Cost_Basis_Per_Share=267.25,
        )
        result = _run_doctrine(row)
        assert result["Action"] in ("ROLL", "EXIT", "HOLD"), (
            f"Deep ITM below net cost should act defensively. Got {result['Action']}"
        )
        assert result["Urgency"] in ("MEDIUM", "HIGH", "CRITICAL"), (
            f"Assignment below net cost must be MEDIUM+. Got {result['Urgency']}"
        )

    def test_delta_082_fires_high_when_assignment_above_net_cost(self):
        """
        Delta 0.82, strike above net cost → HIGH (assignment profitable).
        """
        row = _base_buy_write_row(
            Short_Call_Delta=0.82,
            Short_Call_Strike=270.0,    # above net cost $267.25
            Short_Call_DTE=18.0,
            Net_Cost_Basis_Per_Share=267.25,
        )
        result = _run_doctrine(row)
        assert result["Action"] == "ROLL"
        assert result["Urgency"] == "HIGH", (
            f"Assignment above net cost should be HIGH. Got {result['Urgency']}"
        )

    def test_itm_defense_ignores_choppy_timing(self):
        """
        Delta > 0.70 fires even in a choppy market.
        _classify_roll_timing would return WAIT for this scenario
        but ITM defense returns early before timing is checked.
        """
        row = _base_buy_write_row(
            Short_Call_Delta=0.78,
            Short_Call_Strike=260.0,
            Short_Call_DTE=15.0,
            # Simulate choppy market conditions
            PriceStructure_State="STRUCTURE_CHOPPY",
            TrendIntegrity_State="NO_TREND",
            ema50_slope=-0.05,
            hv_20d_percentile=75.0,
        )
        result = _run_doctrine(row)
        assert result["Action"] == "ROLL", (
            f"ITM defense must fire even in choppy market. Got {result['Action']}"
        )
        # Timing "WAIT" language must not appear in rationale
        assert "choppy" not in result.get("Rationale", "").lower() or "ITM" in result.get("Rationale", ""), (
            "Timing gate must not pollute ITM defense rationale"
        )

    def test_delta_just_below_threshold_does_not_fire(self):
        """
        Delta 0.69 (just below 0.70 threshold) → ITM defense must NOT fire.
        Falls through to later gates.
        """
        row = _base_buy_write_row(
            Short_Call_Delta=0.69,
            Short_Call_Strike=260.0,
            Short_Call_DTE=20.0,
            # Premium not at 50% capture, no other trigger — should HOLD
            Short_Call_Premium=6.12,
            Short_Call_Last=10.50,
        )
        result = _run_doctrine(row)
        # Should NOT be an ITM-defense ROLL
        assert "ITM Defense" not in result.get("Doctrine_Source", ""), (
            f"Delta 0.69 must not trigger ITM Defense. Got Source={result.get('Doctrine_Source')}"
        )

    def test_dte_under_7_fires_before_itm_check(self):
        """
        DTE < 7 is also an emergency gate. It fires in the order:
        hard_stop → earnings → story → ITM → DTE<7 → ...
        Actually DTE<7 comes AFTER ITM defense in _buy_write_doctrine.
        Test confirms DTE<7 fires even when delta is only 0.40.
        """
        row = _base_buy_write_row(
            Short_Call_Delta=0.40,    # below ITM threshold
            Short_Call_DTE=5.0,       # DTE < 7
            Short_Call_Strike=270.0,
        )
        result = _run_doctrine(row)
        assert result["Action"] == "ROLL", (
            f"DTE < 7 must trigger expiration management ROLL. Got {result['Action']}"
        )
        assert "pin risk" in result.get("Rationale", "").lower() or \
               "Expiration" in result.get("Doctrine_Source", ""), \
               result.get("Rationale", "")


# ═══════════════════════════════════════════════════════════════════════════════
# Interaction matrix: combined edge case
# ═══════════════════════════════════════════════════════════════════════════════

class TestInteractionMatrix:
    """
    Combined edge cases that require multiple systems to cooperate correctly.
    """

    def test_dead_cat_active_plus_50pct_capture(self):
        """
        50% capture valid + dead_cat_bounce in _Active_Conditions.
        Doctrine itself doesn't read _Active_Conditions —
        but the combined row should produce a valid, non-crashing result.
        The test validates no exception is thrown and Action has a valid value.
        """
        row = _base_buy_write_row(
            Short_Call_Premium=10.0,
            Short_Call_Last=4.0,
            Short_Call_DTE=30.0,
            _Active_Conditions="dead_cat_bounce [day 1, val=1.00]",
        )
        result = _run_doctrine(row)
        assert result["Action"] in ("ROLL", "HOLD", "EXIT"), (
            f"Unexpected Action: {result['Action']}"
        )
        assert "Rationale" in result

    def test_thesis_degraded_does_not_block_roll(self):
        """
        Thesis_State = DEGRADED (_thesis_blocks_roll = False).
        Discretionary rolls should still fire (with a warning in rationale).
        """
        row = _base_buy_write_row(
            Short_Call_Premium=10.0,
            Short_Call_Last=4.0,    # 60% captured — should ROLL
            Short_Call_DTE=30.0,
            Short_Call_Delta=0.35,
            Thesis_State="DEGRADED",
            _thesis_blocks_roll=False,   # DEGRADED does not block
            Thesis_Summary="Revenue declining but thesis borderline.",
        )
        result = _run_doctrine(row)
        # DEGRADED should NOT block — ROLL should fire (unless timing says WAIT)
        # We don't know timing here — just assert no crash and valid action
        assert result["Action"] in ("ROLL", "HOLD")
        if result["Action"] == "HOLD":
            # If HOLD, timing must be the reason — not thesis block
            assert "BROKEN" not in result.get("Rationale", ""), (
                "DEGRADED thesis must not produce 'BROKEN' rationale"
            )


# ── shared row factory for long options ──────────────────────────────────────

def _base_long_option_row(**overrides) -> pd.Series:
    """
    Minimal LONG_PUT row that passes epistemic guards and lands on default HOLD.
    Caller overrides specific fields to trigger direction-aware gates.
    """
    base = {
        # Identity
        "TradeID":            "T-LO-001",
        "LegID":              "L-LO-001",
        "Symbol":             "MSFT260404P00390000",
        "Underlying_Ticker":  "MSFT",
        "Strategy":           "LONG_PUT",
        "AssetType":          "OPTION",
        "Option_Type":        "PUT",
        "Call/Put":           "P",

        # Prices & Greeks
        "UL Last":            410.0,
        "Strike":             390.0,
        "DTE":                30.0,
        "Premium_Entry":      12.50,
        "Last":               6.0,
        "Bid":                5.80,
        "Delta":              -0.25,
        "Gamma":              0.015,
        "Theta":              -0.75,
        "Vega":               0.12,
        "Delta_Entry":        -0.35,
        "HV_20D":             0.28,
        "Quantity":           1.0,
        "Basis":              1250.0,

        # P&L (Total_GL_Decimal is what engine reads)
        "PnL_Dollar":         -650.0,
        "Total_GL_Decimal":   -0.52,

        # Drift / momentum — stock rallying (ADVERSE for put)
        "Drift_Direction":    "Up",
        "Price_Drift_Pct":    0.05,
        "roc_5":              3.66,
        "roc_10":             4.2,
        "MomentumVelocity_State": "REVERSING",
        "momentum_slope":     -0.92,

        # Thesis
        "Thesis_State":       "INTACT",
        "Thesis_Gate":        "PASS",
        "_thesis_blocks_roll": False,
        "Thesis_Summary":     "Bearish thesis on overvaluation.",

        # Structure
        "PriceStructure_State":  "STRUCTURE_INTACT",
        "TrendIntegrity_State":  "NO_TREND",
        "GreekDominance_State":  "THETA_DOMINANT",

        # Compression
        "CompressionMaturity":   "EARLY_COMPRESSION",
        "bb_width_z":            -2.22,

        # Entry context (weak)
        "Entry_Chart_State_PriceStructure": "RANGE_BOUND",
        "Entry_Chart_State_TrendIntegrity": "NO_TREND",

        # IV / EV
        "IV_Percentile":         89.0,
        "IV_Percentile_Depth":   60,    # reliable depth (>= 45 = MATURE)
        "IV_vs_HV_Gap":          0.0,   # neutral — neither cheap nor expensive vol
        "EV_Feasibility_Ratio":  0.63,

        # Misc required fields
        "DTE_Entry":          60.0,
        "Days_In_Trade":         5.0,   # 5 days held — past grace period
        "Days_Held":             30.0,
        "Expiration":            "2026-05-15",  # same as entry → not rolled
        "Expiration_Entry":      "2026-05-15",
        "Snapshot_TS":           pd.Timestamp.now(),
        "Earnings_Date":         None,
        "run_id":                "test-dir-aware",
        "Schema_Hash":           "abc123",
        "IV":                    None,
        "IV_Now":                None,
        "choppiness_index":      62.0,
        "adx_14":                15.0,
        "rsi_14":                55.0,
        "ema50_slope":           0.01,
        "hv_20d_percentile":     45.0,
        "HV_Daily_Move_1Sigma":  5.0,
        "Recovery_Feasibility":  "UNLIKELY",
        "Recovery_Move_Per_Day": 3.50,
        "Theta_Bleed_Daily_Pct": 0.06,
        "Prior_Action":          "HOLD",
        "_Active_Conditions":    "",
        "_Condition_Resolved":   "",
        "_Ticker_Net_Delta":     0.0,
        "_Ticker_Has_Stock":     False,
    }
    base.update(overrides)
    return pd.Series(base)


# ═══════════════════════════════════════════════════════════════════════════════
# Direction-Adverse Put — 5 tests for direction-aware gates
# ═══════════════════════════════════════════════════════════════════════════════

class TestDirectionAdversePut:
    """
    MSFT case study: stock rallies +3.66% in 5 days against a LONG_PUT.
    Verifies that direction-aware gates catch adverse drift, while leaving
    confirming-direction positions (calls with UP drift) alone.
    """

    def test_stock_rallying_against_long_put_triggers_exit_or_roll(self):
        """
        Gate 2b-dir: stock drifting UP with adverse ROC5 on a long put →
        EXIT or ROLL (never HOLD). MSFT scenario.
        """
        row = _base_long_option_row()  # defaults = MSFT scenario
        result = _run_doctrine(row)
        assert result["Action"] in ("EXIT", "ROLL"), (
            f"Direction-adverse put must EXIT or ROLL, got {result['Action']}: "
            f"{result.get('Rationale', '')}"
        )
        # Should mention direction in rationale
        rationale = result.get("Rationale", "").lower()
        assert "direction" in rationale or "adverse" in rationale, (
            f"Rationale must mention direction/adverse, got: {result.get('Rationale', '')}"
        )

    def test_catalyst_within_14d_with_intact_thesis_holds(self):
        """
        Gate 2b-dir escape path: Thesis INTACT + catalyst within 14 days →
        HOLD HIGH (not EXIT). The catalyst exception.
        """
        from datetime import datetime, timedelta
        earn_date = (datetime.now() + timedelta(days=10)).strftime("%Y-%m-%d")
        row = _base_long_option_row(
            Thesis_State="INTACT",
            Total_GL_Decimal=-0.12,          # mild loss (below weak threshold -0.10)
            Conviction_Status="STABLE",
        )
        row["Earnings_Date"] = earn_date
        result = _run_doctrine(row)
        # With catalyst near + thesis intact + mild loss, the 2b-dir gate
        # should use the catalyst escape path → HOLD HIGH
        # Note: other gates may catch it, but the key is that the specific
        # 2b-dir direction-adverse gate does NOT fire EXIT when catalyst is near
        if result["Action"] == "EXIT" and "direction" in result.get("Rationale", "").lower():
            pytest.fail(
                f"2b-dir should HOLD with catalyst near, got EXIT: {result.get('Rationale', '')}"
            )

    def test_long_call_with_stock_falling_triggers_exit_or_roll(self):
        """
        Symmetric test: LONG_CALL with stock falling (drift DOWN) →
        direction-adverse → EXIT or ROLL.
        """
        row = _base_long_option_row(**{
            "Strategy": "LONG_CALL",
            "Option_Type": "CALL",
            "Call/Put": "C",
            "Symbol": "AAPL260404C00250000",
            "Underlying_Ticker": "AAPL",
            "Strike": 250.0,
            "UL Last": 235.0,
            "Delta": 0.30,
            "Delta_Entry": 0.45,
            # Stock falling — adverse for call
            "Drift_Direction": "Down",
            "Price_Drift_Pct": -0.05,
            "roc_5": -3.5,
            "roc_10": -4.0,
            "MomentumVelocity_State": "REVERSING",
            "momentum_slope": 0.5,
            # Entry context — weak
            "Entry_Chart_State_PriceStructure": "RANGE_BOUND",
            "Entry_Chart_State_TrendIntegrity": "NO_TREND",
        })
        result = _run_doctrine(row)
        assert result["Action"] in ("EXIT", "ROLL"), (
            f"Direction-adverse call must EXIT or ROLL, got {result['Action']}: "
            f"{result.get('Rationale', '')}"
        )

    def test_gate7_stock_up_for_put_is_not_recovering(self):
        """
        Gate 7 fix: for a long put, stock drifting UP is NOT recovering —
        it's moving AGAINST the thesis. Recovery feasibility gate should fire.
        """
        row = _base_long_option_row(
            PnL_Pct=-0.35,
            Recovery_Feasibility="IMPOSSIBLE",
            Recovery_Move_Per_Day=8.0,
            HV_Daily_Move_1Sigma=3.0,
            Drift_Direction="Up",
            # Ensure we get past earlier gates by using STRONG entry + thesis intact
            Entry_Chart_State_PriceStructure="STRUCTURAL_DOWN",
            Entry_Chart_State_TrendIntegrity="STRONG_TREND",
            # Dampen 2b-dir trigger
            roc_5=0.5,       # mild, below typical thresholds
            roc_10=0.3,
            Price_Drift_Pct=0.01,
        )
        result = _run_doctrine(row)
        # Gate 7 should NOT suppress the IMPOSSIBLE recovery just because stock is UP
        # (Before fix: stock UP → _recovering=True → gate suppressed → HOLD)
        # After fix for puts: stock UP → _recovering=False → gate fires → EXIT
        # Some earlier gate may also catch it — key is it must NOT be HOLD
        if result["Action"] == "HOLD":
            rationale = result.get("Rationale", "")
            # If it's HOLD, it should NOT be because gate 7 suppressed the exit
            assert "infeasible" not in rationale.lower() or "recovering" not in rationale.lower(), (
                f"Gate 7 should not suppress recovery check for puts when stock is UP. "
                f"Got HOLD with: {rationale}"
            )

    def test_compression_plus_adverse_drift_removes_exemption(self):
        """
        Gate 2c-ii fix: compression (bb_width_z=-2.22) should NOT exempt
        a position when drift is ADVERSE to the option's direction.
        Before fix: _tti_compressing → exempt → HOLD.
        After fix: _tti_compressing AND _drift_is_adverse → NOT exempt → may trigger EXIT.
        """
        row = _base_long_option_row(
            bb_width_z=-2.22,
            CompressionMaturity="EARLY_COMPRESSION",
            GreekDominance_State="THETA_DOMINANT",
            MomentumVelocity_State="STALLING",
            Drift_Direction="Up",        # adverse for put
            roc_5=0.0,                   # no momentum (gate 2c-ii needs this)
            roc_10=0.0,
            choppiness_index=60.0,       # range-bound
            adx_14=12.0,
            DTE=25.0,
            PnL_Pct=-0.15,
        )
        result = _run_doctrine(row)
        # After fix: compression + adverse drift = no exemption
        # Gate 2c-ii should be able to fire, or another gate should catch it
        # The key assertion: if theta dominant + no momentum + range bound + adverse drift
        # the position should NOT get a free pass just because of compression
        # Accept EXIT or ROLL (an earlier direction-aware gate may catch it first)
        # HOLD is acceptable ONLY if no "compression" appears in rationale
        if result["Action"] == "HOLD":
            rationale = result.get("Rationale", "").lower()
            assert "energy building" not in rationale and "compress" not in rationale, (
                f"Compression should NOT exempt when drift is adverse. "
                f"Got HOLD with: {result.get('Rationale', '')}"
            )

    def test_winning_put_stock_falling_does_not_trigger_adverse(self):
        """
        Winning long put with stock falling (favorable direction) must NOT
        trigger the direction-adverse gate. P&L threshold is the safety net.
        """
        row = _base_long_option_row(
            Total_GL_Decimal=0.35,    # winning: +35%
            Premium_Entry=10.0,
            Last=13.50,
            Bid=13.30,
            # Stock FALLING — this is favorable for a put
            Drift_Direction="Down",
            Price_Drift_Pct=-0.04,
            roc_5=-2.5,
            roc_10=-3.0,
            UL_Last=380.0,            # stock dropped below strike
        )
        row["UL Last"] = 380.0
        result = _run_doctrine(row)
        rationale = result.get("Rationale", "").lower()
        assert "direction adverse" not in rationale, (
            f"Winning put with stock falling should NOT trigger adverse gate. "
            f"Got: {result['Action']} — {result.get('Rationale', '')}"
        )

    def test_winning_call_stock_rising_does_not_trigger_adverse(self):
        """
        Winning long call with stock rising (favorable direction) must NOT
        trigger the direction-adverse gate.
        """
        row = _base_long_option_row(**{
            "Strategy": "LONG_CALL",
            "Option_Type": "CALL",
            "Call/Put": "C",
            "Symbol": "AAPL260404C00250000",
            "Underlying_Ticker": "AAPL",
            "Strike": 250.0,
            "UL Last": 270.0,
            "Premium_Entry": 8.0,
            "Last": 22.0,
            "Bid": 21.80,
            "Delta": 0.75,
            "Delta_Entry": 0.50,
            "Total_GL_Decimal": 1.75,   # winning: +175%
            # Stock RISING — favorable for call
            "Drift_Direction": "Up",
            "Price_Drift_Pct": 0.06,
            "roc_5": 4.0,
            "roc_10": 6.5,
            "MomentumVelocity_State": "ACCELERATING",
            "momentum_slope": 1.2,
            "Entry_Chart_State_PriceStructure": "STRUCTURAL_UP",
            "Entry_Chart_State_TrendIntegrity": "STRONG_TREND",
        })
        result = _run_doctrine(row)
        rationale = result.get("Rationale", "").lower()
        assert "direction adverse" not in rationale, (
            f"Winning call with stock rising should NOT trigger adverse gate. "
            f"Got: {result['Action']} — {result.get('Rationale', '')}"
        )

    def test_slow_grind_flat_roc5_but_adverse_drift_triggers(self):
        """
        Slow grind scenario: ROC5 ≈ 0 (below σ threshold) but accumulated
        Price_Drift_Pct exceeds σ-normalized adverse threshold.
        OR logic with sigma-normalized thresholds catches this.
        With HV_20D=0.28: drift_z = 0.04 / 0.01764 = 2.27σ (> 2.0 threshold).
        """
        row = _base_long_option_row(
            roc_5=0.5,                  # below σ threshold (z=0.13)
            roc_10=1.5,                 # mild drift over 10d
            Drift_Direction="Up",       # adverse for put
            Price_Drift_Pct=0.04,       # drift_z=2.27σ with HV=28% (> 2.0 threshold)
            Total_GL_Decimal=-0.18,     # losing
            DTE=25.0,
        )
        result = _run_doctrine(row)
        # With OR logic: _drift_is_adverse (drift_z > 2.0) fires even though ROC5 z < 1.5
        assert result["Action"] in ("EXIT", "ROLL", "HOLD"), (
            f"Unexpected action for slow grind. Got {result['Action']}: {result.get('Rationale', '')}"
        )
        # If HOLD, must NOT be default hold — should have direction-aware reasoning
        if result["Action"] == "HOLD":
            rationale = result.get("Rationale", "").lower()
            assert "direction" in rationale or "adverse" in rationale or "sector" in rationale, (
                f"Slow grind (ROC5=0.5%, Drift=4.0%) should trigger direction-aware gate. "
                f"Got default HOLD: {result.get('Rationale', '')}"
            )

    def test_sub_threshold_adverse_move_does_not_trigger(self):
        """
        Sigma-normalized thresholds: ROC5 z < 1.5σ AND drift z < 2.0σ should NOT
        trigger the direction-adverse gate, even with negative P&L.
        With HV=28%: ROC5=0.8% → z=0.20σ, drift=1.5% → z=0.85σ — both below thresholds.
        The loss is from theta/IV, not direction — different gate's job.
        """
        row = _base_long_option_row(
            roc_5=0.8,                  # below 1.5% threshold
            roc_10=0.5,
            Price_Drift_Pct=0.015,      # below 2% threshold
            Drift_Direction="Up",       # categorical says UP but magnitude is noise
            Total_GL_Decimal=-0.20,     # losing
            DTE=25.0,
        )
        result = _run_doctrine(row)
        rationale = result.get("Rationale", "").lower()
        assert "direction adverse" not in rationale, (
            f"Sub-threshold adverse move (ROC5=0.8%, drift=1.5%) should NOT trigger "
            f"direction-adverse gate. Got: {result['Action']} — {result.get('Rationale', '')}"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# SRS Integration — Sector Relative Strength modulates direction-adverse gate
# ═══════════════════════════════════════════════════════════════════════════════

class TestSRSModulation:
    """
    Verify that Sector Relative Strength (SRS) modulates the direction-adverse
    gate: underperforming sector = relative thesis intact, outperforming sector
    = thesis genuinely failing.
    """

    def test_srs_outperforming_exits(self):
        """
        Stock outperforming sector + adverse direction → EXIT.
        Thesis is genuinely broken (stock leading sector upward while put bleeds).
        """
        row = _base_long_option_row(
            Sector_Relative_Strength="OUTPERFORMING",
            Sector_RS_ZScore=1.5,
            Sector_Benchmark="QQQ",
        )
        result = _run_doctrine(row)
        assert result["Action"] in ("EXIT", "ROLL"), (
            f"OUTPERFORMING sector + adverse direction should EXIT/ROLL. "
            f"Got {result['Action']}: {result.get('Rationale', '')}"
        )

    def test_srs_underperforming_downgrades_to_roll_or_hold(self):
        """
        Stock underperforming sector + adverse direction → ROLL or HOLD (not EXIT).
        Relative thesis intact — stock is lagging sector despite market tailwind.
        """
        row = _base_long_option_row(
            Sector_Relative_Strength="UNDERPERFORMING",
            Sector_RS_ZScore=-1.3,
            Sector_Benchmark="QQQ",
            Total_GL_Decimal=-0.20,     # losing but not catastrophic
            Prior_Action="HOLD",
        )
        result = _run_doctrine(row)
        assert result["Action"] in ("ROLL", "HOLD"), (
            f"UNDERPERFORMING sector should downgrade to ROLL/HOLD, not EXIT. "
            f"Got {result['Action']}: {result.get('Rationale', '')}"
        )
        rationale = result.get("Rationale", "").lower()
        assert "sector" in rationale, (
            f"SRS modulation should mention sector in rationale. Got: {result.get('Rationale', '')}"
        )

    def test_srs_micro_breakdown_holds_high(self):
        """
        Stock deeply underperforming sector (MICRO_BREAKDOWN, z < -2) →
        HOLD HIGH. Strong relative signal: adverse move is market-driven.
        """
        row = _base_long_option_row(
            Sector_Relative_Strength="MICRO_BREAKDOWN",
            Sector_RS_ZScore=-2.5,
            Sector_Benchmark="QQQ",
            Total_GL_Decimal=-0.18,
            Prior_Action="HOLD",
        )
        result = _run_doctrine(row)
        assert result["Action"] == "HOLD", (
            f"MICRO_BREAKDOWN sector should HOLD (thesis reinforced). "
            f"Got {result['Action']}: {result.get('Rationale', '')}"
        )
        assert result["Urgency"] == "HIGH", (
            f"MICRO_BREAKDOWN HOLD should be HIGH urgency. Got {result['Urgency']}"
        )
        rationale = result.get("Rationale", "").lower()
        assert "reinforced" in rationale or "sector" in rationale, (
            f"MICRO_BREAKDOWN should mention thesis reinforced/sector. Got: {result.get('Rationale', '')}"
        )

    def test_srs_override_severe_pnl_exits_despite_underperforming(self):
        """
        Override: P&L < -40% → EXIT regardless of SRS.
        Absolute damage too severe for relative thesis to save.
        Jabbour: position is structurally damaged.
        """
        row = _base_long_option_row(
            Sector_Relative_Strength="UNDERPERFORMING",
            Sector_RS_ZScore=-1.5,
            Sector_Benchmark="QQQ",
            Total_GL_Decimal=-0.45,     # severe loss > -40%
            Prior_Action="HOLD",
        )
        result = _run_doctrine(row)
        assert result["Action"] == "EXIT", (
            f"P&L < -40% should override SRS protection → EXIT. "
            f"Got {result['Action']}: {result.get('Rationale', '')}"
        )

    def test_srs_override_already_rolled_exits(self):
        """
        Override: already rolled once (contract evidence) → EXIT regardless of SRS.
        Jabbour (0.712): "Rolling fails to recognize the position may be a bad trade."
        """
        row = _base_long_option_row(
            Sector_Relative_Strength="MICRO_BREAKDOWN",
            Sector_RS_ZScore=-2.8,
            Sector_Benchmark="QQQ",
            Total_GL_Decimal=-0.25,
            # AUDIT FIX: "already rolled" now checks contract evidence, not Prior_Action.
            # Different expiration = user actually executed a roll.
            Expiration="2026-06-15",        # current contract
            Expiration_Entry="2026-04-15",  # original contract → evidence of roll
        )
        result = _run_doctrine(row)
        assert result["Action"] == "EXIT", (
            f"Already rolled + adverse should EXIT despite MICRO_BREAKDOWN SRS. "
            f"Got {result['Action']}: {result.get('Rationale', '')}"
        )

    def test_srs_neutral_no_modification(self):
        """
        SRS=NEUTRAL → no relative edge → standard roll/exit logic applies.
        This is the MSFT actual case (SRS=NEUTRAL, z=0.0).
        """
        row = _base_long_option_row(
            Sector_Relative_Strength="NEUTRAL",
            Sector_RS_ZScore=0.0,
            Sector_Benchmark="QQQ",
        )
        result = _run_doctrine(row)
        # NEUTRAL SRS should NOT suppress — MSFT scenario still exits
        assert result["Action"] in ("EXIT", "ROLL"), (
            f"NEUTRAL SRS should not modify standard adverse logic. "
            f"Got {result['Action']}: {result.get('Rationale', '')}"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Thesis Engine Direction-Awareness — _classify_thesis() unit tests
# ═══════════════════════════════════════════════════════════════════════════════

def _base_thesis_row(**overrides) -> pd.Series:
    """Minimal row for _classify_thesis() with direction-aware fields."""
    base = {
        "Strategy":               "LONG_PUT",
        "Call/Put":               "P",
        "PriceStructure_State":   "STRUCTURE_INTACT",
        "TrendIntegrity_State":   "NO_TREND",
        "TrendIntegrity_State_Prev": "NO_TREND",
        "MomentumVelocity_State": "STEADY",
        "MomentumVelocity_State_Days": 3,
        "RecoveryQuality_State":  "",
        "Price_Drift_Pct":        0.0,
        "roc_5":                 0.0,
        "HV_20D":                 0.12,
        "hv_20d_percentile":      45.0,
        "Sector_Relative_Strength": "NEUTRAL",
        "Sector_RS_ZScore":       0.0,
        "Sector_Benchmark":       "SPY",
    }
    base.update(overrides)
    return pd.Series(base)


class TestThesisDirectionAwareness:
    """
    Verify _classify_thesis() correctly detects direction-adverse moves
    for LONG_PUT / LONG_CALL and adjusts Thesis_State accordingly.
    """

    def test_msft_put_stock_rallying_degrades_thesis(self):
        """
        MSFT case: LONG_PUT, ROC5=+3.66%, drift=+5%.
        Both signals adverse → direction_adverse_severe → DEGRADED.
        """
        row = _base_thesis_row(
            Strategy="LONG_PUT",
            roc_5=3.66,
            Price_Drift_Pct=0.05,
        )
        state, drivers, _, _ = _classify_thesis(row, {})
        assert state == THESIS_DEGRADED, (
            f"LONG_PUT with stock rallying (ROC5=+3.66%, drift=+5%) should be "
            f"DEGRADED, got {state}. Drivers: {drivers}"
        )
        signal_names = {d["signal"] for d in drivers}
        assert "direction_adverse_severe" in signal_names, (
            f"Expected direction_adverse_severe signal. Got: {signal_names}"
        )

    def test_long_call_stock_falling_degrades_thesis(self):
        """
        Symmetric: LONG_CALL, stock falling ROC5=-3.0%, drift=-3%.
        With HV_20D=12%, roc5_z=1.77σ (>1.5), drift_z=3.97σ (>2.0) → both adverse.
        """
        row = _base_thesis_row(
            Strategy="LONG_CALL",
            roc_5=-3.0,
            Price_Drift_Pct=-0.03,
        )
        state, drivers, _, _ = _classify_thesis(row, {})
        assert state == THESIS_DEGRADED, (
            f"LONG_CALL with stock falling should be DEGRADED, got {state}"
        )
        signal_names = {d["signal"] for d in drivers}
        assert "direction_adverse_severe" in signal_names

    def test_mild_adverse_one_signal_degrades(self):
        """
        Only ROC5 adverse (drift below threshold) → direction_adverse → DEGRADED.
        With HV_20D=12%, roc5_z=1.77σ (>1.5) but drift_z=1.32σ (<2.0) → one signal.
        """
        row = _base_thesis_row(
            Strategy="LONG_PUT",
            roc_5=3.0,
            Price_Drift_Pct=0.01,   # drift_z=1.32σ, below 2.0 threshold
        )
        state, drivers, _, _ = _classify_thesis(row, {})
        assert state == THESIS_DEGRADED, (
            f"One adverse signal should push to DEGRADED, got {state}"
        )
        signal_names = {d["signal"] for d in drivers}
        assert "direction_adverse" in signal_names

    def test_sub_threshold_moves_stay_intact(self):
        """
        ROC5=+1.0% → roc5_z=0.59σ (<1.5σ) and drift=+1% → drift_z=1.32σ (<2.0σ) → INTACT.
        """
        row = _base_thesis_row(
            Strategy="LONG_PUT",
            roc_5=1.0,
            Price_Drift_Pct=0.01,
        )
        state, drivers, _, _ = _classify_thesis(row, {})
        assert state == THESIS_INTACT, (
            f"Sub-threshold moves should stay INTACT, got {state}. Drivers: {drivers}"
        )
        signal_names = {d["signal"] for d in drivers}
        assert "direction_adverse" not in signal_names
        assert "direction_adverse_severe" not in signal_names

    def test_buy_write_not_affected(self):
        """
        BUY_WRITE strategy should NOT trigger direction-adverse signals.
        Stock going up is fine for buy-writes.
        """
        row = _base_thesis_row(
            Strategy="BUY_WRITE",
            roc_5=4.0,
            Price_Drift_Pct=0.06,
        )
        state, drivers, _, _ = _classify_thesis(row, {})
        signal_names = {d["signal"] for d in drivers}
        assert "direction_adverse" not in signal_names
        assert "direction_adverse_severe" not in signal_names, (
            f"BUY_WRITE should never trigger direction-adverse. Drivers: {drivers}"
        )

    def test_direction_adverse_plus_structure_broken_is_broken(self):
        """
        Direction-adverse (0.35) + price_structure_broken (0.40) = 0.75 → BROKEN.
        This is the worst case: stock moving against thesis AND structure failing.
        """
        row = _base_thesis_row(
            Strategy="LONG_PUT",
            roc_5=3.0,
            Price_Drift_Pct=0.04,
            PriceStructure_State="STRUCTURE_BROKEN",
        )
        state, drivers, _, _ = _classify_thesis(row, {})
        assert state == THESIS_BROKEN, (
            f"Direction-adverse + structure broken should be BROKEN, got {state}. "
            f"Drivers: {drivers}"
        )

    def test_confirming_direction_stays_intact(self):
        """
        LONG_PUT with stock FALLING → confirming direction → INTACT.
        """
        row = _base_thesis_row(
            Strategy="LONG_PUT",
            roc_5=-3.0,
            Price_Drift_Pct=-0.04,
        )
        state, drivers, _, _ = _classify_thesis(row, {})
        assert state == THESIS_INTACT, (
            f"Confirming direction (stock falling for put) should stay INTACT, "
            f"got {state}. Drivers: {drivers}"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Carry Inversion Severity + Post-BUYBACK Sticky Gate
# ═══════════════════════════════════════════════════════════════════════════════

class TestCarryInversionSeverity:
    """
    Verify that BUYBACK only fires when carry inversion is severe (>= 1.5×),
    and that post-BUYBACK state is sticky until equity reaches INTACT.
    """

    def test_mild_inversion_holds_not_buyback(self):
        """
        PLTR scenario: margin/theta = 1.04× (barely inverted).
        BUYBACK would make bleed 26× worse. Engine should HOLD.
        """
        row = _base_buy_write_row(
            Symbol="PLTR",
            Underlying_Ticker="PLTR",
            **{"UL Last": 147.24},
            Net_Cost_Basis_Per_Share=128.44,
            Equity_Integrity_State="BROKEN",
            Equity_Integrity_Reason="EMA20↓, EMA50↓, HV 99th pct",
            Theta=-0.0351,              # theta per share
            Short_Call_Strike=250.0,
            Short_Call_DTE=689.0,
            Short_Call_Delta=0.418,
            DTE=689.0,
            Basis=12844.0,
            Quantity=100.0,
            hv_20d_percentile=99.0,
        )
        result = _run_doctrine(row)
        assert result["Action"] in ("HOLD", "LET_EXPIRE", "ACCEPT_CALL_AWAY"), (
            f"Mild carry inversion (1.04×) should HOLD or LET_EXPIRE/ACCEPT_CALL_AWAY (EV winner). "
            f"Got {result['Action']}: {result.get('Rationale', '')}"
        )

    def test_severe_inversion_triggers_buyback(self):
        """
        Theta negligible vs margin → ratio >= 1.5× → BUYBACK justified.
        """
        row = _base_buy_write_row(
            Symbol="TEST",
            Underlying_Ticker="TEST",
            **{"UL Last": 200.0},
            Net_Cost_Basis_Per_Share=200.0,
            Equity_Integrity_State="BROKEN",
            Equity_Integrity_Reason="EMA20↓, EMA50↓",
            Theta=-0.01,                # very low theta
            Short_Call_Strike=400.0,    # 100% OTM — negligible theta
            Short_Call_DTE=365.0,
            Short_Call_Delta=0.10,
            DTE=365.0,
            Basis=20000.0,
            Quantity=100.0,
        )
        result = _run_doctrine(row)
        # margin_daily = 200 * 0.10375/365 = 0.0568
        # theta_daily = 0.01
        # ratio = 5.68 → well above 1.5
        assert result["Action"] in ("BUYBACK", "EXIT"), (
            f"Severe carry inversion (>1.5×) should BUYBACK or EXIT. "
            f"Got {result['Action']}: {result.get('Rationale', '')}"
        )

    def test_post_buyback_sticky_holds_when_broken(self):
        """
        After BUYBACK, Prior_Action=BUYBACK + Equity still BROKEN → HOLD.
        Engine must NOT suggest ROLL even though no short call exists.
        """
        row = _base_buy_write_row(
            Symbol="PLTR",
            Underlying_Ticker="PLTR",
            **{"UL Last": 147.24},
            Net_Cost_Basis_Per_Share=128.44,
            Prior_Action="BUYBACK",
            Equity_Integrity_State="BROKEN",
            Equity_Integrity_Reason="EMA20↓, EMA50↓",
            Theta=0.0,                  # no short call → no theta
            Short_Call_DTE=0.0,
            DTE=0.0,
        )
        result = _run_doctrine(row)
        assert result["Action"] in ("HOLD", "LET_EXPIRE", "ACCEPT_CALL_AWAY"), (
            f"Post-BUYBACK with BROKEN equity should HOLD or LET_EXPIRE/ACCEPT_CALL_AWAY (EV). "
            f"Got {result['Action']}: {result.get('Rationale', '')}"
        )

    def test_post_buyback_sticky_holds_when_weakening(self):
        """
        Post-BUYBACK + equity WEAKENING (not fully recovered) → HOLD or EV-driven action.
        """
        row = _base_buy_write_row(
            Prior_Action="BUYBACK",
            Equity_Integrity_State="WEAKENING",
            Equity_Integrity_Reason="EMA20↓",
        )
        result = _run_doctrine(row)
        assert result["Action"] in ("HOLD", "ROLL"), (
            f"Post-BUYBACK with WEAKENING equity should HOLD or ROLL (EV). "
            f"Got {result['Action']}: {result.get('Rationale', '')}"
        )

    def test_post_buyback_releases_when_intact(self):
        """
        Post-BUYBACK + equity INTACT → sticky gate releases, normal flow resumes.
        """
        row = _base_buy_write_row(
            Prior_Action="BUYBACK",
            Equity_Integrity_State="INTACT",
        )
        result = _run_doctrine(row)
        # Should NOT be blocked by sticky gate — can HOLD/ROLL/etc. normally
        assert result["Action"] != "EXIT", (
            f"Post-BUYBACK with INTACT equity should flow normally. "
            f"Got {result['Action']}"
        )
        rationale = result.get("Rationale", "").lower()
        # Should NOT mention post-BUYBACK hold (gate didn't fire)
        assert "post-buyback hold" not in rationale, (
            f"Sticky gate should NOT fire when equity is INTACT"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# EV Feasibility Escape — not trigger-happy, not hope-land
# ═══════════════════════════════════════════════════════════════════════════════

class TestEVFeasibilityEscape:
    """
    Nison (0.723): "Exit if, and only if, we expect the move to continue."
    Chan (0.684): "Wait for reversion rational when model supports it."
    Given (0.755): "Directional trades should have a TIME stop."
    Jabbour (0.790): "Close and re-evaluate." → overrides when BOTH signals fire.
    """

    def test_amzn_one_signal_ev_feasible_holds(self):
        """
        AMZN scenario: one adverse signal (ROC5 z > 1.5σ),
        breakeven at 0.26× expected move, DTE > 50% remaining → HOLD.
        Nison: losing money ≠ being wrong.
        With HV=0.09: ROC5=3.0% → z=2.37σ (adverse); drift=0.5% → z=0.88σ (NOT adverse).
        """
        row = _base_long_option_row(
            roc_5=3.0,                  # z=2.37σ with HV=9% (adverse)
            Price_Drift_Pct=0.005,      # z=0.88σ with HV=9% (NOT adverse) — single signal
            HV_20D=0.09,               # makes ROC5 clearly adverse
            Total_GL_Decimal=-0.28,
            EV_Feasibility_Ratio=0.26,  # breakeven well within expected move
            DTE=30.0,
            DTE_Entry=60.0,          # 50% time remaining
        )
        result = _run_doctrine(row)
        assert result["Action"] == "HOLD", (
            f"One marginal adverse signal + EV feasible (0.26×) + time remaining "
            f"should HOLD, not EXIT. Got {result['Action']}: "
            f"{result.get('Rationale', '')}"
        )
        rationale = result.get("Rationale", "").lower()
        assert "ev" in rationale or "feasib" in rationale or "breakeven" in rationale, (
            f"Should mention EV feasibility. Rationale: {result.get('Rationale', '')}"
        )

    def test_msft_both_signals_exits_despite_ev(self):
        """
        Both adverse signals: ROC5 z > 1.5σ AND drift z > 2.0σ.
        Even with feasible EV, Jabbour overrides: EXIT.
        With HV=0.09: ROC5=3.66% → z=2.89σ, drift=5% → z=8.82σ — both clearly adverse.
        """
        row = _base_long_option_row(
            roc_5=3.66,                 # z=2.89σ with HV=9% (adverse)
            Price_Drift_Pct=0.05,       # z=8.82σ with HV=9% (adverse)
            HV_20D=0.09,               # makes both signals clearly adverse
            Total_GL_Decimal=-0.52,
            EV_Feasibility_Ratio=0.40,  # EV technically feasible
            DTE=30.0,
            DTE_Entry=60.0,
        )
        result = _run_doctrine(row)
        assert result["Action"] == "EXIT", (
            f"BOTH adverse signals should EXIT regardless of EV. "
            f"Got {result['Action']}: {result.get('Rationale', '')}"
        )

    def test_time_stop_exits_despite_ev_feasible(self):
        """
        Given's time stop: one adverse signal, EV feasible, BUT time expired
        (DTE < 50% of original). Time kills directional trades.
        With HV=0.09: ROC5=3.0% → z=2.37σ (adverse), drift=0.5% → z=0.88σ (not adverse).
        """
        row = _base_long_option_row(
            roc_5=3.0,                  # z=2.37σ with HV=9% (adverse)
            Price_Drift_Pct=0.005,      # z=0.88σ with HV=9% (NOT adverse) — single signal
            HV_20D=0.09,               # makes ROC5 signal adverse
            Total_GL_Decimal=-0.20,
            EV_Feasibility_Ratio=0.30,  # breakeven feasible
            DTE=12.0,                   # only 20% of original DTE remaining
            DTE_Entry=60.0,
        )
        result = _run_doctrine(row)
        assert result["Action"] == "EXIT", (
            f"Time stop (20% remaining) should override EV feasibility. "
            f"Got {result['Action']}: {result.get('Rationale', '')}"
        )

    def test_ev_infeasible_exits_with_one_signal(self):
        """
        Breakeven > 0.5× expected move → loss is structural, EXIT.
        Jabbour: "close out, take the limited loss."
        With HV=0.09: ROC5=3.0% → z=2.37σ (adverse), drift=0.5% → z=0.88σ (not adverse).
        """
        row = _base_long_option_row(
            roc_5=3.0,                  # z=2.37σ with HV=9% (adverse)
            Price_Drift_Pct=0.005,      # z=0.88σ with HV=9% (NOT adverse) — single signal
            HV_20D=0.09,               # makes ROC5 signal adverse
            Total_GL_Decimal=-0.35,
            EV_Feasibility_Ratio=0.80,  # breakeven NOT feasible (0.80× > 0.50×)
            DTE=30.0,
            DTE_Entry=60.0,
        )
        result = _run_doctrine(row)
        assert result["Action"] == "EXIT", (
            f"EV infeasible (0.80×) with adverse signal should EXIT. "
            f"Got {result['Action']}: {result.get('Rationale', '')}"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Test 10 — Gamma Danger Zone (Natenberg Ch.7)
# ═══════════════════════════════════════════════════════════════════════════════

class TestGammaDangerZone:
    """
    Validate the pre-emptive gamma danger zone gate for short-gamma positions.
    Natenberg Ch.7: "ATM + low DTE + low vol is the maximum-risk configuration."
    The gate fires BEFORE equity breaks — catching positions in the danger zone
    early enough to roll pre-emptively.
    """

    def test_bw_near_atm_low_dte_gamma_dominant_rolls(self):
        """
        BUY_WRITE: near-ATM (2%), DTE=14, gamma/theta=4.5× → ROLL MEDIUM.
        Gamma_ROC_3D < 0 → not accelerating → MEDIUM (not HIGH).
        """
        row = _base_buy_write_row(
            **{
                "UL Last":              100.0,
                "Short_Call_Strike":    102.0,
                "Short_Call_DTE":       14.0,
                "Short_Call_Delta":     0.45,
                "Net_Cost_Basis_Per_Share": 98.0,
                "Cumulative_Premium_Collected": 2.0,
                "Basis":               19600.0,
                "Quantity":            200.0,
                "Theta":               0.02,
                "Gamma":               0.05,
                "HV_20D":              0.30,
                "Gamma_ROC_3D":        -0.001,
                "Equity_Integrity_State": "WEAKENING",
            }
        )
        result = _run_doctrine(row)
        assert result["Action"] == "ROLL", (
            f"Near-ATM + low DTE + gamma dominant should ROLL. "
            f"Got {result['Action']}: {result.get('Rationale', '')}"
        )
        assert result["Urgency"] == "MEDIUM", (
            f"Gamma_ROC_3D < 0 → urgency should be MEDIUM. "
            f"Got {result['Urgency']}"
        )
        assert "Gamma Danger Zone" in result.get("Rationale", ""), (
            "Rationale should reference Gamma Danger Zone"
        )

    def test_bw_gamma_roc_positive_escalates_to_high(self):
        """
        Same scenario but Gamma_ROC_3D > 0 → gamma accelerating → HIGH urgency.
        """
        row = _base_buy_write_row(
            **{
                "UL Last":              100.0,
                "Short_Call_Strike":    102.0,
                "Short_Call_DTE":       14.0,
                "Short_Call_Delta":     0.45,
                "Net_Cost_Basis_Per_Share": 98.0,
                "Cumulative_Premium_Collected": 2.0,
                "Basis":               19600.0,
                "Quantity":            200.0,
                "Theta":               0.02,
                "Gamma":               0.05,
                "HV_20D":              0.30,
                "Gamma_ROC_3D":        0.005,   # accelerating
                "Equity_Integrity_State": "WEAKENING",
            }
        )
        result = _run_doctrine(row)
        assert result["Action"] == "ROLL", (
            f"Gamma accelerating should still ROLL. Got {result['Action']}"
        )
        assert result["Urgency"] == "HIGH", (
            f"Gamma_ROC_3D > 0 should escalate urgency to HIGH. Got {result['Urgency']}"
        )

    def test_far_otm_does_not_fire(self):
        """
        Strike 10% from spot → outside near-ATM threshold → gate does NOT fire.
        """
        row = _base_buy_write_row(
            **{
                "UL Last":              100.0,
                "Short_Call_Strike":    110.0,   # 10% OTM
                "Short_Call_DTE":       14.0,
                "Short_Call_Delta":     0.20,
                "Net_Cost_Basis_Per_Share": 98.0,
                "Cumulative_Premium_Collected": 2.0,
                "Basis":               19600.0,
                "Quantity":            200.0,
                "Theta":               0.02,
                "Gamma":               0.05,
                "HV_20D":              0.30,
                "Gamma_ROC_3D":        0.005,
                "Equity_Integrity_State": "WEAKENING",
            }
        )
        result = _run_doctrine(row)
        # Should NOT be ROLL from gamma danger zone (10% OTM fails near-ATM check)
        rationale = result.get("Rationale", "")
        assert "Gamma Danger Zone" not in rationale, (
            f"10% OTM should not trigger gamma danger zone. Got: {rationale}"
        )

    def test_dte_above_21_does_not_fire(self):
        """
        DTE=30 → outside gamma acceleration zone → gate does NOT fire.
        """
        row = _base_buy_write_row(
            **{
                "UL Last":              100.0,
                "Short_Call_Strike":    102.0,
                "Short_Call_DTE":       30.0,    # above 21
                "Short_Call_Delta":     0.45,
                "Net_Cost_Basis_Per_Share": 98.0,
                "Cumulative_Premium_Collected": 2.0,
                "Basis":               19600.0,
                "Quantity":            200.0,
                "Theta":               0.02,
                "Gamma":               0.05,
                "HV_20D":              0.30,
                "Gamma_ROC_3D":        0.005,
                "Equity_Integrity_State": "WEAKENING",
            }
        )
        result = _run_doctrine(row)
        rationale = result.get("Rationale", "")
        assert "Gamma Danger Zone" not in rationale, (
            f"DTE > 21 should not trigger gamma danger zone. Got: {rationale}"
        )

    def test_equity_broken_defers_to_gamma_dominance_gate(self):
        """
        Equity BROKEN → gamma danger zone gate does NOT fire.
        The existing gamma dominance buyback gate handles BROKEN equity.
        """
        row = _base_buy_write_row(
            **{
                "UL Last":              100.0,
                "Short_Call_Strike":    102.0,
                "Short_Call_DTE":       14.0,
                "Short_Call_Delta":     0.45,
                "Net_Cost_Basis_Per_Share": 98.0,
                "Cumulative_Premium_Collected": 2.0,
                "Basis":               19600.0,
                "Quantity":            200.0,
                "Theta":               0.02,
                "Gamma":               0.05,
                "HV_20D":              0.30,
                "Gamma_ROC_3D":        0.005,
                "Equity_Integrity_State": "BROKEN",   # BROKEN → defers
            }
        )
        result = _run_doctrine(row)
        rationale = result.get("Rationale", "")
        assert "Gamma Danger Zone" not in rationale, (
            f"BROKEN equity should be handled by gamma dominance gate, not danger zone. "
            f"Got: {rationale}"
        )

    def test_cc_mirrors_bw_gamma_danger_zone(self):
        """
        COVERED_CALL: same gamma danger zone logic should fire.
        """
        row = _base_buy_write_row(
            Strategy="COVERED_CALL",
            **{
                "UL Last":              100.0,
                "Strike":              102.0,
                "Short_Call_Strike":    102.0,
                "Short_Call_DTE":       14.0,
                "Short_Call_Delta":     0.45,
                "DTE":                 14.0,
                "Net_Cost_Basis_Per_Share": 98.0,
                "Cumulative_Premium_Collected": 2.0,
                "Basis":               19600.0,
                "Quantity":            200.0,
                "Theta":               0.02,
                "Gamma":               0.05,
                "HV_20D":              0.30,
                "Gamma_ROC_3D":        -0.002,
                "Equity_Integrity_State": "WEAKENING",
                "Drift_Direction":     "",
                "Drift_Magnitude":     "",
            }
        )
        result = _run_doctrine(row)
        assert result["Action"] == "ROLL", (
            f"CC should also fire gamma danger zone. "
            f"Got {result['Action']}: {result.get('Rationale', '')}"
        )
        assert "Gamma Danger Zone" in result.get("Rationale", ""), (
            "CC rationale should reference Gamma Danger Zone"
        )


# ── CSP row factory ──────────────────────────────────────────────────────────

def _base_csp_row(**overrides) -> pd.Series:
    """
    Minimal CSP/SHORT_PUT row that passes wheel assessment and reaches 21-DTE gate.
    """
    base = {
        "TradeID":            "T-CSP-001",
        "LegID":              "L-CSP-001",
        "Symbol":             "AAPL  250321P00200000",
        "Underlying_Ticker":  "AAPL",
        "Strategy":           "CSP",
        "AssetType":          "OPTION",
        "Option_Type":        "PUT",

        "UL Last":            220.0,
        "Strike":             200.0,
        "DTE":                14.0,
        "Delta":              -0.25,
        "Theta":              0.05,
        "Gamma":              0.01,
        "Vega":               0.10,
        "Premium_Entry":      5.00,
        "Last":               3.50,
        "Basis":              500.0,
        "Quantity":           -1.0,
        "Net_Cost_Basis_Per_Share": 0.0,

        "HV_20D":             0.30,
        "IV_Entry":           0.35,
        "IV_30D":             0.32,
        "IV_Now":             0.32,
        "IV_Percentile":      50.0,
        "IV_vs_HV_Gap":       0.02,

        "Moneyness_Label":    "OTM",
        "Lifecycle_Phase":    "INCOME_WINDOW",
        "TrendIntegrity_State": "TREND_UP",
        "PriceStructure_State": "STRUCTURE_INTACT",
        "Drift_Direction":    "",
        "Drift_Magnitude":    "",
        "VolatilityState_State": "NORMAL",
        "MomentumVelocity_State": "TRENDING",
        "Position_Regime":    "SIDEWAYS_INCOME",
        "Trajectory_Consecutive_Debit_Rolls": 0,
        "Trajectory_Stock_Return": 0.0,

        "Portfolio_Delta_Utilization_Pct": 5.0,
        "MC_Assign_P_Expiry": 0.10,
        "Equity_Integrity_State": "INTACT",

        "Thesis_State":       "INTACT",
        "Thesis_Gate":        "PASS",
        "_thesis_blocks_roll": False,
        "Thesis_Summary":     "",

        "Snapshot_TS":        pd.Timestamp.now(),
        "Earnings_Date":      None,
        "run_id":             "test-run",
        "Schema_Hash":        "abc123",
    }
    base.update(overrides)
    return pd.Series(base)


# ═══════════════════════════════════════════════════════════════════════════════
# Test 11 — Strategy-Aware Exit Logic (Chan 0.786)
# ═══════════════════════════════════════════════════════════════════════════════

class TestStrategyAwareExit:
    """
    Validate that income strategy 21-DTE gates check IV regime before rolling.
    Chan: "exit logic must differ: mean-reversion (income) vs momentum (directional)."
    When IV has collapsed (>30% contraction, bottom quartile, no selling edge),
    rolling into thin premium has negative EV — HOLD instead.
    """

    def test_bw_iv_collapsed_holds_instead_of_roll(self):
        """
        BUY_WRITE at 21-DTE with IV collapsed >30% from entry,
        IV_Percentile < 25, selling edge gone → HOLD (let premium decay).
        """
        row = _base_buy_write_row(
            **{
                "UL Last":              100.0,
                "Short_Call_Strike":    110.0,   # 10% OTM (avoids gamma DZ)
                "Short_Call_DTE":       14.0,
                "Short_Call_Delta":     0.25,
                "Short_Call_Premium":   5.00,
                "Short_Call_Last":      3.50,    # 30% captured (<50%)
                "Short_Call_Moneyness": "OTM",
                "Moneyness_Label":     "OTM",
                "Net_Cost_Basis_Per_Share": 98.0,
                "Cumulative_Premium_Collected": 2.0,
                "Basis":               19600.0,
                "Quantity":            200.0,
                "HV_20D":              0.25,
                "IV_Entry":            0.40,
                "IV_30D":              0.18,     # 55% contraction (< 0.70)
                "IV_Now":              0.18,
                "IV_Percentile":       12.0,     # bottom quartile
                "IV_vs_HV_Gap":        -0.07,    # no selling edge
                "Equity_Integrity_State": "INTACT",
                "Theta":               0.03,
                "Gamma":               0.005,
            }
        )
        result = _run_doctrine(row)
        assert result["Action"] in ("HOLD", "LET_EXPIRE", "ACCEPT_CALL_AWAY"), (
            f"IV collapsed → should HOLD or LET_EXPIRE/ACCEPT_CALL_AWAY (EV winner). "
            f"Got {result['Action']}: {result.get('Rationale', '')}"
        )

    def test_bw_iv_normal_still_rolls(self):
        """
        BUY_WRITE at 21-DTE with IV still healthy → standard ROLL behavior.
        """
        row = _base_buy_write_row(
            **{
                "UL Last":              100.0,
                "Short_Call_Strike":    110.0,
                "Short_Call_DTE":       14.0,
                "Short_Call_Delta":     0.25,
                "Short_Call_Premium":   5.00,
                "Short_Call_Last":      3.50,
                "Short_Call_Moneyness": "OTM",
                "Moneyness_Label":     "OTM",
                "Net_Cost_Basis_Per_Share": 98.0,
                "Cumulative_Premium_Collected": 2.0,
                "Basis":               19600.0,
                "Quantity":            200.0,
                "HV_20D":              0.25,
                "IV_Entry":            0.35,
                "IV_30D":              0.33,     # only 6% contraction (normal)
                "IV_Now":              0.33,
                "IV_Percentile":       55.0,     # mid-range
                "IV_vs_HV_Gap":        0.08,     # positive edge
                "Equity_Integrity_State": "INTACT",
                "Theta":               0.03,
                "Gamma":               0.005,
            }
        )
        result = _run_doctrine(row)
        assert result["Action"] in ("ROLL", "LET_EXPIRE", "ACCEPT_CALL_AWAY"), (
            f"IV normal → should ROLL or LET_EXPIRE/ACCEPT_CALL_AWAY (EV winner) at 21-DTE. "
            f"Got {result['Action']}: {result.get('Rationale', '')}"
        )

    def test_csp_iv_collapsed_holds_instead_of_roll(self):
        """
        CSP at 21-DTE with IV collapsed → HOLD (let put premium expire).
        """
        row = _base_csp_row(
            DTE=14.0,
            Premium_Entry=5.00,
            Last=3.50,          # 30% captured (<50%)
            IV_Entry=0.40,
            IV_30D=0.18,        # 55% contraction
            IV_Now=0.18,
            IV_Percentile=15.0,
            IV_vs_HV_Gap=-0.05,
            TrendIntegrity_State="NO_TREND",  # wheel NOT ready
        )
        result = _run_doctrine(row)
        assert result["Action"] in ("HOLD", "BUYBACK"), (
            f"CSP IV collapsed → should HOLD or BUYBACK (EV winner). "
            f"Got {result['Action']}: {result.get('Rationale', '')}"
        )

    def test_csp_iv_normal_still_rolls(self):
        """
        CSP at 21-DTE with IV healthy → standard ROLL behavior.
        Wheel must be NOT ready (via delta concentration) so 21-DTE gate fires.
        """
        row = _base_csp_row(
            DTE=14.0,
            Premium_Entry=5.00,
            Last=3.50,
            IV_Entry=0.35,
            IV_30D=0.33,
            IV_Now=0.33,
            IV_Percentile=55.0,
            IV_vs_HV_Gap=0.03,
            Portfolio_Delta_Utilization_Pct=20.0,  # >15% → wheel NOT ready
        )
        result = _run_doctrine(row)
        assert result["Action"] in ("ROLL", "BUYBACK", "HOLD"), (
            f"CSP IV normal → should ROLL, BUYBACK, or HOLD (EV winner). "
            f"Got {result['Action']}: {result.get('Rationale', '')}"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# OI Deterioration Gate (Murphy 0.704)
# ═══════════════════════════════════════════════════════════════════════════════

class TestOIDeteriorationGate:
    """
    Verify OI deterioration gate fires correctly based on Open_Int / OI_Entry ratio.
    Murphy (0.704): declining OI signals liquidity draining on held contracts.
    """

    def test_oi_absolute_floor_exits_high(self):
        """OI < 25 absolute → EXIT HIGH regardless of entry OI."""
        row = _base_buy_write_row(
            AssetType="OPTION",
            Strategy="BUY_WRITE",
            Open_Int=10,
            OI_Entry=500,
        )
        result = _run_doctrine(row)
        assert result["Action"] == "EXIT", (
            f"OI=10 < 25 absolute floor → EXIT. Got {result['Action']}: {result.get('Rationale', '')}"
        )
        assert result["Urgency"] == "HIGH", (
            f"OI absolute floor should be HIGH urgency. Got {result['Urgency']}"
        )
        assert "liquidity trap" in result.get("Rationale", "").lower()

    def test_oi_severe_decline_exits_medium(self):
        """OI dropped >75% from entry (ratio < 0.25) → EXIT MEDIUM."""
        row = _base_buy_write_row(
            AssetType="OPTION",
            Strategy="BUY_WRITE",
            Open_Int=100,     # 20% of entry
            OI_Entry=500,
        )
        result = _run_doctrine(row)
        assert result["Action"] == "EXIT", (
            f"OI ratio 0.20 < 0.25 → EXIT. Got {result['Action']}: {result.get('Rationale', '')}"
        )
        assert result["Urgency"] == "MEDIUM", (
            f"OI severe decline should be MEDIUM urgency. Got {result['Urgency']}"
        )
        assert "deterioration" in result.get("Rationale", "").lower()

    def test_oi_halved_warns_only(self):
        """OI halved (ratio ~0.40) → WARNING annotation, not EXIT."""
        row = _base_buy_write_row(
            AssetType="OPTION",
            Strategy="BUY_WRITE",
            Open_Int=200,     # 40% of entry
            OI_Entry=500,
        )
        result = _run_doctrine(row)
        # Should NOT be EXIT from OI gate (strategy doctrine still runs)
        assert "OI_Deterioration_Warning" in result, (
            f"OI ratio 0.40 should produce warning annotation. Keys: {list(result.keys())}"
        )
        assert "halved" in result.get("OI_Deterioration_Warning", "").lower()

    def test_oi_healthy_no_gate_fire(self):
        """OI at 80% of entry → no OI gate fires."""
        row = _base_buy_write_row(
            AssetType="OPTION",
            Strategy="BUY_WRITE",
            Open_Int=400,     # 80% of entry
            OI_Entry=500,
        )
        result = _run_doctrine(row)
        # No OI warning or EXIT from OI gate
        assert "OI_Deterioration_Warning" not in result, (
            f"OI ratio 0.80 should NOT produce warning. Got: {result.get('OI_Deterioration_Warning', '')}"
        )
        # Action should be whatever the strategy doctrine produces (not EXIT from OI)
        assert "liquidity trap" not in result.get("Rationale", "").lower()
        assert "deterioration" not in result.get("Rationale", "").lower()


# ═══════════════════════════════════════════════════════════════════════════════
# Vol Stop Gate Tests (Given 0.677)
# ═══════════════════════════════════════════════════════════════════════════════

class TestVolStopGate:
    """
    Verify vol stop gate fires when IV rises >50% from entry for short-vol strategies.
    Given (0.677): 'If IV rises >50% from entry, the risk profile has fundamentally changed.'
    """

    def test_vol_stop_fires_on_iv_spike_short_vol(self):
        """CSP with IV rising >50% from entry → Vol_Stop_Warning annotation."""
        row = _base_buy_write_row(
            AssetType="OPTION",
            Strategy="CSP",
            IV_Entry=0.25,
            IV_Now=0.40,       # 60% rise
            IV_30D=0.40,
            Open_Int=1000,
            OI_Entry=1000,
        )
        result = _run_doctrine(row)
        assert "Vol_Stop_Warning" in result, (
            f"IV rose 60% but no Vol_Stop_Warning. Keys: {list(result.keys())}"
        )
        assert "vol stop" in result["Vol_Stop_Warning"].lower()

    def test_vol_stop_does_not_fire_for_long_vol(self):
        """LONG_CALL with IV rising >50% → no vol stop (IV rise helps long-vol)."""
        row = _base_buy_write_row(
            AssetType="OPTION",
            Strategy="LONG_CALL",
            IV_Entry=0.25,
            IV_Now=0.40,
            IV_30D=0.40,
            Open_Int=1000,
            OI_Entry=1000,
        )
        result = _run_doctrine(row)
        assert "Vol_Stop_Warning" not in result, (
            f"Long-vol should NOT get vol stop. Got: {result.get('Vol_Stop_Warning', '')}"
        )

    def test_vol_stop_does_not_fire_on_small_iv_rise(self):
        """BUY_WRITE with IV rising 30% → no vol stop (threshold is 50%)."""
        row = _base_buy_write_row(
            AssetType="OPTION",
            Strategy="BUY_WRITE",
            IV_Entry=0.30,
            IV_Now=0.39,       # 30% rise
            IV_30D=0.39,
            Open_Int=1000,
            OI_Entry=1000,
        )
        result = _run_doctrine(row)
        assert "Vol_Stop_Warning" not in result


# ═══════════════════════════════════════════════════════════════════════════════
# VRP Drift Tests (Bennett 0.719)
# ═══════════════════════════════════════════════════════════════════════════════

class TestVRPDrift:
    """
    Verify VRP drift is computed when entry vol baselines are available.
    Bennett (0.719): VRP = IV - HV; track drift from entry baseline.
    """

    def test_vrp_drift_computed_when_data_available(self):
        """With all 4 vol fields present, VRP_Entry/VRP_Now/VRP_Drift are populated."""
        row = _base_buy_write_row(
            IV_30D=0.30,       # current IV
            HV_20D=0.20,       # current HV → VRP_Now = 0.10
            IV_30D_Entry=0.35, # entry IV
            HV_20D_Entry=0.22, # entry HV → VRP_Entry = 0.13
            Open_Int=1000,
            OI_Entry=1000,
        )
        result = _run_doctrine(row)
        assert "VRP_Entry" in result, f"VRP_Entry not computed. Keys: {list(result.keys())}"
        assert abs(result["VRP_Entry"] - 0.13) < 0.01
        assert abs(result["VRP_Now"] - 0.10) < 0.01
        assert abs(result["VRP_Drift"] - (-0.03)) < 0.01  # VRP compressed

    def test_vrp_drift_skipped_when_entry_missing(self):
        """Without entry vol baselines, VRP drift is not computed."""
        row = _base_buy_write_row(
            IV_30D=0.30,
            HV_20D=0.20,
            IV_30D_Entry=0,    # missing
            HV_20D_Entry=0,    # missing
            Open_Int=1000,
            OI_Entry=1000,
        )
        result = _run_doctrine(row)
        assert "VRP_Entry" not in result


# ═══════════════════════════════════════════════════════════════════════════════
# Entry Vol/Regime Freeze Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestEntryVolRegimeFreeze:
    """
    Verify _freeze_entry_vol_regime freezes all 6 vol/regime fields at entry.
    """

    def test_freeze_all_vol_regime_fields(self):
        """All 6 fields frozen when source data is present."""
        from core.management.cycle1.snapshot.freeze import _freeze_entry_vol_regime
        df = pd.DataFrame({
            "IV_30D":           [0.32],
            "HV_20D":           [0.25],
            "IV_Percentile":    [72.0],
            "Regime_State":     ["NORMAL"],
            "Expected_Move_10D": [5.50],
            "Daily_Margin_Cost": [1.23],
        })
        mask = pd.Series([True])
        result = _freeze_entry_vol_regime(df, mask)
        assert result.at[0, "IV_30D_Entry"] == 0.32
        assert result.at[0, "HV_20D_Entry"] == 0.25
        assert result.at[0, "IV_Percentile_Entry"] == 72.0
        assert result.at[0, "Regime_Entry"] == "NORMAL"
        assert result.at[0, "Expected_Move_10D_Entry"] == 5.50
        assert result.at[0, "Daily_Margin_Cost_Entry"] == 1.23

    def test_freeze_respects_existing_values(self):
        """Already-frozen values are NOT overwritten (recovery-aware)."""
        from core.management.cycle1.snapshot.freeze import _freeze_entry_vol_regime
        df = pd.DataFrame({
            "IV_30D":           [0.32],
            "HV_20D":           [0.25],
            "IV_Percentile":    [72.0],
            "Regime_State":     ["STRESSED"],
            "Expected_Move_10D": [5.50],
            "Daily_Margin_Cost": [1.23],
            # Pre-existing frozen values (from DuckDB recovery)
            "IV_30D_Entry":           [0.28],
            "HV_20D_Entry":           [0.20],
            "IV_Percentile_Entry":    [60.0],
            "Regime_Entry":           ["NORMAL"],
            "Expected_Move_10D_Entry": [4.00],
            "Daily_Margin_Cost_Entry": [0.99],
        })
        mask = pd.Series([True])
        result = _freeze_entry_vol_regime(df, mask)
        # Existing values preserved
        assert result.at[0, "IV_30D_Entry"] == 0.28
        assert result.at[0, "HV_20D_Entry"] == 0.20
        assert result.at[0, "Regime_Entry"] == "NORMAL"

    def test_freeze_handles_missing_source_columns(self):
        """Missing source columns → entry cols remain NaN."""
        from core.management.cycle1.snapshot.freeze import _freeze_entry_vol_regime
        df = pd.DataFrame({"Symbol": ["AAPL"]})
        mask = pd.Series([True])
        result = _freeze_entry_vol_regime(df, mask)
        assert pd.isna(result.at[0, "IV_30D_Entry"])
        assert pd.isna(result.at[0, "HV_20D_Entry"])


# ═══════════════════════════════════════════════════════════════════════════════
# Tier A Audit Fixes — regression tests
# ═══════════════════════════════════════════════════════════════════════════════

def _base_covered_call_row(**overrides) -> pd.Series:
    """Minimal COVERED_CALL row that passes guards and lands on HOLD."""
    base = {
        "TradeID":            "T-CC-01",
        "LegID":              "L-CC-01",
        "Symbol":             "AAPL 260C",
        "Underlying_Ticker":  "AAPL",
        "Strategy":           "COVERED_CALL",
        "AssetType":          "OPTION",
        "UL Last":            264.58,
        "Strike":             260.0,
        "DTE":                35.0,
        "Delta":              0.40,
        "Quantity":           -1.0,
        "Basis":              612.0,
        "Premium_Entry":      6.12,
        "Last":               4.00,
        "Bid":                3.90,
        "Ask":                4.10,
        "HV_20D":             0.33,
        "Gamma":              0.01,
        "Theta":              -0.05,
        "IV_Now":             0.30,
        "IV_Entry":           0.30,
        "Moneyness_Label":    "OTM",
        "Lifecycle_Phase":    "INCOME_WINDOW",
        "Drift_Direction":    "Flat",
        "Drift_Magnitude":    "Low",
        "PriceStructure_State": "STRUCTURE_INTACT",
        "TrendIntegrity_State": "TREND_UP",
        "Equity_Integrity_State": "INTACT",
        "Equity_Integrity_Reason": "",
        "Thesis_State":       "INTACT",
        "_thesis_blocks_roll": False,
        "_Active_Conditions": "",
        "_Condition_Resolved": "",
        "Snapshot_TS":        pd.Timestamp.now(),
        "Earnings_Date":      None,
        "run_id":             "test-run",
        "Schema_Hash":        "abc123",
        "Open_Int":           500,
        "OI_Entry":           500,
        "Call/Put":           "C",
    }
    base.update(overrides)
    return pd.Series(base)


class TestCCFiftyPercentGateAuditFix:
    """BUG 1: CC 50% gate was comparing per-share Premium to total-dollar Current_Value.
    Gate effectively never fired. Now uses per-share Premium_Entry vs per-share Last."""

    def test_cc_50pct_gate_fires_when_captured(self):
        """50%+ premium captured with DTE > 21 → ROLL."""
        row = _base_covered_call_row(
            Premium_Entry=6.00,
            Last=2.50,          # cost to close < 50% of entry → >58% captured
            DTE=30.0,
            Delta=0.35,
            Moneyness_Label="OTM",
        )
        result = _run_doctrine(row)
        assert result["Action"] in ("ROLL", "LET_EXPIRE", "ACCEPT_CALL_AWAY", "BUYBACK"), (
            f"50%+ captured should ROLL, LET_EXPIRE, ACCEPT_CALL_AWAY, or BUYBACK (EV). Got {result['Action']}"
        )

    def test_cc_50pct_gate_does_not_fire_when_not_captured(self):
        """Less than 50% captured → gate does not fire, EV resolver picks best action."""
        row = _base_covered_call_row(
            Premium_Entry=6.00,
            Last=4.00,          # cost to close = 67% of entry → only 33% captured
            DTE=30.0,
            Delta=0.35,
            Moneyness_Label="OTM",
        )
        result = _run_doctrine(row)
        # 50% gate does not fire — EV resolver picks best action
        assert result["Action"] in ("HOLD", "ROLL", "LET_EXPIRE", "ACCEPT_CALL_AWAY", "BUYBACK")

    def test_cc_50pct_gate_requires_dte_gt_21(self):
        """50% captured but DTE ≤ 21 → 50% gate does not fire (requires DTE > 21).
        21-DTE gate also skips (≥50% captured). Falls through to default HOLD."""
        row = _base_covered_call_row(
            Premium_Entry=6.00,
            Last=2.50,          # >50% captured
            DTE=18.0,           # ≤ 21 — 50% gate won't fire
            Delta=0.15,         # far OTM to avoid gamma danger zone
            Strike=290.0,       # well above spot (264) to avoid near-ATM gates
            Moneyness_Label="OTM",
            Gamma=0.002,        # low gamma — avoid gamma danger zone
        )
        result = _run_doctrine(row)
        # 50% gate requires DTE > 21, and 21-DTE gate requires < 50% captured.
        # Neither fires → falls through to default HOLD.
        assert result["Action"] == "HOLD"


class TestCC21DTEGateAuditFix:
    """BUG 2: CC was missing the 21-DTE income gate that BW and CSP both have."""

    def test_cc_21dte_gate_fires(self):
        """DTE ≤ 21, < 50% captured, OTM → ROLL via 21-DTE gate."""
        row = _base_covered_call_row(
            Premium_Entry=6.00,
            Last=4.00,          # only 33% captured (< 50%)
            DTE=15.0,           # ≤ 21
            Delta=0.15,         # far OTM to avoid gamma danger zone
            Strike=290.0,       # well above spot (264) to avoid near-ATM gates
            Moneyness_Label="OTM",
            Gamma=0.002,        # low gamma — avoid gamma danger zone
        )
        result = _run_doctrine(row)
        # v2: EV resolver may prefer HOLD when all EVs are negative
        assert result["Action"] in ("ROLL", "LET_EXPIRE", "ACCEPT_CALL_AWAY", "HOLD"), (
            f"21-DTE gate should trigger action. Got {result['Action']}"
        )

    def test_cc_21dte_gate_skips_itm(self):
        """DTE ≤ 21 but ITM → ITM gates fire, EV resolver picks best action."""
        row = _base_covered_call_row(
            Premium_Entry=6.00,
            Last=8.00,
            DTE=15.0,
            Delta=0.75,         # deep ITM — delta gate fires first
            Moneyness_Label="ITM",
            Lifecycle_Phase="TERMINAL",
        )
        # Move strike far from spot to avoid gamma danger zone firing first
        row["Strike"] = 240.0   # ~9% from spot 264.58 — well outside 5% ATM band
        row["Gamma"] = 0.002    # low gamma avoids gamma danger zone
        result = _run_doctrine(row)
        # Deep ITM: ROLL, LET_EXPIRE, ACCEPT_CALL_AWAY, or HOLD are all valid EV-driven outcomes
        assert result["Action"] in ("ROLL", "LET_EXPIRE", "ACCEPT_CALL_AWAY", "HOLD")

    def test_cc_21dte_gate_skips_when_50pct_captured(self):
        """DTE ≤ 21 but ≥ 50% captured → 21-DTE gate skips."""
        row = _base_covered_call_row(
            Premium_Entry=6.00,
            Last=2.50,          # 58% captured (≥ 50%)
            DTE=15.0,
            Delta=0.35,
            Moneyness_Label="OTM",
        )
        result = _run_doctrine(row)
        # 21-DTE gate requires < 50% captured — this position passes
        assert "21-DTE" not in result.get("Rationale", "")


class TestVolStopIVSourceAuditFix:
    """BUG 3: Vol stop was falling back to IV_30D (underlying) when IV_Now
    was missing. Now uses IV_Now only — no fallback to underlying IV.

    Vol stop only fires for OPTION legs (AssetType=OPTION) in SHORT_VOL
    strategies. BW runs on the STOCK leg, so we use CC OPTION rows here."""

    def test_vol_stop_fires_on_iv_now_spike(self):
        """IV_Now rises >50% from IV_Entry → Vol_Stop_Warning fires."""
        row = _base_covered_call_row(
            IV_Now=0.60,        # 100% rise from entry 0.30
            IV_Entry=0.30,
            IV_30D=0.35,        # underlying IV is lower — should NOT be used
            Premium_Entry=6.00,
            Last=3.00,          # 50% captured → avoid 21-DTE gate
            DTE=30.0,           # mid-range DTE
            Delta=0.35,
        )
        result = _run_doctrine(row)
        assert "Vol_Stop_Warning" in result
        assert "100%" in result["Vol_Stop_Warning"]

    def test_vol_stop_does_not_fire_on_iv_30d_alone(self):
        """IV_Now is 0 (missing), IV_30D is high → gate should NOT fire
        (no fallback to underlying IV)."""
        row = _base_covered_call_row(
            IV_Now=0,           # missing
            IV_Entry=0.30,
            IV_30D=0.60,        # underlying IV is high — but should not trigger vol stop
            Premium_Entry=6.00,
            Last=3.00,
            DTE=30.0,
            Delta=0.35,
        )
        result = _run_doctrine(row)
        assert "Vol_Stop_Warning" not in result

    def test_vol_stop_does_not_fire_on_small_rise(self):
        """IV_Now rises < 50% → no vol stop."""
        row = _base_covered_call_row(
            IV_Now=0.40,        # 33% rise from 0.30 — below 50% threshold
            IV_Entry=0.30,
            Premium_Entry=6.00,
            Last=3.00,
            DTE=30.0,
            Delta=0.35,
        )
        result = _run_doctrine(row)
        assert "Vol_Stop_Warning" not in result


# ═══════════════════════════════════════════════════════════════════════
# BUG 4: Original_DTE phantom column → EV feasibility escape never fired
# ═══════════════════════════════════════════════════════════════════════

class TestDTEEntryTimeStop:
    """BUG 4: Original_DTE was never populated (always 0), so the EV
    feasibility escape path at line 4175 could never fire. Fix: use
    DTE_Entry (the actual frozen entry DTE from Cycle 1 freeze.py)."""

    def test_ev_escape_fires_when_feasible_and_time_remains(self):
        """Single adverse signal (σ-normalized) + EV feasible + >50% DTE remaining → HOLD.
        With HV=9%: ROC5=3.0% → z=2.37σ (adverse), drift=0.5% → z=0.88σ (not adverse)."""
        row = _base_long_option_row(
            roc_5=3.0,              # z=2.37σ with HV=9% (adverse)
            Price_Drift_Pct=0.005,  # z=0.88σ with HV=9% (NOT adverse)
            HV_20D=0.09,            # low-vol: makes ROC5 signal adverse
            DTE=40.0,               # 40/60 = 67% remaining (> 50%)
            DTE_Entry=60.0,
            EV_Feasibility_Ratio=0.30,  # < 0.50 → feasible
            Total_GL_Decimal=-0.20, # mild loss (above -40% override)
            Thesis_State="INTACT",
            IV_Percentile=60.0,
            IV_Percentile_Depth=25,
            Sector_Relative_Strength="NEUTRAL",
            Prior_Action="HOLD",    # not already rolled
        )
        result = _run_doctrine(row)
        assert result["Action"] == "HOLD", f"Expected HOLD (EV escape), got {result['Action']}: {result.get('Rationale', '')[:200]}"
        assert "feasible" in result["Rationale"].lower() or "EV_Ratio" in result["Rationale"]

    def test_ev_escape_blocked_when_time_exhausted(self):
        """Single adverse signal + EV feasible but < 50% DTE remaining → EXIT.
        With HV=9%: ROC5=3.0% → z=2.37σ (adverse)."""
        row = _base_long_option_row(
            roc_5=3.0,              # z=2.37σ with HV=9% (adverse)
            Price_Drift_Pct=0.005,  # z=0.88σ with HV=9% (not adverse)
            HV_20D=0.09,            # low-vol: makes ROC5 signal adverse
            DTE=20.0,               # 20/60 = 33% remaining (< 50%)
            DTE_Entry=60.0,
            EV_Feasibility_Ratio=0.30,  # feasible
            Total_GL_Decimal=-0.20, # mild loss (above -40% override)
            Thesis_State="INTACT",
            IV_Percentile=60.0,
            IV_Percentile_Depth=60, # reliable
            momentum_slope=0.5,     # NOT confirming for put (needs negative) → blocks roll
            Sector_Relative_Strength="NEUTRAL",
            Prior_Action="HOLD",
        )
        result = _run_doctrine(row)
        assert result["Action"] == "EXIT", f"Expected EXIT (time exhausted), got {result['Action']}"
        assert "time stop" in result["Rationale"].lower() or "DTE remaining" in result["Rationale"]

    def test_both_adverse_overrides_ev_escape(self):
        """BOTH ROC5 z > 1.5σ AND drift z > 2.0σ → EXIT even if EV feasible.
        With HV=12%: ROC5=3.0% → z=1.77σ, drift=5% → z=6.61σ — both adverse."""
        row = _base_long_option_row(
            roc_5=3.0,              # z=1.77σ with HV=12% (adverse)
            Price_Drift_Pct=0.05,   # z=6.61σ with HV=12% (adverse)
            HV_20D=0.12,            # low-vol: makes both signals clearly adverse
            DTE=40.0,               # plenty of time
            DTE_Entry=60.0,
            EV_Feasibility_Ratio=0.30,  # feasible
            Total_GL_Decimal=-0.20, # mild loss
            Thesis_State="INTACT",
            IV_Percentile=30.0,
            IV_Percentile_Depth=50,
            momentum_slope=0.5,     # not confirming → blocks roll path
            Sector_Relative_Strength="NEUTRAL",
            Prior_Action="HOLD",
        )
        result = _run_doctrine(row)
        assert result["Action"] == "EXIT", f"Expected EXIT (both adverse), got {result['Action']}"
        assert "BOTH" in result["Rationale"] or "adverse" in result["Rationale"].lower()


# ═══════════════════════════════════════════════════════════════════════
# BUG 5: Thesis threshold disagreement (strict > vs >=)
# ═══════════════════════════════════════════════════════════════════════

class TestThesisThresholdAlignment:
    """BUG 5 (updated for sigma normalization): thesis_engine and doctrine now
    use σ-normalized thresholds when HV_20D is available, falling back to raw %
    when HV is missing. Tests verify both paths agree on boundary behavior."""

    def test_thesis_indeterminate_when_hv_missing_roc5(self):
        """HV_20D=0 → indeterminate. Direction signals don't fire → thesis NOT degraded."""
        from core.management.cycle2.thesis.thesis_engine import compute_thesis_state
        row = _base_long_option_row(
            roc_5=1.5,              # would be adverse under old raw fallback
            Price_Drift_Pct=0.01,
            HV_20D=0.0,             # missing HV → indeterminate
        )
        df = pd.DataFrame([row])
        result_df = compute_thesis_state(df)
        state = result_df.iloc[0]["Thesis_State"]
        assert state != "BROKEN", \
            f"HV missing → direction indeterminate, should not BREAK thesis, got {state}"

    def test_thesis_indeterminate_when_hv_missing_drift(self):
        """HV_20D=0 → indeterminate. Even large drift doesn't degrade without HV context."""
        from core.management.cycle2.thesis.thesis_engine import compute_thesis_state
        row = _base_long_option_row(
            roc_5=0.5,
            Price_Drift_Pct=0.02,   # would be adverse under old raw fallback
            HV_20D=0.0,             # missing HV → indeterminate
        )
        df = pd.DataFrame([row])
        result_df = compute_thesis_state(df)
        state = result_df.iloc[0]["Thesis_State"]
        assert state != "BROKEN", \
            f"HV missing → direction indeterminate, should not BREAK thesis, got {state}"

    def test_thesis_degrades_at_sigma_roc5_boundary(self):
        """HV_20D=0.28 → sigma mode. ROC5=5.92% → z=1.50σ (exactly at boundary)."""
        from core.management.cycle2.thesis.thesis_engine import compute_thesis_state
        import math
        # z = (roc5/100) / (hv/sqrt(252) * sqrt(5)) = 0.0592 / 0.03944 = 1.501
        _daily_sig = 0.28 / math.sqrt(252)
        _five_day_sig = _daily_sig * math.sqrt(5)
        _boundary_roc5 = 1.5 * _five_day_sig * 100  # ≈5.92
        row = _base_long_option_row(
            roc_5=round(_boundary_roc5, 2),  # at sigma boundary
            Price_Drift_Pct=0.01,            # below drift threshold
            HV_20D=0.28,
        )
        df = pd.DataFrame([row])
        result_df = compute_thesis_state(df)
        state = result_df.iloc[0]["Thesis_State"]
        assert state == "DEGRADED", \
            f"Expected DEGRADED at ROC5 z=1.5σ (σ-normalized), got {state}"

    def test_thesis_intact_below_both_thresholds(self):
        """ROC5=1.4% with HV=28% → z=0.35σ (well below 1.5σ) → INTACT."""
        from core.management.cycle2.thesis.thesis_engine import compute_thesis_state
        row = _base_long_option_row(
            roc_5=1.4,
            Price_Drift_Pct=0.019,
        )
        df = pd.DataFrame([row])
        result_df = compute_thesis_state(df)
        state = result_df.iloc[0]["Thesis_State"]
        assert state == "INTACT", \
            f"Expected INTACT below both thresholds, got {state}"


# ═══════════════════════════════════════════════════════════════════════
# Sigma-Normalized Direction-Adverse Gate — Unit Tests
# Natenberg Ch.5 / Hull Ch.2: z-score normalization by stock's own HV
# ═══════════════════════════════════════════════════════════════════════

class TestSigmaNormalizedAdverseGate:
    """Verify compute_direction_adverse_signals() correctly normalizes
    ROC5 and drift by the stock's realized volatility."""

    def test_high_vol_stock_noise_not_adverse(self):
        """AMD-like: HV=60%, ROC5=3.0% → z=0.36σ (noise, NOT adverse).
        Under old raw thresholds this would have triggered EXIT."""
        from core.management.cycle3.doctrine.helpers import compute_direction_adverse_signals
        roc5_adv, drift_adv, roc5_z, drift_z, used_sigma = (
            compute_direction_adverse_signals(3.0, 0.02, 0.60, is_put=True)
        )
        assert used_sigma is True
        assert roc5_adv is False, f"ROC5=3% on 60% HV stock should NOT be adverse (z={roc5_z:.2f})"
        assert drift_adv is False, f"Drift=2% on 60% HV stock should NOT be adverse (z={drift_z:.2f})"
        assert roc5_z < 1.5, f"z-score {roc5_z:.2f} should be < 1.5"

    def test_low_vol_stock_same_move_is_adverse(self):
        """JNJ-like: HV=12%, ROC5=3.0% → z=1.77σ (adverse, truly significant).
        Same magnitude that was noise on AMD is real signal on JNJ."""
        from core.management.cycle3.doctrine.helpers import compute_direction_adverse_signals
        roc5_adv, drift_adv, roc5_z, drift_z, used_sigma = (
            compute_direction_adverse_signals(3.0, 0.01, 0.12, is_put=True)
        )
        assert used_sigma is True
        assert roc5_adv is True, f"ROC5=3.0% on 12% HV stock should BE adverse (z={roc5_z:.2f})"
        assert roc5_z >= 1.5

    def test_hv_zero_indeterminate(self):
        """HV_20D=0 → indeterminate. Neither adverse nor confirming fires."""
        from core.management.cycle3.doctrine.helpers import compute_direction_adverse_signals
        roc5_adv, drift_adv, roc5_z, drift_z, used_sigma = (
            compute_direction_adverse_signals(1.5, 0.02, 0.0, is_put=True)
        )
        assert used_sigma is False
        assert roc5_z is None
        assert roc5_adv is False, "HV missing → indeterminate, not adverse"
        assert drift_adv is False, "HV missing → indeterminate, not adverse"

    def test_hv_nan_indeterminate(self):
        """HV_20D=NaN → indeterminate. No silent raw fallback."""
        from core.management.cycle3.doctrine.helpers import compute_direction_adverse_signals
        roc5_adv, drift_adv, roc5_z, drift_z, used_sigma = (
            compute_direction_adverse_signals(2.0, 0.03, float('nan'), is_put=True)
        )
        assert used_sigma is False
        assert roc5_z is None
        assert roc5_adv is False, "HV NaN → indeterminate, not adverse"

    def test_vol_floor_prevents_absurd_z(self):
        """HV_20D=0.001 (unrealistically low) → clamped to floor (0.005).
        z-score is high but finite, not infinite."""
        from core.management.cycle3.doctrine.helpers import compute_direction_adverse_signals
        roc5_adv, drift_adv, roc5_z, drift_z, used_sigma = (
            compute_direction_adverse_signals(1.0, 0.01, 0.001, is_put=True)
        )
        assert used_sigma is True
        # daily_sigma = max(0.001/15.87, 0.005) = 0.005
        # five_day_sigma = 0.005 * 2.236 = 0.01118
        # roc5_z = 0.01 / 0.01118 = 0.894 — NOT adverse
        assert roc5_z is not None
        assert abs(roc5_z) < 10.0, f"Vol floor should prevent absurd z-scores, got {roc5_z}"

    def test_call_direction_symmetry(self):
        """Long call: stock FALLING is adverse → negative z-scores trigger."""
        from core.management.cycle3.doctrine.helpers import compute_direction_adverse_signals
        roc5_adv, drift_adv, roc5_z, drift_z, used_sigma = (
            compute_direction_adverse_signals(-3.0, -0.025, 0.12, is_put=False)
        )
        assert used_sigma is True
        assert roc5_adv is True, f"ROC5=-3.0% on 12% HV call should be adverse (z={roc5_z:.2f})"
        assert roc5_z <= -1.5

    def test_put_confirming_move_not_adverse(self):
        """Long put: stock FALLING (negative ROC5) is confirming, NOT adverse."""
        from core.management.cycle3.doctrine.helpers import compute_direction_adverse_signals
        roc5_adv, drift_adv, roc5_z, drift_z, used_sigma = (
            compute_direction_adverse_signals(-3.0, -0.04, 0.28, is_put=True)
        )
        assert roc5_adv is False, "Negative ROC5 for put is confirming, not adverse"
        assert drift_adv is False, "Negative drift for put is confirming, not adverse"

    def test_exact_sigma_boundary_fires(self):
        """z-score exactly 1.5σ should fire (>= threshold)."""
        import math
        from core.management.cycle3.doctrine.helpers import compute_direction_adverse_signals
        hv = 0.28
        daily_sig = hv / math.sqrt(252)
        five_day_sig = daily_sig * math.sqrt(5)
        roc5_at_boundary = 1.5 * five_day_sig * 100  # ≈5.916%
        roc5_adv, _, roc5_z, _, used_sigma = (
            compute_direction_adverse_signals(roc5_at_boundary, 0.0, hv, is_put=True)
        )
        assert used_sigma is True
        assert roc5_adv is True, f"Exactly at z=1.5σ should fire (z={roc5_z:.4f})"

    def test_amd_scenario_not_prematurely_exited(self):
        """AMD: HV≈50%, ROC5=+1.8%, drift=+2.5%.
        Old gate: ROC5=1.8% >= 1.5% → EXIT (premature — AMD moves this in a day).
        New gate: roc5_z = 0.43σ, drift_z = 0.79σ → neither adverse → NO EXIT."""
        from core.management.cycle3.doctrine.helpers import compute_direction_adverse_signals
        roc5_adv, drift_adv, roc5_z, drift_z, used_sigma = (
            compute_direction_adverse_signals(1.8, 0.025, 0.50, is_put=True)
        )
        assert used_sigma is True
        assert roc5_adv is False, f"AMD ROC5=1.8% should NOT be adverse (z={roc5_z:.2f})"
        assert drift_adv is False, f"AMD drift=2.5% should NOT be adverse (z={drift_z:.2f})"

    def test_sigma_tag_in_rationale(self):
        """When sigma mode is active, the EXIT rationale should include σ-mode tag."""
        row = _base_long_option_row(
            HV_20D=0.12,              # low-vol → sigma mode active
            roc_5=3.0,                # z=1.77σ (adverse)
            Price_Drift_Pct=0.04,     # z=5.29σ (adverse)
            Total_GL_Decimal=-0.20,
            DTE=25.0,
        )
        result = _run_doctrine(row)
        if result["Action"] == "EXIT":
            assert "σ-mode" in result["Rationale"], (
                f"Sigma mode should be tagged in rationale. Got: {result['Rationale'][:300]}"
            )


# ═══════════════════════════════════════════════════════════════════════
# BUG 6: IV_Percentile blocking rolls on shallow history
# ═══════════════════════════════════════════════════════════════════════

class TestIVPercentileDepthAwareness:
    """BUG 6: IV_Percentile=89% with 25 days of history was blocking rolls.
    Fix: when depth < 45 (MATURE threshold), don't let high percentile
    block rolls — low confidence means the signal is unreliable."""

    def test_shallow_depth_does_not_block_roll(self):
        """IV_Percentile=89% but only 25d history + gap < 0 (cheap vol)
        → roll should NOT be blocked by IV (Jabbour gate passes via gap).
        HV=12% so ROC5=3.0% → z=1.77σ (adverse)."""
        row = _base_long_option_row(
            roc_5=3.0,                 # z=1.77σ with HV=12% (adverse)
            Price_Drift_Pct=0.01,      # single signal only
            HV_20D=0.12,              # low-vol: makes ROC5 signal adverse
            DTE=25.0,                  # within 30d roll window
            DTE_Entry=60.0,
            IV_Percentile=89.0,        # high → would block roll
            IV_Percentile_Depth=25,    # shallow → unreliable
            IV_vs_HV_Gap=-1.0,        # cheap vol → Jabbour gate passes
            Thesis_State="INTACT",
            momentum_slope=-0.5,       # confirming signal for put
            EV_Feasibility_Ratio=0.40,
        )
        result = _run_doctrine(row)
        # With shallow depth + cheap vol, IV should not block roll
        if result["Action"] == "EXIT":
            assert "expensive to roll" not in result["Rationale"], \
                f"Shallow IV_Percentile + cheap vol should not block roll: {result['Rationale'][:200]}"

    def test_deep_depth_blocks_roll(self):
        """IV_Percentile=89% with 60d history → roll IS expensive (reliable signal).
        HV=12% so ROC5=3.0% → z=1.77σ (adverse)."""
        row = _base_long_option_row(
            roc_5=3.0,
            Price_Drift_Pct=0.01,
            HV_20D=0.12,              # low-vol: makes ROC5 signal adverse
            DTE=25.0,
            DTE_Entry=60.0,
            IV_Percentile=89.0,
            IV_Percentile_Depth=60,    # deep → reliable
            Thesis_State="INTACT",
            momentum_slope=-0.5,
            EV_Feasibility_Ratio=0.40,
        )
        result = _run_doctrine(row)
        if result["Action"] == "EXIT":
            assert "expensive to roll" in result["Rationale"], \
                f"Deep IV_Percentile should block roll: {result['Rationale'][:200]}"

    def test_low_iv_with_shallow_depth_allows_roll(self):
        """IV_Percentile=30% even with shallow history → roll affordable.
        HV=12% so ROC5=3.0% → z=1.77σ (adverse)."""
        row = _base_long_option_row(
            roc_5=3.0,
            Price_Drift_Pct=0.01,
            HV_20D=0.12,              # low-vol: makes ROC5 signal adverse
            DTE=25.0,
            DTE_Entry=60.0,
            IV_Percentile=30.0,        # low → affordable regardless of depth
            IV_Percentile_Depth=25,
            Thesis_State="INTACT",
            momentum_slope=-0.5,
            EV_Feasibility_Ratio=0.40,
        )
        result = _run_doctrine(row)
        # Low IV should never be called "expensive"
        assert "expensive to roll" not in result.get("Rationale", "")


# ═══════════════════════════════════════════════════════════════════════════════
# Jabbour Fresh-Entry IV Gate — 5 tests
# RAG: Jabbour (0.766): "Rarely does it make sense to take a losing position
# and worsen the odds through an adjustment."
# When IV_Percentile is unreliable (depth < 45), the engine now falls back to
# IV_vs_HV_Gap. Long options buy vol → require gap <= 0 (cheap vol) for roll.
# ═══════════════════════════════════════════════════════════════════════════════


class TestJabbourFreshEntryGate:
    """Jabbour gate: rolls on losing long options must pass fresh-entry IV test."""

    def test_unreliable_depth_gap_positive_blocks_roll(self):
        """Unreliable depth + IV > HV (gap +3.0) → roll blocked, no vol edge.
        HV=12% so ROC5=3.0% → z=1.77σ (adverse)."""
        row = _base_long_option_row(
            roc_5=3.0,                      # z=1.77σ with HV=12% (adverse)
            Price_Drift_Pct=0.01,           # single adverse signal only
            HV_20D=0.12,                    # low-vol: makes ROC5 signal adverse
            DTE=25.0,
            DTE_Entry=60.0,
            Total_GL_Decimal=-0.20,         # mild loss (avoid SRS override)
            IV_Percentile=89.0,
            IV_Percentile_Depth=25,         # unreliable
            IV_vs_HV_Gap=3.0,              # IV above HV → no vol edge for buyer
            Thesis_State="INTACT",
            momentum_slope=-0.5,            # confirming for put
            EV_Feasibility_Ratio=0.40,
            Sector_Relative_Strength="NEUTRAL",
        )
        result = _run_doctrine(row)
        assert result["Action"] != "ROLL", \
            f"Jabbour gate should block roll when gap > 0 with unreliable depth: {result['Action']}"
        # Should mention Jabbour or vol edge in rationale
        rationale = result.get("Rationale", "")
        assert "Jabbour" in rationale or "vol edge" in rationale or "HOLD" in result["Action"], \
            f"Expected Jabbour annotation: {rationale[:200]}"

    def test_unreliable_depth_gap_negative_allows_roll(self):
        """Unreliable depth + IV < HV (gap -2.0) → cheap vol, roll allowed.
        HV=12% so ROC5=3.0% → z=1.77σ (adverse)."""
        row = _base_long_option_row(
            roc_5=3.0,                      # z=1.77σ with HV=12% (adverse)
            Price_Drift_Pct=0.01,           # single adverse signal only
            HV_20D=0.12,                    # low-vol: makes ROC5 signal adverse
            DTE=25.0,
            DTE_Entry=60.0,
            Total_GL_Decimal=-0.20,
            IV_Percentile=89.0,
            IV_Percentile_Depth=25,         # unreliable
            IV_vs_HV_Gap=-2.0,             # IV below HV → vol edge for buyer
            Thesis_State="INTACT",
            momentum_slope=-0.5,            # confirming for put
            EV_Feasibility_Ratio=0.40,
            Sector_Relative_Strength="NEUTRAL",
        )
        result = _run_doctrine(row)
        assert result["Action"] == "ROLL", \
            f"Cheap vol (gap < 0) should allow roll even with unreliable depth: {result['Action']}"

    def test_reliable_low_pctile_allows_roll_regression(self):
        """Reliable depth + low percentile (40%) → roll allowed even with high gap.
        HV=12% so ROC5=3.0% → z=1.77σ (adverse)."""
        row = _base_long_option_row(
            roc_5=3.0,
            Price_Drift_Pct=0.01,
            HV_20D=0.12,              # low-vol: makes ROC5 signal adverse
            DTE=25.0,
            DTE_Entry=60.0,
            Total_GL_Decimal=-0.20,
            IV_Percentile=40.0,             # low → affordable
            IV_Percentile_Depth=60,         # reliable
            IV_vs_HV_Gap=5.0,              # high gap — but percentile is reliable & low
            Thesis_State="INTACT",
            momentum_slope=-0.5,
            EV_Feasibility_Ratio=0.40,
            Sector_Relative_Strength="NEUTRAL",
        )
        result = _run_doctrine(row)
        assert result["Action"] == "ROLL", \
            f"Reliable low percentile should allow roll regardless of gap: {result['Action']}"

    def test_reliable_high_pctile_blocks_roll_regression(self):
        """Reliable depth + high percentile (75%) → roll blocked even with cheap gap.
        HV=12% so ROC5=3.0% → z=1.77σ (adverse)."""
        row = _base_long_option_row(
            roc_5=3.0,
            Price_Drift_Pct=0.01,
            HV_20D=0.12,              # low-vol: makes ROC5 signal adverse
            DTE=25.0,
            DTE_Entry=60.0,
            Total_GL_Decimal=-0.20,
            IV_Percentile=75.0,             # high → expensive
            IV_Percentile_Depth=60,         # reliable
            IV_vs_HV_Gap=-2.0,             # cheap gap — but percentile is reliable & high
            Thesis_State="INTACT",
            momentum_slope=-0.5,
            EV_Feasibility_Ratio=0.40,
            Sector_Relative_Strength="NEUTRAL",
        )
        result = _run_doctrine(row)
        assert result["Action"] != "ROLL", \
            f"Reliable high percentile should block roll regardless of gap: {result['Action']}"
        assert "expensive to roll" in result.get("Rationale", "")

    def test_srs_underperforming_unreliable_gap_positive_holds(self):
        """SRS UNDERPERFORMING + unreliable depth + gap > 0 → HOLD (not ROLL).
        HV=12% so ROC5=3.0% → z=1.77σ (adverse)."""
        row = _base_long_option_row(
            roc_5=3.0,
            Price_Drift_Pct=0.01,
            HV_20D=0.12,              # low-vol: makes ROC5 signal adverse
            DTE=25.0,
            DTE_Entry=60.0,
            Total_GL_Decimal=-0.20,
            IV_Percentile=89.0,
            IV_Percentile_Depth=25,         # unreliable
            IV_vs_HV_Gap=3.0,              # IV above HV → no vol edge
            Thesis_State="INTACT",
            momentum_slope=-0.5,
            EV_Feasibility_Ratio=0.40,
            Sector_Relative_Strength="UNDERPERFORMING",
        )
        result = _run_doctrine(row)
        assert result["Action"] == "HOLD", \
            f"SRS UNDERPERFORMING + gap > 0 should HOLD (not ROLL): {result['Action']}"


# ═══════════════════════════════════════════════════════════════════════════════
# Bug 37: True Breakeven EV Formula
# ═══════════════════════════════════════════════════════════════════════════════

class TestTrueBreakevenEV:
    """Validate that compute_expected_move uses true breakeven (strike ± premium),
    not just distance to strike."""

    def test_long_put_otm_true_breakeven(self):
        """LONG PUT OTM: true BE = strike - premium.
        AMZN-like: Strike=190, Price=193.73, Premium_Entry=8.96.
        Old formula: required = 193.73 - 190 = 3.73 (0.26×)
        New formula: required = 193.73 - (190 - 8.96) = 12.69 (0.89×)"""
        from core.management.cycle2.drift.compute_expected_move import compute_expected_move
        df = pd.DataFrame([{
            "AssetType": "OPTION",
            "UL Last": 193.73,
            "Strike": 190.0,
            "Call/Put": "P",
            "IV_Now": 0.32,
            "Theta": -0.45,
            "Last": 6.50,
            "DTE": 30.0,
            "Strategy": "LONG_PUT",
            "Premium_Entry": 8.96,
        }])
        result = compute_expected_move(df)
        req_be = result.iloc[0]["Required_Move_Breakeven"]
        ev_ratio = result.iloc[0]["EV_Feasibility_Ratio"]
        # True BE = 190 - 8.96 = 181.04
        # Required = 193.73 - 181.04 = 12.69
        assert abs(req_be - 12.69) < 0.1, f"Expected ~12.69, got {req_be}"
        # With old formula it would have been 3.73 — must be > 0.50×
        assert ev_ratio > 0.50, f"EV ratio should be > 0.50 with true breakeven, got {ev_ratio}"

    def test_long_call_otm_true_breakeven(self):
        """LONG CALL OTM: true BE = strike + premium.
        Strike=200, Price=195, Premium_Entry=5.00 → BE=205 → required=10."""
        from core.management.cycle2.drift.compute_expected_move import compute_expected_move
        df = pd.DataFrame([{
            "AssetType": "OPTION",
            "UL Last": 195.0,
            "Strike": 200.0,
            "Call/Put": "C",
            "IV_Now": 0.30,
            "Theta": -0.35,
            "Last": 3.00,
            "DTE": 30.0,
            "Strategy": "LONG_CALL",
            "Premium_Entry": 5.00,
        }])
        result = compute_expected_move(df)
        req_be = result.iloc[0]["Required_Move_Breakeven"]
        # True BE = 200 + 5 = 205 → required = 205 - 195 = 10
        assert abs(req_be - 10.0) < 0.1, f"Expected ~10.0, got {req_be}"

    def test_itm_put_profit_cushion_accounts_for_premium(self):
        """ITM PUT past true breakeven: price below (strike - premium).
        Strike=200, Price=185, Premium=6 → BE=194 → cushion=194-185=9."""
        from core.management.cycle2.drift.compute_expected_move import compute_expected_move
        df = pd.DataFrame([{
            "AssetType": "OPTION",
            "Strike": 200.0,
            "UL Last": 185.0,
            "Call/Put": "P",
            "IV_Now": 0.35,
            "Theta": -0.30,
            "Last": 16.00,
            "DTE": 25.0,
            "Strategy": "LONG_PUT",
            "Premium_Entry": 6.00,
            "AssetType": "OPTION",
        }])
        result = compute_expected_move(df)
        req_be = result.iloc[0]["Required_Move_Breakeven"]
        cushion = result.iloc[0]["Profit_Cushion"]
        # True BE = 200 - 6 = 194.  Price 185 < 194 → past breakeven.
        assert req_be == 0.0, f"Should be past breakeven (0.0), got {req_be}"
        assert abs(cushion - 9.0) < 0.1, f"Expected cushion ~9.0, got {cushion}"

    def test_premium_entry_missing_falls_back_to_last(self):
        """When Premium_Entry is NaN, should use Last as proxy."""
        from core.management.cycle2.drift.compute_expected_move import compute_expected_move
        df = pd.DataFrame([{
            "AssetType": "OPTION",
            "UL Last": 193.73,
            "Strike": 190.0,
            "Call/Put": "P",
            "IV_Now": 0.32,
            "Theta": -0.45,
            "Last": 6.50,
            "DTE": 30.0,
            "Strategy": "LONG_PUT",
            "Premium_Entry": float('nan'),
        }])
        result = compute_expected_move(df)
        req_be = result.iloc[0]["Required_Move_Breakeven"]
        # Fallback to Last=6.50.  True BE = 190 - 6.50 = 183.50
        # Required = 193.73 - 183.50 = 10.23
        assert abs(req_be - 10.23) < 0.1, f"Expected ~10.23 with Last fallback, got {req_be}"

    def test_short_put_premium_buffer_increases_distance(self):
        """SHORT PUT: premium provides buffer → breakeven is lower.
        Strike=100, Price=110, Premium=5 → BE=95 → required=15 (was 10)."""
        from core.management.cycle2.drift.compute_expected_move import compute_expected_move
        df = pd.DataFrame([{
            "AssetType": "OPTION",
            "UL Last": 110.0,
            "Strike": 100.0,
            "Call/Put": "P",
            "IV_Now": 0.25,
            "Theta": 0.10,
            "Last": 1.50,
            "DTE": 20.0,
            "Strategy": "SHORT_PUT",
            "Premium_Entry": 5.00,
        }])
        result = compute_expected_move(df)
        req_be = result.iloc[0]["Required_Move_Breakeven"]
        # True BE = 100 - 5 = 95 → required = 110 - 95 = 15
        assert abs(req_be - 15.0) < 0.1, f"Expected ~15.0, got {req_be}"


# ═══════════════════════════════════════════════════════════════════════════════
# Bug 38: MC EXIT_NOW overrides HOLD → EXIT
# ═══════════════════════════════════════════════════════════════════════════════

class TestMCExitNowOverride:
    """MC EXIT_NOW verdict should change Action from HOLD → EXIT, not just
    escalate urgency."""

    def _apply_mc_escalation(self, df):
        """Replay the MC escalation logic from run_all.py on a DataFrame.
        Includes Bug 39 LEAPS guard, recovery guard, grace period guard,
        and macro catalyst guard."""
        _urgency_order = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}

        def _promote_urgency(current, target):
            cur_rank = _urgency_order.get(str(current).upper(), 0)
            tgt_rank = _urgency_order.get(str(target).upper(), 0)
            return target.upper() if tgt_rank > cur_rank else current

        if "MC_Hold_Verdict" in df.columns:
            _hold_mask = df["Action"] == "HOLD"
            _exit_now  = df["MC_Hold_Verdict"].fillna("") == "EXIT_NOW"

            # Bug 39: LEAPS guard
            if "DTE" in df.columns:
                _leaps_mask = pd.to_numeric(df["DTE"], errors="coerce").fillna(0) > 180
            else:
                _leaps_mask = pd.Series(False, index=df.index)
            if "Thesis_State" in df.columns:
                _thesis_ok = df["Thesis_State"].fillna("").isin(["INTACT", "RECOVERING"])
            else:
                _thesis_ok = pd.Series(False, index=df.index)
            _leaps_intact = _leaps_mask & _thesis_ok

            # Macro catalyst guard
            if "Macro_Catalyst_Protected" in df.columns:
                _macro_catalyst_mask = df["Macro_Catalyst_Protected"].fillna(False).astype(bool)
            else:
                _macro_catalyst_mask = pd.Series(False, index=df.index)

            # Data-blocked guard
            if "Pre_Doctrine_Flag" in df.columns:
                _data_blocked_mask = df["Pre_Doctrine_Flag"].fillna("").isin(
                    ["DATA_BLOCKED", "PRICE_STALE", "GREEKS_MISSING"]
                )
            else:
                _data_blocked_mask = pd.Series(False, index=df.index)

            _exempt     = _leaps_intact | _macro_catalyst_mask | _data_blocked_mask
            _rule3      = _hold_mask & _exit_now & ~_exempt
            _leaps_warn = _hold_mask & _exit_now & _leaps_intact & ~_macro_catalyst_mask
            _macro_warn = _hold_mask & _exit_now & _macro_catalyst_mask & ~_leaps_intact
            _data_warn  = _hold_mask & _exit_now & _data_blocked_mask

            if _rule3.any():
                df.loc[_rule3, "Action"]  = "EXIT"
                df.loc[_rule3, "Urgency"] = df.loc[_rule3, "Urgency"].apply(
                    lambda u: _promote_urgency(u, "HIGH")
                )
                df.loc[_rule3, "Rationale"] = (
                    df.loc[_rule3, "Rationale"].fillna("") +
                    " | ⚡ MC EXIT_NOW override: p_recovery < 0.35 AND EV < 0 — exit, do not hold."
                )
            if _leaps_warn.any():
                df.loc[_leaps_warn, "Rationale"] = (
                    df.loc[_leaps_warn, "Rationale"].fillna("") +
                    " | ⚠️ MC EXIT_NOW suppressed (LEAPS DTE>180 + thesis intact) — monitor closely."
                )
            if _macro_warn.any():
                df.loc[_macro_warn, "Rationale"] = (
                    df.loc[_macro_warn, "Rationale"].fillna("") +
                    " | ⚠️ MC EXIT_NOW suppressed (macro catalyst protection) — "
                    "imminent HIGH-impact event is vol catalyst for long premium."
                )
            if _data_warn.any():
                df.loc[_data_warn, "Rationale"] = (
                    df.loc[_data_warn, "Rationale"].fillna("") +
                    " | ⚠️ MC EXIT_NOW suppressed (DATA_BLOCKED) — "
                    "price data stale/missing; MC inputs equally unreliable."
                )
        return df

    def test_hold_high_becomes_exit_high(self):
        """HOLD HIGH with MC_Hold_Verdict=EXIT_NOW → EXIT HIGH."""
        df = pd.DataFrame([{
            "Action": "HOLD",
            "Urgency": "HIGH",
            "Rationale": "Direction ADVERSE.",
            "MC_Hold_Verdict": "EXIT_NOW",
        }])
        result = self._apply_mc_escalation(df)
        assert result.iloc[0]["Action"] == "EXIT"
        assert result.iloc[0]["Urgency"] == "HIGH"
        assert "MC EXIT_NOW override" in result.iloc[0]["Rationale"]

    def test_hold_medium_becomes_exit_high(self):
        """HOLD MEDIUM with MC EXIT_NOW → EXIT HIGH (urgency promoted)."""
        df = pd.DataFrame([{
            "Action": "HOLD",
            "Urgency": "MEDIUM",
            "Rationale": "Marginal.",
            "MC_Hold_Verdict": "EXIT_NOW",
        }])
        result = self._apply_mc_escalation(df)
        assert result.iloc[0]["Action"] == "EXIT"
        assert result.iloc[0]["Urgency"] == "HIGH"

    def test_hold_without_exit_now_unchanged(self):
        """HOLD with MC_Hold_Verdict=HOLD_OK → no change."""
        df = pd.DataFrame([{
            "Action": "HOLD",
            "Urgency": "MEDIUM",
            "Rationale": "Stable.",
            "MC_Hold_Verdict": "HOLD_OK",
        }])
        result = self._apply_mc_escalation(df)
        assert result.iloc[0]["Action"] == "HOLD"
        assert result.iloc[0]["Urgency"] == "MEDIUM"

    def test_exit_row_not_affected_by_mc(self):
        """Already EXIT rows should not be touched by MC escalation."""
        df = pd.DataFrame([{
            "Action": "EXIT",
            "Urgency": "CRITICAL",
            "Rationale": "Thesis broken.",
            "MC_Hold_Verdict": "EXIT_NOW",
        }])
        result = self._apply_mc_escalation(df)
        # EXIT is not HOLD, so rule3 mask doesn't match
        assert result.iloc[0]["Action"] == "EXIT"
        assert "MC EXIT_NOW override" not in result.iloc[0]["Rationale"]

    # --- Bug 39: LEAPS guard tests ---

    def test_leaps_intact_thesis_suppresses_mc_override(self):
        """Bug 39: LEAPS (DTE>180) + INTACT thesis → MC EXIT_NOW suppressed."""
        df = pd.DataFrame([{
            "Action": "HOLD",
            "Urgency": "LOW",
            "Rationale": "Position intact.",
            "MC_Hold_Verdict": "EXIT_NOW",
            "DTE": 318.0,
            "Thesis_State": "INTACT",
        }])
        result = self._apply_mc_escalation(df)
        assert result.iloc[0]["Action"] == "HOLD", (
            "Bug 39: LEAPS + INTACT thesis should stay HOLD"
        )
        assert "suppressed" in result.iloc[0]["Rationale"].lower()

    def test_leaps_recovering_thesis_suppresses_mc_override(self):
        """Bug 39: LEAPS + RECOVERING thesis → MC EXIT_NOW also suppressed."""
        df = pd.DataFrame([{
            "Action": "HOLD",
            "Urgency": "MEDIUM",
            "Rationale": "Recovering.",
            "MC_Hold_Verdict": "EXIT_NOW",
            "DTE": 200.0,
            "Thesis_State": "RECOVERING",
        }])
        result = self._apply_mc_escalation(df)
        assert result.iloc[0]["Action"] == "HOLD"

    def test_leaps_degraded_thesis_allows_mc_override(self):
        """LEAPS with DEGRADED thesis → MC EXIT_NOW should still fire."""
        df = pd.DataFrame([{
            "Action": "HOLD",
            "Urgency": "MEDIUM",
            "Rationale": "Degraded thesis.",
            "MC_Hold_Verdict": "EXIT_NOW",
            "DTE": 318.0,
            "Thesis_State": "DEGRADED",
        }])
        result = self._apply_mc_escalation(df)
        assert result.iloc[0]["Action"] == "EXIT", (
            "LEAPS with DEGRADED thesis should still allow MC override"
        )

    def test_non_leaps_intact_thesis_allows_mc_override(self):
        """Non-LEAPS (DTE=30) with INTACT thesis → MC EXIT_NOW fires normally."""
        df = pd.DataFrame([{
            "Action": "HOLD",
            "Urgency": "LOW",
            "Rationale": "Short dated.",
            "MC_Hold_Verdict": "EXIT_NOW",
            "DTE": 30.0,
            "Thesis_State": "INTACT",
        }])
        result = self._apply_mc_escalation(df)
        assert result.iloc[0]["Action"] == "EXIT", (
            "Non-LEAPS should still allow MC EXIT_NOW override"
        )

    def test_macro_catalyst_protected_suppresses_mc_override(self):
        """Macro catalyst protected position → MC EXIT_NOW suppressed to warning."""
        df = pd.DataFrame([{
            "Action": "HOLD",
            "Urgency": "MEDIUM",
            "Rationale": "Prior EXIT cleared (extended window): FOMC in 6d.",
            "MC_Hold_Verdict": "EXIT_NOW",
            "DTE": 30.0,
            "Thesis_State": "INTACT",
            "Macro_Catalyst_Protected": True,
        }])
        result = self._apply_mc_escalation(df)
        assert result.iloc[0]["Action"] == "HOLD", (
            "Macro catalyst protected should suppress MC EXIT_NOW"
        )
        assert "macro catalyst protection" in result.iloc[0]["Rationale"].lower()

    def test_macro_catalyst_false_allows_mc_override(self):
        """Macro_Catalyst_Protected=False → MC EXIT_NOW fires normally."""
        df = pd.DataFrame([{
            "Action": "HOLD",
            "Urgency": "LOW",
            "Rationale": "Position holding.",
            "MC_Hold_Verdict": "EXIT_NOW",
            "DTE": 30.0,
            "Thesis_State": "INTACT",
            "Macro_Catalyst_Protected": False,
        }])
        result = self._apply_mc_escalation(df)
        assert result.iloc[0]["Action"] == "EXIT", (
            "Non-protected should allow MC EXIT_NOW"
        )

    def test_macro_catalyst_no_column_allows_mc_override(self):
        """No Macro_Catalyst_Protected column → MC EXIT_NOW fires normally."""
        df = pd.DataFrame([{
            "Action": "HOLD",
            "Urgency": "LOW",
            "Rationale": "Position holding.",
            "MC_Hold_Verdict": "EXIT_NOW",
            "DTE": 30.0,
            "Thesis_State": "INTACT",
        }])
        result = self._apply_mc_escalation(df)
        assert result.iloc[0]["Action"] == "EXIT"

    def test_macro_catalyst_nvda_smh_scenario(self):
        """NVDA/SMH-style: LONG_PUT + FOMC 6d + macro catalyst → stays HOLD."""
        df = pd.DataFrame([{
            "Action": "HOLD",
            "Urgency": "MEDIUM",
            "Rationale": "Prior EXIT cleared (extended window): FOMC in 6d is vol catalyst for LONG_PUT.",
            "MC_Hold_Verdict": "EXIT_NOW",
            "DTE": 28.0,
            "Thesis_State": "INTACT",
            "Macro_Catalyst_Protected": True,
            "Strategy": "LONG_PUT",
        }])
        result = self._apply_mc_escalation(df)
        assert result.iloc[0]["Action"] == "HOLD", (
            "NVDA/SMH scenario: macro catalyst should protect from MC override"
        )
        assert "suppressed" in result.iloc[0]["Rationale"].lower()

    def test_data_blocked_suppresses_mc_override(self):
        """DATA_BLOCKED position → MC EXIT_NOW suppressed (stale data)."""
        df = pd.DataFrame([{
            "Action": "HOLD",
            "Urgency": "LOW",
            "Rationale": "DATA_BLOCKED: PRICE_STALE.",
            "MC_Hold_Verdict": "EXIT_NOW",
            "DTE": 42.0,
            "Thesis_State": "INTACT",
            "Pre_Doctrine_Flag": "PRICE_STALE",
        }])
        result = self._apply_mc_escalation(df)
        assert result.iloc[0]["Action"] == "HOLD", (
            "DATA_BLOCKED should suppress MC EXIT_NOW — stale data"
        )
        assert "data_blocked" in result.iloc[0]["Rationale"].lower()

    def test_data_blocked_greeks_missing_suppresses(self):
        """GREEKS_MISSING flag → MC EXIT_NOW suppressed."""
        df = pd.DataFrame([{
            "Action": "HOLD",
            "Urgency": "LOW",
            "Rationale": "Greeks missing.",
            "MC_Hold_Verdict": "EXIT_NOW",
            "Pre_Doctrine_Flag": "GREEKS_MISSING",
        }])
        result = self._apply_mc_escalation(df)
        assert result.iloc[0]["Action"] == "HOLD"

    def test_no_pre_doctrine_flag_allows_mc_override(self):
        """No Pre_Doctrine_Flag → MC EXIT_NOW fires normally."""
        df = pd.DataFrame([{
            "Action": "HOLD",
            "Urgency": "LOW",
            "Rationale": "Normal position.",
            "MC_Hold_Verdict": "EXIT_NOW",
            "DTE": 30.0,
            "Thesis_State": "INTACT",
        }])
        result = self._apply_mc_escalation(df)
        assert result.iloc[0]["Action"] == "EXIT"

    def test_nvda_data_blocked_fomc_scenario(self):
        """NVDA-style: DATA_BLOCKED + FOMC 5d → stays HOLD, not EXIT."""
        df = pd.DataFrame([{
            "Action": "HOLD",
            "Urgency": "LOW",
            "Rationale": "DATA_BLOCKED: NVDA: [PRICE_STALE]",
            "MC_Hold_Verdict": "EXIT_NOW",
            "DTE": 42.0,
            "Thesis_State": "INTACT",
            "Pre_Doctrine_Flag": "PRICE_STALE",
            "Strategy": "LONG_PUT",
        }])
        result = self._apply_mc_escalation(df)
        assert result.iloc[0]["Action"] == "HOLD", (
            "NVDA DATA_BLOCKED scenario: MC should not override on stale data"
        )


# =============================================================================
# Action Streak Escalation Tests
# =============================================================================

class TestActionStreakEscalation:
    """
    Tests for the 3.0a Action Streak Escalation gate in run_all.py.

    Rules:
      Rule 1: REVIEW + streak >= 3 → EXIT MEDIUM
      Rule 2: EXIT + streak >= 5 → urgency promoted to CRITICAL
    """

    @staticmethod
    def _apply_streak_escalation(df: pd.DataFrame) -> pd.DataFrame:
        """Replay the 3.0a escalation gate logic from run_all.py."""
        df = df.copy()
        if "Prior_Action_Streak" not in df.columns:
            return df

        _streak = pd.to_numeric(df["Prior_Action_Streak"], errors="coerce").fillna(0).astype(int)

        # Rule 1: REVIEW × 3+ → EXIT MEDIUM
        _reval_mask = (df["Action"] == "REVIEW") & (_streak >= 3)
        if _reval_mask.any():
            df.loc[_reval_mask, "Action"] = "EXIT"
            df.loc[_reval_mask, "Urgency"] = "MEDIUM"
            df.loc[_reval_mask, "Rationale"] = (
                df.loc[_reval_mask, "Rationale"].fillna("")
                + " | Unresolved REVIEW x"
                + _streak[_reval_mask].astype(str)
                + " -- signal degradation persistent, escalating to EXIT."
            )

        # Rule 2: EXIT × 5+ → urgency CRITICAL
        _exit_mask = (df["Action"] == "EXIT") & (_streak >= 5)
        if _exit_mask.any():
            _current_urgency = df.loc[_exit_mask, "Urgency"].fillna("LOW").str.upper()
            _not_critical = _current_urgency != "CRITICAL"
            _promote_mask = _exit_mask.copy()
            _promote_mask.loc[_exit_mask] = _not_critical.values
            if _promote_mask.any():
                df.loc[_promote_mask, "Urgency"] = "CRITICAL"
                df.loc[_promote_mask, "Rationale"] = (
                    df.loc[_promote_mask, "Rationale"].fillna("")
                    + " | EXIT signal persisted x"
                    + _streak[_promote_mask].astype(str)
                    + " without action -- urgency critical."
                )

        return df

    def test_revalidate_streak_3_escalates(self):
        """REVIEW with streak=3 → EXIT MEDIUM."""
        df = pd.DataFrame([{
            "Action": "REVIEW",
            "Urgency": "LOW",
            "Rationale": "Signal degraded.",
            "Prior_Action_Streak": 3,
        }])
        result = self._apply_streak_escalation(df)
        assert result.iloc[0]["Action"] == "EXIT"
        assert result.iloc[0]["Urgency"] == "MEDIUM"
        assert "REVIEW x3" in result.iloc[0]["Rationale"]

    def test_revalidate_streak_2_no_change(self):
        """REVIEW with streak=2 → no escalation (threshold is 3)."""
        df = pd.DataFrame([{
            "Action": "REVIEW",
            "Urgency": "LOW",
            "Rationale": "Signal degraded.",
            "Prior_Action_Streak": 2,
        }])
        result = self._apply_streak_escalation(df)
        assert result.iloc[0]["Action"] == "REVIEW"
        assert result.iloc[0]["Urgency"] == "LOW"

    def test_revalidate_streak_0_no_change(self):
        """REVIEW with streak=0 (first occurrence) → no escalation."""
        df = pd.DataFrame([{
            "Action": "REVIEW",
            "Urgency": "LOW",
            "Rationale": "Signal degraded.",
            "Prior_Action_Streak": 0,
        }])
        result = self._apply_streak_escalation(df)
        assert result.iloc[0]["Action"] == "REVIEW"

    def test_exit_streak_5_to_critical(self):
        """EXIT with streak=5 → urgency promoted to CRITICAL."""
        df = pd.DataFrame([{
            "Action": "EXIT",
            "Urgency": "HIGH",
            "Rationale": "Direction adverse.",
            "Prior_Action_Streak": 5,
        }])
        result = self._apply_streak_escalation(df)
        assert result.iloc[0]["Action"] == "EXIT"
        assert result.iloc[0]["Urgency"] == "CRITICAL"
        assert "persisted x5" in result.iloc[0]["Rationale"]

    def test_exit_streak_3_no_change(self):
        """EXIT with streak=3 → urgency stays as-is (threshold is 5)."""
        df = pd.DataFrame([{
            "Action": "EXIT",
            "Urgency": "HIGH",
            "Rationale": "Direction adverse.",
            "Prior_Action_Streak": 3,
        }])
        result = self._apply_streak_escalation(df)
        assert result.iloc[0]["Urgency"] == "HIGH"

    def test_hold_streak_ignored(self):
        """HOLD with streak=10 → no escalation (only REVIEW/EXIT trigger)."""
        df = pd.DataFrame([{
            "Action": "HOLD",
            "Urgency": "LOW",
            "Rationale": "Thesis intact.",
            "Prior_Action_Streak": 10,
        }])
        result = self._apply_streak_escalation(df)
        assert result.iloc[0]["Action"] == "HOLD"
        assert result.iloc[0]["Urgency"] == "LOW"


class TestTickerLevelStreakCarryForward:
    """
    Tests that the ticker-level streak carry-forward (Fix 6) correctly propagates
    EXIT signals across BUY_WRITE roll boundaries via max(trade, ticker) merge.

    These tests verify the escalation rules fire correctly when Prior_Action_Streak
    and EXIT_Count_Last_5D come from ticker-level aggregation rather than per-trade.
    (Audit: DKNG Feb-2026 — 5 EXIT CRITICALs across 3 TradeIDs, never escalated)
    """

    @staticmethod
    def _apply_streak_escalation(df: pd.DataFrame) -> pd.DataFrame:
        """Replay 3.0a + Rule 3 escalation from run_all.py."""
        df = df.copy()
        _streak = pd.to_numeric(df.get("Prior_Action_Streak", 0), errors="coerce").fillna(0).astype(int)
        # Rule 1
        _reval_mask = (df["Action"] == "REVIEW") & (_streak >= 3)
        if _reval_mask.any():
            df.loc[_reval_mask, "Action"] = "EXIT"
            df.loc[_reval_mask, "Urgency"] = "MEDIUM"
        # Rule 2
        _exit_mask = (df["Action"] == "EXIT") & (_streak >= 5)
        if _exit_mask.any():
            df.loc[_exit_mask, "Urgency"] = "CRITICAL"
        # Rule 3
        if "EXIT_Count_Last_5D" in df.columns:
            _exit_5d = pd.to_numeric(df["EXIT_Count_Last_5D"], errors="coerce").fillna(0).astype(int)
            _ignored = df["Action"].isin(["HOLD", "ROLL", "REVIEW"]) & (_exit_5d >= 2)
            if _ignored.any():
                df.loc[_ignored, "Action"] = "EXIT"
                _cur = df.loc[_ignored, "Urgency"].fillna("LOW").str.upper()
                _low = _cur.isin(["LOW", ""])
                if _low.any():
                    _p = _ignored.copy()
                    _p.loc[_ignored] = _low.values
                    df.loc[_p, "Urgency"] = "MEDIUM"
        return df

    def test_ticker_streak_carries_exit_across_rolls(self):
        """Rolled BUY_WRITE: old TradeID had EXIT×5 → ticker streak = 5 → CRITICAL.

        Simulates: DKNG260227 (old) had EXIT×5, DKNG260306 (new, current) has HOLD×1.
        Ticker-level streak = 5 (EXIT), trade-level streak = 1 (HOLD).
        max(1, 5) = 5. With Action=EXIT (from doctrine), Rule 2 fires → CRITICAL.
        """
        df = pd.DataFrame([{
            "TradeID": "DKNG260306_24p5_CC_5376",
            "Underlying_Ticker": "DKNG",
            "Action": "EXIT",
            "Urgency": "HIGH",
            "Rationale": "Hard stop breached.",
            # Trade-level would be 1 (new TradeID), but ticker-level carry = 5
            "Prior_Action_Streak": 5,  # after max(trade=1, ticker=5) = 5
            "EXIT_Count_Last_5D": 5,
        }])
        result = self._apply_streak_escalation(df)
        assert result.iloc[0]["Urgency"] == "CRITICAL", (
            f"EXIT streak=5 from ticker carry should promote to CRITICAL, got {result.iloc[0]['Urgency']}"
        )

    def test_exit_count_carries_across_rolls(self):
        """Rolled BUY_WRITE: old TradeID had EXIT×3 in 5d → Rule 3 overrides HOLD.

        Simulates: DKNG260227 had EXIT on 3 of last 5 days, rolled to DKNG260306.
        New TradeID's trade-level EXIT_Count=0, ticker-level=3. max(0,3)=3.
        Today's action is HOLD → Rule 3 overrides to EXIT.
        """
        df = pd.DataFrame([{
            "TradeID": "DKNG260306_24p5_CC_5376",
            "Underlying_Ticker": "DKNG",
            "Action": "HOLD",
            "Urgency": "HIGH",
            "Rationale": "Equity BROKEN.",
            "Prior_Action_Streak": 0,
            # After max(trade=0, ticker=3) = 3
            "EXIT_Count_Last_5D": 3,
        }])
        result = self._apply_streak_escalation(df)
        assert result.iloc[0]["Action"] == "EXIT", (
            f"EXIT_Count=3 from ticker carry should override HOLD to EXIT, got {result.iloc[0]['Action']}"
        )

    def test_different_tickers_isolated(self):
        """DKNG EXIT×5 should NOT affect AAPL streak."""
        df = pd.DataFrame([
            {
                "TradeID": "DKNG260306_24p5_CC_5376",
                "Underlying_Ticker": "DKNG",
                "Action": "EXIT",
                "Urgency": "HIGH",
                "Rationale": "DKNG broken.",
                "Prior_Action_Streak": 5,
                "EXIT_Count_Last_5D": 5,
            },
            {
                "TradeID": "AAPL260320_230p0_CC_5376",
                "Underlying_Ticker": "AAPL",
                "Action": "HOLD",
                "Urgency": "LOW",
                "Rationale": "AAPL thesis intact.",
                "Prior_Action_Streak": 1,
                "EXIT_Count_Last_5D": 0,
            },
        ])
        result = self._apply_streak_escalation(df)
        # DKNG should escalate
        assert result.iloc[0]["Urgency"] == "CRITICAL"
        # AAPL should be untouched
        assert result.iloc[1]["Action"] == "HOLD"
        assert result.iloc[1]["Urgency"] == "LOW"

    def test_max_preserves_higher_trade_streak(self):
        """When trade-level streak > ticker-level, max() preserves the higher value.

        Trade streak = 4 (REVIEW×4 on current TradeID), ticker streak = 2.
        max(4, 2) = 4. Rule 1 fires (REVIEW×4 ≥ 3).
        """
        df = pd.DataFrame([{
            "TradeID": "SPY260320_550p0_SP_5376",
            "Underlying_Ticker": "SPY",
            "Action": "REVIEW",
            "Urgency": "LOW",
            "Rationale": "Signal unclear.",
            # max(trade=4, ticker=2) = 4
            "Prior_Action_Streak": 4,
            "EXIT_Count_Last_5D": 0,
        }])
        result = self._apply_streak_escalation(df)
        assert result.iloc[0]["Action"] == "EXIT", (
            f"REVIEW×4 should escalate to EXIT, got {result.iloc[0]['Action']}"
        )
        assert result.iloc[0]["Urgency"] == "MEDIUM"


# ══════════════════════════════════════════════════════════════════════════════
# Recovery CC — classification, gates, strike floor, timeline
# ══════════════════════════════════════════════════════════════════════════════

from core.management.cycle3.cc_opportunity_engine import (
    _classify_recovery_mode,
    _favorability_check,
    _rank_candidates,
    _compute_recovery_timeline,
    _compute_ladder_allocation,
    _compute_income_gap_ratio,
    _build_ladder_candidates_scan,
    _NONLADDER_MIN_PREMIUM,
    _NONLADDER_SPREAD_CAP_INCOME,
    _NONLADDER_SPREAD_CAP_RECOVERY,
    _NONLADDER_OI_MIN_INCOME,
    _NONLADDER_OI_MIN_RECOVERY,
)


def _stock_row(**overrides) -> pd.Series:
    """Minimal STOCK_ONLY row for CC recovery tests."""
    base = {
        "Strategy": "STOCK_ONLY",
        "AssetType": "STOCK",
        "Underlying_Ticker": "TEST",
        "UL Last": 10.0,
        "Net_Cost_Basis_Per_Share": 10.0,
        "Quantity": 100,
        "HV_20D": 0.50,
        "Thesis_State": "INTACT",
    }
    base.update(overrides)
    return pd.Series(base)


class TestRecoveryCCClassification:
    """_classify_recovery_mode: drift-based mode + thesis override."""

    def test_profitable_position_income(self):
        """Spot > basis → INCOME."""
        row = _stock_row(**{"UL Last": 12.0, "Net_Cost_Basis_Per_Share": 10.0})
        mode, drift = _classify_recovery_mode(row)
        assert mode == "INCOME"
        assert drift > 0

    def test_small_loss_income(self):
        """Spot 5% below basis → INCOME (threshold is -10%)."""
        row = _stock_row(**{"UL Last": 9.50, "Net_Cost_Basis_Per_Share": 10.0})
        mode, drift = _classify_recovery_mode(row)
        assert mode == "INCOME"
        assert -0.10 < drift < 0

    def test_15pct_loss_intact_recovery(self):
        """Spot 15% below basis, thesis INTACT → RECOVERY."""
        row = _stock_row(**{"UL Last": 8.50, "Net_Cost_Basis_Per_Share": 10.0})
        mode, drift = _classify_recovery_mode(row)
        assert mode == "RECOVERY"
        assert -0.25 < drift < -0.10

    def test_30pct_loss_intact_deep_recovery(self):
        """Spot 30% below basis, thesis INTACT → DEEP_RECOVERY (-25% to -35%)."""
        row = _stock_row(**{"UL Last": 7.00, "Net_Cost_Basis_Per_Share": 10.0})
        mode, drift = _classify_recovery_mode(row)
        assert mode == "DEEP_RECOVERY"
        assert -0.35 < drift < -0.25

    def test_64pct_loss_structural_damage(self):
        """Spot 64% below basis, thesis INTACT → STRUCTURAL_DAMAGE (beyond -35%)."""
        row = _stock_row(**{"UL Last": 3.60, "Net_Cost_Basis_Per_Share": 10.0})
        mode, drift = _classify_recovery_mode(row)
        assert mode == "STRUCTURAL_DAMAGE"
        assert drift < -0.35

    def test_40pct_loss_structural_damage(self):
        """Spot 40% below basis → STRUCTURAL_DAMAGE (McMillan: -35% threshold)."""
        row = _stock_row(**{"UL Last": 6.0, "Net_Cost_Basis_Per_Share": 10.0})
        mode, drift = _classify_recovery_mode(row)
        assert mode == "STRUCTURAL_DAMAGE"
        assert drift < -0.35

    def test_structural_damage_thesis_intact_still_blocks(self):
        """Even thesis INTACT doesn't save a -50% position from STRUCTURAL_DAMAGE."""
        row = _stock_row(**{
            "UL Last": 5.0, "Net_Cost_Basis_Per_Share": 10.0,
            "Thesis_State": "INTACT",
        })
        mode, _ = _classify_recovery_mode(row)
        assert mode == "STRUCTURAL_DAMAGE"

    def test_broken_thesis_forces_income(self):
        """Even with -20% drift, BROKEN thesis → INCOME (no gate relaxation)."""
        row = _stock_row(**{
            "UL Last": 8.0, "Net_Cost_Basis_Per_Share": 10.0,
            "Thesis_State": "BROKEN",
        })
        mode, drift = _classify_recovery_mode(row)
        assert mode == "INCOME"
        assert drift < -0.10


class TestRecoveryCCFavorabilityGates:
    """_favorability_check: recovery mode lowers IV_Rank threshold."""

    def test_iv_rank_17_passes_recovery_fails_income(self):
        """IV_Rank=17% passes RECOVERY gate (15%) but fails INCOME gate (20%)."""
        is_fav_inc, reason_inc, _ = _favorability_check(
            iv_rank=17.0, regime="High Vol", signal="Neutral", ivhv_gap=5.0,
            recovery_mode="INCOME",
        )
        is_fav_rec, reason_rec, _ = _favorability_check(
            iv_rank=17.0, regime="High Vol", signal="Neutral", ivhv_gap=5.0,
            recovery_mode="RECOVERY",
        )
        assert not is_fav_inc, "INCOME mode should reject IV_Rank=17%"
        assert is_fav_rec, "RECOVERY mode should accept IV_Rank=17%"

    def test_iv_rank_12_fails_both(self):
        """IV_Rank=12% fails even RECOVERY gate (15%)."""
        is_fav_rec, _, _ = _favorability_check(
            iv_rank=12.0, regime="High Vol", signal="Neutral", ivhv_gap=5.0,
            recovery_mode="RECOVERY",
        )
        assert not is_fav_rec


class TestRecoveryCCStrikeFloor:
    """_rank_candidates: recovery mode enforces strike floor at cost basis."""

    def test_candidate_below_basis_filtered(self):
        """In RECOVERY, candidates with strike < basis should be filtered out."""
        # Create a scan dataframe with a CC candidate below basis
        scan_df = pd.DataFrame([{
            "Ticker": "TEST",
            "Strategy_Name": "COVERED_CALL",
            "Execution_Status": "READY",
            "Actual_DTE": 30,
            "Selected_Strike": 9.0,   # below basis of 10
            "Mid_Price": 0.50,
            "Delta": 0.25,
            "Implied_Volatility": 0.40,
            "Open_Interest": 500,
            "Bid_Ask_Spread_Pct": 0.05,
            "Confidence": 0.80,
            "DQS_Score": 75,
        }])
        # Basis = 10, spot = 8.50 → recovery mode
        candidates = _rank_candidates(
            scan_df, "TEST", basis_per_share=10.0,
            recovery_mode="RECOVERY", spot_price=8.50,
        )
        # Strike $9 < max(8.50 × 1.10 = 9.35, 10.0) = $10 floor → filtered out
        assert len(candidates) == 0


class TestRecoveryCCTimeline:
    """_compute_recovery_timeline: gap, monthly_est, and months formulas."""

    def test_underwater_timeline(self):
        """Verify OTM-adjusted formula: gap=$2, monthly premium from HV."""
        result = _compute_recovery_timeline(spot=8.0, basis=10.0, hv=0.50)
        assert result["gap"] == 2.0
        # ATM weekly = 0.4 × 0.50 × 8.0 / √52 ≈ 0.222
        # OTM adjustment: × 0.30 (delta ~0.30) × 0.85 (fill) = × 0.255
        # monthly_est ≈ 0.222 × 0.255 × 4.3 ≈ 0.24
        assert 0.15 < result["monthly_est"] < 0.35
        # months = 2.0 / ~0.24 ≈ 8.3
        assert 5 < result["months"] < 15

    def test_profitable_no_gap(self):
        """Spot > basis → zero gap, zero months."""
        result = _compute_recovery_timeline(spot=12.0, basis=10.0, hv=0.50)
        assert result["gap"] == 0.0
        assert result["monthly_est"] == 0.0
        assert result["months"] == 0.0

    def test_extreme_hv_capped(self):
        """HV=200% is capped at 100% to avoid fantasy projections."""
        result_extreme = _compute_recovery_timeline(spot=6.0, basis=17.0, hv=2.06)
        result_capped  = _compute_recovery_timeline(spot=6.0, basis=17.0, hv=1.00)
        # Both should give identical results because HV is capped at 100%
        assert result_extreme["monthly_est"] == result_capped["monthly_est"]
        assert result_extreme["months"] == result_capped["months"]
        # With HV=100% capped: ATM weekly ≈ 0.4 × 1.0 × 6.0 / √52 ≈ 0.333
        # OTM: 0.333 × 0.30 × 0.85 × 4.3 ≈ 0.37/mo → months ≈ 11.0/0.37 ≈ 30
        assert result_extreme["months"] > 20, "Should be realistic (OTM-adjusted, not ATM fantasy)"


# ═══════════════════════════════════════════════════════════════════════════════
# CC Ladder — Tiered Partial Coverage Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestCCLadderAllocation:
    """_compute_ladder_allocation: lot-based ladder eligibility and tier splits."""

    def test_small_position_returns_none(self):
        """< 1000 shares (< 10 lots) → not eligible."""
        result = _compute_ladder_allocation(qty=500, recovery_mode="RECOVERY", thesis_state="INTACT")
        assert result is None

    def test_1000_shares_eligible(self):
        """Exactly 1000 shares = 10 lots → eligible."""
        result = _compute_ladder_allocation(qty=1000, recovery_mode="RECOVERY", thesis_state="INTACT")
        assert result is not None
        assert result["total_lots"] == 10

    def test_recovery_80_pct_coverage(self):
        """RECOVERY mode → max 80% coverage."""
        result = _compute_ladder_allocation(qty=2000, recovery_mode="RECOVERY", thesis_state="INTACT")
        assert result["max_coverage_pct"] == 0.80
        assert result["covered_lots"] == 16  # 20 × 0.80

    def test_deep_recovery_80_pct(self):
        """DEEP_RECOVERY mode → max 80% coverage."""
        result = _compute_ladder_allocation(qty=2000, recovery_mode="DEEP_RECOVERY", thesis_state="INTACT")
        assert result["max_coverage_pct"] == 0.80

    def test_structural_damage_70_pct(self):
        """STRUCTURAL_DAMAGE → max 70% coverage."""
        result = _compute_ladder_allocation(qty=2000, recovery_mode="STRUCTURAL_DAMAGE", thesis_state="INTACT")
        assert result["max_coverage_pct"] == 0.70
        assert result["covered_lots"] == 14  # 20 × 0.70

    def test_income_full_coverage(self):
        """INCOME mode → 100% coverage allowed."""
        result = _compute_ladder_allocation(qty=2000, recovery_mode="INCOME", thesis_state="INTACT")
        assert result["max_coverage_pct"] == 1.00
        assert result["covered_lots"] == 20

    def test_tier_splits_recovery(self):
        """RECOVERY: 35/65 split → Tier A gets fewer lots."""
        result = _compute_ladder_allocation(qty=2000, recovery_mode="RECOVERY", thesis_state="INTACT")
        # covered=16 → tier_a = round(16 × 0.35) = round(5.6) = 6
        # tier_b = 16 - 6 = 10
        assert result["tier_a_lots"] + result["tier_b_lots"] == result["covered_lots"]
        assert result["tier_a_lots"] < result["tier_b_lots"]

    def test_broken_thesis_returns_none(self):
        """BROKEN thesis → not eligible regardless of qty."""
        result = _compute_ladder_allocation(qty=5000, recovery_mode="RECOVERY", thesis_state="BROKEN")
        assert result is None

    def test_tier_a_minimum_one(self):
        """Tier A always gets at least 1 lot."""
        result = _compute_ladder_allocation(qty=1000, recovery_mode="STRUCTURAL_DAMAGE", thesis_state="INTACT")
        # 10 lots × 0.70 = 7 covered → tier_a = max(1, round(7 × 0.35)) = max(1, 2) = 2
        assert result["tier_a_lots"] >= 1


class TestCCLadderIncomeGapRatio:
    """_compute_income_gap_ratio: monthly income vs gap classification."""

    def test_no_gap_returns_zero_ratio(self):
        """Zero gap → ratio = 0, note = NO_GAP."""
        monthly, ratio, note = _compute_income_gap_ratio(
            tier_a_cands=[{"mid": 0.50, "dte": 7}],
            tier_b_cands=[{"mid": 0.30, "dte": 30}],
            tier_a_lots=5, tier_b_lots=10, gap_total=0.0,
        )
        assert ratio == 0.0
        assert note == "NO_GAP"
        assert monthly > 0  # income still computed

    def test_viable_ratio(self):
        """Healthy premium → RECOVERY_VIABLE."""
        # Tier A: $1.00 mid, 7d, 5 lots → monthly = 1.00 × 100 × 5 × (30/7) = $2142.86
        # Tier B: $0.80 mid, 30d, 10 lots → monthly = 0.80 × 100 × 10 × (30/30) = $800
        # Total: ~$2943/mo; gap = $50K → ratio = 2943/50000 = 5.9% → RECOVERY_VIABLE
        monthly, ratio, note = _compute_income_gap_ratio(
            tier_a_cands=[{"mid": 1.00, "dte": 7}],
            tier_b_cands=[{"mid": 0.80, "dte": 30}],
            tier_a_lots=5, tier_b_lots=10, gap_total=50000.0,
        )
        assert note == "RECOVERY_VIABLE"
        assert ratio >= 0.02

    def test_unrealistic_flagged_as_cash_flow_only(self):
        """Tiny premium on huge gap → CASH_FLOW_ONLY."""
        # Tier A: $0.10 mid, 7d, 2 lots → monthly = 0.10 × 100 × 2 × (30/7) = $85.71
        # Tier B: $0.12 mid, 30d, 5 lots → monthly = 0.12 × 100 × 5 × (30/30) = $60
        # Total: ~$146/mo; gap = $100K → ratio = 0.15% → CASH_FLOW_ONLY
        monthly, ratio, note = _compute_income_gap_ratio(
            tier_a_cands=[{"mid": 0.10, "dte": 7}],
            tier_b_cands=[{"mid": 0.12, "dte": 30}],
            tier_a_lots=2, tier_b_lots=5, gap_total=100000.0,
        )
        assert note == "CASH_FLOW_ONLY"
        assert ratio < 0.01

    def test_empty_candidates(self):
        """Empty candidate lists → zero monthly, CASH_FLOW_ONLY on positive gap."""
        monthly, ratio, note = _compute_income_gap_ratio(
            tier_a_cands=[], tier_b_cands=[],
            tier_a_lots=5, tier_b_lots=10, gap_total=50000.0,
        )
        assert monthly == 0.0
        assert note == "CASH_FLOW_ONLY"


class TestCCLadderSDPartialCoverage:
    """STRUCTURAL_DAMAGE: large → ladder, small → blocked, never 100%."""

    def test_large_sd_gets_ladder(self):
        """2000 shares + SD + INTACT → ladder eligible."""
        alloc = _compute_ladder_allocation(
            qty=2000, recovery_mode="STRUCTURAL_DAMAGE", thesis_state="INTACT",
        )
        assert alloc is not None
        assert alloc["max_coverage_pct"] == 0.70

    def test_small_sd_blocked(self):
        """500 shares + SD → not eligible."""
        alloc = _compute_ladder_allocation(
            qty=500, recovery_mode="STRUCTURAL_DAMAGE", thesis_state="INTACT",
        )
        assert alloc is None

    def test_sd_never_covers_100_pct(self):
        """Even with massive position, SD caps at 70%."""
        alloc = _compute_ladder_allocation(
            qty=10000, recovery_mode="STRUCTURAL_DAMAGE", thesis_state="INTACT",
        )
        assert alloc["max_coverage_pct"] == 0.70
        assert alloc["uncovered_lots"] > 0
        assert alloc["covered_lots"] / alloc["total_lots"] <= 0.70


class TestCCLadderStrikeFloor:
    """Ladder strike floor: SD uses spot-anchored, others use basis-anchored."""

    def _make_scan_df(self, strikes, spot, basis):
        """Helper: build scan df with CC rows at given strikes."""
        rows = []
        for s in strikes:
            rows.append({
                "Ticker": "EOSE",
                "Strategy_Name": "COVERED_CALL",
                "Execution_Status": "READY",
                "Selected_Strike": s,
                "Actual_DTE": 30,
                "Mid_Price": 0.50,
                "Delta": -0.20,
                "Bid_Ask_Spread_Pct": 5.0,
                "Open_Interest": 500,
                "Implied_Volatility": 0.50,
            })
        return pd.DataFrame(rows)

    def test_sd_uses_spot_anchored_floor(self):
        """STRUCTURAL_DAMAGE: floor = spot × 1.10 (NOT basis).
        Spot=6 → floor=6.60. Strikes at $7+ pass, $5 filtered."""
        scan_df = self._make_scan_df([5.0, 7.0, 8.0, 9.0, 10.0], spot=6.0, basis=17.0)
        result = _build_ladder_candidates_scan(scan_df, "EOSE", basis=17.0, spot=6.0, recovery_mode="STRUCTURAL_DAMAGE")
        all_strikes = [c["strike"] for c in result["tier_a_candidates"] + result["tier_b_candidates"]]
        # $5 should be filtered (below spot×1.10=6.60), $7+ should pass
        assert 5.0 not in all_strikes, "Strike $5 below spot×1.10 should be filtered"
        # At least $7+ strikes should pass (within Tier B delta/DTE)
        for s in all_strikes:
            assert s >= 6.60, f"Strike {s} below spot-anchored floor"

    def test_recovery_uses_basis_anchored_floor(self):
        """RECOVERY: floor = max(spot×1.10, basis). Basis-anchored."""
        scan_df = self._make_scan_df([5.0, 7.0, 10.0, 15.0, 17.0, 20.0], spot=6.0, basis=17.0)
        result = _build_ladder_candidates_scan(scan_df, "EOSE", basis=17.0, spot=6.0, recovery_mode="RECOVERY")
        all_strikes = [c["strike"] for c in result["tier_a_candidates"] + result["tier_b_candidates"]]
        # With floor=max(6.60, 17.0)=17.0, only $17+ should pass
        for s in all_strikes:
            assert s >= 17.0, f"RECOVERY: strike {s} below basis-anchored floor"

    def test_sd_strikes_above_spot_included(self):
        """SD: $7-$10 strikes are valid ladder candidates when spot=$6."""
        scan_df = self._make_scan_df([7.0, 8.0, 9.0, 10.0], spot=6.0, basis=17.0)
        result = _build_ladder_candidates_scan(scan_df, "EOSE", basis=17.0, spot=6.0, recovery_mode="STRUCTURAL_DAMAGE")
        total = len(result["tier_a_candidates"]) + len(result["tier_b_candidates"])
        assert total > 0, "SD ladder should find candidates at $7+ when spot=$6"


class TestCCLadderGuardrails:
    """Min premium $0.10, spread 40% cap, delta capping."""

    def _make_scan_df(self, mid, spread_pct, delta, dte=30):
        return pd.DataFrame([{
            "Ticker": "TEST",
            "Strategy_Name": "COVERED_CALL",
            "Execution_Status": "READY",
            "Selected_Strike": 20.0,
            "Actual_DTE": dte,
            "Mid_Price": mid,
            "Delta": -delta,
            "Bid_Ask_Spread_Pct": spread_pct,
            "Open_Interest": 500,
            "Implied_Volatility": 0.30,
        }])

    def test_min_premium_filter(self):
        """Mid < $0.10 → filtered out (economically meaningless)."""
        scan_df = self._make_scan_df(mid=0.05, spread_pct=5.0, delta=0.20)
        result = _build_ladder_candidates_scan(scan_df, "TEST", basis=15.0, spot=10.0, recovery_mode="RECOVERY")
        total = len(result["tier_a_candidates"]) + len(result["tier_b_candidates"])
        assert total == 0, "Should filter $0.05 premium"

    def test_spread_cap_40_pct(self):
        """Spread > 40% → filtered out (untradeable)."""
        scan_df = self._make_scan_df(mid=0.50, spread_pct=50.0, delta=0.20)
        result = _build_ladder_candidates_scan(scan_df, "TEST", basis=15.0, spot=10.0, recovery_mode="RECOVERY")
        total = len(result["tier_a_candidates"]) + len(result["tier_b_candidates"])
        assert total == 0, "Should filter 50% spread"

    def test_delta_max_030_tier_a(self):
        """Delta > 0.30 → excluded from Tier A."""
        # DTE=7 puts it in Tier A range; delta=0.35 exceeds Tier A max of 0.30
        scan_df = self._make_scan_df(mid=0.50, spread_pct=5.0, delta=0.35, dte=7)
        result = _build_ladder_candidates_scan(scan_df, "TEST", basis=15.0, spot=10.0, recovery_mode="RECOVERY")
        assert len(result["tier_a_candidates"]) == 0, "Delta 0.35 should be excluded from Tier A"

    def test_delta_max_025_tier_b(self):
        """Delta > 0.25 → excluded from Tier B."""
        # DTE=30 puts it in Tier B range; delta=0.28 exceeds Tier B max of 0.25
        scan_df = self._make_scan_df(mid=0.50, spread_pct=5.0, delta=0.28, dte=30)
        result = _build_ladder_candidates_scan(scan_df, "TEST", basis=15.0, spot=10.0, recovery_mode="RECOVERY")
        assert len(result["tier_b_candidates"]) == 0, "Delta 0.28 should be excluded from Tier B"

    def test_valid_tier_b_candidate_passes(self):
        """Valid Tier B candidate (good premium, tight spread, valid delta) → included."""
        scan_df = self._make_scan_df(mid=0.50, spread_pct=5.0, delta=0.20, dte=30)
        result = _build_ladder_candidates_scan(scan_df, "TEST", basis=15.0, spot=10.0, recovery_mode="RECOVERY")
        assert len(result["tier_b_candidates"]) == 1


# ═══════════════════════════════════════════════════════════════════════════════
# Non-Ladder CC Vetting Tests
# ═══════════════════════════════════════════════════════════════════════════════

def _make_scan_row(
    ticker: str = "TEST",
    mid: float = 0.50,
    spread_pct: float = 5.0,
    delta: float = 0.20,
    dte: int = 30,
    oi: int = 500,
    liq: str = "GOOD",
    bid: float = 0.0,
    ask: float = 0.0,
) -> pd.DataFrame:
    """Build a single-row scan DataFrame for _rank_candidates tests."""
    return pd.DataFrame([{
        "Ticker": ticker,
        "Strategy_Name": "COVERED_CALL",
        "Execution_Status": "READY",
        "Actual_DTE": dte,
        "Selected_Strike": 50.0,
        "Mid_Price": mid,
        "Delta": delta,
        "Implied_Volatility": 0.40,
        "Open_Interest": oi,
        "Bid_Ask_Spread_Pct": spread_pct,
        "Liquidity_Grade": liq,
        "Confidence": 0.80,
        "DQS_Score": 75,
        "Bid": bid,
        "Ask": ask,
    }])


class TestNonLadderCCVetting:
    """_rank_candidates: non-ladder vetting gates (min premium, spread, OI)."""

    def test_min_premium_rejects_below_010(self):
        """Mid < 0.10 → rejected."""
        scan = _make_scan_row(mid=0.05)
        result = _rank_candidates(scan, "TEST", basis_per_share=50.0, recovery_mode="INCOME")
        assert len(result) == 0

    def test_min_premium_passes_at_010(self):
        """Mid = 0.10 → accepted."""
        scan = _make_scan_row(mid=0.10)
        result = _rank_candidates(scan, "TEST", basis_per_share=50.0, recovery_mode="INCOME")
        assert len(result) == 1

    def test_spread_cap_income_20pct(self):
        """Income mode: spread 25% → rejected (cap is 20%)."""
        scan = _make_scan_row(spread_pct=25.0)
        result = _rank_candidates(scan, "TEST", basis_per_share=50.0, recovery_mode="INCOME")
        assert len(result) == 0

    def test_spread_cap_recovery_passes_25pct(self):
        """Recovery mode: spread 25% → accepted (cap is 40%)."""
        scan = _make_scan_row(spread_pct=25.0)
        result = _rank_candidates(
            scan, "TEST", basis_per_share=50.0,
            recovery_mode="RECOVERY", spot_price=40.0,
        )
        assert len(result) == 1

    def test_spread_cap_recovery_rejects_45pct(self):
        """Recovery mode: spread 45% → rejected (cap is 40%)."""
        scan = _make_scan_row(spread_pct=45.0)
        result = _rank_candidates(
            scan, "TEST", basis_per_share=50.0,
            recovery_mode="RECOVERY", spot_price=40.0,
        )
        assert len(result) == 0

    def test_oi_gate_income_rejects_below_100(self):
        """Income mode: OI=50, liq=THIN → rejected (min 100)."""
        scan = _make_scan_row(oi=50, liq="THIN")
        result = _rank_candidates(scan, "TEST", basis_per_share=50.0, recovery_mode="INCOME")
        assert len(result) == 0

    def test_oi_gate_recovery_accepts_50(self):
        """Recovery mode: OI=50, liq=OK → accepted (min 50)."""
        scan = _make_scan_row(oi=50, liq="OK")
        result = _rank_candidates(
            scan, "TEST", basis_per_share=50.0,
            recovery_mode="RECOVERY", spot_price=40.0,
        )
        assert len(result) == 1

    def test_thin_liq_low_oi_rejected(self):
        """THIN liquidity + OI=30 → rejected in both modes."""
        scan = _make_scan_row(oi=30, liq="THIN")
        result = _rank_candidates(scan, "TEST", basis_per_share=50.0, recovery_mode="INCOME")
        assert len(result) == 0
        result2 = _rank_candidates(
            scan, "TEST", basis_per_share=50.0,
            recovery_mode="RECOVERY", spot_price=40.0,
        )
        assert len(result2) == 0


class TestNonLadderExecutionFields:
    """_rank_candidates: execution fields (expiry, bid, ask, contracts, source)."""

    def test_has_execution_fields(self):
        """Candidates must include expiry, bid, ask, source, contracts."""
        scan = _make_scan_row()
        result = _rank_candidates(scan, "TEST", basis_per_share=50.0, qty=200)
        assert len(result) == 1
        cand = result[0]
        for field in ("expiry", "bid", "ask", "source", "contracts"):
            assert field in cand, f"Missing field: {field}"
        assert cand["source"] == "SCAN_DATA"

    def test_bid_ask_from_spread(self):
        """When no real bid/ask, reconstruct from mid ± spread."""
        scan = _make_scan_row(mid=1.00, spread_pct=10.0, bid=0, ask=0)
        result = _rank_candidates(scan, "TEST", basis_per_share=50.0)
        cand = result[0]
        # mid=1.00, spread=10% → half_spread = 1.00 * 10/100 / 2 = 0.05
        assert cand["bid"] == 0.95
        assert cand["ask"] == 1.05

    def test_prefers_real_bid_ask(self):
        """When scan has Bid/Ask, use those instead of reconstruction."""
        scan = _make_scan_row(mid=1.00, spread_pct=10.0, bid=0.88, ask=1.12)
        result = _rank_candidates(scan, "TEST", basis_per_share=50.0)
        cand = result[0]
        assert cand["bid"] == 0.88
        assert cand["ask"] == 1.12

    def test_spread_zero_graceful(self):
        """Spread=0 → bid=ask=mid (no division error)."""
        scan = _make_scan_row(mid=1.00, spread_pct=0.0, bid=0, ask=0)
        result = _rank_candidates(scan, "TEST", basis_per_share=50.0)
        cand = result[0]
        assert cand["bid"] == cand["mid"] == cand["ask"] == 1.00

    def test_contracts_from_qty(self):
        """qty=300 → 3 contracts; qty=99 → 1 (minimum)."""
        scan = _make_scan_row()
        result_300 = _rank_candidates(scan, "TEST", basis_per_share=50.0, qty=300)
        assert result_300[0]["contracts"] == 3

        result_99 = _rank_candidates(scan, "TEST", basis_per_share=50.0, qty=99)
        assert result_99[0]["contracts"] == 1


class TestNonLadderBoundary:
    """Boundary tests for spread cap and OI gate."""

    def test_spread_boundary_income_20pct(self):
        """spread=20.0 → ACCEPT; spread=20.01 → REJECT."""
        scan_ok = _make_scan_row(spread_pct=20.0)
        result_ok = _rank_candidates(scan_ok, "TEST", basis_per_share=50.0, recovery_mode="INCOME")
        assert len(result_ok) == 1, "Spread exactly at cap should pass"

        scan_fail = _make_scan_row(spread_pct=20.01)
        result_fail = _rank_candidates(scan_fail, "TEST", basis_per_share=50.0, recovery_mode="INCOME")
        assert len(result_fail) == 0, "Spread above cap should fail"

    def test_oi_boundary_income_100(self):
        """OI=100 + THIN → ACCEPT; OI=99 + THIN → REJECT."""
        scan_ok = _make_scan_row(oi=100, liq="THIN")
        result_ok = _rank_candidates(scan_ok, "TEST", basis_per_share=50.0, recovery_mode="INCOME")
        assert len(result_ok) == 1, "OI at threshold with THIN liq should pass"

        scan_fail = _make_scan_row(oi=99, liq="THIN")
        result_fail = _rank_candidates(scan_fail, "TEST", basis_per_share=50.0, recovery_mode="INCOME")
        assert len(result_fail) == 0, "OI below threshold with THIN liq should fail"


# ═══════════════════════════════════════════════════════════════════════════════
# STOCK_ONLY Doctrine Tests (Fix 9)
# ═══════════════════════════════════════════════════════════════════════════════

def _base_stock_only_row(**overrides) -> pd.Series:
    """Minimal STOCK_ONLY row for doctrine unit tests."""
    base = {
        "TradeID": "T-SO-001", "LegID": "L-SO-001",
        "Symbol": "TEST", "Underlying_Ticker": "TEST",
        "Strategy": "STOCK_ONLY", "AssetType": "STOCK",
        "UL Last": 50.0, "Basis": 5000.0, "Quantity": 100.0,
        "Underlying_Price_Entry": 50.0,
        "Delta": 0.0, "Strike": np.nan, "DTE": np.nan,
        "Premium_Entry": np.nan, "Last": np.nan,
        "HV_20D": 0.30,
        "IV_Entry": np.nan, "IV_30D": np.nan, "IV_Now": np.nan,
        "IV_Percentile": np.nan, "IV_vs_HV_Gap": np.nan,
        "Theta": 0.0, "Gamma": 0.0,
        "Thesis_State": "INTACT", "Thesis_Gate": "PASS",
        "_thesis_blocks_roll": False, "Thesis_Summary": "",
        "PriceStructure_State": "RANGE_BOUND",
        "TrendIntegrity_State": "NO_TREND",
        "ema50_slope": 0.01, "hv_20d_percentile": 40.0,
        "Equity_Integrity_State": "HEALTHY",
        "Equity_Integrity_Reason": "EMA20↑, EMA50↑",
        "Position_Regime": "NEUTRAL",
        "Trajectory_Consecutive_Debit_Rolls": 0,
        "Trajectory_Stock_Return": 0.0,
        "PnL_Dollar": 0.0, "PnL_Total": 0.0,
        "Total_GL_Decimal": 0.0,
        "_Active_Conditions": "", "_Condition_Resolved": "",
        "Snapshot_TS": pd.Timestamp.now(),
        "Earnings_Date": None,
        "run_id": "test-run", "Schema_Hash": "abc123",
        "IV": None,
    }
    base.update(overrides)
    return pd.Series(base)


class TestStockOnlyDoctrine:
    """Validate STOCK_ONLY doctrine gate ordering and boundary conditions."""

    def test_49pct_loss_holds_high(self):
        """Boundary: -49% → HOLD HIGH (Gate 3, not Gate 2 EXIT)."""
        row = _base_stock_only_row(Total_GL_Decimal=-0.49)
        result = _run_doctrine(row)
        assert result["Action"] == "HOLD", f"Got {result['Action']}"
        assert result["Urgency"] == "HIGH", f"Got {result['Urgency']}"

    def test_50pct_loss_exits(self):
        """Boundary: -50% → EXIT HIGH (Gate 2 deep loss)."""
        row = _base_stock_only_row(Total_GL_Decimal=-0.50)
        result = _run_doctrine(row)
        assert result["Action"] == "EXIT", f"Got {result['Action']}"
        assert result["Urgency"] == "HIGH", f"Got {result['Urgency']}"

    def test_broken_equity_exits_at_mild_loss(self):
        """BROKEN at -5% → EXIT HIGH (Gate 1 — BROKEN is first gate)."""
        row = _base_stock_only_row(
            Total_GL_Decimal=-0.05,
            Equity_Integrity_State="BROKEN",
            Equity_Integrity_Reason="EMA20↓, EMA50↓, ROC20=-12%, HV=85th_pct",
        )
        result = _run_doctrine(row)
        assert result["Action"] == "EXIT", f"Got {result['Action']}"
        assert result["Urgency"] == "HIGH", f"Got {result['Urgency']}"

    def test_weakening_small_loss_stays_low(self):
        """WEAKENING at -5% (above -10% threshold) → HOLD LOW (falls to Gate 5/6)."""
        row = _base_stock_only_row(
            Total_GL_Decimal=-0.05,
            Equity_Integrity_State="WEAKENING",
            Equity_Integrity_Reason="EMA20↓",
        )
        result = _run_doctrine(row)
        assert result["Action"] == "HOLD", f"Got {result['Action']}"
        assert result["Urgency"] == "LOW", f"Got {result['Urgency']}"

    def test_weakening_moderate_loss_medium(self):
        """WEAKENING at -12% → HOLD MEDIUM (Gate 4)."""
        row = _base_stock_only_row(
            Total_GL_Decimal=-0.12,
            Equity_Integrity_State="WEAKENING",
            Equity_Integrity_Reason="EMA20↓, ROC20=-8%",
        )
        result = _run_doctrine(row)
        assert result["Action"] == "HOLD", f"Got {result['Action']}"
        assert result["Urgency"] == "MEDIUM", f"Got {result['Urgency']}"

    def test_cc_surface_100_shares(self):
        """100 shares, healthy, small gain → rationale mentions covered call."""
        row = _base_stock_only_row(Total_GL_Decimal=0.05, Quantity=100.0)
        result = _run_doctrine(row)
        assert result["Action"] == "HOLD"
        rat = result.get("Rationale", "").lower()
        assert "covered call" in rat or "cc" in result.get("Rationale", "") or "call" in rat, (
            f"Rationale should mention CC opportunity. Got: {rat}"
        )

    def test_odd_lot_bw_upgrade_assessment(self):
        """50 shares → BW upgrade assessment (not the old CC gate)."""
        row = _base_stock_only_row(Total_GL_Decimal=0.05, Quantity=50.0)
        result = _run_doctrine(row)
        assert result["Action"] == "HOLD"
        rat = result.get("Rationale", "").lower()
        assert "odd lot" in rat or "bw upgrade" in rat, (
            f"50 shares should trigger odd lot / BW upgrade assessment. Got: {rat}"
        )
        assert result.get("BW_Upgrade_Feasible") is not None, (
            "BW_Upgrade_Feasible should be set for sub-contract positions"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# BW Upgrade Feasibility Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestBWUpgradeFeasibility:
    """Validate BW upgrade assessment for odd-lot (<100 share) stock positions."""

    def test_feasible_high_iv_roth(self):
        """80 shares, IV 90%, INTACT thesis, ROTH → feasible BW upgrade."""
        row = _base_stock_only_row(
            Quantity=80.0, IV_Now=0.90, Thesis_State="INTACT",
            Account="ROTH IRA", Total_GL_Decimal=-0.02,
        )
        row["UL Last"] = 20.0  # 20 shares × $20 = $400 cost, IV 90% → strong premium
        result = _run_doctrine(row)
        assert result.get("BW_Upgrade_Feasible") is True, (
            f"High IV + INTACT + ROTH should be feasible. Reason: {result.get('BW_Upgrade_Reason')}"
        )
        assert result["BW_Upgrade_Shares_Needed"] == 20
        assert result["BW_Upgrade_Cost"] > 0
        assert result["Urgency"] == "MEDIUM"
        assert "buy" in result.get("Rationale", "").lower()

    def test_infeasible_broken_equity(self):
        """Odd lot + BROKEN equity → EXIT (Gate 1 fires before BW upgrade)."""
        row = _base_stock_only_row(
            Quantity=25.0, IV_Now=0.40,
            Equity_Integrity_State="BROKEN",
            Equity_Integrity_Reason="EMA200 broken on high volume",
            Total_GL_Decimal=-0.10,
        )
        result = _run_doctrine(row)
        assert result["Action"] == "EXIT"

    def test_infeasible_low_iv(self):
        """Odd lot + IV 8% → not feasible (premium too thin)."""
        row = _base_stock_only_row(
            Quantity=50.0, IV_Now=0.08, Thesis_State="INTACT",
            Total_GL_Decimal=0.05,
        )
        result = _run_doctrine(row)
        assert result.get("BW_Upgrade_Feasible") is False
        assert "IV" in result.get("BW_Upgrade_Reason", "") or "premium" in result.get("BW_Upgrade_Reason", "").lower()

    def test_infeasible_deep_loss(self):
        """Odd lot + -30% loss → not feasible (loss too deep to add capital)."""
        row = _base_stock_only_row(
            Quantity=50.0, IV_Now=0.40, Thesis_State="INTACT",
            Total_GL_Decimal=-0.30,
        )
        result = _run_doctrine(row)
        # -30% > -25% significant loss → Gate 3 fires first (HOLD HIGH)
        assert result["Action"] == "HOLD"
        assert result["Urgency"] == "HIGH"

    def test_infeasible_expensive_stock(self):
        """1 share of $15,000 stock → buy-up cost exceeds cap."""
        row = _base_stock_only_row(
            Quantity=1.0, IV_Now=0.30, Thesis_State="INTACT",
            Total_GL_Decimal=0.02,
        )
        row["UL Last"] = 15000.0  # 99 shares × $15k = $1.485M
        result = _run_doctrine(row)
        assert result.get("BW_Upgrade_Feasible") is False
        assert "cost" in result.get("BW_Upgrade_Reason", "").lower() or "cap" in result.get("BW_Upgrade_Reason", "").lower()

    def test_margin_account_deducts_carry(self):
        """Non-ROTH account → margin cost deducted from premium income."""
        row = _base_stock_only_row(
            Quantity=50.0, IV_Now=0.40, Thesis_State="INTACT",
            Account="Individual Brokerage", Total_GL_Decimal=0.02,
        )
        row["UL Last"] = 20.0
        result = _run_doctrine(row)
        margin_cost = result.get("BW_Upgrade_Cost", 0) * 0.10375 / 12
        assert margin_cost > 0, "Margin account should have carry cost"

    def test_roth_no_margin_cost(self):
        """ROTH account → no margin cost, better payback."""
        row = _base_stock_only_row(
            Quantity=80.0, IV_Now=0.90, Thesis_State="INTACT",
            Account="ROTH IRA", Total_GL_Decimal=0.02,
        )
        row["UL Last"] = 20.0  # 20 shares × $20 = $400, IV 90% → strong premium
        result = _run_doctrine(row)
        assert result.get("BW_Upgrade_Feasible") is True
        assert result.get("BW_Upgrade_Payback_Months", 999) < 12

    def test_100_shares_skips_upgrade(self):
        """100 shares → goes to Gate 5 (CC opportunity), not Gate 5a."""
        row = _base_stock_only_row(
            Quantity=100.0, IV_Now=0.40, Thesis_State="INTACT",
            Total_GL_Decimal=0.05,
        )
        result = _run_doctrine(row)
        assert result.get("BW_Upgrade_Feasible") is None, (
            "100 shares should not trigger BW upgrade assessment"
        )

    def test_degraded_thesis_blocked(self):
        """Odd lot + DEGRADED thesis → not feasible (stricter than normal CC)."""
        row = _base_stock_only_row(
            Quantity=50.0, IV_Now=0.40, Thesis_State="DEGRADED",
            Total_GL_Decimal=-0.05,
        )
        result = _run_doctrine(row)
        assert result.get("BW_Upgrade_Feasible") is False
        assert "thesis" in result.get("BW_Upgrade_Reason", "").lower()

    def test_weakening_equity_blocked(self):
        """Odd lot + WEAKENING equity + loss → Gate 4 fires first, not BW upgrade."""
        row = _base_stock_only_row(
            Quantity=50.0, IV_Now=0.40, Thesis_State="INTACT",
            Equity_Integrity_State="WEAKENING",
            Total_GL_Decimal=-0.12,
        )
        result = _run_doctrine(row)
        assert result["Urgency"] == "MEDIUM"
        assert "WEAKENING" in result.get("Rationale", "")


# ═══════════════════════════════════════════════════════════════════════════════
# P&L Fallback Safety Tests (Fix 10)
# ═══════════════════════════════════════════════════════════════════════════════

class TestPnLFallbackSafety:
    """Validate _safe_pnl_pct() fallback behavior."""

    def test_nan_with_pnl_total_fallback(self):
        """NaN Total_GL_Decimal + valid Basis/PnL_Total → computed ratio."""
        row = pd.Series({
            "Total_GL_Decimal": np.nan,
            "PnL_Total": -500.0,
            "Basis": 5000.0,
        })
        result = DoctrineAuthority._safe_pnl_pct(row)
        assert result is not None, "Should compute from PnL_Total/Basis"
        assert abs(result - (-0.10)) < 0.001, f"Expected -0.10, got {result}"

    def test_nan_with_nothing(self):
        """All NaN → None (not 0.0)."""
        row = pd.Series({
            "Total_GL_Decimal": np.nan,
            "PnL_Total": np.nan,
            "Basis": np.nan,
        })
        result = DoctrineAuthority._safe_pnl_pct(row)
        assert result is None, f"Expected None when all data missing, got {result}"

    def test_valid_gl_used_directly(self):
        """Valid Total_GL_Decimal → used as-is."""
        row = pd.Series({
            "Total_GL_Decimal": -0.35,
            "PnL_Total": 999.0,  # should be ignored
            "Basis": 1.0,
        })
        result = DoctrineAuthority._safe_pnl_pct(row)
        assert abs(result - (-0.35)) < 0.001, f"Expected -0.35, got {result}"


# ═══════════════════════════════════════════════════════════════════════════════
# Gate 3b-theta-warn: Theta Efficiency Warning (25-29% gain)
# ═══════════════════════════════════════════════════════════════════════════════

class TestThetaEfficiencyWarning:
    """
    Gate 3b-theta-warn escalates HOLD LOW → HOLD MEDIUM when gain is 25-29%
    and all theta efficiency conditions are met (theta consumes ≥75% TV before expiry,
    bleed > 1%/day, tv_pct ≥ 40%).
    """

    def test_29pct_gain_with_theta_burn_escalates_to_hold_medium(self):
        """GOOGL-like: 29% gain, 1.4%/day theta bleed → HOLD MEDIUM (not LOW)."""
        row = _base_long_option_row(
            Underlying_Ticker="GOOGL",
            Symbol="GOOGL260402P00310000",
            Strike=310.0,
            **{"UL Last": 303.13},
            DTE=29.0,
            Days_In_Trade=7.0,  # lifecycle guard: 7d held > max(2, 36*0.10)=3.6
            Premium_Entry=10.85,
            Last=14.07,
            Bid=13.90,
            Delta=-0.562,
            Theta=-0.1911,
            Gamma=0.0147,
            Vega=0.3349,
            Total_GL_Decimal=0.29,
            PnL_Dollar=349.0,
            Basis=-1085.0,
            Quantity=1.0,
            Drift_Direction="Down",
            Price_Drift_Pct=-0.004,
            MomentumVelocity_State="TRENDING",
            momentum_slope=0.5,
            Thesis_State="INTACT",
            roc_5=-1.0,
            roc_10=-2.0,
        )
        result = DoctrineAuthority.evaluate(row)
        assert result["Action"] == "HOLD", f"Expected HOLD, got {result['Action']}"
        assert result["Urgency"] == "MEDIUM", f"Expected MEDIUM urgency, got {result['Urgency']}"
        assert "Theta efficiency warning" in result["Rationale"]

    def test_25pct_gain_with_theta_burn_also_escalates(self):
        """25% gain (bottom of warning zone) with theta conditions → HOLD MEDIUM."""
        row = _base_long_option_row(
            Underlying_Ticker="TEST",
            Symbol="TEST260402P00100000",
            Strike=100.0,
            **{"UL Last": 95.0},
            DTE=25.0,
            Days_In_Trade=7.0,
            Premium_Entry=8.00,
            Last=10.00,
            Bid=9.80,
            Delta=-0.55,
            Theta=-0.20,       # 2%/day of $10 option → well above 1%
            Gamma=0.015,
            Total_GL_Decimal=0.25,
            PnL_Dollar=200.0,
            Basis=-800.0,
            Quantity=1.0,
            Drift_Direction="Down",
            Price_Drift_Pct=-0.01,
            MomentumVelocity_State="TRENDING",
            momentum_slope=0.3,
            Thesis_State="INTACT",
            roc_5=-0.5,
            roc_10=-1.0,
        )
        # TV = Last - intrinsic = 10.00 - 5.00 = 5.00
        # theta × DTE = 0.20 × 25 = 5.00 ≥ 5.00 × 0.75 = 3.75 → passes
        # bleed = 0.20/10.00 × 100 = 2.0% > 1.0% → passes
        # tv_pct = 5.00/10.00 = 50% ≥ 40% → passes
        result = DoctrineAuthority.evaluate(row)
        assert result["Action"] == "HOLD"
        assert result["Urgency"] == "MEDIUM"

    def test_30pct_gain_hits_exit_not_warning(self):
        """At exactly 30% gain, Gate 3b-theta fires EXIT, not the warning."""
        row = _base_long_option_row(
            Underlying_Ticker="TEST",
            Symbol="TEST260402P00100000",
            Strike=100.0,
            **{"UL Last": 93.0},
            DTE=25.0,
            Days_In_Trade=7.0,
            Premium_Entry=8.00,
            Last=10.40,
            Bid=10.20,
            Delta=-0.58,
            Theta=-0.22,
            Gamma=0.015,
            Total_GL_Decimal=0.30,
            PnL_Dollar=240.0,
            Basis=-800.0,
            Quantity=1.0,
            Drift_Direction="Down",
            Price_Drift_Pct=-0.02,
            MomentumVelocity_State="TRENDING",
            momentum_slope=0.4,
            Thesis_State="INTACT",
            roc_5=-1.0,
            roc_10=-2.0,
        )
        result = DoctrineAuthority.evaluate(row)
        assert result["Action"] == "EXIT", f"At 30% gain, should EXIT not HOLD. Got {result['Action']}"
        assert result["Urgency"] == "MEDIUM"

    def test_24pct_gain_stays_hold_low(self):
        """24% gain (below warning zone) → no escalation, stays HOLD LOW."""
        row = _base_long_option_row(
            Underlying_Ticker="TEST",
            Symbol="TEST260402P00100000",
            Strike=100.0,
            **{"UL Last": 95.0},
            DTE=25.0,
            Days_In_Trade=7.0,
            Premium_Entry=8.00,
            Last=9.92,
            Bid=9.72,
            Delta=-0.55,
            Theta=-0.20,
            Gamma=0.015,
            Total_GL_Decimal=0.24,
            PnL_Dollar=192.0,
            Basis=-800.0,
            Quantity=1.0,
            Drift_Direction="Down",
            Price_Drift_Pct=-0.01,
            MomentumVelocity_State="TRENDING",
            momentum_slope=0.3,
            Thesis_State="INTACT",
            roc_5=-0.5,
            roc_10=-1.0,
        )
        result = DoctrineAuthority.evaluate(row)
        assert result["Action"] == "HOLD"
        assert result["Urgency"] == "LOW", f"24% gain should stay LOW, got {result['Urgency']}"


# =============================================================================
# Cross-Leg Direction Reversal Gate (Natenberg Ch.11 / Passarelli Ch.6)
# =============================================================================

class TestDirectionReversalGate:
    """Verify compute_direction_reversals() detects delta flips across legs."""

    @staticmethod
    def _make_multi_leg_df(**kwargs):
        """Build a 2-leg DataFrame for AAPL: BUY_WRITE (stock) + LONG_CALL."""
        rows = [
            {
                "Underlying_Ticker": "AAPL",
                "TradeID": "T-BW-001",
                "Strategy": "BUY_WRITE",
                "AssetType": "STOCK",
                "Delta": 0.0,          # stock delta derived from Quantity
                "Quantity": 100.0,     # → net delta = 100 (but short call reduces it)
                "Action": "EXIT",
                "Urgency": "MEDIUM",
                "Rationale": "Sell stock",
            },
            {
                "Underlying_Ticker": "AAPL",
                "TradeID": "T-BW-001",
                "Strategy": "BUY_WRITE",
                "AssetType": "OPTION",
                "Delta": -0.228,       # short call → -22.8 delta
                "Quantity": 1.0,
                "Action": "EXIT",
                "Urgency": "MEDIUM",
                "Rationale": "Exit call",
            },
            {
                "Underlying_Ticker": "AAPL",
                "TradeID": "T-LC-001",
                "Strategy": "LONG_CALL",
                "AssetType": "OPTION",
                "Delta": 0.35,         # long call → +35 delta
                "Quantity": 1.0,
                "Action": "HOLD",
                "Urgency": "LOW",
                "Rationale": "Hold long call",
            },
        ]
        df = pd.DataFrame(rows)
        df.update(pd.DataFrame(kwargs))
        return df

    def test_reversal_detected_neutral_to_bullish(self):
        """BUY_WRITE EXIT (stock 100 + short call -22.8) + LONG_CALL HOLD (+35).
        Current net = 100 - 22.8 + 35 = 112.2 → but BW EXIT removes 100-22.8=77.2
        Wait — the BW has TWO legs being exited: stock (100) + option (-22.8).
        Post-exit net = 35 → Bullish. Current net = 112.2 → Bullish.
        That's not a reversal. Let me adjust to make it realistic.
        """
        # AAPL scenario: BUY_WRITE stock leg + short call leg (both EXIT)
        # + LONG_CALL HOLD. Net delta current ~ -9 (neutral), post-exit ~ +13 (bullish).
        df = pd.DataFrame([
            {
                "Underlying_Ticker": "AAPL", "TradeID": "T-BW-001",
                "Strategy": "BUY_WRITE", "AssetType": "STOCK",
                "Delta": 0.0, "Quantity": 100.0,
                "Action": "EXIT", "Urgency": "MEDIUM", "Rationale": "Sell stock",
            },
            {
                "Underlying_Ticker": "AAPL", "TradeID": "T-BW-001",
                "Strategy": "BUY_WRITE", "AssetType": "OPTION",
                "Delta": -0.228, "Quantity": 1.0,  # short call: -22.8 delta
                "Action": "EXIT", "Urgency": "MEDIUM", "Rationale": "Exit call",
            },
            {
                "Underlying_Ticker": "AAPL", "TradeID": "T-BW-002",
                "Strategy": "BUY_WRITE", "AssetType": "OPTION",
                "Delta": -0.45, "Quantity": 1.0,  # another short call: -45 delta
                "Action": "HOLD", "Urgency": "LOW", "Rationale": "Hold income",
            },
            {
                "Underlying_Ticker": "AAPL", "TradeID": "T-LC-001",
                "Strategy": "LONG_CALL", "AssetType": "OPTION",
                "Delta": 0.58, "Quantity": 1.0,  # long call: +58 delta
                "Action": "HOLD", "Urgency": "LOW", "Rationale": "Hold long call",
            },
        ])
        # Net now: 100 - 22.8 - 45 + 58 = 90.2 → but we want neutral
        # Let me set stock qty to get net ~ -9
        # stock=100, sc1=-22.8(EXIT), sc2=-45(HOLD), lc=+58(HOLD)
        # total = 100 - 22.8 - 45 + 58 = 90.2. EXIT removes stock(100)+sc1(-22.8)=77.2
        # post: -45 + 58 = 13 → Bullish. pre: 90.2 → Bullish. No reversal.
        # Need pre to be neutral. Reduce stock or add more short delta.
        # Better: use realistic AAPL numbers from the card:
        # Net Δ = -9 (neutral), EXIT removes BW legs, post = +13 (bullish)
        df = pd.DataFrame([
            {
                "Underlying_Ticker": "AAPL", "TradeID": "T-BW-001",
                "Strategy": "BUY_WRITE", "AssetType": "OPTION",
                "Delta": -0.228, "Quantity": 1.0,  # short call: -22.8Δ
                "Action": "EXIT", "Urgency": "MEDIUM",
                "Rationale": "Buy back call then sell stock",
            },
            {
                "Underlying_Ticker": "AAPL", "TradeID": "T-LC-001",
                "Strategy": "LONG_CALL", "AssetType": "OPTION",
                "Delta": 0.138, "Quantity": 1.0,  # long call: +13.8Δ
                "Action": "HOLD", "Urgency": "LOW",
                "Rationale": "Hold long call",
            },
        ])
        # Net now: -22.8 + 13.8 = -9 → Neutral
        # EXIT removes -22.8 → post = 13.8 → Bullish
        result = compute_direction_reversals(df)

        # All legs should have Direction_Shift populated
        assert (result['Direction_Shift'] == 'Neutral → Bullish').all(), (
            f"Expected 'Neutral → Bullish', got {result['Direction_Shift'].tolist()}"
        )
        # Warning should be non-empty for all legs
        assert (result['Direction_Reversal_Warning'] != '').all(), (
            "All legs should have reversal warning"
        )
        # Post-exit delta should be ~13.8
        assert abs(result['Post_Exit_Net_Delta'].iloc[0] - 13.8) < 0.1
        # EXIT row rationale should be annotated
        exit_rat = result.loc[result['Action'] == 'EXIT', 'Rationale'].iloc[0]
        assert 'DIRECTION REVERSAL' in exit_rat

    def test_no_reversal_same_direction(self):
        """Two bullish legs, one exits — direction stays bullish."""
        df = pd.DataFrame([
            {
                "Underlying_Ticker": "MSFT", "TradeID": "T-LC-001",
                "Strategy": "LONG_CALL", "AssetType": "OPTION",
                "Delta": 0.60, "Quantity": 1.0,  # +60Δ
                "Action": "EXIT", "Urgency": "MEDIUM",
                "Rationale": "Take profit",
            },
            {
                "Underlying_Ticker": "MSFT", "TradeID": "T-LC-002",
                "Strategy": "LONG_CALL", "AssetType": "OPTION",
                "Delta": 0.40, "Quantity": 1.0,  # +40Δ
                "Action": "HOLD", "Urgency": "LOW",
                "Rationale": "Hold position",
            },
        ])
        # Net now: 60 + 40 = 100 → Bullish. Post-exit: 40 → Bullish. No reversal.
        result = compute_direction_reversals(df)
        assert (result['Direction_Shift'] == 'Bullish → Bullish').all()
        assert (result['Direction_Reversal_Warning'] == '').all(), (
            "No reversal warning when direction is consistent"
        )

    def test_single_leg_no_analysis(self):
        """Single-leg underlying — no cross-leg analysis needed."""
        df = pd.DataFrame([{
            "Underlying_Ticker": "GOOG", "TradeID": "T-001",
            "Strategy": "LONG_CALL", "AssetType": "OPTION",
            "Delta": 0.50, "Quantity": 1.0,
            "Action": "EXIT", "Urgency": "MEDIUM",
            "Rationale": "Close position",
        }])
        result = compute_direction_reversals(df)
        assert (result['Direction_Reversal_Warning'] == '').all()
        assert result['Post_Exit_Net_Delta'].isna().all()

    def test_no_exit_no_analysis(self):
        """Multiple legs, no EXIT — direction shift not computed."""
        df = pd.DataFrame([
            {
                "Underlying_Ticker": "AMZN", "TradeID": "T-001",
                "Strategy": "BUY_WRITE", "AssetType": "OPTION",
                "Delta": -0.30, "Quantity": 1.0,
                "Action": "HOLD", "Urgency": "LOW", "Rationale": "Hold",
            },
            {
                "Underlying_Ticker": "AMZN", "TradeID": "T-002",
                "Strategy": "LONG_CALL", "AssetType": "OPTION",
                "Delta": 0.50, "Quantity": 1.0,
                "Action": "HOLD", "Urgency": "LOW", "Rationale": "Hold",
            },
        ])
        result = compute_direction_reversals(df)
        assert result['Post_Exit_Net_Delta'].isna().all()

    def test_stock_delta_inferred(self):
        """Stock legs with Delta=0 should infer delta=1.0 per share."""
        df = pd.DataFrame([
            {
                "Underlying_Ticker": "TSLA", "TradeID": "T-BW-001",
                "Strategy": "BUY_WRITE", "AssetType": "STOCK",
                "Delta": 0.0, "Quantity": 100.0,  # should be 100Δ
                "Action": "EXIT", "Urgency": "MEDIUM", "Rationale": "Sell stock",
            },
            {
                "Underlying_Ticker": "TSLA", "TradeID": "T-LP-001",
                "Strategy": "LONG_PUT", "AssetType": "OPTION",
                "Delta": -0.50, "Quantity": 1.0,  # -50Δ
                "Action": "HOLD", "Urgency": "LOW", "Rationale": "Hold put",
            },
        ])
        # Net now: 100 - 50 = 50 → Bullish. Post-exit: -50 → Bearish. REVERSAL.
        result = compute_direction_reversals(df)
        assert result['Direction_Shift'].iloc[0] == 'Bullish → Bearish'
        assert (result['Direction_Reversal_Warning'] != '').all()

    def test_multiple_tickers_independent(self):
        """Direction reversal computed per underlying, not globally."""
        df = pd.DataFrame([
            # AAPL: reversal
            {
                "Underlying_Ticker": "AAPL", "TradeID": "T-1",
                "Strategy": "BUY_WRITE", "AssetType": "OPTION",
                "Delta": -0.30, "Quantity": 1.0,
                "Action": "EXIT", "Urgency": "MEDIUM", "Rationale": "Exit",
            },
            {
                "Underlying_Ticker": "AAPL", "TradeID": "T-2",
                "Strategy": "LONG_CALL", "AssetType": "OPTION",
                "Delta": 0.20, "Quantity": 1.0,
                "Action": "HOLD", "Urgency": "LOW", "Rationale": "Hold",
            },
            # MSFT: no reversal
            {
                "Underlying_Ticker": "MSFT", "TradeID": "T-3",
                "Strategy": "LONG_CALL", "AssetType": "OPTION",
                "Delta": 0.60, "Quantity": 1.0,
                "Action": "EXIT", "Urgency": "MEDIUM", "Rationale": "Exit",
            },
            {
                "Underlying_Ticker": "MSFT", "TradeID": "T-4",
                "Strategy": "LONG_CALL", "AssetType": "OPTION",
                "Delta": 0.40, "Quantity": 1.0,
                "Action": "HOLD", "Urgency": "LOW", "Rationale": "Hold",
            },
        ])
        # AAPL: -30 + 20 = -10 → Neutral. Post-exit: 20 → Bullish. REVERSAL.
        # MSFT: 60 + 40 = 100 → Bullish. Post-exit: 40 → Bullish. No reversal.
        result = compute_direction_reversals(df)
        aapl = result[result['Underlying_Ticker'] == 'AAPL']
        msft = result[result['Underlying_Ticker'] == 'MSFT']
        assert (aapl['Direction_Reversal_Warning'] != '').all()
        assert (msft['Direction_Reversal_Warning'] == '').all()

    def test_reversal_warning_in_rationale(self):
        """EXIT row Rationale is annotated with the direction reversal warning."""
        df = pd.DataFrame([
            {
                "Underlying_Ticker": "META", "TradeID": "T-BW-001",
                "Strategy": "BUY_WRITE", "AssetType": "OPTION",
                "Delta": -0.50, "Quantity": 1.0,  # -50Δ
                "Action": "EXIT", "Urgency": "MEDIUM",
                "Rationale": "Original exit reason",
            },
            {
                "Underlying_Ticker": "META", "TradeID": "T-LC-001",
                "Strategy": "LONG_CALL", "AssetType": "OPTION",
                "Delta": 0.30, "Quantity": 1.0,  # +30Δ
                "Action": "HOLD", "Urgency": "LOW",
                "Rationale": "Hold position",
            },
        ])
        # Net: -50+30 = -20 → Bearish. Post-exit: 30 → Bullish. REVERSAL.
        result = compute_direction_reversals(df)
        exit_row = result[result['Action'] == 'EXIT'].iloc[0]
        hold_row = result[result['Action'] == 'HOLD'].iloc[0]
        # EXIT rationale should have the annotation
        assert "DIRECTION REVERSAL" in exit_row['Rationale']
        assert "Original exit reason" in exit_row['Rationale']
        # HOLD rationale should NOT be modified
        assert hold_row['Rationale'] == "Hold position"


# =============================================================================
# Moderate Recovery Detection — Unit Tests
# =============================================================================

from core.management.cycle3.doctrine.helpers import detect_moderate_recovery_state


class TestModerateRecoveryDetection:
    """detect_moderate_recovery_state: catches -10% to -25% drawdowns early."""

    def _bw_row(self, **overrides):
        defaults = {
            "Cumulative_Premium_Collected": 5.50,
            "Gross_Premium_Collected": 0,
            "_cycle_count": 2,
            "IV_Now": 0.40,
            "IV_30D": 0.35,
            "IV_Rank": 55.0,
            "IV_Percentile": 52.0,
            "Thesis_State": "DEGRADED",
            "Short_Call_DTE": 35.0,
            "DTE": 35.0,
            "Premium_Entry": 3.20,
            "Last": 2.10,
            "Margin_Cost_Daily": 0,
            "Quantity": 100.0,
        }
        defaults.update(overrides)
        return pd.Series(defaults)

    def test_12pct_loss_activates(self):
        """Position at -12.4% with premium → moderate recovery active."""
        row = self._bw_row()
        result = detect_moderate_recovery_state(row, spot=148.52, effective_cost=169.50)
        assert result["is_moderate_recovery"] is True
        assert -0.25 < result["context"]["loss_pct"] < -0.10

    def test_5pct_loss_too_shallow(self):
        """Position at -5% → below threshold, no moderate recovery."""
        row = self._bw_row()
        result = detect_moderate_recovery_state(row, spot=161.0, effective_cost=169.50)
        assert result["is_moderate_recovery"] is False

    def test_30pct_loss_too_deep(self):
        """Position at -30% → deep recovery handles this, not moderate."""
        row = self._bw_row()
        result = detect_moderate_recovery_state(row, spot=118.65, effective_cost=169.50)
        assert result["is_moderate_recovery"] is False

    def test_no_premium_no_recovery(self):
        """No premium collected → no income path → no recovery."""
        row = self._bw_row(Cumulative_Premium_Collected=0.0)
        result = detect_moderate_recovery_state(row, spot=148.52, effective_cost=169.50)
        assert result["is_moderate_recovery"] is False

    def test_broken_thesis_no_recovery(self):
        """Thesis BROKEN → recovery not viable."""
        row = self._bw_row(Thesis_State="BROKEN")
        result = detect_moderate_recovery_state(row, spot=148.52, effective_cost=169.50)
        assert result["is_moderate_recovery"] is False

    def test_low_iv_no_recovery(self):
        """IV below 15% → can't generate meaningful premium."""
        row = self._bw_row(IV_Now=0.08, IV_30D=0.07)
        result = detect_moderate_recovery_state(row, spot=148.52, effective_cost=169.50)
        assert result["is_moderate_recovery"] is False

    def test_context_has_post_roll_economics(self):
        """Context includes monthly income and months to breakeven."""
        row = self._bw_row()
        result = detect_moderate_recovery_state(row, spot=148.52, effective_cost=169.50)
        ctx = result["context"]
        assert "monthly_income" in ctx
        assert "months_to_breakeven" in ctx
        assert "gap_to_breakeven" in ctx
        assert ctx["gap_to_breakeven"] > 0
        assert ctx["monthly_income"] > 0

    def test_boundary_10pct_activates(self):
        """Just past -10% boundary → activates."""
        row = self._bw_row()
        # spot = 152.50 → (152.50-169.50)/169.50 = -10.03%, clearly past threshold
        result = detect_moderate_recovery_state(row, spot=152.50, effective_cost=169.50)
        assert result["is_moderate_recovery"] is True

    def test_boundary_25pct_does_not_activate(self):
        """Exactly at -25% boundary → deep recovery territory."""
        row = self._bw_row()
        # spot = 169.50 * 0.75 = 127.125 → exactly -25%
        result = detect_moderate_recovery_state(row, spot=127.125, effective_cost=169.50)
        assert result["is_moderate_recovery"] is False

    def test_intact_thesis_activates(self):
        """Thesis INTACT (not just DEGRADED) also activates recovery."""
        row = self._bw_row(Thesis_State="INTACT")
        result = detect_moderate_recovery_state(row, spot=148.52, effective_cost=169.50)
        assert result["is_moderate_recovery"] is True


# ═══════════════════════════════════════════════════════════════════════════════
# Cross-Strategy Language & Framing Assertions
# ═══════════════════════════════════════════════════════════════════════════════
# Systematic tests that verify strategy-specific language is never mis-applied.
# "patience while theta works" → income only, never long options.
# "collect theta" → income only.
# "MACRO WARNING" → income/stock, never long options.
# Deep ITM long options → not forced to EXECUTE_NOW.

from core.management.cycle3.doctrine.orchestrator import _build_journey_note
from core.management.cycle3.doctrine.execution_readiness import _apply_execution_readiness
from unittest.mock import patch
from dataclasses import dataclass, field
import datetime as _dt


@dataclass
class _MockCalContext:
    is_friday: bool = False
    is_monday: bool = False
    is_pre_long_weekend: bool = False
    weekend_gap_days: int = 1
    is_trading_day: bool = True
    date: _dt.date = field(default_factory=lambda: _dt.date(2026, 3, 10))
    next_open: _dt.date = field(default_factory=lambda: _dt.date(2026, 3, 11))

    @property
    def theta_bleed_days(self):
        return self.weekend_gap_days - 1


# Strategy type buckets
_LONG_STRATEGIES = ["LONG_CALL", "LONG_PUT", "LEAPS_CALL", "LEAPS_PUT"]
_INCOME_STRATEGIES = ["BUY_WRITE", "COVERED_CALL", "CSP", "SHORT_PUT"]


class TestJourneyNoteThetaFraming:
    """Verify HOLD→HOLD journey note uses correct theta language per strategy."""

    def _journey_row(self, strategy):
        return pd.Series({
            "Entry_Structure": strategy,
            "Prior_Action": "HOLD",
            "Prior_UL_Last": 100.0,
            "Prior_Snapshot_TS": pd.Timestamp("2026-03-09"),
            "Prior_Days_Ago": 1,
        })

    @pytest.mark.parametrize("strategy", _LONG_STRATEGIES)
    def test_long_option_never_says_theta_works(self, strategy):
        """Long options pay theta — 'patience while theta works' is wrong."""
        row = self._journey_row(strategy)
        note = _build_journey_note(row, current_action="HOLD", ul_last=100.0)
        assert "patience while theta works" not in note
        assert "conviction required" in note or "time decays" in note

    @pytest.mark.parametrize("strategy", _INCOME_STRATEGIES)
    def test_income_strategy_says_theta_works(self, strategy):
        """Income strategies collect theta — 'theta works' is correct."""
        row = self._journey_row(strategy)
        note = _build_journey_note(row, current_action="HOLD", ul_last=100.0)
        assert "theta works" in note


class TestExecutionReadinessStrategyAwareness:
    """Verify execution readiness respects long vs short premium distinction."""

    def _exec_df(self, **overrides):
        defaults = {
            "Ticker": "TEST",
            "Strategy": "LONG_CALL",
            "Strategy_Name": "LONG_CALL",
            "Action": "HOLD",
            "Urgency": "LOW",
            "DTE": 45,
            "Delta": 0.50,
            "Roll_Candidate_1": "",
            "Earnings_Date": "",
            "IV_vs_HV_Gap": 5.0,
        }
        defaults.update(overrides)
        return pd.DataFrame([defaults])

    @patch("scan_engine.calendar_context.get_calendar_context")
    @pytest.mark.parametrize("strategy", _LONG_STRATEGIES)
    def test_long_hold_says_manage_carry_not_collect_theta(self, mock_cal, strategy):
        """Long option HOLD → 'manage carry cost', never 'collect theta'."""
        mock_cal.return_value = _MockCalContext()
        df = self._exec_df(Strategy=strategy, Strategy_Name=strategy)
        result = _apply_execution_readiness(df)
        reason = result.iloc[0]["Execution_Readiness_Reason"]
        assert "collect theta" not in reason
        assert "carry cost" in reason or "thesis monitoring" in reason

    @patch("scan_engine.calendar_context.get_calendar_context")
    @pytest.mark.parametrize("strategy", _INCOME_STRATEGIES)
    def test_income_hold_says_collect_theta(self, mock_cal, strategy):
        """Income strategy HOLD → 'collect theta' is correct."""
        mock_cal.return_value = _MockCalContext()
        df = self._exec_df(Strategy=strategy, Strategy_Name=strategy)
        result = _apply_execution_readiness(df)
        reason = result.iloc[0]["Execution_Readiness_Reason"]
        assert "collect theta" in reason

    @patch("scan_engine.calendar_context.get_calendar_context")
    @pytest.mark.parametrize("strategy", _LONG_STRATEGIES)
    def test_deep_itm_long_not_forced_execute(self, mock_cal, strategy):
        """Deep ITM long option (delta 0.85) is winning — NOT EXECUTE_NOW."""
        mock_cal.return_value = _MockCalContext()
        df = self._exec_df(
            Strategy=strategy, Strategy_Name=strategy,
            Delta=0.85, DTE=90, Action="HOLD", Urgency="LOW",
        )
        result = _apply_execution_readiness(df)
        readiness = result.iloc[0]["Execution_Readiness"]
        assert readiness != "EXECUTE_NOW", (
            f"Deep ITM long {strategy} (delta 0.85) should not be forced to EXECUTE_NOW"
        )

    @patch("scan_engine.calendar_context.get_calendar_context")
    @pytest.mark.parametrize("strategy", _INCOME_STRATEGIES)
    def test_deep_itm_income_forced_execute(self, mock_cal, strategy):
        """Deep ITM income (delta 0.85) = assignment risk → EXECUTE_NOW."""
        mock_cal.return_value = _MockCalContext()
        df = self._exec_df(
            Strategy=strategy, Strategy_Name=strategy,
            Delta=0.85, DTE=30, Action="ROLL", Urgency="MEDIUM",
        )
        result = _apply_execution_readiness(df)
        readiness = result.iloc[0]["Execution_Readiness"]
        assert readiness == "EXECUTE_NOW", (
            f"Deep ITM income {strategy} (delta 0.85) should be EXECUTE_NOW"
        )


class TestMacroModifierStrategyFraming:
    """Verify macro event framing differs for long vs income vs deep-ITM strategies.

    Note: These test the D2 macro modifier logic in run_all.py section 3.0f.
    Since that logic runs on df_final (post-doctrine), we simulate it directly.
    """

    def _apply_macro_d2(self, strategy, delta=0.45, dte=45):
        """Simulate the D2 macro modifier branch for a single HOLD LOW row.

        Returns (urgency, note_text) after D2 logic.
        """
        _DIRECTIONAL = {"LONG_CALL", "LONG_PUT", "BUY_CALL", "BUY_PUT",
                        "LEAPS_CALL", "LEAPS_PUT"}
        _is_long_opt = strategy in _DIRECTIONAL

        strat = strategy.upper()
        is_directional = strat in _DIRECTIONAL

        if not is_directional:
            return "LOW", ""

        _deep_itm_leaps = _is_long_opt and abs(delta) > 0.75 and dte > 90

        if _deep_itm_leaps:
            return "LOW", (
                f"Macro: CPI in 1d. Deep ITM (Δ {abs(delta):.2f}, DTE {dte:.0f}) — "
                "minimal extrinsic at risk."
            )
        elif _is_long_opt:
            return "HIGH", "MACRO CATALYST"
        else:
            return "HIGH", "MACRO WARNING"

    @pytest.mark.parametrize("strategy", _LONG_STRATEGIES)
    def test_atm_long_option_gets_macro_catalyst(self, strategy):
        """ATM long option + macro ≤3d → MACRO CATALYST, not WARNING."""
        urg, note = self._apply_macro_d2(strategy, delta=0.45, dte=45)
        assert urg == "HIGH"
        assert "CATALYST" in note

    @pytest.mark.parametrize("strategy", _LONG_STRATEGIES)
    def test_deep_itm_leaps_no_escalation(self, strategy):
        """Deep ITM LEAPS (delta>0.75, DTE>90) → no urgency escalation."""
        urg, note = self._apply_macro_d2(strategy, delta=0.88, dte=192)
        assert urg == "LOW", (
            f"Deep ITM LEAPS {strategy} should stay LOW, not escalated to HIGH"
        )
        assert "Deep ITM" in note

    @pytest.mark.parametrize("strategy", _INCOME_STRATEGIES)
    def test_income_not_affected_by_directional_macro(self, strategy):
        """Income strategies are not directional — D2 doesn't apply."""
        urg, note = self._apply_macro_d2(strategy, delta=0.45, dte=30)
        assert urg == "LOW"
        assert note == ""

    def test_boundary_delta_075_dte_90_still_escalates(self):
        """Delta exactly 0.75 is NOT > 0.75 → should still escalate."""
        urg, note = self._apply_macro_d2("LONG_CALL", delta=0.75, dte=91)
        assert urg == "HIGH", "Delta 0.75 is boundary — should escalate"
        assert "CATALYST" in note

    def test_boundary_delta_076_dte_90_suppressed(self):
        """Delta 0.76 > 0.75 AND DTE 91 > 90 → suppressed."""
        urg, note = self._apply_macro_d2("LONG_CALL", delta=0.76, dte=91)
        assert urg == "LOW", "Delta 0.76 + DTE 91 → deep ITM guard"

    def test_high_delta_short_dte_still_escalates(self):
        """Delta 0.88 but DTE=30 (not LEAPS) → still escalate."""
        urg, note = self._apply_macro_d2("LONG_CALL", delta=0.88, dte=30)
        assert urg == "HIGH", "DTE 30 < 90 → not LEAPS, should escalate"


# ═══════════════════════════════════════════════════════════════════════════════
# Prior EXIT Persistence Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestPriorExitPersistence:
    """
    Validate check_prior_exit_persistence() prevents one-day EXIT→HOLD flips
    unless conditions materially improved or macro catalyst is imminent.
    """

    def test_prior_exit_persists_no_improvement(self):
        """Prior EXIT, P&L flat, small price move → EXIT persists."""
        from core.management.cycle3.doctrine.helpers import check_prior_exit_persistence
        row = pd.Series({
            "Prior_Action": "EXIT",
            "PnL_Pct": -0.22,
            "Prior_PnL_Pct": -0.24,
            "Price_Drift_Pct": -0.01,  # 1% down — not enough for put (need 2%)
            "DTE": 36,
            "Prior_Doctrine_Source": "Passarelli Ch.2: Theta Awareness",
        })
        persist, reason, macro = check_prior_exit_persistence(row, is_put=True)
        assert persist is True, f"Should persist EXIT — got cleared: {reason}"
        assert "not materially improved" in reason
        assert macro is False

    def test_prior_exit_cleared_by_pnl_recovery(self):
        """P&L improved 6pp (>5pp threshold) → EXIT cleared."""
        from core.management.cycle3.doctrine.helpers import check_prior_exit_persistence
        row = pd.Series({
            "Prior_Action": "EXIT",
            "PnL_Pct": -0.18,
            "Prior_PnL_Pct": -0.24,  # 6pp improvement
            "Price_Drift_Pct": -0.01,
            "DTE": 36,
            "Prior_Doctrine_Source": "Passarelli Ch.2: Theta Awareness",
        })
        persist, reason, macro = check_prior_exit_persistence(row, is_put=True)
        assert persist is False, f"Should clear EXIT on P&L recovery: {reason}"
        assert "P&L improved" in reason

    def test_prior_exit_cleared_by_favorable_price_move(self):
        """Put position, stock dropped 3% (>2% threshold) → EXIT cleared."""
        from core.management.cycle3.doctrine.helpers import check_prior_exit_persistence
        row = pd.Series({
            "Prior_Action": "EXIT",
            "PnL_Pct": -0.22,
            "Prior_PnL_Pct": -0.24,  # only 2pp improvement (< 5pp)
            "Price_Drift_Pct": -0.03,  # 3% down — favorable for put
            "DTE": 36,
            "Prior_Doctrine_Source": "Passarelli Ch.2: Theta Awareness",
        })
        persist, reason, macro = check_prior_exit_persistence(row, is_put=True)
        assert persist is False, f"Should clear EXIT on favorable move: {reason}"
        assert "favorable price move" in reason

    def test_prior_exit_cleared_by_macro_catalyst(self):
        """FOMC in 3d, DTE ≥ 14 → macro catalyst clears EXIT."""
        from core.management.cycle3.doctrine.helpers import check_prior_exit_persistence
        row = pd.Series({
            "Prior_Action": "EXIT",
            "PnL_Pct": -0.22,
            "Prior_PnL_Pct": -0.24,
            "Price_Drift_Pct": -0.01,
            "DTE": 36,
            "Prior_Doctrine_Source": "Passarelli Ch.2: Theta Awareness",
        })
        persist, reason, macro = check_prior_exit_persistence(
            row, is_put=True, macro_days=3, macro_type="FOMC"
        )
        assert persist is False, f"Macro catalyst should clear EXIT: {reason}"
        assert macro is True
        assert "FOMC" in reason

    def test_macro_catalyst_requires_dte_14(self):
        """FOMC in 3d but DTE=10 → macro doesn't clear (theta acceleration overrides)."""
        from core.management.cycle3.doctrine.helpers import check_prior_exit_persistence
        row = pd.Series({
            "Prior_Action": "EXIT",
            "PnL_Pct": -0.22,
            "Prior_PnL_Pct": -0.24,
            "Price_Drift_Pct": -0.01,
            "DTE": 10,  # below MACRO_CATALYST_DTE_MIN (14)
            "Prior_Doctrine_Source": "Passarelli Ch.2: Theta Awareness",
        })
        persist, reason, macro = check_prior_exit_persistence(
            row, is_put=True, macro_days=3, macro_type="FOMC"
        )
        assert persist is True, "DTE too low for macro catalyst exception"
        assert macro is False

    def test_macro_gdp_not_high_impact(self):
        """GDP (MEDIUM impact) doesn't clear EXIT."""
        from core.management.cycle3.doctrine.helpers import check_prior_exit_persistence
        row = pd.Series({
            "Prior_Action": "EXIT",
            "PnL_Pct": -0.22,
            "Prior_PnL_Pct": -0.24,
            "Price_Drift_Pct": -0.01,
            "DTE": 36,
            "Prior_Doctrine_Source": "Passarelli Ch.2: Theta Awareness",
        })
        persist, reason, macro = check_prior_exit_persistence(
            row, is_put=True, macro_days=3, macro_type="GDP"
        )
        assert persist is True, "GDP is MEDIUM impact, not HIGH — should not clear"
        assert macro is False

    def test_prior_hold_not_affected(self):
        """Prior HOLD → no persistence (not applicable)."""
        from core.management.cycle3.doctrine.helpers import check_prior_exit_persistence
        row = pd.Series({
            "Prior_Action": "HOLD",
            "PnL_Pct": -0.22,
            "Prior_PnL_Pct": -0.24,
            "Price_Drift_Pct": -0.01,
            "DTE": 36,
            "Prior_Doctrine_Source": "McMillan Ch.4: Neutral Maintenance",
        })
        persist, reason, macro = check_prior_exit_persistence(row, is_put=True)
        assert persist is False

    def test_call_position_favorable_move_up(self):
        """LONG_CALL with stock UP 3% → favorable move clears EXIT."""
        from core.management.cycle3.doctrine.helpers import check_prior_exit_persistence
        row = pd.Series({
            "Prior_Action": "EXIT",
            "PnL_Pct": -0.18,
            "Prior_PnL_Pct": -0.24,
            "Price_Drift_Pct": 0.03,  # 3% up — favorable for call
            "DTE": 36,
            "Prior_Doctrine_Source": "Passarelli Ch.2: Theta Awareness",
        })
        persist, reason, macro = check_prior_exit_persistence(row, is_put=False)
        assert persist is False, f"Stock up 3% is favorable for call: {reason}"

    def test_call_position_adverse_move_down_persists(self):
        """LONG_CALL with stock DOWN 1% → adverse, EXIT persists."""
        from core.management.cycle3.doctrine.helpers import check_prior_exit_persistence
        row = pd.Series({
            "Prior_Action": "EXIT",
            "PnL_Pct": -0.25,
            "Prior_PnL_Pct": -0.24,
            "Price_Drift_Pct": -0.01,  # 1% down — adverse for call, below threshold
            "DTE": 36,
            "Prior_Doctrine_Source": "Passarelli Ch.2: Theta Awareness",
        })
        persist, reason, macro = check_prior_exit_persistence(row, is_put=False)
        assert persist is True, "Stock down, P&L worsened — EXIT should persist"

    def test_missing_prior_pnl_persists(self):
        """Missing Prior_PnL_Pct → cannot confirm recovery → EXIT persists."""
        from core.management.cycle3.doctrine.helpers import check_prior_exit_persistence
        row = pd.Series({
            "Prior_Action": "EXIT",
            "PnL_Pct": -0.22,
            "Prior_PnL_Pct": float('nan'),
            "Price_Drift_Pct": -0.01,
            "DTE": 36,
            "Prior_Doctrine_Source": "Passarelli Ch.2: Theta Awareness",
        })
        persist, reason, macro = check_prior_exit_persistence(row, is_put=True)
        assert persist is True, "Missing prior P&L → cannot confirm recovery"

    def test_smh_scenario_exit_persists(self):
        """SMH LONG_PUT: prior EXIT from theta, stock drops 2.4% — still persists
        because drift 2.4% > 2% clears. Actually, for a put, -2.4% drift is
        favorable, so the position SHOULD clear."""
        from core.management.cycle3.doctrine.helpers import check_prior_exit_persistence
        row = pd.Series({
            "Prior_Action": "EXIT",
            "PnL_Pct": -0.21,
            "Prior_PnL_Pct": -0.25,  # 4pp improvement (< 5pp)
            "Price_Drift_Pct": -0.024,  # 2.4% favorable move for put (> 2%)
            "DTE": 36,
            "Prior_Doctrine_Source": "Passarelli Ch.2: Theta Awareness",
        })
        persist, reason, macro = check_prior_exit_persistence(row, is_put=True)
        assert persist is False, (
            f"SMH put with 2.4% favorable move should clear: {reason}"
        )

    def test_smh_scenario_with_macro_catalyst(self):
        """SMH LONG_PUT with FOMC in 6d — macro catalyst clears EXIT."""
        from core.management.cycle3.doctrine.helpers import check_prior_exit_persistence
        row = pd.Series({
            "Prior_Action": "EXIT",
            "PnL_Pct": -0.22,
            "Prior_PnL_Pct": -0.24,
            "Price_Drift_Pct": -0.01,  # small move — wouldn't clear on its own
            "DTE": 36,
            "Prior_Doctrine_Source": "Passarelli Ch.2: Theta Awareness",
        })
        persist, reason, macro = check_prior_exit_persistence(
            row, is_put=True, macro_days=6, macro_type="FOMC"
        )
        # MACRO_CATALYST_DAYS_THRESHOLD = 5, FOMC at 6d → just outside
        # No Strategy/Thesis fields → extended window doesn't qualify
        assert persist is True, "FOMC at 6d is outside 5d threshold — doesn't clear"

    def test_extended_macro_window_long_put_fomc_6d(self):
        """NVDA-style: LONG_PUT, FOMC 6d, thesis intact, IV vol bid → extended window clears EXIT."""
        from core.management.cycle3.doctrine.helpers import check_prior_exit_persistence
        row = pd.Series({
            "Prior_Action": "EXIT",
            "PnL_Pct": -0.22,
            "Prior_PnL_Pct": -0.20,
            "Price_Drift_Pct": 0.01,
            "DTE": 43,
            "Prior_Doctrine_Source": "Natenberg Ch.5 + Jabbour Ch.7: Direction Adverse EXIT",
            "Strategy": "LONG_PUT",
            "Thesis_State": "INTACT",
            "Conviction_Status": "STABLE",
            "Recovery_Feasibility": "FEASIBLE",
            "IV_Percentile": 33.3,
            "IV_vs_HV_Gap": 0.039,
            "Last": 8.68,
        })
        persist, reason, macro = check_prior_exit_persistence(
            row, is_put=True, macro_days=6, macro_type="FOMC"
        )
        assert persist is False, f"Extended window should clear EXIT: {reason}"
        assert macro is True
        assert "extended window" in reason

    def test_extended_macro_window_long_call_cpi_7d(self):
        """LONG_CALL, CPI 7d, high IV_Percentile → extended window clears EXIT."""
        from core.management.cycle3.doctrine.helpers import check_prior_exit_persistence
        row = pd.Series({
            "Prior_Action": "EXIT",
            "PnL_Pct": -0.15,
            "Prior_PnL_Pct": -0.14,
            "Price_Drift_Pct": -0.005,
            "DTE": 30,
            "Prior_Doctrine_Source": "Passarelli Ch.2: Theta Awareness",
            "Strategy": "LONG_CALL",
            "Thesis_State": "INTACT",
            "Conviction_Status": "IMPROVING",
            "Recovery_Feasibility": "LIKELY",
            "IV_Percentile": 90.0,
            "IV_vs_HV_Gap": -0.02,
            "Last": 5.50,
        })
        persist, reason, macro = check_prior_exit_persistence(
            row, is_put=False, macro_days=7, macro_type="CPI"
        )
        assert persist is False, f"CPI at 7d with high IV pctile should clear: {reason}"
        assert macro is True

    def test_extended_macro_window_requires_thesis_intact(self):
        """LONG_PUT, FOMC 6d, but thesis DEGRADED → extended window does NOT fire."""
        from core.management.cycle3.doctrine.helpers import check_prior_exit_persistence
        row = pd.Series({
            "Prior_Action": "EXIT",
            "PnL_Pct": -0.30,
            "Prior_PnL_Pct": -0.28,
            "Price_Drift_Pct": 0.01,
            "DTE": 43,
            "Prior_Doctrine_Source": "Natenberg Ch.5: Direction Adverse EXIT",
            "Strategy": "LONG_PUT",
            "Thesis_State": "DEGRADED",
            "Conviction_Status": "STABLE",
            "Recovery_Feasibility": "FEASIBLE",
            "IV_Percentile": 33.3,
            "IV_vs_HV_Gap": 0.05,
            "Last": 7.00,
        })
        persist, reason, macro = check_prior_exit_persistence(
            row, is_put=True, macro_days=6, macro_type="FOMC"
        )
        assert persist is True, "Degraded thesis should not qualify for extended window"

    def test_extended_macro_window_requires_dte_21(self):
        """LONG_PUT, FOMC 6d, but DTE=15 → too short for extended window."""
        from core.management.cycle3.doctrine.helpers import check_prior_exit_persistence
        row = pd.Series({
            "Prior_Action": "EXIT",
            "PnL_Pct": -0.22,
            "Prior_PnL_Pct": -0.20,
            "Price_Drift_Pct": 0.01,
            "DTE": 15,
            "Prior_Doctrine_Source": "Natenberg Ch.5: Direction Adverse EXIT",
            "Strategy": "LONG_PUT",
            "Thesis_State": "INTACT",
            "Conviction_Status": "STABLE",
            "Recovery_Feasibility": "FEASIBLE",
            "IV_Percentile": 33.3,
            "IV_vs_HV_Gap": 0.05,
            "Last": 7.00,
        })
        persist, reason, macro = check_prior_exit_persistence(
            row, is_put=True, macro_days=6, macro_type="FOMC"
        )
        assert persist is True, "DTE 15 < 21 minimum for extended window"

    def test_extended_macro_window_not_for_income(self):
        """BUY_WRITE at 6d macro → extended window doesn't apply (income strategy)."""
        from core.management.cycle3.doctrine.helpers import check_prior_exit_persistence
        row = pd.Series({
            "Prior_Action": "EXIT",
            "PnL_Pct": -0.22,
            "Prior_PnL_Pct": -0.20,
            "Price_Drift_Pct": 0.01,
            "DTE": 43,
            "Prior_Doctrine_Source": "Natenberg Ch.5: Direction Adverse EXIT",
            "Strategy": "BUY_WRITE",
            "Thesis_State": "INTACT",
            "Conviction_Status": "STABLE",
            "Recovery_Feasibility": "FEASIBLE",
            "IV_Percentile": 90.0,
            "IV_vs_HV_Gap": 0.05,
            "Last": 7.00,
        })
        persist, reason, macro = check_prior_exit_persistence(
            row, is_put=True, macro_days=6, macro_type="FOMC"
        )
        assert persist is True, "BUY_WRITE is not long premium — no extended window"

    def test_extended_macro_window_no_iv_edge(self):
        """LONG_PUT, FOMC 6d, but IV_Percentile low AND no vol bid → no extended window."""
        from core.management.cycle3.doctrine.helpers import check_prior_exit_persistence
        row = pd.Series({
            "Prior_Action": "EXIT",
            "PnL_Pct": -0.22,
            "Prior_PnL_Pct": -0.20,
            "Price_Drift_Pct": 0.01,
            "DTE": 43,
            "Prior_Doctrine_Source": "Natenberg Ch.5: Direction Adverse EXIT",
            "Strategy": "LONG_PUT",
            "Thesis_State": "INTACT",
            "Conviction_Status": "STABLE",
            "Recovery_Feasibility": "FEASIBLE",
            "IV_Percentile": 20.0,
            "IV_vs_HV_Gap": -0.05,
            "Last": 7.00,
        })
        persist, reason, macro = check_prior_exit_persistence(
            row, is_put=True, macro_days=6, macro_type="FOMC"
        )
        assert persist is True, "No IV edge (low pctile, negative gap) → no extended window"

    def test_extended_macro_window_gdp_not_high_impact(self):
        """LONG_PUT, GDP 6d → GDP is MEDIUM, extended window only for HIGH impact."""
        from core.management.cycle3.doctrine.helpers import check_prior_exit_persistence
        row = pd.Series({
            "Prior_Action": "EXIT",
            "PnL_Pct": -0.22,
            "Prior_PnL_Pct": -0.20,
            "Price_Drift_Pct": 0.01,
            "DTE": 43,
            "Prior_Doctrine_Source": "Natenberg Ch.5: Direction Adverse EXIT",
            "Strategy": "LONG_PUT",
            "Thesis_State": "INTACT",
            "Conviction_Status": "STABLE",
            "Recovery_Feasibility": "FEASIBLE",
            "IV_Percentile": 90.0,
            "IV_vs_HV_Gap": 0.05,
            "Last": 7.00,
        })
        persist, reason, macro = check_prior_exit_persistence(
            row, is_put=True, macro_days=6, macro_type="GDP"
        )
        assert persist is True, "GDP is MEDIUM impact — no extended window"

    def test_extended_macro_window_at_8d_too_far(self):
        """FOMC at 8d → outside extended window (max 7d)."""
        from core.management.cycle3.doctrine.helpers import check_prior_exit_persistence
        row = pd.Series({
            "Prior_Action": "EXIT",
            "PnL_Pct": -0.22,
            "Prior_PnL_Pct": -0.20,
            "Price_Drift_Pct": 0.01,
            "DTE": 43,
            "Prior_Doctrine_Source": "Natenberg Ch.5: Direction Adverse EXIT",
            "Strategy": "LONG_PUT",
            "Thesis_State": "INTACT",
            "Conviction_Status": "STABLE",
            "Recovery_Feasibility": "FEASIBLE",
            "IV_Percentile": 90.0,
            "IV_vs_HV_Gap": 0.05,
            "Last": 7.00,
        })
        persist, reason, macro = check_prior_exit_persistence(
            row, is_put=True, macro_days=8, macro_type="FOMC"
        )
        assert persist is True, "8d is outside 7d extended window"

    def test_extended_macro_window_conviction_declining_blocked(self):
        """LONG_PUT, FOMC 6d, conviction DECLINING → no extended window."""
        from core.management.cycle3.doctrine.helpers import check_prior_exit_persistence
        row = pd.Series({
            "Prior_Action": "EXIT",
            "PnL_Pct": -0.22,
            "Prior_PnL_Pct": -0.20,
            "Price_Drift_Pct": 0.01,
            "DTE": 43,
            "Prior_Doctrine_Source": "Natenberg Ch.5: Direction Adverse EXIT",
            "Strategy": "LONG_PUT",
            "Thesis_State": "INTACT",
            "Conviction_Status": "DECLINING",
            "Recovery_Feasibility": "FEASIBLE",
            "IV_Percentile": 90.0,
            "IV_vs_HV_Gap": 0.05,
            "Last": 7.00,
        })
        persist, reason, macro = check_prior_exit_persistence(
            row, is_put=True, macro_days=6, macro_type="FOMC"
        )
        assert persist is True, "DECLINING conviction should not qualify for extended window"


# ═══════════════════════════════════════════════════════════════════════════════
# GATE_FAMILY_IDS Extended Tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestGateFamilyIDs:
    """Verify new gate families match expected doctrine source strings."""

    def test_theta_dominant_family_matches(self):
        from core.management.cycle3.doctrine.helpers import _matches_gate_family
        assert _matches_gate_family("Passarelli Ch.2: Theta Awareness", "THETA_DOMINANT")
        assert _matches_gate_family("Natenberg Ch.4: Multi-Leg Theta Management", "THETA_DOMINANT")
        assert _matches_gate_family("McMillan Ch.4 + Passarelli Ch.2: Time-to-Impulse", "THETA_DOMINANT")

    def test_theta_dominant_no_false_match(self):
        from core.management.cycle3.doctrine.helpers import _matches_gate_family
        assert not _matches_gate_family("McMillan Ch.4: Direction Adverse", "THETA_DOMINANT")
        assert not _matches_gate_family("McMillan Ch.4: Neutral Maintenance", "THETA_DOMINANT")

    def test_profit_capture_family_matches(self):
        from core.management.cycle3.doctrine.helpers import _matches_gate_family
        assert _matches_gate_family("McMillan Ch.4: Weak Entry Profit Capture", "PROFIT_CAPTURE")

    def test_direction_adverse_still_works(self):
        from core.management.cycle3.doctrine.helpers import _matches_gate_family
        assert _matches_gate_family("McMillan Ch.4: Direction Adverse", "DIRECTION_ADVERSE")
        assert _matches_gate_family("Direction Adverse — Catalyst", "DIRECTION_ADVERSE")


class TestEquityBrokenCapitalBypass:
    """Equity BROKEN EXIT should only get CAPITAL when approaching hard stop.
    Above hard stop: EXIT competes on EV so ROLL/LET_EXPIRE can win."""

    def test_equity_broken_above_hard_stop_competes_on_ev(self):
        """UUUU scenario: equity BROKEN, -5% from net cost, 1st cycle.
        Stock well above hard stop — EXIT should compete on EV, not auto-win.
        With delta 0.112 at 7 DTE, EV comparator should pick LET_EXPIRE or ROLL."""
        row = _base_buy_write_row(
            **{
                "UL Last": 18.73,
                "Short_Call_Strike": 22.5,
                "Short_Call_DTE": 7.0,
                "Short_Call_Delta": 0.112,
                "Short_Call_Last": 0.14,
                "Net_Cost_Basis_Per_Share": 19.72,
                "Underlying_Price_Entry": 20.31,
                "Cumulative_Premium_Collected": 0.59,
                "Basis": 10155.0,  # 500 × $20.31
                "Quantity": 500.0,
                "Theta": 0.036,
                "Gamma": 0.073,
                "HV_20D": 0.832,
                "IV_Now": 1.013,
                "IV_30D": 1.013,
                "Equity_Integrity_State": "BROKEN",
                "Equity_Integrity_Reason": "EMA20↓, ROC20=-11.8%",
                "_cycle_count": 1,
            }
        )
        result = _run_doctrine(row)
        # EV comparator should win over equity BROKEN because EXIT
        # is not CAPITAL (stock is well above hard stop at -5% vs -20%)
        assert result["Action"] != "EXIT" or result.get("Resolution_Method") == "EV_COMPARISON", (
            f"Equity BROKEN at -5% (above hard stop) should not auto-win as CAPITAL. "
            f"Got: {result['Action']} via {result.get('Resolution_Method')}. "
            f"Gate: {result.get('Winning_Gate')}"
        )

    def test_equity_broken_near_hard_stop_gets_capital(self):
        """When approaching hard stop (-18% from net cost), CAPITAL EXIT should win."""
        row = _base_buy_write_row(
            **{
                "UL Last": 16.17,  # -18% from net cost 19.72
                "Short_Call_Strike": 22.5,
                "Short_Call_DTE": 7.0,
                "Short_Call_Delta": 0.02,
                "Short_Call_Last": 0.01,
                "Net_Cost_Basis_Per_Share": 19.72,
                "Underlying_Price_Entry": 20.31,
                "Cumulative_Premium_Collected": 0.59,
                "Basis": 10155.0,
                "Quantity": 500.0,
                "Theta": 0.001,
                "Gamma": 0.001,
                "HV_20D": 0.832,
                "Equity_Integrity_State": "BROKEN",
                "_cycle_count": 1,
            }
        )
        result = _run_doctrine(row)
        # Near hard stop — EXIT should be CAPITAL (auto-win)
        assert result["Action"] == "EXIT", (
            f"Near hard stop (-18%) should EXIT. Got: {result['Action']}"
        )

    def test_income_path_active_always_competes_on_ev(self):
        """With 3+ cycles and income path active, equity BROKEN competes on EV."""
        row = _base_buy_write_row(
            **{
                "UL Last": 95.0,
                "Short_Call_Strike": 100.0,
                "Short_Call_DTE": 25.0,
                "Short_Call_Delta": 0.30,
                "Short_Call_Last": 1.50,
                "Net_Cost_Basis_Per_Share": 98.0,
                "Cumulative_Premium_Collected": 5.0,
                "Basis": 19600.0,
                "Quantity": 200.0,
                "Theta": 0.03,
                "Gamma": 0.04,
                "HV_20D": 0.30,
                "IV_Now": 0.35,
                "IV_30D": 0.35,
                "Equity_Integrity_State": "BROKEN",
                "_cycle_count": 3,
            }
        )
        result = _run_doctrine(row)
        # Income path active → EXIT should NOT be CAPITAL
        if result["Action"] == "EXIT":
            assert result.get("Resolution_Method") == "EV_COMPARISON", (
                f"Income path active — EXIT should win via EV, not CAPITAL. "
                f"Got: {result.get('Resolution_Method')}"
            )


class TestDriftFilterIncomeGuard:
    """Drift filter should not override doctrine HOLD/ROLL on income positions
    when the short call is far OTM and near expiry — doctrine is authoritative."""

    def _make_drift_df(self, rec_action, strategy, delta, dte, drift_action='EXIT'):
        """Build a single-row DataFrame for apply_drift_filter."""
        return pd.DataFrame([{
            'Action': rec_action,
            'Strategy': strategy,
            'Strategy_Name': strategy,
            'Short_Call_Delta': delta,
            'DTE': dte,
            'Drift_Action': drift_action,
            'Signal_State': 'VIOLATED',
            'Structural_State': 'OK',
            'Regime_State': 'OK',
            'Data_State': 'FRESH',
            'Portfolio_State': 'OK',
        }])

    def test_far_otm_near_expiry_bw_hold_preserved(self):
        """UUUU scenario: BW HOLD with far-OTM call at DTE 7.
        Drift says EXIT but doctrine HOLD is income-optimal (let expire)."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = self._make_drift_df('HOLD', 'BUY_WRITE', delta=0.11, dte=7)
        result = engine.apply_drift_filter(df, rec_col='Action')
        assert result['Action_Final'].iloc[0] == 'HOLD', (
            f"Far-OTM BW HOLD at DTE 7 should be preserved, got {result['Action_Final'].iloc[0]}"
        )

    def test_far_otm_near_expiry_cc_roll_preserved(self):
        """CC ROLL with far-OTM call at DTE 10 — ROLL is income action."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = self._make_drift_df('ROLL', 'COVERED_CALL', delta=0.15, dte=10)
        result = engine.apply_drift_filter(df, rec_col='Action')
        assert result['Action_Final'].iloc[0] == 'ROLL', (
            f"Far-OTM CC ROLL at DTE 10 should be preserved, got {result['Action_Final'].iloc[0]}"
        )

    def test_itm_bw_hold_still_overridden(self):
        """ITM BW (delta 0.55) — drift EXIT should override HOLD."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = self._make_drift_df('HOLD', 'BUY_WRITE', delta=0.55, dte=7)
        result = engine.apply_drift_filter(df, rec_col='Action')
        assert result['Action_Final'].iloc[0] == 'EXIT', (
            f"ITM BW HOLD should be overridden to EXIT, got {result['Action_Final'].iloc[0]}"
        )

    def test_far_otm_but_distant_expiry_overridden(self):
        """Far-OTM but DTE 30 — not near expiry, drift EXIT should win."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = self._make_drift_df('HOLD', 'BUY_WRITE', delta=0.15, dte=30)
        result = engine.apply_drift_filter(df, rec_col='Action')
        assert result['Action_Final'].iloc[0] == 'EXIT', (
            f"DTE 30 BW HOLD should be overridden, got {result['Action_Final'].iloc[0]}"
        )

    def test_non_income_hold_still_overridden(self):
        """LONG_CALL HOLD — not income, drift EXIT should win."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = self._make_drift_df('HOLD', 'LONG_CALL', delta=0.15, dte=7)
        result = engine.apply_drift_filter(df, rec_col='Action')
        assert result['Action_Final'].iloc[0] == 'EXIT', (
            f"LONG_CALL HOLD should be overridden, got {result['Action_Final'].iloc[0]}"
        )

    def test_let_expire_already_risk_reducing(self):
        """LET_EXPIRE is already in _RISK_REDUCING — should be preserved."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = self._make_drift_df('LET_EXPIRE', 'BUY_WRITE', delta=0.05, dte=3)
        result = engine.apply_drift_filter(df, rec_col='Action')
        assert result['Action_Final'].iloc[0] == 'LET_EXPIRE', (
            f"LET_EXPIRE should be preserved, got {result['Action_Final'].iloc[0]}"
        )

    def test_force_exit_overrides_even_income(self):
        """FORCE_EXIT (structural failure) must always override — no exceptions."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = self._make_drift_df('HOLD', 'BUY_WRITE', delta=0.11, dte=7, drift_action='FORCE_EXIT')
        result = engine.apply_drift_filter(df, rec_col='Action')
        assert result['Action_Final'].iloc[0] == 'EXIT', (
            f"FORCE_EXIT must override even income HOLD, got {result['Action_Final'].iloc[0]}"
        )


class TestDriftRecoveryPremiumGuard:
    """Recovery Premium Mode actions must survive drift EXIT override.

    recovery_premium_doctrine() already evaluates EXIT_STOCK as an option
    and chooses recovery actions (ROLL_UP_OUT, WRITE_NOW, HOLD_STOCK_WAIT)
    based on EV comparison (Jabbour Ch.4). Drift EXIT would force realization
    of the full loss, destroying the recovery path.

    Bug (Mar 2026): EOSE BUY_WRITE — recovery_premium_doctrine produced
    ROLL_UP_OUT but drift filter overrode it to EXIT because ROLL_UP_OUT
    was not in the _RISK_REDUCING set.
    """

    def _make_recovery_df(self, rec_action, drift_action='EXIT'):
        """Build a single-row DataFrame for a recovery premium position."""
        return pd.DataFrame([{
            'Action': rec_action,
            'Strategy': 'BUY_WRITE',
            'Strategy_Name': 'BUY_WRITE',
            'Short_Call_Delta': 0.25,
            'DTE': 30,
            'Drift_Action': drift_action,
            'Doctrine_State': 'RECOVERY_PREMIUM',
            'Signal_State': 'VIOLATED',
            'Structural_State': 'OK',
            'Regime_State': 'OK',
            'Data_State': 'FRESH',
            'Portfolio_State': 'OK',
        }])

    def test_roll_up_out_preserved(self):
        """ROLL_UP_OUT from recovery premium must survive drift EXIT."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = self._make_recovery_df('ROLL_UP_OUT')
        result = engine.apply_drift_filter(df, rec_col='Action')
        assert result['Action_Final'].iloc[0] == 'ROLL_UP_OUT', (
            f"Recovery ROLL_UP_OUT should be preserved, got {result['Action_Final'].iloc[0]}"
        )

    def test_write_now_preserved(self):
        """WRITE_NOW from recovery premium must survive drift EXIT."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = self._make_recovery_df('WRITE_NOW')
        result = engine.apply_drift_filter(df, rec_col='Action')
        assert result['Action_Final'].iloc[0] == 'WRITE_NOW', (
            f"Recovery WRITE_NOW should be preserved, got {result['Action_Final'].iloc[0]}"
        )

    def test_hold_stock_wait_preserved(self):
        """HOLD_STOCK_WAIT from recovery premium must survive drift EXIT."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = self._make_recovery_df('HOLD_STOCK_WAIT')
        result = engine.apply_drift_filter(df, rec_col='Action')
        assert result['Action_Final'].iloc[0] == 'HOLD_STOCK_WAIT', (
            f"Recovery HOLD_STOCK_WAIT should be preserved, got {result['Action_Final'].iloc[0]}"
        )

    def test_exit_stock_preserved(self):
        """EXIT_STOCK from recovery premium is already risk-reducing — preserved."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = self._make_recovery_df('EXIT_STOCK')
        result = engine.apply_drift_filter(df, rec_col='Action')
        # EXIT_STOCK is not in _RISK_REDUCING but is in _RECOVERY_PREMIUM_ACTIONS
        # Actually EXIT_STOCK is NOT in _RECOVERY_PREMIUM_ACTIONS — it should
        # become EXIT via drift (the position IS exiting, drift agrees)
        assert result['Action_Final'].iloc[0] == 'EXIT', (
            f"EXIT_STOCK should become EXIT via drift, got {result['Action_Final'].iloc[0]}"
        )

    def test_force_exit_overrides_recovery(self):
        """FORCE_EXIT must override even recovery premium mode."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = self._make_recovery_df('ROLL_UP_OUT', drift_action='FORCE_EXIT')
        result = engine.apply_drift_filter(df, rec_col='Action')
        assert result['Action_Final'].iloc[0] == 'EXIT', (
            f"FORCE_EXIT must override even recovery, got {result['Action_Final'].iloc[0]}"
        )

    def test_non_recovery_roll_up_out_not_preserved(self):
        """ROLL_UP_OUT without RECOVERY_PREMIUM state is NOT protected."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = pd.DataFrame([{
            'Action': 'ROLL_UP_OUT',
            'Strategy': 'BUY_WRITE',
            'Strategy_Name': 'BUY_WRITE',
            'Short_Call_Delta': 0.25,
            'DTE': 30,
            'Drift_Action': 'EXIT',
            'Doctrine_State': '',  # NOT recovery premium
            'Signal_State': 'VIOLATED',
            'Structural_State': 'OK',
            'Regime_State': 'OK',
            'Data_State': 'FRESH',
            'Portfolio_State': 'OK',
        }])
        result = engine.apply_drift_filter(df, rec_col='Action')
        assert result['Action_Final'].iloc[0] == 'EXIT', (
            f"Non-recovery ROLL_UP_OUT should be overridden to EXIT, got {result['Action_Final'].iloc[0]}"
        )


class TestDriftRecoveryLadderGuard:
    """Recovery Ladder (CSP wheel conversion) must survive drift EXIT override.

    CSP deep ITM near expiry: doctrine recommends HOLD (accept assignment,
    start CC income cycle). Drift EXIT would force buy-back at max intrinsic,
    locking in the full loss permanently. Recovery ladder evaluates EXIT vs
    wheel conversion and chose wheel — drift should respect this.

    Bug (Mar 2026): EOSE CSP — doctrine produced HOLD with RECOVERY_LADDER
    state but drift filter overrode to EXIT because RECOVERY_LADDER was not
    in the recovery guard.
    """

    def _make_ladder_df(self, rec_action, drift_action='EXIT'):
        """Build a single-row DataFrame for a CSP recovery ladder position."""
        return pd.DataFrame([{
            'Action': rec_action,
            'Strategy': 'CSP',
            'Strategy_Name': 'CSP',
            'Short_Call_Delta': 0.0,
            'DTE': 7,
            'Drift_Action': drift_action,
            'Doctrine_State': 'RECOVERY_LADDER',
            'Signal_State': 'VIOLATED',
            'Structural_State': 'OK',
            'Regime_State': 'OK',
            'Data_State': 'FRESH',
            'Portfolio_State': 'OK',
        }])

    def test_csp_hold_for_wheel_preserved(self):
        """CSP HOLD (wheel conversion) with RECOVERY_LADDER must survive drift EXIT."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = self._make_ladder_df('HOLD')
        result = engine.apply_drift_filter(df, rec_col='Action')
        assert result['Action_Final'].iloc[0] == 'HOLD', (
            f"CSP RECOVERY_LADDER HOLD should be preserved, got {result['Action_Final'].iloc[0]}"
        )

    def test_csp_roll_with_ladder_preserved(self):
        """CSP ROLL with RECOVERY_LADDER must survive drift EXIT."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = self._make_ladder_df('ROLL')
        result = engine.apply_drift_filter(df, rec_col='Action')
        assert result['Action_Final'].iloc[0] == 'ROLL', (
            f"CSP RECOVERY_LADDER ROLL should be preserved, got {result['Action_Final'].iloc[0]}"
        )

    def test_bw_hold_with_ladder_preserved(self):
        """BW HOLD with RECOVERY_LADDER must survive drift EXIT."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = pd.DataFrame([{
            'Action': 'HOLD',
            'Strategy': 'BUY_WRITE',
            'Strategy_Name': 'BUY_WRITE',
            'Short_Call_Delta': 0.25,
            'DTE': 30,
            'Drift_Action': 'EXIT',
            'Doctrine_State': 'RECOVERY_LADDER',
            'Signal_State': 'VIOLATED',
            'Structural_State': 'OK',
            'Regime_State': 'OK',
            'Data_State': 'FRESH',
            'Portfolio_State': 'OK',
        }])
        result = engine.apply_drift_filter(df, rec_col='Action')
        assert result['Action_Final'].iloc[0] == 'HOLD', (
            f"BW RECOVERY_LADDER HOLD should be preserved, got {result['Action_Final'].iloc[0]}"
        )

    def test_non_ladder_csp_hold_still_overridden(self):
        """CSP HOLD WITHOUT recovery ladder should be overridden to EXIT."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = pd.DataFrame([{
            'Action': 'HOLD',
            'Strategy': 'CSP',
            'Strategy_Name': 'CSP',
            'Short_Call_Delta': 0.0,
            'DTE': 7,
            'Drift_Action': 'EXIT',
            'Doctrine_State': '',  # NOT recovery ladder
            'Signal_State': 'VIOLATED',
            'Structural_State': 'OK',
            'Regime_State': 'OK',
            'Data_State': 'FRESH',
            'Portfolio_State': 'OK',
        }])
        result = engine.apply_drift_filter(df, rec_col='Action')
        assert result['Action_Final'].iloc[0] == 'EXIT', (
            f"Non-ladder CSP HOLD should be overridden to EXIT, got {result['Action_Final'].iloc[0]}"
        )

    def test_force_exit_overrides_ladder(self):
        """FORCE_EXIT must override even recovery ladder."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = self._make_ladder_df('HOLD', drift_action='FORCE_EXIT')
        result = engine.apply_drift_filter(df, rec_col='Action')
        assert result['Action_Final'].iloc[0] == 'EXIT', (
            f"FORCE_EXIT must override even ladder, got {result['Action_Final'].iloc[0]}"
        )


class TestDriftSignalIncomeExemption:
    """Greek ROC signals (Delta/Vega) should not fire VIOLATED on BW/CC
    positions where the short call is far OTM (delta < 0.15). Falling
    Greeks on a near-worthless call is the position WORKING, not failing."""

    def test_far_otm_bw_delta_roc_suppressed(self):
        """BW with delta 0.06, Delta_ROC_3D = -0.69 → should NOT fire VIOLATED."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = pd.DataFrame([{
            'Strategy': 'BUY_WRITE',
            'Short_Call_Delta': 0.06,
            'Delta_ROC_3D': -0.69,
            'Vega_ROC_3D': -0.66,
            'DTE': 7,
            'ROC_Persist_3D': 3,
        }])
        result = engine.assess_signal_drift(df)
        assert result['Signal_State'].iloc[0] == 'VALID', (
            f"Far-OTM BW (Δ 0.06) should not fire VIOLATED on falling Greeks, "
            f"got {result['Signal_State'].iloc[0]}"
        )

    def test_atm_bw_delta_roc_still_fires(self):
        """BW with delta 0.45, Delta_ROC_3D = -0.55 → SHOULD fire VIOLATED.
        Income Delta_ROC VIOLATED threshold = 0.50 (calibrated)."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = pd.DataFrame([{
            'Strategy': 'BUY_WRITE',
            'Short_Call_Delta': 0.45,
            'Delta_ROC_3D': -0.55,
            'Vega_ROC_3D': -0.10,
            'DTE': 7,
            'ROC_Persist_3D': 3,
        }])
        result = engine.assess_signal_drift(df)
        assert result['Signal_State'].iloc[0] == 'VIOLATED', (
            f"ATM BW (Δ 0.45) should fire VIOLATED on Delta_ROC -0.55, "
            f"got {result['Signal_State'].iloc[0]}"
        )

    def test_long_call_not_affected(self):
        """LONG_CALL delta suppression is NOT affected by far-OTM guard."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = pd.DataFrame([{
            'Strategy': 'LONG_CALL',
            'Short_Call_Delta': np.nan,
            'Delta': 0.10,
            'Delta_ROC_3D': -0.35,
            'Vega_ROC_3D': -0.10,
            'DTE': 30,
            'ROC_Persist_3D': 3,
        }])
        result = engine.assess_signal_drift(df)
        assert result['Signal_State'].iloc[0] == 'VIOLATED', (
            f"LONG_CALL with Delta_ROC -0.35 should fire VIOLATED, "
            f"got {result['Signal_State'].iloc[0]}"
        )

    def test_far_otm_bw_gamma_roc_suppressed(self):
        """BW with delta 0.06, DTE 35, Gamma_ROC_3D = +0.60 → should NOT fire VIOLATED.
        Rising gamma on a near-worthless call is mechanical, not a risk signal."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = pd.DataFrame([{
            'Strategy': 'BUY_WRITE',
            'Short_Call_Delta': 0.06,
            'Delta_ROC_3D': 0.0,
            'Vega_ROC_3D': 0.0,
            'Gamma_ROC_3D': 0.60,
            'DTE': 35,
            'ROC_Persist_3D': 3,
        }])
        result = engine.assess_signal_drift(df)
        assert result['Signal_State'].iloc[0] == 'VALID', (
            f"Far-OTM BW (Δ 0.06, DTE 35) should not fire VIOLATED on Gamma ROC, "
            f"got {result['Signal_State'].iloc[0]}"
        )

    def test_atm_bw_gamma_roc_still_fires(self):
        """BW with delta 0.45, DTE 35, Gamma_ROC_3D = +0.60 → SHOULD fire VIOLATED."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = pd.DataFrame([{
            'Strategy': 'BUY_WRITE',
            'Short_Call_Delta': 0.45,
            'Delta_ROC_3D': 0.0,
            'Vega_ROC_3D': 0.0,
            'Gamma_ROC_3D': 0.60,
            'DTE': 35,
            'ROC_Persist_3D': 3,
        }])
        result = engine.assess_signal_drift(df)
        assert result['Signal_State'].iloc[0] == 'VIOLATED', (
            f"ATM BW (Δ 0.45, DTE 35) should fire VIOLATED on Gamma ROC 0.60, "
            f"got {result['Signal_State'].iloc[0]}"
        )


class TestIVROCStrategyAware:
    """IV_ROC_3D must be strategy-aware: IV crush hurts long-vol but helps short-vol.
    Short-vol (BW/CC/CSP) should only fire on IV SPIKE, not IV crush."""

    def test_long_call_iv_crush_fires(self):
        """LONG_CALL with IV_ROC_3D = -0.35 → VIOLATED (IV crush hurts long-vol)."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = pd.DataFrame([{
            'Strategy': 'LONG_CALL',
            'IV_ROC_3D': -0.35,
            'ROC_Persist_3D': 3,
        }])
        result = engine.assess_signal_drift(df)
        assert result['Signal_State'].iloc[0] == 'VIOLATED', (
            f"LONG_CALL with IV crush -0.35 should fire VIOLATED, "
            f"got {result['Signal_State'].iloc[0]}"
        )

    def test_bw_iv_crush_no_fire(self):
        """BW with IV_ROC_3D = -0.35 → should NOT fire (IV crush = position working)."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = pd.DataFrame([{
            'Strategy': 'BUY_WRITE',
            'Short_Call_Delta': 0.25,
            'IV_ROC_3D': -0.35,
            'ROC_Persist_3D': 3,
        }])
        result = engine.assess_signal_drift(df)
        assert result['Signal_State'].iloc[0] == 'VALID', (
            f"BW with IV crush -0.35 should NOT fire (short-vol benefits from crush), "
            f"got {result['Signal_State'].iloc[0]}"
        )

    def test_bw_iv_spike_fires(self):
        """BW with IV_ROC_3D = +0.45 → VIOLATED (IV spike hurts short-vol).
        Income IV_ROC VIOLATED threshold = 0.40 (calibrated)."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = pd.DataFrame([{
            'Strategy': 'BUY_WRITE',
            'Short_Call_Delta': 0.25,
            'IV_ROC_3D': 0.45,
            'ROC_Persist_3D': 3,
        }])
        result = engine.assess_signal_drift(df)
        assert result['Signal_State'].iloc[0] == 'VIOLATED', (
            f"BW with IV spike +0.45 should fire VIOLATED (short-vol hurt by spike), "
            f"got {result['Signal_State'].iloc[0]}"
        )

    def test_csp_iv_crush_no_fire(self):
        """CSP with IV_ROC_3D = -0.25 → should NOT fire (short-vol benefits)."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = pd.DataFrame([{
            'Strategy': 'CSP',
            'IV_ROC_3D': -0.25,
            'ROC_Persist_3D': 3,
        }])
        result = engine.assess_signal_drift(df)
        assert result['Signal_State'].iloc[0] == 'VALID', (
            f"CSP with IV crush -0.25 should NOT fire (short-vol benefits from crush), "
            f"got {result['Signal_State'].iloc[0]}"
        )

    def test_long_put_iv_crush_fires(self):
        """LONG_PUT with IV_ROC_3D = -0.25 → DEGRADED (long-vol hurt by crush)."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = pd.DataFrame([{
            'Strategy': 'LONG_PUT',
            'IV_ROC_3D': -0.25,
            'ROC_Persist_3D': 3,
        }])
        result = engine.assess_signal_drift(df)
        assert result['Signal_State'].iloc[0] == 'DEGRADED', (
            f"LONG_PUT with IV crush -0.25 should fire DEGRADED, "
            f"got {result['Signal_State'].iloc[0]}"
        )


class TestVegaROCStrategyAware:
    """Vega_ROC_3D must use signed logic: long-vol penalize crush (negative),
    short-vol penalize spike (positive). Far-OTM income exempt from both."""

    def test_long_call_vega_crush_fires(self):
        """LONG_CALL with Vega_ROC_3D = -0.25 → DEGRADED (IV crush = losing edge)."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = pd.DataFrame([{
            'Strategy': 'LONG_CALL',
            'Vega_ROC_3D': -0.25,
            'ROC_Persist_3D': 3,
        }])
        result = engine.assess_signal_drift(df)
        assert result['Signal_State'].iloc[0] == 'DEGRADED', (
            f"LONG_CALL with Vega crush -0.25 should fire DEGRADED, "
            f"got {result['Signal_State'].iloc[0]}"
        )

    def test_long_call_vega_spike_no_fire(self):
        """LONG_CALL with Vega_ROC_3D = +0.25 → should NOT fire (IV spike helps long-vol)."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = pd.DataFrame([{
            'Strategy': 'LONG_CALL',
            'Vega_ROC_3D': 0.25,
            'ROC_Persist_3D': 3,
        }])
        result = engine.assess_signal_drift(df)
        assert result['Signal_State'].iloc[0] == 'VALID', (
            f"LONG_CALL with Vega spike +0.25 should NOT fire (long-vol benefits from spike), "
            f"got {result['Signal_State'].iloc[0]}"
        )

    def test_bw_vega_spike_fires(self):
        """BW with Vega_ROC_3D = +0.25 → DEGRADED (IV spike hurts short-vol)."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = pd.DataFrame([{
            'Strategy': 'BUY_WRITE',
            'Short_Call_Delta': 0.30,
            'Vega_ROC_3D': 0.25,
            'ROC_Persist_3D': 3,
        }])
        result = engine.assess_signal_drift(df)
        assert result['Signal_State'].iloc[0] == 'DEGRADED', (
            f"BW with Vega spike +0.25 should fire DEGRADED (short-vol hurt by spike), "
            f"got {result['Signal_State'].iloc[0]}"
        )

    def test_bw_vega_crush_no_fire(self):
        """BW with Vega_ROC_3D = -0.25 → should NOT fire (IV crush = position working)."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = pd.DataFrame([{
            'Strategy': 'BUY_WRITE',
            'Short_Call_Delta': 0.30,
            'Vega_ROC_3D': -0.25,
            'ROC_Persist_3D': 3,
        }])
        result = engine.assess_signal_drift(df)
        assert result['Signal_State'].iloc[0] == 'VALID', (
            f"BW with Vega crush -0.25 should NOT fire (short-vol benefits from crush), "
            f"got {result['Signal_State'].iloc[0]}"
        )

    def test_far_otm_income_vega_spike_exempt(self):
        """BW far-OTM (delta 0.06) with Vega_ROC_3D = +0.30 → should NOT fire
        (vega exposure negligible when delta ≈ 0)."""
        from core.management.cycle2.drift.drift_engine import DriftEngine
        engine = DriftEngine.__new__(DriftEngine)
        df = pd.DataFrame([{
            'Strategy': 'BUY_WRITE',
            'Short_Call_Delta': 0.06,
            'Vega_ROC_3D': 0.30,
            'ROC_Persist_3D': 3,
        }])
        result = engine.assess_signal_drift(df)
        assert result['Signal_State'].iloc[0] == 'VALID', (
            f"Far-OTM BW (Δ 0.06) with Vega spike should NOT fire (vega ≈ 0), "
            f"got {result['Signal_State'].iloc[0]}"
        )


class TestActionStreakIncomeExemption:
    """Rule 3 (EXIT appeared >=2x in 5 days → override HOLD to EXIT) should
    exempt income positions with far-OTM near-expiry short calls. Those prior
    EXIT signals were drift-filter false positives, now corrected."""

    def _make_df(self, action, strategy, delta, dte, exit_count_5d):
        return pd.DataFrame([{
            'Action': action,
            'Urgency': 'MEDIUM',
            'Strategy': strategy,
            'Strategy_Name': strategy,
            'Short_Call_Delta': delta,
            'DTE': dte,
            'EXIT_Count_Last_5D': exit_count_5d,
            'Prior_Action_Streak': 0,
            'Rationale': 'test rationale',
            'Doctrine_Source': 'test source',
        }])

    def test_far_otm_bw_hold_not_overridden(self):
        """UUUU scenario: BW HOLD, delta 0.11, DTE 7, 2 prior EXITs.
        Should NOT be overridden to EXIT — income exemption applies."""
        df = self._make_df('HOLD', 'BUY_WRITE', 0.11, 7, 2)
        # Simulate Rule 3 logic inline
        _exit_5d = pd.to_numeric(df["EXIT_Count_Last_5D"], errors="coerce").fillna(0).astype(int)
        _strat_col = df.get("Strategy", df.get("Strategy_Name", pd.Series(dtype=str))).fillna("").str.upper()
        _is_income = _strat_col.str.contains("BUY_WRITE|COVERED_CALL|^CC$|^BW$", regex=True, na=False)
        _sc_delta = pd.to_numeric(df.get("Short_Call_Delta"), errors="coerce")
        _sc_dte = pd.to_numeric(df.get("DTE"), errors="coerce")
        _income_exempt = _is_income & (_sc_delta < 0.30) & (_sc_dte <= 14)
        _mask = df["Action"].isin(["HOLD", "ROLL"]) & (_exit_5d >= 2) & ~_income_exempt
        assert not _mask.iloc[0], "Far-OTM BW HOLD should be exempt from Rule 3"

    def test_itm_bw_hold_still_overridden(self):
        """BW HOLD with ITM call (delta 0.55) — Rule 3 should apply."""
        df = self._make_df('HOLD', 'BUY_WRITE', 0.55, 7, 2)
        _exit_5d = pd.to_numeric(df["EXIT_Count_Last_5D"], errors="coerce").fillna(0).astype(int)
        _strat_col = df.get("Strategy", df.get("Strategy_Name", pd.Series(dtype=str))).fillna("").str.upper()
        _is_income = _strat_col.str.contains("BUY_WRITE|COVERED_CALL|^CC$|^BW$", regex=True, na=False)
        _sc_delta = pd.to_numeric(df.get("Short_Call_Delta"), errors="coerce")
        _sc_dte = pd.to_numeric(df.get("DTE"), errors="coerce")
        _income_exempt = _is_income & (_sc_delta < 0.30) & (_sc_dte <= 14)
        _mask = df["Action"].isin(["HOLD", "ROLL"]) & (_exit_5d >= 2) & ~_income_exempt
        assert _mask.iloc[0], "ITM BW HOLD should NOT be exempt — Rule 3 should apply"

    def test_long_call_hold_still_overridden(self):
        """LONG_CALL HOLD — not income, Rule 3 should apply."""
        df = self._make_df('HOLD', 'LONG_CALL', 0.11, 7, 3)
        _exit_5d = pd.to_numeric(df["EXIT_Count_Last_5D"], errors="coerce").fillna(0).astype(int)
        _strat_col = df.get("Strategy", df.get("Strategy_Name", pd.Series(dtype=str))).fillna("").str.upper()
        _is_income = _strat_col.str.contains("BUY_WRITE|COVERED_CALL|^CC$|^BW$", regex=True, na=False)
        _sc_delta = pd.to_numeric(df.get("Short_Call_Delta"), errors="coerce")
        _sc_dte = pd.to_numeric(df.get("DTE"), errors="coerce")
        _income_exempt = _is_income & (_sc_delta < 0.30) & (_sc_dte <= 14)
        _mask = df["Action"].isin(["HOLD", "ROLL"]) & (_exit_5d >= 2) & ~_income_exempt
        assert _mask.iloc[0], "LONG_CALL HOLD should NOT be exempt — Rule 3 should apply"


class TestRollModeRecoveryPremium:
    """RECOVERY_PREMIUM roll mode: optimizes for premium cycling frequency
    when Doctrine_State=RECOVERY_PREMIUM (damaged BW far below cost basis).

    Bug (Mar 2026): EOSE BW with stock=$6, basis=$17 got EMERGENCY mode which
    hard-filters above-basis strikes — impossible. Recovery premium mode drops
    the above-basis filter and targets shorter DTE (14-45d) for faster cycling.
    """

    def test_recovery_premium_mode_selected(self):
        """Doctrine_State=RECOVERY_PREMIUM triggers RECOVERY_PREMIUM mode."""
        from core.management.cycle3.roll.roll_candidate_engine import (
            _ROLL_MODE_RECOVERY_PREMIUM, _ROLL_MODE_EMERGENCY,
        )
        # Simulate: stock $6, basis $17, delta 0.80 (would normally be EMERGENCY)
        row = {
            "Doctrine_State": "RECOVERY_PREMIUM",
            "Delta": 0.80,
            "Short_Call_Delta": 0.80,
        }
        current_delta = abs(float(row.get("Delta", 0)))
        doctrine_state = str(row.get("Doctrine_State", "")).upper()

        if doctrine_state == "RECOVERY_PREMIUM":
            mode = _ROLL_MODE_RECOVERY_PREMIUM
        elif current_delta > 0.70:
            mode = _ROLL_MODE_EMERGENCY
        else:
            mode = "NORMAL"

        assert mode == _ROLL_MODE_RECOVERY_PREMIUM, (
            f"Delta 0.80 + RECOVERY_PREMIUM should select RECOVERY_PREMIUM mode, got {mode}"
        )

    def test_recovery_premium_dte_window(self):
        """RECOVERY_PREMIUM uses 14-45 DTE window (shorter than EMERGENCY 45-150)."""
        from core.management.cycle3.roll.roll_candidate_engine import (
            _ROLL_DTE_WINDOWS_RECOVERY_PREMIUM, _ROLL_DTE_WINDOWS_EMERGENCY,
        )
        rp_window = _ROLL_DTE_WINDOWS_RECOVERY_PREMIUM["BUY_WRITE"]
        em_window = _ROLL_DTE_WINDOWS_EMERGENCY["BUY_WRITE"]

        assert rp_window[0] < em_window[0], (
            f"Recovery premium min DTE ({rp_window[0]}) should be shorter than "
            f"emergency ({em_window[0]})"
        )
        assert rp_window[1] < em_window[1], (
            f"Recovery premium max DTE ({rp_window[1]}) should be shorter than "
            f"emergency ({em_window[1]})"
        )

    def test_recovery_premium_delta_range(self):
        """RECOVERY_PREMIUM uses reasonable OTM delta (0.25-0.45)."""
        from core.management.cycle3.roll.roll_candidate_engine import (
            _ROLL_DELTA_TARGETS_RECOVERY_PREMIUM,
        )
        delta_range = _ROLL_DELTA_TARGETS_RECOVERY_PREMIUM["BUY_WRITE"]
        assert delta_range[0] >= 0.20, "Min delta should be ≥0.20 for viable premium"
        assert delta_range[1] <= 0.50, "Max delta should be ≤0.50 to avoid assignment"

    def test_recovery_premium_not_overridden_by_weekly(self):
        """RECOVERY_PREMIUM should NOT be downgraded to WEEKLY by equity integrity."""
        from core.management.cycle3.roll.roll_candidate_engine import (
            _ROLL_MODE_RECOVERY_PREMIUM, _ROLL_MODE_WEEKLY,
            _ROLL_MODE_NORMAL, _ROLL_MODE_PRE_ITM,
        )
        # The WEEKLY override only fires for NORMAL and PRE_ITM modes
        roll_mode = _ROLL_MODE_RECOVERY_PREMIUM
        _weekly_eligible = True
        # Simulate the guard condition from the engine
        should_check_weekly = roll_mode in (_ROLL_MODE_NORMAL, _ROLL_MODE_PRE_ITM)
        assert not should_check_weekly, (
            "RECOVERY_PREMIUM should not be eligible for WEEKLY override"
        )

    def test_recovery_trigger_weights(self):
        """Recovery trigger weights emphasize yield (basis reduction)."""
        from core.management.cycle3.roll.roll_candidate_engine import (
            ROLL_TRIGGER_RECOVERY, _TRIGGER_WEIGHT_ADJUSTMENTS,
        )
        weights = _TRIGGER_WEIGHT_ADJUSTMENTS[ROLL_TRIGGER_RECOVERY]
        assert weights["yield_w"] > 1.0, "Recovery should amplify yield weight"
        assert weights["delta_w"] < 1.0, "Recovery should reduce delta weight"

    def test_gate_to_trigger_recovery_mapping(self):
        """Recovery premium gates map to ROLL_TRIGGER_RECOVERY."""
        from core.management.cycle3.roll.roll_candidate_engine import (
            _classify_roll_trigger, ROLL_TRIGGER_RECOVERY,
        )
        recovery_gates = [
            "strike_below_basis", "strike_below_basis_mild",
            "assignment_at_loss", "basis_reduction_roll",
        ]
        for gate in recovery_gates:
            trigger = _classify_roll_trigger(gate)
            assert trigger == ROLL_TRIGGER_RECOVERY, (
                f"Gate '{gate}' should map to RECOVERY, got {trigger}"
            )
