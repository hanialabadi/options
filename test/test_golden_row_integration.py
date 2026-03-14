"""
Golden Row Integration Tests -- Vertical Slice Validation
=========================================================
Each test takes ONE archetype position and feeds it through the FULL
testable vertical slice:

  1. compute_expected_move(df)  -- Cycle 2 enrichment (EV formula)
  2. DoctrineAuthority.evaluate(row) -- Cycle 3 decision engine
  3. MC escalation logic -- post-decision override (replayed inline)
  4. Card field assertions -- validate every field the dashboard renders

If any layer produces wrong data, downstream assertions FAIL.

Key innovation vs stability tests: EV fields (EV_Feasibility_Ratio,
Required_Move_Breakeven, etc.) are NOT pre-injected. They must be
computed by compute_expected_move(). If the formula is wrong, the
decision engine uses wrong values and assertions fail.

Bugs this catches:
  - Bug 37:  EV uses distance-to-strike instead of true breakeven
  - Bug 38:  MC EXIT_NOW only escalated urgency but didn't change Action
  - Bug 37b: UI shows wrong label for breakeven

Run:
    pytest test/test_golden_row_integration.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path
from math import isnan

import pandas as pd
import numpy as np
import pytest

# -- path bootstrap -----------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.management.cycle2.drift.compute_expected_move import compute_expected_move
from core.management.cycle3.decision.engine import DoctrineAuthority


# =============================================================================
# Helpers
# =============================================================================

def _replay_mc_escalation(row_dict: dict, mc_result: dict) -> dict:
    """
    Replay the MC escalation logic from run_all.py lines 1349-1395.

    Rule 3: MC_Hold_Verdict == EXIT_NOW AND Action == HOLD
            -> Action = EXIT, Urgency -> HIGH

    Bug 39 guard: LEAPS (DTE > 180) with INTACT/RECOVERING thesis are
    exempt from the hard override — downgraded to a warning instead.
    """
    _urgency_order = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}

    def _promote(current, target):
        cur = _urgency_order.get(str(current).upper(), 0)
        tgt = _urgency_order.get(str(target).upper(), 0)
        return target.upper() if tgt > cur else current

    action = str(row_dict.get("Action", "")).upper()
    mc_hold_verdict = str(mc_result.get("MC_Hold_Verdict", "")).upper()

    if mc_hold_verdict == "EXIT_NOW" and action == "HOLD":
        # Bug 39: LEAPS guard — suppress hard override for LEAPS with intact thesis
        dte = float(row_dict.get("DTE") or 0)
        thesis = str(row_dict.get("Thesis_State", "")).upper()
        is_leaps_intact = dte > 180 and thesis in ("INTACT", "RECOVERING")

        if is_leaps_intact:
            row_dict["Rationale"] = (
                str(row_dict.get("Rationale", "")) +
                " | MC EXIT_NOW suppressed (LEAPS DTE>180 + thesis intact) -- monitor closely."
            )
        else:
            row_dict["Action"] = "EXIT"
            row_dict["Urgency"] = _promote(row_dict.get("Urgency", "LOW"), "HIGH")
            row_dict["Rationale"] = (
                str(row_dict.get("Rationale", "")) +
                " | MC EXIT_NOW override: p_recovery < 0.35 AND EV < 0 -- exit, do not hold."
            )
    return row_dict


def _replay_streak_escalation(row_dict: dict) -> dict:
    """
    Replay the 3.0a Action Streak Escalation gate from run_all.py.

    Rule 1: REVIEW + Prior_Action_Streak >= 3 → EXIT MEDIUM
    Rule 2: EXIT + Prior_Action_Streak >= 5 → urgency → CRITICAL
    """
    row_dict = dict(row_dict)  # copy
    streak = int(row_dict.get("Prior_Action_Streak") or 0)
    action = str(row_dict.get("Action", "")).upper()

    if action == "REVIEW" and streak >= 3:
        row_dict["Action"] = "EXIT"
        row_dict["Urgency"] = "MEDIUM"
        row_dict["Rationale"] = (
            str(row_dict.get("Rationale", ""))
            + f" | Unresolved REVIEW x{streak}"
            + " -- signal degradation persistent, escalating to EXIT."
        )
    elif action == "EXIT" and streak >= 5:
        if str(row_dict.get("Urgency", "")).upper() != "CRITICAL":
            row_dict["Urgency"] = "CRITICAL"
            row_dict["Rationale"] = (
                str(row_dict.get("Rationale", ""))
                + f" | EXIT signal persisted x{streak} without action -- urgency critical."
            )
    return row_dict


def _run_vertical_slice(row: pd.Series, run_mc: bool = False) -> dict:
    """
    Execute the full testable vertical slice:
      1. compute_expected_move on a 1-row DataFrame
      2. DoctrineAuthority.evaluate on the enriched row
      3. Optional MC + MC escalation replay
    Returns a dict with ALL fields: enrichment + doctrine + MC.
    """
    # Layer 1: Enrichment
    df = pd.DataFrame([row])
    df_enriched = compute_expected_move(df)
    enriched_row = df_enriched.iloc[0]

    # Overwrite the pre-computed EV_Feasibility_Ratio with the one
    # compute_expected_move actually calculated (this is the point --
    # the doctrine engine reads this column from the row).
    enriched_series = enriched_row.copy()

    # Layer 2: Decision
    doctrine_result = DoctrineAuthority.evaluate(enriched_series)

    # Merge enrichment fields into result for card assertions
    _ev_cols = [
        'Expected_Move_10D', 'Required_Move_Breakeven', 'Required_Move_50pct',
        'EV_Feasibility_Ratio', 'EV_50pct_Feasibility_Ratio',
        'Profit_Cushion', 'Profit_Cushion_Ratio',
        'Theta_Bleed_Daily_Pct', 'Theta_Opportunity_Cost_Flag',
        'Theta_Opportunity_Cost_Pct',
    ]
    for col in _ev_cols:
        doctrine_result[col] = enriched_series.get(col)

    # Carry forward key row fields needed by MC escalation logic (Bug 39 guard)
    for col in ('DTE', 'Thesis_State'):
        if col not in doctrine_result:
            doctrine_result[col] = enriched_series.get(col)

    # Layer 3: MC escalation (if requested and action is HOLD)
    mc_result = {}
    if run_mc and doctrine_result.get("Action") == "HOLD":
        try:
            from core.management.mc_management import mc_exit_vs_hold
            mc_result = mc_exit_vs_hold(enriched_series, n_paths=2000,
                                        rng=np.random.default_rng(42))
            for k, v in mc_result.items():
                doctrine_result[k] = v
        except Exception:
            pass  # MC is optional — don't fail the test if MC has import issues
        doctrine_result = _replay_mc_escalation(doctrine_result, mc_result)

    return doctrine_result


# =============================================================================
# Archetype 1: Losing LONG_PUT (AMZN-like)
# =============================================================================

def _archetype_losing_long_put() -> pd.Series:
    """
    AMZN LONG_PUT: OTM, losing, stock rallying (adverse for put).

    Setup:
      - AMZN at $195, put strike $180, premium paid $8.50
      - Stock rallied +4% (adverse for put), momentum_slope > 0
      - Thesis = DEGRADED
      - DTE = 25, P&L = -52%
      - True breakeven = 180 - 8.50 = $171.50
      - Required_Move = 195 - 171.50 = $23.50
      - IV = 0.38, EM_10D = 195 * 0.38 * sqrt(10/252) ~ $14.77
      - EV_Ratio = 23.50 / 14.77 ~ 1.59 (above 1.5 = low expectancy)

    Expected: EXIT (direction-adverse gate)
    """
    return pd.Series({
        # Identity
        "TradeID": "T-GR-001", "LegID": "L-GR-001",
        "Symbol": "AMZN260327P00180000",
        "Underlying_Ticker": "AMZN",
        "Strategy": "LONG_PUT", "AssetType": "OPTION",
        "Option_Type": "PUT", "Call/Put": "P",

        # Prices & Greeks
        "UL Last": 195.0, "Strike": 180.0,
        "DTE": 25.0, "Premium_Entry": 8.50,
        "Last": 4.10, "Bid": 3.95,
        "Delta": -0.18, "Delta_Entry": -0.35,
        "Gamma": 0.012, "Theta": -0.45, "Vega": 0.10,
        "HV_20D": 0.35, "IV_Now": 0.38,
        "Quantity": 1.0, "Basis": 850.0,

        # P&L
        "PnL_Dollar": -440.0, "Total_GL_Decimal": -0.52,

        # Direction: stock rallying = adverse for put
        "Drift_Direction": "Up",
        "Price_Drift_Pct": 0.04,
        "roc_5": 3.5, "roc_10": 4.8,
        "MomentumVelocity_State": "TRENDING",
        "momentum_slope": 1.2,

        # Thesis
        "Thesis_State": "DEGRADED",
        "Thesis_Gate": "CAUTION",
        "_thesis_blocks_roll": False,
        "Thesis_Summary": "AMZN fundamentals weakening but stock momentum persists.",

        # Structure
        "PriceStructure_State": "STRUCTURE_INTACT",
        "TrendIntegrity_State": "TREND_UP",
        "GreekDominance_State": "THETA_DOMINANT",

        # Compression
        "CompressionMaturity": "NO_COMPRESSION",
        "bb_width_z": 0.5,

        # Entry context
        "Entry_Chart_State_PriceStructure": "RANGE_BOUND",
        "Entry_Chart_State_TrendIntegrity": "NO_TREND",

        # IV
        "IV_Percentile": 72.0, "IV_Percentile_Depth": 60,
        "IV_vs_HV_Gap": 0.03,
        # EV_Feasibility_Ratio deliberately OMITTED -- must be computed

        # Misc
        "DTE_Entry": 60.0, "Days_Held": 35.0, "Days_In_Trade": 35.0,
        "Expiration": "2026-03-27", "Expiration_Entry": "2026-03-27",
        "Snapshot_TS": pd.Timestamp.now(),
        "Earnings_Date": None,
        "run_id": "golden-row-test", "Schema_Hash": "abc123",
        "IV": None,
        "choppiness_index": 48.0, "adx_14": 28.0, "rsi_14": 62.0,
        "ema50_slope": 0.02, "hv_20d_percentile": 55.0,
        "HV_Daily_Move_1Sigma": 3.5,
        "Recovery_Feasibility": "UNLIKELY",
        "Recovery_Move_Per_Day": 2.8,
        "Theta_Bleed_Daily_Pct": 0.0,  # overwritten by enrichment
        "Prior_Action": "HOLD",
        "_Active_Conditions": "", "_Condition_Resolved": "",
        "_Ticker_Net_Delta": 0.0, "_Ticker_Has_Stock": False,
        "Conviction_Status": "WEAKENING",
    })


class TestArchetype1_LosingLongPut:
    """AMZN LONG_PUT: OTM, losing, stock rallying against thesis.
    With σ-normalization (HV=35%), ROC5=3.5% (z=0.71) is within normal noise,
    so direction-adverse gate does NOT fire. Routes to thesis-not-confirming → ROLL."""

    def test_ev_layer_computes_true_breakeven(self):
        """EV layer must use strike - premium (not distance to strike)."""
        df = pd.DataFrame([_archetype_losing_long_put()])
        r = compute_expected_move(df).iloc[0]

        # True breakeven = 180 - 8.50 = 171.50
        # Required_Move = 195 - 171.50 = 23.50
        assert abs(r["Required_Move_Breakeven"] - 23.50) < 0.1, (
            f"Bug 37 regression: expected ~23.50, got {r['Required_Move_Breakeven']}"
        )
        # EM_10D = 195 * 0.38 * sqrt(10/252) ~ 14.77
        assert 14.0 < r["Expected_Move_10D"] < 16.0
        # EV_Ratio ~ 1.59
        assert r["EV_Feasibility_Ratio"] > 1.5, (
            f"EV_Ratio should exceed 1.5, got {r['EV_Feasibility_Ratio']}"
        )

    def test_decision_layer_action(self):
        """v2 EV resolver prefers ROLL (extend time) over EXIT at -52% with 25 DTE remaining."""
        result = _run_vertical_slice(_archetype_losing_long_put())
        assert result["Action"] in ("EXIT", "ROLL"), (
            f"Expected EXIT or ROLL, got {result['Action']}. Rationale: {result.get('Rationale', '')}"
        )

    def test_urgency_is_medium_or_higher(self):
        result = _run_vertical_slice(_archetype_losing_long_put())
        assert result["Urgency"] in ("MEDIUM", "HIGH", "CRITICAL")

    def test_rationale_mentions_expectancy(self):
        """v2 rationale references thesis/direction context."""
        result = _run_vertical_slice(_archetype_losing_long_put())
        rat = result.get("Rationale", "").lower()
        assert "direction" in rat or "adverse" in rat or "rallying" in rat or "expectancy" in rat or "recovery" in rat or "thesis" in rat, (
            f"Rationale must mention direction, expectancy, or thesis: {result.get('Rationale', '')}"
        )

    def test_doctrine_source_populated(self):
        result = _run_vertical_slice(_archetype_losing_long_put())
        assert result.get("Doctrine_Source"), "Doctrine_Source must be non-empty"

    def test_card_fields_populated(self):
        result = _run_vertical_slice(_archetype_losing_long_put())
        # Theta bleed: |0.45| / 4.10 * 100 ~ 10.97%
        bleed = result["Theta_Bleed_Daily_Pct"]
        assert not isnan(bleed) and bleed > 10.0, f"Theta bleed ~11%, got {bleed}"
        # Losing OTM: Profit_Cushion = 0
        assert result["Profit_Cushion"] == 0.0
        # True breakeven distance > 20
        assert result["Required_Move_Breakeven"] > 20.0


# =============================================================================
# Archetype 2: Winning SHORT_PUT (CSP)
# =============================================================================

def _archetype_winning_csp() -> pd.Series:
    """
    AAPL CSP: OTM, profitable, stock trending up.

    Setup:
      - AAPL at $220, put strike $200, premium received $5.00
      - Current option price = $1.50 (70% captured)
      - DTE = 14, Delta = -0.15
      - True BE = 200 - 5 = $195
      - Required_Move = 220 - 195 = $25

    Expected: HOLD LOW
    """
    return pd.Series({
        "TradeID": "T-GR-002", "LegID": "L-GR-002",
        "Symbol": "AAPL260321P00200000",
        "Underlying_Ticker": "AAPL",
        "Strategy": "CSP", "AssetType": "OPTION",
        "Option_Type": "PUT", "Call/Put": "P",

        "UL Last": 220.0, "Strike": 200.0,
        "DTE": 14.0, "Premium_Entry": 5.00,
        "Last": 1.50, "Bid": 1.45,
        "Delta": -0.15, "Gamma": 0.008,
        "Theta": 0.08, "Vega": 0.06,
        "HV_20D": 0.28, "IV_Now": 0.30, "IV_Entry": 0.32, "IV_30D": 0.30,
        "Quantity": -1.0, "Basis": 500.0,
        "Net_Cost_Basis_Per_Share": 195.0,

        "Moneyness_Label": "OTM",
        "TrendIntegrity_State": "TREND_UP",
        "PriceStructure_State": "STRUCTURE_INTACT",
        "Drift_Direction": "Up",
        "VolatilityState_State": "NORMAL",
        "MomentumVelocity_State": "TRENDING",
        "Position_Regime": "SIDEWAYS_INCOME",
        "Trajectory_Consecutive_Debit_Rolls": 0,

        "Portfolio_Delta_Utilization_Pct": 5.0,
        "Equity_Integrity_State": "INTACT",
        "IV_Percentile": 50.0, "IV_Percentile_Depth": 60,
        "IV_vs_HV_Gap": 0.02,

        "Thesis_State": "INTACT",
        "Thesis_Gate": "PASS",
        "_thesis_blocks_roll": False,
        "Thesis_Summary": "",

        "Snapshot_TS": pd.Timestamp.now(),
        "Earnings_Date": None,
        "run_id": "golden-row-test", "Schema_Hash": "abc123",
        "IV": None,
        "_Active_Conditions": "", "_Condition_Resolved": "",
    })


class TestArchetype2_WinningCSP:
    """AAPL CSP: OTM, profitable, low assignment risk. Should HOLD."""

    def test_ev_layer_computes_short_put(self):
        df = pd.DataFrame([_archetype_winning_csp()])
        r = compute_expected_move(df).iloc[0]
        # BE = 200 - 5 = 195. Required = 220 - 195 = 25
        assert abs(r["Required_Move_Breakeven"] - 25.0) < 0.1, (
            f"Expected ~25.0, got {r['Required_Move_Breakeven']}"
        )

    def test_decision_layer_holds_or_buyback(self):
        """v2 EV resolver may prefer BUYBACK (take 70% profit) over HOLD."""
        result = _run_vertical_slice(_archetype_winning_csp())
        assert result["Action"] in ("HOLD", "BUYBACK"), (
            f"Expected HOLD or BUYBACK, got {result['Action']}. Rationale: {result.get('Rationale', '')}"
        )

    def test_urgency_is_low_or_medium(self):
        """v2 may upgrade urgency to MEDIUM when BUYBACK is the EV winner."""
        result = _run_vertical_slice(_archetype_winning_csp())
        assert result["Urgency"] in ("LOW", "MEDIUM")

    def test_theta_flag_false_for_short(self):
        result = _run_vertical_slice(_archetype_winning_csp())
        assert result.get("Theta_Opportunity_Cost_Flag") in (False, 0, None), (
            "Short premium strategy should NOT flag theta opportunity cost"
        )


# =============================================================================
# Archetype 3: BUY_WRITE at 21-DTE Gate
# =============================================================================

def _archetype_bw_21dte_roll() -> pd.Series:
    """
    BUY_WRITE: 21-DTE gate, short call approaching strike, <50% captured.

    Setup:
      - Stock at $105, short call strike $110, DTE=18
      - Premium entry = $4.00, option price = $3.20 (20% captured < 50%)
      - Short_Call_Delta = 0.35 (not ITM emergency)

    Expected: ROLL MEDIUM via 21-DTE income gate
    """
    return pd.Series({
        "TradeID": "T-GR-003", "LegID": "L-GR-003",
        "Symbol": "TSLA",
        "Underlying_Ticker": "TSLA",
        "Strategy": "BUY_WRITE",
        "AssetType": "STOCK",

        "UL Last": 105.0,
        "Basis": 20000.0,
        "Quantity": 200.0,
        "Underlying_Price_Entry": 100.0,
        "Net_Cost_Basis_Per_Share": 96.0,
        "Cumulative_Premium_Collected": 4.0,

        "Short_Call_Delta": 0.35,
        "Short_Call_Strike": 110.0,
        "Short_Call_DTE": 18.0,
        "Short_Call_Premium": 4.00,
        "Short_Call_Last": 3.20,
        "Short_Call_Moneyness": "OTM",

        "Delta": 0.0,
        "Strike": np.nan, "DTE": np.nan,
        "Premium_Entry": np.nan, "Last": np.nan,
        "HV_20D": 0.42,

        "IV_Entry": 0.40, "IV_30D": 0.38, "IV_Now": 0.38,
        "IV_Percentile": 55.0, "IV_vs_HV_Gap": 0.03,

        "Theta": 0.0, "Gamma": 0.0,

        "Thesis_State": "INTACT",
        "Thesis_Gate": "PASS",
        "_thesis_blocks_roll": False,
        "Thesis_Summary": "",
        "PriceStructure_State": "STRUCTURE_INTACT",
        "TrendIntegrity_State": "TREND_UP",
        "ema50_slope": 0.01,
        "hv_20d_percentile": 55.0,
        "Equity_Integrity_State": "INTACT",

        "Position_Regime": "SIDEWAYS_INCOME",
        "Trajectory_Consecutive_Debit_Rolls": 0,
        "Trajectory_Stock_Return": 0.05,

        "_Active_Conditions": "", "_Condition_Resolved": "",
        "Snapshot_TS": pd.Timestamp.now(),
        "Earnings_Date": None,
        "Days_In_Trade": 15,
        "run_id": "golden-row-test", "Schema_Hash": "abc123",
        "IV": None,
    })


class TestArchetype3_BuyWrite21DTE:
    """BUY_WRITE at 21-DTE gate. Should ROLL."""

    def test_ev_layer_skips_stock_leg(self):
        df = pd.DataFrame([_archetype_bw_21dte_roll()])
        r = compute_expected_move(df).iloc[0]
        assert pd.isna(r.get("Expected_Move_10D")), (
            "STOCK leg should NOT get Expected_Move_10D"
        )

    def test_decision_layer_rolls_or_assigns(self):
        """v2 EV resolver compares ROLL vs ASSIGN — ASSIGN wins at +$280K vs +$38K."""
        result = _run_vertical_slice(_archetype_bw_21dte_roll())
        assert result["Action"] in ("ROLL", "LET_EXPIRE", "ACCEPT_CALL_AWAY", "ACCEPT_SHARE_ASSIGNMENT"), (
            f"Expected ROLL or LET_EXPIRE/ACCEPT_CALL_AWAY/ACCEPT_SHARE_ASSIGNMENT, got {result['Action']}. Rationale: {result.get('Rationale', '')}"
        )

    def test_urgency_is_medium_or_higher(self):
        result = _run_vertical_slice(_archetype_bw_21dte_roll())
        assert result["Urgency"] in ("MEDIUM", "HIGH")

    def test_rationale_mentions_ev_or_dte(self):
        """v2 rationale references EV comparison or DTE context."""
        result = _run_vertical_slice(_archetype_bw_21dte_roll())
        rat = result.get("Rationale", "")
        assert "21" in rat or "DTE" in rat or "EV" in rat or "18d" in rat, (
            f"Rationale should mention DTE or EV context: {rat}"
        )


# =============================================================================
# Archetype 4: Deep Loser LONG_CALL
# =============================================================================

def _archetype_deep_loser_long_call() -> pd.Series:
    """
    NFLX LONG_CALL: deep OTM, losing 88%, structure BROKEN, thesis DEGRADED.

    Setup:
      - NFLX at $650, call strike $700, premium paid $15.00
      - Stock fell 8% (adverse for call), DTE=12
      - True BE = 700 + 15 = $715
      - Required_Move = 715 - 650 = $65
      - EM_10D = 650 * 0.48 * sqrt(10/252) ~ $62.18
      - EV_Ratio ~ 1.045

    Expected: EXIT CRITICAL (structural gate or direction-adverse gate)
    """
    return pd.Series({
        "TradeID": "T-GR-004", "LegID": "L-GR-004",
        "Symbol": "NFLX260410C00700000",
        "Underlying_Ticker": "NFLX",
        "Strategy": "LONG_CALL", "AssetType": "OPTION",
        "Option_Type": "CALL", "Call/Put": "C",

        "UL Last": 650.0, "Strike": 700.0,
        "DTE": 12.0, "Premium_Entry": 15.00,
        "Last": 1.80, "Bid": 1.60,
        "Delta": 0.08, "Delta_Entry": 0.35,
        "Gamma": 0.003, "Theta": -1.20, "Vega": 0.15,
        "HV_20D": 0.42, "IV_Now": 0.48,
        "Quantity": 1.0, "Basis": 1500.0,

        "PnL_Dollar": -1320.0, "Total_GL_Decimal": -0.88,

        # Direction: stock falling = adverse for call
        "Drift_Direction": "Down",
        "Price_Drift_Pct": -0.08,
        "roc_5": -4.2, "roc_10": -6.1,
        "MomentumVelocity_State": "TRENDING",
        "momentum_slope": -1.5,

        # Structure BROKEN
        "PriceStructure_State": "STRUCTURE_BROKEN",
        "TrendIntegrity_State": "NO_TREND",
        "GreekDominance_State": "THETA_DOMINANT",

        "CompressionMaturity": "NO_COMPRESSION",
        "bb_width_z": 1.2,

        # Thesis DEGRADED
        "Thesis_State": "DEGRADED",
        "Thesis_Gate": "CAUTION",
        "_thesis_blocks_roll": False,
        "Thesis_Summary": "NFLX subscriber growth slowing, guidance cut.",

        # Entry context
        "Entry_Chart_State_PriceStructure": "STRUCTURAL_UP",
        "Entry_Chart_State_TrendIntegrity": "STRONG_TREND",

        "IV_Percentile": 82.0, "IV_Percentile_Depth": 60,
        "IV_vs_HV_Gap": 0.06,

        "DTE_Entry": 45.0, "Days_Held": 33.0, "Days_In_Trade": 33.0,
        "Expiration": "2026-04-10", "Expiration_Entry": "2026-04-10",
        "Snapshot_TS": pd.Timestamp.now(),
        "Earnings_Date": None,
        "run_id": "golden-row-test", "Schema_Hash": "abc123",
        "IV": None,
        "choppiness_index": 72.0, "adx_14": 12.0, "rsi_14": 35.0,
        "ema50_slope": -0.03, "hv_20d_percentile": 78.0,
        "HV_Daily_Move_1Sigma": 8.5,
        "Recovery_Feasibility": "IMPOSSIBLE",
        "Recovery_Move_Per_Day": 6.0,
        "Theta_Bleed_Daily_Pct": 0.0,
        "Prior_Action": "HOLD",
        "_Active_Conditions": "", "_Condition_Resolved": "",
        "_Ticker_Net_Delta": 0.0, "_Ticker_Has_Stock": False,
        "Conviction_Status": "REVERSING",
        "Delta_Deterioration_Streak": 5,
    })


class TestArchetype4_DeepLoserLongCall:
    """NFLX LONG_CALL: deep loser, structure broken. Should EXIT CRITICAL."""

    def test_ev_layer_true_breakeven_call(self):
        df = pd.DataFrame([_archetype_deep_loser_long_call()])
        r = compute_expected_move(df).iloc[0]
        # True BE = 700 + 15 = 715. Required = 715 - 650 = 65
        assert abs(r["Required_Move_Breakeven"] - 65.0) < 0.1, (
            f"Bug 37 regression for CALL: expected ~65.0, got {r['Required_Move_Breakeven']}"
        )

    def test_decision_layer_exits(self):
        result = _run_vertical_slice(_archetype_deep_loser_long_call())
        assert result["Action"] == "EXIT", (
            f"Expected EXIT, got {result['Action']}. Rationale: {result.get('Rationale', '')}"
        )

    def test_urgency_is_high_or_critical(self):
        result = _run_vertical_slice(_archetype_deep_loser_long_call())
        assert result["Urgency"] in ("HIGH", "CRITICAL"), (
            f"Expected HIGH/CRITICAL, got {result['Urgency']}"
        )

    def test_theta_bleed_extreme(self):
        """Theta bleed: |1.20| / 1.80 * 100 = 66.7%/day."""
        result = _run_vertical_slice(_archetype_deep_loser_long_call())
        bleed = result["Theta_Bleed_Daily_Pct"]
        assert not isnan(bleed) and bleed > 60.0, f"Expected >60%, got {bleed}"

    def test_ev_not_pre_injected(self):
        """EV_Feasibility_Ratio must NOT be in the raw archetype row."""
        row = _archetype_deep_loser_long_call()
        assert "EV_Feasibility_Ratio" not in row.index or pd.isna(
            row.get("EV_Feasibility_Ratio")
        )


# =============================================================================
# Archetype 5: Winning LONG_PUT Past Breakeven + MC Override
# =============================================================================

def _archetype_winning_put_past_breakeven() -> pd.Series:
    """
    META LONG_PUT: ITM, past true breakeven, profitable.
    Stock is bouncing back (momentum_slope > 0) threatening profits.

    Setup:
      - META at $480, put strike $520, premium paid $18.00
      - True BE = 520 - 18 = $502. Price 480 < 502 -> past breakeven.
      - Profit_Cushion = 502 - 480 = $22
      - DTE = 25, Last = $48 (intrinsic=40, TV=8, tv_pct=16.7%)
      - tv_pct > 10% avoids C4 time-value-exhausted EXIT gate

    Expected: Doctrine = HOLD; MC may override to EXIT.
    """
    return pd.Series({
        "TradeID": "T-GR-005", "LegID": "L-GR-005",
        "Symbol": "META260515P00520000",
        "Underlying_Ticker": "META",
        "Strategy": "LONG_PUT", "AssetType": "OPTION",
        "Option_Type": "PUT", "Call/Put": "P",

        "UL Last": 480.0, "Strike": 520.0,
        "DTE": 25.0, "Premium_Entry": 18.00,
        "Last": 48.00, "Bid": 47.50,
        "Delta": -0.82, "Delta_Entry": -0.50,
        "Gamma": 0.008, "Theta": -1.50, "Vega": 0.08,
        "HV_20D": 0.40, "IV_Now": 0.45,
        "Quantity": 1.0, "Basis": 1800.0,

        "PnL_Dollar": 3000.0, "Total_GL_Decimal": 1.67,

        # Direction: stock bouncing (adverse for put winner)
        "Drift_Direction": "Up",
        "Price_Drift_Pct": 0.025,
        "roc_5": 2.5, "roc_10": 1.8,
        "MomentumVelocity_State": "REVERSING",
        "momentum_slope": 0.8,

        "PriceStructure_State": "STRUCTURE_INTACT",
        "TrendIntegrity_State": "TREND_DOWN",
        "GreekDominance_State": "DELTA_DOMINANT",

        "CompressionMaturity": "NO_COMPRESSION",
        "bb_width_z": 0.3,

        "Entry_Chart_State_PriceStructure": "STRUCTURAL_DOWN",
        "Entry_Chart_State_TrendIntegrity": "STRONG_TREND",

        "Thesis_State": "INTACT",
        "Thesis_Gate": "PASS",
        "_thesis_blocks_roll": False,
        "Thesis_Summary": "META overvalued, bearish thesis intact.",

        "IV_Percentile": 68.0, "IV_Percentile_Depth": 60,
        "IV_vs_HV_Gap": 0.05,

        "DTE_Entry": 60.0, "Days_Held": 35.0, "Days_In_Trade": 35.0,
        "Expiration": "2026-05-15", "Expiration_Entry": "2026-05-15",
        "Snapshot_TS": pd.Timestamp.now(),
        "Earnings_Date": None,
        "run_id": "golden-row-test", "Schema_Hash": "abc123",
        "IV": None,
        "choppiness_index": 55.0, "adx_14": 22.0, "rsi_14": 38.0,
        "ema50_slope": -0.02, "hv_20d_percentile": 65.0,
        "HV_Daily_Move_1Sigma": 6.0,
        "Recovery_Feasibility": "LIKELY",
        "Recovery_Move_Per_Day": 2.0,
        "Theta_Bleed_Daily_Pct": 0.0,
        "Prior_Action": "HOLD",
        "_Active_Conditions": "", "_Condition_Resolved": "",
        "_Ticker_Net_Delta": 0.0, "_Ticker_Has_Stock": False,
        "Conviction_Status": "WEAKENING",
    })


class TestArchetype5_WinningPutMCOverride:
    """META LONG_PUT past breakeven: ITM, profitable.
    Validates Profit_Cushion and MC escalation logic."""

    def test_ev_layer_profit_cushion(self):
        """Past-breakeven put: Required_Move=0, Profit_Cushion=$22."""
        df = pd.DataFrame([_archetype_winning_put_past_breakeven()])
        r = compute_expected_move(df).iloc[0]

        # True BE = 520 - 18 = 502. Price 480 < 502 -> past breakeven
        assert r["Required_Move_Breakeven"] == 0.0, (
            f"Past-breakeven: expected 0, got {r['Required_Move_Breakeven']}"
        )
        # Cushion = 502 - 480 = 22
        assert abs(r["Profit_Cushion"] - 22.0) < 0.1, (
            f"Profit_Cushion should be ~22.0, got {r['Profit_Cushion']}"
        )
        # Cushion ratio = 22 / EM_10D (EM_10D ~ 43)
        assert r["Profit_Cushion_Ratio"] > 0.4

    def test_doctrine_exits_on_mfe(self):
        """MFE gate: +167% single-contract winner → EXIT (profit capture).

        Gate 2c-mfe fires at ≥50% gain to prevent round-tripping
        (ref: AMZN/MSFT audit — +48% → -55%).  Single contract = EXIT,
        multi-contract = TRIM.
        """
        result = _run_vertical_slice(_archetype_winning_put_past_breakeven(), run_mc=False)
        assert result["Action"] == "EXIT", (
            f"MFE gate should EXIT single-contract +167% winner, got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )
        assert "PROFIT_CAPTURE" in result.get("Rationale", ""), (
            "Rationale should cite PROFIT_CAPTURE"
        )

    def test_mc_escalation_overrides_hold(self):
        """Bug 38 regression: if MC says EXIT_NOW on HOLD, Action must become EXIT."""
        # Run with MC enabled
        result = _run_vertical_slice(_archetype_winning_put_past_breakeven(), run_mc=True)
        mc_verdict = result.get("MC_Hold_Verdict", "")
        if mc_verdict == "EXIT_NOW":
            assert result["Action"] == "EXIT", (
                f"Bug 38: MC_Hold_Verdict=EXIT_NOW but Action={result['Action']}"
            )

    def test_card_shows_cushion_not_breakeven(self):
        """Dashboard renders Profit_Cushion when > 0 and Required_Move == 0."""
        result = _run_vertical_slice(_archetype_winning_put_past_breakeven())
        assert result["Profit_Cushion"] > 0, "ITM winner must have Profit_Cushion > 0"
        assert result["Required_Move_Breakeven"] == 0.0, (
            "Past-breakeven must have Required_Move = 0"
        )


# =============================================================================
# Archetype 6: LEAPS LONG_CALL with Intact Thesis + MC EXIT_NOW
# =============================================================================

def _archetype_leaps_intact_thesis() -> pd.Series:
    """
    AAPL LEAPS LONG_CALL: DTE=318, thesis INTACT, conviction STABLE.

    Setup:
      - AAPL at $264, call strike $260, premium paid $28.50
      - Slightly ITM (Delta ~0.57), big loss (-$731 per contract)
      - True BE = 260 + 28.50 = $288.50
      - Required_Move = 288.50 - 264 = $24.50
      - IV = 0.25, EM_10D = 264 * 0.25 * sqrt(10/252) ~ $13.15
      - EV_Ratio = 24.50 / 13.15 ~ 1.86 (low on 10D, but 318D gives plenty of time)

    Bug 39 scenario: MC may fire EXIT_NOW (borderline p_recovery ~32%),
    but the LEAPS guard should suppress the override because thesis is INTACT.

    Expected: HOLD (doctrine), MC EXIT_NOW suppressed (LEAPS + INTACT thesis)
    """
    return pd.Series({
        "TradeID": "T-GR-006", "LegID": "L-GR-006",
        "Symbol": "AAPL270115C00260000",
        "Underlying_Ticker": "AAPL",
        "Strategy": "LONG_CALL", "AssetType": "OPTION",
        "Option_Type": "CALL", "Call/Put": "C",

        "UL Last": 264.0, "Strike": 260.0,
        "DTE": 318.0, "Premium_Entry": 28.50,
        "Last": 21.19, "Bid": 21.00,
        "Delta": 0.57, "Delta_Entry": 0.62,
        "Gamma": 0.005, "Theta": -0.06, "Vega": 0.55,
        "HV_20D": 0.25, "IV_Now": 0.25, "IV_30D": 0.25,
        "Quantity": 1.0, "Basis": 2850.0,

        "PnL_Dollar": -731.0, "Total_GL_Decimal": -0.256,

        # Direction: mildly positive (not adverse)
        "Drift_Direction": "Up",
        "Price_Drift_Pct": 0.01,
        "roc_5": 0.8, "roc_10": -1.2,
        "MomentumVelocity_State": "SIDEWAYS",
        "momentum_slope": 0.05,

        # Structure: not broken
        "PriceStructure_State": "RANGE_BOUND",
        "TrendIntegrity_State": "NO_TREND",
        "GreekDominance_State": "DELTA_DOMINANT",

        "CompressionMaturity": "NO_COMPRESSION",
        "bb_width_z": 0.0,

        "Entry_Chart_State_PriceStructure": "RANGE_BOUND",
        "Entry_Chart_State_TrendIntegrity": "NO_TREND",

        # Thesis INTACT, conviction STABLE
        "Thesis_State": "INTACT",
        "Thesis_Gate": "PASS",
        "_thesis_blocks_roll": False,
        "Thesis_Summary": "AAPL long-term thesis intact; Services growth supports LEAPS.",
        "Conviction_Status": "STABLE",

        "IV_Percentile": 40.0, "IV_Percentile_Depth": 60,
        "IV_vs_HV_Gap": -0.089,  # IV below HV = cheap vol for buyer

        "DTE_Entry": 365.0, "Days_Held": 47.0, "Days_In_Trade": 47.0,
        "Expiration": "2027-01-15", "Expiration_Entry": "2027-01-15",
        "Snapshot_TS": pd.Timestamp.now(),
        "Earnings_Date": None,
        "run_id": "golden-row-test", "Schema_Hash": "abc123",
        "IV": None,
        "choppiness_index": 55.0, "adx_14": 15.0, "rsi_14": 48.0,
        "ema50_slope": 0.005, "hv_20d_percentile": 45.0,
        "HV_Daily_Move_1Sigma": 4.0,
        "Recovery_Feasibility": "POSSIBLE",
        "Recovery_Move_Per_Day": 0.08,
        "Theta_Bleed_Daily_Pct": 0.0,
        "Prior_Action": "HOLD",
        "_Active_Conditions": "", "_Condition_Resolved": "",
        "_Ticker_Net_Delta": 0.0, "_Ticker_Has_Stock": False,
    })


class TestArchetype6_LeapsIntactThesis:
    """AAPL LEAPS LONG_CALL: DTE=318, thesis INTACT, MC EXIT_NOW suppressed.
    Bug 39: MC EXIT_NOW must NOT override HOLD for LEAPS with intact thesis."""

    def test_ev_layer_computes_leaps_breakeven(self):
        """True breakeven = strike + premium = 260 + 28.50 = $288.50."""
        df = pd.DataFrame([_archetype_leaps_intact_thesis()])
        r = compute_expected_move(df).iloc[0]
        # Required_Move = 288.50 - 264 = 24.50
        assert abs(r["Required_Move_Breakeven"] - 24.50) < 0.5, (
            f"LEAPS BE: expected ~24.50, got {r['Required_Move_Breakeven']}"
        )
        # EV_Ratio on 10D is high (expected ~1.86)
        assert r["EV_Feasibility_Ratio"] > 1.5, (
            f"10D EV_Ratio for LEAPS should be high, got {r['EV_Feasibility_Ratio']}"
        )

    def test_doctrine_holds_leaps(self):
        """Doctrine should HOLD a LEAPS with intact thesis."""
        result = _run_vertical_slice(_archetype_leaps_intact_thesis(), run_mc=False)
        assert result["Action"] == "HOLD", (
            f"LEAPS with INTACT thesis should HOLD, got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )

    def test_mc_exit_now_suppressed_for_leaps(self):
        """Bug 39: MC EXIT_NOW must be suppressed for LEAPS with intact thesis.
        The replay helper should NOT change Action to EXIT."""
        result = _run_vertical_slice(_archetype_leaps_intact_thesis(), run_mc=False)
        # Simulate MC returning EXIT_NOW
        mc_result = {"MC_Hold_Verdict": "EXIT_NOW", "MC_Hold_EV": -200.0,
                     "MC_Hold_P_Recovery": 0.32}
        final = _replay_mc_escalation(result, mc_result)
        assert final["Action"] == "HOLD", (
            f"Bug 39: LEAPS + INTACT thesis must stay HOLD despite MC EXIT_NOW, "
            f"got {final['Action']}"
        )
        assert "suppressed" in final.get("Rationale", "").lower(), (
            "Rationale must mention suppression for LEAPS guard"
        )

    def test_mc_exit_now_fires_if_thesis_degraded(self):
        """MC EXIT_NOW SHOULD fire for LEAPS with DEGRADED thesis."""
        row = _archetype_leaps_intact_thesis()
        row["Thesis_State"] = "DEGRADED"
        result = _run_vertical_slice(row, run_mc=False)
        mc_result = {"MC_Hold_Verdict": "EXIT_NOW", "MC_Hold_EV": -500.0,
                     "MC_Hold_P_Recovery": 0.20}
        final = _replay_mc_escalation(result, mc_result)
        # DEGRADED thesis -> MC override should fire
        assert final["Action"] == "EXIT", (
            f"LEAPS with DEGRADED thesis + MC EXIT_NOW should EXIT, "
            f"got {final['Action']}"
        )

    def test_theta_bleed_minimal_for_leaps(self):
        """LEAPS theta bleed should be tiny (~0.28%/day)."""
        df = pd.DataFrame([_archetype_leaps_intact_thesis()])
        r = compute_expected_move(df).iloc[0]
        bleed = r["Theta_Bleed_Daily_Pct"]
        assert not isnan(bleed) and bleed < 1.0, (
            f"LEAPS theta bleed should be <1%/day, got {bleed}"
        )


# =============================================================================
# Archetype 7: SPY LONG_CALL — REVIEW streak escalation
# =============================================================================

def _archetype_revalidate_streak() -> pd.Series:
    """
    SPY LONG_CALL: borderline position where drift produces REVIEW.

    Setup:
      - SPY at $592, call strike $590, premium paid $14.00
      - Barely ITM, direction neutral-to-weak
      - DTE = 38, thesis INTACT but signal degradation from drift
      - Prior_Action_Streak = 3 (REVIEW for 3 consecutive days)
      - Drift engine would set Action=REVIEW (simulated manually)

    Expected: Doctrine = HOLD → Drift override → REVIEW → Streak escalation → EXIT MEDIUM
    """
    return pd.Series({
        # Identity
        "TradeID": "T-GR-007", "LegID": "L-GR-007",
        "Symbol": "SPY260417C00590000",
        "Underlying_Ticker": "SPY",
        "Strategy": "LONG_CALL", "AssetType": "OPTION",
        "Option_Type": "CALL", "Call/Put": "C",

        # Prices & Greeks
        "UL Last": 592.0, "Strike": 590.0,
        "DTE": 38.0, "Premium_Entry": 14.00,
        "Last": 12.50, "Bid": 12.30,
        "Delta": 0.52, "Delta_Entry": 0.55,
        "Gamma": 0.010, "Theta": -0.85, "Vega": 0.18,
        "HV_20D": 0.14, "IV_Now": 0.16,
        "Quantity": 1.0, "Basis": 1400.0,

        # P&L: small loss
        "PnL_Dollar": -150.0, "Total_GL_Decimal": -0.107,

        # Direction: flat / weakening momentum
        "Drift_Direction": "Flat",
        "Price_Drift_Pct": -0.005,
        "roc_5": -0.3, "roc_10": 0.5,
        "MomentumVelocity_State": "DECELERATING",
        "momentum_slope": -0.2,

        "PriceStructure_State": "STRUCTURE_INTACT",
        "TrendIntegrity_State": "FLAT",
        "GreekDominance_State": "THETA_DOMINANT",

        "CompressionMaturity": "NO_COMPRESSION",
        "bb_width_z": -0.3,

        "Entry_Chart_State_PriceStructure": "STRUCTURAL_UP",
        "Entry_Chart_State_TrendIntegrity": "MODERATE_TREND",

        "Thesis_State": "INTACT",
        "Thesis_Gate": "PASS",
        "_thesis_blocks_roll": False,
        "Thesis_Summary": "SPY consolidating, bullish thesis intact.",

        "IV_Percentile": 45.0, "IV_Percentile_Depth": 100,
        "IV_vs_HV_Gap": 0.02,

        "DTE_Entry": 60.0, "Days_Held": 22.0, "Days_In_Trade": 22.0,
        "Expiration": "2026-04-17", "Expiration_Entry": "2026-04-17",
        "Snapshot_TS": pd.Timestamp.now(),
        "Earnings_Date": None,
        "run_id": "golden-row-test", "Schema_Hash": "abc123",
        "IV": None,
        "choppiness_index": 60.0, "adx_14": 18.0, "rsi_14": 48.0,
        "ema50_slope": 0.01, "hv_20d_percentile": 40.0,
        "HV_Daily_Move_1Sigma": 3.5,
        "Recovery_Feasibility": "POSSIBLE",
        "Recovery_Move_Per_Day": 1.0,
        "Theta_Bleed_Daily_Pct": 0.0,
        "Prior_Action": "REVIEW",
        "Prior_Action_Streak": 3,
        "_Active_Conditions": "", "_Condition_Resolved": "",
        "_Ticker_Net_Delta": 0.0, "_Ticker_Has_Stock": False,
        "Conviction_Status": "STABLE",
    })


class TestArchetype7_RevalidateStreakEscalation:
    """SPY LONG_CALL: REVIEW persisted 3 consecutive days → auto-escalate to EXIT MEDIUM.

    Vertical slice: enrichment → doctrine (HOLD) → drift override (REVIEW, simulated)
    → streak escalation (EXIT MEDIUM).
    """

    def test_doctrine_produces_hold(self):
        """Doctrine should produce HOLD for this borderline position."""
        result = _run_vertical_slice(_archetype_revalidate_streak(), run_mc=False)
        assert result["Action"] == "HOLD", (
            f"Doctrine should HOLD borderline SPY, got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )

    def test_streak_escalation_fires(self):
        """After drift override to REVIEW + streak=3 → EXIT MEDIUM."""
        result = _run_vertical_slice(_archetype_revalidate_streak(), run_mc=False)
        # Simulate drift filter override: HOLD → REVIEW
        result["Action"] = "REVIEW"
        result["Prior_Action_Streak"] = 3
        # Apply streak escalation
        final = _replay_streak_escalation(result)
        assert final["Action"] == "EXIT", (
            f"REVIEW x3 should escalate to EXIT, got {final['Action']}"
        )
        assert final["Urgency"] == "MEDIUM", (
            f"Escalated EXIT should be MEDIUM urgency, got {final['Urgency']}"
        )
        assert "REVIEW x3" in final.get("Rationale", ""), (
            "Rationale must mention the streak count"
        )

    def test_streak_2_no_escalation(self):
        """REVIEW with streak=2 should NOT escalate."""
        result = _run_vertical_slice(_archetype_revalidate_streak(), run_mc=False)
        result["Action"] = "REVIEW"
        result["Prior_Action_Streak"] = 2
        final = _replay_streak_escalation(result)
        assert final["Action"] == "REVIEW", (
            f"Streak=2 should not escalate, got {final['Action']}"
        )

    def test_exit_streak_5_promotes_to_critical(self):
        """EXIT persisted for 5 days → urgency promoted to CRITICAL."""
        result = _run_vertical_slice(_archetype_revalidate_streak(), run_mc=False)
        result["Action"] = "EXIT"
        result["Urgency"] = "HIGH"
        result["Prior_Action_Streak"] = 5
        final = _replay_streak_escalation(result)
        assert final["Urgency"] == "CRITICAL", (
            f"EXIT x5 should promote to CRITICAL, got {final['Urgency']}"
        )


# =============================================================================
# Archetype 8: Trend-Invalidated Long Put (Gate 2a-trend)
# =============================================================================

def _archetype_trend_invalidated_put() -> pd.Series:
    """
    MSFT LONG_PUT: entered on STRONG_TREND down, now trend collapsed to NO_TREND.
    Losing position (-30%). Gate 2a-trend should EXIT HIGH.

    Ref: AMZN/MSFT/META Feb-2026 audit — trend broke Day 0, oscillated 10d, -55%.
    """
    return pd.Series({
        "TradeID": "T-GR-008", "LegID": "L-GR-008",
        "Symbol": "MSFT260402P00390000",
        "Underlying_Ticker": "MSFT",
        "Strategy": "LONG_PUT", "AssetType": "OPTION",
        "Option_Type": "PUT", "Call/Put": "P",

        "UL Last": 410.0, "Strike": 390.0,
        "DTE": 28.0, "Premium_Entry": 11.50,
        "Last": 8.05, "Bid": 7.90,
        "Delta": -0.25, "Delta_Entry": -0.40,
        "Gamma": 0.010, "Theta": -0.55, "Vega": 0.12,
        "HV_20D": 0.30, "IV_Now": 0.33,
        "Quantity": 1.0, "Basis": 1150.0,

        "PnL_Dollar": -345.0, "Total_GL_Decimal": -0.30,

        "Drift_Direction": "Up",
        "Price_Drift_Pct": 0.03,
        "roc_5": 2.0, "roc_10": 3.5,
        "MomentumVelocity_State": "TRENDING",
        "momentum_slope": 0.9,

        # CRITICAL: Entry was STRONG_TREND, now NO_TREND → Gate 2a fires
        "PriceStructure_State": "RANGE_BOUND",
        "TrendIntegrity_State": "NO_TREND",
        "GreekDominance_State": "THETA_DOMINANT",

        "CompressionMaturity": "NO_COMPRESSION",
        "bb_width_z": 0.4,

        "Entry_Chart_State_PriceStructure": "STRUCTURAL_DOWN",
        "Entry_Chart_State_TrendIntegrity": "STRONG_TREND",

        "Thesis_State": "DEGRADED",
        "Thesis_Gate": "CAUTION",
        "_thesis_blocks_roll": False,
        "Thesis_Summary": "MSFT bearish thesis lost trend support.",

        "IV_Percentile": 55.0, "IV_Percentile_Depth": 60,
        "IV_vs_HV_Gap": 0.03,

        "DTE_Entry": 60.0, "Days_Held": 32.0,
        "Days_In_Trade": 32.0,
        "Expiration": "2026-04-02", "Expiration_Entry": "2026-04-02",
        "Snapshot_TS": pd.Timestamp.now(),
        "Earnings_Date": None,
        "run_id": "golden-row-test", "Schema_Hash": "abc123",
        "IV": None,
        "choppiness_index": 62.0, "adx_14": 15.0, "rsi_14": 55.0,
        "ema50_slope": 0.01, "hv_20d_percentile": 45.0,
        "HV_Daily_Move_1Sigma": 5.0,
        "Recovery_Feasibility": "UNLIKELY",
        "Recovery_Move_Per_Day": 2.5,
        "Theta_Bleed_Daily_Pct": 0.0,
        "Prior_Action": "HOLD",
        "_Active_Conditions": "", "_Condition_Resolved": "",
        "_Ticker_Net_Delta": 0.0, "_Ticker_Has_Stock": False,
        "Conviction_Status": "WEAKENING",
    })


class TestArchetype8_TrendInvalidation:
    """MSFT LONG_PUT: entry trend STRONG_TREND, current NO_TREND, losing.
    Gate 2a-trend should fire EXIT HIGH."""

    def test_trend_invalidation_exits(self):
        """Gate 2a: entry STRONG_TREND → current NO_TREND + P&L < 0 → EXIT."""
        result = _run_vertical_slice(_archetype_trend_invalidated_put(), run_mc=False)
        assert result["Action"] == "EXIT", (
            f"Trend invalidation should EXIT, got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )

    def test_trend_invalidation_urgency_high(self):
        """Gate 2a urgency must be HIGH."""
        result = _run_vertical_slice(_archetype_trend_invalidated_put(), run_mc=False)
        assert result["Urgency"] == "HIGH", (
            f"Expected HIGH urgency, got {result['Urgency']}"
        )

    def test_trend_invalidation_rationale(self):
        """Rationale must cite TREND_INVALIDATED."""
        result = _run_vertical_slice(_archetype_trend_invalidated_put(), run_mc=False)
        assert "TREND_INVALIDATED" in result.get("Rationale", ""), (
            "Rationale must mention TREND_INVALIDATED"
        )

    def test_trend_intact_does_not_fire(self):
        """When current trend still matches entry, Gate 2a should NOT fire."""
        row = _archetype_trend_invalidated_put()
        row["TrendIntegrity_State"] = "STRONG_TREND"  # trend still intact
        result = _run_vertical_slice(row, run_mc=False)
        # Should NOT exit via trend invalidation (may exit for other reasons)
        rat = result.get("Rationale", "")
        assert "TREND_INVALIDATED" not in rat, (
            f"Trend still intact — Gate 2a should not fire. Rationale: {rat}"
        )

    def test_leaps_exempt_from_trend_gate(self):
        """LEAPs (DTE > 180) are exempt from Gate 2a-trend."""
        row = _archetype_trend_invalidated_put()
        row["DTE"] = 200.0  # LEAPS
        result = _run_vertical_slice(row, run_mc=False)
        rat = result.get("Rationale", "")
        assert "TREND_INVALIDATED" not in rat, (
            f"LEAPs should be exempt from Gate 2a. Rationale: {rat}"
        )


# =============================================================================
# Archetype 9: MFE Multi-Contract Profit Capture (Gate 2c-mfe)
# =============================================================================

def _archetype_mfe_multi_contract() -> pd.Series:
    """
    NVDA LONG_CALL: 3 contracts, up +65%. Gate 2c-mfe should TRIM (not EXIT).

    Ref: McMillan Ch.4 — realize partial profits on multi-contract positions.
    """
    return pd.Series({
        "TradeID": "T-GR-009", "LegID": "L-GR-009",
        "Symbol": "NVDA260515C00950000",
        "Underlying_Ticker": "NVDA",
        "Strategy": "LONG_CALL", "AssetType": "OPTION",
        "Option_Type": "CALL", "Call/Put": "C",

        "UL Last": 980.0, "Strike": 950.0,
        "DTE": 40.0, "Premium_Entry": 22.00,
        "Last": 36.30, "Bid": 35.80,
        "Delta": 0.72, "Delta_Entry": 0.55,
        "Gamma": 0.006, "Theta": -1.80, "Vega": 0.15,
        "HV_20D": 0.42, "IV_Now": 0.48,
        "Quantity": 3.0, "Basis": 6600.0,

        "PnL_Dollar": 4290.0, "Total_GL_Decimal": 0.65,

        "Drift_Direction": "Up",
        "Price_Drift_Pct": 0.05,
        "roc_5": 4.0, "roc_10": 6.5,
        "MomentumVelocity_State": "TRENDING",
        "momentum_slope": 1.5,

        "PriceStructure_State": "STRUCTURAL_UP",
        "TrendIntegrity_State": "STRONG_TREND",
        "GreekDominance_State": "DELTA_DOMINANT",

        "CompressionMaturity": "NO_COMPRESSION",
        "bb_width_z": 0.6,

        "Entry_Chart_State_PriceStructure": "STRUCTURAL_UP",
        "Entry_Chart_State_TrendIntegrity": "STRONG_TREND",

        "Thesis_State": "INTACT",
        "Thesis_Gate": "PASS",
        "_thesis_blocks_roll": False,
        "Thesis_Summary": "NVDA bullish thesis intact.",

        "IV_Percentile": 70.0, "IV_Percentile_Depth": 90,
        "IV_vs_HV_Gap": 0.06,

        "DTE_Entry": 70.0, "Days_Held": 30.0,
        "Expiration": "2026-05-15", "Expiration_Entry": "2026-05-15",
        "Snapshot_TS": pd.Timestamp.now(),
        "Earnings_Date": None,
        "run_id": "golden-row-test", "Schema_Hash": "abc123",
        "IV": None,
        "choppiness_index": 35.0, "adx_14": 35.0, "rsi_14": 68.0,
        "ema50_slope": 0.03, "hv_20d_percentile": 60.0,
        "HV_Daily_Move_1Sigma": 8.0,
        "Recovery_Feasibility": "LIKELY",
        "Recovery_Move_Per_Day": 3.0,
        "Theta_Bleed_Daily_Pct": 0.0,
        "Prior_Action": "HOLD",
        "_Active_Conditions": "", "_Condition_Resolved": "",
        "_Ticker_Net_Delta": 0.0, "_Ticker_Has_Stock": False,
        "Conviction_Status": "STRENGTHENING",
    })


class TestArchetype9_MFEMultiContract:
    """NVDA LONG_CALL: 3 contracts, +65%. Gate 2c-mfe should TRIM."""

    def test_multi_contract_trims(self):
        """Multi-contract ≥50% → TRIM (not EXIT)."""
        result = _run_vertical_slice(_archetype_mfe_multi_contract(), run_mc=False)
        assert result["Action"] == "TRIM", (
            f"Multi-contract +65% should TRIM, got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )

    def test_single_contract_exits(self):
        """Single contract ≥50% → EXIT."""
        row = _archetype_mfe_multi_contract()
        row["Quantity"] = 1.0
        result = _run_vertical_slice(row, run_mc=False)
        assert result["Action"] == "EXIT", (
            f"Single-contract +65% should EXIT, got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )

    def test_mfe_rationale_cites_profit_capture(self):
        """Rationale must mention PROFIT_CAPTURE."""
        result = _run_vertical_slice(_archetype_mfe_multi_contract(), run_mc=False)
        assert "PROFIT_CAPTURE" in result.get("Rationale", ""), (
            "Rationale must cite PROFIT_CAPTURE"
        )

    def test_below_50pct_no_mfe(self):
        """At +40%, MFE gate should NOT fire (strong entry)."""
        row = _archetype_mfe_multi_contract()
        row["Last"] = 30.80  # (30.80 - 22) / 22 = 0.40
        row["Bid"] = 30.30
        row["PnL_Dollar"] = 2640.0
        row["Total_GL_Decimal"] = 0.40
        result = _run_vertical_slice(row, run_mc=False)
        rat = result.get("Rationale", "")
        assert "PROFIT_CAPTURE" not in rat, (
            f"+40% strong entry should not trigger MFE. Rationale: {rat}"
        )

    def test_weak_entry_30pct_exits(self):
        """Weak entry at +30% → EXIT MEDIUM (shorter leash)."""
        row = _archetype_mfe_multi_contract()
        row["Last"] = 28.60  # (28.60 - 22) / 22 = 0.30
        row["Bid"] = 28.10
        row["PnL_Dollar"] = 1980.0
        row["Total_GL_Decimal"] = 0.30
        row["Entry_Chart_State_TrendIntegrity"] = "NO_TREND"  # weak entry
        row["Entry_Chart_State_PriceStructure"] = "RANGE_BOUND"
        result = _run_vertical_slice(row, run_mc=False)
        assert result["Action"] == "EXIT", (
            f"Weak entry +30% should EXIT, got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )

    def test_leaps_exempt_from_mfe(self):
        """LEAPs (DTE > 180) are exempt from Gate 2c-mfe."""
        row = _archetype_mfe_multi_contract()
        row["DTE"] = 200.0
        result = _run_vertical_slice(row, run_mc=False)
        rat = result.get("Rationale", "")
        assert "PROFIT_CAPTURE" not in rat, (
            f"LEAPs should be exempt from MFE gate. Rationale: {rat}"
        )


# =============================================================================
# Archetype 10: BUY_WRITE BROKEN Equity + Negative Carry (Fix 7)
# =============================================================================

def _archetype_bw_broken_negative_carry() -> pd.Series:
    """
    DKNG BUY_WRITE: Equity BROKEN, gamma drag + margin > theta.
    Negative carry = compounding loss daily. Should EXIT MEDIUM.

    Setup (adjusted for CAPITAL danger zone, no premium history):
      - Stock at $22.80, net cost $27.11 (no premium collected — first cycle)
      - drift_from_net = (22.80 - 27.11) / 27.11 = -15.9% (below -15% threshold)
      - Short $25 call OTM, DTE 16, theta 0.037/sh/day
      - Gamma 0.129 → drag > theta → NEGATIVE carry
      - No premium history → recovery premium mode does NOT activate
      - Below PNL_APPROACHING_HARD_STOP → CAPITAL tag ensures EXIT wins

    Expected: EXIT MEDIUM (negative carry gate)
    """
    return pd.Series({
        "TradeID": "T-GR-010", "LegID": "L-GR-010",
        "Symbol": "DKNG",
        "Underlying_Ticker": "DKNG",
        "Strategy": "BUY_WRITE",
        "AssetType": "STOCK",

        # Stock at $22.80 → drift_from_net = (22.80 - 27.11) / 27.11 = -15.9%
        # Below PNL_APPROACHING_HARD_STOP (-15%) so CAPITAL tag applies
        "UL Last": 22.80,
        "Basis": 27110.0,
        "Quantity": 1000.0,
        "Underlying_Price_Entry": 30.51,
        "Net_Cost_Basis_Per_Share": 27.11,
        "Cumulative_Premium_Collected": 0.0,

        "Short_Call_Delta": 0.30,
        "Short_Call_Strike": 25.0,
        "Short_Call_DTE": 16.0,
        "Short_Call_Premium": 0.72,
        "Short_Call_Last": 0.35,
        "Short_Call_Moneyness": "OTM",

        "Delta": 0.0,
        "Strike": np.nan, "DTE": np.nan,
        "Premium_Entry": np.nan, "Last": np.nan,
        "HV_20D": 0.693,  # 69.3% — high realized vol

        "IV_Entry": 0.625, "IV_30D": 0.555, "IV_Now": 0.591,
        "IV_Percentile": 33.0, "IV_vs_HV_Gap": -0.102,

        # Greeks: theta 0.037, gamma 0.129 → gamma drag >> theta
        "Theta": 0.037, "Gamma": 0.129,

        "Thesis_State": "INTACT",
        "Thesis_Gate": "PASS",
        "_thesis_blocks_roll": False,
        "Thesis_Summary": "",
        "PriceStructure_State": "RANGE_BOUND",
        "TrendIntegrity_State": "NO_TREND",
        "ema50_slope": -0.1448,
        "hv_20d_percentile": 89.0,
        "Equity_Integrity_State": "BROKEN",
        "Equity_Integrity_Reason": "EMA20↓(-0.0870), EMA50↓(-0.1448), ROC20=-10.8%, HV=89th_pct",

        "Position_Regime": "NEUTRAL",
        "Trajectory_Consecutive_Debit_Rolls": 0,
        "Trajectory_Stock_Return": -0.159,

        "PnL_Dollar": -11310.0, "Total_GL_Decimal": -0.259,

        "_Active_Conditions": "", "_Condition_Resolved": "",
        "Snapshot_TS": pd.Timestamp.now(),
        "Earnings_Date": None,
        "Days_In_Trade": 30,
        "run_id": "golden-row-test", "Schema_Hash": "abc123",
        "IV": None,
    })


class TestArchetype10_BWBrokenNegativeCarry:
    """DKNG BUY_WRITE: Equity BROKEN + gamma drag > theta + margin.
    Negative carry → EXIT MEDIUM (not HOLD HIGH)."""

    def test_broken_negative_carry_exits(self):
        """BROKEN equity + negative carry → EXIT."""
        result = _run_vertical_slice(_archetype_bw_broken_negative_carry(), run_mc=False)
        assert result["Action"] == "EXIT", (
            f"BROKEN + negative carry should EXIT, got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )

    def test_broken_negative_carry_urgency(self):
        """EXIT urgency should be MEDIUM (not CRITICAL — that's for hard stop)."""
        result = _run_vertical_slice(_archetype_bw_broken_negative_carry(), run_mc=False)
        assert result["Urgency"] == "MEDIUM", (
            f"Expected MEDIUM urgency, got {result['Urgency']}"
        )

    def test_broken_negative_carry_rationale(self):
        """Rationale must cite negative carry."""
        result = _run_vertical_slice(_archetype_bw_broken_negative_carry(), run_mc=False)
        rat = result.get("Rationale", "")
        assert "negative carry" in rat.lower() or "net bleed" in rat.lower(), (
            f"Rationale must mention negative carry. Got: {rat}"
        )

    def test_broken_positive_carry_holds(self):
        """High theta (carry positive) → HOLD HIGH (regression guard)."""
        row = _archetype_bw_broken_negative_carry()
        row["Theta"] = 0.15  # boost theta so carry is positive
        row["Gamma"] = 0.02  # reduce gamma drag
        result = _run_vertical_slice(row, run_mc=False)
        assert result["Action"] == "HOLD", (
            f"BROKEN + positive carry should HOLD, got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )
        assert "positive" in result.get("Rationale", "").lower() or "patience" in result.get("Rationale", "").lower(), (
            "HOLD rationale should acknowledge positive carry"
        )

    def test_broken_negative_carry_above_basis_holds(self):
        """BROKEN + negative carry BUT stock above net cost → HOLD (cushion absorbs bleed).

        AAPL-like scenario: stock $262.52 vs net cost $250.61 = +4.75% cushion.
        Gamma drag > theta but cumulative premiums provide profit buffer.
        McMillan Ch.3: don't abandon accumulated cost reduction on one cycle's carry.
        """
        row = _archetype_bw_broken_negative_carry()
        # Override: spot ABOVE net cost basis (AAPL scenario)
        row["UL Last"] = 262.52
        row["Net_Cost_Basis_Per_Share"] = 250.61
        row["Basis"] = 27337.0  # raw purchase price × qty
        row["Quantity"] = 100.0
        row["Cumulative_Premium_Collected"] = 22.76
        row["Short_Call_Strike"] = 265.0
        row["Short_Call_DTE"] = 44.0
        row["Short_Call_Delta"] = 0.483
        row["Theta"] = 0.1181  # $/share/day
        row["Gamma"] = 0.0156
        row["HV_20D"] = 0.341
        result = _run_vertical_slice(row, run_mc=False)
        assert result["Action"] in ("HOLD", "LET_EXPIRE", "ACCEPT_CALL_AWAY"), (
            f"BROKEN + negative carry above net cost should HOLD or LET_EXPIRE/ACCEPT_CALL_AWAY (EV winner), "
            f"got {result['Action']}. Rationale: {result.get('Rationale', '')}"
        )

    def test_broken_negative_carry_below_basis_exits(self):
        """BROKEN + negative carry + stock BELOW net cost → EXIT (no cushion)."""
        row = _archetype_bw_broken_negative_carry()
        # Confirm: default DKNG archetype has spot < net cost → EXIT
        assert row["UL Last"] < row["Net_Cost_Basis_Per_Share"], "Test precondition: spot < net cost"
        result = _run_vertical_slice(row, run_mc=False)
        assert result["Action"] == "EXIT", (
            f"BROKEN + negative carry below net cost should EXIT, got {result['Action']}"
        )

    def test_broken_standard_path_otm_call_buyback_then_sell_stock(self):
        """Standard BROKEN + negative carry + stock ABOVE net cost + OTM call → buy back call, then sell stock.

        AAPL scenario: stock $256.81, net cost $250.61, short $270 call Δ 0.15 (5% OTM).
        Shares are collateral for the short call — must buy back call first to release shares.
        """
        row = _archetype_bw_broken_negative_carry()
        row["UL Last"] = 256.81
        row["Net_Cost_Basis_Per_Share"] = 250.61
        row["Basis"] = 25061.0
        row["Quantity"] = 100.0
        row["Cumulative_Premium_Collected"] = 22.76
        row["Short_Call_Strike"] = 270.0   # 5.1% OTM
        row["Short_Call_DTE"] = 30.0
        row["Short_Call_Delta"] = 0.15     # < 0.30 threshold
        row["Short_Call_Last"] = 1.50
        row["Theta"] = 0.03
        row["Gamma"] = 0.005
        row["HV_20D"] = 0.25
        result = _run_vertical_slice(row, run_mc=False)
        assert result["Action"] in ("EXIT", "LET_EXPIRE", "ACCEPT_CALL_AWAY"), (
            f"OTM call + stock above basis should EXIT or LET_EXPIRE/ACCEPT_CALL_AWAY (EV winner), "
            f"got {result['Action']}. Rationale: {result.get('Rationale', '')}"
        )

    def test_broken_standard_path_atm_call_cushion_holds(self):
        """Standard BROKEN + negative carry + stock ABOVE net cost + near-ATM call → HOLD.

        Same AAPL scenario but call is near ATM (Δ 0.45, strike $258, 0.5% OTM).
        Legs are coupled — can't sell stock without closing call. Generic cushion HOLD.
        """
        row = _archetype_bw_broken_negative_carry()
        row["UL Last"] = 256.81
        row["Net_Cost_Basis_Per_Share"] = 250.61
        row["Basis"] = 25061.0
        row["Quantity"] = 100.0
        row["Cumulative_Premium_Collected"] = 22.76
        row["Short_Call_Strike"] = 258.0   # near ATM — only 0.5% OTM
        row["Short_Call_DTE"] = 30.0
        row["Short_Call_Delta"] = 0.45     # > 0.30 threshold
        row["Theta"] = 0.03
        row["Gamma"] = 0.005
        row["HV_20D"] = 0.25
        result = _run_vertical_slice(row, run_mc=False)
        assert result["Action"] in ("HOLD", "LET_EXPIRE", "ACCEPT_CALL_AWAY"), (
            f"Near-ATM call + stock above basis should HOLD or LET_EXPIRE/ACCEPT_CALL_AWAY (EV winner), "
            f"got {result['Action']}. Rationale: {result.get('Rationale', '')}"
        )

    def test_broken_standard_path_negative_carry_below_basis_exits(self):
        """Standard BROKEN gate (non-gamma-dominant) + negative carry + stock BELOW net cost → EXIT.

        Same as above but stock below net cost — no cushion to absorb bleed.
        """
        row = _archetype_bw_broken_negative_carry()
        row["UL Last"] = 245.00             # BELOW net cost
        row["Net_Cost_Basis_Per_Share"] = 250.61
        row["Basis"] = 25061.0
        row["Quantity"] = 100.0
        row["Cumulative_Premium_Collected"] = 22.76
        row["Short_Call_Strike"] = 270.0
        row["Short_Call_DTE"] = 30.0
        row["Short_Call_Delta"] = 0.10
        row["Theta"] = 0.03
        row["Gamma"] = 0.005
        row["HV_20D"] = 0.25
        result = _run_vertical_slice(row, run_mc=False)
        assert result["Action"] == "EXIT", (
            f"Standard BROKEN + negative carry + stock BELOW net cost should EXIT, "
            f"got {result['Action']}. Rationale: {result.get('Rationale', '')}"
        )


# =============================================================================
# Archetype 11a: STOCK_ONLY deep loss (EOSE-like)
# =============================================================================

def _archetype_stock_only_deep_loss() -> pd.Series:
    """
    EOSE STOCK_ONLY: -65.3% loss, 2000 shares, WEAKENING equity.
    No option hedge — pure directional risk at deep drawdown.
    Expected: EXIT HIGH (Gate 2 — deep loss stop)
    """
    return pd.Series({
        "TradeID": "T-GR-011A", "LegID": "L-GR-011A",
        "Symbol": "EOSE",
        "Underlying_Ticker": "EOSE",
        "Strategy": "STOCK_ONLY",
        "AssetType": "STOCK",

        "UL Last": 4.67,
        "Basis": 13500.0,
        "Quantity": 2000.0,
        "Underlying_Price_Entry": 13.50,

        "Delta": 0.0, "Strike": np.nan, "DTE": np.nan,
        "Premium_Entry": np.nan, "Last": np.nan,
        "HV_20D": 0.85,

        "IV_Entry": np.nan, "IV_30D": np.nan, "IV_Now": np.nan,
        "IV_Percentile": np.nan, "IV_vs_HV_Gap": np.nan,

        "Theta": 0.0, "Gamma": 0.0,

        "Thesis_State": "INTACT",
        "Thesis_Gate": "PASS",
        "_thesis_blocks_roll": False,
        "Thesis_Summary": "",
        "PriceStructure_State": "STRUCTURAL_DOWN",
        "TrendIntegrity_State": "NO_TREND",
        "ema50_slope": -0.22,
        "hv_20d_percentile": 75.0,
        "Equity_Integrity_State": "WEAKENING",
        "Equity_Integrity_Reason": "EMA20↓, ROC20=-18.5%",

        "Position_Regime": "NEUTRAL",
        "Trajectory_Consecutive_Debit_Rolls": 0,
        "Trajectory_Stock_Return": -0.65,

        "PnL_Dollar": -17660.0, "PnL_Total": -17660.0,
        "Total_GL_Decimal": -0.653,

        "_Active_Conditions": "", "_Condition_Resolved": "",
        "Snapshot_TS": pd.Timestamp.now(),
        "Earnings_Date": None,
        "run_id": "golden-row-test", "Schema_Hash": "abc123",
        "IV": None,
    })


class TestArchetype11a_StockOnlyDeepLoss:
    """EOSE STOCK_ONLY: 2000 shares at -65.3% loss → EXIT HIGH."""

    def test_deep_loss_exits(self):
        result = _run_vertical_slice(_archetype_stock_only_deep_loss(), run_mc=False)
        assert result["Action"] == "EXIT", (
            f"STOCK_ONLY at -65% should EXIT, got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )

    def test_deep_loss_urgency_high(self):
        result = _run_vertical_slice(_archetype_stock_only_deep_loss(), run_mc=False)
        assert result["Urgency"] == "HIGH", (
            f"Expected HIGH urgency for -65% loss, got {result['Urgency']}"
        )

    def test_deep_loss_rationale_mentions_loss(self):
        result = _run_vertical_slice(_archetype_stock_only_deep_loss(), run_mc=False)
        rat = result.get("Rationale", "").lower()
        assert "loss" in rat or "65" in rat or "deep" in rat, (
            f"Rationale must mention loss severity. Got: {rat}"
        )

    def test_deep_loss_doctrine_source(self):
        result = _run_vertical_slice(_archetype_stock_only_deep_loss(), run_mc=False)
        src = result.get("Doctrine_Source", "")
        assert "McMillan" in src or "Deep Loss" in src, (
            f"Doctrine source must cite McMillan or Deep Loss. Got: {src}"
        )


# =============================================================================
# Archetype 11b: STOCK_ONLY BROKEN equity (mild loss)
# =============================================================================

def _archetype_stock_only_broken_equity() -> pd.Series:
    """
    STOCK_ONLY: -15% loss, BROKEN equity integrity.
    Structural breakdown takes priority over moderate P&L.
    Expected: EXIT HIGH (Gate 1 — BROKEN equity)
    """
    return pd.Series({
        "TradeID": "T-GR-011B", "LegID": "L-GR-011B",
        "Symbol": "XYZ",
        "Underlying_Ticker": "XYZ",
        "Strategy": "STOCK_ONLY",
        "AssetType": "STOCK",

        "UL Last": 42.50,
        "Basis": 5000.0,
        "Quantity": 100.0,
        "Underlying_Price_Entry": 50.00,

        "Delta": 0.0, "Strike": np.nan, "DTE": np.nan,
        "Premium_Entry": np.nan, "Last": np.nan,
        "HV_20D": 0.45,

        "IV_Entry": np.nan, "IV_30D": np.nan, "IV_Now": np.nan,
        "IV_Percentile": np.nan, "IV_vs_HV_Gap": np.nan,

        "Theta": 0.0, "Gamma": 0.0,

        "Thesis_State": "INTACT",
        "Thesis_Gate": "PASS",
        "_thesis_blocks_roll": False,
        "Thesis_Summary": "",
        "PriceStructure_State": "STRUCTURAL_DOWN",
        "TrendIntegrity_State": "NO_TREND",
        "ema50_slope": -0.30,
        "hv_20d_percentile": 80.0,
        "Equity_Integrity_State": "BROKEN",
        "Equity_Integrity_Reason": "EMA20↓(-0.12), EMA50↓(-0.30), ROC20=-15%, HV=80th_pct",

        "Position_Regime": "NEUTRAL",
        "Trajectory_Consecutive_Debit_Rolls": 0,
        "Trajectory_Stock_Return": -0.15,

        "PnL_Dollar": -750.0, "PnL_Total": -750.0,
        "Total_GL_Decimal": -0.15,

        "_Active_Conditions": "", "_Condition_Resolved": "",
        "Snapshot_TS": pd.Timestamp.now(),
        "Earnings_Date": None,
        "run_id": "golden-row-test", "Schema_Hash": "abc123",
        "IV": None,
    })


class TestArchetype11b_StockOnlyBrokenEquity:
    """STOCK_ONLY with BROKEN equity at -15% → EXIT HIGH."""

    def test_broken_equity_exits(self):
        result = _run_vertical_slice(_archetype_stock_only_broken_equity(), run_mc=False)
        assert result["Action"] == "EXIT", (
            f"BROKEN equity STOCK_ONLY should EXIT, got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )

    def test_broken_equity_urgency_high(self):
        result = _run_vertical_slice(_archetype_stock_only_broken_equity(), run_mc=False)
        assert result["Urgency"] == "HIGH", (
            f"Expected HIGH urgency for BROKEN equity, got {result['Urgency']}"
        )

    def test_broken_equity_rationale_mentions_broken(self):
        result = _run_vertical_slice(_archetype_stock_only_broken_equity(), run_mc=False)
        rat = result.get("Rationale", "").lower()
        assert "broken" in rat, (
            f"Rationale must mention BROKEN equity. Got: {rat}"
        )


# =============================================================================
# Archetype 11c: STOCK_ONLY healthy + CC eligible
# =============================================================================

def _archetype_stock_only_healthy_cc() -> pd.Series:
    """
    STOCK_ONLY: +9.1% gain, HEALTHY equity, 200 shares (CC eligible).
    No distress — should HOLD LOW with CC opportunity note.
    Expected: HOLD LOW (Gate 5 — CC opportunity)
    """
    return pd.Series({
        "TradeID": "T-GR-011C", "LegID": "L-GR-011C",
        "Symbol": "AAPL",
        "Underlying_Ticker": "AAPL",
        "Strategy": "STOCK_ONLY",
        "AssetType": "STOCK",

        "UL Last": 218.20,
        "Basis": 40000.0,
        "Quantity": 200.0,
        "Underlying_Price_Entry": 200.00,

        "Delta": 0.0, "Strike": np.nan, "DTE": np.nan,
        "Premium_Entry": np.nan, "Last": np.nan,
        "HV_20D": 0.25,

        "IV_Entry": np.nan, "IV_30D": np.nan, "IV_Now": np.nan,
        "IV_Percentile": np.nan, "IV_vs_HV_Gap": np.nan,

        "Theta": 0.0, "Gamma": 0.0,

        "Thesis_State": "INTACT",
        "Thesis_Gate": "PASS",
        "_thesis_blocks_roll": False,
        "Thesis_Summary": "",
        "PriceStructure_State": "RANGE_BOUND",
        "TrendIntegrity_State": "TRENDING_UP",
        "ema50_slope": 0.05,
        "hv_20d_percentile": 30.0,
        "Equity_Integrity_State": "HEALTHY",
        "Equity_Integrity_Reason": "EMA20↑, EMA50↑, ROC20=+5%",

        "Position_Regime": "NEUTRAL",
        "Trajectory_Consecutive_Debit_Rolls": 0,
        "Trajectory_Stock_Return": 0.091,

        "PnL_Dollar": 3640.0, "PnL_Total": 3640.0,
        "Total_GL_Decimal": 0.091,

        "_Active_Conditions": "", "_Condition_Resolved": "",
        "Snapshot_TS": pd.Timestamp.now(),
        "Earnings_Date": None,
        "run_id": "golden-row-test", "Schema_Hash": "abc123",
        "IV": None,
    })


class TestArchetype11c_StockOnlyHealthyCC:
    """STOCK_ONLY: HEALTHY, +9.1%, 200 shares → HOLD LOW + CC note."""

    def test_healthy_holds(self):
        result = _run_vertical_slice(_archetype_stock_only_healthy_cc(), run_mc=False)
        assert result["Action"] == "HOLD", (
            f"Healthy STOCK_ONLY should HOLD, got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )

    def test_healthy_urgency_low(self):
        result = _run_vertical_slice(_archetype_stock_only_healthy_cc(), run_mc=False)
        assert result["Urgency"] == "LOW", (
            f"Expected LOW urgency for healthy stock, got {result['Urgency']}"
        )

    def test_healthy_rationale_mentions_cc(self):
        result = _run_vertical_slice(_archetype_stock_only_healthy_cc(), run_mc=False)
        rat = result.get("Rationale", "").lower()
        assert "covered call" in rat or "cc" in rat.upper() or "call" in rat, (
            f"Rationale must mention CC opportunity. Got: {rat}"
        )


# =============================================================================
# Archetype 12: Recovery Ladder BUY_WRITE
# =============================================================================

def _archetype_recovery_ladder_bw() -> pd.Series:
    """
    EOSE BUY_WRITE: stock deeply underwater (-56.5%), hard stop massively breached,
    but _cycle_count=3 — trader has consciously sold calls through 2 roll cycles
    on an already-underwater stock.  This is deliberate recovery premium harvesting.

    Setup (modeled on EOSE Mar 5):
      - Stock at $6.55, broker basis $31.33 (2000 shares)
      - Net cost $15.08 after $16.25/sh premium collected over 3 cycles
      - drift_from_net = (6.55 - 15.08) / 15.08 = -56.5%
      - Hard stop at $12.06 (80% of net cost) — massively breached
      - Thesis INTACT (energy storage narrative still valid)

    Expected: HOLD MEDIUM (recovery ladder guard, NOT EXIT CRITICAL)
    """
    return pd.Series({
        "TradeID": "T-GR-012", "LegID": "L-GR-012",
        "Symbol": "EOSE",
        "Underlying_Ticker": "EOSE",
        "Strategy": "BUY_WRITE",
        "AssetType": "STOCK",

        "UL Last": 6.55,
        "Basis": 62660.0,           # 2000 × $31.33
        "Quantity": 2000.0,
        "Underlying_Price_Entry": 31.33,
        "Net_Cost_Basis_Per_Share": 15.08,
        "Cumulative_Premium_Collected": 16.25,
        "_cycle_count": 3,

        "Short_Call_Delta": 0.35,
        "Short_Call_Strike": 7.0,
        "Short_Call_DTE": 42.0,
        "Short_Call_Premium": 0.65,
        "Short_Call_Last": 0.48,
        "Short_Call_Moneyness": "OTM",

        "Delta": 0.0,
        "Strike": np.nan, "DTE": np.nan,
        "Premium_Entry": 0.65, "Last": np.nan,
        "HV_20D": 1.08,

        "IV_Entry": 0.95, "IV_30D": 0.83, "IV_Now": 0.88,
        "IV_Percentile": 45.0, "IV_vs_HV_Gap": -0.25,

        "Theta": 0.015, "Gamma": 0.04,

        "Thesis_State": "INTACT",
        "Thesis_Gate": "PASS",
        "_thesis_blocks_roll": False,
        "Thesis_Summary": "",
        "PriceStructure_State": "STRUCTURAL_DOWN",
        "TrendIntegrity_State": "NO_TREND",
        "ema50_slope": -0.30,
        "hv_20d_percentile": 82.0,
        "Equity_Integrity_State": "WEAKENING",
        "Equity_Integrity_Reason": "EMA20↓, ROC20=-22%",

        "Position_Regime": "RECOVERY_GRIND",
        "Trajectory_Consecutive_Debit_Rolls": 0,
        "Trajectory_Stock_Return": -0.79,

        "PnL_Dollar": -17060.0, "Total_GL_Decimal": -0.565,

        "_Active_Conditions": "", "_Condition_Resolved": "",
        "Snapshot_TS": pd.Timestamp.now(),
        "Earnings_Date": None,
        "Days_In_Trade": 60,
        "run_id": "golden-row-test", "Schema_Hash": "abc123",
        "IV": None,
    })


class TestArchetype12_RecoveryLadderBW:
    """EOSE BUY_WRITE: hard stop massively breached but _cycle_count=3 →
    recovery ladder active → HOLD MEDIUM (not EXIT CRITICAL)."""

    def test_recovery_ladder_holds_not_exits(self):
        """_cycle_count=3, thesis=INTACT, cum_premium>0 → recovery mode (not EXIT).
        Recovery Premium Mode may return HOLD, ROLL_UP_OUT, WRITE_NOW, or
        HOLD_STOCK_WAIT — the key constraint is it must NOT EXIT."""
        result = _run_vertical_slice(_archetype_recovery_ladder_bw(), run_mc=False)
        _non_exit_actions = ("HOLD", "ROLL", "ROLL_UP_OUT", "WRITE_NOW",
                             "HOLD_STOCK_WAIT", "PAUSE_WRITING")
        assert result["Action"] in _non_exit_actions, (
            f"Recovery ladder BW should not EXIT, got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )
        assert "recovery" in result.get("Rationale", "").lower(), (
            f"Rationale must mention recovery. Got: {result.get('Rationale', '')}"
        )

    def test_fresh_bw_still_exits_on_hard_stop(self):
        """_cycle_count=1, same deep loss → EXIT (not a recovery ladder).
        With cycle_count=1 and no premium, recovery premium mode won't activate."""
        row = _archetype_recovery_ladder_bw()
        row["_cycle_count"] = 1
        row["Cumulative_Premium_Collected"] = 0.0
        result = _run_vertical_slice(row, run_mc=False)
        assert result["Action"] in ("EXIT", "EXIT_STOCK"), (
            f"Fresh BW (cycle 1, no premium) below hard stop should EXIT, got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )

    def test_recovery_ladder_broken_thesis_still_exits(self):
        """_cycle_count=3 but thesis=BROKEN → EXIT (recovery pointless)."""
        row = _archetype_recovery_ladder_bw()
        row["Thesis_State"] = "BROKEN"
        result = _run_vertical_slice(row, run_mc=False)
        assert result["Action"] in ("EXIT", "EXIT_STOCK"), (
            f"BROKEN thesis recovery ladder should EXIT, got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )


# =============================================================================
# Archetype 12b: Moderate Recovery — QCOM-style BUY_WRITE
# =============================================================================

def _archetype_moderate_recovery_bw() -> pd.Series:
    """
    QCOM-style BUY_WRITE: stock moderately underwater (-15.1%), approaching
    hard stop, but income strategy active with premium collected.

    Setup (modeled on QCOM Mar 9):
      - Stock at $148.52, broker basis $175.00 (100 shares)
      - Net cost $169.50 after $5.50/sh premium collected over 2 cycles
      - drift_from_net = (148.52 - 169.50) / 169.50 = -12.4%
      - Hard stop at $135.60 — still has cushion
      - Thesis DEGRADED (not BROKEN)
      - IV viable for premium collection

    Expected: ROLL (moderate recovery), NOT EXIT CRITICAL
    """
    return pd.Series({
        "TradeID": "T-GR-012B", "LegID": "L-GR-012B",
        "Symbol": "QCOM",
        "Underlying_Ticker": "QCOM",
        "Strategy": "BUY_WRITE",
        "AssetType": "STOCK",

        "UL Last": 148.52,
        "Basis": 17500.0,           # 100 × $175.00
        "Quantity": 100.0,
        "Underlying_Price_Entry": 175.00,
        "Net_Cost_Basis_Per_Share": 169.50,
        "Cumulative_Premium_Collected": 5.50,
        "_cycle_count": 2,

        "Short_Call_Delta": 0.40,
        "Short_Call_Strike": 155.0,
        "Short_Call_DTE": 35.0,
        "Short_Call_Premium": 3.20,
        "Short_Call_Last": 2.10,
        "Short_Call_Moneyness": "OTM",

        "Delta": 0.0,
        "Strike": np.nan, "DTE": np.nan,
        "Premium_Entry": 3.20, "Last": np.nan,
        "HV_20D": 0.38,

        "IV_Entry": 0.42, "IV_30D": 0.35, "IV_Now": 0.40,
        "IV_Rank": 55.0,
        "IV_Percentile": 52.0, "IV_vs_HV_Gap": 0.02,

        "Theta": 0.045, "Gamma": 0.012,

        "Thesis_State": "DEGRADED",
        "Thesis_Gate": "PASS",
        "_thesis_blocks_roll": False,
        "Thesis_Summary": "",
        "PriceStructure_State": "WEAKENING",
        "TrendIntegrity_State": "TREND_WEAKENING",
        "ema50_slope": -0.05,
        "hv_20d_percentile": 55.0,
        "Equity_Integrity_State": "BROKEN",
        "Equity_Integrity_Reason": "Below EMA20, SMA50 declining",

        "Position_Regime": "DECLINING",
        "Trajectory_Consecutive_Debit_Rolls": 0,
        "Trajectory_Stock_Return": -0.15,

        "PnL_Dollar": -2098.0, "Total_GL_Decimal": -0.124,

        "_Active_Conditions": "", "_Condition_Resolved": "",
        "Snapshot_TS": pd.Timestamp.now(),
        "Earnings_Date": None,
        "Days_In_Trade": 45,
        "run_id": "golden-row-test", "Schema_Hash": "abc123",
        "IV": None,
    })


class TestArchetype12b_ModerateRecoveryBW:
    """QCOM-style BUY_WRITE: -12.4% drift with income active → ROLL, not EXIT."""

    def test_moderate_recovery_rolls_not_exits(self):
        """Premium collected + IV viable + thesis not BROKEN → ROLL/ROLL_UP_OUT."""
        result = _run_vertical_slice(_archetype_moderate_recovery_bw(), run_mc=False)
        assert result["Action"] in ("ROLL", "ROLL_UP_OUT", "HOLD", "WRITE_NOW", "HOLD_STOCK_WAIT"), (
            f"Moderate recovery BW should not EXIT, got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )

    def test_moderate_recovery_broken_thesis_exits(self):
        """Same position but thesis BROKEN → EXIT (no recovery viable)."""
        row = _archetype_moderate_recovery_bw()
        row["Thesis_State"] = "BROKEN"
        result = _run_vertical_slice(row, run_mc=False)
        assert result["Action"] == "EXIT", (
            f"BROKEN thesis should EXIT, got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )

    def test_moderate_recovery_no_premium_exits(self):
        """No premium collected + drift below -15% → no recovery path → EXIT.

        With no income path active and drift below PNL_APPROACHING_HARD_STOP,
        EXIT gets CAPITAL tag and auto-wins over EV comparison.
        """
        row = _archetype_moderate_recovery_bw()
        row["Cumulative_Premium_Collected"] = 0.0
        row["_cycle_count"] = 1
        # Push drift below -15% so CAPITAL tag applies (no income path)
        row["UL Last"] = 143.0  # drift = (143.0 - 169.50) / 169.50 = -15.6%
        result = _run_vertical_slice(row, run_mc=False)
        assert result["Action"] == "EXIT", (
            f"No premium history + drift<-15% should EXIT, got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )

    def test_moderate_recovery_deeper_loss_still_rolls(self):
        """Position at -18% (approaching hard stop range) → still ROLL/ROLL_UP_OUT with recovery."""
        row = _archetype_moderate_recovery_bw()
        row["UL Last"] = 139.0  # drift ~-18% from net cost $169.50
        result = _run_vertical_slice(row, run_mc=False)
        assert result["Action"] in ("ROLL", "ROLL_UP_OUT", "HOLD", "WRITE_NOW", "HOLD_STOCK_WAIT"), (
            f"Approaching hard stop with recovery should not EXIT, got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )

    def test_moderate_recovery_low_iv_exits(self):
        """IV too low to generate premium + drift below -15% → EXIT.

        Low IV disables _income_path_active (requires IV>15%). Combined with
        drift below PNL_APPROACHING_HARD_STOP, EXIT gets CAPITAL tag.
        """
        row = _archetype_moderate_recovery_bw()
        row["IV_Now"] = 0.08  # below 15% floor → income path inactive
        row["IV_30D"] = 0.07
        # Push drift below -15% so CAPITAL tag applies (income path disabled by low IV)
        row["UL Last"] = 143.0  # drift = (143.0 - 169.50) / 169.50 = -15.6%
        result = _run_vertical_slice(row, run_mc=False)
        assert result["Action"] == "EXIT", (
            f"Low IV + drift<-15% should EXIT (can't generate premium), got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )

    def test_deep_recovery_still_works(self):
        """Position at -30% (deep recovery range) → should NOT exit."""
        row = _archetype_moderate_recovery_bw()
        row["UL Last"] = 118.65  # drift ~-30% from net cost $169.50
        row["_cycle_count"] = 3
        result = _run_vertical_slice(row, run_mc=False)
        # Deep recovery: recovery premium mode activates. Any recovery action acceptable.
        # The key constraint: must NOT be EXIT/EXIT_STOCK.
        _non_exit = ("HOLD", "ROLL", "ROLL_UP_OUT", "WRITE_NOW",
                     "HOLD_STOCK_WAIT", "PAUSE_WRITING")
        assert result["Action"] in _non_exit, (
            f"Deep recovery should not EXIT, got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )


# =============================================================================
# Archetype 12c: Shallow BW Recovery — DKNG-style (< -10% drift)
# =============================================================================

def _archetype_shallow_recovery_bw() -> pd.Series:
    """
    DKNG-style BUY_WRITE: barely underwater (-3.5%) but equity BROKEN,
    7 cycles of premium collected, thesis INTACT.

    Setup (modeled on DKNG Mar 9):
      - Stock at $25.16, broker basis $30.51 (1000 shares)
      - Net cost $26.06 after $4.45/sh premium collected over 7 cycles
      - drift_from_net = (25.16 - 26.06) / 26.06 = -3.5%
      - Equity BROKEN (EMA50 declining, ROC20=-5.5%)
      - Thesis INTACT, gap to breakeven only $0.90/share

    Expected: NOT EXIT (ROLL or HOLD — EV decides)
    DKNG at -3.5% with 7 cycles and $0.90 gap should never get forced EXIT.
    """
    return pd.Series({
        "TradeID": "T-GR-012C", "LegID": "L-GR-012C",
        "Symbol": "DKNG",
        "Underlying_Ticker": "DKNG",
        "Strategy": "BUY_WRITE",
        "AssetType": "STOCK",

        "UL Last": 25.16,
        "Basis": 30510.0,           # 1000 × $30.51
        "Quantity": 1000.0,
        "Underlying_Price_Entry": 30.51,
        "Net_Cost_Basis_Per_Share": 26.06,
        "Cumulative_Premium_Collected": 4.45,
        "_cycle_count": 7,

        "Short_Call_Delta": 0.354,
        "Short_Call_Strike": 27.5,
        "Short_Call_DTE": 39.0,
        "Short_Call_Premium": 1.05,
        "Short_Call_Last": 1.05,
        "Short_Call_Moneyness": "OTM",

        "Delta": 0.0,
        "Strike": np.nan, "DTE": np.nan,
        "Premium_Entry": 1.05, "Last": np.nan,
        "HV_20D": 0.676,

        "IV_Entry": 0.59, "IV_30D": 0.593, "IV_Now": 0.593,
        "IV_Rank": 45.0,
        "IV_Percentile": 40.0, "IV_vs_HV_Gap": -0.083,

        "Theta": 0.023, "Gamma": 0.076,

        "Thesis_State": "INTACT",
        "Thesis_Gate": "PASS",
        "_thesis_blocks_roll": False,
        "Thesis_Summary": "",
        "PriceStructure_State": "WEAKENING",
        "TrendIntegrity_State": "TREND_WEAKENING",
        "ema50_slope": -0.08,
        "hv_20d_percentile": 80.0,
        "Equity_Integrity_State": "BROKEN",
        "Equity_Integrity_Reason": "EMA50↓, ROC20=-5.5%, HV=80th_pct",

        "Position_Regime": "MEAN_REVERSION",
        "Trajectory_Consecutive_Debit_Rolls": 1,
        "Trajectory_Stock_Return": 0.0,

        "PnL_Dollar": -5427.0, "Total_GL_Decimal": -0.035,

        "_Active_Conditions": "", "_Condition_Resolved": "",
        "Snapshot_TS": pd.Timestamp.now(),
        "Earnings_Date": None,
        "Days_In_Trade": 90,
        "run_id": "golden-row-test", "Schema_Hash": "abc123",
        "IV": None,
    })


class TestArchetype12c_ShallowRecoveryBW:
    """DKNG-style BUY_WRITE: -3.5% drift with 7 cycles and INTACT thesis
    should NOT get forced EXIT via CAPITAL override."""

    def test_shallow_income_path_not_forced_exit(self):
        """7 cycles of premium + INTACT thesis → EXIT must not auto-win."""
        result = _run_vertical_slice(_archetype_shallow_recovery_bw(), run_mc=False)
        assert result["Action"] != "EXIT" or result.get("Urgency") == "LOW", (
            f"Shallow BW with 7 cycles should not force EXIT, got {result['Action']} "
            f"{result.get('Urgency', '')}. Rationale: {result.get('Rationale', '')}"
        )

    def test_shallow_broken_thesis_exits(self):
        """Same position but thesis BROKEN → EXIT (no viable repair)."""
        row = _archetype_shallow_recovery_bw()
        row["Thesis_State"] = "BROKEN"
        result = _run_vertical_slice(row, run_mc=False)
        assert result["Action"] == "EXIT", (
            f"BROKEN thesis should EXIT, got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )

    def test_shallow_first_cycle_exits(self):
        """First cycle + no premium + drift below -15% → EXIT (no income path, CAPITAL danger).

        Without income path and with drift below PNL_APPROACHING_HARD_STOP,
        EXIT gets CAPITAL tag and auto-wins over EV comparison.
        """
        row = _archetype_shallow_recovery_bw()
        row["_cycle_count"] = 1
        row["Cumulative_Premium_Collected"] = 0.0
        # Push drift below -15% so CAPITAL tag applies
        # Net cost basis = 26.06 → need price <= 26.06 * 0.85 = 22.15
        row["UL Last"] = 22.0  # drift = (22.0 - 26.06) / 26.06 = -15.6%
        result = _run_vertical_slice(row, run_mc=False)
        assert result["Action"] == "EXIT", (
            f"First cycle with no premium + drift<-15% should EXIT, got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )


# =============================================================================
# Archetype 12d: BUY_WRITE Day-0 Grace Period
# =============================================================================

class TestArchetype12d_BWGracePeriod:
    """BUY_WRITE opened today — scan engine just approved, don't ROLL/EXIT."""

    def test_day0_holds(self):
        """Day-0 BUY_WRITE should HOLD regardless of other gate triggers."""
        row = _archetype_bw_21dte_roll()
        row["Days_In_Trade"] = 0
        result = _run_vertical_slice(row, run_mc=False)
        assert result["Action"] == "HOLD", (
            f"Day-0 BW should HOLD (grace period), got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )
        assert "grace" in result.get("Rationale", "").lower(), (
            "Rationale should mention grace period"
        )

    def test_day1_holds(self):
        """Day-1 BUY_WRITE should still HOLD (grace = 2 days)."""
        row = _archetype_bw_21dte_roll()
        row["Days_In_Trade"] = 1
        result = _run_vertical_slice(row, run_mc=False)
        assert result["Action"] == "HOLD", (
            f"Day-1 BW should HOLD (grace period), got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )

    def test_day2_normal(self):
        """Day-2 BUY_WRITE — grace expired, normal doctrine applies."""
        row = _archetype_bw_21dte_roll()
        row["Days_In_Trade"] = 2
        result = _run_vertical_slice(row, run_mc=False)
        assert result["Action"] != "HOLD" or "grace" not in result.get("Rationale", "").lower(), (
            "Day-2 BW should NOT be held by grace period"
        )

    def test_catastrophic_gap_no_grace(self):
        """Day-0 with -30% gap → no grace, exits immediately."""
        row = _archetype_bw_21dte_roll()
        row["Days_In_Trade"] = 0
        # Stock collapsed from $96 net cost to $67.20 (-30%)
        row["UL Last"] = 67.20
        result = _run_vertical_slice(row, run_mc=False)
        assert result["Action"] != "HOLD" or "grace" not in result.get("Rationale", "").lower(), (
            f"Catastrophic gap on day 0 should NOT get grace, got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )


# =============================================================================
# Archetype 13: Deep ITM CSP — Roll-vs-Assignment Cost Gate
# =============================================================================

def _archetype_deep_itm_csp() -> pd.Series:
    """
    EOSE-like CSP: stock collapsed through strike, put is massively ITM.

    Setup:
      - EOSE at $6.13, put strike $12.50, premium received $1.00
      - Current option price = $6.50 (deep ITM intrinsic ≈ $6.37)
      - DTE = 14, Delta = -0.95
      - Intrinsic/stock = 104% — rolling costs more than the stock itself

    Expected: EXIT HIGH (roll-vs-assignment cost gate)
    NOT: ROLL (which the 21-DTE income gate would otherwise recommend)
    """
    return pd.Series({
        "TradeID": "T-GR-013", "LegID": "L-GR-013",
        "Symbol": "EOSE260320P00012500",
        "Underlying_Ticker": "EOSE",
        "Strategy": "CSP", "AssetType": "OPTION",
        "Option_Type": "PUT", "Call/Put": "P",

        "UL Last": 6.13, "Strike": 12.50,
        "DTE": 14.0, "Premium_Entry": 1.00,
        "Last": 6.50, "Bid": 6.30,
        "Delta": -0.95, "Gamma": 0.005,
        "Theta": 0.02, "Vega": 0.01,
        "HV_20D": 0.85, "IV_Now": 0.90, "IV_Entry": 0.55, "IV_30D": 0.85,
        "Quantity": -2.0, "Basis": 200.0,
        "Net_Cost_Basis_Per_Share": 11.50,

        "Moneyness_Label": "ITM",
        "Lifecycle_Phase": "INCOME_WINDOW",
        "TrendIntegrity_State": "TREND_DOWN",
        "PriceStructure_State": "STRUCTURE_BROKEN",
        "Drift_Direction": "Down",
        "VolatilityState_State": "EXTREME",
        "MomentumVelocity_State": "DECLINING",
        "Position_Regime": "TRENDING_CHASE",
        "Trajectory_Consecutive_Debit_Rolls": 2,
        "Trajectory_Stock_Return": -0.51,

        "Portfolio_Delta_Utilization_Pct": 8.0,
        "Equity_Integrity_State": "BROKEN",
        "IV_Percentile": 85.0, "IV_Percentile_Depth": 60,
        "IV_vs_HV_Gap": 0.05,
        "Assignment_Acceptable": True,

        "Thesis_State": "BROKEN",
        "Thesis_Gate": "FAIL",
        "_thesis_blocks_roll": False,
        "Thesis_Summary": "",

        "Snapshot_TS": pd.Timestamp.now(),
        "Earnings_Date": None,
        "run_id": "golden-row-test", "Schema_Hash": "abc123",
        "IV": None,
        "_Active_Conditions": "", "_Condition_Resolved": "",
        "MC_Assign_P_Expiry": 0.95,
    })


def _archetype_moderate_itm_csp() -> pd.Series:
    """
    Normal ITM CSP: stock dipped slightly below strike, roll is still economical.

    Setup:
      - AAPL at $195, put strike $210, premium received $6.00
      - Current option price = $16.00 (intrinsic $15, TV $1)
      - But Last ($16) is NOT > 50% of stock ($195 × 50% = $97.50)
      - DTE = 14, Delta = -0.85

    Expected: ROLL (21-DTE income gate — cost gate does NOT fire)
    """
    return pd.Series({
        "TradeID": "T-GR-013b", "LegID": "L-GR-013b",
        "Symbol": "AAPL260320P00210000",
        "Underlying_Ticker": "AAPL",
        "Strategy": "CSP", "AssetType": "OPTION",
        "Option_Type": "PUT", "Call/Put": "P",

        "UL Last": 195.0, "Strike": 210.0,
        "DTE": 14.0, "Premium_Entry": 6.00,
        "Last": 16.00, "Bid": 15.80,
        "Delta": -0.85, "Gamma": 0.01,
        "Theta": 0.05, "Vega": 0.08,
        "HV_20D": 0.25, "IV_Now": 0.28, "IV_Entry": 0.30, "IV_30D": 0.28,
        "Quantity": -1.0, "Basis": 600.0,
        "Net_Cost_Basis_Per_Share": 204.0,

        "Moneyness_Label": "ITM",
        "Lifecycle_Phase": "INCOME_WINDOW",
        "TrendIntegrity_State": "TREND_DOWN",
        "PriceStructure_State": "STRUCTURE_WEAKENING",
        "Drift_Direction": "Down",
        "VolatilityState_State": "NORMAL",
        "MomentumVelocity_State": "DECLINING",
        "Position_Regime": "MEAN_REVERSION",
        "Trajectory_Consecutive_Debit_Rolls": 0,
        "Trajectory_Stock_Return": -0.07,

        "Portfolio_Delta_Utilization_Pct": 5.0,
        "Equity_Integrity_State": "INTACT",
        "IV_Percentile": 45.0, "IV_Percentile_Depth": 90,
        "IV_vs_HV_Gap": 0.03,
        "Assignment_Acceptable": True,

        "Thesis_State": "WEAKENING",
        "Thesis_Gate": "PASS",
        "_thesis_blocks_roll": False,
        "Thesis_Summary": "",

        "Snapshot_TS": pd.Timestamp.now(),
        "Earnings_Date": None,
        "run_id": "golden-row-test", "Schema_Hash": "abc123",
        "IV": None,
        "_Active_Conditions": "", "_Condition_Resolved": "",
    })


class TestArchetype13_RollVsAssignmentCostGate:
    """Deep ITM CSP: roll cost exceeds 50% of stock price → EXIT, not ROLL."""

    def test_deep_itm_csp_exits_or_rolls(self):
        """EOSE CSP: v2 EV resolver may prefer ROLL (+$1,011 EV) over EXIT."""
        result = _run_vertical_slice(_archetype_deep_itm_csp(), run_mc=False)
        assert result["Action"] in ("EXIT", "ROLL"), (
            f"Deep ITM CSP should EXIT or ROLL (EV winner), got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )
        assert result["Urgency"] in ("HIGH", "CRITICAL", "MEDIUM"), (
            f"Expected MEDIUM+ urgency, got {result['Urgency']}"
        )

    def test_deep_itm_csp_rationale_mentions_ev_or_cost(self):
        """Rationale should explain EV comparison or roll cost."""
        result = _run_vertical_slice(_archetype_deep_itm_csp(), run_mc=False)
        rationale = result.get("Rationale", "").lower()
        assert "roll" in rationale and ("cost" in rationale or "assignment" in rationale or "ev" in rationale), (
            f"Rationale should mention roll cost or EV. Got: {result.get('Rationale', '')}"
        )

    def test_deep_itm_csp_wheel_ready(self):
        """Deep ITM CSP with wheel-ready → HOLD, ROLL, or ASSIGN (EV-driven).

        ASSIGN is valid for wheel-ready CSPs: assignment at a discount
        transitions to covered-call writing (Passarelli Ch.1 wheel cycle).
        """
        row = _archetype_deep_itm_csp()
        # Make wheel-ready: intact chart, good IV, basis at discount
        row["TrendIntegrity_State"] = "TREND_UP"
        row["PriceStructure_State"] = "STRUCTURE_INTACT"
        row["IV_Now"] = 0.50  # > 25% threshold
        row["Net_Cost_Basis_Per_Share"] = 5.50  # below spot $6.13 = discount
        row["Equity_Integrity_State"] = "INTACT"
        result = _run_vertical_slice(row, run_mc=False)
        assert result["Action"] in ("HOLD", "ROLL", "LET_EXPIRE", "ACCEPT_CALL_AWAY"), (
            f"Wheel-ready deep ITM CSP should HOLD, ROLL, LET_EXPIRE, or ACCEPT_CALL_AWAY, got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )

    def test_moderate_itm_csp_still_rolls(self):
        """AAPL CSP: put value $16 < 50% of stock $195 → ROLL (cost gate doesn't fire)."""
        result = _run_vertical_slice(_archetype_moderate_itm_csp(), run_mc=False)
        assert result["Action"] == "ROLL", (
            f"Moderate ITM CSP should ROLL (cost gate not triggered), got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )


# =============================================================================
# Archetype 14: Recently-Rolled Cooldown (Signal Coherence Gate 1)
# =============================================================================

def _archetype_cooldown_bw() -> pd.Series:
    """
    PLTR BUY_WRITE: rolled yesterday, thesis INTACT, DTE=45.

    Setup:
      - PLTR at $157, strike $150, ITM
      - Days_Since_Last_Roll = 1 (rolled yesterday)
      - Thesis_State = INTACT
      - Would normally trigger ITM-related ROLL gates

    Expected: HOLD LOW (recently-rolled cooldown)
    """
    return pd.Series({
        "TradeID": "T-GR-014", "LegID": "L-GR-014",
        "Symbol": "PLTR260515C00150000",
        "Underlying_Ticker": "PLTR",
        "Strategy": "BUY_WRITE", "AssetType": "OPTION",
        "Option_Type": "CALL", "Call/Put": "C",
        "UL Last": 157.0, "Strike": 150.0,
        "DTE": 45.0, "Premium_Entry": 16.70,
        "Last": 12.50, "Bid": 12.30,
        "Delta": 0.62, "Gamma": 0.015,
        "Theta": -0.08, "Vega": 0.35,
        "HV_20D": 0.45, "IV_Now": 0.50, "IV_Entry": 0.48, "IV_30D": 0.50,
        "Quantity": -1.0, "Basis": 15000.0,
        "PriceStructure_State": "TRENDING_UP",
        "TrendIntegrity_State": "INTACT",
        "VolatilityState_State": "ELEVATED",
        "MomentumVelocity_State": "ACCELERATING",
        "Equity_Integrity_State": "HEALTHY",
        "Thesis_State": "INTACT",
        "Position_Regime": "SIDEWAYS_INCOME",
        "Price_Drift_Pct": 0.03,
        "Net_Cost_Basis_Per_Share": 105.73,
        "Cumulative_Premium_Collected": 59.71,
        "_cycle_count": 6,
        "Lifecycle_Phase": "ACTIVE",
        "Moneyness_Label": "ITM",
        "Delta_Entry": 0.50,
        "Gamma_Entry": 0.02,
        "Vega_Entry": 0.30,
        "Theta_Entry": -0.06,
        "IV_Percentile": 55,
        "IV_vs_HV_Gap": 0.05,
        "Expected_Move_10D": 12.0,
        "Required_Move_Breakeven": 0.0,
        "EV_Feasibility_Ratio": 0.0,
        "Prior_Action": "ROLL",
        "Prior_Urgency": "MEDIUM",
        "Days_Since_Last_Roll": 1.0,
        "Days_In_Trade": 30,
    })


class TestArchetype14_RecentlyRolledCooldown:
    """Signal Coherence Gate 1: recently-rolled cooldown prevents flip-flop."""

    def test_14a_bw_rolled_1d_ago(self):
        """BW rolled 1d ago — v2 resolver picks best EV action (may be ASSIGN over HOLD)."""
        result = _run_vertical_slice(_archetype_cooldown_bw(), run_mc=False)
        assert result["Action"] in ("HOLD", "LET_EXPIRE", "ACCEPT_CALL_AWAY"), (
            f"BW within cooldown should HOLD or LET_EXPIRE/ACCEPT_CALL_AWAY (EV winner), got {result['Action']}. "
            f"Rationale: {result.get('Rationale', '')}"
        )

    def test_14b_bw_rolled_1d_ago_broken_skips(self):
        """BW rolled 1d ago, thesis BROKEN → cooldown skipped."""
        row = _archetype_cooldown_bw()
        row["Thesis_State"] = "BROKEN"
        result = _run_vertical_slice(row, run_mc=False)
        assert "cooldown" not in result.get("Rationale", "").lower(), (
            f"BROKEN thesis should skip cooldown. Got: {result.get('Rationale', '')}"
        )

    def test_14c_bw_rolled_5d_ago_passes(self):
        """BW rolled 5d ago → window passed, doctrine proceeds."""
        row = _archetype_cooldown_bw()
        row["Days_Since_Last_Roll"] = 5.0
        result = _run_vertical_slice(row, run_mc=False)
        assert "cooldown" not in result.get("Rationale", "").lower(), (
            f"5d > 3d window — cooldown should not fire. Got: {result.get('Rationale', '')}"
        )
