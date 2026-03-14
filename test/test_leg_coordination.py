"""
Leg Coordination Tests — prevent naked short from split-action BUY_WRITE/CC exits.

Validates coordinate_multi_leg_actions() in orchestrator.py:
- STOCK=EXIT + OPTION=HOLD → OPTION escalated to EXIT
- STOCK=EXIT + OPTION=ROLL → OPTION escalated to EXIT
- STOCK=EXIT + OPTION=EXIT → no change (already coherent)
- STOCK=EXIT + OPTION=BUYBACK → no change (already unwinding)
- STOCK=HOLD + OPTION=EXIT → no change (holding stock alone is fine)
- Single-leg trades → untouched
- Non-BUY_WRITE strategies → untouched
- Urgency inheritance: option gets at least the stock leg's urgency

Run:
    pytest test/test_leg_coordination.py -v
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.management.cycle3.doctrine.orchestrator import coordinate_multi_leg_actions


def _make_df(rows: list[dict]) -> pd.DataFrame:
    """Build a minimal DataFrame from row dicts."""
    defaults = {
        'Rationale': 'test rationale',
        'Winning_Gate': 'test_gate',
        'Leg_Coordination_Override': False,
        'Doctrine_Source': '',
    }
    for r in rows:
        for k, v in defaults.items():
            r.setdefault(k, v)
    return pd.DataFrame(rows)


# ── Core: STOCK=EXIT + OPTION=HOLD → escalate ──────────────────────────


class TestStockExitOptionHold:
    """Structural EXIT (hard stop) — naked-short danger: stock exits, option must follow."""

    def test_option_escalated_to_exit_on_structural(self):
        """Hard stop EXIT must escalate option leg to prevent naked short."""
        df = _make_df([
            {'TradeID': 'T1', 'AssetType': 'STOCK', 'Strategy': 'BUY_WRITE',
             'Action': 'EXIT', 'Urgency': 'CRITICAL',
             'Doctrine_Source': 'McMillan Ch.3: Hard Stop'},
            {'TradeID': 'T1', 'AssetType': 'OPTION', 'Strategy': 'BUY_WRITE',
             'Action': 'HOLD', 'Urgency': 'LOW'},
        ])
        result = coordinate_multi_leg_actions(df)
        opt = result[result['AssetType'] == 'OPTION'].iloc[0]
        assert opt['Action'] == 'EXIT'
        assert opt['Leg_Coordination_Override'] == True
        assert opt['Winning_Gate'] == 'leg_coordination_exit'

    def test_urgency_inherits_from_stock_when_higher(self):
        df = _make_df([
            {'TradeID': 'T1', 'AssetType': 'STOCK', 'Strategy': 'BUY_WRITE',
             'Action': 'EXIT', 'Urgency': 'CRITICAL',
             'Doctrine_Source': 'McMillan Ch.3: Hard Stop'},
            {'TradeID': 'T1', 'AssetType': 'OPTION', 'Strategy': 'BUY_WRITE',
             'Action': 'HOLD', 'Urgency': 'LOW'},
        ])
        result = coordinate_multi_leg_actions(df)
        opt = result[result['AssetType'] == 'OPTION'].iloc[0]
        assert opt['Urgency'] == 'CRITICAL'

    def test_urgency_keeps_option_when_higher(self):
        """If option already has higher urgency, don't downgrade."""
        df = _make_df([
            {'TradeID': 'T1', 'AssetType': 'STOCK', 'Strategy': 'BUY_WRITE',
             'Action': 'EXIT', 'Urgency': 'MEDIUM',
             'Doctrine_Source': 'McMillan Ch.1: Deep Loss Stop — EXIT'},
            {'TradeID': 'T1', 'AssetType': 'OPTION', 'Strategy': 'BUY_WRITE',
             'Action': 'HOLD', 'Urgency': 'HIGH'},
        ])
        result = coordinate_multi_leg_actions(df)
        opt = result[result['AssetType'] == 'OPTION'].iloc[0]
        assert opt['Urgency'] == 'HIGH'

    def test_rationale_includes_coordination_note(self):
        df = _make_df([
            {'TradeID': 'T1', 'AssetType': 'STOCK', 'Strategy': 'BUY_WRITE',
             'Action': 'EXIT', 'Urgency': 'HIGH',
             'Doctrine_Source': 'McMillan Ch.3: Hard Stop'},
            {'TradeID': 'T1', 'AssetType': 'OPTION', 'Strategy': 'BUY_WRITE',
             'Action': 'HOLD', 'Urgency': 'LOW'},
        ])
        result = coordinate_multi_leg_actions(df)
        opt = result[result['AssetType'] == 'OPTION'].iloc[0]
        assert 'Leg coordination override' in opt['Rationale']
        assert 'naked short' in opt['Rationale']

    def test_stock_leg_unchanged_on_structural(self):
        """Structural EXIT: coordination modifies only the option leg."""
        df = _make_df([
            {'TradeID': 'T1', 'AssetType': 'STOCK', 'Strategy': 'BUY_WRITE',
             'Action': 'EXIT', 'Urgency': 'CRITICAL',
             'Doctrine_Source': 'McMillan Ch.3: Hard Stop'},
            {'TradeID': 'T1', 'AssetType': 'OPTION', 'Strategy': 'BUY_WRITE',
             'Action': 'HOLD', 'Urgency': 'LOW'},
        ])
        result = coordinate_multi_leg_actions(df)
        stk = result[result['AssetType'] == 'STOCK'].iloc[0]
        assert stk['Action'] == 'EXIT'
        assert stk['Urgency'] == 'CRITICAL'
        assert stk['Leg_Coordination_Override'] == False


# ── STOCK=EXIT + OPTION=ROLL → escalate ─────────────────────────────────


class TestStockExitOptionRoll:
    """Structural EXIT + option ROLL: rolling is unsafe if stock is truly being sold."""

    def test_roll_escalated_to_exit_on_structural(self):
        """Hard stop EXIT must override option ROLL to prevent naked short."""
        df = _make_df([
            {'TradeID': 'T1', 'AssetType': 'STOCK', 'Strategy': 'BUY_WRITE',
             'Action': 'EXIT', 'Urgency': 'HIGH',
             'Doctrine_Source': 'McMillan Ch.3: Hard Stop'},
            {'TradeID': 'T1', 'AssetType': 'OPTION', 'Strategy': 'BUY_WRITE',
             'Action': 'ROLL', 'Urgency': 'MEDIUM'},
        ])
        result = coordinate_multi_leg_actions(df)
        opt = result[result['AssetType'] == 'OPTION'].iloc[0]
        assert opt['Action'] == 'EXIT'
        assert opt['Leg_Coordination_Override'] == True


# ── No-change cases ─────────────────────────────────────────────────────


class TestNoChangeScenarios:
    """Cases where coordination should NOT modify anything."""

    def test_both_legs_exit(self):
        df = _make_df([
            {'TradeID': 'T1', 'AssetType': 'STOCK', 'Strategy': 'BUY_WRITE',
             'Action': 'EXIT', 'Urgency': 'CRITICAL'},
            {'TradeID': 'T1', 'AssetType': 'OPTION', 'Strategy': 'BUY_WRITE',
             'Action': 'EXIT', 'Urgency': 'HIGH'},
        ])
        result = coordinate_multi_leg_actions(df)
        opt = result[result['AssetType'] == 'OPTION'].iloc[0]
        assert opt['Action'] == 'EXIT'
        assert opt['Urgency'] == 'HIGH'  # not changed
        assert opt['Leg_Coordination_Override'] == False

    def test_option_buyback_not_escalated(self):
        df = _make_df([
            {'TradeID': 'T1', 'AssetType': 'STOCK', 'Strategy': 'BUY_WRITE',
             'Action': 'EXIT', 'Urgency': 'CRITICAL'},
            {'TradeID': 'T1', 'AssetType': 'OPTION', 'Strategy': 'BUY_WRITE',
             'Action': 'BUYBACK', 'Urgency': 'HIGH'},
        ])
        result = coordinate_multi_leg_actions(df)
        opt = result[result['AssetType'] == 'OPTION'].iloc[0]
        assert opt['Action'] == 'BUYBACK'
        assert opt['Leg_Coordination_Override'] == False

    def test_stock_hold_option_exit_no_change(self):
        """Selling the call while holding stock is fine (unencumbered stock)."""
        df = _make_df([
            {'TradeID': 'T1', 'AssetType': 'STOCK', 'Strategy': 'BUY_WRITE',
             'Action': 'HOLD', 'Urgency': 'LOW'},
            {'TradeID': 'T1', 'AssetType': 'OPTION', 'Strategy': 'BUY_WRITE',
             'Action': 'EXIT', 'Urgency': 'HIGH'},
        ])
        result = coordinate_multi_leg_actions(df)
        opt = result[result['AssetType'] == 'OPTION'].iloc[0]
        assert opt['Action'] == 'EXIT'
        assert opt['Leg_Coordination_Override'] == False
        stk = result[result['AssetType'] == 'STOCK'].iloc[0]
        assert stk['Action'] == 'HOLD'

    def test_both_hold_no_change(self):
        df = _make_df([
            {'TradeID': 'T1', 'AssetType': 'STOCK', 'Strategy': 'BUY_WRITE',
             'Action': 'HOLD', 'Urgency': 'LOW'},
            {'TradeID': 'T1', 'AssetType': 'OPTION', 'Strategy': 'BUY_WRITE',
             'Action': 'HOLD', 'Urgency': 'LOW'},
        ])
        result = coordinate_multi_leg_actions(df)
        assert (result['Action'] == 'HOLD').all()
        assert (result['Leg_Coordination_Override'] == False).all()


# ── Strategy scoping ─────────────────────────────────────────────────────


class TestStrategyScoping:
    """Only BUY_WRITE and COVERED_CALL are coordinated."""

    def test_covered_call_coordinated_structural(self):
        """Structural EXIT on CC stock leg must escalate option to EXIT."""
        df = _make_df([
            {'TradeID': 'T1', 'AssetType': 'STOCK', 'Strategy': 'COVERED_CALL',
             'Action': 'EXIT', 'Urgency': 'HIGH',
             'Doctrine_Source': 'McMillan Ch.1: Deep Loss Stop — EXIT'},
            {'TradeID': 'T1', 'AssetType': 'OPTION', 'Strategy': 'COVERED_CALL',
             'Action': 'HOLD', 'Urgency': 'LOW'},
        ])
        result = coordinate_multi_leg_actions(df)
        opt = result[result['AssetType'] == 'OPTION'].iloc[0]
        assert opt['Action'] == 'EXIT'

    def test_straddle_not_coordinated(self):
        """STRADDLE: two option legs, no stock — coordination doesn't apply."""
        df = _make_df([
            {'TradeID': 'T1', 'AssetType': 'OPTION', 'Strategy': 'STRADDLE',
             'Action': 'EXIT', 'Urgency': 'HIGH'},
            {'TradeID': 'T1', 'AssetType': 'OPTION', 'Strategy': 'STRADDLE',
             'Action': 'HOLD', 'Urgency': 'LOW'},
        ])
        result = coordinate_multi_leg_actions(df)
        # No stock leg → no coordination → both unchanged
        actions = result['Action'].tolist()
        assert actions == ['EXIT', 'HOLD']

    def test_single_leg_untouched(self):
        df = _make_df([
            {'TradeID': 'T1', 'AssetType': 'OPTION', 'Strategy': 'BUY_CALL',
             'Action': 'EXIT', 'Urgency': 'HIGH'},
        ])
        result = coordinate_multi_leg_actions(df)
        assert result.iloc[0]['Action'] == 'EXIT'
        assert result.iloc[0]['Leg_Coordination_Override'] == False

    def test_stock_only_untouched(self):
        df = _make_df([
            {'TradeID': 'T1', 'AssetType': 'STOCK', 'Strategy': 'STOCK_ONLY',
             'Action': 'EXIT', 'Urgency': 'MEDIUM'},
        ])
        result = coordinate_multi_leg_actions(df)
        assert result.iloc[0]['Action'] == 'EXIT'


# ── Multi-trade isolation ────────────────────────────────────────────────


class TestMultiTradeIsolation:
    """Coordination for one trade must not affect another."""

    def test_different_trades_independent(self):
        df = _make_df([
            # Trade 1: STOCK=EXIT (structural), OPTION=HOLD → should escalate
            {'TradeID': 'T1', 'AssetType': 'STOCK', 'Strategy': 'BUY_WRITE',
             'Action': 'EXIT', 'Urgency': 'CRITICAL',
             'Doctrine_Source': 'McMillan Ch.3: Hard Stop'},
            {'TradeID': 'T1', 'AssetType': 'OPTION', 'Strategy': 'BUY_WRITE',
             'Action': 'HOLD', 'Urgency': 'LOW'},
            # Trade 2: STOCK=HOLD, OPTION=HOLD → should NOT change
            {'TradeID': 'T2', 'AssetType': 'STOCK', 'Strategy': 'BUY_WRITE',
             'Action': 'HOLD', 'Urgency': 'LOW'},
            {'TradeID': 'T2', 'AssetType': 'OPTION', 'Strategy': 'BUY_WRITE',
             'Action': 'HOLD', 'Urgency': 'LOW'},
        ])
        result = coordinate_multi_leg_actions(df)

        t1_opt = result[(result['TradeID'] == 'T1') & (result['AssetType'] == 'OPTION')].iloc[0]
        assert t1_opt['Action'] == 'EXIT'

        t2_opt = result[(result['TradeID'] == 'T2') & (result['AssetType'] == 'OPTION')].iloc[0]
        assert t2_opt['Action'] == 'HOLD'
        assert t2_opt['Leg_Coordination_Override'] == False


# ── Edge cases ───────────────────────────────────────────────────────────


class TestEdgeCases:
    """Defensive edge cases."""

    def test_empty_dataframe(self):
        df = pd.DataFrame(columns=['TradeID', 'AssetType', 'Strategy', 'Action', 'Urgency'])
        result = coordinate_multi_leg_actions(df)
        assert len(result) == 0

    def test_missing_columns_no_crash(self):
        """DataFrame without TradeID/AssetType → returns unchanged."""
        df = pd.DataFrame({'Action': ['HOLD'], 'Urgency': ['LOW']})
        result = coordinate_multi_leg_actions(df)
        assert result.iloc[0]['Action'] == 'HOLD'

    def test_nan_action_treated_as_no_conflict(self):
        """NaN actions shouldn't crash the coordination pass."""
        df = _make_df([
            {'TradeID': 'T1', 'AssetType': 'STOCK', 'Strategy': 'BUY_WRITE',
             'Action': None, 'Urgency': 'LOW'},
            {'TradeID': 'T1', 'AssetType': 'OPTION', 'Strategy': 'BUY_WRITE',
             'Action': 'HOLD', 'Urgency': 'LOW'},
        ])
        result = coordinate_multi_leg_actions(df)
        # Stock action is None (not EXIT) → no escalation
        opt = result[result['AssetType'] == 'OPTION'].iloc[0]
        assert opt['Action'] == 'HOLD'

    def test_eose_real_scenario(self):
        """EOSE BUY_WRITE: hard_stop_exit is structural → escalate option."""
        df = _make_df([
            {'TradeID': 'EOSE260417_9p0_CC_5376',
             'AssetType': 'STOCK', 'Strategy': 'BUY_WRITE',
             'Action': 'EXIT', 'Urgency': 'CRITICAL',
             'Winning_Gate': 'hard_stop_exit',
             'Doctrine_Source': 'McMillan Ch.3: Hard Stop',
             'Rationale': 'Structural breakdown'},
            {'TradeID': 'EOSE260417_9p0_CC_5376',
             'AssetType': 'OPTION', 'Strategy': 'BUY_WRITE',
             'Action': 'HOLD', 'Urgency': 'MEDIUM',
             'Winning_Gate': 'hard_stop_recovery_ladder'},
        ])
        result = coordinate_multi_leg_actions(df)
        opt = result[result['AssetType'] == 'OPTION'].iloc[0]
        assert opt['Action'] == 'EXIT'
        assert opt['Urgency'] == 'CRITICAL'  # inherited from stock
        assert 'hard_stop_recovery_ladder' in opt['Rationale']
        assert opt['Winning_Gate'] == 'leg_coordination_exit'

    def test_dkng_real_scenario_income_override(self):
        """DKNG BUY_WRITE: non-structural stock EXIT + option ROLL → income override.

        The option leg (buy_write doctrine) says ROLL based on EV comparison.
        The stock leg says EXIT based on raw P&L (doesn't see premium credit).
        The option leg is the income authority — stock EXIT should be downgraded.
        """
        df = _make_df([
            {'TradeID': 'DKNG260417_27p5_CC_5376',
             'AssetType': 'STOCK', 'Strategy': 'BUY_WRITE',
             'Action': 'EXIT', 'Urgency': 'MEDIUM',
             'Winning_Gate': 'equity_broken_neg_carry_exit',
             'Doctrine_Source': 'EquityIntegrity: BROKEN + Negative Carry → EXIT'},
            {'TradeID': 'DKNG260417_27p5_CC_5376',
             'AssetType': 'OPTION', 'Strategy': 'BUY_WRITE',
             'Action': 'ROLL', 'Urgency': 'HIGH',
             'Winning_Gate': 'ev_comparator_roll'},
        ])
        result = coordinate_multi_leg_actions(df)
        # Income structure override: stock EXIT downgraded, option ROLL preserved
        stk = result[result['AssetType'] == 'STOCK'].iloc[0]
        assert stk['Action'] == 'HOLD', "Stock EXIT should be downgraded when option says ROLL"
        assert stk['Leg_Coordination_Override'] == True
        assert 'Income structure override' in stk['Rationale']

        opt = result[result['AssetType'] == 'OPTION'].iloc[0]
        assert opt['Action'] == 'ROLL', "Option ROLL must be preserved (income authority)"
        assert opt['Leg_Coordination_Override'] == False


class TestIncomeStructureOverride:
    """Income structure guard: option leg (BW/CC doctrine) is authoritative."""

    def test_non_structural_exit_overridden_by_option_hold(self):
        """Stock EXIT from raw P&L + option HOLD → stock downgraded to HOLD."""
        df = _make_df([
            {'TradeID': 'T1', 'AssetType': 'STOCK', 'Strategy': 'BUY_WRITE',
             'Action': 'EXIT', 'Urgency': 'MEDIUM',
             'Doctrine_Source': 'Passarelli Ch.6: WEAKENING + Loss — HOLD MEDIUM'},
            {'TradeID': 'T1', 'AssetType': 'OPTION', 'Strategy': 'BUY_WRITE',
             'Action': 'HOLD', 'Urgency': 'LOW'},
        ])
        result = coordinate_multi_leg_actions(df)
        stk = result[result['AssetType'] == 'STOCK'].iloc[0]
        assert stk['Action'] == 'HOLD'
        assert 'Income structure override' in stk['Rationale']

        opt = result[result['AssetType'] == 'OPTION'].iloc[0]
        assert opt['Action'] == 'HOLD'  # preserved

    def test_hard_stop_still_escalates_option(self):
        """Hard stop EXIT is structural — must escalate option to EXIT."""
        df = _make_df([
            {'TradeID': 'T1', 'AssetType': 'STOCK', 'Strategy': 'BUY_WRITE',
             'Action': 'EXIT', 'Urgency': 'HIGH',
             'Doctrine_Source': 'McMillan Ch.3: Hard Stop'},
            {'TradeID': 'T1', 'AssetType': 'OPTION', 'Strategy': 'BUY_WRITE',
             'Action': 'ROLL', 'Urgency': 'MEDIUM'},
        ])
        result = coordinate_multi_leg_actions(df)
        opt = result[result['AssetType'] == 'OPTION'].iloc[0]
        assert opt['Action'] == 'EXIT', "Hard stop EXIT must override option ROLL"
        assert opt['Winning_Gate'] == 'leg_coordination_exit'

    def test_thesis_broken_still_escalates_option(self):
        """Thesis/story BROKEN EXIT is structural — must escalate option."""
        df = _make_df([
            {'TradeID': 'T1', 'AssetType': 'STOCK', 'Strategy': 'BUY_WRITE',
             'Action': 'EXIT', 'Urgency': 'HIGH',
             'Doctrine_Source': 'ThesisEngine: BROKEN story gate'},
            {'TradeID': 'T1', 'AssetType': 'OPTION', 'Strategy': 'BUY_WRITE',
             'Action': 'ROLL', 'Urgency': 'MEDIUM'},
        ])
        result = coordinate_multi_leg_actions(df)
        opt = result[result['AssetType'] == 'OPTION'].iloc[0]
        assert opt['Action'] == 'EXIT', "Thesis BROKEN must override option ROLL"

    def test_equity_integrity_broken_overridden_by_income(self):
        """EquityIntegrity BROKEN is price-based (raw cost), NOT structural.
        Income positions at net-cost breakeven after premium credits should
        NOT be force-exited when the option leg says ROLL."""
        df = _make_df([
            {'TradeID': 'T1', 'AssetType': 'STOCK', 'Strategy': 'BUY_WRITE',
             'Action': 'EXIT', 'Urgency': 'MEDIUM',
             'Doctrine_Source': 'EquityIntegrity: BROKEN + Negative Carry → EXIT'},
            {'TradeID': 'T1', 'AssetType': 'OPTION', 'Strategy': 'BUY_WRITE',
             'Action': 'ROLL', 'Urgency': 'HIGH',
             'Winning_Gate': 'ev_comparator_roll'},
        ])
        result = coordinate_multi_leg_actions(df)
        stk = result[result['AssetType'] == 'STOCK'].iloc[0]
        assert stk['Action'] == 'HOLD', "Equity integrity BROKEN is not structural for income positions"
        assert 'Income structure override' in stk['Rationale']
        opt = result[result['AssetType'] == 'OPTION'].iloc[0]
        assert opt['Action'] == 'ROLL', "Option ROLL preserved — income authority"

    def test_deep_loss_stop_still_escalates(self):
        """Deep loss stop EXIT is structural — must escalate option."""
        df = _make_df([
            {'TradeID': 'T1', 'AssetType': 'STOCK', 'Strategy': 'BUY_WRITE',
             'Action': 'EXIT', 'Urgency': 'HIGH',
             'Doctrine_Source': 'McMillan Ch.1: Deep Loss Stop — EXIT'},
            {'TradeID': 'T1', 'AssetType': 'OPTION', 'Strategy': 'BUY_WRITE',
             'Action': 'ROLL', 'Urgency': 'MEDIUM'},
        ])
        result = coordinate_multi_leg_actions(df)
        opt = result[result['AssetType'] == 'OPTION'].iloc[0]
        assert opt['Action'] == 'EXIT'

    def test_option_exit_no_income_override(self):
        """If option ALSO says EXIT, no income override needed — both exit."""
        df = _make_df([
            {'TradeID': 'T1', 'AssetType': 'STOCK', 'Strategy': 'BUY_WRITE',
             'Action': 'EXIT', 'Urgency': 'MEDIUM',
             'Doctrine_Source': 'test source'},
            {'TradeID': 'T1', 'AssetType': 'OPTION', 'Strategy': 'BUY_WRITE',
             'Action': 'EXIT', 'Urgency': 'HIGH'},
        ])
        result = coordinate_multi_leg_actions(df)
        # Both already EXIT — no coordination needed
        assert (result['Action'] == 'EXIT').all()

    def test_recovery_premium_actions_trigger_override(self):
        """Recovery Premium Mode actions (WRITE_NOW, HOLD_STOCK_WAIT) override stock EXIT."""
        df = _make_df([
            {'TradeID': 'T1', 'AssetType': 'STOCK', 'Strategy': 'BUY_WRITE',
             'Action': 'EXIT', 'Urgency': 'MEDIUM',
             'Doctrine_Source': 'test non-structural source'},
            {'TradeID': 'T1', 'AssetType': 'OPTION', 'Strategy': 'BUY_WRITE',
             'Action': 'WRITE_NOW', 'Urgency': 'MEDIUM'},
        ])
        result = coordinate_multi_leg_actions(df)
        stk = result[result['AssetType'] == 'STOCK'].iloc[0]
        assert stk['Action'] == 'HOLD'
        opt = result[result['AssetType'] == 'OPTION'].iloc[0]
        assert opt['Action'] == 'WRITE_NOW'

    def test_let_expire_triggers_income_override(self):
        """LET_EXPIRE = short call expires worthless (best income outcome).
        Stock EXIT should be overridden — the stock stays, call expires."""
        df = _make_df([
            {'TradeID': 'T1', 'AssetType': 'STOCK', 'Strategy': 'BUY_WRITE',
             'Action': 'EXIT', 'Urgency': 'MEDIUM',
             'Doctrine_Source': 'EquityIntegrity: BROKEN + Negative Carry → EXIT'},
            {'TradeID': 'T1', 'AssetType': 'OPTION', 'Strategy': 'BUY_WRITE',
             'Action': 'LET_EXPIRE', 'Urgency': 'MEDIUM',
             'Winning_Gate': 'ev_comparator_let_expire'},
        ])
        result = coordinate_multi_leg_actions(df)
        stk = result[result['AssetType'] == 'STOCK'].iloc[0]
        assert stk['Action'] == 'HOLD', "Stock EXIT should be overridden when option says LET_EXPIRE"
        assert 'Income structure override' in stk['Rationale']
        opt = result[result['AssetType'] == 'OPTION'].iloc[0]
        assert opt['Action'] == 'LET_EXPIRE', "LET_EXPIRE must be preserved"

    def test_accept_call_away_triggers_income_override(self):
        """ACCEPT_CALL_AWAY = profitable assignment at strike.
        Stock EXIT should be overridden — shares called away at strike price."""
        df = _make_df([
            {'TradeID': 'T1', 'AssetType': 'STOCK', 'Strategy': 'BUY_WRITE',
             'Action': 'EXIT', 'Urgency': 'MEDIUM',
             'Doctrine_Source': 'EquityIntegrity: BROKEN + Negative Carry → EXIT'},
            {'TradeID': 'T1', 'AssetType': 'OPTION', 'Strategy': 'BUY_WRITE',
             'Action': 'ACCEPT_CALL_AWAY', 'Urgency': 'LOW'},
        ])
        result = coordinate_multi_leg_actions(df)
        stk = result[result['AssetType'] == 'STOCK'].iloc[0]
        assert stk['Action'] == 'HOLD'
        opt = result[result['AssetType'] == 'OPTION'].iloc[0]
        assert opt['Action'] == 'ACCEPT_CALL_AWAY'
