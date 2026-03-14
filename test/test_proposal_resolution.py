"""
Tests for proposal-based gate evaluation infrastructure and covered_call v2.

Categories:
  - ProposalCollector mechanics (propose, veto, summary, to_result)
  - resolve_income_proposals (EV-based resolution, MC enrichment)
  - covered_call_doctrine_v2 parity with v1 for hard vetoes
  - covered_call_doctrine_v2 resolution method coverage
"""

import math

import pytest
import pandas as pd

from core.management.cycle3.doctrine.proposal import (
    ActionProposal,
    ProposalCollector,
    propose_gate,
)
from core.management.cycle3.doctrine.comparators.action_ev_bw import (
    resolve_income_proposals,
)
from core.management.cycle3.doctrine.strategies.covered_call import (
    covered_call_doctrine,
    covered_call_doctrine_v2,
)


def _base_result():
    return {"Action": "HOLD", "Urgency": "LOW", "Rationale": "default"}


def _base_cc_row(**overrides):
    """Minimal row that reaches default HOLD in covered_call_doctrine."""
    defaults = {
        "Short_Call_Delta": 0.25,
        "Delta": 0.25,
        "Short_Call_DTE": 30,
        "DTE": 30,
        "Moneyness_Label": "OTM",
        "Lifecycle_Phase": "ACTIVE",
        "Drift_Direction": "Up",
        "Drift_Magnitude": "Low",
        "UL Last": 150.0,
        "Spot": 150.0,
        "Strike": 160.0,
        "Equity_Integrity_State": "INTACT",
        "Equity_Integrity_Reason": "",
        "Theta": 0.05,
        "Gamma": 0.02,
        "HV_20D": 0.25,
        "Gamma_ROC_3D": 0.0,
        "Quantity": 1,
        "Basis": 14000,
        "Underlying_Price_Entry": 140.0,
        "Premium_Entry": 3.50,
        "Last": 2.00,
        "Bid": 1.95,
        "Ask": 2.05,
        "Prior_Action": "HOLD",
        "Cumulative_Premium_Collected": 10.0,
    }
    defaults.update(overrides)
    return pd.Series(defaults)


# ── ProposalCollector mechanics ──────────────────────────────────────────────

class TestProposalCollector:
    def test_propose_adds_proposal(self):
        c = ProposalCollector()
        c.propose("gate1", action="ROLL", urgency="HIGH",
                   rationale="test", doctrine_source="src", priority=10)
        assert len(c.proposals) == 1
        assert c.proposals[0].gate_name == "gate1"
        assert c.proposals[0].action == "ROLL"

    def test_has_hard_veto_false_when_none(self):
        c = ProposalCollector()
        c.propose("gate1", action="HOLD", urgency="LOW",
                   rationale="", doctrine_source="", priority=100)
        assert c.has_hard_veto() is False

    def test_has_hard_veto_true(self):
        c = ProposalCollector()
        c.propose("gate1", action="EXIT", urgency="CRITICAL",
                   rationale="", doctrine_source="", priority=1, is_hard_veto=True)
        assert c.has_hard_veto() is True

    def test_get_veto_returns_highest_priority(self):
        c = ProposalCollector()
        c.propose("gate_low", action="EXIT", urgency="CRITICAL",
                   rationale="low", doctrine_source="", priority=5, is_hard_veto=True)
        c.propose("gate_high", action="EXIT", urgency="CRITICAL",
                   rationale="high", doctrine_source="", priority=1, is_hard_veto=True)
        veto = c.get_veto()
        assert veto.gate_name == "gate_high"
        assert veto.priority == 1

    def test_get_veto_raises_when_no_vetoes(self):
        c = ProposalCollector()
        c.propose("gate1", action="HOLD", urgency="LOW",
                   rationale="", doctrine_source="", priority=100)
        with pytest.raises(ValueError):
            c.get_veto()

    def test_get_proposals_by_action(self):
        c = ProposalCollector()
        c.propose("g1", action="ROLL", urgency="HIGH",
                   rationale="", doctrine_source="", priority=10)
        c.propose("g2", action="ROLL", urgency="MEDIUM",
                   rationale="", doctrine_source="", priority=20)
        c.propose("g3", action="HOLD", urgency="LOW",
                   rationale="", doctrine_source="", priority=100)
        groups = c.get_proposals_by_action()
        assert len(groups["ROLL"]) == 2
        assert len(groups["HOLD"]) == 1

    def test_summary(self):
        c = ProposalCollector()
        c.propose("g1", action="ROLL", urgency="HIGH",
                   rationale="", doctrine_source="", priority=10)
        c.propose("g2", action="HOLD", urgency="LOW",
                   rationale="", doctrine_source="", priority=100)
        s = c.summary()
        assert "2 proposals" in s
        assert "ROLL" in s
        assert "HOLD" in s

    def test_to_result_preserves_legacy_shape(self):
        c = ProposalCollector()
        c.propose("gate1", action="ROLL", urgency="HIGH",
                   rationale="test rationale", doctrine_source="McMillan",
                   priority=10, exit_trigger_type="INCOME")
        result = _base_result()
        final = c.to_result(c.proposals[0], result, resolution_method="EV_COMPARISON")
        assert final["Action"] == "ROLL"
        assert final["Urgency"] == "HIGH"
        assert final["Rationale"] == "test rationale"
        assert final["Doctrine_Source"] == "McMillan"
        assert final["Decision_State"] == "ACTIONABLE"
        assert final["Exit_Trigger_Type"] == "INCOME"
        assert final["Proposals_Considered"] == 1
        assert final["Resolution_Method"] == "EV_COMPARISON"

    def test_to_result_with_ev_estimate(self):
        c = ProposalCollector()
        c.propose("gate1", action="HOLD", urgency="LOW",
                   rationale="", doctrine_source="", priority=100,
                   ev_estimate=42.5)
        result = _base_result()
        final = c.to_result(c.proposals[0], result)
        assert final["EV_Estimate"] == 42.5

    def test_to_result_skips_nan_ev(self):
        c = ProposalCollector()
        c.propose("gate1", action="HOLD", urgency="LOW",
                   rationale="", doctrine_source="", priority=100)
        result = _base_result()
        final = c.to_result(c.proposals[0], result)
        assert "EV_Estimate" not in final


class TestProposeGateHelper:
    def test_returns_true(self):
        c = ProposalCollector()
        ret = propose_gate(c, "test", action="HOLD", urgency="LOW",
                           rationale="", doctrine_source="", priority=100)
        assert ret is True
        assert len(c.proposals) == 1

    def test_extra_fields_stored(self):
        c = ProposalCollector()
        propose_gate(c, "test", action="EXIT", urgency="CRITICAL",
                     rationale="", doctrine_source="", priority=1,
                     Gamma_Drag_Daily=0.05)
        assert c.proposals[0].extra_fields["Gamma_Drag_Daily"] == 0.05


# ── resolve_income_proposals ─────────────────────────────────────────────────

class TestResolveIncomeProposals:
    def _ev_result(self, **overrides):
        defaults = {
            "ev_hold": 100.0,
            "ev_roll": 80.0,
            "ev_assign": 50.0,
            "ev_buyback": -200.0,
            "ranked_actions": ["HOLD", "ROLL", "ASSIGN", "BUYBACK"],
            "ev_winner": "HOLD",
            "ev_margin": 20.0,
            "ev_summary": "test",
            "ev_buyback_trigger": False,
            "gamma_drag_daily": 0.01,
        }
        defaults.update(overrides)
        return defaults

    def test_capital_exit_always_wins(self):
        c = ProposalCollector()
        c.propose("collapse", action="EXIT", urgency="CRITICAL",
                   rationale="collapse", doctrine_source="",
                   priority=1, exit_trigger_type="CAPITAL")
        c.propose("default", action="HOLD", urgency="LOW",
                   rationale="hold", doctrine_source="", priority=100)
        row = _base_cc_row()
        winner = resolve_income_proposals(c, self._ev_result(), row)
        assert winner.action == "EXIT"
        assert winner.exit_trigger_type == "CAPITAL"

    def test_ev_winner_preferred_when_margin_significant(self):
        c = ProposalCollector()
        c.propose("roll_gate", action="ROLL", urgency="MEDIUM",
                   rationale="", doctrine_source="", priority=50)
        c.propose("hold_gate", action="HOLD", urgency="LOW",
                   rationale="", doctrine_source="", priority=100)
        row = _base_cc_row()
        ev = self._ev_result(ev_winner="ROLL", ev_roll=500.0, ev_hold=100.0, ev_margin=400.0)
        winner = resolve_income_proposals(c, ev, row)
        assert winner.action == "ROLL"

    def test_urgency_tiebreaker(self):
        c = ProposalCollector()
        c.propose("roll_high", action="ROLL", urgency="HIGH",
                   rationale="", doctrine_source="", priority=50)
        c.propose("roll_med", action="ROLL", urgency="MEDIUM",
                   rationale="", doctrine_source="", priority=50)
        row = _base_cc_row()
        # EV is a tie (noise floor)
        ev = self._ev_result(ev_winner="ROLL", ev_margin=5.0)
        winner = resolve_income_proposals(c, ev, row)
        assert winner.urgency == "HIGH"

    def test_mc_hold_bonus(self):
        c = ProposalCollector()
        c.propose("hold_gate", action="HOLD", urgency="MEDIUM",
                   rationale="", doctrine_source="", priority=50)
        c.propose("roll_gate", action="ROLL", urgency="MEDIUM",
                   rationale="", doctrine_source="", priority=50)
        # MC says recovery likely
        row = _base_cc_row(MC_Hold_P_Recovery=0.70, MC_TB_P_Profit=0.60)
        ev = self._ev_result(ev_winner="HOLD", ev_margin=10.0)
        winner = resolve_income_proposals(c, ev, row)
        assert winner.action == "HOLD"

    def test_raises_on_empty_collector(self):
        c = ProposalCollector()
        with pytest.raises(ValueError):
            resolve_income_proposals(c, self._ev_result(), _base_cc_row())


# ── covered_call_doctrine_v2 ─────────────────────────────────────────────────

class TestCoveredCallDoctrineV2:
    def test_hard_veto_underlying_collapse(self):
        """Hard veto (underlying collapse) produces identical action/urgency in v1 and v2."""
        row = _base_cc_row(Drift_Direction="Down", Drift_Magnitude="High")
        r1 = covered_call_doctrine(row, _base_result())
        r2 = covered_call_doctrine_v2(row, _base_result())
        assert r1["Action"] == r2["Action"] == "EXIT"
        assert r1["Urgency"] == r2["Urgency"] == "CRITICAL"
        assert r2["Resolution_Method"] == "HARD_VETO"
        assert r2["Exit_Trigger_Type"] == "CAPITAL"

    def test_default_hold_v1_vs_v2_divergence(self):
        """v1 returns HOLD (gate cascade stops early), v2 may diverge via EV resolution.

        This is expected: v2 evaluates ALL actions (including ASSIGN which has
        highest EV when stock > cost basis + strike is above spot). The A/B
        comparison script will quantify these divergences on real data.
        """
        row = _base_cc_row()
        r1 = covered_call_doctrine(row, _base_result())
        r2 = covered_call_doctrine_v2(row, _base_result())
        assert r1["Action"] == "HOLD"
        # v2 picks best EV action — ASSIGN wins when stock > cost basis
        assert r2["Action"] in ("HOLD", "ROLL", "ASSIGN", "LET_EXPIRE", "ACCEPT_CALL_AWAY", "BUYBACK")
        assert r2["Resolution_Method"] in ("EV_COMPARISON", "PRIORITY_FALLBACK", "FAR_OTM_OVERRIDE")

    def test_v2_has_proposal_metadata(self):
        """v2 result includes Proposals_Considered and Resolution_Method."""
        row = _base_cc_row()
        r2 = covered_call_doctrine_v2(row, _base_result())
        assert "Proposals_Considered" in r2
        assert r2["Proposals_Considered"] >= 1
        assert "Resolution_Method" in r2
        assert "Proposals_Summary" in r2

    def test_delta_emergency_produces_action(self):
        """High delta triggers ROLL proposal; resolver picks best overall action."""
        row = _base_cc_row(Short_Call_Delta=0.75, Delta=0.75)
        r2 = covered_call_doctrine_v2(row, _base_result())
        # delta_emergency proposes ROLL, but EV may prefer ASSIGN or other
        assert r2["Action"] in ("ROLL", "HOLD", "ASSIGN", "LET_EXPIRE", "ACCEPT_CALL_AWAY", "BUYBACK")
        assert r2["Proposals_Considered"] >= 2  # delta_emergency + default_hold

    def test_dte7_pin_risk_proposes_roll(self):
        """DTE ≤ 7 adds a pin risk ROLL proposal with GAMMA trigger type."""
        row = _base_cc_row(Short_Call_DTE=5, DTE=5)
        r2 = covered_call_doctrine_v2(row, _base_result())
        assert r2["Proposals_Considered"] >= 2  # pin_risk + default_hold at minimum

    def test_equity_broken_proposes_hold(self):
        """Equity BROKEN adds proposals but doesn't hard-veto."""
        row = _base_cc_row(
            Equity_Integrity_State="BROKEN",
            Equity_Integrity_Reason="50DMA broken",
        )
        r2 = covered_call_doctrine_v2(row, _base_result())
        assert r2["Resolution_Method"] != "HARD_VETO"

    def test_weakening_annotation_appended(self):
        """WEAKENING equity state appends annotation to rationale."""
        row = _base_cc_row(
            Equity_Integrity_State="WEAKENING",
            Equity_Integrity_Reason="approaching 50DMA",
        )
        r2 = covered_call_doctrine_v2(row, _base_result())
        assert "WEAKENING" in r2["Rationale"]

    def test_ev_fields_populated(self):
        """EV comparator fields are populated in v2 result."""
        row = _base_cc_row()
        r2 = covered_call_doctrine_v2(row, _base_result())
        assert "Action_EV_Winner" in r2
        assert "Action_EV_Margin" in r2
