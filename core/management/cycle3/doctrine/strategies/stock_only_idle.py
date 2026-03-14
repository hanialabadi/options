"""
STOCK_ONLY_IDLE doctrine — shares held with no covered call written against them.

The CC opportunity engine (cc_opportunity_engine.py) runs post-doctrine and
writes CC_Proposal_Status / CC_Proposal_Verdict / CC_Candidate_* columns.
Doctrine here sets the baseline action and urgency; the CC panel in
manage_view.py surfaces the opportunity details.

McMillan Ch.3: idle long stock is uncapped upside — only write calls when
the income opportunity clearly justifies the cap risk.
"""

from typing import Dict, Any

import pandas as pd

from ..gate_result import (
    fire_gate,
    STATE_ACTIONABLE,
    STATE_NEUTRAL_CONFIDENT,
)
from ..helpers import safe_row_float


def stock_only_idle_doctrine(row: pd.Series, result: Dict[str, Any]) -> Dict[str, Any]:
    """Evaluate STOCK_ONLY_IDLE position. Mutates and returns *result*."""
    ticker = str(row.get("Underlying_Ticker") or row.get("Symbol") or "ticker")
    qty = safe_row_float(row, "Quantity", "Qty")
    shares_label = f"{int(qty):,} shares" if qty > 0 else "shares"

    # Equity Integrity BROKEN guard — block CC proposals on structurally broken stock.
    # Natenberg Ch.8: "Never sell calls against a stock in structural breakdown."
    # McMillan Ch.3: "CC thesis requires the stock to be stable or range-bound."
    _idle_ei_state = str(row.get('Equity_Integrity_State', '') or '').strip()
    _idle_ei_reason = str(row.get('Equity_Integrity_Reason', '') or '').strip()
    if _idle_ei_state == 'BROKEN':
        return fire_gate(
            result,
            action="HOLD",
            urgency="HIGH",
            rationale=(
                f"\U0001f4e6 Idle stock: {ticker} ({shares_label}) — BLOCKED from CC evaluation. "
                f"Equity Integrity BROKEN: {_idle_ei_reason}. "
                f"Do NOT sell covered calls against a structurally declining stock — "
                f"you cap recovery upside while retaining full downside exposure. "
                f"Wait for structure to recover (price reclaim 20D MA + momentum inflection) "
                f"before writing calls. "
                f"(Natenberg Ch.8: CC premise requires stable/range-bound stock; "
                f"McMillan Ch.3: broken structure = CC income thesis invalid)"
            ),
            doctrine_source="EquityIntegrity: BROKEN — CC blocked for idle stock",
            decision_state=STATE_ACTIONABLE,
        )[1]

    # Check if CC engine already ran (post-doctrine ordering — defensive path)
    cc_status = str(row.get("CC_Proposal_Status") or "")
    cc_verdict = str(row.get("CC_Proposal_Verdict") or "")

    if cc_status == "FAVORABLE":
        action = "HOLD"
        urgency = "MEDIUM"
        rationale = (
            f"\U0001f4e6 Idle stock: {ticker} ({shares_label}) — no call written. "
            f"CC opportunity detected: {cc_verdict}. "
            f"Review CC candidates below before next session. "
            f"(McMillan Ch.3: idle stock earns zero theta — covered call converts "
            f"holding cost into income when IV conditions are right)"
        )
        state = STATE_ACTIONABLE
    elif cc_status == "UNFAVORABLE":
        action = "HOLD"
        urgency = "LOW"
        rationale = (
            f"\U0001f4e6 Idle stock: {ticker} ({shares_label}) — no call written. "
            f"CC not advisable now: {cc_verdict}. "
            f"Watch for: {row.get('CC_Watch_Signal', 'improved IV conditions')}. "
            f"(Natenberg Ch.8: sell calls only when IV_Rank > 20% — "
            f"selling in compressed vol gives away upside for thin premium)"
        )
        state = STATE_NEUTRAL_CONFIDENT
    else:
        # CC engine hasn't run yet (normal case — runs post-doctrine)
        action = "HOLD"
        urgency = "LOW"
        rationale = (
            f"\U0001f4e6 Idle stock: {ticker} ({shares_label}) — no covered call written. "
            f"CC opportunity assessment pending (evaluating scan engine output). "
            f"(McMillan Ch.3: {shares_label} of idle stock earns zero theta; "
            f"a covered call converts holding cost into income when conditions are right)"
        )
        state = STATE_NEUTRAL_CONFIDENT

    return fire_gate(
        result,
        action=action,
        urgency=urgency,
        rationale=rationale,
        doctrine_source="McMillan Ch.3: Idle Stock — CC Opportunity Assessment",
        decision_state=state,
    )[1]
