"""
STOCK_ONLY doctrine — shares held with no option overlay.

Gate order (first match returns):
  1. Equity BROKEN → EXIT HIGH (structural breakdown — Natenberg Ch.8)
  2. Deep Loss (≤-50%) → EXIT HIGH (capital preservation — McMillan Ch.1)
  3. Significant Loss (≤-25%) → HOLD HIGH (pure directional risk — Passarelli Ch.6)
  4. WEAKENING + Loss (<-10%) → HOLD MEDIUM (early deterioration — Natenberg Ch.8)
  5a. BW Upgrade (<100 shares) — buy remaining to 100, sell CC for income recovery
  5. CC Opportunity (≥100 shares, not BROKEN, loss < 25%) → HOLD LOW + CC note
  6. Default → HOLD LOW (if <100 shares and BW not feasible → close odd lot)
"""

import math
from typing import Dict, Any

import pandas as pd

from ..gate_result import (
    fire_gate,
    STATE_ACTIONABLE,
    STATE_NEUTRAL_CONFIDENT,
)
from ..helpers import safe_pnl_pct, safe_row_float
from ..thresholds import (
    PNL_DEEP_LOSS_STOP,
    PNL_SIGNIFICANT_LOSS,
    PNL_WEAKENING_LOSS,
    SHARES_CC_ELIGIBLE,
)

# ── BW Upgrade thresholds ────────────────────────────────────────────────────
_BW_UPGRADE_IV_MIN = 0.15          # need ≥15% IV to generate meaningful premium
_BW_UPGRADE_MAX_COST = 10_000.0    # cap additional capital at $10k
_BW_UPGRADE_MAX_PAYBACK = 12.0     # payback must be < 12 months
_BW_UPGRADE_MARGIN_RATE = 0.10375  # annual margin rate (10.375%)


def stock_only_doctrine(row: pd.Series, result: Dict[str, Any]) -> Dict[str, Any]:
    """Evaluate STOCK_ONLY position. Mutates and returns *result*."""
    ticker = str(row.get("Underlying_Ticker") or row.get("Symbol") or "ticker")
    qty = safe_row_float(row, "Quantity", "Qty")
    shares_label = f"{int(qty):,} shares" if qty > 0 else "shares"

    pnl_pct = safe_pnl_pct(row)
    pnl_dollars = float(row.get("PnL_Total", 0) or 0) if pd.notna(row.get("PnL_Total")) else None
    ei_state = str(row.get("Equity_Integrity_State", "") or "").strip()
    ei_reason = str(row.get("Equity_Integrity_Reason", "") or "").strip()

    pnl_str = f"{pnl_pct:+.1%}" if pnl_pct is not None else "N/A"
    pnl_dollar_str = f"${pnl_dollars:+,.0f}" if pnl_dollars is not None else ""

    # Gate 1: Equity BROKEN — structural breakdown
    if ei_state == "BROKEN":
        return fire_gate(
            result,
            action="EXIT",
            urgency="HIGH",
            rationale=(
                f"\U0001f534 Stock {ticker} ({shares_label}) — Equity Integrity BROKEN: {ei_reason}. "
                f"P&L: {pnl_str} {pnl_dollar_str}. "
                f"Structural breakdown on stock with no option hedge — full downside exposure. "
                f"(Natenberg Ch.8: structural breakdown is not cyclical; "
                f"McMillan Ch.1: capital preservation supersedes recovery hope)"
            ),
            doctrine_source="Natenberg Ch.8 + McMillan Ch.1: BROKEN Equity — EXIT",
            decision_state=STATE_ACTIONABLE,
        )[1]

    # Gate 2: Deep loss stop — capital preservation
    if pnl_pct is not None and pnl_pct <= PNL_DEEP_LOSS_STOP:
        # Forward-economics guard: can a CC overlay recover this position?
        # Stock at -50% is pure directional risk, but if IV supports premium
        # generation and position has enough shares, converting to BW is a
        # repair strategy (McMillan Ch.3: CC repair).
        if ei_state != "BROKEN" and qty >= SHARES_CC_ELIGIBLE:
            try:
                from ..helpers import compute_forward_income_economics
                from ..thresholds import FORWARD_ECON_MONTHS_STOCK_DEEP_LOSS, FORWARD_ECON_IV_MIN_VIABLE
                # Estimate cost basis from pnl_pct: cost = spot / (1 + pnl_pct)
                spot = safe_row_float(row, "UL Last", "Last", "Spot")
                _cost_est = spot / (1 + pnl_pct) if pnl_pct and pnl_pct != 0 and spot > 0 else 0
                if _cost_est > 0 and spot > 0:
                    _fe_so = compute_forward_income_economics(row, spot, _cost_est)
                    if (_fe_so["viable"]
                            and _fe_so["months_to_breakeven"] < FORWARD_ECON_MONTHS_STOCK_DEEP_LOSS):
                        return fire_gate(
                            result,
                            action="HOLD",
                            urgency="HIGH",
                            rationale=(
                                f"Forward-economics override: {ticker} ({shares_label}) "
                                f"at {pnl_str} {pnl_dollar_str} but CC overlay recovery "
                                f"viable. IV {_fe_so['iv_now']:.0%} → est. "
                                f"${_fe_so['net_monthly']:.2f}/sh/mo net income. "
                                f"~{_fe_so['months_to_breakeven']:.0f} months to close "
                                f"${_fe_so['gap_to_breakeven']:.2f}/sh gap via premium. "
                                f"Convert to BUY_WRITE and begin basis reduction. "
                                f"Equity: {ei_state or 'UNKNOWN'}. "
                                f"(McMillan Ch.3: CC repair strategy; Jabbour Ch.4: "
                                f"income path supersedes sunk-loss exit when forward "
                                f"breakeven < {FORWARD_ECON_MONTHS_STOCK_DEEP_LOSS}mo)"
                            ),
                            doctrine_source="McMillan Ch.3 + Jabbour Ch.4: Stock CC Repair Override",
                            decision_state=STATE_ACTIONABLE,
                        )[1]
            except Exception:
                pass  # Graceful fallback: continue to original EXIT

        return fire_gate(
            result,
            action="EXIT",
            urgency="HIGH",
            rationale=(
                f"\U0001f534 Stock {ticker} ({shares_label}) — deep loss {pnl_str} {pnl_dollar_str}. "
                f"Equity state: {ei_state or 'UNKNOWN'}. "
                f"No theta cushion, no hedge — pure directional risk at >50% drawdown. "
                f"(McMillan Ch.1: capital preservation supersedes recovery hope; "
                f"Passarelli Ch.6: unhedged stock at deep loss = sunk cost trap)"
            ),
            doctrine_source="McMillan Ch.1: Deep Loss Stop — EXIT",
            decision_state=STATE_ACTIONABLE,
        )[1]

    # Gate 3: Significant loss — elevated monitoring
    if pnl_pct is not None and pnl_pct <= PNL_SIGNIFICANT_LOSS:
        return fire_gate(
            result,
            action="HOLD",
            urgency="HIGH",
            rationale=(
                f"\u26a0\ufe0f Stock {ticker} ({shares_label}) — significant loss {pnl_str} {pnl_dollar_str}. "
                f"Equity state: {ei_state or 'UNKNOWN'}. "
                f"No theta cushion = pure directional risk. Monitor for further deterioration. "
                f"(Passarelli Ch.6: unhedged stock beyond -25% needs active review)"
            ),
            doctrine_source="Passarelli Ch.6: Significant Loss — HOLD HIGH",
            decision_state=STATE_ACTIONABLE,
        )[1]

    # Gate 4: WEAKENING equity + moderate loss
    if ei_state == "WEAKENING" and pnl_pct is not None and pnl_pct < PNL_WEAKENING_LOSS:
        return fire_gate(
            result,
            action="HOLD",
            urgency="MEDIUM",
            rationale=(
                f"\u26a0\ufe0f Stock {ticker} ({shares_label}) — WEAKENING equity at {pnl_str} {pnl_dollar_str}. "
                f"Reason: {ei_reason}. "
                f"Early deterioration signals — watch for further breakdown. "
                f"(Natenberg Ch.8: WEAKENING = early structural warning)"
            ),
            doctrine_source="Natenberg Ch.8: WEAKENING + Loss — HOLD MEDIUM",
            decision_state=STATE_ACTIONABLE,
        )[1]

    # Gate 5a: BW Upgrade — odd lot (<100 shares), buy remaining to enable CC
    # Higher risk: adding capital to an existing position.  Stricter gates than
    # normal CC: thesis INTACT required, IV must be meaningful, payback < 12mo.
    if 0 < qty < SHARES_CC_ELIGIBLE and ei_state not in ("BROKEN", "WEAKENING"):
        bw_upgrade = _evaluate_bw_upgrade(row, ticker, qty, pnl_pct)
        # Attach assessment columns for dashboard display
        result["BW_Upgrade_Feasible"] = bw_upgrade["feasible"]
        result["BW_Upgrade_Shares_Needed"] = bw_upgrade["shares_needed"]
        result["BW_Upgrade_Cost"] = bw_upgrade["cost"]
        result["BW_Upgrade_Expected_Monthly"] = bw_upgrade["monthly_premium"]
        result["BW_Upgrade_Payback_Months"] = bw_upgrade["payback_months"]
        result["BW_Upgrade_Reason"] = bw_upgrade["reason"]

        if bw_upgrade["feasible"]:
            return fire_gate(
                result,
                action="HOLD",
                urgency="MEDIUM",
                rationale=(
                    f"BW upgrade candidate: {ticker} holds {int(qty)} shares — "
                    f"buy {bw_upgrade['shares_needed']} more (${bw_upgrade['cost']:,.0f}) "
                    f"to reach 100 and sell covered calls. "
                    f"IV {bw_upgrade['iv_pct']:.0f}% → est. ${bw_upgrade['monthly_premium']:,.0f}/mo "
                    f"premium. Payback ~{bw_upgrade['payback_months']:.0f} months. "
                    f"P&L: {pnl_str}. Thesis: {bw_upgrade['thesis']}. "
                    f"(Cohen Ch.7: buy-write reduces cost basis; "
                    f"McMillan Ch.3: CC converts idle capital into income. "
                    f"Risk: adding ${bw_upgrade['cost']:,.0f} to an existing position — "
                    f"only proceed if stock thesis supports additional exposure.)"
                ),
                doctrine_source="Cohen Ch.7 + McMillan Ch.3: BW Upgrade Candidate",
                decision_state=STATE_ACTIONABLE,
            )[1]
        else:
            # BW not feasible — recommend closing the odd lot
            return fire_gate(
                result,
                action="HOLD",
                urgency="LOW",
                rationale=(
                    f"Odd lot: {ticker} holds {int(qty)} shares — "
                    f"BW upgrade not feasible: {bw_upgrade['reason']}. "
                    f"P&L: {pnl_str}. "
                    f"Sub-contract position earns zero theta and cannot support "
                    f"covered calls. Consider closing to redeploy capital. "
                    f"(McMillan Ch.1: odd lots without income path = dead capital)"
                ),
                doctrine_source="McMillan Ch.1: Odd Lot — Close or Hold",
                decision_state=STATE_NEUTRAL_CONFIDENT,
            )[1]

    # Gate 5: CC opportunity — idle stock earns zero theta
    if qty >= SHARES_CC_ELIGIBLE and ei_state != "BROKEN" and (pnl_pct is None or pnl_pct > PNL_SIGNIFICANT_LOSS):
        return fire_gate(
            result,
            action="HOLD",
            urgency="LOW",
            rationale=(
                f"\U0001f4e6 Stock {ticker} ({shares_label}) — P&L: {pnl_str}. "
                f"Eligible for covered call overlay (\u2265100 shares, equity {ei_state or 'UNKNOWN'}). "
                f"Idle stock earns zero theta — consider writing calls to generate income. "
                f"(McMillan Ch.3: CC converts holding cost into income)"
            ),
            doctrine_source="McMillan Ch.3: CC Opportunity — HOLD LOW",
            decision_state=STATE_NEUTRAL_CONFIDENT,
        )[1]

    # Gate 6: Default — no actionable signal
    return fire_gate(
        result,
        action="HOLD",
        urgency="LOW",
        rationale=(
            f"\U0001f4e6 Stock {ticker} ({shares_label}) — P&L: {pnl_str}. "
            f"Equity state: {ei_state or 'UNKNOWN'}. "
            f"No doctrinal triggers. "
            f"(McMillan Ch.1: stock position within normal parameters)"
        ),
        doctrine_source="McMillan Ch.1: Neutrality",
        decision_state=STATE_NEUTRAL_CONFIDENT,
    )[1]


def _evaluate_bw_upgrade(
    row: pd.Series, ticker: str, qty: float, pnl_pct: float | None,
) -> Dict[str, Any]:
    """Evaluate whether buying shares to reach 100 for a BW is economically viable.

    Returns dict with feasibility assessment and economics.
    """
    shares_needed = int(100 - qty)
    spot = safe_row_float(row, "UL Last", "Last")
    cost = shares_needed * spot if spot > 0 else 0.0

    # IV: prefer IV_Now, fall back to IV_30D
    iv_raw = safe_row_float(row, "IV_Now", "IV_30D")
    if iv_raw >= 5.0:
        iv_raw /= 100.0
    iv_pct = iv_raw * 100  # for display

    thesis = str(row.get("Thesis_State") or "UNKNOWN").upper()

    # Premium estimate: 30-day OTM call at ~delta 0.30
    # Approximation: premium ≈ spot × IV × √(30/365) × 0.25 per share
    monthly_premium_per_share = spot * iv_raw * math.sqrt(30.0 / 365.0) * 0.25 if iv_raw > 0 else 0.0
    monthly_premium = monthly_premium_per_share * 100  # per contract

    # Margin cost on the additional shares (ROTH/IRA = no margin)
    from core.shared.finance_utils import is_retirement_account as _is_retire
    _is_retirement = _is_retire(str(row.get("Account") or ""))
    monthly_margin = 0.0 if _is_retirement else cost * _BW_UPGRADE_MARGIN_RATE / 12.0

    net_monthly = monthly_premium - monthly_margin
    payback = cost / net_monthly if net_monthly > 0 else float("inf")

    base = {
        "shares_needed": shares_needed,
        "cost": round(cost, 2),
        "iv_raw": iv_raw,
        "iv_pct": iv_pct,
        "monthly_premium": round(monthly_premium, 2),
        "monthly_margin": round(monthly_margin, 2),
        "net_monthly": round(net_monthly, 2),
        "payback_months": round(payback, 1) if payback < 999 else float("inf"),
        "thesis": thesis,
        "feasible": False,
        "reason": "",
    }

    # ── Feasibility gates (stricter than normal CC — adding capital) ──────
    if spot <= 0:
        base["reason"] = "no price data"
        return base

    if thesis not in ("INTACT", "STABLE"):
        base["reason"] = f"thesis {thesis} — don't add capital to a degraded position"
        return base

    if iv_raw < _BW_UPGRADE_IV_MIN:
        base["reason"] = f"IV {iv_pct:.0f}% < {_BW_UPGRADE_IV_MIN*100:.0f}% — premium too thin"
        return base

    if pnl_pct is not None and pnl_pct <= -0.25:
        base["reason"] = f"P&L {pnl_pct:+.1%} — loss too deep to add capital"
        return base

    if cost > _BW_UPGRADE_MAX_COST:
        base["reason"] = f"buy-up cost ${cost:,.0f} > ${_BW_UPGRADE_MAX_COST:,.0f} cap"
        return base

    if net_monthly <= 0:
        base["reason"] = "net income ≤ $0 after margin — premium doesn't cover carry"
        return base

    if payback > _BW_UPGRADE_MAX_PAYBACK:
        base["reason"] = f"payback {payback:.0f} months > {_BW_UPGRADE_MAX_PAYBACK:.0f} month cap"
        return base

    base["feasible"] = True
    base["reason"] = "all gates passed"
    return base
