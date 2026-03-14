"""
RECOVERY_PREMIUM strategy doctrine — optimized for multi-cycle basis reduction.

This is a MODE SWITCH, not a guard. When a damaged buy-write enters Recovery
Premium Mode, the success metric changes from short-term hold EV to cumulative
basis reduction efficiency.

Action vocabulary (different from normal BW doctrine):
  WRITE_NOW         — sell covered call now (favorable conditions)
  HOLD_STOCK_WAIT   — hold stock without call, wait for better premium window
  ROLL_UP_OUT       — roll existing call to better strike/expiration
  PAUSE_WRITING     — temporarily stop call writing (event risk, extreme drawdown)
  EXIT_STOCK        — abandon recovery (thesis broken or uneconomical)

Entry criteria (checked in helpers.py should_enter_recovery_premium_mode):
  - Loss >= 10% from effective cost basis
  - 1+ income cycles completed
  - IV viable for premium generation
  - Thesis not BROKEN

Gate cascade (proposal-based, uses ProposalCollector):
  0.  Thesis guardrail         — BROKEN → EXIT_STOCK (hard veto)
  1.  Earnings risk             — ≤7d to earnings → PAUSE_WRITING or HOLD
  2.  Macro event proximity     — HIGH macro ≤3d → PAUSE_WRITING annotation
  3.  Strike discipline         — below cost basis → ROLL_UP_OUT
  4.  Assignment economics      — delta > 0.70 → evaluate assignment vs roll
  5.  Expiration proximity      — DTE < 7 → WRITE_NOW (new cycle) or ROLL_UP_OUT
  6.  IV environment            — IV_Rank favorable → WRITE_NOW; poor → HOLD_STOCK_WAIT
  7.  Rally timing              — post-rally → WRITE_NOW window
  8.  Premium economics         — 50% capture → let expire or roll to next cycle
  9.  Recovery viability        — uneconomical → EXIT_STOCK
  10. Roth preservation         — extra strike discipline for retirement accounts
  11. Default: basis reduction  — ongoing HOLD or WRITE_NOW based on conditions

RAG citations:
  Jabbour Ch.4: Repair Strategies (damaged positions, basis reduction ladder)
  McMillan Ch.3: Buy-Write Basis Reduction (systematic premium collection)
  Given Ch.6: 21-DTE Income Gate (timing premium cycles)
  Passarelli Ch.5: Patience in Income Positions (theta as ally)
  Natenberg Ch.7: Roll Timing Intelligence (when to roll, when to wait)
"""

import math
import logging
from typing import Dict, Any

import pandas as pd

from core.management.cycle3.doctrine.proposal import ProposalCollector, propose_gate
from core.management.cycle3.doctrine.helpers import safe_row_float
from core.management.cycle3.doctrine.thresholds import (
    DTE_EMERGENCY_ROLL,
    DTE_INCOME_GATE,
    DELTA_ITM_EMERGENCY,
    DELTA_PRE_ITM_WARNING,
    PREMIUM_CAPTURE_TARGET,
    RECOVERY_PREMIUM_IV_FLOOR,
    RECOVERY_IV_RANK_FAVORABLE,
    RECOVERY_IV_RANK_POOR,
    RECOVERY_RALLY_PCT_TRIGGER,
    RECOVERY_STRIKE_COST_BASIS_BUFFER,
    RECOVERY_STRIKE_BELOW_BASIS_WARN,
    RECOVERY_STRIKE_NEAR_SPOT_FLOOR,
    RECOVERY_ANNUALIZED_YIELD_TARGET,
    RECOVERY_MONTHS_TO_BREAKEVEN_EXIT,
    RECOVERY_ROTH_STRIKE_BUFFER,
    RECOVERY_ROTH_MAX_ASSIGNMENT_LOSS,
    EARNINGS_NOTE_WINDOW,
)
from core.management.cycle3.doctrine.gate_result import STATE_ACTIONABLE

logger = logging.getLogger(__name__)

# Recovery Premium action vocabulary
ACTION_WRITE_NOW = "WRITE_NOW"
ACTION_HOLD_STOCK_WAIT = "HOLD_STOCK_WAIT"
ACTION_ROLL_UP_OUT = "ROLL_UP_OUT"
ACTION_PAUSE_WRITING = "PAUSE_WRITING"
ACTION_EXIT_STOCK = "EXIT_STOCK"


def recovery_premium_doctrine(row: pd.Series, result: Dict, ctx: dict) -> Dict:
    """Recovery Premium Mode — proposal-based evaluation.

    Args:
        row: Position data row
        result: Base result dict (same shape as other strategies)
        ctx: Recovery context from should_enter_recovery_premium_mode()

    Returns:
        Updated result dict with recovery premium action/rationale.
    """
    collector = ProposalCollector()

    # ── Unpack context ─────────────────────────────────────────────────────
    spot = ctx["spot"]
    effective_cost = ctx["effective_cost"]
    broker_cost = ctx["broker_cost_per_share"]
    loss_pct = ctx["loss_pct"]
    gap = ctx["gap_to_breakeven"]
    cum_premium_ps = ctx["premium_collected_per_share"]
    cycles = ctx["cycles_completed"]
    basis_reduction_pct = ctx["basis_reduction_pct"]

    monthly_income = ctx["monthly_income"]
    net_monthly = ctx["net_monthly"]
    margin_monthly = ctx["margin_monthly"]
    months_to_be = ctx["months_to_breakeven"]
    ann_yield = ctx["annualized_yield"]

    has_call = ctx["has_active_call"]
    strike = ctx["strike"]
    strike_vs_cost = ctx["strike_vs_cost"]
    dte = ctx["dte"]
    delta = ctx["delta"]
    last_premium = ctx["last_premium"]

    iv_now = ctx["iv_now"]
    iv_rank = ctx["iv_rank"]

    is_roth = ctx["is_retirement"]

    roc5 = ctx["roc5"]
    roc10 = ctx["roc10"]
    adx = ctx["adx"]
    rsi = ctx["rsi"]
    stock_basing = ctx["stock_basing"]
    thesis = ctx["thesis"]

    days_to_earnings = ctx["days_to_earnings"]
    earnings_beat_rate = ctx["earnings_beat_rate"]
    days_to_macro = ctx["days_to_macro"]
    macro_event = ctx["macro_event"]
    days_since_roll = ctx["days_since_roll"]
    qty = ctx["qty"]

    # Common rationale header
    _mode_header = (
        f"[RECOVERY PREMIUM MODE] Loss {loss_pct:.1%} from cost ${effective_cost:.2f}. "
        f"{cycles} cycles, ${cum_premium_ps:.2f}/sh collected "
        f"({basis_reduction_pct:.1%} basis reduction). "
    )
    _roth_tag = " [Roth: capital preservation priority]" if is_roth else ""
    _metrics = (
        f"Annualized yield: {ann_yield:.1%} | "
        f"Months to breakeven: {months_to_be:.0f} | "
        f"Net monthly: ${net_monthly:.2f}/sh"
    )

    # ── 0. Thesis guardrail — BROKEN = abandon recovery ───────────────────
    if thesis == 'BROKEN':
        propose_gate(
            collector, "thesis_broken",
            action=ACTION_EXIT_STOCK, urgency="CRITICAL",
            is_hard_veto=True,
            rationale=(
                f"{_mode_header}Thesis BROKEN — fundamental reason to hold is gone. "
                f"Recovery via premium is bagholder automation without directional thesis. "
                f"Exit stock position. (McMillan Ch.4: exit failed repair)"
            ),
            doctrine_source="McMillan Ch.4: Exit Failed Repair",
            priority=1,
            exit_trigger_type="CAPITAL",
        )
        return _resolve(collector, result)

    # ── 1. Earnings risk — pause writing near binary events ───────────────
    if days_to_earnings <= 7 and has_call:
        _earnings_note = ""
        if earnings_beat_rate >= 0.7:
            _earnings_note = f" Track record: {earnings_beat_rate:.0%} beat rate — historically favorable."
        elif earnings_beat_rate > 0 and earnings_beat_rate < 0.5:
            _earnings_note = f" Track record: {earnings_beat_rate:.0%} beat rate — caution warranted."

        if days_to_earnings <= 2:
            propose_gate(
                collector, "earnings_imminent",
                action=ACTION_PAUSE_WRITING, urgency="HIGH",
                rationale=(
                    f"{_mode_header}Earnings in {days_to_earnings:.0f}d — "
                    f"binary event risk.{_earnings_note} "
                    f"Let existing call expire/settle before writing next cycle. "
                    f"Do NOT open new call before announcement. "
                    f"(Augen Ch.3: earnings binary risk for income positions)"
                ),
                doctrine_source="Augen Ch.3: Earnings Pause",
                priority=15,
            )
        elif delta > DELTA_PRE_ITM_WARNING:
            propose_gate(
                collector, "earnings_near_strike",
                action=ACTION_ROLL_UP_OUT, urgency="MEDIUM",
                rationale=(
                    f"{_mode_header}Earnings in {days_to_earnings:.0f}d with "
                    f"delta {delta:.2f} — near strike. Roll up/out past earnings "
                    f"to avoid assignment on positive surprise.{_earnings_note}"
                ),
                doctrine_source="Jabbour Ch.4: Earnings Roll",
                priority=20,
            )

    # ── 1b. Earnings note for idle stock ──────────────────────────────────
    if days_to_earnings <= EARNINGS_NOTE_WINDOW and not has_call:
        _earn_hold = (
            f" Consider waiting until after earnings to write call — "
            f"IV typically elevated pre-earnings (better premium) but "
            f"gap risk exists."
        )
        if days_to_earnings <= 3:
            propose_gate(
                collector, "earnings_idle_pause",
                action=ACTION_HOLD_STOCK_WAIT, urgency="LOW",
                rationale=(
                    f"{_mode_header}Earnings in {days_to_earnings:.0f}d, no active call. "
                    f"Wait for post-earnings clarity before writing.{_roth_tag}"
                ),
                doctrine_source="Passarelli Ch.5: Post-Earnings Write",
                priority=30,
            )

    # ── 2. Macro event proximity ──────────────────────────────────────────
    if days_to_macro <= 3 and macro_event and not has_call:
        propose_gate(
            collector, "macro_proximity",
            action=ACTION_HOLD_STOCK_WAIT, urgency="LOW",
            rationale=(
                f"{_mode_header}Macro event ({macro_event}) in {days_to_macro:.0f}d. "
                f"Consider waiting for post-event clarity before writing call. "
                f"IV may spike post-event → better premium window."
            ),
            doctrine_source="Murphy Ch.4: Event-Aware Timing",
            priority=35,
        )

    # ── 3. Strike discipline — below cost basis guard ─────────────────────
    if has_call and strike > 0:
        _strike_buffer = RECOVERY_ROTH_STRIKE_BUFFER if is_roth else RECOVERY_STRIKE_COST_BASIS_BUFFER
        _strike_floor = effective_cost * (1 + _strike_buffer)

        if strike < effective_cost * (1 - RECOVERY_STRIKE_BELOW_BASIS_WARN):
            # Strike far below cost basis — getting called away locks in loss
            _assignment_loss = (strike - broker_cost) / broker_cost if broker_cost > 0 else 0
            _roth_guard = ""
            if is_roth and abs(_assignment_loss) > RECOVERY_ROTH_MAX_ASSIGNMENT_LOSS:
                _roth_guard = (
                    f" Roth guard: assignment would lock {_assignment_loss:.1%} loss "
                    f"(>{RECOVERY_ROTH_MAX_ASSIGNMENT_LOSS:.0%} threshold). "
                    f"Roll UP to protect scarce Roth capital."
                )
            propose_gate(
                collector, "strike_below_basis",
                action=ACTION_ROLL_UP_OUT, urgency="MEDIUM",
                rationale=(
                    f"{_mode_header}Strike ${strike:.2f} is {strike_vs_cost:.1%} vs "
                    f"cost ${effective_cost:.2f} — assignment locks in loss. "
                    f"Roll UP toward ${_strike_floor:.2f}+ to preserve recovery path. "
                    f"{_roth_guard}"
                    f"(Jabbour Ch.4: never sell below basis unless intentional exit)"
                ),
                doctrine_source="Jabbour Ch.4: Strike Discipline — Cost Basis Floor",
                priority=25,
            )
        elif strike < effective_cost:
            # Strike below cost but within tolerance — annotate
            propose_gate(
                collector, "strike_below_basis_mild",
                action=ACTION_ROLL_UP_OUT if dte < DTE_INCOME_GATE else "HOLD",
                urgency="LOW",
                rationale=(
                    f"{_mode_header}Strike ${strike:.2f} below cost ${effective_cost:.2f} "
                    f"({strike_vs_cost:+.1%}). "
                    f"{'Roll up on next cycle to protect basis.' if dte < DTE_INCOME_GATE else 'Monitor — consider rolling up at next cycle.'}"
                    f"{_roth_tag}"
                ),
                doctrine_source="McMillan Ch.3: Basis-Aware Strike Selection",
                priority=40,
            )

    # ── 4. Assignment economics (deep ITM) ────────────────────────────────
    if has_call and delta > DELTA_ITM_EMERGENCY:
        _assignment_price = strike
        _assignment_pnl = (_assignment_price - broker_cost) / broker_cost if broker_cost > 0 else 0

        if _assignment_pnl >= 0:
            # Assignment at profit — might be acceptable
            propose_gate(
                collector, "assignment_profitable",
                action="HOLD", urgency="LOW",
                rationale=(
                    f"{_mode_header}Delta {delta:.2f} — assignment likely. "
                    f"Assignment at ${strike:.2f} = {_assignment_pnl:+.1%} from broker cost "
                    f"${broker_cost:.2f}. Profitable assignment — acceptable exit. "
                    f"Let assignment occur or roll up/out if continuing recovery preferred."
                ),
                doctrine_source="McMillan Ch.3: Profitable Assignment Accept",
                priority=50,
            )
        else:
            # Assignment at loss — roll to avoid locking in loss
            propose_gate(
                collector, "assignment_at_loss",
                action=ACTION_ROLL_UP_OUT, urgency="HIGH",
                rationale=(
                    f"{_mode_header}Delta {delta:.2f} — assignment imminent. "
                    f"Assignment at ${strike:.2f} = {_assignment_pnl:+.1%} LOSS from "
                    f"broker cost ${broker_cost:.2f}. Roll UP/OUT to avoid forced loss. "
                    f"Recovery path: continue collecting premium to close gap.{_roth_tag}"
                ),
                doctrine_source="Jabbour Ch.4: Avoid Assignment Below Basis",
                priority=10,
            )

    # ── 5. Expiration proximity — cycle transition ────────────────────────
    if has_call and dte < DTE_EMERGENCY_ROLL:
        # Extrinsic analysis
        _mid = safe_row_float(row, 'Mid', 'Last', default=0.0)
        _intrinsic = max(0, spot - strike) if strike > 0 else 0
        _extrinsic = max(0, _mid - _intrinsic)
        _extrinsic_pct = _extrinsic / _mid if _mid > 0 else 0

        if delta < 0.30:
            # Far OTM, expiring worthless — let expire, prepare next cycle
            propose_gate(
                collector, "expiration_otm",
                action=ACTION_WRITE_NOW, urgency="MEDIUM",
                rationale=(
                    f"{_mode_header}DTE={dte:.0f}, delta {delta:.2f} — call expiring "
                    f"worthless. Full premium capture this cycle. "
                    f"Prepare next call: target strike above ${effective_cost:.2f} "
                    f"(cost basis) with 30-45 DTE.{_roth_tag} "
                    f"(Given Ch.6: 21-DTE next cycle timing)"
                ),
                doctrine_source="Given Ch.6: Premium Cycle Reset",
                priority=30,
            )
        else:
            # Near ATM or ITM at expiration — roll to avoid assignment
            propose_gate(
                collector, "expiration_itm",
                action=ACTION_ROLL_UP_OUT, urgency="HIGH",
                rationale=(
                    f"{_mode_header}DTE={dte:.0f}, delta {delta:.2f} — pin risk zone. "
                    f"Roll out to next cycle (30-45 DTE) at strike ≥ ${effective_cost:.2f}. "
                    f"Extrinsic: {_extrinsic_pct:.0%} — "
                    f"{'credit roll viable' if _extrinsic_pct > 0.25 else 'may require net debit'}."
                    f"{_roth_tag}"
                ),
                doctrine_source="McMillan Ch.3: Pin Risk Roll — Recovery Mode",
                priority=12,
            )

    # ── 6. IV environment assessment ──────────────────────────────────────
    if not has_call:
        # No active call — should we write one?
        if iv_rank >= RECOVERY_IV_RANK_FAVORABLE:
            propose_gate(
                collector, "iv_favorable_write",
                action=ACTION_WRITE_NOW, urgency="MEDIUM",
                rationale=(
                    f"{_mode_header}No active call. IV_Rank {iv_rank:.0f} ≥ {RECOVERY_IV_RANK_FAVORABLE} — "
                    f"favorable premium environment. Write call at strike ≥ "
                    f"${effective_cost:.2f} with 30-45 DTE. "
                    f"{_metrics}{_roth_tag} "
                    f"(McMillan Ch.3: sell premium when IV is elevated)"
                ),
                doctrine_source="McMillan Ch.3: Elevated IV Premium Sale",
                priority=30,
            )
        elif iv_rank < RECOVERY_IV_RANK_POOR:
            propose_gate(
                collector, "iv_poor_wait",
                action=ACTION_HOLD_STOCK_WAIT, urgency="LOW",
                rationale=(
                    f"{_mode_header}No active call. IV_Rank {iv_rank:.0f} < {RECOVERY_IV_RANK_POOR} — "
                    f"premium too thin. Wait for IV expansion (rally, macro event, "
                    f"or sector rotation) before writing. "
                    f"Selling calls at depressed IV locks in poor premium and "
                    f"caps upside during potential recovery.{_roth_tag} "
                    f"(Natenberg Ch.7: don't sell cheap options)"
                ),
                doctrine_source="Natenberg Ch.7: IV Environment — Wait for Premium",
                priority=45,
            )
        else:
            # Moderate IV — evaluate other timing signals
            propose_gate(
                collector, "iv_moderate",
                action=ACTION_WRITE_NOW if roc5 > RECOVERY_RALLY_PCT_TRIGGER else ACTION_HOLD_STOCK_WAIT,
                urgency="LOW",
                rationale=(
                    f"{_mode_header}No active call. IV_Rank {iv_rank:.0f} (moderate). "
                    f"{'Post-rally: ROC5 ' + f'{roc5:+.1f}% — favorable write window.' if roc5 > RECOVERY_RALLY_PCT_TRIGGER else 'No rally signal — consider waiting for better entry.'} "
                    f"{_metrics}{_roth_tag}"
                ),
                doctrine_source="Passarelli Ch.5: Timing Premium Sales",
                priority=50,
            )

    # ── 7. Rally timing — sell into strength ──────────────────────────────
    if not has_call and roc5 > RECOVERY_RALLY_PCT_TRIGGER:
        propose_gate(
            collector, "rally_write_window",
            action=ACTION_WRITE_NOW, urgency="MEDIUM",
            rationale=(
                f"{_mode_header}Stock rallied {roc5:+.1f}% (5d). "
                f"Sell covered call into strength — elevated premium and better "
                f"strike selection after up-move. Target strike at/above "
                f"${effective_cost:.2f} (cost basis).{_roth_tag} "
                f"(Given Ch.6: sell premium after rallies, not drawdowns)"
            ),
            doctrine_source="Given Ch.6: Rally-Timed Premium Sale",
            priority=25,
        )

    # ── 8. Premium capture (50% rule for active calls) ────────────────────
    if has_call and last_premium > 0:
        _mid_now = safe_row_float(row, 'Mid', 'Last', default=0.0)
        _pct_captured = 1.0 - (_mid_now / last_premium) if last_premium > 0 else 0

        if _pct_captured >= PREMIUM_CAPTURE_TARGET and dte > DTE_EMERGENCY_ROLL:
            propose_gate(
                collector, "premium_50pct_capture",
                action=ACTION_WRITE_NOW, urgency="LOW",
                rationale=(
                    f"{_mode_header}{_pct_captured:.0%} premium captured "
                    f"(${last_premium:.2f} → ${_mid_now:.2f}). "
                    f"Close current call and write new cycle for fresh premium. "
                    f"(Passarelli Ch.6: 50% rule — compound cycles)"
                ),
                doctrine_source="Passarelli Ch.6: 50% Premium Capture — Recovery",
                priority=35,
            )

    # ── 9. Recovery viability — periodic check ────────────────────────────
    if months_to_be > RECOVERY_MONTHS_TO_BREAKEVEN_EXIT:
        propose_gate(
            collector, "recovery_uneconomical",
            action=ACTION_EXIT_STOCK, urgency="MEDIUM",
            rationale=(
                f"{_mode_header}Recovery uneconomical: {months_to_be:.0f} months "
                f"to breakeven at current income rate (${net_monthly:.2f}/sh/mo). "
                f"Capital locked for {months_to_be / 12:.1f} years. "
                f"Consider releasing capital for better opportunities. "
                f"(McMillan Ch.4: abandon repair when cost exceeds benefit)"
            ),
            doctrine_source="McMillan Ch.4: Uneconomical Recovery Exit",
            priority=60,
            exit_trigger_type="CAPITAL",
        )

    if ann_yield < RECOVERY_ANNUALIZED_YIELD_TARGET * 0.5 and iv_rank < RECOVERY_IV_RANK_POOR:
        propose_gate(
            collector, "yield_inadequate",
            action=ACTION_HOLD_STOCK_WAIT, urgency="LOW",
            rationale=(
                f"{_mode_header}Yield {ann_yield:.1%} below {RECOVERY_ANNUALIZED_YIELD_TARGET:.0%} target. "
                f"IV_Rank {iv_rank:.0f} depressed — premium generation weak. "
                f"Hold stock and wait for IV expansion before next cycle. "
                f"Do not force writes at unfavorable levels."
            ),
            doctrine_source="Natenberg Ch.7: Yield Floor — Wait for IV",
            priority=55,
        )

    # ── 10. Roth-specific preservation ────────────────────────────────────
    if is_roth and has_call:
        if strike > 0 and strike < spot * (1 + RECOVERY_STRIKE_NEAR_SPOT_FLOOR):
            propose_gate(
                collector, "roth_near_spot_guard",
                action=ACTION_ROLL_UP_OUT, urgency="LOW",
                rationale=(
                    f"{_mode_header}Roth preservation: strike ${strike:.2f} is within "
                    f"{RECOVERY_STRIKE_NEAR_SPOT_FLOOR:.0%} of spot ${spot:.2f} on "
                    f"depressed stock. Selling near-spot calls on Roth positions caps "
                    f"recovery upside. Roll UP to preserve upside optionality — "
                    f"Roth capital is irreplaceable. "
                    f"(Jabbour Ch.4: Roth repair must favor upside preservation)"
                ),
                doctrine_source="Jabbour Ch.4: Roth Capital Preservation",
                priority=38,
            )

    if is_roth and not has_call and loss_pct < -0.30:
        propose_gate(
            collector, "roth_deep_drawdown_caution",
            action=ACTION_HOLD_STOCK_WAIT, urgency="LOW",
            rationale=(
                f"{_mode_header}Roth at {loss_pct:.1%} — deep drawdown on irreplaceable "
                f"capital. {'Stock appears to be basing (ADX<20). ' if stock_basing else ''}"
                f"Wait for stabilization + IV expansion before writing. "
                f"Avoid locking in poor strikes during capitulation.{_roth_tag}"
            ),
            doctrine_source="Jabbour Ch.4: Roth Deep Recovery — Patience",
            priority=42,
        )

    # ── 11. Default: ongoing recovery assessment ──────────────────────────
    if has_call:
        propose_gate(
            collector, "recovery_hold_active_call",
            action="HOLD", urgency="LOW",
            rationale=(
                f"{_mode_header}Active call: ${strike:.2f} strike, {dte:.0f} DTE, "
                f"delta {delta:.2f}. Let theta work. "
                f"{_metrics}{_roth_tag} "
                f"(Passarelli Ch.5: patience in income positions)"
            ),
            doctrine_source="Passarelli Ch.5: Recovery Premium — Active Call HOLD",
            priority=100,
        )
    else:
        _write_rec = iv_rank >= RECOVERY_IV_RANK_POOR
        propose_gate(
            collector, "recovery_idle_stock",
            action=ACTION_WRITE_NOW if _write_rec else ACTION_HOLD_STOCK_WAIT,
            urgency="LOW",
            rationale=(
                f"{_mode_header}No active call. "
                f"{'IV environment acceptable — consider writing next cycle.' if _write_rec else 'IV depressed — wait for better conditions.'} "
                f"{_metrics}{_roth_tag}"
            ),
            doctrine_source="McMillan Ch.3: Recovery Premium — Cycle Assessment",
            priority=100,
        )

    return _resolve(collector, result)


def _resolve(collector: ProposalCollector, result: Dict) -> Dict:
    """Resolve proposals and format result with recovery premium metadata."""
    if collector.has_hard_veto():
        winner = collector.get_veto()
        method = "HARD_VETO"
    else:
        # Priority-based resolution: lowest priority number wins.
        # On tie: highest urgency wins.
        proposals = sorted(
            collector.proposals,
            key=lambda p: (p.priority, -p.urgency_rank),
        )
        winner = proposals[0] if proposals else None

    if winner is None:
        result["Action"] = "HOLD"
        result["Urgency"] = "LOW"
        result["Rationale"] = "[RECOVERY PREMIUM MODE] No proposals generated — default HOLD."
        result["Doctrine_Source"] = "Recovery Premium: Default Hold"
        return result

    method = method if collector.has_hard_veto() else "RECOVERY_PRIORITY"
    result = collector.to_result(winner, result, method)

    # Recovery premium metadata (always present when this mode is active)
    result["Doctrine_State"] = "RECOVERY_PREMIUM"
    result["Recovery_Mode"] = True

    return result
