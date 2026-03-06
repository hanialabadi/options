"""
Outcome Classifier — Doctrine Feedback Loop
============================================

Assigns a categorical outcome label to every closed trade based on the
signals present at entry and the gate that fired at exit.

Taxonomy (mutually exclusive, in evaluation order):
  ✅  THESIS_COMPLETION     — Exit gate fired at optimum / profit target reached
  ✅  THETA_HARVEST         — BW/CC expired worthless or near max premium captured
  ⚠️  PREMATURE_EXIT        — Exited with >20% remaining intrinsic + no gate trigger
  ❌  LATE_CYCLE_ENTRY      — Entry when MomentumVelocity_State was LATE_CYCLE
  ❌  FALSE_GEM             — Entry PCS was high but thesis broke within 5 days
  ❌  VOL_EDGE_MISREAD      — IV > HV at entry on a long-vol strategy (bought expensive vol)
  ❌  MOMENTUM_MISCLASSIFY  — Entered ACCELERATING/TRENDING, actual direction was wrong
  ⚠️  THETA_MISMANAGEMENT   — Held through >70% time decay without rolling/exiting
  ⚠️  IGNORED_EXIT_SIGNAL   — Management_recommendations had EXIT but no action was taken
  ❓  UNCLASSIFIED           — Insufficient data to assign a category

Each label is accompanied by a gate_failed field naming the engine gate that
should have caught the problem (or None for wins).

Design principles:
  - Deterministic: same inputs → same label, always
  - Book-anchored: each branch cites the doctrine source
  - No ML, no probability: pure rule evaluation
  - Statistically guarded: minimum N enforced at aggregation layer, not here
"""

from __future__ import annotations
from typing import Optional


# ── Outcome type constants ────────────────────────────────────────────────────

THESIS_COMPLETION       = "THESIS_COMPLETION"
THETA_HARVEST           = "THETA_HARVEST"
PREMATURE_EXIT          = "PREMATURE_EXIT"
LATE_CYCLE_ENTRY        = "LATE_CYCLE_ENTRY"
FALSE_GEM               = "FALSE_GEM"
VOL_EDGE_MISREAD        = "VOL_EDGE_MISREAD"
MOMENTUM_MISCLASSIFY    = "MOMENTUM_MISCLASSIFY"
THETA_MISMANAGEMENT     = "THETA_MISMANAGEMENT"
IGNORED_EXIT_SIGNAL     = "IGNORED_EXIT_SIGNAL"
# Gate-specific outcomes added in doctrine hardening (Feb 2026)
PIN_RISK_EXIT           = "PIN_RISK_EXIT"        # exited via pin-risk / gamma-critical gate
VOL_FLIP_ROLL           = "VOL_FLIP_ROLL"        # rolled because vol regime flipped against entry
NEGATIVE_CARRY_ESCALATION = "NEGATIVE_CARRY_ESCALATION"  # roll triggered by carry deficit + short DTE
TIME_STOP_EXIT          = "TIME_STOP_EXIT"       # exited via time-stop gate (DTE ≤ threshold)
RECOVERY_OVERRIDE       = "RECOVERY_OVERRIDE"    # recovery-infeasibility gate suppressed by Up drift
UNCLASSIFIED            = "UNCLASSIFIED"

WIN_TYPES  = {THESIS_COMPLETION, THETA_HARVEST}
WARN_TYPES = {PREMATURE_EXIT, THETA_MISMANAGEMENT, IGNORED_EXIT_SIGNAL,
              NEGATIVE_CARRY_ESCALATION, TIME_STOP_EXIT, RECOVERY_OVERRIDE}
LOSS_TYPES = {LATE_CYCLE_ENTRY, FALSE_GEM, VOL_EDGE_MISREAD, MOMENTUM_MISCLASSIFY,
              PIN_RISK_EXIT, VOL_FLIP_ROLL}


# ── Doctrine_Source → Gate tag canonicalization map ──────────────────────────
# Maps substrings from the Doctrine_Source field (as persisted in
# management_recommendations) to a canonical gate tag.
# Used by feedback_engine._record_closure() to derive Gate_Fired from live data
# when no explicit gate field exists.
DOCTRINE_SOURCE_TO_GATE: dict[str, str] = {
    "Pin Risk":                     PIN_RISK_EXIT,
    "Gamma Critical":               PIN_RISK_EXIT,
    "Vol Regime Flip":              VOL_FLIP_ROLL,
    "Vol Regime Flip — Edge Reversal": VOL_FLIP_ROLL,
    "Yield Maintenance + Timing":   NEGATIVE_CARRY_ESCALATION,
    "Yield Maintenance":            NEGATIVE_CARRY_ESCALATION,
    "Time Stop":                    TIME_STOP_EXIT,
    "Recovery Infeasibility":       RECOVERY_OVERRIDE,
    "Forward Expectancy":           "EV_FEASIBILITY_ROLL",
    "Conviction Decay":             "CONVICTION_DECAY_ROLL",
    "Theta Acceleration":           "THETA_ACCELERATION_EXIT",
    "Hard Stop":                    "HARD_STOP_EXIT",
    "ITM Defense":                  "ITM_DEFENSE_ROLL",
    "Expiration Management":        "EXPIRATION_ROLL",
    "Thesis Satisfied":             "THESIS_SATISFIED_EXIT",
    "Profit Target":                "PROFIT_TARGET_EXIT",
    "Structural Exit":              "STRUCTURAL_THESIS_EXIT",
    "Greek Drift":                  "DELTA_COLLAPSE_EXIT",
    "Time-to-be-Right":             "TIME_TO_BE_RIGHT_ROLL",
    "Vol Spike Exit":               "VOL_SPIKE_EXIT",
    "Vol Collapse":                 "VOL_COLLAPSE_EXIT",
}


def gate_tag_from_doctrine_source(doctrine_source: str) -> str:
    """
    Derives a canonical gate tag from a Doctrine_Source string.
    Searches for substring matches against DOCTRINE_SOURCE_TO_GATE.
    Returns 'UNTAGGED' when no match found.
    """
    if not doctrine_source:
        return "UNTAGGED"
    for key, tag in DOCTRINE_SOURCE_TO_GATE.items():
        if key in doctrine_source:
            return tag
    return "UNTAGGED"


def classify_outcome(
    strategy:              str,
    pnl_pct:               float,
    days_held:             float,
    entry_momentum_state:  Optional[str],
    entry_iv_hv_ratio:     Optional[float],   # IV/HV at entry (None = unavailable)
    entry_rsi:             Optional[float],
    entry_roc20:           Optional[float],
    entry_pcs:             Optional[float],
    entry_dte:             Optional[float],
    exit_action:           Optional[str],      # EXIT / ROLL / EXPIRED / ASSIGNED
    exit_doctrine_source:  Optional[str],      # Doctrine_Source of final recommendation
    exit_signal_followed:  bool,               # was EXIT signal acted on?
    mfe_pct:               Optional[float],    # max favorable excursion during hold
    mae_pct:               Optional[float],    # max adverse excursion during hold
) -> tuple[str, Optional[str], str]:
    """
    Returns (outcome_type, gate_failed, rationale_note).

    gate_failed = None for wins; engine gate name for losses/warnings.
    rationale_note = human-readable one-liner with RAG citation.
    """
    strat  = (strategy or "").upper()
    exit_s = (exit_action or "").upper()
    exit_d = (exit_doctrine_source or "")
    exit_d_upper = exit_d.upper()
    mom    = (entry_momentum_state or "").upper()
    is_long_vol = any(x in strat for x in ("LONG_CALL", "LONG_PUT", "LEAP", "BUY_CALL", "BUY_PUT"))
    is_short_vol = any(x in strat for x in ("BUY_WRITE", "COVERED_CALL", "CSP", "SHORT_PUT"))

    # ── 0. Gate-specific doctrine exits (highest specificity — evaluated first) ─
    # These derive outcome type directly from the Doctrine_Source tag, so they
    # are classified precisely regardless of P&L or momentum state.

    if "PIN RISK" in exit_d_upper or "GAMMA CRITICAL" in exit_d_upper:
        return (
            PIN_RISK_EXIT,
            "McMillan Ch.7 + Natenberg Ch.15: Pin Risk / Gamma Critical gate",
            f"Position exited/rolled by pin-risk or gamma-critical gate (DTE ≤ 3 or gamma exploding). "
            f"Outcome={pnl_pct:.0%}. Gate fired correctly; P&L reflects whether exit was timely.",
        )

    if "VOL REGIME FLIP" in exit_d_upper or "EDGE REVERSAL" in exit_d_upper:
        return (
            VOL_FLIP_ROLL,
            "Passarelli Ch.6: Vol Regime Flip — Edge Reversal (short_put_doctrine gate 3a)",
            f"Short put rolled because IV regime flipped EXTREME from a LOW entry. "
            f"Outcome={pnl_pct:.0%}. Entry IV was below 25%; position's edge was lost when vol expanded.",
        )

    if "YIELD MAINTENANCE" in exit_d_upper and "TIMING" in exit_d_upper and pnl_pct < 0:
        return (
            NEGATIVE_CARRY_ESCALATION,
            "McMillan Ch.3: Yield Maintenance + Timing Gate (negative carry at short DTE)",
            f"Negative carry gate escalated to MEDIUM at DTE < 14 with yield < 5%. "
            f"Outcome={pnl_pct:.0%}. Position rolled/exited due to carry deficit near expiry.",
        )

    if "TIME STOP" in exit_d_upper:
        _ts_note = (
            f"Exited at {pnl_pct:.0%} via time-stop gate (DTE ≤ threshold). "
            + ("Gain realized before theta eroded it." if pnl_pct > 0
               else "Losses cut before theta acceleration worsened them.")
        )
        return (
            TIME_STOP_EXIT,
            "Passarelli Ch.2/Ch.8: Time Stop gate (_long_option_doctrine pre-winner check)",
            _ts_note,
        )

    if "RECOVERY INFEASIBILITY" in exit_d_upper:
        # Only reaches here if gate wasn't suppressed (Drift_Direction != Up)
        return (
            RECOVERY_OVERRIDE,
            "Natenberg Ch.5: Recovery Infeasibility gate (drift-direction suppression inactive)",
            f"Recovery-infeasibility exit fired (required daily move >> HV-implied sigma). "
            f"Outcome={pnl_pct:.0%}. Gate correctly identified structural recovery failure.",
        )

    # ── 1. Ignored EXIT signal ────────────────────────────────────────────────
    # Doctrine said EXIT but trade continued. Only detectable when exit_signal_followed=False
    # AND exit happened later at a worse price.
    if not exit_signal_followed and pnl_pct < -0.10:
        return (
            IGNORED_EXIT_SIGNAL,
            "evaluate_with_guard: EXIT signal not acted on",
            "EXIT signal was issued but not followed — position continued to lose "
            "(McMillan Ch.4: acting on signals is part of the system)."
        )

    # ── 2. Late-Cycle entry ───────────────────────────────────────────────────
    # Entered when momentum was already diverging. A win here is luck, not edge.
    if mom == "LATE_CYCLE" and pnl_pct < -0.05:
        return (
            LATE_CYCLE_ENTRY,
            "Gate 2.5 / scale-up anti-chasing (engine.py)",
            "Entry during LATE_CYCLE momentum — RSI diverging, ROC decelerating. "
            "Gate 2.5 should have blocked or required additional confirmation "
            "(McMillan Ch.4: don't enter at exhaustion)."
        )

    # ── 3. Vol edge misread (long vol only) ───────────────────────────────────
    # Bought expensive vol: IV > HV at entry. Natenberg Ch.5: you want IV < HV at entry.
    if is_long_vol and entry_iv_hv_ratio is not None and entry_iv_hv_ratio > 1.15 and pnl_pct < -0.10:
        return (
            VOL_EDGE_MISREAD,
            "Gate 2d (vol edge check, engine.py)",
            f"IV/HV={entry_iv_hv_ratio:.2f} at entry — bought expensive vol. "
            "Gate 2d applies post-entry only; pre-entry vol check needed "
            "(Natenberg Ch.5: edge requires IV < HV at entry)."
        )

    # ── 4. Momentum misclassification ─────────────────────────────────────────
    # Entered ACCELERATING or TRENDING but price moved against thesis significantly.
    if mom in ("ACCELERATING", "TRENDING") and pnl_pct < -0.25:
        if is_long_vol and entry_roc20 is not None and entry_roc20 < -5:
            return (
                MOMENTUM_MISCLASSIFY,
                "MomentumVelocity classifier (compute_primitives.py)",
                f"Entry state was {mom} but ROC20={entry_roc20:.1f}% was already negative — "
                "classifier may have used stale primitives or insufficient lookback "
                "(Natenberg Ch.5: momentum state must agree with price direction)."
            )

    # ── 5. False GEM — high PCS entry, fast breakdown ────────────────────────
    # PCS was strong at scan but thesis broke within 5 days.
    if entry_pcs is not None and entry_pcs > 70 and pnl_pct < -0.20 and days_held <= 5:
        return (
            FALSE_GEM,
            "Scan GEM filter (Step 6) / scan_engine",
            f"PCS={entry_pcs:.0f} at entry but thesis broke in {days_held:.0f} days. "
            "Either scan signals were stale or event risk was unmodeled "
            "(McMillan Ch.4: GEM quality degrades rapidly after earnings/news)."
        )

    # ── 6. Theta mismanagement (long vol only) ────────────────────────────────
    # Held past the point where theta cost exceeded the remaining edge.
    # Proxy: held >80% of DTE without exit gate firing on a losing long.
    if (is_long_vol
            and entry_dte is not None and entry_dte > 0
            and days_held >= entry_dte * 0.80
            and pnl_pct < -0.15):
        return (
            THETA_MISMANAGEMENT,
            "Carry note / winner gate (engine.py _long_option_doctrine)",
            f"Held {days_held:.0f}d of {entry_dte:.0f} DTE original ({days_held/entry_dte:.0%}) "
            "without exit or roll — theta consumed the edge "
            "(Passarelli Ch.5: time-to-be-right window expired)."
        )

    # ── 7. Premature exit (long vol only) ────────────────────────────────────
    # Exited with significant MFE already banked but no gate trigger — may have
    # left money on the table OR correctly took profits early (ambiguous without more context).
    if (is_long_vol
            and mfe_pct is not None and mfe_pct > 0.40
            and pnl_pct > 0.10 and pnl_pct < mfe_pct * 0.60):
        return (
            PREMATURE_EXIT,
            None,
            f"Exited at {pnl_pct:.0%} gain vs MFE of {mfe_pct:.0%} — "
            "possibly left significant upside unrealized "
            "(McMillan Ch.4: let winners run to the measured move target)."
        )

    # ── 8. Thesis completion (long vol) ──────────────────────────────────────
    if is_long_vol and pnl_pct >= 0.40:
        return (
            THESIS_COMPLETION,
            None,
            f"Option gained {pnl_pct:.0%} — directional thesis confirmed and realized "
            "(McMillan Ch.4: thesis completion)."
        )

    # ── 9. Theta harvest (short vol) ─────────────────────────────────────────
    if is_short_vol and ("EXPIRED" in exit_s or "ASSIGNED" in exit_s or pnl_pct >= 0.60):
        return (
            THETA_HARVEST,
            None,
            f"Premium captured: {pnl_pct:.0%} gain via theta decay "
            "(Passarelli Ch.6: let the clock work)."
        )

    # ── 10. Short-vol win (rolled or exited at profit) ────────────────────────
    if is_short_vol and pnl_pct > 0.10:
        return (
            THESIS_COMPLETION,
            None,
            f"Short-vol trade closed at {pnl_pct:.0%} gain "
            "(McMillan Ch.3: premium income realized)."
        )

    return (
        UNCLASSIFIED,
        None,
        "Insufficient signal combination to assign categorical outcome."
    )


def outcome_emoji(outcome_type: str) -> str:
    if outcome_type in WIN_TYPES:
        return "✅"
    if outcome_type in WARN_TYPES:
        return "⚠️"
    if outcome_type in LOSS_TYPES:
        return "❌"
    return "❓"
