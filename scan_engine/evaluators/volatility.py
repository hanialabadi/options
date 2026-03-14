"""
Volatility strategy evaluator — Straddle, Strangle.

Imports all rules from ``doctrine.volatility_doctrine``; applies them in
sequence; accumulates a compliance score.

CRITICAL: Signal direction is BUY_VOL.
  RV/IV > 1.0 = FAVORABLE (HV > IV = options cheap).
  This is the OPPOSITE of income (which sells vol).
"""

from __future__ import annotations

import math
import pandas as pd

from ._types import EvaluationResult
from ._shared import safe_get, safe_float, resolve_strategy_name
from .doctrine import volatility_doctrine as D


def evaluate_volatility(row: pd.Series) -> EvaluationResult:
    """Evaluate a single volatility-family row."""

    strategy = resolve_strategy_name(row)

    # ── Extract fields ────────────────────────────────────────
    delta = safe_float(row, 'Delta')
    gamma = safe_float(row, 'Gamma')
    vega = safe_float(row, 'Vega')
    theta = safe_float(row, 'Theta')
    skew = safe_float(row, 'Put_Call_Skew')
    rv_iv_ratio = safe_float(row, 'RV_IV_Ratio')

    iv_percentile = safe_float(row, 'IV_Percentile', 'IV_Rank', 'IV_Rank_30D', 'IV_Rank_XS')
    catalyst = safe_float(row, 'Earnings_Days_Away', 'Event_Risk')
    actual_dte = safe_float(row, 'Actual_DTE', 'DTE', default=45)

    # Stock price (for dollar-gamma normalization)
    stock_px = safe_float(row, 'Stock_Price', 'last_price', 'closePrice')

    # Regime
    vol_regime = safe_get(row, 'Volatility_Regime', 'Regime')
    if not vol_regime or vol_regime == 'Unknown':
        hv_regime = str(row.get('volatility_regime') or '')
        _MAP = {
            'High_Expansion': 'Expansion', 'Normal_Expansion': 'Expansion',
            'High_Contraction': 'High Vol', 'High': 'High Vol',
            'Normal_Compression': 'Compression', 'High_Compression': 'Compression',
            'Low_Compression': 'Low Vol',
            'Normal_Contraction': 'Compression', 'Normal': 'Compression',
        }
        vol_regime = _MAP.get(hv_regime) or vol_regime

    vvix = safe_float(row, 'VVIX', 'Vol_of_Vol')
    recent_vol_spike = safe_get(row, 'Recent_Vol_Spike')
    iv_term_structure = safe_get(row, 'Surface_Shape', 'IV_Term_Structure')

    # IV momentum fields
    iv_30d_5d_roc = safe_float(row, 'IV_30D_5D_ROC')
    iv_30d_10d_roc = safe_float(row, 'IV_30D_10D_ROC')

    # Total debit for expected-move coverage
    total_debit = safe_float(row, 'Total_Debit', 'Ask')

    is_strangle = 'strangle' in strategy.lower()
    is_straddle_strangle = strategy in ['Long Straddle', 'Long Strangle', 'Straddle', 'Strangle']

    # ── Data completeness ─────────────────────────────────────
    missing: list[str] = []
    if vega is None:
        missing.append('Vega')
    if delta is None:
        missing.append('Delta')
    if skew is None:
        missing.append('Skew')
    if iv_percentile is None:
        missing.append('IV_Percentile')

    data_completeness = ((4 - len(missing)) / 4) * 100

    # Vega is truly critical
    if vega is None:
        return EvaluationResult(
            'Incomplete_Data', data_completeness, ', '.join(missing), 0.0,
            f"❌ CRITICAL data missing: {', '.join(missing)} (Vega REQUIRED for vol strategies)",
        )

    # ── Compliance scoring ────────────────────────────────────
    score = 100.0
    notes: list[str] = []
    abs_delta = abs(delta) if delta is not None else 0.5

    # ── HARD GATES (immediate reject) ─────────────────────────

    # Skew hard gate
    if skew is not None and D.SKEW_HARD_GATE.check(skew):
        return EvaluationResult(
            'Reject', data_completeness, '', 0.0,
            f"❌ SKEW VIOLATION: {skew:.2f} > 1.20 ({D.SKEW_HARD_GATE.citation})",
        )
    elif 'Skew' in missing:
        score -= D.SKEW_MISSING_PENALTY
        notes.append("Skew data unavailable — cannot verify put/call parity")

    # VVIX hard gate
    if vvix is not None and D.VVIX_HARD_GATE.check(vvix):
        return EvaluationResult(
            'Reject', data_completeness, '', 0.0,
            f"❌ HIGH VVIX: {vvix:.0f} > 130 ({D.VVIX_HARD_GATE.citation})",
        )

    # Vol spike hard gate (Sinclair Ch.4)
    if recent_vol_spike is not None and recent_vol_spike:
        days_since = safe_float(row, 'Days_Since_Vol_Spike', default=0)
        if days_since is not None and days_since < D.VOL_SPIKE_RECENT_DAYS:
            return EvaluationResult(
                'Reject', data_completeness, '', 0.0,
                f"❌ RECENT VOL SPIKE: {days_since:.0f} days ago (Sinclair Ch.4: wait for mean reversion)",
            )
        elif days_since is not None:
            score -= 15
            notes.append(f"⚠️ Vol spike {days_since:.0f} days ago (Sinclair: monitor for clustering)")
        else:
            score -= 25
            notes.append("❌ Recent vol spike detected (Sinclair: clustering risk)")

    # ── Vega ──────────────────────────────────────────────────
    if not D.VEGA_FLOOR.check(vega):
        score -= D.VEGA_FLOOR.deduction
        notes.append(f"Low Vega ({vega:.2f} < 0.40) [{D.VEGA_FLOOR.citation}]")

    # ── Gamma ─────────────────────────────────────────────────
    if gamma is not None:
        if gamma <= 0:
            score -= D.GAMMA_NEGATIVE_REJECT.deduction
            notes.append(f"❌ {D.GAMMA_NEGATIVE_REJECT.note_fail} (Gamma={gamma:.3f})")
        elif stock_px and stock_px > 0:
            dollar_gamma = gamma * stock_px
            dgamma_floor = D.GAMMA_DOLLAR_FLOOR_STRANGLE if is_strangle else D.GAMMA_DOLLAR_FLOOR_STRADDLE
            if not dgamma_floor.check(dollar_gamma):
                score -= dgamma_floor.deduction
                notes.append(f"Low dollar-Gamma (${dollar_gamma:.2f} < ${dgamma_floor.threshold:.0f}; Gamma={gamma:.4f} x ${stock_px:.0f})")
            else:
                notes.append(f"✅ Gamma adequate (${dollar_gamma:.2f} dollar-gamma >= ${dgamma_floor.threshold:.0f})")
        else:
            ps_floor = D.GAMMA_PER_SHARE_STRANGLE if is_strangle else D.GAMMA_PER_SHARE_STRADDLE
            if gamma < ps_floor:
                score -= 20
                notes.append(f"Low Gamma ({gamma:.3f} < {ps_floor}; stock price unknown)")
            else:
                notes.append(f"✅ Gamma adequate ({gamma:.3f} >= {ps_floor})")
    else:
        score -= 20
        notes.append("Missing Gamma (cannot validate convexity)")

    # ── Gamma / Theta convexity efficiency ────────────────────
    if gamma is not None and theta is not None and theta != 0:
        abs_theta = abs(theta)
        if stock_px and stock_px > 0:
            gt_ratio = (gamma * stock_px) / abs_theta if abs_theta > 1e-9 else 99.0
        else:
            gt_ratio = gamma / abs_theta if abs_theta > 1e-9 else 99.0

        ded, note, _ = D.GAMMA_THETA_RATIO.evaluate(gt_ratio)
        score -= ded
        notes.append(f"{note} (gamma/theta={gt_ratio:.2f}) [{D.GAMMA_THETA_RATIO.citation}]")

    # ── Delta neutral ─────────────────────────────────────────
    if not D.DELTA_NEUTRAL.check(abs_delta):
        score -= D.DELTA_NEUTRAL.deduction
        notes.append(f"Directional bias (|Delta|={abs_delta:.2f} > 0.15) [{D.DELTA_NEUTRAL.citation}]")

    # ── IV percentile ─────────────────────────────────────────
    if iv_percentile is not None:
        if D.IV_PERCENTILE_TOO_HIGH.check(iv_percentile):
            score -= D.IV_PERCENTILE_TOO_HIGH.deduction
            notes.append(f"⚠️ {D.IV_PERCENTILE_TOO_HIGH.note_fail} (IV%ile={iv_percentile:.0f})")
        elif D.IV_PERCENTILE_ELEVATED.check(iv_percentile):
            score -= D.IV_PERCENTILE_ELEVATED.deduction
            notes.append(f"{D.IV_PERCENTILE_ELEVATED.note_fail} (IV%ile={iv_percentile:.0f})")
        elif D.IV_PERCENTILE_VERY_LOW.check(iv_percentile):
            score -= D.IV_PERCENTILE_VERY_LOW.deduction
            notes.append(f"{D.IV_PERCENTILE_VERY_LOW.note_fail} (IV%ile={iv_percentile:.0f})")
        else:
            notes.append(f"✅ {D.IV_PERCENTILE_SWEET.note_pass} (IV%ile={iv_percentile:.0f})")
    else:
        score -= 10
        notes.append("Missing IV percentile (vol edge unvalidated)")

    # ── RV/IV ratio (CORRECT DIRECTION: > 1.0 = FAVORABLE) ───
    if rv_iv_ratio is not None:
        if D.RV_IV_STRONG.check(rv_iv_ratio):
            notes.append(f"✅ {D.RV_IV_STRONG.note_pass} (RV/IV={rv_iv_ratio:.2f})")
        elif D.RV_IV_EDGE.check(rv_iv_ratio):
            notes.append(f"✅ {D.RV_IV_EDGE.note_pass} (RV/IV={rv_iv_ratio:.2f})")
        elif rv_iv_ratio > 0.85:
            score -= D.RV_IV_MARGINAL.deduction
            notes.append(f"⚠️ {D.RV_IV_MARGINAL.note_fail} (RV/IV={rv_iv_ratio:.2f})")
        elif rv_iv_ratio > 0.70:
            score -= D.RV_IV_WEAK.deduction
            notes.append(f"⚠️ {D.RV_IV_WEAK.note_fail} (RV/IV={rv_iv_ratio:.2f})")
        else:
            score -= D.RV_IV_SEVERE.deduction
            notes.append(f"❌ {D.RV_IV_SEVERE.note_fail} (RV/IV={rv_iv_ratio:.2f})")
    else:
        score -= 15
        notes.append("Missing RV/IV ratio (vol edge unvalidated)")

    # ── Term structure (IV30 / IV60) ──────────────────────────
    iv30 = safe_float(row, 'IV_30D', 'iv_30d')
    iv60 = safe_float(row, 'IV_60D', 'iv_60d')
    if iv30 is not None and iv60 is not None and iv60 > 0:
        # Severe inversion check first
        if not D.TERM_STRUCTURE_SEVERE.check(iv30=iv30, iv60=iv60):
            score -= D.TERM_STRUCTURE_SEVERE.deduction
            notes.append(f"❌ {D.TERM_STRUCTURE_SEVERE.note_fail} (IV30={iv30:.1f}, IV60={iv60:.1f})")
        elif not D.TERM_STRUCTURE.check(iv30=iv30, iv60=iv60):
            score -= D.TERM_STRUCTURE.deduction
            notes.append(f"⚠️ {D.TERM_STRUCTURE.note_fail} (IV30={iv30:.1f}, IV60={iv60:.1f})")
        else:
            notes.append(f"✅ {D.TERM_STRUCTURE.note_pass} (IV30={iv30:.1f}, IV60={iv60:.1f})")
    elif iv_term_structure is not None:
        # Fallback to Surface_Shape string
        ts_upper = str(iv_term_structure).upper()
        if ts_upper == 'INVERTED':
            score -= 10
            notes.append("⚠️ Inverted term structure (Sinclair: front vol overpriced)")
        elif ts_upper == 'CONTANGO':
            notes.append("✅ Normal term structure (favorable for long vol)")

    # ── IV Momentum ───────────────────────────────────────────
    # Severe first (collapsing)
    if iv_30d_10d_roc is not None:
        if not D.IV_MOMENTUM_COLLAPSING.check(iv_30d_10d_roc=iv_30d_10d_roc):
            score -= D.IV_MOMENTUM_COLLAPSING.deduction
            notes.append(f"❌ {D.IV_MOMENTUM_COLLAPSING.note_fail} (10D ROC={iv_30d_10d_roc:.2f})")

    if iv_30d_5d_roc is not None:
        if not D.IV_MOMENTUM_FALLING.check(iv_30d_5d_roc=iv_30d_5d_roc):
            score -= D.IV_MOMENTUM_FALLING.deduction
            notes.append(f"⚠️ {D.IV_MOMENTUM_FALLING.note_fail} (5D ROC={iv_30d_5d_roc:.2f})")
        else:
            notes.append(f"✅ {D.IV_MOMENTUM_FALLING.note_pass}")

    # ── Expected Move Coverage ────────────────────────────────
    if stock_px and stock_px > 0 and iv_percentile is not None and actual_dte is not None and total_debit is not None and total_debit > 0:
        # Use IV_30D if available, else approximate from iv_percentile
        iv_for_move = iv30 if iv30 is not None else None
        if iv_for_move is not None and iv_for_move > 0:
            expected_move = stock_px * (iv_for_move / 100) * math.sqrt(actual_dte / 365)
            em_ratio = expected_move / total_debit
            ded, note, _ = D.EXPECTED_MOVE_COVERAGE.evaluate(em_ratio)
            score -= ded
            notes.append(f"{note} (EM/debit={em_ratio:.2f}) [{D.EXPECTED_MOVE_COVERAGE.citation}]")

    # ── Regime gating (Sinclair) ──────────────────────────────
    if vol_regime is not None:
        if not D.REGIME_EXPANSION.check(vol_regime=vol_regime):
            score -= D.REGIME_EXPANSION.deduction
            notes.append(f"❌ {D.REGIME_EXPANSION.note_fail} ({vol_regime}) [{D.REGIME_EXPANSION.citation}]")
        elif D.REGIME_FAVORABLE.check(vol_regime=vol_regime):
            notes.append(f"✅ {D.REGIME_FAVORABLE.note_pass} ({vol_regime})")
        else:
            score -= 10
            notes.append(f"⚠️ Neutral regime ({vol_regime})")
    else:
        score -= D.REGIME_MISSING.deduction
        notes.append(f"{D.REGIME_MISSING.note_fail} [{D.REGIME_MISSING.citation}]")

    # VVIX elevated (non-reject)
    if vvix is not None and not D.VVIX_HARD_GATE.check(vvix):
        if D.VVIX_ELEVATED.check(vvix):
            score -= D.VVIX_ELEVATED.deduction
            notes.append(f"⚠️ {D.VVIX_ELEVATED.note_fail} (VVIX={vvix:.0f})")
        else:
            notes.append(f"✅ {D.VVIX_ELEVATED.note_pass} (VVIX={vvix:.0f})")

    # ── Catalyst (Sinclair Ch.3) ──────────────────────────────
    if is_straddle_strangle:
        if not D.CATALYST_MISSING.check(catalyst=catalyst):
            score -= D.CATALYST_MISSING.deduction
            notes.append(f"⚠️ {D.CATALYST_MISSING.note_fail}")
        elif catalyst is not None:
            notes.append(f"✅ {D.CATALYST_NEAR_TERM.note_pass}: {catalyst:.0f} days")
    else:
        if catalyst is None:
            score -= 15
            notes.append("No catalyst identified (generic vol bet)")
        else:
            notes.append(f"✅ Catalyst present: {catalyst}")

    # ── Sinclair: Term structure string fallback (already handled above) ──

    # ── Keltner Squeeze — critical for vol strategies (Raschke / Murphy 0.739)
    squeeze_on = row.get('Keltner_Squeeze_On', False)
    squeeze_fired = row.get('Keltner_Squeeze_Fired', False)
    if squeeze_on:
        score += 8
        notes.append(f"✅ Keltner squeeze active — compression before expansion (Raschke: ideal vol setup)")
    elif squeeze_fired:
        score += 5
        notes.append(f"✅ Keltner squeeze FIRED — vol expansion underway (Raschke: entry signal)")

    # Market Structure — consolidation favors vol compression plays
    market_structure = safe_get(row, 'Market_Structure')
    if market_structure is not None and str(market_structure).lower() == 'consolidation':
        score += 3
        notes.append(f"✅ Market structure Consolidation — compression precedes expansion (Murphy)")

    # ── Final status ──────────────────────────────────────────
    gamma_str = f", Gamma={gamma:.3f}" if gamma is not None else ""
    if score >= 70:
        status = 'Valid'
        skew_str = f", Skew={skew:.2f}" if skew is not None else ""
        notes.insert(0, f"✅ Meets vol strategy requirements (Vega={vega:.2f}{skew_str}{gamma_str})")
    elif score >= 50:
        status = 'Watch'
        notes.insert(0, "⚠️ Marginal vol setup (consider stronger edge)")
    else:
        status = 'Reject'
        notes.insert(0, "❌ Fails vol strategy requirements (RAG violations)")

    return EvaluationResult(
        status, data_completeness,
        ', '.join(missing) if missing else '',
        score, ' | '.join(notes),
    )
