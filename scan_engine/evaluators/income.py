"""
Income strategy evaluator — Cash-Secured Put, Covered Call, Buy-Write.

Imports all rules from ``doctrine.income_doctrine``; applies them in
sequence; accumulates a compliance score.

CRITICAL: Signal direction is SELL_VOL.
  RV/IV < 1.0 = FAVORABLE (IV > HV = selling rich premium).
  This is the OPPOSITE of volatility (which buys vol).
"""

from __future__ import annotations

import pandas as pd

from ._types import EvaluationResult
from ._shared import safe_get, safe_float, resolve_strategy_name
from .doctrine import income_doctrine as D


def evaluate_income(row: pd.Series) -> EvaluationResult:
    """Evaluate a single income-family row."""

    strategy = resolve_strategy_name(row)

    # ── Extract fields ────────────────────────────────────────
    theta = safe_float(row, 'Theta')
    vega = safe_float(row, 'Vega')
    gamma = safe_float(row, 'Gamma')
    iv_hv_gap = safe_float(row, 'IVHV_gap_30D', 'IV_HV_Gap')
    pop = safe_float(row, 'Probability_Of_Profit')
    rv_iv_ratio = safe_float(row, 'RV_IV_Ratio')
    iv_percentile = safe_float(row, 'IV_Percentile', 'IV_Rank', 'IV_Rank_30D', 'IV_Rank_XS')
    actual_dte = safe_float(row, 'Actual_DTE', 'DTE')

    trend = safe_get(row, 'Trend', 'Signal_Type')
    price_vs_sma20 = safe_float(row, 'Price_vs_SMA20')

    # IV momentum
    iv_30d_5d_roc = safe_float(row, 'IV_30D_5D_ROC')

    # ── Data completeness ─────────────────────────────────────
    missing: list[str] = []
    if theta is None:
        missing.append('Theta')
    if vega is None:
        missing.append('Vega')
    if iv_hv_gap is None:
        missing.append('IV_HV_Gap')

    if missing:
        data_completeness = ((3 - len(missing)) / 3) * 100
        return EvaluationResult(
            'Incomplete_Data', data_completeness, ', '.join(missing), 0.0,
            f"Missing required data: {', '.join(missing)}",
        )
    data_completeness = 100.0

    # ── Compliance scoring ────────────────────────────────────
    score = 100.0
    notes: list[str] = []
    abs_theta = abs(theta)

    # ── RV/IV ratio (SELL direction: < 1.0 = favorable) ──────
    if rv_iv_ratio is not None:
        if rv_iv_ratio > 1.25:
            score -= D.RV_IV_SEVERE.deduction
            notes.append(f"❌ {D.RV_IV_SEVERE.note_fail} (RV/IV={rv_iv_ratio:.2f}) [{D.RV_IV_SEVERE.citation}]")
        elif rv_iv_ratio > 1.10:
            score -= D.RV_IV_MARGINAL.deduction
            notes.append(f"⚠️ {D.RV_IV_MARGINAL.note_fail} (RV/IV={rv_iv_ratio:.2f})")
        elif rv_iv_ratio > 1.00:
            score -= 15
            notes.append(f"⚠️ Marginal edge (RV/IV={rv_iv_ratio:.2f} > 1.0 — HV slightly above IV)")
        elif D.RV_IV_STRONG.check(rv_iv_ratio):
            notes.append(f"✅ {D.RV_IV_STRONG.note_pass} (RV/IV={rv_iv_ratio:.2f}) [{D.RV_IV_STRONG.citation}]")
        else:
            notes.append(f"✅ {D.RV_IV_EDGE.note_pass} (RV/IV={rv_iv_ratio:.2f})")
    elif iv_hv_gap is not None:
        if iv_hv_gap <= 0:
            score -= 30
            notes.append(f"IV <= RV (gap={iv_hv_gap:.1f} — not selling rich premium)")
        else:
            notes.append(f"✅ IV > RV (gap={iv_hv_gap:.1f} — premium collection justified)")
    else:
        score -= 25
        notes.append("Missing RV/IV data (cannot validate premium selling edge — CRITICAL)")

    # ── Theta / Vega extreme ratio ────────────────────────────
    if abs_theta > 0 and vega / abs_theta > D.VEGA_THETA_EXTREME.threshold:
        score -= D.VEGA_THETA_EXTREME.deduction
        notes.append(f"{D.VEGA_THETA_EXTREME.note_fail} ({vega/abs_theta:.1f}x)")
    else:
        notes.append(f"✅ Acceptable greek profile for income: theta={abs_theta:.3f}, vega={vega:.3f}")

    # ── Gamma sign (Natenberg Ch.7) ───────────────────────────
    # Price-normalize gamma: raw gamma scales inversely with stock price.
    # γ × S / 100 = delta change per 1% move (same economic sensitivity).
    _stock_price_i = safe_float(row, 'Stock_Price', 'last_price', 'Approx_Stock_Price', default=0)
    _gamma_norm_i = (gamma * _stock_price_i / 100.0) if (_stock_price_i > 0 and gamma is not None) else gamma
    if gamma is not None:
        if _gamma_norm_i is not None and _gamma_norm_i > 0.05:
            score -= D.GAMMA_NEGATIVE.deduction
            notes.append(f"⚠️ {D.GAMMA_NEGATIVE.note_fail} (Gamma={gamma:.3f}, γ·S/100={_gamma_norm_i:.3f}) [{D.GAMMA_NEGATIVE.citation}]")
        elif gamma > 0:
            notes.append(f"ℹ️ Near-zero positive Gamma ({gamma:.3f}) — confirm short-option leg")
        else:
            notes.append(f"✅ Negative Gamma ({gamma:.3f}) — confirms short premium structure")

        # Short-DTE gamma spike — also price-normalized
        if D.SHORT_DTE_GAMMA_SPIKE.check(actual_dte=actual_dte, gamma=_gamma_norm_i):
            score -= D.SHORT_DTE_GAMMA_SPIKE.deduction
            notes.append(f"⚠️ {D.SHORT_DTE_GAMMA_SPIKE.note_fail} (DTE={actual_dte:.0f}, Gamma={gamma:.3f}, γ·S/100={_gamma_norm_i:.3f})")
        elif actual_dte is not None and actual_dte < 21:
            notes.append(f"ℹ️ DTE {actual_dte:.0f}d < 21 — monitor short gamma exposure")

    # ── POP gate (GRADUATED: <50 reject, 50-65 penalty, >=65 pass) ──
    if pop is not None:
        ded, note, is_reject = D.POP_GATE.evaluate(pop)
        if is_reject:
            return EvaluationResult(
                'Reject', data_completeness, '', 0.0,
                f"❌ LOW POP: {pop:.1f}% < 50% ({D.POP_GATE.citation})",
            )
        score -= ded
        notes.append(f"{note} (POP={pop:.1f}%)")
    else:
        if strategy in ('Covered Call', 'Buy-Write'):
            score -= 5
            notes.append("POP not calculated for covered call (stock ownership is the hedge)")
        else:
            score -= 25
            notes.append("❌ POP not calculated (Cohen Ch.28: win rate validation REQUIRED)")

    # ── IV Momentum (income: rising IV = mild headwind) ───────
    if iv_30d_5d_roc is not None:
        if D.IV_MOMENTUM_RISING.check(iv_30d_5d_roc=iv_30d_5d_roc):
            score -= D.IV_MOMENTUM_RISING.deduction
            notes.append(f"⚠️ {D.IV_MOMENTUM_RISING.note_fail} (5D ROC={iv_30d_5d_roc:.2f})")
        else:
            notes.append(f"✅ {D.IV_MOMENTUM_RISING.note_pass}")

    # ── Market structure (Murphy) ─────────────────────────────
    if strategy in ('Cash-Secured Put', 'CSP'):
        if trend is not None and not D.CSP_TREND_BULLISH.check(trend=trend):
            score -= D.CSP_TREND_BULLISH.deduction
            notes.append(f"{D.CSP_TREND_BULLISH.note_fail} ({trend})")
        if price_vs_sma20 is not None and not D.CSP_PRICE_ABOVE_SMA20.check(price_vs_sma20=price_vs_sma20):
            score -= D.CSP_PRICE_ABOVE_SMA20.deduction
            notes.append(f"{D.CSP_PRICE_ABOVE_SMA20.note_fail}")
    elif strategy in ('Covered Call', 'Buy-Write'):
        if not D.CC_NOT_BEARISH.check(trend=trend):
            score -= D.CC_NOT_BEARISH.deduction
            notes.append(f"{D.CC_NOT_BEARISH.note_fail} [{D.CC_NOT_BEARISH.citation}]")

    # ── Chart signal confirmation (Murphy, RAG-backed) ────────
    rsi = safe_float(row, 'RSI')
    slowk = safe_float(row, 'SlowK_5_3')
    bb_pos = safe_float(row, 'BB_Position')
    chart_regime = safe_get(row, 'Chart_Regime')

    # Chart Regime — income strategies THRIVE in ranging/compressed, SUFFER in trends
    # Murphy (0.722): "trend-following systems do not work in sideways phases"
    # (Inverse: premium-selling works best in sideways/compressed)
    if chart_regime is not None:
        _regime_lower = str(chart_regime).lower()
        if _regime_lower in ('ranging', 'compressed'):
            score += 5
            notes.append(f"✅ Regime {chart_regime} — favorable for premium selling (Murphy)")
        elif _regime_lower == 'strong_trend':
            score -= 8
            notes.append(f"Regime {chart_regime} — risky for premium sellers (Murphy: trend market)")
        elif _regime_lower == 'trending':
            score -= 5
            notes.append(f"Regime {chart_regime} — moderate risk for income (confirmed trend)")
        elif _regime_lower == 'emerging_trend':
            notes.append(f"Regime {chart_regime} — monitor trend development")

    # RSI entry timing for CSP — oversold = better support entry
    # Murphy (0.678): "30 = oversold" — ideal for put sellers seeking support
    if rsi is not None:
        if strategy in ('Cash-Secured Put', 'CSP'):
            if rsi < 35:
                score += 5
                notes.append(f"✅ RSI {rsi:.0f} oversold — ideal CSP entry at support (Murphy Ch.10)")
            elif rsi > 75:
                score -= 3
                notes.append(f"RSI {rsi:.0f} overbought — CSP entry at potential top (Murphy)")
        elif strategy in ('Covered Call', 'Buy-Write'):
            if rsi > 65:
                score += 3
                notes.append(f"✅ RSI {rsi:.0f} elevated — favorable for call overwriting (Murphy)")
            elif rsi < 30:
                score -= 3
                notes.append(f"RSI {rsi:.0f} oversold — stock may drop, call premium insufficient (Murphy)")

    # Stochastic — Murphy (0.729): oversold/overbought zones for income timing
    if slowk is not None:
        if strategy in ('Cash-Secured Put', 'CSP') and slowk < 20:
            score += 3
            notes.append(f"✅ Stochastic %K={slowk:.0f} oversold — confirms CSP support level (Murphy)")
        elif strategy in ('Covered Call', 'Buy-Write') and slowk > 80:
            score += 3
            notes.append(f"✅ Stochastic %K={slowk:.0f} overbought — confirms CC resistance level (Murphy)")

    # Bollinger Band position — CSP near lower band = good, CC near upper = good
    # Murphy (0.666): "touch lower band = oversold, touch upper band = overbought"
    if bb_pos is not None:
        if strategy in ('Cash-Secured Put', 'CSP') and bb_pos < 15:
            score += 5
            notes.append(f"✅ BB_Position {bb_pos:.0f}% near lower band — strong CSP support (Murphy)")
        elif strategy in ('Covered Call', 'Buy-Write') and bb_pos > 85:
            score += 3
            notes.append(f"✅ BB_Position {bb_pos:.0f}% near upper band — CC at resistance (Murphy)")

    # ── Institutional-grade signals (Murphy, Bulkowski, Raschke) ──

    # Market Structure — income thrives in consolidation
    market_structure = safe_get(row, 'Market_Structure')
    if market_structure is not None and market_structure != 'Unknown':
        _ms_lower = str(market_structure).lower()
        if _ms_lower == 'consolidation':
            score += 3
            notes.append(f"✅ Market structure Consolidation — stable for premium selling (Murphy)")
        elif _ms_lower == 'downtrend' and strategy in ('Cash-Secured Put', 'CSP'):
            score -= 5
            notes.append(f"Market structure Downtrend — risk for CSP assignment (Murphy Ch.4)")
        elif _ms_lower == 'uptrend' and strategy in ('Cash-Secured Put', 'CSP'):
            score += 3
            notes.append(f"✅ Market structure Uptrend — favorable for CSP (Murphy Ch.4)")

    # OBV slope — distribution is warning for CSP
    obv_slope = safe_float(row, 'OBV_Slope')
    if obv_slope is not None:
        if strategy in ('Cash-Secured Put', 'CSP') and obv_slope < -10:
            score -= 3
            notes.append(f"OBV distributing ({obv_slope:+.0f}%) — smart money selling (Murphy Ch.7)")
        elif strategy in ('Cash-Secured Put', 'CSP') and obv_slope > 5:
            notes.append(f"✅ OBV accumulating ({obv_slope:+.0f}%) — supports CSP (Murphy Ch.7)")

    # RSI Divergence — bearish divergence is warning for CSP
    rsi_div = safe_get(row, 'RSI_Divergence')
    if rsi_div == 'Bearish_Divergence' and strategy in ('Cash-Secured Put', 'CSP'):
        score -= 5
        notes.append(f"Bearish RSI divergence — support may fail for CSP (Murphy)")
    elif rsi_div == 'Bullish_Divergence' and strategy in ('Covered Call', 'Buy-Write'):
        score -= 3
        notes.append(f"Bullish RSI divergence — stock may rally through CC strike (Murphy)")

    # Weekly Trend Bias — CSP needs bullish weekly, CC needs non-bearish
    weekly_bias = safe_get(row, 'Weekly_Trend_Bias')
    if weekly_bias == 'CONFLICTING':
        if strategy in ('Cash-Secured Put', 'CSP'):
            score -= 5
            notes.append(f"Weekly trend CONFLICTING — risky for CSP (Murphy: weekly filters daily)")
        elif strategy in ('Covered Call', 'Buy-Write'):
            score -= 3
            notes.append(f"Weekly trend CONFLICTING — CC may get called away (Murphy)")
    elif weekly_bias == 'ALIGNED':
        notes.append(f"✅ Weekly trend ALIGNED (Murphy: higher TF support)")

    # Keltner Squeeze — in squeeze = low vol = good for income; firing = bad
    squeeze_on = row.get('Keltner_Squeeze_On', False)
    squeeze_fired = row.get('Keltner_Squeeze_Fired', False)
    if squeeze_fired:
        score -= 5
        notes.append(f"Keltner squeeze FIRED — volatility expanding, risky for income (Raschke)")
    elif squeeze_on:
        score += 3
        notes.append(f"✅ Keltner squeeze active — low vol environment for premium selling (Raschke)")

    # ── Final status ──────────────────────────────────────────
    if score >= 70:
        status = 'Valid'
        notes.insert(0, "✅ Meets income strategy requirements")
    elif score >= 50:
        status = 'Watch'
        notes.insert(0, "⚠️ Marginal income setup")
    else:
        status = 'Reject'
        notes.insert(0, "❌ Fails income strategy requirements")

    return EvaluationResult(status, data_completeness, '', score, ' | '.join(notes))
