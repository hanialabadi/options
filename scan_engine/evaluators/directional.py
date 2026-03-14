"""
Directional strategy evaluator — Long Call/Put, LEAPs, Debit Spreads.

Imports all rules from ``doctrine.directional_doctrine``; applies them in
sequence; accumulates a compliance score.
"""

from __future__ import annotations

import pandas as pd

from ._types import EvaluationResult, BULLISH_STRATEGIES, BEARISH_STRATEGIES
from ._shared import safe_get, safe_float, resolve_strategy_name, check_required_data
from .doctrine import directional_doctrine as D


def evaluate_directional(row: pd.Series) -> EvaluationResult:
    """Evaluate a single directional-family row."""

    strategy = resolve_strategy_name(row)
    contract_status = row.get('Contract_Status', 'OK')
    is_leap_fallback = contract_status == 'LEAP_FALLBACK'

    delta = safe_float(row, 'Delta')
    gamma = safe_float(row, 'Gamma')
    vega = safe_float(row, 'Vega')
    actual_dte = safe_float(row, 'Actual_DTE', 'DTE', default=45)

    trend = safe_get(row, 'Trend', 'Signal_Type')
    price_vs_sma20 = safe_float(row, 'Price_vs_SMA20')
    volume_trend = safe_get(row, 'Volume_Trend')

    chart_pattern = safe_get(row, 'Chart_Pattern')
    pattern_confidence = safe_float(row, 'Pattern_Confidence')
    candlestick_pattern = safe_get(row, 'Candlestick_Pattern')
    entry_timing = safe_get(row, 'Entry_Timing_Quality')

    # ── Data completeness ─────────────────────────────────────
    missing, _ = check_required_data(row, {
        'Delta': 'Delta', 'Gamma': 'Gamma', 'Vega': 'Vega',
    })
    if missing:
        return EvaluationResult(
            'Incomplete_Data', 33.0, ', '.join(missing), 0.0,
            f"Missing Greeks: {', '.join(missing)} (REQUIRED for directional)",
        )
    data_completeness = 100.0

    # ── Compliance scoring ────────────────────────────────────
    score = 100.0
    notes: list[str] = []
    abs_delta = abs(delta)
    is_leap = 'leap' in strategy.lower() or (actual_dte is not None and actual_dte >= 180)

    # Price-normalize gamma: γ × S / 100 = delta change per 1% stock move.
    # Raw gamma scales inversely with stock price, making fixed thresholds
    # unfairly penalize high-priced stocks (LLY, AVGO, MELI, NVR).
    # Passarelli Ch.4 thresholds were calibrated for ~$100 stocks.
    _stock_price = safe_float(row, 'Stock_Price', 'last_price', 'Approx_Stock_Price', default=0)
    if _stock_price > 0:
        gamma_norm = gamma * _stock_price / 100.0
    else:
        gamma_norm = gamma  # fallback: use raw if no stock price

    # Delta conviction
    if not D.DELTA_CONVICTION.check(abs_delta):
        score -= D.DELTA_CONVICTION.deduction
        notes.append(f"Weak Delta ({abs_delta:.2f} < 0.45) [{D.DELTA_CONVICTION.citation}]")

    # Gamma floor (LEAP vs standard) — price-normalized
    if is_leap:
        rule = D.GAMMA_FLOOR_LEAP
        gamma_label = "LEAP"
    else:
        rule = D.GAMMA_FLOOR
        gamma_label = "directional"
    if not rule.check(gamma_norm):
        score -= rule.deduction
        notes.append(
            f"Low Gamma ({gamma:.3f} raw, γ·S/100={gamma_norm:.3f} < {rule.threshold}"
            f" — insufficient convexity for {gamma_label}; {rule.citation})"
        )
    else:
        notes.append(
            f"✅ Gamma {gamma:.3f} (γ·S/100={gamma_norm:.3f} >= {rule.threshold},"
            f" convexity adequate for {gamma_label})"
        )

    # Weak conviction gate — uses normalized gamma
    if not D.WEAK_CONVICTION.check(abs_delta=abs_delta, gamma=gamma_norm, is_leap=is_leap):
        score -= D.WEAK_CONVICTION.deduction
        notes.append(f"{D.WEAK_CONVICTION.note_fail} [{D.WEAK_CONVICTION.citation}]")

    # Vega floor
    if vega is not None and not D.VEGA_FLOOR.check(vega):
        score -= D.VEGA_FLOOR.deduction
        notes.append(f"Low Vega ({vega:.2f}) [{D.VEGA_FLOOR.citation}]")

    # ── Trend alignment (Murphy Ch.4-6) ──────────────────────
    is_bullish = strategy in BULLISH_STRATEGIES
    is_bearish = strategy in BEARISH_STRATEGIES

    if trend is not None:
        if is_bullish and not D.BULLISH_TREND.check(trend=trend):
            score -= D.BULLISH_TREND.deduction
            notes.append(f"Trend misalignment ({trend}) [{D.BULLISH_TREND.citation}]")
        elif is_bullish:
            notes.append(f"✅ Trend aligned ({trend} — Murphy)")
        elif is_bearish and not D.BEARISH_TREND.check(trend=trend):
            score -= D.BEARISH_TREND.deduction
            notes.append(f"Trend misalignment ({trend}) [{D.BEARISH_TREND.citation}]")
        elif is_bearish:
            notes.append(f"✅ Trend aligned ({trend} — Murphy)")
    else:
        score -= D.MISSING_TREND.deduction
        notes.append(f"{D.MISSING_TREND.note_fail} [{D.MISSING_TREND.citation}]")

    # Price structure (Murphy)
    if price_vs_sma20 is not None:
        if is_bullish and not D.PRICE_VS_SMA20_BULLISH.check(price_vs_sma20=price_vs_sma20):
            score -= D.PRICE_VS_SMA20_BULLISH.deduction
            notes.append(f"Price below SMA20 ({price_vs_sma20:.2f}) [{D.PRICE_VS_SMA20_BULLISH.citation}]")
        elif is_bearish and not D.PRICE_VS_SMA20_BEARISH.check(price_vs_sma20=price_vs_sma20):
            score -= D.PRICE_VS_SMA20_BEARISH.deduction
            notes.append(f"Price above SMA20 ({price_vs_sma20:.2f}) [{D.PRICE_VS_SMA20_BEARISH.citation}]")

    # ── Volume (Murphy Ch.6) ──────────────────────────────────
    if volume_trend is not None:
        if is_bullish:
            if D.VOLUME_BULLISH.check(volume_trend=volume_trend):
                notes.append(f"✅ {D.VOLUME_BULLISH.note_pass} ({volume_trend})")
            else:
                score -= D.VOLUME_BULLISH.deduction
                notes.append(f"{D.VOLUME_BULLISH.note_fail} ({volume_trend})")
        elif is_bearish:
            if D.VOLUME_BEARISH.check(volume_trend=volume_trend):
                notes.append(f"✅ {D.VOLUME_BEARISH.note_pass} ({volume_trend})")
            else:
                score -= D.VOLUME_BEARISH.deduction
                notes.append(f"{D.VOLUME_BEARISH.note_fail} ({volume_trend})")
    else:
        score -= D.VOLUME_MISSING.deduction
        notes.append(f"{D.VOLUME_MISSING.note_fail} [{D.VOLUME_MISSING.citation}]")

    # ── Chart pattern (Bulkowski) ─────────────────────────────
    if chart_pattern is not None and pattern_confidence is not None:
        if D.PATTERN_HIGH_CONF.check(pattern_confidence):
            score -= D.PATTERN_HIGH_CONF.deduction  # negative = bonus
            notes.append(f"✅ {D.PATTERN_HIGH_CONF.note_pass}: {chart_pattern} ({pattern_confidence:.0f}%)")
        elif D.PATTERN_MODERATE_CONF.check(pattern_confidence):
            score -= D.PATTERN_MODERATE_CONF.deduction
            notes.append(f"✅ {D.PATTERN_MODERATE_CONF.note_pass}: {chart_pattern} ({pattern_confidence:.0f}%)")
        elif D.PATTERN_LOW_CONF.check(pattern_confidence):
            score -= D.PATTERN_LOW_CONF.deduction
            notes.append(f"{D.PATTERN_LOW_CONF.note_fail}: {chart_pattern} ({pattern_confidence:.0f}%)")
        else:
            notes.append(f"Pattern detected: {chart_pattern}")

    # ── Entry timing (Nison) ──────────────────────────────────
    is_short_term = actual_dte is not None and actual_dte < 30
    if is_short_term:
        if candlestick_pattern is not None:
            if D.ENTRY_STRONG.check(entry_timing=entry_timing):
                score -= D.ENTRY_STRONG.deduction
                notes.append(f"✅ Entry confirmed: {candlestick_pattern} (Nison: Strong)")
            elif D.ENTRY_MODERATE.check(entry_timing=entry_timing):
                score -= D.ENTRY_MODERATE.deduction
                notes.append(f"✅ Entry signal: {candlestick_pattern} (Nison: Moderate)")
            elif D.ENTRY_WEAK.check(entry_timing=entry_timing):
                score -= D.ENTRY_WEAK.deduction
                notes.append(f"⚠️ {D.ENTRY_WEAK.note_fail}: {candlestick_pattern}")
        else:
            score -= D.ENTRY_MISSING_SHORT_TERM.deduction
            notes.append(f"⚠️ {D.ENTRY_MISSING_SHORT_TERM.note_fail}")
    else:
        # Long-term: bonus only for strong entry WITH pattern direction
        # matching trade direction.  Nison: a Bullish Engulfing confirms
        # a bullish trade but CONTRADICTS a bearish one (and vice versa).
        if candlestick_pattern is not None and entry_timing == 'Strong':
            _pat_lower = str(candlestick_pattern).lower()
            _pat_bullish = 'bullish' in _pat_lower or 'hammer' in _pat_lower or 'morning' in _pat_lower
            _pat_bearish = 'bearish' in _pat_lower or 'shooting' in _pat_lower or 'evening' in _pat_lower
            _trade_bullish = strategy in BULLISH_STRATEGIES
            _trade_bearish = strategy in BEARISH_STRATEGIES
            if (_pat_bullish and _trade_bullish) or (_pat_bearish and _trade_bearish):
                score += 5
                notes.append(f"✅ Entry confirmed: {candlestick_pattern} (Nison: bonus)")
            elif (_pat_bullish and _trade_bearish) or (_pat_bearish and _trade_bullish):
                score -= 5
                notes.append(f"⚠️ Reversal signal: {candlestick_pattern} contradicts {strategy} direction (Nison: -5)")
            else:
                notes.append(f"Pattern detected: {candlestick_pattern} (direction neutral)")

    # ── Chart signal confirmation (Murphy, RAG-backed) ────────
    rsi = safe_float(row, 'RSI')
    macd_hist = safe_float(row, 'MACD_Histogram')
    slowk = safe_float(row, 'SlowK_5_3')
    bb_pos = safe_float(row, 'BB_Position')
    days_cross = safe_float(row, 'Days_Since_Cross')
    chart_regime = safe_get(row, 'Chart_Regime')

    # RSI — Murphy (0.678): "70 = overbought, 30 = oversold.
    # First move into extreme in new trend = WARNING only, not action."
    if rsi is not None:
        if is_bullish and rsi > 75:
            score -= 5
            notes.append(f"RSI {rsi:.0f} overbought — entry may be late (Murphy Ch.10)")
        elif is_bearish and rsi < 25:
            score -= 5
            notes.append(f"RSI {rsi:.0f} oversold — entry may be late (Murphy Ch.10)")
        elif (is_bullish and 40 < rsi < 60) or (is_bearish and 40 < rsi < 60):
            notes.append(f"RSI {rsi:.0f} — neutral zone")
        elif (is_bullish and rsi > 50) or (is_bearish and rsi < 50):
            score += 3
            notes.append(f"✅ RSI {rsi:.0f} confirms direction (Murphy)")

    # MACD histogram — Murphy (0.786): "Histogram divergence = most reliable signal"
    if macd_hist is not None:
        if (is_bullish and macd_hist > 0) or (is_bearish and macd_hist < 0):
            score += 5
            notes.append(f"✅ MACD histogram {'positive' if macd_hist > 0 else 'negative'} ({macd_hist:.3f}) — confirms direction (Murphy Ch.10)")
        elif (is_bullish and macd_hist < 0) or (is_bearish and macd_hist > 0):
            score -= 8
            notes.append(f"MACD histogram {'negative' if macd_hist < 0 else 'positive'} ({macd_hist:.3f}) — contradicts direction (Murphy Ch.10)")

    # Stochastic — Murphy (0.729): "80/20 zones. In strong trends, overbought can persist."
    if slowk is not None:
        if is_bullish and slowk > 85:
            score -= 3
            notes.append(f"Stochastic %K={slowk:.0f} extreme overbought (Murphy: caution)")
        elif is_bearish and slowk < 15:
            score -= 3
            notes.append(f"Stochastic %K={slowk:.0f} extreme oversold (Murphy: caution)")

    # Bollinger Band position — Murphy (0.669): "Touch outer band = overextended"
    if bb_pos is not None:
        if is_bullish and bb_pos > 95:
            score -= 5
            notes.append(f"BB_Position {bb_pos:.0f}% — above upper band, overextended (Murphy)")
        elif is_bearish and bb_pos < 5:
            score -= 5
            notes.append(f"BB_Position {bb_pos:.0f}% — below lower band, overextended (Murphy)")

    # EMA crossover freshness — Murphy (0.685): "crossover = buy/sell signal"
    if days_cross is not None and not pd.isna(days_cross):
        if days_cross <= 5:
            score += 3
            notes.append(f"✅ Fresh EMA crossover ({days_cross:.0f}d ago — Murphy)")
        elif days_cross > 30:
            notes.append(f"Stale EMA crossover ({days_cross:.0f}d ago)")

    # Chart Regime confirmation — directional needs trend, not range
    if chart_regime is not None:
        _regime_lower = str(chart_regime).lower()
        if _regime_lower in ('strong_trend', 'trending'):
            score += 5
            notes.append(f"✅ Regime {chart_regime} — favorable for directional (Murphy)")
        elif _regime_lower == 'ranging':
            score -= 8
            notes.append(f"Regime {chart_regime} — unfavorable for directional (Murphy: use oscillators)")
        elif _regime_lower == 'compressed':
            score -= 5
            notes.append(f"Regime {chart_regime} — low volatility, directional may underperform")
        elif _regime_lower == 'emerging_trend':
            score += 2
            notes.append(f"Regime {chart_regime} — emerging trend (Murphy: early signal)")

    # ── Institutional-grade signals (Murphy, Bulkowski, Raschke) ──

    # Market Structure — HH/HL confirmation (Murphy Ch.4)
    market_structure = safe_get(row, 'Market_Structure')
    if market_structure is not None and market_structure != 'Unknown':
        _ms_lower = str(market_structure).lower()
        if (is_bullish and _ms_lower == 'uptrend') or (is_bearish and _ms_lower == 'downtrend'):
            score += 5
            notes.append(f"✅ Market structure {market_structure} confirms direction (Murphy Ch.4: HH/HL)")
        elif (is_bullish and _ms_lower == 'downtrend') or (is_bearish and _ms_lower == 'uptrend'):
            score -= 8
            notes.append(f"Market structure {market_structure} contradicts direction (Murphy Ch.4)")
        elif _ms_lower == 'consolidation':
            notes.append(f"Market structure Consolidation — no swing-point confirmation")

    # OBV slope — accumulation/distribution (Murphy Ch.7)
    obv_slope = safe_float(row, 'OBV_Slope')
    if obv_slope is not None:
        if is_bullish and obv_slope > 5:
            score += 3
            notes.append(f"✅ OBV accumulating ({obv_slope:+.0f}% — Murphy Ch.7)")
        elif is_bullish and obv_slope < -10:
            score -= 5
            notes.append(f"OBV distributing ({obv_slope:+.0f}%) contradicts bullish (Murphy Ch.7)")
        elif is_bearish and obv_slope < -5:
            score += 3
            notes.append(f"✅ OBV distributing ({obv_slope:+.0f}% — Murphy Ch.7)")
        elif is_bearish and obv_slope > 10:
            score -= 5
            notes.append(f"OBV accumulating ({obv_slope:+.0f}%) contradicts bearish (Murphy Ch.7)")

    # Breakout volume — Bulkowski (0.712)
    volume_ratio = safe_float(row, 'Volume_Ratio')
    if volume_ratio is not None and volume_ratio > 1.5:
        _regime_l = str(chart_regime).lower() if chart_regime else ''
        if _regime_l in ('compressed', 'emerging_trend'):
            score += 5
            notes.append(f"✅ Breakout volume {volume_ratio:.1f}x avg in {chart_regime} (Bulkowski)")
        else:
            score += 2
            notes.append(f"High volume {volume_ratio:.1f}x avg (Bulkowski: confirms move)")

    # RSI Divergence — Murphy (0.691): "serious warning"
    rsi_div = safe_get(row, 'RSI_Divergence')
    if rsi_div is not None and rsi_div != 'None':
        if (is_bullish and rsi_div == 'Bearish_Divergence') or (is_bearish and rsi_div == 'Bullish_Divergence'):
            score -= 8
            notes.append(f"{rsi_div} — contradicts direction (Murphy: most reliable RSI signal)")
        elif (is_bullish and rsi_div == 'Bullish_Divergence') or (is_bearish and rsi_div == 'Bearish_Divergence'):
            score += 5
            notes.append(f"✅ {rsi_div} confirms direction (Murphy)")

    # MACD Divergence — Murphy (0.786)
    macd_div = safe_get(row, 'MACD_Divergence')
    if macd_div is not None and macd_div != 'None':
        if (is_bullish and macd_div == 'Bearish_Divergence') or (is_bearish and macd_div == 'Bullish_Divergence'):
            score -= 8
            notes.append(f"{macd_div} — MACD contradicts direction (Murphy Ch.10)")
        elif (is_bullish and macd_div == 'Bullish_Divergence') or (is_bearish and macd_div == 'Bearish_Divergence'):
            score += 5
            notes.append(f"✅ {macd_div} — MACD confirms direction (Murphy Ch.10)")

    # Weekly Trend Bias — Murphy: "weekly filters for daily"
    weekly_bias = safe_get(row, 'Weekly_Trend_Bias')
    if weekly_bias == 'ALIGNED':
        score += 5
        notes.append(f"✅ Weekly trend ALIGNED with daily (Murphy: higher TF confirmation)")
    elif weekly_bias == 'CONFLICTING':
        score -= 8
        notes.append(f"Weekly trend CONFLICTING with daily (Murphy: trading against weekly)")

    # Keltner Squeeze — Raschke / Murphy (0.739)
    squeeze_on = row.get('Keltner_Squeeze_On', False)
    squeeze_fired = row.get('Keltner_Squeeze_Fired', False)
    if squeeze_fired:
        if macd_hist is not None:
            _fire_bullish = macd_hist > 0
            if (is_bullish and _fire_bullish) or (is_bearish and not _fire_bullish):
                score += 5
                notes.append(f"✅ Keltner squeeze FIRED in direction (Raschke)")
            else:
                score -= 5
                notes.append(f"Keltner squeeze fired AGAINST direction (Raschke)")
        else:
            score += 3
            notes.append(f"Keltner squeeze fired (direction unconfirmed)")
    elif squeeze_on:
        notes.append(f"Keltner squeeze active — breakout pending (Raschke)")

    # Relative Strength vs SPY — Murphy (0.740): intermarket analysis
    rs_spy = safe_float(row, 'RS_vs_SPY_20d')
    if rs_spy is not None:
        if is_bullish and rs_spy > 3:
            score += 3
            notes.append(f"✅ Outperforming SPY by {rs_spy:+.1f}% (Murphy: relative strength)")
        elif is_bullish and rs_spy < -5:
            score -= 3
            notes.append(f"Underperforming SPY by {rs_spy:+.1f}% (Murphy: relative weakness)")
        elif is_bearish and rs_spy < -3:
            score += 3
            notes.append(f"✅ Underperforming SPY by {rs_spy:+.1f}% (Murphy: confirms bearish)")
        elif is_bearish and rs_spy > 5:
            score -= 3
            notes.append(f"Outperforming SPY by {rs_spy:+.1f}% (Murphy: contradicts bearish)")

    # ── LEAP fallback penalty ─────────────────────────────────
    if is_leap_fallback:
        requested_dte = safe_float(row, 'Min_DTE', default=365)
        penalty = D.LEAP_FALLBACK_HALF_DTE if (actual_dte is not None and actual_dte < requested_dte * 0.5) else D.LEAP_FALLBACK_BASE
        score -= penalty
        notes.append(f"⚠️ LEAP_FALLBACK: Requested {requested_dte:.0f}+ DTE, using {actual_dte:.0f} DTE (−{penalty})")

    # ── Final status ──────────────────────────────────────────
    if score >= 70:
        status = 'Valid'
        notes.append(f"✅ Meets directional requirements (Delta={abs_delta:.2f}, Gamma={gamma:.3f})")
    elif score >= 50:
        status = 'Watch'
        notes.append(f"⚠️ Marginal directional setup (consider stronger conviction)")
    else:
        status = 'Reject'
        notes.append(f"❌ Fails directional requirements")

    return EvaluationResult(status, data_completeness, '', score, ' | '.join(notes))
