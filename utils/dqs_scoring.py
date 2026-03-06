"""
DQS — Directional Quality Score

Scores directional options trades (Long Call, Long Put, LEAP variants) on a
0-100 scale, analogous to PCS for income strategies.

Five components (additive, starting from 0):
  1. Delta fit          25 pts  — right strike for defined-risk directional
  2. IV entry timing    20 pts  — cheap vs expensive relative to realised vol
  3. Spread cost        20 pts  — slippage as % of premium paid
  4. DTE fit            10 pts  — not too short (gamma risk), not too long (slow)
  5. Trend confirmation 25 pts  — RSI, EMA crossover, MACD alignment

Total: 100 pts

Weights rationale (RAG: EXECUTION_READINESS_GAP_ANALYSIS.md):
  Entry timing (trend) = 25% — most important, prevents chasing
  IV timing            = 20% — second, but high IV ≠ always fatal for momentum
  Strike/delta         = 25% — conviction of directional exposure
  Execution/spread     = 20% — friction kills convexity edge
  DTE                  = 10% — directional DTE less critical than income theta

Status thresholds:
  Strong   >= 75  (all systems go)
  Eligible >= 50  (tradeable with awareness)
  Weak     <  50  (avoid or size very small)

RAG references:
  Delta range    — STRATEGY_QUALITY_AUDIT.md:72 (Passarelli)
  IV timing      — EXECUTION_READINESS_GAP_ANALYSIS.md:289 (Natenberg Ch.4)
  Entry timing   — EXECUTION_READINESS_GAP_ANALYSIS.md:370 (weight = 25%)
  DTE minimum    — STRATEGY_QUALITY_AUDIT.md:230 (14d hard floor)
  Max spread     — STRATEGY_QUALITY_AUDIT.md:191 (10% directional max)
"""

import pandas as pd
import numpy as np
from typing import Tuple, List


# ---------------------------------------------------------------------------
# Component 1 — Delta fit  (25 pts max)
# ---------------------------------------------------------------------------
# RAG (STRATEGY_QUALITY_AUDIT.md:72-74, Passarelli):
#   Long Call / Long Put : 0.30 – 0.70 (balanced directional)
#   Long Call LEAP       : 0.60 – 0.90 (ITM favoured — synthetic-like)
#   Long Put  LEAP       : 0.60 – 0.90 (abs value)
#
# User calibration:
#   Don't over-penalise 0.60 calls in strong trend.
#   Breakdown trades often use 0.45–0.65.
#   → sweet spot upper extended to 0.65; cap at 0.85 before heavy penalty.

def _score_delta(row: pd.Series) -> Tuple[float, str]:
    delta    = row.get('Delta')
    strategy = str(row.get('Strategy_Name', row.get('Strategy', ''))).lower()
    is_leap  = 'leap' in strategy

    try:
        abs_d = abs(float(delta))
    except (TypeError, ValueError):
        return 0.0, "Delta missing (0/25)"

    if is_leap:
        # RAG: LEAP delta 0.60-0.90 (ITM, stock-surrogate behaviour)
        if 0.60 <= abs_d <= 0.90:
            return 25.0, f"Delta {abs_d:.2f} — LEAP ITM sweet spot (25/25)"
        elif 0.50 <= abs_d < 0.60:
            return 18.0, f"Delta {abs_d:.2f} — slightly below LEAP target (18/25)"
        elif abs_d > 0.90:
            return 10.0, f"Delta {abs_d:.2f} — deep ITM LEAP, low convexity (10/25)"
        elif abs_d >= 0.40:
            return 10.0, f"Delta {abs_d:.2f} — OTM for LEAP, acceptable (10/25)"
        else:
            return 2.0,  f"Delta {abs_d:.2f} — too OTM for LEAP (2/25)"
    else:
        # Standard directional: sweet spot 0.30–0.65
        # 0.65–0.75 = still fine (strong trend continuation)
        # >0.75 = deep ITM, poor leverage for premium paid
        if 0.30 <= abs_d <= 0.65:
            return 25.0, f"Delta {abs_d:.2f} — sweet spot (25/25)"
        elif 0.65 < abs_d <= 0.75:
            # Mild fade — still useful in strong trend
            pts = 25.0 - (abs_d - 0.65) / 0.10 * 8   # 25 → 17 linearly
            return round(pts, 1), f"Delta {abs_d:.2f} — high but valid in strong trend ({pts:.0f}/25)"
        elif 0.75 < abs_d <= 0.85:
            pts = 17.0 - (abs_d - 0.75) / 0.10 * 12  # 17 → 5
            return round(max(5.0, pts), 1), f"Delta {abs_d:.2f} — deep ITM, low leverage ({max(5, int(pts))}/25)"
        elif abs_d > 0.85:
            return 3.0,  f"Delta {abs_d:.2f} — deep ITM, synthetic stock risk (3/25)"
        elif 0.20 <= abs_d < 0.30:
            # Linear fade from 0 at 0.20 to 25 at 0.30
            pts = (abs_d - 0.20) / 0.10 * 25
            return round(pts, 1), f"Delta {abs_d:.2f} — low conviction OTM ({pts:.0f}/25)"
        else:
            return 0.0,  f"Delta {abs_d:.2f} — deep OTM lottery ticket (0/25)"


# ---------------------------------------------------------------------------
# Component 2 — IV entry timing  (20 pts max)
# ---------------------------------------------------------------------------
# RAG (EXECUTION_READINESS_GAP_ANALYSIS.md:289, Natenberg Ch.4):
#   Buying long options: prefer IV Rank < 30% (cheap premium)
#   IV Rank > 70%: expensive — penalise
#
# User calibration:
#   IV > HV is NOT always fatal for directional if trend is accelerating.
#   Floor the penalty: IV Rank 70-100 should get 4 pts minimum, not 2.
#   Weight reduced from 25 → 20 pts (trend is more important for directional).
#
# Primary signal: IVHV_gap_30D (negative = cheap, positive = rich)
# Fallback: IV_Rank (20D / 30D / 252D)

def _score_iv_timing(row: pd.Series) -> Tuple[float, str]:
    gap      = row.get('IVHV_gap_30D')
    iv_rank  = (row.get('IV_Rank_20D')
                or row.get('IV_Rank_30D')
                or row.get('IV_Rank_252D'))

    try:
        gap_f = float(gap)
    except (TypeError, ValueError):
        # Fallback to IV Rank
        try:
            rank_f = float(iv_rank)
            if rank_f <= 25:
                return 20.0, f"IV Rank {rank_f:.0f} — deeply cheap entry (20/20)"
            elif rank_f <= 40:
                return 16.0, f"IV Rank {rank_f:.0f} — cheap entry (16/20)"
            elif rank_f <= 60:
                return 12.0, f"IV Rank {rank_f:.0f} — fair value (12/20)"
            elif rank_f <= 75:
                return 7.0,  f"IV Rank {rank_f:.0f} — slightly rich (7/20)"
            else:
                return 4.0,  f"IV Rank {rank_f:.0f} — rich, expect smaller move (4/20)"
        except (TypeError, ValueError):
            return 10.0, "No IV timing data — neutral (10/20)"

    # Gap-based: negative = IV < HV = cheap for buyers
    if gap_f <= -15:
        return 20.0, f"IV gap {gap_f:+.1f}% — deeply cheap entry (20/20)"
    elif gap_f <= -5:
        return 16.0, f"IV gap {gap_f:+.1f}% — cheap entry (16/20)"
    elif gap_f <= 5:
        return 12.0, f"IV gap {gap_f:+.1f}% — fair value (12/20)"
    elif gap_f <= 15:
        return 7.0,  f"IV gap {gap_f:+.1f}% — rich, overpaying slightly (7/20)"
    else:
        return 4.0,  f"IV gap {gap_f:+.1f}% — rich premium; strong trend required (4/20)"


# ---------------------------------------------------------------------------
# Component 3 — Spread cost as % of premium  (20 pts max)
# ---------------------------------------------------------------------------
# RAG (STRATEGY_QUALITY_AUDIT.md:191): directional max spread = 10%
# Natenberg: <5% institutional, 5-10% retail normal, >10% erodes edge for buyers
# Hard zero at >10% per audit (unlike income strategies which tolerate 12%)

def _score_spread(row: pd.Series) -> Tuple[float, str]:
    bid = row.get('Bid')
    ask = row.get('Ask')
    mid = row.get('Mid_Price') or row.get('Mid')

    try:
        bid_f = float(bid)
        ask_f = float(ask)
        mid_f = float(mid)
        if mid_f <= 0:
            raise ValueError
        spread_pct = (ask_f - bid_f) / mid_f * 100
    except (TypeError, ValueError):
        try:
            spread_pct = float(row.get('Bid_Ask_Spread_Pct'))
        except (TypeError, ValueError):
            return 10.0, "No spread data — neutral (10/20)"

    if spread_pct <= 2.0:
        return 20.0, f"Spread {spread_pct:.1f}% — institutional tight (20/20)"
    elif spread_pct <= 4.0:
        return 17.0, f"Spread {spread_pct:.1f}% — good (17/20)"
    elif spread_pct <= 7.0:
        return 13.0, f"Spread {spread_pct:.1f}% — acceptable (13/20)"
    elif spread_pct <= 10.0:
        return 7.0,  f"Spread {spread_pct:.1f}% — wide, use limit only (7/20)"
    else:
        # >10% = RAG hard limit for directional
        return 0.0,  f"Spread {spread_pct:.1f}% — exceeds directional max (0/20)"


# ---------------------------------------------------------------------------
# Component 4 — DTE fit  (10 pts max)
# ---------------------------------------------------------------------------
# RAG (STRATEGY_QUALITY_AUDIT.md:230): hard floor = 14 days
# RAG (EXECUTION_READINESS_GAP_ANALYSIS.md:309): theta accelerates <30 days (Hull)
# User calibration: 20-30d valid for high-beta gamma expansion (partial credit)
# Sweet spot: 30-60d
# 60-90d: still good
# <14d: zero (hard floor per RAG)
# >180d: LEAP territory — handled separately

def _score_dte(row: pd.Series) -> Tuple[float, str]:
    dte      = row.get('Actual_DTE') or row.get('DTE')
    strategy = str(row.get('Strategy_Name', row.get('Strategy', ''))).lower()
    is_leap  = 'leap' in strategy

    try:
        dte_f = float(dte)
    except (TypeError, ValueError):
        return 5.0, "DTE unknown — neutral (5/10)"

    if is_leap:
        if 180 <= dte_f <= 540:
            return 10.0, f"DTE {dte_f:.0f}d — ideal LEAP window (10/10)"
        elif 90 <= dte_f < 180:
            return 7.0,  f"DTE {dte_f:.0f}d — short for LEAP, usable (7/10)"
        elif dte_f > 540:
            return 6.0,  f"DTE {dte_f:.0f}d — very long, high vega drag (6/10)"
        else:
            return 3.0,  f"DTE {dte_f:.0f}d — too short for LEAP thesis (3/10)"
    else:
        if dte_f < 14:
            # RAG hard floor — gamma danger, theta burn
            return 0.0,  f"DTE {dte_f:.0f}d — below 14d hard floor (0/10)"
        elif 14 <= dte_f < 20:
            return 3.0,  f"DTE {dte_f:.0f}d — very short, high gamma risk (3/10)"
        elif 20 <= dte_f < 30:
            # User: partial credit for high-beta gamma expansion plays
            return 7.0,  f"DTE {dte_f:.0f}d — short window, gamma-expansion potential (7/10)"
        elif 30 <= dte_f <= 60:
            return 10.0, f"DTE {dte_f:.0f}d — sweet spot (10/10)"
        elif 60 < dte_f <= 90:
            return 9.0,  f"DTE {dte_f:.0f}d — slightly long, fine (9/10)"
        elif 90 < dte_f <= 180:
            return 6.0,  f"DTE {dte_f:.0f}d — long, slower convexity (6/10)"
        else:
            return 4.0,  f"DTE {dte_f:.0f}d — use LEAP strategy instead (4/10)"


# ---------------------------------------------------------------------------
# Component 5 — Trend confirmation  (25 pts max)
# ---------------------------------------------------------------------------
# RAG (EXECUTION_READINESS_GAP_ANALYSIS.md:370): entry timing = 25% weight
# Sub-signal weights: EMA > MACD > RSI (EMA crossover is the strongest directional signal)
#   EMA crossover  : 10 pts  (trend regime confirmation)
#   MACD           :  8 pts  (momentum direction)
#   RSI            :  7 pts  (overbought/oversold context)
#
# Thresholds: avoid chasing (RSI > 70 for calls = caution, not full block)

def _score_trend(row: pd.Series) -> Tuple[float, str]:
    strategy   = str(row.get('Strategy_Name', row.get('Strategy', ''))).lower()
    is_bearish = 'put' in strategy

    pts   = 0.0
    hits  = []
    misses = []

    # ── EMA crossover (10 pts) ────────────────────────────────────────────────
    # Column is 'Chart_EMA_Signal' in pipeline output; 'EMA_Signal' / 'EMA_Crossover' as fallbacks
    ema_signal = str(row.get('Chart_EMA_Signal', row.get('EMA_Signal', row.get('EMA_Crossover', '')))).lower()
    if is_bearish:
        if 'bearish' in ema_signal:
            pts += 10.0; hits.append("EMA bearish ✓")
        elif 'bullish' in ema_signal:
            misses.append("EMA bullish (against put)")
        else:
            pts += 4.0   # neutral credit — trend hasn't crossed yet
    else:
        if 'bullish' in ema_signal:
            pts += 10.0; hits.append("EMA bullish ✓")
        elif 'bearish' in ema_signal:
            misses.append("EMA bearish (against call)")
        else:
            pts += 4.0   # neutral credit

    # ── MACD (8 pts) ──────────────────────────────────────────────────────────
    try:
        macd = float(row.get('MACD', 0) or 0)
        if is_bearish:
            if macd < -1.0:
                pts += 8.0; hits.append(f"MACD {macd:.2f} strongly bearish")
            elif macd < 0:
                pts += 5.0; hits.append(f"MACD {macd:.2f} bearish")
            elif macd <= 0.5:
                pts += 2.0; misses.append(f"MACD {macd:.2f} slightly positive")
            else:
                misses.append(f"MACD {macd:.2f} bullish (against put)")
        else:
            if macd > 1.0:
                pts += 8.0; hits.append(f"MACD {macd:.2f} strongly bullish")
            elif macd > 0:
                pts += 5.0; hits.append(f"MACD {macd:.2f} bullish")
            elif macd >= -0.5:
                pts += 2.0; misses.append(f"MACD {macd:.2f} slightly negative")
            else:
                misses.append(f"MACD {macd:.2f} bearish (against call)")
    except (TypeError, ValueError):
        pts += 4.0  # neutral credit

    # ── MACD histogram divergence modifier  (-5 pts max, Murphy Ch.6) ─────────
    # GUARDRAIL 3: Proper price divergence check — Murphy Ch.6: divergence requires
    # comparing PRICE direction to HISTOGRAM direction, not histogram vs histogram.
    # "Price makes a new high while histogram makes a lower high = bearish divergence."
    # Previous implementation used hist_cur < hist_prev (slope-only) which fires on any
    # pullback — including healthy consolidation — and produces false penalties.
    #
    # Correct approach:
    #   Bearish div (call setup): price above EMA20 (new-high proxy) AND histogram < 5-bar-ago
    #   Bullish div (put setup): price below EMA20 (new-low proxy) AND histogram > 5-bar-ago
    #
    # If lookback columns (MACD_Histogram_Prev5, EMA_20, Close) unavailable: SKIP — no penalty.
    # This is safer than firing a false signal on incomplete data.
    try:
        macd_hist      = row.get('MACD_Histogram')
        macd_hist_p5   = row.get('MACD_Histogram_Prev5') or row.get('MACD_Histogram_Prev')
        close_val      = row.get('Close') or row.get('Last_Price')
        ema20_val      = row.get('EMA_20') or row.get('EMA20')
        macd_div_detected = False
        if (macd_hist is not None and macd_hist_p5 is not None
                and close_val is not None and ema20_val is not None):
            hist_cur   = float(macd_hist)
            hist_lookback = float(macd_hist_p5)
            close_f    = float(close_val)
            ema20_f    = float(ema20_val)
            # Bearish divergence: price above EMA20 (upward momentum) but histogram LOWER than 5 bars ago
            price_high_proxy = close_f > ema20_f
            hist_lower_than_prior = hist_cur < hist_lookback
            # Bullish divergence: price below EMA20 (downward pressure) but histogram HIGHER than 5 bars ago
            price_low_proxy = close_f < ema20_f
            hist_higher_than_prior = hist_cur > hist_lookback
            if not is_bearish and price_high_proxy and hist_lower_than_prior:
                pts = max(0.0, pts - 5.0)
                misses.append("MACD bearish div: price above EMA20 but hist fading -5pts")
                macd_div_detected = True
            elif is_bearish and price_low_proxy and hist_higher_than_prior:
                pts = max(0.0, pts - 5.0)
                misses.append("MACD bullish div: price below EMA20 but hist rising -5pts")
                macd_div_detected = True
        # No penalty if lookback data unavailable — safe default (no false signals)
    except (TypeError, ValueError):
        macd_div_detected = False  # histogram data unavailable — no modifier applied

    # ── RSI (7 pts) ───────────────────────────────────────────────────────────
    try:
        rsi = float(row.get('RSI', 50) or 50)
        if is_bearish:
            if rsi < 40:
                pts += 7.0; hits.append(f"RSI {rsi:.0f} oversold — put timing good")
            elif rsi < 50:
                pts += 5.0; hits.append(f"RSI {rsi:.0f} weakening")
            elif rsi < 60:
                pts += 2.0; misses.append(f"RSI {rsi:.0f} mid-range")
            elif rsi < 70:
                pts += 0.0; misses.append(f"RSI {rsi:.0f} high for put entry")
            else:
                # Overbought → put may be well-timed (contrarian case)
                pts += 4.0; hits.append(f"RSI {rsi:.0f} overbought → put timing opportunity")
        else:
            if rsi > 60:
                pts += 7.0; hits.append(f"RSI {rsi:.0f} bullish momentum")
            elif rsi > 50:
                pts += 5.0; hits.append(f"RSI {rsi:.0f} slightly bullish")
            elif rsi > 40:
                pts += 2.0; misses.append(f"RSI {rsi:.0f} mid-range")
            elif rsi > 30:
                pts += 0.0; misses.append(f"RSI {rsi:.0f} weak for call")
            else:
                # Oversold → call may be well-timed (contrarian/bounce case)
                pts += 4.0; hits.append(f"RSI {rsi:.0f} oversold → call bounce potential")
    except (TypeError, ValueError):
        pts += 3.5  # neutral credit

    all_parts = hits + ([f"Against: {', '.join(misses)}"] if misses else [])
    note = f"Trend: {'; '.join(all_parts) if all_parts else 'neutral'} ({pts:.0f}/25)"
    return pts, note


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def calculate_dqs_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate Directional Quality Score for directional strategy rows.

    Only scores rows where strategy is Long Call / Long Put / LEAP variant.
    All other rows get DQS_Score = NaN, DQS_Status = 'N/A'.

    Adds columns:
        DQS_Score     : 0-100 float
        DQS_Status    : 'Strong' | 'Eligible' | 'Weak' | 'N/A'
        DQS_Reason    : plain-English summary
        DQS_Breakdown : pipe-separated component scores

    Component max pts (100 total):
        Delta       25 pts
        IV timing   20 pts
        Spread      20 pts
        DTE         10 pts
        Trend       25 pts
    """
    _DIRECTIONAL = {'long call', 'long put', 'long call leap', 'long put leap'}

    # Component max pts for normalisation in worst-component logic
    _MAX_PTS = {'delta': 25, 'iv': 20, 'spread': 20, 'dte': 10, 'trend': 25}

    df['DQS_Score']     = np.nan
    df['DQS_Status']    = 'N/A'
    df['DQS_Reason']    = ''
    df['DQS_Breakdown'] = ''

    for idx, row in df.iterrows():
        strat = str(row.get('Strategy_Name', row.get('Strategy', ''))).lower().strip()
        if strat not in _DIRECTIONAL:
            continue

        d_pts,  d_note  = _score_delta(row)
        iv_pts, iv_note = _score_iv_timing(row)
        sp_pts, sp_note = _score_spread(row)
        dt_pts, dt_note = _score_dte(row)
        tr_pts, tr_note = _score_trend(row)

        total = round(d_pts + iv_pts + sp_pts + dt_pts + tr_pts, 1)
        total = min(100.0, total)

        if total >= 75:
            status = 'Strong'
            reason = f"Strong directional setup ({total:.0f}/100)"
        elif total >= 50:
            status = 'Eligible'
            reason = f"Eligible — tradeable with awareness ({total:.0f}/100)"
        else:
            status = 'Weak'
            # Find the worst component by fractional score
            components = [
                (d_pts  / _MAX_PTS['delta'],  d_note),
                (iv_pts / _MAX_PTS['iv'],     iv_note),
                (sp_pts / _MAX_PTS['spread'], sp_note),
                (dt_pts / _MAX_PTS['dte'],    dt_note),
                (tr_pts / _MAX_PTS['trend'],  tr_note),
            ]
            worst = min(components, key=lambda x: x[0])
            reason = f"Weak setup ({total:.0f}/100) — {worst[1]}"

        breakdown = ' | '.join([d_note, iv_note, sp_note, dt_note, tr_note])

        df.at[idx, 'DQS_Score']     = total
        df.at[idx, 'DQS_Status']    = status
        df.at[idx, 'DQS_Reason']    = reason
        df.at[idx, 'DQS_Breakdown'] = breakdown

    return df
