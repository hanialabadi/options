"""
Behavioral Memory — Full Scan History Enrichment
==================================================
Reads **all available history since Jan 1 of the current year** from
``technical_indicators``, ``scan_candidates``, ``iv_term_history``, and
``earnings_stats`` to compute a behavioral arc per ticker.

Instead of treating each scan as a fresh snapshot, this module tells
the scanner *how the stock has been behaving* across its full recorded
history — regime transitions, trend persistence, volume conviction,
volatility arc, and earnings track record.

Output columns:
  Regime_Duration       — days in current Chart_Regime (stability)
  Regime_Path           — last 3+ regime transitions ("Compression→Emerging→Trending")
  ADX_Trend             — BUILDING / PEAKING / FADING / FLAT
  RSI_Range             — tight (<15) = trending, wide (>30) = choppy
  Volume_Accumulation   — ACCUMULATING / DISTRIBUTING / NEUTRAL
  Scan_Frequency        — times READY in history (0 = first time)
  DQS_Trend             — CLIMBING / DECLINING / STABLE / V_RECOVERY
  Signal_Age            — consecutive scans with same regime
  IV_Arc                — RISING / FALLING / STABLE / SPIKING
  Earnings_Context      — RELIABLE_BEATER / UNRELIABLE / NO_DATA
  History_Depth         — number of daily data points available
  Behavioral_Score      — composite 0-100 summarizing behavioral quality

Graceful degradation: any DB failure → neutral defaults (no enrichment).
Only queries tickers passed in — not the entire universe.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from core.shared.data_layer.duckdb_utils import get_domain_connection, DbDomain

logger = logging.getLogger(__name__)


def _ytd_start() -> str:
    """Returns Jan 1 of the current year as 'YYYY-01-01'."""
    return f"{datetime.now().year}-01-01"


# ── Output column defaults ───────────────────────────────────────────────────

def _neutral_result() -> dict:
    """Default when history is insufficient."""
    return {
        'Regime_Duration': 0,
        'Regime_Path': '',
        'ADX_Trend': 'UNKNOWN',
        'RSI_Range': 0.0,
        'Volume_Accumulation': 'UNKNOWN',
        'Scan_Frequency': 0,
        'DQS_Trend': 'UNKNOWN',
        'Signal_Age': 0,
        'IV_Arc': 'UNKNOWN',
        'Earnings_Context': 'NO_DATA',
        'Mgmt_Track_Record': 'NO_DATA',
        'Prior_Trades': 0,
        'Mgmt_Confidence': 'NONE',
        'Mgmt_Strategy_Detail': '',
        'Mgmt_Recency_Factor': 1.0,
        'Fault_Pattern': 'INSUFFICIENT_DATA',
        'Contradiction_Flags': '',
        'Move_Drivers': '',
        'Last_Dip_Context': '',
        'Event_Reactions': '',
        'Worst_Event_Type': '',
        'Data_Maturity': 'NEW_TICKER',
        'History_Depth': 0,
        'Behavioral_Score': 50,
    }


# ── Main entry point ─────────────────────────────────────────────────────────

def compute_behavioral_memory(
    tickers: List[str],
    con=None,
) -> Dict[str, dict]:
    """
    Compute YTD behavioral arc for each ticker.

    Queries ``technical_indicators``, ``scan_candidates``,
    ``iv_term_history``, and ``earnings_stats`` from DuckDB.
    Returns dict keyed by ticker → behavioral dict.
    """
    if not tickers:
        return {}

    close_con = False
    iv_close_con = False
    iv_con = None
    if con is None:
        try:
            con = get_domain_connection(DbDomain.MANAGEMENT, read_only=True)
            close_con = True
        except Exception as e:
            logger.debug(f"[BehavioralMemory] DB connection failed: {e}")
            return {t: _neutral_result() for t in tickers}

    # IV history lives in a separate database
    try:
        iv_con = get_domain_connection(DbDomain.IV_HISTORY, read_only=True)
        iv_close_con = True
    except Exception:
        pass

    result: Dict[str, dict] = {}
    try:
        tech_data = _query_tech_history(con, tickers)
        scan_data = _query_scan_history(con, tickers)
        iv_data = _query_iv_history(iv_con, tickers) if iv_con else {}
        earnings_data = _query_earnings_stats(con, tickers)
        mgmt_data = _query_management_history(con, tickers)

        for ticker in tickers:
            tech = tech_data.get(ticker)
            scans = scan_data.get(ticker)
            iv = iv_data.get(ticker)
            earn = earnings_data.get(ticker)
            mgmt = mgmt_data.get(ticker)
            if tech is None or len(tech) < 3:
                result[ticker] = _neutral_result()
                continue
            result[ticker] = _compute_one(ticker, tech, scans or [], iv or [], earn or {}, mgmt or {})

    except Exception as e:
        logger.debug(f"[BehavioralMemory] Computation failed: {e}")
        result = {t: _neutral_result() for t in tickers}
    finally:
        if close_con and con is not None:
            try:
                con.close()
            except Exception:
                pass
        if iv_close_con and iv_con is not None:
            try:
                iv_con.close()
            except Exception:
                pass

    # Fill missing
    for t in tickers:
        if t not in result:
            result[t] = _neutral_result()

    return result


# ── Per-ticker computation ────────────────────────────────────────────────────

def _compute_one(
    ticker: str,
    tech_rows: List[dict],
    scan_rows: List[dict],
    iv_rows: List[dict],
    earnings: dict,
    mgmt: dict,
) -> dict:
    """Compute behavioral metrics from full history."""

    history_depth = len(tech_rows)

    # ── Regime Duration & Path ────────────────────────────────────────
    # Filter out NULLs/empty — older rows may lack Chart_Regime
    regimes = [r.get('Chart_Regime', '') for r in tech_rows
               if r.get('Chart_Regime') and str(r.get('Chart_Regime')) not in ('None', '', 'nan')]
    regime_duration = 0
    if regimes:
        current = regimes[-1]
        regime_duration = 1
        for i in range(len(regimes) - 2, -1, -1):
            if regimes[i] == current:
                regime_duration += 1
            else:
                break

    # If Chart_Regime sparse, supplement with Market_Structure
    if len(regimes) < 3:
        structures = [r.get('Market_Structure', '') for r in tech_rows
                      if r.get('Market_Structure') and str(r.get('Market_Structure')) not in ('None', '', 'nan')]
        if structures:
            regimes = structures
            if structures:
                current = structures[-1]
                regime_duration = 1
                for i in range(len(structures) - 2, -1, -1):
                    if structures[i] == current:
                        regime_duration += 1
                    else:
                        break

    # Regime path: deduplicate consecutive same-regimes, keep last 5 transitions
    regime_path = _dedupe_sequence(regimes)[-5:]
    regime_path_str = '\u2192'.join(regime_path) if regime_path else ''

    # ── ADX Trend ─────────────────────────────────────────────────────
    adx_vals = [float(r['ADX_14']) for r in tech_rows
                if r.get('ADX_14') is not None and not _is_nan(r['ADX_14'])]
    adx_trend = _classify_indicator_trend(adx_vals)

    # ── RSI Range ─────────────────────────────────────────────────────
    rsi_vals = [float(r['RSI_14']) for r in tech_rows
                if r.get('RSI_14') is not None and not _is_nan(r['RSI_14'])]
    rsi_range = 0.0
    if len(rsi_vals) >= 3:
        rsi_range = round(max(rsi_vals) - min(rsi_vals), 1)

    # ── Volume Accumulation ───────────────────────────────────────────
    obv_slopes = [float(r['OBV_Slope']) for r in tech_rows
                  if r.get('OBV_Slope') is not None and not _is_nan(r['OBV_Slope'])]
    vol_accum = _classify_volume(obv_slopes)

    # ── Scan Frequency ────────────────────────────────────────────────
    ready_count = sum(1 for s in scan_rows
                      if str(s.get('Execution_Status', '')).upper() == 'READY')

    # ── DQS Trend ─────────────────────────────────────────────────────
    dqs_scores = [float(s['DQS_Score']) for s in scan_rows
                  if s.get('DQS_Score') is not None and not _is_nan(s['DQS_Score'])]
    dqs_trend = _classify_score_trend(dqs_scores)

    # ── Signal Age (consecutive days in current regime) ───────────────
    signal_age = regime_duration

    # ── IV Arc (from iv_term_history) ─────────────────────────────────
    iv_arc = _classify_iv_arc(iv_rows)

    # ── Earnings Context (from earnings_stats) ────────────────────────
    earnings_ctx = _classify_earnings(earnings)

    # ── Management Track Record (from management_recommendations) ────
    mgmt_track, prior_trades, mgmt_confidence, recency_factor = _classify_mgmt_track(mgmt)

    # ── Strategy Breakdown (per-strategy win/loss) ─────────────────
    strat_detail = _classify_strategy_breakdown(mgmt if isinstance(mgmt, list) else [])

    # ── Fault Pattern (ticker vs strategy vs timing vs structure) ──
    fault_pattern = _classify_fault_pattern(mgmt if isinstance(mgmt, list) else [])

    # ── Contradiction Flags ────────────────────────────────────────
    contradictions = _detect_contradictions(
        dqs_trend=dqs_trend,
        adx_trend=adx_trend,
        vol_accum=vol_accum,
        iv_arc=iv_arc,
        earnings_ctx=earnings_ctx,
        mgmt_track=mgmt_track,
        regime_path=regime_path,
    )

    # ── Move Drivers (why the ticker moved: macro/earnings/organic) ──
    move_ctx = _classify_move_drivers(tech_rows, earnings)

    # ── Per-Event Response Profiling (Augen: per-stock per-event) ──
    event_ctx = _profile_event_reactions(tech_rows)

    # ── Data Maturity (survivorship bias guard — Chan/Harris) ──
    data_maturity = _assess_data_maturity(history_depth, ready_count, tech_rows)

    # ── Behavioral Score (composite 0-100) ────────────────────────────
    score = _compute_behavioral_score(
        regime_duration=regime_duration,
        regime_path=regime_path,
        adx_trend=adx_trend,
        adx_vals=adx_vals,
        rsi_range=rsi_range,
        vol_accum=vol_accum,
        dqs_trend=dqs_trend,
        iv_arc=iv_arc,
        earnings_ctx=earnings_ctx,
        mgmt_track=mgmt_track,
        mgmt_confidence=mgmt_confidence,
        data_points=history_depth,
    )

    return {
        'Regime_Duration': regime_duration,
        'Regime_Path': regime_path_str,
        'ADX_Trend': adx_trend,
        'RSI_Range': rsi_range,
        'Volume_Accumulation': vol_accum,
        'Scan_Frequency': ready_count,
        'DQS_Trend': dqs_trend,
        'Signal_Age': signal_age,
        'IV_Arc': iv_arc,
        'Earnings_Context': earnings_ctx,
        'Mgmt_Track_Record': mgmt_track,
        'Prior_Trades': prior_trades,
        'Mgmt_Confidence': mgmt_confidence,
        'Mgmt_Strategy_Detail': strat_detail,
        'Mgmt_Recency_Factor': recency_factor,
        'Fault_Pattern': fault_pattern,
        'Contradiction_Flags': ', '.join(contradictions) if contradictions else '',
        'Move_Drivers': move_ctx.get('Move_Drivers', ''),
        'Last_Dip_Context': move_ctx.get('Last_Dip_Context', ''),
        'Event_Reactions': event_ctx.get('Event_Reactions', ''),
        'Worst_Event_Type': event_ctx.get('Worst_Event_Type', ''),
        'Data_Maturity': data_maturity,
        'History_Depth': history_depth,
        'Behavioral_Score': score,
    }


# ── Classification helpers ────────────────────────────────────────────────────

def _classify_indicator_trend(vals: List[float]) -> str:
    """
    Classify indicator series into directional trend.

    BUILDING:  rising from low levels (slope > 0)
    PEAKING:   high levels starting to flatten or decline
    FADING:    declining from high levels
    FLAT:      no clear direction
    """
    if len(vals) < 5:
        return 'UNKNOWN'

    # Split into halves for trend detection
    mid = len(vals) // 2
    first_half_mean = np.mean(vals[:mid])
    second_half_mean = np.mean(vals[mid:])
    overall_mean = np.mean(vals)

    # Recent slope (last 5 points)
    recent = vals[-5:]
    slope = (recent[-1] - recent[0]) / max(len(recent) - 1, 1)

    delta = second_half_mean - first_half_mean

    if delta > 3 and slope > 0.3:
        return 'BUILDING'
    elif first_half_mean > overall_mean and slope < -0.3:
        return 'FADING'
    elif second_half_mean > overall_mean * 1.1 and abs(slope) < 0.5:
        return 'PEAKING'
    else:
        return 'FLAT'


def _classify_volume(obv_slopes: List[float]) -> str:
    """
    Classify OBV_Slope history into accumulation pattern.

    ACCUMULATING:  >60% of readings positive
    DISTRIBUTING:  >60% of readings negative
    NEUTRAL:       mixed
    """
    if len(obv_slopes) < 3:
        return 'UNKNOWN'

    positive = sum(1 for s in obv_slopes if s > 0)
    ratio = positive / len(obv_slopes)

    if ratio >= 0.60:
        return 'ACCUMULATING'
    elif ratio <= 0.40:
        return 'DISTRIBUTING'
    else:
        return 'NEUTRAL'


def _classify_score_trend(scores: List[float]) -> str:
    """
    Classify DQS score evolution.

    CLIMBING:     steady upward (last third > first third by >5)
    DECLINING:    steady downward
    V_RECOVERY:   dropped then recovered (min in middle, end near start)
    STABLE:       minimal change
    """
    if len(scores) < 3:
        return 'UNKNOWN'

    first = np.mean(scores[:max(1, len(scores) // 3)])
    last = np.mean(scores[-max(1, len(scores) // 3):])
    delta = last - first

    if len(scores) >= 5:
        mid_min = min(scores[1:-1])
        if mid_min < first - 5 and last > mid_min + 5:
            return 'V_RECOVERY'

    if delta > 5:
        return 'CLIMBING'
    elif delta < -5:
        return 'DECLINING'
    else:
        return 'STABLE'


def _classify_iv_arc(iv_rows: List[dict]) -> str:
    """
    Classify IV evolution from iv_term_history.

    RISING:    IV trending up (second half > first half)
    FALLING:   IV trending down
    SPIKING:   Recent spike (last 5 readings much higher than prior)
    STABLE:    Flat IV
    """
    if len(iv_rows) < 5:
        return 'UNKNOWN'

    iv_vals = [float(r.get('iv_30d', 0) or 0) for r in iv_rows
               if r.get('iv_30d') is not None and not _is_nan(r.get('iv_30d', 0))]

    if len(iv_vals) < 5:
        return 'UNKNOWN'

    mid = len(iv_vals) // 2
    first_half = np.mean(iv_vals[:mid])
    second_half = np.mean(iv_vals[mid:])
    recent = np.mean(iv_vals[-5:])
    overall = np.mean(iv_vals)

    # Spike: recent 5 readings > 1.3× overall mean
    if recent > overall * 1.3 and recent > first_half * 1.2:
        return 'SPIKING'

    delta_pct = (second_half - first_half) / max(first_half, 1.0)
    if delta_pct > 0.10:
        return 'RISING'
    elif delta_pct < -0.10:
        return 'FALLING'
    else:
        return 'STABLE'


def _classify_earnings(earnings: dict) -> str:
    """
    Classify earnings reliability from earnings_stats.

    RELIABLE_BEATER:  beat_rate >= 0.70 (beats 70%+ of quarters)
    MIXED:            beat_rate 0.40-0.69
    UNRELIABLE:       beat_rate < 0.40
    NO_DATA:          no earnings stats available
    """
    if not earnings:
        return 'NO_DATA'

    beat_rate = earnings.get('beat_rate')
    if beat_rate is None or _is_nan(beat_rate):
        return 'NO_DATA'

    beat_rate = float(beat_rate)
    # beat_rate from earnings_stats table is 0-1 decimal (beats/quarters)
    if beat_rate >= 0.70:
        return 'RELIABLE_BEATER'
    elif beat_rate >= 0.40:
        return 'MIXED'
    else:
        return 'UNRELIABLE'


def _classify_mgmt_track(mgmt) -> tuple:
    """
    Classify management track record with sample-size discipline and
    recency decay.

    Accepts either:
      - list of per-trade dicts (new format from _query_management_history)
      - dict with total_trades/winning_trades (legacy test format)

    Returns (classification, prior_trade_count, confidence, recency_factor):
      classification: PROVEN_WINNER / MIXED / PROVEN_LOSER / NO_DATA
      prior_trade_count: int
      confidence: HIGH / MEDIUM / LOW / NONE
      recency_factor: 0.0-1.0 (1.0 = all recent, lower = stale history)
    """
    # Handle legacy dict format (for backward compatibility with tests)
    if isinstance(mgmt, dict):
        if not mgmt:
            return 'NO_DATA', 0, 'NONE', 1.0
        total = int(mgmt.get('total_trades', 0) or 0)
        wins = int(mgmt.get('winning_trades', 0) or 0)
        if total == 0:
            return 'NO_DATA', 0, 'NONE', 1.0
        win_rate = wins / total
        confidence = 'HIGH' if total >= 6 else ('MEDIUM' if total >= 3 else 'LOW')
        label = 'PROVEN_WINNER' if win_rate >= 0.60 else ('MIXED' if win_rate >= 0.40 else 'PROVEN_LOSER')
        return label, total, confidence, 1.0

    # New: list of per-trade dicts
    if not mgmt:
        return 'NO_DATA', 0, 'NONE', 1.0

    trades = mgmt
    closed = [t for t in trades if t.get('is_closed')]
    total = len(closed) if closed else len(trades)

    if total == 0:
        return 'NO_DATA', 0, 'NONE', 1.0

    # ── Recency decay ──────────────────────────────────────────────
    # Recent trades (< 30d) weight 1.0, 30-90d weight 0.7, 90+d weight 0.4
    # This prevents permanently punishing a ticker for mistakes the
    # newer system no longer makes.
    weighted_wins = 0.0
    weighted_total = 0.0
    age_weights = []

    for t in (closed if closed else trades):
        age = int(t.get('age_days', 0) or 0)
        pnl = float(t.get('best_pnl', 0) or 0)

        if age <= 30:
            w = 1.0
        elif age <= 90:
            w = 0.7
        else:
            w = 0.4
        age_weights.append(w)

        weighted_total += w
        if pnl > 0:
            weighted_wins += w

    recency_factor = round(np.mean(age_weights), 2) if age_weights else 1.0

    # ── Recency-weighted win rate ──────────────────────────────────
    weighted_win_rate = weighted_wins / weighted_total if weighted_total > 0 else 0.5

    # ── Sample-size confidence ─────────────────────────────────────
    # 1-2 trades: LOW (one bad trade shouldn't define a ticker)
    # 3-5 trades: MEDIUM
    # 6+  trades: HIGH (enough signal to trust)
    if total >= 6:
        confidence = 'HIGH'
    elif total >= 3:
        confidence = 'MEDIUM'
    else:
        confidence = 'LOW'

    # ── PSR-inspired variance adjustment (Lopez de Prado) ────────
    # High outcome variance means the sample doesn't tell a clear story.
    # If coefficient of variation is extreme, downgrade confidence by one
    # level — even 6+ trades are unreliable if they swing wildly.
    pnl_vals = [float(t.get('best_pnl', 0) or 0) for t in (closed if closed else trades)]
    if len(pnl_vals) >= 3:
        pnl_std = float(np.std(pnl_vals))
        pnl_mean = abs(float(np.mean(pnl_vals)))
        # CoV > 2.0 = outcomes are too noisy to trust the classification.
        # Near-zero mean with high std is also unreliable (large swings
        # that cancel out don't mean the ticker is predictable).
        noisy = (pnl_mean > 0 and (pnl_std / pnl_mean) > 2.0) or \
                (pnl_mean < 1.0 and pnl_std > 100)
        if noisy:
            _DOWNGRADE = {'HIGH': 'MEDIUM', 'MEDIUM': 'LOW', 'LOW': 'LOW'}
            confidence = _DOWNGRADE.get(confidence, confidence)

    # ── Classification ─────────────────────────────────────────────
    if weighted_win_rate >= 0.60:
        label = 'PROVEN_WINNER'
    elif weighted_win_rate >= 0.40:
        label = 'MIXED'
    else:
        label = 'PROVEN_LOSER'

    return label, total, confidence, recency_factor


def _classify_strategy_breakdown(trades: List[dict]) -> str:
    """
    Per-strategy win/loss breakdown.

    Returns compact string like "CC:2W/1L SP:0W/2L" showing which
    strategies work and which don't for this ticker.
    """
    if not trades:
        return ''

    strat_map: Dict[str, dict] = {}
    for t in trades:
        strat = str(t.get('strategy', '') or '').strip()
        if not strat:
            continue
        if strat not in strat_map:
            strat_map[strat] = {'wins': 0, 'losses': 0}
        pnl = float(t.get('best_pnl', 0) or 0)
        is_closed = t.get('is_closed', 0)
        if is_closed:
            if pnl > 0:
                strat_map[strat]['wins'] += 1
            else:
                strat_map[strat]['losses'] += 1

    if not strat_map:
        return ''

    # Abbreviate strategy names
    _ABBREV = {
        'COVERED_CALL': 'CC', 'SHORT_PUT': 'SP', 'BUY_WRITE': 'BW',
        'LONG_CALL': 'LC', 'LONG_PUT': 'LP', 'STOCK_ONLY': 'STK',
        'MULTI_LEG': 'ML',
    }
    parts = []
    for strat, counts in sorted(strat_map.items()):
        abbr = _ABBREV.get(strat, strat[:3].upper())
        parts.append(f"{abbr}:{counts['wins']}W/{counts['losses']}L")
    return ' '.join(parts)


def _classify_fault_pattern(trades: List[dict]) -> str:
    """
    Distinguish ticker fault from execution fault.

    Looks at clustering of losses to determine if the problem is:
      STRATEGY  — losses cluster in one strategy type
      DTE       — losses cluster in short-DTE (<21d) entries
      TIMING    — losses in early lifecycle (caught late/chasing)
      TICKER    — losses across multiple strategies/DTEs
      NONE      — no meaningful losses
      INSUFFICIENT_DATA — too few trades to determine

    This tells the system "avoid ticker" vs "fix implementation."
    """
    if not trades:
        return 'INSUFFICIENT_DATA'

    closed = [t for t in trades if t.get('is_closed')]
    if len(closed) < 2:
        return 'INSUFFICIENT_DATA'

    losers = [t for t in closed if float(t.get('best_pnl', 0) or 0) <= 0]
    if not losers:
        return 'NONE'

    if len(losers) == 1:
        return 'INSUFFICIENT_DATA'

    # Check strategy clustering
    loser_strats = [str(t.get('strategy', '') or '') for t in losers]
    unique_strats = set(s for s in loser_strats if s)
    winner_strats = set(str(t.get('strategy', '') or '')
                        for t in closed if float(t.get('best_pnl', 0) or 0) > 0)

    # If all losses are in one strategy but wins exist in others → STRATEGY fault
    if len(unique_strats) == 1 and winner_strats - unique_strats:
        return 'STRATEGY'

    # Check DTE clustering: are losses mostly short-DTE?
    loser_dtes = [int(t.get('entry_dte', 45) or 45) for t in losers]
    short_dte_losers = sum(1 for d in loser_dtes if d <= 21)
    if short_dte_losers >= len(losers) * 0.7 and len(losers) >= 2:
        return 'DTE'

    # Check lifecycle clustering: losses in early lifecycle = chasing entries
    loser_phases = [str(t.get('lifecycle_phase', '') or '') for t in losers]
    early_phases = sum(1 for p in loser_phases if p in ('Early', 'Opening'))
    if early_phases >= len(losers) * 0.7 and len(losers) >= 2:
        return 'TIMING'

    # Losses across multiple strategies/DTEs → fundamental ticker issue
    if len(unique_strats) >= 2:
        return 'TICKER'

    return 'INSUFFICIENT_DATA'


def _detect_contradictions(
    *,
    dqs_trend: str,
    adx_trend: str,
    vol_accum: str,
    iv_arc: str,
    earnings_ctx: str,
    mgmt_track: str,
    regime_path: List[str],
) -> List[str]:
    """
    Detect contradictions between market evidence and portfolio evidence.

    These flags are where the best learning lives — they surface the gap
    between "this looks good" and "this has actually worked for us."
    """
    flags = []

    # ── Market says good, portfolio says bad ──────────────────────
    if dqs_trend in ('CLIMBING', 'V_RECOVERY') and mgmt_track == 'PROVEN_LOSER':
        flags.append('HIGH_DQS_PROVEN_LOSER')

    if earnings_ctx == 'RELIABLE_BEATER' and mgmt_track == 'PROVEN_LOSER':
        flags.append('STRONG_EARNINGS_WEAK_MGMT')

    if adx_trend == 'BUILDING' and mgmt_track == 'PROVEN_LOSER':
        flags.append('STRONG_MOMENTUM_PROVEN_LOSER')

    # ── Market says bad, portfolio says good ──────────────────────
    if dqs_trend == 'DECLINING' and mgmt_track == 'PROVEN_WINNER':
        flags.append('WEAK_DQS_PROVEN_WINNER')

    if adx_trend == 'FADING' and mgmt_track == 'PROVEN_WINNER':
        flags.append('FADING_MOMENTUM_PROVEN_WINNER')

    # ── Internal technical contradictions ─────────────────────────
    if adx_trend == 'BUILDING' and vol_accum == 'DISTRIBUTING':
        flags.append('MOMENTUM_VOLUME_DIVERGENCE')

    if iv_arc == 'SPIKING' and earnings_ctx == 'RELIABLE_BEATER':
        flags.append('IV_SPIKE_RELIABLE_EARNER')

    # ── Regime contradictions ─────────────────────────────────────
    constructive = {'Trending', 'Emerging_Trend', 'Breakout'}
    if regime_path and regime_path[-1] in constructive and mgmt_track == 'PROVEN_LOSER':
        flags.append('CONSTRUCTIVE_REGIME_PROVEN_LOSER')

    return flags


def _classify_move_drivers(
    tech_rows: List[dict],
    earnings: dict,
) -> dict:
    """
    Correlate significant regime changes with probable causes.

    For each detected regime transition, checks temporal proximity to:
      - Macro events (FOMC/CPI/NFP within ±2 trading days)
      - Earnings (within ±5 days of known earnings date)
      - Market-wide moves (via RS_vs_SPY — if stock moved WITH market)
      - Organic (none of the above — ticker-specific)

    Returns dict:
      Move_Drivers:    "MACRO:2 EARNINGS:1 ORGANIC:3" — driver counts
      Last_Dip_Context: most recent negative transition cause
    """
    result = {'Move_Drivers': '', 'Last_Dip_Context': ''}

    if len(tech_rows) < 5:
        return result

    # Import macro calendar (safe — pure function, no API)
    try:
        from config.macro_calendar import get_macro_proximity
    except ImportError:
        return result

    # Detect regime transitions (changes in Chart_Regime or significant RSI shifts)
    transitions = []
    for i in range(1, len(tech_rows)):
        curr = tech_rows[i]
        prev = tech_rows[i - 1]

        regime_changed = (
            curr.get('Chart_Regime') and prev.get('Chart_Regime')
            and str(curr['Chart_Regime']) not in ('None', '', 'nan')
            and str(prev['Chart_Regime']) not in ('None', '', 'nan')
            and curr['Chart_Regime'] != prev['Chart_Regime']
        )

        # RSI shift > 15 points = significant move
        rsi_shift = False
        try:
            rsi_curr = float(curr.get('RSI_14', 0) or 0)
            rsi_prev = float(prev.get('RSI_14', 0) or 0)
            if rsi_curr > 0 and rsi_prev > 0 and abs(rsi_curr - rsi_prev) > 15:
                rsi_shift = True
        except (TypeError, ValueError):
            pass

        if regime_changed or rsi_shift:
            ts = curr.get('Snapshot_TS')
            if ts is None:
                continue

            # Determine direction (constructive vs destructive)
            _constructive = {'Trending', 'Emerging_Trend', 'Breakout'}
            _destructive = {'Overextended', 'Breakdown', 'Compressed'}
            curr_regime = str(curr.get('Chart_Regime', ''))
            prev_regime = str(prev.get('Chart_Regime', ''))

            if curr_regime in _destructive or (rsi_shift and rsi_curr < rsi_prev):
                direction = 'DOWN'
            elif curr_regime in _constructive or (rsi_shift and rsi_curr > rsi_prev):
                direction = 'UP'
            else:
                direction = 'NEUTRAL'

            # Check RS_vs_SPY — did the stock move with or against the market?
            rs_spy = curr.get('RS_vs_SPY_20d')
            market_aligned = False
            if rs_spy is not None and not _is_nan(rs_spy):
                try:
                    rs_val = float(rs_spy)
                    # If RS near zero (±3%), the move was market-driven
                    if abs(rs_val) < 3.0:
                        market_aligned = True
                except (TypeError, ValueError):
                    pass

            transitions.append({
                'ts': ts,
                'direction': direction,
                'from_regime': prev_regime,
                'to_regime': curr_regime,
                'market_aligned': market_aligned,
            })

    if not transitions:
        return result

    # Classify each transition by cause
    drivers = {'MACRO': 0, 'EARNINGS': 0, 'MARKET_WIDE': 0, 'ORGANIC': 0}
    last_dip_context = ''

    for t in transitions:
        ts = t['ts']
        cause = 'ORGANIC'  # default

        # Convert ts to date for macro check
        try:
            if hasattr(ts, 'date'):
                snap_date = ts.date() if callable(ts.date) else ts.date
            elif hasattr(ts, 'year'):
                snap_date = ts
            else:
                from datetime import datetime as _dt
                snap_date = _dt.fromisoformat(str(ts)).date()
        except Exception:
            continue

        # Check macro proximity (±2 days)
        try:
            macro = get_macro_proximity(snap_date)
            if macro.days_to_next is not None and abs(macro.days_to_next) <= 2:
                cause = 'MACRO'
            elif macro.events_within_5d:
                # Check if any HIGH-impact event was within 2 days before
                from datetime import timedelta
                for evt in macro.events_within_5d:
                    days_diff = (snap_date - evt.event_date).days
                    if -2 <= days_diff <= 2 and evt.impact == 'HIGH':
                        cause = 'MACRO'
                        break
        except Exception:
            pass

        # Check earnings proximity (if we have earnings dates)
        if cause == 'ORGANIC' and earnings:
            # earnings_stats may not have the date directly,
            # but days_to_earnings in tech row is a signal
            # Also check if IV was spiking around this time (earnings IV ramp)
            pass  # earnings proximity handled below

        # If RS_vs_SPY shows market-aligned → market-wide (unless already macro)
        if cause == 'ORGANIC' and t['market_aligned']:
            cause = 'MARKET_WIDE'

        drivers[cause] += 1

        # Track last dip
        if t['direction'] == 'DOWN':
            last_dip_context = cause

    # Format output
    driver_parts = [f"{k}:{v}" for k, v in drivers.items() if v > 0]
    result['Move_Drivers'] = ' '.join(driver_parts)
    result['Last_Dip_Context'] = last_dip_context or 'NONE'

    return result


def _profile_event_reactions(
    tech_rows: List[dict],
) -> dict:
    """
    Per-event-type reaction profiling (Augen: per-stock, per-event analysis).

    For each macro event type (FOMC, CPI, NFP), measures average RSI change
    in tech_rows within ±1 day of the event. RSI delta is a proxy for
    directional price shock — positive = rally, negative = sell-off.

    Returns dict:
      Event_Reactions:  "FOMC:-3.2 CPI:+1.4 NFP:-0.8" (avg RSI delta)
      Worst_Event_Type: which event type causes largest negative reaction
    """
    result = {'Event_Reactions': '', 'Worst_Event_Type': ''}

    if len(tech_rows) < 5:
        return result

    try:
        from config.macro_calendar import MACRO_EVENTS_2026
    except ImportError:
        return result

    # Build date→RSI map from tech rows
    date_rsi = {}
    for r in tech_rows:
        ts = r.get('Snapshot_TS')
        rsi = r.get('RSI_14')
        if ts is None or rsi is None or _is_nan(rsi):
            continue
        try:
            if hasattr(ts, 'date'):
                d = ts.date() if callable(ts.date) else ts.date
            else:
                d = datetime.fromisoformat(str(ts)).date()
            date_rsi[d] = float(rsi)
        except Exception:
            continue

    if len(date_rsi) < 5:
        return result

    sorted_dates = sorted(date_rsi.keys())
    from datetime import timedelta

    # For each event type, find RSI changes around events
    event_deltas: Dict[str, List[float]] = {}
    for evt in MACRO_EVENTS_2026:
        evt_date = evt.event_date
        # Find closest tech row BEFORE event (1-3 days before)
        before_rsi = None
        for offset in range(1, 4):
            check = evt_date - timedelta(days=offset)
            if check in date_rsi:
                before_rsi = date_rsi[check]
                break
        # Find closest tech row AFTER event (0-2 days after)
        after_rsi = None
        for offset in range(0, 3):
            check = evt_date + timedelta(days=offset)
            if check in date_rsi:
                after_rsi = date_rsi[check]
                break

        if before_rsi is not None and after_rsi is not None:
            delta = after_rsi - before_rsi
            etype = evt.event_type
            if etype not in event_deltas:
                event_deltas[etype] = []
            event_deltas[etype].append(delta)

    if not event_deltas:
        return result

    # Compute averages per event type (need at least 2 observations)
    avg_by_type = {}
    for etype, deltas in event_deltas.items():
        if len(deltas) >= 2:
            avg_by_type[etype] = round(float(np.mean(deltas)), 1)

    if not avg_by_type:
        return result

    # Format output
    parts = [f"{k}:{'+' if v >= 0 else ''}{v}" for k, v in sorted(avg_by_type.items())]
    result['Event_Reactions'] = ' '.join(parts)

    # Worst event = largest negative avg RSI delta
    worst = min(avg_by_type.items(), key=lambda x: x[1])
    if worst[1] < -1.0:  # Only flag if meaningfully negative
        result['Worst_Event_Type'] = worst[0]

    return result


def _assess_data_maturity(
    history_depth: int,
    scan_frequency: int,
    tech_rows: List[dict],
) -> str:
    """
    Flag survivorship-bias risk for short-history tickers (Chan/Harris).

    Tickers recently added to the universe or with sparse data get flagged
    so the system doesn't over-trust thin behavioral evidence.

    Returns:
      MATURE:     40+ daily data points, appeared in scans before
      DEVELOPING: 15-39 data points or recent first appearance
      NEW_TICKER: <15 data points — behavioral memory unreliable
    """
    if history_depth < 15:
        return 'NEW_TICKER'

    # Check time span — even with many rows, a narrow date range is suspect
    if len(tech_rows) >= 2:
        try:
            first_ts = tech_rows[0].get('Snapshot_TS')
            last_ts = tech_rows[-1].get('Snapshot_TS')
            if hasattr(first_ts, 'date') and hasattr(last_ts, 'date'):
                first_d = first_ts.date() if callable(first_ts.date) else first_ts.date
                last_d = last_ts.date() if callable(last_ts.date) else last_ts.date
                span_days = (last_d - first_d).days
                if span_days < 20:
                    return 'NEW_TICKER'
        except Exception:
            pass

    if history_depth >= 40:
        return 'MATURE'

    return 'DEVELOPING'


def _compute_behavioral_score(
    *,
    regime_duration: int,
    regime_path: List[str],
    adx_trend: str,
    adx_vals: List[float],
    rsi_range: float,
    vol_accum: str,
    dqs_trend: str,
    iv_arc: str,
    earnings_ctx: str,
    mgmt_track: str,
    mgmt_confidence: str = 'NONE',
    data_points: int,
) -> int:
    """
    Composite behavioral quality score (0-100).

    Rewards: stable constructive regimes, building trends, accumulation,
             improving scores, favorable IV, reliable earnings, proven winners.
    Penalizes: choppy RSI, distribution, declining scores, spiking IV,
             unreliable earnings, proven losers, regime instability.

    Sample-size discipline: management impact is scaled by confidence.
      HIGH (6+ trades):   full weight  (±8/−10)
      MEDIUM (3-5 trades): 60% weight  (±5/−6)
      LOW (1-2 trades):   30% weight   (±2/−3)
      NONE:               0 weight

    This prevents one or two outcomes from over-shaping the score.
    """
    score = 50  # baseline

    # 1. Regime stability: longer = more conviction (up to +15)
    if regime_duration >= 20:
        score += 15
    elif regime_duration >= 10:
        score += 12
    elif regime_duration >= 5:
        score += 8
    elif regime_duration >= 3:
        score += 4

    # 2. Regime path quality: classic breakout path bonus
    _CONSTRUCTIVE_REGIMES = {'Trending', 'Emerging_Trend', 'Breakout'}
    _DESTRUCTIVE_REGIMES = {'Overextended', 'Breakdown'}
    if regime_path:
        current = regime_path[-1]
        if current in _CONSTRUCTIVE_REGIMES:
            score += 8
        elif current in _DESTRUCTIVE_REGIMES:
            score -= 8
        # Classic arc: Compression/Range → Emerging → Trending
        if len(regime_path) >= 2:
            prev = regime_path[-2]
            if prev in ('Compression', 'Range_Bound') and current in ('Emerging_Trend', 'Trending', 'Breakout'):
                score += 5  # classic breakout progression

    # 3. ADX trend: building = momentum forming (+8), fading = exhaustion (-8)
    if adx_trend == 'BUILDING':
        score += 8
    elif adx_trend == 'FADING':
        score -= 8
    elif adx_trend == 'PEAKING':
        score -= 3

    # 4. RSI range: tight = clean trend (+4), wide = choppy (-4)
    if rsi_range > 0:
        if rsi_range < 15:
            score += 4
        elif rsi_range > 35:
            score -= 4

    # 5. Volume conviction
    if vol_accum == 'ACCUMULATING':
        score += 6
    elif vol_accum == 'DISTRIBUTING':
        score -= 6

    # 6. DQS score trend
    if dqs_trend == 'CLIMBING':
        score += 5
    elif dqs_trend == 'V_RECOVERY':
        score += 3
    elif dqs_trend == 'DECLINING':
        score -= 5

    # 7. IV arc: falling IV = cheaper entry (+4), spiking = caution (-5)
    if iv_arc == 'FALLING':
        score += 4  # IV contracting — cheaper to buy premium
    elif iv_arc == 'SPIKING':
        score -= 5  # IV spike — expensive entry, crush risk

    # 8. Earnings reliability
    if earnings_ctx == 'RELIABLE_BEATER':
        score += 5
    elif earnings_ctx == 'UNRELIABLE':
        score -= 4

    # 9. Management track record (from replay/closed trades)
    # Scaled by sample-size confidence — 1-2 trades shouldn't define a ticker
    _CONFIDENCE_SCALE = {'HIGH': 1.0, 'MEDIUM': 0.6, 'LOW': 0.3, 'NONE': 0.0}
    conf_scale = _CONFIDENCE_SCALE.get(mgmt_confidence, 0.0)
    if mgmt_track == 'PROVEN_WINNER':
        score += int(8 * conf_scale)
    elif mgmt_track == 'PROVEN_LOSER':
        score -= int(10 * conf_scale)

    # 10. Data confidence: more history = higher confidence (up to +5)
    if data_points >= 40:
        score += 5
    elif data_points >= 20:
        score += 3
    elif data_points >= 10:
        score += 1
    elif data_points < 5:
        score -= 5

    return max(0, min(100, score))


# ── Utilities ─────────────────────────────────────────────────────────────────

def _dedupe_sequence(items: List[str]) -> List[str]:
    """Remove consecutive duplicates: [A,A,B,B,B,C] → [A,B,C]."""
    if not items:
        return []
    deduped = [items[0]]
    for item in items[1:]:
        if item != deduped[-1]:
            deduped.append(item)
    return deduped


def _is_nan(v) -> bool:
    """Check if value is NaN."""
    try:
        return isinstance(v, float) and np.isnan(v)
    except (TypeError, ValueError):
        return False


# ── DB queries ────────────────────────────────────────────────────────────────

def _query_tech_history(
    con,
    tickers: List[str],
) -> Dict[str, List[dict]]:
    """Query YTD technical_indicators per ticker."""
    ytd = _ytd_start()
    try:
        placeholders = ', '.join(['?' for _ in tickers])
        query = f"""
            SELECT Ticker, ADX_14, RSI_14, Chart_Regime, OBV_Slope,
                   Volume_Ratio, Keltner_Squeeze_On, Trend_Slope,
                   ATR_Rank, Market_Structure, BB_Position,
                   RS_vs_SPY_20d, Snapshot_TS
            FROM technical_indicators
            WHERE Ticker IN ({placeholders})
              AND Snapshot_TS >= '{ytd}'
            ORDER BY Ticker, Snapshot_TS ASC
        """
        df = con.execute(query, tickers).df()
    except Exception:
        try:
            placeholders = ', '.join(['?' for _ in tickers])
            query = f"""
                SELECT Ticker, ADX_14, RSI_14, Snapshot_TS
                FROM technical_indicators
                WHERE Ticker IN ({placeholders})
                  AND Snapshot_TS >= '{ytd}'
                ORDER BY Ticker, Snapshot_TS ASC
            """
            df = con.execute(query, tickers).df()
        except Exception as e:
            logger.debug(f"[BehavioralMemory] Tech query failed: {e}")
            return {}

    if df.empty:
        return {}

    result: Dict[str, List[dict]] = {}
    for ticker, group in df.groupby('Ticker'):
        result[ticker] = group.to_dict('records')
    return result


def _query_scan_history(
    con,
    tickers: List[str],
) -> Dict[str, List[dict]]:
    """Query YTD scan_candidates per ticker."""
    ytd = _ytd_start()
    try:
        placeholders = ', '.join(['?' for _ in tickers])
        query = f"""
            SELECT Ticker, DQS_Score, Execution_Status, Confidence_Band,
                   Regime, Signal_Type, Scan_TS
            FROM scan_candidates
            WHERE Ticker IN ({placeholders})
              AND Scan_TS >= '{ytd}'
            ORDER BY Ticker, Scan_TS ASC
        """
        df = con.execute(query, tickers).df()
    except Exception as e:
        logger.debug(f"[BehavioralMemory] Scan query failed: {e}")
        return {}

    if df.empty:
        return {}

    result: Dict[str, List[dict]] = {}
    for ticker, group in df.groupby('Ticker'):
        result[ticker] = group.to_dict('records')
    return result


def _query_iv_history(
    con,
    tickers: List[str],
) -> Dict[str, List[dict]]:
    """Query YTD IV history from iv_history.duckdb."""
    if con is None:
        return {}
    ytd = _ytd_start()
    try:
        placeholders = ', '.join(['?' for _ in tickers])
        query = f"""
            SELECT ticker, iv_30d, created_at AS snapshot_ts
            FROM iv_term_history
            WHERE ticker IN ({placeholders})
              AND created_at >= '{ytd}'
              AND iv_30d IS NOT NULL
            ORDER BY ticker, created_at ASC
        """
        df = con.execute(query, tickers).df()
    except Exception as e:
        logger.debug(f"[BehavioralMemory] IV query failed: {e}")
        return {}

    if df.empty:
        return {}

    result: Dict[str, List[dict]] = {}
    col_name = 'ticker' if 'ticker' in df.columns else 'Ticker'
    for ticker, group in df.groupby(col_name):
        result[ticker] = group.to_dict('records')
    return result


def _query_management_history(
    con,
    tickers: List[str],
) -> Dict[str, List[dict]]:
    """
    Query management_recommendations for per-trade detail per ticker.

    Returns dict: ticker → list of per-trade dicts with strategy, pnl,
    DTE, age, and lifecycle info for strategy-level analysis, fault
    attribution, and recency decay.
    """
    try:
        placeholders = ', '.join(['?' for _ in tickers])
        query = f"""
            WITH per_trade AS (
                SELECT
                    COALESCE(Underlying_Ticker, SPLIT_PART(TradeID, '_', 1)) AS ticker,
                    TradeID,
                    MAX(Strategy) AS strategy,
                    MAX(CASE WHEN Action IN ('EXIT', 'CLOSE', 'EXPIRED', 'AWAITING_SETTLEMENT')
                         THEN 1 ELSE 0 END) AS is_closed,
                    MAX(COALESCE(
                        TRY_CAST(Trajectory_Stock_Return AS DOUBLE),
                        TRY_CAST(Profit_Cushion AS DOUBLE),
                        TRY_CAST(PnL_Total AS DOUBLE),
                        0
                    )) AS best_pnl,
                    MAX(TRY_CAST(DTE AS INTEGER)) AS entry_dte,
                    MAX(Snapshot_TS) AS last_seen_ts,
                    DATEDIFF('day', CAST(MAX(Snapshot_TS) AS TIMESTAMP), CAST(CURRENT_TIMESTAMP AS TIMESTAMP)) AS age_days,
                    MAX(Lifecycle_Phase) AS lifecycle_phase
                FROM management_recommendations
                WHERE COALESCE(Underlying_Ticker, SPLIT_PART(TradeID, '_', 1)) IN ({placeholders})
                GROUP BY 1, 2
            )
            SELECT * FROM per_trade ORDER BY ticker, last_seen_ts DESC
        """
        df = con.execute(query, tickers).df()
    except Exception as e:
        logger.debug(f"[BehavioralMemory] Management query failed: {e}")
        return {}

    if df.empty:
        return {}

    result: Dict[str, List[dict]] = {}
    col_name = 'ticker' if 'ticker' in df.columns else 'Ticker'
    for ticker_val, group in df.groupby(col_name):
        result[ticker_val] = group.to_dict('records')
    return result


def _query_earnings_stats(
    con,
    tickers: List[str],
) -> Dict[str, dict]:
    """Query earnings_stats for beat rate and crush data."""
    try:
        placeholders = ', '.join(['?' for _ in tickers])
        query = f"""
            SELECT ticker, beat_rate, avg_iv_crush_pct, avg_move_ratio,
                   consecutive_beats, consecutive_misses
            FROM earnings_stats
            WHERE ticker IN ({placeholders})
        """
        df = con.execute(query, tickers).df()
    except Exception as e:
        logger.debug(f"[BehavioralMemory] Earnings query failed: {e}")
        return {}

    if df.empty:
        return {}

    result: Dict[str, dict] = {}
    col_name = 'ticker' if 'ticker' in df.columns else 'Ticker'
    for _, row in df.iterrows():
        result[row[col_name]] = row.to_dict()
    return result
