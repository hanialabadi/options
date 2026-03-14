"""
Trade Replay Engine
===================

Reconstructs the full decision history of each trade and annotates every
daily signal with hindsight outcomes from actual price data.

Two modes:
  1. Per-trade replay:  complete annotated narrative for a single TradeID
  2. Aggregate metrics:  signal accuracy stats across all trades

Data sources (all READ-ONLY):
  - management_recommendations  — daily decision snapshots
  - closed_trades               — entry/exit with outcome classification
  - price_history               — daily OHLCV for underlying
  - entry_anchors               — frozen entry state

Design constraints:
  - Read-only against all DuckDB tables (no writes, no new tables)
  - Non-blocking: per-trade failures log warning, never halt
  - Direction-aware: EXIT correctness depends on strategy type
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd

from core.management.cycle3.feedback.replay_types import (
    DecisionPoint,
    SignalAccuracyMetrics,
    TradeReplay,
)

logger = logging.getLogger(__name__)

# ── Strategy direction classification ──────────────────────────────────
# EXIT is "correct" when the stock moved AGAINST the position afterward.
_BULLISH_STRATEGIES = {
    "LONG_CALL", "BUY_CALL", "LEAPS_CALL",
    "BUY_WRITE", "COVERED_CALL", "CC", "BW",
}
_BEARISH_STRATEGIES = {
    "LONG_PUT", "BUY_PUT",
}
_SHORT_PUT_STRATEGIES = {
    "SHORT_PUT", "CSP", "CASH_SECURED_PUT",
}


def replay_all_trades(
    con,
    trade_ids: Optional[List[str]] = None,
    ticker_filter: Optional[str] = None,
    include_open: bool = True,
    include_closed: bool = True,
) -> Tuple[List[TradeReplay], SignalAccuracyMetrics]:
    """
    Main entry point. Replays all trades (or a filtered subset).

    Returns (replays, aggregate_metrics).
    """
    # ── 1. Load decision history (deduplicated: one row per TradeID per day) ──
    decisions_df = _load_decisions(con, trade_ids, ticker_filter)
    if decisions_df.empty:
        logger.warning("[Replay] No decision history found")
        return [], SignalAccuracyMetrics()

    all_trade_ids = decisions_df["TradeID"].unique().tolist()

    # ── 2. Load closed trades ──
    closures = _load_closures(con)

    # Filter by open/closed
    closed_ids = set(closures.keys())
    if not include_closed:
        all_trade_ids = [t for t in all_trade_ids if t not in closed_ids]
    if not include_open:
        all_trade_ids = [t for t in all_trade_ids if t in closed_ids]

    if not all_trade_ids:
        return [], SignalAccuracyMetrics()

    # ── 3. Load entry anchors ──
    anchors = _load_anchors(con)

    # ── 4. Load price history for all relevant tickers ──
    tickers = decisions_df.loc[
        decisions_df["TradeID"].isin(all_trade_ids), "Underlying_Ticker"
    ].unique().tolist()
    earliest = decisions_df["run_date"].min()
    price_cache = _load_price_history(con, tickers, earliest)

    # ── 5. Replay each trade ──
    replays: List[TradeReplay] = []
    for tid in all_trade_ids:
        try:
            trade_decisions = decisions_df[decisions_df["TradeID"] == tid]
            replay = _replay_single_trade(
                trade_id=tid,
                decisions_df=trade_decisions,
                anchor=anchors.get(tid),
                closure=closures.get(tid),
                price_cache=price_cache,
            )
            replays.append(replay)
        except Exception as e:
            logger.warning("[Replay] Failed for %s: %s", tid, e)

    # ── 6. Aggregate ──
    metrics = _aggregate_metrics(replays)
    replays.sort(key=lambda r: (not r.is_closed, r.ticker, r.trade_id))

    return replays, metrics


# ═══════════════════════════════════════════════════════════════════════
# SQL Loaders
# ═══════════════════════════════════════════════════════════════════════

def _load_decisions(
    con, trade_ids: Optional[List[str]], ticker_filter: Optional[str],
) -> pd.DataFrame:
    """Load decision history, deduplicated to one row per TradeID per day."""
    where_parts = []
    if trade_ids:
        ids_csv = ", ".join(f"'{t}'" for t in trade_ids)
        where_parts.append(f"TradeID IN ({ids_csv})")
    if ticker_filter:
        where_parts.append(f"Underlying_Ticker = '{ticker_filter}'")

    where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    sql = f"""
        WITH ranked AS (
            SELECT
                TradeID,
                Underlying_Ticker,
                Strategy,
                Action,
                Urgency,
                "UL Last" AS ul_price,
                Strike,
                DTE,
                Total_GL_Decimal AS pnl,
                IV_Now,
                Doctrine_Source,
                LEFT(COALESCE(Rationale, ''), 200) AS rationale_digest,
                Snapshot_TS,
                CAST(Snapshot_TS AS DATE) AS run_date,
                ROW_NUMBER() OVER (
                    PARTITION BY TradeID, CAST(Snapshot_TS AS DATE)
                    ORDER BY Snapshot_TS DESC
                ) AS rn
            FROM management_recommendations
            {where_clause}
        )
        SELECT * EXCLUDE (rn)
        FROM ranked
        WHERE rn = 1
        ORDER BY TradeID, run_date
    """
    try:
        return con.execute(sql).fetchdf()
    except Exception as e:
        logger.warning("[Replay] Decision query failed: %s", e)
        return pd.DataFrame()


def _load_closures(con) -> Dict[str, dict]:
    """Load closed trades into a dict keyed by TradeID."""
    sql = """
        SELECT
            TradeID, Underlying_Ticker, Strategy,
            Entry_TS, Exit_TS,
            Entry_UL_Price, Exit_UL_Price,
            PnL_Pct, PnL_Dollar,
            Days_Held, MFE_Pct, MAE_Pct,
            Outcome_Type, Exit_Signal_Followed
        FROM closed_trades
    """
    try:
        df = con.execute(sql).fetchdf()
        return {row["TradeID"]: row.to_dict() for _, row in df.iterrows()}
    except Exception:
        return {}


def _load_anchors(con) -> Dict[str, dict]:
    """Load entry anchors, one per TradeID (option leg preferred)."""
    sql = """
        SELECT
            TradeID,
            Underlying_Price_Entry AS ul_entry,
            Premium_Entry AS premium_entry,
            Strike AS strike_entry,
            IV_Entry AS iv_entry,
            DTE AS dte_entry,
            Entry_Snapshot_TS AS entry_ts
        FROM entry_anchors
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY TradeID
            ORDER BY CASE WHEN Strike IS NOT NULL THEN 0 ELSE 1 END, LegIndex
        ) = 1
    """
    try:
        df = con.execute(sql).fetchdf()
        return {row["TradeID"]: row.to_dict() for _, row in df.iterrows()}
    except Exception:
        return {}


def _load_price_history(
    con, tickers: List[str], earliest: date,
) -> Dict[str, List[dict]]:
    """Load price history into {ticker: [{date, close, high, low}, ...]}."""
    if not tickers:
        return {}

    tickers_csv = ", ".join(f"'{t}'" for t in tickers)
    sql = f"""
        SELECT ticker, date, close_price, high_price, low_price
        FROM price_history
        WHERE ticker IN ({tickers_csv})
          AND date >= '{earliest - timedelta(days=5)}'
        ORDER BY ticker, date
    """
    try:
        df = con.execute(sql).fetchdf()
    except Exception:
        return {}

    cache: Dict[str, List[dict]] = defaultdict(list)
    for _, row in df.iterrows():
        cache[row["ticker"]].append({
            "date": row["date"],
            "close": row["close_price"],
            "high": row["high_price"],
            "low": row["low_price"],
        })
    return dict(cache)


# ═══════════════════════════════════════════════════════════════════════
# Per-Trade Replay
# ═══════════════════════════════════════════════════════════════════════

def _replay_single_trade(
    trade_id: str,
    decisions_df: pd.DataFrame,
    anchor: Optional[dict],
    closure: Optional[dict],
    price_cache: Dict[str, List[dict]],
) -> TradeReplay:
    """Build a complete TradeReplay for one trade."""
    first_row = decisions_df.iloc[0]
    ticker = str(first_row.get("Underlying_Ticker", ""))
    strategy = str(first_row.get("Strategy", ""))

    replay = TradeReplay(
        trade_id=trade_id,
        ticker=ticker,
        strategy=strategy,
        is_closed=closure is not None,
    )

    # Entry state
    if anchor:
        replay.entry_ul_price = _safe_float(anchor.get("ul_entry"))
        replay.entry_strike = _safe_float(anchor.get("strike_entry"))
        replay.entry_premium = _safe_float(anchor.get("premium_entry"))
        replay.entry_iv = _safe_float(anchor.get("iv_entry"))
        entry_ts = anchor.get("entry_ts")
        if entry_ts is not None:
            replay.entry_date = pd.to_datetime(entry_ts).date() if not isinstance(entry_ts, date) else entry_ts

    # Exit state
    if closure:
        replay.exit_pnl_pct = _safe_float(closure.get("PnL_Pct"))
        replay.exit_pnl_dollar = _safe_float(closure.get("PnL_Dollar"))
        replay.outcome_type = closure.get("Outcome_Type")
        replay.exit_ul_price = _safe_float(closure.get("Exit_UL_Price"))
        exit_ts = closure.get("Exit_TS")
        if exit_ts is not None:
            replay.exit_date = pd.to_datetime(exit_ts).date() if not isinstance(exit_ts, date) else exit_ts

    # Build decision points
    for _, row in decisions_df.iterrows():
        dp = DecisionPoint(
            run_date=row["run_date"] if isinstance(row["run_date"], date) else pd.to_datetime(row["run_date"]).date(),
            action=str(row.get("Action", "") or ""),
            urgency=str(row.get("Urgency", "") or ""),
            ul_price=_safe_float(row.get("ul_price")) or 0.0,
            strike=_safe_float(row.get("Strike")),
            dte=_safe_float(row.get("DTE")),
            pnl_at_signal=_safe_float(row.get("pnl")),
            doctrine_source=str(row.get("Doctrine_Source", "") or ""),
            rationale_digest=str(row.get("rationale_digest", "") or ""),
        )
        replay.decisions.append(dp)

    # Price series
    prices = price_cache.get(ticker, [])
    replay.price_series = prices

    # Annotate with hindsight
    _annotate_hindsight(replay.decisions, prices, strategy)

    # Compute signal accuracy + counterfactuals
    _compute_signal_accuracy(replay)

    return replay


# ═══════════════════════════════════════════════════════════════════════
# Hindsight Annotation
# ═══════════════════════════════════════════════════════════════════════

def _annotate_hindsight(
    decisions: List[DecisionPoint],
    price_series: List[dict],
    strategy: str,
) -> None:
    """Annotate each decision with what happened 5 trading days later."""
    if not price_series:
        return

    # Build date→index lookup for fast forward scanning
    price_dates = [p["date"] for p in price_series]
    date_to_idx: Dict[date, int] = {}
    for i, d in enumerate(price_dates):
        dt = d if isinstance(d, date) else pd.to_datetime(d).date()
        date_to_idx[dt] = i

    strat_upper = strategy.upper() if strategy else ""

    for i, dp in enumerate(decisions):
        # Find this date or next available in price data
        idx = date_to_idx.get(dp.run_date)
        if idx is None:
            # Try next few days
            for offset in range(1, 4):
                check = dp.run_date + timedelta(days=offset)
                if check in date_to_idx:
                    idx = date_to_idx[check]
                    break
        if idx is None:
            continue

        # 5 trading days forward
        fwd_idx = min(idx + 5, len(price_series) - 1)
        if fwd_idx <= idx:
            continue

        dp.price_5d_after = price_series[fwd_idx]["close"]
        if dp.ul_price and dp.ul_price > 0 and dp.price_5d_after:
            dp.move_5d_pct = (dp.price_5d_after - dp.ul_price) / dp.ul_price * 100.0

        # Signal correctness
        if dp.action in ("EXIT", "REVIEW") and dp.move_5d_pct is not None:
            dp.signal_correct = _exit_was_correct(strat_upper, dp.move_5d_pct)
            if dp.signal_correct:
                dp.hindsight_note = f"CORRECT — stock moved against position ({dp.move_5d_pct:+.1f}%)"
            else:
                dp.hindsight_note = f"WRONG — stock recovered ({dp.move_5d_pct:+.1f}%)"

        elif dp.action in ("HOLD", "HOLD_FOR_REVERSION"):
            # HOLD correct if P&L improved by next decision change
            next_dp = decisions[i + 1] if i + 1 < len(decisions) else None
            if next_dp and dp.pnl_at_signal is not None and next_dp.pnl_at_signal is not None:
                improved = next_dp.pnl_at_signal >= dp.pnl_at_signal
                dp.signal_correct = improved
                delta = next_dp.pnl_at_signal - dp.pnl_at_signal
                dp.hindsight_note = (
                    f"CORRECT — P&L improved ${delta:+,.0f}"
                    if improved
                    else f"WRONG — P&L worsened ${delta:+,.0f}"
                )
            elif dp.move_5d_pct is not None:
                # Fallback: use price direction
                good = _hold_was_correct(strat_upper, dp.move_5d_pct)
                dp.signal_correct = good
                dp.hindsight_note = (
                    f"CORRECT — price favorable ({dp.move_5d_pct:+.1f}%)"
                    if good
                    else f"WRONG — price adverse ({dp.move_5d_pct:+.1f}%)"
                )

        elif dp.action == "ROLL":
            # ROLL: check if price stabilized (didn't move >5% against)
            if dp.move_5d_pct is not None:
                against = _exit_was_correct(strat_upper, dp.move_5d_pct)
                dp.signal_correct = not against  # roll correct if stock didn't keep falling
                dp.hindsight_note = (
                    f"CORRECT — stock stabilized ({dp.move_5d_pct:+.1f}%)"
                    if dp.signal_correct
                    else f"WRONG — stock continued moving ({dp.move_5d_pct:+.1f}%), should have exited"
                )


def _exit_was_correct(strategy: str, move_5d_pct: float) -> bool:
    """EXIT was correct if the stock moved further against the position."""
    if strategy in _BULLISH_STRATEGIES:
        return move_5d_pct < -1.0   # stock dropped → bullish position loses
    elif strategy in _BEARISH_STRATEGIES:
        return move_5d_pct > 1.0    # stock rose → bearish position loses
    elif strategy in _SHORT_PUT_STRATEGIES:
        return move_5d_pct < -1.0   # stock dropped → CSP approaches strike
    return abs(move_5d_pct) > 2.0   # fallback: any big move = signal had content


def _hold_was_correct(strategy: str, move_5d_pct: float) -> bool:
    """HOLD was correct if the stock moved favorably for the position."""
    if strategy in _BULLISH_STRATEGIES:
        return move_5d_pct > -1.0   # stock didn't drop much
    elif strategy in _BEARISH_STRATEGIES:
        return move_5d_pct < 1.0    # stock didn't rise much
    elif strategy in _SHORT_PUT_STRATEGIES:
        return move_5d_pct > -1.0   # stock didn't drop much
    return abs(move_5d_pct) < 3.0


# ═══════════════════════════════════════════════════════════════════════
# Signal Accuracy + Counterfactuals
# ═══════════════════════════════════════════════════════════════════════

def _compute_signal_accuracy(replay: TradeReplay) -> None:
    """Compute per-trade accuracy counts and counterfactuals."""
    for dp in replay.decisions:
        if dp.signal_correct is None:
            continue

        if dp.action in ("EXIT", "REVIEW"):
            replay.exit_signals_total += 1
            if dp.signal_correct:
                replay.exit_signals_correct += 1
        elif dp.action in ("HOLD", "HOLD_FOR_REVERSION"):
            replay.hold_signals_total += 1
            if dp.signal_correct:
                replay.hold_signals_correct += 1

    # First EXIT signal
    exit_decisions = [d for d in replay.decisions if d.action == "EXIT"]
    if exit_decisions:
        first = exit_decisions[0]
        replay.first_exit_date = first.run_date
        replay.first_exit_pnl = first.pnl_at_signal

        # Delay cost: how much worse off are you for waiting?
        actual_pnl = replay.exit_pnl_dollar
        if actual_pnl is None and replay.decisions:
            actual_pnl = replay.decisions[-1].pnl_at_signal

        if first.pnl_at_signal is not None and actual_pnl is not None:
            # Positive = lost money by waiting, negative = saved money by waiting
            replay.delay_cost = first.pnl_at_signal - actual_pnl

    # Best and worst points
    pnl_points = [(d.run_date, d.pnl_at_signal) for d in replay.decisions
                  if d.pnl_at_signal is not None]
    if pnl_points:
        best = max(pnl_points, key=lambda x: x[1])
        worst = min(pnl_points, key=lambda x: x[1])
        replay.best_exit_date, replay.best_exit_pnl = best
        replay.worst_point_date, replay.worst_point_pnl = worst


# ═══════════════════════════════════════════════════════════════════════
# Aggregate Metrics
# ═══════════════════════════════════════════════════════════════════════

def _aggregate_metrics(replays: List[TradeReplay]) -> SignalAccuracyMetrics:
    """Compute aggregate accuracy across all replayed trades."""
    m = SignalAccuracyMetrics()

    strategy_data: Dict[str, dict] = defaultdict(
        lambda: {"exit_correct": 0, "exit_total": 0, "hold_correct": 0,
                 "hold_total": 0, "delay_costs": [], "n": 0}
    )
    urgency_data: Dict[str, dict] = defaultdict(
        lambda: {"count": 0, "moves_5d": []}
    )

    for r in replays:
        m.exit_total += r.exit_signals_total
        m.exit_correct += r.exit_signals_correct
        m.hold_total += r.hold_signals_total
        m.hold_correct += r.hold_signals_correct

        sd = strategy_data[r.strategy]
        sd["exit_correct"] += r.exit_signals_correct
        sd["exit_total"] += r.exit_signals_total
        sd["hold_correct"] += r.hold_signals_correct
        sd["hold_total"] += r.hold_signals_total
        sd["n"] += 1
        if r.delay_cost is not None:
            sd["delay_costs"].append(r.delay_cost)

        if r.first_exit_date is not None:
            m.trades_with_exit_signal += 1
        if r.delay_cost is not None:
            m.total_delay_cost_dollars += r.delay_cost

        # Urgency buckets per decision point
        for dp in r.decisions:
            if dp.move_5d_pct is not None:
                urg = dp.urgency or "LOW"
                urgency_data[urg]["count"] += 1
                urgency_data[urg]["moves_5d"].append(abs(dp.move_5d_pct))

    # Percentages
    if m.exit_total > 0:
        m.exit_accuracy_pct = m.exit_correct / m.exit_total * 100.0
    if m.hold_total > 0:
        m.hold_accuracy_pct = m.hold_correct / m.hold_total * 100.0
    if m.trades_with_exit_signal > 0:
        m.avg_delay_cost_dollars = m.total_delay_cost_dollars / m.trades_with_exit_signal

    # Strategy buckets
    for strat, sd in strategy_data.items():
        exit_acc = (sd["exit_correct"] / sd["exit_total"] * 100) if sd["exit_total"] > 0 else 0
        hold_acc = (sd["hold_correct"] / sd["hold_total"] * 100) if sd["hold_total"] > 0 else 0
        avg_delay = sum(sd["delay_costs"]) / len(sd["delay_costs"]) if sd["delay_costs"] else 0
        m.strategy_buckets[strat] = {
            "exit_acc": round(exit_acc, 1),
            "hold_acc": round(hold_acc, 1),
            "avg_delay": round(avg_delay, 0),
            "n": sd["n"],
        }

    # Urgency buckets
    for urg, ud in urgency_data.items():
        avg_move = sum(ud["moves_5d"]) / len(ud["moves_5d"]) if ud["moves_5d"] else 0
        m.urgency_buckets[urg] = {
            "count": ud["count"],
            "avg_abs_move_5d": round(avg_move, 2),
        }

    # Trust score
    exit_acc_norm = m.exit_accuracy_pct / 100.0 if m.exit_total > 0 else 0.5
    hold_acc_norm = m.hold_accuracy_pct / 100.0 if m.hold_total > 0 else 0.5

    # Urgency calibration: monotonic if CRITICAL > HIGH > MEDIUM > LOW in avg |move|
    urg_order = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    urg_moves = [m.urgency_buckets.get(u, {}).get("avg_abs_move_5d", 0) for u in urg_order]
    monotonic_pairs = sum(1 for i in range(len(urg_moves) - 1) if urg_moves[i] <= urg_moves[i + 1])
    urg_calibration = monotonic_pairs / max(len(urg_moves) - 1, 1)

    # Delay cost normalized (cap at 500 as "max bad")
    delay_norm = min(abs(m.avg_delay_cost_dollars) / 500.0, 1.0) if m.avg_delay_cost_dollars > 0 else 0

    m.signal_trust_score = round(
        (0.40 * exit_acc_norm + 0.30 * hold_acc_norm
         + 0.15 * urg_calibration + 0.15 * (1.0 - delay_norm)) * 100, 1
    )

    return m


# ═══════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════

def _safe_float(val) -> Optional[float]:
    """Convert to float or return None."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None
