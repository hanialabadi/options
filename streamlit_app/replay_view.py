"""
Trade Replay View — Streamlit dashboard tab for hindsight signal analysis.

Renders aggregate signal accuracy metrics + per-trade annotated timeline.
"""
from __future__ import annotations

import streamlit as st
import pandas as pd

from core.shared.data_layer.duckdb_utils import get_domain_connection, DbDomain
from core.management.cycle3.feedback.replay_engine import replay_all_trades
from core.management.cycle3.feedback.replay_types import (
    SignalAccuracyMetrics,
    TradeReplay,
)


def render_replay_view(db_path: str | None = None) -> None:
    """Main entry point — renders the Replay tab content.

    The db_path parameter is accepted for backward compatibility but ignored;
    connections are routed through the domain connection layer.
    """
    st.subheader("Trade Replay Engine")
    st.caption("Hindsight analysis: was the engine right?")

    try:
        con = get_domain_connection(DbDomain.PIPELINE, read_only=True)
        replays, metrics = replay_all_trades(con, include_open=True)
        con.close()
    except Exception as e:
        st.error(f"Failed to load replay data: {e}")
        return

    if not replays:
        st.info("No trade history available. Run the management engine first.")
        return

    # ── Section A: Aggregate Panel ──
    _render_aggregate_panel(metrics, len(replays))

    st.divider()

    # ── Section B: Per-Trade Replay ──
    trade_options = {
        f"{'CLOSED' if r.is_closed else 'OPEN':>6s} | {r.ticker:<6s} | {r.strategy:<15s} | {r.trade_id}": r
        for r in replays
    }
    selected_key = st.selectbox("Select Trade", list(trade_options.keys()))
    if selected_key:
        _render_trade_replay(trade_options[selected_key])


def _render_aggregate_panel(metrics: SignalAccuracyMetrics, trade_count: int) -> None:
    """Top-level signal accuracy dashboard."""
    st.markdown(f"**{trade_count} trades replayed**")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Trust Score", f"{metrics.signal_trust_score:.0f}/100")
    col2.metric("EXIT Accuracy", f"{metrics.exit_accuracy_pct:.0f}%",
                help=f"{metrics.exit_correct}/{metrics.exit_total} correct")
    col3.metric("HOLD Accuracy", f"{metrics.hold_accuracy_pct:.0f}%",
                help=f"{metrics.hold_correct}/{metrics.hold_total} correct")
    col4.metric("Avg Delay Cost", f"${metrics.avg_delay_cost_dollars:+,.0f}",
                help="Avg $ impact of ignoring first EXIT signal")

    # Strategy breakdown
    if metrics.strategy_buckets:
        with st.expander("Strategy Breakdown", expanded=False):
            rows = []
            for strat, data in sorted(metrics.strategy_buckets.items()):
                rows.append({
                    "Strategy": strat,
                    "Trades": data["n"],
                    "EXIT Acc": f"{data['exit_acc']:.0f}%",
                    "HOLD Acc": f"{data['hold_acc']:.0f}%",
                    "Avg Delay $": f"${data['avg_delay']:+,.0f}",
                })
            st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")

    # Urgency calibration
    if metrics.urgency_buckets:
        with st.expander("Urgency Calibration", expanded=False):
            st.caption("Do higher urgency signals precede bigger price moves?")
            rows = []
            for urg in ["LOW", "MEDIUM", "HIGH", "CRITICAL"]:
                if urg in metrics.urgency_buckets:
                    ud = metrics.urgency_buckets[urg]
                    rows.append({
                        "Urgency": urg,
                        "Signals": ud["count"],
                        "Avg |Move| 5D": f"{ud['avg_abs_move_5d']:.1f}%",
                    })
            if rows:
                st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")


def _render_trade_replay(replay: TradeReplay) -> None:
    """Per-trade replay detail."""
    status = "CLOSED" if replay.is_closed else "OPEN"
    st.markdown(f"### {replay.ticker} | {replay.strategy} | {status}")

    # Entry/Exit metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Entry UL", f"${replay.entry_ul_price:.2f}" if replay.entry_ul_price else "N/A")
    col2.metric("Exit UL", f"${replay.exit_ul_price:.2f}" if replay.exit_ul_price else "Current")
    if replay.exit_pnl_dollar is not None:
        col3.metric("P&L", f"${replay.exit_pnl_dollar:+,.0f}")
    elif replay.decisions and replay.decisions[-1].pnl_at_signal is not None:
        col3.metric("P&L (current)", f"${replay.decisions[-1].pnl_at_signal:+,.0f}")
    else:
        col3.metric("P&L", "N/A")
    if replay.outcome_type:
        col4.metric("Outcome", replay.outcome_type)
    else:
        col4.metric("Days", f"{len(replay.decisions)}")

    # Counterfactual cards
    if replay.first_exit_date or replay.best_exit_pnl is not None:
        st.markdown("**Counterfactual Analysis**")
        cf1, cf2, cf3 = st.columns(3)

        if replay.first_exit_date:
            pnl_str = f"${replay.first_exit_pnl:+,.0f}" if replay.first_exit_pnl is not None else "N/A"
            delta_str = None
            if replay.delay_cost is not None:
                if replay.delay_cost > 0:
                    delta_str = f"Saved ${replay.delay_cost:,.0f} by waiting"
                else:
                    delta_str = f"Lost ${abs(replay.delay_cost):,.0f} by waiting"
            cf1.metric(
                f"First EXIT ({replay.first_exit_date.strftime('%b %d')})",
                pnl_str,
                delta=delta_str,
            )
        if replay.best_exit_pnl is not None:
            cf2.metric(
                f"Best Point ({replay.best_exit_date.strftime('%b %d')})",
                f"${replay.best_exit_pnl:+,.0f}",
            )
        if replay.worst_point_pnl is not None:
            cf3.metric(
                f"Worst Point ({replay.worst_point_date.strftime('%b %d')})",
                f"${replay.worst_point_pnl:+,.0f}",
            )

    # Per-trade accuracy
    exit_acc = (
        f"{replay.exit_signals_correct}/{replay.exit_signals_total} "
        f"({replay.exit_signals_correct / replay.exit_signals_total * 100:.0f}%)"
        if replay.exit_signals_total > 0 else "N/A"
    )
    hold_acc = (
        f"{replay.hold_signals_correct}/{replay.hold_signals_total} "
        f"({replay.hold_signals_correct / replay.hold_signals_total * 100:.0f}%)"
        if replay.hold_signals_total > 0 else "N/A"
    )
    st.caption(f"Signal accuracy — EXIT: {exit_acc} | HOLD: {hold_acc}")

    # Annotated timeline table
    if replay.decisions:
        st.markdown("**Annotated Decision Timeline**")
        rows = []
        prev_action = ""
        for dp in replay.decisions:
            changed = "CHANGED" if dp.action != prev_action and prev_action else ""
            prev_action = dp.action

            correct_str = ""
            if dp.signal_correct is True:
                correct_str = "OK"
            elif dp.signal_correct is False:
                correct_str = "X"

            rows.append({
                "Date": dp.run_date.strftime("%b %d"),
                "Action": dp.action,
                "Urgency": dp.urgency,
                "UL": f"${dp.ul_price:.2f}" if dp.ul_price else "",
                "P&L": f"${dp.pnl_at_signal:+,.0f}" if dp.pnl_at_signal is not None else "",
                "5D Move": f"{dp.move_5d_pct:+.1f}%" if dp.move_5d_pct is not None else "...",
                "?": correct_str,
                "": changed,
                "Hindsight": dp.hindsight_note,
            })
        st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch", height=400)

    # Price chart with decision overlay
    if replay.price_series and replay.decisions:
        _render_price_chart(replay)


def _render_price_chart(replay: TradeReplay) -> None:
    """Price line chart with EXIT signal markers."""
    prices_df = pd.DataFrame(replay.price_series)
    if prices_df.empty or "date" not in prices_df.columns:
        return

    prices_df["date"] = pd.to_datetime(prices_df["date"])
    prices_df = prices_df.set_index("date")[["close"]].rename(columns={"close": "Price"})

    # Filter to trade period
    if replay.decisions:
        start = pd.to_datetime(replay.decisions[0].run_date) - pd.Timedelta(days=3)
        end = pd.to_datetime(replay.decisions[-1].run_date) + pd.Timedelta(days=7)
        prices_df = prices_df.loc[start:end]

    if not prices_df.empty:
        with st.expander("Price Chart", expanded=False):
            st.line_chart(prices_df, height=250)
