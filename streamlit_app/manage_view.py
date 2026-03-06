"""
manage_view.py — Position Monitor with Doctrine Engine

Cycles 1+2+3:
  - When doctrine output exists (positions_latest.csv), shows full recommendations.
  - Falls back to Cycle 1 DuckDB data when doctrine hasn't been run.

Tabs:
  A. Doctrine Recommendations  — EXIT/ROLL/HOLD with urgency, rationale, source
  B. Positions                 — per-trade cards with Greeks and P/L
  C. Expiration Calendar       — sorted by urgency
  D. Portfolio Greeks          — net exposure by ticker + portfolio totals
  E. Raw Data                  — verbatim output
"""

import sys
import streamlit as st
import pandas as pd
import numpy as np
import subprocess
import logging
from datetime import datetime, date
from pathlib import Path

logger = logging.getLogger(__name__)

DOCTRINE_PATH = Path("core/management/outputs/positions_latest.csv")

REQUIRED_DOCTRINE_COLS = [
    "TradeID", "Underlying_Ticker", "Strategy",
    "Decision_State", "Action", "Urgency", "Rationale", "Doctrine_Source",
    "GreekDominance_State", "VolatilityState_State", "AssignmentRisk_State",
    "RegimeStability_State", "Structural_Data_Complete",
]

# ─────────────────────────────────────────────────────────────────────────────
# Data loading
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=30)
def _load_doctrine() -> tuple[pd.DataFrame, bool]:
    """Load doctrine output if available. Returns (df, has_doctrine)."""
    if not DOCTRINE_PATH.exists():
        return pd.DataFrame(), False
    try:
        df = pd.read_csv(DOCTRINE_PATH)
        missing = [c for c in REQUIRED_DOCTRINE_COLS if c not in df.columns]
        if missing:
            logger.warning(f"Doctrine file missing columns: {missing}")
            return pd.DataFrame(), False
        df["Snapshot_TS"] = pd.to_datetime(df["Snapshot_TS"], errors="coerce")
        return df, True
    except Exception as e:
        logger.error(f"Failed to load doctrine output: {e}")
        return pd.DataFrame(), False


def _duckdb_connect_read_only(db_path_str: str):
    """
    Opens a DuckDB connection in read-only mode.
    If a stale write-lock is detected (crashed writer, PID dead), attempts a brief
    write-mode open to force WAL recovery/checkpoint, then retries read-only.
    Returns an open connection or raises.
    """
    import duckdb
    try:
        return duckdb.connect(db_path_str, read_only=True)
    except Exception as e:
        if "Conflicting lock" in str(e) or "lock" in str(e).lower():
            logger.warning(
                f"[DuckDB] Stale write-lock detected — attempting WAL recovery: {e}"
            )
            try:
                # Open in write mode to trigger automatic WAL recovery + checkpoint,
                # then close immediately. This clears the stale lock from a dead process.
                _recovery_con = duckdb.connect(db_path_str, read_only=False)
                _recovery_con.execute("CHECKPOINT")
                _recovery_con.close()
                logger.info("[DuckDB] WAL recovery successful — retrying read-only open.")
                return duckdb.connect(db_path_str, read_only=True)
            except Exception as recover_err:
                logger.error(f"[DuckDB] WAL recovery failed: {recover_err}")
                raise
        raise


@st.cache_data(ttl=30)
def _load_positions_from_duckdb(db_path_str: str) -> pd.DataFrame:
    """Load latest snapshot from DuckDB. READ-ONLY. Cycle 1 fallback."""
    try:
        with _duckdb_connect_read_only(db_path_str) as con:
            tables = con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).df()["table_name"].tolist()
            for t in ["enriched_legs_v1", "clean_legs_v2", "clean_legs"]:
                if t in tables:
                    run_id = con.execute(
                        f"SELECT run_id FROM {t} ORDER BY Snapshot_TS DESC LIMIT 1"
                    ).fetchone()
                    if run_id:
                        return con.execute(
                            f"SELECT * FROM {t} WHERE run_id = ?", [run_id[0]]
                        ).df()
    except Exception as e:
        logger.error(f"Failed to load positions: {e}")
    return pd.DataFrame()


@st.cache_data(ttl=300)
def _load_roll_candidates_from_db(db_path_str: str) -> dict:
    """
    Returns {trade_id: {Roll_Candidate_1: str, Roll_Candidate_2: str, Roll_Candidate_3: str}}
    from the most recent OPTION leg row with non-null Roll_Candidate_1 per TradeID.
    Cached 5 min — avoids DB hit on every card render.
    """
    result = {}
    try:
        with _duckdb_connect_read_only(db_path_str) as _con:
            df = _con.execute("""
                SELECT TradeID, Roll_Candidate_1, Roll_Candidate_2, Roll_Candidate_3
                FROM management_recommendations
                WHERE AssetType = 'OPTION'
                  AND Roll_Candidate_1 IS NOT NULL
                ORDER BY Snapshot_TS DESC
            """).df()
        if not df.empty:
            for _, row in df.drop_duplicates("TradeID").iterrows():
                result[row["TradeID"]] = {
                    "Roll_Candidate_1": row.get("Roll_Candidate_1"),
                    "Roll_Candidate_2": row.get("Roll_Candidate_2"),
                    "Roll_Candidate_3": row.get("Roll_Candidate_3"),
                }
    except Exception:
        pass
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Live intraday refresh helpers
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
def _fetch_live_intraday(ticker: str, _nonce: int = 0) -> dict:
    """
    Fetch live quote + today's 5-min bars + option spread quotes from Schwab.
    Cached 60s per (ticker, nonce).  Passing a new nonce forces a fresh fetch
    without relying on cache.clear() internals (ChatGPT fix #1).

    Returns {
        "quote":      {...},   # underlying quote fields
        "bars":       [...],   # 5-min candles for today
        "opt_quotes": {...},   # {symbol: {bid, ask, mid, spread_pct}} for option legs
        "error":      str|None
    }
    """
    import time as _t
    result = {"quote": {}, "bars": [], "opt_quotes": {}, "error": None}
    try:
        from scan_engine.loaders.schwab_api_client import SchwabClient
        client = SchwabClient()
        # Live underlying quote
        q_data = client.get_quotes([ticker], fields="quote")
        result["quote"] = (q_data.get(ticker, {}) or {}).get("quote", {})

        # 5-min bars for today
        import datetime as _dt
        _today = _dt.date.today()
        _start_ms = int(_t.mktime(_dt.datetime(_today.year, _today.month, _today.day, 9, 25).timetuple()) * 1000)
        _end_ms   = int(_t.time() * 1000)
        ph = client.get_price_history(
            ticker,
            frequencyType="minute",
            frequency=5,
            startDate=_start_ms,
            endDate=_end_ms,
        )
        result["bars"] = ph.get("candles", [])
    except Exception as e:
        result["error"] = str(e)
    return result


def _fetch_option_spreads(option_symbols: list[str]) -> dict:
    """
    Fetch live bid/ask/mid/spread_pct for a list of OCC option symbols.
    Called separately so option quotes don't block the underlying fetch.
    Returns {symbol: {"bid": f, "ask": f, "mid": f, "spread_pct": f}} or {} on error.
    """
    if not option_symbols:
        return {}
    result = {}
    try:
        from scan_engine.loaders.schwab_api_client import SchwabClient
        client = SchwabClient()
        raw = client.get_quotes(option_symbols, fields="quote")
        for sym in option_symbols:
            q = (raw.get(sym, {}) or {}).get("quote", {})
            bid = float(q.get("bidPrice") or q.get("bid") or 0)
            ask = float(q.get("askPrice") or q.get("ask") or 0)
            mid = (bid + ask) / 2 if (bid + ask) > 0 else 0
            sp  = (ask - bid) / mid * 100 if mid > 0 else None
            result[sym] = {"bid": bid, "ask": ask, "mid": mid, "spread_pct": sp}
    except Exception:
        pass
    return result


def _compute_live_signals(quote: dict, bars: list) -> dict:
    """
    Recompute intraday_position_tag, momentum_tag, gap_tag, VWAP, RSI-14
    from live quote + 5-min bars.
    Returns signal dict matching the pipeline column names plus:
      gap_resolved: bool  — True when price has crossed back through prev close
                            (ChatGPT fix #3: explicit "gap unresolved" definition)
    """
    signals = {
        "intraday_position_tag": "MID_RANGE",
        "momentum_tag":          "NORMAL",
        "gap_tag":               "NO_GAP",
        "gap_pct":               0.0,
        "gap_resolved":          True,   # no gap = resolved by default
        "vwap":                  None,
        "rsi_14":                None,
        "last_price":            None,
        "day_high":              None,
        "day_low":               None,
        "net_chg_pct":           None,
        "prev_close":            None,
    }
    try:
        last  = float(quote.get("lastPrice") or quote.get("mark") or 0)
        high  = float(quote.get("highPrice") or 0)
        low   = float(quote.get("lowPrice")  or 0)
        prev  = float(quote.get("closePrice") or 0)
        net_p = float(quote.get("netPercentChangeInDouble") or quote.get("netChange", 0))

        signals["last_price"]  = last
        signals["day_high"]    = high
        signals["day_low"]     = low
        signals["net_chg_pct"] = net_p
        signals["prev_close"]  = prev if prev else None

        # Intraday position tag
        if high > low and last:
            pos_pct = (last - low) / (high - low) * 100
            if pos_pct < 30:
                signals["intraday_position_tag"] = "NEAR_LOW"
            elif pos_pct > 70:
                signals["intraday_position_tag"] = "NEAR_HIGH"

        # Momentum tag from net % change
        if abs(net_p) >= 2.0:
            signals["momentum_tag"] = "STRONG_UP_DAY" if net_p > 0 else "STRONG_DOWN_DAY"
        elif abs(net_p) < 0.5:
            signals["momentum_tag"] = "FLAT_DAY"

        # Gap tag + gap_resolved
        # gap_resolved = True when:
        #   (a) no meaningful gap (<0.5%), OR
        #   (b) GAP_UP: current price has retraced below prev_close (filled upward gap), OR
        #   (c) GAP_DOWN: current price has recovered above prev_close (filled downward gap), OR
        #   (d) price has settled within 0.5% of VWAP for the last 3 bars (stabilized in gap range)
        if bars and prev:
            open_p = float(bars[0].get("open") or 0)
            if open_p and prev:
                g = (open_p - prev) / prev * 100
                signals["gap_pct"] = g
                if g >= 2.0:
                    signals["gap_tag"] = "GAP_UP"
                elif g <= -2.0:
                    signals["gap_tag"] = "GAP_DOWN"

        if signals["gap_tag"] != "NO_GAP" and prev and last:
            gap_d = signals["gap_pct"]
            # Price crossed back through prev close = gap filled
            price_crossed_prev = (
                (gap_d > 0 and last <= prev) or   # gap up, now at/below prev close
                (gap_d < 0 and last >= prev)       # gap down, now at/above prev close
            )
            # Last 3 bars stable within ±0.5% band — price digesting gap, not chasing
            bars_stable = False
            if len(bars) >= 3:
                recent_closes = [float(b["close"]) for b in bars[-3:]]
                bar_range_pct = (max(recent_closes) - min(recent_closes)) / min(recent_closes) * 100
                bars_stable = bar_range_pct < 0.5
            signals["gap_resolved"] = price_crossed_prev or bars_stable
        else:
            signals["gap_resolved"] = True

        # VWAP from 5-min bars (session VWAP, not tick VWAP — noted in docstring)
        if bars:
            import numpy as _np
            tp  = _np.array([(b["high"] + b["low"] + b["close"]) / 3 for b in bars])
            vol = _np.array([b["volume"] for b in bars], dtype=float)
            vol_sum = vol.sum()
            if vol_sum > 0:
                signals["vwap"] = float(_np.sum(tp * vol) / vol_sum)

        # RSI-14 from bar closes
        if len(bars) >= 15:
            import numpy as _np
            closes = _np.array([b["close"] for b in bars])
            deltas = _np.diff(closes)
            gains  = _np.where(deltas > 0, deltas, 0.0)
            losses = _np.where(deltas < 0, -deltas, 0.0)
            avg_g  = gains[:14].mean()
            avg_l  = losses[:14].mean()
            for i in range(14, len(deltas)):
                avg_g = (avg_g * 13 + gains[i])  / 14
                avg_l = (avg_l * 13 + losses[i]) / 14
            rs = avg_g / avg_l if avg_l > 0 else 100.0
            signals["rsi_14"] = float(100 - 100 / (1 + rs))

    except Exception:
        pass
    return signals


def _best_roll_window(
    bars: list,
    signals: dict,
    opt_spreads: dict | None = None,
    days_to_earnings: int | None = None,
    roll_target_dte: int | None = None,
    earnings_date_str: str | None = None,
) -> dict:
    """
    Classify the current moment as FAVORABLE / WAIT / AVOID for rolling.
    Returns {"verdict": str, "label": str, "reasons": list[str], "score": int}

    Rules (Passarelli Ch.6: fill quality degrades in first 30m and last 30m):
      AVOID:     RSI extreme + strong momentum simultaneously
      WAIT:      gap unresolved, strong momentum alone, option spread wide, time traps
      FAVORABLE: mid-session calm, gap resolved/no-gap, near VWAP, tight option spread

    opt_spreads:       {symbol: {"spread_pct": float}} — live option bid/ask spreads
    days_to_earnings:  calendar days until next earnings event (None = unknown)
    roll_target_dte:   DTE of the roll target contract (to check if it lands inside earnings)
    earnings_date_str: human-readable earnings date for display (e.g. "May 04")
    """
    import datetime as _dt
    verdict  = "FAVORABLE"
    reasons  = []
    score    = 50   # start at neutral; gates subtract, positives add

    # Always use ET for market-hours logic — local time is wrong on PST machines
    try:
        import zoneinfo as _zi
        _et_now = _dt.datetime.now(_zi.ZoneInfo("America/New_York"))
    except ImportError:
        import time as _tm
        _utc_off = -5 if _tm.daylight == 0 else -4
        _et_now = _dt.datetime.utcnow() + _dt.timedelta(hours=_utc_off)
    now_hour = _et_now.hour
    now_min  = _et_now.minute
    minutes_since_open = max(0, (now_hour - 9) * 60 + now_min - 30)

    # ── Time traps ────────────────────────────────────────────────────────────
    if minutes_since_open < 20:
        verdict = "WAIT"
        score  -= 30
        reasons.append(f"market open {minutes_since_open}m ago — auction spreads wide")
    if now_hour >= 15 and now_min >= 30:
        if verdict == "FAVORABLE":
            verdict = "WAIT"
        score -= 20
        reasons.append("last 30 min — MOC flow widening spreads")

    mom          = signals.get("momentum_tag", "")
    gap          = signals.get("gap_tag", "NO_GAP")
    gap_resolved = signals.get("gap_resolved", True)
    rsi          = signals.get("rsi_14")
    vwap         = signals.get("vwap")
    last         = signals.get("last_price")

    # ── Gap state — explicit resolution check (ChatGPT fix #3) ───────────────
    if gap != "NO_GAP" and abs(signals.get("gap_pct", 0)) >= 1.5:
        if gap_resolved:
            # Gap existed but price has settled — note it but don't penalize
            score += 5
            reasons.append(f"gap {signals['gap_pct']:+.1f}% — resolved ✓ (price stabilized)")
        else:
            if verdict == "FAVORABLE":
                verdict = "WAIT"
            score -= 25
            reasons.append(
                f"gap {signals['gap_pct']:+.1f}% — unresolved "
                f"(price hasn't crossed prev close ${signals.get('prev_close', '?')} "
                f"and last 3 bars still ranging >0.5%)"
            )

    # ── Momentum ─────────────────────────────────────────────────────────────
    # "Spreads likely wide" is an estimate fired before live spread data is checked.
    # If opt_spreads are already available, suppress the estimate — the measured
    # spread check below (line ~437) will emit a factual reason instead.
    if "STRONG" in mom:
        if verdict == "FAVORABLE":
            verdict = "WAIT"
        score -= 20
        if not opt_spreads:
            # No live spread data — flag as an estimate
            reasons.append(f"strong momentum ({mom.replace('_', ' ').lower()}) — spreads likely wide (no live data yet)")
        else:
            # Live spread data available — omit the estimate; measured spread check below will surface actual values
            reasons.append(f"strong momentum ({mom.replace('_', ' ').lower()}) — check live spread below")
    elif mom == "FLAT_DAY":
        score += 10
        reasons.append("flat day — market maker spreads tighter")

    # ── RSI extreme — AVOID only when combined with strong momentum ───────────
    if rsi is not None:
        if rsi > 78:
            score -= 15
            if "STRONG" in mom:
                verdict = "AVOID"
                reasons.append(f"RSI {rsi:.0f} overbought + strong momentum — AVOID (fill at worst price)")
            else:
                if verdict == "FAVORABLE":
                    verdict = "WAIT"
                reasons.append(f"RSI {rsi:.0f} overbought — wait for mean reversion")
        elif rsi < 22:
            score -= 15
            if "STRONG" in mom:
                verdict = "AVOID"
                reasons.append(f"RSI {rsi:.0f} oversold + strong momentum — AVOID (fill at worst price)")
            else:
                if verdict == "FAVORABLE":
                    verdict = "WAIT"
                reasons.append(f"RSI {rsi:.0f} oversold — wait for stabilization")
        elif 40 <= rsi <= 60:
            score += 5
            reasons.append(f"RSI {rsi:.0f} neutral — no directional pressure on spreads")

    # ── VWAP proximity ───────────────────────────────────────────────────────
    if vwap and last:
        vwap_dist_pct = (last - vwap) / vwap * 100
        if abs(vwap_dist_pct) < 0.3:
            score += 15
            reasons.append(f"price within 0.3% of VWAP ${vwap:.2f} — tightest spread zone")
        elif abs(vwap_dist_pct) < 1.0:
            score += 5
            reasons.append(f"price {vwap_dist_pct:+.2f}% from VWAP — acceptable")
        elif abs(vwap_dist_pct) > 1.5:
            if verdict == "FAVORABLE":
                verdict = "WAIT"
            score -= 10
            reasons.append(f"price {vwap_dist_pct:+.1f}% from VWAP — extended, wait for reversion")

    # ── Live option spread check (ChatGPT fix #B) ─────────────────────────────
    # Rolling is an options execution problem. Wide option spreads override a
    # favorable underlying signal — you'll pay 8–15% just to enter the roll.
    # Threshold: >8% is WAIT_FOR_FILL (McMillan: "always use limit at mid or better").
    if opt_spreads:
        _wide_legs = []
        for sym, sq in opt_spreads.items():
            sp = sq.get("spread_pct")
            if sp is not None:
                if sp > 12.0:
                    score -= 25
                    _wide_legs.append(f"{sym} {sp:.1f}% (very wide)")
                    if verdict == "FAVORABLE":
                        verdict = "WAIT"
                elif sp > 8.0:
                    score -= 12
                    _wide_legs.append(f"{sym} {sp:.1f}% (wide)")
                    if verdict == "FAVORABLE":
                        verdict = "WAIT"
                elif sp <= 4.0:
                    score += 8
                    reasons.append(f"{sym} spread {sp:.1f}% — tight, good fill expected")
        if _wide_legs:
            reasons.append(f"option spread wide — {', '.join(_wide_legs)} — use patient limit at mid")

    # ── Earnings inside roll window ───────────────────────────────────────────
    # If earnings fall before the roll target expiry, the new position will carry
    # the IV event. This is a structural concern — not a timing hesitation.
    # Natenberg Ch.8: IV inflates 2–3 weeks pre-earnings then collapses after event.
    if days_to_earnings is not None and days_to_earnings >= 0:
        _earn_str = f" ({earnings_date_str})" if earnings_date_str else ""
        if roll_target_dte is not None and days_to_earnings < roll_target_dte:
            # Earnings land INSIDE the new contract window
            if days_to_earnings <= 7:
                if verdict not in ("AVOID",):
                    verdict = "WAIT"
                score -= 20
                reasons.append(
                    f"⚠️ Earnings in {days_to_earnings}d{_earn_str} — INSIDE roll target window ({roll_target_dte}d DTE): "
                    f"rolling now means holding through the IV event. "
                    f"Natenberg Ch.8: IV collapses after earnings — intrinsic may be preserved but extrinsic is at risk."
                )
            elif days_to_earnings <= 21:
                score -= 10
                reasons.append(
                    f"⚠️ Earnings in {days_to_earnings}d{_earn_str} — inside roll target window ({roll_target_dte}d DTE): "
                    f"new position carries the IV event. Plan: roll with a strike that profits even after IV crush, "
                    f"or choose a shorter-expiry target that expires before earnings."
                )
            else:
                reasons.append(
                    f"Earnings in {days_to_earnings}d{_earn_str} — within roll target window ({roll_target_dte}d DTE): "
                    f"IV will inflate approaching the date; roll target is priced with some event premium."
                )
        elif roll_target_dte is None and days_to_earnings <= 30:
            # Can't check DTE alignment but earnings are soon — flag it
            reasons.append(
                f"Earnings in {days_to_earnings}d{_earn_str} — verify roll target expiry clears the event date."
            )

    score = max(0, min(100, score))

    if not reasons:
        reasons.append("mid-session calm — conditions favorable for limit order")

    labels = {
        "FAVORABLE": "🟢 FAVORABLE WINDOW",
        "WAIT":      "🟡 WAIT — suboptimal conditions",
        "AVOID":     "🔴 AVOID — high slippage risk",
    }
    return {"verdict": verdict, "label": labels.get(verdict, verdict), "reasons": reasons, "score": score}


def _render_intraday_chart(bars: list, signals: dict, ticker: str):
    """
    Render a compact Altair area chart of today's 5-min price action
    with VWAP overlay and current price marker.
    """
    if not bars:
        st.caption("No intraday bars available.")
        return
    try:
        import altair as alt
        import pandas as _pd
        import datetime as _dt

        df = _pd.DataFrame(bars)
        # Schwab returns datetime as Unix ms UTC. Convert to PT for display.
        df["datetime"] = (
            _pd.to_datetime(df["datetime"], unit="ms", utc=True)
              .dt.tz_convert("America/Los_Angeles")
              .dt.tz_localize(None)   # strip tz so Altair renders as plain time strings
        )
        df = df.sort_values("datetime").reset_index(drop=True)

        vwap = signals.get("vwap")
        last = signals.get("last_price")

        base = alt.Chart(df).encode(
            x=alt.X("datetime:T", title=None, axis=alt.Axis(format="%H:%M", labelAngle=-30, title="PT")),
        )

        # Price area
        area = base.mark_area(opacity=0.3, color="#4c9be8").encode(
            y=alt.Y("low:Q",  scale=alt.Scale(zero=False), title="Price"),
            y2="high:Q",
        )
        # Close line
        line = base.mark_line(color="#4c9be8", strokeWidth=1.5).encode(
            y=alt.Y("close:Q", scale=alt.Scale(zero=False)),
        )

        layers = [area, line]

        # VWAP line
        if vwap:
            vwap_df = _pd.DataFrame({"datetime": df["datetime"], "vwap": vwap})
            vwap_line = alt.Chart(vwap_df).mark_line(
                color="#f0a500", strokeDash=[4, 2], strokeWidth=1.5
            ).encode(
                x="datetime:T",
                y=alt.Y("vwap:Q", scale=alt.Scale(zero=False)),
            )
            layers.append(vwap_line)

        chart = alt.layer(*layers).properties(height=120, width="container").configure_view(strokeWidth=0)
        st.altair_chart(chart, width='stretch')

        # Legend caption
        _vwap_str = f" · VWAP ${vwap:.2f}" if vwap else ""
        _last_str = f" · Last ${last:.2f}" if last else ""
        st.caption(f"5-min bars (PT){_last_str}{_vwap_str} · 🟡 VWAP")
    except Exception as e:
        st.caption(f"Chart unavailable: {e}")


def _compute_dte(expiration_series: pd.Series) -> pd.Series:
    today = pd.Timestamp(date.today())
    exp = pd.to_datetime(expiration_series, errors="coerce")
    return (exp - today).dt.days


def _format_pnl(val: float) -> str:
    return f"+${val:,.0f}" if val >= 0 else f"-${abs(val):,.0f}"

def _pnl_color(val: float) -> str:
    """Return colored markdown for a P&L value — red for losses, green for gains."""
    if val >= 0:
        return f":green[+${val:,.0f}]"
    else:
        return f":red[-${abs(val):,.0f}]"


def _best_gl_for_group(group: "pd.DataFrame") -> float:
    """
    Return the best available P&L dollar amount for a trade group.

    Priority: PnL_Total (recomputed from live Schwab prices during market hours)
    Fallback: $ Total G/L (broker CSV — may be stale)

    For OPTION rows with Greeks_Source='schwab_live', PnL_Total reflects live prices.
    For STOCK rows (and off-hours), $ Total G/L is all we have.
    """
    total = 0.0
    for _, row in group.iterrows():
        _src = str(row.get("Greeks_Source", "") or "")
        _pnl_live = pd.to_numeric(row.get("PnL_Total"), errors="coerce")
        _pnl_csv  = pd.to_numeric(row.get("$ Total G/L"), errors="coerce")
        if _src == "schwab_live" and pd.notna(_pnl_live):
            total += float(_pnl_live)
        elif pd.notna(_pnl_csv):
            total += float(_pnl_csv)
    return total


def _strategy_emoji(strategy: str) -> str:
    s = str(strategy).upper()
    if "BUY_WRITE" in s or "COVERED_CALL" in s:
        return "📝"
    if "CSP" in s or "CASH_SECURED" in s:
        return "🛡️"
    if "LONG_CALL" in s or "LEAPS_CALL" in s:
        return "📈"
    if "LONG_PUT" in s:
        return "📉"
    if "STOCK" in s:
        return "🏦"
    return "🔲"


def _dte_color(dte) -> str:
    if pd.isna(dte):
        return "gray"
    if dte <= 7:
        return "#ff4b4b"
    if dte <= 21:
        return "#ffa500"
    return "#09ab3b"


# ─────────────────────────────────────────────────────────────────────────────
# Section A — Doctrine Recommendations
# ─────────────────────────────────────────────────────────────────────────────

URGENCY_ORDER = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
ACTION_BADGE = {
    "EXIT":       ("🔴", "background-color:#3d1515"),
    "ROLL":       ("🟠", "background-color:#3d2e00"),
    "ROLL_WAIT":  ("⏳", "background-color:#2a2a00"),
    "TRIM":       ("🟡", "background-color:#2d2d00"),
    "HOLD":       ("🟢", ""),
    "SCALE_UP":   ("⬆️", "background-color:#0a2a0a"),
    "WAIT":       ("⏳", ""),
    "REVALIDATE": ("🔄", ""),
    "QUARANTINE": ("🚫", "background-color:#2d001a"),
    "HALT":               ("🛑", "background-color:#3d1515"),
    "AWAITING_SETTLEMENT":("⏳", "background-color:#1a1a2e"),
}
DECISION_BADGE = {
    "ACTIONABLE":          "🔴 ACTIONABLE",
    "NEUTRAL_CONFIDENT":   "🟢 NEUTRAL",
    "UNCERTAIN":           "⚪ UNCERTAIN",
    "BLOCKED_GOVERNANCE":  "🛑 HALTED",
}


def _render_doctrine_recommendations(df: pd.DataFrame):
    st.subheader("Doctrine Recommendations")
    # Show file freshness so users know if they're seeing stale data
    import os as _os
    _doc_mtime = DOCTRINE_PATH.stat().st_mtime if DOCTRINE_PATH.exists() else None
    if _doc_mtime:
        _doc_age_min = (_os.times().elapsed if False else
                        (datetime.now() - datetime.fromtimestamp(_doc_mtime)).total_seconds() / 60)
        _doc_ts_str  = datetime.fromtimestamp(_doc_mtime).strftime("%Y-%m-%d %H:%M")
        _age_label   = (f"{_doc_age_min:.0f}m ago"    if _doc_age_min < 120
                        else f"{_doc_age_min/60:.1f}h ago")
        _age_color   = "green" if _doc_age_min < 30 else ("orange" if _doc_age_min < 120 else "red")
        st.caption(
            f"Generated by DoctrineAuthority (McMillan / Passarelli / Natenberg / Hull). "
            f"Each decision reflects the combined state of drift, chart signals, and strategy doctrine.  "
            f"**Last run:** :{_age_color}[{_doc_ts_str} ({_age_label})]"
        )
    else:
        st.caption("Generated by DoctrineAuthority (McMillan / Passarelli / Natenberg / Hull). "
                   "Each decision reflects the combined state of drift, chart signals, and strategy doctrine.")

    # De-duplicate to trade-level.
    # For multi-leg strategies (BUY_WRITE, COVERED_CALL) the OPTION leg carries the
    # actionable decision (e.g. ROLL the call); the STOCK leg carries an advisory HOLD.
    # Prefer OPTION rows so the headline action reflects the call/put decision, not the
    # stock-level structural block which is already surfaced in the rationale text.
    trade_cols = [c for c in [
        "TradeID", "Underlying_Ticker", "Strategy", "AssetType",
        "Decision_State", "Action", "Urgency", "Rationale", "Doctrine_Source",
        "GreekDominance_State", "VolatilityState_State", "AssignmentRisk_State",
        "RegimeStability_State", "DTE", "Price_Drift_Pct", "PnL_Total",
        "Drift_Direction", "Lifecycle_Phase", "Structural_Data_Complete", "Resolution_Reason",
    ] if c in df.columns]
    _trades_all = df[trade_cols].copy()
    # Sort OPTION rows before STOCK rows so drop_duplicates keeps the option leg
    if "AssetType" in _trades_all.columns:
        _trades_all["_asset_rank"] = _trades_all["AssetType"].map(
            {"OPTION": 0, "STOCK": 1}
        ).fillna(2)
        _trades_all = _trades_all.sort_values("_asset_rank").drop(columns=["_asset_rank"])
    trades = _trades_all.drop_duplicates("TradeID").copy()

    # Filter STOCK_ONLY: show only those with elevated urgency (MEDIUM/HIGH/CRITICAL)
    # AND ≥100 shares.  Sub-contract stock (<100 shares) adds noise — no CC available,
    # limited actionability.  These remain visible only in the Idle tab.
    if "Strategy" in trades.columns:
        _so_mask = trades["Strategy"].str.upper() == "STOCK_ONLY"
        _so_qty = pd.to_numeric(trades["Quantity"], errors="coerce").fillna(0) if "Quantity" in trades.columns else pd.Series(0, index=trades.index)
        _so_actionable = _so_mask & trades["Urgency"].isin(["MEDIUM", "HIGH", "CRITICAL"]) & (_so_qty >= 100)
        trades = trades[~_so_mask | _so_actionable].copy()

    # Sort by urgency then action
    trades["_urgency_rank"] = trades["Urgency"].map(URGENCY_ORDER).fillna(99)
    trades = trades.sort_values(["_urgency_rank", "Action"]).drop(columns=["_urgency_rank"])

    # Headline metrics
    actionable = (trades["Decision_State"] == "ACTIONABLE").sum()
    uncertain = (trades["Decision_State"] == "UNCERTAIN").sum()
    exits = (trades["Action"] == "EXIT").sum()
    rolls = (trades["Action"] == "ROLL").sum()
    roll_waits = (trades["Action"] == "ROLL_WAIT").sum()
    crits = (trades["Urgency"] == "CRITICAL").sum()

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Actionable", int(actionable), delta="⚠️ Requires attention" if actionable else None,
              delta_color="inverse" if actionable else "off")
    c2.metric("EXIT Signals", int(exits), delta="EXIT" if exits else None,
              delta_color="inverse" if exits else "off")
    c3.metric("ROLL Signals", int(rolls), delta=f"+{roll_waits} waiting" if roll_waits else None,
              delta_color="off")
    c4.metric("CRITICAL Urgency", int(crits), delta="CRITICAL" if crits else None,
              delta_color="inverse" if crits else "off")
    c5.metric("Uncertain", int(uncertain))

    st.divider()

    # Render each trade as a doctrine card
    for _, row in trades.iterrows():
        action = str(row.get("Action", "HOLD"))
        urgency = str(row.get("Urgency", "LOW"))
        decision = str(row.get("Decision_State", ""))
        ticker = str(row.get("Underlying_Ticker", ""))
        strategy = str(row.get("Strategy", ""))
        rationale = str(row.get("Rationale", ""))
        doctrine_source = str(row.get("Doctrine_Source", ""))
        trade_id = str(row.get("TradeID", ""))

        emoji, bg = ACTION_BADGE.get(action, ("🔲", ""))
        decision_label = DECISION_BADGE.get(decision, decision)

        drift_pct = row.get("Price_Drift_Pct")
        dte = row.get("DTE")
        pnl = row.get("PnL_Total")
        lifecycle = row.get("Lifecycle_Phase", "")
        drift_dir = row.get("Drift_Direction", "")
        greek_dom = row.get("GreekDominance_State", "")
        vol_state = row.get("VolatilityState_State", "")
        assign_risk = row.get("AssignmentRisk_State", "")

        # Card header
        urgency_marker = "🔴 " if urgency == "CRITICAL" else ("🟠 " if urgency == "HIGH" else "")
        header = (
            f"{urgency_marker}{emoji} **{ticker}** — {strategy}   "
            f"`{action}` · {decision_label}"
        )

        # Auto-expand actionable / critical cards.
        # Exception: HOLD cards with HIGH urgency from MC escalation (not doctrine) stay collapsed —
        # the MC panel inside is already auto-expanded when EXIT_NOW; collapsing the card avoids
        # noise from NEUTRAL HOLDs flooding the view as if they were structural emergencies.
        _mc_hold_v = str(row.get("MC_Hold_Verdict", "") or "")
        _urgency_from_mc_only = (
            action == "HOLD"
            and urgency == "HIGH"
            and decision != "ACTIONABLE"
            and _mc_hold_v not in ("", "SKIP", "HOLD_JUSTIFIED")
        )
        auto_expand = (decision == "ACTIONABLE" or urgency == "CRITICAL") and not _urgency_from_mc_only

        with st.expander(header, expanded=auto_expand):
            # Context strip
            ctx_parts = []
            if pd.notna(dte):
                ctx_parts.append(f"DTE: `{int(dte)}d`")
            if pd.notna(drift_pct):
                drift_color = ":red[" if drift_pct < 0 else ":green["
                ctx_parts.append(f"Drift: {drift_color}`{drift_pct:+.2%}`]")
            if pd.notna(pnl):
                ctx_parts.append(f"P&L: {_pnl_color(float(pnl))}")
            if lifecycle:
                ctx_parts.append(f"Phase: `{lifecycle}`")
            if drift_dir:
                ctx_parts.append(f"Dir: `{drift_dir}`")
            if ctx_parts:
                st.caption("  ·  ".join(ctx_parts))

            # ── MC urgency escalation notice ──────────────────────────────────
            # When MC simulation overrides a HOLD's urgency to HIGH, the orange 🟠
            # marker can look alarming on an otherwise NEUTRAL card. Clarify the source.
            if _urgency_from_mc_only:
                st.caption(
                    f"🟠 Urgency elevated by MC simulation ({_mc_hold_v}) — "
                    "see MC Exit vs Hold panel below for probability detail."
                )

            # ── UNCERTAIN card: human-readable explanation ────────────────────
            if decision == "UNCERTAIN":
                _struct_complete = str(row.get("Structural_Data_Complete", "")).lower()
                _is_data_gap = _struct_complete in ("false", "0", "no", "")
                _resolution_reason = str(row.get("Resolution_Reason", "") or "")

                # Translate Resolution_Reason codes to user-facing explanation
                _reason_map = {
                    "MISSING_PRIMITIVES":         "Price structure primitives (swing counts, ATR range, break-of-structure) could not be computed. This usually means the price history fetch returned fewer than 50 bars.",
                    "INSUFFICIENT_HISTORY":       "Fewer than 50 bars of price history were available for this ticker. Doctrine cannot compute chart state with less than 50 bars.",
                    "NO_HISTORY_RETURNED":        "No price history was returned from the data source for this ticker.",
                    "DATA_FETCH_FAILED":          "Price history fetch failed (network or API error, or Schwab token expired). Check the Schwab status banner in the sidebar — if the token is expired, run `python auth_schwab_minimal.py`.",
                    "DATA_SOURCE_BACKOFF_ACTIVE": "Rate-limit backoff is active for the data source. Price history fetch was skipped this run.",
                    "ENGINE_ERROR":               "An internal error occurred while computing the chart state for this position.",
                }
                # Handle dynamic reasons like INSUFFICIENT_HISTORY_23
                _resolved_msg = None
                for _code, _msg in _reason_map.items():
                    if _resolution_reason.startswith(_code):
                        _resolved_msg = _msg
                        break
                _resolved_msg = _resolved_msg or "The price structure engine returned incomplete data for this position."

                if _is_data_gap or "missing" in rationale.lower() or "uncertainty guard" in doctrine_source.lower():
                    # Check if the doctrine file is stale — if so, UNCERTAIN may already be resolved
                    import os as _os_u
                    _unc_mtime = DOCTRINE_PATH.stat().st_mtime if DOCTRINE_PATH.exists() else None
                    _unc_age   = ((datetime.now() - datetime.fromtimestamp(_unc_mtime)).total_seconds() / 60
                                  if _unc_mtime else None)
                    _stale_note = (
                        f"  \n⚠️ *Note: this doctrine data is {_unc_age:.0f}m old — "
                        f"re-run the management engine (button below) to get the latest state.*"
                        if (_unc_age is not None and _unc_age > 30) else ""
                    )
                    st.info(
                        f"**Doctrine engine needs more data.**  \n"
                        f"**Missing:** {_resolved_msg}  \n"
                        f"No doctrine inference was made — this is not a negative signal. "
                        f"The system is waiting for complete structural data before making a recommendation.  \n"
                        f"**What to do:** Check the Positions tab for live signals. "
                        f"HOLD is appropriate until the next management run resolves this."
                        + _stale_note
                    )
                else:
                    # UNCERTAIN for another reason — show rationale with context
                    _rationale_safe = rationale.replace("$", "\\$")
                    st.markdown(f"> {_rationale_safe}")
                    st.caption(f"**Source:** {doctrine_source}")

                # Interpret state badges for UNCERTAIN cards (avoid raw enum dump)
                _greek_labels = {
                    "GAMMA_DOMINANT": "Gamma dominant — position is sensitive to near-term price moves",
                    "THETA_DOMINANT": "Theta dominant — time decay is the primary driver",
                    "DELTA_DOMINANT": "Delta dominant — directional exposure is dominant",
                    "VEGA_DOMINANT":  "Vega dominant — vol changes are the primary driver",
                    "BALANCED":       "Greeks balanced — no single driver dominates",
                }
                _vol_labels = {
                    "HIGH":        "Vol: High — elevated implied volatility",
                    "LOW":         "Vol: Low — muted implied volatility",
                    "NORMAL":      "Vol: Normal — vol in expected range",
                    "EXPANDING":   "Vol: Expanding — IV rising",
                    "CONTRACTING": "Vol: Contracting — IV falling",
                    "UNKNOWN":     "Vol: Unknown — insufficient IV history",
                }
                _risk_labels = {
                    "LOW":      "Assignment risk: Low",
                    "MODERATE": "Assignment risk: Moderate — monitor",
                    "HIGH":     "Assignment risk: High — review strikes",
                    "CRITICAL": "Assignment risk: Critical — act now",
                    "UNKNOWN":  "Assignment risk: Unknown",
                }
                _greek_display = _greek_labels.get(str(greek_dom or "").upper(), str(greek_dom or "—"))
                _vol_display   = _vol_labels.get(str(vol_state or "").upper(), str(vol_state or "—"))
                _risk_display  = _risk_labels.get(str(assign_risk or "").upper(), str(assign_risk or "—"))
                badge_cols = st.columns(3)
                badge_cols[0].caption(f"⚙️ {_greek_display}")
                badge_cols[1].caption(f"📊 {_vol_display}")
                badge_cols[2].caption(f"📌 {_risk_display}")

            else:
                # ── HOLD + MC EXIT_NOW conflict banner ──────────────────────────
                # MC EXIT_NOW appends a suffix to Rationale in run_all.py for urgency
                # escalation. Strip it from inline display (it has its own MC panel
                # below) and show a prominent conflict banner instead.
                _MC_EXIT_SUFFIX = " | ⚡ MC EXIT_NOW verdict: holding no longer viable — reassess immediately."
                _mc_hold_conflict = (
                    action == "HOLD"
                    and str(row.get("MC_Hold_Verdict", "") or "") == "EXIT_NOW"
                )
                if _mc_hold_conflict:
                    st.error(
                        "⚡ **MC simulation conflicts with HOLD doctrine** — "
                        "probability analysis shows holding is no longer viable. "
                        "See MC Exit vs Hold panel below.",
                        icon="🔴",
                    )
                # Strip the MC suffix from rationale before display — it's shown in the MC panel
                _rationale_display = rationale.replace(_MC_EXIT_SUFFIX, "").strip(" |").strip()

                # Rationale box — escape $ to prevent Streamlit LaTeX rendering
                # Journey note (📖) is split off and rendered as its own callout
                # so the continuous trade narrative stands apart from the current-run analysis.
                _rationale_safe = _rationale_display.replace("$", "\\$")
                if "\n" in _rationale_safe:
                    _journey_line, _body = _rationale_safe.split("\n", 1)
                    if _journey_line.startswith("📖"):
                        st.info(_journey_line)
                        _rationale_md = _body.strip()
                    else:
                        _rationale_md = _rationale_safe
                else:
                    _rationale_md = _rationale_safe
                st.markdown(f"> {_rationale_md}")
                st.caption(f"**Source:** {doctrine_source}")

                # State badges
                badge_cols = st.columns(4)
                badge_cols[0].caption(f"Greek Dom: **{greek_dom or '—'}**")
                badge_cols[1].caption(f"Vol State: **{vol_state or '—'}**")
                badge_cols[2].caption(f"Assign Risk: **{assign_risk or '—'}**")
                badge_cols[3].caption(f"Regime: **{row.get('RegimeStability_State') or '—'}**")

            # ── Action EV Comparator ──────────────────────────────────────────
            _ev_winner  = row.get("Action_EV_Winner")
            _ev_ranking = row.get("Action_EV_Ranking")
            _ev_margin  = row.get("Action_EV_Margin")
            _ev_buyback_trigger = row.get("EV_Buyback_Trigger")

            if _ev_winner and str(_ev_winner) not in ("nan", "None", ""):
                # Parse ranking list if stored as string (CSV serialization)
                if isinstance(_ev_ranking, str):
                    import ast as _ast
                    try:
                        _ev_ranking = _ast.literal_eval(_ev_ranking)
                    except Exception:
                        _ev_ranking = None

                _ev_h = row.get("Action_EV_Hold")
                _ev_r = row.get("Action_EV_Roll")
                _ev_a = row.get("Action_EV_Assign")
                _ev_b = row.get("Action_EV_Buyback")
                _ev_g = row.get("Gamma_Drag_Daily")

                def _ev_fmt(v) -> str:
                    try:
                        f = float(v)
                        return f"+\\${f:,.0f}" if f >= 0 else f"-\\${abs(f):,.0f}"
                    except Exception:
                        return "—"

                _ev_margin_f = float(_ev_margin) if _ev_margin not in (None, "", "nan") else None
                _EV_NOISE_FLOOR_DISP = 50.0
                _ev_is_tie   = _ev_margin_f is not None and _ev_margin_f < _EV_NOISE_FLOOR_DISP
                _ev_runner   = None  # second-place action when tied

                if _ev_is_tie and _ev_ranking:
                    # Show both tied actions in header
                    _ranking_list = _ev_ranking if isinstance(_ev_ranking, list) else []
                    _ev_runner = _ranking_list[1] if len(_ranking_list) > 1 else None
                    _header_winner = f"{_ev_winner}/{_ev_runner} (tie — ≤${_ev_margin_f:.0f})" if _ev_runner else f"{_ev_winner} (tie)"
                    _margin_str = ""
                else:
                    _margin_str = f" (margin: {_ev_fmt(_ev_margin_f)})" if _ev_margin_f is not None else ""
                    _header_winner = str(_ev_winner)

                _ev_color = {
                    "HOLD":    "#1a472a",
                    "ROLL":    "#1a3a5c",
                    "ASSIGN":  "#4a2c6b",
                    "BUYBACK": "#6b3a1a",
                }.get(str(_ev_winner), "")

                # Detect doctrine vs EV conflict: EV winner doesn't match doctrine action
                _doctrine_action = str(row.get("Action") or "").upper()
                _ev_winner_up    = str(_ev_winner or "").upper()
                # Map doctrine actions to EV categories (ROLL_WAIT → ROLL for comparison purposes)
                _doctrine_ev_cat = {
                    "ROLL": "ROLL", "ROLL_WAIT": "ROLL", "HOLD": "HOLD",
                    "EXIT": "ASSIGN", "ASSIGN": "ASSIGN", "BUYBACK": "BUYBACK",
                    "TRIM": "HOLD", "SCALE_UP": "HOLD", "HOLD_FOR_REVERSION": "HOLD",
                }.get(_doctrine_action, _doctrine_action)
                _ev_doctrine_conflict = (
                    not _ev_is_tie
                    and _ev_winner_up != _doctrine_ev_cat
                    and _ev_winner_up in ("HOLD", "ROLL", "ASSIGN", "BUYBACK")
                    and _doctrine_ev_cat in ("HOLD", "ROLL", "ASSIGN", "BUYBACK")
                )

                with st.expander(
                    f"📊 Action EV Comparator — **{_header_winner}** wins{_margin_str}",
                    expanded=bool(_ev_buyback_trigger and str(_ev_buyback_trigger) not in ("False", "nan", "None", ""))
                ):
                    _ev_rows = [
                        {"Action": "HOLD",    "EV (over DTE)": _ev_fmt(_ev_h), "Raw $": _ev_h},
                        {"Action": "ROLL",    "EV (over DTE)": _ev_fmt(_ev_r), "Raw $": _ev_r},
                        {"Action": "ASSIGN",  "EV (over DTE)": _ev_fmt(_ev_a), "Raw $": _ev_a},
                        {"Action": "BUYBACK", "EV (over DTE)": _ev_fmt(_ev_b), "Raw $": _ev_b},
                    ]
                    # Sort by raw value descending (winner at top)
                    try:
                        _ev_rows.sort(key=lambda x: float(x["Raw $"] or 0), reverse=True)
                    except Exception:
                        pass

                    # Rank medals
                    _medals = ["🥇", "🥈", "🥉", "4️⃣"]
                    for _ri, _er in enumerate(_ev_rows):
                        _medal = _medals[_ri] if _ri < len(_medals) else ""
                        _ev_val = _er["EV (over DTE)"]
                        _is_winner = _er["Action"] == str(_ev_winner)
                        _line = f"{_medal} **{_er['Action']}** — {_ev_val}"
                        if _is_winner:
                            st.markdown(f":{('green' if _er['Action'] == 'HOLD' else 'blue')}[{_line}]")
                        else:
                            st.markdown(_line)

                    # ── Tie note: margin below noise floor ──────────────────────
                    if _ev_is_tie:
                        _tie_runner_str = f" and {_ev_runner}" if _ev_runner else ""
                        st.info(
                            f"📐 **Statistical tie** — {_ev_winner}{_tie_runner_str} are within "
                            f"${_ev_margin_f:.0f} of each other (below $50 noise floor). "
                            f"Deterministic EV cannot distinguish these outcomes. "
                            f"**Doctrine gates take precedence** — follow the doctrine action above. "
                            f"(Passarelli Ch.6: when EV is ambiguous, structural/delta signals are the tiebreaker)",
                            icon="🔗",
                        )

                    # ── Doctrine / EV conflict note ──────────────────────────────
                    # When doctrine says ROLL (driven by delta/MC/assignment risk) but EV says
                    # a different action, explain WHY doctrine overrides the deterministic model.
                    # The EV comparator is a *mechanical* expected-value calculation that does NOT
                    # include: MC breach probability, delta-driven assignment urgency, or time-critical
                    # gates (dividend ex-date, BROKEN equity, pre-ITM window). Those are doctrine gates.
                    # This note prevents the trader from reading "EV says ASSIGN" and second-guessing
                    # a ROLL CRITICAL doctrine decision.
                    if _ev_doctrine_conflict:
                        _conflict_why = {
                            # EV says one thing, doctrine says another — explain the gap
                            ("ASSIGN", "ROLL"): (
                                "EV models assignment as a certain, flat outcome — it doesn't model the "
                                "**probability of being assigned early** (delta, DTE, MC breach risk). "
                                "Doctrine fires ROLL because structural signals (delta gate, MC, or "
                                "pre-ITM window) create time-critical urgency that the static EV model cannot capture. "
                                "**Follow ROLL** — the deterministic EV tie/margin is not a reason to defer. "
                                "(McMillan Ch.2: delta > 0.55 and MC breach > 66% override mechanical EV)"
                            ),
                            ("HOLD", "ROLL"): (
                                "EV favors holding the remaining theta — but doctrine fires ROLL because "
                                "a structural gate (delta, equity integrity, or MC breach probability) "
                                "makes 'wait for more theta' the incorrect action. "
                                "**Follow ROLL** — the EV model's theta-carry assumption breaks down "
                                "when assignment or structural risk is elevated. "
                                "(Natenberg Ch.7: when gamma drag + assignment risk > theta carry, HOLD EV is overstated)"
                            ),
                        }.get((str(_ev_winner).upper(), _doctrine_ev_cat), None)

                        _generic_conflict = (
                            f"EV says **{_ev_winner}** but doctrine says **{_doctrine_action}**. "
                            f"The EV comparator is a deterministic model — it doesn't include MC breach probability, "
                            f"delta momentum, dividend timing, or equity integrity signals. "
                            f"**Doctrine action takes precedence when structural gates fire.** "
                            f"(Passarelli Ch.6: quantitative gates override mechanical EV when risk signals are present)"
                        )

                        st.warning(
                            f"⚠️ **EV ↔ Doctrine conflict**: "
                            + (_conflict_why if _conflict_why else _generic_conflict),
                            icon="⚖️",
                        )

                    if _ev_g not in (None, "", "nan"):
                        try:
                            st.caption(f"Gamma drag: \\${float(_ev_g):.2f}/contract/day  ·  EV uses theta carry − carry cost − gamma drag. ROLL uses extrinsic as credit proxy (lower bound). BUYBACK = certain close cost only.")
                        except Exception:
                            pass

                    if _ev_buyback_trigger and str(_ev_buyback_trigger) not in ("False", "nan", "None", ""):
                        st.warning(
                            "⚡ **Buyback trigger active** — gamma drag exceeding theta carry in breakout. "
                            "IV low (cheap to close). Consider buying back the call to capture uncapped upside. "
                            "(McMillan Ch.3: gamma-dominant breakout)"
                        )

                    st.caption(
                        "EV is deterministic (no probability weighting). "
                        "ROLL proxy = extrinsic remaining − slippage − carry on 45d new cycle. "
                        "ASSIGN = certain (strike − basis). "
                        "BUYBACK = certain close cost, naked stock upside not modeled. "
                        "Passarelli Ch.6 / Natenberg Ch.7."
                    )

            # ── Arbitration Panel (long option positions only) ────────────────
            # Shows when the engine ran Gate Conflict Resolver on Gate 6
            # (thesis degradation). Surfaces the structured HOLD vs ROLL decision.
            _arb_gate     = row.get("Arbitration_Gate", "")
            _arb_override = row.get("Arbitration_Override", "")
            _arb_ev_win   = row.get("Arbitration_EV_Winner", "")
            _arb_ev_mar   = row.get("Arbitration_EV_Margin")
            _arb_vol_c    = row.get("Arbitration_Vol_Confidence")
            _arb_cap_i    = row.get("Arbitration_Capital_Impact")
            _arb_summ     = row.get("Arbitration_Summary", "")
            _arb_rank     = row.get("Action_EV_Ranking", "")
            _arb_mc_used  = row.get("Arbitration_MC_Used", False)
            _arb_gate_act = row.get("Arbitration_Gate_Action", "")
            _arb_why      = row.get("Arbitration_Override_Reason", "")

            if _arb_gate and str(_arb_gate) not in ("", "nan", "None"):
                _arb_overrode = str(_arb_override) not in ("", "NONE", "nan", "None")
                _arb_icon = "⚖️ " if _arb_overrode else "📐 "
                _arb_header = (
                    f"{_arb_icon}**Decision Arbitration** — Gate fired **{_arb_gate_act}**, "
                    f"EV+MC says **{_arb_ev_win}** "
                    + (f"→ **Override: {_arb_override}**" if _arb_overrode else "→ Gate confirmed")
                )
                with st.expander(_arb_header, expanded=_arb_overrode):
                    _ac1, _ac2, _ac3, _ac4 = st.columns(4)
                    _ac1.metric("Gate Action", _arb_gate_act or "—",
                                help="What the doctrine gate fired before arbitration")
                    _ac2.metric("EV Winner", _arb_ev_win or "—",
                                help="What the long-option EV comparator recommends")
                    try:
                        _ac3.metric("EV Margin", f"${float(_arb_ev_mar):,.0f}" if _arb_ev_mar not in (None, "", "nan") else "—",
                                    help="Dollar gap between #1 and #2 action. < $75 = statistical tie.")
                    except Exception:
                        _ac3.metric("EV Margin", "—")
                    try:
                        _ac4.metric("Vol Confidence", f"{float(_arb_vol_c):.0%}" if _arb_vol_c not in (None, "", "nan") else "—",
                                    help="IV/HV alignment — how reliable the MC σ estimate is (>60% = trustworthy)")
                    except Exception:
                        _ac4.metric("Vol Confidence", "—")

                    # Capital impact
                    try:
                        _cap_f = float(_arb_cap_i) if _arb_cap_i not in (None, "", "nan") else 0.0
                        if _cap_f > 0:
                            st.metric("Capital at risk (roll debit)", f"\\${_cap_f:,.0f}",
                                      help="Additional capital deployed if ROLL is executed")
                    except Exception:
                        pass

                    # Action ranking
                    if _arb_rank and str(_arb_rank) not in ("", "nan", "None"):
                        st.caption(f"Action ranking: **{_arb_rank}**  ·  {'MC-weighted' if _arb_mc_used else 'Static theta fallback (no MC)'}")

                    # Summary line
                    if _arb_summ and str(_arb_summ) not in ("", "nan", "None"):
                        st.caption(str(_arb_summ))

                    # Override explanation
                    if _arb_overrode:
                        st.info(
                            f"**Why override?** Arbitration rules satisfied: EV+MC both favor HOLD, "
                            f"roll is a debit, vol confidence adequate, DTE not critical. "
                            f"Doctrine action set to ROLL urgency=LOW (HOLD_PREPARE): "
                            f"prepare the roll, execute when MC shifts to ACT\\_NOW or DTE ≤ 14d. "
                            f"(McMillan Ch.4: regime degraded but timing matters — "
                            f"Passarelli Ch.6: roll when price edge, not just when setup changes.)"
                        )
                    elif _arb_why and str(_arb_why) not in ("", "nan", "None", "all-clear"):
                        st.caption(f"Gate confirmed — arbitration override blocked by: {_arb_why}")

            st.caption(f"`{trade_id}`")


# ─────────────────────────────────────────────────────────────────────────────
# Section B — Portfolio Snapshot + Position Cards (Cycle 1 data)
# ─────────────────────────────────────────────────────────────────────────────

def _render_portfolio_snapshot(df: pd.DataFrame, doctrine_df: pd.DataFrame | None = None):
    st.subheader("Portfolio Snapshot")

    options = df[df["AssetType"] == "OPTION"].copy()

    total_gl = _best_gl_for_group(df)

    net_delta = (pd.to_numeric(options["Delta"], errors="coerce") *
                 pd.to_numeric(options["Quantity"], errors="coerce")).sum() * 100
    net_theta = (pd.to_numeric(options["Theta"], errors="coerce") *
                 pd.to_numeric(options["Quantity"], errors="coerce")).sum() * 100
    net_vega = (pd.to_numeric(options["Vega"], errors="coerce") *
                pd.to_numeric(options["Quantity"], errors="coerce")).sum() * 100

    options = options.copy()
    options["DTE"] = _compute_dte(options["Expiration"])
    expiring_soon = (options["DTE"] <= 7).sum()
    tickers = df["Underlying_Ticker"].nunique()

    cols = st.columns(6)
    cols[0].metric("Total G/L", f"{'+'if total_gl>=0 else ''}{total_gl:,.0f}")
    cols[1].metric("Net Δ (Options)", f"{net_delta:+.0f}")
    cols[2].metric("Net θ / Day", f"{net_theta:+.2f}")
    cols[3].metric("Net ν (Vega)", f"{net_vega:+.2f}")
    cols[4].metric("Expiring ≤7 days", int(expiring_soon),
                   delta="⚠️ Action needed" if expiring_soon > 0 else None,
                   delta_color="inverse" if expiring_soon > 0 else "off")
    cols[5].metric("Underlying Tickers", int(tickers))

    # Capital Bucket exposure row — deduplicate to trade level before counting
    # (multi-leg trades like BUY_WRITE have a STOCK leg with no bucket; counting
    # all rows inflates "Unclassified". Use one row per TradeID.)
    if "Capital_Bucket" in df.columns:
        _trade_df = df.drop_duplicates("TradeID") if "TradeID" in df.columns else df
        _bc = _trade_df["Capital_Bucket"].value_counts()
        _total = max(len(_trade_df), 1)
        _t_pct = _bc.get("TACTICAL", 0) / _total * 100
        _s_pct = _bc.get("STRATEGIC", 0) / _total * 100
        _d_pct = _bc.get("DEFENSIVE", 0) / _total * 100
        _u_pct = 100.0 - _t_pct - _s_pct - _d_pct
        st.caption(
            f"**Capital Buckets** — "
            f"🔵 Tactical: **{_t_pct:.0f}%**  "
            f"🟢 Strategic: **{_s_pct:.0f}%**  "
            f"🟡 Defensive: **{_d_pct:.0f}%**"
            + (f"  ⚪ Unclassified: {_u_pct:.0f}%" if _u_pct > 5 else "")
        )
        if _t_pct > 60:
            st.warning("⚠️ Tactical exposure >60% — consider reducing short-dated directional positions in hostile regimes.")

    # --- Circuit Breaker Status ---
    if doctrine_df is not None and "Circuit_Breaker_State" in doctrine_df.columns:
        _cb_state = str(doctrine_df["Circuit_Breaker_State"].iloc[0] if len(doctrine_df) > 0 else "OPEN")
        _cb_reason = ""
        if "Circuit_Breaker_Reason" in doctrine_df.columns and len(doctrine_df) > 0:
            _cb_reason = str(doctrine_df["Circuit_Breaker_Reason"].iloc[0] or "")
        if _cb_state == "TRIPPED":
            st.error(f"**Circuit Breaker: TRIPPED** — {_cb_reason}")
        elif _cb_state == "WARNING":
            st.warning(f"**Circuit Breaker: WARNING** — {_cb_reason}")

    # --- Sector Exposure ---
    if doctrine_df is not None and "Sector_Bucket" in doctrine_df.columns:
        _sect_df = doctrine_df.drop_duplicates("TradeID") if "TradeID" in doctrine_df.columns else doctrine_df
        _basis_col = "Basis" if "Basis" in _sect_df.columns else None
        if _basis_col:
            _sect_df = _sect_df.copy()
            _sect_df["_abs_basis"] = pd.to_numeric(_sect_df[_basis_col], errors="coerce").abs()
            _sector_exp = _sect_df.groupby("Sector_Bucket")["_abs_basis"].sum()
            _total = _sector_exp.sum()
            if _total > 0:
                _sector_pct = (_sector_exp / _total * 100).sort_values(ascending=False)
                _parts = []
                for _s, _p in _sector_pct.items():
                    _marker = "**" if _p > 40 else ""
                    _parts.append(f"{_marker}{_s}: {_p:.0f}%{_marker}")
                st.caption("**Sector Exposure** — " + " | ".join(_parts))

    # --- Concentration Warnings ---
    if doctrine_df is not None and "Portfolio_Risk_Flags" in doctrine_df.columns:
        _all_flags = doctrine_df["Portfolio_Risk_Flags"].dropna().astype(str).str.strip()
        _unique_flags = set()
        for _f in _all_flags:
            for _part in str(_f).split(";"):
                _part = _part.strip()
                if _part and _part != "N/A":
                    _unique_flags.add(_part)
        for _uf in sorted(_unique_flags):
            if "SECTOR_CONCENTRATION" in _uf:
                st.warning(f"Sector over-concentrated: {_uf.replace('SECTOR_CONCENTRATION:', '')}")

    if doctrine_df is not None and "Underlying_Concentration_Risk" in doctrine_df.columns:
        _high_conc = doctrine_df[doctrine_df["Underlying_Concentration_Risk"] == "HIGH"]
        if not _high_conc.empty:
            _tickers = _high_conc["Underlying_Ticker"].unique().tolist()
            st.warning(f"High underlying concentration: {', '.join(str(t) for t in _tickers)}")

    # --- Sector RS Summary ---
    if doctrine_df is not None and "Sector_Relative_Strength" in doctrine_df.columns:
        _srs_trade = doctrine_df.drop_duplicates("TradeID") if "TradeID" in doctrine_df.columns else doctrine_df
        _srs_counts = _srs_trade["Sector_Relative_Strength"].value_counts()
        _out  = int(_srs_counts.get("OUTPERFORMING", 0))
        _neut = int(_srs_counts.get("NEUTRAL", 0))
        _und  = int(_srs_counts.get("UNDERPERFORMING", 0))
        _brk  = int(_srs_counts.get("BROKEN", 0)) + int(_srs_counts.get("MICRO_BREAKDOWN", 0))
        _parts = []
        if _out:  _parts.append(f"Outperforming: **{_out}**")
        if _neut: _parts.append(f"Neutral: **{_neut}**")
        if _und:  _parts.append(f"Underperforming: **{_und}**")
        if _brk:  _parts.append(f"Broken: **{_brk}**")
        if _parts:
            st.caption("**Sector RS** — " + "  |  ".join(_parts))

    # If doctrine is available, show critical signals under the snapshot
    if doctrine_df is not None and not doctrine_df.empty:
        crits = doctrine_df[doctrine_df["Decision_State"] == "ACTIONABLE"]
        if not crits.empty:
            tickers_crit = crits["Underlying_Ticker"].unique().tolist()
            st.error(
                f"⚠️ **{len(crits)} position(s) require action: "
                f"{', '.join(tickers_crit)}** — see Doctrine tab"
            )


def _apply_time_of_day_filter(backend_readiness: str, backend_reason: str, urgency: str) -> tuple:
    """
    Layer 2 — Execution Readiness (UI / time-of-day component).

    Takes the backend Execution_Readiness and applies current time-of-day rules
    that the pipeline can't compute (pipeline runs once; execution happens any time).

    Returns (final_readiness, final_reason, banner_color) where:
      banner_color: 'green' | 'orange' | 'red' | 'blue'

    Time-of-day rules (ET):
      First 10 min  (09:30–09:40): spreads wide, algos churning → WAIT_FOR_WINDOW
                                   UNLESS urgency=CRITICAL or EXECUTE_NOW from EXIT/DTE
      Last 10 min   (15:50–16:00): liquidity fading, MMs stepping back → WAIT_FOR_WINDOW
                                   UNLESS urgency=CRITICAL
      Pre-open      (< 09:30):     market closed → STAGE_AND_RECHECK
      Post-close    (≥ 16:00):     market closed → STAGE_AND_RECHECK
      Weekend:                     market closed → STAGE_AND_RECHECK

    CRITICAL urgency and EXIT override ALL time-of-day gates.
    Passarelli Ch.6: "Execute in the middle of the session — 10:30–15:30 ET is the
    liquidity sweet spot. Avoid open/close chop."
    """
    import datetime as _dt
    try:
        from zoneinfo import ZoneInfo as _ZoneInfo
    except ImportError:                          # Python < 3.9 fallback
        from backports.zoneinfo import ZoneInfo as _ZoneInfo  # type: ignore

    _ET      = _ZoneInfo("America/New_York")
    _now     = _dt.datetime.now(_ET)             # always Eastern Time regardless of server locale
    _weekday = _now.weekday()    # 0=Mon … 4=Fri, 5=Sat, 6=Sun
    _hour    = _now.hour
    _minute  = _now.minute
    _time_min = _hour * 60 + _minute  # minutes since midnight (ET)

    _OPEN_MINS   = 9 * 60 + 30   # 09:30
    _CLOSE_MINS  = 16 * 60        # 16:00
    _OPEN_END    = _OPEN_MINS  + 10  # 09:40
    _CLOSE_START = _CLOSE_MINS - 10  # 15:50

    urgency_upper = str(urgency or '').upper()
    _critical_override = (
        urgency_upper == 'CRITICAL'
        or backend_readiness == 'EXECUTE_NOW'   # EXIT/pin-risk/delta forcing from backend
        and any(kw in backend_reason for kw in ('EXIT action', 'DTE=', 'Delta|=', 'Earnings in 0'))
    )

    # Market closed
    if _weekday >= 5:
        if _critical_override:
            return backend_readiness, backend_reason + ' [weekend — act at Monday open]', 'orange'
        return (
            'STAGE_AND_RECHECK',
            'Market closed (weekend) — review plan now, execute Monday open',
            'blue',
        )

    if _time_min < _OPEN_MINS:
        if _critical_override:
            return backend_readiness, backend_reason + ' [pre-market — act at open]', 'orange'
        return (
            'STAGE_AND_RECHECK',
            f'Pre-market ({_hour:02d}:{_minute:02d} ET) — market opens 09:30; review plan now',
            'blue',
        )

    if _time_min >= _CLOSE_MINS:
        if _critical_override:
            return backend_readiness, backend_reason + ' [after-hours — act at next open]', 'orange'
        return (
            'STAGE_AND_RECHECK',
            'After-hours — market closed; review plan, execute at next open',
            'blue',
        )

    # Market open — time-of-day gates
    if _OPEN_MINS <= _time_min < _OPEN_END:
        if _critical_override:
            return backend_readiness, backend_reason + ' [open chop — act anyway: CRITICAL]', 'red'
        return (
            'WAIT_FOR_WINDOW',
            f'Open volatility window (09:30–09:40 ET) — spreads wide, algos active; '
            f'wait until 09:40+ for liquidity to settle (Passarelli Ch.6)',
            'orange',
        )

    if _CLOSE_START <= _time_min < _CLOSE_MINS:
        if _critical_override:
            return backend_readiness, backend_reason + ' [close window — act anyway: CRITICAL]', 'red'
        return (
            'WAIT_FOR_WINDOW',
            f'Close liquidity fade (15:50–16:00 ET) — MMs stepping back, fills unreliable; '
            f'act before 15:50 or wait for next session (Passarelli Ch.6)',
            'orange',
        )

    # Normal trading window — honour backend decision
    color_map = {
        'EXECUTE_NOW':       'green',
        'WAIT_FOR_WINDOW':   'orange',
        'STAGE_AND_RECHECK': 'blue',
    }
    return (
        backend_readiness,
        backend_reason,
        color_map.get(backend_readiness, 'blue'),
    )


def _build_auto_checklist(doc_row, hard_stop, spot, opt_legs, opt_doc_row=None, db_roll_candidates=None, is_buy_write=False, entry_structure="", net_cost=None, doctrine_action="", preferred_roll_candidate=None) -> list:
    """
    Returns list of (icon, label, detail) tuples — auto-resolved from last pipeline run data.
    Icons: ✅ clear | ⚠️ caution | 🔴 blocking | ☐ unknown/no data

    doc_row:           doctrine Series for this trade (may be STOCK leg — IV/Roll fields NaN)
    opt_doc_row:       doctrine Series for the OPTION leg (carries IV_30D + Roll_Candidate_*)
    hard_stop:         computed hard stop price (float or None)
    spot:              current stock price (float or None)
    opt_legs:          positions DataFrame rows for OPTION legs (for DTE)
    db_roll_candidates: dict from _load_roll_candidates_from_db for this TradeID (DB fallback)
    net_cost:          net cost basis per share (for ITM-defense context on debit rolls)
    doctrine_action:   doctrine Action field (e.g. "ROLL") for context-aware checklist items
    preferred_roll_candidate: pre-identified best candidate dict (e.g. credit harvest Path B)
        — when provided, overrides cand1 for spread/OI/credit-math checks. Use when the winner
        panel has already identified the recommended candidate (e.g. short-listed credit roll
        when cand1 is a debit roll that contradicts the harvest decision).
    """
    import json as _json
    items = []

    # 1. Hard stop status
    if is_buy_write:
        # BUY_WRITE/COVERED_CALL: hard stop is cost-basis driven (20% below net cost)
        if pd.notna(hard_stop) and pd.notna(spot):
            if spot < hard_stop:
                # Recovery ladder: trader consciously sold calls on already-underwater stock
                _rl_cycles = int(doc_row.get('_cycle_count', 1) or 1)
                _rl_doctrine_st = str(doc_row.get('Doctrine_State', '') or '').upper()
                if _rl_cycles >= 2 and _rl_doctrine_st == 'RECOVERY_LADDER':
                    _rl_cum = float(doc_row.get('Cumulative_Premium_Collected', 0) or 0)
                    items.append(("🟡", "Recovery ladder active",
                        f"Stock below hard stop but {_rl_cycles} cycles of premium collection"
                        f" (${_rl_cum:.2f}/sh). Hold call to expiration, then reassess."))
                else:
                    items.append(("🔴", "Below hard stop",
                        f"Stock ${spot:.2f} < stop ${hard_stop:.2f} — don't roll a broken position"))
            else:
                cushion = spot - hard_stop
                _action_up = str(doctrine_action or "").upper()
                if _action_up == "EXIT":
                    # EXIT triggered by income gate / DTE / gamma-theta, not a stop breach.
                    # Label this as context rather than a green light — stop is fine, exit
                    # reason is structural (21-DTE income gate, gamma drag, etc.).
                    items.append(("✅", "Above hard stop",
                        f"${spot:.2f} vs stop ${hard_stop:.2f} (+${cushion:.2f} cushion) — "
                        "stop not breached; EXIT triggered by income gate / gamma-theta degradation"))
                else:
                    items.append(("✅", "Above hard stop",
                        f"${spot:.2f} vs stop ${hard_stop:.2f} (+${cushion:.2f} cushion)"))
        else:
            items.append(("☐", "Hard stop", "No cost basis data — run pipeline first"))
    else:
        # Long option: hard stop is delta collapse (<0.10) or time stop (DTE ≤ 21)
        # These are checked by the doctrine engine — surface a reminder rather than a cost-basis gate
        _delta_now = None
        _dte_now = None
        if opt_doc_row is not None:
            _delta_now = pd.to_numeric(opt_doc_row.get("Delta"), errors="coerce")
            _dte_now   = pd.to_numeric(opt_doc_row.get("DTE"), errors="coerce")
        if not opt_legs.empty:
            if pd.isna(_delta_now):
                _delta_now = pd.to_numeric(opt_legs.iloc[0].get("Delta"), errors="coerce")
            if pd.isna(_dte_now):
                _dte_now = pd.to_numeric(opt_legs.iloc[0].get("DTE"), errors="coerce")
        if pd.notna(_delta_now) and abs(_delta_now) < 0.10:
            items.append(("🔴", "Delta floor breached",
                f"Delta={_delta_now:.2f} — option non-responsive (|Δ| < 0.10), exit (McMillan Ch.4)"))
        elif pd.notna(_dte_now) and _dte_now <= 21:
            items.append(("⚠️", "Time stop approaching",
                f"DTE={_dte_now:.0f} ≤ 21 — theta accelerating, exit or roll now (Passarelli Ch.2)"))
        elif pd.notna(_delta_now):
            items.append(("✅", "Delta healthy",
                f"Delta={_delta_now:.2f} — option still responsive to price moves"))

    # 2. IV level vs HV (credit environment quality)
    # IV_30D lives on the OPTION leg; doc_row may be the STOCK leg (NaN there).
    # Fallback chain: IV_30D → IV_Now → IV_Entry → opt_legs IV column
    def _first_valid(*vals):
        for v in vals:
            n = pd.to_numeric(v, errors="coerce")
            if pd.notna(n) and n > 0:
                return float(n)
        return float("nan")

    # Prefer opt_doc_row (OPTION leg) for IV — it carries IV_30D reliably
    _iv_row = opt_doc_row if opt_doc_row is not None else doc_row
    if _iv_row is not None:
        _iv = _first_valid(
            _iv_row.get("IV_30D"), _iv_row.get("IV_Now"), _iv_row.get("IV_Entry")
        )
        _iv_percentile = pd.to_numeric(_iv_row.get("IV_Percentile"), errors="coerce")
    else:
        _iv = float("nan")
        _iv_percentile = float("nan")

    # HV lives on all rows (stock + option); use whichever is available
    _hv_row = doc_row if doc_row is not None else opt_doc_row
    _hv = _first_valid(_hv_row.get("HV_20D") if _hv_row is not None else None)

    if pd.notna(_iv) and pd.notna(_hv) and _hv > 0:
        # Normalise: values stored as decimals (0.46 = 46%, 1.59 = 159%).
        # Threshold >= 5 means it was stored as a raw percentage (e.g. 46.0) — divide back.
        # This handles both storage conventions without ambiguity for realistic IV ranges.
        _iv_norm = _iv / 100.0 if _iv >= 5 else _iv
        _hv_norm = _hv / 100.0 if _hv >= 5 else _hv
        ratio = _iv_norm / _hv_norm
        iv_pct = f"{_iv_norm:.0%}"   # always multiply-by-100 format: 1.59 → "159%"
        hv_pct = f"{_hv_norm:.0%}"
        _is_long_opt = not is_buy_write and str(entry_structure).upper() not in ("BUY_WRITE", "COVERED_CALL")
        if _is_long_opt:
            # For long options (buyer): low IV/HV = cheap vol = GOOD. High IV/HV = expensive = BAD.
            # IMPORTANT: When managing a WINNER (roll action), IV/HV describes the NEW position's
            # cost — the original entry edge is already captured. The roll is a separate bet.
            # Natenberg Ch.6: "If implied vol is low vs expected future vol, prefer to buy options."
            # But the question is whether future realized vol will continue to exceed IV,
            # not whether the original entry was cheap.
            _is_roll_action = doctrine_action in ("ROLL", "ROLL_WAIT")
            if ratio <= 0.85:
                if _is_roll_action:
                    items.append(("✅", f"IV cheap ({iv_pct})",
                        f"IV/HV = {ratio:.1f}× ({iv_pct}/{hv_pct}) — original entry was cheap vol vs realized. "
                        f"Rolling INTO a new position at this ratio means betting future realized vol "
                        f"stays >{iv_pct} — a separate thesis from the current winner. "
                        f"Natenberg Ch.6: edge holds if realized vol continues to exceed IV; "
                        f"if HV is mean-reverting, this edge is dissipating."))
                else:
                    items.append(("✅", f"IV cheap ({iv_pct})",
                        f"IV/HV = {ratio:.1f}× ({iv_pct}/{hv_pct}) — buying cheap vol vs realized; edge on entry"))
            elif ratio <= 1.15:
                items.append(("⚠️", f"IV fair ({iv_pct})",
                    f"IV/HV = {ratio:.1f}× ({iv_pct}/{hv_pct}) — vol fairly priced; no edge buying here, verify thesis"))
            else:
                items.append(("🔴", f"IV expensive ({iv_pct})",
                    f"IV/HV = {ratio:.1f}× ({iv_pct}/{hv_pct}) — paying elevated premium vs realized vol; consider waiting for IV to retreat"))
        else:
            # For credit sellers (BUY_WRITE): IV/HV matters for ROLL credit environment.
            # For EXIT (accept assignment or active exit), IV/HV does not gate the decision —
            # the assignment proceeds are strike-price-locked, not IV-dependent.
            # Show a condensed context note for EXIT rather than a roll-readiness grade.
            _pct_note = (f"  IV_Percentile={_iv_percentile:.0f}% (5d high — best roll timing this week)"
                         if pd.notna(_iv_percentile) and _iv_percentile >= 80 else
                         f"  IV_Percentile={_iv_percentile:.0f}% (low end of recent range)"
                         if pd.notna(_iv_percentile) and _iv_percentile <= 20 else
                         f"  IV_Percentile={_iv_percentile:.0f}%"
                         if pd.notna(_iv_percentile) else "")
            _action_up_iv = str(doctrine_action or "").upper()
            if _action_up_iv == "EXIT":
                # EXIT context: IV/HV is not a gate — only relevant if choosing active buyback.
                # Accept-assignment path disposes the option at expiry — no IV-dependent trade.
                # Use a clean percentile note without roll-specific language.
                _iv_pct_exit = (
                    f"  IV at {_iv_percentile:.0f}th percentile of recent range."
                    if pd.notna(_iv_percentile) else ""
                )
                _iv_exit_note = (
                    f"IV {iv_pct} vs HV {hv_pct} (ratio {ratio:.1f}×).{_iv_pct_exit}  "
                    "For accept-assignment: irrelevant — option expires naturally, no trade needed. "
                    "For active-exit buyback: lower IV = cheaper buyback cost."
                )
                items.append(("ℹ️", f"IV context ({iv_pct}) — EXIT, not a gate", _iv_exit_note))
            elif ratio >= 1.2:
                items.append(("✅", f"IV elevated ({iv_pct})",
                    f"IV/HV = {ratio:.1f}× ({iv_pct}/{hv_pct}) — good credit environment for rolling.{_pct_note}"))
            elif ratio >= 0.9:
                items.append(("⚠️", f"IV normal ({iv_pct})",
                    f"IV/HV = {ratio:.1f}× ({iv_pct}/{hv_pct}) — adequate; check live chain for current credit.{_pct_note}"))
            else:
                # ratio < 0.9 — IV below HV. Severity depends on where IV sits in its recent
                # range. Percentile ≥ 50 means IV is at or above its median for the week —
                # structurally thin vs HV but not "depressed". Only flag RED when IV is
                # genuinely in the low tail of its own recent range (< 50th percentile).
                # Also check IV backwardation: if near-term IV > back-month, the short-vol
                # entry is actually favorable despite the IV/HV ratio dip (Natenberg Ch.11).
                _iv_bkwd_src = _iv_row if _iv_row is not None else {}
                _iv_bkwd = str(
                    _iv_bkwd_src.get("IV_Term_Structure",
                    _iv_bkwd_src.get("Surface_Shape", "")) or ""
                ).upper()
                _is_backwardated = "BACKWARDA" in _iv_bkwd or "INVERTED" in _iv_bkwd
                if pd.notna(_iv_percentile) and _iv_percentile >= 70:
                    items.append(("⚠️", f"IV below HV, near week-high ({iv_pct})",
                        f"IV/HV = {ratio:.1f}× ({iv_pct}/{hv_pct}) — IV below realized vol but "
                        f"at {_iv_percentile:.0f}th percentile of recent range. "
                        f"Credit is structurally thin vs HV, but this is the best roll opportunity of the week."))
                elif pd.notna(_iv_percentile) and _iv_percentile >= 50:
                    # Above-median IV — not depressed; adequate for rolling.
                    _bkwd_note = (
                        " IV term structure BACKWARDATED: near-term IV elevated vs back-month "
                        "— favorable short-vol entry (Natenberg Ch.11)."
                        if _is_backwardated else ""
                    )
                    items.append(("⚠️", f"IV below HV, mid-range ({iv_pct})",
                        f"IV/HV = {ratio:.1f}× ({iv_pct}/{hv_pct}) — IV below realized vol but "
                        f"at {_iv_percentile:.0f}th percentile (above median). "
                        f"Credit will be slightly thin — acceptable for roll execution.{_bkwd_note}{_pct_note}"))
                else:
                    # Genuinely low IV — below median of its own recent range.
                    _bkwd_override = (
                        " ⚠️ However, term structure is BACKWARDATED — near-term IV elevated vs "
                        "back-month despite low IV/HV ratio. Credit may be better than the ratio implies "
                        "(Natenberg Ch.11)."
                        if _is_backwardated else ""
                    )
                    items.append(("🔴", f"IV depressed ({iv_pct})",
                        f"IV/HV = {ratio:.1f}× ({iv_pct}/{hv_pct}) — credit will be thin; "
                        f"consider waiting for vol expansion.{_bkwd_override}{_pct_note}"))
    else:
        items.append(("☐", "IV level", "No IV/HV data — run pipeline to populate"))

    # 3 & 4. Bid/ask spread + roll credit math (from Roll_Candidate_1 JSON)
    # Roll_Candidate_1 lives on the OPTION leg row; doc_row may be the STOCK leg.
    # Try doc_row first, then fall back to opt_legs rows which carry the candidates.
    def _parse_candidate(raw) -> dict:
        if raw in (None, "", "nan") or (isinstance(raw, float) and pd.isna(raw)):
            return {}
        try:
            return _json.loads(str(raw)) if isinstance(raw, str) else (raw if isinstance(raw, dict) else {})
        except Exception:
            return {}

    cand1 = {}
    _cand1_from_db = False

    # preferred_roll_candidate overrides cand1 for spread/OI/credit-math checks.
    # Used in winner harvest mode when the best credit candidate (Path B) has already
    # been identified by the winner panel — cand1 may be a debit roll that contradicts
    # the harvest decision, so we evaluate the checklist against the Path B candidate instead.
    if preferred_roll_candidate and isinstance(preferred_roll_candidate, dict):
        cand1 = preferred_roll_candidate
    else:
        # Prefer opt_doc_row (OPTION leg) for Roll_Candidate_1 — stock leg row has None.
        # Final fallback: db_roll_candidates from most recent market-hours DB run.
        for _rc_source in (opt_doc_row, doc_row):
            if _rc_source is not None:
                cand1 = _parse_candidate(_rc_source.get("Roll_Candidate_1"))
                if cand1:
                    break
        if not cand1 and db_roll_candidates:
            cand1 = _parse_candidate(db_roll_candidates.get("Roll_Candidate_1"))
            _cand1_from_db = bool(cand1)

    # Check 3a: Open Interest — CLOSE leg (current contract, sell-to-close fill risk)
    # Read from opt_doc_row first (option leg of doctrine df), then opt_legs positions rows.
    _oi = None
    for _src in ([opt_doc_row] if opt_doc_row is not None else []):
        _raw_oi = _src.get("Open_Int") if hasattr(_src, "get") else None
        if _raw_oi not in (None, "", "nan") and not (isinstance(_raw_oi, float) and pd.isna(_raw_oi)):
            try:
                _oi = int(float(_raw_oi))
            except Exception:
                pass
            break
    if _oi is None and not opt_legs.empty and "Open_Int" in opt_legs.columns:
        _oi_series = pd.to_numeric(opt_legs["Open_Int"], errors="coerce").dropna()
        if not _oi_series.empty:
            _oi = int(_oi_series.iloc[0])
    if _oi is not None:
        _action_up_oi = str(doctrine_action or "").upper()
        # For EXIT on BUY_WRITE: OI matters only if doing an active buyback.
        # Accept-assignment path lets the option expire — no STC order placed.
        # Surface as informational rather than a gate for EXIT actions.
        if _action_up_oi == "EXIT" and is_buy_write:
            if _oi >= 200:
                items.append(("✅", f"Close-leg OI adequate ({_oi:,})",
                    f"OI={_oi:,} on current contract — sufficient depth if active buyback needed. "
                    "Not required for accept-assignment path (option expires naturally)."))
            else:
                items.append(("⚠️", f"Close-leg OI thin ({_oi:,})",
                    f"OI={_oi:,} — if choosing active buyback, use limit at mid and be patient. "
                    "Accept-assignment path avoids this risk entirely (no buyback needed)."))
        elif _oi < 200:
            items.append(("🔴", f"Close-leg OI very thin ({_oi:,})",
                f"OI={_oi:,} on current contract (sell-to-close leg) — fill risk high on exit; "
                "market maker may not be present. "
                "Passarelli Ch.6: with thin OI, leg into the roll rather than a simultaneous spread order — "
                "close the old leg first with a patient limit at mid, then open the new leg separately. "
                "A combined spread order on thin OI risks a one-leg fill leaving you with an unintended naked position."))
        elif _oi < 500:
            items.append(("⚠️", f"Close-leg OI low ({_oi:,})",
                f"OI={_oi:,} on current contract (sell-to-close leg) — use limit at mid, be patient."))
        else:
            items.append(("✅", f"Close-leg OI adequate ({_oi:,})",
                f"OI={_oi:,} on current contract — sufficient depth for sell-to-close leg."))

    # Check 3a-open: Open Interest — OPEN leg (roll target, buy-to-open fill risk)
    # Only relevant for ROLL actions — TRIM/EXIT close an existing leg, no new contract opened.
    _cand_oi = cand1.get("oi") if (cand1 and doctrine_action not in ("TRIM", "EXIT")) else None
    if _cand_oi is not None:
        try:
            _cand_oi = int(_cand_oi)
        except (TypeError, ValueError):
            _cand_oi = None
    if _cand_oi is not None:
        _stale_oi_note = " (prior run — verify live)" if _cand1_from_db else ""
        if _cand_oi < 200:
            items.append(("🔴", f"Open-leg OI very thin ({_cand_oi:,}){_stale_oi_note}",
                f"OI={_cand_oi:,} on roll target (buy-to-open leg) — fill risk on new contract; "
                "consider a different strike or expiry with more open interest."))
        elif _cand_oi < 500:
            items.append(("⚠️", f"Open-leg OI low ({_cand_oi:,}){_stale_oi_note}",
                f"OI={_cand_oi:,} on roll target (buy-to-open leg) — use limit at mid, be patient."))
        else:
            items.append(("✅", f"Open-leg OI adequate ({_cand_oi:,}){_stale_oi_note}",
                f"OI={_cand_oi:,} on roll target — sufficient depth for buy-to-open leg."))

    # Check 3-trim: for TRIM/EXIT, evaluate the CURRENT contract's spread (STC leg), not the
    # roll target. The user is selling what they own — that's the fill-risk surface.
    if doctrine_action in ("TRIM", "EXIT") and not opt_legs.empty:
        _stc_bid = pd.to_numeric(opt_legs.iloc[0].get("Bid"), errors="coerce") if len(opt_legs) > 0 else float("nan")
        _stc_ask = pd.to_numeric(opt_legs.iloc[0].get("Ask"), errors="coerce") if len(opt_legs) > 0 else float("nan")
        if pd.notna(_stc_bid) and pd.notna(_stc_ask) and _stc_ask > 0:
            _stc_mid = (_stc_bid + _stc_ask) / 2
            _stc_spread_pct = (_stc_ask - _stc_bid) / _stc_mid * 100 if _stc_mid > 0 else 0
            if _stc_spread_pct <= 5.0:
                items.append(("✅", f"Spread OK ({_stc_spread_pct:.1f}%)",
                    f"Bid ${_stc_bid:.2f} / Ask ${_stc_ask:.2f} — limit at mid ${_stc_mid:.2f} should fill promptly"))
            elif _stc_spread_pct <= 10.0:
                items.append(("⚠️", f"Spread moderate ({_stc_spread_pct:.1f}%)",
                    f"Bid ${_stc_bid:.2f} / Ask ${_stc_ask:.2f} — use patient limit order at mid ${_stc_mid:.2f}"))
            else:
                items.append(("🔴", f"Wide spread ({_stc_spread_pct:.1f}%)",
                    f"Bid ${_stc_bid:.2f} / Ask ${_stc_ask:.2f} — current contract is illiquid; "
                    "consider waiting for better market conditions before trimming"))
        else:
            items.append(("☐", "Bid/ask spread", "No live bid/ask — check chain before executing"))

    if doctrine_action in ("TRIM", "EXIT"):
        # Spread already handled above via the STC path (opt_legs bid/ask).
        # Roll credit math is not applicable for EXIT/TRIM — suppress the ☐ stub.

        # Exit Limit Price — show suggested limit from daily technical levels + delta approx
        if doctrine_action == "EXIT":
            _elp_row = opt_doc_row if opt_doc_row is not None else doc_row
            _elp_price = pd.to_numeric(_elp_row.get("Exit_Limit_Price") if _elp_row is not None else None, errors="coerce")
            _elp_level = str((_elp_row.get("Exit_Limit_Level") if _elp_row is not None else "") or "")
            _elp_rationale = str((_elp_row.get("Exit_Limit_Rationale") if _elp_row is not None else "") or "")

            if pd.notna(_elp_price) and _elp_level not in ("", "SKIP", "Current"):
                items.append(("💲", f"Limit ${_elp_price:.2f} at {_elp_level}",
                    _elp_rationale))
            elif _elp_level == "Current":
                items.append(("ℹ️", "No favorable level",
                    f"No nearby technical target — use market price. {_elp_rationale}"))
            elif _elp_level == "SKIP":
                pass  # STOCK_ONLY / multi-leg: no limit suggestion
            # else: no data — silently omit

    elif cand1:
        # Check 3: spread quality (roll target — only for ROLL/ROLL_WAIT actions)
        spread_pct = cand1.get("spread_pct")
        liq = str(cand1.get("liq_grade", ""))
        _stale_note = " (prior run — verify live)" if _cand1_from_db else ""
        if spread_pct is not None:
            spread_pct = float(spread_pct)
            if spread_pct <= 5.0 or liq in ("Excellent", "Good"):
                items.append(("✅", f"Spread OK ({spread_pct:.1f}%){_stale_note}",
                    f"Liquidity: {liq or 'n/a'} — execute with limit at mid"))
            elif spread_pct <= 10.0:
                items.append(("⚠️", f"Spread moderate ({spread_pct:.1f}%){_stale_note}",
                    f"Liquidity: {liq or 'n/a'} — use patient limit order at mid"))
            else:
                items.append(("🔴", f"Wide spread ({spread_pct:.1f}%){_stale_note}",
                    f"Liquidity: {liq or 'thin'} — wait for market to settle before executing"))
        else:
            items.append(("☐", "Bid/ask spread", "No spread data in candidate — check live chain"))

        # Check 4: roll credit math
        ctr = cand1.get("cost_to_roll", {})
        if isinstance(ctr, dict):
            roll_type = str(ctr.get("type", ""))
            net_per = float(ctr.get("net_per_contract", 0) or 0)
            _new_strike = None
            try:
                _new_strike_raw = cand1.get("strike")
                if _new_strike_raw not in (None, "", "?"):
                    _new_strike = float(_new_strike_raw)
            except Exception:
                pass

            # Detect ITM-defense context: doctrine says ROLL + new strike rescues position above net cost
            _itm_defense_roll = (
                doctrine_action in ("ROLL", "ROLL_WAIT")
                and net_cost is not None
                and pd.notna(net_cost)
                and _new_strike is not None
                and _new_strike > net_cost
                and spot is not None
                and pd.notna(spot)
                and spot > (net_cost * 0.75)  # spot still viable (not catastrophically broken)
            )

            if roll_type == "credit":
                items.append(("✅", f"Net credit roll (+${net_per:.2f}/contract){_stale_note}",
                    "Roll generates income — basis continues reducing"))
            elif roll_type == "debit" and _itm_defense_roll:
                # net_per_contract is already per-share (e.g. -8.95 = $8.95/share debit)
                _strike_above_nc  = _new_strike - net_cost
                _debit_per_share  = abs(net_per)           # net_per_contract IS per-share
                _new_breakeven    = net_cost + _debit_per_share
                _net_total        = ctr.get("net_total")   # pre-computed: net_per × qty × 100
                _total_str        = f"${abs(_net_total):,.0f} total" if _net_total is not None else ""
                # Determine what assignment at the CURRENT strike would deliver.
                # This requires the current short strike, not the roll target.
                # We don't have it directly in ctr, but we can infer from context:
                # If spot > net_cost and spot < new_strike: current strike is somewhere between them.
                # Key question: is current strike ABOVE or BELOW net_cost?
                # _new_strike > net_cost is already guaranteed by _itm_defense_roll.
                # For the "assignment at current strike" language, use spot as proxy:
                # if spot (deeply ITM → current strike ≈ below spot) is above net_cost,
                # assignment would be profitable. If spot < net_cost, it's a loss.
                _assignment_context: str
                if spot is not None and pd.notna(spot) and spot > net_cost:
                    # Stock currently above net cost — current strike (which is below spot/ITM)
                    # may still be above net_cost (assignment would profit) or below it (loss).
                    # Without current_strike directly, be explicit about what we know:
                    _assign_pnl_new = _new_strike - net_cost   # if assigned at NEW strike
                    _assignment_context = (
                        f"Rolling to ${_new_strike:.2f} locks in a ${_assign_pnl_new:.2f}/share "
                        f"profit buffer above net cost ${net_cost:.2f} if assigned. "
                        f"The ${_debit_per_share:.2f} debit buys ${_strike_above_nc:.2f} of "
                        f"additional headroom — pay it to control the exit price."
                    )
                else:
                    # Stock below net cost — assignment at any strike near spot would likely
                    # realize a loss after premiums; the roll is a genuine rescue.
                    _assignment_context = (
                        f"Without this roll, assignment at current strike risks a per-share loss "
                        f"vs net cost ${net_cost:.2f}. "
                        f"The ${_debit_per_share:.2f} debit rescues the position by moving the "
                        f"strike to ${_new_strike:.2f} — ${_strike_above_nc:.2f} above net cost."
                    )
                items.append(("⚠️", f"Debit roll required for ITM defense (-${_debit_per_share:.2f}/share){_stale_note}",
                    f"Pay ${_debit_per_share:.2f}/share{(', ' + _total_str) if _total_str else ''} "
                    f"to roll to ${_new_strike:.2f}. New breakeven ~${_new_breakeven:.2f}/share. "
                    + _assignment_context))
            elif roll_type == "debit" and _is_long_opt:
                # Long options (LONG_PUT, LONG_CALL, LEAPS): paying a debit to extend
                # time is the normal roll mechanic — it's buying more runway, not
                # destroying income. McMillan Ch.4: "The only question is whether the
                # expected move still justifies the time-value cost."
                _debit_per_share = abs(net_per)
                _net_total_dir   = ctr.get("net_total")
                _total_str_dir   = f"${abs(_net_total_dir):,.0f} total" if _net_total_dir is not None else ""
                items.append(("⚠️", f"Debit to extend time (-${_debit_per_share:.2f}/share){_stale_note}",
                    f"Costs ${_debit_per_share:.2f}/share{(', ' + _total_str_dir) if _total_str_dir else ''} "
                    f"to roll — you're buying additional time for the move to develop. "
                    f"McMillan Ch.4: only worth paying if the expected directional move is "
                    f"still intact and exceeds the roll cost."))
            elif roll_type == "debit" and abs(net_per) <= 10:
                items.append(("⚠️", f"Small net debit (-${abs(net_per):.2f}/contract){_stale_note}",
                    "Marginal debit — only worthwhile if new strike meaningfully higher than current"))
            elif roll_type == "debit":
                items.append(("🔴", f"Net debit roll (-${abs(net_per):.2f}/contract){_stale_note}",
                    "Paying to roll destroys basis recovery — reassess or exit"))
            else:
                items.append(("☐", "Roll credit math", "Cost-to-roll not computed — run pipeline during market hours"))
        else:
            items.append(("☐", "Roll credit math", "No candidate data — run pipeline first"))
    else:
        # No candidates, not EXIT/TRIM — this is a ROLL without candidates yet.
        items.append(("☐", "Bid/ask spread", "Run pipeline during market hours for live spread data"))
        items.append(("☐", "Roll credit math", "No roll candidates — run pipeline to generate"))

    # Buyback limit price for ROLL BUY_WRITE/CC — close-leg pricing from exit limit pricer
    if doctrine_action in ("ROLL", "ROLL_WAIT") and is_buy_write:
        _blp_row = opt_doc_row if opt_doc_row is not None else doc_row
        _blp_price = pd.to_numeric(_blp_row.get("Exit_Limit_Price") if _blp_row is not None else None, errors="coerce")
        _blp_level = str((_blp_row.get("Exit_Limit_Level") if _blp_row is not None else "") or "")
        _blp_rationale = str((_blp_row.get("Exit_Limit_Rationale") if _blp_row is not None else "") or "")

        if pd.notna(_blp_price) and _blp_level not in ("", "SKIP", "Current"):
            items.append(("💲", f"Buyback limit ${_blp_price:.2f} at {_blp_level}",
                _blp_rationale))
        elif _blp_level == "Current":
            items.append(("ℹ️", "No favorable buyback level",
                f"No nearby dip target — buy back at market. {_blp_rationale}"))

    # 5. DTE urgency
    # Note: "no DTE urgency" means time alone doesn't force the hand — but it does NOT
    # override a CRITICAL/HIGH urgency from delta/assignment risk. Qualify accordingly.
    _action_urgency = str(doc_row.get("Urgency", "") or "") if doc_row is not None else ""
    _action_is_urgent = _action_urgency.upper() in ("CRITICAL", "HIGH")
    _action_is_roll   = str(doctrine_action or "").upper() in ("ROLL", "ROLL_WAIT")
    if not opt_legs.empty and "DTE" in opt_legs.columns:
        min_dte = pd.to_numeric(opt_legs["DTE"], errors="coerce").min()
        if pd.notna(min_dte):
            min_dte = float(min_dte)
            if min_dte <= 3:
                items.append(("🔴", f"{int(min_dte)}d to expiry",
                    "Roll today — pin risk is real and time value nearly zero"))
            elif min_dte <= 7:
                items.append(("⚠️", f"{int(min_dte)}d to expiry",
                    "Roll this week before theta collapse and pin risk develop"))
            elif _action_is_urgent and not _is_long_opt:
                # EXIT or ROLL with HIGH/CRITICAL urgency on a short option:
                # DTE context depends on whether this is an exit or a roll.
                _action_upper = str(doctrine_action or "").upper()
                if _action_upper in ("EXIT", "TRIM"):
                    if min_dte <= 21:
                        items.append(("⚠️", f"{int(min_dte)}d to expiry — act this week",
                            f"{_action_urgency} urgency: {int(min_dte)}d remaining. "
                            f"Assignment risk is elevated — execute or accept assignment before expiry. "
                            f"Do not wait for a 'better' window that may not come."))
                    else:
                        items.append(("✅", f"{int(min_dte)}d to expiry",
                            f"{_action_urgency} urgency driven by delta/assignment risk, not DTE. "
                            f"Adequate time to choose exit timing — act on doctrine signals."))
                else:
                    # ROLL with HIGH/CRITICAL urgency
                    items.append(("⚠️", f"{int(min_dte)}d to expiry",
                        f"DTE alone not urgent, but {_action_urgency} urgency driven by delta/assignment risk — "
                        f"act on those signals, not DTE"))
            elif _action_is_urgent and _is_long_opt:
                # Long options cannot be assigned — urgency is structural (momentum/vol/time).
                items.append(("⚠️", f"{int(min_dte)}d to expiry",
                    f"DTE alone not urgent, but {_action_urgency} urgency driven by structural signals "
                    f"(momentum, vol regime, or theta trajectory) — review doctrine rationale"))
            else:
                items.append(("✅", f"{int(min_dte)}d to expiry",
                    "No DTE urgency — can wait for better entry conditions"))

    # 6. Scan data freshness
    snap_ts = None
    if doc_row is not None:
        snap_ts = pd.to_datetime(doc_row.get("Snapshot_TS"), errors="coerce")
    if snap_ts is not None and pd.notna(snap_ts):
        _snap_naive = snap_ts.replace(tzinfo=None) if hasattr(snap_ts, 'tzinfo') and snap_ts.tzinfo else snap_ts
        age_h = (datetime.now() - _snap_naive).total_seconds() / 3600
        if age_h < 4:
            items.append(("✅", f"Data fresh ({age_h:.0f}h ago)",
                "Roll candidates reflect recent market conditions"))
        elif age_h < 24:
            items.append(("⚠️", f"Data {age_h:.0f}h old",
                "Re-run pipeline for fresher candidates before acting"))
        else:
            items.append(("🔴", f"Stale data ({age_h:.0f}h old)",
                "Run pipeline first — roll candidates may not reflect current chain"))
    else:
        items.append(("☐", "Data freshness", "No timestamp — run pipeline to get current candidates"))

    # 7. Sector relative strength (z-score normalized, Natenberg Ch.8)
    _srs_row = opt_doc_row if opt_doc_row is not None else doc_row
    if _srs_row is not None:
        _srs      = str(_srs_row.get("Sector_Relative_Strength", "") or "").upper()
        _srs_z    = pd.to_numeric(_srs_row.get("Sector_RS_ZScore", None), errors="coerce")
        _srs_b    = str(_srs_row.get("Sector_Benchmark", "") or "")
        if _srs and _srs not in ("", "NEUTRAL") and pd.notna(_srs_z):
            _z_str = f"z={_srs_z:+.2f}"
            _bench_str = f" vs {_srs_b}" if _srs_b else ""
            if _srs == "OUTPERFORMING":
                items.append(("✅", f"Sector RS: outperforming ({_z_str})",
                    f"Stock leading its benchmark{_bench_str} — sector tailwind (Natenberg Ch.8)"))
            elif _srs == "UNDERPERFORMING":
                items.append(("⚠️", f"Sector RS: lagging ({_z_str})",
                    f"Stock −1σ to −2σ vs {_srs_b or 'benchmark'} — monitor for deterioration (McMillan Ch.1)"))
            elif _srs == "MICRO_BREAKDOWN":
                items.append(("⚠️", f"Sector RS: micro-breakdown ({_z_str})",
                    f"Stock −2σ to −3σ vs {_srs_b or 'benchmark'} — sector headwind. Roll with caution (Natenberg Ch.8)"))
            elif _srs == "BROKEN":
                items.append(("🔴", f"Sector RS: broken ({_z_str})",
                    f"Stock >3σ below {_srs_b or 'benchmark'} — structural sector rotation out. Thesis DEGRADED (McMillan Ch.1)"))
        elif _srs == "NEUTRAL" and pd.notna(_srs_z):
            _z_str = f"z={_srs_z:+.2f}"
            _bench_str = f" vs {_srs_b}" if _srs_b else ""
            items.append(("✅", f"Sector RS: neutral ({_z_str})",
                f"Stock tracking benchmark{_bench_str} within 1σ — no sector headwind"))
        else:
            items.append(("☐", "Sector relative strength",
                "No RS data — run pipeline (thesis_engine.py computes z-score vs sector ETF)"))
    else:
        items.append(("☐", "Sector relative strength", "No doctrine data available"))

    # 8. Earnings proximity — automated from broker CSV "Earnings Date" column
    _earn_row = opt_doc_row if opt_doc_row is not None else doc_row
    _earnings_raw = _earn_row.get("Earnings Date") if _earn_row is not None else None
    _earn_missing = (
        _earnings_raw in (None, "", "nan", "N/A", "n/a")
        or (isinstance(_earnings_raw, float) and pd.isna(_earnings_raw))
    )
    if _earn_missing:
        items.append(("☐", "Earnings date",
            "Not populated in broker CSV — check manually before rolling. "
            "Earnings within option DTE can cause IV spike then crush immediately after."))
    else:
        try:
            _ed = pd.to_datetime(str(_earnings_raw), errors="coerce")
            if pd.notna(_ed):
                _days_to_earn = (_ed.normalize() - pd.Timestamp.now().normalize()).days
                if _days_to_earn < 0:
                    items.append(("✅", f"Earnings passed ({_ed.strftime('%b %d')})",
                        f"Last earnings {abs(_days_to_earn)}d ago — no near-term IV event risk"))
                elif _days_to_earn == 0:
                    items.append(("🔴", f"Earnings TODAY ({_ed.strftime('%b %d')})",
                        "Do NOT roll — IV will collapse immediately after announcement"))
                elif _days_to_earn <= 7:
                    items.append(("🔴", f"Earnings in {_days_to_earn}d ({_ed.strftime('%b %d')})",
                        f"Within 7 days — hold through OR exit before. Rolling into earnings "
                        f"sells vol at peak then gets crushed. Do not establish new positions now."))
                elif _days_to_earn <= 21:
                    items.append(("⚠️", f"Earnings in {_days_to_earn}d ({_ed.strftime('%b %d')})",
                        f"Within 3 weeks — confirm new expiry lands AFTER earnings date. "
                        f"If rolling INTO earnings window: plan for IV event (Natenberg Ch.8)."))
                elif _days_to_earn <= 45:
                    items.append(("⚠️", f"Earnings in {_days_to_earn}d ({_ed.strftime('%b %d')})",
                        f"Within 45 days — ensure roll target expiry clears earnings date. "
                        f"IV typically inflates 2–3 weeks pre-earnings then collapses after."))
                else:
                    items.append(("✅", f"Earnings in {_days_to_earn}d ({_ed.strftime('%b %d')})",
                        f"No near-term earnings risk — roll window clear"))
            else:
                items.append(("☐", "Earnings date",
                    f"Could not parse date '{_earnings_raw}' — verify manually before rolling"))
        except Exception:
            items.append(("☐", "Earnings date",
                "Parse error — check broker CSV Earnings Date field manually"))

    # 9. Pre-market news (always manual)
    items.append(("☐", "Pre-market gap / news",
        "Check: earnings releases, guidance changes, sector news since last close"))

    return items


def _build_copy_text(
    header: str,
    doctrine_row,
    group: "pd.DataFrame",
    stock_legs: "pd.DataFrame",
    opt_legs: "pd.DataFrame",
    entry_structure: str,
    card_metrics: dict,
) -> str:
    """Build a plain-text snapshot of a position card for clipboard copy."""
    lines: list[str] = []

    # ── Header ──────────────────────────────────────────────────────────────
    # Strip markdown bold/backtick/emoji decorators for clean text
    _hdr = header.replace("**", "").replace("`", "")
    lines.append(f"📝 {_hdr}")

    # ── Doctrine ────────────────────────────────────────────────────────────
    if doctrine_row is not None:
        _rat = str(doctrine_row.get("Rationale", "") or "")
        _src = str(doctrine_row.get("Doctrine_Source", "") or "")
        if _rat:
            lines.append(f"Doctrine: {_rat}")
        if _src:
            lines.append(f"Source: {_src}")
    lines.append("")

    # ── Drift State ─────────────────────────────────────────────────────────
    if doctrine_row is not None:
        _da = str(doctrine_row.get("Drift_Action", "") or "")
        _ss = str(doctrine_row.get("Signal_State", "") or "")
        _ds = str(doctrine_row.get("Data_State", "") or "")
        _rs = str(doctrine_row.get("Regime_State", "") or "")
        _dd = str(doctrine_row.get("Drift_Direction", "") or "")
        _dm = str(doctrine_row.get("Drift_Magnitude", "") or "")
        _dp = str(doctrine_row.get("Drift_Persistence", "") or "")
        if any(v for v in [_da, _ss, _ds, _rs]):
            lines.append("📊 Drift State")
            lines.append(
                f"Drift Action: {_da or 'NO_ACTION'} | Signal: {_ss or '—'} | "
                f"Data: {_ds or '—'} | Regime: {_rs or '—'}"
            )
            if _dd:
                _arrow = {"Up": "↑", "Down": "↓", "Flat": "→"}.get(_dd, "")
                _traj = f"{_arrow} {_dd} / {_dm}"
                if _dp and _dp not in ("nan", "None"):
                    _traj += f" ({_dp})"
                lines.append(f"Greek Drift: {_traj}")
            lines.append("")

    # ── Vol State / Greek ROC / Entry Displacement ──────────────────────────
    if doctrine_row is not None:
        # Prefer option row for IV fields
        _opt_row = card_metrics.get("_opt_row")
        def _dget(field, fb=None):
            v = doctrine_row.get(field, fb)
            if v is None or (isinstance(v, float) and pd.isna(v)) or str(v) in ("nan", "None", ""):
                if _opt_row is not None:
                    v = _opt_row.get(field, fb)
            return v

        _iv_now  = _dget("IV_Now")
        _iv_roc1 = _dget("IV_ROC_1D")
        _rp      = _dget("ROC_Persist_3D")
        _ivhv    = _dget("IV_vs_HV_Gap")
        _ivpct   = _dget("IV_Percentile")
        _parts = []
        if _iv_now is not None and not (isinstance(_iv_now, float) and pd.isna(_iv_now)):
            _parts.append(f"IV Now: {float(_iv_now):.1%}")
        if _iv_roc1 is not None and not (isinstance(_iv_roc1, float) and pd.isna(_iv_roc1)):
            _parts.append(f"IV ROC 1D: {float(_iv_roc1):+.2f}")
        if _rp is not None and not (isinstance(_rp, float) and pd.isna(_rp)):
            _parts.append(f"Persist: {int(float(_rp))}d")
        if _ivhv is not None and not (isinstance(_ivhv, float) and pd.isna(_ivhv)):
            _parts.append(f"IV vs HV: {float(_ivhv):+.1%}")
        if _ivpct is not None and not (isinstance(_ivpct, float) and pd.isna(_ivpct)):
            _parts.append(f"Percentile: {float(_ivpct):.0f}%")
        if _parts:
            lines.append(f"📊 Vol State: {' · '.join(_parts)}")

        # Greek ROC
        _d1 = _dget("Delta_ROC_1D")
        _v1 = _dget("Vega_ROC_1D")
        _g1 = _dget("Gamma_ROC_1D")
        _roc_parts = []
        if _d1 is not None and not (isinstance(_d1, float) and pd.isna(_d1)):
            _roc_parts.append(f"Δ ROC 1D: {float(_d1):+.2f}")
        if _v1 is not None and not (isinstance(_v1, float) and pd.isna(_v1)):
            _roc_parts.append(f"ν ROC 1D: {float(_v1):+.2f}")
        if _g1 is not None and not (isinstance(_g1, float) and pd.isna(_g1)):
            _roc_parts.append(f"Γ ROC 1D: {float(_g1):+.2f}")
        if _roc_parts:
            lines.append(f"Greek ROC (1d): {' · '.join(_roc_parts)}")

        # Entry displacement
        _de = doctrine_row.get("Delta_Displacement")
        _ve = doctrine_row.get("Vega_Displacement")
        _ie = doctrine_row.get("IV_Displacement")
        _ue = doctrine_row.get("UL_Displacement")
        _disp = []
        if _de is not None and not (isinstance(_de, float) and pd.isna(_de)):
            _disp.append(f"Δ from entry: {float(_de):+.3f}")
        if _ve is not None and not (isinstance(_ve, float) and pd.isna(_ve)):
            _disp.append(f"ν from entry: {float(_ve):+.3f}")
        if _ie is not None and not (isinstance(_ie, float) and pd.isna(_ie)):
            _disp.append(f"IV from entry: {float(_ie):+.1%}")
        if _ue is not None and not (isinstance(_ue, float) and pd.isna(_ue)):
            _disp.append(f"UL from entry: {float(_ue):+.3f}")
        if _disp:
            lines.append(f"📐 Entry displacement: {' · '.join(_disp)}")
        lines.append("")

    # ── Stock Details (BW/CC) ───────────────────────────────────────────────
    _is_bw = str(entry_structure).upper() in ("BUY_WRITE", "COVERED_CALL")
    if _is_bw and not stock_legs.empty:
        _s = stock_legs.iloc[0]
        _spot_v = pd.to_numeric(_s.get("UL Last"), errors="coerce")
        _raw_cost = pd.to_numeric(_s.get("Basis"), errors="coerce")
        _qty_v = pd.to_numeric(_s.get("Quantity"), errors="coerce")
        _cost_ps = (_raw_cost / abs(_qty_v)) if pd.notna(_raw_cost) and pd.notna(_qty_v) and abs(_qty_v) > 0 else None
        _net_cost = pd.to_numeric(_s.get("Net_Cost_Basis_Per_Share"), errors="coerce")
        _cum_prem = pd.to_numeric(_s.get("Cumulative_Premium_Collected"), errors="coerce")
        _cycle_cnt = _s.get("_cycle_count", 0) or 0

        _stock_parts = []
        if pd.notna(_spot_v):
            _stock_parts.append(f"Stock Price: ${_spot_v:.2f}")
        if _cost_ps and pd.notna(_cost_ps):
            _stock_parts.append(f"Purchase Cost/Share: ${_cost_ps:.2f}")
        if pd.notna(_net_cost):
            _stock_parts.append(f"Net Cost/Share: ${_net_cost:.2f}")
        if pd.notna(_qty_v):
            _stock_parts.append(f"Shares: {int(abs(_qty_v)):,}")
        if _stock_parts:
            lines.append(" | ".join(_stock_parts))

        # Equity state
        _eq = str(doctrine_row.get("Equity_Integrity", "") or "").upper() if doctrine_row is not None else ""
        if _eq == "BROKEN":
            lines.append("🔴 Equity State: BROKEN")
        elif _eq == "WEAK":
            lines.append("🟡 Equity State: WEAK")

        # Position Regime (lifecycle trajectory)
        _pr = str(doctrine_row.get("Position_Regime", "") or "") if doctrine_row is not None else ""
        if _pr and _pr not in ("NEUTRAL", ""):
            _pr_reason = str(doctrine_row.get("Position_Regime_Reason", "") or "") if doctrine_row is not None else ""
            _pr_ret = doctrine_row.get("Trajectory_Stock_Return") if doctrine_row is not None else None
            _pr_debits = doctrine_row.get("Trajectory_Consecutive_Debit_Rolls") if doctrine_row is not None else None
            _pr_eff = str(doctrine_row.get("Trajectory_Roll_Efficiency_Trend", "") or "") if doctrine_row is not None else ""
            _pr_parts = [f"Position Regime: {_pr}"]
            if _pr_ret is not None and not (isinstance(_pr_ret, float) and pd.isna(_pr_ret)):
                _pr_parts.append(f"Stock {float(_pr_ret):+.0%} since entry")
            if _pr_debits is not None and not (isinstance(_pr_debits, float) and pd.isna(_pr_debits)):
                _cd = int(float(_pr_debits))
                if _cd > 0:
                    _pr_parts.append(f"{_cd} consecutive debit rolls")
            if _pr_eff and _pr_eff not in ("nan", "None", ""):
                _pr_parts.append(f"Roll efficiency: {_pr_eff}")
            lines.append(f"🔄 {' · '.join(_pr_parts)}")

        # Trend / Momentum / Vol Regime / Basis Drift / Sector RS
        _trend_parts = []
        _td = str(doctrine_row.get("Trend_Regime", "") or "") if doctrine_row is not None else ""
        if _td:
            _trend_parts.append(f"Trend: {_td}")
        _mom = doctrine_row.get("Momentum_ROC20") if doctrine_row is not None else None
        if _mom is not None and not (isinstance(_mom, float) and pd.isna(_mom)):
            _trend_parts.append(f"Momentum: {float(_mom):.1%}")
        _hv_r = _s.get("HV_Rank") or _s.get("HV_20D")
        if _hv_r is not None and not (isinstance(_hv_r, float) and pd.isna(_hv_r)):
            try:
                _hv_f = float(_hv_r)
                _hv_str = f"{_hv_f:.1%}" if _hv_f <= 1.0 else f"{_hv_f:.1f}"
            except (TypeError, ValueError):
                _hv_str = str(_hv_r)
            _trend_parts.append(f"Vol Regime: HV {_hv_str}")
        _bd = card_metrics.get("basis_drift")
        if _bd is not None:
            _trend_parts.append(f"Basis Drift: {_bd:.1%}")
        _srs_v = str(doctrine_row.get("Sector_Relative_Strength", "") or "") if doctrine_row is not None else ""
        _srs_b = str(doctrine_row.get("Sector_Benchmark", "") or "") if doctrine_row is not None else ""
        if _srs_v:
            _trend_parts.append(f"Sector RS: {_srs_v} vs {_srs_b}")
        if _trend_parts:
            lines.append(" | ".join(_trend_parts))

        # Premium history
        if pd.notna(_cum_prem) and _cum_prem > 0:
            _hard = card_metrics.get("hard_stop")
            _hs_str = f" · Hard stop: ${_hard:.2f}" if _hard else ""
            lines.append(
                f"💰 {_cum_prem:.2f}/share collected across {int(_cycle_cnt)} cycles{_hs_str}"
            )

        # Last roll
        _last_roll = card_metrics.get("last_roll_credit")
        if _last_roll:
            lines.append(f"🔄 Last roll: {_last_roll}")

        # Basis drift from net cost
        if _bd is not None:
            lines.append(f"📍 Underlying drift from net cost: {_bd:.2%}")
        lines.append("")

    # ── Option Leg Details ──────────────────────────────────────────────────
    if not opt_legs.empty:
        for _, _leg in opt_legs.iterrows():
            _cp = _leg.get("Call/Put") or _leg.get("OptionType") or "?"
            _strike = pd.to_numeric(_leg.get("Strike"), errors="coerce")
            _exp = _leg.get("Expiration")
            _dte_l = pd.to_numeric(_leg.get("DTE"), errors="coerce")
            _last_l = pd.to_numeric(_leg.get("Last"), errors="coerce")
            _delta_l = pd.to_numeric(_leg.get("Delta"), errors="coerce")
            _qty_l = pd.to_numeric(_leg.get("Quantity"), errors="coerce")
            _dir = "Short" if (pd.notna(_qty_l) and _qty_l < 0) else "Long"
            _parts_l = [f"{_dir} {_cp}"]
            if pd.notna(_strike):
                _parts_l.append(f"${_strike:.1f}")
            if pd.notna(_exp):
                try:
                    _parts_l.append(f"exp {pd.to_datetime(_exp).strftime('%b %d')}")
                except Exception:
                    pass
            if pd.notna(_last_l):
                _parts_l.append(f"Last ${_last_l:.2f}")
            if pd.notna(_delta_l):
                _parts_l.append(f"Δ {_delta_l:.3f}")
            if pd.notna(_dte_l):
                _parts_l.append(f"DTE {int(_dte_l)}")
            lines.append(" · ".join(_parts_l))

    # ── Net Greeks ──────────────────────────────────────────────────────────
    if not opt_legs.empty:
        _nd = (pd.to_numeric(opt_legs["Delta"], errors="coerce") *
               pd.to_numeric(opt_legs["Quantity"], errors="coerce")).sum() * 100
        _nt = (pd.to_numeric(opt_legs["Theta"], errors="coerce") *
               pd.to_numeric(opt_legs["Quantity"], errors="coerce")).sum() * 100
        _nv = (pd.to_numeric(opt_legs["Vega"], errors="coerce") *
               pd.to_numeric(opt_legs["Quantity"], errors="coerce")).sum() * 100
        _ng = (pd.to_numeric(opt_legs["Gamma"], errors="coerce") *
               pd.to_numeric(opt_legs["Quantity"], errors="coerce")).sum() * 100
        lines.append(
            f"Net Δ: {_nd:+.1f} | Net θ/day: {_nt:+.2f} | "
            f"Net ν: {_nv:+.2f} | Net Γ: {_ng:+.4f}"
        )
        lines.append("")

    # ── Capital Efficiency + Hold EV ────────────────────────────────────────
    _ce = card_metrics.get("cap_eff")
    if _ce:
        _cap = _ce.get("capital_at_risk")
        _hor = _ce.get("horizon_days")
        _carry = _ce.get("theta_carry_30d")
        _gdrag = _ce.get("gamma_drag_30d")
        _hev = _ce.get("hold_ev_net")
        _eff_a = _ce.get("efficiency_ann")
        _mcost_d = _ce.get("margin_cost_daily")
        _mcost_h = _ce.get("margin_cost_horizon")
        _mcost_mo = (_mcost_d * 30) if _mcost_d else None
        _hor_label = f"{int(_hor)}d" if _hor else "—"
        lines.append("📈 Capital Efficiency + Hold EV")
        _ce_parts = []
        if _cap:
            _ce_parts.append(f"Capital at Risk: ${_cap:,.0f}")
        if _carry is not None:
            _ce_parts.append(f"θ Carry ({_hor_label}): ${_carry:,.0f}")
        if _gdrag is not None:
            _ce_parts.append(f"Gamma Drag ({_hor_label}): ${_gdrag:,.0f}")
        if _ce_parts:
            lines.append(" | ".join(_ce_parts))
        # Margin cost line
        if _mcost_d:
            lines.append(f"💰 Margin Cost: ${_mcost_d:.2f}/day (${_mcost_mo:,.0f}/month) @ 10.375%/yr")
        _ce2 = []
        if _hev is not None:
            _ce2.append(f"Hold EV ({_hor_label}): ${_hev:,.0f}")
        if _eff_a is not None:
            _ce2.append(f"Annualised Yield: {_eff_a:.1%}")
        if _hev is not None:
            _qual = "🔴 Negative" if _hev < 0 else ("✅ Strong" if (_eff_a or 0) >= 0.20 else "🟡 Moderate")
            _ce2.append(f"Carry Quality: {_qual}")
        if _ce2:
            lines.append(" | ".join(_ce2))
        lines.append("")

    # ── Recovery Path ───────────────────────────────────────────────────────
    _gap = card_metrics.get("gap")
    _hs = card_metrics.get("hard_stop")
    _gap_stop = card_metrics.get("gap_to_stop")
    _wk_prem = card_metrics.get("weekly_premium")
    _mo_prem = card_metrics.get("monthly_premium")
    _cycles = card_metrics.get("cycles_to_recover")
    _cum_p = card_metrics.get("cum_prem")
    _cycle_c = card_metrics.get("cycle_count")
    _margin_ps_mo = card_metrics.get("margin_ps_monthly")
    if _gap is not None:
        lines.append("📊 Recovery Path")
        _rp1 = [f"Gap to Breakeven: ${_gap:.2f}/share"]
        if _hs is not None:
            _cush = f" (+${_gap_stop:.2f} cushion)" if _gap_stop is not None else ""
            _rp1.append(f"Hard Stop: ${_hs:.2f}{_cush}")
        lines.append(" | ".join(_rp1))
        if _cum_p is not None and _cycle_c:
            lines.append(f"Collected to Date: ${_cum_p:.2f}/share ({int(_cycle_c)} cycles)")
        _rp2 = []
        if _mo_prem is not None:
            _rp2.append(f"IV-Implied Monthly: ~${_mo_prem:.2f}")
        if _margin_ps_mo is not None and _margin_ps_mo > 0:
            _net_mo = (_mo_prem or 0) - _margin_ps_mo
            _rp2.append(f"Margin Bleed: −${_margin_ps_mo:.2f}/mo")
            _rp2.append(f"Net Income: ~${_net_mo:.2f}/mo")
        if _cycles is not None:
            _rp2.append(f"Months to Close: ~{_cycles}" if _cycles < 999 else "Months to Close: ∞ (margin > premium)")
        if _rp2:
            lines.append(" | ".join(_rp2))
        lines.append("")

    # ── Story Check ─────────────────────────────────────────────────────────
    if doctrine_row is not None:
        _ts = str(doctrine_row.get("Thesis_State", "") or "")
        _eq2 = str(doctrine_row.get("Equity_Integrity", "") or "").upper()
        _story = f"🏗 Story Check — Thesis: {_ts}"
        if _eq2 == "BROKEN":
            _story += " ⚠️ equity BROKEN"
        lines.append(_story)

    # ── Forward Expectancy ──────────────────────────────────────────────────
    if doctrine_row is not None:
        _em = doctrine_row.get("Expected_Move_10D")
        _rmb = doctrine_row.get("Required_Move_Breakeven")
        _rm50 = doctrine_row.get("Required_Move_50pct")
        _pc = doctrine_row.get("Profit_Cushion")
        _pcr = doctrine_row.get("Profit_Cushion_Ratio")
        if _em is not None and not (isinstance(_em, float) and pd.isna(_em)):
            _em_f = float(_em)
            _fe_parts = [f"Expected Move (10D): {_em_f:.1f}"]

            # Check if this is an ITM winner (profit cushion populated, breakeven = 0)
            _has_cushion = (_pc is not None and not (isinstance(_pc, float) and pd.isna(_pc))
                           and float(_pc) > 0)
            if _has_cushion:
                _pc_f = float(_pc)
                _pcr_f = float(_pcr) if (_pcr is not None and not (isinstance(_pcr, float) and pd.isna(_pcr))) else 0
                _cush_icon = "🔴" if _pcr_f < 0.5 else ("🟡" if _pcr_f < 1.0 else "🟢")
                _cush_label = "Thin" if _pcr_f < 0.5 else ("Moderate" if _pcr_f < 1.0 else "Deep")
                _fe_parts.append(f"Profit Cushion: ${_pc_f:.1f} ({_pcr_f:.2f}× 10D move) {_cush_icon} {_cush_label}")
            else:
                if _rmb is not None and not (isinstance(_rmb, float) and pd.isna(_rmb)):
                    _rmb_f = float(_rmb)
                    _ratio = _rmb_f / _em_f if _em_f > 0 else 0
                    _fe_parts.append(f"Required move to breakeven: ${_rmb_f:.1f}")
                    _feas = "🟢" if _ratio < 0.50 else ("🟡" if _ratio < 1.0 else "🔴")
                    _fe_parts.append(f"{_feas} {_ratio:.2f}× Feasible")
                if _rm50 is not None and not (isinstance(_rm50, float) and pd.isna(_rm50)):
                    _rm50_f = float(_rm50)
                    _ratio50 = _rm50_f / _em_f if _em_f > 0 else 0
                    _fe_parts.append(f"50% Recovery: ${_rm50_f:.1f} ({_ratio50:.2f}×)")
            lines.append(f"Forward Expectancy (10D): {' · '.join(_fe_parts)}")

        # Theta bleed
        _theta_bleed = doctrine_row.get("Theta_Bleed_Daily_Pct")
        _theta_flag = doctrine_row.get("Theta_Opportunity_Cost_Flag", False)
        if _theta_bleed is not None and not (isinstance(_theta_bleed, float) and pd.isna(_theta_bleed)):
            _bleed_f = float(_theta_bleed)
            if _bleed_f > 0:
                _bleed_icon = "⚠️" if _theta_flag else "✅"
                _bleed_note = " — exceeds 3% flag threshold" if _theta_flag else ""
                lines.append(f"{_bleed_icon} Theta bleed: {_bleed_f:.1f}%/day of remaining premium{_bleed_note}")

        # Conviction state
        _conv = doctrine_row.get("Conviction_Status")
        _conv_streak = doctrine_row.get("Conviction_Fade_Days")
        if _conv is not None and str(_conv) not in ("", "nan", "None"):
            _conv_s = str(_conv)
            _conv_parts = [f"Conviction: {_conv_s}"]
            if _conv_streak is not None and not (isinstance(_conv_streak, float) and pd.isna(_conv_streak)):
                _cs = int(float(_conv_streak))
                if _cs > 0:
                    _conv_parts.append(f"(streak: {_cs}d fading)")
            _conv_icon = "🟢" if "STRENGTH" in _conv_s.upper() else ("🟡" if "STABLE" in _conv_s.upper() else "🔴")
            lines.append(f"{_conv_icon} {' '.join(_conv_parts)}")

    # ── Thesis Price Target (directional positions) ───────────────────────
    _DIRECTIONAL_STRUCTS = {"BUY_CALL", "LONG_CALL", "BUY_PUT", "LONG_PUT",
                            "LEAPS_CALL", "LEAPS_PUT"}
    _es_copy = str(entry_structure).upper()
    if _es_copy in _DIRECTIONAL_STRUCTS and doctrine_row is not None:
        _pt_entry = doctrine_row.get("Price_Target_Entry")
        _ul_copy  = doctrine_row.get("UL Last")
        _dte_entry = doctrine_row.get("DTE_Entry")
        _cp_copy   = str(doctrine_row.get("Call/Put") or "").upper()
        if _pt_entry is not None and not (isinstance(_pt_entry, float) and pd.isna(_pt_entry)):
            _pt_val = float(_pt_entry)
            if _pt_val > 0:
                _is_put_copy = "P" in _cp_copy
                _ul_f = float(_ul_copy) if _ul_copy is not None and not (isinstance(_ul_copy, float) and pd.isna(_ul_copy)) else 0
                if _ul_f > 0:
                    _dist = (_ul_f - _pt_val) if _is_put_copy else (_pt_val - _ul_f)
                    _dist_pct = _dist / _ul_f * 100
                    _dte_str = f", DTE at entry: {int(float(_dte_entry))}d" if _dte_entry is not None and not (isinstance(_dte_entry, float) and pd.isna(_dte_entry)) else ""
                    if (_is_put_copy and _ul_f <= _pt_val) or (not _is_put_copy and _ul_f >= _pt_val):
                        lines.append(f"🎯 Thesis target: **{_pt_val:.2f}** (IV-implied 1σ{_dte_str}) · AT/BEYOND TARGET — harvest signal")
                    else:
                        lines.append(f"📏 Thesis target: **{_pt_val:.2f}** (IV-implied 1σ{_dte_str}) · {_dist:.2f} ({_dist_pct:.1f}%) remaining")
    lines.append("")

    # ── Roll / MC Results ───────────────────────────────────────────────────
    _mc_w = card_metrics.get("mc_wait")
    _mc_a = card_metrics.get("mc_assign")
    if _mc_w:
        lines.append(
            f"🎲 MC Roll Wait-Cost: {_mc_w.get('verdict', '—')} | "
            f"P(breach): {_mc_w.get('breach', '—')} | "
            f"Median Δ: {_mc_w.get('median_delta', '—')}"
        )
    if _mc_a:
        lines.append(
            f"🎲 MC Assignment Risk: {_mc_a.get('urgency', '—')} | "
            f"P(assign): {_mc_a.get('p_assign', '—')} | "
            f"P(touch): {_mc_a.get('p_touch', '—')}"
        )

    # ── Pre-Execution Checklist ─────────────────────────────────────────────
    _chk = card_metrics.get("checklist")
    if _chk:
        lines.append("")
        lines.append("Pre-Execution Checklist:")
        for _icon, _label, _detail in _chk:
            lines.append(f"{_icon} {_label} — {_detail}")

    return "\n".join(lines)


def _compute_bw_capital_efficiency(
    net_cost_per_share: float,
    n_shares: int,
    theta_per_day: float,   # net theta in dollars/day (already ×100 from Greek)
    dte: float,
    hv: float | None,
    iv: float | None,
    spot: float,
    delta: float | None,
) -> dict:
    """
    Capital Efficiency + Hold EV for a BUY_WRITE / COVERED_CALL position.

    Capital at risk = net_cost_per_share × n_shares  (premium-reduced basis)

    Capital Efficiency:
        theta_carry_30d = theta_per_day × min(dte, 30)
        efficiency_pct  = theta_carry_30d / capital_at_risk
        annualised      = efficiency_pct × (365 / 30)

    Hold EV (30-day or to expiry, whichever is shorter):
        theta_carry   = theta_per_day × horizon_days            (certain)
        gamma_drag    = 0.5 × |Gamma_dollar| × (HV_daily_move)²  × horizon_days
                      where HV_daily_move = spot × HV / sqrt(252)
                      and   Gamma_dollar  = Gamma_per_share × n_shares × 100
                      (Gamma_per_share comes from the option leg)
        hold_ev_net   = theta_carry − gamma_drag

    Returns a dict of computed values (all floats, None if not computable).
    """
    import math as _math

    capital_at_risk = net_cost_per_share * n_shares if net_cost_per_share > 0 and n_shares > 0 else None
    horizon = min(float(dte), 30.0) if dte and dte > 0 else None

    # ── Capital Efficiency ────────────────────────────────────────────────────
    theta_carry_30d = theta_per_day * horizon if (horizon and theta_per_day) else None
    efficiency_pct  = (theta_carry_30d / capital_at_risk) if (theta_carry_30d and capital_at_risk) else None
    efficiency_ann  = efficiency_pct * (365.0 / (horizon or 30)) if efficiency_pct else None

    # ── Hold EV: Gamma Drag ───────────────────────────────────────────────────
    # Use HV as the realized-vol estimate (what actually moves the stock).
    # daily_move_1sd = spot × HV / sqrt(252)
    # gamma_drag_day = 0.5 × |net_gamma_dollar| × daily_move_1sd²
    # This is the Black-Scholes gamma P&L formula (Natenberg Ch.9).
    hv_f = float(hv) if hv and hv > 0 else None
    gamma_drag_30d = None
    gamma_drag_day = None
    if hv_f and spot and horizon:
        daily_move_1sd  = spot * hv_f / _math.sqrt(252)
        # net_gamma_dollar: caller passes net_g (already ×100×qty from Greek block)
        # We need gamma per dollar move² — net_g is in $ per 1pt move of underlying
        # Standard: gamma_pnl = 0.5 × gamma_shares × (Δspot)²
        # net_g here is already net_gamma × 100 × qty → that's "dollar gamma" per $1 move
        # Simplification: treat net_g magnitude as $-per-$1-move gamma
        # gamma_drag_day ≈ 0.5 × |net_g| × (daily_move_1sd)²  — directional drag only
        # (short gamma position loses on both up and down moves)
        gamma_drag_day = None   # set below once net_g is passed in

    # ── Margin Carry Cost ──────────────────────────────────────────────────
    # Fidelity 10.375% annualised — the real daily bleed on margined stock.
    # McMillan Ch.3: "The covered writer must earn at least the cost of carrying the stock."
    _MARGIN_RATE_DAILY = 0.10375 / 365  # ~0.0284% per day
    margin_cost_daily  = (net_cost_per_share * n_shares * _MARGIN_RATE_DAILY) if (net_cost_per_share > 0 and n_shares > 0) else None
    margin_cost_horizon = (margin_cost_daily * horizon) if (margin_cost_daily and horizon) else None
    margin_cost_per_share_daily = (net_cost_per_share * _MARGIN_RATE_DAILY) if (net_cost_per_share > 0) else None

    return {
        "capital_at_risk":   capital_at_risk,
        "horizon_days":      horizon,
        "theta_carry_30d":   theta_carry_30d,
        "efficiency_pct":    efficiency_pct,
        "efficiency_ann":    efficiency_ann,
        "gamma_drag_30d":    gamma_drag_30d,   # filled by caller with net_g
        "hold_ev_net":       None,             # filled by caller after gamma_drag
        "hv_daily_move_1sd": (spot * float(hv) / _math.sqrt(252)) if (hv and spot) else None,
        "margin_cost_daily":           margin_cost_daily,
        "margin_cost_horizon":         margin_cost_horizon,
        "margin_cost_per_share_daily": margin_cost_per_share_daily,
    }


def _fill_bw_hold_ev(ev_dict: dict, net_gamma_dollar: float, spot: float) -> dict:
    """
    Second-pass: fill gamma_drag_30d and hold_ev_net once net_g is known.
    net_gamma_dollar = net_g from Greek block (already ×100×qty, signed negative for short gamma).
    Hold EV = θ carry − Γ drag − margin cost  (all three real costs).
    """
    import math as _math
    hv_move = ev_dict.get("hv_daily_move_1sd")
    horizon = ev_dict.get("horizon_days")
    theta_carry = ev_dict.get("theta_carry_30d")
    margin_cost = ev_dict.get("margin_cost_horizon") or 0.0

    if hv_move and horizon and net_gamma_dollar is not None:
        # 0.5 × |gamma_dollar| × (1sd move)²  per day, × horizon days
        # net_gamma_dollar is negative for a short-gamma position; abs() gives drag magnitude
        gamma_drag_30d = 0.5 * abs(net_gamma_dollar) * (hv_move ** 2) * horizon
        ev_dict["gamma_drag_30d"] = gamma_drag_30d
        if theta_carry is not None:
            # For a BW: theta is positive (we collect), gamma drag + margin are costs
            ev_dict["hold_ev_net"] = theta_carry - gamma_drag_30d - margin_cost
    elif theta_carry is not None and margin_cost > 0:
        # No gamma data but margin cost is real — still include it
        ev_dict["hold_ev_net"] = theta_carry - margin_cost
    return ev_dict


def _evaluate_buyback_rationale(
    theta_per_day: float,       # net $/day (positive = collecting)
    gamma_drag_day: float | None,  # expected $/day gamma cost (positive magnitude)
    hv: float | None,           # realized vol (20d)
    iv: float | None,           # implied vol (live or 30d)
    hv_percentile: float | None,  # 0–1, where hv sits in own history
    iv_surface: str,            # CONTANGO / FLAT / BACKWARDATION
    adx: float | None,
    roc20: float | None,
    mom_velocity: str,          # MomentumVelocity_State
    delta: float | None,        # call delta (positive, 0–1)
    strike: float,
    spot: float,
    dte: float,
) -> dict:
    """
    Evaluate three independent conditions for buying back the short call.
    Each returns severity: NONE | WATCH | EVALUATE | ACT
    and a reason string.

    Condition 1 — Vol Collapse:
        Gamma drag has fallen below theta carry (position now earning net positive)
        AND HV percentile < 0.50 (vol genuinely retreating, not a daily blip).
        If both met → no longer short gamma at a loss → re-evaluate higher strike.

    Condition 2 — Breakout Regime:
        ADX > 20 AND ROC20 > 0 AND MomentumVelocity in (ACCELERATING, TRENDING).
        Being short a call in a confirmed breakout caps your upside structurally.
        Surface as EVALUATE; only ACT if delta > 0.80 (deep ITM with acceleration).

    Condition 3 — Gamma Dominance:
        gamma_drag_day > 2× theta_per_day → paying more in expected gamma cost
        than theta collected. Default-to-assignment logic breaks down here.
        Escalate based on ratio:
            1.5–2.0×  → WATCH
            2.0–3.0×  → EVALUATE
            >3.0×     → ACT

    Returns dict:
        {
          "c1": {"severity": str, "label": str, "reason": str},
          "c2": {"severity": str, "label": str, "reason": str},
          "c3": {"severity": str, "label": str, "reason": str},
          "top_severity": str,   # highest across all three
        }
    """
    NONE = "NONE"; WATCH = "WATCH"; EVALUATE = "EVALUATE"; ACT = "ACT"
    _rank = {NONE: 0, WATCH: 1, EVALUATE: 2, ACT: 3}

    # ── Condition 1: Vol Collapse ─────────────────────────────────────────────
    c1_sev = NONE
    if gamma_drag_day is not None and theta_per_day and theta_per_day > 0:
        if gamma_drag_day < theta_per_day:
            # Gamma drag now below theta — position is net positive
            if hv_percentile is not None and hv_percentile < 0.40:
                c1_sev = EVALUATE
                c1_reason = (
                    f"Gamma drag (${gamma_drag_day:.0f}/day) < θ carry (${theta_per_day:.0f}/day) — "
                    f"position is now net-positive on expected value. "
                    f"HV at {hv_percentile:.0%} percentile — vol retreating, not a blip. "
                    f"Consider buying back and resetting at higher strike to capture remaining theta "
                    f"at better risk/reward (Passarelli Ch.6: reset when vol edge improves)."
                )
            elif hv_percentile is not None and hv_percentile < 0.60:
                c1_sev = WATCH
                c1_reason = (
                    f"Gamma drag (${gamma_drag_day:.0f}/day) < θ carry (${theta_per_day:.0f}/day) — "
                    f"approaching net-positive. HV at {hv_percentile:.0%} percentile — "
                    f"vol declining but not yet definitively low. Monitor for 2 more sessions."
                )
            else:
                c1_sev = NONE
                _hv_pct_str = f"{hv_percentile:.0%}" if hv_percentile is not None else "N/A"
                c1_reason = (
                    f"Gamma drag (${gamma_drag_day:.0f}/day) < θ, but HV percentile={_hv_pct_str} — "
                    f"vol still elevated historically. Wait for HV to confirm retreat."
                )
        else:
            c1_sev = NONE
            c1_reason = (
                f"Gamma drag (${gamma_drag_day:.0f}/day) still exceeds θ carry (${theta_per_day:.0f}/day). "
                f"Buying back now locks in the inefficiency — wait for vol to retreat."
            )
    else:
        c1_reason = "Gamma drag unavailable — HV data missing."

    # ── Condition 2: Breakout Regime ──────────────────────────────────────────
    c2_sev = NONE
    _adx_f   = float(adx)   if adx   is not None else 0.0
    _roc20_f = float(roc20) if roc20 is not None else 0.0
    _delta_f = float(delta) if delta is not None else 0.0
    _mom_up  = mom_velocity.upper() in ("ACCELERATING", "TRENDING")

    _breakout_confirmed = (_adx_f > 20 and _roc20_f > 0 and _mom_up)
    _breakout_emerging  = (_adx_f > 17 and _roc20_f > 0 and mom_velocity.upper() == "REVERSING")

    if _breakout_confirmed:
        if _delta_f > 0.80:
            c2_sev = ACT
            c2_reason = (
                f"Breakout CONFIRMED: ADX={_adx_f:.0f}, ROC20={_roc20_f:+.1f}%, "
                f"MomentumVelocity={mom_velocity}. "
                f"Delta={_delta_f:.2f} — deep ITM, every $1 up costs almost $1 in capped upside. "
                f"Buy back now to restore convexity "
                f"(McMillan Ch.3: don't be short a call in a confirmed breakout with deep ITM delta)."
            )
        else:
            c2_sev = EVALUATE
            c2_reason = (
                f"Breakout CONFIRMED: ADX={_adx_f:.0f}, ROC20={_roc20_f:+.1f}%, "
                f"MomentumVelocity={mom_velocity}. "
                f"Being short this call caps upside structurally. Delta={_delta_f:.2f} — "
                f"not yet deep ITM but rising. Evaluate buyback vs. roll to higher strike. "
                f"Passarelli Ch.6: in a confirmed trend, the call becomes a liability not income."
            )
    elif _breakout_emerging:
        c2_sev = WATCH
        c2_reason = (
            f"Breakout EMERGING: ADX={_adx_f:.0f} (rising toward 20), ROC20={_roc20_f:+.1f}%, "
            f"MomentumVelocity={mom_velocity}. "
            f"Not confirmed yet — wait for ADX > 20 + MomentumVelocity to confirm before acting. "
            f"Monitor daily."
        )
    else:
        c2_reason = (
            f"No breakout: ADX={_adx_f:.0f}, ROC20={_roc20_f:+.1f}%, "
            f"MomentumVelocity={mom_velocity}. "
            f"Short call not truncating a confirmed trend — default to assignment."
        )

    # ── Condition 3: Gamma Dominance ──────────────────────────────────────────
    c3_sev = NONE
    if gamma_drag_day is not None and theta_per_day and theta_per_day > 0:
        ratio = gamma_drag_day / theta_per_day
        if ratio > 3.0:
            c3_sev = ACT
            c3_reason = (
                f"Gamma drag = {ratio:.1f}× theta carry — paying ${gamma_drag_day:.0f}/day "
                f"in expected gamma cost vs ${theta_per_day:.0f}/day collected. "
                f"Default-to-assignment assumption has broken down. "
                f"Buy back to stop the gamma bleed "
                f"(Natenberg Ch.9: when Γ cost > θ income, the position has no edge)."
            )
        elif ratio > 2.0:
            c3_sev = EVALUATE
            c3_reason = (
                f"Gamma drag = {ratio:.1f}× theta carry — paying ${gamma_drag_day:.0f}/day "
                f"vs ${theta_per_day:.0f}/day theta. "
                f"Position is net-negative on expected value. "
                f"Evaluate buyback — especially if HV remains elevated next session "
                f"(Natenberg Ch.9: short gamma at this ratio is structurally unprofitable)."
            )
        elif ratio > 1.5:
            c3_sev = WATCH
            c3_reason = (
                f"Gamma drag = {ratio:.1f}× theta carry (${gamma_drag_day:.0f}/day vs "
                f"${theta_per_day:.0f}/day). Elevated but not yet critical. "
                f"Watch: if HV holds or rises, ratio will cross 2× — reassess then."
            )
        else:
            c3_reason = (
                f"Gamma drag = {ratio:.1f}× theta — within normal range for an ITM covered call. "
                f"Assignment remains the rational default."
            )
    else:
        c3_reason = "Gamma drag unavailable — HV data missing."

    # ── Aggregate ─────────────────────────────────────────────────────────────
    top = max(
        _rank[c1_sev], _rank[c2_sev], _rank[c3_sev]
    )
    top_sev = [k for k, v in _rank.items() if v == top][0]

    return {
        "c1": {"severity": c1_sev, "label": "Vol Collapse",      "reason": c1_reason},
        "c2": {"severity": c2_sev, "label": "Breakout Regime",   "reason": c2_reason},
        "c3": {"severity": c3_sev, "label": "Gamma Dominance",   "reason": c3_reason},
        "top_severity": top_sev,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Ticker Aggregation Layer helpers
# ─────────────────────────────────────────────────────────────────────────────

def _exposure_regime_text(structure: str, net_t: float) -> str:
    """One-line exposure warning for the ticker-level banner."""
    mapping = {
        "BULL_VOL_LEVERED":      "Long vol + levered upside. Flat market bleeds straddle; vol crush hurts all legs. Needs trend + vol expansion.",
        "BEAR_VOL_LEVERED":      "Long vol + levered downside. Flat market bleeds straddle. Needs trend lower + vol expansion.",
        "STRADDLE_SYNTHETIC":    "Delta-neutral long vol. Needs a large move — either direction. Theta is the primary risk.",
        "STRADDLE_BULLISH_TILT": "Long vol with bullish delta tilt. Flat market hurts. Needs upside break or vol spike.",
        "STRADDLE_BEARISH_TILT": "Long vol with bearish delta tilt. Needs a drop or vol spike to profit.",
        "CALL_DIAGONAL":         "Diagonal: long LEAP + short near-dated call. Theta harvesting. Delta + limited vol exposure.",
        "BULL_HEDGE":            "Long LEAP call + protective put. Bounded downside, capped total cost.",
        "BEAR_HEDGE":            "Long LEAP put + speculative call. Downside conviction with upside optionality.",
        "PUT_DIAGONAL":          "Diagonal: long LEAP put + short near-dated put. Theta harvesting bearish structure.",
        "INCOME_WITH_LEGS":      "Income generator (BW/CC) with additional speculative legs. Monitor for delta conflicts between legs.",
        "MULTI_LEG_MIXED":       "Multiple open positions — check collective delta and vega exposures for conflicts.",
        "SINGLE_LEG":            "",
    }
    text = mapping.get(structure, "Multiple open positions on same underlying.")
    if text and net_t < -10:
        text += f"  (θ/day: {net_t:.1f} — time decay is active.)"
    return text


def _render_ticker_banner(df: pd.DataFrame, ticker: str, doctrine_by_trade: dict) -> None:
    """Ticker-level L2 underlying view. Rendered once before all trades on the same ticker.

    Three layers of insight:
      1. Combined Greek snapshot + decay regime (existing)
      2. Cross-leg conflict analysis: what happens to the combined position after each action
      3. Cumulative roll cost tracker: net income retained after debit rolls
    """
    ticker_rows = df[df["Underlying_Ticker"] == ticker]
    _qty = pd.to_numeric(ticker_rows.get("Quantity", pd.Series(dtype=float)), errors="coerce").fillna(1)

    net_d = (pd.to_numeric(ticker_rows["Delta"], errors="coerce") * _qty).sum() * 100
    net_v = (pd.to_numeric(ticker_rows["Vega"],  errors="coerce") * _qty).sum() * 100
    net_t = (pd.to_numeric(ticker_rows["Theta"], errors="coerce") * _qty).sum() * 100
    total_gl = pd.to_numeric(ticker_rows["$ Total G/L"], errors="coerce").sum()

    structure = "MULTI_LEG_MIXED"
    strat_mix = ""
    if "_Ticker_Structure_Class" in ticker_rows.columns:
        _sc = ticker_rows["_Ticker_Structure_Class"].dropna()
        if not _sc.empty:
            structure = str(_sc.iloc[0])
    if "_Ticker_Strategy_Mix" in ticker_rows.columns:
        _sm = ticker_rows["_Ticker_Strategy_Mix"].dropna()
        if not _sm.empty:
            strat_mix = str(_sm.iloc[0])
    if not strat_mix:
        for _col in ("Strategy_Name", "Strategy"):
            if _col in ticker_rows.columns:
                _fb = ticker_rows[_col].dropna().astype(str)
                _fb = _fb[_fb.str.strip() != ""]
                if not _fb.empty:
                    strat_mix = ", ".join(sorted(set(_fb.tolist())))
                    break
    _legs_line = f"Legs: `{strat_mix}`  \n" if strat_mix else ""

    trade_count = ticker_rows["TradeID"].nunique() if "TradeID" in ticker_rows.columns else "?"
    trade_ids = list(ticker_rows["TradeID"].unique() if "TradeID" in ticker_rows.columns else [])
    actions = [
        str(doctrine_by_trade[tid].get("Action", "?"))
        for tid in trade_ids if tid in doctrine_by_trade
    ]
    action_summary = ", ".join(sorted(set(actions))) if actions else "—"
    exposure_text  = _exposure_regime_text(structure, net_t)

    decay_regime = "NONE"
    if "_Structural_Decay_Regime" in ticker_rows.columns:
        _dr = ticker_rows["_Structural_Decay_Regime"].dropna()
        if not _dr.empty:
            decay_regime = str(_dr.iloc[0])

    _pnl_str = _pnl_color(total_gl)
    _exposure_line = f"⚠️ {exposure_text}  \n" if exposure_text else ""

    # ── Banner (existing logic) ───────────────────────────────────────────────
    if decay_regime == "STRUCTURAL_DECAY":
        st.error(
            f"🔴 **{ticker} — {trade_count} trades · {structure} · STRUCTURAL DECAY REGIME**  \n"
            f"Net Δ: **{net_d:+.0f}**  ·  Net ν: **{net_v:+.0f}**  ·  "
            f"Net θ/day: **{net_t:+.2f}**  ·  Combined G/L: {_pnl_str}  \n"
            f"{_legs_line}"
            f"🔴 **Silent bleed active:** long vol + chop + IV compression converging. "
            f"Low realized movement + falling IV = theta AND vega working against you simultaneously. "
            f"Review catalyst timeline — if no vol expansion expected near-term, consider reducing exposure.  \n"
            f"Individual doctrine actions: {action_summary}"
        )
    elif decay_regime == "DECAY_RISK":
        st.warning(
            f"⚠️ **{ticker} — {trade_count} trades · {structure} · DECAY RISK**  \n"
            f"Net Δ: **{net_d:+.0f}**  ·  Net ν: **{net_v:+.0f}**  ·  "
            f"Net θ/day: **{net_t:+.2f}**  ·  Combined G/L: {_pnl_str}  \n"
            f"{_legs_line}"
            f"{_exposure_line}"
            f"Individual doctrine actions: {action_summary}"
        )
    else:
        st.info(
            f"📦 **{ticker} — {trade_count} trades · {structure}**  \n"
            f"Net Δ: **{net_d:+.0f}**  ·  Net ν: **{net_v:+.0f}**  ·  "
            f"Net θ/day: **{net_t:+.2f}**  ·  Combined G/L: {_pnl_str}  \n"
            f"{_legs_line}"
            f"{_exposure_line}"
            f"Individual doctrine actions: {action_summary}"
        )

    # ── L2 Underlying View (only when 2+ trades on same ticker) ──────────────
    # This expander provides the three analytical layers the simple banner can't:
    #   A. Post-action delta impact: what does executing each doctrine do to net position?
    #   B. Cross-leg conflict check: do legs offset, conflict, or complement each other?
    #   C. Cumulative roll cost: how much net income is actually retained after debits?
    if trade_count and int(str(trade_count)) > 1:
        with st.expander(
            f"🔭 {ticker} Underlying View — position layers + post-action impact",
            expanded=False,
        ):
            st.caption(
                "**Natenberg Ch.11 / Passarelli Ch.6**: 'Managing multiple positions on the same "
                "underlying requires net Greek analysis — individual leg doctrine only tells part of the story. "
                "What matters is the combined exposure after all actions are executed.'"
            )

            # ── A. Per-trade Greek contribution + action impact ───────────────
            st.markdown("#### A. Individual legs — contribution to combined position")

            # Build per-trade summary
            _trade_rows = []
            _exit_delta_impact = 0.0  # cumulative delta removed by EXIT actions
            _exit_vega_impact  = 0.0

            for _tid in trade_ids:
                if _tid not in doctrine_by_trade:
                    continue
                _tr = ticker_rows[ticker_rows["TradeID"] == _tid]
                _dr = doctrine_by_trade[_tid]
                _tr_qty  = pd.to_numeric(_tr.get("Quantity", pd.Series(dtype=float)), errors="coerce").fillna(1)
                _tr_d    = (pd.to_numeric(_tr["Delta"], errors="coerce") * _tr_qty).sum() * 100
                _tr_v    = (pd.to_numeric(_tr["Vega"],  errors="coerce") * _tr_qty).sum() * 100
                _tr_t    = (pd.to_numeric(_tr["Theta"], errors="coerce") * _tr_qty).sum() * 100
                _tr_gl   = pd.to_numeric(_tr["$ Total G/L"], errors="coerce").sum()
                _tr_strat = str(_dr.get("Strategy", _dr.get("Strategy_Name", "?")))
                _tr_act  = str(_dr.get("Action", "?"))
                _tr_urg  = str(_dr.get("Urgency", ""))
                _act_badge = (
                    f"🔴 {_tr_act}" if _tr_act in ("EXIT", "ROLL") and _tr_urg in ("HIGH", "CRITICAL")
                    else f"🟡 {_tr_act}" if _tr_act == "ROLL"
                    else f"✅ {_tr_act}" if _tr_act == "HOLD"
                    else f"⚪ {_tr_act}"
                )
                _trade_rows.append({
                    "Strategy":  _tr_strat,
                    "Action":    _act_badge,
                    "Net Δ":     f"{_tr_d:+.0f}",
                    "Net ν":     f"{_tr_v:+.0f}",
                    "θ/day":     f"{_tr_t:+.2f}",
                    "G/L":       _pnl_color(_tr_gl),
                })
                # Accumulate EXIT impact (removing this leg's Greeks from combined)
                if _tr_act == "EXIT":
                    _exit_delta_impact += _tr_d
                    _exit_vega_impact  += _tr_v

            if _trade_rows:
                st.dataframe(
                    pd.DataFrame(_trade_rows),
                    hide_index=True,
                    width='stretch',
                )

            # ── B. Post-action delta shift ────────────────────────────────────
            st.markdown("#### B. Post-action exposure — what happens after executing doctrine")

            _has_exit = abs(_exit_delta_impact) > 0.1 or abs(_exit_vega_impact) > 0.1
            _post_d = net_d - _exit_delta_impact
            _post_v = net_v - _exit_vega_impact

            _bcol1, _bcol2, _bcol3 = st.columns(3)
            _bcol1.metric("Current Net Δ", f"{net_d:+.0f}")
            _bcol2.metric(
                "Net Δ after EXIT(s)",
                f"{_post_d:+.0f}",
                delta=f"{-_exit_delta_impact:+.0f} from EXIT",
                delta_color="off",
            )
            # Direction interpretation
            _dir_now  = "Bearish" if net_d < -10 else ("Bullish" if net_d > 10 else "Neutral")
            _dir_post = "Bearish" if _post_d < -10 else ("Bullish" if _post_d > 10 else "Neutral")
            _bcol3.metric(
                "Direction shift",
                f"{_dir_now} → {_dir_post}",
                delta=None,
            )

            # Narrative for the delta shift
            if _has_exit and _dir_now != _dir_post:
                st.warning(
                    f"**Direction reversal after EXIT**: executing doctrine flips combined {ticker} "
                    f"exposure from **{_dir_now}** ({net_d:+.0f}Δ) to **{_dir_post}** ({_post_d:+.0f}Δ).  \n"
                    f"Confirm this is the intended outcome — if the remaining leg (e.g. LONG_CALL HOLD) "
                    f"produces a directional bet you didn't plan, consider whether to adjust it simultaneously."
                )
            elif _has_exit:
                st.info(
                    f"After EXIT: combined {ticker} delta moves from **{net_d:+.0f}** → **{_post_d:+.0f}** "
                    f"(both {_dir_now.lower()}). Direction consistent — remaining legs aligned."
                )

            # Vega shift (long call stays long-vega; removing BW removes short-vega)
            _vcol1, _vcol2 = st.columns(2)
            _vcol1.metric("Current Net ν", f"{net_v:+.0f}")
            _vcol2.metric(
                "Net ν after EXIT(s)",
                f"{_post_v:+.0f}",
                delta=f"{-_exit_vega_impact:+.0f} from EXIT",
                delta_color="off",
            )
            if _has_exit and net_v < 0 and _post_v > 0:
                st.info(
                    f"**Vega flip after EXIT**: removing the BUY_WRITE's short-vega "
                    f"({_exit_vega_impact:+.0f}ν removed) shifts combined position from short-vol "
                    f"to long-vol ({_post_v:+.0f}ν). Any remaining LONG_CALL is now an unhedged long-vol bet."
                )

            # ── Cross-leg conflict check ─────────────────────────────────────
            st.markdown("#### C. Cross-leg conflict analysis")

            _has_income_leg   = any(
                str(doctrine_by_trade.get(t, {}).get("Strategy", "")).upper()
                in ("BUY_WRITE", "COVERED_CALL", "CSP")
                for t in trade_ids if t in doctrine_by_trade
            )
            _has_long_opt_leg = any(
                str(doctrine_by_trade.get(t, {}).get("Strategy", "")).upper()
                in ("LONG_CALL", "LEAPS_CALL", "LONG_PUT", "LEAPS_PUT", "BUY_CALL", "BUY_PUT")
                for t in trade_ids if t in doctrine_by_trade
            )

            if _has_income_leg and _has_long_opt_leg:
                # BUY_WRITE (short call, short vega) + LONG_CALL (long delta, long vega)
                # These partially offset. Surface the specific tensions.
                _conflicts = []
                _complements = []

                if net_v < 0:
                    # Net short vega: BUY_WRITE dominates
                    _conflicts.append(
                        f"**Vega conflict**: BUY_WRITE is short-vol (ν={net_v:+.0f} net) while "
                        f"LONG_CALL is long-vol. Short-vol leg dominates — an IV spike hurts the "
                        f"combined position despite the LONG_CALL."
                    )
                elif net_v > 0:
                    _complements.append(
                        f"**Vega aligned**: LONG_CALL's long-vol offsets BUY_WRITE's short-vol "
                        f"(net ν={net_v:+.0f}). A vol expansion benefits the combined position."
                    )

                if net_d < -20:
                    _conflicts.append(
                        f"**Delta conflict**: combined position is net bearish ({net_d:+.0f}Δ). "
                        f"The short call in BUY_WRITE is capping upside more than the LONG_CALL's "
                        f"long delta offsets. Stock must fall for the combined position to benefit."
                    )
                elif net_d > 20:
                    _complements.append(
                        f"**Delta aligned**: LONG_CALL adds enough long delta to keep combined "
                        f"position bullish ({net_d:+.0f}Δ) despite the BUY_WRITE short call."
                    )
                else:
                    _complements.append(
                        f"**Delta near-neutral** ({net_d:+.0f}Δ): BUY_WRITE short call and "
                        f"LONG_CALL long delta approximately offset. Position is directionally flat."
                    )

                # Partial hedge value: does LONG_CALL cushion BUY_WRITE assignment loss?
                _complements.append(
                    "**Assignment hedge**: LONG_CALL gains value if stock rallies above BUY_WRITE "
                    "strike — partially offsetting the capped upside from the short call. "
                    "Passarelli Ch.6: this is not a collar (same strike) but provides partial upside recovery."
                )

                if _conflicts:
                    st.warning("**Conflicts detected between legs:**")
                    for _c in _conflicts:
                        st.markdown(f"- {_c}")
                if _complements:
                    st.success("**Complementary interactions:**")
                    for _c in _complements:
                        st.markdown(f"- {_c}")
            else:
                st.caption("No income/long-option conflict — single strategy type on this ticker.")

            # ── D. Cumulative roll cost vs. income retained ───────────────────
            st.markdown("#### D. Income integrity — cumulative roll cost tracker")
            st.caption(
                "**Jabbour**: 'The key in determining whether repair is viable even if you roll for "
                "additional debit is to calculate the new breakeven and whether the stock can recover.' "
                "Track total income retained after all debit rolls — if roll costs approach collected "
                "premium, the strategy has consumed its own income."
            )

            # Aggregate across ALL income legs first, then render once
            _agg_cum_prem      = 0.0
            _agg_roll_debit    = 0.0
            _agg_roll_cnt      = 0
            _agg_next_roll_cost = float("nan")
            _found_income      = False
            _income_leg_labels = []

            for _tid in trade_ids:
                if _tid not in doctrine_by_trade:
                    continue
                _dr = doctrine_by_trade[_tid]
                _strat_up = str(_dr.get("Strategy", _dr.get("Strategy_Name", ""))).upper()
                if _strat_up not in ("BUY_WRITE", "COVERED_CALL", "CSP"):
                    continue

                _tr = ticker_rows[ticker_rows["TradeID"] == _tid]
                _stock_rows = _tr[_tr.get("AssetType", pd.Series()) == "STOCK"] if "AssetType" in _tr.columns else pd.DataFrame()

                _cum_prem = float("nan")
                _roll_net = float("nan")
                _roll_cnt = 0

                for _src in [_dr] + ([] if _stock_rows.empty else [_stock_rows.iloc[0]]):
                    if pd.isna(_cum_prem):
                        _v = pd.to_numeric(_src.get("Cumulative_Premium_Collected") if hasattr(_src, "get") else None, errors="coerce")
                        if pd.notna(_v): _cum_prem = float(_v)
                    if pd.isna(_roll_net):
                        _v = pd.to_numeric(_dr.get("Roll_Net_Credit"), errors="coerce")
                        if pd.notna(_v): _roll_net = float(_v)
                    _rc_v = pd.to_numeric(_dr.get("Roll_Count"), errors="coerce")
                    if pd.notna(_rc_v): _roll_cnt = int(_rc_v)

                _roll_debit_paid = max(0.0, -_roll_net) if pd.notna(_roll_net) else 0.0

                if pd.notna(_cum_prem) and _cum_prem > 0:
                    _found_income = True
                    _agg_cum_prem   += _cum_prem
                    _agg_roll_debit += _roll_debit_paid
                    _agg_roll_cnt   += _roll_cnt
                    _income_leg_labels.append(f"{_strat_up} ({_tid})")

                # Use roll candidate cost from the most complete income leg
                if pd.isna(_agg_next_roll_cost):
                    _cand1_raw = _dr.get("Roll_Candidate_1")
                    if _cand1_raw and str(_cand1_raw) not in ("", "nan", "None"):
                        try:
                            import json as _jc
                            _cd = _jc.loads(str(_cand1_raw)) if isinstance(_cand1_raw, str) else _cand1_raw
                            _cr = _cd.get("cost_to_roll", {})
                            if isinstance(_cr, str):
                                _cr = _jc.loads(_cr)
                            _np = abs(float(_cr.get("net_per_contract", 0) or 0)) / 100
                            if _np > 0:
                                _agg_next_roll_cost = _np
                        except Exception:
                            pass

            # Render aggregated Section D (once, not per-leg)
            if _found_income:
                _agg_net_retained = _agg_cum_prem - _agg_roll_debit
                _agg_erosion_pct  = (_agg_roll_debit / _agg_cum_prem * 100) if _agg_cum_prem > 0 else float("nan")
                if len(_income_leg_labels) > 1:
                    st.caption(f"Aggregated across {len(_income_leg_labels)} income legs: {', '.join(_income_leg_labels)}")
                _ic1, _ic2, _ic3, _ic4 = st.columns(4)
                _ic1.metric(
                    "Collected (gross)",
                    f"${_agg_cum_prem:.2f}/sh",
                    help="Total premium collected across all income legs + cycles",
                )
                _ic2.metric(
                    "Roll debits paid",
                    f"−${_agg_roll_debit:.2f}/sh" if _agg_roll_debit > 0 else "$0",
                    delta=f"{_agg_roll_cnt} roll{'s' if _agg_roll_cnt != 1 else ''}" if _agg_roll_cnt else None,
                    delta_color="off",
                )
                _ic3.metric(
                    "Net retained",
                    f"${_agg_net_retained:.2f}/sh",
                    delta=(f"−{_agg_erosion_pct:.0f}% eaten by rolls" if pd.notna(_agg_erosion_pct) and _agg_erosion_pct > 0 else None),
                    delta_color=("inverse" if pd.notna(_agg_erosion_pct) and _agg_erosion_pct > 50 else "off"),
                )
                if pd.notna(_agg_next_roll_cost):
                    _after_next = _agg_net_retained - _agg_next_roll_cost
                    _ic4.metric(
                        "After next roll",
                        f"${_after_next:.2f}/sh",
                        delta=f"−${_agg_next_roll_cost:.2f} debit",
                        delta_color="inverse" if _after_next < _agg_net_retained * 0.5 else "off",
                        help="Net retained if you pay another roll debit (from Roll_Candidate_1 cost)",
                    )
                    if _agg_cum_prem > 0:
                        _pct_after = _after_next / _agg_cum_prem * 100
                        if _pct_after < 30:
                            st.error(
                                f"⛔ **Roll cost gate**: another roll at −${_agg_next_roll_cost:.2f}/sh would leave "
                                f"only ${_after_next:.2f}/sh net retained "
                                f"({_pct_after:.0f}% of original ${_agg_cum_prem:.2f} collected).  \n"
                                f"**Jabbour**: 'When cumulative roll friction approaches the original premium, "
                                f"the strategy has consumed its own income — closing is preferable to rolling again.'"
                            )
                        elif _pct_after < 50:
                            st.warning(
                                f"⚠️ **Roll friction warning**: another roll leaves "
                                f"${_after_next:.2f}/sh ({_pct_after:.0f}% of original premium). "
                                f"Natenberg Ch.11: each additional roll erodes the income thesis — "
                                f"evaluate whether the remaining premium justifies extending risk."
                            )
                else:
                    _ic4.metric("After next roll", "—", help="Run pipeline to populate roll candidate costs")

            if not _found_income:
                st.caption("No BUY_WRITE/COVERED_CALL leg found — roll cost tracker not applicable.")


def _render_equity_state_panel(
    s,
    ei_state: str,
    ei_reason: str,
    ul_last: float,
    cost_basis: float,
) -> None:
    """Structured 📊 Equity State panel for BROKEN or WEAKENING stocks.

    Replaces the raw signal-string dump with a 6-metric breakdown:
    Trend, Momentum, Vol Regime, Drawdown, ATR, Sector RS.
    """
    import math

    def _safe(col, default=None):
        v = s.get(col)
        if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
            return default
        try:
            return float(v)
        except (ValueError, TypeError):
            return str(v) if v != "" else default

    # ── Trend ─────────────────────────────────────────────────────────────────
    ema20 = _safe("ema20_slope")
    ema50 = _safe("ema50_slope")
    if ema20 is not None and ema50 is not None:
        both_down = ema20 < 0 and ema50 < 0
        both_up   = ema20 > 0 and ema50 > 0
        if both_down:
            trend_label, trend_sub = "Down", f"EMA20↓({ema20:+.4f}), EMA50↓({ema50:+.4f})"
        elif both_up:
            trend_label, trend_sub = "Up",   f"EMA20↑({ema20:+.4f}), EMA50↑({ema50:+.4f})"
        else:
            trend_label, trend_sub = "Mixed", f"EMA20({ema20:+.4f}), EMA50({ema50:+.4f})"
    else:
        trend_label, trend_sub = "—", "No slope data"

    # ── Momentum ──────────────────────────────────────────────────────────────
    roc20 = _safe("roc_20")
    if roc20 is not None:
        mom_label = f"{roc20:+.1f}%"
        mom_sub   = "negative" if roc20 < 0 else "positive"
    else:
        mom_label, mom_sub = "—", "No ROC data"

    # ── Vol Regime ────────────────────────────────────────────────────────────
    hv_pct = _safe("hv_20d_percentile")
    if hv_pct is not None:
        hv_pct_display = int(hv_pct * 100 if hv_pct <= 1.0 else hv_pct)
        _hv_sfx = "st" if hv_pct_display % 100 not in (11,12,13) and hv_pct_display % 10 == 1 else \
                  "nd" if hv_pct_display % 100 not in (11,12,13) and hv_pct_display % 10 == 2 else \
                  "rd" if hv_pct_display % 100 not in (11,12,13) and hv_pct_display % 10 == 3 else "th"
        vol_label = f"HV {hv_pct_display}{_hv_sfx} pct"
        if hv_pct_display >= 75:
            vol_sub = "Elevated"
        elif hv_pct_display >= 50:
            vol_sub = "Moderate"
        else:
            vol_sub = "Low"
    else:
        vol_label, vol_sub = "—", "No HV data"

    # ── Basis Drift (renamed from "Drawdown" — label was misleading for positive drift) ──
    # Shows price drift from net cost basis. Positive = above basis (gain); negative = below (loss).
    # Sub-label: only show ei_state if the drift itself contributed to the state.
    # If WEAKENING was triggered by HV percentile (not drawdown), showing "WEAKENING" under
    # a +2.5% drift misleads — it implies the gain is the problem, which it is not.
    if ul_last and cost_basis and cost_basis > 0 and pd.notna(ul_last) and pd.notna(cost_basis):
        dd_pct   = (ul_last - cost_basis) / cost_basis * 100
        dd_label = f"{dd_pct:+.1f}%"
        # Sub-label logic:
        #   positive drift  → "above basis" (no structural concern from drift itself)
        #   negative drift
        #     < -20%        → BROKEN zone (S4c fired, ei_state contributed directly)
        #     < -15%        → structural warning
        #     < -10%        → early warning
        #     0 to -10%     → monitor
        if dd_pct >= 0:
            dd_sub = "above basis"
        elif dd_pct < -20:
            dd_sub = f"critical ({ei_state})"
        elif dd_pct < -15:
            dd_sub = "structural zone"
        elif dd_pct < -10:
            dd_sub = "early warning"
        else:
            dd_sub = "monitor"
    else:
        dd_label, dd_sub = "—", "No basis data"

    # ── ATR ───────────────────────────────────────────────────────────────────
    atr_slope = _safe("atr_slope")
    if atr_slope is not None:
        atr_label = "Expanding" if atr_slope > 0.05 else ("Contracting" if atr_slope < -0.05 else "Flat")
        atr_sub   = f"slope {atr_slope:+.3f}"
    else:
        atr_label, atr_sub = "—", "No ATR data"

    # ── Sector RS ─────────────────────────────────────────────────────────────
    sector_rs  = str(s.get("Sector_Relative_Strength") or "").strip() or None
    rs_z       = _safe("Sector_RS_ZScore")
    benchmark  = str(s.get("Sector_Benchmark") or "").strip() or "benchmark"
    if sector_rs:
        rs_label = sector_rs.replace("_", " ").title()
        rs_sub   = f"vs {benchmark} (z={rs_z:+.2f})" if rs_z is not None else f"vs {benchmark}"
    else:
        rs_label, rs_sub = "—", "No RS data"

    # ── Render ────────────────────────────────────────────────────────────────
    icon = "🔴" if ei_state == "BROKEN" else "🟡"
    header = (
        f"{icon} **Equity State: {ei_state}** — "
        + ("structural deterioration detected." if ei_state == "BROKEN"
           else "early warning signals present.")
    )

    if ei_state == "BROKEN":
        st.error(header)
    else:
        st.warning(header)

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Trend",       trend_label, trend_sub)
    c2.metric("Momentum",    mom_label,   mom_sub)
    c3.metric("Vol Regime",  vol_label,   vol_sub)
    c4.metric("Basis Drift", dd_label,    dd_sub)
    c5.metric("ATR",         atr_label,   atr_sub)
    c6.metric("Sector RS",   rs_label,    rs_sub)


def _build_exit_scenario_matrix(
    spot: float,
    strike: float,
    n_shares: int,
    call_last,
    call_dte,
    hv,
    call_delta,
) -> tuple:
    """
    Compute 5-scenario × 3-path exit matrix for a BUY_WRITE short call.

    Returns:
        rows       – list[dict] for st.dataframe (include hidden _delta_val key)
        sigma_move – 1σ dollar move by expiry (None if HV unavailable)
        ev_path_b  – probability-weighted expected proceeds for Path B (None if delta unavailable)
    """
    import math as _math

    # 1σ expected move: log-normal formula — e^(HV×sqrt(T)) − 1 × spot (multiplicative, not additive)
    # Arithmetic HV×sqrt(T)×spot is fine for short DTE (<90d) but produces negative prices at long DTE.
    # Log-normal: 1σ up = spot × exp(+HV×sqrt(T)), 1σ down = spot × exp(−HV×sqrt(T)).
    # This keeps prices positive by construction and is the correct model for option pricing.
    dte_years = max(float(call_dte), 1) / 252.0 if (call_dte is not None and not _math.isnan(float(call_dte or 0))) else None
    hv_f = float(hv) if (hv is not None and not _math.isnan(float(hv or 0)) and float(hv or 0) > 0) else None
    # sigma_move stored as dollar distance for the banner (asymmetric for display, use upside)
    if hv_f and dte_years:
        _sigma_up   = spot * (_math.exp(hv_f * _math.sqrt(dte_years)) - 1)
        _sigma_down = spot * (1 - _math.exp(-hv_f * _math.sqrt(dte_years)))
        sigma_move  = _sigma_up   # banner uses upside 1σ dollar for display
    else:
        _sigma_up = _sigma_down = sigma_move = None

    # Buyback cost (Path A fixed component): call_last × 100 × n_contracts
    n_contracts = max(1, round(n_shares / 100))
    call_last_f = float(call_last) if (call_last is not None and not _math.isnan(float(call_last or 0)) and float(call_last or 0) > 0) else None
    buyback_cost = call_last_f * 100 * n_contracts if call_last_f else 0.0
    path_a_net = spot * n_shares - buyback_cost

    # Define scenarios — log-normal prices (multiplicative) so no negative prices at any DTE
    if sigma_move and sigma_move > 0 and hv_f and dte_years:
        # Log-normal: price = spot × exp(±k × HV × sqrt(T))
        _p_m2 = spot * _math.exp(-2 * hv_f * _math.sqrt(dte_years))
        _p_m1 = spot * _math.exp(-1 * hv_f * _math.sqrt(dte_years))
        _p_p1 = spot * _math.exp(+1 * hv_f * _math.sqrt(dte_years))
        _p_p2 = spot * _math.exp(+2 * hv_f * _math.sqrt(dte_years))
        scenarios = [
            (f"−2σ", _p_m2, "Tail drop"),
            (f"−1σ", _p_m1, "Bear"),
            ("Flat",  spot,  "No move"),
            (f"+1σ", _p_p1, "Bull"),
            (f"+2σ", _p_p2, "Rally"),
        ]
        # Log-normal weights: same 68/95 rule approximation
        weights = [0.025, 0.135, 0.68, 0.135, 0.025]
    else:
        scenarios = [
            ("−20%", spot * 0.80, "Severe drop"),
            ("−10%", spot * 0.90, "Bear"),
            ("Flat",  spot,       "No move"),
            ("+5%",  spot * 1.05, "Bull"),
            ("+10%", spot * 1.10, "Rally"),
        ]
        weights = [0.05, 0.20, 0.50, 0.15, 0.10]

    rows = []
    ev_b_sum = 0.0
    ev_weights_sum = 0.0

    for (label, p, tag), w in zip(scenarios, weights):
        p = max(p, 0.01)  # floor at penny

        # Path B: let call expire, sell stock at scenario price
        if p >= strike:
            path_b_net = strike * n_shares   # assignment triggers
            path_b_str = f"Assign @${strike:.0f}"
        else:
            path_b_net = p * n_shares
            path_b_str = f"Sell @${p:.2f}"

        # Path C: let assignment happen
        if p >= strike:
            path_c_net = strike * n_shares
            path_c_str = f"${path_c_net:,.0f}"
        else:
            path_c_net = None
            path_c_str = "N/A (OTM)"

        delta_val = path_b_net - path_a_net
        delta_str = f"{delta_val:+,.0f}"

        rows.append({
            "Scenario": f"{label} ({tag})",
            "Price": f"${p:,.2f}",
            "Exit Now (A)": f"${path_a_net:,.0f}",
            "Wait/Expire (B)": f"${path_b_net:,.0f}",
            "B outcome": path_b_str,
            "Δ vs Exit": delta_str,
            "Assign (C)": path_c_str,
            "_delta_val": delta_val,
        })

        ev_b_sum += w * path_b_net
        ev_weights_sum += w

    # Expected-value row
    if ev_weights_sum > 0:
        ev_b = ev_b_sum / ev_weights_sum
        ev_delta = ev_b - path_a_net
        rows.append({
            "Scenario": "Expected Value",
            "Price": "—",
            "Exit Now (A)": f"${path_a_net:,.0f}",
            "Wait/Expire (B)": f"${ev_b:,.0f}",
            "B outcome": "probability-wtd",
            "Δ vs Exit": f"{ev_delta:+,.0f}",
            "Assign (C)": "—",
            "_delta_val": ev_delta,
        })
        ev_path_b = ev_b
    else:
        ev_path_b = None

    return rows, sigma_move, ev_path_b


def _color_scenario_row(row):
    """
    Row-level background color for the scenario matrix dataframe.
    Light pastel backgrounds so Streamlit's dark text stays readable.
    Green  = waiting (Path B) beats exiting now.
    Red    = exiting now is better.
    Blue   = Expected Value summary row.
    """
    try:
        raw = str(row.get("Δ vs Exit", "0"))
        num_str = raw.replace("$", "").replace(",", "").replace("+", "").strip()
        delta_val = float(num_str)
    except Exception:
        delta_val = 0.0

    if row.get("Scenario", "").startswith("Expected"):
        bg = "#cfe2f3"   # light blue — EV row
    elif delta_val > 0:
        bg = "#d9f2d9"   # light green — waiting is better
    elif delta_val < 0:
        bg = "#fde8e8"   # light red — exiting now is better
    else:
        bg = ""

    return [f"background-color: {bg}; color: #111111" if bg else "color: #111111"] * len(row)


def _render_position_cards(df: pd.DataFrame, show_stocks: bool, doctrine_df: pd.DataFrame | None = None, db_path: str | None = None):
    st.subheader("Active Positions")
    # Load roll candidates from DB (most recent market-hours run, not just latest CSV row)
    _db_roll_candidates: dict = _load_roll_candidates_from_db(db_path) if db_path else {}

    options_df = df[df["AssetType"] == "OPTION"].copy()
    options_df["DTE"] = _compute_dte(options_df["Expiration"])

    # Build doctrine lookup by TradeID (first row per trade — may be STOCK leg)
    doctrine_by_trade: dict = {}
    # Also keep option leg row per trade: carries IV_30D + Roll_Candidate_* reliably
    option_row_by_trade: dict = {}
    if doctrine_df is not None and not doctrine_df.empty:
        # For multi-leg strategies (BUY_WRITE, COVERED_CALL), MC columns are written
        # onto the OPTION leg row (Strike/DTE are valid there; stock leg has NaN).
        # Prioritise OPTION legs over STOCK legs so doctrine_by_trade carries live
        # MC data. Fall back to STOCK leg when no OPTION leg exists (STOCK_ONLY).
        if "AssetType" in doctrine_df.columns:
            _opt_first = doctrine_df.sort_values(
                "AssetType",
                key=lambda s: s.map({"OPTION": 0, "STOCK": 1}).fillna(2),
                kind="stable",
            )
        else:
            _opt_first = doctrine_df
        for _, dr in _opt_first.drop_duplicates("TradeID").iterrows():
            doctrine_by_trade[dr["TradeID"]] = dr
        if "AssetType" in doctrine_df.columns:
            opt_doc = doctrine_df[doctrine_df["AssetType"] == "OPTION"]
            # Prefer rows that have Roll_Candidate_1 populated (only generated during market hours)
            if "Roll_Candidate_1" in opt_doc.columns:
                _with_rc = opt_doc[opt_doc["Roll_Candidate_1"].notna()]
                _without_rc = opt_doc[opt_doc["Roll_Candidate_1"].isna()]
                # Index with_rc first so they win dedup; fall back to without_rc for other fields
                _opt_priority = pd.concat([_with_rc, _without_rc], ignore_index=True)
            else:
                _opt_priority = opt_doc
            for _, dr in _opt_priority.drop_duplicates("TradeID").iterrows():
                option_row_by_trade[dr["TradeID"]] = dr

    def _trade_sort_key(tid):
        group = df[df["TradeID"] == tid]
        opt_legs = group[group["AssetType"] == "OPTION"]
        # Prioritize CRITICAL doctrine actions
        if tid in doctrine_by_trade:
            dr = doctrine_by_trade[tid]
            urgency_rank = URGENCY_ORDER.get(str(dr.get("Urgency", "LOW")), 99)
            if str(dr.get("Action")) in ("EXIT", "ROLL"):
                urgency_rank = -1  # Force to top
        else:
            urgency_rank = 99
        if opt_legs.empty:
            return (1, urgency_rank, 9999)
        dte_vals = _compute_dte(opt_legs["Expiration"]).dropna()
        min_dte = float(dte_vals.min()) if not dte_vals.empty else 9999.0
        return (0, urgency_rank, min_dte)

    # Exclude empty/NaN TradeIDs — these are unmatched leftover stock rows that have
    # no real trade structure and should not inflate ticker banner counts or render cards.
    _all_trade_ids = [
        tid for tid in df["TradeID"].unique()
        if tid and str(tid).strip() not in ("", "nan", "None")
    ]
    sorted_trade_ids = sorted(_all_trade_ids, key=_trade_sort_key)

    # Pre-compute ticker trade counts for banner injection
    _ticker_of_trade: dict = {}
    for _tid in sorted_trade_ids:
        _tg = df[df["TradeID"] == _tid]
        if not _tg.empty:
            _ticker_of_trade[_tid] = _tg["Underlying_Ticker"].iloc[0]
    _ticker_trade_counts: dict = {}
    for _t in _ticker_of_trade.values():
        _ticker_trade_counts[_t] = _ticker_trade_counts.get(_t, 0) + 1
    _tickers_banner_rendered: set = set()

    for tid in sorted_trade_ids:
        group = df[df["TradeID"] == tid].copy()
        ticker = group["Underlying_Ticker"].iloc[0]

        # Render ticker-level banner once before the first trade card when 2+ trades share a ticker
        _this_ticker = _ticker_of_trade.get(tid, "")
        if (_ticker_trade_counts.get(_this_ticker, 1) > 1
                and _this_ticker not in _tickers_banner_rendered):
            _tickers_banner_rendered.add(_this_ticker)
            _render_ticker_banner(df, _this_ticker, doctrine_by_trade)
        entry_structure = (group["Entry_Structure"].dropna().iloc[0]
                           if "Entry_Structure" in group.columns and not group["Entry_Structure"].dropna().empty
                           else "UNKNOWN")
        asset_types = group["AssetType"].unique()

        # Determine if this trade is a stock-only (idle) position.
        # clean.py tags Strategy='STOCK_ONLY_IDLE'; Entry_Structure stays 'STOCK'.
        # Check BOTH so the condition works regardless of which column is populated.
        _strategy_vals = group["Strategy"].dropna().str.upper().unique() if "Strategy" in group.columns else []
        _is_idle_stock_trade = (
            "STOCK_ONLY_IDLE" in _strategy_vals
            or (entry_structure in ("UNKNOWN", "STOCK") and not any(t == "OPTION" for t in asset_types))
        )
        if not show_stocks and _is_idle_stock_trade:
            continue

        opt_legs = group[group["AssetType"] == "OPTION"].copy()
        opt_legs["DTE"] = _compute_dte(opt_legs["Expiration"])
        stock_legs = group[group["AssetType"] == "STOCK"]

        min_dte = float(opt_legs["DTE"].min()) if not opt_legs.empty else None
        dte_str = f"{int(min_dte)}d" if (min_dte is not None and pd.notna(min_dte)) else "—"

        total_gl_val = _best_gl_for_group(group)
        emoji = _strategy_emoji(entry_structure)

        # Doctrine badge for this trade
        doctrine_badge = ""
        doctrine_row = doctrine_by_trade.get(tid)
        if doctrine_row is not None:
            action  = str(doctrine_row.get("Action", "HOLD"))
            urgency = str(doctrine_row.get("Urgency", "LOW"))
            badge_emoji, _ = ACTION_BADGE.get(action, ("🔲", ""))
            # Degrade HOLD badge to amber when thesis is DEGRADED or BROKEN —
            # green HOLD next to a ⚠️ Thesis DEGRADED warning is contradictory.
            if action == "HOLD":
                _ts_for_badge = str(doctrine_row.get("Thesis_State", "") or "").upper()
                if _ts_for_badge == "BROKEN":
                    badge_emoji = "🔴"  # HOLD-but-BROKEN = red hold (exit pressure building)
                elif _ts_for_badge == "DEGRADED":
                    badge_emoji = "🟡"  # HOLD-but-DEGRADED = amber hold (monitor)
            doctrine_badge = f"  {badge_emoji} `{action}`" + (f" **{urgency}**" if urgency in ("CRITICAL", "HIGH") else "")

        # P/L freshness tag — broker CSV is stale; schwab_live means both UL and option prices refreshed.
        _price_src = ""
        _has_live_greeks = (
            "Greeks_Source" in group.columns
            and (group["Greeks_Source"] == "schwab_live").any()
        )
        if _has_live_greeks:
            _price_src = " `↻live`"
        elif "Price_Source" in group.columns:
            _src_vals = group["Price_Source"].dropna().unique()
            _src_str  = _src_vals[0] if len(_src_vals) == 1 else (
                "schwab_live" if any("schwab" in str(s) for s in _src_vals) else str(_src_vals[0])
            )
            if "schwab_live" in str(_src_str):
                _price_src = " `UL↻live`"
            elif "scan_cache" in str(_src_str):
                _price_src = " `UL↻scan`"
            # broker_csv: no tag — that's the default, tag absence = stale
        header = f"{emoji} **{ticker}** — {entry_structure}   `DTE: {dte_str}`   P/L: {_pnl_color(total_gl_val)}{_price_src}{doctrine_badge}"

        # Auto-expand on urgency, DTE, loss, or degraded/broken thesis
        _ts_for_expand = str(doctrine_row.get("Thesis_State", "") or "").upper() if doctrine_row is not None else ""
        auto_expand = bool(
            (min_dte is not None and pd.notna(min_dte) and float(min_dte) <= 14)
            or float(total_gl_val) < -500
            or (doctrine_row is not None and str(doctrine_row.get("Urgency", "LOW")) in ("CRITICAL", "HIGH"))
            or _ts_for_expand in ("DEGRADED", "BROKEN")
        )

        with st.expander(header, expanded=auto_expand):
            # ── Copy Card placeholder (filled at bottom after all metrics computed) ──
            _copy_placeholder = st.empty()
            _card_metrics: dict = {"_opt_row": option_row_by_trade.get(tid)}

            # Doctrine inline block (if available)
            if doctrine_row is not None and str(doctrine_row.get("Decision_State")) == "ACTIONABLE":
                rationale = str(doctrine_row.get("Rationale", ""))
                source = str(doctrine_row.get("Doctrine_Source", ""))
                action_str = str(doctrine_row.get("Action", ""))
                bg, _ = ACTION_BADGE.get(action_str, ("🔲", ""))
                _rationale_safe = rationale.replace("$", "\\$")
                _source_safe = source.replace("$", "\\$")
                # Split journey note (📖 prefix) from doctrine body for clean rendering
                if "\n" in _rationale_safe:
                    _jline, _body = _rationale_safe.split("\n", 1)
                    if _jline.startswith("📖"):
                        st.info(_jline)
                        _rationale_safe = _body.strip()
                st.warning(f"**Doctrine:** {_rationale_safe}  \n*Source: {_source_safe}*")

            # Scan conflict banner — surfaced separately from doctrine body for visibility.
            # engine.py sets Scan_Conflict (= the opposing scan bias) when the position
            # direction opposes the latest Step12 scan signal.  Shown on ALL cards so a
            # HOLD card that has a silent scan conflict is still prominently flagged.
            if doctrine_row is not None:
                _sc = str(doctrine_row.get("Scan_Conflict", "") or "").strip().upper()
                _sc_strat = str(doctrine_row.get("Strategy", "") or "").upper()
                if _sc in ("BEARISH", "BULLISH", "MIXED"):
                    _pos_bull_2 = (
                        "LONG_CALL"    in _sc_strat
                        or "LEAP"      in _sc_strat
                        or "BUY_WRITE" in _sc_strat
                        or "COVERED_CALL" in _sc_strat
                    )
                    _pos_bear_2 = "LONG_PUT" in _sc_strat
                    if _sc == "MIXED":
                        st.info(
                            f"📡 **Scan note:** scan engine has BOTH bullish and bearish candidates on "
                            f"**{ticker}** — cross-signal ambiguity. Confirm your intended direction."
                        )
                    else:
                        _conflict_dir = "bullish" if _pos_bull_2 else ("bearish" if _pos_bear_2 else "directional")
                        st.error(
                            f"⚡ **Cross-signal conflict:** scan engine is currently **{_sc}** on "
                            f"**{ticker}** — this position is **{_conflict_dir}**.  \n"
                            f"The two systems disagree. Verify thesis is still intact before holding "
                            f"(McMillan Ch.4: don't hold a directional position against the trend)."
                        )

            # Drift State Strip — surfaces DriftEngine outputs per position card
            if doctrine_row is not None:
                _da  = str(doctrine_row.get("Drift_Action",  "") or "").strip()
                _ss  = str(doctrine_row.get("Signal_State",  "") or "").strip()
                _ds  = str(doctrine_row.get("Data_State",    "") or "").strip()
                _rs  = str(doctrine_row.get("Regime_State",  "") or "").strip()
                _dd  = str(doctrine_row.get("Drift_Direction","") or "").strip()
                _dm  = str(doctrine_row.get("Drift_Magnitude","") or "").strip()
                _dp  = str(doctrine_row.get("Drift_Persistence","") or "").strip()
                _rp   = doctrine_row.get("ROC_Persist_3D")
                _d3   = doctrine_row.get("Delta_ROC_3D")
                _v3   = doctrine_row.get("Vega_ROC_3D")
                _iv3  = doctrine_row.get("IV_ROC_3D")
                _ivhv = doctrine_row.get("IV_vs_HV_Gap")
                _ivpct= doctrine_row.get("IV_Percentile")

                # Vol-state fields live on the OPTION leg row, not the STOCK leg row.
                # For BUY_WRITE/COVERED_CALL, doctrine_by_trade holds the STOCK leg
                # (first after drop_duplicates). Prefer option_row_by_trade for IV fields.
                _opt_row = option_row_by_trade.get(tid)
                def _opt_get(field, fallback):
                    """Return option row value if doctrine row has NaN/None for this field."""
                    v = fallback
                    if v is None or (isinstance(v, float) and pd.isna(v)) or str(v) in ("nan","None",""):
                        if _opt_row is not None:
                            v = _opt_row.get(field, fallback)
                    return v

                _iv3  = _opt_get("IV_ROC_3D",    _iv3)
                _ivhv = _opt_get("IV_vs_HV_Gap", _ivhv)
                _ivpct= _opt_get("IV_Percentile", _ivpct)
                _rp   = _opt_get("ROC_Persist_3D", _rp)
                _d3   = _opt_get("Delta_ROC_3D",  _d3)
                _v3   = _opt_get("Vega_ROC_3D",   _v3)

                # Fallback to 1D window when 3D is null (new positions with < 3 days history).
                # Track which window is active so the label can reflect it.
                def _is_null(v):
                    return v is None or (isinstance(v, float) and pd.isna(v)) or str(v) in ("nan","None","")

                _g3   = _opt_get("Gamma_ROC_3D", doctrine_row.get("Gamma_ROC_3D"))

                _roc_window = "3D"
                if _is_null(_d3) and _is_null(_v3) and _is_null(_iv3) and _is_null(_g3):
                    _d3_1  = _opt_get("Delta_ROC_1D", doctrine_row.get("Delta_ROC_1D"))
                    _v3_1  = _opt_get("Vega_ROC_1D",  doctrine_row.get("Vega_ROC_1D"))
                    _iv3_1 = _opt_get("IV_ROC_1D",    doctrine_row.get("IV_ROC_1D"))
                    _g3_1  = _opt_get("Gamma_ROC_1D", doctrine_row.get("Gamma_ROC_1D"))
                    if not (_is_null(_d3_1) and _is_null(_v3_1) and _is_null(_iv3_1) and _is_null(_g3_1)):
                        _d3, _v3, _iv3, _g3 = _d3_1, _v3_1, _iv3_1, _g3_1
                        _roc_window = "1D"
                # IV_Now: always from option row for BW/CC
                _iv_now_src = _opt_row if _opt_row is not None else doctrine_row

                # Only render if we have at least some meaningful state
                _has_drift_data = any(v not in ("", "nan", "None") for v in [_da, _ss, _ds, _rs])
                if _has_drift_data:
                    # Drift Action badge
                    _da_emoji = {
                        "NO_ACTION": "✅", "REVALIDATE": "🔄", "TRIM_ONLY": "✂️",
                        "EXIT": "🚨", "QUARANTINE": "🔒", "HARD_HALT": "⛔", "FORCE_EXIT": "💀"
                    }.get(_da, "❓")
                    _da_display = f"{_da_emoji} `{_da}`" if _da else "—"

                    # Signal State coloring
                    _ss_color = {"VALID": "green", "DEGRADED": "orange", "VIOLATED": "red"}.get(_ss, "gray")
                    _ss_display = f":{_ss_color}[**{_ss}**]" if _ss else "—"

                    # Data State coloring
                    _ds_color = {"FRESH": "green", "STALE": "orange", "ORPHANED": "red"}.get(_ds, "gray")
                    _ds_display = f":{_ds_color}[{_ds}]" if _ds else "—"

                    # Regime State
                    _rs_color = {"STABLE": "green", "STRESSED": "orange", "HALTED": "red"}.get(_rs, "gray")
                    _rs_display = f":{_rs_color}[{_rs}]" if _rs else "—"

                    # Direction + magnitude
                    _dir_arrow = {"Up": "↑", "Down": "↓", "Flat": "→"}.get(_dd, "")
                    _traj = f"{_dir_arrow} {_dd} / {_dm}" if _dd else "—"
                    if _dp and _dp not in ("", "nan", "None"):
                        _traj += f" ({_dp})"

                    with st.expander("📊 Drift State", expanded=(_da not in ("", "NO_ACTION"))):
                        dc1, dc2, dc3, dc4 = st.columns(4)
                        dc1.metric("Drift Action", _da_display if _da else "NO_ACTION",
                                   help="DriftEngine authoritative action. Risk may only be reduced, never increased.")
                        dc2.markdown(f"**Signal**  \n{_ss_display}",
                                     help="Greek ROC + PCS drift state. DEGRADED=monitor, VIOLATED=act.")
                        dc3.markdown(f"**Data**  \n{_ds_display}  \n**Regime**  \n{_rs_display}",
                                     help="Data freshness and market regime from stress detector.")
                        dc4.markdown(f"**Greek Drift**  \n`{_traj}`",
                                     help="Direction/magnitude/persistence of Greek drift (delta ROC, vega ROC) — not stock price direction.")

                        # Bridging note: explain the apparent contradiction when drift says NO_ACTION
                        # but the main doctrine urgency is HIGH/CRITICAL.
                        # These two systems measure different dimensions:
                        #   DriftEngine = Greek ROC stability (is the position drifting structurally?)
                        #   Doctrine urgency = holistic action signal (gamma ratio, equity state, MC)
                        if _da == "NO_ACTION" and urgency in ("HIGH", "CRITICAL"):
                            st.caption(
                                f"ℹ️ **Drift ✅ NO_ACTION ≠ no urgency.** "
                                f"DriftEngine monitors Greek ROC stability — it sees no structural Greek drift "
                                f"(signal valid, regime stable). The **{urgency}** doctrine urgency above is driven by "
                                f"separate signals (gamma ratio vs theta, equity integrity, MC forward breach probability) "
                                f"that operate outside the drift engine's scope. Both assessments can be simultaneously correct: "
                                f"Greeks are stable *now*, but the structural position is deteriorating."
                            )

                        # Vol State row — IV_Now, IV_ROC_3D, persist, IV_vs_HV_Gap, IV_Percentile
                        def _fmt_roc(val, bad_below, good_above=None):
                            """Color a ROC float: red=deteriorating, green=recovering, plain=neutral."""
                            try:
                                f = float(val)
                                if f < bad_below:
                                    return f":red[`{f:+.2f}`]"
                                if good_above is not None and f > good_above:
                                    return f":green[`{f:+.2f}`]"
                                return f"`{f:+.2f}`"
                            except (ValueError, TypeError):
                                return "—"

                        _iv_now_raw = _iv_now_src.get("IV_Now")
                        _vol_parts = []

                        # IV_Now
                        try:
                            _iv_now_f = float(_iv_now_raw)
                            _vol_parts.append(f"IV Now: **{_iv_now_f:.1%}**")
                        except (ValueError, TypeError):
                            pass

                        # IV ROC (window label reflects actual data used — 1D for new positions)
                        if _iv3 is not None and str(_iv3) not in ("nan", "None", ""):
                            _roc_str = _fmt_roc(_iv3, bad_below=-0.10, good_above=0.05)
                            _persist_sfx = (f" Persist: **{int(float(_rp))}d**"
                                            if _rp is not None and str(_rp) not in ("nan","None","") else "")
                            _vol_parts.append(f"IV ROC {_roc_window}: {_roc_str}{_persist_sfx}")

                        # IV vs HV Gap
                        if _ivhv is not None and str(_ivhv) not in ("nan", "None", ""):
                            try:
                                _gap_f = float(_ivhv)
                                # positive gap = IV premium over realised = selling edge (green for sellers)
                                # negative gap = IV crush (red for long vol, green for short)
                                _gap_color = ":green[" if _gap_f > 0 else ":red["
                                _vol_parts.append(f"IV vs HV: {_gap_color}**{_gap_f:+.1%}**]")
                            except (ValueError, TypeError):
                                pass

                        # IV Percentile
                        if _ivpct is not None and str(_ivpct) not in ("nan", "None", ""):
                            try:
                                _pct_f = float(_ivpct)
                                # Low percentile = cheap vol (good for buyers, bad for sellers)
                                _pct_color = (":green[" if _pct_f <= 25
                                              else (":red[" if _pct_f >= 75 else ""))
                                _pct_close = "]" if _pct_color else ""
                                _vol_parts.append(f"Percentile: {_pct_color}**{_pct_f:.0f}%**{_pct_close}")
                            except (ValueError, TypeError):
                                pass

                        if _vol_parts:
                            st.caption("📊 Vol State:  " + "  ·  ".join(_vol_parts))

                        # Greek ROC strip (Delta, Vega, Gamma) — window label reflects actual data used
                        # Gamma ROC is direction-aware: rising = bad for short-gamma (BW/CC/CSP),
                        # falling = bad for long-gamma (LONG_CALL/PUT/LEAP).
                        _SHORT_GAMMA_STRUCTS = {"BUY_WRITE", "COVERED_CALL", "CSP"}
                        _LONG_GAMMA_STRUCTS  = {"BUY_CALL", "LONG_CALL", "BUY_PUT", "LONG_PUT",
                                                 "LEAPS_CALL", "LEAPS_PUT"}
                        _es_upper = str(entry_structure).upper()
                        _is_short_gamma = _es_upper in _SHORT_GAMMA_STRUCTS
                        _is_long_gamma  = _es_upper in _LONG_GAMMA_STRUCTS

                        _roc_parts = []
                        _window_note = " *(1d — 3d accumulating)*" if _roc_window == "1D" else ""

                        for label, val in [(f"Δ ROC {_roc_window}", _d3),
                                           (f"ν ROC {_roc_window}", _v3)]:
                            if val is not None and str(val) not in ("nan", "None", ""):
                                _roc_parts.append(
                                    f"{label}: {_fmt_roc(val, bad_below=-0.10, good_above=0.05)}"
                                )

                        # Gamma ROC — only render if non-null; coloring is structure-dependent
                        if _g3 is not None and str(_g3) not in ("nan", "None", ""):
                            try:
                                _g3_f = float(_g3)
                                if _is_short_gamma:
                                    # Rising gamma = expanding risk for short-gamma positions
                                    if _g3_f > 0.50:
                                        _g3_str = f":red[`{_g3_f:+.2f}`]"
                                    elif _g3_f > 0.25:
                                        _g3_str = f":orange[`{_g3_f:+.2f}`]"
                                    else:
                                        _g3_str = f"`{_g3_f:+.2f}`"
                                elif _is_long_gamma:
                                    # Falling gamma = sensitivity eroding for long-gamma positions
                                    if _g3_f < -0.50:
                                        _g3_str = f":red[`{_g3_f:+.2f}`]"
                                    elif _g3_f < -0.25:
                                        _g3_str = f":orange[`{_g3_f:+.2f}`]"
                                    else:
                                        _g3_str = f"`{_g3_f:+.2f}`"
                                else:
                                    # Unknown structure — neutral display
                                    _g3_str = f"`{_g3_f:+.2f}`"
                                _roc_parts.append(f"Γ ROC {_roc_window}: {_g3_str}")
                            except (ValueError, TypeError):
                                pass

                        if _roc_parts:
                            st.caption(f"Greek ROC{_window_note}:  " + "  ·  ".join(_roc_parts))
                        elif _da == "NO_ACTION" and _ss == "VALID" and not _vol_parts:
                            st.caption("_ROC history accumulating — will populate after multiple engine runs._")

                        # Entry Displacement — t_now minus t₀ (freeze anchor)
                        # Shows how far each metric has moved since trade entry.
                        # Complements ROC (rolling behavior) with origin-aware deviation.
                        _ed_delta = doctrine_row.get("Delta_Drift_Structural")
                        _ed_vega  = doctrine_row.get("Vega_Drift_Structural")
                        _ed_iv    = doctrine_row.get("IV_Drift_Structural")
                        _ed_price = doctrine_row.get("Price_Drift_Structural")

                        def _fmt_displacement(val, *, pct=False, flip_bad=False):
                            """Color entry displacement: green=moved with thesis, red=against."""
                            try:
                                f = float(val)
                                import math
                                if math.isnan(f) or math.isinf(f):
                                    return None
                                if f == 0:
                                    return "`±0`"
                                sign_good = f > 0 if not flip_bad else f < 0
                                color = "green" if sign_good else "red"
                                fmt = f"{f:+.1%}" if pct else f"{f:+.3f}"
                                return f":{color}[`{fmt}`]"
                            except (ValueError, TypeError):
                                return None

                        _ed_parts = []
                        _delta_e = _fmt_displacement(_ed_delta)
                        if _delta_e:
                            _ed_parts.append(f"Δ from entry: {_delta_e}")
                        _vega_e = _fmt_displacement(_ed_vega)
                        if _vega_e:
                            _ed_parts.append(f"ν from entry: {_vega_e}")
                        _iv_e = _fmt_displacement(_ed_iv, pct=True)
                        if _iv_e:
                            _ed_parts.append(f"IV from entry: {_iv_e}")
                        _price_e = _fmt_displacement(_ed_price)
                        if _price_e:
                            _ed_parts.append(f"UL from entry: {_price_e}")

                        if _ed_parts:
                            st.caption("📐 Entry displacement:  " + "  ·  ".join(_ed_parts))

                        # Structural Decay Regime label
                        _sdr = str(doctrine_row.get("_Structural_Decay_Regime", "") or "").strip()
                        if _sdr == "STRUCTURAL_DECAY":
                            st.markdown(
                                "🔴 **Structural Decay Regime** — chop + IV compression + "
                                "long vol converging. Silent bleed: theta AND vega decaying simultaneously."
                            )
                        elif _sdr == "DECAY_RISK":
                            st.markdown(
                                "⚠️ **Decay Risk** — 2 structural decay signals converging. "
                                "Monitor for IV compression + chop persistence."
                            )

            # Stock context — full buy-write cost picture
            if not stock_legs.empty:
                s = stock_legs.iloc[0]
                ul_last    = pd.to_numeric(s.get("UL Last"), errors="coerce")
                basis_total = pd.to_numeric(s.get("Basis"), errors="coerce")
                qty_shares  = pd.to_numeric(s.get("Quantity"), errors="coerce")

                # Original purchase cost/share from Fidelity Basis field
                cost_per_share = (basis_total / qty_shares) if (pd.notna(basis_total) and pd.notna(qty_shares) and qty_shares != 0) else None

                # Net cost after cumulative premium collected (from premium_ledger)
                net_cost      = pd.to_numeric(s.get("Net_Cost_Basis_Per_Share"), errors="coerce")
                cum_premium   = pd.to_numeric(s.get("Cumulative_Premium_Collected"), errors="coerce")
                gross_premium = pd.to_numeric(s.get("Gross_Premium_Collected"), errors="coerce")
                total_close   = pd.to_numeric(s.get("Total_Close_Cost"), errors="coerce")
                has_debit_rolls = bool(s.get("Has_Debit_Rolls", False))
                cycle_count   = s.get("_cycle_count", None)

                # Use net cost for drift if available, else fall back to raw cost
                effective_for_drift = net_cost if (pd.notna(net_cost) and net_cost > 0) else cost_per_share

                drift_str = ""
                if pd.notna(ul_last) and effective_for_drift and effective_for_drift > 0:
                    drift_pct = (ul_last - effective_for_drift) / effective_for_drift
                    drift_str = f"{drift_pct:+.2%}"

                sc1, sc2, sc3, sc4 = st.columns(4)
                sc1.metric("Stock Price", f"${ul_last:.2f}" if pd.notna(ul_last) else "—")
                sc2.metric(
                    "Purchase Cost/Share",
                    f"${cost_per_share:.2f}" if cost_per_share else "—"
                )
                sc3.metric(
                    "Net Cost/Share",
                    f"${net_cost:.2f}" if pd.notna(net_cost) and net_cost > 0 else "—",
                    delta="after premiums",
                    delta_color="off",
                )
                sc4.metric("Shares", f"{int(qty_shares):,}" if pd.notna(qty_shares) else "—")

                # Equity Integrity State — structured panel for BROKEN/WEAKENING
                _ei_state  = str(s.get("Equity_Integrity_State", "") or "").strip()
                _ei_reason = str(s.get("Equity_Integrity_Reason", "") or "").strip()
                if _ei_state in ("BROKEN", "WEAKENING"):
                    _render_equity_state_panel(s, _ei_state, _ei_reason, ul_last, effective_for_drift)
                # HEALTHY: no banner — clean state, no noise added

                # Position Regime badge — lifecycle trajectory classification
                _pos_regime = str(s.get("Position_Regime", "") or "").strip()
                _pos_regime_reason = str(s.get("Position_Regime_Reason", "") or "").strip()
                if _pos_regime == "TRENDING_CHASE":
                    _chase_ret = s.get("Trajectory_Stock_Return")
                    _chase_debits = s.get("Trajectory_Consecutive_Debit_Rolls")
                    _chase_detail = "Stock outrunning strike cadence"
                    if _chase_ret is not None and not (isinstance(_chase_ret, float) and pd.isna(_chase_ret)):
                        _chase_detail += f" · Stock {float(_chase_ret):+.0%} since entry"
                    if _chase_debits is not None and not (isinstance(_chase_debits, float) and pd.isna(_chase_debits)):
                        _cd_int = int(float(_chase_debits))
                        if _cd_int > 0:
                            _chase_detail += f" · {_cd_int} consecutive debit roll(s)"
                    st.warning(f"TRENDING_CHASE — {_chase_detail}")
                elif _pos_regime == "SIDEWAYS_INCOME":
                    st.success("SIDEWAYS_INCOME — income strategy working as designed")
                elif _pos_regime == "RECOVERY_GRIND":
                    st.info("RECOVERY_GRIND — underwater but recovering")
                elif _pos_regime == "MEAN_REVERSION":
                    st.info("MEAN_REVERSION — classic income cycle, oscillating around entry")
                # NEUTRAL: no badge — default/insufficient data

                # Winner Lifecycle badge (pyramid progression for long option positions)
                _winner_lc = str(s.get("Winner_Lifecycle", "") or "").strip()
                _pyramid_t_raw = s.get("Pyramid_Tier")
                _pyramid_t_i = int(float(_pyramid_t_raw)) if _pyramid_t_raw and str(_pyramid_t_raw) not in ("nan", "None", "N/A", "") else None
                if _winner_lc and _winner_lc not in ("N/A", "", "THESIS_UNPROVEN"):
                    _tier_str = f" | Pyramid Tier {_pyramid_t_i}/3" if _pyramid_t_i is not None else ""
                    if _winner_lc == "THESIS_CONFIRMED":
                        st.info(f"THESIS CONFIRMED — gain validated, scale-up eligible{_tier_str}")
                    elif _winner_lc == "CONVICTION_BUILDING":
                        st.info(f"CONVICTION BUILDING — 1st add done, monitoring for 2nd{_tier_str}")
                    elif _winner_lc == "FULL_POSITION":
                        st.success(f"FULL POSITION — pyramid complete, protect gains{_tier_str}")
                    elif _winner_lc == "THESIS_EXHAUSTING":
                        st.warning(f"THESIS EXHAUSTING — momentum/conviction fading{_tier_str}")

                # Premium recovery line
                if pd.notna(cum_premium) and cum_premium > 0:
                    hard_stop = effective_for_drift * 0.80 if effective_for_drift else None
                    cycle_str = f" across {int(cycle_count)} cycles" if pd.notna(cycle_count) and cycle_count else ""
                    hard_stop_str = f"  ·  Hard stop: ${hard_stop:.2f}" if hard_stop else ""
                    if (has_debit_rolls
                            and pd.notna(gross_premium) and gross_premium > 0
                            and pd.notna(total_close) and total_close > 0.01):
                        # Show gross/net breakdown when debit rolls exist
                        st.caption(
                            f"💰 Net: ${cum_premium:.2f}/share{cycle_str} "
                            f"(Gross ${gross_premium:.2f} − buybacks ${total_close:.2f}) · "
                            f"reduced basis from ${cost_per_share:.2f} → ${net_cost:.2f}"
                            f"{hard_stop_str}"
                        )
                    else:
                        st.caption(
                            f"💰 ${cum_premium:.2f}/share collected{cycle_str} · "
                            f"reduced basis from ${cost_per_share:.2f} → ${net_cost:.2f}"
                            f"{hard_stop_str}"
                        )

                # Roll debit/credit awareness
                _roll_net = pd.to_numeric(s.get("Roll_Net_Credit"), errors="coerce")
                _roll_prior = pd.to_numeric(s.get("Roll_Prior_Credit"), errors="coerce")
                if pd.notna(_roll_net) and pd.notna(_roll_prior) and _roll_prior > 0:
                    _new_credit = _roll_prior + _roll_net
                    if _roll_net < -0.05:
                        _roll_text = f"net debit {abs(_roll_net):.2f}/share"
                        st.caption(
                            f"🔄 Last roll: net **debit** ${abs(_roll_net):.2f}/share "
                            f"(closed ${_roll_prior:.2f} → opened ${_new_credit:.2f})"
                        )
                    elif abs(_roll_net) <= 0.05:
                        _roll_text = f"scratch (≈flat)"
                        st.caption(
                            f"🔄 Last roll: **scratch** (≈flat) — "
                            f"closed ${_roll_prior:.2f} → opened ${_new_credit:.2f} call"
                        )
                    else:
                        _roll_text = f"net credit {_roll_net:.2f}/share"
                        st.caption(
                            f"🔄 Last roll: net **credit** ${_roll_net:.2f}/share "
                            f"(closed ${_roll_prior:.2f} → opened ${_new_credit:.2f})"
                        )
                    _card_metrics["last_roll_credit"] = _roll_text

                if drift_str:
                    st.caption(f"📍 Underlying drift from net cost: **{drift_str}**")

            # Option legs table
            if not opt_legs.empty:
                leg_display = []
                for _, leg in opt_legs.iterrows():
                    qty = int(leg.get("Quantity", 0) or 0)
                    direction = "Long" if qty > 0 else "Short"
                    strike = leg.get("Strike")
                    cp = leg.get("Call/Put") or leg.get("OptionType") or "?"
                    exp = leg.get("Expiration")
                    exp_str = pd.to_datetime(exp).strftime("%b %d '%y") if pd.notna(exp) else "—"
                    dte_v = leg.get("DTE")
                    last = pd.to_numeric(leg.get("Last"), errors="coerce")
                    delta = pd.to_numeric(leg.get("Delta"), errors="coerce")
                    theta = pd.to_numeric(leg.get("Theta"), errors="coerce")
                    vega = pd.to_numeric(leg.get("Vega"), errors="coerce")
                    basis = pd.to_numeric(leg.get("Basis"), errors="coerce")
                    gl = pd.to_numeric(leg.get("$ Total G/L"), errors="coerce")

                    leg_display.append({
                        "Dir": direction,
                        "Qty": abs(qty),
                        "Type": cp,
                        "Strike": f"${strike:.1f}" if pd.notna(strike) else "—",
                        "Exp": exp_str,
                        "DTE": int(dte_v) if pd.notna(dte_v) else "—",
                        "Last": f"${last:.2f}" if pd.notna(last) else "—",
                        "Δ": f"{delta:+.3f}" if pd.notna(delta) else "—",
                        "θ/day": f"${theta*100:.2f}" if pd.notna(theta) else "—",
                        "ν": f"{vega*100:.2f}" if pd.notna(vega) else "—",
                        "Basis": f"${basis:.0f}" if pd.notna(basis) else "—",
                        "G/L": f"{_format_pnl(gl)}" if pd.notna(gl) else "—",
                    })

                leg_df = pd.DataFrame(leg_display)

                def _style_legs(row, _leg_df=leg_df):
                    styles = [""] * len(row)
                    dte_idx = _leg_df.columns.get_loc("DTE")
                    try:
                        dte_val = int(str(row["DTE"]).replace("—", "9999"))
                    except Exception:
                        dte_val = 9999
                    if dte_val <= 7:
                        styles[dte_idx] = "color: #ff4b4b; font-weight: bold"
                    elif dte_val <= 21:
                        styles[dte_idx] = "color: #ffa500"

                    gl_idx = _leg_df.columns.get_loc("G/L")
                    gl_str = str(row["G/L"])
                    if gl_str.startswith("+"):
                        styles[gl_idx] = "color: #09ab3b"
                    elif gl_str.startswith("-"):
                        styles[gl_idx] = "color: #ff4b4b"

                    dir_idx = _leg_df.columns.get_loc("Dir")
                    styles[dir_idx] = "color: #ffa500" if row["Dir"] == "Short" else "color: #09ab3b"
                    return styles

                st.dataframe(
                    leg_df.style.apply(_style_legs, axis=1),
                    hide_index=True,
                    width="stretch",
                )

            # Net Greeks for this trade (initialize defensively — used later in roll candidate theta comparison)
            net_d = None
            net_t = None
            net_v = None
            net_g = None
            if not opt_legs.empty:
                net_d = (pd.to_numeric(opt_legs["Delta"], errors="coerce") *
                         pd.to_numeric(opt_legs["Quantity"], errors="coerce")).sum() * 100
                net_t = (pd.to_numeric(opt_legs["Theta"], errors="coerce") *
                         pd.to_numeric(opt_legs["Quantity"], errors="coerce")).sum() * 100
                net_v = (pd.to_numeric(opt_legs["Vega"], errors="coerce") *
                         pd.to_numeric(opt_legs["Quantity"], errors="coerce")).sum() * 100
                net_g = (pd.to_numeric(opt_legs["Gamma"], errors="coerce") *
                         pd.to_numeric(opt_legs["Quantity"], errors="coerce")).sum() * 100

                gc1, gc2, gc3, gc4 = st.columns(4)
                gc1.metric("Net Δ", f"{net_d:+.1f}")
                gc2.metric("Net θ/day", f"{net_t:+.2f}")
                gc3.metric("Net ν", f"{net_v:+.2f}")
                gc4.metric("Net Γ", f"{net_g:+.4f}")

            # ── Capital Efficiency + Hold EV (BUY_WRITE / COVERED_CALL) ──────
            # Only rendered for income strategies where theta carry is the edge.
            # Capital Efficiency: how much yield does the theta provide on capital at risk?
            # Hold EV: theta carry minus expected gamma drag over the remaining horizon.
            # Natenberg Ch.9: short-gamma P&L = theta_collected − 0.5×Γ×(ΔS)²
            _bw_for_ev = str(entry_structure).upper() in ("BUY_WRITE", "COVERED_CALL")
            if _bw_for_ev and not opt_legs.empty and not stock_legs.empty:
                _ev_s       = stock_legs.iloc[0]
                _ev_spot    = pd.to_numeric(_ev_s.get("UL Last"), errors="coerce")
                _ev_ncp     = pd.to_numeric(_ev_s.get("Net_Cost_Basis_Per_Share"), errors="coerce")
                _ev_basis   = pd.to_numeric(_ev_s.get("Basis"), errors="coerce")
                _ev_qty_s   = pd.to_numeric(_ev_s.get("Quantity"), errors="coerce")
                _ev_hv      = pd.to_numeric(_ev_s.get("HV_20D"), errors="coerce")
                _ev_iv      = pd.to_numeric(opt_legs.iloc[0].get("IV_30D") or opt_legs.iloc[0].get("IV_Now"), errors="coerce")
                _ev_dte     = pd.to_numeric(opt_legs.iloc[0].get("DTE"), errors="coerce")
                _ev_delta   = pd.to_numeric(opt_legs.iloc[0].get("Delta"), errors="coerce")
                # net_t is already in scope from Greek block above ($/day, ×100×qty)
                # net_g is already in scope (Gamma dollar, signed, ×100×qty)

                _ev_ncp_safe = _ev_ncp if (pd.notna(_ev_ncp) and _ev_ncp > 0) else (
                    (_ev_basis / _ev_qty_s) if (pd.notna(_ev_basis) and pd.notna(_ev_qty_s) and _ev_qty_s > 0) else None
                )
                _ev_qty_int = int(_ev_qty_s) if pd.notna(_ev_qty_s) else 0

                if (_ev_ncp_safe and _ev_qty_int > 0
                        and pd.notna(_ev_spot) and pd.notna(net_t) and pd.notna(_ev_dte)):

                    _ev = _compute_bw_capital_efficiency(
                        net_cost_per_share = float(_ev_ncp_safe),
                        n_shares           = _ev_qty_int,
                        theta_per_day      = float(net_t),
                        dte                = float(_ev_dte),
                        hv                 = float(_ev_hv) if pd.notna(_ev_hv) else None,
                        iv                 = float(_ev_iv) if pd.notna(_ev_iv) else None,
                        spot               = float(_ev_spot),
                        delta              = float(_ev_delta) if pd.notna(_ev_delta) else None,
                    )
                    # Second pass: fill gamma drag now that net_g is in scope
                    if pd.notna(net_g):
                        _ev = _fill_bw_hold_ev(_ev, float(net_g), float(_ev_spot))
                    _card_metrics["cap_eff"] = _ev

                    with st.expander("📈 Capital Efficiency + Hold EV", expanded=False):
                        _cap = _ev.get("capital_at_risk")
                        _hor = _ev.get("horizon_days")
                        _carry = _ev.get("theta_carry_30d")
                        _eff_p = _ev.get("efficiency_pct")
                        _eff_a = _ev.get("efficiency_ann")
                        _gdrag = _ev.get("gamma_drag_30d")
                        _hev   = _ev.get("hold_ev_net")
                        _hv1sd = _ev.get("hv_daily_move_1sd")

                        _hor_label = f"{int(_hor)}d" if _hor else "—"
                        _hv_pct_ev = f"{float(_ev_hv):.0%}" if pd.notna(_ev_hv) else "—"

                        # Row 1: Capital at risk + efficiency
                        ev1, ev2, ev3 = st.columns(3)
                        ev1.metric(
                            "Capital at Risk",
                            f"${_cap:,.0f}" if _cap else "—",
                            delta=f"{_ev_ncp_safe:.2f}/share net basis",
                        )
                        ev2.metric(
                            f"θ Carry ({_hor_label})",
                            f"${_carry:,.0f}" if _carry else "—",
                            delta=f"{_eff_p:.2%}/capital" if _eff_p else None,
                        )
                        ev3.metric(
                            "Annualised Yield",
                            f"{_eff_a:.1%}" if _eff_a else "—",
                            delta="on net basis",
                        )

                        st.divider()

                        # Row 2: Gamma drag + Margin cost + Hold EV
                        _mcost_d = _ev.get("margin_cost_daily")
                        _mcost_h = _ev.get("margin_cost_horizon")
                        _mcost_ps = _ev.get("margin_cost_per_share_daily")
                        _mcost_mo = (_mcost_d * 30) if _mcost_d else None

                        ev4, ev5, ev6 = st.columns(3)
                        ev4.metric(
                            f"Gamma Drag ({_hor_label})",
                            f"${_gdrag:,.0f}" if _gdrag is not None else "—",
                            delta=f"HV={_hv_pct_ev}, ±${_hv1sd:.2f}/day 1σ" if _hv1sd else None,
                            delta_color="inverse",
                        )
                        ev5.metric(
                            "Margin Cost",
                            f"${_mcost_d:.2f}/day" if _mcost_d else "—",
                            delta=f"${_mcost_mo:,.0f}/month" + (f" · {_hor_label}: ${_mcost_h:,.0f}" if _mcost_h else "") if _mcost_mo else None,
                            delta_color="inverse",
                            help=f"Fidelity 10.375%/yr on ${_ev_ncp_safe:.2f}/share net basis × {_ev_qty_int} shares"
                                 if _mcost_d else None,
                        )
                        ev6.metric(
                            f"Hold EV ({_hor_label})",
                            f"${_hev:,.0f}" if _hev is not None else "—",
                            delta="θ − Γ drag − margin" if _hev is not None else "HV unavailable",
                            delta_color="normal" if (_hev is not None and _hev > 0) else "inverse",
                        )

                        st.divider()

                        # Row 3: Carry quality summary
                        # Efficiency context: must reconcile annualised yield WITH hold EV.
                        # Annualised yield is theta-only; if costs > theta, hold EV is
                        # negative — "strong carry" is wrong when you're losing money net.
                        _eff_context = ""
                        if _hev is not None and _hev < 0:
                            _eff_context = "🔴 Negative carry (costs > θ)"
                        elif _eff_a:
                            if _eff_a >= 0.20:
                                _eff_context = "✅ Strong carry (>20%/yr)"
                            elif _eff_a >= 0.10:
                                _eff_context = "🟡 Moderate carry (10–20%/yr)"
                            else:
                                _eff_context = "🔴 Thin carry (<10%/yr)"
                        ev7, ev8, ev9 = st.columns(3)
                        ev7.metric("Carry Quality", _eff_context or "—")
                        # Net yield after margin: how much of theta actually survives
                        if _mcost_d and net_t:
                            _theta_after_margin = net_t - _mcost_d
                            _pct_kept = _theta_after_margin / net_t if net_t > 0 else 0
                            ev8.metric(
                                "θ After Margin",
                                f"${_theta_after_margin:+.2f}/day",
                                delta=f"{_pct_kept:.0%} of gross θ retained",
                                delta_color="normal" if _theta_after_margin > 0 else "inverse",
                            )
                        if _mcost_ps:
                            _margin_ann_pct = 0.10375  # match constant
                            ev9.metric(
                                "Margin Rate",
                                "10.375%/yr",
                                delta=f"${_mcost_ps:.3f}/share/day",
                                delta_color="off",
                            )

                        # Formula transparency
                        _margin_formula = ""
                        if _mcost_d:
                            _margin_formula = (
                                f"Margin cost = \\${_ev_ncp_safe:.2f} × {_ev_qty_int}sh × 10.375%/365 "
                                f"= \\${_mcost_d:.2f}/day (\\${_mcost_mo:,.0f}/month). "
                            )
                        st.caption(
                            f"**How computed:** "
                            f"Capital at risk = net basis \\${_ev_ncp_safe:.2f} × {_ev_qty_int} shares. "
                            f"θ carry = \\${net_t:+.2f}/day × {_hor_label}. "
                            + (f"Γ drag = 0.5 × {abs(net_g):.4f} × (\\${_hv1sd:.2f} daily 1σ)² × {_hor_label} "
                               f"(Net Γ = {net_g:+.4f}; Natenberg Ch.9: short-gamma P&L = θ − ½Γ(ΔS)²). "
                               if _hv1sd and _gdrag is not None else
                               "Γ drag unavailable — HV missing. ")
                            + _margin_formula
                            + "Hold EV = θ carry − Γ drag − margin cost. "
                            + "Annualised yield = (θ carry / capital) × (365 / horizon)."
                        )

                        # ── Buyback Evaluation ────────────────────────────────
                        st.divider()

                        # Gather signals (needed for both modes below)
                        _bb_adx   = pd.to_numeric(_ev_s.get("adx_14"),              errors="coerce")
                        _bb_roc20 = pd.to_numeric(_ev_s.get("roc_20"),              errors="coerce")
                        _bb_mom   = str(_ev_s.get("MomentumVelocity_State") or "")
                        _bb_hvpct = pd.to_numeric(_ev_s.get("hv_20d_percentile"),   errors="coerce")
                        _bb_ivsrf = str(opt_legs.iloc[0].get("iv_surface_shape") or "FLAT")
                        _bb_strk  = pd.to_numeric(opt_legs.iloc[0].get("Strike"),   errors="coerce")
                        _bb_drag_day = (_gdrag / _hor) if (_gdrag is not None and _hor and _hor > 0) else None

                        _bb = _evaluate_buyback_rationale(
                            theta_per_day   = float(net_t),
                            gamma_drag_day  = _bb_drag_day,
                            hv              = float(_ev_hv)  if pd.notna(_ev_hv)   else None,
                            iv              = float(_ev_iv)  if pd.notna(_ev_iv)   else None,
                            hv_percentile   = float(_bb_hvpct) if pd.notna(_bb_hvpct) else None,
                            iv_surface      = _bb_ivsrf,
                            adx             = float(_bb_adx)  if pd.notna(_bb_adx)  else None,
                            roc20           = float(_bb_roc20) if pd.notna(_bb_roc20) else None,
                            mom_velocity    = _bb_mom or "UNKNOWN",
                            delta           = float(_ev_delta) if pd.notna(_ev_delta) else None,
                            strike          = float(_bb_strk)  if pd.notna(_bb_strk)  else float(_ev_spot),
                            spot            = float(_ev_spot),
                            dte             = float(_ev_dte),
                        )

                        _sev_colour = {
                            "NONE":     "🟢",
                            "WATCH":    "🟡",
                            "EVALUATE": "🟠",
                            "ACT":      "🔴",
                        }
                        _top = _bb["top_severity"]

                        # ── Mode: doctrine has already decided → pivot to execution ──
                        # When doctrine says ROLL or EXIT, "should you buy back?" is
                        # answered. The panel reframes to: WHY + WHEN to execute the BTC.
                        # The three-condition detail is collapsed and relabelled as
                        # confirmation context, not a decision gate.
                        # _doc_action is assigned later in the card loop — read it here directly.
                        _bb_doc_row    = doctrine_by_trade.get(tid)
                        _bb_doc_action = str(_bb_doc_row.get("Action", "")) if _bb_doc_row is not None else ""
                        _bb_urgency    = str(_bb_doc_row.get("Urgency", "LOW")) if _bb_doc_row is not None else "LOW"
                        _doctrine_decided_btc = _bb_doc_action in ("ROLL", "EXIT")

                        if _doctrine_decided_btc:
                            # Header: confirmed action, not a question
                            _btc_urgency = _bb_urgency
                            _btc_icon = "🔴" if _btc_urgency == "CRITICAL" else "🟠" if _btc_urgency == "HIGH" else "🟡"
                            st.markdown(f"**📣 Buy Back the Short Call — {_btc_icon} {_bb_doc_action} {_btc_urgency}**")
                            # For EXIT: redirect to the Exit Winner Panel, not Roll Scenarios.
                            # The Winner Panel resolves the call disposition (buyback vs let expire vs
                            # accept assignment) — the roll scaffold is irrelevant for EXIT.
                            if _bb_doc_action == "EXIT":
                                st.info(
                                    "Doctrine has decided: **EXIT**. "
                                    "See the **Exit Winner Panel** below for the exact execution directive — "
                                    "it resolves whether to buy back the call, accept assignment, or let it expire.  \n"
                                    "Signals below confirm the structural rationale."
                                )
                            else:
                                st.info(
                                    f"Doctrine has decided: **{_bb_doc_action}**. "
                                    "The question is no longer *whether* to buy back — it's *when* and *how*.  \n"
                                    "Signals below confirm the rationale. "
                                    "Use the Roll Scenarios section for execution mechanics."
                                )

                            # Timing guidance for the BTC leg
                            # BTC = buying to close a short call → want IV low (afternoon trough)
                            # For EXIT actions: the Exit Winner Panel owns timing guidance.
                            # Only show the BTC timing window if doctrine is ROLL (not EXIT).
                            _btc_iv_roc = float(_ev_s.get("IV_ROC_3D") or 0)
                            _btc_best_window = "1:00–3:30 PM ET"
                            _btc_avoid = "9:30–10:15 AM ET"
                            _btc_window_reason = "IV trough in afternoon = cheapest buyback cost"
                            _btc_avoid_reason  = "IV spikes at open — you'd overpay for the buyback"

                            if _bb_doc_action == "EXIT":
                                # Timing is resolved by the Exit Winner Panel below.
                                # Only show a DTE-gamma warning if ≤7d (gamma convexity risk).
                                if pd.notna(_ev_dte) and float(_ev_dte) <= 7:
                                    st.warning(
                                        f"⚡ DTE {int(_ev_dte)} — gamma convexity high. "
                                        "See Exit Winner Panel below for execution path."
                                    )
                                # No buyback timing columns — Winner Panel owns this.
                            else:
                                # DTE gate: if ≤7, don't optimize timing
                                if pd.notna(_ev_dte) and float(_ev_dte) <= 7:
                                    st.warning(
                                        f"⚡ DTE {int(_ev_dte)} — gamma is convex. "
                                        "Don't wait for optimal window. Execute BTC now."
                                    )
                                elif _btc_urgency in ("HIGH", "CRITICAL"):
                                    # Show timing window but flag urgency
                                    _bw1, _bw2 = st.columns(2)
                                    _bw1.success(f"**Best window**  \n{_btc_best_window}  \n{_btc_window_reason}")
                                    _bw2.warning(f"**Avoid**  \n{_btc_avoid}  \n{_btc_avoid_reason}")
                                    st.warning(
                                        f"⚠️ {_btc_urgency} urgency — if already in the avoid window, "
                                        "execute anyway. Don't sacrifice a session waiting for 1pm "
                                        "when doctrine says act today."
                                    )
                                else:
                                    _bw1, _bw2 = st.columns(2)
                                    _bw1.success(f"**Best window**  \n{_btc_best_window}  \n{_btc_window_reason}")
                                    _bw2.warning(f"**Avoid**  \n{_btc_avoid}  \n{_btc_avoid_reason}")
                                    if _btc_iv_roc < -0.02:
                                        st.caption(f"✅ IV ROC {_btc_iv_roc:+.2f} (falling) — vol compressing, BTC gets cheaper. Favor waiting for afternoon window.")
                                    elif _btc_iv_roc > 0.02:
                                        st.caption(f"⚠️ IV ROC {_btc_iv_roc:+.2f} (rising) — don't wait; IV rising makes BTC more expensive. Execute at next opportunity.")

                            # Confirmation context (collapsed — decision already made)
                            with st.expander("📊 Signal confirmation (why doctrine decided this)", expanded=False):
                                for _ckey in ("c1", "c2", "c3"):
                                    _cd = _bb[_ckey]
                                    _ico = _sev_colour.get(_cd["severity"], "⚪")
                                    st.markdown(f"**{_ico} {_cd['label']} · {_cd['severity']}**")
                                    _safe_reason = _cd["reason"].replace("$", r"\$")
                                    st.markdown(
                                        f"<small>{_safe_reason}</small>",
                                        unsafe_allow_html=True,
                                    )

                        else:
                            # ── Mode: doctrine is HOLD — evaluate whether to buy back ──
                            st.markdown("**📣 Should You Buy Back the Short Call?**")

                            if _top == "ACT":
                                st.error(
                                    "🔴 **ACT — one or more conditions require immediate review.** "
                                    "Default-to-assignment assumption may no longer hold."
                                )
                            elif _top == "EVALUATE":
                                st.warning(
                                    "🟠 **EVALUATE — conditions detected that may justify buying back.** "
                                    "Review each condition below before next session."
                                )
                            elif _top == "WATCH":
                                st.info(
                                    "🟡 **WATCH — early signals present.** "
                                    "Monitor for confirmation before acting."
                                )
                            else:
                                st.caption(
                                    "🟢 Default to assignment — no buyback conditions triggered. "
                                    "Theta carry is the rational choice."
                                )

                            for _ckey in ("c1", "c2", "c3"):
                                _cd = _bb[_ckey]
                                _ico = _sev_colour.get(_cd["severity"], "⚪")
                                with st.expander(
                                    f"{_ico} {_cd['label']}  ·  {_cd['severity']}",
                                    expanded=(_cd["severity"] in ("EVALUATE", "ACT")),
                                ):
                                    _safe_reason = _cd["reason"].replace("$", r"\$")
                                    st.markdown(
                                        f"<small>{_safe_reason}</small>",
                                        unsafe_allow_html=True,
                                    )

            # ── Recovery Path Analysis (BUY_WRITE under pressure) ─────────────
            # Surfaces the math of recovery via continued rolling.
            # McMillan Ch.3: the question is not "should I exit?" but
            # "what does recovery require, and is that credible?"
            _eff_cost = None  # initialise here so all downstream blocks can safely reference it
            _spot = None
            _is_bw = str(entry_structure).upper() in ("BUY_WRITE", "COVERED_CALL")
            if _is_bw and not stock_legs.empty:
                _s = stock_legs.iloc[0]
                _spot       = pd.to_numeric(_s.get("UL Last"), errors="coerce")
                _net_cost   = pd.to_numeric(_s.get("Net_Cost_Basis_Per_Share"), errors="coerce")
                _raw_cost   = pd.to_numeric(_s.get("Basis"), errors="coerce")
                _qty        = pd.to_numeric(_s.get("Quantity"), errors="coerce")
                _cum_prem   = pd.to_numeric(_s.get("Cumulative_Premium_Collected"), errors="coerce")
                _cycle_cnt  = _s.get("_cycle_count", 0)
                _hv         = pd.to_numeric(_s.get("HV_20D"), errors="coerce")
                _raw_cost_ps = (_raw_cost / _qty) if (pd.notna(_raw_cost) and pd.notna(_qty) and _qty > 0) else None
                _eff_cost   = _net_cost if (pd.notna(_net_cost) and _net_cost > 0) else _raw_cost_ps
                if pd.notna(_cum_prem):
                    _card_metrics.update(cum_prem=float(_cum_prem), cycle_count=int(_cycle_cnt or 0))

                if pd.notna(_spot) and _eff_cost and _eff_cost > 0:
                    _drift = (_spot - _eff_cost) / _eff_cost
                    _gap_to_breakeven = _eff_cost - _spot   # $ stock needs to rise to reach net cost
                    _hard_stop        = _eff_cost * 0.80
                    _is_below_stop    = bool(_spot < _hard_stop)
                    _gap_to_stop      = _spot - _hard_stop  # negative = already breached
                    _card_metrics.update(gap=_gap_to_breakeven, hard_stop=_hard_stop,
                                         gap_to_stop=_gap_to_stop, basis_drift=_drift)

                    # Only show recovery analysis when position is under meaningful pressure
                    if _drift < -0.08:
                        # Get IV from doctrine row (option leg) for accurate premium estimate
                        _iv_for_recovery = float("nan")
                        _doc_row_for_iv = doctrine_by_trade.get(tid)
                        if _doc_row_for_iv is not None:
                            _iv_for_recovery = pd.to_numeric(
                                _doc_row_for_iv.get("IV_30D") or _doc_row_for_iv.get("IV_Now"),
                                errors="coerce"
                            )
                        if pd.isna(_iv_for_recovery):
                            _iv_for_recovery = pd.to_numeric(
                                option_row_by_trade.get(tid, pd.Series()).get("IV_30D")
                                if option_row_by_trade.get(tid) is not None else None,
                                errors="coerce"
                            )

                        with st.expander("📊 Recovery Path Analysis", expanded=_is_below_stop):
                            st.caption(
                                "**What does recovery require?** "
                                "McMillan Ch.3: recovery = stock appreciation + premium collected. "
                                "Premium estimate uses **IV** (what market pays), not HV (realized vol)."
                            )

                            # Row 1: current state
                            r1a, r1b, r1c = st.columns(3)
                            r1a.metric(
                                "Gap to Breakeven",
                                f"${_gap_to_breakeven:.2f}/share",
                                delta=f"${_gap_to_breakeven * (_qty or 0):+,.0f} total",
                                delta_color="inverse",
                            )
                            r1b.metric(
                                "Hard Stop",
                                f"${_hard_stop:.2f}",
                                delta=f"${_gap_to_stop:+.2f} cushion",
                                delta_color="inverse" if _gap_to_stop < 0 else "normal",
                            )
                            _cum_display = f"${_cum_prem:.2f}/share" if pd.notna(_cum_prem) and _cum_prem > 0 else "—"
                            _cycle_display = f"({int(_cycle_cnt)} cycles)" if _cycle_cnt else ""
                            r1c.metric("Collected to Date", _cum_display, delta=_cycle_display)

                            st.divider()

                            # Row 2: Rolling recovery scenarios using IV (what market actually pays)
                            st.markdown("**Rolling Scenarios** — premium at current IV vs gap to close:")

                            # Use IV if available; fall back to HV with a clear warning
                            _vol_for_est = _iv_for_recovery if pd.notna(_iv_for_recovery) and _iv_for_recovery > 0 else _hv
                            _using_iv    = pd.notna(_iv_for_recovery) and _iv_for_recovery > 0
                            _iv_lt_hv    = _using_iv and pd.notna(_hv) and _hv > 0 and _iv_for_recovery < _hv

                            if pd.notna(_vol_for_est) and _vol_for_est > 0 and pd.notna(_spot):
                                # ATM call at ~1 week DTE ≈ 0.4 × σ × S / √52
                                _weekly_premium_est  = 0.4 * _vol_for_est * _spot / (52 ** 0.5)
                                _monthly_premium_est = _weekly_premium_est * 4.3

                                # Margin bleed: real cost that eats into recovery premium
                                _MARGIN_RATE_DAILY = 0.10375 / 365
                                _margin_ps_daily = _eff_cost * _MARGIN_RATE_DAILY if _eff_cost else 0.0
                                _margin_ps_monthly = _margin_ps_daily * 30
                                _net_monthly_income = _monthly_premium_est - _margin_ps_monthly
                                _cycles_to_recover = int(_gap_to_breakeven / max(_net_monthly_income, 0.01)) + 1 if _net_monthly_income > 0.01 else 999
                                _card_metrics.update(weekly_premium=_weekly_premium_est,
                                                     monthly_premium=_monthly_premium_est,
                                                     cycles_to_recover=_cycles_to_recover,
                                                     margin_ps_monthly=_margin_ps_monthly)
                                _vol_label = "IV" if _using_iv else "HV"
                                _vol_pct   = f"{_vol_for_est:.0%}" if _vol_for_est < 2 else f"{_vol_for_est:.0f}%"

                                sc1, sc2, sc3, sc4 = st.columns(4)
                                sc1.metric(
                                    f"{_vol_label}-Implied Monthly",
                                    f"~${_monthly_premium_est:.2f}/share",
                                    help=f"ATM call ≈ 0.4 × {_vol_label}({_vol_pct}) × ${_spot:.2f} / √52 × 4.3"
                                )
                                sc2.metric(
                                    "Margin Bleed",
                                    f"−${_margin_ps_monthly:.2f}/share/mo",
                                    delta=f"10.375%/yr on ${_eff_cost:.2f} basis",
                                    delta_color="inverse",
                                )
                                sc3.metric(
                                    "Net Monthly Income",
                                    f"~${_net_monthly_income:.2f}/share" if _net_monthly_income > 0 else "Negative",
                                    delta=f"premium − margin" if _net_monthly_income > 0 else "margin > premium",
                                    delta_color="normal" if _net_monthly_income > 0 else "inverse",
                                )
                                sc4.metric(
                                    "Months to Close Gap",
                                    f"~{_cycles_to_recover}" if _cycles_to_recover < 999 else "∞",
                                    help=f"${_gap_to_breakeven:.2f} gap ÷ ${_net_monthly_income:.2f}/mo net income"
                                         if _net_monthly_income > 0 else "Margin cost exceeds premium — gap widens",
                                    delta=f"net of margin cost" if _net_monthly_income > 0 else "recovery infeasible",
                                    delta_color="off" if _net_monthly_income > 0 else "inverse",
                                )

                                # IV < HV warning: market pricing less vol than realised
                                if _iv_lt_hv:
                                    _hv_pct = f"{_hv:.0%}" if _hv < 2 else f"{_hv:.0f}%"
                                    st.warning(
                                        f"**IV ({_vol_pct}) < HV ({_hv_pct})** — market is pricing less volatility "
                                        f"than has been realized. Premium you can collect is lower than HV suggests. "
                                        f"The prior estimate using HV was optimistic."
                                    )

                                # Hard stop breach overrides feasibility verdict
                                # McMillan Ch.3: "You are no longer managing — you are gambling on recovery."
                                if _is_below_stop:
                                    st.error(
                                        f"**Hard stop already breached** (stock ${_spot:.2f} < stop ${_hard_stop:.2f}). "
                                        f"Rolling for ~{_cycles_to_recover} months of ${_monthly_premium_est:.2f}/month income "
                                        f"while the stock can fall $1/day is not a recovery plan — it is negative expected value. "
                                        f"**McMillan Ch.3**: exit stock + buy back call. "
                                        f"Premium income cannot outrun continued stock deterioration."
                                    )
                                elif _cycles_to_recover <= 3:
                                    st.success(f"**Feasible**: ~{_cycles_to_recover} months of rolling can close the ${_gap_to_breakeven:.2f}/share gap — if stock stabilizes.")
                                elif _cycles_to_recover <= 6:
                                    st.warning(f"**Marginal**: ~{_cycles_to_recover} months required. Each dollar of stock decline adds another month. Verify thesis first.")
                                else:
                                    st.error(f"**Unlikely at current vol**: ~{_cycles_to_recover} months at {_vol_pct} IV. The math doesn't work — consider exit.")
                            else:
                                st.info("Vol data unavailable — cannot estimate rolling recovery timeline.")

                            # Row 3: What matters for the decision
                            st.markdown("**Before next roll, verify:**")
                            checks = []
                            if _is_below_stop:
                                checks.append(f"🔴 Already **below hard stop** (${_hard_stop:.2f}) — rolling locks in deeper loss if stock continues falling")
                            if not opt_legs.empty:
                                _min_dte_val = float(opt_legs["DTE"].min()) if "DTE" in opt_legs.columns else 999
                                if _min_dte_val <= 7:
                                    checks.append(f"🟠 Option expires in **{int(_min_dte_val)}d** — roll this week before pin risk")
                                elif _min_dte_val <= 21:
                                    checks.append(f"🟡 **{int(_min_dte_val)} DTE** — consider rolling early to lock in remaining time value")
                            checks.append("📋 Is the business thesis still intact? (earnings, guidance, sector catalyst)")
                            checks.append("📈 Is HV elevated? (higher vol = higher premium = faster recovery)")
                            for chk in checks:
                                st.caption(chk)

                            # ── Recovery Quality Gate ─────────────────────────
                            # Surfaces the engine's dead-cat bounce vs structural
                            # recovery classification directly on the card.
                            _rq_row_a = doctrine_by_trade.get(tid)
                            _rq_row = _rq_row_a if _rq_row_a is not None else option_row_by_trade.get(tid)
                            _rq_state = str(_rq_row.get("RecoveryQuality_State", "") or "").upper() if _rq_row is not None else ""
                            _rq_reason = str(_rq_row.get("RecoveryQuality_Resolution_Reason", "") or "") if _rq_row is not None else ""

                            if _rq_state and _rq_state not in ("", "UNKNOWN", "NOT_IN_RECOVERY", "NOT_APPLICABLE"):
                                st.divider()
                                st.markdown("**📡 Recovery Signal Classification:**")
                                if "DEAD_CAT" in _rq_state:
                                    st.error(
                                        f"🐱 **Dead-Cat Bounce** — structure has NOT changed.  \n"
                                        f"{_rq_reason}  \n"
                                        f"**Do not adapt** the roll strike or timing to this move. "
                                        f"Wait for: higher low forming → break above prior swing high → "
                                        f"ROC10 > 0 → EMA20 turning up."
                                    )
                                elif "STRUCTURAL_RECOVERY" in _rq_state:
                                    st.success(
                                        f"✅ **Structural Recovery Confirmed** — regime shift detected.  \n"
                                        f"{_rq_reason}  \n"
                                        f"Adaptation is now rational: higher strike, tighter DTE, "
                                        f"or adjust basis target upward."
                                    )
                                elif "STILL_DECLINING" in _rq_state:
                                    st.error(
                                        f"📉 **Still Declining** — no bounce, trend continues down.  \n"
                                        f"{_rq_reason}  \n"
                                        f"Hard stop discipline applies. Do not roll for premium — "
                                        f"premium cannot outrun a continuing decline."
                                    )

            # ── Thesis State Panel ────────────────────────────────────────────
            # Layer 0: is the underlying company still aligned with the thesis?
            _thesis_row_a = doctrine_by_trade.get(tid)
            _thesis_row   = _thesis_row_a if _thesis_row_a is not None else option_row_by_trade.get(tid)
            _thesis_state   = str(_thesis_row.get("Thesis_State", "") or "").upper() if _thesis_row is not None else ""
            _thesis_summary = str(_thesis_row.get("Thesis_Summary", "") or "") if _thesis_row is not None else ""
            _thesis_type    = str(_thesis_row.get("Thesis_Drawdown_Type", "") or "") if _thesis_row is not None else ""
            _thesis_drivers_raw = str(_thesis_row.get("Thesis_Drivers", "[]") or "[]") if _thesis_row is not None else "[]"

            if _thesis_state and _thesis_state not in ("", "UNKNOWN"):
                st.divider()
                # Pre-compute regime-roll flag so we can use it in the expander header
                _doctrine_action_pre    = str(_thesis_row.get("Action",    "") or "") if _thesis_row is not None else ""
                _doctrine_rationale_pre = str(_thesis_row.get("Rationale", "") or "") if _thesis_row is not None else ""
                _ei_state_pre = str(
                    (_thesis_row.get("Equity_Integrity_State") if _thesis_row is not None else None) or ""
                ).strip().upper()
                _is_regime_roll_pre = (
                    _thesis_state == "INTACT"
                    and _doctrine_action_pre in ("ROLL", "EXIT")
                    and any(kw in _doctrine_rationale_pre.lower() for kw in
                            ("regime", "vol regime", "thesis regime", "vol shift", "degraded", "compressed", "expanding"))
                )
                _is_equity_broken_intact = (_thesis_state == "INTACT" and _ei_state_pre == "BROKEN")
                # Header suffix: flag contradictions visible without expanding
                _thesis_header_suffix = (
                    (f"  ·  {_thesis_type}" if _thesis_type and _thesis_type != "UNKNOWN" else "")
                    + ("  ⚠️ vol-regime override" if _is_regime_roll_pre else "")
                    + ("  ⚠️ equity BROKEN" if _is_equity_broken_intact and not _is_regime_roll_pre else "")
                )
                _thesis_expand = (
                    _thesis_state == "BROKEN"
                    or _is_regime_roll_pre
                    or _is_equity_broken_intact
                )
                with st.expander(
                    f"🏗 Story Check — Thesis: **{_thesis_state}**" + _thesis_header_suffix,
                    expanded=_thesis_expand
                ):
                    if _thesis_state == "BROKEN":
                        st.error(
                            f"🚫 **Thesis BROKEN** — {_thesis_summary}  \n"
                            f"**Rolling now amplifies the loss.** Evaluate:  \n"
                            f"• If **STRUCTURAL** (rev miss, guidance cut, sector rotation): exit stock + buy back call.  \n"
                            f"• If **TEMPORARY** (earnings gap, macro correction): hold, monitor recovery signals.  \n"
                            f"**Rolls blocked** until thesis repairs *(Passarelli Ch.2: story check)*."
                        )
                    elif _thesis_state == "DEGRADED":
                        _dtype_str = f" ({_thesis_type})" if _thesis_type and _thesis_type not in ("UNKNOWN", "N/A", "") else ""
                        st.warning(
                            f"⚠️ **Thesis DEGRADED**{_dtype_str} — {_thesis_summary}  \n"
                            f"Roll with caution. Avoid aggressive strike moves. "
                            f"Monitor for further deterioration before next cycle."
                        )
                    elif _thesis_state == "INTACT":
                        # Check if Equity_Integrity contradicts the INTACT thesis.
                        # Equity_Integrity_State=BROKEN means chart/price structure is destroyed —
                        # which IS the thesis. Showing green INTACT alongside red BROKEN is
                        # contradictory. When equity is BROKEN, downgrade display to DEGRADED.
                        _ei_state_thesis = str(
                            (_thesis_row.get("Equity_Integrity_State") if _thesis_row is not None else None) or ""
                        ).strip().upper()
                        if _ei_state_thesis == "BROKEN":
                            st.warning(
                                "⚠️ **Thesis DEGRADED** (equity structure BROKEN) — "
                                "Price/trend structure is destroyed. Story Check shows INTACT on fundamentals, "
                                "but the chart thesis is broken. Roll with caution or exit. "
                                "*(Equity_Integrity overrides Thesis_State when BROKEN)*"
                            )
                        else:
                            # Strip any state prefix the engine may have baked into the summary string
                            _intact_body = _thesis_summary or ''
                            for _pfx in ("Thesis INTACT — ", "Thesis INTACT—", "INTACT — ", "INTACT—"):
                                if _intact_body.startswith(_pfx):
                                    _intact_body = _intact_body[len(_pfx):]
                                    break
                            st.success(
                                f"✅ **Thesis INTACT** — {_intact_body or 'No structural concerns detected.'}"
                            )
                            # Clarify when doctrine says ROLL/EXIT but Story Check says INTACT.
                            if _is_regime_roll_pre:
                                st.caption(
                                    "ℹ️ *Story Check = fundamentals only (EPS, analyst, news). "
                                    "Doctrine rolled on **vol-regime shift** — a separate signal. Both can be true simultaneously: "
                                    "the business is fine, but the vol environment that made the trade attractive has changed.*"
                                )

                    # Driver detail table
                    try:
                        import json as _json
                        _drivers = _json.loads(_thesis_drivers_raw)
                        if _drivers:
                            _driver_df = pd.DataFrame(_drivers)[["signal", "weight", "note"]]
                            _driver_df.columns = ["Signal", "Weight", "Detail"]
                            _driver_df["Weight"] = _driver_df["Weight"].apply(lambda w: f"{w:+.2f}")
                            st.dataframe(_driver_df, hide_index=True, width='stretch')
                    except Exception:
                        pass

                    # ── Forward Expectancy Panel ──────────────────────────────────
                    # Cycle 2.6.5: EV ratio, theta bleed, conviction decay
                    # Source row: doctrine_by_trade for this trade (same as thesis)
                    _fev_row = _thesis_row  # already resolved above
                    if _fev_row is not None:
                        _ev_ratio    = _fev_row.get('EV_Feasibility_Ratio')
                        _ev_50_ratio = _fev_row.get('EV_50pct_Feasibility_Ratio')
                        _em_10       = _fev_row.get('Expected_Move_10D')
                        _req_be      = _fev_row.get('Required_Move_Breakeven')
                        _req_50      = _fev_row.get('Required_Move_50pct')
                        _theta_bleed = _fev_row.get('Theta_Bleed_Daily_Pct')
                        _theta_flag  = _fev_row.get('Theta_Opportunity_Cost_Flag', False)
                        _conv_raw    = _fev_row.get('Conviction_Status')
                        _conv_status = '' if (not _conv_raw or str(_conv_raw).upper() in ('NAN', 'N/A', 'NONE', '')) else str(_conv_raw).strip()
                        _det_streak  = _fev_row.get('Delta_Deterioration_Streak')

                        _has_fev = pd.notna(_ev_ratio) and _ev_ratio > 0

                        # Profit cushion for ITM winners (breakeven = 0)
                        _pc_val  = _fev_row.get('Profit_Cushion')
                        _pcr_val = _fev_row.get('Profit_Cushion_Ratio')
                        _has_cushion = (pd.notna(_pc_val) and float(_pc_val or 0) > 0)

                        if _has_fev or _has_cushion:
                            st.divider()
                            st.caption("**Forward Expectancy** (IV-based, 10-day window)")
                            _em_str = f"${_em_10:.1f}" if pd.notna(_em_10) else "—"

                        if _has_cushion:
                            # ITM winner: show profit cushion instead of breakeven/recovery
                            _pc_f   = float(_pc_val)
                            _pcr_f  = float(_pcr_val) if pd.notna(_pcr_val) else 0
                            _c_icon = "🔴" if _pcr_f < 0.5 else ("🟡" if _pcr_f < 1.0 else "🟢")
                            _c_lbl  = "Thin" if _pcr_f < 0.5 else ("Moderate" if _pcr_f < 1.0 else "Deep")
                            st.caption(
                                f"Expected Move (10D): {_em_str}  ·  "
                                f"Profit Cushion: ${_pc_f:.1f} ({_pcr_f:.2f}× 10D move) "
                                f"{_c_icon} {_c_lbl}"
                            )

                        elif _has_fev:
                            # OTM / losing: show breakeven and recovery
                            if _ev_ratio < 0.5:
                                _ratio_icon = "🟢"
                                _ratio_label = "Feasible"
                            elif _ev_ratio < 1.5:
                                _ratio_icon = "🟡"
                                _ratio_label = "Monitor"
                            else:
                                _ratio_icon = "🔴"
                                _ratio_label = "Low Expectancy"

                            _be_str  = f"${_req_be:.1f}" if pd.notna(_req_be)  else "—"
                            _50_str  = f"${_req_50:.1f}" if pd.notna(_req_50)  else "—"
                            _r50_str = f"{_ev_50_ratio:.2f}×" if pd.notna(_ev_50_ratio) else "—"

                            st.caption(
                                f"Expected Move (10D): {_em_str}  ·  "
                                f"Required move to breakeven: {_be_str} {_ratio_icon} {_ev_ratio:.2f}× {_ratio_label}  ·  "
                                f"50% Recovery: {_50_str} ({_r50_str})"
                            )

                        # Theta bleed — suppress at ≤ 3 DTE (bleed is always extreme near expiry)
                        _fev_dte = float(_fev_row.get('DTE') or 0) if _fev_row is not None else 0
                        if pd.notna(_theta_bleed) and _theta_bleed > 0 and _fev_dte > 3:
                            _bleed_icon = "⚠️" if _theta_flag else "✅"
                            st.caption(
                                f"{_bleed_icon} Theta bleed: {_theta_bleed:.1f}%/day of remaining premium"
                                + (" — exceeds 3% flag threshold" if _theta_flag else "")
                            )

                        # Conviction status
                        if _conv_status:
                            _conv_icons = {
                                "STRENGTHENING": "🟢",
                                "STABLE":        "🟡",
                                "WEAKENING":     "🟠",
                                "REVERSING":     "🔴",
                            }
                            _conv_icon = _conv_icons.get(_conv_status.upper(), "⚪")
                            _streak_int = int(_det_streak) if pd.notna(_det_streak) else 0
                            st.caption(
                                f"{_conv_icon} Conviction: **{_conv_status}** "
                                f"(streak: {_streak_int}d consecutive deteriorating)"
                            )

                        # Directional thesis price target — frozen at entry
                        # Natenberg Ch.11: once stock hits the IV-implied 1-sigma target, edge is captured
                        if _fev_row is not None:
                            _pt_entry    = _fev_row.get('Price_Target_Entry')
                            _ul_now      = float(_fev_row.get('UL Last') or 0)
                            _dte_entry   = _fev_row.get('DTE_Entry')
                            _cp_fev      = str(_fev_row.get('Call/Put') or '').upper()
                            _strategy_fev = str(_fev_row.get('Strategy') or '').upper()
                            _directional = any(s in _strategy_fev for s in ('LONG_PUT','LONG_CALL','BUY_PUT','BUY_CALL','LEAPS'))
                            if _directional and pd.notna(_pt_entry) and float(_pt_entry) > 0:
                                _pt_val = float(_pt_entry)
                                _is_put_fev = 'P' in _cp_fev
                                # Distance remaining to target
                                _dist_to_target = (_ul_now - _pt_val) if _is_put_fev else (_pt_val - _ul_now)
                                _dist_pct = (_dist_to_target / _ul_now * 100) if _ul_now > 0 else 0
                                if _is_put_fev:
                                    _at_target = _ul_now <= _pt_val
                                    _near_target = _ul_now <= _pt_val * 1.05
                                else:
                                    _at_target = _ul_now >= _pt_val
                                    _near_target = _ul_now >= _pt_val * 0.95
                                if _at_target:
                                    _target_icon = "🎯"
                                    _target_status = "AT/BEYOND TARGET — harvest signal"
                                elif _near_target:
                                    _target_icon = "🔔"
                                    _target_status = f"approaching (${_dist_pct:.1f}% away)"
                                else:
                                    _target_icon = "📏"
                                    _target_status = f"${_dist_to_target:.2f} ({_dist_pct:.1f}%) remaining"
                                _dte_entry_str = f", DTE at entry: {int(_dte_entry)}d" if pd.notna(_dte_entry) else ""
                                st.caption(
                                    f"{_target_icon} Thesis target: **${_pt_val:.2f}** "
                                    f"(IV-implied 1σ{_dte_entry_str}) · {_target_status}"
                                )

            # ── Exit Path Breakdown (BUY_WRITE) ──────────────────────────────
            # Passarelli Ch.6: "EXIT" is not one action — it splits into two
            # independent decisions: (1) what to do with the stock, and
            # (2) what to do with the short call. These have different economics.
            _exit_doc = doctrine_by_trade.get(tid)
            _exit_action = str(_exit_doc.get("Action", "")) if _exit_doc is not None else ""
            _exit_spot    = pd.to_numeric(stock_legs.iloc[0].get("UL Last"), errors="coerce") if not stock_legs.empty else float("nan")
            _exit_netcost = pd.to_numeric(stock_legs.iloc[0].get("Net_Cost_Basis_Per_Share"), errors="coerce") if not stock_legs.empty else float("nan")
            _exit_rawcost = pd.to_numeric(stock_legs.iloc[0].get("Basis"), errors="coerce") if not stock_legs.empty else float("nan")
            _exit_qty     = pd.to_numeric(stock_legs.iloc[0].get("Quantity"), errors="coerce") if not stock_legs.empty else float("nan")
            _exit_rawps   = (_exit_rawcost / _exit_qty) if (pd.notna(_exit_rawcost) and pd.notna(_exit_qty) and _exit_qty > 0) else float("nan")
            _exit_effcost = _exit_netcost if (pd.notna(_exit_netcost) and _exit_netcost > 0) else _exit_rawps
            _show_exit_breakdown = (
                _is_bw
                and not stock_legs.empty
                and not opt_legs.empty
                and pd.notna(_exit_spot)
                and pd.notna(_exit_effcost)
            )
            if _show_exit_breakdown:
                with st.expander(
                    "🚪 Exit Path Breakdown",
                    expanded=(_exit_action == "EXIT"),
                ):
                    if _exit_action == "ROLL":
                        st.caption(
                            "**Context: Doctrine = ROLL.** "
                            "The buyback here is NOT an exit — it's decoupling the short call from the stock "
                            "so each leg can be managed independently. "
                            "Passarelli Ch.6: buying back to restructure is different from buying back to exit. "
                            "After the buyback, the stock decision and the next-call-sell decision are separate."
                        )
                    else:
                        st.caption(
                            "**Passarelli Ch.6**: 'EXIT' splits into two separate decisions. "
                            "Never buy back an OTM call just to exit — that destroys premium you've already earned."
                        )

                    # Extract call leg details
                    _exit_opt = opt_legs.iloc[0]
                    _call_last   = pd.to_numeric(_exit_opt.get("Last"), errors="coerce")
                    _call_strike = pd.to_numeric(_exit_opt.get("Strike"), errors="coerce")
                    _call_delta  = pd.to_numeric(_exit_opt.get("Delta"), errors="coerce")
                    _call_dte    = pd.to_numeric(_exit_opt.get("DTE"), errors="coerce")
                    _call_exp    = _exit_opt.get("Expiration")
                    _call_qty    = abs(pd.to_numeric(_exit_opt.get("Quantity"), errors="coerce") or 0)
                    _n_contracts = int(_call_qty)  # 1 contract = 100 shares
                    _n_shares    = int(_exit_qty if pd.notna(_exit_qty) else 0)
                    _spot        = _exit_spot  # alias for readability in this block

                    # Cost to actively buy back the call (vs letting assignment/expiry happen naturally)
                    # For a short call: buyback cost = current Last price × 100 × n_contracts
                    _call_buyback_cost = (
                        float(_call_last) * 100 * _n_contracts
                        if pd.notna(_call_last) and _n_contracts > 0
                        else 0.0
                    )

                    # Determine moneyness
                    _call_otm = pd.notna(_call_strike) and pd.notna(_spot) and _spot < _call_strike
                    _call_itm = pd.notna(_call_strike) and pd.notna(_spot) and _spot >= _call_strike
                    _moneyness = "OTM" if _call_otm else ("ITM" if _call_itm else "ATM")
                    _otm_gap  = (_call_strike - _spot) if (pd.notna(_call_strike) and pd.notna(_spot)) else None
                    _itm_depth = (_spot - _call_strike) if (pd.notna(_call_strike) and pd.notna(_spot)) else None

                    # ─ Moneyness status ─
                    _exp_str = pd.to_datetime(_call_exp).strftime("%b %d") if pd.notna(_call_exp) else "?"
                    _dte_str = f"{int(_call_dte)}d" if pd.notna(_call_dte) else "?"
                    if _call_otm:
                        _mono_icon, _mono_msg = "⬆️", (
                            f"Call is **OTM** — stock \\${_spot:.2f} vs strike \\${_call_strike:.2f} "
                            f"(\\${_otm_gap:.2f} above strike needed). "
                            f"Assignment will NOT occur at expiry unless stock rallies above \\${_call_strike:.2f}."
                        )
                    elif _call_itm:
                        _mono_icon, _mono_msg = "⬇️", (
                            f"Call is **ITM** — stock \\${_spot:.2f} vs strike \\${_call_strike:.2f} "
                            f"(\\${_itm_depth:.2f} in the money). "
                            f"Assignment LIKELY at expiry ({_exp_str}, {_dte_str})."
                        )
                    else:
                        _mono_icon, _mono_msg = "➡️", (
                            f"Call is **ATM** — stock \\${_spot:.2f} ≈ strike \\${_call_strike:.2f}. "
                            f"Assignment is a coin flip at expiry ({_exp_str}, {_dte_str})."
                        )
                    st.markdown(f"{_mono_icon} {_mono_msg}")
                    st.divider()

                    # ─ Expected move banner ─────────────────────────────────
                    _hv_exit = pd.to_numeric(
                        stock_legs.iloc[0].get("HV_20D"), errors="coerce"
                    ) if not stock_legs.empty else float("nan")

                    _matrix_rows, _sigma_move, _ev_path_b = _build_exit_scenario_matrix(
                        spot=float(_spot),
                        strike=float(_call_strike) if pd.notna(_call_strike) else float(_spot),
                        n_shares=_n_shares,
                        call_last=float(_call_last) if pd.notna(_call_last) else None,
                        call_dte=float(_call_dte) if pd.notna(_call_dte) else 5.0,
                        hv=float(_hv_exit) if pd.notna(_hv_exit) else None,
                        call_delta=float(_call_delta) if pd.notna(_call_delta) else None,
                    )

                    if _sigma_move is not None:
                        import math as _math_bv
                        _hv_pct = f"{_hv_exit:.0%}" if _hv_exit < 2 else f"{_hv_exit:.0f}%"
                        _dte_yrs_bv = max(float(_call_dte) if pd.notna(_call_dte) else 1.0, 1) / 252.0
                        _hv_bv = float(_hv_exit) if pd.notna(_hv_exit) and float(_hv_exit or 0) > 0 else 0.0
                        if _hv_bv > 0:
                            _lo1 = _spot * _math_bv.exp(-1 * _hv_bv * _math_bv.sqrt(_dte_yrs_bv))
                            _hi1 = _spot * _math_bv.exp(+1 * _hv_bv * _math_bv.sqrt(_dte_yrs_bv))
                            _lo2 = _spot * _math_bv.exp(-2 * _hv_bv * _math_bv.sqrt(_dte_yrs_bv))
                            _hi2 = _spot * _math_bv.exp(+2 * _hv_bv * _math_bv.sqrt(_dte_yrs_bv))
                        else:
                            _lo1 = _spot - _sigma_move
                            _hi1 = _spot + _sigma_move
                            _lo2 = _spot - 2 * _sigma_move
                            _hi2 = _spot + 2 * _sigma_move
                        st.info(
                            f"📐 **1σ range by {_exp_str}**: \\${_lo1:.2f} – \\${_hi1:.2f}  "
                            f"(HV={_hv_pct}, log-normal)  ·  "
                            f"2σ: \\${_lo2:.2f} – \\${_hi2:.2f}"
                        )

                    # ─ Scenario matrix ───────────────────────────────────────
                    st.markdown(
                        "**Scenario Outcomes — net proceeds by exit path:**  \n"
                        "_Path A = sell everything now (certain). "
                        "Path B = let call expire, sell stock at scenario price. "
                        "Assign (C) = stock called away at strike (only if ITM at expiry)._"
                    )
                    _display_rows = [
                        {k: v for k, v in r.items() if k != "_delta_val"}
                        for r in _matrix_rows
                    ]
                    _matrix_df = pd.DataFrame(_display_rows)
                    if not _matrix_df.empty:
                        st.dataframe(
                            _matrix_df.style.apply(_color_scenario_row, axis=1),
                            hide_index=True,
                            width='stretch',
                        )

                    # ─ Expected-value summary ─────────────────────────────────
                    if _ev_path_b is not None and _matrix_rows:
                        # Path A net is constant — pull from first data row
                        _pa_str = _matrix_rows[0].get("Exit Now (A)", "$0")
                        try:
                            _pa_net = float(_pa_str.replace("$","").replace(",",""))
                        except Exception:
                            _pa_net = 0.0
                        _ev_delta = _ev_path_b - _pa_net
                        if _ev_delta > 0:
                            if _call_itm:
                                # ITM call: EV of waiting > exit now.
                                # This means: probability-weighted proceeds of (assignment @strike OR
                                # selling below strike if stock drops) beat paying the buyback NOW.
                                # The edge source is avoiding the buyback cost — not a downside gift.
                                _ev_rationale = (
                                    f"**Passarelli Ch.6**: call is ITM — "
                                    f"waiting avoids the \\${_call_buyback_cost:,.0f} buyback cost entirely. "
                                    f"If stock stays above \\${_call_strike:.2f}, assignment delivers \\${_call_strike:.2f}/share "
                                    f"without paying to close the call. "
                                    f"Only if stock drops well below strike does the downside "
                                    f"outweigh the saved buyback cost."
                                )
                            else:
                                _ev_rationale = (
                                    f"**Passarelli Ch.6**: call is OTM with {_dte_str} left — "
                                    f"letting it expire captures the remaining time value at no cost."
                                )
                            st.success(
                                f"**Probability-weighted edge of waiting**: +\\${_ev_delta:,.0f} vs exiting now  "
                                f"— expected proceeds of Path B exceed exiting now. {_ev_rationale}"
                            )
                        else:
                            if _call_itm:
                                # EV of waiting < exit now: the downside tail (stock drops far below
                                # strike, Path B sells at market < strike) outweighs the saved buyback.
                                _ev_rationale = (
                                    f"**McMillan Ch.6**: call is ITM — "
                                    f"the downside tail (stock drops far below \\${_call_strike:.2f}, "
                                    f"Path B sells at market) outweighs the \\${_call_buyback_cost:,.0f} saved by not buying back. "
                                    f"Consider buying back the call now to lock in the certain proceeds."
                                )
                            else:
                                _ev_rationale = (
                                    f"**McMillan Ch.3**: when stock is below hard stop, "
                                    f"premium income cannot outrun continued deterioration."
                                )
                            st.warning(
                                f"**Probability-weighted**: exiting now is \\${abs(_ev_delta):,.0f} better — "
                                f"the downside scenarios dominate. {_ev_rationale}"
                            )

                    # Doctrine note
                    st.divider()
                    if _call_otm:
                        if _exit_action == "ROLL":
                            st.caption(
                                f"**Decision framework (OTM call, ROLL doctrine, {_dte_str} to expiry):**  "
                                f"1️⃣ **Call decision (urgent):** Buy back the short call at \\${_call_last:.2f} "
                                f"to stop gamma bleed and decouple from the stock — this is the ROLL action.  "
                                f"2️⃣ **Stock decision (separate):** After the buyback, decide the stock independently: "
                                f"sell if thesis broken, hold if temporary, then re-sell a new 30–45 DTE call.  "
                                f"These are two distinct actions with different urgencies. The call buyback is the urgent one."
                            )
                        else:
                            st.caption(
                                f"**Decision framework (OTM call, {_dte_str} to expiry):**  "
                                f"1️⃣ Stock decision: sell now vs hold through expiry.  "
                                f"2️⃣ Call decision: independent — it expires worthless automatically unless stock rallies above \\${_call_strike:.2f}.  "
                                f"Don't conflate the two. If you want out of the stock, sell the stock. "
                                f"You do not need to buy back the call to exit the stock position."
                            )
                    elif _call_itm:
                        # Net proceeds of each path:
                        #   Accept assignment: strike × n_shares (no buyback cost, stock called away)
                        #   Active exit now:   spot × n_shares − buyback_cost
                        _assignment_net  = int(_call_strike * _n_shares) if pd.notna(_call_strike) else 0
                        _active_exit_net = int(_spot * _n_shares - _call_buyback_cost)
                        st.caption(
                            f"**Decision framework (ITM call, {_dte_str} to expiry):**  "
                            f"1️⃣ Accept assignment — stock called away at \\${_call_strike:.2f}/share = **\\${_assignment_net:,} net** "
                            f"(no buyback cost, cleaner exit).  "
                            f"2️⃣ Active exit now — sell stock at \\${_spot:.2f} and buy back call = "
                            f"**\\${_active_exit_net:,} net** (\\${_call_buyback_cost:,.0f} buyback cost deducted).  "
                            f"**McMillan Ch.6**: if strike > net cost basis, assignment is acceptable — you're exiting with a gain. "
                            f"Active exit only makes sense if you need to choose the exact timing."
                        )

                    # ── Exit Winner Panel (BUY_WRITE) ─────────────────────────
                    # Only render when doctrine action is EXIT. Synthesises the
                    # ambiguous "EXIT" command into one mechanical directive:
                    # what exactly to do with the call, what exactly to do with
                    # the stock, whether this is a buyback / assignment / roll /
                    # re-entry decision — so the user never has to guess.
                    if _exit_action == "EXIT":
                        st.divider()
                        st.markdown("### 🏆 Exit Winner Panel — What to Execute")

                        # ── Determine recommended path ──────────────────────
                        # Decision tree (Passarelli Ch.6 + McMillan Ch.6):
                        #   ITM + DTE≤7  → accept assignment (do nothing on call)
                        #   ITM + DTE>7  → accept assignment preferred if assignment_net≥active_exit_net
                        #                  else active exit (buyback + sell stock)
                        #   OTM + DTE≤7  → let call expire; sell stock separately if wanted
                        #   OTM + DTE>7  → sell stock first (independent); let call ride/expire
                        # In no case does "EXIT" mean "roll" or "open a new position".

                        _ewp_dte        = float(_call_dte) if pd.notna(_call_dte) else 30.0
                        _ewp_itm        = _call_itm
                        _ewp_otm        = _call_otm
                        _ewp_assign_net = float(_call_strike * _n_shares) if (pd.notna(_call_strike) and _n_shares > 0) else 0.0
                        _ewp_active_net = float(_spot * _n_shares - _call_buyback_cost)
                        _ewp_basis      = float(_exit_effcost) if pd.notna(_exit_effcost) else 0.0
                        _ewp_gain       = (_ewp_assign_net - _ewp_basis * _n_shares) if (_ewp_itm and _n_shares > 0) else 0.0
                        _ewp_profitable = _ewp_assign_net > (_ewp_basis * _n_shares) if _n_shares > 0 else False
                        _ewp_bb_cost    = _call_buyback_cost  # alias

                        if _ewp_itm:
                            if _ewp_dte <= 7:
                                _ewp_rec        = "ACCEPT_ASSIGNMENT"
                                _ewp_rec_label  = "Accept Assignment (do nothing on call)"
                                _ewp_rec_icon   = "🟢"
                                _ewp_rationale  = (
                                    f"Call is ITM with only {int(_ewp_dte)}d left — assignment is near-certain. "
                                    f"Buying back now costs \\${_ewp_bb_cost:,.0f} to avoid something that will happen anyway. "
                                    f"Do nothing on the call; broker handles assignment at expiry."
                                )
                                _ewp_call_action  = "Do nothing — let assignment execute at expiry."
                                _ewp_stock_action = f"Stock called away at \\${_call_strike:.2f}/share automatically."
                                _ewp_not_a_roll   = True
                            else:
                                # DTE>7: compare net proceeds
                                if _ewp_assign_net >= _ewp_active_net:
                                    _ewp_rec        = "ACCEPT_ASSIGNMENT"
                                    _ewp_rec_label  = "Accept Assignment (preferred — higher net)"
                                    _ewp_rec_icon   = "🟢"
                                    _ewp_rationale  = (
                                        f"Assignment delivers \\${_ewp_assign_net:,.0f} (\\${_call_strike:.2f}/share, no buyback). "
                                        f"Active exit nets \\${_ewp_active_net:,.0f} after \\${_ewp_bb_cost:,.0f} buyback. "
                                        f"Waiting for natural assignment is \\${_ewp_assign_net - _ewp_active_net:,.0f} better — "
                                        f"but only if you can tolerate {int(_ewp_dte)}d more exposure."
                                    )
                                    _ewp_call_action  = "Do nothing — let assignment execute at expiry."
                                    _ewp_stock_action = f"Stock called away at \\${_call_strike:.2f}/share automatically."
                                    _ewp_not_a_roll   = True
                                else:
                                    _ewp_rec        = "ACTIVE_EXIT"
                                    _ewp_rec_label  = "Active Exit Now (buyback + sell stock)"
                                    _ewp_rec_icon   = "🟠"
                                    _ewp_rationale  = (
                                        f"Active exit nets \\${_ewp_active_net:,.0f} vs assignment \\${_ewp_assign_net:,.0f}. "
                                        f"Buyback cost \\${_ewp_bb_cost:,.0f} is offset by selling stock at market (\\${_spot:.2f}) "
                                        f"rather than being locked into \\${_call_strike:.2f} strike. "
                                        f"Use this path when you need to control exact exit timing."
                                    )
                                    _ewp_call_action  = f"Buy to close the \\${_call_strike:.0f} call (limit at mid \\${float(_call_last):.2f} if live)."
                                    _ewp_stock_action = f"Sell stock at market / limit after call is closed."
                                    _ewp_not_a_roll   = True
                        else:
                            # OTM call
                            if _ewp_dte <= 7:
                                _ewp_rec        = "LET_EXPIRE"
                                _ewp_rec_label  = "Let Call Expire + Sell Stock Separately"
                                _ewp_rec_icon   = "🟢"
                                _ewp_rationale  = (
                                    f"Call is OTM with {int(_ewp_dte)}d left — buying back costs \\${_ewp_bb_cost:,.0f} "
                                    f"to avoid something worth near zero at expiry. Wasteful. "
                                    f"Sell the stock independently now if desired; call expires worthless on its own."
                                )
                                _ewp_call_action  = "Do nothing — OTM call expires worthless in ≤7 days."
                                _ewp_stock_action = f"Sell stock separately (limit near \\${_spot:.2f}) if you want out now."
                                _ewp_not_a_roll   = True
                            else:
                                _ewp_rec        = "SELL_STOCK_LET_CALL_RIDE"
                                _ewp_rec_label  = "Sell Stock Now, Let Call Ride"
                                _ewp_rec_icon   = "🟡"
                                _ewp_rationale  = (
                                    f"Call is OTM — the two legs are independent decisions. "
                                    f"Stock: sell now at \\${_spot:.2f} to lock the gain. "
                                    f"Call: remains short — it can expire worthless or you can buy it back later "
                                    f"if it gets cheap (< 20% of original credit). "
                                    f"Passarelli: 'Never buy back an OTM call just to exit the stock.'"
                                )
                                _ewp_call_action  = "Leave the short call open — it is OTM. Close it only if cheap (<20% of original premium)."
                                _ewp_stock_action = f"Sell stock now (limit near \\${_spot:.2f})."
                                _ewp_not_a_roll   = True

                        # ── "Is this a roll / re-entry?" — explicit clarification ──
                        # This is the most common confusion point. EXIT never means roll.
                        _ewp_clarify_col1, _ewp_clarify_col2 = st.columns(2)
                        with _ewp_clarify_col1:
                            st.markdown(f"**{_ewp_rec_icon} Recommended: {_ewp_rec_label}**")
                            _urgency_fn_ewp = st.error if urgency == "CRITICAL" else (st.warning if urgency == "HIGH" else st.info)
                            _urgency_fn_ewp(_ewp_rationale)

                        with _ewp_clarify_col2:
                            st.markdown("**What 'EXIT' means — and doesn't:**")
                            st.markdown(
                                "✅ **EXIT = close this position entirely**  \n"
                                "❌ EXIT ≠ roll to a new strike  \n"
                                "❌ EXIT ≠ open a replacement position  \n"
                                "❌ EXIT ≠ convert to a different strategy  \n\n"
                                "After closing, re-assess from scratch. "
                                "If the thesis recovers, the **scan engine** will surface a new entry — "
                                "do not pre-emptively roll to 'stay in the trade.'"
                            )

                        # ── Mechanical steps ─────────────────────────────────
                        st.markdown("**Execution Steps:**")
                        _mech_c1, _mech_c2 = st.columns(2)
                        with _mech_c1:
                            st.markdown("**1. Call leg**")
                            st.info(_ewp_call_action)
                        with _mech_c2:
                            st.markdown("**2. Stock leg**")
                            st.info(_ewp_stock_action)

                        # ── Net proceeds summary ─────────────────────────────
                        if _n_shares > 0 and pd.notna(_call_strike):
                            _ewp_cols = st.columns(3)
                            if _ewp_itm:
                                _ewp_cols[0].metric(
                                    "Accept assignment",
                                    f"\\${_ewp_assign_net:,.0f}",
                                    f"\\${_call_strike:.2f}/share × {_n_shares} shares",
                                )
                                _ewp_cols[1].metric(
                                    "Active exit now",
                                    f"\\${_ewp_active_net:,.0f}",
                                    f"after \\${_ewp_bb_cost:,.0f} buyback",
                                )
                            else:
                                _ewp_cols[0].metric(
                                    "Sell stock at market",
                                    f"\\${int(_spot * _n_shares):,}",
                                    f"\\${_spot:.2f} × {_n_shares} shares",
                                )
                                _ewp_cols[1].metric(
                                    "Buyback OTM call (skip)",
                                    f"\\${_ewp_bb_cost:,.0f}",
                                    "Not recommended — OTM",
                                )
                            _gain_str = f"+\\${_ewp_gain:,.0f}" if _ewp_gain >= 0 else f"-\\${abs(_ewp_gain):,.0f}"
                            _ewp_cols[2].metric(
                                "Gain vs cost basis",
                                _gain_str,
                                f"basis \\${_ewp_basis:.2f}/share" if _ewp_basis > 0 else "basis unknown",
                            )

                        # ── Timing window ────────────────────────────────────
                        st.caption(
                            "**Best execution window** (Passarelli Ch.6): "
                            "1:00–3:30 PM ET for stock (avoid open volatility spike and EOD spread widening). "
                            "For call buyback: execute before 2:00 PM ET to avoid gamma acceleration "
                            "near close if ITM."
                        )

            # ── Weekend Roll Pre-Staging ──────────────────────────────────────
            # When market is closed, pre-compute roll scenarios for review.
            # On Monday open, the user can act immediately with scenarios already evaluated.
            # Passarelli Ch.6: "Never roll reactively — plan the roll in advance."
            _doc_row = doctrine_by_trade.get(tid)
            _doc_action = str(_doc_row.get("Action", "")) if _doc_row is not None else ""
            _is_weekend_or_closed = True  # always show pre-staging; market check done inside

            try:
                import datetime as _dt
                # Always evaluate market hours in US/Eastern regardless of local timezone.
                # datetime.now() returns local time — on PST machines market opens at 6:30am
                # but the naive hour check (9 <= hour < 16) would fail until 9am PST (12pm ET).
                try:
                    import zoneinfo as _zi
                    _et = _dt.datetime.now(_zi.ZoneInfo("America/New_York"))
                except ImportError:
                    # Python <3.9 fallback: use UTC offset (ET = UTC-5 winter, UTC-4 summer)
                    import time as _time_mod
                    _utc_offset = -5 if _time_mod.daylight == 0 else -4
                    _et = _dt.datetime.utcnow() + _dt.timedelta(hours=_utc_offset)
                _weekday = _et.weekday()   # 0=Mon … 4=Fri, 5=Sat, 6=Sun
                # Regular session: 9:30am – 4:00pm ET
                _market_hour = (_et.hour == 9 and _et.minute >= 30) or (10 <= _et.hour < 16)
                _is_market_open = (_weekday < 5) and _market_hour
            except Exception:
                _is_market_open = False

            # ── Close Position block — directional long option EXIT ───────────────
            # LONG_CALL / LONG_PUT / LEAPS EXIT: doctrine says close.
            # Provide explicit close mechanics — limit at mid, proceeds, loss realised.
            _DIRECTIONAL_LONG_STRUCTS = {
                "LONG_CALL", "BUY_CALL", "LONG_PUT", "BUY_PUT",
                "LEAPS_CALL", "LEAPS_PUT",
            }
            _is_directional_long = str(entry_structure).upper() in _DIRECTIONAL_LONG_STRUCTS
            if _doc_action == "EXIT" and _is_directional_long and not opt_legs.empty:
                with st.expander("🚪 Close Position — Execution Mechanics", expanded=True):
                    # Context-aware exit message — not all EXITs are thesis-broken.
                    # Time value exhaustion (C4), profit targets, and delta collapse
                    # are structurally sound exits on winning positions.
                    _exit_source = str(_doc_row.get("Doctrine_Source", "")) if _doc_row is not None else ""
                    _exit_thesis = str(_doc_row.get("Thesis_State", "")) if _doc_row is not None else ""
                    if "Time Value Exhausted" in _exit_source:
                        st.warning(
                            "**Doctrine Action: EXIT** — time value exhausted. "
                            "Option is nearly 100% intrinsic — holding pays theta for no additional premium. "
                            "Close to capture the intrinsic gain; re-enter next cycle if thesis still intact."
                        )
                    elif "Profit Target" in _exit_source or "Winner" in _exit_source:
                        st.success(
                            "**Doctrine Action: EXIT** — profit target reached. "
                            "Close to lock in gains. Re-enter if thesis persists and setup recurs."
                        )
                    elif _exit_thesis.upper() == "INTACT":
                        st.warning(
                            "**Doctrine Action: EXIT** — structural trigger fired (thesis still intact). "
                            "Close position per doctrine gate; re-evaluate re-entry if conditions improve."
                        )
                    else:
                        st.error(
                            "**Doctrine Action: EXIT** — thesis is BROKEN. "
                            "Do not roll — rolling buys more time for the same broken thesis. "
                            "Close the position and redeploy when structure recovers."
                        )
                    for _, _cl_leg in opt_legs.iterrows():
                        _cl_qty    = abs(int(_cl_leg.get("Quantity", 1) or 1))
                        _cl_last   = pd.to_numeric(_cl_leg.get("Last"), errors="coerce")
                        _cl_bid    = pd.to_numeric(_cl_leg.get("Bid"), errors="coerce")
                        _cl_ask    = pd.to_numeric(_cl_leg.get("Ask"), errors="coerce")
                        _cl_basis  = pd.to_numeric(_cl_leg.get("Basis"), errors="coerce")
                        _cl_gl     = pd.to_numeric(_cl_leg.get("$ Total G/L"), errors="coerce")
                        _cl_strike = _cl_leg.get("Strike")
                        _cl_cp     = str(_cl_leg.get("Call/Put") or _cl_leg.get("OptionType") or "")
                        _cl_exp    = _cl_leg.get("Expiration")
                        _cl_exp_s  = pd.to_datetime(_cl_exp).strftime("%b %d '%y") if pd.notna(_cl_exp) else "—"
                        _cl_dte    = pd.to_numeric(_cl_leg.get("DTE"), errors="coerce")

                        # Mid price for limit order
                        if pd.notna(_cl_bid) and pd.notna(_cl_ask) and _cl_ask > 0:
                            _cl_mid = (_cl_bid + _cl_ask) / 2
                        elif pd.notna(_cl_last) and _cl_last > 0:
                            _cl_mid = _cl_last
                        else:
                            _cl_mid = None

                        _cl_proceeds = _cl_mid * 100 * _cl_qty if _cl_mid else None
                        _cl_realised = (_cl_gl * _cl_qty) if pd.notna(_cl_gl) else None

                        st.markdown(
                            f"**{_cl_cp} ${_cl_strike:.0f}** exp {_cl_exp_s}"
                            + (f" · DTE {int(_cl_dte)}d" if pd.notna(_cl_dte) else "")
                            + f" · {_cl_qty} contract{'s' if _cl_qty > 1 else ''}"
                        )
                        _cc1, _cc2, _cc3, _cc4 = st.columns(4)
                        if _cl_mid:
                            _cc1.metric("Sell limit at (mid)", f"${_cl_mid:.2f}")
                        else:
                            _cc1.metric("Sell limit at (mid)", "—")
                            _cc1.caption("No live price — check chain")
                        if pd.notna(_cl_bid) and pd.notna(_cl_ask):
                            _cc2.metric("Bid / Ask", f"${_cl_bid:.2f} / ${_cl_ask:.2f}")
                        else:
                            _cc2.metric("Bid / Ask", "—")
                        if _cl_proceeds:
                            _cc3.metric("Proceeds (mid)", f"${_cl_proceeds:,.0f}")
                            _cc3.caption(f"{_cl_qty} × ${_cl_mid:.2f} × 100")
                        else:
                            _cc3.metric("Proceeds (mid)", "—")
                        if pd.notna(_cl_gl):
                            _gl_total = _cl_gl
                            _cc4.metric(
                                "Realised G/L",
                                f"{'−' if _gl_total < 0 else '+'}${abs(_gl_total):,.0f}",
                            )
                        else:
                            _cc4.metric("Realised G/L", "—")

                        if pd.notna(_cl_bid) and _cl_mid:
                            _spread_pct = (_cl_ask - _cl_bid) / _cl_mid * 100 if _cl_mid > 0 else 0
                            if _spread_pct > 5:
                                st.caption(
                                    f"⚠️ Wide spread {_spread_pct:.1f}% — use limit at mid \\${_cl_mid:.2f}.  \n"
                                    f"Do NOT hit the bid (\\${_cl_bid:.2f})."
                                )
                            else:
                                st.caption(
                                    f"Spread {_spread_pct:.1f}% — limit at mid \\${_cl_mid:.2f} should fill promptly."
                                )

                        # ── GTC / Optimize Exit toggle ────────────────────────
                        # Two modes: GTC (set-and-forget) vs Optimize (timing-aware).
                        # Direction: long option STC = sell-to-close.
                        # Best STC window = early session IV peak (opposite of BTC).
                        _exit_mode_key = f"exit_mode_{tid}_{_cl_strike}"
                        if _exit_mode_key not in st.session_state:
                            st.session_state[_exit_mode_key] = "GTC"

                        _em_col1, _em_col2 = st.columns(2)
                        if _em_col1.button(
                            "📋 GTC — Set & Forget",
                            key=f"gtc_btn_{tid}_{_cl_strike}",
                            type="primary" if st.session_state[_exit_mode_key] == "GTC" else "secondary",
                            width='stretch',
                        ):
                            st.session_state[_exit_mode_key] = "GTC"
                            st.rerun()
                        if _em_col2.button(
                            "⏰ Optimize Exit",
                            key=f"opt_btn_{tid}_{_cl_strike}",
                            type="primary" if st.session_state[_exit_mode_key] == "OPTIMIZE" else "secondary",
                            width='stretch',
                        ):
                            st.session_state[_exit_mode_key] = "OPTIMIZE"
                            st.rerun()

                        st.divider()

                        if st.session_state[_exit_mode_key] == "GTC":
                            # ── GTC mode ─────────────────────────────────────
                            if _cl_mid:
                                _gtc_step   = 0.05
                                _gtc_floor  = round(_cl_bid, 2) if pd.notna(_cl_bid) else None
                                _gtc_steps  = max(1, int((_cl_mid - (_gtc_floor or _cl_mid)) / _gtc_step)) if _gtc_floor else 1
                                st.markdown("**📋 GTC Order — Set & Forget**")
                                _g1, _g2, _g3 = st.columns(3)
                                _g1.metric("Limit (mid)", f"\\${_cl_mid:.2f}")
                                _g2.metric("Floor (bid)", f"\\${_gtc_floor:.2f}" if _gtc_floor else "—")
                                _g3.metric("Step-down", f"\\$0.05 / 15 min")
                                st.info(
                                    f"Enter **sell limit \\${_cl_mid:.2f} GTC**.  \n"
                                    f"If unfilled after 15 min → step down \\$0.05 toward \\${_gtc_floor:.2f}.  \n"
                                    f"Max {_gtc_steps} step{'s' if _gtc_steps != 1 else ''} before accepting bid.  \n"
                                    "✅ Check back tomorrow if unfilled — GTC stays live."
                                )
                                if urgency in ("HIGH", "CRITICAL"):
                                    st.warning(
                                        f"⚠️ Urgency {urgency} — don't let this sit more than 1 session. "
                                        "Accept bid if unfilled by EOD."
                                    )
                            else:
                                st.info("No live price available — check chain for current bid/ask before entering GTC order.")

                        else:
                            # ── Optimize mode ────────────────────────────────
                            # Pull timing signals from the option leg + doc row
                            _opt_iv_roc3  = pd.to_numeric(_cl_leg.get("IV_ROC_3D"), errors="coerce")
                            _opt_iv_now   = pd.to_numeric(_cl_leg.get("IV_Now") or _cl_leg.get("IV_30D"), errors="coerce")
                            _opt_hv       = pd.to_numeric(_cl_leg.get("HV_20D"), errors="coerce")
                            _opt_theta    = abs(pd.to_numeric(_cl_leg.get("Theta"), errors="coerce") or 0) * 100
                            _opt_delta    = abs(pd.to_numeric(_cl_leg.get("Delta"), errors="coerce") or 0)
                            _opt_dte      = pd.to_numeric(_cl_leg.get("DTE"), errors="coerce")
                            _opt_hv_move  = pd.to_numeric(_cl_leg.get("HV_Daily_Move_1Sigma"), errors="coerce")

                            # Normalize IV/HV to pct
                            _iv_pct = float(_opt_iv_now) * 100 if pd.notna(_opt_iv_now) and float(_opt_iv_now) < 5 else float(_opt_iv_now) if pd.notna(_opt_iv_now) else None
                            _hv_pct = float(_opt_hv) * 100 if pd.notna(_opt_hv) and float(_opt_hv) < 5 else float(_opt_hv) if pd.notna(_opt_hv) else None
                            _iv_roc = float(_opt_iv_roc3) if pd.notna(_opt_iv_roc3) else 0.0
                            _dte_i  = int(_opt_dte) if pd.notna(_opt_dte) else None
                            _theta_day = _opt_theta if _opt_theta > 0 else None

                            st.markdown("**⏰ Optimized Exit — Timing & Trigger**")

                            # Urgency gate: HIGH/CRITICAL overrides all timing optimization
                            if urgency in ("HIGH", "CRITICAL"):
                                st.error(
                                    f"🚨 **Urgency {urgency} — execute today, don't wait for optimal window.**  \n"
                                    "Timing refinement is secondary when doctrine says EXIT HIGH/CRITICAL. "
                                    "If market is open: enter GTC now at mid. Accept bid by EOD if unfilled."
                                )

                            # DTE gate: ≤7 DTE → gamma risk dominates, don't optimize
                            if _dte_i is not None and _dte_i <= 7:
                                st.warning(
                                    f"⚡ **DTE {_dte_i} — gamma risk outweighs timing benefit.**  \n"
                                    "Don't wait for an optimal window. Gamma is convex this close to expiry — "
                                    "a single adverse move can destroy remaining value. Execute now."
                                )

                            # Timing window (long STC: want IV high → morning session)
                            _best_window = "9:45–10:30 AM ET"
                            _avoid_window = "1:00–3:30 PM ET"
                            _window_reason = "IV tends to peak at open — vega lifts long option proceeds"
                            _avoid_reason  = "IV trough in afternoon compresses your sale price"

                            # If IV is already rising (ROC > 0), morning peak is more likely
                            # If IV ROC < 0 (falling), a bounce back is less certain — wait for catalyst
                            _iv_signal = ""
                            if _iv_roc > 0.02:
                                _iv_signal = f"✅ IV ROC +{_iv_roc:.2f} (rising) — morning IV spike likely. Favor open window."
                            elif _iv_roc < -0.02:
                                _iv_signal = f"⚠️ IV ROC {_iv_roc:.2f} (falling) — vol compressing. Consider waiting for a stock bounce to lift delta before closing."
                            else:
                                _iv_signal = f"IV ROC {_iv_roc:+.2f} (flat) — no strong timing signal. Default to morning window."

                            _tw1, _tw2 = st.columns(2)
                            _tw1.success(f"**Best window**  \n{_best_window}  \n{_window_reason}")
                            _tw2.warning(f"**Avoid**  \n{_avoid_window}  \n{_avoid_reason}")

                            if _iv_signal:
                                st.caption(_iv_signal)

                            # Trigger: what to watch for before pulling the trigger
                            st.markdown("**📡 Entry Trigger**")
                            _triggers = []
                            if _iv_roc >= 0:
                                _triggers.append("IV opens higher than prior close → vega adds to proceeds → enter limit at mid")
                            if _hv_pct and _opt_hv_move and pd.notna(_opt_hv_move):
                                _bounce_tgt = round(float(_opt_hv_move) * 0.3, 2)
                                _triggers.append(f"Stock intraday bounce ≥ \\${_bounce_tgt:.2f} (0.3× 1σ HV move) → delta lift → improves mid")
                            if _opt_delta < 0.20:
                                _triggers.append("⚠️ Delta very low — stock needs a meaningful bounce to lift this. Don't wait indefinitely; theta cost is real.")
                            for _t in _triggers:
                                st.caption(f"• {_t}")

                            # Theta deadline
                            if _theta_day and _theta_day > 0 and _cl_mid:
                                _theta_cost_2d = _theta_day * 2
                                _theta_pct_2d  = _theta_cost_2d / (_cl_mid * 100) * 100
                                _max_wait_days = max(1, int((_cl_mid * 100 * 0.05) / _theta_day))  # 5% of value
                                st.caption(
                                    f"⏳ **Theta deadline:** \\${_theta_day:.2f}/day bleed.  \n"
                                    f"Waiting 2 days costs \\${_theta_cost_2d:.2f} ({_theta_pct_2d:.1f}% of proceeds).  \n"
                                    f"Hard deadline: close within **{_max_wait_days} day{'s' if _max_wait_days != 1 else ''}** "
                                    "— after that, timing benefit < theta cost of waiting."
                                )

                            # Order mechanics
                            if _cl_mid:
                                st.markdown("**📋 Order Mechanics**")
                                _o1, _o2, _o3 = st.columns(3)
                                _o1.metric("Start limit", f"\\${_cl_mid:.2f}")
                                _floor = round(_cl_bid, 2) if pd.notna(_cl_bid) else None
                                _o2.metric("Floor (max 1 step)", f"\\${round(_cl_mid - 0.05, 2):.2f}")
                                _o3.metric("Accept bid if", "unfilled at close")
                                st.caption(
                                    f"Enter limit \\${_cl_mid:.2f} at open of best window.  \n"
                                    f"If unfilled after 15 min → step down once to \\${round(_cl_mid - 0.05, 2):.2f}.  \n"
                                    f"Accept bid (\\${_floor:.2f}) only at EOD if still unfilled." if _floor else
                                    f"Enter limit \\${_cl_mid:.2f} at open of best window. Step down \\$0.05 once if unfilled."
                                )

            # ── Trim Execution block — directional long option TRIM ──────────────
            # TRIM has two distinct variants with different rationale:
            #
            #   PROFIT TRIM (Gate 3b-pre): option is up 50%+ on multi-contract position.
            #     → Bank gains on half, let the rest run free-riding on locked profits.
            #     → McMillan Ch.4: "Partial profit-taking locks gains and resets risk on the survivor."
            #
            #   RISK-REDUCTION TRIM (Gate 2a): delta redundancy — stock position already
            #     provides full directional exposure; this call adds overlapping risk at a loss.
            #     → Close half to reduce redundant exposure. NOT a profit-taking action.
            #     → McMillan Ch.4: "Portfolio Delta Management — avoid doubling directional risk."
            #
            # Detect which variant: profit trim = position is net positive; risk trim = at a loss
            # or rationale contains redundancy/delta keywords.
            if _doc_action == "TRIM" and _is_directional_long and not opt_legs.empty:
                # Classify trim type from P&L and rationale
                _trim_rationale = str(_doc_row.get("Rationale", "") or "") if _doc_row is not None else ""
                _trim_total_gl  = pd.to_numeric(
                    (_doc_row.get("$ Total G/L") if _doc_row is not None else None), errors="coerce"
                )
                _is_risk_trim = (
                    (pd.notna(_trim_total_gl) and _trim_total_gl < 0)
                    or any(kw in _trim_rationale.lower() for kw in
                           ("redundant", "at a loss", "delta management", "delta redundancy",
                            "already long from stock", "overlapping"))
                )

                with st.expander("✂️ Trim Position — Execution Mechanics", expanded=True):
                    if _is_risk_trim:
                        st.warning(
                            "**Doctrine Action: TRIM (Risk Reduction)** — close half to reduce "
                            "redundant directional exposure. This position overlaps with existing "
                            "stock/other option delta on the same ticker. "
                            "This is NOT a profit-taking action — you are closing at a loss to cut risk."
                        )
                    else:
                        st.warning(
                            "**Doctrine Action: TRIM** — bank partial gains, keep residual exposure. "
                            "This is a Sell-to-Close on a portion of the position, not a roll."
                        )
                    for _, _tr_leg in opt_legs.iterrows():
                        _tr_qty_total = abs(int(_tr_leg.get("Quantity", 1) or 1))
                        # Trim half, keep half (engine logic: floor(qty/2) trimmed)
                        _tr_qty_close = max(1, int(_tr_qty_total / 2))
                        _tr_qty_keep  = _tr_qty_total - _tr_qty_close
                        _tr_last   = pd.to_numeric(_tr_leg.get("Last"), errors="coerce")
                        _tr_bid    = pd.to_numeric(_tr_leg.get("Bid"),  errors="coerce")
                        _tr_ask    = pd.to_numeric(_tr_leg.get("Ask"),  errors="coerce")
                        _tr_gl     = pd.to_numeric(_tr_leg.get("$ Total G/L"), errors="coerce")
                        _tr_strike = _tr_leg.get("Strike")
                        _tr_cp     = str(_tr_leg.get("Call/Put") or _tr_leg.get("OptionType") or "")
                        _tr_exp    = _tr_leg.get("Expiration")
                        _tr_exp_s  = pd.to_datetime(_tr_exp).strftime("%b %d '%y") if pd.notna(_tr_exp) else "—"
                        _tr_dte    = pd.to_numeric(_tr_leg.get("DTE"), errors="coerce")

                        if pd.notna(_tr_bid) and pd.notna(_tr_ask) and _tr_ask > 0:
                            _tr_mid = (_tr_bid + _tr_ask) / 2
                        elif pd.notna(_tr_last) and _tr_last > 0:
                            _tr_mid = _tr_last
                        else:
                            _tr_mid = None

                        _tr_proceeds_close = _tr_mid * 100 * _tr_qty_close if _tr_mid else None
                        # P/L attribution: split total GL proportionally across contracts
                        _tr_gl_per_contract = _tr_gl / _tr_qty_total if (pd.notna(_tr_gl) and _tr_qty_total > 0) else None
                        _tr_gl_closed = _tr_gl_per_contract * _tr_qty_close if _tr_gl_per_contract is not None else None
                        _tr_gl_remain = _tr_gl_per_contract * _tr_qty_keep  if (_tr_gl_per_contract is not None and _tr_qty_keep > 0) else None

                        st.markdown(
                            f"**{_tr_cp} \\${_tr_strike:.0f}** exp {_tr_exp_s}"
                            + (f" · DTE {int(_tr_dte)}d" if pd.notna(_tr_dte) else "")
                            + f" · Total: {_tr_qty_total} contract{'s' if _tr_qty_total > 1 else ''}"
                        )
                        _trc1, _trc2, _trc3, _trc4 = st.columns(4)
                        with _trc1:
                            _close_help = (
                                "Sell-to-Close this many contracts to reduce delta redundancy — not to capture a gain"
                                if _is_risk_trim else
                                "Sell-to-Close this many contracts at mid to bank partial gains"
                            )
                            st.metric("Close (STC)",
                                      f"{_tr_qty_close} contract{'s' if _tr_qty_close > 1 else ''}",
                                      help=_close_help)
                        with _trc2:
                            if _tr_qty_keep > 0:
                                _keep_help = (
                                    "Remaining contracts stay open — redundant exposure reduced, thesis can still run"
                                    if _is_risk_trim else
                                    "Remaining contracts stay open — thesis still running, cost basis now protected"
                                )
                                st.metric("Keep open",
                                          f"{_tr_qty_keep} contract{'s' if _tr_qty_keep > 1 else ''}",
                                          help=_keep_help)
                            else:
                                st.metric("Keep open", "0 — full close",
                                          help="Single contract: closing the full position. Consider EXIT instead if thesis is broken.")
                        with _trc3:
                            if _tr_mid:
                                st.metric("Sell limit at (mid)", f"\\${_tr_mid:.2f}",
                                          help="Enter limit order at mid. If unfilled after 15 min, step down $0.05 toward bid.")
                            else:
                                st.metric("Sell limit at (mid)", "—")
                        with _trc4:
                            if _tr_proceeds_close:
                                _proc_label = "Proceeds (partial exit)" if _is_risk_trim else "Proceeds locked"
                                st.metric(_proc_label,
                                          f"\\${_tr_proceeds_close:,.0f}",
                                          help=f"{_tr_qty_close} × \\${_tr_mid:.2f} × 100")
                            else:
                                st.metric("Proceeds (partial exit)" if _is_risk_trim else "Proceeds locked", "—")

                        # G/L summary — context-aware framing
                        if _tr_gl_closed is not None:
                            _gl_icon = "🟢" if _tr_gl_closed >= 0 else "🔴"
                            _gl_sign = "+" if _tr_gl_closed >= 0 else "−"
                            if _is_risk_trim:
                                # Loss-cutting: frame as "realising a loss to reduce risk"
                                _gl_remain_str = ""
                                if _tr_gl_remain is not None and _tr_qty_keep > 0:
                                    _rem_icon = "🟢" if _tr_gl_remain >= 0 else "🔴"
                                    _rem_sign = "+" if _tr_gl_remain >= 0 else "−"
                                    _gl_remain_str = (
                                        f" Remaining {_tr_qty_keep} contract{'s' if _tr_qty_keep > 1 else ''}: "
                                        f"{_rem_icon} {_rem_sign}\\${abs(_tr_gl_remain):,.0f} still at risk."
                                    )
                                st.caption(
                                    f"{_gl_icon} **P/L on closed leg:** "
                                    f"{_gl_sign}\\${abs(_tr_gl_closed):,.0f} realised "
                                    f"on {_tr_qty_close} contract{'s' if _tr_qty_close > 1 else ''}. "
                                    f"You are accepting this loss to remove redundant delta — "
                                    f"the stock position already covers the directional exposure."
                                    f"{_gl_remain_str}"
                                )
                            else:
                                # Profit-taking: lock gains, survivor runs free
                                st.caption(
                                    f"{_gl_icon} **P/L locked:** "
                                    f"{_gl_sign}\\${abs(_tr_gl_closed):,.0f} "
                                    f"on {_tr_qty_close} closed contract{'s' if _tr_qty_close > 1 else ''}. "
                                    + (f"Remaining {_tr_qty_keep} contract{'s' if _tr_qty_keep > 1 else ''} "
                                       f"continue running — breakeven-protected on the survivor."
                                       if _tr_qty_keep > 0 else "")
                                )

                        if pd.notna(_tr_bid) and _tr_mid:
                            _tr_spread_pct = (_tr_ask - _tr_bid) / _tr_mid * 100 if _tr_mid > 0 else 0
                            if _tr_spread_pct > 5:
                                st.caption(
                                    f"⚠️ Wide spread {_tr_spread_pct:.1f}% — use limit at mid \\${_tr_mid:.2f}.  \n"
                                    f"Do NOT hit the bid (\\${_tr_bid:.2f})."
                                )
                            else:
                                st.caption(
                                    f"Spread {_tr_spread_pct:.1f}% — limit at mid \\${_tr_mid:.2f} should fill promptly."
                                )
                        if _is_risk_trim:
                            st.caption(
                                "McMillan Ch.4: 'Portfolio delta management — when a stock position already "
                                "provides full directional exposure, a redundant long call adds risk without "
                                "additional edge. Close the overlap, keep the better-positioned leg.' "
                                "Once filled, the stock position remains your primary directional vehicle."
                            )
                        else:
                            st.caption(
                                "McMillan Ch.4: 'Partial profit-taking is not abandoning the thesis — "
                                "it resets your cost basis on the survivor so you can hold through a pullback.' "
                                "Once filled, update your position tracker. The remaining contract(s) are now free-riding."
                            )

            # Hoisted winner-panel outputs — read by Roll Scenarios section below.
            # Initialised here so they're always defined regardless of whether panel fires.
            _wm_rec_for_roll   = ""       # e.g. "ROLL_DOWN" → demote debit candidates
            _wm_path_b_strike  = None     # strike of the credit-harvest candidate (Path B)
            _wm_path_b_exp     = None     # expiry of the credit-harvest candidate
            _wm_path_b_cand    = None     # full candidate dict for Path B (checklist uses this)

            # ── Winner Management Panel ──────────────────────────────────────────
            # Fires when: LONG option + P&L > 50% + Action == "ROLL"
            # Purpose: prevent user from treating a live winner as a realized gain,
            # and provide structured path (Close / Roll-Down / Roll-Down+Out).
            # Sits above Roll Scenarios — contextualizes the candidates, doesn't replace them.
            # ChatGPT confirmed architecture: intrinsic ratio is the primary decision axis.
            #   >65%  = trim default (intrinsic-heavy — harvest before decay)
            #   40–65% = mixed (momentum + P&L decide)
            #   <40%  = time-value dominant (hold or close for realized gain)
            _wm_fire = (
                _is_directional_long
                and _doc_action == "ROLL"
                and not opt_legs.empty
            )
            def _fmt_strike(s):
                """Format strike price: drop .0 suffix for whole numbers (e.g. 200.0 → 200)."""
                try:
                    f = float(s)
                    return str(int(f)) if f == int(f) else f"{f:.2f}"
                except Exception:
                    return str(s)

            if _wm_fire:
                # ── Compute winner metrics from the option leg ──────────────
                _wm_leg = opt_legs.iloc[0]
                _wm_qty        = abs(int(_wm_leg.get("Quantity", 1) or 1))
                _wm_last       = pd.to_numeric(_wm_leg.get("Last"),  errors="coerce")
                _wm_bid        = pd.to_numeric(_wm_leg.get("Bid"),   errors="coerce")
                _wm_ask        = pd.to_numeric(_wm_leg.get("Ask"),   errors="coerce")
                _wm_basis      = pd.to_numeric(_wm_leg.get("Basis"), errors="coerce")  # total cost paid
                _wm_gl         = pd.to_numeric(_wm_leg.get("$ Total G/L"), errors="coerce")
                _wm_strike     = pd.to_numeric(_wm_leg.get("Strike"), errors="coerce")
                _wm_dte        = pd.to_numeric(_wm_leg.get("DTE"),   errors="coerce")
                _wm_cp         = str(_wm_leg.get("Call/Put") or _wm_leg.get("OptionType") or "")
                _wm_exp        = _wm_leg.get("Expiration")
                _wm_exp_s      = pd.to_datetime(_wm_exp).strftime("%b %d '%y") if pd.notna(_wm_exp) else "—"
                _wm_delta      = pd.to_numeric(_wm_leg.get("Delta"), errors="coerce")

                # Mid price (live first, fallback to last)
                if pd.notna(_wm_bid) and pd.notna(_wm_ask) and _wm_ask > 0:
                    _wm_mid = (_wm_bid + _wm_ask) / 2
                elif pd.notna(_wm_last) and _wm_last > 0:
                    _wm_mid = _wm_last
                else:
                    _wm_mid = None

                # P&L% computation — prefer Premium_Entry (exact entry price per share) over Basis.
                # Fidelity's Basis field on options is a running adjusted cost (not the original
                # premium paid), so it produces wrong P&L%. Premium_Entry is stored by the engine
                # and equals the price/share at entry (e.g. $12.81 → cost per contract = $1281).
                _wm_premium_entry = pd.to_numeric(_wm_leg.get("Premium_Entry"), errors="coerce")
                _wm_cost_per_contract = None
                if pd.notna(_wm_premium_entry) and _wm_premium_entry > 0:
                    _wm_cost_per_contract = _wm_premium_entry * 100  # per contract
                elif pd.notna(_wm_basis) and _wm_qty > 0:
                    _wm_cost_per_contract = abs(_wm_basis) / _wm_qty  # fallback: Basis / qty
                _wm_pnl_pct = None
                if _wm_cost_per_contract and _wm_cost_per_contract > 0 and _wm_mid:
                    _wm_pnl_pct = (_wm_mid * 100 - _wm_cost_per_contract) / _wm_cost_per_contract * 100

                # Intrinsic value & intrinsic ratio
                # CALL intrinsic: max(0, spot - strike). PUT intrinsic: max(0, strike - spot)
                _wm_spot = None
                # Priority 1: stock leg Last price (buy-write / covered positions)
                if not stock_legs.empty:
                    _wm_spot = pd.to_numeric(stock_legs.iloc[0].get("Last"), errors="coerce")
                # Priority 2: "UL Last" column on the option leg (Fidelity exports underlying last)
                if pd.isna(_wm_spot) or _wm_spot is None:
                    _ul_last = pd.to_numeric(_wm_leg.get("UL Last"), errors="coerce")
                    if pd.notna(_ul_last) and _ul_last > 5:
                        _wm_spot = _ul_last
                # Priority 3: Underlying_Price_Entry as a ballpark (entry price, not live)
                if pd.isna(_wm_spot) or _wm_spot is None:
                    _ul_entry = pd.to_numeric(_wm_leg.get("Underlying_Price_Entry"), errors="coerce")
                    if pd.notna(_ul_entry) and _ul_entry > 5:
                        _wm_spot = _ul_entry
                # Priority 4: any group row with a large Last — but only accept values
                # that look like a stock price (> 20) to avoid picking up option prices.
                if pd.isna(_wm_spot) or _wm_spot is None:
                    for _, _sg in group.iterrows():
                        _sg_last = pd.to_numeric(_sg.get("UL Last") or _sg.get("Last"), errors="coerce")
                        if pd.notna(_sg_last) and _sg_last > 20:
                            _wm_spot = _sg_last
                            break

                _wm_intrinsic = None
                _wm_ir = None  # intrinsic ratio 0–1
                if pd.notna(_wm_strike) and _wm_spot is not None and pd.notna(_wm_spot):
                    _cp_upper = _wm_cp.upper()
                    if "P" in _cp_upper:
                        _wm_intrinsic = max(0.0, float(_wm_strike) - float(_wm_spot))
                    else:
                        _wm_intrinsic = max(0.0, float(_wm_spot) - float(_wm_strike))
                if _wm_intrinsic is not None and _wm_mid and _wm_mid > 0:
                    # Both _wm_intrinsic and _wm_mid are per-share — divide directly.
                    # Do NOT multiply mid by 100; that would produce a ratio 100× too small.
                    _wm_ir = _wm_intrinsic / _wm_mid
                    _wm_ir = min(1.0, max(0.0, _wm_ir))

                # Classify intrinsic ratio
                if _wm_ir is not None:
                    if _wm_ir >= 0.65:
                        _wm_ir_class = "HIGH"    # >65% — trim default
                        _wm_ir_label = "High (>65%)"
                        _wm_ir_color = "🟠"
                    elif _wm_ir >= 0.40:
                        _wm_ir_class = "MIXED"   # 40–65% — momentum decides
                        _wm_ir_label = "Mixed (40–65%)"
                        _wm_ir_color = "🟡"
                    else:
                        _wm_ir_class = "LOW"     # <40% — time value dominant
                        _wm_ir_label = "Low (<40%)"
                        _wm_ir_color = "🟢"
                else:
                    _wm_ir_class = "UNKNOWN"
                    _wm_ir_label = "—"
                    _wm_ir_color = "⚪"

                # Momentum proxy: delta trend
                # High delta (>0.75) on a long call = deep ITM, intrinsic dominant.
                # Falling delta = momentum reversing. Rising delta = thesis extending.
                _wm_momentum = "UNKNOWN"
                _wm_abs_delta = None
                if pd.notna(_wm_delta):
                    _wm_abs_delta = abs(float(_wm_delta))
                    if _wm_abs_delta >= 0.85:
                        _wm_momentum = "EXTREME_ITM"  # Natenberg: option behaves like stock — hard close zone
                    elif _wm_abs_delta >= 0.75:
                        _wm_momentum = "DEEP_ITM"     # harvest signal
                    elif _wm_abs_delta >= 0.55:
                        _wm_momentum = "ITM"          # strong — extend or harvest
                    elif _wm_abs_delta >= 0.40:
                        _wm_momentum = "NEAR_ATM"     # approaching ATM — watch
                    else:
                        _wm_momentum = "OTM"          # fading — protect gains

                # IV percentile for Path C edge check
                _wm_iv_rank = None
                if _doc_row is not None:
                    _wm_iv_rank = pd.to_numeric(_doc_row.get("IV_Rank"), errors="coerce")
                if pd.isna(_wm_iv_rank) and not opt_legs.empty:
                    _wm_iv_rank = pd.to_numeric(_wm_leg.get("IV_Rank"), errors="coerce")

                # OTM vol-expansion flag: gain came from IV spike, not directional move
                # (Second Leg Down: OTM options can reprice dramatically without ever touching intrinsic)
                _wm_is_vol_expansion_winner = (
                    _wm_ir_class in ("LOW", "UNKNOWN")
                    and _wm_pnl_pct is not None
                    and _wm_pnl_pct >= 50
                    and (_wm_abs_delta is None or _wm_abs_delta < 0.45)
                )

                # DTE override flags (Augen: theta accelerates 5–10× in final 14 days)
                _wm_dte_val = float(_wm_dte) if pd.notna(_wm_dte) else None
                _wm_final_week   = _wm_dte_val is not None and _wm_dte_val <= 7
                _wm_decay_zone   = _wm_dte_val is not None and _wm_dte_val <= 14
                # Hard-close gate: delta > 0.85 AND DTE < 14 → rolling adds no edge (Natenberg + Given)
                _wm_hard_close = (
                    _wm_momentum == "EXTREME_ITM"
                    and _wm_decay_zone
                )

                # Recommendation logic:
                # Priority 1 (override): hard-close gate (delta>0.85 + DTE<14) → always Path A
                # Priority 2 (override): final week (DTE≤7) → close immediately regardless of IR
                # Priority 3 (override): DTE≤14 + LOW IR → close before theta acceleration eats gain
                # Priority 4: intrinsic ratio × momentum × P&L matrix
                _wm_rec = "HOLD"
                _wm_rec_path = "C"   # A=Close, B=RollDown, C=RollDownOut, D=PartialClose
                _wm_rec_rationale = ""
                _wm_override_reason = ""

                if _wm_hard_close:
                    _wm_rec = "CLOSE"
                    _wm_rec_path = "A"
                    _wm_override_reason = "hard_close"
                    _wm_rec_rationale = (
                        f"Delta {_wm_abs_delta:.2f} (>0.85) + DTE {int(_wm_dte_val)}d (<14) — "
                        "option now behaves like stock with accelerating gamma/theta risk. "
                        "Natenberg: at this ITM depth, rolling adds complexity with no volatility edge. "
                        "Close now and redeploy when structure recovers. "
                        "Given: 'time value <15% of premium at this delta — hold has no edge premium.'"
                    )
                elif _wm_final_week:
                    _wm_rec = "CLOSE"
                    _wm_rec_path = "A"
                    _wm_override_reason = "final_week"
                    _wm_rec_rationale = (
                        f"DTE {int(_wm_dte_val)}d — final week. "
                        "Augen (Trading Options at Expiration): theta accelerates 5–10× in the final 7 days. "
                        "Every day held erodes 5–15% of remaining time value. "
                        "Close immediately — do not roll into another position from final-week urgency."
                    )
                elif _wm_decay_zone and _wm_ir_class == "LOW" and _wm_pnl_pct is not None and _wm_pnl_pct >= 50:
                    _wm_rec = "CLOSE"
                    _wm_rec_path = "A"
                    _wm_override_reason = "decay_zone_low_ir"
                    _wm_rec_rationale = (
                        f"DTE {int(_wm_dte_val)}d + intrinsic ratio <40% — "
                        "time value is dominant but theta is accelerating rapidly. "
                        "Augen: final 14 days see 5–10× theta acceleration on long options. "
                        "P&L {_wm_pnl_pct:.0f}% in hand — close before decay erodes the gain. "
                        "McMillan Ch.4: 'partial profit-taking at this stage locks in "
                        "what theta would otherwise destroy.'"
                    )
                elif _wm_is_vol_expansion_winner:
                    # OTM vol-expansion winner: gain came from IV spike, not directional move
                    _wm_rec = "CLOSE"
                    _wm_rec_path = "A"
                    _wm_override_reason = "vol_expansion"
                    _wm_rec_rationale = (
                        "OTM position — gain came from implied volatility expansion, not directional movement. "
                        "Second Leg Down (Krishnan): 'OTM options can reprice dramatically without ever having intrinsic value.' "
                        "This means the gain evaporates as fast as it appeared when IV reverts. "
                        "Close before IV mean-reverts — this is a vol event, not a thesis run. "
                        "Rolling into a new position would be entering at elevated IV, not an edge."
                    )
                elif _wm_pnl_pct is not None and _wm_pnl_pct >= 50:
                    if _wm_ir_class == "HIGH":
                        if _wm_pnl_pct >= 100 or _wm_momentum in ("EXTREME_ITM", "DEEP_ITM"):
                            _wm_rec = "ROLL_DOWN"
                            _wm_rec_path = "B"
                            _wm_rec_rationale = (
                                "Intrinsic ratio >65% + position deeply ITM. "
                                "Most of the value is locked intrinsic — time value erosion is working against you. "
                                "Second Leg Down: 'roll strikes down, thereby monetising profit.' "
                                "Roll down to a lower strike to capture the intrinsic credit and reset theta income."
                            )
                        else:
                            if _wm_pnl_pct >= 75:
                                # Multi-contract partial close available
                                _wm_rec = "PARTIAL_CLOSE"
                                _wm_rec_path = "D"
                                _wm_rec_rationale = (
                                    "Intrinsic ratio >65% + P&L >75% + momentum still extending. "
                                    "Jabbour: 'Close 50% at first target — the survivor rides on house money.' "
                                    "Bank half now (realized), let the rest run down+out if thesis continues. "
                                    "This avoids full debit extension when you already have a large gain in hand."
                                )
                            else:
                                _wm_rec = "ROLL_DOWN_OUT"
                                _wm_rec_path = "C"
                                _wm_rec_rationale = (
                                    "Intrinsic ratio >65% + momentum still extending (delta < 0.75). "
                                    "Thesis has room to run further. "
                                    "Roll down-and-out: capture intrinsic credit while adding time for additional gain."
                                )
                    elif _wm_ir_class == "MIXED":
                        if _wm_momentum in ("EXTREME_ITM", "DEEP_ITM", "OTM"):
                            _wm_rec = "ROLL_DOWN"
                            _wm_rec_path = "B"
                            _wm_rec_rationale = (
                                "Intrinsic ratio 40–65% + momentum at inflection. "
                                "Roll down to harvest the credit gain before intrinsic decays."
                            )
                        elif _wm_pnl_pct >= 75:
                            _wm_rec = "PARTIAL_CLOSE"
                            _wm_rec_path = "D"
                            _wm_rec_rationale = (
                                "Intrinsic ratio 40–65% + P&L >75% + thesis running. "
                                "Jabbour (Option Trader Handbook): close half at first target, let the "
                                "remaining half run — 'house money' on the survivor reduces emotional bias "
                                "and changes holding psychology fundamentally. "
                                "Close 50%, then evaluate roll on the remainder."
                            )
                        else:
                            _wm_rec = "ROLL_DOWN_OUT"
                            _wm_rec_path = "C"
                            _wm_rec_rationale = (
                                "Intrinsic ratio 40–65% + thesis still running. "
                                "Roll down-and-out to extend duration while capturing partial credit."
                            )
                    else:  # LOW intrinsic ratio, no override triggered
                        if _wm_pnl_pct >= 75:
                            _wm_rec = "CLOSE"
                            _wm_rec_path = "A"
                            _wm_rec_rationale = (
                                "Intrinsic ratio <40% — option is mostly time value. "
                                "P&L >75% with time value dominant: "
                                "close entirely and take the realized gain. "
                                "McMillan Ch.4: once you've captured 75%+ of max profit on a pure time-value play, "
                                "the remaining upside rarely justifies the ongoing theta cost. "
                                "Given: 'credits diminish with each roll — eventually closing is the right move.'"
                            )
                        else:
                            _wm_rec = "HOLD"
                            _wm_rec_rationale = (
                                "Intrinsic ratio <40% — option is time-value dominant. "
                                "Thesis is still running cleanly. Hold and let theta continue working. "
                                "Revisit when P&L reaches 75%+ or delta rises above 0.70, "
                                "or if DTE drops below 14 (theta acceleration zone)."
                            )

                # Single-contract override: PARTIAL_CLOSE (Path D) requires qty ≥ 2.
                # For qty=1, the roll IS the trim — downgrade to the credit-harvest path instead.
                if _wm_qty == 1 and _wm_rec == "PARTIAL_CLOSE":
                    _wm_rec = "ROLL_DOWN"
                    _wm_rec_path = "B"
                    _wm_rec_rationale = (
                        "Single contract — partial close not available. "
                        "Roll down to a lower strike (credit roll) to harvest the intrinsic gain: "
                        "net credit received = the locked portion of the paper gain. "
                        "McMillan Ch.4: 'For single-contract positions, rolling extracts the same "
                        "economic benefit as a partial trim — the credit received is the harvested gain.'"
                    )

                # Hoist recommendation into outer scope so Roll Scenarios section can read it.
                _wm_rec_for_roll = _wm_rec

                # Only show panel if P&L > 50% (guard — _wm_fire already checked ROLL+LONG)
                if _wm_pnl_pct is None or _wm_pnl_pct >= 50:
                    # NOTE: st.expander nested inside another st.expander silently breaks in Streamlit.
                    # The card itself is a st.expander — so we use a plain container with a visible
                    # header divider instead. All content renders directly in the card body.
                    st.markdown("---")
                    st.markdown(f"### 🏆 Winner Management — {_wm_rec.replace('_', ' ')} Recommended")
                    if True:  # scope block — keeps indent consistent with former expander body
                        st.success(
                            f"**This position is a winner.** Doctrine says ROLL — "
                            f"this panel maps your options before you scroll to roll candidates below."
                        )

                        # ── Fix 3: Near-high + strong-down-day → peak intrinsic window callout ──
                        # Nison: strong down day after rally near highs = potential reversal signal.
                        # For a long put, this is the moment of MAXIMUM intrinsic value.
                        # The WAIT signal in roll scenarios is about spread width — not put thesis.
                        _wm_live_sigs = st.session_state.get(f"live_refresh_{tid}") or {}
                        if isinstance(_wm_live_sigs, dict):
                            _wm_pos_tag = _wm_live_sigs.get("intraday_position_tag", "")
                            _wm_mom_tag = _wm_live_sigs.get("momentum_tag", "")
                            _wm_peak_intrinsic_window = (
                                _wm_pos_tag == "NEAR_HIGH"
                                and "STRONG_DOWN" in _wm_mom_tag
                                and "PUT" in str(entry_structure).upper()
                                and _wm_ir_class in ("HIGH", "MIXED")
                            )
                            if _wm_peak_intrinsic_window:
                                st.info(
                                    "📍 **Peak Intrinsic Window** — underlying is near its high with a strong down day. "
                                    "For a long put, this is the moment of maximum intrinsic value. "
                                    "Nison (Candlestick Techniques): 'strong down day near a recent high signals "
                                    "supply overwhelming demand — bears gaining force.' "
                                    "This is the **harvest window**, not a delay signal. "
                                    "The WAIT notice in Roll Scenarios refers to spread width during momentum — "
                                    "your intrinsic is at its peak right now, not later."
                                )

                        # ── Pre-select roll candidates for Path B / Path C ────────────────────
                        # Hoisted here so both the single-contract binary text AND the Roll
                        # Scenario Guide below use the same candidate selection logic:
                        #   Path B = highest net credit (credit roll, lower strike)
                        #   Path C = furthest DTE (duration extension — debit or small credit)
                        # Running this once prevents the two sections from showing different expiries.
                        _wm_db_rc = _db_roll_candidates.get(tid, {})

                        def _wm_get_candidate(i):
                            import json as _jj
                            raw = None
                            if _doc_row is not None:
                                raw = _doc_row.get(f"Roll_Candidate_{i}")
                            if raw in (None, "", "nan") or (isinstance(raw, float) and pd.isna(raw)):
                                raw = _wm_db_rc.get(f"Roll_Candidate_{i}")
                            if raw in (None, "", "nan") or (isinstance(raw, float) and pd.isna(raw)):
                                return None
                            try:
                                c = _jj.loads(str(raw)) if isinstance(raw, str) else raw
                                return c if isinstance(c, dict) else None
                            except Exception:
                                return None

                        _wm_all_cands = [c for c in [_wm_get_candidate(i) for i in range(1, 4)] if c]

                        _best_credit = -999
                        _best_dte    = -1
                        for _wc in _wm_all_cands:
                            _wc_cost = _wc.get("cost_to_roll", {})
                            if isinstance(_wc_cost, str):
                                try:
                                    import json as _jj2; _wc_cost = _jj2.loads(_wc_cost)
                                except Exception:
                                    _wc_cost = {}
                            _wc_type    = _wc_cost.get("type", "")
                            _wc_net_per = float(_wc_cost.get("net_per_contract", 0) or 0)
                            _wc_dte     = int(_wc.get("dte", 0) or 0)
                            if _wc_type == "credit" and _wc_net_per > _best_credit:
                                _best_credit = _wc_net_per
                                _wm_path_b_cand = _wc
                            if _wc_dte > _best_dte:
                                _best_dte = _wc_dte
                                _wm_path_c_cand = _wc

                        if _wm_path_b_cand:
                            _wm_path_b_strike = _wm_path_b_cand.get("strike")
                            _wm_path_b_exp    = _wm_path_b_cand.get("expiry")

                        # ── Fix 4: Single-contract binary explanation ────────────────────────────
                        # Passarelli Ch.6: for a single contract, the roll IS the trim.
                        # There is no partial close — the user must choose one of two economics:
                        # collect credit (Path B) or pay debit (Path C). Make this explicit.
                        # Uses _wm_path_b_cand / _wm_path_c_cand selected above.
                        if _wm_qty == 1:
                            _wm_single_credit_str = ""
                            _wm_single_debit_str  = ""
                            # Reuse pre-selected candidates — same source of truth as Roll Scenario Guide
                            if _wm_path_b_cand:
                                _b_cost_sc = _wm_path_b_cand.get("cost_to_roll", {})
                                if isinstance(_b_cost_sc, str):
                                    try:
                                        import json as _jjsc; _b_cost_sc = _jjsc.loads(_b_cost_sc)
                                    except Exception:
                                        _b_cost_sc = {}
                                _b_net_sc = float(_b_cost_sc.get("net_per_contract", 0) or 0)
                                _wm_single_credit_str = (
                                    f"**Path B (collect \\${_b_net_sc:.2f}/share):** "
                                    f"roll to \\${_fmt_strike(_wm_path_b_cand.get('strike'))} "
                                    f"exp {_wm_path_b_cand.get('expiry')}. "
                                )
                            if _wm_path_c_cand and _wm_path_c_cand is not _wm_path_b_cand:
                                _c_cost_sc = _wm_path_c_cand.get("cost_to_roll", {})
                                if isinstance(_c_cost_sc, str):
                                    try:
                                        import json as _jjsc2; _c_cost_sc = _jjsc2.loads(_c_cost_sc)
                                    except Exception:
                                        _c_cost_sc = {}
                                _c_net_sc = abs(float(_c_cost_sc.get("net_per_contract", 0) or 0))
                                _c_type_sc = _c_cost_sc.get("type", "debit")
                                _c_verb = "pay" if _c_type_sc == "debit" else "collect"
                                _wm_single_debit_str = (
                                    f"**Path C ({_c_verb} \\${_c_net_sc:.2f}/share):** "
                                    f"roll to \\${_fmt_strike(_wm_path_c_cand.get('strike'))} "
                                    f"exp {_wm_path_c_cand.get('expiry')}. "
                                )
                            st.warning(
                                "⚠️ **Single contract — the roll IS the trim.** "
                                "You cannot sell half a contract. Your two economic outcomes are:  \n"
                                + (_wm_single_credit_str if _wm_single_credit_str else "No credit candidate found. ") + "  \n"
                                + (_wm_single_debit_str  if _wm_single_debit_str  else "No debit candidate found. ") + "  \n"
                                "McMillan Ch.4: 'For single-contract positions, rolling to a lower strike "
                                "extracts the same economic benefit as a partial trim — it locks the gain "
                                "differential as a net credit received.' "
                                "Passarelli Ch.6: choose based on whether you want cash now (credit) "
                                "or more time (debit)."
                            )

                        # ── Profit snapshot ──────────────────────────────────
                        _wc1, _wc2, _wc3, _wc4 = st.columns(4)
                        if _wm_pnl_pct is not None:
                            _pnl_sign = "+" if _wm_pnl_pct >= 0 else ""
                            _wc1.metric(
                                "Unrealized Gain",
                                f"{_pnl_sign}{_wm_pnl_pct:.0f}%",
                                help="Based on cost basis vs current mid. NOT realized until you close or roll.",
                            )
                        else:
                            _wc1.metric("Unrealized Gain", "—", help="No basis data")

                        _wc2.metric(
                            "Intrinsic Ratio",
                            f"{_wm_ir_color} {_wm_ir_label}",
                            help=(
                                "Intrinsic / Option Price. "
                                ">65% = mostly locked value (harvest). "
                                "40–65% = mixed (momentum decides). "
                                "<40% = time value dominant (hold or close)."
                            ),
                        )
                        if _wm_intrinsic is not None:
                            _wc3.metric(
                                "Intrinsic Value",
                                f"\\${_wm_intrinsic:.2f}/share",
                                help="Value the option would be worth if exercised right now.",
                            )
                        else:
                            _wc3.metric("Intrinsic Value", "—", help="Need spot price to compute")

                        if pd.notna(_wm_dte):
                            _wc4.metric(
                                "DTE",
                                f"{int(_wm_dte)}d",
                                help="Days to expiration. < 21d: time value decaying fast.",
                            )
                        else:
                            _wc4.metric("DTE", "—")

                        # ── Realized gain warning ────────────────────────────
                        if _wm_gl is not None and pd.notna(_wm_gl) and _wm_pnl_pct is not None:
                            st.warning(
                                f"⚠️ **Unrealized P&L is \\${_wm_gl:+,.0f} — this is NOT realized** until you "
                                f"close or execute a roll. A credit roll harvests the credit received, not the "
                                f"full paper gain. Do not treat these as the same number."
                            )

                        # ── Override banners ─────────────────────────────────
                        if _wm_override_reason == "hard_close":
                            st.error(
                                f"🔴 **HARD CLOSE — delta {_wm_abs_delta:.2f} + DTE {int(_wm_dte_val)}d** — "
                                "Natenberg: option behaves like stock at this depth. Rolling adds no edge. "
                                "Close immediately."
                            )
                        elif _wm_override_reason == "final_week":
                            st.error(
                                f"🔴 **FINAL WEEK — {int(_wm_dte_val)}d remaining** — "
                                "Augen: theta accelerates 5–10× in the final 7 days. "
                                "Every day held erodes 5–15% of remaining time value. Close now."
                            )
                        elif _wm_override_reason == "decay_zone_low_ir":
                            st.warning(
                                f"⚠️ **DECAY ZONE — {int(_wm_dte_val)}d remaining + time-value dominant** — "
                                "Augen: theta acceleration starts at 14 DTE. "
                                "Close before decay erodes the gain."
                            )
                        elif _wm_override_reason == "vol_expansion":
                            st.warning(
                                "⚠️ **VOL EXPANSION WINNER** — This gain came from IV spike, not directional movement. "
                                "Second Leg Down: OTM options can reprice dramatically without intrinsic. "
                                "Close before IV reverts — rolling would mean entering at elevated IV."
                            )

                        st.divider()

                        # ── Four paths ───────────────────────────────────────
                        st.markdown("#### Your Four Paths")
                        _pa, _pb, _pc, _pd = st.columns(4)

                        with _pa:
                            _pa_rec = "✅ **Recommended**" if _wm_rec_path == "A" else ""
                            st.markdown(f"**Path A — Close Entirely** {_pa_rec}")
                            st.caption(
                                "Sell-to-Close. Gain is **realized** at this point — "
                                "you exit cleanly with the full P&L in hand. "
                                "Best when: intrinsic ratio high, DTE < 14, or vol-expansion winner."
                            )
                            if _wm_mid and _wm_pnl_pct is not None:
                                _pa_proceeds = _wm_mid * 100 * _wm_qty
                                st.metric(
                                    "Proceeds (STC)",
                                    f"\\${_pa_proceeds:,.0f}",
                                    help=f"{_wm_qty} × \\${_wm_mid:.2f} × 100 — realized on fill",
                                )

                        with _pb:
                            _pb_rec = "✅ **Recommended**" if _wm_rec_path == "B" else ""
                            st.markdown(f"**Path B — Roll Down (Credit Harvest)** {_pb_rec}")
                            st.caption(
                                "Close this contract + open a lower strike same-expiry. "
                                "**Harvests net credit** — the credit received is the only locked amount. "
                                "Gain is NOT fully realized — you now hold the lower-strike option. "
                                "Second Leg Down: 'roll strikes down, thereby monetising profit.'"
                            )
                            st.caption(
                                "Framing: **harvesting** — exchange intrinsic for cash while "
                                "keeping optionality on the lower strike."
                            )

                        with _pc:
                            _pc_rec = "✅ **Recommended**" if _wm_rec_path == "C" else ""
                            st.markdown(f"**Path C — Roll Down+Out (Extend Duration)** {_pc_rec}")
                            st.caption(
                                "Close this + open lower strike at further expiry. "
                                "**New capital allocation** — paying a debit (or smaller credit) "
                                "to add time for the thesis to extend further. "
                                "Natenberg: 'rolling adds a new directional bet, not a neutral adjustment.'"
                            )
                            # IV rank check: if IV is compressed, flag that Path C has thin edge
                            if pd.notna(_wm_iv_rank) and _wm_iv_rank < 20:
                                st.caption(
                                    f"⚠️ IV Rank {_wm_iv_rank:.0f} — compressed. "
                                    "New position entered at thin premium. "
                                    "Natenberg: low-IV entry = poor risk/reward for debit roll."
                                )
                            st.caption(
                                "Framing: **new capital deployment** — old gain not realized; "
                                "it converts into the new position's cost."
                            )

                        with _pd:
                            _pd_rec = "✅ **Recommended**" if _wm_rec_path == "D" else ""
                            st.markdown(f"**Path D — Partial Close (House Money)** {_pd_rec}")
                            st.caption(
                                "Close half the position now (realized gain) — "
                                "let the remaining half run on house money. "
                                "Jabbour (Option Trader Handbook): 'Close 50% at first target — "
                                "the survivor rides on locked profits, changing holding psychology.'"
                            )
                            st.caption(
                                "Framing: **hybrid** — you realize half the gain immediately, "
                                "then manage the survivor as a free-riding position with no emotional attachment."
                            )
                            if _wm_mid and _wm_qty >= 2:
                                _pd_close_qty  = max(1, _wm_qty // 2)
                                _pd_keep_qty   = _wm_qty - _pd_close_qty
                                _pd_proceeds   = _wm_mid * 100 * _pd_close_qty
                                st.metric(
                                    f"Close {_pd_close_qty}, keep {_pd_keep_qty}",
                                    f"\\${_pd_proceeds:,.0f} locked",
                                    help=f"{_pd_close_qty} × \\${_wm_mid:.2f} × 100",
                                )
                            elif _wm_qty < 2:
                                st.caption("Single contract — use Path A (close) or Path B/C (roll full).")

                        st.divider()

                        # ── Engine recommendation ────────────────────────────
                        _rec_icons = {
                            "CLOSE":         "🟢 **Close Entirely (Path A)**",
                            "ROLL_DOWN":     "🟠 **Roll Down — Credit Harvest (Path B)**",
                            "ROLL_DOWN_OUT": "🔵 **Roll Down+Out — Extend Duration (Path C)**",
                            "PARTIAL_CLOSE": "🟡 **Partial Close — House Money (Path D)**",
                            "HOLD":          "⚪ **Hold — No Action Yet**",
                        }
                        _rec_display = _rec_icons.get(_wm_rec, f"⚪ {_wm_rec}")
                        st.markdown(f"#### Engine Recommendation: {_rec_display}")
                        if _wm_rec_rationale:
                            _urgency_fn = st.error if _wm_override_reason in ("hard_close", "final_week") else st.info
                            _urgency_fn(_wm_rec_rationale)

                        # ── Roll friction warning (Natenberg: more rolls = more friction) ───
                        _wm_roll_mode = str(_doc_row.get("Roll_Mode", "") or "") if _doc_row is not None else ""
                        _wm_roll_count_raw = _doc_row.get("Roll_Count") if _doc_row is not None else None
                        _wm_roll_count_num = pd.to_numeric(_wm_roll_count_raw, errors="coerce")
                        _wm_roll_count = int(_wm_roll_count_num) if pd.notna(_wm_roll_count_num) else 0
                        _has_been_rolled = _wm_roll_count > 0 or bool(_wm_roll_mode)
                        if _has_been_rolled and _wm_rec_path in ("B", "C"):
                            _roll_count_str = f"{_wm_roll_count}×" if _wm_roll_count > 0 else "previously"
                            st.caption(
                                f"⚠️ **Roll friction** — this position has been rolled {_roll_count_str}. "
                                "Natenberg Ch.11: 'More rolls = more friction costs, not more profit.' "
                                "Each roll costs bid/ask spread + commission. "
                                "Weigh cumulative friction against the incremental gain before rolling again."
                            )

                        # _wm_path_b_cand / _wm_path_c_cand / _wm_all_cands selected above (hoisted).
                        if _wm_all_cands:
                            st.markdown("**Roll Scenario Guide** — candidates below are pre-mapped:")
                            _guide_lines = []
                            if _wm_path_b_cand:
                                _b_strike = _wm_path_b_cand.get("strike", "?")
                                _b_exp    = _wm_path_b_cand.get("expiry", "?")
                                _b_cost   = _wm_path_b_cand.get("cost_to_roll", {})
                                if isinstance(_b_cost, str):
                                    try:
                                        import json as _jj3; _b_cost = _jj3.loads(_b_cost)
                                    except Exception:
                                        _b_cost = {}
                                _b_net = float(_b_cost.get("net_per_contract", 0) or 0)
                                _guide_lines.append(
                                    f"**Path B (Credit Harvest)** → \\${_fmt_strike(_b_strike)} exp {_b_exp} "
                                    f"(net \\${_b_net:+.2f}/contract)"
                                )
                            if _wm_path_c_cand and _wm_path_c_cand != _wm_path_b_cand:
                                _c_strike = _wm_path_c_cand.get("strike", "?")
                                _c_exp    = _wm_path_c_cand.get("expiry", "?")
                                _c_dte    = _wm_path_c_cand.get("dte", "?")
                                _guide_lines.append(
                                    f"**Path C (Duration Extension)** → \\${_fmt_strike(_c_strike)} exp {_c_exp} "
                                    f"({_c_dte}d DTE)"
                                )
                            for _gl in _guide_lines:
                                st.caption(_gl)
                        else:
                            st.caption(
                                "No roll candidates loaded yet. "
                                "Run the management engine during market hours to populate Roll_Candidate_1/2/3."
                            )

                        # ── McMillan doctrine ────────────────────────────────
                        st.caption(
                            "McMillan Ch.4 (Long Options): 'Once a long option is deep ITM and intrinsic "
                            "dominates, continuing to hold exposes you to theta decay on intrinsic recovery. "
                            "Roll down to a lower strike to lock the profit differential — "
                            "the credit received is the harvested gain.' "
                            "Natenberg Ch.11: 'Rolling to capture intrinsic is not a new trade — "
                            "it is a position adjustment that converts paper profit into locked credit.'"
                        )

            # Roll expander only makes sense for BUY_WRITE/COVERED_CALL (short call to roll)
            # or when action is explicitly ROLL. EXIT on a LONG_CALL means close — no roll scaffold.
            _needs_roll = (
                (_doc_action in ("ROLL", "ROLL_WAIT"))
                or (_is_bw and _doc_action == "EXIT")
                or (_is_bw and not stock_legs.empty and '_drift' in dir() and _drift < -0.08)
            )

            if _needs_roll and not opt_legs.empty:
                # Check CSV row first, then DB cache (populated from last market-hours run)
                _db_rc_for_trade = _db_roll_candidates.get(tid, {})
                def _rc_is_valid(v) -> bool:
                    """True only when v is a non-empty, non-NaN string with JSON content."""
                    if v is None:
                        return False
                    try:
                        import math as _m
                        if isinstance(v, float) and _m.isnan(v):
                            return False
                    except Exception:
                        pass
                    return str(v).strip() not in ("", "nan", "None")

                _has_roll_candidates = (
                    any(_rc_is_valid(_doc_row.get(f"Roll_Candidate_{i}")) for i in range(1, 4))
                    if _doc_row is not None else False
                ) or bool(_db_rc_for_trade.get("Roll_Candidate_1"))

                # Build unified candidate list: prefer CSV (fresh), fallback to DB
                def _get_candidate(i):
                    """Return parsed candidate dict for index i, CSV-first then DB."""
                    import json as _j
                    raw = None
                    if _doc_row is not None:
                        raw = _doc_row.get(f"Roll_Candidate_{i}")
                    if raw in (None, "", "nan") or (isinstance(raw, float) and pd.isna(raw)):
                        raw = _db_rc_for_trade.get(f"Roll_Candidate_{i}")
                    if raw in (None, "", "nan") or (isinstance(raw, float) and pd.isna(raw)):
                        return None, False
                    try:
                        cand = _j.loads(str(raw)) if isinstance(raw, str) else raw
                        from_db = (_doc_row is None or _doc_row.get(f"Roll_Candidate_{i}") in (None, "", "nan")
                                   or (isinstance(_doc_row.get(f"Roll_Candidate_{i}"), float)
                                       and pd.isna(_doc_row.get(f"Roll_Candidate_{i}"))))
                        return (cand if isinstance(cand, dict) else None), from_db
                    except Exception:
                        return None, False

                with st.expander(
                    ("🚪 Exit Path — Call Leg Disposition" if _doc_action == "EXIT" and _is_bw
                     else "🗓️ Weekend Prep — Roll Scenarios" if not _is_market_open
                     else "📋 Roll Scenarios"),
                    expanded=(not _is_market_open and _doc_action in ("EXIT", "ROLL"))
                ):
                    # EXIT override — show explicit decision framing before candidates
                    # so user doesn't interpret roll list as a recommendation.
                    if _doc_action == "EXIT" and _is_bw:
                        st.error(
                            "**🔴 Doctrine Action: EXIT** — see Exit Winner Panel above for the "
                            "recommended path (accept assignment vs active exit). "
                            "The candidates below are reference-only for the call leg disposition — "
                            "they are NOT a roll recommendation. "
                            "Passarelli Ch.6: decouple the stock exit from the call decision."
                        )

                    if not _is_market_open:
                        st.info(
                            "**Market closed.** These scenarios are pre-staged for Monday open. "
                            "Re-evaluate live prices before executing — IV and bid/ask will differ."
                        )
                    else:
                        st.caption("Market open — evaluate these against live chain before acting.")

                    # ── Live intraday refresh (market hours only) ─────────────────────
                    # One button per position card. Uses nonce-based cache invalidation
                    # so double-clicking always fetches fresh data (ChatGPT fix #1).
                    # Also fetches live option spreads for current short + top roll candidate.
                    _live_key   = f"live_refresh_{tid}"
                    _nonce_key  = f"refresh_nonce_{tid}"
                    _live_data  = st.session_state.get(_live_key)
                    _cur_nonce  = st.session_state.get(_nonce_key, 0)

                    # Collect option symbols for live quote fetch.
                    # We keep the current short leg(s) separate so we can compute
                    # the NET roll credit/debit per candidate: buy_back + sell_new.
                    # Net credit bid  = new_leg_bid  − old_leg_ask  (worst fill you'd get)
                    # Net credit ask  = new_leg_ask  − old_leg_bid  (best fill, unlikely)
                    # Net credit mid  = (net_bid + net_ask) / 2     (where to start limit)
                    _current_leg_syms = []
                    if not opt_legs.empty and "Symbol" in opt_legs.columns:
                        _current_leg_syms = [
                            str(s) for s in opt_legs["Symbol"].dropna().tolist()
                            if str(s) not in ("", "nan")
                        ]
                    # Candidate symbols (all 3) so we can compute net per candidate
                    _cand_syms = []
                    for _ci in range(1, 4):
                        try:
                            _craw, _ = _get_candidate(_ci)
                            if _craw:
                                _csym = _craw.get("symbol") or _craw.get("contract_symbol")
                                if _csym and str(_csym) not in ("", "nan"):
                                    _cand_syms.append(str(_csym))
                        except Exception:
                            pass
                    _opt_syms_for_spread = list(dict.fromkeys(_current_leg_syms + _cand_syms))

                    if _is_market_open:
                        _btn_col, _status_col = st.columns([1, 3])
                        with _btn_col:
                            if st.button("🔄 Refresh Live", key=f"btn_{_live_key}", help="Fetch live Schwab quote + 5-min bars + option spreads"):
                                # Increment nonce — guaranteed new cache key, no .clear() needed
                                _cur_nonce += 1
                                st.session_state[_nonce_key] = _cur_nonce
                                with st.spinner(f"Fetching live data for {ticker}…"):
                                    _live_data = _fetch_live_intraday(ticker, _nonce=_cur_nonce)
                                    # Option spread fetch is separate — doesn't block chart render
                                    _opt_spreads = _fetch_option_spreads(_opt_syms_for_spread)
                                    _live_data["opt_spreads"] = _opt_spreads
                                    st.session_state[_live_key] = _live_data

                        if _live_data:
                            if _live_data.get("error"):
                                with _status_col:
                                    st.warning(f"Live fetch error: {_live_data['error']}")
                            else:
                                _live_sigs   = _compute_live_signals(_live_data["quote"], _live_data["bars"])
                                _opt_spreads = _live_data.get("opt_spreads", {})

                                # Compute earnings context for roll window
                                _rw_days_to_earn = None
                                _rw_earn_str     = None
                                _rw_cand1_dte    = None
                                try:
                                    _earn_raw = _doc_row.get("Earnings Date") if _doc_row is not None else None
                                    if _earn_raw and str(_earn_raw) not in ("", "nan", "None", "N/A"):
                                        _ed = pd.to_datetime(str(_earn_raw), errors="coerce")
                                        if pd.notna(_ed):
                                            _rw_days_to_earn = (_ed.normalize() - pd.Timestamp.now().normalize()).days
                                            _rw_earn_str = _ed.strftime("%b %d")
                                    # Roll target DTE — use #2 (credit candidate, Path B) as primary
                                    for _rw_ci in (2, 1, 3):
                                        _rw_cr, _ = _get_candidate(_rw_ci)
                                        if _rw_cr:
                                            _rw_dte_v = _rw_cr.get("dte")
                                            if _rw_dte_v and str(_rw_dte_v) not in ("", "nan", "None"):
                                                _rw_cand1_dte = int(float(_rw_dte_v))
                                                break
                                except Exception:
                                    pass

                                _roll_window = _best_roll_window(
                                    _live_data["bars"],
                                    _live_sigs,
                                    opt_spreads=_opt_spreads,
                                    days_to_earnings=_rw_days_to_earn,
                                    roll_target_dte=_rw_cand1_dte,
                                    earnings_date_str=_rw_earn_str,
                                )

                                # Window advisory banner with score
                                _wv    = _roll_window["verdict"]
                                _score = _roll_window.get("score", 50)
                                _score_str = f" · readiness {_score}/100"
                                if _wv == "FAVORABLE":
                                    st.success(f"**{_roll_window['label']}**{_score_str}")
                                elif _wv == "WAIT":
                                    st.warning(f"**{_roll_window['label']}**{_score_str}")
                                else:
                                    st.error(f"**{_roll_window['label']}**{_score_str}")

                                for _reason in _roll_window["reasons"]:
                                    st.caption(f"• {_reason}")

                                # Live signal chips
                                _lsc1, _lsc2, _lsc3, _lsc4, _lsc5 = st.columns(5)
                                _pos_tag = _live_sigs.get("intraday_position_tag", "—")
                                _mom_tag = _live_sigs.get("momentum_tag", "—").replace("_", " ").title()
                                _rsi_val = _live_sigs.get("rsi_14")
                                _vwap_v  = _live_sigs.get("vwap")
                                _last_v  = _live_sigs.get("last_price")
                                _lsc1.metric("Position", _pos_tag.replace("_", " ").title())
                                _lsc2.metric("Momentum", _mom_tag)
                                _lsc3.metric("RSI-14", f"{_rsi_val:.0f}" if _rsi_val else "—")
                                if _vwap_v and _last_v:
                                    _vwap_delta = (_last_v - _vwap_v) / _vwap_v * 100
                                    _lsc4.metric("vs VWAP", f"{_vwap_delta:+.2f}%")
                                else:
                                    _lsc4.metric("VWAP", f"${_vwap_v:.2f}" if _vwap_v else "—")
                                # Option spread chip — most important for rolling decision
                                if _opt_spreads:
                                    _spreads_list = [
                                        (sym, sq.get("spread_pct"))
                                        for sym, sq in _opt_spreads.items()
                                        if sq.get("spread_pct") is not None
                                    ]
                                    if _spreads_list:
                                        _worst_sp = max(sp for _, sp in _spreads_list)
                                        _sp_color = "🔴" if _worst_sp > 8 else ("🟡" if _worst_sp > 4 else "🟢")
                                        _lsc5.metric("Opt Spread", f"{_sp_color} {_worst_sp:.1f}%")
                                    else:
                                        _lsc5.metric("Opt Spread", "—")
                                else:
                                    _lsc5.metric("Opt Spread", "—")

                                # Intraday chart
                                _render_intraday_chart(_live_data["bars"], _live_sigs, ticker)

                    # ── Execution Readiness (Layer 2: hybrid) ──────────────────
                    _backend_er  = str(_doc_row.get("Execution_Readiness", "")  or "") if _doc_row is not None else ""
                    _backend_err = str(_doc_row.get("Execution_Readiness_Reason", "") or "") if _doc_row is not None else ""
                    _doc_urgency = str(_doc_row.get("Urgency", "LOW") or "LOW") if _doc_row is not None else "LOW"
                    if not _backend_er:
                        _backend_er  = "STAGE_AND_RECHECK"
                        _backend_err = "Execution_Readiness not populated — re-run pipeline"
                    _final_er, _final_err, _er_color = _apply_time_of_day_filter(
                        _backend_er, _backend_err, _doc_urgency
                    )

                    # ── Signal Arbitration — one synthesized directive ──────────
                    # Hierarchy: structural (what) → urgency (deadline) → timing (how) → MC (context).
                    # Rule: timing WAIT never overrides structural + HIGH urgency.
                    _sa_structural = _wm_rec_for_roll  # "ROLL_DOWN", "ROLL_DOWN_OUT", "CLOSE", etc.
                    _sa_urgency    = _doc_urgency.upper()
                    _sa_timing_wait = False
                    _sa_timing_reason = ""
                    if _live_data and not _live_data.get("error"):
                        _rw = locals().get("_roll_window") or {}
                        if isinstance(_rw, dict) and _rw.get("verdict") == "WAIT":
                            _sa_timing_wait = True
                            _sa_timing_reason = "; ".join(
                                r for r in _rw.get("reasons", []) if r
                            )
                    _sa_path_label = {
                        "ROLL_DOWN":     "Path B — Credit Roll",
                        "ROLL_DOWN_OUT": "Path C — Debit Extension",
                        "CLOSE":         "Path A — Close",
                        "PARTIAL_CLOSE": "Path D — Partial Close",
                    }.get(_sa_structural, _sa_structural.replace("_", " ") if _sa_structural else "")
                    _sa_leg_in = False
                    if not opt_legs.empty:
                        _sa_oi_raw = pd.to_numeric(opt_legs.iloc[0].get("Open_Int"), errors="coerce")
                        _sa_leg_in = pd.notna(_sa_oi_raw) and _sa_oi_raw < 200

                    # For EXIT on BUY_WRITE: the execution decision lives in the Exit Winner
                    # Panel (accept assignment vs active exit). The roll scaffold's execution
                    # banner is irrelevant — suppress it and redirect.
                    if _doc_action == "EXIT" and _is_bw:
                        st.info(
                            "🏆 **Exit execution path resolved above** — see the Exit Winner Panel "
                            "for the specific directive (accept assignment vs active exit). "
                            "Candidates below are reference-only."
                        )
                    elif _sa_structural and _sa_structural not in ("", "HOLD"):
                        _sa_urgency_high = _sa_urgency in ("CRITICAL", "HIGH")
                        if _sa_timing_wait and _sa_urgency_high:
                            _sa_exec_note = "leg in — close old leg first" if _sa_leg_in else "use limit at mid"
                            st.warning(
                                f"🎯 **Execute {_sa_path_label} today — {_sa_exec_note}.** "
                                f"Timing shows suboptimal conditions "
                                f"({_sa_timing_reason or 'spreads wide'}), "
                                f"but structural urgency ({_sa_urgency}) overrides timing hesitation. "
                                f"Target the next calm window this session — do not defer to tomorrow."
                            )
                        elif _sa_timing_wait and not _sa_urgency_high:
                            st.info(
                                f"🎯 **Stage {_sa_path_label} — wait for spreads to normalize.** "
                                f"Urgency is {_sa_urgency} — no hard deadline today. "
                                f"Execute when: {_sa_timing_reason or 'momentum stabilizes and spreads tighten'}."
                            )
                        elif _final_er == "EXECUTE_NOW":
                            _sa_exec_note = "leg in — close old leg first" if _sa_leg_in else "use limit at mid"
                            st.success(
                                f"🎯 **Execute {_sa_path_label} — {_sa_exec_note}.** "
                                f"Conditions are favorable. {_final_err.replace('$', '')}."
                            )
                        else:
                            st.info(
                                f"🎯 **Stage {_sa_path_label}.** "
                                f"{_final_err.replace('$', '')}."
                            )
                    else:
                        # Non-winner-panel positions: standard execution readiness banner.
                        _er_icons = {'EXECUTE_NOW': '🟢', 'WAIT_FOR_WINDOW': '🟡', 'STAGE_AND_RECHECK': '🔵'}
                        _er_icon = _er_icons.get(_final_er, '⚪')
                        _er_labels = {'EXECUTE_NOW': 'EXECUTE NOW', 'WAIT_FOR_WINDOW': 'WAIT FOR WINDOW', 'STAGE_AND_RECHECK': 'STAGE & RECHECK'}
                        _er_label = _er_labels.get(_final_er, _final_er)
                        _er_reason_safe = _final_err.replace("$", "\\$")
                        if _final_er == 'EXECUTE_NOW':
                            st.success(f"{_er_icon} **Execution: {_er_label}** — {_er_reason_safe}")
                        elif _final_er == 'WAIT_FOR_WINDOW':
                            st.warning(f"{_er_icon} **Execution: {_er_label}** — {_er_reason_safe}")
                        else:
                            st.info(f"{_er_icon} **Execution: {_er_label}** — {_er_reason_safe}")

                    # Show pre-staged roll candidates from doctrine engine (Schwab chain fetch)
                    if _has_roll_candidates:
                        st.markdown("**System-Ranked Roll Candidates** (from last scan):")
                        _any_shown = False

                        # Check for no-viable-roll verdict (set by EMERGENCY mode when no above-basis roll exists)
                        _cand1_raw, _ = _get_candidate(1)
                        if _cand1_raw and _cand1_raw.get("no_viable_roll"):
                            _nvr_verdict   = _cand1_raw.get("verdict", "NO_DATA")
                            _nvr_rationale = str(_cand1_raw.get("roll_rationale", "") or "")
                            _nvr_rationale_safe = _nvr_rationale.replace("$", "\\$")
                            if _nvr_verdict == "ASSIGNMENT_PREFERABLE":
                                st.success(
                                    f"**No credit roll found — Assignment is the better outcome.**  \n"
                                    f"{_nvr_rationale_safe}"
                                )
                            else:
                                st.error(
                                    f"**🚨 No viable above-basis roll found in 45–150 DTE range.**  \n"
                                    f"{_nvr_rationale_safe}"
                                )
                        else:
                            # Build all valid candidates first so we can compute relative comparisons
                            _all_cands = []
                            for i in range(1, 4):
                                cand, _from_db = _get_candidate(i)
                                if cand:
                                    _all_cands.append((i, cand, _from_db))

                            for _rank, (i, cand, _from_db) in enumerate(_all_cands):
                                _any_shown = True
                                _stale_tag = " *(prior run)*" if _from_db else ""
                                _cand_mode = cand.get("roll_mode", "")
                                # PRE_ITM means opposite things for long vs income options:
                                # - Income (BUY_WRITE/CC/CSP): short strike approaching stock price → assignment risk warning
                                # - Long (LONG_PUT/LONG_CALL): option deeply ITM → intrinsic-heavy, harvest winner
                                if _cand_mode == "PRE_ITM":
                                    _pre_itm_tag = (
                                        " 💰 *Deeply ITM — rolling to capture intrinsic*"
                                        if _is_directional_long
                                        else " ⚠️ *Pre-ITM window*"
                                    )
                                else:
                                    _pre_itm_tag = ""
                                _roll_mode_tag = {
                                    "WEEKLY":           " 🟡 *Weekly cycle (fragile position)*",
                                    "EMERGENCY":        " 🚨 *Emergency DTE search*",
                                    "BROKEN_RECOVERY":  " 🔴 *Broken recovery — 30–45 DTE (gamma reduction mode)*",
                                }.get(_cand_mode, _pre_itm_tag)

                                _cstrike  = cand.get("strike", "?")
                                _cexp     = cand.get("expiry", "?")
                                _cmid     = float(cand.get("mid", cand.get("credit", 0)) or 0)
                                _cyield   = float(cand.get("annualized_yield_pct", cand.get("annualized_yield", 0)) or 0)
                                _cdelta   = float(cand.get("delta", 0) or 0)
                                _cscore   = float(cand.get("score", 0) or 0)
                                _cliq     = cand.get("liq_grade", "")
                                _cspread  = cand.get("spread_pct")
                                _cdte     = cand.get("dte", "?")
                                _cprob    = float(cand.get("prob_otm_at_expiry", 0) or 0)
                                _ctheta   = float(cand.get("theta_per_day_dollars", 0) or 0)
                                _cotm_pct = float(cand.get("otm_pct", 0) or 0)
                                _cbe      = float(cand.get("breakeven_after_roll", 0) or 0)
                                _rationale = cand.get("roll_rationale", "")
                                _cal_note  = cand.get("calendar_note", "")

                                _cost_info = cand.get("cost_to_roll", {})
                                if isinstance(_cost_info, str):
                                    try:
                                        import json as _jj
                                        _cost_info = _jj.loads(_cost_info)
                                    except Exception:
                                        _cost_info = {}
                                _roll_type     = _cost_info.get("type", "")
                                _net_per       = float(_cost_info.get("net_per_contract", 0) or 0)
                                _net_total     = float(_cost_info.get("net_total", 0) or 0)
                                _n_contracts   = int(_cost_info.get("contracts", 1) or 1)

                                # ── One-sentence trade-off summary ──────────────────────
                                # Each candidate has a distinct character — surface it plainly.
                                # Compare against the other candidates to explain the trade-off.
                                _tradeoff_parts = []

                                # Cost framing (debit vs credit)
                                if _roll_type == "debit" and abs(_net_per) > 0:
                                    _tradeoff_parts.append(
                                        f"Costs **\\${abs(_net_per):.2f}/share** to roll "
                                        f"(\\${abs(_net_total):,.0f} total debit, {_n_contracts} contracts)"
                                    )
                                elif _roll_type == "credit" and _net_per > 0:
                                    _tradeoff_parts.append(
                                        f"Collects **\\${_net_per:.2f}/share** net credit"
                                    )

                                # Strike positioning
                                if _cotm_pct > 0:
                                    if _is_directional_long:
                                        # For LONG_PUT/LONG_CALL, "OTM" = the option hasn't paid off yet.
                                        # The relevant framing: how far does price need to move?
                                        # Only show P(ITM) when prob_otm_at_expiry is a real value
                                        # (> 0 AND < 100). When it's 0 (missing/unset), 100 - 0 = 100%
                                        # which is nonsense for a slightly OTM option.
                                        _cprob_valid = _cprob > 0 and _cprob < 100
                                        # Direction label: PUT needs price to fall; CALL needs price to rise
                                        _move_dir = "further decline" if "PUT" in str(entry_structure).upper() else "further rise"
                                        if _cprob_valid:
                                            _prob_itm = 100 - _cprob  # P(ITM at expiry)
                                            _tradeoff_parts.append(
                                                f"strike {_cotm_pct:.1f}% OTM (needs {_cotm_pct:.1f}% {_move_dir}; ~{_prob_itm:.0f}% P(ITM))"
                                            )
                                        else:
                                            # No probability data — show distance and direction only
                                            _tradeoff_parts.append(
                                                f"strike {_cotm_pct:.1f}% OTM (needs {_cotm_pct:.1f}% {_move_dir})"
                                            )
                                    else:
                                        _tradeoff_parts.append(
                                            f"strike {_cotm_pct:.1f}% OTM ({_cprob:.0f}% prob expires worthless)"
                                        )

                                # Theta
                                if _ctheta > 0:
                                    if _is_directional_long:
                                        # Theta is a COST for the buyer. Show new leg's decay AND
                                        # compare to current decay when available — rolling to longer
                                        # DTE typically reduces the daily burn rate.
                                        # McMillan Ch.8: "Theta is the rent you pay for the time you need."
                                        # net_t is negative for long options (e.g. -18.64 = $18.64/day loss)
                                        _cur_theta_abs = abs(float(net_t)) if (
                                            net_t is not None and float(net_t) != 0
                                        ) else None
                                        if _cur_theta_abs and _cur_theta_abs > 0:
                                            _theta_delta = _cur_theta_abs - _ctheta   # positive = roll saves
                                            if _theta_delta > 0.10:
                                                _tradeoff_parts.append(
                                                    f"costs **\\${_ctheta:.2f}/day** in theta decay "
                                                    f"(saves \\${_theta_delta:.2f}/day vs current)"
                                                )
                                            elif _theta_delta < -0.10:
                                                _tradeoff_parts.append(
                                                    f"costs **\\${_ctheta:.2f}/day** in theta decay "
                                                    f"(\\${abs(_theta_delta):.2f}/day more than current)"
                                                )
                                            else:
                                                _tradeoff_parts.append(
                                                    f"costs **\\${_ctheta:.2f}/day** in theta decay (≈ same as current)"
                                                )
                                        else:
                                            _tradeoff_parts.append(f"costs **\\${_ctheta:.2f}/day** in theta decay")
                                    else:
                                        _tradeoff_parts.append(f"earns **\\${_ctheta:.2f}/day** theta")

                                # Yield vs alternatives — income strategies only
                                # Yield is meaningless for long option buyers (they pay premium, not collect it)
                                if _cyield > 0 and not _is_directional_long:
                                    _yield_str = f"{_cyield:.0f}%/yr"
                                    # Flag relative to other candidates
                                    _other_yields = [
                                        float(c.get("annualized_yield_pct", 0) or 0)
                                        for _, c, _ in _all_cands
                                        if c is not cand and float(c.get("annualized_yield_pct", 0) or 0) > 0
                                    ]
                                    if _other_yields:
                                        _max_other = max(_other_yields)
                                        _min_other = min(_other_yields)
                                        if _cyield >= _max_other:
                                            _yield_str += " *(highest yield)*"
                                        elif _cyield <= _min_other:
                                            _yield_str += " *(lowest yield — more time buffer)*"
                                    _tradeoff_parts.append(f"yield {_yield_str}")

                                # Breakeven
                                if _cbe > 0:
                                    if _is_directional_long:
                                        # For LONG_PUT: breakeven = strike − premium paid (stock must fall below this)
                                        # For LONG_CALL: breakeven = strike + premium paid (stock must rise above this)
                                        _tradeoff_parts.append(f"breakeven at \\${_cbe:.2f} (stock must cross this to profit)")
                                    elif _eff_cost and pd.notna(_eff_cost):
                                        _be_vs_cost = _cbe - float(_eff_cost)
                                        _be_dir = f"+\\${_be_vs_cost:.2f} vs net cost" if _be_vs_cost > 0 else f"-\\${abs(_be_vs_cost):.2f} below net cost"
                                        _tradeoff_parts.append(f"new breakeven \\${_cbe:.2f} ({_be_dir})")

                                _tradeoff_sentence = " · ".join(_tradeoff_parts) if _tradeoff_parts else ""

                                # ── Header line ─────────────────────────────────────────
                                # Default: engine ranks by score (#1 = recommended).
                                # Override: if winner management panel recommends Path B (credit
                                # harvest), demote any debit candidate from "Recommended" — a debit
                                # roll contradicts the harvest decision. Credit roll is Path B target.
                                _cand_cost_info = cand.get("cost_to_roll", {})
                                if isinstance(_cand_cost_info, str):
                                    try:
                                        import json as _jj_rc; _cand_cost_info = _jj_rc.loads(_cand_cost_info)
                                    except Exception:
                                        _cand_cost_info = {}
                                _cand_roll_type = _cand_cost_info.get("type", "")
                                _winner_harvest_mode = _wm_rec_for_roll == "ROLL_DOWN" and _is_directional_long
                                if _doc_action == "EXIT" and _is_bw:
                                    # EXIT on BUY_WRITE: candidates are reference-only, not a roll recommendation.
                                    # Winner Panel owns the execution decision — suppress Recommended badge.
                                    _rec_tag = " *(reference only — EXIT action)*" if i == 1 else ""
                                elif _winner_harvest_mode and _cand_roll_type == "debit" and i == 1:
                                    # Debit candidate ranked #1 but winner panel says credit harvest
                                    _rec_tag = " ⚠️ *Debit roll — see Winner Panel for credit harvest candidate*"
                                elif _winner_harvest_mode and _cand_roll_type == "credit":
                                    # Credit candidate: promote as harvest path
                                    _rec_tag = " ✅ **Credit Harvest (Path B)**"
                                else:
                                    if i == 1:
                                        # For directional losers rolling with a significant debit,
                                        # qualify the recommendation so the cost is visible in the header.
                                        _is_debit_roll = str(_cand_roll_type).lower() == "debit"
                                        _net_ctr_val = 0.0
                                        try:
                                            _net_ctr_val = float(_cand_cost_info.get("net_per_contract", 0) or 0)
                                        except (ValueError, TypeError):
                                            pass
                                        _debit_per_share = abs(_net_ctr_val) / 100
                                        _position_at_loss = _is_directional_long and (total_gl_val < 0)
                                        if _is_debit_roll and _position_at_loss and _debit_per_share >= 5.0:
                                            _rec_tag = f" ✅ **Recommended** *(debit roll — costs \\${_debit_per_share:.2f}/sh to extend)*"
                                        else:
                                            _rec_tag = " ✅ **Recommended**"
                                    else:
                                        _rec_tag = ""
                                _liq_warn = (f" ⚠️ *Liquidity: {_cliq} — verify live spread*"
                                             if i == 1 and _cliq in ("THIN", "AVOID") else "")
                                _yield_display = f"{_cyield:.0%}" if _cyield < 5 else f"{_cyield:.0f}%"
                                # Yield is income-strategy framing — suppress for directional long buyers
                                _yield_part = (
                                    f" · Yield **{_yield_display}/yr**"
                                    if _cyield and _cyield > 0 and not _is_directional_long
                                    else ""
                                )

                                # _stale_tag removed from header — shown as a caption instead
                                st.markdown(
                                    f"**#{i}**{_rec_tag}{_roll_mode_tag}{_liq_warn}  \n"
                                    f"Strike **\\${_fmt_strike(_cstrike)}** · Exp **{_cexp}** ({_cdte}d) · "
                                    f"Mid **\\${_cmid:.2f}/share**{_yield_part} · "
                                    f"Δ {_cdelta:.2f} · Liq {_cliq}"
                                    + (f" · Spread {_cspread:.1f}%" if _cspread is not None else "")
                                )
                                # Staleness notice — separate from header so it reads as a warning, not a tag
                                if _from_db:
                                    st.caption(
                                        "⏳ *From prior run — prices, OI, and spread may have moved. "
                                        "Verify live before executing.*"
                                    )

                                # ── Chase & Bound (execution discipline block) ────────────────
                                # Fidelity/Schwab show per-leg quotes. The NET roll credit is:
                                #   net_bid = new_leg_bid − old_leg_ask  (worst you'd get)
                                #   net_ask = new_leg_ask − old_leg_bid  (best you'd get)
                                #   net_mid = (net_bid + net_ask) / 2    → start limit here
                                #
                                # McMillan Ch.6: "Use a limit at mid-market. If unfilled after
                                # 3 minutes, move 10% of the spread toward market — max 2×.
                                # Cancel if still unfilled — do not chase to market."
                                try:
                                    # Resolve live quotes for this candidate's new leg + current leg
                                    _cand_sym = cand.get("symbol") or cand.get("contract_symbol")
                                    _new_leg_q  = (_opt_spreads or {}).get(str(_cand_sym) if _cand_sym else "", {})
                                    _old_leg_q  = {}
                                    for _cls in _current_leg_syms:
                                        if _cls in (_opt_spreads or {}):
                                            _old_leg_q = _opt_spreads[_cls]
                                            break

                                    _have_live = bool(_new_leg_q.get("bid") or _new_leg_q.get("ask"))

                                    if _have_live:
                                        # Live two-leg net roll pricing
                                        _new_bid = float(_new_leg_q.get("bid") or 0)
                                        _new_ask = float(_new_leg_q.get("ask") or 0)
                                        _old_bid = float(_old_leg_q.get("bid") or 0)
                                        _old_ask = float(_old_leg_q.get("ask") or 0)

                                        # Credit roll: sell new, buy back old
                                        # Net credit worst (bid side): new_bid − old_ask
                                        # Net credit best (ask side): new_ask − old_bid
                                        _net_bid = _new_bid - _old_ask   # what you'd get at market
                                        _net_ask = _new_ask - _old_bid   # what you'd collect if MMs fill at ask
                                        _net_mid = (_net_bid + _net_ask) / 2 if (_net_bid + _net_ask) != 0 else 0
                                        _net_width = max(0.0, _net_ask - _net_bid)
                                        _data_label = "live"
                                    else:
                                        # No live data — use stale candidate mid only.
                                        # Do NOT back-calculate fake bid/ask from spread_pct;
                                        # show mid as reference only, require live refresh to execute.
                                        _net_mid   = _cmid
                                        _net_width = None   # unknown without live data
                                        _net_bid   = None
                                        _net_ask   = None
                                        _data_label = "stale scan"

                                    _net_sp_pct = (_net_width / abs(_net_mid) * 100) if (_net_width and _net_mid) else None

                                    _is_credit_roll = _roll_type == "credit" or (not _is_directional_long and _net_per >= 0)

                                    # Chase step: 10% of net width, minimum $0.01 (options tick).
                                    # When net_width is unknown (stale), default $0.01.
                                    _MIN_TICK = 0.01
                                    _chase_step_credit = max(_MIN_TICK, (_net_width or 0) * 0.10)
                                    _chase_step_debit  = max(_MIN_TICK, (_net_width or 0) * 0.10)

                                    if _is_credit_roll and _net_mid > 0:
                                        # Credit: receive premium. Start at net mid.
                                        # Floor = net_bid when live; 75% of mid otherwise.
                                        # Never chase below the floor — market isn't there.
                                        _limit_start = _net_mid
                                        if _have_live and _net_bid is not None:
                                            _effective_floor = max(_net_bid, _net_mid * 0.75, _MIN_TICK)
                                            _bid_ask_line = (
                                                f"**Net Bid:** \\${_net_bid:.2f} · "
                                                f"**Net Mid:** \\${_net_mid:.2f} · "
                                                f"**Net Ask:** \\${_net_ask:.2f}  \n"
                                            )
                                        else:
                                            _effective_floor = _net_mid * 0.75
                                            _bid_ask_line = (
                                                f"**Mid (stale):** \\${_net_mid:.2f} — "
                                                f"refresh for live net bid/ask  \n"
                                            )

                                        _price_source = "live" if _have_live else "stale — refresh before ordering"
                                        _exec_box = (
                                            f"**📋 Execution Order** (credit roll · {_price_source})  \n"
                                            + _bid_ask_line +
                                            f"**Start limit at:** \\${_limit_start:.2f}/share net credit  \n"
                                            f"**Chase:** \\${_chase_step_credit:.2f} lower every 3 min · max 2× · cancel at \\${_effective_floor:.2f}  \n"
                                            f"**Rule:** floor hit with no fill → cancel, wait 15 min, re-enter"
                                        )
                                        _exec_color = "success" if (_net_sp_pct or 99) < 5 else "warning"

                                    elif not _is_credit_roll and _net_mid > 0:
                                        # Debit: paying to roll. Chase UP max 2×, never above net_ask.
                                        _limit_start = _net_mid
                                        if _have_live and _net_ask is not None:
                                            _effective_ceil = _net_ask
                                            _bid_ask_line = (
                                                f"**Net Bid:** \\${_net_bid:.2f} · "
                                                f"**Net Mid:** \\${_net_mid:.2f} · "
                                                f"**Net Ask:** \\${_net_ask:.2f}  \n"
                                            )
                                        else:
                                            _effective_ceil = _net_mid * 1.25
                                            _bid_ask_line = (
                                                f"**Mid (stale):** \\${_net_mid:.2f} — "
                                                f"refresh for live net bid/ask  \n"
                                            )

                                        _price_source = "live" if _have_live else "stale — refresh before ordering"
                                        _exec_box = (
                                            f"**📋 Execution Order** (debit roll · {_price_source})  \n"
                                            + _bid_ask_line +
                                            f"**Start limit at:** \\${_limit_start:.2f}/share net debit  \n"
                                            f"**Chase:** \\${_chase_step_debit:.2f} higher every 3 min · max 2× · ceiling \\${_effective_ceil:.2f}  \n"
                                            f"**Rule:** ceiling hit with no fill → cancel, re-evaluate timing"
                                        )
                                        _exec_color = "warning"

                                    else:
                                        _exec_box = None
                                        _exec_color = None

                                    if _exec_box:
                                        if _exec_color == "success":
                                            st.success(_exec_box)
                                        else:
                                            st.warning(_exec_box)

                                        if not _have_live:
                                            st.caption(
                                                "⚠️ Prices above are from the last scan — click **🔄 Refresh Live** "
                                                "to get current net bid/ask before placing the order."
                                            )
                                        elif _net_sp_pct > 12:
                                            st.error(
                                                f"⚠️ Net spread **{_net_sp_pct:.1f}%** — very wide combo. "
                                                "Consider legging: buy back old leg first (limit at mid), "
                                                "then sell new leg separately when spread tightens."
                                            )
                                        elif _net_sp_pct > 6:
                                            st.caption(
                                                f"Net spread {_net_sp_pct:.1f}% — moderate. "
                                                "Post limit at mid; do not chase past the floor."
                                            )
                                        else:
                                            st.caption(f"Net spread {_net_sp_pct:.1f}% — tight. Mid limit should fill within 1–2 min.")
                                except Exception:
                                    pass  # never break candidate rendering

                                # Trade-off sentence — one line, always shown
                                if _tradeoff_sentence:
                                    st.caption(f"↳ {_tradeoff_sentence}")

                                # Calendar urgency note — shown when present
                                if _cal_note:
                                    st.caption(_cal_note.strip(" |"))

                                # Full rationale — collapsible, not shown by default
                                if _rationale and len(_rationale) > 40:
                                    with st.expander("Details", expanded=False):
                                        st.caption(_rationale.replace("$", "\\$"))

                            if not _any_shown:
                                st.caption("Candidates stored but could not be parsed.")
                    else:
                        # No pre-fetched candidates
                        if _doc_action == "ROLL_WAIT":
                            # ROLL_WAIT means the engine already gated this — don't show a manual
                            # framework that implies the roll should happen now. The whole point
                            # of ROLL_WAIT is that execution requires live chain confirmation first.
                            st.info(
                                "**Roll structure confirmed — awaiting live chain data.**  \n"
                                "EV favors rolling but the credit is unverified (no Schwab chain fetched).  \n"
                                "Run the pipeline during market hours to populate actual bid/ask, "
                                "spread quality, and roll candidates before executing."
                            )
                        else:
                            # ROLL with no candidates — show manual scenario framework as fallback
                            st.markdown("**Manual Roll Framework** (Schwab chain not fetched):")

                        # Pull current call leg details — shown for both ROLL and ROLL_WAIT
                        for _, _leg in opt_legs.iterrows():
                            _cur_strike = pd.to_numeric(_leg.get("Strike"), errors="coerce")
                            _cur_exp    = _leg.get("Expiration")
                            _cur_dte    = pd.to_numeric(_leg.get("DTE"), errors="coerce")
                            _cur_last   = pd.to_numeric(_leg.get("Last"), errors="coerce")
                            _cur_delta  = pd.to_numeric(_leg.get("Delta"), errors="coerce")

                            if pd.notna(_cur_strike):
                                st.markdown(f"**Current:** Short Call `${_cur_strike:.1f}` exp `{pd.to_datetime(_cur_exp).strftime('%b %d') if pd.notna(_cur_exp) else '?'}` · Last `${_cur_last:.2f}` · Δ `{_cur_delta:.3f}`")

                        if _doc_action != "ROLL_WAIT" and _eff_cost and pd.notna(_spot):
                            st.markdown("**Consider rolling to:**")
                            roll_rows = []
                            # Determine if we're rolling a call or put to set strike direction.
                            # Calls: strikes go UP from spot (OTM = higher).
                            # Puts:  strikes go DOWN from spot (OTM = lower).
                            _roll_is_call = True  # default: BW/CC always roll calls
                            if not opt_legs.empty and "Call/Put" in opt_legs.columns:
                                _cp_val = str(opt_legs.iloc[0].get("Call/Put", "Call") or "Call").upper()
                                _roll_is_call = _cp_val in ("C", "CALL")

                            if _roll_is_call:
                                _scenarios = [
                                    ("Aggressive (ATM)", 1.00),
                                    ("Neutral (+2% OTM)", 1.02),
                                    ("Defensive (+5% OTM)", 1.05),
                                ]
                            else:
                                _scenarios = [
                                    ("Aggressive (ATM)", 1.00),
                                    ("Neutral (-2% OTM)", 0.98),
                                    ("Defensive (-5% OTM)", 0.95),
                                ]

                            for _label, _strike_pct in _scenarios:
                                _new_strike = round(_spot * _strike_pct, 1)
                                roll_rows.append({
                                    "Scenario": _label,
                                    "Strike": f"${_new_strike:.1f}",
                                    "vs Net Cost": f"{'ABOVE' if _new_strike >= _eff_cost else 'below'} ${_eff_cost:.2f}",
                                    "Est. Credit": "Check live chain",
                                    "New Basis Est.": "= Net Cost − Credit",
                                })
                            st.dataframe(pd.DataFrame(roll_rows), hide_index=True)

                        if _doc_action != "ROLL_WAIT":
                            st.caption(
                                "💡 **Rule of thumb** (McMillan Ch.3): roll to a strike where "
                                "the credit received ≥ cost-to-close current short. "
                                "Net debit rolls only make sense if the new strike is meaningfully above net cost."
                            )

                    # Auto-resolved checklist from last pipeline run data
                    _hs = locals().get("_hard_stop")
                    _sp = locals().get("_spot")
                    _hard_stop_val = float(_hs) if _hs is not None and pd.notna(_hs) else None
                    _spot_val = float(_sp) if _sp is not None and pd.notna(_sp) else None

                    # Execution readiness + arbitration rendered before candidates (see block below)

                    # ── Intraday Roll Advisory (CRITICAL/HIGH ROLL only) ────────
                    # Shown only when timing classified as BREAKOUT_UP or BREAKOUT_DOWN.
                    # Non-blocking: the ROLL decision stands; this answers "when within today?"
                    # Passarelli Ch.6: intraday execution timing affects fill quality.
                    _intraday_adv = None
                    if _doc_row is not None:
                        import json as _j2
                        _iadv_raw = _doc_row.get("Intraday_Advisory_JSON")
                        if _iadv_raw and str(_iadv_raw) not in ("nan", "None", ""):
                            try:
                                _intraday_adv = (
                                    _iadv_raw if isinstance(_iadv_raw, dict)
                                    else _j2.loads(str(_iadv_raw))
                                )
                            except Exception:
                                _intraday_adv = None

                    _doc_urgency_upper = str(_doc_row.get("Urgency", "") or "").upper() if _doc_row is not None else ""
                    _show_intraday = (
                        _intraday_adv is not None
                        and _doc_action in ("ROLL", "EXIT")
                        and _doc_urgency_upper in ("CRITICAL", "HIGH")
                    )
                    if _show_intraday:
                        _pv = _intraday_adv.get("proxy_verdict", "VERIFY_FIRST")
                        _pv_icons = {
                            "EXECUTE_NOW":       "🔴",
                            "FAVORABLE_WINDOW":  "🟡",
                            "VERIFY_FIRST":      "🔵",
                        }
                        _pv_icon = _pv_icons.get(_pv, "⚪")
                        with st.expander(
                            f"📡 Intraday {'Exit' if _doc_action == 'EXIT' else 'Roll'} Advisory — {_pv_icon} {_pv.replace('_', ' ')}",
                            expanded=(_pv == "EXECUTE_NOW"),
                        ):
                            st.caption(
                                "⚠️ **System uses end-of-day bars only** — intraday candles, VWAP, "
                                "and real-time volume are not available. The proxies below are "
                                "derived from available live data. The checklist items require "
                                "manual verification in your broker chart."
                            )
                            _ps = _intraday_adv.get("proxy_summary", "")
                            if _pv == "EXECUTE_NOW":
                                st.error(f"**{_ps}**")
                            elif _pv == "FAVORABLE_WINDOW":
                                st.warning(f"**{_ps}**")
                            else:
                                st.info(f"**{_ps}**")

                            # Live proxy signals grid
                            _sigs = _intraday_adv.get("signals", {})
                            if _sigs:
                                st.markdown("**Live Proxy Signals** (from current session data):")
                                _sig_cols = st.columns(len(_sigs))
                                _sig_items = list(_sigs.items())
                                for _si, (_sk, _sv) in enumerate(_sig_items):
                                    _sig_label = {
                                        "intraday_chg_pct": "Intraday Chg",
                                        "delta_drift_pct":  "Delta Drift",
                                        "iv_drift_pct":     "IV Drift",
                                        "atr_multiple":     "Move / ATR",
                                        "adx_strength":     "ADX Strength",
                                        "spread_pct":       "Spread %",
                                        "volume_vs_oi":     "Vol / OI",
                                        "distance_to_target_pct": "Dist to Target",
                                        "momentum_alignment": "Mom. Aligned",
                                        "theta_to_move_ratio": "θ/Move",
                                    }.get(_sk, _sk.replace("_", " ").title())
                                    _sig_val = (
                                        f"{_sv:+.1f}%" if "pct" in _sk and isinstance(_sv, (int, float))
                                        else f"{_sv:.2f}×" if _sk in ("atr_multiple", "theta_to_move_ratio") and isinstance(_sv, (int, float))
                                        else str(_sv) if _sv is not None
                                        else "—"
                                    )
                                    _sig_cols[_si].metric(_sig_label, _sig_val)

                            # Proxy notes
                            _notes = _intraday_adv.get("notes", [])
                            if _notes:
                                for _n in _notes:
                                    st.caption(f"• {_n}")

                            # Manual verification checklist
                            _chk = _intraday_adv.get("checklist", [])
                            if _chk:
                                st.divider()
                                st.markdown(
                                    "**Manual Verification Checklist** — "
                                    "check in your broker (Fidelity/Schwab) before sending the order:"
                                )
                                for _ci, _ch in enumerate(_chk, 1):
                                    with st.expander(
                                        f"{'✅' if _pv == 'EXECUTE_NOW' else '☐'} "
                                        f"{_ci}. {_ch['item']}",
                                        expanded=False
                                    ):
                                        st.caption(_ch["description"])

                    # ── MC Management Panels ───────────────────────────────────
                    # Three context-sensitive MC panels, each shown only when relevant.
                    # All data comes from positions_latest.csv (populated by run_all.py).

                    # Panel A: Roll Wait-Cost (ROLL / STAGE_AND_RECHECK rows)
                    if _doc_row is not None and _doc_action in ("ROLL", "ROLL_WAIT") or _final_er == "STAGE_AND_RECHECK":
                        _mc_w_verdict = str(_doc_row.get("MC_Wait_Verdict", "") or "") if _doc_row is not None else ""
                        _mc_w_note    = str(_doc_row.get("MC_Wait_Note",    "") or "") if _doc_row is not None else ""
                        _mc_w_p_imp   = _doc_row.get("MC_Wait_P_Improve") if _doc_row is not None else None
                        _mc_w_p_brch  = _doc_row.get("MC_Wait_P_Assign")  if _doc_row is not None else None
                        _mc_w_cr_dlt  = _doc_row.get("MC_Wait_Credit_Delta") if _doc_row is not None else None
                        _mc_w_days    = _doc_row.get("MC_Wait_Days", 3)    if _doc_row is not None else 3

                        if _mc_w_verdict and _mc_w_verdict not in ("", "SKIP", "MC_SKIP"):
                            _card_metrics["mc_wait"] = {
                                "verdict": _mc_w_verdict,
                                "breach": f"{float(_mc_w_p_brch):.0%}" if _mc_w_p_brch is not None and str(_mc_w_p_brch) not in ("nan","None","") else "—",
                                "median_delta": f"${float(_mc_w_cr_dlt):+.0f}" if _mc_w_cr_dlt is not None and str(_mc_w_cr_dlt) not in ("nan","None","") else "—",
                            }
                            # Icon semantics (consistent across long and short options):
                            #   WAIT    → 🟢 (conditions favor waiting — no structural urgency)
                            #   HOLD    → 🟡 (mixed signals, no clear urgency)
                            #   ACT_NOW → 🔴 (act now — genuine risk signal)
                            # Exception: for DIRECTIONAL LONG positions where doctrine urgency is
                            # HIGH/CRITICAL, MC "WAIT" means "the roll price is good — option retains
                            # value" — NOT "delay the structural decision". In this case the 🟢 icon
                            # contradicts the urgency banner, so we demote to 🟡 "WAIT*" to signal
                            # "MC says wait on price, but structure overrides."
                            _mc_structural_override = (
                                _is_directional_long
                                and _mc_w_verdict == "WAIT"
                                and _doc_urgency_upper in ("HIGH", "CRITICAL")
                            )
                            if _mc_w_verdict == "WAIT":
                                _w_icon = "🟡" if _mc_structural_override else "🟢"
                                _w_label = "WAIT* (structure overrides)" if _mc_structural_override else "WAIT"
                            elif _mc_w_verdict == "HOLD":
                                _w_icon = "🟡"
                                _w_label = "HOLD"
                            else:  # ACT_NOW
                                _w_icon = "🔴"
                                _w_label = "ACT_NOW"
                            with st.expander(f"🎲 MC Roll Wait-Cost — {_w_icon} {_w_label}", expanded=(_mc_w_verdict == "ACT_NOW")):
                                try:
                                    _wc1, _wc2, _wc3 = st.columns(3)
                                    with _wc1:
                                        _imp_f = (float(_mc_w_p_imp) if _mc_w_p_imp is not None
                                                  and str(_mc_w_p_imp) not in ("nan","None","") else None)
                                        if _is_directional_long:
                                            # "credit improves" = roll gets cheaper = option lost value = thesis stalling
                                            _imp_icon = "🟡" if (_imp_f or 0) >= 0.35 else "🟢"
                                            st.metric(f"{_imp_icon} P(roll cheaper +20%)", f"{_imp_f:.0%}" if _imp_f is not None else "—",
                                                      help=f"Probability that waiting {_mc_w_days}d makes the roll CHEAPER by ≥20% — "
                                                           f"happens when price moves against your thesis (option loses value)")
                                        else:
                                            _imp_icon = "🟢" if (_imp_f or 0) >= 0.35 else "🟡"
                                            st.metric(f"{_imp_icon} P(credit +20%)", f"{_imp_f:.0%}" if _imp_f is not None else "—",
                                                      help=f"Probability that waiting {_mc_w_days}d improves roll credit by ≥20%")
                                    with _wc2:
                                        _brch_f = (float(_mc_w_p_brch) if _mc_w_p_brch is not None
                                                   and str(_mc_w_p_brch) not in ("nan","None","") else None)
                                        if _is_directional_long:
                                            # MC_Wait_P_Assign for long options = P(adverse move — option goes OTM).
                                            # Low = good (option stays ITM). High = warn (option at OTM risk).
                                            # Icon: 🟢 = low adverse risk (<15%), 🟡 = moderate, 🔴 = high (>40%)
                                            _put_or_call = "put" if "PUT" in str(entry_structure).upper() else "call"
                                            _adverse_f = _brch_f  # P(option goes OTM in wait window)
                                            _itm_prob = (1 - _adverse_f) if _adverse_f is not None else None
                                            _brch_icon = ("🟢" if (_adverse_f or 1) < 0.15
                                                          else ("🟡" if (_adverse_f or 1) < 0.40 else "🔴"))
                                            _itm_label = f"P(stays ITM in {_mc_w_days}d)"
                                            st.metric(f"{_brch_icon} {_itm_label}",
                                                      f"{_itm_prob:.0%}" if _itm_prob is not None else "—",
                                                      help=f"Probability your long {_put_or_call} remains in-the-money over "
                                                           f"the {_mc_w_days}d wait window. "
                                                           f"Higher = safer to wait (thesis working). "
                                                           f"Low = option at risk of going OTM — consider acting sooner.")
                                        else:
                                            _brch_icon = "🔴" if (_brch_f or 0) >= 0.25 else ("🟡" if (_brch_f or 0) >= 0.10 else "🟢")
                                            st.metric(f"{_brch_icon} P(breach in wait)", f"{_brch_f:.0%}" if _brch_f is not None else "—",
                                                      help=f"Probability short strike is breached during {_mc_w_days}d wait window")
                                    with _wc3:
                                        _crd_f = (float(_mc_w_cr_dlt) if _mc_w_cr_dlt is not None
                                                  and str(_mc_w_cr_dlt) not in ("nan","None","") else None)
                                        if _is_directional_long:
                                            # MC engine now stores credit_delta_per_contract with correct sign
                                            # for long options: positive = option gained value (thesis working).
                                            _crd_icon = "🟢" if (_crd_f or 0) > 200 else ("🟡" if (_crd_f or 0) > 0 else "🔴")
                                            st.metric(f"{_crd_icon} Median Option Value Δ",
                                                      f"${_crd_f:+,.0f}/contract" if _crd_f is not None else "—",
                                                      help="Expected change in your option's value if you wait "
                                                           "(positive = option gained value, thesis working; "
                                                           "negative = option lost value, thesis stalling)")
                                        else:
                                            st.metric("Median Credit Δ", f"${_crd_f:+,.0f}/contract" if _crd_f is not None else "—",
                                                      help="Expected change in roll credit if you wait (positive = better credit)")
                                    if _mc_w_note and not _mc_w_note.startswith("MC_"):
                                        # MC note is already correctly formatted by the engine:
                                        # - Long options: "P(ITM)=83% | P(value+20%)=..." with correct sign
                                        # - Short options: "P(thesis)=17% | P(credit+20%)=..."
                                        # Display verbatim — no reframing needed.
                                        st.caption(f"_{_mc_w_note}_")

                                    # ── MC verdict vs ROLL doctrine reconciliation ──────────────
                                    # MC WAIT/HOLD can appear to conflict with ROLL doctrine.
                                    # These measure different things:
                                    #   WAIT  = option value likely improves if you delay the roll
                                    #   HOLD  = mixed signals, no clear roll urgency from MC alone
                                    # In both cases doctrine may still say ROLL on structural grounds
                                    # (vol regime shift, momentum reversal, thesis degraded) that are
                                    # independent of whether the roll price improves by waiting.
                                    if _is_directional_long and _mc_w_verdict == "WAIT" and _doc_urgency_upper in ("HIGH", "CRITICAL"):
                                        st.info(
                                            "**MC 'WAIT' ≠ delay the roll.** "
                                            "The MC simulation shows the option retains value if you wait — "
                                            "but doctrine fired on momentum/intrinsic signals that are independent "
                                            "of roll pricing. Rolling now harvests the gain before a potential reversal; "
                                            "waiting risks giving back intrinsic if the thesis turns. "
                                            "MC tells you the roll won't be cheaper — not that the structural urgency is wrong."
                                        )
                                    elif _is_directional_long and _mc_w_verdict == "HOLD":
                                        st.info(
                                            "**MC 'HOLD' ≠ hold the position.** "
                                            "MC sees mixed signals — no strong probability edge in either direction. "
                                            "Doctrine rolled on a structural signal (vol regime, thesis degradation) "
                                            "that MC does not model. Roll is the structural response; "
                                            "MC indifference is not a reason to override it."
                                        )

                                    # ── Vol-regime conflict caveat ──────────────────────────────
                                    # If doctrine fired because of a vol-regime shift, the MC σ
                                    # (backward-looking HV) contradicts the forward vol assumption.
                                    # Surface the assumption conflict explicitly so the trader knows
                                    # the wait-cost estimate is backward-looking on vol.
                                    _doc_rationale = str(_doc_row.get("Rationale", "") or "") if _doc_row is not None else ""
                                    _regime_shift_keywords = (
                                        "vol regime", "regime shift", "regime degraded",
                                        "extreme→", "compressed", "expanding", "vol shift",
                                        "volatility regime", "regime changed",
                                    )
                                    _is_regime_shift_roll = any(
                                        kw in _doc_rationale.lower() for kw in _regime_shift_keywords
                                    )
                                    if _is_regime_shift_roll and _mc_w_note:
                                        # Extract σ value and source tag from MC note
                                        # Note format: "σ=57%[EWMA]" or "σ=61%[HV_30D]" or "σ=61%"
                                        import re as _re2
                                        _sigma_match = _re2.search(r"σ=(\d+(?:\.\d+)?)%(?:\[([^\]]+)\])?", _mc_w_note)
                                        if _sigma_match:
                                            _sigma_val = _sigma_match.group(1)
                                            _sigma_src = _sigma_match.group(2) or "HV"
                                        else:
                                            _sigma_val, _sigma_src = None, "HV"
                                        _is_ewma     = _sigma_src == "EWMA"
                                        _is_hmm      = "HMM" in str(_sigma_src).upper()
                                        if _sigma_val:
                                            _sigma_label = f"σ={_sigma_val}% ({_sigma_src})"
                                        else:
                                            _sigma_label = "historical σ"
                                        # Detect regime direction from rationale
                                        _rat_lower = _doc_rationale.lower()
                                        _vol_falling = any(kw in _rat_lower for kw in (
                                            "expanding→compressed", "expanding→compress",
                                            "high→low", "extreme→normal", "high vol→",
                                            "vol crush", "vol compression", "compressed",
                                        ))
                                        _vol_rising = any(kw in _rat_lower for kw in (
                                            "compressed→expand", "low→high", "normal→high",
                                            "vol expansion", "vol spike", "vol rising",
                                        ))
                                        if _is_hmm:
                                            # HMM_BLEND: regime-aware sigma — most accurate source.
                                            # Explain what it means in context of the specific shift direction.
                                            if _vol_falling:
                                                st.info(
                                                    f"📐 **MC uses HMM regime σ ({_sigma_val}%)** — "
                                                    f"this reflects the *current detected regime* (HIGH_VOL state, realized "
                                                    f"before compression began). Forward vol under the new compressed regime "
                                                    f"will likely be lower. MC is therefore **overstating expected moves** "
                                                    f"and **understating vega bleed speed**. "
                                                    f"Treat wait-cost as a *floor* — actual cost of waiting may be higher "
                                                    f"as vega erodes faster in a low-vol environment.",
                                                )
                                            elif _vol_rising:
                                                st.info(
                                                    f"📐 **MC uses HMM regime σ ({_sigma_val}%)** — "
                                                    f"this reflects the *current detected regime* (LOW_VOL state, realized "
                                                    f"before expansion). Forward vol under the new expanding regime "
                                                    f"will likely be higher. MC is therefore **understating expected moves** "
                                                    f"and the position may move faster than modeled. "
                                                    f"Treat wait-cost as a *ceiling* — actual credit improvement may be larger.",
                                                )
                                            else:
                                                st.info(
                                                    f"📐 **MC uses HMM regime σ ({_sigma_val}%)** — "
                                                    f"regime-conditioned blend (70% HMM state mean, 30% EWMA). "
                                                    f"Most accurate available sigma. "
                                                    f"Doctrine fired on a vol-regime structural signal not captured by MC paths.",
                                                )
                                        elif _is_ewma:
                                            # EWMA: partially reactive, still lags
                                            st.info(
                                                f"📐 **MC uses EWMA σ ({_sigma_val}%)** — "
                                                f"reactive to recent vol (λ=0.94, ≈17d window) but may lag the "
                                                f"regime transition by 2–3 sessions. "
                                                f"Doctrine fired on the structural shift; MC output will self-correct "
                                                f"as new sessions are added.",
                                            )
                                        else:
                                            # Flat HV — least reactive, original warning
                                            st.warning(
                                                f"⚠️ **MC assumption conflict:** This simulation uses {_sigma_label} "
                                                f"(backward-looking realized vol). Doctrine fired because the vol regime "
                                                f"has shifted — the historical sigma does not reflect the new environment. "
                                                f"Treat the MC output as directionally informative only.",
                                                icon="📐",
                                            )
                                except Exception:
                                    st.caption(f"🎲 MC wait-cost: {_mc_w_note}")

                    # Panel B: Exit vs Hold (HOLD rows)
                    if _doc_row is not None and _doc_action in ("HOLD", "HOLD_FOR_REVERSION", "REVALIDATE"):
                        _mc_h_verdict = str(_doc_row.get("MC_Hold_Verdict",    "") or "") if _doc_row is not None else ""
                        _mc_h_p_rec   = _doc_row.get("MC_Hold_P_Recovery") if _doc_row is not None else None
                        _mc_h_p_ml    = _doc_row.get("MC_Hold_P_MaxLoss")  if _doc_row is not None else None
                        _mc_h_p10     = _doc_row.get("MC_Hold_P10")        if _doc_row is not None else None
                        _mc_h_p50     = _doc_row.get("MC_Hold_P50")        if _doc_row is not None else None
                        _mc_h_ev      = _doc_row.get("MC_Hold_EV")         if _doc_row is not None else None
                        _mc_h_note    = str(_doc_row.get("MC_Hold_Note",   "") or "") if _doc_row is not None else ""

                        if _mc_h_verdict and _mc_h_verdict not in ("", "SKIP"):
                            _h_icon = {"HOLD_JUSTIFIED": "🟢", "EXIT_NOW": "🔴", "MONITOR": "🟡"}.get(_mc_h_verdict, "⚪")
                            with st.expander(f"🎲 MC Exit vs Hold — {_h_icon} {_mc_h_verdict}", expanded=(_mc_h_verdict == "EXIT_NOW")):
                                try:
                                    _hc1, _hc2, _hc3, _hc4 = st.columns(4)
                                    with _hc1:
                                        _rec_f = float(_mc_h_p_rec) if _mc_h_p_rec and str(_mc_h_p_rec) not in ("nan","None","") else None
                                        _rec_icon = "🟢" if (_rec_f or 0) >= 0.55 else ("🟡" if (_rec_f or 0) >= 0.35 else "🔴")
                                        st.metric(f"{_rec_icon} P(recovery)", f"{_rec_f:.0%}" if _rec_f is not None else "—",
                                                  help="Probability position recovers to breakeven or better at expiry")
                                    with _hc2:
                                        _ml_f = float(_mc_h_p_ml) if _mc_h_p_ml and str(_mc_h_p_ml) not in ("nan","None","") else None
                                        _ml_icon = "🔴" if (_ml_f or 0) >= 0.40 else ("🟡" if (_ml_f or 0) >= 0.20 else "🟢")
                                        st.metric(f"{_ml_icon} P(max loss)", f"{_ml_f:.0%}" if _ml_f is not None else "—",
                                                  help="Probability position decays to ≥85% of max theoretical loss")
                                    with _hc3:
                                        _p10_f = float(_mc_h_p10) if _mc_h_p10 and str(_mc_h_p10) not in ("nan","None","") else None
                                        st.metric("P10 outcome", f"${_p10_f:+,.0f}" if _p10_f is not None else "—",
                                                  help="10th-percentile P&L per contract if held to expiry")
                                    with _hc4:
                                        _ev_f = float(_mc_h_ev) if _mc_h_ev and str(_mc_h_ev) not in ("nan","None","") else None
                                        _ev_icon = "🟢" if (_ev_f or 0) >= 0 else "🔴"
                                        st.metric(f"{_ev_icon} EV (hold)", f"${_ev_f:+,.0f}" if _ev_f is not None else "—",
                                                  help="Expected value per contract at expiry vs locking current P&L now")
                                    if _mc_h_note and not _mc_h_note.startswith("MC_"):
                                        st.caption(f"_{_mc_h_note}_")

                                    # ── EV paradox explanation ───────────────────────────
                                    # EXIT_NOW with positive EV is not a contradiction:
                                    # a long put has asymmetric payoff — the winning paths
                                    # (stock collapses below strike) win very large, while
                                    # the losing paths (stock stays above strike) each lose
                                    # exactly the premium paid. When P(max_loss) is high,
                                    # most paths lose the full premium but the few winning
                                    # paths pull the EV positive. That is a low-probability,
                                    # high-magnitude bet — not a "HOLD is safe" signal.
                                    if _mc_h_verdict == "EXIT_NOW" and _ev_f is not None and _ev_f > 0:
                                        st.info(
                                            f"**Why EXIT_NOW with EV=\\${_ev_f:+,.0f}?** "
                                            f"EV is positive because a long option has asymmetric payoff: "
                                            f"the {100 - round((_ml_f or 0)*100):.0f}% of paths where the stock moves your way "
                                            f"produce large gains that mathematically offset "
                                            f"the {round((_ml_f or 0)*100):.0f}% of paths losing the full premium. "
                                            f"But {round((_ml_f or 0)*100):.0f}% max-loss probability means "
                                            f"**most paths fail** — this is a low-probability, high-magnitude bet. "
                                            f"EXIT_NOW reflects that the *median path* (P50) is full loss, "
                                            f"not the expected value. "
                                            f"Doctrine says HOLD because the thesis is structurally intact — "
                                            f"the tension is real and requires a judgment call on conviction."
                                        )
                                    elif _mc_h_verdict == "EXIT_NOW" and _ev_f is not None and _ev_f <= 0:
                                        st.warning(
                                            f"**MC EXIT_NOW + negative EV (\\${_ev_f:+,.0f}):** "
                                            f"Both probability (P(max_loss)={round((_ml_f or 0)*100):.0f}%) "
                                            f"and expected value argue against holding. "
                                            f"Doctrine override requires strong conviction in a near-term directional move."
                                        )
                                except Exception:
                                    st.caption(f"🎲 MC hold analysis: {_mc_h_note}")

                    # Panel C: Assignment Risk (income positions only)
                    _strategy_name_mc = str(_doc_row.get("Strategy", _doc_row.get("Strategy_Name", "")) or "") if _doc_row is not None else ""
                    _mc_a_urgency = str(_doc_row.get("MC_Assign_Urgency",  "") or "") if _doc_row is not None else ""
                    _mc_a_p_exp   = _doc_row.get("MC_Assign_P_Expiry") if _doc_row is not None else None
                    _mc_a_p_tch   = _doc_row.get("MC_Assign_P_Touch")  if _doc_row is not None else None
                    _mc_a_note    = str(_doc_row.get("MC_Assign_Note",  "") or "") if _doc_row is not None else ""

                    # Guard: only show for income/short-option positions that can be assigned.
                    # Long options (LONG_PUT, LONG_CALL, LEAPS) cannot be assigned — assignment
                    # risk is only meaningful for short puts (CSP), short calls (CC/BW), and spreads.
                    # MC_Assign_Urgency arrives as float NaN → str() gives "nan" which is truthy —
                    # must explicitly exclude "nan" alongside "" and "SKIP".
                    _valid_assign_urgencies = {"LOW", "MEDIUM", "HIGH", "CRITICAL"}
                    if _mc_a_urgency in _valid_assign_urgencies and not _is_directional_long:
                        _card_metrics["mc_assign"] = {
                            "urgency": _mc_a_urgency,
                            "p_assign": f"{float(_mc_a_p_exp):.0%}" if _mc_a_p_exp and str(_mc_a_p_exp) not in ("nan","None","") else "—",
                            "p_touch": f"{float(_mc_a_p_tch):.0%}" if _mc_a_p_tch and str(_mc_a_p_tch) not in ("nan","None","") else "—",
                        }
                        _a_icons = {"LOW": "🟢", "MEDIUM": "🟡", "HIGH": "🟠", "CRITICAL": "🔴"}
                        _a_icon  = _a_icons.get(_mc_a_urgency, "⚪")
                        with st.expander(
                            f"🎲 MC Assignment Risk — {_a_icon} {_mc_a_urgency}",
                            expanded=(_mc_a_urgency in ("HIGH", "CRITICAL"))
                        ):
                            try:
                                _ac1, _ac2 = st.columns(2)
                                with _ac1:
                                    _p_exp_f = float(_mc_a_p_exp) if _mc_a_p_exp and str(_mc_a_p_exp) not in ("nan","None","") else None
                                    _exp_icon = "🔴" if (_p_exp_f or 0) >= 0.50 else ("🟠" if (_p_exp_f or 0) >= 0.30 else ("🟡" if (_p_exp_f or 0) >= 0.15 else "🟢"))
                                    st.metric(f"{_exp_icon} P(assign @ expiry)", f"{_p_exp_f:.0%}" if _p_exp_f is not None else "—",
                                              help="Probability short strike is ITM at expiry (GBM terminal distribution)")
                                with _ac2:
                                    _p_tch_f = float(_mc_a_p_tch) if _mc_a_p_tch and str(_mc_a_p_tch) not in ("nan","None","") else None
                                    st.metric("P(touch anytime)", f"{_p_tch_f:.0%}" if _p_tch_f is not None else "—",
                                              help="Probability underlying touches short strike at any point before expiry (barrier approximation — more conservative)")
                                if _mc_a_note and not _mc_a_note.startswith("MC_"):
                                    # Strip "execute roll or exit immediately" when doctrine is
                                    # already EXIT — rolling is not on the table. Replace with
                                    # doctrine-consistent language.
                                    _mc_note_display = _mc_a_note
                                    if _doc_action == "EXIT":
                                        import re as _re_mc
                                        _mc_note_display = _re_mc.sub(
                                            r"execute roll or exit immediately",
                                            "see Exit Winner Panel for execution path",
                                            _mc_note_display,
                                            flags=_re_mc.IGNORECASE,
                                        )
                                        _mc_note_display = _re_mc.sub(
                                            r"execute roll or exit",
                                            "see Exit Winner Panel for execution path",
                                            _mc_note_display,
                                            flags=_re_mc.IGNORECASE,
                                        )
                                    st.caption(f"_{_mc_note_display}_")
                            except Exception:
                                st.caption(f"🎲 MC assignment risk: {_mc_a_note}")

                    # Panel D: Weighting Wheel Assessment (CSP positions only)
                    _is_csp = "CSP" in str(entry_structure).upper() or "SHORT_PUT" in str(entry_structure).upper() or "CASH_SECURED" in str(entry_structure).upper()
                    if _is_csp and _doc_row is not None:
                        _wheel_ready     = _doc_row.get("Wheel_Ready")
                        _wheel_note      = str(_doc_row.get("Wheel_Note")  or "")
                        _wheel_basis     = _doc_row.get("Wheel_Basis")
                        _wheel_iv_ok     = _doc_row.get("Wheel_IV_Ok")
                        _wheel_chart_ok  = _doc_row.get("Wheel_Chart_Ok")
                        _wheel_capital_ok = _doc_row.get("Wheel_Capital_Ok")

                        # Normalize booleans (may arrive as bool or string from CSV)
                        def _bool_val(v):
                            if isinstance(v, bool): return v
                            return str(v).strip().lower() in ("true", "1", "yes")

                        _w_ready  = _bool_val(_wheel_ready)
                        _w_iv     = _bool_val(_wheel_iv_ok)
                        _w_chart  = _bool_val(_wheel_chart_ok)
                        _w_cap    = _bool_val(_wheel_capital_ok)

                        # Only render if at least one wheel column was populated (not all defaults)
                        _wheel_populated = any(str(x) not in ("False", "None", "nan", "N/A", "") for x in [_wheel_ready, _wheel_iv_ok, _wheel_chart_ok, _wheel_capital_ok])
                        if _wheel_populated or _w_ready or _wheel_note:
                            _wheel_header_icon = "🟢" if _w_ready else "🟡"
                            _wheel_header_status = "READY" if _w_ready else "NOT READY"
                            with st.expander(
                                f"🎡 Wheel Assessment — {_wheel_header_icon} {_wheel_header_status}",
                                expanded=_w_ready
                            ):
                                try:
                                    st.caption(
                                        "Passarelli Ch.1: When assignment probability is elevated, evaluate whether "
                                        "intentional assignment (CSP → CC cycle) is preferable to rolling/exiting. "
                                        "All 4 conditions must pass for Wheel_Ready=True."
                                    )
                                    _wc1, _wc2, _wc3, _wc4 = st.columns(4)
                                    with _wc1:
                                        _basis_f = float(_wheel_basis) if _wheel_basis and str(_wheel_basis) not in ("nan", "None", "N/A", "") else None
                                        st.metric(
                                            "✅ Basis" if _w_ready else ("✅ Basis" if _basis_f and _basis_f > 0 else "⚠️ Basis"),
                                            f"\\${_basis_f:.2f}" if _basis_f else "—",
                                            help="Effective cost per share if assigned (Net_Cost_Basis > Broker > Strike−Premium). Must be ≥3% below current spot."
                                        )
                                    with _wc2:
                                        _iv_icon = "✅" if _w_iv else "❌"
                                        st.metric(
                                            f"{_iv_icon} IV ≥ 25%",
                                            "Pass" if _w_iv else "Fail",
                                            help="IV_Now ≥ 25% — enough premium to sell a covered call post-assignment (Passarelli Ch.1: covered call entry requires adequate IV)"
                                        )
                                    with _wc3:
                                        _chart_icon = "✅" if _w_chart else "❌"
                                        st.metric(
                                            f"{_chart_icon} Chart OK",
                                            "Pass" if _w_chart else "Fail",
                                            help="TrendIntegrity + PriceStructure not BROKEN — stock worth owning at this price"
                                        )
                                    with _wc4:
                                        _cap_icon = "✅" if _w_cap else "❌"
                                        st.metric(
                                            f"{_cap_icon} Capital",
                                            "Pass" if _w_cap else "Fail",
                                            help="Portfolio delta utilization < 15% — capacity to absorb the shares without over-concentration"
                                        )
                                    if _wheel_note:
                                        _note_color = "🟢" if _w_ready else "🟡"
                                        st.info(f"{_note_color} {_wheel_note}")
                                    if _w_ready:
                                        st.success(
                                            "**Wheel path open.** Assignment at this strike delivers shares at a favorable basis. "
                                            "On assignment: sell covered call at next resistance / IV-implied target. "
                                            "No defensive action required — hold to expiry."
                                        )
                                    else:
                                        st.warning(
                                            "**Wheel path blocked.** Assignment at this price would not be on favorable terms. "
                                            "Maintain standard assignment defense (roll out/down or close)."
                                        )
                                except Exception:
                                    if _wheel_note:
                                        st.caption(f"🎡 Wheel: {_wheel_note}")

                    # Panel E: Scale Plan (long option positions with Action=SCALE_UP)
                    # Surfaces trigger price, add-on size, and sizing rationale.
                    # McMillan Ch.4: Pyramid on Strength — add to winners on pullback at ½ size.
                    _is_long_opt = any(
                        s in str(entry_structure).upper()
                        for s in ("LONG_CALL", "LONG_PUT", "BUY_CALL", "BUY_PUT", "LEAPS")
                    )
                    if _is_long_opt and _doc_row is not None and _doc_action == "SCALE_UP":
                        _scale_trigger = _doc_row.get("Scale_Trigger_Price")
                        _scale_add_c   = _doc_row.get("Scale_Add_Contracts")
                        _scale_trigger_f = float(_scale_trigger) if _scale_trigger and str(_scale_trigger) not in ("nan", "None", "N/A", "") else None
                        _scale_add_i     = int(float(_scale_add_c)) if _scale_add_c and str(_scale_add_c) not in ("nan", "None", "N/A", "") else None
                        _pyr_tier_d = _doc_row.get("Pyramid_Tier")
                        _pyr_tier_i = int(float(_pyr_tier_d)) if _pyr_tier_d and str(_pyr_tier_d) not in ("nan", "None", "N/A", "") else 0
                        _pyr_next = min(3, _pyr_tier_i + 1)
                        _tier_sizing = "1/2-size" if _pyr_tier_i == 0 else "1/4-size" if _pyr_tier_i == 1 else "MAX"
                        with st.expander(f"⬆️ Scale Plan — Tier {_pyr_tier_i}→{_pyr_next} ({_tier_sizing})", expanded=True):
                            try:
                                _urgency_sc = str(_doc_row.get("Urgency", "MEDIUM"))
                                if _urgency_sc == "HIGH":
                                    st.success("🎯 **Pullback level reached — scale now.**")
                                else:
                                    st.info("📋 **Scale conditions met — waiting for pullback entry.**")
                                st.caption(
                                    "McMillan Ch.4: Pyramid on Strength. "
                                    "Murphy: each add smaller than the last (1/2 → 1/4). "
                                    "Natenberg Ch.12: ATR-scaled sizing caps exposure to EWMA-CVaR budget."
                                )
                                _sc1, _sc2, _sc3, _sc4 = st.columns(4)
                                with _sc1:
                                    st.metric(
                                        "Trigger Price",
                                        f"\\${_scale_trigger_f:.2f}" if _scale_trigger_f else "—",
                                        help="Pullback level (EMA9 > SMA20 > Lower BB). Add when UL Last touches this level."
                                    )
                                with _sc2:
                                    st.metric(
                                        "Add-On Contracts",
                                        str(_scale_add_i) if _scale_add_i else "—",
                                        help=f"{_tier_sizing} add, capped by EWMA-CVaR budget and portfolio delta utilization."
                                    )
                                with _sc3:
                                    _delta_util_sc_d = _doc_row.get("Portfolio_Delta_Utilization_Pct")
                                    _delta_util_sc_f = float(_delta_util_sc_d) if _delta_util_sc_d and str(_delta_util_sc_d) not in ("nan", "None", "N/A", "") else None
                                    st.metric(
                                        "δ Utilization",
                                        f"{_delta_util_sc_f:.1f}%" if _delta_util_sc_f is not None else "—",
                                        help="Portfolio delta utilization — must stay below 15% after add (McMillan Ch.3)."
                                    )
                                with _sc4:
                                    st.metric(
                                        "Pyramid Tier",
                                        f"{_pyr_tier_i}/3",
                                        help="0=base, 1=first add, 2=second add, 3=max. Each tier add is smaller."
                                    )
                                if _scale_trigger_f and _scale_trigger_f > 0:
                                    # Show current price distance from trigger
                                    _ul_cur_sc = float(_doc_row.get("UL Last", 0) or 0)
                                    if _ul_cur_sc > 0:
                                        _dist_pct = (_ul_cur_sc - _scale_trigger_f) / _scale_trigger_f
                                        _dist_str = f"UL ${_ul_cur_sc:.2f} is {abs(_dist_pct):.1%} {'above' if _dist_pct > 0 else 'below'} trigger"
                                        if abs(_dist_pct) <= 0.005:
                                            st.success(f"✅ {_dist_str} — **at trigger level, act now.**")
                                        elif abs(_dist_pct) <= 0.02:
                                            st.warning(f"⚠️ {_dist_str} — approaching trigger.")
                                        else:
                                            st.caption(f"📍 {_dist_str}")
                            except Exception:
                                st.caption("⬆️ Scale Plan: data unavailable.")

                    # Panel F: Wheel Continuation (STOCK_ONLY cards when a sibling CSP exists)
                    # The wheel cycle is: CSP → assignment → Covered Call → repeat.
                    # When the user holds stock from a CSP assignment (or a STOCK_ONLY position
                    # alongside an active/settling CSP), surface the CC entry guidance here.
                    # Passarelli Ch.1: "The covered call is the natural continuation of the wheel."
                    _is_stock_card = str(entry_structure).upper() in ("STOCK_ONLY", "STOCK")
                    if _is_stock_card:
                        # Look for sibling CSP rows for the same ticker in the full df
                        _sibling_csps = df[
                            (df["Underlying_Ticker"] == ticker)
                            & (df["Strategy"].str.upper().isin(["CSP", "SHORT_PUT", "CASH_SECURED_PUT"]))
                        ] if "Strategy" in df.columns else pd.DataFrame()

                        _wheel_note_f    = ""
                        _wheel_ready_f   = False
                        _wheel_basis_f   = None
                        _wheel_iv_ok_f   = False
                        _wheel_chart_ok_f = False
                        _wheel_cap_ok_f  = False

                        if not _sibling_csps.empty:
                            # Prefer settled/expired row (DTE=0) for the post-assignment guidance
                            _csp_row_f = _sibling_csps.iloc[0]
                            for _, _cr in _sibling_csps.iterrows():
                                _cr_dte = pd.to_numeric(_cr.get("DTE"), errors="coerce")
                                if pd.notna(_cr_dte) and _cr_dte <= 1:
                                    _csp_row_f = _cr
                                    break
                            def _bv(v):
                                if isinstance(v, bool): return v
                                return str(v).strip().lower() in ("true", "1", "yes")
                            _wheel_note_f     = str(_csp_row_f.get("Wheel_Note", "") or "")
                            _wheel_ready_f    = _bv(_csp_row_f.get("Wheel_Ready"))
                            _wheel_basis_f_raw = _csp_row_f.get("Wheel_Basis")
                            _wheel_basis_f    = float(_wheel_basis_f_raw) if _wheel_basis_f_raw and str(_wheel_basis_f_raw) not in ("nan","None","N/A","") else None
                            _wheel_iv_ok_f    = _bv(_csp_row_f.get("Wheel_IV_Ok"))
                            _wheel_chart_ok_f = _bv(_csp_row_f.get("Wheel_Chart_Ok"))
                            _wheel_cap_ok_f   = _bv(_csp_row_f.get("Wheel_Capital_Ok"))

                        # Also check if the stock row itself has wheel columns populated
                        # (future runs may propagate wheel data to stock leg directly)
                        if _doc_row is not None and not _wheel_note_f:
                            _wheel_note_f     = str(_doc_row.get("Wheel_Note", "") or "")
                            _wheel_ready_f    = _bv(_doc_row.get("Wheel_Ready"))

                        _has_wheel_data = bool(_wheel_note_f or _wheel_ready_f or not _sibling_csps.empty)
                        if _has_wheel_data:
                            _wh_icon   = "🟢" if _wheel_ready_f else "🟡"
                            _wh_status = "READY" if _wheel_ready_f else "REVIEW CONDITIONS"
                            with st.expander(
                                f"🎡 Wheel Continuation — {_wh_icon} {_wh_status}",
                                expanded=_wheel_ready_f,
                            ):
                                st.caption(
                                    "Passarelli Ch.1: The wheel cycle = CSP → assignment → Covered Call → repeat. "
                                    "You hold stock from a CSP assignment (or alongside an active CSP). "
                                    "Evaluate whether to sell a covered call now."
                                )
                                if not _sibling_csps.empty:
                                    _csp_dte_disp = pd.to_numeric(_csp_row_f.get("DTE"), errors="coerce")
                                    _csp_strike_disp = _csp_row_f.get("Strike")
                                    _csp_action_disp = str(_csp_row_f.get("Action", "") or "")
                                    if pd.notna(_csp_dte_disp) and _csp_dte_disp <= 1:
                                        st.info(
                                            f"📋 **CSP settled** (Strike \\${_csp_strike_disp:.2f}, DTE={int(_csp_dte_disp)}) — "
                                            f"awaiting broker confirmation. "
                                            f"Once shares appear in account: sell a covered call to start the CC cycle."
                                        )
                                    else:
                                        st.info(
                                            f"📋 **Active CSP** (Strike \\${_csp_strike_disp:.2f}, "
                                            f"DTE={int(_csp_dte_disp) if pd.notna(_csp_dte_disp) else '?'}, "
                                            f"Doctrine: {_csp_action_disp}) alongside this stock position."
                                        )
                                _wfc1, _wfc2, _wfc3, _wfc4 = st.columns(4)
                                with _wfc1:
                                    st.metric(
                                        "✅ Basis" if _wheel_basis_f else "⚠️ Basis",
                                        f"\\${_wheel_basis_f:.2f}" if _wheel_basis_f else "—",
                                        help="Effective cost per share (strike − premium collected). Sell CC at/above this to protect basis."
                                    )
                                with _wfc2:
                                    st.metric("IV ≥ 25%", "✅ Pass" if _wheel_iv_ok_f else "❌ Fail",
                                              help="IV ≥ 25% means enough premium to sell a CC (Natenberg: <25% IV = poor CC entry)")
                                with _wfc3:
                                    st.metric("Chart OK", "✅ Pass" if _wheel_chart_ok_f else "❌ Fail",
                                              help="Structure not broken — stock worth owning at this price")
                                with _wfc4:
                                    st.metric("Capital", "✅ Pass" if _wheel_cap_ok_f else "❌ Fail",
                                              help="Portfolio delta utilization < 15%")
                                if _wheel_note_f:
                                    st.info(f"{_wh_icon} {_wheel_note_f}")
                                if _wheel_ready_f:
                                    st.success(
                                        "**All conditions met.** Sell a covered call at/above basis "
                                        f"({f'\\${_wheel_basis_f:.2f}' if _wheel_basis_f else 'effective basis'}). "
                                        "Target: first OTM strike above resistance with ≥ 25% IV "
                                        "and 20–45 DTE. "
                                        "Passarelli Ch.1: 'The CC sale completes the wheel entry.'"
                                    )
                                else:
                                    st.warning(
                                        "Not all conditions met — review before selling a covered call. "
                                        "Wait for IV to rise or chart to stabilize."
                                    )

                    # Panel G: CC Opportunity (STOCK_ONLY_IDLE — naked stock, no call written)
                    # Surfaces cc_opportunity_engine output: favorability verdict,
                    # yield-ranked candidates by DTE bucket, or watch conditions.
                    # Use _is_idle_stock_trade (set above) — Entry_Structure is 'STOCK',
                    # not 'STOCK_ONLY_IDLE'; the idle flag lives in Strategy column.
                    if _is_idle_stock_trade and _doc_row is not None:
                        _cc_status  = str(_doc_row.get("CC_Proposal_Status") or "")
                        _cc_verdict = str(_doc_row.get("CC_Proposal_Verdict") or "")
                        _cc_unfav   = str(_doc_row.get("CC_Unfavorable_Reason") or "")
                        _cc_watch   = str(_doc_row.get("CC_Watch_Signal") or "")
                        _cc_regime  = str(_doc_row.get("CC_Regime") or "")
                        _cc_iv_rank = _doc_row.get("CC_IV_Rank")
                        _cc_best_dte= str(_doc_row.get("CC_Best_DTE_Bucket") or "")
                        _cc_best_y  = _doc_row.get("CC_Best_Ann_Yield")
                        _cc_scan_ts = str(_doc_row.get("CC_Scan_TS") or "")

                        _cc_icon = {
                            "FAVORABLE":   "🟢",
                            "UNFAVORABLE": "🔴",
                            "SCAN_MISS":   "⚪",
                            "ERROR":       "⚠️",
                        }.get(_cc_status, "⚪")

                        # Refine header label based on the arbitration verdict embedded in CC_Proposal_Verdict
                        _arb_tag = _cc_verdict.split(" — ")[0].strip() if " — " in _cc_verdict else ""
                        _cc_header_label = {
                            "FAVORABLE":   "OPPORTUNITY DETECTED",
                            "UNFAVORABLE": (
                                "HOLD STOCK — CONVEXITY SUPERIOR" if _arb_tag == "HOLD_STOCK" else
                                "MONITOR — CONDITIONS EVOLVING"   if _arb_tag == "MONITOR"    else
                                "NOT ADVISABLE NOW"
                            ),
                            "SCAN_MISS":   "NO SCAN DATA",
                            "ERROR":       "EVALUATION ERROR",
                        }.get(_cc_status, _cc_status or "PENDING")

                        with st.expander(
                            f"📈 CC Opportunity — {_cc_icon} {_cc_header_label}",
                            expanded=(_cc_status == "FAVORABLE"),
                        ):
                            st.caption(
                                "McMillan Ch.3: idle long stock earns zero theta — "
                                "a covered call converts holding cost into income when IV conditions are right. "
                                "Natenberg Ch.8: sell calls when IV_Rank > 20% and IV > HV."
                            )

                            if _cc_status == "FAVORABLE":
                                # Header metrics
                                _cco1, _cco2, _cco3 = st.columns(3)
                                with _cco1:
                                    _iv_r_disp = f"{float(_cc_iv_rank):.0f}%" if _cc_iv_rank is not None and str(_cc_iv_rank) not in ("nan","None","") else "—"
                                    st.metric("IV_Rank", _iv_r_disp,
                                              help="IV_Rank from latest scan — seller's edge when > 20% (Natenberg Ch.8)")
                                with _cco2:
                                    st.metric("Regime", _cc_regime or "—",
                                              help="Vol regime from scan — High Vol / Elevated = best CC environment")
                                with _cco3:
                                    _best_y_disp = f"{float(_cc_best_y):.1%}" if _cc_best_y is not None and str(_cc_best_y) not in ("nan","None","") else "—"
                                    st.metric(f"Best Yield ({_cc_best_dte})", _best_y_disp,
                                              help="Annualised yield of top-ranked candidate (premium / net basis × 365 / DTE)")

                                st.success(f"✅ {_cc_verdict}")

                                # Candidate table
                                _candidates = []
                                for _ci in range(1, 4):
                                    _craw = _doc_row.get(f"CC_Candidate_{_ci}")
                                    if _craw and str(_craw) not in ("nan", "None", ""):
                                        try:
                                            import json as _json
                                            _cd = _json.loads(str(_craw))
                                            _candidates.append(_cd)
                                        except Exception:
                                            pass

                                if _candidates:
                                    st.markdown("**Ranked Call Candidates** *(from latest scan — verify live prices at open)*")
                                    for _ci, _cd in enumerate(_candidates, 1):
                                        _bucket = _cd.get("bucket", "?")
                                        _strike = _cd.get("strike", 0)
                                        _dte    = _cd.get("dte", 0)
                                        _mid    = _cd.get("mid", 0)
                                        _delta  = _cd.get("delta", 0)
                                        _ay     = _cd.get("ann_yield", 0)
                                        _liq    = _cd.get("liq", "")
                                        _sprd   = _cd.get("spread_pct", 0)
                                        _oi     = _cd.get("oi", 0)
                                        _iv_pct = _cd.get("iv_pct", 0)
                                        _rank_icon = "✅ Best" if _ci == 1 else f"#{_ci}"
                                        st.markdown(
                                            f"**{_rank_icon} · {_bucket}** — "
                                            f"Strike **\\${_strike:.2f}** · {_dte}d · "
                                            f"Mid **\\${_mid:.2f}/share** · "
                                            f"Δ {_delta:.2f} · "
                                            f"Liq {_liq} · "
                                            f"Spread {_sprd:.1f}%"
                                        )
                                        st.caption(
                                            f"Ann. yield: **{_ay:.1%}** · "
                                            f"IV: {_iv_pct:.0f}% · "
                                            f"OI: {_oi:,}"
                                        )
                                        if _ci < len(_candidates):
                                            st.divider()
                                else:
                                    st.info(
                                        "Market favorable but no ranked candidates found in scan. "
                                        "Re-run pipeline during market hours to populate chain data."
                                    )

                                if _cc_scan_ts:
                                    st.caption(f"Scan data: {_cc_scan_ts} — re-run pipeline at open for live prices")

                            elif _cc_status == "UNFAVORABLE":
                                st.warning(f"🔴 {_cc_verdict}")
                                if _cc_unfav:
                                    st.markdown("**Why CC is not advisable:**")
                                    for _reason in _cc_unfav.split(" | "):
                                        st.markdown(f"- {_reason}")
                                if _cc_watch:
                                    st.markdown("**Watch for these signals before entering:**")
                                    for _w in _cc_watch.split(" | "):
                                        st.markdown(f"- 👁 {_w}")
                                if _cc_iv_rank is not None and str(_cc_iv_rank) not in ("nan","None",""):
                                    st.caption(
                                        f"Current: IV_Rank={float(_cc_iv_rank):.0f}%, Regime={_cc_regime}. "
                                        f"No action needed — leave stock unencumbered until conditions improve."
                                    )

                            elif _cc_status == "SCAN_MISS":
                                st.info(
                                    f"⚪ {_cc_verdict}  \n"
                                    f"Run the scan pipeline with {ticker} in the watchlist to generate CC proposals."
                                )

                            elif _cc_status == "ERROR":
                                st.error(f"⚠️ CC evaluation error: {_cc_verdict}")

                            else:
                                st.info(
                                    "CC opportunity assessment pending. "
                                    "Re-run the management engine to evaluate this position."
                                )

                    if _doc_action == "ROLL_WAIT":
                        st.markdown(
                            "**Pre-Execution Checklist** *(ROLL structurally indicated — "
                            "monitor timing/credit before executing):*"
                        )
                    else:
                        st.markdown("**Pre-Execution Checklist:**")
                    # In winner harvest mode (LONG option + ROLL_DOWN), the roll engine
                    # may rank the debit extension (Path C) as #1 due to score weighting.
                    # The checklist should evaluate the Path B (credit harvest) candidate —
                    # that's the recommended execution target, not the debit roll.
                    _checklist_preferred_cand = (
                        _wm_path_b_cand
                        if (_wm_rec_for_roll == "ROLL_DOWN" and _is_directional_long and _wm_path_b_cand)
                        else None
                    )
                    _checklist_items = _build_auto_checklist(
                        doc_row=_doc_row,
                        opt_doc_row=option_row_by_trade.get(tid),
                        hard_stop=_hard_stop_val,
                        spot=_spot_val,
                        opt_legs=opt_legs,
                        db_roll_candidates=_db_roll_candidates.get(tid),
                        is_buy_write=_is_bw,
                        entry_structure=str(entry_structure),
                        net_cost=_eff_cost if _is_bw else None,
                        doctrine_action=_doc_action,
                        preferred_roll_candidate=_checklist_preferred_cand,
                    )
                    _card_metrics["checklist"] = _checklist_items
                    for _chk_icon, _chk_label, _chk_detail in _checklist_items:
                        _ci, _ct = st.columns([0.06, 0.94])
                        _ci.markdown(_chk_icon)
                        # Escape $ in both label and detail — prevents Streamlit LaTeX rendering
                        _chk_label_safe  = str(_chk_label).replace("$", "\\$")
                        _chk_detail_safe = str(_chk_detail).replace("$", "\\$")
                        _ct.markdown(f"**{_chk_label_safe}** — {_chk_detail_safe}")

            # ── Copy Card — fill placeholder with structured text snapshot ──────
            # Rendered for ALL positions (not just roll-eligible), using the
            # _copy_placeholder reserved at top of expander (line ~3488).
            _copy_text = _build_copy_text(
                header, _doc_row, group, stock_legs, opt_legs,
                entry_structure, _card_metrics,
            )
            with _copy_placeholder.expander("📋 Copy Card", expanded=False):
                st.code(_copy_text, language=None)


# ─────────────────────────────────────────────────────────────────────────────
# Section: Portfolio Optimization
# ─────────────────────────────────────────────────────────────────────────────

def _render_portfolio_optimization(df: pd.DataFrame, doctrine_df: pd.DataFrame | None = None):
    """
    Portfolio Optimization Tab — capital clarity across all positions.

    Four panels:
      1. Liquidation Impact  — If I closed X today, what improves?
      2. Vega Top Contributors — Which 3 positions drive vega exposure?
      3. Drawdown Leaders     — Which 3 positions contribute most to loss?
      4. Thesis Alignment     — Which positions no longer fit current signal?

    All computed from existing columns — no new pipeline data required.
    """
    st.subheader("Portfolio Optimization")
    st.caption(
        "Capital clarity across all positions. "
        "Use this to identify where capital is misallocated, "
        "where risk is concentrated, and which positions to prioritize."
    )

    # ── Data prep ─────────────────────────────────────────────────────────────
    # Merge doctrine fields into positions if available
    working = df.copy()
    if doctrine_df is not None and not doctrine_df.empty:
        _doc_cols = [c for c in [
            "TradeID", "Action", "Urgency", "Signal_State", "Scan_Conflict",
            "_Structural_Decay_Regime", "Execution_Readiness",
            "Capital_Bucket", "Thesis_State",
        ] if c in doctrine_df.columns]
        if "TradeID" in doctrine_df.columns and "TradeID" in working.columns:
            working = working.merge(
                doctrine_df[_doc_cols].drop_duplicates("TradeID"),
                on="TradeID", how="left", suffixes=("", "_doc"),
            )

    # Numeric helpers
    def _num(col, default=0.0):
        if col not in working.columns:
            return pd.Series(default, index=working.index)
        return pd.to_numeric(working[col], errors="coerce").fillna(default)

    # Aggregate to TradeID level (sum option + stock legs)
    _group_cols = ["TradeID", "Underlying_Ticker"]
    # Pull strategy from first non-null row per TradeID
    if "Strategy" in working.columns:
        _strat_map = (
            working[working["Strategy"].notna()]
            .groupby("TradeID")["Strategy"]
            .first()
        )
    else:
        _strat_map = pd.Series(dtype=str)

    # Greek and P/L aggregation
    working["_Vega_signed"]  = _num("Vega")  * _num("Quantity", 1)
    working["_Delta_signed"] = _num("Delta") * _num("Quantity", 1)
    working["_Theta_signed"] = _num("Theta") * _num("Quantity", 1)
    working["_GL"]           = _num("$ Total G/L")
    working["_Capital"]      = _num("Current Value").abs()

    # Per-trade doctrine fields (first non-null per TradeID)
    _doc_field_map = {}
    for _dc in ["Action", "Urgency", "Signal_State", "Scan_Conflict",
                "_Structural_Decay_Regime", "Execution_Readiness", "Capital_Bucket"]:
        if _dc in working.columns:
            _doc_field_map[_dc] = working.groupby("TradeID")[_dc].first()

    trades = (
        working.groupby(["TradeID", "Underlying_Ticker"], as_index=False)
        .agg(
            Vega_Net   =("_Vega_signed",  "sum"),
            Delta_Net  =("_Delta_signed", "sum"),
            Theta_Net  =("_Theta_signed", "sum"),
            GL_Total   =("_GL",           "sum"),
            Capital    =("_Capital",      "sum"),
        )
    )
    trades["Strategy"]    = trades["TradeID"].map(_strat_map).fillna("—")
    for _dc, _series in _doc_field_map.items():
        trades[_dc] = trades["TradeID"].map(_series).fillna("")

    # Portfolio-level totals
    port_vega  = trades["Vega_Net"].sum()
    port_gl    = trades["GL_Total"].sum()
    port_cap   = trades["Capital"].sum()
    port_theta = trades["Theta_Net"].sum()

    n_trades = len(trades)
    if n_trades == 0:
        st.info("No position data available for optimization analysis.")
        return

    # ── Portfolio health summary bar ─────────────────────────────────────────
    _c1, _c2, _c3, _c4 = st.columns(4)
    _c1.metric("Portfolio P/L", f"${port_gl:+,.0f}")
    _c2.metric("Net Vega", f"{port_vega:+,.1f}")
    _c3.metric("Net Theta/day", f"${port_theta * 100:+,.1f}")
    _c4.metric("Positions", n_trades)
    st.divider()

    # ── Panel 1: Liquidation Impact ───────────────────────────────────────────
    st.markdown("### 1️⃣ Liquidation Impact — *If I closed X, what improves?*")
    st.caption(
        "Ranks positions by how much closing them would improve portfolio health. "
        "Improvement score = vega freed (% of port) + thesis misalignment penalty + "
        "loss-lock benefit. Higher = stronger case for closing."
    )

    liq_rows = []
    for _, tr in trades.iterrows():
        ticker  = tr["Underlying_Ticker"]
        gl      = tr["GL_Total"]
        vega    = tr["Vega_Net"]
        delta   = tr["Delta_Net"]
        theta   = tr["Theta_Net"]
        cap     = tr["Capital"]
        action  = str(tr.get("Action", "") or "")
        signal  = str(tr.get("Signal_State", "") or "")
        decay   = str(tr.get("_Structural_Decay_Regime", "") or "")
        scan_cf = str(tr.get("Scan_Conflict", "") or "")
        er      = str(tr.get("Execution_Readiness", "") or "")

        # Vega concentration relief (abs % of portfolio freed)
        vega_relief_pct = abs(vega) / max(abs(port_vega), 1) * 100

        # Thesis misalignment flag
        misaligned = (
            signal in ("VIOLATED", "DEGRADED")
            or decay == "STRUCTURAL_DECAY"
            or scan_cf in ("BEARISH", "BULLISH")   # any conflict
            or action in ("EXIT",)
        )

        # Loss-lock benefit: closing a losing trade removes ongoing bleed
        # Give full score if doctrine says EXIT; partial if losing AND misaligned
        loss_score = 0.0
        if gl < 0:
            if action == "EXIT":
                loss_score = 1.0
            elif misaligned:
                loss_score = 0.6
            else:
                loss_score = 0.2   # losing but thesis intact — low urgency to close

        # Capital efficiency: theta return on capital
        theta_roic = (abs(theta) * 100 * 252 / max(cap, 1) * 100) if cap > 0 else 0  # annualised %

        # Composite improvement score (0–100)
        improvement_score = min(100, round(
            vega_relief_pct * 0.35           # 35%: vega concentration relief
            + (30 if misaligned else 0)      # 30%: thesis misalignment
            + loss_score * 20                # 20%: loss-lock benefit
            + (15 if action == "EXIT" else (10 if action == "ROLL" else 0))  # 15%: doctrine urgency
        , 1))

        # What changes label
        _changes = []
        if abs(vega) > 0:
            _sign = "↓" if vega > 0 else "↑"
            _changes.append(f"Vega {_sign}{abs(vega):.0f} ({vega_relief_pct:.0f}% of port)")
        if abs(delta) > 0:
            _changes.append(f"Delta {'-' if delta > 0 else '+'}{abs(delta):.2f}")
        if abs(theta) > 0:
            _changes.append(f"Theta {'loses' if theta > 0 else 'frees'} ${abs(theta)*100:.1f}/day")
        if gl < -100:
            _changes.append(f"Locks ${gl:,.0f} loss")
        elif gl > 100:
            _changes.append(f"Locks ${gl:+,.0f} gain")
        if misaligned:
            _changes.append("🔴 removes misaligned position")

        _action_badge = {
            "EXIT":         "🔴 EXIT",
            "ROLL":         "🟡 ROLL",
            "ROLL_WAIT":    "⏸ ROLL_WAIT",
            "HOLD":         "🟢 HOLD",
            "TRIM":         "⚠️ TRIM",
        }.get(action, action or "—")

        liq_rows.append({
            "Score":        improvement_score,
            "Ticker":       ticker,
            "Strategy":     tr["Strategy"],
            "Doctrine":     _action_badge,
            "P/L":          f"${gl:+,.0f}",
            "Vega":         f"{vega:+.1f}",
            "If Closed →":  " · ".join(_changes) if _changes else "minimal portfolio impact",
        })

    liq_df = pd.DataFrame(liq_rows).sort_values("Score", ascending=False)
    _top3_liq = liq_df.head(3)

    # Highlight top 3
    st.markdown("**Top candidates for closure (highest improvement score first):**")
    for _rank, (_, _row) in enumerate(_top3_liq.iterrows(), 1):
        _rank_icon = ["🥇", "🥈", "🥉"][_rank - 1]
        with st.expander(
            f"{_rank_icon} **{_row['Ticker']}** ({_row['Strategy']}) — "
            f"Doctrine: {_row['Doctrine']} · P/L: {_row['P/L']} · Score: {_row['Score']:.0f}/100",
            expanded=(_rank == 1),
        ):
            _detail_safe = str(_row["If Closed →"]).replace("$", "\\$")
            st.markdown(f"**If closed today:** {_detail_safe}")

    st.markdown("**All positions ranked:**")
    _liq_display = liq_df[["Score", "Ticker", "Strategy", "Doctrine", "P/L", "Vega", "If Closed →"]].copy()
    _liq_display["Score"] = _liq_display["Score"].apply(lambda x: f"{x:.0f}")
    st.dataframe(_liq_display, hide_index=True, width="stretch")

    st.divider()

    # ── Panel 2: Vega Concentration ───────────────────────────────────────────
    st.markdown("### 2️⃣ Vega Concentration — *Which positions drive vega risk?*")
    st.caption(
        "Positions with >20% of portfolio vega are concentration risks. "
        "A single earnings event or vol regime shift in that ticker moves your entire book."
    )

    # Compute vega contribution %
    vega_ranked = trades.copy()
    vega_ranked["Vega_Abs"]   = vega_ranked["Vega_Net"].abs()
    vega_ranked["Vega_Pct"]   = (vega_ranked["Vega_Abs"] / max(vega_ranked["Vega_Abs"].sum(), 0.001) * 100).round(1)
    vega_ranked["Vega_Signed_Pct"] = (vega_ranked["Vega_Net"] / max(vega_ranked["Vega_Abs"].sum(), 0.001) * 100).round(1)
    vega_ranked = vega_ranked.sort_values("Vega_Abs", ascending=False)

    _top3_vega = vega_ranked.head(3)
    _vega_concentrated = vega_ranked[vega_ranked["Vega_Pct"] > 20]

    if not _vega_concentrated.empty:
        _vega_tickers = ", ".join(_vega_concentrated["Underlying_Ticker"].tolist())
        st.error(
            f"⚠️ **Vega concentration risk:** {_vega_tickers} each drive >20% of portfolio vega. "
            "A vol event in any of these dominates the book. (Natenberg Ch.19)"
        )
    else:
        st.success("✅ No single position exceeds 20% of portfolio vega — well distributed.")

    st.markdown("**Top 3 vega contributors:**")
    _v_cols = st.columns(3)
    for _i, (_, _vr) in enumerate(_top3_vega.iterrows()):
        _conc_flag = " 🔴" if _vr["Vega_Pct"] > 20 else (" ⚠️" if _vr["Vega_Pct"] > 10 else "")
        _v_cols[_i].metric(
            label=f"{_vr['Underlying_Ticker']} ({_vr['Strategy']})",
            value=f"{_vr['Vega_Net']:+.1f}",
            delta=f"{_vr['Vega_Pct']:.1f}% of port{_conc_flag}",
        )

    _vega_display = vega_ranked[["Underlying_Ticker", "Strategy", "Vega_Net", "Vega_Pct"]].copy()
    _vega_display.columns = ["Ticker", "Strategy", "Net Vega", "% of Port Vega"]
    _vega_display["Net Vega"]        = _vega_display["Net Vega"].apply(lambda x: f"{x:+.2f}")
    _vega_display["% of Port Vega"]  = _vega_display["% of Port Vega"].apply(lambda x: f"{x:.1f}%")
    st.dataframe(_vega_display, hide_index=True, width="stretch")

    st.divider()

    # ── Panel 3: Drawdown Leaders ─────────────────────────────────────────────
    st.markdown("### 3️⃣ Drawdown Leaders — *Which positions contribute most to loss?*")
    st.caption(
        "Sorted by total P/L. The key question is not just *who's losing* "
        "but *who's losing with a broken thesis and no exit plan.*"
    )

    dd_ranked = trades.sort_values("GL_Total")
    _losers   = dd_ranked[dd_ranked["GL_Total"] < 0]
    _top3_dd  = dd_ranked.head(3)

    if _losers.empty:
        st.success("✅ No positions in a loss — portfolio fully profitable.")
    else:
        _total_loss = _losers["GL_Total"].sum()
        st.warning(
            f"**{len(_losers)} position(s) in drawdown** — "
            f"total unrealized loss: **\\${abs(_total_loss):,.0f}**"
        )

    st.markdown("**Worst 3 positions by P/L:**")
    _dd_cols = st.columns(3)
    for _i, (_, _dr) in enumerate(_top3_dd.iterrows()):
        _action  = str(_dr.get("Action", "") or "")
        _signal  = str(_dr.get("Signal_State", "") or "")
        _dd_flag = ""
        if _action == "EXIT" or _signal == "VIOLATED":
            _dd_flag = " 🔴 thesis broken"
        elif _action == "ROLL":
            _dd_flag = " 🟡 roll indicated"
        _dd_cols[_i].metric(
            label=f"{_dr['Underlying_Ticker']} ({_dr['Strategy']})",
            value=f"${_dr['GL_Total']:+,.0f}",
            delta=_action + _dd_flag if _action else "—",
            delta_color="inverse" if _dr["GL_Total"] < 0 else "normal",
        )

    _dd_display = dd_ranked[["Underlying_Ticker", "Strategy", "GL_Total", "Action", "Signal_State"]].copy()
    _dd_display.columns = ["Ticker", "Strategy", "P/L", "Doctrine Action", "Signal State"]
    _dd_display["P/L"] = _dd_display["P/L"].apply(lambda x: f"${x:+,.0f}")
    # Flag rows where doctrine says act but loss is ongoing
    def _dd_flag_row(row):
        if row["Doctrine Action"] in ("EXIT", "ROLL") and "$-" in row["P/L"]:
            return "🔴 Act required"
        if row["Signal State"] in ("VIOLATED", "DEGRADED"):
            return "⚠️ Thesis degraded"
        return "—"
    _dd_display["Priority"] = _dd_display.apply(_dd_flag_row, axis=1)
    st.dataframe(_dd_display, hide_index=True, width="stretch")

    st.divider()

    # ── Panel 4: Thesis Alignment ─────────────────────────────────────────────
    st.markdown("### 4️⃣ Thesis Alignment — *Which positions no longer fit current signal?*")
    st.caption(
        "A position is misaligned when: scan signal conflicts with position direction, "
        "Signal_State is VIOLATED/DEGRADED, or structural decay is active. "
        "Holding a misaligned position is not management — it's hope."
    )

    alignment_rows = []
    for _, tr in trades.iterrows():
        ticker   = tr["Underlying_Ticker"]
        strategy = tr["Strategy"]
        action   = str(tr.get("Action", "")                  or "")
        signal   = str(tr.get("Signal_State", "")             or "")
        decay    = str(tr.get("_Structural_Decay_Regime", "") or "")
        scan_cf  = str(tr.get("Scan_Conflict", "")            or "")
        er       = str(tr.get("Execution_Readiness", "")      or "")
        urgency  = str(tr.get("Urgency", "")                  or "")

        # Build misalignment reasons
        reasons = []
        if signal == "VIOLATED":
            reasons.append("Signal VIOLATED — structural thesis broken")
        elif signal == "DEGRADED":
            reasons.append("Signal DEGRADED — thesis weakening")
        if decay == "STRUCTURAL_DECAY":
            reasons.append("Structural decay active — long vol bleeding in chop")
        if scan_cf and scan_cf not in ("", "NONE", "NAN", "NEUTRAL"):
            _direction = "bearish" if scan_cf == "BEARISH" else "bullish"
            reasons.append(f"Scan now {_direction} — position direction conflicts with current signal")
        if action == "EXIT":
            reasons.append("Doctrine says EXIT — thesis not just misaligned, it's broken")
        elif action == "ROLL" and urgency in ("HIGH", "CRITICAL"):
            reasons.append(f"Doctrine ROLL + {urgency} urgency — action needed")

        if reasons:
            _alignment = "🔴 MISALIGNED"
        elif signal in ("VALID", "") and action in ("HOLD", "ROLL_WAIT", "") and not decay and not scan_cf:
            _alignment = "✅ ALIGNED"
        else:
            _alignment = "⚠️ MONITOR"

        alignment_rows.append({
            "Alignment":    _alignment,
            "Ticker":       ticker,
            "Strategy":     strategy,
            "Doctrine":     action or "—",
            "Signal":       signal or "—",
            "Scan Conflict": scan_cf or "—",
            "Reasons":      " · ".join(reasons) if reasons else "No misalignment detected",
        })

    alignment_df = pd.DataFrame(alignment_rows)
    # Sort: misaligned first, then monitor, then aligned
    _sort_order = {"🔴 MISALIGNED": 0, "⚠️ MONITOR": 1, "✅ ALIGNED": 2}
    alignment_df["_sort"] = alignment_df["Alignment"].map(_sort_order).fillna(3)
    alignment_df = alignment_df.sort_values("_sort").drop(columns=["_sort"])

    _misaligned = alignment_df[alignment_df["Alignment"] == "🔴 MISALIGNED"]
    _monitored  = alignment_df[alignment_df["Alignment"] == "⚠️ MONITOR"]
    _aligned    = alignment_df[alignment_df["Alignment"] == "✅ ALIGNED"]

    if not _misaligned.empty:
        st.error(
            f"**{len(_misaligned)} position(s) misaligned with current thesis:** "
            f"{', '.join(_misaligned['Ticker'].tolist())} — review doctrine action immediately."
        )
    if not _monitored.empty:
        st.warning(
            f"**{len(_monitored)} position(s) under monitoring:** "
            f"{', '.join(_monitored['Ticker'].tolist())} — no immediate action, but watch closely."
        )
    if _misaligned.empty and _monitored.empty:
        st.success("✅ All positions aligned with current thesis — no conflicts detected.")

    # Detail expanders for misaligned
    for _, _ma in _misaligned.iterrows():
        with st.expander(f"🔴 **{_ma['Ticker']}** ({_ma['Strategy']}) — {_ma['Doctrine']}", expanded=True):
            for _reason in _ma["Reasons"].split(" · "):
                st.markdown(f"- {_reason}")

    st.markdown("**Full alignment table:**")
    _align_display = alignment_df[["Alignment", "Ticker", "Strategy", "Doctrine", "Signal", "Scan Conflict", "Reasons"]].copy()
    st.dataframe(_align_display, hide_index=True, width="stretch")


# ─────────────────────────────────────────────────────────────────────────────
# Section: Idle Positions Tab
# Stock-only positions (no option leg written). Two distinct cases:
#   HEALTHY  — loss < 10%, thesis intact → CC opportunity evaluation
#   RECOVERY — loss 10-35%, thesis intact → recovery path + conditional CC
#   CRITICAL — loss > 35% OR thesis DEGRADED/BROKEN → cut vs. hold + CC constraint
# ─────────────────────────────────────────────────────────────────────────────

def _render_idle_positions_tab(df: pd.DataFrame, doctrine_df: pd.DataFrame | None = None):
    """
    Dedicated view for stock-only (idle) positions.
    Each card: triage state → hard stop → recovery path → CC viability.
    """
    # Identify idle rows
    # A stock is "covered" (not idle) only when a SHORT CALL is written against it.
    # A CSP is backed by CASH, not the stock — holding stock alongside a CSP does not
    # make the stock "covered." Long puts, long calls, and CSPs do NOT remove idle status.
    if "AssetType" not in df.columns:
        st.info("No position data available.")
        return

    # Tickers that have a short call leg (Strategy = BUY_WRITE or COVERED_CALL, or
    # Call/Put = C with negative quantity) — these are the only ones that cover the stock.
    _call_covered_tickers: set = set()
    _opt_rows = df[df["AssetType"] == "OPTION"]
    for _, _or in _opt_rows.iterrows():
        _strat   = str(_or.get("Strategy") or "").upper()
        _cp      = str(_or.get("Call/Put") or "").upper()
        _qty     = float(_or.get("Quantity") or 0)
        _ul      = str(_or.get("Underlying_Ticker") or "")
        # Short call: BUY_WRITE/COVERED_CALL strategy OR explicit call with negative qty
        if _strat in ("BUY_WRITE", "COVERED_CALL") or (_cp in ("C", "CALL") and _qty < 0):
            _call_covered_tickers.add(_ul)

    idle_df = df[
        (df["AssetType"] == "STOCK") &
        ~df["Underlying_Ticker"].isin(_call_covered_tickers)
    ].copy()

    if idle_df.empty:
        st.success("✅ No idle stock positions — all held shares have an active call written against them.")
        return

    # Compute per-share basis from total Basis / Quantity
    def _per_share_basis(row):
        try:
            import numpy as _np
            q_raw = row.get("Quantity")
            b_raw = row.get("Basis")
            q = float(q_raw) if q_raw is not None else 0.0
            b = float(b_raw) if b_raw is not None else 0.0
            # float("nan") is truthy so `or 0` does NOT protect against NaN strings/values
            if _np.isnan(q) or _np.isnan(b) or q == 0:
                return None
            return abs(b / q)
        except Exception:
            return None

    idle_df["_basis_per_share"] = idle_df.apply(_per_share_basis, axis=1)
    idle_df["_quantity_f"] = pd.to_numeric(idle_df["Quantity"], errors="coerce").fillna(0)

    # Split: CC-eligible (≥100 shares) vs sub-contract holdings
    cc_eligible_df = idle_df[idle_df["_quantity_f"] >= 100].copy()
    sub_contract_df = idle_df[idle_df["_quantity_f"] < 100].copy()

    # Build doctrine lookup
    doctrine_by_ticker = {}
    if doctrine_df is not None and not doctrine_df.empty and "Underlying_Ticker" in doctrine_df.columns:
        for _, drow in doctrine_df.iterrows():
            t = str(drow.get("Underlying_Ticker", "") or "")
            if not t:
                continue
            # Prefer STOCK rows over OPTION rows for CC columns.
            # The CC engine only writes CC_Proposal_* onto stock rows; an OPTION row
            # winning first-match means CC_Proposal_Status = NaN even when the stock row
            # has a valid verdict.
            existing = doctrine_by_ticker.get(t)
            if existing is None:
                doctrine_by_ticker[t] = drow.to_dict()
            elif str(existing.get("AssetType", "")) == "OPTION" and str(drow.get("AssetType", "")) == "STOCK":
                # Upgrade to the STOCK row — it carries the CC proposal columns
                doctrine_by_ticker[t] = drow.to_dict()

    # Summary header
    total_gl = pd.to_numeric(idle_df["$ Total G/L"], errors="coerce").sum()
    total_basis = pd.to_numeric(idle_df["Basis"], errors="coerce").abs().sum()
    n = len(idle_df)
    n_cc = len(cc_eligible_df)
    n_sub = len(sub_contract_df)
    gl_color = "🔴" if total_gl < 0 else "🟢"
    st.markdown(
        f"**{n} idle position{'s' if n != 1 else ''}** · "
        f"CC-eligible: **{n_cc}** · Sub-contract: **{n_sub}** · "
        f"Total basis **${total_basis:,.0f}** · "
        f"{gl_color} Unrealized P&L **${total_gl:+,.0f}** · "
        f"Zero theta working on any of these."
    )
    st.caption(
        "McMillan Ch.3: idle long stock earns zero theta. "
        "CC-eligible (≥100 shares): triaged HEALTHY/RECOVERY/CRITICAL. "
        "Sub-contract (<100 shares): CC not available — hold or exit decision only.  \n"
        "**Note:** Only a **short call** (BUY_WRITE/CC) removes a stock from this tab. "
        "CSP, long puts, and long calls do NOT cover the stock — those appear here too."
    )
    st.divider()

    # ── Sub-contract holdings section (collapsed by default — low actionability) ──
    if not sub_contract_df.empty:
        _sub_gl = pd.to_numeric(sub_contract_df["$ Total G/L"], errors="coerce").sum()
        _sub_icon = "🔴" if _sub_gl < 0 else "🟢"
        with st.expander(
            f"🔹 Sub-Contract Holdings — {n_sub} positions · {_sub_icon} ${_sub_gl:+,.0f} "
            f"(< 100 shares — CC not available)",
            expanded=False,
        ):
            st.caption(
                "These positions do not have enough shares for a standard options contract (100 shares minimum). "
                "Covered calls are **not available**. Decision: **hold or exit?**"
            )
            for _, row in sub_contract_df.sort_values("$ Total G/L").iterrows():
                ticker     = str(row.get("Underlying_Ticker", "?"))
                last       = float(row.get("Last") or 0)
                basis_ps   = row.get("_basis_per_share")
                quantity   = float(row.get("_quantity_f") or 0)
                total_gl_p = float(row.get("$ Total G/L") or 0)
                drift      = ((last - basis_ps) / basis_ps) if basis_ps else 0.0
                drift_str  = f"{drift:+.1%}" if basis_ps else "—"
                gl_icon    = "🔴" if total_gl_p < 0 else "🟢"

                st.markdown(
                    f"{gl_icon} **{ticker}** — {quantity:.0f} shares · {drift_str} · ${total_gl_p:+,.0f}"
                )
                sc1, sc2, sc3, sc4 = st.columns(4)
                sc1.metric("Last", f"${last:.2f}")
                sc2.metric("Basis/Share", f"${basis_ps:.2f}" if basis_ps else "—")
                sc3.metric("Shares", f"{quantity:.0f}")
                sc4.metric("Unrealized P&L", f"${total_gl_p:+,.0f}")

                thesis = str(
                    row.get("Thesis_State")
                    or doctrine_by_ticker.get(ticker, {}).get("Thesis_State", "UNKNOWN")
                    or "UNKNOWN"
                ).upper()
                if thesis in ("DEGRADED", "BROKEN"):
                    st.error(f"⚠️ Thesis: {thesis} — strong case for exiting this fractional position.")
                st.divider()

    if cc_eligible_df.empty:
        st.info("No CC-eligible idle positions (≥ 100 shares).")
        return

    st.markdown("### 📋 CC-Eligible Idle Positions (≥ 100 shares)")

    # Sort: CRITICAL first, then RECOVERY, then HEALTHY; within each group by $ loss
    def _triage(row):
        try:
            import numpy as _np
            last  = float(row.get("Last") or 0)
            basis = row.get("_basis_per_share")
            if basis is None or (isinstance(basis, float) and _np.isnan(basis)) or basis == 0:
                return "UNKNOWN", 0.0
            drift = (last - basis) / basis
            thesis = str(doctrine_by_ticker.get(str(row.get("Underlying_Ticker", "")), {}).get("Thesis_State", "INTACT") or "INTACT").upper()
            if drift < -0.35 or thesis in ("DEGRADED", "BROKEN"):
                return "CRITICAL", drift
            elif drift < -0.10:
                return "RECOVERY", drift
            else:
                return "HEALTHY", drift
        except Exception:
            return "UNKNOWN", 0.0

    cc_eligible_df["_triage_state"], cc_eligible_df["_drift"] = zip(*cc_eligible_df.apply(_triage, axis=1))
    _order = {"CRITICAL": 0, "RECOVERY": 1, "HEALTHY": 2, "UNKNOWN": 3}
    cc_eligible_df["_sort"] = cc_eligible_df["_triage_state"].map(_order)
    cc_eligible_df = cc_eligible_df.sort_values(["_sort", "$ Total G/L"]).reset_index(drop=True)

    # Build per-ticker option leg context — used to warn when a paired option
    # has an active EXIT/ROLL doctrine that should be resolved before stock decisions.
    _option_legs_by_ticker: dict = {}
    if "AssetType" in df.columns and "Underlying_Ticker" in df.columns:
        for _, _or in df[df["AssetType"] == "OPTION"].iterrows():
            _ul = str(_or.get("Underlying_Ticker") or "")
            if _ul:
                _option_legs_by_ticker.setdefault(_ul, []).append(_or.to_dict())

    for _, row in cc_eligible_df.iterrows():
        ticker      = str(row.get("Underlying_Ticker", "?"))
        last        = float(row.get("Last") or 0)
        basis_ps    = row.get("_basis_per_share")
        quantity    = float(row.get("_quantity_f") or row.get("Quantity") or 0)
        total_gl_pos = float(row.get("$ Total G/L") or 0)
        triage      = row.get("_triage_state", "UNKNOWN")
        drift       = row.get("_drift", 0.0)
        hv          = float(row.get("HV_20D") or 0)
        iv_entry    = float(row.get("IV_Entry") or 0)
        iv_30d      = float(row.get("IV_30D") or 0) if str(row.get("IV_30D","")) not in ("nan","None","") else 0.0
        iv_surface  = str(row.get("iv_surface_shape") or "")
        hv_pct      = float(row.get("hv_20d_percentile") or 0) if str(row.get("hv_20d_percentile","")) not in ("nan","None","") else 0.0
        thesis      = str(row.get("Thesis_State") or doctrine_by_ticker.get(ticker, {}).get("Thesis_State", "UNKNOWN") or "UNKNOWN").upper()
        doc_row     = doctrine_by_ticker.get(ticker, {})

        _triage_icon = {"CRITICAL": "🔴", "RECOVERY": "🟠", "HEALTHY": "🟢", "UNKNOWN": "⚪"}.get(triage, "⚪")
        import math as _math
        _drift_str   = f"{drift:+.1%}" if drift != 0 and not _math.isnan(drift) else "—"
        _account_str = str(row.get("Account") or "").strip()
        _acct_label  = f" · {_account_str}" if _account_str else ""

        # CC Recovery badge from engine
        _cc_rec_mode = str(row.get("CC_Recovery_Mode") or "")
        _cc_rec_badge = ""
        if _cc_rec_mode == "STRUCTURAL_DAMAGE":
            _cc_rec_badge = " · CC:STRUCTURAL_DAMAGE"
        elif _cc_rec_mode == "DEEP_RECOVERY":
            _cc_rec_badge = " · CC:DEEP_RECOVERY"
        elif _cc_rec_mode == "RECOVERY":
            _cc_rec_badge = " · CC:RECOVERY"

        with st.expander(
            f"{_triage_icon} **{ticker}**{_acct_label} — {triage} · {_drift_str} from basis"
            + (f" · ${total_gl_pos:+,.0f}" if total_gl_pos else "")
            + _cc_rec_badge,
            expanded=(triage in ("CRITICAL", "RECOVERY")),
        ):
            # ── Row 1: key metrics ────────────────────────────────────────
            mc1, mc2, mc3, mc4, mc5 = st.columns(5)
            mc1.metric("Last", f"${last:.2f}")
            import numpy as _np_mc
            _basis_ps_valid = basis_ps is not None and not (isinstance(basis_ps, float) and _np_mc.isnan(basis_ps)) and basis_ps > 0
            mc2.metric("Basis/Share", f"${basis_ps:.2f}" if _basis_ps_valid else "—")
            _ldr_total_lots = doc_row.get("CC_Ladder_Total_Lots")
            _agg_shares = int(float(_ldr_total_lots) * 100) if _ldr_total_lots and str(_ldr_total_lots) not in ("nan", "None", "") and float(_ldr_total_lots or 0) > 0 else 0
            if _agg_shares > 0 and _agg_shares != int(quantity):
                mc3.metric("Shares", f"{quantity:,.0f}", help=f"Aggregate across rows: {_agg_shares:,} ({_agg_shares // 100} lots)")
            else:
                mc3.metric("Shares", f"{quantity:,.0f}")
            mc4.metric("Drift", _drift_str, delta_color="inverse")
            mc5.metric("HV-20D", f"{hv*100:.0f}%" if hv else "—")

            # ── Triage verdict ────────────────────────────────────────────
            if triage == "CRITICAL":
                st.error(
                    f"**🔴 CRITICAL** — {_drift_str} from cost basis"
                    + (" · Thesis: " + thesis if thesis not in ("INTACT", "UNKNOWN") else "")
                    + ".  \nPriority question: **is the thesis still intact?** "
                    "If not, selling a covered call caps a potential recovery bounce and locks in the loss. "
                    "Cut or hold — do NOT write calls until thesis is confirmed. "
                    "(McMillan Ch.3: don't sell calls on broken positions hoping premium saves you.)"
                )
            elif triage == "RECOVERY":
                st.warning(
                    f"**🟠 RECOVERY MODE** — {_drift_str} from cost basis.  \n"
                    "Covered calls viable **only if** strike is well above current price — "
                    "don't cap a recovery bounce for thin premium. "
                    "Strike target: ≥10% OTM from current price, not from cost basis."
                )
            elif triage == "UNKNOWN":
                st.info(
                    f"**⚪ BASIS UNKNOWN** — cost basis not available for this lot.  \n"
                    "CC viability analysis requires basis data. "
                    "Enter the cost basis for this position to enable full analysis. "
                    "IV and regime analysis still available below."
                )
            else:
                st.success(
                    f"**🟢 HEALTHY** — {_drift_str} from cost basis.  \n"
                    "Standard covered call opportunity — IV and regime determine timing."
                )

            # ── Paired option leg warning ─────────────────────────────────
            # If this stock has active option legs (CSP, long options, etc.),
            # surface their doctrine action — the stock decision is subordinate to
            # resolving the option-side first.
            _paired_legs = _option_legs_by_ticker.get(ticker, [])
            if _paired_legs:
                for _leg in _paired_legs:
                    _leg_sym    = str(_leg.get("Symbol") or _leg.get("Ticker") or "")
                    _leg_strat  = str(_leg.get("Strategy") or "")
                    _leg_action = str(_leg.get("Action") or "")
                    _leg_urg    = str(_leg.get("Urgency") or "")
                    _leg_rat    = str(_leg.get("Rationale") or "")[:120]
                    if _leg_action in ("EXIT", "ROLL"):
                        _urg_icon = "🔴" if _leg_urg == "HIGH" else "🟠"
                        st.error(
                            f"{_urg_icon} **Active option leg requires action first:** "
                            f"{_leg_sym} ({_leg_strat}) → **{_leg_action} {_leg_urg}**  \n"
                            f"{_leg_rat}  \n"
                            "Resolve the option position before making stock-level decisions."
                        )
                    elif _leg_action == "HOLD":
                        st.info(
                            f"ℹ️ **Paired option leg:** {_leg_sym} ({_leg_strat}) → HOLD.  \n"
                            "Stock shares are paired with an active option position."
                        )

            # ── Hard stop & recovery math ─────────────────────────────────
            if _basis_ps_valid and last:
                _stop_mult  = 0.70 if triage == "CRITICAL" else 0.80
                _hard_stop  = basis_ps * _stop_mult
                _gap        = max(0.0, basis_ps - last)   # gap to break even
                _cushion    = last - _hard_stop

                st.markdown("**📊 Recovery Path Analysis**" if _gap > 0 else "**📊 Position Risk Metrics**")
                rp1, rp2, rp3, rp4 = st.columns(4)
                rp1.metric("Hard Stop", f"${_hard_stop:.2f}",
                           help=f"{'70%' if triage == 'CRITICAL' else '80%'} of cost basis — McMillan structural stop")
                rp2.metric("Stop Cushion", f"${_cushion:.2f}" if _cushion > 0 else "❌ BREACHED",
                           delta_color="normal" if _cushion > 0 else "inverse")
                rp3.metric("Gap to Breakeven", f"${_gap:.2f}/sh" if _gap > 0 else "✅ Above basis")
                rp4.metric("Total Gap", f"${_gap * quantity:,.0f}" if _gap > 0 else "—")

                # Pre-resolve CC status for recovery suppression
                _cc_status_early = str(doc_row.get("CC_Proposal_Status") or "SCAN_MISS")
                _has_ladder_early = (
                    doc_row.get("CC_Ladder_Eligible")
                    and str(doc_row.get("CC_Ladder_Monthly_Est") or "") not in ("nan", "None", "")
                    and float(doc_row.get("CC_Ladder_Monthly_Est") or 0) > 0
                )

                # When CC is UNFAVORABLE and no ladder: show stock-appreciation-only message
                if _cc_status_early == "UNFAVORABLE" and not _has_ladder_early and _gap > 0:
                    st.info(
                        "Recovery depends on stock appreciation — CC income currently blocked "
                        "(IV < HV or other gate). Re-check when conditions in CC Viability "
                        "section below are met."
                    )

                # Priority: ladder data → engine CC_Recovery_* → HV fallback
                _ldr_monthly_total = doc_row.get("CC_Ladder_Monthly_Est")
                _ldr_months_raw    = doc_row.get("CC_Ladder_Recovery_Months")
                _has_ladder = (
                    doc_row.get("CC_Ladder_Eligible")
                    and _ldr_monthly_total is not None
                    and str(_ldr_monthly_total) not in ("nan", "None", "")
                    and float(_ldr_monthly_total or 0) > 0
                )

                _eng_gap = doc_row.get("CC_Recovery_Gap")
                _eng_monthly = doc_row.get("CC_Recovery_Monthly_Est")
                _eng_months = doc_row.get("CC_Recovery_Months")
                _has_engine = (
                    _eng_gap is not None and _eng_monthly is not None
                    and str(_eng_gap) not in ("nan", "None", "")
                    and float(_eng_monthly or 0) > 0
                )

                def _safe_months(raw) -> int | None:
                    """Convert raw months to int, treating <1 as 1 (not None)."""
                    try:
                        v = float(raw or 0)
                        if v <= 0:
                            return None
                        return max(1, int(v + 0.5))   # round, minimum 1
                    except (TypeError, ValueError):
                        return None

                _recovery_source = "hv"
                if _has_ladder:
                    # Ladder: total $ → per-share (use aggregate shares when available)
                    _shares_for_ladder = _agg_shares if _agg_shares > 0 else quantity
                    _monthly_est = float(_ldr_monthly_total) / max(_shares_for_ladder, 1)
                    _weekly_est  = _monthly_est / 4.3
                    _months      = _safe_months(_ldr_months_raw)
                    _recovery_source = "ladder"
                elif _has_engine and _cc_status_early != "UNFAVORABLE":
                    _monthly_est = float(_eng_monthly)
                    _weekly_est  = _monthly_est / 4.3
                    _months      = _safe_months(_eng_months)
                    _recovery_source = "engine"
                elif _gap > 0 and hv and quantity >= 1 and _cc_status_early != "UNFAVORABLE":
                    # Natenberg ATM approx × OTM discount × fill haircut
                    # Matches engine _compute_recovery_timeline()
                    # Suppressed when CC is UNFAVORABLE — showing income while IV < HV is misleading
                    _hv_cap = min(hv, 1.0)       # cap at 100%
                    _atm_wk = 0.4 * _hv_cap * last / (52 ** 0.5)
                    _weekly_est  = _atm_wk * 0.30 * 0.85   # OTM delta ~0.30, fill haircut
                    _monthly_est = _weekly_est * 4.3
                    _months      = max(1, int(_gap / max(_monthly_est, 0.01)) + 1) if _monthly_est > 0 else None
                else:
                    _weekly_est = _monthly_est = 0.0
                    _months = None

                if _gap > 0 and _monthly_est > 0:
                    if _recovery_source == "ladder":
                        _src_label = "Ladder Est."
                        _mo_help = "Based on tiered CC ladder plan (partial coverage)"
                    elif _recovery_source == "engine":
                        _src_label = "OTM CC Est."
                        _mo_help = "Natenberg OTM call estimate (Δ~0.30, fill-adjusted)"
                    else:
                        _src_label = "OTM CC Est."
                        _mo_help = "Natenberg OTM call approximation (Δ~0.30, 85% fill) — verify with live chain"

                    rp_note_cols = st.columns(3)
                    rp_note_cols[0].metric(f"{_src_label} Weekly", f"~${_weekly_est:.2f}/sh")
                    rp_note_cols[1].metric(f"{_src_label} Monthly", f"~${_monthly_est:.2f}/sh")
                    rp_note_cols[2].metric("Months to Close Gap", f"~{_months}" if _months else "—",
                                           help=_mo_help)

                    if _cushion <= 0 and triage == "CRITICAL":
                        _src_note = (
                            f"Ladder income (\\${_monthly_est:.2f}/sh/month, partial coverage)"
                            if _recovery_source == "ladder"
                            else f"current vol (\\${_monthly_est:.2f}/sh/month)"
                        )
                        st.warning(
                            f"⚠️ Hard stop already breached. Recovery would require ~{_months} months "
                            f"at {_src_note}. "
                            "This is speculative — the stock must first stabilize and reclaim the "
                            f"stop level (\\${_hard_stop:.2f}) before a rolling path is viable. "
                            "Capital redeployment is the primary decision here."
                        )
                    elif _months and _months > 18:
                        st.warning(
                            f"⚠️ Recovery requires ~{_months} months"
                            f"{' (ladder est.)' if _recovery_source == 'ladder' else ' at current vol'}"
                            " — consider whether the capital is better deployed elsewhere."
                        )
                    elif _months:
                        st.caption(f"Feasible: ~{_months} months of rolling at \\${_monthly_est:.2f}/sh/month can close the \\${_gap:.2f} gap — if stock stabilizes.")

            # ── CC Viability gate ──────────────────────────────────────────
            # Read doctrine engine verdict first — it has already run the 4-gate arbitration
            # (structural, vol-edge, directional, opportunity-cost). Use it to gate the
            # top-line label so we never show "✅ CC writing appropriate" when the engine
            # determined HOLD_STOCK or MONITOR.
            _cc_status  = str(doc_row.get("CC_Proposal_Status") or "SCAN_MISS")
            _cc_verdict = str(doc_row.get("CC_Proposal_Verdict") or "")
            _cc_unfav   = str(doc_row.get("CC_Unfavorable_Reason") or "")
            _cc_watch   = str(doc_row.get("CC_Watch_Signal") or "")
            _cc_iv_rank = doc_row.get("CC_IV_Rank")
            _cc_regime  = str(doc_row.get("CC_Regime") or "")
            _cc_best_y  = doc_row.get("CC_Best_Ann_Yield")
            _cc_best_dte= str(doc_row.get("CC_Best_DTE_Bucket") or "")
            _cc_scan_ts = str(doc_row.get("CC_Scan_TS") or "")
            _arb_tag    = _cc_verdict.split(" — ")[0].strip() if " — " in _cc_verdict else ""

            st.markdown("**📋 CC Viability**")
            # Check if ladder-eligible BEFORE triage gate
            _ladder_eligible_here = doc_row.get("CC_Ladder_Eligible")
            _ladder_json_here = doc_row.get("CC_Ladder_JSON")
            _is_ladder_eligible = (
                _ladder_eligible_here
                and _ladder_json_here
                and str(_ladder_json_here) not in ("nan", "None", "", "N/A")
            )
            if triage == "CRITICAL" and not _is_ladder_eligible:
                st.error(
                    "🚫 **Do not write calls yet.** "
                    "Selling a call caps upside at the strike — if the stock recovers, "
                    "you'll be forced to sell at a loss to your cost basis. "
                    "Resolve thesis first. If INTACT, wait for price to recover to at least "
                    f"\\${last * 1.15:.2f} (+15%) before writing the first call."
                )
            elif triage == "CRITICAL" and _is_ladder_eligible:
                st.warning(
                    "⚠️ **Partial-coverage ladder available** — "
                    "position is CRITICAL but large enough for tiered CC income. "
                    "Uncovered lots preserve rally participation. See ladder plan below."
                )
            elif triage == "RECOVERY" and last and _basis_ps_valid:
                _min_cc_price = last * 1.10   # strike must be ≥10% OTM
                _cc_verdict_str = (
                    f"Conditional: write calls only at strike ≥ **\\${_min_cc_price:.2f}** "
                    f"(10% OTM from \\${last:.2f}). "
                    "At this level you collect premium without capping the recovery path. "
                    "If no viable strike exists at 10% OTM in the near-term chain, wait."
                )
                st.warning(f"🟡 {_cc_verdict_str}")
            elif _cc_status == "UNFAVORABLE" or _arb_tag in ("HOLD_STOCK", "MONITOR"):
                # Doctrine engine already determined CC is inadvisable — don't emit green checkmark.
                # The full reason will display below in the CC engine block.
                if _arb_tag == "HOLD_STOCK":
                    st.warning("🔴 **Hold stock — selling convexity not advisable now.** See analysis below.")
                elif _arb_tag == "MONITOR":
                    st.info("🟡 **Monitor — conditions not yet right for covered call.** See analysis below.")
                else:
                    st.warning("🔴 **CC not advisable now.** See analysis below.")
            elif _cc_status in ("SCAN_MISS", "ERROR", "", "nan", "None"):
                # Engine hasn't evaluated yet — no verdict, no green checkmark.
                # The HV context block below will emit the appropriate pre-scan guidance.
                pass
            else:
                st.success("✅ CC writing appropriate — see scan for ranked candidates below.")

            # ── HV / IV surface context (book-backed pre-scan guidance) ───
            # _iv_ref / _iv_known used both here and in SCAN_MISS block below
            _iv_ref = iv_30d if iv_30d > 0 else iv_entry
            _iv_known = _iv_ref > 0
            if hv > 0.60:  # HV > 60% annualized — extreme realized vol
                _hv_pct_str = f" (HV at {hv_pct:.0%} of historical range)" if hv_pct > 0 else ""
                _iv_vs_hv = ""
                if _iv_known:
                    if _iv_ref < hv:
                        _iv_vs_hv = (
                            f" IV({_iv_ref*100:.0f}%) < HV({hv*100:.0f}%) — "
                            "realized vol exceeds implied: seller's edge is **negative** "
                            "(Natenberg Ch.7). Selling calls here = structurally negative EV. "
                            "**Wait for IV to rise above HV before writing.**"
                        )
                    else:
                        _iv_vs_hv = (
                            f" IV({_iv_ref*100:.0f}%) ≥ HV({hv*100:.0f}%) — "
                            "seller's edge present despite high HV. Premium is rich."
                        )
                _surface_note = ""
                if iv_surface == "BACKWARDATION":
                    _surface_note = (
                        " Term structure: **BACKWARDATION** (short-term IV > long-term) — "
                        "vol is spiking. Near-term premiums are elevated, "
                        "but mean-reversion risk is high: "
                        "the stock can make large moves in either direction. "
                        "McMillan Ch.3: in backwardation, prefer monthly (30–45d) over weekly "
                        "to avoid gamma-blowup on a single day's move."
                    )
                st.warning(
                    f"⚠️ **Extreme HV: {hv*100:.0f}%**{_hv_pct_str}.{_iv_vs_hv}{_surface_note}"
                )

            # ── CC Opportunity from doctrine engine ───────────────────────
            # Note: _cc_status / _cc_verdict / etc already resolved above in CC Viability gate.
            # Ladder-eligible CRITICAL positions bypass the triage gate — the ladder
            # IS the plan for structural damage (partial coverage, not full CC).
            if _cc_status == "FAVORABLE" and (triage != "CRITICAL" or _is_ladder_eligible):
                _iv_r_disp  = f"{float(_cc_iv_rank):.0f}%" if _cc_iv_rank and str(_cc_iv_rank) not in ("nan","None","") else "—"
                _best_y_disp= f"{float(_cc_best_y):.1%}" if _cc_best_y and str(_cc_best_y) not in ("nan","None","") else "—"
                cc1, cc2, cc3 = st.columns(3)
                cc1.metric("IV Rank", _iv_r_disp)
                cc2.metric("Regime", _cc_regime or "—")
                cc3.metric(f"Best Yield ({_cc_best_dte})", _best_y_disp)
                if _cc_verdict:
                    st.success(f"✅ {_cc_verdict}")
                # Candidate list (execution-ready format)
                def _fmt_expiry_nl(exp_str: str) -> str:
                    try:
                        from datetime import datetime as _dt
                        return _dt.fromisoformat(exp_str).strftime("%b %d")
                    except Exception:
                        return exp_str or "?"

                for _ci in range(1, 4):
                    _craw = doc_row.get(f"CC_Candidate_{_ci}")
                    if _craw and str(_craw) not in ("nan","None",""):
                        try:
                            import json as _json
                            _cd = _json.loads(str(_craw))
                            _bucket = _cd.get("bucket", "?")
                            _cstrike= _cd.get("strike", 0)
                            _cdte   = _cd.get("dte", 0)
                            _cmid   = _cd.get("mid", 0)
                            _cdelta = _cd.get("delta", 0)
                            _cay    = _cd.get("ann_yield", 0)
                            _cliq   = _cd.get("liq", "")
                            _csprd  = _cd.get("spread_pct", 0)
                            _coi    = _cd.get("oi", 0)
                            _civ    = _cd.get("iv_pct", 0)
                            _cbid   = _cd.get("bid", 0)
                            _cask   = _cd.get("ask", 0)
                            _cexp   = _cd.get("expiry", "")
                            _csrc   = _cd.get("source", "")
                            _ccontr = _cd.get("contracts", 1)
                            _rec    = "✅ Best" if _ci == 1 else f"#{_ci}"
                            _exp_fmt_nl = _fmt_expiry_nl(_cexp) if _cexp else f"{_cdte}d"
                            st.markdown(
                                f"**{_rec} · {_bucket}** — "
                                f"Strike **\\${_cstrike:.2f}** · "
                                f"Exp **{_exp_fmt_nl}** ({_cdte}d) · "
                                f"Mid **\\${_cmid:.2f}/sh** · "
                                f"Δ {_cdelta:.2f} · Liq {_cliq}"
                                + (f" · Spread {_csprd:.1f}%" if _csprd else "")
                            )
                            # Execution details
                            _exec_lines = []
                            _exec_lines.append(
                                f"Sell {_ccontr}× {ticker} {_exp_fmt_nl} ${_cstrike:.2f}C "
                                f"@ limit **\\${_cmid:.2f}**"
                            )
                            if _cbid or _cask:
                                _exec_lines.append(
                                    f"Bid \\${_cbid:.2f} / Mid \\${_cmid:.2f} / Ask \\${_cask:.2f}"
                                )
                            _total_cr = _cmid * 100 * _ccontr
                            _exec_lines.append(
                                f"Total credit: **\\${_total_cr:,.0f}** · "
                                f"Ann yield: **{_cay:.1%}**"
                                + (f" · IV: {_civ:.0f}% · OI: {_coi:,}" if _coi else "")
                            )
                            _stale_nl = " *(scan est.)*" if _csrc == "SCAN_DATA" else ""
                            st.caption("  \n".join(_exec_lines) + _stale_nl)
                        except Exception:
                            pass

                # Partial-coverage advisory
                _pc_note = doc_row.get("CC_Partial_Coverage_Note")
                if _pc_note and str(_pc_note) not in ("nan", "None", ""):
                    st.info(f"ℹ️ {_pc_note}")
                # ── CC Ladder display ──────────────────────────────────────
                _ladder_eligible = doc_row.get("CC_Ladder_Eligible")
                _ladder_json_raw = doc_row.get("CC_Ladder_JSON")
                if _ladder_eligible and _ladder_json_raw and str(_ladder_json_raw) not in ("nan", "None", "", "N/A"):
                    try:
                        import json as _json
                        _lplan = _json.loads(str(_ladder_json_raw))
                        _l_framing = _lplan.get("framing", "")
                        _l_cov = _lplan.get("covered_lots", 0)
                        _l_tot = _lplan.get("total_lots", 0)
                        _l_unc = _lplan.get("uncovered_lots", 0)
                        _l_cov_pct = _lplan.get("max_coverage_pct", 0)
                        _l_monthly = _lplan.get("monthly_income_est", 0)
                        _l_igr = _lplan.get("income_gap_ratio", 0)
                        _l_rmo = _lplan.get("recovery_months_est", 0)
                        _l_ta = _lplan.get("tier_a_lots", 0)
                        _l_tb = _lplan.get("tier_b_lots", 0)
                        _l_gap_ps = _lplan.get("gap_per_share", 0)
                        _l_floor = _lplan.get("strike_floor", 0)
                        _l_ta_best = _lplan.get("tier_a_best")
                        _l_tb_best = _lplan.get("tier_b_best")
                        _l_cbr = _lplan.get("cost_basis_reduction_annual", 0)
                        _l_b1yr = _lplan.get("basis_after_1yr", 0)

                        st.markdown("---")
                        st.markdown("**CC Ladder — Tiered Partial Coverage**")

                        # Framing banner
                        if _l_framing == "CASH_FLOW_ONLY":
                            st.error(
                                "🚫 **CASH FLOW ONLY** — Ladder generates income but "
                                "cannot realistically repair the gap. Do NOT frame as recovery."
                            )
                        elif _l_framing == "PARTIAL_REPAIR":
                            st.warning(
                                f"⚠️ **PARTIAL REPAIR** — Basis reduction {_l_cbr:.1%}/yr "
                                f"→ ${_l_b1yr:.2f}/sh after 1yr. "
                                f"~{_l_rmo:.0f} months at current rates."
                            )
                        else:
                            st.success(
                                f"✅ **RECOVERY VIABLE** — Basis reduction {_l_cbr:.1%}/yr "
                                f"→ ${_l_b1yr:.2f}/sh after 1yr. ~{_l_rmo:.0f}mo to close gap."
                            )

                        # Summary metrics
                        _lc1, _lc2, _lc3, _lc4 = st.columns(4)
                        _lc1.metric("Covered", f"{_l_cov}/{_l_tot} lots ({_l_cov_pct:.0%})")
                        _lc2.metric("Uncovered", f"{_l_unc} lots (rally)")
                        _lc3.metric("Est. Monthly", f"${_l_monthly:,.0f}")
                        _lc4.metric("Basis Reduction", f"{_l_cbr:.1%}/yr" if _l_cbr > 0 else "—")

                        # ── Tier display helper ──────────────────────
                        def _fmt_expiry(exp_str: str) -> str:
                            """'2026-03-28' → 'Mar 28'"""
                            try:
                                from datetime import datetime as _dt
                                return _dt.fromisoformat(exp_str).strftime("%b %d")
                            except Exception:
                                return exp_str or "?"

                        def _render_tier(label: str, lots: int, best: dict | None):
                            if not best:
                                st.caption(f"{label} ({lots} contracts) — no viable strikes in window")
                                return
                            _exp_fmt = _fmt_expiry(best.get("expiry", ""))
                            _dte_v   = best.get("dte", 0)
                            _strike_v = best.get("strike", 0)
                            _mid_v   = best.get("mid", 0)
                            _bid_v   = best.get("bid", 0)
                            _ask_v   = best.get("ask", 0)
                            _delta_v = best.get("delta", 0)
                            _liq_v   = best.get("liq", "")
                            _ay_v    = best.get("ann_yield", 0)
                            _sprd_v  = best.get("spread_pct", 0)
                            _oi_v    = best.get("oi", 0)
                            _iv_v    = best.get("iv_pct", 0)
                            _src_v   = best.get("source", "")
                            st.markdown(
                                f"**{label}** ({lots} contracts) — "
                                f"Strike **\\${_strike_v:.2f}** · "
                                f"Exp **{_exp_fmt}** ({_dte_v}d) · "
                                f"Mid **\\${_mid_v:.2f}/sh** · "
                                f"Δ {_delta_v:.2f} · Liq {_liq_v}"
                                + (f" · Spread {_sprd_v:.1f}%" if _sprd_v else "")
                            )
                            # Execution details: bid/ask + limit order
                            _exec_parts = []
                            if _bid_v or _ask_v:
                                _exec_parts.append(f"Bid **\\${_bid_v:.2f}** · Mid **\\${_mid_v:.2f}** · Ask **\\${_ask_v:.2f}**")
                            _exec_parts.append(f"Sell {lots}× {ticker} {_exp_fmt} ${_strike_v:.2f}C @ limit **\\${_mid_v:.2f}**")
                            _total_credit = _mid_v * 100 * lots
                            _exec_parts.append(f"Total credit: **\\${_total_credit:,.0f}** ({lots}×100×${_mid_v:.2f})")
                            if _ay_v:
                                _exec_parts.append(f"Ann. yield: **{_ay_v:.1%}** · IV: {_iv_v:.0f}% · OI: {_oi_v:,}")
                            _stale = " *(scan est.)*" if _src_v == "SCAN_DATA" else ""
                            st.caption("  \n".join(_exec_parts) + _stale)

                        # Tier A
                        _render_tier("Tier A", _l_ta, _l_ta_best)
                        # Tier B
                        _render_tier("Tier B", _l_tb, _l_tb_best)

                        st.caption(
                            f"**Tier C:** {_l_unc} lots uncovered (upside participation). "
                            f"Strike floor: ${_l_floor:.2f}. Gap: ${_l_gap_ps:.2f}/sh."
                        )
                    except Exception:
                        pass

                # Recovery-mode info box (non-ladder)
                elif _cc_rec_mode in ("RECOVERY", "DEEP_RECOVERY"):
                    _rec_gap_d = doc_row.get("CC_Recovery_Gap")
                    _rec_mo_d  = doc_row.get("CC_Recovery_Months")
                    _rec_gap_v = f"${float(_rec_gap_d):.2f}/sh" if _rec_gap_d and str(_rec_gap_d) not in ("nan","None","") else ""
                    _rec_mo_v  = f"~{float(_rec_mo_d):.0f} months" if _rec_mo_d and str(_rec_mo_d) not in ("nan","None","") else ""
                    _floor_v   = f"${max(last * 1.10, basis_ps):.2f}" if _basis_ps_valid and last else ""
                    st.info(
                        f"🔧 **{_cc_rec_mode}** — Gap: {_rec_gap_v}. "
                        f"Timeline: {_rec_mo_v}. "
                        f"Strike floor: {_floor_v} (never cap below breakeven). "
                        "IV gate relaxed to 15%. (Jabbour Ch.4)"
                    )
                if _cc_scan_ts:
                    st.caption(f"Scan: {_cc_scan_ts} — re-run at open for live prices")

            elif _cc_status == "UNFAVORABLE":
                if _cc_rec_mode == "STRUCTURAL_DAMAGE":
                    _sd_gap = doc_row.get("CC_Recovery_Gap")
                    _sd_gap_v = f"${float(_sd_gap):.2f}/sh" if _sd_gap and str(_sd_gap) not in ("nan","None","") else ""
                    # Small position SD → full block. Ladder-eligible SD → FAVORABLE path above.
                    st.error(
                        f"🚫 **STRUCTURAL DAMAGE** — Gap: {_sd_gap_v} ({_drift_str}). "
                        "Position too small for partial-coverage ladder (<1000 shares). "
                        "CC income cannot close this gap. McMillan Ch.3: don't sell calls on "
                        "a position deeper than -35%. "
                        "**Decision: cut loss and redeploy capital, or hold for thesis only.**"
                    )
                elif _cc_verdict:
                    st.warning(f"🔴 {_cc_verdict}")
                if _cc_unfav:
                    for _r in _cc_unfav.split(" | "):
                        st.caption(f"• {_r}")
                if _cc_watch:
                    st.caption(f"👁 Watch: {_cc_watch}")

                # ── ETF awareness badge ──────────────────────────────
                _is_etf_pos = doc_row.get("Is_ETF", False)
                if _is_etf_pos and str(_is_etf_pos) not in ("False", "0", "nan", "None", ""):
                    st.info(
                        "**ETF** — No earnings risk. "
                        "HV is macro-driven and mean-reverts faster than single-stock HV. "
                        "When HV starts declining, the IV/HV crossover window opens "
                        "sooner and more predictably."
                    )

                # ── Re-check guidance with current numeric values ──────
                _recheck_triggers = []
                _unfav_lower = (_cc_unfav or "").lower()
                _cc_iv_rank_v = doc_row.get("CC_IV_Rank")
                _cc_regime_v  = str(doc_row.get("CC_Regime") or "")
                _hv_20d_v     = float(doc_row.get("HV_20D") or 0)

                # Parse IV from verdict/reason text for display
                import re as _re
                _iv_match = _re.search(r'IV[_ ]?(?:30D)?[=:]\s*(\d+\.?\d*)%?', _cc_unfav or _cc_verdict or "")
                _iv_val_disp = f"{float(_iv_match.group(1)):.0f}%" if _iv_match else None

                if "iv_rank" in _unfav_lower or "iv rank" in _unfav_lower:
                    _cur_ivr = f"{float(_cc_iv_rank_v):.0f}%" if _cc_iv_rank_v and str(_cc_iv_rank_v) not in ("nan","None","") else "N/A"
                    _recheck_triggers.append(f"IV_Rank crosses above 25% (currently {_cur_ivr})")

                if "iv/hv" in _unfav_lower or "iv(" in _unfav_lower or "iv <" in _unfav_lower or "iv < hv" in _unfav_lower:
                    _iv_disp = _iv_val_disp or ("N/A")
                    _hv_disp = f"{_hv_20d_v*100:.0f}%" if _hv_20d_v > 0 else "N/A"
                    _recheck_triggers.append(f"IV rises above HV (currently IV {_iv_disp} vs HV {_hv_disp})")

                if "regime" in _unfav_lower:
                    _recheck_triggers.append(f"Regime shifts to High Vol or Elevated (currently {_cc_regime_v or 'Unknown'})")

                if "uptrend" in _unfav_lower or "trend" in _unfav_lower:
                    _recheck_triggers.append("Momentum cools — ADX drops below 30 or signal turns Neutral")

                if _recheck_triggers:
                    _triggers_str = " · ".join(_recheck_triggers)
                    st.caption(f"**Re-check when:** {_triggers_str}")

            elif _cc_status == "SCAN_MISS" or not _cc_status or _cc_status in ("", "nan"):
                # Show data-aware pre-scan guidance when we have HV/IV from management engine
                if _iv_known and hv > 0:
                    if _iv_ref < hv * 0.90:
                        st.warning(
                            f"⏳ Scan not yet run — but pre-scan signal: "
                            f"**IV({_iv_ref*100:.0f}%) < HV({hv*100:.0f}%)** "
                            "→ realized vol exceeds implied. "
                            "Natenberg Ch.7: wait for IV to rise above HV before selling calls. "
                            "Re-run pipeline during market hours for current IV_Rank."
                        )
                    else:
                        st.caption(
                            f"⏳ Scan not yet run — IV({_iv_ref*100:.0f}%) vs HV({hv*100:.0f}%): "
                            "spread looks favorable for CC. Run pipeline during market hours "
                            "to confirm IV_Rank, chain candidates, and regime classification."
                        )
                elif hv > 0.60:
                    st.warning(
                        f"⏳ Scan not yet run — note: HV={hv*100:.0f}% is extreme. "
                        "Run pipeline during market hours to check if IV is above HV "
                        "before writing calls."
                    )
                else:
                    st.caption(
                        "⏳ CC evaluation not yet run — run the pipeline during market hours "
                        "to populate IV_Rank, chain candidates, and regime classification."
                    )

            # ── Thesis note ────────────────────────────────────────────────
            # For CRITICAL positions, "INTACT" is not a green light — it means the
            # business story is still alive, but that does NOT lift the CC constraint
            # or reduce urgency.  Show it as a neutral info note, not a success banner.
            if thesis in ("DEGRADED", "BROKEN"):
                st.error(f"⚠️ Thesis: {thesis} — review before any action. Damaged thesis + calls = locked-in loss.")
            elif thesis == "INTACT":
                if triage == "CRITICAL":
                    st.info(
                        "**Thesis: INTACT** — the business story is still alive, but this does not lift "
                        "the CRITICAL constraint. Thesis intact ≠ safe to write calls. "
                        "The position is still deeply underwater. Resolve capital risk first."
                    )
                else:
                    st.caption("✅ Thesis: INTACT")

            # ── Copy Card ─────────────────────────────────────────────────
            _cc_copy_lines: list[str] = []
            _cc_copy_lines.append(
                f"{_triage_icon} {ticker}{_acct_label} — {triage} · {_drift_str} from basis"
                + (f" · ${total_gl_pos:+,.0f}" if total_gl_pos else "")
                + (_cc_rec_badge if _cc_rec_badge else "")
            )
            _cc_copy_lines.append(
                f"Last: ${last:.2f} | Basis: ${basis_ps:.2f}/sh | Shares: {int(quantity):,} | "
                f"Drift: {_drift_str} | HV-20D: {hv*100:.0f}%"
                if _basis_ps_valid else f"Last: ${last:.2f} | Shares: {int(quantity):,} | HV-20D: {hv*100:.0f}%"
            )
            _cc_copy_lines.append("")

            # Recovery path / risk metrics
            if _basis_ps_valid and last > 0:
                _gap_c = max(0.0, basis_ps - last)
                _cc_copy_lines.append("Recovery Path Analysis" if _gap_c > 0 else "Position Risk Metrics")
                _cc_copy_lines.append(
                    f"Gap: ${_gap_c:.2f}/sh (${_gap_c * quantity:,.0f} total) | "
                    f"Hard Stop: ${basis_ps * (0.70 if triage == 'CRITICAL' else 0.80):.2f}"
                )
                if _gap_c > 0 and _monthly_est > 0:
                    _src_c = "Ladder" if _recovery_source == "ladder" else "OTM CC"
                    _mo_str = f"~{_months}" if _months else "< 1"
                    _cc_copy_lines.append(
                        f"{_src_c} Est: ~${_monthly_est:.2f}/sh/mo | {_mo_str} months to close gap"
                    )
                _cc_copy_lines.append("")

            # CC viability
            _cc_copy_lines.append(f"CC Status: {_cc_status}")
            if _cc_verdict:
                _cc_copy_lines.append(f"Verdict: {_cc_verdict}")
            _cc_copy_lines.append("")

            # Ladder plan
            if _is_ladder_eligible:
                try:
                    import json as _json_copy
                    _lp = _json_copy.loads(str(doc_row.get("CC_Ladder_JSON", "")))
                    _cc_copy_lines.append("CC Ladder — Tiered Partial Coverage")
                    _cc_copy_lines.append(
                        f"Framing: {_lp.get('framing', '')} | "
                        f"Covered: {_lp.get('covered_lots', 0)}/{_lp.get('total_lots', 0)} lots "
                        f"({_lp.get('max_coverage_pct', 0):.0%}) | "
                        f"Uncovered: {_lp.get('uncovered_lots', 0)} lots"
                    )
                    _cc_copy_lines.append(
                        f"Est Monthly: ${_lp.get('monthly_income_est', 0):,.0f} | "
                        f"Basis Reduction: {_lp.get('cost_basis_reduction_annual', 0):.1%}/yr | "
                        f"Basis After 1yr: ${_lp.get('basis_after_1yr', 0):.2f}/sh"
                    )
                    def _copy_tier(label: str, lots: int, best: dict | None) -> list[str]:
                        if not best:
                            return [f"{label} ({lots} contracts): no viable strikes"]
                        _exp_s = best.get("expiry", "")
                        try:
                            from datetime import datetime as _dtc
                            _exp_s = _dtc.fromisoformat(_exp_s).strftime("%b %d")
                        except Exception:
                            pass
                        _stk = best.get("strike", 0)
                        _dte_c = best.get("dte", 0)
                        _mid_c = best.get("mid", 0)
                        _bid_c = best.get("bid", 0)
                        _ask_c = best.get("ask", 0)
                        _dlt_c = best.get("delta", 0)
                        _liq_c = best.get("liq", "")
                        _ay_c  = best.get("ann_yield", 0)
                        _sprd_c = best.get("spread_pct", 0)
                        _oi_c  = best.get("oi", 0)
                        _iv_c  = best.get("iv_pct", 0)
                        _total_c = _mid_c * 100 * lots
                        lines = [
                            f"{label} ({lots} contracts):",
                            f"  Sell {lots}x {ticker} {_exp_s} ${_stk:.2f}C @ limit ${_mid_c:.2f}",
                            f"  Exp {_exp_s} ({_dte_c}d) · Bid ${_bid_c:.2f} / Mid ${_mid_c:.2f} / Ask ${_ask_c:.2f}"
                            + (f" · Spread {_sprd_c:.1f}%" if _sprd_c else ""),
                            f"  D {_dlt_c:.2f} · Liq {_liq_c} · OI {_oi_c:,} · IV {_iv_c:.0f}%",
                            f"  Ann yield {_ay_c:.1%} · Total credit ${_total_c:,.0f}",
                        ]
                        return lines

                    _ta_c = _lp.get("tier_a_best")
                    _tb_c = _lp.get("tier_b_best")
                    _cc_copy_lines.extend(_copy_tier("Tier A", _lp.get("tier_a_lots", 0), _ta_c))
                    _cc_copy_lines.extend(_copy_tier("Tier B", _lp.get("tier_b_lots", 0), _tb_c))
                    _cc_copy_lines.append(
                        f"Tier C: {_lp.get('uncovered_lots', 0)} lots uncovered (rally) | "
                        f"Strike floor: ${_lp.get('strike_floor', 0):.2f}"
                    )
                    _cc_copy_lines.append("")
                except Exception:
                    pass
            elif _cc_status == "FAVORABLE":
                # Non-ladder candidates (execution-ready)
                for _ci_c in range(1, 4):
                    _craw_c = doc_row.get(f"CC_Candidate_{_ci_c}")
                    if _craw_c and str(_craw_c) not in ("nan", "None", ""):
                        try:
                            import json as _json_copy2
                            _cd_c = _json_copy2.loads(str(_craw_c))
                            _rec_c = "Best" if _ci_c == 1 else f"#{_ci_c}"
                            _exp_nl = _cd_c.get("expiry", "")
                            try:
                                from datetime import datetime as _dtc2
                                _exp_nl = _dtc2.fromisoformat(_exp_nl).strftime("%b %d")
                            except Exception:
                                pass
                            _bid_nl = _cd_c.get("bid", 0)
                            _ask_nl = _cd_c.get("ask", 0)
                            _mid_nl = _cd_c.get("mid", 0)
                            _contr_nl = _cd_c.get("contracts", 1)
                            _dte_nl = _cd_c.get("dte", 0)
                            _dlt_nl = _cd_c.get("delta", 0)
                            _liq_nl = _cd_c.get("liq", "")
                            _ay_nl  = _cd_c.get("ann_yield", 0)
                            _oi_nl  = _cd_c.get("oi", 0)
                            _iv_nl  = _cd_c.get("iv_pct", 0)
                            _sprd_nl = _cd_c.get("spread_pct", 0)
                            _total_nl = _mid_nl * 100 * _contr_nl
                            _cc_copy_lines.append(
                                f"{_rec_c} · {_cd_c.get('bucket', '?')}:"
                            )
                            _cc_copy_lines.append(
                                f"  Sell {_contr_nl}x {ticker} {_exp_nl} ${_cd_c.get('strike', 0):.2f}C @ limit ${_mid_nl:.2f}"
                            )
                            _cc_copy_lines.append(
                                f"  Exp {_exp_nl} ({_dte_nl}d) · "
                                f"Bid ${_bid_nl:.2f} / Mid ${_mid_nl:.2f} / Ask ${_ask_nl:.2f}"
                                + (f" · Spread {_sprd_nl:.1f}%" if _sprd_nl else "")
                            )
                            _cc_copy_lines.append(
                                f"  D {_dlt_nl:.2f} · Liq {_liq_nl}"
                                + (f" · OI {_oi_nl:,}" if _oi_nl else "")
                                + (f" · IV {_iv_nl:.0f}%" if _iv_nl else "")
                            )
                            _cc_copy_lines.append(
                                f"  Ann yield {_ay_nl:.1%} · Total credit ${_total_nl:,.0f}"
                            )
                        except Exception:
                            pass
                # Partial-coverage note in copy card
                _pc_note_c = doc_row.get("CC_Partial_Coverage_Note")
                if _pc_note_c and str(_pc_note_c) not in ("nan", "None", ""):
                    _cc_copy_lines.append(str(_pc_note_c))
                _cc_copy_lines.append("")

            _cc_copy_lines.append(f"Thesis: {thesis}")

            with st.expander("📋 Copy Card", expanded=False):
                st.code("\n".join(_cc_copy_lines), language=None)


# ─────────────────────────────────────────────────────────────────────────────
# Section C — Expiration Calendar
# ─────────────────────────────────────────────────────────────────────────────

def _render_expiration_calendar(df: pd.DataFrame):
    st.subheader("Expiration Calendar")

    options = df[df["AssetType"] == "OPTION"].copy()
    if options.empty:
        st.info("No option positions found.")
        return

    options["DTE"] = _compute_dte(options["Expiration"])
    options["Exp_Date"] = pd.to_datetime(options["Expiration"], errors="coerce").dt.date

    by_exp = (
        options.groupby("Exp_Date")
        .agg(
            Contracts=("Quantity", lambda x: abs(x).sum()),
            Tickers=("Underlying_Ticker", lambda x: ", ".join(sorted(x.unique()))),
            Min_DTE=("DTE", "min"),
            Net_Theta=("Theta", lambda x: (x * options.loc[x.index, "Quantity"].astype(float)).sum() * 100),
        )
        .reset_index()
        .sort_values("Exp_Date")
    )

    def _urgency(dte):
        if pd.isna(dte):      return "⚪"
        if dte <= 7:           return "🔴"
        if dte <= 21:          return "🟠"
        return "🟢"

    by_exp["Urgency"] = by_exp["Min_DTE"].apply(_urgency)
    by_exp["Exp_Date"] = by_exp["Exp_Date"].astype(str)
    by_exp["Min_DTE"]  = by_exp["Min_DTE"].apply(lambda x: f"{int(x)}d" if pd.notna(x) else "—")
    by_exp["Net_Theta"] = by_exp["Net_Theta"].apply(lambda x: f"${x:+.2f}/day")
    by_exp = by_exp[["Urgency", "Exp_Date", "Min_DTE", "Tickers", "Contracts", "Net_Theta"]]
    by_exp = by_exp.reset_index(drop=True)

    st.dataframe(by_exp, hide_index=True, width='stretch')
    st.caption("🔴 ≤7 days  🟠 ≤21 days  🟢 >21 days")


# ─────────────────────────────────────────────────────────────────────────────
# Section D — Portfolio Greek Breakdown
# ─────────────────────────────────────────────────────────────────────────────

def _render_greek_breakdown(df: pd.DataFrame, doctrine_df: pd.DataFrame | None = None):
    st.subheader("Net Greek Breakdown")

    options = df[df["AssetType"] == "OPTION"].copy()
    if options.empty:
        st.info("No option positions.")
        return

    options["Quantity"] = pd.to_numeric(options["Quantity"], errors="coerce")
    for g in ["Delta", "Gamma", "Theta", "Vega"]:
        options[g] = pd.to_numeric(options[g], errors="coerce")

    # Portfolio totals (from doctrine if available)
    if doctrine_df is not None and not doctrine_df.empty:
        port_cols = ["Portfolio_Net_Delta", "Portfolio_Net_Vega", "Portfolio_Net_Gamma", "Portfolio_Net_Theta"]
        if all(c in doctrine_df.columns for c in port_cols):
            first = doctrine_df.iloc[0]
            p1, p2, p3, p4 = st.columns(4)
            p1.metric("Portfolio Net Δ", f"{float(first['Portfolio_Net_Delta']):+.1f}")
            p2.metric("Portfolio Net ν", f"{float(first['Portfolio_Net_Vega']):+.2f}")
            p3.metric("Portfolio Net Γ", f"{float(first['Portfolio_Net_Gamma']):+.4f}")
            p4.metric("Portfolio Net θ/day", f"{float(first['Portfolio_Net_Theta']):+.2f}")
            st.divider()

    greek_summary = (
        options.groupby("Underlying_Ticker")
        .apply(lambda g: pd.Series({
            "Net Δ": (g["Delta"] * g["Quantity"]).sum() * 100,
            "Net Γ": (g["Gamma"] * g["Quantity"]).sum() * 100,
            "Net θ/day": (g["Theta"] * g["Quantity"]).sum() * 100,
            "Net ν": (g["Vega"] * g["Quantity"]).sum() * 100,
            "Contracts": g["Quantity"].abs().sum(),
        }), include_groups=False)
        .reset_index()
        .sort_values("Net θ/day", ascending=True)
    )

    def _style_greeks(row):
        styles = []
        for col in greek_summary.columns:
            if col in ["Net Δ", "Net Γ", "Net θ/day", "Net ν"]:
                try:
                    val = float(row[col])
                    styles.append("color: #09ab3b" if val > 0 else ("color: #ff4b4b" if val < 0 else ""))
                except Exception:
                    styles.append("")
            else:
                styles.append("")
        return styles

    st.dataframe(
        greek_summary.style.apply(_style_greeks, axis=1).format({
            "Net Δ": "{:+.1f}", "Net Γ": "{:+.4f}",
            "Net θ/day": "${:+.2f}", "Net ν": "{:+.2f}", "Contracts": "{:.0f}",
        }),
        hide_index=True, width="stretch",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Run Doctrine Engine
# ─────────────────────────────────────────────────────────────────────────────

def _render_run_engine_control(latest_csv: str):
    """Button to trigger run_all.py from the dashboard."""
    st.subheader("Run Doctrine Engine")
    st.caption(
        "Runs the full Cycle 1→2→3 pipeline against the latest uploaded CSV. "
        "Computes drift, chart states, and doctrine recommendations."
    )

    col_btn, col_status = st.columns([1, 3])
    with col_btn:
        run_clicked = st.button("▶ Run Now", width="stretch",
                                help="Triggers run_all.py with the latest uploaded Fidelity CSV")

    if run_clicked:
        input_path = Path(latest_csv)
        if not input_path.exists():
            st.error(f"Input CSV not found: {input_path}")
            return

        with st.spinner("Running Doctrine Engine (Cycles 1→2→3)..."):
            try:
                result = subprocess.run(
                    [
                        sys.executable, "core/management/run_all.py",
                        "--input", str(input_path),
                        "--emit", "core/management/outputs/positions_latest.csv",
                        "--archive", "core/management/outputs/history/",
                        "--audit", "core/management/outputs/audit/",
                        "--allow-system-time",
                    ],
                    capture_output=True, text=True, timeout=180
                )
                if result.returncode == 0:
                    st.success("✅ Doctrine Engine complete. Refresh data to see recommendations.")
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error("❌ Pipeline failed.")
                    with st.expander("Error details"):
                        st.code(result.stderr[-3000:])
            except subprocess.TimeoutExpired:
                st.error("⏱️ Pipeline timed out after 3 minutes.")
            except Exception as e:
                st.error(f"❌ Failed to run pipeline: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def render_manage_view(core_project_root, sanitize_func, set_view_func):
    if st.button("← Back to Home"):
        set_view_func("home")

    st.title("📋 Position Monitor")

    # ── Load data ───────────────────────────────────────────────────────────
    from core.shared.data_contracts.config import PIPELINE_DB_PATH

    doctrine_df, has_doctrine = _load_doctrine()
    db_df = _load_positions_from_duckdb(str(PIPELINE_DB_PATH))

    # Use doctrine data if available (has all Cycle 1 fields + enrichment)
    # For the position cards we need Cycle 1 columns; doctrine output has them
    if has_doctrine and not doctrine_df.empty and "$ Total G/L" in doctrine_df.columns:
        df = doctrine_df.copy()
    elif not db_df.empty:
        df = db_df.copy()
    else:
        st.warning("No positions found. Upload a Fidelity CSV in **Upload Positions** first.")
        if st.button("Go to Upload"):
            set_view_func("perception")
        return

    # ── Sidebar ─────────────────────────────────────────────────────────────
    with st.sidebar:
        st.header("⚙️ Filters")

        if has_doctrine:
            ts = pd.to_datetime(doctrine_df["Snapshot_TS"]).max()
            st.success(f"✅ Doctrine: {ts.strftime('%b %d %H:%M')}")
        else:
            st.warning("⚠️ Doctrine not run yet")

        # Ticker filter — only tickers with at least one OPTION leg
        if "AssetType" in df.columns:
            ticker_options = sorted(df.loc[df["AssetType"] == "OPTION", "Underlying_Ticker"].dropna().unique())
        else:
            ticker_options = sorted(df["Underlying_Ticker"].dropna().unique())
        selected_tickers = st.multiselect(
            "Filter by Ticker",
            ticker_options,
            key="manage_ticker_filter",
        )

        st.divider()
        if st.button("🔄 Refresh", width="stretch"):
            st.cache_data.clear()
            st.rerun()

    if selected_tickers:
        df = df[df["Underlying_Ticker"].isin(selected_tickers)]
        if has_doctrine:
            doctrine_df = doctrine_df[doctrine_df["Underlying_Ticker"].isin(selected_tickers)]

    # Filter to OPTION/STOCK
    df_positions = df[df["AssetType"].isin(["OPTION", "STOCK"])].copy() if "AssetType" in df.columns else df.copy()

    # ── Compute idle position count for tab badge ────────────────────────────
    _idle_count = 0
    _critical_count = 0
    if "AssetType" in df_positions.columns:
        _opt_t = set(df_positions.loc[df_positions["AssetType"] == "OPTION", "Underlying_Ticker"].dropna())
        _idle_rows = df_positions[(df_positions["AssetType"] == "STOCK") & ~df_positions["Underlying_Ticker"].isin(_opt_t)]
        _idle_count = len(_idle_rows)
        # Count CRITICAL: loss > 35%
        for _, _ir in _idle_rows.iterrows():
            try:
                _q  = float(_ir.get("Quantity") or 0)
                _b  = float(_ir.get("Basis") or 0)
                _l  = float(_ir.get("Last") or 0)
                _bps= abs(_b / _q) if _q else 0
                if _bps and _l and (_l - _bps) / _bps < -0.35:
                    _critical_count += 1
            except Exception:
                pass

    _idle_tab_label = (
        f"📦 Idle ({_idle_count})" if _idle_count == 0
        else f"📦 Idle ({_idle_count}) 🔴" if _critical_count > 0
        else f"📦 Idle ({_idle_count})"
    )

    # ── Tabs ────────────────────────────────────────────────────────────────
    if has_doctrine:
        tabs = st.tabs(["🧠 Doctrine", "📊 Positions", _idle_tab_label, "🎯 Optimize", "📅 Calendar", "🔢 Greeks", "▶ Run Engine", "🗄️ Raw Data"])
        tab_doctrine, tab_pos, tab_idle, tab_opt, tab_cal, tab_greek, tab_run, tab_raw = tabs
    else:
        tabs = st.tabs(["📊 Positions", _idle_tab_label, "🎯 Optimize", "📅 Calendar", "🔢 Greeks", "▶ Run Engine", "🗄️ Raw Data"])
        tab_doctrine = None
        tab_pos, tab_idle, tab_opt, tab_cal, tab_greek, tab_run, tab_raw = tabs

    if tab_doctrine is not None:
        with tab_doctrine:
            _render_doctrine_recommendations(doctrine_df)

    with tab_pos:
        _render_portfolio_snapshot(
            df_positions,
            doctrine_df=doctrine_df if has_doctrine else None
        )
        st.divider()
        _render_position_cards(
            df_positions,
            show_stocks=False,   # idle stocks have their own tab now
            doctrine_df=doctrine_df if has_doctrine else None,
            db_path=str(PIPELINE_DB_PATH),
        )

    with tab_idle:
        _render_idle_positions_tab(
            df_positions,
            doctrine_df=doctrine_df if has_doctrine else None,
        )

    with tab_opt:
        _render_portfolio_optimization(
            df_positions,
            doctrine_df=doctrine_df if has_doctrine else None,
        )

    with tab_cal:
        _render_expiration_calendar(df_positions)

    with tab_greek:
        _render_greek_breakdown(
            df_positions,
            doctrine_df=doctrine_df if has_doctrine else None
        )

    with tab_run:
        # Find latest uploaded CSV — match both "Accounts" and "Account" variants
        brokerage_dir = Path("data/brokerage_inputs")
        csvs = sorted(
            list(brokerage_dir.glob("Positions_All_Accounts*.csv")) +
            list(brokerage_dir.glob("Positions_All_Account_*.csv")),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        # Deduplicate (a file could match both patterns on some OS)
        seen = set()
        csvs = [p for p in csvs if not (str(p) in seen or seen.add(str(p)))]
        latest_csv = str(csvs[0]) if csvs else ""
        if latest_csv:
            st.caption(f"Will run against: `{Path(latest_csv).name}`")
        else:
            st.warning("No Positions CSV found in `data/brokerage_inputs/`. Upload one in the Perception tab first.")
        _render_run_engine_control(latest_csv)

    with tab_raw:
        st.caption("Verbatim output. All columns shown.")
        st.dataframe(sanitize_func(df), width="stretch")
