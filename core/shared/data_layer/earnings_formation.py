"""
Earnings Formation: Phase 1→2→3 Detection

Detects when the market starts positioning for earnings, not just what
happened on the day.

Three phases:
    - QUIET (D-40 → D-15): flat IV, normal volume
    - POSITIONING (D-15 → D-1): IV ramp, volume surge, price drift
    - EXPLOSION (D-1 → D+5): gap, crush, reversal/continuation

Tables:
    - earnings_formation: daily time-series per event (D-30 to D+5)
    - earnings_formation_summary: per-event summary metrics

DESIGN PRINCIPLES:
    - IV data is sparse (~44 collection dates) — use nearest-reading with
      no look-ahead (window_after=0 for pre-earnings, window_before=0 for post)
    - Price + volume data is dense (~125 daily bars) — no interpolation needed
    - Formation quality reflects IV coverage: COMPLETE ≥15 pts, PARTIAL ≥5, INSUFFICIENT <5
"""

import duckdb
import math
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Table definitions (called by earnings_history.initialize_tables)
# ---------------------------------------------------------------------------

def create_formation_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Create earnings_formation + earnings_formation_summary tables (idempotent)."""

    # Daily time-series per earnings event
    con.execute("""
        CREATE TABLE IF NOT EXISTS earnings_formation (
            ticker          VARCHAR NOT NULL,
            earnings_date   DATE NOT NULL,
            days_relative   INTEGER NOT NULL,
            obs_date        DATE NOT NULL,
            iv_30d          DOUBLE,
            iv_delta_1d     DOUBLE,
            iv_accel        DOUBLE,
            close_price     DOUBLE,
            price_change_1d DOUBLE,
            price_drift_5d  DOUBLE,
            volume          BIGINT,
            volume_ratio    DOUBLE,
            phase_label     VARCHAR,
            PRIMARY KEY (ticker, earnings_date, days_relative)
        )
    """)
    con.execute("""
        CREATE INDEX IF NOT EXISTS idx_formation_ticker_event
        ON earnings_formation(ticker, earnings_date, days_relative)
    """)

    # Per-event summary
    con.execute("""
        CREATE TABLE IF NOT EXISTS earnings_formation_summary (
            ticker                VARCHAR NOT NULL,
            earnings_date         DATE NOT NULL,
            phase2_start_day      INTEGER,
            phase2_iv_velocity    DOUBLE,
            phase2_duration_days  INTEGER,
            price_drift_direction INTEGER,
            drift_predicted_gap   BOOLEAN,
            iv_ramp_magnitude_pct DOUBLE,
            volume_surge_day      INTEGER,
            quiet_baseline_iv     DOUBLE,
            peak_pre_earnings_iv  DOUBLE,
            iv_data_points        INTEGER,
            price_data_points     INTEGER,
            formation_quality     VARCHAR,
            PRIMARY KEY (ticker, earnings_date)
        )
    """)

    logger.info("earnings formation tables initialized")


# ---------------------------------------------------------------------------
# Phase classification
# ---------------------------------------------------------------------------

def _classify_formation_phase(
    days_relative: int,
    iv_delta_1d: Optional[float],
    volume_ratio: Optional[float],
    iv_accel: Optional[float],
) -> str:
    """
    Classify a single day into formation phase.

    Rules:
        POST: days_relative > 0
        EXPLOSION: days_relative == 0
        POSITIONING: D-15 to D-1 AND (iv rising OR volume elevated)
            Override to QUIET if flat IV + low volume
        QUIET: everything else
    """
    if days_relative > 0:
        return "POST"
    if days_relative == 0:
        return "EXPLOSION"

    # D-15 to D-1 window: potential positioning
    if -15 <= days_relative <= -1:
        iv_rising = iv_delta_1d is not None and iv_delta_1d > 0
        vol_elevated = volume_ratio is not None and volume_ratio > 1.2
        accel_positive = iv_accel is not None and iv_accel > 0

        if iv_rising or vol_elevated:
            return "POSITIONING"
        # Even in the window, flat IV + normal volume = still quiet
        return "QUIET"

    return "QUIET"


# ---------------------------------------------------------------------------
# Formation timeseries builder
# ---------------------------------------------------------------------------

def _build_formation_timeseries(
    pipeline_con: duckdb.DuckDBPyConnection,
    iv_con: duckdb.DuckDBPyConnection,
    ticker: str,
    earnings_date: date,
    days_before: int = 30,
    days_after: int = 5,
) -> pd.DataFrame:
    """
    Build D-{days_before} to D+{days_after} daily timeseries for one earnings event.

    Uses dense price_history as the date spine, joins sparse IV data with
    nearest-reading logic (no look-ahead for pre-earnings days).

    Returns DataFrame with columns matching earnings_formation table.
    """
    ed = earnings_date
    if isinstance(ed, datetime):
        ed = ed.date()

    start_date = ed - timedelta(days=days_before + 10)  # extra buffer for weekends
    end_date = ed + timedelta(days=days_after + 5)

    # Get price + volume (dense)
    prices = pipeline_con.execute("""
        SELECT date, close_price, volume
        FROM price_history
        WHERE ticker = ?
          AND date BETWEEN ?::DATE AND ?::DATE
        ORDER BY date
    """, [ticker, start_date, end_date]).df()

    if prices.empty:
        return pd.DataFrame()

    # Normalize dates
    prices["date"] = pd.to_datetime(prices["date"]).dt.date

    # Compute days_relative for each price bar
    prices["days_relative"] = prices["date"].apply(lambda d: (d - ed).days)

    # Filter to desired window
    prices = prices[
        (prices["days_relative"] >= -days_before) &
        (prices["days_relative"] <= days_after)
    ].copy()

    if prices.empty:
        return pd.DataFrame()

    # Get IV data (sparse)
    iv_start = ed - timedelta(days=days_before + 15)
    iv_end = ed + timedelta(days=days_after + 5)
    iv_data = iv_con.execute("""
        SELECT date, iv_30d
        FROM iv_term_history
        WHERE ticker = ?
          AND date BETWEEN ?::DATE AND ?::DATE
          AND iv_30d IS NOT NULL
        ORDER BY date
    """, [ticker, iv_start, iv_end]).df()

    # Build IV lookup by date
    iv_map = {}
    if not iv_data.empty:
        iv_data["date"] = pd.to_datetime(iv_data["date"]).dt.date
        iv_map = dict(zip(iv_data["date"], iv_data["iv_30d"]))

    # For each price bar, find nearest IV (no look-ahead for pre-earnings)
    def _get_iv_for_date(obs_date: date) -> Optional[float]:
        """Get IV for a date, using nearest available with no look-ahead constraint."""
        if obs_date in iv_map:
            return float(iv_map[obs_date])

        # Search within ±5 days, but for pre-earnings: no look-ahead past obs_date
        best_iv = None
        best_gap = 999
        for iv_date, iv_val in iv_map.items():
            gap = abs((iv_date - obs_date).days)
            # No look-ahead: for pre-earnings days, don't use IV from after obs_date
            if obs_date < ed and iv_date > obs_date:
                continue
            if gap < best_gap and gap <= 5:
                best_gap = gap
                best_iv = float(iv_val)
        return best_iv

    # Build rows
    rows = []
    prev_iv = None
    prev_prev_iv = None
    prev_close = None

    # 20-day trailing volume average (for volume_ratio)
    vol_series = prices["volume"].values
    dates_series = prices["date"].values

    # Pre-compute 20-day volume averages
    vol_avg_20d = {}
    all_vols = pipeline_con.execute("""
        SELECT date, volume
        FROM price_history
        WHERE ticker = ?
          AND date BETWEEN ?::DATE AND ?::DATE
        ORDER BY date
    """, [ticker, start_date - timedelta(days=30), end_date]).df()

    if not all_vols.empty:
        all_vols["date"] = pd.to_datetime(all_vols["date"]).dt.date
        all_vols = all_vols.sort_values("date")
        vol_vals = all_vols["volume"].values
        vol_dates = all_vols["date"].values
        for i in range(len(vol_vals)):
            lookback = vol_vals[max(0, i - 20):i]
            if len(lookback) > 0:
                vol_avg_20d[vol_dates[i]] = float(np.mean(lookback))

    for _, row in prices.iterrows():
        obs_date = row["date"]
        days_rel = row["days_relative"]
        close = float(row["close_price"]) if pd.notna(row["close_price"]) else None
        volume = int(row["volume"]) if pd.notna(row["volume"]) else None

        iv = _get_iv_for_date(obs_date)

        # IV delta (1-day change)
        iv_delta = None
        if iv is not None and prev_iv is not None:
            iv_delta = iv - prev_iv

        # IV acceleration (2nd derivative)
        iv_accel = None
        if iv is not None and prev_iv is not None and prev_prev_iv is not None:
            prev_delta = prev_iv - prev_prev_iv
            curr_delta = iv - prev_iv
            iv_accel = curr_delta - prev_delta

        # Price change (1-day)
        price_change = None
        if close is not None and prev_close is not None and prev_close > 0:
            price_change = (close - prev_close) / prev_close

        # Price drift (5-day rolling return)
        price_drift_5d = None
        if len(rows) >= 5:
            close_5d_ago = rows[-5].get("close_price")
            if close is not None and close_5d_ago is not None and close_5d_ago > 0:
                price_drift_5d = (close - close_5d_ago) / close_5d_ago

        # Volume ratio
        vol_ratio = None
        if volume is not None and obs_date in vol_avg_20d and vol_avg_20d[obs_date] > 0:
            vol_ratio = volume / vol_avg_20d[obs_date]

        # Phase classification
        phase = _classify_formation_phase(days_rel, iv_delta, vol_ratio, iv_accel)

        formation_row = {
            "ticker": ticker,
            "earnings_date": ed,
            "days_relative": days_rel,
            "obs_date": obs_date,
            "iv_30d": iv,
            "iv_delta_1d": iv_delta,
            "iv_accel": iv_accel,
            "close_price": close,
            "price_change_1d": price_change,
            "price_drift_5d": price_drift_5d,
            "volume": volume,
            "volume_ratio": vol_ratio,
            "phase_label": phase,
        }
        rows.append(formation_row)

        prev_prev_iv = prev_iv
        prev_iv = iv
        prev_close = close

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Formation summary computation
# ---------------------------------------------------------------------------

def _compute_formation_summary(
    formation_df: pd.DataFrame,
    ticker: str,
    earnings_date: date,
    gap_pct: Optional[float] = None,
) -> Dict:
    """
    Compute per-event summary from formation timeseries.

    Detects:
        - phase2_start_day: when positioning began
        - iv_ramp_magnitude: how much IV rose
        - drift direction: did price drift predict the gap?
        - volume surge: first day volume >1.5x
    """
    ed = earnings_date
    if isinstance(ed, datetime):
        ed = ed.date()

    iv_points = formation_df["iv_30d"].notna().sum()
    price_points = formation_df["close_price"].notna().sum()

    # Formation quality
    if iv_points >= 15:
        quality = "COMPLETE"
    elif iv_points >= 5:
        quality = "PARTIAL"
    else:
        quality = "INSUFFICIENT"

    # Pre-earnings data
    pre = formation_df[formation_df["days_relative"] < 0].copy()
    quiet_zone = pre[pre["days_relative"] <= -15]
    positioning_zone = pre[(pre["days_relative"] > -15) & (pre["days_relative"] < 0)]

    # Quiet baseline IV (median of D-30 to D-15)
    quiet_ivs = quiet_zone["iv_30d"].dropna()
    quiet_baseline = float(quiet_ivs.median()) if len(quiet_ivs) > 0 else None

    # Peak pre-earnings IV (max in D-5 to D-1)
    late_pre = pre[pre["days_relative"] >= -5]
    peak_ivs = late_pre["iv_30d"].dropna()
    peak_iv = float(peak_ivs.max()) if len(peak_ivs) > 0 else None

    # IV ramp magnitude
    ramp_magnitude = None
    if quiet_baseline is not None and peak_iv is not None and quiet_baseline > 0:
        ramp_magnitude = (peak_iv - quiet_baseline) / quiet_baseline

    # Phase 2 start: first day with 3+ consecutive rising IV AND cumulative >3%
    phase2_start = None
    phase2_velocity = None
    phase2_duration = None

    if not pre.empty and iv_points >= 5:
        iv_deltas = pre[["days_relative", "iv_delta_1d", "iv_30d"]].dropna(subset=["iv_delta_1d"])
        if len(iv_deltas) >= 3:
            # Walk forward through pre-earnings, find first 3-day rising streak
            sorted_pre = iv_deltas.sort_values("days_relative")
            consecutive_rising = 0
            streak_start_day = None
            streak_start_iv = None

            for _, r in sorted_pre.iterrows():
                if r["iv_delta_1d"] > 0:
                    if consecutive_rising == 0:
                        streak_start_day = int(r["days_relative"])
                        streak_start_iv = r["iv_30d"] - r["iv_delta_1d"]
                    consecutive_rising += 1

                    # Check cumulative rise > 3%
                    if consecutive_rising >= 3 and streak_start_iv and streak_start_iv > 0:
                        cumulative = (r["iv_30d"] - streak_start_iv) / streak_start_iv
                        if cumulative > 0.03:
                            phase2_start = streak_start_day
                            break
                else:
                    consecutive_rising = 0
                    streak_start_day = None
                    streak_start_iv = None

        # Fallback: first volume_ratio > 1.5 before D-5
        if phase2_start is None:
            vol_surge = pre[
                (pre["days_relative"] < -5) &
                (pre["volume_ratio"].notna()) &
                (pre["volume_ratio"] > 1.5)
            ]
            if not vol_surge.empty:
                phase2_start = int(vol_surge.iloc[0]["days_relative"])

    # Phase 2 velocity and duration
    if phase2_start is not None:
        positioning_data = pre[
            (pre["days_relative"] >= phase2_start) &
            (pre["iv_delta_1d"].notna())
        ]
        if not positioning_data.empty:
            phase2_velocity = float(positioning_data["iv_delta_1d"].mean())
            phase2_duration = len(positioning_data)

    # Volume surge day: first day volume >1.5x in pre-earnings
    volume_surge_day = None
    vol_surges = pre[
        (pre["volume_ratio"].notna()) & (pre["volume_ratio"] > 1.5)
    ]
    if not vol_surges.empty:
        volume_surge_day = int(vol_surges.iloc[0]["days_relative"])

    # Price drift direction
    drift_direction = None
    drift_predicted = None

    if phase2_start is not None:
        start_row = pre[pre["days_relative"] == phase2_start]
        last_pre = pre[pre["days_relative"] == -1]

        if not start_row.empty and not last_pre.empty:
            start_price = start_row.iloc[0]["close_price"]
            end_price = last_pre.iloc[0]["close_price"]

            if pd.notna(start_price) and pd.notna(end_price) and start_price > 0:
                drift = (end_price - start_price) / start_price
                if drift > 0.005:
                    drift_direction = 1
                elif drift < -0.005:
                    drift_direction = -1
                else:
                    drift_direction = 0

                # Check if drift predicted gap
                if gap_pct is not None and drift_direction != 0:
                    gap_direction = 1 if gap_pct > 0 else (-1 if gap_pct < 0 else 0)
                    drift_predicted = (drift_direction == gap_direction)

    return {
        "ticker": ticker,
        "earnings_date": ed,
        "phase2_start_day": phase2_start,
        "phase2_iv_velocity": phase2_velocity,
        "phase2_duration_days": phase2_duration,
        "price_drift_direction": drift_direction,
        "drift_predicted_gap": drift_predicted,
        "iv_ramp_magnitude_pct": ramp_magnitude,
        "volume_surge_day": volume_surge_day,
        "quiet_baseline_iv": quiet_baseline,
        "peak_pre_earnings_iv": peak_iv,
        "iv_data_points": int(iv_points),
        "price_data_points": int(price_points),
        "formation_quality": quality,
    }


# ---------------------------------------------------------------------------
# Orchestrator: compute formation for a single event
# ---------------------------------------------------------------------------

def compute_formation_for_event(
    pipeline_con: duckdb.DuckDBPyConnection,
    iv_con: duckdb.DuckDBPyConnection,
    ticker: str,
    earnings_date: date,
) -> Optional[Dict]:
    """
    Build formation timeseries + summary for one earnings event.
    Upserts to both earnings_formation and earnings_formation_summary tables.
    Returns summary dict or None if insufficient data.
    """
    ed = earnings_date
    if isinstance(ed, datetime):
        ed = ed.date()

    # Build timeseries
    formation_df = _build_formation_timeseries(pipeline_con, iv_con, ticker, ed)
    if formation_df.empty:
        logger.debug(f"No formation data for {ticker} {ed}")
        return None

    # Get gap_pct from earnings_iv_crush (if available)
    gap_pct = None
    try:
        crush_row = pipeline_con.execute("""
            SELECT gap_pct FROM earnings_iv_crush
            WHERE ticker = ? AND earnings_date = ?
        """, [ticker, ed]).fetchone()
        if crush_row and crush_row[0] is not None:
            gap_pct = float(crush_row[0])
    except Exception:
        pass  # Table may not exist yet

    # Compute summary
    summary = _compute_formation_summary(formation_df, ticker, ed, gap_pct=gap_pct)

    # Upsert timeseries rows
    df_ts = formation_df.copy()
    # Ensure proper types for DuckDB
    for col in ["iv_30d", "iv_delta_1d", "iv_accel", "close_price",
                "price_change_1d", "price_drift_5d", "volume_ratio"]:
        if col in df_ts.columns:
            df_ts[col] = df_ts[col].astype("Float64")

    if "volume" in df_ts.columns:
        df_ts["volume"] = df_ts["volume"].astype("Int64")

    pipeline_con.execute("""
        DELETE FROM earnings_formation
        WHERE ticker = ? AND earnings_date = ?
    """, [ticker, ed])

    pipeline_con.execute("""
        INSERT INTO earnings_formation
            (ticker, earnings_date, days_relative, obs_date, iv_30d,
             iv_delta_1d, iv_accel, close_price, price_change_1d,
             price_drift_5d, volume, volume_ratio, phase_label)
        SELECT ticker, earnings_date, days_relative, obs_date, iv_30d,
               iv_delta_1d, iv_accel, close_price, price_change_1d,
               price_drift_5d, volume, volume_ratio, phase_label
        FROM df_ts
    """)

    # Upsert summary
    df_summ = pd.DataFrame([summary])
    pipeline_con.execute("""
        INSERT INTO earnings_formation_summary
            (ticker, earnings_date, phase2_start_day, phase2_iv_velocity,
             phase2_duration_days, price_drift_direction, drift_predicted_gap,
             iv_ramp_magnitude_pct, volume_surge_day, quiet_baseline_iv,
             peak_pre_earnings_iv, iv_data_points, price_data_points,
             formation_quality)
        SELECT ticker, earnings_date, phase2_start_day, phase2_iv_velocity,
               phase2_duration_days, price_drift_direction, drift_predicted_gap,
               iv_ramp_magnitude_pct, volume_surge_day, quiet_baseline_iv,
               peak_pre_earnings_iv, iv_data_points, price_data_points,
               formation_quality
        FROM df_summ
        ON CONFLICT (ticker, earnings_date) DO UPDATE SET
            phase2_start_day = EXCLUDED.phase2_start_day,
            phase2_iv_velocity = EXCLUDED.phase2_iv_velocity,
            phase2_duration_days = EXCLUDED.phase2_duration_days,
            price_drift_direction = EXCLUDED.price_drift_direction,
            drift_predicted_gap = EXCLUDED.drift_predicted_gap,
            iv_ramp_magnitude_pct = EXCLUDED.iv_ramp_magnitude_pct,
            volume_surge_day = EXCLUDED.volume_surge_day,
            quiet_baseline_iv = EXCLUDED.quiet_baseline_iv,
            peak_pre_earnings_iv = EXCLUDED.peak_pre_earnings_iv,
            iv_data_points = EXCLUDED.iv_data_points,
            price_data_points = EXCLUDED.price_data_points,
            formation_quality = EXCLUDED.formation_quality
    """)

    logger.info(
        f"Formation computed for {ticker} {ed}: "
        f"quality={summary['formation_quality']}, "
        f"phase2_start=D{summary['phase2_start_day']}, "
        f"iv_pts={summary['iv_data_points']}"
    )
    return summary


# ---------------------------------------------------------------------------
# Forward detection: is a ticker currently in positioning phase?
# ---------------------------------------------------------------------------

def detect_current_phase(
    pipeline_con: duckdb.DuckDBPyConnection,
    iv_con: duckdb.DuckDBPyConnection,
    ticker: str,
    as_of_date: date,
    next_earnings_date: Optional[date] = None,
) -> Optional[Dict]:
    """
    Detect if a ticker is currently in an earnings positioning phase.

    Queries recent IV (last 20d) + price, compares to historical patterns.

    Returns dict with:
        phase: QUIET | EARLY_POSITIONING | LATE_POSITIONING | IMMINENT | NO_UPCOMING
        days_to_earnings: int or None
        iv_velocity: avg daily IV change over last 5d
        confidence: LOW | MEDIUM | HIGH
    """
    aod = as_of_date
    if isinstance(aod, datetime):
        aod = aod.date()

    # Find next earnings date if not provided
    if next_earnings_date is None:
        try:
            row = pipeline_con.execute("""
                SELECT earnings_date FROM earnings_history
                WHERE ticker = ? AND earnings_date > ?
                ORDER BY earnings_date ASC LIMIT 1
            """, [ticker, aod]).fetchone()
            if row:
                next_earnings_date = row[0]
                if isinstance(next_earnings_date, datetime):
                    next_earnings_date = next_earnings_date.date()
        except Exception:
            pass

        # Also check earnings_calendar
        if next_earnings_date is None:
            try:
                row = pipeline_con.execute("""
                    SELECT next_earnings_date FROM earnings_calendar
                    WHERE ticker = ? AND next_earnings_date > ?
                    ORDER BY next_earnings_date ASC LIMIT 1
                """, [ticker, aod]).fetchone()
                if row and row[0] is not None:
                    next_earnings_date = row[0]
                    if isinstance(next_earnings_date, datetime):
                        next_earnings_date = next_earnings_date.date()
            except Exception:
                pass

    if next_earnings_date is None:
        return {
            "phase": "NO_UPCOMING",
            "days_to_earnings": None,
            "iv_velocity": None,
            "confidence": "LOW",
        }

    days_to = (next_earnings_date - aod).days
    if days_to < 0:
        return {
            "phase": "NO_UPCOMING",
            "days_to_earnings": None,
            "iv_velocity": None,
            "confidence": "LOW",
        }

    # Get recent IV readings (last 20d)
    iv_start = aod - timedelta(days=25)
    iv_readings = iv_con.execute("""
        SELECT date, iv_30d
        FROM iv_term_history
        WHERE ticker = ?
          AND date BETWEEN ?::DATE AND ?::DATE
          AND iv_30d IS NOT NULL
        ORDER BY date DESC
    """, [ticker, iv_start, aod]).df()

    iv_velocity = None
    if not iv_readings.empty and len(iv_readings) >= 2:
        # Last 5 readings: compute avg daily change
        recent = iv_readings.head(min(5, len(iv_readings)))
        if len(recent) >= 2:
            ivs = recent["iv_30d"].values
            deltas = [ivs[i] - ivs[i + 1] for i in range(len(ivs) - 1)]
            iv_velocity = float(np.mean(deltas))

    # Get historical average ramp from earnings_formation_summary
    avg_ramp_start = None
    try:
        hist = pipeline_con.execute("""
            SELECT AVG(phase2_start_day) AS avg_start
            FROM earnings_formation_summary
            WHERE ticker = ?
              AND phase2_start_day IS NOT NULL
              AND formation_quality IN ('COMPLETE', 'PARTIAL')
        """, [ticker]).fetchone()
        if hist and hist[0] is not None:
            avg_ramp_start = abs(float(hist[0]))
    except Exception:
        pass

    # Classification
    if days_to <= 1:
        phase = "IMMINENT"
        confidence = "HIGH"
    elif days_to <= 5:
        phase = "LATE_POSITIONING"
        confidence = "HIGH" if (iv_velocity is not None and iv_velocity > 0.3) else "MEDIUM"
    elif days_to <= 15:
        # In the typical positioning window
        if iv_velocity is not None and iv_velocity > 0.2:
            phase = "EARLY_POSITIONING"
            confidence = "MEDIUM"
        elif avg_ramp_start is not None and days_to <= avg_ramp_start:
            phase = "EARLY_POSITIONING"
            confidence = "LOW"
        else:
            phase = "QUIET"
            confidence = "MEDIUM"
    elif days_to <= 45:
        phase = "QUIET"
        confidence = "HIGH" if days_to > 30 else "MEDIUM"
    else:
        return {
            "phase": "NO_UPCOMING",
            "days_to_earnings": days_to,
            "iv_velocity": iv_velocity,
            "confidence": "LOW",
        }

    return {
        "phase": phase,
        "days_to_earnings": days_to,
        "iv_velocity": iv_velocity,
        "confidence": confidence,
    }


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def get_formation_timeseries(
    con: duckdb.DuckDBPyConnection,
    ticker: str,
    earnings_date: date,
) -> pd.DataFrame:
    """Return formation timeseries for a specific earnings event."""
    return con.execute("""
        SELECT * FROM earnings_formation
        WHERE ticker = ? AND earnings_date = ?
        ORDER BY days_relative
    """, [ticker, earnings_date]).df()


def get_formation_summary(
    con: duckdb.DuckDBPyConnection,
    ticker: str,
    earnings_date: Optional[date] = None,
) -> pd.DataFrame:
    """Return formation summaries. If no date, return all events for ticker."""
    if earnings_date:
        return con.execute("""
            SELECT * FROM earnings_formation_summary
            WHERE ticker = ? AND earnings_date = ?
        """, [ticker, earnings_date]).df()
    return con.execute("""
        SELECT * FROM earnings_formation_summary
        WHERE ticker = ?
        ORDER BY earnings_date DESC
    """, [ticker]).df()


def get_avg_formation_stats(
    con: duckdb.DuckDBPyConnection,
    ticker: str,
) -> Optional[Dict]:
    """
    Return averaged formation stats across all events for a ticker.
    Used by management engine for enrichment.
    """
    rows = con.execute("""
        SELECT
            AVG(phase2_start_day) AS avg_phase2_start,
            AVG(phase2_iv_velocity) AS avg_phase2_velocity,
            AVG(phase2_duration_days) AS avg_phase2_duration,
            AVG(iv_ramp_magnitude_pct) AS avg_ramp_magnitude,
            SUM(CASE WHEN drift_predicted_gap = TRUE THEN 1 ELSE 0 END) AS drift_correct,
            COUNT(CASE WHEN drift_predicted_gap IS NOT NULL THEN 1 END) AS drift_total,
            COUNT(*) AS event_count
        FROM earnings_formation_summary
        WHERE ticker = ?
          AND formation_quality IN ('COMPLETE', 'PARTIAL')
    """, [ticker]).fetchone()

    if rows is None or rows[6] == 0:
        return None

    drift_rate = float(rows[4]) / float(rows[5]) if rows[5] and rows[5] > 0 else None

    return {
        "avg_phase2_start_day": float(rows[0]) if rows[0] is not None else None,
        "avg_phase2_velocity": float(rows[1]) if rows[1] is not None else None,
        "avg_phase2_duration": float(rows[2]) if rows[2] is not None else None,
        "avg_ramp_magnitude_pct": float(rows[3]) if rows[3] is not None else None,
        "drift_predicted_gap_rate": drift_rate,
        "event_count": int(rows[6]),
    }
