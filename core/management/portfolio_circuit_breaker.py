"""
Portfolio Circuit Breaker — Emergency halt mechanism for portfolio-level distress.

Monitors aggregate portfolio health and triggers emergency overrides when
drawdown, simultaneous distress, or Greek exposure exceeds safe thresholds.

States:
    OPEN      — Normal operation, entries allowed
    WARNING   — Approaching distress threshold (75% of trigger), caution
    TRIPPED   — Portfolio-level distress, force EXIT CRITICAL on all positions

Triggers (any one trips the breaker):
    1. Drawdown > 8% from peak equity
    2. >3 simultaneous EXIT CRITICAL recommendations from doctrine
    3. Portfolio net delta > 2× conservative limit
    4. CRISIS market stress + unrealized portfolio loss > 5%

Cooldown: 1-day cooldown after conditions clear before returning to OPEN.

References:
    - McMillan Ch.3: Portfolio-level risk control
    - Passarelli Ch.6: Emergency position management
    - Natenberg Ch.19: Portfolio stress testing
"""

import pandas as pd
import numpy as np
import logging
from typing import Tuple, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


# --- Thresholds ---
DRAWDOWN_TRIP_PCT = 0.08       # 8% drawdown from peak → TRIPPED
DRAWDOWN_WARNING_PCT = 0.06    # 6% drawdown from peak → WARNING (75% of trip)
CRITICAL_EXIT_TRIP_COUNT = 3   # >3 simultaneous CRITICAL exits → TRIPPED
DELTA_MULTIPLIER_TRIP = 2.0    # Delta > 2× conservative limit → TRIPPED
CRISIS_LOSS_TRIP_PCT = 0.05    # 5% unrealized loss + CRISIS market → TRIPPED
COOLDOWN_HOURS = 24            # Hours after conditions clear before OPEN


def check_circuit_breaker(
    df_positions: pd.DataFrame,
    account_balance: float = 100_000.0,
    peak_equity: Optional[float] = None,
    market_stress_level: str = "NORMAL",
    prior_breaker_state: str = "OPEN",
    prior_breaker_tripped_at: Optional[datetime] = None,
) -> Tuple[str, str]:
    """
    Check portfolio-level circuit breaker conditions.

    Args:
        df_positions: DataFrame with position-level data (after Cycle 2, before doctrine).
                      Expected columns: Action, Urgency, Delta, '$ Total G/L', UL Last
        account_balance: Total account equity
        peak_equity: Historical peak equity (for drawdown calculation).
                     If None, drawdown check is skipped.
        market_stress_level: Market stress from classify_market_stress()
                             ("NORMAL", "ELEVATED", "HIGH", "CRISIS")
        prior_breaker_state: State from previous run ("OPEN", "WARNING", "TRIPPED")
        prior_breaker_tripped_at: Timestamp when breaker last tripped (for cooldown)

    Returns:
        Tuple of (state, reason):
            state: "OPEN" | "WARNING" | "TRIPPED"
            reason: Human-readable explanation of the state
    """
    if df_positions.empty:
        return "OPEN", "No positions — breaker inactive."

    reasons_trip = []
    reasons_warn = []

    # --- Trigger 1: Drawdown from peak equity ---
    if peak_equity is not None and peak_equity > 0:
        drawdown_pct = (peak_equity - account_balance) / peak_equity
        if drawdown_pct >= DRAWDOWN_TRIP_PCT:
            reasons_trip.append(
                f"Drawdown {drawdown_pct:.1%} exceeds {DRAWDOWN_TRIP_PCT:.0%} threshold "
                f"(peak=${peak_equity:,.0f}, current=${account_balance:,.0f})"
            )
        elif drawdown_pct >= DRAWDOWN_WARNING_PCT:
            reasons_warn.append(
                f"Drawdown {drawdown_pct:.1%} approaching trip at {DRAWDOWN_TRIP_PCT:.0%}"
            )

    # --- Trigger 2: Simultaneous CRITICAL exits ---
    if 'Action' in df_positions.columns and 'Urgency' in df_positions.columns:
        critical_exit_mask = (
            (df_positions['Action'] == 'EXIT')
            & (df_positions['Urgency'] == 'CRITICAL')
        )
        critical_exit_count = int(critical_exit_mask.sum())
        if critical_exit_count > CRITICAL_EXIT_TRIP_COUNT:
            reasons_trip.append(
                f"{critical_exit_count} simultaneous EXIT CRITICAL signals "
                f"(threshold: >{CRITICAL_EXIT_TRIP_COUNT})"
            )
        elif critical_exit_count == CRITICAL_EXIT_TRIP_COUNT:
            reasons_warn.append(
                f"{critical_exit_count} EXIT CRITICAL signals — one more trips breaker"
            )

    # --- Trigger 3: Portfolio delta > 2× conservative limit ---
    delta_col = 'Portfolio_Net_Delta' if 'Portfolio_Net_Delta' in df_positions.columns else 'Delta'
    if delta_col in df_positions.columns:
        if delta_col == 'Portfolio_Net_Delta':
            # Portfolio-level already aggregated — take first non-null value
            net_delta = df_positions[delta_col].dropna().iloc[0] if df_positions[delta_col].notna().any() else 0.0
        else:
            net_delta = float(df_positions['Delta'].fillna(0).sum())

        # Conservative limit: 50 delta per $100k, scaled
        scale = account_balance / 100_000.0
        conservative_delta_limit = 50.0 * scale
        delta_ratio = abs(net_delta) / max(conservative_delta_limit, 1.0)

        if delta_ratio > DELTA_MULTIPLIER_TRIP:
            reasons_trip.append(
                f"Portfolio |delta|={abs(net_delta):.1f} exceeds "
                f"{DELTA_MULTIPLIER_TRIP:.0f}× limit ({conservative_delta_limit:.0f})"
            )
        elif delta_ratio > DELTA_MULTIPLIER_TRIP * 0.75:
            reasons_warn.append(
                f"Portfolio |delta|={abs(net_delta):.1f} at "
                f"{delta_ratio:.1f}× limit (trip at {DELTA_MULTIPLIER_TRIP:.0f}×)"
            )

    # --- Trigger 4: CRISIS market + unrealized loss > 5% ---
    if market_stress_level == "CRISIS":
        gl_col = '$ Total G/L' if '$ Total G/L' in df_positions.columns else 'Total_GL_Decimal'
        if gl_col in df_positions.columns:
            total_unrealized = float(df_positions[gl_col].fillna(0).sum())
            loss_pct = abs(total_unrealized) / max(account_balance, 1.0) if total_unrealized < 0 else 0.0
            if loss_pct >= CRISIS_LOSS_TRIP_PCT:
                reasons_trip.append(
                    f"CRISIS market + unrealized loss {loss_pct:.1%} "
                    f"(threshold: {CRISIS_LOSS_TRIP_PCT:.0%})"
                )
            elif loss_pct >= CRISIS_LOSS_TRIP_PCT * 0.75:
                reasons_warn.append(
                    f"CRISIS market + unrealized loss {loss_pct:.1%} approaching trip"
                )
        else:
            # CRISIS alone is a warning
            reasons_warn.append("CRISIS market detected — monitoring for loss threshold")

    # --- Trigger 5: Term-Structure Vega Imbalance (Natenberg Ch.7, 0.796) ---
    # "A portfolio with net vega near zero can still have enormous vol-structure
    # risk if the vega is distributed across expiration months."
    # Group options by DTE bucket, compute signed position-vega per bucket.
    # If offsetting buckets exist AND single-bucket magnitude is large → WARNING.
    if 'Vega' in df_positions.columns and 'DTE' in df_positions.columns:
        _at_col = 'AssetType' if 'AssetType' in df_positions.columns else None
        _opt_mask = (
            df_positions[_at_col].isin(['OPTION', 'CALL', 'PUT'])
            if _at_col else pd.Series(True, index=df_positions.index)
        )
        _opt_df = df_positions.loc[_opt_mask].copy()
        if len(_opt_df) >= 2:
            _opt_df['_vega'] = pd.to_numeric(_opt_df['Vega'], errors='coerce').fillna(0)
            _opt_df['_dte'] = pd.to_numeric(
                _opt_df.get('Short_Call_DTE', _opt_df['DTE']),
                errors='coerce',
            ).fillna(30)
            _opt_df['_qty'] = pd.to_numeric(_opt_df['Quantity'], errors='coerce').fillna(0)
            # Position vega = per-share vega × quantity × 100 (per contract)
            _opt_df['_pos_vega'] = _opt_df['_vega'] * _opt_df['_qty'] * 100

            _opt_df['_bucket'] = pd.cut(
                _opt_df['_dte'],
                bins=[-1, 30, 60, 90, 9999],
                labels=['0-30d', '30-60d', '60-90d', '90+d'],
            )
            _bucket_vega = _opt_df.groupby('_bucket', observed=True)['_pos_vega'].sum()
            _has_long = (_bucket_vega > 0).any()
            _has_short = (_bucket_vega < 0).any()

            if _has_long and _has_short:
                _max_bucket_mag = float(_bucket_vega.abs().max())
                _net_vega = float(_bucket_vega.sum())
                # Per-$100k threshold: $300 single-bucket vega = 3% P&L per 1% IV shift
                _vega_limit = 300.0 * (account_balance / 100_000.0)
                _imbalance_summary = ", ".join(
                    f"{k}: {v:+.0f}" for k, v in _bucket_vega.items() if abs(v) > 1
                )

                if _max_bucket_mag > _vega_limit * 2:
                    reasons_trip.append(
                        f"Term-structure vega imbalance: offsetting vega across expiries "
                        f"(buckets: {_imbalance_summary}). "
                        f"Max bucket ${_max_bucket_mag:.0f} > 2x limit ${_vega_limit:.0f}. "
                        f"Net vega ${_net_vega:+.0f} masks ${_max_bucket_mag:.0f} gross risk "
                        f"(Natenberg Ch.7: vol-curve shift = directional risk in disguise)"
                    )
                elif _max_bucket_mag > _vega_limit:
                    reasons_warn.append(
                        f"Term-structure vega imbalance: offsetting vega across expiries "
                        f"(buckets: {_imbalance_summary}). "
                        f"Max bucket ${_max_bucket_mag:.0f} > limit ${_vega_limit:.0f}. "
                        f"Net vega ${_net_vega:+.0f} — term-structure shift risk present "
                        f"(Natenberg Ch.7)"
                    )

    # --- State determination ---
    if reasons_trip:
        state = "TRIPPED"
        reason = "CIRCUIT BREAKER TRIPPED: " + "; ".join(reasons_trip)
        logger.critical(f"🚨 {reason}")
        return state, reason

    # Cooldown check: if previously tripped, stay TRIPPED until cooldown expires
    if prior_breaker_state == "TRIPPED" and prior_breaker_tripped_at is not None:
        elapsed = datetime.utcnow() - prior_breaker_tripped_at
        if elapsed < timedelta(hours=COOLDOWN_HOURS):
            remaining = COOLDOWN_HOURS - (elapsed.total_seconds() / 3600)
            reason = (
                f"Cooldown active — breaker tripped {elapsed.total_seconds()/3600:.1f}h ago, "
                f"{remaining:.1f}h remaining before OPEN"
            )
            logger.warning(f"⏳ {reason}")
            return "TRIPPED", reason

    if reasons_warn:
        state = "WARNING"
        reason = "Portfolio stress warning: " + "; ".join(reasons_warn)
        logger.warning(f"⚠️ {reason}")
        return state, reason

    return "OPEN", "All portfolio risk metrics within normal bounds."


def persist_equity_curve(
    conn,
    account_balance: float,
    peak_equity: float,
    circuit_breaker_state: str,
    positions_count: int,
) -> float:
    """
    Persist daily equity curve entry to DuckDB for drawdown tracking.

    Args:
        conn: DuckDB connection
        account_balance: Current account equity
        peak_equity: Historical peak equity
        circuit_breaker_state: Current breaker state
        positions_count: Number of active positions

    Returns:
        Updated peak equity (max of prior peak and current balance)
    """
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS portfolio_equity_curve (
                snapshot_date DATE PRIMARY KEY,
                peak_equity DOUBLE,
                current_equity DOUBLE,
                drawdown_pct DOUBLE,
                circuit_breaker_state VARCHAR,
                positions_count INTEGER
            )
        """)

        new_peak = max(peak_equity, account_balance) if peak_equity else account_balance
        drawdown = (new_peak - account_balance) / new_peak if new_peak > 0 else 0.0
        today = datetime.utcnow().strftime('%Y-%m-%d')

        conn.execute("""
            INSERT OR REPLACE INTO portfolio_equity_curve
            (snapshot_date, peak_equity, current_equity, drawdown_pct,
             circuit_breaker_state, positions_count)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [today, new_peak, account_balance, drawdown,
              circuit_breaker_state, positions_count])

        logger.info(
            f"[EquityCurve] {today}: equity=${account_balance:,.0f}, "
            f"peak=${new_peak:,.0f}, dd={drawdown:.2%}, state={circuit_breaker_state}"
        )
        return new_peak

    except Exception as e:
        logger.warning(f"⚠️ Equity curve persistence failed (non-fatal): {e}")
        return peak_equity or account_balance


def load_peak_equity(conn) -> Tuple[Optional[float], Optional[datetime]]:
    """
    Load peak equity and last trip timestamp from DuckDB.

    Returns:
        Tuple of (peak_equity, last_tripped_at)
    """
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS portfolio_equity_curve (
                snapshot_date DATE PRIMARY KEY,
                peak_equity DOUBLE,
                current_equity DOUBLE,
                drawdown_pct DOUBLE,
                circuit_breaker_state VARCHAR,
                positions_count INTEGER
            )
        """)

        result = conn.execute("""
            SELECT peak_equity, circuit_breaker_state, snapshot_date
            FROM portfolio_equity_curve
            ORDER BY snapshot_date DESC
            LIMIT 1
        """).fetchone()

        if result is None:
            return None, None

        peak = float(result[0]) if result[0] is not None else None
        state = result[1]
        snap_date = result[2]

        # Find last TRIPPED timestamp
        tripped_at = None
        if state == "TRIPPED" and snap_date is not None:
            tripped_at = datetime.combine(snap_date, datetime.min.time())

        return peak, tripped_at

    except Exception as e:
        logger.warning(f"⚠️ Peak equity load failed (non-fatal): {e}")
        return None, None
