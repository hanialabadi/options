"""
Position Trajectory — Lifecycle-Aware Regime Classification

Distinguishes "sideways income" from "chasing strikes" by analysing three
data streams from DuckDB:

  1. Stock price trajectory from entry to now  (price_history table)
  2. Roll history — consecutive debits, cost   (premium_ledger table)
  3. IV trajectory from entry to now           (iv_term_history table)

Output columns (12 total):

  Position_Regime                       — SIDEWAYS_INCOME | TRENDING_CHASE |
                                          RECOVERY_GRIND  | MEAN_REVERSION | NEUTRAL
  Position_Regime_Reason                — collapsed signal list
  Trajectory_Stock_Return               — (current − entry) / entry
  Trajectory_MFE                        — max favorable excursion since entry
  Trajectory_MAE                        — max adverse excursion since entry
  Trajectory_Range_Ratio                — (max_close − min_close) / entry
  Trajectory_Strike_Crossings           — sign changes of (close − strike)
  Trajectory_Slope                      — linregress slope / entry price (per day)
  Trajectory_Consecutive_Debit_Rolls    — count from tail where net < 0
  Trajectory_Roll_Efficiency_Trend      — IMPROVING | STABLE | DEGRADING
  Trajectory_Total_Roll_Cost            — sum of close_costs across all cycles
  Trajectory_IV_Change                  — COMPRESSED | EXPANDED | STABLE

Design contract:
  - Follows compute_equity_integrity pattern: takes df, adds columns, returns df.
  - Fires on positions with Entry_Snapshot_TS populated.  Propagates to all
    legs of the same TradeID.
  - Non-blocking: any per-row exception → NEUTRAL (never halts pipeline).
  - Uses 3 batched DuckDB queries (not per-position) for performance.
  - Direction-aware: BUY_WRITE/CC trend UP = chase; SHORT_PUT trend DOWN = chase.

McMillan Ch.3: Income overlay requires range-bound or mean-reverting behaviour.
  When stock trends structurally, the covered call caps participation and each
  roll chases the next strike — a losing game.

Natenberg Ch.8: Strategy-regime fit — volatility strategies must match the
  underlying's realized behaviour, not just its current snapshot.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

import duckdb
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Thresholds ────────────────────────────────────────────────────────────────

_MIN_BARS = 10             # minimum price bars since entry for classification
_SLOPE_FLAT = 0.0005       # |slope/entry| per day — below = flat
_SLOPE_STRONG = 0.002      # |slope/entry| per day — above = strong trend
_RANGE_NARROW = 0.12       # (max − min) / entry — below = sideways
_RANGE_WIDE = 0.20         # above = trending or volatile
_STRIKE_CROSS_MANY = 4     # many crossings = range-bound
_RETURN_SMALL = 0.03       # |return| < 3% = near entry
_RETURN_LARGE = 0.10       # |return| > 10% = significant move
_EXCURSION_MFE_LARGE = 0.15
_EXCURSION_MAE_SMALL = -0.05
_EXCURSION_BOTH = 0.10     # both MFE > 10% and MAE < -10% = mean-reverting
_RECOVERY_DROP = -0.05     # stock dropped > 5% but recovering
_IV_CHANGE_THRESHOLD = 0.15  # 15% relative change = material

_TICKER_ACCT_RE = re.compile(r'^([A-Z]+)\d{6}.*?(\d{4})(?:_.*)?$')

# Strategies where UP trend = chase (short call being outrun)
_CALL_CHASE_STRATEGIES = {
    "BUY_WRITE", "COVERED_CALL", "CC", "BW",
}
# Strategies where DOWN trend = chase (short put being crashed into)
_PUT_CHASE_STRATEGIES = {
    "SHORT_PUT", "CSP", "CASH_SECURED_PUT",
}


def compute_position_trajectory(
    df: pd.DataFrame,
    con: Optional[duckdb.DuckDBPyConnection] = None,
    db_path: str = "data/pipeline.duckdb",
) -> pd.DataFrame:
    """
    Add Position_Regime and 11 companion columns to *df*.

    Parameters
    ----------
    df : pd.DataFrame
        The enriched position DataFrame (after entry anchors + premium ledger).
    con : duckdb.DuckDBPyConnection, optional
        Existing read-only connection to pipeline.duckdb.  One is opened
        transiently if not supplied.
    db_path : str
        Path to pipeline.duckdb (contains price_history, premium_ledger).
    """
    if df.empty:
        return df

    df = df.copy()

    # Initialize all output columns
    for col in _OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan if col not in ("Position_Regime", "Position_Regime_Reason",
                                            "Trajectory_Roll_Efficiency_Trend",
                                            "Trajectory_IV_Change") else ""

    # ── Identify scorable rows ────────────────────────────────────────────────
    # Need Entry_Snapshot_TS + Underlying_Ticker to query trajectory
    _has_entry = (
        df.get("Entry_Snapshot_TS", pd.Series(dtype="object")).notna()
        & df.get("Underlying_Ticker", pd.Series(dtype="object")).notna()
    )
    if not _has_entry.any():
        df["Position_Regime"] = "NEUTRAL"
        return df

    # ── Batch data loads ──────────────────────────────────────────────────────
    _own_con = False
    try:
        if con is None:
            con = duckdb.connect(db_path, read_only=True)
            _own_con = True

        # Collect unique (ticker, entry_date) pairs
        _pairs = (
            df.loc[_has_entry, ["Underlying_Ticker", "Entry_Snapshot_TS"]]
            .drop_duplicates("Underlying_Ticker")
        )
        _tickers = _pairs["Underlying_Ticker"].tolist()

        price_cache = _batch_load_prices(con, _pairs)
        roll_cache = _batch_load_roll_history(con, df)
        iv_cache = _batch_load_iv_trajectory(con, _pairs, db_path)

    except Exception as e:
        logger.warning(f"[PositionTrajectory] batch load failed (non-fatal): {e}")
        price_cache, roll_cache, iv_cache = {}, {}, {}
    finally:
        if _own_con and con is not None:
            try:
                con.close()
            except Exception:
                pass

    # ── Per-row scoring ───────────────────────────────────────────────────────
    for idx in df.index[_has_entry]:
        try:
            row = df.loc[idx]
            ticker = str(row.get("Underlying_Ticker", ""))
            strategy = str(row.get("Strategy", "") or row.get("Entry_Structure", "") or "").upper()
            entry_price = _float(row.get("Underlying_Price_Entry"))
            current_price = _float(row.get("UL Last"))
            current_strike = _float(row.get("Strike"))

            if not ticker or entry_price is None or entry_price <= 0:
                df.at[idx, "Position_Regime"] = "NEUTRAL"
                continue

            spot = current_price if current_price and current_price > 0 else entry_price

            # Stock trajectory
            stock = _compute_stock_metrics(
                price_cache.get(ticker, []),
                entry_price, spot, current_strike,
            )

            # Roll history — from premium_ledger + live DataFrame fallback
            tid = str(row.get("TradeID", "") or "")
            roll = roll_cache.get(_ticker_acct_key(tid), _EMPTY_ROLL).copy()

            # Augment with live Roll_Net_Credit from the DataFrame row.
            # The premium_ledger table may have close_cost=0 when the debit
            # was computed live by the Cycle 2 drift engine (Roll_Net_Credit column)
            # rather than captured from the Fidelity activity export.
            _live_rnc = _float(row.get("Roll_Net_Credit"))
            if _live_rnc is not None and _live_rnc < -0.005 and roll.get("consecutive_debits", 0) == 0:
                # The live data shows a debit roll but ledger didn't capture it
                roll["consecutive_debits"] = max(roll.get("consecutive_debits", 0), 1)
                _live_cc = abs(_live_rnc)
                roll["total_close_cost"] = max(roll.get("total_close_cost", 0.0), _live_cc)

            # IV trajectory
            iv_change = iv_cache.get(ticker, "STABLE")

            # Classify
            regime, reason = _classify_regime(stock, roll, iv_change, strategy)

            # Write columns
            df.at[idx, "Position_Regime"] = regime
            df.at[idx, "Position_Regime_Reason"] = reason
            df.at[idx, "Trajectory_Stock_Return"] = stock.get("return", np.nan)
            df.at[idx, "Trajectory_MFE"] = stock.get("mfe", np.nan)
            df.at[idx, "Trajectory_MAE"] = stock.get("mae", np.nan)
            df.at[idx, "Trajectory_Range_Ratio"] = stock.get("range_ratio", np.nan)
            df.at[idx, "Trajectory_Strike_Crossings"] = stock.get("strike_crossings", 0)
            df.at[idx, "Trajectory_Slope"] = stock.get("slope", np.nan)
            df.at[idx, "Trajectory_Consecutive_Debit_Rolls"] = roll.get("consecutive_debits", 0)
            df.at[idx, "Trajectory_Roll_Efficiency_Trend"] = roll.get("efficiency_trend", "STABLE")
            df.at[idx, "Trajectory_Total_Roll_Cost"] = roll.get("total_close_cost", 0.0)
            df.at[idx, "Trajectory_IV_Change"] = iv_change

        except Exception as e:
            logger.debug(f"[PositionTrajectory] row {idx} failed (non-fatal): {e}")
            df.at[idx, "Position_Regime"] = "NEUTRAL"

    # Fill any remaining NaN regimes
    df["Position_Regime"] = df["Position_Regime"].fillna("NEUTRAL").replace("", "NEUTRAL")

    # ── Propagate regime to all legs of same TradeID ──────────────────────────
    _propagate_to_legs(df)

    # ── Summary logging ───────────────────────────────────────────────────────
    _counts = df["Position_Regime"].value_counts()
    _non_neutral = {k: v for k, v in _counts.items() if k != "NEUTRAL"}
    if _non_neutral:
        logger.info(f"[PositionTrajectory] {_non_neutral}")

    return df


# ── Output column names ──────────────────────────────────────────────────────

_OUTPUT_COLUMNS = [
    "Position_Regime", "Position_Regime_Reason",
    "Trajectory_Stock_Return", "Trajectory_MFE", "Trajectory_MAE",
    "Trajectory_Range_Ratio", "Trajectory_Strike_Crossings", "Trajectory_Slope",
    "Trajectory_Consecutive_Debit_Rolls", "Trajectory_Roll_Efficiency_Trend",
    "Trajectory_Total_Roll_Cost", "Trajectory_IV_Change",
]

_EMPTY_ROLL: dict = {
    "consecutive_debits": 0,
    "total_close_cost": 0.0,
    "efficiency_trend": "STABLE",
}


# ── Batch loaders ─────────────────────────────────────────────────────────────

def _batch_load_prices(
    con: duckdb.DuckDBPyConnection,
    pairs: pd.DataFrame,
) -> dict[str, list[dict]]:
    """
    Load daily closes for all tickers from their entry dates onward.
    Returns {ticker: [{date, close, high, low}, ...]}.
    """
    result: dict[str, list[dict]] = {}
    try:
        # Check table exists
        tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
        if "price_history" not in tables:
            return result

        # Build earliest entry date per ticker
        earliest: dict[str, str] = {}
        for _, r in pairs.iterrows():
            t = str(r["Underlying_Ticker"])
            ts = r["Entry_Snapshot_TS"]
            if pd.notna(ts):
                d = pd.to_datetime(ts).strftime("%Y-%m-%d")
                if t not in earliest or d < earliest[t]:
                    earliest[t] = d

        if not earliest:
            return result

        # Single batched query
        tickers_sql = ", ".join(f"'{t}'" for t in earliest)
        min_date = min(earliest.values())
        rows = con.execute(f"""
            SELECT ticker, date, close_price, high_price, low_price
            FROM price_history
            WHERE UPPER(ticker) IN ({tickers_sql})
              AND date >= '{min_date}'
            ORDER BY ticker, date
        """).fetchall()

        for (ticker, dt, close, high, low) in rows:
            t_upper = str(ticker).upper()
            entry_d = earliest.get(t_upper)
            if entry_d is None:
                continue
            row_date = pd.to_datetime(dt).strftime("%Y-%m-%d")
            if row_date < entry_d:
                continue
            result.setdefault(t_upper, []).append({
                "date": row_date,
                "close": float(close or 0),
                "high": float(high or 0),
                "low": float(low or 0),
            })
    except Exception as e:
        logger.debug(f"[PositionTrajectory] price batch load: {e}")

    return result


def _batch_load_roll_history(
    con: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
) -> dict[str, dict]:
    """
    Load premium_ledger rows and compute roll metrics per ticker+account.
    Returns {ticker_acct_key: {consecutive_debits, total_close_cost, efficiency_trend}}.
    """
    result: dict[str, dict] = {}
    try:
        tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
        if "premium_ledger" not in tables:
            return result

        # Collect all ticker_acct_keys from df
        keys_needed: set[str] = set()
        for tid in df["TradeID"].dropna().unique():
            k = _ticker_acct_key(str(tid))
            if k:
                keys_needed.add(k)

        if not keys_needed:
            return result

        # Fetch all premium_ledger rows, ordered by expiry
        all_rows = con.execute(
            "SELECT trade_id, credit_received, COALESCE(close_cost, 0.0) AS close_cost, "
            "       expiry, status "
            "FROM premium_ledger ORDER BY expiry ASC"
        ).fetchall()

        # Group by ticker_acct_key
        key_cycles: dict[str, list[dict]] = {}
        for (lid_tid, credit, close_cost, expiry, status) in all_rows:
            k = _ticker_acct_key(str(lid_tid))
            if k is None or k not in keys_needed:
                continue
            key_cycles.setdefault(k, []).append({
                "credit": float(credit or 0),
                "close_cost": float(close_cost or 0),
                "net": float(credit or 0) - float(close_cost or 0),
                "status": str(status or ""),
                "expiry": str(expiry or ""),
            })

        # Compute metrics per key
        for k, cycles in key_cycles.items():
            # Filter to completed cycles (not OPEN)
            closed = [c for c in cycles if c["status"] in ("EXPIRED", "ROLLED", "ASSIGNED")]

            # Consecutive debit rolls from tail
            consec = 0
            for c in reversed(closed):
                if c["net"] < -0.005:  # net < 0 = debit roll
                    consec += 1
                else:
                    break

            # Total close cost
            total_cc = sum(c["close_cost"] for c in cycles)

            # Roll efficiency trend (last 3 completed cycles)
            last_3 = closed[-3:] if len(closed) >= 3 else closed
            if len(last_3) >= 2:
                nets = [c["net"] for c in last_3]
                # Trend: monotonically decreasing = DEGRADING, increasing = IMPROVING
                if all(nets[i] < nets[i - 1] for i in range(1, len(nets))):
                    eff = "DEGRADING"
                elif all(nets[i] > nets[i - 1] for i in range(1, len(nets))):
                    eff = "IMPROVING"
                else:
                    eff = "STABLE"
            else:
                eff = "STABLE"

            result[k] = {
                "consecutive_debits": consec,
                "total_close_cost": total_cc,
                "efficiency_trend": eff,
            }

    except Exception as e:
        logger.debug(f"[PositionTrajectory] roll batch load: {e}")

    return result


def _batch_load_iv_trajectory(
    con: duckdb.DuckDBPyConnection,
    pairs: pd.DataFrame,
    db_path: str,
) -> dict[str, str]:
    """
    Compute IV change (entry → now) for each ticker.
    Returns {ticker: "COMPRESSED" | "EXPANDED" | "STABLE"}.
    """
    result: dict[str, str] = {}
    try:
        # IV history lives in a separate database
        iv_db = db_path.replace("pipeline.duckdb", "iv_history.duckdb")
        iv_con = duckdb.connect(iv_db, read_only=True)
        try:
            tables = [r[0] for r in iv_con.execute("SHOW TABLES").fetchall()]
            if "iv_term_history" not in tables:
                return result

            # Build earliest entry dates
            earliest: dict[str, str] = {}
            for _, r in pairs.iterrows():
                t = str(r["Underlying_Ticker"])
                ts = r["Entry_Snapshot_TS"]
                if pd.notna(ts):
                    d = pd.to_datetime(ts).strftime("%Y-%m-%d")
                    if t not in earliest or d < earliest[t]:
                        earliest[t] = d

            if not earliest:
                return result

            tickers_sql = ", ".join(f"'{t}'" for t in earliest)
            min_date = min(earliest.values())

            # Get first and last IV per ticker since entry
            rows = iv_con.execute(f"""
                SELECT ticker, date, iv_30d
                FROM iv_term_history
                WHERE UPPER(ticker) IN ({tickers_sql})
                  AND date >= '{min_date}'
                  AND iv_30d IS NOT NULL
                ORDER BY ticker, date
            """).fetchall()

            # Group by ticker, get first and last iv_30d
            ticker_ivs: dict[str, list[float]] = {}
            for (ticker, dt, iv30) in rows:
                t_upper = str(ticker).upper()
                entry_d = earliest.get(t_upper)
                if entry_d is None:
                    continue
                row_date = pd.to_datetime(dt).strftime("%Y-%m-%d")
                if row_date < entry_d:
                    continue
                ticker_ivs.setdefault(t_upper, []).append(float(iv30))

            for t, ivs in ticker_ivs.items():
                if len(ivs) < 2:
                    result[t] = "STABLE"
                    continue
                iv_entry = ivs[0]
                iv_now = ivs[-1]
                if iv_entry <= 0:
                    result[t] = "STABLE"
                    continue
                change_pct = (iv_now - iv_entry) / iv_entry
                if change_pct < -_IV_CHANGE_THRESHOLD:
                    result[t] = "COMPRESSED"
                elif change_pct > _IV_CHANGE_THRESHOLD:
                    result[t] = "EXPANDED"
                else:
                    result[t] = "STABLE"

        finally:
            iv_con.close()

    except Exception as e:
        logger.debug(f"[PositionTrajectory] IV batch load: {e}")

    return result


# ── Per-row computation ───────────────────────────────────────────────────────

def _compute_stock_metrics(
    bars: list[dict],
    entry_price: float,
    current_price: float,
    current_strike: float | None,
) -> dict:
    """Compute trajectory metrics from price bars since entry."""
    if not bars or len(bars) < _MIN_BARS:
        return {"n_bars": len(bars) if bars else 0}

    closes = np.array([b["close"] for b in bars])
    highs = np.array([b["high"] for b in bars])
    lows = np.array([b["low"] for b in bars])

    stock_return = (current_price - entry_price) / entry_price
    mfe = (highs.max() - entry_price) / entry_price
    mae = (lows.min() - entry_price) / entry_price
    range_ratio = (closes.max() - closes.min()) / entry_price

    # Linear regression slope (normalized by entry price)
    x = np.arange(len(closes), dtype=float)
    if len(x) >= 2:
        coeffs = np.polyfit(x, closes, 1)
        slope_per_day = coeffs[0] / entry_price
    else:
        slope_per_day = 0.0

    # Strike crossings
    strike_crossings = 0
    if current_strike is not None and current_strike > 0:
        diffs = closes - current_strike
        sign_changes = np.diff(np.sign(diffs))
        strike_crossings = int(np.count_nonzero(sign_changes))

    return {
        "return": stock_return,
        "mfe": mfe,
        "mae": mae,
        "range_ratio": range_ratio,
        "slope": slope_per_day,
        "strike_crossings": strike_crossings,
        "n_bars": len(bars),
    }


def _classify_regime(
    stock: dict,
    roll: dict,
    iv_change: str,
    strategy: str,
) -> tuple[str, str]:
    """
    Deterministic decision tree for Position_Regime classification.

    Returns (regime, reason).
    """
    if stock.get("n_bars", 0) < _MIN_BARS:
        return ("NEUTRAL", f"Insufficient data ({stock.get('n_bars', 0)} bars)")

    ret = stock.get("return", 0.0)
    slope = stock.get("slope", 0.0)
    rr = stock.get("range_ratio", 0.0)
    mfe = stock.get("mfe", 0.0)
    mae = stock.get("mae", 0.0)
    xc = stock.get("strike_crossings", 0)
    consec = roll.get("consecutive_debits", 0)
    eff = roll.get("efficiency_trend", "STABLE")

    # Direction awareness: which direction constitutes "chasing"?
    is_call_strategy = strategy in _CALL_CHASE_STRATEGIES
    is_put_strategy = strategy in _PUT_CHASE_STRATEGIES

    # For calls: UP trend = chase; for puts: DOWN trend = chase
    if is_call_strategy:
        trend_chasing = slope > _SLOPE_STRONG and ret > _RETURN_LARGE
        trend_strong = slope > _SLOPE_STRONG
    elif is_put_strategy:
        trend_chasing = slope < -_SLOPE_STRONG and ret < -_RETURN_LARGE
        trend_strong = slope < -_SLOPE_STRONG
    else:
        trend_chasing = abs(slope) > _SLOPE_STRONG and abs(ret) > _RETURN_LARGE
        trend_strong = abs(slope) > _SLOPE_STRONG

    # ── 1. TRENDING_CHASE ─────────────────────────────────────────────────
    reasons: list[str] = []

    if trend_chasing and rr > _RANGE_WIDE and consec >= 2:
        reasons = [
            f"slope={slope:+.4f}/day",
            f"return={ret:+.0%}",
            f"range={rr:.0%}",
            f"{consec} consecutive debit rolls",
        ]
        return ("TRENDING_CHASE", " | ".join(reasons))

    if trend_chasing and mfe > _EXCURSION_MFE_LARGE and mae > _EXCURSION_MAE_SMALL and consec >= 1:
        reasons = [
            f"MFE={mfe:+.0%} without pullback",
            f"return={ret:+.0%}",
            f"{consec} debit roll(s)",
        ]
        return ("TRENDING_CHASE", " | ".join(reasons))

    if consec >= 2 and eff == "DEGRADING" and trend_strong:
        reasons = [
            f"roll efficiency DEGRADING",
            f"{consec} consecutive debits",
            f"slope={slope:+.4f}/day",
        ]
        return ("TRENDING_CHASE", " | ".join(reasons))

    # ── 2. RECOVERY_GRIND ─────────────────────────────────────────────────
    if ret < _RECOVERY_DROP and slope > _SLOPE_FLAT:
        reasons = [
            f"return={ret:+.0%} (underwater)",
            f"slope={slope:+.4f}/day (recovering)",
        ]
        if mae < -0.15:
            reasons.append(f"MAE={mae:+.0%}")
        return ("RECOVERY_GRIND", " | ".join(reasons))

    # ── 3. MEAN_REVERSION ─────────────────────────────────────────────────
    if abs(ret) < _RETURN_SMALL and mfe > _EXCURSION_BOTH and mae < -_EXCURSION_BOTH:
        reasons = [
            f"return={ret:+.1%} (near entry)",
            f"MFE={mfe:+.0%}",
            f"MAE={mae:+.0%}",
        ]
        return ("MEAN_REVERSION", " | ".join(reasons))

    if abs(ret) < _RETURN_SMALL and xc > _STRIKE_CROSS_MANY:
        reasons = [
            f"return={ret:+.1%} (near entry)",
            f"{xc} strike crossings",
        ]
        return ("MEAN_REVERSION", " | ".join(reasons))

    # ── 4. SIDEWAYS_INCOME ────────────────────────────────────────────────
    if rr < _RANGE_NARROW and xc > _STRIKE_CROSS_MANY:
        reasons = [
            f"range={rr:.0%} (narrow)",
            f"{xc} strike crossings",
        ]
        return ("SIDEWAYS_INCOME", " | ".join(reasons))

    if rr < _RANGE_NARROW and abs(slope) < _SLOPE_FLAT:
        reasons = [
            f"range={rr:.0%} (narrow)",
            f"slope={slope:+.5f}/day (flat)",
        ]
        return ("SIDEWAYS_INCOME", " | ".join(reasons))

    if consec <= 1 and rr < _RANGE_WIDE and xc > _STRIKE_CROSS_MANY:
        reasons = [
            f"{consec} debit roll(s)" if consec else "no debit rolls",
            f"range={rr:.0%}",
            f"{xc} strike crossings",
        ]
        return ("SIDEWAYS_INCOME", " | ".join(reasons))

    # ── 5. NEUTRAL (default) ──────────────────────────────────────────────
    return ("NEUTRAL", f"No strong pattern (return={ret:+.1%}, range={rr:.0%})")


# ── Propagation ───────────────────────────────────────────────────────────────

def _propagate_to_legs(df: pd.DataFrame) -> None:
    """
    Copy Position_Regime from scored rows to all legs of the same TradeID.
    Follows the compute_equity_integrity propagation pattern.
    """
    if "TradeID" not in df.columns:
        return

    # Find rows with non-NEUTRAL regime
    scored = df[df["Position_Regime"].isin({
        "TRENDING_CHASE", "RECOVERY_GRIND", "MEAN_REVERSION", "SIDEWAYS_INCOME",
    })]

    for idx, row in scored.iterrows():
        tid = row.get("TradeID")
        if not tid:
            continue
        regime = row["Position_Regime"]
        reason = row.get("Position_Regime_Reason", "")

        # All other legs of the same trade that haven't been scored
        same_trade = (
            (df["TradeID"] == tid)
            & (df.index != idx)
            & (df["Position_Regime"].isin({"NEUTRAL", "", np.nan}))
        )
        if same_trade.any():
            for col in _OUTPUT_COLUMNS:
                df.loc[same_trade, col] = row.get(col, "")
            logger.debug(
                f"[PositionTrajectory] Propagated {regime} to "
                f"{same_trade.sum()} leg(s) of trade {tid}"
            )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ticker_acct_key(trade_id: str) -> str | None:
    """Extract '{TICKER}_{ACCT}' from a trade_id like 'DKNG260306_24p5_CC_5376'."""
    m = _TICKER_ACCT_RE.match(str(trade_id or ""))
    return f"{m.group(1)}_{m.group(2)}" if m else None


def _float(val) -> float | None:
    """Safe float coercion — returns None on NaN/None/non-numeric."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if np.isnan(f) else f
    except (ValueError, TypeError):
        return None
