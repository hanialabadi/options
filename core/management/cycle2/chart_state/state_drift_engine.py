"""
Cycle-2.5: Chart State Temporal Memory

Tracks how long each chart state has persisted (consecutive snapshots) and
whether the state changed since last run. Enables doctrine to reason about
THETA_DOMINANT-for-N-days, sustained NO_TREND, etc.

Column naming: uses the actual output column pattern (*_State suffix),
not the old Chart_State_* prefix.
"""

import pandas as pd
import logging
import duckdb

logger = logging.getLogger(__name__)

# The 6 most doctrine-relevant states to track for persistence
TRACKED_STATE_COLS = [
    "MomentumVelocity_State",
    "GreekDominance_State",
    "TrendIntegrity_State",
    "VolatilityState_State",
    "PriceStructure_State",
    "RegimeStability_State",
]


def load_prev_states(db_path: str, leg_ids: list) -> pd.DataFrame:
    """
    Load the most recent known state for each LegID from DuckDB history.
    Returns a DataFrame indexed by LegID with *_State_Days, *_State_Prev cols,
    plus Snapshot_TS so the caller can detect same-day re-runs.
    """
    if not leg_ids:
        return pd.DataFrame()
    try:
        con = duckdb.connect(db_path, read_only=True)
        placeholders = ", ".join(f"'{lid}'" for lid in leg_ids)
        state_cols_sql = ", ".join(
            [f'"{c}"' for c in TRACKED_STATE_COLS] +
            [f'"{c}_Days"' for c in TRACKED_STATE_COLS]
        )
        query = f"""
            SELECT LegID, Snapshot_TS, {state_cols_sql}
            FROM management_recommendations
            WHERE LegID IN ({placeholders})
            QUALIFY ROW_NUMBER() OVER (PARTITION BY LegID ORDER BY Snapshot_TS DESC) = 1
        """
        prev = con.execute(query).df()
        con.close()
        return prev.set_index("LegID")
    except Exception as e:
        logger.warning(f"⚠️ state_drift_engine: could not load prior states: {e}")
        return pd.DataFrame()


def compute_state_drift(df: pd.DataFrame, db_path: str = "data/pipeline.duckdb") -> pd.DataFrame:
    """
    For each tracked state column, compute:
      - {col}_Prev  : state value in the previous snapshot (or UNKNOWN)
      - {col}_Days  : consecutive snapshots in current state (1 if just entered)
      - {col}_Change: ENTERED | STABLE | UNKNOWN

    Reads prior state from DuckDB management_recommendations table.
    Falls back gracefully if table is missing or leg has no history.
    """
    if df.empty:
        return df

    df = df.copy()

    # Initialize all output columns with defaults
    for col in TRACKED_STATE_COLS:
        df[f"{col}_Prev"] = "UNKNOWN"
        df[f"{col}_Days"] = 1
        df[f"{col}_Change"] = "UNKNOWN"

    # Load prior states from DB
    leg_ids = df["LegID"].dropna().tolist() if "LegID" in df.columns else []
    prev_df = load_prev_states(db_path, leg_ids)

    if prev_df.empty:
        # No history: every state is "ENTERED" (day 1), Prev = UNKNOWN
        for col in TRACKED_STATE_COLS:
            if col in df.columns:
                curr = df[col].astype(str).str.split(".").str[-1].str.upper()
                known = curr.notna() & (curr != "NAN") & (curr != "UNKNOWN") & (curr != "NONE")
                df.loc[known, f"{col}_Change"] = "ENTERED"
                df.loc[~known, f"{col}_Change"] = "UNKNOWN"
        return df

    # Row-by-row update using prior state
    for idx, row in df.iterrows():
        leg_id = row.get("LegID")
        if leg_id not in prev_df.index:
            # New leg — all states are ENTERED at day 1
            for col in TRACKED_STATE_COLS:
                if col in df.columns:
                    curr_raw = row.get(col, "UNKNOWN")
                    curr = str(getattr(curr_raw, "value", None) or curr_raw).split(".")[-1].upper()
                    if curr not in ("UNKNOWN", "NONE", "NAN", ""):
                        df.at[idx, f"{col}_Change"] = "ENTERED"
            continue

        prev_row = prev_df.loc[leg_id]

        # Cap guard: state_days can never exceed the position's total age.
        # Corrupted counts from prior same-day-increment runs are silently capped.
        _days_in_trade = float(row.get("Days_In_Trade", 0) or 0)

        # Detect same-calendar-day re-run: if the prior snapshot was recorded
        # on the same date as today, do NOT increment the days counter — just
        # carry the existing count forward. Only increment on a new calendar day.
        # This prevents multiple intraday runs from inflating state_days.
        import datetime as _dt
        _prev_ts_raw = prev_row.get("Snapshot_TS")
        try:
            _prev_date = pd.to_datetime(_prev_ts_raw).date() if pd.notna(_prev_ts_raw) else None
        except Exception:
            _prev_date = None
        _today = _dt.date.today()
        _same_day = (_prev_date == _today) if _prev_date is not None else False

        for col in TRACKED_STATE_COLS:
            if col not in df.columns:
                continue
            curr_raw = row.get(col, "UNKNOWN")
            curr = str(getattr(curr_raw, "value", None) or curr_raw).split(".")[-1].upper()

            prev_raw = prev_row.get(col, "UNKNOWN")
            prev = str(getattr(prev_raw, "value", None) or prev_raw).split(".")[-1].upper()

            prev_days_raw = prev_row.get(f"{col}_Days", 0)
            try:
                prev_days = int(prev_days_raw) if pd.notna(prev_days_raw) else 0
            except (ValueError, TypeError):
                prev_days = 0
            # Cap at position age to repair any prior inflation from same-day runs
            if _days_in_trade > 0 and prev_days > _days_in_trade:
                prev_days = int(_days_in_trade)

            df.at[idx, f"{col}_Prev"] = prev if prev not in ("NAN", "NONE", "") else "UNKNOWN"

            if curr in ("UNKNOWN", "NONE", "NAN", ""):
                df.at[idx, f"{col}_Change"] = "UNKNOWN"
                df.at[idx, f"{col}_Days"] = 0
            elif curr != prev:
                df.at[idx, f"{col}_Change"] = "ENTERED"
                df.at[idx, f"{col}_Days"] = 1
            elif _same_day:
                # Same state, same calendar day — carry count as-is, no increment
                df.at[idx, f"{col}_Change"] = "STABLE"
                df.at[idx, f"{col}_Days"] = prev_days
            else:
                # Same state, new calendar day — increment by 1
                df.at[idx, f"{col}_Change"] = "STABLE"
                df.at[idx, f"{col}_Days"] = prev_days + 1

    logger.info(f"✅ State drift computed for {len(df)} legs across {len(TRACKED_STATE_COLS)} state columns")
    return df
