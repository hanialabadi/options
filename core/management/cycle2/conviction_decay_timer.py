"""
Conviction Decay Timer

Tracks structural non-progress for long OPTION positions by querying
management_recommendations history in pipeline.duckdb.

Output columns:
  Delta_Deterioration_Streak — consecutive cycles where Delta_ROC_3D < -0.05
  Conviction_Status          — STRENGTHENING | STABLE | WEAKENING | REVERSING
  Conviction_Fade_Days       — total days conviction has been weakening (last 10)

Doctrine:
  Passarelli Ch.2: "A position that consistently moves away from its thesis
    is not 'waiting' — it is deteriorating. Holding requires a structural reason,
    not just hope."
  McMillan Ch.4: "Delta trajectory is the most reliable early-warning signal
    for long directional options — delta falling toward zero signals the market
    disagrees with your thesis."

Streak interpretation:
  0             → STRENGTHENING (if ROC > +0.05) or STABLE
  1–2 cycles    → WEAKENING (early deterioration — monitor)
  3+ cycles     → REVERSING (structural non-progress — escalation candidate)

Gate thresholds (used in engine.py):
  Conviction_Status == REVERSING AND Delta_Deterioration_Streak >= 3
  AND DTE < 45 AND pnl_pct < -0.20 → ROLL MEDIUM
"""

import duckdb
import logging
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)

_DB_PATH = Path(__file__).parents[4] / "data" / "pipeline.duckdb"

# Deterioration threshold: ROC_3D below this = deteriorating
_ROC_DETERIORATION_THRESHOLD = -0.05

# Strengthening threshold: ROC_3D above this = actively improving
_ROC_STRENGTHENING_THRESHOLD = 0.05

# History window to query (trading days)
_HISTORY_LIMIT = 10


def compute_conviction_decay(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each OPTION leg, query DuckDB management_recommendations to retrieve
    the last _HISTORY_LIMIT Delta_ROC_3D values and compute decay metrics.

    Degrades gracefully: if DuckDB is unavailable or no history exists,
    sets Conviction_Status=STABLE and leaves streak/fade as NaN.
    """
    df = df.copy()

    _output_cols = ['Delta_Deterioration_Streak', 'Conviction_Status', 'Conviction_Fade_Days']
    for col in _output_cols:
        if col not in df.columns:
            df[col] = None

    _computed = 0
    _no_history = 0
    _skipped = 0

    # Open one connection for all legs — more efficient than per-row connections
    try:
        con = duckdb.connect(str(_DB_PATH), read_only=True)
        _db_available = True
    except Exception as e:
        logger.warning(f"[ConvictionDecay] DuckDB unavailable: {e}. All legs will be STABLE.")
        _db_available = False
        con = None

    for idx, row in df.iterrows():
        if str(row.get('AssetType', '')).upper() != 'OPTION':
            _skipped += 1
            continue

        try:
            leg_id = str(row.get('LegID') or row.get('TradeID') or '').strip()
            if not leg_id:
                df.at[idx, 'Conviction_Status'] = 'STABLE'
                _no_history += 1
                continue

            if not _db_available:
                df.at[idx, 'Conviction_Status'] = 'STABLE'
                continue

            # Query last N Delta_ROC_3D values for this leg
            hist = con.execute("""
                SELECT Delta_ROC_3D, Snapshot_TS
                FROM management_recommendations
                WHERE LegID = ?
                  AND Delta_ROC_3D IS NOT NULL
                ORDER BY Snapshot_TS DESC
                LIMIT ?
            """, [leg_id, _HISTORY_LIMIT]).fetchdf()

            if hist.empty:
                df.at[idx, 'Conviction_Status'] = 'STABLE'
                _no_history += 1
                continue

            roc_vals = hist['Delta_ROC_3D'].tolist()

            # Streak: consecutive leading negative ROC values (most recent first)
            streak = 0
            for v in roc_vals:
                try:
                    if float(v) < _ROC_DETERIORATION_THRESHOLD:
                        streak += 1
                    else:
                        break  # stop at first non-deteriorating reading
                except (TypeError, ValueError):
                    break

            # Fade days: total count of negative ROC in the window
            fade_days = sum(
                1 for v in roc_vals
                if pd.notna(v) and float(v) < _ROC_DETERIORATION_THRESHOLD
            )

            # Status classification
            if streak == 0:
                try:
                    latest = float(roc_vals[0])
                except (TypeError, ValueError):
                    latest = 0.0
                status = 'STRENGTHENING' if latest > _ROC_STRENGTHENING_THRESHOLD else 'STABLE'
            elif streak <= 2:
                status = 'WEAKENING'
            else:
                status = 'REVERSING'

            df.at[idx, 'Delta_Deterioration_Streak'] = int(streak)
            df.at[idx, 'Conviction_Status']           = status
            df.at[idx, 'Conviction_Fade_Days']        = int(fade_days)
            _computed += 1

        except Exception as e:
            logger.debug(f"[ConvictionDecay] idx={idx} error: {e}")
            df.at[idx, 'Conviction_Status'] = 'STABLE'
            _skipped += 1
            continue

    if con is not None:
        try:
            con.close()
        except Exception:
            pass

    logger.info(
        f"[ConvictionDecay] {_computed} legs computed, "
        f"{_no_history} no history (→STABLE), {_skipped} skipped/non-option"
    )
    return df
