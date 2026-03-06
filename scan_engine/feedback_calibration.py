"""
Doctrine Feedback Calibration — Scan Engine Layer
==================================================

Reads the `doctrine_feedback` table (built by management/feedback_engine.py from
closed_trades outcomes) and translates it into a DQS adjustment for Step 12.

Design constraints:
  - READ-ONLY: never writes to the database
  - GRACEFUL: any DB failure returns neutral (multiplier=1.0, note="")
  - GUARDED: only adjusts when N >= MIN_SAMPLE (15 trades) in the bucket
  - ADDITIVE: applies on TOP of the existing timing penalty (×0.85), not instead of it
  - 90-DAY WINDOW: doctrine_feedback is already rolling 90d (maintained by feedback_engine)

Adjustment logic (per doctrine_feedback.suggested_action):
  TIGHTEN  (win_rate < 0.35, avg_pnl < -10%)  → DQS × 0.80  (demote — structural underperformance)
  HOLD     (mid-range outcomes)                → DQS × 1.00  (neutral — no adjustment)
  REINFORCE (win_rate > 0.70, avg_pnl > +20%) → DQS × 1.10  (promote — cap at +10%, avoid overfitting)
  INSUFFICIENT_SAMPLE                          → DQS × 1.00  (neutral — not enough data)

Output columns added to each READY/CONDITIONAL row:
  Feedback_Win_Rate      — historical win rate for this (strategy, momentum_state) bucket
  Feedback_Sample_N      — number of closed trades in the bucket
  Feedback_Action        — TIGHTEN | HOLD | REINFORCE | INSUFFICIENT_SAMPLE
  Feedback_Note          — human-readable explanation surfaced in scan view
  Calibrated_Confidence  — HIGH | MEDIUM | LOW (may be downgraded from confidence_band)

The bucket key is: "{STRATEGY}::{ENTRY_MOMENTUM_STATE}"
  Strategy comes from the scan row's Strategy column (e.g. LONG_PUT, CSP, BUY_WRITE)
  Momentum state comes from entry_timing_context → mapped to momentum labels used at management entry

Regime mapping (timing context → momentum bucket):
  EARLY_LONG / MODERATE  → TRENDING (stock moving in thesis direction early)
  LATE_LONG / LATE_SHORT → LATE_CYCLE (stock extended, momentum stalling)
  (no match)             → UNKNOWN
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, Dict, Tuple

logger = logging.getLogger(__name__)

# Only adjust when bucket has at least this many closed trades
_MIN_SAMPLE = 15

# DQS multipliers by suggested_action
_MULTIPLIERS: Dict[str, float] = {
    "TIGHTEN":             0.80,
    "HOLD":                1.00,
    "REINFORCE":           1.10,
    "INSUFFICIENT_SAMPLE": 1.00,
}

# Pipeline DB path — relative to project root
_DB_PATH = Path(__file__).parents[1] / "data" / "pipeline.duckdb"

# Module-level cache: loaded once per process, refreshed if stale
_feedback_cache: Optional[Dict[str, dict]] = None


def _load_feedback_cache() -> Dict[str, dict]:
    """
    Load doctrine_feedback from DuckDB into a dict keyed on condition_key.
    Returns empty dict on any failure (graceful degradation).
    """
    global _feedback_cache
    if _feedback_cache is not None:
        return _feedback_cache

    try:
        import duckdb
        con = duckdb.connect(str(_DB_PATH), read_only=True)
        rows = con.execute("""
            SELECT
                condition_key,
                strategy,
                sample_n,
                win_rate,
                avg_pnl_pct,
                avg_days_held,
                suggested_action,
                confidence
            FROM doctrine_feedback
        """).fetchdf()
        con.close()

        cache = {}
        for _, row in rows.iterrows():
            cache[str(row["condition_key"]).upper()] = {
                "strategy":         str(row["strategy"] or ""),
                "sample_n":         int(row["sample_n"] or 0),
                "win_rate":         float(row["win_rate"] or 0.0),
                "avg_pnl_pct":      float(row["avg_pnl_pct"] or 0.0),
                "avg_days_held":    float(row.get("avg_days_held") or 0.0),
                "suggested_action": str(row["suggested_action"] or "INSUFFICIENT_SAMPLE").upper(),
                "confidence":       str(row["confidence"] or "INSUFFICIENT_SAMPLE").upper(),
            }

        _feedback_cache = cache
        logger.info(f"[FeedbackCalibration] Loaded {len(cache)} doctrine_feedback buckets.")
        return cache

    except Exception as e:
        # BUG 4 FIX: info-level log so operators can see calibration is inactive
        # (expected on fresh install or before first management cycle closes a trade)
        logger.info(
            f"[FeedbackCalibration] doctrine_feedback table not found or unreadable: {e} "
            f"— using base DQS (expected on fresh install)"
        )
        _feedback_cache = {}
        return {}


def _timing_to_momentum(entry_timing_context: str) -> str:
    """
    Map scan-engine timing context to the momentum state label used in management
    entry snapshots (which is what doctrine_feedback is keyed on).
    """
    ctx = str(entry_timing_context or "").upper()
    if ctx in ("EARLY_LONG", "MODERATE", "EARLY_SHORT"):
        return "TRENDING"
    if ctx in ("LATE_LONG", "LATE_SHORT"):
        return "LATE_CYCLE"
    return "UNKNOWN"


def get_feedback_calibration(
    strategy: str,
    entry_timing_context: str,
    momentum_state: Optional[str] = None,
) -> Tuple[float, dict]:
    """
    Return (dqs_multiplier, metadata_dict) for a given scan candidate.

    Args:
        strategy:              e.g. "LONG_PUT", "CSP", "BUY_WRITE"
        entry_timing_context:  e.g. "EARLY_LONG", "LATE_SHORT", "MODERATE"
        momentum_state:        optional override (if scan already computed momentum)

    Returns:
        multiplier: float — apply to DQS_Score before threshold check
        meta: dict  — {win_rate, sample_n, suggested_action, note, confidence_adjustment}
    """
    neutral = {
        "win_rate":             None,
        "sample_n":             0,
        "suggested_action":     "INSUFFICIENT_SAMPLE",
        "note":                 "",
        "confidence_adjustment": None,
    }

    try:
        cache = _load_feedback_cache()
        if not cache:
            return 1.0, neutral

        # Build the bucket key
        strat = str(strategy or "").upper().strip()
        mom   = (str(momentum_state or "").upper().strip()
                 or _timing_to_momentum(entry_timing_context))
        key   = f"{strat}::{mom}"

        bucket = cache.get(key)
        if bucket is None:
            # Try UNKNOWN bucket as fallback
            bucket = cache.get(f"{strat}::UNKNOWN")

        if bucket is None:
            return 1.0, neutral

        suggested = bucket["suggested_action"]
        n         = bucket["sample_n"]
        win_rate  = bucket["win_rate"]
        avg_pnl   = bucket["avg_pnl_pct"]

        # Only apply adjustment when sample is sufficient
        if n < _MIN_SAMPLE or suggested == "INSUFFICIENT_SAMPLE":
            neutral["win_rate"]         = win_rate
            neutral["sample_n"]         = n
            neutral["suggested_action"] = "INSUFFICIENT_SAMPLE"
            neutral["note"]             = (
                f"Feedback: {n} trades recorded in {key} bucket "
                f"(need {_MIN_SAMPLE} for calibration)."
            )
            return 1.0, neutral

        multiplier = _MULTIPLIERS.get(suggested, 1.0)

        # Confidence adjustment:
        #   TIGHTEN → cap confidence at MEDIUM (even if IV/DQS gates say HIGH)
        #   REINFORCE → promote MEDIUM → HIGH (only when bucket confidence is also HIGH)
        conf_adj = None
        if suggested == "TIGHTEN":
            conf_adj = "MEDIUM"   # hard cap
        elif suggested == "REINFORCE" and bucket["confidence"] == "HIGH":
            conf_adj = "HIGH"     # explicit promotion

        # Human-readable note for scan view
        direction = "📉 underperforming" if suggested == "TIGHTEN" else (
            "📈 outperforming" if suggested == "REINFORCE" else "➡️ neutral"
        )
        note = (
            f"Feedback ({n} trades, {key}): "
            f"{win_rate:.0%} win rate, avg P&L {avg_pnl:+.0%} — "
            f"{direction}. DQS ×{multiplier:.2f}."
        )

        meta = {
            "win_rate":             win_rate,
            "sample_n":             n,
            "suggested_action":     suggested,
            "note":                 note,
            "confidence_adjustment": conf_adj,
        }
        return multiplier, meta

    except Exception as e:
        logger.debug(f"[FeedbackCalibration] get_feedback_calibration error: {e}")
        return 1.0, neutral


def prime_cache(conn) -> None:
    """
    Pre-warm the feedback cache using an existing DuckDB connection.

    Call this from pipeline.py BEFORE Step 12 runs, passing the pipeline's
    existing write connection.  This avoids the exclusive-lock conflict that
    occurs when _load_feedback_cache() tries to open a second read_only
    connection to the same pipeline.duckdb file.

    If the cache is already populated this is a no-op.
    """
    global _feedback_cache
    if _feedback_cache is not None:
        return  # already warm

    try:
        rows = conn.execute("""
            SELECT
                condition_key,
                strategy,
                sample_n,
                win_rate,
                avg_pnl_pct,
                avg_days_held,
                suggested_action,
                confidence
            FROM doctrine_feedback
        """).fetchdf()

        cache = {}
        for _, row in rows.iterrows():
            cache[str(row["condition_key"]).upper()] = {
                "strategy":         str(row["strategy"] or ""),
                "sample_n":         int(row["sample_n"] or 0),
                "win_rate":         float(row["win_rate"] or 0.0),
                "avg_pnl_pct":      float(row["avg_pnl_pct"] or 0.0),
                "avg_days_held":    float(row.get("avg_days_held") or 0.0),
                "suggested_action": str(row["suggested_action"] or "INSUFFICIENT_SAMPLE").upper(),
                "confidence":       str(row["confidence"] or "INSUFFICIENT_SAMPLE").upper(),
            }

        _feedback_cache = cache
        logger.info(f"[FeedbackCalibration] Cache pre-warmed via pipeline conn: {len(cache)} buckets.")

    except Exception as e:
        # Graceful — expected on fresh install before first management run
        logger.info(
            f"[FeedbackCalibration] prime_cache: doctrine_feedback not found: {e} "
            f"— using base DQS (expected on fresh install)"
        )
        _feedback_cache = {}


def invalidate_cache() -> None:
    """Force reload on next call (call after management run completes)."""
    global _feedback_cache
    _feedback_cache = None
    logger.info("[FeedbackCalibration] Cache invalidated.")
