"""
Pyramid Tier Tracker

Tracks how many confirmed SCALE_UP executions have occurred per TradeID
by querying management_recommendations history in pipeline.duckdb.

Output columns:
  Pyramid_Tier     — 0=base, 1=first add, 2=second add, 3=max (capped)
  Winner_Lifecycle — THESIS_UNPROVEN | THESIS_CONFIRMED | CONVICTION_BUILDING
                     | FULL_POSITION | THESIS_EXHAUSTING

Doctrine:
  Murphy (0.724): "Pyramid rules — each add smaller than the last."
  Jabbour (0.721): "Scale with profits (house money), not fresh capital."
  Nison (0.770):  "Trailing stops protect accumulated gains."
  Given (0.750):  "Minimum profit at 25% before any scaling."
  McMillan Ch.4:  "Pyramid on Strength — add on a retest of support."

State transitions:
  THESIS_UNPROVEN ── gain≥25% + conviction OK ──▶ THESIS_CONFIRMED
  THESIS_CONFIRMED ── tier 0→1 add ──▶ CONVICTION_BUILDING
  CONVICTION_BUILDING ── tier 1→2 add ──▶ FULL_POSITION
  FULL_POSITION ── momentum/conviction degrades ──▶ THESIS_EXHAUSTING
  Any state ── gain<25% or conviction WEAKENING/REVERSING ──▶ THESIS_UNPROVEN
"""

import duckdb
import logging
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)

_DB_PATH = Path(__file__).parents[3] / "data" / "pipeline.duckdb"

# Pyramid cap: maximum tier (no more adds beyond this)
_PYRAMID_MAX_TIER = 3

# Gain threshold: must be this much in profit before thesis is "confirmed"
_GAIN_THRESHOLD = 0.25

# Lifecycle states
LIFECYCLE_UNPROVEN = "THESIS_UNPROVEN"
LIFECYCLE_CONFIRMED = "THESIS_CONFIRMED"
LIFECYCLE_BUILDING = "CONVICTION_BUILDING"
LIFECYCLE_FULL = "FULL_POSITION"
LIFECYCLE_EXHAUSTING = "THESIS_EXHAUSTING"

# Conviction states that block scaling
_CONVICTION_UNFAVORABLE = {"WEAKENING", "REVERSING"}

# Momentum states that signal thesis exhaustion
_MOMENTUM_EXHAUSTING = {"LATE_CYCLE", "REVERSING"}


def compute_pyramid_tier(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each OPTION leg, query DuckDB management_recommendations to count
    confirmed SCALE_UP executions (Urgency=HIGH = trigger was hit) per TradeID.

    Derives Pyramid_Tier and Winner_Lifecycle.

    Degrades gracefully: if DuckDB is unavailable or no history exists,
    sets Pyramid_Tier=0 and Winner_Lifecycle=THESIS_UNPROVEN.
    """
    df = df.copy()

    _output_cols = ["Pyramid_Tier", "Winner_Lifecycle"]
    for col in _output_cols:
        if col not in df.columns:
            df[col] = 0 if col == "Pyramid_Tier" else LIFECYCLE_UNPROVEN

    _computed = 0
    _no_history = 0
    _skipped = 0

    # Open one connection for all rows
    try:
        con = duckdb.connect(str(_DB_PATH), read_only=True)
        # Check if table exists
        _table_exists = con.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_name = 'management_recommendations'"
        ).fetchone()[0] > 0
        _db_available = _table_exists
    except Exception as e:
        logger.warning(f"[PyramidTier] DuckDB unavailable: {e}. All legs will be tier 0.")
        _db_available = False
        con = None

    # Cache tier per TradeID — all legs in same trade share one tier
    _tier_cache: dict[str, int] = {}

    for idx, row in df.iterrows():
        if str(row.get("AssetType", "")).upper() != "OPTION":
            _skipped += 1
            continue

        try:
            trade_id = str(row.get("TradeID") or "").strip()
            if not trade_id:
                df.at[idx, "Pyramid_Tier"] = 0
                df.at[idx, "Winner_Lifecycle"] = LIFECYCLE_UNPROVEN
                _no_history += 1
                continue

            # Query tier from cache or DuckDB
            if trade_id in _tier_cache:
                tier = _tier_cache[trade_id]
            elif _db_available:
                # Count confirmed SCALE_UP executions (HIGH urgency = trigger was hit)
                result = con.execute("""
                    SELECT COUNT(*) AS add_count
                    FROM management_recommendations
                    WHERE TradeID = ?
                      AND Action = 'SCALE_UP'
                      AND Urgency = 'HIGH'
                """, [trade_id]).fetchone()
                tier = min(_PYRAMID_MAX_TIER, result[0]) if result else 0
                _tier_cache[trade_id] = tier
            else:
                tier = 0
                _tier_cache[trade_id] = tier

            # Derive Winner_Lifecycle from tier + gain + conviction + momentum
            # Read current metrics (already computed by earlier Cycle 2 modules)
            _entry_price = float(row.get("Premium_Entry", 0) or row.get("Basis_Entry", 0) or 0)
            _last_price = float(row.get("Last", 0) or 0)
            _gain_pct = (
                (_last_price - _entry_price) / _entry_price
                if _entry_price > 0 and _last_price > 0
                else 0.0
            )

            _conv_status = str(row.get("Conviction_Status", "") or "").upper()
            _mom_state = str(row.get("MomentumVelocity_State", "") or "").upper()

            lifecycle = _derive_lifecycle(tier, _gain_pct, _conv_status, _mom_state)

            df.at[idx, "Pyramid_Tier"] = tier
            df.at[idx, "Winner_Lifecycle"] = lifecycle
            _computed += 1

        except Exception as e:
            logger.debug(f"[PyramidTier] idx={idx} error: {e}")
            df.at[idx, "Pyramid_Tier"] = 0
            df.at[idx, "Winner_Lifecycle"] = LIFECYCLE_UNPROVEN
            _skipped += 1
            continue

    if con is not None:
        try:
            con.close()
        except Exception:
            pass

    logger.info(
        f"[PyramidTier] {_computed} legs computed, "
        f"{_no_history} no history (→tier 0), {_skipped} skipped/non-option"
    )
    return df


def _derive_lifecycle(
    tier: int,
    gain_pct: float,
    conv_status: str,
    mom_state: str,
) -> str:
    """
    Deterministic state machine for Winner_Lifecycle.

    Priority (top to bottom):
    1. Gain < 25% OR conviction unfavorable → THESIS_UNPROVEN
    2. Tier ≥ 2 + degrading momentum/conviction → THESIS_EXHAUSTING
    3. Tier ≥ 2 → FULL_POSITION
    4. Tier 1 + favorable → CONVICTION_BUILDING
    5. Tier 0 + gain ≥ 25% + conviction OK → THESIS_CONFIRMED
    6. Fallback → THESIS_UNPROVEN
    """
    _conv_unfavorable = conv_status in _CONVICTION_UNFAVORABLE
    _mom_exhausting = mom_state in _MOMENTUM_EXHAUSTING

    # 1. Not proven yet
    if gain_pct < _GAIN_THRESHOLD:
        return LIFECYCLE_UNPROVEN

    # 2. Conviction failing → unproven (regardless of tier)
    if _conv_unfavorable and tier < 2:
        return LIFECYCLE_UNPROVEN

    # 3. Full position with degrading signals → exhausting
    if tier >= 2 and (_mom_exhausting or conv_status == "REVERSING"):
        return LIFECYCLE_EXHAUSTING

    # 4. Full position → protect
    if tier >= 2:
        return LIFECYCLE_FULL

    # 5. Tier 1 + favorable → building conviction
    if tier == 1 and not _conv_unfavorable and not _mom_exhausting:
        return LIFECYCLE_BUILDING

    # 6. Tier 1 but unfavorable conditions → exhausting
    if tier == 1 and (_mom_exhausting or conv_status == "REVERSING"):
        return LIFECYCLE_EXHAUSTING

    # 7. Tier 0, gain confirmed, conviction OK → thesis confirmed
    if tier == 0 and gain_pct >= _GAIN_THRESHOLD and not _conv_unfavorable:
        return LIFECYCLE_CONFIRMED

    return LIFECYCLE_UNPROVEN
