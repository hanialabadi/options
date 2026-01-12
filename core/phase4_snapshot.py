import os
import logging
from datetime import datetime, time
from pathlib import Path
import pandas as pd
from core.data_contracts import save_snapshot
from core.freeze_entry_data import freeze_entry_data

logger = logging.getLogger(__name__)

# Timezone handling
try:
    from zoneinfo import ZoneInfo
    US_EASTERN = ZoneInfo("America/New_York")
except ImportError:
    # Fallback for Python < 3.9
    try:
        import pytz
        US_EASTERN = pytz.timezone("America/New_York")
    except ImportError:
        logger.warning("⚠️  Neither zoneinfo nor pytz available. Market session classification may be incorrect.")
        US_EASTERN = None

# Column name constants for schema integrity
SNAPSHOT_TS_COL = "Snapshot_TS"
RUN_ID_COL = "run_id"
TRADE_ID_COL = "TradeID"
CLEAN_LEGS_TABLE = "clean_legs"
FIRST_SEEN_TABLE = "trade_first_seen"  # New: Deterministic entry tracking

# Phase 3 structural integrity columns (not opinionated, but required for semantic coherence)
PHASE3_REQUIRED_COLS = [
    TRADE_ID_COL,
    "Strategy",
    "AssetType",
    "LegType",
    "Symbol",
]

# Issue 7: Schema versioning for forensic tracking
SNAPSHOT_SCHEMA_VERSION = "1.0"  # Increment on breaking schema changes
SCHEMA_HASH_COL = "Schema_Hash"  # Column to store hash of column set


def _get_market_session(dt: datetime) -> str:
    """
    Determine market session for US equities markets (Eastern Time).
    
    Parameters
    ----------
    dt : datetime
        Snapshot timestamp (will be converted to ET if timezone-aware)
    
    Returns
    -------
    str
        "PreMarket" | "Regular" | "AfterHours" | "Closed"
    
    Notes
    -----
    US market hours (ET):
    - PreMarket: 4:00 AM - 9:30 AM
    - Regular: 9:30 AM - 4:00 PM
    - AfterHours: 4:00 PM - 8:00 PM
    - Closed: 8:00 PM - 4:00 AM, weekends, holidays
    
    Timezone Handling:
    - If dt is timezone-aware, converts to ET
    - If dt is timezone-naive, assumes ET (logs warning)
    """
    # Convert to Eastern Time if timezone-aware
    if US_EASTERN and dt.tzinfo is not None:
        dt = dt.astimezone(US_EASTERN)
    elif dt.tzinfo is None and US_EASTERN:
        logger.warning(f"⚠️  Timestamp is timezone-naive, assuming Eastern Time: {dt}")
    
    weekday = dt.weekday()  # 0=Monday, 6=Sunday
    
    # Weekend: Always closed
    if weekday >= 5:  # Saturday or Sunday
        return "Closed"
    
    # Extract time component
    snapshot_time = dt.time()
    
    # Market hours (ET)
    premarket_start = time(4, 0)
    regular_start = time(9, 30)
    regular_end = time(16, 0)
    afterhours_end = time(20, 0)
    
    if premarket_start <= snapshot_time < regular_start:
        return "PreMarket"
    elif regular_start <= snapshot_time < regular_end:
        return "Regular"
    elif regular_end <= snapshot_time < afterhours_end:
        return "AfterHours"
    else:
        return "Closed"


def _is_market_open(dt: datetime) -> bool:
    """
    Check if regular market hours are active.
    
    Parameters
    ----------
    dt : datetime
        Snapshot timestamp
    
    Returns
    -------
    bool
        True if regular market hours (9:30 AM - 4:00 PM ET, weekdays)
    """
    return _get_market_session(dt) == "Regular"


def _get_snapshot_day_type(dt: datetime) -> str:
    """
    Classify snapshot day type.
    
    Parameters
    ----------
    dt : datetime
        Snapshot timestamp
    
    Returns
    -------
    str
        "Weekday" | "Weekend" | "Holiday"
    
    Notes
    -----
    TODO: Integrate US market holiday calendar (NYSE/NASDAQ)
    Current implementation only detects weekends.
    """
    weekday = dt.weekday()
    
    # TODO: Check against holiday calendar
    # Example: from pandas.tseries.holiday import USFederalHolidayCalendar
    
    if weekday >= 5:  # Saturday or Sunday
        return "Weekend"
    else:
        return "Weekday"


def _get_or_create_first_seen_dates(con, trade_ids: list, snapshot_ts: pd.Timestamp) -> tuple[dict, list]:
    """
    Track first observation date for each TradeID (deterministic entry tracking).
    
    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Active DuckDB connection
    trade_ids : list
        TradeIDs in current snapshot
    snapshot_ts : pd.Timestamp
        Current snapshot timestamp
    
    Returns
    -------
    tuple[dict, list]
        - first_seen_map: {TradeID: First_Seen_Date} mapping for all trades
        - new_trade_ids: List of TradeIDs that were newly registered in this call
    
    Notes
    -----
    - Replaces non-deterministic Entry_Date from Phase 2
    - First_Seen_Date = timestamp of FIRST snapshot containing TradeID
    - Stored in separate table for efficient lookup
    - New trades get snapshot_ts as First_Seen_Date
    """
    # Check if first_seen table exists
    table_exists = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_name = '{FIRST_SEEN_TABLE}' AND table_schema = 'main'
    """).fetchone()[0] > 0
    
    if not table_exists:
        # Create first_seen tracking table
        con.execute(f"""
            CREATE TABLE {FIRST_SEEN_TABLE} (
                TradeID VARCHAR PRIMARY KEY,
                First_Seen_Date TIMESTAMP NOT NULL
            )
        """)
        logger.info(f"Created {FIRST_SEEN_TABLE} table for deterministic entry tracking")
    
    # Query existing First_Seen_Date entries
    placeholders = ', '.join(['?' for _ in trade_ids])
    existing = con.execute(f"""
        SELECT TradeID, First_Seen_Date 
        FROM {FIRST_SEEN_TABLE} 
        WHERE TradeID IN ({placeholders})
    """, trade_ids).fetchall()
    
    first_seen_map = {row[0]: row[1] for row in existing}
    
    # Identify new trades (not yet seen)
    new_trade_ids = [tid for tid in trade_ids if tid not in first_seen_map]
    
    if new_trade_ids:
        # Insert new trades with current snapshot timestamp
        new_records = [(tid, snapshot_ts) for tid in new_trade_ids]
        con.executemany(f"""
            INSERT INTO {FIRST_SEEN_TABLE} (TradeID, First_Seen_Date) 
            VALUES (?, ?)
        """, new_records)
        
        logger.info(f"Registered {len(new_trade_ids)} new trades with First_Seen_Date = {snapshot_ts}")
        
        # Add to map
        for tid in new_trade_ids:
            first_seen_map[tid] = snapshot_ts
    
    return first_seen_map, new_trade_ids

def _compute_schema_hash(columns: list) -> str:
    """
    Compute deterministic hash of column set for schema drift detection.
    
    Issue 7: Enables forensic tracking of incompatible schema changes.
    """
    import hashlib
    # Sort columns for deterministic hash (order-independent)
    col_str = ",".join(sorted(columns))
    return hashlib.md5(col_str.encode()).hexdigest()[:16]  # 16-char hash

def _pandas_dtype_to_duckdb(dtype) -> str:
    """
    Map pandas dtype to DuckDB column type.
    Preserves type fidelity for numeric, boolean, datetime, and string columns.
    
    Issue 1: Prevents VARCHAR coercion of all new columns.
    Issue 2: Handles pandas nullable dtypes (Int64, boolean, etc.)
    """
    dtype_str = str(dtype)
    
    # Issue 2: Pandas nullable integers (Int64, Int32, etc.) - exact match first
    if dtype_str in ['Int64', 'Int32', 'Int16', 'Int8']:
        return 'BIGINT'
    if dtype_str in ['UInt64', 'UInt32', 'UInt16', 'UInt8']:
        return 'UBIGINT'
    
    # Standard numeric types (lowercase check)
    dtype_lower = dtype_str.lower()
    if dtype_lower.startswith('int') or dtype_lower in ['int64', 'int32', 'int16', 'int8']:
        return 'BIGINT'
    if dtype_lower.startswith('uint') or dtype_lower in ['uint64', 'uint32', 'uint16', 'uint8']:
        return 'UBIGINT'
    if dtype_lower in ['float64', 'float32']:
        return 'DOUBLE'
    
    # Issue 3: Boolean types - precise match (exact string, not substring)
    if dtype_str == 'boolean' or dtype_str == 'bool' or dtype_lower == 'bool':
        return 'BOOLEAN'
    
    # Datetime types
    if 'datetime' in dtype_lower or 'timestamp' in dtype_lower:
        return 'TIMESTAMP'
    if dtype_lower == 'date':
        return 'DATE'
    
    # Issue 3: Default to VARCHAR for object/string/unknown types
    # Note: Object columns containing booleans will be VARCHAR (intentional)
    return 'VARCHAR'

def save_clean_snapshot(
    df: pd.DataFrame,
    db_path: str = None,
    to_csv: bool = True,
    to_db: bool = True,
) -> tuple[pd.DataFrame, str, str, bool, bool]:
    """
    Phase 4: Snapshot Persistence Layer
    
    Saves a complete snapshot of Phase 3 enriched data with temporal metadata.
    
    Design Philosophy:
    - Append-only: Never overwrites historical data
    - Leg-level: Preserves multi-leg trade granularity
    - Non-opinionated: No filtering, aggregation, or decision logic
    - Schema-agnostic: Dynamically persists ALL Phase 3 columns
    
    This is a TRUTH LEDGER, not a presentation layer.
    Column filtering belongs in downstream consumers (dashboard, analytics).
    
    Supports:
    - Drift analysis (PCS, Capital, Greeks, Moneyness across snapshots)
    - Position aging (DTE, expiration tracking)
    - Audit trail (Phase 2C validation history)
    - Cross-account integrity (Account column preserved)
    
    Parameters
    ----------
    df : pd.DataFrame
        Phase 3 enriched DataFrame (all columns preserved)
    db_path : str, optional
        DuckDB path for time-series append storage.
        If None, uses workspace-relative path: <workspace>/data/pipeline.duckdb
    to_csv : bool, default True
        Save snapshot to CSV via data_contracts
    to_db : bool, default True
        Append snapshot to DuckDB clean_legs table
    
    Returns
    -------
    tuple[pd.DataFrame, str, str, bool, bool]
        - DataFrame: Complete snapshot with temporal metadata (Issue 6: includes Snapshot_TS, run_id, Schema_Hash - Phase 4 output)
        - str: CSV snapshot path (or "" if not saved)
        - str: run_id for this snapshot (YYYY-MM-DD_HH-MM-SS-mmm)
        - bool: csv_success (True if CSV saved successfully)
        - bool: db_success (True if DuckDB saved successfully)
    
    Raises
    ------
    ValueError
        If DataFrame is empty or missing required columns
    RuntimeError
        If DuckDB save fails
    
    Notes
    -----
    Phase 4 does NOT mutate upstream columns (TradeID, Strategy, LegType).
    Adds: Snapshot_TS, run_id, Schema_Hash (Issue 6: Phase 4 metadata columns).
    
    Issue 6: CSV output contains Phase 4 metadata and is NOT a pure Phase 3 snapshot.
    CSV is for convenience/debugging only. Use DuckDB truth ledger for production queries.
    
    Multi-leg trades remain as multiple rows grouped by TradeID.
    Flattening belongs in dashboard/analysis layers, not persistence.
    
    Idempotency: Appending same data multiple times creates duplicate snapshots.
    Use run_id to deduplicate in queries if needed.
    """
    # Issue 3: Validate input DataFrame
    if df is None or df.empty:
        raise ValueError("❌ Cannot save empty DataFrame to snapshot")
    
    # Issue 7: Validate Phase 3 structural integrity columns
    missing_cols = [col for col in PHASE3_REQUIRED_COLS if col not in df.columns]
    if missing_cols:
        raise ValueError(
            f"❌ Missing required Phase 3 columns for snapshot: {missing_cols}. "
            f"Snapshot requires minimal structural integrity (TradeID, Strategy, AssetType, LegType, Symbol)."
        )
    
    # Deduplicate columns in DataFrame just in case (prevents DuckDB insertion errors)
    if df.columns.duplicated().any():
        dup_cols = df.columns[df.columns.duplicated()].unique().tolist()
        logger.warning(f"⚠️ Duplicate columns detected in DataFrame: {dup_cols}. Deduplicating.")
        df = df.loc[:, ~df.columns.duplicated()].copy()

    logger.info(f"Saving snapshot for {len(df)} positions")
    
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H-%M-%S")
    millis_str = now.strftime("%f")[:3]  # Issue 6: Add milliseconds for collision safety
    run_id = f"{date_str}_{time_str}-{millis_str}"

    # Issue 15: Clean variable naming - single copy
    df_snapshot = df.copy()
    df_snapshot[SNAPSHOT_TS_COL] = pd.Timestamp(now)  # Issue 5: Store as datetime, not ISO string
    df_snapshot[RUN_ID_COL] = run_id
    
    # Issue 7: Add schema hash for forensic tracking
    schema_hash = _compute_schema_hash(list(df.columns))  # Hash original Phase 3 columns (before metadata)
    df_snapshot[SCHEMA_HASH_COL] = schema_hash
    logger.info(f"Snapshot schema hash: {schema_hash} (Phase 3 columns: {len(df.columns)})")
    
    # NEW: Add market-aware perception context (Phase 4 metadata)
    df_snapshot["Market_Session"] = _get_market_session(now)
    df_snapshot["Is_Market_Open"] = _is_market_open(now)
    df_snapshot["Snapshot_DayType"] = _get_snapshot_day_type(now)
    logger.info(
        f"Market context: {df_snapshot['Market_Session'].iloc[0]}, "
        f"Open={df_snapshot['Is_Market_Open'].iloc[0]}, "
        f"DayType={df_snapshot['Snapshot_DayType'].iloc[0]}"
    )

    # Issue 8: Use workspace-relative path if not provided
    if db_path is None:
        workspace_root = Path(__file__).parent.parent  # Go up to workspace root
        db_path = str(workspace_root / "data" / "pipeline.duckdb")
    
    # Initialize return values
    snapshot_path = ""
    csv_success = False
    db_success = False
    
    # Issue 4: Enforce atomicity - DB first (truth ledger), then CSV (convenience)
    # If DB fails, CSV is not written (prevents partial persistence)

    # --- Save/Append to DuckDB with Schema Migration (DB first for atomicity) ---
    if to_db:
        import duckdb
        
        try:
            # Issue 8: Ensure db directory exists
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            
            # Issue 11: Use context manager for connection safety
            with duckdb.connect(db_path) as con:
                # Issue 14: Use transaction for atomicity
                con.execute("BEGIN TRANSACTION")
                
                try:
                    # NEW: Track First_Seen_Date for each TradeID (deterministic entry tracking)
                    # This must happen before insertion to clean_legs
                    trade_ids = df_snapshot[TRADE_ID_COL].unique().tolist()
                    first_seen_map, new_trade_ids = _get_or_create_first_seen_dates(con, trade_ids, pd.Timestamp(now))
                    
                    # Add First_Seen_Date column to snapshot (replacing Entry_Date from Phase 2)
                    df_snapshot["First_Seen_Date"] = df_snapshot[TRADE_ID_COL].map(first_seen_map)
                    
                    # 1. Load existing entry data from DB to prevent overwriting
                    df_existing_entry = _get_existing_entry_data(con, trade_ids)
                    
                    if not df_existing_entry.empty:
                        # Merge existing entry data into current snapshot
                        # This ensures freeze_entry_data sees the already frozen values
                        entry_cols = [c for c in df_existing_entry.columns if c != TRADE_ID_COL]
                        
                        # Drop these columns from df_snapshot if they exist (they shouldn't yet, but defensive)
                        df_snapshot = df_snapshot.drop(columns=[c for c in entry_cols if c in df_snapshot.columns])
                        
                        df_snapshot = df_snapshot.merge(df_existing_entry, on=TRADE_ID_COL, how='left')
                        logger.info(f"Rehydrated entry data for {len(df_existing_entry)} existing trades from DB")

                    # 2. Freeze entry data for new positions (Delta_Entry, IV_Entry, etc.)
                    # This adds all _Entry columns to the DataFrame before we check the DB schema
                    df_snapshot = freeze_entry_data(df_snapshot, new_trade_ids=new_trade_ids)

                    # Issue 3: Check if table exists in main schema
                    table_exists = con.execute(f"""
                        SELECT COUNT(*) FROM information_schema.tables 
                        WHERE table_name = '{CLEAN_LEGS_TABLE}' AND table_schema = 'main'
                    """).fetchone()[0] > 0

                    if table_exists:
                        # Use PRAGMA table_info for more reliable column detection in DuckDB
                        db_cols_info = con.execute(f"PRAGMA table_info('{CLEAN_LEGS_TABLE}')").fetchall()
                        db_cols = {row[1]: row[2] for row in db_cols_info}  # {name: type}
                        
                        df_cols = list(df_snapshot.columns)
                        existing_db_cols = set(db_cols.keys())
                        
                        # Identify missing columns
                        missing_in_db = [col for col in df_cols if col not in existing_db_cols]
                        missing_in_df = existing_db_cols - set(df_cols)
                        
                        # Issue 4: Detect type drift for existing columns
                        type_mismatches = []
                        for col in set(df_cols) & existing_db_cols:
                            expected_type = _pandas_dtype_to_duckdb(df_snapshot[col].dtype)
                            actual_type = db_cols[col].upper()
                            # Normalize type names for comparison
                            if expected_type != actual_type and not (
                                # Allow compatible types
                                (expected_type == 'BIGINT' and actual_type in ['INTEGER', 'BIGINT']) or
                                (expected_type == 'DOUBLE' and actual_type in ['FLOAT', 'DOUBLE', 'REAL']) or
                                (expected_type == 'VARCHAR' and 'VARCHAR' in actual_type)
                            ):
                                type_mismatches.append(f"{col}: expected {expected_type}, DB has {actual_type}")
                        
                        if type_mismatches:
                            logger.warning(
                                f"⚠️ Type drift detected in {len(type_mismatches)} columns:\n  " + 
                                "\n  ".join(type_mismatches[:5]) +
                                (f"\n  ... and {len(type_mismatches) - 5} more" if len(type_mismatches) > 5 else "") +
                                "\n  Once created, column types are not altered (permanent drift)."
                            )
                        
                        if missing_in_db:
                            # Issue 1, 4: Migrate schema with proper type inference
                            logger.warning(
                                f"⚠️ Schema migration: adding {len(missing_in_db)} new columns to {CLEAN_LEGS_TABLE}"
                            )
                            for col in missing_in_db:
                                # Double check existence right before adding to prevent race conditions or duplicate entries in missing_in_db
                                check_exists = con.execute(f"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{CLEAN_LEGS_TABLE}' AND column_name = ?", [col]).fetchone()[0] > 0
                                if not check_exists:
                                    duckdb_type = _pandas_dtype_to_duckdb(df_snapshot[col].dtype)
                                    con.execute(f'ALTER TABLE {CLEAN_LEGS_TABLE} ADD COLUMN "{col}" {duckdb_type}')
                                    logger.info(f"  Added column: {col} ({duckdb_type})")
                        
                        if missing_in_df:
                            # Columns exist in DB but not in current snapshot (warn but continue)
                            logger.warning(
                                f"⚠️ {len(missing_in_df)} columns in DB not present in snapshot. "
                                f"These will be NULL for this run_id."
                            )
                    else:
                        # Issue 2, 10: Create table with explicit schema (not LIMIT 0 inference)
                        logger.info(f"Creating new table: {CLEAN_LEGS_TABLE}")
                        
                        # Build explicit CREATE TABLE statement with proper types
                        col_definitions = []
                        for col in df_snapshot.columns:
                            duckdb_type = _pandas_dtype_to_duckdb(df_snapshot[col].dtype)
                            col_definitions.append(f'"{col}" {duckdb_type}')
                        
                        create_sql = f"CREATE TABLE {CLEAN_LEGS_TABLE} ({', '.join(col_definitions)})"
                        con.execute(create_sql)
                        logger.info(f"  Created table with {len(col_definitions)} columns (explicit schema)")

                    # Issue 1: Insert with explicit column list (avoid implicit column order dependency)
                    # Now includes all metadata and entry columns in a single operation
                    col_list = ', '.join([f'"{col}"' for col in df_snapshot.columns])
                    con.execute(f"INSERT INTO {CLEAN_LEGS_TABLE} ({col_list}) SELECT {col_list} FROM df_snapshot")
                    
                    logger.info(f"✅ Snapshot saved with {len(df_snapshot)} rows and {len(df_snapshot.columns)} columns")
                    
                    # Issue 14: Commit transaction
                    con.execute("COMMIT")
                    db_success = True
                    logger.info(f"✅ DuckDB snapshot appended: {db_path} [run_id={run_id}, {len(df_snapshot)} rows]")
                    
                except Exception as e:
                    # Issue 14: Rollback on error
                    con.execute("ROLLBACK")
                    raise
                    
        except Exception as e:
            logger.error(f"❌ Failed to save to DuckDB: {e}")
            db_success = False
            # Issue 4: If DB fails, don't write CSV (atomicity enforcement)
            if to_csv:
                logger.error(f"❌ Skipping CSV save due to DB failure (atomicity enforcement)")
            raise RuntimeError(f"Failed to save snapshot to DuckDB: {e}") from e
    
    # Issue 4: Write CSV only after DB succeeds (atomicity guarantee)
    # DB = truth ledger (required), CSV = convenience (optional)
    if to_csv and db_success:
        try:
            snapshot_path = str(save_snapshot(df_snapshot))
            csv_success = True
            logger.info(f"✅ Snapshot saved to CSV: {snapshot_path}")
        except Exception as e:
            logger.error(f"❌ Failed to save snapshot CSV: {e}")
            csv_success = False
            # CSV failure after DB success is non-critical (truth ledger preserved)
            logger.warning(f"⚠️ DB snapshot succeeded but CSV failed. Data preserved in truth ledger.")
    elif to_csv and not db_success:
        logger.warning(f"⚠️ Skipping CSV save due to DB failure (atomicity enforcement)")

    # Issue 7: Return detailed success status
    return df_snapshot, snapshot_path, run_id, csv_success, db_success
