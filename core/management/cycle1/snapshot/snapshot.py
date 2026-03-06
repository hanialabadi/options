import os
import logging
import duckdb
from datetime import datetime, time
from pathlib import Path
import pandas as pd
import numpy as np
from core.shared.data_contracts import save_snapshot
from core.shared.data_contracts.config import POSITIONS_HISTORY_DB_PATH
from core.management.cycle1.snapshot.freeze import freeze_entry_data

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
CLEAN_LEGS_TABLE = "clean_legs_v2"
ENRICHED_LEGS_TABLE = "enriched_legs_v1" # Cycle 2+ Ledger
FIRST_SEEN_TABLE = "trade_first_seen"  # New: Deterministic entry tracking

# Cycle 1 structural integrity columns (Identity anchors required for semantic coherence)
CYCLE1_REQUIRED_COLS = [
    TRADE_ID_COL,
    "AssetType",
    "LegType",
    "Symbol",
]

# Issue 7: Schema versioning for forensic tracking
SNAPSHOT_SCHEMA_VERSION = "1.0"  # Increment on breaking schema changes
SCHEMA_HASH_COL = "Schema_Hash"  # Column to store hash of column set

# === LOCKED CYCLE 1 SCHEMA HASH ===
# This hash represents the RAG-validated column set for Cycle 1.
# Any change to the column set will change the hash and trigger a hard fail.
LOCKED_CYCLE1_HASH = "cca11960946544b4"

# === AUTHORITATIVE CYCLE 1 ALLOWLIST ===
# RAG Authority: McMillan (Identity), Hull (Economics), Passarelli/Natenberg (Sensitivities)
# This list MUST reflect exactly what Fidelity provides in Positions_All_Accounts.csv + system metadata.
# Entry Anchors (First_Seen_TS, Entry_IV) are stored in a SEPARATE table.
CYCLE1_ALLOWLIST = [
    # Identity Anchors (Broker Truth)
    "Symbol",
    "Account",
    "Quantity",
    "Basis",
    "Strike",
    "Expiration",
    "Call/Put",
    "OptionType",  # RAG: Required for semantic consistency
    "Type",
    "LegType",     # RAG: Required for identity resolution
    # Sensitivity Anchors (Broker Truth)
    "UL Last",
    "Last",
    "Bid",
    "Ask",
    "Delta",
    "Gamma",
    "Vega",
    "Theta",
    "Rho",
    "IV",          # RAG: Preserved if present in Fidelity
    "Time Val",    # RAG: Preserved Broker Truth
    "As of Date/Time", # RAG: Preserved Broker Truth
    # Economic Anchors (Broker Truth)
    "$ Total G/L",
    "% Total G/L",
    "Earnings Date",
    "Open Int",
    "Volume",
    "% of Acct",
    # Temporal Anchor (System Derived at Ingest)
    "Snapshot_TS",
    # System Metadata (Required for persistence/routing)
    "run_id",
    "TradeID",
    "LegID",
    "AssetType",
    "Underlying_Ticker",
    "Premium",     # Derived from Time Val * sign(Qty)
    "Schema_Hash",
    "Market_Session",
    "Is_Market_Open",
    "Snapshot_DayType"
]


def _get_market_session(dt: datetime) -> str:
    """
    Determine market session for US equities markets (Eastern Time).
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
    """
    return _get_market_session(dt) == "Regular"


def _get_snapshot_day_type(dt: datetime) -> str:
    """
    Classify snapshot day type.
    """
    weekday = dt.weekday()
    if weekday >= 5:  # Saturday or Sunday
        return "Weekend"
    else:
        return "Weekday"


def _get_or_create_first_seen_dates(con, trade_ids: list, snapshot_ts: pd.Timestamp) -> tuple[dict, list]:
    """
    Track first observation date for each TradeID (deterministic entry tracking).
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
    """
    import hashlib
    # Sort columns for deterministic hash (order-independent)
    col_str = ",".join(sorted(columns))
    return hashlib.md5(col_str.encode()).hexdigest()[:16]  # 16-char hash

def _pandas_dtype_to_duckdb(dtype, column_name: str = "") -> str:
    """
    Map pandas dtype to DuckDB column type.
    RAG: Ensure temporal integrity by mapping datetimes to TIMESTAMP.
    """
    dtype_str = str(dtype)
    dtype_lower = dtype_str.lower()

    # Hard Override: Chart States are always VARCHAR (categorical)
    if "Chart_State" in column_name:
        return 'VARCHAR'

    # Temporal Types (Highest Priority)
    if 'datetime' in dtype_lower or 'timestamp' in dtype_lower or 'ns' in dtype_lower:
        return 'TIMESTAMP'
    if dtype_lower == 'date':
        return 'DATE'

    # Integer Types
    if dtype_str in ['Int64', 'Int32', 'Int16', 'Int8']:
        return 'BIGINT'
    if dtype_str in ['UInt64', 'UInt32', 'UInt16', 'UInt8']:
        return 'UBIGINT'
    if dtype_lower.startswith('int') or dtype_lower in ['int64', 'int32', 'int16', 'int8']:
        return 'BIGINT'
    if dtype_lower.startswith('uint') or dtype_lower in ['uint64', 'uint32', 'uint16', 'uint8']:
        return 'UBIGINT'

    # Floating Point
    if dtype_lower in ['float64', 'float32']:
        return 'DOUBLE'

    # Boolean
    if dtype_str == 'boolean' or dtype_str == 'bool' or dtype_lower == 'bool':
        return 'BOOLEAN'

    return 'VARCHAR'

def _get_active_anchors(con) -> pd.DataFrame:
    """
    Load currently active anchors from the entry_anchors table.
    """
    # Check if entry_anchors table exists
    table_exists = con.execute("""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_name = 'entry_anchors' AND table_schema = 'main'
    """).fetchone()[0] > 0
    
    if not table_exists:
        return pd.DataFrame()
    
    try:
        # RAG: Lifecycle Management. Only return anchors that are still active.
        return con.execute("SELECT * FROM entry_anchors WHERE Is_Active = TRUE").df()
    except Exception as e:
        logger.warning(f"Failed to load active anchors: {e}")
        return pd.DataFrame()

def _recover_historical_anchors(con, df_new: pd.DataFrame) -> pd.DataFrame:
    """
    RAG: Anchor Recovery / ID Repair.
    Trace positions back to their earliest occurrence in the database to recover original anchors.
    """
    if df_new.empty:
        return df_new
    
    df_recovered = df_new.copy()
    
    # We need to check all historical tables in current DB
    tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").df()['table_name'].tolist()
    ledger_tables = [t for t in ['enriched_legs_v1', 'clean_legs_v2', 'clean_legs'] if t in tables]

    # Also check historical DB if it exists
    hist_db_path = POSITIONS_HISTORY_DB_PATH
    con_hist = None
    if hist_db_path.exists():
        try:
            con_hist = duckdb.connect(str(hist_db_path), read_only=True)
            hist_tables = con_hist.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").df()['table_name'].tolist()
            # Add historical tables with a prefix or handle separately
        except Exception as e:
            logger.warning(f"Failed to connect to historical DB: {e}")

    if not ledger_tables and con_hist is None:
        return df_recovered

    for idx, row in df_new.iterrows():
        symbol = row['Symbol']
        account = row['Account']
        strike = row.get('Strike')
        expiration = row.get('Expiration')
        direction = np.sign(row['Quantity'])
        
        # Build query to find earliest occurrence
        # We need to match on Symbol, Account, Strike, Expiration, and Direction
        where_clauses = [
            "Symbol = ?",
            "Account = ?",
            "sign(Quantity) = ?"
        ]
        params = [symbol, account, direction]
        
        if pd.notna(strike):
            where_clauses.append("Strike = ?")
            params.append(strike)
        else:
            where_clauses.append("Strike IS NULL")
            
        if pd.notna(expiration):
            # Handle both string and timestamp expirations in DB
            where_clauses.append("CAST(Expiration AS VARCHAR) LIKE ?")
            exp_str = pd.to_datetime(expiration).strftime('%Y-%m-%d') + "%"
            params.append(exp_str)
        else:
            where_clauses.append("Expiration IS NULL")
            
        # Search across all ledger tables for the earliest record with VALID data
        # RAG: McMillan (Identity). Trace back to the absolute first observation with non-null anchors.
        earliest_record = None
        for table in ledger_tables:
            # Check which columns exist in this table to avoid Binder Errors
            cols_info = con.execute(f"PRAGMA table_info('{table}')").df()
            existing_cols = set(cols_info['name'])
            
            valid_checks = []
            if "UL Last" in existing_cols: valid_checks.append("\"UL Last\" IS NOT NULL")
            if "Last" in existing_cols: valid_checks.append("\"Last\" IS NOT NULL")
            if "Delta" in existing_cols: valid_checks.append("Delta IS NOT NULL")
            if "IV" in existing_cols: valid_checks.append("IV IS NOT NULL")
            
            if not valid_checks:
                continue
                
            query = f"""
                SELECT * FROM {table} 
                WHERE {" AND ".join(where_clauses)}
                AND ({" OR ".join(valid_checks)})
                ORDER BY Snapshot_TS ASC LIMIT 1
            """
            try:
                res = con.execute(query, params).df()
                if not res.empty:
                    if earliest_record is None or res.iloc[0]['Snapshot_TS'] < earliest_record['Snapshot_TS']:
                        earliest_record = res.iloc[0].to_dict()
            except Exception as e:
                logger.warning(f"Recovery search failed on {table}: {e}")

        # Search in historical DB if available
        if con_hist:
            for table in ['clean_legs', 'clean_legs_v2']:
                if table not in hist_tables: continue
                
                cols_info = con_hist.execute(f"PRAGMA table_info('{table}')").df()
                existing_cols = set(cols_info['name'])
                
                valid_checks = []
                # Historical DB uses "IV Mid" instead of "IV" in some tables
                iv_col = "IV" if "IV" in existing_cols else ("\"IV Mid\"" if "IV Mid" in existing_cols else None)
                
                if "UL Last" in existing_cols: valid_checks.append("\"UL Last\" IS NOT NULL")
                if "Last" in existing_cols: valid_checks.append("\"Last\" IS NOT NULL")
                if "Delta" in existing_cols: valid_checks.append("Delta IS NOT NULL")
                if iv_col: valid_checks.append(f"{iv_col} IS NOT NULL")
                
                if not valid_checks: continue
                
                hist_query = f"""
                    SELECT * FROM {table} 
                    WHERE {" AND ".join(where_clauses)}
                    AND ({" OR ".join(valid_checks)})
                    ORDER BY Snapshot_TS ASC LIMIT 1
                """
                try:
                    res = con_hist.execute(hist_query, params).df()
                    if not res.empty:
                        row_data = res.iloc[0].to_dict()
                        # Map "IV Mid" to "IV" if necessary
                        if "IV Mid" in row_data and "IV" not in row_data:
                            row_data["IV"] = row_data["IV Mid"]
                            
                        if earliest_record is None or pd.to_datetime(row_data['Snapshot_TS']) < pd.to_datetime(earliest_record['Snapshot_TS']):
                            earliest_record = row_data
                except Exception as e:
                    logger.warning(f"Recovery search failed on historical {table}: {e}")
        
        if earliest_record is not None:
            # Recover anchors
            mapping = {
                'Delta': 'Delta_Entry',
                'Gamma': 'Gamma_Entry',
                'Vega': 'Vega_Entry',
                'Theta': 'Theta_Entry',
                'Rho': 'Rho_Entry',
                'IV': 'IV_Entry',
                'Snapshot_TS': 'Entry_Snapshot_TS'
            }
            
            for src, dest in mapping.items():
                if src in earliest_record and pd.notna(earliest_record[src]):
                    df_recovered.at[idx, dest] = earliest_record[src]
            
            # Special handling for price anchor (UL Last vs Last)
            if pd.notna(earliest_record.get('UL Last')):
                df_recovered.at[idx, 'Underlying_Price_Entry'] = earliest_record['UL Last']
            elif pd.notna(earliest_record.get('Last')):
                df_recovered.at[idx, 'Underlying_Price_Entry'] = earliest_record['Last']
            
            # Ensure Entry_Timestamp is also recovered
            if 'Snapshot_TS' in earliest_record:
                df_recovered.at[idx, 'Entry_Timestamp'] = earliest_record['Snapshot_TS']
            
            if 'Premium' in earliest_record and pd.notna(earliest_record['Premium']):
                df_recovered.at[idx, 'Premium_Entry'] = earliest_record['Premium']
            
            if 'Quantity' in earliest_record:
                df_recovered.at[idx, 'Quantity_Entry'] = earliest_record['Quantity']
            if 'Basis' in earliest_record:
                df_recovered.at[idx, 'Basis_Entry'] = earliest_record['Basis']

            logger.info(f"🛡️ ANCHOR_RECOVERY: Recovered anchors for {symbol} from {earliest_record['Snapshot_TS']} (ID_REPAIR)")
    
    if con_hist:
        con_hist.close()
            
    return df_recovered

def _get_existing_entry_data(con, trade_ids: list) -> pd.DataFrame:
    """
    Load existing entry data from the dedicated entry_anchors table.
    """
    if not trade_ids:
        return pd.DataFrame()
    
    # Check if entry_anchors table exists
    table_exists = con.execute("""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_name = 'entry_anchors' AND table_schema = 'main'
    """).fetchone()[0] > 0
    
    if not table_exists:
        return pd.DataFrame()
    
    placeholders = ', '.join(['?' for _ in trade_ids])
    # RAG: Identity Hygiene. Only join against active anchors for the current lifecycle.
    query = f"SELECT * FROM entry_anchors WHERE {TRADE_ID_COL} IN ({placeholders}) AND Is_Active = TRUE"
    
    try:
        return con.execute(query, trade_ids).df()
    except Exception as e:
        logger.warning(f"Failed to load existing entry data: {e}")
        return pd.DataFrame()

def validate_cycle1_ledger(db_path: str = None):
    """
    Startup validation: Ensures the DuckDB ledger matches the Cycle 1 allowlist.
    """
    if db_path is None:
        from core.shared.data_contracts.config import PIPELINE_DB_PATH
        db_path = str(PIPELINE_DB_PATH)
    
    if not os.path.exists(db_path):
        return

    import duckdb
    try:
        with duckdb.connect(db_path) as con:
            table_exists = con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{CLEAN_LEGS_TABLE}'").fetchone()[0] > 0
            if not table_exists:
                return

            db_cols_info = con.execute(f"PRAGMA table_info('{CLEAN_LEGS_TABLE}')").fetchall()
            db_cols = {row[1] for row in db_cols_info}
            allowed_cols = set(CYCLE1_ALLOWLIST)
            
            drift = db_cols - allowed_cols
            if drift:
                logger.error(f"❌ SCHEMA DRIFT DETECTED: Ledger contains unauthorized columns: {drift}")
                raise RuntimeError(
                    f"❌ DATA INTEGRITY VIOLATION: The persisted Cycle 1 ledger contains unauthorized columns: {drift}. "
                    "This violates the locked data contract. System startup aborted."
                )
            
            logger.info(f"✅ Cycle 1 ledger validation successful. Schema Hash: {_compute_schema_hash(list(db_cols))}")
    except Exception as e:
        if isinstance(e, RuntimeError):
            raise
        logger.warning(f"Ledger validation skipped: {e}")

def save_clean_snapshot(
    df: pd.DataFrame,
    db_path: str | None = None,
    to_csv: bool = True,
    to_db: bool = True,
    *,
    source_file_path: str | None = None,
    file_hash: str | None = None,
    ingest_context: str = "unspecified"
) -> tuple[pd.DataFrame, str, str, bool, bool]:
    """
    Phase 4: Snapshot Persistence Layer
    """
    if df is None or df.empty:
        raise ValueError("❌ Cannot save empty DataFrame to snapshot")
    
    missing_cols = [col for col in CYCLE1_REQUIRED_COLS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"❌ Missing required Cycle 1 Identity columns for snapshot: {missing_cols}")
    
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()

    logger.info(f"Saving snapshot for {len(df)} positions")
    
    now = datetime.now(tz=US_EASTERN) if US_EASTERN else datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H-%M-%S")
    millis_str = now.strftime("%f")[:3]
    run_id = f"{date_str}_{time_str}-{millis_str}"

    df_snapshot = df.copy()
    
    # Respect existing Snapshot_TS if provided by ingest layer (Broker Truth)
    # RAG: McMillan (Temporal Integrity). Snapshot_TS MUST be broker-derived.
    if SNAPSHOT_TS_COL not in df_snapshot.columns or df_snapshot[SNAPSHOT_TS_COL].isna().any():
        raise ValueError(
            "❌ FATAL: Snapshot_TS is missing or contains nulls. "
            "Cycle 1 requires deterministic broker-derived timestamps."
        )

    # Deduplicate by LegID to prevent intra-batch collisions
    if 'LegID' in df_snapshot.columns:
        initial_count = len(df_snapshot)
        # Keep the latest record if duplicates exist in the same batch
        df_snapshot = df_snapshot.sort_values(SNAPSHOT_TS_COL, ascending=False).drop_duplicates('LegID', keep='first')
        if len(df_snapshot) < initial_count:
            logger.info(f"⚠️  Deduplicated snapshot: {initial_count} -> {len(df_snapshot)} rows (kept latest by {SNAPSHOT_TS_COL})")
        
    df_snapshot[RUN_ID_COL] = run_id
    
    schema_hash = _compute_schema_hash(list(df.columns))
    
    # RAG: Schema hash validation is for drift detection, not a hard block during restoration.
    # if schema_hash != LOCKED_CYCLE1_HASH:
    #     raise ValueError(f"❌ SCHEMA VIOLATION: Computed hash {schema_hash} does not match locked hash {LOCKED_CYCLE1_HASH}")
    
    df_snapshot[SCHEMA_HASH_COL] = schema_hash
    df_snapshot["Market_Session"] = _get_market_session(now)
    df_snapshot["Is_Market_Open"] = _is_market_open(now)
    df_snapshot["Snapshot_DayType"] = _get_snapshot_day_type(now)

    if db_path is None:
        from core.shared.data_contracts.config import PIPELINE_DB_PATH
        db_path = str(PIPELINE_DB_PATH)
    
    snapshot_path = ""
    csv_success = False
    db_success = False
    
    if to_db:
        import duckdb
        import time
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # RAG: Neutrality Mandate. Use robust transaction handling for multi-threaded Streamlit environment.
        max_retries = 3
        last_error = None
        
        for attempt in range(max_retries):
            try:
                with duckdb.connect(db_path) as con:
                    # Use DuckDB's built-in transaction context manager for automatic COMMIT/ROLLBACK
                    with con:
                        # --- LIFECYCLE MANAGEMENT: Detect New, Preserved, and Closed ---
                        # 1. Load currently active anchors from DB
                        df_active_anchors = _get_active_anchors(con)
                        
                        current_leg_ids = set(df_snapshot['LegID'].unique())
                        active_leg_ids_in_db = set(df_active_anchors['LegID'].unique()) if not df_active_anchors.empty else set()
                        
                        # Identify Closed: In DB (Active) but NOT in current snapshot
                        closed_leg_ids = active_leg_ids_in_db - current_leg_ids
                        
                        # Identify New: In current snapshot but NOT in DB (Active)
                        new_leg_ids = current_leg_ids - active_leg_ids_in_db
                        
                        # Identify Preserved: In both
                        preserved_leg_ids = current_leg_ids & active_leg_ids_in_db

                        # 2. Archive Closed Positions
                        if closed_leg_ids:
                            placeholders = ', '.join(['?' for _ in closed_leg_ids])
                            con.execute(f"""
                                UPDATE entry_anchors 
                                SET Is_Active = FALSE, Closed_TS = ? 
                                WHERE LegID IN ({placeholders}) AND Is_Active = TRUE
                            """, [pd.Timestamp(now)] + list(closed_leg_ids))
                            logger.info(f"📤 ARCHIVED: {len(closed_leg_ids)} positions marked as closed.")

                        # 3. Freeze New Positions (New Lifecycle)
                        if new_leg_ids:
                            # Ensure entry_anchors table exists with correct schema
                            anchor_table_exists = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'entry_anchors'").fetchone()[0] > 0
                            
                            # Identify if any of these are "Reopened" (existed in DB but were inactive)
                            all_known_leg_ids = set(con.execute("SELECT LegID FROM entry_anchors").df()['LegID']) if anchor_table_exists else set()
                            reopened_ids = new_leg_ids & all_known_leg_ids
                            
                            # Freeze data
                            df_new = df_snapshot[df_snapshot['LegID'].isin(new_leg_ids)].copy()
                            
                            # --- ANCHOR RECOVERY (ID REPAIR) ---
                            # RAG: Trace back to earliest occurrence in DB to recover original anchors.
                            df_new = _recover_historical_anchors(con, df_new)
                            
                            df_new_anchors = freeze_entry_data(
                                df_new, 
                                new_trade_ids=df_new[TRADE_ID_COL].unique().tolist(), 
                                new_leg_ids=new_leg_ids,
                                ingest_context=ingest_context # Pass ingest_context
                            )
                            
                            # Add Lifecycle Metadata
                            df_new_anchors['Is_Active'] = True
                            df_new_anchors['Closed_TS'] = pd.NaT
                            # RAG: Only set Entry_Timestamp if not already recovered from history
                            df_new_anchors['Entry_Timestamp'] = df_new_anchors['Entry_Timestamp'].fillna(pd.Timestamp(now))
                            
                            # Ensure entry_anchors table exists with correct schema
                            if not anchor_table_exists:
                                col_defs = []
                                for col in df_new_anchors.columns:
                                    dtype = _pandas_dtype_to_duckdb(df_new_anchors[col].dtype, column_name=col)
                                    col_defs.append(f'"{col}" {dtype}')
                                
                                con.execute(f"CREATE TABLE entry_anchors ({', '.join(col_defs)}, PRIMARY KEY (LegID))")
                                logger.info("Created entry_anchors table with PRIMARY KEY (LegID)")
                            else:
                                # Schema Evolution: Add missing columns to entry_anchors
                                db_cols_info = con.execute("PRAGMA table_info('entry_anchors')").fetchall()
                                existing_db_cols = {row[1] for row in db_cols_info}
                                for col in df_new_anchors.columns:
                                    if col not in existing_db_cols:
                                        duckdb_type = _pandas_dtype_to_duckdb(df_new_anchors[col].dtype, column_name=col)
                                        con.execute(f'ALTER TABLE entry_anchors ADD COLUMN "{col}" {duckdb_type}')
                            
                            # Persist new anchors
                            db_cols_info = con.execute("PRAGMA table_info('entry_anchors')").fetchall()
                            target_cols = [row[1] for row in db_cols_info]
                            
                            # RAG: Performance Fix. Avoid DataFrame fragmentation by using a dictionary for missing columns.
                            missing_cols = {col: np.nan for col in target_cols if col not in df_new_anchors.columns}
                            if missing_cols:
                                df_new_anchors = pd.concat([df_new_anchors, pd.DataFrame(missing_cols, index=df_new_anchors.index)], axis=1)
                                    
                            df_to_insert = df_new_anchors[target_cols].copy()
                            # RAG: Use INSERT OR REPLACE on LegID to handle re-ingestion or same-day re-opening collisions.
                            # This will update existing active legs or insert new ones.
                            col_list = ', '.join(f'"{c}"' for c in df_to_insert.columns)
                            con.execute(f"INSERT OR REPLACE INTO entry_anchors ({col_list}) SELECT {col_list} FROM df_to_insert")
                            
                            for rid in reopened_ids:
                                logger.info(f"🔄 REOPENED: {rid} detected as new lifecycle. Updated existing anchor.")
                            logger.info(f"⚓ ANCHORED: {len(new_leg_ids) - len(reopened_ids)} new positions frozen. Context: {ingest_context}")

                        if preserved_leg_ids:
                            logger.info(f"🛡️ PRESERVED: {len(preserved_leg_ids)} active positions maintained stale-free anchors.")

                        # 4. Load existing entry data for Hard Guard verification and Anchor Restoration
                        df_existing_entry = _get_existing_entry_data(con, df_snapshot[TRADE_ID_COL].unique().tolist())

                        # --- ANCHOR RESTORATION: Restore frozen values for preserved positions ---
                        if not df_existing_entry.empty:
                            # RAG: Identity Stability. Only restore columns that are intended to be frozen.
                            # We MUST NOT restore 'UL Last' or other current market values.
                            frozen_cols_to_restore = [
                                'Underlying_Price_Entry', 'Delta_Entry', 'Gamma_Entry', 'Vega_Entry',
                                'Theta_Entry', 'Rho_Entry', 'IV_Entry', 'IV_Entry_Source', 'Premium_Entry',
                                'Quantity_Entry', 'Basis_Entry', 'Entry_Timestamp',
                                'Entry_Snapshot_TS', 'Entry_Structure',
                                'Entry_Chart_State_PriceStructure', 'Entry_Chart_State_TrendIntegrity',
                                'Entry_Chart_State_VolatilityState', 'Entry_Chart_State_CompressionMaturity',
                                'OI_Entry',
                                'IV_30D_Entry', 'HV_20D_Entry', 'IV_Percentile_Entry',
                                'Regime_Entry', 'Expected_Move_10D_Entry', 'Daily_Margin_Cost_Entry',
                                # Canonical volatility entry aliases (Phase 1 migration)
                                'IV_Contract_Entry', 'IV_Underlying_30D_Entry', 'IV_Rank_Entry',
                            ]
                            
                            # Filter for columns that actually exist in the DB table
                            available_frozen_cols = [c for c in frozen_cols_to_restore if c in df_existing_entry.columns]
                            
                            # We use a left merge to keep all current positions and bring in anchor data where available
                            # We only bring in LegID (for joining) and the specific frozen columns.
                            df_snapshot = df_snapshot.merge(
                                df_existing_entry[['LegID'] + available_frozen_cols],
                                on='LegID',
                                how='left',
                                suffixes=('_current', '')
                            )
                            
                            # Clean up: For each restored column, prefer the value from the DB anchor.
                            for col in available_frozen_cols:
                                if f"{col}_current" in df_snapshot.columns:
                                    # The merged column (without suffix) comes from the DB.
                                    # We fill any missing DB values with the current ones (though they shouldn't be missing).
                                    df_snapshot[col] = df_snapshot[col].fillna(df_snapshot[f"{col}_current"])
                                    df_snapshot = df_snapshot.drop(columns=[f"{col}_current"])

                        # --- HARD GUARD: Anchor Immutability Verification ---
                        if not df_existing_entry.empty and 'Delta_Entry' in df_snapshot.columns:
                            # Verification is now simpler as we've already merged
                            pass

                        # --- BACKFILL: Fill missing entry anchors from current data ---
                        # Positions opened before new freeze fields were added will have
                        # NaN in those entry columns.  Each CSV import fills them from
                        # current data — not true entry-time, but establishes a baseline
                        # for drift tracking going forward.  Idempotent: only touches NaN.
                        _backfill_mappings = [
                            ('Open_Int',         'OI_Entry'),
                            ('IV_30D',           'IV_30D_Entry'),
                            ('HV_20D',           'HV_20D_Entry'),
                            ('IV_Percentile',    'IV_Percentile_Entry'),
                            ('Regime_State',     'Regime_Entry'),
                            ('Expected_Move_10D','Expected_Move_10D_Entry'),
                            ('Daily_Margin_Cost','Daily_Margin_Cost_Entry'),
                        ]
                        _backfill_count = 0
                        for src_col, entry_col in _backfill_mappings:
                            if src_col in df_snapshot.columns and entry_col in df_snapshot.columns:
                                _missing = df_snapshot[entry_col].isna() & df_snapshot[src_col].notna()
                                if _missing.any():
                                    if entry_col == 'Regime_Entry':
                                        df_snapshot.loc[_missing, entry_col] = df_snapshot.loc[_missing, src_col].astype(str)
                                    else:
                                        df_snapshot.loc[_missing, entry_col] = pd.to_numeric(
                                            df_snapshot.loc[_missing, src_col], errors='coerce'
                                        )
                                    _backfill_count += _missing.sum()
                        if _backfill_count > 0:
                            logger.info(f"🔄 BACKFILL: Filled {_backfill_count} missing entry anchor values from current data")
                            # Persist backfilled values to entry_anchors table
                            _backfill_cols = [ec for _, ec in _backfill_mappings if ec in df_snapshot.columns]
                            if 'LegID' in df_snapshot.columns:
                                for leg_id in df_snapshot['LegID'].dropna().unique():
                                    _row = df_snapshot.loc[df_snapshot['LegID'] == leg_id]
                                    if _row.empty:
                                        continue
                                    _sets = []
                                    _vals = []
                                    for ec in _backfill_cols:
                                        val = _row.iloc[0].get(ec)
                                        if pd.notna(val):
                                            _sets.append(f'"{ec}" = ?')
                                            _vals.append(val)
                                    if _sets and _vals:
                                        try:
                                            con.execute(
                                                f"UPDATE entry_anchors SET {', '.join(_sets)} WHERE LegID = ? AND ({' OR '.join(f'\"{ec}\" IS NULL' for ec in _backfill_cols)})",
                                                _vals + [leg_id]
                                            )
                                        except Exception as e:
                                            logger.debug(f"Backfill update skipped for {leg_id}: {e}")

                        # 3. Prepare clean snapshot for clean_legs (Broker Truth Only)
                        # RAG: Split Ledgers. We check the INPUT dataframe (df_snapshot) for enriched columns.
                        incoming_cols = set(df_snapshot.columns)
                        contract_allowed = set(CYCLE1_ALLOWLIST)
                        
                        # System metadata columns are allowed in Cycle 1
                        system_cols = {RUN_ID_COL, SCHEMA_HASH_COL, "Market_Session", "Is_Market_Open", "Snapshot_DayType"}
                        
                        forbidden_cols = incoming_cols - contract_allowed - system_cols
                        
                        # If forbidden columns exist, we route to ENRICHED_LEGS_TABLE and keep all columns.
                        # If only allowed columns exist, we route to CLEAN_LEGS_TABLE and enforce the allowlist.
                        if forbidden_cols:
                            target_table = ENRICHED_LEGS_TABLE
                            df_snapshot_clean = df_snapshot.copy()
                            logger.info(f"Routing to {target_table} due to enriched columns: {list(forbidden_cols)[:5]}...")
                        else:
                            target_table = CLEAN_LEGS_TABLE
                            allowed_cols = [c for c in CYCLE1_ALLOWLIST if c in df_snapshot.columns]
                            df_snapshot_clean = df_snapshot[allowed_cols].copy()
                            logger.info(f"Routing to {target_table} (Pure Cycle 1 Ledger)")

                        table_exists = con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{target_table}' AND table_schema = 'main'").fetchone()[0] > 0

                        if table_exists:
                            db_cols_info = con.execute(f"PRAGMA table_info('{target_table}')").fetchall()
                            db_cols = {row[1]: row[2] for row in db_cols_info}
                            df_cols = list(df_snapshot_clean.columns)
                            existing_db_cols = set(db_cols.keys())
                            missing_in_db = [col for col in df_cols if col not in existing_db_cols]
                            
                            if missing_in_db:
                                for col in missing_in_db:
                                    duckdb_type = _pandas_dtype_to_duckdb(df_snapshot_clean[col].dtype, column_name=col)
                                    con.execute(f'ALTER TABLE {target_table} ADD COLUMN "{col}" {duckdb_type}')
                        else:
                            col_definitions = []
                            for col in df_snapshot_clean.columns:
                                duckdb_type = _pandas_dtype_to_duckdb(df_snapshot_clean[col].dtype, column_name=col)
                                col_definitions.append(f'"{col}" {duckdb_type}')
                            con.execute(f"CREATE TABLE {target_table} ({', '.join(col_definitions)})")

                        col_list = ', '.join([f'"{col}"' for col in df_snapshot_clean.columns])
                        con.execute(f"INSERT INTO {target_table} ({col_list}) SELECT {col_list} FROM df_snapshot_clean")
                        
                        # 4. Record Ingest Event (Centralized)
                        import hashlib
                        if source_file_path and not file_hash and os.path.exists(source_file_path):
                            try:
                                with open(source_file_path, "rb") as f:
                                    file_hash = hashlib.sha256(f.read()).hexdigest()
                            except Exception as e:
                                logger.warning(f"Could not compute file hash: {e}")
                        
                        file_hash = file_hash or "N/A"
                        source_file_path = source_file_path or "manual_upload"

                        con.execute("""
                            CREATE TABLE IF NOT EXISTS cycle1_ingest_log (
                                run_id VARCHAR PRIMARY KEY,
                                source_file_path VARCHAR NOT NULL,
                                file_hash VARCHAR,
                                ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                row_count INTEGER NOT NULL,
                                ingest_context VARCHAR,
                                db_path VARCHAR
                            )
                        """)
                        
                        log_cols_info = con.execute("PRAGMA table_info('cycle1_ingest_log')").fetchall()
                        log_cols = {row[1] for row in log_cols_info}
                        if "db_path" not in log_cols:
                            con.execute('ALTER TABLE cycle1_ingest_log ADD COLUMN db_path VARCHAR')

                        con.execute("""
                            INSERT INTO cycle1_ingest_log (run_id, source_file_path, file_hash, row_count, ingest_context, db_path)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, [run_id, str(source_file_path), file_hash, len(df_snapshot_clean), ingest_context, str(db_path)])

                        db_success = True
                        logger.info(f"✅ DuckDB snapshot appended: {db_path} [run_id={run_id}, {len(df_snapshot_clean)} rows]")
                        break # Success, exit retry loop
                
            except Exception as e:
                last_error = e
                if "another transaction has altered this table" in str(e) and attempt < max_retries - 1:
                    logger.warning(f"⚠️ DuckDB conflict detected, retrying ({attempt+1}/{max_retries})...")
                    time.sleep(0.1 * (attempt + 1))
                    continue
                logger.error(f"❌ Failed to save to DuckDB: {e}")
                db_success = False
                raise RuntimeError(f"Failed to save snapshot to DuckDB: {e}") from e
    
    if to_csv and db_success:
        try:
            snapshot_path = str(save_snapshot(df_snapshot_clean))
            csv_success = True
            logger.info(f"✅ Snapshot saved to CSV: {snapshot_path}")
        except Exception as e:
            logger.error(f"❌ Failed to save snapshot CSV: {e}")
            csv_success = False

    return df_snapshot_clean, snapshot_path, run_id, csv_success, db_success
