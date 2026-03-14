import pandas as pd
import numpy as np
import re
import inspect
import logging # Import logging
from datetime import datetime
from pathlib import Path
from typing import Tuple
from core.shared.data_contracts.config import PROJECT_ROOT
from core.management.cycle1.identity.constants import OCC_OPTION_PATTERN, IGNORED_SYMBOLS

logger = logging.getLogger(__name__) # Initialize logger

# Canonical input path (future-proof for multi-broker support)
CANONICAL_INPUT_PATH = PROJECT_ROOT / "data" / "brokerage_inputs" / "fidelity_positions.csv"
CANONICAL_SNAPSHOT_DIR = PROJECT_ROOT / "data" / "snapshots" / "phase1"
BROKERAGE_INPUTS_DIR = PROJECT_ROOT / "data" / "brokerage_inputs"


def auto_detect_latest_positions(search_dir: Path = None) -> Path:
    """
    Find the newest Fidelity positions CSV by modification time.

    Scans for Positions_All_Account*.csv and Positions_Live_*.csv patterns.
    Falls back to CANONICAL_INPUT_PATH (symlink) if no candidates found.

    RAG: Natenberg Ch.8 — stale data during event windows is catastrophic.
    """
    search_dir = search_dir or BROKERAGE_INPUTS_DIR
    candidates = list(search_dir.glob("Positions_All_Account*.csv"))
    candidates += list(search_dir.glob("Positions_Live_*.csv"))
    if not candidates:
        logger.warning(
            f"[auto_detect] No position CSVs found in {search_dir}. "
            f"Falling back to symlink: {CANONICAL_INPUT_PATH}"
        )
        return CANONICAL_INPUT_PATH
    newest = max(candidates, key=lambda p: p.stat().st_mtime)
    logger.info(f"[auto_detect] Latest positions CSV: {newest.name}")
    return newest

# File parsing constants
FIDELITY_HEADER_ROWS = 2
MAX_SYMBOL_LENGTH = 50

# === CYCLE 1 SCHEMA CONTRACT (LOCKED) ===
# RAG Authority: McMillan (Identity), Hull (Economics), Passarelli/Natenberg (Sensitivities)
# This list MUST reflect exactly what Fidelity provides in Positions_All_Accounts.csv
CYCLE1_WHITELIST = [
    'Symbol',
    'Quantity',
    'UL Last',
    'Last',
    'Bid',
    'Ask',
    'Basis',
    '$ Total G/L',
    'Total_GL_Decimal', # Renamed from '% Total G/L'
    'Earnings Date',
    'Theta',
    'Vega',
    'Gamma',
    'Delta',
    'Rho',
    'Time Val',
    'Account',
    'As of Date/Time',
    'Type',
    'Strike',
    'Call/Put',
    'Expiration',
    'Open Int',
    'Volume',
    '% of Acct',
    'Intrinsic Val',  # ITM intrinsic value — precise assignment risk; 0 for OTM, >0 for ITM
    'IV'  # Optional: Preserved if present
]

# Columns required for derivation but NOT for persistence
DERIVATION_REQUIRED_COLS = [
    'As of Date/Time'
]


def phase1_load_and_clean_positions(
    input_path: Path,
    *,
    save_snapshot: bool = True,
    allow_system_time: bool = False
) -> Tuple[pd.DataFrame, str]:
    """
    Phase 1: Active Position Intake (Management Engine)

    Loads raw brokerage position data and produces a clean, minimal dataset
    suitable for active trade management (drift tracking, P/L monitoring).
    """
    # Ensure input_path is always a Path object
    input_path = Path(input_path)
    
    logger.debug(f"DEBUG: Checking for file existence at: {input_path}")
    if not input_path.exists():
        logger.error(f"❌ Input file not found: {input_path}")
        logger.error(f"   Expected: {input_path.absolute()}")
        return pd.DataFrame(), ""

    # --- Import Audit Logging (Fingerprinting) ---
    import hashlib
    file_bytes = input_path.read_bytes()
    file_hash = hashlib.sha256(file_bytes).hexdigest()
    file_size = len(file_bytes)
    
    logger.info(f"🔍 [IMPORT_AUDIT] File: {input_path.name}")
    logger.info(f"🔍 [IMPORT_AUDIT] Size: {file_size} bytes")
    logger.info(f"🔍 [IMPORT_AUDIT] SHA256: {file_hash}")

    # --- Extract Global Timestamp from Header ---
    header_ts = None
    try:
        # Read first 10 lines to find the "as of" timestamp
        with open(input_path, 'r', encoding='utf-8-sig') as f:
            header_lines = [f.readline() for _ in range(10)]
            header_text = "".join(header_lines)
            # Match "as of 01/26/2026 at 10:37:31 PM"
            match = re.search(r'as of (\d{2}/\d{2}/\d{4}) at (\d{2}:\d{2}:\d{2} [AP]M)', header_text)
            if match:
                dt_str = f"{match.group(1)} {match.group(2)}"
                header_ts = pd.to_datetime(dt_str)
                logger.info(f"🔍 [IMPORT_AUDIT] Header Timestamp: {header_ts}")
    except Exception as e:
        logger.warning(f"⚠️  Warning: Could not extract header timestamp: {e}")

    # --- Staleness Guard ---
    # RAG: Natenberg Ch.8 — stale IV/price data during event windows is catastrophic.
    # Passarelli Ch.6 — roll timing requires fresh Greeks.
    _data_freshness = 'UNKNOWN'
    _data_age_days = -1
    if header_ts is not None:
        try:
            from core.shared.data_layer.market_time import trading_days_diff
            from datetime import date
            _data_age_days = trading_days_diff(header_ts.date(), date.today())
            if _data_age_days > 2:
                _data_freshness = 'STALE'
                logger.warning(
                    f"⚠️ STALENESS WARNING: Broker CSV is {_data_age_days} trading days old "
                    f"(exported {header_ts}). Consider re-exporting from Fidelity."
                )
            else:
                _data_freshness = 'FRESH'
                logger.info(f"✅ CSV freshness: {_data_age_days} trading day(s) old — FRESH")
        except Exception as _staleness_err:
            logger.debug(f"Staleness check failed: {_staleness_err}")

    try:
        # RAG: Smart Header Detection.
        # Fidelity raw exports have 2 metadata rows.
        # If we are re-processing a cleaned file, we shouldn't skip.
        with open(input_path, 'r', encoding='utf-8-sig') as f:
            first_line = f.readline()
            
        skip = FIDELITY_HEADER_ROWS
        if "Symbol" in first_line and "Quantity" in first_line:
            print("🔍 [IMPORT_AUDIT] Clean header detected on line 1. Disabling skiprows.")
            skip = 0
            
        # Skip Fidelity header rows (metadata at top of file)
        # RAG: Use utf-8-sig to handle potential BOM in Fidelity exports
        df = pd.read_csv(input_path, skiprows=skip, encoding='utf-8-sig')
        print(f"🔍 [IMPORT_AUDIT] Raw row count: {len(df)}")
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return pd.DataFrame(), ""

    # Normalize column names (strip whitespace, collapse multiple spaces)
    df.columns = df.columns.str.strip().str.replace(r'[\s]+', ' ', regex=True)
    # RAG: Ensure no duplicate columns after normalization
    df = df.loc[:, ~df.columns.duplicated()].copy()

    # RAG: Global N/A cleanup for Fidelity's '--' marker
    # Use future-proof downcasting opt-in to silence FutureWarning
    with pd.option_context('future.no_silent_downcasting', True):
        df = df.replace('--', np.nan).infer_objects(copy=False)

    # Identify all potential timestamp columns
    TIMESTAMP_COLS = [
        "Snapshot_TS", "Last_Fetch_TS", "Backoff_Until", "Anchor_TS", "AsOf",
        "As of Date/Time", "Entry_Snapshot_TS", "First_Seen_Date", "Entry_Timestamp",
        "Earnings Date"
    ]

    # Enforce datetime conversion for all identified timestamp columns at the earliest ingestion point
    for col in TIMESTAMP_COLS:
        if col in df.columns:
            original_dtype = df[col].dtype
            # Strip Fidelity's " ET" timezone suffix before parsing (not a recognized tz)
            if df[col].dtype == object:
                df[col] = df[col].astype(str).str.replace(r'\s+ET$', '', regex=True)
            df[col] = pd.to_datetime(df[col], errors="coerce")
            if df[col].dtype != original_dtype:
                logger.debug(f"Converted '{col}' from {original_dtype} to {df[col].dtype}")
            if df[col].isna().any():
                logger.debug(f"Column '{col}' contains {df[col].isna().sum()} NaT values pre-filter (footer rows expected).")

    # Type conversion helpers
    def clean_percent(col: str) -> pd.Series:
        """Convert percentage strings (e.g. '12.3%') to normalized decimals (0.123)."""
        return pd.to_numeric(
            df[col].astype(str).str.replace('%', '', regex=False).str.replace(',', ''),
            errors='coerce'
        ) / 100

    def clean_money(col: str) -> pd.Series:
        """Convert currency-like strings to numeric floats. Handles parenthesis negatives."""
        series = df[col].astype(str).str.strip()
        # Detect parenthesis negatives: ($1,234.56) -> -1234.56
        is_negative = series.str.startswith('(') & series.str.endswith(')')
        # Remove parenthesis, $, commas, and --
        cleaned = series.str.replace('[()$,]', '', regex=True).str.replace('--', '', regex=False)
        result = pd.to_numeric(cleaned, errors='coerce')
        # Apply negative sign where parenthesis were detected
        result = result.where(~is_negative, -result)
        return result

    def clean_integer(col: str) -> pd.Series:
        """Convert integer-like strings to nullable integers."""
        return pd.to_numeric(
            df[col].astype(str).str.replace(',', ''),
            errors='coerce'
        ).astype('Int64')

    # Apply type conversions to management-critical columns
    if 'Quantity' in df.columns:
        df['Quantity'] = pd.to_numeric(
            df['Quantity'].astype(str).str.replace(',', ''),
            errors='coerce'
        )
    
    # Handle '% Total G/L' to 'Total_GL_Decimal' conversion and drop original
    if '% Total G/L' in df.columns:
        df['Total_GL_Decimal'] = clean_percent('% Total G/L')
        df = df.drop(columns=['% Total G/L'])
    
    if '% of Acct' in df.columns:
        df['% of Acct'] = clean_percent('% of Acct')
    
    # Clean all money and sensitivity columns
    # RAG: Greeks must be converted to numeric to survive Cycle 1 hardening.
    numeric_cols = [
        '$ Total G/L', 'Basis', 'Last', 'Bid', 'Ask', 'UL Last', 'Time Val', 'Intrinsic Val',
        'Delta', 'Gamma', 'Vega', 'Theta', 'Rho', 'Strike', 'IV'
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = clean_money(col)

    for col in ['Volume', 'Open Int']:
        if col in df.columns:
            df[col] = clean_integer(col)
            # FAIL LOUD: Negative values are data integrity violations
            if (df[col] < 0).any():
                invalid_count = (df[col] < 0).sum()
                invalid_symbols = df.loc[df[col] < 0, 'Symbol'].tolist()[:5]
                raise ValueError(
                    f"❌ FATAL: {invalid_count} positions have negative {col}.\n"
                    f"   Sample symbols: {invalid_symbols}\n"
                    f"   {col} cannot be negative. This is a data quality violation.\n"
                    f"   Action: Fix broker export or exclude invalid positions."
                )

    # Validate Account column presence (CRITICAL GUARDRAIL)
    if 'Account' not in df.columns:
        raise ValueError(
            "❌ FATAL: 'Account' column missing from input CSV.\n"
            "   Trades cannot cross accounts. This is a data integrity requirement."
        )
    
    # === ENFORCE CYCLE 1 WHITELIST (HARD FAIL) ===
    # RAG: IV and Intrinsic Val are optional — Fidelity only exports them on expanded CSV views.
    _OPTIONAL_COLS = {'IV', 'Intrinsic Val'}
    effective_whitelist = [c for c in CYCLE1_WHITELIST if c not in _OPTIONAL_COLS or c in df.columns]
    # RAG: Ensure unique columns to avoid duplicate selection errors
    all_required = list(dict.fromkeys(effective_whitelist + DERIVATION_REQUIRED_COLS))
    missing_required = [col for col in all_required if col not in df.columns]
    if missing_required:
        raise ValueError(
            f"❌ DATA CONTRACT VIOLATION: Missing required Cycle 1 columns: {missing_required}\n"
            "   Cycle 1 requires these anchors for auditability and drift attribution."
        )
    
    # Keep only whitelisted + derivation columns for now
    df = df[all_required].copy()

    # Remove footer rows (disclosure text, NaN symbols, invalid tickers)
    footer_patterns = r'(?i)disclosure|data and information|^total$|^account|^symbol'
    df = df[
        df['Symbol'].notna() & 
        (df['Symbol'].str.len() < MAX_SYMBOL_LENGTH) &
        (df['Symbol'].str.len() > 0) &
        ~df['Symbol'].str.contains(footer_patterns, case=False, na=False, regex=True)
    ].copy()

    # RAG: Noise Reduction. Filter out ignored symbols (cash equivalents, etc.)
    df = df[~df['Symbol'].isin(IGNORED_SYMBOLS)].copy()
    
    # Remove rows where Quantity is NaN (invalid positions)
    df = df[df['Quantity'].notna()].copy()
    
    # Early exit if no positions remain after filtering
    if df.empty:
        print("⚠️  No positions found after filtering")
        return pd.DataFrame(), ""
    
    # Validate Account is present for all positions (CRITICAL GUARDRAIL)
    if df['Account'].isna().any():
        raise ValueError(
            f"❌ FATAL: {df['Account'].isna().sum()} positions missing Account identifier.\n"
            "   All positions must have an account. Data integrity violation."
        )
    
    # Classify asset type (OPTION vs STOCK vs UNKNOWN)
    def classify_asset_type(symbol: str) -> str:
        """Determine if symbol is an OPTION or STOCK based on OCC pattern."""
        if pd.isnull(symbol):
            return 'UNKNOWN'
        
        symbol_str = str(symbol).strip()
        
        # Check if it's an option (Regex handles internal spaces natively)
        if OCC_OPTION_PATTERN.match(symbol_str):
            return 'OPTION'
        
        # Check if it's a valid stock ticker (1-5 uppercase letters, no numbers)
        if re.match(r'^[A-Z]{1,5}$', symbol_str):
            return 'STOCK'
        
        return 'UNKNOWN'
    
    df['AssetType'] = df['Symbol'].apply(classify_asset_type)
    
    # === Canonical Underlying Ticker (Identity Law) ===
    def extract_underlying_ticker(row: pd.Series) -> str:
        symbol = str(row['Symbol']).strip()
        if row['AssetType'] == 'STOCK':
            return symbol
        
        # RAG: Handle internal spaces in Fidelity symbols (e.g. "AAPL  260116C240")
        # Use authoritative regex to extract ticker without mutation
        match = OCC_OPTION_PATTERN.match(symbol)
        if match:
            return match.group(1)
        return symbol # Fallback

    df['Underlying_Ticker'] = df.apply(extract_underlying_ticker, axis=1)

    # === Orphan Row Filter ===
    # UNKNOWN rows (Cash (SPAXX), etc.) are dropped entirely — no tradeable content.
    # STOCK rows with NO associated option legs are tagged STOCK_ONLY_IDLE and kept:
    #   - They represent idle capital (e.g. 100 shares with no CC sold against them)
    #   - The CC opportunity engine evaluates whether selling calls is favorable
    #   - Previously these were silently dropped; now they surface in the management UI
    # STOCK rows WITH an option counterpart pass through unchanged (BUY_WRITE paired stock).
    option_tickers = set(
        df.loc[df['AssetType'] == 'OPTION', 'Underlying_Ticker'].dropna().unique()
    )
    orphan_stock_mask = (df['AssetType'] == 'STOCK') & ~df['Underlying_Ticker'].isin(option_tickers)
    unknown_mask = df['AssetType'] == 'UNKNOWN'

    # Tag idle stocks before filtering
    if orphan_stock_mask.any():
        idle_tickers = df.loc[orphan_stock_mask, 'Symbol'].tolist()
        logger.info(
            f"📦 Idle stock detected: {len(idle_tickers)} position(s) with no option coverage: "
            f"{idle_tickers} — tagged STOCK_ONLY_IDLE for CC opportunity evaluation"
        )
        df.loc[orphan_stock_mask, 'Strategy'] = 'STOCK_ONLY_IDLE'

    # Drop only UNKNOWN rows (cash, etc.) — never drop idle stocks
    dropped_unknown = df[unknown_mask]['Symbol'].tolist()
    if dropped_unknown:
        logger.info(f"🧹 Dropped {len(dropped_unknown)} UNKNOWN rows (cash/sweep): {dropped_unknown}")
    df = df[~unknown_mask].copy()

    # === Premium derivation (BROKER TRUTH) ===
    if 'Time Val' in df.columns:
        # Apply sign: Long (+) / Short (-)
        # This matches the convention expected by compute_breakeven.py
        df['Premium'] = df['Time Val'] * np.sign(df['Quantity'])
        stock_mask = df['AssetType'] == 'STOCK'
        df.loc[stock_mask, 'Premium'] = pd.NA
        print(f"✅ Premium derived from Time Val (broker truth) for {(~stock_mask).sum()} option positions")
    else:
        if 'Premium' not in df.columns:
            print("⚠️  Warning: 'Time Val' column not found and no 'Premium' column exists.")
    
    # === Authoritative Temporal Anchor (Broker Truth) ===
    # RAG: McMillan (Temporal Integrity). Snapshot_TS MUST be broker-derived.
    if allow_system_time:
        # Override: If system time is allowed, use current time directly.
        current_system_time = datetime.now()
        df['Snapshot_TS'] = current_system_time
        print(f"✅ Using system time for Snapshot_TS: {current_system_time} (--allow-system-time).")
    elif 'As of Date/Time' in df.columns:
        # If the early timestamp conversion (line 152) already parsed this column,
        # use the datetime values directly. Otherwise, parse from raw string.
        if pd.api.types.is_datetime64_any_dtype(df['As of Date/Time']):
            df['Snapshot_TS'] = df['As of Date/Time']
        else:
            ts_series = (df['As of Date/Time'].astype(str)
                         .str.replace(' ET', '', regex=False)
                         .str.replace(' at ', ' ', regex=False)
                         .str.replace('/', '-', regex=False))
            df['Snapshot_TS'] = pd.to_datetime(ts_series, format='%m-%d-%Y %I:%M:%S %p', errors='coerce')

        # Fallback to header_ts for missing values before failing
        if header_ts is not None:
            missing_mask = df['Snapshot_TS'].isna()
            if missing_mask.any():
                df.loc[missing_mask, 'Snapshot_TS'] = header_ts
                print(f"✅ Filled {missing_mask.sum()} missing timestamps from header anchor.")
        
        # FAIL LOUD: Determinism > Convenience
        if df['Snapshot_TS'].isna().any():
            raise ValueError(
                "❌ FATAL: 'As of Date/Time' failed to parse for some positions. "
                "Cycle 1 requires deterministic broker timestamps."
            )
    else:
        raise ValueError(
            "❌ FATAL: 'As of Date/Time' column missing. "
            "Cycle 1 requires broker-derived temporal anchors."
        )

    # === Final Schema Hardening ===
    # Drop derivation-only columns before persistence
    # RAG: Ensure no duplicate columns before selection
    df = df.loc[:, ~df.columns.duplicated()].copy()
    
    # RAG: Ensure all whitelisted columns exist (fill with NULL if missing from broker)
    for col in CYCLE1_WHITELIST:
        if col not in df.columns:
            df[col] = pd.NA

    # === Price Coalesce (Dashboard Alignment) ===
    # RAG: For Stocks, 'Last' is the underlying price. 
    # We mirror it to 'UL Last' to ensure drift/drawdown math works in Cycle 2.
    stock_mask = df['AssetType'] == 'STOCK'
    df.loc[stock_mask, 'UL Last'] = df.loc[stock_mask, 'UL Last'].fillna(df.loc[stock_mask, 'Last'])
            
    df = df[CYCLE1_WHITELIST + ['Snapshot_TS', 'AssetType', 'Underlying_Ticker', 'Premium']].copy()

    # Canonicalize column names: space → underscore for Fidelity columns that pipeline expects
    _col_renames = {c: c.replace(' ', '_') for c in ['Open Int', 'Intrinsic Val', 'Time Val', 'Earnings Date'] if c in df.columns}
    if _col_renames:
        df = df.rename(columns=_col_renames)

    # Optional: Save snapshot
    snapshot_path = ""
    if save_snapshot:
        snapshot_dir = Path(CANONICAL_SNAPSHOT_DIR)
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp_str = df['Snapshot_TS'].iloc[0].strftime('%Y-%m-%d_%H-%M-%S')
        filename = f"phase1_positions_{timestamp_str}.csv"
        snapshot_path = str(snapshot_dir / filename)
        
        df.to_csv(snapshot_path, index=False)
        print(f"💾 Snapshot: {snapshot_path}")

    # Data freshness + provenance stamps (flow into management_recommendations time-series)
    df['Data_Freshness'] = _data_freshness
    df['Data_Age_Trading_Days'] = _data_age_days
    df['Greeks_Source'] = 'broker_csv'  # default — overwritten by LiveGreeksProvider later
    if header_ts is not None:
        df['Greeks_TS'] = header_ts.isoformat()
    else:
        df['Greeks_TS'] = None

    # Summary
    print(f"✅ Phase 1 complete: {len(df)} positions, {len(df.columns)} columns")

    return df, snapshot_path

# Parity Assertion: Enforce keyword-only configuration for Phase 1
EXPECTED_SIGNATURE = inspect.Signature([
    inspect.Parameter('input_path', inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=Path),
    inspect.Parameter('save_snapshot', inspect.Parameter.KEYWORD_ONLY, annotation=bool, default=True),
    inspect.Parameter('allow_system_time', inspect.Parameter.KEYWORD_ONLY, annotation=bool, default=False)
], return_annotation=Tuple[pd.DataFrame, str])

assert inspect.signature(phase1_load_and_clean_positions) == EXPECTED_SIGNATURE


# Backward compatibility alias (deprecated)
def phase1_load_and_clean_raw_v2(
    input_path: str = None,
    snapshot_dir: str = None,
    save_snapshot: bool = False
) -> Tuple[pd.DataFrame, str]:
    """
    DEPRECATED: Use phase1_load_and_clean_positions() instead.
    """
    import warnings
    warnings.warn(
        "phase1_load_and_clean_raw_v2() is deprecated. "
        "Use phase1_load_and_clean_positions() instead.",
        DeprecationWarning,
        stacklevel=2
    )
    return phase1_load_and_clean_positions(
        input_path=Path(CANONICAL_INPUT_PATH),
        save_snapshot=save_snapshot
    )
