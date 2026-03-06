"""
Step 2: Load & Enrich IV/HV Snapshot

AUTHORITATIVE SOURCES (owned by this step):
- Signal_Type: Bullish/Bearish/Bidirectional (from Trend_State via Murphy indicators)
- Regime: High Vol/Low Vol/Compression/Expansion (from IV_Rank + IV_Trend + VVIX)
- IV_Maturity_State: MATURE/DEVELOPING/IMMATURE/MISSING (from iv_term_history count)
- IV_Rank_30D: Percentile ranking from Schwab IV history (iv_history.duckdb)

CRITICAL: Downstream steps (3-13) MUST preserve Signal_Type and Regime unchanged.
Any step that overwrites these fields breaks the execution gate logic in Step 12.

BEHAVIOR: This step does THREE things:
1. Load CSV snapshot (raw IV/HV data)
2. Enrich volatility metrics (IV Rank, IV Trend, HV Trend, Term Structure, IV Maturity)
3. Enrich technical indicators (RSI, ADX, SMA, ATR, patterns)

OUTPUT: Enriched DataFrame with 80+ columns ready for gap analysis (Step 3).
"""

import pandas as pd
import numpy as np
import logging
import os
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Tuple, List, Dict, Optional
from core.shared.data_contracts.config import SCAN_SNAPSHOT_DIR
from core.shared.data_layer.price_history_loader import load_price_history
from utils.ta_lib_utils import calculate_rsi, calculate_adx
from core.shared.data_layer.technical_data_repository import insert_technical_indicators
import duckdb # Import duckdb globally for type hinting
from datetime import datetime # Ensure datetime is imported for datetime.now()
from scipy.stats import percentileofscore # For IV Rank calculation
from scan_engine.enrichment.iv_engine import IVEngine # Import the new IVEngine

logger = logging.getLogger(__name__)

TIMESTAMP_FIELDS = [
    'snapshot_ts', 'timestamp', 'Snapshot_TS', 'AsOf',
    'quote_time', 'trade_time', 'Quote_Time', 'Last_Price_Time',
    'Trade_Date', 'Expiration_Date', 'Entry_Time', 'Exit_Time',
    'Created_At', 'Updated_At', 'Last_Fetch_TS', 'Backoff_Until', 'Anchor_TS'
]

_EPOCH_FIELDS = {'quote_time', 'trade_time', 'Quote_Time', 'Last_Price_Time'}


def _normalize_timestamp_series(series: pd.Series, field_name: str) -> pd.Series:
    if series.dtype.kind in {'i', 'u', 'f'} or field_name in _EPOCH_FIELDS:
        # Treat numeric values as epoch (ms if >1e12, else seconds)
        numeric = pd.to_numeric(series, errors='coerce')
        unit = 'ms' if (numeric.dropna() > 1e12).any() else 's'
        ts = pd.to_datetime(numeric, errors='coerce', unit=unit)
    else:
        ts = pd.to_datetime(series, errors='coerce')

    if hasattr(ts.dt, "tz") and ts.dt.tz is not None:
        ts = ts.dt.tz_convert('UTC').dt.tz_localize(None)

    return ts


def _resolve_canonical_timestamp(df: pd.DataFrame) -> pd.Series:
    precedence = ['snapshot_ts', 'timestamp', 'Snapshot_TS', 'AsOf']
    for col in precedence:
        if col in df.columns:
            candidate = df[col]
            if pd.notna(candidate).any():
                return candidate
    return pd.Series([pd.NaT] * len(df))


def load_latest_live_snapshot(snapshot_dir: str = None) -> str:
    """
    Load the most recent ivhv_snapshot_live_*.csv file from Step 0.
    """
    snapshot_path = Path(snapshot_dir) if snapshot_dir else SCAN_SNAPSHOT_DIR
    
    if not snapshot_path.exists():
        raise FileNotFoundError(
            f"❌ Snapshot directory not found: {snapshot_path}\n"
            f"   Run Step 0 first: python -m scan_engine/step0_schwab_snapshot.py"
        )
    
    # Deterministic Selection: Sort by filename timestamp instead of filesystem mtime
    def extract_timestamp(f):
        try:
            ts_str = f.stem.replace("ivhv_snapshot_live_", "")
            return datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
        except ValueError:
            return datetime.min

    live_snapshots = sorted(
        snapshot_path.glob("ivhv_snapshot_live_*.csv"),
        key=extract_timestamp,
        reverse=True
    )
    
    if not live_snapshots:
        raise FileNotFoundError(
            f"❌ No live snapshots found in {snapshot_path}\n"
            f"   Expected pattern: ivhv_snapshot_live_YYYYMMDD_HHMMSS.csv\n"
            f"   Run Step 0 first: python -m scan_engine/step0_schwab_snapshot.py"
        )
    
    latest = live_snapshots[0]
    
    # Extract timestamp from filename for deterministic age calculation
    # Pattern: ivhv_snapshot_live_YYYYMMDD_HHMMSS.csv
    try:
        ts_str = latest.stem.replace("ivhv_snapshot_live_", "")
        file_dt = datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
        age_hours = (datetime.now() - file_dt).total_seconds() / 3600
    except Exception:
        # Fallback to mtime only for diagnostic age if filename parsing fails
        file_dt = datetime.fromtimestamp(latest.stat().st_mtime) # Ensure file_dt is defined
        age_hours = (datetime.now() - file_dt).total_seconds() / 3600
    
    logger.info(f"✅ Found latest live snapshot: {latest.name} (age: {age_hours:.1f}h)")
    return str(latest.resolve())


def _resolve_canonical_price(df: pd.DataFrame, id_col: str, con: Optional[duckdb.DuckDBPyConnection] = None) -> pd.DataFrame:
    """
    Resolve canonical Stock_Price from available sources WITHOUT auto-fetching.

    TRUST CONTRACT (Demand-Driven):
    - Does NOT auto-fetch from Yahoo Finance
    - Uses fallback chain: schwab_last_price → cached_ohlc_close → explicit tagging
    - Missing prices tagged with YF_Demand_Status=AWAITING_FETCH
    - Exports demand to output/yf_demand_tickers.csv

    Fallback Chain:
    1. Schwab last_price (primary, authoritative)
    2. Cached OHLC Close from DuckDB (if available)
    3. Explicit tagging if both unavailable

    Args:
        df: DataFrame with 'Ticker' and 'last_price' columns
        id_col: Name of ticker column
        con: Optional DuckDB connection for OHLC cache lookup

    Returns:
        DataFrame with resolved Stock_Price and demand status fields
    """
    from core.shared.data_layer.duckdb_utils import get_duckdb_connection

    # Initialize canonical price column and demand status
    df = df.copy()
    df['Stock_Price'] = np.nan
    df['Stock_Price_Source'] = 'UNKNOWN'
    df['YF_Demand_Status'] = 'NOT_NEEDED'
    df['Resolution_Path'] = ''

    # Use provided connection or open new one
    _con = con
    owns_connection = False
    if _con is None:
        try:
            _con = get_duckdb_connection()
            owns_connection = True
        except Exception as e:
            logger.warning(f"⚠️ Could not open DuckDB for OHLC cache lookup: {e}")
            _con = None

    demand_records = []

    try:
        for idx in df.index:
            ticker = df.loc[idx, id_col]
            schwab_price = df.loc[idx, 'last_price'] if 'last_price' in df.columns else None

            # Priority 1: Schwab last_price (authoritative)
            if pd.notna(schwab_price) and schwab_price > 0:
                df.loc[idx, 'Stock_Price'] = schwab_price
                df.loc[idx, 'Stock_Price_Source'] = 'SCHWAB_LAST_PRICE'
                df.loc[idx, 'YF_Demand_Status'] = 'NOT_NEEDED'
                continue

            # Priority 2: Cached OHLC Close from DuckDB (if available)
            cached_close = None
            if _con is not None:
                try:
                    # Query price_history table for most recent cached close
                    result = _con.execute("""
                        SELECT close_price
                        FROM price_history
                        WHERE UPPER(ticker) = UPPER(?)
                        ORDER BY date DESC
                        LIMIT 1
                    """, [ticker]).fetchone()

                    if result and result[0] is not None and result[0] > 0:
                        cached_close = result[0]
                except Exception as e:
                    logger.debug(f"{ticker}: Could not query OHLC cache: {e}")

            if cached_close is not None:
                df.loc[idx, 'Stock_Price'] = cached_close
                df.loc[idx, 'Stock_Price_Source'] = 'CACHED_OHLC_CLOSE'
                df.loc[idx, 'YF_Demand_Status'] = 'NOT_NEEDED'
                continue

            # Priority 3: Explicit tagging - price unavailable
            logger.warning(f"⚠️ {ticker}: No valid price from Schwab or cached OHLC - tagging for YF fetch")
            df.loc[idx, 'Stock_Price'] = np.nan
            df.loc[idx, 'Stock_Price_Source'] = 'MISSING'
            df.loc[idx, 'YF_Demand_Status'] = 'AWAITING_FETCH'
            df.loc[idx, 'Resolution_Path'] = 'Fetch Yahoo Finance OHLC'

            # Add to demand export
            demand_records.append({
                'ticker': ticker,
                'reason': 'Stock_Price missing (Schwab and cached OHLC unavailable)',
                'requested_at': datetime.now().isoformat()
            })

    finally:
        # Only close connection if we opened it ourselves
        if owns_connection and _con is not None:
            try:
                _con.close()
            except:
                pass

    # Export demand to CSV if any tickers need YF fetch
    if demand_records:
        output_dir = Path(__file__).resolve().parents[1] / "output"
        output_dir.mkdir(exist_ok=True)
        demand_path = output_dir / "yf_demand_tickers.csv"

        demand_df = pd.DataFrame(demand_records)
        demand_df.to_csv(demand_path, index=False)
        logger.info(f"📊 Exported {len(demand_records)} tickers to YF demand file: {demand_path}")
        logger.info(f"   Run: python yf_fetch.py --mode demand (to fetch missing prices)")
    else:
        logger.info("✅ All tickers have valid prices from Schwab or cached OHLC - no YF demand")

    # Summary statistics
    source_counts = df['Stock_Price_Source'].value_counts().to_dict()
    logger.info(f"📊 Price Resolution Summary:")
    for source, count in source_counts.items():
        logger.info(f"   {source}: {count} tickers")

    return df


def load_raw_snapshot(
    snapshot_path: str = None,
    max_age_hours: int = 48,
    use_live_snapshot: bool = False
) -> Tuple[pd.DataFrame, str]:
    """
    Step 2A: Load and validate raw snapshot CSV.
    """
    if use_live_snapshot:
        if snapshot_path is not None:
            logger.warning("⚠️ Both use_live_snapshot=True and snapshot_path provided. Ignoring snapshot_path.")
        snapshot_path = load_latest_live_snapshot()
    elif snapshot_path is None:
        snapshot_path = os.getenv('SNAPSHOT_PATH',
                                   '/Users/haniabadi/Documents/Github/options/data/OptionsSnapshots/ivhv_snapshot.csv')
    
    snapshot_path = Path(snapshot_path)
    df = pd.read_csv(snapshot_path)
    logger.info(f"✅ Loaded snapshot: {snapshot_path} ({df.shape[0]} rows)")

    # Identify potential timestamp columns and enforce datetime type
    for col in TIMESTAMP_FIELDS:
        if col in df.columns:
            df[col] = _normalize_timestamp_series(df[col], col)

    # Resolve canonical snapshot timestamp (authoritative)
    canonical_ts = _resolve_canonical_timestamp(df)
    if canonical_ts.isna().all():
        warning_msg = "Step 2 timestamp resolution failed: no valid canonical snapshot timestamp found."
        logger.warning(f"⚠️ {warning_msg}")
        df["Step2_Warning"] = df.apply(lambda row: f"{row['Step2_Warning']}; {warning_msg}" if "Step2_Warning" in row and pd.notna(row["Step2_Warning"]) else warning_msg, axis=1)
        # Do not raise, continue with enrichment
    df['timestamp'] = canonical_ts

    # ============================================================
    # SNAPSHOT SOURCE DIAGNOSTIC (Data Integrity Enforcement)
    # RAG Source: docs/EXECUTION_SEMANTICS.md:28-31
    # Requirement: Fresh Schwab Data (snapshot timestamp <48 hours old)
    # ============================================================
    snapshot_file_path_str = str(snapshot_path.resolve())
    snapshot_data_timestamp = df['timestamp'].iloc[0] if not df.empty else pd.NaT

    # Extract expected timestamp from filename
    def _extract_file_timestamp(p):
        try:
            ts_str = p.stem.replace("ivhv_snapshot_live_", "").replace("snapshot_", "")
            return datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
        except:
            return None

    file_timestamp = _extract_file_timestamp(snapshot_path)

    # Calculate age delta
    if pd.notna(snapshot_data_timestamp) and file_timestamp is not None:
        delta_hours = (file_timestamp - snapshot_data_timestamp).total_seconds() / 3600
        abs_delta_hours = abs(delta_hours)

        # Log diagnostic
        logger.info(f"[SNAPSHOT_SOURCE] csv_file={snapshot_file_path_str}")
        logger.info(f"[SNAPSHOT_SOURCE] file_timestamp={file_timestamp.strftime('%Y-%m-%d %H:%M:%S') if file_timestamp else 'UNKNOWN'}")
        logger.info(f"[SNAPSHOT_SOURCE] data_timestamp={snapshot_data_timestamp.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(snapshot_data_timestamp) else 'UNKNOWN'}")
        logger.info(f"[SNAPSHOT_SOURCE] Δ={abs_delta_hours:.1f}h (file-to-data discrepancy)")

        # Market-aware freshness (IV/HV): <= 1 trading day since last market close
        from core.shared.data_layer.market_time import ivhv_freshness
        is_fresh, age_td = ivhv_freshness(snapshot_data_timestamp, max_trading_days=1)
        logger.info(f"[SNAPSHOT_SOURCE] data_age_trading_days={age_td} (relative to last market close)")

        if not is_fresh:
            warning_msg = (
                f"STALE DATA VIOLATION: Data timestamp: {snapshot_data_timestamp}, Trading-day age: {age_td}. "
                f"Requirement: IV/HV fresh <= 1 trading day since last market close. Marking iv_data_stale."
            )
            logger.warning(f"⚠️ {warning_msg}")
            df['iv_data_stale'] = True
            df["Step2_Warning"] = df.apply(lambda row: f"{row['Step2_Warning']}; {warning_msg}" if "Step2_Warning" in row and pd.notna(row["Step2_Warning"]) else warning_msg, axis=1)
            try:
                from scan_engine.debug.debug_mode import get_debug_manager
                get_debug_manager().log_event(
                    "step2_snapshot",
                    "WARN",
                    "STALE_IVHV",
                    "Snapshot IV/HV exceeds freshness threshold",
                    {"age_trading_days": age_td}
                )
            except Exception:
                pass

        # Hard assertion: File timestamp and data timestamp must be same-day or within 24 hours
        # This catches multi-day stale data while allowing intraday snapshots after market close
        # Example: File created at 22:00, data from 16:00 same day = OK (6h delta)
        # Example: File dated Feb 2, data from Jan 30 = FAIL (3-day delta)
        if abs_delta_hours > 24.0:
            warning_msg = (
                f"SNAPSHOT INTEGRITY VIOLATION: File: {snapshot_file_path_str}, File timestamp: {file_timestamp}, "
                f"Data timestamp: {snapshot_data_timestamp}. Discrepancy: {abs_delta_hours:.1f} hours. "
                f"This indicates the CSV file contains stale data that doesn't match its filename. Continuing safely."
            )
            logger.warning(f"⚠️ {warning_msg}")
            df['snapshot_integrity_violation'] = True
            df["Step2_Warning"] = df.apply(lambda row: f"{row['Step2_Warning']}; {warning_msg}" if "Step2_Warning" in row and pd.notna(row["Step2_Warning"]) else warning_msg, axis=1)
            try:
                from scan_engine.debug.debug_mode import get_debug_manager
                get_debug_manager().log_event(
                    "step2_snapshot",
                    "WARN",
                    "SNAPSHOT_INTEGRITY_VIOLATION",
                    "Snapshot file/data timestamp mismatch",
                    {"delta_hours": abs_delta_hours}
                )
            except Exception:
                pass
    elif pd.notna(snapshot_data_timestamp):
        # File timestamp not available from filename, log data timestamp only
        logger.info(f"[SNAPSHOT_SOURCE] csv_file={snapshot_file_path_str}")
        logger.info(f"[SNAPSHOT_SOURCE] data_timestamp={snapshot_data_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.warning(f"[SNAPSHOT_SOURCE] Could not extract file timestamp from filename for validation")
    else:
        warning_msg = f"Snapshot data contains no valid timestamp for {snapshot_file_path_str}"
        logger.warning(f"⚠️ {warning_msg}")
        df["Step2_Warning"] = df.apply(lambda row: f"{row['Step2_Warning']}; {warning_msg}" if "Step2_Warning" in row and pd.notna(row["Step2_Warning"]) else warning_msg, axis=1)
        # Do not raise, continue with enrichment
    # ============================================================

    # 1. Freshness check
    if 'timestamp' in df.columns:
        snapshot_time = df['timestamp'].iloc[0]
        
        # Deterministic Age: Use filename timestamp as reference to avoid runtime variance in ledger
        def _extract_ts(p):
            try:
                ts_str = p.stem.replace("ivhv_snapshot_live_", "").replace("snapshot_", "")
                return datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
            except:
                return None
        
        ref_now = _extract_ts(snapshot_path) or datetime.fromtimestamp(snapshot_path.stat().st_mtime)
        
        # Ensure file_dt is defined before use
        file_dt_for_age = snapshot_time # Use snapshot_time as the primary reference for age
        if pd.isna(file_dt_for_age):
            file_dt_for_age = ref_now # Fallback if snapshot_time is NaN

        # Defensive guard for datetime arithmetic
        if pd.isna(file_dt_for_age):
            age_hours = np.nan # Or a very large number to indicate an old snapshot
            logger.warning(f"⚠️ Could not determine snapshot age due to invalid timestamp.")
        else:
            age_hours = (datetime.now() - file_dt_for_age).total_seconds() / 3600
        
        df['Snapshot_Age_Hours'] = age_hours
        # Market-aware freshness warning
        from core.shared.data_layer.market_time import ivhv_freshness
        if pd.notna(file_dt_for_age):
            is_fresh, age_td = ivhv_freshness(file_dt_for_age, max_trading_days=1)
            df['Snapshot_Age_Trading_Days'] = age_td
            if not is_fresh:
                warning_msg = f"Snapshot is {age_td} trading days old (IV/HV freshness requires <=1 trading day)."
                logger.warning(f"⚠️ {warning_msg}")
                df["Step2_Warning"] = df.apply(lambda row: f"{row['Step2_Warning']}; {warning_msg}" if "Step2_Warning" in row and pd.notna(row["Step2_Warning"]) else warning_msg, axis=1)
                try:
                    from scan_engine.debug.debug_mode import get_debug_manager
                    get_debug_manager().log_event(
                        "step2_snapshot",
                        "WARN",
                        "STALE_IVHV",
                        "Snapshot IV/HV exceeds freshness threshold",
                        {"age_trading_days": age_td}
                    )
                except Exception:
                    pass
    
    # 2. Data type enforcement
    hv_cols = [col for col in df.columns if col.startswith('HV_') and '_Cur' in col]
    for col in hv_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 3. Duplicate handling
    id_col = 'Symbol' if 'Symbol' in df.columns else 'Ticker'
    if id_col in df.columns and df[id_col].duplicated().any():
        df = df.drop_duplicates(subset=id_col, keep='first')
    
    if id_col not in df.columns:
        # Fallback to first column if neither Symbol nor Ticker found
        id_col = df.columns[0]
        logger.warning(f"⚠️ Neither 'Symbol' nor 'Ticker' found. Using '{id_col}' as identifier.")
        
    return df, id_col








def enrich_volatility_metrics(df: pd.DataFrame, id_col: str) -> pd.DataFrame:
    """
    Step 2B: IV Surface rehydration and Sinclair regime classification.

    Args:
        df: DataFrame with ticker data
        id_col: Name of ticker column
    """
    from core.shared.data_layer.iv_term_history import get_iv_history_db_path

    # Initialize IVEngine
    debug_mode = os.getenv("DEBUG_IV_MODE", "False").lower() == "true"
    debug_tickers_str = os.getenv("DEBUG_TICKERS")
    debug_tickers = [t.strip().upper() for t in debug_tickers_str.split(',') if t.strip()] if debug_tickers_str else []
    iv_engine = IVEngine(debug_mode=debug_mode, debug_tickers=debug_tickers)

    # Load historical IV data for all tickers in the current snapshot
    tickers_in_snapshot = df[id_col].dropna().astype(str).str.upper().unique().tolist()
    
    db_path = get_iv_history_db_path()
    if not db_path.exists():
        logger.warning(f"⚠️ iv_history.duckdb not found at {db_path}. Cannot compute derived IV metrics.")
        df["Step2_Warning"] = df.apply(lambda row: f"{row['Step2_Warning']}; iv_history.duckdb not found, derived IV metrics not computed" if "Step2_Warning" in row and pd.notna(row["Step2_Warning"]) else "iv_history.duckdb not found, derived IV metrics not computed", axis=1)
        return df

    # Always open iv_history.duckdb directly — iv_term_history lives there, not in pipeline.duckdb
    owns_connection = True
    try:
        import duckdb as _duckdb
        _con = _duckdb.connect(str(db_path), read_only=True)
    except Exception as e:
        logger.warning(f"⚠️ Could not open iv_history.duckdb for derived IV metrics computation: {e}")
        df["Step2_Warning"] = df.apply(lambda row: f"{row['Step2_Warning']}; DuckDB connection failed for derived IV metrics: {e}" if "Step2_Warning" in row and pd.notna(row["Step2_Warning"]) else f"DuckDB connection failed for derived IV metrics: {e}", axis=1)
        return df

    historical_iv_df = pd.DataFrame()
    try:
        if tickers_in_snapshot:
            placeholders = ", ".join([f"'{t}'" for t in tickers_in_snapshot])
            query = f"""
                SELECT ticker, date as trade_date, iv_7d, iv_14d, iv_21d, iv_30d, iv_60d, iv_90d, iv_120d, iv_180d, iv_360d
                FROM iv_term_history
                WHERE UPPER(ticker) IN ({placeholders})
                  AND iv_30d IS NOT NULL
                ORDER BY ticker, date
            """
            historical_iv_df = _con.execute(query).fetchdf()
            logger.info(f"✅ Loaded {len(historical_iv_df)} historical IV records for {len(tickers_in_snapshot)} tickers.")
        else:
            logger.info("No tickers in snapshot to load historical IV data for.")
    except Exception as e:
        logger.error(f"❌ Failed to load historical IV data from DuckDB: {e}", exc_info=True)
        df["Step2_Warning"] = df.apply(lambda row: f"{row['Step2_Warning']}; Failed to load historical IV data: {e}" if "Step2_Warning" in row and pd.notna(row["Step2_Warning"]) else f"Failed to load historical IV data: {e}", axis=1)
        return df
    finally:
        if owns_connection and _con is not None:
            try:
                _con.close()
            except:
                pass

    # Calculate derived metrics using the IVEngine
    if not historical_iv_df.empty:
        logger.info("📊 Calculating derived IV metrics using IVEngine...")
        t_iv_engine = time.time()
        derived_metrics_df = iv_engine.calculate_derived_metrics(historical_iv_df)
        logger.info(f"⏱️ Step 2B (IV Engine): {time.time() - t_iv_engine:.1f}s for {len(tickers_in_snapshot)} tickers")
        
        # Merge the derived metrics back into the main DataFrame
        # We need to merge the LATEST derived metrics for each ticker
        if not derived_metrics_df.empty:
            latest_derived_metrics = derived_metrics_df.sort_values('trade_date').groupby('ticker').tail(1).drop(columns=['trade_date'])
            
            # Ensure ticker column is consistent for merge
            latest_derived_metrics['ticker_upper'] = latest_derived_metrics['ticker'].str.upper()
            df['ticker_upper'] = df[id_col].str.upper()

            # Drop existing columns that will be replaced by derived_metrics_df to avoid suffixes
            cols_to_drop = [col for col in latest_derived_metrics.columns if col in df.columns and col not in ['ticker', 'ticker_upper']]
            df = df.drop(columns=cols_to_drop, errors='ignore')

            df = df.merge(latest_derived_metrics, left_on='ticker_upper', right_on='ticker_upper', how='left', suffixes=('', '_derived'))
            df = df.drop(columns=['ticker_upper', 'ticker_derived'], errors='ignore') # Clean up merge columns
            
            # Rename 'ticker' from derived_metrics_df to id_col if it was merged
            if 'ticker' in df.columns and 'ticker' != id_col:
                df = df.drop(columns=['ticker'])

            logger.info("✅ Derived IV metrics merged into snapshot.")

            # REGIME_DAYS_IN — Sinclair 2020 Ch.3: regime clustering means long runs are more
            # predictable than short ones. A 15-day HIGH_VOL streak is more likely to continue
            # than a 2-day streak. Computed from the IV_Regime time series in derived_metrics_df.
            # Logic: for each ticker, count consecutive trailing days with the same IV_Regime
            # as today (the most recent day in the series). No new DB connection required.
            try:
                if 'IV_Regime' in derived_metrics_df.columns:
                    _regime_days: dict = {}
                    for _tkr, _grp in derived_metrics_df.sort_values('trade_date').groupby('ticker'):
                        _grp = _grp.reset_index(drop=True)
                        _latest_regime = _grp['IV_Regime'].iloc[-1] if len(_grp) else None
                        if _latest_regime is None or (isinstance(_latest_regime, float) and np.isnan(_latest_regime)):
                            _regime_days[_tkr.upper()] = 0
                            continue
                        # Walk backward from most recent row counting streak
                        _count = 0
                        for _i in range(len(_grp) - 1, -1, -1):
                            if _grp['IV_Regime'].iloc[_i] == _latest_regime:
                                _count += 1
                            else:
                                break
                        _regime_days[_tkr.upper()] = _count
                    df['Regime_Days_In'] = df[id_col].str.upper().map(_regime_days).fillna(0).astype(int)
                    _median_streak = int(df['Regime_Days_In'].median()) if len(df) else 0
                    logger.info(f"[Regime_Days_In] computed: median streak={_median_streak}d "
                                f"(Sinclair 2020 Ch.3: clustering signal for streaks ≥ 10d)")
                else:
                    df['Regime_Days_In'] = 0
                    logger.debug("[Regime_Days_In] IV_Regime not in derived_metrics_df — streak set to 0")
            except Exception as _rdi_err:
                df['Regime_Days_In'] = 0
                logger.warning(f"[Regime_Days_In] computation failed (non-fatal): {_rdi_err}")

            # ── IV_Trend_7D: slope of iv_30d over last 7 trading days ───────
            # Sinclair Ch.2 + Augen Ch.3: IV trend direction matters for income sellers.
            # Rising IV below rank 40 = trend just starting, wait for crest.
            try:
                def _compute_iv_trend_7d(ticker_grp):
                    """7-day slope of iv_30d → Rising/Falling/Stable."""
                    recent = ticker_grp.tail(7)['iv_30d'].dropna()
                    if len(recent) < 5:
                        return 'Unknown'
                    x = np.arange(len(recent))
                    try:
                        slope = np.polyfit(x, recent.values, 1)[0]  # pts per day
                    except (np.linalg.LinAlgError, ValueError):
                        # Fallback: median daily change
                        diffs = recent.diff().dropna()
                        slope = float(diffs.median()) if len(diffs) > 0 else 0.0
                    weekly = slope * 5  # pts per trading week
                    if weekly > 0.5:
                        return 'Rising'
                    elif weekly < -0.5:
                        return 'Falling'
                    return 'Stable'

                _iv_trends: dict = {}
                for _tkr, _grp in historical_iv_df.sort_values('trade_date').groupby('ticker'):
                    _iv_trends[_tkr.upper()] = _compute_iv_trend_7d(_grp)
                df['IV_Trend_7D'] = df[id_col].str.upper().map(_iv_trends).fillna('Unknown')
                _trend_dist = df['IV_Trend_7D'].value_counts().to_dict()
                logger.info(f"[IV_Trend_7D] computed: {_trend_dist}")
            except Exception as _ivt_err:
                df['IV_Trend_7D'] = 'Unknown'
                logger.warning(f"[IV_Trend_7D] computation failed (non-fatal): {_ivt_err}")

            # ── HV_Accel_Proxy: HV30 vs HV10 acceleration proxy ──────────
            # Not a true 30D trend (would require historical HV series).
            # Proxy: HV30 > HV10 by >2pts → realized vol accelerating.
            # Used in Step 3 MeanReversion/Expansion pattern detection.
            if 'HV30' in df.columns and 'HV10' in df.columns:
                try:
                    _hv30 = pd.to_numeric(df['HV30'], errors='coerce')
                    _hv10 = pd.to_numeric(df['HV10'], errors='coerce')
                    _hv_diff = _hv30 - _hv10
                    df['HV_Accel_Proxy'] = np.where(
                        _hv_diff > 2.0, 'Rising',
                        np.where(_hv_diff < -2.0, 'Falling', 'Stable')
                    )
                    df['HV_Accel_Proxy'] = df['HV_Accel_Proxy'].where(_hv_diff.notna(), 'Unknown')
                    _hv_dist = df['HV_Accel_Proxy'].value_counts().to_dict()
                    logger.info(f"[HV_Accel_Proxy] computed (HV30-HV10): {_hv_dist}")
                except Exception as _hv_err:
                    df['HV_Accel_Proxy'] = 'Unknown'
                    logger.warning(f"[HV_Accel_Proxy] computation failed (non-fatal): {_hv_err}")
            else:
                df['HV_Accel_Proxy'] = 'Unknown'

            # Debug print if enabled
            iv_engine._debug_print(df)
        else:
            logger.warning("⚠️ IVEngine returned an empty DataFrame for derived metrics.")
            df["Step2_Warning"] = df.apply(lambda row: f"{row['Step2_Warning']}; IVEngine returned empty derived metrics" if "Step2_Warning" in row and pd.notna(row["Step2_Warning"]) else "IVEngine returned empty derived metrics", axis=1)
            df['Regime_Days_In'] = 0
    else:
        logger.warning("⚠️ No historical IV data available for derived metrics calculation.")
        df["Step2_Warning"] = df.apply(lambda row: f"{row['Step2_Warning']}; No historical IV data for derived metrics" if "Step2_Warning" in row and pd.notna(row["Step2_Warning"]) else "No historical IV data for derived metrics", axis=1)
        df['Regime_Days_In'] = 0

    # The following sections are now handled by IVEngine
    # Remove old IV Maturity State and IV Rank computation
    # Remove Term Structure & Trends, Sinclair Regimes, Volatility Regime Classification, IV Timing State
    # as these are now handled by the IVEngine or are being replaced.

    # Ensure required columns are present, even if NaN
    required_new_columns = [
        'IV_History_Count', 'IV_Maturity_Level', 'IV_Rank_20D', 'IV_Rank_30D',
        'IV_Rank_60D', 'IV_Rank_252D', 'IV_7D_5D_ROC', 'IV_30D_5D_ROC',
        'IV_30D_10D_ROC', 'IV_90D_10D_ROC', 'IV_30D_Mean_30', 'IV_30D_Std_30',
        'IV_30D_ZScore_30', 'Slope_30_7', 'Slope_90_30', 'Slope_180_90',
        'Surface_Steepness', 'Surface_Shape', 'IV_Regime',
        'Structural_IV_Cycle', 'LongTerm_ZScore'
    ]
    for col in required_new_columns:
        if col not in df.columns:
            df[col] = np.nan
    # Regime_Days_In: integer, default 0 (not NaN) — always present
    if 'Regime_Days_In' not in df.columns:
        df['Regime_Days_In'] = 0

    # Map IV_History_Count → IV_Maturity_State using the shared classifier
    # (single source of truth: core/shared/volatility/maturity_classifier.py)
    # Thresholds: MATURE ≥120d, PARTIAL_MATURE 30-119d, IMMATURE 1-29d, MISSING 0d
    from core.shared.volatility.maturity_classifier import classify_iv_maturity
    df['IV_Maturity_State'] = df['IV_History_Count'].fillna(0).astype(int).apply(classify_iv_maturity)

    # Map IV_Regime to Regime for downstream compatibility
    # Contract expects: "High Vol", "Low Vol", "Compression", "Expansion", "Unknown"
    _REGIME_MAP = {
        'HIGH_VOL': 'High Vol',
        'LOW_VOL':  'Low Vol',
        'NORMAL':   'Compression',  # Normal/stable → Compression
    }
    if 'IV_Regime' in df.columns:
        df['Regime'] = df['IV_Regime'].map(_REGIME_MAP).fillna('Unknown')
    else:
        df['Regime'] = 'Unknown'

    # Regime fallback: IVEngine only computes IV_Regime at ≥60d history (Phase 3).
    # For PARTIAL_MATURE tickers (30-59d), derive regime from IV_Rank_30D instead.
    # Sinclair Ch.2: rank < 20 = suppressed vol (Low Vol), rank > 80 = elevated (High Vol).
    if 'IV_Rank_30D' in df.columns:
        _unknown_mask = df['Regime'] == 'Unknown'
        _rank = df.loc[_unknown_mask, 'IV_Rank_30D']
        df.loc[_unknown_mask & (_rank < 20), 'Regime'] = 'Low Vol'
        df.loc[_unknown_mask & (_rank > 80), 'Regime'] = 'High Vol'
        df.loc[_unknown_mask & (_rank >= 20) & (_rank <= 80), 'Regime'] = 'Compression'

    # Ensure IV_Trend_7D and HV_Accel_Proxy are present (may not be set if no historical data)
    if 'IV_Trend_7D' not in df.columns:
        df['IV_Trend_7D'] = 'Unknown'
    if 'HV_Accel_Proxy' not in df.columns:
        df['HV_Accel_Proxy'] = 'Unknown'
    # Legacy compat: downstream (step3) may reference HV_Trend_30D
    if 'HV_Trend_30D' not in df.columns:
        df['HV_Trend_30D'] = df['HV_Accel_Proxy']
    if 'VVIX' not in df.columns:
        df['VVIX'] = np.nan
    # Recent_Vol_Spike: detect abnormal vol expansion in last 5 days
    # Sinclair Ch.4: vol clustering means a recent spike predicts near-term vol instability.
    # Criteria: IV jumped >20% in 5 days OR short-term HV spiked >50% above 30-day HV.
    if 'Recent_Vol_Spike' not in df.columns:
        _iv_roc_5d = df.get('IV_30D_5D_ROC', pd.Series(dtype=float))
        _hv10 = df.get('hv_10', df.get('HV_10_D_Cur', pd.Series(dtype=float)))
        _hv30 = df.get('hv_30', df.get('HV_30_D_Cur', pd.Series(dtype=float)))
        _iv_spike = _iv_roc_5d.fillna(0).abs() > 0.20
        _hv_spike = (_hv10.fillna(0) / _hv30.replace(0, np.nan).fillna(1)) > 1.5
        df['Recent_Vol_Spike'] = _iv_spike | _hv_spike

    # GAP 1 FIX: VVIX-based Regime override
    # Natenberg Ch.19 + Sinclair 2020 Ch.5 (Positional Option Trading):
    #   VVIX > 130 = vol spike imminent; vol-of-VIX at extreme = unpredictable vol environment.
    #   Sinclair 2020 Ch.5: VVIX above 90th percentile of own 1-year history → subsequent VIX decline
    #   likely (profitable short). Here we use absolute 130 as operational proxy (requires no
    #   historical VVIX series). 27 of 31 historical extreme-VVIX signals produced profitable VIX shorts.
    # A Regime of 'Compression' while VVIX is screaming expansion is an authority contradiction.
    # VVIX column is populated by Step 0 (fetched as ^VVIX quote) when available.
    # Rule: VVIX > 130 → override to 'Expansion'; VVIX > 100 AND Regime = 'Compression' → 'Uncertain'
    # 'Uncertain' is a new valid Regime value (downstream gates treat it as cautious-neutral).
    #
    # GUARDRAIL 1: Preserve Regime_Original before any VVIX override.
    # Regime stays untouched (original Step 2 IV-rank authority).
    # Regime_Adjusted carries the VVIX-corrected value for downstream strategy gates.
    # VVIX_Override_Applied = True when the two differ — full audit trail without silent destruction.
    df['Regime_Original'] = df['Regime'].copy()
    df['Regime_Adjusted'] = df['Regime'].copy()
    df['VVIX_Override_Applied'] = False
    if 'VVIX' in df.columns:
        vvix_series = pd.to_numeric(df['VVIX'], errors='coerce')
        df['VVIX'] = vvix_series  # Ensure numeric
        # Add VVIX_Flag for audit trail
        df['VVIX_Flag'] = 'NORMAL'
        df.loc[vvix_series > 100, 'VVIX_Flag'] = 'ELEVATED'
        df.loc[vvix_series > 130, 'VVIX_Flag'] = 'EXTREME'
        # Apply overrides to Regime_Adjusted ONLY — Regime (original) is never touched.
        mask_extreme = vvix_series > 130
        mask_elevated = (vvix_series > 100) & (vvix_series <= 130) & (df['Regime_Adjusted'] == 'Compression')
        df.loc[mask_extreme, 'Regime_Adjusted'] = 'Expansion'
        df.loc[mask_elevated, 'Regime_Adjusted'] = 'Uncertain'
        df['VVIX_Override_Applied'] = df['Regime_Adjusted'] != df['Regime_Original']
        extreme_count = mask_extreme.sum()
        uncertain_count = mask_elevated.sum()
        if extreme_count > 0:
            logger.info(f"[GAP1] VVIX > 130: set Regime_Adjusted='Expansion' for {extreme_count} rows (Regime_Original preserved)")
        if uncertain_count > 0:
            logger.info(f"[GAP1] VVIX > 100 + Compression: set Regime_Adjusted='Uncertain' for {uncertain_count} rows")
    if 'IV_Timing_State' not in df.columns:
        df['IV_Timing_State'] = 'BASELINE'
    if 'IV_Chase_Risk' not in df.columns:
        df['IV_Chase_Risk'] = 'LOW'

    return df


def _check_ohlc_availability(df: pd.DataFrame, id_col: str, con: Optional[duckdb.DuckDBPyConnection] = None) -> pd.DataFrame:
    """
    Check OHLC availability for all tickers before pattern detection.

    Exports demand file (ohlc_demand_tickers.csv) for tickers with insufficient OHLC.
    Tags tickers with OHLC_Status field for visibility.

    Args:
        df: DataFrame with tickers
        id_col: Name of ticker column
        con: Optional DuckDB connection

    Returns:
        DataFrame with OHLC_Status and metadata columns added
    """
    from core.shared.data_layer.duckdb_utils import get_duckdb_connection

    df = df.copy()
    df['OHLC_Status'] = 'UNKNOWN'
    df['OHLC_Bar_Count'] = 0
    df['OHLC_Last_Date'] = pd.NaT

    # Use provided connection or open new one
    _con = con
    owns_connection = False
    if _con is None:
        try:
            _con = get_duckdb_connection()
            owns_connection = True
        except Exception as e:
            logger.warning(f"⚠️ Could not open DuckDB for OHLC check: {e}")
            df["Step2_Warning"] = df.apply(lambda row: f"{row['Step2_Warning']}; DuckDB unavailable for OHLC check: {e}" if "Step2_Warning" in row and pd.notna(row["Step2_Warning"]) else f"DuckDB unavailable for OHLC check: {e}", axis=1)
            _con = None

    ohlc_demand = []

    try:
        for idx in df.index:
            ticker = df.loc[idx, id_col]

            if _con is None:
                # No DuckDB access - mark as awaiting fetch
                df.loc[idx, 'OHLC_Status'] = 'AWAITING_FETCH'
                ohlc_demand.append({
                    'ticker': ticker,
                    'reason': 'DuckDB unavailable - cannot check OHLC',
                    'requested_at': datetime.now().isoformat()
                })
                continue

            # Query DuckDB for OHLC metadata
            try:
                result = _con.execute("""
                    SELECT COUNT(*) as bar_count, MAX(date) as last_date
                    FROM price_history
                    WHERE UPPER(ticker) = UPPER(?)
                """, [ticker]).fetchone()

                bar_count = result[0] if result else 0
                last_date = result[1] if result else None

                if bar_count < 30:
                    # Insufficient OHLC
                    df.loc[idx, 'OHLC_Status'] = 'AWAITING_FETCH'
                    df.loc[idx, 'OHLC_Bar_Count'] = bar_count

                    ohlc_demand.append({
                        'ticker': ticker,
                        'reason': f'Insufficient OHLC ({bar_count} bars, need ≥30)',
                        'requested_at': datetime.now().isoformat()
                    })
                else:
                    # Sufficient OHLC
                    df.loc[idx, 'OHLC_Status'] = 'OK'
                    df.loc[idx, 'OHLC_Bar_Count'] = bar_count
                    if last_date:
                        df.loc[idx, 'OHLC_Last_Date'] = pd.to_datetime(last_date)

            except Exception as e:
                logger.debug(f"{ticker}: OHLC availability check failed: {e}")
                df.loc[idx, 'OHLC_Status'] = 'AWAITING_FETCH'
                ohlc_demand.append({
                    'ticker': ticker,
                    'reason': f'OHLC query error: {e}',
                    'requested_at': datetime.now().isoformat()
                })
                df["Step2_Warning"] = df.loc[idx].apply(lambda val: f"{val}; OHLC availability check failed for {ticker}: {e}" if pd.notna(val) else f"OHLC availability check failed for {ticker}: {e}")

    finally:
        # Only close connection if we opened it ourselves
        if owns_connection and _con is not None:
            try:
                _con.close()
            except:
                pass

    # Export demand file if any tickers need OHLC
    if ohlc_demand:
        output_dir = Path(__file__).resolve().parents[1] / "output"
        output_dir.mkdir(exist_ok=True)
        demand_path = output_dir / "ohlc_demand_tickers.csv"

        demand_df = pd.DataFrame(ohlc_demand)
        demand_df.to_csv(demand_path, index=False)

        logger.info(f"📊 Exported {len(ohlc_demand)} tickers to OHLC demand file: {demand_path}")
        logger.info(f"   Run: python yf_fetch.py --mode demand")
        logger.info(f"   Or bulk backfill: python yf_fetch.py --mode bulk")
    else:
        logger.info("✅ All tickers have sufficient OHLC (≥30 bars)")

    # Summary statistics
    status_counts = df['OHLC_Status'].value_counts().to_dict()
    logger.info(f"📊 OHLC Availability Summary:")
    for status, count in status_counts.items():
        logger.info(f"   {status}: {count} tickers")

    return df


def enrich_technical_indicators(df: pd.DataFrame, id_col: str, snapshot_ts: datetime, skip_patterns: bool = False, con: Optional[duckdb.DuckDBPyConnection] = None, schwab_client=None) -> pd.DataFrame:
    """
    Step 2C: Murphy indicators and Bulkowski patterns (Parallelized).
    Uses an existing DuckDB connection if provided for price history loading.
    Pass schwab_client to fetch fresh OHLC from Schwab before falling back to cache.
    """
    from .throttled_executor import ThrottledExecutor
    from core.shared.data_layer.price_history_loader import load_price_history

    # Pre-load fresh OHLC from Schwab into DuckDB BEFORE Murphy indicators run,
    # so both Murphy indicators and pattern detection read fresh data from DuckDB.
    t_step2c = time.time()
    if schwab_client is not None and con is not None:
        all_tickers = df[id_col].tolist()
        logger.info(f"📥 Pre-loading Schwab OHLC for {len(all_tickers)} tickers (writing to DuckDB)...")
        t_schwab = time.time()
        schwab_written = 0
        for _t in all_tickers:
            try:
                _df, _src = load_price_history(_t, days=200, client=schwab_client, skip_auto_fetch=True, con=con)
                if _df is not None and not _df.empty:
                    schwab_written += 1
            except Exception as _e:
                logger.debug(f"{_t}: Schwab OHLC pre-load failed: {_e}")
        logger.info(f"⏱️ Schwab OHLC pre-load: {time.time() - t_schwab:.1f}s ({schwab_written}/{len(all_tickers)} tickers)")

    # Batch pre-load ALL OHLC from DuckDB in a single query (replaces 571 individual reads)
    t_batch = time.time()
    ohlc_cache = {}
    if con is not None:
        try:
            all_tickers = df[id_col].tolist()
            placeholders = ", ".join([f"'{t}'" for t in all_tickers])
            ohlc_batch = con.execute(f"""
                SELECT ticker, date, open_price AS Open, high_price AS High,
                       low_price AS Low, close_price AS Close, volume AS Volume
                FROM price_history
                WHERE UPPER(ticker) IN ({placeholders})
                ORDER BY ticker, date
            """).fetchdf()
            if not ohlc_batch.empty:
                ohlc_batch['date'] = pd.to_datetime(ohlc_batch['date'])
                ohlc_batch = ohlc_batch.set_index('date')
                for tkr, grp in ohlc_batch.groupby('ticker'):
                    ohlc_cache[tkr.upper()] = grp.drop(columns=['ticker']).tail(200)
            logger.info(f"📊 Batch OHLC pre-load: {len(ohlc_cache)}/{len(all_tickers)} tickers from DuckDB ({time.time() - t_batch:.1f}s)")
        except Exception as e:
            logger.warning(f"⚠️ Batch OHLC pre-load failed (falling back to per-ticker): {e}")
            ohlc_cache = {}

    # Murphy Indicators — uses batch-loaded OHLC when available, per-ticker DuckDB fallback otherwise
    t_murphy = time.time()
    all_tickers_list = df[id_col].tolist()
    all_cached = len(ohlc_cache) >= len(all_tickers_list) * 0.9  # 90%+ cache hit = no I/O needed

    if all_cached:
        # All OHLC in memory — no network I/O, no rate limiting needed
        from concurrent.futures import ThreadPoolExecutor as _TPE
        logger.info(f"📊 Enriching Murphy indicators (in-memory, no rate limit, {len(all_tickers_list)} tickers)...")
        with _TPE(max_workers=10) as pool:
            murphy_results = list(pool.map(
                lambda ticker: _calculate_murphy_indicators(ticker, id_col, snapshot_ts, con=con, ohlc_df=ohlc_cache.get(ticker.upper())),
                all_tickers_list
            ))
    else:
        # Some tickers need DuckDB/network reads — use rate-limited executor
        logger.info("📊 Enriching with Murphy technical indicators (parallelized, rate-limited)...")
        with ThrottledExecutor(max_workers=10, requests_per_second=5) as executor:
            murphy_results = executor.map_parallel(
                lambda ticker: _calculate_murphy_indicators(ticker, id_col, snapshot_ts, con=con, ohlc_df=ohlc_cache.get(ticker.upper())),
                all_tickers_list,
                desc="Murphy Indicators"
            )
    
    # Identify columns that will be merged from murphy_results
    # Ensure murphy_results is not empty before trying to access its elements
    if murphy_results:
        murphy_cols_to_merge = [col for col_dict in murphy_results for col in col_dict.keys() if col != id_col]
        murphy_cols_to_merge = list(set(murphy_cols_to_merge)) # Get unique column names
        
        # Drop existing murphy_results columns from df to avoid merge conflicts
        df = df.drop(columns=[col for col in murphy_cols_to_merge if col in df.columns], errors='ignore')
    
    df = df.merge(pd.DataFrame(murphy_results), on=id_col, how='left')
    n_batch_hits = sum(1 for t in df[id_col] if t.upper() in ohlc_cache)
    logger.info(f"⏱️ Step 2C (Murphy): {time.time() - t_murphy:.1f}s ({n_batch_hits} batch hits, {len(df) - n_batch_hits} fallback)")

    # RV/IV Ratio
    if 'RV_10D' in df.columns and 'IV_30_D_Call' in df.columns:
        df['RV_IV_Ratio'] = np.where((df['RV_10D'].notna()) & (df['IV_30_D_Call'] > 0), df['RV_10D'] / df['IV_30_D_Call'], np.nan)

    # OHLC Availability Check (before pattern detection)
    if not skip_patterns:
        logger.info("📊 Checking OHLC availability for pattern detection...")
        df = _check_ohlc_availability(df, id_col, con)

    # Bulkowski Patterns
    if not skip_patterns:
        # Separate tickers with/without OHLC (based on OHLC_Status from availability check)
        has_ohlc_status = 'OHLC_Status' in df.columns
        if has_ohlc_status:
            tickers_with_ohlc = df[df['OHLC_Status'] == 'OK'][id_col].tolist()
            tickers_without_ohlc = df[df['OHLC_Status'] == 'AWAITING_FETCH'][id_col].tolist()
        else:
            # Fallback: run for all tickers if OHLC_Status not available
            tickers_with_ohlc = df[id_col].tolist()
            tickers_without_ohlc = []

        # Pre-tag tickers without OHLC as NOT_EVALUATED
        pattern_results = []
        if tickers_without_ohlc:
            logger.warning(f"⚠️ Skipping pattern detection for {len(tickers_without_ohlc)} tickers with insufficient OHLC")
            for ticker in tickers_without_ohlc:
                pattern_results.append({
                    id_col: ticker,
                    'Chart_Pattern': 'NOT_EVALUATED',
                    'Pattern_Confidence': 0.0,
                    'Candlestick_Pattern': 'NOT_EVALUATED',
                    'Entry_Timing_Quality': 'LOW',
                    'Reversal_Confirmation': False,
                    'Pattern_Resolution_Path': 'Run: python yf_fetch.py --mode demand'
                })

        # Run pattern detection only for tickers with sufficient OHLC
        if tickers_with_ohlc:
            logger.info(f"📊 Detecting Bulkowski chart patterns for {len(tickers_with_ohlc)} tickers with sufficient OHLC (parallelized)...")
            from utils.pattern_detection import detect_bulkowski_patterns, detect_nison_candlestick

            # Pre-load OHLC on the main thread from DuckDB cache.
            # Schwab data was already written to DuckDB at the top of this function,
            # so _load_from_duckdb_cache gets fresh data for all Schwab-fetched tickers.
            # Workers must NOT open their own DuckDB connections while the pipeline
            # holds a read-write connection — DuckDB rejects mixed-mode opens.
            from core.shared.data_layer.price_history_loader import _load_from_duckdb_cache
            logger.info(f"📥 Pre-loading OHLC for {len(tickers_with_ohlc)} tickers from DuckDB (main thread)...")
            ohlc_cache: dict = {}
            cache_ok = 0
            for _t in tickers_with_ohlc:
                try:
                    _df = _load_from_duckdb_cache(_t, days=200, con=con)
                    if _df is not None and not _df.empty:
                        ohlc_cache[_t] = _df
                        cache_ok += 1
                except Exception as _e:
                    logger.debug(f"{_t}: OHLC pre-load failed: {_e}")
            logger.info(f"📥 Pre-loaded OHLC: {len(ohlc_cache)}/{len(tickers_with_ohlc)} tickers from DuckDB")

            def _process_patterns(ticker):
                try:
                    df_price = ohlc_cache.get(ticker)
                    # Pass pre-loaded DataFrame directly — no DB access in workers
                    p, conf = detect_bulkowski_patterns(ticker, df_price=df_price, skip_db_fetch=True)
                    c, timing = detect_nison_candlestick(ticker, df_price=df_price, skip_db_fetch=True)

                    result = {
                        id_col: ticker,
                        'Chart_Pattern': p,
                        'Pattern_Confidence': conf,
                        'Candlestick_Pattern': c,
                        'Entry_Timing_Quality': timing,
                        'Reversal_Confirmation': (timing == 'Strong')
                    }

                    if p == 'NOT_EVALUATED' or c == 'NOT_EVALUATED':
                        result['Pattern_Resolution_Path'] = 'Insufficient OHLC history - fetch Yahoo Finance OHLC'

                    return result

                except Exception as e:
                    logger.warning(f"{ticker}: Pattern detection failed unexpectedly: {e}")
                    return {
                        id_col: ticker,
                        'Chart_Pattern': 'NOT_EVALUATED',
                        'Pattern_Confidence': 0.0,
                        'Candlestick_Pattern': 'NOT_EVALUATED',
                        'Entry_Timing_Quality': 'LOW',
                        'Reversal_Confirmation': False,
                        'Pattern_Resolution_Path': 'Pattern detection exception occurred'
                    }

            with ThrottledExecutor(max_workers=10, requests_per_second=10) as executor:
                pattern_results_with_ohlc = executor.map_parallel(_process_patterns, tickers_with_ohlc, desc="Pattern Detection")
                pattern_results.extend(pattern_results_with_ohlc)
        elif not tickers_without_ohlc:
            # No tickers at all - log error
            logger.error("❌ No tickers available for pattern detection")
            df["Step2_Warning"] = df.apply(lambda row: f"{row['Step2_Warning']}; No tickers available for pattern detection" if "Step2_Warning" in row and pd.notna(row["Step2_Warning"]) else "No tickers available for pattern detection", axis=1)
            return df
        
        # Identify columns that will be merged from pattern_results
        if pattern_results:
            pattern_cols_to_merge = [col for col_dict in pattern_results for col in col_dict.keys() if col != id_col]
            pattern_cols_to_merge = list(set(pattern_cols_to_merge)) # Get unique column names
            
            # Drop existing pattern_results columns from df to avoid merge conflicts
            df = df.drop(columns=[col for col in pattern_cols_to_merge if col in df.columns], errors='ignore')
        
        df = df.merge(pd.DataFrame(pattern_results), on=id_col, how='left')

    # Signal Type Mapping
    if 'Trend_State' in df.columns:
        df['Signal_Type'] = df['Trend_State'].map({'Bullish': 'Bullish', 'Bearish': 'Bearish', 'Neutral': 'Bidirectional'}).fillna('Unknown')

    logger.info(f"⏱️ Step 2C total: {time.time() - t_step2c:.1f}s")
    return df


def enrich_market_context(df: pd.DataFrame, id_col: str) -> pd.DataFrame:
    """
    Step 2D: Entry quality and Earnings proximity.
    """
    # Entry Quality
    try:
        from .loaders.entry_quality_enhancements import enrich_snapshot_with_entry_quality
        df = enrich_snapshot_with_entry_quality(df)
    except Exception as e:
        logger.warning(f"⚠️ Entry quality enrichment failed: {e}")
        df["Step2_Warning"] = df.apply(lambda row: f"{row['Step2_Warning']}; Entry quality enrichment failed: {e}" if "Step2_Warning" in row and pd.notna(row["Step2_Warning"]) else f"Entry quality enrichment failed: {e}", axis=1)

    # Earnings Proximity
    try:
        from core.shared.data_layer.earnings_calendar import add_earnings_proximity
        snapshot_ts = df['timestamp'].iloc[0]
        df = add_earnings_proximity(df, pd.to_datetime(snapshot_ts))
    except Exception as e:
        logger.warning(f"⚠️ Earnings proximity failed: {e}")
        df['earnings_proximity_flag'] = False
        df["Step2_Warning"] = df.apply(lambda row: f"{row['Step2_Warning']}; Earnings proximity failed: {e}" if "Step2_Warning" in row and pd.notna(row["Step2_Warning"]) else f"Earnings proximity failed: {e}", axis=1)

    return df


from .debug.debug_mode import get_debug_manager # Import DebugManager
import duckdb # Import duckdb globally for type hinting

def load_ivhv_snapshot(
    snapshot_path: str = None,
    max_age_hours: int = 48,
    skip_pattern_detection: bool = False,
    use_live_snapshot: bool = False,
    con: Optional[duckdb.DuckDBPyConnection] = None,
    schwab_client=None,
) -> pd.DataFrame:
    """
    Modularized Step 2 Orchestrator.
    """
    df = pd.DataFrame()
    id_col = 'Ticker'

    # Initialize Step2_Warning column
    df['Step2_Warning'] = np.nan

    try:
        # 2A: Load
        df, id_col = load_raw_snapshot(snapshot_path, max_age_hours, use_live_snapshot)
        df['Step2_Warning'] = np.nan

        # 2A.5: Canonical Price Resolution
        logger.info("📊 Resolving canonical Stock_Price (demand-driven, no auto-fetch)")
        df = _resolve_canonical_price(df, id_col, con=con)

        # 2B: Volatility
        df = enrich_volatility_metrics(df, id_col)

        # 2C: Technical (pass Schwab client for live OHLC fetch)
        snapshot_ts_for_tech = df['timestamp'].iloc[0]
        df = enrich_technical_indicators(df, id_col, snapshot_ts=snapshot_ts_for_tech, con=con, schwab_client=schwab_client)
        
        # 2D: Context
        df = enrich_market_context(df, id_col)
        
        # Step 7 Compatibility
        if 'IV_30_D_Call' in df.columns and 'HV_30_D_Cur' in df.columns:
            df['IVHV_gap_30D'] = df['IV_30_D_Call'] - df['HV_30_D_Cur']

        # Centralized Universe Restriction (Controlled by DEBUG_TICKER_MODE)
        debug_manager = get_debug_manager()
        if os.getenv("DEBUG_TICKER_MODE") == "1":
            original_df_len = len(df)
            debug_tickers_upper = [t.upper() for t in debug_manager.debug_tickers]
            df = df[df['Ticker'].str.upper().isin(debug_tickers_upper)].copy()
            logger.info(f"🧪 DEBUG TICKER MODE ACTIVE - Restricted universe from {original_df_len} to {len(df)} tickers: {debug_manager.debug_tickers}")
            if df.empty:
                warning_msg = f"DataFrame became empty after universe restriction. Debug tickers {debug_manager.debug_tickers} not found in snapshot."
                logger.warning(f"⚠️ {warning_msg}")
                df = pd.DataFrame(columns=df.columns) # Ensure empty df has columns
                df['Step2_Warning'] = warning_msg # Add warning to the (empty) df
                # Do not raise, continue with empty df
            
        logger.info(f"✅ Step 2 complete: {len(df)} tickers loaded and enriched")

        # Phase 6: Add Deterministic Logging
        if not df.empty:
            if 'IV_Maturity_State' in df.columns:
                logger.info(f"📊 IV Maturity Distribution: {df['IV_Maturity_State'].value_counts().to_dict()}")
        else:
            logger.info("📊 No tickers to log IV Maturity Distribution or IV Surface Availability (DataFrame is empty).")

        return df
        
    except Exception as e:
        error_msg = f"Step 2 failed: {e}"
        logger.error(f"❌ {error_msg}", exc_info=True)
        # Ensure df has 'Step2_Warning' column even if it failed early
        if 'Step2_Warning' not in df.columns:
            df['Step2_Warning'] = np.nan
        df['Step2_Warning'] = df.apply(lambda row: f"{row['Step2_Warning']}; {error_msg}" if pd.notna(row["Step2_Warning"]) else error_msg, axis=1)
        return df # Return df with error logged as warning, do not raise


def _calculate_murphy_indicators(id_val: str, id_col_name: str, snapshot_ts: datetime, con: Optional[duckdb.DuckDBPyConnection] = None, ohlc_df: Optional[pd.DataFrame] = None) -> dict:
    """
    Calculate Murphy technical indicators.
    Uses batch-loaded ohlc_df when provided, otherwise falls back to per-ticker DuckDB load.
    """
    result = {
        id_col_name: id_val,
        'Trend_State': 'Unknown',
        'Price_vs_SMA20': np.nan,
        'Price_vs_SMA50': np.nan,
        'Volume_Trend': 'Unknown',
        'ADX': np.nan,
        'RSI': np.nan,
        'Trend_Strength': 'Unknown',
        'RV_10D': np.nan,
        'RV_Calculated': False,
        'Step2_Warning': np.nan # Add warning column to results
    }

    try:
        if ohlc_df is not None and not ohlc_df.empty:
            df_price = ohlc_df
            source = "BATCH_CACHE"
        else:
            # DEMAND-DRIVEN: Use cached OHLC only, do NOT auto-fetch from YF during pipeline
            df_price, source = load_price_history(id_val, days=200, skip_auto_fetch=True, con=con)

        if df_price is None or df_price.empty or len(df_price) < 30:
            result['Step2_Warning'] = f"Insufficient price history for {id_val} to calculate Murphy indicators."
            return result
        
        current_price = df_price['Close'].iloc[-1]
        sma20 = df_price['Close'].rolling(20).mean().iloc[-1]
        sma50 = df_price['Close'].rolling(50).mean().iloc[-1]
        
        if pd.notna(sma20):
            result['Price_vs_SMA20'] = ((current_price - sma20) / sma20) * 100
        if pd.notna(sma50):
            result['Price_vs_SMA50'] = ((current_price - sma50) / sma50) * 100
        
        try:
            returns_10d = df_price['Close'].pct_change().tail(10)
            if len(returns_10d) >= 10:
                rv_10d = returns_10d.std() * np.sqrt(252) * 100
                result['RV_10D'] = rv_10d
                result['RV_Calculated'] = True
            else:
                result['RV_10D'] = np.nan
                result['RV_Calculated'] = False
        except Exception as e:
            result['RV_10D'] = np.nan
            result['RV_Calculated'] = False
            result['Step2_Warning'] = f"RV_10D calculation failed for {id_val}: {e}"
        
        if pd.notna(sma20) and pd.notna(sma50):
            if current_price > sma20 and current_price > sma50:
                result['Trend_State'] = 'Bullish'
            elif current_price < sma20 and current_price < sma50:
                result['Trend_State'] = 'Bearish'
            else:
                result['Trend_State'] = 'Neutral'
        
        if 'Volume' in df_price.columns:
            vol_sma20 = df_price['Volume'].rolling(20).mean()
            current_vol = df_price['Volume'].iloc[-1]
            avg_vol = vol_sma20.iloc[-1]
            
            if pd.notna(avg_vol) and avg_vol > 0:
                vol_ratio = current_vol / avg_vol
                if vol_ratio > 1.2:
                    result['Volume_Trend'] = 'Rising'
                elif vol_ratio < 0.8:
                    result['Volume_Trend'] = 'Falling'
                else:
                    result['Volume_Trend'] = 'Stable'
        
        # Use ta_lib_utils for RSI and ADX
        result['RSI'] = calculate_rsi(df_price['Close'], timeperiod=14).iloc[-1]
        result['ADX'] = calculate_adx(df_price['High'], df_price['Low'], df_price['Close'], timeperiod=14).iloc[-1]
        
        adx_val = result['ADX']
        if pd.notna(adx_val):
            if adx_val > 25:
                result['Trend_Strength'] = 'Strong'
            elif adx_val > 15:
                result['Trend_Strength'] = 'Moderate'
            else:
                result['Trend_Strength'] = 'Weak'

    except Exception as e:
        logger.error(f"[ERROR] {id_val}: {type(e).__name__}: {str(e)}")
        result['Step2_Warning'] = f"Murphy indicator calculation failed for {id_val}: {e}"
    
    return result
