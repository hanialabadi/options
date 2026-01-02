"""
Step 2: Load IV/HV Snapshot from Fidelity Export
"""

import pandas as pd
import numpy as np
import logging
import os
from pathlib import Path
from datetime import datetime, timedelta
import yfinance as yf

try:
    import pandas_ta
    TA_AVAILABLE = True
except ImportError:
    TA_AVAILABLE = False
    logging.warning("pandas_ta not available - ADX/RSI will be unavailable")

logger = logging.getLogger(__name__)


def load_latest_live_snapshot(snapshot_dir: str = "data/snapshots") -> str:
    """
    Load the most recent ivhv_snapshot_live_*.csv file from Step 0.
    
    Args:
        snapshot_dir: Directory containing live snapshots (default: data/snapshots)
    
    Returns:
        str: Absolute path to the latest live snapshot file
    
    Raises:
        FileNotFoundError: If no live snapshot files found
    
    Example:
        >>> path = load_latest_live_snapshot()
        >>> df = pd.read_csv(path)
    """
    snapshot_path = Path(snapshot_dir)
    
    if not snapshot_path.exists():
        raise FileNotFoundError(
            f"❌ Snapshot directory not found: {snapshot_path}\n"
            f"   Run Step 0 first: python core/scan_engine/step0_schwab_snapshot.py"
        )
    
    # Find all live snapshot files (pattern: ivhv_snapshot_live_YYYYMMDD_HHMMSS.csv)
    live_snapshots = sorted(
        snapshot_path.glob("ivhv_snapshot_live_*.csv"),
        key=lambda f: f.stat().st_mtime,  # Sort by modification time
        reverse=True
    )
    
    if not live_snapshots:
        raise FileNotFoundError(
            f"❌ No live snapshots found in {snapshot_path}\n"
            f"   Expected pattern: ivhv_snapshot_live_YYYYMMDD_HHMMSS.csv\n"
            f"   Run Step 0 first: python core/scan_engine/step0_schwab_snapshot.py"
        )
    
    latest = live_snapshots[0]
    age_hours = (datetime.now() - datetime.fromtimestamp(latest.stat().st_mtime)).total_seconds() / 3600
    
    logger.info(f"✅ Found latest live snapshot: {latest.name} (age: {age_hours:.1f}h)")
    
    # Validate required columns
    try:
        df = pd.read_csv(latest)
        required = ['Ticker', 'last_price', 'hv_10', 'hv_20', 'hv_30', 'hv_slope', 
                   'volatility_regime', 'data_source', 'snapshot_ts']
        missing = [col for col in required if col not in df.columns]
        if missing:
            logger.warning(f"⚠️ Live snapshot missing columns: {missing}")
    except Exception as e:
        logger.error(f"❌ Failed to validate snapshot: {e}")
    
    return str(latest.resolve())


def load_ivhv_snapshot(
    snapshot_path: str = None, 
    max_age_hours: int = 48, 
    skip_pattern_detection: bool = False,
    use_live_snapshot: bool = False
) -> pd.DataFrame:
    """
    Load IV/HV snapshot from CSV with validation and enrichment.
    
    NOTE:
    This step is strictly DESCRIPTIVE.
    It must not introduce strategy assumptions, thresholds,
    pass/fail flags, or trade intent.
    All strategy decisions occur in later phases.
    
    Purpose:
        Load implied volatility (IV) and historical volatility (HV) data from:
        - Live Schwab snapshots (Step 0) if use_live_snapshot=True
        - Manual Fidelity exports (legacy) if use_live_snapshot=False
        
        Validates data freshness, fixes data types, and computes derived fields
        (IV Rank, term structure, trends). Handles NaN IV values gracefully.
    
    Args:
        snapshot_path (str, optional): Path to IV/HV CSV file. 
            If None, uses FIDELITY_SNAPSHOT_PATH env var (legacy mode)
            or loads latest live snapshot (if use_live_snapshot=True).
        max_age_hours (int, optional): Max acceptable snapshot age. Default 48 hours.
            Logs warning (doesn't fail) if exceeded.
        skip_pattern_detection (bool, optional): Skip Bulkowski/Nison pattern detection
            (saves ~3 minutes for 175 tickers). Default False. Use True for testing/validation.
        use_live_snapshot (bool, optional): If True, automatically load latest live snapshot
            from Step 0 (data/snapshots/ivhv_snapshot_live_*.csv). Default False for
            backward compatibility with manual CSV workflows.
    
    Returns:
        pd.DataFrame: Validated snapshot with columns:
            Original columns:
            - Symbol: Stock symbol # Changed 'Ticker' to 'Symbol'
            - timestamp: Snapshot timestamp (parsed to datetime)
            - IV_7_D_Call, IV_30_D_Call, IV_90_D_Call: Multi-timeframe IV
            - IV_30_D_Call_1W, IV_30_D_Call_1M: Historical IV (1 week/month ago)
            - IV_30_D_CallChg: Daily IV change
            - HV_10_D_Cur, HV_30_D_Cur, HV_90_D_Cur: Multi-timeframe HV
            
            Derived columns (added by this function):
            - IV_Rank_30D: Per-ticker recent-range percentile (not 52-week IV Rank)
                          Based on 1-month lookback: (current - min) / (max - min) * 100
            - IV_Term_Structure: 'Contango', 'Inverted', 'Flat', or 'Unknown'
            - IV_Trend_7D: 'Rising', 'Falling', 'Stable' (based on 1-week change)
            - HV_Trend_30D: 'Rising', 'Falling', 'Stable' (HV10 vs HV30)
            - Snapshot_Age_Hours: Hours since snapshot creation
    
    Raises:
        FileNotFoundError: If snapshot file doesn't exist
        ValueError: If required columns missing or data malformed
    
    Example:
        >>> df = load_ivhv_snapshot('/path/to/snapshot.csv')
        >>> print(df[['Symbol', 'IV_Rank_30D', 'IV_Term_Structure']].head()) # Changed 'Ticker' to 'Symbol'
    
    Data Quality Checks:
        - Timestamp validation (warns if stale)
        - Data type enforcement (HV columns forced to numeric)
        - Missing value detection
        - Duplicate symbol detection # Changed 'ticker' to 'symbol'
    """
    # Step 0 Integration: Load latest live snapshot if requested
    if use_live_snapshot:
        if snapshot_path is not None:
            logger.warning(
                f"⚠️ Both use_live_snapshot=True and snapshot_path provided. "
                f"Ignoring snapshot_path and loading latest live snapshot."
            )
        snapshot_path = load_latest_live_snapshot()
        logger.info(f"✅ Using live snapshot from Step 0: {Path(snapshot_path).name}")
    elif snapshot_path is None:
        snapshot_path = os.getenv('FIDELITY_SNAPSHOT_PATH', 
                                   '/Users/haniabadi/Documents/Windows/OptionsSnapshots/fidelity_ivhv_snapshot.csv')
    
    snapshot_path = Path(snapshot_path)
    
    try:
        df = pd.read_csv(snapshot_path)
        logger.info(f"✅ Loaded snapshot: {snapshot_path} ({df.shape[0]} rows, {df.shape[1]} cols)")
        
        # ====================
        # DATA QUALITY CHECKS
        # ====================
        
        # 1. Validate timestamp and check freshness
        if 'timestamp' not in df.columns:
            logger.warning("⚠️ No 'timestamp' column found. Cannot validate freshness.")
            df['Snapshot_Age_Hours'] = np.nan
        else:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            snapshot_time = df['timestamp'].iloc[0]
            age_hours = (datetime.now() - snapshot_time).total_seconds() / 3600
            df['Snapshot_Age_Hours'] = age_hours
            
            if age_hours > max_age_hours:
                logger.warning(
                    f"⚠️ Snapshot is {age_hours:.1f} hours old "
                    f"(threshold: {max_age_hours}h). Data may be stale."
                )
            else:
                logger.info(f"✅ Snapshot age: {age_hours:.1f} hours (fresh)")
        
        # 2. Fix data types (HV columns stored as strings in CSV)
        hv_cols = [col for col in df.columns if col.startswith('HV_') and '_Cur' in col]
        for col in hv_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        logger.info(f"✅ Fixed data types for {len(hv_cols)} HV columns")
        
        # 3. Check for duplicates
        if 'Symbol' in df.columns and df['Symbol'].duplicated().any(): # Added 'Symbol' in df.columns check
            dup_count = df['Symbol'].duplicated().sum()
            logger.warning(f"⚠️ Found {dup_count} duplicate symbols. Keeping first occurrence.")
            df = df.drop_duplicates(subset='Symbol', keep='first')
        elif 'Ticker' in df.columns and df['Ticker'].duplicated().any(): # Added fallback for 'Ticker'
            dup_count = df['Ticker'].duplicated().sum()
            logger.warning(f"⚠️ Found {dup_count} duplicate tickers. Keeping first occurrence.")
            df = df.drop_duplicates(subset='Ticker', keep='first')
        
        # 4. Detect missing critical columns
        # Prioritize 'Symbol', fallback to 'Ticker' if 'Symbol' is not present
        if 'Symbol' in df.columns:
            required_id_col = 'Symbol'
        elif 'Ticker' in df.columns:
            required_id_col = 'Ticker'
        else:
            raise ValueError("Missing required identifier column: 'Symbol' or 'Ticker'")

        # Required columns: Ticker/Symbol and HV are mandatory, IV is optional
        # (Step 0 can run in HV-only mode where IV columns will be NaN)
        required_cols = [required_id_col, 'HV_30_D_Cur']
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        
        # Warn if IV data is missing (expected for HV-only Step 0 runs)
        if 'IV_30_D_Call' not in df.columns:
            logger.warning(
                "⚠️ IV_30_D_Call column not found. "
                "IV-based enrichments (IV Rank, term structure, trends) will be skipped. "
                "This is expected for HV-only snapshots from Step 0."
            )
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        
        # ====================
        # DERIVED FIELDS
        # ====================
        
        # 5. Calculate IV Rank (per-ticker percentile based on 1-month lookback)
        if 'IV_30_D_Call_1M' in df.columns:
            df['IV_Rank_30D'] = df.apply(
                lambda row: _calculate_iv_rank(
                    current=row.get('IV_30_D_Call'),
                    iv_1w=row.get('IV_30_D_Call_1W'),
                    iv_1m=row.get('IV_30_D_Call_1M')
                ), axis=1
            )
            logger.info("✅ Calculated IV_Rank_30D (per-ticker percentile)")
        else:
            logger.warning("⚠️ Cannot calculate IV Rank: IV_30_D_Call_1M missing")
            df['IV_Rank_30D'] = np.nan
        
        # 6. Detect IV term structure (contango vs inversion)
        if all(col in df.columns for col in ['IV_7_D_Call', 'IV_30_D_Call', 'IV_90_D_Call']):
            df['IV_Term_Structure'] = df.apply(
                lambda row: _classify_term_structure(
                    iv7=row.get('IV_7_D_Call'),
                    iv30=row.get('IV_30_D_Call'),
                    iv90=row.get('IV_90_D_Call')
                ), axis=1
            )
            logger.info("✅ Classified IV term structure")
        else:
            logger.warning("⚠️ Cannot classify term structure: Missing IV columns")
            df['IV_Term_Structure'] = 'Unknown'
        
        # 7. Detect IV trend (rising vs falling)
        if 'IV_30_D_Call_1W' in df.columns:
            df['IV_Trend_7D'] = df.apply(
                lambda row: _detect_trend(
                    current=row.get('IV_30_D_Call'),
                    past=row.get('IV_30_D_Call_1W'),
                    threshold=0.05  # 5% change threshold
                ), axis=1
            )
            logger.info("✅ Detected IV_Trend_7D")
        else:
            df['IV_Trend_7D'] = 'Unknown'
        
        # 8. Detect HV trend
        if all(col in df.columns for col in ['HV_10_D_Cur', 'HV_30_D_Cur']):
            df['HV_Trend_30D'] = df.apply(
                lambda row: _detect_trend(
                    current=row.get('HV_10_D_Cur'),
                    past=row.get('HV_30_D_Cur'),
                    threshold=0.05
                ), axis=1
            )
            logger.info("✅ Detected HV_Trend_30D")
        else:
            df['HV_Trend_30D'] = 'Unknown'
        
        # ====================
        # SINCLAIR: VOLATILITY REGIME CLASSIFICATION
        # ====================
        
        # 9. Calculate VVIX (vol-of-vol proxy: 20-day rolling std of IV)
        if all(col in df.columns for col in ['IV_30_D_Call', 'IV_30_D_Call_1W', 'IV_30_D_Call_1M']):
            df['VVIX'] = df.apply(
                lambda row: _calculate_vvix(
                    current=row.get('IV_30_D_Call'),
                    iv_1w=row.get('IV_30_D_Call_1W'),
                    iv_1m=row.get('IV_30_D_Call_1M')
                ), axis=1
            )
            logger.info("✅ Calculated VVIX (vol-of-vol proxy)")
        else:
            logger.warning("⚠️ Cannot calculate VVIX: Missing IV historical columns")
            df['VVIX'] = np.nan
        
        # 10. Detect recent vol spike (clustering)
        if all(col in df.columns for col in ['IV_30_D_Call', 'IV_30_D_Call_1W']):
            df['Recent_Vol_Spike'] = df.apply(
                lambda row: _detect_vol_spike(
                    current=row.get('IV_30_D_Call'),
                    iv_1w=row.get('IV_30_D_Call_1W'),
                    iv_1m=row.get('IV_30_D_Call_1M')
                ), axis=1
            )
            logger.info("✅ Detected Recent_Vol_Spike (clustering check)")
        else:
            logger.warning("⚠️ Cannot detect vol spikes: Missing IV_30_D_Call_1W")
            df['Recent_Vol_Spike'] = False
        
        # 11. Classify Volatility Regime (Sinclair Ch.2-4)
        if all(col in df.columns for col in ['IV_30_D_Call', 'IV_Rank_30D', 'IV_Trend_7D', 'VVIX']):
            df['Volatility_Regime'] = df.apply(
                lambda row: _classify_volatility_regime(
                    iv_rank=row.get('IV_Rank_30D'),
                    iv_trend=row.get('IV_Trend_7D'),
                    vvix=row.get('VVIX')
                ), axis=1
            )
            logger.info("✅ Classified Volatility_Regime (Sinclair)")
        else:
            logger.warning("⚠️ Cannot classify Volatility_Regime: Missing prerequisites")
            df['Volatility_Regime'] = 'Unknown'
        
        # ====================
        # MURPHY: TREND & MOMENTUM INDICATORS
        # ====================
        
        logger.info("📊 Enriching with Murphy technical indicators (trend/momentum)...")
        
        logger.info("📊 Enriching with Murphy technical indicators (trend/momentum)...")
        
        # 12. Add Murphy trend and momentum indicators per symbol/ticker
        murphy_results = []
        # Use the identified required_id_col for iteration
        for id_val in df[required_id_col]:
            murphy_data = _calculate_murphy_indicators(id_val, required_id_col)
            murphy_results.append(murphy_data)
        
        murphy_df = pd.DataFrame(murphy_results)
        df = df.merge(murphy_df, on=required_id_col, how='left')
        
        logger.info(f"✅ Added Murphy indicators: Trend_State, Volume_Trend, ADX, RSI, Price_vs_SMA20/50, RV_10D")
        
        # Calculate RV/IV Ratio (CRITICAL for vol edge validation)
        # RAG: "Long vol strategies only if IV/RV <0.90"
        # Audit: "60% false positive reduction with RV/IV validation"
        if 'RV_10D' in df.columns and 'IV_30_D_Call' in df.columns:
            df['RV_IV_Ratio'] = np.where(
                (df['RV_10D'].notna()) & (df['IV_30_D_Call'].notna()) & (df['IV_30_D_Call'] > 0),
                df['RV_10D'] / df['IV_30_D_Call'],
                np.nan
            )
            logger.info(f"✅ Calculated RV/IV Ratio for vol edge detection")
        else:
            df['RV_IV_Ratio'] = np.nan
            logger.warning("⚠️ Cannot calculate RV/IV Ratio: Missing RV_10D or IV_30_D_Call")
        
        # ====================
        # BULKOWSKI: CHART PATTERN DETECTION
        # ====================
        
        if skip_pattern_detection:
            logger.info("⏭️ Skipping pattern detection (skip_pattern_detection=True)")
            # Add empty pattern columns for downstream compatibility
            df['Chart_Pattern'] = None
            df['Pattern_Confidence'] = 0.0
            df['Candlestick_Pattern'] = None
            df['Entry_Timing_Quality'] = None
            df['Reversal_Confirmation'] = False
        else:
            logger.info("📊 Detecting Bulkowski chart patterns (statistical edge)...")
            
            from utils.pattern_detection import detect_bulkowski_patterns, detect_nison_candlestick
            
            pattern_results = []
            for id_val in df[required_id_col]: # Use the identified required_id_col for iteration
                try:
                    # Detect Bulkowski patterns
                    pattern, confidence = detect_bulkowski_patterns(id_val)
                    
                    # Detect Nison candlestick signals
                    candlestick, entry_timing = detect_nison_candlestick(id_val)
                    
                    pattern_results.append({
                        required_id_col: id_val, # Use the identified required_id_col
                        'Chart_Pattern': pattern,
                        'Pattern_Confidence': confidence,
                        'Candlestick_Pattern': candlestick,
                        'Entry_Timing_Quality': entry_timing,
                        'Reversal_Confirmation': (entry_timing == 'Strong' if entry_timing else False)
                    })
                except Exception as e:
                    logger.debug(f"{id_val}: Pattern detection failed: {e}")
                    pattern_results.append({
                        required_id_col: id_val, # Use the identified required_id_col
                        'Chart_Pattern': None,
                        'Pattern_Confidence': 0.0,
                        'Candlestick_Pattern': None,
                        'Entry_Timing_Quality': None,
                        'Reversal_Confirmation': False
                    })
            
            pattern_df = pd.DataFrame(pattern_results)
            df = df.merge(pattern_df, on=required_id_col, how='left')
            
            patterns_found = df['Chart_Pattern'].notna().sum()
            candlesticks_found = df['Candlestick_Pattern'].notna().sum()
            logger.info(f"✅ Detected Bulkowski patterns: {patterns_found}/{len(df)} {required_id_col.lower()}s")
            logger.info(f"✅ Detected Nison candlesticks: {candlesticks_found}/{len(df)} {required_id_col.lower()}s")
        
        # ====================
        # ADD STEP 7 COMPATIBILITY FIELDS (normally from Step 3)
        # ====================
        
        # Calculate IVHV_gap_30D (IV-HV divergence)
        if 'IV_30_D_Call' in df.columns and 'HV_30_D_Cur' in df.columns:
            df['IVHV_gap_30D'] = df['IV_30_D_Call'] - df['HV_30_D_Cur']
            logger.info(f"✅ Calculated IVHV_gap_30D (IV-HV divergence)")
        else:
            df['IVHV_gap_30D'] = 0.0
            logger.warning("⚠️ Cannot calculate IVHV_gap_30D: Missing IV or HV columns")
        
        # Add Signal_Type (directional bias from Murphy Trend_State)
        if 'Trend_State' in df.columns:
            df['Signal_Type'] = df['Trend_State'].map({
                'Bullish': 'Bullish',
                'Bearish': 'Bearish',
                'Neutral': 'Bidirectional'
            }).fillna(pd.NA) # Use pd.NA for missing values
            logger.info(f"✅ Mapped Trend_State → Signal_Type")
        else:
            df['Signal_Type'] = 'Unknown'  # Default to 'Unknown' not pd.NA to avoid hard failure
            logger.warning("⚠️ No Trend_State column, Signal_Type defaulted to 'Unknown' (chart signals will be computed in Step 5)")
        
        # Add Regime (alias for Volatility_Regime for Step 7 compatibility)
        # 🚨 AUTHORITATIVE: 'Regime' is an authoritative output of Step 2 ONLY.
        # Downstream steps (e.g., Step 3) must NOT mutate this column.
        if 'Volatility_Regime' in df.columns:
            df['Regime'] = df['Volatility_Regime']
            logger.info(f"✅ Aliased Volatility_Regime → Regime")
        else:
            df['Regime'] = 'Unknown'  # Default to 'Unknown' not pd.NA
            logger.warning("⚠️ No Volatility_Regime column, Regime defaulted to 'Unknown'")
        
        # Conditional validation: Only enforce hard failure if NOT using live Schwab snapshot
        # (Schwab snapshots lack Murphy indicators until Step 5)
        if use_live_snapshot:
            logger.info("ℹ️ Using live snapshot mode - Signal_Type and Regime will be enriched in Step 5")
        else:
            # Enforce "Fail Fast" for legacy snapshots that should have these fields
            if df['Signal_Type'].isna().any() or (df['Signal_Type'] == 'Unknown').any():
                logger.warning("⚠️ Signal_Type contains Unknown values - chart signals not yet computed")
            if df['Regime'].isna().any() or (df['Regime'] == 'Unknown').any():
                logger.warning("⚠️ Regime contains Unknown values - volatility regime not yet classified")
        
        # ====================
        # ENTRY QUALITY ENRICHMENT (NEW - Scan-Time Enhancements) - TEMPORARILY DISABLED
        # ====================
        # Add intraday range, 52W context, and momentum metrics for entry timing
        # try:
        #     from core.scan_engine.entry_quality_enhancements import enrich_snapshot_with_entry_quality
        #     df = enrich_snapshot_with_entry_quality(df)
        # except Exception as e:
        #     logger.warning(f"⚠️ Entry quality enrichment failed (non-critical): {e}")

        logger.info(f"✅ Step 2 complete: {len(df)} {required_id_col.lower()}s loaded and enriched (Sinclair + Murphy data added)")
        return df
        
    except FileNotFoundError:
        logger.error(f"❌ Snapshot file not found: {snapshot_path}")
        raise
    except Exception as e:
        logger.error(f"❌ Error loading snapshot: {e}")
        raise


def _calculate_iv_rank(current, iv_1w, iv_1m):
    """
    Calculate IV Rank: where current IV sits within recent range (0-100 scale).
    
    Uses 1-month lookback: min(current, iv_1w, iv_1m) to max(...).
    RAG says: "Study IV over most recent 6-month period for mean reversion."
    
    Note: Ideally needs 52-week IV history, but using 1-month as proxy.
    This is NOT true IV Rank (52-week percentile) - it's a recent-range indicator.
    """
    if pd.isna(current) or pd.isna(iv_1w) or pd.isna(iv_1m):
        return np.nan
    
    iv_values = [current, iv_1w, iv_1m]
    iv_min = min(iv_values)
    iv_max = max(iv_values)
    iv_range = iv_max - iv_min
    
    if iv_range == 0:
        return 50.0  # Flat IV, assign midpoint
    
    return 100 * (current - iv_min) / iv_range


def _classify_term_structure(iv7, iv30, iv90):
    """
    Classify IV term structure.
    
    RAG says: "When volatility is low, front month trades at lower IV than back months."
    - Contango: IV7 < IV30 < IV90 (normal, stable)
    - Inverted: IV7 > IV30 > IV90 (fear, event risk)
    - Flat: All within 10% of each other (transition)
    """
    if any(pd.isna(v) for v in [iv7, iv30, iv90]):
        return 'Unknown'
    
    # Check if all values within 10% range (flat)
    avg_iv = (iv7 + iv30 + iv90) / 3
    if all(abs(v - avg_iv) / avg_iv < 0.10 for v in [iv7, iv30, iv90]):
        return 'Flat'
    
    # Check contango (ascending)
    if iv7 < iv30 < iv90:
        return 'Contango'
    
    # Check inversion (descending)
    if iv7 > iv30 > iv90:
        return 'Inverted'
    
    return 'Mixed'


def _detect_trend(current, past, threshold=0.05):
    """
    Detect if metric is rising, falling, or stable.
    
    Args:
        current: Current value
        past: Historical value (1 week or 1 month ago)
        threshold: Percent change threshold to classify as rising/falling
    
    Returns:
        'Rising', 'Falling', or 'Stable'
    """
    if pd.isna(current) or pd.isna(past) or past == 0:
        return 'Unknown'
    
    pct_change = (current - past) / past
    
    if pct_change > threshold:
        return 'Rising'
    elif pct_change < -threshold:
        return 'Falling'
    else:
        return 'Stable'


# ====================
# SINCLAIR: VOLATILITY REGIME FUNCTIONS
# ====================

def _calculate_vvix(current, iv_1w, iv_1m):
    """
    Calculate VVIX (vol-of-vol): Volatility of implied volatility.
    
    RAG (Sinclair Ch.6): "Volatility clusters - high vol begets high vol.
    Vol-of-vol measures uncertainty about volatility itself."
    
    Approximation: Standard deviation of IV values over available lookback.
    Ideally needs 20-day IV history; using 3-point approximation (current, 1W, 1M).
    
    Returns:
        float: Standard deviation of IV (proxy for VVIX)
    """
    if pd.isna(current) or pd.isna(iv_1w) or pd.isna(iv_1m):
        return np.nan
    
    iv_series = [current, iv_1w, iv_1m]
    return np.std(iv_series)


def _detect_vol_spike(current, iv_1w, iv_1m):
    """
    Detect if IV spiked recently (volatility clustering risk).
    
    RAG (Sinclair Ch.5): "After a vol spike, mean reversion is likely.
    Don't buy vol immediately after a spike - wait for normalization."
    
    Logic:
        - Calculate mean and std from historical IV
        - If current > mean + 2*std → spike detected
    
    Returns:
        bool: True if recent spike detected
    """
    if pd.isna(current) or pd.isna(iv_1w) or pd.isna(iv_1m):
        return False
    
    iv_series = [iv_1w, iv_1m]  # Historical only (not current)
    mean_iv = np.mean(iv_series)
    std_iv = np.std(iv_series)
    
    if std_iv == 0:
        return False  # Flat IV, no spike
    
    # Spike if current > 2 standard deviations above historical mean
    return current > (mean_iv + 2 * std_iv)


def _classify_volatility_regime(iv_rank, iv_trend, vvix):
    """
    Classify volatility regime for long volatility strategy gating.
    
    RAG (Sinclair Ch.2-4):
    - "Low Vol regime: IV rank <30, stable/falling IV, low VVIX
       → Favorable for buying vol (cheap, likely to expand)"
    
    - "Compression regime: IV rank 30-50, stable IV, moderate VVIX
       → Neutral-to-favorable (coiled spring)"
    
    - "Expansion regime: IV rank 50-70, rising IV, elevated VVIX
       → Caution (vol already moving, late entry)"
    
    - "High Vol regime: IV rank >70, elevated VVIX
       → Avoid buying vol (expensive, mean reversion likely)"
    
    Args:
        iv_rank: IV_Rank_30D (0-100 percentile)
        iv_trend: 'Rising', 'Falling', 'Stable'
        vvix: Vol-of-vol (std of IV)
    
    Returns:
        str: 'Low Vol', 'Compression', 'Expansion', 'High Vol', or 'Unknown'
    """
    if pd.isna(iv_rank) or pd.isna(vvix):
        return 'Unknown'
    
    # Thresholds
    LOW_VOL_THRESHOLD = 30
    COMPRESSION_THRESHOLD = 50
    EXPANSION_THRESHOLD = 70
    HIGH_VVIX_THRESHOLD = 5.0  # Points (absolute volatility of IV)
    
    # High Vol: IV rank > 70 OR very high VVIX (uncertainty)
    if iv_rank > EXPANSION_THRESHOLD or vvix > HIGH_VVIX_THRESHOLD:
        return 'High Vol'
    
    # Low Vol: IV rank < 30, stable/falling IV, low VVIX
    if iv_rank < LOW_VOL_THRESHOLD:
        if iv_trend in ['Falling', 'Stable'] and vvix < 3.0:
            return 'Low Vol'
        else:
            return 'Compression'  # IV low but starting to move
    
    # Compression: IV rank 30-50, stable IV
    if iv_rank < COMPRESSION_THRESHOLD:
        if iv_trend == 'Stable':
            return 'Compression'
        elif iv_trend == 'Rising':
            return 'Expansion'  # Transitioning
        else:
            return 'Compression'  # Falling into compression
    
    # Expansion: IV rank 50-70, rising IV
    if iv_rank < EXPANSION_THRESHOLD:
        if iv_trend == 'Rising':
            return 'Expansion'
        else:
            return 'Compression'  # Peaked but not yet high
    
    return 'Unknown'


# ====================
# MURPHY: TREND & MOMENTUM FUNCTIONS
# ====================

def _calculate_murphy_indicators(id_val: str, id_col_name: str) -> dict:
    """
    Calculate Murphy technical indicators for trend and momentum analysis.
    
    RAG (Murphy Ch.4-6, Ch.10-11):
    - "Trend alignment: Price above SMA20/SMA50 = bullish structure"
    - "Momentum confirmation: ADX >25 = strong trend, RSI 40-60 = healthy"
    - "Volume precedes price: Rising volume confirms trend"
    
    Args:
        id_val (str): The stock symbol or ticker.
        id_col_name (str): The name of the identifier column ('Symbol' or 'Ticker').
    
    Returns:
        dict: Murphy indicators including:
            - id_col_name: Stock symbol/ticker
            - Trend_State: 'Bullish', 'Bearish', 'Neutral'
            - Price_vs_SMA20: % above/below SMA20 (positive = above)
            - Price_vs_SMA50: % above/below SMA50
            - Volume_Trend: 'Rising', 'Falling', 'Stable'
            - ADX: Trend strength (0-100, >25 = strong)
            - RSI: Momentum oscillator (0-100, 40-60 = healthy)
            - Trend_Strength: 'Strong', 'Moderate', 'Weak'
    """
    result = {
        id_col_name: id_val,
        'Trend_State': 'Unknown',
        'Price_vs_SMA20': np.nan,
        'Price_vs_SMA50': np.nan,
        'Volume_Trend': 'Unknown',
        'ADX': np.nan,
        'RSI': np.nan,
        'Trend_Strength': 'Unknown'
    }
    
    try:
        # Fetch 90 days of price data (sufficient for SMAs)
        df_price = yf.download(id_val, period='90d', interval='1d',
                               progress=False)
        
        # yfinance returns MultiIndex columns - flatten them
        if isinstance(df_price.columns, pd.MultiIndex):
            df_price.columns = [col[0] for col in df_price.columns]
        
        if df_price.empty or len(df_price) < 50:
            logger.debug(f"Insufficient price data for {id_val}")
            return result
        
        # Current price
        current_price = df_price['Close'].iloc[-1]
        
        # Calculate SMAs (Murphy Ch.4)
        sma20 = df_price['Close'].rolling(20).mean().iloc[-1]
        sma50 = df_price['Close'].rolling(50).mean().iloc[-1]
        
        if pd.notna(sma20):
            result['Price_vs_SMA20'] = ((current_price - sma20) / sma20) * 100
        if pd.notna(sma50):
            result['Price_vs_SMA50'] = ((current_price - sma50) / sma50) * 100
        
        # Calculate Realized Volatility (RV) and RV/IV Ratio
        # RAG: "Never buy volatility without confirming IV > RV. Statistical edge requires vol mispricing."
        # Audit: "CRITICAL Missing Data - RV/IV Ratio needed for vol edge validation"
        try:
            # 10-day realized volatility (annualized)
            returns_10d = df_price['Close'].pct_change().tail(10)
            if len(returns_10d) >= 10:
                rv_10d = returns_10d.std() * np.sqrt(252) * 100  # Annualized %
                result['RV_10D'] = rv_10d
                
                # RV/IV Ratio - key metric for vol edge
                # <0.90 = IV cheap (buy vol)
                # >1.15 = IV expensive (sell vol)
                # Note: This will be enriched with actual IV data in the merge step
                result['RV_Calculated'] = True
            else:
                result['RV_10D'] = np.nan
                result['RV_Calculated'] = False
        except Exception as e:
            logger.debug(f"RV calculation failed for {symbol}: {e}") # Changed 'ticker' to 'symbol'
            result['RV_10D'] = np.nan
            result['RV_Calculated'] = False
        
        # Trend State (Murphy Ch.4-5)
        if pd.notna(sma20) and pd.notna(sma50):
            if current_price > sma20 and current_price > sma50:
                result['Trend_State'] = 'Bullish'
            elif current_price < sma20 and current_price < sma50:
                result['Trend_State'] = 'Bearish'
            else:
                result['Trend_State'] = 'Neutral'
        
        # Volume Trend (Murphy Ch.7)
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
        
        # Technical indicators (Murphy Ch.10-11) - Manual calculation
        # RSI (Relative Strength Index)
        result['RSI'] = _calculate_rsi(df_price['Close'], period=14)
        
        # ADX (Average Directional Index)
        result['ADX'] = _calculate_adx(df_price, period=14)
        
        # Classify trend strength based on ADX
        adx_val = result['ADX']
        if pd.notna(adx_val):
            if adx_val > 25:
                result['Trend_Strength'] = 'Strong'
            elif adx_val > 15:
                result['Trend_Strength'] = 'Moderate'
            else:
                result['Trend_Strength'] = 'Weak'
        
    except Exception as e:
        logger.debug(f"Failed to fetch Murphy indicators for {symbol}: {e}") # Changed 'ticker' to 'symbol'
    
    return result


def _calculate_rsi(prices: pd.Series, period: int = 14) -> float:
    """
    Calculate RSI (Relative Strength Index) manually.
    
    RSI = 100 - (100 / (1 + RS))
    RS = Average Gain / Average Loss
    
    RAG (Murphy Ch.11): RSI 40-60 = healthy, >70 = overbought, <30 = oversold
    
    Args:
        prices: Series of closing prices
        period: RSI period (default 14)
    
    Returns:
        RSI value (0-100) or NaN if insufficient data
    """
    try:
        if len(prices) < period + 1:
            return np.nan
        
        # Calculate price changes
        delta = prices.diff()
        
        # Separate gains and losses
        gains = delta.where(delta > 0, 0.0)
        losses = -delta.where(delta < 0, 0.0)
        
        # Calculate average gain/loss using Wilder's smoothing
        avg_gain = gains.rolling(window=period, min_periods=period).mean().iloc[-1]
        avg_loss = losses.rolling(window=period, min_periods=period).mean().iloc[-1]
        
        if pd.isna(avg_gain) or pd.isna(avg_loss) or avg_loss == 0:
            return np.nan
        
        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))
        
        return rsi
    except Exception as e:
        logger.debug(f"RSI calculation failed: {e}")
        return np.nan


def _calculate_adx(df: pd.DataFrame, period: int = 14) -> float:
    """
    Calculate ADX (Average Directional Index) manually.
    
    ADX measures trend strength (0-100):
    - >25: Strong trend
    - 15-25: Moderate trend
    - <15: Weak/choppy
    
    RAG (Murphy Ch.10): ADX indicates trend strength, not direction
    
    Args:
        df: DataFrame with High, Low, Close columns
        period: ADX period (default 14)
    
    Returns:
        ADX value (0-100) or NaN if insufficient data
    """
    try:
        if len(df) < period * 3 or 'High' not in df.columns or 'Low' not in df.columns:
            return np.nan
        
        high = df['High'].values
        low = df['Low'].values
        close = df['Close'].values
        
        # Calculate True Range (TR)
        tr = np.zeros(len(df))
        for i in range(1, len(df)):
            hl = high[i] - low[i]
            hc = abs(high[i] - close[i-1])
            lc = abs(low[i] - close[i-1])
            tr[i] = max(hl, hc, lc)
        
        # Calculate Directional Movement (+DM, -DM)
        plus_dm = np.zeros(len(df))
        minus_dm = np.zeros(len(df))
        
        for i in range(1, len(df)):
            up_move = high[i] - high[i-1]
            down_move = low[i-1] - low[i]
            
            if up_move > down_move and up_move > 0:
                plus_dm[i] = up_move
            if down_move > up_move and down_move > 0:
                minus_dm[i] = down_move
        
        # Calculate smoothed ATR, +DI, -DI
        atr = pd.Series(tr).rolling(window=period, min_periods=period).mean()
        plus_di_smooth = pd.Series(plus_dm).rolling(window=period, min_periods=period).mean()
        minus_di_smooth = pd.Series(minus_dm).rolling(window=period, min_periods=period).mean()
        
        # Calculate +DI and -DI
        plus_di = 100 * (plus_di_smooth / atr)
        minus_di = 100 * (minus_di_smooth / atr)
        
        # Calculate DX (Directional Index)
        dx = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di)
        
        # Replace inf/nan in dx
        dx = dx.replace([np.inf, -np.inf], np.nan)
        
        # Calculate ADX (smoothed DX)
        adx_series = dx.rolling(window=period, min_periods=period).mean()
        
        # Get last valid ADX value
        adx = adx_series.iloc[-1]
        
        return adx if pd.notna(adx) else np.nan
    except Exception as e:
        logger.debug(f"ADX calculation failed: {e}")
        return np.nan
