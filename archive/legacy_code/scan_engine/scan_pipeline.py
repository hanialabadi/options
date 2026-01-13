"""
Scan Pipeline - Modular functions for GEM/PCS market scanning
Integrates Steps 1-14 from the scan notebook into reusable functions
"""

import pandas as pd
import numpy as np
import logging
import os
from pathlib import Path
from datetime import datetime
import yfinance as yf
import time

logger = logging.getLogger(__name__)

# ==========================================
# Input Validation Helper
# ==========================================
def validate_input(df: pd.DataFrame, required_cols: list, step_name: str) -> bool:
    """Validate input DataFrame before processing."""
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        logger.error(f"❌ {step_name}: Missing columns {missing}")
        raise ValueError(f"Cannot proceed: {missing}")
    if df.empty:
        logger.warning(f"⚠️ {step_name}: Input is empty")
        return False
    logger.info(f"✅ {step_name}: {len(df)} rows, all required columns present")
    return True


# ==========================================
# Step 2: Load IV/HV Snapshot
# ==========================================
def load_ivhv_snapshot(snapshot_path: str = None) -> pd.DataFrame:
    """
    Load Fidelity IV/HV snapshot from CSV.
    
    Purpose:
        Load pre-saved implied volatility (IV) and historical volatility (HV) data
        from Fidelity's export tool. This snapshot includes IV30, HV30, IV Rank,
        and is the base dataset for GEM filtering.
    
    Args:
        snapshot_path (str, optional): Path to IV/HV CSV file. 
            If None, uses FIDELITY_SNAPSHOT_PATH env var.
    
    Returns:
        pd.DataFrame: Raw snapshot with columns:
            - Ticker: Stock symbol
            - IV_30_D_Call: 30-day implied volatility for calls
            - HV_30_D_Cur: 30-day historical volatility
            - IV_7_D_Call: 7-day IV (short-term)
            - IV_90_D_Call: 90-day IV (long-term)
            - Additional IV/HV metrics
    
    Raises:
        FileNotFoundError: If snapshot file doesn't exist
        Exception: For other read errors (encoding, format, etc.)
    
    Example:
        >>> df = load_ivhv_snapshot('/path/to/snapshot.csv')
        >>> print(df.columns)
        ['Ticker', 'IV_30_D_Call', 'HV_30_D_Cur', ...]
    """
    if snapshot_path is None:
        snapshot_path = os.getenv('FIDELITY_SNAPSHOT_PATH', 
                                   '/Users/haniabadi/Documents/Windows/OptionsSnapshots/fidelity_ivhv_snapshot.csv')
    
    snapshot_path = Path(snapshot_path)
    
    try:
        df = pd.read_csv(snapshot_path)
        logger.info(f"✅ Loaded snapshot: {snapshot_path} ({df.shape[0]} rows, {df.shape[1]} cols)")
        return df
    except FileNotFoundError:
        logger.error(f"❌ Snapshot file not found: {snapshot_path}")
        raise
    except Exception as e:
        logger.error(f"❌ Error loading snapshot: {e}")
        raise


# ==========================================
# Step 3: IVHV Gap Filtering & Persona Tagging
# ==========================================
def filter_ivhv_gap(df: pd.DataFrame, min_gap: float = 2.0) -> pd.DataFrame:
    """
    Filter tickers by IV-HV gap and add persona tags (GEM/PSC classification).
    
    Purpose:
        Identifies tickers with volatility edge where IV significantly exceeds HV.
        This edge is critical for premium selling strategies (CSP, CC, strangles).
        Applies liquidity filters and normalizes IV Rank for comparison.
    
    Logic Flow:
        1. Convert IV/HV columns to numeric (handle NaNs)
        2. Filter: IV30_Call >= 15 AND HV30 > 0 (liquidity baseline)
        3. Calculate IVHV_gap_30D = IV30_Call - HV30
        4. Normalize IV_Rank_XS (0-100 scale) across all tickers
        5. Filter: IVHV_gap_30D >= min_gap
        6. Deduplicate: Keep highest gap per ticker
        7. Tag personas based on gap magnitude
    
    Persona Tags:
        - HardPass: IVHV gap >= 5.0 (strong edge, directional bias)
        - SoftPass: IVHV gap 3.5-5.0 (moderate edge, GEM candidate)
        - PSC_Pass: IVHV gap 2.0-3.5 (neutral/income strategies)
        - LowRank: IV_Rank < 30 (low relative IV, caution)
    
    Args:
        df (pd.DataFrame): Input snapshot with IV/HV columns
        min_gap (float, optional): Minimum IVHV gap threshold. Default 2.0.
    
    Returns:
        pd.DataFrame: Filtered tickers with added columns:
            - IV30_Call: Numeric IV (30-day calls)
            - HV30: Numeric HV (30-day)
            - IVHV_gap_30D: IV minus HV (the edge)
            - IV_Rank_XS: Normalized IV percentile (0-100)
            - HardPass, SoftPass, PSC_Pass, LowRank: Boolean flags
            - df_gem, df_psc: Boolean split indicators
    
    Raises:
        ValueError: If required columns missing
        
    Example:
        >>> df_filtered = filter_ivhv_gap(df_snapshot, min_gap=3.5)
        >>> print(f"GEM candidates: {df_filtered['df_gem'].sum()}")
        >>> print(df_filtered[['Ticker', 'IVHV_gap_30D', 'HardPass']].head())
    
    Notes:
        - Empty result warns but doesn't raise (allows pipeline continuation)
        - Handles flat IV surface (all IVs identical) gracefully
        - Deduplication ensures 1 row per ticker
    """
    # Convert IV/HV to numeric
    df['IV30_Call'] = pd.to_numeric(df['IV_30_D_Call'], errors='coerce')
    df['HV30'] = pd.to_numeric(df['HV_30_D_Cur'], errors='coerce')
    
    # Basic liquidity filter
    initial_count = len(df)
    df = df[(df['IV30_Call'] >= 15) & (df['HV30'] > 0)]
    logger.info(f"📊 Liquidity filter: {initial_count} → {len(df)} rows (IV ≥ 15, HV > 0)")
    
    # Calculate IV–HV gap
    df['IVHV_gap_30D'] = df['IV30_Call'] - df['HV30']
    
    # Normalize IV Rank (0–100) with safety check
    iv_min = df['IV30_Call'].min()
    iv_max = df['IV30_Call'].max()
    iv_range = iv_max - iv_min
    
    if iv_range == 0:
        logger.warning("⚠️ All IV values identical. Setting IV_Rank to 50.")
        df['IV_Rank_XS'] = 50.0
    else:
        df['IV_Rank_XS'] = 100 * (df['IV30_Call'] - iv_min) / iv_range
    
    # Filter for minimum edge
    df_filtered = df[df['IVHV_gap_30D'] >= min_gap].copy()
    
    if df_filtered.empty:
        logger.warning(f"⚠️ No tickers passed IVHV gap ≥ {min_gap} filter.")
        return df_filtered
    
    # Enforce one row per ticker (highest IVHV gap)
    df_filtered = df_filtered.sort_values(by='IVHV_gap_30D', ascending=False)
    df_filtered = df_filtered.drop_duplicates(subset='Ticker', keep='first')
    logger.info(f"✅ After dedup: {len(df_filtered)} unique tickers")
    
    # Persona tags
    df_filtered['HardPass'] = df_filtered['IVHV_gap_30D'] >= 5
    df_filtered['SoftPass'] = (df_filtered['IVHV_gap_30D'] >= 3.5) & (df_filtered['IVHV_gap_30D'] < 5)
    df_filtered['PSC_Pass'] = (df_filtered['IVHV_gap_30D'] >= 2.0) & (df_filtered['IVHV_gap_30D'] < 3.5)
    df_filtered['LowRank'] = df_filtered['IV_Rank_XS'] < 30
    
    # Separate DataFrames
    df_filtered['df_gem'] = df_filtered['IVHV_gap_30D'] >= 3.5
    df_filtered['df_psc'] = df_filtered['PSC_Pass']
    
    logger.info(f"✅ IVHV filtering complete: {len(df_filtered)} tickers qualified")
    
    return df_filtered


# ==========================================
# Step 5: Chart Scoring & Regime Classification
# ==========================================
def classify_regime(row: dict) -> str:
    """Classify market environment based on price structure."""
    trend_slope = row.get('Trend_Slope', 0)
    atr_pct = row.get('Atr_Pct')
    price_vs_sma20 = row.get('Price_vs_SMA20', 0)
    sma20 = row.get('SMA20', 1)
    
    if pd.isna(sma20) or sma20 == 0:
        return "Neutral"
    
    overextension_pct = abs(price_vs_sma20) / sma20
    
    if overextension_pct > 0.40:
        return "Overextended"
    elif atr_pct is not None and atr_pct < 1.0:
        return "Compressed"
    elif abs(trend_slope) > 2.0:
        return "Trending"
    elif abs(trend_slope) < 0.5 and overextension_pct < 0.10:
        return "Ranging"
    else:
        return "Neutral"


def compute_chart_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute technical indicators: EMA crossovers, ATR breakout, trend slope, and market regime.
    
    Purpose:
        Adds momentum and structure signals to qualify tickers for directional vs neutral strategies.
        Fetches 90-day price history from yfinance and computes moving averages, volatility,
        and regime classification. Critical for timing entries (fresh crossovers = Tier 1 GEMs).
    
    Logic Flow (per ticker):
        1. Fetch 90-day price history via yfinance
        2. Calculate ATR (14-period Average True Range)
        3. Compute moving averages: EMA9, EMA21, SMA20, SMA50
        4. Detect EMA9/EMA21 crossovers and days since last cross
        5. Calculate trend slope (EMA9 delta over 5 days)
        6. Measure price distance from SMAs (overextension check)
        7. Classify market regime (Trending, Ranging, Compressed, Overextended, Neutral)
    
    ATR Filter:
        - Rejects directional plays if ATR < 1.0% (insufficient volatility)
        - Allows neutral strategies (straddles/PSC) regardless of ATR
    
    Regime Classification:
        - Overextended: Price >40% above SMA20 (caution)
        - Compressed: ATR < 1.0% (base building, potential breakout)
        - Trending: Trend slope > 2.0 (strong momentum)
        - Ranging: Trend slope < 0.5, price near SMAs (neutral)
        - Neutral: Default (mixed signals)
    
    Args:
        df (pd.DataFrame): Input with at least ['Ticker', 'IVHV_gap_30D']
            Optional: ['Best_Strategy', 'Mapped_Strategy', 'Vega']
    
    Returns:
        pd.DataFrame: Original data merged with chart metrics:
            - Regime: Market environment classification
            - EMA_Signal: "Bullish" or "Bearish" (EMA9 vs EMA21)
            - Signal_Type: Crossover type or structure ("Bullish", "Base", "Neutral")
            - Days_Since_Cross: Days since last EMA crossover (NaN if none)
            - Has_Crossover: Boolean, whether crossover detected
            - Trend_Slope: EMA9 5-day delta (momentum)
            - Price_vs_SMA20, Price_vs_SMA50: Distance from moving averages
            - SMA20, SMA50: Moving average values
            - Atr_Pct: ATR as % of price (volatility measure)
            - Early_Breakout: Boolean placeholder (future enhancement)
    
    Rate Limiting:
        - Sleeps 0.5s every 10 tickers to avoid yfinance throttling
        - For 100+ tickers, consider adding caching (see TODO #3)
    
    Error Handling:
        - Skips tickers with <30 days data (logs warning)
        - Catches yfinance errors per ticker (doesn't fail entire batch)
        - Returns empty DataFrame if all tickers fail
    
    Raises:
        ValueError: If required input columns missing
    
    Example:
        >>> df_charted = compute_chart_signals(df_filtered)
        >>> print(df_charted[['Ticker', 'Regime', 'Signal_Type', 'Atr_Pct']].head())
        >>> trending = df_charted[df_charted['Regime'] == 'Trending']
    
    Performance:
        - ~1 second per ticker (yfinance fetch + calculations)
        - 50 tickers ≈ 55 seconds with rate limiting
        - Consider parallel processing for >200 tickers (future enhancement)
    """
    validate_input(df, ['Ticker', 'IVHV_gap_30D'], 'Step 5')
    
    # Ensure required columns exist
    if 'Best_Strategy' not in df.columns:
        logger.warning("⚠️ 'Best_Strategy' not found. Creating default...")
        df['Best_Strategy'] = 'Neutral'
    
    if 'Mapped_Strategy' not in df.columns:
        logger.warning("⚠️ 'Mapped_Strategy' not found. Creating default...")
        df['Mapped_Strategy'] = df.get('Strategy', 'Neutral')
    
    if 'Vega' not in df.columns:
        logger.warning("⚠️ 'Vega' column not found. Setting default = 0.0")
        df['Vega'] = 0.0
    
    chart_results = []
    skipped_count = 0
    rejected_count = 0
    
    for idx, (_, row) in enumerate(df.iterrows()):
        ticker = row['Ticker']
        
        # Rate limiting
        if idx > 0 and idx % 10 == 0:
            time.sleep(0.5)
        
        try:
            hist = yf.Ticker(ticker).history(period="90d")
            close_prices = hist['Close']
            
            if len(close_prices) < 30:
                logger.warning(f"[SKIP] {ticker}: insufficient data ({len(close_prices)} days)")
                skipped_count += 1
                continue
            
            # ATR Calculation
            high = hist['High']
            low = hist['Low']
            prev_close = hist['Close'].shift(1)
            tr = pd.concat([
                high - low,
                (high - prev_close).abs(),
                (low - prev_close).abs()
            ], axis=1).max(axis=1)
            atr = tr.rolling(window=14).mean()
            atr_pct = atr.iloc[-1] / close_prices.iloc[-1] if close_prices.iloc[-1] != 0 else np.nan
            atr_value = round(atr_pct * 100, 2) if not pd.isna(atr_pct) else np.nan
            
            # Moving Averages
            ema9 = close_prices.ewm(span=9, adjust=False).mean()
            ema21 = close_prices.ewm(span=21, adjust=False).mean()
            sma20 = close_prices.rolling(window=20).mean()
            sma50 = close_prices.rolling(window=50).mean()
            
            # Trend Slope
            trend_slope = round(ema9.iloc[-1] - ema9.iloc[-5], 4) if len(ema9) >= 5 else np.nan
            
            # Price vs SMA
            price_vs_sma20 = close_prices.iloc[-1] - sma20.iloc[-1] if not np.isnan(sma20.iloc[-1]) else np.nan
            price_vs_sma50 = close_prices.iloc[-1] - sma50.iloc[-1] if not np.isnan(sma50.iloc[-1]) else np.nan
            
            # EMA Signal
            ema_signal = "Bullish" if ema9.iloc[-1] > ema21.iloc[-1] else "Bearish"
            
            # Crossover detection
            signal_series = (ema9 > ema21).astype(int)
            cross_diff = signal_series.diff()
            cross_dates = cross_diff[cross_diff != 0].index
            
            latest_date = close_prices.index[-1]
            valid_cross_dates = [d for d in cross_dates if d <= latest_date]
            
            if len(valid_cross_dates) > 0:
                last_cross = valid_cross_dates[-1]
                days_since_cross = (latest_date - last_cross).days
                cross_type = "Bullish" if ema9[last_cross] > ema21[last_cross] else "Bearish"
                has_crossover = True
            else:
                days_since_cross = float('nan')
                cross_type = "None"
                has_crossover = False
            
            # Regime classification
            regime = classify_regime({
                'Trend_Slope': trend_slope,
                'Atr_Pct': atr_value,
                'Price_vs_SMA20': price_vs_sma20,
                'SMA20': sma20.iloc[-1] if not np.isnan(sma20.iloc[-1]) else np.nan
            })
            
            chart_results.append({
                "Ticker": ticker,
                "Regime": regime,
                "EMA_Signal": ema_signal,
                "Signal_Type": cross_type,
                "Days_Since_Cross": days_since_cross,
                "Has_Crossover": has_crossover,
                "Trend_Slope": trend_slope,
                "Price_vs_SMA20": round(price_vs_sma20, 2),
                "Price_vs_SMA50": round(price_vs_sma50, 2),
                "SMA20": round(sma20.iloc[-1], 2) if not np.isnan(sma20.iloc[-1]) else np.nan,
                "SMA50": round(sma50.iloc[-1], 2) if not np.isnan(sma50.iloc[-1]) else np.nan,
                "Atr_Pct": atr_value,
                "Early_Breakout": False  # Placeholder
            })
            
        except Exception as e:
            logger.error(f"[ERROR] {ticker}: {type(e).__name__}: {str(e)}")
            skipped_count += 1
    
    if not chart_results:
        logger.error("❌ No chart results generated")
        return pd.DataFrame()
    
    chart_df = pd.DataFrame(chart_results)
    chart_df = chart_df.drop_duplicates(subset="Ticker", keep="last")
    logger.info(f"✅ Chart processing: {len(chart_df)} tickers ({skipped_count} skipped, {rejected_count} rejected)")
    
    # Merge with original data
    df_charted = pd.merge(df, chart_df, on="Ticker", how="inner")
    logger.info(f"✅ Merge complete: {len(df_charted)} rows")
    
    return df_charted


# ==========================================
# Step 6: GEM Candidate Filtering
# ==========================================
def filter_gem_candidates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply final GEM (Growth-Edge-Momentum) filtering with quality gates.
    
    Purpose:
        Narrows charted tickers to high-probability trade candidates by enforcing:
        - Strong IV-HV edge (>=3.5)
        - Reasonable price extension (<25% from SMAs)
        - Valid signal structure (crossovers or neutral setups)
        Produces actionable watchlist with PCS (Probability-Convexity-Structure) seed scores.
    
    Logic Flow:
        1. Validate input columns (Ticker, IVHV_gap_30D, Signal_Type)
        2. Apply is_valid_candidate filter:
           - Directional: IVHV >= 3.5, extension < 25% from SMA20/50
           - Neutral: IVHV >= 3.5, signal in [Base, Neutral, Sustained Bullish]
        3. Filter allowed signal types (exclude overextended/bearish)
        4. Add derived fields:
           - Trend_Direction: Map EMA_Signal to Uptrend/Downtrend/Neutral
           - Scan_Tier: Classify by crossover freshness (Tier 1/2 or Trend_Hold)
           - PCS_Seed: Preliminary score (68-75) based on setup quality
    
    Validation Gates:
        - Directional pass: Strong edge + modest extension
        - Neutral pass: Strong edge + neutral structure (allows straddles/PSC)
        - Rejects: Weak edge, extreme overextension, no structure
    
    Scan Tiers:
        - GEM_Tier_1: Fresh crossover (<=5 days) - highest priority
        - GEM_Tier_2: Recent crossover (6-15 days) - good entry window
        - Trend_Hold: Older trend (>15 days) - momentum play
        - No_Crossover: Neutral/base setup - volatility play
    
    PCS Seed Logic:
        - 75: Fresh breakout (Days_Since_Cross <= 5)
        - 72: Recent breakout or Vega-supported
        - 70: Base/Neutral setup (range-bound)
        - 68: Older trend (fallback)
    
    Args:
        df (pd.DataFrame): Input with chart metrics from compute_chart_signals()
            Required: ['Ticker', 'IVHV_gap_30D', 'Signal_Type', 
                      'Price_vs_SMA20', 'Price_vs_SMA50', 'SMA20', 'SMA50',
                      'EMA_Signal', 'Days_Since_Cross']
    
    Returns:
        pd.DataFrame: Filtered GEM candidates with added columns:
            - Trend_Direction: 'Uptrend', 'Downtrend', or 'Neutral'
            - Scan_Tier: Entry timing classification
            - PCS_Seed: Preliminary quality score (68-75)
            Empty DataFrame if no candidates pass
    
    Raises:
        ValueError: If required columns missing
    
    Example:
        >>> gem = filter_gem_candidates(df_charted)
        >>> tier1 = gem[gem['Scan_Tier'] == 'GEM_Tier_1']
        >>> print(f"Tier 1 entries: {len(tier1)}")
        >>> print(gem[['Ticker', 'Scan_Tier', 'PCS_Seed']].head(10))
    
    Usage Notes:
        - Empty result is valid (strict filters by design)
        - Combine with Step 9+ for full option chain analysis
        - PCS_Seed is preliminary; refined in later scoring steps
    
    Alignment with Personas:
        - GEM (Growth-Edge-Momentum): Directional plays, fresh crossovers
        - PSC (Premium-Structure-Convexity): Neutral plays, volatility edge
        - Both require IVHV >= 3.5; differ in signal structure requirements
    """
    validate_input(df, ['Ticker', 'IVHV_gap_30D', 'Signal_Type'], 'Step 6')
    
    def is_valid_candidate(row):
        """Filter based on IV-HV edge and price extension."""
        ivhv = row.get('IVHV_gap_30D', 0)
        signal = row.get('Signal_Type', "")
        sma20_pct = abs(row.get('Price_vs_SMA20', 0)) / abs(row.get('SMA20', 1e-9))
        sma50_pct = abs(row.get('Price_vs_SMA50', 0)) / abs(row.get('SMA50', 1e-9))
        
        # Directional pass
        directional_pass = (
            ivhv >= 3.5 and 
            sma20_pct < 0.25 and 
            sma50_pct < 0.25
        )
        
        # Neutral pass
        neutral_pass = (
            ivhv >= 3.5 and 
            signal in ["Base", "Neutral", "Sustained Bullish", "Overextended but Vega Valid"]
        )
        
        return directional_pass or neutral_pass
    
    pre_filter_count = len(df)
    gem_candidates = df[df.apply(is_valid_candidate, axis=1)].copy()
    logger.info(f"🔍 Pre-filter: {pre_filter_count} → {len(gem_candidates)} after validity check")
    
    if gem_candidates.empty:
        logger.warning("⚠️ No GEM candidates passed filters")
        return gem_candidates
    
    # Allowed signal types
    allowed_signals = ["Bullish", "Sustained Bullish", "Base", "Neutral", "Overextended but Vega Valid"]
    gem_candidates = gem_candidates[gem_candidates['Signal_Type'].isin(allowed_signals)]
    logger.info(f"✅ {len(gem_candidates)} GEM candidates qualified")
    
    # Add trend direction
    gem_candidates['Trend_Direction'] = gem_candidates['EMA_Signal'].map({
        'Bullish': 'Uptrend',
        'Bearish': 'Downtrend'
    }).fillna('Neutral')
    
    # Scan tier assignment
    def scan_tier(x):
        if pd.isna(x):
            return "No_Crossover"
        if x <= 5:
            return "GEM_Tier_1"
        elif x <= 15:
            return "GEM_Tier_2"
        else:
            return "Trend_Hold"
    
    gem_candidates['Scan_Tier'] = gem_candidates['Days_Since_Cross'].apply(scan_tier)
    
    # PCS seed score
    def seed_score(row):
        if row['Signal_Type'] in ['Base', 'Neutral']:
            return 70
        elif row['Signal_Type'] in ['Sustained Bullish', 'Overextended but Vega Valid']:
            return 72
        elif pd.notna(row['Days_Since_Cross']) and row['Days_Since_Cross'] <= 5:
            return 75
        elif pd.notna(row['Days_Since_Cross']) and row['Days_Since_Cross'] <= 15:
            return 72
        else:
            return 68
    
    gem_candidates['PCS_Seed'] = gem_candidates.apply(seed_score, axis=1)
    
    return gem_candidates


# ==========================================
# Full Pipeline Runner
# ==========================================
def run_full_scan_pipeline(snapshot_path: str = None, output_dir: str = None) -> dict:
    """
    Run the complete scan pipeline from Step 2 to Step 6.
    
    Args:
        snapshot_path: Path to IV/HV snapshot CSV
        output_dir: Directory to save outputs
        
    Returns:
        Dictionary with all intermediate DataFrames
    """
    if output_dir is None:
        output_dir = Path(os.getenv('OUTPUT_DIR', './output'))
    else:
        output_dir = Path(output_dir)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    results = {}
    
    # Step 2: Load snapshot
    logger.info("📊 Step 2: Loading IV/HV snapshot...")
    df_snapshot = load_ivhv_snapshot(snapshot_path)
    results['snapshot'] = df_snapshot
    
    # Step 3: Filter by IVHV gap
    logger.info("📊 Step 3: Filtering by IVHV gap...")
    df_filtered = filter_ivhv_gap(df_snapshot)
    results['filtered'] = df_filtered
    
    if df_filtered.empty:
        logger.warning("⚠️ No tickers passed Step 3. Pipeline stopped.")
        return results
    
    # Step 5: Chart scoring
    logger.info("📊 Step 5: Computing chart signals...")
    df_charted = compute_chart_signals(df_filtered)
    results['charted'] = df_charted
    
    if df_charted.empty:
        logger.warning("⚠️ No tickers passed Step 5. Pipeline stopped.")
        return results
    
    # Step 6: GEM filtering
    logger.info("📊 Step 6: Filtering GEM candidates...")
    gem_candidates = filter_gem_candidates(df_charted)
    results['gem_candidates'] = gem_candidates
    
    # Export results
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        df_filtered.to_csv(output_dir / f"Step3_Filtered_{timestamp}.csv", index=False)
        df_charted.to_csv(output_dir / f"Step5_Charted_{timestamp}.csv", index=False)
        gem_candidates.to_csv(output_dir / f"Step6_GEM_{timestamp}.csv", index=False)
        logger.info(f"✅ Exports complete → {output_dir}")
    except Exception as e:
        logger.error(f"❌ Export failed: {e}")
    
    return results
