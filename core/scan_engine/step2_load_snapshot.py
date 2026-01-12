"""
Step 2: Load IV/HV Snapshot from Fidelity Export
"""

import pandas as pd
import numpy as np
import logging
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Tuple, List, Dict, Optional
from .price_history_loader import load_price_history

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
    """
    snapshot_path = Path(snapshot_dir)
    
    if not snapshot_path.exists():
        raise FileNotFoundError(
            f"❌ Snapshot directory not found: {snapshot_path}\n"
            f"   Run Step 0 first: python core/scan_engine/step0_schwab_snapshot.py"
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
            f"   Run Step 0 first: python core/scan_engine/step0_schwab_snapshot.py"
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
        age_hours = (datetime.now() - datetime.fromtimestamp(latest.stat().st_mtime)).total_seconds() / 3600
    
    logger.info(f"✅ Found latest live snapshot: {latest.name} (age: {age_hours:.1f}h)")
    return str(latest.resolve())


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
        snapshot_path = os.getenv('FIDELITY_SNAPSHOT_PATH', 
                                   '/Users/haniabadi/Documents/Windows/OptionsSnapshots/fidelity_ivhv_snapshot.csv')
    
    snapshot_path = Path(snapshot_path)
    df = pd.read_csv(snapshot_path)
    logger.info(f"✅ Loaded snapshot: {snapshot_path} ({df.shape[0]} rows)")

    # 🧪 TEST-ONLY INGRESS RESTRICTION (Phase 0 Hard Gate)
    # This restriction is applied at the earliest possible point in the ingress.
    # The pipeline remains unmodified and unaware of this restriction.
    TEST_TICKERS = ["AAPL", "AMZN", "NVDA"]
    
    # Identify ID column for filtering
    id_col_temp = 'Symbol' if 'Symbol' in df.columns else 'Ticker'
    if id_col_temp in df.columns:
        logger.info("🧪" + "!"*50)
        logger.info(f"🧪 TEST MODE ACTIVE: Restricting ingress to {TEST_TICKERS}")
        logger.info("🧪" + "!"*50)
        
        df = df[df[id_col_temp].isin(TEST_TICKERS)].copy()
        logger.info(f"🧪 Ingress restricted to {len(df)} test tickers.")

    # 1. Freshness check
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        snapshot_time = df['timestamp'].iloc[0]
        
        # Deterministic Age: Use filename timestamp as reference to avoid runtime variance in ledger
        def _extract_ts(p):
            try:
                ts_str = p.stem.replace("ivhv_snapshot_live_", "").replace("snapshot_", "")
                return datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
            except:
                return None
        
        ref_now = _extract_ts(snapshot_path) or datetime.fromtimestamp(snapshot_path.stat().st_mtime)
        age_hours = (ref_now - snapshot_time).total_seconds() / 3600
        
        df['Snapshot_Age_Hours'] = age_hours
        if age_hours > max_age_hours:
            logger.warning(f"⚠️ Snapshot is {age_hours:.1f} hours old (threshold: {max_age_hours}h).")
    
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
    """
    # Rehydrate IV Surface
    try:
        from core.data_layer.ivhv_timeseries_loader import load_latest_iv_surface
        snapshot_ts = df['snapshot_ts'].iloc[0] if 'snapshot_ts' in df.columns else (df['timestamp'].iloc[0] if 'timestamp' in df.columns else datetime.now())
        df = load_latest_iv_surface(df, pd.to_datetime(snapshot_ts))
        logger.info("✅ IV surface rehydrated")
    except Exception as e:
        logger.warning(f"⚠️ IV surface rehydration failed: {e}")

    # IV Rank
    if 'iv_rank_252d' in df.columns:
        df['IV_Rank_30D'] = df['iv_rank_252d']
    elif 'IV_30_D_Call_1M' in df.columns:
        df['IV_Rank_30D'] = df.apply(lambda r: _calculate_iv_rank(r.get('IV_30_D_Call'), r.get('IV_30_D_Call_1W'), r.get('IV_30_D_Call_1M')), axis=1)

    # Term Structure & Trends
    if all(col in df.columns for col in ['IV_7_D_Call', 'IV_30_D_Call', 'IV_90_D_Call']):
        df['IV_Term_Structure'] = df.apply(lambda r: _classify_term_structure(r.get('IV_7_D_Call'), r.get('IV_30_D_Call'), r.get('IV_90_D_Call')), axis=1)
    else:
        df['IV_Term_Structure'] = 'Unknown'
    
    # Ensure trend columns exist for Step 3 validation
    if 'IV_30_D_Call_1W' in df.columns:
        df['IV_Trend_7D'] = df.apply(lambda r: _detect_trend(r.get('IV_30_D_Call'), r.get('IV_30_D_Call_1W')), axis=1)
    else:
        df['IV_Trend_7D'] = 'Unknown'
    
    if all(col in df.columns for col in ['HV_10_D_Cur', 'HV_30_D_Cur']):
        df['HV_Trend_30D'] = df.apply(lambda r: _detect_trend(r.get('HV_10_D_Cur'), r.get('HV_30_D_Cur')), axis=1)
    else:
        df['HV_Trend_30D'] = 'Unknown'

    # Sinclair Regimes
    if all(col in df.columns for col in ['IV_30_D_Call', 'IV_30_D_Call_1W', 'IV_30_D_Call_1M']):
        df['VVIX'] = df.apply(lambda r: _calculate_vvix(r.get('IV_30_D_Call'), r.get('IV_30_D_Call_1W'), r.get('IV_30_D_Call_1M')), axis=1)
        df['Recent_Vol_Spike'] = df.apply(lambda r: _detect_vol_spike(r.get('IV_30_D_Call'), r.get('IV_30_D_Call_1W'), r.get('IV_30_D_Call_1M')), axis=1)
    else:
        df['VVIX'] = np.nan
        df['Recent_Vol_Spike'] = False
    
    if 'IV_Rank_30D' in df.columns:
        df['Volatility_Regime'] = df.apply(lambda r: _classify_volatility_regime(r.get('IV_Rank_30D'), r.get('IV_Trend_7D', 'Stable'), r.get('VVIX', 0)), axis=1)
        df['Regime'] = df['Volatility_Regime']
        df['IV_Rank_Source'] = 'LOCAL'
    else:
        df['Volatility_Regime'] = 'Unknown'
        df['Regime'] = 'Unknown'
        df['IV_Rank_Source'] = 'NEUTRAL'

    # FIX 1: Introduce IV_Maturity_State
    # This concept distinguishes between "bad data" and "early data".
    if 'iv_history_days' in df.columns:
        def _get_iv_maturity(days):
            if pd.isna(days) or days < 30: return "IMMATURE"
            if days < 120: return "PARTIAL"
            return "MATURE"
        df['IV_Maturity_State'] = df['iv_history_days'].apply(_get_iv_maturity)
    else:
        df['IV_Maturity_State'] = "IMMATURE"
        
    return df


def enrich_technical_indicators(df: pd.DataFrame, id_col: str, skip_patterns: bool = False) -> pd.DataFrame:
    """
    Step 2C: Murphy indicators and Bulkowski patterns (Parallelized).
    """
    from .throttled_executor import ThrottledExecutor
    
    # Murphy Indicators
    logger.info("📊 Enriching with Murphy technical indicators (parallelized)...")
    with ThrottledExecutor(max_workers=10, requests_per_second=5) as executor:
        murphy_results = executor.map_parallel(
            lambda ticker: _calculate_murphy_indicators(ticker, id_col),
            df[id_col].tolist(),
            desc="Murphy Indicators"
        )
    df = df.merge(pd.DataFrame(murphy_results), on=id_col, how='left')

    # RV/IV Ratio
    if 'RV_10D' in df.columns and 'IV_30_D_Call' in df.columns:
        df['RV_IV_Ratio'] = np.where((df['RV_10D'].notna()) & (df['IV_30_D_Call'] > 0), df['RV_10D'] / df['IV_30_D_Call'], np.nan)

    # Bulkowski Patterns
    if not skip_patterns:
        logger.info("📊 Detecting Bulkowski chart patterns (parallelized)...")
        from utils.pattern_detection import detect_bulkowski_patterns, detect_nison_candlestick
        
        def _process_patterns(ticker):
            try:
                p, conf = detect_bulkowski_patterns(ticker)
                c, timing = detect_nison_candlestick(ticker)
                return {id_col: ticker, 'Chart_Pattern': p, 'Pattern_Confidence': conf, 'Candlestick_Pattern': c, 'Entry_Timing_Quality': timing, 'Reversal_Confirmation': (timing == 'Strong')}
            except:
                return {id_col: ticker, 'Chart_Pattern': None, 'Pattern_Confidence': 0.0, 'Candlestick_Pattern': None, 'Entry_Timing_Quality': None, 'Reversal_Confirmation': False}

        with ThrottledExecutor(max_workers=10, requests_per_second=10) as executor:
            pattern_results = executor.map_parallel(_process_patterns, df[id_col].tolist(), desc="Pattern Detection")
        df = df.merge(pd.DataFrame(pattern_results), on=id_col, how='left')

    # Signal Type Mapping
    if 'Trend_State' in df.columns:
        df['Signal_Type'] = df['Trend_State'].map({'Bullish': 'Bullish', 'Bearish': 'Bearish', 'Neutral': 'Bidirectional'}).fillna('Unknown')
    
    return df


def enrich_market_context(df: pd.DataFrame, id_col: str) -> pd.DataFrame:
    """
    Step 2D: Entry quality and Earnings proximity.
    """
    # Entry Quality
    try:
        from core.scan_engine.entry_quality_enhancements import enrich_snapshot_with_entry_quality
        df = enrich_snapshot_with_entry_quality(df)
    except Exception as e:
        logger.warning(f"⚠️ Entry quality enrichment failed: {e}")

    # Earnings Proximity
    try:
        from core.data_layer.earnings_calendar import add_earnings_proximity
        snapshot_ts = df['snapshot_ts'].iloc[0] if 'snapshot_ts' in df.columns else datetime.now()
        df = add_earnings_proximity(df, pd.to_datetime(snapshot_ts))
    except Exception as e:
        logger.warning(f"⚠️ Earnings proximity failed: {e}")
        df['earnings_proximity_flag'] = False

    return df


def load_ivhv_snapshot(
    snapshot_path: str = None, 
    max_age_hours: int = 48, 
    skip_pattern_detection: bool = False,
    use_live_snapshot: bool = False
) -> pd.DataFrame:
    """
    Modularized Step 2 Orchestrator.
    """
    try:
        # 2A: Load
        df, id_col = load_raw_snapshot(snapshot_path, max_age_hours, use_live_snapshot)
        
        # 2B: Volatility
        df = enrich_volatility_metrics(df, id_col)
        
        # 2C: Technical
        df = enrich_technical_indicators(df, id_col, skip_pattern_detection)
        
        # 2D: Context
        df = enrich_market_context(df, id_col)
        
        # Step 7 Compatibility
        if 'IV_30_D_Call' in df.columns and 'HV_30_D_Cur' in df.columns:
            df['IVHV_gap_30D'] = df['IV_30_D_Call'] - df['HV_30_D_Cur']
            
        logger.info(f"✅ Step 2 complete: {len(df)} tickers loaded and enriched")
        return df
        
    except Exception as e:
        logger.error(f"❌ Step 2 failed: {e}", exc_info=True)
        raise


def _calculate_iv_rank(current, iv_1w, iv_1m):
    """
    Calculate IV Rank: where current IV sits within recent range (0-100 scale).
    """
    if pd.isna(current) or pd.isna(iv_1w) or pd.isna(iv_1m):
        return np.nan
    
    iv_values = [current, iv_1w, iv_1m]
    iv_min = min(iv_values)
    iv_max = max(iv_values)
    iv_range = iv_max - iv_min
    
    if iv_range == 0:
        return 50.0
    
    return 100 * (current - iv_min) / iv_range


def _classify_term_structure(iv7, iv30, iv90):
    """
    Classify IV term structure.
    """
    if any(pd.isna(v) for v in [iv7, iv30, iv90]):
        return 'Unknown'
    
    avg_iv = (iv7 + iv30 + iv90) / 3
    if all(abs(v - avg_iv) / avg_iv < 0.10 for v in [iv7, iv30, iv90]):
        return 'Flat'
    
    if iv7 < iv30 < iv90:
        return 'Contango'
    
    if iv7 > iv30 > iv90:
        return 'Inverted'
    
    return 'Mixed'


def _detect_trend(current, past, threshold=0.05):
    """
    Detect if metric is rising, falling, or stable.
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


def _calculate_vvix(current, iv_1w, iv_1m):
    """
    Calculate VVIX (vol-of-vol) proxy.
    """
    if pd.isna(current) or pd.isna(iv_1w) or pd.isna(iv_1m):
        return np.nan
    
    iv_series = [current, iv_1w, iv_1m]
    return np.std(iv_series)


def _detect_vol_spike(current, iv_1w, iv_1m):
    """
    Detect if IV spiked recently.
    """
    if pd.isna(current) or pd.isna(iv_1w) or pd.isna(iv_1m):
        return False
    
    iv_series = [iv_1w, iv_1m]
    mean_iv = np.mean(iv_series)
    std_iv = np.std(iv_series)
    
    if std_iv == 0:
        return False
    
    return current > (mean_iv + 2 * std_iv)


def _classify_volatility_regime(iv_rank, iv_trend, vvix):
    """
    Classify volatility regime.
    """
    if pd.isna(iv_rank) or pd.isna(vvix):
        return 'Unknown'
    
    LOW_VOL_THRESHOLD = 30
    COMPRESSION_THRESHOLD = 50
    EXPANSION_THRESHOLD = 70
    HIGH_VVIX_THRESHOLD = 5.0
    
    if iv_rank > EXPANSION_THRESHOLD or vvix > HIGH_VVIX_THRESHOLD:
        return 'High Vol'
    
    if iv_rank < LOW_VOL_THRESHOLD:
        if iv_trend in ['Falling', 'Stable'] and vvix < 3.0:
            return 'Low Vol'
        else:
            return 'Compression'
    
    if iv_rank < COMPRESSION_THRESHOLD:
        if iv_trend == 'Stable':
            return 'Compression'
        elif iv_trend == 'Rising':
            return 'Expansion'
        else:
            return 'Compression'
    
    if iv_rank < EXPANSION_THRESHOLD:
        if iv_trend == 'Rising':
            return 'Expansion'
        else:
            return 'Compression'
    
    return 'Unknown'


def _calculate_murphy_indicators(id_val: str, id_col_name: str) -> dict:
    """
    Calculate Murphy technical indicators.
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
        # Use unified loader (no client here, will use cache or yfinance)
        df_price, source = load_price_history(id_val, days=90)
        
        if df_price is None or df_price.empty or len(df_price) < 30:
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
        except:
            result['RV_10D'] = np.nan
            result['RV_Calculated'] = False
        
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
        
        result['RSI'] = _calculate_rsi(df_price['Close'], period=14)
        result['ADX'] = _calculate_adx(df_price, period=14)
        
        adx_val = result['ADX']
        if pd.notna(adx_val):
            if adx_val > 25:
                result['Trend_Strength'] = 'Strong'
            elif adx_val > 15:
                result['Trend_Strength'] = 'Moderate'
            else:
                result['Trend_Strength'] = 'Weak'
        
    except:
        pass
    
    return result


def _calculate_rsi(prices: pd.Series, period: int = 14) -> float:
    """
    Calculate RSI manually.
    """
    try:
        if len(prices) < period + 1:
            return np.nan
        
        delta = prices.diff()
        gains = delta.where(delta > 0, 0.0)
        losses = -delta.where(delta < 0, 0.0)
        
        avg_gain = gains.rolling(window=period, min_periods=period).mean().iloc[-1]
        avg_loss = losses.rolling(window=period, min_periods=period).mean().iloc[-1]
        
        if pd.isna(avg_gain) or pd.isna(avg_loss) or avg_loss == 0:
            return np.nan
        
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))
    except:
        return np.nan


def _calculate_adx(df: pd.DataFrame, period: int = 14) -> float:
    """
    Calculate ADX manually.
    """
    try:
        if len(df) < period * 3 or 'High' not in df.columns or 'Low' not in df.columns:
            return np.nan
        
        high = df['High'].values
        low = df['Low'].values
        close = df['Close'].values
        
        tr = np.zeros(len(df))
        for i in range(1, len(df)):
            hl = high[i] - low[i]
            hc = abs(high[i] - close[i-1])
            lc = abs(low[i] - close[i-1])
            tr[i] = max(hl, hc, lc)
        
        plus_dm = np.zeros(len(df))
        minus_dm = np.zeros(len(df))
        
        for i in range(1, len(df)):
            up_move = high[i] - high[i-1]
            down_move = low[i-1] - low[i]
            
            if up_move > down_move and up_move > 0:
                plus_dm[i] = up_move
            if down_move > up_move and down_move > 0:
                minus_dm[i] = down_move
        
        atr = pd.Series(tr).rolling(window=period, min_periods=period).mean()
        plus_di_smooth = pd.Series(plus_dm).rolling(window=period, min_periods=period).mean()
        minus_di_smooth = pd.Series(minus_dm).rolling(window=period, min_periods=period).mean()
        
        plus_di = 100 * (plus_di_smooth / atr)
        minus_di = 100 * (minus_di_smooth / atr)
        
        dx = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di)
        dx = dx.replace([np.inf, -np.inf], np.nan)
        
        adx_series = dx.rolling(window=period, min_periods=period).mean()
        adx = adx_series.iloc[-1]
        
        return adx if pd.notna(adx) else np.nan
    except:
        return np.nan
