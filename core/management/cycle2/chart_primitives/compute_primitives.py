import pandas as pd
import numpy as np
import logging
import re
from typing import Optional, Dict, Any
from core.shared.data_layer.price_history_loader import load_price_history, ChartDataStatus

logger = logging.getLogger(__name__)

_PIPELINE_DB_PATH = "data/pipeline.duckdb"
_IV_HISTORY_DB_PATH = "data/iv_history.duckdb"


def _enrich_from_scan_engine(df: pd.DataFrame, tickers: list) -> pd.DataFrame:
    """
    Read real technical indicators from the scan engine's DuckDB table
    and overwrite stub values in df for tickers that have scan engine data.

    Overwrites: adx_14 (was hardcoded 25.0)
    Adds:       rsi_14, macd, macd_signal, slow_k_5_3, slow_d_5_3

    Architecture: scan engine is the producer; management engine is the consumer.
    Separation is preserved — no scan engine code is imported, only DuckDB read.
    """
    if not tickers:
        return df
    try:
        import duckdb
        con = duckdb.connect(_PIPELINE_DB_PATH, read_only=True)
        placeholders = ", ".join(f"'{t}'" for t in tickers)
        rows = con.execute(f"""
            SELECT Ticker, RSI_14, ADX_14, MACD, MACD_Signal, SlowK_5_3, SlowD_5_3
            FROM technical_indicators
            WHERE Ticker IN ({placeholders})
            QUALIFY ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY Snapshot_TS DESC) = 1
        """).df()
        con.close()
    except Exception as e:
        logger.warning(f"⚠️ scan engine enrichment skipped (non-fatal): {e}")
        return df

    if rows.empty:
        return df

    source_col = "Underlying_Ticker" if "Underlying_Ticker" in df.columns else "Ticker"

    # Ensure new columns exist
    for col in ("rsi_14", "macd", "macd_signal", "slow_k_5_3", "slow_d_5_3"):
        if col not in df.columns:
            df[col] = np.nan

    for _, row in rows.iterrows():
        ticker = row["Ticker"]
        mask = df[source_col] == ticker
        if not mask.any():
            continue

        adx = row.get("ADX_14")
        if pd.notna(adx):
            df.loc[mask, "adx_14"] = float(adx)

        for src, dst in [
            ("RSI_14",      "rsi_14"),
            ("MACD",        "macd"),
            ("MACD_Signal", "macd_signal"),
            ("SlowK_5_3",   "slow_k_5_3"),
            ("SlowD_5_3",   "slow_d_5_3"),
        ]:
            val = row.get(src)
            if pd.notna(val):
                df.loc[mask, dst] = float(val)

        logger.debug(f"scan-engine enrichment applied for {ticker}: ADX={adx:.1f}" if pd.notna(adx) else f"scan-engine enrichment: no ADX for {ticker}")

    enriched_count = rows["Ticker"].nunique()
    logger.info(f"✅ Scan-engine enrichment: {enriched_count}/{len(tickers)} tickers had real ADX/RSI/MACD data")
    return df


def _enrich_iv_term_structure(df: pd.DataFrame, tickers: list) -> pd.DataFrame:
    """
    Read IV term structure from iv_history.duckdb (primary) with pipeline.duckdb
    fidelity_iv_long_term_history as fallback. Adds:

      iv_surface_shape   : CONTANGO | BACKWARDATION | FLAT
      iv_ts_slope_30_90  : IV_90d - IV_30d (pts, positive = contango)
      iv_ts_slope_30_180 : IV_180d - IV_30d (pts, wider view)

    Doctrine use (Natenberg Ch.5/11):
      BACKWARDATION → near-term IV spike. Short premium: favorable (collecting inflated vol).
                       Long options: unfavorable unless thesis is strong (buying elevated near-term).
      CONTANGO      → normal/calm market. No term-structure edge for either side.
      Steep contango (slope > 5pts): LEAP / far-dated long options sitting in "cheap" end of curve.
    """
    if not tickers:
        return df

    source_col = "Underlying_Ticker" if "Underlying_Ticker" in df.columns else "Ticker"

    for col in ("iv_surface_shape", "iv_ts_slope_30_90", "iv_ts_slope_30_180"):
        if col not in df.columns:
            df[col] = np.nan
    if "iv_surface_shape" in df.columns:
        df["iv_surface_shape"] = df["iv_surface_shape"].astype(object)

    rows = pd.DataFrame()

    # Primary: iv_history.duckdb (REST collected, fresher)
    try:
        import duckdb
        placeholders = ", ".join(f"'{t}'" for t in tickers)
        con = duckdb.connect(_IV_HISTORY_DB_PATH, read_only=True)
        rows = con.execute(f"""
            SELECT ticker AS Ticker, iv_30d, iv_60d, iv_90d, iv_180d
            FROM iv_term_history
            WHERE ticker IN ({placeholders})
              AND iv_30d IS NOT NULL AND iv_90d IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) = 1
        """).df()
        con.close()
        if not rows.empty:
            logger.debug(f"IV term structure: {len(rows)} tickers from iv_history.duckdb")
    except Exception as e:
        logger.warning(f"⚠️ IV term structure (primary) skipped: {e}")

    # Fallback: fidelity_iv_long_term_history (call-side, wider tenor range)
    if rows.empty:
        try:
            con = duckdb.connect(_PIPELINE_DB_PATH, read_only=True)
            placeholders = ", ".join(f"'{t}'" for t in tickers)
            fid = con.execute(f"""
                SELECT Ticker AS Ticker,
                       IV_30_D_Call AS iv_30d,
                       IV_60_D_Call AS iv_60d,
                       IV_90_D_Call AS iv_90d,
                       IV_180_D_Call AS iv_180d
                FROM fidelity_iv_long_term_history
                WHERE Ticker IN ({placeholders})
                  AND IV_30_D_Call IS NOT NULL AND IV_90_D_Call IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY timestamp DESC) = 1
            """).df()
            con.close()
            rows = fid
            if not rows.empty:
                logger.debug(f"IV term structure: {len(rows)} tickers from fidelity fallback")
        except Exception as e:
            logger.warning(f"⚠️ IV term structure (fallback) skipped: {e}")

    if rows.empty:
        return df

    for _, row in rows.iterrows():
        ticker = row["Ticker"]
        mask = df[source_col] == ticker
        if not mask.any():
            continue

        iv_30 = float(row.get("iv_30d") or 0)
        iv_90 = float(row.get("iv_90d") or 0)
        iv_180 = float(row.get("iv_180d") or 0)

        if iv_30 <= 0 or iv_90 <= 0:
            continue

        slope_30_90 = iv_90 - iv_30
        slope_30_180 = (iv_180 - iv_30) if iv_180 > 0 else np.nan

        # Classification thresholds: ±1.5 pts is noise, beyond that is signal
        if slope_30_90 > 1.5:
            shape = "CONTANGO"
        elif slope_30_90 < -1.5:
            shape = "BACKWARDATION"
        else:
            shape = "FLAT"

        df.loc[mask, "iv_surface_shape"] = shape
        df.loc[mask, "iv_ts_slope_30_90"] = round(slope_30_90, 2)
        df.loc[mask, "iv_ts_slope_30_180"] = round(slope_30_180, 2) if not np.isnan(slope_30_180) else np.nan

    covered = rows["Ticker"].nunique()
    logger.info(f"✅ IV term structure: {covered}/{len(tickers)} tickers enriched")
    return df


def compute_chart_primitives(df: pd.DataFrame, client=None) -> pd.DataFrame:
    """
    Management-owned primitive computation layer.
    Fetches raw market data and computes technical primitives required for Cycle 2 Chart States.
    
    Strictly decoupled from scan_engine.
    """
    if df.empty:
        return df
        
    df = df.copy()
    
    # RAG: Fix — Filter symbols to underlyings only.
    option_pattern = re.compile(r'\d{6}[CP]\d+')
    source_col = 'Underlying_Ticker' if 'Underlying_Ticker' in df.columns else 'Ticker'
    
    tickers = [
        str(s) for s in df[source_col].unique() 
        if isinstance(s, str) and not option_pattern.search(s)
    ]

    # Hard Assertion: Ensure ONLY underlyings (no OCC symbols like AAPL260320C260)
    bad = [t for t in tickers if option_pattern.search(str(t))]
    if bad:
        logger.error(f"CHART PRIMITIVE TICKERS (RUNTIME): {tickers}")
        raise RuntimeError(f"OCC SYMBOLS IN CHART PRIMITIVES: {bad}")
    
    primitive_data = {}
    
    for ticker in tickers:
        try:
            # Fetch 180 days of history for robust lookbacks (ADX, SMA50, Swings)
            hist, source = load_price_history(ticker, days=180, client=client)
            
            mask = (df['Underlying_Ticker'] == ticker) if 'Underlying_Ticker' in df.columns else (df['Ticker'] == ticker)

            # RAG: Smart Resolution Tracking. Surface the specific reason for missing data.
            if source == ChartDataStatus.BLOCKED_RATE_LIMIT:
                df.loc[mask, 'Resolution_Reason'] = "DATA_SOURCE_BACKOFF_ACTIVE"
                continue
            elif source == ChartDataStatus.NO_HISTORY:
                df.loc[mask, 'Resolution_Reason'] = "NO_HISTORY_RETURNED"
                continue
            elif source == ChartDataStatus.FAILED:
                df.loc[mask, 'Resolution_Reason'] = "DATA_FETCH_FAILED"
                continue

            if hist is None or len(hist) < 50:
                logger.warning(f"Insufficient history for {ticker} ({len(hist) if hist is not None else 0} bars) Source: {source}")
                df.loc[mask, 'Resolution_Reason'] = f"INSUFFICIENT_HISTORY_{len(hist) if hist is not None else 0}"
                continue
                
            primitives = _calculate_primitives_for_ticker(hist)
            primitive_data[ticker] = primitives
            
        except Exception as e:
            logger.error(f"Failed to compute primitives for {ticker}: {e}")
            
    # Map primitives back to the main dataframe
    # RAG: Correct Mapping. Ensure all tickers are enriched, even if columns already exist.
    for ticker, data in primitive_data.items():
        mask = (df['Underlying_Ticker'] == ticker) if 'Underlying_Ticker' in df.columns else (df['Ticker'] == ticker)
        for col, value in data.items():
            if col not in df.columns:
                df[col] = np.nan
            df.loc[mask, col] = value

    # Enrich with real scan-engine indicators (ADX, RSI, MACD, Stochastic) from DuckDB.
    # This overwrites stub values (adx_14=25.0) for tickers covered by the scan engine.
    df = _enrich_from_scan_engine(df, tickers)

    # Enrich with IV term structure (contango/backwardation) from iv_history.duckdb.
    # Natenberg Ch.5/11: term structure shape gates entry favorability for long/short vol.
    df = _enrich_iv_term_structure(df, tickers)

    return df

def _calculate_primitives_for_ticker(hist: pd.DataFrame) -> Dict[str, Any]:
    """Internal math for primitive derivation."""
    c = hist['Close']
    h = hist['High']
    l = hist['Low']
    v = hist['Volume']
    
    # 1. Basic Indicators
    ema9  = c.ewm(span=9, adjust=False).mean()
    ema20 = c.ewm(span=20, adjust=False).mean()
    ema50 = c.ewm(span=50, adjust=False).mean()
    sma20 = c.rolling(window=20).mean()
    sma50 = c.rolling(window=50).mean()
    
    # 2. ATR & Volatility
    prev_c = c.shift(1)
    tr = pd.concat([h - l, (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    atr14 = tr.rolling(window=14).mean()
    
    std20 = c.rolling(window=20).std()
    bb_upper = sma20 + (std20 * 2)
    bb_lower = sma20 - (std20 * 2)
    bb_width = bb_upper - bb_lower
    bb_width_pct = bb_width / sma20
    
    # 3. Momentum & Slopes
    ema20_slope = (ema20.iloc[-1] - ema20.iloc[-5]) / 5
    ema50_slope = (ema50.iloc[-1] - ema50.iloc[-5]) / 5
    
    roc5 = ((c.iloc[-1] - c.iloc[-6]) / c.iloc[-6]) * 100 if c.iloc[-6] != 0 else 0
    roc10 = ((c.iloc[-1] - c.iloc[-11]) / c.iloc[-11]) * 100 if c.iloc[-11] != 0 else 0
    roc20 = ((c.iloc[-1] - c.iloc[-21]) / c.iloc[-21]) * 100 if c.iloc[-21] != 0 else 0
    
    # 4. Swing Structure
    # HH: High > previous 5 highs
    # LL: Low < previous 5 lows
    hh_mask = h > h.shift(1).rolling(5).max()
    ll_mask = l < l.shift(1).rolling(5).min()
    hl_mask = (l > l.shift(1).rolling(5).min()) & ll_mask.shift(5).rolling(10).max().astype(bool)
    lh_mask = (h < h.shift(1).rolling(5).max()) & hh_mask.shift(5).rolling(10).max().astype(bool)
    
    # 5. Efficiency & Choppiness
    net_move = abs(c.iloc[-1] - c.iloc[-11])
    sum_abs_diff = (c.diff().abs()).rolling(10).sum().iloc[-1]
    kaufman_er = net_move / sum_abs_diff if sum_abs_diff > 0 else 0

    # Real Choppiness Index (14-period): 100 * log10(sum_ATR14 / (H14_max - L14_min)) / log10(14)
    # Range: 0 (perfectly trending) to 100 (completely choppy). Threshold: <38.2 trending, >61.8 choppy.
    n_chop = 14
    atr_sum_14 = tr.tail(n_chop).sum()
    h14_max = h.tail(n_chop).max()
    l14_min = l.tail(n_chop).min()
    price_range_14 = h14_max - l14_min
    if price_range_14 > 0 and atr_sum_14 > 0:
        choppiness_index = 100.0 * np.log10(atr_sum_14 / price_range_14) / np.log10(n_chop)
        choppiness_index = float(np.clip(choppiness_index, 0, 100))
    else:
        choppiness_index = 50.0  # neutral fallback only when price data is degenerate

    # HV 20D percentile: position of current 20D HV within its 252-day rolling distribution
    log_ret = np.log(c / c.shift(1)).dropna()
    hv20_series = log_ret.rolling(20).std() * np.sqrt(252)
    hv20_current = hv20_series.iloc[-1]
    hv20_history = hv20_series.dropna().tail(252)
    hv_20d_percentile = float((hv20_history < hv20_current).mean()) if len(hv20_history) > 5 else 0.5

    # 6. Range Expansion
    recent_range = h.tail(10).max() - l.tail(10).min()
    prev_range = h.shift(10).tail(10).max() - l.shift(10).tail(10).min()
    range_expansion = recent_range / (atr14.iloc[-1] * 10) if atr14.iloc[-1] > 0 else 1.0

    # Weekly trend alignment: compare EMA20 slope sign vs EMA20 slope 5 bars ago
    # Positive agreement (both slopes same sign) → 1.0, disagreement → 0.0
    ema20_slope_prev = (ema20.iloc[-5] - ema20.iloc[-10]) / 5 if len(ema20) >= 10 else 0.0
    daily_weekly_align = 1.0 if (ema20_slope * ema20_slope_prev > 0) else 0.0

    # 7. Late-cycle / acceleration primitives (new — required for deterministic MomentumVelocityState)
    #
    # sma_distance_pct: how far price is above/below SMA20 as a fraction.
    #   >+15% = overextended upper; < -15% = oversold.  Key late-cycle threshold.
    sma20_last = sma20.iloc[-1]
    sma_distance_pct = float((c.iloc[-1] - sma20_last) / sma20_last) if sma20_last > 0 else 0.0

    # atr_slope: 5-bar change in ATR14, normalised by current ATR so it's dimensionless.
    #   Positive → volatility expanding (confirms acceleration).
    #   Negative → volatility contracting (late-cycle squeeze or trend ending).
    atr_slope = float(
        (atr14.iloc[-1] - atr14.iloc[-6]) / atr14.iloc[-6]
        if len(atr14) >= 6 and atr14.iloc[-6] > 0
        else 0.0
    )

    # rsi_slope: 5-bar change in RSI (computed from price series directly — not from DuckDB,
    #   which only gives a single point).  Used to detect bearish divergence:
    #   price_acceleration > 0 AND rsi_slope < 0  →  late-cycle warning.
    # RSI-14 from price series (Wilder smoothing via ewm):
    delta_c = c.diff()
    gain = delta_c.clip(lower=0)
    loss = (-delta_c).clip(lower=0)
    avg_gain = gain.ewm(com=13, adjust=False).mean()
    avg_loss = loss.ewm(com=13, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi_series = 100 - (100 / (1 + rs))
    rsi_slope = float(
        rsi_series.iloc[-1] - rsi_series.iloc[-6]
        if len(rsi_series) >= 6
        else 0.0
    )

    return {
        "ema20_slope": ema20_slope,
        "ema50_slope": ema50_slope,
        "ema_alignment_score": 1.0 if ema20.iloc[-1] > ema50.iloc[-1] else 0.0,
        "adx_14": 25.0,  # placeholder — overwritten by scan-engine enrichment when available
        "price_dist_to_ema_atr": (c.iloc[-1] - ema20.iloc[-1]) / atr14.iloc[-1] if atr14.iloc[-1] > 0 else 0,
        "hv_20d_percentile": hv_20d_percentile,
        "atr_14": atr14.iloc[-1],
        "bb_width_pct": bb_width_pct.iloc[-1],
        "bb_width_z": (bb_width_pct.iloc[-1] - bb_width_pct.tail(20).mean()) / bb_width_pct.tail(20).std() if bb_width_pct.tail(20).std() > 0 else 0,
        "swing_hh_count": int(hh_mask.tail(20).sum()),
        "swing_hl_count": int(hl_mask.tail(20).sum()),
        "swing_lh_count": int(lh_mask.tail(20).sum()),
        "swing_ll_count": int(ll_mask.tail(20).sum()),
        "break_of_structure": bool((c.iloc[-1] > h.shift(1).rolling(20).max().iloc[-1]) or (c.iloc[-1] < l.shift(1).rolling(20).min().iloc[-1])),
        "atr_normalized_range_expansion": float(range_expansion),
        "close_location_in_structure": (c.iloc[-1] - l.tail(20).min()) / (h.tail(20).max() - l.tail(20).min()) if h.tail(20).max() != l.tail(20).min() else 0.5,
        "kaufman_efficiency_ratio": float(kaufman_er),
        "choppiness_index": choppiness_index,
        "net_movement_total_movement_ratio": float(net_move / sum_abs_diff) if sum_abs_diff > 0 else 0.5,
        "roc_5": float(roc5),
        "roc_10": float(roc10),
        "roc_20": float(roc20),
        "momentum_slope": float(ema20_slope),
        "price_acceleration": float(roc5 - roc10),
        "up_volume_down_volume_ratio": float(v[c > prev_c].tail(10).sum() / v[c < prev_c].tail(10).sum()) if v[c < prev_c].tail(10).sum() > 0 else 1.0,
        "close_position_daily_range": float((c.iloc[-1] - l.iloc[-1]) / (h.iloc[-1] - l.iloc[-1])) if h.iloc[-1] != l.iloc[-1] else 0.5,
        "vwap_deviation": 0.0,
        "delta_pressure_proxy": 0.0,
        "consecutive_low_bb_width_bars": int((bb_width_pct < bb_width_pct.rolling(50).mean()).tail(10).sum()),
        "bb_width_slope": float(bb_width_pct.iloc[-1] - bb_width_pct.iloc[-5]),
        "range_contraction_ratio": float(recent_range / prev_range) if prev_range > 0 else 1.0,
        "inside_bar_count": int(((h < h.shift(1)) & (l > l.shift(1))).tail(10).sum()),
        "daily_weekly_trend_alignment": daily_weekly_align,
        "daily_intraday_trend_alignment": 1.0,  # intraday data not available
        "compression_alignment_score": 1.0,      # multi-timeframe BB not available
        # Late-cycle / acceleration primitives
        "sma_distance_pct": sma_distance_pct,
        "atr_slope": atr_slope,
        "rsi_slope": rsi_slope,
        # RSI-14 value (fallback when DuckDB technical_indicators missing)
        "rsi_14": float(rsi_series.iloc[-1]) if len(rsi_series) > 0 and not np.isnan(rsi_series.iloc[-1]) else np.nan,
        # Raw price levels for scale-up pullback anchors (McMillan Ch.4)
        "EMA9": float(ema9.iloc[-1]),
        "SMA20": float(sma20.iloc[-1]) if not np.isnan(sma20.iloc[-1]) else 0.0,
        "SMA50": float(sma50.iloc[-1]) if not np.isnan(sma50.iloc[-1]) else 0.0,
        "LowerBand_20": float(bb_lower.iloc[-1]) if not np.isnan(bb_lower.iloc[-1]) else 0.0,
        "UpperBand_20": float(bb_upper.iloc[-1]) if not np.isnan(bb_upper.iloc[-1]) else 0.0,
    }
