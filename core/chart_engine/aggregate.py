import numpy as np
import pandas as pd

from .volume_engine import compute_volume_overlays
from .pattern_engine import detect_candlestick_patterns
from .trend_engine import get_chart_trend_state
from .momentum_engine import compute_momentum_indicators

# === Composite Chart Scoring ===
def score_chart_composite(metrics):
    score = 50
    if metrics.get("MACD_Cross"): score += 10
    if 45 < metrics.get("RSI", 50) < 60: score += 10
    if not metrics.get("Volume_Divergence"): score += 5
    if not metrics.get("Price_Overextended"): score += 5
    if not metrics.get("ATR_Spike"): score += 5
    if "Hammer" in metrics.get("Chart_Tags", []): score += 5
    if "ShootingStar" in metrics.get("Chart_Tags", []): score -= 10
    if "Doji" in metrics.get("Chart_Tags", []): score -= 7
    return max(0, min(100, score))

# === Signal Filter Logic ===
def apply_signal_filters(metrics):
    filters = {
        "Reject_Volume_Divergence": metrics.get("Volume_Divergence") is True and metrics.get("Chart_Trend") == "Sustained Bullish",
        "Reject_Overextended": metrics.get("Price_Overextended") is True and not metrics.get("MACD_Cross"),
        "Reject_ATR_Spike": metrics.get("ATR_Spike") is True and metrics.get("RSI", 50) > 70,
        "Reject_Doji": "Doji" in metrics.get("Chart_Tags", [])
    }
    filters["Reject_Triggered"] = any(filters.values())
    return filters

# === Main Aggregator ===
def aggregate_chart_tags(df):
    # Run all engines on a chart DataFrame (OHLCV)
    if df.shape[0] < 100:
        return {
            "Chart_Trend": "Unknown",
            "Chart_Score": 0,
            "Chart_Tags": [],
            "Exit_Flag": None,
            "Chart_CompositeScore": 0,
            "Reject_Triggered": True,
            "Reason": "Insufficient data (<100 candles)"
        }
    df = df.astype("float64", errors="ignore")  # Patch: Ensure dtype compatibility
    compute_volume_overlays(df)
    momentum = compute_momentum_indicators(df)
    candles, candle_data = detect_candlestick_patterns(df)
    trend_tag, chart_score, trend_data = get_chart_trend_state(df)
    tags = candles
    exit_flag = any(t.get('exit_flag') for t in tags)
    atr_spike = df['ATR'].iloc[-1] > 0.015 * df['Close'].iloc[-1]
    overextended = bool(
        (df['Close'].iloc[-1] > 1.4 * trend_data.get('SMA20', 0)) or
        (df['Close'].iloc[-1] > 1.4 * trend_data.get('SMA50', 0))
    )
    metrics = {
        "Chart_Score": chart_score,
        "Chart_Trend": trend_tag,
        "Chart_Tags": [t['tag'] for t in tags],
        "Exit_Flag": exit_flag,
        **trend_data,
        "OBV": df['OBV'].iloc[-1],
        "ATR": df['ATR'].iloc[-1],
        **candle_data,
        **momentum,
        "ATR_Spike": atr_spike,
        "Volume_Divergence": bool(df['Volume_Trend'].iloc[-1]),
        "Price_Overextended": overextended
    }
    metrics["Chart_CompositeScore"] = score_chart_composite(metrics)
    metrics.update(apply_signal_filters(metrics))
    # Patch: Clean inf/NaN in scoring fields
    for key in ["Chart_CompositeScore", "Chart_Score"]:
        if key in metrics:
            metrics[key] = 0 if pd.isna(metrics[key]) or np.isinf(metrics[key]) else metrics[key]
    if metrics.get("Chart_Trend") is None:
        metrics["Chart_Trend"] = "Unknown"
    return metrics

# === Batch Runner (on a master table CSV) ===
def run_phase8_chart_engine(master_path):
    df = pd.read_csv(master_path)
    results = []
    for _, row in df.iterrows():
        ticker = row.get("Underlying")
        trade_id = row.get("TradeID")
        if pd.notna(ticker):
            try:
                import yfinance as yf
                hist = yf.Ticker(ticker).history(start="2023-01-01", interval="1d").copy()
                hist.reset_index(inplace=True)
                verdict = aggregate_chart_tags(hist)
                verdict["TradeID"] = trade_id
                results.append(verdict)
            except Exception as e:
                print(f"⚠️ Failed to analyze {ticker}: {e}")
    df_chart = pd.DataFrame(results)
    if "TradeID" not in df_chart.columns:
        print("❌ Chart Engine failed: no valid results to merge.")
        return df
    # Drop any existing chart columns to avoid suffixes
    chart_cols = [
        "Chart_Trend", "Chart_Score", "Chart_Tags", "Exit_Flag", "Chart_CompositeScore",
        "Reject_Triggered", "Reject_ATR_Spike", "Reject_Doji", "Reject_Overextended", "Reject_Volume_Divergence",
        "RSI", "MACD_Line", "MACD_Signal", "MACD_Cross", "ATR", "ADX", "DMI+", "DMI-", "OBV", "MFI", "CCI",
        "EMA9", "EMA21", "SMA20", "SMA50", "BB_Upper", "BB_Mid", "BB_Lower",
        "Candle_Body", "Candle_UpperShadow", "Candle_LowerShadow", "Candle_FullRange",
        "Price_Overextended", "Volume_Divergence", "ATR_Spike"
    ]
    df = df.drop(columns=[c for c in chart_cols if c in df.columns], errors="ignore")
    df_updated = df.merge(df_chart, on="TradeID", how="left")
    df_updated.to_csv(master_path, index=False)
    print("✅ Phase 8: Chart Engine verdicts added to active_master.csv")
    return df_updated

# === Run on Only New Trades ===
def run_chart_verdict_on_new(df_flat):
    df_flat = df_flat.copy()
    if "IsNewTrade" not in df_flat.columns:
        print("⚠️ IsNewTrade not in dataframe — skipping chart verdicts")
        return df_flat
    df_new = df_flat[df_flat["IsNewTrade"] == True]
    if df_new.empty:
        print("✅ No new trades — skipping chart analysis")
        return df_flat
    for idx, row in df_new.iterrows():
        ticker = row.get("Ticker") or row.get("Underlying")
        if not ticker:
            continue
        try:
            import yfinance as yf
            hist = yf.Ticker(ticker).history(period="3mo", interval="1d").copy()
            if hist.empty:
                print(f"⚠️ No candle data for {ticker}")
                continue
            verdict = aggregate_chart_tags(hist)
            # Inject verdict into flat df
            df_flat.loc[idx, "ChartVerdict"] = verdict.get("Chart_Trend")
            df_flat.loc[idx, "BreakoutConfirmed"] = (
                verdict.get("Chart_Trend") == "Sustained Bullish"
                and not verdict.get("Reject_Triggered", False)
            )
            df_flat.loc[idx, "Overextended"] = verdict.get("Price_Overextended")
            df_flat.loc[idx, "SqueezeActive"] = verdict.get("Chart_Tags") and "Squeeze" in verdict.get("Chart_Tags")
        except Exception as e:
            print(f"❌ Chart error on {ticker}: {e}")
    return df_flat
