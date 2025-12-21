# chart_engine.py
# 📈 Chart Pattern Engine – Full TA Logic Restored (No Placeholders)

import pandas as pd
import numpy as np
import yfinance as yf

try:
    import pandas_ta as ta
    TA_LIB_ENABLED = True
except ImportError:
    TA_LIB_ENABLED = False

# === 🔊 Volume Overlay Computation ===
def compute_volume_overlays(df):
    df["Volume"] = df["Volume"].fillna(0)  # ⛑ Prevent crash in OBV
    df['OBV'] = (np.sign(df['Close'].diff()) * df['Volume']).fillna(0).cumsum()
    df['ATR'] = df['High'].rolling(14).max() - df['Low'].rolling(14).min()
    df['Volume_Trend'] = df['Volume'].rolling(3).mean().iloc[-1] < df['Volume'].rolling(10).mean().iloc[-1]
    return df

# === 🕯️ Candlestick Pattern Detection ===
def detect_candlestick_patterns(df):
    patterns = []
    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else last

    body = abs(last['Close'] - last['Open'])
    upper_shadow = last['High'] - max(last['Close'], last['Open'])
    lower_shadow = min(last['Close'], last['Open']) - last['Low']
    full_range = last['High'] - last['Low']

    if upper_shadow > 2 * body and lower_shadow < 0.1 * body and last['Close'] < last['Open']:
        patterns.append({"tag": "ShootingStar", "confidence": 0.9, "exit_flag": True})
    if abs(last['Open'] - last['Close']) <= 0.05 * full_range:
        patterns.append({"tag": "Doji", "confidence": 0.8, "exit_flag": True})
    if lower_shadow > 2 * body and upper_shadow < 0.1 * body and last['Close'] > last['Open']:
        patterns.append({"tag": "Hammer", "confidence": 0.85, "exit_flag": False})
    if last['Open'] < last['Close'] < prev['Open'] and last['Open'] > prev['Close']:
        patterns.append({"tag": "BullishEngulfing", "confidence": 0.8, "exit_flag": False})
    if last['Open'] > last['Close'] > prev['Open'] and last['Close'] < prev['Close']:
        patterns.append({"tag": "BearishEngulfing", "confidence": 0.8, "exit_flag": True})

    return patterns, {
        "Candle_Body": body,
        "Candle_UpperShadow": upper_shadow,
        "Candle_LowerShadow": lower_shadow,
        "Candle_FullRange": full_range
    }

# === Trend Structure ===
def get_chart_trend_state(df):
    ema9 = df['Close'].ewm(span=9).mean()
    ema21 = df['Close'].ewm(span=21).mean()
    sma20 = df['Close'].rolling(20).mean()
    sma50 = df['Close'].rolling(50).mean()

    trend_data = {
        "EMA9": ema9.iloc[-1],
        "EMA21": ema21.iloc[-1],
        "SMA20": sma20.iloc[-1],
        "SMA50": sma50.iloc[-1]
    }

    overextended = df['Close'].iloc[-1] > 1.4 * sma20.iloc[-1] or df['Close'].iloc[-1] > 1.4 * sma50.iloc[-1]
    if ema9.iloc[-1] > ema21.iloc[-1]:
        trend_tag = 'Sustained Bullish' if not overextended else 'Overextended'
        score = 0.9
    elif ema9.iloc[-1] < ema21.iloc[-1]:
        trend_tag = 'Downtrend'
        score = 0.9
    else:
        trend_tag = 'Neutral'
        score = 0.5

    return trend_tag, score, trend_data

# === Momentum Indicators (MACD, RSI, BB, CCI, ADX, MFI) ===
def compute_momentum_indicators(df):
    result = {}
    if TA_LIB_ENABLED:
        result['RSI'] = df.ta.rsi(length=14).iloc[-1]
        macd = df.ta.macd(fast=12, slow=26, signal=9)
        result['MACD_Line'] = macd['MACD_12_26_9'].iloc[-1]
        result['MACD_Signal'] = macd['MACDs_12_26_9'].iloc[-1]
        result['MACD_Cross'] = result['MACD_Line'] > result['MACD_Signal']
        bb = df.ta.bbands(length=20)
        result['BB_Upper'] = bb['BBU_20_2.0'].iloc[-1]
        result['BB_Lower'] = bb['BBL_20_2.0'].iloc[-1]
        result['BB_Mid'] = bb['BBM_20_2.0'].iloc[-1]
        result['CCI'] = df.ta.cci().iloc[-1]
        adx = df.ta.adx()
        result['ADX'] = adx['ADX_14'].iloc[-1]
        result['DMI+'] = adx['DMP_14'].iloc[-1]
        result['DMI-'] = adx['DMN_14'].iloc[-1]
        result['MFI'] = df.ta.mfi().iloc[-1]
    else:
        result['RSI'] = result['MACD_Line'] = result['MACD_Signal'] = result['MACD_Cross'] = np.nan
        result['BB_Upper'] = result['BB_Lower'] = result['BB_Mid'] = result['CCI'] = result['ADX'] = result['DMI+'] = result['DMI-'] = result['MFI'] = np.nan
    return result

# === Composite Score ===
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

# === Signal Filters ===
def apply_signal_filters(metrics):
    filters = {
        "Reject_Volume_Divergence": metrics.get("Volume_Divergence") is True and metrics.get("Chart_Trend") == "Sustained Bullish",
        "Reject_Overextended": metrics.get("Price_Overextended") is True and not metrics.get("MACD_Cross"),
        "Reject_ATR_Spike": metrics.get("ATR_Spike") is True and metrics.get("RSI", 50) > 70,
        "Reject_Doji": "Doji" in metrics.get("Chart_Tags", [])
    }
    filters["Reject_Triggered"] = any(filters.values())
    return filters

# === Chart Aggregator ===
def aggregate_chart_tags(df):
    if df.shape[0] < 100:
        print(f"⚠️ Insufficient candles for TA indicators: only {df.shape[0]} rows")
        return {
            "Chart_Trend": "Unknown",
            "Chart_Score": 0,
            "Chart_Tags": [],
            "Exit_Flag": None,
            "Chart_CompositeScore": 0,
            "Reject_Triggered": True,
            "Reason": "Insufficient data (<100 candles)"
        }

    df = df.astype("float64", errors="ignore")  # ✅ Patch: Ensure dtype compatibility for TA
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

    # ✅ Patch: Clean inf/NaN in scoring fields
    for key in ["Chart_CompositeScore", "Chart_Score"]:
        if key in metrics:
            metrics[key] = 0 if pd.isna(metrics[key]) or np.isinf(metrics[key]) else metrics[key]

    if metrics.get("Chart_Trend") is None:
        metrics["Chart_Trend"] = "Unknown"

    return metrics

# === 🛰️ Batch Phase 8: Add Chart Verdicts to Master CSV ===
def run_phase8_chart_engine(master_path="/Users/haniabadi/Documents/Windows/Optionrec/active_master.csv"):
    df = pd.read_csv(master_path)
    results = []

    for _, row in df.iterrows():
        ticker = row.get("Underlying")
        trade_id = row.get("TradeID")
        if pd.notna(ticker):
            try:
                hist = yf.Ticker(ticker).history(start="2023-01-01", interval="1d").copy()

                hist.reset_index(inplace=True)
                hist.rename(columns={
                    'Open': 'Open', 'High': 'High', 'Low': 'Low', 'Close': 'Close', 'Volume': 'Volume'
                }, inplace=True)
                verdict = aggregate_chart_tags(hist)
                verdict["TradeID"] = trade_id
                results.append(verdict)
            except Exception as e:
                print(f"⚠️ Failed to analyze {ticker}: {e}")

    df_chart = pd.DataFrame(results)
    if "TradeID" not in df_chart.columns:
        print("❌ Chart Engine failed: no valid results to merge.")
        return df

# === ✅ Drop existing chart-related columns before merge to avoid suffix conflict
    chart_cols = [
        "Chart_Trend", "Chart_Score", "Chart_Tags", "Exit_Flag", "Chart_CompositeScore",
        "Reject_Triggered", "Reject_ATR_Spike", "Reject_Doji", "Reject_Overextended", "Reject_Volume_Divergence",
        "RSI", "MACD_Line", "MACD_Signal", "MACD_Cross", "ATR", "ADX", "DMI+", "DMI-", "OBV", "MFI", "CCI",
        "EMA9", "EMA21", "SMA20", "SMA50", "BB_Upper", "BB_Mid", "BB_Lower",
        "Candle_Body", "Candle_UpperShadow", "Candle_LowerShadow", "Candle_FullRange",
        "Price_Overextended", "Volume_Divergence", "ATR_Spike"
    ]
    df = df.drop(columns=[c for c in chart_cols if c in df.columns], errors="ignore")

# === Merge new chart results
    
    df_updated = df.merge(df_chart, on="TradeID", how="left")
    df_updated.to_csv(master_path, index=False)
    print("✅ Phase 8: Chart Engine verdicts added to active_master.csv")

    return df_updated


# === 🧠 Run Chart Engine Only for New Trades
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
