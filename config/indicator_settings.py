# Default settings for TA-Lib indicators

RSI_SETTINGS = {
    "timeperiod": 14
}

ADX_SETTINGS = {
    "timeperiod": 14
}

SMA_SETTINGS = {
    "timeperiod_20": 20,
    "timeperiod_50": 50,
    "timeperiod_3": 3 # For drift smoothing if needed
}

EMA_SETTINGS = {
    "timeperiod_9": 9,
    "timeperiod_21": 21
}

ATR_SETTINGS = {
    "timeperiod": 14
}

MACD_SETTINGS = {
    "fastperiod": 12,
    "slowperiod": 26,
    "signalperiod": 9
}

BBANDS_SETTINGS = {
    "timeperiod": 20,
    "nbdevup": 2,
    "nbdevdn": 2,
    "matype": 0 # 0 for SMA
}

STOCH_SETTINGS = {
    "fastk_period": 5,
    "slowk_period": 3,
    "slowk_matype": 0, # 0 for SMA
    "slowd_period": 3,
    "slowd_matype": 0  # 0 for SMA
}

MARKET_STRESS_THRESHOLDS = {
    "ATR_LOW": 0.5,      # SPY ATR % below this is considered LOW stress
    "ATR_ELEVATED": 1.5, # SPY ATR % above this is considered ELEVATED stress
    "ATR_CRISIS": 2.5,   # SPY ATR % above this is considered CRISIS stress
    "VIX_ELEVATED": 25,  # VIX above this is considered ELEVATED stress
    "VIX_CRISIS": 35     # VIX above this is considered CRISIS stress
}

# Other configurable thresholds for chart signals
REGIME_CLASSIFICATION_THRESHOLDS = {
    "overextension_pct": 0.40,
    "atr_compressed_pct": 1.0,
    "trend_slope_strong": 2.0,
    "trend_slope_weak": 0.5,
    "price_near_sma_pct": 0.10
}
