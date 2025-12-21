import numpy as np
import pandas as pd

try:
    import pandas_ta as ta
    TA_LIB_ENABLED = True
except ImportError:
    TA_LIB_ENABLED = False

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
