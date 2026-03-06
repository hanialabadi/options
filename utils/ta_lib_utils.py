import talib as TA_Lib_C
import pandas as pd
import numpy as np
import logging
import config.indicator_settings as indicator_settings # Import as module

# Access settings directly from the imported module
RSI_SETTINGS = indicator_settings.RSI_SETTINGS
ADX_SETTINGS = indicator_settings.ADX_SETTINGS
SMA_SETTINGS = indicator_settings.SMA_SETTINGS
EMA_SETTINGS = indicator_settings.EMA_SETTINGS
ATR_SETTINGS = indicator_settings.ATR_SETTINGS
MACD_SETTINGS = indicator_settings.MACD_SETTINGS
BBANDS_SETTINGS = indicator_settings.BBANDS_SETTINGS # Added
STOCH_SETTINGS = indicator_settings.STOCH_SETTINGS # Added

logger = logging.getLogger(__name__)

def calculate_rsi(series: pd.Series, **kwargs) -> pd.Series:
    """
    Calculates Relative Strength Index (RSI) using TA-Lib.
    Returns a Pandas Series with NaN for initial periods.
    """
    timeperiod = kwargs.get("timeperiod", RSI_SETTINGS["timeperiod"])
    if len(series) < timeperiod:
        return pd.Series(np.nan, index=series.index)
    return pd.Series(TA_Lib_C.RSI(series.values, timeperiod=timeperiod), index=series.index)

def calculate_adx(high: pd.Series, low: pd.Series, close: pd.Series, **kwargs) -> pd.Series:
    """
    Calculates Average Directional Movement Index (ADX) using TA-Lib.
    Returns a Pandas Series with NaN for initial periods.
    """
    timeperiod = kwargs.get("timeperiod", ADX_SETTINGS["timeperiod"])
    if len(high) < timeperiod * 2 or len(low) < timeperiod * 2 or len(close) < timeperiod * 2:
        return pd.Series(np.nan, index=high.index)
    return pd.Series(TA_Lib_C.ADX(high.values, low.values, close.values, timeperiod=timeperiod), index=high.index)

def calculate_sma(series: pd.Series, timeperiod: int = None, **kwargs) -> pd.Series:
    """
    Calculates Simple Moving Average (SMA) using TA-Lib.
    Returns a Pandas Series with NaN for initial periods.
    """
    if timeperiod is None:
        # Attempt to get a specific SMA period from kwargs or default to a common one
        timeperiod = kwargs.get("timeperiod", SMA_SETTINGS.get("timeperiod_20", 20))
    
    if len(series) < timeperiod:
        return pd.Series(np.nan, index=series.index)
    return pd.Series(TA_Lib_C.SMA(series.values, timeperiod=timeperiod), index=series.index)

def calculate_ema(series: pd.Series, timeperiod: int = None, **kwargs) -> pd.Series:
    """
    Calculates Exponential Moving Average (EMA) using TA-Lib.
    Returns a Pandas Series with NaN for initial periods.
    """
    if timeperiod is None:
        # Attempt to get a specific EMA period from kwargs or default to a common one
        timeperiod = kwargs.get("timeperiod", EMA_SETTINGS.get("timeperiod_9", 9))

    if len(series) < timeperiod:
        return pd.Series(np.nan, index=series.index)
    return pd.Series(TA_Lib_C.EMA(series.values, timeperiod=timeperiod), index=series.index)

def calculate_atr(high: pd.Series, low: pd.Series, close: pd.Series, **kwargs) -> pd.Series:
    """
    Calculates Average True Range (ATR) using TA-Lib.
    Returns a Pandas Series with NaN for initial periods.
    """
    timeperiod = kwargs.get("timeperiod", ATR_SETTINGS["timeperiod"])
    if len(high) < timeperiod or len(low) < timeperiod or len(close) < timeperiod:
        return pd.Series(np.nan, index=high.index)
    return pd.Series(TA_Lib_C.ATR(high.values, low.values, close.values, timeperiod=timeperiod), index=high.index)

def calculate_macd(series: pd.Series, **kwargs) -> tuple[pd.Series, pd.Series, pd.Series]:
    """
    Calculates Moving Average Convergence/Divergence (MACD) using TA-Lib.
    Returns three Pandas Series: macd, macdsignal, macdhist.
    """
    fastperiod = kwargs.get("fastperiod", MACD_SETTINGS["fastperiod"])
    slowperiod = kwargs.get("slowperiod", MACD_SETTINGS["slowperiod"])
    signalperiod = kwargs.get("signalperiod", MACD_SETTINGS["signalperiod"])

    if len(series) < slowperiod + signalperiod: # Minimum data for MACD
        nan_series = pd.Series(np.nan, index=series.index)
        return nan_series, nan_series, nan_series
    
    macd, macdsignal, macdhist = TA_Lib_C.MACD(
        series.values,
        fastperiod=fastperiod,
        slowperiod=slowperiod,
        signalperiod=signalperiod
    )
    return pd.Series(macd, index=series.index), pd.Series(macdsignal, index=series.index), pd.Series(macdhist, index=series.index)

def calculate_bbands(series: pd.Series, **kwargs) -> tuple[pd.Series, pd.Series, pd.Series]:
    """
    Calculates Bollinger Bands (BBANDS) using TA-Lib.
    Returns three Pandas Series: upperband, middleband, lowerband.
    """
    timeperiod = kwargs.get("timeperiod", BBANDS_SETTINGS["timeperiod"])
    nbdevup = kwargs.get("nbdevup", BBANDS_SETTINGS["nbdevup"])
    nbdevdn = kwargs.get("nbdevdn", BBANDS_SETTINGS["nbdevdn"])
    matype = kwargs.get("matype", BBANDS_SETTINGS["matype"])

    if len(series) < timeperiod:
        nan_series = pd.Series(np.nan, index=series.index)
        return nan_series, nan_series, nan_series
    
    upperband, middleband, lowerband = TA_Lib_C.BBANDS(
        series.values,
        timeperiod=timeperiod,
        nbdevup=nbdevup,
        nbdevdn=nbdevdn,
        matype=matype
    )
    return pd.Series(upperband, index=series.index), pd.Series(middleband, index=series.index), pd.Series(lowerband, index=series.index)

def calculate_stoch(high: pd.Series, low: pd.Series, close: pd.Series, **kwargs) -> tuple[pd.Series, pd.Series]:
    """
    Calculates Stochastic Oscillator (STOCH) using TA-Lib.
    Returns two Pandas Series: slowk, slowd.
    """
    fastk_period = kwargs.get("fastk_period", STOCH_SETTINGS["fastk_period"])
    slowk_period = kwargs.get("slowk_period", STOCH_SETTINGS["slowk_period"])
    slowk_matype = kwargs.get("slowk_matype", STOCH_SETTINGS["slowk_matype"])
    slowd_period = kwargs.get("slowd_period", STOCH_SETTINGS["slowd_period"])
    slowd_matype = kwargs.get("slowd_matype", STOCH_SETTINGS["slowd_matype"])

    if len(high) < fastk_period or len(low) < fastk_period or len(close) < fastk_period:
        nan_series = pd.Series(np.nan, index=high.index)
        return nan_series, nan_series
    
    slowk, slowd = TA_Lib_C.STOCH(
        high.values,
        low.values,
        close.values,
        fastk_period=fastk_period,
        slowk_period=slowk_period,
        slowk_matype=slowk_matype,
        slowd_period=slowd_period,
        slowd_matype=slowd_matype
    )
    return pd.Series(slowk, index=high.index), pd.Series(slowd, index=high.index)
