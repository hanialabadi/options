import pandas as pd
import numpy as np
from scipy.stats import percentileofscore


def _rolling_percentile_rank(series: pd.Series, window: int) -> pd.Series:
    """Vectorized rolling percentile rank — replaces explicit for-loop.

    Uses rolling().apply(raw=True) to pass NumPy arrays directly,
    avoiding per-row iloc indexing and Series construction overhead.
    Produces identical output to the previous for-loop implementation
    (same percentileofscore call with default kind='rank').
    """
    def _pctrank(arr):
        if np.isnan(arr[-1]):
            return np.nan
        valid = arr[~np.isnan(arr)]
        if len(valid) < window:
            return np.nan
        return percentileofscore(valid, arr[-1])
    return series.rolling(window=window, min_periods=window).apply(_pctrank, raw=True)


class IVEngine:
    def __init__(self, debug_mode=False, debug_tickers=None):
        self.debug_mode = debug_mode
        self.debug_tickers = debug_tickers if debug_tickers is not None else []

    def calculate_derived_metrics(self, history_df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates all derived IV metrics based on the historical IV term structure.
        history_df is expected to have columns: ticker, trade_date, iv_7d, iv_14d, ..., iv_1080d
        """
        if history_df.empty:
            return pd.DataFrame()

        # Ensure trade_date is datetime and sorted
        history_df['trade_date'] = pd.to_datetime(history_df['trade_date'])
        history_df = history_df.sort_values(by=['ticker', 'trade_date'])

        # Initialize new columns with NaN
        new_columns = [
            'IV_History_Count', 'IV_Maturity_Level', 'IV_Rank_20D', 'IV_Rank_30D',
            'IV_Rank_60D', 'IV_Rank_252D', 'IV_7D_5D_ROC', 'IV_30D_5D_ROC',
            'IV_30D_10D_ROC', 'IV_90D_10D_ROC', 'IV_30D_Mean_30', 'IV_30D_Std_30',
            'IV_30D_ZScore_30', 'Slope_30_7', 'Slope_30_21', 'Slope_90_30', 'Slope_180_90',
            'Surface_Steepness', 'Surface_Shape', 'IV_Regime',
            'Structural_IV_Cycle', 'LongTerm_ZScore', 'IV_Rank_Source'
        ]
        for col in new_columns:
            if col not in history_df.columns:
                history_df[col] = np.nan

        # Group by ticker for calculations
        grouped = history_df.groupby('ticker')

        # Debug mode: filter to debug tickers only
        if self.debug_mode and self.debug_tickers:
            debug_tickers_upper = [t.upper() for t in self.debug_tickers]
            history_df = history_df[history_df['ticker'].str.upper().isin(debug_tickers_upper)]
            if history_df.empty:
                return pd.DataFrame()
            grouped = history_df.groupby('ticker')

        results = []
        for _, group in grouped:
            group = group.reset_index(drop=True)

            # Calculate IV_History_Count
            group['IV_History_Count'] = len(group)

            # Calculate IV_Maturity_Level
            group['IV_Maturity_Level'] = group['IV_History_Count'].apply(self._calculate_maturity_level)

            # Apply rolling calculations
            group = self._calculate_rolling_metrics(group)
            group = self._calculate_surface_metrics(group)
            group = self._calculate_phase2_metrics(group)
            group = self._calculate_phase3_metrics(group)
            group = self._calculate_phase4_metrics(group)

            results.append(group)

        if not results:
            return pd.DataFrame()

        result_df = pd.concat(results).reset_index(drop=True)

        if self.debug_mode:
            self._debug_print(result_df)

        return result_df

    def _calculate_maturity_level(self, history_count: int) -> int:
        if history_count < 20:
            return 1
        elif history_count < 60:
            return 2
        elif history_count < 120:
            return 3
        elif history_count < 180:
            return 4
        else:
            return 5

    def _calculate_rolling_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        # Rolling Change Metrics
        for iv_col, days_ago in [('iv_7d', 5), ('iv_30d', 5), ('iv_30d', 10), ('iv_90d', 10)]:
            if iv_col in df.columns:
                col_name = f"{iv_col.upper()}_{days_ago}D_ROC"
                df[f'prev_{iv_col}_{days_ago}d'] = df[iv_col].shift(days_ago)
                df[col_name] = (df[iv_col] - df[f'prev_{iv_col}_{days_ago}d']) / df[f'prev_{iv_col}_{days_ago}d']
                df = df.drop(columns=[f'prev_{iv_col}_{days_ago}d'])

        # Short-Term Rolling Rank (20D)
        if 'iv_30d' in df.columns:
            df['IV_Rank_20D'] = _rolling_percentile_rank(df['iv_30d'], 20) if len(df) >= 20 else np.nan
            df['IV_Rank_Source'] = df['IV_Rank_20D'].apply(lambda x: "ROLLING_20D" if not pd.isna(x) else np.nan)
        return df

    def _calculate_surface_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        # Surface Structure Metrics
        if 'iv_30d' in df.columns and 'iv_7d' in df.columns:
            df['Slope_30_7'] = df['iv_30d'] - df['iv_7d']
        # iv_21d: near-term slope (30d minus 21d) — captures front-end term structure
        if 'iv_30d' in df.columns and 'iv_21d' in df.columns:
            df['Slope_30_21'] = df['iv_30d'] - df['iv_21d']
        if 'iv_90d' in df.columns and 'iv_30d' in df.columns:
            df['Slope_90_30'] = df['iv_90d'] - df['iv_30d']
        if 'iv_180d' in df.columns and 'iv_90d' in df.columns:
            df['Slope_180_90'] = df['iv_180d'] - df['iv_90d']
        if 'iv_360d' in df.columns and 'iv_7d' in df.columns:
            df['Surface_Steepness'] = df['iv_360d'] - df['iv_7d']

        # Surface_Shape: prefer Slope_30_21 when available (more stable than Slope_30_7)
        df['Surface_Shape'] = "FLAT"
        slope_short = 'Slope_30_21' if 'Slope_30_21' in df.columns and df['Slope_30_21'].notna().any() else 'Slope_30_7'
        if slope_short in df.columns and 'Slope_90_30' in df.columns:
            df.loc[(df[slope_short] > 0) & (df['Slope_90_30'] > 0), 'Surface_Shape'] = "CONTANGO"
            df.loc[df[slope_short] < 0, 'Surface_Shape'] = "INVERTED"
        return df

    def _calculate_phase2_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        # Phase 2: Developing Engine (>= 30 days)
        if df['IV_History_Count'].iloc[0] >= 30 and 'iv_30d' in df.columns:
            df['IV_30D_Mean_30'] = df['iv_30d'].rolling(window=30, min_periods=30).mean()
            df['IV_30D_Std_30'] = df['iv_30d'].rolling(window=30, min_periods=30).std()
            df['IV_30D_ZScore_30'] = (df['iv_30d'] - df['IV_30D_Mean_30']) / df['IV_30D_Std_30']
            df['IV_Rank_30D'] = _rolling_percentile_rank(df['iv_30d'], 30)
        return df

    def _calculate_phase3_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        # Phase 3: Intermediate Engine (>= 60 days)
        if df['IV_History_Count'].iloc[0] >= 60 and 'iv_30d' in df.columns:
            df['IV_Rank_60D'] = _rolling_percentile_rank(df['iv_30d'], 60)

            df['IV_Regime'] = np.nan
            df.loc[df['IV_Rank_60D'] < 20, 'IV_Regime'] = "LOW_VOL"
            df.loc[df['IV_Rank_60D'] > 80, 'IV_Regime'] = "HIGH_VOL"
            df['IV_Regime'] = df['IV_Regime'].fillna("NORMAL")
        return df

    def _calculate_phase4_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        # Phase 4: Full Maturity (>= 180-252 days)
        if df['IV_History_Count'].iloc[0] >= 252 and 'iv_30d' in df.columns:
            df['IV_Rank_252D'] = _rolling_percentile_rank(df['iv_30d'], 252)

            # Placeholder for Structural_IV_Cycle and LongTerm_ZScore
            df['Structural_IV_Cycle'] = np.nan
            df['LongTerm_ZScore'] = np.nan
        return df

    def _debug_print(self, df: pd.DataFrame):
        if self.debug_mode:
            print("\n--- IV Engine Diagnostic Block ---")
            print(f"Debug Mode: {self.debug_mode}")
            print(f"Debug Tickers: {self.debug_tickers}")
            print("\nProcessed DataFrame Head:")
            print(df.head())
            print("\nProcessed DataFrame Tail:")
            print(df.tail())
            print("\nDataFrame Info:")
            df.info()
            print("\nDataFrame Describe:")
            print(df.describe())
            print("--- End IV Engine Diagnostic Block ---\n")
