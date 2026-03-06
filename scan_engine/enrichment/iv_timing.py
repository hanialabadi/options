"""
IV Timing State Classifier

Pure, deterministic classifier using iv_term_history-derived IV series.
No side effects beyond optional logging.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IVTimingConfig:
    iv_col: str = "iv_30d"
    date_col: str = "date"
    group_col: str = "ticker"
    sma_windows: Tuple[int, int] = (5, 10)
    slope_windows: Tuple[int, int] = (5, 10)
    std_windows: Tuple[int, int] = (5, 10)


def classify_iv_timing(
    df: pd.DataFrame,
    config: Optional[IVTimingConfig] = None,
    log_metrics: bool = True,
    include_metrics: bool = False,
) -> pd.DataFrame:
    """
    Classify IV timing state and chase risk using rolling metrics.

    Rules:
    - PEAKING: IV above SMA10 AND slope turning negative
    - CRUSHING: IV below SMA10 AND negative slope + falling std
    - BUILDING: IV above SMA5 AND positive slope
    - BASELINE: none of the above

    Outputs:
    - IV_Timing_State ∈ {BUILDING, PEAKING, CRUSHING, BASELINE}
    - IV_Chase_Risk ∈ {LOW, MEDIUM, HIGH}

    Pure function: does not modify PCS, does not trigger trades.
    """
    if config is None:
        config = IVTimingConfig()

    required_cols = {config.iv_col, config.date_col, config.group_col}
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns for IV timing: {missing}")

    df_out = df.copy()

    # Ensure sortable datetime
    df_out[config.date_col] = pd.to_datetime(df_out[config.date_col])
    df_out = df_out.sort_values([config.group_col, config.date_col])

    w5, w10 = config.sma_windows
    s5, s10 = config.slope_windows
    v5, v10 = config.std_windows

    def _compute_group(g: pd.DataFrame) -> pd.DataFrame:
        iv = g[config.iv_col].astype(float)

        sma5 = iv.rolling(window=w5, min_periods=w5).mean()
        sma10 = iv.rolling(window=w10, min_periods=w10).mean()

        # Slope as average change per calendar day over window (time-aware)
        dates = g[config.date_col]
        delta_days_5 = (dates - dates.shift(s5)).dt.days
        delta_days_10 = (dates - dates.shift(s10)).dt.days

        slope5 = (iv - iv.shift(s5)) / delta_days_5.replace({0: np.nan})
        slope10 = (iv - iv.shift(s10)) / delta_days_10.replace({0: np.nan})

        std5 = iv.rolling(window=v5, min_periods=v5).std()
        std10 = iv.rolling(window=v10, min_periods=v10).std()

        # Conditions (defensive std guard)
        stds_present = std5.notna() & std10.notna()
        is_crushing = (iv < sma10) & (slope10 < 0) & stds_present & (std5 < std10)
        is_peaking = (iv > sma10) & (slope5 < 0)
        is_building = (iv > sma5) & (slope5 > 0)

        # Explicit precedence: CRUSHING > PEAKING > BUILDING > BASELINE
        state = np.select(
            [is_crushing, is_peaking, is_building],
            ["CRUSHING", "PEAKING", "BUILDING"],
            default="BASELINE",
        )

        # Chase risk mapping (deterministic)
        chase_risk = np.where(
            state == "PEAKING",
            "HIGH",
            np.where(state == "CRUSHING", "MEDIUM", np.where(state == "BUILDING", "MEDIUM", "LOW")),
        )

        g = g.copy()
        # Intermediate metrics (diagnostic only)
        g["iv_sma_5"] = sma5
        g["iv_sma_10"] = sma10
        g["iv_slope_5"] = slope5
        g["iv_slope_10"] = slope10
        g["iv_std_5"] = std5
        g["iv_std_10"] = std10
        g["IV_Timing_State"] = state
        g["IV_Chase_Risk"] = chase_risk

        if log_metrics:
            # Log all intermediate metrics for the latest row only to avoid log spam
            latest = g.iloc[-1]
            logger.info(
                "[IV_TIMING] %s %s iv=%.4f sma5=%.4f sma10=%.4f slope5=%.4f slope10=%.4f std5=%.4f std10=%.4f state=%s risk=%s",
                latest[config.group_col],
                latest[config.date_col].date(),
                latest[config.iv_col] if pd.notna(latest[config.iv_col]) else float("nan"),
                latest["iv_sma_5"] if pd.notna(latest["iv_sma_5"]) else float("nan"),
                latest["iv_sma_10"] if pd.notna(latest["iv_sma_10"]) else float("nan"),
                latest["iv_slope_5"] if pd.notna(latest["iv_slope_5"]) else float("nan"),
                latest["iv_slope_10"] if pd.notna(latest["iv_slope_10"]) else float("nan"),
                latest["iv_std_5"] if pd.notna(latest["iv_std_5"]) else float("nan"),
                latest["iv_std_10"] if pd.notna(latest["iv_std_10"]) else float("nan"),
                latest["IV_Timing_State"],
                latest["IV_Chase_Risk"],
            )

        # Drop intermediate columns to keep output minimal unless requested
        if not include_metrics:
            g = g.drop(columns=["iv_sma_5", "iv_sma_10", "iv_slope_5", "iv_slope_10", "iv_std_5", "iv_std_10"])
        return g

    df_out = df_out.groupby(config.group_col, group_keys=False).apply(_compute_group)
    return df_out
