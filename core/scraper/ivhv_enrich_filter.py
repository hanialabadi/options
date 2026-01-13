"""
Step 3 — Enrich IV/HV metrics and filter dual IV edge cohorts (GEM & PSC).

This module consumes the raw IV/HV surface snapshot (output of the Fidelity scraper)
and produces three DataFrames:
    - df_filtered: all tickers passing the base IVHV gap screen
    - df_gem: strong edge candidates (IVHV gap >= 3.5)
    - df_psc: neutral/income candidates (IVHV gap between 2.0 and 3.5)

Optional exports write the above to CSVs under OUTPUT_DIR (default: ./output).
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

from core.scraper.ivhv_bootstrap import get_latest_ivhv_snapshot

DEFAULT_OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "./output"))


def _get_logger(logger: Optional[logging.Logger]) -> logging.Logger:
    if logger:
        return logger

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    return logging.getLogger(__name__)


def enrich_and_filter_ivhv(
    df: pd.DataFrame,
    logger: Optional[logging.Logger] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Enrich IV/HV metrics and split into GEM/PSC cohorts.

    Parameters
    ----------
    df : pd.DataFrame
        Raw IV/HV snapshot with at least: Ticker, IV_30_D_Call, IV_30_D_Put, HV_30_D_Cur

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]
        (df_filtered, df_gem, df_psc)
    """
    logger = _get_logger(logger)
    required_cols = {"Ticker", "IV_30_D_Call", "IV_30_D_Put", "HV_30_D_Cur"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    working = df.copy()

    # Representative IV/HV (30D reference)
    working["IV30_Call"] = pd.to_numeric(working["IV_30_D_Call"], errors="coerce")
    working["IV30_Put"] = pd.to_numeric(working["IV_30_D_Put"], errors="coerce")
    working["HV30"] = pd.to_numeric(working["HV_30_D_Cur"], errors="coerce")
    working["IV30_Mid"] = working[["IV30_Call", "IV30_Put"]].mean(axis=1)

    # Basic sanity / liquidity filter
    initial_count = len(working)
    working = working[(working["IV30_Mid"] >= 15) & (working["HV30"] > 0)]
    logger.info("Liquidity filter: %s → %s rows", initial_count, len(working))

    if working.empty:
        logger.warning("No rows remain after IV/HV liquidity filter.")
        empty = working.copy()
        return empty, empty.copy(), empty.copy()

    # IV–HV gap
    working["IVHV_gap_30D"] = working["IV30_Mid"] - working["HV30"]

    # Cross-sectional IV rank
    iv_min = working["IV30_Mid"].min()
    iv_max = working["IV30_Mid"].max()
    iv_range = iv_max - iv_min

    if pd.isna(iv_range) or iv_range == 0:
        logger.warning("Flat IV surface detected. Setting IV_Rank_XS = 50 for all.")
        working["IV_Rank_XS"] = 50.0
    else:
        working["IV_Rank_XS"] = 100 * (working["IV30_Mid"] - iv_min) / iv_range

    # PSC / GEM thresholding
    df_filtered = working[working["IVHV_gap_30D"] >= 2.0].copy()
    if df_filtered.empty:
        logger.warning("No tickers passed IVHV gap >= 2.0.")
        return df_filtered, df_filtered.copy(), df_filtered.copy()

    # One row per ticker (dominance rule)
    df_filtered = (
        df_filtered.sort_values(by="IVHV_gap_30D", ascending=False)
        .drop_duplicates(subset="Ticker", keep="first")
    )

    df_filtered["HardPass"] = df_filtered["IVHV_gap_30D"] >= 5.0
    df_filtered["SoftPass"] = (df_filtered["IVHV_gap_30D"] >= 3.5) & (df_filtered["IVHV_gap_30D"] < 5.0)
    df_filtered["PSC_Pass"] = (df_filtered["IVHV_gap_30D"] >= 2.0) & (df_filtered["IVHV_gap_30D"] < 3.5)
    df_filtered["LowRank"] = df_filtered["IV_Rank_XS"] < 30

    df_gem = df_filtered[df_filtered["IVHV_gap_30D"] >= 3.5].copy()
    df_psc = df_filtered[df_filtered["PSC_Pass"]].copy()

    return df_filtered, df_gem, df_psc


def export_step3_outputs(
    df_filtered: pd.DataFrame,
    df_gem: pd.DataFrame,
    df_psc: pd.DataFrame,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    logger: Optional[logging.Logger] = None,
) -> None:
    """Persist step outputs to CSV."""
    logger = _get_logger(logger)
    output_dir.mkdir(parents=True, exist_ok=True)

    df_filtered.to_csv(output_dir / "PCS_Step3_IVHV_Filtered.csv", index=False)
    df_gem.to_csv(output_dir / "PCS_Step3_GEM_Filtered.csv", index=False)
    df_psc.to_csv(output_dir / "PCS_Step3_PSC_Filtered.csv", index=False)

    logger.info("Step 3 exports written to %s", output_dir)


def run_step3_ivhv_filter(
    input_path: Optional[Path | str] = None,
    output_dir: Optional[Path | str] = None,
    export: bool = True,
    logger: Optional[logging.Logger] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Load the latest IV/HV snapshot (or a provided file), run enrichment, and optionally export CSVs.
    """
    logger = _get_logger(logger)
    resolved_input = Path(input_path) if input_path else Path(get_latest_ivhv_snapshot())
    resolved_output = Path(output_dir) if output_dir else DEFAULT_OUTPUT_DIR

    logger.info("Loading IV/HV snapshot: %s", resolved_input)
    df_raw = pd.read_csv(resolved_input)
    df_filtered, df_gem, df_psc = enrich_and_filter_ivhv(df_raw, logger=logger)

    if export:
        export_step3_outputs(df_filtered, df_gem, df_psc, resolved_output, logger=logger)

    if not df_filtered.empty:
        summary = (
            df_filtered.groupby(["HardPass", "SoftPass", "PSC_Pass", "LowRank"])
            .size()
            .reset_index(name="Count")
        )
        logger.info("Step 3 complete: %s tickers", len(df_filtered))
        logger.info("Summary breakdown:\n%s", summary)
        logger.info(
            "Top sample:\n%s",
            df_filtered[["Ticker", "IVHV_gap_30D", "IV_Rank_XS", "HardPass", "SoftPass", "PSC_Pass"]].head(),
        )
    else:
        logger.info("Step 3 produced no candidates.")

    return df_filtered, df_gem, df_psc


def main():
    parser = argparse.ArgumentParser(description="Step 3: Enrich IV/HV metrics and filter GEM/PSC candidates.")
    parser.add_argument(
        "--input",
        type=str,
        help="Path to IV/HV snapshot CSV. Defaults to the latest snapshot in data/ivhv_archive.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        help="Directory for CSV outputs. Defaults to OUTPUT_DIR env var or ./output.",
    )
    parser.add_argument(
        "--no-export",
        action="store_true",
        help="Run processing without writing CSV exports.",
    )
    args = parser.parse_args()

    run_step3_ivhv_filter(
        input_path=args.input,
        output_dir=args.output_dir,
        export=not args.no_export,
    )


if __name__ == "__main__":
    main()
