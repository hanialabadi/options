"""
Portfolio Admission Gate
========================
Post-acceptance, pre-output filter that annotates READY contracts with portfolio-level
constraint violations.  Does NOT change acceptance_status — it adds Portfolio_Admission
and Portfolio_Admission_Note columns so the dashboard and operator can enforce sizing rules
without the scan engine making irreversible admission decisions.

Rules (all configurable via kwargs):
    1. POSITION_CAP   — single contract > max_position_pct of total READY capital
    2. SECTOR_CAP     — sector (by benchmark ETF) > max_sector_pct of total READY capital
    3. DELTA_SKEW     — bull capital / total capital > max_bull_pct OR < (1 - max_bull_pct)
    4. CONCENTRATION  — effective positions (1/HHI) < min_effective_positions

Each rule annotates independently.  A contract can carry multiple flags.

Doctrine sources:
    - Vince (Mathematics of Money Management): position sizing by f-fraction, not cost basis
    - López de Prado (Advances in Financial ML): HRP cluster penalty for correlated groups
    - Carver (Systematic Trading): instrument weight cap ~10-12% of portfolio
    - McMillan (Options as a Strategic Investment Ch.4): sector concentration awareness

Called from: scan_engine/pipeline.py _finalize_results()
    df_enriched = apply_portfolio_admission(df_enriched)
"""

from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ── Default thresholds ────────────────────────────────────────────────────────
_DEFAULTS = {
    "max_position_pct":        0.20,   # single position ≤ 20% of total READY capital
    "max_sector_pct":          0.35,   # sector cluster ≤ 35% of total READY capital
    "max_bull_pct":            0.65,   # bull capital ≤ 65% of total (bear ≤ 65% symmetric)
    "min_effective_positions": 6,      # 1/HHI ≥ 6 (HHI ≤ 0.167)
}

_ADMIT = "ADMIT"


def _load_sector_map() -> dict[str, str]:
    """Load sector_benchmarks.SECTOR_BENCHMARK_MAP, fall back to empty dict on error."""
    try:
        from config.sector_benchmarks import SECTOR_BENCHMARK_MAP
        return SECTOR_BENCHMARK_MAP
    except Exception:
        return {}


def apply_portfolio_admission(
    df: pd.DataFrame,
    *,
    max_position_pct: float = _DEFAULTS["max_position_pct"],
    max_sector_pct: float = _DEFAULTS["max_sector_pct"],
    max_bull_pct: float = _DEFAULTS["max_bull_pct"],
    min_effective_positions: int = _DEFAULTS["min_effective_positions"],
    ready_col: str = "acceptance_status",
    ready_val: str = "READY",
    capital_col: str = "Capital_Requirement",
    bias_col: str = "Trade_Bias",
    ticker_col: str = "Ticker",
) -> pd.DataFrame:
    """
    Annotate READY contracts with portfolio-level admission flags.

    Args:
        df:                        Full Step12 output DataFrame.
        max_position_pct:          Single-position cap as fraction of total READY capital.
        max_sector_pct:            Sector cap as fraction of total READY capital.
        max_bull_pct:              Max fraction of capital allowed on one directional side.
        min_effective_positions:   Minimum 1/HHI (effective number of positions).
        ready_col / ready_val:     Column and value that identifies READY rows.
        capital_col:               Column holding capital requirement per contract.
        bias_col:                  Column with Trade_Bias ('Bullish' / 'Bearish').
        ticker_col:                Column with ticker symbol.

    Returns:
        df with two new columns added:
            Portfolio_Admission      — "ADMIT" or pipe-separated flag codes
            Portfolio_Admission_Note — human-readable explanation of each flag
    """
    df = df.copy()
    df["Portfolio_Admission"] = _ADMIT
    df["Portfolio_Admission_Note"] = ""

    ready_mask = df.get(ready_col, pd.Series(dtype=str)) == ready_val
    ready_idx = df.index[ready_mask]

    if len(ready_idx) == 0:
        logger.info("[PortfolioAdmission] No READY contracts — gate skipped.")
        return df

    # ── Extract capital series (fill missing with 0) ──────────────────────────
    cap = df.loc[ready_idx, capital_col].fillna(0).astype(float)
    total_cap = cap.sum()

    if total_cap == 0:
        logger.warning("[PortfolioAdmission] Total READY capital = 0 — gate skipped.")
        return df

    sector_map = _load_sector_map()

    # ── Sector grouping ───────────────────────────────────────────────────────
    tickers = df.loc[ready_idx, ticker_col].fillna("_unknown")
    sectors = tickers.map(lambda t: sector_map.get(t, sector_map.get("_default", "SPY")))
    sector_cap = cap.groupby(sectors).sum()            # {benchmark_etf: total_capital}

    # ── Directional totals ────────────────────────────────────────────────────
    biases = df.loc[ready_idx, bias_col].fillna("Unknown")
    bull_cap = cap[biases == "Bullish"].sum()
    bear_cap = cap[biases == "Bearish"].sum()
    bull_frac = bull_cap / total_cap
    bear_frac = bear_cap / total_cap

    # ── HHI (concentration) ───────────────────────────────────────────────────
    shares = cap / total_cap
    hhi = float((shares ** 2).sum())
    eff_positions = 1.0 / hhi if hhi > 0 else float("inf")

    # ── Portfolio-level log summary ───────────────────────────────────────────
    logger.info("[PortfolioAdmission] Portfolio snapshot:")
    logger.info(f"  READY contracts : {len(ready_idx)}")
    logger.info(f"  Total capital   : ${total_cap:,.0f}")
    logger.info(f"  Bull fraction   : {bull_frac*100:.1f}%  Bear fraction: {bear_frac*100:.1f}%")
    logger.info(f"  HHI             : {hhi:.4f}  (effective positions: {eff_positions:.1f})")
    for etf, sc in sector_cap.items():
        pct = sc / total_cap * 100
        flag = " ⚠️ SECTOR_CAP_EXCEEDED" if sc / total_cap > max_sector_pct else ""
        logger.info(f"  Sector {etf:6s}  : ${sc:>10,.0f}  ({pct:.1f}%){flag}")

    # ── Per-row annotation ────────────────────────────────────────────────────
    for idx in ready_idx:
        flags: list[str] = []
        notes: list[str] = []

        pos_cap_val  = float(df.at[idx, capital_col] or 0)
        pos_frac     = pos_cap_val / total_cap
        ticker_val   = df.at[idx, ticker_col]
        sector_val   = sector_map.get(ticker_val, sector_map.get("_default", "SPY"))
        sec_frac     = float(sector_cap.get(sector_val, 0)) / total_cap

        # Rule 1: position cap
        if pos_frac > max_position_pct:
            flags.append("POSITION_CAP")
            notes.append(
                f"{ticker_val} = {pos_frac*100:.1f}% of READY capital "
                f"(limit {max_position_pct*100:.0f}%). "
                f"Carver: single instrument weight should not exceed ~{max_position_pct*100:.0f}%."
            )

        # Rule 2: sector cap (flag every contract in an over-represented sector)
        if sec_frac > max_sector_pct:
            flags.append("SECTOR_CAP")
            notes.append(
                f"Sector {sector_val} = {sec_frac*100:.1f}% of READY capital "
                f"(limit {max_sector_pct*100:.0f}%). "
                f"López de Prado: correlated cluster concentration — reduce weight or trim cluster."
            )

        # Rule 3: directional skew (flag the dominant side only)
        bias_val = str(df.at[idx, bias_col] or "")
        if bias_val == "Bullish" and bull_frac > max_bull_pct:
            flags.append("DELTA_SKEW_BULL")
            notes.append(
                f"Portfolio bull fraction = {bull_frac*100:.1f}% "
                f"(limit {max_bull_pct*100:.0f}%). "
                f"Vince: capital-weighted directional exposure should not exceed ~{max_bull_pct*100:.0f}%."
            )
        elif bias_val == "Bearish" and bear_frac > max_bull_pct:
            flags.append("DELTA_SKEW_BEAR")
            notes.append(
                f"Portfolio bear fraction = {bear_frac*100:.1f}% "
                f"(limit {max_bull_pct*100:.0f}%). "
                f"Vince: capital-weighted directional exposure should not exceed ~{max_bull_pct*100:.0f}%."
            )

        # Rule 4: concentration (portfolio-level — same note on every row)
        if eff_positions < min_effective_positions:
            flags.append("CONCENTRATION_RISK")
            notes.append(
                f"Effective positions (1/HHI) = {eff_positions:.1f} "
                f"(minimum {min_effective_positions}). "
                f"López de Prado HRP: portfolio is under-diversified."
            )

        if flags:
            df.at[idx, "Portfolio_Admission"] = "|".join(flags)
            df.at[idx, "Portfolio_Admission_Note"] = " | ".join(notes)

    # ── Summary log ──────────────────────────────────────────────────────────
    admission_counts = df.loc[ready_idx, "Portfolio_Admission"].value_counts().to_dict()
    flagged = sum(1 for v in df.loc[ready_idx, "Portfolio_Admission"] if v != _ADMIT)
    logger.info(
        f"[PortfolioAdmission] Complete: {len(ready_idx) - flagged} ADMIT, "
        f"{flagged} flagged. Distribution: {admission_counts}"
    )

    return df
