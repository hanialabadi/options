"""
BW/CC Efficiency Scorecard — carry-adjusted income analysis.

Answers the core question: "Is the BW/CC capital structure earning its keep?"

Per-position metrics:
  - Net_Yield_Annual_Pct     — annualized premium yield MINUS margin carry
  - Premium_vs_Carry_Ratio   — cumulative premium ÷ cumulative carry
  - Days_Until_Carry_Eats_GL — days until carry cost wipes remaining unrealized gain
  - Carry_Efficiency_Grade   — A / B / C / D / F

Portfolio-level metrics (stored in df.attrs["bw_efficiency"]):
  - total_bw_cc_positions, total_premium_collected, total_carry_paid
  - portfolio_net_yield, portfolio_premium_carry_ratio
  - grade_distribution, worst_performers, best_performers

McMillan Ch.3: "The covered writer must earn at least the cost of carrying the stock."
"""

from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Grade thresholds ─────────────────────────────────────────────────────────
# Premium_vs_Carry_Ratio determines grade:
#   A: ratio >= 5.0  (premium is 5× carry — excellent)
#   B: ratio >= 3.0  (premium is 3× carry — good)
#   C: ratio >= 1.5  (premium barely covers carry — marginal)
#   D: ratio >= 1.0  (premium = carry — break-even, not worth the risk)
#   F: ratio <  1.0  (carry exceeds premium — losing money to Fidelity)

GRADE_A_THRESHOLD = 5.0
GRADE_B_THRESHOLD = 3.0
GRADE_C_THRESHOLD = 1.5
GRADE_D_THRESHOLD = 1.0

# BW/CC strategy names
_BW_CC_STRATEGIES = frozenset({"BUY_WRITE", "COVERED_CALL"})


class BWEfficiencyCalculator:
    """Computes carry-adjusted efficiency for BUY_WRITE and COVERED_CALL positions."""

    def __init__(self):
        self._portfolio_metrics: dict = {}

    def enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add efficiency columns to BW/CC stock legs. Non-blocking."""
        try:
            df = self._compute_per_position(df)
            self._compute_portfolio_metrics(df)
            self._log_summary(df)
        except Exception as e:
            logger.warning(f"BWEfficiencyCalculator failed (non-fatal): {e}")
        return df

    @property
    def portfolio_metrics(self) -> dict:
        return self._portfolio_metrics

    # ── Internal ──────────────────────────────────────────────────────────

    def _bw_cc_stock_mask(self, df: pd.DataFrame) -> pd.Series:
        """Mask for stock legs of BW/CC positions (where carry cost lives)."""
        strategy = df.get("Strategy", pd.Series("", index=df.index)).astype(str)
        is_bw_cc = strategy.isin(_BW_CC_STRATEGIES)
        asset = df.get("AssetType", pd.Series("", index=df.index)).astype(str)
        is_stock = asset.isin(("EQUITY", "STOCK"))
        return is_bw_cc & is_stock

    def _compute_per_position(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute efficiency metrics for each BW/CC stock leg."""
        mask = self._bw_cc_stock_mask(df)

        # Initialize columns
        df["Net_Yield_Annual_Pct"] = np.nan
        df["Premium_vs_Carry_Ratio"] = np.nan
        df["Days_Until_Carry_Eats_GL"] = np.nan
        df["Carry_Efficiency_Grade"] = ""

        if not mask.any():
            return df

        # ── Inputs ────────────────────────────────────────────────────
        cum_premium = pd.to_numeric(
            df.loc[mask, "Cumulative_Premium_Collected"] if "Cumulative_Premium_Collected" in df.columns
            else pd.Series(0.0, index=df.loc[mask].index),
            errors="coerce",
        ).fillna(0.0)

        cum_carry = pd.to_numeric(
            df.loc[mask, "Cumulative_Margin_Carry"] if "Cumulative_Margin_Carry" in df.columns
            else pd.Series(0.0, index=df.loc[mask].index),
            errors="coerce",
        ).fillna(0.0)

        daily_cost = pd.to_numeric(
            df.loc[mask, "Daily_Margin_Cost"] if "Daily_Margin_Cost" in df.columns
            else pd.Series(0.0, index=df.loc[mask].index),
            errors="coerce",
        ).fillna(0.0)

        days = pd.to_numeric(
            df.loc[mask, "Days_In_Trade"] if "Days_In_Trade" in df.columns
            else pd.Series(1.0, index=df.loc[mask].index),
            errors="coerce",
        ).fillna(1.0).clip(lower=1)

        qty = pd.to_numeric(
            df.loc[mask, "Quantity"] if "Quantity" in df.columns
            else pd.Series(1.0, index=df.loc[mask].index),
            errors="coerce",
        ).abs().clip(lower=1)

        spot = pd.to_numeric(
            df.loc[mask, "UL Last"] if "UL Last" in df.columns
            else pd.Series(0.0, index=df.loc[mask].index),
            errors="coerce",
        ).fillna(0.0)

        gl = pd.to_numeric(
            df.loc[mask, "Total_GL_Decimal"] if "Total_GL_Decimal" in df.columns
            else pd.Series(0.0, index=df.loc[mask].index),
            errors="coerce",
        ).fillna(0.0)

        # ── Net Yield (annualized) ────────────────────────────────────
        # Premium collected per share per day, minus carry per share per day,
        # annualized as % of capital at risk (spot price).
        #
        # Cumulative_Premium_Collected is per-share.
        # Cumulative_Margin_Carry and Daily_Margin_Cost are per-contract (×100 shares).
        # Normalize carry to per-share: divide by (qty × 100).
        carry_per_share = cum_carry / (qty * 100)
        net_income_per_share = cum_premium - carry_per_share

        # Annualize: (net_income / days_held) × 365, as % of spot
        capital = spot.replace(0, np.nan)
        daily_net = net_income_per_share / days
        annual_net_pct = (daily_net / capital * 365 * 100).round(2)
        df.loc[mask, "Net_Yield_Annual_Pct"] = annual_net_pct

        # ── Premium vs Carry Ratio ────────────────────────────────────
        # How many dollars of premium collected per dollar of carry paid.
        # Compare in same units: premium per contract vs carry per contract.
        premium_per_contract = cum_premium * 100  # per-share → per-contract
        carry_per_contract = cum_carry  # already per-contract
        carry_nz = carry_per_contract.replace(0, np.nan)
        ratio = (premium_per_contract / carry_nz).round(2)
        df.loc[mask, "Premium_vs_Carry_Ratio"] = ratio

        # ── Days Until Carry Eats GL ──────────────────────────────────
        # If position has unrealized gain and daily carry > 0, how many days
        # until carry wipes it out?
        # GL is total (per-contract), daily_cost is per-contract.
        daily_nz = daily_cost.replace(0, np.nan)
        has_gain = gl > 0
        days_left = pd.Series(np.nan, index=df.loc[mask].index)
        gain_mask = has_gain & daily_cost.notna() & (daily_cost > 0.01)
        days_left[gain_mask] = (gl[gain_mask] / daily_nz[gain_mask]).round(0)
        df.loc[mask, "Days_Until_Carry_Eats_GL"] = days_left

        # ── Efficiency Grade ──────────────────────────────────────────
        grades = pd.Series("", index=df.loc[mask].index)

        # For positions with carry: grade by ratio
        has_carry = cum_carry > 0.01
        grades[has_carry & (ratio >= GRADE_A_THRESHOLD)] = "A"
        grades[has_carry & (ratio >= GRADE_B_THRESHOLD) & (ratio < GRADE_A_THRESHOLD)] = "B"
        grades[has_carry & (ratio >= GRADE_C_THRESHOLD) & (ratio < GRADE_B_THRESHOLD)] = "C"
        grades[has_carry & (ratio >= GRADE_D_THRESHOLD) & (ratio < GRADE_C_THRESHOLD)] = "D"
        grades[has_carry & (ratio < GRADE_D_THRESHOLD)] = "F"

        # Positions with no carry (retirement, no margin) → grade by premium only
        no_carry = ~has_carry
        has_premium = cum_premium > 0
        grades[no_carry & has_premium] = "A"  # Free carry = always efficient
        grades[no_carry & ~has_premium] = "—"  # New position, no data yet

        df.loc[mask, "Carry_Efficiency_Grade"] = grades

        return df

    def _compute_portfolio_metrics(self, df: pd.DataFrame) -> None:
        """Aggregate BW/CC efficiency into portfolio-level summary."""
        mask = self._bw_cc_stock_mask(df)

        if not mask.any():
            self._portfolio_metrics = {}
            return

        bw_df = df.loc[mask]

        cum_premium = pd.to_numeric(
            bw_df.get("Cumulative_Premium_Collected", pd.Series(0.0)),
            errors="coerce",
        ).fillna(0.0)

        cum_carry = pd.to_numeric(
            bw_df.get("Cumulative_Margin_Carry", pd.Series(0.0)),
            errors="coerce",
        ).fillna(0.0)

        qty = pd.to_numeric(
            bw_df.get("Quantity", pd.Series(1)),
            errors="coerce",
        ).abs().clip(lower=1)

        # Total premium in dollars (per-share × qty × 100)
        total_premium = float((cum_premium * qty * 100).sum())
        total_carry = float(cum_carry.sum())

        # Portfolio-level ratio
        ratio = round(total_premium / total_carry, 2) if total_carry > 0.01 else float("inf")

        # Grade distribution
        grades = bw_df.get("Carry_Efficiency_Grade", pd.Series(""))
        grade_dist = grades.value_counts().to_dict()

        # Worst and best performers
        net_yield = pd.to_numeric(bw_df.get("Net_Yield_Annual_Pct", pd.Series()), errors="coerce")
        tickers = bw_df.get("Ticker", bw_df.get("Underlying_Ticker", pd.Series("")))

        worst = []
        best = []
        if net_yield.notna().any():
            sorted_idx = net_yield.sort_values().index
            for idx in sorted_idx[:3]:
                if pd.notna(net_yield[idx]):
                    worst.append({
                        "ticker": str(tickers.get(idx, "?")),
                        "net_yield": float(net_yield[idx]),
                        "grade": str(grades.get(idx, "")),
                    })
            for idx in sorted_idx[-3:][::-1]:
                if pd.notna(net_yield[idx]):
                    best.append({
                        "ticker": str(tickers.get(idx, "?")),
                        "net_yield": float(net_yield[idx]),
                        "grade": str(grades.get(idx, "")),
                    })

        # Daily theta from short call legs of these same trades
        daily_theta = 0.0
        trade_ids = set(bw_df.get("TradeID", pd.Series()).dropna().astype(str))
        if trade_ids and "TradeID" in df.columns and "Theta" in df.columns:
            call_mask = (
                df["TradeID"].astype(str).isin(trade_ids) &
                (df.get("AssetType", pd.Series("")) == "OPTION")
            )
            if call_mask.any():
                theta_vals = pd.to_numeric(df.loc[call_mask, "Theta"], errors="coerce").fillna(0)
                qty_vals = pd.to_numeric(df.loc[call_mask, "Quantity"], errors="coerce").fillna(0)
                daily_theta = float((theta_vals * qty_vals).sum() * 100)

        daily_carry = pd.to_numeric(bw_df.get("Daily_Margin_Cost", pd.Series(0.0)), errors="coerce").fillna(0).sum()

        metrics = {
            "total_bw_cc_positions": int(mask.sum()),
            "total_premium_collected": round(total_premium, 2),
            "total_carry_paid": round(total_carry, 2),
            "portfolio_premium_carry_ratio": ratio,
            "portfolio_net_yield_daily": round(daily_theta - float(daily_carry), 2),
            "bw_cc_daily_theta": round(daily_theta, 2),
            "bw_cc_daily_carry": round(float(daily_carry), 2),
            "grade_distribution": grade_dist,
            "worst_performers": worst,
            "best_performers": best,
        }
        self._portfolio_metrics = metrics
        df.attrs["bw_efficiency"] = metrics

    def _log_summary(self, df: pd.DataFrame) -> None:
        m = self._portfolio_metrics
        if not m:
            return
        logger.info(
            f"[BWEfficiency] {m['total_bw_cc_positions']} BW/CC positions | "
            f"Premium: ${m['total_premium_collected']:,.0f} | "
            f"Carry: ${m['total_carry_paid']:,.0f} | "
            f"Ratio: {m['portfolio_premium_carry_ratio']:.1f}× | "
            f"θ: ${m['bw_cc_daily_theta']:.2f}/day | "
            f"Carry: ${m['bw_cc_daily_carry']:.2f}/day | "
            f"Grades: {m['grade_distribution']}"
        )
