"""
Margin Carry Calculator — cumulative carry cost and portfolio burn rate.

Builds on top of ``Daily_Margin_Cost`` (computed in ``compute_basic_drift.py``).
Adds:
  - Cumulative_Margin_Carry  — total interest paid since entry
  - Carry_Adjusted_GL        — Total_GL_Decimal minus cumulative carry
  - Carry_Adjusted_GL_Pct    — percentage form
  - Carry_Theta_Ratio        — daily margin cost ÷ daily theta income (income strategies)
  - Carry_Classification     — NONE / COVERED / MILD_INVERSION / SEVERE_INVERSION

Portfolio-level metrics stored in ``df.attrs``:
  - portfolio_daily_margin_burn, portfolio_monthly_margin_burn, portfolio_annual_margin_burn
  - portfolio_cumulative_carry, portfolio_theta_income_daily, portfolio_net_carry

McMillan Ch.3: "The covered writer must earn at least the cost of carrying the stock."
Passarelli Ch.6: "Negative carry — yield below financing rate — is a ROLL signal."
"""

from __future__ import annotations

import logging
import os
from typing import Optional

import numpy as np
import pandas as pd

from core.management.cycle1.identity.constants import (
    FIDELITY_MARGIN_RATE,
    FIDELITY_MARGIN_RATE_DAILY,
)
from core.management.cycle3.doctrine.thresholds import (
    CARRY_INVERSION_SEVERE,
    CARRY_INVERSION_MILD,
)
from core.shared.finance_utils import is_retirement_account

logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────

# Allow broker-specific override via environment variable
_MARGIN_RATE_OVERRIDE = os.getenv("MARGIN_RATE_OVERRIDE")
MARGIN_RATE = float(_MARGIN_RATE_OVERRIDE) if _MARGIN_RATE_OVERRIDE else FIDELITY_MARGIN_RATE
MARGIN_RATE_DAILY = MARGIN_RATE / 365

# Actual margin debit balance (from Fidelity margin calculator).
# Fidelity charges interest on the DEBIT (borrowed cash), not position values.
# Set this env var for exact portfolio-level burn rate.
_MARGIN_DEBIT_ENV = os.getenv("MARGIN_DEBIT")
MARGIN_DEBIT = float(_MARGIN_DEBIT_ENV) if _MARGIN_DEBIT_ENV else None

# Portfolio-level warning thresholds
MARGIN_BURN_WARNING_DAILY = 50.0    # $50/day → flag in dashboard
MARGIN_BURN_CRITICAL_DAILY = 100.0  # $100/day → red alert

# Income strategies where theta comparison is meaningful
_INCOME_STRATEGIES = frozenset({
    "BUY_WRITE", "COVERED_CALL", "CSP", "SHORT_PUT", "PMCC",
})


# ── Calculator ───────────────────────────────────────────────────────────────

class MarginCarryCalculator:
    """Computes daily, cumulative, and portfolio-level margin carry costs.

    Designed to run in Cycle 2 after ``compute_basic_drift()`` has set
    ``Daily_Margin_Cost`` and ``Days_In_Trade``.
    """

    def __init__(self, margin_rate: Optional[float] = None, margin_debit: Optional[float] = None):
        self.rate = margin_rate if margin_rate is not None else MARGIN_RATE
        self.rate_daily = self.rate / 365
        self.margin_debit = margin_debit if margin_debit is not None else MARGIN_DEBIT
        self._portfolio_metrics: dict = {}

    # ── Public API ───────────────────────────────────────────────────────

    def enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add carry columns to the management DataFrame. Non-blocking."""
        try:
            df = self._tag_retirement_accounts(df)
            df = self._compute_cumulative_carry(df)
            df = self._compute_carry_adjusted_pnl(df)
            df = self._compute_carry_theta_ratio(df)
            df = self._classify_carry(df)
            self._compute_portfolio_metrics(df)
            self._log_summary(df)
        except Exception as e:
            logger.warning(f"MarginCarryCalculator failed (non-fatal): {e}")
        return df

    @property
    def portfolio_metrics(self) -> dict:
        return self._portfolio_metrics

    # ── Internal ─────────────────────────────────────────────────────────

    def _tag_retirement_accounts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Tag positions as retirement (no margin) vs taxable (margin).

        Roth IRA, Traditional IRA, 401K, etc. are cash-only — no margin
        interest applies. This column drives all downstream carry logic.
        """
        if "Account" in df.columns:
            df["Is_Retirement"] = df["Account"].apply(
                lambda a: is_retirement_account(str(a)) if pd.notna(a) else False
            )
        else:
            df["Is_Retirement"] = False
        return df

    def _compute_cumulative_carry(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cumulative_Margin_Carry = Daily_Margin_Cost × Days_In_Trade."""
        daily = df.get("Daily_Margin_Cost", pd.Series(0.0, index=df.index))
        days = df.get("Days_In_Trade", pd.Series(0.0, index=df.index))

        daily_f = pd.to_numeric(daily, errors="coerce").fillna(0.0)
        days_f = pd.to_numeric(days, errors="coerce").fillna(0.0).clip(lower=0)

        df["Cumulative_Margin_Carry"] = (daily_f * days_f).round(2)
        return df

    def _compute_carry_adjusted_pnl(self, df: pd.DataFrame) -> pd.DataFrame:
        """Carry_Adjusted_GL = Total_GL_Decimal − Cumulative_Margin_Carry."""
        gl = pd.to_numeric(
            df.get("Total_GL_Decimal", pd.Series(0.0, index=df.index)),
            errors="coerce",
        ).fillna(0.0)

        carry = df.get("Cumulative_Margin_Carry", pd.Series(0.0, index=df.index))

        df["Carry_Adjusted_GL"] = (gl - carry).round(2)

        # Percentage: carry-adjusted GL / position cost basis
        basis = pd.to_numeric(
            df.get("Basis", df.get("Cost_Basis", pd.Series(np.nan, index=df.index))),
            errors="coerce",
        )
        basis_abs = basis.abs().replace(0, np.nan)
        df["Carry_Adjusted_GL_Pct"] = (
            (df["Carry_Adjusted_GL"] / basis_abs) * 100
        ).round(2)

        return df

    def _compute_carry_theta_ratio(self, df: pd.DataFrame) -> pd.DataFrame:
        """Carry_Theta_Ratio: daily margin cost ÷ daily theta income.

        Only meaningful for income strategies with short premium.
        < 1.0 = theta covers carry.  > 1.0 = carry exceeds theta income.
        """
        df["Carry_Theta_Ratio"] = np.nan

        daily_cost = pd.to_numeric(
            df.get("Daily_Margin_Cost", pd.Series(np.nan, index=df.index)),
            errors="coerce",
        )

        # Theta income: |Theta| × 100 × |Quantity| for short option legs
        theta = pd.to_numeric(
            df.get("Theta", pd.Series(np.nan, index=df.index)),
            errors="coerce",
        ).abs()
        qty = pd.to_numeric(
            df.get("Quantity", pd.Series(0, index=df.index)),
            errors="coerce",
        ).abs()

        # Only compute for short option legs in income strategies
        is_option = df.get("AssetType", pd.Series("", index=df.index)) == "OPTION"
        is_short = qty > 0  # already abs
        raw_qty = pd.to_numeric(
            df.get("Quantity", pd.Series(0, index=df.index)),
            errors="coerce",
        )
        is_short = raw_qty < 0
        strategy = df.get("Strategy", pd.Series("", index=df.index))
        is_income = strategy.isin(_INCOME_STRATEGIES)

        mask = is_option & is_short & is_income & theta.notna() & (theta > 0)

        if mask.any():
            theta_income_daily = theta.loc[mask] * 100 * qty.loc[mask]
            theta_income_daily = theta_income_daily.replace(0, np.nan)
            df.loc[mask, "Carry_Theta_Ratio"] = (
                daily_cost.loc[mask] / theta_income_daily
            ).round(3)

        return df

    def _classify_carry(self, df: pd.DataFrame) -> pd.DataFrame:
        """Classify each position's carry situation."""
        daily_cost = pd.to_numeric(
            df.get("Daily_Margin_Cost", pd.Series(0.0, index=df.index)),
            errors="coerce",
        ).fillna(0.0)

        ratio = pd.to_numeric(
            df.get("Carry_Theta_Ratio", pd.Series(np.nan, index=df.index)),
            errors="coerce",
        )

        classifications = pd.Series("NONE", index=df.index)

        # Positions with carry cost
        has_carry = daily_cost > 0.01
        classifications[has_carry] = "UNCOVERED"

        # Income strategies: classify by theta coverage
        has_ratio = ratio.notna() & has_carry
        classifications[has_ratio & (ratio < CARRY_INVERSION_MILD)] = "COVERED"
        classifications[has_ratio & (ratio >= CARRY_INVERSION_MILD) & (ratio < CARRY_INVERSION_SEVERE)] = "MILD_INVERSION"
        classifications[has_ratio & (ratio >= CARRY_INVERSION_SEVERE)] = "SEVERE_INVERSION"

        df["Carry_Classification"] = classifications
        return df

    def _compute_portfolio_metrics(self, df: pd.DataFrame) -> None:
        """Aggregate portfolio-level carry metrics into df.attrs."""
        daily_cost = pd.to_numeric(
            df.get("Daily_Margin_Cost", pd.Series(0.0, index=df.index)),
            errors="coerce",
        ).fillna(0.0)

        cumulative = pd.to_numeric(
            df.get("Cumulative_Margin_Carry", pd.Series(0.0, index=df.index)),
            errors="coerce",
        ).fillna(0.0)

        # Theta income from short legs
        theta = pd.to_numeric(
            df.get("Theta", pd.Series(0.0, index=df.index)),
            errors="coerce",
        ).fillna(0.0).abs()
        qty = pd.to_numeric(
            df.get("Quantity", pd.Series(0, index=df.index)),
            errors="coerce",
        )
        is_option = df.get("AssetType", pd.Series("", index=df.index)) == "OPTION"
        is_short = qty < 0
        short_theta_mask = is_option & is_short

        theta_income_daily = 0.0
        if short_theta_mask.any():
            theta_income_daily = float(
                (theta.loc[short_theta_mask] * 100 * qty.loc[short_theta_mask].abs()).sum()
            )

        # Per-position estimated burn (sum of per-position Daily_Margin_Cost)
        estimated_burn = float(daily_cost.sum())

        # If actual margin debit is known, use it for exact portfolio-level cost.
        # Fidelity charges interest on the debit balance, not on individual positions.
        if self.margin_debit is not None and self.margin_debit > 0:
            daily_burn = round(self.margin_debit * self.rate_daily, 2)
            burn_source = "ACTUAL_DEBIT"
        else:
            daily_burn = estimated_burn
            burn_source = "ESTIMATED"

        # Per-account breakdown: retirement (cash) vs taxable (margin)
        is_retirement = df.get("Is_Retirement", pd.Series(False, index=df.index))
        taxable_mask = ~is_retirement
        retirement_mask = is_retirement

        taxable_burn = float(daily_cost[taxable_mask].sum()) if taxable_mask.any() else 0.0
        retirement_positions = int(retirement_mask.sum()) if retirement_mask.any() else 0
        taxable_positions = int(taxable_mask.sum()) if taxable_mask.any() else 0
        taxable_cumulative = float(cumulative[taxable_mask].sum()) if taxable_mask.any() else 0.0

        metrics = {
            "portfolio_daily_margin_burn": round(daily_burn, 2),
            "portfolio_monthly_margin_burn": round(daily_burn * 30, 2),
            "portfolio_annual_margin_burn": round(daily_burn * 365, 2),
            "margin_debit": round(self.margin_debit, 2) if self.margin_debit else None,
            "burn_source": burn_source,
            "estimated_daily_burn": round(estimated_burn, 2),
            "portfolio_cumulative_carry": round(float(cumulative.sum()), 2),
            "portfolio_theta_income_daily": round(theta_income_daily, 2),
            "portfolio_net_carry": round(theta_income_daily - daily_burn, 2),
            "portfolio_margin_positions": int((daily_cost > 0.01).sum()),
            "portfolio_carry_health": (
                "GREEN" if daily_burn < MARGIN_BURN_WARNING_DAILY
                else "YELLOW" if daily_burn < MARGIN_BURN_CRITICAL_DAILY
                else "RED"
            ),
            # Per-account breakdown
            "taxable_positions": taxable_positions,
            "taxable_daily_burn": round(taxable_burn, 2),
            "taxable_cumulative_carry": round(taxable_cumulative, 2),
            "retirement_positions": retirement_positions,
            "retirement_daily_burn": 0.0,  # always zero — no margin in retirement
        }
        self._portfolio_metrics = metrics
        df.attrs["margin_carry"] = metrics

    def _log_summary(self, df: pd.DataFrame) -> None:
        m = self._portfolio_metrics
        if not m:
            return
        severe = (df.get("Carry_Classification", pd.Series()) == "SEVERE_INVERSION").sum()
        logger.info(
            f"[MarginCarry] Burn: ${m['portfolio_daily_margin_burn']:.2f}/day "
            f"(${m['portfolio_monthly_margin_burn']:,.0f}/mo) | "
            f"θ income: ${m['portfolio_theta_income_daily']:.2f}/day | "
            f"Net: ${m['portfolio_net_carry']:.2f}/day | "
            f"Health: {m['portfolio_carry_health']} | "
            f"Taxable: {m['taxable_positions']} pos (${m['taxable_daily_burn']:.2f}/day) | "
            f"Retirement: {m['retirement_positions']} pos ($0/day) | "
            f"Severe inversions: {severe}"
        )
