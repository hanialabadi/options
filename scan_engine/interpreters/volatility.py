"""
Volatility Interpreter — Long Straddle, Long Strangle.

Handles: Straddle, Strangle, Long Straddle, Long Strangle.

Key differences from directional/income:
  - Delta-neutral required (|Δ| < 0.15)
  - Vega dominant over theta (buying vol expansion)
  - Expected move vs total debit = core payoff metric
  - Squeeze detection = primary catalyst signal
  - IV regime: buy when cheap, avoid when rich (opposite of income)

Component weights (120 pts total):
  Expected move coverage  20 pts  — em_ratio vs premium paid (Natenberg Ch.7)
  IV cheapness            20 pts  — buying cheap vol (Natenberg Ch.4)
  Delta neutrality        15 pts  — structure validity
  Vega magnitude          15 pts  — exposure to vol expansion
  Squeeze catalyst        15 pts  — Keltner/BB compression (Raschke/Murphy)
  Gamma convexity         15 pts  — profiting from moves (Passarelli Ch.4)
  Spread/liquidity        10 pts  — execution quality
  DTE fit                 10 pts  — vol strategies need 21-60d
"""

from __future__ import annotations

import math
import logging
from typing import Dict, List

import pandas as pd

from .base import (
    StrategyInterpreter, ScoredComponent, VolContext,
    _sf, _ss, _expected_move,
)

logger = logging.getLogger(__name__)


class VolatilityInterpreter(StrategyInterpreter):

    @property
    def family(self) -> str:
        return "volatility"

    @property
    def handles(self) -> List[str]:
        return [
            "long straddle", "long strangle",
            "straddle", "strangle",
        ]

    def _classify_status(self, pct: float) -> str:
        if pct >= 70:
            return "Strong"
        elif pct >= 50:
            return "Eligible"
        else:
            return "Weak"

    def _score_components(self, row: pd.Series, direction: str) -> Dict[str, ScoredComponent]:
        c: Dict[str, ScoredComponent] = {}

        c['move_coverage'] = self._score_move_coverage(row)
        c['iv_cheapness'] = self._score_iv_cheapness(row)
        c['delta_neutrality'] = self._score_delta_neutrality(row)
        c['vega_magnitude'] = self._score_vega(row)
        c['squeeze_catalyst'] = self._score_squeeze(row)
        c['gamma_convexity'] = self._score_gamma(row)
        c['spread_liquidity'] = self._score_spread(row)
        c['dte_fit'] = self._score_dte(row)

        return c

    # ── Move coverage (20 pts) ────────────────────────────────────────────

    def _score_move_coverage(self, row: pd.Series) -> ScoredComponent:
        """Natenberg Ch.7: expected move must exceed premium paid."""
        em = _expected_move(row)
        premium = _sf(row, 'Mid_Price', 'Mid', 'Premium', 'Total_Debit')
        price = _sf(row, 'Last', 'UL Last', 'Stock_Price')

        if em <= 0 or premium <= 0 or price <= 0:
            return ScoredComponent(5, 20, "Move coverage data missing", "Natenberg Ch.7")

        em_ratio = em / premium if premium > 0 else 0

        if em_ratio >= 2.0:
            return ScoredComponent(20, 20,
                                   f"Expected move covers debit {em_ratio:.1f}× — strong edge",
                                   "Natenberg Ch.7: significant move potential vs cost")
        elif em_ratio >= 1.5:
            return ScoredComponent(15, 20,
                                   f"Expected move covers debit {em_ratio:.1f}× — good",
                                   "Natenberg Ch.7")
        elif em_ratio >= 1.0:
            return ScoredComponent(8, 20,
                                   f"Expected move covers debit {em_ratio:.1f}× — barely",
                                   "Natenberg Ch.7: breakeven at edge of expected move")
        elif em_ratio >= 0.7:
            return ScoredComponent(3, 20,
                                   f"Expected move covers debit {em_ratio:.1f}× — unfavorable",
                                   "Natenberg Ch.7: overpaying for vol")
        else:
            return ScoredComponent(0, 20,
                                   f"Expected move covers debit {em_ratio:.1f}× — poor structure",
                                   "Natenberg Ch.7")

    # ── IV cheapness (20 pts) ─────────────────────────────────────────────

    def _score_iv_cheapness(self, row: pd.Series) -> ScoredComponent:
        """Natenberg Ch.4: vol buyers want IV < HV (buying cheap expansion)."""
        iv = _sf(row, 'IV_30D', 'IV_Now', 'iv_30d')
        hv = _sf(row, 'HV_20D', 'HV_30D', 'hv_20', 'hv_30')
        iv_rank = _sf(row, 'IV_Rank_30D', 'IV_Rank')

        if iv <= 0 or hv <= 0:
            return ScoredComponent(8, 20, "IV/HV data missing", "Natenberg Ch.4")

        rv_iv_ratio = hv / iv if iv > 0 else 1.0

        if rv_iv_ratio >= 1.3:
            return ScoredComponent(20, 20,
                                   f"RV/IV {rv_iv_ratio:.2f} — very cheap vol to buy",
                                   "Natenberg Ch.4: ideal for vol expansion plays")
        elif rv_iv_ratio >= 1.1:
            return ScoredComponent(15, 20,
                                   f"RV/IV {rv_iv_ratio:.2f} — favorable pricing",
                                   "Natenberg Ch.4")
        elif rv_iv_ratio >= 0.9:
            return ScoredComponent(8, 20,
                                   f"RV/IV {rv_iv_ratio:.2f} — fair pricing", "Natenberg Ch.4")
        elif rv_iv_ratio >= 0.7:
            return ScoredComponent(3, 20,
                                   f"RV/IV {rv_iv_ratio:.2f} — vol is rich, buying expensive",
                                   "Natenberg Ch.4")
        else:
            return ScoredComponent(0, 20,
                                   f"RV/IV {rv_iv_ratio:.2f} — very expensive vol",
                                   "Natenberg Ch.4: severe pricing disadvantage")

    # ── Delta neutrality (15 pts) ─────────────────────────────────────────

    def _score_delta_neutrality(self, row: pd.Series) -> ScoredComponent:
        """Vol strategies need near-zero net delta."""
        delta = abs(_sf(row, 'Delta'))

        if delta <= 0.05:
            return ScoredComponent(15, 15, f"Δ {delta:.3f} — excellent neutrality",
                                   "Natenberg Ch.11: vol-play requires delta neutrality")
        elif delta <= 0.10:
            return ScoredComponent(12, 15, f"Δ {delta:.3f} — good neutrality",
                                   "Natenberg Ch.11")
        elif delta <= 0.15:
            return ScoredComponent(8, 15, f"Δ {delta:.3f} — acceptable", "Natenberg Ch.11")
        elif delta <= 0.25:
            return ScoredComponent(4, 15, f"Δ {delta:.3f} — directional bias emerging",
                                   "Natenberg Ch.11")
        else:
            return ScoredComponent(0, 15, f"Δ {delta:.3f} — too directional for vol strategy",
                                   "Natenberg Ch.11")

    # ── Vega magnitude (15 pts) ───────────────────────────────────────────

    def _score_vega(self, row: pd.Series) -> ScoredComponent:
        """Hull Ch.26: vol strategies need high vega to profit from IV expansion."""
        vega = abs(_sf(row, 'Vega'))
        price = _sf(row, 'Last', 'UL Last', default=100)

        if vega <= 0:
            return ScoredComponent(0, 15, "Vega missing", "Hull Ch.26")

        # Dollar-vega normalized by stock price
        vega_norm = vega / price * 100 if price > 0 else vega

        if vega >= 0.40:
            return ScoredComponent(15, 15, f"ν={vega:.2f} — high vol sensitivity",
                                   "Hull Ch.26")
        elif vega >= 0.20:
            pts = 8 + (vega - 0.20) / 0.20 * 7
            return ScoredComponent(pts, 15, f"ν={vega:.2f} — good", "Hull Ch.26")
        elif vega >= 0.10:
            return ScoredComponent(5, 15, f"ν={vega:.2f} — moderate", "Hull Ch.26")
        else:
            return ScoredComponent(2, 15, f"ν={vega:.2f} — low vol sensitivity",
                                   "Hull Ch.26")

    # ── Squeeze catalyst (15 pts) ─────────────────────────────────────────

    def _score_squeeze(self, row: pd.Series) -> ScoredComponent:
        """Raschke/Murphy 0.739: Keltner squeeze = vol expansion catalyst."""
        squeeze_on = _ss(row, 'Keltner_Squeeze_On').upper()
        squeeze_fired = _ss(row, 'Keltner_Squeeze_Fired').upper()
        chart_regime = _ss(row, 'Chart_Regime').upper()
        atr_rank = _sf(row, 'ATR_Rank')

        pts = 0.0
        notes = []

        if squeeze_fired in ('TRUE', '1', 'YES'):
            pts += 12
            notes.append("squeeze fired — vol expansion underway")
        elif squeeze_on in ('TRUE', '1', 'YES'):
            pts += 8
            notes.append("squeeze ON — compression detected, expansion pending")
        else:
            pts += 2
            notes.append("no squeeze detected")

        # ATR rank < 20 = compressed
        if atr_rank > 0 and atr_rank < 20:
            pts += 3
            notes.append(f"ATR rank {atr_rank:.0f} — compressed (expansion favorable)")

        if 'COMPRESSED' in chart_regime:
            pts += 2

        pts = min(15, pts)
        return ScoredComponent(pts, 15, " | ".join(notes),
                               "Raschke + Murphy 0.739: Keltner squeeze")

    # ── Gamma convexity (15 pts) ──────────────────────────────────────────

    def _score_gamma(self, row: pd.Series) -> ScoredComponent:
        """Passarelli Ch.4: vol strategies need gamma for move capture."""
        gamma = abs(_sf(row, 'Gamma'))
        price = _sf(row, 'Last', 'UL Last', default=100)

        if gamma <= 0:
            return ScoredComponent(0, 15, "Gamma missing", "Passarelli Ch.4")

        # Dollar-gamma
        gamma_dollar = gamma * price / 100

        if gamma_dollar >= 0.04:
            return ScoredComponent(15, 15,
                                   f"$γ={gamma_dollar:.3f} — strong convexity",
                                   "Passarelli Ch.4: gamma captures moves")
        elif gamma_dollar >= 0.02:
            pts = 8 + (gamma_dollar - 0.02) / 0.02 * 7
            return ScoredComponent(pts, 15, f"$γ={gamma_dollar:.3f} — good",
                                   "Passarelli Ch.4")
        elif gamma_dollar >= 0.01:
            return ScoredComponent(5, 15, f"$γ={gamma_dollar:.3f} — moderate",
                                   "Passarelli Ch.4")
        else:
            return ScoredComponent(2, 15, f"$γ={gamma_dollar:.4f} — low convexity",
                                   "Passarelli Ch.4")

    # ── Spread (10 pts) ───────────────────────────────────────────────────

    def _score_spread(self, row: pd.Series) -> ScoredComponent:
        """Vol strategies tolerate wider spreads (15% max)."""
        spread_pct = _sf(row, 'Bid_Ask_Spread_Pct')
        oi = _sf(row, 'Open_Interest')

        if spread_pct <= 0:
            return ScoredComponent(5, 10, "Spread data missing", "RAG")

        if spread_pct <= 5:
            pts = 10
        elif spread_pct <= 10:
            pts = 7
        elif spread_pct <= 15:
            pts = 4
        else:
            pts = 0

        notes = [f"spread {spread_pct:.1f}%"]
        if oi < 50:
            pts = max(0, pts - 2)
            notes.append(f"low OI {oi:.0f}")

        return ScoredComponent(pts, 10, " | ".join(notes), "RAG: STRATEGY_QUALITY_AUDIT")

    # ── DTE fit (10 pts) ──────────────────────────────────────────────────

    def _score_dte(self, row: pd.Series) -> ScoredComponent:
        """Vol strategies: 21-60d optimal (Natenberg)."""
        dte = _sf(row, 'Actual_DTE', 'DTE', default=0)

        if 21 <= dte <= 60:
            return ScoredComponent(10, 10, f"DTE {dte:.0f} — vol strategy sweet spot",
                                   "Natenberg Ch.11")
        elif 14 <= dte < 21:
            return ScoredComponent(5, 10, f"DTE {dte:.0f} — short, needs catalyst soon",
                                   "Natenberg Ch.11")
        elif 60 < dte <= 90:
            return ScoredComponent(7, 10, f"DTE {dte:.0f} — longer, more time for expansion",
                                   "Natenberg Ch.11")
        elif dte < 14:
            return ScoredComponent(0, 10, f"DTE {dte:.0f} — too short, theta dominant",
                                   "Natenberg Ch.15")
        else:
            return ScoredComponent(4, 10, f"DTE {dte:.0f} — long for vol play",
                                   "Natenberg Ch.11")

    # ── Vol interpretation ────────────────────────────────────────────────

    def interpret_volatility(self, row: pd.Series) -> VolContext:
        """Volatility = buying vol expansion → cheap IV is favorable."""
        iv = _sf(row, 'IV_30D', 'IV_Now', 'iv_30d')
        hv = _sf(row, 'HV_20D', 'HV_30D', 'hv_20', 'hv_30')
        iv_rank = _sf(row, 'IV_Rank_30D', 'IV_Rank')
        squeeze_on = _ss(row, 'Keltner_Squeeze_On').upper() in ('TRUE', '1', 'YES')
        squeeze_fired = _ss(row, 'Keltner_Squeeze_Fired').upper() in ('TRUE', '1', 'YES')

        rv_iv = hv / iv if iv > 0 else 1.0

        if rv_iv >= 1.2 and iv_rank <= 40:
            regime, edge = "COMPRESSED_CHEAP", "FAVORABLE"
            narrative = (
                f"RV/IV {rv_iv:.2f}, IV Rank {iv_rank:.0f} — vol is cheap and compressed. "
                f"Ideal setup for vol expansion plays. "
            )
            if squeeze_on:
                narrative += "Keltner squeeze confirms compression. "
            if squeeze_fired:
                narrative += "Squeeze FIRED — expansion may be starting now. "
        elif iv_rank >= 70:
            regime, edge = "ELEVATED_VOL", "UNFAVORABLE"
            narrative = (
                f"IV Rank {iv_rank:.0f} — vol is already elevated. "
                f"Buying vol at the top of its range risks IV contraction "
                f"working against the position (Natenberg Ch.4)."
            )
        else:
            regime, edge = "NEUTRAL", "NEUTRAL"
            narrative = f"IV Rank {iv_rank:.0f} — mid-range vol, no strong edge."

        return VolContext(regime=regime, edge_direction=edge, narrative=narrative)

    def card_sections(self) -> List[str]:
        return [
            "stock_context", "contract", "entry_pricing", "exit_rules",
            "greeks", "gamma_profile",  # vol-specific
            "risk_profile", "volatility", "squeeze_analysis",  # vol-specific
            "score_breakdown", "thesis",
        ]
