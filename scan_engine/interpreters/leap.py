"""
LEAP Interpreter — Long Call LEAP, Long Put LEAP.

Key differences from DirectionalInterpreter:
  - Vega exposure is the primary driver (Hull Ch.26: LEAPs are vega instruments)
  - Trend durability > short-term momentum (Weekly_Trend_Bias, Market_Structure)
  - Long DTE is a FEATURE, not a penalty
  - Gamma is near-zero by nature — not scored
  - Expected move less reliable over multi-year horizons — reduced weight

Component weights (120 pts total):
  Delta fit           20 pts  — ITM preferred for synthetic-stock (Passarelli Ch.8)
  Trend durability    25 pts  — weekly bias, market structure, ADX persistence
  Vega exposure       15 pts  — sensitivity to IV changes (Hull Ch.26)
  IV entry timing     20 pts  — vol regime: buy cheap, avoid rich (Natenberg Ch.4)
  Spread cost         15 pts  — execution friction
  DTE quality         10 pts  — 180-540d window is optimal (Passarelli Ch.8)
  Term structure      15 pts  — surface shape + IV rank context
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


class LeapInterpreter(StrategyInterpreter):

    @property
    def family(self) -> str:
        return "leap"

    @property
    def handles(self) -> List[str]:
        return ["long call leap", "long put leap"]

    def _score_components(self, row: pd.Series, direction: str) -> Dict[str, ScoredComponent]:
        c: Dict[str, ScoredComponent] = {}

        c['delta_fit'] = self._score_delta(row)
        c['trend_durability'] = self._score_trend_durability(row, direction)
        c['vega_exposure'] = self._score_vega(row)
        c['iv_timing'] = self._score_iv_timing(row)
        c['spread_cost'] = self._score_spread(row)
        c['dte_quality'] = self._score_dte(row)
        c['term_structure'] = self._score_term_structure(row)

        return c

    # ── Delta (20 pts) ────────────────────────────────────────────────────

    def _score_delta(self, row: pd.Series) -> ScoredComponent:
        """Passarelli Ch.8: LEAP delta 0.60–0.90 (ITM, synthetic-stock)."""
        d = abs(_sf(row, 'Delta'))
        if d == 0:
            return ScoredComponent(0, 20, "Delta missing", "Passarelli Ch.8")
        if 0.60 <= d <= 0.90:
            return ScoredComponent(20, 20, f"Δ {d:.2f} — LEAP ITM sweet spot",
                                   "Passarelli Ch.8: synthetic-stock exposure")
        elif 0.50 <= d < 0.60:
            return ScoredComponent(14, 20, f"Δ {d:.2f} — slightly below LEAP target",
                                   "Passarelli Ch.8")
        elif d > 0.90:
            return ScoredComponent(8, 20, f"Δ {d:.2f} — deep ITM, low convexity/leverage",
                                   "Hull Ch.26: capital efficiency reduced")
        elif 0.40 <= d < 0.50:
            return ScoredComponent(8, 20, f"Δ {d:.2f} — OTM for LEAP, acceptable",
                                   "Passarelli Ch.8")
        else:
            return ScoredComponent(2, 20, f"Δ {d:.2f} — too OTM, lottery ticket behavior",
                                   "Passarelli Ch.8: deep OTM LEAPs rarely pay off")

    # ── Trend durability (25 pts) — LEAP-specific ─────────────────────────

    def _score_trend_durability(self, row: pd.Series, direction: str) -> ScoredComponent:
        """Weekly trend + market structure + ADX persistence (not short-term RSI)."""
        weekly = _ss(row, 'Weekly_Trend_Bias').upper()
        mkt_struct = _ss(row, 'Market_Structure').upper()
        adx = _sf(row, 'adx_14', 'ADX_14', 'ADX')
        trend_slope = _sf(row, 'Trend_Slope')

        pts = 0.0
        notes = []

        # Weekly trend alignment (0-10 pts) — most important for LEAPs
        # Murphy Ch.1: "weekly trend filters daily noise"
        if direction == 'bullish' and weekly == 'BULLISH':
            pts += 10
            notes.append("weekly trend confirms bullish")
        elif direction == 'bearish' and weekly == 'BEARISH':
            pts += 10
            notes.append("weekly trend confirms bearish")
        elif weekly in ('NEUTRAL', 'UNKNOWN', ''):
            pts += 3
            notes.append("weekly neutral")
        else:
            pts -= 5
            notes.append(f"weekly opposes ({weekly})")

        # Market structure (0-8 pts)
        if direction == 'bullish' and mkt_struct == 'UPTREND':
            pts += 8
            notes.append("HH/HL structure intact")
        elif direction == 'bearish' and mkt_struct == 'DOWNTREND':
            pts += 8
            notes.append("LH/LL structure intact")
        elif mkt_struct == 'CONSOLIDATION':
            pts += 2
            notes.append("consolidation — breakout pending")
        elif mkt_struct:
            pts -= 3

        # ADX persistence (0-5 pts) — sustained trend, not spike
        if adx >= 25:
            pts += 5
            notes.append(f"ADX {adx:.0f} — sustained trend")
        elif adx >= 15:
            pts += 2
        else:
            notes.append(f"ADX {adx:.0f} — weak trend persistence")

        # Trend slope (0-2 pts bonus)
        if direction == 'bullish' and trend_slope > 0.5:
            pts += 2
        elif direction == 'bearish' and trend_slope < -0.5:
            pts += 2

        pts = max(-5, min(25, pts))
        return ScoredComponent(pts, 25, " | ".join(notes) if notes else "No trend data",
                               "Murphy Ch.1 + Hull Ch.26")

    # ── Vega exposure (15 pts) — G4 fix ───────────────────────────────────

    def _score_vega(self, row: pd.Series) -> ScoredComponent:
        """Hull Ch.26: LEAPs are vega instruments — score exposure magnitude."""
        vega = abs(_sf(row, 'Vega'))
        premium = _sf(row, 'Mid_Price', 'Mid', 'Premium', default=0)

        if vega <= 0:
            return ScoredComponent(0, 15, "Vega missing", "Hull Ch.26")

        # Vega as % of premium — how sensitive is the position
        vega_pct = (vega / premium * 100) if premium > 0 else 0

        if vega >= 0.30:
            return ScoredComponent(15, 15,
                                   f"ν={vega:.2f} — high vol sensitivity, LEAP structural advantage",
                                   "Hull Ch.26: long-dated options are vega instruments")
        elif vega >= 0.15:
            pts = 8 + (vega - 0.15) / 0.15 * 7  # 8→15
            return ScoredComponent(pts, 15, f"ν={vega:.2f} — good vol sensitivity",
                                   "Hull Ch.26")
        elif vega >= 0.05:
            return ScoredComponent(5, 15, f"ν={vega:.2f} — moderate", "Hull Ch.26")
        else:
            return ScoredComponent(2, 15, f"ν={vega:.2f} — low, deep ITM near-stock behavior",
                                   "Hull Ch.26")

    # ── IV timing (20 pts) — higher weight for LEAPs ──────────────────────

    def _score_iv_timing(self, row: pd.Series) -> ScoredComponent:
        """Natenberg Ch.4: vol regime matters MORE for LEAPs (longer vega exposure)."""
        iv = _sf(row, 'IV_30D', 'IV_Now', 'iv_30d')
        hv = _sf(row, 'HV_20D', 'HV_30D', 'hv_20', 'hv_30')
        iv_rank = _sf(row, 'IV_Rank_30D', 'IV_Rank')

        if iv <= 0 or hv <= 0:
            return ScoredComponent(8, 20, "IV/HV data missing — neutral", "Natenberg Ch.4")

        gap_pct = (iv - hv) / hv * 100 if hv > 0 else 0

        # For LEAPs, IV rank matters more than IV-HV gap
        # because long-dated vol mean-reverts
        if iv_rank <= 25 and gap_pct <= 0:
            return ScoredComponent(20, 20,
                                   f"IV Rank {iv_rank:.0f} + gap {gap_pct:+.0f}% — ideal LEAP entry",
                                   "Natenberg Ch.4: buy LEAPs in low-vol regime")
        elif iv_rank <= 40:
            return ScoredComponent(15, 20,
                                   f"IV Rank {iv_rank:.0f} — below-average vol, favorable",
                                   "Natenberg Ch.4")
        elif iv_rank <= 60:
            return ScoredComponent(10, 20,
                                   f"IV Rank {iv_rank:.0f} — mid-range vol", "Natenberg Ch.4")
        elif iv_rank <= 80:
            pts = 10 - (iv_rank - 60) / 20 * 6  # 10→4
            return ScoredComponent(max(4, pts), 20,
                                   f"IV Rank {iv_rank:.0f} — elevated, LEAP premium is rich",
                                   "Natenberg Ch.4: avoid buying LEAPs in high-vol regime")
        else:
            return ScoredComponent(0, 20,
                                   f"IV Rank {iv_rank:.0f} — extreme, LEAP will suffer if vol contracts",
                                   "Natenberg Ch.4")

    # ── Spread (15 pts) ───────────────────────────────────────────────────

    def _score_spread(self, row: pd.Series) -> ScoredComponent:
        """LEAP spreads naturally wider — adjust tolerance."""
        spread_pct = _sf(row, 'Bid_Ask_Spread_Pct')
        oi = _sf(row, 'Open_Interest')

        if spread_pct <= 0:
            return ScoredComponent(8, 15, "Spread data missing", "RAG")

        # LEAPs tolerate wider spreads (12% vs 10% for directional)
        if spread_pct <= 5:
            pts = 15
        elif spread_pct <= 8:
            pts = 12
        elif spread_pct <= 12:
            pts = 7
        elif spread_pct <= 18:
            pts = 3
        else:
            pts = 0

        notes = [f"spread {spread_pct:.1f}%"]

        # OI is typically lower for LEAPs
        if oi < 50:
            pts = max(0, pts - 3)
            notes.append(f"low OI {oi:.0f}")

        return ScoredComponent(pts, 15, " | ".join(notes), "RAG: STRATEGY_QUALITY_AUDIT")

    # ── DTE quality (10 pts) — long DTE is a FEATURE ─────────────────────

    def _score_dte(self, row: pd.Series) -> ScoredComponent:
        """Passarelli Ch.8: 180-540d is optimal LEAP window."""
        dte = _sf(row, 'Actual_DTE', 'DTE', default=0)

        if 180 <= dte <= 540:
            return ScoredComponent(10, 10, f"DTE {dte:.0f} — LEAP sweet spot",
                                   "Passarelli Ch.8: optimal time horizon")
        elif 120 <= dte < 180:
            return ScoredComponent(6, 10, f"DTE {dte:.0f} — shorter than ideal LEAP",
                                   "Passarelli Ch.8")
        elif dte > 540:
            return ScoredComponent(7, 10, f"DTE {dte:.0f} — very long, high vega but capital lock-up",
                                   "Passarelli Ch.8")
        elif 90 <= dte < 120:
            return ScoredComponent(3, 10, f"DTE {dte:.0f} — approaching time-stop zone",
                                   "Passarelli Ch.8: roll 3mo before expiry")
        else:
            return ScoredComponent(0, 10, f"DTE {dte:.0f} — too short for LEAP strategy",
                                   "Passarelli Ch.8")

    # ── Term structure (15 pts) ───────────────────────────────────────────

    def _score_term_structure(self, row: pd.Series) -> ScoredComponent:
        """Surface shape + vol regime context for long-dated positioning."""
        surface = _ss(row, 'Surface_Shape').upper()
        iv_rank = _sf(row, 'IV_Rank_30D', 'IV_Rank')

        pts = 5.0  # neutral baseline
        notes = []

        # Surface shape
        if surface == 'CONTANGO':
            pts += 5
            notes.append("contango — normal term structure, LEAP priced in line")
        elif surface == 'BACKWARDATION':
            pts += 2
            notes.append("backwardation — near-term fear premium, LEAP relatively cheaper")
        elif surface == 'FLAT':
            pts += 3
            notes.append("flat — no term structure edge")
        elif surface == 'INVERTED':
            pts -= 2
            notes.append("inverted — unusual, check for structural event")

        # IV rank regime bonus/penalty
        if iv_rank <= 30:
            pts += 5
            notes.append(f"low IV rank {iv_rank:.0f} — vol likely to expand (favorable for long vega)")
        elif iv_rank >= 80:
            pts -= 3
            notes.append(f"high IV rank {iv_rank:.0f} — vol likely to contract (unfavorable for long vega)")

        pts = max(0, min(15, pts))
        return ScoredComponent(pts, 15, " | ".join(notes) if notes else "No vol context",
                               "Hull Ch.26 + Natenberg Ch.12")

    # ── Vol interpretation ────────────────────────────────────────────────

    def interpret_volatility(self, row: pd.Series) -> VolContext:
        """LEAP-specific: vol REGIME matters more than spot IV/HV gap."""
        iv = _sf(row, 'IV_30D', 'IV_Now', 'iv_30d')
        hv = _sf(row, 'HV_20D', 'HV_30D', 'hv_20', 'hv_30')
        iv_rank = _sf(row, 'IV_Rank_30D', 'IV_Rank')
        surface = _ss(row, 'Surface_Shape', default='UNKNOWN')

        if iv_rank <= 30:
            regime, edge = "LOW_VOL_REGIME", "FAVORABLE"
            narrative = (
                f"IV Rank {iv_rank:.0f} — vol is near historical lows. "
                f"LEAPs benefit from vol expansion over their holding period. "
                f"This is the ideal vol regime for LEAP entry (Natenberg Ch.4)."
            )
        elif iv_rank >= 70:
            regime, edge = "HIGH_VOL_REGIME", "UNFAVORABLE"
            narrative = (
                f"IV Rank {iv_rank:.0f} — vol is historically elevated. "
                f"If vol mean-reverts, the LEAP's vega exposure will work against you. "
                f"Consider waiting for vol contraction before LEAP entry (Natenberg Ch.4)."
            )
        else:
            regime, edge = "NEUTRAL_VOL_REGIME", "NEUTRAL"
            narrative = f"IV Rank {iv_rank:.0f} — mid-range vol, no regime edge."

        reconciliation = ""
        if iv_rank > 80 and iv < hv:
            reconciliation = (
                "IV Rank is high (top of its historical range) but IV < HV. "
                "For LEAPs, IV Rank matters more than the IV/HV gap because "
                "long-dated vol tends to mean-revert to its historical range. "
                "The high rank suggests premium is structurally expensive."
            )

        return VolContext(regime=regime, edge_direction=edge,
                          narrative=narrative, reconciliation=reconciliation)

    def card_sections(self) -> List[str]:
        return [
            "stock_context", "contract", "entry_pricing", "exit_rules",
            "greeks", "vega_analysis",  # LEAP-specific
            "risk_profile", "volatility", "term_structure",  # LEAP-specific
            "score_breakdown", "thesis",
        ]
