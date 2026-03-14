"""
Directional Interpreter — Long Call, Long Put (short-dated).

Handles: Long Call, Long Put, Bull Call Spread, Bear Put Spread,
         Call Debit Spread, Put Debit Spread.

Does NOT handle LEAPs — those go to LeapInterpreter (different drivers).

Key differences from existing DQS:
  G1: Gamma responsiveness scored (10 pts) — Passarelli Ch.4
  G2: ADX magnitude weighted, not just tier — Murphy Ch.2
  G3: Expected move vs breakeven coverage — Natenberg Ch.7

Component weights (120 pts total):
  Delta fit          20 pts  — strike selection (Passarelli Ch.4)
  Trend strength     20 pts  — ADX magnitude + EMA structure (Murphy Ch.2)
  Gamma response     10 pts  — convexity for directional trades (Passarelli Ch.4)
  IV entry timing    15 pts  — cheap vs expensive relative to HV (Natenberg Ch.4)
  Spread cost        15 pts  — execution friction (RAG: STRATEGY_QUALITY_AUDIT)
  DTE fit            10 pts  — decay acceleration window
  Momentum timing    15 pts  — RSI, entry context, extension
  Move coverage      15 pts  — expected move vs breakeven (Natenberg Ch.7)
"""

from __future__ import annotations

import math
import logging
from typing import Dict, List

import pandas as pd

from .base import (
    StrategyInterpreter, ScoredComponent, VolContext,
    _sf, _ss, _expected_move, _breakeven_distance_pct,
)

logger = logging.getLogger(__name__)


class DirectionalInterpreter(StrategyInterpreter):

    @property
    def family(self) -> str:
        return "directional"

    @property
    def handles(self) -> List[str]:
        return [
            "long call", "long put",
            "bull call spread", "bear put spread",
            "call debit spread", "put debit spread",
        ]

    def _score_components(self, row: pd.Series, direction: str) -> Dict[str, ScoredComponent]:
        c: Dict[str, ScoredComponent] = {}

        c['delta_fit'] = self._score_delta(row)
        c['trend_strength'] = self._score_trend_strength(row, direction)
        c['gamma_response'] = self._score_gamma(row)
        c['iv_timing'] = self._score_iv_timing(row)
        c['spread_cost'] = self._score_spread(row)
        c['dte_fit'] = self._score_dte(row)
        c['momentum'] = self._score_momentum(row, direction)
        c['move_coverage'] = self._score_move_coverage(row)

        return c

    # ── Delta (20 pts) ────────────────────────────────────────────────────

    def _score_delta(self, row: pd.Series) -> ScoredComponent:
        """Passarelli Ch.4: directional 0.45–0.65 sweet spot."""
        d = abs(_sf(row, 'Delta'))
        if d == 0:
            return ScoredComponent(0, 20, "Delta missing", "Passarelli Ch.4")
        if 0.45 <= d <= 0.65:
            return ScoredComponent(20, 20, f"Δ {d:.2f} — sweet spot", "Passarelli Ch.4")
        elif 0.35 <= d < 0.45:
            pts = 12 + (d - 0.35) / 0.10 * 8  # 12→20 linear
            return ScoredComponent(pts, 20, f"Δ {d:.2f} — slightly low", "Passarelli Ch.4")
        elif 0.65 < d <= 0.75:
            pts = 20 - (d - 0.65) / 0.10 * 5  # 20→15
            return ScoredComponent(pts, 20, f"Δ {d:.2f} — high but valid in strong trend", "Passarelli Ch.4")
        elif 0.75 < d <= 0.85:
            return ScoredComponent(8, 20, f"Δ {d:.2f} — deep ITM, synthetic risk", "Passarelli Ch.4")
        elif d > 0.85:
            return ScoredComponent(3, 20, f"Δ {d:.2f} — essentially stock", "Hull Ch.13")
        else:
            return ScoredComponent(5, 20, f"Δ {d:.2f} — too OTM for directional", "Passarelli Ch.4")

    # ── Trend strength (20 pts) — G2 fix ──────────────────────────────────

    def _score_trend_strength(self, row: pd.Series, direction: str) -> ScoredComponent:
        """Murphy Ch.2: ADX magnitude matters, not just tier."""
        adx = _sf(row, 'adx_14', 'ADX_14', 'ADX')
        weekly = _ss(row, 'Weekly_Trend_Bias').upper()
        mkt_struct = _ss(row, 'Market_Structure').upper()

        pts = 0.0
        notes = []

        # ADX magnitude (0-12 pts) — G2: scaled, not tiered
        if adx >= 40:
            pts += 12
            notes.append(f"ADX {adx:.0f} strong trend")
        elif adx >= 30:
            pts += 8 + (adx - 30) / 10 * 4   # 8→12
            notes.append(f"ADX {adx:.0f} trending")
        elif adx >= 20:
            pts += 3 + (adx - 20) / 10 * 5   # 3→8
            notes.append(f"ADX {adx:.0f} emerging")
        else:
            # Ranging — unfavorable for directional
            pts -= 5
            notes.append(f"ADX {adx:.0f} ranging — directional unfavorable")

        # Weekly trend alignment (0-5 pts)
        if weekly == direction.upper() or (weekly == 'BULLISH' and direction == 'bullish') or (weekly == 'BEARISH' and direction == 'bearish'):
            pts += 5
            notes.append("weekly confirms")
        elif weekly and weekly not in ('', 'NEUTRAL', 'UNKNOWN'):
            pts -= 3
            notes.append(f"weekly opposes ({weekly})")

        # Market structure (0-3 pts)
        if direction == 'bullish' and mkt_struct == 'UPTREND':
            pts += 3
        elif direction == 'bearish' and mkt_struct == 'DOWNTREND':
            pts += 3
        elif mkt_struct == 'CONSOLIDATION':
            pts -= 2

        pts = max(-5, min(20, pts))
        return ScoredComponent(pts, 20, " | ".join(notes) if notes else "No trend data",
                               "Murphy Ch.2")

    # ── Gamma responsiveness (10 pts) — G1 fix ───────────────────────────

    def _score_gamma(self, row: pd.Series) -> ScoredComponent:
        """Passarelli Ch.4: directional trades need gamma to convert moves into P&L."""
        gamma = abs(_sf(row, 'Gamma'))
        price = _sf(row, 'Last', 'UL Last', 'Stock_Price', default=100)

        if gamma == 0:
            return ScoredComponent(0, 10, "Gamma missing", "Passarelli Ch.4")

        # Normalize: dollar-gamma = gamma × price / 100
        # Measures: how much does delta change per 1% stock move
        gamma_norm = gamma * price / 100.0

        if gamma_norm >= 0.04:
            return ScoredComponent(10, 10, f"γ-norm {gamma_norm:.3f} — responsive",
                                   "Passarelli Ch.4: gamma is the directional trader's edge")
        elif gamma_norm >= 0.02:
            pts = 5 + (gamma_norm - 0.02) / 0.02 * 5
            return ScoredComponent(pts, 10, f"γ-norm {gamma_norm:.3f} — moderate",
                                   "Passarelli Ch.4")
        elif gamma_norm >= 0.01:
            return ScoredComponent(3, 10, f"γ-norm {gamma_norm:.3f} — low, needs large move",
                                   "Passarelli Ch.4")
        else:
            return ScoredComponent(0, 10, f"γ-norm {gamma_norm:.4f} — near-zero responsiveness",
                                   "Passarelli Ch.4")

    # ── IV entry timing (15 pts) ──────────────────────────────────────────

    def _score_iv_timing(self, row: pd.Series) -> ScoredComponent:
        """Natenberg Ch.4: prefer buying when IV < HV (cheap vol)."""
        iv = _sf(row, 'IV_30D', 'IV_Now', 'iv_30d')
        hv = _sf(row, 'HV_20D', 'HV_30D', 'hv_20', 'hv_30')
        iv_rank = _sf(row, 'IV_Rank_30D', 'IV_Rank')

        if iv <= 0 or hv <= 0:
            return ScoredComponent(5, 15, "IV/HV data missing — neutral", "Natenberg Ch.4")

        gap_pct = (iv - hv) / hv * 100 if hv > 0 else 0

        # IV < HV = cheap for buyers; IV > HV = expensive
        if gap_pct <= -15:
            return ScoredComponent(15, 15, f"IV {gap_pct:+.0f}% vs HV — very cheap entry",
                                   "Natenberg Ch.4")
        elif gap_pct <= -5:
            return ScoredComponent(12, 15, f"IV {gap_pct:+.0f}% vs HV — favorable",
                                   "Natenberg Ch.4")
        elif gap_pct <= 5:
            return ScoredComponent(8, 15, f"IV ≈ HV — fair entry", "Natenberg Ch.4")
        elif gap_pct <= 15:
            pts = 8 - (gap_pct - 5) / 10 * 5  # 8→3
            return ScoredComponent(max(3, pts), 15,
                                   f"IV {gap_pct:+.0f}% above HV — premium is rich",
                                   "Natenberg Ch.4: buying expensive vol reduces edge")
        else:
            return ScoredComponent(0, 15,
                                   f"IV {gap_pct:+.0f}% above HV — very expensive",
                                   "Natenberg Ch.4")

    # ── Spread cost (15 pts) ──────────────────────────────────────────────

    def _score_spread(self, row: pd.Series) -> ScoredComponent:
        """RAG: STRATEGY_QUALITY_AUDIT — directional max 10% spread."""
        spread_pct = _sf(row, 'Bid_Ask_Spread_Pct')
        oi = _sf(row, 'Open_Interest')

        if spread_pct <= 0:
            return ScoredComponent(10, 15, "Spread data missing — partial credit",
                                   "RAG: STRATEGY_QUALITY_AUDIT")

        pts = 15.0
        notes = []

        if spread_pct <= 3:
            notes.append(f"tight spread {spread_pct:.1f}%")
        elif spread_pct <= 7:
            pts -= (spread_pct - 3) / 4 * 5  # 15→10
            notes.append(f"moderate spread {spread_pct:.1f}%")
        elif spread_pct <= 12:
            pts -= 5 + (spread_pct - 7) / 5 * 5  # 10→5
            notes.append(f"wide spread {spread_pct:.1f}%")
        else:
            pts = 0
            notes.append(f"very wide spread {spread_pct:.1f}%")

        if oi < 100:
            pts = max(0, pts - 3)
            notes.append(f"low OI {oi:.0f}")

        return ScoredComponent(max(0, pts), 15, " | ".join(notes),
                               "RAG: STRATEGY_QUALITY_AUDIT")

    # ── DTE fit (10 pts) ──────────────────────────────────────────────────

    def _score_dte(self, row: pd.Series) -> ScoredComponent:
        """30-60d optimal window; <14d forbidden (RAG hard floor)."""
        dte = _sf(row, 'Actual_DTE', 'DTE', default=0)

        if dte < 14:
            return ScoredComponent(-5, 10, f"DTE {dte:.0f} — below 14d hard floor",
                                   "RAG: STRATEGY_QUALITY_AUDIT:230")
        elif 30 <= dte <= 60:
            return ScoredComponent(10, 10, f"DTE {dte:.0f} — optimal window",
                                   "Passarelli Ch.6: theta acceleration")
        elif 21 <= dte < 30:
            return ScoredComponent(7, 10, f"DTE {dte:.0f} — acceptable",
                                   "Passarelli Ch.6")
        elif 14 <= dte < 21:
            return ScoredComponent(4, 10, f"DTE {dte:.0f} — short, fast theta",
                                   "Passarelli Ch.6")
        elif 60 < dte <= 90:
            return ScoredComponent(8, 10, f"DTE {dte:.0f} — slightly long for directional",
                                   "Passarelli Ch.6")
        else:
            return ScoredComponent(5, 10, f"DTE {dte:.0f} — long for short-dated strategy",
                                   "Passarelli Ch.6")

    # ── Momentum timing (15 pts) ──────────────────────────────────────────

    def _score_momentum(self, row: pd.Series, direction: str) -> ScoredComponent:
        """RSI + entry context + extension risk."""
        rsi = _sf(row, 'rsi_14', 'RSI_14', 'RSI')
        entry_ctx = _ss(row, 'Entry_Timing_Quality').upper()
        ext_pct = abs(_sf(row, 'Price_vs_SMA20'))

        pts = 0.0
        notes = []

        # RSI sweet spot (0-6 pts)
        if direction == 'bullish':
            if 55 <= rsi <= 72:
                pts += 6
                notes.append(f"RSI {rsi:.0f} — bullish sweet spot")
            elif 45 <= rsi < 55:
                pts += 4
            elif rsi > 80:
                pts -= 2
                notes.append(f"RSI {rsi:.0f} — overbought exhaustion risk")
            elif rsi < 30:
                pts += 2
                notes.append(f"RSI {rsi:.0f} — oversold, contrarian for calls")
        else:  # bearish
            if 28 <= rsi <= 45:
                pts += 6
                notes.append(f"RSI {rsi:.0f} — bearish sweet spot")
            elif 45 < rsi <= 55:
                pts += 4
            elif rsi < 20:
                pts -= 2
                notes.append(f"RSI {rsi:.0f} — oversold exhaustion risk")
            elif rsi > 70:
                pts += 2
                notes.append(f"RSI {rsi:.0f} — overbought, contrarian for puts")

        # Entry context (0-5 pts)
        if entry_ctx in ('EARLY', 'MODERATE'):
            pts += 5
            notes.append(f"entry: {entry_ctx}")
        elif entry_ctx in ('LATE_LONG', 'LATE_SHORT'):
            pts -= 2
            notes.append(f"entry: {entry_ctx} — late")

        # Extension risk (0-4 pts)
        if ext_pct <= 3:
            pts += 4
        elif ext_pct <= 6:
            pts += 2
        elif ext_pct > 10:
            pts -= 3
            notes.append(f"extended {ext_pct:.1f}% from SMA20")

        pts = max(-3, min(15, pts))
        return ScoredComponent(pts, 15, " | ".join(notes) if notes else f"RSI {rsi:.0f}",
                               "Murphy Ch.9 + Passarelli Ch.6")

    # ── Move coverage (15 pts) — G3 fix ───────────────────────────────────

    def _score_move_coverage(self, row: pd.Series) -> ScoredComponent:
        """Natenberg Ch.7: compare expected move to breakeven distance."""
        em = _expected_move(row)
        be_dist = _breakeven_distance_pct(row)
        price = _sf(row, 'Last', 'UL Last', 'Stock_Price', default=0)

        if em <= 0 or be_dist <= 0 or price <= 0:
            return ScoredComponent(5, 15, "Move coverage data insufficient — neutral",
                                   "Natenberg Ch.7")

        # em is dollar amount; be_dist is percentage
        em_pct = em / price * 100  # expected move as % of stock price
        coverage = em_pct / be_dist if be_dist > 0 else 0

        if coverage >= 2.0:
            return ScoredComponent(15, 15,
                                   f"Expected move covers breakeven {coverage:.1f}× — excellent",
                                   "Natenberg Ch.7: required move well within 1σ")
        elif coverage >= 1.5:
            return ScoredComponent(12, 15,
                                   f"Expected move covers breakeven {coverage:.1f}× — good",
                                   "Natenberg Ch.7")
        elif coverage >= 1.0:
            return ScoredComponent(8, 15,
                                   f"Expected move covers breakeven {coverage:.1f}× — tight",
                                   "Natenberg Ch.7: breakeven at edge of expected distribution")
        elif coverage >= 0.7:
            return ScoredComponent(3, 15,
                                   f"Expected move covers breakeven {coverage:.1f}× — risky",
                                   "Natenberg Ch.7: breakeven exceeds expected move")
        else:
            return ScoredComponent(0, 15,
                                   f"Expected move covers breakeven {coverage:.1f}× — unrealistic",
                                   "Natenberg Ch.7: breakeven far beyond expected distribution")

    # ── Vol interpretation ────────────────────────────────────────────────

    def interpret_volatility(self, row: pd.Series) -> VolContext:
        """Directional = buying premium → cheap IV is favorable."""
        iv = _sf(row, 'IV_30D', 'IV_Now', 'iv_30d')
        hv = _sf(row, 'HV_20D', 'HV_30D', 'hv_20', 'hv_30')
        iv_rank = _sf(row, 'IV_Rank_30D', 'IV_Rank')
        surface = _ss(row, 'Surface_Shape', default='UNKNOWN')

        gap = iv - hv if (iv > 0 and hv > 0) else 0
        gap_pct = (gap / hv * 100) if hv > 0 else 0

        if gap_pct <= -10:
            regime, edge = "CHEAP_VOL", "FAVORABLE"
            narrative = (f"IV is {abs(gap_pct):.0f}% below HV — you're buying cheap premium. "
                         f"Favorable for directional entry (Natenberg Ch.4).")
        elif gap_pct >= 10:
            regime, edge = "RICH_VOL", "UNFAVORABLE"
            narrative = (f"IV is {gap_pct:.0f}% above HV — premium is expensive. "
                         f"Directional edge requires strong momentum to overcome cost (Natenberg Ch.4).")
        else:
            regime, edge = "NEUTRAL", "NEUTRAL"
            narrative = f"IV roughly equals HV — fair pricing, no vol edge."

        # Reconciliation: IV rank vs gap can appear contradictory
        reconciliation = ""
        if iv_rank > 80 and gap_pct < 0:
            reconciliation = (
                "IV Rank is high (historically elevated) but IV < HV (currently cheap vs realized). "
                "This means: IV is at the top of its historical range, but the stock has been "
                "moving even more than IV implies. Both signals are valid — different timeframes."
            )
        elif iv_rank < 20 and gap_pct > 0:
            reconciliation = (
                "IV Rank is low (historically cheap) but IV > HV (currently rich vs realized). "
                "This means: IV is near historical lows, but the stock has been very calm lately. "
                "The option looks rich vs recent moves but cheap vs history."
            )

        return VolContext(regime=regime, edge_direction=edge,
                          narrative=narrative, reconciliation=reconciliation)
