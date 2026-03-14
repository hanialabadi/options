"""
Income Interpreter — Cash-Secured Put, Covered Call, Buy-Write.

Handles: CSP, Covered Call, Buy-Write, Short Iron Condor, Credit Spread.

Key differences from directional:
  - SELLING premium → rich IV is FAVORABLE (opposite of directional)
  - Theta dominance over vega = structural advantage
  - Premium yield = most important metric (McMillan Ch.2)
  - Assignment dynamics matter (distance to strike, OTM buffer)
  - Gamma is a risk (short gamma), not an edge

Component weights (120 pts total):
  Premium yield       20 pts  — annualized call/put yield (McMillan Ch.2)
  IV richness         20 pts  — IV > HV = rich premium to sell (Natenberg Ch.4)
  Theta dominance     15 pts  — theta/vega ratio (Passarelli Ch.6)
  OTM buffer          15 pts  — distance from strike to stock price
  Spread/liquidity    15 pts  — execution quality
  DTE fit             10 pts  — income DTE window (21-45d optimal)
  Trend safety        15 pts  — assignment risk context
  Assignment dynamics 10 pts  — probability of touch / expiry risk
"""

from __future__ import annotations

import math
import logging
from typing import Dict, List

import pandas as pd

from .base import (
    StrategyInterpreter, ScoredComponent, VolContext,
    _sf, _ss,
)

logger = logging.getLogger(__name__)


class IncomeInterpreter(StrategyInterpreter):

    @property
    def family(self) -> str:
        return "income"

    @property
    def handles(self) -> List[str]:
        return [
            "cash-secured put", "covered call", "buy-write", "pmcc",
            "short iron condor", "credit spread",
            "bull put spread", "bear call spread",
        ]

    def _classify_status(self, pct: float) -> str:
        """Income uses PCS-style thresholds."""
        if pct >= 75:
            return "Valid"
        elif pct >= 50:
            return "Watch"
        else:
            return "Rejected"

    def _score_components(self, row: pd.Series, direction: str) -> Dict[str, ScoredComponent]:
        c: Dict[str, ScoredComponent] = {}
        sub = self._sub_strategy(row)

        c['premium_yield'] = self._score_premium_yield(row, sub)
        c['iv_richness'] = self._score_iv_richness(row)
        c['theta_dominance'] = self._score_theta_dominance(row)
        c['otm_buffer'] = self._score_otm_buffer(row, sub)
        c['spread_liquidity'] = self._score_spread(row)
        c['dte_fit'] = self._score_dte(row)
        c['trend_safety'] = self._score_trend_safety(row, sub)
        c['assignment'] = self._score_assignment(row, sub)

        return c

    def _sub_strategy(self, row: pd.Series) -> str:
        s = _ss(row, 'Strategy_Name', 'Strategy').lower()
        if 'buy-write' in s or 'buy_write' in s:
            return 'buy_write'
        elif 'covered call' in s:
            return 'covered_call'
        elif 'cash-secured' in s or 'cash_secured' in s or 'short put' in s:
            return 'csp'
        elif 'iron condor' in s:
            return 'iron_condor'
        elif 'credit spread' in s or 'bull put' in s or 'bear call' in s:
            return 'credit_spread'
        return 'csp'  # default

    # ── Premium yield (20 pts) — G5 fix ───────────────────────────────────

    def _score_premium_yield(self, row: pd.Series, sub: str) -> ScoredComponent:
        """McMillan Ch.2: annualized premium yield is the core income metric."""
        premium = _sf(row, 'Mid_Price', 'Mid', 'Premium')
        strike = _sf(row, 'Selected_Strike', 'Strike')
        price = _sf(row, 'Last', 'UL Last', 'Stock_Price')
        dte = _sf(row, 'Actual_DTE', 'DTE', default=30)

        if premium <= 0 or dte <= 0:
            return ScoredComponent(5, 20, "Premium data missing", "McMillan Ch.2")

        # Capital base depends on sub-strategy
        if sub == 'buy_write':
            capital = price * 100 if price > 0 else strike * 100
        elif sub == 'csp':
            capital = strike * 100 if strike > 0 else price * 100
        else:
            capital = strike * 100 if strike > 0 else price * 100

        if capital <= 0:
            return ScoredComponent(5, 20, "Capital base unknown", "McMillan Ch.2")

        # Annualized yield
        raw_yield = (premium * 100) / capital  # per-period yield
        ann_yield = raw_yield * (365 / dte) if dte > 0 else 0

        if ann_yield >= 0.15:  # 15%+ annualized
            return ScoredComponent(20, 20,
                                   f"Yield {ann_yield:.1%} ann. — excellent premium",
                                   "McMillan Ch.2: covered call premium as yield enhancement")
        elif ann_yield >= 0.10:
            return ScoredComponent(15, 20, f"Yield {ann_yield:.1%} ann. — strong",
                                   "McMillan Ch.2")
        elif ann_yield >= 0.06:
            return ScoredComponent(10, 20, f"Yield {ann_yield:.1%} ann. — adequate",
                                   "McMillan Ch.2")
        elif ann_yield >= 0.03:
            return ScoredComponent(5, 20, f"Yield {ann_yield:.1%} ann. — thin",
                                   "McMillan Ch.2: minimum yield for income viability")
        else:
            return ScoredComponent(0, 20,
                                   f"Yield {ann_yield:.1%} ann. — insufficient for income strategy",
                                   "McMillan Ch.2")

    # ── IV richness (20 pts) — inverted from directional ──────────────────

    def _score_iv_richness(self, row: pd.Series) -> ScoredComponent:
        """Natenberg Ch.4: sellers WANT IV > HV (rich premium to sell)."""
        iv = _sf(row, 'IV_30D', 'IV_Now', 'iv_30d')
        hv = _sf(row, 'HV_20D', 'HV_30D', 'hv_20', 'hv_30')
        iv_rank = _sf(row, 'IV_Rank_30D', 'IV_Rank')

        if iv <= 0 or hv <= 0:
            return ScoredComponent(8, 20, "IV/HV data missing", "Natenberg Ch.4")

        gap_pct = (iv - hv) / hv * 100 if hv > 0 else 0

        # OPPOSITE of directional: high IV = GOOD for sellers
        if gap_pct >= 20 and iv_rank >= 60:
            return ScoredComponent(20, 20,
                                   f"IV {gap_pct:+.0f}% above HV, rank {iv_rank:.0f} — premium-rich",
                                   "Natenberg Ch.4: ideal for premium selling")
        elif gap_pct >= 10:
            return ScoredComponent(15, 20,
                                   f"IV {gap_pct:+.0f}% above HV — good premium environment",
                                   "Natenberg Ch.4")
        elif gap_pct >= 0:
            return ScoredComponent(10, 20,
                                   f"IV ≈ HV — fair premium", "Natenberg Ch.4")
        elif gap_pct >= -10:
            return ScoredComponent(5, 20,
                                   f"IV {gap_pct:+.0f}% below HV — cheap premium to sell",
                                   "Natenberg Ch.4: selling cheap vol, low edge")
        else:
            return ScoredComponent(0, 20,
                                   f"IV {gap_pct:+.0f}% below HV — vol too cheap to sell",
                                   "Natenberg Ch.4")

    # ── Theta dominance (15 pts) ──────────────────────────────────────────

    def _score_theta_dominance(self, row: pd.Series) -> ScoredComponent:
        """Passarelli Ch.6: income needs theta > vega (decay dominant)."""
        theta = abs(_sf(row, 'Theta'))
        vega = abs(_sf(row, 'Vega'))

        if theta <= 0:
            return ScoredComponent(3, 15, "Theta missing", "Passarelli Ch.6")

        if vega > 0:
            ratio = theta / vega
        else:
            ratio = 2.0  # no vega = pure decay

        if ratio >= 1.5:
            return ScoredComponent(15, 15,
                                   f"θ/ν ratio {ratio:.2f} — strong decay dominance",
                                   "Passarelli Ch.6: time decay is the income engine")
        elif ratio >= 1.0:
            return ScoredComponent(10, 15,
                                   f"θ/ν ratio {ratio:.2f} — decay exceeds vol sensitivity",
                                   "Passarelli Ch.6")
        elif ratio >= 0.5:
            return ScoredComponent(5, 15,
                                   f"θ/ν ratio {ratio:.2f} — vol sensitivity exceeds decay",
                                   "Passarelli Ch.6: position is more vol-play than income")
        else:
            return ScoredComponent(0, 15,
                                   f"θ/ν ratio {ratio:.2f} — vega-dominated, not income structure",
                                   "Passarelli Ch.6")

    # ── OTM buffer (15 pts) ───────────────────────────────────────────────

    def _score_otm_buffer(self, row: pd.Series, sub: str) -> ScoredComponent:
        """Distance from stock to strike — safety margin for premium sellers."""
        price = _sf(row, 'Last', 'UL Last', 'Stock_Price')
        strike = _sf(row, 'Selected_Strike', 'Strike')
        delta = abs(_sf(row, 'Delta'))

        if price <= 0 or strike <= 0:
            return ScoredComponent(5, 15, "Price/strike data missing", "McMillan Ch.7")

        if sub in ('csp', 'credit_spread'):
            # Put: buffer = how far below stock the strike sits
            buffer_pct = (price - strike) / price * 100
        else:
            # Call: buffer = how far above stock the strike sits
            buffer_pct = (strike - price) / price * 100

        if buffer_pct >= 8:
            return ScoredComponent(15, 15,
                                   f"Buffer {buffer_pct:.1f}% OTM — comfortable safety margin",
                                   "McMillan Ch.7: probability of profit favors wide buffer")
        elif buffer_pct >= 5:
            return ScoredComponent(12, 15, f"Buffer {buffer_pct:.1f}% OTM — moderate",
                                   "McMillan Ch.7")
        elif buffer_pct >= 2:
            return ScoredComponent(8, 15, f"Buffer {buffer_pct:.1f}% OTM — tight",
                                   "McMillan Ch.7")
        elif buffer_pct >= 0:
            return ScoredComponent(4, 15, f"Buffer {buffer_pct:.1f}% — at-the-money",
                                   "McMillan Ch.7: ATM has highest assignment risk")
        else:
            return ScoredComponent(0, 15, f"ITM by {abs(buffer_pct):.1f}% — assignment likely",
                                   "McMillan Ch.7")

    # ── Spread/liquidity (15 pts) ─────────────────────────────────────────

    def _score_spread(self, row: pd.Series) -> ScoredComponent:
        """Income tolerates slightly wider spreads (12% max)."""
        spread_pct = _sf(row, 'Bid_Ask_Spread_Pct')
        oi = _sf(row, 'Open_Interest')

        if spread_pct <= 0:
            return ScoredComponent(8, 15, "Spread data missing", "RAG")

        pts = 15.0
        notes = []

        if spread_pct <= 4:
            notes.append(f"tight spread {spread_pct:.1f}%")
        elif spread_pct <= 8:
            pts -= (spread_pct - 4) / 4 * 3
            notes.append(f"moderate spread {spread_pct:.1f}%")
        elif spread_pct <= 12:
            pts -= 3 + (spread_pct - 8) / 4 * 5
            notes.append(f"wide spread {spread_pct:.1f}%")
        else:
            pts = 2
            notes.append(f"very wide spread {spread_pct:.1f}%")

        if oi >= 500:
            pts = min(15, pts + 2)
            notes.append(f"strong OI {oi:.0f}")
        elif oi < 100:
            pts = max(0, pts - 3)
            notes.append(f"low OI {oi:.0f}")

        return ScoredComponent(max(0, pts), 15, " | ".join(notes), "RAG: STRATEGY_QUALITY_AUDIT")

    # ── DTE fit (10 pts) ──────────────────────────────────────────────────

    def _score_dte(self, row: pd.Series) -> ScoredComponent:
        """Income optimal: 21-45d (theta acceleration without too much gamma)."""
        dte = _sf(row, 'Actual_DTE', 'DTE', default=0)

        if 21 <= dte <= 45:
            return ScoredComponent(10, 10, f"DTE {dte:.0f} — income sweet spot",
                                   "Passarelli Ch.6: peak theta decay")
        elif 14 <= dte < 21:
            return ScoredComponent(7, 10, f"DTE {dte:.0f} — short but fast decay",
                                   "Passarelli Ch.6")
        elif 5 <= dte < 14:
            return ScoredComponent(4, 10, f"DTE {dte:.0f} — very short, gamma risk",
                                   "Natenberg Ch.15")
        elif 45 < dte <= 60:
            return ScoredComponent(7, 10, f"DTE {dte:.0f} — slightly long", "Passarelli Ch.6")
        elif dte < 5:
            return ScoredComponent(0, 10, f"DTE {dte:.0f} — expiry week, pure gamma",
                                   "Natenberg Ch.15")
        else:
            return ScoredComponent(4, 10, f"DTE {dte:.0f} — too long for income cycle",
                                   "Passarelli Ch.6")

    # ── Trend safety (15 pts) ─────────────────────────────────────────────

    def _score_trend_safety(self, row: pd.Series, sub: str) -> ScoredComponent:
        """Income prefers sideways/mild-trend; strong trends risk assignment."""
        adx = _sf(row, 'adx_14', 'ADX_14', 'ADX')
        mkt_struct = _ss(row, 'Market_Structure').upper()
        chart_regime = _ss(row, 'Chart_Regime').upper()

        pts = 0.0
        notes = []

        # ADX — ranging is GOOD for income (opposite of directional)
        if adx < 20:
            pts += 10
            notes.append(f"ADX {adx:.0f} — ranging, ideal for premium selling")
        elif adx < 30:
            pts += 6
            notes.append(f"ADX {adx:.0f} — moderate trend, manageable")
        elif adx < 40:
            pts += 2
            notes.append(f"ADX {adx:.0f} — trending, higher assignment risk")
        else:
            pts -= 3
            notes.append(f"ADX {adx:.0f} — strong trend, risky for short premium")

        # Market structure alignment
        if sub in ('csp', 'credit_spread'):
            # CSP: uptrend is safe (stock moving away from put strike)
            if mkt_struct == 'UPTREND':
                pts += 5
                notes.append("uptrend — stock moving away from put strike")
            elif mkt_struct == 'DOWNTREND':
                pts -= 3
                notes.append("downtrend — stock moving toward put strike")
        else:
            # Covered call / buy-write: sideways is best
            if mkt_struct == 'CONSOLIDATION':
                pts += 5
                notes.append("consolidation — ideal for covered calls")
            elif mkt_struct == 'UPTREND':
                pts += 2
                notes.append("uptrend — stock may be called away")

        pts = max(-3, min(15, pts))
        return ScoredComponent(pts, 15, " | ".join(notes) if notes else f"ADX {adx:.0f}",
                               "Murphy Ch.2 + McMillan Ch.2")

    # ── Assignment dynamics (10 pts) ──────────────────────────────────────

    def _score_assignment(self, row: pd.Series, sub: str) -> ScoredComponent:
        """McMillan Ch.7: assignment probability context."""
        delta = abs(_sf(row, 'Delta'))
        moneyness = _ss(row, 'Moneyness_Label').upper()
        mc_assign = _sf(row, 'MC_Assign_P_Expiry')

        pts = 5.0  # neutral baseline
        notes = []

        # Use MC probability if available
        if mc_assign > 0:
            if mc_assign < 0.15:
                pts += 5
                notes.append(f"P(assign) {mc_assign:.0%} — low")
            elif mc_assign < 0.35:
                pts += 2
                notes.append(f"P(assign) {mc_assign:.0%} — moderate")
            elif mc_assign < 0.60:
                pts -= 2
                notes.append(f"P(assign) {mc_assign:.0%} — elevated")
            else:
                pts -= 5
                notes.append(f"P(assign) {mc_assign:.0%} — high, expect assignment")
        else:
            # Delta-based fallback
            if delta < 0.20:
                pts += 3
                notes.append(f"Δ {delta:.2f} — comfortably OTM")
            elif delta < 0.35:
                pts += 1
            elif delta > 0.50:
                pts -= 3
                notes.append(f"Δ {delta:.2f} — ITM, assignment probable")

        if moneyness == 'ITM':
            pts -= 2
            notes.append("currently ITM")

        pts = max(0, min(10, pts))
        return ScoredComponent(pts, 10, " | ".join(notes) if notes else "No assignment data",
                               "McMillan Ch.7 + Natenberg Ch.19")

    # ── Vol interpretation ────────────────────────────────────────────────

    def interpret_volatility(self, row: pd.Series) -> VolContext:
        """Income = selling premium → rich IV is FAVORABLE."""
        iv = _sf(row, 'IV_30D', 'IV_Now', 'iv_30d')
        hv = _sf(row, 'HV_20D', 'HV_30D', 'hv_20', 'hv_30')
        iv_rank = _sf(row, 'IV_Rank_30D', 'IV_Rank')

        gap = iv - hv if (iv > 0 and hv > 0) else 0
        gap_pct = (gap / hv * 100) if hv > 0 else 0

        if gap_pct >= 10 and iv_rank >= 50:
            regime, edge = "RICH_VOL", "FAVORABLE"
            narrative = (
                f"IV is {gap_pct:.0f}% above HV with rank {iv_rank:.0f} — "
                f"premium is rich, ideal for selling. Time decay works in your favor "
                f"as long as realized vol stays below implied (Natenberg Ch.4)."
            )
        elif gap_pct <= -10:
            regime, edge = "CHEAP_VOL", "UNFAVORABLE"
            narrative = (
                f"IV is {abs(gap_pct):.0f}% below HV — premium is cheap. "
                f"Selling cheap options means thin yield and higher probability of realized "
                f"vol exceeding implied. Wait for vol expansion (Natenberg Ch.4)."
            )
        else:
            regime, edge = "NEUTRAL", "NEUTRAL"
            narrative = f"IV roughly equals HV — fair premium pricing."

        return VolContext(regime=regime, edge_direction=edge, narrative=narrative)

    def card_sections(self) -> List[str]:
        return [
            "stock_context", "contract", "entry_pricing", "yield_analysis",
            "exit_rules", "greeks", "risk_profile", "assignment_profile",
            "volatility", "score_breakdown", "thesis",
        ]
