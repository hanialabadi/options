"""
Strategy Interpreter — Base Class & Shared Types

Architecture: compute signals once (Signal Hub), interpret per strategy.

Four interpreters (not 15):
  DirectionalInterpreter  — Long Call, Long Put
  LeapInterpreter         — Long Call LEAP, Long Put LEAP
  IncomeInterpreter       — CSP, Covered Call, Buy-Write
  VolatilityInterpreter   — Straddle, Strangle

Each interpreter receives the same raw signal row and produces:
  1. ScoredResult — transparent component-by-component breakdown
  2. VolContext   — strategy-specific IV interpretation
  3. card_sections() — which UI sections to render

RAG: STRATEGY_QUALITY_AUDIT.md — strategy-specific evaluation
RAG: EXECUTION_READINESS_GAP_ANALYSIS.md — weight calibration
"""

from __future__ import annotations

import math
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


# ── Data types ────────────────────────────────────────────────────────────────

@dataclass
class ScoredComponent:
    """One scoring factor with transparent rationale."""
    score: float
    max_pts: float
    rationale: str
    rag_source: str = ""

    @property
    def pct(self) -> float:
        return (self.score / self.max_pts * 100) if self.max_pts > 0 else 0.0


@dataclass
class ScoredResult:
    """Complete strategy evaluation with full audit trail."""
    strategy: str
    score: float
    max_possible: float
    status: str                              # Strong / Eligible / Weak / Watch / Rejected
    components: Dict[str, ScoredComponent] = field(default_factory=dict)
    interpretation: str = ""                 # One-sentence strategy-specific summary

    @property
    def pct(self) -> float:
        return (self.score / self.max_possible * 100) if self.max_possible > 0 else 0.0

    def to_breakdown_str(self) -> str:
        """Pipe-delimited component summary for CSV persistence."""
        parts = []
        for name, c in self.components.items():
            parts.append(f"{name}={c.score:.0f}/{c.max_pts:.0f}")
        return " | ".join(parts)

    def to_json(self) -> str:
        """JSON component breakdown for dashboard consumption."""
        return json.dumps({
            k: {"score": c.score, "max": c.max_pts, "rationale": c.rationale,
                "rag": c.rag_source}
            for k, c in self.components.items()
        })


@dataclass
class VolContext:
    """Strategy-specific volatility interpretation."""
    regime: str                # e.g. "CHEAP_VOL", "RICH_VOL", "NEUTRAL"
    edge_direction: str        # "FAVORABLE" / "UNFAVORABLE" / "NEUTRAL"
    narrative: str             # Human-readable explanation
    reconciliation: str = ""   # Explains apparent contradictions (e.g. IV rank vs IV/HV gap)


# ── Safe accessors ────────────────────────────────────────────────────────────

def _sf(row, *keys, default=0.0) -> float:
    """Safe float extraction from row, trying multiple column names."""
    for k in keys:
        v = row.get(k)
        if v is not None and not (isinstance(v, float) and math.isnan(v)):
            try:
                return float(v)
            except (TypeError, ValueError):
                continue
    return default


def _ss(row, *keys, default="") -> str:
    """Safe string extraction from row."""
    for k in keys:
        v = row.get(k)
        if v is not None and str(v).strip().lower() not in ("", "nan", "none"):
            return str(v).strip()
    return default


def _expected_move(row) -> float:
    """1σ expected move = price × IV × √(DTE/365)."""
    price = _sf(row, 'Last', 'UL Last', 'Stock_Price', default=0)
    iv = _sf(row, 'IV_30D', 'IV_Now', 'iv_30d', default=0)
    dte = _sf(row, 'Actual_DTE', 'DTE', default=30)
    if price <= 0 or iv <= 0 or dte <= 0:
        return 0.0
    # IV stored as percentage (e.g. 26.5 = 26.5%)
    iv_dec = iv / 100.0 if iv > 1.0 else iv
    return price * iv_dec * math.sqrt(dte / 365.0)


def _breakeven_distance_pct(row) -> float:
    """Distance from current price to breakeven as % of stock price."""
    price = _sf(row, 'Last', 'UL Last', 'Stock_Price', default=0)
    strike = _sf(row, 'Selected_Strike', 'Strike', default=0)
    premium = _sf(row, 'Mid_Price', 'Mid', 'Premium', default=0)
    strategy = _ss(row, 'Strategy_Name', 'Strategy').lower()

    if price <= 0 or strike <= 0:
        return 0.0

    if 'call' in strategy:
        breakeven = strike + premium
    elif 'put' in strategy:
        breakeven = strike - premium
    else:
        return 0.0

    return abs(breakeven - price) / price * 100.0


# ── Abstract base ─────────────────────────────────────────────────────────────

class StrategyInterpreter(ABC):
    """
    Base interpreter — subclasses implement strategy-specific scoring.

    Usage:
        interpreter = get_interpreter(strategy_name)
        result = interpreter.score(row)
        vol_ctx = interpreter.interpret_volatility(row)
    """

    @property
    @abstractmethod
    def family(self) -> str:
        """Family name: 'directional', 'leap', 'income', 'volatility'."""
        ...

    @property
    @abstractmethod
    def handles(self) -> List[str]:
        """Strategy names this interpreter handles (lowercase)."""
        ...

    @abstractmethod
    def _score_components(self, row: pd.Series, direction: str) -> Dict[str, ScoredComponent]:
        """
        Core scoring logic — return named components.

        Args:
            row: DataFrame row with all signal columns
            direction: 'bullish' or 'bearish' (derived from strategy name)
        """
        ...

    def score(self, row: pd.Series) -> ScoredResult:
        """Score a row and return transparent result."""
        strategy = _ss(row, 'Strategy_Name', 'Strategy')
        direction = self._detect_direction(strategy)

        components = self._score_components(row, direction)

        total = sum(c.score for c in components.values())
        max_possible = sum(c.max_pts for c in components.values())
        pct = (total / max_possible * 100) if max_possible > 0 else 0

        status = self._classify_status(pct)
        interpretation = self._interpret(components, pct, direction, row)

        return ScoredResult(
            strategy=strategy,
            score=total,
            max_possible=max_possible,
            status=status,
            components=components,
            interpretation=interpretation,
        )

    def interpret_volatility(self, row: pd.Series) -> VolContext:
        """Default vol context — subclasses override for strategy-specific interpretation."""
        iv = _sf(row, 'IV_30D', 'IV_Now', 'iv_30d')
        hv = _sf(row, 'HV_20D', 'HV_30D', 'hv_20', 'hv_30')
        iv_rank = _sf(row, 'IV_Rank_30D', 'IV_Rank')
        surface = _ss(row, 'Surface_Shape', default='UNKNOWN')

        gap = iv - hv if (iv > 0 and hv > 0) else 0
        return VolContext(
            regime="NEUTRAL",
            edge_direction="NEUTRAL",
            narrative=f"IV {iv:.1f}% vs HV {hv:.1f}% (gap {gap:+.1f}%), rank {iv_rank:.0f}",
        )

    def card_sections(self) -> List[str]:
        """Default card sections — subclasses can override."""
        return [
            "stock_context", "contract", "entry_pricing", "exit_rules",
            "greeks", "risk_profile", "volatility", "score_breakdown", "thesis",
        ]

    # ── Internal helpers ──────────────────────────────────────────────────

    def _detect_direction(self, strategy: str) -> str:
        s = strategy.lower()
        if any(k in s for k in ('call', 'bull', 'buy-write', 'buy_write', 'covered call')):
            return 'bullish'
        elif any(k in s for k in ('put', 'bear')):
            return 'bearish'
        return 'neutral'

    def _classify_status(self, pct: float) -> str:
        if pct >= 75:
            return "Strong"
        elif pct >= 50:
            return "Eligible"
        else:
            return "Weak"

    def _interpret(self, components: Dict[str, ScoredComponent],
                   pct: float, direction: str, row: pd.Series) -> str:
        """Generate one-sentence strategy summary from weakest components."""
        if not components:
            return ""
        weakest = min(components.values(), key=lambda c: c.pct)
        strongest = max(components.values(), key=lambda c: c.pct)
        return (
            f"Score {pct:.0f}% — strongest: {strongest.rationale}; "
            f"weakest: {weakest.rationale}"
        )
