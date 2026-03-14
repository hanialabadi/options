"""
Strategy Interpreter Registry

Maps strategy names → interpreter instances (4 total):
  DirectionalInterpreter  — Long Call, Long Put, spreads
  LeapInterpreter         — Long Call LEAP, Long Put LEAP
  IncomeInterpreter       — CSP, Covered Call, Buy-Write
  VolatilityInterpreter   — Straddle, Strangle

Usage:
    from scan_engine.interpreters import get_interpreter
    interp = get_interpreter("Long Call LEAP")
    result = interp.score(row)
    vol_ctx = interp.interpret_volatility(row)
"""

from .base import StrategyInterpreter, ScoredResult, ScoredComponent, VolContext
from .directional import DirectionalInterpreter
from .leap import LeapInterpreter
from .income import IncomeInterpreter
from .volatility import VolatilityInterpreter

# Singleton instances
_DIRECTIONAL = DirectionalInterpreter()
_LEAP = LeapInterpreter()
_INCOME = IncomeInterpreter()
_VOLATILITY = VolatilityInterpreter()

# Build lookup: strategy_name (lowercase) → interpreter
_REGISTRY: dict[str, StrategyInterpreter] = {}
for _interp in (_DIRECTIONAL, _LEAP, _INCOME, _VOLATILITY):
    for _name in _interp.handles:
        _REGISTRY[_name] = _interp


def get_interpreter(strategy_name: str) -> StrategyInterpreter:
    """
    Return the interpreter for a given strategy name.

    Falls back to DirectionalInterpreter if strategy is unknown.
    """
    key = str(strategy_name or '').strip().lower()
    return _REGISTRY.get(key, _DIRECTIONAL)


def get_all_interpreters() -> list[StrategyInterpreter]:
    """Return all interpreter instances (for testing)."""
    return [_DIRECTIONAL, _LEAP, _INCOME, _VOLATILITY]


__all__ = [
    'get_interpreter', 'get_all_interpreters',
    'StrategyInterpreter', 'ScoredResult', 'ScoredComponent', 'VolContext',
    'DirectionalInterpreter', 'LeapInterpreter',
    'IncomeInterpreter', 'VolatilityInterpreter',
]
