"""
Thesis quality gate — detects structural signal conflicts on READY contracts.

Pure function: (row_dict) -> (pass, issues, wait_conditions).
No DB, no file I/O, no side effects.

Catches cases like: READY Long Put on a stock with ADX=9, Uptrend structure,
and CONFLICTING weekly — the execution gates passed, but the thesis is hollow.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

# ---------------------------------------------------------------------------
# Thresholds (Murphy / Natenberg backed)
# ---------------------------------------------------------------------------
ADX_MIN_DIRECTIONAL = 15        # Murphy Ch.14: ADX<15 = no trend exists
ADX_PROMOTION_TARGET = 20       # Wait for ADX≥20 to confirm trend emergence
INTERPRETER_WEAK_THRESHOLD = 50 # Below 50/120 = "Weak" interpreter score


def check_thesis_quality(
    row: Dict[str, Any],
) -> Tuple[bool, List[str], List[Dict[str, Any]]]:
    """
    Check whether a READY contract has genuine thesis support.

    Only applies to DIRECTIONAL strategies (Long Call, Long Put, LEAP).
    Income/Volatility strategies have their own gates (income eligibility,
    theory compliance) and are not checked here.

    Args:
        row: Dict-like with Step12 output fields.

    Returns:
        (passed, issues, wait_conditions)
        - passed: True if thesis is sound, False if should demote to CONDITIONAL
        - issues: List of human-readable issue strings
        - wait_conditions: List of condition dicts for wait_condition_generator format
    """
    strategy_type = str(row.get('Strategy_Type') or row.get('strategy_type') or '').upper()
    if strategy_type != 'DIRECTIONAL':
        return True, [], []

    trade_bias = str(row.get('Trade_Bias') or '').upper()
    is_bearish = 'BEAR' in trade_bias
    is_bullish = 'BULL' in trade_bias

    issues: List[str] = []
    conditions: List[Dict[str, Any]] = []

    # ── Hard demotions (any single one triggers) ──────────────────────────

    # 1. ADX < 15: No trend exists (Murphy Ch.14)
    #    Directional bets in trendless markets are pure theta decay.
    adx = _safe_float(row.get('ADX'))
    if adx is not None and adx < ADX_MIN_DIRECTIONAL:
        issues.append(
            f'ADX={adx:.0f} < {ADX_MIN_DIRECTIONAL} — no trend exists '
            f'(Murphy Ch.14: directional bets decay in trendless markets)'
        )
        conditions.append(_technical_condition(
            'ADX', 'greater_than', ADX_PROMOTION_TARGET,
            f'ADX must rise above {ADX_PROMOTION_TARGET} confirming trend emergence '
            f'(currently {adx:.0f}) — Murphy Ch.14'
        ))

    # 2. Market Structure directly opposes Trade Bias
    #    Uptrend + Bearish = fighting swing structure (Murphy Ch.4: HH/HL still intact)
    #    Downtrend + Bullish = fighting swing structure (LH/LL still intact)
    mkt_structure = str(row.get('Market_Structure') or '').strip()
    if mkt_structure and mkt_structure != 'Consolidation':
        structure_bullish = mkt_structure == 'Uptrend'
        structure_bearish = mkt_structure == 'Downtrend'
        if (is_bearish and structure_bullish) or (is_bullish and structure_bearish):
            target_structure = 'Downtrend' if is_bearish else 'Uptrend'
            issues.append(
                f'Market_Structure={mkt_structure} opposes {trade_bias} thesis — '
                f'swing highs/lows still favor the opposite direction (Murphy Ch.4)'
            )
            conditions.append(_technical_condition(
                'Market_Structure', 'equals', target_structure,
                f'Market structure must shift to {target_structure} '
                f'(currently {mkt_structure}) — Murphy Ch.4: trade with the swing'
            ))

    # ── Soft demotions (need 2+ to trigger) ───────────────────────────────

    soft_issues: List[str] = []
    soft_conditions: List[Dict[str, Any]] = []

    # 3. Market structure is Consolidation (no swing confirmation)
    #    Consolidation = no HH/HL or LH/LL sequence — directional thesis has
    #    no structural support.  Not as bad as outright opposition (hard demotion),
    #    but when combined with other soft flags it tips the balance.
    if mkt_structure == 'Consolidation':
        target_structure = 'Downtrend' if is_bearish else 'Uptrend'
        soft_issues.append(
            f'Market_Structure=Consolidation — no swing confirmation for '
            f'{trade_bias} thesis (Murphy Ch.4: need HH/HL or LH/LL sequence)'
        )
        soft_conditions.append(_technical_condition(
            'Market_Structure', 'equals', target_structure,
            f'Market structure must develop {target_structure} swing sequence '
            f'(currently Consolidation) — Murphy Ch.4'
        ))

    # 4. Chart regime is Ranging (price oscillating without trend)
    #    Ranging regime favors mean-reversion / oscillator strategies, not
    #    directional bets that need sustained moves (Murphy Ch.14).
    chart_regime = str(row.get('Chart_Regime') or '').strip()
    if chart_regime == 'Ranging':
        soft_issues.append(
            'Chart_Regime=Ranging — price oscillating without trend; '
            'directional bets decay in range-bound markets (Murphy Ch.14)'
        )
        soft_conditions.append(_technical_condition(
            'Chart_Regime', 'equals', 'Trending',
            'Chart regime must shift to Trending (currently Ranging) — '
            'Murphy Ch.14: directional trades need trend confirmation'
        ))

    # 5. Weekly trend conflicts with daily
    weekly_bias = str(row.get('Weekly_Trend_Bias') or '').upper()
    if weekly_bias == 'CONFLICTING':
        soft_issues.append(
            'Weekly_Trend_Bias=CONFLICTING — higher timeframe does not confirm '
            '(Murphy: weekly filters daily noise)'
        )
        soft_conditions.append(_technical_condition(
            'Weekly_Trend_Bias', 'equals', 'ALIGNED',
            'Weekly trend must align with daily bias — Murphy: weekly filters daily'
        ))

    # 6. Keltner Squeeze ON but not fired (direction unknown)
    squeeze_on = _safe_bool(row.get('Keltner_Squeeze_On'))
    squeeze_fired = _safe_bool(row.get('Keltner_Squeeze_Fired'))
    if squeeze_on and not squeeze_fired:
        soft_issues.append(
            'Keltner Squeeze ON but not fired — coiled energy, '
            'breakout direction unknown (Raschke/Murphy 0.739)'
        )
        soft_conditions.append(_technical_condition(
            'Keltner_Squeeze_Fired', 'equals', True,
            'Keltner Squeeze must fire confirming breakout direction — Raschke/Murphy'
        ))

    # 7. Strategy Interpreter score very weak
    interp_score = _safe_float(row.get('Interp_Score'))
    if interp_score is not None and interp_score < INTERPRETER_WEAK_THRESHOLD:
        interp_max = _safe_float(row.get('Interp_Max')) or 120
        soft_issues.append(
            f'Strategy Interpreter {interp_score:.0f}/{interp_max:.0f} (Weak) — '
            f'multi-signal assessment does not support this trade'
        )
        # No specific condition for interpreter — it'll recalculate on next scan

    # Apply soft demotions if 2+ present
    if len(soft_issues) >= 2:
        issues.extend(soft_issues)
        conditions.extend(soft_conditions)

    passed = len(issues) == 0
    return passed, issues, conditions


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        import math
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


def _safe_bool(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    return str(v).upper() in ('TRUE', '1', 'YES')


def _technical_condition(
    metric: str, operator: str, threshold: Any, description: str,
) -> Dict[str, Any]:
    """Build a condition dict compatible with wait_condition_generator format."""
    import uuid
    return {
        'condition_id': f'thesis_{metric}_{str(uuid.uuid4())[:8]}',
        'type': 'technical',
        'description': description,
        'config': {
            'metric': metric,
            'operator': operator,
            'threshold': threshold,
        },
    }
