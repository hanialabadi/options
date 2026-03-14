"""
Tests for OI-aware expiration cascade in Step 10 contract selection.

Validates:
1. Cascade not triggered when first expiration passes liquidity
2. Cascade finds liquid contract at alternative expiration
3. LEAP cascade steps down in DTE (nearer = more liquid)
4. Non-LEAP cascade sorts by distance from target
5. min_dte floor respected (LEAPs ≥ 180, non-LEAPs ≥ step9 min)
6. MAX_EXPIRATION_CASCADE honored
7. All expirations fail → final FAILED_LIQUIDITY_FILTER
8. Tracking columns populated correctly
9. OI_FALLBACK status accepted by evaluator precheck
10. Helper: _rank_cascade_expirations ordering
"""

import pytest
import numpy as np
from datetime import datetime, timedelta

from scan_engine.step10_fetch_contracts_schwab import (
    fetch_contracts_for_strategy,
    _rank_cascade_expirations,
    _attempt_single_expiration,
    _select_contract_for_strategy,
    CONTRACT_STATUS_OK,
    CONTRACT_STATUS_OI_FALLBACK,
    CONTRACT_STATUS_LIQUIDITY_FAIL,
    MAX_EXPIRATION_CASCADE,
    calculate_dte,
)


# ── Test chain builders ──────────────────────────────────────────────────


def _future_date(days: int) -> str:
    """Return YYYY-MM-DD string N days from today."""
    return (datetime.today() + timedelta(days=days)).strftime('%Y-%m-%d')


def _build_chain(expirations_config: dict) -> dict:
    """
    Build a minimal Schwab-format chain for testing.

    expirations_config: {
        'YYYY-MM-DD': {
            'calls': {strike: {oi, volume, bid, ask, delta, ...}},
            'puts': {strike: {oi, volume, bid, ask, delta, ...}},
        }
    }
    """
    call_map = {}
    put_map = {}

    for exp_date, sides in expirations_config.items():
        dte = calculate_dte(exp_date)
        exp_key = f'{exp_date}:{dte}'

        for side in ('calls', 'puts'):
            if side not in sides:
                continue
            target_map = call_map if side == 'calls' else put_map
            strikes_dict = {}
            for strike, attrs in sides[side].items():
                strikes_dict[str(float(strike))] = [{
                    'strikePrice': float(strike),
                    'putCall': 'CALL' if side == 'calls' else 'PUT',
                    'delta': attrs.get('delta', 0.50 if side == 'calls' else -0.50),
                    'gamma': attrs.get('gamma', 0.02),
                    'vega': attrs.get('vega', 0.15),
                    'theta': attrs.get('theta', -0.05),
                    'rho': attrs.get('rho', 0.01),
                    'bid': attrs.get('bid', 5.0),
                    'ask': attrs.get('ask', 5.50),
                    'last': attrs.get('last', 5.25),
                    'mark': attrs.get('mark', (attrs.get('bid', 5.0) + attrs.get('ask', 5.50)) / 2),
                    'totalVolume': attrs.get('volume', 100),
                    'openInterest': attrs.get('oi', 500),
                    'volatility': attrs.get('iv', 30.0),
                    'symbol': f'{exp_date}C{strike}' if side == 'calls' else f'{exp_date}P{strike}',
                    'bidSize': 10,
                    'askSize': 10,
                }]
            target_map[exp_key] = strikes_dict

    return {
        'callExpDateMap': call_map,
        'putExpDateMap': put_map,
        'underlyingPrice': 100.0,
    }


def _liquid_strike(strike, side='calls', oi=500, spread_pct=3.0):
    """Shorthand for a liquid strike config."""
    mid = 10.0
    half_spread = mid * spread_pct / 200
    d = 0.50 if side == 'calls' else -0.50
    return {
        strike: {
            'oi': oi, 'volume': 200, 'bid': mid - half_spread,
            'ask': mid + half_spread, 'delta': d,
        }
    }


def _illiquid_strike(strike, side='calls', oi=0, spread_pct=5.0):
    """Shorthand for an illiquid strike config (OI=0)."""
    mid = 10.0
    half_spread = mid * spread_pct / 200
    d = 0.50 if side == 'calls' else -0.50
    return {
        strike: {
            'oi': oi, 'volume': 0, 'bid': mid - half_spread,
            'ask': mid + half_spread, 'delta': d,
        }
    }


# ── 1. Helper: _rank_cascade_expirations ─────────────────────────────────


class TestRankCascadeExpirations:
    """Ordering logic for cascade alternatives."""

    def test_non_leap_sorts_by_distance_from_target(self):
        exp_40 = _future_date(40)
        exp_50 = _future_date(50)
        exp_60 = _future_date(60)
        exp_30 = _future_date(30)

        ranked = _rank_cascade_expirations(
            [exp_30, exp_40, exp_50, exp_60],
            tried={exp_40},
            target_dte=45,
            min_dte=20,
            is_leap=False,
        )
        # exp_50 (5 away) before exp_30 (15 away) before exp_60 (15 away)
        assert ranked[0] == exp_50

    def test_leap_sorts_ascending_dte(self):
        exp_200 = _future_date(200)
        exp_300 = _future_date(300)
        exp_400 = _future_date(400)
        exp_600 = _future_date(600)

        ranked = _rank_cascade_expirations(
            [exp_200, exp_300, exp_400, exp_600],
            tried={exp_600},
            target_dte=500,
            min_dte=180,
            is_leap=True,
        )
        # Nearer first: 200, 300, 400
        assert ranked == [exp_200, exp_300, exp_400]

    def test_respects_min_dte_floor(self):
        exp_10 = _future_date(10)
        exp_30 = _future_date(30)
        exp_50 = _future_date(50)

        ranked = _rank_cascade_expirations(
            [exp_10, exp_30, exp_50],
            tried=set(),
            target_dte=40,
            min_dte=25,
            is_leap=False,
        )
        # exp_10 excluded (below min_dte=25)
        assert exp_10 not in ranked
        assert len(ranked) == 2

    def test_excludes_tried(self):
        exp_40 = _future_date(40)
        exp_50 = _future_date(50)

        ranked = _rank_cascade_expirations(
            [exp_40, exp_50],
            tried={exp_40, exp_50},
            target_dte=45,
            min_dte=20,
            is_leap=False,
        )
        assert ranked == []


# ── 2. Cascade: first expiration passes → no fallback ────────────────────


class TestCascadeNotTriggered:
    """When first expiration is liquid, cascade should not activate."""

    def test_liquid_first_attempt(self):
        exp = _future_date(45)
        chain = _build_chain({
            exp: {
                'calls': _liquid_strike(100),
                'puts': _liquid_strike(100, side='puts'),
            }
        })

        result = fetch_contracts_for_strategy(
            chain, 'TEST', 'Long Call', 'Bullish',
            min_dte=30, max_dte=60, target_dte=45,
            underlying_price=100.0, market_open=True,
        )

        assert result['Contract_Status'] == CONTRACT_STATUS_OK
        assert result['Expiration_Fallback_Used'] is False
        assert result['Expiration_Attempts'] == 1
        assert result['Contract_Selection_Status'] == 'Contracts_Available'


# ── 3. Cascade: first fails OI, second passes ────────────────────────────


class TestCascadeFindsLiquid:
    """Cascade should find liquid contract at alternative expiration."""

    def test_oi_fallback_to_second_expiration(self):
        exp_far = _future_date(45)   # Target — illiquid
        exp_near = _future_date(38)  # Alternative — liquid

        chain = _build_chain({
            exp_far: {
                'calls': _illiquid_strike(100),
                'puts': _illiquid_strike(100, side='puts'),
            },
            exp_near: {
                'calls': _liquid_strike(100),
                'puts': _liquid_strike(100, side='puts'),
            },
        })

        result = fetch_contracts_for_strategy(
            chain, 'TEST', 'Long Call', 'Bullish',
            min_dte=30, max_dte=60, target_dte=45,
            underlying_price=100.0, market_open=True,
        )

        assert result['Contract_Status'] == CONTRACT_STATUS_OI_FALLBACK
        assert result['Expiration_Fallback_Used'] is True
        assert result['Expiration_Attempts'] == 2
        assert result['Selected_Expiration'] == exp_near
        assert result['Contract_Selection_Status'] == 'Contracts_Available'

    def test_put_fallback(self):
        exp_target = _future_date(40)
        exp_alt = _future_date(50)

        chain = _build_chain({
            exp_target: {
                'puts': _illiquid_strike(100, side='puts'),
                'calls': _illiquid_strike(100),
            },
            exp_alt: {
                'puts': _liquid_strike(100, side='puts'),
                'calls': _liquid_strike(100),
            },
        })

        result = fetch_contracts_for_strategy(
            chain, 'TEST', 'Long Put', 'Bearish',
            min_dte=30, max_dte=60, target_dte=40,
            underlying_price=100.0, market_open=True,
        )

        assert result['Contract_Status'] == CONTRACT_STATUS_OI_FALLBACK
        assert result['Selected_Expiration'] == exp_alt


# ── 4. LEAP cascade steps down in DTE ─────────────────────────────────


class TestLeapCascade:
    """LEAP cascade prefers nearer expirations (where liquidity lives)."""

    def test_leap_cascades_to_nearer_dte(self):
        exp_700 = _future_date(700)  # Target — illiquid
        exp_465 = _future_date(465)  # Illiquid
        exp_313 = _future_date(313)  # Liquid

        chain = _build_chain({
            exp_700: {
                'calls': _illiquid_strike(100, oi=0),
                'puts': _illiquid_strike(100, side='puts', oi=0),
            },
            exp_465: {
                'calls': _illiquid_strike(100, oi=2),
                'puts': _illiquid_strike(100, side='puts', oi=2),
            },
            exp_313: {
                'calls': _liquid_strike(100, oi=800),
                'puts': _liquid_strike(100, side='puts', oi=800),
            },
        })

        result = fetch_contracts_for_strategy(
            chain, 'TEST', 'Long Call LEAP', 'Bullish',
            min_dte=365, max_dte=730, target_dte=547,
            underlying_price=100.0, market_open=True,
        )

        # Should cascade to 313 DTE (below 365 target but above 180 floor)
        assert result['Contract_Status'] == CONTRACT_STATUS_OI_FALLBACK
        assert result['Selected_Expiration'] == exp_313
        assert result['Expiration_Fallback_Used'] is True

    def test_leap_respects_180_floor(self):
        exp_700 = _future_date(700)
        exp_100 = _future_date(100)  # Below 180 floor — should be skipped

        chain = _build_chain({
            exp_700: {
                'calls': _illiquid_strike(100, oi=0),
                'puts': _illiquid_strike(100, side='puts', oi=0),
            },
            exp_100: {
                'calls': _liquid_strike(100, oi=1000),
                'puts': _liquid_strike(100, side='puts', oi=1000),
            },
        })

        result = fetch_contracts_for_strategy(
            chain, 'TEST', 'Long Put LEAP', 'Bearish',
            min_dte=365, max_dte=730, target_dte=547,
            underlying_price=100.0, market_open=True,
        )

        # exp_100 is below 180 floor — should NOT be selected
        assert result['Contract_Status'] == CONTRACT_STATUS_LIQUIDITY_FAIL
        assert result['Expiration_Fallback_Used'] is False


# ── 5. MAX_EXPIRATION_CASCADE honored ─────────────────────────────────


class TestCascadeLimit:
    """Cascade should not exceed MAX_EXPIRATION_CASCADE retries."""

    def test_max_retries_honored(self):
        # Create 6 expirations, all illiquid
        exps = [_future_date(30 + i * 7) for i in range(6)]
        config = {}
        for exp in exps:
            config[exp] = {
                'calls': _illiquid_strike(100, oi=0),
                'puts': _illiquid_strike(100, side='puts', oi=0),
            }

        chain = _build_chain(config)

        result = fetch_contracts_for_strategy(
            chain, 'TEST', 'Long Call', 'Bullish',
            min_dte=25, max_dte=90, target_dte=45,
            underlying_price=100.0, market_open=True,
        )

        # 1 initial + MAX_EXPIRATION_CASCADE retries = 4 max
        assert result['Expiration_Attempts'] <= 1 + MAX_EXPIRATION_CASCADE
        assert result['Contract_Status'] == CONTRACT_STATUS_LIQUIDITY_FAIL
        assert result['Expiration_Fallback_Used'] is False


# ── 6. All expirations fail ──────────────────────────────────────────


class TestCascadeExhausted:
    """When all expirations fail liquidity, final status is FAILED_LIQUIDITY_FILTER."""

    def test_all_fail(self):
        exp1 = _future_date(40)
        exp2 = _future_date(50)

        chain = _build_chain({
            exp1: {
                'calls': _illiquid_strike(100, oi=0),
                'puts': _illiquid_strike(100, side='puts', oi=0),
            },
            exp2: {
                'calls': _illiquid_strike(100, oi=1),
                'puts': _illiquid_strike(100, side='puts', oi=1),
            },
        })

        result = fetch_contracts_for_strategy(
            chain, 'TEST', 'Long Call', 'Bullish',
            min_dte=30, max_dte=60, target_dte=45,
            underlying_price=100.0, market_open=True,
        )

        assert result['Contract_Status'] == CONTRACT_STATUS_LIQUIDITY_FAIL
        assert result['Expiration_Fallback_Used'] is False
        assert result['Expiration_Attempts'] == 2


# ── 7. Spread-blocked cascade ────────────────────────────────────────


class TestSpreadCascade:
    """Cascade also helps when the block is wide spread, not just OI."""

    def test_wide_spread_cascades_to_tighter_expiration(self):
        exp_wide = _future_date(45)   # Wide spread
        exp_tight = _future_date(38)  # Tight spread

        chain = _build_chain({
            exp_wide: {
                'calls': {100: {
                    'oi': 50, 'volume': 10, 'bid': 5.0, 'ask': 12.0,
                    'delta': 0.50,
                }},
                'puts': {100: {
                    'oi': 50, 'volume': 10, 'bid': 5.0, 'ask': 12.0,
                    'delta': -0.50,
                }},
            },
            exp_tight: {
                'calls': _liquid_strike(100),
                'puts': _liquid_strike(100, side='puts'),
            },
        })

        result = fetch_contracts_for_strategy(
            chain, 'TEST', 'Long Call', 'Bullish',
            min_dte=30, max_dte=60, target_dte=45,
            underlying_price=100.0, market_open=True,
        )

        assert result['Contract_Status'] == CONTRACT_STATUS_OI_FALLBACK
        assert result['Selected_Expiration'] == exp_tight


# ── 8. Tracking columns ──────────────────────────────────────────────


class TestTrackingColumns:
    """Verify Expiration_Fallback_Used and Expiration_Attempts populated."""

    def test_no_chain_has_tracking(self):
        result = fetch_contracts_for_strategy(
            None, 'TEST', 'Long Call', 'Bullish',
            min_dte=30, max_dte=60, target_dte=45,
            underlying_price=100.0, market_open=True,
        )
        assert result['Expiration_Fallback_Used'] is False
        assert result['Expiration_Attempts'] == 0

    def test_single_attempt_tracking(self):
        exp = _future_date(45)
        chain = _build_chain({
            exp: {
                'calls': _liquid_strike(100),
                'puts': _liquid_strike(100, side='puts'),
            },
        })

        result = fetch_contracts_for_strategy(
            chain, 'TEST', 'Long Call', 'Bullish',
            min_dte=30, max_dte=60, target_dte=45,
            underlying_price=100.0, market_open=True,
        )

        assert result['Expiration_Attempts'] == 1
        assert result['Expiration_Fallback_Used'] is False


# ── 9. OI_FALLBACK accepted by evaluator ──────────────────────────────


class TestEvaluatorAcceptsOIFallback:
    """OI_FALLBACK should pass through evaluator precheck (not rejected)."""

    def test_precheck_returns_none(self):
        import pandas as pd
        from scan_engine.evaluators._shared import contract_status_precheck

        row = pd.Series({'Contract_Status': 'OI_FALLBACK', 'Failure_Reason': '', 'is_market_open': True})
        result = contract_status_precheck(row)
        assert result is None  # None = proceed to evaluation


# ── 10. _attempt_single_expiration unit test ──────────────────────────


class TestAttemptSingleExpiration:
    """Direct tests for the single-attempt helper."""

    def test_liquid_attempt_passes(self):
        exp = _future_date(45)
        chain = _build_chain({
            exp: {
                'calls': _liquid_strike(100, oi=1000),
                'puts': _liquid_strike(100, side='puts', oi=1000),
            },
        })

        attempt = _attempt_single_expiration(
            chain, exp, 'TEST', 'Long Call',
            100.0, True, 'ANY', 'NORMAL',
        )

        assert attempt['_cascade_passed'] is True
        assert attempt['_cascade_failure_type'] is None
        assert attempt['Selected_Expiration'] == exp

    def test_illiquid_attempt_fails_with_oi_type(self):
        exp = _future_date(45)
        chain = _build_chain({
            exp: {
                'calls': _illiquid_strike(100, oi=0),
                'puts': _illiquid_strike(100, side='puts', oi=0),
            },
        })

        attempt = _attempt_single_expiration(
            chain, exp, 'TEST', 'Long Call',
            100.0, True, 'ANY', 'NORMAL',
        )

        assert attempt['_cascade_passed'] is False
        assert attempt['_cascade_failure_type'] == 'oi'


# ── 11. Income strategy cascade ──────────────────────────────────────


class TestIncomeCascade:
    """Income strategies (CSP, Covered Call) also benefit from cascade."""

    def test_csp_cascades(self):
        exp_target = _future_date(35)
        exp_alt = _future_date(45)

        chain = _build_chain({
            exp_target: {
                'puts': {95: {
                    'oi': 0, 'volume': 0, 'bid': 2.0, 'ask': 2.50,
                    'delta': -0.25,
                }},
                'calls': _illiquid_strike(100, oi=0),
            },
            exp_alt: {
                'puts': {95: {
                    'oi': 500, 'volume': 100, 'bid': 3.0, 'ask': 3.15,
                    'delta': -0.25,
                }},
                'calls': _liquid_strike(100),
            },
        })

        result = fetch_contracts_for_strategy(
            chain, 'TEST', 'CSP', 'Bullish',
            min_dte=25, max_dte=60, target_dte=35,
            underlying_price=100.0, market_open=True,
        )

        assert result['Contract_Status'] == CONTRACT_STATUS_OI_FALLBACK
        assert result['Selected_Expiration'] == exp_alt
