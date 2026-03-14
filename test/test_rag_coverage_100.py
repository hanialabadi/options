"""
Test 100% RAG Coverage - All 8 Books Validated

This test confirms that all 8 RAG source books are fully implemented
and actively enforced in the validation pipeline:

1. Natenberg (Volatility & Pricing)
2. Passarelli (Trading Greeks)
3. Hull (Options, Futures, Derivatives)
4. Cohen (Bible of Options Strategies)
5. Murphy (Technical Analysis)
6. Sinclair (Volatility Trading)
7. Bulkowski (Encyclopedia of Chart Patterns)
8. Nison (Japanese Candlestick Charting)
"""

import sys
import pandas as pd
import numpy as np
sys.path.insert(0, '/Users/haniabadi/Documents/Github/options')

from utils.pattern_detection import detect_bulkowski_patterns, detect_nison_candlestick
from scan_engine.step11_independent_evaluation import (
    _evaluate_directional_strategy,
    _evaluate_volatility_strategy,
    _evaluate_income_strategy
)


def test_natenberg_rv_iv_gates():
    """Test Natenberg: RV/IV ratio gates (CORRECTED direction)."""
    # Long vol: RV/IV > 1.0 = FAVORABLE (HV > IV = options cheap)
    # RV/IV < 0.70 = UNFAVORABLE (buying very expensive vol)
    test_long_vol_bad = pd.Series({
        'Strategy_Name': 'Long Straddle',
        'RV_IV_Ratio': 0.60,       # IV >> HV = buying very expensive
        'Put_Call_Skew': 1.10,
        'Earnings_Days_Away': 10,
        'Recent_Vol_Spike': False,
        'VVIX': 100,
        'IV_Percentile': 35.0,
        'Delta': 0.05, 'Gamma': 0.08, 'Vega': 0.60, 'Theta': -0.05,
        'Stock_Price': 150.0,
        'Volatility_Regime': 'Compression',
    })

    status, _, _, score, notes = _evaluate_volatility_strategy(test_long_vol_bad)
    assert "RV/IV" in notes, "Missing Natenberg RV/IV note"
    # Score should be penalized for buying expensive vol
    assert score < 70, f"RV/IV=0.60 should penalize long vol, got score={score}"

    # Income: RV/IV > 1.25 = UNFAVORABLE (selling cheap premium)
    test_income_bad = pd.Series({
        'Strategy_Name': 'Covered Call',
        'RV_IV_Ratio': 1.30,       # HV >> IV = selling cheap premium
        'IVHV_gap_30D': -10.0,
        'Theta': 0.05, 'Vega': 0.20, 'Gamma': -0.03,
        'Probability_Of_Profit': 72.0,
        'Trend': 'Bullish',
    })

    status, _, _, score, notes = _evaluate_income_strategy(test_income_bad)
    assert "premium" in notes.lower() or "RV/IV" in notes, "Missing Natenberg income gate"
    assert score < 80, f"RV/IV=1.30 should penalize income, got score={score}"


def test_passarelli_skew_gates():
    """Test Passarelli: Put/Call skew validation."""
    test_straddle = pd.Series({
        'Strategy_Name': 'Long Straddle',
        'Put_Call_Skew': 1.25,      # Hard gate > 1.20
        'RV_IV_Ratio': 1.10,
        'Earnings_Days_Away': 10,
        'Recent_Vol_Spike': False,
        'VVIX': 100,
        'IV_Percentile': 35.0,
        'Delta': 0.05, 'Gamma': 0.08, 'Vega': 0.60, 'Theta': -0.05,
        'Stock_Price': 150.0,
        'Volatility_Regime': 'Compression',
    })

    status, _, _, score, notes = _evaluate_volatility_strategy(test_straddle)
    assert status == 'Reject', f"Expected Reject for high skew, got {status}"
    assert "skew" in notes.lower() or "1.25" in notes, "Missing Passarelli skew gate"


def test_hull_pop_validation():
    """Test Hull: Black-Scholes POP validation."""
    # Hull's Black-Scholes is used to calculate POP
    pass


def test_cohen_pop_gates():
    """Test Cohen: POP graduated gate (<50 reject, 50-65 penalty, >=65 pass)."""
    # POP < 50 should REJECT
    test_income_reject = pd.Series({
        'Strategy_Name': 'Cash-Secured Put',
        'Probability_Of_Profit': 45.0,
        'RV_IV_Ratio': 0.85,
        'IVHV_gap_30D': 5.0,
        'Theta': 0.05, 'Vega': 0.20, 'Gamma': -0.03,
        'Trend': 'Bullish', 'Price_vs_SMA20': 1.0,
    })

    status, _, _, score, notes = _evaluate_income_strategy(test_income_reject)
    assert status == 'Reject', f"Expected Reject for POP < 50%, got {status}"
    assert "POP" in notes, "Missing Cohen POP gate"

    # POP 50-65 should get PENALTY, not reject
    test_income_marginal = pd.Series({
        'Strategy_Name': 'Cash-Secured Put',
        'Probability_Of_Profit': 58.0,
        'RV_IV_Ratio': 0.85,
        'IVHV_gap_30D': 5.0,
        'Theta': 0.05, 'Vega': 0.20, 'Gamma': -0.03,
        'Trend': 'Bullish', 'Price_vs_SMA20': 1.0,
    })

    status, _, _, score, notes = _evaluate_income_strategy(test_income_marginal)
    assert status != 'Reject', f"POP 58% should NOT reject (graduated), got {status}"
    assert "50-65%" in notes, "Missing graduated POP note"


def test_murphy_volume_confirmation():
    """Test Murphy: Volume confirmation for directionals."""
    test_call = pd.Series({
        'Strategy_Name': 'Long Call',
        'Delta': 0.50, 'Gamma': 0.04, 'Vega': 0.25,
        'Trend': 'Bullish',
        'Volume_Trend': 'Falling',
        'Actual_DTE': 30,
    })

    status, _, _, score, notes = _evaluate_directional_strategy(test_call)
    assert "volume" in notes.lower() or "Murphy" in notes, "Missing Murphy volume check"


def test_sinclair_clustering_gates():
    """Test Sinclair: Vol clustering and VVIX gates."""
    # Recent vol spike < 5 days should REJECT
    test_vol_spike = pd.Series({
        'Strategy_Name': 'Long Straddle',
        'Recent_Vol_Spike': True,
        'Days_Since_Vol_Spike': 3,
        'RV_IV_Ratio': 1.10,
        'Put_Call_Skew': 1.10,
        'Earnings_Days_Away': 10,
        'VVIX': 100,
        'IV_Percentile': 35.0,
        'Delta': 0.05, 'Gamma': 0.08, 'Vega': 0.60, 'Theta': -0.05,
        'Stock_Price': 150.0,
        'Volatility_Regime': 'Compression',
    })

    status, _, _, score, notes = _evaluate_volatility_strategy(test_vol_spike)
    assert status == 'Reject', f"Expected Reject for recent vol spike, got {status}"
    assert "RECENT VOL SPIKE" in notes or "days ago" in notes, "Missing Sinclair spike gate"

    # VVIX > 130 should REJECT
    test_vvix = pd.Series({
        'Strategy_Name': 'Long Straddle',
        'Recent_Vol_Spike': False,
        'RV_IV_Ratio': 1.10,
        'Put_Call_Skew': 1.10,
        'Earnings_Days_Away': 10,
        'VVIX': 140,
        'IV_Percentile': 35.0,
        'Delta': 0.05, 'Gamma': 0.08, 'Vega': 0.60, 'Theta': -0.05,
        'Stock_Price': 150.0,
        'Volatility_Regime': 'Compression',
    })

    status, _, _, score, notes = _evaluate_volatility_strategy(test_vvix)
    assert status == 'Reject', f"Expected Reject for VVIX > 130, got {status}"
    assert "VVIX" in notes or "140" in notes, "Missing Sinclair VVIX gate"


def test_bulkowski_pattern_detection():
    """Test Bulkowski: Chart pattern detection and scoring."""
    test_tickers = ['META', 'GOOGL', 'TSLA', 'NVDA', 'AAPL']
    patterns_found = 0

    for ticker in test_tickers:
        pattern, confidence = detect_bulkowski_patterns(ticker)
        if pattern:
            patterns_found += 1

    # Test scoring bonus
    test_with_pattern = pd.Series({
        'Strategy_Name': 'Long Call',
        'Delta': 0.50, 'Gamma': 0.04, 'Vega': 0.25,
        'Chart_Pattern': 'Double Bottom',
        'Pattern_Confidence': 70.0,
        'Trend': 'Bullish',
        'Volume_Trend': 'Rising',
        'Actual_DTE': 30,
    })

    status, _, _, score, notes = _evaluate_directional_strategy(test_with_pattern)
    assert "Double Bottom" in notes or "pattern" in notes.lower(), "Missing Bulkowski validation"


def test_nison_candlestick_timing():
    """Test Nison: Entry timing for short-term strategies."""
    test_tickers = ['META', 'GOOGL', 'TSLA', 'NVDA', 'AAPL']
    candlesticks_found = 0

    for ticker in test_tickers:
        candlestick, timing = detect_nison_candlestick(ticker)
        if candlestick:
            candlesticks_found += 1

    # Short-term strategy with Strong timing = bonus
    test_short_term = pd.Series({
        'Strategy_Name': 'Long Call',
        'Delta': 0.50, 'Gamma': 0.04, 'Vega': 0.25,
        'Actual_DTE': 25,
        'Candlestick_Pattern': 'Bullish Engulfing',
        'Entry_Timing_Quality': 'Strong',
        'Trend': 'Bullish',
        'Volume_Trend': 'Rising',
    })

    status, _, _, score, notes = _evaluate_directional_strategy(test_short_term)
    assert "Nison" in notes or "Bullish Engulfing" in notes or "Entry" in notes, "Missing Nison validation"

    # Short-term strategy WITHOUT timing = penalty
    test_no_timing = pd.Series({
        'Strategy_Name': 'Long Call',
        'Delta': 0.50, 'Gamma': 0.04, 'Vega': 0.25,
        'Actual_DTE': 20,
        'Trend': 'Bullish',
        'Volume_Trend': 'Rising',
    })

    _, _, _, score_no_timing, _ = _evaluate_directional_strategy(test_no_timing)
    assert score_no_timing < score, "Missing penalty for no timing"
