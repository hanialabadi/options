"""
Market Regime Classifier (Diagnostic Only)

Purpose:
    Define expected output ranges for different market conditions.
    Helps distinguish selective (correct 0 trades) from broken (unexpected 0 trades).

Constraints:
    - Does NOT modify pipeline logic
    - Does NOT change acceptance thresholds
    - Does NOT integrate into strategy decisions
    - PURELY diagnostic/validation

Usage:
    >>> regime_info = classify_market_regime(df_step5, df_step3)
    >>> print(f"Regime: {regime_info['regime']}")
    >>> print(f"Expected READY_NOW: {regime_info['expected_ready_range']}")
    >>> # Compare to actual: len(results['acceptance_ready'])
"""

import pandas as pd
import logging
from typing import Dict, Tuple

logger = logging.getLogger(__name__)


def classify_market_regime(df_step5: pd.DataFrame, df_step3: pd.DataFrame) -> Dict:
    """
    Classify current market regime based on existing pipeline signals.
    
    Args:
        df_step5: Step 5 output (chart signals + Phase 1 enrichment)
        df_step3: Step 3 output (IVHV filtered tickers)
    
    Returns:
        dict: {
            'regime': str,
            'confidence': 'LOW' | 'MEDIUM' | 'HIGH',
            'expected_ready_range': (min, max),
            'explanation': str
        }
    
    Regime Types:
        - VOL_EXPANSION_BULL: High IV expansion + bullish positioning
        - VOL_EXPANSION_BEAR: High IV expansion + bearish positioning
        - VOL_CONTRACTION: Low IV, compressed ranges, no premium
        - TREND_BULL: Strong uptrend, low vol, upper positioning
        - TREND_BEAR: Strong downtrend, rising vol, lower positioning
        - CHOP_RANGEBOUND: Mixed signals, no clear direction
        - EARNINGS_CLUSTER: Many tickers in earnings avoid mode
    
    Example Output:
        {
            'regime': 'VOL_EXPANSION_BULL',
            'confidence': 'HIGH',
            'expected_ready_range': (10, 30),
            'explanation': '65% expansion regime + 58% bullish 52w positioning'
        }
    """
    # Validate inputs
    if df_step5.empty or df_step3.empty:
        return {
            'regime': 'INSUFFICIENT_DATA',
            'confidence': 'LOW',
            'expected_ready_range': (0, 0),
            'explanation': 'Empty pipeline outputs - cannot classify regime'
        }
    
    # Extract signal distributions
    signals = _extract_signal_distributions(df_step5, df_step3)
    
    # Apply regime classification rules
    regime_info = _apply_regime_rules(signals)
    
    return regime_info


def _extract_signal_distributions(df_step5: pd.DataFrame, df_step3: pd.DataFrame) -> Dict:
    """
    Extract signal distributions from pipeline outputs.
    
    Returns:
        dict: {
            'expansion_pct': float,
            'contraction_pct': float,
            'stable_pct': float,
            'bullish_52w_pct': float,
            'bearish_52w_pct': float,
            'upper_third_pct': float,
            'lower_third_pct': float,
            'avg_iv_rank': float,
            'avg_gap_size': float,
            'total_tickers': int,
            'step3_pass_rate': float
        }
    """
    signals = {}
    
    # Volatility regime distribution (from Step 3)
    if 'volatility_regime' in df_step3.columns:
        regime_counts = df_step3['volatility_regime'].value_counts()
        total = len(df_step3)
        
        # Map Step 3 regime values to simplified categories
        # Step 3 uses: Normal_Compression, Normal_Contraction, High_Expansion, etc.
        expansion_regimes = [r for r in regime_counts.index if 'Expansion' in r]
        contraction_regimes = [r for r in regime_counts.index if 'Contraction' in r]
        compression_regimes = [r for r in regime_counts.index if 'Compression' in r]
        stable_regimes = [r for r in regime_counts.index if r in ['Normal', 'STABLE']]
        
        expansion_count = sum(regime_counts.get(r, 0) for r in expansion_regimes)
        contraction_count = sum(regime_counts.get(r, 0) for r in contraction_regimes)
        compression_count = sum(regime_counts.get(r, 0) for r in compression_regimes)
        stable_count = sum(regime_counts.get(r, 0) for r in stable_regimes)
        
        signals['expansion_pct'] = expansion_count / total if total > 0 else 0
        signals['contraction_pct'] = (contraction_count + compression_count) / total if total > 0 else 0
        signals['stable_pct'] = stable_count / total if total > 0 else 0
    else:
        signals['expansion_pct'] = 0
        signals['contraction_pct'] = 0
        signals['stable_pct'] = 0
    
    # 52-week regime distribution (from Step 5 Phase 1)
    if '52w_regime_tag' in df_step5.columns:
        bullish_regimes = ['BREAKOUT_BULL', 'NEAR_HIGHS', 'BULL_PULLBACK']
        bearish_regimes = ['BREAKDOWN_BEAR', 'NEAR_LOWS', 'BEAR_RALLY']
        
        bullish_count = df_step5['52w_regime_tag'].isin(bullish_regimes).sum()
        bearish_count = df_step5['52w_regime_tag'].isin(bearish_regimes).sum()
        total = len(df_step5)
        
        signals['bullish_52w_pct'] = bullish_count / total if total > 0 else 0
        signals['bearish_52w_pct'] = bearish_count / total if total > 0 else 0
    else:
        signals['bullish_52w_pct'] = 0
        signals['bearish_52w_pct'] = 0
    
    # Intraday position distribution (from Step 5 Phase 1)
    if 'intraday_position_tag' in df_step5.columns:
        position_counts = df_step5['intraday_position_tag'].value_counts()
        total = len(df_step5)
        signals['upper_third_pct'] = position_counts.get('UPPER_THIRD', 0) / total if total > 0 else 0
        signals['lower_third_pct'] = position_counts.get('LOWER_THIRD', 0) / total if total > 0 else 0
    else:
        signals['upper_third_pct'] = 0
        signals['lower_third_pct'] = 0
    
    # IV metrics (from Step 3)
    if 'IV_Rank_30D' in df_step3.columns:
        signals['avg_iv_rank'] = df_step3['IV_Rank_30D'].mean()
    else:
        signals['avg_iv_rank'] = 0
    
    # Gap metrics (from Step 5 Phase 1)
    if 'gap_tag' in df_step5.columns:
        gap_size_map = {'LARGE_GAP_UP': 3, 'GAP_UP': 2, 'NO_GAP': 1, 'GAP_DOWN': -2, 'LARGE_GAP_DOWN': -3}
        gaps = df_step5['gap_tag'].map(gap_size_map).fillna(0)
        signals['avg_gap_size'] = gaps.mean()
    else:
        signals['avg_gap_size'] = 0
    
    # Funnel metrics
    signals['total_tickers'] = len(df_step5)
    signals['step3_pass_rate'] = len(df_step3) / len(df_step5) if len(df_step5) > 0 else 0
    
    return signals


def _apply_regime_rules(signals: Dict) -> Dict:
    """
    Apply regime classification rules to signal distributions.
    
    Classification Logic:
        1. Check for Vol Expansion regimes (highest priority)
        2. Check for Vol Contraction regimes
        3. Check for Trend regimes (strong directional bias)
        4. Default to Chop/Rangebound
    
    Args:
        signals: Signal distribution dict from _extract_signal_distributions()
    
    Returns:
        dict: Regime classification with expected ranges
    """
    # Extract key signals
    exp_pct = signals['expansion_pct']
    con_pct = signals['contraction_pct']
    bull_pct = signals['bullish_52w_pct']
    bear_pct = signals['bearish_52w_pct']
    upper_pct = signals['upper_third_pct']
    lower_pct = signals['lower_third_pct']
    iv_rank = signals['avg_iv_rank']
    gap_size = signals['avg_gap_size']
    
    # Check if Phase 1 enrichment is available
    has_phase1 = (bull_pct > 0 or bear_pct > 0 or upper_pct > 0 or lower_pct > 0)
    
    # ============================================================
    # SPECIAL CASE: Phase 1 enrichment missing (DATA_INCOMPLETE)
    # ============================================================
    if not has_phase1:
        return {
            'regime': 'DATA_INCOMPLETE',
            'confidence': 'LOW',
            'expected_ready_range': (0, 0),
            'explanation': (
                f'Phase 1 enrichment data missing or insufficient. '
                f'Cannot assess market regime with high confidence.'
            )
        }
    
    # ============================================================
    # RULE 1: VOL_EXPANSION_BULL
    # Conditions: High expansion + bullish positioning + upper third dominance
    # Expected: 10-30 READY_NOW (premium selling paradise)
    # ============================================================
    if exp_pct > 0.60 and bull_pct > 0.50 and upper_pct > 0.40:
        confidence = 'HIGH' if (exp_pct > 0.70 and bull_pct > 0.60) else 'MEDIUM'
        return {
            'regime': 'VOL_EXPANSION_BULL',
            'confidence': confidence,
            'expected_ready_range': (10, 30),
            'explanation': (
                f'{exp_pct:.0%} expansion regime + {bull_pct:.0%} bullish 52w + '
                f'{upper_pct:.0%} upper third positioning. '
                f'High IV + bullish = many premium selling opportunities.'
            )
        }
    
    # ============================================================
    # RULE 2: VOL_EXPANSION_BEAR
    # Conditions: High expansion + bearish positioning + lower third dominance
    # Expected: 0-5 READY_NOW (GEM prefers bull markets)
    # ============================================================
    if exp_pct > 0.60 and bear_pct > 0.50 and lower_pct > 0.40:
        confidence = 'HIGH' if (exp_pct > 0.70 and bear_pct > 0.60) else 'MEDIUM'
        return {
            'regime': 'VOL_EXPANSION_BEAR',
            'confidence': confidence,
            'expected_ready_range': (0, 5),
            'explanation': (
                f'{exp_pct:.0%} expansion regime + {bear_pct:.0%} bearish 52w + '
                f'{lower_pct:.0%} lower third positioning. '
                f'High IV but bearish = limited GEM opportunities (prefers bull).'
            )
        }
    
    # ============================================================
    # RULE 3: VOL_CONTRACTION
    # Conditions: High contraction + low IV rank + small gaps
    # Expected: 0-3 READY_NOW (no premium to sell)
    # ============================================================
    if con_pct > 0.50 and iv_rank < 30:
        confidence = 'HIGH' if (con_pct > 0.60 and iv_rank < 20) else 'MEDIUM'
        return {
            'regime': 'VOL_CONTRACTION',
            'confidence': confidence,
            'expected_ready_range': (0, 3),
            'explanation': (
                f'{con_pct:.0%} contraction regime + {iv_rank:.0f} avg IV rank + '
                f'small gaps ({gap_size:.1f}). '
                f'Low volatility = no premium to sell.'
            )
        }
    
    # ============================================================
    # RULE 4: TREND_BULL
    # Conditions: Strong bullish positioning + upper third + low/stable vol
    # Expected: 5-15 READY_NOW (moderate opportunities)
    # ============================================================
    if bull_pct > 0.60 and upper_pct > 0.50 and exp_pct < 0.40:
        confidence = 'MEDIUM' if bull_pct > 0.70 else 'LOW'
        return {
            'regime': 'TREND_BULL',
            'confidence': confidence,
            'expected_ready_range': (5, 15),
            'explanation': (
                f'{bull_pct:.0%} bullish 52w + {upper_pct:.0%} upper third + '
                f'{exp_pct:.0%} expansion (trending but not explosive). '
                f'Steady uptrend = moderate premium opportunities.'
            )
        }
    
    # ============================================================
    # RULE 5: TREND_BEAR
    # Conditions: Strong bearish positioning + lower third + rising vol
    # Expected: 0-3 READY_NOW (GEM avoids bearish trends)
    # ============================================================
    if bear_pct > 0.60 and lower_pct > 0.50:
        confidence = 'MEDIUM' if bear_pct > 0.70 else 'LOW'
        return {
            'regime': 'TREND_BEAR',
            'confidence': confidence,
            'expected_ready_range': (0, 3),
            'explanation': (
                f'{bear_pct:.0%} bearish 52w + {lower_pct:.0%} lower third. '
                f'Bearish trend = GEM avoids (prefers bullish setups).'
            )
        }
    
    # ============================================================
    # RULE 6: MARKET_EQUILIBRIUM (High Confidence: No Edge)
    # Conditions: Phase 1 data present but balanced (no edge)
    # Expected: 0-5 READY_NOW
    # ============================================================
    return {
        'regime': 'MARKET_EQUILIBRIUM',
        'confidence': 'HIGH',
        'expected_ready_range': (0, 5),
        'explanation': (
            f'Market in equilibrium: {bull_pct:.0%} bullish vs {bear_pct:.0%} bearish. '
            f'Balanced signals indicate high-confidence neutral regime (no statistical edge).'
        )
    }


# ============================================================
# INLINE EXAMPLES (for validation)
# ============================================================

def _example_bull_day():
    """
    Example: Strong Bull Day (Jan 2, 2026)
    
    Characteristics:
        - 72% tickers in EXPANSION regime (VIX spike overnight)
        - 65% tickers bullish 52w (BREAKOUT_BULL, NEAR_HIGHS)
        - 55% tickers in UPPER_THIRD (intraday strength)
        - Avg IV Rank: 62 (high premium)
        - Avg gap: +1.5 (gap ups)
    
    Expected Classification:
        regime: VOL_EXPANSION_BULL
        confidence: HIGH
        expected_ready_range: (10, 30)
    """
    import pandas as pd
    
    # Simulate Step 3 data
    df_step3 = pd.DataFrame({
        'Ticker': ['AAPL', 'NVDA', 'TSLA', 'MSFT', 'META', 'GOOGL', 'AMZN'],
        'volatility_regime': ['EXPANSION', 'EXPANSION', 'EXPANSION', 'EXPANSION', 'EXPANSION', 'STABLE', 'STABLE'],
        'IV_Rank_30D': [68, 72, 55, 60, 70, 45, 50]
    })
    
    # Simulate Step 5 data
    df_step5 = pd.DataFrame({
        'Ticker': ['AAPL', 'NVDA', 'TSLA', 'MSFT', 'META', 'GOOGL', 'AMZN'],
        '52w_regime_tag': ['BREAKOUT_BULL', 'NEAR_HIGHS', 'BREAKOUT_BULL', 'NEAR_HIGHS', 'BULL_PULLBACK', 'MIDRANGE', 'MIDRANGE'],
        'intraday_position_tag': ['UPPER_THIRD', 'UPPER_THIRD', 'UPPER_THIRD', 'UPPER_THIRD', 'MIDDLE', 'UPPER_THIRD', 'MIDDLE'],
        'gap_tag': ['GAP_UP', 'LARGE_GAP_UP', 'GAP_UP', 'GAP_UP', 'NO_GAP', 'GAP_UP', 'NO_GAP']
    })
    
    result = classify_market_regime(df_step5, df_step3)
    
    assert result['regime'] == 'VOL_EXPANSION_BULL', f"Expected VOL_EXPANSION_BULL, got {result['regime']}"
    assert result['confidence'] in ['MEDIUM', 'HIGH'], f"Expected MEDIUM/HIGH confidence, got {result['confidence']}"
    assert result['expected_ready_range'][0] >= 10, f"Expected min 10, got {result['expected_ready_range']}"
    
    print("✅ Bull Day Example Passed")
    print(f"   Regime: {result['regime']}")
    print(f"   Confidence: {result['confidence']}")
    print(f"   Expected Range: {result['expected_ready_range']}")
    print(f"   Explanation: {result['explanation']}\n")


def _example_chop_day():
    """
    Example: Choppy/Rangebound Day
    
    Characteristics:
        - 45% STABLE, 30% EXPANSION, 25% CONTRACTION (mixed)
        - 40% bullish, 35% bearish, 25% midrange (no clear direction)
        - 33% upper, 33% middle, 33% lower (balanced)
        - Avg IV Rank: 35 (moderate)
        - Avg gap: +0.2 (small gaps)
    
    Expected Classification:
        regime: CHOP_RANGEBOUND
        confidence: LOW
        expected_ready_range: (0, 8)
    """
    import pandas as pd
    
    df_step3 = pd.DataFrame({
        'Ticker': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'],
        'volatility_regime': ['STABLE', 'STABLE', 'STABLE', 'STABLE', 'EXPANSION', 'EXPANSION', 'EXPANSION', 'CONTRACTION', 'CONTRACTION', 'STABLE'],
        'IV_Rank_30D': [30, 35, 40, 32, 38, 42, 28, 25, 35, 40]
    })
    
    df_step5 = pd.DataFrame({
        'Ticker': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'],
        '52w_regime_tag': ['BREAKOUT_BULL', 'NEAR_HIGHS', 'MIDRANGE', 'MIDRANGE', 'BREAKDOWN_BEAR', 'NEAR_LOWS', 'BULL_PULLBACK', 'MIDRANGE', 'BEAR_RALLY', 'NEAR_HIGHS'],
        'intraday_position_tag': ['UPPER_THIRD', 'UPPER_THIRD', 'MIDDLE', 'MIDDLE', 'LOWER_THIRD', 'LOWER_THIRD', 'UPPER_THIRD', 'MIDDLE', 'LOWER_THIRD', 'MIDDLE'],
        'gap_tag': ['NO_GAP', 'GAP_UP', 'NO_GAP', 'NO_GAP', 'GAP_DOWN', 'NO_GAP', 'NO_GAP', 'NO_GAP', 'GAP_DOWN', 'GAP_UP']
    })
    
    result = classify_market_regime(df_step5, df_step3)
    
    assert result['regime'] == 'CHOP_RANGEBOUND', f"Expected CHOP_RANGEBOUND, got {result['regime']}"
    assert result['confidence'] == 'LOW', f"Expected LOW confidence, got {result['confidence']}"
    assert result['expected_ready_range'][1] <= 10, f"Expected max 10, got {result['expected_ready_range']}"
    
    print("✅ Chop Day Example Passed")
    print(f"   Regime: {result['regime']}")
    print(f"   Confidence: {result['confidence']}")
    print(f"   Expected Range: {result['expected_ready_range']}")
    print(f"   Explanation: {result['explanation']}\n")


def _example_vol_contraction():
    """
    Example: Volatility Contraction Day
    
    Characteristics:
        - 70% tickers in CONTRACTION regime (VIX sub-12)
        - Low IV ranks (avg 18)
        - Small gaps (avg 0.1)
        - Mixed directional bias
    
    Expected Classification:
        regime: VOL_CONTRACTION
        confidence: HIGH
        expected_ready_range: (0, 3)
    """
    import pandas as pd
    
    df_step3 = pd.DataFrame({
        'Ticker': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'],
        'volatility_regime': ['CONTRACTION'] * 7 + ['STABLE'] * 3,
        'IV_Rank_30D': [15, 18, 12, 20, 16, 22, 14, 25, 28, 19]
    })
    
    df_step5 = pd.DataFrame({
        'Ticker': ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'],
        '52w_regime_tag': ['NEAR_HIGHS', 'MIDRANGE', 'MIDRANGE', 'NEAR_HIGHS', 'MIDRANGE', 'BULL_PULLBACK', 'MIDRANGE', 'MIDRANGE', 'NEAR_LOWS', 'MIDRANGE'],
        'intraday_position_tag': ['MIDDLE', 'MIDDLE', 'UPPER_THIRD', 'MIDDLE', 'MIDDLE', 'UPPER_THIRD', 'MIDDLE', 'LOWER_THIRD', 'LOWER_THIRD', 'MIDDLE'],
        'gap_tag': ['NO_GAP', 'NO_GAP', 'NO_GAP', 'NO_GAP', 'NO_GAP', 'NO_GAP', 'NO_GAP', 'NO_GAP', 'GAP_DOWN', 'NO_GAP']
    })
    
    result = classify_market_regime(df_step5, df_step3)
    
    assert result['regime'] == 'VOL_CONTRACTION', f"Expected VOL_CONTRACTION, got {result['regime']}"
    assert result['confidence'] in ['MEDIUM', 'HIGH'], f"Expected MEDIUM/HIGH confidence, got {result['confidence']}"
    assert result['expected_ready_range'][1] <= 5, f"Expected max 5, got {result['expected_ready_range']}"
    
    print("✅ Vol Contraction Example Passed")
    print(f"   Regime: {result['regime']}")
    print(f"   Confidence: {result['confidence']}")
    print(f"   Expected Range: {result['expected_ready_range']}")
    print(f"   Explanation: {result['explanation']}\n")


if __name__ == '__main__':
    """Run inline examples for validation."""
    print("="*70)
    print("MARKET REGIME CLASSIFIER - INLINE EXAMPLES")
    print("="*70)
    print()
    
    _example_bull_day()
    _example_chop_day()
    _example_vol_contraction()
    
    print("="*70)
    print("ALL EXAMPLES PASSED ✅")
    print("="*70)
