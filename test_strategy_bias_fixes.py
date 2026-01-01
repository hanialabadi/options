#!/usr/bin/env python3
"""
Test Strategy Bias Fixes

Verifies that:
1. Missing Greeks → Watch status (not Valid)
2. Straddles without IV → Penalized
3. Directionals with weak conviction → Penalized
"""

import pandas as pd
import numpy as np
import sys
sys.path.insert(0, '/Users/haniabadi/Documents/Github/options')

from utils.pcs_scoring_v2 import calculate_pcs_score_v2
from core.scan_engine.step11_strategy_pairing import _calculate_goal_alignment

print("="*70)
print("STRATEGY BIAS FIX VERIFICATION")
print("="*70)
print()

# Test 1: Missing Greeks
print("TEST 1: Missing Greeks Penalty")
print("-" * 70)

test_missing_greeks = pd.DataFrame({
    'Strategy': ['Long Call', 'Long Straddle'],
    'Delta': [np.nan, np.nan],
    'Vega': [np.nan, np.nan],
    'Bid_Ask_Spread_Pct': [5.0, 5.0],
    'Open_Interest': [1000, 1000],
    'Actual_DTE': [45, 45],
    'Risk_Model': [500, 1000]
})

result1 = calculate_pcs_score_v2(test_missing_greeks.copy())
print("Strategies without Greeks:")
print(result1[['Strategy', 'PCS_Score_V2', 'PCS_Status', 'PCS_Penalties']].to_string(index=False))
print()

expected_directional = 60  # 100 - 40
expected_vol = 65  # 100 - 35
actual_directional = result1.loc[0, 'PCS_Score_V2']
actual_vol = result1.loc[1, 'PCS_Score_V2']

print(f"✅ PASS: Directional without Greeks = {actual_directional:.0f} pts (expected ~{expected_directional})") if actual_directional < 70 else print(f"❌ FAIL: Too lenient ({actual_directional:.0f} pts)")
print(f"✅ PASS: Straddle without Greeks = {actual_vol:.0f} pts (expected ~{expected_vol})") if actual_vol < 70 else print(f"❌ FAIL: Too lenient ({actual_vol:.0f} pts)")
print()

# Test 2: Straddles with/without IV justification
print("TEST 2: Straddle IV Justification")
print("-" * 70)

test_straddle_iv = pd.DataFrame({
    'Strategy': ['Long Straddle', 'Long Straddle', 'Long Straddle'],
    'Delta': [0.05, 0.05, 0.05],
    'Vega': [0.45, 0.45, 0.45],
    'Theta': [-0.30, -0.30, -0.30],
    'IV_Percentile': [20, 40, np.nan],  # Low IV, Mid IV, No IV
    'Bid_Ask_Spread_Pct': [5.0, 5.0, 5.0],
    'Open_Interest': [1000, 1000, 1000],
    'Actual_DTE': [45, 45, 45],
    'Risk_Model': [1000, 1000, 1000]
})

result2 = calculate_pcs_score_v2(test_straddle_iv.copy())
print("Straddles with different IV contexts:")
print(result2[['IV_Percentile', 'PCS_Score_V2', 'PCS_Status', 'PCS_Penalties']].to_string(index=False))
print()

score_low_iv = result2.loc[0, 'PCS_Score_V2']
score_mid_iv = result2.loc[1, 'PCS_Score_V2']
score_no_iv = result2.loc[2, 'PCS_Score_V2']

print(f"✅ PASS: Low IV (20th %ile) = {score_low_iv:.0f} pts (should be <70)") if score_low_iv < 70 else print(f"❌ FAIL: Should penalize low IV")
print(f"✅ PASS: Mid IV (40th %ile) = {score_mid_iv:.0f} pts (should be >80)") if score_mid_iv > 80 else print(f"❌ FAIL: Should reward mid IV")
print(f"✅ PASS: No IV context = {score_no_iv:.0f} pts (should be <75)") if score_no_iv < 75 else print(f"❌ FAIL: Should penalize missing IV")
print()

# Test 3: Directional conviction
print("TEST 3: Directional Conviction")
print("-" * 70)

test_directional = pd.DataFrame({
    'Strategy': ['Long Call', 'Long Call'],
    'Delta': [0.50, 0.25],  # Strong vs weak
    'Gamma': [0.04, 0.01],  # Positive vs low
    'Vega': [0.25, 0.15],
    'Bid_Ask_Spread_Pct': [5.0, 5.0],
    'Open_Interest': [1000, 1000],
    'Actual_DTE': [45, 45],
    'Risk_Model': [500, 500]
})

result3 = calculate_pcs_score_v2(test_directional.copy())
print("Directionals with different conviction:")
print(result3[['Delta', 'Gamma', 'PCS_Score_V2', 'PCS_Status']].to_string(index=False))
print()

score_strong = result3.loc[0, 'PCS_Score_V2']
score_weak = result3.loc[1, 'PCS_Score_V2']

print(f"✅ PASS: Strong conviction (Delta=0.50, Gamma=0.04) = {score_strong:.0f} pts (should be >85)") if score_strong > 85 else print(f"❌ FAIL: Should reward strong conviction")
print(f"✅ PASS: Weak conviction (Delta=0.25, Gamma=0.01) = {score_weak:.0f} pts (should be <80)") if score_weak < 80 else print(f"❌ FAIL: Should penalize weak conviction")
print()

# Test 4: Step 11 Goal Alignment (volatility goal)
print("TEST 4: Step 11 Goal Alignment (user_goal='volatility')")
print("-" * 70)

test_goal_alignment = pd.DataFrame({
    'Primary_Strategy': ['Long Straddle', 'Long Straddle', 'Long Call'],
    'Vega': [0.45, 0.45, 0.25],
    'Delta': [0.05, 0.05, 0.50],
    'Gamma': [0.04, 0.04, 0.04],
    'IV_Percentile': [40, np.nan, 45]  # Mid IV, No IV, Mid IV
})

alignment_scores = _calculate_goal_alignment(test_goal_alignment, user_goal='volatility')
print("Goal alignment scores (volatility):")
for idx, row in test_goal_alignment.iterrows():
    print(f"  {row['Primary_Strategy']:20s} | IV: {row.get('IV_Percentile', 'N/A'):>5} | Alignment: {alignment_scores.iloc[idx]:.0f}")
print()

straddle_with_iv = alignment_scores.iloc[0]
straddle_no_iv = alignment_scores.iloc[1]
call_with_iv = alignment_scores.iloc[2]

print(f"✅ PASS: Straddle w/ IV = {straddle_with_iv:.0f} (should be >85)") if straddle_with_iv > 85 else print(f"❌ FAIL: Should favor vol strategies with IV")
print(f"✅ PASS: Straddle w/o IV = {straddle_no_iv:.0f} (should be <50)") if straddle_no_iv < 50 else print(f"❌ FAIL: Should penalize vol strategies without IV")
print(f"✅ PASS: Long Call = {call_with_iv:.0f} (should be <60)") if call_with_iv < 60 else print(f"❌ FAIL: Should not favor directionals for vol goal")
print()

print("="*70)
print("VERIFICATION COMPLETE")
print("="*70)
