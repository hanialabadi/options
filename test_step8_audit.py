"""
Test Step 8 Auditable Decision Records

Verifies that Step 8 produces complete WHY explanations for every trade:
1. WHY this strategy was selected
2. WHY this expiration and strike were chosen
3. WHY liquidity is acceptable (with context)
4. WHY the capital allocation and sizing were approved
5. WHY other strategies for the same ticker were not chosen
"""

import pandas as pd
from core.scan_engine import step8_position_sizing
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

print("="*80)
print("TESTING STEP 8: AUDITABLE DECISION RECORDS")
print("="*80)
print()

# Create mock Step 11 output with complete data for audit
mock_data = []

# Ticker 1: AAPL - Multiple strategies, clear winner
mock_data.extend([
    {
        'Ticker': 'AAPL',
        'Primary_Strategy': 'Long Straddle',
        'Strategy_Rank': 1,
        'Comparison_Score': 78.5,
        'Trade_Bias': 'Neutral',
        'Greeks_Quality_Score': 82,
        'Confidence': 75,
        'Expiration': '2025-02-21',
        'Actual_DTE': 55,
        'Target_DTE': 57,
        'Horizon_Class': 'Short',
        'Strike': 185.0,
        'Underlying_Price': 185.50,
        'Liquidity_Class': 'Excellent',
        'Liquidity_Context': 'High volume ticker with deep ATM liquidity',
        'Open_Interest': 12500,
        'Bid_Ask_Spread_Pct': 3.2,
        'Contract_Selection_Status': 'Success',
        'Total_Debit': 850,
        'Execution_Ready': True,
        'Success_Probability': 0.60
    },
    {
        'Ticker': 'AAPL',
        'Primary_Strategy': 'Long Call',
        'Strategy_Rank': 2,
        'Comparison_Score': 72.0,
        'Trade_Bias': 'Bullish',
        'Greeks_Quality_Score': 70,
        'Confidence': 68,
        'Expiration': '2025-02-21',
        'Actual_DTE': 55,
        'Target_DTE': 42,
        'Horizon_Class': 'Short',
        'Strike': 190.0,
        'Underlying_Price': 185.50,
        'Liquidity_Class': 'Good',
        'Liquidity_Context': 'OTM call with adequate liquidity',
        'Open_Interest': 8500,
        'Bid_Ask_Spread_Pct': 5.8,
        'Contract_Selection_Status': 'Success',
        'Total_Debit': 420,
        'Execution_Ready': True,
        'Success_Probability': 0.55
    }
])

# Ticker 2: BKNG - Expensive LEAP with thin liquidity
mock_data.append({
    'Ticker': 'BKNG',
    'Primary_Strategy': 'Long Call',
    'Strategy_Rank': 1,
    'Comparison_Score': 71.3,
    'Trade_Bias': 'Bullish',
    'Greeks_Quality_Score': 68,
    'Confidence': 70,
    'Expiration': '2026-01-16',
    'Actual_DTE': 385,
    'Target_DTE': 365,
    'Horizon_Class': 'LEAP',
    'Strike': 5500.0,
    'Underlying_Price': 5440.14,
    'Liquidity_Class': 'Thin',
    'Liquidity_Context': 'High-price underlying - wide spreads expected; LEAP horizon - lower liquidity acceptable',
    'Open_Interest': 19,
    'Bid_Ask_Spread_Pct': 17.4,
    'Is_LEAP': True,
    'LEAP_Reason': 'DTE > 365',
    'Contract_Selection_Status': 'Success',
    'Total_Debit': 9560,
    'Execution_Ready': True,
    'Success_Probability': 0.52
})

# Ticker 3: TSLA - Good liquidity, medium allocation
mock_data.append({
    'Ticker': 'TSLA',
    'Primary_Strategy': 'Long Put',
    'Strategy_Rank': 1,
    'Comparison_Score': 74.8,
    'Trade_Bias': 'Bearish',
    'Greeks_Quality_Score': 76,
    'Confidence': 72,
    'Expiration': '2025-03-21',
    'Actual_DTE': 83,
    'Target_DTE': 87,
    'Horizon_Class': 'Medium',
    'Strike': 400.0,
    'Underlying_Price': 415.30,
    'Liquidity_Class': 'Good',
    'Liquidity_Context': 'Adequate OI and spreads for medium-term hold',
    'Open_Interest': 4200,
    'Bid_Ask_Spread_Pct': 7.5,
    'Contract_Selection_Status': 'Success',
    'Total_Debit': 1850,
    'Execution_Ready': True,
    'Success_Probability': 0.58
})

df_mock = pd.DataFrame(mock_data)

print(f"Mock Step 11 output: {len(df_mock)} strategies")
print(f"  Unique tickers: {df_mock['Ticker'].nunique()}")
print(f"  Strategies: {df_mock['Primary_Strategy'].value_counts().to_dict()}")
print()

# Run Step 8 with audit generation
print("Running Step 8 with audit generation...")
print()

df_final = step8_position_sizing.finalize_and_size_positions(
    df_mock,
    account_balance=100000,
    max_positions=10,
    min_comparison_score=65.0,
    sizing_method='fixed_fractional',
    max_trade_risk=0.02
)

print()
print("="*80)
print("VALIDATION RESULTS")
print("="*80)
print()

# Check results
print(f"Final trades: {len(df_final)}")
print(f"Unique tickers: {df_final['Ticker'].nunique()}")
print()

# Verify audit records exist
if 'Selection_Audit' in df_final.columns:
    print("✅ Selection_Audit column present")
    
    # Check completeness
    incomplete = df_final['Selection_Audit'].str.contains('INCOMPLETE', na=True).sum()
    complete = len(df_final) - incomplete
    
    print(f"   Complete audits: {complete}/{len(df_final)}")
    if incomplete > 0:
        print(f"   ⚠️ Incomplete audits: {incomplete}")
    print()
    
    # Show sample audits
    print("SAMPLE AUDIT RECORDS:")
    print("="*80)
    
    for idx, row in df_final.iterrows():
        ticker = row['Ticker']
        strategy = row['Primary_Strategy']
        audit = row['Selection_Audit']
        
        print(f"\n{ticker} - {strategy}:")
        print("-" * 80)
        for line in audit.split('\n'):
            print(f"  {line}")
        print()
    
    # Verify key components present
    print()
    print("AUDIT COMPONENT VERIFICATION:")
    print("-" * 80)
    
    for idx, row in df_final.iterrows():
        ticker = row['Ticker']
        audit = row['Selection_Audit']
        
        components = {
            'STRATEGY SELECTION': 'STRATEGY SELECTION:' in audit,
            'CONTRACT CHOICE': 'CONTRACT CHOICE:' in audit,
            'LIQUIDITY JUSTIFICATION': 'LIQUIDITY JUSTIFICATION:' in audit,
            'CAPITAL ALLOCATION': 'CAPITAL ALLOCATION:' in audit,
            'COMPETITIVE COMPARISON': 'COMPETITIVE COMPARISON:' in audit
        }
        
        all_present = all(components.values())
        status = "✅" if all_present else "❌"
        
        print(f"\n{status} {ticker}:")
        for component, present in components.items():
            symbol = "  ✅" if present else "  ❌"
            print(f"  {symbol} {component}")
    
    print()
    print("="*80)
    if complete == len(df_final):
        print("✅ SUCCESS: All trades have complete auditable decision records")
    else:
        print("❌ FAILURE: Some trades missing required audit components")
    print("="*80)
    
else:
    print("❌ Selection_Audit column MISSING")
    print("   Step 8 did not generate audit records")

# Verify Position_Valid logic
if 'Position_Valid' in df_final.columns:
    print()
    print("Position validation:")
    valid_count = df_final['Position_Valid'].sum()
    print(f"  Valid positions: {valid_count}/{len(df_final)}")
else:
    print("\n⚠️ Position_Valid column missing")
