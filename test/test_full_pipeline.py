import pandas as pd

print('🧪 Full Pipeline Test with Real Tier 1 Recommendations\n')
print('='*70)

# Step 2: Load (with enrichment built-in)
print('\n📊 Step 2: Load Latest Snapshot')
from scan_engine.step2_load_and_enrich_snapshot import load_ivhv_snapshot
df = load_ivhv_snapshot('data/ivhv_archive/ivhv_snapshot_2025-12-26.csv')
print(f'   Loaded: {len(df)} tickers from 2025-12-26 (TODAY)')

# Step 3: Filter
print('\n🔍 Step 3: Filter by IV/HV Gap')
from scan_engine.step3_filter_ivhv import filter_ivhv_gap
df_filtered = filter_ivhv_gap(df, min_gap=2.0)
print(f'   Filtered: {len(df_filtered)} tickers')

# Step 5: Chart (15 tickers for better sample)
print('\n📈 Step 5: Chart Signals (15 tickers)')
from scan_engine import compute_chart_signals
df_charted = compute_chart_signals(df_filtered.head(15))
print(f'   Charted: {len(df_charted)} tickers')

# Step 6: GEM
print('\n💎 Step 6: GEM Filter')
from scan_engine import validate_data_quality
df_gem = validate_data_quality(df_charted)
print(f'   GEM: {len(df_gem)} tickers')

# Step 7: Context
print('\n📋 Step 7: Market Context')
from scan_engine.step7_strategy_recommendation import recommend_strategies
df_context = recommend_strategies(df_gem)
print(f'   Context: {len(df_context)} tickers')

# Step 7B: Multi-Strategy
print('\n🔀 Step 7B: Generate Personalized Strategies')
from scan_engine.step7b_multi_strategy_ranker import generate_multi_strategy_suggestions
df_all = generate_multi_strategy_suggestions(
    df_context,
    max_strategies_per_ticker=6,
    account_size=10000,
    risk_tolerance='Moderate',
    primary_goal='Income'
)

# Separate tiers
tier1 = df_all[df_all['Execution_Ready'] == True].copy()
tier2 = df_all[df_all['Execution_Ready'] == False].copy()

print(f'\n' + '='*70)
print(f'📊 RESULTS FROM TODAY\'S SNAPSHOT (2025-12-26):')
print(f'='*70)
print(f'   Total Strategies: {len(df_all)}')
print(f'   ✅ Tier 1 (Executable): {len(tier1)}')
print(f'   📋 Tier 2+ (Watch List): {len(tier2)}')

# Show Tier 1 recommendations
print(f'\n' + '='*70)
print(f'✅ TIER 1 EXECUTABLE RECOMMENDATIONS:')
print(f'='*70)

for idx, row in tier1.head(10).iterrows():
    print(f'\n📌 Ticker: {row["Ticker"]}')
    print(f'   Strategy: {row["Strategy_Name"]}')
    print(f'   Timeframe: {row["Timeframe_Category"]} ({row.get("Target_DTE_Min", "N/A")}-{row.get("Target_DTE_Max", "N/A")} DTE)')
    print(f'   Risk: {row["Risk_Profile"]} | Tier: {int(row["Strategy_Tier"])} | Broker Approval: Level {row["Broker_Approval_Level"]}')
    print(f'   Win Rate: {row["Success_Probability"]*100:.1f}% | Suitability: {row["Suitability_Score"]:.2f}/10')
    print(f'   Capital: ${row["Capital_Requirement_Est"]:,.0f} ({row["Capital_Requirement_Est"]/100:.1f}% of account)')
    if "Rationale" in row:
        print(f'   Rationale: {row["Rationale"][:100]}')

print(f'\n' + '='*70)
print(f'📋 TIER 1 STRATEGY DISTRIBUTION:')
print(f'='*70)
for strat, count in tier1['Strategy_Name'].value_counts().items():
    print(f'   ✅ {strat}: {count} recs')

print(f'\n' + '='*70)
print(f'📋 TIER 2+ STRATEGIES (Future Capability):')
print(f'='*70)
for strat, count in tier2['Strategy_Name'].value_counts().items():
    blocker = tier2[tier2['Strategy_Name'] == strat]['Execution_Blocker'].iloc[0]
    print(f'   📋 {strat}: {count} recs | Blocker: {blocker}')

# Save for RAG validation
print(f'\n' + '='*70)
print(f'💾 Saving results for RAG validation...')
tier1.to_csv('output/tier1_recommendations_test.csv', index=False)
print(f'   Saved: output/tier1_recommendations_test.csv')
