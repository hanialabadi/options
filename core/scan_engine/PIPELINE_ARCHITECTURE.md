# Scan Engine Pipeline Architecture

## Complete Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                    SCAN ENGINE PIPELINE                              │
│                                                                       │
│  DESCRIPTIVE PHASE (Steps 2-6): WHAT IS                             │
│  ────────────────────────────────────────────                        │
│                                                                       │
│  ┌──────────────────────────────────────────────────┐               │
│  │ Step 2: Load & Enrich IV/HV Snapshot             │               │
│  │ ─────────────────────────────────────            │               │
│  │ Input: Raw Fidelity CSV (175 tickers)            │               │
│  │ Output: + IV_Rank_30D, IV_Term_Structure,        │               │
│  │          IV_Trend_7D, HV_Trend_30D                │               │
│  │ Purpose: Enrich with per-ticker IV percentiles    │               │
│  └──────────────────────────────────────────────────┘               │
│                          ↓                                           │
│  ┌──────────────────────────────────────────────────┐               │
│  │ Step 3: Filter IV-HV Gap & Classify Regimes      │               │
│  │ ────────────────────────────────────────         │               │
│  │ Input: Enriched snapshot (175 tickers)           │               │
│  │ Output: Filtered (127 tickers)                   │               │
│  │         + IV_Rich, IV_Cheap, MeanReversion_Setup,│               │
│  │           Expansion_Setup, IVHV_gap_abs          │               │
│  │ Purpose: Bidirectional volatility pattern detect │               │
│  └──────────────────────────────────────────────────┘               │
│                          ↓                                           │
│  ┌──────────────────────────────────────────────────┐               │
│  │ Step 5: Compute Chart Signals & Regimes          │               │
│  │ ────────────────────────────────────────         │               │
│  │ Input: Filtered (127 tickers)                    │               │
│  │ Output: Charted (46 tickers with price data)     │               │
│  │         + Regime, EMA_Signal, Signal_Type,       │               │
│  │           Days_Since_Cross, Trend_Slope, Atr_Pct │               │
│  │ Purpose: Technical indicators (descriptive only) │               │
│  └──────────────────────────────────────────────────┘               │
│                          ↓                                           │
│  ┌──────────────────────────────────────────────────┐               │
│  │ Step 6: Validate Data Completeness               │               │
│  │ ───────────────────────────────────              │               │
│  │ Input: Charted (46 tickers)                      │               │
│  │ Output: Validated (ALL 46 tickers returned)      │               │
│  │         + Data_Complete, Crossover_Age_Bucket    │               │
│  │ Purpose: Quality checks (no filtering)           │               │
│  └──────────────────────────────────────────────────┘               │
│                          ↓                                           │
│  ════════════════════════════════════════════════════               │
│                                                                       │
│  PRESCRIPTIVE PHASE (Step 7+): WHAT TO DO                           │
│  ──────────────────────────────────────────                          │
│                                                                       │
│  ┌──────────────────────────────────────────────────┐               │
│  │ Step 7: Recommend Strategies                     │               │
│  │ ────────────────────────────                     │               │
│  │ Input: Validated (46 tickers)                    │               │
│  │ Output: Recommendations (with strategies)        │               │
│  │         + Primary_Strategy, Secondary_Strategy,  │               │
│  │           Strategy_Type, Confidence,             │               │
│  │           Success_Probability, Trade_Bias,       │               │
│  │           Entry_Priority, Risk_Level             │               │
│  │ Purpose: Strategy selection & confidence scoring │               │
│  └──────────────────────────────────────────────────┘               │
│                          ↓                                           │
│  ┌──────────────────────────────────────────────────┐               │
│  │ Step 8: Position Sizing (Future)                 │               │
│  │ ────────────────────────────                     │               │
│  │ - Kelly Criterion                                │               │
│  │ - Fixed fractional                               │               │
│  │ - Volatility-based sizing                        │               │
│  └──────────────────────────────────────────────────┘               │
│                          ↓                                           │
│  ┌──────────────────────────────────────────────────┐               │
│  │ Step 9: Risk Management (Future)                 │               │
│  │ ───────────────────────────                      │               │
│  │ - Portfolio heat limits                          │               │
│  │ - Correlation checks                             │               │
│  │ - Max loss per trade                             │               │
│  └──────────────────────────────────────────────────┘               │
│                          ↓                                           │
│  ┌──────────────────────────────────────────────────┐               │
│  │ Step 10: Order Generation (Future)               │               │
│  │ ─────────────────────────────                    │               │
│  │ - Strike selection                               │               │
│  │ - Expiration selection                           │               │
│  │ - Order types (limit, stop, etc.)                │               │
│  └──────────────────────────────────────────────────┘               │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘
```

## Key Design Principles

### 1. Separation of Concerns
- **Descriptive (2-6):** Observe and measure market state
- **Prescriptive (7+):** Decide and recommend actions

### 2. No Filtering Until Step 7
- Steps 2-6 ADD data, never remove rows
- Only Step 3 filters (by IVHV gap threshold)
- Step 6 returns ALL input rows (pure validation)
- Step 7+ can filter for specific strategies

### 3. Neutral Language in Descriptive Steps
- Step 3: "IV_Rich" / "IV_Cheap" (not "opportunity")
- Step 5: "Regime=Trending" (not "favorable")
- Step 6: "Crossover_Age_Bucket=Age_0_5" (not "Recent")

### 4. Each Step is Self-Contained
```python
# Can run individually for testing
from core.scan_engine import (
    load_ivhv_snapshot,
    filter_ivhv_gap,
    compute_chart_signals,
    validate_data_quality,
    recommend_strategies
)

df2 = load_ivhv_snapshot('snapshot.csv')
df3 = filter_ivhv_gap(df2)
df5 = compute_chart_signals(df3)
df6 = validate_data_quality(df5)
df7 = recommend_strategies(df6)  # OPTIONAL
```

### 5. Guardrail Comments
Every step has a comment enforcing design:
```python
"""
NOTE:
This step is strictly DESCRIPTIVE.
It must not introduce strategy assumptions, thresholds,
pass/fail flags, or trade intent.
All strategy decisions occur in later phases.
"""
```

## Data Flow Example

```python
# Starting point: 175 tickers in Fidelity snapshot
df2 = load_ivhv_snapshot()  # 175 rows, +enrichment columns

# Filter by IV-HV divergence >= 3.5
df3 = filter_ivhv_gap(df2, min_gap=3.5)  # 127 rows, +volatility tags

# Add chart indicators (only for tickers with price data)
df5 = compute_chart_signals(df3)  # 46 rows (81 failed to fetch)

# Validate completeness (returns ALL rows)
df6 = validate_data_quality(df5)  # 46 rows, +quality flags

# Generate strategy recommendations
df7 = recommend_strategies(df6)  # 46 rows, +strategy columns
```

## Column Evolution

### After Step 2:
```
Ticker, IV_30D, HV_30D, IVHV_gap_30D,
IV_Rank_30D, IV_Term_Structure, IV_Trend_7D, HV_Trend_30D
```

### After Step 3:
```
+ IV_Rich, IV_Cheap, MeanReversion_Setup, Expansion_Setup, IVHV_gap_abs
```

### After Step 5:
```
+ Regime, EMA_Signal, Signal_Type, Days_Since_Cross, Trend_Slope, Atr_Pct
```

### After Step 6:
```
+ Data_Complete, Crossover_Age_Bucket
```

### After Step 7:
```
+ Primary_Strategy, Secondary_Strategy, Strategy_Type, Confidence,
  Success_Probability, Trade_Bias, Entry_Priority, Risk_Level
```

## Pipeline Execution Modes

### Mode 1: Full Pipeline (with strategies)
```python
results = run_full_scan_pipeline(include_step7=True)
recommendations = results['recommendations']
```

### Mode 2: Descriptive Only (no strategies)
```python
results = run_full_scan_pipeline(include_step7=False)
validated_data = results['validated_data']
# Use with custom strategy engine
```

### Mode 3: Step-by-Step (debugging)
```python
df2 = load_ivhv_snapshot()
print(f"Step 2: {len(df2)} tickers")

df3 = filter_ivhv_gap(df2)
print(f"Step 3: {len(df3)} tickers")

df5 = compute_chart_signals(df3)
print(f"Step 5: {len(df5)} tickers")

df6 = validate_data_quality(df5)
print(f"Step 6: {len(df6)} tickers")

df7 = recommend_strategies(df6)
print(f"Step 7: {len(df7[df7['Primary_Strategy'] != 'None'])} strategies")
```

## File Organization Benefits

### Before (Monolithic)
```
scan_pipeline.py  (500+ lines, mixed concerns)
```

### After (Modular)
```
step2_load_snapshot.py          (200 lines, data loading)
step3_filter_ivhv.py            (150 lines, volatility filtering)
step5_chart_signals.py          (180 lines, chart indicators)
step6_gem_filter.py             (130 lines, data validation)
step7_strategy_recommendation.py (400 lines, strategy logic)
```

**Advantages:**
- ✅ Easy to find specific logic
- ✅ Clear responsibility boundaries
- ✅ Independent testing per step
- ✅ Parallel development (different devs, different steps)
- ✅ Clean git history (changes isolated to relevant files)

## Testing Strategy

### Unit Tests (per step)
```python
def test_step2_enrichment():
    df = load_ivhv_snapshot('test_snapshot.csv')
    assert 'IV_Rank_30D' in df.columns
    assert df['IV_Rank_30D'].between(0, 100).all()

def test_step3_bidirectional_filtering():
    df = filter_ivhv_gap(mock_data, min_gap=3.5)
    assert (df['IVHV_gap_abs'] >= 3.5).all()
    assert 'IV_Rich' in df.columns
    assert 'IV_Cheap' in df.columns

def test_step7_strategy_selection():
    df = recommend_strategies(mock_data)
    assert 'Primary_Strategy' in df.columns
    assert df['Confidence'].between(0, 100).all()
```

### Integration Tests (full pipeline)
```python
def test_full_pipeline():
    results = run_full_scan_pipeline(include_step7=True)
    assert 'snapshot' in results
    assert 'filtered' in results
    assert 'charted' in results
    assert 'validated_data' in results
    assert 'recommendations' in results
```

## Summary

The modular architecture provides:
1. **Clear Boundaries:** Descriptive vs Prescriptive
2. **Easy Maintenance:** Each step in its own file
3. **High Testability:** Unit tests per step
4. **Flexibility:** Mix and match steps as needed
5. **Scalability:** Add Steps 8-10 without breaking existing code

**Next:** Implement Steps 8-10 (position sizing, risk management, orders)
