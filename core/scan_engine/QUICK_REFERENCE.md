# Scan Engine - Quick Reference Guide

## Complete Step Summary

| Step | File | Purpose | Input → Output | Type |
|------|------|---------|----------------|------|
| **2** | `step2_load_snapshot.py` | Load & enrich IV/HV data | 175 → 175 | Descriptive |
| **3** | `step3_filter_ivhv.py` | Filter by IVHV gap | 175 → 127 | Descriptive |
| **5** | `step5_chart_signals.py` | Add chart indicators | 127 → 46 | Descriptive |
| **6** | `step6_gem_filter.py` | Validate data quality | 46 → 46 | Descriptive |
| **7** | `step7_strategy_recommendation.py` | Recommend strategies | 46 → 46 | **Prescriptive** |

## Quick Start

### Run Full Pipeline
```python
from core.scan_engine import run_full_scan_pipeline

# With strategy recommendations (Steps 2-7)
results = run_full_scan_pipeline(include_step7=True)
df = results['recommendations']

# Descriptive only (Steps 2-6)
results = run_full_scan_pipeline(include_step7=False)
df = results['validated_data']
```

### Run Individual Steps
```python
from core.scan_engine import (
    load_ivhv_snapshot,
    filter_ivhv_gap,
    compute_chart_signals,
    validate_data_quality,
    recommend_strategies
)

df2 = load_ivhv_snapshot('snapshot.csv')
df3 = filter_ivhv_gap(df2, min_gap=3.5)
df5 = compute_chart_signals(df3)
df6 = validate_data_quality(df5)
df7 = recommend_strategies(df6)  # Optional
```

## Key Columns by Step

### Step 2 Output
```python
['Ticker', 'IV_30D', 'HV_30D', 'IVHV_gap_30D', 'IV_Rank_30D', 
 'IV_Term_Structure', 'IV_Trend_7D', 'HV_Trend_30D', 'Snapshot_Age_Hours']
```

### Step 3 Output (adds)
```python
['IV_Rich', 'IV_Cheap', 'MeanReversion_Setup', 'Expansion_Setup', 
 'IVHV_gap_abs', 'IV_Pattern']
```

### Step 5 Output (adds)
```python
['Regime', 'EMA_Signal', 'Signal_Type', 'Days_Since_Cross', 
 'Trend_Slope', 'Price_vs_SMA20', 'Price_vs_SMA50', 'Atr_Pct']
```

### Step 6 Output (adds)
```python
['Data_Complete', 'Crossover_Age_Bucket']
```

### Step 7 Output (adds)
```python
['Primary_Strategy', 'Secondary_Strategy', 'Strategy_Type', 
 'Confidence', 'Success_Probability', 'Trade_Bias', 
 'Entry_Priority', 'Risk_Level']
```

## Common Queries

### High Confidence Setups
```python
high_conf = df[df['Confidence'] >= 70]
```

### Specific Strategy Type
```python
directional = df[df['Strategy_Type'] == 'Directional']
neutral = df[df['Strategy_Type'] == 'Neutral']
volatility = df[df['Strategy_Type'] == 'Volatility']
```

### Recent Signals Only
```python
recent = df[df['Crossover_Age_Bucket'] == 'Age_0_5']
```

### High IV Premium Selling
```python
premium_selling = df[
    (df['IV_Rank_30D'] >= 70) & 
    (df['IV_Rich'] == True) &
    (df['Strategy_Type'].isin(['Neutral', 'Directional']))
]
```

### Low IV Premium Buying
```python
premium_buying = df[
    (df['IV_Rank_30D'] <= 30) & 
    (df['IV_Cheap'] == True) &
    (df['Strategy_Type'] == 'Directional')
]
```

## Configuration Options

### Step 3: Filter Parameters
```python
df3 = filter_ivhv_gap(
    df2,
    min_gap=3.5,        # Minimum IV-HV gap (absolute)
    min_iv_rank=None    # Optional: minimum IV rank
)
```

### Step 7: Strategy Selection
```python
df7 = recommend_strategies(
    df6,
    min_iv_rank=50.0,           # Min IV rank for premium selling
    min_ivhv_gap=3.5,           # Min gap for any strategy
    enable_directional=True,    # Enable directional strategies
    enable_neutral=True,        # Enable neutral strategies
    enable_volatility=True      # Enable volatility strategies
)
```

## Output Files

Pipeline automatically exports to `output/` directory:
```
output/
├── Step3_Filtered_YYYYMMDD_HHMMSS.csv
├── Step5_Charted_YYYYMMDD_HHMMSS.csv
├── Step6_Validated_YYYYMMDD_HHMMSS.csv
└── Step7_Recommendations_YYYYMMDD_HHMMSS.csv  (if include_step7=True)
```

## Logging

Enable detailed logs:
```python
import logging
logging.basicConfig(level=logging.INFO)

results = run_full_scan_pipeline()
```

Expected output:
```
INFO - 📊 Step 2: Loading IV/HV snapshot...
INFO - ✅ Step 2: 175 rows, all required columns present
INFO - 📊 Step 3: Filtering by IVHV gap...
INFO - ✅ Step 3: 127 tickers qualified
INFO - 📊 Step 5: Computing chart signals...
INFO - ✅ Step 5: 46 tickers charted
INFO - 📊 Step 6: Validating data quality...
INFO - ⏱️ Crossover age distribution: {'Age_16_plus': 30, 'Age_6_15': 9, 'Age_0_5': 7}
INFO - 🎯 Step 7: Generating strategy recommendations...
INFO - 📊 Strategy distribution: {'Mixed': 3, 'Directional': 1, ...}
INFO - ✅ Step 7 complete: 46 tickers with strategy recommendations
```

## Troubleshooting

### Missing Columns Error
```
ValueError: Missing columns ['IV_Rank_30D', 'Crossover_Age_Bucket']
```
**Solution:** Run full pipeline from Step 2 (don't skip steps)

### Empty DataFrame
```
⚠️ No tickers passed Step 3. Pipeline stopped.
```
**Solution:** Lower `min_gap` parameter or check input data quality

### ImportError
```
ImportError: cannot import name 'recommend_strategies'
```
**Solution:** Update import:
```python
from core.scan_engine.step7_strategy_recommendation import recommend_strategies
```

## Testing

### Quick Test with Mock Data
```python
import pandas as pd
import numpy as np

# Create mock data
df = pd.DataFrame({
    'Ticker': ['AAPL', 'MSFT', 'GOOGL'],
    'IV_Rank_30D': [80, 45, 25],
    'IVHV_gap_30D': [15.2, 8.5, 4.1],
    'Signal_Type': ['Bullish', 'Bearish', 'Bullish'],
    'Regime': ['Trending', 'Ranging', 'Trending'],
    'Crossover_Age_Bucket': ['Age_0_5', 'Age_6_15', 'Age_0_5'],
    'Data_Complete': [True, True, True],
    'IV_Rich': [True, False, False],
    'IV_Cheap': [False, False, True],
    'MeanReversion_Setup': [True, False, False],
    'Expansion_Setup': [False, False, True],
})

# Test Step 7
df7 = recommend_strategies(df)
print(df7[['Ticker', 'Primary_Strategy', 'Confidence']])
```

## Best Practices

1. **Always run from Step 2** - Enrichment columns are required
2. **Use include_step7 flag** - Control descriptive vs prescriptive
3. **Check Data_Complete** - Filter by this before manual review
4. **Validate Confidence scores** - Backtest before using in production
5. **Export intermediate CSVs** - Debug pipeline issues easily

## Performance

Typical execution times (50 tickers):
- Step 2: <1 second (file load)
- Step 3: <1 second (filtering)
- Step 5: ~50 seconds (1 sec per ticker for yfinance)
- Step 6: <1 second (validation)
- Step 7: <1 second (strategy logic)

**Total: ~60 seconds for 50 tickers**

## Architecture Benefits

✅ **Modular:** Each step is independently testable  
✅ **Clear:** Descriptive (2-6) vs Prescriptive (7+)  
✅ **Flexible:** Mix and match steps as needed  
✅ **Maintainable:** Logic isolated to specific files  
✅ **Scalable:** Easy to add Steps 8-10  

## What's Next?

- **Step 8:** Position sizing (Kelly, fixed fractional)
- **Step 9:** Risk management (portfolio heat, correlation)
- **Step 10:** Order generation (strikes, expirations, order types)
- **Dashboard UI:** Streamlit interface for recommendations
- **Backtesting:** Historical validation of strategies
