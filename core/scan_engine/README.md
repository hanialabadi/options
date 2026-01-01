# Scan Engine - Modular Pipeline Structure

## 📁 File Organization

```
core/scan_engine/
├── __init__.py              # Package exports and version
├── utils.py                 # Shared validation helpers
├── step2_load_snapshot.py   # Load IV/HV snapshot from CSV
├── step3_filter_ivhv.py     # Filter by IVHV gap + persona tags
├── step5_chart_signals.py   # Chart indicators + regime classification
├── step6_gem_filter.py      # Final GEM candidate filtering
└── pipeline.py              # Full pipeline orchestrator
```

## 🎯 Why This Structure?

### Benefits:
1. **Modularity:** Each step is independently testable
2. **Clarity:** File names match dashboard step numbers
3. **Maintainability:** Easy to locate and update specific logic
4. **Reusability:** Import only the steps you need
5. **Documentation:** Each file has comprehensive docstrings

### vs. Monolithic `scan_pipeline.py`:
- ❌ Old: 500+ lines in one file
- ✅ New: ~100-200 lines per file (easier to read)
- ❌ Old: Hard to find specific logic
- ✅ New: File name tells you exactly what's inside
- ❌ Old: Difficult to test individual steps
- ✅ New: `python -m pytest core/scan_engine/step3_filter_ivhv.py`

## 📦 Importing

### Full Pipeline (orchestrator):
```python
from core.scan_engine import run_full_scan_pipeline

results = run_full_scan_pipeline()
gem_candidates = results['gem_candidates']
```

### Individual Steps (for testing/debugging):
```python
from core.scan_engine import (
    load_ivhv_snapshot,      # Step 2
    filter_ivhv_gap,         # Step 3
    compute_chart_signals,   # Step 5
    filter_gem_candidates    # Step 6
)

# Run step by step
df = load_ivhv_snapshot('/path/to/snapshot.csv')
df_filtered = filter_ivhv_gap(df, min_gap=3.5)
df_charted = compute_chart_signals(df_filtered)
gem = filter_gem_candidates(df_charted)
```

### Helper Functions:
```python
from core.scan_engine import validate_input, classify_regime

# Validate before processing
validate_input(df, ['Ticker', 'IVHV_gap_30D'], 'My Step')

# Classify a single ticker
regime = classify_regime({
    'Trend_Slope': 2.5,
    'Atr_Pct': 1.8,
    'Price_vs_SMA20': 10.0,
    'SMA20': 150.0
})
```

## 🧪 Testing Individual Steps

### Test Step 2 (Load):
```python
from core.scan_engine import load_ivhv_snapshot

df = load_ivhv_snapshot()
print(f"Rows: {len(df)}, Columns: {len(df.columns)}")
print(df.head())
```

### Test Step 3 (Filter):
```python
from core.scan_engine import load_ivhv_snapshot, filter_ivhv_gap

df = load_ivhv_snapshot()
df_filtered = filter_ivhv_gap(df, min_gap=2.5)

print(f"Qualified: {len(df_filtered)}")
print(f"HardPass: {df_filtered['HardPass'].sum()}")
print(df_filtered[['Ticker', 'IVHV_gap_30D']].head())
```

### Test Step 5 (Charts):
```python
from core.scan_engine import filter_ivhv_gap, compute_chart_signals
import pandas as pd

# Test with single ticker for speed
df_test = pd.DataFrame({'Ticker': ['AAPL'], 'IVHV_gap_30D': [4.5]})
df_charted = compute_chart_signals(df_test)

print(df_charted[['Ticker', 'Regime', 'EMA_Signal', 'Atr_Pct']])
```

### Test Step 6 (GEM):
```python
from core.scan_engine import filter_gem_candidates

# Assuming df_charted from Step 5
gem = filter_gem_candidates(df_charted)

print(f"GEM Candidates: {len(gem)}")
print(f"Tier 1: {(gem['Scan_Tier'] == 'GEM_Tier_1').sum()}")
print(gem[['Ticker', 'Scan_Tier', 'PCS_Seed']].head())
```

## 📝 File Descriptions

### `__init__.py`
- Package initialization
- Exports all public functions
- Version tracking

### `utils.py`
- `validate_input()`: Pre-flight DataFrame validation
- Shared helper functions
- Logging setup

### `step2_load_snapshot.py`
- Loads Fidelity IV/HV CSV export
- Validates file format
- Handles env var fallback for path
- ~60 lines

### `step3_filter_ivhv.py`
- Converts IV/HV to numeric
- Applies liquidity filter (IV >= 15, HV > 0)
- Calculates IVHV_gap_30D
- Normalizes IV_Rank_XS (0-100)
- Adds persona tags (HardPass, SoftPass, PSC_Pass, LowRank)
- Deduplicates by ticker
- ~100 lines

### `step5_chart_signals.py`
- `classify_regime()`: Market environment classifier
- `compute_chart_signals()`: Main chart processor
  - Fetches 90d price history from yfinance
  - Calculates EMA9/21, SMA20/50, ATR
  - Detects crossovers and trend slope
  - Classifies regime per ticker
- Rate limiting (0.5s every 10 tickers)
- ~180 lines

### `step6_gem_filter.py`
- Applies directional + neutral validation gates
- Filters allowed signal types
- Adds Trend_Direction, Scan_Tier, PCS_Seed
- ~120 lines

### `pipeline.py`
- `run_full_scan_pipeline()`: Orchestrates Steps 2-6
- Handles errors at each step
- Exports CSVs with timestamps
- Returns all intermediate DataFrames
- ~100 lines

## 🔄 Migration from Old Structure

### Old Import (deprecated):
```python
from core.scan_pipeline import load_ivhv_snapshot  # ❌ Old
```

### New Import (current):
```python
from core.scan_engine import load_ivhv_snapshot   # ✅ New
```

**Note:** The old `core/scan_pipeline.py` can be kept temporarily for backward compatibility, but all new code should use `core/scan_engine/`.

## 📊 Line Count Comparison

| File | Lines | Responsibility |
|------|-------|----------------|
| **Old: scan_pipeline.py** | ~550 | Everything |
| **New: step2_load_snapshot.py** | ~60 | Load only |
| **New: step3_filter_ivhv.py** | ~100 | Filter only |
| **New: step5_chart_signals.py** | ~180 | Charts only |
| **New: step6_gem_filter.py** | ~120 | GEM only |
| **New: pipeline.py** | ~100 | Orchestration |
| **New: utils.py** | ~30 | Helpers |
| **New: __init__.py** | ~20 | Exports |
| **Total New** | ~610 | (slightly more due to headers/docs) |

The slight increase is due to:
- Better documentation per file
- Explicit imports in each module
- Improved error messages with context

**Trade-off:** +10% code → +200% maintainability

## 🚀 Next Steps

### Adding New Steps:
1. Create `step7_xxx.py` in `core/scan_engine/`
2. Add comprehensive docstring
3. Export in `__init__.py`
4. Import in `pipeline.py` orchestrator
5. Add to dashboard

### Example (Step 7 - Momentum):
```python
# core/scan_engine/step7_momentum.py
"""
Step 7: Add Momentum Indicators (MACD, RSI)
"""

def compute_momentum_indicators(df):
    """
    Add MACD and RSI to charted tickers.
    
    Purpose:
        Enhance directional signals with momentum confirmation.
    ...
    """
    # Implementation
    pass
```

Then in `__init__.py`:
```python
from .step7_momentum import compute_momentum_indicators

__all__ = [
    # ... existing exports
    'compute_momentum_indicators'
]
```

## 📞 Support

**To understand logic:** Read docstrings in individual step files  
**To test a step:** Import and run in Python console  
**To debug:** Check logs for step-specific context  
**To extend:** Add new step file and update `__init__.py`
