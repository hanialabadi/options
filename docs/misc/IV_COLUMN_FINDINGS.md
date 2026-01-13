# IV Column Tracking: Findings

## Executive Summary

**Root Cause Identified:** IV surface data (7D, 14D, 21D, 60D, 90D, etc.) is **never loaded** into the snapshot.

## Evidence

### Step 0: Raw Snapshot Load
```
IV Columns (32): Present
  - IV_7_D_Call = NaN
  - IV_14_D_Call = NaN  
  - IV_21_D_Call = NaN
  - IV_30_D_Call = 22.093 ✅ (ONLY non-null IV)
  - IV_60_D_Call = NaN
  - IV_90_D_Call = NaN
  - IV_120_D_Call = NaN
  - IV_180_D_Call = NaN
  - IV_360_D_Call = NaN
  - IV_720_D_Call = NaN
  - ALL Put columns = NaN
```

### AAPL Specific Data (Step 0)
```
IV_30_D_Call: 22.093  ← ONLY populated IV
iv_30d: 22.093        ← Duplicate of above
All other IV_*: NaN   ← 30 columns empty
```

## What This Means

1. **Snapshot file has column headers but no data**
   - Columns exist in schema
   - All values are NaN (missing)

2. **Only current 30D IV is populated**
   - Probably from live API fetch
   - Historical/surface data never loaded

3. **Step 12 acceptance correctly rejects**
   - Sees missing IV surface → blocks trade
   - System working as designed
   - Problem is upstream data loading

## What's Missing

The IV time-series loader that should:
1. Read historical IV data from disk/database
2. Join it to snapshot by ticker+date
3. Populate IV_7D, IV_14D, IV_21D, IV_60D columns

## Next Steps

**Option 1: Find existing time-series data**
- Check if `data/iv_history/` or similar exists
- Look for time-series rehydration code

**Option 2: Accept current state**
- System only has 30D IV from live API
- Cannot evaluate IV surface without historical data
- Need to collect IV data over time before strategies work

**Option 3: Use alternative data source**
- Fetch IV surface from options chains
- Calculate from ATM options at each DTE
- Store for future use
