# Step 9B Implementation: Strategy-Aware Contract Fetching

**Date**: December 27, 2025  
**Status**: ✅ COMPLETE & READY FOR TESTING

## Overview

Step 9B has been updated to support **strategy-aware** contract fetching, complementing the Step 9A redesign. Each (Ticker, Strategy, DTE) combination now gets its own option chain fetch and contract selection.

## Problem Statement

### Before (Implicit Ticker-Level)
```python
Input: Multi-strategy ledger (after Step 9A)
  AAPL | Long Call     | 35-50 DTE
  AAPL | Long Straddle | 45-60 DTE
  AAPL | Buy-Write     | 30-45 DTE

Risk: If ticker-level grouping existed, strategies would collapse
```

### After (Explicit Strategy-Aware)
```python
Input: Multi-strategy ledger with DTE ranges
  AAPL | Long Call     | 35-50 DTE | Target: 42
  AAPL | Long Straddle | 45-60 DTE | Target: 52
  AAPL | Buy-Write     | 30-45 DTE | Target: 37

Output: Each strategy gets independent contract selection
  AAPL | Long Call     | Exp: 2025-02-14 | Strike: 150C | Status: Success
  AAPL | Long Straddle | Exp: 2025-02-28 | Strike: 150C+150P | Status: Success
  AAPL | Buy-Write     | Exp: 2025-02-07 | Strike: 155C | Status: Success
```

## Implementation Changes

### 1. Header Documentation Update

**New Emphasis**:
- **Strategy-aware processing**: Each (Ticker, Strategy, DTE) processed independently
- **No grouping by ticker**: Preserves multi-strategy ledger
- **Multi-contract support**: Same ticker can have multiple contract fetches
- **Row preservation**: Input rows == Output rows (assertion added)

**Example Clarification**:
```python
Input: 266 strategies (127 tickers with multiple strategies each)
  AAPL | Long Call     | 35-50 DTE
  AAPL | Long Straddle | 45-60 DTE
  AAPL | Buy-Write     | 30-45 DTE
  
Output: 266 strategies with contracts (NO ROWS DROPPED)
  AAPL | Long Call     | Exp: 2025-02-14 | Strike: 150C | Status: Success
  AAPL | Long Straddle | Exp: 2025-02-28 | Strike: 150C+150P | Status: Success
  AAPL | Buy-Write     | Exp: 2025-02-07 | Strike: 155C | Status: Success
```

### 2. Row Count Preservation

**Added to `fetch_and_select_contracts()`**:
```python
def fetch_and_select_contracts(df: pd.DataFrame, ...) -> pd.DataFrame:
    # Track input rows
    input_row_count = len(df)
    logger.info(f"🎯 Step 9B: Processing {input_row_count} strategies (strategy-aware contract fetching)")
    
    # ... process each row ...
    
    # Verify no rows dropped
    output_row_count = len(df)
    assert output_row_count == input_row_count, (
        f"❌ Row count mismatch: {output_row_count} != {input_row_count}. "
        f"Step 9B must preserve all strategies (no silent filtering)."
    )
    logger.info(f"✅ Row count preserved: {output_row_count} strategies processed")
    
    return df
```

### 3. Multi-Contract Audit

**New Function: `_audit_multi_contract_tickers()`**:
```python
def _audit_multi_contract_tickers(df: pd.DataFrame):
    """
    Audit and log tickers with multiple contract selections (multi-DTE architecture).
    
    Example Output:
      📊 Multi-Contract Architecture: 117 tickers have multiple contracts
         AAPL: 3 contracts
            - Long Call (DTE=42): Exp 2025-02-14, Strike ["150"], Status=Success
            - Long Straddle (DTE=52): Exp 2025-02-28, Strike ["150","150"], Status=Success
            - Buy-Write (DTE=37): Exp 2025-02-07, Strike ["155"], Status=Success
         
      📊 Contract Distribution:
         Total tickers: 127
         Total contracts: 266
         Avg contracts/ticker: 2.09
         Multi-contract tickers: 117 (92.1%)
    """
    # Finds tickers with multiple successful contract selections
    # Logs top 5 examples with strategy details
    # Reports summary statistics
```

### 4. Strategy-Aware Logging

**Updated Main Loop**:
```python
for idx, row in df.iterrows():
    ticker = row['Ticker']
    
    # Use Strategy_Name (new) or Primary_Strategy (legacy)
    strategy = row.get('Strategy_Name', row.get('Primary_Strategy', 'Unknown'))
    
    # Use Target_DTE (new) or Preferred_DTE (legacy)
    target_dte = int(row.get('Target_DTE', row.get('Preferred_DTE', (min_dte + max_dte) // 2)))
    
    # Enhanced logging shows ticker AND strategy
    logger.info(f"📊 {ticker} | {strategy}: Fetching chains for DTE {min_dte}-{max_dte} (target: {target_dte})")
```

**Before**:
```
📊 AAPL: Fetching chains for DTE range 30-60 days (preferred: 45) (Long Call)
```

**After**:
```
📊 AAPL | Long Call: Fetching chains for DTE 35-50 (target: 42)
📊 AAPL | Long Straddle: Fetching chains for DTE 45-60 (target: 52)
📊 AAPL | Buy-Write: Fetching chains for DTE 30-45 (target: 37)
```

## Architectural Guarantees

### Critical Rules Enforced

1. **Row Preservation**:
   - `assert output_row_count == input_row_count`
   - Every strategy gets contract selection attempt (may fail, but row preserved)

2. **Strategy-Aware Processing**:
   - Process each (Ticker, Strategy, DTE) independently
   - NO grouping or collapsing by ticker

3. **Multi-Contract Support**:
   - Same ticker CAN have multiple contract fetches
   - Each fetch uses strategy-specific DTE range

4. **Audit Logging**:
   - Tracks multi-contract tickers
   - Reports distribution statistics
   - Shows example contracts for validation

### Processing Flow

```
Step 9A Output (266 strategies):
  AAPL | Long Call     | Min_DTE=35 | Max_DTE=50 | Target_DTE=42
  AAPL | Long Straddle | Min_DTE=45 | Max_DTE=60 | Target_DTE=52
  AAPL | Buy-Write     | Min_DTE=30 | Max_DTE=45 | Target_DTE=37
  ↓
Step 9B Processing:
  1. Fetch AAPL expirations in range 35-50 days
     → Select expiration closest to 42 days (e.g., 2025-02-14)
     → Fetch option chain
     → Select Long Call strike (e.g., 150C)
     → Status: Success
  
  2. Fetch AAPL expirations in range 45-60 days
     → Select expiration closest to 52 days (e.g., 2025-02-28)
     → Fetch option chain
     → Select Straddle strikes (e.g., 150C + 150P)
     → Status: Success
  
  3. Fetch AAPL expirations in range 30-45 days
     → Select expiration closest to 37 days (e.g., 2025-02-07)
     → Fetch option chain
     → Select Covered Call strike (e.g., 155C)
     → Status: Success
  ↓
Step 9B Output (266 strategies):
  AAPL | Long Call     | Exp=2025-02-14 | Strike=150C | Status=Success
  AAPL | Long Straddle | Exp=2025-02-28 | Strike=150C+150P | Status=Success
  AAPL | Buy-Write     | Exp=2025-02-07 | Strike=155C | Status=Success
```

## Backward Compatibility

### Column Name Fallbacks

**Strategy Name**:
```python
strategy = row.get('Strategy_Name', row.get('Primary_Strategy', 'Unknown'))
```
- New: `Strategy_Name` (from Step 9A redesign)
- Legacy: `Primary_Strategy` (existing column)

**Target DTE**:
```python
target_dte = int(row.get('Target_DTE', row.get('Preferred_DTE', (min_dte + max_dte) // 2)))
```
- New: `Target_DTE` (from Step 9A redesign)
- Legacy: `Preferred_DTE` (existing column)
- Fallback: Midpoint calculation

## Files Modified

1. **`/core/scan_engine/step9b_fetch_contracts.py`** (1151 lines)
   - Updated header documentation (strategy-aware emphasis)
   - Added row count tracking and assertion
   - Added `_audit_multi_contract_tickers()` function
   - Updated main loop logging (show ticker + strategy)
   - Added backward compatibility for column names

## Testing Requirements

### Unit Test Scenarios

1. **Multi-Strategy Ticker Test**:
   ```python
   Input: AAPL with 3 strategies (3 different DTE ranges)
   Expected: 3 contract selections, 3 different expirations
   Validation: Each strategy uses its own DTE range
   ```

2. **Row Preservation Test**:
   ```python
   Input: 266 strategies
   Expected: 266 strategies output (even if some fail)
   Validation: Assert passes, no exceptions
   ```

3. **Multi-Contract Audit Test**:
   ```python
   Input: 127 tickers, 266 strategies
   Expected: Audit logs show ~117 multi-contract tickers
   Validation: Avg contracts/ticker ≈ 2.09
   ```

### Integration Test

**Full Pipeline Test**:
```python
# Step 9A: Assign DTE ranges
df_step9a = determine_option_timeframe(df_step8)
assert len(df_step9a) == 266

# Step 9B: Fetch contracts
df_step9b = fetch_and_select_contracts(df_step9a)
assert len(df_step9b) == 266  # Row preservation

# Verify multi-contract architecture
aapl_contracts = df_step9b[df_step9b['Ticker'] == 'AAPL']
assert len(aapl_contracts) == 3  # 3 strategies
assert len(aapl_contracts['Selected_Expiration'].unique()) >= 2  # Multiple expirations
```

## Expected Behavior

### Production Run Expectations

**Input (from Step 9A)**:
- 266 strategies
- 127 unique tickers
- Avg 2.09 strategies per ticker
- Multi-strategy tickers: ~117 (92%)

**Output (Step 9B)**:
- 266 strategies (NO ROWS DROPPED)
- Contract selection status:
  - Success: ~200-250 strategies (depends on liquidity)
  - No_Expirations: ~5-10 strategies
  - Low_Liquidity: ~10-20 strategies
  - Failed: ~0-5 strategies
- Multi-contract tickers: ~117 (same as input)
- Avg contracts/ticker: 2.09 (unchanged)

**Audit Output**:
```
📊 Multi-Contract Architecture: 117 tickers have multiple contracts
   AAPL: 3 contracts
      - Long Call (DTE=42): Exp 2025-02-14, Strike ["150"], Status=Success
      - Long Straddle (DTE=52): Exp 2025-02-28, Strike ["150","150"], Status=Success
      - Buy-Write (DTE=37): Exp 2025-02-07, Strike ["155"], Status=Success
   
📊 Contract Distribution:
   Total tickers: 127
   Total contracts: 266
   Avg contracts/ticker: 2.09
   Multi-contract tickers: 117 (92.1%)
```

## Next Steps

1. **Test Step 9B with Real Data**:
   - Run full pipeline (Step 1-9B)
   - Verify row count preservation
   - Validate multi-contract audit output

2. **Step 11 Redesign** (Strategy Comparison):
   - Input: 266 strategies with contracts
   - Process: Compare strategies per ticker (apples-to-apples)
   - Output: Ranked strategies with comparison metrics
   - Logic: Use real Greeks, premiums, bid-ask

3. **Step 8 Repositioning** (Move to End):
   - Current: Step 7 → Step 8 → Step 9A → Step 9B
   - Target: Step 7 → Step 9A → Step 9B → Step 10 → Step 11 → Step 8
   - Reason: Step 8 needs real contracts for final decision

4. **CLI Audit Enhancement** (Sections G & H):
   - Section G: Multi-DTE Window Validation
   - Section H: Contract Distribution Validation
   - Verify end-to-end strategy-aware architecture

## Success Metrics

✅ **Row Preservation**: Assertion added (input == output)  
✅ **Strategy-Aware Processing**: Each (Ticker, Strategy, DTE) processed independently  
✅ **Multi-Contract Support**: Audit function validates architecture  
✅ **Enhanced Logging**: Shows ticker + strategy in logs  
✅ **Backward Compatibility**: Supports old and new column names  
✅ **Import Tested**: Module loads without errors  

## Conclusion

Step 9B now explicitly supports strategy-aware contract fetching, ensuring that each (Ticker, Strategy, DTE) combination gets its own option chain fetch and contract selection. This complements the Step 9A redesign and enables proper multi-strategy processing through the pipeline.

**Architecture Principle**: Contract fetching must be strategy-aware, not ticker-aware.

---

**Implementation by**: GitHub Copilot  
**Status**: Ready for testing  
**Production Ready**: After integration testing
