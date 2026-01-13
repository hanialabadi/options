# Step 0 NaN Price Bug: Quick Reference

## Problem
Step 0 produced all NaN prices (177 tickers) because it only tried `quote.get('lastPrice')` with no fallback.

## Root Cause (Bug Location)
```python
# OLD CODE (BROKEN) - step0_schwab_snapshot.py:335-340
quote = data[ticker].get('quote', {})
results[ticker] = {
    'last_price': quote.get('lastPrice', np.nan),  # Only tries lastPrice!
    'volume': quote.get('totalVolume', np.nan)
}
```

## Solution
```python
# NEW CODE (FIXED) - step0_schwab_snapshot.py:433-530
# 1. Check market hours
is_market_open, market_status = is_market_open_schwab(client)

# 2. Extract price with fallback cascade
price, source = extract_best_price(quote_block, is_market_open)

# 3. Track metadata
results[ticker] = {
    'last_price': price,
    'volume': volume,
    'price_source': source,  # NEW: "lastPrice", "mark", "closePrice", etc.
    'quote_time': quote_time,  # NEW: milliseconds epoch
    'trade_time': trade_time,  # NEW: milliseconds epoch
    'raw_quote': quote_block  # NEW: for debugging
}
```

## Fallback Cascade

**Market OPEN** (prefer live data):
```
lastPrice → mark → bidAskMid → closePrice → regularMarketLastPrice → None
```

**Market CLOSED** (prefer stable data):
```
mark → closePrice → lastPrice → bidAskMid → regularMarketLastPrice → None
```

## New Validation Columns

| Column | Description | Example |
|--------|-------------|---------|
| `price_source` | Which field was used | `"mark"`, `"lastPrice"`, `"closePrice"` |
| `quote_time` | Schwab quote timestamp (ms) | `1735689654000` |
| `trade_time` | Last trade timestamp (ms) | `1735689654000` |
| `quote_age_sec` | Seconds since quote/trade | `14.5` |
| `is_market_open` | Market status at snapshot time | `True` / `False` |
| `market_status` | Market status string | `"OPEN"`, `"CLOSED"`, `"UNKNOWN"` |

## Quality Check

**Validation Rule**: Reject snapshots with >30% NaN prices

```python
nan_count = df['last_price'].isna().sum()
if (nan_count / len(df)) > 0.30:
    raise ValueError("Snapshot rejected: too many NaN prices")
```

**Behavior**:
- **0-30% NaN**: Warning logged, snapshot saved ✅
- **>30% NaN**: Exception raised, snapshot rejected ❌

## Files Changed

1. **`core/scan_engine/step0_schwab_snapshot.py`** (~250 lines changed)
   - Added `is_market_open_schwab()` function
   - Added `extract_best_price()` function  
   - Refactored `fetch_batch_quotes()` to use fallback
   - Updated `fetch_all_quotes()` to return market status
   - Added 6 validation columns to output
   - Added >30% NaN quality check

2. **`tests/test_extract_best_price.py`** (NEW)
   - Unit tests for fallback logic
   - 8/8 tests passing ✅

3. **`tests/test_step0_schwab_snapshot_quality.py`** (NEW)
   - E2E CLI test for snapshot generation
   - Requires Schwab authentication 🔐

## Testing

**Unit Test** (no API needed):
```bash
python tests/test_extract_best_price.py
# ✅ All tests passed!
```

**E2E Test** (requires Schwab auth):
```bash
python tests/test_step0_schwab_snapshot_quality.py
# Prints: validation table, price source coverage, raw JSON samples
```

## Expected Output Example

**Console Log**:
```
📊 STEP 0 COMPLETE - SUMMARY
   Total tickers: 25
   Runtime: 8.2s
   Market status: CLOSED

   Price Source Coverage:
     lastPrice                  5 (20.0%)
     mark                      18 (72.0%)
     closePrice                 2 ( 8.0%)
     none                       0 ( 0.0%)

   ✅ All 25 tickers have valid prices!
```

**Validation Table**:
```
Ticker   Price      Source             HV_30    Market     Age(s)     Status
AAPL     $234.87    mark               28.5%    CLOSED     3600s      CLOSED
MSFT     $425.12    closePrice         31.2%    CLOSED     3605s      CLOSED
GOOGL    $178.44    mark               24.8%    CLOSED     3598s      CLOSED
```

## Status

✅ **Bug Fixed**: Fallback cascade implemented  
✅ **Unit Tested**: 8/8 tests passing  
🔐 **E2E Test**: Requires Schwab re-authentication  
📊 **Dashboard**: Ready for market status + source coverage charts  

**Next Action**: Re-authenticate with Schwab, run E2E test to validate with live API.
