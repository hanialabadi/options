# Corrected IV Maturity Definition: Trading Days Only

**Date:** 2026-02-03
**Status:** AUTHORITATIVE (post-forensic audit)

---

## Problem Statement

**Current (BROKEN):**
- Database contains **calendar days** (including weekends)
- 252 rows ≠ 252 trading days
- AAPL example: 252 rows = 180 trading days + 72 weekend days

**Impact:**
- MATURE threshold (252 days) is **incorrect**
- Weighted IV blending math uses **wrong denominators**
- "1 trading year" claim is **false** (only 0.71 years)

---

## Corrected Definitions

### Trading Days vs Calendar Days

| Concept | Definition | AAPL Example |
|---------|-----------|--------------|
| **Calendar days** | All dates (Mon-Sun) | 252 rows in DB |
| **Weekend days** | Saturdays + Sundays | 72 rows |
| **Trading days** | Mon-Fri (excluding holidays) | 180 actual trading days |

**Formula:**
```
trading_days = total_rows - weekend_rows - holiday_rows
```

### Maturity Thresholds (Trading Days)

| State | Old (Broken) | New (Correct) | Rationale |
|-------|-------------|---------------|-----------|
| **MATURE** | 252 calendar days | **252 trading days** | Hull Ch.15: 1 trading year |
| **IMMATURE** | 120-251 calendar days | **120-251 trading days** | Hull Ch.15: 0.5-1 year |
| **MISSING** | 0-119 calendar days | **0-119 trading days** | Insufficient for percentiles |

**Calendar Day Equivalents** (approximate):
- 252 trading days ≈ **350-365 calendar days** (accounting for weekends + ~10 holidays)
- 120 trading days ≈ **168-175 calendar days**

---

## Implementation Strategy

### Option 1: Delete Weekend Rows (Immediate)

**SQL:**
```sql
DELETE FROM iv_term_history
WHERE strftime('%w', date) IN ('0', '6');  -- Remove Saturdays (6) and Sundays (0)
```

**Impact:**
- AAPL: 252 rows → 180 rows (MATURE → IMMATURE)
- All "MATURE" tickers become IMMATURE or MISSING
- Weighted IV blending activates for previously-MATURE tickers

**Pros:**
- Immediate data cleanliness
- Aligns with trading calendar
- No ambiguity

**Cons:**
- Breaks existing thresholds (252 becomes unattainable short-term)
- Requires threshold recalibration

### Option 2: Count Trading Days Only (Defensive)

**Modify `get_iv_history_depth()` to filter weekends:**

```python
def get_iv_history_depth(
    con: duckdb.DuckDBPyConnection,
    ticker: str
) -> int:
    """
    Get number of TRADING days (Mon-Fri only) of IV history.

    CORRECTED (2026-02-03): Excludes weekends to prevent calendar contamination.
    """
    result = con.execute("""
        SELECT COUNT(*) as trading_days
        FROM iv_term_history
        WHERE ticker = ?
        AND strftime('%w', date) NOT IN ('0', '6')  -- Exclude weekends
    """, [ticker]).fetchone()

    return result[0] if result else 0
```

**Impact:**
- Database keeps weekend rows (for debugging)
- Logic only counts Mon-Fri dates
- AAPL: reports 180 trading days (not 252)

**Pros:**
- Preserves raw data for audit trail
- Non-destructive fix
- Accurate trading day count

**Cons:**
- Weekend rows still waste storage
- Potential confusion if someone queries DB directly

### Option 3: Prevent Future Weekend Inserts (Root Cause Fix)

**Modify `append_daily_iv_data()` to reject weekends:**

```python
def append_daily_iv_data(
    con: duckdb.DuckDBPyConnection,
    df_iv_data: pd.DataFrame,
    trade_date: Optional[date] = None
):
    """
    Append daily IV data to iv_term_history table.

    CORRECTED (2026-02-03): Rejects weekend dates to prevent calendar contamination.
    """
    if df_iv_data.empty:
        logger.warning("No IV data to append")
        return

    if trade_date is None:
        trade_date = date.today()

    # GUARD: Reject weekend dates
    if trade_date.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        logger.warning(f"⚠️ Rejecting weekend date {trade_date} (not a trading day)")
        return

    # ... rest of function
```

**Impact:**
- Future inserts only accept Mon-Fri dates
- Prevents recurrence of weekend contamination
- Existing weekend rows remain until cleanup

---

## Recommended Action Sequence

1. **Immediate (Option 2):** Fix `get_iv_history_depth()` to count trading days only
2. **Short-term (Option 1):** Delete existing weekend rows from database
3. **Long-term (Option 3):** Add weekend guard to `append_daily_iv_data()`

**Timeline:**
- **Now:** Deploy Option 2 (defensive counting)
- **Within 24h:** Execute Option 1 (cleanup)
- **Within 48h:** Deploy Option 3 (prevention)

---

## Maturity Threshold Recalibration

After weekend cleanup, update thresholds:

```python
# OLD (broken)
MATURITY_THRESHOLD = 252  # Calendar days

# NEW (correct)
MATURITY_THRESHOLD_TRADING_DAYS = 252  # Mon-Fri only
MATURITY_THRESHOLD_CALENDAR_ESTIMATE = 350  # ~252 trading days + weekends
```

**Expected State After Cleanup:**
- AAPL: 180 trading days → **IMMATURE** (was falsely MATURE)
- Weighted IV blending activates: `fidelity_weight = (252 - 180) / 252 = 28.6%`
- Fidelity structural reference becomes active

---

## Validation Criteria

**Post-cleanup invariants:**

```sql
-- 1. No weekend dates
SELECT COUNT(*)
FROM iv_term_history
WHERE strftime('%w', date) IN ('0', '6');
-- Expected: 0

-- 2. Trading day count matches expectation
SELECT
    ticker,
    COUNT(*) as trading_days,
    MIN(date) as first_day,
    MAX(date) as last_day
FROM iv_term_history
WHERE ticker = 'AAPL'
GROUP BY ticker;
-- Expected: trading_days ≈ 180, span ≈ 251 calendar days

-- 3. Gap distribution shows weekends removed
WITH gaps AS (
    SELECT date_diff('day', LAG(date) OVER (ORDER BY date), date) as gap
    FROM iv_term_history
    WHERE ticker = 'AAPL'
)
SELECT gap, COUNT(*) as count
FROM gaps
WHERE gap IS NOT NULL
GROUP BY gap
ORDER BY gap;
-- Expected: gap=1 (weekdays), gap=3 (Mon after Fri), no gap=2
```

---

## Fidelity Reconciliation (Next Step)

After trading-day cleanup, reconcile Schwab percentiles with Fidelity reference:

**Fidelity IV30 (AAPL, Feb 3, 2026):** 23.94%

**Schwab IV30 percentile calculation:**
1. Get Schwab IV30 history (180 trading days, weekends removed)
2. Compute percentile rank of current IV30
3. Compare to Fidelity's term structure position

**Expected alignment:**
- If Schwab shows 23.94% at 40th percentile (example)
- Fidelity structure should confirm similar relative position
- Material divergence → investigate data quality

---

**Status:** READY FOR IMPLEMENTATION

Corrected maturity definition treats database as **trading days only**, excluding weekends and holidays.
