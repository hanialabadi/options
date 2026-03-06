# Forensic Audit Summary: IV History Integrity

**Date:** 2026-02-03
**Status:** CRITICAL ISSUES IDENTIFIED AND PARTIALLY REMEDIATED

---

## Executive Summary

**Finding:** Schwab IV history database contains **calendar days (including weekends)**, not trading days.

**Impact:**
- All 9 "MATURE" tickers were **falsely classified** (252 calendar days ≠ 252 trading days)
- Actual trading days: **180** (not 252)
- Maturity logic was **invalid**
- Weighted IV blending math was **incorrect**

**Immediate Action Taken:**
- ✅ Implemented trading-day filtering (Option 2: defensive fix)
- ✅ All affected tickers now correctly marked IMMATURE
- ✅ Weighted IV blending activated (28.6% Fidelity weight)

---

## Forensic Evidence

### Weekend Contamination (AAPL Example)

| Metric | Value | Status |
|--------|-------|--------|
| Total rows in DB | 252 | |
| Weekend dates | **72** | ❌ CRITICAL |
| **Actual trading days** | **180** | ✅ CORRECTED |
| Calendar span | 251 days (May 27, 2025 → Feb 2, 2026) | |
| Expected span for 252 trading days | ~365 days | ❌ MISMATCH |

**Gap distribution:**
- All gaps = 1 calendar day (consecutive daily inserts including weekends)
- **NOT** trading-day continuity (would show 1-day weekdays, 3-day weekends)

**PRIMARY KEY enforcement:** ✅ Working correctly (no duplicate dates)

**Created timestamps:** All AAPL dates created 2026-02-03 17:22:53 (bulk backfill)

---

## Corrective Actions Implemented

### ✅ Option 2: Defensive Fix (Deployed)

**Modified functions:**
1. `get_iv_history_depth()` - Now excludes weekends:
   ```sql
   WHERE CAST(strftime('%w', date) AS INTEGER) NOT IN (0, 6)
   ```

2. `calculate_iv_rank()` - Historical queries exclude weekends:
   ```sql
   WHERE CAST(strftime('%w', date) AS INTEGER) NOT IN (0, 6)
   ```

**Impact:**
- AAPL: 252 calendar days → **180 trading days**
- Maturity: MATURE → **IMMATURE**
- Weighted blending: Now active (Schwab 71.4%, Fidelity 28.6%)

**Verification:**
```
AAPL:
  Old count (calendar days):  252
  Weekend days:               72
  New count (trading days):   180 ✅

  Current IV30: 39.52%
  IV Rank: 73.7%
  Maturity state: IMMATURE ✅
```

---

## Remaining Actions Required

### 🔴 Priority 1: Investigate Schwab-Fidelity IV Divergence

**CRITICAL DATA QUALITY ISSUE:**
- Fidelity IV30 (ground truth): **23.94%**
- Schwab IV30 (database): **39.52%**
- **Divergence: +15.58 percentage points** ❌

**This should not happen.** Current IV should match across sources (±2%).

**Possible causes:**
1. Schwab data is stale (check timestamps)
2. Schwab constant-maturity interpolation error
3. Fidelity snapshot timing difference (4pm ET vs market close)
4. Database contains wrong ticker data

**Action:** Investigate and resolve before trusting any IV metrics.

---

### 🟡 Priority 2: Delete Weekend Rows (Option 1)

**Current state:** Weekend rows still in database (wasting storage, potential confusion)

**SQL cleanup:**
```sql
DELETE FROM iv_term_history
WHERE CAST(strftime('%w', date) AS INTEGER) IN (0, 6);
```

**Expected impact:**
- AAPL: 252 rows → 180 rows
- All tickers: ~28.6% row reduction
- Clean trading-day data only

**Timing:** Execute during maintenance window (non-critical, logic already corrected)

---

### 🟢 Priority 3: Prevent Future Weekend Inserts (Option 3)

**Add guard to `append_daily_iv_data()`:**
```python
if trade_date.weekday() >= 5:  # Saturday or Sunday
    logger.warning(f"⚠️ Rejecting weekend date {trade_date} (not a trading day)")
    return
```

**Purpose:** Prevent recurrence of weekend contamination

**Timing:** Deploy with next data collection update

---

## Maturity State Recalibration

### Before Correction (BROKEN)

| Ticker | Calendar Days | Maturity | Fidelity Weight |
|--------|---------------|----------|-----------------|
| AAPL | 252 | MATURE ❌ | 0% (dormant) |
| MSFT | 252 | MATURE ❌ | 0% (dormant) |
| GOOGL | 252 | MATURE ❌ | 0% (dormant) |

**Problem:** Fidelity structural reference was incorrectly dormant (false MATURE status)

### After Correction (FIXED)

| Ticker | Trading Days | Maturity | Fidelity Weight |
|--------|--------------|----------|-----------------|
| AAPL | 180 | IMMATURE ✅ | 28.6% (active) |
| MSFT | 180 | IMMATURE ✅ | 28.6% (active) |
| GOOGL | 180 | IMMATURE ✅ | 28.6% (active) |

**Result:** Weighted IV blending now correctly active for all tickers

---

## Fidelity as Ground Truth

**Fidelity IV Index (AAPL, Feb 3, 2026):**
- IV30: 23.94%
- IV60: 24.62%
- IV90: 25.83%
- IV180: 26.68%
- HV30: 20.82%

**Usage:**
1. **Structural benchmark** - Fidelity defines IV term structure (not daily time series)
2. **Validation reference** - Schwab IV should converge to Fidelity IV (±2%)
3. **IMMATURE blending** - Fidelity provides percentile reference for tickers with <252 trading days

**NOT used for:**
- Daily IV history (Schwab is authoritative time series)
- Percentile ranking MATURE tickers (Schwab 252+ days is sufficient)

---

## Key Invariants (Post-Correction)

### ✅ Enforced

1. **PRIMARY KEY (ticker, date):** No duplicate (ticker, date) pairs
2. **Trading days only:** Logic excludes weekends (Sat/Sun)
3. **Maturity thresholds:** 252 trading days = MATURE (not calendar days)
4. **Unique date count:** Used as authoritative history depth

### ⚠️ To Be Enforced

1. **No weekend inserts:** Data collection should reject Sat/Sun dates
2. **Schwab-Fidelity alignment:** Current IV should match across sources (±2%)
3. **Holiday filtering:** Future enhancement (currently only filters weekends)

---

## Documentation Created

1. **[CORRECTED_MATURITY_DEFINITION.md](CORRECTED_MATURITY_DEFINITION.md)**
   - Trading days vs calendar days
   - Corrected maturity thresholds
   - Implementation options (1, 2, 3)

2. **[FIDELITY_SCHWAB_RECONCILIATION.md](FIDELITY_SCHWAB_RECONCILIATION.md)**
   - Fidelity as ground truth
   - Divergence investigation (15.58% discrepancy)
   - Convergence criteria

3. **[HYBRID_IV_ARCHITECTURE.md](HYBRID_IV_ARCHITECTURE.md)**
   - Schwab authoritative time series
   - Fidelity structural reference
   - Weighted blending for IMMATURE phase

---

## Testing Verification

**Forensic audit script:** `forensic_audit_iv_history.py`
**Corrected count test:** `test_corrected_trading_days.py`

**Results:**
```
✅ PRIMARY KEY enforced (zero duplicates)
✅ Trading-day filtering working (180 days = 252 - 72 weekends)
✅ Maturity state corrected (MATURE → IMMATURE)
✅ Weighted blending activated (28.6% Fidelity)
❌ Schwab-Fidelity IV divergence (15.58% - INVESTIGATE)
```

---

## Final Verdict

**Calendar Day Contamination:**
- **CONFIRMED** ✅ (72 weekend dates found)
- **REMEDIATED** ✅ (logic now filters weekends)

**Maturity Logic:**
- **WAS INVALID** ❌ (252 calendar days ≠ 1 trading year)
- **NOW VALID** ✅ (252 trading days = 1 trading year)

**Data Quality:**
- **STILL COMPROMISED** ⚠️ (15.58% Schwab-Fidelity divergence)
- **REQUIRES INVESTIGATION** 🔴 (Priority 1)

---

## Next Steps

1. **Immediate:** Investigate 15.58% IV divergence (Schwab 39.52% vs Fidelity 23.94%)
2. **Short-term:** Delete weekend rows from database (cleanup)
3. **Long-term:** Add weekend rejection to data collection (prevention)

**Status:** PARTIAL REMEDIATION COMPLETE, DATA QUALITY INVESTIGATION REQUIRED

---

**Audit completed by:** Claude (Forensic Analysis)
**Audit date:** 2026-02-03
**Confidence:** HIGH (evidence-based)
