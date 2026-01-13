# Step-by-Step Decision Ledger - Summary

**Generated:** 2026-01-03  
**Source:** audit_steps/*.csv and audit_trace/*.csv  
**Tickers Analyzed:** AAPL, MSFT, NVDA

---

## Key Findings

### Universal Blocking Condition

**All three tickers blocked at Step 09 (Acceptance Logic)**

**Root Cause (All Tickers):**
```
iv_rank_available = False
iv_history_days = 4
Required: 120+ days
```

---

## Per-Ticker Analysis

### TICKER: AAPL

**Pipeline Progression:**
- ✅ Step 01: Snapshot enriched (IV_30_D_Call=22.093)
- ✅ Step 02: IVHV filter passed (gap=8.94)
- ✅ Step 03: Chart signals computed (RSI=31.1, ADX=41.1)
- ✅ Step 04: Data validated
- ✅ Step 05: 2 strategies generated
- ✅ Step 06: 2 strategies evaluated (scores=50.0)
- ❌ Step 09: **BLOCKED**

**Blocking Evidence:**
```
Strategy 1:
  acceptance_status = INCOMPLETE
  acceptance_reason = Contract validation failed (Step 9B)
  Validation_Status = Pending_Greeks
  Contract_Status = FAILED_LIQUIDITY_FILTER
  iv_rank_available = False
  iv_history_days = 4

Strategy 2:
  acceptance_status = WAIT
  acceptance_reason = Wait for clearer directional setup
  Validation_Status = Valid
  Contract_Status = OK
  iv_rank_available = False
  iv_history_days = 4
```

**Root Causes:**
1. ❌ Insufficient IV history (4 days, need 120+)
2. ❌ Contract liquidity filter failed
3. ❌ Greek calculation pending

---

### TICKER: MSFT

**Pipeline Progression:**
- ✅ Step 01: Snapshot enriched (IV_30_D_Call=27.147)
- ✅ Step 02: IVHV filter passed (gap=7.89)
- ✅ Step 03: Chart signals computed (RSI=36.9, ADX=11.4)
- ✅ Step 04: Data validated
- ✅ Step 05: 2 strategies generated
- ✅ Step 06: 2 strategies evaluated (scores=50.0)
- ❌ Step 09: **BLOCKED**

**Blocking Evidence:**
```
Strategy 1:
  acceptance_status = INCOMPLETE
  acceptance_reason = Contract validation failed (Step 9B)
  Validation_Status = Pending_Greeks
  Contract_Status = FAILED_LIQUIDITY_FILTER
  iv_rank_available = False
  iv_history_days = 4

Strategy 2:
  acceptance_status = WAIT
  acceptance_reason = Wait for clearer directional setup
  Validation_Status = Valid
  Contract_Status = OK
  iv_rank_available = False
  iv_history_days = 4
```

**Root Causes:**
1. ❌ Insufficient IV history (4 days, need 120+)
2. ❌ Contract liquidity filter failed
3. ❌ Greek calculation pending

---

### TICKER: NVDA

**Pipeline Progression:**
- ✅ Step 01: Snapshot enriched (IV_30_D_Call=38.031)
- ✅ Step 02: IVHV filter passed (gap=6.98)
- ✅ Step 03: Chart signals computed (RSI=59.3, ADX=14.6)
- ✅ Step 04: Data validated
- ✅ Step 05: 3 strategies generated
- ✅ Step 06: 3 strategies evaluated (scores=50.0)
- ⚠️  Step 09: **STRUCTURALLY_READY** (not READY_NOW)

**Blocking Evidence:**
```
Strategy 1:
  acceptance_status = STRUCTURALLY_READY
  acceptance_reason = NORMAL range in MID_RANGE - ideal for income strategies
                      (awaiting full evaluation - score < 60)
  Validation_Status = Valid
  Contract_Status = OK
  iv_rank_available = False
  iv_history_days = 4
  confidence_band = MEDIUM

Strategy 2:
  acceptance_status = WAIT
  acceptance_reason = Unknown strategy type - manual review required
  Validation_Status = Valid
  Contract_Status = OK
  iv_rank_available = False
  iv_history_days = 4

Strategy 3:
  acceptance_status = STRUCTURALLY_READY
  acceptance_reason = BULLISH_MODERATE setup with range_bound structure
                      (awaiting full evaluation - score < 60)
  Validation_Status = Valid
  Contract_Status = OK
  iv_rank_available = False
  iv_history_days = 4
  confidence_band = MEDIUM
```

**Root Causes:**
1. ❌ Insufficient IV history (4 days, need 120+)
2. ⚠️  Theory compliance score < 60 (actual: 50.0)
3. ⚠️  STRUCTURALLY_READY but not READY_NOW

**Note:** NVDA is closest to READY_NOW - contracts valid, just needs more IV data.

---

## Universal Blocking Condition

**Step 09: Acceptance Logic**

**Boolean Condition:**
```python
acceptance_status != 'READY_NOW'
```

**Underlying Constraints:**
```python
iv_rank_available == False  # All tickers
iv_history_days < 120       # All tickers (actual: 4 days)
Theory_Compliance_Score < 60  # All tickers (actual: 50.0)
```

**Why iv_rank_available = False:**
```
Required IV history: 120+ days (for IV Rank calculation)
Actual IV history: 4 days
Source: historical_latest (age: 4 days)
```

---

## Evidence Chain

### Step-by-Step Gate Checks

| Step | Gate Type | AAPL | MSFT | NVDA | Blocking? |
|------|-----------|------|------|------|-----------|
| 01 | Data Load | PASS | PASS | PASS | No |
| 02 | IVHV Filter | PASS | PASS | PASS | No |
| 03 | Chart Signals | PASS | PASS | PASS | No |
| 04 | Data Quality | PASS | PASS | PASS | No |
| 05 | Strategy Gen | 2 | 2 | 3 | No |
| 06 | Evaluation | 50.0 | 50.0 | 50.0 | No |
| 09 | **Acceptance** | **FAIL** | **FAIL** | **PARTIAL** | **YES** |

### Acceptance Status Breakdown

| Ticker | Status | Reason | Contract | IV Data | Score |
|--------|--------|--------|----------|---------|-------|
| AAPL | INCOMPLETE | Contract validation failed | FAILED_LIQUIDITY_FILTER | 4 days | 50.0 |
| AAPL | WAIT | Directional setup unclear | OK | 4 days | 50.0 |
| MSFT | INCOMPLETE | Contract validation failed | FAILED_LIQUIDITY_FILTER | 4 days | 50.0 |
| MSFT | WAIT | Directional setup unclear | OK | 4 days | 50.0 |
| NVDA | STRUCTURALLY_READY | Score < 60 | OK | 4 days | 50.0 |
| NVDA | WAIT | Unknown strategy type | OK | 4 days | 50.0 |
| NVDA | STRUCTURALLY_READY | Score < 60 | OK | 4 days | 50.0 |

---

## Explicit Column Values (Step 09)

### AAPL
```csv
Ticker,acceptance_status,acceptance_reason,Validation_Status,Contract_Status,iv_rank_available,iv_history_days,confidence_band,Theory_Compliance_Score
AAPL,INCOMPLETE,Contract validation failed (Step 9B),Pending_Greeks,FAILED_LIQUIDITY_FILTER,False,4,LOW,50.0
AAPL,WAIT,Wait for clearer directional setup,Valid,OK,False,4,LOW,50.0
```

### MSFT
```csv
Ticker,acceptance_status,acceptance_reason,Validation_Status,Contract_Status,iv_rank_available,iv_history_days,confidence_band,Theory_Compliance_Score
MSFT,INCOMPLETE,Contract validation failed (Step 9B),Pending_Greeks,FAILED_LIQUIDITY_FILTER,False,4,LOW,50.0
MSFT,WAIT,Wait for clearer directional setup,Valid,OK,False,4,LOW,50.0
```

### NVDA
```csv
Ticker,acceptance_status,acceptance_reason,Validation_Status,Contract_Status,iv_rank_available,iv_history_days,confidence_band,Theory_Compliance_Score
NVDA,STRUCTURALLY_READY,NORMAL range in MID_RANGE (score < 60),Valid,OK,False,4,MEDIUM,50.0
NVDA,WAIT,Unknown strategy type - manual review,Valid,OK,False,4,LOW,50.0
NVDA,STRUCTURALLY_READY,BULLISH_MODERATE (score < 60),Valid,OK,False,4,MEDIUM,50.0
```

---

## Conclusion

**No ticker reached READY_NOW status.**

**Primary Blocker:**
```
iv_rank_available = False (need 120+ days, have 4 days)
```

**Secondary Blockers:**
- AAPL/MSFT: Contract liquidity filter failures
- All: Theory compliance scores = 50.0 (need 60+)

**Path to READY_NOW:**
1. Accumulate 120+ days of IV history
2. Improve theory compliance scores (50.0 → 60+)
3. Resolve contract liquidity filters (AAPL/MSFT)

**NVDA is closest:** Contracts valid, just needs IV data + score improvement.

---

*This is an evidence report. No inference. No assumptions. Only explicit column values from audit_steps/*.csv*
