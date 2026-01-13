# BLOCKING EVIDENCE REPORT

**Source:** audit_steps/*.csv, audit_trace/*.csv  
**Analysis Date:** 2026-01-03  
**Tickers Analyzed:** AAPL, MSFT, NVDA

---

## EXECUTIVE SUMMARY

**Universal Blocking Condition:**
- **Earliest Detectable:** Step 01 (iv_surface_age_days = 4)
- **Earliest Enforcement:** Step 09 (Acceptance Logic)
- **Boolean Condition:** `(iv_rank_available == False) AND (iv_history_days < 120)`
- **Result:** `acceptance_status != 'READY_NOW'` for all strategies

---

## TICKER: AAPL

### Earliest READY_NOW Blocking Step

**Step 09 – Acceptance Logic (Enforcement)**

### Exact Boolean Conditions

```python
# Primary blocking condition (applies to ALL strategies):
(iv_rank_available == False)  # Evaluates to: True
(iv_history_days < 120)       # Evaluates to: True (actual: 4)

# Result:
acceptance_status != 'READY_NOW'  # Evaluates to: True

# Additional blocking for Strategy 1:
(Contract_Status == 'FAILED_LIQUIDITY_FILTER')  # Evaluates to: True
(Validation_Status == 'Pending_Greeks')         # Evaluates to: True
```

### Evidence from CSVs

**Step 01 – Snapshot Enrichment**
```csv
Ticker,iv_surface_source,iv_surface_age_days
AAPL,historical_latest,4
```

**Step 09 – Acceptance Logic**
```csv
Ticker,Strategy_ID,acceptance_status,iv_rank_available,iv_history_days,Contract_Status,Validation_Status,Theory_Compliance_Score
AAPL,1,INCOMPLETE,False,4,FAILED_LIQUIDITY_FILTER,Pending_Greeks,50.0
AAPL,2,WAIT,False,4,OK,Valid,50.0
```

### Failure Classification

| Failure Type | Specific Condition | Category |
|--------------|-------------------|----------|
| **Primary** | `iv_history_days < 120` | Data Insufficiency |
| **Secondary** | `Contract_Status = FAILED_LIQUIDITY_FILTER` (Strategy 1) | Contract Validation Failure |

### Reversibility Analysis

| Failure | Reversible with more data? | Reversible by relaxing rules? | Details |
|---------|---------------------------|------------------------------|---------|
| Data Insufficiency (iv_rank_available = False) | **YES** | **NO** | Need 116 more days of IV history (4 → 120) |
| Rule-Based Gate (iv_history_days < 120) | **YES** | **NO** | Hard constraint for IV Rank statistical validity |
| Contract Validation (FAILED_LIQUIDITY_FILTER) | **POTENTIALLY** | **NO** | Depends on market liquidity improvement |

---

## TICKER: MSFT

### Earliest READY_NOW Blocking Step

**Step 09 – Acceptance Logic (Enforcement)**

### Exact Boolean Conditions

```python
# Primary blocking condition (applies to ALL strategies):
(iv_rank_available == False)  # Evaluates to: True
(iv_history_days < 120)       # Evaluates to: True (actual: 4)

# Result:
acceptance_status != 'READY_NOW'  # Evaluates to: True

# Additional blocking for Strategy 1:
(Contract_Status == 'FAILED_LIQUIDITY_FILTER')  # Evaluates to: True
(Validation_Status == 'Pending_Greeks')         # Evaluates to: True
```

### Evidence from CSVs

**Step 01 – Snapshot Enrichment**
```csv
Ticker,iv_surface_source,iv_surface_age_days
MSFT,historical_latest,4
```

**Step 09 – Acceptance Logic**
```csv
Ticker,Strategy_ID,acceptance_status,iv_rank_available,iv_history_days,Contract_Status,Validation_Status,Theory_Compliance_Score
MSFT,1,INCOMPLETE,False,4,FAILED_LIQUIDITY_FILTER,Pending_Greeks,50.0
MSFT,2,WAIT,False,4,OK,Valid,50.0
```

### Failure Classification

| Failure Type | Specific Condition | Category |
|--------------|-------------------|----------|
| **Primary** | `iv_history_days < 120` | Data Insufficiency |
| **Secondary** | `Contract_Status = FAILED_LIQUIDITY_FILTER` (Strategy 1) | Contract Validation Failure |

### Reversibility Analysis

| Failure | Reversible with more data? | Reversible by relaxing rules? | Details |
|---------|---------------------------|------------------------------|---------|
| Data Insufficiency (iv_rank_available = False) | **YES** | **NO** | Need 116 more days of IV history (4 → 120) |
| Rule-Based Gate (iv_history_days < 120) | **YES** | **NO** | Hard constraint for IV Rank statistical validity |
| Contract Validation (FAILED_LIQUIDITY_FILTER) | **POTENTIALLY** | **NO** | Depends on market liquidity improvement |

---

## TICKER: NVDA

### Earliest READY_NOW Blocking Step

**Step 09 – Acceptance Logic (Enforcement)**

### Exact Boolean Conditions

```python
# Primary blocking condition (applies to ALL strategies):
(iv_rank_available == False)  # Evaluates to: True
(iv_history_days < 120)       # Evaluates to: True (actual: 4)

# Result:
acceptance_status != 'READY_NOW'  # Evaluates to: True

# Additional blocking for ALL strategies:
(Theory_Compliance_Score < 60)  # Evaluates to: True (actual: 50.0)
```

### Evidence from CSVs

**Step 01 – Snapshot Enrichment**
```csv
Ticker,iv_surface_source,iv_surface_age_days
NVDA,historical_latest,4
```

**Step 09 – Acceptance Logic**
```csv
Ticker,Strategy_ID,acceptance_status,iv_rank_available,iv_history_days,Contract_Status,Validation_Status,Theory_Compliance_Score
NVDA,1,STRUCTURALLY_READY,False,4,OK,Valid,50.0
NVDA,2,WAIT,False,4,OK,Valid,50.0
NVDA,3,STRUCTURALLY_READY,False,4,OK,Valid,50.0
```

### Failure Classification

| Failure Type | Specific Condition | Category |
|--------------|-------------------|----------|
| **Primary** | `iv_history_days < 120` | Data Insufficiency |
| **Secondary** | `Theory_Compliance_Score < 60` | Rule-Based Gate |

### Reversibility Analysis

| Failure | Reversible with more data? | Reversible by relaxing rules? | Details |
|---------|---------------------------|------------------------------|---------|
| Data Insufficiency (iv_rank_available = False) | **YES** | **NO** | Need 116 more days of IV history (4 → 120) |
| Rule-Based Gate (iv_history_days < 120) | **YES** | **NO** | Hard constraint for IV Rank statistical validity |
| Rule-Based Gate (Theory_Compliance_Score < 60) | **POTENTIALLY** | **NO** | Score may improve with fresh Greeks, but gate remains enforced |

---

## CODE EVIDENCE

### Boolean Condition Source

**File:** [core/scan_engine/step12_acceptance.py](core/scan_engine/step12_acceptance.py#L741-L770)

```python
# Lines 741-770: IV Availability Gate
if 'iv_rank_available' in df_result.columns:
    insufficient_iv_mask = (
        (df_result['acceptance_status'] == 'READY_NOW') &
        (~df_result['iv_rank_available'])
    )
    
    insufficient_iv_count = insufficient_iv_mask.sum()
    if insufficient_iv_count > 0:
        logger.info(f"\n📊 IV availability gate: {insufficient_iv_count} READY_NOW strategies lack sufficient IV history")
        logger.info(f"   Downgrading to STRUCTURALLY_READY (requires 120+ days of IV data)")
        
        # Downgrade READY_NOW → STRUCTURALLY_READY
        for idx in df_result[insufficient_iv_mask].index:
            iv_history = int(df_result.loc[idx, 'iv_history_days'])
            iv_diagnostic = get_iv_diagnostic_reason(iv_history)
            
            current_reason = df_result.loc[idx, 'acceptance_reason']
            df_result.at[idx, 'acceptance_status'] = 'STRUCTURALLY_READY'
            df_result.at[idx, 'acceptance_reason'] = f"{current_reason} ({iv_diagnostic})"
```

**Key Logic:**
```python
# READY_NOW requires:
(acceptance_status == 'READY_NOW') AND (iv_rank_available == True)

# If iv_rank_available == False:
acceptance_status = 'STRUCTURALLY_READY'  # Downgraded

# iv_rank_available determined by:
(iv_history_days >= 120)  # Hard constraint
```

---

## CROSS-TICKER COMPARISON

| Ticker | Blocking Step | Primary Condition | Secondary Condition | Status Assigned |
|--------|---------------|-------------------|---------------------|-----------------|
| AAPL | Step 09 | `iv_history_days < 120` | `Contract_Status = FAILED_LIQUIDITY_FILTER` (Strategy 1) | INCOMPLETE, WAIT |
| MSFT | Step 09 | `iv_history_days < 120` | `Contract_Status = FAILED_LIQUIDITY_FILTER` (Strategy 1) | INCOMPLETE, WAIT |
| NVDA | Step 09 | `iv_history_days < 120` | `Theory_Compliance_Score < 60` | STRUCTURALLY_READY, WAIT |

**Universal Truth:**
```python
∀ ticker ∈ {AAPL, MSFT, NVDA}:
    iv_rank_available == False
    ∧ iv_history_days == 4
    ∧ iv_history_days < 120
    ⟹ acceptance_status != 'READY_NOW'
```

---

## REVERSIBILITY MATRIX

| Condition | Type | Reversible with Data? | Reversible with Rule Change? | Timeline |
|-----------|------|----------------------|----------------------------|----------|
| `iv_rank_available = False` | Data Insufficiency | ✅ YES | ❌ NO | ~116 days (organic accumulation) |
| `iv_history_days < 120` | Rule-Based Gate | ✅ YES | ❌ NO | ~116 days (same as above) |
| `Contract_Status = FAILED_LIQUIDITY_FILTER` | Contract Validation | ⚠️ POTENTIALLY | ❌ NO | Market-dependent (unpredictable) |
| `Theory_Compliance_Score < 60` | Rule-Based Gate | ⚠️ POTENTIALLY | ❌ NO | Greeks-dependent (could improve with fresh data) |

**Legend:**
- ✅ YES: Reversible with certainty
- ⚠️ POTENTIALLY: Reversible under certain conditions
- ❌ NO: Not reversible by design (hard constraint)

---

## DETECTION VS ENFORCEMENT

| Step | Phase | Action | Evidence Available? |
|------|-------|--------|---------------------|
| **Step 01** | Detection | Load IV surface metadata (`iv_surface_age_days = 4`) | ✅ YES (iv_surface_age_days column) |
| **Step 02** | Pass-Through | No IV metadata columns present | ❌ NO (column not added yet) |
| **Step 09** | Enforcement | Apply IV availability gate (`iv_rank_available = False`) | ✅ YES (iv_rank_available column) |

**Key Insight:**
- IV insufficiency is **detectable at Step 01** (iv_surface_age_days = 4)
- IV insufficiency is **enforced at Step 09** (acceptance_status assignment)
- Pipeline allows progression through intermediate steps for audit purposes

---

## APPENDIX: RAW CSV EVIDENCE

### Step 01 – All Tickers
```csv
Ticker,iv_surface_source,iv_surface_age_days
AAPL,historical_latest,4
MSFT,historical_latest,4
NVDA,historical_latest,4
```

### Step 09 – All Tickers (Complete)
```csv
Ticker,Strategy_ID,acceptance_status,acceptance_reason,iv_rank_available,iv_history_days,Contract_Status,Validation_Status,Theory_Compliance_Score,confidence_band
AAPL,1,INCOMPLETE,Contract validation failed (Step 9B),False,4,FAILED_LIQUIDITY_FILTER,Pending_Greeks,50.0,LOW
AAPL,2,WAIT,Moderate directional (awaiting full evaluation - score < 60),False,4,OK,Valid,50.0,MEDIUM
MSFT,1,INCOMPLETE,Contract validation failed (Step 9B),False,4,FAILED_LIQUIDITY_FILTER,Pending_Greeks,50.0,LOW
MSFT,2,WAIT,Moderate directional (awaiting full evaluation - score < 60),False,4,OK,Valid,50.0,MEDIUM
NVDA,1,STRUCTURALLY_READY,Strong momentum + favorable timing (awaiting full evaluation - score < 60),False,4,OK,Valid,50.0,HIGH
NVDA,2,WAIT,Moderate directional (awaiting full evaluation - score < 60),False,4,OK,Valid,50.0,MEDIUM
NVDA,3,STRUCTURALLY_READY,Strong momentum + favorable timing (awaiting full evaluation - score < 60),False,4,OK,Valid,50.0,HIGH
```

---

## END OF REPORT

**Analysis Method:** Direct CSV reads with pandas, no inference  
**Boolean Conditions:** Extracted from step12_acceptance.py source code  
**Reversibility Assessment:** Based on rule design and data accumulation mechanics  
**No Recommendations Provided:** Evidence-only report as requested
