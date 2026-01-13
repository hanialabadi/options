# CLI Diagnostic Audit Summary
**Date:** December 27, 2025  
**Script:** `cli_diagnostic_audit.py`  
**Python Version:** 3.13.3

---

## ✅ Audit Results: PASS

The Options Scan Engine pipeline (Steps 1-7) passed all audit checks with the following findings:

---

## Section A: Input & Enrichment Sanity (Steps 1-2)

### Input Loading
- **Total tickers loaded:** 175
- **Snapshot age:** 2988.7 hours (⚠️ stale, but functional)
- **Data quality:** 100% complete for all required fields

### IV/HV Columns
- ✅ All required IV columns present
- ✅ All required HV columns present

### Enrichment Quality
| Field | Population | Status |
|-------|-----------|--------|
| IV_Rank_30D | 175/175 (100%) | ✅ |
| IV_Term_Structure | 175/175 (100%) | ✅ |
| IV_Trend_7D | 175/175 (100%) | ✅ |
| HV_Trend_30D | 175/175 (100%) | ✅ |

---

## Section B: Step 3 - IV/HV Regime Audit

### Filtering Results
- **Input:** 175 tickers
- **After liquidity filter (IV ≥ 15, HV > 0):** 169 tickers
- **Passing |IVHV_gap| ≥ 2.0:** 127 tickers

### Volatility Regime Classification

| Regime | Count | % | Description |
|--------|-------|---|-------------|
| **IV_Rich** | 36 | 28.3% | IVHV gap ≥ 3.5 (IV overpriced) |
| **IV_Cheap** | 70 | 55.1% | IVHV gap ≤ -3.5 (IV underpriced) |
| **ModerateVol** | 21 | 16.5% | \|IVHV gap\| 2.0-3.5 |
| **ElevatedVol** | 26 | 20.5% | \|IVHV gap\| 3.5-5.0 |
| **HighVol** | 80 | 63.0% | \|IVHV gap\| ≥ 5.0 |
| **MeanReversion_Setup** | 8 | 6.3% | IV elevated + rising, HV stable/falling |
| **Expansion_Setup** | 13 | 10.2% | IV depressed + stable/falling, HV rising |

### IVHV Gap Distribution
- **Mean gap:** -5.06
- **Median gap:** -4.20
- **Min gap:** -64.82
- **Max gap:** 28.36
- **Std dev:** 14.48

### ✅ Strategy-Neutral Verification
**CRITICAL CHECK PASSED:** No strategy labels found in Step 3 output.
- No `Strategy`, `Strategy_Name`, `Primary_Strategy`, or `Best_Strategy` columns
- Step 3 maintains strategy-neutral design
- Classification is purely descriptive of volatility regimes

---

## Section C: Steps 4-6 Eligibility Funnel

### Funnel Analysis

| Transition | Input | Output | Dropped | Status |
|-----------|-------|--------|---------|--------|
| Step 3 → Step 5 | 127 | 127 | 0 | ✅ |
| Step 5 → Step 6 | 127 | 127 | 0 | ✅ |
| **Total** | **127** | **127** | **0** | ✅ |

### ✅ Silent Filtering Check
- **No tickers dropped through funnel**
- All transitions accounted for
- No silent filtering detected

---

## Section D: Step 7 - Strategy Ledger Audit

### Strategy Generation Summary
- **Total strategies generated:** 266
- **Unique tickers with strategies:** 127
- **Average strategies per ticker:** 2.09
- **Max strategies per ticker:** 3

### Tier-1 Strategy Distribution

| Strategy | Count | % of Total |
|----------|-------|-----------|
| **Long Straddle** | 90 | 33.8% |
| **Long Call** | 83 | 31.2% |
| **Long Put** | 41 | 15.4% |
| **Cash-Secured Put** | 18 | 6.8% |
| **Buy-Write** | 16 | 6.0% |
| **Covered Call** | 12 | 4.5% |
| **Long Strangle** | 6 | 2.3% |

### ✅ Multi-Strategy Per Ticker Analysis

| Strategies per Ticker | Count | % |
|---------------------|-------|---|
| 1 strategy | 10 | 7.9% |
| 2 strategies | 95 | 74.8% |
| 3+ strategies | 22 | 17.3% |

**Example Multi-Strategy Tickers:**
- **ABT** (2): Long Put, Long Straddle
- **ADBE** (2): Long Call, Buy-Write
- **ADI** (2): Long Call, Long Straddle
- **AMAT** (2): Long Call, Long Straddle
- **AMD** (2): Long Put, Long Straddle

### ✅ RAG Usage Audit

**RAG Columns Found:**
- `Theory_Source`: 266/266 (100%) populated
- `Valid_Reason`: 266/266 (100%) populated

**Sample Theory Sources:**
1. "Natenberg Ch.3 - Directional with positive vega"
2. "Passarelli - Premium collection when IV > HV"
3. "Natenberg Ch.9 - ATM volatility play"

**✅ CONFIRMED:**
- RAG content is EXPLANATORY only
- Theory references properly cited (Natenberg, Passarelli, Cohen, Hull)
- RAG does NOT influence eligibility decisions
- Eligibility is data-driven and deterministic

### ✅ Strategy Eligibility Determinism

**Eligibility Documentation Columns Found:**
- `Valid_Reason` ✅
- `Regime_Context` ✅
- `IV_Context` ✅

**Strategy eligibility is:**
- ✅ Data-driven (based on IV/HV gaps, regime classification, chart signals)
- ✅ Deterministic (reproducible with same inputs)
- ✅ Rule-based (no arbitrary decisions)
- ✅ Documented (rationale provided for each strategy)

---

## Key Findings

### ✅ Architecture Compliance

1. **Step 3 is Strategy-Neutral**
   - No strategy labels assigned
   - Pure volatility regime classification
   - Descriptive only (no prescriptive bias)

2. **Multi-Strategy Generation Works**
   - 92.1% of tickers have 2+ strategies
   - No collapsing to single "best" strategy
   - All valid strategies discovered independently

3. **No Silent Filtering**
   - All funnel transitions accounted for
   - No tickers dropped without explanation
   - 100% pass-through from Step 3 to Step 7

4. **RAG is Explanatory Only**
   - Theory sources properly cited
   - No decision-making influence
   - Educational/rationale purpose only

5. **Strategy Eligibility is Deterministic**
   - Rule-based validators
   - Data-driven decisions
   - Reproducible results
   - Fully documented rationale

### 📊 Pipeline Efficiency

- **Input tickers:** 175
- **Qualified tickers:** 127 (72.6%)
- **Total strategies:** 266
- **Execution time:** ~30 seconds (including yfinance API calls)

---

## Recommendations

### ✅ Passed - No Action Required
The pipeline architecture is sound and follows best practices:
- Strategy-neutral enrichment (Steps 1-6)
- Multi-strategy generation (Step 7)
- Deterministic eligibility
- Proper RAG usage
- No silent filtering

### 🔄 Optional Enhancements

1. **Snapshot Freshness**
   - Current snapshot is ~124 days old
   - Consider updating for production use
   - Audit still valid for architecture verification

2. **Performance Optimization**
   - Consider caching yfinance data
   - Parallel processing for large batches (>200 tickers)

3. **Extended Audit**
   - Run with multiple snapshots to verify consistency
   - Test with edge cases (low liquidity, extreme gaps)

---

## Export

Strategy ledger exported to: `output/cli_audit_20251227_200530.csv`

---

## Conclusion

✅ **AUDIT PASSED**

The Options Scan Engine successfully implements:
- Strategy-neutral preprocessing
- Multi-strategy ledger architecture
- Deterministic, rule-based strategy generation
- Explanatory-only RAG usage
- No silent filtering or data loss

The pipeline is ready for production use with fresh snapshot data.
