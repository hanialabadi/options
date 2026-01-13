# Phase 3 Pipeline Quick Reference Card

**Version**: Production Hardened (2024-12-28)  
**Status**: ✅ LOCKED & VALIDATED

---

## 🚀 Quick Start

### Run Full Pipeline
```bash
python3 scan_live.py --tickers NVDA,TSLA,AAPL --max-tickers 10
```

### Run Regression Tests
```bash
# First time only: Install pytest
pip install pytest

# Run all tests
python3 -m pytest tests/test_pipeline_invariants.py -v

# Run specific test class
python3 -m pytest tests/test_pipeline_invariants.py::TestStep9BValidationStatusUpdate -v
```

---

## 📊 Pipeline Flow (10-Second Overview)

```
Market Data (Schwab) 
    ↓
Step 2: Phase 1 Enrichment (13 market context columns)
    ↓
Step 9B: Phase 2 Enrichment (4 execution columns) + Contract Fetching
    ├─→ FIX #1: Update Validation_Status (Pending_Greeks → Valid)
    └─→ INVARIANT #1: All OK/LEAP_FALLBACK must be Valid
    ↓
Step 12: Phase 3 Acceptance (7 acceptance columns)
    ├─→ FIX #2: Pre-filter failed Contract_Status → INCOMPLETE
    └─→ INVARIANT #2: All READY_NOW must be Valid + OK/LEAP_FALLBACK
    ↓
Step 8: Position Sizing (filter Validation_Status=='Valid')
    ↓
FINAL TRADES (30 contracts typical from 372 evaluated)
    ↓
Health Summary (CLI + dashboard dict)
```

---

## 🔑 Critical Contracts

### Contract #1: Step 9B → Step 12
**Rule**: All `Contract_Status` in ['OK', 'LEAP_FALLBACK'] MUST have `Validation_Status='Valid'`

**Enforced By**:
- Implementation: `step9b_fetch_contracts_schwab.py:1120-1135`
- Invariant: `pipeline.py:237-256`

**Failure**: ValueError with sample violations, pipeline halts

---

### Contract #2: Step 12 → Step 8
**Rule**: All `acceptance_status='READY_NOW'` MUST have:
1. `Validation_Status == 'Valid'`
2. `Contract_Status` in ['OK', 'LEAP_FALLBACK']

**Enforced By**:
- Implementation: `step12_acceptance.py:574-595` (pre-filter)
- Invariant: `pipeline.py:295-320`

**Failure**: ValueError with violation breakdown, pipeline halts

---

## 📈 Health Summary Metrics

### CLI Output Location
End of pipeline run, before `return results`

### Key Metrics
- **Step 9B success rate**: % of contracts successfully fetched (typical: 40-50%)
- **Step 12 acceptance rate**: % of contracts accepted as READY_NOW (typical: 5-10%)
- **Step 8 conversion rate**: % of READY_NOW converted to final trades (target: 100%)
- **End-to-end rate**: % of evaluated contracts → final trades (typical: 5-10%)

### Dashboard Access
```python
results['pipeline_health'] = {
    'step9b': {'total_contracts': 372, 'valid': 166, 'failed': 206},
    'step12': {'total_evaluated': 372, 'ready_now': 30, 'wait': 88, 'avoid': 48, 'incomplete': 206},
    'step8': {'final_trades': 30},
    'quality': {
        'step9b_success_rate': 44.6,
        'step12_acceptance_rate': 8.1,
        'step8_conversion_rate': 100.0,
        'end_to_end_rate': 8.1
    }
}
```

---

## 🐛 Troubleshooting

### Problem: Dashboard shows 0 trades
**Diagnosis**: Check health summary for Step 8 final_trades count

**Common Causes**:
1. Step 9B didn't update Validation_Status → Check invariant #1 passes
2. Step 12 accepting invalid contracts → Check invariant #2 passes
3. Step 8 filtering too aggressively → Verify filter logic

**Solution**: Run regression tests to verify fixes haven't regressed

---

### Problem: Invariant #1 violation (Step 9B)
**Error**: "PIPELINE INVARIANT VIOLATED (Step 9B): Found N contracts with successful Contract_Status but Validation_Status != 'Valid'"

**Root Cause**: Step 9B failed to update Validation_Status

**Fix Location**: `core/scan_engine/step9b_fetch_contracts_schwab.py` lines 1120-1135

**Check**:
```python
# This logic must be present:
success_statuses = ['OK', 'LEAP_FALLBACK']
successful_contracts = result_df['Contract_Status'].isin(success_statuses)
pending_greeks = result_df['Validation_Status'] == 'Pending_Greeks'
contracts_to_update = successful_contracts & pending_greeks
result_df.loc[contracts_to_update, 'Validation_Status'] = 'Valid'
```

---

### Problem: Invariant #2 violation (Step 12)
**Error**: "PIPELINE INVARIANT VIOLATED (Step 12): Check 2 FAILED: N READY_NOW contracts with failed Contract_Status"

**Root Cause**: Step 12 accepting contracts with failed Contract_Status

**Fix Location**: `core/scan_engine/step12_acceptance.py` lines 574-595

**Check**:
```python
# This pre-filter must be present:
successful_statuses = ['OK', 'LEAP_FALLBACK']
failed_contracts = ~df_result['Contract_Status'].isin(successful_statuses)
df_result.loc[failed_contracts, 'acceptance_status'] = 'INCOMPLETE'
```

---

### Problem: Tests failing
**Diagnosis**: Run tests with detailed output
```bash
python3 -m pytest tests/test_pipeline_invariants.py -v --tb=long
```

**Common Issues**:
1. pytest not installed → `pip install pytest`
2. Import errors → Verify `PYTHONPATH` includes project root
3. Specific test fails → Check corresponding implementation file

---

## 📁 Key Files

### Pipeline Core
- `core/scan_engine/pipeline.py` - Orchestration + invariant checks + health summary
- `core/scan_engine/step2_enrich.py` - Phase 1 market context
- `core/scan_engine/step9b_fetch_contracts_schwab.py` - Phase 2 execution context + Fix #1
- `core/scan_engine/step12_acceptance.py` - Phase 3 acceptance logic + Fix #2

### Testing
- `tests/test_pipeline_invariants.py` - Regression test suite (8 tests)
- `tests/TEST_README.md` - Test running guide

### Documentation
- `PHASE_3_COMPLETE_SUMMARY.md` - Complete implementation overview
- `DASHBOARD_STEP8_FIX_COMPLETE.md` - Step 8 compatibility fix details
- `PIPELINE_HARDENING_COMPLETE.md` - Hardening implementation details
- `PIPELINE_QUICK_REFERENCE.md` - This file

---

## 🎯 Acceptance Criteria

### Phase 3 is COMPLETE when:
- ✅ All 3 strategy types implemented (DIRECTIONAL, INCOME, VOLATILITY)
- ✅ Step 12 integrated into pipeline (Step 2→12→8 flow)
- ✅ Dashboard and CLI agree on trade count
- ✅ Both invariant checks pass (Step 9B + Step 12)
- ✅ Health summary displayed in CLI
- ✅ Health summary stored in results dict
- ✅ All regression tests pass (8/8)

### Current Status:
✅ **ALL CRITERIA MET** - Phase 3 is PRODUCTION READY

---

## 🚨 Emergency Rollback

If critical regression detected:

1. **Revert to last stable commit**:
   ```bash
   git log --oneline  # Find last stable commit
   git revert <commit-hash>
   ```

2. **Or use git tag**:
   ```bash
   git checkout phase_1_2_3_complete  # Before hardening
   git checkout phase_3_production_ready  # Current stable
   ```

3. **Verify tests pass**:
   ```bash
   python3 -m pytest tests/test_pipeline_invariants.py -v
   ```

4. **Run limited pipeline**:
   ```bash
   python3 scan_live.py --tickers NVDA,TSLA --max-tickers 2
   ```

---

## 📞 Support & Resources

### Documentation Links
- Full implementation: [PHASE_3_COMPLETE_SUMMARY.md](PHASE_3_COMPLETE_SUMMARY.md)
- Step 8 fix: [DASHBOARD_STEP8_FIX_COMPLETE.md](DASHBOARD_STEP8_FIX_COMPLETE.md)
- Hardening details: [PIPELINE_HARDENING_COMPLETE.md](PIPELINE_HARDENING_COMPLETE.md)
- Test guide: [tests/TEST_README.md](tests/TEST_README.md)

### Common Commands
```bash
# Full pipeline run
python3 scan_live.py --tickers NVDA,TSLA,AAPL --max-tickers 10

# Regression tests
python3 -m pytest tests/test_pipeline_invariants.py -v

# Check pipeline health (last run)
ls -lt output/Step8_Final_*.csv | head -n 1

# View CSV exports
cd output/
ls -lt *.csv | head -n 10
```

---

**Last Updated**: 2024-12-28  
**Version**: Production Hardened  
**Status**: ✅ LOCKED & VALIDATED
