# Quick Start: Running Pipeline Hardening Tests

## Prerequisites

Install pytest (not currently in requirements.txt):
```bash
pip install pytest
```

Or add to requirements.txt:
```
pytest>=7.4.0
```

## Running Tests

**Full regression suite** (8 tests):
```bash
python3 -m pytest tests/test_pipeline_invariants.py -v
```

**Individual test class**:
```bash
# Test Step 9B validation status update
python3 -m pytest tests/test_pipeline_invariants.py::TestStep9BValidationStatusUpdate -v

# Test Step 12 pre-filter logic
python3 -m pytest tests/test_pipeline_invariants.py::TestStep12PreFilter -v

# Test Step 12 invariants
python3 -m pytest tests/test_pipeline_invariants.py::TestStep12Invariants -v

# Test end-to-end happy path
python3 -m pytest tests/test_pipeline_invariants.py::TestEndToEndHappyPath -v

# Test health summary structure
python3 -m pytest tests/test_pipeline_invariants.py::TestPipelineHealthSummary -v
```

**With detailed output**:
```bash
python3 -m pytest tests/test_pipeline_invariants.py -v --tb=long
```

## Expected Output

All 8 tests should pass:
```
tests/test_pipeline_invariants.py::TestStep9BValidationStatusUpdate::test_successful_contracts_get_valid_status PASSED
tests/test_pipeline_invariants.py::TestStep9BValidationStatusUpdate::test_pipeline_invariant_check_step9b PASSED
tests/test_pipeline_invariants.py::TestStep12PreFilter::test_failed_contracts_marked_incomplete PASSED
tests/test_pipeline_invariants.py::TestStep12PreFilter::test_only_valid_contracts_evaluated PASSED
tests/test_pipeline_invariants.py::TestStep12Invariants::test_ready_now_must_be_valid PASSED
tests/test_pipeline_invariants.py::TestStep12Invariants::test_ready_now_must_have_successful_contract_status PASSED
tests/test_pipeline_invariants.py::TestEndToEndHappyPath::test_happy_path_all_valid PASSED
tests/test_pipeline_invariants.py::TestPipelineHealthSummary::test_health_summary_structure PASSED

======================================== 8 passed in 0.45s =========================================
```

## Test Coverage

✅ **Step 9B Validation Status Update** (2 tests)
- Verifies Fix #1: Validation_Status update for successful contracts
- Tests pipeline invariant detection

✅ **Step 12 Pre-Filter** (2 tests)  
- Verifies Fix #2: Failed contracts marked INCOMPLETE
- Tests pre-filter correctness

✅ **Step 12 Invariants** (2 tests)
- Tests READY_NOW must have Valid status
- Tests READY_NOW must have successful Contract_Status

✅ **End-to-End Happy Path** (1 test)
- Tests complete pipeline flow with valid data

✅ **Pipeline Health Summary** (1 test)
- Validates health summary dict structure and metrics

## Next Steps After Tests Pass

1. Run full E2E pipeline with hardening:
   ```bash
   python3 scan_live.py --tickers NVDA,TSLA,AAPL --max-tickers 10
   ```

2. Verify health summary appears in CLI output

3. Verify health summary stored in results['pipeline_health']

4. Update dashboard to display health metrics

## Troubleshooting

If tests fail with import errors:
```bash
# Make sure you're in the project root
cd /Users/haniabadi/Documents/Github/options

# Verify Python can import core modules
python3 -c "from core.scan_engine import pipeline; print('OK')"
```

If specific tests fail, check:
- Step 9B: `core/scan_engine/step9b_fetch_contracts_schwab.py` lines 1120-1135
- Step 12: `core/scan_engine/step12_acceptance.py` lines 574-595
- Pipeline: `core/scan_engine/pipeline.py` lines 237-256, 295-320, 380-580
