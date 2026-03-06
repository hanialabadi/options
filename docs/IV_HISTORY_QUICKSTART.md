# IV History System - Quick Start Guide

**Date:** 2026-02-03
**Phase:** 4 - IV History System Implementation
**Status:** ✅ READY FOR TESTING

---

## What Was Implemented

### 1. DuckDB IV History Table ✅
- **File**: `core/shared/data_layer/iv_term_history.py`
- **Schema**: Stores 8 constant-maturity IV points per ticker per day
- **Functions**:
  - `calculate_iv_rank()` - IV Rank from historical data
  - `get_iv_maturity_state()` - MATURE/IMMATURE/MISSING classification
  - `get_iv_history_depth()` - Days of history available
  - `append_daily_iv_data()` - Daily collection persistence

### 2. Daily Collection Job ✅
- **File**: `scripts/daily_jobs/collect_iv_history.py`
- **Purpose**: Fetch IV from Schwab API and populate DuckDB
- **Schedule**: Daily, after market close (5:00 PM ET)

### 3. Scan Engine Refactor ✅
- **File**: `scan_engine/step2_load_snapshot.py`
- **Change**: Added `_enrich_iv_rank_from_duckdb()` function
- **Before**: Computed IV Rank in-scan from snapshot columns
- **After**: Reads IV Rank from DuckDB iv_term_history table
- **Fallback**: Uses old calculation only if DuckDB fails

### 4. Dashboard Update ✅
- **File**: `streamlit_app/scan_view.py`
- **Change**: `get_snapshot_info()` queries DuckDB for IV history depth
- **Before**: Counted CSV files in archive directory
- **After**: Displays median history depth from DuckDB

### 5. Bootstrap Script ✅
- **File**: `scripts/admin/bootstrap_iv_history.py`
- **Purpose**: Initialize database with sample or historical data
- **Modes**: `sample`, `snapshots`, `fidelity`

### 6. Integration Tests ✅
- **File**: `test/test_iv_history_integration.py`
- **Coverage**: Daily collection, IV Rank, maturity states, scan integration, Fidelity triggering

---

## Quick Start (3 Steps)

### Step 1: Bootstrap Database
Initialize with sample data for testing:

```bash
cd /Users/haniabadi/Documents/Github/options

# Generate 130 days of sample IV data for 9 tickers
venv/bin/python scripts/admin/bootstrap_iv_history.py --mode sample --days 130
```

**Expected Output**:
```
🔵 IV HISTORY BOOTSTRAP
Generating 130 days of sample IV data...
✅ Generated 1170 sample records

📊 BOOTSTRAP COMPLETE
Total Tickers: 9
Date Range: 2025-09-26 to 2026-02-03
Avg History Depth: 130.0 days
Median Depth: 130 days
Tickers with 120+ days: 9
Tickers with 252+ days: 0
```

---

### Step 2: Verify Integration
Run a scan to verify the system works:

```bash
# Run scan with audit mode
venv/bin/python scripts/cli/scan_live.py --audit --tickers AAPL,MSFT,NVDA
```

**Look for these logs**:
```
📊 Loading IV Rank from DuckDB iv_term_history...
✅ IV Rank enrichment: 3 from DuckDB, 0 fallback
📊 IV Maturity: {'MATURE': 3, 'IMMATURE': 0, 'MISSING': 0}
```

**Verify Fidelity Triggering**:
```
[FIDELITY_SKIP] AAPL: SKIP_R2 - Schwab IV MATURE (130 days), sufficient for execution
[FIDELITY_SKIP] MSFT: SKIP_R2 - Schwab IV MATURE (130 days), sufficient for execution
[FIDELITY_SKIP] NVDA: SKIP_R2 - Schwab IV MATURE (130 days), sufficient for execution
```

✅ **Success Criteria**: All 3 tickers should be MATURE and Fidelity should be SKIPPED

---

### Step 3: Run Integration Tests
Verify all components work end-to-end:

```bash
# Run full test suite
pytest test/test_iv_history_integration.py -v -s
```

**Expected Output**:
```
test_daily_collection_workflow PASSED
test_iv_rank_calculation PASSED
test_iv_maturity_states PASSED
test_scan_engine_integration PASSED
test_fidelity_bootstrap_trigger PASSED
test_history_summary PASSED
```

---

## Production Deployment

### 1. Schedule Daily Collection

**Option A: Cron (Linux/Mac)**
```bash
# Run daily at 5:00 PM ET
crontab -e

# Add this line:
0 17 * * 1-5 /Users/haniabadi/Documents/Github/options/venv/bin/python /Users/haniabadi/Documents/Github/options/scripts/daily_jobs/collect_iv_history.py
```

**Option B: Systemd Timer (Linux)**
Create `/etc/systemd/system/iv-collection.timer`:
```ini
[Unit]
Description=Daily IV History Collection Timer

[Timer]
OnCalendar=Mon-Fri 17:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

Create `/etc/systemd/system/iv-collection.service`:
```ini
[Unit]
Description=Collect IV History from Schwab API

[Service]
Type=oneshot
User=yourusername
WorkingDirectory=/Users/haniabadi/Documents/Github/options
ExecStart=/Users/haniabadi/Documents/Github/options/venv/bin/python scripts/daily_jobs/collect_iv_history.py
```

Enable:
```bash
sudo systemctl enable iv-collection.timer
sudo systemctl start iv-collection.timer
```

---

### 2. Monitor Collection

**Check logs**:
```bash
# View last collection
tail -f /var/log/iv_collection.log

# Or check DuckDB directly
venv/bin/python -c "
from core.shared.data_layer.iv_term_history import *
import duckdb
con = duckdb.connect(str(get_iv_history_db_path()), read_only=True)
summary = get_history_summary(con)
print(f'Median Depth: {summary[\"median_depth\"]} days')
print(f'120+ days: {summary[\"tickers_120plus\"]} tickers')
con.close()
"
```

---

## Verification Checklist

### Database Populated ✅
```bash
# Check database exists and has data
ls -lh data/iv_history.duckdb

# Should show file size (e.g., 1.2 MB for 9 tickers × 130 days)
```

### Scan Engine Reading from DuckDB ✅
Look for this log in scan output:
```
✅ IV Rank enrichment: X from DuckDB, 0 fallback
```
- **X > 0**: DuckDB is working
- **fallback = 0**: No computation fallbacks

### Fidelity Correctly Skipped for MATURE ✅
Look for these logs:
```
[FIDELITY_SKIP] AAPL: SKIP_R2 - Schwab IV MATURE (130 days)
```
- **SKIP_R2**: Correct rule applied
- **130 days**: History depth from DuckDB

### Dashboard Displays Real Depth ✅
Open Streamlit dashboard:
```bash
streamlit run streamlit_app/dashboard.py
```

Go to **Scan Market** tab → Check **IV History** metric:
- Should show median depth from DuckDB (not hardcoded "0/120")

---

## Troubleshooting

### "No IV data fetched from Schwab"
**Cause**: Schwab API credentials expired or rate limited
**Fix**:
1. Check Schwab auth: `venv/bin/python auth_schwab_minimal.py`
2. Verify rate limits: Max 120 requests/minute

### "DuckDB file locked"
**Cause**: Multiple processes accessing database
**Fix**: Ensure only one process writes at a time
- Daily collection job should have exclusive write access
- Scans use read-only connections

### "IV Rank fallback count > 0"
**Cause**: Some tickers missing from DuckDB
**Fix**:
1. Check which tickers failed: Look for "DuckDB IV lookup failed" logs
2. Run collection job to populate missing tickers

### "IV Maturity: IMMATURE"
**Cause**: Not enough history yet (<120 days)
**Expected**: Normal for first 4 months of operation
**Fix**: Wait for daily collection to accumulate 120+ days

---

## Key Files Reference

| File | Purpose |
|------|---------|
| `core/shared/data_layer/iv_term_history.py` | Data layer API for IV history |
| `scripts/daily_jobs/collect_iv_history.py` | Daily collection job (Schwab → DuckDB) |
| `scripts/admin/bootstrap_iv_history.py` | Database initialization |
| `scan_engine/step2_load_snapshot.py` | Scan engine integration (`_enrich_iv_rank_from_duckdb`) |
| `streamlit_app/scan_view.py` | Dashboard IV history display |
| `test/test_iv_history_integration.py` | Integration tests |
| `docs/IV_HISTORY_SYSTEM.md` | Full architecture documentation |
| `data/iv_history.duckdb` | DuckDB database file (created on first run) |

---

## Migration Timeline

### Week 1: Bootstrap & Testing
- ✅ Run bootstrap script with sample data
- ✅ Verify scan integration
- ✅ Run integration tests
- Goal: All tickers MATURE (130 days sample data)

### Week 2-4: Accumulate Real Data
- Schedule daily collection job
- Monitor depth growth (0 → 30 days)
- Expect: IMMATURE state, Fidelity triggered for DIRECTIONAL

### Month 2-4: Transition Period
- Depth grows to 60 → 120 days
- Watch for IMMATURE → MATURE transitions
- Expect: Fidelity triggering decreases as tickers mature

### Month 5+: Full Maturity
- All tracked tickers have 120+ days
- Fidelity only triggered for INCOME/LEAP strategies
- DIRECTIONAL strategies use Schwab IV exclusively

---

## Success Metrics

| Metric | Target | How to Check |
|--------|--------|--------------|
| **Median IV Depth** | 120+ days | `get_history_summary()` |
| **DuckDB Read Success** | >95% | Scan logs: "X from DuckDB, Y fallback" |
| **Fidelity Skip Rate** | >70% for DIRECTIONAL | Fidelity trigger logs |
| **Maturity Rate** | >80% MATURE | Scan logs: "IV Maturity: {...}" |
| **Collection Success** | >95% tickers/day | Daily collection logs |

---

## Next Steps

1. ✅ **Bootstrap database**: Run `bootstrap_iv_history.py --mode sample`
2. ✅ **Verify scan integration**: Run `scan_live.py --audit`
3. ✅ **Run tests**: `pytest test/test_iv_history_integration.py`
4. 🔄 **Schedule daily collection**: Add to cron/systemd
5. 📊 **Monitor depth growth**: Check dashboard weekly

---

**Questions or Issues?**
- Check logs: Scan engine, daily collection, Fidelity triggers
- Review: `docs/IV_HISTORY_SYSTEM.md` for full architecture
- Run tests: `pytest test/test_iv_history_integration.py -v`
