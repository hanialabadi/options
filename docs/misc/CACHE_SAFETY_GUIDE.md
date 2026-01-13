# Cache Safety Guide for Financial Data

## ⚠️ Problem: Stale Cache = Wrong Decisions

When working with financial data and trading decisions, **stale cached data is dangerous**:
- Old logic can persist in Python bytecode (`.pyc` files)
- Streamlit session state can hold pre-fix dataframes
- Wrong calculations → Wrong trades → Real money loss

## ✅ Safeguards Implemented

### 1. **Version Tracking** (Automatic Detection)
Each calculation step now includes version metadata:

```python
# In step3_filter_ivhv.py
STEP3_VERSION = "20251227_02"  # Increment when logic changes
STEP3_LOGIC_HASH = "a3f2d1b9"  # Hash of calculation formula

# Stored with data
df.attrs['step3_version'] = STEP3_VERSION
df.attrs['step3_logic_hash'] = STEP3_LOGIC_HASH
df.attrs['step3_computed_at'] = datetime.now().isoformat()
```

### 2. **UI Validation** (Automatic Blocking)
Dashboard checks cached data version:

```python
if cached_version != STEP3_VERSION:
    st.error("⚠️ STALE DATA DETECTED")
    st.stop()  # Block UI until cache cleared
```

**Result**: You can't accidentally use old calculations. UI forces cache refresh.

### 3. **Clear Cache Button** (Manual Override)
Every step has a "🔄 Clear Cache" button for instant invalidation.

---

## 🛠️ Development Workflow

### When You Change Step Logic:

1. **Update version constant**:
   ```python
   STEP3_VERSION = "20251227_03"  # Increment
   ```

2. **Clear Python bytecode**:
   ```bash
   find core/scan_engine -name "*.pyc" -delete
   find core/scan_engine -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
   ```

3. **Restart Streamlit**:
   ```bash
   # Ctrl+C to stop
   streamlit run streamlit_app/dashboard.py
   ```

4. **Click "Clear Cache"** in UI

5. **Rerun the step**

### Quick Command (Add to aliases):
```bash
alias clear_cache='find core -name "*.pyc" -delete && find core -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true'
```

---

## 🚀 Production Deployment

### Pre-Deployment Checklist:

- [ ] Clear ALL bytecode cache
- [ ] Restart application fresh
- [ ] Run full pipeline end-to-end
- [ ] Verify version numbers match across all steps
- [ ] Check logs for version metadata
- [ ] Test cache validation triggers correctly

### Docker/Container Deployment:
```dockerfile
# Ensure clean build
RUN find /app -name "*.pyc" -delete
RUN find /app -name "__pycache__" -delete

# Set Python to not write bytecode (fresh compile every time)
ENV PYTHONDONTWRITEBYTECODE=1
```

### Environment Variable:
```bash
# Force fresh imports in production
export PYTHONDONTWRITEBYTECODE=1
```

---

## 📊 Version History Log

| Date | Version | Change | Hash |
|------|---------|--------|------|
| 2025-12-27 | 20251227_02 | Absolute magnitude for Edge flags | a3f2d1b9 |
| 2025-12-27 | 20251227_01 | Initial version tracking | - |

---

## 🧪 Testing Cache Validation

### Test Scenario 1: Detect Stale Data
```python
# Manually corrupt version to test validation
df = st.session_state['step3_filtered']
df.attrs['step3_version'] = "20251226_99"  # Old version
# Expected: UI shows red error and blocks
```

### Test Scenario 2: Version Mismatch
```python
# Change STEP3_VERSION in code but don't rerun
# Expected: Next run shows stale data warning
```

---

## 🔐 Best Practices Summary

### DO:
✅ Increment version on ANY logic change (even small)  
✅ Clear bytecode after editing step files  
✅ Restart Streamlit after code changes  
✅ Use "Clear Cache" button liberally  
✅ Check version metadata in logs  
✅ Test with intentionally stale data  

### DON'T:
❌ Assume Streamlit auto-reloads Python modules correctly  
❌ Trust cached data after code edits  
❌ Skip version increment for "minor" changes  
❌ Deploy without clearing bytecode  
❌ Use cached data across code versions  

---

## 🚨 Emergency: "I Think I'm Using Stale Data"

1. **Stop immediately** - don't make decisions on suspect data
2. **Clear everything**:
   ```bash
   # Clear bytecode
   find core -name "*.pyc" -delete
   find core -name "__pycache__" -type d -exec rm -rf {} +
   
   # Clear Streamlit cache
   rm -rf ~/.streamlit/cache
   ```
3. **Full restart**:
   - Kill Streamlit process (Ctrl+C)
   - Restart: `streamlit run streamlit_app/dashboard.py`
4. **Click "Clear Cache"** on EVERY step
5. **Rerun entire pipeline** from Step 2 onwards
6. **Verify versions** match in logs and UI

---

## 📝 Logging Recommendations

Add to each step's logger:
```python
logger.info(f"✅ Step 3 complete (v{STEP3_VERSION}, hash={STEP3_LOGIC_HASH})")
logger.info(f"   Computed at: {datetime.now().isoformat()}")
logger.info(f"   Edge count: {edge_count}")
```

Check logs before trading decisions:
```bash
tail -f logs/*.log | grep "version\|hash\|computed"
```

---

## 🎯 Philosophy

> **"In finance, stale data is wrong data. Wrong data is expensive."**

This system treats cache invalidation as a **safety feature**, not a performance optimization.

When in doubt: **Clear, restart, rerun.**
