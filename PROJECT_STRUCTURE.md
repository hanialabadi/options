# Project Structure - Complete Overview

## 📂 Current Organization (After Reorganization)

```
options/
│
├── 📁 core/                          # Core business logic
│   ├── phase1_clean.py               # Position cleaning (existing)
│   ├── phase2_parse.py               # Symbol parsing (existing)
│   ├── phase3_enrich/                # Enrichment logic (existing)
│   ├── phase6_freeze/                # Freeze logic (existing)
│   │
│   └── 📁 scan_engine/               # ✨ NEW: Modular scan pipeline
│       ├── __init__.py               # Package exports & version
│       ├── README.md                 # Usage guide & examples
│       ├── utils.py                  # Shared validation helpers
│       ├── step2_load_snapshot.py    # Load IV/HV CSV (~60 lines)
│       ├── step3_filter_ivhv.py      # IVHV gap filtering (~100 lines)
│       ├── step5_chart_signals.py    # Chart indicators (~180 lines)
│       ├── step6_gem_filter.py       # GEM candidate filtering (~120 lines)
│       └── pipeline.py               # Full orchestrator (~100 lines)
│
├── 📁 streamlit_app/                 # Dashboard UI
│   ├── dashboard.py                  # ✅ Updated: uses scan_engine imports
│   └── dashboard/                    # Dashboard modules
│       ├── chart_engine_runner.py
│       ├── pcs_engine_runner.py
│       └── ...
│
├── 📁 utils/                         # Utility helpers
├── 📁 agents/                        # Agent logic
├── 📁 cli/                           # CLI tools
├── 📁 output/                        # Scan outputs (CSVs)
│
├── 📄 .env.template                  # Environment variable template
├── 📄 requirements.txt               # Python dependencies
├── 📄 run_dashboard.sh               # Quick launcher
│
└── 📚 Documentation/
    ├── DASHBOARD_README.md           # How to run dashboard
    ├── SCAN_GUIDE.md                 # User guide (2000+ words)
    ├── IMPLEMENTATION_SUMMARY.md     # What we built
    └── REORGANIZATION_SUMMARY.md     # This reorganization
```

---

## 🔄 Data Flow Through Scan Engine

```
┌─────────────────────────────────────────────────────────────┐
│                    User Input                                │
│  • Upload CSV or provide file path                           │
│  • Set min_gap threshold                                     │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  Step 2: load_ivhv_snapshot.py                               │
│  • Load Fidelity IV/HV snapshot                              │
│  • Validate file format                                      │
│  Output: Raw DataFrame (~500-1000 rows)                      │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  Step 3: filter_ivhv_gap.py                                  │
│  • Convert IV/HV to numeric                                  │
│  • Apply liquidity filter (IV>=15, HV>0)                     │
│  • Calculate IVHV_gap_30D                                    │
│  • Normalize IV_Rank_XS (0-100)                              │
│  • Add persona tags (HardPass, SoftPass, PSC_Pass)           │
│  • Deduplicate by ticker                                     │
│  Output: Filtered tickers (~50-150 rows)                     │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  Step 5: compute_chart_signals.py                            │
│  • Fetch 90d history from yfinance                           │
│  • Calculate EMA9/21, SMA20/50, ATR                          │
│  • Detect EMA crossovers                                     │
│  • Calculate trend slope                                     │
│  • Classify regime (Trending, Ranging, Compressed, etc.)     │
│  Output: Chart-enriched tickers (~40-120 rows)               │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  Step 6: filter_gem_candidates.py                            │
│  • Apply directional/neutral validation gates                │
│  • Filter allowed signal types                               │
│  • Assign Scan_Tier (Tier 1/2/Trend_Hold)                    │
│  • Calculate PCS_Seed (68-75)                                │
│  Output: Final GEM candidates (~10-50 rows)                  │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                   Export & Display                           │
│  • Save CSV with timestamp                                   │
│  • Display in dashboard with metrics                         │
│  • Download button for CSV                                   │
│  • JSON summary with stats                                   │
└─────────────────────────────────────────────────────────────┘
```

---

## 🎯 Import Patterns

### Pattern 1: Full Pipeline (One-Click)
```python
from core.scan_engine import run_full_scan_pipeline

# Run everything
results = run_full_scan_pipeline(
    snapshot_path='/path/to/snapshot.csv',
    output_dir='./output'
)

# Access results
snapshot = results['snapshot']        # Step 2 output
filtered = results['filtered']        # Step 3 output
charted = results['charted']          # Step 5 output
gem_candidates = results['gem_candidates']  # Step 6 output
```

### Pattern 2: Step-by-Step (Dashboard Style)
```python
from core.scan_engine import (
    load_ivhv_snapshot,
    filter_ivhv_gap,
    compute_chart_signals,
    filter_gem_candidates
)

# Run independently
df_step2 = load_ivhv_snapshot()
df_step3 = filter_ivhv_gap(df_step2, min_gap=2.0)
df_step5 = compute_chart_signals(df_step3)
df_step6 = filter_gem_candidates(df_step5)
```

### Pattern 3: Custom Workflow
```python
from core.scan_engine import (
    load_ivhv_snapshot,
    filter_ivhv_gap,
    classify_regime
)

# Load and filter only
df = load_ivhv_snapshot()
df_filtered = filter_ivhv_gap(df, min_gap=3.5)

# Custom regime analysis
for _, row in df_filtered.iterrows():
    regime = classify_regime({
        'Trend_Slope': row['Trend_Slope'],
        'Atr_Pct': row['Atr_Pct'],
        'Price_vs_SMA20': row['Price_vs_SMA20'],
        'SMA20': row['SMA20']
    })
    print(f"{row['Ticker']}: {regime}")
```

---

## 🧩 Module Responsibilities

| Module | Single Responsibility | Imports From | Exported Functions |
|--------|----------------------|--------------|-------------------|
| `utils.py` | Validation helpers | pandas, logging | `validate_input()` |
| `step2_load_snapshot.py` | CSV loading | pandas, os, Path | `load_ivhv_snapshot()` |
| `step3_filter_ivhv.py` | IVHV filtering | pandas, numpy | `filter_ivhv_gap()` |
| `step5_chart_signals.py` | Technical analysis | pandas, yfinance, utils | `compute_chart_signals()`, `classify_regime()` |
| `step6_gem_filter.py` | GEM filtering | pandas, utils | `filter_gem_candidates()` |
| `pipeline.py` | Orchestration | all above steps | `run_full_scan_pipeline()` |
| `__init__.py` | Public API | all modules | All public functions |

---

## 📈 Lines of Code by Responsibility

```
utils.py                 ▓░░░░░░░░░░░  30 lines  (5%)
step2_load_snapshot.py   ▓▓▓▓░░░░░░░░  60 lines  (10%)
step3_filter_ivhv.py     ▓▓▓▓▓▓░░░░░░ 100 lines (16%)
step5_chart_signals.py   ▓▓▓▓▓▓▓▓▓▓░░ 180 lines (30%)
step6_gem_filter.py      ▓▓▓▓▓▓▓░░░░░ 120 lines (20%)
pipeline.py              ▓▓▓▓▓▓░░░░░░ 100 lines (16%)
__init__.py              ▓░░░░░░░░░░░  20 lines  (3%)
────────────────────────────────────────────────
Total                                 610 lines
```

---

## 🎓 Learning Path for New Developers

### Step 1: Understand Individual Steps
1. Read `core/scan_engine/README.md`
2. Review docstrings in each step file
3. Run individual steps in Python console

### Step 2: Trace Data Flow
1. Start with `step2_load_snapshot.py`
2. Follow data transformations through each step
3. Inspect intermediate DataFrames

### Step 3: Test Modifications
1. Pick a step to modify (e.g., `step3_filter_ivhv.py`)
2. Make changes in isolation
3. Test step independently
4. Run full pipeline to verify integration

### Step 4: Extend Pipeline
1. Create `stepX_new_feature.py`
2. Follow docstring template from existing steps
3. Export in `__init__.py`
4. Update `pipeline.py` if needed

---

## 🔧 Maintenance Checklist

### When Fixing Bugs:
- [ ] Identify which step has the issue
- [ ] Open specific step file (not entire pipeline)
- [ ] Fix logic in isolation
- [ ] Test step independently
- [ ] Run full pipeline to verify
- [ ] Update docstring if logic changed

### When Adding Features:
- [ ] Decide if it's a new step or modification
- [ ] Create new file or modify existing
- [ ] Write comprehensive docstring
- [ ] Add to `__init__.py` exports
- [ ] Update `pipeline.py` if orchestration needed
- [ ] Add to dashboard if user-facing

### When Refactoring:
- [ ] Focus on one step at a time
- [ ] Maintain backward compatibility
- [ ] Update docstrings
- [ ] Test before/after behavior matches
- [ ] Update README if usage changes

---

## ✅ Success Metrics

**Code Quality:**
- ✅ Average file size: ~100-150 lines (easy to read)
- ✅ Each file has single responsibility
- ✅ Comprehensive docstrings (200+ lines of docs)
- ✅ Clear import structure

**Maintainability:**
- ✅ Easy to locate specific logic
- ✅ Changes scoped to single file
- ✅ Independent testing possible
- ✅ Multiple devs can work in parallel

**Usability:**
- ✅ Import only what you need
- ✅ Run full pipeline or individual steps
- ✅ Clear error messages with context
- ✅ Dashboard integration seamless

---

## 🎉 Final Status

**Reorganization: COMPLETE** ✅

All code is now:
- 📁 **Organized** in logical modules
- 📝 **Documented** with comprehensive docstrings
- 🧪 **Testable** independently per step
- 🔄 **Maintainable** with clear separation
- 🚀 **Scalable** for future additions

**Next Action:** Start using the new structure!

```bash
# Run the dashboard
streamlit run streamlit_app/dashboard.py

# Or test in Python
python -c "from core.scan_engine import *; print('Ready!')"
```
