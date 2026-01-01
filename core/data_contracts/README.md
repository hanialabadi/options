# Data Contracts - Single Source of Truth

**Critical Rule:** NO code outside this package should directly read/write CSVs for active trades or snapshots.

---

## Purpose

This package provides:
1. **Centralized path management** - No hardcoded paths anywhere else
2. **Single I/O interface** - All CSV operations go through here
3. **Schema validation** - Enforce data contracts
4. **Environment-agnostic** - Works on any machine via config

---

## Usage

### Loading Active Master

```python
from core.data_contracts import load_active_master, save_active_master

# Load current active trades
df = load_active_master()

# Modify trades
df["PCS_Drift"] = df["PCS"] - df["PCS_Entry"]

# Save back
save_active_master(df)
```

### Working with Snapshots

```python
from core.data_contracts import save_snapshot, load_snapshot_timeseries

# Save current state
save_snapshot(df)

# Load historical timeseries for drift analysis
active_ids = get_active_trade_ids()
df_history = load_snapshot_timeseries(active_trade_ids=active_ids)
```

### Configuration

```python
from core.data_contracts.config import ACTIVE_MASTER_PATH, SNAPSHOT_DIR

# Paths are centralized, but can be overridden via environment variables
print(ACTIVE_MASTER_PATH)  # /Users/.../active_master.csv
print(SNAPSHOT_DIR)         # /Users/.../drift/
```

---

## Schema Documentation

### active_master.csv

#### Frozen at Entry (Never Recomputed)
- `TradeID`: Unique identifier
- `Symbol`: Underlying ticker
- `Strategy`: Strategy type (e.g., "Long Call", "Iron Condor")
- `TradeDate`: Entry date
- `Contract_Symbols`: OSI symbols
- `Strikes`: Strike prices
- `Expiration`: Expiration date
- `PCS_Entry`: PCS score at entry
- `Vega_Entry`, `Delta_Entry`, `Gamma_Entry`, `Theta_Entry`: Greeks at entry
- `IVHV_Gap_Entry`: IV/HV gap at entry
- `Chart_Trend_Entry`: Chart pattern at entry

#### Updated Live (Recomputed Daily)
- `PCS`: Current PCS score
- `Vega`, `Delta`, `Gamma`, `Theta`: Current Greeks
- `IVHV_Gap`: Current IV/HV gap
- `Days_Held`: Days since entry
- `Held_ROI%`: Current return on investment

#### Derived (Computed from Above)
- `PCS_Drift`: PCS - PCS_Entry
- `Vega_ROC`: (Vega - Vega_Entry) / Days_Held
- `Flag_PCS_Drift`: Boolean flag if drift > threshold
- `Flag_Vega_Flat`: Boolean flag if Vega declining

#### Management State
- `Rec_Action`: Current recommendation (HOLD/EXIT/TRIM/REVALIDATE)
- `Rec_V6`: Full recommendation text from V6 engine
- `OutcomeTag`: Trade outcome classification
- `Leg_Status`: Position status (Active/Closed/Partial)

### Snapshots (positions_*.csv)

**Required columns for drift analysis:**
- `TradeID`: Links to active_master
- `Snapshot_TS`: Timestamp (added by loader)
- `PCS`, `Delta`, `Gamma`, `Vega`, `Theta`: Current values
- `IVHV_Gap`: Current IV/HV gap

---

## Migration Notes

**Before:**
```python
# ❌ Old way - hardcoded path, scattered everywhere
df = pd.read_csv("/Users/haniabadi/Documents/Windows/Optionrec/active_master.csv")
```

**After:**
```python
# ✅ New way - centralized, clean
from core.data_contracts import load_active_master
df = load_active_master()
```

---

## Environment Variables

Override default paths for different environments:

```bash
export ACTIVE_MASTER_PATH="/custom/path/active_master.csv"
export SNAPSHOT_DIR="/custom/path/snapshots/"
export OPTIONS_DATA_DIR="/custom/data/"
```

---

## File Responsibilities

- `config.py`: All path definitions
- `master_data.py`: Active trades I/O
- `snapshot_data.py`: Timestamped snapshots I/O
- `__init__.py`: Clean public API

---

## Next Steps (Phase B)

Once all code uses these contracts:
1. Management engine can safely read/write through this interface
2. CLI and Dashboard consume same data source
3. No more path mismatches or duplicate reads
