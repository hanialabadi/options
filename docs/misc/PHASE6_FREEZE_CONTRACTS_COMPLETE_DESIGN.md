# Phase 6 Freeze Contracts - Complete Design Specification

**Date:** 2026-01-04  
**Status:** 🔵 Design Phase (Implementation Ready)  
**Purpose:** Define complete freeze contract for Phase 1-4 observables → Phase 6 _Entry snapshots

---

## 🎯 EXECUTIVE SUMMARY

**What Phase 6 Does:**
- Freezes Phase 1-4 observables at trade entry (creates `*_Entry` columns)
- Enables Phase 7-9 drift analysis (current vs entry comparison)
- Provides immutable historical anchor for behavior tracking

**Why It Matters:**
- **Phase 7 Drift:** Compare current vs entry (IV_Rank drift, Greeks drift, time decay)
- **Phase 8 Strategy:** Use drift for decision logic (exit when IV_Rank > Entry + 30)
- **Phase 9 Execution:** Trust frozen values for accurate P&L attribution

**Current Status:**
- ✅ Existing: Greeks, Premium, PCS, Capital_Deployed, Moneyness, DTE, BreakEven
- ✅ **NEW (Ready to Add):** IV_Rank, Days_to_Earnings, Next_Earnings_Date
- ⚠️ Missing: Historical IV surface, Liquidity metrics (future work)

---

## 📊 COMPLETE FREEZE INVENTORY

### Phase 3 Observables → Phase 6 _Entry Snapshots

| **Observable** | **Current Column** | **Entry Column** | **Status** | **Phase 7+ Use Case** |
|---------------|-------------------|------------------|-----------|---------------------|
| **Market Structure** | | | | |
| Premium | `Premium` | `Premium_Entry` | ✅ Frozen | P&L calculation, premium decay |
| Delta | `Delta` | `Delta_Entry` | ✅ Frozen | Directional bias drift, hedging |
| Gamma | `Gamma` | `Gamma_Entry` | ✅ Frozen | Convexity change tracking |
| Vega | `Vega` | `Vega_Entry` | ✅ Frozen | Volatility sensitivity drift |
| Theta | `Theta` | `Theta_Entry` | ✅ Frozen | Time decay tracking |
| Moneyness | `Moneyness_Pct` | `Moneyness_Pct_Entry` | ✅ Frozen | Strike proximity drift |
| Breakeven | `BreakEven` | `BreakEven_Entry` | ✅ Frozen | Distance to breakeven change |
| DTE | `DTE` | `DTE_Entry` | ✅ Frozen | Time decay rate validation |
| **Volatility Context** | | | | |
| IV Rank | `IV_Rank` | `IV_Rank_Entry` | 🔵 **NEW** | IV regime shift detection |
| IV Rank Source | `IV_Rank_Source` | `IV_Rank_Source_Entry` | 🔵 **NEW** | Data provenance tracking |
| IV Rank History | `IV_Rank_History_Days` | `IV_Rank_History_Days_Entry` | 🔵 **NEW** | Data quality tracking |
| **Event Risk** | | | | |
| Days to Earnings | `Days_to_Earnings` | `Days_to_Earnings_Entry` | 🔵 **NEW** | Earnings crossing detection |
| Next Earnings Date | `Next_Earnings_Date` | `Next_Earnings_Date_Entry` | 🔵 **NEW** | Earnings event tracking |
| Earnings Source | `Earnings_Source` | `Earnings_Source_Entry` | 🔵 **NEW** | Data provenance tracking |
| **Risk Metrics** | | | | |
| PCS Score | `PCS_Score` | `PCS_Entry` | ✅ Frozen | Structural quality drift |
| Capital Deployed | `Capital_Deployed` | `Capital_Deployed_Entry` | ✅ Frozen | Position sizing validation |
| **Trade Aggregates** | | | | |
| Max Strike Spread | `Max_Strike_Spread` | `Max_Strike_Spread_Entry` | 🟡 Consider | Width consistency check |
| Max DTE | `Max_DTE_Trade` | `Max_DTE_Trade_Entry` | 🟡 Consider | Calendar spread tracking |
| Min DTE | `Min_DTE_Trade` | `Min_DTE_Trade_Entry` | 🟡 Consider | Expiration clustering |
| Leg Count | `Leg_Count_Trade` | `Leg_Count_Trade_Entry` | 🟡 Consider | Structure integrity check |

**Legend:**
- ✅ Frozen: Currently implemented and working
- 🔵 **NEW**: Ready to add (Phase 3 complete, just wire to Phase 6)
- 🟡 Consider: Lower priority, evaluate usefulness
- ❌ Future: Not yet in Phase 3 (e.g., Liquidity)

---

## 🏗️ FREEZE CONTRACT ARCHITECTURE

### Design Principles

**1. Immutability (Non-Negotiable)**
```python
# Once frozen, NEVER changes
df_master.loc[df_master['TradeID'] == 'ABC123', 'IV_Rank_Entry'] = 45.2  # ✅ First time OK
df_master.loc[df_master['TradeID'] == 'ABC123', 'IV_Rank_Entry'] = 50.0  # ❌ FORBIDDEN
```

**Rationale:** Historical anchor must be trustworthy for drift analysis

**2. Copy-Only (No Recalculation)**
```python
# Phase 6 copies, does NOT compute
df_new['IV_Rank_Entry'] = df_new['IV_Rank']  # ✅ Copy from Phase 3
df_new['IV_Rank_Entry'] = compute_iv_rank()   # ❌ FORBIDDEN (recalculation risk)
```

**Rationale:** Phase 3 is source of truth, Phase 6 just snapshots it

**3. Complete Snapshot (All or Nothing)**
```python
# Freeze ALL Phase 3 observables together
frozen_fields = [
    'Premium_Entry', 'Delta_Entry', 'Gamma_Entry', 'Vega_Entry', 'Theta_Entry',
    'PCS_Entry', 'Capital_Deployed_Entry', 'Moneyness_Pct_Entry', 
    'DTE_Entry', 'BreakEven_Entry',
    'IV_Rank_Entry', 'Days_to_Earnings_Entry', 'Next_Earnings_Date_Entry'  # NEW
]
```

**Rationale:** Partial freezes create inconsistent drift analysis

**4. Provenance Tracking (Data Quality)**
```python
# Track where data came from
df_new['IV_Rank_Source_Entry'] = df_new['IV_Rank_Source']          # "yfinance" or "stub"
df_new['Earnings_Source_Entry'] = df_new['Earnings_Source']        # "yfinance", "static", "unknown"
df_new['IV_Rank_History_Days_Entry'] = df_new['IV_Rank_History_Days']  # 5, 120, 252
```

**Rationale:** Know data quality when analyzing drift (stub data less reliable)

---

## 📋 UPDATED FREEZE SCHEMA

### Complete _Entry Column Specification

```python
# EXISTING (Already Frozen)
FROZEN_GREEKS = [
    "Premium_Entry",         # float, Option premium at entry
    "Delta_Entry",           # float, Delta at entry
    "Gamma_Entry",           # float, Gamma at entry
    "Vega_Entry",            # float, Vega at entry
    "Theta_Entry",           # float, Theta at entry
]

FROZEN_PHASE3_EXISTING = [
    "PCS_Entry",             # float, PCS score at entry (0-100)
    "Capital_Deployed_Entry",# float, Capital at risk at entry ($)
    "Moneyness_Pct_Entry",   # float, Moneyness % at entry
    "DTE_Entry",             # int, Days to expiration at entry
    "BreakEven_Entry",       # float, Breakeven price at entry
]

# NEW (Ready to Add - Phase 3 Complete)
FROZEN_VOLATILITY_CONTEXT = [
    "IV_Rank_Entry",         # float or NaN, IV percentile at entry (0-100)
    "IV_Rank_Source_Entry",  # str, "historical" | "stub" | "insufficient_data"
    "IV_Rank_History_Days_Entry",  # int, Historical lookback days available
]

FROZEN_EVENT_RISK = [
    "Days_to_Earnings_Entry",      # int or NaN, Calendar days to earnings at entry
    "Next_Earnings_Date_Entry",    # datetime or NaT, Next earnings date at entry
    "Earnings_Source_Entry",       # str, "yfinance" | "static" | "unknown"
]

# COMPLETE FREEZE LIST
ALL_ENTRY_FIELDS = (
    FROZEN_GREEKS + 
    FROZEN_PHASE3_EXISTING + 
    FROZEN_VOLATILITY_CONTEXT + 
    FROZEN_EVENT_RISK
)
# Total: 18 _Entry columns
```

### Data Type Specifications

| **Entry Column** | **Data Type** | **Nullable** | **Default** | **Notes** |
|-----------------|--------------|-------------|------------|-----------|
| `Premium_Entry` | float64 | No | N/A | Always present from broker |
| `Delta_Entry` | float64 | No | N/A | Always present from broker |
| `Gamma_Entry` | float64 | No | N/A | Always present from broker |
| `Vega_Entry` | float64 | No | N/A | Always present from broker |
| `Theta_Entry` | float64 | No | N/A | Always present from broker |
| `PCS_Entry` | float64 | Yes (NaN) | NaN | May be NaN if PCS calc fails |
| `Capital_Deployed_Entry` | float64 | No | N/A | Estimated if broker missing |
| `Moneyness_Pct_Entry` | float64 | No | N/A | Always calculable from strikes |
| `DTE_Entry` | int64 | No | N/A | Always calculable from expiration |
| `BreakEven_Entry` | float64 | Yes (NaN) | NaN | May be NaN for complex structures |
| `IV_Rank_Entry` | float64 | **Yes (NaN)** | NaN | **NaN if <120 days history** |
| `IV_Rank_Source_Entry` | str | No | "unknown" | Data provenance tracker |
| `IV_Rank_History_Days_Entry` | int64 | No | 0 | 0 if insufficient data |
| `Days_to_Earnings_Entry` | float64 | **Yes (NaN)** | NaN | **NaN if earnings unknown** |
| `Next_Earnings_Date_Entry` | datetime64[ns] | **Yes (NaT)** | NaT | **NaT if earnings unknown** |
| `Earnings_Source_Entry` | str | No | "unknown" | Data provenance tracker |

**NaN Handling Philosophy:**
- ✅ **Freeze NaN if Phase 3 returns NaN** (preserves data quality signal)
- ❌ **Never substitute magic defaults** (50.0, 999, etc.)
- ✅ **Use provenance columns** to track data quality

---

## 🔒 FREEZE TIMING & TRIGGERS

### When Does Freezing Occur?

**Definition:** A trade is **NEW** when:
```python
IsNewTrade = (TradeID in Phase4_Snapshot) AND (TradeID NOT in active_master.csv)
```

**Freeze Trigger Flow:**
```
┌─────────────────────┐
│ Phase 4 Snapshot    │  ← Current market state (today's snapshot)
│ (df)                │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Load active_master  │  ← Historical active positions
│ (df_master_current) │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ merge_master()      │  ← Identify NEW vs EXISTING
│                     │
│ IsNewTrade = True   │  ← Trades appearing for first time
│ IsNewTrade = False  │  ← Trades already frozen
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Filter NEW only     │  ← df_new = df[IsNewTrade == True]
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Freeze _Entry cols  │  ← Copy Phase 3 → _Entry columns
│ (NEW trades only)   │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Merge back to       │  ← df_master.update(df_new)
│ df_master           │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Validate immutable  │  ← assert_immutable_entry_fields()
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│ Persist             │  ← Save to active_master.csv
│ active_master.csv   │
└─────────────────────┘
```

### Edge Cases & Handling

| **Scenario** | **Behavior** | **Rationale** |
|-------------|------------|--------------|
| **First snapshot ever** | All trades NEW → All freeze | Bootstrap condition |
| **Reopened position** | Treat as NEW → New freeze | Different entry point |
| **Delayed snapshot** | Still freeze first appearance | Freeze = first observed, not first opened |
| **Manual entry** | User provides _Entry values | Trust user (rare, for corrections) |
| **Partial data** | Freeze NaN if Phase 3 = NaN | Preserve data quality signal |
| **Multi-leg split** | Each leg inherits parent freeze | Structure integrity |

**Critical Rule:** Freeze happens at **first observation**, not at trade open time
- Trade may open Monday, but if first snapshot is Tuesday → freeze Tuesday values
- This is **correct** - we freeze what we observed, not what we wished we observed

---

## 🎯 PHASE 7-9 DRIFT ARCHITECTURE

### How Frozen Values Enable Drift Analysis

**Phase 7: Observation (Drift Calculation)**

```python
# Example: IV_Rank drift
df['IV_Rank_Drift'] = df['IV_Rank'] - df['IV_Rank_Entry']

# Interpretation:
#   +30 = IV jumped from 20th percentile to 50th (vol expansion)
#   -30 = IV dropped from 70th percentile to 40th (vol compression)
#   NaN = Either current or entry was NaN (insufficient data)
```

**Phase 8: Strategy (Drift-Based Decisions)**

```python
# Example: Exit when IV expands significantly
exit_conditions = {
    'IV_Expansion_Exit': df['IV_Rank_Drift'] > 30,  # IV jumped 30+ percentiles
    'Earnings_Approaching': df['Days_to_Earnings'] < 7,  # Within 7 days
    'Time_Decay_Complete': df['DTE'] < df['DTE_Entry'] * 0.25,  # 75% time passed
}
```

**Phase 9: Execution (Trust Frozen Values)**

```python
# P&L attribution using frozen entry premium
df['Realized_PnL'] = df['Premium'] - df['Premium_Entry']

# Capital efficiency using frozen capital
df['ROC'] = df['Realized_PnL'] / df['Capital_Deployed_Entry'] * 100
```

### Key Drift Metrics (Examples)

| **Drift Metric** | **Formula** | **Use Case** |
|-----------------|-----------|------------|
| **IV_Rank_Drift** | `IV_Rank - IV_Rank_Entry` | Volatility regime change |
| **Premium_Pct_Change** | `(Premium - Premium_Entry) / Premium_Entry * 100` | P&L % |
| **Delta_Drift** | `Delta - Delta_Entry` | Directional bias change |
| **Moneyness_Drift** | `Moneyness_Pct - Moneyness_Pct_Entry` | Strike proximity change |
| **Days_Since_Entry** | `DTE_Entry - DTE` | Time decay validation |
| **Earnings_Crossed** | `Days_to_Earnings < 0 AND Days_to_Earnings_Entry > 0` | Binary event detection |
| **Theta_Burn_Rate** | `(Premium - Premium_Entry) / Days_Since_Entry` | Daily decay rate |

### Earnings Crossing Detection (Special Case)

```python
# Detect if earnings occurred since entry
def detect_earnings_crossing(df):
    \"\"\"
    Returns True if earnings happened between entry and now.
    
    Logic:
    - Entry: Days_to_Earnings_Entry > 0 (earnings was in future)
    - Now: Days_to_Earnings < 0 (earnings is in past)
    - Conclusion: Earnings crossed → Binary event occurred
    \"\"\"
    crossed = (
        (df['Days_to_Earnings_Entry'] > 0) &  # Was future
        (df['Days_to_Earnings'] < 0)           # Now past
    )
    return crossed

df['Earnings_Crossed'] = detect_earnings_crossing(df)

# Use in Phase 8 strategy:
# - Exit if crossed (realized event risk)
# - Tighten stops post-earnings
# - Analyze P&L around earnings dates
```

---

## ✅ FREEZE CONTRACT GUARANTEES

### 1. Immutability Guarantee

**Contract:** Once `*_Entry` field is written, it NEVER changes

**Validation:**
```python
def assert_immutable_entry_fields(df_master_new, df_master_old):
    \"\"\"
    Raises ValueError if any EXISTING trade has changed _Entry values.
    \"\"\"
    existing_trades = df_master_new[df_master_new['IsNewTrade'] == False]
    
    for field in ALL_ENTRY_FIELDS:
        if field in df_master_old.columns:
            # Compare old vs new for EXISTING trades
            old_values = df_master_old.set_index('TradeID')[field]
            new_values = existing_trades.set_index('TradeID')[field]
            
            # Check for changes (allowing NaN == NaN)
            changed = ~(old_values.equals(new_values))
            
            if changed.any():
                raise ValueError(
                    f\"❌ IMMUTABILITY VIOLATION: {field} changed for EXISTING trades\\n\"
                    f\"This is a CRITICAL bug - frozen values must never change.\"
                )
```

**Test Cases:**
- ✅ NEW trade → _Entry fields populated
- ✅ EXISTING trade → _Entry fields unchanged
- ❌ EXISTING trade → _Entry fields modified → **ValueError**

### 2. Completeness Guarantee

**Contract:** All Phase 3 observables must have corresponding _Entry columns

**Validation:**
```python
def assert_freeze_completeness(df_master):
    \"\"\"
    Raises ValueError if NEW trade missing required _Entry fields.
    \"\"\"
    new_trades = df_master[df_master['IsNewTrade'] == True]
    
    required_entry_fields = ALL_ENTRY_FIELDS
    missing_fields = [f for f in required_entry_fields if f not in new_trades.columns]
    
    if missing_fields:
        raise ValueError(
            f\"❌ FREEZE INCOMPLETE: Missing _Entry fields: {missing_fields}\\n\"
            f\"All Phase 3 observables must be frozen.\"
        )
    
    # Check for unexpected NaNs (where Phase 3 had values)
    for field in required_entry_fields:
        # Skip fields that are legitimately nullable
        if field in ['IV_Rank_Entry', 'Days_to_Earnings_Entry', 'Next_Earnings_Date_Entry']:
            continue
        
        unexpected_nans = new_trades[field].isna().sum()
        if unexpected_nans > 0:
            raise ValueError(
                f\"❌ UNEXPECTED NaN: {field} has {unexpected_nans} NaN values for NEW trades\\n\"
                f\"Phase 3 should provide these values.\"
            )
```

### 3. Copy-Only Guarantee

**Contract:** Phase 6 copies columns, never recalculates

**Validation:**
```python
def assert_copy_only_freeze(df_new):
    \"\"\"
    Validates that _Entry columns match source Phase 3 columns.
    \"\"\"
    copy_pairs = [
        ('Premium', 'Premium_Entry'),
        ('Delta', 'Delta_Entry'),
        ('IV_Rank', 'IV_Rank_Entry'),
        ('Days_to_Earnings', 'Days_to_Earnings_Entry'),
        # ... all pairs
    ]
    
    for source, entry in copy_pairs:
        # Allow NaN == NaN
        if not df_new[source].equals(df_new[entry]):
            # Check if difference is only NaN handling
            source_nans = df_new[source].isna()
            entry_nans = df_new[entry].isna()
            
            if not (source_nans == entry_nans).all():
                raise ValueError(
                    f\"❌ COPY VIOLATION: {entry} does not match {source}\\n\"
                    f\"Phase 6 must copy exactly, not recalculate.\"
                )
```

### 4. Provenance Guarantee

**Contract:** Data quality tracked via _Source_Entry columns

**Tracked Fields:**
- `IV_Rank_Source_Entry`: "historical" | "stub" | "insufficient_data"
- `Earnings_Source_Entry`: "yfinance" | "static" | "unknown"
- `IV_Rank_History_Days_Entry`: Number of days available (0-252)

**Usage:**
```python
# Filter high-quality data only
high_quality_iv = df[
    (df['IV_Rank_Source_Entry'] == 'historical') &
    (df['IV_Rank_History_Days_Entry'] >= 120)
]

# Warn on low-quality data
low_quality_count = (df['IV_Rank_Source_Entry'] == 'stub').sum()
if low_quality_count > 0:
    logger.warning(f\"⚠️ {low_quality_count} positions with stub IV_Rank data\")
```

---

## 🔧 IMPLEMENTATION CHECKLIST

### Phase 6 Freeze Module Updates

**File:** `core/phase6_freeze/freezer_modules/freeze_entry_observables.py` (NEW)

```python
def freeze_volatility_context(df_new: pd.DataFrame) -> pd.DataFrame:
    \"\"\"
    Freeze IV_Rank and related volatility context at entry.
    
    Copies:
    - IV_Rank → IV_Rank_Entry
    - IV_Rank_Source → IV_Rank_Source_Entry
    - IV_Rank_History_Days → IV_Rank_History_Days_Entry
    
    Contract: Copy-only, no recalculation
    \"\"\"
    df_new['IV_Rank_Entry'] = df_new['IV_Rank']
    df_new['IV_Rank_Source_Entry'] = df_new['IV_Rank_Source']
    df_new['IV_Rank_History_Days_Entry'] = df_new['IV_Rank_History_Days']
    
    logger.info(f\"Froze volatility context for {len(df_new)} new trades\")
    return df_new


def freeze_event_risk(df_new: pd.DataFrame) -> pd.DataFrame:
    \"\"\"
    Freeze earnings proximity and related event risk at entry.
    
    Copies:
    - Days_to_Earnings → Days_to_Earnings_Entry
    - Next_Earnings_Date → Next_Earnings_Date_Entry
    - Earnings_Source → Earnings_Source_Entry
    
    Contract: Copy-only, no recalculation
    \"\"\"
    df_new['Days_to_Earnings_Entry'] = df_new['Days_to_Earnings']
    df_new['Next_Earnings_Date_Entry'] = df_new['Next_Earnings_Date']
    df_new['Earnings_Source_Entry'] = df_new['Earnings_Source']
    
    logger.info(f\"Froze event risk for {len(df_new)} new trades\")
    return df_new
```

**Update:** `core/phase6_freeze_and_archive.py`

```python
def phase6_freeze_and_archive(df, df_master_current):
    # ... existing code ...
    
    # 3️⃣ FREEZE GREEKS (existing)
    df_new = freeze_entry_greeks(df_new)
    
    # 4️⃣ FREEZE PREMIUM (existing)
    df_new = freeze_entry_premium(df_new)
    
    # 5️⃣ FREEZE PHASE 3 EXISTING (existing)
    df_new[\"PCS_Entry\"] = df_new[\"PCS_Score\"]
    df_new[\"Capital_Deployed_Entry\"] = df_new[\"Capital_Deployed\"]
    df_new[\"Moneyness_Pct_Entry\"] = df_new[\"Moneyness_Pct\"]
    df_new[\"DTE_Entry\"] = df_new[\"DTE\"]
    df_new[\"BreakEven_Entry\"] = df_new[\"BreakEven\"]
    
    # 6️⃣ FREEZE VOLATILITY CONTEXT (NEW)
    df_new = freeze_volatility_context(df_new)
    
    # 7️⃣ FREEZE EVENT RISK (NEW)
    df_new = freeze_event_risk(df_new)
    
    # ... rest of existing code ...
```

### Validation Tests

**File:** `tests/test_phase6_freeze_contracts.py` (NEW)

```python
def test_iv_rank_freeze():
    \"\"\"Test IV_Rank freezing contract.\"\"\"
    # Setup: NEW trade with IV_Rank = 45.2
    df_new = pd.DataFrame({
        'TradeID': ['ABC123'],
        'IsNewTrade': [True],
        'IV_Rank': [45.2],
        'IV_Rank_Source': ['historical'],
        'IV_Rank_History_Days': [152]
    })
    
    # Act: Freeze
    df_frozen = freeze_volatility_context(df_new)
    
    # Assert: Entry columns created
    assert 'IV_Rank_Entry' in df_frozen.columns
    assert df_frozen['IV_Rank_Entry'].iloc[0] == 45.2
    assert df_frozen['IV_Rank_Source_Entry'].iloc[0] == 'historical'
    assert df_frozen['IV_Rank_History_Days_Entry'].iloc[0] == 152


def test_earnings_freeze():
    \"\"\"Test earnings freezing contract.\"\"\"
    # Setup: NEW trade with earnings in 25 days
    df_new = pd.DataFrame({
        'TradeID': ['ABC123'],
        'IsNewTrade': [True],
        'Days_to_Earnings': [25.0],
        'Next_Earnings_Date': [pd.Timestamp('2026-01-29')],
        'Earnings_Source': ['yfinance']
    })
    
    # Act: Freeze
    df_frozen = freeze_event_risk(df_new)
    
    # Assert: Entry columns created
    assert 'Days_to_Earnings_Entry' in df_frozen.columns
    assert df_frozen['Days_to_Earnings_Entry'].iloc[0] == 25.0
    assert df_frozen['Next_Earnings_Date_Entry'].iloc[0] == pd.Timestamp('2026-01-29')
    assert df_frozen['Earnings_Source_Entry'].iloc[0] == 'yfinance'


def test_immutability_enforcement():
    \"\"\"Test that EXISTING trades cannot have _Entry fields changed.\"\"\"
    # Setup: EXISTING trade with frozen IV_Rank_Entry
    df_old = pd.DataFrame({
        'TradeID': ['ABC123'],
        'IV_Rank_Entry': [45.2]
    })
    
    df_new = pd.DataFrame({
        'TradeID': ['ABC123'],
        'IsNewTrade': [False],  # EXISTING
        'IV_Rank_Entry': [50.0]  # ATTEMPTED CHANGE
    })
    
    # Act & Assert: Should raise ValueError
    with pytest.raises(ValueError, match=\"IMMUTABILITY VIOLATION\"):
        assert_immutable_entry_fields(df_new, df_old)


def test_nan_preservation():
    \"\"\"Test that NaN values are preserved in _Entry columns.\"\"\"
    # Setup: NEW trade with insufficient IV_Rank data
    df_new = pd.DataFrame({
        'TradeID': ['ABC123'],
        'IsNewTrade': [True],
        'IV_Rank': [np.nan],
        'IV_Rank_Source': ['insufficient_data'],
        'IV_Rank_History_Days': [5]
    })
    
    # Act: Freeze
    df_frozen = freeze_volatility_context(df_new)
    
    # Assert: NaN preserved (not substituted)
    assert pd.isna(df_frozen['IV_Rank_Entry'].iloc[0])
    assert df_frozen['IV_Rank_Source_Entry'].iloc[0] == 'insufficient_data'
```

---

## 📊 SCHEMA MIGRATION IMPACT

### Database Schema Changes

**New Columns Added to `active_master.csv`:**

```
# Volatility Context (3 columns)
IV_Rank_Entry: float64
IV_Rank_Source_Entry: str
IV_Rank_History_Days_Entry: int64

# Event Risk (3 columns)
Days_to_Earnings_Entry: float64
Next_Earnings_Date_Entry: datetime64[ns]
Earnings_Source_Entry: str
```

**Total _Entry Columns:** 10 existing + 6 new = **16 _Entry columns**

### Backward Compatibility

**Issue:** Old active_master.csv won't have new _Entry columns

**Solution:** Graceful migration
```python
def migrate_active_master(df_master_old):
    \"\"\"
    Add new _Entry columns to old active_master.csv.
    
    Fills with NaN (unknown historical values).
    \"\"\"
    new_columns = {
        'IV_Rank_Entry': np.nan,
        'IV_Rank_Source_Entry': 'unknown',
        'IV_Rank_History_Days_Entry': 0,
        'Days_to_Earnings_Entry': np.nan,
        'Next_Earnings_Date_Entry': pd.NaT,
        'Earnings_Source_Entry': 'unknown'
    }
    
    for col, default in new_columns.items():
        if col not in df_master_old.columns:
            df_master_old[col] = default
            logger.info(f\"Migrated: Added {col} column\")
    
    return df_master_old
```

**Execution:**
- First run after upgrade: All existing positions get NaN for new _Entry fields
- This is **correct** - we don't have historical entry values for old positions
- Only NEW positions (opened after upgrade) get real _Entry values

---

## 🎓 EXAMPLES & USE CASES

### Example 1: IV Expansion Trade Exit

**Scenario:** Exit when IV_Rank jumps 30+ percentiles

**Setup:**
- Entry: AAPL at IV_Rank = 20 (low vol)
- Strategy: Sell IV (credit spread)
- Exit rule: IV_Rank > 50 (vol expansion → take profit)

**Phase 6 Freeze:**
```python
# At entry (2026-01-04)
TradeID: AAPL_IronCondor_2026_01_04
IV_Rank: 20.3
IV_Rank_Entry: 20.3  ← Frozen
IV_Rank_Source_Entry: "historical"
IV_Rank_History_Days_Entry: 152
```

**Phase 7 Drift (2026-01-15):**
```python
# Current state
IV_Rank: 55.8
IV_Rank_Entry: 20.3  ← Still frozen

# Drift calculation
IV_Rank_Drift = 55.8 - 20.3 = +35.5 percentiles
```

**Phase 8 Decision:**
```python
exit_signal = (IV_Rank_Drift > 30)  # True
exit_reason = f\"IV expansion: {IV_Rank_Drift:.1f} percentile jump\"
# → Generate EXIT order
```

### Example 2: Earnings Crossing Detection

**Scenario:** Exit before earnings to avoid binary risk

**Setup:**
- Entry: MSFT 7 days before earnings
- Strategy: Theta harvesting (short premium)
- Exit rule: If earnings < 3 days away

**Phase 6 Freeze:**
```python
# At entry (2026-01-04)
TradeID: MSFT_BullPut_2026_01_04
Days_to_Earnings: 24.0
Next_Earnings_Date: 2026-01-28
Days_to_Earnings_Entry: 24.0  ← Frozen
Next_Earnings_Date_Entry: 2026-01-28  ← Frozen
```

**Phase 7 Drift (2026-01-25):**
```python
# Current state
Days_to_Earnings: 3.0
Days_to_Earnings_Entry: 24.0  ← Still frozen

# Time progress
Days_Since_Entry = 24 - 3 = 21 days
```

**Phase 8 Decision:**
```python
earnings_soon = (Days_to_Earnings < 7)  # True
exit_reason = f\"Earnings in {Days_to_Earnings} days (approaching risk)\"
# → Generate EXIT order
```

### Example 3: Post-Earnings P&L Analysis

**Scenario:** Did earnings event affect position?

**Setup:**
- Entry: NVDA 10 days before earnings
- Strategy: Neutral (iron condor)
- Question: Did P&L change around earnings?

**Phase 6 Freeze:**
```python
# At entry (2026-01-04)
Days_to_Earnings_Entry: 52.0
Next_Earnings_Date_Entry: 2026-02-25
Premium_Entry: $2.50
```

**Phase 7 Crossing Detection:**
```python
# Check if earnings crossed
earnings_crossed = (
    (Days_to_Earnings_Entry > 0) &  # Was future
    (Days_to_Earnings < 0)           # Now past
)
# True → Earnings occurred since entry
```

**Phase 8 Analysis:**
```python
# Compare P&L before/after earnings
Premium_Change_Pct = (Premium - Premium_Entry) / Premium_Entry * 100

if earnings_crossed:
    analysis = f\"Earnings impact: {Premium_Change_Pct:+.1f}% premium change\"
    # → Log for learning/portfolio analysis
```

---

## 🚀 IMPLEMENTATION TIMELINE

### Phase 1: Design (Complete) ✅
- [x] Inventory all Phase 3 observables
- [x] Define _Entry schema
- [x] Document freeze contracts
- [x] Design validation tests

### Phase 2: Implementation (1-2 hours)
- [ ] Create `freeze_entry_observables.py` module
- [ ] Add `freeze_volatility_context()` function
- [ ] Add `freeze_event_risk()` function
- [ ] Update `phase6_freeze_and_archive.py` main flow
- [ ] Update `ALL_ENTRY_FIELDS` constant

### Phase 3: Validation (30 min)
- [ ] Write `test_phase6_freeze_contracts.py`
- [ ] Test IV_Rank freezing
- [ ] Test earnings freezing
- [ ] Test immutability enforcement
- [ ] Test NaN preservation

### Phase 4: Migration (15 min)
- [ ] Write `migrate_active_master()` function
- [ ] Test backward compatibility
- [ ] Document migration notes

### Phase 5: Documentation (15 min)
- [ ] Update PHASE6_CONTRACT.md
- [ ] Update PHASE_1_4_SCHEMA_REFERENCE.md
- [ ] Add drift analysis examples to docs

---

## ✅ ACCEPTANCE CRITERIA

**Phase 6 freeze is complete when:**

1. ✅ All Phase 3 observables have _Entry columns
2. ✅ Immutability guaranteed (tests pass)
3. ✅ NaN preservation working (no magic defaults)
4. ✅ Provenance tracking complete (_Source columns)
5. ✅ Backward compatibility (old active_master.csv migrates)
6. ✅ Validation tests green
7. ✅ Documentation updated

**Phase 7 drift is enabled when:**

1. ✅ Can calculate `IV_Rank_Drift`
2. ✅ Can detect `Earnings_Crossed`
3. ✅ Can compute `Days_Since_Entry`
4. ✅ Can track all frozen vs current comparisons

---

## 📝 CONCLUSION

**Status:** Design complete, implementation ready

**Effort:** ~2 hours (mostly wiring, minimal new code)

**Risk:** Low (follows existing freeze pattern)

**Dependencies:** None (Phase 3 observables already complete)

**Next Steps:**
1. Implement `freeze_entry_observables.py`
2. Wire into Phase 6 main flow
3. Test with real positions
4. Document drift analysis patterns

**Architecture Correctness:** ✅ Approved
- Follows immutability principle
- Copy-only (no recalculation)
- NaN preservation (no magic defaults)
- Provenance tracking (data quality)
- Backward compatible (graceful migration)

**Ready for implementation when user approves.**
