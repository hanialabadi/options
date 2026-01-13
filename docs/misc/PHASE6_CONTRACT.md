# Phase 6 Contract: Entry Value Freeze & Trade Lifecycle Checkpoint

**Authority**: [`core/phase6_freeze_and_archive.py`](core/phase6_freeze_and_archive.py)  
**Pipeline Position**: After Phase 4 snapshot persistence, before Phase 7+  
**Purpose**: Historical anchoring — freeze entry values for new trades, enforce immutability

---

## 1. Inputs

### Required Inputs
- **`df`** (DataFrame): Phase 4 enriched snapshot from latest pipeline run
  - Must include: Symbol, Strategy, Premium, Delta, Gamma, Vega, Theta, PCS_Score, Capital_Deployed, Moneyness_Pct, DTE, BreakEven
  - Source: Phase 3 enrichment → Phase 4 persistence
  
- **`df_master_current`** (DataFrame): Historical active_master.csv from previous runs
  - Contains: All trades with _Entry fields frozen at trade inception
  - Lifecycle states: NEW, EXISTING, CLOSED
  - File: `data/active_master.csv`

### Optional Inputs
- **`run_id`** (str): Pipeline execution identifier for audit trail
- **`config`** (dict): Phase 6 configuration overrides (rarely used)

---

## 2. Outputs

### Primary Output
- **`df_master`** (DataFrame): Updated active_master with:
  - **NEW trades**: All _Entry fields frozen (Phase 5 values → _Entry columns)
  - **EXISTING trades**: _Entry fields preserved (immutable, no overwrites)
  - **CLOSED trades**: Tracked but not updated
  
### Side Effects
- **Persists**: `data/active_master.csv` (updated master ledger)
- **Logs**: Phase 6 execution summary (new/existing/closed counts)
- **Validates**: Multi-leg structure integrity via `evaluate_leg_status()`

---

## 3. Core Guarantees

### ✅ Immutability Guarantee
**Contract**: Once a `*_Entry` field is written for a trade, it NEVER changes.

**Enforcement**:
- `assert_immutable_entry_fields()` validates no overwrites on EXISTING trades
- Only NEW trades (IsNewTrade == True) get _Entry fields populated
- Raises ValueError if attempted overwrite detected

**Protected Fields**:
```python
_Entry_fields = [
    "Premium_Entry",        # Greeks
    "Delta_Entry",
    "Gamma_Entry", 
    "Vega_Entry",
    "Theta_Entry",
    "PCS_Entry",            # Phase 3 enrichment
    "Capital_Deployed_Entry",
    "Moneyness_Pct_Entry",
    "DTE_Entry",
    "BreakEven_Entry"
]
```

### ✅ Lifecycle Detection Guarantee
**Contract**: Phase 6 correctly identifies NEW, EXISTING, and CLOSED trades.

**Implementation**: `merge_master(df, df_master_current)`
- **NEW**: Trade ID in Phase 4 snapshot, not in active_master
- **EXISTING**: Trade ID in both Phase 4 snapshot and active_master
- **CLOSED**: Trade ID in active_master, not in Phase 4 snapshot

**Marks**: Adds `IsNewTrade` boolean column (True only for NEW)

### ✅ Copy-Only Guarantee  
**Contract**: Phase 6 performs NO calculations, only column copying.

**Design**:
- Freezers copy Phase 5 columns → _Entry columns
- No derivations, no formulas, no estimation
- Source truth: Phase 1-4 pipeline outputs

**Example**:
```python
df_new["Premium_Entry"] = df_new["Premium"]  # Copy, not compute
df_new["Delta_Entry"] = df_new["Delta"]      # Copy, not compute
```

### ✅ Structure Integrity Guarantee
**Contract**: Multi-leg positions maintain parent-leg relationships.

**Validation**: `evaluate_leg_status(df_master)`
- Checks: All legs of a multi-leg trade have consistent lifecycle states
- Flags: Orphaned legs, mismatched closures
- Warns: Does not block execution (warn-and-mark pattern)

---

## 4. Execution Flow

### Step-by-Step Contract
```python
def phase6_freeze_and_archive(df, df_master_current):
    # 1️⃣ LIFECYCLE DETECTION
    df_master = merge_master(df, df_master_current)
    # → Adds IsNewTrade column
    # → Preserves existing _Entry fields
    
    # 2️⃣ FILTER NEW TRADES ONLY
    df_new = df_master[df_master["IsNewTrade"] == True].copy()
    # → Only NEW trades proceed to freezing
    
    # 3️⃣ FREEZE GREEKS (new trades only)
    df_new = freeze_entry_greeks(df_new)
    # → Creates: Delta_Entry, Gamma_Entry, Vega_Entry, Theta_Entry
    
    # 4️⃣ FREEZE PREMIUM (new trades only)
    df_new = freeze_entry_premium(df_new)
    # → Creates: Premium_Entry
    
    # 5️⃣ FREEZE PHASE 3 FIELDS (inline, new trades only)
    df_new["PCS_Entry"] = df_new["PCS_Score"]
    df_new["Capital_Deployed_Entry"] = df_new["Capital_Deployed"]
    df_new["Moneyness_Pct_Entry"] = df_new["Moneyness_Pct"]
    df_new["DTE_Entry"] = df_new["DTE"]
    df_new["BreakEven_Entry"] = df_new["BreakEven"]
    # → Creates: 5 Phase 3 _Entry columns
    
    # 6️⃣ MERGE BACK
    df_master.update(df_new)
    # → NEW trades now have _Entry fields
    # → EXISTING trades unchanged
    
    # 7️⃣ VALIDATE STRUCTURE
    df_master = evaluate_leg_status(df_master)
    # → Checks multi-leg integrity
    # → Adds warnings if orphaned legs detected
    
    # 8️⃣ ENFORCE IMMUTABILITY
    assert_immutable_entry_fields(df_master, df_master_current)
    # → Raises ValueError if EXISTING _Entry fields changed
    
    # 9️⃣ PERSIST
    save_active_master(df_master)
    # → Writes to data/active_master.csv
    
    return df_master
```

---

## 5. What Phase 6 Does NOT Do

### ❌ Does NOT calculate or derive
- No Premium estimation (uses broker truth from Phase 1)
- No Greek computation (uses Phase 5 outputs)
- No PCS recalculation (uses Phase 3 enrichment)

### ❌ Does NOT delete trades
- Closed trades tracked, not removed
- Historical audit trail preserved
- Master ledger append-only for _Entry fields

### ❌ Does NOT modify EXISTING trades
- _Entry fields immutable after first freeze
- Current values can change (Phase 4 snapshot)
- Entry values locked forever

### ❌ Does NOT orchestrate multiple freezers
- Uses only 2 freezer modules directly (Greeks, Premium)
- Phase 3 fields frozen inline (no modules)
- Simpler than Implementation B's modular approach

---

## 6. Critical Dependencies

### Core Modules (Production)
- **`freeze_merge_master.py`**: Lifecycle detection, preserves _Entry fields
- **`freeze_entry_greeks.py`**: Copies Delta/Gamma/Vega/Theta → _Entry
- **`freeze_entry_premium.py`**: Copies Premium → Premium_Entry
- **`evaluate_leg_status.py`**: Multi-leg structure validation
- **`freeze_helpers.py`**: assert_immutable_entry_fields(), detect_new_trades()
- **`save_active_master.py`**: Persistence layer (CSV write)

### Legacy Modules (Not Used by Authority)
- **`freeze_entry_ivhv.py`**: IV/HV freeze (Implementation B only)
- **`freeze_entry_chart.py`**: Chart data (Implementation B only)
- **`freeze_entry_metadata.py`**: Metadata (Implementation B only)
- **`freeze_all_entry_fields.py`**: Orchestrator (Implementation B only)

### Quarantined Files
Moved to `core/phase6_freeze/_quarantine/`:
- `freeze_archive_export.py` (0 references)
- `freeze_legs_export.py` (1 reference, export utility)
- `phase6_fork_reroute.py` (1 reference, routing)
- `freeze_fields.py` (4 references, superseded)

### Deprecated Implementations
Renamed with `_LEGACY.py` suffix (DO NOT IMPORT):
- `core/phase6_freeze/phase6_freeze_and_archive_LEGACY.py`
- `core/phase6_freeze/freezer_modules/phase6_freeze_and_archive_LEGACY.py`

---

## 7. Integration Points

### Upstream Dependencies (Phase 1-4)
- **Phase 1**: Premium derivation from Time Val (broker truth only)
- **Phase 2**: Strategy parsing, OCC symbol validation
- **Phase 3**: Enrichment (PCS, Capital, Moneyness, Breakeven)
- **Phase 4**: DuckDB + CSV snapshot persistence

### Downstream Dependencies (Phase 7+)
- **Phase 7** (planned): Risk scoring, portfolio-level metrics
- **Phase 8** (planned): Exit signal generation, trade management
- **Dashboard**: Displays _Entry vs current values for P&L tracking

### Master Ledger
- **File**: `data/active_master.csv`
- **Schema**: Phase 4 snapshot + _Entry fields
- **Versioning**: run_id column for audit trail
- **Lifecycle**: Append-only for NEW trades, immutable for EXISTING

---

## 8. Testing Contract

### Unit Tests (Required)
- ✅ **test_immutability_enforcement**: Verify _Entry fields never overwritten
- ✅ **test_new_trade_detection**: Verify IsNewTrade marking correct
- ✅ **test_closed_trade_tracking**: Verify closed trades not updated
- ✅ **test_leg_status_validation**: Verify multi-leg integrity checks

### Integration Tests (Required)
- ✅ **test_phase1_to_phase6**: Full pipeline with Phase 6 integration
- ✅ **test_entry_freeze_persistence**: Verify CSV write correctness
- ✅ **test_entry_vs_current**: Verify _Entry != current for price moves

### Edge Cases (Required)
- ✅ **Empty active_master**: First run with no prior trades
- ✅ **All EXISTING trades**: No NEW trades in Phase 4 snapshot
- ✅ **All CLOSED trades**: Entire portfolio liquidated
- ✅ **Mixed lifecycle**: NEW + EXISTING + CLOSED in same run

---

## 9. Error Handling

### Fail-Loud Errors (Pipeline Halts)
- **ValueError**: Attempted overwrite of _Entry field on EXISTING trade
- **KeyError**: Missing required column in Phase 4 snapshot (e.g., Premium, Delta)
- **FileNotFoundError**: active_master.csv expected but not found (non-first run)

### Warn-and-Mark Errors (Pipeline Continues)
- **Orphaned legs**: Multi-leg position missing child/parent leg (logged, execution continues)
- **Negative _Entry values**: Premium_Entry < 0 (logged, not blocked in Phase 6)

### Silent Success
- **All NEW trades**: active_master.csv empty on first run (expected)
- **No NEW trades**: Phase 4 snapshot matches active_master exactly (no freezing needed)

---

## 10. Regression Prevention

### Phase 6 Boundary Rules
- **DO NOT** add calculations to Phase 6 (enrichment belongs in Phase 3)
- **DO NOT** modify _Entry fields after first freeze (immutability contract)
- **DO NOT** use Phase 6 for data quality gates (belongs in Phase 1-4)
- **DO NOT** merge Implementation A + B (A is authority, B is abandoned)

### Before Adding Features
Ask:
1. Does this violate _Entry immutability?
2. Does this belong in Phase 3 enrichment instead?
3. Does this change lifecycle detection logic?
4. Does this break master ledger append-only contract?

If YES to any → reject feature or move to correct phase.

---

## 11. Historical Context

### Why Two Implementations Existed
- **Implementation A (current)**: Simple, inline Phase 3 freezing, production-ready
- **Implementation B (legacy)**: Modular freezers, never completed, missing files

**Implementation B was abandoned** because:
- Missing `freeze_entry_pcs_score.py` and `freeze_entry_date.py`
- No closed trade tracking (no merge_master)
- No leg status validation (no evaluate_leg_status)
- No persistence (no save_active_master)

**Implementation A is authority** because:
- Complete, self-contained, tested
- Used in production pipeline
- Enforces all guarantees (immutability, lifecycle, structure)

### Why Phase 6 Is Not Enrichment
Phase 6 is **historical anchoring**, not **data derivation**.

- **Enrichment** (Phase 3): Computes PCS_Score, Moneyness_Pct, etc.
- **Freeze** (Phase 6): Copies computed values → _Entry columns (no calculation)

This separation ensures:
- _Entry fields are snapshots, not live calculations
- P&L tracking compares _Entry (historical) vs current (live)
- No regression risk from enrichment logic changes affecting historical values

---

## 12. Success Criteria

### Phase 6 is correct if:
- ✅ All NEW trades have _Entry fields populated
- ✅ No EXISTING trades have _Entry fields modified
- ✅ CLOSED trades tracked but not updated
- ✅ Multi-leg positions validated for integrity
- ✅ Master ledger persisted to CSV
- ✅ No calculations performed (copy-only)

### Phase 6 has failed if:
- ❌ EXISTING trade _Entry field overwritten
- ❌ NEW trade missing _Entry field
- ❌ _Entry field contains calculated value (not copied)
- ❌ Closed trade incorrectly marked as NEW
- ❌ Master ledger corrupted or missing

---

## 13. Maintenance Guidelines

### Safe Modifications
- ✅ Add new _Entry fields (if Phase 3 enrichment added)
- ✅ Improve leg status validation logic
- ✅ Enhance logging/audit trail
- ✅ Optimize CSV persistence format

### Forbidden Modifications
- ❌ Change _Entry field population logic (immutability contract)
- ❌ Add calculations to freeze logic (copy-only contract)
- ❌ Modify lifecycle detection (NEW/EXISTING/CLOSED)
- ❌ Skip immutability validation (assert_immutable_entry_fields)

### Review Checklist
Before merging Phase 6 changes:
1. [ ] All unit tests pass (immutability, lifecycle, structure)
2. [ ] Integration test passes (Phase 1-6 full pipeline)
3. [ ] No _Entry field calculations added
4. [ ] No changes to merge_master() logic
5. [ ] No bypass of assert_immutable_entry_fields()

---

**Version**: 1.0  
**Last Updated**: 2026-01-03  
**Authority File**: [`core/phase6_freeze_and_archive.py`](core/phase6_freeze_and_archive.py)  
**Status**: Production-ready, contracts enforced
