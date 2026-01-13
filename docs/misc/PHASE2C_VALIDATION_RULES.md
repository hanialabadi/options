# Phase 2C: Structural Validation Gate

## 📋 Overview

Phase 2C is a **read-only structural integrity gate** that runs inside Phase 2, after strategy tagging but before Phase 3 enrichment.

**Purpose**: Enforce structural contracts without mutating core columns (TradeID, Strategy, LegType, Account, Structure).

**Architecture**: This is a GATE, not a FIXER. Flags are raised, but no rows are dropped and no strategies are reassigned.

---

## 🔍 Validation Rules

### 1️⃣ TradeID Integrity

Every TradeID must satisfy:

| Rule | Validation | Flag on Failure |
|------|-----------|-----------------|
| **Single Account** | All positions in TradeID have same Account | `Cross_Account_TradeID` |
| **Single Underlying** | All positions in TradeID have same Underlying | `Mixed_Underlying` |

**Why it matters**: Phase 3 enrichment assumes TradeID is an atomic unit with consistent account and underlying. Cross-account trades break portfolio accounting.

---

### 2️⃣ Strategy ↔ Leg Consistency

Validates that the detected strategy matches the actual position structure:

#### Covered Call
| Required Legs | Forbidden Legs | Validation Flags |
|--------------|----------------|------------------|
| ≥1 STOCK (Qty > 0) | No Puts | `Missing_Leg:STOCK` |
| ≥1 SHORT_CALL | | `Missing_Leg:SHORT_CALL` |
| | | `Extra_Leg:PUT` |
| | | `Invalid_Structure:Stock_Not_Long` |

**Example Valid**: 100 AAPL + -1 AAPL250117C150  
**Example Invalid**: -1 AAPL250117C150 only (no stock)

---

#### Cash-Secured Put (CSP)
| Required Legs | Forbidden Legs | Validation Flags |
|--------------|----------------|------------------|
| 1 SHORT Put only | No Stock | `Missing_Leg:SHORT_PUT` |
| | No Calls | `Extra_Leg:STOCK` |
| | | `Extra_Leg:CALL` |
| | | `Invalid_Structure:Multi_Leg_CSP` |

**Example Valid**: -1 TSLA260220P200  
**Example Invalid**: -1 TSLA260220P200 + 100 TSLA (has stock)

---

#### Long Straddle
| Required Legs | Forbidden Legs | Validation Flags |
|--------------|----------------|------------------|
| 1 Call + 1 Put | No Stock | `Missing_Leg:Straddle_Incomplete` |
| Same Strike | | `Strategy_Mismatch:Strikes_Differ` |
| Same Expiration | | `Strategy_Mismatch:Expirations_Differ` |
| | | `Illegal_Leg_Combination:Stock_In_Straddle` |

**Example Valid**: +1 SHOP250117C165 + +1 SHOP250117P165  
**Example Invalid**: +1 SHOP250117C165 only (missing put leg)

---

#### Long Strangle
| Required Legs | Forbidden Legs | Validation Flags |
|--------------|----------------|------------------|
| 1 Call + 1 Put | No Stock | `Missing_Leg:Strangle_Incomplete` |
| Different Strikes | | `Strategy_Mismatch:Same_Strike_Not_Strangle` |
| Same Expiration | | `Strategy_Mismatch:Expirations_Differ` |
| | | `Illegal_Leg_Combination:Stock_In_Strangle` |

**Example Valid**: +1 SPY250117C600 + +1 SPY250117P580  
**Example Invalid**: +1 SPY250117C600 + +1 SPY250220P580 (expiration mismatch)

---

#### Buy Call / Buy Put
| Required Legs | Forbidden Legs | Validation Flags |
|--------------|----------------|------------------|
| Exactly 1 option leg | No Stock | `Invalid_Structure:Multi_Leg_Buy_Call` |
| Quantity > 0 | | `Strategy_Mismatch:Not_Long_Call` |
| | | `Extra_Leg:STOCK` |

**Example Valid**: +1 NVDA280121C800  
**Example Invalid**: +1 NVDA280121C800 + 100 NVDA (has stock)

---

### 3️⃣ Structural Sanity Checks

Cross-position structural integrity rules:

| Check | Description | Flag on Failure |
|-------|-------------|-----------------|
| **No Duplicate Legs** | Symbol appears only once per TradeID | `Duplicate_Leg:{Symbol}` |
| **No Illegal Combinations** | Stock cannot exist in Straddles/Strangles | `Illegal_Leg_Combination:Stock_In_Straddle` |
| **Strategy-Asset Alignment** | Option-only strategies have no stock | `Extra_Leg:STOCK` |

---

## 📤 Output Columns

Phase 2C appends these columns:

| Column | Type | Description |
|--------|------|-------------|
| `Structure_Valid` | bool | `False` if any validation rule fails |
| `Validation_Errors` | str | Pipe-delimited list of error flags |
| `Needs_Structural_Fix` | bool | `True` if structure would break Phase 3 assumptions |

**Example Output**:
```python
TradeID                          | Structure_Valid | Validation_Errors                      | Needs_Structural_Fix
---------------------------------|-----------------|----------------------------------------|---------------------
AAPL_260220_CoveredCall_5376    | True            |                                        | False
TSLA260220P200_Short_CSP_4854   | False           | Extra_Leg:STOCK                        | True
SHOP_250117_LongStraddle_5376   | False           | Missing_Leg:Straddle_Incomplete        | True
```

---

## 🚫 Explicit Constraints

Phase 2C validation **DOES NOT**:

- ❌ Reassign Strategy labels
- ❌ Modify TradeID values
- ❌ Create or merge legs
- ❌ Use market data or APIs
- ❌ Touch Phase 3 code
- ❌ Auto-correct invalid structures
- ❌ Drop rows

**This is diagnostic only.**

---

## ✅ Success Criteria

When working correctly:

1. Phase 2 runs without errors
2. Structural flags are visible in output DataFrame
3. Invalid structures are flagged but not dropped
4. Phase 3 can trust validated structures completely
5. Optional: Execution guard can block Phase 3 if `Needs_Structural_Fix == True`

---

## 🔧 Integration

### Before (Phase 2 only):
```python
df = phase2_parse_symbols(df)      # Phase 2A: Parse OCC symbols
df = phase21_strategy_tagging(df)  # Phase 2B: Detect strategies
return df
```

### After (Phase 2 + Validation):
```python
df = phase2_parse_symbols(df)      # Phase 2A: Parse OCC symbols
df = phase21_strategy_tagging(df)  # Phase 2B: Detect strategies
df = validate_structures(df)       # Phase 2C: Structural validation gate ← NEW
return df
```

---

## 📊 Validation Summary Output

When validation runs, you'll see:

### ✅ All Valid:
```
✅ Phase 2C Validation: All 28 TradeIDs structurally valid
```

### ⚠️ Issues Detected:
```
⚠️  Phase 2C Validation Summary:
   Total TradeIDs: 28
   Invalid Structures: 3
   Critical Issues (Needs Fix): 5 positions

   Top Issues:
     • Missing_Leg:SHORT_CALL: 2
     • Extra_Leg:STOCK: 2
     • Duplicate_Leg:AAPL250117C150: 1
```

---

## 🔄 Next Steps

### Optional Enhancements:

1. **Add Phase 2 → Phase 3 execution guard** ✅ IMPLEMENTED:
   ```python
   from core.phase2_validate_structures import enforce_validation_gate
   
   df = phase2_run_all(df)
   df = enforce_validation_gate(df, strict=True)  # Blocks if invalid
   df = phase3_enrich(df)  # Only runs if validation passed
   ```
   
   Or use warning mode for debugging:
   ```python
   df = enforce_validation_gate(df, strict=False)  # Warns but continues
   ```

2. **Design Phase 3 "trust contract"**:
   - Phase 3 assumes all inputs passed validation
   - Document Phase 3 dependencies on validated structures
   - Define Phase 3 behavior when validation is skipped

3. **Export validation report**:
   ```python
   from core.phase2_validate_structures import get_validation_summary
   
   invalid = get_validation_summary(df)
   if not invalid.empty:
       invalid.to_csv("validation_report.csv")
   ```

---

## 🧪 Testing

Test the validator with intentionally malformed structures:

```python
# Test 1: Cross-account trade
# Expected: Cross_Account_TradeID flag

# Test 2: Covered call without stock
# Expected: Missing_Leg:STOCK flag

# Test 3: Straddle with different strikes
# Expected: Strategy_Mismatch:Strikes_Differ flag

# Test 4: Duplicate symbol
# Expected: Duplicate_Leg flag
```

---

## 📁 Files Modified

1. **Created**: `core/phase2_validate_structures.py` (new validation module)
2. **Modified**: `core/phase2_parse.py` (added import and call in `phase2_run_all()`)
3. **Unchanged**: `core/phase3_enrich/*` (no Phase 3 modifications)

---

## 🎯 Architectural Insertion Point

Phase 2C sits at the perfect boundary:

```
Phase 1 (Intake)
    ↓
Phase 2A (Symbol Parsing)
    ↓
Phase 2B (Strategy Detection)
    ↓
Phase 2C (Structural Validation) ← YOU ARE HERE
    ↓
Phase 3 (Enrichment - trusts validated structures)
```

This is the correct enforcement layer. Phase 3 remains unchanged.
