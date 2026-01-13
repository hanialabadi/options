# Quick Reference: Exploration vs Selection

## The Golden Rules

### 1. Exploration (Step 9B)
```
NOTHING is rejected during exploration.
Everything is annotated with context.
```

### 2. Selection (Step 8)
```
NO trade is valid without complete audit record.
5 WHY explanations required.
```

### 3. Volume
```
Volume is informational ONLY.
Open Interest is primary signal.
Volume must NEVER be a hard gate.
```

### 4. LEAPs
```
LEAPs must NEVER disappear.
LEAPs get relaxed criteria.
Is_LEAP must be explicit.
```

---

## Key Annotations

### Step 9B Output Columns

| Column | Purpose | Example |
|--------|---------|---------|
| `Liquidity_Class` | Grade: Excellent/Good/Acceptable/Thin | "Thin" |
| `Liquidity_Context` | Human explanation | "High-price underlying - wide spreads expected" |
| `Is_LEAP` | Boolean flag | `True` |
| `Horizon_Class` | Short/Medium/LEAP | "LEAP" |
| `LEAP_Reason` | Why classified as LEAP | "DTE > 365" |
| `Contract_Selection_Status` | Success/Low_Liquidity/etc | "Success" |

### Step 8 Audit Components

1. **STRATEGY SELECTION** - Why this strategy
2. **CONTRACT CHOICE** - Why this expiration/strike
3. **LIQUIDITY JUSTIFICATION** - Why liquidity acceptable
4. **CAPITAL ALLOCATION** - Why sizing approved
5. **COMPETITIVE COMPARISON** - Why others rejected

---

## Code Snippets

### Check Step 9B Integrity
```python
# Count must be preserved
assert len(df_out) == len(df_in), "Strategies disappeared!"

# LEAPs must be visible
if df_in['Max_DTE'].max() >= 365:
    assert df_out['Is_LEAP'].any(), "LEAPs missing!"
```

### Validate Step 8 Audit
```python
# All trades must have complete audits
incomplete = df_final['Selection_Audit'].str.contains('INCOMPLETE').sum()
assert incomplete == 0, f"{incomplete} trades have incomplete audits!"

# Verify 5 components present
required = [
    'STRATEGY SELECTION:',
    'CONTRACT CHOICE:',
    'LIQUIDITY JUSTIFICATION:',
    'CAPITAL ALLOCATION:',
    'COMPETITIVE COMPARISON:'
]

for idx, row in df_final.iterrows():
    audit = row['Selection_Audit']
    missing = [comp for comp in required if comp not in audit]
    assert not missing, f"{row['Ticker']}: Missing {missing}"
```

### Review Audit Record
```python
# Print full audit for ticker
ticker = 'BKNG'
audit = df_final[df_final['Ticker'] == ticker]['Selection_Audit'].iloc[0]

print(f"\n{ticker} Audit:")
print("="*80)
for line in audit.split('\n'):
    print(line)
```

---

## Common Patterns

### Pattern 1: LEAP with Thin Liquidity
```python
# Step 9B annotates
Is_LEAP: True
Horizon_Class: "LEAP"
Liquidity_Class: "Thin"
Liquidity_Context: "LEAP horizon - lower liquidity acceptable"
Status: "Success"  # NOT rejected

# Step 8 explains
LIQUIDITY JUSTIFICATION: thin liquidity - requires context-aware 
execution; context: LEAP horizon - lower liquidity acceptable; 
limited OI (19 contracts); wide spread (17.4%)
```

### Pattern 2: Expensive Elite Stock
```python
# Step 9B annotates
Liquidity_Class: "Thin"
Liquidity_Context: "High-price underlying - wide spreads expected"
Capital_Requirement: 95600
Status: "Success"  # NOT hidden

# Step 8 explains
LIQUIDITY JUSTIFICATION: thin liquidity - requires context-aware 
execution; context: High-price underlying - wide spreads expected
```

### Pattern 3: Competitive Selection
```python
# Step 11 ranks
AAPL Long Straddle: Rank=1, Score=78.5
AAPL Long Call: Rank=2, Score=72.0

# Step 8 explains
COMPETITIVE COMPARISON: selected over 1 alternatives; moderate 
advantage (score: 78.5 vs 72.0); rejected alternatives: Long Call
```

---

## Debugging

### Problem: Strategies Disappearing
```python
# Check count preservation
print(f"Step 9A: {len(df_9a)} strategies")
print(f"Step 9B: {len(df_9b)} strategies")
assert len(df_9b) == len(df_9a), "Count mismatch!"

# Check status distribution
print(df_9b['Contract_Selection_Status'].value_counts())
```

### Problem: LEAPs Missing
```python
# Check LEAP tagging
leaps = df_9b[df_9b['Is_LEAP'] == True]
print(f"LEAPs found: {len(leaps)}")
print(leaps[['Ticker', 'Actual_DTE', 'Horizon_Class', 'LEAP_Reason']])

# Verify input had LEAPs
long_dte = df_9a[df_9a['Max_DTE'] >= 365]
print(f"Long DTE strategies in input: {len(long_dte)}")
```

### Problem: Incomplete Audits
```python
# Find incomplete audits
incomplete = df_final[
    df_final['Selection_Audit'].str.contains('INCOMPLETE', na=False)
]
print(f"Incomplete audits: {len(incomplete)}")

# Show what's missing
for idx, row in incomplete.iterrows():
    print(f"\n{row['Ticker']}:")
    print(row['Selection_Audit'])
```

---

## Testing Checklist

### Step 9B (Exploration)
- [ ] Count IN == Count OUT
- [ ] Status annotations for all strategies
- [ ] Is_LEAP flag for DTE >= 365
- [ ] Liquidity_Class present (not None)
- [ ] Liquidity_Context has text
- [ ] No silent rejections
- [ ] BKNG visible if in input
- [ ] Step 10/11 compatibility

### Step 8 (Selection)
- [ ] Selection_Audit column present
- [ ] All 5 components in each audit
- [ ] No INCOMPLETE audits in final output
- [ ] Position_Valid logic working
- [ ] Competitive comparison explains alternatives
- [ ] LEAP justifications reference horizon
- [ ] Capital explanations quantify risk %

---

## Performance Metrics

### Expected Results

**Step 9B:**
- Count preservation: 100%
- LEAP visibility: 100%
- API calls: 50-70% reduction (chain caching)
- Status distribution: ~5-10% Success, rest annotated failures

**Step 8:**
- Audit completeness: 100%
- Selection rate: ~20% (50 trades from 266 strategies)
- Invalid trades: 0 (incomplete audits filtered)

---

## File Locations

### Core Implementation
- `core/scan_engine/step9b_fetch_contracts.py` - Exploration engine
- `core/scan_engine/step8_position_sizing.py` - Auditable selection

### Tests
- `test_step9b_exploration.py` - Exploration validation
- `test_step8_audit.py` - Audit validation

### Documentation
- `EXPLORATION_VS_SELECTION_COMPLETE.md` - Full implementation guide
- `BEFORE_AND_AFTER_COMPARISON.md` - Transformation summary
- `EXPLORATION_VS_SELECTION_QUICK_REFERENCE.md` - This guide

---

## Emergency Fixes

### If Strategies Disappearing
```python
# Step 9B should preserve count
# Check integrity check output in logs
# Look for: "✅ Integrity Check 1/4: Row count preserved"
```

### If LEAPs Rejected
```python
# Check LEAP thresholds in Step 9B
# Should be: min_oi=5 (not 50), max_spread=25% (not 10%)
# Verify Is_LEAP flag being set
```

### If Audit Incomplete
```python
# Check all 5 _explain_* functions return non-empty strings
# Verify input data has required columns
# Look for warnings in log: "⚠️ Incomplete audits"
```

---

**Quick Reference Version:** 1.0  
**Last Updated:** December 28, 2025  
**Status:** Production Ready ✅
