# Step 10: PCS Recalibration & Pre-Filter

## Overview

Step 10 provides **neutral, rules-based validation** of option contracts selected by Step 9B. It filters poor-risk setups before execution by applying multi-factor quality scoring and structural validation.

**Purpose:** Ensure only high-quality, executable contracts proceed to trade execution
**Input:** Step 9B output (option chains with selected contracts)
**Output:** Validated contracts with PCS scores and execution readiness flags

---

## Architecture

### Core Design Principles

1. **Neutral Scoring:** No directional bias, purely structural quality assessment
2. **Configurable Thresholds:** Adjustable for different risk tolerances
3. **Multi-Factor Validation:** Combines liquidity, risk, DTE, structure, and strategy-specific rules
4. **Clear Status Assignment:** Valid / Watch / Rejected with detailed reasons
5. **Execution Promotion:** Valid contracts upgraded from 'Scan' to 'Execution_Candidate'

### Integration with Step 9B

Step 10 operates on Step 9B's output structure:

```python
# Step 9B Output Columns (consumed by Step 10)
- Selected_Expiration
- Actual_DTE
- Selected_Strikes (JSON)
- Contract_Symbols (JSON)
- Actual_Risk_Per_Contract
- Total_Debit / Total_Credit
- Bid_Ask_Spread_Pct
- Open_Interest
- Liquidity_Score
- Risk_Model (Debit_Max / Credit_Max / Stock_Dependent)
- Contract_Intent ('Scan' initially)
- Structure_Simplified (calendar/diagonal flag)
- Contract_Selection_Status
```

---

## Validation Rules

### Rule 1: Liquidity Validation

**Metrics:**
- `Liquidity_Score`: Multi-factor score (0-100) from Step 9B
  - Open Interest: 40%
  - Bid-Ask Spread: 40%
  - Volume: 20%
- `Bid_Ask_Spread_Pct`: Percentage spread width
- `Open_Interest`: Total OI for selected contracts

**Thresholds:**
- `min_liquidity_score`: Default 30.0 (strict mode: 45.0)
- `max_spread_pct`: Default 8.0% (strict mode: 5.6%)

**Logic:**
```python
if liquidity_score < min_liquidity_score:
    reasons.append("Low liquidity score")
    
if spread_pct > max_spread_pct:
    reasons.append("Wide spread")
```

### Rule 2: DTE Validation

**Metrics:**
- `Actual_DTE`: Days to expiration from Step 9B

**Thresholds:**
- `min_dte`: Default 5 days (strict mode: 7 days)
- Strategy-specific: LEAPS requires DTE ≥ 90

**Logic:**
```python
if actual_dte < min_dte:
    reasons.append("DTE too short")
    status = 'Rejected'
    
if 'LEAPS' in strategy and actual_dte < 90:
    reasons.append("LEAPS requires DTE≥90")
    status = 'Rejected'
```

### Rule 3: Structure Validation

**Metrics:**
- `Structure_Simplified`: Boolean flag from Step 9B

**Logic:**
```python
if structure_simplified:
    reasons.append("Calendar/Diagonal simplified")
    status = 'Watch'  # Always Watch (multi-expiration not fully supported)
```

### Rule 4: Risk Model Validation

**Metrics:**
- `Risk_Model`: Debit_Max / Credit_Max / Stock_Dependent
- `Actual_Risk_Per_Contract`: Dollar risk (or None for Stock_Dependent)

**Logic:**
```python
if risk_model == 'Stock_Dependent' and risk_per_contract is None:
    reasons.append("Stock_Dependent risk requires portfolio validation")
    # Not rejected - needs external validation

# Score assignment:
# Debit_Max / Credit_Max: 100% (20% weight)
# Stock_Dependent: 50% (neutral - needs portfolio check)
# Other: 0%
```

### Rule 5: Strategy-Specific Validation

**Credit Spreads:**
- OI < 50: -30 points
- OI < 100: -10 points

**Debit Spreads:**
- Risk > $2000: -30 points
- Risk > $1500: -15 points

**Straddles/Strangles:**
- OI < 100: -40 points
- OI < 200: -15 points

**Iron Condors:**
- OI < 150: -50 points
- OI < 300: -20 points

**Threshold:** Strategy score < 50 triggers rejection

---

## PCS Score Calculation

**Formula:** Weighted sum of 4 components (0-100 scale)

```python
PCS_Score = (
    (Liquidity_Score / 100 * 100) * 0.30 +  # 30% weight
    (DTE / 60 * 100) * 0.20 +                # 20% weight (normalized to 60 days)
    (Risk_Clarity) * 0.20 +                  # 20% weight (0/50/100)
    (Strategy_Specific_Score) * 0.30         # 30% weight
)
```

**Component Details:**

1. **Liquidity (30%):** Direct mapping from Step 9B's Liquidity_Score
2. **DTE Appropriateness (20%):** Normalized to 60-day standard (DTE/60 * 100, capped at 100)
3. **Risk Clarity (20%):**
   - Debit_Max / Credit_Max: 100 points
   - Stock_Dependent: 50 points (needs portfolio validation)
   - Other: 0 points
4. **Strategy-Specific (30%):** Quality checks tailored to strategy type (see Rule 5)

**Example:**
```
Debit Spread: DTE=45, Liquidity=75, OI=500, Risk=$500
- Liquidity: (75/100*100)*0.30 = 22.5
- DTE: (45/60*100)*0.20 = 15.0
- Risk: 100*0.20 = 20.0
- Strategy: 100*0.30 = 30.0
- Total: 87.5
```

---

## Status Assignment

### Valid ✅
**Criteria:**
- All validation rules passed
- No reasons collected
- PCS_Score typically 70-100

**Actions:**
- `Pre_Filter_Status = 'Valid'`
- `Execution_Ready = True`
- `Contract_Intent = 'Execution_Candidate'`

### Watch ⚠️
**Criteria:**
- Non-critical issues detected
- Simplified calendar/diagonal structures
- Low liquidity or wide spreads (but not critical)
- PCS_Score typically 40-70

**Actions:**
- `Pre_Filter_Status = 'Watch'`
- `Execution_Ready = False`
- `Contract_Intent` remains 'Scan'

**Common Watch Reasons:**
- Wide spread (> max_spread_pct)
- Low liquidity (< min_liquidity_score)
- Calendar/Diagonal simplified
- Stock_Dependent risk (needs portfolio check)

### Rejected ❌
**Criteria:**
- Critical failures detected
- DTE too short (< min_dte)
- LEAPS with insufficient DTE (< 90)
- Strategy-specific score < 50
- PCS_Score typically 0-50

**Actions:**
- `Pre_Filter_Status = 'Rejected'`
- `Execution_Ready = False`
- `Contract_Intent` remains 'Scan'

**Common Rejection Reasons:**
- DTE too short (< 5 days)
- LEAPS requires DTE≥90
- Strategy-specific validation failed
- Combination of critical issues

---

## Output Columns

Step 10 adds the following columns to Step 9B's DataFrame:

| Column | Type | Description |
|--------|------|-------------|
| `Pre_Filter_Status` | str | 'Valid' / 'Watch' / 'Rejected' |
| `Filter_Reason` | str | Detailed explanation of status |
| `PCS_Score` | float | Quality score 0-100 |
| `Execution_Ready` | bool | True only if status='Valid' |
| `Contract_Intent` | str | Updated to 'Execution_Candidate' if valid |

**Note:** All Step 9B columns are preserved in the output.

---

## Usage Examples

### Basic Usage (Default Parameters)

```python
from scan_engine import recalibrate_and_filter
import pandas as pd

# Load Step 9B output
step9b_df = pd.read_csv('Step9B_Chain_Scan_Output.csv')

# Apply Step 10 validation
step10_df = recalibrate_and_filter(step9b_df)

# Filter for execution
execution_ready = step10_df[step10_df['Execution_Ready'] == True]
print(f"Execution candidates: {len(execution_ready)}/{len(step10_df)}")
```

### Conservative (Strict Mode)

```python
# Tighter thresholds for risk-averse trading
step10_df = recalibrate_and_filter(
    step9b_df,
    min_liquidity_score=30.0,  # Base threshold
    max_spread_pct=8.0,
    min_dte=5,
    strict_mode=True  # Applies 1.5x liquidity, 0.7x spread, 1.4x DTE
)
# Effective thresholds: liquidity≥45, spread≤5.6%, DTE≥7
```

### Aggressive (Relaxed Thresholds)

```python
# Allow more contracts for liquid underlyings
step10_df = recalibrate_and_filter(
    step9b_df,
    min_liquidity_score=20.0,  # Lower liquidity requirement
    max_spread_pct=12.0,        # Wider spreads tolerated
    min_dte=3                   # Shorter DTE allowed
)
```

### Review Watch Status

```python
# Analyze Watch contracts
watch_df = step10_df[step10_df['Pre_Filter_Status'] == 'Watch']
print(watch_df[['Ticker', 'Primary_Strategy', 'Filter_Reason', 'PCS_Score']])

# Manually approve specific Watch contracts if desired
# (custom logic based on Filter_Reason)
```

---

## Pipeline Integration

Step 10 integrates seamlessly into the full scan pipeline:

```python
from scan_engine.pipeline import run_full_scan_pipeline

df = run_full_scan_pipeline(
    output_dir='output',
    include_step9b=True,
    include_step10=True,
    tradier_token='YOUR_TOKEN',
    
    # Step 10 parameters
    pcs_min_liquidity=30.0,
    pcs_max_spread=8.0,
    pcs_strict_mode=False,
    
    # Other steps...
)

# CSV exports:
# - Step9B_Chain_Scan_YYYYMMDD_HHMMSS.csv (raw contracts)
# - Step10_Filtered_YYYYMMDD_HHMMSS.csv (validated contracts)
```

---

## Logging & Diagnostics

Step 10 provides detailed logging for transparency:

```
🔍 Step 10: PCS Recalibration for 42 contracts

📊 Step 10 PCS Filter Summary:
   ✅ Valid: 18/42 (42.9%)
   ⚠️  Watch: 15/42 (35.7%)
   ❌ Rejected: 9/42 (21.4%)
   Avg Valid PCS Score: 78.3

   Top Rejection Reasons:
     • DTE too short (< 5): 5
     • Strategy-specific validation failed: 3
     • Wide spread (> 8.0%): 1
```

**Diagnostic Tips:**
- High rejection rate (>40%): Consider relaxing thresholds or improving Step 9B liquidity filtering
- Low average PCS score (<60): Check data quality and liquidity of underlying tickers
- Many Watch statuses: Review reasons for manual approval opportunities

---

## Testing

Comprehensive test suite validates all functionality:

```bash
python test_step10.py
```

**Tests:**
1. ✅ Valid Contract Validation
2. ✅ Wide Spread Filtering
3. ✅ Low Liquidity Watch
4. ✅ Short DTE Rejection
5. ✅ Simplified Calendar Structure
6. ✅ Strict Mode Filtering
7. ✅ Execution Ready Promotion

**Result:** 7/7 tests passed - Step 10 is production-ready

---

## Best Practices

### 1. Match Thresholds to Underlying Liquidity

**Liquid Stocks (AAPL, TSLA, SPY):**
```python
min_liquidity_score=40.0  # Can afford to be selective
max_spread_pct=5.0
```

**Mid-Cap / Less Liquid:**
```python
min_liquidity_score=25.0  # More lenient
max_spread_pct=10.0
```

### 2. Use Strict Mode for Live Trading

```python
step10_df = recalibrate_and_filter(df, strict_mode=True)
```
- Reduces execution risk
- Filters marginal contracts
- Recommended for automated execution

### 3. Review Watch Status Before Discarding

Watch contracts may be viable with manual review:
- Check actual market conditions (live quotes)
- Verify OI on both legs individually
- Consider time of day (spreads narrow at open/close)

### 4. Log PCS Scores for Analysis

```python
# Track score distribution
import matplotlib.pyplot as plt

plt.hist(step10_df['PCS_Score'], bins=20)
plt.axvline(x=70, color='g', label='High Quality')
plt.axvline(x=50, color='y', label='Moderate')
plt.axvline(x=30, color='r', label='Low Quality')
plt.legend()
plt.show()
```

### 5. Adjust for Market Conditions

**Volatile Markets (VIX > 25):**
- Increase `max_spread_pct` (spreads naturally widen)
- Maintain or increase `min_liquidity_score`

**Low Volatility (VIX < 15):**
- Can decrease `max_spread_pct` (tighter spreads expected)
- Consider increasing `min_dte` (less urgency)

---

## Troubleshooting

### Issue: All contracts rejected

**Possible Causes:**
- Thresholds too strict for underlying liquidity
- Step 9B filtering too aggressive
- Data quality issues

**Solutions:**
```python
# Check rejection reasons
rejected = step10_df[step10_df['Pre_Filter_Status'] == 'Rejected']
print(rejected['Filter_Reason'].value_counts())

# Relax thresholds incrementally
step10_df = recalibrate_and_filter(
    df,
    min_liquidity_score=20.0,  # Lower from 30.0
    max_spread_pct=12.0         # Raise from 8.0
)
```

### Issue: Low PCS scores even for Valid contracts

**Possible Causes:**
- DTE too short (lowers DTE component)
- Strategy-specific penalties accumulating
- Liquidity scores from Step 9B low

**Solutions:**
- Review Step 9B liquidity calculation
- Check if OI requirements in strategy-specific validation are appropriate
- Consider underlying's typical option liquidity

### Issue: Too many Watch statuses

**Interpretation:** Not necessarily a problem - Watch means "review recommended"

**Actions:**
- Review `Filter_Reason` column for patterns
- Manually approve Watch contracts that meet your criteria
- Adjust thresholds if Watch reasons are acceptable

---

## Future Enhancements

Potential improvements for future versions:

1. **Greeks Validation:** Incorporate Delta/Gamma/Vega thresholds
2. **IV Rank/Percentile:** Filter by implied volatility environment
3. **Time-of-Day Adjustments:** Dynamic spread thresholds based on market hours
4. **Backtesting Integration:** Track PCS scores vs. actual P&L
5. **Machine Learning:** Learn optimal thresholds from historical execution data
6. **Multi-Leg OI Validation:** Check OI separately for each strike (not just total)

---

## References

**Related Documentation:**
- [STEP9B_FINAL_FIXES.md](STEP9B_FINAL_FIXES.md) - Step 9B architecture
- [DASHBOARD_README.md](DASHBOARD_README.md) - Full pipeline overview
- [SCAN_GUIDE.md](SCAN_GUIDE.md) - Usage guide

**Code Files:**
- [step10_pcs_recalibration.py](core/scan_engine/step10_pcs_recalibration.py)
- [test_step10.py](test_step10.py)
- [pipeline.py](core/scan_engine/pipeline.py)

**Version:** 1.0.0  
**Last Updated:** 2025-01-XX  
**Status:** Production Ready ✅
