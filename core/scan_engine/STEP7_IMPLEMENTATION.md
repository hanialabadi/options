# Step 7: Strategy Recommendation Engine

## Overview

Step 7 is the **first prescriptive step** in the scan pipeline. All prior steps (2-6) were purely descriptive, observing market conditions without making trade recommendations.

## Design Philosophy

### Clear Separation of Concerns

**DESCRIPTIVE (Steps 2-6):** What IS happening
- Step 2: Load and enrich IV/HV data
- Step 3: Filter and classify volatility patterns  
- Step 5: Compute chart indicators and regimes
- Step 6: Validate data completeness

**PRESCRIPTIVE (Step 7+):** What TO DO about it
- Step 7: Select strategies and score confidence
- Future steps: Position sizing, risk management, order generation

### Why This Matters

1. **Reusability:** Steps 2-6 can be used with ANY strategy framework
2. **Testability:** Test strategy logic independently from data processing
3. **Clarity:** Clear boundary between observation and action
4. **Flexibility:** Swap strategy engines without changing data pipeline

## Step 7 Implementation

### File Location
```
core/scan_engine/step7_strategy_recommendation.py
```

### Main Function
```python
def recommend_strategies(
    df: pd.DataFrame,
    min_iv_rank: float = 50.0,
    min_ivhv_gap: float = 3.5,
    enable_directional: bool = True,
    enable_neutral: bool = True,
    enable_volatility: bool = True
) -> pd.DataFrame
```

### Input (from Step 6)
Required columns:
- `Ticker`, `IVHV_gap_30D`, `IV_Rank_30D`
- `Signal_Type`, `Regime`, `Crossover_Age_Bucket`
- `Data_Complete`, `IV_Rich`, `IV_Cheap`
- `MeanReversion_Setup`, `Expansion_Setup`

### Output Columns Added
- `Primary_Strategy`: Main recommended strategy
- `Secondary_Strategy`: Alternative strategy
- `Strategy_Type`: 'Directional', 'Neutral', 'Volatility', 'Mixed'
- `Confidence`: 0-100 score (higher = stronger setup)
- `Success_Probability`: Estimated probability of profit (0-1)
- `Trade_Bias`: 'Bullish', 'Bearish', 'Neutral', 'Bidirectional'
- `Entry_Priority`: 'High', 'Medium', 'Low'
- `Risk_Level`: 'Low', 'Medium', 'High'

## Strategy Selection Logic

### 1. High IV Strategies (Premium Selling)
**Condition:** IV_Rank >= 70 AND IV_Rich

**Directional:**
- Bullish signal → Put Credit Spread / Naked Put
- Bearish signal → Call Credit Spread / Bear Put Spread

**Neutral:**
- Base/Neutral signal → Iron Condor / Short Strangle

**Volatility:**
- MeanReversion_Setup → Calendar Spread

### 2. Low IV Strategies (Premium Buying)
**Condition:** IV_Rank <= 30 AND IV_Cheap

**Directional:**
- Bullish signal → Call Debit Spread / Long Call
- Bearish signal → Put Debit Spread / Long Put

**Volatility:**
- Expansion_Setup → Long Straddle / Long Strangle

### 3. Moderate IV Strategies
**Condition:** 50 <= IV_Rank < 70

**Directional:**
- Bullish signal → Diagonal Spread (Bullish)
- Bearish signal → Diagonal Spread (Bearish)

## Confidence Scoring

Confidence ranges from 0-100 based on signal alignment:

- **High (70-100):**
  - Multiple aligned signals
  - Strong IV edge (IV_Rank >= 80 or <= 20)
  - Fresh crossover (Age_0_5)
  - Clear regime (Trending or Ranging)

- **Medium (55-69):**
  - Some alignment
  - Moderate IV edge
  - Aging signal (Age_6_15)

- **Low (0-54):**
  - Weak alignment
  - Marginal IV edge
  - Stale signal (Age_16_plus)

## Success Probability Estimation

Base probability = Confidence / 100

**Adjustments:**
- Neutral strategies: +10% (higher base POP)
- High IV extremes (>80): +5% (mean reversion potential)
- Low IV extremes (<20): -5% (expansion risk)
- Recent crossovers: +5% (signal freshness)
- Extended crossovers: -5% (signal staleness)

**Constraints:** Capped between 30% and 85% (no guarantees)

## Usage Examples

### Basic Usage
```python
from core.scan_engine import recommend_strategies

# df6 from Step 6 (validated data)
df7 = recommend_strategies(df6)

# View high-confidence recommendations
high_conf = df7[df7['Confidence'] >= 70]
print(high_conf[['Ticker', 'Primary_Strategy', 'Confidence', 'Success_Probability']])
```

### Custom Parameters
```python
# Only directional strategies, high IV rank threshold
df7 = recommend_strategies(
    df6,
    min_iv_rank=60.0,
    enable_directional=True,
    enable_neutral=False,
    enable_volatility=False
)
```

### Full Pipeline
```python
from core.scan_engine import run_full_scan_pipeline

# Run Steps 2-7 (includes strategy recommendations)
results = run_full_scan_pipeline(include_step7=True)
recommendations = results['recommendations']

# Export to CSV
recommendations.to_csv('step7_recommendations.csv', index=False)
```

### Descriptive Only (No Strategies)
```python
# Run Steps 2-6 only (no strategy logic)
results = run_full_scan_pipeline(include_step7=False)
validated_data = results['validated_data']

# Use validated data for custom strategy engine
my_custom_strategies = my_strategy_engine(validated_data)
```

## Test Results

Using mock data for 10 tickers:

```
Strategy Distribution:
- Mixed: 3 (Premium selling + volatility plays)
- Volatility: 3 (Long straddles for expansion)
- Directional: 1 (Diagonal spread)
- Neutral: 1 (Iron condor)
- None: 2 (No clear setup)

Trade Bias:
- Bullish: 4
- Bidirectional: 3
- Neutral: 3

Entry Priority:
- High: 4 (confidence >= 70)
- Medium: 4 (confidence 55-69)
- Low: 2 (confidence < 55)

Average Success Probability: 63.72%
```

## Integration with Existing rec_engine_v6

Step 7 is designed to be:
1. **Simpler** - Clear logic, easy to understand
2. **Modular** - Fits cleanly into scan_engine architecture
3. **Independent** - Doesn't require rec_engine_v6 machinery

**Migration Path:**
- Use Step 7 for new scan_engine pipelines
- Keep rec_engine_v6 for existing workflows
- Optionally merge best features from both

## Future Enhancements

### Step 8: Position Sizing
- Kelly Criterion
- Fixed fractional
- Volatility-based sizing

### Step 9: Risk Management
- Portfolio heat limits
- Correlation checks
- Max loss per trade

### Step 10: Order Generation
- Strike selection
- Expiration selection
- Order types (limit, stop, etc.)

## Summary

Step 7 marks the transition from **observation to action**:
- Steps 2-6: Describe market state (neutral)
- Step 7+: Prescribe trade actions (strategy-specific)

This separation keeps the codebase:
- **Maintainable:** Easy to understand where logic lives
- **Testable:** Test strategies independent of data processing
- **Flexible:** Swap strategy engines without breaking data pipeline
- **Professional:** Clear separation of concerns

---

**Next Steps:**
1. Test Step 7 with live data pipeline
2. Backtest strategy recommendations
3. Implement Steps 8-10 (position sizing, risk management, orders)
4. Build dashboard UI for strategy recommendations
