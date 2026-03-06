# Phase 1 Integration Guide: Entry Quality, Premium Pricing, Market Context

**Date:** 2026-02-03
**Status:** Ready for Integration
**Priority:** High (Critical Execution Readiness)

---

## Quick Integration

### Step 5.5: Entry Quality Validation

**Where to integrate:** Between Step 5 (chart signals) and Step 7 (strategy recommendation)

**In your pipeline file:**
```python
# After Step 5 (chart signals)
from scan_engine.step5_5_entry_quality import validate_entry_quality, filter_quality_entries

# Validate entry quality
df_step5 = validate_entry_quality(df_step5)

# Filter out chasing entries (optional but recommended)
df_step5_quality = filter_quality_entries(
    df_step5,
    min_quality_score=65.0,  # GOOD or better
    allow_fair=False          # Don't allow FAIR (wait for pullback)
)

# Continue with Step 7 using filtered data
df_step7 = recommend_strategies(df_step5_quality)
```

**Required Input Columns:**
- `intraday_change_pct`: Today's price change %
- `last_price`: Current stock price
- `ma_50` or `MA_50`: 50-day moving average
- `volume` or `Volume`: Current volume
- `avg_volume` or `Average_Volume`: Average volume
- `Signal_Strength`: Chart signal strength
- `trend_direction`: Trend direction (optional)
- `momentum_direction`: Momentum direction (optional)

**Output Columns Added:**
- `Entry_Quality`: EXCELLENT | GOOD | FAIR | CHASING
- `Entry_Quality_Score`: 0-100
- `Entry_Flags`: Quality issues
- `Entry_Recommendation`: ENTER_NOW | WAIT_PULLBACK | AVOID

---

### Step 10: Premium Pricing Gate (Already Integrated)

**No code changes required!** Premium pricing penalties are automatically applied in `calculate_pcs_score_v2()`.

**How it works:**
- Directional strategies: Penalizes overpaying (>5% premium vs fair value)
- Income strategies: Penalizes underselling (<-5% vs fair value)
- IV Rank alignment: Don't buy high IV, don't sell low IV
- Theta burn check: Don't buy if losing >5% per day

**Required Input Columns (already present in Step 10):**
- `Premium_vs_FairValue_Pct`: From Black-Scholes calculation
- `IV_Rank_30D` or `IV_Rank`: IV percentile
- `Theta`: Theta value
- `Mid`: Mid price
- `Strategy`: Strategy name

**Output:**
- Penalties automatically included in `PCS_Score_V2`
- Shows in `PCS_Penalties` column (e.g., "Overpaying +12% vs fair value, -21 pts")

---

### Step 12.5: Market Context Validation

**Where to integrate:** In Step 12 (Execution Gate), after all other validation

**In your Step 12 acceptance logic:**
```python
from scan_engine.step12_5_market_context import validate_market_context, filter_favorable_context

# After Step 12 acceptance logic
df_step12 = validate_market_context(df_step12)

# Filter unfavorable market conditions (optional but recommended)
df_step12_ready = filter_favorable_context(
    df_step12,
    allow_neutral=True  # Allow NEUTRAL context (only block UNFAVORABLE)
)

# Final READY candidates
df_ready = df_step12_ready[df_step12_ready['Execution_Status'] == 'READY']
```

**Required Input Columns:**
- `Ticker`: Stock symbol
- `Strategy_Type`: Directional | Income | Volatility

**Market Data (auto-fetched from market_stress_detector):**
- VIX
- SPY change %
- Sector data (optional)

**Output Columns Added:**
- `Market_Context`: FAVORABLE | NEUTRAL | UNFAVORABLE
- `Market_Flags`: Market condition issues
- `Market_Proceed`: True/False

---

## Full Pipeline Integration Example

```python
def run_pipeline_with_phase1(df_snapshot):
    """
    Complete pipeline with Phase 1 execution readiness gates.
    """

    # Step 0-2: Snapshot + IV Rank (existing)
    df = load_snapshot_with_iv_rank(df_snapshot)

    # Step 3-5: IVHV filter + Chart signals (existing)
    df = filter_ivhv(df)
    df = generate_chart_signals(df)

    # ========================================
    # PHASE 1: Step 5.5 - Entry Quality
    # ========================================
    from scan_engine.step5_5_entry_quality import validate_entry_quality, filter_quality_entries

    df = validate_entry_quality(df)
    df = filter_quality_entries(df, min_quality_score=65.0, allow_fair=False)

    logger.info(f"✅ After Entry Quality Filter: {len(df)} tickers (no chasing)")

    # Step 7-9: Strategy recommendation + Contract selection (existing)
    df = recommend_strategies(df)
    df = determine_dte_windows(df)
    df = fetch_contracts(df)

    # ========================================
    # PHASE 1: Step 10 - Premium Pricing (Auto)
    # ========================================
    # Premium pricing penalties already integrated in calculate_pcs_score_v2()
    df = recalibrate_and_filter(df)  # Includes premium pricing validation

    logger.info(f"✅ After PCS Recalibration (with pricing): {len(df)} contracts")

    # Step 11: Independent evaluation (existing)
    df = evaluate_strategies_independently(df)

    # Step 12: Execution gate (existing)
    df = apply_acceptance_logic(df)

    # ========================================
    # PHASE 1: Step 12.5 - Market Context
    # ========================================
    from scan_engine.step12_5_market_context import validate_market_context, filter_favorable_context

    df = validate_market_context(df)
    df = filter_favorable_context(df, allow_neutral=True)

    logger.info(f"✅ After Market Context Filter: {len(df)} candidates (favorable conditions)")

    # Final READY candidates
    df_ready = df[df['Execution_Status'] == 'READY']

    logger.info(f"🎯 Final READY: {len(df_ready)} high-conviction candidates")

    return df_ready
```

---

## Expected Impact

### Before Phase 1
```
Step 5 (Chart Signals): 100 tickers
  ↓
Step 10 (PCS): 13 contracts (2.2% acceptance)
  ↓
Step 12 (Acceptance): 13 READY

Issues:
❌ Some may be chasing (extended +6% today)
❌ Some may be overpaying (premium vs FV +15%)
❌ Some during market stress (SPY down -3%)
```

### After Phase 1
```
Step 5 (Chart Signals): 100 tickers
  ↓
✅ Step 5.5 (Entry Quality): 65 tickers (35 removed as chasing)
  ↓
Step 10 (PCS with Pricing): 25 contracts (4.3% acceptance)
  ↓
Step 12 (Acceptance): 25 candidates
  ↓
✅ Step 12.5 (Market Context): 20 READY (5 removed due to unfavorable market)

Result:
✅ No chasing entries (patient timing)
✅ Good pricing edge (buy cheap, sell expensive)
✅ Favorable market conditions (VIX regime, no stress)
✅ Higher conviction (75+ average conviction score)
```

---

## Monitoring & Validation

### Key Metrics to Track

**Entry Quality:**
```python
from scan_engine.step5_5_entry_quality import get_entry_quality_metrics

metrics = get_entry_quality_metrics(df_after_step5_5)
print(f"Chasing Rate: {metrics['chasing_count']/metrics['total_tickers']*100:.1f}%")
print(f"Quality Entries: {metrics['quality_entries_pct']:.1f}%")
```

**Premium Pricing (in PCS output):**
```python
# Check PCS_Penalties column for pricing issues
pricing_issues = df[df['PCS_Penalties'].str.contains('Overpaying|Underselling', na=False)]
print(f"Pricing Issues: {len(pricing_issues)} contracts")
```

**Market Context:**
```python
from scan_engine.step12_5_market_context import get_market_context_metrics

metrics = get_market_context_metrics(df_after_step12_5)
print(f"Blocked by Market: {metrics['blocked_count']}")
print(f"Proceed Rate: {metrics['proceed_pct']:.1f}%")
```

---

## Rollback Plan

If Phase 1 gates prove too restrictive:

### Option 1: Disable Individual Gates

**Disable Entry Quality:**
```python
# Skip step5_5_entry_quality.py entirely
# df = validate_entry_quality(df)  # Comment out
```

**Disable Premium Pricing:**
```python
# In utils/pcs_scoring_v2.py, comment out:
# pricing_penalty, pricing_reasons = _calculate_premium_pricing_penalties(row)
# base_score -= pricing_penalty
```

**Disable Market Context:**
```python
# Skip step12_5_market_context.py entirely
# df = validate_market_context(df)  # Comment out
```

### Option 2: Relax Thresholds

**Entry Quality (more permissive):**
```python
df = filter_quality_entries(df, min_quality_score=50.0, allow_fair=True)
# Was: 65.0 (GOOD+), Now: 50.0 (FAIR+)
```

**Market Context (allow unfavorable):**
```python
# Don't filter at all, just log warnings
df = validate_market_context(df)  # Adds columns but doesn't filter
# Skip: filter_favorable_context(df)
```

---

## Success Criteria

### Phase 1 Implementation Success

- [x] Entry quality validation implemented ✅
- [x] Premium pricing penalties integrated ✅
- [x] Market context gates implemented ✅
- [ ] Pipeline integration tested with real data
- [ ] Acceptance rate 4-5% (quality over quantity)
- [ ] Chasing rate <10% (disciplined entries)
- [ ] Win rate >55% (measured over 2 weeks)

### Production Readiness

- [ ] Run full pipeline with Phase 1 gates
- [ ] Compare before/after acceptance rates
- [ ] Validate entry quality metrics (chasing eliminated?)
- [ ] Check premium pricing impact (overpaying eliminated?)
- [ ] Monitor market context blocks (sensible during stress?)
- [ ] Track win rate for 2 weeks
- [ ] Adjust thresholds if needed

---

## Next Steps

1. **Test with real data:** Run pipeline with latest snapshot
2. **Monitor metrics:** Track entry quality, pricing, market context
3. **Validate impact:** Compare acceptance rates, win rates
4. **Fine-tune:** Adjust thresholds based on results
5. **Phase 2:** Implement strike quality scoring and unified conviction score

---

**Created by:** Claude (Implementation - Phase 1)
**Date:** 2026-02-03
**Status:** Ready for Integration Testing
