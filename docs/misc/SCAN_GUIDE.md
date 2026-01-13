# Modular Scan Pipeline - User Guide

## 🎯 Overview

The scan interface is now **fully modular** — execute and validate each step independently before proceeding to the next. This allows you to:

1. **Inspect intermediate outputs** at each stage
2. **Validate logic** before committing to full pipeline
3. **Debug issues** at the specific step where they occur
4. **Adjust parameters** and re-run individual steps

---

## 📂 File Upload Options

### Option 1: Upload CSV
1. Select "Upload CSV" in sidebar
2. Click "Browse files" and select your IV/HV snapshot
3. File is loaded into memory (no file path needed)

### Option 2: Use File Path
1. Select "Use File Path" in sidebar
2. Enter full path to CSV file
3. Uses environment variable `FIDELITY_SNAPSHOT_PATH` as default

---

## 🔄 Step-by-Step Execution

### Step 2: Load IV/HV Snapshot

**What it does:**
- Loads raw Fidelity IV/HV export CSV
- Validates file format and required columns
- No transformations applied

**How to validate:**
1. Click "▶️ Run Step 2"
2. Check metrics: rows and columns count
3. Expand "🔍 Inspect Step 2 Output"
4. Review tabs:
   - **Preview:** First 20 rows
   - **Columns:** Lists all IV/HV columns found
   - **Statistics:** Data types and missing values

**What to look for:**
- ✅ Expected row count matches your export
- ✅ IV_30_D_Call, HV_30_D_Cur columns present
- ✅ No major missing data in key columns

**If it fails:**
- Check file path is correct
- Verify CSV format (not Excel, not corrupted)
- Ensure columns match expected names

---

### Step 3: Filter by IVHV Gap & Add Personas

**What it does:**
- Converts IV/HV to numeric
- Applies liquidity filter (IV >= 15, HV > 0)
- Calculates IVHV_gap_30D (IV minus HV)
- Normalizes IV_Rank_XS (0-100 percentile)
- Filters by min gap threshold (default 2.0)
- Deduplicates (keeps highest gap per ticker)
- Adds persona tags

**How to validate:**
1. Adjust "Min IVHV Gap" slider if needed
2. Click "▶️ Run Step 3"
3. Check metrics:
   - Qualified: How many passed filters
   - HardPass: IVHV >= 5.0 (strongest edge)
   - SoftPass: IVHV 3.5-5.0 (GEM candidates)
   - PSC Pass: IVHV 2.0-3.5 (neutral strategies)
4. Expand "🔍 Inspect Step 3 Output"
5. Review:
   - **Top Candidates:** Sorted by IVHV gap (highest first)
   - **Persona Distribution:** Bar chart of persona counts

**What to look for:**
- ✅ IVHV_gap_30D values make sense (typically 2-10)
- ✅ Personas assigned correctly
- ✅ No unexpected drops (check filter settings if too few)

**If results are empty:**
- Lower min_gap threshold in sidebar
- Check Step 2 data has valid IV/HV values
- Verify liquidity filter isn't too strict

---

### Step 5: Compute Chart Signals & Regime

**What it does:**
- Fetches 90-day price history from yfinance (per ticker)
- Calculates technical indicators:
  - EMA9, EMA21, SMA20, SMA50
  - ATR (14-period Average True Range)
  - Trend slope (5-day EMA9 delta)
- Detects EMA9/EMA21 crossovers
- Classifies market regime (Trending, Ranging, Compressed, etc.)
- Merges chart data back to original DataFrame

**How to validate:**
1. Click "▶️ Run Step 5" (be patient, ~1 sec per ticker)
2. Check metrics:
   - Charted: How many completed successfully
   - Trending: Count in trending regime
   - Compressed: Count in compressed regime (potential breakouts)
   - Bullish: Count with EMA9 > EMA21
3. Expand "🔍 Inspect Step 5 Output"
4. Review:
   - **Chart Signals:** EMA signals, trend slope, ATR
   - **Regime Analysis:** Distribution charts

**What to look for:**
- ✅ Regime assignments make sense (check a few manually)
- ✅ Days_Since_Cross is reasonable (recent crossovers are < 15 days)
- ✅ Atr_Pct > 1.0 for most directional candidates
- ✅ Trend_Slope sign matches EMA_Signal direction

**Performance notes:**
- 50 tickers ≈ 55 seconds
- Rate limited to avoid yfinance throttling
- Skips tickers with insufficient data (< 30 days)

**If it's slow:**
- Normal! Each ticker requires API call
- Consider caching for repeat runs (TODO #3)
- For 100+ tickers, expect 2-3 minutes

---

### Step 6: Filter GEM Candidates

**What it does:**
- Applies final quality gates:
  - Directional: IVHV >= 3.5, extension < 25% from SMAs
  - Neutral: IVHV >= 3.5, valid neutral signals
- Filters allowed signal types (excludes overextended bearish)
- Assigns Scan_Tier based on crossover freshness
- Calculates PCS_Seed (preliminary quality score 68-75)

**How to validate:**
1. Click "▶️ Run Step 6"
2. Check metrics:
   - GEM Total: Final candidate count
   - Tier 1: Fresh crossovers (<=5 days) - highest priority
   - Tier 2: Recent crossovers (6-15 days)
   - Avg PCS: Average seed score
3. Expand "🔍 Inspect Step 6 Output"
4. Review:
   - **Top Candidates:** Sorted by PCS_Seed (best first)
   - **Tier Distribution:** Entry timing breakdown
   - **Export:** Download CSV and review summary JSON

**What to look for:**
- ✅ Tier 1 candidates have fresh crossovers (verify Days_Since_Cross)
- ✅ PCS_Seed scores align with signal quality
- ✅ Regime matches expected patterns (Trending for directional, Ranging for neutral)
- ✅ IVHV gaps are strong (>= 3.5 for all)

**If no candidates:**
- This is valid! Filters are strict by design
- Review Step 5 output — are signals valid?
- Check if tickers are overextended (Price_vs_SMA20 > 25%)
- Consider running on a different day (market conditions vary)

---

## 🔍 Validation Workflow

### Recommended inspection order:

1. **Step 2 → Preview tab**
   - Verify tickers look correct
   - Check IV/HV columns exist

2. **Step 3 → Top Candidates tab**
   - Sort by IVHV_gap_30D
   - Pick 2-3 familiar tickers
   - Manually verify IV-HV gap makes sense

3. **Step 5 → Chart Signals tab**
   - Pick same tickers from Step 3
   - Cross-reference EMA_Signal with a chart (TradingView)
   - Verify Regime classification matches visual

4. **Step 6 → Top Candidates tab**
   - Review Tier 1 tickers
   - Check if they align with your manual research
   - Validate PCS_Seed rankings

---

## 📊 Understanding the Output

### Key Columns Explained:

| Column | Meaning | Good Values |
|--------|---------|-------------|
| `IVHV_gap_30D` | IV minus HV (the edge) | >= 3.5 for GEM |
| `IV_Rank_XS` | Relative IV percentile | 30-70 (not too low/high) |
| `Regime` | Market environment | Trending or Compressed |
| `Signal_Type` | Crossover/structure | "Bullish" or "Base" |
| `EMA_Signal` | Direction | "Bullish" for uptrend |
| `Trend_Slope` | Momentum strength | > 0 for uptrend |
| `Atr_Pct` | Volatility (% of price) | > 1.0 for directional |
| `Days_Since_Cross` | Entry timing | <= 5 (Tier 1), 6-15 (Tier 2) |
| `Scan_Tier` | Entry priority | GEM_Tier_1 (best) |
| `PCS_Seed` | Quality score | 75 (excellent) down to 68 |

### Persona Tags:

- **HardPass (IVHV >= 5.0):** Extreme edge, event-driven, high conviction
- **SoftPass (IVHV 3.5-5.0):** Strong edge, GEM candidates, directional bias
- **PSC_Pass (IVHV 2.0-3.5):** Moderate edge, neutral strategies (straddles, PSC)
- **LowRank (IV < 30):** Low relative IV, proceed with caution

---

## 🐛 Troubleshooting

### "No tickers passed filters"
- **Step 3:** Lower min_gap threshold or check Step 2 data quality
- **Step 6:** Review Step 5 signals — might be weak market environment

### "Import error: core module"
- Ensure you're running from project root
- Dashboard auto-adds parent to sys.path (should work automatically)

### "yfinance timeout or throttle"
- Built-in 0.5s delays should prevent this
- If it persists, reduce ticker count or add caching

### "Missing columns error"
- Step 2: Check your CSV matches Fidelity format
- Step 3+: Re-run previous step (data might not be in session state)

### "Can't run Step X"
- Button is disabled until previous step completes
- Check for error messages in previous step
- Session state persists across reruns

---

## 💾 Export & Next Steps

After Step 6, use the **Export tab**:
1. Download CSV with all GEM candidates
2. Review summary JSON for quick stats
3. Feed into Step 9+ (option chain analysis) — coming soon in dashboard

**Manual workflow:**
- Copy Tier 1 tickers to watchlist
- Research option chains manually
- Use PCS_Seed as tiebreaker for similar setups

---

## 📞 Support & Feedback

**Docstrings:** Each function in `core/scan_pipeline.py` has comprehensive docstrings explaining:
- Purpose and logic flow
- Input/output schemas
- Error handling
- Usage examples

**To review logic:** Open `core/scan_pipeline.py` and read docstrings for:
- `load_ivhv_snapshot()` - Step 2
- `filter_ivhv_gap()` - Step 3
- `compute_chart_signals()` - Step 5
- `filter_gem_candidates()` - Step 6

**Test individual functions:**
```python
from core.scan_pipeline import filter_ivhv_gap
df = pd.read_csv('snapshot.csv')
result = filter_ivhv_gap(df, min_gap=3.5)
print(result.head())
```
