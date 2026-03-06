# Hybrid IV Architecture: Schwab + Fidelity Integration

**Date:** 2026-02-03
**Status:** ✅ IMPLEMENTED

---

## Design Principles

### 1. Schwab IV is Authoritative Daily Time Series

- **Source:** Schwab API constant-maturity IV (7D, 14D, 30D, 60D, 90D, 120D, 180D, 360D)
- **Storage:** `iv_history.duckdb` → `iv_term_history` table
- **Cadence:** One row per unique trading day (PRIMARY KEY: ticker, date)
- **Protection:** `ON CONFLICT UPDATE` prevents duplicate intraday inserts
- **Invariant:** 252 days = 252 unique trading dates (verified in calculate_iv_rank)

### 2. Fidelity IV is Structural Reference Layer

- **Source:** Fidelity IV surface snapshots (scrape-based)
- **Storage:** `pipeline.duckdb` → `fidelity_iv_long_term_history` table
- **Purpose:**
  - Bootstrap new tickers (before Schwab history accumulates)
  - Validate IV extremes (sanity check)
  - Dormant once Schwab reaches maturity (≥252 days)
- **Cadence:** Event-driven, not time-driven (triggered by IMMATURE state + missing data)

### 3. IV Maturity States

| State | Definition | Days of Schwab History | Behavior |
|-------|-----------|------------------------|----------|
| **MATURE** | Reliable percentile ranking | 252+ days (1 year) | Use Schwab IV Rank only |
| **IMMATURE** | Partial history | 120-251 days | Weighted blend: Schwab + Fidelity |
| **MISSING** | No history | 0-119 days | REJECTED (insufficient data) |

**Threshold Rationale:**
- Hull Ch.15: Minimum 120 days (0.5 year) for reliable percentiles
- 252 days = 1 trading year (industry standard, Natenberg Ch.8)

---

## Weighted IV Rank Algorithm (IMMATURE Phase)

### Formula

```python
fidelity_weight = (252 - schwab_days) / 252
schwab_weight = 1.0 - fidelity_weight

weighted_iv_rank = (schwab_weight × schwab_rank) + (fidelity_weight × fidelity_rank)
```

### Example Weights

| Schwab Days | Schwab Weight | Fidelity Weight | Interpretation |
|-------------|---------------|-----------------|----------------|
| 0 | 0% | 100% | Pure bootstrap (no Schwab history) |
| 63 | 25% | 75% | Mostly Fidelity reference |
| 126 | 50% | 50% | Equal blend |
| 189 | 75% | 25% | Mostly Schwab history |
| 252+ | 100% | 0% | MATURE (Fidelity dormant) |

### Fidelity Reference Percentile

Computed from `fidelity_iv_long_term_history.IV_30_D_Call` historical snapshots:

```python
percentile = (current_iv - min_fidelity_iv) / (max_fidelity_iv - min_fidelity_iv) × 100
```

**Fallback:** If no Fidelity data, use Schwab only (even if IMMATURE).

---

## Implementation Details

### Step 2: IV Rank Enrichment (`_enrich_iv_rank_from_duckdb`)

```python
# 1. Get Schwab IV Rank + history depth
iv_rank, history_depth = calculate_iv_rank(con_iv, ticker, current_iv, lookback_days=252)

# 2. Determine maturity state
maturity_state, reason = get_iv_maturity_state(con_iv, ticker, current_iv, maturity_threshold=120)

# 3. Apply weighted blending for IMMATURE tickers
if maturity_state == 'IMMATURE' and con_fidelity is not None:
    weighted_rank, blend_source = calculate_weighted_iv_rank(
        con_iv, con_fidelity, ticker, current_iv, schwab_history_days=history_depth
    )
    iv_rank = weighted_rank
    iv_rank_source = blend_source  # e.g., "Blended (75% Schwab 189d, 25% Fidelity)"
```

### Invariant Checks

#### 1. Unique Trading Days Invariant

**Location:** `calculate_iv_rank()` in `iv_term_history.py`

```python
unique_dates = df_history['date'].nunique()
total_rows = len(df_history)

if unique_dates != total_rows:
    logger.warning(
        f"⚠️ INVARIANT VIOLATION: {ticker} has {total_rows} rows but only "
        f"{unique_dates} unique dates. Duplicate intraday inserts detected!"
    )
    history_depth = unique_dates  # Use unique count as authoritative
```

**Purpose:** Prevents multiple intraday scan runs from inflating history depth.

#### 2. PRIMARY KEY Enforcement

**Schema:**
```sql
CREATE TABLE iv_term_history (
    ticker VARCHAR NOT NULL,
    date DATE NOT NULL,
    ...
    PRIMARY KEY (ticker, date)
)
```

**Insert Logic:**
```sql
INSERT INTO iv_term_history (...)
SELECT * FROM df_insert
ON CONFLICT (ticker, date) DO UPDATE SET
    iv_7d = EXCLUDED.iv_7d,
    iv_14d = EXCLUDED.iv_14d,
    ...
```

**Result:** Multiple runs on same trading day → UPDATE, not INSERT (prevents duplicates).

---

## Event-Driven Fidelity Trigger

### Trigger Conditions (R1-R5 Rules)

| Rule | Strategy Type | IV State | Fidelity Needed? | Reason |
|------|--------------|----------|------------------|--------|
| R0.2 | Any | Illiquid contract | ❌ NO | Structural rejection (pre-gate) |
| R1 | INCOME | IMMATURE/MISSING | ✅ YES | Premium selling requires percentile (Natenberg Ch.4) |
| R2 | DIRECTIONAL | MATURE Schwab IV | ❌ NO | Schwab sufficient for directional thesis |
| R3 | DIRECTIONAL | IMMATURE/MISSING | ❌ NO | Reject (insufficient data) |
| R4 | LEAP | MATURE with term structure | ❌ NO | IV_360D available from Schwab (Passarelli Ch.8) |
| R4 | LEAP | IMMATURE/MISSING | ✅ YES | LEAP needs validation |
| R5 | VOLATILITY/UNKNOWN | Any | ❌ NO | Strategy type doesn't require Fidelity |

**Key Insight:** Fidelity is NOT time-driven (daily scrape). It's event-driven:
- Triggered ONLY when INCOME/LEAP strategies encounter IMMATURE/MISSING IV
- Once Schwab history reaches MATURE (252+ days), Fidelity becomes dormant

---

## Logging and Diagnostics

### Step 2 Output

```
✅ IV Rank enrichment: 204 from DuckDB, 0 fallback
🔀 Hybrid IV Architecture: 42 IMMATURE tickers used Schwab+Fidelity weighted blending
📊 IV history merge verification: 204/204 tickers have iv_history_days data
📊 IV Maturity: {'MATURE': 22, 'IMMATURE': 42, 'MISSING': 140}
```

### Step 12 Diagnostic

```
📊 [DIAGNOSTIC] IV metadata check before acceptance filtering:
   IV_Maturity_State distribution: {'MATURE': 22, 'IMMATURE': 42, 'MISSING': 140}
   iv_history_days: 64/204 strategies have data
   Average iv_history_days: 78 days
   IV_Rank_30D: 64/204 strategies have data
```

### Fidelity Trigger Logs

```
[FIDELITY_TRIGGER] AAPL Cash-Secured Put: R1 - INCOME with IMMATURE IV requires Fidelity validation
[FIDELITY_SKIP] MSFT Long Call: R2 - DIRECTIONAL with MATURE DuckDB IV (252+ days sufficient)
```

---

## Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│ STEP 0: Schwab Snapshot                                         │
│ ─────────────────────────                                       │
│ • Fetch current IV surface (IV_7D, IV_30D, IV_360D, etc.)      │
│ • Append to iv_term_history (one row per trading day)          │
│ • PRIMARY KEY prevents duplicate intraday inserts               │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 2: IV Rank Enrichment (Hybrid Architecture)                │
│ ──────────────────────────────────────────────────               │
│ FOR each ticker:                                                 │
│   1. Query iv_term_history → get Schwab rank + history_depth   │
│   2. Determine maturity: MATURE / IMMATURE / MISSING            │
│   3. IF IMMATURE:                                               │
│        - Calculate weighted blend (Schwab + Fidelity)           │
│        - Weight decays: 100% Fidelity @ 0d → 0% @ 252d         │
│   4. IF MATURE:                                                 │
│        - Use Schwab only (Fidelity dormant)                     │
│   5. IF MISSING:                                                │
│        - IV_Rank = 0.0, iv_history_days = 0                    │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 7: Strategy Generation                                     │
│ ────────────────────────                                        │
│ • All Step 2 metadata propagates (IV_Maturity_State, etc.)     │
│ • Multi-strategy ledger preserves full row context             │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 9B: Contract Selection                                     │
│ ─────────────────────────                                       │
│ • IV metadata still intact (row.to_dict() preserves all cols)  │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ STEP 12: Acceptance Gate                                        │
│ ──────────────────────────                                      │
│ • Check IV_Maturity_State for INCOME/LEAP strategies           │
│ • MATURE → READY (if liquidity OK)                             │
│ • IMMATURE → Weighted IV Rank used (blended percentile)        │
│ • MISSING → REJECTED (insufficient data)                        │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ WAIT LOOP: Fidelity Trigger (Event-Driven)                      │
│ ─────────────────────────────────────────                       │
│ IF strategy is AWAIT_CONFIRMATION AND IV_Maturity = IMMATURE:  │
│   • Trigger Fidelity scrape (bootstrap structural reference)    │
│   • Update fidelity_iv_long_term_history                        │
│   • Re-evaluate with updated weighted blend                     │
└─────────────────────────────────────────────────────────────────┘
```

---

## File Changes

| File | Purpose | Key Changes |
|------|---------|-------------|
| `iv_term_history.py` | IV calculation | Added `calculate_weighted_iv_rank()`, `_get_fidelity_reference_percentile()`, unique_trading_days invariant check |
| `step2_load_snapshot.py` | IV enrichment | Integrated weighted blending for IMMATURE tickers, dual DB connections (Schwab + Fidelity) |
| `step12_acceptance.py` | Acceptance gate | Added pre-acceptance IV metadata diagnostic (Patch 3) |
| `fidelity_trigger.py` | Event trigger | R1-R5 rules enforce event-driven scraping (not time-driven) |

---

## Verification Checklist

- [x] **Audit:** iv_term_history has no duplicate (ticker, date) entries
- [x] **Invariant:** calculate_iv_rank checks unique_dates == total_rows
- [x] **Weighted Blend:** IMMATURE tickers use Schwab+Fidelity percentile blend
- [x] **Weight Decay:** 100% Fidelity @ 0d → 0% Fidelity @ 252d (linear)
- [x] **Fidelity Dormant:** MATURE tickers (252+ days) use Schwab only
- [x] **Logging:** Step 2 reports weighted blend count
- [x] **Propagation:** IV_Maturity_State + iv_history_days survive all steps
- [x] **Event-Driven:** Fidelity trigger only fires for IMMATURE INCOME/LEAP strategies

---

## Next Steps

1. **Bootstrap Remaining Tickers** (Optional)
   - Identify high-priority tickers without Schwab history
   - One-time Fidelity scrape to establish structural reference
   - Let Schwab daily accumulation take over (event-driven only thereafter)

2. **Monitor Weighted Blend Usage**
   - Track how many IMMATURE tickers benefit from blending
   - Verify weights decay correctly as Schwab history grows
   - Confirm Fidelity becomes dormant at 252+ days

3. **Daily IV Collection**
   - Ensure Step 0 appends to iv_term_history daily
   - Verify PRIMARY KEY prevents duplicate intraday inserts
   - Monitor history depth growth (0 → 120 → 252 days)

---

**Status:** ✅ COMPLETE

Hybrid IV architecture implemented. Schwab is authoritative, Fidelity is structural reference. Weighted blending bridges IMMATURE phase. Event-driven triggers replace time-driven scraping.
