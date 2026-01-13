# PHASE 1-4 SCHEMA QUICK REFERENCE

**Updated:** January 4, 2026  
**Purpose:** Quick lookup for new observable columns

---

## 📊 NEW COLUMNS (15 total)

### Phase 3 Observables (11 columns)

#### Time Observable
```python
DTE: int
    Days to expiration
    Formula: (Expiration - Snapshot_TS).days
    Range: -999 to 365+ days
    Negative = expired position
```

#### Volatility Observable
```python
IV_Rank: float
    IV percentile rank (0-100)
    0 = lowest historical IV (compression)
    100 = highest historical IV (spike)
    50 = neutral (STUB default)
    ⚠️ Currently uses within-snapshot percentile (STUB)
```

#### Event Observable
```python
Days_to_Earnings: int
    Days until next earnings announcement
    Positive = days until earnings
    Negative = days since earnings
    999 = unknown (STUB default)
    ⚠️ Currently returns 999 for all (STUB)
```

#### Capital Observable
```python
Capital_Deployed: float
    Total capital at risk (USD)
    Source priority:
      1. Broker "Margin Required" (most accurate)
      2. Broker "Buying Power Effect"
      3. Estimated from strategy (conservative)
    Always positive (absolute value)
```

#### Trade-Level Aggregates (5 columns)
```python
Delta_Trade: float
    Net delta across all legs in trade
    Sum of leg-level Delta values
    Example: Iron Condor might be -0.05 (directionally neutral)

Gamma_Trade: float
    Net gamma across all legs
    Sum of leg-level Gamma values
    Example: 0.02 (convexity exposure)

Theta_Trade: float
    Net theta across all legs
    Sum of leg-level Theta values
    Example: +5.50 (daily decay income)

Vega_Trade: float
    Net vega across all legs
    Sum of leg-level Vega values
    Example: -12.30 (short vol exposure)

Premium_Trade: float
    Total premium across all legs
    Sum of leg-level Premium values
    Example: 450.00 (total time value)
```

---

### Phase 4 Market Context (4 columns)

#### Market Timing
```python
Market_Session: str
    Trading session classification
    Values: "PreMarket" | "Regular" | "AfterHours" | "Closed"
    Hours (ET):
      PreMarket: 4:00 AM - 9:30 AM
      Regular: 9:30 AM - 4:00 PM
      AfterHours: 4:00 PM - 8:00 PM
      Closed: 8:00 PM - 4:00 AM, weekends

Is_Market_Open: bool
    Regular market hours indicator
    True: 9:30 AM - 4:00 PM ET, weekdays
    False: All other times

Snapshot_DayType: str
    Calendar day classification
    Values: "Weekday" | "Weekend" | "Holiday"
    (Holiday detection TBD - currently only Weekday/Weekend)
```

#### Deterministic Entry Tracking
```python
First_Seen_Date: timestamp
    First observation timestamp for TradeID
    Replaces: Entry_Date (removed from Phase 2)
    Deterministic: Same across reprocessing
    Storage: DuckDB trade_first_seen table
    Logic: Timestamp of first snapshot containing TradeID
```

---

## 🗑️ REMOVED COLUMNS (1 total)

### Phase 2 (Removed)
```python
Entry_Date: timestamp ❌ REMOVED
    Previously: pd.Timestamp.now() (non-deterministic)
    Replaced by: First_Seen_Date (Phase 4, deterministic)
    Reason: Broke deterministic replay
```

---

## 📈 USAGE EXAMPLES

### Calculate DTE Decay
```python
# Current DTE
current_dte = df["DTE"]

# DTE at entry (after Phase 6 freeze)
entry_dte = df["DTE_Entry"]  # Phase 6 will add this

# DTE burned
dte_burned = entry_dte - current_dte
```

### Check Trade-Level Net Exposure
```python
# Net delta exposure per trade
net_delta = df.groupby("TradeID")["Delta_Trade"].first()

# Identify directional trades
directional_trades = net_delta[abs(net_delta) > 0.20]

# Identify neutral trades
neutral_trades = net_delta[abs(net_delta) <= 0.10]
```

### Filter by Market Context
```python
# Regular hours snapshots only
regular_hours = df[df["Is_Market_Open"] == True]

# After-hours snapshots
after_hours = df[df["Market_Session"] == "AfterHours"]

# Weekend snapshots
weekends = df[df["Snapshot_DayType"] == "Weekend"]
```

### Track Position Entry
```python
# Days held (using First_Seen_Date)
df["Days_Held"] = (df["Snapshot_TS"] - df["First_Seen_Date"]).dt.days

# New positions (first snapshot)
new_positions = df[df["Snapshot_TS"] == df["First_Seen_Date"]]

# Aged positions (held > 7 days)
aged_positions = df[df["Days_Held"] > 7]
```

### Capital Allocation
```python
# Total capital deployed
total_capital = df["Capital_Deployed"].sum()

# Capital per trade
capital_by_trade = df.groupby("TradeID")["Capital_Deployed"].first()

# Capital concentration (% of total)
df["Capital_Pct"] = df["Capital_Deployed"] / total_capital * 100
```

---

## 🔍 COLUMN AVAILABILITY MATRIX

| Column | Phase 1 | Phase 2 | Phase 3 | Phase 4 | Frozen in Phase 6? |
|--------|---------|---------|---------|---------|-------------------|
| DTE | ❌ | ❌ | ✅ | ✅ | Yes → DTE_Entry |
| IV_Rank | ❌ | ❌ | ✅ | ✅ | Yes → IV_Rank_Entry |
| Days_to_Earnings | ❌ | ❌ | ✅ | ✅ | Yes → Earnings_Proximity_Entry |
| Capital_Deployed | ❌ | ❌ | ✅ | ✅ | Yes → Capital_Deployed_Entry |
| Delta_Trade | ❌ | ❌ | ✅ | ✅ | Yes → Delta_Trade_Entry |
| Gamma_Trade | ❌ | ❌ | ✅ | ✅ | Yes → Gamma_Trade_Entry |
| Theta_Trade | ❌ | ❌ | ✅ | ✅ | Yes → Theta_Trade_Entry |
| Vega_Trade | ❌ | ❌ | ✅ | ✅ | Yes → Vega_Trade_Entry |
| Premium_Trade | ❌ | ❌ | ✅ | ✅ | Yes → Premium_Trade_Entry |
| Market_Session | ❌ | ❌ | ❌ | ✅ | No (metadata) |
| Is_Market_Open | ❌ | ❌ | ❌ | ✅ | No (metadata) |
| Snapshot_DayType | ❌ | ❌ | ❌ | ✅ | No (metadata) |
| First_Seen_Date | ❌ | ❌ | ❌ | ✅ | No (deterministic tracking) |

---

## 🚨 BREAKING CHANGES

### For Existing Code
1. **Entry_Date removed** - Use `First_Seen_Date` (Phase 4) instead
2. **Trade aggregates denormalized** - Each leg has same `*_Trade` value
3. **DTE now explicit** - No need to calculate from Expiration

### Backward Compatibility
- Existing Phase 1-2 code: **No changes required**
- Existing Phase 3 code: **No changes required** (new columns additive)
- Existing Phase 4 code: **Entry_Date removed** (replace with First_Seen_Date)

---

## 📝 DATA TYPES

```python
# Phase 3 observables
DTE: int                    # Calendar days
IV_Rank: float64            # 0-100 scale
Days_to_Earnings: int       # Calendar days (999 = unknown)
Capital_Deployed: float64   # USD
Delta_Trade: float64        # Unitless
Gamma_Trade: float64        # Unitless
Theta_Trade: float64        # USD per day
Vega_Trade: float64         # USD per 1% IV change
Premium_Trade: float64      # USD

# Phase 4 metadata
Market_Session: str         # Categorical
Is_Market_Open: bool        # Boolean
Snapshot_DayType: str       # Categorical
First_Seen_Date: datetime64 # Timestamp
```

---

## 🔗 QUICK LINKS

- Technical Details: [PHASE_1_4_IMPLEMENTATION_SUMMARY.md](PHASE_1_4_IMPLEMENTATION_SUMMARY.md)
- Completion Status: [PHASE_1_4_IMPLEMENTATION_COMPLETE.md](PHASE_1_4_IMPLEMENTATION_COMPLETE.md)
- Validation Test: [test_phase1_4_determinism.py](test_phase1_4_determinism.py)
- Audit Report: [PHASE_1_4_AUDIT_REPORT.md](PHASE_1_4_AUDIT_REPORT.md)

---

**Last Updated:** January 4, 2026  
**Schema Version:** 2.0 (Phase 1-4 Enhanced)
