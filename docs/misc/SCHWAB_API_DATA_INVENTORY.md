# Schwab Trader API - Market & Options Data Inventory

**Purpose**: Enumerate all fields available from Schwab Trader API for options scan engine
**Scope**: Pre-trade discovery only (NOT trade management)
**Date**: January 2, 2026

---

## 1. API ENDPOINTS OVERVIEW

The scan engine currently uses **3 primary Schwab API endpoints**:

### 1.1 `/marketdata/v1/quotes` (Batch Quotes)
- **Purpose**: Real-time/delayed quotes for underlying equities
- **Rate Limit**: 100 symbols per request (batched)
- **Current Usage**: Step 0 snapshot (price + volume)
- **Latency**: ~1-2 seconds per 100 symbols

### 1.2 `/marketdata/v1/pricehistory` (Historical Candles)
- **Purpose**: OHLCV data for volatility computation
- **Rate Limit**: 1 request per ticker
- **Current Usage**: Step 0 snapshot (HV calculation)
- **Latency**: ~500ms per ticker (with caching)

### 1.3 `/marketdata/v1/chains` (Option Chains)
- **Purpose**: Full option chain with Greeks, pricing, OI, volume
- **Rate Limit**: ~2 requests/second (self-throttled)
- **Current Usage**: Step 9B contract selection
- **Latency**: ~500ms per chain

---

## 2. QUOTE ENDPOINT FIELDS (Underlying / Equity)

**Endpoint**: `/marketdata/v1/quotes?symbols=AAPL,MSFT&fields=quote`

**Response Structure**: 
```json
{
  "AAPL": {
    "quote": { /* fields below */ },
    "reference": { /* symbol metadata */ }
  }
}
```

### 2.1 Quote Block Fields (Currently Used ✅ / Unused ⚠️)

#### **Pricing Fields**
| Field | Type | Description | Status | Notes |
|-------|------|-------------|--------|-------|
| `lastPrice` | float | Last trade price | ✅ USED | Primary price source (market hours) |
| `mark` | float | Mid-point of bid/ask | ✅ USED | Preferred after-hours fallback |
| `bidPrice` | float | Current bid | ✅ USED | Liquidity check + mid calculation |
| `askPrice` | float | Current ask | ✅ USED | Liquidity check + mid calculation |
| `closePrice` | float | Previous close | ✅ USED | Fallback when live data stale |
| `openPrice` | float | Today's open | ⚠️ UNUSED | Could detect gap moves |
| `highPrice` | float | Day high | ⚠️ UNUSED | Intraday range analysis |
| `lowPrice` | float | Day low | ⚠️ UNUSED | Intraday range analysis |
| `regularMarketLastPrice` | float | Last regular market price | ✅ USED | Final fallback for price |

#### **Volume & Activity**
| Field | Type | Description | Status | Notes |
|-------|------|-------------|--------|-------|
| `totalVolume` | int | Total daily volume | ✅ USED | Liquidity proxy in Step 2 |
| `bidSize` | int | Shares at bid | ⚠️ UNUSED | Level 1 depth |
| `askSize` | int | Shares at ask | ⚠️ UNUSED | Level 1 depth |
| `volatility` | float | Historical volatility (Schwab's calc) | ⚠️ UNUSED | **May be unreliable** |

#### **Price Changes**
| Field | Type | Description | Status | Notes |
|-------|------|-------------|--------|-------|
| `netChange` | float | $ change from close | ⚠️ UNUSED | Momentum indicator |
| `netPercentChange` | float | % change from close | ⚠️ UNUSED | Relative strength |

#### **52-Week Range**
| Field | Type | Description | Status | Notes |
|-------|------|-------------|--------|-------|
| `52WeekHigh` | float | 52-week high price | ⚠️ UNUSED | Mean reversion context |
| `52WeekLow` | float | 52-week low price | ⚠️ UNUSED | Breakout detection |

#### **Timestamps**
| Field | Type | Description | Status | Notes |
|-------|------|-------------|--------|-------|
| `quoteTime` | int (ms) | Quote update timestamp | ✅ USED | Staleness check |
| `tradeTime` | int (ms) | Last trade timestamp | ✅ USED | Trade recency |

#### **Dividends & Fundamentals**
| Field | Type | Description | Status | Notes |
|-------|------|-------------|--------|-------|
| `dividendAmount` | float | Annual dividend | ⚠️ UNUSED | Yield context for covered calls |
| `dividendYield` | float | Dividend yield % | ⚠️ UNUSED | Income strategy filtering |
| `dividendDate` | string | Ex-dividend date | ⚠️ UNUSED | Avoid assignment risk |
| `peRatio` | float | Price-to-earnings | ⚠️ UNUSED | Valuation context |

#### **Market Status**
| Field | Type | Description | Status | Notes |
|-------|------|-------------|--------|-------|
| `securityStatus` | string | Trading status | ⚠️ UNUSED | Halt detection |
| `tradeable` | bool | Can be traded | ⚠️ UNUSED | Eligibility check |
| `quoteTimeInLong` | int | Quote time (ms) | ⚠️ UNUSED | Duplicate of quoteTime |

### 2.2 Reference Block Fields (Symbol Metadata)

**Not currently parsed**, but available in response under `AAPL.reference`:
- `symbol`: Ticker symbol
- `description`: Company name
- `exchange`: Primary exchange (NYSE, NASDAQ, etc.)
- `exchangeName`: Full exchange name
- `cusip`: CUSIP identifier
- `htbQuantity`: Hard-to-borrow quantity (for short selling)
- `htbRate`: Hard-to-borrow rate

---

## 3. PRICE HISTORY ENDPOINT FIELDS (OHLCV)

**Endpoint**: `/marketdata/v1/pricehistory?symbol=AAPL&periodType=year&frequencyType=daily`

**Response Structure**:
```json
{
  "candles": [
    {
      "datetime": 1672531200000,  // ms timestamp
      "open": 130.28,
      "high": 130.90,
      "low": 124.17,
      "close": 125.07,
      "volume": 112117471
    }
  ],
  "symbol": "AAPL",
  "empty": false
}
```

### 3.1 Candle Fields

| Field | Type | Description | Status | Notes |
|-------|------|-------------|--------|-------|
| `datetime` | int (ms) | Candle timestamp | ✅ USED | Date alignment |
| `open` | float | Open price | ⚠️ UNUSED | Gap detection |
| `high` | float | High price | ⚠️ UNUSED | Volatility proxy |
| `low` | float | Low price | ⚠️ UNUSED | Volatility proxy |
| `close` | float | Close price | ✅ USED | HV calculation (log returns) |
| `volume` | int | Volume | ⚠️ UNUSED | Volume spike detection |

### 3.2 Current HV Calculation

**Currently Used**: Only `close` prices for log-return HV calculation across multiple windows:
- HV_10D, HV_20D, HV_30D, HV_60D, HV_90D, HV_120D, HV_150D, HV_180D

**Formula**: 
```python
log_returns = np.log(close[t] / close[t-1])
std_dev = log_returns.std()
hv = std_dev * sqrt(252) * 100  # Annualized %
```

**Potential Enhancement**:
- Parkinson volatility: Uses high/low (more efficient estimator)
- Garman-Klass: Uses OHLC (even more efficient)
- Yang-Zhang: OHLC with overnight gaps

---

## 4. OPTION CHAIN ENDPOINT FIELDS (Per Contract)

**Endpoint**: `/marketdata/v1/chains?symbol=AAPL&strikeCount=30&range=NTM&includeQuotes=TRUE`

**Response Structure**:
```json
{
  "symbol": "AAPL",
  "underlyingPrice": 178.23,
  "callExpDateMap": {
    "2025-02-14:10": {
      "180.0": [
        { /* contract fields below */ }
      ]
    }
  },
  "putExpDateMap": { /* same structure */ }
}
```

### 4.1 Contract-Level Fields (Currently Used ✅ / Unused ⚠️)

#### **Greeks (PRIMARY DATA SOURCE)**
| Field | Type | Description | Status | Reliability |
|-------|------|-------------|--------|-------------|
| `delta` | float | Delta (0-1 for calls, -1-0 for puts) | ✅ USED | **Excellent** |
| `gamma` | float | Gamma (convexity) | ✅ USED | **Excellent** |
| `vega` | float | Vega (IV sensitivity) | ✅ USED | **Excellent** |
| `theta` | float | Theta (time decay) | ✅ USED | **Excellent** |
| `rho` | float | Rho (interest rate sensitivity) | ✅ USED | **Good** (rarely used) |

**NOTE**: All Greeks are **contract-level** and **accurate**. There is NO underlying-level IV/IVR/IVP from Schwab.

#### **Implied Volatility (Contract-Specific)**
| Field | Type | Description | Status | Reliability |
|-------|------|-------------|--------|-------------|
| `volatility` | float | Implied volatility (decimal, e.g., 0.35 = 35%) | ✅ USED | **Excellent** |

**CRITICAL**: `volatility` is **per-option-contract only**. There is NO separate `underlyingIV` field. Step 0 computes **proxy underlying IV** by averaging ATM call+put IVs (30-45 DTE).

#### **Pricing (Execution Quality)**
| Field | Type | Description | Status | Notes |
|-------|------|-------------|--------|-------|
| `bid` | float | Bid price | ✅ USED | Liquidity + spread calc |
| `ask` | float | Ask price | ✅ USED | Liquidity + spread calc |
| `last` | float | Last trade price | ✅ USED | Execution reference |
| `mark` | float | Mid-point (bid+ask)/2 | ✅ USED | Fair value |
| `bidSize` | int | Contracts at bid | ⚠️ UNUSED | Depth analysis |
| `askSize` | int | Contracts at ask | ⚠️ UNUSED | Depth analysis |

#### **Liquidity Metrics**
| Field | Type | Description | Status | Scan Usage |
|-------|------|-------------|--------|-----------|
| `openInterest` | int | Total open contracts | ✅ USED | Primary liquidity filter |
| `totalVolume` | int | Daily volume | ✅ USED | Activity confirmation |

**Current Thresholds** (from [step9b_fetch_contracts_schwab.py](step9b_fetch_contracts_schwab.py#L95-L108)):
- **Excellent**: OI ≥ 500, spread < 3%
- **Good**: OI ≥ 100, spread < 5%
- **Acceptable**: OI ≥ 25, spread < 10% (market hours) / 20% (off-hours)
- **Thin**: OI ≥ 10
- **Illiquid**: OI < 10

#### **Contract Identifiers**
| Field | Type | Description | Status | Notes |
|-------|------|-------------|--------|-------|
| `symbol` | string | OCC symbol (e.g., "AAPL250214C180") | ✅ USED | Trade execution |
| `strikePrice` | float | Strike price | ✅ USED | Contract selection |
| `expirationDate` | string | YYYY-MM-DD format | ✅ USED | DTE calculation |
| `daysToExpiration` | int | Calendar days (NOT trading days) | ⚠️ UNUSED | DTE recalculated locally |
| `putCall` | string | "CALL" or "PUT" | ✅ USED | Type filtering |

#### **Intrinsic/Extrinsic (Calculated Fields)**
| Field | Type | Description | Status | Notes |
|-------|------|-------------|--------|-------|
| `intrinsicValue` | float | Max(0, S-K) for calls / Max(0, K-S) for puts | ⚠️ UNUSED | Moneyness calculation |
| `extrinsicValue` | float | mark - intrinsicValue | ⚠️ UNUSED | Time value premium |
| `inTheMoney` | bool | ITM flag | ⚠️ UNUSED | Moneyness filter |

#### **Spread Metrics (Theoretical)**
| Field | Type | Description | Status | Notes |
|-------|------|-------------|--------|-------|
| `theoreticalOptionValue` | float | Black-Scholes fair value | ⚠️ UNUSED | Mispricing detection |
| `theoreticalVolatility` | float | Model-implied IV | ⚠️ UNUSED | Usually matches `volatility` |
| `percentChange` | float | % change from previous close | ⚠️ UNUSED | Momentum tracking |

#### **Delivery & Settlement**
| Field | Type | Description | Status | Notes |
|-------|------|-------------|--------|-------|
| `deliverables` | string | Non-standard deliverables | ⚠️ UNUSED | Corporate action tracking |
| `settlementType` | string | "P" (PM cash) / "A" (AM cash) | ⚠️ UNUSED | Index option settlement |
| `multiplier` | float | Contract multiplier (usually 100) | ⚠️ UNUSED | Position sizing |

#### **Timestamps**
| Field | Type | Description | Status | Notes |
|-------|------|-------------|--------|-------|
| `quoteTimeInLong` | int (ms) | Quote update time | ⚠️ UNUSED | Staleness check |
| `tradeTimeInLong` | int (ms) | Last trade time | ⚠️ UNUSED | Activity recency |

### 4.2 Chain-Level Metadata

| Field | Type | Description | Status | Notes |
|-------|------|-------------|--------|-------|
| `symbol` | string | Underlying ticker | ✅ USED | Validation |
| `underlyingPrice` | float | Current underlying price | ⚠️ UNUSED | Already from quotes endpoint |
| `volatility` | float | Underlying IV (30-day) | ❌ **UNRELIABLE** | Often missing/stale |
| `interestRate` | float | Risk-free rate | ⚠️ UNUSED | Greeks recalculation |
| `isDelayed` | bool | Whether data is delayed | ⚠️ UNUSED | Real-time verification |

**CRITICAL FINDING**: The chain-level `volatility` field (underlying IV) is **NOT consistently populated** by Schwab. This is why Step 0 computes a **proxy IV** from ATM options.

---

## 5. DATA RELIABILITY ASSESSMENT

### 5.1 Fields with Known Issues

| Field | Endpoint | Issue | Workaround |
|-------|----------|-------|-----------|
| `volatility` (underlying) | chains | Often missing/stale | ✅ Step 0 ATM proxy |
| `daysToExpiration` | chains | Calendar days (not trading days) | ✅ Recalculated locally |
| `volatility` (quote) | quotes | Schwab's HV calc (opaque) | ✅ Local HV calculation |
| `dividendDate` | quotes | Sometimes missing | ⚠️ Not critical for scan |

### 5.2 Market Hours Considerations

**During Market Hours** (9:30 AM - 4:00 PM ET):
- `lastPrice` is **live** (preferred)
- `bid`/`ask` spreads are **tight**
- `volume` is **accumulating**
- Greeks update **every few minutes**

**After Market Hours**:
- `lastPrice` may be **stale** (use `mark` or `closePrice`)
- Spreads **widen significantly** (relax thresholds)
- `volume` is **frozen** (wait until next day)
- Greeks are **stale** but structure-valid

**Current Handling**: [step9b_fetch_contracts_schwab.py](step9b_fetch_contracts_schwab.py#L106-L108) adjusts liquidity thresholds:
```python
# Market hours: OI ≥ 25, spread < 10%
# Off hours:    OI ≥ 10, spread < 20%
```

---

## 6. CURRENTLY USED vs UNUSED DATA

### 6.1 Fully Utilized Fields ✅

**From Quotes Endpoint**:
- `lastPrice`, `mark`, `bidPrice`, `askPrice`, `closePrice` → Price fallback cascade
- `totalVolume` → Liquidity proxy
- `quoteTime`, `tradeTime` → Staleness validation

**From Price History Endpoint**:
- `close` → HV calculation (8 windows: 10D-180D)
- `datetime` → Date alignment

**From Option Chains Endpoint**:
- `delta`, `gamma`, `vega`, `theta`, `rho` → Greeks for all strategies
- `volatility` (per-contract) → IV tracking
- `bid`, `ask`, `last`, `mark` → Pricing + spread quality
- `openInterest`, `totalVolume` → Liquidity grading
- `symbol`, `strikePrice`, `expirationDate`, `putCall` → Contract identity

### 6.2 Unused but Potentially Valuable Fields ⚠️

#### **High Priority** (Scan-Relevant)

1. **`openPrice`, `highPrice`, `lowPrice`** (quotes)
   - **Use Case**: Detect gap moves, intraday range compression
   - **Scan Logic**: 
     - Gap > 2% → Momentum signal
     - (high-low)/close < 1% → Compression → Breakout setup
   - **Implementation**: Add to Step 2 (load snapshot) as derived fields

2. **`52WeekHigh`, `52WeekLow`** (quotes)
   - **Use Case**: Mean reversion context, breakout detection
   - **Scan Logic**:
     - Price within 2% of 52W high → Momentum strategy
     - Price within 10% of 52W low → Contrarian setup
   - **Implementation**: Add to Step 5 (chart signals) or Step 6 (GEM validation)

3. **`netChange`, `netPercentChange`** (quotes)
   - **Use Case**: Daily momentum, relative strength
   - **Scan Logic**: 
     - netChange > 2σ → Outlier move (volatility spike expected)
     - Rank tickers by % change → Focus on movers
   - **Implementation**: Step 3 (IVHV gap filtering) or Step 5 (chart)

4. **`dividendDate`, `dividendYield`** (quotes)
   - **Use Case**: Avoid assignment risk for short options
   - **Scan Logic**: 
     - Ex-div within DTE window → Flag "High Assignment Risk"
     - Yield > 2% → Favor covered calls over CSPs
   - **Implementation**: Step 7 (strategy recommendation) or Step 11 (evaluation)

5. **`bidSize`, `askSize`** (chains)
   - **Use Case**: Depth beyond OI/volume (execution quality)
   - **Scan Logic**: 
     - bidSize + askSize > 50 contracts → "Deep book"
     - bidSize/askSize ratio > 3 → Imbalanced (avoid)
   - **Implementation**: Step 9B (liquidity grading enhancement)

6. **High/Low from Price History** (pricehistory)
   - **Use Case**: Parkinson volatility (more efficient than close-only HV)
   - **Formula**: `σ_P = sqrt( (1/(4ln2)) * (ln(high/low))^2 )`
   - **Benefit**: 5x more efficient than close-only estimator
   - **Implementation**: Add to Step 0 HV calculation

#### **Medium Priority** (Context-Aware)

7. **`securityStatus`, `tradeable`** (quotes)
   - **Use Case**: Exclude halted/untradeable tickers
   - **Scan Logic**: Filter out before Step 3
   - **Implementation**: Step 2 (load snapshot) validation

8. **`intrinsicValue`, `extrinsicValue`** (chains)
   - **Use Case**: Time premium analysis
   - **Scan Logic**: 
     - extrinsic/intrinsic ratio → "Premium efficiency"
     - High extrinsic → Good theta harvesting candidate
   - **Implementation**: Step 11 (strategy evaluation)

9. **`theoreticalOptionValue`** (chains)
   - **Use Case**: Detect mispriced options
   - **Scan Logic**: |mark - theoretical| / mark > 5% → "Mispriced"
   - **Implementation**: Step 9B (contract selection) as edge signal

10. **`peRatio`** (quotes)
    - **Use Case**: Valuation context (growth vs value)
    - **Scan Logic**: PE > 30 → Growth (volatility expansion likely)
    - **Implementation**: Step 6 (GEM validation) as regime filter

#### **Low Priority** (Nice-to-Have)

11. **`multiplier`, `deliverables`** (chains)
    - **Use Case**: Non-standard options (mini, adjusted)
    - **Scan Logic**: Flag if multiplier ≠ 100
    - **Implementation**: Step 9B contract validation

12. **`settlementType`** (chains)
    - **Use Case**: Index option settlement risk
    - **Scan Logic**: "A" (AM) → Flag early exercise risk
    - **Implementation**: Step 11 (strategy evaluation)

13. **`quoteTimeInLong`, `tradeTimeInLong`** (chains)
    - **Use Case**: Detect stale option quotes
    - **Scan Logic**: (now - quoteTime) > 5 min → "Stale"
    - **Implementation**: Step 9B (contract selection) quality check

---

## 7. FIELDS NOT PROVIDED BY SCHWAB (Confirmed Limitations)

### 7.1 Missing Volatility Metrics

**Schwab does NOT provide**:
- ✗ Underlying ATM implied volatility (IV) - **Must compute from ATM options**
- ✗ IV Rank (IVR) - Current IV percentile vs 52-week range
- ✗ IV Percentile (IVP) - % of days IV was below current level
- ✗ Term structure (IV skew across expirations)
- ✗ Skew (IV difference between OTM puts and calls)

**Current Workaround**:
- IV (30D): Step 0 averages ATM call+put IVs at 30-45 DTE
- IVR/IVP: Could compute locally from historical IV snapshots (not implemented)
- Skew: Could compute from Step 9B chain data (puts vs calls IV delta)

### 7.2 Missing Fundamental Data

**Schwab does NOT provide** (would need 3rd-party API):
- ✗ Earnings dates (only `dividendDate`)
- ✗ Analyst ratings
- ✗ Institutional ownership
- ✗ Short interest
- ✗ Revenue/EPS estimates

**NOTE**: `peRatio` and `dividendYield` ARE available (see Section 6.2)

### 7.3 Missing Technical Indicators

**Schwab does NOT provide**:
- ✗ Moving averages (SMA, EMA)
- ✗ RSI, MACD, Bollinger Bands
- ✗ Support/resistance levels

**Current Workaround**: Step 5 (chart signals) could compute these from price history

---

## 8. RECOMMENDED ENHANCEMENTS (Scan-Only)

### 8.1 Immediate Value (Low Effort, High Impact)

**Priority 1: Intraday Range Detection**
```python
# Add to Step 2 (load_snapshot) or Step 5 (chart_signals)
def detect_compression(quote):
    daily_range = (quote['highPrice'] - quote['lowPrice']) / quote['lastPrice']
    if daily_range < 0.01:  # < 1% range
        return "COMPRESSION"  # Breakout setup
    elif daily_range > 0.05:  # > 5% range
        return "EXPANSION"  # Already moving
    return "NORMAL"
```

**Priority 2: 52-Week Position**
```python
# Add to Step 6 (GEM validation) or Step 11 (evaluation)
def assess_52w_position(quote):
    price = quote['lastPrice']
    high52w = quote['52WeekHigh']
    low52w = quote['52WeekLow']
    
    pct_off_high = (high52w - price) / high52w
    pct_off_low = (price - low52w) / low52w
    
    if pct_off_high < 0.02:
        return "NEAR_HIGH"  # Momentum
    elif pct_off_low < 0.10:
        return "NEAR_LOW"  # Contrarian
    else:
        return "NEUTRAL"
```

**Priority 3: Dividend Risk Flag**
```python
# Add to Step 11 (strategy_evaluation) for short options
def check_dividend_risk(quote, dte):
    div_date = quote.get('dividendDate')
    if not div_date:
        return "NO_DATA"
    
    days_to_dividend = (pd.to_datetime(div_date) - pd.Timestamp.now()).days
    
    if 0 < days_to_dividend < dte:
        return "HIGH_RISK"  # Ex-div within option window
    else:
        return "LOW_RISK"
```

### 8.2 Medium-Term (Moderate Effort)

**Priority 4: Enhanced HV Calculation** (Parkinson estimator)
```python
# Add to Step 0 (schwab_snapshot) HV calculation
def calculate_parkinson_hv(df, window):
    """More efficient volatility estimator using high/low"""
    df['HL_ratio'] = np.log(df['high'] / df['low'])
    df['HL_squared'] = df['HL_ratio'] ** 2
    
    variance = df['HL_squared'].rolling(window).mean() / (4 * np.log(2))
    hv = np.sqrt(variance * 252) * 100  # Annualized
    
    return hv
```

**Priority 5: Bid/Ask Depth Analysis**
```python
# Add to Step 9B (liquidity grading)
def grade_depth(bidSize, askSize, oi):
    total_depth = bidSize + askSize
    imbalance = abs(bidSize - askSize) / total_depth if total_depth > 0 else 1.0
    
    if total_depth > 50 and imbalance < 0.3:
        return "DEEP_BALANCED"
    elif total_depth > 20:
        return "ADEQUATE"
    else:
        return "THIN"
```

**Priority 6: IV Skew Detection** (from chain data)
```python
# Add to Step 9B or new Step 9C (chain analysis)
def calculate_iv_skew(chain_df, underlying_price):
    """Measure put/call IV differential (volatility smile)"""
    # OTM puts (delta ~ -0.25)
    otm_puts = chain_df[
        (chain_df['putCall'] == 'PUT') & 
        (chain_df['delta'].between(-0.35, -0.15))
    ]
    
    # OTM calls (delta ~ 0.25)
    otm_calls = chain_df[
        (chain_df['putCall'] == 'CALL') & 
        (chain_df['delta'].between(0.15, 0.35))
    ]
    
    put_iv_avg = otm_puts['volatility'].mean() * 100
    call_iv_avg = otm_calls['volatility'].mean() * 100
    
    skew = put_iv_avg - call_iv_avg  # Positive = puts expensive (fear)
    
    return {
        'skew': skew,
        'put_iv': put_iv_avg,
        'call_iv': call_iv_avg,
        'interpretation': 'FEAR' if skew > 5 else 'GREED' if skew < -5 else 'NEUTRAL'
    }
```

### 8.3 Long-Term (Research Phase)

**Priority 7: Local IV Rank/Percentile**
- Requires: Historical IV snapshot storage (daily cron job)
- Implementation: Compute 52-week IVR from stored snapshots
- Use Case: Filter for high-IV environments (premium selling)

**Priority 8: Theoretical Pricing Arbitrage**
- Requires: Black-Scholes calculator
- Implementation: Compare `mark` vs `theoreticalOptionValue`
- Use Case: Identify mispriced options (edge signal)

**Priority 9: Chart Pattern Detection**
- Requires: Price history + TA library (pandas-ta)
- Implementation: Step 5 enhancement (already has chart_signals stub)
- Use Case: Filter for technical setups (e.g., bull flags, support bounces)

---

## 9. DATA QUALITY & LIMITATIONS SUMMARY

### 9.1 Reliable Fields (Production-Ready)

**Excellent Quality** (use with confidence):
- Option Greeks (delta, gamma, vega, theta) - **Real-time, accurate**
- Option pricing (bid, ask, mark, last) - **Level 1 depth**
- Liquidity metrics (OI, volume) - **Accurate during market hours**
- Contract identifiers (symbol, strike, expiration) - **Always accurate**
- Underlying price (lastPrice with fallbacks) - **Robust cascade**

**Good Quality** (minor caveats):
- Historical OHLCV - **Accurate but requires caching**
- Quote timestamps - **Useful for staleness detection**
- Dividend data - **Sometimes missing, not critical**

### 9.2 Unreliable/Missing Fields (Avoid)

**Do NOT use**:
- `volatility` (underlying, from chains) - **Often missing/stale**
- `daysToExpiration` - **Calendar days, not trading days**
- `volatility` (quote endpoint) - **Schwab's opaque HV calc**

**Not provided**:
- IV Rank (IVR), IV Percentile (IVP)
- Earnings dates (only dividends)
- Term structure, skew (must compute)
- Technical indicators (must compute)

### 9.3 Market Hours Impact

| Metric | Market Hours | After Hours |
|--------|--------------|-------------|
| Underlying price | ✅ `lastPrice` preferred | ⚠️ Use `mark` or `closePrice` |
| Option spreads | ✅ Tight (< 5%) | ⚠️ Wide (10-20%) |
| OI/Volume | ✅ Live updates | ❌ Frozen until next day |
| Greeks | ✅ Updated every ~2 min | ⚠️ Stale but structure-valid |

**Current Handling**: Step 9B adjusts thresholds based on market status ([code](step9b_fetch_contracts_schwab.py#L106-L108))

---

## 10. CONCLUSIONS & RECOMMENDATIONS

### 10.1 Current Data Consumption (Efficient)

The scan engine is **well-architected** for Schwab API limitations:
- ✅ Computes missing underlying IV from ATM options (Step 0)
- ✅ Calculates local HV from price history (Step 0)
- ✅ Uses robust price fallback cascade (Step 0)
- ✅ Adjusts liquidity thresholds for market hours (Step 9B)
- ✅ Focuses on **contract-level** Greeks (always reliable)

### 10.2 Untapped Data Sources (Low-Hanging Fruit)

**Immediate value** (already in API responses, not yet consumed):
1. Intraday range (`highPrice`, `lowPrice`) → Compression/expansion signals
2. 52-week position (`52WeekHigh`, `52WeekLow`) → Momentum/contrarian context
3. Daily momentum (`netChange`, `netPercentChange`) → Relative strength
4. Dividend dates (`dividendDate`) → Assignment risk for short options
5. Bid/ask depth (`bidSize`, `askSize`) → Execution quality

**Moderate effort** (requires new calculations):
6. Parkinson HV (uses high/low) → More efficient volatility estimator
7. IV skew (put vs call IVs) → Fear/greed gauge
8. Theoretical pricing delta → Mispricing detection

### 10.3 Fields Schwab Will NEVER Provide

**Must accept or compute locally**:
- IV Rank, IV Percentile (requires historical IV storage)
- Earnings dates (need 3rd-party API)
- Technical indicators (compute from price history)
- Term structure (compute from multiple expirations)

### 10.4 Final Assessment

**Question**: Can we extract additional useful scan-time data without violating boundaries?

**Answer**: **YES** - but mostly through **local computation** rather than new API fields.

**Recommendations**:
1. **Immediate** (1-2 hours): Add intraday range + 52W position to Step 2/5
2. **Short-term** (1 day): Implement Parkinson HV + dividend risk checks
3. **Medium-term** (1 week): Add IV skew calculation to Step 9B
4. **Long-term** (1 month): Build historical IV storage for IVR/IVP

**Non-Recommendations** (out of scan scope):
- ✗ Earnings data (would require 3rd-party API like Alpha Vantage)
- ✗ News/sentiment (trade management, not scan-time)
- ✗ Portfolio-level Greeks (position tracking, not discovery)

---

## APPENDIX A: Field Extraction Examples

### Example 1: Quote with All Fields
```python
# From /marketdata/v1/quotes?symbols=AAPL
quote = {
    'lastPrice': 178.23,
    'mark': 178.25,
    'bidPrice': 178.20,
    'askPrice': 178.30,
    'closePrice': 177.50,
    'openPrice': 177.80,
    'highPrice': 179.00,
    'lowPrice': 177.20,
    'totalVolume': 52419000,
    'bidSize': 300,
    'askSize': 400,
    'netChange': 0.73,
    'netPercentChange': 0.41,
    '52WeekHigh': 199.62,
    '52WeekLow': 164.08,
    'quoteTime': 1704225600000,
    'tradeTime': 1704225595000,
    'dividendAmount': 0.96,
    'dividendYield': 2.15,
    'dividendDate': '2025-02-14',
    'peRatio': 29.34,
    'volatility': 0.35,  # Schwab's HV (opaque calc)
}
```

### Example 2: Option Contract with All Fields
```python
# From /marketdata/v1/chains?symbol=AAPL
contract = {
    'symbol': 'AAPL250214C180',
    'strikePrice': 180.0,
    'expirationDate': '2025-02-14',
    'daysToExpiration': 42,
    'putCall': 'CALL',
    'delta': 0.52,
    'gamma': 0.03,
    'vega': 0.08,
    'theta': -0.05,
    'rho': 0.02,
    'volatility': 0.32,  # IV for THIS contract
    'bid': 3.80,
    'ask': 3.90,
    'last': 3.85,
    'mark': 3.85,
    'bidSize': 125,
    'askSize': 200,
    'openInterest': 5420,
    'totalVolume': 1250,
    'intrinsicValue': 0.00,  # OTM
    'extrinsicValue': 3.85,  # All time value
    'inTheMoney': False,
    'theoreticalOptionValue': 3.82,
    'theoreticalVolatility': 0.32,
    'percentChange': 2.67,
    'quoteTimeInLong': 1704225600000,
    'tradeTimeInLong': 1704225580000,
    'multiplier': 100,
    'settlementType': 'P',
}
```

---

**Document Version**: 1.0  
**Last Updated**: January 2, 2026  
**Maintained By**: Scan Engine Architecture Team
