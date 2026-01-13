# PHASE 1-4 OBSERVABLE REALITY AUDIT
**Date:** January 4, 2026  
**Question:** "What does the world look like right now, before any trader thinks or acts?"  
**Scope:** Phase 1-4 market observation completeness

---

## 🎯 PHILOSOPHICAL FRAMEWORK

### The Question

> **"What does the world look like RIGHT NOW, before any trader thinks or acts?"**

This is the foundational question for Phase 1-4. These phases must capture **pure observable reality** — no interpretation, no trader judgment, no historical dependencies (except as explicit context).

### Technical Analysis Principle (Murphy, 1999)

> "Market action discounts everything. The technician believes that anything that can possibly affect the price—fundamentally, politically, psychologically, or otherwise—is actually reflected in the price of that market."

**Implication:** We don't need to capture ALL world events — we capture **price action** (OHLCV), which already reflects everything.

### Current vs Missing Observables

**What we HAVE** (from broker export):
- ✅ Option Greeks (Delta, Gamma, Vega, Theta, Rho)
- ✅ Current Price (Last, Bid, Ask, UL Last)
- ✅ Position P&L ($ Total G/L, Basis)
- ✅ Time Value (extrinsic premium)
- ✅ Implied Volatility (IV Mid)
- ✅ Contract Identity (Strike, Expiration, Symbol)

**What we COMPUTE** (Phase 3 enrichment):
- ✅ DTE (Days to Expiration)
- ✅ Moneyness (% ITM/OTM)
- ✅ IV_Rank (252-day percentile) - **IMPLEMENTED**
- ✅ Earnings Proximity (Days to earnings) - **IMPLEMENTED**
- ✅ Capital Deployed (margin/notional)
- ✅ Trade Aggregates (net position Greeks)

**What we LACK** (GAP analysis):
- ❌ **Price History (OHLCV)** — Underlying's recent price action
- ❌ **Volume Context** — Is volume increasing/decreasing?
- ❌ **Trend Context** — Is UL in uptrend/downtrend/range?
- ❌ **Support/Resistance** — Where are key levels?
- ❌ **Momentum Indicators** — RSI, MACD state
- ❌ **Volatility Surface** — Cross-strike IV skew (live, not historical)

---

## 📊 GAP ANALYSIS: MISSING OBSERVABLES

### 1. Price History (OHLCV Data)

**What it is:**
- Recent candlestick data for the underlying (e.g., last 60-90 days, daily bars)
- Open, High, Low, Close, Volume (OHLCV)

**Why it matters:**
- Cannot assess "overextended" without knowing recent price range
- Cannot detect "support test" without knowing prior price levels
- Cannot contextualize "IV spike" without seeing vol expansion pattern

**Current Status:** ❌ NOT CAPTURED

**Where it belongs:** Phase 1 or early Phase 3 (observable NOW, not computed)

**Data Sources Available:**
1. **Schwab API** (PREFERRED) — `/marketdata/v1/pricehistory` - Already implemented & production-ready
2. **Yahoo Finance** (yfinance) — Free, daily data, 5-year history (fallback)
3. **Tradier API** — Historical bars endpoint (alternative)

**Implementation exists:**
- ✅ **`core/scan_engine/step0_schwab_snapshot.py:fetch_price_history_with_retry()`** — PRODUCTION IMPLEMENTATION
  - Fetches 180 days of OHLCV from Schwab API
  - Retry logic (3 attempts, exponential backoff)
  - File-based caching (24hr TTL)
  - Returns DataFrame with: date, open, high, low, close, volume
- ⚠️ `core/chart_engine.py` uses yfinance but runs in Phase 8 (wrong phase)
- `utils/chart_inspect.py` has `fetch_underlying_data(ticker, period="60d")` (yfinance wrapper)

**Problem:** Scan engine has perfect OHLCV implementation but it's **separate from portfolio pipeline**
**Solution:** Reuse scan engine's `fetch_price_history_with_retry()` in Phase 1-4 pipeline

### 2. Volume Context

**What it is:**
- Current volume vs average volume (Volume Ratio = Today_Vol / Avg_Vol_20)
- On-Balance Volume (OBV) trend
- Volume trend (increasing/decreasing over last N bars)

**Why it matters:**
- High volume + breakout = strong move (Bulkowski: "Heavy breakout volume performance")
- Low volume drift = weak hands, reversible
- Volume spike = institutional interest or news

**Current Status:** ❌ NOT CAPTURED (chart_engine computes it but in Phase 8)

**Where it belongs:** Phase 3 (computed from OHLCV observable)

**Implementation exists:**
- `core/chart_engine.py:compute_volume_overlays()` computes OBV, ATR, Volume_Trend

### 3. Trend Context

**What it is:**
- Simple Moving Averages (SMA20, SMA50, SMA200)
- Exponential Moving Averages (EMA9, EMA21)
- Price position relative to key MAs
- Trend classification: "Uptrend" / "Downtrend" / "Ranging"

**Why it matters:**
- Bullish options strategies work better in uptrends (Murphy: "Trade with the trend")
- Puts underperform in strong bull trends (wasted premium)
- Ranging markets favor iron condors, straddles

**Current Status:** ❌ NOT CAPTURED (chart_engine computes in Phase 8)

**Where it belongs:** Phase 3 (computed from OHLCV observable)

**Implementation exists:**
- `core/chart_engine.py:get_chart_trend_state()` computes EMA9, EMA21, SMA20, SMA50
- Returns: `Chart_Trend` ("Bullish"/"Bearish"/"Neutral"), `Chart_Score` (0-100)

### 4. Support & Resistance Levels

**What it is:**
- Recent swing highs (resistance)
- Recent swing lows (support)
- Pivot points, Fibonacci retracements (optional)

**Why it matters:**
- Strike selection should respect S/R (sell puts at support, calls at resistance)
- Breakout confirmation (price clearing resistance = bullish)
- Stop-loss placement (below support for longs)

**Current Status:** ❌ NOT CAPTURED (chart_engine computes in Phase 8)

**Where it belongs:** Phase 3 (computed from OHLCV observable)

**Implementation exists:**
- `core/chart_engine.py:get_chart_trend_state()` returns `Chart_Support`, `Chart_Resistance`

### 5. Momentum Indicators

**What it is:**
- RSI (Relative Strength Index): 0-100, overbought >70, oversold <30
- MACD (Moving Average Convergence/Divergence): Signal line crossovers
- Bollinger Bands: Price distance from 2σ envelope
- ADX (Average Directional Index): Trend strength

**Why it matters:**
- RSI >70 on high IV = sell premium opportunity (overbought + high premium)
- RSI <30 on low IV = buy options opportunity (oversold + cheap premium)
- MACD bullish crossover = trend confirmation
- Bollinger Band squeeze = volatility breakout imminent

**Current Status:** ❌ NOT CAPTURED (chart_engine computes in Phase 8)

**Where it belongs:** Phase 3 (computed from OHLCV observable)

**Implementation exists:**
- `core/chart_engine.py:compute_momentum_indicators()` computes RSI, MACD, BB, CCI, ADX, MFI

### 6. Implied Volatility Surface (Live)

**What it is:**
- IV skew across strikes (25Δ, ATM, -25Δ)
- IV term structure (30DTE vs 60DTE vs 90DTE)
- IV percentile (current IV vs 1-year range)

**Why it matters:**
- Steep skew = tail risk priced in (prefer credit spreads over naked options)
- Flat skew = balanced risk (straddles/strangles viable)
- High IV percentile = sell premium (vol likely to revert)
- Low IV percentile = buy options (vol likely to expand)

**Current Status:** ⚠️ **PARTIALLY CAPTURED**

**What we have:**
- ❌ IV_Rank implemented (252-day percentile) but **0% coverage** (insufficient history: 5 days vs 120 needed)
- ✅ Skew/Kurtosis calculated BUT requires "IV Mid" column (Fidelity exports lack this, only Schwab scans)

**What we're missing:**
- ❌ Live IV term structure (30/60/90 DTE IV comparison)
- ❌ Cross-strike skew (25Δ put IV vs ATM vs 25Δ call IV)
- ❌ Historical IV (HV 20/30/60) for IV/HV ratio

**Where it belongs:** Phase 3 (observable from option chain)

**Data Sources Available:**
1. **Schwab API** — `/marketdata/v1/chains` includes `volatility` per strike
2. **Tradier API** — Option chain endpoint with greeks/IV

**Implementation gap:** Need to fetch full option chain, not just current position IVs

---

## 🔧 IMPLEMENTATION PLAN

### Priority 1: Price History (OHLCV) — CRITICAL

**Goal:** Add OHLCV candlestick data for each underlying in portfolio

**Location:** Phase 1 (enrich raw broker data with market context)

**Steps:**
1. Extract unique underlyings from position list
2. Fetch last 60-90 days of daily OHLCV data per ticker
3. Store in DataFrame or side table (not denormalized per position)
4. Attach "current snapshot" (latest bar) to position rows

**New Columns (per position):**
- `UL_Close_T0` (underlying close price, most recent bar)
- `UL_Close_T1` (1 day ago)
- `UL_High_5D` (5-day high)
- `UL_Low_5D` (5-day low)
- `UL_Volume_Ratio` (today vol / 20-day avg vol)

**Alternative:** Store OHLCV in separate table, join on-demand (cleaner schema)

**Recommended Source:** **Schwab API** — Already implemented in scan engine, production-ready

**Reuse Pattern (from scan engine):**
```python
# Import from scan engine
from core.scan_engine.step0_schwab_snapshot import fetch_price_history_with_retry
from core.scan_engine.schwab_api_client import SchwabClient

def fetch_underlying_ohlcv(ticker: str, client: SchwabClient):
    """
    Fetch recent price history for underlying.
    Reuses scan engine's production implementation.
    
    Returns:
        DataFrame with columns: date, open, high, low, close, volume
        Or None if fetch fails
    """
    df, status = fetch_price_history_with_retry(client, ticker, use_cache=True)
    if df is not None and status == "OK":
        return df[['date', 'open', 'high', 'low', 'close', 'volume']]
    return None
```

**Benefits vs Yahoo Finance:**
- ✅ Same data source as scan engine (consistency)
- ✅ Production-tested (retry logic, caching, error handling)
- ✅ 24hr cache (faster subsequent runs)
- ✅ 180-day history (vs 60 days initially planned)
- ✅ No external dependency (uses existing Schwab auth)

### Priority 2: Trend & Momentum Indicators — HIGH

**Goal:** Move chart_engine computations from Phase 8 → Phase 3

**Why:** These are **observables NOW** (computed from current OHLCV), not trader decisions

**Refactor Strategy:**
1. Split `chart_engine.py` into:
   - `observable_chart_context.py` (Phase 3: compute SMA, RSI, MACD, S/R from OHLCV)
   - `chart_exit_signals.py` (Phase 8+: decision logic like "Exit if RSI >80")

2. Phase 3 outputs (new columns):
   - `UL_Trend` ("Uptrend"/"Downtrend"/"Range")
   - `UL_RSI` (0-100)
   - `UL_Price_vs_SMA20` (+5.2% = price 5.2% above SMA20)
   - `UL_MACD_Signal` ("Bullish"/"Bearish"/"Neutral")
   - `UL_Support` (nearest support level)
   - `UL_Resistance` (nearest resistance level)

3. Phase 8 uses these observables for exit logic (not computing them fresh)

**Key Principle:**
- Phase 3: "What is RSI right now?" (observation)
- Phase 8+: "Should I exit because RSI >80?" (decision)

### Priority 3: Live IV Surface — MEDIUM

**Goal:** Capture cross-strike IV data from option chain

**Location:** Phase 3 (API call to fetch chain data)

**Challenge:** Broker exports only show IVs for **positions held**, not full chain

**Solution:**
1. Fetch full option chain for each underlying via API
2. Calculate:
   - `IV_Skew_25D` (25Δ put IV - 25Δ call IV)
   - `IV_ATM` (at-the-money IV)
   - `IV_Term_30_60` (30 DTE IV - 60 DTE IV)
3. Attach to position rows as contextual metadata

**New Columns:**
- `IV_Skew_Pct` (skew as % of ATM IV)
- `IV_Rank_Chain` (percentile rank of current IV across chain)
- `IV_vs_HV` (Implied Vol / Historical Vol ratio)

**Data Source:** **Schwab API** (`/marketdata/v1/chains`)

**Alternative:** Skip full chain, use **Tradier or CBOE** for IV index (VIX, VXN, etc.)

---

## 📋 DECISION MATRIX: WHAT TO ADD NOW

| Observable | Priority | Complexity | Data Source | Add to Phase | Impact |
|------------|----------|------------|-------------|--------------|--------|
| **OHLCV History** | 🔴 CRITICAL | Low | Yahoo Finance | Phase 1 | HIGH — Enables all downstream trend/momentum |
| **Trend (SMA/EMA)** | 🔴 HIGH | Low | Computed from OHLCV | Phase 3 | HIGH — Context for strategy viability |
| **RSI/MACD** | 🔴 HIGH | Low | Computed from OHLCV | Phase 3 | HIGH — Overbought/oversold awareness |
| **Volume Ratio** | 🟡 MEDIUM | Low | Computed from OHLCV | Phase 3 | MEDIUM — Conviction signal |
| **Support/Resistance** | 🟡 MEDIUM | Medium | Computed from OHLCV | Phase 3 | MEDIUM — Strike selection context |
| **IV Surface (chain)** | 🟡 MEDIUM | High | Schwab/Tradier API | Phase 3 | MEDIUM — Skew-aware pricing |
| **HV vs IV Ratio** | 🟢 LOW | Medium | Computed from OHLCV+IV | Phase 3 | LOW — Nice-to-have (already have IV_Rank stub) |

### Recommended Immediate Action

**Add in this session:**
1. ✅ **OHLCV History** — Fetch 60-day bars via yfinance in Phase 1
2. ✅ **Trend Indicators** — Move SMA/EMA/RSI/MACD from chart_engine (Phase 8) to Phase 3
3. ⚠️ **Refactor chart_engine** — Split observable computation (Phase 3) from exit signals (Phase 8)

**Defer to next session:**
4. ⏸️ **IV Surface** — Requires option chain API integration (1-2 hours)
5. ⏸️ **Support/Resistance** — Requires swing high/low detection logic (30 min)

---

## 🔍 CURRENT STATE ANALYSIS

### What Phase 1-4 Already Captures

**From Broker Export (Phase 1):**
- Option contract details (Strike, Expiration, Symbol)
- Current Greeks (Delta, Gamma, Vega, Theta, Rho)
- Current Prices (Last, Bid, Ask, UL Last)
- Position P&L ($ Total G/L, Basis, Time Val)
- Implied Volatility (IV Mid) — **Schwab only, not Fidelity**

**Computed in Phase 3 (Currently):**
- DTE (Days to Expiration)
- Moneyness (Strike vs UL Last %)
- IV_Rank (252-day percentile) — **0% coverage** (need 120+ days history)
- Earnings Proximity (Days to next earnings) — **57.9% coverage** (Yahoo Finance)
- Capital Deployed (margin/notional)
- Trade Aggregates (net Delta/Gamma/Vega/Theta)
- PCS Score (position quality snapshot)
- Skew/Kurtosis (cross-leg IV analysis) — **Fails gracefully if IV Mid missing**

**PCS Coverage Diagnostics (Just Added):**
- ✅ PCS_Data_Quality: PARTIAL (50% of optional inputs)
- ✅ PCS_Coverage_Flags: 0x003B (Greeks, Premium, Earnings, DTE, Moneyness present)
- ✅ PCS_Missing_Inputs: IV_Rank, Liquidity, IV Surface
- ✅ PCS_Input_Score: 50%

### What's Missing for "Complete Observable Reality"

**Price Action Context:**
- ❌ Recent OHLCV bars (cannot see "UL rallied 15% in last 5 days")
- ❌ Trend classification (uptrend/downtrend/range)
- ❌ Overextension detection (price vs moving averages)

**Volume Context:**
- ❌ Volume trend (increasing/decreasing)
- ❌ Volume spikes (institutional activity)
- ❌ OBV (on-balance volume) direction

**Momentum Context:**
- ❌ RSI (overbought/oversold)
- ❌ MACD (bullish/bearish crossover)
- ❌ Bollinger Band position (squeeze/expansion)

**Volatility Context:**
- ⚠️ IV_Rank exists but **0% coverage** (insufficient history)
- ❌ IV term structure (30/60/90 DTE comparison)
- ❌ Cross-strike skew (25Δ put vs call IV)
- ❌ Historical Volatility (HV 20/30/60)

---

## 🎯 FINAL RECOMMENDATION

### What to implement NOW (this session):

#### 1. Add OHLCV History to Phase 1
**File:** `core/phase1_clean.py` or new `core/phase1_enrich_market_context.py`

**Strategy:** Reuse scan engine's Schwab price history implementation (DRY principle)

**Function:**
```python
def enrich_with_ohlcv(df: pd.DataFrame, client=None) -> pd.DataFrame:
    """
    Fetch OHLCV history for each underlying and attach current bar snapshot.
    
    REUSES scan engine's production Schwab implementation (no duplicate code).
    
    Returns df with new columns:
    - UL_Close (latest close)
    - UL_High_5D (5-day high)
    - UL_Low_5D (5-day low)
    - UL_Volume_Ratio (today vol / 20-day avg)
    - UL_OHLCV_Available (bool flag)
    """
    from core.scan_engine.step0_schwab_snapshot import fetch_price_history_with_retry
    from core.scan_engine.schwab_api_client import SchwabClient
    
    # Initialize Schwab client (reuse scan engine's auth)
    if client is None:
        client = SchwabClient()
    
    underlyings = df['Underlying'].dropna().unique()
    ohlcv_cache = {}
    
    for ticker in underlyings:
        try:
            # Reuse scan engine's fetch (with caching!)
            hist, status = fetch_price_history_with_retry(client, ticker, use_cache=True)
            
            if hist is not None and len(hist) > 0:
                ohlcv_cache[ticker] = {
                    'Close': hist['close'].iloc[-1],
                    'High_5D': hist['high'].tail(5).max(),
                    'Low_5D': hist['low'].tail(5).min(),
                    'Volume_Ratio': hist['volume'].iloc[-1] / hist['volume'].tail(20).mean()
                }
            else:
                logger.warning(f"No OHLCV data for {ticker} (status: {status})")
                ohlcv_cache[ticker] = None
        except Exception as e:
            logger.warning(f"Failed to fetch OHLCV for {ticker}: {e}")
            ohlcv_cache[ticker] = None
    
    # Attach to positions
    df['UL_Close'] = df['Underlying'].map(lambda t: ohlcv_cache.get(t, {}).get('Close'))
    df['UL_High_5D'] = df['Underlying'].map(lambda t: ohlcv_cache.get(t, {}).get('High_5D'))
    df['UL_Low_5D'] = df['Underlying'].map(lambda t: ohlcv_cache.get(t, {}).get('Low_5D'))
    df['UL_Volume_Ratio'] = df['Underlying'].map(lambda t: ohlcv_cache.get(t, {}).get('Volume_Ratio'))
    df['UL_OHLCV_Available'] = df['UL_Close'].notna()
    
    logger.info(f"📊 OHLCV coverage: {df['UL_OHLCV_Available'].sum()}/{len(df)} positions")
    
    return df
```

#### 2. Add Trend/Momentum Indicators to Phase 3
**File:** `core/phase3_enrich/compute_chart_observables.py` (new)

**Strategy:** Reuse Schwab OHLCV data fetched in Phase 1 (don't re-fetch)

**Function:**
```python
def compute_chart_observables(df: pd.DataFrame, client=None) -> pd.DataFrame:
    """
    Compute trend and momentum indicators from OHLCV data.
    
    Requires: Schwab client for fetching OHLCV (uses scan engine cache)
    
    Returns df with new columns:
    - UL_Trend (Uptrend/Downtrend/Range)
    - UL_RSI (0-100)
    - UL_MACD_Signal (Bullish/Bearish/Neutral)
    - UL_Price_vs_SMA20 (% above/below SMA20)
    - UL_Support (nearest support level)
    - UL_Resistance (nearest resistance level)
    """
    import pandas_ta as ta
    from core.scan_engine.step0_schwab_snapshot import fetch_price_history_with_retry
    from core.scan_engine.schwab_api_client import SchwabClient
    
    if client is None:
        client = SchwabClient()
    
    underlyings = df['Underlying'].dropna().unique()
    chart_cache = {}
    
    for ticker in underlyings:
        try:
            # Reuse Schwab price history (cached from Phase 1 if recent)
            hist, status = fetch_price_history_with_retry(client, ticker, use_cache=True)
            if hist is None or len(hist) < 20:
                continue
                
            # Compute indicators (note: Schwab uses lowercase column names)
            hist['SMA20'] = hist['close'].rolling(20).mean()
            hist['SMA50'] = hist['close'].rolling(50).mean()
            hist['RSI'] = ta.rsi(hist['close'], length=14)
            
            macd = ta.macd(hist['close'])
            hist['MACD'] = macd['MACD_12_26_9']
            hist['MACD_Signal'] = macd['MACDs_12_26_9']
            
            latest = hist.iloc[-1]
            
            # Trend classification
            if pd.notna(latest['SMA20']) and pd.notna(latest['SMA50']):
                if latest['close'] > latest['SMA20'] > latest['SMA50']:
                    trend = "Uptrend"
                elif latest['close'] < latest['SMA20'] < latest['SMA50']:
                    trend = "Downtrend"
                else:
                    trend = "Range"
            else:
                trend = "Unknown"
            
            # MACD signal
            if pd.notna(latest['MACD']) and pd.notna(latest['MACD_Signal']):
                if latest['MACD'] > latest['MACD_Signal']:
                    macd_signal = "Bullish"
                else:
                    macd_signal = "Bearish"
            else:
                macd_signal = "Neutral"
            
            # Support/Resistance (simple: recent swing high/low)
            support = hist['low'].tail(20).min()
            resistance = hist['high'].tail(20).max()
            
            chart_cache[ticker] = {
                'Trend': trend,
                'RSI': latest['RSI'],
                'MACD_Signal': macd_signal,
                'Price_vs_SMA20': ((latest['close'] / latest['SMA20']) - 1) * 100 if pd.notna(latest['SMA20']) else None,
                'Support': support,
                'Resistance': resistance
            }
        except Exception as e:
            logger.warning(f"Failed to compute chart observables for {ticker}: {e}")
            chart_cache[ticker] = None
    
    # Attach to positions
    df['UL_Trend'] = df['Underlying'].map(lambda t: chart_cache.get(t, {}).get('Trend'))
    df['UL_RSI'] = df['Underlying'].map(lambda t: chart_cache.get(t, {}).get('RSI'))
    df['UL_MACD_Signal'] = df['Underlying'].map(lambda t: chart_cache.get(t, {}).get('MACD_Signal'))
    df['UL_Price_vs_SMA20'] = df['Underlying'].map(lambda t: chart_cache.get(t, {}).get('Price_vs_SMA20'))
    df['UL_Support'] = df['Underlying'].map(lambda t: chart_cache.get(t, {}).get('Support'))
    df['UL_Resistance'] = df['Underlying'].map(lambda t: chart_cache.get(t, {}).get('Resistance'))
    
    logger.info(f"📈 Chart observables coverage: {df['UL_Trend'].notna().sum()}/{len(df)} positions")
    
    return df
```

#### 3. Update Phase 3 Orchestrator
**File:** `core/phase3_enrich/sus_compose_pcs_snapshot.py`

**Add after DTE/IV_Rank/Earnings:**
```python
def run_phase3_enrichment(df: pd.DataFrame, snapshot_ts=None) -> pd.DataFrame:
    # ... existing code ...
    
    # Existing observables
    df = compute_dte(df, snapshot_ts=snapshot_ts)
    df = compute_iv_rank(df)
    df = compute_earnings_proximity(df)
    df = compute_capital_deployed(df)
    df = compute_trade_aggregates(df)
    
    # NEW: Chart observables (trend, momentum)
    df = compute_chart_observables(df)  # <-- ADD THIS
    
    # Continue with enrichments
    df = calculate_breakeven(df)
    df = compute_moneyness(df)
    df = calculate_skew_and_kurtosis(df)
    df = calculate_pcs(df)
    
    return df
```

---

## 📝 SUMMARY

**Question:** "What does the world look like right now, before any trader thinks or acts?"

**Answer:** Phase 1-4 currently captures:
- ✅ Option contract state (Greeks, prices, P&L)
- ✅ Time context (DTE, market session)
- ✅ Volatility context (IV Mid, IV_Rank stub, Earnings proximity)
- ✅ Capital context (deployed capital, trade aggregates)
- ❌ **MISSING:** Price action context (OHLCV, trend, momentum, volume)

**Critical Gap:** Cannot answer "Is the underlying overextended?" or "Is this a bullish setup given current trend?" without OHLCV + trend indicators.

**Recommended Action:**
1. ✅ **Reuse scan engine's Schwab OHLCV implementation** (already production-ready with caching!)
2. Add trend/momentum computation in Phase 3 (SMA, RSI, MACD, S/R from Schwab data)
3. Refactor chart_engine to split observables (Phase 3) from exit signals (Phase 8)

**Key Advantages:**
- **No code duplication** — Scan engine already has perfect Schwab price history fetcher
- **Same data source** — Scan engine and portfolio pipeline use same Schwab API
- **Built-in caching** — 24hr TTL, avoids redundant API calls
- **Production-tested** — Retry logic, error handling, rate limiting already implemented
- **180-day history** — More data than initially planned (60 days)

**Impact:** Complete "observable reality" snapshot — every position now has full market context (Greeks + Price Action + Volatility + Earnings + Momentum).

**Estimated Effort:** 30-45 minutes (reuse existing code, just wire up Phase 1 → scan engine)

---

**End of Audit Report**
