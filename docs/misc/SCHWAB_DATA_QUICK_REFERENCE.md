# Schwab API Data - Quick Reference

**For**: Options scan engine (pre-trade discovery only)  
**Date**: January 2, 2026

---

## 🎯 TL;DR - What Can Schwab Actually Provide?

### ✅ EXCELLENT (Production-Ready, Always Reliable)
- **Option Greeks**: delta, gamma, vega, theta, rho (per-contract, real-time)
- **Option Pricing**: bid, ask, mark, last (Level 1 depth)
- **Liquidity**: openInterest, totalVolume (accurate during market hours)
- **OHLCV History**: Daily candles for HV calculation (cacheable)
- **Contract Identity**: symbol, strike, expiration, putCall

### ⚠️ GOOD (With Caveats)
- **Underlying Price**: lastPrice (market hours) / mark/closePrice (after hours)
- **Quote Timestamps**: quoteTime, tradeTime (staleness detection)
- **Dividend Data**: dividendDate, dividendYield (sometimes missing)

### ❌ UNRELIABLE / MISSING (Do NOT Use)
- **Underlying IV**: Chain-level `volatility` field is often missing/stale
  - ✅ **Workaround**: Step 0 computes proxy from ATM options (30-45 DTE)
- **IV Rank / IV Percentile**: Not provided by Schwab
  - ⚠️ **Workaround**: Requires local historical IV storage
- **Days to Expiration**: Calendar days (not trading days)
  - ✅ **Workaround**: Recalculated locally
- **Earnings Dates**: Not provided (only dividends)
- **Technical Indicators**: Not provided (compute from price history)

---

## 📊 API Endpoints Used

| Endpoint | Purpose | Rate Limit | Current Usage |
|----------|---------|------------|---------------|
| `/marketdata/v1/quotes` | Underlying quotes | 100 symbols/req | Step 0 (price + volume) |
| `/marketdata/v1/pricehistory` | OHLCV candles | 1 req/ticker | Step 0 (HV calculation) |
| `/marketdata/v1/chains` | Option chains | ~2 req/sec | Step 9B (contract selection) |

---

## 🔍 Currently Used vs Untapped Fields

### Currently Used (Step 0, 9B)
**Quotes**: lastPrice, mark, bidPrice, askPrice, closePrice, totalVolume, quoteTime, tradeTime  
**Price History**: close (for HV calc)  
**Option Chains**: delta, gamma, vega, theta, rho, volatility, bid, ask, mark, last, openInterest, totalVolume, symbol, strikePrice, expirationDate, putCall

### Untapped (Low-Hanging Fruit)
**High Priority**:
- `highPrice`, `lowPrice` → Intraday compression/expansion signals
- `52WeekHigh`, `52WeekLow` → Momentum/contrarian context
- `netChange`, `netPercentChange` → Daily momentum
- `dividendDate` → Assignment risk for short options
- `bidSize`, `askSize` → Execution quality (depth)

**Medium Priority**:
- `openPrice` → Gap detection
- `peRatio`, `dividendYield` → Valuation context
- `intrinsicValue`, `extrinsicValue` → Time premium analysis
- `theoreticalOptionValue` → Mispricing detection

---

## 💡 Quick Recommendations

### Immediate (< 2 hours)
Add to Step 2 or Step 5:
```python
# Compression detection
daily_range_pct = (high - low) / last
if daily_range_pct < 0.01:
    signal = "COMPRESSION_BREAKOUT_SETUP"

# 52-week position
pct_off_high = (high52w - price) / high52w
if pct_off_high < 0.02:
    context = "NEAR_52W_HIGH_MOMENTUM"
```

### Short-Term (< 1 day)
Add to Step 0:
```python
# Parkinson HV (more efficient than close-only)
hv_parkinson = sqrt(ln(high/low)^2 / (4*ln(2))) * sqrt(252)

# Dividend risk check (Step 11)
if 0 < days_to_dividend < dte:
    flag = "HIGH_ASSIGNMENT_RISK"
```

### Medium-Term (< 1 week)
Add to Step 9B:
```python
# IV skew calculation
otm_put_iv = avg(put_delta_-0.25_contracts['volatility'])
otm_call_iv = avg(call_delta_0.25_contracts['volatility'])
skew = otm_put_iv - otm_call_iv
# Positive skew = puts expensive (fear/protection demand)
```

---

## 🚨 Known Limitations (Accept or Workaround)

### Schwab Will NEVER Provide
- ❌ IV Rank, IV Percentile → Requires local historical IV storage
- ❌ Earnings dates → Need 3rd-party API (Alpha Vantage, etc.)
- ❌ Technical indicators (RSI, MACD) → Compute from price history
- ❌ Term structure (IV across expirations) → Compute from chain data
- ❌ Reliable underlying ATM IV → Already worked around in Step 0

### Market Hours Impact
| Metric | Market Hours | After Hours |
|--------|--------------|-------------|
| Price | ✅ lastPrice | ⚠️ mark/closePrice |
| Spreads | ✅ Tight (< 5%) | ⚠️ Wide (10-20%) |
| OI/Volume | ✅ Live | ❌ Frozen |
| Greeks | ✅ Updated ~2 min | ⚠️ Stale (structure valid) |

**Current Handling**: Step 9B adjusts liquidity thresholds automatically

---

## 📋 Detailed Field Reference

See [SCHWAB_API_DATA_INVENTORY.md](SCHWAB_API_DATA_INVENTORY.md) for:
- Complete field listings (50+ fields documented)
- Reliability assessments per field
- Code examples for each enhancement
- Market hours behavior details
- Response structure samples

---

## ✅ Final Assessment

**Question**: Can we extract more useful scan-time data from Schwab API?

**Answer**: **YES** - but primarily through **local computation** of existing fields rather than new API fields.

**Impact**:
- **High value**: Intraday range, 52W position, dividend risk (already in responses)
- **Medium value**: Parkinson HV, IV skew, depth analysis (requires calculation)
- **Low priority**: IVR/IVP (requires historical storage), earnings (3rd-party API)

**Recommendation**: Start with Section "Immediate" enhancements (2 hours work, immediate scan quality boost).

---

**See Also**: [SCHWAB_API_DATA_INVENTORY.md](SCHWAB_API_DATA_INVENTORY.md) (full documentation)
