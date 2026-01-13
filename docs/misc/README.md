# Phase 1: Active Position Intake

## Purpose
Phase 1 is the **management engine's entry point** for active positions.  
It loads raw brokerage data and produces a clean, minimal dataset suitable for:
- Drift tracking
- P/L monitoring
- Position management

**This is NOT a scanning phase.**

## Input Contract

**Canonical Path:** `data/brokerage_inputs/fidelity_positions.csv`

Expected columns from Fidelity export:
- `Symbol` - Option symbol (e.g., "AAPL 240119C00150000")
- `Quantity` - Number of contracts
- `Last`, `Bid`, `Ask` - Current market prices
- `$ Total G/L` - P/L in dollars
- `% Total G/L` - P/L percentage
- `Basis` - Cost basis
- `Theta`, `Vega`, `Delta`, `Gamma` - Broker-provided Greeks
- `Volume`, `Open Int` - Market data
- `Time Val`, `Intrinsic Val` - Option values

## Output Schema

Phase 1 produces a DataFrame with **18 columns**:

### Position Identity
- `Symbol` - Option symbol OR stock ticker
- `AssetType` - OPTION or STOCK (OCC pattern matching)

### Quantity & Cost
- `Quantity` - Contracts held (options) or shares (stocks)
- `Basis` - Cost basis

### P/L
- `$ Total G/L` - Dollar P/L
- `% Total G/L` - Percentage P/L

### Market Data
- `Last` - Current price
- `Bid`, `Ask` - Current quotes
- `Volume`, `Open Int` - Liquidity metrics

### Greeks (Broker Truth - Options Only)
- `Theta`, `Vega`, `Delta`, `Gamma` - Broker-provided Greeks
- Note: Stock positions will have NaN/null for Greeks

### Option Values (Options Only)
- `Time Val` - Time value
- `Intrinsic Val` - Intrinsic value

### Metadata
- `Snapshot_TS` - Timestamp (for drift tracking)

## What Phase 1 Does NOT Include

Phase 1 removes these columns (they will be derived elsewhere):

❌ **IV Metrics** (market-derived, time-sensitive)
- `IV Mid`, `IV Bid`, `IV Ask`

❌ **Parsed Fields** (derivable from Symbol)
- `Expiration`, `Strike`, `Call`, `Put`

❌ **Non-Critical Metadata**
- `Earnings Date`

## Why This Schema?

This schema represents **broker truth at a point in time**.  
It includes:
- ✅ Static position data (Symbol, Quantity, Basis)
- ✅ Asset classification (OPTION vs STOCK)
- ✅ Broker-provided values (Greeks, P/L)
- ✅ Market snapshots (Last, Bid, Ask, Volume)

It excludes:
- ❌ Analytical/derived metrics (will come from management_engine)
- ❌ Time-sensitive market data (will be refreshed via Schwab API)
- ❌ Scanning logic (separate phase)

**Key Change:** Phase 1 now includes BOTH stock and option positions.  
This enables:
- Buy-write detection (long stock + short call)
- Covered call detection (in Phase 2)
- True portfolio view (not just options)

## Asset Type Classification

`AssetType` is determined by Symbol pattern matching:

**OPTION:** Matches OCC standard pattern
- Pattern: `TICKERYYMMDD[C|P]STRIKE`
- Examples: `AAPL250118C150`, `TSLA250115P200`
- Supports short positions: `-AAPL250118C150`

**STOCK:** Everything else
- Examples: `AAPL`, `MSFT`, `SPY`

## Usage

```python
from core.phase1_clean import phase1_load_and_clean_positions

# Load positions and save snapshot
df, snapshot_path = phase1_load_and_clean_positions(
    input_path=Path("data/brokerage_inputs/fidelity_positions.csv"),
    save_snapshot=True
)

# Snapshot saved to: data/snapshots/phase1/phase1_positions_YYYY-MM-DD_HH-MM-SS.csv
```

## Future: Schwab Integration

When Schwab API is integrated, Phase 1 will:
1. Load Fidelity positions (legacy)
2. Enrich with live Schwab data (Greeks, prices, IV)
3. Produce a unified schema

**The 17-column schema is the contract. Future phases adapt to it.**

## Validation

Run test:
```bash
python -c "from pathlib import Path; from core.phase1_clean import phase1_load_and_clean_positions, CANONICAL_INPUT_PATH; df, _ = phase1_load_and_clean_positions(input_path=Path(CANONICAL_INPUT_PATH)); print(f'✅ {len(df)} positions ({dict(df[\"AssetType\"].value_counts())})')"
```

Expected output:
- 9 positions (as of Jan 2026)
- 18 columns
- AssetType distribution (e.g., 9 OPTION, 0 STOCK)
- Snapshot saved to `data/snapshots/phase1/`
