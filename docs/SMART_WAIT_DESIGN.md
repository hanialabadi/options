# Smart WAIT Loop: Design Specification

## 📋 Overview

The Smart WAIT Loop transforms the scan engine from a single-shot discovery system into a closed-loop execution funnel that continuously tracks trade ideas from discovery through confirmation to execution or rejection.

**Core Principle**: The system decides what to execute now, what to wait on, and what to discard — automatically, repeatedly, and without emotion.

---

## 🎯 Execution Funnel (Canonical)

```
DISCOVER → WAIT (looped) → READY_NOW → EXECUTE
                 ↘
                 REJECT (expiry / invalidation)
```

**WAIT is not passive. WAIT is a stateful, looped hypothesis that is re-evaluated every scan until it resolves.**

---

## 🔄 Trade State Machine

### State Definitions

#### 1. READY_NOW
**Semantics**: High-conviction trade executable immediately
- All execution gates passed (R0.x - R2.x)
- Strike + expiration finalized
- Liquidity confirmed
- All confirmation conditions satisfied (if any)
- **Outcome**: Hand off to execution system

#### 2. AWAIT_CONFIRMATION
**Semantics**: Valid setup waiting on explicit, testable confirmation
- Passed structural gates (R0.1 - R0.5)
- Strategy is sound
- Waiting on specific, binary conditions
- Has not expired (within TTL)
- **Outcome**: Re-evaluate on next scan

#### 3. REJECTED
**Semantics**: Permanently removed from consideration
- Failed structural gates (liquidity, data, compatibility)
- Expired waiting period (TTL exceeded)
- Invalidated by market conditions
- **Outcome**: Log reason, discard

### State Transitions

```
           ┌─────────────────────────┐
           │   DISCOVERY (Step 12)   │
           └───────────┬─────────────┘
                       │
           ┌───────────▼────────────┐
           │  Execution Gate Check  │
           └───┬────────────┬───────┘
               │            │
        [PASS] │            │ [FAIL]
               │            │
               ▼            ▼
       ┌──────────┐   ┌──────────┐
       │  Quality │   │ REJECTED │
       │   Gates  │   └──────────┘
       └─────┬────┘
             │
      [Needs │ Confirmation?]
             │
      ┌──────┴───────┐
      │              │
   [YES]          [NO]
      │              │
      ▼              ▼
┌──────────┐   ┌──────────┐
│  AWAIT   │   │  READY   │
│CONFIRMAT.│   │   NOW    │
└────┬─────┘   └──────────┘
     │
     │ [Re-evaluate every scan]
     │
     ├──[Conditions Met]──────> READY_NOW
     ├──[TTL Expired]─────────> REJECTED
     └──[Still Waiting]───────> AWAIT_CONFIRMATION
```

---

## 📊 Data Schema: WAIT Trade Persistence

### DuckDB Table: `wait_list`

```sql
CREATE TABLE IF NOT EXISTS wait_list (
    -- Identity
    wait_id VARCHAR PRIMARY KEY,              -- UUID for each WAIT entry
    ticker VARCHAR NOT NULL,
    strategy_name VARCHAR NOT NULL,
    strategy_type VARCHAR NOT NULL,           -- DIRECTIONAL, INCOME, etc.

    -- Contract Details (proposed)
    proposed_strike DOUBLE,
    proposed_expiration DATE,
    contract_symbol VARCHAR,

    -- Wait Metadata
    wait_started_at TIMESTAMP NOT NULL,       -- When entered WAIT state
    wait_expires_at TIMESTAMP NOT NULL,       -- TTL deadline
    last_evaluated_at TIMESTAMP NOT NULL,     -- Last scan timestamp
    evaluation_count INTEGER DEFAULT 1,       -- How many times re-evaluated

    -- Confirmation Conditions (JSON array)
    wait_conditions JSON NOT NULL,            -- Array of testable conditions
    conditions_met JSON DEFAULT '[]',         -- Array of satisfied conditions
    wait_progress DOUBLE DEFAULT 0.0,         -- % complete (0.0 - 1.0)

    -- Snapshot State (frozen at wait entry)
    entry_price DOUBLE,
    entry_iv_30d DOUBLE,
    entry_hv_30 DOUBLE,
    entry_chart_signal VARCHAR,
    entry_pcs_score DOUBLE,

    -- Current State (updated each evaluation)
    current_price DOUBLE,
    current_iv_30d DOUBLE,
    current_chart_signal VARCHAR,
    price_change_pct DOUBLE,

    -- Exit Conditions
    invalidation_price DOUBLE,                -- Price level that invalidates setup
    max_sessions_wait INTEGER DEFAULT 3,
    max_days_wait INTEGER DEFAULT 5,

    -- Status
    status VARCHAR DEFAULT 'ACTIVE',          -- ACTIVE, PROMOTED, EXPIRED, INVALIDATED
    rejection_reason VARCHAR,

    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_wait_list_status ON wait_list(status);
CREATE INDEX idx_wait_list_ticker ON wait_list(ticker);
CREATE INDEX idx_wait_list_expires ON wait_list(wait_expires_at);
```

### Example Wait Conditions (JSON Schema)

```json
{
  "wait_conditions": [
    {
      "condition_id": "price_reclaim_260",
      "type": "price_level",
      "operator": "above",
      "threshold": 260.00,
      "timeframe": "close",
      "description": "Close above $260 resistance"
    },
    {
      "condition_id": "two_green_candles",
      "type": "candle_pattern",
      "pattern": "consecutive_green",
      "count": 2,
      "timeframe": "30m",
      "description": "Two consecutive 30m green candles"
    },
    {
      "condition_id": "liquidity_improvement",
      "type": "liquidity",
      "metric": "bid_ask_spread_pct",
      "operator": "less_than",
      "threshold": 0.05,
      "description": "Bid/ask spread < 5%"
    },
    {
      "condition_id": "time_based_recheck",
      "type": "time_delay",
      "delay_hours": 24,
      "description": "Wait 24 hours for volatility to settle"
    }
  ]
}
```

---

## 🔍 Confirmation Condition Types

### 1. Price Level Conditions
```python
{
    "type": "price_level",
    "operator": "above" | "below" | "between",
    "threshold": float | [low, high],
    "timeframe": "intraday" | "close" | "session_high" | "session_low"
}
```

**Examples:**
- Close above VWAP
- Break above resistance
- Reclaim moving average
- Stay below invalidation level

### 2. Candle Pattern Conditions
```python
{
    "type": "candle_pattern",
    "pattern": "consecutive_green" | "consecutive_red" | "engulfing" | "hammer",
    "count": int,
    "timeframe": "5m" | "15m" | "30m" | "1h" | "1d"
}
```

**Examples:**
- Two consecutive green 30m candles
- Bullish engulfing on daily
- No red candles for 3 sessions

### 3. Liquidity Conditions
```python
{
    "type": "liquidity",
    "metric": "bid_ask_spread_pct" | "open_interest" | "volume",
    "operator": "less_than" | "greater_than",
    "threshold": float
}
```

**Examples:**
- Spread tightens to <5%
- Open interest increases by 20%
- Daily volume >500k

### 4. Time-Based Conditions
```python
{
    "type": "time_delay",
    "delay_hours": int,
    "delay_sessions": int,
    "next_session": bool
}
```

**Examples:**
- Wait 24 hours for volatility to settle
- Wait for next trading session
- Recheck after 3 sessions

### 5. Volatility Conditions
```python
{
    "type": "volatility",
    "metric": "iv_30d" | "hv_30" | "ivhv_gap",
    "operator": "less_than" | "greater_than" | "between",
    "threshold": float | [low, high]
}
```

**Examples:**
- IV settles below 40%
- HV confirms uptrend (>35%)
- IV/HV gap expands to >10%

---

## ⚙️ Re-Evaluation Engine

### Flow: Every Scan Execution

```
1. LOAD WAIT LIST
   ↓
2. EXPIRE STALE ENTRIES (TTL check)
   ↓
3. RE-EVALUATE ACTIVE WAITS
   ├─ Fetch fresh market data
   ├─ Check each wait_condition
   ├─ Update conditions_met
   ├─ Calculate wait_progress
   └─ Check invalidation triggers
   ↓
4. APPLY TRANSITIONS
   ├─ All conditions met → PROMOTE to READY_NOW
   ├─ Invalidated → REJECT
   └─ Still waiting → Update last_evaluated_at
   ↓
5. PROCEED TO NEW DISCOVERY
   (Steps 0-12 as normal)
   ↓
6. MERGE OUTPUTS
   ├─ Promoted WAIT trades (READY_NOW)
   ├─ New READY_NOW from discovery
   ├─ New AWAIT_CONFIRMATION from discovery
   └─ Rejected trades
```

### Re-Evaluation Module: `core/wait_loop/evaluator.py`

```python
class WaitConditionEvaluator:
    """Evaluates wait conditions and manages state transitions."""

    def evaluate_wait_list(self, con: duckdb.DuckDBPyConnection) -> WaitEvaluationResult:
        """
        Re-evaluates all ACTIVE wait list entries.

        Returns:
            WaitEvaluationResult with promoted, rejected, and still_waiting trades
        """

    def check_condition(self, condition: dict, market_data: dict) -> bool:
        """Check if a single wait condition is satisfied."""

    def check_ttl_expiry(self, wait_entry: dict) -> bool:
        """Check if wait entry has exceeded TTL."""

    def check_invalidation(self, wait_entry: dict, market_data: dict) -> Optional[str]:
        """Check if setup has been invalidated. Returns reason if invalidated."""

    def promote_to_ready(self, wait_id: str, con: duckdb.DuckDBPyConnection):
        """Promote wait entry to READY_NOW status."""

    def reject_wait(self, wait_id: str, reason: str, con: duckdb.DuckDBPyConnection):
        """Reject wait entry with reason."""
```

---

## 🎯 Promotion Logic (WAIT → READY)

A trade is promoted to READY_NOW **if and only if**:

### Prerequisites (All Must Pass)
1. ✅ **All wait_conditions satisfied** (`wait_progress == 1.0`)
2. ✅ **Liquidity gates still pass** (re-check spread, OI, volume)
3. ✅ **Data gates still pass** (re-check data completeness)
4. ✅ **Strategy remains valid** in current regime
5. ✅ **Not invalidated** (price hasn't hit invalidation level)
6. ✅ **Within expiration window** (contracts still available)

### Promotion Flow
```python
def attempt_promotion(wait_entry: dict, current_market: dict) -> PromotionResult:
    """
    Attempt to promote WAIT entry to READY_NOW.

    Returns:
        - PROMOTED: All conditions met, ready to execute
        - STILL_WAITING: Some conditions pending
        - REJECTED: Failed re-validation
    """
    # 1. Check all wait conditions
    if not all_conditions_met(wait_entry):
        return PromotionResult.STILL_WAITING

    # 2. Re-run quality gates
    gates_result = re_validate_gates(wait_entry, current_market)
    if not gates_result.passed:
        return PromotionResult.REJECTED(reason=gates_result.failure_reason)

    # 3. Check contract availability
    contracts = fetch_contracts(wait_entry.ticker, wait_entry.strategy)
    if not contracts:
        return PromotionResult.REJECTED(reason="No contracts available")

    # 4. Apply Fidelity escalation if needed (existing RAG rules only)
    if requires_fidelity_iv(wait_entry):
        fidelity_result = check_fidelity_iv(wait_entry.ticker)
        if not fidelity_result.available:
            return PromotionResult.STILL_WAITING  # Escalate, don't reject

    # 5. Promote
    return PromotionResult.PROMOTED
```

**No shortcuts. No gate bypasses.**

---

## ⏱️ TTL & Expiry Logic

### TTL Parameters (Per Strategy Type)

```python
TTL_CONFIG = {
    "DIRECTIONAL": {
        "max_sessions_wait": 3,      # Max 3 trading sessions
        "max_days_wait": 5,           # Max 5 calendar days
        "invalidate_if_no_progress": True
    },
    "INCOME": {
        "max_sessions_wait": 5,      # More patience for income
        "max_days_wait": 7,
        "invalidate_if_no_progress": False
    },
    "LEAP": {
        "max_sessions_wait": 10,     # Longer timeframe
        "max_days_wait": 14,
        "invalidate_if_no_progress": False
    }
}
```

### Expiry Rules

```python
def should_expire(wait_entry: dict) -> Tuple[bool, Optional[str]]:
    """
    Check if wait entry should be expired.

    Returns:
        (should_expire: bool, reason: Optional[str])
    """
    now = datetime.now()
    ttl_config = TTL_CONFIG[wait_entry['strategy_type']]

    # 1. Hard TTL deadline
    if now > wait_entry['wait_expires_at']:
        return True, f"TTL_EXPIRED: {ttl_config['max_days_wait']} days"

    # 2. Session count exceeded
    sessions_elapsed = count_trading_sessions(
        wait_entry['wait_started_at'],
        now
    )
    if sessions_elapsed > ttl_config['max_sessions_wait']:
        return True, f"MAX_SESSIONS: {sessions_elapsed}/{ttl_config['max_sessions_wait']}"

    # 3. No progress and config requires it
    if ttl_config['invalidate_if_no_progress']:
        if wait_entry['wait_progress'] == 0.0 and sessions_elapsed >= 2:
            return True, "NO_PROGRESS: 0% after 2 sessions"

    return False, None
```

---

## 🏗️ Implementation Plan

### Files to Create

#### 1. `core/wait_loop/__init__.py`
- Module initialization
- Export public API

#### 2. `core/wait_loop/schema.py`
- DuckDB schema for wait_list table
- Condition type definitions
- Data contracts

#### 3. `core/wait_loop/evaluator.py`
- WaitConditionEvaluator class
- Condition checking logic
- State transition logic

#### 4. `core/wait_loop/conditions.py`
- Condition factory
- Individual condition checkers (price, candle, liquidity, time, volatility)
- Condition validation

#### 5. `core/wait_loop/ttl.py`
- TTL configuration
- Expiry logic
- Trading session counting

#### 6. `core/wait_loop/persistence.py`
- Save/load wait list entries
- Update operations
- Query helpers

### Files to Modify

#### 1. `scan_engine/step12_acceptance.py`
**Changes:**
- Split CONDITIONAL into AWAIT_CONFIRMATION vs REJECTED
- Generate wait_conditions for AWAIT_CONFIRMATION trades
- Save new AWAIT_CONFIRMATION trades to wait_list

#### 2. `scan_engine/pipeline.py`
**Changes:**
- Add Step -1: Re-evaluate WAIT list (before discovery)
- Merge promoted WAIT trades with new discoveries
- Update final output format

#### 3. `core/shared/data_contracts/schema.py`
**Changes:**
- Add WaitListEntry contract
- Add ConfirmationCondition contract
- Add PromotionResult contract

### Files for Output Formatting

#### 4. `core/wait_loop/output_formatter.py`
- Format EXECUTE NOW section
- Format WAITLIST section
- Format REJECTED section

---

## 📤 Output Format (Every Scan)

### Terminal Output Structure

```
================================================================================
🟢 EXECUTE NOW (3 trades)
================================================================================

1. AAPL - Iron Condor (INCOME)
   Strike: 260/265/270/275
   Expiration: 2026-03-21
   Rationale: IV crush after earnings, liquidity confirmed
   Confidence: 89%
   Origin: Promoted from WAIT (conditions met after 2 sessions)

2. MSFT - Bull Put Spread (DIRECTIONAL)
   Strike: 450/445
   Expiration: 2026-02-28
   Rationale: Reclaimed VWAP, two green candles confirmed
   Confidence: 92%
   Origin: New discovery (READY_NOW)

3. NVDA - Covered Call (INCOME)
   Strike: 900
   Expiration: 2026-03-14
   Rationale: High IV rank (87%), above 52w support
   Confidence: 85%
   Origin: Promoted from WAIT (liquidity improved)

================================================================================
🟡 WAITLIST (12 trades)
================================================================================

1. TSLA - Bull Put Spread (DIRECTIONAL)
   Strike: 380/375 (proposed)
   Expiration: 2026-03-07 (proposed)
   Waiting on: [2/3 conditions met]
     ✅ Close above $380 resistance
     ✅ Two consecutive green 30m candles
     ⏳ Bid/ask spread < 5% (currently 7.2%)
   Progress: 67%
   TTL: 3 days remaining (expires 2026-02-06)
   Sessions elapsed: 1/3

2. COIN - Iron Condor (INCOME)
   Strike: 260/265/270/275 (proposed)
   Expiration: 2026-03-21 (proposed)
   Waiting on: [0/2 conditions met]
     ⏳ IV settles below 50% (currently 62%)
     ⏳ Wait 24 hours for volatility to settle (18h remaining)
   Progress: 0%
   TTL: 4 days remaining (expires 2026-02-07)
   Sessions elapsed: 0/5

[... 10 more ...]

================================================================================
🔴 REJECTED (8 trades)
================================================================================

1. PLTR - Bull Call Spread (DIRECTIONAL)
   Reason: FAILED_LIQUIDITY_FILTER (Step 9B)
   Details: Bid/ask spread 18.2% (threshold: 10%)

2. ABT - Iron Condor (INCOME)
   Reason: TTL_EXPIRED (3 days, no confirmation)
   Details: Price never reclaimed $120 resistance

3. ADBE - Covered Call (INCOME)
   Reason: INVALIDATED (price broke below $540 support)
   Details: Setup invalidated on 2026-02-02 16:30

[... 5 more ...]

================================================================================
📊 SCAN SUMMARY
================================================================================
Total Strategies Evaluated: 564
  └─ From Wait List: 20 re-evaluated
     ├─ Promoted to READY_NOW: 3
     ├─ Expired/Rejected: 5
     └─ Still Waiting: 12
  └─ From New Discovery: 544
     ├─ READY_NOW: 0
     ├─ AWAIT_CONFIRMATION: 0
     └─ REJECTED: 544

Final Counts:
  🟢 READY_NOW: 3 trades
  🟡 AWAIT_CONFIRMATION: 12 trades (active in wait loop)
  🔴 REJECTED: 549 trades
================================================================================
```

---

## 🔐 Constraint Compliance

### What This Design Does NOT Do

❌ **Relax liquidity thresholds** - All existing gates remain unchanged
❌ **Bypass execution gates** - WAIT trades must pass gates on promotion
❌ **Auto-trigger Fidelity** - Only existing RAG rules apply
❌ **Add discretionary heuristics** - All conditions are binary and testable
❌ **Weaken RAG semantics** - Execution semantics remain authoritative

### What This Design DOES Do

✅ **Track valid setups across scans** - Stateful persistence
✅ **Enforce explicit confirmation** - No gut feel, only testable conditions
✅ **Expire stale ideas** - TTL prevents zombie trades
✅ **Re-evaluate continuously** - Loop until resolution
✅ **Maintain high conviction** - READY_NOW remains rare and meaningful

---

## 🎯 Success Criteria

### Qualitative Goals
1. **High Conviction Trades**: READY_NOW represents <5% of discoveries
2. **No Zombie Trades**: All WAIT entries resolve within TTL
3. **Explicit Conditions**: 100% of WAIT trades have testable conditions
4. **Continuous Operation**: System runs autonomously without manual intervention

### Quantitative Metrics
```python
# Track across scans
metrics = {
    "wait_list_size": int,              # Active WAIT trades
    "promotion_rate": float,             # WAIT → READY_NOW conversion %
    "expiry_rate": float,                # % expired without promotion
    "avg_time_to_promotion_hours": float,
    "avg_conditions_per_trade": float,
    "ready_now_count": int,
    "rejected_count": int
}
```

---

## 🚀 Next Steps

1. **User Review**: Confirm design alignment with vision
2. **Implementation Priority**:
   - Phase 1: Core wait loop (schema, persistence, evaluator)
   - Phase 2: Condition system (types, checkers, validators)
   - Phase 3: Integration (Step 12 modification, pipeline integration)
   - Phase 4: Output formatting and monitoring
3. **Testing Strategy**: Use recent scan results to simulate WAIT scenarios
4. **Rollout**: Enable wait loop alongside existing pipeline (additive, not disruptive)

---

**Last Updated**: 2026-02-03
**Status**: Design Complete - Awaiting User Approval
**RAG Source**: docs/EXECUTION_SEMANTICS.md, ARCHITECTURE_ROADMAP.md
