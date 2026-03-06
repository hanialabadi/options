# Enrichment System Architecture

**Date:** 2026-02-04
**Status:** Implemented
**Location:** `core/enrichment/`

---

## Problem Statement

The scan pipeline had a structural flaw where trades entering `AWAIT_CONFIRMATION` would stall indefinitely because:

1. IV history was detected as missing (`IV_Maturity_State = MISSING`)
2. No automated enrichment was triggered
3. Trades had no path to resolution

The previous approach of "if IV missing, run Fidelity scraper" was rejected because it:
- Hard-coded policy decisions into code
- Privileged certain strategies over others
- Was not extensible or testable

---

## Solution: Bias-Free, Deterministic Enrichment

The new system separates concerns into four distinct layers:

```
┌─────────────────────────────────────────────────────────────────┐
│                     PIPELINE STAGES                              │
│  Step 2 → Step 3 → ... → Step 12 → AWAIT_CONFIRMATION            │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│              1. REQUIREMENT DETECTOR (Pure Function)             │
│  • Examines DataFrame                                            │
│  • Emits DataRequirement[] for each trade                       │
│  • NO side effects, NO IO, NO policy decisions                  │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│              2. RESOLVER REGISTRY (Configuration)                │
│  • Maps RequirementType → List[ResolverConfig]                  │
│  • Defines rate limits, priorities, batch sizes                 │
│  • Extensible via config, not code changes                      │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│              3. ENRICHMENT EXECUTOR (Orchestrator)               │
│  • Executes resolvers with rate limiting                        │
│  • Tracks attempts, implements backoff                          │
│  • Returns enriched data dictionary                             │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│              4. PIPELINE INTEGRATION (Merge + Re-entry)          │
│  • Merges enriched data back into DataFrame                     │
│  • Determines which stages need re-running                      │
│  • Triggers next enrichment cycle if needed                     │
└─────────────────────────────────────────────────────────────────┘
```

---

## Data Requirement Schema

Every unsatisfied data need is expressed as a `DataRequirement`:

```python
@dataclass(frozen=True)
class DataRequirement:
    requirement_type: RequirementType   # e.g., IV_HISTORY, IV_RANK, LIQUIDITY
    entity_id: str                      # e.g., "AAPL"
    field_name: str                     # e.g., "iv_history_count"
    current_value: Any                  # e.g., 0
    required_threshold: Any             # e.g., 120
    priority: RequirementPriority       # P1_BLOCKING, P2_IMPORTANT, P3_ENHANCING
```

### Requirement Types

| Type | Description | Actionable? |
|------|-------------|-------------|
| `IV_HISTORY` | Historical IV term structure | Yes |
| `IV_RANK` | Computed IV percentile | Yes |
| `QUOTE_FRESHNESS` | Bid/ask quote availability | Yes |
| `LIQUIDITY_METRICS` | OI, spread % | **No** (market-dependent) |
| `GREEKS` | Delta, gamma, theta, vega | Yes |
| `PRICE_HISTORY` | Historical prices for technicals | Yes |

### Priority Levels

| Priority | Meaning |
|----------|---------|
| `P1_BLOCKING` | Cannot proceed without this data |
| `P2_IMPORTANT` | Significantly impacts decision quality |
| `P3_ENHANCING` | Nice to have, not blocking |

---

## Resolver Registry

The registry maps requirements to resolution strategies:

```python
RequirementType.IV_HISTORY → [
    ResolverConfig(IV_HISTORY_DB, priority=1),      # Check cache first
    ResolverConfig(SCHWAB_API, priority=2),         # API if not cached
    ResolverConfig(FIDELITY_SCRAPER, priority=3),   # Scraper as fallback
]

RequirementType.LIQUIDITY_METRICS → [
    ResolverConfig(MARKET_WAIT, is_actionable=False)  # Cannot actively resolve
]
```

### Rate Limiting

Each resolver has configurable rate limits:

```python
RateLimit(
    requests_per_minute=120,
    requests_per_hour=1000,
    requests_per_day=10000,
    cooldown_on_failure=timedelta(minutes=15)
)
```

---

## Infinite Loop Prevention

The system prevents infinite enrichment loops through:

1. **Max Attempts per Requirement**: Default 3 attempts before marking `UNRESOLVABLE`
2. **Cooldown Period**: Exponential backoff (1h, 2h, 4h, ...)
3. **Cycle Limit**: Maximum 3 enrichment cycles per pipeline run
4. **Terminal States**: Once `SATISFIED` or `UNRESOLVABLE`, no more attempts

```
Attempt 1 → Failure → Wait 1 hour
Attempt 2 → Failure → Wait 2 hours
Attempt 3 → Failure → Status = UNRESOLVABLE (no more attempts)
```

---

## Blocker Decomposition

Instead of monolithic `AWAIT_CONFIRMATION`, each trade has structured blockers:

```python
TradeBlockers(
    trade_id="AAPL_Long_Call",
    ticker="AAPL",
    strategy_name="Long Call",
    requirements=[
        DataRequirement(IV_HISTORY, "AAPL", "iv_history_count", 0, 120, P1_BLOCKING),
        DataRequirement(LIQUIDITY, "AAPL", "liquidity", {...}, {...}, P2_IMPORTANT),
    ]
)
```

### Blocker Summary

Human-readable summary:
```
AAPL Long Call: BLOCKED: IV_HISTORY(iv_history_count); LIQUIDITY_METRICS(liquidity)
MSFT CSP: READY
```

---

## Pipeline Integration

### Usage

```python
from core.enrichment import enrich_pipeline_data, get_blocker_analysis

# At end of Step 12
df, result = enrich_pipeline_data(df, stage_name="step12")

if result.should_rerun_pipeline:
    # Re-run affected stages with enriched data
    for stage in result.affected_stages:
        df = run_stage(df, stage)

# Get blocker analysis for dashboard
blockers_df = get_blocker_analysis(df)
```

### EnrichmentResult

```python
@dataclass
class EnrichmentResult:
    tickers_enriched: List[str]
    data_obtained: Dict[str, Any]
    requirements_satisfied: int
    requirements_remaining: int
    elapsed_seconds: float
    should_rerun_pipeline: bool
    affected_stages: List[str]  # e.g., ["step2", "step7", "step10"]
```

---

## Strategy Agnosticism

The system is explicitly **strategy-agnostic**:

| What It Does | What It Does NOT Do |
|--------------|---------------------|
| Detect missing IV history | Prioritize CSP over directional |
| Emit requirement for any missing data | Run Fidelity for CSP but not calls |
| Apply same thresholds to all trades | Hard-code strategy-specific rules |
| Let resolvers handle enrichment | Make trading decisions |

**Key Insight**: The requirement detector only looks at **data fields**, never at `Strategy_Name`.

---

## Extensibility

### Adding a New Requirement Type

```python
# 1. Add to RequirementType enum
class RequirementType(Enum):
    MY_NEW_DATA = auto()

# 2. Add detection logic to requirement_detector.py
def _detect_my_new_data_requirement(row, ticker, thresholds):
    if row.get('my_field') is None:
        return DataRequirement(...)
    return None

# 3. Register resolvers in resolver_registry.py
registry.register(RequirementType.MY_NEW_DATA, ResolverConfig(...))

# 4. Implement resolver function
def my_resolver(tickers, config):
    return {ticker: fetch_data(ticker) for ticker in tickers}

executor.register_resolver_impl(ResolverType.MY_RESOLVER, my_resolver)
```

### Customizing Thresholds

```python
custom_thresholds = DetectionThresholds(
    iv_history_mature_days=90,   # Lower threshold
    liquidity_min_oi=50,         # More permissive
    iv_rank_required=False,      # Make optional
)

hook = PipelineEnrichmentHook(thresholds=custom_thresholds)
```

---

## File Structure

```
core/enrichment/
├── __init__.py                 # Public API exports
├── data_requirements.py        # Requirement schema and value objects
├── requirement_detector.py     # Pure detection functions
├── resolver_registry.py        # Resolver configuration
├── enrichment_executor.py      # Execution with rate limiting
└── pipeline_integration.py     # Pipeline hooks and merge logic
```

---

## Testing

The system is designed for testability:

1. **Requirement Detector**: Pure functions, no mocks needed
2. **Resolver Registry**: Configuration only, no execution
3. **Executor**: Can run in `dry_run=True` mode
4. **Integration**: Deterministic given same input data

```python
# Test requirement detection
reqs = detect_requirements_for_row(mock_row, DEFAULT_THRESHOLDS)
assert len(reqs) == 2
assert reqs[0].requirement_type == RequirementType.IV_HISTORY

# Test resolver chain
chain = get_resolver_chain(RequirementType.IV_HISTORY)
assert chain[0].resolver_type == ResolverType.IV_HISTORY_DB
assert chain[1].resolver_type == ResolverType.SCHWAB_API
```

---

## Summary

| Concern | Solution |
|---------|----------|
| What is missing? | `DataRequirement` schema |
| What can resolve it? | `ResolverRegistry` configuration |
| How to execute? | `EnrichmentExecutor` with rate limits |
| How to integrate? | `PipelineEnrichmentHook` with merge |
| How to prevent loops? | Max attempts, cooldown, cycle limit |
| How to keep bias-free? | Strategy-agnostic detection |

The system is **deterministic**, **extensible**, **observable**, and **safe**.
