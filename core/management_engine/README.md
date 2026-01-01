# Management Engine (Phase 2)

This package contains all logic for monitoring, revalidating, and managing active option trades.

## Responsibilities
- Drift detection and live metric deltas
- PCS re-evaluation for active trades
- Exit / hold / trim recommendations
- Revalidation pipeline orchestration

## Canonical Modules

| Module | Purpose | Entry Point |
|--------|---------|-------------|
| `monitor.py` | Drift + metric deltas | `run_drift_engine()` |
| `pcs_live.py` | Live PCS recomputation | `compute_live_pcs()` |
| `recommend.py` | Exit / hold decisions | `generate_trade_recommendations()` |
| `revalidate.py` | Orchestration entrypoint | `run_revalidation_pipeline()` |

## What This Package Does NOT Do

❌ No scanning logic (use `scan_engine/`)  
❌ No UI logic (use `streamlit_app/`)  
❌ No data I/O (use `data_contracts/`)

## Usage

```python
from core.management_engine import (
    run_drift_engine,
    compute_live_pcs,
    generate_trade_recommendations,
    run_revalidation_pipeline
)

# Monitor active trades for drift
run_drift_engine()

# Recompute PCS for all active trades
compute_live_pcs()

# Generate exit/hold recommendations
recommendations = generate_trade_recommendations()

# Full revalidation pipeline
run_revalidation_pipeline()
```

## Data Flow

1. **Load active trades** via `data_contracts.load_active_master()`
2. **Monitor drift** via `monitor.py` → snapshot deltas
3. **Recompute PCS** via `pcs_live.py` → live Greeks + market conditions
4. **Generate recommendations** via `recommend.py` → HOLD / EXIT / TRIM / REVALIDATE
5. **Orchestrate updates** via `revalidate.py` → Greeks → Charts → PCS → Recommendations

## Migration Notes

Previously scattered across:
- `core/phase7_drift_engine.py` → now `monitor.py`
- `core/pcs_engine_v3_unified.py` → now `pcs_live.py`
- `core/rec_engine_v6_overlay.py` → now `recommend.py`
- `core/phase10_revalidate_pipeline.py` → now `revalidate.py`

All imports updated. Legacy versions quarantined in `core/legacy/`.
