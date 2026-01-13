"""
ML Training Data Collection

Collects completed trades with full context for machine learning:
- Entry conditions (Cycle 1 first snapshot)
- Evolution metrics (Cycle 2 drift analysis)
- Chart context (Cycle 3 market timing)
- Outcomes (exit P&L, win/loss)

Training Pipeline:
1. Detect completed trades (exited positions)
2. Extract entry baseline from first_seen snapshot
3. Compute evolution trajectory (drift over time)
4. Load chart context at entry and exit
5. Record outcome (P&L, exit reason, timing)
6. Store in training dataset
"""

__all__ = [
    'collect_completed_trades',
    'extract_training_features',
    'prepare_ml_dataset',
]
