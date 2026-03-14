import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from core.management.cycle2.drift.drift_engine import DriftEngine


def test_drift_authoritative_overrides():
    """Test drift engine structural overrides: data staleness, orphaned, broken legs."""
    now = datetime.now()
    data = {
        'TradeID': ['T1', 'T2', 'T3', 'T4'],
        'Symbol': ['AAPL_C', 'MSFT_P', 'GOOGL_C', 'META_C'],
        'Snapshot_TS': [
            now - timedelta(minutes=5),   # T1: Fresh
            now - timedelta(minutes=20),  # T2: Stale
            now - timedelta(minutes=70),  # T3: Orphaned
            now - timedelta(minutes=5),   # T4: Fresh + Broken leg
        ],
        'LegStatus': ['Active', 'Active', 'Active', 'Broken'],
        'Rec_Action': ['HOLD', 'HOLD', 'HOLD', 'HOLD'],
        'Delta_Trade': [1, 1, 1, 1],
        'Underlying': ['AAPL', 'MSFT', 'GOOGL', 'META']
    }
    df = pd.DataFrame(data)

    engine = DriftEngine(persona='conservative')
    import core.management.cycle2.drift.drift_engine as de
    original_check = de.check_market_stress
    de.check_market_stress = lambda: ('GREEN', 20.0, 'mock')

    original_els = de.evaluate_leg_status
    de.evaluate_leg_status = lambda df, legs_dir=None: df

    try:
        df_analyzed = engine.run_drift_analysis(df)
        df_final = engine.apply_drift_filter(df_analyzed, rec_col='Rec_Action')

        # T1: Fresh, Valid, Intact → HOLD (no override)
        assert df_final.loc[0, 'Rec_Action_Final'] == 'HOLD'

        # T2: Stale → REVIEW
        assert df_final.loc[1, 'Rec_Action_Final'] == 'REVIEW'

        # T3: Orphaned → QUARANTINE → REVIEW
        assert df_final.loc[2, 'Rec_Action_Final'] == 'REVIEW'

        # T4: Broken leg → FORCE_EXIT → EXIT
        assert df_final.loc[3, 'Rec_Action_Final'] == 'EXIT'

    finally:
        de.check_market_stress = original_check
        de.evaluate_leg_status = original_els


def test_regime_halt_overrides():
    """Test RED regime → HARD_HALT → WAIT override."""
    now = datetime.now()
    data = {
        'TradeID': ['T1'],
        'Symbol': ['AAPL_C'],
        'Snapshot_TS': [now],
        'Delta_Trade': [1],
        'Underlying': ['AAPL'],
        'LegStatus': ['Active'],
        'Rec_Action': ['HOLD']
    }
    df = pd.DataFrame(data)

    engine = DriftEngine(persona='conservative')
    import core.management.cycle2.drift.drift_engine as de
    original_check = de.check_market_stress
    original_els = de.evaluate_leg_status
    de.evaluate_leg_status = lambda df, legs_dir=None: df

    try:
        de.check_market_stress = lambda: ('RED', 45.0, 'mock')
        df_red = engine.run_drift_analysis(df.copy())
        df_red_final = engine.apply_drift_filter(df_red, rec_col='Rec_Action')

        assert df_red_final.loc[0, 'Regime_State'] == 'HALTED'
        assert df_red_final.loc[0, 'Rec_Action_Final'] == 'WAIT'

    finally:
        de.check_market_stress = original_check
        de.evaluate_leg_status = original_els


if __name__ == "__main__":
    test_drift_authoritative_overrides()
    test_regime_halt_overrides()
