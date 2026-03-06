# recommend.py
# Recommendation overlay: DriftEngine resilience layer + DoctrineAuthority alpha layer.
# rec_engine_v6 is removed — doctrine lives in core.management.cycle3.decision.engine.

import pandas as pd
import logging

from core.management.cycle2.drift.drift_engine import DriftEngine
from core.management.cycle3.decision.engine import generate_recommendations

logger = logging.getLogger(__name__)


def run_v6_overlay(df: pd.DataFrame) -> pd.DataFrame:
    """
    Combined recommendation overlay:
      1. Alpha layer  — DoctrineAuthority per-strategy rules (generate_recommendations)
      2. Resilience   — DriftEngine 5-category drift analysis
      3. Synthesis    — drift_filter intersects doctrine action with drift state

    Input df must have columns produced by Cycle 1+2 enrichment:
      TradeID, Strategy, AssetType, Delta, Gamma, Vega, Theta, IV_30D, HV_20D,
      DTE, Lifecycle_Phase, Moneyness_Label, Underlying_Price_Entry, UL Last,
      PriceStructure_State, MomentumVelocity_State, VolatilityState_State,
      Drift_Direction, Drift_Magnitude, Drift_Persistence, Structural_Data_Complete
    """
    if df.empty:
        logger.warning("run_v6_overlay: empty dataframe — nothing to process")
        return df

    # 1. Alpha Layer: DoctrineAuthority produces Action, Urgency, Rationale, Decision_State
    df = generate_recommendations(df)

    # 2. Resilience Layer: DriftEngine 5-category assessment
    drift_engine = DriftEngine(persona='conservative')
    df = drift_engine.run_drift_analysis(df)

    # 3. Synthesis: intersect doctrine action with drift state
    # Only override action if drift signals a more urgent intervention
    df = drift_engine.apply_drift_filter(df, rec_col='Action')

    logger.info(f"run_v6_overlay complete — {len(df)} positions processed")
    return df
