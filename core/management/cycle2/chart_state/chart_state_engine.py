import pandas as pd
import json
import logging
from .state_extractors.price_structure import compute_price_structure
from .state_extractors.trend_integrity import compute_trend_integrity
from .state_extractors.volatility_state import compute_volatility_state
from .state_extractors.compression_maturity import compute_compression_maturity
from .state_extractors.momentum_velocity import compute_momentum_velocity
from .state_extractors.directional_balance import compute_directional_balance
from .state_extractors.range_efficiency import compute_range_efficiency
from .state_extractors.timeframe_agreement import compute_timeframe_agreement
from .state_extractors.greek_dominance import compute_greek_dominance
from .state_extractors.assignment_risk import compute_assignment_risk
from .state_extractors.regime_stability import compute_regime_stability
from .state_extractors.recovery_quality import compute_recovery_quality
from .state_extractors.wave_phase import compute_wave_phase

logger = logging.getLogger(__name__)

def compute_chart_state(df: pd.DataFrame, client=None) -> pd.DataFrame:
    """
    Orchestrates the full-spectrum chart-derived technical measurement layer.
    Produces objective market states (deterministic, audit-ready).
    """
    if df.empty:
        return df
        
    df = df.copy()

    # --- Phase 1.3: Management-Owned Primitive Fetching ---
    from ..chart_primitives.compute_primitives import compute_chart_primitives
    df = compute_chart_primitives(df, client=client)
    
    state_functions = {
        "PriceStructure": compute_price_structure,
        "TrendIntegrity": compute_trend_integrity,
        "VolatilityState": compute_volatility_state,
        "CompressionMaturity": compute_compression_maturity,
        "MomentumVelocity": compute_momentum_velocity,
        "DirectionalBalance": compute_directional_balance,
        "RangeEfficiency": compute_range_efficiency,
        "TimeframeAgreement": compute_timeframe_agreement,
        "GreekDominance": compute_greek_dominance,
        "AssignmentRisk": compute_assignment_risk,
        "RegimeStability": compute_regime_stability,
        "RecoveryQuality": compute_recovery_quality,
    }
    
    from core.management.cycle1.identity.constants import STRATEGY_STOCK

    for name, func in state_functions.items():
        try:
            # RAG: Efficiency. Skip complex technical measurement for non-option stocks.
            # We only need PriceStructure for basic PnL/Drift tracking.
            if name != "PriceStructure":
                # Create a mask for positions that should be processed
                if "Strategy" in df.columns:
                    process_mask = df["Strategy"] != STRATEGY_STOCK
                else:
                    logger.warning("⚠️ Strategy column missing — processing all rows for chart state")
                    process_mask = pd.Series(True, index=df.index)
                
                # Initialize results with a default "SKIPPED" state for stocks
                results = pd.Series([None] * len(df), index=df.index)
                
                if process_mask.any():
                    results.loc[process_mask] = df.loc[process_mask].apply(func, axis=1)
                
                # Fill the rest with a dummy object that has the expected attributes
                class SkippedState:
                    def __init__(self):
                        self.state = "NOT_APPLICABLE"
                        self.raw_metrics = {}
                        self.resolution_reason = "STRATEGY_STOCK_SKIPPED"
                
                results.loc[~process_mask] = [SkippedState() for _ in range((~process_mask).sum())]
            else:
                # PriceStructure is required for all positions to maintain ledger integrity
                results = df.apply(func, axis=1)
            
            # Extract state and raw metrics using canonical naming convention
            df[f"{name}_State"] = results.apply(lambda x: x.state)
            df[f"{name}_Raw"] = results.apply(lambda x: json.dumps(x.raw_metrics))
            df[f"{name}_Resolution_Reason"] = results.apply(lambda x: x.resolution_reason)
            
            # Special handling for structural completeness
            if name == "PriceStructure":
                df["Structural_Data_Complete"] = results.apply(lambda x: x.data_complete)
                df["Resolution_Reason"] = results.apply(lambda x: x.resolution_reason)
            
        except Exception as e:
            logger.error(f"Error computing chart state {name}: {e}")
            df[f"{name}_State"] = "UNKNOWN"
            df[f"{name}_Raw"] = "{}"
            df[f"{name}_Resolution_Reason"] = "ENGINE_ERROR"
            if name == "PriceStructure":
                df["Structural_Data_Complete"] = False

    # ── WavePhase: runs AFTER all other extractors (reads their _State columns) ──
    try:
        if "Strategy" in df.columns:
            _wp_mask = df["Strategy"] != STRATEGY_STOCK
        else:
            _wp_mask = pd.Series(True, index=df.index)

        _wp_results = pd.Series([None] * len(df), index=df.index)
        if _wp_mask.any():
            _wp_results.loc[_wp_mask] = df.loc[_wp_mask].apply(compute_wave_phase, axis=1)

        class _WPSkipped:
            def __init__(self):
                self.state = "NOT_APPLICABLE"
                self.raw_metrics = {}
                self.resolution_reason = "STRATEGY_STOCK_SKIPPED"

        _wp_results.loc[~_wp_mask] = [_WPSkipped() for _ in range((~_wp_mask).sum())]

        df["WavePhase_State"] = _wp_results.apply(lambda x: x.state)
        df["WavePhase_Raw"] = _wp_results.apply(lambda x: json.dumps(x.raw_metrics))
        df["WavePhase_Resolution_Reason"] = _wp_results.apply(lambda x: x.resolution_reason)
    except Exception as e:
        logger.error(f"Error computing WavePhase: {e}")
        df["WavePhase_State"] = "UNKNOWN"
        df["WavePhase_Raw"] = "{}"
        df["WavePhase_Resolution_Reason"] = "ENGINE_ERROR"

    return df
