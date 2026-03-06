import pandas as pd
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

def reduce_trade_states(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cycle-3 Preprocessing: Trade-Level State Reducer
    Converts per-leg states -> per-trade states deterministically.
    
    Required Aggregations:
    - Assignment Risk: MAX severity (IMMINENT > ELEVATED > LOW > UNKNOWN)
    - Greek Dominance: MAX abs exposure (GAMMA_DOMINANT > THETA_DOMINANT > BALANCED > UNKNOWN)
    - Volatility State: Consensus -> else CONFLICT
    - Trend Integrity: Worst-case (NO_TREND > TREND_EXHAUSTED > WEAK_TREND > STRONG_TREND > UNKNOWN)
    - Compression: Highest maturity (POST_EXPANSION > RELEASING > MATURE_COMPRESSION > EARLY_COMPRESSION > UNKNOWN)
    - Momentum: Strongest magnitude (REVERSING > ACCELERATING > DECELERATING > STALLING > UNKNOWN)
    """
    if df.empty:
        return df
        
    if "TradeID" not in df.columns:
        logger.warning("TradeID not found in DataFrame. Skipping trade state reduction.")
        return df

    # Define severity rankings
    SEVERITY = {
        "AssignmentRisk": ["UNKNOWN", "LOW", "ELEVATED", "IMMINENT"],
        "GreekDominance": ["UNKNOWN", "BALANCED", "THETA_DOMINANT", "GAMMA_DOMINANT"],
        "TrendIntegrity": ["UNKNOWN", "STRONG_TREND", "WEAK_TREND", "TREND_EXHAUSTED", "NO_TREND"],
        "Compression": ["UNKNOWN", "EARLY_COMPRESSION", "MATURE_COMPRESSION", "RELEASING", "POST_EXPANSION"],
        "Momentum": ["UNKNOWN", "STALLING", "DECELERATING", "ACCELERATING", "REVERSING"]
    }

    def get_max_severity(states: List[str], dimension: str) -> str:
        rank = SEVERITY.get(dimension, [])
        if not states:
            return "UNKNOWN"
        # Filter out UNKNOWN if there are other states
        valid_states = [s for s in states if s in rank and s != "UNKNOWN"]
        if not valid_states:
            return "UNKNOWN"
        return max(valid_states, key=lambda x: rank.index(x))

    def get_consensus(states: List[str]) -> str:
        if not states:
            return "UNKNOWN"
        unique_states = set(s for s in states if s != "UNKNOWN")
        if len(unique_states) == 0:
            return "UNKNOWN"
        if len(unique_states) == 1:
            return list(unique_states)[0]
        return "CONFLICT"

    # Group by TradeID
    trade_groups = df.groupby("TradeID")
    
    trade_states = []
    for trade_id, group in trade_groups:
        # Assignment Risk
        risk_states = group["AssignmentRisk_State"].tolist()
        trade_risk = get_max_severity(risk_states, "AssignmentRisk")
        
        # Greek Dominance
        greek_states = group["GreekDominance_State"].tolist()
        trade_greek = get_max_severity(greek_states, "GreekDominance")
        
        # Volatility State
        vol_states = group["VolatilityState_State"].tolist()
        trade_vol = get_consensus(vol_states)
        
        # Trend Integrity
        trend_states = group["TrendIntegrity_State"].tolist()
        trade_trend = get_max_severity(trend_states, "TrendIntegrity")
        
        # Compression
        comp_states = group["CompressionMaturity_State"].tolist()
        trade_comp = get_max_severity(comp_states, "Compression")
        
        # Momentum
        mom_states = group["MomentumVelocity_State"].tolist()
        trade_mom = get_max_severity(mom_states, "Momentum")
        
        trade_states.append({
            "TradeID": trade_id,
            "Trade_State_AssignmentRisk": trade_risk,
            "Trade_State_GreekDominance": trade_greek,
            "Trade_State_VolatilityState": trade_vol,
            "Trade_State_TrendIntegrity": trade_trend,
            "Trade_State_Compression": trade_comp,
            "Trade_State_Momentum": trade_mom,
            "Trade_State_Conflict": trade_vol == "CONFLICT",
            "Structural_Data_Complete": group["Structural_Data_Complete"].all(),
            "Resolution_Reason": group["PriceStructure_Resolution_Reason"].iloc[0]
        })
        
    trade_df = pd.DataFrame(trade_states)
    
    # Merge back to original df or return as summary? 
    # Usually we want to enrich the original df with trade-level states
    return df.merge(trade_df, on="TradeID", how="left")
