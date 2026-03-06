import pandas as pd
import numpy as np

def pcs_engine_v3_unified(df):
    """
    Unified PCS Engine with extreme safety guards.
    """
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        return df
        
    if not isinstance(df, pd.DataFrame):
        return df

    df = df.copy()

    # === Safety Defaults ===
    # Ensure critical columns exist to prevent KeyErrors during vector operations
    critical_cols = {
        "Vega": 0.0, "Gamma": 0.0, "Theta": 0.0, "Delta": 0.0, 
        "PCS": 75.0, "PCS_Drift": 0.0, "Chart_CompositeScore": 80.0, 
        "Held_ROI%": 0.0, "Days_Held": 1, "Exit_Flag": False, 
        "Chart_Trend": "Unknown", "Strategy": "Unknown"
    }
    for col, default in critical_cols.items():
        if col not in df.columns:
            df[col] = default
        else:
            df[col] = df[col].fillna(default)

    # Use .get() even for columns we just ensured exist, for absolute safety
    df["ROI_Rate"] = df.get("Held_ROI%", 0) / df.get("Days_Held", 1).replace(0, 1)
    df["Chart_Support"] = ~df.get("Exit_Flag", False) & (df.get("Chart_Trend", "Unknown") != "Broken")

    # === 📊 Composite Signal Score ===
    df["PCS_SignalScore"] = (
        0.3 * df.get("Vega", 0) +
        0.2 * df.get("Gamma", 0) +
        0.2 * df.get("ROI_Rate", 0) +
        0.2 * df.get("Chart_CompositeScore", 0) +
        0.1 * df.get("Delta", 0)
    )

    # === 🎯 Unified Health Score (PCS_UnifiedScore) ===
    df["PCS_UnifiedScore"] = (
        0.5 * df.get("PCS", 0) +
        0.3 * df.get("PCS_SignalScore", 0) +
        0.2 * df.get("Chart_CompositeScore", 0)
    )

    # === 🚦 Inflection Detection (Decision-Causal & Persistence-Gated)
    strategy_ser = df.get("Strategy", pd.Series(["Unknown"]*len(df))).str.lower()
    is_csp = strategy_ser.str.contains("csp|cash-secured put", na=False)
    is_directional = strategy_ser.str.contains("call|directional", na=False)
    is_income = strategy_ser.str.contains("income", na=False) & ~is_csp
    
    # Momentum Rollover (Exhaustion) - Requires multi-bar confirmation
    rsi_ser = df.get("RSI", df.get("rsi", pd.Series([50.0]*len(df))))
    mom_slope = df.get("momentum_slope", pd.Series([0.0]*len(df)))
    accel_ser = df.get("price_acceleration", pd.Series([0.0]*len(df)))
    # Rollover confirmed if RSI is high AND momentum is negative AND accelerating downward
    df["Momentum_Rollover"] = (rsi_ser > 70) & (mom_slope < 0) & (accel_ser < 0)
    
    # Volatility Expansion (Sinclair) - Decision-causal regime shift
    vol_state = df.get("VolatilityState_State", pd.Series(["NORMAL"]*len(df)))
    df["Vol_Expansion"] = vol_state.isin(["EXPANDING", "EXTREME"])
    
    # Structural Invalidation (Murphy) - Hard gate for thesis collapse
    struct_state = df.get("PriceStructure_State", pd.Series(["STABLE"]*len(df)))
    df["Structural_Invalidation"] = struct_state == "STRUCTURE_BROKEN"

    # === 💎 CSP Continuation Value (Expectancy Aggregator)
    # Usefulness = max(Further theta decay, Reversion capture, Assignment into CC)
    premium_now = df.get("Last", pd.Series([1.0]*len(df))).abs()
    premium_entry = df.get("Premium_Entry", pd.Series([1.0]*len(df))).abs()
    df["Premium_Captured_Pct"] = (1 - (premium_now / premium_entry.replace(0, 1))) * 100
    
    # Opportunity Cost Proxy: If < 10% premium remains, continuation value is low
    df["Low_Continuation_Value"] = df["Premium_Captured_Pct"] > 90
    
    # Reversion Probability: Collapses if structure breaks or momentum is parabolic without rollover
    df["Reversion_Prob_Collapse"] = df["Structural_Invalidation"] | ((rsi_ser > 80) & ~df["Momentum_Rollover"])

    # === 🚦 Persona Violation Flag
    vega_ser = df.get("Vega", 0)
    theta_ser = df.get("Theta", 0)
    
    df["Persona_Violation"] = (
        (is_directional & (vega_ser < 0.2)) |
        ((is_income | is_csp) & (theta_ser.abs() < vega_ser.abs()))
    )

    # === 🔄 Recovery Bias Tag
    df["Recovery_Bias"] = (
        (vega_ser.abs() > 0.2) &
        (df.get("PCS_Drift", 0) < 10) &
        (df.get("Chart_Support", True))
    )

    # === 📌 Recommendation Logic ===
    def decide(row):
        strategy = str(row.get("Strategy", "Unknown")).lower()
        is_csp_row = "csp" in strategy or "cash-secured put" in strategy
        
        pcs_unified = row.get("PCS_UnifiedScore", 0)
        pcs_drift = row.get("PCS_Drift", 0)
        
        # 1. Hard Exit: Structural Invalidation (Thesis Collapse)
        if row.get("Structural_Invalidation", False):
            return "EXIT"
            
        # 2. CSP Specific Logic: Expectancy Preservation (Triple-Gate TRIM)
        if is_csp_row:
            # Upward drift is success, not risk.
            # TRIM only fires if TRIPLE GATE is met:
            # Reversion probability collapses AND Premium left < opportunity cost AND Vol expansion
            if (row.get("Reversion_Prob_Collapse", False) and 
                row.get("Low_Continuation_Value", False) and 
                row.get("Vol_Expansion", False)):
                return "TRIM"
            
            # If momentum rollover is confirmed (Exhaustion), we might exit to preserve captured edge
            if row.get("Momentum_Rollover", False) and row.get("Vol_Expansion", False):
                return "EXIT_TO_PRESERVE_EDGE"
            
            # Resolve to HOLD_FOR_REVERSION if price is extended but structure is intact
            # This overrides raw PCS drift interpretation
            if pcs_drift > 20:
                return "HOLD_FOR_REVERSION"
                
            return "HOLD"

        # 3. Default Logic for other strategies
        if pcs_unified < 60 or pcs_drift > 20:
            return "EXIT"
        if not row.get("Chart_Support", True):
            return "REVALIDATE"
        if row.get("Persona_Violation", False):
            return "REVALIDATE"
        if pcs_unified < 70:
            return "TRIM"
        return "HOLD"

    df["Rec_Action"] = df.apply(decide, axis=1)

    # === 📣 Composite Rationale
    def explain(row):
        rec_action = row.get("Rec_Action", "HOLD")
        if rec_action == "EXIT":
            if row.get("Structural_Invalidation", False):
                return "Structural invalidation (Murphy)"
            return "PCS breakdown or heavy drift"
        if rec_action == "EXIT_TO_PRESERVE_EDGE":
            return "Momentum rollover + Vol expansion; preserving captured edge"
        if rec_action == "HOLD_FOR_REVERSION":
            return "Extended success; holding for premium regeneration"
        if rec_action == "REVALIDATE":
            if row.get("Persona_Violation", False):
                return "Strategy mismatch or chart failure"
            return "Signal drift or uncertain trend"
        if rec_action == "TRIM":
            if row.get("is_csp", False):
                return "Triple-gate met: Reversion collapse + Low value + Vol expansion"
            return "Signal weakening below Tier 1"
        return "Edge intact"

    df["Rationale_Composite"] = df.apply(explain, axis=1)

    # === Tiers
    tier_map = {
        "EXIT": 1, 
        "EXIT_TO_PRESERVE_EDGE": 1,
        "REVALIDATE": 2, 
        "TRIM": 3, 
        "HOLD": 4,
        "HOLD_FOR_REVERSION": 4
    }
    df["Rec_Tier"] = df.get("Rec_Action", "HOLD").map(tier_map).fillna(4).astype(int)

    # === Health Buckets
    def health_tier(score):
        if score < 60: return "Broken"
        elif score < 70: return "At Risk"
        elif score < 80: return "Valid"
        return "Strong"

    df["Trade_Health_Tier"] = df.get("PCS_UnifiedScore", 0).apply(health_tier)

    return df
