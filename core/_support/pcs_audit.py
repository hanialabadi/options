import pandas as pd
import numpy as np
from datetime import datetime

from phase3_pcs_score import calculate_pcs
from phase2_parse import phase2_parse_symbols, unify_trade_ids, phase21_strategy_tagging
from pcs_engine_v3_unified import pcs_engine_v3_2_strategy_aware

# === Load base CSV
df = pd.read_csv("/Users/haniabadi/Documents/Windows/Optionrec/active_master.csv")

# === Phase 2 Parse & Strategy Tagging Pipeline
df = phase2_parse_symbols(df)
df = unify_trade_ids(df)  # ✅ CRITICAL FIX: ensures both legs share same TradeID
df = phase21_strategy_tagging(df, debug=True)

# === PCS & Scoring
df = calculate_pcs(df)
df["PCS_Entry"] = df["PCS"]
df = pcs_engine_v3_2_strategy_aware(df)

# === Audit fields for inspection
audit_cols = [
    "Symbol", "TradeID", "Underlying", "OptionType", "Strategy", "Strategy_Tier",
    "PCS", "PCS_Entry", "PCS_UnifiedScore", "PCS_PersonaScore", "PCS_Tier",
    "Gamma", "Vega", "IVHV_Gap_Entry", "Chart_Trend", "Rec_Action",
    "Needs_Revalidation", "PCS_Drift", "Trade_Health_Tier"
]
audit_df = df[audit_cols].copy()

# === Diagnostic: print full TradeID groups for multi-leg
target_ids = ["CRM250822_Straddle", "BKNG250801_Strangle"]
for tid in target_ids:
    print(f"\n🧠 TradeID Debug: {tid}")
    print(df[df["TradeID"] == tid][["Symbol", "TradeID", "OptionType", "Strike", "Strategy", "PCS", "PCS_UnifiedScore"]])

# === Filter and view all straddles/strangles
multi_leg = audit_df[audit_df["Strategy"].str.contains("Straddle|Strangle", na=False)]

print("\n✅ PCS Strategy Audit Complete")
print(multi_leg.sort_values("PCS_UnifiedScore", ascending=False).to_string(index=False))

# === Optional export
multi_leg.to_csv("pcs_audit_output.csv", index=False)
