
# # run_pipeline_debug.py
# import argparse
# import pandas as pd
# from datetime import datetime
# import os

# # === Core Imports ===
# from core.phase1_clean import phase1_load_and_clean_raw_v2
# from core.phase2_parse import phase2_parse_symbols, phase21_strategy_tagging
# from core.phase3_pcs_score import calculate_pcs, calculate_ivhv_gap, calculate_skew_and_kurtosis
# from core.phase3_5_freeze_fields import phase3_5_fill_freeze_fields
# from core.phase6_freeze_and_archive import phase6_freeze_and_archive
# from core.phase6_5 import phase6_5_inject_derived_fields
# from core.phase7_drift_engine import run_phase7_drift_engine
# from core.chart_engine import run_phase8_chart_engine
# from core.rec_engine_v6_overlay import run_v6_overlay

# # === CLI Entrypoint ===
# def run_pipeline(input_path, mode="full", debug=False):
#     snapshot_dir = "/Users/haniabadi/Documents/Windows/Optionrec/drift"
#     master_path = "/Users/haniabadi/Documents/Windows/Optionrec/active_master.csv"

#     print(f"🚀 Starting pipeline | Mode: {mode} | Input: {input_path}")

#     if mode in ["full", "freeze"]:
#         df, snapshot_path = phase1_load_and_clean_raw_v2(input_path=input_path, snapshot_dir=snapshot_dir)
#         print(f"✅ Phase 1 complete: {df.shape}")

#         df = phase2_parse_symbols(df)
#         df = phase21_strategy_tagging(df)
#         print(f"✅ Phase 2 complete | Multi-leg TradeIDs: {df['TradeID'].nunique()}")

#         if debug:
#             print("🔍 Multi-leg Trade Check:")
#             print(df["TradeID"].value_counts().value_counts())

#         df = calculate_pcs(df)
#         df = calculate_ivhv_gap(df)
#         df = calculate_skew_and_kurtosis(df)
#         print("✅ Phase 3 complete: PCS & IVHV scored")

#         df = phase3_5_fill_freeze_fields(df)
#         print("✅ Phase 3.5 Freeze complete")

#         df_master_current = pd.read_csv(master_path) if os.path.exists(master_path) else pd.DataFrame()
#         df_master = phase6_freeze_and_archive(df, df_master_current)
#         print("✅ Phase 6 merge complete")

#         df_master = phase6_5_inject_derived_fields(df_master, save_path=master_path)
#         print("✅ Phase 6.5 Drift + Outcome tagging complete")

#     if mode in ["full", "drift"]:
#         drift_export = f"/Users/haniabadi/Documents/Windows/Optionrec/drift_audits/drift_audit_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
#         df_drift = run_phase7_drift_engine(
#             drift_dir=snapshot_dir, export_csv=drift_export, update_master=True
#         )
#         print("✅ Phase 7 Drift Engine complete")

#     if mode in ["full", "chart"]:
#         run_phase8_chart_engine(master_path=master_path)
#         print("✅ Phase 8 Chart Engine complete")

#     if mode in ["full", "v6"]:
#         df_final = run_v6_overlay(df_master)
#         print("✅ Phase 9 V6 Overlay complete")
#         cols = ["TradeID", "PCS", "Rec_V6"]
#         if "Success_Prob" in df_final.columns:
#             cols.append("Success_Prob")

#         print(df_final[cols].tail())

#     print("🎉 Pipeline execution complete!")

# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="🔁 Run Full Trade Pipeline")
#     parser.add_argument("--input", type=str, required=True, help="Path to raw broker file (CSV)")
#     parser.add_argument("--mode", type=str, default="full", choices=["full", "freeze", "drift", "chart", "v6"], help="Execution mode")
#     parser.add_argument("--debug", action="store_true", help="Enable debug outputs")

#     args = parser.parse_args()
#     run_pipeline(input_path=args.input, mode=args.mode, debug=args.debug)

# cli/run_pipeline_audit.py
import sys
import os
import pandas as pd

# 🛠 Fix import path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core.phase6_freeze.flatten_snapshot_per_tradeid import flatten_snapshot_per_tradeid
from core.phase6_freeze.freezer_modules.freeze_all_entry_fields import freeze_all_entry_fields

# === Load Flattened Snapshot ===
FLATTENED_PATH = "/Users/haniabadi/Documents/Windows/Optionrec/drift/2025-08-05/flattened_2025-08-05_21-05-21.csv"
df_flat = pd.read_csv(FLATTENED_PATH)
print(f"📥 Loaded flattened snapshot: {df_flat.shape}")

# === Mock: Mark all rows as new ===
df_flat["IsNewTrade"] = True

# === Freeze all _Entry fields ===
df_frozen = freeze_all_entry_fields(df_flat, mode="flat")

# ✅ Show sample
print("✅ Frozen entries sample:")
print(df_frozen[[
    "TradeID", "Strategy", "Delta_Entry", "Vega_Entry", "Premium_Entry", "IV_Entry"
]])

# 💾 Save result
FROZEN_PATH = FLATTENED_PATH.replace("flattened", "frozen")
df_frozen.to_csv(FROZEN_PATH, index=False)
print(f"💾 Frozen snapshot saved: {FROZEN_PATH}")
