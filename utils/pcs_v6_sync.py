import pandas as pd
from core.pcs_engine_v3_unified import score_pcs_batch
from core.rec_engine_v6_overlay import run_v6_overlay

def run_pcs_v6_sync_pipeline(master_path="/Users/haniabadi/Documents/Windows/Optionrec/active_master.csv", auto_fix=True):
    print("📁 Loading master data...")
    df = pd.read_csv(master_path)

    # Step 1: Ensure PCS scoring is present
    if "PCS_UnifiedScore" not in df.columns or not pd.api.types.is_numeric_dtype(df["PCS_UnifiedScore"]):
        if auto_fix:
            print("⚠️ PCS_UnifiedScore missing — running PCS scoring...")
            score_pcs_batch(master_path)
            df = pd.read_csv(master_path)
        else:
            raise ValueError("❌ PCS_UnifiedScore is missing or invalid. Cannot continue to overlay.")

    # Step 2: Run V6 overlay
    print("🧠 Running V6 overlay...")
    df = run_v6_overlay(master_path)

    # Step 3: Validate overlay fields exist
    required_cols = ["PCS_Live", "PCS_Health_Score", "Rec_V6", "Action"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"❌ Overlay failed — missing columns: {missing}")

    # Step 4: Add diagnostics (optional)
    df["PCS_Diff"] = df["PCS_Live"] - df.get("PCS_Entry", df["PCS_Live"])
    df["Scaling_Opportunity"] = df.get("Can_Scale", False).map({True: "🚀 SCALE", False: ""})

    # Step 4.5: Freeze integrity check (optional)
    if "PCS_Entry" in df.columns:
        df["PCS"] = df["PCS_Entry"]  # ❄️ Reinforce freeze

    df["Flag_PCS_Overwrite"] = (df["PCS"] != df["PCS_Entry"]) & df.get("Days_Held", 0).gt(0)
    
    # Step 5: Save
    df.to_csv(master_path, index=False)
    return df
