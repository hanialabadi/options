# phase10_revalidate_pipeline.py
# 🔁 Full Trade Revalidation Pipeline (Triggered by REVALIDATE flags)

import pandas as pd
from datetime import datetime
from core.tradier_chain import update_greeks_for_active
from core.chart_engine import run_phase8_chart_engine
from core.pcs_engine_v3_unified import score_pcs_batch
from core.rec_engine_v6_overlay import run_v6_overlay

# === Optional Utility ===
def market_is_open():
    now = datetime.now()
    weekday = now.weekday()  # Monday = 0
    if weekday >= 5:
        return False
    current_time = now.time()
    market_open = current_time >= datetime.strptime("06:30", "%H:%M").time()
    market_close = current_time <= datetime.strptime("13:00", "%H:%M").time()
    return market_open and market_close

def run_full_revalidation_pipeline():
    if not market_is_open():
        print("📴 Market is closed. Skipping revalidation.")
        return

    print("🔁 Starting revalidation for flagged trades...")
    df = pd.read_csv("/Users/haniabadi/Documents/Windows/Optionrec/active_master.csv")

    if "Rec_V6" not in df.columns:
        print("⚠️ Rec_V6 column missing. Run initial overlay first.")
        return

    df_to_revalidate = df[df["Rec_V6"].astype(str).str.startswith("REVALIDATE")]
    if df_to_revalidate.empty:
        print("✅ No trades require revalidation.")
        return

    print(f"🔎 {len(df_to_revalidate)} trades flagged for revalidation.")

    # Step 1: Update Greeks
    print("📈 Updating Greeks...")
    update_greeks_for_active()

    # Step 2: Update chart signals
    print("📊 Updating chart patterns...")
    run_phase8_chart_engine()

    # Step 3: Re-score PCS
    print("📉 Recomputing PCS scores...")
    score_pcs_batch()

    # Step 4: Rerun V6 overlay
    print("🧠 Running V6 overlay...")
    run_v6_overlay()

    print("✅ Revalidation complete.")

if __name__ == "__main__":
    run_full_revalidation_pipeline()
