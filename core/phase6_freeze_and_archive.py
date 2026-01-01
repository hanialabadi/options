from core.data_contracts import save_active_master, ACTIVE_MASTER_PATH
from core.phase6_freeze.freeze_merge_master import merge_master
from core.phase6_freeze.evaluate_leg_status import evaluate_leg_status

def phase6_freeze_and_archive(df, df_master_current):
    print("📦 Starting Phase 6 modular freeze and archive...")

    df_master = merge_master(df, df_master_current)
    # Note: legs_dir still hardcoded - will be moved to config in Phase B
    df_master = evaluate_leg_status(df_master, legs_dir="/Users/haniabadi/Documents/Windows/Optionrec/legs")
    save_active_master(df_master)  # Use data contract

    return df_master
