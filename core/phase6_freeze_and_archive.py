from core.phase6_freeze.freeze_merge_master import merge_master
from core.phase6_freeze.evaluate_leg_status import evaluate_leg_status
from core.phase6_freeze.freeze_archive_export import save_master

def phase6_freeze_and_archive(df, df_master_current):
    print("📦 Starting Phase 6 modular freeze and archive...")

    df_master = merge_master(df, df_master_current)
    df_master = evaluate_leg_status(df_master, legs_dir="/Users/haniabadi/Documents/Windows/Optionrec/legs")
    save_master(df_master, path="/Users/haniabadi/Documents/Windows/Optionrec/active_master.csv")

    return df_master
