
import os
import sys

# Automatically add project root (parent of dashboard/) to sys.path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
sys.path.append(PROJECT_ROOT)


from core.phase3_5_freeze.freeze_greeks import freeze_core_greeks
from core.phase3_5_freeze.freeze_fields import freeze_additional_fields
from core.phase3_5_freeze.freeze_breakeven import compute_breakeven
from core.phase3_5_freeze.freeze_confidence import freeze_confidence_tier
from core.phase3_5_freeze.freeze_tags import tag_strategy_metadata
from core.phase3_5_freeze.freeze_earnings import tag_earnings_window
from core.phase3_5_freeze.freeze_validation import validate_freeze_snapshot
from core.phase3_5_freeze.freeze_legs_export import generate_legs_df

def phase3_5_fill_freeze_fields(df):
    print("📌 Phase 3.5: Running modular freeze pipeline...")
    df = freeze_core_greeks(df)
    df = freeze_additional_fields(df)
    df = compute_breakeven(df)
    df = freeze_confidence_tier(df)
    df = tag_strategy_metadata(df)
    df = tag_earnings_window(df)
    validate_freeze_snapshot(df)
    df_collapsed, legs_path = generate_legs_df(df)
    print("✅ Freeze complete. Returning collapsed strategy rows.")
    return df_collapsed
