# ═════════════════════════════════════════════════════════════════════════════
# ⚠️  LEGACY / DO NOT IMPORT — DUPLICATE IMPLEMENTATION
# ═════════════════════════════════════════════════════════════════════════════
#
# This file is a DUPLICATE of an abandoned Phase 6 implementation.
# It is incomplete and unsafe.
#
# ✅ AUTHORITY: core/phase6_freeze_and_archive.py (Implementation A)
#
# This file is preserved for historical reference only.
# Do NOT import. Do NOT use. Do NOT modify.
# ═════════════════════════════════════════════════════════════════════════════

from core.phase6_freeze.utils.freeze_helpers import detect_new_trades, assert_immutable_entry_fields
from core.phase6_freeze.freezer_modules.freeze_entry_greeks import freeze_entry_greeks

def phase6_freeze_and_archive(df_flat, df_master):
    df_flat = df_flat.copy()

    # Step 1: Mark new trades
    df_flat["IsNewTrade"] = detect_new_trades(df_flat, df_master)

    # Step 2: Filter only new trades
    df_new = df_flat[df_flat["IsNewTrade"]].copy()

    # Step 3: Apply freezer module(s)
    df_new = freeze_entry_greeks(df_new)

    # Step 4: Merge frozen fields back into df_flat
    frozen_cols = [col for col in df_new.columns if col.endswith("_Entry")]
    df_flat.update(df_new[["TradeID"] + frozen_cols].set_index("TradeID"))

    # Step 5: Drop helper
    df_flat.drop(columns=["IsNewTrade"], inplace=True)

    # Optional: Safety check (for existing trades)
    entry_fields = ["Delta_Entry", "Gamma_Entry", "Vega_Entry", "Theta_Entry"]
    assert_immutable_entry_fields(df_flat, df_master, entry_fields)

    return df_flat
