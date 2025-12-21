from core.phase6_freeze.utils.freeze_helpers import detect_new_trades, assert_immutable_entry_fields
from core.phase6_freeze.freezer_modules.freeze_entry_greeks import freeze_entry_greeks
from core.phase6_freeze.freezer_modules.freeze_entry_premium import freeze_entry_premium
from core.phase6_freeze.freezer_modules.freeze_entry_ivhv import freeze_entry_ivhv
from core.phase6_freeze.freezer_modules.freeze_entry_pcs_score import freeze_entry_pcs_score
from core.phase6_freeze.freezer_modules.freeze_entry_date import freeze_entry_date

def phase6_freeze_and_archive(df_flat, df_master):
    df_flat = df_flat.copy()

    # Step 1: Mark new trades
    df_flat["IsNewTrade"] = detect_new_trades(df_flat, df_master)

    # Step 2: Filter only new trades
    df_new = df_flat[df_flat["IsNewTrade"]].copy()

    if not df_new.empty:
        # Step 3: Apply all freezer modules
        df_new = freeze_entry_greeks(df_new)
        df_new = freeze_entry_premium(df_new)
        df_new = freeze_entry_ivhv(df_new)
        df_new = freeze_entry_pcs_score(df_new)
        df_new = freeze_entry_date(df_new)

        # Step 4: Merge frozen fields back into df_flat
        frozen_cols = [col for col in df_new.columns if col.endswith("_Entry") or col in ["EntryDate", "DaysHeld"]]
        df_flat.update(df_new[["TradeID"] + frozen_cols].set_index("TradeID"))

    # Step 5: Drop helper column
    df_flat.drop(columns=["IsNewTrade"], inplace=True)

    # Step 6: Immutability check
    entry_fields = [
        "Delta_Entry", "Gamma_Entry", "Vega_Entry", "Theta_Entry",
        "Premium_Entry", "IV_Entry", "HV30_Entry",
        "PCS_Entry", "Confidence_Tier_Entry", "EntryDate"
    ]
    assert_immutable_entry_fields(df_flat, df_master, entry_fields)

    return df_flat
