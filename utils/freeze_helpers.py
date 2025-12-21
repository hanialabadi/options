import pandas as pd

def detect_new_trades(df_flat: pd.DataFrame, df_master: pd.DataFrame) -> pd.Series:
    """Returns a boolean Series marking which rows are new trades."""
    existing_ids = set(df_master["TradeID"]) if not df_master.empty else set()
    return ~df_flat["TradeID"].isin(existing_ids)


def assert_immutable_entry_fields(df_flat: pd.DataFrame, df_master: pd.DataFrame, entry_fields: list):
    """
    Ensures that _Entry fields in df_master are not accidentally overwritten in df_flat.
    Raises ValueError if any mismatch is found.
    """
    if df_master.empty:
        return  # Nothing to check

    overlapping_ids = df_flat["TradeID"].isin(df_master["TradeID"])
    for field in entry_fields:
        if field not in df_master.columns or field not in df_flat.columns:
            continue  # Field might not exist yet (new module)

        flat_vals = df_flat.loc[overlapping_ids, ["TradeID", field]].set_index("TradeID")
        master_vals = df_master.set_index("TradeID").loc[flat_vals.index, field]

        mismatches = (flat_vals[field] != master_vals).fillna(False)
        if mismatches.any():
            mismatch_ids = flat_vals[mismatches].index.tolist()
            raise ValueError(
                f"❌ Immutable violation in '{field}' for TradeID(s): {mismatch_ids}"
            )
