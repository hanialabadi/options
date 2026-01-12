import pandas as pd

def detect_new_trades(df_flat: pd.DataFrame, df_master: pd.DataFrame) -> pd.Series:
    """
    Returns a boolean Series marking which rows are new trades.
    Works at leg level using TradeID + Symbol for multi-leg trades.
    """
    if df_master.empty:
        return pd.Series([True] * len(df_flat), index=df_flat.index)
    
    # Build leg-level key
    if "Symbol" in df_flat.columns:
        df_flat_key = df_flat["TradeID"] + "_" + df_flat["Symbol"].astype(str)
        df_master_key = df_master["TradeID"] + "_" + df_master["Symbol"].astype(str)
        existing_keys = set(df_master_key)
        return ~df_flat_key.isin(existing_keys)
    else:
        # Fallback to TradeID only
        existing_ids = set(df_master["TradeID"])
        return ~df_flat["TradeID"].isin(existing_ids)


def assert_immutable_entry_fields(df_current: pd.DataFrame, df_previous: pd.DataFrame, entry_fields: list):
    """
    Ensures that _Entry fields from df_previous are not accidentally overwritten in df_current.
    Works at leg level (TradeID + Symbol) for multi-leg trades.
    
    Raises ValueError if any mismatch is found.
    """
    if df_previous.empty:
        return  # Nothing to check

    # Build leg-level key for matching
    if "Symbol" in df_current.columns and "Symbol" in df_previous.columns:
        df_current["_CheckKey"] = df_current["TradeID"] + "_" + df_current["Symbol"].astype(str)
        df_previous["_CheckKey"] = df_previous["TradeID"] + "_" + df_previous["Symbol"].astype(str)
        
        # Find overlapping legs
        overlapping = df_current["_CheckKey"].isin(df_previous["_CheckKey"])
        
        if not overlapping.any():
            return  # No overlapping legs to check
        
        df_current_overlap = df_current[overlapping].set_index("_CheckKey")
        df_previous_map = df_previous.set_index("_CheckKey")
        
        for field in entry_fields:
            if field not in df_previous.columns or field not in df_current.columns:
                continue  # Field might not exist yet
            
            # Compare values for overlapping legs
            for key in df_current_overlap.index:
                if key in df_previous_map.index:
                    current_val = df_current_overlap.loc[key, field]
                    previous_val = df_previous_map.loc[key, field]
                    
                    # Handle NaN comparisons
                    if pd.isna(current_val) and pd.isna(previous_val):
                        continue
                    
                    if pd.notna(previous_val) and current_val != previous_val:
                        # Extract TradeID for error message
                        tradeid = key.split("_")[0] if "_" in key else key
                        raise ValueError(
                            f"❌ Immutability violation in '{field}' for leg '{key}'\n"
                            f"   Previous: {previous_val}, Current: {current_val}"
                        )
        
        # Cleanup temporary keys
        df_current.drop(columns=["_CheckKey"], inplace=True, errors="ignore")
        df_previous.drop(columns=["_CheckKey"], inplace=True, errors="ignore")
    else:
        # Fallback to TradeID-level checking (less precise for multi-leg)
        overlapping_ids = df_current["TradeID"].isin(df_previous["TradeID"])
        
        for field in entry_fields:
            if field not in df_previous.columns or field not in df_current.columns:
                continue
            
            current_subset = df_current.loc[overlapping_ids, ["TradeID", field]].set_index("TradeID")
            previous_subset = df_previous.set_index("TradeID").loc[current_subset.index, field]
            
            mismatches = (current_subset[field] != previous_subset).fillna(False)
            if mismatches.any():
                mismatch_ids = current_subset[mismatches].index.tolist()
                raise ValueError(
                    f"❌ Immutability violation in '{field}' for TradeID(s): {mismatch_ids}"
                )
