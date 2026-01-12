
import pandas as pd
from datetime import datetime

def merge_master(df_new: pd.DataFrame, df_current: pd.DataFrame) -> pd.DataFrame:
    """
    Phase 6: Merge current snapshot with master, preserving immutable _Entry fields.
    
    Key behavior:
    - Detects new trades (TradeID not in master)
    - Preserves _Entry fields for existing trades (immutability)
    - Identifies closed trades (in master, not in current)
    - Maintains leg-level granularity (no flattening)
    
    Entry fields are frozen per leg, not per TradeID.
    Multi-leg trades: each leg has independent _Entry values.
    """
    print("\n🧊 Phase 6: Merging current snapshot with master...")

    if "TradeID" not in df_new.columns:
        raise ValueError("❌ 'TradeID' column missing in new DataFrame")

    df_new = df_new.copy()
    df_current = df_current.copy() if df_current is not None and not df_current.empty else pd.DataFrame(columns=df_new.columns)

    # Detect closed trades (in master but not in current snapshot)
    if not df_current.empty:
        closed_ids = df_current[~df_current["TradeID"].isin(df_new["TradeID"])].copy()
        closed_ids["IsClosed"] = True
        print(f"📤 Closed trades: {len(closed_ids['TradeID'].unique())} TradeIDs, {closed_ids.shape[0]} legs")
    else:
        closed_ids = pd.DataFrame(columns=df_new.columns)

    # Identify all _Entry fields to preserve
    entry_cols = [col for col in df_new.columns if col.startswith("Entry_") or col.endswith("_Entry")]
    if "TradeDate" in df_new.columns:
        entry_cols.append("TradeDate")

    # Build unique leg identifier for multi-leg position tracking
    # Use TradeID + Symbol (or TradeID + OptionType + Strike if Symbol not unique)
    if "Symbol" in df_new.columns:
        df_new["_LegKey"] = df_new["TradeID"] + "_" + df_new["Symbol"].astype(str)
        if not df_current.empty:
            df_current["_LegKey"] = df_current["TradeID"] + "_" + df_current["Symbol"].astype(str)
    else:
        # Fallback: use TradeID + row index (less robust)
        df_new["_LegKey"] = df_new["TradeID"]
        if not df_current.empty:
            df_current["_LegKey"] = df_current["TradeID"]

    # Preserve _Entry fields from existing legs
    if not df_current.empty and entry_cols:
        existing_legs = df_current[df_current["_LegKey"].isin(df_new["_LegKey"])]
        
        for col in entry_cols:
            if col in df_current.columns and col in df_new.columns:
                # Map existing _Entry values back to matching legs
                entry_map = existing_legs.set_index("_LegKey")[col].to_dict()
                df_new[col] = df_new["_LegKey"].map(entry_map).combine_first(df_new[col])
        
        print(f"✅ Preserved {len(entry_cols)} _Entry fields for {len(existing_legs)} existing legs")

    # Mark new vs existing
    if not df_current.empty:
        df_new["IsNewTrade"] = ~df_new["_LegKey"].isin(df_current["_LegKey"])
    else:
        df_new["IsNewTrade"] = True

    # Combine active + closed
    df_merged = pd.concat([df_new, closed_ids], ignore_index=True)
    
    # Clean up temporary key
    df_merged.drop(columns=["_LegKey"], inplace=True, errors="ignore")

    print(f"📥 Total after merge: {df_merged.shape[0]} legs ({len(df_merged['TradeID'].unique())} TradeIDs)")
    print(f"   New: {df_merged['IsNewTrade'].sum() if 'IsNewTrade' in df_merged.columns else 0} legs")
    return df_merged
