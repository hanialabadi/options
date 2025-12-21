
import pandas as pd
from datetime import datetime

def merge_master(df_new: pd.DataFrame, df_current: pd.DataFrame) -> pd.DataFrame:
    """
    Merge new frozen trades with current master.
    Detect closed trades, retain old Entry fields where applicable.
    """
    print("\n🧊 Phase 6.1: Merging current snapshot with master...")

    if "TradeID" not in df_new.columns:
        raise ValueError("❌ 'TradeID' column missing in new DataFrame")

    df_new = df_new.copy()
    df_current = df_current.copy() if df_current is not None else pd.DataFrame(columns=df_new.columns)

    # Detect closed trades
    if not df_current.empty:
        closed_ids = df_current[~df_current["TradeID"].isin(df_new["TradeID"])]
        print(f"📤 Closed trades: {closed_ids.shape[0]}")
    else:
        closed_ids = pd.DataFrame(columns=df_new.columns)

    # Detect all Entry_* fields to preserve
    entry_cols = [col for col in df_new.columns if col.startswith("Entry_") or col.endswith("_Entry")]
    if "TradeDate" in df_new.columns:
        entry_cols.append("TradeDate")

    # Preserve entry fields from df_current if TradeID matches
    for tid in df_new["TradeID"].unique():
        if tid in df_current["TradeID"].values:
            for col in entry_cols:
                if col in df_new.columns and col in df_current.columns:
                    old_val = df_current.loc[df_current["TradeID"] == tid, col].values[0]
                    df_new.loc[df_new["TradeID"] == tid, col] = old_val

    # Combine with closed trades
    df_merged = pd.concat([df_new, closed_ids], ignore_index=True)
    df_merged = df_merged.drop_duplicates(subset="TradeID", keep="first")

    print(f"📥 Total trades after merge: {df_merged.shape[0]}")
    return df_merged
