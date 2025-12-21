import pandas as pd

def clean_rec_output(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure final recommendation columns are clean and safe for downstream display.
    """
    # Clean up rec_action with fallback
    df["Rec_Action"] = df.get("Rec_Action", "HOLD").fillna("HOLD")
    df["Rec_Tier"] = df.get("Rec_Tier", 3).fillna(3).astype(int)
    df["Confidence"] = df.get("Confidence", "Unknown").fillna("Unknown")

    # Drop intermediary columns if present
    drop_cols = [col for col in df.columns if col.startswith("Tmp_") or col.endswith("_debug")]
    df.drop(columns=drop_cols, errors="ignore", inplace=True)

    return df
