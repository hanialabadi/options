import pandas as pd
import numpy as np

def tag_earnings_flags(df: pd.DataFrame) -> pd.DataFrame:
    today = pd.to_datetime("today").normalize()

    # Normalize column naming
    if "Earnings_Date" not in df.columns and "Earnings Date" in df.columns:
        df["Earnings_Date"] = df["Earnings Date"]

    if "Earnings_Date" in df.columns:
        df["Earnings_Date"] = pd.to_datetime(df["Earnings_Date"], errors="coerce")
        df["Days_to_Earnings"] = (df["Earnings_Date"] - today).dt.days

        df["Is_Event_Setup"] = (
            df["Strategy"].str.contains("Straddle|Strangle", na=False) &
            (df["Vega"] > 2.0) &
            (df["Days_to_Earnings"].between(0, 7, inclusive="both"))
        )

        # Optional reason tag for audit/debug
        df["Event_Reason"] = np.where(
            df["Is_Event_Setup"], "Straddle+Vega+Earnings <7d", ""
        )
    else:
        df["Days_to_Earnings"] = np.nan
        df["Is_Event_Setup"] = False
        df["Event_Reason"] = ""

    return df
