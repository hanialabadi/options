def compute_pcs_drift(df):
    df["PCS_Drift"] = df["PCS"] - df["PCS_Entry"]
    df["Flag_PCS_Drift"] = df["PCS_Drift"].abs() > 15
    return df

def compute_vega_roc(df):
    df["Days_Held"] = df["Days_Held"].replace(0, 1)
    df["Vega_ROC"] = (df["Vega"] - df["Vega_Entry"]) / df["Days_Held"]
    df["Flag_Vega_Flat"] = df["Vega_ROC"] < 0
    return df
