def validate_freeze_snapshot(df):
    required_cols = ["PCS", "Confidence Tier", "Premium", "Skew_Entry", "Kurtosis_Entry"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"❌ Missing column: {col} in Phase 3.5 freeze")
        if df[col].isnull().any():
            raise ValueError(f"❌ Null values found in required column: {col}")
    return df