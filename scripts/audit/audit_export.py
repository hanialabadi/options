import pandas as pd
import numpy as np

def audit_csv(file_path):
    df = pd.read_csv(file_path)
    
    print("--- Acceptance Status Distribution ---")
    print(df['acceptance_status'].value_counts(dropna=False))
    print("\n")
    
    print("--- Missing Data Audit ---")
    critical_fields = [
        'IV_Rank_30D', 'IV_Rank_XS', 'iv_rank_available', 
        'Liquidity_Grade', 'Theory_Compliance_Score',
        'Delta', 'Bid', 'Ask'
    ]
    
    for field in critical_fields:
        if field in df.columns:
            missing_count = df[field].isna().sum()
            unknown_count = (df[field] == 'Unknown').sum() if df[field].dtype == object else 0
            print(f"{field}: Missing={missing_count}, Unknown={unknown_count}")
        else:
            print(f"{field}: COLUMN MISSING")
            
    print("\n--- Missing Required Data Column Summary ---")
    if 'Missing_Required_Data' in df.columns:
        print(df['Missing_Required_Data'].value_counts().head(10))
    
    print("\n--- Failure Reasons ---")
    if 'Failure_Reason' in df.columns:
        print(df['Failure_Reason'].value_counts().head(10))

    print("\n--- Acceptance Reasons for STRUCTURALLY_READY ---")
    sr_df = df[df['acceptance_status'] == 'STRUCTURALLY_READY']
    if not sr_df.empty:
        print(sr_df['acceptance_reason'].unique())

    print("\n--- Liquidity Issues ---")
    if 'Liquidity_Grade' in df.columns:
        print(df['Liquidity_Grade'].value_counts())
        print("\nTop Liquidity Reasons for 'Thin' or 'Illiquid':")
        print(df[df['Liquidity_Grade'].isin(['Thin', 'Illiquid'])]['Liquidity_Reason'].value_counts().head(5))

if __name__ == "__main__":
    # The file content was provided, I'll assume it's saved as 'temp_export.csv'
    audit_csv('temp_export.csv')
