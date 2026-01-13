import pandas as pd
import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(os.getcwd())

from core.data_contracts import load_active_master
from core.management_engine.pcs_live import pcs_engine_v3_2_strategy_aware

def audit_shop():
    print("🔍 Auditing SHOP Straddle Position...")
    
    # Load active trades
    df = load_active_master()
    
    # Filter for SHOP
    # In active_master.csv, the ticker is in the 'Underlying' column
    ticker_col = 'Underlying' if 'Underlying' in df.columns else 'Ticker'
    
    shop_df = df[df[ticker_col] == 'SHOP'].copy()
    
    if shop_df.empty:
        print(f"❌ SHOP not found in active_master.csv (searched column: {ticker_col})")
        # Try Symbol column as fallback for stock positions
        shop_df = df[df['Symbol'] == 'SHOP'].copy()
        if shop_df.empty:
            return

    print(f"📈 Found {len(shop_df)} legs for SHOP.")
    
    # Run the PCS engine logic
    # Note: The engine expects certain columns like Vega, Gamma, Delta, Theta, PCS, etc.
    # These should be in active_master.csv already from the latest snapshot.
    
    # Ensure required columns for PCS engine exist
    if 'PCS_Drift' not in shop_df.columns:
        if 'PCS' in shop_df.columns and 'PCS_Entry' in shop_df.columns:
            shop_df['PCS_Drift'] = shop_df['PCS'] - shop_df['PCS_Entry']
        else:
            shop_df['PCS_Drift'] = 0
            
    if 'Chart_CompositeScore' not in shop_df.columns:
        shop_df['Chart_CompositeScore'] = 0
        
    if 'Held_ROI%' not in shop_df.columns and '% Total G/L' in shop_df.columns:
        shop_df['Held_ROI%'] = shop_df['% Total G/L']

    audited_df = pcs_engine_v3_2_strategy_aware(shop_df)
    
    cols_to_show = [
        'TradeID', 'Strategy', 'Rec_Action', 'Rationale_Composite', 
        'PCS_UnifiedScore', 'Trade_Health_Tier', 'Strategy_Match_Rank',
        'Delta', 'Vega', 'Gamma', 'Theta'
    ]
    
    print("\n--- System Audit Results ---")
    print(audited_df[cols_to_show].to_string(index=False))
    
    print("\n--- Detailed Metrics ---")
    for _, row in audited_df.iterrows():
        print(f"\nTradeID: {row['TradeID']}")
        print(f"  Action: {row['Rec_Action']}")
        print(f"  Rationale: {row['Rationale_Composite']}")
        print(f"  Unified Score: {row['PCS_UnifiedScore']:.2f}")
        print(f"  Health: {row['Trade_Health_Tier']}")
        print(f"  Persona Match: {row['Strategy_Match_Rank']}")
        print(f"  Greeks: Delta={row['Delta']:.4f}, Vega={row['Vega']:.4f}, Gamma={row['Gamma']:.4f}, Theta={row['Theta']:.4f}")

if __name__ == "__main__":
    audit_shop()
