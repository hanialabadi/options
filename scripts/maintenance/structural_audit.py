import pandas as pd
import numpy as np

def perform_audit():
    # Use the active master as the source of truth
    master_path = '/Users/haniabadi/Documents/Windows/Optionrec/active_master.csv'
    print(f"Auditing file: {master_path}")
    
    df = pd.read_csv(master_path)
    
    # Ensure we have the necessary columns
    if 'Underlying' in df.columns and 'Ticker' not in df.columns:
        df['Ticker'] = df['Underlying']

    required_cols = [
        'Ticker', 'Symbol', 'AssetType', 'Quantity', 
        'Strategy', 'First_Seen_Date', 'TradeID'
    ]
    
    available_cols = [c for c in required_cols if c in df.columns]
    
    # Extract relevant data
    audit_df = df[available_cols].copy()
    
    # Ground Truth Mapping (from backfill scripts)
    ground_truth_dates = {
        'PYPL260123C60': '2026-01-08',
        'SOFI260123P25': '2026-01-06',
        'CVX260123C165': '2026-01-10',
        'SOFI260130C29': '2025-12-29',
        'AAPL260130C275': '2025-12-29',
        'UUUU260206P14': '2025-12-30',
        'SHOP260220C165': '2025-12-29',
        'MSCI260220C580': '2025-12-29',
        'TXN260220P185': '2026-01-06',
        'INTC260220C38': '2025-12-29',
        'QCOM260220P175': '2026-01-06',
        'AAPL260220C280': '2025-12-29',
        'VZ260227C40': '2026-01-15',
        'UUUU270115C17': '2025-12-04',
        'AAPL270115C260': '2025-12-29',
        'PLTR280121C250': '2025-12-10',
        'AMZN280121C220': '2025-12-15',
        'UUUU': '2025-12-30',
        'AAPL': '2025-12-29',
        'SOFI': '2025-12-29',
        'PYPL': '2024-03-01',
        'PLTR': '2025-11-10',
        'CVX': '2026-01-05',
        'INTC': '2024-12-24'
    }

    def get_entry_date(row):
        if row['Symbol'] in ground_truth_dates:
            return ground_truth_dates[row['Symbol']]
        if row['Ticker'] in ground_truth_dates and row['AssetType'] == 'STOCK':
            return ground_truth_dates[row['Ticker']]
        if pd.notna(row.get('First_Seen_Date')):
            return pd.to_datetime(row['First_Seen_Date']).date()
        return 'Unknown'

    audit_df['Entry_Date'] = audit_df.apply(get_entry_date, axis=1)

    # Group by Ticker and Entry_Date
    groups = audit_df.groupby(['Ticker', 'Entry_Date'])
    
    results = []
    
    for (ticker, entry_date), group in groups:
        stock_legs = group[group['AssetType'] == 'STOCK']
        option_legs = group[group['AssetType'] == 'OPTION']
        
        if option_legs.empty:
            continue # We are auditing option positions and their stock legs
            
        stock_qty = stock_legs['Quantity'].sum()
        option_qty = abs(option_legs['Quantity'].sum()) # Contracts
        
        # Classification logic
        # BUY_WRITE: unitary stock + option at inception
        # COVERED_CALL: option written against pre-existing stock
        # OPTION_ONLY: no stock leg at inception
        
        # Note: Since we are looking at a snapshot, "at inception" means 
        # they share the same Entry_Date in our records.
        
        has_stock = not stock_legs.empty
        has_option = not option_legs.empty
        
        # RAG: Absolute Authority. Strategy identity is an immutable inception property.
        # Order of precedence:
        # 1. Frozen Entry_Structure (Canonical)
        # 2. Inception Strategy (if present)
        # 3. Inference from legs (Fallback only)
        
        classification = "UNKNOWN"
        
        # 1. Check Entry_Structure (Tier 1 Authority)
        if 'Entry_Structure' in group.columns:
            valid_structures = group['Entry_Structure'].dropna().unique()
            specific = [s for s in valid_structures if s not in ['Unknown', 'STOCK', 'OPTION', 'UNKNOWN']]
            if specific:
                classification = specific[0]
        
        # 2. Check existing Strategy column (Tier 2 Authority)
        if classification == "UNKNOWN" and 'Strategy' in group.columns:
            existing_strats = group['Strategy'].dropna().unique()
            specific = [s for s in existing_strats if s not in ['Unknown', 'UNKNOWN', 'STOCK', 'OPTION']]
            if specific:
                classification = existing_strats[0]
        
        # 3. Fallback to inference (Tier 3)
        if classification == "UNKNOWN":
            if has_stock and has_option:
                classification = "BUY_WRITE"
            elif has_option:
                classification = "OPTION_ONLY"
        
        # Quantity alignment
        alignment = "N/A"
        if has_stock and has_option:
            # 100 shares <-> 1 contract
            expected_stock = option_qty * 100
            if abs(stock_qty) == expected_stock:
                alignment = f"MATCH ({int(abs(stock_qty))} shares ↔ {int(option_qty)} contracts)"
            else:
                alignment = f"MISMATCH ({int(abs(stock_qty))} shares ↔ {int(option_qty)} contracts)"
        
        # Flags
        flags = []
        
        # Flag: BUY_WRITE reclassified as COVERED_CALL
        # This happens if Entry_Structure was BUY_WRITE but Strategy is now Covered Call
        if 'Entry_Structure' in group.columns:
            if any(group['Entry_Structure'] == 'BUY_WRITE') and any(group['Strategy'] == 'Covered Call'):
                flags.append("RECLASSIFIED: BUY_WRITE -> COVERED_CALL")
        
        # Flag: Strategy origin collapse (e.g. stock leg missing from group but expected)
        if any(group['Strategy'] == 'Buy-Write') and stock_legs.empty:
            flags.append("COLLAPSE: Buy-Write missing stock leg")

        results.append({
            'Ticker': ticker,
            'Entry_Date': entry_date,
            'Classification': classification,
            'Stock_Legs': ", ".join(stock_legs['Symbol'].tolist()) if not stock_legs.empty else "None",
            'Option_Legs': ", ".join(option_legs['Symbol'].tolist()),
            'Alignment': alignment,
            'Flags': "; ".join(flags) if flags else "None"
        })
        
    # Output results
    report = pd.DataFrame(results)
    if not report.empty:
        print(report.to_markdown(index=False))
    else:
        print("No option positions found for audit.")

if __name__ == "__main__":
    perform_audit()
