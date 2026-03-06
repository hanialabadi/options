import pandas as pd
import numpy as np
from pathlib import Path

def reconcile():
    master_path = Path('/Users/haniabadi/Documents/Windows/Optionrec/active_master.csv')
    if not master_path.exists():
        print(f"❌ Master file not found at {master_path}")
        return

    print(f"🔄 Reconciling strategy origin for: {master_path}")
    df = pd.read_csv(master_path)

    # 1. Define Reconciliation Targets
    # AAPL & SOFI: Restore BUY_WRITE (Unitary at 2025-12-29)
    # UUUU: Downgrade to COVERED_CALL (Temporal inversion)

    # AAPL Reconciliation
    aapl_mask = df['Underlying'] == 'AAPL'
    if aapl_mask.any():
        df.loc[aapl_mask, 'Strategy'] = 'Buy-Write'
        df.loc[aapl_mask, 'Entry_Structure'] = 'BUY_WRITE'
        print("  ✅ AAPL: Restored BUY_WRITE origin")

    # SOFI Reconciliation
    sofi_mask = df['Underlying'] == 'SOFI'
    if sofi_mask.any():
        df.loc[sofi_mask, 'Strategy'] = 'Buy-Write'
        df.loc[sofi_mask, 'Entry_Structure'] = 'BUY_WRITE'
        print("  ✅ SOFI: Restored BUY_WRITE origin")

    # UUUU Reconciliation
    uuuu_mask = df['Underlying'] == 'UUUU'
    if uuuu_mask.any():
        # Based on audit, UUUU270115C17 was entered 2025-12-04, Stock 2025-12-30.
        df.loc[uuuu_mask, 'Strategy'] = 'Covered Call'
        df.loc[uuuu_mask, 'Entry_Structure'] = 'COVERED_CALL'
        
        # Special case for the Put leg which is a CSP
        csp_mask = (df['Symbol'] == 'UUUU260206P14')
        if csp_mask.any():
            df.loc[csp_mask, 'Strategy'] = 'Cash-Secured Put'
            df.loc[csp_mask, 'Entry_Structure'] = 'CSP'
            print("  ✅ UUUU: Downgraded to COVERED_CALL (Non-unitary), CSP leg preserved")

    # 2. Save changes
    backup_path = master_path.with_suffix(f'.reconcile_backup.csv')
    df.to_csv(backup_path, index=False)
    df.to_csv(master_path, index=False)
    print(f"✅ Reconciliation complete. Saved to {master_path}")
    print(f"📦 Backup created at {backup_path}")

if __name__ == '__main__':
    reconcile()
