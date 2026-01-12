from core.data_contracts import save_active_master
from core.phase6_freeze.freezer_modules.freeze_merge_master import merge_master
from core.phase6_freeze.evaluate_leg_status import evaluate_leg_status
from core.phase6_freeze.freezer_modules.freeze_entry_greeks import freeze_entry_greeks
from core.phase6_freeze.freezer_modules.freeze_entry_premium import freeze_entry_premium
from core.phase6_freeze.utils.freeze_helpers import assert_immutable_entry_fields

def phase6_freeze_and_archive(df, df_master_current):
    """
    Phase 6: Trade Lifecycle Checkpoint + Entry Freeze Layer
    
    Responsibilities:
    1. Detect new/existing/closed trades (via merge_master)
    2. Freeze entry-time values for NEW trades only
    3. Enforce immutability of _Entry fields for existing trades
    4. Track multi-leg structure integrity (evaluate_leg_status)
    5. Preserve leg-level granularity (no flattening)
    6. Archive closed trades
    
    Does NOT:
    - Flatten multi-leg trades
    - Re-run enrichment or PCS (already in Phase 3)
    - Compute aggregates or drift (Phase 5 or analytics)
    - Call external APIs
    
    Entry fields frozen per leg (not per TradeID):
    - Greeks: Delta, Gamma, Vega, Theta
    - Premium
    - PCS, Capital_Deployed, Moneyness (if present)
    - DTE, IV/HV (if available)
    """
    print("📦 Phase 6: Lifecycle Checkpoint + Entry Freeze...")

    # Step 1: Merge with master (detects new/existing/closed, preserves _Entry immutability)
    df_master = merge_master(df, df_master_current)
    
    # Step 2: Freeze entry fields for NEW trades only
    if "IsNewTrade" in df_master.columns:
        df_new = df_master[df_master["IsNewTrade"] == True].copy()
        
        if not df_new.empty:
            print(f"🧊 Freezing entry fields for {len(df_new)} new legs...")
            
            # Apply freezer modules
            df_new = freeze_entry_greeks(df_new)
            df_new = freeze_entry_premium(df_new)
            
            # Freeze Phase 3 enrichment fields if present
            phase3_freeze_fields = {
                'PCS_Entry': 'PCS',
                'Capital_Deployed_Entry': 'Capital Deployed',
                'Moneyness_Pct_Entry': 'Moneyness_Pct',
                'DTE_Entry': 'DTE',
                'BreakEven_Entry': 'BreakEven'
            }
            
            for entry_col, source_col in phase3_freeze_fields.items():
                if source_col in df_new.columns:
                    df_new[entry_col] = df_new[source_col]
            
            # Merge frozen fields back using leg-level key (Symbol)
            if "Symbol" in df_new.columns:
                frozen_cols = [col for col in df_new.columns if col.endswith("_Entry")]
                merge_key = ["TradeID", "Symbol"]
                
                # Update df_master with frozen values for new legs
                for col in frozen_cols:
                    if col in df_new.columns:
                        df_master.loc[df_master["IsNewTrade"] == True, col] = df_new[col].values
    
    # Step 3: Track multi-leg structure integrity
    df_master = evaluate_leg_status(df_master)
    
    # Step 4: Enforce immutability for existing trades
    if not df_master_current.empty:
        entry_fields = [col for col in df_master.columns if col.endswith("_Entry")]
        if entry_fields:
            try:
                assert_immutable_entry_fields(df_master, df_master_current, entry_fields)
                print(f"✅ Immutability verified for {len(entry_fields)} _Entry fields")
            except ValueError as e:
                print(f"⚠️ Immutability warning: {e}")
    
    # Step 5: Save to master (via data_contracts)
    save_active_master(df_master)

    return df_master
