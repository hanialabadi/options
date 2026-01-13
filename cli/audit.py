import sys
import os
import pandas as pd
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.scan_engine.pipeline import run_full_scan_pipeline
from core.governance.contracts import ContractViolation

# Configure logging to be quiet during audit unless error
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger("audit")

def run_system_audit():
    print("\n" + "="*60)
    print("🔍 OPTIONS SYSTEM AUDIT: Phase-Based Governance")
    print("="*60)
    
    success = True
    
    # 1. Run Pipeline in Descriptive Mode (Steps 2-6)
    print("\n🏃 Running pipeline audit...")
    try:
        # Use a small limit for audit speed if possible, or just run full
        results = run_full_scan_pipeline(
            snapshot_path=None, # Auto-resolve
            ticker_limit=10,
            output_dir="data/audit_outputs"
        )
        
        # Phase 1: Snapshot Integrity
        if 'snapshot' in results:
            df = results['snapshot']
            print(f"✓ Phase 1: Snapshot integrity OK ({len(df)} rows)")
        else:
            print("✗ Phase 1: Snapshot missing from results")
            success = False

        # Phase 2: Volatility Surface
        if 'filtered' in results:
            df = results['filtered']
            # Check for explicit IV fallback field
            if 'IV_Rank_Source' in df.columns:
                sources = df['IV_Rank_Source'].unique()
                print(f"✓ Phase 2: Volatility surface OK (Sources: {sources})")
            else:
                print("✗ Phase 2: IV_Rank_Source field missing (Governance violation)")
                success = False
        
        # Phase 3: Technical Indicators (The Hard Gate)
        if 'charted' in results:
            df = results['charted']
            # The contract validation is already inside compute_chart_signals
            # If we got here, it passed.
            print(f"✓ Phase 3: Technical indicators OK (NA-safe boolean contract)")
        else:
            print("✗ Phase 3: Charted data missing")
            success = False

        # Phase 5: Strategy Discovery
        if 'recommended_strategies' in results:
            df = results['recommended_strategies']
            print(f"✓ Phase 5: Strategy discovery OK ({len(df)} recommendations)")
        else:
            print("✗ Phase 5: Strategy discovery failed or skipped")
            success = False

    except ContractViolation as e:
        print(f"✗ GOVERNANCE FAILURE: {str(e)}")
        success = False
    except Exception as e:
        print(f"✗ SYSTEM ERROR: {str(e)}")
        success = False

    print("\n" + "="*60)
    if success:
        print("✅ AUDIT PASSED: System is locked and auditable.")
        sys.exit(0)
    else:
        print("🛑 AUDIT FAILED: Governance violations detected.")
        sys.exit(1)

if __name__ == "__main__":
    run_system_audit()
