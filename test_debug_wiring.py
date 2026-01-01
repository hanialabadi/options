#!/usr/bin/env python3
"""
Quick Test - Validate Debug Script Wiring

Tests that all imports work and functions are callable.
Does NOT run full pipeline - just validates setup.
"""

import sys
from pathlib import Path

print("🧪 Testing debug script wiring...\n")

# Test imports
try:
    sys.path.insert(0, str(Path(__file__).parent.parent))
    
    from core.scan_engine.step2_load_snapshot import load_ivhv_snapshot
    print("✅ step2_load_snapshot imported")
    
    from core.scan_engine.step3_filter_ivhv import filter_ivhv_gap
    print("✅ step3_filter_ivhv imported")
    
    from core.scan_engine.step5_chart_signals import compute_chart_signals
    print("✅ step5_chart_signals imported")
    
    from core.scan_engine.step6_gem_filter import validate_data_quality
    print("✅ step6_gem_filter imported")
    
    from core.scan_engine.step7b_multi_strategy_ranker import generate_multi_strategy_suggestions
    print("✅ step7b_multi_strategy_ranker imported")
    
    from core.scan_engine.step9b_fetch_contracts import fetch_and_select_contracts
    print("✅ step9b_fetch_contracts imported")
    
    from core.strategy_tiers import get_strategy_tier, is_execution_ready, get_execution_blocker
    print("✅ strategy_tiers imported")
    
    print("\n✨ All imports successful!")
    print("\nNext steps:")
    print("  1. Ensure you have a snapshot in data/snapshots/")
    print("  2. Run: python cli/run_pipeline_debug.py --ticker AAPL")
    print("  3. Watch for PASS/FAIL at each step")
    
except ImportError as e:
    print(f"\n❌ Import failed: {e}")
    print("\nCheck:")
    print("  - Are you in the project root?")
    print("  - Is the venv activated?")
    print("  - Do all the step files exist in core/scan_engine/?")
    sys.exit(1)
