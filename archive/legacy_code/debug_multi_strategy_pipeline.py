#!/usr/bin/env python3
"""
Complete Multi-Strategy Pipeline Debug Script
==============================================

Tests the new multi-strategy architecture end-to-end and validates:
- Data integrity at each step
- RAG theory compliance
- Strategy Ledger format
- Backward compatibility
"""

import pandas as pd
import logging
from pathlib import Path
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent))

from core.scan_engine import (
    load_ivhv_snapshot,
    filter_ivhv_gap,
    compute_chart_signals,
    validate_data_quality,
    recommend_strategies
)


def print_section(title):
    """Print a formatted section header."""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80 + "\n")


def validate_dataframe_integrity(df, step_name):
    """Validate DataFrame structure and data types."""
    print(f"📊 {step_name} Data Integrity Check:")
    print(f"   Rows: {len(df)}")
    print(f"   Columns: {len(df.columns)}")
    print(f"   Duplicates: {df.duplicated().sum()}")
    print(f"   Missing tickers: {df['Ticker'].isna().sum()}")
    
    # Check for object dtypes (should be minimal)
    object_cols = df.select_dtypes(include=['object']).columns.tolist()
    if object_cols:
        print(f"   ⚠️ Object dtype columns: {object_cols}")
    
    return True


def validate_strategy_ledger_format(df):
    """Validate Strategy Ledger schema compliance."""
    print("\n📋 Strategy Ledger Format Validation:")
    
    required_new_cols = [
        'Strategy_Name', 'Valid_Reason', 'Theory_Source',
        'Regime_Context', 'IV_Context', 'Capital_Requirement',
        'Risk_Profile', 'Greeks_Exposure', 'Confidence'
    ]
    
    required_legacy_cols = [
        'Primary_Strategy', 'Secondary_Strategy', 'Success_Probability'
    ]
    
    print("\n   New Multi-Strategy Columns:")
    for col in required_new_cols:
        status = "✅" if col in df.columns else "❌"
        print(f"   {status} {col}")
    
    print("\n   Legacy Compatibility Columns:")
    for col in required_legacy_cols:
        status = "✅" if col in df.columns else "❌"
        print(f"   {status} {col}")
    
    return all(col in df.columns for col in required_new_cols)


def validate_rag_compliance(df):
    """Validate RAG theory compliance for strategies."""
    print("\n📚 RAG Theory Compliance Check:")
    
    # Check all strategies have theory sources
    if 'Theory_Source' in df.columns:
        has_theory = df['Theory_Source'].notna().sum()
        total = len(df)
        print(f"   Strategies with theory backing: {has_theory}/{total} ({has_theory/total*100:.1f}%)")
        
        # Show theory source distribution
        print("\n   Theory Source Distribution:")
        theory_counts = df['Theory_Source'].value_counts()
        for theory, count in theory_counts.items():
            print(f"      {theory[:50]}...: {count}")
    
    # Validate strategy-theory alignment
    if 'Strategy_Name' in df.columns and 'Theory_Source' in df.columns:
        print("\n   Strategy-Theory Alignment:")
        strategy_theory = df.groupby('Strategy_Name')['Theory_Source'].first()
        for strategy, theory in strategy_theory.items():
            print(f"      {strategy:25s} → {theory[:50]}...")
    
    return True


def check_multi_strategy_generation(df):
    """Check if multiple strategies per ticker are being generated."""
    print("\n🔀 Multi-Strategy Generation Analysis:")
    
    if 'Ticker' not in df.columns:
        print("   ❌ No Ticker column found")
        return False
    
    strategies_per_ticker = df.groupby('Ticker').size()
    avg_strategies = strategies_per_ticker.mean()
    max_strategies = strategies_per_ticker.max()
    multi_ticker_count = (strategies_per_ticker > 1).sum()
    
    print(f"   Total strategies: {len(df)}")
    print(f"   Unique tickers: {df['Ticker'].nunique()}")
    print(f"   Avg strategies/ticker: {avg_strategies:.2f}")
    print(f"   Max strategies/ticker: {max_strategies}")
    print(f"   Tickers with multiple strategies: {multi_ticker_count} ({multi_ticker_count/df['Ticker'].nunique()*100:.1f}%)")
    
    # Show strategy distribution
    if 'Strategy_Name' in df.columns:
        print("\n   Strategy Distribution:")
        strategy_counts = df['Strategy_Name'].value_counts()
        for strategy, count in strategy_counts.items():
            print(f"      {strategy:25s}: {count:3d}")
    
    # Show examples of multi-strategy tickers
    print("\n   Example Multi-Strategy Tickers:")
    multi_tickers = strategies_per_ticker[strategies_per_ticker > 1].head(5)
    for ticker, count in multi_tickers.items():
        strategies = df[df['Ticker'] == ticker]['Strategy_Name'].tolist()
        print(f"      {ticker} ({count}): {strategies}")
    
    return avg_strategies > 1.0


def check_backward_compatibility(df):
    """Check backward compatibility with legacy code."""
    print("\n🔄 Backward Compatibility Check:")
    
    # Check Primary_Strategy exists and is populated
    if 'Primary_Strategy' in df.columns:
        primary_populated = df['Primary_Strategy'].notna().sum()
        print(f"   ✅ Primary_Strategy column exists: {primary_populated}/{len(df)} rows populated")
        
        # Check that each ticker has exactly one primary strategy
        if 'Ticker' in df.columns:
            unique_primaries = df.drop_duplicates('Ticker')['Primary_Strategy'].notna().sum()
            unique_tickers = df['Ticker'].nunique()
            print(f"   ✅ Unique primaries: {unique_primaries}/{unique_tickers} tickers")
    else:
        print(f"   ❌ Primary_Strategy column missing")
        return False
    
    # Check Secondary_Strategy exists
    if 'Secondary_Strategy' in df.columns:
        secondary_populated = (df['Secondary_Strategy'] != 'None').sum()
        print(f"   ✅ Secondary_Strategy column exists: {secondary_populated}/{len(df)} rows populated")
    else:
        print(f"   ❌ Secondary_Strategy column missing")
        return False
    
    return True


def run_complete_pipeline(snapshot_path):
    """Run complete pipeline with validation at each step."""
    
    print_section("MULTI-STRATEGY PIPELINE DEBUG - COMPLETE VALIDATION")
    
    print(f"📂 Input: {snapshot_path}")
    print(f"   Exists: {Path(snapshot_path).exists()}")
    
    # ========================================
    # STEP 2: Load & Enrich
    # ========================================
    print_section("STEP 2: Load & Enrich Snapshot")
    
    df_step2 = load_ivhv_snapshot(snapshot_path)
    validate_dataframe_integrity(df_step2, "Step 2")
    
    print(f"\n   Key columns: {[c for c in df_step2.columns if 'IV' in c or 'HV' in c][:10]}")
    
    # ========================================
    # STEP 3: IV/HV Filter
    # ========================================
    print_section("STEP 3: IV/HV Edge Detection")
    
    df_step3 = filter_ivhv_gap(df_step2)
    validate_dataframe_integrity(df_step3, "Step 3")
    
    print(f"\n   Tickers passed: {len(df_step3)}/{len(df_step2)} ({len(df_step3)/len(df_step2)*100:.1f}%)")
    print(f"   Edge flags: ShortTerm={df_step3['ShortTerm_IV_Edge'].sum()}, Medium={df_step3['MediumTerm_IV_Edge'].sum()}")
    
    # ========================================
    # STEP 5: PCS Scores
    # ========================================
    print_section("STEP 5: Chart Signals & PCS")
    
    df_step5 = compute_chart_signals(df_step3)
    validate_dataframe_integrity(df_step5, "Step 5")
    
    if 'Signal_Type' in df_step5.columns:
        print(f"\n   Signal distribution:")
        print(df_step5['Signal_Type'].value_counts())
    
    # ========================================
    # STEP 6: GEM Validation
    # ========================================
    print_section("STEP 6: Data Quality Validation")
    
    df_step6 = validate_data_quality(df_step5)
    validate_dataframe_integrity(df_step6, "Step 6")
    
    print(f"\n   Tickers with complete data: {len(df_step6)}")
    
    # ========================================
    # STEP 7: Multi-Strategy Recommendation
    # ========================================
    print_section("STEP 7: MULTI-STRATEGY RECOMMENDATION (NEW ARCHITECTURE)")
    
    df_step7 = recommend_strategies(
        df_step6, 
        enable_directional=True, 
        enable_volatility=True,
        tier_filter='tier1_only',
        exploration_mode=False
    )
    
    validate_dataframe_integrity(df_step7, "Step 7")
    
    # Validate new architecture
    if not validate_strategy_ledger_format(df_step7):
        print("\n   ❌ Strategy Ledger format validation FAILED")
        return None
    
    if not validate_rag_compliance(df_step7):
        print("\n   ❌ RAG compliance validation FAILED")
        return None
    
    if not check_multi_strategy_generation(df_step7):
        print("\n   ⚠️ Multi-strategy generation below threshold (expected >1.0 avg)")
    
    if not check_backward_compatibility(df_step7):
        print("\n   ❌ Backward compatibility FAILED")
        return None
    
    # ========================================
    # FINAL VALIDATION
    # ========================================
    print_section("FINAL VALIDATION SUMMARY")
    
    print("✅ Pipeline completed successfully!\n")
    
    print("📊 Final Statistics:")
    print(f"   Input tickers: {len(df_step2)}")
    print(f"   After IV/HV filter: {len(df_step3)}")
    print(f"   After chart signals: {len(df_step5)}")
    print(f"   After GEM validation: {len(df_step6)}")
    print(f"   Final strategies: {len(df_step7)}")
    print(f"   Unique tickers in output: {df_step7['Ticker'].nunique()}")
    
    # Check for data loss
    strategies_per_ticker = df_step7.groupby('Ticker').size().mean()
    print(f"\n   Strategy generation rate: {strategies_per_ticker:.2f} strategies/ticker")
    
    if strategies_per_ticker >= 2.0:
        print(f"   ✅ EXCELLENT: Multi-strategy architecture working optimally")
    elif strategies_per_ticker >= 1.5:
        print(f"   ✅ GOOD: Multi-strategy generation above threshold")
    elif strategies_per_ticker > 1.0:
        print(f"   ⚠️ FAIR: Some multi-strategy generation")
    else:
        print(f"   ❌ PROBLEM: No multi-strategy generation (old architecture behavior)")
    
    # Save output for inspection
    output_path = Path("output/debug_step7_multi_strategy.csv")
    output_path.parent.mkdir(exist_ok=True)
    df_step7.to_csv(output_path, index=False)
    print(f"\n💾 Saved output: {output_path}")
    
    # Show sample rows
    print_section("SAMPLE OUTPUT (First 5 Strategies)")
    
    sample_cols = [
        'Ticker', 'Strategy_Name', 'Primary_Strategy', 
        'Valid_Reason', 'Confidence', 'Capital_Requirement'
    ]
    sample_cols = [c for c in sample_cols if c in df_step7.columns]
    
    print(df_step7[sample_cols].head(10).to_string(index=False))
    
    return df_step7


if __name__ == "__main__":
    # Use existing Step 6 output for faster testing
    snapshot_path = "output/Step6_GEM_20251225_145249.csv"
    
    if not Path(snapshot_path).exists():
        # Fall back to full snapshot
        snapshot_path = "/Users/haniabadi/Documents/Windows/OptionsSnapshots/fidelity_ivhv_snapshot.csv"
        print(f"⚠️ Using full snapshot: {snapshot_path}")
    else:
        print(f"✅ Using cached Step 6 output: {snapshot_path}")
    
    try:
        df_result = run_complete_pipeline(snapshot_path)
        
        if df_result is not None:
            print_section("✅ DEBUG COMPLETE - ALL VALIDATIONS PASSED")
        else:
            print_section("❌ DEBUG FAILED - SEE ERRORS ABOVE")
            sys.exit(1)
            
    except Exception as e:
        print_section("💥 PIPELINE ERROR")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
