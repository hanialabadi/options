#!/usr/bin/env python3
"""
Phase 1-5 Pipeline CLI Runner

Executes the options pipeline from Phase 1 (Clean) through Phase 5 (Portfolio aggregation)
STOPS BEFORE Phase 6 (Freeze contracts).

This runner tests:
✅ Phase 1: Load and clean Schwab/Fidelity positions
✅ Phase 2: Parse leg identity, structure, strategy tagging
✅ Phase 3: Enrich with new observables (IV_Rank, Earnings, DTE, Capital, Trade aggregates)
✅ Phase 4: Snapshot with market context (Snapshot_TS, First_Seen_Date)
✅ Phase 5: Portfolio aggregation (Total Greeks, Total Capital, Risk Metrics)

Phase 6 (freeze _Entry contracts) is NOT executed.

Usage:
    python run_phase1_to_5_cli.py
    python run_phase1_to_5_cli.py --input data/brokerage_inputs/fidelity_positions.csv
"""

import sys
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
import argparse

# Add project root to path
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.phase1_clean import phase1_load_and_clean_positions
from core.phase1_enrich_market_context import enrich_with_ohlcv
from core.phase2_parse import phase2_run_all
from core.phase3_enrich import run_phase3_enrichment
from core.phase3_enrich.compute_chart_observables import compute_chart_observables
from core.phase4_snapshot import save_clean_snapshot
from core.phase5_portfolio_limits import (
    compute_portfolio_greeks,
    check_portfolio_limits,
    analyze_correlation_risk,
    get_persona_limits,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def print_section(title: str):
    """Print section header."""
    logger.info("\n" + "=" * 80)
    logger.info(f"  {title}")
    logger.info("=" * 80)


def print_phase_summary(phase_name: str, df: pd.DataFrame, prev_df: pd.DataFrame = None):
    """Print phase completion summary."""
    logger.info(f"\n✅ {phase_name} COMPLETE")
    logger.info(f"   Rows: {len(df)}")
    logger.info(f"   Columns: {len(df.columns)}")
    
    if prev_df is not None:
        new_cols = set(df.columns) - set(prev_df.columns)
        if new_cols:
            logger.info(f"   New columns added: {len(new_cols)}")
            logger.info(f"   {sorted(new_cols)}")


def display_sample(df: pd.DataFrame, title: str, n: int = 3):
    """Display sample rows from dataframe."""
    logger.info(f"\n{title}")
    logger.info("-" * 80)
    
    if len(df) == 0:
        logger.warning("⚠️  No rows to display")
        return
    
    # Select key columns to display
    display_cols = []
    
    # Always show these if present
    priority_cols = ['Symbol', 'Leg_Identity', 'TradeID', 'Strategy', 'Leg_Role']
    for col in priority_cols:
        if col in df.columns:
            display_cols.append(col)
    
    # Add new observable columns
    observable_cols = ['DTE', 'IV_Rank', 'Days_to_Earnings', 'Capital_Deployed', 'Moneyness_Pct']
    for col in observable_cols:
        if col in df.columns and col not in display_cols:
            display_cols.append(col)
    
    if not display_cols:
        display_cols = df.columns[:5].tolist()
    
    sample = df[display_cols].head(n)
    logger.info(sample.to_string(index=False))


def validate_phase3_observables(df: pd.DataFrame) -> bool:
    """Validate that Phase 3 added expected observables."""
    logger.info("\n🔍 Validating Phase 3 Observables...")
    
    expected_observables = {
        'DTE': 'Days to expiration',
        'IV_Rank': 'IV percentile rank (0-100)',
        'IV_Rank_Source': 'IV data source provenance',
        'IV_Rank_History_Days': 'Days of IV history available',
        'Days_to_Earnings': 'Days until next earnings',
        'Next_Earnings_Date': 'Next earnings date',
        'Earnings_Source': 'Earnings data source provenance',
        'Capital_Deployed': 'Capital at risk',
        'Moneyness_Pct': 'Distance from strike (%)',
        'BreakEven': 'Breakeven price',
    }
    
    missing = []
    present = []
    
    for col, description in expected_observables.items():
        if col in df.columns:
            present.append(col)
            
            # Check data availability
            if col in ['IV_Rank', 'Days_to_Earnings']:
                non_null_count = df[col].notna().sum()
                non_null_pct = (non_null_count / len(df) * 100) if len(df) > 0 else 0
                logger.info(f"   ✅ {col}: {non_null_count}/{len(df)} ({non_null_pct:.1f}%) - {description}")
            else:
                logger.info(f"   ✅ {col}: Present - {description}")
        else:
            missing.append(col)
            logger.warning(f"   ❌ {col}: MISSING - {description}")
    
    if missing:
        logger.error(f"\n❌ Validation failed: {len(missing)} observable(s) missing")
        return False
    
    logger.info(f"\n✅ All {len(expected_observables)} observables present")
    return True


def run_phase1_to_5(input_path: str = None) -> pd.DataFrame:
    """Run Phase 1-5 pipeline."""
    
    print_section("PHASE 1-5 PIPELINE EXECUTION")
    
    start_time = datetime.now()
    
    # Default input path
    if input_path is None:
        input_path = "data/brokerage_inputs/fidelity_positions.csv"
    
    input_path = Path(input_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    logger.info(f"📄 Input: {input_path}")
    logger.info(f"⏰ Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # ========================================================================
    # PHASE 1: Load and Clean
    # ========================================================================
    print_section("PHASE 1: Load & Clean")
    logger.info(f"Loading positions from {input_path}...")
    
    result = phase1_load_and_clean_positions(input_path=Path(input_path))
    # phase1_load_and_clean_positions returns a dict with 'df' key
    if isinstance(result, dict):
        df_phase1 = result['df']
    else:
        # Old signature returned (df, snapshot_path) tuple
        df_phase1, _ = result
    
    print_phase_summary("Phase 1", df_phase1)
    display_sample(df_phase1, "📋 Phase 1 Sample (Cleaned Positions)")
    
    # ========================================================================
    # PHASE 2: Parse & Identity
    # ========================================================================
    print_section("PHASE 2: Parse Leg Identity & Structure")
    logger.info("Parsing leg identity, roles, and strategy structure...")
    
    df_phase2 = phase2_run_all(df_phase1)
    
    print_phase_summary("Phase 2", df_phase2, df_phase1)
    display_sample(df_phase2, "📋 Phase 2 Sample (Parsed Structure)")
    
    # Validate Entry_Date removal
    if "Entry_Date" in df_phase2.columns:
        logger.warning("⚠️  Entry_Date still present in Phase 2 (should be moved to Phase 4)")
    else:
        logger.info("✅ Entry_Date correctly removed from Phase 2 (moved to Phase 4)")
    
    # ========================================================================
    # PHASE 2c: Filter Unreferenced STOCK Rows
    # ========================================================================
    print_section("PHASE 2c: Filter Unreferenced STOCK Rows")
    logger.info("Removing STOCK rows not referenced by any OPTION row...")
    
    initial_count = len(df_phase2)
    stock_rows_before = (df_phase2['AssetType'] == 'STOCK').sum()
    option_rows = (df_phase2['AssetType'] == 'OPTION').sum()
    
    # Get all underlyings referenced by OPTION rows
    option_underlyings = set(df_phase2[df_phase2['AssetType'] == 'OPTION']['Underlying'].dropna().unique())
    
    # Keep all OPTION rows + STOCK rows that are referenced as underlyings
    mask_keep = (
        (df_phase2['AssetType'] == 'OPTION') |  # Keep all options
        ((df_phase2['AssetType'] == 'STOCK') & (df_phase2['Symbol'].isin(option_underlyings)))  # Keep referenced stocks
    )
    
    df_phase2 = df_phase2[mask_keep].copy()
    
    stock_rows_after = (df_phase2['AssetType'] == 'STOCK').sum()
    stock_rows_dropped = stock_rows_before - stock_rows_after
    
    logger.info(f"✅ Filtered STOCK rows:")
    logger.info(f"   Before: {stock_rows_before} STOCK, {option_rows} OPTION = {initial_count} total")
    logger.info(f"   After:  {stock_rows_after} STOCK (referenced), {option_rows} OPTION = {len(df_phase2)} total")
    logger.info(f"   Dropped: {stock_rows_dropped} unreferenced STOCK rows")
    
    if stock_rows_dropped > 0:
        logger.info(f"   Referenced underlyings: {len(option_underlyings)}")
    
    # ========================================================================
    # PHASE 2b: Market Context (OHLCV from Schwab)
    # ========================================================================
    print_section("PHASE 2b: Market Context (OHLCV)")
    logger.info("Fetching price history from Schwab API (reusing scan engine)...")
    
    try:
        df_phase2 = enrich_with_ohlcv(df_phase2)
        ohlcv_count = df_phase2['UL_OHLCV_Available'].sum()
        ohlcv_total = len(df_phase2['Underlying'].dropna().unique())
        ohlcv_pct = (ohlcv_count / ohlcv_total * 100) if ohlcv_total > 0 else 0
        logger.info(f"✅ OHLCV coverage: {ohlcv_count}/{ohlcv_total} underlyings ({ohlcv_pct:.1f}%)")
    except Exception as e:
        logger.error(f"❌ OHLCV enrichment failed: {e}")
        logger.warning("⚠️  Continuing without OHLCV data...")
    
    # ========================================================================
    # PHASE 3: Enrich with New Observables
    # ========================================================================
    print_section("PHASE 3: Enrich with Observables")
    logger.info("Computing observables: IV_Rank, Earnings, DTE, Capital, Moneyness...")
    
    snapshot_ts = pd.Timestamp.now()
    df_phase3 = run_phase3_enrichment(df_phase2, snapshot_ts=snapshot_ts)
    
    print_phase_summary("Phase 3", df_phase3, df_phase2)
    display_sample(df_phase3, "📋 Phase 3 Sample (Enriched Observables)")    
    # ========================================================================
    # PHASE 3b: Chart Observables (Trend/Momentum)
    # ========================================================================
    print_section("PHASE 3b: Chart Observables (Trend/Momentum)")
    logger.info("Computing trend indicators from Schwab OHLCV (cached)...")
    
    try:
        df_phase3 = compute_chart_observables(df_phase3)
        chart_count = df_phase3['UL_Chart_Available'].sum()
        chart_total = len(df_phase3['Underlying'].dropna().unique())
        chart_pct = (chart_count / chart_total * 100) if chart_total > 0 else 0
        logger.info(f"✅ Chart coverage: {chart_count}/{chart_total} underlyings ({chart_pct:.1f}%)")
        
        # Show sample chart data
        if 'UL_Trend' in df_phase3.columns:
            trend_summary = df_phase3.groupby('Underlying').agg({
                'UL_Trend': 'first',
                'UL_RSI': 'first',
                'UL_MACD_Signal': 'first'
            }).dropna()
            if len(trend_summary) > 0:
                logger.info("\n📊 Chart Context Sample:")
                logger.info(trend_summary.head().to_string())
    except Exception as e:
        logger.error(f"❌ Chart observables failed: {e}")
        logger.warning("⚠️  Continuing without chart data...")
    
    # Validate observables
    validate_phase3_observables(df_phase3)    
    # Validate observables
    validation_passed = validate_phase3_observables(df_phase3)
    if not validation_passed:
        logger.warning("⚠️  Phase 3 validation found missing observables")
    
    # ========================================================================
    # PHASE 4: Snapshot with Market Context
    # ========================================================================
    print_section("PHASE 4: Snapshot with Market Context")
    logger.info("Adding snapshot metadata: Snapshot_TS, First_Seen_Date, Schema_Hash...")
    
    # Phase 4 saves to CSV and returns (df, csv_path, db_table, csv_success, db_success)
    result = save_clean_snapshot(
        df_phase3,
        db_path="output/positions_history.duckdb",
        to_csv=True,
        to_db=True
    )
    
    df_phase4, output_path, db_table, csv_success, db_success = result
    
    print_phase_summary("Phase 4", df_phase4, df_phase3)
    
    # Validate Phase 4 metadata
    phase4_cols = ['Snapshot_TS', 'run_id', 'Schema_Hash', 'First_Seen_Date']
    present = [col for col in phase4_cols if col in df_phase4.columns]
    missing = [col for col in phase4_cols if col not in df_phase4.columns]
    
    logger.info(f"\n📊 Phase 4 metadata columns:")
    for col in present:
        logger.info(f"   ✅ {col}")
    for col in missing:
        logger.warning(f"   ❌ {col} (MISSING)")
    
    logger.info(f"\n💾 Phase 4 snapshot saved: {output_path}")
    
    # ========================================================================
    # PHASE 5: Portfolio Aggregation & Risk Limits
    # ========================================================================
    print_section("PHASE 5: Portfolio Aggregation & Risk Limits")
    logger.info("Computing portfolio-level metrics...")
    
    # Compute portfolio Greeks
    portfolio_greeks = compute_portfolio_greeks(df_phase4)
    
    # Check portfolio limits (using conservative limits by default)
    df_phase5, diagnostics = check_portfolio_limits(
        df_phase4,
        limits=get_persona_limits('conservative'),
        account_balance=100000.0
    )
    
    # Analyze correlation and concentration risk
    df_phase5 = analyze_correlation_risk(df_phase5)
    
    # Basic portfolio stats (for display)
    portfolio_stats = {
        'Total Positions': len(df_phase5),
        'Total Capital Deployed': df_phase5['Capital_Deployed'].sum() if 'Capital_Deployed' in df_phase5.columns else 0,
        'Net Delta': portfolio_greeks['net_delta'],
        'Net Gamma': portfolio_greeks['net_gamma'],
        'Net Theta': portfolio_greeks['net_theta'],
        'Net Vega': portfolio_greeks['net_vega'],
        'Unique Symbols': df_phase5['Symbol'].nunique() if 'Symbol' in df_phase5.columns else 0,
    }
    
    logger.info("\n📊 Portfolio Summary:")
    for metric, value in portfolio_stats.items():
        if isinstance(value, float):
            logger.info(f"   {metric}: {value:,.2f}")
        else:
            logger.info(f"   {metric}: {value:,}")
    
    # Display limit utilization
    if 'utilization' in diagnostics:
        logger.info("\n📈 Portfolio Limit Utilization:")
        util = diagnostics['utilization']
        logger.info(f"   Delta: {util['delta_pct']:.1f}% of limit")
        logger.info(f"   Vega: {util['vega_pct']:.1f}% of limit")
        logger.info(f"   Gamma: {util['gamma_pct']:.1f}% of limit")
        logger.info(f"   Theta: {util['theta_pct']:.1f}% of limit")
    
    # ========================================================================
    # FINAL SUMMARY
    # ========================================================================
    duration = (datetime.now() - start_time).total_seconds()
    
    print_section("PIPELINE COMPLETE ✅")
    
    logger.info(f"⏱️  Duration: {duration:.2f}s")
    logger.info(f"📊 Final output: {len(df_phase5)} positions")
    logger.info(f"💾 Saved to: {output_path}")
    
    logger.info("\n📋 Phase Summary:")
    logger.info(f"   Phase 1 (Clean):     {len(df_phase1)} rows")
    logger.info(f"   Phase 2 (Parse):     {len(df_phase2)} rows")
    logger.info(f"   Phase 3 (Enrich):    {len(df_phase3)} rows")
    logger.info(f"   Phase 4 (Snapshot):  {len(df_phase4)} rows")
    logger.info(f"   Phase 5 (Portfolio): Portfolio-level aggregation complete")
    
    logger.info("\n🔵 STOPPED BEFORE PHASE 6 (Freeze Contracts)")
    logger.info("   Phase 6 would freeze _Entry snapshots (not executed)")
    
    # Observable Coverage Summary
    logger.info("\n📊 Observable Coverage Summary:")
    
    # OHLCV Coverage
    if 'UL_OHLCV_Available' in df_phase4.columns:
        ohlcv_count = df_phase4['UL_OHLCV_Available'].sum()
        ohlcv_total = len(df_phase4)
        ohlcv_pct = (ohlcv_count / ohlcv_total * 100) if ohlcv_total > 0 else 0
        logger.info(f"📊 OHLCV (Price History): {ohlcv_count}/{ohlcv_total} ({ohlcv_pct:.1f}%)")
    
    # Chart Coverage
    if 'UL_Chart_Available' in df_phase4.columns:
        chart_count = df_phase4['UL_Chart_Available'].sum()
        chart_total = len(df_phase4)
        chart_pct = (chart_count / chart_total * 100) if chart_total > 0 else 0
        logger.info(f"📈 Chart Observables: {chart_count}/{chart_total} ({chart_pct:.1f}%)")
        
        # Trend breakdown
        if 'UL_Trend' in df_phase4.columns:
            trend_counts = df_phase4.groupby('Underlying')['UL_Trend'].first().value_counts()
            logger.info("   Trend breakdown:")
            for trend, count in trend_counts.items():
                logger.info(f"      {trend}: {count} underlyings")
    
    # Check IV_Rank and Earnings data quality
    if 'IV_Rank' in df_phase4.columns:
        iv_available = df_phase4['IV_Rank'].notna().sum()
        iv_pct = (iv_available / len(df_phase4) * 100) if len(df_phase4) > 0 else 0
        logger.info(f"\n📊 IV_Rank Coverage: {iv_available}/{len(df_phase4)} ({iv_pct:.1f}%)")
        
        if iv_available == 0:
            logger.warning("⚠️  IV_Rank: 0% coverage (insufficient historical data)")
            logger.info("   Current: 5 days of IV snapshots (need 120+ days)")
        else:
            logger.info(f"✅ IV_Rank: {iv_pct:.1f}% coverage")
    
    if 'Days_to_Earnings' in df_phase4.columns:
        earnings_available = df_phase4['Days_to_Earnings'].notna().sum()
        earnings_pct = (earnings_available / len(df_phase4) * 100) if len(df_phase4) > 0 else 0
        logger.info(f"\n📊 Earnings Coverage: {earnings_available}/{len(df_phase4)} ({earnings_pct:.1f}%)")
        
        if earnings_available > 0:
            logger.info(f"✅ Earnings: {earnings_pct:.1f}% coverage (Yahoo Finance integration working)")
            
            # Show earnings distribution
            earnings_data = df_phase4[df_phase4['Days_to_Earnings'].notna()]
            if len(earnings_data) > 0:
                logger.info(f"   Days to earnings range: {earnings_data['Days_to_Earnings'].min():.0f} to {earnings_data['Days_to_Earnings'].max():.0f}")
        else:
            logger.warning("⚠️  Earnings: 0% coverage (check Yahoo Finance integration)")
    
    # ========================================================================
    # P&L and Performance Summary
    # ========================================================================
    if 'Unrealized_PnL' in df_phase4.columns:
        total_pnl = df_phase4['Unrealized_PnL'].sum()
        winning_positions = (df_phase4['Unrealized_PnL'] > 0).sum()
        losing_positions = (df_phase4['Unrealized_PnL'] < 0).sum()
        
        logger.info(f"\n💰 P&L Summary:")
        logger.info(f"   Total Unrealized P&L: ${total_pnl:,.2f}")
        logger.info(f"   Winning Positions: {winning_positions}/{len(df_phase4)} ({winning_positions/len(df_phase4)*100:.1f}%)")
        logger.info(f"   Losing Positions: {losing_positions}/{len(df_phase4)} ({losing_positions/len(df_phase4)*100:.1f}%)")
        
        if 'Days_In_Trade' in df_phase4.columns:
            avg_days = df_phase4['Days_In_Trade'].mean()
            logger.info(f"   Average Days in Trade: {avg_days:.1f}")
        
        if 'ROI_Current' in df_phase4.columns:
            avg_roi = df_phase4['ROI_Current'].mean()
            logger.info(f"   Average ROI: {avg_roi:.1f}%")
        
        # Show top winners/losers
        if len(df_phase4) > 0:
            top_winner = df_phase4.nlargest(1, 'Unrealized_PnL')[['Symbol', 'Strategy', 'Unrealized_PnL']].iloc[0]
            top_loser = df_phase4.nsmallest(1, 'Unrealized_PnL')[['Symbol', 'Strategy', 'Unrealized_PnL']].iloc[0]
            
            logger.info(f"   Top Winner: {top_winner['Symbol']} ({top_winner['Strategy']}) +${top_winner['Unrealized_PnL']:,.2f}")
            logger.info(f"   Top Loser: {top_loser['Symbol']} ({top_loser['Strategy']}) ${top_loser['Unrealized_PnL']:,.2f}")
    
    # Assignment Risk Summary
    if 'Assignment_Risk_Score' in df_phase4.columns:
        logger.info(f"\n⚠️  Assignment Risk Summary:")
        
        critical_risk = df_phase4[df_phase4['Assignment_Risk_Level'] == 'CRITICAL']
        high_risk = df_phase4[df_phase4['Assignment_Risk_Level'] == 'HIGH']
        pin_risk = df_phase4[df_phase4['Pin_Risk'] == True]
        early_risk = df_phase4[df_phase4['Early_Assignment_Risk'] == True]
        
        logger.info(f"   CRITICAL Risk: {len(critical_risk)} positions")
        logger.info(f"   HIGH Risk: {len(high_risk)} positions")
        logger.info(f"   Pin Risk: {len(pin_risk)} positions")
        logger.info(f"   Early Assignment Risk: {len(early_risk)} positions")
        
        # Show critical/high risk positions
        at_risk = df_phase4[df_phase4['Assignment_Risk_Score'] >= 60].copy()
        if len(at_risk) > 0:
            logger.warning(f"\n⚠️  {len(at_risk)} position(s) with HIGH/CRITICAL assignment risk:")
            at_risk_sorted = at_risk.sort_values('Assignment_Risk_Score', ascending=False)
            for idx in at_risk_sorted.head(5).index:
                symbol = df_phase4.at[idx, 'Symbol']
                score = df_phase4.at[idx, 'Assignment_Risk_Score']
                level = df_phase4.at[idx, 'Assignment_Risk_Level']
                dte = df_phase4.at[idx, 'DTE']
                itm_pct = df_phase4.at[idx, 'ITM_Pct']
                is_pin = df_phase4.at[idx, 'Pin_Risk']
                is_early = df_phase4.at[idx, 'Early_Assignment_Risk']
                
                flags = []
                if is_pin:
                    flags.append("PIN")
                if is_early:
                    flags.append("EARLY")
                flag_str = f" [{', '.join(flags)}]" if flags else ""
                
                logger.warning(f"      {symbol}: {level} (Score: {score:.0f}, DTE: {dte}, ITM: {itm_pct:.1f}%){flag_str}")
        else:
            logger.info("   ✅ No positions at high assignment risk")
    
    logger.info("\n" + "=" * 80)
    
    return df_phase5


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Run Phase 1-5 pipeline (stops before Phase 6 freeze contracts)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use default input (fidelity_positions.csv)
  python run_phase1_to_5_cli.py
  
  # Specify custom input file
  python run_phase1_to_5_cli.py --input data/brokerage_inputs/custom_positions.csv
        """
    )
    
    parser.add_argument(
        '--input',
        type=str,
        default=None,
        help='Path to input positions CSV (default: data/brokerage_inputs/fidelity_positions.csv)'
    )
    
    args = parser.parse_args()
    
    try:
        df_final = run_phase1_to_5(input_path=args.input)
        logger.info("\n✅ Pipeline execution successful")
        return 0
    except Exception as e:
        logger.error(f"\n❌ Pipeline failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
