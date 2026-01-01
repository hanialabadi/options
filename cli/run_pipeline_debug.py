#!/usr/bin/env python3
"""
Pipeline Debug Mode - Single Ticker Execution Trace

PURPOSE:
    Trace why Tier-1 strategies do or do not result in executable contracts.
    Provides step-by-step PASS/FAIL visibility for diagnostic purposes.

USAGE:
    python cli/run_pipeline_debug.py --ticker AAPL
    python cli/run_pipeline_debug.py --ticker MSFT --min-gap 3.5

CONSTRAINTS:
    - Uses EXISTING pipeline logic (no duplication)
    - Does NOT modify filters or logic
    - Does NOT execute Tier-2+ strategies
    - CLI output only (no dashboard)

OUTPUT:
    - Console: Step-by-step PASS/FAIL with reasons
    - File: output/debug_execution_trace_{ticker}_{timestamp}.json
"""

import sys
import argparse
import logging
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any
import pandas as pd

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.scan_engine.step2_load_snapshot import load_ivhv_snapshot
from core.scan_engine.step3_filter_ivhv import filter_ivhv_gap
from core.scan_engine.step5_chart_signals import compute_chart_signals
from core.scan_engine.step6_gem_filter import validate_data_quality
from core.scan_engine.step7b_multi_strategy_ranker import generate_multi_strategy_suggestions
from core.scan_engine.step9b_fetch_contracts import fetch_and_select_contracts
from core.strategy_tiers import get_strategy_tier, is_execution_ready, get_execution_blocker

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PipelineDebugTracer:
    """
    Traces single-ticker execution through all pipeline stages.
    Records PASS/FAIL at each step with explicit reasons.
    """
    
    def __init__(self, ticker: str, min_gap: float = 2.0):
        self.ticker = ticker.upper()
        self.min_gap = min_gap
        self.trace_log: List[Dict[str, Any]] = []
        self.current_df: pd.DataFrame = None
        self.strategies: pd.DataFrame = None
        self.final_result: str = "UNKNOWN"
        self.blockers: List[str] = []
        
    def log_step(self, step: str, result: str, detail: str = "", reason: str = "", count: int = None):
        """Record step result in trace log"""
        entry = {
            "step": step,
            "result": result,  # PASS, FAIL, SKIP, INFO
            "detail": detail,
            "reason": reason,
            "count": count,
            "timestamp": datetime.now().isoformat()
        }
        self.trace_log.append(entry)
        
        # Console output
        status_icon = {
            "PASS": "✅",
            "FAIL": "❌",
            "SKIP": "⏭️",
            "INFO": "ℹ️"
        }.get(result, "•")
        
        msg = f"{status_icon} Step {step}: {result}"
        if detail:
            msg += f" | {detail}"
        if reason:
            msg += f" | Reason: {reason}"
        if count is not None:
            msg += f" | Count: {count}"
            
        print(msg)
        logger.info(msg)
        
        # Track blockers
        if result == "FAIL" and reason:
            self.blockers.append(f"{step}: {reason}")
    
    def run_full_trace(self) -> Dict[str, Any]:
        """Execute full pipeline trace for single ticker"""
        print("\n" + "="*80)
        print(f"🔍 PIPELINE DEBUG TRACE - TICKER: {self.ticker}")
        print("="*80 + "\n")
        
        try:
            # Step 1: Load snapshot
            self._step1_load_snapshot()
            
            # Step 2: Parse snapshot
            if self.current_df is not None:
                self._step2_parse_snapshot()
            
            # Step 3: IVHV filter
            if self.current_df is not None and len(self.current_df) > 0:
                self._step3_ivhv_filter()
            
            # Step 5: Chart classification
            if self.current_df is not None and len(self.current_df) > 0:
                self._step5_chart_classification()
            
            # Step 6: GEM filter
            if self.current_df is not None and len(self.current_df) > 0:
                self._step6_gem_filter()
            
            # Step 7B: Strategy ranking + Tier assignment
            if self.current_df is not None and len(self.current_df) > 0:
                self._step7b_strategy_ranking()
            
            # Step 9A: Tier execution gate
            if self.strategies is not None and len(self.strategies) > 0:
                self._step9a_tier_gate()
            
            # Step 9B: Fetch option contracts
            if self.strategies is not None and len(self.strategies) > 0:
                self._step9b_fetch_contracts()
            
            # Step 11: Final execution decision
            self._step11_final_decision()
            
        except Exception as e:
            self.log_step("ERROR", "FAIL", reason=str(e))
            self.final_result = "ERROR"
        
        # Summary
        self._print_summary()
        
        # Save structured log
        output_path = self._save_structured_log()
        
        return {
            "ticker": self.ticker,
            "final_result": self.final_result,
            "blockers": self.blockers,
            "trace_log": self.trace_log,
            "output_file": output_path
        }
    
    def _step1_load_snapshot(self):
        """Step 1: Load latest snapshot"""
        print("\n📂 STEP 1: Load Snapshot")
        print("-" * 80)
        
        snapshot_dir = Path("data/snapshots")
        if not snapshot_dir.exists():
            self.log_step("1", "FAIL", reason="Snapshot directory not found")
            return
        
        # Find latest snapshot
        snapshots = sorted(snapshot_dir.glob("snapshot_*.csv"), reverse=True)
        if not snapshots:
            self.log_step("1", "FAIL", reason="No snapshots found")
            return
        
        latest_snapshot = snapshots[0]
        df = pd.read_csv(latest_snapshot)
        
        # Store snapshot path for Step 2
        self.snapshot_path = str(latest_snapshot)
        
        # Filter for target ticker
        df_ticker = df[df['Ticker'] == self.ticker]
        
        if df_ticker.empty:
            self.log_step("1", "FAIL", 
                         detail=f"Ticker not in snapshot",
                         reason=f"{self.ticker} not found in {latest_snapshot.name}")
            return
        
        self.current_df = df_ticker
        self.log_step("1", "PASS", 
                     detail=f"Loaded {latest_snapshot.name}",
                     count=len(df_ticker))
    
    def _step2_parse_snapshot(self):
        """Step 2: Load and enrich snapshot data"""
        print("\n📊 STEP 2: Load & Enrich Snapshot")
        print("-" * 80)
        
        try:
            # Use the snapshot we found in Step 1, not the default path
            df_enriched = load_ivhv_snapshot(snapshot_path=self.snapshot_path)
            df_enriched = df_enriched[df_enriched['Ticker'] == self.ticker]  # Filter to our ticker
            
            if df_enriched.empty:
                self.log_step("2", "FAIL", reason="Enrichment returned empty dataframe")
                self.current_df = None
                return
            
            # Check enrichment fields
            required_fields = ['IV_Rank_30D', 'IV_Term_Structure', 'IV_Trend_7D', 'HV_Trend_30D']
            missing = [f for f in required_fields if f not in df_enriched.columns]
            
            if missing:
                self.log_step("2", "PASS", 
                             detail=f"Loaded but missing fields: {missing}",
                             count=len(df_enriched))
            else:
                # Show enrichment details
                row = df_enriched.iloc[0]
                enrichment = (f"IV_Rank={row.get('IV_Rank_30D', 0):.1f}, "
                            f"IV_Trend={row.get('IV_Trend_7D', 'N/A')}, "
                            f"HV_Trend={row.get('HV_Trend_30D', 'N/A')}")
                self.log_step("2", "PASS", detail=enrichment, count=len(df_enriched))
            
            self.current_df = df_enriched
            
        except Exception as e:
            self.log_step("2", "FAIL", reason=str(e))
            self.current_df = None
    
    def _step3_ivhv_filter(self):
        """Step 3: IVHV gap filter"""
        print("\n📉 STEP 3: IVHV Gap Filter")
        print("-" * 80)
        
        try:
            df_filtered = filter_ivhv_gap(self.current_df, min_gap=self.min_gap)
            
            if df_filtered.empty:
                row = self.current_df.iloc[0]
                iv = row.get('IV_30_D_Call', 0)
                hv = row.get('HV_30_D_Cur', 0)
                gap = abs(float(iv) - float(hv)) if (iv and hv) else 0
                
                self.log_step("3", "FAIL",
                             detail=f"IV={iv:.1f}, HV={hv:.1f}, Gap={gap:.1f}",
                             reason=f"Gap {gap:.1f} < threshold {self.min_gap}")
                self.current_df = None
                return
            
            # Show volatility regime
            row = df_filtered.iloc[0]
            gap = row.get('IVHV_gap_30D', 0)
            regime_tags = []
            if row.get('HighVol', False):
                regime_tags.append("HighVol")
            if row.get('ElevatedVol', False):
                regime_tags.append("ElevatedVol")
            if row.get('IV_Rich', False):
                regime_tags.append("IV_Rich")
            if row.get('IV_Cheap', False):
                regime_tags.append("IV_Cheap")
            
            regime = ", ".join(regime_tags) if regime_tags else "ModerateVol"
            
            self.log_step("3", "PASS",
                         detail=f"Gap={gap:.1f}, Regime=[{regime}]",
                         count=len(df_filtered))
            
            self.current_df = df_filtered
            
        except Exception as e:
            self.log_step("3", "FAIL", reason=str(e))
            self.current_df = None
    
    def _step5_chart_classification(self):
        """Step 5: Chart signals and market regime"""
        print("\n📈 STEP 5: Chart Signals & Market Regime")
        print("-" * 80)
        
        try:
            df_charted = compute_chart_signals(self.current_df)
            
            if df_charted.empty:
                self.log_step("5", "FAIL", reason="Chart signal computation failed")
                self.current_df = None
                return
            
            # Show chart signals
            row = df_charted.iloc[0]
            signal_type = row.get('Signal_Type', 'Unknown')
            regime = row.get('Market_Regime', 'Unknown')
            trend_slope = row.get('Trend_Slope', 0)
            
            self.log_step("5", "PASS",
                         detail=f"Signal={signal_type}, Regime={regime}, Trend={trend_slope:.2f}",
                         count=len(df_charted))
            
            self.current_df = df_charted
            
        except Exception as e:
            self.log_step("5", "FAIL", reason=str(e))
            self.current_df = None
    
    def _step6_gem_filter(self):
        """Step 6: Data quality validation"""
        print("\n💎 STEP 6: Data Quality Validation")
        print("-" * 80)
        
        try:
            df_validated = validate_data_quality(self.current_df)
            
            if df_validated.empty:
                self.log_step("6", "FAIL", reason="Data quality validation failed")
                self.current_df = None
                return
            
            # Filter to complete data only
            df_complete = df_validated[df_validated.get('Data_Complete', True) == True]
            
            if df_complete.empty:
                self.log_step("6", "FAIL", reason="No tickers with complete data")
                self.current_df = None
                return
            
            # Show data quality metrics
            row = df_complete.iloc[0]
            crossover_age = row.get('Crossover_Age_Bucket', 'None')
            
            self.log_step("6", "PASS",
                         detail=f"Data complete, Crossover={crossover_age}",
                         count=len(df_complete))
            
            self.current_df = df_complete
            
        except Exception as e:
            self.log_step("6", "FAIL", reason=str(e))
            self.current_df = None
    
    def _step7b_strategy_ranking(self):
        """Step 7B: Multi-strategy ranking + Tier assignment"""
        print("\n🎯 STEP 7B: Strategy Ranking + Tier Assignment")
        print("-" * 80)
        
        try:
            df_strategies = generate_multi_strategy_suggestions(
                self.current_df,
                max_strategies_per_ticker=6,
                account_size=10000.0,
                risk_tolerance='Moderate',
                primary_goal='Income'
            )
            
            if df_strategies.empty:
                self.log_step("7B", "FAIL", reason="No strategies generated")
                self.strategies = None
                return
            
            # Filter for this ticker
            df_ticker_strats = df_strategies[df_strategies['Ticker'] == self.ticker]
            
            if df_ticker_strats.empty:
                self.log_step("7B", "FAIL", reason=f"No strategies for {self.ticker}")
                self.strategies = None
                return
            
            # Show strategy breakdown by tier
            tier1 = df_ticker_strats[df_ticker_strats['Execution_Ready'] == True]
            tier2 = df_ticker_strats[df_ticker_strats['Strategy_Tier'] == 2]
            tier3 = df_ticker_strats[df_ticker_strats['Strategy_Tier'] == 3]
            
            print(f"\n  Strategy Breakdown:")
            print(f"  • Total Strategies: {len(df_ticker_strats)}")
            print(f"  • Tier 1 (Broker-Approved): {len(tier1)}")
            print(f"  • Tier 2 (Broker-Blocked): {len(tier2)}")
            print(f"  • Tier 3 (Logic-Blocked): {len(tier3)}")
            
            # List top 5 strategies by tier
            if len(tier1) > 0:
                print(f"\n  ✅ Tier 1 Strategies (Executable):")
                for _, row in tier1.head(5).iterrows():
                    print(f"     • {row['Strategy_Name']} (Score: {row.get('Strategy_Score', 0):.2f})")
            
            if len(tier2) > 0:
                print(f"\n  ⏭️  Tier 2 Strategies (Broker-Blocked):")
                for _, row in tier2.head(3).iterrows():
                    blocker = row.get('Execution_Blocker', 'Unknown')
                    print(f"     • {row['Strategy_Name']} - {blocker}")
            
            if len(tier3) > 0:
                print(f"\n  ⏭️  Tier 3 Strategies (Logic-Blocked):")
                for _, row in tier3.head(3).iterrows():
                    blocker = row.get('Execution_Blocker', 'Unknown')
                    print(f"     • {row['Strategy_Name']} - {blocker}")
            
            self.log_step("7B", "PASS",
                         detail=f"T1={len(tier1)}, T2={len(tier2)}, T3={len(tier3)}",
                         count=len(df_ticker_strats))
            
            self.strategies = df_ticker_strats
            
        except Exception as e:
            self.log_step("7B", "FAIL", reason=str(e))
            self.strategies = None
    
    def _step9a_tier_gate(self):
        """Step 9A: Tier execution gate"""
        print("\n🚪 STEP 9A: Tier Execution Gate")
        print("-" * 80)
        
        # Filter to Tier 1 only
        tier1_strategies = self.strategies[self.strategies['Execution_Ready'] == True]
        tier2_plus = self.strategies[self.strategies['Execution_Ready'] == False]
        
        if len(tier1_strategies) == 0:
            self.log_step("9A", "FAIL",
                         detail=f"0 Tier-1 strategies",
                         reason="All strategies are Tier 2+ (non-executable)")
            
            # Log what was blocked
            if len(tier2_plus) > 0:
                print(f"\n  ⛔ Blocked Strategies (Tier 2+):")
                for _, row in tier2_plus.iterrows():
                    tier = row.get('Strategy_Tier', '?')
                    blocker = row.get('Execution_Blocker', 'Unknown')
                    print(f"     • {row['Strategy_Name']} (Tier {tier}) - {blocker}")
            
            self.strategies = pd.DataFrame()  # Empty - nothing to execute
            return
        
        # Log Tier 1 strategies proceeding
        print(f"\n  ✅ Proceeding with {len(tier1_strategies)} Tier-1 strategies:")
        for _, row in tier1_strategies.iterrows():
            print(f"     • {row['Strategy_Name']}")
        
        # Log Tier 2+ skipped
        if len(tier2_plus) > 0:
            print(f"\n  ⏭️  Skipped {len(tier2_plus)} Tier-2+ strategies:")
            for _, row in tier2_plus.head(5).iterrows():
                tier = row.get('Strategy_Tier', '?')
                blocker = row.get('Execution_Blocker', 'Unknown')
                print(f"     • {row['Strategy_Name']} (Tier {tier}) - {blocker}")
        
        self.log_step("9A", "PASS",
                     detail=f"{len(tier1_strategies)} Tier-1 strategies approved",
                     count=len(tier1_strategies))
        
        # Update strategies to Tier 1 only
        self.strategies = tier1_strategies
    
    def _step9b_fetch_contracts(self):
        """Step 9B: Fetch option contracts"""
        print("\n📋 STEP 9B: Fetch Option Contracts")
        print("-" * 80)
        
        try:
            # Note: fetch_and_select_contracts expects specific columns from Step 9A
            # For debug mode, we'll check if it has required columns
            required_cols = ['Primary_Strategy', 'Min_DTE', 'Max_DTE', 'Trade_Bias', 'Num_Contracts', 'Dollar_Allocation']
            missing = [c for c in required_cols if c not in self.strategies.columns]
            
            if missing:
                self.log_step("9B", "FAIL", 
                             detail="Missing required columns from Step 9A",
                             reason=f"Need: {missing}")
                self.strategies = None
                return
            
            df_with_contracts = fetch_and_select_contracts(
                self.strategies,
                min_open_interest=50,
                max_spread_pct=10.0
            )
            
            if df_with_contracts.empty:
                self.log_step("9B", "FAIL", reason="Contract fetch returned empty")
                self.strategies = None
                return
            
            # Check contract counts
            has_contracts = df_with_contracts[df_with_contracts['Status'] == 'Contracts_Fetched']
            no_contracts = df_with_contracts[df_with_contracts['Status'] != 'Contracts_Fetched']
            
            if len(has_contracts) == 0:
                reasons = no_contracts['Status'].value_counts().to_dict()
                reason_str = ", ".join([f"{k}={v}" for k, v in reasons.items()])
                
                self.log_step("9B", "FAIL",
                             detail="0 strategies with contracts",
                             reason=reason_str)
                
                # Show detailed failures
                print(f"\n  ❌ Contract Fetch Failures:")
                for status, group in no_contracts.groupby('Status'):
                    print(f"     • {status}: {len(group)} strategies")
                    for _, row in group.head(2).iterrows():
                        print(f"        - {row['Strategy_Name']}")
                
                self.strategies = None
                return
            
            # Show success details
            print(f"\n  ✅ Contract Fetch Results:")
            print(f"     • Success: {len(has_contracts)} strategies")
            print(f"     • Failed: {len(no_contracts)} strategies")
            
            # Show liquidity filter results if available
            if 'Liquidity_Status' in has_contracts.columns:
                passed_liquidity = has_contracts[has_contracts['Liquidity_Status'] == 'Pass']
                failed_liquidity = has_contracts[has_contracts['Liquidity_Status'] == 'Fail']
                
                print(f"\n  💧 Liquidity Filter:")
                print(f"     • Passed: {len(passed_liquidity)}")
                print(f"     • Failed: {len(failed_liquidity)}")
                
                if len(failed_liquidity) > 0:
                    print(f"     • Failure Reasons:")
                    for _, row in failed_liquidity.head(3).iterrows():
                        reason = row.get('Liquidity_Fail_Reason', 'Unknown')
                        print(f"        - {row['Strategy_Name']}: {reason}")
            
            self.log_step("9B", "PASS",
                         detail=f"{len(has_contracts)} with contracts",
                         count=len(has_contracts))
            
            self.strategies = df_with_contracts
            
        except Exception as e:
            self.log_step("9B", "FAIL", reason=str(e))
            self.strategies = None
    
    def _step10_pcs_scoring(self):
        """Step 10: PCS scoring filter"""
        print("\n🎲 STEP 10: PCS Scoring Filter")
        print("-" * 80)
        
        try:
            df_scored = score_pcs_filter(self.strategies)
            
            if df_scored.empty:
                self.log_step("10", "FAIL", reason="PCS scoring returned empty")
                self.strategies = None
                return
            
            # Check PCS thresholds
            if 'PCS_Score' in df_scored.columns:
                passed_pcs = df_scored[df_scored['PCS_Score'] >= 70]
                failed_pcs = df_scored[df_scored['PCS_Score'] < 70]
                
                print(f"\n  PCS Score Results:")
                print(f"     • Passed (≥70): {len(passed_pcs)}")
                print(f"     • Failed (<70): {len(failed_pcs)}")
                
                if len(passed_pcs) > 0:
                    print(f"\n  ✅ Top PCS Scores:")
                    for _, row in passed_pcs.nlargest(3, 'PCS_Score').iterrows():
                        print(f"     • {row['Strategy_Name']}: PCS={row['PCS_Score']:.1f}")
                
                if len(failed_pcs) > 0:
                    print(f"\n  ❌ Failed PCS (<70):")
                    for _, row in failed_pcs.head(3).iterrows():
                        print(f"     • {row['Strategy_Name']}: PCS={row['PCS_Score']:.1f}")
                
                if len(passed_pcs) == 0:
                    avg_score = df_scored['PCS_Score'].mean()
                    self.log_step("10", "FAIL",
                                 detail=f"Avg PCS={avg_score:.1f}",
                                 reason="All strategies PCS < 70")
                    self.strategies = df_scored  # Keep for analysis
                    return
                
                self.log_step("10", "PASS",
                             detail=f"{len(passed_pcs)} passed PCS ≥ 70",
                             count=len(passed_pcs))
                self.strategies = passed_pcs
            else:
                self.log_step("10", "INFO",
                             detail="PCS_Score column not found",
                             count=len(df_scored))
                self.strategies = df_scored
            
        except Exception as e:
            self.log_step("10", "FAIL", reason=str(e))
            # Keep strategies for analysis even if PCS fails
    
    def _step11_final_decision(self):
        """Step 11: Final execution decision"""
        print("\n🎯 STEP 11: Final Execution Decision")
        print("-" * 80)
        
        if self.strategies is None or len(self.strategies) == 0:
            self.final_result = "NO_EXECUTION"
            self.log_step("11", "FAIL",
                         detail="No strategies survived pipeline",
                         reason="See blockers above")
            return
        
        # Check if any strategies are actually executable
        executable = self.strategies[
            (self.strategies.get('Execution_Ready', False) == True) &
            (self.strategies.get('Status', '') == 'Contracts_Fetched')
        ]
        
        if len(executable) == 0:
            self.final_result = "NO_EXECUTION"
            self.log_step("11", "FAIL",
                         detail=f"{len(self.strategies)} strategies but 0 executable",
                         reason="Failed contract fetch or liquidity filters")
            return
        
        # SUCCESS - show executable strategies
        self.final_result = "EXECUTION_READY"
        print(f"\n  ✅ EXECUTABLE STRATEGIES ({len(executable)}):")
        for _, row in executable.iterrows():
            strategy = row.get('Strategy_Name', row.get('Primary_Strategy', 'Unknown'))
            strike = row.get('Selected_Strikes', 'N/A')
            print(f"     • {strategy} | Strike: {strike}")
        
        self.log_step("11", "PASS",
                     detail=f"{len(executable)} executable strategies",
                     count=len(executable))
    
    def _print_summary(self):
        """Print final summary"""
        print("\n" + "="*80)
        print("📋 EXECUTION SUMMARY")
        print("="*80)
        
        print(f"\nTicker: {self.ticker}")
        print(f"Final Result: {self.final_result}")
        
        if self.blockers:
            print(f"\n⛔ BLOCKERS:")
            for blocker in self.blockers:
                print(f"   • {blocker}")
        else:
            print(f"\n✅ No blockers - execution ready!")
        
        # Count pass/fail
        passes = sum(1 for log in self.trace_log if log['result'] == 'PASS')
        fails = sum(1 for log in self.trace_log if log['result'] == 'FAIL')
        
        print(f"\nPipeline Stats:")
        print(f"   • Passed Steps: {passes}")
        print(f"   • Failed Steps: {fails}")
        print(f"   • Total Steps: {len(self.trace_log)}")
        
        print("\n" + "="*80 + "\n")
    
    def _save_structured_log(self) -> str:
        """Save structured JSON log"""
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"debug_execution_trace_{self.ticker}_{timestamp}.json"
        filepath = output_dir / filename
        
        log_data = {
            "ticker": self.ticker,
            "timestamp": datetime.now().isoformat(),
            "min_gap": self.min_gap,
            "final_result": self.final_result,
            "blockers": self.blockers,
            "steps": self.trace_log,
            "summary": {
                "total_steps": len(self.trace_log),
                "passed_steps": sum(1 for log in self.trace_log if log['result'] == 'PASS'),
                "failed_steps": sum(1 for log in self.trace_log if log['result'] == 'FAIL'),
            }
        }
        
        with open(filepath, 'w') as f:
            json.dump(log_data, f, indent=2)
        
        print(f"📄 Structured log saved: {filepath}")
        return str(filepath)


def main():
    """CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Pipeline Debug Mode - Trace single ticker execution",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cli/run_pipeline_debug.py --ticker AAPL
  python cli/run_pipeline_debug.py --ticker MSFT --min-gap 3.5
  
Purpose:
  Diagnose why Tier-1 strategies do or do not result in executable contracts.
  Shows step-by-step PASS/FAIL with explicit reasons.
        """
    )
    
    parser.add_argument('--ticker', '-t', required=True,
                       help='Ticker symbol to debug (e.g., AAPL)')
    parser.add_argument('--min-gap', '-g', type=float, default=2.0,
                       help='Minimum IVHV gap threshold (default: 2.0)')
    
    args = parser.parse_args()
    
    # Run trace
    tracer = PipelineDebugTracer(ticker=args.ticker, min_gap=args.min_gap)
    result = tracer.run_full_trace()
    
    # Exit code based on result
    if result['final_result'] == 'EXECUTION_READY':
        sys.exit(0)  # Success
    else:
        sys.exit(1)  # No execution (not an error, just diagnostic result)


if __name__ == '__main__':
    main()
