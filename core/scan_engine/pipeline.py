"""
Full Scan Pipeline Orchestrator

Combines all steps into a single run_full_scan_pipeline() function.
"""

import pandas as pd
import logging
import os
from pathlib import Path
from datetime import datetime

from .step2_load_snapshot import load_ivhv_snapshot
from .step3_filter_ivhv import filter_ivhv_gap
from .step5_chart_signals import compute_chart_signals
from .step6_gem_filter import validate_data_quality
from .step7_strategy_recommendation import recommend_strategies
from .step8_position_sizing import allocate_portfolio_capital
from .step9a_determine_timeframe import determine_timeframe
from .step9b_fetch_contracts_schwab import fetch_and_select_contracts_schwab  # Production Schwab version
from .step11_independent_evaluation import evaluate_strategies_independently
from .step12_acceptance import apply_acceptance_logic, filter_ready_contracts  # Phase 3 acceptance logic

logger = logging.getLogger(__name__)


def run_full_scan_pipeline(
    snapshot_path: str, # Now a required argument, resolved by caller
    output_dir: str = None,
    account_balance: float = 100000.0,
    max_portfolio_risk: float = 0.20,
    sizing_method: str = 'volatility_scaled'
) -> dict:
    """
    Run the complete scan pipeline.
    
    Purpose:
        Execute all authoritative steps in sequence with automatic error handling and output exports.
        Returns all intermediate DataFrames for inspection and debugging.
    
    🚨 AUTHORITATIVE PIPELINE FLOW:
    Step 2 → Step 3 → Step 5 → Step 6 → Step 7 → Step 11 → Step 9A → Step 9B → Step 12 → Step 8
    This flow is orchestrated exclusively by `core/scan_engine/pipeline.py`.
    
    # AGENT SAFETY: This file is the ONLY valid entry point for the Scan Engine.
    # No UI, test, or script is permitted to invoke intermediate steps directly in production contexts.
    # This prevents agents from "helpfully" resurrecting invalid logic or bypassing architectural boundaries.

    Pipeline Steps:
        DESCRIPTIVE (Strategy-Neutral):
        1. Step 2: Load IV/HV snapshot with enrichment (core/scan_engine/step2_load_snapshot.py)
        2. Step 3: Filter by IVHV gap and classify volatility regimes (core/scan_engine/step3_filter_ivhv.py)
        3. Step 5: Compute chart signals and regime classification (core/scan_engine/step5_chart_signals.py)
        4. Step 6: Validate data completeness and quality (core/scan_engine/step6_gem_filter.py)
        
        PRESCRIPTIVE (Strategy-Specific):
        5. Step 7: Strategy Recommendation - Multi-strategy ledger (core/scan_engine/step7_strategy_recommendation.py)
        6. Step 11: Independent Strategy Evaluation (core/scan_engine/step11_independent_evaluation.py)
        7. Step 9A: Determine timeframes for each strategy (core/scan_engine/step9a_determine_timeframe.py)
        8. Step 9B: Fetch option contracts (core/scan_engine/step9b_fetch_contracts.py)
        9. Step 12: Acceptance Logic - Phase 1-2-3 enrichment (core/scan_engine/step12_acceptance.py)
        10. Step 8: Final selection & position sizing (core/scan_engine/step8_position_sizing.py)
    
    Args:
        snapshot_path (str): The absolute path to the IV/HV CSV file, resolved by the caller.
        output_dir (str, optional): Directory for CSV exports. Uses OUTPUT_DIR env var if None.
        account_balance (float): Account size for position sizing. Default $100,000.
        max_portfolio_risk (float): Max portfolio risk (0-1). Default 0.20 (20%).
        sizing_method (str): Position sizing method. Options: 'fixed_fractional', 'kelly', 
                            'volatility_scaled', 'equal_weight'. Default 'volatility_scaled'.
    
    Returns:
        dict: Dictionary with keys:
            - 'snapshot': Raw IV/HV data (Step 2)
            - 'filtered': IVHV-filtered tickers (Step 3)
            - 'charted': Chart-enriched tickers (Step 5)
            - 'validated_data': Data quality validated tickers (Step 6)
            - 'recommended_strategies': Multi-strategy recommendations (Step 7)
            - 'evaluated_strategies': Evaluated strategies (Step 11)
            - 'timeframes': Strategy timeframes (Step 9A)
            - 'selected_contracts': Selected contracts (Step 9B)
            - 'acceptance_all': All contracts with acceptance status (Step 12)
            - 'acceptance_ready': READY_NOW contracts with MEDIUM+ confidence (Step 12)
            - 'final_trades': Final selected & sized positions (Step 8)
            Empty dict keys if step fails
    
    Side Effects:
        - Exports CSV files to output_dir with timestamps:
          - Step3_Filtered_YYYYMMDD_HHMMSS.csv
          - Step5_Charted_YYYYMMDD_HHMMSS.csv
          - Step6_Validated_YYYYMMDD_HHMMSS.csv
          - Step7_Recommended_YYYYMMDD_HHMMSS.csv
          - Step11_Evaluated_YYYYMMDD_HHMMSS.csv
          - Step9A_Timeframes_YYYYMMDD_HHMMSS.csv
          - Step9B_SelectedContracts_YYYYMMDD_HHMMSS.csv
          - Step8_Final_YYYYMMDD_HHMMSS.csv
    
    Error Handling:
        - Logs errors at each step
        - Stops pipeline if critical step fails
        - Returns partial results if later steps fail
    
    Example:
        >>> # Run full pipeline with all steps
        >>> results = run_full_scan_pipeline(
        ...     snapshot_path="data/ivhv_snapshot_20250101.csv",
        ...     account_balance=50000,
        ...     max_portfolio_risk=0.15,
        ...     sizing_method='volatility_scaled'
        ... )
        >>> print(f"Final trades: {len(results['final_trades'])}")
        >>> 
        >>> # Run descriptive steps only (no strategy or sizing)
        >>> results = run_full_scan_pipeline(snapshot_path="data/ivhv_snapshot_20250101.csv")
        >>> validated_df = results['validated_data']
    
    Performance:
        - Step 2: <1 second (file load)
        - Step 3: <1 second (filtering)
        - Step 5: ~1 sec per ticker (yfinance API)
        - Step 6: <1 second (validation)
        - Step 7: ~0.1 sec per ticker (strategy generation)
        - Step 11: ~0.1 sec per strategy (evaluation)
        - Step 9A: <1 second (timeframe assignment)
        - Step 9B: ~2 sec per ticker (contract fetch)
        - Step 8: <1 second (position sizing)
        Total for 50 tickers: ~120 seconds (with contract fetching)
    """
    if output_dir is None:
        output_dir = Path(os.getenv('OUTPUT_DIR', './output'))
    else:
        output_dir = Path(output_dir)
    
    output_dir.mkdir(parents=True, exist_ok=True)

    results = {}
    timeframes_df = pd.DataFrame() # Initialize
    evaluated_strategies_with_contracts = pd.DataFrame() # Initialize

    # Step 2: Load snapshot
    logger.info("📊 Step 2: Loading IV/HV snapshot...")
    df_snapshot = load_ivhv_snapshot(snapshot_path)
    results['snapshot'] = df_snapshot

    # Step 3: Filter by IVHV gap
    logger.info("📊 Step 3: Filtering by IVHV gap...")
    df_filtered = filter_ivhv_gap(df_snapshot)
    results['filtered'] = df_filtered

    if df_filtered.empty:
        logger.warning("⚠️ No tickers passed Step 3. Pipeline stopped.")
        return results

    # Step 5: Chart scoring
    logger.info("📊 Step 5: Computing chart signals...")
    df_charted = compute_chart_signals(df_filtered)
    results['charted'] = df_charted

    if df_charted.empty:
        logger.warning("⚠️ No tickers passed Step 5. Pipeline stopped.")
        return results

    # Step 6: Data quality validation
    logger.info("📊 Step 6: Validating data quality...")
    validated_data = validate_data_quality(df_charted)
    results['validated_data'] = validated_data

    if validated_data.empty:
        logger.warning("⚠️ No tickers passed Step 6. Pipeline stopped.")
        return results

    # Step 7: Strategy Recommendation (Multi-Strategy Ledger)
    logger.info("🎯 Step 7: Generating strategy recommendations...")
    try:
        recommended_strategies = recommend_strategies(validated_data)
        results['recommended_strategies'] = recommended_strategies
        logger.info(f"✅ Step 7 complete: {len(recommended_strategies)} strategies recommended")
        logger.debug(f"DEBUG: Columns after Step 7: {recommended_strategies.columns.tolist()}")
    except Exception as e:
        logger.error(f"❌ Step 7 failed: {e}")
        results['recommended_strategies'] = pd.DataFrame()
        return results

    if recommended_strategies.empty:
        logger.warning("⚠️ No strategies recommended from Step 7. Pipeline stopped.")
        return results

    # Step 11: Independent Strategy Evaluation
    logger.info("🎯 Step 11: Independent strategy evaluation...")
    try:
        evaluated_strategies = evaluate_strategies_independently(
            recommended_strategies,
            user_goal='income',
            account_size=account_balance,
            risk_tolerance='moderate'
        )
        results['evaluated_strategies'] = evaluated_strategies
        logger.info(f"✅ Step 11 complete: {len(evaluated_strategies)} strategies independently evaluated")
        logger.debug(f"DEBUG: Columns after Step 11: {evaluated_strategies.columns.tolist()}")
    except Exception as e:
        logger.error(f"❌ Step 11 failed: {e}")
        results['evaluated_strategies'] = pd.DataFrame()

    # Conditional execution for Step 9A and 9B
    if results['evaluated_strategies'].empty:
        logger.warning("⚠️ Step 9A/9B skipped: No evaluated strategies from Step 11")
        results['timeframes'] = pd.DataFrame()
        results['selected_contracts'] = pd.DataFrame()
        evaluated_strategies_with_contracts = pd.DataFrame() # Ensure this is empty if no strategies
    else:
        # Step 9A: Determine Timeframe
        logger.info("⏱️ Step 9A: Determining timeframes for evaluated strategies...")
        try:
            timeframes_df = determine_timeframe(results['evaluated_strategies'])
            results['timeframes'] = timeframes_df
            logger.info(f"✅ Step 9A complete: {len(timeframes_df)} timeframes determined")
        except Exception as e:
            logger.error(f"❌ Step 9A failed: {e}")
            results['timeframes'] = pd.DataFrame()
            timeframes_df = pd.DataFrame() # Ensure timeframes_df is empty on failure

        if timeframes_df.empty:
            logger.warning("⚠️ Step 9B skipped: No timeframes from Step 9A")
            results['selected_contracts'] = pd.DataFrame()
            evaluated_strategies_with_contracts = results['evaluated_strategies'] # Pass original strategies if no contracts
        else:
            # Step 9B: Fetch and Select Contracts (Schwab Production)
            logger.info("⛓️ Step 9B: Fetching and selecting contracts from Schwab...")
            try:
                evaluated_strategies_with_contracts = fetch_and_select_contracts_schwab(
                    results['evaluated_strategies'],
                    timeframes_df
                )
                results['selected_contracts'] = evaluated_strategies_with_contracts
                logger.info(f"✅ Step 9B complete: {len(evaluated_strategies_with_contracts)} contracts selected")
            except Exception as e:
                logger.error(f"❌ Step 9B failed: {e}", exc_info=True) # Added exc_info
                results['selected_contracts'] = pd.DataFrame()
                evaluated_strategies_with_contracts = results['evaluated_strategies'] # Fallback to original strategies

    # Step 12: Acceptance Logic (Phase 3)
    if not evaluated_strategies_with_contracts.empty:
        logger.info("✅ Step 12: Applying acceptance logic (Phase 1-2-3)...")
        try:
            # Apply acceptance logic to all contracts
            evaluated_strategies_with_contracts = apply_acceptance_logic(evaluated_strategies_with_contracts)
            
            # Filter for READY_NOW with MEDIUM+ confidence
            ready_contracts = filter_ready_contracts(evaluated_strategies_with_contracts, min_confidence='MEDIUM')
            
            if not ready_contracts.empty:
                logger.info(f"✅ Step 12 complete: {len(ready_contracts)} READY_NOW contracts (MEDIUM+ confidence)")
                logger.info(f"   Total evaluated: {len(evaluated_strategies_with_contracts)} | "
                           f"Filtered: {len(evaluated_strategies_with_contracts) - len(ready_contracts)}")
            else:
                logger.info("⚠️ Step 12: No READY_NOW contracts at MEDIUM+ confidence")
                logger.info(f"   All {len(evaluated_strategies_with_contracts)} contracts filtered by acceptance logic")
            
            # Store both full and filtered results
            results['acceptance_all'] = evaluated_strategies_with_contracts  # All contracts with acceptance status
            results['acceptance_ready'] = ready_contracts  # Only READY_NOW with MEDIUM+ confidence
            
            # Use ready_contracts for Step 8 position sizing
            evaluated_strategies_with_contracts = ready_contracts
            
        except Exception as e:
            logger.error(f"❌ Step 12 failed: {e}", exc_info=True)
            # Continue with unfiltered contracts if acceptance logic fails
            results['acceptance_all'] = pd.DataFrame()
            results['acceptance_ready'] = pd.DataFrame()
    else:
        logger.warning("⚠️ Step 12 skipped: No contracts from Step 9B")
        results['acceptance_all'] = pd.DataFrame()
        results['acceptance_ready'] = pd.DataFrame()

    # Step 8: Final Selection & Position Sizing
    # Use evaluated_strategies_with_contracts, which is either the enriched DF or the original evaluated_strategies
    if evaluated_strategies_with_contracts.empty:
        logger.warning("⚠️ Step 8 skipped: No evaluated strategies or contracts for sizing")
        results['final_trades'] = pd.DataFrame()
    else:
        logger.info("💰 Step 8: Portfolio capital allocation...")
        try:
            final_trades = allocate_portfolio_capital(
                evaluated_strategies_with_contracts,
                account_balance=account_balance,
                max_portfolio_risk=max_portfolio_risk,
                max_trade_risk=0.02,
                min_compliance_score=60.0,
                max_strategies_per_ticker=50,
                sizing_method=sizing_method
            )
            results['final_trades'] = final_trades
            logger.info(f"✅ Step 8 complete: {len(final_trades)} final trades selected")
        except Exception as e:
            logger.error(f"❌ Step 8 failed: {e}", exc_info=True) # Added exc_info
            results['final_trades'] = pd.DataFrame()

    # Export results
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        df_filtered.to_csv(output_dir / f"Step3_Filtered_{timestamp}.csv", index=False)
        df_charted.to_csv(output_dir / f"Step5_Charted_{timestamp}.csv", index=False)
        validated_data.to_csv(output_dir / f"Step6_Validated_{timestamp}.csv", index=False)
        if not results.get('recommended_strategies', pd.DataFrame()).empty:
            results['recommended_strategies'].to_csv(output_dir / f"Step7_Recommended_{timestamp}.csv", index=False)
        if not results['evaluated_strategies'].empty:
            results['evaluated_strategies'].to_csv(output_dir / f"Step11_Evaluated_{timestamp}.csv", index=False)
        if not results['timeframes'].empty: # Export Step 9A output
            results['timeframes'].to_csv(output_dir / f"Step9A_Timeframes_{timestamp}.csv", index=False)
        if not results['selected_contracts'].empty: # Export Step 9B output
            results['selected_contracts'].to_csv(output_dir / f"Step9B_SelectedContracts_{timestamp}.csv", index=False)
        if not results.get('acceptance_all', pd.DataFrame()).empty: # Export Step 12 all contracts
            results['acceptance_all'].to_csv(output_dir / f"Step12_Acceptance_{timestamp}.csv", index=False)
        if not results.get('acceptance_ready', pd.DataFrame()).empty: # Export Step 12 READY_NOW
            results['acceptance_ready'].to_csv(output_dir / f"Step12_Ready_{timestamp}.csv", index=False)
        if not results['final_trades'].empty:
            results['final_trades'].to_csv(output_dir / f"Step8_Final_{timestamp}.csv", index=False)
        logger.info(f"✅ Exports complete → {output_dir}")
    except Exception as e:
        logger.error(f"❌ Export failed: {e}", exc_info=True) # Added exc_info

    return results
