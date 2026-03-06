"""
Step 12D: Bias-Free Enrichment

PURPOSE:
    Detects missing data requirements and triggers enrichment based PURELY on data fields,
    NOT strategy types. All trades are treated identically regardless of Strategy_Name or Strategy_Type.

STAGE POSITION:
    - Runs AFTER Step 12C (final acceptance gate)
    - BEFORE Step 12E (maturity & eligibility)

STRATEGY AGNOSTICISM GUARANTEE:
    - This function NEVER inspects Strategy_Name, Strategy_Type, or Position_Type
    - All enrichment decisions based on data field values only
    - Same thresholds apply universally to all trades
"""

import pandas as pd
import logging
from datetime import datetime
import duckdb
from core.enrichment.pipeline_hook import run_post_step12_enrichment, validate_no_strategy_bias

logger = logging.getLogger(__name__)


def enrich_bias_free(ctx, run_ts: datetime, con: duckdb.DuckDBPyConnection) -> bool:
    """
    Step 12D: Bias-Free Enrichment System - Post-Step 12 Data Enrichment

    BEHAVIOR:
        1. Detect all unsatisfied data requirements (IV_HISTORY, IV_RANK, etc.)
        2. Emit machine-readable blockers for each trade
        3. Execute resolvers for actionable requirements
        4. Merge enriched data back (DOES NOT modify Execution_Status)
        5. Optionally trigger Step 12 re-evaluation if data improved

    Args:
        ctx: PipelineContext with results['acceptance_all']
        run_ts: Current run timestamp
        con: DuckDB connection for persistence

    Returns:
        bool: True if enrichment completed successfully (always returns True to preserve original data on failure)
    """
    logger.info("=" * 70)
    logger.info("STEP 12D: BIAS-FREE ENRICHMENT SYSTEM")
    logger.info("=" * 70)

    df = ctx.results.get('acceptance_all', pd.DataFrame())
    if df.empty:
        logger.info("No trades to enrich - skipping bias-free enrichment")
        return True

    # Store original for bias validation
    df_before = df.copy()

    try:
        # Execute enrichment passes (max 2 cycles)
        df_enriched, metrics = run_post_step12_enrichment(
            df,
            id_col='Ticker',
            max_cycles=2
        )

        # Log metrics summary
        logger.info("Enrichment Metrics:")
        logger.info(f"  Cycles executed: {metrics.get('cycles', 0)}")
        logger.info(f"  Requirements satisfied: {metrics.get('total_requirements_satisfied', 0)}")
        logger.info(f"  Requirements remaining: {metrics.get('final_requirements_remaining', 0)}")

        # Validate no strategy bias was introduced
        bias_report = validate_no_strategy_bias(df_before, df_enriched)
        if not bias_report['valid']:
            logger.error("STRATEGY BIAS DETECTED - rolling back enrichment")
            for check in bias_report['checks']:
                if not check.get('passed', True):
                    logger.error(f"  FAILED: {check.get('message', check.get('check'))}")
            return True  # Return original data

        for warning in bias_report.get('warnings', []):
            logger.warning(f"  Bias check warning: {warning}")

        # Update context with enriched data
        ctx.results['acceptance_all'] = df_enriched

        # Store enrichment metrics for audit
        ctx.results['enrichment_metrics'] = metrics

        # If significant requirements were satisfied, we could re-run Step 12
        # However, we leave that decision to the pipeline orchestrator
        if metrics.get('total_requirements_satisfied', 0) > 0:
            logger.info("Data was enriched - trades may benefit from Step 12 re-evaluation")

        logger.info("✅ Step 12D: Bias-free enrichment complete")
        return True

    except Exception as e:
        logger.error(f"❌ Step 12D: Bias-free enrichment failed: {e}", exc_info=True)
        # On failure, preserve original data
        return True
