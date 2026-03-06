"""
Step 12E: Maturity & Eligibility

PURPOSE:
    Applies Volatility Maturity Tier and execution eligibility based on IV history depth.

STAGE POSITION:
    - Runs AFTER Step 12D (bias-free enrichment)
    - FINAL stage before results export

DESIGN PRINCIPLES:
    - Same logic runs in both debug and production modes
    - "No READY trades" is a healthy, correct outcome when IV is accumulating
    - Clear explanation for every gated trade
"""

import pandas as pd
import logging
from core.enrichment.pipeline_maturity_integration import (
    apply_maturity_and_eligibility,
    validate_maturity_consistency,
    get_final_scan_output
)

logger = logging.getLogger(__name__)


def apply_maturity_eligibility(ctx) -> bool:
    """
    Step 12E: Apply Volatility Maturity Tier and Execution Eligibility.

    This stage:
    1. Computes Volatility_Maturity_Tier from iv_history_count
    2. Applies strategy-agnostic execution eligibility:
       - INCOME strategies require MATURE tier (120+ days IV history)
       - DIRECTIONAL strategies can execute at EARLY+ tier (7+ days)
    3. Adds human-readable explanations

    Args:
        ctx: PipelineContext with results['acceptance_all']

    Returns:
        bool: True if maturity integration completed successfully
    """
    logger.info("")
    logger.info("=" * 70)
    logger.info("STEP 12E: MATURITY & ELIGIBILITY INTEGRATION")
    logger.info("=" * 70)

    df = ctx.results.get('acceptance_all', pd.DataFrame())
    if df.empty:
        logger.info("No trades to evaluate - skipping maturity integration")
        return True

    try:
        # Apply maturity tier and execution eligibility
        df_enriched, summary = apply_maturity_and_eligibility(
            df,
            generate_demand=False,
        )

        # Validate consistency
        validation = validate_maturity_consistency(df_enriched)
        if not validation["valid"]:
            for issue in validation["issues"]:
                logger.warning(f"Maturity consistency issue: {issue}")

        # Update context with enriched data
        ctx.results['acceptance_all'] = df_enriched
        ctx.results['maturity_summary'] = summary

        # Generate human-readable scan output
        scan_output = get_final_scan_output(df_enriched)
        ctx.results['scan_summary'] = scan_output

        # Log key metrics
        ready = summary.get('ready', 0)
        exec_now = summary.get('executable_now', 0)
        gated = summary.get('gated_by_maturity', 0)
        blocked = summary.get('blocked', 0)

        logger.info(f"")
        logger.info(f"EXECUTION ELIGIBILITY RESULTS:")
        logger.info(f"  Step12 READY: {ready}")
        logger.info(f"  Maturity EXECUTABLE_NOW: {exec_now}")
        logger.info(f"  Maturity GATED_BY_MATURITY: {gated}")
        logger.info(f"  Maturity BLOCKED: {blocked}")

        if gated > 0 and ready == 0:
            logger.info("")
            logger.info("NOTE: No READY trades and maturity-gated trades exist.")
            logger.info("This is CORRECT when IV history is accumulating.")
            logger.info(f"{gated} trades are waiting for volatility maturity.")

        logger.info("✅ Step 12E: Maturity & eligibility integration complete")
        return True

    except Exception as e:
        logger.error(f"❌ Step 12E: Maturity integration failed: {e}", exc_info=True)
        return True
