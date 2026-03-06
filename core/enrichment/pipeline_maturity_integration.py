"""
Pipeline Maturity Integration - Post-Step12 Processing

This module integrates the new Volatility_Maturity_Tier and Execution_Eligibility
concepts into the pipeline output after Step 12.

It provides a single entry point that:
1. Computes Volatility_Maturity_Tier from iv_history_count
2. Applies execution eligibility (EVALUATED_OK vs EXECUTABLE_NOW)
3. Adds human-readable explanations
4. Generates Fidelity enrichment demand
5. Produces the final scan summary

DESIGN PRINCIPLES:
1. NON-DESTRUCTIVE - Adds columns, never removes or overwrites protected fields
2. DETERMINISTIC - Same input produces same output
3. TRANSPARENT - Every decision is explained in output
4. PROGRESSIVE - "No READY trades" is a healthy, correct outcome
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path
from typing import Tuple, Optional, Dict, Any
from datetime import datetime

from .volatility_maturity import (
    VolatilityMaturityTier,
    compute_maturity_for_dataframe,
    get_tickers_needing_enrichment,
)
from .execution_eligibility import (
    EvaluationStatus,
    ExecutionStatus,
    compute_eligibility_for_dataframe,
    get_executable_trades,
    get_gated_trades,
)
from .trade_explanation import (
    add_explanations_to_dataframe,
    generate_scan_summary,
)
from .fidelity_demand import (
    generate_fidelity_demand,
    DemandReport,
)

logger = logging.getLogger(__name__)

# Protected columns that should never be modified
PROTECTED_COLUMNS = frozenset([
    "Execution_Status",
    "Strategy_Name",
    "Strategy_Type",
    "Ticker",
    "Contract_Symbol",
])


def apply_maturity_and_eligibility(
    df: pd.DataFrame,
    generate_demand: bool = True,
    demand_output_path: Optional[Path] = None,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Apply volatility maturity tier and execution eligibility to pipeline output.

    This is the main integration entry point, called after Step 12.

    Args:
        df: DataFrame from Step 12 (with Execution_Status, Strategy_Name, etc.)
        generate_demand: If True, generate Fidelity demand file
        demand_output_path: Custom path for demand file

    Returns:
        Tuple of (enriched DataFrame, summary dict)
    """
    logger.info("=" * 60)
    logger.info("APPLYING MATURITY & ELIGIBILITY LAYER")
    logger.info("=" * 60)

    if df.empty:
        logger.warning("Empty DataFrame - nothing to process")
        return df, {"status": "empty", "total": 0}

    df = df.copy()
    original_count = len(df)

    # Step 1: Compute Volatility Maturity Tier
    logger.info("Step 1: Computing Volatility_Maturity_Tier...")
    df = compute_maturity_for_dataframe(df)

    tier_counts = df["Volatility_Maturity_Tier"].value_counts().to_dict()
    logger.info(f"   Tier distribution: {tier_counts}")

    # Step 2: Compute Execution Eligibility
    logger.info("Step 2: Computing Execution Eligibility...")
    df = compute_eligibility_for_dataframe(df)

    exec_counts = df["Maturity_Execution_Status"].value_counts().to_dict() if "Maturity_Execution_Status" in df.columns else {}
    logger.info(f"   Maturity execution status: {exec_counts}")

    # Step 3: Add Human-Readable Explanations
    logger.info("Step 3: Adding explanation layer...")
    df = add_explanations_to_dataframe(df)

    # Step 4: Generate Fidelity Demand (if requested)
    demand_report = None
    if generate_demand:
        logger.info("Step 4: Generating Fidelity demand...")
        demand_report = generate_fidelity_demand(df, output_path=demand_output_path)
        logger.info(f"   Tickers needing scrape: {len(demand_report.tickers_needing_scrape)}")

    # Build summary — use Maturity_Execution_Status for maturity-layer counts,
    # and Step12's Execution_Status for actual READY count
    step12_counts = df["Execution_Status"].value_counts().to_dict() if "Execution_Status" in df.columns else {}
    summary = {
        "status": "complete",
        "total": original_count,
        "tier_counts": tier_counts,
        "execution_counts": exec_counts,
        "executable_now": exec_counts.get("EXECUTABLE_NOW", 0),
        "gated_by_maturity": exec_counts.get("GATED_BY_MATURITY", 0),
        "blocked": exec_counts.get("BLOCKED", 0),
        "ready": step12_counts.get("READY", 0),
        "demand_report": demand_report,
    }

    # Log final summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("MATURITY & ELIGIBILITY COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total trades: {original_count}")
    logger.info(f"Step12 READY: {summary['ready']}")
    logger.info(f"Maturity EXECUTABLE_NOW: {summary['executable_now']}")
    logger.info(f"Maturity GATED_BY_MATURITY: {summary['gated_by_maturity']}")
    logger.info(f"Maturity BLOCKED: {summary['blocked']}")

    if summary["gated_by_maturity"] > 0 and summary["ready"] == 0:
        logger.info("")
        logger.info("NOTE: No trades are READY (Step12) and maturity-gated trades exist.")
        logger.info("This is a HEALTHY, CORRECT outcome when IV history is accumulating.")
        logger.info(f"{summary['gated_by_maturity']} trades are awaiting volatility maturity.")

    return df, summary


def get_final_scan_output(df: pd.DataFrame) -> str:
    """
    Generate the final human-readable scan output.

    This should be displayed to the user after a scan completes.
    """
    return generate_scan_summary(df)


def export_with_explanations(
    df: pd.DataFrame,
    output_path: Path,
    include_all_columns: bool = False,
) -> Path:
    """
    Export DataFrame with explanations to CSV.

    Args:
        df: DataFrame with maturity and eligibility columns
        output_path: Path for output file
        include_all_columns: If False, export only key columns

    Returns:
        Path to exported file
    """
    if include_all_columns:
        df_export = df
    else:
        # Select key columns for human review
        key_columns = [
            "Ticker",
            "Strategy_Name",
            "Strategy_Type",
            "Volatility_Maturity_Tier",
            "iv_history_count",
            "days_to_mature",
            "Execution_Status",
            "Evaluation_Status",
            "Explanation_Summary",
            "Missing_Data",
            "Resolution_Path",
            "Is_Healthy_Wait",
            # Include original status for comparison
            "Execution_Status",
            "Gate_Reason",
        ]

        # Only include columns that exist
        available = [c for c in key_columns if c in df.columns]
        df_export = df[available]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_export.to_csv(output_path, index=False)

    logger.info(f"Exported {len(df_export)} trades to {output_path}")
    return output_path


def validate_maturity_consistency(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate that maturity tier is consistent with execution status.

    Returns dict with validation results and any inconsistencies found.
    """
    issues = []

    # Check: INCOME strategies should not be EXECUTABLE_NOW unless MATURE
    if "Strategy_Type" in df.columns and "Execution_Status" in df.columns:
        income_exec = df[
            (df["Strategy_Type"] == "INCOME") &
            (df["Execution_Status"] == "EXECUTABLE_NOW") &
            (df["Volatility_Maturity_Tier"] != "MATURE")
        ]
        if len(income_exec) > 0:
            issues.append({
                "type": "INCOME_NOT_MATURE_BUT_EXECUTABLE",
                "count": len(income_exec),
                "tickers": income_exec["Ticker"].tolist()[:5],
            })

    # Check: DIRECTIONAL strategies should not be EXECUTABLE in SPOT_ONLY
    if "Strategy_Type" in df.columns and "Execution_Status" in df.columns:
        dir_spot = df[
            (df["Strategy_Type"] == "DIRECTIONAL") &
            (df["Execution_Status"] == "EXECUTABLE_NOW") &
            (df["Volatility_Maturity_Tier"] == "SPOT_ONLY")
        ]
        if len(dir_spot) > 0:
            issues.append({
                "type": "DIRECTIONAL_SPOT_ONLY_BUT_EXECUTABLE",
                "count": len(dir_spot),
                "tickers": dir_spot["Ticker"].tolist()[:5],
            })

    return {
        "valid": len(issues) == 0,
        "issues": issues,
    }


def run_post_step12_integration(
    df: pd.DataFrame,
    output_dir: Optional[Path] = None,
) -> Tuple[pd.DataFrame, str]:
    """
    Complete post-Step12 integration with all outputs.

    This is the recommended entry point for pipeline integration.

    Args:
        df: DataFrame from Step 12
        output_dir: Directory for output files (default: output/)

    Returns:
        Tuple of (enriched DataFrame, scan summary string)
    """
    if output_dir is None:
        output_dir = Path("output")

    output_dir.mkdir(parents=True, exist_ok=True)

    # Apply maturity and eligibility
    df_enriched, summary = apply_maturity_and_eligibility(
        df,
        generate_demand=True,
        demand_output_path=output_dir / "fidelity_iv_demand.csv",
    )

    # Validate consistency
    validation = validate_maturity_consistency(df_enriched)
    if not validation["valid"]:
        logger.warning(f"Maturity consistency issues found: {validation['issues']}")

    # Generate scan summary
    scan_summary = get_final_scan_output(df_enriched)

    # Export with explanations
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    export_path = output_dir / f"Step12_WithExplanations_{timestamp}.csv"
    export_with_explanations(df_enriched, export_path)

    return df_enriched, scan_summary
