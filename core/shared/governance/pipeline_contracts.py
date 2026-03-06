"""
Pipeline Step Contracts - Fail-Fast Data Validation

This module defines step-level data contracts for critical pipeline stages.
Each contract specifies REQUIRED vs OPTIONAL fields with explicit failure modes.

DESIGN PRINCIPLE: Fail-fast validation at step boundaries prevents silent data degradation.

USAGE:
    from core.shared.governance.pipeline_contracts import validate_step_output, STEP_2_OUTPUTS

    # After Step 2 completes:
    validate_step_output(df, step_num=2, contract=STEP_2_OUTPUTS)  # Raises ValueError on violation

CONTRACT STRUCTURE:
    {
        "field_name": {
            "requirement": "REQUIRED" | "OPTIONAL",
            "dtype": "float64" | "str" | etc. (optional),
            "valid_values": [...] (optional enum check),
            "range": [min, max] (optional numeric range),
            "failure_mode": "HALT" | "WARN" | "INFO",
            "source": "Step X (Description)" (documentation)
        }
    }
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


# ============================================================
# STEP 2: SNAPSHOT LOAD & ENRICHMENT
# ============================================================

STEP_2_OUTPUTS = {
    # Canonical Market Context (owned by Step 2)
    "Signal_Type": {
        "requirement": "REQUIRED",
        "dtype": "str",
        "valid_values": ["Bullish", "Bearish", "Bidirectional", "Unknown"],
        "failure_mode": "WARN",
        "source": "Step 2 (Snapshot - Murphy indicators)",
        "notes": "Authoritative directional bias. Can be 'Unknown' when OHLC data is missing (demand-driven). MUST NOT be overwritten by downstream steps. Strict validation at Step 10/12."
    },
    "Regime": {
        "requirement": "REQUIRED",
        "dtype": "str",
        "valid_values": ["High Vol", "Low Vol", "Compression", "Expansion", "Unknown"],
        "failure_mode": "WARN",
        "source": "Step 2 (Snapshot - IV_Rank + IV_Trend + VVIX)",
        "notes": "Authoritative volatility regime. Can be 'Unknown' when IV_Rank data is missing (demand-driven). MUST NOT be overwritten by downstream steps. Strict validation at Step 10/12."
    },

    # IV Maturity & Quality
    "IV_Maturity_State": {
        "requirement": "REQUIRED",
        "dtype": "str",
        "valid_values": ["MATURE", "PARTIAL_MATURE", "IMMATURE", "MISSING"],
        "failure_mode": "HALT",
        "source": "Step 2 (iv_term_history count → maturity_classifier.py)",
        "notes": "Based on days of IV history: MATURE ≥120d, PARTIAL_MATURE 60-119d, IMMATURE 1-59d, MISSING 0d"
    },
    "IV_Rank_30D": {
        "requirement": "OPTIONAL",
        "dtype": "float64",
        "range": [0.0, 100.0],
        "failure_mode": "WARN",
        "source": "Step 2 (Fidelity long-term IV database)",
        "notes": "Percentile ranking from Fidelity. Can be NaN when Fidelity data is legitimately missing (demand-driven). Strict validation deferred to Step 10/12."
    },
    "IV_Rank_Source": {
        "requirement": "OPTIONAL",
        "dtype": "str",
        "valid_values": ["ROLLING_20D", "ROLLING_30D", "ROLLING_60D", "ROLLING_252D"],
        "failure_mode": "WARN",
        "source": "Step 2 (IVEngine rolling rank)",
        "notes": "Tracks which rolling window was used for IV_Rank. Set by IVEngine from iv_term_history data."
    },

    # Ticker Identity
    "Ticker": {
        "requirement": "REQUIRED",
        "dtype": "str",
        "failure_mode": "HALT",
        "source": "Step 2 (Snapshot input)"
    },
    "Stock_Price": {
        "requirement": "REQUIRED",
        "dtype": "float64",
        "failure_mode": "HALT",
        "source": "Step 2 (Snapshot input)",
        "notes": "Must be > 0"
    }
}


# ============================================================
# STEP 10: SCHWAB CONTRACT FETCH (old Step 9B)
# ============================================================

STEP_10_OUTPUTS = {
    # Greeks (required for PCS scoring and risk assessment)
    "Delta": {
        "requirement": "REQUIRED",
        "dtype": "float64",
        "failure_mode": "HALT",
        "source": "Step 10 (Schwab API)",
        "notes": "Cannot be NaN. If Schwab auth fails, pipeline should HALT, not continue with NaN."
    },
    "Gamma": {
        "requirement": "REQUIRED",
        "dtype": "float64",
        "failure_mode": "HALT",
        "source": "Step 10 (Schwab API)"
    },
    "Vega": {
        "requirement": "REQUIRED",
        "dtype": "float64",
        "failure_mode": "HALT",
        "source": "Step 10 (Schwab API)"
    },
    "Theta": {
        "requirement": "REQUIRED",
        "dtype": "float64",
        "failure_mode": "HALT",
        "source": "Step 10 (Schwab API)"
    },

    # Contract Identity
    "Contract_Symbol": {
        "requirement": "REQUIRED",
        "dtype": "str",
        "failure_mode": "HALT",
        "source": "Step 10 (Schwab API)",
        "notes": "OCC symbol for selected contract"
    },
    "Strike": {
        "requirement": "REQUIRED",
        "dtype": "float64",
        "failure_mode": "HALT",
        "source": "Step 10 (Schwab API)"
    },

    # Pricing
    "Bid": {
        "requirement": "REQUIRED",
        "dtype": "float64",
        "failure_mode": "HALT",
        "source": "Step 10 (Schwab API)",
        "notes": "Must be ≥ 0"
    },
    "Ask": {
        "requirement": "REQUIRED",
        "dtype": "float64",
        "failure_mode": "HALT",
        "source": "Step 10 (Schwab API)",
        "notes": "Must be ≥ Bid"
    },
    "Mid_Price": {
        "requirement": "REQUIRED",
        "dtype": "float64",
        "failure_mode": "HALT",
        "source": "Step 10 (computed from Bid/Ask)"
    }
}


# ============================================================
# STEP 12: ACCEPTANCE LAYER INPUTS (extends existing STEP_12_INPUTS)
# ============================================================

STEP_12_REQUIRED_INPUTS = {
    # Liquidity & Execution Quality
    "Liquidity_Grade": {
        "requirement": "REQUIRED",
        "dtype": "str",
        "valid_values": ["Excellent", "Good", "Acceptable", "Thin", "Illiquid"],
        "failure_mode": "HALT",
        "source": "Step 10 (Schwab contract data)",
        "notes": "Cannot default to 'Illiquid'. Missing value = pipeline HALT."
    },
    "Data_Completeness_Overall": {
        "requirement": "REQUIRED",
        "dtype": "str",
        "valid_values": ["Complete", "Partial", "Missing"],
        "failure_mode": "HALT",
        "source": "Step 11 (PCS recalibration)",
        "notes": "Tracks whether all required data for strategy evaluation is present."
    },

    # Greeks (must be present from Step 10)
    "Delta": {
        "requirement": "REQUIRED",
        "dtype": "float64",
        "failure_mode": "HALT",
        "source": "Step 10 (Schwab API)"
    },
    "Gamma": {
        "requirement": "REQUIRED",
        "dtype": "float64",
        "failure_mode": "HALT",
        "source": "Step 10 (Schwab API)"
    },

    # Scraper Status
    "Scraper_Status": {
        "requirement": "REQUIRED",
        "dtype": "str",
        "valid_values": ["OK", "PARTIAL", "FAILED", "AUTH_BLOCKED"],
        "failure_mode": "HALT",
        "source": "Step 10 (Schwab API status)",
        "notes": "Cannot default to 'FAILED'. Missing status = unclear data provenance."
    }
}


# ============================================================
# VALIDATION FUNCTIONS
# ============================================================

def validate_step_output(
    df: pd.DataFrame,
    step_num: int,
    contract: Dict[str, Dict[str, Any]]
) -> None:
    """
    Validates DataFrame against step-level data contract. Raises exception on HALT violations.

    This function implements fail-fast validation at pipeline step boundaries.

    Args:
        df: DataFrame to validate
        step_num: Step number (e.g., 2, 10, 12) for error messages
        contract: Contract dictionary (e.g., STEP_2_OUTPUTS)

    Raises:
        ValueError: If any REQUIRED field is missing or contains invalid values (failure_mode: HALT)
        RuntimeError: If DataFrame is empty when data is expected

    Example:
        >>> from core.shared.governance.pipeline_contracts import validate_step_output, STEP_2_OUTPUTS
        >>> validate_step_output(df, step_num=2, contract=STEP_2_OUTPUTS)
        # Raises ValueError if Signal_Type, Regime, or IV_Rank_30D are missing
    """
    step_label = f"Step {step_num}"

    # Check for empty DataFrame
    if df is None or df.empty:
        raise RuntimeError(
            f"❌ {step_label}: DataFrame is empty. Cannot continue pipeline without data."
        )

    violations = []
    warnings = []

    for field, spec in contract.items():
        requirement = spec.get("requirement", "OPTIONAL")
        failure_mode = spec.get("failure_mode", "INFO")

        # Check field existence
        if field not in df.columns:
            if requirement == "REQUIRED" and failure_mode == "HALT":
                violations.append(f"Missing REQUIRED field: {field}")
            elif requirement == "REQUIRED":
                warnings.append(f"Missing required field: {field} (failure_mode: {failure_mode})")
            continue

        # Check for all-null columns
        if df[field].isna().all():
            if requirement == "REQUIRED" and failure_mode == "HALT":
                violations.append(f"Field {field} is all-null (REQUIRED)")
            elif requirement == "REQUIRED":
                warnings.append(f"Field {field} is all-null (requirement: {requirement})")

        # Enum validation
        valid_values = spec.get("valid_values")
        if valid_values is not None and field in df.columns:
            # Exclude NaN from validation if field is not all-null
            non_null_values = df[field].dropna()
            if len(non_null_values) > 0:
                invalid = non_null_values[~non_null_values.isin(valid_values)].unique()
                if len(invalid) > 0:
                    if failure_mode == "HALT":
                        violations.append(
                            f"Field {field} has invalid values: {invalid.tolist()} "
                            f"(valid: {valid_values})"
                        )
                    else:
                        warnings.append(
                            f"Field {field} has invalid values: {invalid.tolist()}"
                        )

        # Range validation (numeric fields)
        value_range = spec.get("range")
        if value_range is not None and field in df.columns:
            min_val, max_val = value_range
            non_null_values = df[field].dropna()
            if len(non_null_values) > 0:
                out_of_range = (
                    (non_null_values < min_val) | (non_null_values > max_val)
                ).sum()
                if out_of_range > 0:
                    if failure_mode == "HALT":
                        violations.append(
                            f"Field {field} has {out_of_range} values out of range "
                            f"[{min_val}, {max_val}]"
                        )
                    else:
                        warnings.append(
                            f"Field {field} has {out_of_range} values out of range"
                        )

        # Dtype validation
        expected_dtype = spec.get("dtype")
        if expected_dtype and field in df.columns:
            actual_dtype = str(df[field].dtype)
            # Flexible dtype matching (float64 matches float, object matches str, etc.)
            if expected_dtype == "float64" and not pd.api.types.is_float_dtype(df[field]):
                warnings.append(f"Field {field} has dtype {actual_dtype}, expected {expected_dtype}")
            elif expected_dtype == "str" and not pd.api.types.is_object_dtype(df[field]):
                warnings.append(f"Field {field} has dtype {actual_dtype}, expected {expected_dtype}")

    # Log warnings
    for warning in warnings:
        logger.warning(f"⚠️ {step_label} validation warning: {warning}")

    # Fail fast on violations
    if violations:
        error_msg = f"❌ {step_label} VALIDATION FAILED:\n" + "\n".join(f"  - {v}" for v in violations)
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info(f"✅ {step_label} validation passed ({len(df)} rows)")


def validate_no_overwrites(
    df_before: pd.DataFrame,
    df_after: pd.DataFrame,
    canonical_fields: List[str],
    stage_name: str = "enrichment"
) -> None:
    """
    Validates that canonical fields were not modified by enrichment or other downstream processes.

    This function enforces single data authority - fields owned by earlier steps (e.g., Step 2)
    must not be overwritten by later enrichment stages.

    Args:
        df_before: DataFrame before enrichment
        df_after: DataFrame after enrichment
        canonical_fields: List of field names that must remain unchanged
        stage_name: Name of stage for error messages (default: "enrichment")

    Raises:
        ValueError: If any canonical field was modified

    Example:
        >>> canonical = ['Signal_Type', 'Regime', 'IV_Rank_30D', 'IV_Maturity_State']
        >>> validate_no_overwrites(df_before, df_after, canonical, stage_name="Step 12D enrichment")
    """
    violations = []

    for field in canonical_fields:
        if field not in df_before.columns:
            logger.warning(f"⚠️ Canonical field {field} missing in 'before' DataFrame")
            continue

        if field not in df_after.columns:
            violations.append(f"Canonical field {field} was REMOVED by {stage_name}")
            continue

        # Compare values (handle NaN equality)
        changed_mask = df_before[field] != df_after[field]
        # NaN == NaN should be True for this check
        changed_mask = changed_mask & ~(df_before[field].isna() & df_after[field].isna())

        changed_count = changed_mask.sum()
        if changed_count > 0:
            violations.append(
                f"Canonical field {field} was modified by {stage_name} "
                f"({changed_count}/{len(df_before)} rows changed)"
            )

    if violations:
        error_msg = f"❌ AUTHORITY VIOLATION in {stage_name}:\n" + "\n".join(f"  - {v}" for v in violations)
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info(f"✅ Authority validation passed: {canonical_fields} unchanged after {stage_name}")


# ============================================================
# CANONICAL FIELD DEFINITIONS
# ============================================================

# Fields owned by Step 2 that MUST NOT be overwritten by downstream steps
STEP_2_CANONICAL_FIELDS = [
    "Signal_Type",
    "Regime",
    "IV_Rank_30D",
    "IV_Maturity_State",
    "IV_Rank_Source"
]

# Fields owned by Step 10 that MUST NOT be overwritten by downstream steps
STEP_10_CANONICAL_FIELDS = [
    "Delta",
    "Gamma",
    "Vega",
    "Theta",
    "Contract_Symbol",
    "Strike",
    "Bid",
    "Ask",
    "Mid_Price"
]
