"""
Governance Data Contracts - Authoritative Truth Table

This file defines the explicit data contracts for the options scan pipeline.
It serves as a read-only reference for field provenance, requirements, and failure modes.

STRICT GOVERNANCE RULES:
1. If a REQUIRED field is missing, the row must be DEFERRED (WAIT) or HALTED.
2. No heuristics or soft logic may be used to fill missing data.
3. Step 12 is the sovereign decision authority for execution eligibility.
"""

# ============================================================
# STEP 12: ACCEPTANCE LAYER CONTRACT
# ============================================================

STEP_12_INPUTS = {
    # Phase 1: Entry Quality (REQUIRED)
    "compression_tag": {
        "source": "Step 5 (Charts)",
        "requirement": "REQUIRED",
        "failure_mode": "WAIT",
        "valid_values": ["COMPRESSION", "NORMAL", "EXPANSION"]
    },
    "gap_tag": {
        "source": "Step 5 (Charts)",
        "requirement": "REQUIRED",
        "failure_mode": "WAIT",
        "valid_values": ["NO_GAP", "GAP_UP", "GAP_DOWN"]
    },
    "intraday_position_tag": {
        "source": "Step 5 (Charts)",
        "requirement": "REQUIRED",
        "failure_mode": "WAIT",
        "valid_values": ["NEAR_LOW", "MID_RANGE", "NEAR_HIGH"]
    },
    "52w_regime_tag": {
        "source": "Step 5 (Charts)",
        "requirement": "REQUIRED",
        "failure_mode": "WAIT",
        "valid_values": ["NEAR_52W_LOW", "MID_RANGE", "NEAR_52W_HIGH"]
    },
    "momentum_tag": {
        "source": "Step 5 (Charts)",
        "requirement": "REQUIRED",
        "failure_mode": "WAIT",
        "valid_values": ["STRONG_DOWN_DAY", "FLAT_DAY", "NORMAL", "STRONG_UP_DAY"]
    },
    "entry_timing_context": {
        "source": "Step 5 (Charts)",
        "requirement": "REQUIRED",
        "failure_mode": "WAIT",
        "valid_values": ["EARLY_LONG", "MODERATE", "LATE_LONG", "EARLY_SHORT", "LATE_SHORT"]
    },

    # Phase 2: Execution Quality (OPTIONAL)
    "execution_quality": {
        "source": "Step 9B (Contracts)",
        "requirement": "OPTIONAL",
        "failure_mode": "INFO",
        "default": "UNKNOWN"
    },
    "balance_tag": {
        "source": "Step 9B (Contracts)",
        "requirement": "OPTIONAL",
        "failure_mode": "INFO",
        "default": "UNKNOWN"
    },
    "dividend_risk": {
        "source": "Step 9B (Contracts)",
        "requirement": "OPTIONAL",
        "failure_mode": "INFO",
        "default": "UNKNOWN"
    },

    # Phase 3: Volatility Identity (STRATEGY-DEPENDENT)
    "IV_Rank_30D": {
        "source": "Step 2 (Snapshot)",
        "requirement": "STRATEGY_DEPENDENT",
        "failure_mode": "WAIT (Volatility) | INFO (Directional/Income)",
        "notes": "Hard gate for Volatility strategies; informational for others."
    },
    "history_depth_ok": {
        "source": "Step 2 (Snapshot)",
        "requirement": "REQUIRED",
        "failure_mode": "INFO (Confidence Cap)",
        "notes": "If False, confidence is capped at LOW."
    }
}

def validate_phase_output(df, phase, required_cols=None, enum_checks=None):
    """
    Validates that a DataFrame meets the contract for a specific phase.
    
    Args:
        df: DataFrame to validate
        phase: Phase identifier (e.g., "P3")
        required_cols: List of columns that must exist
        enum_checks: Dict of {col: [valid_values]}
    """
    import logging
    logger = logging.getLogger(__name__)
    
    if df is None or df.empty:
        logger.warning(f"⚠️ Governance: {phase} output is empty")
        return
        
    if required_cols:
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            logger.error(f"❌ Governance Violation ({phase}): Missing required columns: {missing}")
            # In strict mode, we would raise an exception here
            
    if enum_checks:
        for col, valid_values in enum_checks.items():
            if col in df.columns:
                invalid = df[~df[col].isin(valid_values)][col].unique()
                if len(invalid) > 0:
                    logger.warning(f"⚠️ Governance Warning ({phase}): Column '{col}' has invalid values: {invalid}")

# ============================================================
# MARKET STRESS GATE CONTRACT
# ============================================================

MARKET_STRESS_CONTRACT = {
    "RED": {
        "action": "HARD_HALT",
        "status": "HALTED_MARKET_STRESS",
        "sizing": "ZERO"
    },
    "YELLOW": {
        "action": "PROCEED_WITH_CAUTION",
        "status": "READY_NOW",
        "sizing": "NORMAL"
    },
    "GREEN": {
        "action": "PROCEED",
        "status": "READY_NOW",
        "sizing": "NORMAL"
    },
    "UNKNOWN": {
        "action": "PROCEED_INFORMATIONAL",
        "status": "READY_NOW",
        "sizing": "NORMAL",
        "notes": "UNKNOWN is informational by design. Do not gate."
    }
}

# ============================================================
# DATA LAYER INVARIANTS
# ============================================================

DATA_LAYER_INVARIANTS = {
    "IV_Canonical_Monotonicity": {
        "description": "The canonical IV/HV time-series must never decrease in historical depth for any ticker.",
        "enforcement": "Post-write assertion in core/data_layer/ivhv_timeseries_loader.py",
        "failure_mode": "CRITICAL_LOG",
        "notes": "Prevents history stagnation and ensures IV Rank maturity."
    }
}
