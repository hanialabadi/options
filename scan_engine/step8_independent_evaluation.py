"""
Step 8 / 11: Strategy Independent Evaluation — ORCHESTRATOR.

Thin wrapper that routes each row to the correct per-family evaluator in
``scan_engine.evaluators``.  All thresholds and RAG citations live in the
doctrine layer (``evaluators.doctrine``), not here.

Public API (unchanged):
    evaluate_strategies_independently(df, user_goal, account_size, risk_tolerance) -> DataFrame
    compare_and_rank_strategies(...)  # deprecated alias

Output columns (unchanged):
    Validation_Status, Theory_Compliance_Score, Evaluation_Notes,
    Data_Completeness_Pct, Missing_Required_Data, Strategy_Family,
    Strategy_Family_Rank, acceptance_status
"""

import logging

import pandas as pd

from .evaluators import (
    DIRECTIONAL_STRATEGIES,
    VOLATILITY_STRATEGIES,
    INCOME_STRATEGIES,
    evaluate_directional,
    evaluate_volatility,
    evaluate_income,
    contract_status_precheck,
    resolve_strategy_name,
)
from .evaluators._ranking import rank_within_families
from .evaluators._audit import audit_independent_evaluation, log_evaluation_summary

logger = logging.getLogger(__name__)


def evaluate_strategies_independently(
    df: pd.DataFrame,
    user_goal: str = 'income',
    account_size: float = 10000.0,
    risk_tolerance: str = 'moderate',
) -> pd.DataFrame:
    """Evaluate each strategy independently against its own requirements.

    Signature and output columns are identical to the previous monolith so that
    ``pipeline.py`` and downstream steps (Step 12, dashboard) require no changes.
    """

    # Ensure 'Strategy' column exists (Step 7 uses 'Strategy_Name')
    if 'Strategy' not in df.columns and 'Strategy_Name' in df.columns:
        df['Strategy'] = df['Strategy_Name']
        logger.info("ℹ️ Aliased Strategy_Name -> Strategy for Step 8 compatibility")

    if df.empty:
        logger.warning("⚠️ Empty DataFrame passed to Step 8")
        return df

    # Runtime assertion: critical upstream columns
    for col in ('Signal_Type', 'Regime'):
        if col not in df.columns:
            raise ValueError(f"❌ Step 8 Input Error: Missing required column '{col}' from upstream pipeline.")
        if df[col].isnull().any():
            raise ValueError(f"❌ Step 8 Input Error: Column '{col}' contains null values.")
    logger.info(f"✅ Step 8 Input Assertion Passed: Required columns exist and are non-null.")

    input_row_count = len(df)
    logger.info(f"🎯 Step 8: Evaluating {input_row_count} strategies independently")
    logger.info(f"   Mode: STRATEGY ISOLATION (no cross-strategy competition)")
    logger.info(f"   User Goal: {user_goal} (for portfolio layer, not scoring)")

    # Initialize evaluation columns
    df_eval = df.copy()
    df_eval['Validation_Status'] = 'Pending'
    df_eval['Data_Completeness_Pct'] = 0.0
    df_eval['Missing_Required_Data'] = ''
    df_eval['Theory_Compliance_Score'] = 0.0
    df_eval['Evaluation_Notes'] = ''

    iv_maturity = df_eval.get('IV_Maturity_State', pd.Series(['MATURE'] * len(df_eval)))

    for idx, row in df_eval.iterrows():
        result = _evaluate_row(row)
        status, completeness, missing, compliance, notes = result

        # Immature IV: demote instead of reject
        maturity = iv_maturity.get(idx, 'MATURE')
        if maturity == 'IMMATURE' and status in ('Reject', 'Incomplete_Data'):
            status = 'DATA_NOT_MATURE'
            notes = f"Diagnostic: IV context still forming ({maturity}). " + notes

        df_eval.at[idx, 'Validation_Status'] = status
        df_eval.at[idx, 'Data_Completeness_Pct'] = completeness
        df_eval.at[idx, 'Missing_Required_Data'] = missing
        df_eval.at[idx, 'Theory_Compliance_Score'] = compliance
        df_eval.at[idx, 'Evaluation_Notes'] = notes

    # Rank within families
    df_eval = rank_within_families(df_eval)

    # Backward compat: acceptance_status for Dashboard
    if 'acceptance_status' not in df_eval.columns:
        df_eval['acceptance_status'] = df_eval.get('Validation_Status', 'UNKNOWN')

    # Audit
    audit_independent_evaluation(df_eval)

    # Row preservation assertion
    assert len(df_eval) == input_row_count, (
        f"❌ Row count mismatch: {len(df_eval)} != {input_row_count}. Step 8 must preserve all strategies."
    )

    logger.info(f"✅ Step 8 Complete: {len(df_eval)} strategies independently evaluated")
    log_evaluation_summary(df_eval, user_goal)
    return df_eval


def _evaluate_row(row: pd.Series):
    """Route a single row to the correct evaluator."""

    # Contract status pre-check (short-circuit for rejected/deferred)
    precheck = contract_status_precheck(row)
    if precheck is not None:
        return precheck

    strategy = resolve_strategy_name(row)

    if strategy in DIRECTIONAL_STRATEGIES:
        return evaluate_directional(row)
    if strategy in VOLATILITY_STRATEGIES:
        return evaluate_volatility(row)
    if strategy in INCOME_STRATEGIES:
        return evaluate_income(row)

    # Unknown strategy family
    return ('Watch', 50.0, 'Strategy family not classified', 50.0,
            f"Strategy '{strategy}' not in known families")


# ── Backward compatibility ────────────────────────────────────

def compare_and_rank_strategies(
    df: pd.DataFrame,
    user_goal: str = 'income',
    account_size: float = 10000.0,
    risk_tolerance: str = 'moderate',
) -> pd.DataFrame:
    """DEPRECATED: Legacy alias. Redirects to evaluate_strategies_independently."""
    logger.warning("⚠️ compare_and_rank_strategies() is DEPRECATED — use evaluate_strategies_independently()")
    return evaluate_strategies_independently(df, user_goal=user_goal, account_size=account_size, risk_tolerance=risk_tolerance)
