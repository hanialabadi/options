"""
Audit and summary logging for the evaluator package.
"""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def audit_independent_evaluation(df: pd.DataFrame) -> None:
    """Log audit of evaluation results."""

    total = len(df)
    logger.info("    📊 Independent Evaluation Audit:")

    status_counts = df['Validation_Status'].value_counts().to_dict()
    for status in (
        'Valid', 'Watch', 'Deferred_DTE', 'Deferred_Liquidity', 'Pending_Greeks',
        'Blocked_No_IV', 'Blocked_No_Contracts', 'Reject', 'Incomplete_Data',
    ):
        count = status_counts.get(status, 0)
        if count > 0:
            pct = count / total * 100 if total else 0
            logger.info(f"       {status}: {count} ({pct:.1f}%)")

    if 'Contract_Status' in df.columns:
        leap_fb = (df['Contract_Status'] == 'LEAP_FALLBACK').sum()
        if leap_fb:
            logger.info(f"       📌 LEAP_FALLBACK used: {leap_fb} strategies")

    if 'Evaluation_Notes' in df.columns:
        blocked = df[df['Validation_Status'].isin(
            ['Reject', 'Blocked_No_IV', 'Blocked_No_Contracts',
             'Deferred_DTE', 'Deferred_Liquidity']
        )]
        if len(blocked):
            reasons = blocked['Evaluation_Notes'].str.split(' | ').str[0].value_counts().head(5)
            if len(reasons):
                logger.info("    📋 Top rejection/deferral reasons:")
                for reason, count in reasons.items():
                    short = reason[:80] + '...' if len(reason) > 80 else reason
                    logger.info(f"       • {short}: {count}")

    if 'Strategy_Family' in df.columns:
        logger.info("    📊 By Strategy Family:")
        for family, count in df['Strategy_Family'].value_counts().items():
            valid = len(df[(df['Strategy_Family'] == family) & (df['Validation_Status'] == 'Valid')])
            logger.info(f"       {family}: {count} total, {valid} valid")

    avg_comp = df['Data_Completeness_Pct'].mean()
    logger.info(f"    📊 Avg Data Completeness: {avg_comp:.1f}%")

    if 'IV_30_D_Call' in df.columns:
        has_iv = df['IV_30_D_Call'].notna().sum()
        miss_iv = df['IV_30_D_Call'].isna().sum()
        logger.info(f"    📊 IV Status: {has_iv} have IV, {miss_iv} missing ({miss_iv/total*100:.1f}%)" if total else "")

    valid_watch = df[df['Validation_Status'].isin(['Valid', 'Watch'])]
    if not valid_watch.empty:
        avg_tc = valid_watch['Theory_Compliance_Score'].mean()
        logger.info(f"    📊 Avg Theory Compliance: {avg_tc:.1f} (valid/watch only)")


def log_evaluation_summary(df: pd.DataFrame, user_goal: str) -> None:
    """Log concise evaluation summary."""

    valid = df[df['Validation_Status'] == 'Valid']
    logger.info(f"   📊 Evaluation Summary:")
    logger.info(f"      Valid Strategies: {len(valid)}")

    if not valid.empty:
        for family in ('Directional', 'Volatility', 'Income'):
            fv = valid[valid['Strategy_Family'] == family]
            if not fv.empty:
                top = fv.nsmallest(1, 'Strategy_Family_Rank').iloc[0]
                name = top.get('Strategy') or top.get('Primary_Strategy', '')
                ticker = top.get('Ticker', '')
                tc = top['Theory_Compliance_Score']
                logger.info(f"      Best {family}: {ticker} {name} (compliance: {tc:.0f})")

    incomplete = df[df['Validation_Status'] == 'Incomplete_Data']
    if not incomplete.empty:
        logger.info(f"      ⚠️ Incomplete Data: {len(incomplete)} strategies")
        for data, count in incomplete['Missing_Required_Data'].value_counts().head(3).items():
            logger.info(f"         {data}: {count} occurrences")

    logger.info(f"   💡 User Goal: {user_goal} (guides portfolio allocation, not scoring)")
    logger.info(f"      All valid strategies available regardless of goal")
