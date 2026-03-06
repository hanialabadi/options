"""
Exit Coordinator — Sequences multiple simultaneous exits to minimize market impact.

When doctrine produces >3 simultaneous EXIT actions, naive execution can:
1. Create cascading fills at progressively worse prices
2. Trigger liquidity-driven slippage in correlated names
3. Miss optimal sequencing (lock gains first, then cut losses)

This module orders exits by priority and assigns an execution sequence.

Sequencing Rules (priority order):
    1. CRITICAL urgency first (structural failures, breaker overrides)
    2. Most liquid positions next (highest Open_Int, tightest bid-ask)
    3. Winners before losers (lock gains → preserve capital)
    4. Correlated names spaced apart (avoid sector cascade)

References:
    - McMillan Ch.3: Portfolio exit management
    - Passarelli Ch.6: Execution timing and sequencing
    - Harris (Trading and Exchanges): Market impact minimization
"""

import pandas as pd
import numpy as np
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Coordination activates when exit_count > MIN_EXITS_TO_COORDINATE (i.e., 4+ exits).
# At exactly 3 or fewer exits, each gets sequence=1 (uncoordinated parallel execution).
MIN_EXITS_TO_COORDINATE = 3


def sequence_exits(
    df: pd.DataFrame,
    account_balance: float = 100_000.0,
) -> pd.DataFrame:
    """
    Assign execution sequence to simultaneous exits.

    Only activates when >MIN_EXITS_TO_COORDINATE exits are pending.
    Returns df with added columns:
        - Exit_Sequence: integer priority (1 = execute first)
        - Exit_Priority_Reason: why this position has its sequence rank

    Args:
        df: DataFrame after doctrine (has Action, Urgency columns)
        account_balance: Account equity for proportional sizing

    Returns:
        df with Exit_Sequence and Exit_Priority_Reason columns added
    """
    # Initialize columns
    df['Exit_Sequence'] = np.nan
    df['Exit_Priority_Reason'] = ''

    if df.empty or 'Action' not in df.columns:
        return df

    # Identify all EXIT rows
    exit_mask = df['Action'] == 'EXIT'
    exit_count = int(exit_mask.sum())

    if exit_count <= MIN_EXITS_TO_COORDINATE:
        # Not enough exits to warrant coordination — mark all as sequence 1
        if exit_count > 0:
            df.loc[exit_mask, 'Exit_Sequence'] = 1
            df.loc[exit_mask, 'Exit_Priority_Reason'] = 'Below coordination threshold'
        return df

    logger.info(
        f"[ExitCoordinator] {exit_count} simultaneous exits detected — "
        f"coordinating sequence (threshold: >{MIN_EXITS_TO_COORDINATE})"
    )

    # Extract exit rows for scoring
    exits = df.loc[exit_mask].copy()

    # --- Score each exit for priority ---
    # Higher score = execute first
    exits['_priority_score'] = 0.0
    exits['_priority_reasons'] = ''

    # Factor 1: Urgency (CRITICAL=100, HIGH=60, MEDIUM=30, LOW=10)
    urgency_map = {'CRITICAL': 100, 'HIGH': 60, 'MEDIUM': 30, 'LOW': 10}
    if 'Urgency' in exits.columns:
        exits['_urgency_score'] = exits['Urgency'].map(urgency_map).fillna(10)
        exits['_priority_score'] += exits['_urgency_score']
        exits.loc[exits['_urgency_score'] >= 100, '_priority_reasons'] += 'CRITICAL urgency; '

    # Factor 2: Liquidity (Open_Int normalized, 0-30 points)
    if 'Open_Int' in exits.columns:
        oi = exits['Open_Int'].fillna(0).astype(float)
        oi_max = max(oi.max(), 1.0)
        exits['_liquidity_score'] = (oi / oi_max) * 30.0
        exits['_priority_score'] += exits['_liquidity_score']
    else:
        exits['_liquidity_score'] = 15.0  # Default mid-range
        exits['_priority_score'] += 15.0

    # Factor 3: Winners first — positive P&L gets priority (lock gains)
    # Normalized to 0-40 points
    gl_col = '$ Total G/L' if '$ Total G/L' in exits.columns else 'Total_GL_Decimal'
    if gl_col in exits.columns:
        gl = exits[gl_col].fillna(0).astype(float)
        # Winners (positive GL) get higher scores
        gl_max = max(abs(gl).max(), 1.0)
        # +40 for biggest winner, -20 for biggest loser
        exits['_gl_score'] = (gl / gl_max) * 40.0
        exits['_priority_score'] += exits['_gl_score']
        exits.loc[gl > 0, '_priority_reasons'] += 'Lock gains; '
        exits.loc[gl < 0, '_priority_reasons'] += 'Cut loss; '
    else:
        exits['_gl_score'] = 0.0

    # Factor 4: DTE urgency — lower DTE = more urgent (0-20 points)
    if 'DTE' in exits.columns:
        dte = exits['DTE'].fillna(30).astype(float).clip(lower=0)
        # DTE 0 = 20 points, DTE 60+ = 0 points
        exits['_dte_score'] = ((60.0 - dte.clip(upper=60)) / 60.0) * 20.0
        exits['_priority_score'] += exits['_dte_score']

    # Factor 5: Circuit breaker override gets maximum priority
    if '_circuit_breaker_override' in exits.columns:
        cb_mask = exits['_circuit_breaker_override'] == True
        exits.loc[cb_mask, '_priority_score'] += 200.0
        exits.loc[cb_mask, '_priority_reasons'] += 'Circuit breaker override; '

    # --- Sector spacing penalty ---
    # If multiple exits share the same sector/ticker group, space them apart.
    # We don't lower priority but add a note about staggered execution.
    if 'Underlying_Ticker' in exits.columns:
        ticker_exit_counts = exits['Underlying_Ticker'].value_counts()
        multi_ticker = ticker_exit_counts[ticker_exit_counts > 1].index
        if len(multi_ticker) > 0:
            for idx in exits.index:
                tkr = exits.at[idx, 'Underlying_Ticker']
                if tkr in multi_ticker:
                    exits.at[idx, '_priority_reasons'] += f'Correlated: {ticker_exit_counts[tkr]} {tkr} exits; '

    # --- Assign sequence ---
    # Sort by priority score descending, then by ticker for determinism
    sort_cols = ['_priority_score']
    if 'Underlying_Ticker' in exits.columns:
        sort_cols.append('Underlying_Ticker')

    exits = exits.sort_values(sort_cols, ascending=[False, True] if len(sort_cols) == 2 else [False])
    exits['Exit_Sequence'] = range(1, len(exits) + 1)
    exits['Exit_Priority_Reason'] = exits['_priority_reasons'].str.rstrip('; ')

    # Write back to original df
    for idx in exits.index:
        df.at[idx, 'Exit_Sequence'] = exits.at[idx, 'Exit_Sequence']
        df.at[idx, 'Exit_Priority_Reason'] = exits.at[idx, 'Exit_Priority_Reason']

    # Log summary
    top3 = exits.head(3)
    for _, row in top3.iterrows():
        tkr = row.get('Underlying_Ticker', '?')
        seq = int(row['Exit_Sequence'])
        reason = row['Exit_Priority_Reason']
        logger.info(f"[ExitCoordinator] #{seq}: {tkr} — {reason}")

    logger.info(
        f"[ExitCoordinator] Sequenced {len(exits)} exits "
        f"(first 3 execute immediately, remainder stagger by priority)"
    )

    return df
