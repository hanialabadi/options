"""
Step 7.5: IV Demand Emitter

PURPOSE:
    Identifies tickers that require IV data based on discovered strategies.
    This phase does NOT fetch data; it only emits intent for out-of-band collection.

GOVERNANCE:
    Uses IV_Maturity_Level from IVEngine to determine IV demand:
    - Level 1 (<20d): IV collection priority HIGH
    - Level 2 (20-60d): IV collection priority MEDIUM
    - Level 3+ (60d+): IV collection priority LOW (sufficient history)
"""

import pandas as pd
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

def emit_iv_demand(df_ledger: pd.DataFrame) -> pd.DataFrame:
    """
    Analyzes the Strategy Ledger and emits a list of tickers requiring IV enrichment.

    Returns:
        DataFrame: Demand list with columns [Ticker, IV_Required, Reason]
    """
    if df_ledger.empty:
        return pd.DataFrame(columns=['Ticker', 'Strategy', 'IV_Required', 'Reason'])

    demand_rows = []

    for _, row in df_ledger.iterrows():
        ticker = row['Ticker']
        strategy_name = row.get('Strategy_Name', 'Unknown')
        iv_maturity_level = row.get('IV_Maturity_Level', 1)
        if pd.isna(iv_maturity_level):
            iv_maturity_level = 1
        iv_maturity_level = int(iv_maturity_level)

        # Determine IV collection priority based on maturity level
        if iv_maturity_level <= 1:
            iv_required = True
            reason = f"HIGH priority: IV Maturity Level {iv_maturity_level} (<20 trading days)"
        elif iv_maturity_level == 2:
            iv_required = True
            reason = f"MEDIUM priority: IV Maturity Level {iv_maturity_level} (20-60 trading days)"
        else:
            iv_required = False
            reason = f"LOW priority: IV Maturity Level {iv_maturity_level} (sufficient history)"

        demand_rows.append({
            'Ticker': ticker,
            'Strategy': strategy_name,
            'IV_Required': iv_required,
            'Reason': reason
        })

    df_demand = pd.DataFrame(demand_rows)

    if not df_demand.empty:
        # Deduplicate to ticker level
        ticker_demand = df_demand.groupby('Ticker').agg({
            'IV_Required': 'max',
            'Reason': lambda x: '|'.join(x.unique())
        }).reset_index()

        required_count = ticker_demand['IV_Required'].sum()
        logger.info(f"📡 Phase 7.5: Emitted IV demand for {len(ticker_demand)} tickers ({required_count} require collection)")
        return ticker_demand

    return pd.DataFrame(columns=['Ticker', 'IV_Required', 'Reason'])
