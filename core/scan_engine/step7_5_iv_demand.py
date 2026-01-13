"""
Step 7.5: IV Demand Emitter

PURPOSE:
    Identifies tickers that require IV data based on discovered strategies.
    This phase does NOT fetch data; it only emits intent for out-of-band scraping.

GOVERNANCE:
    - Demand is driven by strategy requirements, not universal scraping.
    - Volatility strategies: REQUIRED (Hard Gate)
    - Directional strategies: OPTIONAL (Confidence Cap)
    - Income strategies: NOT REQUIRED (Immediate Execution)
"""

import pandas as pd
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

def emit_iv_demand(df_ledger: pd.DataFrame) -> pd.DataFrame:
    """
    Analyzes the Strategy Ledger and emits a list of tickers requiring IV enrichment.
    
    Returns:
        DataFrame: Demand list with columns [Ticker, Strategy, IV_Required, Reason]
    """
    if df_ledger.empty:
        return pd.DataFrame(columns=['Ticker', 'Strategy', 'IV_Required', 'Reason'])

    demand_rows = []
    
    # Strategy Family Mapping (aligned with Step 11/12)
    VOLATILITY_STRATEGIES = ['Long Straddle', 'Long Strangle']
    DIRECTIONAL_STRATEGIES = ['Long Call', 'Long Put', 'Long Call LEAP', 'Long Put LEAP', 'Call Debit Spread', 'Put Debit Spread']
    
    for _, row in df_ledger.iterrows():
        ticker = row['Ticker']
        strategy = row.get('Strategy_Name', 'Unknown')
        
        # Check if IV is already present (Step 2 hydration)
        has_iv = pd.notna(row.get('IV_Rank_30D')) or pd.notna(row.get('IV_Rank_XS'))
        
        if not has_iv:
            if strategy in VOLATILITY_STRATEGIES:
                demand_rows.append({
                    'Ticker': ticker,
                    'Strategy': strategy,
                    'IV_Required': True,
                    'Reason': 'VOLATILITY_STRATEGY_HARD_REQUIREMENT'
                })
            elif strategy in DIRECTIONAL_STRATEGIES:
                demand_rows.append({
                    'Ticker': ticker,
                    'Strategy': strategy,
                    'IV_Required': False, # Optional but desired
                    'Reason': 'DIRECTIONAL_STRATEGY_CONFIDENCE_ENHANCEMENT'
                })
                
    df_demand = pd.DataFrame(demand_rows)
    
    if not df_demand.empty:
        # Deduplicate to ticker level for the scraper
        ticker_demand = df_demand.groupby('Ticker').agg({
            'IV_Required': 'max',
            'Reason': lambda x: '|'.join(x.unique())
        }).reset_index()
        
        logger.info(f"📡 Phase 7.5: Emitted IV demand for {len(ticker_demand)} tickers")
        return ticker_demand
        
    return pd.DataFrame(columns=['Ticker', 'IV_Required', 'Reason'])
