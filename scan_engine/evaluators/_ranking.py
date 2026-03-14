"""
Within-family ranking (NOT cross-family).

Each strategy family gets independent ranking by Theory_Compliance_Score.
"""

from __future__ import annotations

import logging

import pandas as pd

from ._types import (
    DIRECTIONAL_STRATEGIES, VOLATILITY_STRATEGIES, INCOME_STRATEGIES,
)
from ._shared import resolve_strategy_name

logger = logging.getLogger(__name__)


def rank_within_families(df: pd.DataFrame) -> pd.DataFrame:
    """Add ``Strategy_Family`` and ``Strategy_Family_Rank`` columns."""

    df_ranked = df.copy()
    df_ranked['Strategy_Family'] = ''
    df_ranked['Strategy_Family_Rank'] = 0

    for idx, row in df_ranked.iterrows():
        strategy = resolve_strategy_name(row)
        if strategy in DIRECTIONAL_STRATEGIES:
            df_ranked.at[idx, 'Strategy_Family'] = 'Directional'
        elif strategy in VOLATILITY_STRATEGIES:
            df_ranked.at[idx, 'Strategy_Family'] = 'Volatility'
        elif strategy in INCOME_STRATEGIES:
            df_ranked.at[idx, 'Strategy_Family'] = 'Income'
        else:
            df_ranked.at[idx, 'Strategy_Family'] = 'Other'

    for family in ('Directional', 'Volatility', 'Income', 'Other'):
        mask = df_ranked['Strategy_Family'] == family
        if mask.any():
            df_ranked.loc[mask, 'Strategy_Family_Rank'] = (
                df_ranked.loc[mask, 'Theory_Compliance_Score']
                .rank(method='dense', ascending=False)
                .astype(int)
            )

    for family in ('Directional', 'Volatility', 'Income'):
        family_df = df_ranked[df_ranked['Strategy_Family'] == family]
        if not family_df.empty:
            rank1 = len(family_df[family_df['Strategy_Family_Rank'] == 1])
            logger.info(f"   📊 {family}: {len(family_df)} strategies, {rank1} top-ranked")

    return df_ranked
