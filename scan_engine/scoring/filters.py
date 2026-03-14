"""
Filtering utilities + regime strategy matrix extracted from step12_acceptance.py.

Pure functions: no DB writes, no file I/O.
"""

import logging

import pandas as pd


logger = logging.getLogger(__name__)


# Natenberg Ch.19 / McMillan Ch.1 / Passarelli Ch.2 matrix.
# Maps (Regime, Stress_Level) -> fit/caution/mismatch per Capital_Bucket.
REGIME_STRATEGY_MATRIX = {
    ('High Vol',    'CRISIS'):    {'fit': ['DEFENSIVE'],                          'caution': ['STRATEGIC'], 'mismatch': ['TACTICAL']},
    ('High Vol',    'ELEVATED'):  {'fit': ['DEFENSIVE', 'STRATEGIC'],             'caution': ['TACTICAL'],  'mismatch': []},
    ('High Vol',    'NORMAL'):    {'fit': ['DEFENSIVE', 'STRATEGIC'],             'caution': ['TACTICAL'],  'mismatch': []},
    ('High Vol',    'LOW'):       {'fit': ['DEFENSIVE', 'STRATEGIC', 'TACTICAL'], 'caution': [],            'mismatch': []},
    ('Compression', 'CRISIS'):    {'fit': ['DEFENSIVE'],                          'caution': ['STRATEGIC'], 'mismatch': ['TACTICAL']},
    ('Compression', 'ELEVATED'):  {'fit': ['DEFENSIVE'],                          'caution': ['STRATEGIC'], 'mismatch': ['TACTICAL']},
    ('Compression', 'NORMAL'):    {'fit': ['DEFENSIVE', 'STRATEGIC'],             'caution': ['TACTICAL'],  'mismatch': []},
    ('Compression', 'LOW'):       {'fit': ['DEFENSIVE', 'STRATEGIC', 'TACTICAL'], 'caution': [],            'mismatch': []},
    ('Low Vol',     'CRISIS'):    {'fit': ['DEFENSIVE'],                          'caution': ['STRATEGIC'], 'mismatch': ['TACTICAL']},
    ('Low Vol',     'ELEVATED'):  {'fit': ['DEFENSIVE'],                          'caution': ['STRATEGIC'], 'mismatch': ['TACTICAL']},
    ('Low Vol',     'NORMAL'):    {'fit': ['STRATEGIC', 'TACTICAL'],              'caution': ['DEFENSIVE'], 'mismatch': []},
    ('Low Vol',     'LOW'):       {'fit': ['STRATEGIC', 'TACTICAL'],              'caution': ['DEFENSIVE'], 'mismatch': []},
    ('Unknown',     'CRISIS'):    {'fit': ['DEFENSIVE'],                          'caution': [],            'mismatch': ['TACTICAL']},
    ('Unknown',     'ELEVATED'):  {'fit': ['DEFENSIVE', 'STRATEGIC'],             'caution': ['TACTICAL'],  'mismatch': []},
    ('Unknown',     'NORMAL'):    {'fit': ['DEFENSIVE', 'STRATEGIC', 'TACTICAL'], 'caution': [],            'mismatch': []},
    ('Unknown',     'LOW'):       {'fit': ['DEFENSIVE', 'STRATEGIC', 'TACTICAL'], 'caution': [],            'mismatch': []},
    # VVIX-driven regime overrides
    ('Expansion',   'CRISIS'):    {'fit': ['DEFENSIVE'],                          'caution': ['STRATEGIC'], 'mismatch': ['TACTICAL']},
    ('Expansion',   'ELEVATED'):  {'fit': ['DEFENSIVE', 'STRATEGIC'],             'caution': ['TACTICAL'],  'mismatch': []},
    ('Expansion',   'NORMAL'):    {'fit': ['DEFENSIVE', 'STRATEGIC'],             'caution': ['TACTICAL'],  'mismatch': []},
    ('Expansion',   'LOW'):       {'fit': ['DEFENSIVE', 'STRATEGIC', 'TACTICAL'], 'caution': [],            'mismatch': []},
    ('Uncertain',   'CRISIS'):    {'fit': ['DEFENSIVE'],                          'caution': [],            'mismatch': ['TACTICAL']},
    ('Uncertain',   'ELEVATED'):  {'fit': ['DEFENSIVE', 'STRATEGIC'],             'caution': ['TACTICAL'],  'mismatch': []},
    ('Uncertain',   'NORMAL'):    {'fit': ['DEFENSIVE', 'STRATEGIC', 'TACTICAL'], 'caution': [],            'mismatch': []},
    ('Uncertain',   'LOW'):       {'fit': ['DEFENSIVE', 'STRATEGIC', 'TACTICAL'], 'caution': [],            'mismatch': []},
}


def lookup_regime_fit(row: pd.Series, matrix: dict = None) -> str:
    """
    Look up regime-strategy fit for a single row.

    Returns: 'FIT' | 'CAUTION' | 'MISMATCH' | 'UNKNOWN'
    """
    if matrix is None:
        matrix = REGIME_STRATEGY_MATRIX
    regime  = str(row.get('Regime') or 'Unknown')
    stress  = str(row.get('market_stress') or row.get('Market_Stress') or 'NORMAL').upper()
    bucket  = str(row.get('Capital_Bucket') or '').upper()
    if not bucket:
        return 'UNKNOWN'
    entry = matrix.get((regime, stress))
    if entry is None:
        for known_stress in ('CRISIS', 'ELEVATED', 'NORMAL', 'LOW'):
            if known_stress in stress:
                entry = matrix.get((regime, known_stress))
                break
    if entry is None:
        return 'UNKNOWN'
    if bucket in entry.get('fit', []):
        return 'FIT'
    if bucket in entry.get('caution', []):
        return 'CAUTION'
    if bucket in entry.get('mismatch', []):
        return 'MISMATCH'
    return 'UNKNOWN'


def filter_ready_contracts(df: pd.DataFrame, min_confidence: str = 'LOW') -> pd.DataFrame:
    """
    Filter for READY contracts at or above min_confidence.

    Returns:
        Filtered DataFrame (copy).
    """
    confidence_hierarchy = {'LOW': 1, 'MEDIUM': 2, 'HIGH': 3}
    min_level = confidence_hierarchy.get(min_confidence, 1)

    df_ready = df[df['Execution_Status'] == 'READY'].copy()

    if not df_ready.empty:
        df_ready['_confidence_level'] = df_ready['confidence_band'].map(confidence_hierarchy)
        df_ready = df_ready[df_ready['_confidence_level'] >= min_level]
        df_ready.drop(columns=['_confidence_level'], inplace=True)

    logger.info(f"Filtered for READY with {min_confidence}+ confidence: {len(df_ready)} contracts")

    return df_ready


def sort_by_confidence(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sort contracts by confidence band (HIGH -> MEDIUM -> LOW).

    Returns:
        Sorted DataFrame (copy).
    """
    confidence_order = {'HIGH': 1, 'MEDIUM': 2, 'LOW': 3, 'UNKNOWN': 4}
    df_sorted = df.copy()
    df_sorted['_confidence_sort'] = df_sorted['confidence_band'].map(confidence_order)
    df_sorted = df_sorted.sort_values('_confidence_sort')
    df_sorted.drop(columns=['_confidence_sort'], inplace=True)

    return df_sorted
