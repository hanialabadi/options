"""
Shared types for the evaluator package.
"""

from typing import NamedTuple

# Strategy family constants
DIRECTIONAL_STRATEGIES = [
    'Long Call', 'Long Put', 'Long Call LEAP', 'Long Put LEAP',
    'Bull Call Spread', 'Bear Put Spread', 'Call Debit Spread', 'Put Debit Spread',
]

VOLATILITY_STRATEGIES = [
    'Long Straddle', 'Long Strangle',
    'Straddle', 'Strangle',  # Step 6 emits these names
]

INCOME_STRATEGIES = [
    'Cash-Secured Put', 'Covered Call', 'Buy-Write', 'PMCC',
    'Short Iron Condor', 'Credit Spread',
]

BULLISH_STRATEGIES = [
    'Long Call', 'Long Call LEAP', 'Bull Call Spread', 'Call Debit Spread',
]

BEARISH_STRATEGIES = [
    'Long Put', 'Long Put LEAP', 'Bear Put Spread', 'Put Debit Spread',
]


class EvaluationResult(NamedTuple):
    """Return value from per-strategy evaluators.

    Fields
    ------
    validation_status : str
        Valid | Watch | Reject | Incomplete_Data | Deferred_* | Pending_Greeks
    data_completeness_pct : float
        0-100 percent of required data present.
    missing_required_data : str
        Comma-separated list of missing column names.
    theory_compliance_score : float
        0-100 RAG compliance score.
    evaluation_notes : str
        Pipe-delimited human-readable notes.
    """

    validation_status: str
    data_completeness_pct: float
    missing_required_data: str
    theory_compliance_score: float
    evaluation_notes: str
