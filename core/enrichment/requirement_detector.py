"""
Requirement Detector - Pure Function Data Requirement Detection

This module contains PURE FUNCTIONS that examine data and emit requirements.
No side effects, no IO, no policy decisions - just objective data analysis.

DESIGN PRINCIPLES:
1. PURE FUNCTIONS - Same input always produces same output
2. NO IO - Does not read files, databases, or network
3. NO POLICY - Emits what's missing, not what to do about it
4. STRATEGY-AGNOSTIC - Same logic for all strategy types
5. CONFIGURABLE THRESHOLDS - Thresholds come from config, not hardcoded
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import logging

from .data_requirements import (
    DataRequirement,
    RequirementType,
    RequirementPriority,
    TradeBlockers
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DetectionThresholds:
    """
    Configurable thresholds for requirement detection.

    These are NOT trading opinions - they are data quality thresholds.
    They can be loaded from config files for different environments.
    """
    # IV History thresholds
    iv_history_mature_days: int = 120      # Days for MATURE classification
    iv_history_partial_days: int = 60      # Days for PARTIAL_MATURE
    iv_history_minimum_days: int = 1       # Minimum to be non-MISSING

    # Price History thresholds
    price_history_minimum_days: int = 30   # Minimum for technical indicators
    price_history_ideal_days: int = 90     # Ideal for full analysis

    # Quote Freshness thresholds
    quote_max_age_hours: int = 48          # Maximum acceptable quote age

    # Liquidity thresholds (these are descriptive, not prescriptive)
    liquidity_min_oi: int = 100            # Minimum open interest
    liquidity_max_spread_pct: float = 0.05 # Maximum bid-ask spread %

    # IV Rank thresholds
    iv_rank_required: bool = True          # Whether IV rank is required

    # Greeks thresholds
    greeks_required: bool = True           # Whether greeks are required


# Default thresholds (can be overridden via config)
DEFAULT_THRESHOLDS = DetectionThresholds()


def detect_requirements_for_row(
    row: pd.Series,
    thresholds: DetectionThresholds = DEFAULT_THRESHOLDS
) -> List[DataRequirement]:
    """
    Detect all data requirements for a single trade row.

    This is a PURE FUNCTION - no side effects, no IO.
    It examines the row and emits a list of requirements.

    Args:
        row: A single row from the trades DataFrame
        thresholds: Configurable detection thresholds

    Returns:
        List of DataRequirement objects describing what data is missing/insufficient
    """
    requirements = []
    ticker = row.get('Ticker', row.get('Symbol', 'UNKNOWN'))
    trade_id = row.get('Trade_ID', f"{ticker}_{row.get('Strategy_Name', 'UNKNOWN')}")

    # 1. IV History Requirement
    req = _detect_iv_history_requirement(row, ticker, thresholds)
    if req:
        requirements.append(req)

    # 2. IV Rank Requirement
    req = _detect_iv_rank_requirement(row, ticker, thresholds)
    if req:
        requirements.append(req)

    # 3. Quote Freshness Requirement
    req = _detect_quote_freshness_requirement(row, ticker, thresholds)
    if req:
        requirements.append(req)

    # 4. Liquidity Requirement
    req = _detect_liquidity_requirement(row, ticker, thresholds)
    if req:
        requirements.append(req)

    # 5. Greeks Requirement
    req = _detect_greeks_requirement(row, ticker, thresholds)
    if req:
        requirements.append(req)

    # 6. Price History Requirement
    req = _detect_price_history_requirement(row, ticker, thresholds)
    if req:
        requirements.append(req)

    return requirements


def _detect_iv_history_requirement(
    row: pd.Series,
    ticker: str,
    thresholds: DetectionThresholds
) -> Optional[DataRequirement]:
    """Detect IV history requirement based on maturity state."""
    iv_maturity = row.get('IV_Maturity_State', 'MISSING')
    history_count = row.get('iv_history_count', 0)

    # If MATURE, no requirement
    if iv_maturity == 'MATURE':
        return None

    # If history_count is already at threshold, no requirement
    if pd.notna(history_count) and history_count >= thresholds.iv_history_mature_days:
        return None

    # Determine priority based on current state
    if iv_maturity == 'MISSING' or history_count == 0:
        priority = RequirementPriority.P1_BLOCKING
    elif iv_maturity == 'IMMATURE':
        priority = RequirementPriority.P2_IMPORTANT
    else:  # PARTIAL_MATURE
        priority = RequirementPriority.P3_ENHANCING

    return DataRequirement(
        requirement_type=RequirementType.IV_HISTORY,
        entity_id=ticker,
        field_name='iv_history_count',
        current_value=history_count if pd.notna(history_count) else 0,
        required_threshold=thresholds.iv_history_mature_days,
        priority=priority,
        metadata={
            'iv_maturity_state': iv_maturity,
            'days_needed': thresholds.iv_history_mature_days - (history_count or 0)
        }
    )


def _detect_iv_rank_requirement(
    row: pd.Series,
    ticker: str,
    thresholds: DetectionThresholds
) -> Optional[DataRequirement]:
    """Detect IV Rank requirement."""
    if not thresholds.iv_rank_required:
        return None

    iv_rank = row.get('IV_Rank_30D')
    iv_rank_source = row.get('IV_Rank_Source', '')

    # If we have a valid IV rank, no requirement
    if pd.notna(iv_rank) and 0 <= iv_rank <= 100:
        return None

    # Check if it was skipped due to missing data vs other reasons
    is_data_missing = (
        pd.isna(iv_rank) or
        'No Fidelity' in str(iv_rank_source) or
        'SKIPPED' in str(iv_rank_source)
    )

    if not is_data_missing:
        return None

    return DataRequirement(
        requirement_type=RequirementType.IV_RANK,
        entity_id=ticker,
        field_name='IV_Rank_30D',
        current_value=iv_rank if pd.notna(iv_rank) else None,
        required_threshold=True,  # Just need a valid value
        priority=RequirementPriority.P2_IMPORTANT,
        metadata={
            'iv_rank_source': str(iv_rank_source),
            'reason': 'IV Rank not available from any source'
        }
    )


def _detect_quote_freshness_requirement(
    row: pd.Series,
    ticker: str,
    thresholds: DetectionThresholds
) -> Optional[DataRequirement]:
    """Detect quote freshness requirement."""
    bid = row.get('Bid', row.get('bid'))
    ask = row.get('Ask', row.get('ask'))

    # Check for missing bid/ask
    has_bid = pd.notna(bid) and bid > 0
    has_ask = pd.notna(ask) and ask > 0

    if has_bid and has_ask:
        return None

    return DataRequirement(
        requirement_type=RequirementType.QUOTE_FRESHNESS,
        entity_id=ticker,
        field_name='bid_ask',
        current_value={'bid': bid if pd.notna(bid) else None, 'ask': ask if pd.notna(ask) else None},
        required_threshold={'bid': True, 'ask': True},
        priority=RequirementPriority.P1_BLOCKING,
        metadata={
            'has_bid': has_bid,
            'has_ask': has_ask,
            'reason': 'Missing bid/ask quote data'
        }
    )


def _detect_liquidity_requirement(
    row: pd.Series,
    ticker: str,
    thresholds: DetectionThresholds
) -> Optional[DataRequirement]:
    """
    Detect liquidity requirement.

    NOTE: Liquidity is often MARKET-DEPENDENT and cannot be actively resolved.
    This requirement is informational - the resolver will be "wait".
    """
    oi = row.get('Open_Interest', row.get('OI', row.get('openInterest')))
    bid = row.get('Bid', row.get('bid', 0)) or 0
    ask = row.get('Ask', row.get('ask', 0)) or 0

    # Calculate spread %
    spread_pct = None
    if bid > 0 and ask > 0:
        mid = (bid + ask) / 2
        spread_pct = (ask - bid) / mid if mid > 0 else None

    # Check OI threshold
    oi_ok = pd.notna(oi) and oi >= thresholds.liquidity_min_oi

    # Check spread threshold
    spread_ok = spread_pct is not None and spread_pct <= thresholds.liquidity_max_spread_pct

    if oi_ok and spread_ok:
        return None

    return DataRequirement(
        requirement_type=RequirementType.LIQUIDITY_METRICS,
        entity_id=ticker,
        field_name='liquidity',
        current_value={'oi': oi, 'spread_pct': spread_pct},
        required_threshold={'oi': thresholds.liquidity_min_oi, 'spread_pct': thresholds.liquidity_max_spread_pct},
        priority=RequirementPriority.P2_IMPORTANT,
        metadata={
            'oi_ok': oi_ok,
            'spread_ok': spread_ok,
            'is_market_dependent': True,  # Cannot be actively resolved
            'reason': f"Thin liquidity: OI={oi}, Spread={spread_pct:.2%}" if spread_pct else f"Thin liquidity: OI={oi}"
        }
    )


def _detect_greeks_requirement(
    row: pd.Series,
    ticker: str,
    thresholds: DetectionThresholds
) -> Optional[DataRequirement]:
    """Detect Greeks requirement."""
    if not thresholds.greeks_required:
        return None

    delta = row.get('Delta', row.get('delta'))
    gamma = row.get('Gamma', row.get('gamma'))
    theta = row.get('Theta', row.get('theta'))
    vega = row.get('Vega', row.get('vega'))

    missing_greeks = []
    if pd.isna(delta):
        missing_greeks.append('delta')
    if pd.isna(gamma):
        missing_greeks.append('gamma')
    if pd.isna(theta):
        missing_greeks.append('theta')
    if pd.isna(vega):
        missing_greeks.append('vega')

    if not missing_greeks:
        return None

    return DataRequirement(
        requirement_type=RequirementType.GREEKS,
        entity_id=ticker,
        field_name='greeks',
        current_value={'delta': delta, 'gamma': gamma, 'theta': theta, 'vega': vega},
        required_threshold={'delta': True, 'gamma': True, 'theta': True, 'vega': True},
        priority=RequirementPriority.P2_IMPORTANT,
        metadata={
            'missing_greeks': missing_greeks,
            'reason': f"Missing Greeks: {', '.join(missing_greeks)}"
        }
    )


def _detect_price_history_requirement(
    row: pd.Series,
    ticker: str,
    thresholds: DetectionThresholds
) -> Optional[DataRequirement]:
    """Detect price history requirement for technical indicators."""
    # Check if technical indicators were computed
    rsi = row.get('RSI')
    adx = row.get('ADX')
    trend_state = row.get('Trend_State', 'Unknown')

    # If we have technical indicators, price history was sufficient
    if pd.notna(rsi) and pd.notna(adx) and trend_state != 'Unknown':
        return None

    return DataRequirement(
        requirement_type=RequirementType.PRICE_HISTORY,
        entity_id=ticker,
        field_name='price_history',
        current_value={'rsi': rsi, 'adx': adx, 'trend_state': trend_state},
        required_threshold=thresholds.price_history_minimum_days,
        priority=RequirementPriority.P3_ENHANCING,
        metadata={
            'has_rsi': pd.notna(rsi),
            'has_adx': pd.notna(adx),
            'has_trend': trend_state != 'Unknown',
            'reason': 'Insufficient price history for technical analysis'
        }
    )


def detect_all_requirements(
    df: pd.DataFrame,
    thresholds: DetectionThresholds = DEFAULT_THRESHOLDS,
    id_col: str = 'Ticker'
) -> Dict[str, TradeBlockers]:
    """
    Detect requirements for all trades in a DataFrame.

    This is a PURE FUNCTION - examines data and emits requirements.
    No side effects, no database writes, no enrichment triggered.

    Args:
        df: DataFrame with trade data
        thresholds: Configurable detection thresholds
        id_col: Column name for ticker identifier

    Returns:
        Dictionary mapping trade_id to TradeBlockers
    """
    all_blockers = {}

    for idx, row in df.iterrows():
        ticker = row.get(id_col, row.get('Symbol', f'UNKNOWN_{idx}'))
        strategy = row.get('Strategy_Name', 'UNKNOWN')
        trade_id = row.get('Trade_ID', f"{ticker}_{strategy}")

        requirements = detect_requirements_for_row(row, thresholds)

        blockers = TradeBlockers(
            trade_id=trade_id,
            ticker=ticker,
            strategy_name=strategy,
            requirements=requirements
        )

        all_blockers[trade_id] = blockers

    # Log summary
    total_trades = len(all_blockers)
    ready_trades = sum(1 for b in all_blockers.values() if b.is_ready)
    blocked_trades = total_trades - ready_trades

    logger.info(f"Requirement Detection Summary:")
    logger.info(f"  Total trades: {total_trades}")
    logger.info(f"  Ready: {ready_trades}")
    logger.info(f"  Blocked: {blocked_trades}")

    # Aggregate by requirement type
    req_counts = {}
    for blockers in all_blockers.values():
        for req in blockers.unsatisfied_requirements:
            t = req.requirement_type.name
            req_counts[t] = req_counts.get(t, 0) + 1

    if req_counts:
        logger.info(f"  Unsatisfied requirements by type:")
        for req_type, count in sorted(req_counts.items(), key=lambda x: -x[1]):
            logger.info(f"    {req_type}: {count}")

    return all_blockers


def get_enrichment_candidates(
    blockers: Dict[str, TradeBlockers]
) -> Dict[RequirementType, List[str]]:
    """
    Get tickers that need enrichment, grouped by requirement type.

    This is a PURE FUNCTION that aggregates requirements.
    It does NOT decide which resolver to use - that's the dispatcher's job.

    Args:
        blockers: Dictionary of trade blockers from detect_all_requirements

    Returns:
        Dictionary mapping RequirementType to list of tickers needing that data
    """
    candidates = {}

    for trade_id, trade_blockers in blockers.items():
        for req in trade_blockers.actionable_requirements:
            req_type = req.requirement_type
            if req_type not in candidates:
                candidates[req_type] = set()
            candidates[req_type].add(req.entity_id)

    # Convert sets to sorted lists
    return {k: sorted(list(v)) for k, v in candidates.items()}
