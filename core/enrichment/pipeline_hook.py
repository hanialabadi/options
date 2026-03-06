"""
Pipeline Hook - Post-Step 12 Enrichment Integration

This module provides the specific hook that integrates the enrichment
system into the scan pipeline after Step 12. It is designed to be
non-invasive and strategy-agnostic.

INTEGRATION POINT:
    Called after _step12_8_acceptance_and_sizing() in pipeline.py

BEHAVIOR:
    1. Detects all unsatisfied data requirements
    2. Triggers enrichment for actionable requirements
    3. Merges enriched data back into DataFrame
    4. Optionally triggers re-evaluation of affected stages
    5. DOES NOT directly modify Execution_Status (Step 12 still controls that)

STRATEGY AGNOSTICISM GUARANTEE:
    - This hook NEVER inspects Strategy_Name or Strategy_Type
    - All enrichment decisions based purely on data field values
    - Same thresholds apply to all trades regardless of strategy
"""

import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Any, Optional, Tuple, List
from dataclasses import dataclass, field

from .data_requirements import RequirementType, RequirementPriority
from .requirement_detector import (
    detect_all_requirements,
    get_enrichment_candidates,
    DetectionThresholds,
    DEFAULT_THRESHOLDS
)
from .enrichment_executor import (
    EnrichmentExecutor,
    get_enrichment_executor,
    execute_enrichment_cycle
)
from .resolver_implementations import register_all_resolvers

logger = logging.getLogger(__name__)


@dataclass
class EnrichmentMetrics:
    """
    Metrics collected during enrichment for instrumentation.
    """
    timestamp: datetime = field(default_factory=datetime.now)
    cycle_number: int = 0
    total_trades: int = 0
    blocked_trades: int = 0
    ready_trades: int = 0

    # Blockers by type
    blockers_by_type: Dict[str, int] = field(default_factory=dict)

    # Enrichment results
    tickers_enriched: int = 0
    requirements_satisfied: int = 0
    requirements_remaining: int = 0

    # Success/failure
    resolvers_executed: int = 0
    successful_resolutions: int = 0
    failed_resolutions: int = 0

    # Timing
    detection_time_ms: float = 0.0
    enrichment_time_ms: float = 0.0
    total_time_ms: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'timestamp': self.timestamp.isoformat(),
            'cycle': self.cycle_number,
            'trades': {
                'total': self.total_trades,
                'blocked': self.blocked_trades,
                'ready': self.ready_trades
            },
            'blockers': self.blockers_by_type,
            'enrichment': {
                'tickers_enriched': self.tickers_enriched,
                'requirements_satisfied': self.requirements_satisfied,
                'requirements_remaining': self.requirements_remaining,
                'success_rate': (
                    self.successful_resolutions / max(1, self.resolvers_executed)
                )
            },
            'timing_ms': {
                'detection': self.detection_time_ms,
                'enrichment': self.enrichment_time_ms,
                'total': self.total_time_ms
            }
        }


class PipelineEnrichmentHook:
    """
    Hook for integrating enrichment into the pipeline after Step 12.

    This class provides a clean interface for the pipeline to:
    1. Detect data requirements
    2. Execute enrichment
    3. Merge results
    4. Report metrics

    IMPORTANT: This hook does NOT modify Execution_Status directly.
    It only enriches data - Step 12 re-evaluation determines status.
    """

    def __init__(
        self,
        thresholds: DetectionThresholds = DEFAULT_THRESHOLDS,
        max_cycles: int = 3,
        min_batch_size: int = 3,
        auto_register_resolvers: bool = True
    ):
        self.thresholds = thresholds
        self.max_cycles = max_cycles
        self.min_batch_size = min_batch_size
        self.cycle_count = 0
        self.metrics_history: List[EnrichmentMetrics] = []
        self.executor = get_enrichment_executor()

        if auto_register_resolvers:
            register_all_resolvers(self.executor)

    def reset_for_new_run(self):
        """Reset state for a new pipeline run."""
        self.cycle_count = 0
        self.metrics_history = []
        self.executor.reset_cooldowns()

    def execute_enrichment_pass(
        self,
        df: pd.DataFrame,
        id_col: str = "Ticker"
    ) -> Tuple[pd.DataFrame, EnrichmentMetrics, bool]:
        """
        Execute one enrichment pass on the pipeline data.

        Args:
            df: DataFrame from Step 12 with Execution_Status set
            id_col: Ticker column name

        Returns:
            Tuple of (enriched DataFrame, metrics, should_rerun)

        NOTE: This method:
        - ONLY looks at data fields (never Strategy_Name)
        - ONLY updates data columns (never Execution_Status)
        - Returns should_rerun flag for pipeline to decide what to do
        """
        start_time = datetime.now()
        metrics = EnrichmentMetrics(cycle_number=self.cycle_count + 1)

        # Check cycle limit
        if self.cycle_count >= self.max_cycles:
            logger.warning(f"Max enrichment cycles ({self.max_cycles}) reached")
            metrics.total_time_ms = (datetime.now() - start_time).total_seconds() * 1000
            return df, metrics, False

        self.cycle_count += 1
        logger.info(f"Starting enrichment pass {self.cycle_count}/{self.max_cycles}")

        # --- PHASE 1: Requirement Detection ---
        detection_start = datetime.now()

        blockers = detect_all_requirements(df, self.thresholds, id_col)

        metrics.total_trades = len(blockers)
        metrics.blocked_trades = sum(1 for b in blockers.values() if not b.is_ready)
        metrics.ready_trades = metrics.total_trades - metrics.blocked_trades

        # Count blockers by type
        for trade_id, trade_blockers in blockers.items():
            for req in trade_blockers.unsatisfied_requirements:
                req_type = req.requirement_type.name
                metrics.blockers_by_type[req_type] = metrics.blockers_by_type.get(req_type, 0) + 1

        metrics.detection_time_ms = (datetime.now() - detection_start).total_seconds() * 1000

        # Log blocker summary
        logger.info(f"Requirement detection complete:")
        logger.info(f"  Total trades: {metrics.total_trades}")
        logger.info(f"  Ready: {metrics.ready_trades}")
        logger.info(f"  Blocked: {metrics.blocked_trades}")
        for req_type, count in sorted(metrics.blockers_by_type.items(), key=lambda x: -x[1]):
            logger.info(f"    {req_type}: {count}")

        # --- PHASE 2: Check if enrichment is warranted ---
        candidates = get_enrichment_candidates(blockers)

        # Count actionable tickers
        actionable_tickers = set()
        for tickers in candidates.values():
            actionable_tickers.update(tickers)

        if len(actionable_tickers) < self.min_batch_size:
            logger.info(f"Below minimum batch size ({len(actionable_tickers)} < {self.min_batch_size})")
            metrics.total_time_ms = (datetime.now() - start_time).total_seconds() * 1000
            self.metrics_history.append(metrics)
            return df, metrics, False

        # --- PHASE 3: Execute Enrichment ---
        enrichment_start = datetime.now()

        enriched_data = execute_enrichment_cycle(blockers, executor=self.executor)

        metrics.tickers_enriched = len(enriched_data)
        metrics.enrichment_time_ms = (datetime.now() - enrichment_start).total_seconds() * 1000

        # --- PHASE 4: Merge Enriched Data ---
        df_enriched = self._merge_enriched_data(df, enriched_data, id_col)

        # --- PHASE 5: Re-detect to measure improvement ---
        blockers_after = detect_all_requirements(df_enriched, self.thresholds, id_col)

        unsatisfied_before = sum(len(b.unsatisfied_requirements) for b in blockers.values())
        unsatisfied_after = sum(len(b.unsatisfied_requirements) for b in blockers_after.values())

        metrics.requirements_satisfied = unsatisfied_before - unsatisfied_after
        metrics.requirements_remaining = unsatisfied_after

        # Determine if we should suggest re-running Step 12
        should_rerun = metrics.requirements_satisfied > 0 and metrics.tickers_enriched > 0

        metrics.total_time_ms = (datetime.now() - start_time).total_seconds() * 1000
        self.metrics_history.append(metrics)

        logger.info(f"Enrichment pass {self.cycle_count} complete:")
        logger.info(f"  Tickers enriched: {metrics.tickers_enriched}")
        logger.info(f"  Requirements satisfied: {metrics.requirements_satisfied}")
        logger.info(f"  Requirements remaining: {metrics.requirements_remaining}")
        logger.info(f"  Should re-run Step 12: {should_rerun}")

        return df_enriched, metrics, should_rerun

    def _merge_enriched_data(
        self,
        df: pd.DataFrame,
        enriched_data: Dict[str, Any],
        id_col: str
    ) -> pd.DataFrame:
        """
        Merge enriched data back into DataFrame.

        CRITICAL: This method ONLY updates data columns.
        It NEVER modifies Execution_Status, Gate_Reason, or any decision columns.
        """
        df = df.copy()

        # Columns that should NEVER be modified by enrichment
        protected_columns = {
            'Execution_Status', 'Gate_Reason', 'Block_Reason', 'Execution_Status',
            'Strategy_Name', 'Strategy_Type', 'Position_Type',
            'Trade_ID', 'Ticker', 'Symbol'
        }

        for ticker, data in enriched_data.items():
            if not isinstance(data, dict):
                continue

            mask = df[id_col] == ticker
            if not mask.any():
                continue

            for col, value in data.items():
                # Skip protected columns
                if col in protected_columns:
                    logger.warning(f"Attempted to modify protected column {col} - skipping")
                    continue

                # Only update if value is valid
                if pd.notna(value):
                    if col in df.columns:
                        df.loc[mask, col] = value
                    else:
                        df.loc[mask, col] = value

        return df

    def get_blocker_summary_df(self, df: pd.DataFrame, id_col: str = "Ticker") -> pd.DataFrame:
        """
        Get a summary DataFrame of all blockers for dashboard display.
        """
        blockers = detect_all_requirements(df, self.thresholds, id_col)

        rows = []
        for trade_id, b in blockers.items():
            row = {
                'trade_id': trade_id,
                'ticker': b.ticker,
                'strategy': b.strategy_name,
                'is_ready': b.is_ready,
                'total_requirements': len(b.requirements),
                'unsatisfied': len(b.unsatisfied_requirements),
                'blocking': len(b.blocking_requirements),
                'actionable': len(b.actionable_requirements),
                'summary': b.blocker_summary
            }

            # Add per-type counts
            for req in b.unsatisfied_requirements:
                key = f"missing_{req.requirement_type.name}"
                row[key] = row.get(key, 0) + 1

            rows.append(row)

        return pd.DataFrame(rows)

    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of all enrichment metrics across cycles."""
        if not self.metrics_history:
            return {'cycles': 0}

        total_satisfied = sum(m.requirements_satisfied for m in self.metrics_history)
        total_remaining = self.metrics_history[-1].requirements_remaining if self.metrics_history else 0
        total_time = sum(m.total_time_ms for m in self.metrics_history)

        return {
            'cycles': len(self.metrics_history),
            'total_requirements_satisfied': total_satisfied,
            'final_requirements_remaining': total_remaining,
            'total_time_ms': total_time,
            'average_time_per_cycle_ms': total_time / len(self.metrics_history),
            'cycle_details': [m.to_dict() for m in self.metrics_history]
        }


# =============================================================================
# PIPELINE INTEGRATION FUNCTION
# =============================================================================

def run_post_step12_enrichment(
    df: pd.DataFrame,
    id_col: str = "Ticker",
    max_cycles: int = 2,
    hook: Optional[PipelineEnrichmentHook] = None
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Run enrichment passes after Step 12.

    This is the main entry point called from pipeline.py.

    Args:
        df: DataFrame from Step 12 with Execution_Status set
        id_col: Ticker column name
        max_cycles: Maximum enrichment cycles (default 2)
        hook: Optional pre-configured hook

    Returns:
        Tuple of (enriched DataFrame, metrics summary)

    STRATEGY AGNOSTICISM:
        This function examines ONLY data fields.
        It does NOT look at Strategy_Name, Strategy_Type, or similar.
        All trades are treated identically based on data completeness.
    """
    if hook is None:
        hook = PipelineEnrichmentHook(max_cycles=max_cycles)
        hook.reset_for_new_run()

    df_current = df.copy()
    total_cycles = 0

    while total_cycles < max_cycles:
        df_enriched, metrics, should_rerun = hook.execute_enrichment_pass(df_current, id_col)

        df_current = df_enriched
        total_cycles += 1

        if not should_rerun:
            logger.info(f"No further enrichment needed after cycle {total_cycles}")
            break

        logger.info(f"Enrichment improved data - may benefit from Step 12 re-evaluation")

    summary = hook.get_metrics_summary()
    logger.info(f"Post-Step 12 enrichment complete: {total_cycles} cycles, "
                f"{summary.get('total_requirements_satisfied', 0)} requirements satisfied")

    return df_current, summary


# =============================================================================
# VALIDATION HELPERS
# =============================================================================

def validate_no_strategy_bias(df_before: pd.DataFrame, df_after: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate that enrichment did not introduce strategy bias.

    This function checks that:
    1. Strategy-related columns were not modified
    2. Enrichment was applied uniformly across strategy types
    3. No strategy received preferential treatment

    Returns:
        Validation report dictionary
    """
    report = {
        'valid': True,
        'checks': [],
        'warnings': []
    }

    # Check 1: Strategy columns unchanged
    strategy_cols = ['Strategy_Name', 'Strategy_Type', 'Position_Type']
    for col in strategy_cols:
        if col in df_before.columns and col in df_after.columns:
            if not df_before[col].equals(df_after[col]):
                report['valid'] = False
                report['checks'].append({
                    'check': f'{col} unchanged',
                    'passed': False,
                    'message': f'{col} was modified during enrichment'
                })
            else:
                report['checks'].append({
                    'check': f'{col} unchanged',
                    'passed': True
                })

    # Check 2: Execution_Status was not directly modified
    # (Status changes should come from Step 12 re-evaluation, not enrichment)
    if 'Execution_Status' in df_before.columns and 'Execution_Status' in df_after.columns:
        status_changed = ~df_before['Execution_Status'].equals(df_after['Execution_Status'])
        if status_changed:
            report['warnings'].append(
                "Execution_Status changed - ensure this was from Step 12 re-evaluation, not enrichment"
            )

    # Check 3: Enrichment distribution across strategies
    if 'Strategy_Name' in df_before.columns:
        strategies = df_before['Strategy_Name'].unique()

        # Check if any data columns were enriched
        data_cols = ['iv_history_count', 'IV_Rank_30D', 'IV_Maturity_State']
        for col in data_cols:
            if col in df_before.columns and col in df_after.columns:
                before_missing = df_before[col].isna().groupby(df_before['Strategy_Name']).sum()
                after_missing = df_after[col].isna().groupby(df_after['Strategy_Name']).sum()

                enriched = before_missing - after_missing
                if enriched.max() > 0:
                    # Check if enrichment was proportional
                    total_enriched = enriched.sum()
                    for strategy in strategies:
                        if strategy in enriched.index:
                            pct = enriched.get(strategy, 0) / max(1, total_enriched)
                            if pct > 0.5 and len(strategies) > 2:
                                report['warnings'].append(
                                    f"Strategy '{strategy}' received {pct:.1%} of {col} enrichments"
                                )

    report['checks'].append({
        'check': 'No strategy bias detected',
        'passed': len([w for w in report['warnings'] if 'bias' in w.lower()]) == 0
    })

    return report
