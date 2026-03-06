"""
Pipeline Integration - Hooks Enrichment System into Scan Pipeline

This module provides the integration layer between the enrichment system
and the main scan pipeline. It handles:
- Detecting requirements after pipeline stages
- Triggering enrichment when needed
- Merging enriched data back into pipeline DataFrame
- Re-running affected pipeline stages

DESIGN PRINCIPLES:
1. NON-INVASIVE - Works with existing pipeline, no major refactoring
2. DETERMINISTIC - Same data produces same enrichment decisions
3. OBSERVABLE - Full audit trail of enrichment actions
4. SAFE - Never corrupts pipeline state; rollback on failure
5. STRATEGY-AGNOSTIC - No special handling for any strategy type
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field

from .data_requirements import (
    DataRequirement,
    RequirementType,
    TradeBlockers,
    RequirementPriority
)
from .requirement_detector import (
    detect_all_requirements,
    get_enrichment_candidates,
    DetectionThresholds,
    DEFAULT_THRESHOLDS
)
from .resolver_registry import (
    ResolverRegistry,
    DEFAULT_REGISTRY,
    ResolverType
)
from .enrichment_executor import (
    EnrichmentExecutor,
    execute_enrichment_cycle,
    get_enrichment_executor
)

logger = logging.getLogger(__name__)


@dataclass
class EnrichmentResult:
    """Result of an enrichment cycle."""
    tickers_enriched: List[str]
    data_obtained: Dict[str, Any]
    requirements_satisfied: int
    requirements_remaining: int
    elapsed_seconds: float
    should_rerun_pipeline: bool
    affected_stages: List[str]


@dataclass
class PipelineEnrichmentHook:
    """
    Hook that integrates enrichment into the pipeline.

    This class provides methods to:
    1. Detect requirements at any pipeline stage
    2. Trigger enrichment when thresholds are met
    3. Merge enriched data back into DataFrame
    4. Determine which stages need to be re-run
    """
    thresholds: DetectionThresholds = field(default_factory=lambda: DEFAULT_THRESHOLDS)
    registry: ResolverRegistry = field(default_factory=lambda: DEFAULT_REGISTRY)
    executor: EnrichmentExecutor = field(default_factory=get_enrichment_executor)

    # Configuration
    auto_enrich: bool = True               # Automatically trigger enrichment
    min_enrichment_batch_size: int = 5     # Don't enrich for fewer tickers
    max_enrichment_cycles: int = 3         # Prevent infinite loops
    enrichment_cycle_count: int = 0        # Current cycle count

    # Audit trail
    enrichment_history: List[Dict[str, Any]] = field(default_factory=list)

    def detect_and_enrich(
        self,
        df: pd.DataFrame,
        stage_name: str = "unknown",
        id_col: str = "Ticker"
    ) -> Tuple[pd.DataFrame, EnrichmentResult]:
        """
        Main entry point: Detect requirements and enrich if needed.

        This method:
        1. Detects all unsatisfied requirements
        2. Determines if enrichment is warranted
        3. Executes enrichment if auto_enrich is True
        4. Merges results back into DataFrame
        5. Returns updated DataFrame and result summary

        Args:
            df: Pipeline DataFrame at current stage
            stage_name: Name of current pipeline stage (for logging)
            id_col: Column name for ticker identifier

        Returns:
            Tuple of (enriched DataFrame, EnrichmentResult)
        """
        start_time = datetime.now()

        # Detect all requirements
        blockers = detect_all_requirements(df, self.thresholds, id_col)

        # Get enrichment candidates
        candidates = get_enrichment_candidates(blockers)

        # Count unsatisfied requirements
        total_unsatisfied = sum(
            len(b.unsatisfied_requirements) for b in blockers.values()
        )

        # Log summary
        logger.info(f"[{stage_name}] Requirement detection complete:")
        logger.info(f"  Total trades: {len(blockers)}")
        logger.info(f"  Unsatisfied requirements: {total_unsatisfied}")
        for req_type, tickers in candidates.items():
            logger.info(f"    {req_type.name}: {len(tickers)} tickers")

        # Check if enrichment is warranted
        should_enrich = self._should_enrich(candidates, blockers)

        if not should_enrich or not self.auto_enrich:
            result = EnrichmentResult(
                tickers_enriched=[],
                data_obtained={},
                requirements_satisfied=0,
                requirements_remaining=total_unsatisfied,
                elapsed_seconds=(datetime.now() - start_time).total_seconds(),
                should_rerun_pipeline=False,
                affected_stages=[]
            )
            return df, result

        # Check cycle limit
        if self.enrichment_cycle_count >= self.max_enrichment_cycles:
            logger.warning(f"Max enrichment cycles ({self.max_enrichment_cycles}) reached. "
                           "Stopping to prevent infinite loop.")
            result = EnrichmentResult(
                tickers_enriched=[],
                data_obtained={},
                requirements_satisfied=0,
                requirements_remaining=total_unsatisfied,
                elapsed_seconds=(datetime.now() - start_time).total_seconds(),
                should_rerun_pipeline=False,
                affected_stages=[]
            )
            return df, result

        # Execute enrichment
        self.enrichment_cycle_count += 1
        logger.info(f"Starting enrichment cycle {self.enrichment_cycle_count}")

        enriched_data = execute_enrichment_cycle(blockers, executor=self.executor)

        # Merge enriched data back into DataFrame
        df_enriched, merge_stats = self._merge_enriched_data(df, enriched_data, id_col)

        # Re-detect to see what was satisfied
        blockers_after = detect_all_requirements(df_enriched, self.thresholds, id_col)
        total_unsatisfied_after = sum(
            len(b.unsatisfied_requirements) for b in blockers_after.values()
        )

        satisfied = total_unsatisfied - total_unsatisfied_after

        # Determine which stages need re-running
        affected_stages = self._determine_affected_stages(enriched_data)

        elapsed = (datetime.now() - start_time).total_seconds()

        result = EnrichmentResult(
            tickers_enriched=list(enriched_data.keys()),
            data_obtained=enriched_data,
            requirements_satisfied=satisfied,
            requirements_remaining=total_unsatisfied_after,
            elapsed_seconds=elapsed,
            should_rerun_pipeline=satisfied > 0,
            affected_stages=affected_stages
        )

        # Record in audit trail
        self.enrichment_history.append({
            'timestamp': datetime.now().isoformat(),
            'stage': stage_name,
            'cycle': self.enrichment_cycle_count,
            'tickers_enriched': len(result.tickers_enriched),
            'requirements_satisfied': satisfied,
            'elapsed_seconds': elapsed
        })

        logger.info(f"Enrichment cycle {self.enrichment_cycle_count} complete:")
        logger.info(f"  Tickers enriched: {len(result.tickers_enriched)}")
        logger.info(f"  Requirements satisfied: {satisfied}")
        logger.info(f"  Requirements remaining: {total_unsatisfied_after}")
        logger.info(f"  Should rerun pipeline: {result.should_rerun_pipeline}")

        return df_enriched, result

    def _should_enrich(
        self,
        candidates: Dict[RequirementType, List[str]],
        blockers: Dict[str, TradeBlockers]
    ) -> bool:
        """Determine if enrichment should be triggered."""
        # No candidates = nothing to enrich
        if not candidates:
            return False

        # Count actionable tickers
        actionable_tickers = set()
        for tickers in candidates.values():
            actionable_tickers.update(tickers)

        # Check minimum batch size
        if len(actionable_tickers) < self.min_enrichment_batch_size:
            logger.debug(f"Below minimum batch size ({len(actionable_tickers)} < "
                         f"{self.min_enrichment_batch_size})")
            return False

        # Check if any P1 (blocking) requirements exist
        has_blocking = any(
            any(r.priority == RequirementPriority.P1_BLOCKING
                for r in b.unsatisfied_requirements)
            for b in blockers.values()
        )

        if has_blocking:
            logger.debug("Has blocking (P1) requirements - enrichment warranted")
            return True

        # Check if significant portion of trades are blocked
        blocked_ratio = sum(1 for b in blockers.values() if not b.is_ready) / len(blockers)
        if blocked_ratio > 0.5:
            logger.debug(f"High block ratio ({blocked_ratio:.1%}) - enrichment warranted")
            return True

        return False

    def _merge_enriched_data(
        self,
        df: pd.DataFrame,
        enriched_data: Dict[str, Any],
        id_col: str
    ) -> Tuple[pd.DataFrame, Dict[str, int]]:
        """
        Merge enriched data back into the DataFrame.

        This is a non-destructive merge that:
        1. Only updates cells where enriched data is available
        2. Preserves existing data that wasn't enriched
        3. Tracks what was updated for auditing

        Returns:
            Tuple of (updated DataFrame, merge statistics)
        """
        df = df.copy()
        stats = {'rows_updated': 0, 'cells_updated': 0}

        for ticker, data in enriched_data.items():
            if not isinstance(data, dict):
                continue

            # Find rows for this ticker
            mask = df[id_col] == ticker
            if not mask.any():
                continue

            stats['rows_updated'] += mask.sum()

            # Update each field
            for col, value in data.items():
                if col in df.columns:
                    # Only update if value is not None/NaN
                    if pd.notna(value):
                        df.loc[mask, col] = value
                        stats['cells_updated'] += mask.sum()
                else:
                    # Add new column
                    df.loc[mask, col] = value
                    stats['cells_updated'] += mask.sum()

        logger.debug(f"Merged enriched data: {stats['rows_updated']} rows, "
                     f"{stats['cells_updated']} cells updated")

        return df, stats

    def _determine_affected_stages(
        self,
        enriched_data: Dict[str, Any]
    ) -> List[str]:
        """Determine which pipeline stages are affected by the enriched data."""
        affected = set()

        # Check what types of data were enriched
        all_fields = set()
        for data in enriched_data.values():
            if isinstance(data, dict):
                all_fields.update(data.keys())

        # Map fields to stages
        field_to_stage = {
            'iv_history_count': ['step2', 'step3'],
            'IV_Maturity_State': ['step2', 'step12'],
            'IV_Rank_30D': ['step2', 'step7', 'step10'],
            'bid': ['step9', 'step12'],
            'ask': ['step9', 'step12'],
            'delta': ['step8', 'step11'],
            'gamma': ['step8', 'step11'],
            'theta': ['step8', 'step11'],
            'vega': ['step8', 'step11'],
        }

        for field in all_fields:
            field_lower = field.lower()
            for known_field, stages in field_to_stage.items():
                if known_field.lower() in field_lower:
                    affected.update(stages)

        return sorted(list(affected))

    def reset_cycle_count(self):
        """Reset the enrichment cycle count (call at start of each pipeline run)."""
        self.enrichment_cycle_count = 0

    def get_blocker_summary_df(
        self,
        df: pd.DataFrame,
        id_col: str = "Ticker"
    ) -> pd.DataFrame:
        """
        Get a DataFrame summarizing blockers for each trade.

        Useful for debugging and dashboard display.
        """
        blockers = detect_all_requirements(df, self.thresholds, id_col)

        rows = []
        for trade_id, b in blockers.items():
            rows.append({
                'trade_id': trade_id,
                'ticker': b.ticker,
                'strategy': b.strategy_name,
                'is_ready': b.is_ready,
                'total_requirements': len(b.requirements),
                'unsatisfied_count': len(b.unsatisfied_requirements),
                'blocking_count': len(b.blocking_requirements),
                'blocker_summary': b.blocker_summary,
                'actionable_count': len(b.actionable_requirements)
            })

        return pd.DataFrame(rows)


# Global hook instance
_hook: Optional[PipelineEnrichmentHook] = None


def get_pipeline_enrichment_hook() -> PipelineEnrichmentHook:
    """Get or create the global pipeline enrichment hook."""
    global _hook
    if _hook is None:
        _hook = PipelineEnrichmentHook()
    return _hook


def enrich_pipeline_data(
    df: pd.DataFrame,
    stage_name: str = "unknown",
    id_col: str = "Ticker",
    hook: Optional[PipelineEnrichmentHook] = None
) -> Tuple[pd.DataFrame, EnrichmentResult]:
    """
    Convenience function to enrich pipeline data.

    This is the main entry point for pipeline stages that want
    to trigger enrichment.

    Usage in pipeline:
        df, result = enrich_pipeline_data(df, stage_name="step12")
        if result.should_rerun_pipeline:
            # Re-run affected stages
            pass
    """
    if hook is None:
        hook = get_pipeline_enrichment_hook()

    return hook.detect_and_enrich(df, stage_name, id_col)


def get_blocker_analysis(
    df: pd.DataFrame,
    id_col: str = "Ticker"
) -> pd.DataFrame:
    """
    Get a DataFrame analyzing all blockers.

    Returns a DataFrame with one row per trade showing:
    - Whether it's ready
    - What's blocking it
    - Whether it can be actively resolved
    """
    hook = get_pipeline_enrichment_hook()
    return hook.get_blocker_summary_df(df, id_col)
