"""
Enrichment System - Bias-Free, Deterministic Data Enrichment

This package provides a strategy-agnostic, data-driven enrichment system
for the options scan pipeline. It detects missing data, triggers appropriate
resolvers, and cleanly re-enters enriched data into the pipeline.

ARCHITECTURE OVERVIEW
=====================

The system is composed of four main components:

1. DATA REQUIREMENTS (data_requirements.py)
   - Defines the schema for expressing data requirements
   - Pure value objects - immutable, hashable, testable
   - Strategy-agnostic - same schema for all trade types

2. REQUIREMENT DETECTOR (requirement_detector.py)
   - Pure functions that examine data and emit requirements
   - No side effects, no IO, no policy decisions
   - Configurable thresholds via DetectionThresholds

3. RESOLVER REGISTRY (resolver_registry.py)
   - Maps requirements to available resolver implementations
   - Defines rate limits, priorities, and batch capabilities
   - Configuration, not code - easily extensible

4. ENRICHMENT EXECUTOR (enrichment_executor.py)
   - Executes resolvers with rate limiting and backoff
   - Tracks attempts to prevent infinite loops
   - Returns enriched data for pipeline re-entry

5. PIPELINE INTEGRATION (pipeline_integration.py)
   - Hooks into the main scan pipeline
   - Merges enriched data back into DataFrame
   - Determines which stages need re-running


USAGE
=====

Basic usage in pipeline:

    from core.enrichment import enrich_pipeline_data, get_blocker_analysis

    # At end of Step 12 or any stage
    df, result = enrich_pipeline_data(df, stage_name="step12")

    if result.should_rerun_pipeline:
        # Re-run affected stages with enriched data
        for stage in result.affected_stages:
            df = run_stage(df, stage)

    # Get blocker analysis for debugging
    blockers_df = get_blocker_analysis(df)


DESIGN PRINCIPLES
=================

1. STRATEGY-AGNOSTIC
   - No special handling for CSP vs directional vs volatility strategies
   - Requirements are detected based on data, not trading intent

2. DATA-DRIVEN
   - All decisions based on measurable data thresholds
   - No "opinions" hardcoded into the logic

3. DETERMINISTIC
   - Same input data produces same requirements
   - Reproducible for testing and auditing

4. EXTENSIBLE
   - New requirement types can be added via schema
   - New resolvers can be registered via registry
   - Thresholds can be configured externally

5. OBSERVABLE
   - Full audit trail of enrichment actions
   - Statistics and metrics for monitoring

6. SAFE
   - Rate limiting prevents API abuse
   - Max attempts prevent infinite loops
   - Non-destructive merges preserve existing data


INFINITE LOOP PREVENTION
========================

The system prevents infinite loops through multiple mechanisms:

1. MAX_ATTEMPTS - Each requirement has a maximum attempt count (default: 3)
2. COOLDOWN - Exponential backoff between attempts
3. CYCLE_LIMIT - Maximum enrichment cycles per pipeline run (default: 3)
4. UNRESOLVABLE STATE - After max attempts, requirement marked unresolvable


ADDING NEW RESOLVERS
====================

To add a new resolver:

1. Add enum value to ResolverType in resolver_registry.py
2. Register configuration in create_default_registry()
3. Implement the resolver function with signature:
       def my_resolver(tickers: List[str], config: ResolverConfig) -> Dict[str, Any]
4. Register implementation with executor:
       executor.register_resolver_impl(ResolverType.MY_RESOLVER, my_resolver)


CONFIGURATION
=============

Thresholds can be customized:

    from core.enrichment import DetectionThresholds

    custom_thresholds = DetectionThresholds(
        iv_history_mature_days=90,  # Lower threshold
        liquidity_min_oi=50,        # More permissive
    )

    hook = PipelineEnrichmentHook(thresholds=custom_thresholds)
    df, result = hook.detect_and_enrich(df, "step12")

"""

# Data Requirements
from .data_requirements import (
    DataRequirement,
    RequirementType,
    RequirementPriority,
    ResolutionStatus,
    EnrichmentAttempt,
    RequirementResolutionState,
    TradeBlockers,
)

# Requirement Detection
from .requirement_detector import (
    DetectionThresholds,
    DEFAULT_THRESHOLDS,
    detect_requirements_for_row,
    detect_all_requirements,
    get_enrichment_candidates,
)

# Resolver Registry
from .resolver_registry import (
    ResolverType,
    ResolverConfig,
    RateLimit,
    ResolverRegistry,
    DEFAULT_REGISTRY,
    create_default_registry,
    get_resolver_chain,
    get_actionable_resolver_chain,
)

# Enrichment Executor
from .enrichment_executor import (
    EnrichmentExecutor,
    get_enrichment_executor,
    execute_enrichment_cycle,
)

# Pipeline Integration
from .pipeline_integration import (
    EnrichmentResult,
    PipelineEnrichmentHook,
    get_pipeline_enrichment_hook,
    enrich_pipeline_data,
    get_blocker_analysis,
)

# Pipeline Hook (Post-Step 12)
from .pipeline_hook import (
    run_post_step12_enrichment,
    PipelineEnrichmentHook as PostStep12Hook,
    validate_no_strategy_bias,
    EnrichmentMetrics,
)

# Resolver Implementations
from .resolver_implementations import register_all_resolvers

# Volatility Maturity Tier - First-Class Data Classification
from .volatility_maturity import (
    VolatilityMaturityTier,
    MaturityAssessment,
    compute_maturity_tier,
    compute_maturity_for_dataframe,
    get_tickers_needing_enrichment,
)

# Execution Eligibility - Decoupled Evaluation & Execution
from .execution_eligibility import (
    EvaluationStatus,
    ExecutionStatus,
    ExecutionGate,
    EligibilityAssessment,
    classify_strategy_type,
    assess_eligibility,
    compute_eligibility_for_dataframe,
    get_executable_trades,
    get_gated_trades,
)

# Fidelity Demand - Progressive Enrichment Queue
from .fidelity_demand import (
    EnrichmentProgress,
    DemandReport,
    generate_fidelity_demand,
    update_progress_after_scrape,
    get_enrichment_summary,
    check_daily_scrape_needed,
    get_daily_scrape_recommendation,
)

# Trade Explanation - Human-Readable Status Layer
from .trade_explanation import (
    DataGapType,
    DataGap,
    TradeExplanation,
    detect_data_gaps,
    explain_trade,
    add_explanations_to_dataframe,
    generate_scan_summary,
)

# Pipeline Maturity Integration - Post-Step12 Processing
from .pipeline_maturity_integration import (
    apply_maturity_and_eligibility,
    get_final_scan_output,
    export_with_explanations,
    validate_maturity_consistency,
    run_post_step12_integration,
)

__all__ = [
    # Data Requirements
    'DataRequirement',
    'RequirementType',
    'RequirementPriority',
    'ResolutionStatus',
    'EnrichmentAttempt',
    'RequirementResolutionState',
    'TradeBlockers',

    # Requirement Detection
    'DetectionThresholds',
    'DEFAULT_THRESHOLDS',
    'detect_requirements_for_row',
    'detect_all_requirements',
    'get_enrichment_candidates',

    # Resolver Registry
    'ResolverType',
    'ResolverConfig',
    'RateLimit',
    'ResolverRegistry',
    'DEFAULT_REGISTRY',
    'create_default_registry',
    'get_resolver_chain',
    'get_actionable_resolver_chain',

    # Enrichment Executor
    'EnrichmentExecutor',
    'get_enrichment_executor',
    'execute_enrichment_cycle',

    # Pipeline Integration
    'EnrichmentResult',
    'PipelineEnrichmentHook',
    'get_pipeline_enrichment_hook',
    'enrich_pipeline_data',
    'get_blocker_analysis',

    # Pipeline Hook (Post-Step 12)
    'run_post_step12_enrichment',
    'PostStep12Hook',
    'validate_no_strategy_bias',
    'EnrichmentMetrics',
    'register_all_resolvers',

    # Volatility Maturity Tier
    'VolatilityMaturityTier',
    'MaturityAssessment',
    'compute_maturity_tier',
    'compute_maturity_for_dataframe',
    'get_tickers_needing_enrichment',

    # Execution Eligibility
    'EvaluationStatus',
    'ExecutionStatus',
    'ExecutionGate',
    'EligibilityAssessment',
    'classify_strategy_type',
    'assess_eligibility',
    'compute_eligibility_for_dataframe',
    'get_executable_trades',
    'get_gated_trades',

    # Fidelity Demand
    'EnrichmentProgress',
    'DemandReport',
    'generate_fidelity_demand',
    'update_progress_after_scrape',
    'get_enrichment_summary',
    'check_daily_scrape_needed',
    'get_daily_scrape_recommendation',

    # Trade Explanation
    'DataGapType',
    'DataGap',
    'TradeExplanation',
    'detect_data_gaps',
    'explain_trade',
    'add_explanations_to_dataframe',
    'generate_scan_summary',

    # Pipeline Maturity Integration
    'apply_maturity_and_eligibility',
    'get_final_scan_output',
    'export_with_explanations',
    'validate_maturity_consistency',
    'run_post_step12_integration',
]
