"""
Phase 3: Enrichment Runner

Orchestrates all Phase 3 observable computations.

This module is the entry point for Phase 3 enrichment.
It applies all observable calculations in sequence:
- Greeks analysis (skew, kurtosis)
- Time observables (DTE)
- Volatility observables (IV_Rank)
- Event observables (Earnings proximity)
- Capital observables (Capital_Deployed)
- Trade-level aggregates
- Structural enrichments (breakeven, moneyness)
- Quality scoring (PCS - snapshot quality only)

All computations are deterministic and snapshot-safe.
No freezing, no historical dependencies, no exit logic.
"""

import pandas as pd
import logging
from core.data_contracts.config import MANAGEMENT_SAFE_MODE

# Observable modules
from .skew_kurtosis import calculate_skew_and_kurtosis
from .compute_dte import compute_dte
from .auto_enrich_iv import auto_enrich_iv_from_archive  # Automatic IV enrichment
from .compute_iv_rank import compute_iv_rank
from .compute_earnings_proximity import compute_earnings_proximity
from .compute_capital_deployed import compute_capital_deployed
from .compute_trade_aggregates import compute_trade_aggregates
from .compute_breakeven import compute_breakeven
from .compute_moneyness import compute_moneyness
from .compute_pnl_metrics import compute_pnl_metrics, aggregate_trade_pnl
from .compute_pnl_attribution import compute_pnl_attribution, aggregate_trade_pnl_attribution, rehydrate_entry_data
from .compute_assignment_risk import compute_assignment_risk
from .pcs_score import calculate_pcs

logger = logging.getLogger(__name__)


def run_phase3_enrichment(df: pd.DataFrame, snapshot_ts: pd.Timestamp = None) -> pd.DataFrame:
    """
    Run all Phase 3 observable enrichments.
    
    Parameters
    ----------
    df : pd.DataFrame
        Phase 2 output (must contain TradeID, Strategy, Greeks, etc.)
    snapshot_ts : pd.Timestamp, optional
        Explicit snapshot timestamp for deterministic DTE/Earnings calculation
        If None, uses pd.Timestamp.now() (Phase 4 will provide this)
    
    Returns
    -------
    pd.DataFrame
        Fully enriched Phase 3 snapshot with all observables
    
    Notes
    -----
    Execution order is important:
    1. Time observables (DTE) - needed for other calculations
    2. Volatility observables (IV_Rank)
    3. Event observables (Earnings)
    4. Capital observables (Capital_Deployed)
    5. Trade aggregates (requires leg-level Greeks)
    6. Structural enrichments (breakeven, moneyness)
    7. Greeks analysis (skew, kurtosis)
    8. Quality scoring (PCS - uses current Greeks, NOT entry)
    
    All observables are recomputable. No historical dependencies.
    """
    if df.empty:
        logger.warning("Empty DataFrame passed to run_phase3_enrichment")
        return df
    
    logger.info(f"Starting Phase 3 enrichment for {len(df)} positions")
    
    # === Canonical Identity Law Enforcement ===
    # Ensure Underlying_Ticker exists and is populated for all options
    option_mask = df['AssetType'] == 'OPTION'
    if option_mask.any():
        missing_ticker = df.loc[option_mask, 'Underlying_Ticker'].isna()
        if missing_ticker.any():
            bad_symbols = df.loc[option_mask & missing_ticker, 'Symbol'].tolist()[:5]
            raise ValueError(
                f"❌ DATA CONTRACT VIOLATION: {missing_ticker.sum()} options missing Underlying_Ticker.\n"
                f"   Symbols: {bad_symbols}\n"
                f"   Underlying_Ticker must be populated in Phase 1/2 normalization."
            )
    
    # 0. Rehydrate entry data from DB (CRITICAL: must happen before any attribution)
    df = rehydrate_entry_data(df)
    
    # Use provided timestamp or current time
    reference_ts = snapshot_ts if snapshot_ts is not None else pd.Timestamp.now()
    
    # 1. Time observables
    df = compute_dte(df, snapshot_ts=reference_ts)
    
    # 2. Automatic IV enrichment (fetch from archive before IV_Rank calculation)
    if not MANAGEMENT_SAFE_MODE:
        df = auto_enrich_iv_from_archive(df, as_of_date=reference_ts)
    
    # 3. Volatility observables (requires IV Mid from step 2)
    if not MANAGEMENT_SAFE_MODE:
        df = compute_iv_rank(df)
    
    # 4. Event observables
    if not MANAGEMENT_SAFE_MODE:
        df = compute_earnings_proximity(df, snapshot_ts=reference_ts)
    
    # 5. Capital observables
    df = compute_capital_deployed(df)
    
    # 6. Trade-level aggregates (CRITICAL: must come after leg-level Greeks)
    df = compute_trade_aggregates(df)
    
    # 7. Structural enrichments
    df = compute_breakeven(df)
    df = compute_moneyness(df)
    
    # 8. P&L and Performance Metrics
    df = compute_pnl_metrics(df, snapshot_ts=reference_ts)
    df = aggregate_trade_pnl(df)
    
    # 8b. P&L Attribution (requires entry Greeks from Phase 4 freeze)
    df = compute_pnl_attribution(df)
    df = aggregate_trade_pnl_attribution(df)
    
    # 8c. Phase 7A/B: Windowed Drift & Smoothing (Facts Only)
    # Computes deltas across 1D, 3D, 10D, and Structural windows + SMA/Accel
    df = compute_windowed_drift(df)
    
    # 9. Assignment Risk Scoring
    df = compute_assignment_risk(df)
    
    # 10. Greeks analysis
    df = calculate_skew_and_kurtosis(df)
    
    # 11. Quality scoring (Current_PCS - evolving score using current Greeks and time-series)
    # Note: Entry_PCS (frozen baseline) is computed in Phase 4 freeze_entry_data
    df = calculate_pcs(df)  # Legacy Current_PCS (Greeks-only)
    
    # 11b. Current_PCS v2 (RAG-compliant multi-factor: IV_Rank 30%, Liquidity 25%, Greeks 20%)
    from .compute_current_pcs_v2 import compute_current_pcs_v2
    df = compute_current_pcs_v2(df)
    
    logger.info(f"Phase 3 enrichment complete: {len(df.columns)} columns")
    
    return df
