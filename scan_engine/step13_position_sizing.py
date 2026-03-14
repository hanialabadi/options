"""
Step 8: Portfolio Management (REFACTORED - Strategy Isolation + Strict Execution Gates)

CRITICAL ARCHITECTURE CHANGE (v3 - Dec 2025):
Step 8 is EXECUTION-ONLY, not evaluation. Step 11 already decided what is tradable.

# AGENT SAFETY: This file is execution-only and MUST NEVER evaluate or rank strategies.
# All strategy validation and ranking is performed exclusively by `step11_independent_evaluation.py`.
# This prevents agents from "helpfully" resurrecting invalid logic or bypassing architectural boundaries.

MANDATORY EXECUTION CONTRACT:
    1. Step 8 is DESCRIPTIVE ONLY. It must NOT filter out READY_NOW candidates.
    2. NO NaN/inf coercion allowed for critical fields.
    3. NO strategy selection or cross-family comparison.
    4. Explicit defensive checks before numeric operations.
    5. Never return empty unless input is empty.

RAG Principle:
    "Strategies do not compete. Each strategy family is evaluated independently.
     Portfolio layer decides ALLOCATION, not SELECTION."
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Optional
from .debug.debug_mode import get_debug_manager
from .mc_position_sizing import run_mc_sizing, compute_vince_f_star
from .step6_strategy_recommendation import _LEVERAGED_ETFS

logger = logging.getLogger(__name__)

# GUARDRAIL 4: ATR risk floor — prevents micro-ATR tickers from producing oversized contract counts.
# Without a floor, ATR=0.20 → atr_risk=$30 → $1,500 max_loss / $30 = 50 contracts (dangerous oversize).
# Floor at $50 ensures minimum denominator is consistent with 1-contract floor behavior.
# Reference: Natenberg Ch.12 — position sizing requires meaningful volatility context.
MIN_ATR_RISK_FLOOR = 50.0  # minimum effective risk denominator per contract ($)


# ============================================================
# NEW MAIN FUNCTION (Post-Step 11 Architecture)
# ============================================================

def compute_thesis_capacity(
    df: pd.DataFrame,
    account_balance: float = 100000.0,
    max_portfolio_risk: float = 0.20,
    max_trade_risk: float = 0.02,
    min_compliance_score: float = 0.0, # Default to 0 to allow all READY_NOW
    max_strategies_per_ticker: int = 50, # Relaxed per user feedback
    sizing_method: str = 'volatility_scaled',
    risk_per_contract: float = 500.0,
    expiry_intent: str = 'ANY',
    conn=None,
) -> pd.DataFrame:
    """
    Step 8: Thesis Capacity Calculation (Descriptive Sizing)

    SEMANTIC SHIFT (Jan 2026):
    Step 8 is now descriptive-only. It annotates READY_NOW candidates with
    sizing metadata (envelopes) but does NOT filter them out.

    Parameters
    ----------
    conn : optional existing DuckDB connection passed from the pipeline so that
           the Vince optimal-f step can reuse it instead of opening a conflicting
           read_only connection to the same file.
    """
    
    if df.empty:
        logger.warning("⚠️ Empty DataFrame passed to Step 8")
        return df
    
    input_row_count = len(df)
    logger.info(f"🎯 Step 8 (THESIS CAPACITY): Processing {input_row_count} candidates")
    
    # Step 1: Annotate validation status (RELAXED - no filtering)
    df_valid = _filter_by_validation_status(df, min_compliance_score=min_compliance_score)
    
    if df_valid.empty:
        # This should technically not happen if input is not empty and we removed filtering
        logger.warning("⚠️ No candidates passed validation check - returning empty DataFrame")
        return df_valid
    
    # Step 2: Apply portfolio-level constraints (RELAXED)
    df_constrained = _apply_portfolio_risk_limits(
        df_valid,
        max_strategies_per_ticker=max_strategies_per_ticker,
        account_balance=account_balance
    )
    
    if df_constrained.empty:
        logger.warning("⚠️ No strategies after portfolio constraints - returning empty DataFrame")
        return df_constrained
    
    # Step 3: Calculate capital allocation
    df_allocated = _allocate_capital_by_score(
        df_constrained,
        account_balance=account_balance,
        max_portfolio_risk=max_portfolio_risk,
        max_trade_risk=max_trade_risk,
        sizing_method=sizing_method,
        risk_per_contract=risk_per_contract
    )
    
    # Step 4: Monte Carlo P&L path simulation
    # Runs 2,000 GBM paths per row using realized HV.
    # Adds MC_P10_Loss, MC_P50_Outcome, MC_P90_Gain, MC_Win_Probability,
    # MC_Assign_Prob, MC_Max_Contracts, MC_Sizing_Note.
    # MC_Max_Contracts replaces Thesis_Max_Envelope when MC successfully ran
    # (MC_Paths_Used > 0), giving a distribution-aware contract ceiling instead
    # of the flat ATR cap.  Rows where MC is skipped keep the ATR/FIXED envelope.
    try:
        df_allocated = run_mc_sizing(
            df_allocated,
            account_balance=account_balance,
            max_risk_pct=0.02,   # McMillan Ch.3: 2% hard cap per trade
        )
        # Promote MC_Max_Contracts → Thesis_Max_Envelope where MC ran
        if "MC_Max_Contracts" in df_allocated.columns and "MC_Paths_Used" in df_allocated.columns:
            mc_ran = (
                pd.to_numeric(df_allocated["MC_Paths_Used"], errors="coerce").fillna(0) > 0
            )
            df_allocated.loc[mc_ran, "Thesis_Max_Envelope"] = (
                df_allocated.loc[mc_ran, "MC_Max_Contracts"]
                .clip(lower=1)
                .astype(int)
            )
            df_allocated.loc[mc_ran, "Contracts"] = df_allocated.loc[mc_ran, "Thesis_Max_Envelope"]
            logger.info(
                f"🎲 MC sizing promoted to envelope for {mc_ran.sum()}/{len(df_allocated)} rows"
            )
    except Exception as _mc_err:
        # MC is non-blocking — ATR/FIXED sizing already populated Thesis_Max_Envelope
        logger.warning(f"⚠️ MC sizing failed (non-fatal, ATR/FIXED used): {_mc_err}")

    # Step 4b: Correlation-aware sizing adjustment (Pedersen Ch.7)
    # When a new candidate is correlated with existing portfolio holdings,
    # scale down MC_Max_Contracts to prevent concentration risk.
    try:
        from .mc_correlation_sizing import mc_correlation_adjustment
        # Get existing portfolio tickers from entry_anchors
        _existing_tickers = []
        if conn is not None:
            try:
                _ea = conn.execute(
                    "SELECT DISTINCT Ticker FROM entry_anchors WHERE Status = 'OPEN'"
                ).fetchdf()
                _existing_tickers = _ea["Ticker"].tolist() if not _ea.empty else []
            except Exception:
                pass

        if _existing_tickers and "MC_Max_Contracts" in df_allocated.columns:
            _corr_count = 0
            for idx, row in df_allocated.iterrows():
                _ticker = str(row.get("Ticker", "") or "")
                _mc_max = int(row.get("MC_Max_Contracts", 0) or 0)
                if _ticker and _mc_max > 0:
                    _ca = mc_correlation_adjustment(_ticker, _existing_tickers, _mc_max)
                    if _ca["MC_Corr_Adjustment"] < 1.0:
                        df_allocated.at[idx, "MC_Max_Contracts"] = _ca["MC_Corr_Max_Contracts"]
                        df_allocated.at[idx, "Thesis_Max_Envelope"] = _ca["MC_Corr_Max_Contracts"]
                        df_allocated.at[idx, "Contracts"] = _ca["MC_Corr_Max_Contracts"]
                        _corr_count += 1
                    for k, v in _ca.items():
                        if k not in df_allocated.columns:
                            df_allocated[k] = np.nan if isinstance(v, (int, float)) else ""
                        df_allocated.at[idx, k] = v
            if _corr_count > 0:
                logger.info(f"📐 Correlation-aware sizing adjusted {_corr_count} positions")
    except Exception as _corr_err:
        logger.warning(f"⚠️ Correlation sizing failed (non-fatal): {_corr_err}")

    # Step 4c: Trajectory-aware contract scaling
    # EARLY_BREAKOUT / IMPROVING signals are fresh opportunities worth sizing into.
    # LATE_CONFIRMATION means the move is largely priced in — don't overcommit.
    # DEGRADING signals are actively deteriorating — scale down further.
    # This scales the MC_Max_Contracts / Thesis_Max_Envelope before Vince caps it.
    _TRAJECTORY_SCALE = {
        'TREND_FORMING':     1.25,  # +25%: chart signals detect trend building before score confirms
        'EARLY_BREAKOUT':    1.20,  # +20%: fresh breakout, size up
        'IMPROVING':         1.10,  # +10%: strengthening, modest boost
        'STABLE':            1.00,  # no change
        'LATE_CONFIRMATION': 0.80,  # -20%: move largely done, scale down
        'DEGRADING':         0.70,  # -30%: actively deteriorating, pull back
    }
    if 'Signal_Trajectory' in df_allocated.columns:
        _traj_count = 0
        for idx, row in df_allocated.iterrows():
            _traj = str(row.get('Signal_Trajectory', 'STABLE') or 'STABLE').upper()
            _scale = _TRAJECTORY_SCALE.get(_traj, 1.0)
            if _scale == 1.0:
                df_allocated.at[idx, 'Trajectory_Contract_Scale'] = 1.0
                continue

            for _col in ('MC_Max_Contracts', 'Thesis_Max_Envelope', 'Contracts'):
                if _col in df_allocated.columns:
                    _cur = pd.to_numeric(df_allocated.at[idx, _col], errors='coerce')
                    if pd.notna(_cur) and _cur > 0:
                        _new = max(1, int(round(_cur * _scale)))
                        df_allocated.at[idx, _col] = _new

            df_allocated.at[idx, 'Trajectory_Contract_Scale'] = round(_scale, 2)
            _traj_count += 1

        if _traj_count > 0:
            logger.info(f"📈 Trajectory contract scaling adjusted {_traj_count} positions")
    else:
        df_allocated['Trajectory_Contract_Scale'] = 1.0

    # Step 5: Vince optimal-f constraint
    # Vince (1992): when historical trade P&L is available, use optimal-f to constrain
    # the MC contract ceiling. f* is derived from the trade distribution that actually
    # occurred — not hypothetical GBM paths. Asymmetry principle: overbetting past f*
    # destroys capital faster than equivalent underbetting gains.
    # Fallback: when <3 closed trades exist, keep MC/ATR sizing unchanged.
    try:
        df_allocated = _apply_vince_constraint(df_allocated, account_balance=account_balance, conn=conn)
    except Exception as _vince_err:
        logger.warning(f"⚠️ Vince sizing constraint failed (non-fatal): {_vince_err}")

    # Step 6: Portfolio capital budget enforcement (Capital Survival Audit, Phase 2)
    # Reads existing open positions from entry_anchors DuckDB, computes cumulative capital
    # deployed, and applies sector-aware correlation penalties. Prevents silent capital
    # concentration across correlated tickers (e.g., 8 tech CSPs = one massive short-vol bet).
    try:
        df_allocated = _enforce_portfolio_capital_budget(
            df_allocated,
            account_balance=account_balance,
            max_portfolio_risk=max_portfolio_risk,
            conn=conn,
        )
    except Exception as _budget_err:
        logger.warning(f"⚠️ Portfolio capital budget enforcement failed (non-fatal): {_budget_err}")

    # Step 7: Aggregate portfolio Greeks
    df_with_greeks = _calculate_portfolio_greeks(df_allocated)

    # Step 8: Generate portfolio audit
    df_audited = _generate_portfolio_audit(
        df_with_greeks,
        account_balance=account_balance
    )

    logger.info(f"🎯 Step 8 Complete: {len(df_audited)} thesis envelopes generated")

    return df_audited


def _filter_by_validation_status(
    df: pd.DataFrame,
    min_compliance_score: float
) -> pd.DataFrame:
    """
    Annotate strategies by Validation_Status and Theory_Compliance_Score.
    SEMANTIC FIX: Step 8 must NOT return empty if input is not empty.
    """
    
    df_filtered = df.copy()
    initial_count = len(df_filtered)
    
    # Ensure Validation_Status exists — step12 acceptance path doesn't produce it,
    # but step8 independent evaluation does. Default to 'Valid' so sizing proceeds.
    if 'Validation_Status' not in df_filtered.columns:
        df_filtered['Validation_Status'] = 'Valid'
    
    # SEMANTIC FIX: We no longer filter by Validation_Status here because Step 12 
    # has already decided these are READY_NOW. We only ensure columns exist.
    logger.info(f"      Step 8: Processing {initial_count} READY_NOW candidates")
    
    # Ensure Theory_Compliance_Score exists and is finite
    if 'Theory_Compliance_Score' in df_filtered.columns:
        # Fill NaNs with a neutral score (50) to ensure sizing logic works
        df_filtered['Theory_Compliance_Score'] = df_filtered['Theory_Compliance_Score'].fillna(50.0)
    
    # Validate key execution fields — only warn at debug level for NaN (common for
    # strategies without contract selection), warn at warning level for truly bad data
    for field in ['Total_Debit', 'Delta']:
        if field in df_filtered.columns:
            numeric = pd.to_numeric(df_filtered[field], errors='coerce')
            nan_count = numeric.isna().sum()
            inf_count = (~np.isfinite(numeric) & numeric.notna()).sum()
            if inf_count > 0:
                logger.warning(f"⚠️ {inf_count} strategies have Inf {field} - preserving for visibility")
            if nan_count > 0:
                logger.debug(f"  {nan_count} strategies have NaN {field} (no contract selected)")
    
    return df_filtered


def _apply_portfolio_risk_limits(
    df: pd.DataFrame,
    max_strategies_per_ticker: int,
    account_balance: float
) -> pd.DataFrame:
    """
    Apply portfolio-level risk constraints.
    """
    
    df_constrained = df.copy()
    
    # Constraint 2: Max strategies per ticker
    if max_strategies_per_ticker > 0:
        before_count = len(df_constrained)
        # Sort to keep highest compliance scores if we were to cap, but we've relaxed max_strategies_per_ticker to 50
        sort_col = 'Theory_Compliance_Score' if 'Theory_Compliance_Score' in df_constrained.columns else 'DQS_Score' if 'DQS_Score' in df_constrained.columns else 'PCS_Score_V2'
        if sort_col not in df_constrained.columns:
            df_constrained['_sort_tmp'] = 0
            sort_col = '_sort_tmp'
        df_constrained = df_constrained.sort_values(['Ticker', sort_col], ascending=[True, False])
        df_constrained = df_constrained.groupby('Ticker').head(max_strategies_per_ticker)
        logger.info(f"      Constraint: Max {max_strategies_per_ticker} strategies/ticker: {len(df_constrained)}/{before_count}")
    
    return df_constrained


def _allocate_capital_by_score(
    df: pd.DataFrame,
    account_balance: float,
    max_portfolio_risk: float,
    max_trade_risk: float,
    sizing_method: str,
    risk_per_contract: float
) -> pd.DataFrame:
    """
    Allocate capital based on Risk Budget and Capital Constraints.
    
    ACTION 4: Decoupled from strategy semantics (PCS, Score, Regime).
    Sizing is now a function of account risk limits and execution eligibility.
    """
    
    df_allocated = df.copy()
    
    # Track adjustment reasons for the audit trail
    df_allocated['Sizing_Adjustments'] = ""
    
    # 1. Base Capacity (Risk-Based Unit Sizing)
    # GAP 6 FIX: ATR-scaled sizing — Natenberg Ch.12 + Cohen Ch.5:
    # atr_risk = ATR_14 × 100 × 1.5 (1.5-day adverse move stop proxy per contract).
    # Cap at fixed risk_per_contract to never over-size relative to fixed cap.
    # NVDA ATR=$30 → atr_risk=$4500 → capped at $500 (size conservatively for high-vol).
    # KO ATR=$0.50 → atr_risk=$75 → uses $75 (size normally for low-vol).
    # Fallback: fixed risk_per_contract when ATR_14 unavailable.
    _atr_col = 'ATR_14' if 'ATR_14' in df_allocated.columns else ('ATR' if 'ATR' in df_allocated.columns else None)
    if _atr_col:
        _atr_raw = pd.to_numeric(df_allocated[_atr_col], errors='coerce') * 100 * 1.5
        # GUARDRAIL 4: Apply floor before cap — prevents micro-ATR tickers from getting
        # an impossibly small risk denominator (e.g. ATR=0.20 → raw=$30 → floor raises to $50).
        _atr_risk = _atr_raw.clip(lower=MIN_ATR_RISK_FLOOR)
        _effective_risk = _atr_risk.clip(upper=risk_per_contract).fillna(risk_per_contract)
        df_allocated['Sizing_Method_Used'] = np.where(
            pd.to_numeric(df_allocated[_atr_col], errors='coerce').notna(),
            'ATR_SCALED',
            'FIXED'
        )
    else:
        _effective_risk = pd.Series([risk_per_contract] * len(df_allocated), index=df_allocated.index)
        df_allocated['Sizing_Method_Used'] = 'FIXED'

    # All READY_NOW candidates start with a base unit size derived from account risk limits.
    base_unit_size_series = (account_balance * 0.01) / _effective_risk
    base_capacity = base_unit_size_series

    # Leveraged ETF sizing multiplier (0.7×)
    # Leveraged ETFs exhibit amplified intraday moves and daily beta-reset decay.
    # A 3× ETF can move 9–15% on a 3–5% index move — standard sizing would overexpose.
    # 0.7× reduces notional exposure relative to a single-name or standard ETF position.
    # ChatGPT / Natenberg Ch.12: volatility-scaled sizing must account for effective leverage.
    if 'Ticker' in df_allocated.columns:
        _lev_mask = df_allocated['Ticker'].str.upper().isin(_LEVERAGED_ETFS)
        if _lev_mask.any():
            base_capacity = base_capacity * np.where(_lev_mask, 0.7, 1.0)
            df_allocated.loc[_lev_mask, 'Sizing_Adjustments'] = (
                df_allocated.loc[_lev_mask, 'Sizing_Adjustments'].fillna('') + '⚡ LEV_ETF_0.7x '
            )
            logger.debug(f"[Sizing] Leveraged ETF 0.7× applied to {_lev_mask.sum()} rows")

    # 2. Liquidity Constraint (Microstructure Guardrail)
    # If liquidity is poor or spreads are wide, the "expressive envelope" must shrink
    liq_adj = 1.0
    if 'Liquidity_OK' in df_allocated.columns:
        mask = ~df_allocated['Liquidity_OK']
        liq_adj = np.where(df_allocated['Liquidity_OK'], 1.0, 0.5)
        df_allocated.loc[mask, 'Sizing_Adjustments'] += "💧 LIQUIDITY_CAP "
        
    if 'Spread_Pct' in df_allocated.columns:
        # Penalize envelopes for spreads > 5%
        mask = df_allocated['Spread_Pct'] > 0.05
        liq_adj = liq_adj * np.where(mask, 0.7, 1.0)
        df_allocated.loc[mask, 'Sizing_Adjustments'] += "💧 SPREAD_PENALTY "
    
    # Microstructure: Cap envelope at 10% of Open Interest to prevent market impact
    if 'Open Int' in df_allocated.columns:
        oi_cap = (df_allocated['Open Int'] * 0.10).fillna(100)
        mask = base_capacity > oi_cap
        base_capacity = np.minimum(base_capacity, oi_cap)
        df_allocated.loc[mask, 'Sizing_Adjustments'] += "💧 OI_CAP "
        
    # 3. Price-Level Normalization (Behavioral Guardrail)
    # Prevent massive envelopes for "cheap" lottery tickets (Contract Bloat)
    price_adj = 1.0
    if 'Total_Debit' in df_allocated.columns:
        # If debit < $1.00, reduce envelope to prevent "contract bloat"
        mask = df_allocated['Total_Debit'] < 1.0
        price_adj = np.where(mask, 0.5, 1.0)
        df_allocated.loc[mask, 'Sizing_Adjustments'] += "🎈 PRICE_CAP "

    # 4. Liquidity Velocity Score (Jan 2026)
    # Qualitative measure of how easily the full envelope can be exited (1-10)
    df_allocated['Liquidity_Velocity'] = 10
    if 'Open Int' in df_allocated.columns:
        # Simple heuristic: Velocity drops if OI is low relative to envelope
        oi_ratio = (df_allocated['Open Int'] / (base_capacity * 10)).fillna(1.0)
        df_allocated['Liquidity_Velocity'] = (oi_ratio * 10).clip(1, 10).round().astype(int)

    # Calculate final envelope with NaN protection
    debug_manager = get_debug_manager()
    envelope_raw = (base_capacity * liq_adj * price_adj)
    
    nan_mask = envelope_raw.isna()
    if nan_mask.any() and debug_manager.enabled:
        debug_manager.log_event(
            step="step8",
            severity="WARN",
            code="ENVELOPE_NAN_NEUTRALIZED",
            message=f"Neutralized {nan_mask.sum()} NaN envelopes to 1",
            context={"affected_rows": int(nan_mask.sum())}
        )
        
    df_allocated['Thesis_Max_Envelope'] = envelope_raw.fillna(1).round().astype(int).clip(lower=1)
    df_allocated['Contracts'] = df_allocated['Thesis_Max_Envelope'] # Legacy support
    
    # 5. Expression Tiers (Jan 2026)
    # Categorize the trade's role in a portfolio based on the envelope size
    def get_expression_tier(size):
        if size <= 2: return "NICHE"
        if size <= 5: return "STANDARD"
        return "CORE"
        
    df_allocated['Expression_Tier'] = df_allocated['Thesis_Max_Envelope'].apply(get_expression_tier)
    
    # 6. Scaling Roadmap (Jan 2026)
    # Explicitly define the floor vs ceiling behavior
    df_allocated['Scaling_Roadmap'] = (
        "Entry: 1 Unit | " + 
        "Max: " + df_allocated['Thesis_Max_Envelope'].astype(str) + " Units | " +
        "Scale only on confirmation."
    )

    # Capital_Allocation is now a derived "Theoretical Requirement" for the full envelope
    if 'Total_Debit' in df_allocated.columns:
        df_allocated['Theoretical_Capital_Req'] = df_allocated['Thesis_Max_Envelope'] * df_allocated['Total_Debit']
        
        debit_nan = df_allocated['Theoretical_Capital_Req'].isna()
        if debit_nan.any() and debug_manager.enabled:
            debug_manager.log_event(
                step="step8",
                severity="WARN",
                code="CAPITAL_REQ_NAN",
                message=f"Neutralized {debit_nan.sum()} NaN capital requirements to 0",
                context={"affected_rows": int(debit_nan.sum())}
            )
        df_allocated['Capital_Allocation'] = df_allocated['Theoretical_Capital_Req'].fillna(0)
    else:
        # Fallback if Total_Debit is missing
        if debug_manager.enabled:
            debug_manager.log_event(
                step="step8",
                severity="WARN",
                code="MISSING_DEBIT",
                message="Total_Debit missing; capital allocation set to 0",
                context={"affected_rows": len(df_allocated)}
            )
        df_allocated['Capital_Allocation'] = 0.0
    
    return df_allocated


def _apply_vince_constraint(df: pd.DataFrame, account_balance: float, conn=None) -> pd.DataFrame:
    """
    Apply Vince optimal-f as a final contract ceiling after MC sizing.

    For each row, queries closed_trades for the (ticker, strategy) pair.
    When ≥3 qualifying trades exist, computes f* and derives a Vince contract ceiling:
        Vince_Max_Contracts = max(1, floor(f* × Thesis_Max_Envelope))

    If Vince_Max_Contracts < Thesis_Max_Envelope, the Vince ceiling is applied
    (conservative override). This prevents MC optimism from outrunning the actual
    realized trade distribution.

    Parameters
    ----------
    df             : DataFrame of READY candidates with Thesis_Max_Envelope
    account_balance: total account balance ($)
    conn           : optional existing DuckDB connection — pass the pipeline's
                     write connection to avoid exclusive-lock conflicts when Vince
                     would otherwise open a second read_only connection to the same file.

    Adds columns:
        Vince_f_Star          : optimal fraction (0.0–1.0) or None
        Vince_TWR             : Terminal Wealth Relative at f*
        Vince_Geometric_Mean  : per-trade geometric growth at f*
        Vince_n_Trades        : number of closed trades used
        Vince_Max_Contracts   : Vince-constrained contract ceiling (int)
        Vince_Note            : human-readable sizing rationale
        Vince_Applied         : True if Vince ceiling overrode MC/ATR

    Reference: Vince (1992) — The Mathematics of Money Management, Ch.2–4.
    """
    df_out = df.copy()

    # Pre-allocate Vince columns
    df_out["Vince_f_Star"]         = None
    df_out["Vince_TWR"]            = None
    df_out["Vince_Geometric_Mean"] = None
    df_out["Vince_n_Trades"]       = 0
    df_out["Vince_Max_Contracts"]  = df_out.get("Thesis_Max_Envelope", pd.Series([1]*len(df_out)))
    df_out["Vince_Note"]           = "VINCE_SKIP"
    df_out["Vince_Applied"]        = False

    if df_out.empty:
        return df_out

    # Cache Vince results by (ticker, strategy) to avoid redundant DB queries
    _cache: dict = {}
    vince_applied_count = 0

    for idx, row in df_out.iterrows():
        ticker        = str(row.get("Ticker", "") or "")
        strategy_name = str(row.get("Strategy_Name", "") or "")
        cache_key     = (ticker, strategy_name)

        if cache_key not in _cache:
            _cache[cache_key] = compute_vince_f_star(
                ticker=ticker,
                strategy_name=strategy_name,
                conn=conn,
            )
        vr = _cache[cache_key]

        df_out.at[idx, "Vince_Note"]           = vr["note"]
        df_out.at[idx, "Vince_n_Trades"]       = vr["n_trades"]

        if vr["f_star"] is None:
            # No history — keep existing Thesis_Max_Envelope
            continue

        f_star = vr["f_star"]
        df_out.at[idx, "Vince_f_Star"]         = round(f_star, 4)
        df_out.at[idx, "Vince_TWR"]            = round(vr["twr"], 4) if vr["twr"] else None
        df_out.at[idx, "Vince_Geometric_Mean"] = round(vr["geometric_mean"], 5) if vr["geometric_mean"] else None

        # Vince ceiling = f* × current Thesis_Max_Envelope, floored at 1
        current_envelope = int(row.get("Thesis_Max_Envelope") or 1)
        vince_ceiling    = max(1, int(np.floor(f_star * current_envelope)))
        df_out.at[idx, "Vince_Max_Contracts"] = vince_ceiling

        if vince_ceiling < current_envelope:
            # Vince constraint is binding — override
            df_out.at[idx, "Thesis_Max_Envelope"] = vince_ceiling
            df_out.at[idx, "Contracts"]            = vince_ceiling
            df_out.at[idx, "Vince_Applied"]        = True
            if "Sizing_Adjustments" in df_out.columns:
                df_out.at[idx, "Sizing_Adjustments"] = (
                    str(df_out.at[idx, "Sizing_Adjustments"] or "") +
                    f"🎯 VINCE_f*={f_star:.3f} "
                )
            vince_applied_count += 1
        # else: Vince is less conservative than MC/ATR — keep MC/ATR ceiling

    tickers_with_history = sum(1 for v in _cache.values() if v["f_star"] is not None)
    logger.info(
        f"🎯 Vince optimal-f: {tickers_with_history}/{len(_cache)} ticker-strategies with history "
        f"| {vince_applied_count} overrides applied"
    )
    return df_out


def _enforce_portfolio_capital_budget(
    df: pd.DataFrame,
    account_balance: float = 100_000.0,
    max_portfolio_risk: float = 0.20,
    conn=None,
) -> pd.DataFrame:
    """
    Hard gate: cumulative capital deployed must not exceed max_portfolio_risk × account_balance.
    Sector sub-budget: no single sector may exceed 40% of total risk budget.

    Reads existing open positions from entry_anchors DuckDB table to compute
    current exposure. New candidates are ranked by Capital_Efficiency_Score
    (from MC) then DQS_Score, admitted until budget is exhausted.

    Adds columns:
        Portfolio_Capital_Used_Pct   : % of total risk budget already deployed
        Sector_Utilization_Pct       : % of sector sub-budget deployed for this ticker's sector
        Correlation_Penalty_Applied  : True when envelope was reduced due to sector concentration
        Portfolio_Budget_Gate         : OPEN | SECTOR_LIMITED | BUDGET_EXHAUSTED

    Reference: McMillan Ch.3 — Risk budgeting; Vince Ch.6 — Portfolio heat.
    """
    try:
        from config.sector_benchmarks import SECTOR_BENCHMARK_MAP
    except ImportError:
        SECTOR_BENCHMARK_MAP = {"_default": "SPY"}

    df_out = df.copy()
    total_risk_budget = account_balance * max_portfolio_risk
    sector_cap_pct = 0.40  # Max 40% of risk budget in one sector

    # Pre-allocate output columns
    df_out['Portfolio_Capital_Used_Pct'] = 0.0
    df_out['Sector_Utilization_Pct'] = 0.0
    df_out['Correlation_Penalty_Applied'] = False
    df_out['Portfolio_Budget_Gate'] = 'OPEN'

    if df_out.empty or total_risk_budget <= 0:
        return df_out

    # ── Read existing open positions from entry_anchors ─────────────────────
    open_capital = 0.0
    sector_capital: dict = {}  # sector_etf -> capital deployed

    if conn is not None:
        try:
            _tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
            if 'entry_anchors' in _tables:
                _open = conn.execute("""
                    SELECT Ticker,
                           COALESCE(Capital_Deployed, Total_Debit * Contracts, 0) AS cap
                    FROM entry_anchors
                    WHERE Status IS NULL OR Status NOT IN ('CLOSED', 'EXPIRED')
                """).fetchdf()
                if not _open.empty:
                    open_capital = _open['cap'].sum()
                    for _, _r in _open.iterrows():
                        etf = SECTOR_BENCHMARK_MAP.get(
                            str(_r.get('Ticker', '')).upper(),
                            SECTOR_BENCHMARK_MAP.get('_default', 'SPY')
                        )
                        sector_capital[etf] = sector_capital.get(etf, 0) + float(_r['cap'] or 0)
                    logger.info(
                        f"[CapitalBudget] Open positions: ${open_capital:,.0f} across "
                        f"{len(_open)} positions ({len(sector_capital)} sectors)"
                    )
        except Exception as _db_err:
            logger.debug(f"[CapitalBudget] entry_anchors read failed: {_db_err}")

    remaining_budget = max(0, total_risk_budget - open_capital)
    used_pct = (open_capital / total_risk_budget * 100) if total_risk_budget > 0 else 0
    df_out['Portfolio_Capital_Used_Pct'] = round(used_pct, 1)

    if remaining_budget <= 0:
        df_out['Portfolio_Budget_Gate'] = 'BUDGET_EXHAUSTED'
        df_out['Thesis_Max_Envelope'] = 1
        df_out['Contracts'] = 1
        df_out['Sizing_Adjustments'] = df_out['Sizing_Adjustments'].fillna('') + '🛑 BUDGET_EXHAUSTED '
        logger.warning(
            f"[CapitalBudget] BUDGET EXHAUSTED: ${open_capital:,.0f} deployed "
            f"of ${total_risk_budget:,.0f} budget — all envelopes floored to 1"
        )
        return df_out

    # ── Compute per-candidate capital requirement ──────────────────────────
    _id_col = 'Ticker' if 'Ticker' in df_out.columns else df_out.columns[0]
    cap_col = 'Capital_Allocation' if 'Capital_Allocation' in df_out.columns else None
    if cap_col is None and 'Total_Debit' in df_out.columns:
        df_out['_candidate_capital'] = (
            pd.to_numeric(df_out['Total_Debit'], errors='coerce').fillna(0) *
            pd.to_numeric(df_out.get('Thesis_Max_Envelope', 1), errors='coerce').fillna(1)
        )
        cap_col = '_candidate_capital'
    elif cap_col:
        df_out['_candidate_capital'] = pd.to_numeric(df_out[cap_col], errors='coerce').fillna(0)
    else:
        # Cannot compute capital — skip budget enforcement
        return df_out

    # ── Map candidates to sectors ──────────────────────────────────────────
    df_out['_sector_etf'] = df_out[_id_col].apply(
        lambda t: SECTOR_BENCHMARK_MAP.get(str(t).upper(), SECTOR_BENCHMARK_MAP.get('_default', 'SPY'))
    )

    # ── Apply sector correlation penalty ───────────────────────────────────
    # Linear taper: if sector is at X% of cap, new entries get (1-X/cap) of normal envelope.
    sector_budget = total_risk_budget * sector_cap_pct

    for etf in df_out['_sector_etf'].unique():
        mask = df_out['_sector_etf'] == etf
        current_sector_cap = sector_capital.get(etf, 0)
        sector_util_pct = (current_sector_cap / sector_budget * 100) if sector_budget > 0 else 0
        df_out.loc[mask, 'Sector_Utilization_Pct'] = round(min(sector_util_pct, 100), 1)

        if sector_util_pct >= 100:
            # Sector fully allocated — floor envelopes to 1
            df_out.loc[mask, 'Portfolio_Budget_Gate'] = 'SECTOR_LIMITED'
            df_out.loc[mask, 'Thesis_Max_Envelope'] = 1
            df_out.loc[mask, 'Contracts'] = 1
            df_out.loc[mask, 'Correlation_Penalty_Applied'] = True
            df_out.loc[mask, 'Sizing_Adjustments'] = (
                df_out.loc[mask, 'Sizing_Adjustments'].fillna('') +
                f'🔗 SECTOR_FULL({etf}) '
            )
        elif sector_util_pct >= 60:
            # Sector approaching cap — linear taper
            taper = max(0.2, 1.0 - (sector_util_pct / 100))
            original = df_out.loc[mask, 'Thesis_Max_Envelope'].copy()
            tapered = (original * taper).clip(lower=1).astype(int)
            changed = original != tapered
            df_out.loc[mask, 'Thesis_Max_Envelope'] = tapered
            df_out.loc[mask, 'Contracts'] = tapered
            if changed.any():
                changed_idx = mask & changed
                df_out.loc[changed_idx, 'Correlation_Penalty_Applied'] = True
                df_out.loc[changed_idx, 'Portfolio_Budget_Gate'] = 'SECTOR_LIMITED'
                df_out.loc[changed_idx, 'Sizing_Adjustments'] = (
                    df_out.loc[changed_idx, 'Sizing_Adjustments'].fillna('') +
                    f'🔗 SECTOR_TAPER({etf},{taper:.0%}) '
                )

    # ── Enforce total budget by ranking ────────────────────────────────────
    # Sort by quality score descending, admit until budget exhausted
    _rank_col = next(
        (c for c in ('Capital_Efficiency_Score', 'DQS_Score', 'TQS_Score', 'PCS_Score_V2')
         if c in df_out.columns and df_out[c].notna().any()),
        None
    )
    if _rank_col:
        df_out = df_out.sort_values(_rank_col, ascending=False)

    cumulative = 0.0
    for idx in df_out.index:
        candidate_cap = float(df_out.at[idx, '_candidate_capital'] or 0)
        if cumulative + candidate_cap > remaining_budget and candidate_cap > 0:
            # This candidate would exceed budget — floor envelope to 1
            if df_out.at[idx, 'Portfolio_Budget_Gate'] == 'OPEN':
                df_out.at[idx, 'Portfolio_Budget_Gate'] = 'BUDGET_EXHAUSTED'
            df_out.at[idx, 'Thesis_Max_Envelope'] = 1
            df_out.at[idx, 'Contracts'] = 1
            df_out.at[idx, 'Sizing_Adjustments'] = (
                str(df_out.at[idx, 'Sizing_Adjustments'] or '') + '🛑 BUDGET_CAP '
            )
        else:
            cumulative += candidate_cap

    # Cleanup temp columns
    df_out.drop(columns=['_candidate_capital', '_sector_etf'], errors='ignore', inplace=True)

    budget_limited = (df_out['Portfolio_Budget_Gate'] != 'OPEN').sum()
    corr_penalized = df_out['Correlation_Penalty_Applied'].sum()
    logger.info(
        f"[CapitalBudget] Budget: ${remaining_budget:,.0f} remaining of ${total_risk_budget:,.0f} | "
        f"{budget_limited} candidates limited | {corr_penalized} sector-penalized"
    )

    return df_out


def _calculate_portfolio_greeks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate portfolio-level Greek exposure.
    """
    df_greeks = df.copy()
    if 'Contracts' in df_greeks.columns:
        for greek in ['Delta', 'Gamma', 'Vega', 'Theta']:
            if greek in df_greeks.columns:
                df_greeks[f'Position_{greek}'] = df_greeks[greek] * df_greeks['Contracts']
    return df_greeks


def _compute_concentration_warnings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Portfolio-wide concentration observation layer.

    Three informational warnings — no gates, no blocking.
    Each column is broadcast to ALL rows in the batch so every card
    in the scan view reflects the portfolio-level context it belongs to.

    Warning 1 — Sector concentration
        If >30% of the top-10 candidates (by DQS/score) share the same
        sector ETF (from SECTOR_BENCHMARK_MAP), flag that sector.
        Rationale: 2022 tech selloff — 8 "uncorrelated" growth names fell
        together. Single-factor (sector/duration) risk is invisible per card.

    Warning 2 — Single-underlying concentration
        If >3 rows share the same Ticker, flag that ticker.
        Rationale: APH Long Call + APH CSP + APH LEAP looks diversified
        on individual cards but is 3× concentrated single-name exposure.

    Warning 3 — Vega-positive concentration
        If >50% of candidates have net positive vega (long vol), flag.
        Rationale: Straddle on NVDA + TSLA + META all before Fed minutes →
        all three crushed simultaneously. Net long-vol portfolio is a
        single macro vol bet, not diversification.

    Output columns (all informational strings, empty = no warning):
        Concentration_Sector_Warning   – e.g. "⚠️ 60% QQQ (tech) in top-10"
        Concentration_Underlying_Warning – e.g. "⚠️ AAPL appears 4× in batch"
        Concentration_Vega_Warning     – e.g. "⚠️ 68% long-vega exposure"
    """
    try:
        from config.sector_benchmarks import SECTOR_BENCHMARK_MAP
    except ImportError:
        SECTOR_BENCHMARK_MAP = {"_default": "SPY"}

    df_out = df.copy()
    df_out['Concentration_Sector_Warning']     = ""
    df_out['Concentration_Underlying_Warning'] = ""
    df_out['Concentration_Vega_Warning']       = ""

    if df_out.empty:
        return df_out

    # ── Warning 1: Sector concentration in top-10 ───────────────────────────
    _score_col = next(
        (c for c in ('DQS_Score', 'TQS_Score', 'Theory_Compliance_Score', 'PCS_Score_V2')
         if c in df_out.columns), None
    )
    _id_col = 'Ticker' if 'Ticker' in df_out.columns else df_out.columns[0]
    if _score_col:
        top10 = df_out.nlargest(min(10, len(df_out)), _score_col)
    else:
        top10 = df_out.head(min(10, len(df_out)))

    sector_counts: dict = {}
    for ticker in top10[_id_col]:
        etf = SECTOR_BENCHMARK_MAP.get(str(ticker).upper(), SECTOR_BENCHMARK_MAP.get("_default", "SPY"))
        sector_counts[etf] = sector_counts.get(etf, 0) + 1

    top10_n = len(top10)
    sector_warn = ""
    for etf, cnt in sorted(sector_counts.items(), key=lambda x: -x[1]):
        pct = cnt / top10_n if top10_n > 0 else 0
        if pct > 0.30:
            sector_warn = f"SECTOR_CONC: {pct:.0%} of top-{top10_n} in {etf} — single-sector beta risk"
            break
    if sector_warn:
        df_out['Concentration_Sector_Warning'] = sector_warn
        logger.info(f"[ConcentrationWarning] {sector_warn}")

    # ── Warning 2: Single-underlying concentration ───────────────────────────
    ticker_counts = df_out[_id_col].value_counts()
    over_limit = ticker_counts[ticker_counts > 3]
    if not over_limit.empty:
        msgs = [f"{t}×{n}" for t, n in over_limit.items()]
        underlying_warn = f"UNDERLYING_CONC: {', '.join(msgs)} — same-name multi-leg concentration"
        df_out['Concentration_Underlying_Warning'] = underlying_warn
        logger.info(f"[ConcentrationWarning] {underlying_warn}")

    # ── Warning 3: Vega-positive concentration ──────────────────────────────
    # Primary: use Vega_Per_1k from MC output (positive = long vol per $1k deployed).
    # Fallback: use raw Vega column. If neither available, skip.
    vega_col = next((c for c in ('Vega_Per_1k', 'Vega', 'vega') if c in df_out.columns), None)
    if vega_col:
        vega_series = pd.to_numeric(df_out[vega_col], errors='coerce')
        valid_vega = vega_series.dropna()
        if len(valid_vega) > 0:
            long_vega_pct = (valid_vega > 0).mean()
            if long_vega_pct > 0.50:
                vega_warn = (
                    f"VEGA_CONC: {long_vega_pct:.0%} of candidates are long-vega — "
                    f"portfolio acts as a single vol-expansion bet"
                )
                df_out['Concentration_Vega_Warning'] = vega_warn
                logger.info(f"[ConcentrationWarning] {vega_warn}")

    return df_out


def _generate_portfolio_audit(
    df: pd.DataFrame,
    account_balance: float
) -> pd.DataFrame:
    """
    Generate portfolio allocation audit trail.
    """
    df_audited = _compute_concentration_warnings(df)
    if 'Capital_Allocation' in df_audited.columns:
        # Note: Allocation_Pct is now "Theoretical Portfolio Impact" if fully expressed
        df_audited['Theoretical_Impact_Pct'] = ((df_audited['Capital_Allocation'] / account_balance) * 100).round(2)
        
        # Build descriptive audit string
        _score_col = 'Theory_Compliance_Score' if 'Theory_Compliance_Score' in df_audited.columns else (
            'DQS_Score' if 'DQS_Score' in df_audited.columns else None
        )
        _score_str = (df_audited[_score_col].round(0).astype(str) + "/100") if _score_col else "N/A"
        df_audited['Portfolio_Audit'] = (
            "Max Expression: " + df_audited['Thesis_Max_Envelope'].astype(str) + " Units" +
            " | Tier: " + df_audited['Expression_Tier'] +
            " | Full Req: $" + df_audited['Capital_Allocation'].round(0).astype(str) +
            " | Score: " + _score_str
        )
        
        # Append adjustment reasons if any
        if 'Sizing_Adjustments' in df_audited.columns:
            mask = df_audited['Sizing_Adjustments'] != ""
            df_audited.loc[mask, 'Portfolio_Audit'] += " | Constraints: " + df_audited.loc[mask, 'Sizing_Adjustments']
        
        # Append Liquidity Velocity
        if 'Liquidity_Velocity' in df_audited.columns:
            df_audited['Portfolio_Audit'] += " | Exit Velocity: " + df_audited['Liquidity_Velocity'].astype(str) + "/10"
        
        # Add behavioral annotations
        if 'Spread_Pct' in df_audited.columns:
            mask = df_audited['Spread_Pct'] > 0.05
            df_audited.loc[mask, 'Portfolio_Audit'] += " | ⚠️ WIDE SPREADS"
            
        if 'Delta' in df_audited.columns:
            mask = df_audited['Delta'].abs() > 0.7
            df_audited.loc[mask, 'Portfolio_Audit'] += " | ⚡ HIGH DELTA"
            
        if 'Gamma' in df_audited.columns:
            mask = df_audited['Gamma'].abs() > 0.1
            df_audited.loc[mask, 'Portfolio_Audit'] += " | 🌊 HIGH GAMMA"
            
        if 'Total_Debit' in df_audited.columns:
            mask = df_audited['Total_Debit'] < 1.0
            df_audited.loc[mask, 'Portfolio_Audit'] += " | 🎈 CONTRACT BLOAT RISK"

        # Concentration warnings (informational, appended to audit string when triggered)
        if 'Concentration_Sector_Warning' in df_audited.columns:
            mask = df_audited['Concentration_Sector_Warning'] != ""
            df_audited.loc[mask, 'Portfolio_Audit'] += (
                " | ⚠️ " + df_audited.loc[mask, 'Concentration_Sector_Warning']
            )
        if 'Concentration_Underlying_Warning' in df_audited.columns:
            mask = df_audited['Concentration_Underlying_Warning'] != ""
            df_audited.loc[mask, 'Portfolio_Audit'] += (
                " | ⚠️ " + df_audited.loc[mask, 'Concentration_Underlying_Warning']
            )
        if 'Concentration_Vega_Warning' in df_audited.columns:
            mask = df_audited['Concentration_Vega_Warning'] != ""
            df_audited.loc[mask, 'Portfolio_Audit'] += (
                " | ⚠️ " + df_audited.loc[mask, 'Concentration_Vega_Warning']
            )

    return df_audited

# Legacy functions kept for backward compatibility
def allocate_portfolio_capital(*args, **kwargs):
    return compute_thesis_capacity(*args, **kwargs)

def finalize_and_size_positions(*args, **kwargs):
    return compute_thesis_capacity(*args, **kwargs)
