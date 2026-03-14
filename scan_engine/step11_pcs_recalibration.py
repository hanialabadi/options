"""
Step 10: PCS Recalibration and Pre-Filter

PURPOSE:
    Apply neutral, strategy-aware pre-filter to validate structural trade quality.
    Evaluates liquidity, risk parameters, strategy-specific thresholds, and Greek alignment.
    Filters out poor-risk setups before final execution approval.

DESIGN PRINCIPLE:
    - Neutral scoring (no directional bias)
    - **STRATEGY-AWARE validation rules** (via `calculate_pcs_score_v2`)
    - Greek-based validation (Delta/Vega alignment with strategy)
    - Conservative risk filters (wide spreads, low liquidity, short DTE)
    - Outputs Pre_Filter_Status: 'Valid', 'Watch', or 'Rejected'

STRATEGY-SPECIFIC THRESHOLDS (Updated 2026-02-03):
    Spread Tolerance:
        - Directional: 10% (single-leg, tight execution)
        - Income: 12% (multi-leg, net credit tolerates wider)
        - Volatility: 15% (OTM strikes naturally wider)

    Liquidity Requirements:
        - Directional: OI≥100 (quality execution for buy-and-hold)
        - Income: OI≥100 (HIGHEST - frequent rolling/adjustments)
        - Volatility: OI≥50 (OTM strikes less liquid)

    Minimum DTE:
        - Directional: 14 days (avoid Gamma risk, thesis development)
        - Income: 5 days (weekly theta decay acceptable)
        - Volatility: 21 days (Vega needs time for IV changes)

HARD RULES (from Authoritative Contract):
    - No portfolio awareness.
    - No cross-strategy comparison.
    - Scoring is per-strategy only.

INPUTS (from Step 9B):
    - Ticker, Primary_Strategy, Trade_Bias
    - Actual_DTE, Selected_Strikes, Contract_Symbols
    - Actual_Risk_Per_Contract, Total_Debit, Total_Credit
    - Bid_Ask_Spread_Pct, Open_Interest, Liquidity_Score
    - Risk_Model, Contract_Intent, Structure_Simplified
    - Delta, Vega, Gamma (Greeks extracted from Contract_Symbols JSON)
    - Put_Call_Skew, Probability_Of_Profit (from Step 9B for vol/income strategies)

OUTPUTS (added columns):
    - Pre_Filter_Status: 'Valid', 'Watch', 'Rejected'
    - Filter_Reason: Explanation if Watch/Rejected (includes strategy-specific threshold violations)
    - PCS_Score: 0-100 quality score (strategy-aware penalties)
    - Execution_Ready: True/False (Valid + Contract_Intent promoted)
"""

import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path
from typing import Dict, Tuple

# Add project root to path for imports
from core.shared.data_contracts.config import PROJECT_ROOT
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.greek_extraction import extract_greeks_to_columns, validate_greek_extraction
from utils.pcs_scoring_v2 import calculate_pcs_score_v2, analyze_pcs_distribution
from utils.dqs_scoring import calculate_dqs_score
from utils.tqs_scoring import calculate_tqs_score
from utils.options_pricing import black_scholes_price, RISK_FREE_RATE, DAYS_IN_YEAR # Import Black-Scholes

logger = logging.getLogger(__name__)


def recalibrate_and_filter(
    df: pd.DataFrame,
    min_liquidity_score: float = 30.0,
    max_spread_pct: float = 12.0,
    min_dte: int = 5,
    strict_mode: bool = False
) -> pd.DataFrame:
    """
    Apply PCS recalibration and pre-filter to Step 9B contracts.

    Validates structural trade quality using neutral, rules-based scoring.
    Filters out poor-risk setups (wide spreads, low liquidity, weak parameters).

    Args:
        df (pd.DataFrame): Step 9B output with contract selections
        min_liquidity_score (float): Minimum acceptable liquidity score. Default 30.
        max_spread_pct (float): Maximum acceptable bid-ask spread %. Default 12% (raised 2026-02-03, was 8%).
        min_dte (int): Minimum DTE for any strategy. Default 5.
        strict_mode (bool): If True, apply stricter thresholds. Default False.
    
    Returns:
        pd.DataFrame: Original df with Pre_Filter_Status, Filter_Reason, PCS_Score, Execution_Ready
    
    Side Effects:
        - Logs summary of filter results
        - Marks simplified structures as Watch
        - Promotes valid contracts to Execution_Candidate
    
    Example:
        >>> df_contracts = fetch_and_select_contracts(df_timeframed)
        >>> df_filtered = recalibrate_and_filter(
        ...     df_contracts,
        ...     min_liquidity_score=40.0,
        ...     max_spread_pct=6.0,
        ...     strict_mode=True
        ... )
        >>> valid_trades = df_filtered[df_filtered['Pre_Filter_Status'] == 'Valid']
    """
    
    # Validate input
    required_cols = [
        'Ticker', 'Primary_Strategy', 'Actual_DTE', 'Bid_Ask_Spread_Pct',
        'Open_Interest', 'Liquidity_Score', 'Contract_Selection_Status'
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns from Step 9B: {missing}")
    
    # Add Risk_Model if missing (backward compatibility)
    if 'Risk_Model' not in df.columns:
        df['Risk_Model'] = 'Unknown'
        logger.info("⚠️ Risk_Model column missing - added default 'Unknown' values")
    
    if df.empty:
        logger.warning("⚠️ Empty DataFrame passed to Step 10")
        return df
    
    logger.info(f"🔍 Step 10: PCS Recalibration for {len(df)} contracts")
    
    # Adjust thresholds for strict mode
    if strict_mode:
        min_liquidity_score = min(min_liquidity_score * 1.5, 100.0)
        max_spread_pct = max_spread_pct * 0.75  # 12.0% → 9.0% (tightened 2026-02-03, was 0.7)
        min_dte = min_dte + 2
        logger.info(f"⚡ Strict mode enabled: liquidity≥{min_liquidity_score:.1f}, spread≤{max_spread_pct:.1f}%, DTE≥{min_dte}")
    
    df = df.copy()
    
    # ========================================
    # PHASE 1: EXTRACT GREEKS FROM JSON
    # ========================================
    logger.info("📊 Phase 1: Extracting Greeks from Contract_Symbols JSON...")
    try:
        df = extract_greeks_to_columns(df)
        validation = validate_greek_extraction(df)
        logger.info(f"   ✅ Greek extraction complete")
        logger.info(f"      Coverage: {validation['delta_coverage']}")
        logger.info(f"      Quality: {validation['quality']}")
    except Exception as e:
        logger.warning(f"   ⚠️  Greek extraction failed: {e}")
        logger.warning(f"      Continuing without Greeks (reduced PCS accuracy)")
    
    # ========================================
    # PHASE 2: CALCULATE THEORETICAL PRICE & PREMIUM METRICS
    # ========================================
    logger.info("💰 Phase 2: Calculating theoretical option prices and premium metrics...")
    
    # Ensure required columns for Black-Scholes are present
    bs_required_cols = ['last_price', 'Selected_Strike', 'Actual_DTE', 'Implied_Volatility', 'Option_Type']
    for col in bs_required_cols:
        if col not in df.columns:
            df[col] = np.nan # Add missing columns as NaN to prevent errors
            logger.warning(f"⚠️ Missing Black-Scholes input column: {col}. Theoretical prices will be NaN.")

    df['Theoretical_Price'] = np.nan
    df['Premium_vs_FairValue_Pct'] = np.nan
    df['Entry_Band_Lower'] = np.nan
    df['Entry_Band_Upper'] = np.nan

    for idx, row in df.iterrows():
        try:
            S = row['last_price']
            K = row['Selected_Strike']
            T = row['Actual_DTE'] / DAYS_IN_YEAR if pd.notna(row['Actual_DTE']) else np.nan
            sigma = row['Implied_Volatility'] / 100 if pd.notna(row['Implied_Volatility']) else np.nan # Convert % to decimal
            option_type = row['Option_Type'].lower() if pd.notna(row['Option_Type']) else None

            if all(pd.notna([S, K, T, sigma])) and option_type in ['call', 'put']:
                theoretical_price = black_scholes_price(S, K, T, RISK_FREE_RATE, sigma, option_type)
                df.at[idx, 'Theoretical_Price'] = theoretical_price

                # Mid_Price is the column name from step9b/step10
                market_mid_price = row.get('Mid_Price') if pd.notna(row.get('Mid_Price')) else row.get('Mid')
                if pd.notna(market_mid_price) and theoretical_price > 0:
                    premium_vs_fair_value_pct = ((market_mid_price - theoretical_price) / theoretical_price) * 100
                    df.at[idx, 'Premium_vs_FairValue_Pct'] = premium_vs_fair_value_pct

                    # Entry bands: buying strategies pay <= fair value, selling strategies receive >= fair value
                    strategy_name = row.get('Strategy_Name', '').lower()
                    strategy_type = row.get('Strategy_Type', '').upper()
                    is_income = strategy_type == 'INCOME' or any(k in strategy_name for k in ['credit', 'secured', 'buy-write', 'covered call', 'naked put', 'iron condor'])
                    if is_income:
                        # Selling premium: want to receive at or above fair value
                        df.at[idx, 'Entry_Band_Lower'] = theoretical_price * 1.005  # 0.5% above fair value min
                        df.at[idx, 'Entry_Band_Upper'] = theoretical_price * 1.02   # 2% above fair value max
                    else:
                        # Buying premium (directional long calls/puts): want to pay at or below fair value
                        df.at[idx, 'Entry_Band_Lower'] = theoretical_price * 0.98   # 2% below fair value
                        df.at[idx, 'Entry_Band_Upper'] = theoretical_price * 1.005  # 0.5% above fair value (don't overpay)
            
        except Exception as e:
            logger.warning(f"⚠️ Failed to calculate theoretical price for {row.get('Ticker')} {row.get('Strategy_Name')}: {e}")
            # NaNs will remain for this row

    logger.info(f"   ✅ Theoretical prices and premium metrics calculated for {len(df[df['Theoretical_Price'].notna()])} contracts.")

    df['Execution_Ready'] = False

    # DQS: Directional Quality Score (Long Call / Long Put / LEAP variants)
    # MUST run BEFORE PCS early-return — DQS does not depend on IV maturity.
    # Directional trades get scored regardless of IV history length.
    try:
        df = calculate_dqs_score(df)
        dqs_rows = df['DQS_Score'].notna().sum()
        if dqs_rows > 0:
            dqs_mean = df['DQS_Score'].dropna().mean()
            strong   = (df['DQS_Status'] == 'Strong').sum()
            eligible = (df['DQS_Status'] == 'Eligible').sum()
            weak     = (df['DQS_Status'] == 'Weak').sum()
            logger.info(f"   ✅ DQS scoring: {dqs_rows} directional rows | mean {dqs_mean:.1f} | Strong {strong} / Eligible {eligible} / Weak {weak}")
    except Exception as e:
        logger.warning(f"   ⚠️ DQS scoring failed: {e} — skipping")

    # TQS: Timing Quality Score — orthogonal to DQS.
    # DQS = "Is this direction correct?" (structural edge)
    # TQS = "Is this the right moment to enter?" (tactical timing)
    # A trade with DQS=93 and TQS=30 = strong thesis, terrible timing (overextended).
    # Does NOT affect gating — surfaced as information for the trader.
    try:
        df = calculate_tqs_score(df)
        tqs_rows = df['TQS_Score'].notna().sum()
        if tqs_rows > 0:
            tqs_mean  = df['TQS_Score'].dropna().mean()
            ideal     = (df['TQS_Band'] == 'Ideal').sum()
            acceptable = (df['TQS_Band'] == 'Acceptable').sum()
            stretched = (df['TQS_Band'] == 'Stretched').sum()
            chase     = (df['TQS_Band'] == 'Chase').sum()
            logger.info(
                f"   ✅ TQS scoring: {tqs_rows} directional rows | mean {tqs_mean:.1f} | "
                f"Ideal {ideal} / Acceptable {acceptable} / Stretched {stretched} / Chase {chase}"
            )
    except Exception as e:
        logger.warning(f"   ⚠️ TQS scoring failed: {e} — skipping")

    # ========================================
    # PHASE 4: CALCULATE PCS SCORES V2
    # ========================================
    logger.info("📈 Phase 4: Calculating enhanced PCS scores...")
    try:
        # Log IV maturity distribution (informational only — no hard gate)
        # Graduated penalties in _calculate_history_penalties handle quality discount:
        #   Level 1 (<20d): -15 pts | Level 2 (20-60d): -10 pts | Level 3 (60-120d): -5 pts | Level 4+: 0
        if 'IV_Maturity_State' in df.columns:
            mat_counts = df['IV_Maturity_State'].value_counts().to_dict()
            logger.info(f"   IV maturity distribution: {mat_counts}")

        df = calculate_pcs_score_v2(df)

        analysis = analyze_pcs_distribution(df)
        logger.info(f"   ✅ PCS scoring complete")
        logger.info(f"      Mean score: {analysis.get('mean_score', 0):.1f}")
        logger.info(f"      Distribution: {analysis.get('valid_pct', '0%')} Valid, {analysis.get('watch_pct', '0%')} Watch, {analysis.get('rejected_pct', '0%')} Rejected")
    except Exception as e:
        logger.warning(f"   ⚠️  PCS V2 scoring failed: {e}")
        logger.warning(f"      Falling back to legacy scoring...")
        # Fall back to old method
        df['Pre_Filter_Status'] = 'Pending'
        df['Filter_Reason'] = ''
        df['PCS_Score'] = 0.0
    
    # Map PCS_Status to Pre_Filter_Status for compatibility
    if 'PCS_Status' in df.columns:
        df['Pre_Filter_Status'] = df['PCS_Status']
        # Keep both PCS_Score_V2 and legacy PCS_Score
        if 'PCS_Score_V2' in df.columns:
            df['PCS_Score'] = df['PCS_Score_V2']
    else:
        # Legacy mode
        df['Pre_Filter_Status'] = 'Pending'
        df['Filter_Reason'] = ''
        df['PCS_Score'] = 0.0
    
    df['Execution_Ready'] = False

    # ========================================
    # PHASE 4B: STRATEGY INTERPRETER SCORING
    # ========================================
    # Runs ALONGSIDE existing DQS/TQS/PCS — does not replace them.
    # Each strategy family gets a transparent component-by-component score.
    # Adds: Interp_Score, Interp_Max, Interp_Status, Interp_Breakdown, Interp_Family
    try:
        from scan_engine.interpreters import get_interpreter
        df['Interp_Score'] = np.nan
        df['Interp_Max'] = np.nan
        df['Interp_Status'] = ''
        df['Interp_Breakdown'] = ''
        df['Interp_Family'] = ''
        df['Interp_Vol_Edge'] = ''
        df['Interp_Interpretation'] = ''

        _interp_count = 0
        for idx, row in df.iterrows():
            strategy = str(row.get('Strategy_Name', row.get('Strategy', '')) or '')
            if not strategy:
                continue
            interp = get_interpreter(strategy)
            result = interp.score(row)
            vol_ctx = interp.interpret_volatility(row)

            df.at[idx, 'Interp_Score'] = result.score
            df.at[idx, 'Interp_Max'] = result.max_possible
            df.at[idx, 'Interp_Status'] = result.status
            df.at[idx, 'Interp_Breakdown'] = result.to_breakdown_str()
            df.at[idx, 'Interp_Family'] = interp.family
            df.at[idx, 'Interp_Vol_Edge'] = vol_ctx.edge_direction
            df.at[idx, 'Interp_Interpretation'] = result.interpretation
            _interp_count += 1

        if _interp_count > 0:
            _mean = df['Interp_Score'].dropna().mean()
            _max_mean = df['Interp_Max'].dropna().mean()
            _pct = (_mean / _max_mean * 100) if _max_mean > 0 else 0
            logger.info(
                f"   ✅ Interpreter scoring: {_interp_count} rows | "
                f"mean {_mean:.1f}/{_max_mean:.0f} ({_pct:.0f}%) | "
                f"families: {df['Interp_Family'].value_counts().to_dict()}"
            )
    except Exception as e:
        logger.warning(f"   ⚠️ Interpreter scoring failed (non-blocking): {e}")

    # ========================================
    # PHASE 5: LEGACY VALIDATION (IF NEEDED)
    # ========================================
    # Only apply legacy validation if PCS V2 failed
    if 'PCS_Status' not in df.columns:
        logger.info("⚙️  Phase 5: Applying legacy validation rules...")
        for idx, row in df.iterrows():
            # Skip failed contract selections
            if row['Contract_Selection_Status'] != 'Success':
                df.at[idx, 'Pre_Filter_Status'] = 'Rejected'
                df.at[idx, 'Filter_Reason'] = f"Contract selection failed: {row['Contract_Selection_Status']}"
                continue
            
            # Apply validation rules
            status, reason, score = _apply_validation_rules(
                row,
                min_liquidity_score=min_liquidity_score,
                max_spread_pct=max_spread_pct,
                min_dte=min_dte
            )
            
            df.at[idx, 'Pre_Filter_Status'] = status
            df.at[idx, 'Filter_Reason'] = reason
            df.at[idx, 'PCS_Score'] = score
    
    # ========================================
    # PHASE 6: PROMOTE TO EXECUTION
    # ========================================
    logger.info("🚀 Phase 6: Promoting valid contracts to execution...")
    for idx, row in df.iterrows():
        status = row.get('Pre_Filter_Status')
        # Promote valid contracts to execution candidate
        if status == 'Valid' and row.get('Contract_Intent') == 'Scan':
            df.at[idx, 'Contract_Intent'] = 'Execution_Candidate'
            df.at[idx, 'Execution_Ready'] = True
    
    # Log summary
    _log_filter_summary(df)
    
    return df


def _finalize_step10(df: pd.DataFrame) -> pd.DataFrame:
    """Helper to finalize Step 10 columns and promotion logic."""
    if 'PCS_Status' in df.columns:
        df['Pre_Filter_Status'] = df['PCS_Status']
        if 'PCS_Score_V2' in df.columns:
            df['PCS_Score'] = df['PCS_Score_V2']
    
    df['Execution_Ready'] = False
    for idx, row in df.iterrows():
        status = row.get('Pre_Filter_Status')
        if status == 'Valid' and row.get('Contract_Intent') == 'Scan':
            df.at[idx, 'Contract_Intent'] = 'Execution_Candidate'
            df.at[idx, 'Execution_Ready'] = True
            
    _log_filter_summary(df)
    return df


# Removed legacy _apply_validation_rules, _validate_greek_alignment, _validate_strategy_specific functions.
# These are replaced by calculate_pcs_score_v2.


def _log_filter_summary(df: pd.DataFrame):
    """Log summary of PCS filter results."""
    
    status_counts = df['Pre_Filter_Status'].value_counts().to_dict()
    total = len(df)
    
    valid_count = status_counts.get('Valid', 0)
    watch_count = status_counts.get('Watch', 0)
    rejected_count = status_counts.get('Rejected', 0)
    
    logger.info(f"\n📊 Step 10 PCS Filter Summary:")
    logger.info(f"   ✅ Valid: {valid_count}/{total} ({valid_count/total*100:.1f}%)")
    logger.info(f"   ⚠️  Watch: {watch_count}/{total} ({watch_count/total*100:.1f}%)")
    logger.info(f"   ❌ Rejected: {rejected_count}/{total} ({rejected_count/total*100:.1f}%)")
    
    # Log average PCS score by status
    if valid_count > 0:
        avg_valid_score = df[df['Pre_Filter_Status'] == 'Valid']['PCS_Score'].mean()
        logger.info(f"   Avg Valid PCS Score: {avg_valid_score:.1f}")
    
    # Log top rejection reasons
    if rejected_count > 0:
        rejection_reasons = df[df['Pre_Filter_Status'] == 'Rejected']['Filter_Reason'].value_counts().head(3)
        logger.info(f"\n   Top Rejection Reasons:")
        for reason, count in rejection_reasons.items():
            logger.info(f"     • {reason}: {count}")
    
    # Log premium metrics summary
    if 'Premium_vs_FairValue_Pct' in df.columns and not df['Premium_vs_FairValue_Pct'].empty:
        avg_premium_diff = df['Premium_vs_FairValue_Pct'].mean()
        median_premium_diff = df['Premium_vs_FairValue_Pct'].median()
        
        logger.info(f"\n💰 Premium vs. Fair Value Summary:")
        logger.info(f"   Avg Premium vs. Fair Value: {avg_premium_diff:.2f}%")
        logger.info(f"   Median Premium vs. Fair Value: {median_premium_diff:.2f}%")
        
        overpriced_count = (df['Premium_vs_FairValue_Pct'] > 1.0).sum() # >1% over fair value
        underpriced_count = (df['Premium_vs_FairValue_Pct'] < -1.0).sum() # >1% under fair value
        
        logger.info(f"   Overpriced (>1%): {overpriced_count} contracts")
        logger.info(f"   Underpriced (<-1%): {underpriced_count} contracts")
