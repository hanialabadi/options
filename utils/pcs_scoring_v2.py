"""
Enhanced PCS Scoring (Version 2)

Strategy-aware scoring with gradient penalties instead of binary pass/fail.

Key improvements:
1. Strategy-aware Greek validation (directional needs Delta, volatility needs Vega)
2. Gradient penalties (not binary) - wider spread = more penalty, not rejection
3. Detailed penalty breakdown in Filter_Reason
4. Status classification: Valid (80-100), Watch (50-79), Rejected (<50)

Usage:
    from utils.pcs_scoring_v2 import calculate_pcs_score_v2
    
    df = calculate_pcs_score_v2(df)
    # Now df has PCS_Score_V2 and detailed Filter_Reason
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional


# Strategy categories
DIRECTIONAL_STRATEGIES = [
    'Long Call', 'Long Put', 'Short Call', 'Short Put',
    'Bull Call Spread', 'Bear Put Spread', 'Bull Put Spread', 'Bear Call Spread'
]

VOLATILITY_STRATEGIES = [
    'Long Straddle', 'Long Strangle', 'Short Straddle', 'Short Strangle',
    'Long Butterfly', 'Long Condor'
]

INCOME_STRATEGIES = [
    'Covered Call', 'Cash-Secured Put', 'Covered Strangle',
    'Short Iron Condor', 'Short Butterfly',
    'Buy-Write',  # Stock purchase + short call package (Cohen Ch.7)
]

# Buy-Write is income but capital-intensive — tracked separately for capital checks
BUY_WRITE_STRATEGIES = ['Buy-Write']


def calculate_pcs_score_v2(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate enhanced PCS score with strategy-aware validation.
    
    Requires columns:
        - Delta, Gamma, Vega, Theta (from extract_greeks_to_columns)
        - Liquidity_Score, Bid_Ask_Spread_Pct, Open_Interest
        - Actual_DTE, Risk_Model (or similar risk metric)
        - Strategy
    
    Adds columns:
        - PCS_Score_V2: 0-100 (gradient)
        - PCS_Status: Valid (80+), Watch (50-79), Rejected (<50)
        - PCS_Penalties: JSON with penalty breakdown
        - Filter_Reason: Human-readable explanation
    
    Args:
        df: DataFrame with Greek and liquidity columns
        
    Returns:
        DataFrame with PCS scoring columns
    """
    
    # Initialize columns
    df['PCS_Score_V2'] = 100.0  # Start at 100, subtract penalties
    df['PCS_Penalties'] = ''
    df['Filter_Reason'] = ''
    
    # Apply penalties for each row
    for idx, row in df.iterrows():
        penalties = []
        base_score = 100.0
        
        # 1. Greek validation penalties (strategy-aware)
        greek_penalty, greek_reasons = _calculate_greek_penalties(row)
        base_score -= greek_penalty
        penalties.extend(greek_reasons)
        
        # 2. Liquidity penalties (gradient)
        liquidity_penalty, liquidity_reasons = _calculate_liquidity_penalties(row)
        base_score -= liquidity_penalty
        penalties.extend(liquidity_reasons)
        
        # 3. DTE penalties
        dte_penalty, dte_reasons = _calculate_dte_penalties(row)
        base_score -= dte_penalty
        penalties.extend(dte_reasons)
        
        # 4. Risk penalties
        risk_penalty, risk_reasons = _calculate_risk_penalties(row)
        base_score -= risk_penalty
        penalties.extend(risk_reasons)
        
        # 5. History Quality penalties (Volatility Identity Card)
        history_penalty, history_reasons = _calculate_history_penalties(row)
        base_score -= history_penalty
        penalties.extend(history_reasons)

        # 6. Premium Pricing penalties (NEW - 2026-02-03)
        pricing_penalty, pricing_reasons = _calculate_premium_pricing_penalties(row)
        base_score -= pricing_penalty
        penalties.extend(pricing_reasons)

        # Clamp to [0, 100]
        final_score = max(0.0, min(100.0, base_score))
        
        # Assign to DataFrame
        df.at[idx, 'PCS_Score_V2'] = final_score
        df.at[idx, 'PCS_Penalties'] = ' | '.join(penalties) if penalties else 'None'
        
        # Generate filter reason
        if final_score >= 80:
            status = 'Valid'
            reason = 'Premium Collection Standard met'
        elif final_score >= 50:
            status = 'Watch'
            reason = f'Marginal quality ({final_score:.0f}/100): ' + (penalties[0] if penalties else 'borderline metrics')
        else:
            status = 'Rejected'
            reason = f'Below PCS threshold ({final_score:.0f}/100): ' + ', '.join(penalties[:2])
        
        df.at[idx, 'PCS_Status'] = status
        df.at[idx, 'Filter_Reason'] = reason
    
    return df


def _calculate_greek_penalties(row: pd.Series) -> Tuple[float, List[str]]:
    """
    Calculate strategy-aware Greek penalties.
    
    Directional: Need |Delta| > 0.35, Vega > 0.18
    Volatility: Need Vega > 0.25, |Delta| < 0.15
    Income: Need |Theta| > Vega (decay dominant)
    
    Returns:
        (total_penalty, list_of_reasons)
    """
    
    strategy = row.get('Strategy', '') or row.get('Strategy_Name', '')
    delta = row.get('Delta')
    vega = row.get('Vega')
    theta = row.get('Theta')
    
    penalties = []
    total_penalty = 0.0
    
    # STRICT: Missing Greeks = Watch status (cannot be Valid)
    # RAG: Natenberg - "Never trade without Greeks"
    if pd.isna(delta) or pd.isna(vega):
        if strategy in DIRECTIONAL_STRATEGIES:
            return 40.0, ['Missing Delta/Vega - Directional unvalidated (-40 pts)']
        elif strategy in VOLATILITY_STRATEGIES:
            return 35.0, ['Missing Vega - Vol strategy unvalidated (-35 pts)']
        else:
            return 25.0, ['Missing Greeks - Strategy unvalidated (-25 pts)']
    
    # Directional strategies
    if strategy in DIRECTIONAL_STRATEGIES:
        # Need meaningful delta
        abs_delta = abs(delta)
        if abs_delta < 0.35:
            penalty = (0.35 - abs_delta) * 50  # Up to 17.5 pts
            total_penalty += penalty
            penalties.append(f'Low Delta ({abs_delta:.2f} < 0.35, -{penalty:.0f} pts)')
        
        # Weak conviction check (low Delta + low Gamma)
        gamma = row.get('Gamma')
        if not pd.isna(gamma) and abs_delta < 0.30 and gamma < 0.02:
            penalty = 20.0  # Weak conviction penalty
            total_penalty += penalty
            penalties.append(f'Weak Conviction (Delta={abs_delta:.2f}, Gamma={gamma:.2f}, -{penalty:.0f} pts)')
        
        # Need some vega for adjustment potential
        if vega < 0.18:
            penalty = (0.18 - vega) * 30  # Up to 5.4 pts
            total_penalty += penalty
            penalties.append(f'Low Vega ({vega:.2f} < 0.18, -{penalty:.0f} pts)')
    
    # Volatility strategies
    elif strategy in VOLATILITY_STRATEGIES:
        # RAG: Natenberg - "Straddles require realized vol > implied vol"
        # STRICT JUSTIFICATION REQUIRED
        
        # 1. Need high vega (measure of vol sensitivity)
        if vega < 0.40:
            penalty = (0.40 - vega) * 60  # Up to 24 pts
            total_penalty += penalty
            penalties.append(f'Low Vega ({vega:.2f} < 0.40, -{penalty:.0f} pts)')
        
        # 2. Should be near delta-neutral (not directional bet)
        abs_delta = abs(delta)
        if abs_delta > 0.15:
            penalty = (abs_delta - 0.15) * 40  # Stricter than before
            total_penalty += penalty
            penalties.append(f'Directional Bias ({abs_delta:.2f} > 0.15, -{penalty:.0f} pts)')
        
        # 3. CRITICAL: Check IV justification (requires IV percentile column)
        # Without IV edge, straddle is pure speculation
        iv_rank = row.get('IV_Percentile') or row.get('IV_Rank')
        if pd.notna(iv_rank):
            # Straddle should have IV justification
            # Low IV = expensive premium with no edge
            if iv_rank < 30:  # Below 30th percentile = low IV
                penalty = (30 - iv_rank) * 1.0  # Up to 30 pts (increased from 0.5)
                total_penalty += penalty
                penalties.append(f'Low IV Edge (IV%ile={iv_rank:.0f} < 30, -{penalty:.0f} pts)')
        else:
            # RAG VIOLATION: Cannot validate vol strategy without IV context
            total_penalty += 20.0
            penalties.append('No IV context - Vol strategy unvalidated (-20 pts)')
        
        # 4. Check for event risk or catalyst (optional but recommended)
        # TODO: Implement earnings/event calendar check
        # For now, penalize generic straddles without clear catalyst
        has_catalyst = row.get('Earnings_Days_Away') or row.get('Event_Risk')
        if pd.isna(has_catalyst):
            total_penalty += 15.0
            penalties.append('No catalyst identified - Generic vol bet (-15 pts)')
    
    # Income strategies
    elif strategy in INCOME_STRATEGIES:
        # Theta should dominate (decay collection) — RAG: Cohen, "theta must dominate for income to work"
        # Graduated by theta/vega ratio: deeper deficit = heavier penalty
        if not pd.isna(theta):
            abs_theta = abs(theta)
            if abs_theta <= vega:
                ratio = abs_theta / vega if vega > 0 else 0.0
                # ratio=1.0 → barely failing (light), ratio=0.0 → theta ≈ 0 (severe)
                if ratio >= 0.75:
                    penalty = 10.0   # Borderline: theta almost covers vega
                elif ratio >= 0.50:
                    penalty = 17.0   # Moderate deficit
                else:
                    penalty = 25.0   # Severe deficit: vega dominates by 2x+
                total_penalty += penalty
                penalties.append(f'Weak Theta (θ/v={ratio:.2f}, θ={abs_theta:.2f} ≤ v={vega:.2f}, -{penalty:.0f} pts)')
    
    return total_penalty, penalties


def _calculate_liquidity_penalties(row: pd.Series) -> Tuple[float, List[str]]:
    """
    Calculate gradient liquidity penalties (STRATEGY-AWARE).

    DIRECTIONAL (single-leg): Stricter spreads (10%), higher OI requirement (100)
    INCOME (multi-leg): Baseline spreads (12%), highest OI requirement (100) for rolling
    VOLATILITY (OTM strikes): Wider spreads (15%), lower OI requirement (50)

    Rationale:
    - Directional: Simple execution, tight spreads critical
    - Income: Frequent adjustments/rolling need excellent liquidity
    - Volatility: OTM strikes naturally have wider spreads

    Updated: 2026-02-03 (Strategy Quality Audit)

    Returns:
        (total_penalty, list_of_reasons)
    """

    strategy = row.get('Strategy', '')
    spread_pct = row.get('Bid_Ask_Spread_Pct')
    oi = row.get('Open_Interest')

    penalties = []
    total_penalty = 0.0

    # Convert to float/int safely
    try:
        spread_pct = float(spread_pct) if pd.notna(spread_pct) else None
    except (ValueError, TypeError):
        spread_pct = None

    try:
        oi = int(oi) if pd.notna(oi) else None
    except (ValueError, TypeError):
        oi = None

    # Get strategy-specific thresholds
    if strategy in DIRECTIONAL_STRATEGIES:
        spread_threshold = 10.0  # Tighter spreads (single-leg)
        oi_threshold = 100       # Higher OI (quality execution)
    elif strategy in INCOME_STRATEGIES:
        spread_threshold = 12.0  # Multi-leg tolerates wider spreads
        oi_threshold = 75        # Sufficient for weekly CSPs/CCs (RAG: 75 covers weekly cycles)
    elif strategy in VOLATILITY_STRATEGIES:
        spread_threshold = 15.0  # OTM strikes naturally wider
        oi_threshold = 50        # Lower OI acceptable for OTM
    else:
        spread_threshold = 12.0  # Conservative default
        oi_threshold = 75        # Moderate default

    # GAP 7 FIX: Gamma-adjusted spread threshold.
    # Harris (Trading and Exchanges Ch.5) + Natenberg Ch.7:
    # Market maker's hedge cost scales with Gamma × realized vol. A 5% spread at high Gamma
    # (≥0.08) costs ~2× more in effective slippage than the same spread at low Gamma (<0.05).
    # Tighten spread threshold ×0.75 when Gamma is high. Only when Gamma is known.
    try:
        _gamma_raw = row.get('Gamma')
        _gamma_val = abs(float(_gamma_raw)) if _gamma_raw is not None and pd.notna(_gamma_raw) else 0.0
    except (TypeError, ValueError):
        _gamma_val = 0.0
    _gamma_adj_factor = 0.75 if _gamma_val >= 0.08 else 1.0
    _effective_spread_threshold = spread_threshold * _gamma_adj_factor

    # Spread penalty (gradient, strategy-aware, gamma-adjusted)
    if spread_pct is not None and spread_pct > _effective_spread_threshold:
        penalty = (spread_pct - _effective_spread_threshold) * 2.0  # -2 pts per % over threshold
        total_penalty += penalty
        _gamma_note = f', Gamma={_gamma_val:.3f}≥0.08→thr×0.75' if _gamma_adj_factor < 1.0 else ''
        penalties.append(f'Wide Spread ({spread_pct:.1f}% > {_effective_spread_threshold:.1f}%{_gamma_note}, -{penalty:.0f} pts)')

    # OI penalty (gradient, strategy-aware)
    if oi is not None and oi < oi_threshold:
        penalty = (oi_threshold - oi) * 0.2  # -0.2 pts per contract below threshold
        total_penalty += penalty
        penalties.append(f'Low OI ({oi:.0f} < {oi_threshold}, -{penalty:.0f} pts)')

    return total_penalty, penalties


def _calculate_dte_penalties(row: pd.Series) -> Tuple[float, List[str]]:
    """
    Calculate DTE penalties (STRATEGY-AWARE).

    DIRECTIONAL: Min 14 days (avoid extreme Gamma risk, need time for thesis)
    INCOME: Min 5 days (weekly theta decay acceptable)
    VOLATILITY: Min 21 days (Vega needs time for IV changes)

    Rationale:
    - Directional: <7 DTE = Gamma explosion risk (Natenberg)
    - Income: Weekly CSP/CC strategies acceptable (Cohen)
    - Volatility: IV changes need time to materialize (Sinclair)

    Updated: 2026-02-03 (Strategy Quality Audit)

    Returns:
        (total_penalty, list_of_reasons)
    """

    strategy = row.get('Strategy', '')
    dte = row.get('Actual_DTE')

    penalties = []
    total_penalty = 0.0

    # Convert to int safely
    try:
        dte = int(dte) if pd.notna(dte) else None
    except (ValueError, TypeError):
        dte = None

    if dte is None:
        return total_penalty, penalties

    # Get strategy-specific DTE thresholds
    if strategy in DIRECTIONAL_STRATEGIES:
        min_dte_critical = 14  # Avoid Gamma risk
        min_dte_moderate = 21  # Ideal for thesis development
    elif strategy in INCOME_STRATEGIES:
        min_dte_critical = 5   # Weekly theta decay OK
        min_dte_moderate = 14  # Standard monthly cycle
    elif strategy in VOLATILITY_STRATEGIES:
        min_dte_critical = 21  # Vega needs time
        min_dte_moderate = 30  # Ideal for IV changes
    else:
        min_dte_critical = 7   # Conservative default
        min_dte_moderate = 14

    # Apply gradient penalties
    if dte < min_dte_critical:
        penalty = (min_dte_critical - dte) * 3.0  # -3 pts per day below critical
        total_penalty += penalty
        penalties.append(f'Very Short DTE ({dte:.0f}d < {min_dte_critical}d, -{penalty:.0f} pts)')
    elif dte < min_dte_moderate:
        penalty = (min_dte_moderate - dte) * 1.0  # -1 pt per day below moderate
        total_penalty += penalty
        penalties.append(f'Short DTE ({dte:.0f}d < {min_dte_moderate}d, -{penalty:.0f} pts)')

    return total_penalty, penalties


def _calculate_risk_penalties(row: pd.Series) -> Tuple[float, List[str]]:
    """
    Calculate risk penalties.

    Risk > $5k: Moderate penalty (portfolio concentration)
    Risk > $10k: High penalty

    Buy-Write: Additional capital concentration check — stock purchase = large capital lock-up.
    A $500/share stock = $50k per contract. Penalize if capital_req > $20k (concentration risk).

    Returns:
        (total_penalty, list_of_reasons)
    """

    strategy = row.get('Strategy', '') or row.get('Strategy_Name', '')
    risk = row.get('Risk_Model') or row.get('Actual_Risk_Per_Contract')

    penalties = []
    total_penalty = 0.0

    # Convert to float safely (Risk_Model might be string, Actual_Risk_Per_Contract should be numeric)
    try:
        risk = float(risk) if pd.notna(risk) and str(risk).replace('.', '').replace('-', '').isdigit() else None
    except (ValueError, TypeError):
        risk = None

    if risk is not None and risk > 5000:
        penalty = (risk - 5000) / 100 * 0.5  # -0.5 pts per $100 over $5k
        total_penalty += penalty
        penalties.append(f'High Risk (${risk:,.0f}, -{penalty:.0f} pts)')

    # Buy-Write: capital concentration penalty (100 shares purchased = large single-name exposure)
    # Cohen: only worth doing if you can absorb the full stock downside
    if strategy in BUY_WRITE_STRATEGIES:
        capital_req = row.get('Capital_Requirement')
        try:
            capital_req = float(capital_req) if pd.notna(capital_req) else None
        except (ValueError, TypeError):
            capital_req = None

        if capital_req is not None:
            if capital_req > 50_000:
                penalty = 20.0  # Very high-priced stock (e.g. BKNG $5k/share)
                total_penalty += penalty
                penalties.append(f'Buy-Write capital concentration (${capital_req:,.0f}/contract, -{penalty:.0f} pts)')
            elif capital_req > 20_000:
                penalty = 10.0  # High-priced stock (e.g. NVDA $130/share is fine; $200+ gets here)
                total_penalty += penalty
                penalties.append(f'Buy-Write elevated capital (${capital_req:,.0f}/contract, -{penalty:.0f} pts)')

    return total_penalty, penalties


def _calculate_history_penalties(row: pd.Series) -> Tuple[float, List[str]]:
    """
    Calculate penalties based on IV History quality (Volatility Identity Card).

    Uses IV_Maturity_Level (1-5) from IVEngine for graduated penalties:
    - Level 1 (<20d): -15 pts (minimal history)
    - Level 2 (20-60d): -10 pts (early history)
    - Level 3 (60-120d): -5 pts (developing history)
    - Level 4-5 (120d+): no penalty (mature)

    Also penalizes stale IV data and low regime confidence.

    Returns:
        (total_penalty, list_of_reasons)
    """
    iv_data_stale = row.get('iv_data_stale', False)
    regime_confidence = row.get('regime_confidence', 1.0)
    maturity_level = row.get('IV_Maturity_Level')

    penalties = []
    total_penalty = 0.0

    if iv_data_stale:
        penalty = 15.0
        total_penalty += penalty
        penalties.append(f'Stale IV History (-{penalty:.0f} pts)')

    if regime_confidence < 0.5:
        penalty = 10.0
        total_penalty += penalty
        penalties.append(f'Low Regime Confidence ({regime_confidence:.2f}, -{penalty:.0f} pts)')

    # Graduated maturity penalty from IVEngine
    if pd.notna(maturity_level):
        maturity_level = int(maturity_level)
        if maturity_level == 1:
            penalty = 15.0
            total_penalty += penalty
            penalties.append(f'Minimal IV History (Maturity Level 1, -{penalty:.0f} pts)')
        elif maturity_level == 2:
            penalty = 10.0
            total_penalty += penalty
            penalties.append(f'Early IV History (Maturity Level 2, -{penalty:.0f} pts)')
        elif maturity_level == 3:
            penalty = 5.0
            total_penalty += penalty
            penalties.append(f'Developing IV History (Maturity Level 3, -{penalty:.0f} pts)')
        # Level 4-5: no penalty
    else:
        # No maturity data at all
        penalty = 15.0
        total_penalty += penalty
        penalties.append(f'Missing IV History (-{penalty:.0f} pts)')

    return total_penalty, penalties


def _calculate_premium_pricing_penalties(row: pd.Series) -> Tuple[float, List[str]]:
    """
    Calculate premium pricing penalties (NEW - 2026-02-03).

    STRATEGY-AWARE: Buy cheap, sell expensive
    - Directional (buy premium): Penalize if overpaying (premium vs FV >5%)
    - Income (sell premium): Penalize if underselling (premium vs FV <-5%)
    - Volatility: Penalize buying high IV Rank (>70) or selling low IV Rank (<30)

    Returns:
        (total_penalty, list_of_reasons)
    """

    strategy = row.get('Strategy', '') or row.get('Strategy_Name', '')
    premium_vs_fv = row.get('Premium_vs_FairValue_Pct', 0)
    iv_rank = row.get('IV_Rank_30D') or row.get('IV_Rank', 50)
    theta = row.get('Theta', 0)
    mid_price = row.get('Mid', 0)

    penalties = []
    total_penalty = 0.0

    # 0. Buy-Write: net premium yield check
    # Cohen: the short call must collect enough premium to justify capital lock-up.
    # Minimum acceptable yield: 1% of stock price per contract (annualised would be ~12%).
    # If mid_price (call premium) < 1% of stock price → income not worth the risk.
    if strategy in BUY_WRITE_STRATEGIES:
        capital_req = row.get('Capital_Requirement')
        try:
            capital_req = float(capital_req) if pd.notna(capital_req) else None
            mid_f = float(mid_price) if pd.notna(mid_price) else None
        except (ValueError, TypeError):
            capital_req = None
            mid_f = None

        if capital_req is not None and capital_req > 0 and mid_f is not None and mid_f > 0:
            # Use Approx_Stock_Price directly (set by Step 6) — avoids fragile reverse math.
            # Fallback: capital_req / 100 only for legacy rows that predate the column.
            _asp = row.get('Approx_Stock_Price')
            try:
                _stock_price_bw = float(_asp) if (_asp is not None and float(_asp) > 0) else capital_req / 100.0
            except (TypeError, ValueError):
                _stock_price_bw = capital_req / 100.0  # noqa: F841 (kept for future use in yield msg)
            premium_total = mid_f * 100            # option mid × 100 shares
            premium_yield_pct = premium_total / capital_req * 100  # as % of total capital

            if premium_yield_pct < 0.5:
                penalty = 20.0  # < 0.5% yield — negligible income for capital risked
                total_penalty += penalty
                penalties.append(f'Buy-Write yield too low ({premium_yield_pct:.2f}% of capital, need >1%, -{penalty:.0f} pts)')
            elif premium_yield_pct < 1.0:
                penalty = 10.0  # 0.5–1.0% yield — marginal
                total_penalty += penalty
                penalties.append(f'Buy-Write low yield ({premium_yield_pct:.2f}% of capital, ideal >1%, -{penalty:.0f} pts)')
            # >= 1% yield: no penalty — worthwhile income for capital deployed

    # 1. Premium vs Fair Value validation
    if strategy in DIRECTIONAL_STRATEGIES:
        # Buying premium - want discount (<0%), penalize premium (>5%)
        if premium_vs_fv > 5:
            penalty = (premium_vs_fv - 5) * 3.0  # -3 pts per % overpaying
            total_penalty += penalty
            penalties.append(f'Overpaying ({premium_vs_fv:+.1f}% vs fair value, -{penalty:.0f} pts)')
        elif premium_vs_fv < -5:
            # Getting discount - bonus
            bonus = min(abs(premium_vs_fv) - 5, 10) * 0.5  # Up to +5 pts bonus (subtract from penalty)
            total_penalty -= bonus  # Negative penalty = bonus
            penalties.append(f'Discount pricing ({premium_vs_fv:+.1f}% vs fair value, +{bonus:.0f} pts)')

    elif strategy in INCOME_STRATEGIES:
        # Selling premium - want premium (>5%), penalize discount (<-5%)
        if premium_vs_fv < -5:
            penalty = (abs(premium_vs_fv) - 5) * 3.0  # -3 pts per % underselling
            total_penalty += penalty
            penalties.append(f'Underselling ({premium_vs_fv:+.1f}% vs fair value, -{penalty:.0f} pts)')
        elif premium_vs_fv > 5:
            # Selling at premium - bonus
            bonus = min(premium_vs_fv - 5, 10) * 0.5  # Up to +5 pts bonus
            total_penalty -= bonus
            penalties.append(f'Premium pricing ({premium_vs_fv:+.1f}% vs fair value, +{bonus:.0f} pts)')

    # 2. IV Rank alignment (don't buy high IV, don't sell low IV)
    # RAG: Buying at IV Rank >80 = 2-sigma spike (Natenberg). Selling at <15 = cardinal sin (Cohen).
    # Steepened from 0.5x to nonlinear: penalty accelerates in the danger zone.
    if pd.notna(iv_rank):
        if strategy in DIRECTIONAL_STRATEGIES:  # Buying premium — penalize buying expensive IV
            if iv_rank > 70:
                excess = iv_rank - 70
                # Nonlinear: first 10 pts of excess = 0.5x, next 20 pts = 1.0x
                penalty = min(excess, 10) * 0.5 + max(excess - 10, 0) * 1.0  # Up to -25 pts at rank=100
                total_penalty += penalty
                penalties.append(f'Buying high IV (IV Rank {iv_rank:.0f} > 70, -{penalty:.0f} pts)')

        elif strategy in INCOME_STRATEGIES:  # Selling premium — penalize selling cheap IV
            if iv_rank < 30:
                deficit = 30 - iv_rank
                # Nonlinear: first 15 pts of deficit = 0.5x, next 15 pts = 1.0x
                penalty = min(deficit, 15) * 0.5 + max(deficit - 15, 0) * 1.0  # Up to -22.5 pts at rank=0
                total_penalty += penalty
                penalties.append(f'Selling low IV (IV Rank {iv_rank:.0f} < 30, -{penalty:.0f} pts)')

    # 3. Theta burn check (for premium buyers)
    if strategy in DIRECTIONAL_STRATEGIES:
        if abs(theta) > 0 and mid_price > 0:
            theta_pct = (abs(theta) / mid_price) * 100
            if theta_pct > 5:  # Losing >5% per day
                penalty = (theta_pct - 5) * 4.0  # -4 pts per % over 5%
                total_penalty += penalty
                penalties.append(f'High theta burn ({theta_pct:.1f}%/day, -{penalty:.0f} pts)')

    # 4. Surface Shape awareness (term structure signal)
    surface_shape = row.get('Surface_Shape')
    if pd.notna(surface_shape):
        if strategy in DIRECTIONAL_STRATEGIES and surface_shape == 'INVERTED':
            # Buying when short-term IV is elevated vs long-term — paying up for near-term fear
            penalty = 8.0
            total_penalty += penalty
            penalties.append(f'Inverted surface (short-term IV elevated, -{penalty:.0f} pts)')
        elif strategy in INCOME_STRATEGIES and surface_shape == 'CONTANGO':
            # Normal term structure favors credit selling
            bonus = 5.0
            total_penalty -= bonus
            penalties.append(f'Favorable contango for income (+{bonus:.0f} pts)')
        elif strategy in INCOME_STRATEGIES and surface_shape == 'INVERTED':
            # RAG: inverted = short-term fear spike = early buyback at loss risk for sellers
            penalty = 8.0
            total_penalty += penalty
            penalties.append(f'Inverted surface (short-term IV spike, buyback risk, -{penalty:.0f} pts)')

    # 5. IV Regime awareness
    iv_regime = row.get('IV_Regime')
    if pd.notna(iv_regime):
        if strategy in DIRECTIONAL_STRATEGIES and iv_regime == 'HIGH_VOL':
            penalty = 10.0
            total_penalty += penalty
            penalties.append(f'Buying in HIGH_VOL regime (-{penalty:.0f} pts)')
        elif strategy in INCOME_STRATEGIES and iv_regime == 'LOW_VOL':
            penalty = 10.0
            total_penalty += penalty
            penalties.append(f'Selling in LOW_VOL regime (-{penalty:.0f} pts)')

    return total_penalty, penalties


def analyze_pcs_distribution(df: pd.DataFrame) -> Dict[str, any]:
    """
    Analyze PCS score distribution and quality.
    
    Args:
        df: DataFrame with PCS_Score_V2 column
        
    Returns:
        Dictionary with distribution metrics
    """
    
    if 'PCS_Score_V2' not in df.columns:
        return {'error': 'PCS_Score_V2 column not found'}
    
    scores = df['PCS_Score_V2'].dropna()
    
    if len(scores) == 0:
        return {'error': 'No scores available'}
    
    # Status counts
    status_counts = df['PCS_Status'].value_counts().to_dict() if 'PCS_Status' in df.columns else {}
    
    # Score distribution
    return {
        'total_rows': len(df),
        'rows_with_scores': len(scores),
        'mean_score': scores.mean(),
        'median_score': scores.median(),
        'std_score': scores.std(),
        'min_score': scores.min(),
        'max_score': scores.max(),
        'status_valid': status_counts.get('Valid', 0),
        'status_watch': status_counts.get('Watch', 0),
        'status_rejected': status_counts.get('Rejected', 0),
        'valid_pct': f"{100 * status_counts.get('Valid', 0) / len(df):.1f}%",
        'watch_pct': f"{100 * status_counts.get('Watch', 0) / len(df):.1f}%",
        'rejected_pct': f"{100 * status_counts.get('Rejected', 0) / len(df):.1f}%"
    }


if __name__ == '__main__':
    # Quick test
    print("="*70)
    print("PCS SCORING V2 TEST")
    print("="*70)
    print()
    
    # Sample data with Greeks
    data = {
        'Ticker': ['AAPL'] * 5,
        'Strategy': ['Long Call', 'Long Put', 'Long Straddle', 'Long Strangle', 'Covered Call'],
        'Delta': [0.52, -0.48, 0.04, 0.03, 0.52],
        'Gamma': [0.03, 0.03, 0.06, 0.04, 0.03],
        'Vega': [0.25, 0.25, 0.50, 0.40, 0.20],
        'Theta': [-0.15, -0.15, -0.30, -0.20, -0.25],
        'Bid_Ask_Spread_Pct': [5.0, 6.0, 7.0, 12.0, 4.0],
        'Open_Interest': [1000, 800, 500, 30, 1200],
        'Actual_DTE': [45, 45, 45, 45, 30],
        'Risk_Model': [500, 500, 1000, 800, 0]
    }
    
    df = pd.DataFrame(data)
    
    print("Before PCS scoring:")
    print(df[['Strategy', 'Delta', 'Vega']].to_string(index=False))
    print()
    
    # Calculate PCS scores
    df = calculate_pcs_score_v2(df)
    
    print("After PCS scoring:")
    print(df[['Strategy', 'PCS_Score_V2', 'PCS_Status']].to_string(index=False))
    print()
    
    print("Penalties:")
    for idx, row in df.iterrows():
        print(f"{row['Strategy']:20s} | {row['PCS_Score_V2']:5.0f} | {row['PCS_Penalties']}")
    
    print()
    
    # Analysis
    analysis = analyze_pcs_distribution(df)
    print("Distribution:")
    for key, value in analysis.items():
        print(f"  {key}: {value}")
    
    print()
    print("✅ PCS scoring V2 working!")
