"""
Step 7: Strategy Recommendation Engine (Multi-Strategy Ledger Architecture)

🚨 ARCHITECTURAL CHANGE (2025-01-XX):
Moved from single-strategy-per-ticker to Strategy Ledger pattern.

**Strategy Ledger Pattern**:
- Each row = (Ticker × Strategy) pairing
- Independent validators (no if/elif chains)
- Additive logic (append all valid strategies)
- Theory-explicit (Valid_Reason + Theory_Source)

**Theory Compliance**:
- Multiple strategies can coexist for same ticker (Hull)
- Bullish ticker can have: Long Call + CSP + Buy-Write (capital/risk-dependent)
- Expansion ticker can have: Long Straddle + Long Strangle (budget-dependent)
- Strategy discovery ≠ execution filtering (Step 7 vs Step 9B)

Purpose:
    Takes validated market data from Steps 2-6 and generates
    MULTIPLE strategy recommendations per ticker (when theory allows).
    
Design:
    Independent validators ensure order-independence and theory compliance.
    No mutual exclusion - all valid strategies are discovered.
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ==========================================
# TIER 1 ENFORCEMENT
# Allowed strategies for live trading:
#   Buy-writes, Covered Calls, Long Calls, Long Puts,
#   Long Call LEAPs, Long Put LEAPs,
#   Cash-Secured Puts, Long Straddles, Long Strangles
#
# FIDELITY_DISABLED (broker does not support multi-leg spreads):
#   Call Debit Spread, Put Debit Spread
# ==========================================
TIER1_ONLY = True

# BUG 1 FIX — Entry quality gate
# Directional strategies are suppressed when entry is flagged as CHASING.
# Income and Volatility strategies are exempt (intraday extension doesn't affect their edge).
# Murphy Ch.4: "Wait for the pullback"; Bulkowski: "Chase after 5%+ = statistical losing trade"
_DIRECTIONAL_STRATEGIES = {'long call', 'long put', 'long call leap', 'long put leap',
                           'call debit spread', 'put debit spread'}

# Leveraged ETFs: LEAP strategies are structurally invalid on these products.
# Reasons:
#   1. No LEAP-tenor options (chains max at 90-180 DTE; Step 9B → NEAR_LEAP_FALLBACK).
#   2. Daily-reset beta slippage makes multi-year thesis inapplicable (Hull Ch.10 requires
#      stable underlying compounding — leveraged ETFs reset daily, breaking LEAP math).
#   3. Legitimate strategies: Long Call/Put (≤90 DTE momentum), CSP, Buy-Write (cautiously).
# ETNs excluded (counterparty risk). Only ETFs with real option chains.
_LEVERAGED_ETFS = frozenset({
    'TQQQ', 'SOXL', 'SPXL', 'UPRO', 'SSO',
    'FAS',  'LABU', 'NUGT', 'TNA',  'TECL',
    'QLD',  'DDM',  'UDOW',
})


# ==========================================
# HELPER FUNCTIONS
# ==========================================

def _calculate_approx_stock_price(row: pd.Series) -> Tuple[float, str]:
    """
    Calculate approximate stock price using Step 5 derived data.

    Returns:
        (price: float, source: str)
        source is one of: 'SMA20_PCT' | 'Close' | 'Missing'

    Priority:
        1. SMA20 × (1 + Price_vs_SMA20 / 100)  — most accurate (Step 4 derived)
        2. Close                                  — direct quote fallback
        3. 0.0 / 'Missing'                        — no price data available
    """
    sma20 = row.get('SMA20', 0)
    price_vs_sma20 = row.get('Price_vs_SMA20', 0)

    if sma20 and price_vs_sma20:
        # Price_vs_SMA20 is a percentage (e.g., +5.0 = 5% above SMA20), not a dollar offset
        return sma20 * (1 + price_vs_sma20 / 100), 'SMA20_PCT'

    close = row.get('Close')
    if close is not None and not (isinstance(close, float) and pd.isna(close)) and float(close) > 0:
        return float(close), 'Close'

    return 0.0, 'Missing'

def _get_iv_rank(row: pd.Series, default: float = 50.0) -> float:
    """
    Resolve IV Rank from available columns.

    Priority: IV_Rank_30D → IV_Rank → IV_Rank_XS → default.
    IV_Rank_XS was removed when IVEngine replaced it with suffixed columns.
    Returns `default` (50) when all null — callers should treat null rank
    as IMMATURE and apply appropriate income/directional threshold logic.
    """
    for col in ('IV_Rank_30D', 'IV_Rank_60D', 'IV_Rank', 'IV_Rank_XS'):
        val = row.get(col)
        if val is not None and not (isinstance(val, float) and np.isnan(val)):
            try:
                return float(val)
            except (TypeError, ValueError):
                continue
    return default


def _iv_rank_confidence_adjustment(iv_rank: float, iv_rank_known: bool) -> int:
    """
    Scale confidence by IV_Rank percentile for income strategies.

    Jabbour & Budwick (0.769): "IV should be taken into consideration before
    opening a position" — not just as a gate but as a conviction weight.
    Sinclair (Volatility Trading Ch.4): high IV_Rank = BEST time to sell.

    Args:
        iv_rank:      Resolved IV_Rank (0-100). Default 50 when IMMATURE.
        iv_rank_known: True if IV_Rank came from real data (not default fill).

    Returns:
        Integer adjustment to add to base confidence (-15 to +15).
        When IV_Rank is unknown (IMMATURE default), returns 0.
    """
    if not iv_rank_known:
        return 0  # IMMATURE: no data → no adjustment, no guessing
    # Linear scale from rank 50 (neutral) with ±15 cap
    # Rank 80 → +9, Rank 35 → -5, Rank 95 → +14
    raw = (iv_rank - 50) * 0.30
    return int(max(-15, min(15, raw)))


def _create_neutral_strategy(ticker: str, original_row: pd.Series) -> Dict:
    """
    Creates a default neutral strategy for a ticker when no strong signals are found.
    It copies all original row data and then populates/overrides strategy-specific columns.
    """
    # Start with all original row data
    neutral_strategy_data = original_row.to_dict()
    
    # Override/add strategy-specific columns
    neutral_strategy_data.update({
        'Ticker': ticker, # Ensure ticker is correct
        'Strategy_Name': 'Neutral / Watch',
        'Strategy_Type': 'Neutral',
        'Signal_Type': original_row.get('Signal_Type', 'Unknown'), # Preserve original signal type
        'Regime': original_row.get('Regime', 'Unknown'),           # Preserve original regime
        'Reason': 'No strong signals, awaiting clearer market conditions',
        'Confidence': 25,
        'Execution_Ready': False,
        'Strategy_Tier': 1, # Still Tier 1, but not executable
        'Valid_Reason': 'No strong signals, awaiting clearer market conditions',
        'Theory_Source': 'Default neutral strategy for data completeness',
        'Regime_Context': original_row.get('Regime', 'Unknown'),
        'IV_Context': 'Neutral / Undefined',
        'Capital_Requirement': 0,
        'Risk_Profile': 'None (monitoring)',
        'Greeks_Exposure': 'None',
        'Trade_Bias': 'Neutral',
    })
    return neutral_strategy_data

# ==========================================
# INDEPENDENT STRATEGY VALIDATORS
# (Multi-Strategy Ledger Architecture)
# ==========================================

def _validate_long_call(ticker: str, row: pd.Series) -> Optional[Dict]:
    """
    Validate Long Call strategy.
    
    Theory: Natenberg Ch.3 - Directional with positive vega.
    Entry: Bullish signal + Cheap IV (gap < 0).
    """
    stock_price, _price_source = _calculate_approx_stock_price(row)
    if stock_price == 0:
        return None # Cannot determine capital without stock price
    
    # Approximate capital for 1 contract (e.g., $5 premium)
    capital_req = 5 * 100 # $500 for a typical call option
    
    signal = row.get('Signal_Type', '')
    gap_30d = row.get('IVHV_gap_30D', 0)
    
    # Calculate longer-term gaps — only when both IV and HV are available
    iv_180 = row.get('IV_180_D_Call')
    hv_180 = row.get('HV_180_D_Cur')
    gap_180d = (float(iv_180) - float(hv_180)) if (pd.notna(iv_180) and pd.notna(hv_180)) else None

    iv_60 = row.get('IV_60_D_Call')
    hv_60 = row.get('HV_60_D_Cur')
    gap_60d = (float(iv_60) - float(hv_60)) if (pd.notna(iv_60) and pd.notna(hv_60)) else None

    # Rejection criteria — signal required; IV gap is context not a hard block.
    # Natenberg Ch.3: directional buyers benefit from cheap IV (gap < 0) but are not
    # prohibited when IV ≈ HV. The gap informs pricing edge, not trade validity.
    # Only hard-block when IV is severely expensive (gap > +15) — Sinclair: selling edge,
    # not buying edge. Unknown gap (immature IV) → allow (directional thesis is chart-driven).
    if signal not in ['Bullish', 'Sustained Bullish']:
        return None
    best_gap_lc = gap_180d if gap_180d is not None else (gap_60d if gap_60d is not None else gap_30d)
    if best_gap_lc is not None and best_gap_lc > 15:
        return None  # IV severely expensive for a buyer (Sinclair: that edge belongs to sellers)

    # Build human-readable IV context (omit gaps we don't have data for)
    iv_parts = [f"gap_30d={gap_30d:.1f}"]
    if gap_60d is not None:
        iv_parts.append(f"gap_60d={gap_60d:.1f}")
    if gap_180d is not None:
        iv_parts.append(f"gap_180d={gap_180d:.1f}")
    iv_context_str = ", ".join(iv_parts)

    best_gap = gap_180d if gap_180d is not None else (gap_60d if gap_60d is not None else gap_30d)
    best_gap_label = "gap_180d" if gap_180d is not None else ("gap_60d" if gap_60d is not None else "gap_30d")

    return {
        'Ticker': ticker,
        'Strategy_Name': 'Long Call',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Bullish signal, IV context: {best_gap_label}={best_gap:.1f}" if best_gap is not None else "Bullish signal (IV history immature)",
        'Theory_Source': 'Natenberg Ch.3 - Directional with positive vega',
        'Regime_Context': signal,
        'IV_Context': iv_context_str,
        'Capital_Requirement': capital_req,
        'Risk_Profile': 'Defined (max loss = premium paid)',
        'Greeks_Exposure': 'Long Delta, Long Vega, Short Theta',
        'Execution_Ready': True,
        'Confidence': 65,
        'Trade_Bias': 'Bullish',
        'Strategy_Type': 'Directional',
        'Approx_Stock_Price': stock_price,
        'Approx_Stock_Price_Source': _price_source,
    }


def _validate_long_put(ticker: str, row: pd.Series) -> Optional[Dict]:
    """
    Validate Long Put strategy.

    Theory: Natenberg Ch.3 - Directional with positive vega.
    Entry: Bearish signal + Cheap IV (gap < 0).
    """
    _lp_stock_price, _lp_price_source = _calculate_approx_stock_price(row)
    signal = row.get('Signal_Type', '')
    gap_30d = row.get('IVHV_gap_30D', 0)

    # Calculate longer-term gaps — only when both IV and HV are available
    iv_180 = row.get('IV_180_D_Put')
    hv_180 = row.get('HV_180_D_Cur')
    gap_180d = (float(iv_180) - float(hv_180)) if (pd.notna(iv_180) and pd.notna(hv_180)) else None

    iv_60 = row.get('IV_60_D_Put')
    hv_60 = row.get('HV_60_D_Cur')
    gap_60d = (float(iv_60) - float(hv_60)) if (pd.notna(iv_60) and pd.notna(hv_60)) else None

    # Rejection criteria — signal required; IV gap is context not a hard block.
    # Natenberg Ch.3: directional buyers benefit from cheap IV (gap < 0) but are not
    # prohibited when IV ≈ HV. Only hard-block when IV is severely expensive (gap > +15).
    # Unknown gap (immature IV) → allow (directional thesis is chart-driven).
    if signal not in ['Bearish']:
        return None
    best_gap = gap_180d if gap_180d is not None else (gap_60d if gap_60d is not None else gap_30d)
    if best_gap is not None and best_gap > 15:
        return None  # IV severely expensive for a buyer (Sinclair: that edge belongs to sellers)

    # Build human-readable IV context (omit gaps we don't have data for)
    iv_parts = [f"gap_30d={gap_30d:.1f}"]
    if gap_60d is not None:
        iv_parts.append(f"gap_60d={gap_60d:.1f}")
    if gap_180d is not None:
        iv_parts.append(f"gap_180d={gap_180d:.1f}")
    iv_context_str = ", ".join(iv_parts)

    best_gap_label = "gap_180d" if gap_180d is not None else ("gap_60d" if gap_60d is not None else "gap_30d")

    return {
        'Ticker': ticker,
        'Strategy_Name': 'Long Put',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Bearish signal, IV context: {best_gap_label}={best_gap:.1f}" if best_gap is not None else "Bearish signal (IV history immature)",
        'Theory_Source': 'Natenberg Ch.3 - Directional with negative delta',
        'Regime_Context': signal,
        'IV_Context': iv_context_str,
        'Capital_Requirement': 500,
        'Risk_Profile': 'Defined (max loss = premium paid)',
        'Greeks_Exposure': 'Short Delta, Long Vega, Short Theta',
        'Execution_Ready': True,
        'Confidence': 65,
        'Trade_Bias': 'Bearish',
        'Strategy_Type': 'Directional',
        'Approx_Stock_Price': _lp_stock_price,
        'Approx_Stock_Price_Source': _lp_price_source,
    }


def _validate_csp(ticker: str, row: pd.Series) -> Optional[Dict]:
    """
    Validate Cash-Secured Put strategy.

    Theory: Passarelli - Premium collection when IV > HV.
    Entry: Bullish signal + Rich IV (gap > 0) + Moderate IV_Rank (≤70).
    """
    stock_price, _price_source = _calculate_approx_stock_price(row)
    if stock_price == 0:
        return None  # Cannot compute capital obligation without stock price

    signal = row.get('Signal_Type', '')
    gap_30d = row.get('IVHV_gap_30D')  # None if key missing; NaN if key exists with no value
    iv_rank = _get_iv_rank(row)
    _iv_rank_known = iv_rank != 50.0 or any(
        row.get(c) is not None and not (isinstance(row.get(c), float) and pd.isna(row.get(c)))
        for c in ('IV_Rank_30D', 'IV_Rank_60D', 'IV_Rank', 'IV_Rank_XS')
    )

    # Rejection criteria
    _csp_bidirectional_override = False
    if signal not in ['Bullish', 'Sustained Bullish']:
        # Bidirectional (Neutral trend) with bullish EMA and strong IV gap
        # can still be a valid income setup (Passarelli Ch.7: premium edge
        # comes from IV-HV gap, not directional conviction).
        # Require higher gap (6.0) to compensate for weaker trend.
        ema_signal = str(row.get('Chart_EMA_Signal', '') or '').strip()
        if (signal == 'Bidirectional' and ema_signal == 'Bullish'
                and gap_30d is not None and not (isinstance(gap_30d, float) and pd.isna(gap_30d))
                and gap_30d >= 6.0):
            _csp_bidirectional_override = True
        else:
            return None
    # NaN guard: NaN <= 0 evaluates False in pandas, silently bypassing the gate.
    # Require explicit positive value for income premium edge.
    if gap_30d is None or (isinstance(gap_30d, float) and pd.isna(gap_30d)):
        return None  # IV gap unknown — cannot confirm premium edge
    if gap_30d < 3.0:
        return None  # IV edge < 3pts — too thin for premium selling (Sinclair Ch.4)
    # Sinclair (Volatility Trading Ch.4): high IV_Rank = BEST time to sell — never a disqualifier.

    capital_req = stock_price * 100  # CSP collateral = 100 shares at current price

    # GAP 4 FIX: IV_Trend_7D soft gate for income sellers.
    # Augen (Volatility Edge Ch.3) + Bennett (Trading Volatility Ch.5):
    # "Don't sell premium into rising IV below rank 40 — you're fighting the seller's wind.
    # Rising IV with low rank = trend just starting; wait for IV to crest before selling."
    # Not a hard block (income sellers sometimes want rising IV at high rank — that's fine).
    # Only penalize: rising trend AND rank known AND rank < 40 (edge hasn't arrived yet).
    _csp_iv_trend = str(row.get('IV_Trend_7D') or '').strip()
    _csp_base = 60 if _csp_bidirectional_override else 70
    # IV_Rank confidence scaling: selling premium at high rank = stronger edge
    _csp_rank_adj = _iv_rank_confidence_adjustment(iv_rank, _iv_rank_known)
    _csp_confidence = max(40, min(95, _csp_base + _csp_rank_adj))
    _csp_trend_note = ''
    _csp_signal_note = ' [Bidirectional→Income: EMA Bullish + gap≥6]' if _csp_bidirectional_override else ''
    _csp_rank_note = f' [IV_Rank={iv_rank:.0f}→conf{_csp_rank_adj:+d}]' if _csp_rank_adj != 0 else ''
    if _csp_iv_trend == 'Rising' and iv_rank < 40:
        _csp_confidence = max(_csp_confidence - 10, 50)
        _csp_trend_note = f' [IV_Trend_7D=Rising, Rank={iv_rank:.0f}<40 — selling into rising IV below threshold; Augen Ch.3]'

    _csp_signal_label = signal if not _csp_bidirectional_override else 'Bidirectional (EMA Bullish)'
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Cash-Secured Put',
        'Strategy_Tier': 1,
        'Valid_Reason': f"{_csp_signal_label} + Rich IV (gap_30d={gap_30d:.1f}, IV_Rank={iv_rank:.0f}){_csp_signal_note}{_csp_rank_note}" + _csp_trend_note,
        'Theory_Source': 'Passarelli - Premium collection when IV > HV',
        'Regime_Context': signal,
        'IV_Context': f"gap_30d={gap_30d:.1f}, IV_Rank={iv_rank:.0f}, IV_Trend_7D={_csp_iv_trend or 'unknown'}",
        'Capital_Requirement': capital_req,
        'Risk_Profile': 'Obligation (max loss = strike - premium)',
        'Greeks_Exposure': 'Long Delta, Short Vega, Long Theta',
        'Execution_Ready': True,
        'Confidence': _csp_confidence,
        'Trade_Bias': 'Bullish',
        'Strategy_Type': 'INCOME',
        'Approx_Stock_Price': stock_price,
        'Approx_Stock_Price_Source': _price_source,
    }


def _validate_covered_call(ticker: str, row: pd.Series) -> Optional[Dict]:
    """
    Validate Covered Call strategy.

    Theory: Passarelli - Premium collection on held stock.
    Entry: Bearish signal + Rich IV (gap > 0) + Stock ownership.
    """
    stock_price, _price_source = _calculate_approx_stock_price(row)
    signal = row.get('Signal_Type', '')
    gap_30d = row.get('IVHV_gap_30D')  # None if key missing; NaN if key exists with no value

    # Rejection criteria
    if signal not in ['Bearish']:
        return None
    # NaN guard: NaN <= 0 evaluates False in pandas, silently bypassing the gate.
    if gap_30d is None or (isinstance(gap_30d, float) and pd.isna(gap_30d)):
        return None  # IV gap unknown — cannot confirm premium edge
    if gap_30d < 3.0:
        return None  # IV edge < 3pts — too thin for income strategy (Sinclair Ch.4)

    # GAP 4 FIX: IV_Trend_7D soft gate for CC sellers (same as CSP).
    _cc_iv_rank = _get_iv_rank(row)
    _cc_iv_rank_known = _cc_iv_rank != 50.0 or any(
        row.get(c) is not None and not (isinstance(row.get(c), float) and pd.isna(row.get(c)))
        for c in ('IV_Rank_30D', 'IV_Rank_60D', 'IV_Rank', 'IV_Rank_XS')
    )
    _cc_iv_trend = str(row.get('IV_Trend_7D') or '').strip()
    _cc_base = 70
    _cc_rank_adj = _iv_rank_confidence_adjustment(_cc_iv_rank, _cc_iv_rank_known)
    _cc_confidence = max(40, min(95, _cc_base + _cc_rank_adj))
    _cc_trend_note = ''
    _cc_rank_note = f' [IV_Rank={_cc_iv_rank:.0f}→conf{_cc_rank_adj:+d}]' if _cc_rank_adj != 0 else ''
    if _cc_iv_trend == 'Rising' and _cc_iv_rank < 40:
        _cc_confidence = max(_cc_confidence - 10, 50)
        _cc_trend_note = f' [IV_Trend_7D=Rising, Rank={_cc_iv_rank:.0f}<40 — selling into rising IV below threshold; Augen Ch.3]'

    return {
        'Ticker': ticker,
        'Strategy_Name': 'Covered Call',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Bearish + Rich IV (gap_30d={gap_30d:.1f}, IV_Rank={_cc_iv_rank:.0f}) [requires stock ownership]{_cc_rank_note}" + _cc_trend_note,
        'Theory_Source': 'Passarelli - Premium collection on held stock',
        'Regime_Context': signal,
        'IV_Context': f"gap_30d={gap_30d:.1f}, IV_Rank={_cc_iv_rank:.0f}, IV_Trend_7D={_cc_iv_trend or 'unknown'}",
        'Capital_Requirement': 0,  # stock assumed held; no new capital deployed
        'Risk_Profile': 'Unlimited downside (stock ownership)',
        'Greeks_Exposure': 'Long Delta (from stock), Short Vega, Long Theta',
        'Execution_Ready': False,  # Requires stock ownership confirmation
        'Confidence': _cc_confidence,
        'Trade_Bias': 'Bearish',
        'Strategy_Type': 'INCOME',
        'Approx_Stock_Price': stock_price,
        'Approx_Stock_Price_Source': _price_source,
    }


def _validate_buy_write(ticker: str, row: pd.Series) -> Optional[Dict]:
    """
    Validate Buy-Write strategy (stock + short call).

    Theory: Cohen Ch.7 - Buy stock + sell call when IV very rich.
    Entry: Bullish signal + Rich IV (gap_30d > 0).
    IV Rank gate: only enforced when rank is available (>50 required);
    when IMMATURE (null), gap-based check is sufficient.
    """
    stock_price, _price_source = _calculate_approx_stock_price(row)
    if stock_price == 0:
        return None  # Cannot compute capital without stock price

    signal = row.get('Signal_Type', '')
    gap_30d = row.get('IVHV_gap_30D')  # None if key missing; NaN if key exists with no value
    iv_rank_raw = None
    for col in ('IV_Rank_30D', 'IV_Rank_60D', 'IV_Rank', 'IV_Rank_XS'):
        v = row.get(col)
        if v is not None and not (isinstance(v, float) and np.isnan(v)):
            try:
                iv_rank_raw = float(v)
                break
            except (TypeError, ValueError):
                pass
    iv_rank = iv_rank_raw if iv_rank_raw is not None else 50.0

    # Rejection criteria
    _bw_bidirectional_override = False
    if signal not in ['Bullish', 'Sustained Bullish']:
        # Bidirectional with bullish EMA + strong IV gap → valid income setup
        ema_signal = str(row.get('Chart_EMA_Signal', '') or '').strip()
        if (signal == 'Bidirectional' and ema_signal == 'Bullish'
                and gap_30d is not None and not (isinstance(gap_30d, float) and pd.isna(gap_30d))
                and gap_30d >= 6.0):
            _bw_bidirectional_override = True
        else:
            return None
    # NaN guard: NaN <= 0 evaluates False in pandas, silently bypassing the gate.
    if gap_30d is None or (isinstance(gap_30d, float) and pd.isna(gap_30d)):
        return None  # IV gap unknown — cannot confirm premium edge
    if gap_30d < 3.0:
        return None  # IV edge < 3pts — too thin for income strategy (Sinclair Ch.4)
    # Only enforce IV Rank gate when rank is known AND below threshold.
    # 50.0 is the neutral fill value for IMMATURE tickers — do not gate on it.
    if iv_rank_raw is not None and iv_rank < 50:
        return None  # IV Rank known and below 50 — premium edge too thin

    _bw_signal_note = ' [Bidirectional→Income: EMA Bullish + gap≥6]' if _bw_bidirectional_override else ''
    _bw_signal_label = signal if not _bw_bidirectional_override else 'Bidirectional (EMA Bullish)'
    _bw_base = 65 if _bw_bidirectional_override else 75
    _bw_iv_rank_known = iv_rank_raw is not None
    _bw_rank_adj = _iv_rank_confidence_adjustment(iv_rank, _bw_iv_rank_known)
    _bw_confidence = max(40, min(95, _bw_base + _bw_rank_adj))
    _bw_rank_note = f' [IV_Rank={iv_rank:.0f}→conf{_bw_rank_adj:+d}]' if _bw_rank_adj != 0 else ''
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Buy-Write',
        'Strategy_Tier': 1,
        'Valid_Reason': f"{_bw_signal_label} + Very Rich IV (IV_Rank={iv_rank:.0f}){_bw_signal_note}{_bw_rank_note}",
        'Theory_Source': 'Cohen Ch.7 - Reduces cost basis via call premium',
        'Regime_Context': signal,
        'IV_Context': f"gap_30d={gap_30d:.1f}, IV_Rank={iv_rank:.0f}",
        'Capital_Requirement': stock_price * 100,  # Cost of 100 shares at current price
        'Risk_Profile': 'Stock downside risk offset by call premium',
        'Greeks_Exposure': 'Long Delta (from stock), Short Vega, Long Theta',
        'Execution_Ready': True,
        'Confidence': _bw_confidence,
        'Trade_Bias': 'Bullish',
        'Strategy_Type': 'INCOME',
        'Approx_Stock_Price': stock_price,
        'Approx_Stock_Price_Source': _price_source,
    }


def _validate_long_straddle(ticker: str, row: pd.Series) -> Optional[Dict]:
    """
    Validate Long Straddle strategy.

    Theory: Natenberg Ch.9 - Volatility buying when expecting expansion.
    Entry: Expansion setup + Very Cheap IV (IV_Rank < 35 OR gap_180d < -15).
    """
    stock_price, _price_source = _calculate_approx_stock_price(row)
    if stock_price == 0:
        return None  # Cannot compute capital without stock price

    # Infer expansion from regime and signal patterns
    regime = row.get('Regime', '')
    signal = row.get('Signal_Type', '')
    gap_30d = row.get('IVHV_gap_30D', 0)
    iv_rank = _get_iv_rank(row)
    
    # Calculate longer-term gaps
    iv_180 = row.get('IV_180_D_Call', 0)
    hv_180 = row.get('HV_180_D_Cur', 0)
    gap_180d = (iv_180 - hv_180) if (iv_180 and hv_180) else 0
    
    iv_60 = row.get('IV_60_D_Call', 0)
    hv_60 = row.get('HV_60_D_Cur', 0)
    gap_60d = (iv_60 - hv_60) if (iv_60 and hv_60) else 0
    
    # Natenberg Ch.9: Buy volatility when IV is cheap vs recent realized vol.
    # Gap (IV-HV) is the primary cheapness signal — negative gap = IV below HV = cheap.
    # IV_Rank provides additional context when data is mature; if immature (default=50),
    # rely solely on gap evidence rather than blocking on the synthetic rank value.
    # Fix 6: explicit loop avoids treating 0.0 as falsy (the 'or' chain bug).
    iv_rank_raw = None
    for _col in ('IV_Rank_30D', 'IV_Rank'):
        _v = row.get(_col)
        if _v is not None and not (isinstance(_v, float) and np.isnan(_v)):
            try:
                iv_rank_raw = float(_v)
                break
            except (TypeError, ValueError):
                pass
    iv_rank_is_real = iv_rank_raw is not None

    # Expansion proxy: gap shows cheap IV (primary gate)
    expansion = (gap_180d < 0 or gap_60d < 0)

    # Rejection criteria
    if not expansion and signal != 'Bidirectional':
        return None  # No gap evidence of cheap IV AND no bidirectional signal
    if gap_180d >= 0 and gap_60d >= 0:
        return None  # IV not cheap vs HV across all timeframes
    # Only apply rank gate when we have real (non-default) history
    # Natenberg: iv_rank < 40 is the sweet spot for vol buying, but absence of
    # history does not mean IV is expensive — use gap as the arbiter.
    # IMP 4: CONTANGO override — Sinclair Ch.4: "CONTANGO surface with elevated rank still valid
    # for straddle because back-month hasn't repriced yet."
    surface_shape = str(row.get('Surface_Shape') or '').upper()
    contango_override = (iv_rank_is_real and iv_rank >= 40 and surface_shape == 'CONTANGO')
    if iv_rank_is_real and iv_rank >= 40 and not contango_override:
        return None  # Rank known and elevated — vol not cheap enough for straddle/strangle

    # GAP 5 FIX: Hard-block when Regime=Expansion or Regime=High Vol.
    # Bennett (Trading Volatility Ch.5) + Sinclair 2020 Ch.5:
    # "If the vol surface is already in Expansion (VVIX>130 override) or High Vol regime,
    # the expansion has already occurred — buying long vol here is buying after the move.
    # The premium to own vol is now elevated; the risk/reward is unfavorable for the long side."
    # Expansion = VVIX>130 regime override (step2 Regime_Adjusted). High Vol = IV_Rank historically elevated.
    # Exception: CONTANGO override (back-month not repriced) already handled above — if we reach
    # this block with contango_override=True, it means a CONTANGO straddle is still valid despite
    # elevated rank. Don't double-block it.
    _regime_now = str(row.get('Regime_Adjusted') or row.get('Regime') or '').strip()
    if _regime_now in ('Expansion', 'High Vol') and not contango_override:
        return None  # Vol already expanded — long vol thesis invalid (Bennett Ch.5; Sinclair 2020 Ch.5)

    vvix = row.get('VVIX')
    vvix_warning = ''
    straddle_confidence = 72
    try:
        vvix_val = float(vvix) if vvix is not None else None
        if vvix_val is not None and vvix_val > 130:
            straddle_confidence = max(30, straddle_confidence - 20)
            vvix_warning = f'; VVIX={vvix_val:.0f}>130 — vol expansion may be priced in (Sinclair 2020 Ch.5)'
    except (TypeError, ValueError):
        vvix_val = None

    if contango_override:
        valid_reason = f"CONTANGO override: back-month not yet repriced (IV_Rank={iv_rank:.0f}, Surface=CONTANGO)"
        theory_src = 'Sinclair 2020 Ch.5 - CONTANGO override (back-month not repriced); Natenberg Ch.9 - ATM volatility play'
        straddle_confidence = min(straddle_confidence, 55)
    else:
        valid_reason = f"Expansion + Very Cheap IV (IV_Rank={iv_rank:.0f}, gap_180d={gap_180d:.1f})"
        theory_src = 'Natenberg Ch.9 - ATM volatility play'

    return {
        'Ticker': ticker,
        'Strategy_Name': 'Straddle',
        'Strategy_Tier': 1,
        'Valid_Reason': valid_reason + vvix_warning,
        'Theory_Source': theory_src,
        'Regime_Context': signal if signal == 'Bidirectional' else 'Expansion',
        'IV_Context': f"gap_30d={gap_30d:.1f}, gap_60d={gap_60d:.1f}, gap_180d={gap_180d:.1f}, IV_Rank={iv_rank:.0f}",
        'Capital_Requirement': stock_price * 0.08 * 100,  # ~8% of stock price for ATM straddle
        'Risk_Profile': 'Defined (max loss = total premium)',
        'Greeks_Exposure': 'Delta-neutral, Long Vega, Short Theta',
        'Execution_Ready': True,
        'Confidence': straddle_confidence,
        'Trade_Bias': 'Bidirectional',
        'Strategy_Type': 'Volatility',
        'Approx_Stock_Price': stock_price,
        'Approx_Stock_Price_Source': _price_source,
    }


def _validate_long_strangle(ticker: str, row: pd.Series) -> Optional[Dict]:
    """
    Validate Long Strangle strategy.

    Theory: Natenberg Ch.9 - OTM volatility play (cheaper than straddle).
    Entry: Expansion setup + Moderately Cheap IV (35 ≤ IV_Rank < 50).
    """
    stock_price, _price_source = _calculate_approx_stock_price(row)
    if stock_price == 0:
        return None  # Cannot compute capital without stock price

    regime = row.get('Regime', '')
    signal = row.get('Signal_Type', '')
    gap_30d = row.get('IVHV_gap_30D', 0)
    iv_rank = _get_iv_rank(row)

    # Calculate longer-term gaps
    iv_180 = row.get('IV_180_D_Call', 0)
    hv_180 = row.get('HV_180_D_Cur', 0)
    gap_180d = (iv_180 - hv_180) if (iv_180 and hv_180) else 0

    iv_60 = row.get('IV_60_D_Call', 0)
    hv_60 = row.get('HV_60_D_Cur', 0)
    gap_60d = (iv_60 - hv_60) if (iv_60 and hv_60) else 0

    # Natenberg Ch.9: Strangle = OTM vol play (cheaper entry than straddle).
    # Gap is the primary cheapness evidence. IV_Rank < 50 is the ideal zone,
    # but when rank is immature (default=50), gap alone is sufficient.
    # Fix 6: explicit loop avoids treating 0.0 as falsy (the 'or' chain bug).
    iv_rank_raw_str = None
    for _col_s in ('IV_Rank_30D', 'IV_Rank'):
        _v_s = row.get(_col_s)
        if _v_s is not None and not (isinstance(_v_s, float) and np.isnan(_v_s)):
            try:
                iv_rank_raw_str = float(_v_s)
                break
            except (TypeError, ValueError):
                pass
    iv_rank_is_real_str = iv_rank_raw_str is not None

    # Expansion proxy: gap shows cheap IV (primary gate)
    expansion = (gap_180d < 0 or gap_60d < 0)

    # Rejection criteria
    if not expansion and signal != 'Bidirectional':
        return None  # No cheapness evidence AND no bidirectional catalyst
    if gap_180d >= 0 and gap_60d >= 0:
        return None  # IV not cheap across any timeframe
    # Strangle is a cheaper version of straddle — allow up to rank 50 when known.
    # Block only on confirmed expensive IV (rank >= 50 with real data).
    if iv_rank_is_real_str and iv_rank >= 50:
        return None  # IV rank elevated — vol not cheap enough for strangle buyer

    # GAP 5 FIX: Hard-block when Regime=Expansion or Regime=High Vol (mirrors straddle logic).
    # Bennett (Trading Volatility Ch.5) + Sinclair 2020 Ch.5:
    # When vol has already expanded (VVIX>130 override or HIGH_VOL regime), the strangle buyer
    # is entering after the move — premium is elevated and edge has reversed.
    _regime_now_str = str(row.get('Regime_Adjusted') or row.get('Regime') or '').strip()
    if _regime_now_str in ('Expansion', 'High Vol'):
        return None  # Vol already expanded — long vol thesis invalid (Bennett Ch.5; Sinclair 2020 Ch.5)

    # GAP 3 FIX: VVIX suppress — Sinclair Ch.4: "When VVIX > 130, do not buy straddles/strangles."
    # Reduce Confidence by 20 (min 30), don't hard-block.
    vvix_str = row.get('VVIX')
    vvix_warning_str = ''
    strangle_confidence = 68
    try:
        vvix_val_str = float(vvix_str) if vvix_str is not None else None
        if vvix_val_str is not None and vvix_val_str > 130:
            strangle_confidence = max(30, strangle_confidence - 20)
            vvix_warning_str = f'; VVIX={vvix_val_str:.0f}>130 — vol expansion may be priced in (Sinclair Ch.4)'
    except (TypeError, ValueError):
        pass

    return {
        'Ticker': ticker,
        'Strategy_Name': 'Strangle',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Expansion + Moderately Cheap IV (IV_Rank={iv_rank:.0f})" + vvix_warning_str,
        'Theory_Source': 'Natenberg Ch.9 - OTM volatility (requires significant price movement)',
        'Regime_Context': signal if signal == 'Bidirectional' else 'Expansion',
        'IV_Context': f"gap_30d={gap_30d:.1f}, gap_60d={gap_60d:.1f}, gap_180d={gap_180d:.1f}, IV_Rank={iv_rank:.0f}",
        'Capital_Requirement': stock_price * 0.05 * 100,  # ~5% of stock price for OTM strangle
        'Risk_Profile': 'Defined (max loss = total premium)',
        'Greeks_Exposure': 'Delta-neutral, Long Vega, Short Theta',
        'Execution_Ready': True,
        'Confidence': strangle_confidence,
        'Trade_Bias': 'Bidirectional',
        'Strategy_Type': 'Volatility',
        'Approx_Stock_Price': stock_price,
        'Approx_Stock_Price_Source': _price_source,
    }


def _validate_long_call_leap(ticker: str, row: pd.Series) -> Optional[Dict]:
    """
    Validate Long Call LEAP strategy.

    Theory: Hull Ch.10 - Multi-year directional thesis with structural conviction.
    Entry: Sustained bullish signal + Low IV + Cheap long-term IV.

    LEAP-Specific Criteria (distinguish from short-term Long Call):
    - Sustained bullish signal (not just short-term momentum)
    - IV_Rank < 40 (long-term structural entry)
    - gap_180d < -5 (cheap long-term IV)
    - Capital-heavy but defined risk (typical $2000-$5000 per contract)

    Leveraged ETF guard: LEAP structurally invalid — daily reset breaks multi-year thesis.
    """
    # Leveraged ETF guard: daily-reset products cannot sustain a multi-year LEAP thesis.
    # Chain depth also maxes at ~90-180 DTE on leveraged ETFs (no true LEAP tenor available).
    if ticker.upper() in _LEVERAGED_ETFS:
        return None

    stock_price, _price_source = _calculate_approx_stock_price(row)
    if stock_price == 0:
        return None  # Cannot compute capital without stock price

    signal = row.get('Signal_Type', '')
    gap_30d = row.get('IVHV_gap_30D', 0)
    iv_rank = _get_iv_rank(row)
    
    # Calculate longer-term gaps
    iv_180 = row.get('IV_180_D_Call', 0)
    hv_180 = row.get('HV_180_D_Cur', 0)
    gap_180d = (iv_180 - hv_180) if (iv_180 and hv_180) else 0
    
    iv_60 = row.get('IV_60_D_Call', 0)
    hv_60 = row.get('HV_60_D_Cur', 0)
    gap_60d = (iv_60 - hv_60) if (iv_60 and hv_60) else 0
    
    # LEAP criteria (stricter than short-term)
    # Note: 'Sustained Bullish' is stricter; fallback to 'Bullish' if not available
    if signal not in ['Sustained Bullish', 'Bullish']:
        return None
    # Hull Ch.10: cheap long-term IV improves LEAP entry edge.
    # Use best available gap (180d preferred, fall back to 60d or 30d).
    # Hard-block only when IV is severely expensive (>15 pts above HV).
    # Hull does NOT mandate negative gap — it is context for timing, not a prerequisite.
    _has_180 = pd.notna(iv_180) and pd.notna(hv_180) and iv_180 != 0 and hv_180 != 0
    _has_60 = pd.notna(iv_60) and pd.notna(hv_60) and iv_60 != 0 and hv_60 != 0
    best_gap_leap_lc = gap_180d if _has_180 else (gap_60d if _has_60 else gap_30d)
    if best_gap_leap_lc is not None and best_gap_leap_lc > 15:
        return None  # IV severely expensive for a buyer — no edge (Sinclair)
    
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Long Call LEAP',
        'Strategy_Tier': 1,
        'Valid_Reason': (
            f"Bullish + Structural thesis + IV context: best_gap={best_gap_leap_lc:.1f}"
            if best_gap_leap_lc is not None
            else "Bullish + Structural thesis (IV history immature)"
        ),
        'Theory_Source': 'Hull Ch.10 - Multi-year directional with defined risk',
        'Regime_Context': signal,
        'IV_Context': f"gap_30d={gap_30d:.1f}, best_leap_gap={best_gap_leap_lc:.1f}" if best_gap_leap_lc is not None else f"gap_30d={gap_30d:.1f}, leap_gap=immature",
        'Capital_Requirement': stock_price * 0.20 * 100,  # ~20% of stock price for deep ITM LEAP call
        'Risk_Profile': f'Defined (max loss = premium, approx ${stock_price * 0.20 * 100:,.0f})',
        'Greeks_Exposure': 'Long Delta (lower), Long Vega, Short Theta (minimal)',
        'Execution_Ready': True,
        'Confidence': 70,
        'Trade_Bias': 'Bullish',
        'Strategy_Type': 'Directional',
        'Approx_Stock_Price': stock_price,
        'Approx_Stock_Price_Source': _price_source,
    }


def _validate_long_put_leap(ticker: str, row: pd.Series) -> Optional[Dict]:
    """
    Validate Long Put LEAP strategy.

    Theory: Hull Ch.10 - Multi-year hedging or structural bearish thesis.
    Entry: Sustained bearish signal + Low IV + Cheap long-term IV.

    LEAP-Specific Criteria:
    - Sustained bearish signal or hedge rationale
    - IV_Rank < 40 (long-term protection cost-efficiency)
    - gap_180d < -5 (cheap long-term IV)

    Leveraged ETF guard: LEAP structurally invalid — daily reset breaks multi-year thesis.
    """
    # Leveraged ETF guard: same reason as Long Call LEAP — daily reset invalidates thesis.
    if ticker.upper() in _LEVERAGED_ETFS:
        return None

    stock_price, _price_source = _calculate_approx_stock_price(row)
    if stock_price == 0:
        return None  # Cannot compute capital without stock price

    signal = row.get('Signal_Type', '')
    gap_30d = row.get('IVHV_gap_30D', 0)
    iv_rank = _get_iv_rank(row)
    
    # Calculate longer-term gaps
    iv_180 = row.get('IV_180_D_Put', 0)
    hv_180 = row.get('HV_180_D_Cur', 0)
    gap_180d = (iv_180 - hv_180) if (iv_180 and hv_180) else 0
    
    iv_60 = row.get('IV_60_D_Put', 0)
    hv_60 = row.get('HV_60_D_Cur', 0)
    gap_60d = (iv_60 - hv_60) if (iv_60 and hv_60) else 0
    
    # LEAP criteria
    if signal not in ['Sustained Bearish', 'Bearish']:
        return None
    # Hull Ch.10: cheap long-term IV improves LEAP entry edge.
    # Use best available gap (180d preferred, fall back to 60d or 30d).
    # Hard-block only when IV is severely expensive (>15 pts above HV).
    _has_180_lp = pd.notna(iv_180) and iv_180 != 0 and pd.notna(hv_180) and hv_180 != 0
    _has_60_lp = pd.notna(iv_60) and iv_60 != 0 and pd.notna(hv_60) and hv_60 != 0
    best_gap_leap_lp = gap_180d if _has_180_lp else (gap_60d if _has_60_lp else gap_30d)
    if best_gap_leap_lp is not None and best_gap_leap_lp > 15:
        return None  # IV severely expensive for a buyer — no edge (Sinclair)
    
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Long Put LEAP',
        'Strategy_Tier': 1,
        'Valid_Reason': (
            f"Bearish + Structural thesis + IV context: best_gap={best_gap_leap_lp:.1f}"
            if best_gap_leap_lp is not None
            else "Bearish + Structural thesis (IV history immature)"
        ),
        'Theory_Source': 'Hull Ch.10 - Multi-year protective or directional',
        'Regime_Context': signal,
        'IV_Context': f"gap_30d={gap_30d:.1f}, best_leap_gap={best_gap_leap_lp:.1f}" if best_gap_leap_lp is not None else f"gap_30d={gap_30d:.1f}, leap_gap=immature",
        'Capital_Requirement': stock_price * 0.20 * 100,  # ~20% of stock price for deep ITM LEAP put
        'Risk_Profile': f'Defined (max loss = premium, approx ${stock_price * 0.20 * 100:,.0f})',
        'Greeks_Exposure': 'Short Delta (lower), Long Vega, Short Theta (minimal)',
        'Execution_Ready': True,
        'Confidence': 70,
        'Trade_Bias': 'Bearish',
        'Strategy_Type': 'Directional',
        'Approx_Stock_Price': stock_price,
        'Approx_Stock_Price_Source': _price_source,
    }


def _validate_call_debit_spread(ticker: str, row: pd.Series) -> Optional[Dict]:
    """
    Validate Call Debit Spread strategy.
    
    Theory: Natenberg Ch.5 - Directional bullish with defined risk/reward.
    Entry: Bullish signal + Any IV regime.
    """
    stock_price, _price_source = _calculate_approx_stock_price(row)
    if stock_price == 0:
        return None
    
    # Approximate capital for a debit spread (e.g., 2% of stock price for spread width)
    capital_req = stock_price * 0.02 * 100
    
    signal = row.get('Signal_Type', '')
    
    # Rejection criteria
    if signal not in ['Bullish', 'Sustained Bullish']:
        return None
    
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Call Debit Spread',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Bullish + Defined Risk/Reward",
        'Theory_Source': 'Natenberg Ch.5 - Directional with limited risk',
        'Regime_Context': signal,
        'IV_Context': 'Any IV regime',
        'Capital_Requirement': capital_req,
        'Risk_Profile': 'Defined (max loss = debit paid)',
        'Greeks_Exposure': 'Long Delta, Short Vega, Short Theta',
        'Execution_Ready': True,
        'Confidence': 60,
        'Trade_Bias': 'Bullish',
        'Strategy_Type': 'Directional',
        'Approx_Stock_Price': stock_price,
        'Approx_Stock_Price_Source': _price_source,
    }


def _validate_put_debit_spread(ticker: str, row: pd.Series) -> Optional[Dict]:
    """
    Validate Put Debit Spread strategy.
    
    Theory: Natenberg Ch.5 - Directional bearish with defined risk/reward.
    Entry: Bearish signal + Any IV regime.
    """
    stock_price, _price_source = _calculate_approx_stock_price(row)
    if stock_price == 0:
        return None
    
    # Approximate capital for a debit spread (e.g., 2% of stock price for spread width)
    capital_req = stock_price * 0.02 * 100
    
    signal = row.get('Signal_Type', '')
    
    # Rejection criteria
    if signal not in ['Bearish']:
        return None
    
    return {
        'Ticker': ticker,
        'Strategy_Name': 'Put Debit Spread',
        'Strategy_Tier': 1,
        'Valid_Reason': f"Bearish + Defined Risk/Reward",
        'Theory_Source': 'Natenberg Ch.5 - Directional with limited risk',
        'Regime_Context': signal,
        'IV_Context': 'Any IV regime',
        'Capital_Requirement': capital_req,
        'Risk_Profile': 'Defined (max loss = debit paid)',
        'Greeks_Exposure': 'Short Delta, Short Vega, Short Theta',
        'Execution_Ready': True,
        'Confidence': 60,
        'Trade_Bias': 'Bearish',
        'Strategy_Type': 'Directional',
        'Approx_Stock_Price': stock_price,
        'Approx_Stock_Price_Source': _price_source,
    }


# ==========================================
# MAIN RECOMMENDATION FUNCTION
# ==========================================

def recommend_strategies(
    df: pd.DataFrame,
    enable_directional: bool = True,
    enable_income: bool = True,
    enable_neutral: bool = True,
    enable_volatility: bool = True,
    tier_filter: str = 'tier1_only',
    exploration_mode: bool = False
) -> pd.DataFrame:
    """
    Generate multi-strategy recommendations using Strategy Ledger architecture.
    
    🚨 ARCHITECTURAL CHANGE (2025-01-XX):
    Moved from single-strategy-per-ticker to Strategy Ledger pattern.
    Each ticker may generate MULTIPLE strategies simultaneously.
    
    **Strategy Ledger Pattern**:
    - Each row = (Ticker × Strategy) pairing
    - Independent validators (no if/elif chains)
    - Additive logic (append all valid strategies)
    - Theory-explicit (Valid_Reason + Theory_Source)
    
    **Theory Compliance**:
    - Multiple strategies can coexist for same ticker (Hull)
    - Bullish ticker can have: Long Call + CSP + Buy-Write (capital/risk-dependent)
    - Expansion ticker can have: Long Straddle + Long Strangle (budget-dependent)
    - Strategy discovery ≠ execution filtering (Step 7 vs Step 9B)
    
    Returns:
        DataFrame with Strategy Ledger format:
        - Multiple rows per ticker (if multiple strategies valid)
        - Columns: Ticker, Strategy_Name, Valid_Reason, Theory_Source, etc.
        - No Primary_Strategy (deprecated single-strategy schema)
    
    Example Output:
        | Ticker | Strategy_Name      | Valid_Reason                          | Capital_Requirement |
        |--------|--------------------|---------------------------------------|---------------------|
        | AAPL   | Long Call          | Bullish + Cheap IV (gap_180d=-12.3)  | 500                 |
        | AAPL   | Cash-Secured Put   | Bullish + Rich IV (IV_Rank=65)       | 15000               |
        | MELI   | Long Straddle      | Expansion + Very Cheap IV (rank=28)  | 8000                |
        | MELI   | Long Strangle      | Expansion + Moderately Cheap IV      | 5000                |
    
    Strategy Selection Logic:
        **TIER-1 STRATEGIES (Broker-Approved)**:
        1. **Long Call**: Bullish + Cheap IV (gap < 0)
        2. **Long Put**: Bearish + Cheap IV (gap < 0)
        3. **Cash-Secured Put**: Bullish + Rich IV (gap > 0, IV_Rank ≤ 70)
        4. **Covered Call**: Bearish + Rich IV (requires stock ownership)
        5. **Buy-Write**: Bullish + Very Rich IV (IV_Rank > 70)
        6. **Long Straddle**: Expansion + Very Cheap IV (IV_Rank < 35)
        7. **Long Strangle**: Expansion + Moderately Cheap IV (35 ≤ IV_Rank < 50)
    
    Usage Notes:
        - Step 7 = DISCOVERY ONLY (no execution filtering)
        - Step 9B = EXECUTION VALIDATION (liquidity, strikes, capital, Greeks)
        - User chooses from multiple strategies based on capital/risk preference
    """
    from .utils import validate_input
    
    # Create working copy
    df = df.copy()
    
    # Validate required columns (flexible IV_Rank column name)
    required_cols = [
        'Ticker', 'IVHV_gap_30D', 'Signal_Type', 'Regime'
    ]
    validate_input(df, required_cols, 'Step 7')
    
    # Handle IV_Rank column flexibility
    if 'IV_Rank_XS' not in df.columns:
        if 'IV_Rank_30D' in df.columns:
            df['IV_Rank_XS'] = df['IV_Rank_30D'].fillna(50.0) # Fill NaN with default neutral rank
            logger.info(f"ℹ️ Using IV_Rank_30D as IV_Rank_XS, filling NaNs with 50.0")
        elif 'IV30_Call' in df.columns and 'HV30' in df.columns:
            # Calculate IV_Rank from IV30 if neither rank column exists
            logger.info(f"ℹ️ Calculating IV_Rank_XS from IV30_Call")
            df['IV_Rank_XS'] = 50.0  # Default to neutral rank
            # Per-ticker percentile calculation would go here if we had historical data
        else:
            # Last resort: use constant neutral value
            logger.warning(f"⚠️ No IV_Rank column found, using neutral value (50.0)")
            df['IV_Rank_XS'] = 50.0
    else:
        # If IV_Rank_XS already exists, ensure its NaNs are filled too
        df['IV_Rank_XS'] = df['IV_Rank_XS'].fillna(50.0)
        logger.info(f"ℹ️ Existing IV_Rank_XS column found, filling NaNs with 50.0")
    
    # Work on all data (no Data_Complete filtering since Step 6 already validates)
    df_complete = df.copy()
    logger.info(f"🎯 Step 7 (MULTI-STRATEGY): Processing {len(df_complete)} tickers")
    
    if df_complete.empty:
        logger.warning("⚠️ No tickers with complete data")
        return df
    
    # === MULTI-STRATEGY LEDGER GENERATION ===
    # Additive logic: append all valid strategies (no if/elif chains)
    strategies = []
    
    # Define independent validators (order-independent)
    validators = []
    if enable_directional:
        validators.extend([
            _validate_long_call,
            _validate_long_put,
            _validate_long_call_leap,
            _validate_long_put_leap,
            # FIDELITY_DISABLED: _validate_call_debit_spread,  (multi-leg spreads not supported by broker)
            # FIDELITY_DISABLED: _validate_put_debit_spread,   (multi-leg spreads not supported by broker)
        ])
    if enable_income:
        validators.extend([
            _validate_csp,
            _validate_covered_call,
            _validate_buy_write,
        ])
    if enable_volatility:
        validators.extend([
            _validate_long_straddle,
            _validate_long_strangle,
        ])

    # Apply validators additively, ensuring at least one strategy per ticker
    for idx, row in df_complete.iterrows():
        ticker = row['Ticker']
        current_ticker_strategies = []

        # BUG 1 FIX — Entry quality gate pre-check
        # Murphy Ch.4: "Wait for the pullback" — CHASING entry suppresses directional strategies only.
        # Income (CSP, CC, Buy-Write) and Volatility (Straddle, Strangle) are unaffected.
        entry_quality = str(row.get('Entry_Quality') or '').upper()
        entry_recommendation = str(row.get('Entry_Recommendation') or '').upper()
        is_chasing = (entry_quality == 'CHASING' and entry_recommendation == 'AVOID')

        # Run all validators (independent, no mutual exclusion)
        gate_applied_this_ticker = False
        for validator in validators:
            strategy = validator(ticker, row)
            if strategy:  # If valid, append
                # CHASING awareness: penalize confidence instead of suppressing.
                # Murphy Ch.4 / Bulkowski: chasing entry = lower conviction, not zero conviction.
                # The scan should surface all eligible strategies; the decision layer ranks them.
                strat_name = str(strategy.get('Strategy_Name', '')).lower()
                if is_chasing and strat_name in _DIRECTIONAL_STRATEGIES:
                    _chase_penalty = 15
                    _orig_conf = strategy.get('Confidence', 70)
                    strategy['Confidence'] = max(_orig_conf - _chase_penalty, 40)
                    strategy['Chasing_Penalized'] = True
                    _chase_note = (
                        f" [CHASING: Entry_Quality=CHASING, confidence {_orig_conf}→{strategy['Confidence']}; "
                        f"Murphy Ch.4: wait for pullback]"
                    )
                    strategy['Valid_Reason'] = str(strategy.get('Valid_Reason', '')) + _chase_note
                    logger.debug(
                        f"[CHASING_GATE] {ticker}: penalized '{strategy['Strategy_Name']}' "
                        f"confidence {_orig_conf}→{strategy['Confidence']} (was: suppress)"
                    )
                    gate_applied_this_ticker = True

                # Copy all original row data for Step 9B
                strategy_with_context = {**row.to_dict(), **strategy}
                strategy_with_context['Entry_Quality_Gate_Applied'] = gate_applied_this_ticker
                current_ticker_strategies.append(strategy_with_context)

        # Backfill Entry_Quality_Gate_Applied on all strategies for this ticker
        # (gate may have fired for a suppressed strategy even if income/vol passed through)
        if gate_applied_this_ticker:
            for s in current_ticker_strategies:
                s['Entry_Quality_Gate_Applied'] = True

        if not current_ticker_strategies:
            # TIER1_ONLY: suppress Neutral/Watch fallback — no output if no Tier 1 strategy qualifies
            # TIER2_DISABLED: neutral_strategy = _create_neutral_strategy(ticker, row)
            # TIER2_DISABLED: strategies.append(neutral_strategy)
            if is_chasing:
                logger.info(f"[CHASING_GATE] {ticker}: no strategies qualified (CHASING entry, validators returned None)")
            else:
                logger.debug(f"[TIER1] {ticker}: no Tier 1 strategy qualified — skipping neutral fallback")
        else:
            # Ensure every strategy has Entry_Quality_Gate_Applied set (False if gate never fired)
            for s in current_ticker_strategies:
                if 'Entry_Quality_Gate_Applied' not in s:
                    s['Entry_Quality_Gate_Applied'] = False
            strategies.extend(current_ticker_strategies)
    
    # Convert to DataFrame (Strategy Ledger)
    if not strategies:
        logger.warning("⚠️ No strategies generated even after adding neutral ones. This should not happen if input df was not empty.")
        return df
    
    df_ledger = pd.DataFrame(strategies)
    
    # Ensure 'thesis' column exists (required by Step 11/Dashboard)
    if not df_ledger.empty and 'Valid_Reason' in df_ledger.columns:
        df_ledger['thesis'] = df_ledger['Valid_Reason']
    
    # Log multi-strategy stats
    strategies_per_ticker = df_ledger.groupby('Ticker').size()
    avg_strategies = strategies_per_ticker.mean()
    max_strategies = strategies_per_ticker.max()
    
    logger.info(f"📊 STRATEGY LEDGER STATS:")
    logger.info(f"   Total strategies: {len(df_ledger)}")
    logger.info(f"   Unique tickers: {df_ledger['Ticker'].nunique()}")
    logger.info(f"   Avg strategies/ticker: {avg_strategies:.2f}")
    logger.info(f"   Max strategies/ticker: {max_strategies}")
    
    # Strategy breakdown
    strategy_counts = df_ledger['Strategy_Name'].value_counts()
    logger.info(f"   Strategy breakdown:")
    for strategy, count in strategy_counts.items():
        logger.info(f"      {strategy}: {count}")
    
    # === TIER-1 ENFORCEMENT ===
    # Apply tier filtering if requested
    if tier_filter == 'tier1_only' and not exploration_mode:
        total_count = len(df_ledger)
        tier1_count = (df_ledger['Strategy_Tier'] == 1).sum()
        df_ledger = df_ledger[df_ledger['Strategy_Tier'] == 1].copy()
        logger.info(f"🔒 TIER-1 FILTER: {tier1_count}/{total_count} strategies are Tier-1")
    elif tier_filter == 'include_tier2':
        df_ledger = df_ledger[df_ledger['Strategy_Tier'].isin([1, 2])].copy()
        logger.info(f"📋 TIER-1+2 FILTER: Including Tier-1 and Tier-2")
    elif tier_filter == 'all_tiers' or exploration_mode:
        logger.info(f"📚 EXPLORATION MODE: Including all tiers")
    
    # Tag execution readiness
    if exploration_mode or tier_filter != 'tier1_only':
        df_ledger['EXECUTABLE'] = (df_ledger['Strategy_Tier'] == 1).astype('bool')
        non_exec_count = (~df_ledger['EXECUTABLE']).sum()
        logger.warning(f"⚠️ {non_exec_count} strategies tagged NON_EXECUTABLE")
    else:
        df_ledger['EXECUTABLE'] = True
    
    # 🚨 HARD RULE: No ranking or single-strategy selection in Step 7.
    # The multi-strategy ledger is the authoritative output.
    # Removed _add_legacy_columns to enforce this.
    
    return df_ledger
