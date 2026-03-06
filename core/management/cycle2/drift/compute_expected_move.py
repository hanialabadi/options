"""
Forward Expectancy: Expected Move vs Required Move

Computes closed-form forward feasibility for long OPTION positions.

Guardrails (per doctrine review):
  1. Computes both breakeven AND 50% recovery targets — not just breakeven.
  2. Uses implied IV (forward-looking), NOT historical HV.
  3. Uses 10-day rolling window — not full DTE — to represent near-term expectancy.

Formula:
  Expected_Move_10D = price × IV × sqrt(10 / 252)

Required_Move_Breakeven uses TRUE breakeven (strike ± premium paid/received),
NOT just distance to strike.  For puts: BE = strike − premium.  For calls: BE = strike + premium.

Interpretation of EV_Feasibility_Ratio (Required_Move_Breakeven / Expected_Move_10D):
  < 0.5  → feasible (required move < half of 1-sigma 10D move)
  0.5–1.5 → monitor (within 1-sigma — possible but not trivial)
  > 1.5  → low expectancy → doctrine escalation candidate
  > 2.0  → very low expectancy → ROLL/EXIT gate

References:
  McMillan Ch.4: "Forward probability of reaching strike drives option value."
  Passarelli Ch.2: "Theta erodes while you wait for a move that may not arrive in time."
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)

# Long-premium strategies where theta bleed is a concern
_LONG_PREMIUM_STRATEGIES = {
    'LONG_CALL', 'LONG_PUT', 'BUY_CALL', 'BUY_PUT',
    'LEAPS_CALL', 'LEAPS_PUT', 'STRADDLE', 'STRANGLE',
    'LONG_STRADDLE', 'LONG_STRANGLE',
}

# Theta bleed flag threshold: > 3% of current premium per day = significant
_THETA_BLEED_THRESHOLD_PCT = 3.0

# EV feasibility thresholds
_EV_RATIO_MONITOR   = 0.5
_EV_RATIO_LOW       = 1.5
_EV_RATIO_VERY_LOW  = 2.0

# Rolling window in trading days (guardrail: use 10-15D, not full DTE)
_EM_WINDOW_DAYS = 10


def compute_expected_move(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each OPTION leg, compute:

    Expected_Move_10D         — 1-sigma price move over 10 trading days (using IV, not HV)
    Required_Move_Breakeven   — distance from current price to TRUE breakeven
                                 (strike ± premium, not just strike)
    Required_Move_50pct       — distance to achieve 50% recovery (halfway between
                                 current underlying and true breakeven)
    EV_Feasibility_Ratio      — Required_Move_Breakeven / Expected_Move_10D
    EV_50pct_Feasibility_Ratio— Required_Move_50pct / Expected_Move_10D
    Theta_Bleed_Daily_Pct     — abs(Theta) / Last × 100 (daily % of premium consumed)
    Theta_Opportunity_Cost_Flag — True when long-premium strategy AND bleed > 3%/day
    Theta_Opportunity_Cost_Pct  — same as Theta_Bleed_Daily_Pct (named for schema clarity)

    OPTION legs only — STOCK/EQUITY legs are skipped (all output cols remain NaN).

    For ITM options (Required_Move_Breakeven = 0), also computes:
    Profit_Cushion            — intrinsic value (distance from price to strike in $ terms)
    Profit_Cushion_Ratio      — Profit_Cushion / Expected_Move_10D (how many 10D sigmas of
                                 adverse move protection the position has)
    Interpretation:
      < 0.5  → thin cushion (one 10D sigma move wipes intrinsic — elevated risk)
      0.5–1.0 → moderate cushion (manageable but worth monitoring)
      > 1.0  → deep cushion (would take a >1σ adverse move to lose intrinsic)
    """
    df = df.copy()

    _output_cols = [
        'Expected_Move_10D',
        'Required_Move_Breakeven',
        'Required_Move_50pct',
        'EV_Feasibility_Ratio',
        'EV_50pct_Feasibility_Ratio',
        'Profit_Cushion',
        'Profit_Cushion_Ratio',
        'Theta_Bleed_Daily_Pct',
        'Theta_Opportunity_Cost_Flag',
        'Theta_Opportunity_Cost_Pct',
    ]
    for col in _output_cols:
        if col not in df.columns:
            df[col] = float('nan') if col != 'Theta_Opportunity_Cost_Flag' else False

    _computed = 0
    _skipped  = 0

    for idx, row in df.iterrows():
        if str(row.get('AssetType', '')).upper() != 'OPTION':
            _skipped += 1
            continue

        try:
            price  = float(row.get('UL Last') or 0)
            # Guardrail 2: use IV (forward-looking), not HV
            iv     = float(row.get('IV_Now') or row.get('IV_30D') or 0)
            strike = float(row.get('Strike') or 0)
            theta  = float(row.get('Theta') or 0)
            last   = float(row.get('Last') or 0)
            dte    = float(row.get('DTE') or 0)
            cp     = str(row.get('Call/Put', '')).upper()
            strategy = str(row.get('Strategy', '') or '').upper()

            # Premium at entry (frozen at inception); fall back to current
            # option value if entry premium is missing.
            _raw_pe = row.get('Premium_Entry')
            premium_entry = abs(float(_raw_pe)) if pd.notna(_raw_pe) and float(_raw_pe) != 0 else abs(last)

            if price <= 0 or iv <= 0 or strike <= 0:
                _skipped += 1
                continue

            # Guardrail 3: use 10D rolling window, not full DTE
            # 1-sigma expected move over _EM_WINDOW_DAYS trading days
            em_10 = price * iv * (_EM_WINDOW_DAYS / 252) ** 0.5

            # Required move to TRUE breakeven (including premium paid/received).
            # PUT: breakeven price = strike − premium → required = max(0, price − BE)
            # CALL: breakeven price = strike + premium → required = max(0, BE − price)
            if 'P' in cp:
                true_be_price = strike - premium_entry
                required_be = max(0.0, price - true_be_price)
            else:
                true_be_price = strike + premium_entry
                required_be = max(0.0, true_be_price - price)

            # Guardrail 1: also compute 50% recovery target
            # 50% recovery = half the distance to breakeven
            required_50 = required_be * 0.5

            # EV feasibility ratios
            ratio_be = required_be / em_10 if em_10 > 0 else float('nan')
            ratio_50 = required_50 / em_10 if em_10 > 0 else float('nan')

            # Profit cushion: for positions past true breakeven, how much adverse
            # move before P&L turns negative (distance from price to true BE).
            # PUT past BE: price < true_be_price → cushion = true_be_price - price
            # CALL past BE: price > true_be_price → cushion = price - true_be_price
            if required_be == 0.0:
                if 'P' in cp:
                    profit_cushion = true_be_price - price
                else:
                    profit_cushion = price - true_be_price
                profit_cushion = max(0.0, profit_cushion)
                cushion_ratio = profit_cushion / em_10 if em_10 > 0 else float('nan')
            else:
                profit_cushion = 0.0
                cushion_ratio = float('nan')

            # Theta bleed: daily theta as % of current option price
            theta_bleed = (abs(theta) / last * 100) if last > 0 else float('nan')

            # Theta opportunity cost flag: long premium + bleed > threshold
            # DTE guard: at ≤ 3 DTE theta bleed is structurally 100%+ (time value
            # collapses to zero) — the flag is meaningless and always fires.
            # Only flag when there is meaningful time value left to lose.
            is_long_premium = any(s in strategy for s in _LONG_PREMIUM_STRATEGIES)
            theta_flag = (
                is_long_premium
                and not pd.isna(theta_bleed)
                and theta_bleed > _THETA_BLEED_THRESHOLD_PCT
                and dte > 3
            )

            df.at[idx, 'Expected_Move_10D']           = round(em_10, 2)
            df.at[idx, 'Required_Move_Breakeven']      = round(required_be, 2)
            df.at[idx, 'Required_Move_50pct']          = round(required_50, 2)
            df.at[idx, 'EV_Feasibility_Ratio']         = round(ratio_be, 3) if not pd.isna(ratio_be) else float('nan')
            df.at[idx, 'EV_50pct_Feasibility_Ratio']   = round(ratio_50, 3) if not pd.isna(ratio_50) else float('nan')
            df.at[idx, 'Profit_Cushion']               = round(profit_cushion, 2)
            df.at[idx, 'Profit_Cushion_Ratio']         = round(cushion_ratio, 3) if not pd.isna(cushion_ratio) else float('nan')
            df.at[idx, 'Theta_Bleed_Daily_Pct']        = round(theta_bleed, 3) if not pd.isna(theta_bleed) else float('nan')
            df.at[idx, 'Theta_Opportunity_Cost_Flag']  = theta_flag
            df.at[idx, 'Theta_Opportunity_Cost_Pct']   = round(theta_bleed, 3) if not pd.isna(theta_bleed) else float('nan')

            _computed += 1

        except Exception as e:
            logger.debug(f"[compute_expected_move] idx={idx} error: {e}")
            _skipped += 1
            continue

    logger.info(
        f"[ForwardEV] Expected move computed for {_computed} legs "
        f"({_skipped} skipped/non-option)"
    )
    return df
