"""
Income eligibility gate extracted from step12_acceptance.py.

Pure function: (row, dte) -> (eligible, reason).
"""

from typing import Tuple

import pandas as pd


def check_income_eligibility(row: pd.Series, actual_dte: float) -> Tuple[bool, str]:
    """
    Determine whether an income strategy (CSP / Covered Call / Buy-Write) has
    a genuine volatility edge to sell premium.

    Does NOT require 120+ days of IV history. Requires:
      1. IV > HV  -- there is premium to sell (gap_30d > 0)
      2. IV not at the bottom of its recent distribution -- rank > 25 OR gap large enough
      3. Term structure supports near-term selling -- Surface_Shape not inverted,
         OR inverted with a large gap (>10 pts) as compensation
      4. No earnings event inside the trade window

    Returns:
        (eligible: bool, reason: str)
    """
    gap_30d       = row.get('IVHV_gap_30D', None)
    iv_rank_20d   = row.get('IV_Rank_20D', None)
    iv_rank_30d   = row.get('IV_Rank_30D', None)
    surface_shape = str(row.get('Surface_Shape', '') or '').upper()
    days_to_earn  = row.get('days_to_earnings', None)
    iv_hist       = row.get('IV_History_Count', 0) or 0

    # -- Condition 1: IV > HV (gap must be positive to sell premium)
    try:
        gap = float(gap_30d)
    except (TypeError, ValueError):
        return False, 'BLOCK: IV vs HV gap unavailable — cannot confirm premium edge'

    if gap <= 0:
        return False, f'BLOCK: IV not elevated vs HV (gap_30d={gap:.1f}) — no premium edge'

    # -- Condition 2: IV not at the floor of its distribution
    rank_20d_ok = iv_rank_20d is not None and pd.notna(iv_rank_20d)
    rank_30d_ok = iv_rank_30d is not None and pd.notna(iv_rank_30d)

    if rank_20d_ok:
        rank = float(iv_rank_20d)
        rank_src = 'IV_Rank_20D'
        if rank < 25:
            return False, f'BLOCK: IV Rank too low ({rank:.0f}/100) — IV near 20d floor, no edge'
    elif rank_30d_ok:
        rank = float(iv_rank_30d)
        rank_src = 'IV_Rank_30D'
        if rank < 25:
            return False, f'BLOCK: IV Rank too low ({rank:.0f}/100, 30D) — IV near floor, no edge'
    else:
        if gap < 8:
            return False, (
                f'BLOCK: IV Rank unavailable ({int(iv_hist)}d history) and gap_30d too small '
                f'({gap:.1f} pts) — insufficient evidence of elevation'
            )
        rank = None
        rank_src = 'gap_proxy'

    # -- Condition 3: Term structure
    if surface_shape == 'INVERTED' and gap <= 10:
        return False, (
            f'BLOCK: Inverted term structure (Surface_Shape=INVERTED) with moderate gap '
            f'({gap:.1f} pts) — front IV spike makes premium selling risky'
        )

    # -- Condition 4: Earnings + Formation Phase
    formation_phase = str(row.get('Earnings_Formation_Phase', '') or '').upper()
    _iv_velocity = row.get('Earnings_IV_Velocity')
    _vel_note = f', IV velocity={float(_iv_velocity):+.2f}/d' if _iv_velocity is not None and pd.notna(_iv_velocity) else ''

    if formation_phase == 'IMMINENT':
        return False, (
            f'BLOCK: Earnings IMMINENT (phase={formation_phase}{_vel_note}) — '
            f'binary event risk, do not sell premium into earnings'
        )

    if formation_phase == 'LATE_POSITIONING':
        return False, (
            f'BLOCK: Pre-earnings positioning detected (phase={formation_phase}{_vel_note}) — '
            f'IV already inflated from earnings ramp, crush risk even if trade expires before event'
        )

    try:
        dte_earn = float(days_to_earn)
        if dte_earn > 0:
            if actual_dte is None or (isinstance(actual_dte, float) and pd.isna(actual_dte)):
                return False, (
                    f'BLOCK: Earnings in {dte_earn:.0f}d but Actual_DTE unknown — '
                    f'cannot confirm earnings are outside trade window'
                )
            if dte_earn <= float(actual_dte):
                return False, f'BLOCK: Earnings in {dte_earn:.0f}d inside trade window ({actual_dte:.0f} DTE) — binary risk'
    except (TypeError, ValueError):
        pass

    # -- All conditions met
    if rank is not None:
        rank_str = f'{rank_src}={rank:.0f}'
    else:
        rank_str = f'gap_proxy={gap:.1f}pts'
    shape_str = surface_shape if surface_shape else 'UNKNOWN'

    _early_note = ''
    if formation_phase == 'EARLY_POSITIONING':
        _early_note = f' [CAUTION: EARLY_POSITIONING detected{_vel_note} — IV may inflate further before earnings]'

    return True, f'OK: gap_30d={gap:.1f}, {rank_str}, shape={shape_str}{_early_note}'
