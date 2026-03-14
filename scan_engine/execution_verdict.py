"""
Execution Verdict Engine
========================
Post-step12 triage: takes READY candidates and produces an execution-ready
sorted list with EXECUTE / SKIP / ALTERNATIVE verdicts.

Filters applied (in order):
  1. Position overlap — Murphy Ch.10 pyramiding rules:
     - CONFLICT (opposite direction) → SKIP always
     - SIZE_UP + PROVEN_LOSER → SKIP (Murphy: never add to losing position)
     - SIZE_UP + winning/unknown → note only (Murphy: add only to winning)
  2. Signal conflicts (Weekly_Trend_Bias CONFLICTING + Blind_Spot < 0.90) → SKIP
  3. Variance Premium EXPENSIVE on directional → SKIP
  4. IV Headwind severe (< 0.80) + Timing POOR → SKIP
  5. PMCC CVaR sanity check → SKIP
  6a. Interpreter score below floor (< 60/120) → SKIP (Passarelli Ch.8)
  6b. Interpreter + Vol edge UNFAVORABLE — graduated (Passarelli 0.788):
      60-69 + UNFAVORABLE → SKIP (weak mechanics + wrong vol)
      70-79 + UNFAVORABLE → note (viable thesis, wait for vol to cheapen)
  7. Intraday execution DEFER → SKIP (Murphy: timing matters)
  8. RSI overextended entry → SKIP (Murphy Ch.10: extreme oscillator = late entry)
  9. Income PCS floor (< 55) → SKIP (structural: premium quality too weak)
 10. Income premium underselling (> 8% below BS fair value) → SKIP (transient: wait)
 11. LEAP put rho headwind — informational note only (Passarelli Ch.6, Krishnan)
 12. Same-ticker dedup: keep best by Trade_Edge_Score, mark rest ALTERNATIVE
 13. Priority ranking within EXECUTE tier

Returns the same DataFrame with 3 new columns:
  - Execution_Verdict:  EXECUTE | SKIP | ALTERNATIVE
  - Verdict_Reason:     human-readable reason
  - Execution_Rank:     1..N within EXECUTE tier (None for SKIP/ALTERNATIVE)
"""

import logging
from typing import Optional, List, Dict, Any

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# ── Thresholds ────────────────────────────────────────────────────────────────
IV_HEADWIND_SEVERE = 0.80       # IV_Headwind_Multiplier below this = severe
BLIND_SPOT_SEVERE = 0.90        # Blind_Spot_Multiplier below this = stacking penalties
EDGE_SCORE_FLOOR = 20.0         # Minimum Trade_Edge_Score for EXECUTE

# RAG-backed: Passarelli Ch.8 — interpreter conviction threshold for directional
INTERP_SCORE_WEAK = 80          # out of 120; below this + vol unfavorable = SKIP
INTERP_SCORE_MARGINAL = 70      # 70-79 + vol unfavorable = CONDITIONAL (not hard SKIP)
INTERP_SCORE_FLOOR = 60         # absolute floor; below this = SKIP regardless of vol edge
INTERP_MAX_SCORE = 120          # interpreter max score

# RAG-backed: Murphy Ch.10 — RSI extreme = overextended entry
RSI_PUT_OVEREXTENDED = 30       # Put entry when RSI < 30 = move already happened
RSI_CALL_OVEREXTENDED = 75      # Call entry when RSI > 75 = overbought exhaustion

# Income strategy quality gates
PCS_SCORE_FLOOR = 55            # Premium Collection Standard — below this = too weak to sell
INCOME_UNDERSELL_LIMIT = -8.0   # % below BS fair value — selling too cheap (transient, market-driven)


def compute_execution_verdicts(
    df: pd.DataFrame,
    scale_up_tickers: Optional[set] = None,
) -> pd.DataFrame:
    """
    Triage READY candidates into EXECUTE / SKIP / ALTERNATIVE.

    Args:
        df: DataFrame of READY candidates (already filtered to Execution_Status == 'READY')
        scale_up_tickers: set of tickers with pending management SCALE_UP requests

    Returns:
        Same DataFrame with Execution_Verdict, Verdict_Reason, Execution_Rank columns
    """
    if df.empty:
        return df

    df = df.copy()
    if scale_up_tickers is None:
        scale_up_tickers = set()

    # Initialize verdict columns
    df['Execution_Verdict'] = 'EXECUTE'
    df['Verdict_Reason'] = ''
    df['Execution_Rank'] = np.nan

    # ── PASS 1: Individual candidate filters ──────────────────────────────

    for idx in df.index:
        reasons = []
        notes = []    # informational — appended to Verdict_Reason but don't trigger SKIP

        ticker = str(df.at[idx, 'Ticker'])
        strategy = str(df.at[idx, 'Strategy_Name'] if 'Strategy_Name' in df.columns else '')
        strategy_lower = strategy.lower()

        # Detect strategy family
        is_income = any(kw in strategy_lower for kw in [
            'buy-write', 'buy_write', 'covered call', 'cash-secured', 'csp',
            'short put', 'pmcc', 'iron condor', 'credit spread',
        ])
        is_directional = not is_income

        # 1. Position overlap — Murphy Ch.10: pyramiding rules
        #    CONFLICT (opposite direction) = always SKIP
        #    SIZE_UP (same direction) = allow if winning, block if losing
        #    RAG: Murphy 0.773 "Add only to winning positions. Never add to a losing."
        #    RAG: Carver 0.672 "pyramiding positions, buying into a strengthening trend"
        pos_conflict = str(df.at[idx, 'Position_Conflict'] if 'Position_Conflict' in df.columns else '')
        has_scale_up = (
            ticker in scale_up_tickers or
            bool(df.at[idx, 'Scale_Up_Candidate'] if 'Scale_Up_Candidate' in df.columns else False)
        )

        if pos_conflict.startswith('CONFLICT'):
            # Opposite direction = always skip unless explicit hedge
            reasons.append(f"opposing open position ({pos_conflict[:60]})")

        elif pos_conflict.startswith('SIZE_UP') and not has_scale_up:
            # Same direction — check track record as proxy for winning/losing
            track_record = str(df.at[idx, 'Mgmt_Track_Record']
                               if 'Mgmt_Track_Record' in df.columns else '')
            if track_record == 'PROVEN_LOSER':
                # Murphy 10c: "Never add to a losing position"
                reasons.append(
                    f"position overlap on losing ticker ({pos_conflict[:50]}) — "
                    f"Murphy: never add to a losing position"
                )
            else:
                # Winning or unknown — annotate, don't block
                # Murphy 10a-b: "Add only to winning positions, each layer smaller"
                notes.append(
                    f"note: same-direction overlap ({pos_conflict[:50]}) — "
                    f"Murphy pyramiding: confirm position is profitable before adding"
                )

        # 2. Signal conflicts: Weekly CONFLICTING + blind spot stacking
        weekly = str(df.at[idx, 'Weekly_Trend_Bias'] if 'Weekly_Trend_Bias' in df.columns else '')
        blind_spot = float(df.at[idx, 'Blind_Spot_Multiplier'] if 'Blind_Spot_Multiplier' in df.columns else 1.0)
        if pd.isna(blind_spot):
            blind_spot = 1.0

        if weekly == 'CONFLICTING' and blind_spot < BLIND_SPOT_SEVERE:
            reasons.append(f"weekly CONFLICTING + blind spot {blind_spot:.2f}")
        elif weekly == 'CONFLICTING' and is_directional:
            reasons.append(f"weekly trend CONFLICTING with directional thesis")

        # 3. Variance Premium EXPENSIVE on directional
        vp_verdict = str(df.at[idx, 'MC_VP_Verdict'] if 'MC_VP_Verdict' in df.columns else '')
        if vp_verdict == 'EXPENSIVE' and is_directional:
            vp_score = df.at[idx, 'MC_VP_Score'] if 'MC_VP_Score' in df.columns else None
            vp_str = f" (VP={vp_score:.2f})" if pd.notna(vp_score) else ''
            reasons.append(f"variance premium EXPENSIVE{vp_str} — overpaying for vol")

        # 4. IV Headwind severe + poor timing
        iv_headwind = float(df.at[idx, 'IV_Headwind_Multiplier'] if 'IV_Headwind_Multiplier' in df.columns else 1.0)
        if pd.isna(iv_headwind):
            iv_headwind = 1.0
        timing = str(df.at[idx, 'timing_quality'] if 'timing_quality' in df.columns else '')

        if iv_headwind < IV_HEADWIND_SEVERE and timing == 'POOR':
            reasons.append(f"severe IV headwind ({iv_headwind:.2f}) + POOR timing")
        elif iv_headwind < IV_HEADWIND_SEVERE and blind_spot < BLIND_SPOT_SEVERE:
            reasons.append(f"IV headwind ({iv_headwind:.2f}) + blind spot ({blind_spot:.2f}) stacking")

        # 5. PMCC CVaR sanity check (known data issue)
        cvar = df.at[idx, 'MC_CVaR'] if 'MC_CVaR' in df.columns else None
        mid_price = df.at[idx, 'Mid_Price'] if 'Mid_Price' in df.columns else None
        if pd.notna(cvar) and pd.notna(mid_price):
            # CVaR should be roughly proportional to mid_price × 100
            expected_max = abs(float(mid_price)) * 100 * 5  # 5x is generous
            if abs(float(cvar)) > max(expected_max, 10000) and 'pmcc' in strategy_lower:
                reasons.append(f"CVaR ${abs(float(cvar)):,.0f} appears miscalculated for PMCC")

        # 6a. Strategy Interpreter absolute floor (Passarelli Ch.8)
        #     Below 60/120, the strategy mechanics are fundamentally broken:
        #     negative trend strength, zero gamma response, poor move coverage.
        #     No vol edge can compensate for a trade the mechanics don't support.
        if is_directional and 'Interp_Score' in df.columns:
            interp_score = pd.to_numeric(df.at[idx, 'Interp_Score'], errors='coerce')
            if pd.notna(interp_score) and interp_score < INTERP_SCORE_FLOOR:
                reasons.append(
                    f"interpreter {interp_score:.0f}/{INTERP_MAX_SCORE} below floor "
                    f"({INTERP_SCORE_FLOOR}) — strategy mechanics don't support entry"
                )

        # 6b. Strategy Interpreter weak + Vol edge UNFAVORABLE (Passarelli Ch.8)
        #     Graduated response:
        #       60-69 + UNFAVORABLE = SKIP (weak mechanics + wrong vol regime)
        #       70-79 + UNFAVORABLE = CONDITIONAL (thesis has merit, wait for vol)
        #     RAG: Passarelli 0.788 "buying long-term options with IV in the lower
        #     third of the 12-month range helps improve chances of success" — this is
        #     a preference ("helps improve"), not a veto. Score 70+ = viable thesis
        #     that should wait for better vol entry, not be rejected outright.
        if is_directional and 'Interp_Score' in df.columns and 'Interp_Vol_Edge' in df.columns:
            interp_score = pd.to_numeric(df.at[idx, 'Interp_Score'], errors='coerce')
            interp_vol = str(df.at[idx, 'Interp_Vol_Edge'] if pd.notna(df.at[idx, 'Interp_Vol_Edge']) else '')
            if pd.notna(interp_score) and interp_vol == 'UNFAVORABLE':
                if INTERP_SCORE_FLOOR <= interp_score < INTERP_SCORE_MARGINAL:
                    # 60-69: weak mechanics + wrong vol = hard SKIP
                    reasons.append(
                        f"interpreter {interp_score:.0f}/{INTERP_MAX_SCORE} + vol edge UNFAVORABLE "
                        f"— weak conviction to buy premium (Passarelli Ch.8)"
                    )
                elif INTERP_SCORE_MARGINAL <= interp_score < INTERP_SCORE_WEAK:
                    # 70-79: viable thesis but vol regime unfavorable — waitlist, don't reject
                    # Passarelli: "lower third of IV range helps improve chances" = wait for it
                    notes.append(
                        f"interpreter {interp_score:.0f}/{INTERP_MAX_SCORE} + vol edge UNFAVORABLE "
                        f"— thesis viable, wait for vol regime to cheapen (Passarelli Ch.8)"
                    )

        # 7. Intraday execution DEFER (Murphy: timing matters)
        #    The system computed execution timing — respect it.
        if 'Intraday_Readiness' in df.columns:
            intraday = str(df.at[idx, 'Intraday_Readiness'] if pd.notna(df.at[idx, 'Intraday_Readiness']) else '')
            if intraday == 'DEFER':
                intraday_score = pd.to_numeric(
                    df.at[idx, 'Intraday_Execution_Score'] if 'Intraday_Execution_Score' in df.columns else None,
                    errors='coerce'
                )
                score_str = f" (score {intraday_score:.0f})" if pd.notna(intraday_score) else ''
                reasons.append(f"intraday execution DEFER{score_str} — unfavorable market microstructure")

        # 8. RSI overextended entry (Murphy Ch.10)
        #    Buying puts when RSI already oversold = chasing the move.
        #    Buying calls when RSI overbought = exhaustion risk.
        if is_directional and 'RSI_14' in df.columns:
            rsi = pd.to_numeric(df.at[idx, 'RSI_14'], errors='coerce')
            is_put = 'put' in strategy_lower
            is_call = 'call' in strategy_lower
            if pd.notna(rsi):
                if is_put and rsi < RSI_PUT_OVEREXTENDED:
                    reasons.append(
                        f"RSI {rsi:.0f} already oversold for put entry — "
                        f"bearish move extended (Murphy Ch.10)"
                    )
                elif is_call and rsi > RSI_CALL_OVEREXTENDED:
                    reasons.append(
                        f"RSI {rsi:.0f} overbought for call entry — "
                        f"bullish exhaustion risk (Murphy Ch.10)"
                    )

        # 9. Income PCS floor — structural quality gate
        #    PCS < 55 = Rejected/low-Watch tier. Premium collection mechanics are
        #    too weak: wide bid-ask, poor Greeks, insufficient premium per risk.
        if is_income:
            pcs_score = pd.to_numeric(
                df.at[idx, 'PCS_Score_V2'] if 'PCS_Score_V2' in df.columns
                else df.at[idx, 'PCS_Score'] if 'PCS_Score' in df.columns
                else None,
                errors='coerce'
            )
            if pd.notna(pcs_score) and pcs_score < PCS_SCORE_FLOOR:
                pcs_status = str(df.at[idx, 'PCS_Status'] if 'PCS_Status' in df.columns else '')
                reasons.append(
                    f"PCS {pcs_score:.0f} below floor ({PCS_SCORE_FLOOR}) "
                    f"[{pcs_status}] — income quality too weak to sell"
                )

        # 10. Income premium underselling — transient market-driven gate
        #     Selling > 8% below BS fair value = giving away edge. Wait for
        #     market to reprice (bid-ask tightens, vol shifts, time decay).
        if is_income and 'Premium_vs_FairValue_Pct' in df.columns:
            prem_vs_fv = pd.to_numeric(df.at[idx, 'Premium_vs_FairValue_Pct'], errors='coerce')
            if pd.notna(prem_vs_fv) and prem_vs_fv < INCOME_UNDERSELL_LIMIT:
                reasons.append(
                    f"selling {abs(prem_vs_fv):.1f}% below BS fair value "
                    f"(limit {abs(INCOME_UNDERSELL_LIMIT):.0f}%) — wait for better premium"
                )

        # 11. LEAP put rho headwind annotation (Passarelli Ch.6, Krishnan)
        #    Rising rates erode LEAP put value (negative rho). Informational only —
        #    with exit targets at +100%/-50% and DTE≤90 time stop, holding period is
        #    3-6 months. Rho cost is <1% of position over that horizon. Delta/vega dominate.
        is_leap = 'leap' in strategy_lower
        if is_leap and is_directional and 'LEAP_Rate_Sensitivity' in df.columns:
            rate_sens = str(df.at[idx, 'LEAP_Rate_Sensitivity'] if pd.notna(df.at[idx, 'LEAP_Rate_Sensitivity']) else '')
            if rate_sens.startswith('HIGH') and 'put' in strategy_lower:
                notes.append(
                    f"note: LEAP put rho headwind ({rate_sens})"
                )

        # Apply verdict
        if reasons:
            df.at[idx, 'Execution_Verdict'] = 'SKIP'
            df.at[idx, 'Verdict_Reason'] = '; '.join(reasons)
        if notes:
            # Append notes to reason (informational, doesn't change verdict)
            existing = df.at[idx, 'Verdict_Reason']
            note_str = '; '.join(notes)
            df.at[idx, 'Verdict_Reason'] = f"{existing}; {note_str}" if existing else note_str

    # ── PASS 2: Same-ticker dedup ─────────────────────────────────────────
    # For each ticker with multiple EXECUTE candidates, keep the best by
    # Trade_Edge_Score and mark the rest as ALTERNATIVE.

    execute_mask = df['Execution_Verdict'] == 'EXECUTE'
    if execute_mask.any():
        execute_df = df[execute_mask].copy()

        # Group by ticker
        for ticker, group in execute_df.groupby('Ticker'):
            if len(group) <= 1:
                continue

            # Separate income vs directional — allow ONE of each per ticker
            income_mask = group.index.isin(
                group[group.apply(
                    lambda r: any(kw in str(r.get('Strategy_Name', '')).lower()
                                  for kw in ['buy-write', 'buy_write', 'covered call',
                                             'cash-secured', 'csp', 'short put', 'pmcc',
                                             'iron condor', 'credit spread']),
                    axis=1
                )].index
            )
            directional_idx = group[~income_mask].index
            income_idx = group[income_mask].index

            # Within directional: keep best, mark rest ALTERNATIVE
            if len(directional_idx) > 1:
                edge_col = 'Trade_Edge_Score' if 'Trade_Edge_Score' in df.columns else 'DQS_Score'
                dir_group = group.loc[directional_idx]
                dir_scores = dir_group[edge_col].fillna(0).astype(float)
                best_dir = dir_scores.idxmax()

                for alt_idx in directional_idx:
                    if alt_idx != best_dir:
                        best_strat = str(df.at[best_dir, 'Strategy_Name'])
                        alt_strat = str(df.at[alt_idx, 'Strategy_Name'])
                        df.at[alt_idx, 'Execution_Verdict'] = 'ALTERNATIVE'
                        df.at[alt_idx, 'Verdict_Reason'] = (
                            f"same ticker {ticker}: {best_strat} has higher edge "
                            f"({df.at[best_dir, edge_col]:.0f} vs {df.at[alt_idx, edge_col]:.0f})"
                        )

            # Within income: keep best, mark rest ALTERNATIVE
            if len(income_idx) > 1:
                edge_col = 'Trade_Edge_Score' if 'Trade_Edge_Score' in df.columns else 'DQS_Score'
                inc_group = group.loc[income_idx]
                inc_scores = inc_group[edge_col].fillna(0).astype(float)
                best_inc = inc_scores.idxmax()

                for alt_idx in income_idx:
                    if alt_idx != best_inc:
                        best_strat = str(df.at[best_inc, 'Strategy_Name'])
                        alt_strat = str(df.at[alt_idx, 'Strategy_Name'])
                        df.at[alt_idx, 'Execution_Verdict'] = 'ALTERNATIVE'
                        df.at[alt_idx, 'Verdict_Reason'] = (
                            f"same ticker {ticker}: {best_strat} preferred "
                            f"(pick one income strategy per ticker)"
                        )

    # ── PASS 3: Rank EXECUTE candidates ───────────────────────────────────
    execute_final = df[df['Execution_Verdict'] == 'EXECUTE'].copy()
    if not execute_final.empty:
        # Rank by Trade_Edge_Score descending
        edge_col = 'Trade_Edge_Score' if 'Trade_Edge_Score' in df.columns else 'DQS_Score'
        if edge_col in execute_final.columns:
            ranked = execute_final[edge_col].fillna(0).astype(float).rank(
                ascending=False, method='min'
            ).astype(int)
            for r_idx, rank_val in ranked.items():
                df.at[r_idx, 'Execution_Rank'] = rank_val

    # Summary log
    n_exec = (df['Execution_Verdict'] == 'EXECUTE').sum()
    n_skip = (df['Execution_Verdict'] == 'SKIP').sum()
    n_alt = (df['Execution_Verdict'] == 'ALTERNATIVE').sum()
    logger.info(
        f"[ExecutionVerdict] {len(df)} READY → "
        f"{n_exec} EXECUTE, {n_skip} SKIP, {n_alt} ALTERNATIVE"
    )

    return df


# ── Verdict → Wait Condition Mapping ────────────────────────────────────────

def generate_verdict_wait_conditions(
    verdict_reason: str,
    row: pd.Series,
) -> List[Dict[str, Any]]:
    """
    Generate specific clearance conditions from a verdict SKIP reason.

    Maps each SKIP filter to testable conditions that the Smart WAIT Loop
    can monitor on subsequent scan runs. When all conditions clear, the
    candidate is eligible for re-promotion to READY/EXECUTE.

    Args:
        verdict_reason: The Verdict_Reason string (semicolon-separated reasons)
        row: The candidate row with full scan data

    Returns:
        List of condition dicts compatible with wait_condition_generator format
    """
    from scan_engine.wait_condition_generator import (
        _create_technical_condition,
        _create_volatility_condition,
        _create_time_delay_condition,
    )

    conditions: List[Dict[str, Any]] = []
    reasons = verdict_reason.split('; ')

    strategy = str(row.get('Strategy_Name', '') or '')
    strategy_lower = strategy.lower()
    is_put = 'put' in strategy_lower
    is_call = 'call' in strategy_lower

    for reason in reasons:
        reason_lower = reason.lower()

        # Skip informational notes — they don't generate wait conditions
        if reason_lower.startswith('note:'):
            continue

        # Filter 1: Position overlap — wait for management resolution
        if 'position overlap' in reason_lower or 'opposing open position' in reason_lower:
            conditions.append(_create_time_delay_condition(
                delay_sessions=1,
                description=(
                    "Wait for management to resolve position conflict — "
                    "check after next management cycle"
                )
            ))

        # Filter 2: Weekly CONFLICTING + blind spot
        elif 'weekly conflicting' in reason_lower or 'weekly trend conflicting' in reason_lower:
            conditions.append(_create_technical_condition(
                metric="Weekly_Trend_Bias",
                operator="equals",
                threshold=0,  # sentinel — actual check is string != CONFLICTING
                description=(
                    "Weekly_Trend_Bias must flip to ALIGNED or NEUTRAL — "
                    "Murphy: don't trade against the weekly trend"
                )
            ))
            if 'blind spot' in reason_lower:
                conditions.append(_create_technical_condition(
                    metric="Blind_Spot_Multiplier",
                    operator="greater_than",
                    threshold=BLIND_SPOT_SEVERE,
                    description=(
                        f"Blind_Spot_Multiplier must recover above {BLIND_SPOT_SEVERE:.2f} — "
                        f"signal conflict penalty must ease"
                    )
                ))

        # Filter 3: Variance premium EXPENSIVE
        elif 'variance premium expensive' in reason_lower:
            conditions.append(_create_volatility_condition(
                metric="MC_VP_Score",
                operator="greater_than",
                threshold=0.75,
                description=(
                    "Variance premium must cheapen (VP score > 0.75) — "
                    "Natenberg: don't overpay for vol on directional"
                )
            ))

        # Filter 4: IV headwind + timing/blind spot stacking
        elif 'iv headwind' in reason_lower:
            conditions.append(_create_volatility_condition(
                metric="IV_Headwind_Multiplier",
                operator="greater_than",
                threshold=IV_HEADWIND_SEVERE,
                description=(
                    f"IV_Headwind_Multiplier must recover above {IV_HEADWIND_SEVERE:.2f} — "
                    f"IV regime must become less adverse"
                )
            ))
            if 'poor timing' in reason_lower:
                conditions.append(_create_technical_condition(
                    metric="timing_quality",
                    operator="equals",
                    threshold=0,  # sentinel — check string != POOR
                    description="Timing quality must improve from POOR to FAIR or GOOD"
                ))

        # Filter 5: PMCC CVaR miscalculation — data issue, recheck next session
        elif 'cvar' in reason_lower and 'miscalculated' in reason_lower:
            conditions.append(_create_time_delay_condition(
                delay_sessions=1,
                description="Recheck PMCC CVaR calculation after next data refresh"
            ))

        # Filter 6a: Interpreter below absolute floor
        elif 'interpreter' in reason_lower and 'below floor' in reason_lower:
            conditions.append(_create_technical_condition(
                metric="Interp_Score",
                operator="greater_than",
                threshold=float(INTERP_SCORE_FLOOR),
                description=(
                    f"Interpreter score must exceed {INTERP_SCORE_FLOOR}/{INTERP_MAX_SCORE} — "
                    f"strategy mechanics must show minimum viability"
                )
            ))

        # Filter 6b: Interpreter weak + vol edge UNFAVORABLE (Passarelli Ch.8)
        elif 'interpreter' in reason_lower and 'vol edge unfavorable' in reason_lower:
            # IV_Rank must drop for vol edge to flip FAVORABLE
            iv_rank = pd.to_numeric(row.get('IV_Rank_Pctile') or row.get('IV_Rank'), errors='coerce')
            if pd.notna(iv_rank) and iv_rank > 40:
                conditions.append(_create_volatility_condition(
                    metric="IV_Rank",
                    operator="less_than",
                    threshold=40.0,
                    description=(
                        f"IV_Rank must drop below 40 (currently {iv_rank:.0f}) — "
                        f"Passarelli: buy long-term options in lower third of IV range"
                    )
                ))
            else:
                conditions.append(_create_volatility_condition(
                    metric="IV_Rank",
                    operator="less_than",
                    threshold=40.0,
                    description=(
                        "IV_Rank must drop below 40 — "
                        "vol edge must flip to FAVORABLE for directional conviction"
                    )
                ))
            # Interpreter score itself could improve on next scan (new chart data)
            conditions.append(_create_technical_condition(
                metric="Interp_Score",
                operator="greater_than",
                threshold=float(INTERP_SCORE_WEAK),
                description=(
                    f"Interpreter score must exceed {INTERP_SCORE_WEAK}/{INTERP_MAX_SCORE} — "
                    f"strategy mechanics must show stronger conviction"
                )
            ))

        # Filter 9: Income PCS floor — structural, unlikely to clear quickly
        elif 'pcs' in reason_lower and 'below floor' in reason_lower:
            conditions.append(_create_technical_condition(
                metric="PCS_Score_V2",
                operator="greater_than",
                threshold=float(PCS_SCORE_FLOOR),
                description=(
                    f"PCS must exceed {PCS_SCORE_FLOOR} — "
                    f"structural income quality gate (premium, Greeks, liquidity)"
                )
            ))

        # Filter 10: Income premium underselling — transient, market-driven
        elif 'below bs fair value' in reason_lower and 'selling' in reason_lower:
            conditions.append(_create_volatility_condition(
                metric="Premium_vs_FairValue_Pct",
                operator="greater_than",
                threshold=float(INCOME_UNDERSELL_LIMIT),
                description=(
                    f"Premium must recover to within {abs(INCOME_UNDERSELL_LIMIT):.0f}% of BS fair value — "
                    f"bid-ask tightening or vol shift will reprice"
                )
            ))

        # Filter 7: Intraday execution DEFER — transient, recheck next session
        elif 'intraday execution defer' in reason_lower:
            conditions.append(_create_time_delay_condition(
                next_session=True,
                description=(
                    "Wait for next trading session — "
                    "intraday microstructure conditions are transient"
                )
            ))

        # Filter 8: RSI overextended (Murphy Ch.10)
        elif 'rsi' in reason_lower and ('oversold' in reason_lower or 'overbought' in reason_lower):
            rsi = pd.to_numeric(row.get('RSI_14'), errors='coerce')
            if is_put and 'oversold' in reason_lower:
                conditions.append(_create_technical_condition(
                    metric="RSI_14",
                    operator="greater_than",
                    threshold=float(RSI_PUT_OVEREXTENDED + 5),  # 35 — must recover past threshold
                    description=(
                        f"RSI must recover above {RSI_PUT_OVEREXTENDED + 5} "
                        f"(currently {f'{rsi:.0f}' if pd.notna(rsi) else '?'}) — "
                        f"Murphy Ch.10: wait for mean reversion before put entry"
                    )
                ))
            elif is_call and 'overbought' in reason_lower:
                conditions.append(_create_technical_condition(
                    metric="RSI_14",
                    operator="less_than",
                    threshold=float(RSI_CALL_OVEREXTENDED - 5),  # 70 — must pull back past threshold
                    description=(
                        f"RSI must pull back below {RSI_CALL_OVEREXTENDED - 5} "
                        f"(currently {f'{rsi:.0f}' if pd.notna(rsi) else '?'}) — "
                        f"Murphy Ch.10: wait for pullback before call entry"
                    )
                ))

    # Fallback: if no specific conditions generated, add a generic session wait
    if not conditions:
        conditions.append(_create_time_delay_condition(
            next_session=True,
            description="Wait for next scan to re-evaluate verdict conditions"
        ))

    logger.info(
        f"[VerdictWait] Generated {len(conditions)} clearance conditions "
        f"for {row.get('Ticker', '?')}/{strategy}"
    )
    return conditions
