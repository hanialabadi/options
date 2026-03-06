"""
CC Opportunity Engine — Covered Call Proposal for Idle Stock Positions

For each STOCK_ONLY_IDLE position (100+ shares, no option leg sold against them),
reads the latest scan engine output and answers three questions:

  1. Is the current market favorable for selling calls on this ticker?
     (IV_Rank, Regime, trend direction, IVHV gap)

  2. If YES — which DTE bucket yields the best risk-adjusted income?
     Ranked: weekly (7d), biweekly (14d), monthly (30-45d)
     Yield metric: annualized premium / net cost basis per share

  3. If NO — why not, and what signal to watch for entry?
     ("IV_Rank=12% — wait for expansion above 25%. Watch: IV_ROC turning positive")

Design:
  - Reads from latest Step12_Acceptance_*.csv — no extra Schwab API calls
  - Non-blocking: any exception → CC_Proposal_Status=ERROR, position still managed
  - Output columns written directly onto the STOCK_ONLY_IDLE row in df_final
  - No strategy bias introduced — purely surfaces scan engine findings

Output columns (added to STOCK_ONLY_IDLE rows):
  CC_Proposal_Status   : FAVORABLE | UNFAVORABLE | SCAN_MISS | ERROR
  CC_Proposal_Verdict  : one-line summary for the card header
  CC_Unfavorable_Reason: why CC is not advised right now (if UNFAVORABLE)
  CC_Watch_Signal      : what to monitor before entering (if UNFAVORABLE)
  CC_Candidate_1/2/3   : JSON — best call strikes ranked by annualised yield
  CC_Best_DTE_Bucket   : WEEKLY | BIWEEKLY | MONTHLY (winning bucket)
  CC_Best_Ann_Yield    : annualised yield of top candidate (decimal, e.g. 0.18 = 18%)
  CC_IV_Rank           : IV_Rank used for the assessment
  CC_Regime            : Regime from scan output
  CC_Scan_TS           : timestamp of the scan file used

Book backing:
  McMillan Ch.3: "The best time to sell calls is when IV is high and trending down."
  Natenberg Ch.8: "Sell options when IV_Rank > 25% — edge is on the seller's side."
  Passarelli Ch.6: "Match DTE to income goal: weekly = max yield, monthly = max theta."
  Cohen Ch.7: "Buy-write works when the underlying trend is not aggressively bullish."
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Thresholds ────────────────────────────────────────────────────────────────
_IV_RANK_MIN          = 20.0    # below this → premium too thin (Natenberg Ch.8)
_IV_RANK_STRONG       = 40.0    # above this → strong CC environment
_IVHV_GAP_MIN         = 1.5     # IV must exceed HV by ≥1.5 pts (seller's edge)
_UNFAVORABLE_REGIMES  = {"Low Vol", "Compression", "Unknown"}  # thin premium regimes
_TREND_BLOCK          = {"Bullish"}  # strong uptrend → don't cap gains (McMillan Ch.3)

# Recovery-aware thresholds (Jabbour Ch.4: repair strategies for underwater positions)
_IV_RANK_MIN_RECOVERY          = 15.0   # thin premium still helps close gap
_RECOVERY_DRIFT_THRESHOLD      = -0.10  # drift < -10% → RECOVERY mode
_DEEP_RECOVERY_DRIFT_THRESHOLD = -0.25  # drift < -25% → DEEP_RECOVERY mode
_STRUCTURAL_DAMAGE_THRESHOLD   = -0.35  # drift < -35% → STRUCTURAL_DAMAGE (McMillan Ch.3: stop)
_HV_CAP_FOR_TIMELINE           = 1.00   # cap HV at 100% in recovery timeline (avoids fantasy)

# ── Tiered CC Ladder (partial coverage for large positions) ─────────────────
# RAG backing: Passarelli (0.705 — monthly credit aggregation),
# Jabbour (0.703 — ratio writes/partial coverage),
# Augen (0.704 — adaptive strike selection per cycle)
_LADDER_MIN_LOTS            = 10     # 1000 shares minimum for ladder eligibility
_LADDER_MAX_COVERAGE_SD     = 0.70   # STRUCTURAL_DAMAGE: max 70% covered
_LADDER_MAX_COVERAGE_REC    = 0.80   # RECOVERY/DEEP_RECOVERY: max 80%
_LADDER_MAX_COVERAGE_INCOME = 1.00   # INCOME: full coverage allowed

# Tier A: income harvesting — near-term, moderate delta
_TIER_A_DELTA_MIN   = 0.25
_TIER_A_DELTA_MAX   = 0.30   # capped at 0.30 for ALL modes (delta ≈ assignment prob)
_TIER_A_DTE_MIN     = 5
_TIER_A_DTE_MAX     = 14     # weekly/biweekly

# Tier B: recovery positioning — monthly, lower delta
_TIER_B_DELTA_MIN = 0.15
_TIER_B_DELTA_MAX = 0.25
_TIER_B_DTE_MIN   = 22
_TIER_B_DTE_MAX   = 50

# Allocation splits (of covered lots)
_TIER_A_PCT_RECOVERY = 0.35;  _TIER_B_PCT_RECOVERY = 0.65
_TIER_A_PCT_INCOME   = 0.40;  _TIER_B_PCT_INCOME   = 0.60

# Income-to-gap viability thresholds
_INCOME_GAP_RATIO_VIABLE = 0.02   # 2% of gap/month ≈ 50mo — viable
_INCOME_GAP_RATIO_WARN   = 0.01   # <1%/month — unrealistic, CASH_FLOW_ONLY

# Ladder guardrails
_MIN_PREMIUM_PER_CONTRACT = 0.10   # skip calls < $0.10/sh — economically meaningless
_LADDER_MAX_SPREAD_PCT    = 40.0   # wider than normal 20%: deeply OTM strikes have wider
                                   # spreads, but 40%+ is untradeable (bad fills, no liquidity)

# Non-ladder vetting thresholds (same standard as ladder for execution-readiness)
_NONLADDER_MIN_PREMIUM         = 0.10   # same as ladder _MIN_PREMIUM_PER_CONTRACT
_NONLADDER_SPREAD_CAP_INCOME   = 20.0   # income: 20% spread max (tighter, good fills)
_NONLADDER_SPREAD_CAP_RECOVERY = 40.0   # recovery: 40% (same as ladder, deeply OTM)
_NONLADDER_OI_MIN_INCOME       = 100    # income: OI >= 100
_NONLADDER_OI_MIN_RECOVERY     = 50     # recovery: OI >= 50 (relaxed)

# DTE buckets: (label, min_dte, max_dte, target_delta_max)
_DTE_BUCKETS = [
    ("WEEKLY",   5,  10, 0.25),
    ("BIWEEKLY", 11, 21, 0.28),
    ("MONTHLY",  22, 50, 0.30),
]

# Columns we need from Step12 output
_SCAN_COLS = [
    "Ticker", "Strategy_Name", "Execution_Status", "Signal_Type", "Regime",
    "IV_Rank_30D", "IV_Rank_20D", "IV_Rank", "IVHV_gap_30D",
    "Surface_Shape", "Confidence", "DQS_Score", "TQS_Score",
    "Actual_DTE", "Selected_Strike", "Mid_Price", "Delta",
    "Liquidity_Grade", "Bid_Ask_Spread_Pct", "Open_Interest",
    "Implied_Volatility", "Approx_Stock_Price", "last_price",
    "snapshot_ts", "IV_Trend_7D", "IV_Rank_Source",
]


def _find_latest_scan_file() -> Optional[Path]:
    """Return the most-recently-written Step12_Acceptance_*.csv."""
    output_dir = Path(__file__).parents[3] / "output"
    candidates = sorted(output_dir.glob("Step12_Acceptance_*.csv"), reverse=True)
    return candidates[0] if candidates else None


def _load_scan(path: Path) -> pd.DataFrame:
    """Load scan output, keeping only columns we need (graceful if absent)."""
    df = pd.read_csv(path, low_memory=False)
    present = [c for c in _SCAN_COLS if c in df.columns]
    return df[present].copy()


def _iv_rank(scan_row: pd.Series) -> Optional[float]:
    """Best available IV_Rank from scan row (30D > 20D > generic)."""
    for col in ("IV_Rank_30D", "IV_Rank_20D", "IV_Rank"):
        v = scan_row.get(col)
        if v is not None and not (isinstance(v, float) and np.isnan(v)):
            try:
                return float(v)
            except (ValueError, TypeError):
                pass
    return None


def _spot(pos_row: pd.Series, scan_row: Optional[pd.Series]) -> Optional[float]:
    """Current stock price — prefer management position row (live), fall back to scan."""
    for col in ("UL Last", "Last", "last_price"):
        v = pos_row.get(col)
        if v and not (isinstance(v, float) and np.isnan(v)):
            try:
                return float(v)
            except (ValueError, TypeError):
                pass
    if scan_row is not None:
        for col in ("last_price", "Approx_Stock_Price"):
            v = scan_row.get(col)
            if v and not (isinstance(v, float) and np.isnan(v)):
                try:
                    return float(v)
                except (ValueError, TypeError):
                    pass
    return None


def _basis(pos_row: pd.Series) -> Optional[float]:
    """Net cost basis per share — used for yield calculation."""
    for col in ("Net_Cost_Basis_Per_Share", "Basis_Per_Share", "Purchase_Price"):
        v = pos_row.get(col)
        if v and not (isinstance(v, float) and np.isnan(v)):
            try:
                f = float(v)
                return f if f > 0 else None
            except (ValueError, TypeError):
                pass
    # Derive from total basis / quantity
    total = pos_row.get("Basis") or pos_row.get("Cost Basis Total")
    qty   = pos_row.get("Quantity") or pos_row.get("Qty")
    if total and qty:
        try:
            t, q = float(total), float(qty)
            return abs(t / q) if q != 0 else None
        except (ValueError, TypeError):
            pass
    return None


def _classify_recovery_mode(pos_row: pd.Series) -> tuple[str, float]:
    """
    Classify position into recovery mode based on drift from cost basis.

    Returns (mode, drift_pct):
      INCOME:            profitable or small loss (drift > -10%)
      RECOVERY:          underwater -10% to -25%, thesis INTACT
      DEEP_RECOVERY:     underwater -25% to -35%, thesis INTACT
      STRUCTURAL_DAMAGE: underwater > -35% — CC writing blocked, redeploy capital

    BROKEN/DEGRADED thesis → forces INCOME (no gate relaxation; let Gate 1 handle).

    Book backing:
      Jabbour Ch.4: positions at -10% to -25% can be repaired via income generation
        if thesis remains intact. Below -35% is structural damage.
      McMillan Ch.3: don't sell calls on a position deeper than -35%.
      Given Ch.7: stop-loss should trigger near breakeven — don't keep writing hoping premium saves you.
      Nison: 'The loss has already been incurred; think about current risk, not sunk cost.'
    """
    basis = _basis(pos_row)
    if basis is None or basis <= 0:
        return "INCOME", 0.0

    spot = None
    for col in ("UL Last", "Last", "last_price"):
        v = pos_row.get(col)
        if v is not None and not (isinstance(v, float) and np.isnan(v)):
            try:
                spot = float(v)
                if spot > 0:
                    break
            except (ValueError, TypeError):
                pass
    if spot is None or spot <= 0:
        return "INCOME", 0.0

    drift = (spot - basis) / basis

    thesis = str(pos_row.get("Thesis_State") or "INTACT").upper()
    if thesis in ("DEGRADED", "BROKEN"):
        return "INCOME", drift

    if drift < _STRUCTURAL_DAMAGE_THRESHOLD:
        return "STRUCTURAL_DAMAGE", drift
    elif drift < _DEEP_RECOVERY_DRIFT_THRESHOLD:
        return "DEEP_RECOVERY", drift
    elif drift < _RECOVERY_DRIFT_THRESHOLD:
        return "RECOVERY", drift
    else:
        return "INCOME", drift


def _compute_recovery_timeline(spot: float, basis: float, hv: float) -> dict:
    """
    Compute recovery timeline metrics for an underwater position.

    Returns dict with:
      gap:          gap to breakeven in $/share
      monthly_est:  estimated monthly premium income (HV-based)
      months:       months of rolling to close the gap

    Book backing:
      Natenberg Ch.8: ATM call premium ≈ 0.4 × σ × S / √T_annual
      McMillan Ch.3: recovery timeline must be credible, not aspirational

    HV is capped at 100% to avoid fantasy projections — extreme HV (200%+)
    inflates the Natenberg approximation beyond what you can actually collect.
    """
    gap = max(0.0, basis - spot)
    if gap <= 0 or hv <= 0 or spot <= 0:
        return {"gap": 0.0, "monthly_est": 0.0, "months": 0.0}

    # Cap HV: extreme realized vol doesn't translate to collectible premium.
    # A stock with 200% HV has wild swings — the premium is high but so is
    # assignment/gap risk. Cap at 100% for conservative estimate.
    hv_capped = min(hv, _HV_CAP_FOR_TIMELINE)

    # Natenberg Ch.8: ATM call ≈ 0.4 × σ × S / √T_annual.
    # But CC writers sell OTM (delta ~0.25-0.30), not ATM.
    # OTM premium ≈ 25-35% of ATM for typical deltas — use 0.30 as
    # conservative multiplier to avoid fantasy recovery timelines.
    # Additional 0.85 haircut for bid/ask slippage on fill.
    _atm_weekly = 0.4 * hv_capped * spot / (52 ** 0.5)
    _otm_factor = 0.30       # OTM delta ~0.25-0.30 captures ~30% of ATM
    _fill_haircut = 0.85     # realistic fill vs mid
    weekly_est = _atm_weekly * _otm_factor * _fill_haircut
    monthly_est = weekly_est * 4.3

    if monthly_est <= 0.01:
        months = 999.0
    else:
        months = round(gap / monthly_est, 1)

    return {
        "gap": round(gap, 2),
        "monthly_est": round(monthly_est, 2),
        "months": months,
    }


def _compute_ladder_allocation(
    qty: float,
    recovery_mode: str,
    thesis_state: str = "INTACT",
) -> Optional[dict]:
    """
    Compute tiered CC ladder allocation for a large stock position.

    Returns None if ineligible (qty < 1000 or thesis BROKEN/DEGRADED).
    Otherwise returns dict with lot allocation per tier.

    Book backing:
      Jabbour Ch.4: ratio writes / partial coverage — never cover all shares
        when underwater. Keep 30%+ uncovered for upside participation.
      Passarelli Ch.6: split coverage across time horizons for credit aggregation.
    """
    total_lots = int(qty // 100)
    if total_lots < _LADDER_MIN_LOTS:
        return None

    thesis = thesis_state.upper() if thesis_state else "INTACT"
    if thesis in ("BROKEN", "DEGRADED"):
        return None

    # Max coverage depends on recovery mode
    if recovery_mode == "STRUCTURAL_DAMAGE":
        max_cov_pct = _LADDER_MAX_COVERAGE_SD
    elif recovery_mode in ("RECOVERY", "DEEP_RECOVERY"):
        max_cov_pct = _LADDER_MAX_COVERAGE_REC
    else:
        max_cov_pct = _LADDER_MAX_COVERAGE_INCOME

    covered_lots = int(total_lots * max_cov_pct)
    uncovered_lots = total_lots - covered_lots

    # Tier split: recovery 35/65 A/B, income 40/60 A/B
    if recovery_mode in ("RECOVERY", "DEEP_RECOVERY", "STRUCTURAL_DAMAGE"):
        tier_a_pct = _TIER_A_PCT_RECOVERY
        tier_b_pct = _TIER_B_PCT_RECOVERY
    else:
        tier_a_pct = _TIER_A_PCT_INCOME
        tier_b_pct = _TIER_B_PCT_INCOME

    tier_a_lots = max(1, round(covered_lots * tier_a_pct))
    tier_b_lots = max(1, covered_lots - tier_a_lots)
    # Adjust if rounding inflated total
    if tier_a_lots + tier_b_lots > covered_lots:
        tier_b_lots = covered_lots - tier_a_lots

    return {
        "total_lots": total_lots,
        "max_coverage_pct": max_cov_pct,
        "covered_lots": covered_lots,
        "tier_a_lots": tier_a_lots,
        "tier_b_lots": tier_b_lots,
        "uncovered_lots": uncovered_lots,
    }


def _build_ladder_candidates_live(
    ticker: str,
    spot: float,
    basis: float,
    hv: float,
    schwab_client,
    recovery_mode: str,
) -> dict:
    """
    Fetch tiered CC ladder candidates from Schwab chain API.

    Returns {"tier_a_candidates": [...], "tier_b_candidates": [...], "source": "LIVE_CHAIN"}

    Guardrails:
      - Strike floor: STRUCTURAL_DAMAGE → spot × 1.10 (basis is unreachable;
          uncovered lots handle rally). Others → max(spot × 1.10, cost_basis).
      - Min premium: $0.10/sh (skip economically meaningless calls)
      - Spread cap: 40% (untradeable fill quality above this)
      - Delta cap: Tier A ≤ 0.30, Tier B ≤ 0.25
    """
    from datetime import date

    result = {"tier_a_candidates": [], "tier_b_candidates": [], "source": "LIVE_CHAIN"}
    # SD-ladder: spot-anchored floor (basis unreachable; uncovered lots = rally protection)
    # Others: basis-anchored floor (protect breakeven)
    if recovery_mode == "STRUCTURAL_DAMAGE":
        strike_floor = spot * 1.10
    else:
        strike_floor = max(spot * 1.10, basis) if basis > 0 else spot * 1.10

    try:
        schwab_client.ensure_valid_token()
        chain = schwab_client.get_chains(
            symbol=ticker,
            strikeCount=30,
            range="OTM",
            strategy="SINGLE",
        )
    except Exception as e:
        logger.warning(f"[CCLadder-Live] Chain fetch failed for {ticker}: {e}")
        return result

    if not chain or "callExpDateMap" not in chain:
        return result

    today = date.today()

    for exp_str, strikes_map in chain["callExpDateMap"].items():
        try:
            exp_date_str = exp_str.split(":")[0]
            exp_date = date.fromisoformat(exp_date_str)
            dte = (exp_date - today).days
        except Exception:
            continue

        # Determine tier from DTE
        tier = None
        delta_min, delta_max = 0.0, 0.0
        if _TIER_A_DTE_MIN <= dte <= _TIER_A_DTE_MAX:
            tier = "A"
            delta_min, delta_max = _TIER_A_DELTA_MIN, _TIER_A_DELTA_MAX
        elif _TIER_B_DTE_MIN <= dte <= _TIER_B_DTE_MAX:
            tier = "B"
            delta_min, delta_max = _TIER_B_DELTA_MIN, _TIER_B_DELTA_MAX
        else:
            continue

        for strike_str, contracts in strikes_map.items():
            try:
                strike = float(strike_str)
            except ValueError:
                continue

            if strike < strike_floor:
                continue

            contract = contracts[0] if isinstance(contracts, list) else contracts
            try:
                bid   = float(contract.get("bid") or 0)
                ask   = float(contract.get("ask") or 0)
                mid   = (bid + ask) / 2 if bid > 0 and ask > 0 else 0.0
                delta = abs(float(contract.get("delta") or 0))
                oi    = int(contract.get("openInterest") or 0)
                vol   = int(contract.get("totalVolume") or 0)
                iv    = float(contract.get("volatility") or 0)
                iv_dec = iv / 100.0 if iv > 5 else iv
            except Exception:
                continue

            # Min premium gate
            if mid < _MIN_PREMIUM_PER_CONTRACT:
                continue

            # Delta range filter
            if delta < delta_min or delta > delta_max:
                continue

            # Spread cap
            spread_pct = ((ask - bid) / mid * 100) if mid > 0 else 999.0
            if spread_pct > _LADDER_MAX_SPREAD_PCT:
                continue

            # Liquidity: require OI ≥ 50 or volume ≥ 10 for ladder (relaxed)
            if oi < 50 and vol < 10:
                continue

            # Annualised yield on cost basis
            _eff_basis = basis if basis > 0 else spot
            ann_y = _ann_yield(mid, _eff_basis, dte) if mid > 0 and dte > 0 else 0.0

            # Liquidity grade (same thresholds as regular CC path)
            if oi >= 500 or vol >= 100:
                liq = "GOOD"
            elif oi >= 100 or vol >= 20:
                liq = "OK"
            else:
                liq = "THIN"

            cand = {
                "tier":       tier,
                "bucket":     "WEEKLY" if tier == "A" else "MONTHLY",
                "strike":     round(strike, 2),
                "dte":        dte,
                "expiry":     exp_date_str,
                "mid":        round(mid, 2),
                "bid":        round(bid, 2),
                "ask":        round(ask, 2),
                "delta":      round(delta, 3),
                "ann_yield":  round(ann_y, 4),
                "liq":        liq,
                "spread_pct": round(spread_pct, 1),
                "oi":         oi,
                "iv_pct":     round(iv_dec * 100, 1),
                "source":     "LIVE_CHAIN",
            }

            if tier == "A":
                result["tier_a_candidates"].append(cand)
            else:
                result["tier_b_candidates"].append(cand)

    # Sort each tier by mid descending (best premium first)
    result["tier_a_candidates"].sort(key=lambda x: x["mid"], reverse=True)
    result["tier_b_candidates"].sort(key=lambda x: x["mid"], reverse=True)

    return result


def _build_ladder_candidates_scan(
    scan_df: pd.DataFrame,
    ticker: str,
    basis: float,
    spot: float,
    recovery_mode: str,
) -> dict:
    """
    Build tiered CC ladder candidates from scan CSV data.

    Same return structure and guardrails as live version.
    SD-ladder: spot-anchored floor. Others: basis-anchored floor.
    """
    from datetime import date, timedelta
    result = {"tier_a_candidates": [], "tier_b_candidates": [], "source": "SCAN_DATA"}
    if recovery_mode == "STRUCTURAL_DAMAGE":
        strike_floor = spot * 1.10
    else:
        strike_floor = max(spot * 1.10, basis) if basis > 0 else spot * 1.10

    if scan_df.empty or "Ticker" not in scan_df.columns:
        return result

    mask = (
        (scan_df["Ticker"] == ticker)
        & scan_df.get("Strategy_Name", pd.Series("", index=scan_df.index)).str.upper().isin(
            {"COVERED CALL", "COVERED_CALL", "BUY-WRITE", "BUY_WRITE", "CC"}
        )
        & scan_df.get("Execution_Status", pd.Series("", index=scan_df.index)).isin(
            {"READY", "CONDITIONAL", "REVIEW"}
        )
    )
    subset = scan_df[mask].copy() if mask.any() else pd.DataFrame()
    if subset.empty:
        return result

    for _, row in subset.iterrows():
        try:
            strike = float(row.get("Selected_Strike") or 0)
            dte    = int(row.get("Actual_DTE") or 0)
            mid    = float(row.get("Mid_Price") or 0)
            delta  = abs(float(row.get("Delta") or 0))
            spread = float(row.get("Bid_Ask_Spread_Pct") or 0)
            oi     = int(row.get("Open_Interest") or 0)
            iv     = float(row.get("Implied_Volatility") or 0)
        except Exception:
            continue

        # Strike floor
        if strike < strike_floor:
            continue

        # Min premium gate
        if mid < _MIN_PREMIUM_PER_CONTRACT:
            continue

        # Spread cap
        if spread > _LADDER_MAX_SPREAD_PCT:
            continue

        # Determine tier from DTE + delta
        tier = None
        if _TIER_A_DTE_MIN <= dte <= _TIER_A_DTE_MAX:
            if _TIER_A_DELTA_MIN <= delta <= _TIER_A_DELTA_MAX:
                tier = "A"
        elif _TIER_B_DTE_MIN <= dte <= _TIER_B_DTE_MAX:
            if _TIER_B_DELTA_MIN <= delta <= _TIER_B_DELTA_MAX:
                tier = "B"

        if tier is None:
            continue

        # Annualised yield on cost basis
        _eff_basis_s = basis if basis > 0 else spot
        ann_y = _ann_yield(mid, _eff_basis_s, dte) if mid > 0 and dte > 0 else 0.0

        # Liquidity grade from scan data
        liq_scan = str(row.get("Liquidity_Grade") or "")
        if not liq_scan:
            liq_scan = "GOOD" if oi >= 500 else ("OK" if oi >= 100 else "THIN")

        # Derive expiry from DTE
        _exp_date_s = (date.today() + timedelta(days=dte)).isoformat() if dte > 0 else ""
        # Derive bid/ask from mid and spread_pct
        _half_spread = mid * (spread / 100.0) / 2 if spread > 0 and mid > 0 else 0.0
        _bid_s = round(max(0, mid - _half_spread), 2)
        _ask_s = round(mid + _half_spread, 2)

        cand = {
            "tier":       tier,
            "bucket":     "WEEKLY" if tier == "A" else "MONTHLY",
            "strike":     round(strike, 2),
            "dte":        dte,
            "expiry":     _exp_date_s,
            "mid":        round(mid, 2),
            "bid":        _bid_s,
            "ask":        _ask_s,
            "delta":      round(delta, 3),
            "ann_yield":  round(ann_y, 4),
            "liq":        liq_scan,
            "spread_pct": round(spread, 1),
            "oi":         oi,
            "iv_pct":     round(iv * 100, 1) if iv < 5 else round(iv, 1),
            "source":     "SCAN_DATA",
        }

        if tier == "A":
            result["tier_a_candidates"].append(cand)
        else:
            result["tier_b_candidates"].append(cand)

    result["tier_a_candidates"].sort(key=lambda x: x["mid"], reverse=True)
    result["tier_b_candidates"].sort(key=lambda x: x["mid"], reverse=True)

    return result


def _compute_income_gap_ratio(
    tier_a_cands: list[dict],
    tier_b_cands: list[dict],
    tier_a_lots: int,
    tier_b_lots: int,
    gap_total: float,
) -> tuple[float, float, str]:
    """
    Compute income-to-gap ratio: monthly_income / gap_total.

    Returns (monthly_income, ratio, note):
      ratio < 0.01 → "CASH_FLOW_ONLY — cannot realistically repair"
      ratio < 0.02 → "PARTIAL_REPAIR — marginal, frame as cash flow + partial repair"
      ratio ≥ 0.02 → "RECOVERY_VIABLE — ~N months to close gap"
    """
    monthly_income = 0.0

    # Tier A: annualise to monthly using (30/dte) factor
    if tier_a_cands and tier_a_lots > 0:
        best_a = tier_a_cands[0]
        dte_a = max(best_a.get("dte", 7), 1)
        monthly_a = best_a["mid"] * 100 * tier_a_lots * (30.0 / dte_a)
        monthly_income += monthly_a

    # Tier B: same formula
    if tier_b_cands and tier_b_lots > 0:
        best_b = tier_b_cands[0]
        dte_b = max(best_b.get("dte", 30), 1)
        monthly_b = best_b["mid"] * 100 * tier_b_lots * (30.0 / dte_b)
        monthly_income += monthly_b

    if gap_total <= 0:
        return monthly_income, 0.0, "NO_GAP"

    ratio = monthly_income / gap_total

    if ratio < _INCOME_GAP_RATIO_WARN:
        note = "CASH_FLOW_ONLY"
    elif ratio < _INCOME_GAP_RATIO_VIABLE:
        note = "PARTIAL_REPAIR"
    else:
        note = "RECOVERY_VIABLE"

    return round(monthly_income, 2), round(ratio, 4), note


def _build_ladder_plan(
    pos_row: pd.Series,
    allocation: dict,
    tier_a_cands: list[dict],
    tier_b_cands: list[dict],
    recovery_mode: str,
    override_qty: float = 0.0,
) -> dict:
    """
    Orchestrate the full ladder plan: allocation + candidates + income-gap analysis.

    Returns dict with all ladder fields for both JSON storage and flat columns.
    override_qty: use aggregate shares when BUY_WRITE stock is split across multiple rows.
    """
    spot_px = _spot(pos_row, None) or 0.0
    basis_px = _basis(pos_row) or 0.0
    gap = max(0.0, basis_px - spot_px)
    qty = override_qty if override_qty > 0 else float(pos_row.get("Quantity") or pos_row.get("Qty") or 0)
    gap_total = gap * qty if gap > 0 else 0.0

    monthly_income, ratio, framing = _compute_income_gap_ratio(
        tier_a_cands, tier_b_cands,
        allocation["tier_a_lots"], allocation["tier_b_lots"],
        gap_total,
    )

    # Recovery months estimate
    recovery_months = round(gap_total / monthly_income, 1) if monthly_income > 0 else 999.0

    # Cost basis reduction: annual premium / total cost basis
    # More meaningful than gap ratio for STRUCTURAL_DAMAGE positions where
    # the goal is to reduce basis over time, not close the gap to breakeven.
    cost_basis_total = basis_px * qty if basis_px > 0 and qty > 0 else 0.0
    annual_income = monthly_income * 12
    cost_basis_reduction_annual = (
        round(annual_income / cost_basis_total, 4) if cost_basis_total > 0 else 0.0
    )
    basis_after_1yr = round(basis_px - annual_income / qty, 2) if qty > 0 else basis_px

    # Strike floor: SD-ladder uses spot-anchored (basis is unreachable)
    if recovery_mode == "STRUCTURAL_DAMAGE":
        _floor = round(spot_px * 1.10, 2)
    else:
        _floor = round(max(spot_px * 1.10, basis_px), 2) if basis_px > 0 else 0.0

    plan = {
        "recovery_mode": recovery_mode,
        "framing": framing,
        "total_lots": allocation["total_lots"],
        "covered_lots": allocation["covered_lots"],
        "uncovered_lots": allocation["uncovered_lots"],
        "max_coverage_pct": allocation["max_coverage_pct"],
        "tier_a_lots": allocation["tier_a_lots"],
        "tier_b_lots": allocation["tier_b_lots"],
        "tier_a_best": tier_a_cands[0] if tier_a_cands else None,
        "tier_b_best": tier_b_cands[0] if tier_b_cands else None,
        "monthly_income_est": monthly_income,
        "income_gap_ratio": ratio,
        "recovery_months_est": recovery_months,
        "gap_per_share": round(gap, 2),
        "gap_total": round(gap_total, 2),
        "strike_floor": _floor,
        "cost_basis_total": round(cost_basis_total, 2),
        "cost_basis_reduction_annual": cost_basis_reduction_annual,
        "basis_after_1yr": basis_after_1yr,
    }

    return plan


def _query_iv_percentile_from_history(ticker: str) -> Optional[float]:
    """
    Query iv_term_history directly for IV percentile when scan data is stale.
    Used as fallback when IV_Rank is unavailable during CC evaluation.

    Returns IV percentile (0-100) or None if insufficient history.
    Non-blocking: any exception returns None.
    """
    try:
        from core.shared.data_contracts.config import IV_HISTORY_DB_PATH
        import duckdb as _duckdb

        if not IV_HISTORY_DB_PATH.exists():
            return None

        con = _duckdb.connect(str(IV_HISTORY_DB_PATH), read_only=True)
        try:
            result = con.execute("""
                WITH latest AS (
                    SELECT ticker, iv_30d AS current_iv
                    FROM iv_term_history
                    WHERE ticker = ?
                      AND iv_30d IS NOT NULL
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) = 1
                ),
                history AS (
                    SELECT ticker, iv_30d
                    FROM iv_term_history
                    WHERE ticker = ?
                      AND iv_30d IS NOT NULL
                )
                SELECT
                    CAST(SUM(CASE WHEN h.iv_30d < l.current_iv THEN 1 ELSE 0 END) AS DOUBLE)
                        / COUNT(*) * 100.0 AS iv_pctile,
                    COUNT(*) AS depth
                FROM latest l
                JOIN history h ON l.ticker = h.ticker
                GROUP BY l.ticker, l.current_iv
                HAVING COUNT(*) >= 20
            """, [ticker, ticker]).fetchone()

            if result and result[0] is not None:
                logger.info(
                    f"[CCOpportunity-IV] {ticker}: IV percentile={result[0]:.1f}% "
                    f"(depth={result[1]}d) from iv_term_history"
                )
                return float(result[0])
            return None
        finally:
            con.close()
    except Exception as e:
        logger.debug(f"[CCOpportunity-IV] iv_term_history query failed for {ticker}: {e}")
        return None


def _ann_yield(premium_per_share: float, basis_per_share: float, dte: int) -> float:
    """Annualised yield: (premium / basis) × (365 / dte)."""
    if basis_per_share <= 0 or dte <= 0:
        return 0.0
    return (premium_per_share / basis_per_share) * (365.0 / dte)


def _favorability_check(
    iv_rank: Optional[float],
    regime: str,
    signal: str,
    ivhv_gap: Optional[float],
    recovery_mode: str = "INCOME",
    ticker: str = "",
) -> tuple[bool, str, str]:
    """
    Returns (is_favorable, unfavorable_reason, watch_signal).
    All three returned regardless of outcome — caller decides what to surface.

    recovery_mode: INCOME | RECOVERY | DEEP_RECOVERY
      RECOVERY/DEEP_RECOVERY lowers IV_Rank threshold from 20% → 15%
      (Jabbour Ch.4: thin premium still helps close the gap on underwater positions)
    """
    reasons = []
    watches = []

    # IV_Rank gate (Natenberg Ch.8)
    # Recovery mode: lower threshold — any income generation accelerates recovery
    _iv_min = _IV_RANK_MIN_RECOVERY if recovery_mode in ("RECOVERY", "DEEP_RECOVERY") else _IV_RANK_MIN
    if iv_rank is None:
        reasons.append("IV_Rank unavailable (insufficient history)")
        watches.append("Wait for 30+ days of IV history to accumulate")
    elif iv_rank < _iv_min:
        reasons.append(f"IV_Rank={iv_rank:.0f}% — premium too thin (need >{_iv_min:.0f}%)")
        watches.append(f"IV_Rank crossing {_iv_min:.0f}% upward; IV_ROC turning positive")

    # Regime gate
    if regime in _UNFAVORABLE_REGIMES:
        reasons.append(f"Regime={regime} — vol compressed, option sellers at disadvantage")
        watches.append("Regime shift to High Vol or Elevated")

    # IVHV gap gate (seller's edge)
    if ivhv_gap is not None and ivhv_gap < _IVHV_GAP_MIN:
        reasons.append(
            f"IV/HV gap={ivhv_gap:.1f}pt — IV not sufficiently rich vs realized vol "
            f"(need ≥{_IVHV_GAP_MIN}pt)"
        )
        watches.append(f"IVHV gap widening above {_IVHV_GAP_MIN}pt")

    # Trend gate (McMillan Ch.3: don't cap a strongly bullish position)
    if signal in _TREND_BLOCK:
        reasons.append(
            f"Signal={signal} — strong uptrend; selling calls risks capping gains. "
            "Consider waiting for neutral/pullback before writing calls."
        )
        watches.append("Signal shifting to Neutral or Bearish (pullback to SMA20)")

    # ETF-specific watch signal (appended, not replacing)
    if reasons and ticker:
        from config.sector_benchmarks import is_etf as _is_etf_fav
        if _is_etf_fav(ticker):
            watches.append(
                "ETF: no earnings risk — HV mean-reversion typically faster. "
                "Monitor HV_20D trend"
            )

    is_favorable = len(reasons) == 0
    unfavorable_reason = " | ".join(reasons) if reasons else ""
    watch_signal       = " | ".join(watches) if watches else ""
    return is_favorable, unfavorable_reason, watch_signal


def _rank_candidates(
    scan_df: pd.DataFrame,
    ticker: str,
    basis_per_share: float,
    recovery_mode: str = "INCOME",
    spot_price: float = 0.0,
    qty: float = 0.0,
) -> list[dict]:
    """
    From scan rows for this ticker, rank CC candidates by annualised yield
    across DTE buckets. Returns up to 3 best candidates as dicts.

    recovery_mode: RECOVERY/DEEP_RECOVERY widens MONTHLY delta cap to 0.35
    and enforces strike floor = max(spot × 1.10, cost_basis).
    """
    # Filter to CC/BUY_WRITE nominations for this ticker that are READY/CONDITIONAL
    mask = (
        (scan_df["Ticker"] == ticker)
        & scan_df.get("Strategy_Name", pd.Series("", index=scan_df.index)).str.upper().isin(
            {"COVERED CALL", "COVERED_CALL", "BUY-WRITE", "BUY_WRITE", "CC"}
        )
        & scan_df.get("Execution_Status", pd.Series("", index=scan_df.index)).isin(
            {"READY", "CONDITIONAL", "REVIEW"}
        )
    ) if "Ticker" in scan_df.columns else pd.Series(False, index=scan_df.index)

    subset = scan_df[mask].copy() if mask.any() else pd.DataFrame()

    candidates = []

    # Recovery-aware DTE buckets: widen MONTHLY delta cap for more premium
    _dte_buckets = list(_DTE_BUCKETS)
    if recovery_mode in ("RECOVERY", "DEEP_RECOVERY"):
        _dte_buckets = [
            ("WEEKLY",   5,  10, 0.25),   # unchanged — weekly too risky in recovery
            ("BIWEEKLY", 11, 21, 0.30),   # slightly wider
            ("MONTHLY",  22, 50, 0.35),   # accept more premium for recovery
        ]

    # Recovery strike floor: never cap below cost basis
    _strike_floor = 0.0
    if recovery_mode in ("RECOVERY", "DEEP_RECOVERY") and spot_price > 0:
        _strike_floor = max(spot_price * 1.10, basis_per_share)

    for bucket_label, min_dte, max_dte, delta_cap in _dte_buckets:
        # Find rows in this DTE bucket
        if subset.empty or "Actual_DTE" not in subset.columns:
            break
        bucket_rows = subset[
            subset["Actual_DTE"].between(min_dte, max_dte, inclusive="both")
        ]
        if "Delta" in bucket_rows.columns:
            bucket_rows = bucket_rows[
                bucket_rows["Delta"].abs() <= delta_cap
            ]
        if bucket_rows.empty:
            continue

        # Pick highest Confidence × DQS_Score within bucket
        score_col = "DQS_Score" if "DQS_Score" in bucket_rows.columns else "Confidence"
        best = bucket_rows.sort_values(score_col, ascending=False).iloc[0]

        mid   = float(best.get("Mid_Price") or 0)
        dte   = int(best.get("Actual_DTE") or 0)
        strike= float(best.get("Selected_Strike") or 0)

        # Recovery strike floor: skip candidates that would cap below cost basis
        if _strike_floor > 0 and strike < _strike_floor:
            continue
        delta = float(best.get("Delta") or 0)
        liq   = str(best.get("Liquidity_Grade") or "")
        spread= float(best.get("Bid_Ask_Spread_Pct") or 0)
        oi    = int(best.get("Open_Interest") or 0)
        iv    = float(best.get("Implied_Volatility") or 0)
        conf  = float(best.get("Confidence") or 0)
        dqs   = float(best.get("DQS_Score") or 0)

        # ── Vetting gates (same standard as ladder) ────────────────
        # Min premium gate
        if mid < _NONLADDER_MIN_PREMIUM:
            continue

        # Spread cap (mode-aware)
        _spread_cap = (_NONLADDER_SPREAD_CAP_RECOVERY
                       if recovery_mode in ("RECOVERY", "DEEP_RECOVERY", "STRUCTURAL_DAMAGE")
                       else _NONLADDER_SPREAD_CAP_INCOME)
        if spread > _spread_cap:
            continue

        # OI / liquidity gate
        _oi_min = (_NONLADDER_OI_MIN_RECOVERY
                   if recovery_mode in ("RECOVERY", "DEEP_RECOVERY", "STRUCTURAL_DAMAGE")
                   else _NONLADDER_OI_MIN_INCOME)
        if not liq:
            liq = "GOOD" if oi >= 500 else ("OK" if oi >= _oi_min else "THIN")
        if liq == "THIN" and oi < _oi_min:
            continue

        ann_y = _ann_yield(mid, basis_per_share, dte) if mid > 0 and dte > 0 else 0.0

        # ── Execution fields ───────────────────────────────────────
        from datetime import date, timedelta
        _exp_date_r = (date.today() + timedelta(days=dte)).isoformat() if dte > 0 else ""

        # Prefer real bid/ask from scan; reconstruct from spread only as fallback
        _raw_bid = float(best.get("Bid") or 0)
        _raw_ask = float(best.get("Ask") or 0)
        if _raw_bid > 0 and _raw_ask > 0:
            _bid_r = round(_raw_bid, 2)
            _ask_r = round(_raw_ask, 2)
        else:
            _half_spread = mid * (spread / 100.0) / 2 if spread > 0 and mid > 0 else 0.0
            _bid_r = round(max(0, mid - _half_spread), 2)
            _ask_r = round(mid + _half_spread, 2)

        _contracts = max(1, int(qty // 100)) if qty >= 100 else 1

        candidates.append({
            "bucket":       bucket_label,
            "strike":       round(strike, 2),
            "dte":          dte,
            "expiry":       _exp_date_r,
            "mid":          round(mid, 2),
            "bid":          _bid_r,
            "ask":          _ask_r,
            "delta":        round(abs(delta), 3),
            "ann_yield":    round(ann_y, 4),
            "liq":          liq,
            "spread_pct":   round(spread, 2),
            "oi":           oi,
            "iv_pct":       round(iv * 100, 1) if iv < 5 else round(iv, 1),
            "confidence":   round(conf, 1),
            "dqs":          round(dqs, 1),
            "source":       "SCAN_DATA",
            "contracts":    _contracts,
        })

    # Sort by annualised yield descending; return top 3
    candidates.sort(key=lambda x: x["ann_yield"], reverse=True)
    return candidates[:3]


def _cc_arbitration(pos_row: pd.Series, is_fav_scan: bool, unfav_reason: str, chain_iv: float = 0.0, recovery_mode: str = "INCOME") -> tuple[str, str]:
    """
    HOLD_STOCK vs WRITE_CALL vs MONITOR arbitration.

    This is the management override layer — it answers whether selling convexity
    is CURRENTLY superior to holding it. The scan/chain only answers "are strikes
    available?".  This function answers the harder question.

    Four gates (applied sequentially — first failure = HOLD_STOCK or MONITOR):

    Gate 1 — Structural: triage must be HEALTHY or RECOVERY.
      CRITICAL positions: no calls. Selling a call caps upside on a broken position
      and delays the cut/recover decision. (McMillan Ch.3)

    Gate 2 — Vol Edge: IV must exceed HV (seller's edge present).
      IV < HV → realized vol exceeds implied → selling calls is structurally negative EV.
      (Natenberg Ch.7: "Never sell options when IV < HV — you are giving edge away.")
      Special case: backwardation (short-term IV spike) — near-term premiums are elevated
      but mean-reversion risk is very high. Requires extra OTM buffer. (McMillan Ch.3)

    Gate 3 — Directional Conviction: stock must not be in strong uptrend.
      Aggressively bullish regime → selling calls caps a position that should be held
      for directional gain. Wait for neutral/pullback. (McMillan Ch.3, Cohen Ch.7)

    Gate 4 — Opportunity Cost: theta earned must be meaningful vs stock volatility.
      If HV_Daily_Move (1σ daily) > 3× expected premium/day → stock moves more than
      the premium collects → no edge. MONITOR rather than WRITE_CALL.

    Returns: (verdict, reason)
      verdict: 'WRITE_CALL' | 'HOLD_STOCK' | 'MONITOR'
      reason:  human-readable explanation
    """
    # ── Gate 1: Structural (triage) ───────────────────────────────────────────
    # Derive triage from position drift and thesis
    drift_pct = float(pos_row.get("Price_Drift_Pct") or 0)
    thesis    = str(pos_row.get("Thesis_State") or "INTACT").upper()
    if drift_pct < -0.35 or thesis in ("DEGRADED", "BROKEN"):
        return (
            "HOLD_STOCK",
            f"Gate 1: CRITICAL triage (drift={drift_pct:+.1%}, thesis={thesis}) — "
            "do not sell calls on a broken position. Resolve capital risk first. "
            "(McMillan Ch.3: 'Don't sell calls hoping premium saves a losing position.')"
        )

    # ── Gate 2: Vol Edge ──────────────────────────────────────────────────────
    hv     = float(pos_row.get("HV_20D") or 0)
    iv_30d = float(pos_row.get("IV_30D") or 0) if str(pos_row.get("IV_30D","")) not in ("nan","None","") else 0.0
    iv_entry= float(pos_row.get("IV_Entry") or 0) if str(pos_row.get("IV_Entry","")) not in ("nan","None","") else 0.0
    iv_ref = iv_30d if iv_30d > 0 else iv_entry if iv_entry > 0 else chain_iv
    iv_surf = str(pos_row.get("iv_surface_shape") or "")

    # Read ticker once for ETF context (used in Gate 2 and Gate 4)
    _arb_ticker = str(pos_row.get("Underlying_Ticker") or pos_row.get("Symbol") or "")

    if hv > 0 and iv_ref > 0:
        if iv_ref < hv * 0.95:
            # IV clearly below HV → selling calls is negative EV
            _g2_reason = (
                f"Gate 2: IV({iv_ref*100:.0f}%) < HV({hv*100:.0f}%) — realized vol exceeds "
                "implied. Selling calls is structurally negative EV. "
                "(Natenberg Ch.7: wait for IV to rise above HV.)"
            )
            # ETF context: macro-driven HV mean-reverts faster
            from config.sector_benchmarks import is_etf as _is_etf, is_commodity_etf as _is_commodity_etf
            if _is_etf(_arb_ticker):
                _g2_reason += (
                    " ETF context: no earnings risk. "
                    "Macro-driven HV tends to mean-revert faster than single-stock HV — "
                    "monitor HV_20D trend for crossover signal."
                )
                if _is_commodity_etf(_arb_ticker):
                    _g2_reason += " Commodity ETFs show pronounced macro-flow vol cycles."
            return ("HOLD_STOCK", _g2_reason)
        elif iv_surf == "BACKWARDATION":
            # Backwardation: near-term IV spike — premiums elevated but gamma risk extreme
            # Don't block entirely (premium IS rich) but require MONTHLY+ DTE
            return (
                "MONITOR",
                f"Gate 2: BACKWARDATION — IV({iv_ref*100:.0f}%) vs HV({hv*100:.0f}%). "
                "Near-term premium elevated but mean-reversion gamma risk is high. "
                "Prefer 30–45d monthly expiration to avoid single-day blowup. "
                "(McMillan Ch.3: in backwardation, use longer DTE to absorb vol spikes.)"
            )
    elif hv > 0.60 and iv_ref == 0:
        # No IV data but HV is extreme — try iv_term_history before returning MONITOR
        ticker = str(pos_row.get("Underlying_Ticker") or pos_row.get("Symbol") or "")
        _hist_iv_pctile = None
        if ticker and recovery_mode in ("RECOVERY", "DEEP_RECOVERY"):
            try:
                _hist_iv_pctile = _query_iv_percentile_from_history(ticker)
            except Exception:
                pass

        if _hist_iv_pctile is not None and _hist_iv_pctile >= _IV_RANK_MIN_RECOVERY:
            return (
                "WRITE_CALL",
                f"Gate 2: HV={hv*100:.0f}% extreme, but IV_Percentile={_hist_iv_pctile:.0f}% "
                f"from iv_term_history confirms seller's edge for recovery CC. "
                "(Natenberg Ch.8: IV history used when live scan unavailable.)"
            )
        else:
            return (
                "MONITOR",
                f"Gate 2: HV={hv*100:.0f}% extreme but IV unavailable"
                f"{f' (IV_Pctile={_hist_iv_pctile:.0f}% too low)' if _hist_iv_pctile is not None else ''}. "
                "Cannot confirm seller's edge. Run pipeline during market hours for IV_Rank."
            )

    # ── Gate 3: Directional Conviction ────────────────────────────────────────
    # Use chart state signals from management engine
    trend_state = str(pos_row.get("TrendIntegrity_State") or "").upper()
    mom_state   = str(pos_row.get("MomentumVelocity_State") or "").upper()
    roc_5       = float(pos_row.get("roc_5") or 0)
    adx         = float(pos_row.get("adx_14") or 0)

    # Aggressive uptrend: ADX > 30 + positive 5-day ROC → capping a winner
    if adx > 30 and roc_5 > 0.05:
        return (
            "MONITOR",
            f"Gate 3: Strong uptrend (ADX={adx:.0f}, ROC5={roc_5:+.1%}) — "
            "selling calls risks capping directional gain. "
            "Wait for pullback to SMA20 before writing. (McMillan Ch.3, Cohen Ch.7)"
        )

    # ── Gate 4: Opportunity Cost ──────────────────────────────────────────────
    # HV_Daily_Move (1σ) vs expected premium per day from HV estimate
    hv_daily    = float(pos_row.get("HV_Daily_Move_1Sigma") or 0)
    spot        = float(pos_row.get("Last") or pos_row.get("UL Last") or 0)
    basis       = _basis(pos_row)
    basis_eff   = basis or spot or 1.0

    if hv > 0 and spot > 0:
        # Rough premium per day: 40% of HV-implied weekly / 5 days
        weekly_est  = 0.4 * hv * spot / (52 ** 0.5)
        premium_day = weekly_est / 5.0
        # 1σ daily move
        daily_move  = hv / (252 ** 0.5) * spot
        if daily_move > 3.0 * premium_day and hv > 0.80:
            # Stock moves more than 3× daily premium → no edge
            _g4_reason = (
                f"Gate 4: Daily 1σ move (${daily_move:.2f}) > 3× daily premium est "
                f"(${premium_day:.2f}). At HV={hv*100:.0f}%, stock volatility overwhelms "
                "premium collected. Wait for vol to normalize. "
                "(Natenberg Ch.11: opportunity cost of selling convexity in high-HV regimes.)"
            )
            from config.sector_benchmarks import is_etf as _is_etf_g4
            if _is_etf_g4(_arb_ticker):
                _g4_reason += (
                    " ETF context: macro-driven vol spikes tend to normalize — "
                    "monitor HV_20D trend."
                )
            return ("MONITOR", _g4_reason)

    # ── All gates passed: WRITE_CALL ──────────────────────────────────────────
    edge_note = ""
    if iv_ref > 0 and hv > 0:
        edge_note = f" IV/HV edge: {iv_ref/hv:.2f}× (seller's edge confirmed)."
    return (
        "WRITE_CALL",
        f"All 4 arbitration gates passed.{edge_note} "
        "(McMillan Ch.3: conditions met for covered call entry.)"
    )


def _fetch_cc_candidates_live(
    ticker: str,
    spot: float,
    basis: float,
    hv: float,
    schwab_client,
    recovery_mode: str = "INCOME",
) -> tuple[bool, str, str, list[dict]]:
    """
    Fetch covered call candidates directly from Schwab chain API.
    Used as a fallback when the ticker is not in the latest scan CSV.

    Returns (is_favorable, unfav_reason, watch_signal, candidates).
    Mirrors the roll engine's _get_chain() approach — same API, OTM calls only.

    Book backing:
      McMillan Ch.3: sell calls above current price (OTM) to preserve upside.
      Natenberg Ch.8: IV/HV gap determines seller's edge — require IV > HV.
      Passarelli Ch.6: three DTE buckets — weekly, biweekly, monthly.
    """
    from datetime import date, timedelta
    import math

    try:
        schwab_client.ensure_valid_token()
        chain = schwab_client.get_chains(
            symbol=ticker,
            strikeCount=20,
            range="OTM",           # Only OTM calls (above spot) — we don't want ITM CCs
            strategy="SINGLE",
            # Note: optionType="C" removed — SchwabClient.get_chains() doesn't accept it.
            # callExpDateMap is read exclusively below (line ~448), so puts are ignored.
        )
    except Exception as e:
        logger.warning(f"[CCOpportunity-Live] Chain fetch failed for {ticker}: {e}")
        return False, f"Chain fetch error: {e}", "", []

    if not chain or "callExpDateMap" not in chain:
        return False, "No call chain data returned", "", []

    today = date.today()
    candidates = []

    for exp_str, strikes_map in chain["callExpDateMap"].items():
        # exp_str format: "2026-04-17:45" (date:dte)
        try:
            exp_date_str = exp_str.split(":")[0]
            exp_date = date.fromisoformat(exp_date_str)
            dte = (exp_date - today).days
        except Exception:
            continue

        # Only look at our DTE buckets (recovery: widen monthly delta cap)
        bucket_label = None
        delta_cap = 0.30
        for lbl, min_d, max_d, dcap in _DTE_BUCKETS:
            if min_d <= dte <= max_d:
                bucket_label = lbl
                delta_cap = dcap
                if recovery_mode in ("RECOVERY", "DEEP_RECOVERY") and lbl == "MONTHLY":
                    delta_cap = 0.35
                elif recovery_mode in ("RECOVERY", "DEEP_RECOVERY") and lbl == "BIWEEKLY":
                    delta_cap = 0.30
                break
        if bucket_label is None:
            continue

        for strike_str, contracts in strikes_map.items():
            try:
                strike = float(strike_str)
            except ValueError:
                continue

            # Only OTM: strike above spot
            if strike <= spot:
                continue

            # Recovery strike floor: max(spot × 1.10, cost basis)
            if recovery_mode in ("RECOVERY", "DEEP_RECOVERY"):
                _strike_floor = max(spot * 1.10, basis)
                if strike < _strike_floor:
                    continue

            contract = contracts[0] if isinstance(contracts, list) else contracts
            try:
                bid   = float(contract.get("bid") or 0)
                ask   = float(contract.get("ask") or 0)
                mid   = (bid + ask) / 2 if bid > 0 and ask > 0 else 0.0
                delta = abs(float(contract.get("delta") or 0))
                oi    = int(contract.get("openInterest") or 0)
                vol   = int(contract.get("totalVolume") or 0)
                iv    = float(contract.get("volatility") or 0)
                # Schwab returns IV as percent (e.g. 83.2 = 83.2%); normalise to decimal
                iv_dec = iv / 100.0 if iv > 5 else iv
            except Exception:
                continue

            if mid <= 0 or delta > delta_cap or delta <= 0:
                continue

            # Liquidity: require OI ≥ 100 or volume ≥ 10
            # Recovery: relax to OI ≥ 50 (thin-chain stocks — Jabbour Ch.4)
            _oi_min = 50 if recovery_mode in ("RECOVERY", "DEEP_RECOVERY") else 100
            liq = "GOOD" if oi >= 500 else ("OK" if oi >= _oi_min or vol >= 10 else "THIN")
            if liq == "THIN":
                continue

            spread_pct = ((ask - bid) / mid * 100) if mid > 0 else 999.0
            _spread_max = 20.0 if recovery_mode in ("RECOVERY", "DEEP_RECOVERY") else 15.0
            if spread_pct > _spread_max:
                continue

            ann_y = _ann_yield(mid, basis if basis > 0 else spot, dte)

            candidates.append({
                "bucket":      bucket_label,
                "strike":      round(strike, 2),
                "dte":         dte,
                "expiry":      exp_date_str,
                "mid":         round(mid, 2),
                "bid":         round(bid, 2),
                "ask":         round(ask, 2),
                "delta":       round(delta, 3),
                "ann_yield":   round(ann_y, 4),
                "liq":         liq,
                "spread_pct":  round(spread_pct, 1),
                "oi":          oi,
                "iv_pct":      round(iv_dec * 100, 1),
                "source":      "LIVE_CHAIN",
            })

    if not candidates:
        return False, "No viable OTM call candidates found in chain", "Wait for better strike availability", []

    # Sort by ann_yield descending; return best 3 across distinct buckets
    candidates.sort(key=lambda x: x["ann_yield"], reverse=True)

    # Favorability from position-level HV/IV (scan not available)
    # Use HV_20D from the position row as a proxy — if HV is extreme (>60%)
    # and we have no IV context, emit a conditional verdict
    if hv > 0:
        iv_hv_gap = None   # No IV_Rank from scan — use HV as signal quality flag
        is_fav = True
        unfav = ""
        watch = ""
        if hv > 0.60:
            # Extreme HV — warn but don't block; user already sees the HV warning card
            unfav = f"HV={hv*100:.0f}% extreme — verify IV > HV before executing"
            watch = "Run pipeline during market hours for current IV_Rank"
    else:
        is_fav = True
        unfav = ""
        watch = ""

    return is_fav, unfav, watch, candidates[:3]


def evaluate_cc_opportunities(df: pd.DataFrame, schwab_client=None) -> pd.DataFrame:
    """
    Main entry point. Called from run_all.py after doctrine.

    For each uncovered STOCK_ONLY / STOCK_ONLY_IDLE row:
      1. Locates the ticker in the latest scan output (Step12_Acceptance_*.csv)
      2. If found: runs full favorability check + ranks CC candidates from scan data
      3. If not found (SCAN_MISS) AND schwab_client is available:
         → fetches chain directly and ranks OTM calls by DTE bucket and ann yield
         → same logic as the roll candidate engine: targeted, on-demand, no full scan needed
      4. Writes CC_Proposal_* columns onto the row

    Non-blocking: any per-row exception → CC_Proposal_Status=ERROR.
    Rows that are already covered (short call written) are untouched.
    """
    # Accept STOCK_ONLY_IDLE, STOCK_ONLY, and BUY_WRITE stock rows.
    # BUY_WRITE stock rows are included because the paired call may have expired,
    # leaving the shares idle and eligible for new CC evaluation (e.g. EOSE after
    # CC expiry — stock is still structurally tagged BUY_WRITE but has no covering call).
    _strategy_col = df.get("Strategy", pd.Series("", index=df.index))
    _asset_col = df.get("AssetType", pd.Series("", index=df.index))
    _base_mask = _asset_col == "STOCK"

    # For BUY_WRITE stock rows, verify no active short call covers the shares.
    # An active short call = same TradeID, AssetType=OPTION, Call/Put or OptionType = CALL, DTE > 0.
    _bw_stock = _base_mask & (_strategy_col == "BUY_WRITE")
    _bw_idle = pd.Series(False, index=df.index)
    if _bw_stock.any():
        for bw_idx in df.index[_bw_stock]:
            _tid = df.at[bw_idx, "TradeID"]
            # Find option legs in same trade
            _option_legs = df[
                (df["TradeID"] == _tid) & (df["AssetType"] == "OPTION")
            ]
            # Check if any short call leg is still active (DTE > 0)
            _has_active_call = False
            for _, _ol in _option_legs.iterrows():
                _otype = str(_ol.get("OptionType") or _ol.get("Call/Put") or "").upper()
                _dte = float(_ol.get("DTE") or 0)
                _qty = float(_ol.get("Quantity") or 0)
                if "CALL" in _otype and _qty < 0 and _dte > 0:
                    _has_active_call = True
                    break
            if not _has_active_call:
                _bw_idle.at[bw_idx] = True

    idle_mask = (
        (_base_mask & _strategy_col.isin(["STOCK_ONLY_IDLE", "STOCK_ONLY"]))
        | _bw_idle
    )
    if not idle_mask.any():
        return df

    # Pre-allocate output columns
    _str_cols = [
        "CC_Proposal_Status", "CC_Proposal_Verdict", "CC_Unfavorable_Reason",
        "CC_Watch_Signal", "CC_Best_DTE_Bucket", "CC_Regime", "CC_Scan_TS",
        "CC_Candidate_1", "CC_Candidate_2", "CC_Candidate_3",
        "CC_Partial_Coverage_Note",
    ]
    _float_cols = ["CC_IV_Rank", "CC_Best_Ann_Yield"]
    _recovery_str_cols = ["CC_Recovery_Mode"]
    _recovery_float_cols = ["CC_Recovery_Gap", "CC_Recovery_Monthly_Est", "CC_Recovery_Months"]
    _ladder_str_cols = ["CC_Ladder_JSON"]
    _ladder_bool_cols = ["CC_Ladder_Eligible"]
    _ladder_float_cols = [
        "CC_Ladder_Total_Lots", "CC_Ladder_Covered_Lots",
        "CC_Ladder_Tier_A_Lots", "CC_Ladder_Tier_B_Lots", "CC_Ladder_Tier_C_Lots",
        "CC_Ladder_Monthly_Est", "CC_Ladder_Income_Gap_Ratio", "CC_Ladder_Recovery_Months",
    ]
    for col in _str_cols + _recovery_str_cols + _ladder_str_cols:
        if col not in df.columns:
            df[col] = pd.NA
    for col in _float_cols + _recovery_float_cols + _ladder_float_cols:
        if col not in df.columns:
            df[col] = pd.NA
    for col in _ladder_bool_cols:
        if col not in df.columns:
            df[col] = False

    # Load scan output once
    scan_path = _find_latest_scan_file()
    if scan_path is None:
        logger.warning("[CCOpportunity] No Step12 scan file found — skipping CC proposals")
        df.loc[idle_mask, "CC_Proposal_Status"]  = "SCAN_MISS"
        df.loc[idle_mask, "CC_Proposal_Verdict"] = "No scan data available — run pipeline first"
        return df

    try:
        scan_df = _load_scan(scan_path)
        scan_ts = str(scan_path.stem).replace("Step12_Acceptance_", "")
    except Exception as e:
        logger.warning(f"[CCOpportunity] Failed to load scan file {scan_path}: {e}")
        df.loc[idle_mask, "CC_Proposal_Status"]  = "ERROR"
        df.loc[idle_mask, "CC_Proposal_Verdict"] = f"Scan load error: {e}"
        return df

    # For BUY_WRITE rows split across multiple sub-rows (e.g. EOSE 500 + 1500),
    # compute total shares per (ticker, account) so the ladder allocation uses
    # the full position within a single account. Cross-account aggregation is wrong
    # because Roth, IRA, and taxable accounts are separate legal positions.
    _acct_total_shares: dict[tuple[str, str], float] = {}
    for _im_idx in df.index[idle_mask]:
        _im_ticker = str(df.at[_im_idx, "Underlying_Ticker"] if "Underlying_Ticker" in df.columns else df.at[_im_idx, "Symbol"])
        _im_acct = str(df.at[_im_idx, "Account"] if "Account" in df.columns else "")
        _im_qty = float(df.at[_im_idx, "Quantity"] if "Quantity" in df.columns else 0)
        _acct_total_shares[(_im_ticker, _im_acct)] = _acct_total_shares.get((_im_ticker, _im_acct), 0.0) + _im_qty

    # Deduplicate: only process the first row per (ticker, account) for ladder evaluation.
    # Write the ladder result to ALL idle rows for that (ticker, account).
    _seen_ticker_accts: set[tuple[str, str]] = set()

    logger.info(
        f"[CCOpportunity] Evaluating {idle_mask.sum()} idle stock position(s) "
        f"against scan {scan_ts}"
    )

    for idx in df.index[idle_mask]:
        try:
            pos_row = df.loc[idx]
            ticker  = str(pos_row.get("Underlying_Ticker") or pos_row.get("Symbol") or "")
            if not ticker:
                df.at[idx, "CC_Proposal_Status"]  = "SCAN_MISS"
                df.at[idx, "CC_Proposal_Verdict"] = "No ticker available"
                continue

            # Dedup: for multi-row same (ticker, account) — e.g. BUY_WRITE with
            # split lots in the same account. Copy first row's CC result to subsequent.
            _acct = str(pos_row.get("Account") or "")
            _ta_key = (ticker, _acct)
            if _ta_key in _seen_ticker_accts:
                # Copy CC columns from the first-evaluated row for this (ticker, account)
                _first_idx = next(
                    i for i in df.index[idle_mask]
                    if (str(df.at[i, "Underlying_Ticker"] if "Underlying_Ticker" in df.columns else df.at[i, "Symbol"]) == ticker
                        and str(df.at[i, "Account"] if "Account" in df.columns else "") == _acct)
                    and i != idx
                )
                _cc_copy_cols = (
                    _str_cols + _float_cols + _recovery_str_cols + _recovery_float_cols
                    + _ladder_str_cols + _ladder_bool_cols + _ladder_float_cols
                )
                for _cc in _cc_copy_cols:
                    if _cc in df.columns:
                        df.at[idx, _cc] = df.at[_first_idx, _cc]
                continue
            _seen_ticker_accts.add(_ta_key)

            # Use aggregate shares for multi-row positions WITHIN the same account
            # (e.g. EOSE 500+1500 in same brokerage). Cross-account aggregation is
            # incorrect — Roth and taxable are separate legal positions.
            _agg_qty = _acct_total_shares.get(_ta_key, float(pos_row.get("Quantity") or pos_row.get("Qty") or 0))

            # ── Recovery mode classification ──────────────────────────────
            rec_mode, rec_drift = _classify_recovery_mode(pos_row)
            df.at[idx, "CC_Recovery_Mode"] = rec_mode

            # STRUCTURAL_DAMAGE: beyond -35% — check for ladder eligibility first.
            # Large positions (≥1000 shares, thesis INTACT) → partial coverage ladder.
            # Small positions / broken thesis → CC blocked, redeploy capital.
            if rec_mode == "STRUCTURAL_DAMAGE":
                _spot_sd = _spot(pos_row, None) or 0.0
                _basis_sd = _basis(pos_row) or 0.0
                _gap_sd = max(0.0, _basis_sd - _spot_sd)
                _thesis_sd = str(pos_row.get("Thesis_State") or "INTACT").upper()
                df.at[idx, "CC_Recovery_Gap"] = round(_gap_sd, 2)

                # Check ladder eligibility using aggregate qty (≥ 10 lots AND thesis INTACT)
                _ladder_alloc = _compute_ladder_allocation(
                    _agg_qty, "STRUCTURAL_DAMAGE", _thesis_sd,
                )
                if _ladder_alloc is not None:
                    # Ladder eligible → partial coverage mode (skip to ladder branch below)
                    logger.info(
                        f"[CCOpportunity] {ticker}: STRUCTURAL_DAMAGE but ladder-eligible "
                        f"({_ladder_alloc['total_lots']} lots, {_ladder_alloc['max_coverage_pct']:.0%} max coverage)"
                    )
                    # Fall through to ladder branch — do NOT continue
                else:
                    # Small position or broken thesis → block CC outright
                    df.at[idx, "CC_Proposal_Status"]  = "UNFAVORABLE"
                    df.at[idx, "CC_Proposal_Verdict"] = (
                        f"STRUCTURAL_DAMAGE — drift={rec_drift:.1%}, gap=${_gap_sd:.2f}/sh. "
                        f"Position is beyond recovery via CC income (McMillan: -35% threshold). "
                        f"Decision: cut loss and redeploy capital, or hold for thesis only — "
                        f"do NOT write calls hoping premium closes a ${_gap_sd:.2f} gap."
                    )
                    logger.info(
                        f"[CCOpportunity] {ticker}: STRUCTURAL_DAMAGE (drift={rec_drift:.1%}) "
                        f"— CC blocked, recommending capital redeployment"
                    )
                    continue

            if rec_mode in ("RECOVERY", "DEEP_RECOVERY"):
                _spot_rec = _spot(pos_row, None) or 0.0
                _basis_rec = _basis(pos_row) or _spot_rec
                _hv_rec = float(pos_row.get("HV_20D") or 0)
                timeline = _compute_recovery_timeline(_spot_rec, _basis_rec, _hv_rec)
                df.at[idx, "CC_Recovery_Gap"]         = timeline["gap"]
                df.at[idx, "CC_Recovery_Monthly_Est"]  = timeline["monthly_est"]
                df.at[idx, "CC_Recovery_Months"]       = timeline["months"]
                logger.info(
                    f"[CCOpportunity] {ticker}: {rec_mode} (drift={rec_drift:.1%}) "
                    f"gap=${timeline['gap']:.2f}, ~{timeline['months']:.0f}mo to close"
                )

            # ── Tiered CC Ladder branch ────────────────────────────────────
            # For large positions (≥1000 shares, thesis INTACT), build a
            # partial-coverage ladder instead of a single CC candidate.
            # SD positions that fell through: always use ladder (skip gates).
            # Non-SD positions: check allocation eligibility here too.
            _thesis_for_ladder = str(pos_row.get("Thesis_State") or "INTACT").upper()
            if rec_mode == "STRUCTURAL_DAMAGE":
                # _ladder_alloc was computed in SD block above (fall-through = eligible)
                _ladder_alloc_check = _ladder_alloc  # type: ignore[possibly-undefined]
            else:
                _ladder_alloc_check = _compute_ladder_allocation(
                    _agg_qty, rec_mode, _thesis_for_ladder,
                )

            if _ladder_alloc_check is not None:
                _spot_lad = _spot(pos_row, None) or 0.0
                _basis_lad = _basis(pos_row) or 0.0
                _hv_lad = float(pos_row.get("HV_20D") or 0)

                # Build per-tier candidates: scan first, supplement with live chain
                _lad_scan = _build_ladder_candidates_scan(
                    scan_df, ticker, _basis_lad, _spot_lad, rec_mode,
                ) if not scan_df.empty else {"tier_a_candidates": [], "tier_b_candidates": []}

                _lad_a = _lad_scan["tier_a_candidates"]
                _lad_b = _lad_scan["tier_b_candidates"]

                # Supplement empty tiers with live chain if available
                if schwab_client is not None and (not _lad_a or not _lad_b):
                    _lad_live = _build_ladder_candidates_live(
                        ticker, _spot_lad, _basis_lad, _hv_lad, schwab_client, rec_mode,
                    )
                    if not _lad_a:
                        _lad_a = _lad_live["tier_a_candidates"]
                    if not _lad_b:
                        _lad_b = _lad_live["tier_b_candidates"]

                # Build full ladder plan
                _plan = _build_ladder_plan(
                    pos_row, _ladder_alloc_check, _lad_a, _lad_b, rec_mode,
                    override_qty=_agg_qty,
                )

                # IV Rank / Regime — query iv_term_history for ladder positions
                # (same enrichment the regular scan path does at line 1542-1544)
                _lad_iv_rank = _query_iv_percentile_from_history(ticker)
                # Derive chain IV from best candidate (fallback when no IV history)
                _lad_chain_iv = None
                if _lad_a and _lad_a[0].get("iv_pct"):
                    _lad_chain_iv = _lad_a[0]["iv_pct"] / 100
                elif _lad_b and _lad_b[0].get("iv_pct"):
                    _lad_chain_iv = _lad_b[0]["iv_pct"] / 100

                if _lad_iv_rank is not None:
                    df.at[idx, "CC_IV_Rank"] = _lad_iv_rank
                elif _lad_chain_iv is not None:
                    # No IV history — use chain IV as proxy (same as live-chain path)
                    df.at[idx, "CC_IV_Rank"] = round(_lad_chain_iv * 100, 1)
                    _lad_iv_rank = round(_lad_chain_iv * 100, 1)

                # Regime from IV rank or chain IV context
                _iv_for_regime = _lad_iv_rank if _lad_iv_rank is not None else (
                    round(_lad_chain_iv * 100, 1) if _lad_chain_iv else None
                )
                if _iv_for_regime is not None:
                    if _iv_for_regime >= 50:
                        df.at[idx, "CC_Regime"] = "High Vol"
                    elif _iv_for_regime < 30:
                        df.at[idx, "CC_Regime"] = "Low Vol"
                    else:
                        df.at[idx, "CC_Regime"] = "Normal"

                # Best annualised yield from ladder candidates
                _best_ay = 0.0
                if _lad_a and _lad_a[0].get("ann_yield", 0) > _best_ay:
                    _best_ay = _lad_a[0]["ann_yield"]
                if _lad_b and _lad_b[0].get("ann_yield", 0) > _best_ay:
                    _best_ay = _lad_b[0]["ann_yield"]
                if _best_ay > 0:
                    df.at[idx, "CC_Best_Ann_Yield"] = _best_ay

                # Write ladder columns
                df.at[idx, "CC_Ladder_Eligible"]          = True
                df.at[idx, "CC_Ladder_JSON"]              = json.dumps(_plan, default=str)
                df.at[idx, "CC_Ladder_Total_Lots"]        = _plan["total_lots"]
                df.at[idx, "CC_Ladder_Covered_Lots"]      = _plan["covered_lots"]
                df.at[idx, "CC_Ladder_Tier_A_Lots"]       = _plan["tier_a_lots"]
                df.at[idx, "CC_Ladder_Tier_B_Lots"]       = _plan["tier_b_lots"]
                df.at[idx, "CC_Ladder_Tier_C_Lots"]       = _plan["uncovered_lots"]
                df.at[idx, "CC_Ladder_Monthly_Est"]       = _plan["monthly_income_est"]
                df.at[idx, "CC_Ladder_Income_Gap_Ratio"]  = _plan["income_gap_ratio"]
                df.at[idx, "CC_Ladder_Recovery_Months"]   = _plan["recovery_months_est"]

                # Write best candidates as CC_Candidate_1/2/3 for compatibility
                _cand_idx = 1
                if _plan["tier_a_best"]:
                    df.at[idx, f"CC_Candidate_{_cand_idx}"] = json.dumps(_plan["tier_a_best"])
                    _cand_idx += 1
                if _plan["tier_b_best"]:
                    df.at[idx, f"CC_Candidate_{_cand_idx}"] = json.dumps(_plan["tier_b_best"])

                # Set verdict with framing
                _framing = _plan["framing"]
                _cov_pct = _plan["max_coverage_pct"]
                _monthly = _plan["monthly_income_est"]
                _igr = _plan["income_gap_ratio"]
                _rmo = _plan["recovery_months_est"]

                _cbr = _plan.get("cost_basis_reduction_annual", 0)
                _b1yr = _plan.get("basis_after_1yr", 0)

                if _framing == "CASH_FLOW_ONLY":
                    df.at[idx, "CC_Proposal_Status"]  = "FAVORABLE"
                    df.at[idx, "CC_Proposal_Verdict"] = (
                        f"CC_LADDER — CASH_FLOW_ONLY. {_plan['covered_lots']}/{_plan['total_lots']} "
                        f"lots covered ({_cov_pct:.0%}). Est ${_monthly:,.0f}/mo. "
                        f"Income-to-gap ratio {_igr:.2%} — cannot realistically repair. "
                        f"Frame as cash flow generation only, NOT recovery."
                    )
                elif _framing == "PARTIAL_REPAIR":
                    df.at[idx, "CC_Proposal_Status"]  = "FAVORABLE"
                    df.at[idx, "CC_Proposal_Verdict"] = (
                        f"CC_LADDER — PARTIAL_REPAIR. {_plan['covered_lots']}/{_plan['total_lots']} "
                        f"lots covered ({_cov_pct:.0%}). Est ${_monthly:,.0f}/mo. "
                        f"Basis reduction {_cbr:.1%}/yr → ${_b1yr:.2f}/sh after 1yr. "
                        f"Income-to-gap {_igr:.2%}/mo — ~{_rmo:.0f}mo timeline."
                    )
                else:
                    df.at[idx, "CC_Proposal_Status"]  = "FAVORABLE"
                    df.at[idx, "CC_Proposal_Verdict"] = (
                        f"CC_LADDER — RECOVERY_VIABLE. {_plan['covered_lots']}/{_plan['total_lots']} "
                        f"lots covered ({_cov_pct:.0%}). Est ${_monthly:,.0f}/mo. "
                        f"Basis reduction {_cbr:.1%}/yr → ${_b1yr:.2f}/sh after 1yr. "
                        f"Income-to-gap {_igr:.2%}/mo — ~{_rmo:.0f}mo to close gap."
                    )

                df.at[idx, "CC_Best_DTE_Bucket"] = "LADDER"
                df.at[idx, "CC_Scan_TS"] = scan_ts

                logger.info(
                    f"[CCOpportunity-Ladder] {ticker}: {_framing} — "
                    f"{_plan['covered_lots']}/{_plan['total_lots']} lots, "
                    f"${_monthly:,.0f}/mo, ratio={_igr:.2%}"
                )
                continue

            # Find scan rows for this ticker
            ticker_scan = (
                scan_df[scan_df["Ticker"] == ticker] if "Ticker" in scan_df.columns
                else pd.DataFrame()
            )
            if ticker_scan.empty:
                # Ticker not in scan CSV — try live chain fetch if client available.
                # This is the same on-demand pattern as the roll candidate engine.
                if schwab_client is not None:
                    hv_pos = float(pos_row.get("HV_20D") or 0)
                    spot_pos = _spot(pos_row, None)
                    basis_pos = _basis(pos_row)
                    if spot_pos and spot_pos > 0:
                        is_fav_live, unfav_live, watch_live, cands_live = _fetch_cc_candidates_live(
                            ticker, spot_pos, basis_pos or spot_pos, hv_pos, schwab_client,
                            recovery_mode=rec_mode,
                        )
                        # Extract ATM IV from best candidate (chain-derived) for Gate 2
                        _chain_iv = cands_live[0]["iv_pct"] / 100 if cands_live else 0.0
                        # Run the full arbitration gate, passing chain IV for idle positions
                        # that lack IV_30D in their row data
                        arb_verdict, arb_reason = _cc_arbitration(
                            pos_row, is_fav_live, unfav_live, chain_iv=_chain_iv,
                            recovery_mode=rec_mode,
                        )
                        df.at[idx, "CC_Scan_TS"]            = "LIVE_CHAIN"
                        df.at[idx, "CC_Unfavorable_Reason"] = unfav_live
                        df.at[idx, "CC_Watch_Signal"]       = watch_live
                        if _chain_iv > 0:
                            df.at[idx, "CC_IV_Rank"] = round(_chain_iv * 100, 1)
                        if arb_verdict == "HOLD_STOCK":
                            df.at[idx, "CC_Proposal_Status"]  = "UNFAVORABLE"
                            df.at[idx, "CC_Proposal_Verdict"] = f"HOLD_STOCK — {arb_reason}"
                            logger.info(f"[CCOpportunity-Live] {ticker}: HOLD_STOCK — {arb_reason}")
                        elif arb_verdict == "MONITOR":
                            df.at[idx, "CC_Proposal_Status"]  = "UNFAVORABLE"
                            df.at[idx, "CC_Proposal_Verdict"] = f"MONITOR — {arb_reason}"
                            logger.info(f"[CCOpportunity-Live] {ticker}: MONITOR — {arb_reason}")
                        elif cands_live:
                            best = cands_live[0]
                            for i, cand in enumerate(cands_live, 1):
                                df.at[idx, f"CC_Candidate_{i}"] = json.dumps(cand)
                            df.at[idx, "CC_Proposal_Status"]  = "FAVORABLE"
                            df.at[idx, "CC_Best_DTE_Bucket"]  = best["bucket"]
                            df.at[idx, "CC_Best_Ann_Yield"]   = best["ann_yield"]
                            _rec_tag = ""
                            if rec_mode in ("RECOVERY", "DEEP_RECOVERY"):
                                _tl = _compute_recovery_timeline(
                                    spot_pos, basis_pos or spot_pos, hv_pos
                                )
                                _rec_tag = (
                                    f" [{rec_mode}: ${_tl['gap']:.2f}/sh gap, "
                                    f"~{_tl['months']:.0f}mo to close]"
                                )
                            df.at[idx, "CC_Proposal_Verdict"] = (
                                f"WRITE_CALL — Live chain. Best: {best['bucket']} "
                                f"${best['strike']}C (${best['mid']:.2f}/sh, "
                                f"{best['ann_yield']:.1%}/yr). {arb_reason}{_rec_tag}"
                            )
                            logger.info(
                                f"[CCOpportunity-Live] {ticker}: WRITE_CALL — "
                                f"{best['bucket']} ${best['strike']}C {best['ann_yield']:.1%}/yr"
                            )
                        else:
                            # No viable candidates — recovery mode reports strike floor
                            if rec_mode in ("RECOVERY", "DEEP_RECOVERY"):
                                _floor = max(
                                    (spot_pos or 0) * 1.10,
                                    basis_pos or 0,
                                )
                                df.at[idx, "CC_Proposal_Status"]  = "UNFAVORABLE"
                                df.at[idx, "CC_Proposal_Verdict"] = (
                                    f"NO_VIABLE_STRIKE — floor=${_floor:.2f} "
                                    f"(basis=${basis_pos:.2f}). {unfav_live}"
                                )
                            else:
                                df.at[idx, "CC_Proposal_Status"]  = "UNFAVORABLE"
                                df.at[idx, "CC_Proposal_Verdict"] = f"No viable candidates: {unfav_live}"
                        continue
                # No Schwab client — fall back to SCAN_MISS
                df.at[idx, "CC_Proposal_Status"]  = "SCAN_MISS"
                df.at[idx, "CC_Proposal_Verdict"] = (
                    f"{ticker} not in latest scan — run pipeline with this ticker in watchlist"
                )
                df.at[idx, "CC_Scan_TS"] = scan_ts
                continue

            # Use first scan row for market context (all rows same ticker)
            ctx = ticker_scan.iloc[0]

            iv_rank  = _iv_rank(ctx)
            regime   = str(ctx.get("Regime") or "Unknown")
            signal   = str(ctx.get("Signal_Type") or "")
            ivhv_gap = float(ctx.get("IVHV_gap_30D") or 0) or None
            spot_px  = _spot(pos_row, ctx)
            basis_px = _basis(pos_row)
            qty      = float(pos_row.get("Quantity") or pos_row.get("Qty") or 0)

            # IV enrichment fallback: query iv_term_history when scan lacks IV_Rank
            if iv_rank is None:
                iv_rank = _query_iv_percentile_from_history(ticker)

            # Favorability check (scan-data gate) — recovery mode relaxes IV_Rank
            is_fav, unfav_reason, watch_signal = _favorability_check(
                iv_rank, regime, signal, ivhv_gap, recovery_mode=rec_mode,
                ticker=ticker,
            )

            # Management arbitration gate (runs BEFORE showing candidates)
            arb_verdict, arb_reason = _cc_arbitration(
                pos_row, is_fav, unfav_reason, recovery_mode=rec_mode,
            )

            df.at[idx, "CC_IV_Rank"]            = iv_rank
            df.at[idx, "CC_Regime"]             = regime
            df.at[idx, "CC_Scan_TS"]            = scan_ts
            df.at[idx, "CC_Unfavorable_Reason"] = unfav_reason
            df.at[idx, "CC_Watch_Signal"]       = watch_signal

            if arb_verdict == "HOLD_STOCK":
                df.at[idx, "CC_Proposal_Status"]  = "UNFAVORABLE"
                df.at[idx, "CC_Proposal_Verdict"] = f"HOLD_STOCK — {arb_reason}"
                logger.info(f"[CCOpportunity] {ticker}: HOLD_STOCK — {arb_reason}")
                continue

            if arb_verdict == "MONITOR" or not is_fav:
                reason_str = arb_reason if arb_verdict == "MONITOR" else unfav_reason.split(" | ")[0]
                df.at[idx, "CC_Proposal_Status"]  = "UNFAVORABLE"
                df.at[idx, "CC_Proposal_Verdict"] = f"{arb_verdict} — {reason_str}"
                logger.info(f"[CCOpportunity] {ticker}: {arb_verdict} — {reason_str}")
                continue

            # All gates passed → rank candidates from scan
            effective_basis = basis_px or spot_px or 1.0
            candidates = _rank_candidates(
                scan_df, ticker, effective_basis,
                recovery_mode=rec_mode,
                spot_price=spot_px or 0.0,
                qty=qty,
            )

            if not candidates:
                # Favorable market but no scan CC candidates
                _iv_d = f"{iv_rank:.0f}%" if iv_rank is not None else "N/A"
                if rec_mode in ("RECOVERY", "DEEP_RECOVERY"):
                    _floor = max((spot_px or 0) * 1.10, basis_px or 0)
                    df.at[idx, "CC_Proposal_Status"]  = "FAVORABLE"
                    df.at[idx, "CC_Proposal_Verdict"] = (
                        f"NO_VIABLE_STRIKE — floor=${_floor:.2f} (basis=${effective_basis:.2f}). "
                        f"IV_Rank={_iv_d}, Regime={regime}. "
                        f"No scan candidates above strike floor — widen scan or check chain"
                    )
                else:
                    df.at[idx, "CC_Proposal_Status"]  = "FAVORABLE"
                    df.at[idx, "CC_Proposal_Verdict"] = (
                        f"WRITE_CALL viable (IV_Rank={_iv_d}, Regime={regime}) "
                        f"but no CC candidates in latest scan — check chain during market hours"
                    )
                df.at[idx, "CC_Best_DTE_Bucket"] = "NO_CANDIDATES"
                logger.info(f"[CCOpportunity] {ticker}: WRITE_CALL but no CC scan candidates")
                continue

            # Write top 3 candidates
            best = candidates[0]
            for i, cand in enumerate(candidates, 1):
                df.at[idx, f"CC_Candidate_{i}"] = json.dumps(cand)

            df.at[idx, "CC_Proposal_Status"]  = "FAVORABLE"
            df.at[idx, "CC_Best_DTE_Bucket"]  = best["bucket"]
            df.at[idx, "CC_Best_Ann_Yield"]   = best["ann_yield"]

            # ── Partial-coverage advisory for small RECOVERY positions ──
            _lots = max(1, int(qty // 100)) if qty >= 100 else 1
            if (rec_mode in ("RECOVERY", "DEEP_RECOVERY", "STRUCTURAL_DAMAGE")
                    and qty < 1000 and _lots > 1):
                df.at[idx, "CC_Partial_Coverage_Note"] = (
                    f"Consider covering {_lots - 1} of {_lots} lots — "
                    f"keep 1 lot uncovered for upside recovery."
                )

            _rec_tag_scan = ""
            if rec_mode in ("RECOVERY", "DEEP_RECOVERY"):
                _tl_scan = _compute_recovery_timeline(
                    spot_px or 0, basis_px or (spot_px or 0),
                    float(pos_row.get("HV_20D") or 0),
                )
                _rec_tag_scan = (
                    f" [{rec_mode}: ${_tl_scan['gap']:.2f}/sh gap, "
                    f"~{_tl_scan['months']:.0f}mo to close]"
                )
            _iv_display = f"{iv_rank:.0f}%" if iv_rank is not None else "N/A"
            df.at[idx, "CC_Proposal_Verdict"] = (
                f"WRITE_CALL — IV_Rank={_iv_display}, Regime={regime}. "
                f"Best: {best['bucket']} ${best['strike']}C "
                f"(${best['mid']:.2f}/share, {best['ann_yield']:.1%}/yr). "
                f"{arb_reason}{_rec_tag_scan}"
            )
            logger.info(
                f"[CCOpportunity] {ticker}: FAVORABLE — best={best['bucket']} "
                f"strike={best['strike']} ann_yield={best['ann_yield']:.1%}"
            )

        except Exception as e:
            logger.warning(f"[CCOpportunity] Row {idx} failed (non-fatal): {e}")
            df.at[idx, "CC_Proposal_Status"]  = "ERROR"
            df.at[idx, "CC_Proposal_Verdict"] = f"Evaluation error: {e}"

    return df
