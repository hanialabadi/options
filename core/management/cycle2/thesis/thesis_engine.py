"""
Thesis State Engine  (Cycle 2 — Layer 0 above chart/vol/Greeks)
================================================================
Answers ONE question before anything else:
    "Is the underlying company still aligned with my capital thesis?"

If the answer is NO, vol optimization, roll timing, and Greek management
are all irrelevant — you're managing a broken position, not a live thesis.

OUTPUT COLUMNS (added to df before doctrine):
    Thesis_State         : INTACT | DEGRADED | BROKEN | UNKNOWN
    Thesis_Drivers       : JSON list of active signals and their weights
    Thesis_Drawdown_Type : TEMPORARY | STRUCTURAL | MACRO | MIXED | UNKNOWN
    Thesis_Summary       : human-readable one-liner for the position card

SIGNAL BUCKETS (Phase A — observable market signals, no paid data):
────────────────────────────────────────────────────────────────────
1. MICRO (company-specific, yfinance free tier)
   • Earnings surprise direction (last quarter EPS actual vs estimate)
   • Analyst revision trend (net upgrades/downgrades in last 30d)
   • Post-earnings drift classification

2. MACRO (regime-level, from scan pipeline data)
   • Sector ETF relative performance vs stock (if sector ETF in universe)
   • Market vol regime (VIX proxy via hv_20d_percentile)
   • Rates regime (available via bond proxies if tracked)

3. STRUCTURAL vs TEMPORARY classifier
   Inputs: earnings gap timing, chart state sequence, sector alignment
   Logic: matrix of (cause × magnitude × duration) → classification

ARCHITECTURE PRINCIPLES:
    • Non-blocking: ALL external calls (yfinance) are wrapped in try/except
      with graceful degradation to UNKNOWN — never crash the pipeline
    • Per-ticker caching: yfinance called ONCE per unique Underlying_Ticker
      per run, not once per row — avoids rate limiting
    • Management Safe Mode: honours MANAGEMENT_SAFE_MODE flag (skips yf calls)
    • Runs AFTER chart states (has access to all primitives), BEFORE doctrine
    • Output is advisory to doctrine: Thesis_State = BROKEN blocks rolls
      but doctrine emergency gates (hard stop, DTE<7) still override

McMillan Ch.3 (thesis validity for buy-write management)
Passarelli Ch.2 ("Story Check — is the thesis still intact?")
"""

from __future__ import annotations

import json
import logging
import math
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

# Thesis state enum values
THESIS_INTACT    = "INTACT"
THESIS_DEGRADED  = "DEGRADED"
THESIS_BROKEN    = "BROKEN"
THESIS_UNKNOWN   = "UNKNOWN"

# Drawdown type classifier
DRAWDOWN_TEMPORARY  = "TEMPORARY"
DRAWDOWN_STRUCTURAL = "STRUCTURAL"
DRAWDOWN_MACRO      = "MACRO"
DRAWDOWN_MIXED      = "MIXED"
DRAWDOWN_UNKNOWN    = "UNKNOWN"

# Signal weights: how much each signal shifts the thesis score
# Score 0.0 = INTACT, 1.0 = BROKEN threshold
_WEIGHTS = {
    # Micro signals
    "earnings_miss":           0.35,  # last Q EPS actual < estimate
    "earnings_miss_severe":    0.55,  # EPS miss > 20%
    "analyst_downgrade_trend": 0.25,  # net downgrades in last 30d
    "analyst_upgrade_trend":   -0.15, # net upgrades (improves thesis)
    "post_earnings_gap_down":  0.30,  # stock gapped down > 5% on earnings
    "post_earnings_recovered": -0.20, # gap fully recovered in 5d (temporary)

    # Chart state signals (already in df — no external calls)
    "price_structure_broken":  0.40,  # PriceStructure_State = STRUCTURE_BROKEN
    "trend_collapsed":         0.30,  # TrendIntegrity was STRONG → now EXHAUSTED/NO_TREND
    "momentum_reversing":      0.15,  # MomentumVelocity = REVERSING sustained 5+ days
    "recovery_dead_cat":       0.20,  # RecoveryQuality_State = DEAD_CAT_BOUNCE (confirmed)

    # Macro signals (observable from position data)
    "sector_relative_weak":    0.20,  # stock underperforming sector ETF > 10%
    "high_vol_regime":         0.10,  # hv_20d_percentile > 80 (fear regime)

    # Direction-adverse signals (LONG_PUT / LONG_CALL only — Passarelli Ch.2 "Story Check")
    # Stock moving against the option's directional thesis.
    # Thresholds match doctrine Gate 2b-dir: ROC5 > 1.5%, Price_Drift > 2%
    "direction_adverse":         0.25,  # one signal (ROC5 or drift) against thesis → DEGRADED
    "direction_adverse_severe":  0.35,  # both ROC5 + drift against thesis → solid DEGRADED

    # Positive signals that reduce broken score
    "structural_recovery":     -0.35, # RecoveryQuality_State = STRUCTURAL_RECOVERY
    "trend_restoring":         -0.15, # TrendIntegrity improving from EXHAUSTED → WEAK
    "momentum_accelerating":   -0.10, # MomentumVelocity = ACCELERATING
}

# Thresholds for Thesis_State classification
_BROKEN_THRESHOLD   = 0.55
_DEGRADED_THRESHOLD = 0.25

# yfinance rate-limit backoff shared across all calls in this run
_YF_BACKOFF_UNTIL = 0.0

# ── Sector Relative Strength constants ────────────────────────────────────────

# z-score thresholds (Natenberg Ch.8: meaningful moves in volatility-normalized terms)
# z < 0   → stock lagging benchmark
# z < -1  → 1σ underperformance — worth watching
# z < -2  → 2σ underperformance — sector is actively working against the position
# z < -3  → 3σ underperformance — structural breakdown vs sector
_SRS_OUTPERFORMING   = "OUTPERFORMING"    # z > +1
_SRS_NEUTRAL         = "NEUTRAL"          # -1 <= z <= +1
_SRS_UNDERPERFORMING = "UNDERPERFORMING"  # -2 <= z < -1
_SRS_MICRO_BREAKDOWN = "MICRO_BREAKDOWN"  # -3 <= z < -2
_SRS_BROKEN          = "BROKEN"           # z < -3

# Benchmark return cache: (ticker, benchmark) → return value, avoids repeat yf calls
_SRS_CACHE: Dict[str, float] = {}


def compute_sector_relative_strength(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add Sector_Relative_Strength column to df.

    Uses z-score normalized relative return (Natenberg Ch.8, McMillan Ch.1):
        relative_return = stock_30d_return - benchmark_30d_return
        sigma_30d       = HV_20D / sqrt(252) * sqrt(30)   (already in pipeline)
        z_score         = relative_return / sigma_30d

    This normalizes by the stock's own volatility, so a high-vol name (EOSE HV=107%)
    needs a much larger absolute move to register as BROKEN vs a low-vol utility.

    OUTPUT:
        Sector_Relative_Strength : OUTPERFORMING | NEUTRAL | UNDERPERFORMING |
                                   MICRO_BREAKDOWN | BROKEN
        Sector_Benchmark         : ETF ticker used (e.g. "ICLN", "QQQ", "SPY")
        Sector_RS_ZScore         : float, rounded to 2dp (for display/audit)

    Called from compute_thesis_state() — adds columns before _classify_thesis().
    Non-blocking: any yfinance error → NEUTRAL (unknown is less actionable than
    a false BROKEN signal).
    """
    if df.empty:
        df["Sector_Relative_Strength"] = _SRS_NEUTRAL
        df["Sector_Benchmark"]         = ""
        df["Sector_RS_ZScore"]         = 0.0
        return df

    # Check safe mode
    safe_mode = False
    try:
        from core.shared.data_contracts.config import MANAGEMENT_SAFE_MODE
        safe_mode = bool(MANAGEMENT_SAFE_MODE)
    except Exception:
        pass

    # Load benchmark map — graceful fallback to SPY-only if config missing
    try:
        from config.sector_benchmarks import SECTOR_BENCHMARK_MAP
    except Exception:
        SECTOR_BENCHMARK_MAP = {"_default": "SPY"}

    df = df.copy()
    df["Sector_Relative_Strength"] = _SRS_NEUTRAL
    df["Sector_Benchmark"]         = ""
    df["Sector_RS_ZScore"]         = 0.0

    ticker_col = "Underlying_Ticker" if "Underlying_Ticker" in df.columns else "Symbol"
    trade_col  = "TradeID" if "TradeID" in df.columns else None

    # Collect unique (ticker, hv_20d) pairs — one yf fetch per ticker
    if trade_col:
        # Use stock leg representative row per trade
        unique_pairs: Dict[str, tuple] = {}  # ticker → (benchmark, hv_20d)
        for trade_id in df[trade_col].dropna().unique():
            trade_rows = df[df[trade_col] == trade_id]
            stock_rows = trade_rows[trade_rows.get("AssetType", pd.Series()) == "STOCK"] \
                if "AssetType" in trade_rows.columns else pd.DataFrame()
            rep = stock_rows.iloc[0] if not stock_rows.empty else trade_rows.iloc[0]
            ticker    = str(rep.get(ticker_col, "") or "")
            hv_20d    = float(rep.get("HV_20D", 0) or 0)
            benchmark = SECTOR_BENCHMARK_MAP.get(ticker, SECTOR_BENCHMARK_MAP.get("_default", "SPY"))
            if ticker and ticker not in unique_pairs:
                unique_pairs[ticker] = (benchmark, hv_20d)
    else:
        unique_pairs = {}
        for _, row in df.iterrows():
            ticker    = str(row.get(ticker_col, "") or "")
            hv_20d    = float(row.get("HV_20D", 0) or 0)
            benchmark = SECTOR_BENCHMARK_MAP.get(ticker, SECTOR_BENCHMARK_MAP.get("_default", "SPY"))
            if ticker and ticker not in unique_pairs:
                unique_pairs[ticker] = (benchmark, hv_20d)

    # Fetch 30d returns for all tickers + benchmarks (deduplicated)
    returns_cache: Dict[str, Optional[float]] = {}  # ticker/etf → 30d return

    def _fetch_30d_return(symbol: str) -> Optional[float]:
        if symbol in returns_cache:
            return returns_cache[symbol]
        if safe_mode or not symbol or symbol in ("nan", "None", ""):
            returns_cache[symbol] = None
            return None
        global _YF_BACKOFF_UNTIL
        if time.time() < _YF_BACKOFF_UNTIL:
            returns_cache[symbol] = None
            return None
        try:
            import yfinance as yf
            hist = yf.download(symbol, period="35d", progress=False, auto_adjust=True)
            if hist is None or hist.empty or len(hist) < 20:
                returns_cache[symbol] = None
                return None
            # 30-day return: last close vs close 30 trading days ago (use -31 to -1 slice)
            close = hist["Close"].dropna()
            if len(close) < 20:
                returns_cache[symbol] = None
                return None
            ret = float(close.iloc[-1] / close.iloc[0] - 1.0)
            returns_cache[symbol] = ret
            return ret
        except Exception as e:
            err_str = str(e).lower()
            if "rate" in err_str or "429" in err_str or "too many" in err_str:
                logger.warning(f"[SectorRS] Rate limit hit fetching {symbol}. Backing off 60s.")
                _YF_BACKOFF_UNTIL = time.time() + 60
            else:
                logger.debug(f"[SectorRS] yfinance failed for {symbol}: {e}")
            returns_cache[symbol] = None
            return None

    # Compute z-scores per ticker
    ticker_results: Dict[str, tuple] = {}  # ticker → (srs_label, benchmark, z_score)

    for ticker, (benchmark, hv_20d) in unique_pairs.items():
        stock_ret     = _fetch_30d_return(ticker)
        benchmark_ret = _fetch_30d_return(benchmark)

        if stock_ret is None or benchmark_ret is None:
            ticker_results[ticker] = (_SRS_NEUTRAL, benchmark, 0.0)
            continue

        # sigma_30d: annualized HV → daily → 30-day (sqrt-of-time scaling)
        # HV_20D is stored as a decimal fraction (0.46 = 46%) in this pipeline
        hv_daily  = hv_20d / math.sqrt(252)
        sigma_30d = hv_daily * math.sqrt(30)

        if sigma_30d <= 0:
            # Can't normalize — skip, treat as NEUTRAL
            ticker_results[ticker] = (_SRS_NEUTRAL, benchmark, 0.0)
            continue

        relative_return = stock_ret - benchmark_ret
        z_score         = relative_return / sigma_30d

        if z_score > 1.0:
            label = _SRS_OUTPERFORMING
        elif z_score >= -1.0:
            label = _SRS_NEUTRAL
        elif z_score >= -2.0:
            label = _SRS_UNDERPERFORMING
        elif z_score >= -3.0:
            label = _SRS_MICRO_BREAKDOWN
        else:
            label = _SRS_BROKEN

        ticker_results[ticker] = (label, benchmark, round(z_score, 2))
        logger.debug(
            f"[SectorRS] {ticker} vs {benchmark}: "
            f"stock={stock_ret:.1%} bench={benchmark_ret:.1%} "
            f"rel={relative_return:.1%} σ30d={sigma_30d:.1%} z={z_score:.2f} → {label}"
        )

    # Broadcast results onto all rows
    for ticker, (label, benchmark, z_score) in ticker_results.items():
        mask = df[ticker_col] == ticker
        df.loc[mask, "Sector_Relative_Strength"] = label
        df.loc[mask, "Sector_Benchmark"]         = benchmark
        df.loc[mask, "Sector_RS_ZScore"]         = z_score

    srs_counts = df["Sector_Relative_Strength"].value_counts().to_dict()
    logger.info(f"[SectorRS] Completed: {srs_counts}")

    return df


# ── Main Entry Point ──────────────────────────────────────────────────────────

def compute_thesis_state(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add Thesis_State, Thesis_Drivers, Thesis_Drawdown_Type, Thesis_Summary
    columns to df.

    Called once per run, after compute_chart_state(), before doctrine.
    Operates at the TRADE level (one result per TradeID), applied to all legs.
    """
    if df.empty:
        return df

    df = df.copy()

    # Initialize output columns
    df["Thesis_State"]         = THESIS_UNKNOWN
    df["Thesis_Drivers"]       = "[]"
    df["Thesis_Drawdown_Type"] = DRAWDOWN_UNKNOWN
    df["Thesis_Summary"]       = ""

    # ── Sector Relative Strength (runs first — adds Sector_* columns for _classify_thesis) ──
    try:
        df = compute_sector_relative_strength(df)
    except Exception as _srs_err:
        logger.warning(f"[ThesisEngine] Sector RS computation failed (non-fatal): {_srs_err}")
        if "Sector_Relative_Strength" not in df.columns:
            df["Sector_Relative_Strength"] = _SRS_NEUTRAL
        if "Sector_Benchmark" not in df.columns:
            df["Sector_Benchmark"] = ""
        if "Sector_RS_ZScore" not in df.columns:
            df["Sector_RS_ZScore"] = 0.0

    # Get unique tickers (use Underlying_Ticker — canonical identity)
    ticker_col = "Underlying_Ticker" if "Underlying_Ticker" in df.columns else "Symbol"
    unique_tickers = df[ticker_col].dropna().unique()

    # Batch fetch micro signals ONCE per ticker (avoids per-row API calls)
    micro_cache: Dict[str, Dict] = {}
    for ticker in unique_tickers:
        micro_cache[ticker] = _fetch_micro_signals(str(ticker))

    # Process each unique trade (group by TradeID → pick stock or first row)
    trade_col = "TradeID" if "TradeID" in df.columns else None

    if trade_col:
        trade_ids = df[trade_col].dropna().unique()
        for trade_id in trade_ids:
            trade_mask = df[trade_col] == trade_id
            trade_rows = df[trade_mask]

            # Pick representative row: prefer stock leg for micro signals
            stock_rows = trade_rows[trade_rows.get("AssetType", pd.Series()) == "STOCK"] \
                if "AssetType" in trade_rows.columns else pd.DataFrame()
            rep_row = stock_rows.iloc[0] if not stock_rows.empty else trade_rows.iloc[0]

            ticker = str(rep_row.get(ticker_col, "") or "")
            micro  = micro_cache.get(ticker, {})

            state, drivers, drawdown_type, summary = _classify_thesis(rep_row, micro)

            df.loc[trade_mask, "Thesis_State"]         = state
            df.loc[trade_mask, "Thesis_Drivers"]       = json.dumps(drivers)
            df.loc[trade_mask, "Thesis_Drawdown_Type"] = drawdown_type
            df.loc[trade_mask, "Thesis_Summary"]       = summary
    else:
        # No TradeID — process row by row
        for idx, row in df.iterrows():
            ticker = str(row.get(ticker_col, "") or "")
            micro  = micro_cache.get(ticker, {})
            state, drivers, drawdown_type, summary = _classify_thesis(row, micro)
            df.at[idx, "Thesis_State"]         = state
            df.at[idx, "Thesis_Drivers"]       = json.dumps(drivers)
            df.at[idx, "Thesis_Drawdown_Type"] = drawdown_type
            df.at[idx, "Thesis_Summary"]       = summary

    intact  = (df["Thesis_State"] == THESIS_INTACT).sum()
    degraded= (df["Thesis_State"] == THESIS_DEGRADED).sum()
    broken  = (df["Thesis_State"] == THESIS_BROKEN).sum()
    unknown = (df["Thesis_State"] == THESIS_UNKNOWN).sum()
    logger.info(
        f"[ThesisEngine] {len(unique_tickers)} tickers evaluated — "
        f"INTACT={intact} DEGRADED={degraded} BROKEN={broken} UNKNOWN={unknown}"
    )

    return df


# ── Classification Core ───────────────────────────────────────────────────────

def _classify_thesis(
    row: pd.Series,
    micro: Dict,
) -> Tuple[str, List[Dict], str, str]:
    """
    Compute thesis score from chart states + micro signals.
    Returns (Thesis_State, drivers_list, drawdown_type, summary_str).
    """
    score    = 0.0
    drivers  = []   # [{signal, weight, note}]

    # ── Chart state signals (always available — no external calls) ─────────

    price_state = str(row.get("PriceStructure_State", "") or "").upper()
    trend_state = str(row.get("TrendIntegrity_State", "") or "").upper()
    momentum    = str(row.get("MomentumVelocity_State", "") or "").upper()
    rq_state    = str(row.get("RecoveryQuality_State", "") or "").upper()
    trend_prev  = str(row.get("TrendIntegrity_State_Prev", "") or "").upper()
    mom_days    = int(row.get("MomentumVelocity_State_Days", 0) or 0)
    drift_pct   = float(row.get("Price_Drift_Pct", 0) or 0)

    if "STRUCTURE_BROKEN" in price_state:
        w = _WEIGHTS["price_structure_broken"]
        score += w
        drivers.append({"signal": "price_structure_broken", "weight": w,
                         "note": f"PriceStructure={price_state}"})

    if trend_state in ("TREND_EXHAUSTED", "NO_TREND") and trend_prev in ("STRONG_TREND", "WEAK_TREND"):
        w = _WEIGHTS["trend_collapsed"]
        score += w
        drivers.append({"signal": "trend_collapsed", "weight": w,
                         "note": f"Trend: {trend_prev} → {trend_state}"})

    if momentum == "REVERSING" and mom_days >= 5:
        w = _WEIGHTS["momentum_reversing"]
        score += w
        drivers.append({"signal": "momentum_reversing", "weight": w,
                         "note": f"REVERSING for {mom_days} days"})

    if rq_state == "DEAD_CAT_BOUNCE":
        w = _WEIGHTS["recovery_dead_cat"]
        score += w
        drivers.append({"signal": "recovery_dead_cat", "weight": w,
                         "note": "Bounce without structural confirmation"})

    if rq_state == "STRUCTURAL_RECOVERY":
        w = _WEIGHTS["structural_recovery"]
        score += w
        drivers.append({"signal": "structural_recovery", "weight": w,
                         "note": "All recovery gates passed"})

    if trend_state == "WEAK_TREND" and trend_prev == "TREND_EXHAUSTED":
        w = _WEIGHTS["trend_restoring"]
        score += w
        drivers.append({"signal": "trend_restoring", "weight": w,
                         "note": f"Trend: {trend_prev} → {trend_state}"})

    if momentum == "ACCELERATING":
        w = _WEIGHTS["momentum_accelerating"]
        score += w
        drivers.append({"signal": "momentum_accelerating", "weight": w,
                         "note": "Momentum ACCELERATING"})

    hv_pct = float(row.get("hv_20d_percentile", 50) or 50)
    if hv_pct > 80:
        w = _WEIGHTS["high_vol_regime"]
        score += w
        drivers.append({"signal": "high_vol_regime", "weight": w,
                         "note": f"HV at {hv_pct:.0f}th pctile (fear)"})

    # ── Direction-adverse signal (LONG_PUT / LONG_CALL only) ─────────────
    # Passarelli Ch.2: "Story Check — is the thesis still intact?"
    # For directional long options, stock moving AGAINST the bet IS the thesis
    # failing, even if generic stock health looks fine.
    # Uses same magnitude thresholds as doctrine Gate 2b-dir for consistency.
    _strategy = str(row.get("Strategy", "") or "").upper()
    _is_long_directional = _strategy in ("LONG_PUT", "LONG_CALL")

    if _is_long_directional:
        _is_put_dir = "PUT" in _strategy
        _roc5 = float(row.get("roc_5", 0) or 0)
        _hv_20d_te = float(row.get("HV_20D", 0) or 0) if pd.notna(row.get("HV_20D")) else 0.0
        # Sigma-normalized adverse detection — same helper as doctrine Gate 2b-dir.
        # Ensures thesis_engine and doctrine agree on what constitutes "adverse."
        from core.management.cycle3.doctrine.helpers import compute_direction_adverse_signals
        _roc5_adverse, _drift_adverse, _roc5_z_te, _drift_z_te, _used_sigma_te = (
            compute_direction_adverse_signals(_roc5, drift_pct, _hv_20d_te, _is_put_dir)
        )
        if _used_sigma_te:
            _z_note = f" [roc5_z={_roc5_z_te:+.1f}σ, drift_z={_drift_z_te:+.1f}σ]"
        else:
            _z_note = " [HV missing — direction indeterminate]"

        if _roc5_adverse and _drift_adverse:
            w = _WEIGHTS["direction_adverse_severe"]
            score += w
            _dir_label = "UP" if _is_put_dir else "DOWN"
            drivers.append({"signal": "direction_adverse_severe", "weight": w,
                             "note": f"Stock {_dir_label} (ROC5={_roc5:+.1f}%, "
                                     f"Drift={drift_pct:+.1%}{_z_note}) against {_strategy}"})
        elif _roc5_adverse or _drift_adverse:
            w = _WEIGHTS["direction_adverse"]
            score += w
            _dir_label = "UP" if _is_put_dir else "DOWN"
            _sig = f"ROC5={_roc5:+.1f}%" if _roc5_adverse else f"Drift={drift_pct:+.1%}"
            drivers.append({"signal": "direction_adverse", "weight": w,
                             "note": f"Stock trending {_dir_label} ({_sig}{_z_note}) "
                                     f"against {_strategy}"})

    # ── Sector Relative Strength (z-score normalized, Natenberg Ch.8) ──────
    srs        = str(row.get("Sector_Relative_Strength", "") or "").upper()
    srs_z      = float(row.get("Sector_RS_ZScore", 0) or 0)
    srs_bench  = str(row.get("Sector_Benchmark", "SPY") or "SPY")

    if srs in (_SRS_UNDERPERFORMING, _SRS_MICRO_BREAKDOWN, _SRS_BROKEN):
        w = _WEIGHTS["sector_relative_weak"]
        score += w
        drivers.append({"signal": "sector_relative_weak", "weight": w,
                         "note": f"Sector RS={srs} (z={srs_z:.2f} vs {srs_bench})"})

    # ── Micro signals (from yfinance cache, may be empty) ─────────────────

    eps_surprise  = micro.get("eps_surprise_pct")   # float, negative = miss
    analyst_trend = micro.get("analyst_net_change")  # int, positive = net upgrades
    earnings_gap  = micro.get("post_earnings_gap_pct")  # float, negative = gap down
    gap_recovered = micro.get("gap_recovered")       # bool

    if eps_surprise is not None:
        if eps_surprise < -0.20:
            w = _WEIGHTS["earnings_miss_severe"]
            score += w
            drivers.append({"signal": "earnings_miss_severe", "weight": w,
                             "note": f"EPS miss {eps_surprise:.0%}"})
        elif eps_surprise < 0:
            w = _WEIGHTS["earnings_miss"]
            score += w
            drivers.append({"signal": "earnings_miss", "weight": w,
                             "note": f"EPS miss {eps_surprise:.0%}"})

    if analyst_trend is not None:
        if analyst_trend < -1:
            w = _WEIGHTS["analyst_downgrade_trend"]
            score += w
            drivers.append({"signal": "analyst_downgrade_trend", "weight": w,
                             "note": f"Net {analyst_trend} analyst changes (30d)"})
        elif analyst_trend > 1:
            w = _WEIGHTS["analyst_upgrade_trend"]
            score += w
            drivers.append({"signal": "analyst_upgrade_trend", "weight": w,
                             "note": f"Net +{analyst_trend} upgrades (30d)"})

    if earnings_gap is not None and earnings_gap < -0.05:
        w = _WEIGHTS["post_earnings_gap_down"]
        score += w
        drivers.append({"signal": "post_earnings_gap_down", "weight": w,
                         "note": f"Earnings gap down {earnings_gap:.1%}"})
        if gap_recovered:
            w2 = _WEIGHTS["post_earnings_recovered"]
            score += w2
            drivers.append({"signal": "post_earnings_recovered", "weight": w2,
                             "note": "Gap recovered in 5d — likely temporary"})

    # ── Score → State ──────────────────────────────────────────────────────

    score = max(0.0, score)   # clamp: positive signals can cancel negatives but floor at 0

    # Chart state availability check — all 4 primary states being empty/UNKNOWN
    # means price history never computed, not that the thesis is bad.
    _chart_available = any(
        s not in ("", "UNKNOWN", "N/A")
        for s in [price_state, trend_state, momentum, rq_state]
    )

    if score >= _BROKEN_THRESHOLD:
        state = THESIS_BROKEN
    elif score >= _DEGRADED_THRESHOLD:
        state = THESIS_DEGRADED
    elif not _chart_available:
        state = THESIS_UNKNOWN   # chart states not computed — genuinely can't assess
    else:
        # Chart states are available and show no concerning signals → INTACT.
        # yfinance micro signal failure is NOT a reason to mark thesis unknown
        # when Schwab price history is driving chart state computation.
        state = THESIS_INTACT

    # ── Structural vs Temporary classifier ────────────────────────────────

    drawdown_type = _classify_drawdown_type(
        state, drivers, drift_pct, micro,
        price_state, rq_state, hv_pct
    )

    # ── Summary ───────────────────────────────────────────────────────────

    summary = _build_summary(state, drawdown_type, drivers, score, micro)

    return state, drivers, drawdown_type, summary


def _classify_drawdown_type(
    state: str,
    drivers: List[Dict],
    drift_pct: float,
    micro: Dict,
    price_state: str,
    rq_state: str,
    hv_pct: float,
) -> str:
    """
    Classify whether the drawdown (if any) is structural, temporary, or macro.

    Matrix:
      STRUCTURAL: earnings miss + broken structure + analyst downgrades + no recovery
      TEMPORARY:  earnings gap but recovering, sector correction, market vol spike
      MACRO:      high vol regime + sector relative weakness, company-specific intact
      MIXED:      multiple causes present (e.g., macro + earnings)
    """
    if state == THESIS_INTACT or not drivers:
        return DRAWDOWN_UNKNOWN  # no drawdown to classify

    signal_names = {d["signal"] for d in drivers}

    has_structural = (
        "price_structure_broken" in signal_names
        or "trend_collapsed" in signal_names
        or "earnings_miss_severe" in signal_names
        or "analyst_downgrade_trend" in signal_names
    )
    has_temporary = (
        "post_earnings_recovered" in signal_names
        or rq_state == "STRUCTURAL_RECOVERY"
        or (
            "post_earnings_gap_down" in signal_names
            and "post_earnings_recovered" not in signal_names
            and "price_structure_broken" not in signal_names
        )
    )
    has_macro = (
        "high_vol_regime" in signal_names
        or "sector_relative_weak" in signal_names
    )
    has_earnings_miss = (
        "earnings_miss" in signal_names
        or "earnings_miss_severe" in signal_names
    )

    # Classify
    causes = sum([has_structural, has_temporary, has_macro])
    if causes >= 2:
        return DRAWDOWN_MIXED
    if has_structural and not has_temporary:
        return DRAWDOWN_STRUCTURAL
    if has_temporary and not has_structural:
        return DRAWDOWN_TEMPORARY
    if has_macro and not has_structural:
        return DRAWDOWN_MACRO

    return DRAWDOWN_UNKNOWN


def _build_summary(
    state: str,
    drawdown_type: str,
    drivers: List[Dict],
    score: float,
    micro: Dict,
) -> str:
    """
    One-liner for the position card.  Concise, actionable.
    """
    if state == THESIS_UNKNOWN:
        return "Thesis unknown — insufficient signal data."

    if state == THESIS_INTACT:
        pos_drivers = [d for d in drivers if d["weight"] < 0]
        if pos_drivers:
            return f"Thesis INTACT — positive signals: {', '.join(d['signal'] for d in pos_drivers[:2])}."
        return "Thesis INTACT — no structural concerns detected."

    top = sorted(drivers, key=lambda d: abs(d["weight"]), reverse=True)[:2]
    top_str = "; ".join(d["note"] for d in top if d["weight"] > 0)

    if state == THESIS_BROKEN:
        dtype_str = f" Drawdown type: {drawdown_type}." if drawdown_type != DRAWDOWN_UNKNOWN else ""
        return (
            f"Thesis BROKEN (score={score:.2f}){dtype_str} "
            f"Key signals: {top_str}. "
            f"Do not roll — exit consideration warranted (Passarelli Ch.2: story check)."
        )

    if state == THESIS_DEGRADED:
        dtype_str = f" ({drawdown_type})" if drawdown_type != DRAWDOWN_UNKNOWN else ""
        return (
            f"Thesis DEGRADED{dtype_str} (score={score:.2f}). "
            f"Watch: {top_str}. Roll with caution."
        )

    return ""


# ── Micro Signal Fetcher (Schwab + DuckDB primary, yfinance analyst-only) ─────

def _fetch_micro_signals(ticker: str) -> Dict:
    """
    Fetch company-specific micro signals.
    Returns dict with keys: eps_surprise_pct, analyst_net_change,
    post_earnings_gap_pct, gap_recovered, fetch_failed.

    Data sources (in priority order):
      - Schwab API: post-earnings gap detection (primary)
      - DuckDB earnings_history: EPS surprise (covers 569/571 tickers)
      - yfinance: analyst recommendations only (30d upgrade/downgrade trend)

    Gracefully returns {"fetch_failed": True} on any error.
    Rate-limited: skips if in backoff window.
    """
    global _YF_BACKOFF_UNTIL

    result: Dict = {"fetch_failed": True}

    # Check safe mode
    try:
        from core.shared.data_contracts.config import MANAGEMENT_SAFE_MODE
        if MANAGEMENT_SAFE_MODE:
            return result
    except Exception:
        pass

    if not ticker or ticker in ("nan", "None", ""):
        return result

    if time.time() < _YF_BACKOFF_UNTIL:
        logger.debug(f"[ThesisEngine] Skipping {ticker} — yfinance backoff active.")
        return result

    # ── Schwab fundamental fallback (post-earnings gap via price history) ──
    # Schwab exposes lastEarningsDate + EPS via get_quotes(fields="fundamental").
    # We use it to compute post-earnings gap when yfinance is unavailable.
    # This also marks fetch_failed=False so chart-only thesis is not blocked.
    try:
        from scan_engine.loaders.schwab_api_client import SchwabClient
        _sc = SchwabClient()
        _fund_data = _sc.get_quotes([ticker], fields="quote,fundamental")
        _fund = _fund_data.get(ticker, {}).get("fundamental", {})
        _last_earnings_raw = _fund.get("lastEarningsDate")
        if _last_earnings_raw:
            _edt = pd.Timestamp(_last_earnings_raw, tz="UTC")
            _now = pd.Timestamp.now(tz="UTC")
            _days_since = (_now - _edt).days
            if 0 <= _days_since <= 5:
                # Near earnings — fetch Schwab price history for gap detection
                try:
                    _ph = _sc.get_price_history(ticker, periodType="day", period=10, frequencyType="daily", frequency=1)
                    _candles = _ph.get("candles", [])
                    if len(_candles) >= 3:
                        _edt_date = _edt.date()
                        for _i, _c in enumerate(_candles):
                            _c_date = pd.Timestamp(_c["datetime"], unit="ms").date()
                            if _c_date >= _edt_date and _i > 0:
                                _prior_close = float(_candles[_i - 1]["close"])
                                _gap_pct = (float(_c["open"]) - _prior_close) / _prior_close
                                result["post_earnings_gap_pct"] = _gap_pct
                                _cur_close = float(_candles[-1]["close"])
                                result["gap_recovered"] = bool(_cur_close >= _prior_close * 0.99)
                                break
                except Exception:
                    pass
            result["fetch_failed"] = False   # Schwab available → chart states sufficient for thesis
    except Exception:
        pass   # Schwab unavailable — fall through to yfinance

    # ── EPS Surprise (DuckDB only — covers 569/571 tickers) ─────────────
    try:
        import duckdb as _thesis_duckdb
        from core.shared.data_contracts.config import PIPELINE_DB_PATH as _THESIS_DB
        _eps_con = _thesis_duckdb.connect(str(_THESIS_DB), read_only=True)
        try:
            _eps_row = _eps_con.execute("""
                SELECT eps_surprise_pct FROM earnings_history
                WHERE ticker = ? ORDER BY earnings_date DESC LIMIT 1
            """, [ticker]).fetchone()
            if _eps_row and _eps_row[0] is not None:
                result["eps_surprise_pct"] = float(_eps_row[0]) / 100.0
                result["fetch_failed"] = False
        finally:
            _eps_con.close()
    except Exception as e:
        logger.debug(f"[ThesisEngine] EPS surprise DuckDB lookup failed for {ticker}: {e}")

    # ── Analyst Recommendation Trend (yfinance — only remaining yf call) ──
    try:
        import yfinance as yf
        stock = yf.Ticker(ticker)
        recs = stock.recommendations
        if recs is not None and not recs.empty:
            cutoff = pd.Timestamp.now() - pd.Timedelta(days=30)
            if recs.index.tz is not None:
                cutoff = cutoff.tz_localize(recs.index.tz)
            recent = recs[recs.index >= cutoff]
            if not recent.empty:
                upgrades   = (recent.get("Action", pd.Series()) == "up").sum()
                downgrades = (recent.get("Action", pd.Series()) == "down").sum()
                result["analyst_net_change"] = int(upgrades) - int(downgrades)
    except Exception as e:
        err_str = str(e).lower()
        if "rate" in err_str or "429" in err_str or "too many" in err_str:
            logger.warning(f"[ThesisEngine] Rate limit hit for {ticker}. Backing off 120s.")
            _YF_BACKOFF_UNTIL = time.time() + 120
        else:
            logger.debug(f"[ThesisEngine] Analyst recs fetch failed for {ticker}: {e}")

    return result
