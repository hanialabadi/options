"""
Intraday Execution Check (Cycle 3)
====================================
Post-Step 12 validation layer that fetches real-time intraday data for READY
candidates and scores their execution readiness.

Checks:
    1. VWAP Location — is price above/below session VWAP? (direction-aware)
    2. Intraday Momentum — 30-minute slope direction (direction-aware)
    3. Spread Quality — live bid/ask spread on the specific contract
    4. IV Spike — live IV vs daily IV_30D for sudden changes (strategy-aware)

Outputs (informational only — does NOT change Execution_Status):
    - Intraday_VWAP_Signal:      FAVORABLE / UNFAVORABLE / NEUTRAL / NO_DATA / OFF_HOURS
    - Intraday_Momentum:         ALIGNED / OPPOSING / FLAT / NO_DATA / OFF_HOURS
    - Intraday_Spread_Quality:   TIGHT / NORMAL / WIDE / ILLIQUID / NO_DATA / OFF_HOURS
    - Intraday_IV_Spike:         SPIKE_FAVORABLE / SPIKE_UNFAVORABLE / STABLE / NO_DATA / OFF_HOURS
    - IV_Spike_Pct:              float (percentage change)
    - Intraday_Execution_Score:  0-100 composite
    - Intraday_Readiness:        EXECUTE_NOW / STAGE_AND_WAIT / DEFER / OFF_HOURS / N/A

Called from: scan_engine/pipeline.py (after Strategy Overlap, before DuckDB persist)

Doctrine sources:
    - Passarelli (Trading Options Greeks Ch.6): VWAP as institutional flow reference
    - Sinclair (Option Trading Ch.5): spread quality gates for execution
    - Natenberg (Option Volatility Ch.8): IV regime awareness for entry timing
"""

from __future__ import annotations

import logging
import time as _time
from datetime import datetime, time, date
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Strategy bias classification ─────────────────────────────────────────────
_BULLISH_STRATEGIES = {
    'Cash Secured Put', 'CSP', 'Buy-Write', 'BW', 'BUY_WRITE',
    'Long Call', 'LONG_CALL', 'Covered Call', 'CC', 'COVERED_CALL',
    'Bull Put Spread', 'Bull Call Spread',
}
_BEARISH_STRATEGIES = {
    'Long Put', 'LONG_PUT', 'Bear Call Spread', 'Bear Put Spread',
}
_NEUTRAL_STRATEGIES = {
    'Iron Condor', 'Straddle', 'Strangle', 'Calendar Spread',
    'Iron Butterfly', 'IRON_CONDOR',
}

# ── Spread quality thresholds ────────────────────────────────────────────────
_SPREAD_TIGHT = 3.0       # < 3%
_SPREAD_NORMAL = 8.0      # 3-8%
_SPREAD_WIDE = 15.0       # 8-15%
# > 15% = ILLIQUID

# ── IV spike thresholds ──────────────────────────────────────────────────────
_IV_SPIKE_UP_PCT = 15.0   # > +15% = spike up
_IV_SPIKE_DOWN_PCT = -10.0  # < -10% = spike down

# ── Composite score weights ──────────────────────────────────────────────────
_SCORE_MAP = {
    'vwap': {'FAVORABLE': 25, 'NEUTRAL': 15, 'UNFAVORABLE': 5, 'NO_DATA': 15},
    'momentum': {'ALIGNED': 25, 'FLAT': 15, 'OPPOSING': 5, 'NO_DATA': 15},
    'spread': {'TIGHT': 25, 'NORMAL': 20, 'WIDE': 10, 'ILLIQUID': 0, 'NO_DATA': 15},
    'iv_spike': {'SPIKE_FAVORABLE': 25, 'STABLE': 20, 'SPIKE_UNFAVORABLE': 5, 'NO_DATA': 15},
}

# ── Readiness thresholds ─────────────────────────────────────────────────────
_EXECUTE_NOW_THRESHOLD = 70
_STAGE_AND_WAIT_THRESHOLD = 50

# ── Momentum slope threshold ─────────────────────────────────────────────────
_MOMENTUM_FLAT_PCT = 0.05  # |slope_pct| < 0.05% per bar → FLAT

# ── Off-hours / N/A defaults ─────────────────────────────────────────────────
_INTRADAY_COLUMNS = [
    'Intraday_VWAP_Signal', 'Intraday_Momentum', 'Intraday_Spread_Quality',
    'Intraday_IV_Spike', 'IV_Spike_Pct', 'Intraday_Execution_Score',
    'Intraday_Readiness',
]


def _get_strategy_bias(row: pd.Series) -> str:
    """Classify strategy as bullish/bearish/neutral from Strategy_Name or Trade_Bias."""
    strategy = str(row.get('Strategy_Name', '') or '').strip()
    trade_bias = str(row.get('Trade_Bias', '') or '').strip().upper()

    if strategy in _BULLISH_STRATEGIES:
        return 'BULLISH'
    if strategy in _BEARISH_STRATEGIES:
        return 'BEARISH'
    if strategy in _NEUTRAL_STRATEGIES:
        return 'NEUTRAL'

    # Fallback to Trade_Bias column
    if trade_bias in ('BULLISH', 'BULLISH_STRONG', 'BULLISH_MODERATE'):
        return 'BULLISH'
    if trade_bias in ('BEARISH', 'BEARISH_STRONG', 'BEARISH_MODERATE'):
        return 'BEARISH'

    return 'NEUTRAL'


def _is_income_strategy(row: pd.Series) -> bool:
    """Return True if the strategy is income-oriented (selling premium)."""
    strategy_type = str(row.get('Strategy_Type', '') or '').strip().upper()
    strategy_name = str(row.get('Strategy_Name', '') or '').strip()
    if strategy_type == 'INCOME':
        return True
    if strategy_name in ('Cash Secured Put', 'CSP', 'Buy-Write', 'BW',
                         'BUY_WRITE', 'Covered Call', 'CC', 'COVERED_CALL',
                         'Iron Condor', 'IRON_CONDOR'):
        return True
    return False


def _is_market_open(now_et: Optional[datetime] = None) -> bool:
    """Check if US equity market is open (9:35-15:55 ET, weekdays).

    Uses 9:35 (not 9:30) to allow opening bars to form.
    Uses 15:55 (not 16:00) to avoid close auction noise.
    """
    if now_et is None:
        try:
            from zoneinfo import ZoneInfo
        except ImportError:
            from backports.zoneinfo import ZoneInfo
        now_et = datetime.now(ZoneInfo("US/Eastern"))

    if now_et.weekday() >= 5:
        return False
    return time(9, 35) <= now_et.time() <= time(15, 55)


# ── Data fetching ────────────────────────────────────────────────────────────

def _fetch_intraday_bars(ticker: str, schwab_client) -> list[dict]:
    """Fetch today's 5-min bars from Schwab for a single ticker."""
    try:
        today = date.today()
        start_ms = int(_time.mktime(
            datetime(today.year, today.month, today.day, 9, 25).timetuple()
        ) * 1000)
        end_ms = int(_time.time() * 1000)

        ph = schwab_client.get_price_history(
            ticker,
            frequencyType="minute",
            frequency=5,
            startDate=start_ms,
            endDate=end_ms,
        )
        return ph.get("candles", [])
    except Exception as e:
        logger.warning(f"[IntradayCheck] Failed to fetch bars for {ticker}: {e}")
        return []


def _fetch_live_quotes(tickers: list[str], schwab_client) -> dict:
    """Fetch batched live quotes for equity tickers."""
    try:
        raw = schwab_client.get_quotes(tickers, fields="quote")
        result = {}
        for t in tickers:
            q = (raw.get(t, {}) or {}).get("quote", {})
            result[t] = q
        return result
    except Exception as e:
        logger.warning(f"[IntradayCheck] Failed to fetch quotes: {e}")
        return {}


def _fetch_option_quotes(occ_symbols: list[str], schwab_client) -> dict:
    """Fetch live bid/ask for OCC option symbols."""
    if not occ_symbols:
        return {}
    try:
        raw = schwab_client.get_quotes(occ_symbols, fields="quote")
        result = {}
        for sym in occ_symbols:
            q = (raw.get(sym, {}) or {}).get("quote", {})
            bid = float(q.get("bidPrice") or q.get("bid") or 0)
            ask = float(q.get("askPrice") or q.get("ask") or 0)
            mid = (bid + ask) / 2 if (bid + ask) > 0 else 0
            spread_pct = (ask - bid) / mid * 100 if mid > 0 else None
            iv = float(q.get("volatility") or q.get("impliedVolatility") or 0)
            result[sym] = {
                "bid": bid, "ask": ask, "mid": mid,
                "spread_pct": spread_pct, "iv": iv,
            }
        return result
    except Exception as e:
        logger.warning(f"[IntradayCheck] Failed to fetch option quotes: {e}")
        return {}


# ── Individual checks ────────────────────────────────────────────────────────

def compute_vwap(bars: list[dict]) -> Optional[float]:
    """Compute session VWAP from 5-min bars (typical price × volume)."""
    if not bars:
        return None
    try:
        tp = np.array([(b["high"] + b["low"] + b["close"]) / 3 for b in bars])
        vol = np.array([b["volume"] for b in bars], dtype=float)
        vol_sum = vol.sum()
        if vol_sum <= 0:
            return None
        return float(np.sum(tp * vol) / vol_sum)
    except (KeyError, TypeError, ValueError):
        return None


def check_vwap_signal(last_price: float, vwap: Optional[float], bias: str) -> str:
    """Score VWAP location relative to strategy bias."""
    if vwap is None or last_price <= 0:
        return 'NO_DATA'

    above_vwap = last_price > vwap

    if bias == 'NEUTRAL':
        return 'NEUTRAL'
    if bias == 'BULLISH':
        return 'FAVORABLE' if above_vwap else 'UNFAVORABLE'
    if bias == 'BEARISH':
        return 'FAVORABLE' if not above_vwap else 'UNFAVORABLE'

    return 'NEUTRAL'


def check_momentum(bars: list[dict], bias: str) -> str:
    """Compute 30-min slope (last 6 bars) and compare to strategy direction."""
    if not bars or len(bars) < 3:
        return 'NO_DATA'

    try:
        recent = [float(b["close"]) for b in bars[-6:]]
        if len(recent) < 3:
            return 'NO_DATA'
        x = np.arange(len(recent))
        slope = np.polyfit(x, recent, 1)[0]
        slope_pct = slope / recent[0] * 100 if recent[0] > 0 else 0.0

        if abs(slope_pct) < _MOMENTUM_FLAT_PCT:
            return 'FLAT'

        positive_slope = slope_pct > 0

        if bias == 'NEUTRAL':
            return 'FLAT'
        if bias == 'BULLISH':
            return 'ALIGNED' if positive_slope else 'OPPOSING'
        if bias == 'BEARISH':
            return 'ALIGNED' if not positive_slope else 'OPPOSING'

        return 'FLAT'
    except (np.linalg.LinAlgError, ValueError, TypeError):
        return 'NO_DATA'


def check_spread_quality(live_spread_pct: Optional[float],
                         step10_spread_pct: Optional[float]) -> tuple[str, bool]:
    """Grade live spread and detect widening vs Step 10 snapshot."""
    if live_spread_pct is None:
        return 'NO_DATA', False

    if live_spread_pct < _SPREAD_TIGHT:
        grade = 'TIGHT'
    elif live_spread_pct < _SPREAD_NORMAL:
        grade = 'NORMAL'
    elif live_spread_pct < _SPREAD_WIDE:
        grade = 'WIDE'
    else:
        grade = 'ILLIQUID'

    widened = False
    if (step10_spread_pct is not None and step10_spread_pct > 0
            and live_spread_pct > step10_spread_pct * 1.5):
        widened = True

    return grade, widened


def check_iv_spike(live_iv: float, iv_30d: float,
                   is_income: bool) -> tuple[str, float]:
    """Detect IV spike and classify as favorable/unfavorable per strategy type."""
    if live_iv <= 0 or iv_30d <= 0:
        return 'NO_DATA', 0.0

    spike_pct = (live_iv - iv_30d) / iv_30d * 100

    if spike_pct > _IV_SPIKE_UP_PCT:
        # IV spiked up — good for sellers, bad for buyers
        return ('SPIKE_FAVORABLE' if is_income else 'SPIKE_UNFAVORABLE'), spike_pct
    elif spike_pct < _IV_SPIKE_DOWN_PCT:
        # IV dropped — bad for sellers, good for buyers
        return ('SPIKE_UNFAVORABLE' if is_income else 'SPIKE_FAVORABLE'), spike_pct
    else:
        return 'STABLE', spike_pct


def compute_composite_score(vwap_signal: str, momentum: str,
                            spread_quality: str, iv_spike: str) -> int:
    """Compute 0-100 composite execution readiness score."""
    score = 0
    score += _SCORE_MAP['vwap'].get(vwap_signal, 15)
    score += _SCORE_MAP['momentum'].get(momentum, 15)
    score += _SCORE_MAP['spread'].get(spread_quality, 15)
    score += _SCORE_MAP['iv_spike'].get(iv_spike, 15)
    return min(100, max(0, score))


def classify_readiness(score: int) -> str:
    """Classify execution readiness from composite score."""
    if score >= _EXECUTE_NOW_THRESHOLD:
        return 'EXECUTE_NOW'
    elif score >= _STAGE_AND_WAIT_THRESHOLD:
        return 'STAGE_AND_WAIT'
    else:
        return 'DEFER'


# ── Main entry point ─────────────────────────────────────────────────────────

def evaluate_intraday_readiness(
    df: pd.DataFrame,
    schwab_client=None,
    now_et: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Evaluate intraday execution readiness for READY scan candidates.

    Adds 7 informational columns. Does NOT change Execution_Status.

    Parameters
    ----------
    df : pd.DataFrame
        The acceptance_all DataFrame from Step 12.
    schwab_client : SchwabClient or None
        Live Schwab API client. If None, all checks return OFF_HOURS.
    now_et : datetime or None
        Override for current Eastern time (for testing). If None, uses real clock.

    Returns
    -------
    pd.DataFrame
        Same DataFrame with 7 new intraday columns added.
    """
    df = df.copy()

    # Initialize all columns with defaults
    for col in _INTRADAY_COLUMNS:
        if col == 'IV_Spike_Pct':
            df[col] = np.nan
        elif col == 'Intraday_Execution_Score':
            df[col] = np.nan
        else:
            df[col] = 'N/A'

    # Identify READY rows
    ready_mask = df.get('Execution_Status', pd.Series(dtype=str)) == 'READY'
    if not ready_mask.any():
        logger.info("[IntradayCheck] No READY candidates — skipping")
        return df

    ready_count = ready_mask.sum()

    # Market hours gate
    if not _is_market_open(now_et):
        logger.info(f"[IntradayCheck] Market closed — setting {ready_count} READY rows to OFF_HOURS")
        for col in _INTRADAY_COLUMNS:
            if col == 'IV_Spike_Pct':
                df.loc[ready_mask, col] = np.nan
            elif col == 'Intraday_Execution_Score':
                df.loc[ready_mask, col] = np.nan
            else:
                df.loc[ready_mask, col] = 'OFF_HOURS'
        return df

    # No Schwab client → OFF_HOURS
    if schwab_client is None:
        logger.info(f"[IntradayCheck] No Schwab client — setting {ready_count} READY rows to OFF_HOURS")
        for col in _INTRADAY_COLUMNS:
            if col == 'IV_Spike_Pct':
                df.loc[ready_mask, col] = np.nan
            elif col == 'Intraday_Execution_Score':
                df.loc[ready_mask, col] = np.nan
            else:
                df.loc[ready_mask, col] = 'OFF_HOURS'
        return df

    # ── Gather unique tickers from READY rows ────────────────────────────
    ticker_col = 'Ticker' if 'Ticker' in df.columns else None
    if ticker_col is None:
        logger.warning("[IntradayCheck] No Ticker column — skipping")
        return df

    ready_tickers = df.loc[ready_mask, ticker_col].dropna().unique().tolist()
    logger.info(f"[IntradayCheck] Evaluating {len(ready_tickers)} tickers: {ready_tickers}")

    # ── Batch fetch: equity quotes ───────────────────────────────────────
    equity_quotes = _fetch_live_quotes(ready_tickers, schwab_client)

    # ── Per-ticker: fetch 5-min bars (throttled 1/sec) ───────────────────
    bars_by_ticker = {}
    for ticker in ready_tickers:
        bars_by_ticker[ticker] = _fetch_intraday_bars(ticker, schwab_client)
        _time.sleep(0.5)  # respect API throttle

    # ── Batch fetch: option quotes for contract symbols ──────────────────
    contract_col = 'Contract_Symbol'
    occ_symbols = []
    if contract_col in df.columns:
        occ_symbols = df.loc[ready_mask, contract_col].dropna().unique().tolist()
    option_quotes = _fetch_option_quotes(occ_symbols, schwab_client) if occ_symbols else {}

    # ── Evaluate each READY row ──────────────────────────────────────────
    for idx in df.index[ready_mask]:
        row = df.loc[idx]
        ticker = str(row.get(ticker_col, ''))
        bias = _get_strategy_bias(row)
        is_income = _is_income_strategy(row)

        # Get data for this ticker
        quote = equity_quotes.get(ticker, {})
        bars = bars_by_ticker.get(ticker, [])
        last_price = float(quote.get('lastPrice') or quote.get('mark') or 0)

        # Check 1: VWAP
        vwap = compute_vwap(bars)
        vwap_signal = check_vwap_signal(last_price, vwap, bias)

        # Check 2: Momentum
        momentum = check_momentum(bars, bias)

        # Check 3: Spread quality
        contract_sym = str(row.get(contract_col, '') or '')
        opt_q = option_quotes.get(contract_sym, {})
        live_spread = opt_q.get('spread_pct')
        step10_spread = None
        if 'Bid_Ask_Spread_Pct' in df.columns:
            s10 = row.get('Bid_Ask_Spread_Pct')
            if pd.notna(s10):
                step10_spread = float(s10)
        spread_grade, spread_widened = check_spread_quality(live_spread, step10_spread)

        # Check 4: IV spike
        live_iv = opt_q.get('iv', 0)
        iv_30d = 0.0
        for iv_col in ['IV_30D', 'iv_30d', 'IV30']:
            if iv_col in df.columns and pd.notna(row.get(iv_col)):
                iv_30d = float(row.get(iv_col))
                break
        iv_spike_signal, iv_spike_pct = check_iv_spike(live_iv, iv_30d, is_income)

        # Composite score
        score = compute_composite_score(vwap_signal, momentum, spread_grade, iv_spike_signal)

        # Calendar risk adjustment — Friday/holiday theta awareness, DTE-scaled
        _cal_flag = str(row.get('Calendar_Risk_Flag', '') or '').upper()
        if _cal_flag in ('HIGH_BLEED', 'ELEVATED_BLEED', 'PRE_HOLIDAY_EDGE', 'ADVANTAGEOUS'):
            _cal_dte = float(row.get('Actual_DTE', 0) or 0)
            _cal_theta_f = min(1.0, 45.0 / _cal_dte) if _cal_dte > 0 else 1.0
            _cal_adj = {
                'HIGH_BLEED': -25, 'ELEVATED_BLEED': -20,
                'PRE_HOLIDAY_EDGE': 20, 'ADVANTAGEOUS': 15,
            }[_cal_flag]
            score = max(0, min(100, score + int(round(_cal_adj * _cal_theta_f))))

        readiness = classify_readiness(score)

        # Write results
        df.at[idx, 'Intraday_VWAP_Signal'] = vwap_signal
        df.at[idx, 'Intraday_Momentum'] = momentum
        df.at[idx, 'Intraday_Spread_Quality'] = spread_grade
        df.at[idx, 'Intraday_IV_Spike'] = iv_spike_signal
        df.at[idx, 'IV_Spike_Pct'] = iv_spike_pct
        df.at[idx, 'Intraday_Execution_Score'] = score
        df.at[idx, 'Intraday_Readiness'] = readiness

        logger.info(
            f"[IntradayCheck] {ticker} {row.get('Strategy_Name', '?')}: "
            f"VWAP={vwap_signal} Mom={momentum} Spread={spread_grade} "
            f"IV={iv_spike_signal}({iv_spike_pct:+.1f}%) → {readiness} (score={score})"
        )

    logger.info(
        f"[IntradayCheck] Complete: {ready_count} candidates evaluated, "
        f"{(df.loc[ready_mask, 'Intraday_Readiness'] == 'EXECUTE_NOW').sum()} EXECUTE_NOW, "
        f"{(df.loc[ready_mask, 'Intraday_Readiness'] == 'STAGE_AND_WAIT').sum()} STAGE_AND_WAIT, "
        f"{(df.loc[ready_mask, 'Intraday_Readiness'] == 'DEFER').sum()} DEFER"
    )

    return df
