"""
Market Context Collector — fetches market-wide indicators and writes to DuckDB.

Collects: VIX, VIX_3M (^VXV), VVIX, SKEW, HYG, LQD, universe breadth, avg correlation.
Computes: term spread/ratio, VIX percentile, VIX SMA-20, composite regime.

Designed for daily collection at 15:50 ET via launchd (5 min after IV collection).
Source-robust: individual fetch failures degrade gracefully — system continues with
reduced confidence. Never blocks on missing data.

Pattern follows: scripts/cli/collect_iv_daily.py + scan_engine/iv_collector/rest_collector.py
"""

import json
import logging
import math
import numpy as np
import pandas as pd
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from core.shared.calendar.trading_calendar import is_trading_day
from core.shared.data_layer.market_context import (
    initialize_market_context_table,
    market_context_collected_today,
    write_market_context,
    query_vix_history,
)
from core.shared.data_layer.market_regime_classifier import classify_market_regime
from core.shared.data_contracts.config import DATA_DIR

logger = logging.getLogger(__name__)

# ── Status File ───────────────────────────────────────────────────────────────
_STATUS_PATH = DATA_DIR / "market_context_status.json"

# ── yfinance Symbols ──────────────────────────────────────────────────────────
# Source-agnostic: if ^VXV becomes unavailable, vix_3m will be None and
# confidence drops. No business logic depends on "VXV exists."
_YF_SYMBOLS = {
    "vix": "^VIX",
    "vix_3m": "^VIX3M",
    "vvix": "^VVIX",
    "skew": "^SKEW",
    "hyg": "HYG",
    "lqd": "LQD",
}


# ── Fetch Functions ───────────────────────────────────────────────────────────

def _fetch_index_data() -> dict:
    """
    Batch-fetch market index data via yfinance.

    Returns dict with keys matching _YF_SYMBOLS keys, values are latest close prices.
    Missing/failed symbols → None (never raises).
    """
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("[MarketCollector] yfinance not installed — cannot fetch indices")
        return {}

    result = {}
    symbols = list(_YF_SYMBOLS.values())
    try:
        # Batch download — single API call, ~5-10 seconds
        data = yf.download(
            " ".join(symbols),
            period="5d",
            group_by="ticker",
            progress=False,
            threads=True,
        )
        for key, symbol in _YF_SYMBOLS.items():
            try:
                if len(_YF_SYMBOLS) == 1:
                    # Single ticker — no multi-level columns
                    col = data["Close"]
                else:
                    col = data[(symbol, "Close")]
                val = col.dropna().iloc[-1] if not col.dropna().empty else None
                result[key] = float(val) if val is not None else None
            except (KeyError, IndexError, TypeError):
                result[key] = None
                logger.debug(f"[MarketCollector] Could not extract {symbol} ({key})")
    except Exception as e:
        logger.warning(f"[MarketCollector] yfinance batch download failed: {e}")
        # Try individual fetches as fallback
        for key, symbol in _YF_SYMBOLS.items():
            try:
                import yfinance as yf
                ticker = yf.Ticker(symbol)
                hist = ticker.history(period="5d")
                if not hist.empty:
                    result[key] = float(hist["Close"].dropna().iloc[-1])
                else:
                    result[key] = None
            except Exception:
                result[key] = None

    return result


def _compute_universe_breadth() -> dict:
    """
    Compute breadth from our ticker universe using price_history in DuckDB.

    Returns:
        universe_breadth_pct_sma50: % of tickers with close > SMA50
        universe_breadth_advancing_5d: % of tickers with positive 5d return
    """
    result = {
        "universe_breadth_pct_sma50": None,
        "universe_breadth_advancing_5d": None,
    }
    try:
        from core.shared.data_layer.duckdb_utils import (
            DbDomain, get_domain_connection,
        )
        con = get_domain_connection(DbDomain.CHART, read_only=True)
        try:
            # SMA50 breadth
            df = con.execute("""
                WITH latest_prices AS (
                    SELECT ticker, close_price,
                           AVG(close_price) OVER (
                               PARTITION BY ticker ORDER BY date ROWS 49 PRECEDING
                           ) AS sma50
                    FROM price_history
                    WHERE date >= CURRENT_DATE - INTERVAL '90 days'
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY ticker ORDER BY date DESC
                    ) = 1
                )
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE close_price > sma50) AS above_sma50
                FROM latest_prices
                WHERE sma50 IS NOT NULL
            """).fetchdf()

            if not df.empty and df.iloc[0]["total"] > 0:
                total = float(df.iloc[0]["total"])
                above = float(df.iloc[0]["above_sma50"])
                result["universe_breadth_pct_sma50"] = round(above / total * 100, 2)

            # 5d advancing
            df2 = con.execute("""
                WITH ranked AS (
                    SELECT ticker, close_price, date,
                           ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
                    FROM price_history
                    WHERE date >= CURRENT_DATE - INTERVAL '30 days'
                ),
                pairs AS (
                    SELECT a.ticker,
                           a.close_price AS latest,
                           b.close_price AS prev_5d
                    FROM ranked a
                    JOIN ranked b ON a.ticker = b.ticker AND b.rn = 6
                    WHERE a.rn = 1
                )
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE latest > prev_5d) AS advancing
                FROM pairs
            """).fetchdf()

            if not df2.empty and df2.iloc[0]["total"] > 0:
                total2 = float(df2.iloc[0]["total"])
                adv = float(df2.iloc[0]["advancing"])
                result["universe_breadth_advancing_5d"] = round(adv / total2 * 100, 2)

        finally:
            con.close()
    except Exception as e:
        logger.debug(f"[MarketCollector] Breadth computation failed: {e}")

    return result


def _compute_avg_correlation(max_tickers: int = 100) -> Optional[float]:
    """
    Compute mean pairwise correlation of daily returns over 60 trading days.

    Uses upper triangle of correlation matrix. Caps at *max_tickers* with most
    history to limit compute time.
    """
    try:
        from core.shared.data_layer.duckdb_utils import (
            DbDomain, get_domain_connection,
        )
        con = get_domain_connection(DbDomain.CHART, read_only=True)
        try:
            # Get tickers with enough history (at least 40 rows in last 90 days)
            tickers_df = con.execute(f"""
                SELECT ticker, COUNT(*) AS cnt
                FROM price_history
                WHERE date >= CURRENT_DATE - INTERVAL '90 days'
                GROUP BY ticker
                HAVING COUNT(*) >= 40
                ORDER BY cnt DESC
                LIMIT {max_tickers}
            """).fetchdf()

            if len(tickers_df) < 5:
                return None

            ticker_list = tickers_df["ticker"].tolist()
            placeholders = ", ".join([f"'{t}'" for t in ticker_list])

            prices_df = con.execute(f"""
                SELECT ticker, date, close_price
                FROM price_history
                WHERE ticker IN ({placeholders})
                  AND date >= CURRENT_DATE - INTERVAL '90 days'
                ORDER BY date
            """).fetchdf()

            if prices_df.empty:
                return None

            # Pivot to wide format: rows=dates, cols=tickers
            pivot = prices_df.pivot(index="date", columns="ticker", values="close_price")
            # Daily returns
            returns = pivot.pct_change().dropna(how="all")
            # Drop tickers with too few returns
            returns = returns.dropna(axis=1, thresh=40)

            if returns.shape[1] < 5:
                return None

            # Correlation matrix
            corr = returns.corr()
            # Upper triangle mean (excluding diagonal)
            mask = np.triu(np.ones(corr.shape, dtype=bool), k=1)
            upper = corr.values[mask]
            upper = upper[~np.isnan(upper)]

            if len(upper) == 0:
                return None

            return round(float(np.mean(upper)), 4)
        finally:
            con.close()
    except Exception as e:
        logger.debug(f"[MarketCollector] Correlation computation failed: {e}")
        return None


def _compute_vix_percentile(vix_today: float) -> Optional[float]:
    """Compute 252-day percentile rank of today's VIX.

    Primary source: stored market_context_daily VIX history.
    Bootstrap fallback: yfinance ^VIX when stored history < 50 days.
    """
    history = query_vix_history(days=252)
    if len(history) < 50:
        # Bootstrap from yfinance — stored history is still building up
        try:
            import yfinance as yf
            _vix_df = yf.download("^VIX", period="1y", progress=False, auto_adjust=True)
            if _vix_df is not None and not _vix_df.empty:
                _vix_close = _vix_df["Close"].dropna()
                if hasattr(_vix_close, "columns"):
                    _vix_close = _vix_close.iloc[:, 0]
                if len(_vix_close) >= 50:
                    history = _vix_close
                    logger.info("[MarketCtx] VIX percentile bootstrapped from yfinance (%d days)", len(history))
        except Exception as e:
            logger.debug("[MarketCtx] yfinance VIX bootstrap failed: %s", e)
    if len(history) < 10:
        return None
    below = (history < vix_today).sum()
    return round(float(below) / len(history) * 100, 2)


def _compute_vix_sma20() -> Optional[float]:
    """Compute 20-day SMA of VIX from stored history."""
    history = query_vix_history(days=20)
    if len(history) < 10:
        return None
    return round(float(history.tail(20).mean()), 4)


def _write_status(ok: bool, message: str, data: dict | None = None) -> None:
    """Write collection status JSON for dashboard."""
    status = {
        "ok": ok,
        "message": message,
        "collection_ts": datetime.now().isoformat(),
        "date": str(date.today()),
    }
    if data:
        status["summary"] = {
            k: v for k, v in data.items()
            if k in ("vix", "vix_3m", "vvix", "skew", "market_regime",
                      "regime_score", "regime_confidence")
        }
    try:
        _STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
        _STATUS_PATH.write_text(json.dumps(status, indent=2, default=str))
    except Exception as e:
        logger.debug(f"[MarketCollector] Status write failed: {e}")


# ── Main Collector ────────────────────────────────────────────────────────────

def collect_market_context(force: bool = False) -> dict:
    """
    Collect market-wide indicators and write to DuckDB.

    Args:
        force: If True, bypass holiday guard and duplicate guard.

    Returns:
        {"ok": bool, "message": str, "data": dict | None}
    """
    today = date.today()

    # Holiday guard
    if not force and not is_trading_day(today):
        reason = "weekend" if today.weekday() >= 5 else "NYSE holiday"
        msg = f"Today ({today}) is a {reason} — skipping market context collection."
        logger.info(f"[MarketCollector] {msg}")
        return {"ok": True, "message": msg, "data": None}

    # Duplicate guard
    if not force and market_context_collected_today(today):
        msg = f"Market context already collected for {today}."
        logger.info(f"[MarketCollector] {msg}")
        return {"ok": True, "message": msg, "data": None}

    logger.info(f"[MarketCollector] Starting collection for {today}")

    # 1. Fetch index data from yfinance
    indices = _fetch_index_data()
    vix = indices.get("vix")
    vix_3m = indices.get("vix_3m")
    vvix = indices.get("vvix")
    skew = indices.get("skew")
    hyg = indices.get("hyg")
    lqd = indices.get("lqd")

    # 2. Compute derived fields
    vix_term_spread = None
    vix_term_ratio = None
    if vix is not None and vix_3m is not None and vix_3m > 0:
        vix_term_spread = round(vix_3m - vix, 4)
        vix_term_ratio = round(vix / vix_3m, 4)

    credit_spread_proxy = None
    if hyg is not None and lqd is not None and lqd > 0:
        credit_spread_proxy = round(hyg / lqd, 6)

    # 3. Universe breadth
    breadth = _compute_universe_breadth()

    # 4. Average correlation
    avg_corr = _compute_avg_correlation(max_tickers=100)

    # 5. VIX percentile + SMA
    vix_pctl = _compute_vix_percentile(vix) if vix is not None else None
    vix_sma = _compute_vix_sma20()

    # 6. Build context for classifier
    ctx = {
        "vix": vix,
        "vix_3m": vix_3m,
        "vvix": vvix,
        "skew": skew,
        "vix_term_spread": vix_term_spread,
        "vix_term_ratio": vix_term_ratio,
        "credit_spread_proxy": credit_spread_proxy,
        "hyg_price": hyg,
        "lqd_price": lqd,
        "universe_breadth_pct_sma50": breadth.get("universe_breadth_pct_sma50"),
        "universe_breadth_advancing_5d": breadth.get("universe_breadth_advancing_5d"),
        "avg_correlation": avg_corr,
        "vix_percentile_252d": vix_pctl,
        "vix_sma_20": vix_sma,
        "staleness_bdays": 0,  # freshly collected
    }

    # 7. Classify regime
    regime = classify_market_regime(ctx)
    ctx["market_regime"] = regime.regime
    ctx["regime_score"] = regime.score
    ctx["regime_confidence"] = regime.confidence
    ctx["regime_basis"] = "COMPOSITE"
    ctx["regime_detail_json"] = {
        "components": regime.components,
        "missing": [k for k, v in regime.components.items() if not v.get("present")],
        "raw_score": regime.score,
        "confidence": regime.confidence,
        "regime": regime.regime,
        "basis": "COMPOSITE",
    }
    ctx["collection_ts"] = datetime.now()
    ctx["source"] = "yfinance"

    # 8. Write to DuckDB
    try:
        write_market_context(ctx, d=today)
        msg = (
            f"Market context collected: VIX={vix}, Regime={regime.regime} "
            f"(score={regime.score:.1f}, conf={regime.confidence:.2f})"
        )
        logger.info(f"[MarketCollector] {msg}")
        _write_status(True, msg, ctx)

        # 9. Record macro event impact if today is an event day
        try:
            from core.shared.data_layer.macro_impact_collector import collect_macro_impact
            macro_result = collect_macro_impact(d=today, force=force)
            if macro_result["events_processed"] > 0:
                msg += f" | Macro: {macro_result['message']}"
                logger.info(f"[MarketCollector] {macro_result['message']}")
        except Exception as e:
            logger.debug(f"[MarketCollector] Macro impact collection failed (non-blocking): {e}")

        return {"ok": True, "message": msg, "data": ctx}
    except Exception as e:
        msg = f"Failed to write market context: {e}"
        logger.error(f"[MarketCollector] {msg}")
        _write_status(False, msg)
        return {"ok": False, "message": msg, "data": None}
