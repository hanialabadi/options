"""
Live Price Provider
===================
Overlays fresh prices on top of broker CSV position data.

Design:
  1. Market open  → always call Schwab get_quotes() (one lightweight batch call).
  2. Market closed → use scan_results_latest if fresh (≤2h), else broker CSV.
  3. Three-tier fallback:
       a. Live Schwab quote           (market hours)
       b. scan_results_latest DuckDB  (off-hours, scan ran recently)
       c. Broker CSV UL Last          (always available — original baseline)
  4. No chain calls — quotes only; options IV handled by governed_iv_provider.
  5. Audit log    — price_refresh_log table for every run.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Thresholds
_SCAN_MAX_AGE_HOURS   = 2       # scan_results_latest is stale after 2h

_CREATE_LOG_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS price_refresh_log (
    log_id      VARCHAR PRIMARY KEY,
    fetch_ts    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ticker      VARCHAR,
    old_price   DOUBLE,
    new_price   DOUBLE,
    delta_pct   DOUBLE,
    source      VARCHAR,
    market_open BOOLEAN
)
"""


class LivePriceProvider:
    """
    Orchestrates smart price refresh for management pipeline.

    Usage in run_all.py:
        provider = LivePriceProvider()
        if provider.should_refresh(df_enriched):
            live_prices = provider.fetch_live_prices(tickers, schwab_client)
            df_enriched = provider.apply_to_df(df_enriched, live_prices)
    """

    def __init__(self):
        self._ensure_log_table()

    # ── Public API ────────────────────────────────────────────────────────────

    def should_refresh(self, df: pd.DataFrame, last_fetch_ts: Optional[datetime] = None) -> bool:
        """
        Returns True when live prices should be fetched:
          - Market open → always True (one lightweight get_quotes() call)
          - Market closed → True only if scan cache is fresh (≤2h) as Tier 2 fallback
        """
        from core.shared.data_layer.market_time import is_market_open
        if is_market_open():
            logger.info("[LivePriceProvider] Market open — refreshing live prices.")
            return True

        # Off-hours: check if scan cache has fresh data worth pulling
        if self._scan_cache_is_fresh():
            logger.info("[LivePriceProvider] Market closed but scan cache fresh — refreshing from cache.")
            return True

        logger.debug("[LivePriceProvider] Market closed, no fresh cache — using broker CSV.")
        return False

    def fetch_live_prices(
        self,
        tickers: List[str],
        schwab_client=None,
    ) -> Dict[str, Dict]:
        """
        Returns {ticker: {'ul_last': float, 'source': str}} for all tickers.
        Tries: Schwab → scan_results_latest → broker CSV.
        """
        if not tickers:
            return {}

        from core.shared.data_layer.market_time import is_market_open
        market_open = is_market_open()

        prices: Dict[str, Dict] = {}

        # ── Tier 1: Live Schwab (market hours only) ─────────────────────────
        if market_open and schwab_client is not None:
            prices = self._fetch_from_schwab(tickers, schwab_client)

        # ── Tier 2: scan_results_latest (Schwab failed or off-hours) ────────
        missing = [t for t in tickers if t not in prices]
        if missing:
            scan_prices = self._fetch_from_scan_cache(missing)
            prices.update(scan_prices)

        logger.info(
            f"[LivePriceProvider] Fetched prices for {len(prices)}/{len(tickers)} tickers. "
            f"Sources: {set(v['source'] for v in prices.values())}"
        )
        return prices

    def apply_to_df(self, df: pd.DataFrame, live_prices: Dict[str, Dict]) -> pd.DataFrame:
        """
        Overwrite 'UL Last' with live prices where available.
        Adds 'Price_Source' column.
        """
        if 'Price_Source' not in df.columns:
            df['Price_Source'] = 'broker_csv'

        ticker_col = 'Underlying_Ticker' if 'Underlying_Ticker' in df.columns else 'Ticker'
        if ticker_col not in df.columns:
            return df

        log_rows = []
        now = datetime.now(tz=timezone.utc)

        for ticker, info in live_prices.items():
            mask = df[ticker_col] == ticker
            if not mask.any():
                continue

            old_price = df.loc[mask, 'UL Last'].iloc[0] if 'UL Last' in df.columns else None
            new_price = info.get('ul_last')
            source    = info.get('source', 'unknown')

            if new_price is None or np.isnan(new_price):
                continue

            df.loc[mask, 'UL Last']      = new_price
            df.loc[mask, 'Price_Source'] = source
            df.loc[mask, 'Price_TS']    = now.isoformat()

            if old_price is not None and old_price > 0:
                delta_pct = (new_price - old_price) / old_price
            else:
                delta_pct = 0.0

            log_rows.append({
                'log_id':      str(uuid.uuid4()),
                'fetch_ts':    now,
                'ticker':      ticker,
                'old_price':   float(old_price) if old_price is not None else None,
                'new_price':   float(new_price),
                'delta_pct':   float(delta_pct),
                'source':      source,
                'market_open': True,
            })

            if abs(delta_pct) >= 0.005:
                logger.info(
                    f"[LivePriceProvider] {ticker}: {old_price:.2f} → {new_price:.2f} "
                    f"({delta_pct:+.1%}) [{source}]"
                )

        if log_rows:
            self._log_refresh(log_rows)

        return df

    # ── Price Fetching ────────────────────────────────────────────────────────

    def _fetch_from_schwab(
        self, tickers: List[str], schwab_client
    ) -> Dict[str, Dict]:
        """Batch Schwab quotes call.  Returns {ticker: {ul_last, source}}."""
        prices: Dict[str, Dict] = {}
        try:
            # Schwab limit: 100 symbols per call — management never has >50 tickers
            response = schwab_client.get_quotes(symbols=tickers, fields="quote")
            if not response:
                return prices

            for ticker in tickers:
                quote_data = response.get(ticker, {})
                # Schwab returns nested: { 'AAPL': { 'quote': { 'lastPrice': ... } } }
                quote_block = quote_data.get('quote', quote_data)
                last_price = (
                    quote_block.get('lastPrice')
                    or quote_block.get('last')
                    or quote_block.get('bidPrice')
                )
                if last_price is not None:
                    try:
                        prices[ticker] = {
                            'ul_last': float(last_price),
                            'source':  'schwab_live',
                        }
                    except (TypeError, ValueError):
                        pass

            logger.info(f"[LivePriceProvider] Schwab live: {len(prices)}/{len(tickers)} tickers.")
        except Exception as e:
            logger.warning(f"[LivePriceProvider] Schwab quote fetch failed: {e}")
        return prices

    def _fetch_from_scan_cache(self, tickers: List[str]) -> Dict[str, Dict]:
        """
        Read scan_results_latest from scan domain DB (freshness ≤2h).
        Returns {ticker: {ul_last, source}}.
        """
        prices: Dict[str, Dict] = {}
        try:
            from core.shared.data_layer.duckdb_utils import get_domain_connection, DbDomain
            cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=_SCAN_MAX_AGE_HOURS)
            con = get_domain_connection(DbDomain.SCAN, read_only=True)
            try:
                rows = con.execute("""
                    SELECT Ticker, "UL Last"
                    FROM scan_results_latest
                    WHERE Ticker = ANY(?)
                      AND scan_timestamp >= ?
                """, [tickers, cutoff]).fetchall()

                for ticker, ul_last in rows:
                    if ul_last is not None:
                        try:
                            prices[ticker] = {
                                'ul_last': float(ul_last),
                                'source':  'scan_cache',
                            }
                        except (TypeError, ValueError):
                            pass
            finally:
                con.close()

            if prices:
                logger.info(
                    f"[LivePriceProvider] scan_results_latest: {len(prices)} tickers found."
                )
        except Exception as e:
            logger.debug(f"[LivePriceProvider] scan_results_latest query failed: {e}")
        return prices

    # ── Cache Freshness ──────────────────────────────────────────────────────

    def _scan_cache_is_fresh(self) -> bool:
        """Check if scan_results_latest has data within the last 2 hours."""
        try:
            from core.shared.data_layer.duckdb_utils import get_domain_connection, DbDomain
            cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=_SCAN_MAX_AGE_HOURS)
            con = get_domain_connection(DbDomain.SCAN, read_only=True)
            try:
                row = con.execute(
                    "SELECT COUNT(*) FROM scan_results_latest WHERE scan_timestamp >= ?",
                    [cutoff],
                ).fetchone()
                return row is not None and row[0] > 0
            finally:
                con.close()
        except Exception:
            return False

    # ── Audit Log ─────────────────────────────────────────────────────────────

    def _ensure_log_table(self) -> None:
        try:
            from core.shared.data_layer.duckdb_utils import get_domain_write_connection, DbDomain
            con = get_domain_write_connection(DbDomain.MANAGEMENT)
            try:
                con.execute(_CREATE_LOG_TABLE_SQL)
            finally:
                con.close()
        except Exception as e:
            logger.debug(f"[LivePriceProvider] Log table init: {e}")

    def _log_refresh(self, rows: List[Dict]) -> None:
        if not rows:
            return
        try:
            from core.shared.data_layer.duckdb_utils import get_domain_write_connection, DbDomain
            con = get_domain_write_connection(DbDomain.MANAGEMENT)
            try:
                for r in rows:
                    con.execute("""
                        INSERT INTO price_refresh_log
                            (log_id, fetch_ts, ticker, old_price, new_price, delta_pct, source, market_open)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (log_id) DO NOTHING
                    """, [r['log_id'], r['fetch_ts'], r['ticker'],
                          r['old_price'], r['new_price'], r['delta_pct'],
                          r['source'], r['market_open']])
            finally:
                con.close()
        except Exception as e:
            logger.debug(f"[LivePriceProvider] Log write failed: {e}")
