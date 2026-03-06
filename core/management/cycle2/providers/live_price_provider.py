"""
Live Price Provider (Smart Schwab Refresh)
==========================================
Fetches live position prices during market hours only, and only when the price
has moved enough to change doctrine outcomes.

Design principles:
  1. Market hours gate  — Schwab is only called 9:30–16:00 ET (weekdays).
  2. Context gate       — Only fetch if price moved >0.5% vs broker CSV, OR Greeks
                          are stale (last Schwab fetch was >4h ago).
  3. Single batch call  — All underlying tickers in one get_quotes() call (≤100 symbols).
  4. Three-tier fallback:
       a. Live Schwab quote           (market hours + context added)
       b. scan_results_latest DuckDB  (Schwab failed, or off-hours but scan fresh ≤2h)
       c. Broker CSV UL Last          (always available — original baseline)
  5. No chain calls     — quotes only; options IV handled by governed_iv_provider.
  6. Audit log          — price_refresh_log table for every run.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_PIPELINE_DB_PATH = "data/pipeline.duckdb"

# Thresholds
_MOVE_THRESHOLD_PCT   = 0.005   # 0.5% — minimum price move to justify Schwab call
_GREEKS_MAX_AGE_HOURS = 4       # refresh if last Schwab IV fetch was >4h ago
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

    def __init__(self, db_path: str = _PIPELINE_DB_PATH):
        self.db_path = db_path
        self._ensure_log_table()

    # ── Public API ────────────────────────────────────────────────────────────

    def should_refresh(self, df: pd.DataFrame, last_fetch_ts: Optional[datetime] = None) -> bool:
        """
        Returns True if a live price call adds context:
          - Market is currently open (9:30–16:00 ET, weekdays)
          - AND at least one of:
              a. Any position's UL Last has moved >0.5% vs last_known_price in pipeline.duckdb
              b. Greeks are stale (last_fetch_ts > 4h ago)
        Off-hours: returns False → broker CSV is used, no API call.
        """
        from core.shared.data_layer.market_time import is_market_open
        if not is_market_open():
            logger.debug("[LivePriceProvider] Market closed — skipping live price refresh.")
            return False

        # Greek staleness gate
        if last_fetch_ts is None:
            last_fetch_ts = self._get_last_schwab_fetch_ts()

        if last_fetch_ts is not None:
            age_h = (datetime.now(tz=timezone.utc) - last_fetch_ts).total_seconds() / 3600
            if age_h > _GREEKS_MAX_AGE_HOURS:
                logger.info(f"[LivePriceProvider] Greeks stale ({age_h:.1f}h) — refresh triggered.")
                return True

        # Price move gate
        if self._any_significant_price_move(df):
            return True

        logger.debug("[LivePriceProvider] No significant move and Greeks fresh — skipping refresh.")
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

            if abs(delta_pct) >= _MOVE_THRESHOLD_PCT:
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
        Read scan_results_latest from pipeline.duckdb (freshness ≤2h).
        Returns {ticker: {ul_last, source}}.
        """
        prices: Dict[str, Dict] = {}
        try:
            import duckdb
            cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=_SCAN_MAX_AGE_HOURS)
            with duckdb.connect(self.db_path, read_only=True) as con:
                # scan_results_latest has 'Ticker' and 'UL Last' (or similar)
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

            if prices:
                logger.info(
                    f"[LivePriceProvider] scan_results_latest: {len(prices)} tickers found."
                )
        except Exception as e:
            logger.debug(f"[LivePriceProvider] scan_results_latest query failed: {e}")
        return prices

    # ── Move Detection ────────────────────────────────────────────────────────

    def _any_significant_price_move(self, df: pd.DataFrame) -> bool:
        """
        Compare current df 'UL Last' against last persisted price in management_recommendations.
        Returns True if any ticker moved > MOVE_THRESHOLD_PCT.
        """
        ticker_col = 'Underlying_Ticker' if 'Underlying_Ticker' in df.columns else 'Ticker'
        if ticker_col not in df.columns or 'UL Last' not in df.columns:
            return False

        try:
            import duckdb
            tickers = df[ticker_col].dropna().unique().tolist()
            if not tickers:
                return False

            with duckdb.connect(self.db_path, read_only=True) as con:
                rows = con.execute("""
                    SELECT Underlying_Ticker, "UL Last"
                    FROM v_latest_recommendations
                    WHERE Underlying_Ticker = ANY(?)
                """, [tickers]).fetchall()

            last_known = {r[0]: r[1] for r in rows if r[1] is not None}

            for ticker, group in df.groupby(ticker_col):
                current = group['UL Last'].dropna().iloc[0] if not group['UL Last'].dropna().empty else None
                previous = last_known.get(ticker)
                if current and previous and previous > 0:
                    move = abs(current - previous) / previous
                    if move > _MOVE_THRESHOLD_PCT:
                        logger.info(
                            f"[LivePriceProvider] {ticker} moved {move:+.1%} since last run — refresh needed."
                        )
                        return True
        except Exception as e:
            logger.debug(f"[LivePriceProvider] Move detection failed: {e}")
            # When in doubt (no history), refresh during market hours
            return True

        return False

    def _get_last_schwab_fetch_ts(self) -> Optional[datetime]:
        """Read last successful Schwab IV fetch from iv_metadata."""
        try:
            import duckdb
            with duckdb.connect(self.db_path, read_only=True) as con:
                row = con.execute("""
                    SELECT MAX(computed_ts)
                    FROM iv_metadata
                    WHERE last_status = 'success'
                """).fetchone()
            if row and row[0]:
                ts = row[0]
                if not ts.tzinfo:
                    ts = ts.replace(tzinfo=timezone.utc)
                return ts
        except Exception:
            pass
        return None

    # ── Audit Log ─────────────────────────────────────────────────────────────

    def _ensure_log_table(self) -> None:
        try:
            import duckdb
            with duckdb.connect(self.db_path) as con:
                con.execute(_CREATE_LOG_TABLE_SQL)
        except Exception as e:
            logger.debug(f"[LivePriceProvider] Log table init: {e}")

    def _log_refresh(self, rows: List[Dict]) -> None:
        if not rows:
            return
        try:
            import duckdb
            with duckdb.connect(self.db_path) as con:
                for r in rows:
                    con.execute("""
                        INSERT INTO price_refresh_log
                            (log_id, fetch_ts, ticker, old_price, new_price, delta_pct, source, market_open)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (log_id) DO NOTHING
                    """, [r['log_id'], r['fetch_ts'], r['ticker'],
                          r['old_price'], r['new_price'], r['delta_pct'],
                          r['source'], r['market_open']])
        except Exception as e:
            logger.debug(f"[LivePriceProvider] Log write failed: {e}")
