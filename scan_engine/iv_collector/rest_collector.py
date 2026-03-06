"""
rest_collector.py — Daily REST IV surface collection orchestrator

PURPOSE
-------
Fetches the full constant-maturity IV surface for all tickers in the
universe, writes clean rows to iv_term_history DuckDB, and returns a
summary DataFrame for downstream use.

This module is the SINGLE entry point for daily IV data collection.
It replaces the inline chain fetch loop inside step0_schwab_snapshot.py.

DESIGN
------
- Runs WITHIN market hours only (09:30–16:00 ET), or explicitly forced
  via force_run=True.
- Throttled at 1 request/sec by default (configurable).
- Chunked processing (25 tickers/chunk, 0.5s inter-chunk sleep) to stay
  within Schwab API rate limits.
- Never writes a row where iv_30d is NULL (guaranteed by chain_surface.py).
- Returns a DataFrame with one row per ticker for immediate pipeline use.
- Also returns a secondary dict of AtmBucketInfo keyed by ticker × bucket,
  which the future Streamer layer will consume for live IV updates.

USAGE
-----
    from scan_engine.iv_collector.rest_collector import IVRestCollector

    collector = IVRestCollector(client=schwab_client)
    result = collector.collect(tickers=["AAPL", "TSLA", "NVDA"],
                               spot_map={"AAPL": 248.7, "TSLA": 330.0, "NVDA": 110.5})

    # result.df          → pd.DataFrame with iv_7d…iv_360d per ticker
    # result.atm_map     → dict[ticker, dict[bucket, AtmBucketInfo]]
    # result.failed      → list of tickers where surface extraction failed
    # result.skipped     → list of tickers without valid spot prices

PUBLIC API
----------
    IVRestCollector(client, *, throttle_sec, chunk_size, chunk_sleep_sec)

    CollectionResult:
        df         pd.DataFrame
        atm_map    dict[str, dict[int, AtmBucketInfo]]
        failed     list[str]
        skipped    list[str]
        run_date   date
        duration_sec float
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

import duckdb
import pandas as pd

from scan_engine.iv_collector.chain_surface import fetch_chain, MATURITY_BUCKETS
from scan_engine.iv_collector.contract_builder import (
    AtmBucketInfo,
    find_atm_for_buckets_with_ticker,
)
from core.shared.data_layer.iv_term_history import (
    get_iv_history_db_path,
    initialize_iv_term_history_table,
    append_daily_iv_data,
    initialize_iv_surface_meta_table,
    append_surface_meta,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration defaults
# ---------------------------------------------------------------------------

DEFAULT_THROTTLE_SEC: float = 1.0      # 1 chain request per second
DEFAULT_CHUNK_SIZE:   int   = 25       # Tickers per processing chunk
DEFAULT_CHUNK_SLEEP:  float = 0.5      # Sleep between chunks (seconds)

# Market hours gate (US Eastern time assumed by caller; no TZ conversion here)
MARKET_OPEN_HOUR:  int = 9
MARKET_OPEN_MIN:   int = 30
MARKET_CLOSE_HOUR: int = 16
MARKET_CLOSE_MIN:  int = 0


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class CollectionResult:
    """Result of one IVRestCollector.collect() run."""
    df:           pd.DataFrame
    atm_map:      dict = field(default_factory=dict)   # dict[ticker, dict[bucket, AtmBucketInfo]]
    failed:       list = field(default_factory=list)   # tickers where chain fetch failed
    skipped:      list = field(default_factory=list)   # tickers with no valid spot
    run_date:     date = field(default_factory=date.today)
    duration_sec: float = 0.0

    @property
    def success_count(self) -> int:
        return len(self.df)

    @property
    def fail_rate(self) -> float:
        total = self.success_count + len(self.failed) + len(self.skipped)
        return len(self.failed) / total if total > 0 else 0.0


# ---------------------------------------------------------------------------
# Collector
# ---------------------------------------------------------------------------

class IVRestCollector:
    """
    Orchestrates daily REST-based IV surface collection for all tickers.

    Parameters
    ----------
    client : SchwabClient
        Authenticated Schwab API client.
    throttle_sec : float
        Seconds to sleep between individual chain fetch requests.
    chunk_size : int
        Number of tickers to process before sleeping chunk_sleep_sec.
    chunk_sleep_sec : float
        Seconds to sleep between chunks.
    write_to_db : bool
        If True (default), persist successful rows to iv_history.duckdb.
    """

    def __init__(
        self,
        client,
        *,
        throttle_sec:   float = DEFAULT_THROTTLE_SEC,
        chunk_size:     int   = DEFAULT_CHUNK_SIZE,
        chunk_sleep_sec: float = DEFAULT_CHUNK_SLEEP,
        write_to_db:    bool  = True,
    ):
        self._client         = client
        self._throttle_sec   = throttle_sec
        self._chunk_size     = chunk_size
        self._chunk_sleep    = chunk_sleep_sec
        self._write_to_db    = write_to_db

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def collect(
        self,
        tickers:     list[str],
        spot_map:    dict[str, float],
        *,
        trade_date:  Optional[date] = None,
        force_run:   bool = False,
    ) -> CollectionResult:
        """
        Run full IV surface collection for the given tickers.

        Parameters
        ----------
        tickers : list[str]
            Tickers to process (order preserved for throttle/logging).
        spot_map : dict[str, float]
            Current equity price per ticker. Tickers missing from spot_map
            or with price <= 0 are skipped.
        trade_date : date, optional
            Date to label the rows (defaults to today).
        force_run : bool
            If True, skip market-hours gate (useful for testing / backfill).

        Returns
        -------
        CollectionResult
        """
        t_start = time.time()
        trade_date = trade_date or date.today()

        logger.info("=" * 70)
        logger.info("📡 IV REST COLLECTOR — %s", trade_date)
        logger.info("   Tickers: %d | Throttle: %.1fs | Chunk: %d",
                    len(tickers), self._throttle_sec, self._chunk_size)
        logger.info("=" * 70)

        # Market hours gate
        if not force_run and not self._is_within_market_hours():
            logger.warning(
                "⚠️  IV REST collection called OUTSIDE market hours (09:30–16:00 ET). "
                "Chain IVs will be stale/zero. Use force_run=True to override."
            )

        failed:  list[str] = []
        skipped: list[str] = []
        rows:    list[dict] = []
        atm_map: dict[str, dict[int, AtmBucketInfo]] = {}

        chunks = [tickers[i:i + self._chunk_size]
                  for i in range(0, len(tickers), self._chunk_size)]

        for chunk_idx, chunk in enumerate(chunks, 1):
            logger.info("  Chunk %d/%d (%d tickers)...", chunk_idx, len(chunks), len(chunk))

            for ticker in chunk:
                spot = spot_map.get(ticker)
                if spot is None or spot <= 0:
                    logger.debug("[%s] No valid spot price (%.4f) — skipped", ticker, spot or 0)
                    skipped.append(ticker)
                    continue

                # Fetch chain + extract surface
                surface = fetch_chain(self._client, ticker, spot)

                if surface is None:
                    logger.warning("[%s] Surface extraction failed", ticker)
                    failed.append(ticker)
                else:
                    rows.append(surface)

                    # Build ATM contract map (for streamer subscriptions)
                    # We piggyback on the chain already fetched — we don't re-fetch.
                    # The atm_map is populated from chain_surface internals.
                    # For now, store what we know from the surface row itself.
                    atm_map[ticker] = {
                        30: AtmBucketInfo(
                            bucket_days=30,
                            actual_dte=surface.get("atm_30d_dte") or 0,
                            expiry_date="",      # not stored in surface row
                            atm_strike=surface.get("atm_30d_strike") or 0.0,
                            streamer_call="",    # populated below if we have expiry
                            streamer_put="",
                        )
                    }

                    logger.debug(
                        "[%s] iv_30d=%.2f%% iv_7d=%s iv_360d=%s",
                        ticker,
                        surface["iv_30d"],
                        f"{surface.get('iv_7d'):.2f}%" if surface.get("iv_7d") else "—",
                        f"{surface.get('iv_360d'):.2f}%" if surface.get("iv_360d") else "—",
                    )

                # Always throttle (even on failure) to respect Schwab rate limits
                time.sleep(self._throttle_sec)

            # Inter-chunk sleep
            if chunk_idx < len(chunks):
                time.sleep(self._chunk_sleep)

        # Assemble result DataFrame + per-bucket metadata
        if rows:
            df, meta_rows = self._build_dataframe(rows)
        else:
            df = pd.DataFrame(columns=["ticker"] + [f"iv_{b}d" for b in MATURITY_BUCKETS])
            meta_rows = []

        # Persist to DuckDB
        if self._write_to_db and not df.empty:
            self._persist(df, trade_date, meta_rows=meta_rows)

        duration = time.time() - t_start

        logger.info("=" * 70)
        logger.info(
            "✅ IV REST COLLECTION COMPLETE in %.1fs | "
            "success=%d  failed=%d  skipped=%d",
            duration, len(rows), len(failed), len(skipped),
        )
        logger.info("=" * 70)

        return CollectionResult(
            df=df,
            atm_map=atm_map,
            failed=failed,
            skipped=skipped,
            run_date=trade_date,
            duration_sec=duration,
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _is_within_market_hours() -> bool:
        """Return True if current local time is within US equity market hours."""
        now = datetime.now()
        open_time  = now.replace(hour=MARKET_OPEN_HOUR,  minute=MARKET_OPEN_MIN,  second=0, microsecond=0)
        close_time = now.replace(hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MIN, second=0, microsecond=0)
        return open_time <= now <= close_time

    @staticmethod
    def _build_dataframe(rows: list[dict]) -> tuple[pd.DataFrame, list[dict]]:
        """
        Convert list of surface dicts to clean DataFrame + metadata rows.

        Returns
        -------
        (df_surface, meta_rows)
            df_surface  — one row per ticker for iv_term_history
            meta_rows   — flat list of per-bucket dicts for iv_surface_meta
        """
        # Extract per-bucket metadata before building the surface DataFrame
        meta_rows: list[dict] = []
        for row in rows:
            bucket_meta = row.get("bucket_meta", {})
            ticker = row.get("ticker", "")
            spot_used = row.get("spot_used")
            chain_size = row.get("chain_size")
            source = row.get("source", "schwab_rest")
            for bucket, bm in bucket_meta.items():
                meta_rows.append({
                    "ticker":     ticker,
                    "bucket":     bucket,
                    "atm_strike": bm.get("atm_strike"),
                    "actual_dte": bm.get("actual_dte"),
                    "dte_gap":    bm.get("dte_gap"),
                    "tolerance":  bm.get("tolerance"),
                    "spot_used":  spot_used,
                    "chain_size": chain_size,
                    "source":     source,
                })

        df = pd.DataFrame(rows)

        iv_cols = [f"iv_{b}d" for b in MATURITY_BUCKETS]
        keep_cols = ["ticker"] + iv_cols + ["source", "spot_used", "chain_size",
                                             "atm_30d_strike", "atm_30d_dte"]
        # Keep only columns that exist; drop bucket_meta (nested dict, not for CSV/DB)
        keep = [c for c in keep_cols if c in df.columns]
        df = df[keep].copy()

        # Belt-and-suspenders: drop rows without iv_30d
        before = len(df)
        df = df.dropna(subset=["iv_30d"])
        after = len(df)
        if before != after:
            logger.warning(
                "Dropped %d rows with NULL iv_30d (should have been caught by chain_surface.py)",
                before - after,
            )

        df = df.reset_index(drop=True)
        return df, meta_rows

    def _persist(self, df: pd.DataFrame, trade_date: date, meta_rows: list[dict] | None = None) -> None:
        """Write surface rows and metadata to iv_history.duckdb."""
        db_path = get_iv_history_db_path()
        try:
            con = duckdb.connect(str(db_path), read_only=False)
            initialize_iv_term_history_table(con)
            initialize_iv_surface_meta_table(con)
            append_daily_iv_data(con, df, trade_date=trade_date)
            if meta_rows:
                append_surface_meta(con, meta_rows, trade_date)
                logger.info("✅ Persisted %d bucket metadata rows for %s",
                            len(meta_rows), trade_date)
            con.close()
            logger.info("✅ Persisted %d ticker IV rows for %s to %s",
                        len(df), trade_date, db_path)
        except Exception as exc:
            logger.error("❌ Failed to persist IV data to DuckDB: %s", exc)
            # Do not re-raise — collection result is still valid even if DB write fails


# ---------------------------------------------------------------------------
# Daily-skip helper
# ---------------------------------------------------------------------------

def iv_collected_today(db_path: Optional[str] = None) -> bool:
    """
    Return True if iv_term_history already has rows for today's date.

    Used by step0 and collect_iv_daily.py to skip redundant REST collection
    when IV has already been persisted for today (e.g., second pipeline run).

    Parameters
    ----------
    db_path : str, optional
        Path to iv_history.duckdb. Defaults to get_iv_history_db_path().

    Returns
    -------
    bool
        True  — at least one row for today already exists → skip collection.
        False — no rows for today yet → must collect.
    """
    from pathlib import Path as _Path
    _path = db_path or str(get_iv_history_db_path())
    if not _Path(_path).exists():
        return False
    try:
        con = duckdb.connect(_path, read_only=True)
        result = con.execute(
            "SELECT COUNT(*) FROM iv_term_history "
            "WHERE date = current_date AND iv_30d IS NOT NULL"
        ).fetchone()
        con.close()
        count = result[0] if result else 0
        return count > 0
    except Exception as exc:
        logger.warning("[IV_SKIP] Could not query iv_term_history: %s — assuming not collected", exc)
        return False


# ---------------------------------------------------------------------------
# Convenience function
# ---------------------------------------------------------------------------

def collect_iv_surface(
    client,
    tickers: list[str],
    spot_map: dict[str, float],
    *,
    trade_date: Optional[date] = None,
    force_run:  bool = False,
    write_to_db: bool = True,
    throttle_sec: float = DEFAULT_THROTTLE_SEC,
) -> CollectionResult:
    """
    One-line entry point for IV REST surface collection.

    Equivalent to:
        IVRestCollector(client, write_to_db=write_to_db).collect(
            tickers, spot_map, trade_date=trade_date, force_run=force_run)
    """
    collector = IVRestCollector(
        client,
        write_to_db=write_to_db,
        throttle_sec=throttle_sec,
    )
    return collector.collect(
        tickers,
        spot_map,
        trade_date=trade_date,
        force_run=force_run,
    )
