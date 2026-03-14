"""
Resolver Implementations - Actual Data Fetching Logic

This module contains the actual implementations of resolvers that fetch data
from various sources (Schwab API, Fidelity scraper, DuckDB cache, etc.).

DESIGN PRINCIPLES:
1. STRATEGY-AGNOSTIC - No resolver looks at Strategy_Name or strategy type
2. DATA-ONLY TRIGGER - Resolvers are triggered by missing data, not trading intent
3. FAIL-SAFE - All resolvers handle errors gracefully and return partial results
4. RATE-AWARE - Implementations respect rate limits defined in registry
5. OBSERVABLE - Full logging for debugging and auditing

NOTE: These implementations are wired to the executor via:
    executor.register_resolver_impl(ResolverType.XXX, impl_function)
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from pathlib import Path

from .resolver_registry import ResolverConfig, ResolverType

logger = logging.getLogger(__name__)


# =============================================================================
# IV_HISTORY RESOLVERS
# =============================================================================

def resolve_iv_history_from_db(
    tickers: List[str],
    config: ResolverConfig
) -> Dict[str, Any]:
    """
    Resolve IV history from iv_history.duckdb cache.

    This is the fastest resolver - reads from local cache.
    Returns iv_history_count and IV_Maturity_State.
    """
    from core.shared.data_layer.duckdb_utils import get_domain_connection, DbDomain

    results = {}

    con = None
    try:
        con = get_domain_connection(DbDomain.IV_HISTORY, read_only=True)

        # Batch query for all tickers
        placeholders = ', '.join([f"'{t.upper()}'" for t in tickers])
        query = f"""
            SELECT UPPER(ticker) as ticker, COUNT(DISTINCT date) as history_days
            FROM iv_term_history
            WHERE UPPER(ticker) IN ({placeholders})
              AND iv_30d IS NOT NULL
            GROUP BY UPPER(ticker)
        """
        df = con.execute(query).fetchdf()

        # Build results
        history_lookup = {row['ticker']: row['history_days'] for _, row in df.iterrows()}

        # Use consolidated maturity classifier for consistency
        from core.shared.volatility.maturity_classifier import classify_iv_maturity

        for ticker in tickers:
            ticker_upper = ticker.upper()
            history_days = history_lookup.get(ticker_upper, 0)
            state = classify_iv_maturity(history_days)

            results[ticker] = {
                'iv_history_count': history_days,
                'IV_Maturity_State': state
            }

        logger.info(f"Resolved IV history for {len(results)} tickers from cache")
        return results

    except Exception as e:
        logger.error(f"Failed to resolve IV history from DB: {e}")
        return results
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


def resolve_iv_history_from_fidelity(
    tickers: List[str],
    config: ResolverConfig
) -> Dict[str, Any]:
    """
    Resolve IV history by triggering Fidelity scraper.

    This resolver:
    1. Exports tickers to a demand file
    2. Optionally triggers the scraper (if auto_scrape=True)
    3. Returns whatever data is already in the cache

    NOTE: The actual scraping may happen asynchronously.
    """
    from core.shared.data_layer.duckdb_utils import get_domain_connection, DbDomain

    results = {}

    # Export demand file for manual/scheduled scraper execution
    demand_path = Path("output/enrichment_iv_demand.csv")
    demand_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Append to existing demand file or create new
        existing_tickers = set()
        if demand_path.exists():
            df_existing = pd.read_csv(demand_path)
            existing_tickers = set(df_existing['Ticker'].tolist())

        new_tickers = set(tickers) - existing_tickers
        if new_tickers:
            df_demand = pd.DataFrame({
                'Ticker': list(new_tickers),
                'Requested_At': datetime.now().isoformat(),
                'Requirement_Type': 'IV_HISTORY'
            })

            if demand_path.exists():
                df_demand.to_csv(demand_path, mode='a', header=False, index=False)
            else:
                df_demand.to_csv(demand_path, index=False)

            logger.info(f"Added {len(new_tickers)} tickers to IV demand file: {demand_path}")

    except Exception as e:
        logger.error(f"Failed to write IV demand file: {e}")

    # Check what's already in the Fidelity cache
    try:
        con = get_domain_connection(DbDomain.PIPELINE, read_only=True)

        # GUARD: Check if fidelity_iv_long_term_history table exists
        table_exists = con.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_name = 'fidelity_iv_long_term_history'
            AND table_schema = 'main'
        """).fetchone()[0] > 0

        if not table_exists:
            logger.warning(
                f"⚠️ DIAGNOSTIC: fidelity_iv_long_term_history table does not exist. "
                f"Cannot check Fidelity cache for {len(tickers)} tickers. "
                f"Run Fidelity scraper (scan_engine/iv2_v2.py) to populate IV history."
            )
            con.close()
            return results

        placeholders = ', '.join([f"'{t}'" for t in tickers])
        query = f"""
            SELECT Ticker, timestamp, IV_30_D_Call, Scraper_Status_Fidelity
            FROM fidelity_iv_long_term_history
            WHERE Ticker IN ({placeholders})
            QUALIFY ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY timestamp DESC) = 1
        """
        df = con.execute(query).fetchdf()
        con.close()

        if not df.empty:
            for _, row in df.iterrows():
                ticker = row['Ticker']
                age_days = (datetime.now() - row['timestamp']).total_seconds() / 86400

                if row['Scraper_Status_Fidelity'] == 'OK' and age_days <= 30:
                    results[ticker] = {
                        'IV_Rank_30D_Fidelity': row.get('IV_30_D_Call'),
                        'Fidelity_Snapshot_Age_Days': age_days,
                        'IV_Source_Fidelity': 'FIDELITY_CACHE'
                    }

        logger.info(f"Found {len(results)} tickers in Fidelity cache")

    except Exception as e:
        logger.warning(f"⚠️ DIAGNOSTIC: Could not check Fidelity cache: {type(e).__name__}: {e}")

    return results


# =============================================================================
# IV_RANK RESOLVERS
# =============================================================================

def resolve_iv_rank_from_cache(
    tickers: List[str],
    config: ResolverConfig
) -> Dict[str, Any]:
    """
    Resolve IV Rank from iv_term_history (IVEngine-computed).

    Returns IV_Rank_30D from the latest available iv_term_history rows
    for each ticker. Requires >= 30 days of history.
    """
    from core.shared.data_layer.duckdb_utils import get_domain_connection, DbDomain

    results = {}

    try:
        con = get_domain_connection(DbDomain.IV_HISTORY, read_only=True)

        placeholders = ", ".join([f"'{t.upper()}'" for t in tickers])
        df = con.execute(f"""
            SELECT UPPER(ticker) AS ticker, date, iv_30d
            FROM iv_term_history
            WHERE UPPER(ticker) IN ({placeholders})
              AND iv_30d IS NOT NULL
            ORDER BY ticker, date
        """).fetchdf()
        con.close()

        if not df.empty:
            import pandas as pd
            from scipy.stats import percentileofscore
            for ticker, group in df.groupby('ticker'):
                group = group.reset_index(drop=True)
                if len(group) >= 30:
                    window = group['iv_30d'].tail(30)
                    iv_rank = percentileofscore(window, window.iloc[-1])
                    results[ticker] = {
                        'IV_Rank_30D': iv_rank,
                        'IV_Rank_Source': 'iv_term_history (IVEngine)',
                    }

        logger.info(f"Resolved IV Rank for {len(results)} tickers from iv_term_history")

    except Exception as e:
        logger.error(f"Resolver Pipeline DB Cache failed: {type(e).__name__}: {e}")

    return results


def resolve_iv_rank_compute(
    tickers: List[str],
    config: ResolverConfig
) -> Dict[str, Any]:
    """
    DEPRECATED: Advisory-only IV Rank computation from iv_term_history.

    WARNING: This resolver produces ADVISORY data only and MUST NOT overwrite Step 2's
    canonical IV_Rank_30D field. Results are stored as 'IV_Rank_Computed' (separate field)
    for comparison and debugging purposes only.

    CANONICAL SOURCE: Step 2 owns IV_Rank_30D (from Fidelity long-term database).
    This resolver provides an independent calculation for validation/comparison only.

    STRICT REQUIREMENT: Requires >= 120 days of IV history for valid IV Rank.

    Per design doctrine:
    - DO NOT infer IV Rank from Schwab spot IV
    - DO NOT bootstrap percentiles from < 120 days of data
    - DO NOT substitute HV Rank, delta, or any proxy for IV Rank
    - Tickers with insufficient history get IV_Rank_Computed = None (explicitly UNAVAILABLE)

    Informational-only fields (clearly labeled) are provided for context,
    but NEVER used for execution eligibility.
    """
    from core.shared.data_layer.duckdb_utils import get_domain_connection, DbDomain

    results = {}

    con = None
    try:
        con = get_domain_connection(DbDomain.IV_HISTORY, read_only=True)

        for ticker in tickers:
            try:
                # Get IV history count and values
                query = """
                    SELECT iv_30d
                    FROM iv_term_history
                    WHERE UPPER(ticker) = UPPER(?)
                      AND iv_30d IS NOT NULL
                    ORDER BY date DESC
                    LIMIT 252
                """
                df = con.execute(query, [ticker]).fetchdf()
                history_count = len(df)

                if history_count >= 120:
                    # VALID: Sufficient IV history for percentile calculation
                    iv_values = df['iv_30d'].values
                    current_iv = iv_values[0]
                    rank = (np.sum(iv_values <= current_iv) / len(iv_values)) * 100

                    results[ticker] = {
                        'IV_Rank_Computed': round(rank, 1),  # Advisory-only, not canonical
                        'IV_Rank_Status': 'VALID',
                        'IV_Rank_Source': f'IV History ({history_count} days) [ADVISORY]',
                        'iv_history_count': history_count,
                        'IV_30D_Current': round(current_iv, 2),
                    }
                else:
                    # INSUFFICIENT: Not enough history for valid IV Rank
                    # Provide informational data ONLY - clearly labeled as NOT for execution
                    current_iv = df['iv_30d'].values[0] if history_count > 0 else None
                    days_needed = 120 - history_count

                    results[ticker] = {
                        'IV_Rank_Computed': None,  # Advisory-only, not canonical  # Explicitly None - no substitutes
                        'IV_Rank_Status': 'UNAVAILABLE',
                        'IV_Rank_Reason': f'Insufficient history ({history_count}/120 days)',
                        'iv_history_count': history_count,
                        'days_to_iv_rank_valid': days_needed,
                        # Informational only - clearly labeled
                        'IV_30D_Current_INFO_ONLY': round(current_iv, 2) if current_iv else None,
                        'INFO_LABEL': 'Current IV is informational only, not usable for IV Rank',
                    }
            except Exception as ticker_error:
                logger.debug(f"Error processing {ticker}: {ticker_error}")
                results[ticker] = {
                    'IV_Rank_Computed': None,  # Advisory-only, not canonical
                    'IV_Rank_Status': 'UNAVAILABLE',
                    'IV_Rank_Reason': f'Query error: {str(ticker_error)[:30]}',
                    'iv_history_count': 0,
                }

        valid_count = sum(1 for r in results.values() if r.get('IV_Rank_Status') == 'VALID')
        unavailable_count = len(results) - valid_count
        logger.info(f"IV Rank computed: {valid_count} valid, {unavailable_count} unavailable (need more history)")

    except Exception as e:
        logger.error(f"Failed to compute IV Rank from history: {e}")
        # Return explicit UNAVAILABLE for all tickers on error
        for ticker in tickers:
            if ticker not in results:
                results[ticker] = {
                    'IV_Rank_Computed': None,  # Advisory-only, not canonical
                    'IV_Rank_Status': 'UNAVAILABLE',
                    'IV_Rank_Reason': f'Database error: {str(e)[:50]}',
                    'iv_history_count': 0,
                }
    finally:
        # Always close connection to prevent resource leaks
        if con is not None:
            try:
                con.close()
            except Exception:
                pass  # Ignore close errors

    return results


# =============================================================================
# QUOTE_FRESHNESS RESOLVERS
# =============================================================================

def resolve_quotes_from_schwab(
    tickers: List[str],
    config: ResolverConfig
) -> Dict[str, Any]:
    """
    Resolve quote freshness from Schwab API.

    Fetches current bid/ask quotes for options.
    """
    results = {}

    try:
        from scan_engine.loaders.schwab_api_client import get_schwab_client

        client = get_schwab_client()
        if client is None:
            logger.warning("Schwab client not available")
            return results

        # Batch quote fetch
        for ticker in tickers:
            try:
                quote_data = client.get_quote(ticker)

                if quote_data and 'quote' in quote_data:
                    q = quote_data['quote']
                    results[ticker] = {
                        'Bid': q.get('bidPrice'),
                        'Ask': q.get('askPrice'),
                        'Last': q.get('lastPrice'),
                        'Quote_Source': 'Schwab API',
                        'Quote_Timestamp': datetime.now().isoformat()
                    }

            except Exception as e:
                logger.debug(f"Quote fetch failed for {ticker}: {e}")
                continue

        logger.info(f"Resolved quotes for {len(results)} tickers from Schwab")

    except ImportError:
        logger.warning("Schwab client not available for quote resolution")
    except Exception as e:
        logger.error(f"Failed to resolve quotes from Schwab: {e}")

    return results


# =============================================================================
# GREEKS RESOLVERS
# =============================================================================

def resolve_greeks_from_chain(
    tickers: List[str],
    config: ResolverConfig
) -> Dict[str, Any]:
    """
    Resolve Greeks from options chain data.

    Fetches delta, gamma, theta, vega from chain.
    """
    results = {}

    try:
        from scan_engine.loaders.schwab_api_client import get_schwab_client

        client = get_schwab_client()
        if client is None:
            return results

        for ticker in tickers:
            try:
                chain = client.get_option_chain(ticker)

                if chain and 'callExpDateMap' in chain:
                    # Get ATM option Greeks
                    # This is simplified - real impl would match strike/expiry
                    for exp_date, strikes in chain['callExpDateMap'].items():
                        for strike, options in strikes.items():
                            if options:
                                opt = options[0]
                                results[ticker] = {
                                    'Delta': opt.get('delta'),
                                    'Gamma': opt.get('gamma'),
                                    'Theta': opt.get('theta'),
                                    'Vega': opt.get('vega'),
                                    'Greeks_Source': 'Schwab Chain'
                                }
                                break
                        break

            except Exception as e:
                logger.debug(f"Greeks fetch failed for {ticker}: {e}")
                continue

        logger.info(f"Resolved Greeks for {len(results)} tickers")

    except ImportError:
        logger.warning("Schwab client not available for Greeks resolution")
    except Exception as e:
        logger.error(f"Failed to resolve Greeks: {e}")

    return results


# =============================================================================
# PRICE_HISTORY RESOLVERS
# =============================================================================

def resolve_price_history_from_cache(
    tickers: List[str],
    config: ResolverConfig
) -> Dict[str, Any]:
    """
    Resolve price history from local cache.

    Returns indicators computed from cached price data.
    """
    from core.shared.data_layer.price_history_loader import load_price_history
    from utils.ta_lib_utils import calculate_rsi, calculate_adx

    results = {}

    for ticker in tickers:
        try:
            df_price, source = load_price_history(ticker, days=90)

            if df_price is not None and len(df_price) >= 30:
                current_price = df_price['Close'].iloc[-1]
                sma20 = df_price['Close'].rolling(20).mean().iloc[-1]
                sma50 = df_price['Close'].rolling(50).mean().iloc[-1]

                # Trend state
                if pd.notna(sma20) and pd.notna(sma50):
                    if current_price > sma20 and current_price > sma50:
                        trend = 'Bullish'
                    elif current_price < sma20 and current_price < sma50:
                        trend = 'Bearish'
                    else:
                        trend = 'Neutral'
                else:
                    trend = 'Unknown'

                results[ticker] = {
                    'RSI': calculate_rsi(df_price['Close'], 14).iloc[-1],
                    'ADX': calculate_adx(df_price['High'], df_price['Low'], df_price['Close'], 14).iloc[-1],
                    'Trend_State': trend,
                    'Price_History_Days': len(df_price),
                    'Price_History_Source': source
                }

        except Exception as e:
            logger.debug(f"Price history failed for {ticker}: {e}")
            continue

    logger.info(f"Resolved price history for {len(results)} tickers from cache")
    return results


def resolve_price_history_from_yfinance(
    tickers: List[str],
    config: ResolverConfig
) -> Dict[str, Any]:
    """
    Resolve price history from Yahoo Finance.

    Fallback when cache is empty.
    """
    results = {}

    try:
        import yfinance as yf
        from utils.ta_lib_utils import calculate_rsi, calculate_adx

        for ticker in tickers:
            try:
                stock = yf.Ticker(ticker)
                df = stock.history(period="3mo")

                if len(df) >= 30:
                    current_price = df['Close'].iloc[-1]
                    sma20 = df['Close'].rolling(20).mean().iloc[-1]
                    sma50 = df['Close'].rolling(50).mean().iloc[-1]

                    if pd.notna(sma20) and pd.notna(sma50):
                        if current_price > sma20 and current_price > sma50:
                            trend = 'Bullish'
                        elif current_price < sma20 and current_price < sma50:
                            trend = 'Bearish'
                        else:
                            trend = 'Neutral'
                    else:
                        trend = 'Unknown'

                    results[ticker] = {
                        'RSI': calculate_rsi(df['Close'], 14).iloc[-1],
                        'ADX': calculate_adx(df['High'], df['Low'], df['Close'], 14).iloc[-1],
                        'Trend_State': trend,
                        'Price_History_Days': len(df),
                        'Price_History_Source': 'yfinance'
                    }

            except Exception as e:
                logger.debug(f"yfinance failed for {ticker}: {e}")
                continue

        logger.info(f"Resolved price history for {len(results)} tickers from yfinance")

    except ImportError:
        logger.warning("yfinance not available")
    except Exception as e:
        logger.error(f"Failed to resolve price history from yfinance: {e}")

    return results


# =============================================================================
# RESOLVER REGISTRATION
# =============================================================================

def register_all_resolvers(executor) -> None:
    """
    Register all resolver implementations with the enrichment executor.

    This is called during pipeline initialization to wire up the
    resolver implementations.
    """
    # IV_HISTORY resolvers
    executor.register_resolver_impl(
        ResolverType.IV_HISTORY_DB,
        resolve_iv_history_from_db
    )
    executor.register_resolver_impl(
        ResolverType.FIDELITY_SCRAPER,
        resolve_iv_history_from_fidelity
    )

    # IV_RANK resolvers
    executor.register_resolver_impl(
        ResolverType.DUCKDB_CACHE,
        resolve_iv_rank_from_cache
    )
    executor.register_resolver_impl(
        ResolverType.COMPUTE_IV_RANK,
        resolve_iv_rank_compute
    )

    # QUOTE_FRESHNESS resolvers
    executor.register_resolver_impl(
        ResolverType.SCHWAB_API,
        resolve_quotes_from_schwab
    )

    # GREEKS resolvers
    executor.register_resolver_impl(
        ResolverType.COMPUTE_FROM_CHAIN,
        resolve_greeks_from_chain
    )

    # PRICE_HISTORY resolvers
    executor.register_resolver_impl(
        ResolverType.YFINANCE,
        resolve_price_history_from_yfinance
    )

    logger.info("All resolver implementations registered")
