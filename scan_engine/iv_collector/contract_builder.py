"""
contract_builder.py — ATM strike detection + Schwab streamer symbol construction

PURPOSE
-------
Given a stock price and a Schwab /chains response (or an expiration list),
find the ATM strike for each maturity bucket and construct the canonical
Schwab streamer symbol string.

This module is intentionally stateless and pure — no I/O, no API calls.
It exists to decouple symbol construction from the surface extraction so the
streamer layer (Phase C) can subscribe to the right contracts without re-fetching
the chain.

STREAMER SYMBOL FORMAT (confirmed from Schwab Streamer API docs + real data)
------------------------------------------------------------------------------
    RRRRRRYYMMDDsWWWWWddd

    RRRRRR — root symbol, LEFT-JUSTIFIED, padded to 6 chars with spaces
    YYMMDD — expiration date
    s      — 'C' or 'P'
    WWWWW  — whole-dollar strike, 5 digits, zero-padded
    ddd    — fractional strike × 1000, 3 digits, zero-padded

    Example: AAPL expiring 2026-02-25, call, strike $265.00
        → "AAPL  260225C00265000"

    For fractional strikes (e.g. $265.50):
        WWWWW = 265, ddd = 500
        → "AAPL  260225C00265500"

    For high-value stocks (e.g. AMZN $3,455.00):
        WWWWW = 3455, ddd = 000
        → "AMZN  260225C03455000"

PUBLIC API
----------
    build_streamer_symbol(ticker, expiry_date, right, strike) -> str

    find_atm_for_buckets(call_map, spot, buckets) -> dict[int, AthBucketInfo]

DATACLASS
---------
    AtmBucketInfo:
        bucket_days   int     — target maturity (e.g. 30)
        actual_dte    int     — DTE of selected expiration
        expiry_date   str     — "YYYY-MM-DD"
        atm_strike    float   — nearest-to-spot strike value
        streamer_call str     — Schwab streamer symbol for the call
        streamer_put  str     — Schwab streamer symbol for the put
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dataclass
# ---------------------------------------------------------------------------

@dataclass
class AtmBucketInfo:
    """ATM contract info for one maturity bucket."""
    bucket_days:   int
    actual_dte:    int
    expiry_date:   str    # "YYYY-MM-DD"
    atm_strike:    float
    streamer_call: str    # Schwab streamer symbol (call)
    streamer_put:  str    # Schwab streamer symbol (put)


# ---------------------------------------------------------------------------
# Streamer symbol construction
# ---------------------------------------------------------------------------

def build_streamer_symbol(
    ticker: str,
    expiry_date: str,
    right: str,
    strike: float,
) -> str:
    """
    Build the canonical Schwab streamer option symbol.

    Parameters
    ----------
    ticker : str
        Root equity symbol (e.g. "AAPL", "AMZN").
    expiry_date : str
        Expiration date in "YYYY-MM-DD" format.
    right : str
        "C" for call or "P" for put.
    strike : float
        Strike price (e.g. 265.0, 265.5, 3455.0).

    Returns
    -------
    str
        Schwab streamer symbol, e.g. "AAPL  260225C00265000"

    Raises
    ------
    ValueError
        If expiry_date is malformed, right is not 'C'/'P', or strike <= 0.
    """
    if right not in ("C", "P"):
        raise ValueError(f"right must be 'C' or 'P', got {right!r}")
    if strike <= 0:
        raise ValueError(f"strike must be positive, got {strike}")

    # Parse expiry date → YYMMDD
    try:
        dt = datetime.strptime(expiry_date, "%Y-%m-%d")
        yymmdd = dt.strftime("%y%m%d")
    except ValueError as exc:
        raise ValueError(f"Invalid expiry_date {expiry_date!r}: {exc}") from exc

    # Pad root to 6 characters (left-justified, space-padded)
    root = ticker[:6].ljust(6)

    # Strike encoding: whole dollars (5 digits) + fractional × 1000 (3 digits)
    whole = int(strike)
    frac  = round((strike - whole) * 1000)

    # Guard against floating-point artefacts (e.g. 0.5 → 500.0000000001)
    frac = min(frac, 999)

    strike_str = f"{whole:05d}{frac:03d}"

    return f"{root}{yymmdd}{right}{strike_str}"


# ---------------------------------------------------------------------------
# ATM detection across maturity buckets
# ---------------------------------------------------------------------------

def _parse_dte(date_key: str, today: datetime) -> Optional[int]:
    """Parse DTE from Schwab expDateMap key "YYYY-MM-DD:DTE"."""
    date_part = date_key.split(":")[0]
    try:
        expiry = datetime.strptime(date_part, "%Y-%m-%d")
        return (expiry - today).days
    except ValueError:
        return None


def _bucket_tolerance(target_days: int) -> int:
    """Acceptance window for a maturity bucket (same rule as chain_surface.py)."""
    return max(int(target_days * 0.25), 7)


def _nearest_atm_strike(strikes_map: dict, spot: float) -> Optional[float]:
    """
    Return the float value of the strike nearest to spot.

    Considers all strike keys in the dict regardless of liquidity.
    (Liquidity filtering happens at IV extraction time, not symbol construction.)
    """
    candidates = []
    for key in strikes_map:
        try:
            candidates.append(float(key))
        except ValueError:
            continue
    if not candidates:
        return None
    return min(candidates, key=lambda s: abs(s - spot))


def find_atm_for_buckets(
    call_map: dict,
    spot: float,
    buckets: list[int],
    today: Optional[datetime] = None,
) -> dict[int, AtmBucketInfo]:
    """
    For each maturity bucket, find the nearest-DTE expiration and ATM strike.

    Parameters
    ----------
    call_map : dict
        Schwab callExpDateMap: {"YYYY-MM-DD:DTE": {strike: [contracts...]}, ...}
    spot : float
        Current equity price.
    buckets : list[int]
        Target maturity buckets in calendar days (e.g. [7, 14, 30, 60, 90, 120, 180, 360]).
    today : datetime, optional
        Reference date (defaults to datetime.now()).

    Returns
    -------
    dict[int, AtmBucketInfo]
        Mapping of bucket_days → AtmBucketInfo.
        Buckets that cannot be matched are absent from the dict.
    """
    if today is None:
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    # Build sorted (DTE, date_key, expiry_date_str) list
    exp_list: list[tuple[int, str, str]] = []
    for date_key in call_map:
        dte = _parse_dte(date_key, today)
        if dte is None or dte < 3:
            continue
        expiry_str = date_key.split(":")[0]
        exp_list.append((dte, date_key, expiry_str))
    exp_list.sort(key=lambda t: t[0])

    result: dict[int, AtmBucketInfo] = {}

    for bucket in buckets:
        tol = _bucket_tolerance(bucket)

        # Find nearest expiration within tolerance
        candidates = [
            (abs(dte - bucket), dte, dk, es)
            for dte, dk, es in exp_list
        ]
        candidates.sort(key=lambda t: t[0])

        for delta, dte, date_key, expiry_str in candidates:
            if delta > tol:
                break  # No closer match exists

            strikes_map = call_map.get(date_key, {})
            atm = _nearest_atm_strike(strikes_map, spot)
            if atm is None:
                logger.debug(
                    "bucket=%dd DTE=%d: no strikes in call_map key %s",
                    bucket, dte, date_key,
                )
                continue

            try:
                sym_call = build_streamer_symbol(ticker="PLACEHOLDER", expiry_date=expiry_str,
                                                 right="C", strike=atm)
                sym_put  = build_streamer_symbol(ticker="PLACEHOLDER", expiry_date=expiry_str,
                                                 right="P", strike=atm)
                # Note: caller must replace "PLACEHOLDER" with actual ticker
            except ValueError as exc:
                logger.warning("Could not build streamer symbol for bucket=%dd: %s", bucket, exc)
                continue

            result[bucket] = AtmBucketInfo(
                bucket_days=bucket,
                actual_dte=dte,
                expiry_date=expiry_str,
                atm_strike=atm,
                streamer_call=sym_call,
                streamer_put=sym_put,
            )
            break

    return result


def find_atm_for_buckets_with_ticker(
    ticker: str,
    call_map: dict,
    spot: float,
    buckets: list[int],
    today: Optional[datetime] = None,
) -> dict[int, AtmBucketInfo]:
    """
    Convenience wrapper that fills in the ticker correctly in streamer symbols.

    Parameters
    ----------
    ticker : str
        Equity symbol (e.g. "AAPL").
    call_map : dict
        Schwab callExpDateMap.
    spot : float
        Current stock price.
    buckets : list[int]
        Maturity bucket targets.
    today : datetime, optional
        Reference date.

    Returns
    -------
    dict[int, AtmBucketInfo]
        Fully populated, ticker-correct AtmBucketInfo per bucket.
    """
    raw = find_atm_for_buckets(call_map, spot, buckets, today)
    # Re-build symbols with the real ticker
    result: dict[int, AtmBucketInfo] = {}
    for bucket, info in raw.items():
        sym_call = build_streamer_symbol(ticker, info.expiry_date, "C", info.atm_strike)
        sym_put  = build_streamer_symbol(ticker, info.expiry_date, "P", info.atm_strike)
        result[bucket] = AtmBucketInfo(
            bucket_days=info.bucket_days,
            actual_dte=info.actual_dte,
            expiry_date=info.expiry_date,
            atm_strike=info.atm_strike,
            streamer_call=sym_call,
            streamer_put=sym_put,
        )
    return result
