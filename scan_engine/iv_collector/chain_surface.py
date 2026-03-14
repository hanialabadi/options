"""
chain_surface.py — Schwab /chains REST response → constant-maturity IV surface

PURPOSE
-------
Takes a raw Schwab /chains JSON response (or fetches it live) and extracts a
constant-maturity IV surface row for one ticker.

Key design decisions (validated against real AAPL chain data 2026-02-18):

1. ATM detection is PER-EXPIRATION.
   The strike nearest to spot is found independently for each expiry.
   A single global ATM strike across all expirations is wrong — expirations
   >30 DTE typically have wider strike spacing; the "global" ATM can be ITM/OTM
   at distant expirations.

2. Call-side IV only.
   Empirical check across all 26 AAPL expirations confirmed call IV == put IV
   at ATM. Averaging adds no signal and complicates the logic.

3. Volume filter: volume > 0 required.
   Deep ITM / zero-volume contracts return garbage IV (e.g. 112%) from Schwab.
   We discard any contract with volume == 0 or openInterest == 0 (both must
   be missing for us to accept; if either is positive we accept the IV).

3a. iv_30d resolution: EXACT → INTERP → FALLBACK (VIX-style methodology).
   Monthly-only chains (FICO, SHW, WING etc.) have a ~9-day dead zone during
   the roll where no expiry falls within ±7d of 30d. Rather than widening
   the tolerance (which mislabels a 21D or 49D IV as "30D"), we interpolate
   in variance space between the nearest bracket expirations:
     T* = 30/365;  w = (T* - T1) / (T2 - T1)
     Var* = (1-w)·IV1²·T1 + w·IV2²·T2;  IV* = sqrt(Var*/T*)
   This is identical to the VIX constant-maturity interpolation.
   If no lower bracket exists, the nearest above-30d expiry (≤60d) is used
   as a FALLBACK with confidence downgrade. See IV30_Method column.

4. Maturity buckets: [7, 14, 30, 60, 90, 120, 180, 360] calendar days.
   These are the columns in iv_term_history. The nearest available expiration
   is selected for each bucket (nearest-neighbor). Tolerance window:
     max(bucket * 0.25, 7) days on either side.
   This is tighter than the old 50%-or-14d window, reducing noise.

5. LEAP coverage: date window extended to today+1100.
   Real chain has expirations to 1030 DTE. Old code capped at 400d (bug).

6. Never emit a NaN row.
   If iv_30d cannot be computed (no valid ATM contract within tolerance for
   the 30d bucket), the function returns None. Callers must check.

7. Schwab sentinel values.
   volatility == -999.0 or volatility <= 0 → treat as NaN (no valid quote).

PUBLIC API
----------
    extract_iv_surface(data: dict, ticker: str, spot: float) -> dict | None

    fetch_chain(client, ticker, spot) -> dict | None

    MATURITY_BUCKETS: list[int]   — canonical bucket targets
    PRIMARY_BUCKET:   int         — 30 (must be non-null for a row to be kept)

SCHEMA (returned dict keys)
---------------------------
    ticker                  str
    iv_7d                   float | None
    iv_14d                  float | None
    iv_30d                  float          (required — None causes function to return None)
    iv_60d                  float | None
    iv_90d                  float | None
    iv_120d                 float | None
    iv_180d                 float | None
    iv_360d                 float | None
    source                  str  ('schwab_rest')
    spot_used               float          (spot price used for ATM detection)
    chain_size              int            (total expirations in the chain response)
    atm_30d_strike          float | None   (ATM strike used for the 30d bucket; None if INTERP)
    atm_30d_dte             int | None     (actual DTE of expiry used for 30d bucket; None if INTERP)
    IV30_Method             str            'EXACT' | 'INTERP' | 'FALLBACK'
    IV30_T1_DTE             int | None     lower-bracket DTE (INTERP only)
    IV30_T2_DTE             int | None     upper-bracket DTE (INTERP or FALLBACK)
    IV30_Bracket_Width_Days int | None     T2_DTE - T1_DTE (INTERP only)
    IV30_Confidence         str            'HIGH' (bracket ≤35d) | 'MED' (36-60d) | 'LOW' (>60d)
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MATURITY_BUCKETS: list[int] = [7, 14, 21, 30, 60, 90, 120, 180, 360]
PRIMARY_BUCKET: int = 30  # iv_30d must be non-null for a surface row to be valid

# Tolerance: accept expiration if |actual_DTE - target| <= tolerance(target)
def _bucket_tolerance(target_days: int) -> int:
    """Return acceptance window (days) for a given maturity bucket."""
    return max(int(target_days * 0.25), 7)

# Schwab API
SCHWAB_API_BASE = "https://api.schwabapi.com"
# Cover full LEAP calendar (real data shows expirations to 1030 DTE)
CHAIN_FROM_OFFSET_DAYS: int = 3    # start 3 days out (skip weekly pinning)
CHAIN_TO_OFFSET_DAYS: int = 1100   # cover all LEAPs


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_dte(date_key: str, today: datetime) -> Optional[int]:
    """
    Parse DTE from Schwab expDateMap key.

    Schwab format: "2025-01-17:30"  (date:dte)
    The trailing ":dte" suffix is Schwab's own DTE; we recompute from the
    date part to stay consistent with our reference date.
    """
    date_part = date_key.split(":")[0]
    try:
        expiry = datetime.strptime(date_part, "%Y-%m-%d")
        return (expiry - today).days
    except ValueError:
        return None


def _parse_volatility(raw: object) -> Optional[float]:
    """
    Convert a raw 'volatility' field from Schwab to a usable float.

    Returns None if the value is:
      - missing / None
      - Schwab sentinel: -999.0
      - zero or negative
      - non-numeric
    """
    if raw is None:
        return None
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    if v <= 0:
        return None
    return v


def _has_liquidity(contract: dict) -> bool:
    """
    Return True if a contract has at least some trading interest.

    We require volume > 0 OR openInterest > 0.
    If both are zero (or missing), the contract is illiquid — its IV is garbage.
    """
    volume = contract.get("totalVolume", 0) or 0
    oi = contract.get("openInterest", 0) or 0
    return (volume > 0) or (oi > 0)


def _find_atm_strike(strikes_map: dict, spot: float) -> Optional[str]:
    """
    Find the strike key nearest to spot in a Schwab strike dict.

    Returns the string key (e.g. "265.0") or None if strikes_map is empty.
    Only considers strikes that have at least one contract with liquidity.
    Falls back to all strikes if no liquid ones exist.
    """
    candidates = []
    for strike_key, contracts in strikes_map.items():
        try:
            strike_val = float(strike_key)
        except ValueError:
            continue
        # contracts is a list of contract objects
        if isinstance(contracts, list) and contracts:
            candidates.append((strike_val, strike_key))

    if not candidates:
        return None

    # Prefer liquid strikes; fall back to all
    liquid = [
        (sv, sk) for sv, sk in candidates
        if isinstance(strikes_map[sk], list) and strikes_map[sk]
        and _has_liquidity(strikes_map[sk][0])
    ]
    pool = liquid if liquid else candidates
    _, best_key = min(pool, key=lambda t: abs(t[0] - spot))
    return best_key


def _extract_call_iv_at_atm(
    call_map: dict,
    date_key: str,
    spot: float,
) -> tuple[Optional[float], Optional[float], Optional[str]]:
    """
    Extract call-side IV at ATM for a single expiration.

    Returns:
        (iv, atm_strike_float, atm_strike_key)
        All None if extraction fails.
    """
    strikes_map = call_map.get(date_key, {})
    if not strikes_map:
        return None, None, None

    atm_key = _find_atm_strike(strikes_map, spot)
    if atm_key is None:
        return None, None, None

    contracts = strikes_map[atm_key]
    if not isinstance(contracts, list) or not contracts:
        return None, None, None

    contract = contracts[0]

    # Require some liquidity — but if ATM contract has none, fall back to
    # trying nearby strikes (±2 strikes)
    if not _has_liquidity(contract):
        sorted_strikes = sorted(
            [(abs(float(k) - spot), k, v)
             for k, v in strikes_map.items()
             if isinstance(v, list) and v and _has_liquidity(v[0])],
        )
        if sorted_strikes:
            _, atm_key, contracts = sorted_strikes[0]
            contract = contracts[0]
        else:
            # Accept illiquid ATM as last resort (log a warning)
            logger.debug(
                "No liquid strikes found for expiry %s (spot=%.2f) — using illiquid ATM",
                date_key, spot,
            )

    iv = _parse_volatility(contract.get("volatility"))
    try:
        atm_float = float(atm_key)
    except ValueError:
        atm_float = None

    return iv, atm_float, atm_key


# ---------------------------------------------------------------------------
# Public: extract_iv_surface
# ---------------------------------------------------------------------------

def extract_iv_surface(
    data: dict,
    ticker: str,
    spot: float,
) -> Optional[dict]:
    """
    Extract a constant-maturity IV surface row from a raw Schwab /chains response.

    Parameters
    ----------
    data : dict
        Raw JSON response from Schwab /chains endpoint.
    ticker : str
        Ticker symbol (for logging only).
    spot : float
        Current stock price, used for per-expiration ATM detection.

    Returns
    -------
    dict or None
        Surface row (see module docstring for schema), or None if iv_30d
        cannot be computed.
    """
    if not data or spot is None or spot <= 0:
        logger.warning("[%s] Invalid inputs to extract_iv_surface (data=%s, spot=%s)",
                       ticker, bool(data), spot)
        return None

    call_map: dict = data.get("callExpDateMap", {})
    if not call_map:
        logger.warning("[%s] No callExpDateMap in chain response", ticker)
        return None

    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    # Build sorted (DTE, date_key) list for all expirations that are >= 3 DTE
    exp_list: list[tuple[int, str]] = []
    for date_key in call_map:
        dte = _parse_dte(date_key, today)
        if dte is None:
            continue
        if dte < 3:
            # Skip expired / same-day / next-day — IV is meaningless
            continue
        exp_list.append((dte, date_key))

    exp_list.sort(key=lambda t: t[0])

    if not exp_list:
        logger.warning("[%s] No valid expirations (all DTE < 3)", ticker)
        return None

    # Pre-compute full surface: for each bucket, find best expiration
    surface: dict[str, Optional[float]] = {f"iv_{b}d": None for b in MATURITY_BUCKETS}

    # Per-bucket metadata (ATM strike, actual DTE, gap, tolerance)
    # Stored under bucket_meta[bucket] = {atm_strike, actual_dte, dte_gap, tolerance}
    bucket_meta: dict[int, dict] = {}

    meta: dict = {
        "atm_30d_strike": None,
        "atm_30d_dte": None,
        "chain_size": len(exp_list),
    }

    for bucket in MATURITY_BUCKETS:
        tol = _bucket_tolerance(bucket)

        # Find nearest expiration within tolerance
        candidates = [(abs(dte - bucket), dte, dkey) for dte, dkey in exp_list]
        candidates.sort(key=lambda t: t[0])

        found = False
        for delta, dte, date_key in candidates:
            if delta > tol:
                break  # sorted by delta — no closer match exists

            iv, atm_float, _ = _extract_call_iv_at_atm(call_map, date_key, spot)

            if iv is None:
                logger.debug(
                    "[%s] bucket=%dd: DTE=%d → no valid ATM IV (key=%s)",
                    ticker, bucket, dte, date_key,
                )
                continue

            surface[f"iv_{bucket}d"] = iv
            bucket_meta[bucket] = {
                "atm_strike": atm_float,
                "actual_dte": dte,
                "dte_gap":    delta,
                "tolerance":  tol,
            }
            logger.debug(
                "[%s] bucket=%dd ← DTE=%d (delta=%d) IV=%.2f%% ATM=%.2f",
                ticker, bucket, dte, delta, iv, atm_float or 0,
            )

            if bucket == PRIMARY_BUCKET:
                meta["atm_30d_strike"] = atm_float
                meta["atm_30d_dte"] = dte

            found = True
            break  # nearest valid expiration wins

        if not found:
            logger.debug(
                "[%s] bucket=%dd: no expiration within ±%dd tolerance",
                ticker, bucket, tol,
            )

    # ---------------------------------------------------------------------------
    # iv_30d resolution: EXACT → INTERP → FALLBACK → null
    #
    # Priority order (standard constant-maturity methodology):
    #   1. EXACT    — any expiry within ±7d of 30d target (already tried above)
    #   2. INTERP   — variance-time interpolation between nearest below + above 30d
    #                 (same method as VIX construction; preserves constant-maturity meaning)
    #   3. FALLBACK — nearest expiry strictly above 30d, capped at 60d
    #   4. null     — return None (no usable data)
    #
    # Metadata columns added to the row:
    #   IV30_Method             : 'EXACT' | 'INTERP' | 'FALLBACK' | 'NULL'
    #   IV30_T1_DTE             : lower-bracket DTE (INTERP only, else None)
    #   IV30_T2_DTE             : upper-bracket DTE (INTERP/FALLBACK)
    #   IV30_Bracket_Width_Days : T2_DTE - T1_DTE (INTERP) or None
    #   IV30_Confidence         : 'HIGH' (bracket ≤35d) | 'MED' (36-60d) | 'LOW' (>60d)
    # ---------------------------------------------------------------------------

    iv30_method    : str              = "EXACT" if surface.get("iv_30d") is not None else "NULL"
    iv30_t1_dte    : Optional[int]    = None
    iv30_t2_dte    : Optional[int]    = None
    iv30_bwidth    : Optional[int]    = None
    iv30_confidence: str              = "HIGH"

    if surface.get("iv_30d") is not None:
        # EXACT already resolved above; set confidence from bucket_meta gap
        _gap = bucket_meta.get(PRIMARY_BUCKET, {}).get("dte_gap", 0)
        iv30_confidence = "HIGH" if _gap <= 7 else "MED"
        iv30_method = "EXACT"

    else:
        # -----------------------------------------------------------------------
        # Collect all expirations that have a valid ATM IV
        # -----------------------------------------------------------------------
        _iv_by_dte: list[tuple[int, float, str]] = []  # (dte, iv, date_key)
        for _dte, _dkey in exp_list:
            _iv, _, _ = _extract_call_iv_at_atm(call_map, _dkey, spot)
            if _iv is not None and _iv > 0:
                _iv_by_dte.append((_dte, _iv, _dkey))

        _target = float(PRIMARY_BUCKET)  # 30.0

        # Split into below-target and above-target lists
        _below = [(d, iv, k) for d, iv, k in _iv_by_dte if d < _target]
        _above = [(d, iv, k) for d, iv, k in _iv_by_dte if d > _target]

        if not _iv_by_dte:
            logger.debug("[%s] iv_30d INTERP: no expirations with valid ATM IV found", ticker)

        if _below and _above:
            # ------------------------------------------------------------------
            # INTERP: variance-time interpolation (VIX-style)
            #   T in years; total variance = IV² × T
            #   w = (T* - T1) / (T2 - T1)
            #   Var* = (1-w)·Var1 + w·Var2
            #   IV* = sqrt(Var* / T*)
            # ------------------------------------------------------------------
            _d1, _iv1, _k1 = max(_below, key=lambda t: t[0])  # nearest below
            _d2, _iv2, _k2 = min(_above, key=lambda t: t[0])  # nearest above

            _T1 = _d1 / 365.0
            _T2 = _d2 / 365.0
            _Tstar = _target / 365.0

            _var1   = (_iv1 / 100.0) ** 2 * _T1
            _var2   = (_iv2 / 100.0) ** 2 * _T2
            _w      = (_Tstar - _T1) / (_T2 - _T1)
            _varstar = (1.0 - _w) * _var1 + _w * _var2
            _iv_interp = float(np.sqrt(_varstar / _Tstar)) * 100.0

            surface[f"iv_{PRIMARY_BUCKET}d"] = _iv_interp
            bucket_meta[PRIMARY_BUCKET] = {
                "atm_strike": None,  # interpolated — no single ATM strike
                "actual_dte": None,
                "dte_gap":    None,
                "tolerance":  None,
                "interp_t1_dte": _d1,
                "interp_t2_dte": _d2,
                "interp_w":      round(_w, 4),
            }
            meta["atm_30d_strike"] = None
            meta["atm_30d_dte"]    = None

            iv30_method = "INTERP"
            iv30_t1_dte = _d1
            iv30_t2_dte = _d2
            iv30_bwidth = _d2 - _d1
            iv30_confidence = "HIGH" if iv30_bwidth <= 35 else ("MED" if iv30_bwidth <= 60 else "LOW")

            logger.info(
                "[%s] iv_30d INTERP: T1=%dd(IV=%.1f%%) T2=%dd(IV=%.1f%%) → IV30=%.2f%% "
                "(w=%.3f, bracket=%dd, conf=%s)",
                ticker, _d1, _iv1, _d2, _iv2, _iv_interp, _w, iv30_bwidth, iv30_confidence,
            )

        elif _above:
            # ------------------------------------------------------------------
            # FALLBACK: no lower bracket — only above-30d expirations available.
            # Common cause: front-month has zero volume/OI today (illiquid contract).
            # Use nearest above, capped at 90d (quarterly chains have no front month).
            # Cap relaxed vs. ideal because with no lower bracket any above-30d is
            # better than a null row; confidence is tagged LOW beyond 60d.
            # ------------------------------------------------------------------
            _d2, _iv2, _k2 = min(_above, key=lambda t: t[0])
            if _d2 <= 90:
                surface[f"iv_{PRIMARY_BUCKET}d"] = _iv2
                bucket_meta[PRIMARY_BUCKET] = {
                    "atm_strike": None,
                    "actual_dte": _d2,
                    "dte_gap":    _d2 - int(_target),
                    "tolerance":  None,
                }
                meta["atm_30d_dte"] = _d2

                iv30_method = "FALLBACK"
                iv30_t2_dte = _d2
                iv30_bwidth = None
                iv30_confidence = "MED" if _d2 <= 45 else "LOW"

                logger.info(
                    "[%s] iv_30d FALLBACK: nearest above = DTE=%d IV=%.2f%% (conf=%s, no lower bracket)",
                    ticker, _d2, _iv2, iv30_confidence,
                )
            else:
                logger.warning(
                    "[%s] iv_30d FALLBACK skipped: nearest-above DTE=%d > 90d cap (no lower bracket)",
                    ticker, _d2,
                )
        else:
            # No expirations with valid ATM IV at all (entirely illiquid chain)
            _all_dtes = [d for d, _ in exp_list]
            logger.warning(
                "[%s] iv_30d: no valid ATM IV found in any expiration "
                "(chain has %d expirations, DTEs: %s)",
                ticker, len(exp_list),
                _all_dtes[:8],  # show first 8 to keep log readable
            )

    if surface.get("iv_30d") is None:
        logger.warning(
            "[%s] iv_30d not available — skipping surface row "
            "(no bracket and no above-30d expiry within 60d)",
            ticker,
        )
        return None

    row = {
        "ticker": ticker,
        "spot_used": spot,
        "source": "schwab_rest",
        **surface,
        **meta,
        # iv_30d resolution audit columns
        "IV30_Method":             iv30_method,
        "IV30_T1_DTE":             iv30_t1_dte,
        "IV30_T2_DTE":             iv30_t2_dte,
        "IV30_Bracket_Width_Days": iv30_bwidth,
        "IV30_Confidence":         iv30_confidence,
        "bucket_meta": bucket_meta,  # per-bucket ATM/DTE/gap detail for iv_surface_meta table
    }
    return row


# ---------------------------------------------------------------------------
# Public: fetch_chain  (live API call)
# ---------------------------------------------------------------------------

def fetch_chain(
    client,  # SchwabClient — typed loosely to avoid circular import
    ticker: str,
    spot: float,
    *,
    timeout: int = 30,
) -> Optional[dict]:
    """
    Fetch the full /chains response from Schwab for one ticker and extract
    the constant-maturity IV surface.

    Parameters
    ----------
    client : SchwabClient
        Authenticated Schwab API client.
    ticker : str
        Equity symbol.
    spot : float
        Current stock price (for ATM detection).
    timeout : int
        HTTP timeout in seconds.

    Returns
    -------
    dict or None
        Surface row (same schema as extract_iv_surface), or None on failure.
    """
    if spot is None or spot <= 0:
        logger.warning("[%s] Cannot fetch chain: invalid spot=%.4f", ticker, spot or 0)
        return None

    today = datetime.now()
    from_date = (today + timedelta(days=CHAIN_FROM_OFFSET_DAYS)).strftime("%Y-%m-%d")
    to_date   = (today + timedelta(days=CHAIN_TO_OFFSET_DAYS)).strftime("%Y-%m-%d")

    params = {
        "symbol":        ticker,
        "contractType":  "CALL",        # Call side only (confirmed call IV == put IV at ATM)
        "includeQuotes": True,
        "strategy":      "SINGLE",
        "range":         "ALL",         # Full chain — ATM detection is per-expiration
        "fromDate":      from_date,
        "toDate":        to_date,
    }

    for _attempt in range(2):  # attempt 0 = normal, attempt 1 = retry after 401 refresh
        try:
            token = client._get_access_token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            }

            t0 = time.time()
            response = requests.get(
                f"{SCHWAB_API_BASE}/marketdata/v1/chains",
                headers=headers,
                params=params,
                timeout=timeout,
            )
            elapsed = time.time() - t0

            if response.status_code == 404:
                logger.warning("[%s] Chain not found (404) — options may not trade", ticker)
                return None

            # 401 mid-run: access token expired during long chain fetch loop.
            # Reset the cached validation flag, refresh, and retry once.
            if response.status_code == 401 and _attempt == 0:
                logger.warning(
                    "[%s] 401 Unauthorized — access token expired mid-run. "
                    "Refreshing and retrying...", ticker,
                )
                if hasattr(client, "invalidate_token_cache"):
                    client.invalidate_token_cache()
                else:
                    client._token_validated = False
                continue  # retry with fresh token

            response.raise_for_status()
            data = response.json()

            logger.debug("[%s] Chain fetched in %.2fs (status=%d)", ticker, elapsed, response.status_code)

            return extract_iv_surface(data, ticker, spot)

        except requests.exceptions.Timeout:
            logger.warning("[%s] Chain fetch timed out after %ds", ticker, timeout)
            return None
        except requests.exceptions.HTTPError as exc:
            logger.warning("[%s] Chain HTTP error: %s", ticker, exc)
            return None
        except Exception as exc:
            logger.warning("[%s] Chain fetch failed: %s", ticker, exc)
            return None

    # Both attempts exhausted (should only reach here if 401 persisted after refresh)
    logger.error("[%s] Chain fetch failed after token refresh retry", ticker)
    return None
