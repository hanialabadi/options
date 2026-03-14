"""
Live Greeks Provider (Schwab Chain Refresh for Held Positions)
==============================================================
Fetches live option chain data for contracts currently held, during market hours.

Design:
  - Only called during 9:30–16:00 ET on weekdays (is_market_open() gate)
  - Per-symbol chain fetch via SchwabClient.get_chains() — one call per ticker
  - Extracts Delta, Gamma, Vega, Theta, IV for the specific contract we hold
    (matched by strike + expiration + call/put)
  - Writes to df as 'IV_Now', 'Delta_Live', etc. — these are transient (not frozen)
  - Also updates option Last/Bid/Ask from live chain to fix P&L staleness
    (Fidelity CSV export timestamps lag market prices → P&L shows stale values)
  - Results cached in session (no repeat calls within same run)
  - Throttled: 500ms between ticker calls to respect Schwab rate limits

Why NOT frozen:
  IV_Entry is intentionally frozen at trade inception.
  IV_Now / Delta_Live / etc. are current-state inputs for doctrine math only.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_CHAIN_DELAY_SEC = 0.5   # 500ms between calls → 2 req/sec max
_OCC_SYMBOL_COL  = "Symbol"   # the full OCC symbol, e.g. INTC260417C50


class LiveGreeksProvider:
    """
    Refreshes option Greeks from live Schwab chains for held positions.

    Usage in run_all.py (after Cycle 2 chart primitives, before Cycle 3 doctrine):
        provider = LiveGreeksProvider()
        if provider.should_refresh():
            df_with_drift = provider.enrich(df_with_drift, schwab_client)
    """

    def __init__(self) -> None:
        self._session_cache: Dict[str, Dict] = {}  # {occ_symbol: greeks_dict}

    # ── Public API ────────────────────────────────────────────────────────────

    def should_refresh(self) -> bool:
        """
        True when live Greeks add value:
          - Market open → always True (chains are live)
          - Market closed → True if broker CSV Greeks are from prior session
            (allows enrichment from session cache when available)
        """
        try:
            from core.shared.data_layer.market_time import is_market_open
            if is_market_open():
                logger.info("[LiveGreeks] Market open — refreshing live Greeks.")
                return True
            # Off-hours: still worth refreshing if we have a Schwab client
            # (chain data from last session close is better than stale CSV)
            logger.debug("[LiveGreeks] Market closed — will attempt refresh from session cache.")
            return True
        except Exception:
            return False

    def enrich(self, df: pd.DataFrame, schwab_client) -> pd.DataFrame:
        """
        For each OPTION row in df, fetch live Greeks from Schwab chain and
        inject as IV_Now, Delta_Live, Gamma_Live, Vega_Live, Theta_Live.

        Broker CSV Greeks remain untouched (Delta, Vega, etc.) — the _Live
        columns are what doctrine uses for real-time edge calculations.
        """
        if df.empty or schwab_client is None:
            return df

        option_mask = df["AssetType"] == "OPTION"
        if not option_mask.any():
            return df

        df = df.copy()
        _ensure_live_cols(df)

        # Determine which symbols to fetch (one chain call per underlying ticker)
        option_rows = df[option_mask].copy()
        tickers = option_rows["Underlying_Ticker"].dropna().unique().tolist()

        fetched_count = 0
        for ticker in tickers:
            ticker_mask = option_mask & (df["Underlying_Ticker"] == ticker)
            ticker_rows = df[ticker_mask]

            # Collect (strike, expiry, call_put) tuples we need
            contracts_needed = _extract_contracts(ticker_rows)
            if not contracts_needed:
                continue

            # Fetch chain (with session cache)
            chain_data = self._get_chain(ticker, schwab_client)
            if not chain_data:
                continue

            # Map chain data back to rows
            _skipped = 0
            for idx in ticker_rows.index:
                row  = df.loc[idx]
                strike   = float(row.get("Strike", 0) or 0)
                exp_date = _parse_expiry(row.get("Expiration"))
                cp       = str(row.get("Call/Put", "") or "").upper()
                if not exp_date or not cp or strike == 0:
                    logger.debug(f"[LiveGreeks] Skipping {ticker} row: missing strike/exp/cp")
                    _skipped += 1
                    continue

                greeks = _extract_greeks_for_contract(chain_data, strike, exp_date, cp)
                if greeks:
                    df.loc[idx, "IV_Now"]       = greeks.get("iv",    np.nan)
                    df.loc[idx, "Delta_Live"]   = greeks.get("delta", np.nan)
                    df.loc[idx, "Gamma_Live"]   = greeks.get("gamma", np.nan)
                    df.loc[idx, "Vega_Live"]    = greeks.get("vega",  np.nan)
                    df.loc[idx, "Theta_Live"]   = greeks.get("theta", np.nan)
                    # Bridge live Greeks to canonical columns doctrine reads
                    if pd.notna(greeks.get("delta")):
                        df.loc[idx, "Delta"] = greeks["delta"]
                    if pd.notna(greeks.get("gamma")):
                        df.loc[idx, "Gamma"] = greeks["gamma"]
                    if pd.notna(greeks.get("vega")):
                        df.loc[idx, "Vega"] = greeks["vega"]
                    if pd.notna(greeks.get("theta")):
                        df.loc[idx, "Theta"] = greeks["theta"]
                    df.loc[idx, "Greeks_Source"] = "schwab_live"
                    df.loc[idx, "Greeks_TS"]    = datetime.now(tz=timezone.utc).isoformat()

                    # Update option pricing from live chain
                    _live_bid  = greeks.get("bid")
                    _live_ask  = greeks.get("ask")
                    _live_last = greeks.get("last")
                    _live_mark = greeks.get("mark")
                    _live_oi   = greeks.get("openInterest")
                    _live_vol  = greeks.get("totalVolume")

                    # Best price estimate: mark > (bid+ask)/2 > last
                    _best_price = None
                    if _live_mark is not None and _live_mark > 0:
                        _best_price = _live_mark
                    elif (_live_bid is not None and _live_ask is not None
                          and _live_bid > 0 and _live_ask > 0):
                        _best_price = (_live_bid + _live_ask) / 2.0
                    elif _live_last is not None and _live_last > 0:
                        _best_price = _live_last

                    if _best_price is not None:
                        _old_last = pd.to_numeric(df.loc[idx, "Last"], errors="coerce")
                        df.loc[idx, "Last"] = round(_best_price, 4)
                        if pd.notna(_old_last) and abs(_best_price - _old_last) > 0.005:
                            logger.debug(
                                f"[LiveGreeks] {ticker} option Last updated: "
                                f"${_old_last:.2f} → ${_best_price:.2f}"
                            )
                    if _live_bid is not None and _live_bid > 0:
                        df.loc[idx, "Bid"] = round(_live_bid, 4)
                    if _live_ask is not None and _live_ask > 0:
                        df.loc[idx, "Ask"] = round(_live_ask, 4)
                    if _live_oi is not None:
                        df.loc[idx, "Open_Int"] = int(_live_oi)
                    if _live_vol is not None:
                        df.loc[idx, "Volume"] = int(_live_vol)

                    fetched_count += 1
                else:
                    logger.debug(
                        f"[LiveGreeks] No chain match for {ticker} "
                        f"strike={strike} exp={exp_date} cp={cp}"
                    )
                    _skipped += 1

            if _skipped:
                logger.info(f"[LiveGreeks] {ticker}: {_skipped} contracts had no chain match.")
            time.sleep(_CHAIN_DELAY_SEC)

        if fetched_count:
            logger.info(
                f"[LiveGreeks] ✅ Refreshed live Greeks + prices for {fetched_count} "
                f"option legs across {len(tickers)} tickers."
            )

            # Recompute P&L for refreshed option rows (Last changed → PnL_Total stale)
            _recompute_option_pnl(df, option_mask)

        return df

    # ── Chain fetch ───────────────────────────────────────────────────────────

    def _get_chain(self, ticker: str, schwab_client) -> Optional[dict]:
        """Fetch and cache the full chain for a ticker (session-scoped)."""
        if ticker in self._session_cache:
            return self._session_cache[ticker]

        try:
            schwab_client.ensure_valid_token()
            chain = schwab_client.get_chains(
                symbol=ticker,
                strikeCount=20,       # ±20 strikes around ATM
                range="ALL",
                strategy="SINGLE",
            )
            if chain:
                self._session_cache[ticker] = chain
                logger.debug(f"[LiveGreeks] Chain fetched for {ticker}.")
            return chain
        except Exception as e:
            logger.warning(f"[LiveGreeks] Chain fetch failed for {ticker}: {e}")
            return None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ensure_live_cols(df: pd.DataFrame) -> None:
    for col in ("IV_Now", "Delta_Live", "Gamma_Live", "Vega_Live", "Theta_Live",
                "Greeks_Source", "Greeks_TS"):
        if col not in df.columns:
            df[col] = np.nan if col not in ("Greeks_Source",) else None


def _float_or_nan(val) -> float:
    """Convert to float, preserving valid 0.0. Only None → NaN."""
    return float(val) if val is not None else np.nan


def safe_update(df: pd.DataFrame, col: str, new_values: pd.Series,
                source_label: str = "") -> int:
    """
    Update df[col] with new_values, but refuse to overwrite non-null with null/NaN.
    Returns count of rejected overwrites (for audit logging).

    RAG: Natenberg (Neutrality) — never destroy valid data with absence of data.
    """
    if col not in df.columns:
        df[col] = new_values
        return 0
    valid_existing = df[col].notna()
    new_is_null = new_values.isna()
    reject_mask = valid_existing & new_is_null
    rejected = int(reject_mask.sum())
    if rejected > 0:
        logger.debug(
            f"[safe_update] Rejected {rejected} NaN overwrites on '{col}'"
            + (f" [{source_label}]" if source_label else "")
        )
    # Only update where we have new non-null values OR existing is null
    update_mask = ~reject_mask
    df.loc[update_mask.index[update_mask], col] = new_values.loc[update_mask.index[update_mask]]
    return rejected


def _parse_expiry(val) -> Optional[str]:
    """Return 'YYYY-MM-DD' string or None."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    try:
        return pd.to_datetime(val).strftime("%Y-%m-%d")
    except Exception:
        return None


def _extract_contracts(ticker_rows: pd.DataFrame) -> List[Tuple]:
    contracts = []
    for _, row in ticker_rows.iterrows():
        strike = float(row.get("Strike", 0) or 0)
        exp    = _parse_expiry(row.get("Expiration"))
        cp     = str(row.get("Call/Put", "") or "").upper()
        if strike and exp and cp in ("C", "P", "CALL", "PUT"):
            contracts.append((strike, exp, cp))
    return contracts


def _extract_greeks_for_contract(
    chain: dict,
    strike: float,
    exp_date: str,
    cp: str,
) -> Optional[dict]:
    """
    Navigate Schwab chain JSON structure to find the specific contract.

    Schwab chain response structure:
    {
      "callExpDateMap": {
        "2026-04-17:55": {           <- "YYYY-MM-DD:DTE"
          "50.0": [ { delta, gamma, vega, theta, volatility, ... } ]
        }
      },
      "putExpDateMap": { ... }
    }
    """
    try:
        cp_norm = cp[0].upper()  # 'C' or 'P'
        exp_map_key = "callExpDateMap" if cp_norm == "C" else "putExpDateMap"
        exp_map = chain.get(exp_map_key, {})

        # Find expiration key (format: "2026-04-17:55" — date:DTE)
        target_key = None
        for key in exp_map:
            if key.startswith(exp_date):
                target_key = key
                break

        if target_key is None:
            logger.debug(f"[LiveGreeks] No expiration match for {exp_date} in {cp_norm} chain")
            return None

        strikes_map = exp_map[target_key]

        # Find strike key — Schwab uses string keys like "50.0"
        strike_key = None
        for sk in strikes_map:
            try:
                if abs(float(sk) - strike) < 0.01:
                    strike_key = sk
                    break
            except (ValueError, TypeError):
                continue

        if strike_key is None:
            logger.debug(f"[LiveGreeks] No strike match for {strike} in expiration {target_key}")
            return None

        contracts = strikes_map[strike_key]
        if not contracts or not isinstance(contracts, list):
            logger.debug(f"[LiveGreeks] Empty contract list for {strike} / {target_key}")
            return None

        c = contracts[0]
        iv_raw = c.get("volatility", c.get("impliedVolatility", None))
        iv = float(iv_raw) / 100.0 if iv_raw is not None else np.nan

        # Extract pricing data for P&L accuracy
        def _safe_float(key):
            v = c.get(key)
            if v is None:
                return None
            try:
                f = float(v)
                return f if f >= 0 else None
            except (ValueError, TypeError):
                return None

        def _safe_int(key):
            v = c.get(key)
            if v is None:
                return None
            try:
                return int(float(v))
            except (ValueError, TypeError):
                return None

        return {
            "iv":    iv,
            "delta": _float_or_nan(c.get("delta")),
            "gamma": _float_or_nan(c.get("gamma")),
            "vega":  _float_or_nan(c.get("vega")),
            "theta": _float_or_nan(c.get("theta")),
            # Pricing — fixes P&L staleness
            "bid":           _safe_float("bid"),
            "ask":           _safe_float("ask"),
            "last":          _safe_float("last"),
            "mark":          _safe_float("mark"),
            "openInterest":  _safe_int("openInterest"),
            "totalVolume":   _safe_int("totalVolume"),
        }

    except Exception as e:
        logger.debug(f"[LiveGreeks] Contract extraction error: {e}")
        return None


def _recompute_option_pnl(df: pd.DataFrame, option_mask: pd.Series) -> None:
    """
    Recompute PnL_Total and Total_GL_Decimal for OPTION rows after live price refresh.

    Mirrors the formula in compute_basic_drift.py:
        Current_Value = Last * Quantity * 100
        PnL_Total = Current_Value - (Basis * sign(Quantity))
        Total_GL_Decimal = PnL_Total / abs(Basis)  [when Basis > 0]
    """
    try:
        refreshed = option_mask & (df.get("Greeks_Source") == "schwab_live")
        if not refreshed.any():
            return

        _last = pd.to_numeric(df.loc[refreshed, "Last"], errors="coerce")
        _qty  = pd.to_numeric(df.loc[refreshed, "Quantity"], errors="coerce")
        _basis = pd.to_numeric(df.loc[refreshed, "Basis"], errors="coerce")

        _current_val = _last * _qty * 100.0
        _pnl = _current_val - (_basis * np.sign(_qty))

        if "PnL_Total" in df.columns:
            df.loc[refreshed, "PnL_Total"] = _pnl
        if "Current_Value" in df.columns:
            df.loc[refreshed, "Current_Value"] = _current_val

        # Update percentage P&L (Total_GL_Decimal) — preferred by _safe_pnl_pct()
        if "Total_GL_Decimal" in df.columns:
            _abs_basis = _basis.abs()
            _gl_pct = np.where(_abs_basis > 0, _pnl / _abs_basis, np.nan)
            df.loc[refreshed, "Total_GL_Decimal"] = _gl_pct

        _updated = int(refreshed.sum())
        logger.debug(f"[LiveGreeks] Recomputed P&L for {_updated} option rows after price refresh.")
    except Exception as e:
        logger.warning(f"[LiveGreeks] P&L recomputation failed (non-fatal): {e}")
