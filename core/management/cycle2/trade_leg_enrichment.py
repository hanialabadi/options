"""
Trade-Level Leg Enrichment  (Cycle 2 → Cycle 3 bridge)
=======================================================
Solves the multi-leg blindness problem: doctrine runs per-row via df.apply(),
so when it receives the STOCK leg of a BUY_WRITE it sees Strike=NaN, Delta=0,
DTE=NaN, Premium_Entry=NaN — all option-specific gates are silently blind.

Fix: one groupby pass per TradeID.  For each trade, extract key fields from
the relevant option leg and broadcast them onto EVERY leg row of that trade
as prefixed columns (Short_Call_Strike, Short_Call_Delta, …).

Doctrine reads these prefixed columns with fallback to raw column names,
so the fix is fully backwards-compatible.

Leg selection rules
-------------------
Short call  : AssetType=OPTION, Quantity < 0, OptionType contains 'C'
              If multiple (diagonal spreads), pick the one with shortest DTE
              that is still in the future (DTE > 0).
Long put    : AssetType=OPTION, Quantity > 0, OptionType contains 'P' (CSP hedge)
Stock       : AssetType=STOCK

Columns broadcast onto all leg rows
-------------------------------------
Short_Call_Strike        float  — call strike price
Short_Call_Delta         float  — call delta (0–1, absolute value)
Short_Call_DTE           float  — days to expiry
Short_Call_Last          float  — current market price (bid ≈ last for short)
Short_Call_Premium       float  — Premium_Entry (credit received when sold)
Short_Call_Symbol        str    — OCC symbol for audit trail
Short_Call_Expiration    str    — expiry date string
Short_Call_Moneyness     str    — "ITM" / "OTM" / "ATM" relative to stock spot
Stock_Spot               float  — UL Last from stock leg (canonical spot)
Stock_Qty                float  — number of shares (absolute)
"""

from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Strategies that have a meaningful short-call leg
_SHORT_CALL_STRATEGIES = {"BUY_WRITE", "COVERED_CALL"}
# Strategies that may have a short-put leg
_SHORT_PUT_STRATEGIES = {"CSP"}


def enrich_trade_leg_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Broadcast per-leg fields onto all rows of each TradeID.

    Parameters
    ----------
    df : DataFrame with columns including TradeID, AssetType, Strategy,
         Quantity, Strike, Delta, DTE, Last, Premium_Entry, Symbol, Expiration.

    Returns
    -------
    Same DataFrame with additional prefixed columns appended.
    Non-blocking: any per-trade failure logs a warning and leaves that trade's
    columns as NaN.
    """
    if df.empty or "TradeID" not in df.columns:
        return df

    # Initialise all output columns as NaN / empty string
    _FLOAT_COLS = [
        "Short_Call_Strike", "Short_Call_Delta", "Short_Call_DTE",
        "Short_Call_Last",   "Short_Call_Premium",
        "Stock_Spot",        "Stock_Qty",
    ]
    _STR_COLS = [
        "Short_Call_Symbol", "Short_Call_Expiration", "Short_Call_Moneyness",
    ]
    for col in _FLOAT_COLS:
        df[col] = np.nan
    for col in _STR_COLS:
        df[col] = ""

    enriched = 0
    for trade_id, grp in df.groupby("TradeID"):
        try:
            strategy = str(grp["Strategy"].iloc[0] or "").upper()

            if strategy in _SHORT_CALL_STRATEGIES:
                _enrich_short_call(df, grp, trade_id)
                enriched += 1

            elif strategy in _SHORT_PUT_STRATEGIES:
                _enrich_short_put(df, grp, trade_id)
                enriched += 1

            # Stock spot + qty is useful for every strategy
            _enrich_stock(df, grp, trade_id)

        except Exception as e:
            logger.warning(
                f"[TradeLegEnrichment] Trade {trade_id} enrichment failed (non-fatal): {e}"
            )

    logger.info(
        f"[TradeLegEnrichment] Enriched {enriched} trades with per-leg summary columns."
    )
    return df


# ── Internal helpers ──────────────────────────────────────────────────────────

def _enrich_short_call(df: pd.DataFrame, grp: pd.DataFrame, trade_id) -> None:
    """
    Find the short call leg and broadcast its fields.
    For diagonals with multiple short calls, pick the one with shortest future DTE.
    """
    opt = grp[
        (grp.get("AssetType", pd.Series(dtype=str)) == "OPTION")
        & (pd.to_numeric(grp.get("Quantity", pd.Series(dtype=float)),
                         errors="coerce").fillna(0) < 0)
    ].copy()

    if opt.empty:
        # Fallback: any OPTION leg with negative quantity (AssetType may differ)
        opt = grp[
            pd.to_numeric(grp.get("Quantity", pd.Series(dtype=float)),
                          errors="coerce").fillna(0) < 0
        ].copy()

    if opt.empty:
        return

    # Filter to calls only if OptionType or Symbol hint is available
    if "OptionType" in opt.columns:
        calls = opt[opt["OptionType"].astype(str).str.upper().str.contains("C", na=False)]
        if not calls.empty:
            opt = calls

    # Among remaining candidates, pick shortest future DTE
    dte_col = pd.to_numeric(opt.get("DTE", pd.Series(dtype=float)), errors="coerce")
    future = opt[dte_col.fillna(-1) >= 0]
    if not future.empty:
        opt = future
    leg = opt.sort_values(
        by="DTE" if "DTE" in opt.columns else opt.columns[0],
        ascending=True,
        key=lambda s: pd.to_numeric(s, errors="coerce").fillna(9999)
    ).iloc[0]

    strike  = _flt(leg, "Strike")
    delta   = abs(_flt(leg, "Delta") or 0.0)
    dte     = _flt(leg, "DTE")
    last    = _flt(leg, "Last")
    premium = _flt(leg, "Premium_Entry")
    symbol  = str(leg.get("Symbol") or "")
    expiry  = str(leg.get("Expiration") or "")

    # Moneyness relative to stock spot
    spot    = _stock_spot(grp)
    moneyness = ""
    if strike is not None and spot is not None:
        if spot > strike * 1.005:
            moneyness = "ITM"
        elif spot < strike * 0.995:
            moneyness = "OTM"
        else:
            moneyness = "ATM"

    mask = df["TradeID"] == trade_id
    df.loc[mask, "Short_Call_Strike"]     = strike
    df.loc[mask, "Short_Call_Delta"]      = delta
    df.loc[mask, "Short_Call_DTE"]        = dte
    df.loc[mask, "Short_Call_Last"]       = last
    df.loc[mask, "Short_Call_Premium"]    = premium
    df.loc[mask, "Short_Call_Symbol"]     = symbol
    df.loc[mask, "Short_Call_Expiration"] = expiry
    df.loc[mask, "Short_Call_Moneyness"]  = moneyness


def _enrich_short_put(df: pd.DataFrame, grp: pd.DataFrame, trade_id) -> None:
    """Broadcast short put leg fields (CSP etc.)."""
    opt = grp[
        (grp.get("AssetType", pd.Series(dtype=str)) == "OPTION")
        & (pd.to_numeric(grp.get("Quantity", pd.Series(dtype=float)),
                         errors="coerce").fillna(0) < 0)
    ].copy()
    if opt.empty:
        return
    leg = opt.iloc[0]

    mask = df["TradeID"] == trade_id
    df.loc[mask, "Short_Call_Strike"]  = _flt(leg, "Strike")   # reuse prefix; it's the put strike
    df.loc[mask, "Short_Call_Delta"]   = abs(_flt(leg, "Delta") or 0.0)
    df.loc[mask, "Short_Call_DTE"]     = _flt(leg, "DTE")
    df.loc[mask, "Short_Call_Last"]    = _flt(leg, "Last")
    df.loc[mask, "Short_Call_Premium"] = _flt(leg, "Premium_Entry")
    df.loc[mask, "Short_Call_Symbol"]  = str(leg.get("Symbol") or "")


def _enrich_stock(df: pd.DataFrame, grp: pd.DataFrame, trade_id) -> None:
    """Broadcast stock leg spot price and quantity."""
    stock = grp[grp.get("AssetType", pd.Series(dtype=str)) == "STOCK"]
    if stock.empty:
        return
    leg  = stock.iloc[0]
    spot = _flt(leg, "UL Last") or _flt(leg, "Last")
    qty  = abs(_flt(leg, "Quantity") or 0.0)
    mask = df["TradeID"] == trade_id
    df.loc[mask, "Stock_Spot"] = spot
    df.loc[mask, "Stock_Qty"]  = qty


def _flt(row: pd.Series, col: str) -> Optional[float]:
    v = row.get(col)
    if v is None:
        return None
    try:
        f = float(v)
        return None if np.isnan(f) else f
    except Exception:
        return None


def _stock_spot(grp: pd.DataFrame) -> Optional[float]:
    stock = grp[grp.get("AssetType", pd.Series(dtype=str)) == "STOCK"]
    if stock.empty:
        return None
    leg = stock.iloc[0]
    return _flt(leg, "UL Last") or _flt(leg, "Last")
