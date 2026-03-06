"""
Equity Integrity State — Lightweight Structural Deterioration Monitor

Answers three questions per stock-backed position:
  1. Is price structurally deteriorating?   (MA slopes, ROC20, lower-high pattern)
  2. Has volatility regime changed?          (HV vs percentile, ATR expansion)
  3. Is price in an early warning zone?      (graduated drawdown from entry)

Output: one column per row — Equity_Integrity_State
  HEALTHY    — no flags
  WEAKENING  — 1–2 signals; monitor without action
  BROKEN     — 3+ signals or hard structural break; doctrine can escalate

Design contract:
  - Only fires on STOCK legs (AssetType == STOCK or EQUITY).
  - All option rows pass through unchanged with state = HEALTHY.
  - Non-blocking: any exception yields HEALTHY (never halts pipeline).
  - No external calls — uses only columns already on df at call time.
  - Companion column Equity_Integrity_Reason carries the collapsed signal list.

Signal scoring (each signal = 1 point):

  Price structure (max 3 pts):
    S1  ema20_slope < 0             — 20-day trend declining
    S2  ema50_slope < 0             — 50-day trend declining (slower confirmation)
    S3  roc_20 < -3.0               — 20-bar momentum negative > 3%

  Drawdown zones (max 1 pt; graduated severity):
    S4a  price_drift_pct < -0.10   — early warning: -10% from entry
    S4b  price_drift_pct < -0.15   — structural: -15% from entry
    S4c  price_drift_pct < -0.20   — critical: within 20% hard-stop zone
    (S4a/S4b/S4c are mutually exclusive; only the deepest fires)

  Volatility regime (max 2 pts):
    S5  hv_20d_percentile > 0.75    — HV in top quartile of own history
    S6  atr_slope > 0.10            — ATR expanding (range widening = instability)

  MA position:
    S7  sma_distance_pct < -0.05   — price > 5% below SMA20

Thresholds:
  score 0       → HEALTHY
  score 1–2     → WEAKENING
  score 3+      → BROKEN
  score >= 2 AND S4c (deep drawdown)  → BROKEN immediately (override)

Natenberg Ch.8: vol regime change is the primary early warning.
McMillan Ch.1: trend deterioration confirmed by MA slope + momentum.
Passarelli Ch.6: drawdown zones set relative to entry basis.
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

# ── Thresholds ────────────────────────────────────────────────────────────────
_EMA20_SLOPE_BEAR   = 0.0     # below zero = declining
_EMA50_SLOPE_BEAR   = 0.0
_ROC20_BEAR         = -3.0    # % — 3% decline over 20 bars
_DRAWDOWN_EARLY     = -0.10   # -10% from entry
_DRAWDOWN_STRUCT    = -0.15   # -15%
_DRAWDOWN_CRITICAL  = -0.20   # -20% (approaching hard stop at -20%)
_HV_PCT_HIGH        = 0.75    # HV in top quartile
_ATR_SLOPE_EXPAND   = 0.10    # ATR slope normalized — expanding range
_SMA_BELOW          = -0.05   # price 5%+ below SMA20

_WEAKENING_SCORE    = 1
_BROKEN_SCORE       = 3


def compute_equity_integrity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add Equity_Integrity_State and Equity_Integrity_Reason columns.

    Only STOCK/EQUITY rows are scored; all other rows receive HEALTHY.
    """
    if df.empty:
        return df

    df = df.copy()
    df["Equity_Integrity_State"]  = "HEALTHY"
    df["Equity_Integrity_Reason"] = ""

    _stock_mask = df.get("AssetType", pd.Series("", index=df.index)).str.upper().isin(
        {"STOCK", "EQUITY"}
    )

    if not _stock_mask.any():
        return df

    for idx in df.index[_stock_mask]:
        try:
            row    = df.loc[idx]
            score  = 0
            flags: list[str] = []

            # ── S1: 20MA slope ────────────────────────────────────────────────
            _e20s = _float(row.get("ema20_slope"))
            if _e20s is not None and _e20s < _EMA20_SLOPE_BEAR:
                score += 1
                flags.append(f"EMA20↓({_e20s:+.4f})")

            # ── S2: 50MA slope ────────────────────────────────────────────────
            _e50s = _float(row.get("ema50_slope"))
            if _e50s is not None and _e50s < _EMA50_SLOPE_BEAR:
                score += 1
                flags.append(f"EMA50↓({_e50s:+.4f})")

            # ── S3: ROC20 momentum ────────────────────────────────────────────
            _roc20 = _float(row.get("roc_20"))
            if _roc20 is not None and _roc20 < _ROC20_BEAR:
                score += 1
                flags.append(f"ROC20={_roc20:+.1f}%")

            # ── S4: Drawdown from entry ───────────────────────────────────────
            _ul_now   = _float(row.get("UL Last"))
            _ul_entry = _float(row.get("Underlying_Price_Entry"))
            _net_cost = _float(row.get("Net_Cost_Basis_Per_Share"))
            _basis    = _net_cost if (_net_cost and _net_cost > 0) else _ul_entry

            drawdown_critical = False
            if _ul_now is not None and _basis and _basis > 0:
                drift_pct = (_ul_now - _basis) / _basis
                if drift_pct < _DRAWDOWN_CRITICAL:
                    score += 1
                    drawdown_critical = True
                    flags.append(f"drawdown={drift_pct:+.1%}(critical)")
                elif drift_pct < _DRAWDOWN_STRUCT:
                    score += 1
                    flags.append(f"drawdown={drift_pct:+.1%}(structural)")
                elif drift_pct < _DRAWDOWN_EARLY:
                    score += 1
                    flags.append(f"drawdown={drift_pct:+.1%}(early)")

            # ── S5: HV percentile (vol regime shift) ─────────────────────────
            _hvpct = _float(row.get("hv_20d_percentile"))
            if _hvpct is not None and _hvpct > _HV_PCT_HIGH:
                score += 1
                # Format as ordinal rank (e.g. "92nd pct") not a percentage level
                # to avoid confusion with "HV=92%" (the actual vol level).
                # hv_20d_percentile is stored as a decimal (0.92 = 92nd percentile).
                _hvpct_rank = int(_hvpct * 100 if _hvpct <= 1.0 else _hvpct)
                _sfx = "st" if _hvpct_rank % 100 not in (11,12,13) and _hvpct_rank % 10 == 1 else \
                       "nd" if _hvpct_rank % 100 not in (11,12,13) and _hvpct_rank % 10 == 2 else \
                       "rd" if _hvpct_rank % 100 not in (11,12,13) and _hvpct_rank % 10 == 3 else "th"
                flags.append(f"HV={_hvpct_rank}{_sfx}_pct")

            # ── S6: ATR slope (range expansion) ──────────────────────────────
            _atrs = _float(row.get("atr_slope"))
            if _atrs is not None and _atrs > _ATR_SLOPE_EXPAND:
                score += 1
                flags.append(f"ATR_slope={_atrs:+.2f}")

            # ── S7: Price below SMA20 ─────────────────────────────────────────
            _sma_d = _float(row.get("sma_distance_pct"))
            if _sma_d is not None and _sma_d < _SMA_BELOW:
                score += 1
                flags.append(f"SMA20_dist={_sma_d:+.1%}")

            # ── Classify ──────────────────────────────────────────────────────
            if score >= _BROKEN_SCORE or (score >= 2 and drawdown_critical):
                state = "BROKEN"
            elif score >= _WEAKENING_SCORE:
                state = "WEAKENING"
            else:
                state = "HEALTHY"

            df.at[idx, "Equity_Integrity_State"]  = state
            df.at[idx, "Equity_Integrity_Reason"] = ", ".join(flags) if flags else ""

        except Exception as _e:
            logger.debug(f"[EquityIntegrity] row {idx} failed (non-fatal): {_e}")
            # Leave as HEALTHY — safe failure mode

    _broken   = (df["Equity_Integrity_State"] == "BROKEN").sum()
    _weakening = (df["Equity_Integrity_State"] == "WEAKENING").sum()
    if _broken + _weakening > 0:
        logger.info(
            f"[EquityIntegrity] BROKEN={_broken}  WEAKENING={_weakening}  "
            f"HEALTHY={_stock_mask.sum() - _broken - _weakening}"
        )

    # ── Propagate stock equity state to option legs of the same trade ─────────
    # The CC/BUY_WRITE covered-call doctrine gate reads Equity_Integrity_State
    # from the OPTION row. Without propagation, the option leg sees HEALTHY even
    # when the underlying stock row is BROKEN, causing the EV engine to recommend
    # ROLL into a structurally declining stock.
    #
    # Propagation rule: if any STOCK leg in a trade is BROKEN/WEAKENING, copy
    # that state to all OPTION legs of the same trade (same TradeID).
    # Option legs do NOT override to HEALTHY — they can only receive a worse state.
    # HEALTHY stock → no change to option legs (they default to HEALTHY already).
    if "TradeID" in df.columns:
        _stock_rows = df[_stock_mask & df["Equity_Integrity_State"].isin({"BROKEN", "WEAKENING"})]
        for _s_idx, _s_row in _stock_rows.iterrows():
            _tid   = _s_row.get("TradeID")
            _state = _s_row["Equity_Integrity_State"]
            _reason = _s_row["Equity_Integrity_Reason"]
            if not _tid:
                continue
            # Option legs in the same trade
            _opt_mask = (
                (df["TradeID"] == _tid)
                & ~_stock_mask
                & (df["Equity_Integrity_State"] == "HEALTHY")  # never downgrade BROKEN → WEAKENING
            )
            if _opt_mask.any():
                df.loc[_opt_mask, "Equity_Integrity_State"]  = _state
                df.loc[_opt_mask, "Equity_Integrity_Reason"] = _reason
                logger.debug(
                    f"[EquityIntegrity] Propagated {_state} from stock to "
                    f"{_opt_mask.sum()} option leg(s) of trade {_tid}"
                )

    return df


def _float(val) -> float | None:
    """Safe float coercion — returns None on NaN/None/non-numeric."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if np.isnan(f) else f
    except (ValueError, TypeError):
        return None
