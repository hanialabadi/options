"""
Orchestrator — generate_recommendations() for the management engine.

Extracted from engine.py. Contains:
- _build_journey_note() helper
- generate_recommendations() main orchestration function

This is the entry point called by run_all.py to evaluate all positions
through the doctrine engine.
"""
import os
import logging
from typing import Dict, Any

import numpy as np
import pandas as pd

from core.shared.data_layer.technical_data_repository import get_latest_technical_indicators
from core.shared.data_layer.market_stress_detector import (
    classify_market_stress, should_halt_trades, get_halt_reason,
)
from scan_engine.loaders.schwab_api_client import SchwabClient

from core.management.cycle3.decision.resolver import StrategyResolver
from core.management.cycle3.doctrine import (
    DoctrineAuthority,
    STATE_ACTIONABLE,
    STATE_NEUTRAL_CONFIDENT,
    STATE_UNCERTAIN,
    STATE_BLOCKED_GOVERNANCE,
    STATE_UNRESOLVED_IDENTITY,
    REASON_IV_AUTHORITY_MISSING,
    REASON_STOCK_LEG_NOT_AVAILABLE,
)

logger = logging.getLogger(__name__)


def _build_journey_note(row: pd.Series, current_action: str, ul_last: float) -> str:
    """
    Build a compact trade journey note for inclusion in any rationale string.

    Reads the Prior_* columns injected by run_all.py (2.95 Journey Context block)
    and returns a one-liner that tells the continuous story of the trade so far,
    with RAG book citations for each transition type.

    Returns empty string if no prior context is available (first run or DB failure).

    RAG grounding:
      EXIT→HOLD  — McMillan Ch.4: "If price pulls back after a profit-take signal,
                   re-evaluate: is the pullback a retracement or a new downtrend?"
      ROLL→HOLD  — Passarelli Ch.6: "Don't force a roll into a choppy market —
                   pre-stage and wait for directional clarity."
      HOLD n-day — Passarelli Ch.5: "Patience while theta works is a position,
                   not a lapse in management."
      HOLD→EXIT  — Natenberg Ch.11: "Sustained LATE_CYCLE + rising hold cost =
                   edge is being consumed. Escalate."
    """
    prior_action  = str(row.get("Prior_Action")  or "").strip().upper()
    prior_price   = row.get("Prior_UL_Last")
    prior_ts      = row.get("Prior_Snapshot_TS")
    days_ago      = row.get("Prior_Days_Ago")

    # Translate legacy labels stored before the rename
    prior_action = {"REVALIDATE": "REVIEW", "ASSIGN": "LET_EXPIRE"}.get(prior_action, prior_action)

    # User-facing display names — one label per behavior, details in the note text
    _ACTION_DISPLAY_NAMES = {
        "WRITE_NOW": "WRITE CALL", "HOLD_STOCK_WAIT": "DEFER WRITING",
        "PAUSE_WRITING": "DEFER WRITING", "EXIT_STOCK": "EXIT",
        "ROLL_UP_OUT": "ROLL", "ROLL_WAIT": "ROLL",
        "HOLD_WITH_CAUTION": "HOLD", "HOLD_FOR_REVERSION": "HOLD",
        "LET_EXPIRE": "LET EXPIRE",
        "ACCEPT_CALL_AWAY": "ACCEPT CALL AWAY",
        "ACCEPT_SHARE_ASSIGNMENT": "ACCEPT SHARE ASSIGNMENT",
        "SCALE_UP": "SCALE UP",
    }
    _prior_disp = _ACTION_DISPLAY_NAMES.get(prior_action, prior_action)
    _current_disp = _ACTION_DISPLAY_NAMES.get(current_action, current_action)

    if not prior_action or prior_action in ("", "NONE", "NAN"):
        return ""   # first run — no history

    # Format prior timestamp as human-readable
    try:
        _ts_str = pd.Timestamp(prior_ts).strftime("%b %-d") if prior_ts is not None else "prior run"
    except Exception:
        _ts_str = "prior run"

    # Days label
    try:
        _days = float(days_ago)
        _days_str = f"{_days:.0f}d ago" if _days >= 1 else "today"
    except (TypeError, ValueError):
        _days_str = "recently"

    # Price change since last signal
    _price_note = ""
    try:
        _pp = float(prior_price)
        _delta_pct = (ul_last - _pp) / _pp
        _dir = "↑" if _delta_pct > 0 else "↓"
        _price_note = f" (stock {_dir}{abs(_delta_pct):.1%} since then: ${_pp:.2f} → ${ul_last:.2f})"
    except (TypeError, ValueError):
        pass

    # Transition-aware citation
    flip = f"{prior_action}→{current_action}"
    if prior_action == "EXIT" and current_action == "HOLD":
        cite = (
            "Exit signal not acted on — stock retraced. "
            "Re-evaluate: retracement or new downtrend? "
            "(McMillan Ch.4: re-entry after failed exit)"
        )
    elif prior_action == "EXIT" and current_action == "EXIT":
        cite = "Exit signal persists — urgency confirmed (McMillan Ch.4)."
    elif prior_action == "ROLL" and current_action == "HOLD":
        cite = (
            "Roll blocked by timing gate — pre-staged candidates ready when market clarifies "
            "(Passarelli Ch.6: don't force a roll into choppy market)."
        )
    elif prior_action == "HOLD" and current_action == "HOLD":
        # Strategy-aware HOLD language: "theta works" is wrong for long options
        # where theta is an enemy (net theta < 0).
        _entry_struct = str(row.get("Entry_Structure", "") or "").upper()
        _is_long_option = _entry_struct in (
            "LONG_CALL", "LONG_PUT", "LEAPS_CALL", "LEAPS_PUT",
            "BUY_CALL", "BUY_PUT", "LEAP_CALL", "LEAP_PUT",
        )
        if _is_long_option:
            cite = (
                f"Holding {_days_str} — thesis monitoring continues "
                f"(Passarelli Ch.5: directional conviction required while time decays)."
            )
        else:
            cite = f"Holding {_days_str} — thesis monitoring continues (Passarelli Ch.5: patience while theta works)."
    elif prior_action == "HOLD" and current_action == "EXIT":
        cite = "Condition deteriorated since last HOLD — escalating to EXIT (Natenberg Ch.11: edge consumed)."
    elif prior_action == "HOLD" and current_action == "ROLL":
        cite = "Timing gate cleared — executing pre-staged roll (Passarelli Ch.6)."
    elif prior_action == "TRIM" and current_action == "HOLD":
        cite = "Partial trim executed — holding remaining position (McMillan Ch.4: scale out, not all-or-nothing)."
    elif current_action == "BUYBACK":
        cite = "Carry inversion detected — buy back short call, hold stock unencumbered (Given Ch.6 + Jabbour Ch.11)."
    elif prior_action == "BUYBACK" and current_action == "HOLD":
        cite = "Short call bought back — holding stock only until structure resolves (McMillan Ch.3: uncap position)."
    else:
        cite = f"Prior: {_prior_disp} → Now: {_current_disp}."

    return f"📖 Journey ({_ts_str}, {_days_str}): Prior signal was **{_prior_disp}**{_price_note}. {cite}"


def coordinate_multi_leg_actions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Post-doctrine leg coordination for multi-leg trades.

    Multi-leg trades (BUY_WRITE, COVERED_CALL) have STOCK + OPTION rows
    evaluated independently by doctrine. If the STOCK leg says EXIT but the
    OPTION leg says HOLD or ROLL, acting on the stock exit while keeping the
    short call creates a naked short position.

    This pass enforces coherent unwinding: if the stock leg exits, the
    option leg must exit too.

    Doctrine: McMillan Ch.2 — "Never allow a covered write to become an
    uncovered write through partial execution of exit signals."
    """
    _MULTI_LEG_STRATEGIES = {'BUY_WRITE', 'COVERED_CALL', 'PMCC'}
    if 'TradeID' not in df.columns or 'AssetType' not in df.columns:
        return df

    for trade_id, grp in df.groupby('TradeID'):
        if len(grp) < 2:
            continue
        strategy = str(grp['Strategy'].iloc[0] or '').upper()
        if strategy not in _MULTI_LEG_STRATEGIES:
            continue

        stock_mask = grp['AssetType'].str.upper() == 'STOCK'
        option_mask = grp['AssetType'].str.upper() == 'OPTION'
        if not stock_mask.any() or not option_mask.any():
            continue

        stock_idx = grp.index[stock_mask][0]
        stock_action = str(grp.loc[stock_mask, 'Action'].iloc[0] or '').upper()
        stock_urgency = str(grp.loc[stock_mask, 'Urgency'].iloc[0] or '').upper()

        if stock_action != 'EXIT':
            continue

        # ── Income structure guard ──────────────────────────────────────
        # In BW/CC, the option leg (buy_write/covered_call doctrine) is the
        # authoritative voice — it evaluates combined position EV, recovery
        # state, premium credit, and thesis.  The stock leg runs stock_only
        # doctrine which sees raw broker P&L without premium adjustment.
        #
        # When the option leg says ROLL/HOLD/WRITE_NOW (EV-positive income
        # action) but the stock leg says EXIT (based on raw P&L), the option
        # leg's recommendation should override — not the other way around.
        #
        # Exception: if the stock leg EXIT is from a truly structural cause
        # (hard stop breach, thesis/story BROKEN), it should always win.
        #
        # NOT structural: EquityIntegrity BROKEN — this is a price-based
        # metric that uses raw purchase cost, not net cost after premiums.
        # For income positions at near-breakeven after premium credits
        # (e.g. DKNG at -0.3% net cost with $4.45/share collected),
        # equity integrity BROKEN is a false positive that should NOT
        # override the option leg's EV-based ROLL recommendation.
        #
        # Jabbour Ch.4: "Recovery income path is a portfolio commitment —
        # the income leg's EV calculation is authoritative for the structure."
        _stock_source = str(df.at[stock_idx, 'Doctrine_Source'] or '').upper()
        _stock_rationale = str(df.at[stock_idx, 'Rationale'] or '').upper()
        _is_structural_exit = (
            'HARD STOP' in _stock_source
            or 'HARD_STOP' in _stock_source
            or 'HARD_HALT' in _stock_source
            or 'DEEP LOSS STOP' in _stock_source
            or ('STORY' in _stock_source and 'BROKEN' in _stock_source)
            or ('THESIS' in _stock_source and 'BROKEN' in _stock_source)
            or 'STORY CHECK' in _stock_source
        )

        # Check if any option leg has an active income recommendation.
        # LET_EXPIRE = short call expires worthless (keep premium, best income outcome).
        # ACCEPT_CALL_AWAY = shares called at strike (profitable assignment).
        # Both are income-structure decisions by the option leg authority.
        _income_actions = {
            'ROLL', 'HOLD', 'ROLL_UP_OUT', 'WRITE_NOW', 'HOLD_STOCK_WAIT',
            'LET_EXPIRE', 'ACCEPT_CALL_AWAY',
        }
        _option_says_income = any(
            str(df.at[oidx, 'Action'] or '').upper() in _income_actions
            for oidx in grp.index[option_mask]
        )

        if _option_says_income and not _is_structural_exit:
            # Option leg (income authority) overrides stock leg EXIT.
            # Downgrade stock leg to match option leg — the stock stays.
            _opt_first = grp.index[option_mask][0]
            _opt_action = str(df.at[_opt_first, 'Action'] or '').upper()
            _opt_urgency = str(df.at[_opt_first, 'Urgency'] or '').upper()

            df.at[stock_idx, 'Action'] = 'HOLD'
            df.at[stock_idx, 'Urgency'] = _opt_urgency
            _override_note = (
                f"\n📊 **Income structure override**: Stock leg EXIT downgraded to HOLD — "
                f"option leg recommends {_opt_action} {_opt_urgency} "
                f"(income authority). Stock_only doctrine evaluated raw broker P&L "
                f"without premium credit adjustment. The option leg's EV calculation "
                f"is authoritative for this income structure. "
                f"(Jabbour Ch.4: income leg is authoritative for BW/CC positions)"
            )
            df.at[stock_idx, 'Rationale'] = str(df.at[stock_idx, 'Rationale'] or '') + _override_note
            df.at[stock_idx, 'Winning_Gate'] = 'income_structure_override'
            df.at[stock_idx, 'Leg_Coordination_Override'] = True

            logger.info(
                f"[LegCoordination] {trade_id}: stock EXIT downgraded to HOLD — "
                f"option leg {_opt_action} is income authority"
            )
            continue  # Skip the EXIT escalation below

        # Stock is exiting (structural) — check each option leg
        for opt_idx in grp.index[option_mask]:
            opt_action = str(df.at[opt_idx, 'Action'] or '').upper()
            if opt_action in ('EXIT', 'BUYBACK'):
                continue  # already unwinding — no conflict

            # Escalate: HOLD/ROLL → EXIT to prevent naked short
            _prior_action = opt_action
            _prior_gate = str(df.at[opt_idx, 'Winning_Gate'] or '')
            _prior_urgency = str(df.at[opt_idx, 'Urgency'] or '')

            df.at[opt_idx, 'Action'] = 'EXIT'
            # Match or exceed stock leg urgency
            _urg_rank = {'CRITICAL': 4, 'HIGH': 3, 'MEDIUM': 2, 'LOW': 1}
            _stock_urg_rank = _urg_rank.get(stock_urgency, 1)
            _opt_urg_rank = _urg_rank.get(_prior_urgency, 1)
            df.at[opt_idx, 'Urgency'] = (
                stock_urgency if _stock_urg_rank > _opt_urg_rank else _prior_urgency
            )

            _coord_note = (
                f"\n⚠️ **Leg coordination override**: Option was {_prior_action} {_prior_urgency} "
                f"(gate: {_prior_gate}) but stock leg is EXIT {stock_urgency}. "
                f"Escalated to EXIT to prevent naked short — cannot hold short call "
                f"without underlying stock. Close both legs together. "
                f"(McMillan Ch.2: never allow covered write to become uncovered)"
            )
            df.at[opt_idx, 'Rationale'] = str(df.at[opt_idx, 'Rationale'] or '') + _coord_note
            df.at[opt_idx, 'Winning_Gate'] = 'leg_coordination_exit'
            df.at[opt_idx, 'Leg_Coordination_Override'] = True

            logger.info(
                f"[LegCoordination] {trade_id}: option leg {_prior_action}→EXIT "
                f"(stock exiting {stock_urgency}, gate={_prior_gate})"
            )

    return df


def generate_recommendations(df_signals: pd.DataFrame) -> pd.DataFrame:
    if df_signals.empty:
        return df_signals
    
    if df_signals.columns.duplicated().any():
        dupes = df_signals.columns[df_signals.columns.duplicated()].unique().tolist()
        raise ValueError(f"Cycle-3 Contract Violation: Duplicate columns detected: {dupes}")

    df = StrategyResolver.resolve(df_signals)

    # Initialize Schwab client (optional, for live data)
    schwab_client = None
    try:
        client_id = os.getenv("SCHWAB_APP_KEY")
        client_secret = os.getenv("SCHWAB_APP_SECRET")
        if client_id and client_secret:
            schwab_client = SchwabClient(client_id, client_secret)
            logger.info("✅ Schwab client initialized for market stress detection.")
    except Exception as e:
        logger.warning(f"⚠️ Schwab client initialization failed for market stress: {e}")

    # Classify market stress once per run
    market_stress_level, market_stress_metric, market_stress_basis = classify_market_stress(client=schwab_client)
    logger.info(f"📊 Global Market Stress: {market_stress_level} (Metric: {market_stress_basis}={market_stress_metric:.2f})")

    # Fetch technical indicators from the repository
    # Note: _enrich_from_scan_engine() in compute_primitives.py already reads most
    # Signal Hub columns. This second fetch catches any columns not in that explicit
    # list and acts as a safety net. Merge on Underlying_Ticker only (not Snapshot_TS,
    # which would silently produce NaN joins due to timestamp precision mismatch).
    if 'Underlying_Ticker' in df.columns:
        underlying_tickers = df['Underlying_Ticker'].dropna().unique().tolist()
        if underlying_tickers:
            df_tech_indicators = get_latest_technical_indicators(underlying_tickers)
            if df_tech_indicators is not None and not df_tech_indicators.empty:
                # Rename Ticker → Underlying_Ticker if needed before merging
                if 'Ticker' in df_tech_indicators.columns and 'Underlying_Ticker' not in df_tech_indicators.columns:
                    df_tech_indicators = df_tech_indicators.rename(columns={'Ticker': 'Underlying_Ticker'})
                # Drop Snapshot_TS from tech indicators to avoid merge key mismatch —
                # management Snapshot_TS and scan Snapshot_TS have different timestamps.
                _tech_drop = [c for c in ['Snapshot_TS', 'Computed_TS'] if c in df_tech_indicators.columns]
                if _tech_drop:
                    df_tech_indicators = df_tech_indicators.drop(columns=_tech_drop)
                # Dedup: one row per ticker (get_latest already does this, but belt-and-suspenders)
                df_tech_indicators = df_tech_indicators.drop_duplicates(subset=['Underlying_Ticker'])
                df = pd.merge(df, df_tech_indicators, on='Underlying_Ticker', how='left', suffixes=('', '_Tech'))
                logger.info(f"✅ Merged {len(df_tech_indicators)} technical indicator records.")
            else:
                logger.warning("⚠️ No technical indicators found in repository for current tickers.")
        else:
            logger.warning("⚠️ No underlying tickers found to fetch technical indicators.")
    else:
        logger.warning("⚠️ 'Underlying_Ticker' column missing, cannot fetch technical indicators.")

    # === Ticker-Level Holistic Analysis (RAG: Strategic Context) ===
    # We calculate holistic health per ticker to inform individual leg decisions.
    # This addresses the "CSP evaluated in isolation" problem.
    if 'Underlying_Ticker' in df.columns and 'PCS' in df.columns:
        ticker_health = df.groupby('Underlying_Ticker')['PCS'].transform('mean')
        df['Ticker_Holistic_Health'] = ticker_health
        # Assignment is acceptable if holistic ticker health is strong
        df['Assignment_Acceptable'] = ticker_health > 70
    else:
        logger.warning("⚠️ Underlying_Ticker or PCS missing — using default holistic health")
        df['Ticker_Holistic_Health'] = 75.0
        df['Assignment_Acceptable'] = True

    # === Per-Ticker Context (for multi-leg coherence in doctrine) ===
    # Computes net delta and stock presence per underlying, injected as _Ticker_* columns
    # so doctrine functions can reason about portfolio coherence without signature changes.

    def _classify_ticker_structure(strategies: list, net_delta: float, net_vega: float) -> str:
        """Detect collective structure type for multi-trade tickers."""
        s = set(strategies)
        has_bw     = bool(s & {"BUY_WRITE", "COVERED_CALL"})
        has_pmcc   = "PMCC" in s
        has_call   = bool(s & {"BUY_CALL", "LONG_CALL"})
        has_put    = bool(s & {"BUY_PUT",  "LONG_PUT"})
        has_leap_c = "LEAPS_CALL" in s
        has_leap_p = "LEAPS_PUT"  in s

        if has_pmcc:                                    return "CALL_DIAGONAL"
        if has_bw:                                      return "INCOME_WITH_LEGS"
        if has_call and has_put and has_leap_c:         return "BULL_VOL_LEVERED"
        if has_call and has_put and has_leap_p:         return "BEAR_VOL_LEVERED"
        if has_call and has_put:
            if net_delta > 0.3:                         return "STRADDLE_BULLISH_TILT"
            if net_delta < -0.3:                        return "STRADDLE_BEARISH_TILT"
            return "STRADDLE_SYNTHETIC"
        if has_leap_c and has_call and not has_put:     return "CALL_DIAGONAL"
        if has_leap_c and has_put  and not has_call:    return "BULL_HEDGE"
        if has_leap_p and has_call and not has_put:     return "BEAR_HEDGE"
        if has_leap_p and has_put  and not has_call:    return "PUT_DIAGONAL"
        if len(s) == 1:                                 return "SINGLE_LEG"
        return "MULTI_LEG_MIXED"

    ticker_ctx: dict = {}
    if 'Underlying_Ticker' in df.columns:
        for ticker, grp in df.groupby('Underlying_Ticker'):
            # Exclude expired options (DTE <= 0) from delta sum — stale data, settlement pending
            if 'DTE' in grp.columns and 'AssetType' in grp.columns:
                live_mask = ~((grp['AssetType'].str.upper() == 'OPTION') & (grp['DTE'].fillna(999) <= 0))
                live_grp = grp[live_mask]
            else:
                live_grp = grp
            delta_sum = float(live_grp['Delta'].fillna(0).sum()) if 'Delta' in live_grp.columns else 0.0
            has_stock = bool((grp.get('AssetType', pd.Series(dtype=str)) == 'STOCK').any()) \
                if 'AssetType' in grp.columns else False
            net_vega  = float(live_grp['Vega'].fillna(0).sum())  if 'Vega'  in live_grp.columns else 0.0
            net_theta = float(live_grp['Theta'].fillna(0).sum()) if 'Theta' in live_grp.columns else 0.0
            strat_list = live_grp['Strategy'].dropna().tolist() if 'Strategy' in live_grp.columns else []
            ticker_ctx[str(ticker)] = {
                'net_delta':       delta_sum,
                'has_stock':       has_stock,
                'net_vega':        net_vega,
                'net_theta':       net_theta,
                'trade_count':     live_grp['TradeID'].nunique() if 'TradeID' in live_grp.columns else 1,
                'strategy_mix':    ",".join(sorted(set(strat_list))),
                'structure_class': _classify_ticker_structure(strat_list, delta_sum, net_vega),
            }

    if ticker_ctx and 'Underlying_Ticker' in df.columns:
        df['_Ticker_Net_Delta'] = df['Underlying_Ticker'].map(
            lambda t: ticker_ctx.get(str(t), {}).get('net_delta', 0.0)
        ).fillna(0.0)
        df['_Ticker_Has_Stock'] = df['Underlying_Ticker'].map(
            lambda t: ticker_ctx.get(str(t), {}).get('has_stock', False)
        ).fillna(False)
        for _col_key, _col_name in [
            ('net_vega',        '_Ticker_Net_Vega'),
            ('net_theta',       '_Ticker_Net_Theta'),
            ('trade_count',     '_Ticker_Trade_Count'),
            ('strategy_mix',    '_Ticker_Strategy_Mix'),
            ('structure_class', '_Ticker_Structure_Class'),
        ]:
            df[_col_name] = df['Underlying_Ticker'].map(
                lambda t, k=_col_key: ticker_ctx.get(str(t), {}).get(k, None)
            )
    else:
        df['_Ticker_Net_Delta'] = 0.0
        df['_Ticker_Has_Stock'] = False
        df['_Ticker_Net_Vega']       = 0.0
        df['_Ticker_Net_Theta']      = 0.0
        df['_Ticker_Trade_Count']    = 1
        df['_Ticker_Strategy_Mix']   = ""
        df['_Ticker_Structure_Class'] = "SINGLE_LEG"

    # === Structural Decay Regime Classifier ===
    # Synthesises cross-signal convergence for long-vol structures.
    # Individual signals (IV_ROC, Delta_ROC, chop, theta) are component-aware.
    # This pass makes the engine regime-aware: it detects when those components
    # align simultaneously into "chop + IV compression + long vol = silent bleed."
    #
    # Output column: _Structural_Decay_Regime
    #   STRUCTURAL_DECAY  — 3+ unfavorable signals converging; escalate Signal_State
    #   DECAY_RISK        — 2 signals converging; warn but don't escalate
    #   NONE              — insufficient convergence or not a long-vol structure

    _LONG_VOL_STRUCTURES = {
        "BULL_VOL_LEVERED", "BEAR_VOL_LEVERED",
        "STRADDLE_SYNTHETIC", "STRADDLE_BULLISH_TILT", "STRADDLE_BEARISH_TILT",
        "CALL_DIAGONAL", "PUT_DIAGONAL", "BULL_HEDGE", "BEAR_HEDGE",
        "MULTI_LEG_MIXED",  # include — needs manual review
    }

    def _score_structural_decay(row) -> str:
        structure = str(row.get('_Ticker_Structure_Class', '') or '')
        net_vega  = float(row.get('_Ticker_Net_Vega',  0) or 0)
        net_theta = float(row.get('_Ticker_Net_Theta', 0) or 0)

        # Only classify long-vol structures that are net-vega-long and theta-negative
        if structure not in _LONG_VOL_STRUCTURES:
            return "NONE"
        if net_vega <= 0:
            return "NONE"
        if net_theta >= -5:          # less than $5/day theta drag — not material
            return "NONE"

        score = 0

        # Signal 1: IV actively compressing
        # Threshold is IV-level-relative, not absolute.
        # Natenberg Ch.8: "meaningful moves in volatility-normalized terms."
        # A -0.08 absolute drop on a 25% IV stock = 32% of its level (extreme).
        # The same drop on a 120% IV stock (MSTR/COIN) = 7% of its level (noise).
        # Formula: threshold = max(0.08, IV_Now × 0.20) — 20% of current IV level.
        # Floor of 0.08 prevents the threshold collapsing on low-vol names.
        # Severe: 2× the base threshold (regime-scaled).
        iv_roc  = pd.to_numeric(row.get('IV_ROC_3D'), errors='coerce')
        iv_now  = pd.to_numeric(row.get('IV_Now'),    errors='coerce')
        _iv_lvl = float(iv_now) if pd.notna(iv_now) and iv_now > 0 else float('nan')
        # Normalise: stored as decimal (0.33) or percentage (33.0)
        if pd.notna(_iv_lvl) and _iv_lvl >= 5:
            _iv_lvl = _iv_lvl / 100.0
        _iv_compress_thresh = max(0.08, _iv_lvl * 0.20) if pd.notna(_iv_lvl) else 0.08
        _iv_severe_thresh   = _iv_compress_thresh * 2.0

        if pd.notna(iv_roc) and iv_roc < -_iv_compress_thresh:
            score += 1
            if iv_roc < -_iv_severe_thresh:      # severe compression (regime-scaled)
                score += 1

        # Signal 2: Range-bound / choppy price action (realized vol not helping)
        chop = float(row.get('choppiness_index', 50) or 50)
        adx  = float(row.get('adx_14', 25) or 25)
        if chop > 55 or adx < 22:
            score += 1

        # Signal 3: No directional move to rescue long delta/gamma
        d_roc = pd.to_numeric(row.get('Delta_ROC_3D'), errors='coerce')
        if pd.notna(d_roc) and abs(d_roc) < 0.08:
            score += 1

        # Signal 4: IV below HV (selling premium into you is cheap — you're losing edge)
        iv_gap = pd.to_numeric(row.get('IV_vs_HV_Gap'), errors='coerce')
        if pd.notna(iv_gap) and iv_gap < -0.05:
            score += 1

        if score >= 3:
            return "STRUCTURAL_DECAY"
        if score >= 2:
            return "DECAY_RISK"
        return "NONE"

    df['_Structural_Decay_Regime'] = df.apply(_score_structural_decay, axis=1)

    STRATEGY_NORMALIZATION_MAP = {
        'Covered_Call': 'COVERED_CALL',
        'Cash_Secured_Put': 'CSP',
        'Buy_Call': 'BUY_CALL',
        'Buy_Put': 'BUY_PUT',
        'LEAPS_Call': 'LEAPS_CALL',
        'LEAPS_Put': 'LEAPS_PUT',
        'Buy_Write': 'BUY_WRITE',
        'Long_Straddle': 'STRADDLE',
        'Long_Strangle': 'STRANGLE',
        'PMCC': 'PMCC',
        'STOCK_ONLY': 'STOCK_ONLY',
        'STOCK_ONLY_IDLE': 'STOCK_ONLY_IDLE',
        'LEAPS_CALL': 'LEAPS_CALL',
        'LEAPS_PUT': 'LEAPS_PUT',
        'Unknown': 'UNKNOWN'
    }

    if 'Strategy' in df.columns:
        df['Strategy'] = df['Strategy'].map(lambda x: STRATEGY_NORMALIZATION_MAP.get(x, x))
    else:
        logger.warning("⚠️ Strategy column missing in generate_recommendations")

    # RAG: Ensure all required columns for dashboard exist
    for col in ['Uncertainty_Reasons', 'Missing_Data_Fields']:
        if col not in df.columns:
            df[col] = [[] for _ in range(len(df))]

    def apply_guards(row):
        reasons = []
        missing_fields = []
        strategy = row.get('Strategy', 'UNKNOWN')
        
        # Global Market Stress Guard (Tier 1)
        if should_halt_trades(market_stress_level):
            return {
                "Action": "HALT",
                "Urgency": "CRITICAL",
                "Rationale": get_halt_reason(market_stress_level, market_stress_metric, market_stress_basis),
                "Decision_State": STATE_BLOCKED_GOVERNANCE,
                "Uncertainty_Reasons": ["MARKET_STRESS_HALT"],
                "Missing_Data_Fields": [],
                "Doctrine_Source": "System: Market Stress Guard",
                "Required_Conditions_Met": False
            }

        # Structural_Data_Complete reflects PriceStructure swing metrics (swing_hh_count,
        # break_of_structure, etc.).  These are supplementary context — NOT required for
        # core doctrine evaluation.  The doctrine engine uses equity integrity, Greeks,
        # IV/HV, DTE, and cost basis, all of which are independent of swing metrics.
        # Blocking doctrine evaluation here caused ALL positions to get HOLD with
        # "Structure unresolved" (PriceStructure metrics never populated).
        if not row.get('Structural_Data_Complete', True):
            logger.debug(f"PriceStructure incomplete for {row.get('Symbol')} — doctrine will evaluate with available data")

        # RAG: Epistemic Strictness
        # IV is only required for options. Stocks do not have IV.
        is_option_leg = row.get('AssetType') == 'OPTION'
        if is_option_leg and pd.isna(row.get('IV_30D')):
            logger.debug(f"DEBUG: IV_AUTHORITY_MISSING triggered for LegID: {row.get('LegID')}, AssetType: {row.get('AssetType')}, IV_30D: {row.get('IV_30D')}")
            reasons.append(REASON_IV_AUTHORITY_MISSING)
            missing_fields.append('IV_30D')
        # Removed HV_DATA_MISSING check as market stress is now handled globally

        # Check for chart state completeness (distilled from raw indicators by ChartStateEngine)
        # Raw indicator columns (RSI_14, ADX_14, etc.) are not required — chart states are sufficient.
        required_chart_states = ['TrendIntegrity_State', 'VolatilityState_State', 'AssignmentRisk_State']
        chart_states_missing = [s for s in required_chart_states if pd.isna(row.get(s)) or str(row.get(s, '')).strip() == '']
        if chart_states_missing:
            reasons.append(f"MISSING_CHART_STATES: {', '.join(chart_states_missing)}")
            missing_fields.extend(chart_states_missing)

        if strategy == 'BUY_WRITE':
            if pd.isna(row.get('Underlying_Price_Entry')):
                reasons.append(REASON_STOCK_LEG_NOT_AVAILABLE)
                missing_fields.append('Underlying_Price_Entry')
        
        if reasons:
            return {
                "Action": "HOLD",
                "Urgency": "LOW",
                "Rationale": f"Action refused due to uncertainty: {', '.join(reasons)}",
                "Decision_State": STATE_UNCERTAIN,
                "Uncertainty_Reasons": reasons,
                "Missing_Data_Fields": missing_fields,
                "Doctrine_Source": "System: Uncertainty Guard",
                "Required_Conditions_Met": False
            }
        return None
    
    guards = df.apply(apply_guards, axis=1)
    
    def evaluate_with_guard(row):
        # Pre-doctrine data integrity gate: if position is DATA_BLOCKED,
        # short-circuit to governance state. No doctrine evaluation.
        _pre_flag = str(row.get('Pre_Doctrine_Flag', '') or '').upper()
        if _pre_flag in ('DATA_BLOCKED', 'PRICE_STALE', 'GREEKS_MISSING'):
            _pre_detail = str(row.get('Pre_Doctrine_Detail', '') or '')
            return {
                "Action": "HOLD",
                "Urgency": "LOW",
                "Decision_State": STATE_BLOCKED_GOVERNANCE,
                "Doctrine_Source": "System: Data Integrity Gate",
                "Resolution_Method": "GOVERNANCE_BLOCK",
                "Rationale": (
                    f"DATA_BLOCKED: {_pre_detail}. "
                    f"No doctrine evaluation possible. "
                    f"Position held pending data refresh."
                ),
                "Missing_Data_Fields": [_pre_detail],
                "Required_Conditions_Met": False,
            }

        # ── Write-off filter: skip doctrine for micro-positions ────────
        # Positions below $100 market value are noise — dead options,
        # fractional shares, cash sweeps.  Tag as WRITE_OFF so they
        # appear on the dashboard in a collapsed section but don't
        # clutter active doctrine recommendations.
        try:
            from core.management.cycle3.doctrine.thresholds import WRITEOFF_MIN_MARKET_VALUE
            from core.management.cycle3.doctrine.helpers import safe_row_float
            _wo_qty = abs(safe_row_float(row, 'Quantity', 'Qty'))
            _wo_price = safe_row_float(row, 'UL Last', 'Last', 'Spot')
            _wo_asset = str(row.get('AssetType', '') or '').upper()
            # Options: qty × price × 100; Stock: qty × price
            _wo_mult = 100.0 if _wo_asset in ('OPTION', 'CALL', 'PUT') else 1.0
            _wo_mkt_val = _wo_qty * _wo_price * _wo_mult
            _wo_ticker = str(row.get('Underlying_Ticker') or row.get('Symbol') or 'ticker')
            if _wo_mkt_val < WRITEOFF_MIN_MARKET_VALUE and _wo_qty > 0:
                logger.info(f"[WRITE_OFF] {_wo_ticker}: mkt_val=${_wo_mkt_val:,.0f} < ${WRITEOFF_MIN_MARKET_VALUE:,.0f}")
                return {
                    "Action": "HOLD",
                    "Urgency": "LOW",
                    "Decision_State": STATE_NEUTRAL_CONFIDENT,
                    "Doctrine_Source": "System: Write-Off Filter",
                    "Doctrine_State": "WRITE_OFF",
                    "Rationale": (
                        f"Micro-position: {_wo_ticker} — market value "
                        f"${_wo_mkt_val:,.0f} < ${WRITEOFF_MIN_MARKET_VALUE:,.0f} threshold. "
                        f"Position parked as write-off. No doctrine evaluation."
                    ),
                    "Required_Conditions_Met": False,
                }
        except Exception as _wo_exc:
            logger.warning(f"[WRITE_OFF] Exception for {row.get('Underlying_Ticker', '?')}: {_wo_exc}")
            pass  # Graceful fallback: evaluate normally

        guard_result = guards.loc[row.name]
        result = guard_result if guard_result else DoctrineAuthority.evaluate(row)

        # Prepend trade journey context to every rationale.
        # Reads Prior_* columns injected by run_all.py step 2.95.
        # Guards (HALT, data incomplete) are excluded — they fire before doctrine and
        # their rationale is system-generated, not trader-narrative.
        _action = result.get("Action", "")
        _decision_state = result.get("Decision_State", "")
        if _action not in ("HALT",) and _decision_state != STATE_BLOCKED_GOVERNANCE:
            # Journey note — cosmetic, non-blocking
            try:
                _ul = float(row.get("UL Last") or row.get("Last") or 0)
                _prior_action = str(row.get("Prior_Action") or "").strip().upper()
                _jnote = _build_journey_note(row, _action, _ul)
                if _jnote:
                    result["Rationale"] = _jnote + "\n" + result.get("Rationale", "")

                # Action-flip urgency escalation:
                # EXIT→HOLD means we had a profit-take signal but didn't act. The stock
                # retraced. This is not a clean HOLD — it needs fresh eyes with MEDIUM urgency.
                # (McMillan Ch.4: re-evaluate after a missed exit before the edge erodes further.)
                if _prior_action == "EXIT" and _action == "HOLD":
                    if result.get("Urgency", "LOW") == "LOW":
                        result["Urgency"] = "MEDIUM"
                        result["Rationale"] = result.get("Rationale", "") + (
                            " ⚠️ Urgency elevated: prior EXIT signal not acted on — "
                            "re-evaluate whether this pullback confirms hold or whether "
                            "the exit thesis still stands (McMillan Ch.4)."
                        )
            except Exception:
                pass  # journey note is cosmetic — safe to skip

            # Scan conflict detection — decision-critical (affects Urgency), must log errors
            try:
                _scan_bias = str(row.get("Scan_Current_Bias") or "").strip().upper()
                if _scan_bias and _scan_bias not in ("", "NONE", "NAN", "NEUTRAL"):
                    _pos_strat  = str(row.get("Strategy", "") or "").upper()
                    _pos_qty    = float(row.get("Quantity", 1) or 1)
                    _pos_is_bullish = (
                        "LONG_CALL"     in _pos_strat
                        or "LEAP"       in _pos_strat
                        or "BUY_WRITE"  in _pos_strat
                        or "COVERED_CALL" in _pos_strat
                        or "PMCC"       in _pos_strat
                        or ("LONG_CALL" not in _pos_strat and "LONG_PUT" not in _pos_strat
                            and _pos_qty > 0 and "CALL" in _pos_strat)
                    )
                    _pos_is_bearish = (
                        "LONG_PUT" in _pos_strat
                        or ("PUT" in _pos_strat and "CASH_SECURED" not in _pos_strat
                            and "SHORT_PUT" not in _pos_strat and _pos_qty > 0)
                    )

                    _conflict_note = ""
                    if _scan_bias == "MIXED":
                        _conflict_note = (
                            " 📡 **Scan note:** scan engine has BOTH bullish and bearish "
                            "candidates on this ticker simultaneously — cross-signal ambiguity. "
                            "Verify position direction aligns with your intended thesis before adding exposure."
                        )
                    elif _pos_is_bullish and _scan_bias == "BEARISH":
                        _conflict_note = (
                            " ⚡ **Scan conflict:** scan engine is currently **BEARISH** on "
                            f"{row.get('Underlying_Ticker', 'this ticker')} while this position is "
                            "long directional. The two systems disagree — the scan signal suggests "
                            "the setup that justified entry has reversed. "
                            "Verify thesis is still intact before holding "
                            "(McMillan Ch.4: don't hold a directional position against the trend)."
                        )
                        # Escalate HOLD to MEDIUM when scan conflicts — silent HOLD is dangerous here
                        if _action == "HOLD" and result.get("Urgency", "LOW") == "LOW":
                            result["Urgency"] = "MEDIUM"
                    elif _pos_is_bearish and _scan_bias == "BULLISH":
                        _conflict_note = (
                            " ⚡ **Scan conflict:** scan engine is currently **BULLISH** on "
                            f"{row.get('Underlying_Ticker', 'this ticker')} while this position is "
                            "bearish directional. The two systems disagree — the scan signal suggests "
                            "the bearish setup has reversed to bullish. "
                            "Verify put thesis is still intact "
                            "(McMillan Ch.4: don't hold a directional position against the trend)."
                        )
                        if _action == "HOLD" and result.get("Urgency", "LOW") == "LOW":
                            result["Urgency"] = "MEDIUM"

                    if _conflict_note:
                        result["Rationale"] = result.get("Rationale", "") + _conflict_note
                        result["Scan_Conflict"] = _scan_bias   # surface for display layer

            except Exception as _sc_err:
                logger.warning(f"Scan conflict detection failed for {row.get('Underlying_Ticker', '?')}: {_sc_err}")

        # ── Structural Decay Regime annotation ───────────────────────────────
        # Reads _Structural_Decay_Regime (computed vectorially above).
        # Non-blocking: never overrides Action, only annotates Signal_State
        # and appends to Rationale so the dashboard drift strip reflects it.
        try:
            _sdr = str(row.get('_Structural_Decay_Regime', 'NONE') or 'NONE')
            if _sdr in ('STRUCTURAL_DECAY', 'DECAY_RISK'):
                _net_t = float(row.get('_Ticker_Net_Theta', 0) or 0)
                _net_v = float(row.get('_Ticker_Net_Vega',  0) or 0)
                _iv_r  = pd.to_numeric(row.get('IV_ROC_3D'),   errors='coerce')
                _chop  = float(row.get('choppiness_index', 50) or 50)
                _adx   = float(row.get('adx_14', 25) or 25)
                _iv_gap= pd.to_numeric(row.get('IV_vs_HV_Gap'), errors='coerce')

                # Reconstruct the scaled IV threshold for display (mirrors _score_structural_decay)
                _ann_iv_now  = pd.to_numeric(row.get('IV_Now'), errors='coerce')
                _ann_iv_lvl  = float(_ann_iv_now) if pd.notna(_ann_iv_now) and _ann_iv_now > 0 else float('nan')
                if pd.notna(_ann_iv_lvl) and _ann_iv_lvl >= 5:
                    _ann_iv_lvl = _ann_iv_lvl / 100.0
                _ann_thresh  = max(0.08, _ann_iv_lvl * 0.20) if pd.notna(_ann_iv_lvl) else 0.08

                _decay_detail = (
                    f"θ/day={_net_t:+.1f}, ν={_net_v:+.0f}"
                    + (f", IV ROC 3D={_iv_r:+.2f} (thresh={_ann_thresh:.2f})" if pd.notna(_iv_r) else "")
                    + f", chop={_chop:.0f}, ADX={_adx:.0f}"
                    + (f", IV vs HV={_iv_gap:+.1%}" if pd.notna(_iv_gap) else "")
                )

                if _sdr == 'STRUCTURAL_DECAY':
                    # ── Directional Confirmation Branch ───────────────────────────────
                    # The structural decay regime was designed for STAGNANT long-vol
                    # positions (range-bound stock, falling IV, theta/vega bleeding with
                    # no directional offset). If direction IS working — stock moving toward
                    # the strike, delta growing in the thesis direction, momentum sustained —
                    # then theta cost is being offset by intrinsic gain and the bleed
                    # characterisation is wrong. Suppress the DEGRADED escalation.
                    #
                    # Thesis-aware directional check:
                    #   LONG_PUT: price must be falling (Price_Drift_Pct < -2%) AND
                    #             delta growing more negative (Delta_ROC_3D < -0.05, i.e.
                    #             option gaining sensitivity as stock moves toward strike) AND
                    #             momentum not stalling/reversing
                    #   LONG_CALL: mirror — price rising, delta growing more positive
                    #
                    # The Delta_ROC_3D signal is the decisive one: the scorer penalises
                    # abs(d_roc) < 0.08 as "no directional move". For a LONG_PUT with the
                    # stock falling, d_roc will be < -0.08 (delta going more negative = put
                    # gaining intrinsic). That same number suppresses the false DEGRADED here.
                    #
                    # Doctrine: McMillan Ch.4: "A long put that is working — stock declining,
                    #   delta increasing, momentum sustained — is not in decay. It is in
                    #   theta-offset directional gain. Decay only applies when there is NO
                    #   directional component to counterbalance the carry cost."
                    # ─────────────────────────────────────────────────────────────────────
                    _strat_sdr = str(row.get('Strategy', '') or '').upper()
                    _is_long_put_sdr  = any(s in _strat_sdr for s in ('LONG_PUT', 'BUY_PUT', 'LEAPS_PUT'))
                    _is_long_call_sdr = any(s in _strat_sdr for s in ('LONG_CALL', 'BUY_CALL', 'LEAPS_CALL'))

                    _price_drift_sdr = float(row.get('Price_Drift_Pct', 0) or 0)
                    _d_roc_sdr       = pd.to_numeric(row.get('Delta_ROC_3D'), errors='coerce')
                    _mom_sdr_raw     = str(row.get('MomentumVelocity_State', '') or '').split('.')[-1].upper()
                    _mom_sustained   = _mom_sdr_raw in ('TRENDING', 'SUSTAINED', 'ACCELERATING')

                    # Direction working: price moved ≥1σ toward strike AND delta growing
                    # in thesis direction AND momentum not stalling/reversing.
                    # Sigma-normalized: a 2% drift on AMD (HV=52%, z=0.91) is noise and
                    # should NOT suppress decay warnings. The same 2% on JNJ (HV=12%,
                    # z=2.65) is genuinely confirming and should suppress.
                    import math as _math_sdr
                    from core.management.cycle3.doctrine.thresholds import (
                        SIGMA_DAILY_VOL_FLOOR, SIGMA_DRIFT_Z_CONFIRMING,
                    )
                    _hv_sdr = float(row.get('HV_20D', 0) or 0) if pd.notna(row.get('HV_20D')) else 0.0
                    _hv_valid_sdr = _hv_sdr > 0
                    if _hv_valid_sdr:
                        _daily_sigma_sdr = max(_hv_sdr / _math_sdr.sqrt(252), SIGMA_DAILY_VOL_FLOOR)
                        _drift_z_sdr = _price_drift_sdr / _daily_sigma_sdr
                    else:
                        _drift_z_sdr = None  # HV missing — indeterminate

                    if _drift_z_sdr is None:
                        # HV unavailable: can't confirm direction is working.
                        # Don't suppress decay warning on incomplete data.
                        _direction_working = False
                    elif _is_long_put_sdr:
                        _drift_confirming = _drift_z_sdr <= -SIGMA_DRIFT_Z_CONFIRMING
                        _direction_working = (
                            _drift_confirming
                            and pd.notna(_d_roc_sdr) and _d_roc_sdr < -0.05  # put gaining sensitivity
                            and _mom_sustained                              # not stalling
                        )
                    elif _is_long_call_sdr:
                        _drift_confirming = _drift_z_sdr >= SIGMA_DRIFT_Z_CONFIRMING
                        _direction_working = (
                            _drift_confirming
                            and pd.notna(_d_roc_sdr) and _d_roc_sdr > 0.05   # call gaining sensitivity
                            and _mom_sustained                              # not stalling
                        )
                    else:
                        _direction_working = False

                    if _direction_working:
                        # Structural decay signals present but direction is offsetting.
                        # Downgrade to DECAY_RISK note (warn, don't escalate to DEGRADED).
                        _sigma_note_sdr = (f", drift_z={_drift_z_sdr:+.1f}σ"
                                           if _drift_z_sdr is not None else "")
                        _decay_note = (
                            f"  ⚠️ **Decay signals present but direction working** — "
                            f"stock moving toward strike ({_price_drift_sdr:+.1%} drift{_sigma_note_sdr}, "
                            f"Δ ROC 3D={float(_d_roc_sdr):+.2f}, {_mom_sdr_raw} momentum). "
                            f"Theta cost is being offset by intrinsic gain. "
                            f"Monitor: if momentum stalls or drift reverses, decay escalates. "
                            f"({_decay_detail})"
                        )
                        # Do NOT escalate Signal_State — direction is working
                    else:
                        _decay_note = (
                            f"  🔴 **Structural Decay Regime:** long-vol structure in "
                            f"chop + IV compression — bleeding theta AND vega simultaneously. "
                            f"Low realized movement + falling IV = silent bleed. "
                            f"({_decay_detail})  "
                            f"Review whether the vol expansion thesis still has a near-term catalyst."
                        )
                        # Escalate Signal_State to DEGRADED if not already VIOLATED
                        _cur_ss = result.get('Signal_State', 'VALID')
                        if _cur_ss not in ('VIOLATED', 'DEGRADED'):
                            result['Signal_State'] = 'DEGRADED'
                    result['Drift_Action'] = result.get('Drift_Action', 'NO_ACTION')
                else:  # DECAY_RISK
                    _decay_note = (
                        f"  ⚠️ **Decay Risk:** 2 structural decay signals converging "
                        f"(chop + IV compression or delta flatline). "
                        f"Not yet critical but monitor — if realized vol stays low "
                        f"and IV continues to compress, this becomes active bleed. "
                        f"({_decay_detail})"
                    )

                result['Rationale'] = result.get('Rationale', '') + _decay_note
                result['_Structural_Decay_Regime'] = _sdr
        except Exception as _sd_err:
            logger.debug(f"Structural decay annotation skipped for {row.get('Underlying_Ticker', '?')}: {_sd_err}")

        return result

    decisions = df.apply(evaluate_with_guard, axis=1)
    decision_df = pd.DataFrame(decisions.tolist(), index=df.index)

    # Preserve existing columns from df_signals that are not overwritten by decision_df
    # This includes PnL_Total, PnL_Unexplained, etc.
    for col in df_signals.columns:
        if col not in df.columns: # Only add if not already present (e.g., from StrategyResolver)
            df[col] = df_signals[col]

    for col in decision_df.columns:
        if col == 'Strategy': continue # Strategy is already handled
        df[col] = decision_df[col]

    df['RAG_Citation'] = df['Doctrine_Source']

    # ── Post-Doctrine Leg Coordination ─────────────────────────────────
    df = coordinate_multi_leg_actions(df)

    # ── CRISIS Confirmation Guard ────────────────────────────────────────
    # Addresses "Sunday EXIT CRITICAL" class of false triggers.
    # When CRISIS regime is active AND an EXIT recommendation is based mainly
    # on price drift (thesis INTACT, not assignment/time-decay), cap urgency
    # at HIGH instead of CRITICAL. Requires one extra confirmation cycle.
    if 'Market_Regime' in df.columns:
        _crisis_mask = (
            (df['Market_Regime'] == 'CRISIS')
            & (df.get('Action', pd.Series(dtype=str)) == 'EXIT')
            & (df.get('Urgency', pd.Series(dtype=str)) == 'CRITICAL')
        )
        if _crisis_mask.any():
            # Only cap if thesis is not degraded — price-drift-only EXIT
            for _idx in df[_crisis_mask].index:
                _rationale = str(df.at[_idx, 'Rationale'] if 'Rationale' in df.columns else '')
                _thesis_ok = (
                    'thesis' not in _rationale.lower()
                    or 'intact' in _rationale.lower()
                    or 'not degraded' in _rationale.lower()
                )
                _no_assignment = 'assignment' not in _rationale.lower()
                if _thesis_ok and _no_assignment:
                    df.at[_idx, 'Urgency'] = 'HIGH'
                    _old_rat = df.at[_idx, 'Rationale'] if 'Rationale' in df.columns else ''
                    df.at[_idx, 'Rationale'] = (
                        str(_old_rat) + ' [CRISIS guard: capped CRITICAL→HIGH, '
                        'thesis intact + price-drift-only — requires confirmation]'
                    )
                    logger.info(
                        f"[CRISISGuard] {df.at[_idx, 'Underlying_Ticker'] if 'Underlying_Ticker' in df.columns else '?'}: "
                        f"EXIT CRITICAL → HIGH (thesis intact, price-drift during CRISIS)"
                    )

    # NOTE: _apply_execution_readiness() is intentionally called in run_all.py
    # AFTER all post-processing (drift overrides, Action mutations, schema enforcement)
    # so it sees the final resolved Action column — not the pre-drift version.

    return df
