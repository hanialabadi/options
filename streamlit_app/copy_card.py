"""
Copy-card builder extracted from scan_view.py.

Pure function: (row, ctx, fmt) → plain-text string for clipboard.
No Streamlit imports, no DB, no file I/O.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional

import pandas as pd


def _safe(v: Any) -> str:
    """Stringify a value, returning '' for None/nan/NaN."""
    if v is None:
        return ''
    s = str(v)
    if s in ('nan', 'None', 'NaN', ''):
        return ''
    return s


def build_copy_card_text(
    row: pd.Series,
    ctx: Dict[str, Any],
    fmt_price: Callable = str,
    fmt_pct: Callable = str,
    fmt_float: Callable = lambda v, d=3: str(v),
    safe_get: Callable = None,
) -> str:
    """
    Build the full plain-text copy card for a single contract.

    Parameters
    ----------
    row : pd.Series
        The Step12 output row.
    ctx : dict
        Pre-computed values from the card rendering scope. Keys:
            conf_color, conf_band, gate_plain, ticker, strat_name, trade_bias,
            mid, bid, ask, strike, dte, opt_type, expiry, contract_sym,
            stock_price, net_chg_pct, rsi_val, adx_val, trend_st, ema_sig,
            hi52, lo52, pos52,
            is_income, is_buy_write, is_directional, is_volatility, _is_leap,
            breakeven, max_loss, cap_display,
            _em_pct_f, _em_dollar,
            chase_limit, chase_limit_label,
            entry_lo, entry_hi, prem_vs_fv, last_opt,
            liq_grade, liq_reason, spread_pct, oi,
            _badge_label, _now_score, _reason_str
    fmt_price, fmt_pct, fmt_float : callables
        Formatting helpers (injected from formatters.py).
    safe_get : callable or None
        Multi-key getter for row fields (injected _g). Falls back to row.get.
    """
    _g = safe_get if safe_get else lambda r, *keys, default=None: r.get(keys[0], default)

    # Shorthand for ctx access
    def c(key, default=None):
        return ctx.get(key, default)

    lines = []

    # ── Header ──
    lines.append(
        f"{c('conf_color')} {c('ticker')} — {c('strat_name')} — "
        f"{str(c('trade_bias', '')).title()} · Mid ${fmt_price(c('mid'))} · "
        f"{c('conf_band')} confidence"
    )
    lines.append(f"Gate: {c('gate_plain')}")

    # Regime × strategy fit + bucket
    try:
        _rsf = _safe(_g(row, 'Regime_Strategy_Fit')).upper()
        _rsn = _safe(_g(row, 'Regime_Strategy_Note'))
        _bucket = _safe(_g(row, 'Capital_Bucket'))
        _ssw = _safe(_g(row, 'Surface_Shape_Warning'))
        parts = []
        if _rsf and _rsf != 'FIT':
            parts.append(f"Regime fit: {_rsf}")
        if _rsn:
            parts.append(_rsn)
        if _bucket:
            parts.append(f"Bucket: {_bucket}")
        if _ssw:
            parts.append(f"Surface warning: {_ssw}")
        if parts:
            lines.append(" | ".join(parts))
    except Exception:
        pass

    # Calendar deferral + risk flags
    try:
        _def_ret = bool(_g(row, 'Calendar_Deferred_Return', default=False))
        _def_from = _safe(_g(row, 'Deferred_From_Date'))
        if _def_ret and _def_from:
            lines.append(f"DEFERRED RETURN — originally deferred {_def_from} (Friday theta bleed), calendar clear today")
        _cal_flag = _safe(_g(row, 'Calendar_Risk_Flag')).upper()
        _cal_note = _safe(_g(row, 'Calendar_Risk_Note'))
        if _cal_flag == 'HIGH_BLEED':
            lines.append(f"Calendar risk — pre-holiday long premium: {_cal_note}")
        elif _cal_flag == 'ELEVATED_BLEED':
            lines.append(f"Calendar note: {_cal_note}")
        elif _cal_flag == 'PRE_HOLIDAY_EDGE':
            lines.append(f"Calendar edge — pre-holiday income entry: {_cal_note}")
        elif _cal_flag == 'ADVANTAGEOUS' and _cal_note:
            lines.append(f"Calendar: {_cal_note}")
    except Exception:
        pass
    lines.append("")

    # ── Stock context ──
    lines.append("Stock Price")
    net_chg_pct = c('net_chg_pct')
    arrow = "▲" if (net_chg_pct or 0) >= 0 else "▼"
    lines.append(f"${fmt_price(c('stock_price'))}")
    lines.append(f"{arrow} {fmt_pct(net_chg_pct)}")
    lines.append("RSI")
    lines.append(f"{fmt_float(c('rsi_val'), 1)}")
    lines.append(f"ADX {fmt_float(c('adx_val'), 1)}")
    lines.append("")
    lines.append("Trend / EMA")
    lines.append(f"{c('trend_st')} / {c('ema_sig')}")
    pos52 = c('pos52')
    pos_str = fmt_pct(pos52, 0) if pos52 else "—"
    lines.append("52W Position")
    lines.append(pos_str)
    lines.append(f"H {fmt_price(c('hi52'))} / L {fmt_price(c('lo52'))}")

    # Institutional signals
    try:
        ms = _g(row, 'Market_Structure')
        wb = _g(row, 'Weekly_Trend_Bias')
        obv = _g(row, 'OBV_Slope')
        rsid = _g(row, 'RSI_Divergence')
        macdd = _g(row, 'MACD_Divergence')
        ksq = _g(row, 'Keltner_Squeeze_On')
        kfr = _g(row, 'Keltner_Squeeze_Fired')
        rspy = _g(row, 'RS_vs_SPY_20d')
        sig_parts = []
        if _safe(ms):
            sig_parts.append(f"Structure: {ms}")
        if _safe(wb):
            sig_parts.append(f"Weekly: {wb}")
        if _safe(obv) and _safe(obv) not in ('0', '0.0'):
            sig_parts.append(f"OBV: {fmt_float(obv, 2)}")
        if _safe(rsid) and _safe(rsid) != 'NONE':
            sig_parts.append(f"RSI Div: {rsid}")
        if _safe(macdd) and _safe(macdd) != 'NONE':
            sig_parts.append(f"MACD Div: {macdd}")
        if str(kfr).upper() in ('TRUE', '1', 'YES'):
            sig_parts.append("Squeeze FIRED")
        elif str(ksq).upper() in ('TRUE', '1', 'YES'):
            sig_parts.append("Squeeze ON")
        if _safe(rspy):
            try:
                sig_parts.append(f"RS vs SPY: {float(rspy):+.1f}%")
            except (ValueError, TypeError):
                pass
        if sig_parts:
            lines.append("Signals: " + " | ".join(sig_parts))
    except Exception:
        pass
    lines.append("")

    # ── Contract ──
    lines.append("📋 Contract")
    lines.append("Symbol")
    lines.append(f"{c('contract_sym')}")
    lines.append("Expiration")
    lines.append(f"{c('expiry')}")
    lines.append("Strike / Type")
    _cc_strike = c('strike')
    _cc_strike_str = str(_cc_strike) if _cc_strike and '$' in str(_cc_strike) else fmt_price(_cc_strike)
    lines.append(f"{_cc_strike_str} {c('opt_type')}")
    lines.append("DTE")
    dte = c('dte')
    lines.append(f"{int(float(dte))} days" if dte else "—")

    # Now Score
    try:
        lines.append(f"{c('_badge_label')}  score {c('_now_score')} · {c('_reason_str')}")
    except Exception:
        pass

    # ── Entry Pricing ──
    mid = c('mid')
    bid = c('bid')
    ask = c('ask')
    is_income = c('is_income')
    is_buy_write = c('is_buy_write')
    is_directional = c('is_directional')
    is_volatility = c('is_volatility')
    _is_leap = c('_is_leap')
    opt_type = c('opt_type')
    stock_price = c('stock_price')
    strike = c('strike')

    lines.append("💵 Entry Pricing")
    lines.append("Bid / Ask")
    lines.append(f"{fmt_price(bid)} / {fmt_price(ask)}")
    lines.append(f"Spread: {fmt_pct(c('spread_pct'))}")
    lines.append("")
    lines.append("Mid (target entry)")
    lines.append(f"${fmt_price(mid)}")

    # Last trade divergence
    try:
        last_opt = c('last_opt')
        mid_lt = float(mid) if mid else None
        last_lt = float(last_opt) if last_opt else None
        if last_lt and mid_lt and abs(last_lt - mid_lt) / mid_lt > 0.20:
            lines.append(f"Last trade: ${fmt_price(last_opt)} — stale print, use mid")
        elif last_lt and mid_lt and abs(last_lt - mid_lt) / mid_lt > 0.03:
            div_pct = abs(last_lt - mid_lt) / mid_lt * 100
            lines.append(f"Last trade: ${fmt_price(last_opt)} — {div_pct:.1f}% from mid")
        else:
            lines.append(f"Last trade: ${fmt_price(last_opt)}")
    except Exception:
        pass
    lines.append("")

    # BS fair-value band
    entry_lo = c('entry_lo')
    entry_hi = c('entry_hi')
    prem_vs_fv = c('prem_vs_fv')
    try:
        lines.append("BS Fair-value band")
        lines.append(f"{fmt_price(entry_lo)} – {fmt_price(entry_hi)}")
        if prem_vs_fv:
            pvf = float(prem_vs_fv)
            if is_income or is_buy_write:
                if pvf > 0:
                    lines.append(f"✅ Selling {pvf:.1f}% above BS fair value")
                elif pvf < 0:
                    lines.append(f"🔴 Selling {abs(pvf):.1f}% below BS fair value")
                else:
                    lines.append("✅ At BS fair value")
            else:
                # Deep ITM check
                deep_itm = False
                try:
                    sp = float(stock_price or 0)
                    strk = float(strike or 0)
                    hi_f = float(entry_hi or 0)
                    intrinsic = (
                        max(0.0, strk - sp) if opt_type == 'PUT'
                        else max(0.0, sp - strk)
                    )
                    deep_itm = intrinsic > 0 and hi_f > 0 and intrinsic >= hi_f * 0.85
                except Exception:
                    pass
                if deep_itm:
                    tv = max(0.0, float(mid or 0) - intrinsic)
                    lines.append(
                        f"ℹ️ Deep ITM: intrinsic ${intrinsic:.2f}, "
                        f"time value ${tv:.2f}"
                    )
                elif pvf < 0:
                    lines.append(f"✅ Buying {abs(pvf):.1f}% below BS fair value")
                elif pvf > 0:
                    lines.append(f"⚠️ Paying {pvf:.1f}% above BS fair value")
                else:
                    lines.append("✅ At BS fair value")
    except Exception:
        pass
    lines.append("")

    # Chase limit (directional only)
    try:
        chase_limit = c('chase_limit')
        chase_limit_label = c('chase_limit_label')
        if chase_limit and chase_limit_label:
            lines.append(f"{chase_limit_label}: ${chase_limit:.2f}")
    except Exception:
        pass

    # Income: you receive / Directional: you pay
    if is_income or is_buy_write:
        lines.append("💰 You receive")
        lines.append(f"${fmt_price(mid)}")
        lines.append("Sell at mid or better (higher = more premium)")
    else:
        lines.append("💰 You pay")
        lines.append(f"${fmt_price(mid)}")
        lines.append("Buy at mid or better (lower = cheaper entry)")
    lines.append("")
    lines.append(f"Liquidity: {c('liq_grade')}{' — ' + str(c('liq_reason')) if c('liq_reason') else ''}")
    lines.append("")
    oi = c('oi')
    lines.append(f"OI: {int(float(oi)):,}" if oi and str(oi) not in ('nan', 'None', '') else "OI: —")
    lines.append("")

    # ── GTC Exit Rules ──
    try:
        mid_f = float(mid)
        if is_income or is_buy_write:
            profit_tgt = mid_f * 0.50
            stop_loss = mid_f * 2.0
            lines.append("🎯 GTC Exit Rules (Good-Till-Cancelled)")
            lines.append("Profit target: +50% Buy back at")
            lines.append(f"{profit_tgt:.2f}")
            lines.append(f"(+{profit_tgt * 100:.0f}/contract)")
            lines.append("")
            lines.append("Stop loss: –200% Buy back at ")
            lines.append(f"{stop_loss:.2f}")
            lines.append(f"(−{(stop_loss - mid_f) * 100:.0f}/contract)")
        else:
            profit_tgt = mid_f * 2.0
            stop_loss = mid_f * 0.50
            lines.append("🎯 GTC Exit Rules (Good-Till-Cancelled)")
            lines.append("Profit target: +100% Sell at")
            lines.append(f"{profit_tgt:.2f}")
            lines.append(f"(+{(profit_tgt - mid_f) * 100:.0f}/contract)")
            lines.append("")
            lines.append("Stop loss: –50% Sell at")
            lines.append(f"{stop_loss:.2f}")
            lines.append(f"(−{(mid_f - stop_loss) * 100:.0f}/contract)")
        lines.append("")
        if _is_leap:
            lines.append("Time stop: DTE ≤ 90 Roll or exit 3 months before expiry")
        elif is_buy_write:
            lines.append("Time stop: DTE ≤ 14 Close or roll the call")
        else:
            lines.append("Time stop: DTE ≤ 14 Exit regardless of P&L")
        lines.append("")
    except Exception:
        pass

    # ── Greeks ──
    lines.append("🔢 Greeks")
    try:
        d_v = _g(row, 'Delta')
        g_v = _g(row, 'Gamma')
        v_v = _g(row, 'Vega')
        t_v = _g(row, 'Theta')
        iv_c = _g(row, 'Implied_Volatility')

        # Buy-write combined delta
        if is_buy_write and d_v:
            try:
                call_d = float(d_v)
                comb_d = 1.0 - call_d
                lines.append(f"Delta (Δ): {call_d:.3f} call | Combined Δ ≈ {comb_d:.3f} (stock 1.0 − call {call_d:.3f})")
            except Exception:
                lines.append(f"Delta (Δ): {float(d_v):.3f}" if d_v else "Delta (Δ): —")
        else:
            lines.append(f"Delta (Δ): {float(d_v):.3f}" if d_v else "Delta (Δ): —")

        # Gamma + practical impact
        if g_v:
            try:
                gf = float(g_v)
                if _is_leap and gf < 0.02:
                    ds10 = gf * 10
                    lines.append(f"Gamma (Γ): {gf:.3f} — LEAP: $10 move shifts Δ by {ds10:.2f}")
                elif _is_leap:
                    ds10 = gf * 10
                    lines.append(f"Gamma (Γ): {gf:.3f} — $10 move shifts Δ by {ds10:.2f}")
                else:
                    ds5 = gf * 5
                    lines.append(f"Gamma (Γ): {gf:.3f} — $5 move shifts Δ by {ds5:.2f}")
            except Exception:
                lines.append(f"Gamma (Γ): {float(g_v):.3f}")
        else:
            lines.append("Gamma (Γ): —")

        lines.append(f"Vega (V): {float(v_v):.3f}" if v_v else "Vega (V): —")

        # Theta + practical interpretation
        if t_v:
            try:
                tf = float(t_v)
                mid_th = float(mid) if mid else None
                if (is_income or is_buy_write) and mid_th and mid_th > 0:
                    th_pct = abs(tf) / mid_th * 100
                    lines.append(f"Theta (Θ): {tf:.3f} — earn {th_pct:.1f}%/day of premium")
                elif mid_th and mid_th > 0 and tf < 0:
                    th_pct = abs(tf) / mid_th * 100
                    if _is_leap and th_pct < 0.1:
                        lines.append(f"Theta (Θ): {tf:.3f} — {th_pct:.2f}%/day (negligible for LEAP)")
                    else:
                        lines.append(f"Theta (Θ): {tf:.3f} — {th_pct:.1f}%/day of premium")
                else:
                    lines.append(f"Theta (Θ): {tf:.3f}")
            except Exception:
                lines.append(f"Theta (Θ): {float(t_v):.3f}")
        else:
            lines.append("Theta (Θ): —")

        lines.append(f"Contract IV: {float(iv_c):.1f}%" if iv_c else "Contract IV: —")
    except Exception:
        pass
    lines.append("")

    # ── Risk Profile ──
    lines.append("⚠️ Risk Profile")
    max_loss = c('max_loss')
    breakeven = c('breakeven')
    cap_display = c('cap_display')
    _em_pct_f = c('_em_pct_f')
    _em_dollar = c('_em_dollar')
    try:
        if max_loss:
            lines.append("Max Loss (1 contract)")
            if is_income:
                lines.append("Opportunity cost")
            else:
                lines.append(f"${max_loss:,.0f}")
        if breakeven:
            lines.append(f"Breakeven at expiry: ${float(breakeven):.2f}")
            # ITM / cushion analysis
            try:
                sp_be = float(stock_price or 0)
                be_val = float(breakeven)
                if opt_type == 'PUT' and sp_be > 0 and sp_be < be_val:
                    cush = be_val - sp_be
                    cush_pct = cush / sp_be * 100
                    lines.append(f"Already profitable at entry — stock ${sp_be:.2f} is ${cush:.2f} ({cush_pct:.1f}%) below breakeven")
                elif opt_type == 'PUT' and sp_be > 0:
                    gap = sp_be - be_val
                    gap_pct = gap / sp_be * 100
                    lines.append(f"Gap to breakeven: ${gap:.2f} ({gap_pct:.1f}% decline needed)")
                elif opt_type == 'CALL' and sp_be > 0 and sp_be > be_val:
                    cush = sp_be - be_val
                    cush_pct = cush / sp_be * 100
                    lines.append(f"Already profitable at entry — stock ${sp_be:.2f} is ${cush:.2f} ({cush_pct:.1f}%) above breakeven")
                elif opt_type == 'CALL' and sp_be > 0:
                    gap = be_val - sp_be
                    gap_pct = gap / sp_be * 100
                    lines.append(f"Gap to breakeven: ${gap:.2f} ({gap_pct:.1f}% rise needed)")
            except Exception:
                pass
        # Capital + buy-write net capital
        if is_buy_write and cap_display is not None:
            try:
                net_outlay = float(cap_display) - float(mid) * 100
                lines.append(f"Net Capital (1 lot): ${net_outlay:,.0f} (stock ${float(cap_display):,.0f} − premium ${float(mid)*100:,.0f})")
            except Exception:
                lines.append(f"Capital Required: ${float(cap_display):,.0f}")
        elif cap_display:
            lines.append(f"Capital Required: ${float(cap_display):,.0f}")
        # Expected Move + coverage ratio
        if _em_pct_f is not None:
            em_cc = f"Expected Move (1σ): {_em_pct_f:.1f}%"
            if _em_dollar is not None:
                em_cc += f" (${_em_dollar:,.0f})"
            try:
                sp_em = float(stock_price)
                be_em = float(breakeven)
                be_gap = abs(sp_em - be_em)
                be_gap_pct = be_gap / sp_em * 100 if sp_em > 0 else 0
                if be_gap_pct > 0:
                    cov = _em_pct_f / be_gap_pct
                    em_cc += f" | BE gap {be_gap_pct:.1f}% — EM covers {cov:.1f}x breakeven"
            except Exception:
                pass
            lines.append(em_cc)
    except Exception:
        pass
    lines.append("")

    # ── Volatility Context ──
    lines.append("📈 Volatility Context")
    try:
        iv30 = _g(row, 'iv_30d')
        hv30 = _g(row, 'HV30')
        gap_c = _g(row, 'IVHV_gap_30D')
        ivr = _g(row, 'IV_Rank_20D')
        ss = _g(row, 'Surface_Shape')
        ivm = _g(row, 'IV_Maturity_State')
        ivml = _g(row, 'IV_Maturity_Level')
        ihc = _g(row, 'IV_History_Count')

        lines.append("IV 30D / HV 30D")
        lines.append(f"{float(iv30):.1f}% / {float(hv30):.1f}%" if iv30 and hv30 else "—")
        if gap_c:
            lines.append(f"Gap: {'+' if float(gap_c) > 0 else ''}{float(gap_c):.1f}%")
        lines.append("")
        lines.append("IV Rank")
        lines.append(f"{float(ivr):.1f}" if ivr else "—")
        lines.append("")
        lines.append("Surface Shape")
        lines.append(f"{ss}" if ss else "—")

        # Surface shape interpretation (strategy-aware)
        try:
            ss_upper = str(ss or '').upper()
            if is_income or is_buy_write:
                shape_notes = {'CONTANGO': "Normal — favours income sellers", 'INVERTED': "Near-term IV elevated — assignment risk", 'FLAT': "No term structure edge"}
            elif _is_leap:
                shape_notes = {'CONTANGO': "Normal — fair long-dated vol", 'INVERTED': "LEAP buyers benefit — buying cheaper long-dated vol", 'FLAT': "No term structure edge"}
            else:
                shape_notes = {'CONTANGO': "Normal — no structural disadvantage", 'INVERTED': "Near-term IV elevated — pays off if move is fast", 'FLAT': "No term structure edge"}
            sn = shape_notes.get(ss_upper, '')
            if sn:
                lines.append(f"  ({sn})")
        except Exception:
            pass
        lines.append("")
        lines.append("IV Maturity")
        lines.append(f"Level {int(float(ivml))} ({int(float(ihc))}d collected)" if ivml else "—")
    except Exception:
        pass
    lines.append("")

    # ── Thesis ──
    lines.append("🧠 Thesis & Signal Reference")
    try:
        thesis = _g(row, 'thesis')
        theory = _g(row, 'Theory_Source')
        regime_ctx = _g(row, 'Regime_Context')
        iv_ctx = _g(row, 'IV_Context')
        chart_r = _g(row, 'Chart_Regime')
        ema_s = _g(row, 'Chart_EMA_Signal')
        sma20 = _g(row, 'SMA20')
        sma50 = _g(row, 'SMA50')
        macd = _g(row, 'MACD')
        atr = _g(row, 'Atr_Pct')
        dir_b = _g(row, 'directional_bias')
        str_b = _g(row, 'structure_bias')
        tim_q = _g(row, 'timing_quality')
        mom_t = _g(row, 'momentum_tag')
        comp_t = _g(row, 'compression_tag')
        ent_t = _g(row, 'entry_timing_context')
        conf_sc = _g(row, 'Confidence')

        if thesis:
            lines.append(f"{thesis}")
        if theory:
            lines.append(f"Theory source: {theory}")
        lines.append("")
        lines.append("Price Structure")
        lines.append(f"Chart regime: {chart_r}")
        lines.append(f"EMA signal: {ema_s}")
        lines.append(f"SMA20: {fmt_price(sma20)}  |  SMA50: {fmt_price(sma50)}")
        lines.append(f"MACD: {fmt_float(macd, 2)} | ATR: {fmt_pct(atr)}")
        lines.append("")
        lines.append("Execution Context")
        lines.append(f"Directional bias: {dir_b}")
        lines.append(f"Structure: {str_b}")
        lines.append(f"Timing quality: {tim_q}")
        lines.append(f"Momentum: {mom_t} | Compression: {comp_t}")
        lines.append(f"Entry context: {ent_t}")
        lines.append("")
        lines.append("IV & Regime")
        lines.append(f"Regime context: {regime_ctx}")
        lines.append(f"IV context: {iv_ctx}")
        lines.append(f"System confidence: {conf_sc}/100" if conf_sc else "Confidence: —")
    except Exception:
        pass

    # ── Monte Carlo ──
    try:
        mc_p10 = row.get('MC_P10_Loss')
        mc_p50 = row.get('MC_P50_Outcome')
        mc_p90 = row.get('MC_P90_Gain')
        mc_win = row.get('MC_Win_Probability')
        mc_cvar = row.get('MC_CVaR')
        mc_maxc = row.get('MC_Max_Contracts')
        mc_ratio = row.get('MC_CVaR_P10_Ratio')
        mc_paths = row.get('MC_Paths_Used')
        mc_note = str(row.get('MC_Sizing_Note') or '')
        mc_asgn = row.get('MC_Assign_Prob')
        mc_ran = (
            mc_p10 is not None
            and str(mc_p10) not in ('nan', 'None', '')
            and int(float(mc_paths or 0)) > 0
        )
        if mc_ran:
            mc_vsrc = (mc_note.split('[')[1].split(']')[0] if '[' in mc_note and ']' in mc_note else 'HV')
            lines.append("")
            # Status-aware MC header
            mc_exec = _safe(_g(row, 'Execution_Status')).upper() or 'READY'
            if mc_exec == 'READY':
                lines.append("🎲 Position Sizing — Monte Carlo")
            elif mc_exec in ('AWAIT_CONFIRMATION', 'BLOCKED'):
                lines.append("📊 Thesis Evaluation — Monte Carlo")
            elif mc_exec == 'WAIT_FOR_FILL':
                lines.append("⚠️ Risk Preview — Monte Carlo")
            else:
                lines.append("🎲 Monte Carlo")
            p10f = float(mc_p10)
            p50f = float(mc_p50)
            p90f = float(mc_p90)
            winf = float(mc_win)
            maxci = int(float(mc_maxc))
            cvarf = float(mc_cvar) if mc_cvar and str(mc_cvar) not in ('nan', 'None', '') else None
            ratiof = float(mc_ratio) if mc_ratio and str(mc_ratio) not in ('nan', 'None', '') else None
            lines.append(f"Max Contracts: {maxci}")
            if cvarf is not None:
                lines.append(f"CVaR (tail mean): ${cvarf:+,.0f}")
            if ratiof is not None:
                tail_label = "fat tail — size conservatively" if ratiof > 1.5 else "normal tail"
                lines.append(f"Tail Fatness: {ratiof:.2f}x ({tail_label})")
            lines.append(f"P10: ${p10f:+,.0f} | P50: ${p50f:+,.0f} | P90: ${p90f:+,.0f}")
            lines.append(f"Win Prob: {winf:.0%}")
            # Risk/Reward ratio
            if p10f != 0:
                rr = abs(p90f / p10f)
                lines.append(f"Reward/Risk: {rr:.1f}x (P90/|P10|)")
            if mc_asgn and str(mc_asgn) not in ('nan', 'None', ''):
                lines.append(f"Assignment Prob: {float(mc_asgn):.0%}")
            # Thesis verdict
            if mc_exec in ('AWAIT_CONFIRMATION', 'BLOCKED'):
                if winf >= 0.55 and p50f > 0 and p10f != 0 and abs(p90f / p10f) >= 1.5:
                    lines.append("Thesis: STRONG — favorable win rate, positive EV, good reward/risk")
                elif winf >= 0.45 and p50f >= 0:
                    lines.append("Thesis: MARGINAL — borderline EV, monitor but don't chase")
                else:
                    lines.append("Thesis: WEAK — unfavorable distribution")
            # MC note
            if mc_note and 'MC(' in mc_note:
                lines.append(mc_note)
            else:
                dte_mc = float(dte) if dte else 0
                lines.append(f"MC({int(float(mc_paths))}p, DTE={int(dte_mc)}): CVaR=${cvarf:+,.0f}|P10=${p10f:+,.0f}|P50=${p50f:+,.0f}|P90=${p90f:+,.0f} | Win={winf:.0%} | MaxC={maxci}")
    except Exception:
        pass

    # ── MC Variance Premium ──
    try:
        _vp_verdict = str(row.get('MC_VP_Verdict', '') or '')
        if _vp_verdict and _vp_verdict not in ('SKIP', 'nan', 'None', ''):
            lines.append("")
            _vp_score = row.get('MC_VP_Score')
            _vp_fair = row.get('MC_VP_Premium_Fair')
            _vp_edge = row.get('MC_VP_Edge')
            _vp_note = str(row.get('MC_VP_Note', '') or '')
            lines.append(f"Variance Premium: {_vp_verdict}")
            _vp_parts = []
            if pd.notna(_vp_score):
                _vp_parts.append(f"Score={float(_vp_score):.2f}")
            if pd.notna(_vp_fair):
                _vp_parts.append(f"Fair=${float(_vp_fair):.2f}")
            if pd.notna(_vp_edge):
                _vp_parts.append(f"Edge=${float(_vp_edge):+,.0f}/contract")
            if _vp_parts:
                lines.append(" | ".join(_vp_parts))
            if _vp_note and _vp_note not in ('nan', 'None', 'MC_SKIP'):
                lines.append(_vp_note)
    except Exception:
        pass

    # ── MC Earnings Event ──
    try:
        _earn_verdict = str(row.get('MC_Earn_Verdict', '') or '')
        if _earn_verdict and _earn_verdict not in ('SKIP', 'nan', 'None', ''):
            lines.append("")
            lines.append(f"Earnings Event: {_earn_verdict.replace('_', ' ')}")
            _ev_hold = row.get('MC_Earn_EV_Hold')
            _ev_close = row.get('MC_Earn_EV_Close')
            _earn_edge = row.get('MC_Earn_Edge')
            _earn_pprof = row.get('MC_Earn_P_Profit')
            _earn_parts = []
            if pd.notna(_ev_hold):
                _earn_parts.append(f"EV(Hold)=${float(_ev_hold):+,.0f}")
            if pd.notna(_ev_close):
                _earn_parts.append(f"EV(Close)=${float(_ev_close):+,.0f}")
            if pd.notna(_earn_edge):
                _earn_parts.append(f"Edge=${float(_earn_edge):+,.0f}")
            if pd.notna(_earn_pprof):
                _earn_parts.append(f"P(Profit)={float(_earn_pprof):.0%}")
            if _earn_parts:
                lines.append(" | ".join(_earn_parts))
            _earn_note = str(row.get('MC_Earn_Note', '') or '')
            if _earn_note:
                lines.append(_earn_note)
    except Exception:
        pass

    # ── Action Priority ──
    try:
        _edge = _g(row, 'Trade_Edge_Score')
        if _edge and str(_edge) not in ('nan', 'None', ''):
            _edge_f = float(_edge)
            _aps_tier = 'PRIORITY' if _edge_f >= 75 else ('STAGE' if _edge_f >= 50 else ('WATCHLIST' if _edge_f >= 25 else 'PASS'))
            lines.append("")
            lines.append(f"Action Priority: {_aps_tier} (edge {_edge_f:.0f}/100)")
    except Exception:
        pass

    # ── Scores — strategy-aware ──
    try:
        lines.append("")
        if is_directional or _is_leap:
            dqs = _g(row, 'DQS_Score')
            dqs_st = _g(row, 'DQS_Status', default='')
            tqs = _g(row, 'TQS_Score')
            tqs_band = _g(row, 'TQS_Band', default='')
            if dqs and str(dqs) not in ('nan', 'None', ''):
                lines.append("🎯 DQS — Directional Quality Score")
                lines.append(f"{float(dqs):.0f}/100 — {dqs_st}")
                dqs_bd = _g(row, 'DQS_Breakdown', default='')
                if dqs_bd and str(dqs_bd) not in ('nan', 'None', ''):
                    lines.append(f"Components: {dqs_bd}")
            if tqs and str(tqs) not in ('nan', 'None', ''):
                lines.append("")
                lines.append("⏱ TQS — Timing Quality Score")
                lines.append(f"{float(tqs):.0f}/100 — {tqs_band}")
                tqs_bd = _g(row, 'TQS_Breakdown', default='')
                if tqs_bd and str(tqs_bd) not in ('nan', 'None', ''):
                    lines.append(f"Components: {tqs_bd}")
        elif is_income or is_buy_write:
            pcs = _g(row, 'PCS_Score_V2', 'PCS_Score')
            pcs_st = _g(row, 'PCS_Status', default='INACTIVE')
            if pcs and str(pcs) not in ('nan', 'None', ''):
                lines.append("🏆 PCS — Premium Collection Standard")
                lines.append(f"{float(pcs):.0f}/100 — {pcs_st}")
                pens = _g(row, 'PCS_Penalties', default='')
                if pens and str(pens) not in ('nan', 'None', ''):
                    lines.append(f"Penalties: {pens}")
        elif is_volatility:
            vol_sc = _g(row, 'Theory_Compliance_Score')
            if vol_sc and str(vol_sc) not in ('nan', 'None', ''):
                lines.append("📊 Vol Strategy Score")
                lines.append(f"{float(vol_sc):.0f}/100")

        # Interpreter score
        interp = _g(row, 'Interp_Score')
        interp_max = _g(row, 'Interp_Max')
        interp_st = _g(row, 'Interp_Status', default='')
        interp_fam = _g(row, 'Interp_Family', default='')
        interp_bd = _g(row, 'Interp_Breakdown', default='')
        interp_edge = _g(row, 'Interp_Vol_Edge', default='')
        if interp and str(interp) not in ('nan', 'None', ''):
            lines.append("")
            lines.append(f"📐 Strategy Interpreter ({interp_fam})")
            lines.append(f"{float(interp):.0f}/{float(interp_max):.0f} — {interp_st}")
            if interp_bd and str(interp_bd) not in ('nan', 'None', ''):
                lines.append(f"Components: {interp_bd}")
            if interp_edge and str(interp_edge) not in ('nan', 'None', '', 'NEUTRAL'):
                lines.append(f"Vol edge: {interp_edge}")
    except Exception:
        pass

    return "\n".join(lines)
