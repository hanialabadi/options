"""
Execution Readiness classification layer.
Extracted from engine.py lines 7497-7680.

Runs after doctrine + drift filter to classify each row as:
  EXECUTE_NOW / WAIT_FOR_WINDOW / STAGE_AND_RECHECK

Calendar-aware rules (Phase 2):
  - ROLL on Friday with DTE ≤ 14 → EXECUTE_NOW (don't carry expiring roll over weekend)
  - STAGE_AND_RECHECK on Friday with long premium → WAIT_FOR_WINDOW (no new exposure)
  - HOLD + short premium on Friday → annotate (weekend theta advantage)
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)

# Strategy classification for calendar awareness
_LONG_PREMIUM_STRATEGIES = {
    'LONG_PUT', 'LONG_CALL', 'LEAPS_PUT', 'LEAPS_CALL',
    'LONG_STRADDLE', 'LONG_STRANGLE', 'DEBIT_SPREAD',
}
_SHORT_PREMIUM_STRATEGIES = {
    'CSP', 'COVERED_CALL', 'BUY_WRITE', 'SHORT_PUT', 'SHORT_CALL',
    'CREDIT_SPREAD', 'IRON_CONDOR', 'SHORT_STRADDLE', 'SHORT_STRANGLE',
}

def _apply_execution_readiness(df: pd.DataFrame) -> pd.DataFrame:
    """
    Layer 2 — Execution Readiness (backend component).

    Runs after doctrine + drift filter to classify each row as:
      EXECUTE_NOW      — act on current run; structural urgency or risk override
      WAIT_FOR_WINDOW  — action is valid but execution conditions are poor;
                         wait for liquidity/spread to improve
      STAGE_AND_RECHECK — signal is marginal or conditions mixed;
                          pre-stage candidates, monitor next run

    Inputs used (all from pipeline data — no live chain call):
      Action, Urgency: from doctrine engine
      DTE: option time-to-expiry
      Delta: for gamma/convexity proximity detection
      Roll_Candidate_1: spread_pct from pre-staged candidates
      Earnings_Date: broker CSV earnings proximity
      IV_vs_HV_Gap: spread environment quality proxy

    Rules (priority order — first match wins):

    EXECUTE_NOW forced:
      • Action=EXIT (any urgency)          — thesis broken, delay compounds loss
      • Action=ROLL/TRIM + Urgency=CRITICAL — time-critical, don't wait
      • DTE ≤ 3                            — pin risk, theta zero, must act
      • |Delta| ≥ 0.70 (ITM deep)         — gamma dominance, rapid value decay
      • Earnings within 1 day             — IV event imminent

    WAIT_FOR_WINDOW:
      • Action=HOLD                        — no structural trigger, time is available
      • Action=ROLL_WAIT                   — doctrine already gated this
      • Spread ≥ 12% on Roll_Candidate_1  — execution cost destroys credit
      • IV_vs_HV_Gap ≤ -5 (IV crushed)   — vol too low to sell into for credit roll

    STAGE_AND_RECHECK (default for ROLL/TRIM with no forcing condition):
      • Action=ROLL + Urgency=LOW/MEDIUM  — valid but not urgent; pre-stage, wait for window
      • Spread 8–12% on candidate         — marginal execution, check later

    EXECUTE_NOW is the default for any unclassified ROLL/EXIT/TRIM with MEDIUM+ urgency.

    Passarelli Ch.6: "Decouple the decision to roll from the moment of execution.
    The decision is structural; the execution is tactical."
    McMillan Ch.3: "Never execute a roll into a wide spread — the credit is theoretical."
    """
    import json as _json

    # Calendar context — imported lazily so management engine works without scan_engine
    try:
        from scan_engine.calendar_context import get_calendar_context
        _cal_ctx = get_calendar_context()
        _is_friday = _cal_ctx.is_friday
        _is_pre_long_wk = _cal_ctx.is_pre_long_weekend
    except Exception:
        _is_friday = False
        _is_pre_long_wk = False

    def _readiness(row: pd.Series):
        action   = str(row.get('Action', '')  or '').upper()
        urgency  = str(row.get('Urgency', '') or '').upper()
        dte      = pd.to_numeric(row.get('DTE'), errors='coerce')
        delta    = abs(pd.to_numeric(row.get('Delta', 0), errors='coerce') or 0)

        # Strategy premium direction for calendar rules
        _strat = str(row.get('Strategy', '') or row.get('Strategy_Name', '') or '').upper()
        _is_long_prem = any(s in _strat for s in _LONG_PREMIUM_STRATEGIES)
        _is_short_prem = any(s in _strat for s in _SHORT_PREMIUM_STRATEGIES)

        # Parse spread from Roll_Candidate_1 if available
        spread_pct = None
        _rc1_raw = row.get('Roll_Candidate_1')
        if _rc1_raw and _rc1_raw not in ('', 'nan'):
            try:
                _rc1 = _json.loads(str(_rc1_raw)) if isinstance(_rc1_raw, str) else _rc1_raw
                if isinstance(_rc1, dict):
                    spread_pct = float(_rc1.get('spread_pct', 0) or 0) or None
            except Exception:
                pass

        iv_hv_gap = pd.to_numeric(row.get('IV_vs_HV_Gap'), errors='coerce')

        # Parse earnings proximity
        days_to_earn = None
        _earn_raw = row.get('Earnings_Date')
        if _earn_raw not in (None, '', 'nan', 'N/A') and not (
            isinstance(_earn_raw, float) and pd.isna(_earn_raw)
        ):
            try:
                _ed = pd.to_datetime(str(_earn_raw), errors='coerce')
                if pd.notna(_ed):
                    days_to_earn = (_ed.normalize() - pd.Timestamp.now().normalize()).days
            except Exception:
                pass

        reasons = []

        # ── EXECUTE_NOW forcing conditions ────────────────────────────────────
        if action == 'EXIT':
            return 'EXECUTE_NOW', 'EXIT action — thesis broken; delay compounds loss'

        if action in ('ROLL', 'TRIM', 'HALT') and urgency == 'CRITICAL':
            return 'EXECUTE_NOW', f'{action} + CRITICAL urgency — time-sensitive, act immediately'

        if pd.notna(dte) and dte <= 3:
            return 'EXECUTE_NOW', f'DTE={int(dte)}d — pin risk active, theta near zero; act today'

        if delta >= 0.70 and not _is_long_prem:
            # Income/short premium: deep ITM = assignment risk, extrinsic vanishing.
            # Long premium: deep ITM = position is winning, not a problem.
            return 'EXECUTE_NOW', (
                f'|Delta|={delta:.2f} ≥ 0.70 — deep ITM gamma dominance; '
                'intrinsic decaying, roll before extrinsic gone'
            )

        if days_to_earn is not None and 0 <= days_to_earn <= 1:
            return 'EXECUTE_NOW', (
                f'Earnings in {days_to_earn}d — IV event imminent; '
                'execute before vol crush or IV spike (Natenberg Ch.8)'
            )

        # ── Calendar-Aware Forcing: ROLL + Friday/pre-holiday + DTE ≤ 14 ────
        # Don't carry an expiring roll need over the weekend — Monday opens with
        # 2-3 fewer DTE and potential gap risk. Passarelli Ch.6.
        if (
            action == 'ROLL'
            and (_is_friday or _is_pre_long_wk)
            and pd.notna(dte) and dte <= 14
        ):
            _cal_label = 'pre-long-weekend' if _is_pre_long_wk else 'Friday'
            return 'EXECUTE_NOW', (
                f'ROLL + {_cal_label} + DTE={int(dte)}d ≤ 14 — '
                'execute before weekend; Monday opens with fewer DTE and gap risk '
                '(Passarelli Ch.6)'
            )

        # ── Market Regime Guards ──────────────────────────────────────────────
        # CRISIS regime: block new entries (STAGE → WAIT_FOR_WINDOW)
        _mkt_regime = str(row.get('Market_Regime', '') or '').upper()
        _mkt_term = str(row.get('Market_Term_Structure', '') or '').upper()
        if _mkt_regime == 'CRISIS' and action in ('STAGE', 'SCALE_UP'):
            return 'WAIT_FOR_WINDOW', (
                'CRISIS regime — blocking new entries; '
                'wait for regime to stabilize before opening positions'
            )
        # RISK_OFF + income ROLL: bump urgency context note
        if _mkt_regime == 'RISK_OFF' and action == 'ROLL' and _is_short_prem:
            reasons.append('RISK_OFF regime — income ROLL urgency elevated')
        # Backwardation + income roll: front-month IV premium elevated
        if _mkt_term == 'BACKWARDATION' and action == 'ROLL' and _is_short_prem:
            reasons.append('Backwardation — front-month IV premium elevated, roll costs higher')

        # ── WAIT_FOR_WINDOW conditions ────────────────────────────────────────
        # Calendar annotation: HOLD + short premium + Friday → enhanced note
        if (
            action in ('HOLD', 'HOLD_FOR_REVERSION')
            and (_is_friday or _is_pre_long_wk)
            and _is_short_prem
        ):
            _cal_label = 'pre-long-weekend' if _is_pre_long_wk else 'Friday'
            return 'WAIT_FOR_WINDOW', (
                f'HOLD — no structural trigger; {_cal_label} short premium position '
                'benefits from weekend theta decay (Passarelli Ch.6)'
            )

        if action in ('HOLD', 'HOLD_FOR_REVERSION'):
            if _is_long_prem:
                return 'WAIT_FOR_WINDOW', 'HOLD — no structural trigger; thesis monitoring continues, manage carry cost'
            return 'WAIT_FOR_WINDOW', 'HOLD — no structural trigger; collect theta, wait for setup'

        # LET_EXPIRE / ACCEPT_CALL_AWAY / ACCEPT_SHARE_ASSIGNMENT — passive postures,
        # wait for expiration or assignment to occur naturally.
        if action == 'LET_EXPIRE':
            return 'WAIT_FOR_WINDOW', 'LET_EXPIRE — call OTM, let it expire worthless; collect full premium'
        if action == 'ACCEPT_CALL_AWAY':
            return 'WAIT_FOR_WINDOW', 'ACCEPT_CALL_AWAY — call ITM, shares will be called away at strike; profitable assignment'
        if action == 'ACCEPT_SHARE_ASSIGNMENT':
            return 'WAIT_FOR_WINDOW', 'ACCEPT_SHARE_ASSIGNMENT — put ITM, shares will be assigned at strike; accept stock'

        # REVIEW — data stale or signal degraded; re-examine thesis before acting.
        if action == 'REVIEW':
            return 'STAGE_AND_RECHECK', 'REVIEW — signal degraded or data stale; re-examine thesis by next session'

        if action == 'ROLL_WAIT':
            return 'WAIT_FOR_WINDOW', 'ROLL_WAIT — doctrine gated this; conditions not yet met'

        if spread_pct is not None and spread_pct >= 12.0:
            reasons.append(f'spread={spread_pct:.1f}% ≥ 12% — credit theoretical at this width')

        if pd.notna(iv_hv_gap) and iv_hv_gap <= -5.0:
            reasons.append(f'IV/HV gap={iv_hv_gap:+.1f}pt — IV crushed vs realized; credit environment poor')

        if reasons and action in ('ROLL', 'TRIM'):
            return 'WAIT_FOR_WINDOW', '; '.join(reasons) + ' (McMillan Ch.3: wait for spread to tighten)'

        # ── STAGE_AND_RECHECK ─────────────────────────────────────────────────
        if action in ('ROLL', 'TRIM') and urgency in ('LOW', 'MEDIUM'):
            _stage_reason = f'{action} + {urgency} urgency — valid signal, not urgent'
            if spread_pct is not None and 8.0 <= spread_pct < 12.0:
                _stage_reason += f'; spread={spread_pct:.1f}% marginal — wait for tighter window'
            # Calendar override: don't STAGE long premium entry over weekend
            if (_is_friday or _is_pre_long_wk) and _is_long_prem:
                _cal_label = 'pre-long-weekend' if _is_pre_long_wk else 'Friday'
                return 'WAIT_FOR_WINDOW', (
                    _stage_reason + f'; {_cal_label} long premium — '
                    'no new exposure over weekend theta bleed (Natenberg Ch.11)'
                )
            return 'STAGE_AND_RECHECK', _stage_reason + ' (Passarelli Ch.6: decouple decision from execution)'

        # ── Default: EXECUTE_NOW for any remaining active action ─────────────
        return 'EXECUTE_NOW', f'{action} + {urgency} urgency — proceed in next good window'

    results = df.apply(_readiness, axis=1)
    df['Execution_Readiness']        = results.apply(lambda x: x[0])
    df['Execution_Readiness_Reason'] = results.apply(lambda x: x[1])

    # ── Scan Feedback Integration (Capital Survival Audit, Phase 4) ──────────
    # Scan_DQS_Score and Scan_Confidence are injected by run_all.py from the
    # latest Step12 output.  When the scan engine sees the current setup as LOW
    # quality (DQS < 50), it de-risks two specific management decisions:
    #   1. ROLL → HOLD: a low-quality entry setup means the roll target is also
    #      questionable; wait for setup to improve before extending exposure.
    #   2. Scale-up (HOLD with scale intent): DQS < 50 blocks scale-up entirely
    #      per Vince (f-fraction scaling requires same edge quality as original entry).
    # NEVER blocks EXIT — exits are driven by position state, not entry quality.
    # Doctrine: Chan (Quantitative Trading Ch.3), Vince (Mathematics of Money Mgmt).
    if 'Scan_DQS_Score' in df.columns:
        def _apply_scan_feedback(row):
            dqs_raw = row.get('Scan_DQS_Score')
            action  = str(row.get('Action', '') or '').upper()
            try:
                dqs = float(dqs_raw) if dqs_raw is not None and pd.notna(dqs_raw) else None
            except (TypeError, ValueError):
                dqs = None

            if dqs is None or dqs >= 50:
                return row  # Insufficient data or acceptable quality — no change

            # DQS < 50: weak scan quality for this ticker's current setup
            if action == 'ROLL':
                # Emergency gates bypass scan feedback — never downgrade a structural emergency
                _dte_sf = pd.to_numeric(row.get('DTE'), errors='coerce')
                _delta_sf = abs(pd.to_numeric(row.get('Delta', 0), errors='coerce') or 0)
                _urgency_sf = str(row.get('Urgency', '') or '').upper()
                if (pd.notna(_dte_sf) and _dte_sf < 7) or _delta_sf >= 0.70 or _urgency_sf == 'CRITICAL':
                    return row  # Emergency ROLL — do not override with scan feedback
                row = row.copy()
                row['Action']   = 'HOLD'
                row['Urgency']  = 'LOW'
                row['Rationale'] = (
                    f"[ScanFeedback] Scan_DQS={dqs:.0f}<50 — setup quality degraded. "
                    f"ROLL target is also weak. Hold, wait for DQS recovery. "
                    f"Original: {row.get('Rationale', '')} | "
                    f"Chan Ch.3: do not extend exposure into a low-edge environment."
                )
                row['Doctrine_Source'] = 'ScanFeedback_DQS_Low: ROLL→HOLD (Chan Ch.3)'
            return row

        df = df.apply(_apply_scan_feedback, axis=1)
        low_dqs_rolls = (
            (df.get('Scan_DQS_Score', pd.Series(dtype=float)).fillna(100) < 50) &
            (df.get('Action', pd.Series(dtype=str)) == 'HOLD') &
            (df.get('Doctrine_Source', pd.Series(dtype=str)).str.startswith('ScanFeedback', na=False))
        ).sum()
        if low_dqs_rolls > 0:
            logger.info(f"[ScanFeedback] {low_dqs_rolls} ROLL→HOLD overrides applied (Scan_DQS_Score < 50)")

    return df
