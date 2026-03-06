import pandas as pd
from ..base import ChartStateResult
from ..state_definitions import RecoveryQualityState


def compute_recovery_quality(row: pd.Series) -> ChartStateResult:
    """
    RecoveryQuality_State — distinguishes genuine structural recovery from dead-cat bounce.

    This gate addresses the fundamental doctrine problem: a 1–2 day price uptick after a
    heavy decline looks the same to simple ROC/RSI checks but is economically different.

    Passarelli Ch.6: "Don't adapt the roll strike to noise. Only adapt when structure changes."
    McMillan Ch.3: "A bounce in a broken trend is not a recovery — it is a trap."

    States:
        STRUCTURAL_RECOVERY — regime has genuinely shifted upward; adaptation is rational.
            Requires ALL of: consecutive higher lows, break above prior swing high,
            positive momentum confirmed on 5 AND 10-day basis, trend integrity restoring,
            buyer dominance, EMA20 turning positive.

        DEAD_CAT_BOUNCE — 1–2 day uptick in a still-broken downtrend.
            Signals: short-term ROC positive but 10-day still negative (or barely positive),
            swing structure still shows lower highs, trend integrity WEAK/EXHAUSTED,
            EMA20 slope flat or negative.

        STILL_DECLINING — no bounce at all; trend continuing down.

        NOT_IN_RECOVERY — position is not under downside pressure (drift > -5%),
            so the classification is irrelevant.

        UNKNOWN — insufficient primitives to classify.

    Primitives used (all available from compute_primitives.py):
        drift_from_net      — position drift from net cost (if available) or sma_distance_pct
        roc_5               — 5-day rate of change
        roc_10              — 10-day rate of change
        swing_hl_count      — consecutive higher lows in last 20 bars
        swing_lh_count      — lower highs count (downtrend structure)
        swing_ll_count      — lower lows count
        break_of_structure  — price broke 20-bar high (structural breakout)
        ema20_slope         — EMA20 slope (positive = EMA turning up)
        TrendIntegrity_State
        DirectionalBalance_State
        MomentumVelocity_State
    """
    def _sn(col):
        v = row.get(col, '') or ''
        return (getattr(v, 'value', None) or str(v).split('.')[-1]).upper()

    # Read primitives
    roc_5  = row.get("roc_5")
    roc_10 = row.get("roc_10")
    hl     = row.get("swing_hl_count")
    lh     = row.get("swing_lh_count")
    ll     = row.get("swing_ll_count")
    bos    = row.get("break_of_structure")
    ema20s = row.get("ema20_slope")

    # Drift — prefer Net_Cost_Basis drift if available, else SMA distance as proxy
    drift_raw = row.get("drift_from_net") or row.get("sma_distance_pct")

    # Check for missing critical primitives
    critical = [roc_5, roc_10, hl, lh, ll, bos, ema20s]
    if any(v is None or (isinstance(v, float) and pd.isna(v)) for v in critical):
        return ChartStateResult(
            state=RecoveryQualityState.UNKNOWN,
            raw_metrics={},
            resolution_reason="MISSING_PRIMITIVES",
            data_complete=False,
        )

    roc_5  = float(roc_5)
    roc_10 = float(roc_10)
    hl     = int(hl)
    lh     = int(lh)
    ll     = int(ll)
    bos    = bool(bos)
    ema20s = float(ema20s)
    drift  = float(drift_raw) if (drift_raw is not None and not pd.isna(float(drift_raw))) else None

    trend_int = _sn("TrendIntegrity_State")
    dir_bal   = _sn("DirectionalBalance_State")
    mom_vel   = _sn("MomentumVelocity_State")

    raw_metrics = {
        "roc_5": roc_5,
        "roc_10": roc_10,
        "swing_hl_count": hl,
        "swing_lh_count": lh,
        "swing_ll_count": ll,
        "break_of_structure": bos,
        "ema20_slope": ema20s,
        "drift_from_net": drift,
        "TrendIntegrity_State": trend_int,
        "DirectionalBalance_State": dir_bal,
        "MomentumVelocity_State": mom_vel,
    }

    # ── Gate 0: is the position even under downside pressure? ────────────────
    # If drift is positive (or only modestly negative), this state is not relevant.
    # Use a -5% threshold: anything shallower than that is normal position fluctuation.
    if drift is not None and drift > -0.05:
        return ChartStateResult(
            state=RecoveryQualityState.NOT_IN_RECOVERY,
            raw_metrics=raw_metrics,
            resolution_reason="DRIFT_POSITIVE_OR_SHALLOW",
            data_complete=True,
        )

    # ── Gate 1: is the stock trending down with no bounce? ───────────────────
    still_declining = (
        roc_5 <= 0
        and roc_10 < -1.0
        and mom_vel in ("DECELERATING", "STALLING", "REVERSING", "UNKNOWN")
        and trend_int in ("STRONG_TREND", "WEAK_TREND")   # trending downward
        and dir_bal == "SELLER_DOMINANT"
    )
    if still_declining:
        return ChartStateResult(
            state=RecoveryQualityState.STILL_DECLINING,
            raw_metrics=raw_metrics,
            resolution_reason="ROC_NEGATIVE_SELLER_DOMINANT",
            data_complete=True,
        )

    # ── Gate 2: STRUCTURAL_RECOVERY — all criteria must be met ───────────────
    # These are conjunctive: a bounce that fails any one of these is noise.
    # (1) Higher low structure forming: more HL than LL in recent 20 bars
    has_higher_lows = hl >= 2 and hl > ll

    # (2) Price broke above a prior swing high (structural breakout, not just a tick)
    has_bos = bool(bos)

    # (3) Momentum confirmed on BOTH timeframes — not just a 1-day RSI bounce
    momentum_confirmed = roc_5 > 1.0 and roc_10 > 0.0

    # (4) Trend integrity is restoring (not still exhausted/broken)
    trend_restoring = trend_int in ("STRONG_TREND", "WEAK_TREND")

    # (5) Buyers re-establishing control
    buyers_present = dir_bal in ("BUYER_DOMINANT", "BALANCED")

    # (6) EMA20 turning up (not flat or falling) — the medium-term trend is inflecting
    ema_turning_up = ema20s > 0.0

    is_structural = (
        has_higher_lows
        and has_bos
        and momentum_confirmed
        and trend_restoring
        and buyers_present
        and ema_turning_up
    )

    if is_structural:
        return ChartStateResult(
            state=RecoveryQualityState.STRUCTURAL_RECOVERY,
            raw_metrics=raw_metrics,
            resolution_reason=(
                f"ALL_GATES_PASSED: HL≥2, BOS={bos}, ROC5={roc_5:.1f}%, "
                f"ROC10={roc_10:.1f}%, trend={trend_int}, dir={dir_bal}, "
                f"ema20_slope={ema20s:.4f}"
            ),
            data_complete=True,
        )

    # ── Gate 3: DEAD_CAT_BOUNCE — bounce present but structure not confirmed ─
    # Short-term uptick (ROC5 positive) but one or more structural gates failed.
    short_term_uptick = roc_5 > 0.5   # at least 0.5% up over 5 days

    dead_cat_signals = []
    if not has_higher_lows:
        dead_cat_signals.append(f"no_consecutive_HL (hl={hl}, ll={ll})")
    if not has_bos:
        dead_cat_signals.append("no_break_of_prior_swing_high")
    if not momentum_confirmed:
        dead_cat_signals.append(f"ROC10_not_confirmed ({roc_10:.1f}%)")
    if not ema_turning_up:
        dead_cat_signals.append(f"ema20_still_falling ({ema20s:.4f})")
    if not buyers_present:
        dead_cat_signals.append(f"sellers_still_dominant ({dir_bal})")

    if short_term_uptick and dead_cat_signals:
        return ChartStateResult(
            state=RecoveryQualityState.DEAD_CAT_BOUNCE,
            raw_metrics=raw_metrics,
            resolution_reason=(
                f"BOUNCE_WITHOUT_STRUCTURE: ROC5={roc_5:.1f}% up but: "
                + ", ".join(dead_cat_signals)
            ),
            data_complete=True,
        )

    # Residual: declining or neutral but no clear signal
    return ChartStateResult(
        state=RecoveryQualityState.STILL_DECLINING,
        raw_metrics=raw_metrics,
        resolution_reason="NO_STRUCTURAL_SIGNALS_PRESENT",
        data_complete=True,
    )
