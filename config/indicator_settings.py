# Default settings for TA-Lib indicators

RSI_SETTINGS = {
    "timeperiod": 14
}

ADX_SETTINGS = {
    "timeperiod": 14
}

SMA_SETTINGS = {
    "timeperiod_20": 20,
    "timeperiod_50": 50,
    "timeperiod_3": 3 # For drift smoothing if needed
}

EMA_SETTINGS = {
    "timeperiod_9": 9,
    "timeperiod_21": 21
}

ATR_SETTINGS = {
    "timeperiod": 14
}

MACD_SETTINGS = {
    "fastperiod": 12,
    "slowperiod": 26,
    "signalperiod": 9
}

BBANDS_SETTINGS = {
    "timeperiod": 20,
    "nbdevup": 2,
    "nbdevdn": 2,
    "matype": 0 # 0 for SMA
}

STOCH_SETTINGS = {
    "fastk_period": 5,
    "slowk_period": 3,
    "slowk_matype": 0, # 0 for SMA
    "slowd_period": 3,
    "slowd_matype": 0  # 0 for SMA
}

MARKET_STRESS_THRESHOLDS = {
    "ATR_LOW": 0.5,      # SPY ATR % below this is considered LOW stress
    "ATR_ELEVATED": 1.5, # SPY ATR % above this is considered ELEVATED stress
    "ATR_CRISIS": 2.5,   # SPY ATR % above this is considered CRISIS stress
    "VIX_ELEVATED": 25,  # VIX above this is considered ELEVATED stress
    "VIX_CRISIS": 35     # VIX above this is considered CRISIS stress
}

# Other configurable thresholds for chart signals
# ADX tiers: Murphy — "ADX drop from above 40 = trend weakening.
# Rise back above 20 = new trend starting."
REGIME_CLASSIFICATION_THRESHOLDS = {
    "overextension_pct": 0.06,   # Murphy: "3% envelopes = overextended". 6% for medium-term
    # ATR self-referencing: bottom Nth percentile of own ATR% history → Compressed
    "atr_compressed_percentile": 20,
    # ADX tier boundaries (Murphy's framework)
    "adx_range_bound": 20,   # ADX < 20 → Ranging (cf. doctrine thresholds.py ADX_WEAK_TREND=20)
    "adx_emerging":    30,   # 20 ≤ ADX < 30 → Emerging_Trend (cf. doctrine ADX_STRONG_TREND=30)
    "adx_trending":    40,   # 30 ≤ ADX < 40 → Trending (doctrine uses 25 as "trending" — lower bar for position decisions)
    # ADX ≥ 40 → Strong_Trend
}

# Blind-spot detection thresholds (DQS multiplier penalties)
# Applied in step12_acceptance._apply_dqs_multiplier_chain()
BLIND_SPOT_THRESHOLDS = {
    # Divergence opposing trade direction (Murphy Ch.10: "serious warning")
    "divergence_single_mult": 0.95,     # One of RSI/MACD diverges
    "divergence_double_mult": 0.90,     # Both RSI+MACD diverge

    # BB extremes on directional entries (Murphy: "overextended band touch")
    # Trend-adjusted: strong trends "walk the band" (Bollinger), so raise threshold
    "bb_extreme_bullish": 85,           # Long calls above this BB% (ADX < 30)
    "bb_extreme_bearish": 15,           # Long puts below this BB% (ADX < 30)
    "bb_extreme_trending": 90,          # Raised threshold for ADX 30-39 (trending)
    "bb_extreme_mult": 0.95,
    "bb_strong_trend_adx": 40,          # ADX ≥ 40: skip BB penalty (band-walking expected)
    "bb_trending_adx": 30,              # ADX 30-39: use raised threshold

    # OBV slope conflicts with trade direction (Murphy Ch.7: smart money flow)
    "obv_bullish_floor": -15,           # OBV below this contradicts bullish
    "obv_bearish_ceiling": 15,          # OBV above this contradicts bearish
    "obv_conflict_mult": 0.95,

    # LEAP IV amplifier (Natenberg: vega 2-3x for LEAPs, IV mean-reverts)
    "leap_iv_rank_threshold": 80,       # IV_Rank above this on LEAP = amplified penalty
    "leap_iv_amplifier": 0.90,          # Stacks with base IV Headwind 0.85

    # Structure conflict: directional trade against swing structure (Murphy Ch.4)
    # Long Call on Downtrend or Long Put on Uptrend = fighting the structure
    "structure_conflict_mult": 0.90,    # Severe: 9-month LEAP against prevailing swings

    # Weekly trend conflict on LEAPs (Murphy: "weekly signals filter daily")
    # LEAPs are multi-month, so weekly timeframe matters MORE than daily
    "weekly_conflict_leap_mult": 0.95,  # LEAP + Weekly_Trend_Bias=CONFLICTING

    # ADX conviction gate for directional trades (Murphy 0.764: "trade highest trend ratings")
    # Short-dated directionals in flat/ranging markets burn theta with no movement
    "adx_no_trend": 15,                 # ADX < 15: no trend conviction → ×0.90
    "adx_no_trend_mult": 0.90,
    "adx_weak_trend": 20,              # ADX 15-19: weak trend → annotation only

    # Earnings IV crush gate for short-dated directionals (Augen 0.754: "IV collapses post-announcement")
    # Buying non-LEAP directionals near earnings = paying inflated IV that will crush
    # Track record (beat_rate, avg_iv_crush, avg_move_ratio) enriches the penalty context
    "earnings_proximity_days": 5,       # ≤5 trading days to earnings → penalty zone
    "earnings_iv_crush_mult": 0.90,     # ×0.90 for short-dated directional in crush zone
    "earnings_high_crush_pct": 30,      # avg_iv_crush > 30% → severe crush warning
    "earnings_low_move_ratio": 0.6,     # move_ratio < 0.6 → market overprices (stacks to ×0.85)
}

# ── Signal Drift Thresholds (Greek ROC) ───────────────────────────────────────
# Used by signal_profiles.py — single source of truth for all Greek ROC
# escalation thresholds. Strategy-specific sign conventions are declared in
# signal profiles, NOT here. These are the magnitude thresholds only.
SIGNAL_DRIFT_THRESHOLDS = {
    # ── Directional strategies (LONG_CALL, LONG_PUT) ──
    # Distributions are tight (std ~0.20), fire rate 2-10% — well-calibrated.
    "DELTA_ROC_DEGRADED": 0.15,
    "DELTA_ROC_VIOLATED": 0.30,
    "VEGA_ROC_DEGRADED": 0.20,
    "VEGA_ROC_VIOLATED": 0.40,
    "GAMMA_ROC_DEGRADED": 0.25,
    "GAMMA_ROC_VIOLATED": 0.50,
    "IV_ROC_DEGRADED": 0.15,
    "IV_ROC_VIOLATED": 0.30,
    # ── Income / CSP overrides ──
    # Distributions are much wider (std ~0.40), directional thresholds fire
    # on 40-50% of snapshots = pure noise. Calibrated from 6,696 management
    # snapshots (Mar 2026): target ~15-20% DEGRADED, ~8-10% VIOLATED.
    "INCOME_DELTA_ROC_DEGRADED": 0.30,   # was 0.15 → fired 48% (noise)
    "INCOME_DELTA_ROC_VIOLATED": 0.50,   # was 0.30 → fired 24% (noise)
    "INCOME_IV_ROC_DEGRADED": 0.25,      # was 0.15 → fired 23% (positive bias)
    "INCOME_IV_ROC_VIOLATED": 0.40,      # was 0.30 → fired 22% (positive bias)
    "CSP_DELTA_ROC_DEGRADED": 0.30,      # was 0.15 → fired 42%
    "CSP_DELTA_ROC_VIOLATED": 0.50,      # was 0.30 → fired 29%
    "CSP_IV_ROC_DEGRADED": 0.25,         # was 0.15 → fired 24%
    "CSP_IV_ROC_VIOLATED": 0.40,         # was 0.30 → fired 21%
    # Gamma DTE gate: suppress when DTE <= this (mechanical expiry spike)
    "GAMMA_DTE_GATE": 30,
    # Far-OTM income exemption: Short_Call_Delta below this → suppress signal
    "FAR_OTM_DELTA_THRESHOLD": 0.15,
    # PCS drift (Tier 1 — blanket, not profile-driven)
    "PCS_DRIFT_DEGRADED": 15,
    "PCS_DRIFT_VIOLATED": 25,
    # Delta 1D tail (Tier 2 — blanket)
    "DELTA_1D_TAIL_DEGRADED": 0.20,
}

# ── Market-Wide Regime Classification ─────────────────────────────────────────
# Composite classifier: 8 indicators → weighted score → regime bucket.
# Weights approved by user review (Mar 2026). SKEW intentionally low (tail-pricing,
# not immediate realized stress). Term structure + breadth weighted higher
# (operationally more relevant for options engine).
MARKET_REGIME_THRESHOLDS = {
    # VIX level (weight: 20%)
    "VIX_CAUTIOUS": 20,
    "VIX_RISK_OFF": 25,
    "VIX_CRISIS": 35,
    # VIX percentile 252d (weight: 15%)
    "VIX_PCTL_CAUTIOUS": 60,
    "VIX_PCTL_RISK_OFF": 80,
    "VIX_PCTL_CRISIS": 95,
    # Term structure — VIX / VIX_3M ratio (weight: 20%)
    # >1.0 = backwardation (front-month fear > back-month)
    "TERM_CAUTIOUS": 0.95,
    "TERM_RISK_OFF": 1.0,
    "TERM_CRISIS": 1.1,
    # VVIX — volatility of volatility (weight: 10%)
    "VVIX_CAUTIOUS": 120,
    "VVIX_RISK_OFF": 130,
    "VVIX_CRISIS": 150,
    # SKEW — tail risk pricing (weight: 5%)
    "SKEW_CAUTIOUS": 135,
    "SKEW_RISK_OFF": 140,
    "SKEW_CRISIS": 155,
    # Credit proxy — HYG/LQD ratio (weight: 10%)
    # Lower = stress (HY bonds sold off relative to IG)
    "CREDIT_CAUTIOUS": 0.94,
    "CREDIT_RISK_OFF": 0.92,
    "CREDIT_CRISIS": 0.88,
    # Universe breadth — % of our tickers above SMA50 (weight: 15%)
    "BREADTH_CAUTIOUS": 50,
    "BREADTH_RISK_OFF": 40,
    "BREADTH_CRISIS": 25,
    # Average pairwise correlation (weight: 5%)
    "CORR_CAUTIOUS": 0.4,
    "CORR_RISK_OFF": 0.5,
    "CORR_CRISIS": 0.7,
    # Staleness — business-day aware
    "STALENESS_WARNING_BDAYS": 2,
    "STALENESS_CRITICAL_BDAYS": 3,
    # DQS multipliers by regime
    "DQS_MULT_CRISIS": 0.85,
    "DQS_MULT_RISK_OFF": 0.90,
    "DQS_MULT_CAUTIOUS": 0.95,
    # ── SKEW DQS Multipliers (strategy-aware, direct) ──────────────────────
    # Applied in step12 DQS chain on top of composite regime multiplier.
    # Long-vega penalised: elevated SKEW = market pricing tail risk, buying
    # calls/puts at premium. Income boosted: sellers collect richer premium.
    # Normal SKEW range: 100-130. Elevated: 135+. Extreme: 150+.
    "SKEW_DQS_LONG_PENALTY_THRESHOLD": 140,     # SKEW above this penalises long-vega
    "SKEW_DQS_LONG_PENALTY_MILD": 0.97,         # 140-149: mild penalty
    "SKEW_DQS_LONG_PENALTY_SEVERE": 0.93,       # 150+: stronger penalty
    "SKEW_DQS_INCOME_BOOST_THRESHOLD": 135,     # SKEW above this boosts income
    "SKEW_DQS_INCOME_BOOST": 1.03,              # income gets richer premium
}
