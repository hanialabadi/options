"""
Tests for signal_profiles.py — strategy-aware Greek ROC signal assessment.

Tests validate:
1. Profile lookup: every strategy maps to the correct profile
2. Mode semantics: each sign convention fires on the correct direction
3. Far-OTM income exemption: suppressed when Short_Call_Delta < threshold
4. DTE gate: gamma suppressed at low DTE
5. Default profile: unknown strategies get conservative fallback
6. Multi-leg exemption: direction-neutral strategies exempt from all signals
7. Cross-strategy isolation: fixing one profile doesn't break another
"""
import numpy as np
import pandas as pd
import pytest

from core.management.cycle2.drift.signal_profiles import (
    PROFILES, DEFAULT_PROFILE, get_profile, apply_signal_profiles,
    FAR_OTM_DELTA_THRESHOLD,
)


# ---------------------------------------------------------------------------
# Profile lookup
# ---------------------------------------------------------------------------
class TestProfileLookup:

    def test_long_call_strategies_resolve(self):
        for s in ('LONG_CALL', 'BUY_CALL', 'LEAPS_CALL'):
            assert get_profile(s).name == 'LONG_CALL'

    def test_long_put_strategies_resolve(self):
        for s in ('LONG_PUT', 'BUY_PUT', 'LEAPS_PUT'):
            assert get_profile(s).name == 'LONG_PUT'

    def test_income_strategies_resolve(self):
        for s in ('BUY_WRITE', 'COVERED_CALL', 'PMCC'):
            assert get_profile(s).name == 'INCOME_SHORT_CALL'

    def test_csp_strategies_resolve(self):
        for s in ('CSP', 'SHORT_PUT'):
            assert get_profile(s).name == 'CSP'

    def test_multi_leg_strategies_resolve(self):
        for s in ('SPREAD', 'STRADDLE', 'BUTTERFLY', 'IRON_CONDOR'):
            assert get_profile(s).name == 'MULTI_LEG'

    def test_unknown_strategy_gets_default(self):
        p = get_profile('SOME_EXOTIC_STRATEGY')
        assert p.name == 'DEFAULT'

    def test_case_insensitive(self):
        assert get_profile('buy_write').name == 'INCOME_SHORT_CALL'
        assert get_profile('Long_Call').name == 'LONG_CALL'


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
def _make_df(strategy, **kwargs):
    """Build a single-row df with strategy + any Greek ROC columns."""
    row = {'Strategy': strategy, 'ROC_Persist_3D': 3}
    row.update(kwargs)
    return pd.DataFrame([row])


def _signal(df):
    return apply_signal_profiles(df)['Signal_State'].iloc[0]


# ---------------------------------------------------------------------------
# Delta ROC mode semantics
# ---------------------------------------------------------------------------
class TestDeltaROCModes:

    def test_long_call_negative_delta_fires(self):
        """SIGNED_CALL: negative Delta ROC = deterioration."""
        assert _signal(_make_df('LONG_CALL', Delta_ROC_3D=-0.35)) == 'VIOLATED'

    def test_long_call_positive_delta_no_fire(self):
        """SIGNED_CALL: positive Delta ROC = position working."""
        assert _signal(_make_df('LONG_CALL', Delta_ROC_3D=0.35)) == 'VALID'

    def test_long_put_positive_delta_fires(self):
        """SIGNED_PUT: positive Delta ROC = deterioration (delta recovering toward 0)."""
        assert _signal(_make_df('LONG_PUT', Delta_ROC_3D=0.35)) == 'VIOLATED'

    def test_long_put_negative_delta_no_fire(self):
        """SIGNED_PUT: negative Delta ROC = put thesis working."""
        assert _signal(_make_df('LONG_PUT', Delta_ROC_3D=-0.35)) == 'VALID'

    def test_bw_unsigned_both_directions_fire(self):
        """UNSIGNED: income strategy fires on either direction (if not far-OTM exempt).
        Income Delta_ROC thresholds: DEGRADED=0.30, VIOLATED=0.50."""
        assert _signal(_make_df('BUY_WRITE', Short_Call_Delta=0.30,
                                Delta_ROC_3D=-0.55)) == 'VIOLATED'
        assert _signal(_make_df('BUY_WRITE', Short_Call_Delta=0.30,
                                Delta_ROC_3D=0.55)) == 'VIOLATED'

    def test_bw_far_otm_exempt_from_delta(self):
        """Far-OTM income: delta ROC suppressed regardless of direction."""
        assert _signal(_make_df('BUY_WRITE', Short_Call_Delta=0.06,
                                Delta_ROC_3D=-0.69)) == 'VALID'


# ---------------------------------------------------------------------------
# Vega ROC mode semantics
# ---------------------------------------------------------------------------
class TestVegaROCModes:

    def test_long_vol_crush_fires(self):
        """SIGNED_LONG: negative vega ROC = IV crush = deterioration."""
        assert _signal(_make_df('LONG_CALL', Vega_ROC_3D=-0.45)) == 'VIOLATED'

    def test_long_vol_spike_no_fire(self):
        """SIGNED_LONG: positive vega ROC = IV spike = beneficial."""
        assert _signal(_make_df('LONG_CALL', Vega_ROC_3D=0.45)) == 'VALID'

    def test_short_vol_spike_fires(self):
        """SIGNED_SHORT: positive vega ROC = IV spike = deterioration.
        Vega thresholds are shared (not income-specific): VIOLATED=0.40."""
        assert _signal(_make_df('BUY_WRITE', Short_Call_Delta=0.30,
                                Vega_ROC_3D=0.45)) == 'VIOLATED'

    def test_short_vol_crush_no_fire(self):
        """SIGNED_SHORT: negative vega ROC = IV crush = position working."""
        assert _signal(_make_df('BUY_WRITE', Short_Call_Delta=0.30,
                                Vega_ROC_3D=-0.45)) == 'VALID'

    def test_csp_spike_fires(self):
        """CSP is short-vol: IV spike = deterioration."""
        assert _signal(_make_df('CSP', Vega_ROC_3D=0.45)) == 'VIOLATED'

    def test_far_otm_income_vega_exempt(self):
        """Far-OTM income: vega exposure negligible, suppress signal."""
        assert _signal(_make_df('COVERED_CALL', Short_Call_Delta=0.06,
                                Vega_ROC_3D=0.45)) == 'VALID'


# ---------------------------------------------------------------------------
# Gamma ROC mode semantics
# ---------------------------------------------------------------------------
class TestGammaROCModes:

    def test_short_gamma_rising_fires(self):
        """SHORT_GAMMA: rising gamma = acceleration against you."""
        assert _signal(_make_df('BUY_WRITE', Short_Call_Delta=0.30,
                                Gamma_ROC_3D=0.55, DTE=35)) == 'VIOLATED'

    def test_short_gamma_falling_no_fire(self):
        """SHORT_GAMMA: falling gamma = pressure relieving."""
        assert _signal(_make_df('BUY_WRITE', Short_Call_Delta=0.30,
                                Gamma_ROC_3D=-0.55, DTE=35)) == 'VALID'

    def test_long_gamma_falling_fires(self):
        """LONG_GAMMA: falling gamma = losing convexity."""
        assert _signal(_make_df('LONG_CALL', Gamma_ROC_3D=-0.55, DTE=35)) == 'VIOLATED'

    def test_long_gamma_rising_no_fire(self):
        """LONG_GAMMA: rising gamma = gaining convexity = beneficial."""
        assert _signal(_make_df('LONG_CALL', Gamma_ROC_3D=0.55, DTE=35)) == 'VALID'

    def test_dte_gate_suppresses_gamma(self):
        """DTE <= 30: gamma suppressed for all strategies."""
        assert _signal(_make_df('BUY_WRITE', Short_Call_Delta=0.30,
                                Gamma_ROC_3D=0.55, DTE=25)) == 'VALID'
        assert _signal(_make_df('LONG_CALL', Gamma_ROC_3D=-0.55, DTE=25)) == 'VALID'

    def test_far_otm_income_gamma_exempt(self):
        """Far-OTM income: gamma spike mechanical, not risk."""
        assert _signal(_make_df('BUY_WRITE', Short_Call_Delta=0.06,
                                Gamma_ROC_3D=0.55, DTE=35)) == 'VALID'


# ---------------------------------------------------------------------------
# IV ROC mode semantics
# ---------------------------------------------------------------------------
class TestIVROCModes:

    def test_long_vol_crush_fires(self):
        """SIGNED_LONG: IV crush hurts long-vol."""
        assert _signal(_make_df('LONG_CALL', IV_ROC_3D=-0.35)) == 'VIOLATED'

    def test_long_vol_spike_no_fire(self):
        """SIGNED_LONG: IV spike benefits long-vol."""
        assert _signal(_make_df('LONG_CALL', IV_ROC_3D=0.35)) == 'VALID'

    def test_short_vol_spike_fires(self):
        """SIGNED_SHORT: IV spike hurts short-vol.
        Income IV_ROC VIOLATED threshold is 0.40 (calibrated from live data)."""
        assert _signal(_make_df('BUY_WRITE', Short_Call_Delta=0.30,
                                IV_ROC_3D=0.45)) == 'VIOLATED'

    def test_short_vol_crush_no_fire(self):
        """SIGNED_SHORT: IV crush benefits short-vol."""
        assert _signal(_make_df('BUY_WRITE', Short_Call_Delta=0.30,
                                IV_ROC_3D=-0.45)) == 'VALID'

    def test_csp_crush_no_fire(self):
        """CSP is short-vol: IV crush = position working."""
        assert _signal(_make_df('CSP', IV_ROC_3D=-0.45)) == 'VALID'

    def test_csp_spike_fires(self):
        """CSP is short-vol: IV spike = deterioration.
        CSP IV_ROC VIOLATED threshold is 0.40."""
        assert _signal(_make_df('CSP', IV_ROC_3D=0.45)) == 'VIOLATED'


# ---------------------------------------------------------------------------
# Multi-leg exemption
# ---------------------------------------------------------------------------
class TestMultiLegExemption:

    def test_straddle_exempt_from_all(self):
        """Direction-neutral: all Greek ROCs exempt."""
        df = _make_df('STRADDLE', Delta_ROC_3D=-0.50, Vega_ROC_3D=-0.50,
                      Gamma_ROC_3D=0.60, IV_ROC_3D=-0.50, DTE=35)
        assert _signal(df) == 'VALID'

    def test_iron_condor_exempt(self):
        df = _make_df('IRON_CONDOR', Delta_ROC_3D=-0.50, Vega_ROC_3D=-0.50,
                      Gamma_ROC_3D=0.60, IV_ROC_3D=-0.50, DTE=35)
        assert _signal(df) == 'VALID'


# ---------------------------------------------------------------------------
# Default profile (unknown strategy)
# ---------------------------------------------------------------------------
class TestDefaultProfile:

    def test_unknown_strategy_unsigned_delta(self):
        """Unknown strategy: UNSIGNED mode fires on both directions."""
        assert _signal(_make_df('SOME_NEW_STRATEGY', Delta_ROC_3D=-0.35)) == 'VIOLATED'
        assert _signal(_make_df('SOME_NEW_STRATEGY', Delta_ROC_3D=0.35)) == 'VIOLATED'

    def test_unknown_strategy_unsigned_vega(self):
        assert _signal(_make_df('SOME_NEW_STRATEGY', Vega_ROC_3D=-0.45)) == 'VIOLATED'
        assert _signal(_make_df('SOME_NEW_STRATEGY', Vega_ROC_3D=0.45)) == 'VIOLATED'

    def test_unknown_strategy_gamma_exempt(self):
        """Unknown strategy: gamma exempt (can't guess position sign)."""
        assert _signal(_make_df('SOME_NEW_STRATEGY', Gamma_ROC_3D=0.60, DTE=35)) == 'VALID'


# ---------------------------------------------------------------------------
# Cross-strategy isolation
# ---------------------------------------------------------------------------
class TestCrossStrategyIsolation:
    """Verify that different strategies in the SAME dataframe are assessed
    independently — fixing BW can't break LONG_CALL."""

    def test_mixed_strategies_independent(self):
        df = pd.DataFrame([
            {'Strategy': 'BUY_WRITE', 'Short_Call_Delta': 0.06,
             'IV_ROC_3D': -0.35, 'ROC_Persist_3D': 3},   # far-OTM, IV crush = working
            {'Strategy': 'LONG_CALL', 'Short_Call_Delta': np.nan,
             'IV_ROC_3D': -0.35, 'ROC_Persist_3D': 3},   # long-vol, IV crush = hurt
            {'Strategy': 'STRADDLE', 'Short_Call_Delta': np.nan,
             'IV_ROC_3D': -0.35, 'ROC_Persist_3D': 3},   # exempt
        ])
        result = apply_signal_profiles(df)
        assert result['Signal_State'].iloc[0] == 'VALID', "BW far-OTM should be VALID"
        assert result['Signal_State'].iloc[1] == 'VIOLATED', "LONG_CALL should be VIOLATED"
        assert result['Signal_State'].iloc[2] == 'VALID', "STRADDLE should be VALID (exempt)"

    def test_csp_and_long_put_same_df(self):
        """CSP and LONG_PUT in same df: IV crush hurts put, helps CSP."""
        df = pd.DataFrame([
            {'Strategy': 'CSP', 'IV_ROC_3D': -0.35, 'ROC_Persist_3D': 3},
            {'Strategy': 'LONG_PUT', 'IV_ROC_3D': -0.35, 'ROC_Persist_3D': 3},
        ])
        result = apply_signal_profiles(df)
        assert result['Signal_State'].iloc[0] == 'VALID', "CSP IV crush = working"
        assert result['Signal_State'].iloc[1] == 'VIOLATED', "LONG_PUT IV crush = hurt"


# ---------------------------------------------------------------------------
# Persistence gate
# ---------------------------------------------------------------------------
class TestPersistenceGate:

    def test_degraded_requires_persist_1(self):
        """DEGRADED requires ROC_Persist_3D >= 1."""
        df = _make_df('LONG_CALL', Delta_ROC_3D=-0.20, ROC_Persist_3D=0)
        assert _signal(df) == 'VALID'  # persist=0 < 1 → suppressed
        df2 = _make_df('LONG_CALL', Delta_ROC_3D=-0.20, ROC_Persist_3D=1)
        assert _signal(df2) == 'DEGRADED'  # persist=1 >= 1 → fires

    def test_violated_no_persist_gate(self):
        """VIOLATED fires regardless of ROC_Persist_3D (3D ROC is its own persistence)."""
        df = _make_df('LONG_CALL', Delta_ROC_3D=-0.35, ROC_Persist_3D=0)
        assert _signal(df) == 'VIOLATED'  # persist=0 OK for VIOLATED

    def test_no_persist_column_defaults_to_fire(self):
        """Missing ROC_Persist_3D → assume satisfied."""
        df = pd.DataFrame([{'Strategy': 'LONG_CALL', 'Delta_ROC_3D': -0.20}])
        assert _signal(df) == 'DEGRADED'


# ---------------------------------------------------------------------------
# Escalation semantics
# ---------------------------------------------------------------------------
class TestEscalation:

    def test_violated_never_downgraded(self):
        """Once VIOLATED, can't be downgraded back to DEGRADED."""
        df = _make_df('LONG_CALL', Delta_ROC_3D=-0.35, Vega_ROC_3D=-0.05,
                      IV_ROC_3D=-0.05)
        result = _signal(df)
        assert result == 'VIOLATED'

    def test_degraded_can_upgrade_to_violated(self):
        """Start DEGRADED from one Greek, upgrade to VIOLATED from another."""
        df = _make_df('LONG_CALL', Delta_ROC_3D=-0.20, IV_ROC_3D=-0.35)
        result = _signal(df)
        assert result == 'VIOLATED'
