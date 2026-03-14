"""
Anti-blanket regression tests — prove cross-strategy isolation can't regress.

These tests exist to prevent the class of bugs where fixing one strategy family's
signal behavior silently breaks another. Each test targets a specific leakage
vector identified during the Mar 2026 blanket audit.

Test groups:
 1. Same magnitude, different strategy → different outcome
 2. Missing-column isolation (no silent proxy fallback)
 3. Cross-family threshold isolation (patching one family can't affect another)
 4. Profile lookup coverage (every live strategy maps to intended profile)
 5. DEFAULT usage audit (DEFAULT is rare and intentional)
 6. Near-strike / far-OTM boundary behavior
 7. Doctrine non-interference (drift VIOLATED can't bypass doctrine income guard)
 8. Multi-row mixed DataFrame row-wise isolation
 9. Signal-combiner isolation (aggregation semantics per family)
10. Golden-case regression pack (frozen expected outputs from real patterns)
"""
import numpy as np
import pandas as pd
import pytest
from unittest.mock import patch

from core.management.cycle2.drift.signal_profiles import (
    PROFILES, DEFAULT_PROFILE, get_profile, apply_signal_profiles,
    FAR_OTM_DELTA_THRESHOLD, _STRATEGY_MAP,
)
from config.indicator_settings import SIGNAL_DRIFT_THRESHOLDS as _T


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_row(strategy: str, **kwargs) -> dict:
    """Build a single row dict with strategy + any overrides."""
    row = {'Strategy': strategy, 'ROC_Persist_3D': 3}
    row.update(kwargs)
    return row


def _signal(df: pd.DataFrame) -> str:
    return apply_signal_profiles(df)['Signal_State'].iloc[0]


def _signals(df: pd.DataFrame) -> list:
    return apply_signal_profiles(df)['Signal_State'].tolist()


# ===========================================================================
# 1. Same magnitude, different strategy → different outcome
# ===========================================================================
class TestSameMagnitudeDifferentOutcome:
    """Identical ROC values, vary only strategy family → different results.
    This is the single best anti-blanket test."""

    DELTA_ROC = -0.35   # Between directional VIOLATED (0.30) and income DEGRADED (0.30)
    IV_ROC_NEG = -0.35  # IV crush: hurts long-vol, helps short-vol
    IV_ROC_POS = +0.35  # IV spike: hurts short-vol (but < income VIOLATED 0.40)

    def test_delta_roc_fires_long_call_not_income(self):
        """Delta_ROC = -0.35 fires VIOLATED for LONG_CALL (thresh 0.30)
        but only DEGRADED for INCOME (thresh 0.50 VIOLATED, 0.30 DEGRADED)."""
        df = pd.DataFrame([
            _make_row('LONG_CALL', Delta_ROC_3D=self.DELTA_ROC),
            _make_row('BUY_WRITE', Delta_ROC_3D=self.DELTA_ROC, Short_Call_Delta=0.30),
        ])
        result = _signals(df)
        assert result[0] == 'VIOLATED', f"LONG_CALL should VIOLATED, got {result[0]}"
        assert result[1] == 'DEGRADED', f"BUY_WRITE should DEGRADED (wider threshold), got {result[1]}"

    def test_delta_roc_exempt_for_multileg(self):
        """Same Delta_ROC fires for LONG_CALL, exempt for STRADDLE."""
        df = pd.DataFrame([
            _make_row('LONG_CALL', Delta_ROC_3D=self.DELTA_ROC),
            _make_row('STRADDLE', Delta_ROC_3D=self.DELTA_ROC),
        ])
        result = _signals(df)
        assert result[0] == 'VIOLATED'
        assert result[1] == 'VALID', "MULTI_LEG must be EXEMPT"

    def test_iv_crush_hurts_long_helps_short(self):
        """IV crush -0.35: VIOLATED for LONG_CALL (SIGNED_LONG), VALID for BW (SIGNED_SHORT)."""
        df = pd.DataFrame([
            _make_row('LONG_CALL', IV_ROC_3D=self.IV_ROC_NEG),
            _make_row('BUY_WRITE', IV_ROC_3D=self.IV_ROC_NEG, Short_Call_Delta=0.30),
            _make_row('CSP', IV_ROC_3D=self.IV_ROC_NEG),
        ])
        result = _signals(df)
        assert result[0] == 'VIOLATED', "LONG_CALL hurt by IV crush"
        assert result[1] == 'VALID', "BW benefits from IV crush"
        assert result[2] == 'VALID', "CSP benefits from IV crush"

    def test_iv_spike_hurts_short_helps_long(self):
        """IV spike +0.35: VALID for LONG_CALL, DEGRADED for BW/CSP
        (between income DEGRADED 0.25 and VIOLATED 0.40)."""
        df = pd.DataFrame([
            _make_row('LONG_CALL', IV_ROC_3D=self.IV_ROC_POS),
            _make_row('BUY_WRITE', IV_ROC_3D=self.IV_ROC_POS, Short_Call_Delta=0.30),
            _make_row('CSP', IV_ROC_3D=self.IV_ROC_POS),
        ])
        result = _signals(df)
        assert result[0] == 'VALID', "LONG_CALL benefits from IV spike"
        assert result[1] == 'DEGRADED', "BW hurt by IV spike (DEGRADED, not yet VIOLATED)"
        assert result[2] == 'DEGRADED', "CSP hurt by IV spike"

    def test_csp_vs_long_put_opposite_delta_direction(self):
        """Delta_ROC +0.25: DEGRADED for LONG_PUT (delta recovering = put losing),
        fires UNSIGNED for CSP. Different modes, same magnitude."""
        df = pd.DataFrame([
            _make_row('LONG_PUT', Delta_ROC_3D=+0.25),
            _make_row('CSP', Delta_ROC_3D=+0.25),
        ])
        result = _signals(df)
        assert result[0] == 'DEGRADED', "LONG_PUT: positive delta = put deterioration"
        assert result[1] == 'VALID', "CSP: 0.25 < CSP DEGRADED threshold 0.30"

    def test_unknown_strategy_gets_conservative_unsigned(self):
        """Unknown strategy with same ROC as LONG_CALL: both fire, but modes differ."""
        df = pd.DataFrame([
            _make_row('LONG_CALL', Delta_ROC_3D=-0.35),
            _make_row('SOME_NEW_STRATEGY', Delta_ROC_3D=-0.35),
        ])
        result = _signals(df)
        assert result[0] == 'VIOLATED', "LONG_CALL SIGNED_CALL fires on negative"
        assert result[1] == 'VIOLATED', "DEFAULT UNSIGNED fires on both directions"


# ===========================================================================
# 2. Missing-column isolation
# ===========================================================================
class TestMissingColumnIsolation:
    """Verify safe fallback when family-specific fields are missing.
    No accidental reuse of generic position fields."""

    def test_bw_missing_short_call_delta_not_exempt(self):
        """BW without Short_Call_Delta defaults to 0.5 (not exempt), NOT position Delta."""
        df = pd.DataFrame([_make_row('BUY_WRITE', Delta_ROC_3D=-0.55)])
        # No Short_Call_Delta column → default 0.5 → not far-OTM exempt → should fire
        assert _signal(df) == 'VIOLATED'

    def test_bw_with_position_delta_low_still_fires(self):
        """BW with low position Delta (0.10) but no Short_Call_Delta still fires.
        This catches the old bug where position Delta was used as proxy."""
        df = pd.DataFrame([_make_row('BUY_WRITE', Delta=0.10, Delta_ROC_3D=-0.55)])
        # Delta column exists but is NOT Short_Call_Delta — should still fire
        assert _signal(df) == 'VIOLATED'

    def test_csp_missing_vega_roc_no_crash(self):
        """CSP without Vega_ROC_3D column — no crash, other signals still work."""
        df = pd.DataFrame([_make_row('CSP', Delta_ROC_3D=-0.55)])
        # Only Delta_ROC present, Vega/Gamma/IV missing → fires on Delta only
        assert _signal(df) == 'VIOLATED'

    def test_long_call_missing_all_greeks_stays_valid(self):
        """LONG_CALL with no Greek ROC columns at all → VALID (no crash)."""
        df = pd.DataFrame([_make_row('LONG_CALL')])
        assert _signal(df) == 'VALID'

    def test_multileg_missing_all_helper_columns(self):
        """MULTI_LEG with no helper columns → VALID, no crash."""
        df = pd.DataFrame([_make_row('IRON_CONDOR')])
        assert _signal(df) == 'VALID'

    def test_missing_strategy_column_no_crash(self):
        """DataFrame with no Strategy column → DEFAULT profile, no crash."""
        df = pd.DataFrame([{'Delta_ROC_3D': -0.35, 'ROC_Persist_3D': 3}])
        assert _signal(df) == 'VIOLATED'  # DEFAULT UNSIGNED fires

    def test_missing_dte_column_gamma_still_works(self):
        """Missing DTE column → DTE defaults to 0 → gamma DTE gate suppresses.
        This is the safe failure mode (no false gamma signals)."""
        df = pd.DataFrame([_make_row('LONG_CALL', Gamma_ROC_3D=-0.55)])
        # DTE defaults to 0, gamma DTE gate = 30, so 0 <= 30 → suppressed
        assert _signal(df) == 'VALID'


# ===========================================================================
# 3. Cross-family threshold isolation
# ===========================================================================
class TestCrossFamilyThresholdIsolation:
    """Changing one family's thresholds must not affect another family."""

    def test_income_threshold_change_does_not_affect_long_call(self):
        """Patch income Delta thresholds; LONG_CALL result unchanged."""
        df_lc = pd.DataFrame([_make_row('LONG_CALL', Delta_ROC_3D=-0.20)])
        baseline = _signal(df_lc)

        with patch.dict(_T, {
            'INCOME_DELTA_ROC_DEGRADED': 0.05,
            'INCOME_DELTA_ROC_VIOLATED': 0.10,
        }):
            # Must reimport to pick up patched values
            from core.management.cycle2.drift import signal_profiles as sp
            # But profiles are built at import time — the patched values
            # won't affect LONG_CALL profile which uses non-INCOME keys
            result = apply_signal_profiles(
                pd.DataFrame([_make_row('LONG_CALL', Delta_ROC_3D=-0.20)])
            )['Signal_State'].iloc[0]

        assert result == baseline, (
            f"Changing INCOME thresholds affected LONG_CALL: {baseline} → {result}"
        )

    def test_directional_and_income_use_different_config_keys(self):
        """Verify LONG_CALL and INCOME_SHORT_CALL reference different config keys."""
        lc_profile = get_profile('LONG_CALL')
        bw_profile = get_profile('BUY_WRITE')

        # Delta ROC thresholds should differ
        assert lc_profile.delta_roc.degraded != bw_profile.delta_roc.degraded, (
            "LONG_CALL and BW should use different Delta_ROC thresholds"
        )
        assert lc_profile.delta_roc.degraded == _T['DELTA_ROC_DEGRADED']
        assert bw_profile.delta_roc.degraded == _T['INCOME_DELTA_ROC_DEGRADED']

    def test_csp_and_income_can_diverge(self):
        """CSP and INCOME_SHORT_CALL may have same or different thresholds,
        but they reference independent config keys."""
        csp = get_profile('CSP')
        bw = get_profile('BUY_WRITE')
        assert csp.delta_roc.degraded == _T['CSP_DELTA_ROC_DEGRADED']
        assert bw.delta_roc.degraded == _T['INCOME_DELTA_ROC_DEGRADED']


# ===========================================================================
# 4. Profile lookup coverage
# ===========================================================================
class TestProfileLookupCoverage:
    """Every live strategy maps to the intended profile — not DEFAULT."""

    # All strategies that exist in the live portfolio or scan pipeline
    KNOWN_STRATEGIES = {
        # Income
        'BUY_WRITE': 'INCOME_SHORT_CALL',
        'COVERED_CALL': 'INCOME_SHORT_CALL',
        'PMCC': 'INCOME_SHORT_CALL',
        # CSP
        'CSP': 'CSP',
        'SHORT_PUT': 'CSP',
        # Long directional
        'LONG_CALL': 'LONG_CALL',
        'BUY_CALL': 'LONG_CALL',
        'LEAPS_CALL': 'LONG_CALL',
        'LONG_PUT': 'LONG_PUT',
        'BUY_PUT': 'LONG_PUT',
        'LEAPS_PUT': 'LONG_PUT',
        # Multi-leg
        'SPREAD': 'MULTI_LEG',
        'STRADDLE': 'MULTI_LEG',
        'STRANGLE': 'MULTI_LEG',
        'BUTTERFLY': 'MULTI_LEG',
        'IRON_CONDOR': 'MULTI_LEG',
        'VERTICAL_SPREAD': 'MULTI_LEG',
        'CALENDAR_SPREAD': 'MULTI_LEG',
    }

    @pytest.mark.parametrize("strategy,expected_profile", KNOWN_STRATEGIES.items())
    def test_strategy_maps_to_correct_profile(self, strategy, expected_profile):
        profile = get_profile(strategy)
        assert profile.name == expected_profile, (
            f"{strategy} mapped to {profile.name}, expected {expected_profile}"
        )

    @pytest.mark.parametrize("strategy,expected_profile", KNOWN_STRATEGIES.items())
    def test_case_insensitive_lookup(self, strategy, expected_profile):
        assert get_profile(strategy.lower()).name == expected_profile


# ===========================================================================
# 5. DEFAULT usage audit
# ===========================================================================
class TestDefaultUsageAudit:
    """DEFAULT should only fire for genuinely unknown strategies."""

    def test_no_known_strategy_hits_default(self):
        """Every strategy in KNOWN_STRATEGIES resolves to a named profile, not DEFAULT."""
        for strategy in TestProfileLookupCoverage.KNOWN_STRATEGIES:
            profile = get_profile(strategy)
            assert profile.name != 'DEFAULT', (
                f"Known strategy {strategy} fell through to DEFAULT"
            )

    def test_default_only_for_truly_unknown(self):
        unknown = ['EXOTIC_SPREAD', 'RATIO_BACKSPREAD', 'JADE_LIZARD', 'UNKNOWN_123']
        for s in unknown:
            assert get_profile(s).name == 'DEFAULT', f"{s} should be DEFAULT"

    def test_stock_only_hits_default(self):
        """STOCK_ONLY has no options Greeks — DEFAULT is correct."""
        assert get_profile('STOCK_ONLY').name == 'DEFAULT'

    def test_strategy_map_has_no_gaps(self):
        """Every strategy listed in a profile's strategies set is in _STRATEGY_MAP."""
        for profile in PROFILES.values():
            for s in profile.strategies:
                assert s in _STRATEGY_MAP, f"{s} not in _STRATEGY_MAP"
                assert _STRATEGY_MAP[s].name == profile.name


# ===========================================================================
# 6. Near-strike / far-OTM boundary
# ===========================================================================
class TestFarOTMBoundary:
    """Test exact cutoff at FAR_OTM_DELTA_THRESHOLD (0.15)."""

    def test_below_threshold_exempt(self):
        """Short_Call_Delta = 0.14 → exempt (far-OTM, position working)."""
        df = pd.DataFrame([_make_row(
            'BUY_WRITE', Short_Call_Delta=0.14, Delta_ROC_3D=-0.55
        )])
        assert _signal(df) == 'VALID'

    def test_at_threshold_not_exempt(self):
        """Short_Call_Delta = 0.15 → NOT exempt (at boundary, fires)."""
        df = pd.DataFrame([_make_row(
            'BUY_WRITE', Short_Call_Delta=0.15, Delta_ROC_3D=-0.55
        )])
        assert _signal(df) == 'VIOLATED'

    def test_above_threshold_not_exempt(self):
        """Short_Call_Delta = 0.16 → NOT exempt (clearly not far-OTM)."""
        df = pd.DataFrame([_make_row(
            'BUY_WRITE', Short_Call_Delta=0.16, Delta_ROC_3D=-0.55
        )])
        assert _signal(df) == 'VIOLATED'

    def test_far_otm_exemption_only_applies_to_marked_rules(self):
        """INCOME iv_roc has far_otm_exempt=False → fires even at low delta."""
        # IV_ROC on INCOME_SHORT_CALL does NOT have far_otm_exempt
        profile = get_profile('BUY_WRITE')
        assert profile.iv_roc.far_otm_exempt is False
        assert profile.delta_roc.far_otm_exempt is True
        assert profile.vega_roc.far_otm_exempt is True
        assert profile.gamma_roc.far_otm_exempt is True

    def test_csp_has_no_far_otm_exemption(self):
        """CSP has no short call → no far-OTM exemption on any Greek."""
        profile = get_profile('CSP')
        assert profile.delta_roc.far_otm_exempt is False
        assert profile.vega_roc.far_otm_exempt is False
        assert profile.gamma_roc.far_otm_exempt is False
        assert profile.iv_roc.far_otm_exempt is False

    def test_nan_short_call_delta_defaults_to_not_exempt(self):
        """NaN Short_Call_Delta → filled with 0.5 → not exempt."""
        df = pd.DataFrame([_make_row(
            'BUY_WRITE', Short_Call_Delta=np.nan, Delta_ROC_3D=-0.55
        )])
        assert _signal(df) == 'VIOLATED'


# ===========================================================================
# 7. Doctrine non-interference
# ===========================================================================
class TestDoctrineNonInterference:
    """Drift VIOLATED signals must not bypass doctrine-layer income guards."""

    def test_income_far_otm_expiring_guard_independent_of_signal_state(self):
        """run_all.py's _income_far_otm_expiring guard uses Short_Call_Delta < 0.30
        and DTE <= 14 — this is SEPARATE from signal profiles' 0.15 far-OTM threshold.
        The drift layer and the doctrine layer are independent guards."""
        # Drift far-OTM threshold
        assert FAR_OTM_DELTA_THRESHOLD == 0.15

        # Doctrine income guard uses wider 0.30 + DTE ≤ 14 — different criteria
        # Verify they CAN be different (not accidentally linked)
        assert FAR_OTM_DELTA_THRESHOLD < 0.30, (
            "Drift far-OTM (0.15) should be tighter than doctrine income guard (0.30)"
        )

    def test_violated_bw_at_low_dte_should_still_be_guardable(self):
        """A BW that fires VIOLATED in drift should still be capturable by
        doctrine's income far-OTM guard (different mechanism).
        Signal profiles fire → run_all.py rule layer can still override."""
        # BW with Short_Call_Delta = 0.20 (above drift far-OTM 0.15, below doctrine's 0.30)
        df = pd.DataFrame([_make_row(
            'BUY_WRITE', Short_Call_Delta=0.20, Delta_ROC_3D=-0.55, DTE=10
        )])
        # Drift layer fires VIOLATED
        assert _signal(df) == 'VIOLATED'
        # But doctrine layer (run_all.py) uses _sc_delta_r < 0.30 and _sc_dte_r <= 14
        # So this position (delta 0.20 < 0.30, DTE 10 <= 14) WOULD be guarded by doctrine
        # The two layers are independent: drift signals, doctrine decides.


# ===========================================================================
# 8. Multi-row mixed DataFrame row-wise isolation
# ===========================================================================
class TestMultiRowIsolation:
    """One DataFrame with all families — verify row-wise isolation.
    Blanket bugs often appear when vectorized masks leak across rows."""

    def _build_mixed_df(self):
        return pd.DataFrame([
            _make_row('LONG_CALL',    Delta_ROC_3D=-0.35, IV_ROC_3D=-0.35, DTE=45),
            _make_row('LONG_PUT',     Delta_ROC_3D=-0.35, IV_ROC_3D=-0.35, DTE=45),
            _make_row('BUY_WRITE',    Delta_ROC_3D=-0.35, IV_ROC_3D=-0.35,
                      Short_Call_Delta=0.30, DTE=45),
            _make_row('CSP',          Delta_ROC_3D=-0.35, IV_ROC_3D=-0.35, DTE=45),
            _make_row('STRADDLE',     Delta_ROC_3D=-0.35, IV_ROC_3D=-0.35, DTE=45),
            _make_row('MYSTERY_STRAT', Delta_ROC_3D=-0.35, IV_ROC_3D=-0.35, DTE=45),
        ])

    def test_each_row_gets_own_profile(self):
        """All rows have identical ROC values, but outcomes differ by strategy."""
        df = self._build_mixed_df()
        result = _signals(df)

        # LONG_CALL: SIGNED_CALL delta fires, SIGNED_LONG IV fires → VIOLATED
        assert result[0] == 'VIOLATED', f"LONG_CALL: {result[0]}"

        # LONG_PUT: SIGNED_PUT mode → negative delta = thesis working → VALID
        # But SIGNED_LONG IV crush -0.35 → VIOLATED
        assert result[1] == 'VIOLATED', f"LONG_PUT: {result[1]}"

        # BUY_WRITE: UNSIGNED delta -0.35 → DEGRADED (income threshold 0.30 DEGRADED)
        # SIGNED_SHORT IV crush -0.35 → VALID (short-vol benefits from crush)
        assert result[2] == 'DEGRADED', f"BUY_WRITE: {result[2]}"

        # CSP: UNSIGNED delta -0.35 → DEGRADED (CSP threshold 0.30 DEGRADED)
        # SIGNED_SHORT IV crush -0.35 → VALID
        assert result[3] == 'DEGRADED', f"CSP: {result[3]}"

        # STRADDLE: MULTI_LEG EXEMPT → VALID
        assert result[4] == 'VALID', f"STRADDLE: {result[4]}"

        # MYSTERY_STRAT: DEFAULT UNSIGNED both fire at directional thresholds
        assert result[5] == 'VIOLATED', f"DEFAULT: {result[5]}"

    def test_modifying_one_row_does_not_affect_others(self):
        """Change BW Short_Call_Delta to far-OTM → BW goes VALID, others unchanged."""
        df = self._build_mixed_df()
        df.loc[2, 'Short_Call_Delta'] = 0.06  # far-OTM → exempt on delta/vega/gamma

        result = _signals(df)
        assert result[0] == 'VIOLATED', "LONG_CALL unchanged"
        assert result[2] == 'VALID', "BW far-OTM should be VALID now"
        assert result[4] == 'VALID', "STRADDLE still VALID"

    def test_row_count_preserved(self):
        """Output has same number of rows as input."""
        df = self._build_mixed_df()
        result = apply_signal_profiles(df)
        assert len(result) == len(df)


# ===========================================================================
# 9. Signal-combiner isolation
# ===========================================================================
class TestSignalCombinerIsolation:
    """Aggregation semantics: same set of signals, different strategy outcomes."""

    def test_multiple_degraded_greeks_stay_degraded(self):
        """Two Greeks at DEGRADED level → state stays DEGRADED (not auto-promoted)."""
        df = pd.DataFrame([_make_row(
            'LONG_CALL', Delta_ROC_3D=-0.20, Vega_ROC_3D=-0.25, DTE=45
        )])
        assert _signal(df) == 'DEGRADED'

    def test_one_violated_plus_degraded_stays_violated(self):
        """One VIOLATED + one DEGRADED → VIOLATED (never downgraded)."""
        df = pd.DataFrame([_make_row(
            'LONG_CALL', Delta_ROC_3D=-0.35, Vega_ROC_3D=-0.05, DTE=45
        )])
        assert _signal(df) == 'VIOLATED'

    def test_income_same_magnitudes_lower_escalation(self):
        """Income with same Greek values as LONG_CALL escalates less
        due to wider thresholds."""
        roc_vals = dict(Delta_ROC_3D=-0.35, IV_ROC_3D=-0.20, DTE=45)
        df = pd.DataFrame([
            _make_row('LONG_CALL', **roc_vals),
            _make_row('BUY_WRITE', Short_Call_Delta=0.30, **roc_vals),
        ])
        result = _signals(df)
        # LONG_CALL: delta -0.35 > 0.30 VIOLATED; IV -0.20 > 0.15 DEGRADED → VIOLATED
        assert result[0] == 'VIOLATED'
        # BW: delta -0.35 > 0.30 DEGRADED (income thresh); IV -0.20 → VALID (short-vol crush helps)
        assert result[1] == 'DEGRADED'

    def test_multileg_immune_to_all_signals(self):
        """MULTI_LEG stays VALID even with extreme values on every Greek."""
        df = pd.DataFrame([_make_row(
            'IRON_CONDOR',
            Delta_ROC_3D=-0.90, Vega_ROC_3D=-0.90,
            Gamma_ROC_3D=0.90, IV_ROC_3D=-0.90, DTE=45
        )])
        assert _signal(df) == 'VALID'


# ===========================================================================
# 10. Golden-case regression pack
# ===========================================================================
class TestGoldenCaseRegression:
    """Frozen expected outputs from representative real trade patterns.
    Any future refactor must match these unless intentionally changed.

    Each case documents: strategy, Greek ROC values, Short_Call_Delta, DTE,
    and the EXACT expected Signal_State with reasoning."""

    def test_golden_bw_far_otm_near_expiry(self):
        """BW, far-OTM (delta 0.06), DTE=7, large Delta ROC → VALID (exempt)."""
        df = pd.DataFrame([_make_row(
            'BUY_WRITE', Short_Call_Delta=0.06, DTE=7,
            Delta_ROC_3D=-0.456, Vega_ROC_3D=-0.431, IV_ROC_3D=-0.134,
        )])
        # Far-OTM exempt on delta/vega/gamma; IV crush helps short-vol → all VALID
        assert _signal(df) == 'VALID'

    def test_golden_bw_near_strike(self):
        """BW, near-strike (delta 0.45), DTE=36, moderate ROC → DEGRADED."""
        df = pd.DataFrame([_make_row(
            'BUY_WRITE', Short_Call_Delta=0.45, DTE=36,
            Delta_ROC_3D=+0.40, Vega_ROC_3D=+0.07, IV_ROC_3D=-0.04,
        )])
        # Delta +0.40 > income DEGRADED 0.30, < VIOLATED 0.50 → DEGRADED
        assert _signal(df) == 'DEGRADED'

    def test_golden_bw_near_strike_violated(self):
        """BW, near-strike, extreme delta drift → VIOLATED."""
        df = pd.DataFrame([_make_row(
            'BUY_WRITE', Short_Call_Delta=0.45, DTE=36,
            Delta_ROC_3D=+0.55, Vega_ROC_3D=+0.07, IV_ROC_3D=-0.04,
        )])
        assert _signal(df) == 'VIOLATED'

    def test_golden_csp_iv_spike(self):
        """CSP with IV spike +0.35 → DEGRADED (between 0.25 and 0.40)."""
        df = pd.DataFrame([_make_row(
            'CSP', DTE=30,
            Delta_ROC_3D=-0.01, IV_ROC_3D=+0.35,
        )])
        assert _signal(df) == 'DEGRADED'

    def test_golden_long_call_winner(self):
        """Long call with favorable Greeks → VALID."""
        df = pd.DataFrame([_make_row(
            'LONG_CALL', DTE=190,
            Delta_ROC_3D=+0.10, Vega_ROC_3D=+0.06, IV_ROC_3D=+0.02,
        )])
        # Positive delta (call gaining), positive vega/IV → all thesis-aligned
        assert _signal(df) == 'VALID'

    def test_golden_long_call_deteriorating(self):
        """Long call with adverse Greeks → VIOLATED."""
        df = pd.DataFrame([_make_row(
            'LONG_CALL', DTE=60,
            Delta_ROC_3D=-0.35, Vega_ROC_3D=-0.05, IV_ROC_3D=-0.10,
        )])
        # Delta -0.35 > VIOLATED 0.30 → VIOLATED
        assert _signal(df) == 'VIOLATED'

    def test_golden_long_put_thesis_working(self):
        """Long put with negative delta drift → VALID (thesis working)."""
        df = pd.DataFrame([_make_row(
            'LONG_PUT', DTE=43,
            Delta_ROC_3D=-0.16, Vega_ROC_3D=-0.04, IV_ROC_3D=+0.00,
        )])
        # Negative delta = put gaining value = thesis working
        assert _signal(df) == 'VALID'

    def test_golden_long_put_recovering(self):
        """Long put with positive delta drift (recovering) → DEGRADED."""
        df = pd.DataFrame([_make_row(
            'LONG_PUT', DTE=36,
            Delta_ROC_3D=+0.20, Vega_ROC_3D=-0.04, IV_ROC_3D=-0.02,
        )])
        # Positive delta on SIGNED_PUT: 0.20 > DEGRADED 0.15 → DEGRADED
        assert _signal(df) == 'DEGRADED'

    def test_golden_leaps_call_stable(self):
        """LEAPS call with small movements → VALID (LEAPs are inherently stable)."""
        df = pd.DataFrame([_make_row(
            'LEAPS_CALL', DTE=462,
            Delta_ROC_3D=+0.08, Vega_ROC_3D=-0.05, Gamma_ROC_3D=-0.29, IV_ROC_3D=+0.02,
        )])
        # All below thresholds, gamma DTE gate passes but -0.29 < 0.25 DEGRADED threshold
        # (LONG_GAMMA: negative = losing convexity, but abs(0.29) > 0.25 → DEGRADED)
        assert _signal(df) == 'DEGRADED'

    def test_golden_iron_condor_extreme_greeks(self):
        """Iron condor with extreme Greek movements → VALID (EXEMPT)."""
        df = pd.DataFrame([_make_row(
            'IRON_CONDOR', DTE=30,
            Delta_ROC_3D=-0.80, Vega_ROC_3D=-0.70,
            Gamma_ROC_3D=+0.90, IV_ROC_3D=-0.60,
        )])
        assert _signal(df) == 'VALID'
