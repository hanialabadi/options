"""
Signal Profiles — strategy-aware Greek ROC thresholds for drift engine.

Each strategy family declares HOW each Greek ROC signal should be interpreted.
The drift engine loops over profiles instead of inline if/else chains.
Adding a new strategy = adding a new profile entry — no engine changes needed.

Profile modes:
  SIGNED_LONG:   negative ROC = deterioration (crush/falling sensitivity)
  SIGNED_SHORT:  positive ROC = deterioration (spike/rising exposure against you)
  SIGNED_CALL:   negative ROC = deterioration (delta falling on long call)
  SIGNED_PUT:    positive ROC = deterioration (delta recovering on long put)
  UNSIGNED:      abs(ROC) = deterioration (direction-agnostic)
  EXEMPT:        no signal — strategy is neutral to this Greek
  SHORT_GAMMA:   positive Gamma ROC = deterioration (acceleration against short-gamma)
  LONG_GAMMA:    negative Gamma ROC = deterioration (losing convexity)

Far-OTM income exemption (Short_Call_Delta < 0.15) is handled at the profile
level — income profiles declare `far_otm_exempt: True`, and the engine checks
the delta threshold before applying any escalation.

RAG basis:
  Natenberg Ch.7: vega exposure is directional (long-vol vs short-vol).
  Passarelli Ch.2: Greek drift relative to thesis direction.
  McMillan Ch.3-4: direction alignment, income follow-up depends on strike relationship.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, Optional, Set

import numpy as np
import pandas as pd

from config.indicator_settings import SIGNAL_DRIFT_THRESHOLDS

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Profile dataclass
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class GreekRule:
    """How a single Greek ROC signal fires for a strategy family."""
    mode: str             # SIGNED_LONG, SIGNED_SHORT, SIGNED_CALL, SIGNED_PUT,
                          # UNSIGNED, EXEMPT, SHORT_GAMMA, LONG_GAMMA
    degraded: float       # threshold for DEGRADED (applied per mode sign convention)
    violated: float       # threshold for VIOLATED
    far_otm_exempt: bool = False   # suppress when Short_Call_Delta < far_otm_threshold
    dte_gate: Optional[float] = None  # only fire when DTE > this (gamma expiry spike)


@dataclass(frozen=True)
class SignalProfile:
    """Complete signal sensitivity profile for a strategy family."""
    name: str
    strategies: frozenset   # strategy names that map to this profile
    delta_roc: GreekRule
    vega_roc: GreekRule
    gamma_roc: GreekRule
    iv_roc: GreekRule


# ---------------------------------------------------------------------------
# Thresholds from config (single source of truth)
# ---------------------------------------------------------------------------
_T = SIGNAL_DRIFT_THRESHOLDS

FAR_OTM_DELTA_THRESHOLD = _T["FAR_OTM_DELTA_THRESHOLD"]  # 0.15


# ---------------------------------------------------------------------------
# Profile definitions
# ---------------------------------------------------------------------------
PROFILES: Dict[str, SignalProfile] = {}


def _register(p: SignalProfile) -> None:
    PROFILES[p.name] = p


_register(SignalProfile(
    name='LONG_CALL',
    strategies=frozenset({'LONG_CALL', 'BUY_CALL', 'LEAPS_CALL'}),
    delta_roc=GreekRule('SIGNED_CALL',  _T['DELTA_ROC_DEGRADED'], _T['DELTA_ROC_VIOLATED']),
    vega_roc=GreekRule('SIGNED_LONG',   _T['VEGA_ROC_DEGRADED'],  _T['VEGA_ROC_VIOLATED']),
    gamma_roc=GreekRule('LONG_GAMMA',   _T['GAMMA_ROC_DEGRADED'], _T['GAMMA_ROC_VIOLATED'],
                        dte_gate=_T['GAMMA_DTE_GATE']),
    iv_roc=GreekRule('SIGNED_LONG',     _T['IV_ROC_DEGRADED'],    _T['IV_ROC_VIOLATED']),
))

_register(SignalProfile(
    name='LONG_PUT',
    strategies=frozenset({'LONG_PUT', 'BUY_PUT', 'LEAPS_PUT'}),
    delta_roc=GreekRule('SIGNED_PUT',   _T['DELTA_ROC_DEGRADED'], _T['DELTA_ROC_VIOLATED']),
    vega_roc=GreekRule('SIGNED_LONG',   _T['VEGA_ROC_DEGRADED'],  _T['VEGA_ROC_VIOLATED']),
    gamma_roc=GreekRule('LONG_GAMMA',   _T['GAMMA_ROC_DEGRADED'], _T['GAMMA_ROC_VIOLATED'],
                        dte_gate=_T['GAMMA_DTE_GATE']),
    iv_roc=GreekRule('SIGNED_LONG',     _T['IV_ROC_DEGRADED'],    _T['IV_ROC_VIOLATED']),
))

_register(SignalProfile(
    name='INCOME_SHORT_CALL',
    strategies=frozenset({'BUY_WRITE', 'COVERED_CALL', 'PMCC'}),
    delta_roc=GreekRule('UNSIGNED',     _T['INCOME_DELTA_ROC_DEGRADED'], _T['INCOME_DELTA_ROC_VIOLATED'],
                        far_otm_exempt=True),
    vega_roc=GreekRule('SIGNED_SHORT',  _T['VEGA_ROC_DEGRADED'],  _T['VEGA_ROC_VIOLATED'],
                       far_otm_exempt=True),
    gamma_roc=GreekRule('SHORT_GAMMA',  _T['GAMMA_ROC_DEGRADED'], _T['GAMMA_ROC_VIOLATED'],
                        far_otm_exempt=True, dte_gate=_T['GAMMA_DTE_GATE']),
    iv_roc=GreekRule('SIGNED_SHORT',    _T['INCOME_IV_ROC_DEGRADED'], _T['INCOME_IV_ROC_VIOLATED']),
))

_register(SignalProfile(
    name='CSP',
    strategies=frozenset({'CSP', 'SHORT_PUT'}),
    delta_roc=GreekRule('UNSIGNED',     _T['CSP_DELTA_ROC_DEGRADED'], _T['CSP_DELTA_ROC_VIOLATED']),
    vega_roc=GreekRule('SIGNED_SHORT',  _T['VEGA_ROC_DEGRADED'],  _T['VEGA_ROC_VIOLATED']),
    gamma_roc=GreekRule('SHORT_GAMMA',  _T['GAMMA_ROC_DEGRADED'], _T['GAMMA_ROC_VIOLATED'],
                        dte_gate=_T['GAMMA_DTE_GATE']),
    iv_roc=GreekRule('SIGNED_SHORT',    _T['CSP_IV_ROC_DEGRADED'], _T['CSP_IV_ROC_VIOLATED']),
))

_register(SignalProfile(
    name='MULTI_LEG',
    strategies=frozenset({'SPREAD', 'STRADDLE', 'STRANGLE', 'BUTTERFLY',
                          'IRON_CONDOR', 'VERTICAL_SPREAD', 'CALENDAR_SPREAD'}),
    delta_roc=GreekRule('EXEMPT', 0, 0),
    vega_roc=GreekRule('EXEMPT', 0, 0),
    gamma_roc=GreekRule('EXEMPT', 0, 0),
    iv_roc=GreekRule('EXEMPT', 0, 0),
))

# Default profile: conservative unsigned logic (backward-compatible fallback)
DEFAULT_PROFILE = SignalProfile(
    name='DEFAULT',
    strategies=frozenset(),
    delta_roc=GreekRule('UNSIGNED', _T['DELTA_ROC_DEGRADED'], _T['DELTA_ROC_VIOLATED']),
    vega_roc=GreekRule('UNSIGNED',  _T['VEGA_ROC_DEGRADED'],  _T['VEGA_ROC_VIOLATED']),
    gamma_roc=GreekRule('EXEMPT', 0, 0),  # unknown structure → don't guess gamma sign
    iv_roc=GreekRule('UNSIGNED',    _T['IV_ROC_DEGRADED'],    _T['IV_ROC_VIOLATED']),
)


# ---------------------------------------------------------------------------
# Strategy → Profile lookup (built once at import)
# ---------------------------------------------------------------------------
_STRATEGY_MAP: Dict[str, SignalProfile] = {}
for _p in PROFILES.values():
    for _s in _p.strategies:
        _STRATEGY_MAP[_s] = _p


def get_profile(strategy: str) -> SignalProfile:
    """Return the signal profile for a strategy name. Falls back to DEFAULT."""
    return _STRATEGY_MAP.get(strategy.upper(), DEFAULT_PROFILE)


# ---------------------------------------------------------------------------
# Profile-driven signal assessment
# ---------------------------------------------------------------------------
def apply_signal_profiles(df: pd.DataFrame) -> pd.DataFrame:
    """Apply strategy-aware Greek ROC signal assessment using profiles.

    Replaces the inline if/else chains in assess_signal_drift() with a
    declarative, profile-driven loop. Same escalation semantics:
    - VALID → DEGRADED → VIOLATED (escalate only, never downgrade)
    - Persistence gate: ROC_Persist_3D >= 2 for DEGRADED, >= 1 for VIOLATED
    - Far-OTM income exemption: Short_Call_Delta < threshold → suppress signal
    - DTE gate: gamma ROC suppressed when DTE <= gate (mechanical expiry spike)

    Tier 1 (PCS drift) and Tier 2 (Delta_1D_DriftTail) are NOT profile-driven —
    they remain simple blanket rules in assess_signal_drift(). Only Tier 3
    (Greek ROC: delta, vega, gamma, IV) is refactored here.
    """
    df['Signal_State'] = df.get('Signal_State', 'VALID')

    # Pre-compute shared series
    _strat = (df['Strategy'].fillna('').str.upper()
              if 'Strategy' in df.columns
              else pd.Series('', index=df.index))

    # Short_Call_Delta is the short call's own delta (0.30 = 30-delta call).
    # Do NOT fall back to position Delta — that's net of stock + option,
    # typically ~0.10 for BW, which falsely triggers far-OTM exemption.
    _sc_delta = pd.to_numeric(
        df.get('Short_Call_Delta', pd.Series(0.5, index=df.index)),
        errors='coerce'
    ).fillna(0.5).abs()

    _dte = pd.to_numeric(df.get('DTE', pd.Series(0, index=df.index)),
                         errors='coerce').fillna(0)

    # Persistence gates
    # ROC_Persist_3D counts consecutive daily position-delta declines.
    # Delta_ROC_3D is already a 3-day window — requiring additional daily
    # persistence on top of a 3D ROC double-counts and suppresses legitimate
    # signals (54% of snapshots had persist=0, blocking ALL escalation).
    # Fix: VIOLATED needs no extra persistence (3D ROC IS the persistence).
    # DEGRADED needs >= 1 day of confirmation (mild signal, one-day filter).
    if 'ROC_Persist_3D' in df.columns:
        _persist = pd.to_numeric(df['ROC_Persist_3D'], errors='coerce').fillna(0)
        _persist_degraded = _persist >= 1
        _persist_violated = pd.Series(True, index=df.index)
    else:
        _persist_degraded = pd.Series(True, index=df.index)
        _persist_violated = pd.Series(True, index=df.index)

    def _escalate(mask: pd.Series, target: str) -> None:
        _already_violated = df['Signal_State'] == 'VIOLATED'
        if target == 'DEGRADED':
            df.loc[mask & _persist_degraded & ~_already_violated, 'Signal_State'] = 'DEGRADED'
        elif target == 'VIOLATED':
            df.loc[mask & _persist_violated, 'Signal_State'] = 'VIOLATED'

    # Map each row to its profile
    _profile_names = _strat.map(lambda s: get_profile(s).name)

    # Column mapping for each Greek ROC
    _greek_cols = {
        'delta_roc': 'Delta_ROC_3D',
        'vega_roc':  'Vega_ROC_3D',
        'gamma_roc': 'Gamma_ROC_3D',
        'iv_roc':    'IV_ROC_3D',
    }

    # Process each profile × each Greek
    for profile in list(PROFILES.values()) + [DEFAULT_PROFILE]:
        _mask = _profile_names == profile.name
        if not _mask.any():
            continue

        for greek_attr, col_name in _greek_cols.items():
            if col_name not in df.columns:
                continue

            rule: GreekRule = getattr(profile, greek_attr)
            if rule.mode == 'EXEMPT':
                continue

            _val = df[col_name]

            # DTE gate (gamma expiry spike suppression)
            _dte_ok = pd.Series(True, index=df.index)
            if rule.dte_gate is not None:
                _dte_ok = _dte > rule.dte_gate

            # Far-OTM income exemption
            _not_exempt = pd.Series(True, index=df.index)
            if rule.far_otm_exempt:
                _not_exempt = _sc_delta >= FAR_OTM_DELTA_THRESHOLD

            _base = _mask & _dte_ok & _not_exempt

            # Apply mode-specific sign convention
            if rule.mode == 'SIGNED_CALL':
                # Negative ROC = deterioration (delta falling on long call)
                _escalate(_base & (_val < -rule.degraded), 'DEGRADED')
                _escalate(_base & (_val < -rule.violated), 'VIOLATED')
            elif rule.mode == 'SIGNED_PUT':
                # Positive ROC = deterioration (delta recovering toward zero on long put)
                _escalate(_base & (_val > rule.degraded), 'DEGRADED')
                _escalate(_base & (_val > rule.violated), 'VIOLATED')
            elif rule.mode == 'SIGNED_LONG':
                # Negative ROC = deterioration (IV crush / vega crush hurts long-vol)
                _escalate(_base & (_val < -rule.degraded), 'DEGRADED')
                _escalate(_base & (_val < -rule.violated), 'VIOLATED')
            elif rule.mode == 'SIGNED_SHORT':
                # Positive ROC = deterioration (IV spike / vega spike hurts short-vol)
                _escalate(_base & (_val > rule.degraded), 'DEGRADED')
                _escalate(_base & (_val > rule.violated), 'VIOLATED')
            elif rule.mode == 'SHORT_GAMMA':
                # Rising gamma = acceleration against short-gamma position
                _escalate(_base & (_val > rule.degraded), 'DEGRADED')
                _escalate(_base & (_val > rule.violated), 'VIOLATED')
            elif rule.mode == 'LONG_GAMMA':
                # Falling gamma = losing convexity on long-gamma position
                _escalate(_base & (_val < -rule.degraded), 'DEGRADED')
                _escalate(_base & (_val < -rule.violated), 'VIOLATED')
            elif rule.mode == 'UNSIGNED':
                # Both directions are risks (conservative fallback)
                _escalate(_base & (_val < -rule.degraded), 'DEGRADED')
                _escalate(_base & (_val < -rule.violated), 'VIOLATED')
                # Also check positive direction for unsigned
                _escalate(_base & (_val > rule.degraded), 'DEGRADED')
                _escalate(_base & (_val > rule.violated), 'VIOLATED')

    return df
