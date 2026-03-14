"""
Drift Threshold Calibration — empirical analysis of Greek ROC signals vs outcomes.

Queries management_recommendations + closed_trades to determine:
1. Which signals fire most often by strategy family
2. False positive rate (signal fired → trade was actually fine)
3. Missed signals (no signal fired → trade lost money)
4. Optimal threshold for each Greek ROC by strategy family

Usage:
    python scripts/validation/calibrate_drift_thresholds.py
    python scripts/validation/calibrate_drift_thresholds.py --strategy BUY_WRITE
    python scripts/validation/calibrate_drift_thresholds.py --export csv
"""
from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.indicator_settings import SIGNAL_DRIFT_THRESHOLDS as _T
from core.management.cycle2.drift.signal_profiles import (
    PROFILES, DEFAULT_PROFILE, get_profile, SignalProfile, GreekRule,
)

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "pipeline.duckdb"

# Strategy family mapping (same as signal_profiles.py)
FAMILY_MAP = {}
for p in list(PROFILES.values()) + [DEFAULT_PROFILE]:
    for s in p.strategies:
        FAMILY_MAP[s] = p.name


def _get_family(strategy: str) -> str:
    return FAMILY_MAP.get(str(strategy).upper(), 'DEFAULT')


def load_signal_history(con, strategy_filter: Optional[str] = None) -> pd.DataFrame:
    """Load all management snapshots with Greek ROC columns."""
    query = """
        SELECT
            m.TradeID,
            m.Strategy,
            m.Snapshot_TS,
            m.DTE,
            m.Action,
            m.Urgency,
            m.Signal_State,
            m.Delta,
            m.Delta_ROC_3D,
            m.Gamma_ROC_3D,
            m.Vega_ROC_3D,
            m.IV_ROC_3D,
            m.ROC_Persist_3D,
            m.Drift_Direction,
            m.Drift_Magnitude,
            m.Doctrine_Source,
            m.Rationale
        FROM management_recommendations m
        WHERE m.Delta_ROC_3D IS NOT NULL
    """
    if strategy_filter:
        query += f" AND UPPER(m.Strategy) LIKE '%{strategy_filter.upper()}%'"
    query += " ORDER BY m.Snapshot_TS"

    df = con.execute(query).df()
    df['Strategy_Family'] = df['Strategy'].apply(_get_family)
    return df


def load_closed_trades(con) -> pd.DataFrame:
    """Load closed trade outcomes."""
    query = """
        SELECT
            TradeID,
            Strategy,
            PnL_Pct,
            PnL_Dollar,
            Days_Held,
            MFE_Pct,
            MAE_Pct,
            Outcome_Type,
            Gate_Failed,
            Gate_Fired,
            Entry_TS,
            Exit_TS
        FROM closed_trades
        WHERE PnL_Pct IS NOT NULL
    """
    return con.execute(query).df()


def compute_signal_distributions(df: pd.DataFrame) -> pd.DataFrame:
    """Compute distribution stats for each Greek ROC by strategy family."""
    greeks = ['Delta_ROC_3D', 'Vega_ROC_3D', 'Gamma_ROC_3D', 'IV_ROC_3D']
    rows = []

    for family in sorted(df['Strategy_Family'].unique()):
        fam_df = df[df['Strategy_Family'] == family]
        profile = get_profile(next(iter(PROFILES.get(family, DEFAULT_PROFILE).strategies), ''))
        n = len(fam_df)

        for greek in greeks:
            vals = pd.to_numeric(fam_df[greek], errors='coerce').dropna()
            if vals.empty:
                continue

            rule: GreekRule = getattr(profile, greek.replace('_ROC_3D', '_roc').replace('IV_ROC_3D', 'iv_roc').lower(), None)
            if rule is None:
                # Manual mapping
                attr_map = {
                    'Delta_ROC_3D': 'delta_roc',
                    'Vega_ROC_3D': 'vega_roc',
                    'Gamma_ROC_3D': 'gamma_roc',
                    'IV_ROC_3D': 'iv_roc',
                }
                rule = getattr(profile, attr_map[greek])

            rows.append({
                'Family': family,
                'Greek': greek,
                'Mode': rule.mode,
                'N': len(vals),
                'Mean': vals.mean(),
                'Std': vals.std(),
                'P5': vals.quantile(0.05),
                'P25': vals.quantile(0.25),
                'P50': vals.quantile(0.50),
                'P75': vals.quantile(0.75),
                'P95': vals.quantile(0.95),
                'Min': vals.min(),
                'Max': vals.max(),
                'Current_DEGRADED': rule.degraded,
                'Current_VIOLATED': rule.violated,
                'Pct_Would_Fire_DEGRADED': _pct_would_fire(vals, rule, 'DEGRADED'),
                'Pct_Would_Fire_VIOLATED': _pct_would_fire(vals, rule, 'VIOLATED'),
            })

    return pd.DataFrame(rows)


def _pct_would_fire(vals: pd.Series, rule: GreekRule, level: str) -> float:
    """What % of observations would fire at this level given the mode."""
    if rule.mode == 'EXEMPT':
        return 0.0
    thresh = rule.degraded if level == 'DEGRADED' else rule.violated
    if rule.mode in ('SIGNED_CALL', 'SIGNED_LONG', 'LONG_GAMMA'):
        return (vals < -thresh).mean() * 100
    elif rule.mode in ('SIGNED_PUT', 'SIGNED_SHORT', 'SHORT_GAMMA'):
        return (vals > thresh).mean() * 100
    else:  # UNSIGNED
        return ((vals.abs() > thresh).mean()) * 100


def compute_signal_vs_outcome(signals_df: pd.DataFrame, closed_df: pd.DataFrame) -> pd.DataFrame:
    """Cross-reference fired signals with trade outcomes."""
    if closed_df.empty:
        return pd.DataFrame()

    # For each closed trade, find the LAST signal snapshot before exit
    rows = []
    for _, trade in closed_df.iterrows():
        tid = trade['TradeID']
        trade_signals = signals_df[signals_df['TradeID'] == tid].copy()
        if trade_signals.empty:
            continue

        # Get last 5 snapshots (most recent signal state leading up to exit)
        last_signals = trade_signals.tail(5)
        last_row = trade_signals.iloc[-1]

        # Did any signal fire DEGRADED or VIOLATED?
        any_degraded = (last_signals['Signal_State'] == 'DEGRADED').any()
        any_violated = (last_signals['Signal_State'] == 'VIOLATED').any()

        # What was the outcome?
        pnl = trade['PnL_Pct']
        was_loss = pnl < -5  # >5% loss
        was_win = pnl > 5    # >5% gain

        # Classification
        if any_violated and was_loss:
            classification = 'TRUE_POSITIVE'   # Signal fired, trade lost
        elif any_violated and not was_loss:
            classification = 'FALSE_POSITIVE'  # Signal fired, trade was OK
        elif not any_violated and was_loss:
            classification = 'MISSED_SIGNAL'   # No signal, trade lost
        else:
            classification = 'TRUE_NEGATIVE'   # No signal, trade was OK

        rows.append({
            'TradeID': tid,
            'Strategy': trade.get('Strategy', last_row.get('Strategy', '')),
            'Family': _get_family(str(trade.get('Strategy', last_row.get('Strategy', '')))),
            'PnL_Pct': pnl,
            'Days_Held': trade.get('Days_Held', None),
            'Final_Signal_State': last_row.get('Signal_State', 'VALID'),
            'Final_Action': last_row.get('Action', ''),
            'Any_DEGRADED': any_degraded,
            'Any_VIOLATED': any_violated,
            'Classification': classification,
            'Delta_ROC_3D': last_row.get('Delta_ROC_3D', None),
            'Vega_ROC_3D': last_row.get('Vega_ROC_3D', None),
            'Gamma_ROC_3D': last_row.get('Gamma_ROC_3D', None),
            'IV_ROC_3D': last_row.get('IV_ROC_3D', None),
            'ROC_Persist_3D': last_row.get('ROC_Persist_3D', None),
            'DTE': last_row.get('DTE', None),
            'Outcome_Type': trade.get('Outcome_Type', ''),
        })

    return pd.DataFrame(rows)


def compute_threshold_sweep(signals_df: pd.DataFrame, closed_df: pd.DataFrame) -> pd.DataFrame:
    """Sweep thresholds to find optimal fire rate vs accuracy tradeoff."""
    if closed_df.empty:
        return pd.DataFrame()

    greeks = ['Delta_ROC_3D', 'Vega_ROC_3D', 'Gamma_ROC_3D', 'IV_ROC_3D']

    # Build per-trade last-snapshot + outcome
    trade_data = []
    for _, trade in closed_df.iterrows():
        tid = trade['TradeID']
        trade_signals = signals_df[signals_df['TradeID'] == tid]
        if trade_signals.empty:
            continue
        last = trade_signals.iloc[-1]
        trade_data.append({
            'TradeID': tid,
            'Strategy': str(trade.get('Strategy', last.get('Strategy', ''))).upper(),
            'PnL_Pct': trade['PnL_Pct'],
            **{g: last.get(g, None) for g in greeks},
        })

    if not trade_data:
        return pd.DataFrame()

    tdf = pd.DataFrame(trade_data)
    tdf['Family'] = tdf['Strategy'].apply(_get_family)
    tdf['Was_Loss'] = tdf['PnL_Pct'] < -5

    rows = []
    for family in sorted(tdf['Family'].unique()):
        fam = tdf[tdf['Family'] == family]
        profile = get_profile(next(iter(PROFILES.get(family, DEFAULT_PROFILE).strategies), ''))
        n_trades = len(fam)
        if n_trades < 3:
            continue

        for greek in greeks:
            attr_map = {
                'Delta_ROC_3D': 'delta_roc',
                'Vega_ROC_3D': 'vega_roc',
                'Gamma_ROC_3D': 'gamma_roc',
                'IV_ROC_3D': 'iv_roc',
            }
            rule: GreekRule = getattr(profile, attr_map[greek])
            if rule.mode == 'EXEMPT':
                continue

            vals = pd.to_numeric(fam[greek], errors='coerce')
            losses = fam['Was_Loss']

            # Sweep thresholds
            for thresh in [0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.40, 0.50]:
                if rule.mode in ('SIGNED_CALL', 'SIGNED_LONG', 'LONG_GAMMA'):
                    fired = vals < -thresh
                elif rule.mode in ('SIGNED_PUT', 'SIGNED_SHORT', 'SHORT_GAMMA'):
                    fired = vals > thresh
                else:
                    fired = vals.abs() > thresh

                fired = fired.fillna(False)
                n_fired = fired.sum()
                if n_fired == 0:
                    tp, fp, precision = 0, 0, 0
                else:
                    tp = (fired & losses).sum()
                    fp = (fired & ~losses).sum()
                    precision = tp / n_fired * 100 if n_fired > 0 else 0

                n_missed = (~fired & losses).sum()
                n_losses = losses.sum()
                recall = (tp / n_losses * 100) if n_losses > 0 else 100

                rows.append({
                    'Family': family,
                    'Greek': greek,
                    'Mode': rule.mode,
                    'Threshold': thresh,
                    'N_Trades': n_trades,
                    'N_Fired': int(n_fired),
                    'Fire_Rate_Pct': n_fired / n_trades * 100,
                    'True_Positives': int(tp),
                    'False_Positives': int(fp),
                    'Missed_Losses': int(n_missed),
                    'Precision_Pct': precision,
                    'Recall_Pct': recall,
                    'Current_Thresh': rule.degraded,
                    'Is_Current': abs(thresh - rule.degraded) < 0.01,
                })

    return pd.DataFrame(rows)


def print_report(dist_df: pd.DataFrame, outcome_df: pd.DataFrame,
                 sweep_df: pd.DataFrame, signals_df: pd.DataFrame) -> None:
    """Print calibration report to stdout."""
    print("=" * 80)
    print("DRIFT THRESHOLD CALIBRATION REPORT")
    print("=" * 80)

    # Overview
    n_snapshots = len(signals_df)
    n_families = signals_df['Strategy_Family'].nunique()
    print(f"\nData: {n_snapshots:,} management snapshots across {n_families} strategy families")
    print(f"Closed trades for outcome analysis: {len(outcome_df)}")

    # Signal state distribution
    print("\n--- SIGNAL STATE DISTRIBUTION ---")
    state_dist = signals_df.groupby(['Strategy_Family', 'Signal_State']).size().unstack(fill_value=0)
    if not state_dist.empty:
        # Add percentage columns
        state_dist['Total'] = state_dist.sum(axis=1)
        for col in ['VALID', 'DEGRADED', 'VIOLATED']:
            if col in state_dist.columns:
                state_dist[f'{col}_%'] = (state_dist[col] / state_dist['Total'] * 100).round(1)
        print(state_dist.to_string())

    # Greek ROC distributions
    print("\n--- GREEK ROC DISTRIBUTIONS BY FAMILY ---")
    if not dist_df.empty:
        for family in sorted(dist_df['Family'].unique()):
            print(f"\n  [{family}]")
            fam = dist_df[dist_df['Family'] == family]
            for _, row in fam.iterrows():
                fire_d = row['Pct_Would_Fire_DEGRADED']
                fire_v = row['Pct_Would_Fire_VIOLATED']
                print(f"    {row['Greek']:20s} mode={row['Mode']:15s}  "
                      f"mean={row['Mean']:+.3f}  std={row['Std']:.3f}  "
                      f"[P5={row['P5']:+.3f} P95={row['P95']:+.3f}]  "
                      f"fire_D={fire_d:.1f}%  fire_V={fire_v:.1f}%")

    # Outcome cross-reference
    if not outcome_df.empty:
        print("\n--- SIGNAL vs OUTCOME (Closed Trades) ---")
        class_counts = outcome_df['Classification'].value_counts()
        total = len(outcome_df)
        for cls, cnt in class_counts.items():
            print(f"  {cls:20s}: {cnt:3d} ({cnt/total*100:.0f}%)")

        # Per-family breakdown
        print("\n  Per-family:")
        for family in sorted(outcome_df['Family'].unique()):
            fam = outcome_df[outcome_df['Family'] == family]
            n = len(fam)
            tp = (fam['Classification'] == 'TRUE_POSITIVE').sum()
            fp = (fam['Classification'] == 'FALSE_POSITIVE').sum()
            missed = (fam['Classification'] == 'MISSED_SIGNAL').sum()
            tn = (fam['Classification'] == 'TRUE_NEGATIVE').sum()
            precision = tp / (tp + fp) * 100 if (tp + fp) > 0 else 0
            recall = tp / (tp + missed) * 100 if (tp + missed) > 0 else 0
            print(f"    {family:25s}: n={n:2d}  TP={tp} FP={fp} Miss={missed} TN={tn}  "
                  f"precision={precision:.0f}% recall={recall:.0f}%")

        # Show individual missed signals and false positives
        print("\n  FALSE POSITIVES (signal fired, trade was OK):")
        fps = outcome_df[outcome_df['Classification'] == 'FALSE_POSITIVE']
        for _, row in fps.iterrows():
            print(f"    {row['TradeID'][:30]:30s} {row['Family']:20s} "
                  f"PnL={row['PnL_Pct']:+.1f}%  Signal={row['Final_Signal_State']}  "
                  f"DeltaROC={row.get('Delta_ROC_3D', '?')}")

        print("\n  MISSED SIGNALS (no signal, trade lost >5%):")
        misses = outcome_df[outcome_df['Classification'] == 'MISSED_SIGNAL']
        for _, row in misses.iterrows():
            print(f"    {row['TradeID'][:30]:30s} {row['Family']:20s} "
                  f"PnL={row['PnL_Pct']:+.1f}%  DeltaROC={row.get('Delta_ROC_3D', '?')}  "
                  f"VegaROC={row.get('Vega_ROC_3D', '?')}")

    # Threshold sweep
    if not sweep_df.empty:
        print("\n--- THRESHOLD SWEEP (Precision/Recall at different thresholds) ---")
        for family in sorted(sweep_df['Family'].unique()):
            print(f"\n  [{family}]")
            fam = sweep_df[sweep_df['Family'] == family]
            for greek in sorted(fam['Greek'].unique()):
                g = fam[fam['Greek'] == greek]
                if g.empty:
                    continue
                print(f"    {greek}  (mode={g.iloc[0]['Mode']})")
                for _, row in g.iterrows():
                    marker = " ← CURRENT" if row['Is_Current'] else ""
                    print(f"      thresh={row['Threshold']:.2f}  "
                          f"fire={row['Fire_Rate_Pct']:5.1f}%  "
                          f"TP={row['True_Positives']}  FP={row['False_Positives']}  "
                          f"miss={row['Missed_Losses']}  "
                          f"prec={row['Precision_Pct']:5.1f}%  "
                          f"recall={row['Recall_Pct']:5.1f}%{marker}")

    # Recommendations
    if not sweep_df.empty:
        print("\n--- THRESHOLD RECOMMENDATIONS ---")
        for family in sorted(sweep_df['Family'].unique()):
            fam = sweep_df[sweep_df['Family'] == family]
            for greek in sorted(fam['Greek'].unique()):
                g = fam[fam['Greek'] == greek]
                if g.empty:
                    continue
                # Find threshold with best F1 score (balance precision and recall)
                g = g.copy()
                g['F1'] = 2 * (g['Precision_Pct'] * g['Recall_Pct']) / (
                    g['Precision_Pct'] + g['Recall_Pct']).replace(0, 1)
                best = g.loc[g['F1'].idxmax()]
                current = g[g['Is_Current']]
                if not current.empty:
                    curr = current.iloc[0]
                    if abs(best['Threshold'] - curr['Threshold']) > 0.04:
                        direction = "TIGHTEN" if best['Threshold'] < curr['Threshold'] else "LOOSEN"
                        print(f"  {family:25s} {greek:20s}: {direction} "
                              f"{curr['Threshold']:.2f} → {best['Threshold']:.2f}  "
                              f"(F1: {curr['F1']:.0f} → {best['F1']:.0f})")
                    else:
                        print(f"  {family:25s} {greek:20s}: KEEP {curr['Threshold']:.2f}  "
                              f"(F1={curr['F1']:.0f})")

    print("\n" + "=" * 80)


def main():
    parser = argparse.ArgumentParser(description="Calibrate drift signal thresholds")
    parser.add_argument('--strategy', type=str, help='Filter by strategy name')
    parser.add_argument('--export', choices=['csv'], help='Export results to CSV')
    args = parser.parse_args()

    import duckdb
    if not DB_PATH.exists():
        print(f"ERROR: Database not found at {DB_PATH}")
        sys.exit(1)

    con = duckdb.connect(str(DB_PATH), read_only=True)

    # Check table exists
    tables = con.execute("SHOW TABLES").df()['name'].tolist()
    if 'management_recommendations' not in tables:
        print("ERROR: management_recommendations table not found")
        sys.exit(1)

    print("Loading signal history...")
    signals_df = load_signal_history(con, args.strategy)
    print(f"  → {len(signals_df):,} snapshots loaded")

    print("Loading closed trades...")
    closed_df = load_closed_trades(con) if 'closed_trades' in tables else pd.DataFrame()
    print(f"  → {len(closed_df)} closed trades loaded")

    print("Computing distributions...")
    dist_df = compute_signal_distributions(signals_df)

    print("Cross-referencing signals vs outcomes...")
    outcome_df = compute_signal_vs_outcome(signals_df, closed_df) if not closed_df.empty else pd.DataFrame()

    print("Running threshold sweep...")
    sweep_df = compute_threshold_sweep(signals_df, closed_df) if not closed_df.empty else pd.DataFrame()

    con.close()

    print_report(dist_df, outcome_df, sweep_df, signals_df)

    if args.export == 'csv':
        out_dir = Path(__file__).resolve().parents[2] / "output"
        out_dir.mkdir(exist_ok=True)
        dist_df.to_csv(out_dir / "drift_calibration_distributions.csv", index=False)
        if not outcome_df.empty:
            outcome_df.to_csv(out_dir / "drift_calibration_outcomes.csv", index=False)
        if not sweep_df.empty:
            sweep_df.to_csv(out_dir / "drift_calibration_sweep.csv", index=False)
        print(f"\nExported to {out_dir}/drift_calibration_*.csv")


if __name__ == '__main__':
    main()
