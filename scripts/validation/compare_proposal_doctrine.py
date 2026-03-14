"""
A/B comparison: v1 gate cascade vs v2 proposal-based doctrine for all strategies.

Replays historical rows from management_recommendations through both
functions and reports agreement rate, disagreements, and resolution method distribution.

Usage:
    python scripts/validation/compare_proposal_doctrine.py [--days 30] [--verbose] [--strategy COVERED_CALL]
"""

from __future__ import annotations

import argparse
import logging
import math
import sys
from pathlib import Path

import duckdb
import pandas as pd

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from core.management.cycle3.doctrine.strategies.covered_call import (
    covered_call_doctrine,
    covered_call_doctrine_v2,
)
from core.management.cycle3.doctrine.strategies.buy_write import (
    buy_write_doctrine,
    buy_write_doctrine_v2,
)
from core.management.cycle3.doctrine.strategies.long_option import (
    long_option_doctrine,
    long_option_doctrine_v2,
)
from core.management.cycle3.doctrine.strategies.short_put import (
    short_put_doctrine,
    short_put_doctrine_v2,
)
from core.management.cycle3.doctrine.strategies.multi_leg import (
    multi_leg_doctrine,
    multi_leg_doctrine_v2,
)

logger = logging.getLogger(__name__)

# Strategy → (v1_fn, v2_fn) mapping
# Keys match the Strategy column values stored in management_recommendations
STRATEGY_DISPATCH = {
    "COVERED_CALL": (covered_call_doctrine, covered_call_doctrine_v2),
    "BUY_WRITE": (buy_write_doctrine, buy_write_doctrine_v2),
    "BUY_CALL": (long_option_doctrine, long_option_doctrine_v2),
    "BUY_PUT": (long_option_doctrine, long_option_doctrine_v2),
    "LONG_CALL": (long_option_doctrine, long_option_doctrine_v2),
    "LONG_PUT": (long_option_doctrine, long_option_doctrine_v2),
    "LEAPS_CALL": (long_option_doctrine, long_option_doctrine_v2),
    "LEAPS_PUT": (long_option_doctrine, long_option_doctrine_v2),
    "CSP": (short_put_doctrine, short_put_doctrine_v2),
    "STRADDLE": (multi_leg_doctrine, multi_leg_doctrine_v2),
    "STRANGLE": (multi_leg_doctrine, multi_leg_doctrine_v2),
}


def _base_result():
    return {"Action": "HOLD", "Urgency": "LOW", "Rationale": "default"}


def load_rows(con, lookback_days: int = 30, strategy_filter: str | None = None) -> pd.DataFrame:
    """Load rows from management_recommendations."""
    strategies = list(STRATEGY_DISPATCH.keys())
    if strategy_filter:
        strategies = [s for s in strategies if s == strategy_filter.upper()]
        if not strategies:
            print(f"Unknown strategy: {strategy_filter}")
            return pd.DataFrame()

    placeholders = ", ".join(f"'{s}'" for s in strategies)
    df = con.execute(f"""
        SELECT *
        FROM management_recommendations
        WHERE Strategy IN ({placeholders})
          AND Snapshot_TS >= CURRENT_TIMESTAMP::TIMESTAMP - INTERVAL '{lookback_days}' DAY
        ORDER BY Strategy, Snapshot_TS
    """).fetchdf()
    logger.info(f"Loaded {len(df)} rows from last {lookback_days} days")
    return df


def run_comparison(con, lookback_days: int = 30, verbose: bool = False,
                   strategy_filter: str | None = None):
    """Run v1 vs v2 comparison on historical rows."""
    df = load_rows(con, lookback_days, strategy_filter)
    if df.empty:
        print("No matching rows found in management_recommendations.")
        return

    results = []
    errors = 0
    for idx, row in df.iterrows():
        trade_id = row.get("TradeID", "")
        snap_ts = row.get("Snapshot_TS", "")
        strategy = str(row.get("Strategy", "")).upper()

        dispatch = STRATEGY_DISPATCH.get(strategy)
        if not dispatch:
            continue

        v1_fn, v2_fn = dispatch

        try:
            r1_raw = v1_fn(row, _base_result())
            # v1 fire_gate() returns (bool, dict) — unwrap
            r1 = r1_raw[1] if isinstance(r1_raw, tuple) else r1_raw
        except Exception as e:
            logger.warning(f"v1 failed for {trade_id} ({strategy}): {e}")
            r1 = {"Action": "ERROR", "Urgency": "N/A"}
            errors += 1

        try:
            r2 = v2_fn(row, _base_result())
        except Exception as e:
            logger.warning(f"v2 failed for {trade_id} ({strategy}): {e}")
            r2 = {"Action": "ERROR", "Urgency": "N/A"}
            errors += 1

        if r1.get("Action") == "ERROR" or r2.get("Action") == "ERROR":
            continue

        agreement = r1.get("Action") == r2.get("Action")
        urgency_agree = r1.get("Urgency") == r2.get("Urgency")

        # Extract timing adjustment details from v2 rationale
        v2_rationale = r2.get("Rationale", "")
        has_iv_adj = "IV depressed" in v2_rationale
        has_macro_adj = "Macro " in v2_rationale and "discounted" in v2_rationale
        has_squeeze_adj = "Keltner squeeze" in v2_rationale and "discounted" in v2_rationale
        has_debit_adj = "consecutive debit" in v2_rationale
        timing_count = sum([has_iv_adj, has_macro_adj, has_squeeze_adj, has_debit_adj])

        # Recovery state tracking
        v1_doctrine_state = r1.get("Doctrine_State", "")
        v2_doctrine_state = r2.get("Doctrine_State", "")
        v1_recovery = v1_doctrine_state == "RECOVERY_LADDER"
        v2_recovery = v2_doctrine_state == "RECOVERY_LADDER" or r2.get("Resolution_Method", "") == "RECOVERY_LADDER"

        results.append({
            "Strategy": strategy,
            "TradeID": trade_id,
            "Ticker": row.get("Underlying_Ticker", ""),
            "Snapshot_TS": snap_ts,
            "v1_Action": r1.get("Action", ""),
            "v1_Urgency": r1.get("Urgency", ""),
            "v2_Action": r2.get("Action", ""),
            "v2_Urgency": r2.get("Urgency", ""),
            "v2_Resolution": r2.get("Resolution_Method", ""),
            "v2_Proposals": r2.get("Proposals_Considered", 0),
            "v2_Summary": r2.get("Proposals_Summary", ""),
            "v2_Winning_Gate": r2.get("Winning_Gate", ""),
            "Agreement": agreement,
            "Urgency_Agreement": urgency_agree,
            "Timing_Adj_Count": timing_count,
            "Has_IV_Adj": has_iv_adj,
            "Has_Macro_Adj": has_macro_adj,
            "Has_Squeeze_Adj": has_squeeze_adj,
            "Has_Debit_Roll_Adj": has_debit_adj,
            "Consec_Debit_Rolls": int(float(row.get("Trajectory_Consecutive_Debit_Rolls", 0) or 0)) if not math.isnan(float(row.get("Trajectory_Consecutive_Debit_Rolls", 0) or 0)) else 0,
            "v1_Recovery": v1_recovery,
            "v2_Recovery": v2_recovery,
        })

    df_results = pd.DataFrame(results)
    if df_results.empty:
        print("No results to report.")
        return

    # ── Overall Report ────────────────────────────────────────────────────
    total = len(df_results)
    agreed = df_results["Agreement"].sum()
    disagreed = total - agreed

    min_ts = df_results["Snapshot_TS"].min()
    max_ts = df_results["Snapshot_TS"].max()
    n_trades = df_results["TradeID"].nunique()

    title = strategy_filter.upper() if strategy_filter else "All Strategies"
    print(f"\n{'='*70}")
    print(f"  Proposal Doctrine A/B Comparison — {title}")
    print(f"{'='*70}")
    print(f"Period: {min_ts} to {max_ts}")
    print(f"Rows evaluated: {total} across {n_trades} TradeIDs")
    if errors:
        print(f"Errors: {errors}")
    print()
    print(f"Overall agreement: {agreed/total*100:.1f}% ({agreed}/{total})")

    # ── Per-Strategy Breakdown ────────────────────────────────────────────
    print(f"\n{'Strategy':15s} {'Rows':>6s} {'Agree':>6s} {'Rate':>7s} {'AvgProp':>8s}")
    print("-" * 50)
    for strat, grp in df_results.groupby("Strategy"):
        n = len(grp)
        a = grp["Agreement"].sum()
        avg_p = grp["v2_Proposals"].mean()
        print(f"{strat:15s} {n:6d} {a:6.0f} {a/n*100:6.1f}% {avg_p:7.1f}")

    # ── Urgency Agreement ─────────────────────────────────────────────────
    urg_agreed = df_results["Urgency_Agreement"].sum()
    print(f"Urgency agreement: {urg_agreed/total*100:.1f}% ({urg_agreed}/{total})")

    # ── Timing Adjustments ────────────────────────────────────────────────
    n_iv = df_results["Has_IV_Adj"].sum()
    n_macro = df_results["Has_Macro_Adj"].sum()
    n_squeeze = df_results["Has_Squeeze_Adj"].sum()
    n_debit = df_results["Has_Debit_Roll_Adj"].sum()
    n_any_timing = (df_results["Timing_Adj_Count"] > 0).sum()
    print(f"\nTiming adjustments fired: {n_any_timing}/{total} rows ({n_any_timing/total*100:.1f}%)")
    print(f"  IV depressed:     {n_iv:4d}")
    print(f"  Macro proximity:  {n_macro:4d}")
    print(f"  Keltner squeeze:  {n_squeeze:4d}")
    print(f"  Debit roll hist:  {n_debit:4d}")
    avg_debit = df_results["Consec_Debit_Rolls"].mean()
    max_debit = df_results["Consec_Debit_Rolls"].max()
    print(f"  Avg consecutive debit rolls: {avg_debit:.1f} (max: {max_debit})")

    # ── Recovery Ladder ─────────────────────────────────────────────────────
    n_v1_recovery = df_results["v1_Recovery"].sum()
    n_v2_recovery = df_results["v2_Recovery"].sum()
    if n_v1_recovery > 0 or n_v2_recovery > 0:
        print(f"\nRecovery ladder activations:")
        print(f"  v1: {n_v1_recovery:4d} rows")
        print(f"  v2: {n_v2_recovery:4d} rows")
        # Show which positions entered recovery
        for _, grp in df_results[df_results["v2_Recovery"]].groupby(["Strategy", "Ticker"]):
            r = grp.iloc[0]
            print(f"  {r['Strategy']:15s} {r['Ticker']:8s} ({len(grp)} rows)")

    # ── Resolution Method Distribution ────────────────────────────────────
    if "v2_Resolution" in df_results.columns:
        res_dist = df_results["v2_Resolution"].value_counts()
        print(f"\nResolution methods:")
        for method, count in res_dist.items():
            print(f"  {method:25s} {count:4d} ({count/total*100:.1f}%)")

    # ── Disagreements ─────────────────────────────────────────────────────
    if disagreed > 0:
        print(f"\nDisagreements ({disagreed} rows):")
        print(f"{'Strategy':15s} {'TradeID':25s} {'Ticker':8s} {'v1':12s} {'v2':12s} {'Resolution':20s} {'Gate'}")
        print("-" * 110)
        for _, row in df_results[~df_results["Agreement"]].head(50).iterrows():
            v1_str = f"{row['v1_Action']} {row['v1_Urgency']}"
            v2_str = f"{row['v2_Action']} {row['v2_Urgency']}"
            print(
                f"{str(row['Strategy']):15s} "
                f"{str(row['TradeID'])[:25]:25s} "
                f"{str(row['Ticker']):8s} "
                f"{v1_str:12s} "
                f"{v2_str:12s} "
                f"{str(row['v2_Resolution']):20s} "
                f"{str(row['v2_Winning_Gate'])}"
            )
        if disagreed > 50:
            print(f"  ... and {disagreed - 50} more")

        # Transition matrix
        print(f"\nAction transition matrix (v1 → v2):")
        transitions = df_results[~df_results["Agreement"]].groupby(
            ["v1_Action", "v2_Action"]
        ).size().reset_index(name="count")
        for _, t in transitions.iterrows():
            print(f"  {t['v1_Action']:10s} → {t['v2_Action']:10s}  ({t['count']})")
    else:
        print("\nNo disagreements — v1 and v2 produce identical actions.")

    if verbose:
        print(f"\n{'='*70}")
        print("Full results:")
        print(df_results.to_string(index=False))

    # Save CSV
    suffix = f"_{strategy_filter.lower()}" if strategy_filter else "_all"
    out_path = Path(f"output/proposal_comparison{suffix}.csv")
    out_path.parent.mkdir(exist_ok=True)
    df_results.to_csv(out_path, index=False)
    print(f"\nResults saved to {out_path}")

    return df_results


def main():
    parser = argparse.ArgumentParser(description="Compare doctrine v1 vs v2 (proposal-based)")
    parser.add_argument("--days", type=int, default=30, help="Lookback days (default: 30)")
    parser.add_argument("--verbose", action="store_true", help="Print full results")
    parser.add_argument("--strategy", type=str, default=None,
                        help="Filter to one strategy (e.g. COVERED_CALL, BUY_WRITE, CSP)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    db_path = Path("data/pipeline.duckdb")
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        sys.exit(1)

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        run_comparison(con, lookback_days=args.days, verbose=args.verbose,
                       strategy_filter=args.strategy)
    finally:
        con.close()


if __name__ == "__main__":
    main()
