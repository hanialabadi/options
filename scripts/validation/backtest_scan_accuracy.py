"""
Scan Pipeline Backtest — Measure READY candidate accuracy against forward returns.

Reads historical Step12_Acceptance CSVs and computes:
  - How many READY candidates were profitable at 5d / 10d / 20d
  - Win rate by strategy, confidence band, and behavioral score tier
  - DQS score correlation with forward returns

Uses price_history from DuckDB (no API calls needed).

Usage:
    python scripts/validation/backtest_scan_accuracy.py                # Full history
    python scripts/validation/backtest_scan_accuracy.py --days 30      # Last 30 days
    python scripts/validation/backtest_scan_accuracy.py --strategy LONG_CALL
    python scripts/validation/backtest_scan_accuracy.py --verbose      # Per-trade detail
"""

import argparse
import glob
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))


def _parse_scan_date(filename: str):
    """Extract scan date from filename like Step12_Acceptance_20260310_204421.csv"""
    base = os.path.basename(filename)
    try:
        parts = base.replace('Step12_Acceptance_', '').replace('.csv', '').split('_')
        if len(parts) >= 1 and len(parts[0]) == 8:
            return datetime.strptime(parts[0], '%Y%m%d').date()
    except Exception:
        pass
    return None


def _load_forward_returns(con, tickers: list, start_date, end_date) -> dict:
    """Load price history and compute forward returns for each (ticker, date)."""
    if not tickers:
        return {}

    placeholders = ', '.join(['?' for _ in tickers])
    try:
        df = con.execute(f"""
            SELECT ticker AS Ticker, date AS Trade_Date, close_price AS Close
            FROM price_history
            WHERE ticker IN ({placeholders})
              AND date >= ?
              AND date <= ?
            ORDER BY ticker, date ASC
        """, tickers + [str(start_date), str(end_date + timedelta(days=30))]).fetchdf()
    except Exception as e:
        print(f"Warning: price_history query failed: {e}")
        return {}

    if df.empty:
        return {}

    returns = {}
    for ticker, group in df.groupby('Ticker'):
        group = group.sort_values('Trade_Date').reset_index(drop=True)
        prices = group['Close'].values
        dates = group['Trade_Date'].values

        for i, d in enumerate(dates):
            d_key = pd.Timestamp(d).to_pydatetime().date()
            fwd = {}
            for horizon, label in [(5, '5d'), (10, '10d'), (20, '20d')]:
                if i + horizon < len(prices):
                    fwd[label] = (prices[i + horizon] - prices[i]) / prices[i] * 100
            if fwd:
                returns[(ticker, d_key)] = fwd

    return returns


def main():
    parser = argparse.ArgumentParser(description="Backtest scan READY candidates")
    parser.add_argument("--days", type=int, help="Limit to last N days")
    parser.add_argument("--strategy", help="Filter by strategy")
    parser.add_argument("--verbose", action="store_true", help="Show per-trade detail")
    args = parser.parse_args()

    import duckdb

    output_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'output')
    csvs = sorted(glob.glob(os.path.join(output_dir, 'Step12_Acceptance_20*.csv')))

    if not csvs:
        print("No Step12_Acceptance CSVs found in output/")
        sys.exit(1)

    # Filter by date range
    cutoff = None
    if args.days:
        cutoff = (datetime.now() - timedelta(days=args.days)).date()

    # Deduplicate: keep ONE scan per date (latest timestamp)
    date_to_csv = {}
    for csv_path in csvs:
        scan_date = _parse_scan_date(csv_path)
        if scan_date is None:
            continue
        if cutoff and scan_date < cutoff:
            continue
        date_to_csv[scan_date] = csv_path  # last one wins (latest timestamp)

    print(f"Found {len(date_to_csv)} unique scan dates from {min(date_to_csv.keys())} to {max(date_to_csv.keys())}")

    # Collect READY candidates
    all_ready = []
    for scan_date, csv_path in sorted(date_to_csv.items()):
        try:
            df = pd.read_csv(csv_path)
        except Exception:
            continue

        # Column names evolved across CSV generations
        status_col = 'acceptance_status' if 'acceptance_status' in df.columns else 'Execution_Status'
        if status_col not in df.columns:
            continue

        ready = df[df[status_col] == 'READY'].copy()

        strategy_col = 'Strategy' if 'Strategy' in df.columns else 'Strategy_Name'
        if strategy_col not in df.columns:
            continue

        if args.strategy:
            ready = ready[ready[strategy_col].str.upper() == args.strategy.upper()]

        if ready.empty:
            continue

        ready['Scan_Date'] = scan_date
        # Normalize column names for downstream
        ready = ready.rename(columns={strategy_col: 'Strategy'})
        conf_col = 'confidence_band' if 'confidence_band' in ready.columns else 'Confidence_Band'
        ready = ready.rename(columns={conf_col: 'Confidence_Band'})

        keep_cols = ['Ticker', 'Strategy', 'Scan_Date']
        for opt in ['DQS_Score', 'Confidence_Band', 'Behavioral_Score', 'Trade_Bias']:
            if opt in ready.columns:
                keep_cols.append(opt)
        all_ready.append(ready[keep_cols].copy())

    if not all_ready:
        print("No READY candidates found in the selected date range.")
        return

    ready_df = pd.concat(all_ready, ignore_index=True)
    # Deduplicate: same ticker+strategy on same date → keep first
    ready_df = ready_df.drop_duplicates(subset=['Ticker', 'Strategy', 'Scan_Date'])

    tickers = ready_df['Ticker'].dropna().unique().tolist()
    start_date = ready_df['Scan_Date'].min()
    end_date = ready_df['Scan_Date'].max()

    print(f"READY candidates: {len(ready_df)} unique (ticker, strategy, date) combos")
    print(f"Unique tickers: {len(tickers)}")
    print()

    # Load forward returns — use a tmp copy to avoid DB lock with Streamlit
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'pipeline.duckdb'))
    import shutil
    tmp_db = '/tmp/pipeline_backtest.duckdb'
    shutil.copy2(db_path, tmp_db)
    con = duckdb.connect(tmp_db, read_only=True)
    returns = _load_forward_returns(con, tickers, start_date, end_date)
    con.close()

    # Match returns to candidates
    results = []
    for _, row in ready_df.iterrows():
        ticker = row['Ticker']
        scan_date = row['Scan_Date']
        fwd = returns.get((ticker, scan_date))

        if fwd is None:
            # Try adjacent dates (scans may run on weekends, prices on trading days)
            for offset in range(1, 4):
                fwd = returns.get((ticker, scan_date + timedelta(days=offset)))
                if fwd:
                    break
            if fwd is None:
                for offset in range(1, 4):
                    fwd = returns.get((ticker, scan_date - timedelta(days=offset)))
                    if fwd:
                        break

        if fwd is None:
            continue

        bias = str(row.get('Trade_Bias', 'BULLISH')).upper()
        # For bearish trades (puts), invert the return
        sign = -1.0 if 'BEAR' in bias or 'PUT' in str(row.get('Strategy', '')).upper() else 1.0

        results.append({
            'Ticker': ticker,
            'Strategy': row['Strategy'],
            'Scan_Date': scan_date,
            'DQS_Score': row.get('DQS_Score'),
            'Confidence_Band': row.get('Confidence_Band'),
            'Behavioral_Score': row.get('Behavioral_Score'),
            'Fwd_5d': fwd.get('5d', np.nan) * sign,
            'Fwd_10d': fwd.get('10d', np.nan) * sign,
            'Fwd_20d': fwd.get('20d', np.nan) * sign,
        })

    if not results:
        print("No forward returns available for READY candidates (price_history may be empty).")
        return

    rdf = pd.DataFrame(results)
    matched = len(rdf)
    total = len(ready_df)
    print(f"Matched forward returns: {matched}/{total} ({matched/total*100:.0f}%)")
    print()

    # Overall accuracy
    print("=" * 70)
    print("SCAN ACCURACY REPORT")
    print("=" * 70)

    for horizon in ['5d', '10d', '20d']:
        col = f'Fwd_{horizon}'
        valid = rdf[col].dropna()
        if valid.empty:
            continue
        wins = (valid > 0).sum()
        total_h = len(valid)
        avg_ret = valid.mean()
        med_ret = valid.median()
        print(f"\n{horizon} Forward Return ({total_h} trades):")
        print(f"  Win Rate: {wins}/{total_h} ({wins/total_h*100:.1f}%)")
        print(f"  Avg Return: {avg_ret:+.2f}%  Median: {med_ret:+.2f}%")
        print(f"  Best: {valid.max():+.2f}%  Worst: {valid.min():+.2f}%")

    # By Strategy
    print(f"\n{'─' * 70}")
    print("BY STRATEGY (10d):")
    for strat, group in rdf.groupby('Strategy'):
        valid = group['Fwd_10d'].dropna()
        if len(valid) < 2:
            continue
        wins = (valid > 0).sum()
        print(f"  {strat:20s}: {wins}/{len(valid)} wins ({wins/len(valid)*100:.0f}%), "
              f"avg {valid.mean():+.2f}%")

    # By DQS tier
    if 'DQS_Score' in rdf.columns and rdf['DQS_Score'].notna().any():
        print(f"\n{'─' * 70}")
        print("BY DQS TIER (10d):")
        rdf['DQS_Tier'] = pd.cut(rdf['DQS_Score'].dropna(), bins=[0, 50, 70, 85, 100],
                                  labels=['<50', '50-70', '70-85', '85+'], right=True)
        for tier, group in rdf.groupby('DQS_Tier', observed=True):
            valid = group['Fwd_10d'].dropna()
            if len(valid) < 2:
                continue
            wins = (valid > 0).sum()
            print(f"  DQS {tier:8s}: {wins}/{len(valid)} wins ({wins/len(valid)*100:.0f}%), "
                  f"avg {valid.mean():+.2f}%")

    # By Behavioral Score tier
    if 'Behavioral_Score' not in rdf.columns or rdf['Behavioral_Score'].isna().all():
        print(f"\n{'─' * 70}")
        print("BY BEHAVIORAL SCORE (10d): [column not available in older CSVs]")
    else:
        print(f"\n{'─' * 70}")
        print("BY BEHAVIORAL SCORE (10d):")
        rdf['BM_Tier'] = pd.cut(rdf['Behavioral_Score'].dropna(), bins=[0, 40, 55, 70, 100],
                                 labels=['Hostile(<40)', 'Neutral(40-55)', 'Good(55-70)', 'Strong(70+)'],
                                 right=True)
        for tier, group in rdf.groupby('BM_Tier', observed=True):
            valid = group['Fwd_10d'].dropna()
            if len(valid) < 2:
                continue
            wins = (valid > 0).sum()
            print(f"  {tier:20s}: {wins}/{len(valid)} wins ({wins/len(valid)*100:.0f}%), "
                  f"avg {valid.mean():+.2f}%")

    print(f"\n{'=' * 70}")

    if args.verbose:
        print("\nDETAILED TRADES:")
        display_cols = ['Scan_Date', 'Ticker', 'Strategy', 'Fwd_5d', 'Fwd_10d', 'Fwd_20d']
        for opt in ['DQS_Score', 'Behavioral_Score']:
            if opt in rdf.columns:
                display_cols.insert(3, opt)
        print(rdf[display_cols].sort_values('Fwd_10d', ascending=False).to_string(index=False))


if __name__ == "__main__":
    main()
