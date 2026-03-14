"""
Historical backtest of chart signals vs forward returns.

Validates whether chart engine classifications (regime, RSI, MACD, Stochastic,
Bollinger Bands) have predictive power by comparing scan-time signals against
subsequent price movement.

Usage:
    python scripts/validation/backtest_chart_signals.py [--days 20] [--min-scans 5]

Data sources:
    - Historical Step12 CSVs in output/ (scan-time signals)
    - yfinance for forward returns after each scan date
"""

import argparse
import glob
import os
import sys
import logging
from datetime import datetime, timedelta
from collections import defaultdict

import pandas as pd
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

logger = logging.getLogger(__name__)


def load_historical_scans(output_dir: str = 'output', min_date: str = None) -> pd.DataFrame:
    """Load all historical Step12 CSVs and combine with scan timestamps."""
    pattern = os.path.join(output_dir, 'Step12_Acceptance_*.csv')
    files = sorted(glob.glob(pattern))

    if not files:
        print(f"No Step12 files found in {output_dir}/")
        return pd.DataFrame()

    all_scans = []
    for f in files:
        # Extract timestamp from filename: Step12_Acceptance_YYYYMMDD_HHMMSS.csv
        basename = os.path.basename(f)
        parts = basename.replace('Step12_Acceptance_', '').replace('.csv', '')
        try:
            scan_ts = datetime.strptime(parts, '%Y%m%d_%H%M%S')
        except ValueError:
            continue

        if min_date and scan_ts < datetime.strptime(min_date, '%Y-%m-%d'):
            continue

        try:
            df = pd.read_csv(f, low_memory=False)
            df['scan_date'] = scan_ts.date()
            df['scan_ts'] = scan_ts
            all_scans.append(df)
        except Exception as e:
            logger.warning(f"Skip {basename}: {e}")

    if not all_scans:
        print("No valid scan data loaded")
        return pd.DataFrame()

    combined = pd.concat(all_scans, ignore_index=True)
    print(f"Loaded {len(all_scans)} scans, {len(combined)} total rows")
    return combined


def fetch_forward_returns(tickers: list, scan_dates: dict, forward_days: list = [5, 10, 20]) -> pd.DataFrame:
    """
    Fetch forward returns for each (ticker, scan_date) pair.

    Args:
        tickers: List of unique tickers
        scan_dates: {ticker: [date1, date2, ...]}
        forward_days: List of forward periods to measure

    Returns:
        DataFrame with columns: Ticker, scan_date, fwd_5d, fwd_10d, fwd_20d
    """
    import yfinance as yf

    # Find global date range needed
    all_dates = []
    for dates in scan_dates.values():
        all_dates.extend(dates)
    if not all_dates:
        return pd.DataFrame()

    start = min(all_dates) - timedelta(days=5)
    end = max(all_dates) + timedelta(days=max(forward_days) + 5)

    print(f"Fetching price data for {len(tickers)} tickers ({start} to {end})...")

    results = []
    batch_size = 50
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        try:
            data = yf.download(batch, start=start, end=end, progress=False, group_by='ticker')
        except Exception as e:
            logger.warning(f"yfinance batch {i}: {e}")
            continue

        for ticker in batch:
            if len(batch) > 1:
                try:
                    close = data[ticker]['Close'].dropna()
                except (KeyError, TypeError):
                    continue
            else:
                try:
                    close = data['Close'].dropna()
                except (KeyError, TypeError):
                    continue

            for scan_date in scan_dates.get(ticker, []):
                # Find the closest trading day on or after scan_date
                scan_dt = pd.Timestamp(scan_date)
                mask = close.index >= scan_dt
                if mask.sum() == 0:
                    continue

                entry_idx = close.index[mask][0]
                entry_price = close[entry_idx]

                row = {'Ticker': ticker, 'scan_date': scan_date}
                for fwd in forward_days:
                    fwd_mask = close.index >= entry_idx + timedelta(days=fwd)
                    if fwd_mask.sum() > 0:
                        fwd_price = close[close.index[fwd_mask][0]]
                        row[f'fwd_{fwd}d'] = round((fwd_price - entry_price) / entry_price * 100, 2)
                    else:
                        row[f'fwd_{fwd}d'] = np.nan

                results.append(row)

    return pd.DataFrame(results)


def analyze_regime_accuracy(scans: pd.DataFrame, returns: pd.DataFrame, fwd_col: str = 'fwd_10d'):
    """Analyze whether regime classification predicts forward returns."""
    merged = scans.merge(returns, on=['Ticker', 'scan_date'], how='inner')

    if 'Chart_Regime' not in merged.columns or fwd_col not in merged.columns:
        print("Missing Chart_Regime or forward returns")
        return

    print(f"\n{'='*70}")
    print(f"REGIME ACCURACY ({fwd_col} forward returns)")
    print(f"{'='*70}")

    for regime in ['Strong_Trend', 'Trending', 'Emerging_Trend', 'Ranging', 'Compressed', 'Overextended',
                   # Legacy names in case older scans have them
                   'Neutral']:
        subset = merged[merged['Chart_Regime'] == regime]
        if len(subset) < 5:
            continue

        fwd = subset[fwd_col].dropna()
        if len(fwd) == 0:
            continue

        mean_ret = fwd.mean()
        median_ret = fwd.median()
        pos_pct = (fwd > 0).sum() / len(fwd) * 100
        abs_mean = fwd.abs().mean()

        print(f"\n  {regime:20s} (n={len(fwd):4d})")
        print(f"    Mean return:   {mean_ret:+.2f}%")
        print(f"    Median return: {median_ret:+.2f}%")
        print(f"    % positive:    {pos_pct:.1f}%")
        print(f"    Avg |move|:    {abs_mean:.2f}%")


def analyze_signal_accuracy(scans: pd.DataFrame, returns: pd.DataFrame, fwd_col: str = 'fwd_10d'):
    """Analyze individual signal predictive power."""
    merged = scans.merge(returns, on=['Ticker', 'scan_date'], how='inner')

    print(f"\n{'='*70}")
    print(f"SIGNAL ACCURACY ({fwd_col} forward returns)")
    print(f"{'='*70}")

    # RSI zones
    if 'RSI' in merged.columns:
        print(f"\n  RSI Zones:")
        for label, lo, hi in [('Oversold (<30)', 0, 30), ('Neutral (30-70)', 30, 70), ('Overbought (>70)', 70, 100)]:
            subset = merged[(merged['RSI'] >= lo) & (merged['RSI'] < hi)]
            fwd = subset[fwd_col].dropna()
            if len(fwd) >= 5:
                print(f"    {label:25s}  n={len(fwd):4d}  mean={fwd.mean():+.2f}%  median={fwd.median():+.2f}%  %pos={((fwd>0).sum()/len(fwd)*100):.0f}%")

    # MACD histogram
    if 'MACD_Histogram' in merged.columns:
        print(f"\n  MACD Histogram:")
        for label, cond in [('Positive (bullish)', merged['MACD_Histogram'] > 0),
                             ('Negative (bearish)', merged['MACD_Histogram'] < 0)]:
            subset = merged[cond]
            fwd = subset[fwd_col].dropna()
            if len(fwd) >= 5:
                print(f"    {label:25s}  n={len(fwd):4d}  mean={fwd.mean():+.2f}%  median={fwd.median():+.2f}%  %pos={((fwd>0).sum()/len(fwd)*100):.0f}%")

    # MACD — fallback: compute histogram from MACD and MACD_Signal columns
    elif 'MACD' in merged.columns and 'MACD_Signal' in merged.columns:
        merged['_macd_hist'] = merged['MACD'] - merged['MACD_Signal']
        print(f"\n  MACD Histogram (computed):")
        for label, cond in [('Positive (bullish)', merged['_macd_hist'] > 0),
                             ('Negative (bearish)', merged['_macd_hist'] < 0)]:
            subset = merged[cond]
            fwd = subset[fwd_col].dropna()
            if len(fwd) >= 5:
                print(f"    {label:25s}  n={len(fwd):4d}  mean={fwd.mean():+.2f}%  median={fwd.median():+.2f}%  %pos={((fwd>0).sum()/len(fwd)*100):.0f}%")

    # Stochastic
    if 'SlowK_5_3' in merged.columns:
        print(f"\n  Stochastic %K:")
        for label, lo, hi in [('Oversold (<20)', 0, 20), ('Neutral (20-80)', 20, 80), ('Overbought (>80)', 80, 100)]:
            subset = merged[(merged['SlowK_5_3'] >= lo) & (merged['SlowK_5_3'] < hi)]
            fwd = subset[fwd_col].dropna()
            if len(fwd) >= 5:
                print(f"    {label:25s}  n={len(fwd):4d}  mean={fwd.mean():+.2f}%  median={fwd.median():+.2f}%  %pos={((fwd>0).sum()/len(fwd)*100):.0f}%")

    # Bollinger Band Position
    if 'BB_Position' in merged.columns:
        print(f"\n  Bollinger Band Position:")
        for label, lo, hi in [('Below lower (<10)', 0, 10), ('Mid (10-90)', 10, 90), ('Above upper (>90)', 90, 101)]:
            subset = merged[(merged['BB_Position'] >= lo) & (merged['BB_Position'] < hi)]
            fwd = subset[fwd_col].dropna()
            if len(fwd) >= 5:
                print(f"    {label:25s}  n={len(fwd):4d}  mean={fwd.mean():+.2f}%  median={fwd.median():+.2f}%  %pos={((fwd>0).sum()/len(fwd)*100):.0f}%")

    # ADX
    if 'ADX' in merged.columns:
        print(f"\n  ADX Tiers:")
        for label, lo, hi in [('<20 Ranging', 0, 20), ('20-30 Emerging', 20, 30),
                               ('30-40 Trending', 30, 40), ('>=40 Strong', 40, 100)]:
            subset = merged[(merged['ADX'] >= lo) & (merged['ADX'] < hi)]
            fwd = subset[fwd_col].dropna()
            if len(fwd) >= 5:
                abs_mean = fwd.abs().mean()
                print(f"    {label:25s}  n={len(fwd):4d}  mean={fwd.mean():+.2f}%  |move|={abs_mean:.2f}%  %pos={((fwd>0).sum()/len(fwd)*100):.0f}%")

    # EMA crossover freshness
    if 'Days_Since_Cross' in merged.columns:
        print(f"\n  EMA Crossover Freshness:")
        for label, lo, hi in [('Fresh (<=5d)', 0, 6), ('Recent (6-15d)', 6, 16),
                               ('Stale (16-30d)', 16, 31), ('Old (>30d)', 31, 9999)]:
            subset = merged[(merged['Days_Since_Cross'] >= lo) & (merged['Days_Since_Cross'] < hi)]
            fwd = subset[fwd_col].dropna()
            if len(fwd) >= 5:
                print(f"    {label:25s}  n={len(fwd):4d}  mean={fwd.mean():+.2f}%  %pos={((fwd>0).sum()/len(fwd)*100):.0f}%")


def analyze_strategy_regime_fit(scans: pd.DataFrame, returns: pd.DataFrame, fwd_col: str = 'fwd_10d'):
    """Analyze whether strategy-regime pairings produce better outcomes."""
    merged = scans.merge(returns, on=['Ticker', 'scan_date'], how='inner')

    if 'Strategy_Name' not in merged.columns or 'Chart_Regime' not in merged.columns:
        return

    print(f"\n{'='*70}")
    print(f"STRATEGY × REGIME FIT ({fwd_col})")
    print(f"{'='*70}")

    directional = ['Long Call', 'Long Put', 'LEAP Call', 'LEAP Put', 'Debit Spread']
    income = ['Cash-Secured Put', 'Covered Call', 'Buy-Write']

    for family, strats in [('DIRECTIONAL', directional), ('INCOME', income)]:
        family_data = merged[merged['Strategy_Name'].isin(strats)]
        if len(family_data) < 10:
            continue

        print(f"\n  {family} strategies:")
        for regime in ['Strong_Trend', 'Trending', 'Emerging_Trend', 'Ranging', 'Compressed']:
            subset = family_data[family_data['Chart_Regime'] == regime]
            fwd = subset[fwd_col].dropna()
            if len(fwd) >= 5:
                print(f"    {regime:20s}  n={len(fwd):4d}  mean={fwd.mean():+.2f}%  %pos={((fwd>0).sum()/len(fwd)*100):.0f}%")


def print_summary_table(scans: pd.DataFrame, returns: pd.DataFrame, fwd_col: str = 'fwd_10d'):
    """Print a compact summary table of all signals."""
    merged = scans.merge(returns, on=['Ticker', 'scan_date'], how='inner')
    fwd = merged[fwd_col].dropna()

    print(f"\n{'='*70}")
    print(f"SUMMARY: SIGNAL PREDICTIVE POWER ({fwd_col})")
    print(f"{'='*70}")
    print(f"{'Signal':<30s} {'Condition':<20s} {'n':>5s} {'MeanRet':>8s} {'HitRate':>8s} {'EdgeVsBase':>10s}")
    print('-' * 85)

    baseline_mean = fwd.mean() if len(fwd) > 0 else 0

    rows = []

    # Regime
    if 'Chart_Regime' in merged.columns:
        for regime in ['Strong_Trend', 'Trending', 'Emerging_Trend', 'Ranging', 'Compressed']:
            sub = merged[merged['Chart_Regime'] == regime][fwd_col].dropna()
            if len(sub) >= 5:
                rows.append(('Chart_Regime', regime, len(sub), sub.mean(), (sub > 0).sum() / len(sub) * 100))

    # RSI
    if 'RSI' in merged.columns:
        for label, lo, hi in [('Oversold', 0, 30), ('Overbought', 70, 100)]:
            sub = merged[(merged['RSI'] >= lo) & (merged['RSI'] < hi)][fwd_col].dropna()
            if len(sub) >= 5:
                rows.append(('RSI', label, len(sub), sub.mean(), (sub > 0).sum() / len(sub) * 100))

    # MACD
    hist_col = 'MACD_Histogram' if 'MACD_Histogram' in merged.columns else None
    if hist_col is None and 'MACD' in merged.columns and 'MACD_Signal' in merged.columns:
        merged['_mh'] = merged['MACD'] - merged['MACD_Signal']
        hist_col = '_mh'
    if hist_col:
        for label, cond in [('Positive', merged[hist_col] > 0), ('Negative', merged[hist_col] < 0)]:
            sub = merged[cond][fwd_col].dropna()
            if len(sub) >= 5:
                rows.append(('MACD_Hist', label, len(sub), sub.mean(), (sub > 0).sum() / len(sub) * 100))

    # Stochastic
    if 'SlowK_5_3' in merged.columns:
        for label, lo, hi in [('Oversold', 0, 20), ('Overbought', 80, 100)]:
            sub = merged[(merged['SlowK_5_3'] >= lo) & (merged['SlowK_5_3'] < hi)][fwd_col].dropna()
            if len(sub) >= 5:
                rows.append(('Stochastic', label, len(sub), sub.mean(), (sub > 0).sum() / len(sub) * 100))

    # ADX
    if 'ADX' in merged.columns:
        for label, lo, hi in [('<20', 0, 20), ('>=40', 40, 100)]:
            sub = merged[(merged['ADX'] >= lo) & (merged['ADX'] < hi)][fwd_col].dropna()
            if len(sub) >= 5:
                rows.append(('ADX', label, len(sub), sub.mean(), (sub > 0).sum() / len(sub) * 100))

    for signal, condition, n, mean_ret, hit_rate in rows:
        edge = mean_ret - baseline_mean
        print(f"{signal:<30s} {condition:<20s} {n:>5d} {mean_ret:>+7.2f}% {hit_rate:>7.1f}% {edge:>+9.2f}%")

    print(f"\n  Baseline: n={len(fwd)}, mean={baseline_mean:+.2f}%")


def main():
    parser = argparse.ArgumentParser(description='Backtest chart signals vs forward returns')
    parser.add_argument('--days', type=int, default=10, help='Forward return period (default: 10)')
    parser.add_argument('--min-scans', type=int, default=5, help='Min observations per bucket')
    parser.add_argument('--min-date', type=str, default=None, help='Earliest scan date (YYYY-MM-DD)')
    parser.add_argument('--output-dir', type=str, default='output', help='Directory with Step12 CSVs')
    parser.add_argument('--skip-fetch', action='store_true', help='Skip yfinance fetch (use cached)')
    parser.add_argument('--cache', type=str, default='output/backtest_returns_cache.csv', help='Cache file')
    args = parser.parse_args()

    fwd_col = f'fwd_{args.days}d'

    # 1. Load historical scans
    scans = load_historical_scans(args.output_dir, min_date=args.min_date)
    if scans.empty:
        return

    # Deduplicate: keep one scan per (ticker, date)
    scans = scans.sort_values('scan_ts').drop_duplicates(subset=['Ticker', 'scan_date'], keep='last')
    print(f"Unique (ticker, date) pairs: {len(scans)}")

    # 2. Fetch forward returns
    scan_dates = defaultdict(list)
    for _, row in scans[['Ticker', 'scan_date']].drop_duplicates().iterrows():
        scan_dates[row['Ticker']].append(row['scan_date'])

    if args.skip_fetch and os.path.exists(args.cache):
        print(f"Loading cached returns from {args.cache}")
        returns = pd.read_csv(args.cache)
        returns['scan_date'] = pd.to_datetime(returns['scan_date']).dt.date
    else:
        returns = fetch_forward_returns(
            list(scan_dates.keys()),
            dict(scan_dates),
            forward_days=[5, 10, 20]
        )
        if not returns.empty:
            returns.to_csv(args.cache, index=False)
            print(f"Cached returns to {args.cache}")

    if returns.empty:
        print("No forward returns data")
        return

    print(f"Forward returns: {len(returns)} observations")

    # 3. Run all analyses
    analyze_regime_accuracy(scans, returns, fwd_col)
    analyze_signal_accuracy(scans, returns, fwd_col)
    analyze_strategy_regime_fit(scans, returns, fwd_col)
    print_summary_table(scans, returns, fwd_col)

    print(f"\nDone. Results based on {len(returns)} (ticker, date) observations.")


if __name__ == '__main__':
    main()
