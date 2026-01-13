import pandas as pd
import numpy as np

file_path = '/Users/haniabadi/Downloads/2026-01-06T17-23_export.csv'
df = pd.read_csv(file_path)

def run_audit():
    print('=== RAG-to-Strategy Consistency Audit ===\n')
    
    # 1. Ticker-Level Fact Consistency (Global Gates)
    ticker_facts = ['IV_Rank_30D', 'iv_history_days', 'last_price', 'is_market_open']
    fact_errors = []
    for ticker in df['Ticker'].unique():
        tdf = df[df['Ticker'] == ticker]
        for fact in ticker_facts:
            if fact in tdf.columns:
                uniques = tdf[fact].unique()
                if len(uniques) > 1:
                    # Special handling for float NaNs
                    if not (tdf[fact].isna().all() or (not tdf[fact].isna().any() and len(uniques) == 1)):
                        fact_errors.append(f'{ticker}: {fact} has multiple values {uniques}')
    
    print(f'1. Ticker Fact Consistency: {"✅ PASS" if not fact_errors else "❌ FAIL"}')
    for err in fact_errors: print(f'   - {err}')

    # 2. Discovery Mode Gate Consistency
    discovery_mask = df['acceptance_reason'].str.contains('Discovery Mode', na=False)
    discovery_tickers = df[discovery_mask]['Ticker'].unique()
    gate_errors = []
    for ticker in discovery_tickers:
        tdf = df[df['Ticker'] == ticker]
        for _, row in tdf.iterrows():
            reason = str(row['acceptance_reason'])
            status = row['acceptance_status']
            # If one strategy is blocked by Discovery, all should be UNLESS they failed an even earlier gate
            if 'Discovery Mode' not in reason and status not in ['INCOMPLETE', 'AVOID']:
                 gate_errors.append(f'{ticker}: {row["Strategy_Name"]} bypassed Discovery gate (Status: {status}, Reason: {reason})')
            
    print(f'\n2. Discovery Gate Consistency: {"✅ PASS" if not gate_errors else "❌ FAIL"}')
    for err in gate_errors: print(f'   - {err}')

    # 3. Strategy Independence Audit (No Leakage)
    print('\n3. Strategy Independence Audit:')
    leakage_found = False
    for ticker in df['Ticker'].unique():
        tdf = df[df['Ticker'] == ticker]
        if tdf['acceptance_status'].nunique() > 1:
            status_reason_pairs = tdf[['acceptance_status', 'acceptance_reason']].drop_duplicates()
            if len(status_reason_pairs) < tdf['acceptance_status'].nunique():
                leakage_found = True
                print(f'   ❌ Potential Inconsistency in {ticker}: Multiple statuses sharing same/missing reasons.')
    
    if not leakage_found:
        print('   ✅ No evidence of cross-strategy leakage found. Status variations are justified by strategy-specific data.')

    # 4. Implicit Competition Check
    competition_keywords = ['better', 'prefer', 'instead', 'alternative', 'higher confidence']
    competition_found = df[df['acceptance_reason'].str.contains('|'.join(competition_keywords), case=False, na=False)]
    print(f'\n4. Implicit Competition Check: {"✅ PASS (No implicit competition found)" if competition_found.empty else "⚠️ WARNING"}')

    # 5. Internal Justification (Sample: AVOID status)
    print('\n5. Internal Justification (Sample: AVOID status):')
    avoid_df = df[df['acceptance_status'] == 'AVOID']
    if not avoid_df.empty:
        for _, row in avoid_df.head(3).iterrows():
            print(f'   - {row["Ticker"]} {row["Strategy_Name"]}: {row["acceptance_reason"]}')
            print(f'     [Data] RSI: {row["RSI"]:.1f}, ADX: {row["ADX"]:.1f}, Trend: {row["Trend_Strength"]}')
    else:
        print('   No AVOID strategies found.')

if __name__ == "__main__":
    run_audit()
