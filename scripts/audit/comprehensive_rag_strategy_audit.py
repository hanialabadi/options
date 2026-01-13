import pandas as pd
import numpy as np
import re

def run_comprehensive_audit(file_path='temp_export.csv'):
    print(f"=== Comprehensive RAG-to-Strategy Consistency Audit ===")
    print(f"Target File: {file_path}\n")
    
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return

    # 1. Ticker-Level Fact Consistency (Global Gates)
    # All strategies for the same ticker must share the same underlying data
    ticker_facts = ['IV_Rank_30D', 'iv_history_days', 'last_price', 'is_market_open', 'days_to_earnings', 'Regime']
    fact_errors = []
    for ticker in df['Ticker'].unique():
        tdf = df[df['Ticker'] == ticker]
        for fact in ticker_facts:
            if fact in tdf.columns:
                uniques = tdf[fact].dropna().unique()
                if len(uniques) > 1:
                    fact_errors.append(f"{ticker}: {fact} has inconsistent values {uniques}")
    
    print(f"1. Ticker Fact Consistency: {'✅ PASS' if not fact_errors else '❌ FAIL'}")
    for err in fact_errors: print(f"   - {err}")

    # 2. Global Gate Consistency (Discovery Mode, Market Stress, Earnings)
    # If a global gate triggers for one strategy, it must trigger for all (unless rejected earlier)
    gate_errors = []
    for ticker in df['Ticker'].unique():
        tdf = df[df['Ticker'] == ticker]
        
        # Discovery Mode Check (IV History < 120 days)
        discovery_mask = tdf['acceptance_reason'].str.contains('Discovery Mode|Awaiting IV Maturity', na=False)
        if discovery_mask.any():
            # If any are in Discovery Mode, none should be READY_NOW
            ready_now = tdf[tdf['acceptance_status'] == 'READY_NOW']
            if not ready_now.empty:
                for _, row in ready_now.iterrows():
                    gate_errors.append(f"{ticker}: {row['Strategy_Name']} is READY_NOW but ticker is in Discovery Mode")
        
        # Earnings Gate Check (days_to_earnings <= 7)
        earnings_mask = tdf['acceptance_reason'].str.contains('Earnings|Binary Event', na=False)
        if earnings_mask.any():
            ready_now = tdf[tdf['acceptance_status'] == 'READY_NOW']
            if not ready_now.empty:
                for _, row in ready_now.iterrows():
                    gate_errors.append(f"{ticker}: {row['Strategy_Name']} is READY_NOW but ticker is near Earnings")

    print(f"\n2. Global Gate Consistency: {'✅ PASS' if not gate_errors else '❌ FAIL'}")
    for err in gate_errors: print(f"   - {err}")

    # 3. Strategy Isolation & Internal Justification
    # Each strategy must be justified by its own metrics, not by comparison
    justification_errors = []
    
    for idx, row in df.iterrows():
        strategy = str(row.get('Strategy_Name', ''))
        status = row.get('acceptance_status', '')
        reason = str(row.get('acceptance_reason', ''))
        notes = str(row.get('Evaluation_Notes', ''))
        
        # Check for competitive language (Implicit Competition)
        competition_keywords = ['better', 'prefer', 'instead', 'alternative', 'higher confidence', 'compared to']
        for kw in competition_keywords:
            if re.search(rf'\b{kw}\b', reason, re.I) or re.search(rf'\b{kw}\b', notes, re.I):
                justification_errors.append(f"{row['Ticker']} {strategy}: Implicit competition found ('{kw}') in reason/notes")

        # Internal Justification Check (Sample RAG Rules)
        if status == 'READY_NOW' or (status == 'STRUCTURALLY_READY' and 'awaiting full evaluation' not in reason):
            # Directional
            if any(kw in strategy for kw in ['Call', 'Put', 'LEAP', 'Spread']) and 'Iron Condor' not in strategy:
                delta = abs(row.get('Delta', 0))
                gamma = row.get('Gamma', 0)
                if delta < 0.30 and 'Reject' not in notes: # Very loose check for justification
                     justification_errors.append(f"{row['Ticker']} {strategy}: READY with weak Delta ({delta:.2f})")
            
            # Volatility
            if 'Straddle' in strategy or 'Strangle' in strategy:
                vega = row.get('Vega', 0)
                skew = row.get('Put_Call_Skew', 0)
                if skew > 1.20:
                    justification_errors.append(f"{row['Ticker']} {strategy}: READY despite Skew violation ({skew:.2f} > 1.20)")
                if vega < 0.10:
                    justification_errors.append(f"{row['Ticker']} {strategy}: READY with negligible Vega ({vega:.2f})")

            # Income
            if 'CSP' in strategy or 'Cash-Secured Put' in strategy or 'Covered Call' in strategy or 'Buy-Write' in strategy:
                pop = row.get('Probability_Of_Profit', 100)
                if pop < 60:
                    justification_errors.append(f"{row['Ticker']} {strategy}: READY with low POP ({pop:.1f}%)")

    print(f"\n3. Strategy Isolation & Internal Justification: {'✅ PASS' if not justification_errors else '❌ FAIL'}")
    # Limit output if too many errors
    for err in justification_errors[:15]: print(f"   - {err}")
    if len(justification_errors) > 15: print(f"   ... and {len(justification_errors)-15} more errors.")

    # 4. Cross-Strategy Leakage Check
    # Ensure reasons are unique to the strategy's merits and don't reference other strategies
    leakage_errors = []
    for ticker in df['Ticker'].unique():
        tdf = df[df['Ticker'] == ticker]
        if len(tdf) > 1:
            strategy_names = [str(s).lower() for s in tdf['Strategy_Name'].dropna().unique()]
            for idx, row in tdf.iterrows():
                reason = str(row.get('acceptance_reason', '')).lower()
                current_strat = str(row.get('Strategy_Name', '')).lower()
                
                for other_strat_lower in strategy_names:
                    if other_strat_lower != current_strat and other_strat_lower in reason:
                        # Find the original case name for reporting
                        orig_name = tdf[tdf['Strategy_Name'].str.lower() == other_strat_lower]['Strategy_Name'].iloc[0]
                        leakage_errors.append(f"{ticker} {row['Strategy_Name']}: Reason mentions another strategy '{orig_name}'")

    print(f"\n4. Cross-Strategy Leakage Check: {'✅ PASS' if not leakage_errors else '❌ FAIL'}")
    for err in leakage_errors: print(f"   - {err}")

    # 5. Summary Statistics
    print(f"\n=== Audit Summary ===")
    print(f"Total Strategies Audited: {len(df)}")
    print(f"Tickers Represented: {df['Ticker'].nunique()}")
    print(f"Status Distribution:\n{df['acceptance_status'].value_counts().to_string()}")

if __name__ == "__main__":
    run_comprehensive_audit()
