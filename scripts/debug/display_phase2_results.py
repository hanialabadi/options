import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)

df = pd.read_csv('output/Step9B_PHASE2_VALIDATION.csv')

print('='*100)
print('PHASE 2 VALIDATION - STEP 9B OUTPUT')
print('='*100)
print(f'\nTotal rows: {len(df)}')
print(f'Total columns: {len(df.columns)}')

# Show Phase 2 columns
phase2_cols = ['depth_tag', 'balance_tag', 'execution_quality', 'dividend_risk']
available_phase2 = [c for c in phase2_cols if c in df.columns]
print(f'\nPhase 2 enrichment columns: {available_phase2}')

# Show sample columns
display_cols = ['Ticker', 'Strategy_Name', 'strikePrice', 'bid', 'ask', 
                'depth_tag', 'balance_tag', 'execution_quality', 'dividend_risk']
existing_cols = [c for c in display_cols if c in df.columns]

print(f'\n' + '='*100)
print('SAMPLE ROWS (5 contracts with Phase 2 enrichment)')
print('='*100)
print(df[existing_cols].head(5).to_string(index=False))

# Show distribution of Phase 2 tags
print(f'\n' + '='*100)
print('PHASE 2 ENRICHMENT DISTRIBUTIONS')
print('='*100)
for col in available_phase2:
    if col in df.columns:
        dist = df[col].value_counts().to_dict()
        print(f'{col}: {dist}')
