import pandas as pd

df = pd.read_csv('output/Step9B_PHASE2_VALIDATION.csv')

print('Checking for raw Schwab fields:')
raw_fields = ['bidSize', 'askSize', 'bid', 'ask', 'openInterest', 'totalVolume']
for field in raw_fields:
    if field in df.columns:
        sample = df[field].head(5).tolist()
        non_null = df[field].notna().sum()
        print(f'  {field}: {non_null}/{len(df)} populated - Sample: {sample}')
    else:
        print(f'  {field}: MISSING from columns')
