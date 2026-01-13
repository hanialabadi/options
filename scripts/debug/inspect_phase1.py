import core.phase1_clean
import inspect
import pandas as pd
import os

print(f"CWD: {os.getcwd()}")
print(f"File: {core.phase1_clean.__file__}")
print("-" * 40)
print("Source of phase1_load_and_clean_positions:")
try:
    print(inspect.getsource(core.phase1_clean.phase1_load_and_clean_positions))
except Exception as e:
    print(f"Could not get source: {e}")

print("-" * 40)
result = core.phase1_clean.phase1_load_and_clean_positions(
    input_path=Path(core.phase1_clean.CANONICAL_INPUT_PATH),
    save_snapshot=False
)
print(f"Return type: {type(result)}")
if isinstance(result, tuple):
    print(f"Tuple length: {len(result)}")
    df = result[0]
    print(f"First element type: {type(df)}")
    if isinstance(df, pd.DataFrame):
        print(f"Columns: {df.columns.tolist()}")
elif isinstance(result, dict):
    print(f"Dict keys: {result.keys()}")
