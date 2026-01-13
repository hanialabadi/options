import sys
from pathlib import Path
import pandas as pd
import os

# Match dashboard path logic
project_root = Path.cwd()
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.phase1_clean import phase1_load_and_clean_positions

target_path = "data/brokerage_inputs/fidelity_positions.csv"
# Ensure the file exists for the call to not return early
os.makedirs(os.path.dirname(target_path), exist_ok=True)
if not os.path.exists(target_path):
    with open(target_path, "w") as f:
        f.write("Symbol,Quantity,Last,Bid,Ask,$ Total G/L,% Total G/L,Basis,Theta,Vega,Delta,Gamma,Volume,Open Int,Time Val,Intrinsic Val,Account\n")
        f.write("Header1,Header2\n")
        f.write("Header3,Header4\n")
        f.write("AAPL,10,150,149,151,100,1%,140,0.1,0.2,0.5,0.01,1000,500,1,0,ACC1\n")

print(f"Calling with input_path={target_path}")
try:
    df, path = phase1_load_and_clean_positions(
        input_path=Path(target_path),
        save_snapshot=True
    )
    print("Success!")
except TypeError as e:
    print(f"Caught expected TypeError: {e}")
except Exception as e:
    print(f"Caught unexpected error: {type(e).__name__}: {e}")
