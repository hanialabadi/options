import sys
from pathlib import Path
import pandas as pd

# Match dashboard path logic
project_root = Path(__file__).resolve().parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.phase1_clean import phase1_load_and_clean_positions, CANONICAL_INPUT_PATH

print("Attempting call with input_path...")
try:
    df, path = phase1_load_and_clean_positions(
        input_path=Path(CANONICAL_INPUT_PATH),
        save_snapshot=False
    )
    print("Success!")
except TypeError as e:
    print(f"Caught expected TypeError: {e}")
except Exception as e:
    print(f"Caught unexpected error: {type(e).__name__}: {e}")
