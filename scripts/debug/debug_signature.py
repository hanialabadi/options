import inspect
import sys
import os
from pathlib import Path

# Add project root to path (matching dashboard logic)
project_root = Path(__file__).resolve().parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

try:
    from core.phase1_clean import phase1_load_and_clean_positions
    print(f"File: {inspect.getfile(phase1_load_and_clean_positions)}")
    print(f"Signature: {inspect.signature(phase1_load_and_clean_positions)}")
    
    # Try to call it with input_path
    try:
        # Use a dummy path that doesn't exist to avoid actual processing if it works
        phase1_load_and_clean_positions(input_path=Path("non_existent.csv"), save_snapshot=False)
        print("Call with input_path succeeded (or at least didn't throw TypeError for keyword)")
    except TypeError as e:
        print(f"Call with input_path failed: {e}")
    except Exception as e:
        print(f"Call failed with other error (expected if file missing): {type(e).__name__}: {e}")

except ImportError as e:
    print(f"Import failed: {e}")
except Exception as e:
    print(f"An error occurred: {e}")
