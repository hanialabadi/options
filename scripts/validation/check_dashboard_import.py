import sys
from pathlib import Path
import os

# Dashboard logic
project_root = Path("/Users/haniabadi/Documents/Github/options/streamlit_app/dashboard.py").resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.phase1_clean import phase1_load_and_clean_positions
import inspect
print(f"Signature: {inspect.signature(phase1_load_and_clean_positions)}")
print(f"File: {inspect.getfile(phase1_load_and_clean_positions)}")
