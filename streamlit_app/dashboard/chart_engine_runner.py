# chart_engine_runner.py

import subprocess
import pandas as pd
from dashboard.config import MASTER_PATH, TAENV_PYTHON

def run_chart_engine():
    subprocess.run([TAENV_PYTHON, "core/chart_engine.py"], check=True)

def reload_master():
    return pd.read_csv(MASTER_PATH)
