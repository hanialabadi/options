# drift_engine_runner.py

from core.management_engine.monitor import run_phase7_drift_engine
from dashboard.config import SNAPSHOT_DIR
from datetime import datetime
import os

def run_drift_engine():
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    export_csv = os.path.join(SNAPSHOT_DIR, f"drift_audit_{timestamp}.csv")
    return run_phase7_drift_engine(drift_dir=SNAPSHOT_DIR, export_csv=export_csv, update_master=True)
