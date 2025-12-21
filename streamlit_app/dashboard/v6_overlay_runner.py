# v6_overlay_runner.py

from utils.pcs_v6_sync import run_pcs_v6_sync_pipeline

def apply_v6_overlay(df):
    return run_pcs_v6_sync_pipeline(df)
