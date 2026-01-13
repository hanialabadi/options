import pandas as pd
import json
import os
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

AUDIT_TRACE_DIR = Path("audit_trace")

def init_audit_trace():
    """Ensure audit trace directory exists."""
    AUDIT_TRACE_DIR.mkdir(parents=True, exist_ok=True)

def save_df(tag: str, df: pd.DataFrame):
    """Save DataFrame as CSV for deep audit (avoiding Parquet type issues)."""
    init_audit_trace()
    if df is None:
        logger.warning(f"Audit: {tag} DataFrame is None")
        return None
    
    path = AUDIT_TRACE_DIR / f"{tag}.csv"
    df.to_csv(path, index=False)
    
    # Also save schema and null report
    schema_report(tag, df)
    null_report(tag, df)
    return df

def schema_report(tag: str, df: pd.DataFrame):
    """Save schema information."""
    schema = {col: str(dtype) for col, dtype in df.dtypes.items()}
    path = AUDIT_TRACE_DIR / f"{tag}_schema.json"
    with open(path, 'w') as f:
        json.dump(schema, f, indent=2)

def null_report(tag: str, df: pd.DataFrame):
    """Save null value distribution."""
    nulls = df.isna().sum().to_frame(name='null_count')
    nulls['null_pct'] = (nulls['null_count'] / len(df)) * 100
    path = AUDIT_TRACE_DIR / f"{tag}_nulls.csv"
    nulls.to_csv(path)

def profile(tag: str, df: pd.DataFrame, duration_ms: float):
    """Save performance and row count metrics."""
    path = AUDIT_TRACE_DIR / f"{tag}_metrics.json"
    
    dirpath = os.path.dirname(path)
    if dirpath and not os.path.exists(dirpath):
        os.makedirs(dirpath, exist_ok=True)

    metrics = {
        "tag": tag,
        "timestamp": datetime.now().isoformat(),
        "row_count": len(df) if df is not None else 0,
        "column_count": len(df.columns) if df is not None else 0,
        "duration_ms": round(duration_ms, 2)
    }
    
    with open(path, 'w') as f:
        json.dump(metrics, f, indent=2)

def save_rejections(tag: str, rejections_df: pd.DataFrame):
    """Save explicit rejection reasons."""
    if rejections_df is not None and not rejections_df.empty:
        init_audit_trace()
        path = AUDIT_TRACE_DIR / f"{tag}_rejections.csv"
        rejections_df.to_csv(path, index=False)

def save_demand(df_demand: pd.DataFrame):
    """Save Phase 7.5 IV demand emissions."""
    if df_demand is not None and not df_demand.empty:
        init_audit_trace()
        path = AUDIT_TRACE_DIR / "step7_5_iv_demand.csv"
        df_demand.to_csv(path, index=False)
        logger.info(f"🛡️ Saved IV demand trace ({len(df_demand)} tickers)")

def export_ready_now_evidence(df: pd.DataFrame):
    """
    Specialized audit for Step 12 -> Step 8 boundary.
    Captures full provenance for execution candidates.
    """
    if df is None or df.empty:
        return
    
    ready_now = df[df['acceptance_status'] == 'READY_NOW'].copy()
    if not ready_now.empty:
        init_audit_trace()
        # Select critical provenance fields
        provenance_cols = [
            'Ticker', 'Strategy_Name', 'acceptance_status', 'acceptance_reason',
            'IV_Rank_Source', 'Regime', 'Signal_Type', 'Contract_Status', 
            'PCS_Final', 'Validation_Status'
        ]
        # Only include columns that actually exist
        existing_cols = [c for c in provenance_cols if c in ready_now.columns]
        evidence = ready_now[existing_cols]
        
        path = AUDIT_TRACE_DIR / "READY_NOW_evidence_packet.csv"
        evidence.to_csv(path, index=False)
        logger.info(f"🛡️ Saved READY_NOW evidence packet ({len(evidence)} rows)")
