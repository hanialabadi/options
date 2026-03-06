import pandas as pd
import hashlib
import os
from datetime import datetime
import pytz

TRACE_TERMINAL = True

def print_provenance(phase: str, source: str, path: str, df: pd.DataFrame):
    """
    Prints a provenance block showing source, path, row count, columns, timestamp, and hash.
    """
    if not TRACE_TERMINAL:
        return

    # Calculate SHA256 if it's a file
    file_hash = "N/A"
    if path and os.path.exists(path) and os.path.isfile(path):
        sha256_hash = hashlib.sha256()
        with open(path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        file_hash = sha256_hash.hexdigest()

    # Get timezone-aware timestamp (ET)
    et_tz = pytz.timezone('US/Eastern')
    timestamp = datetime.now(et_tz).strftime("%Y-%m-%d %H:%M:%S %Z")

    print(f"\n[PROVENANCE] Phase: {phase.upper()}")
    print(f"Source: {source}")
    print(f"Path: {os.path.abspath(path) if path else 'N/A'}")
    print(f"Rows: {len(df)}")
    
    cols = list(df.columns)
    if len(cols) > 10:
        cols_str = str(cols[:10]).rstrip(']') + ', ...]'
    else:
        cols_str = str(cols)
    print(f"Columns: {cols_str}")
    
    print(f"Timestamp: {timestamp}")
    print(f"SHA256: {file_hash}")
    print("-" * 40)

def print_preview(name: str, df: pd.DataFrame, key_columns: list = None):
    """
    Prints a preview of the DataFrame (head 5) with only key columns.
    """
    if not TRACE_TERMINAL:
        return

    print(f"\n[PREVIEW] {name}")
    print(f"Columns: {len(df.columns)}")
    
    if key_columns:
        # Filter key columns that actually exist in the DataFrame
        existing_keys = [col for col in key_columns if col in df.columns]
        print(df[existing_keys].head(5).to_string())
    else:
        print(df.head(5).to_string())
    print("-" * 40)
