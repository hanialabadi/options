#!/usr/bin/env python3
"""
Pipeline Audit Mode - Forensic Evidence Generation

Purpose: Materialize EVERY pipeline step for manual inspection.
Contract: No explanations. Only evidence.

Architecture:
  1. Fixed ticker universe (no dynamic filtering)
  2. CSV at every step (never overwrite)
  3. Per-ticker trace tables (vertical progression)
  4. Explicit NaN (never drop columns silently)
  
Usage:
  venv/bin/python scan_live.py --audit \
    --tickers AAPL,MSFT,NVDA,TSLA,GOOGL,AMZN,META,NFLX,AMD,INTC \
    --snapshot data/snapshots/ivhv_snapshot_live_20260102_124337.csv
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

# Audit directories
AUDIT_STEPS_DIR = Path("audit_steps")
AUDIT_TRACE_DIR = Path("audit_trace")

# Mandatory IV columns to track (never drop these)
MANDATORY_IV_COLUMNS = [
    'IV_7_D_Call', 'IV_14_D_Call', 'IV_21_D_Call', 'IV_30_D_Call',
    'IV_60_D_Call', 'IV_90_D_Call', 'IV_120_D_Call', 'IV_180_D_Call',
    'iv_surface_source', 'iv_surface_age_days', 'iv_surface_date',
    'iv_rank', 'iv_rank_available', 'iv_percentile', 'iv_history_days'
]

# Mandatory acceptance columns
MANDATORY_ACCEPTANCE_COLUMNS = [
    'acceptance_status', 'acceptance_reason', 'confidence_band'
]


class PipelineAuditMode:
    """
    Forensic audit mode for pipeline inspection.
    
    Responsibilities:
      - Save CSV at every step
      - Track column additions/removals
      - Generate per-ticker trace tables
      - Never summarize, never infer
    """
    
    def __init__(self, audit_tickers: List[str], snapshot_path: str):
        """
        Initialize audit mode.
        
        Args:
            audit_tickers: Fixed list of tickers to audit (no dynamic filtering)
            snapshot_path: Path to snapshot file
        """
        self.audit_tickers = [t.upper() for t in audit_tickers]
        self.snapshot_path = snapshot_path
        self.step_counter = 0
        self.trace_data = {ticker: [] for ticker in self.audit_tickers}
        
        # Create audit directories
        AUDIT_STEPS_DIR.mkdir(exist_ok=True)
        AUDIT_TRACE_DIR.mkdir(exist_ok=True)
        
        logger.info(f"="*80)
        logger.info(f"PIPELINE AUDIT MODE ACTIVATED")
        logger.info(f"="*80)
        logger.info(f"Audit tickers: {self.audit_tickers}")
        logger.info(f"Snapshot: {snapshot_path}")
        logger.info(f"Output directories:")
        logger.info(f"  Steps: {AUDIT_STEPS_DIR.resolve()}")
        logger.info(f"  Traces: {AUDIT_TRACE_DIR.resolve()}")
        logger.info(f"="*80)
    
    def filter_to_audit_tickers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter DataFrame to only audit tickers.
        
        CRITICAL: This must be called immediately after Step 2 snapshot load
        to freeze the universe before any dynamic filtering.
        
        Args:
            df: Full snapshot DataFrame
            
        Returns:
            DataFrame with only audit tickers
        """
        if 'Ticker' in df.columns:
            ticker_col = 'Ticker'
        elif 'ticker' in df.columns:
            ticker_col = 'ticker'
        else:
            logger.warning("⚠️ No Ticker column found - cannot filter to audit tickers")
            return df
        
        # Case-insensitive filtering
        df_filtered = df[df[ticker_col].str.upper().isin(self.audit_tickers)].copy()
        
        found_tickers = df_filtered[ticker_col].str.upper().unique().tolist()
        missing_tickers = set(self.audit_tickers) - set(found_tickers)
        
        if missing_tickers:
            logger.warning(f"⚠️ Audit tickers not in snapshot: {sorted(missing_tickers)}")
        
        logger.info(f"🔍 Audit filter: {len(df)} → {len(df_filtered)} rows")
        logger.info(f"   Found tickers: {found_tickers}")
        
        return df_filtered
    
    
    def save_step(self, df: pd.DataFrame, step_name: str, step_description: str) -> pd.DataFrame:
        """
        Save pipeline step to CSV and log statistics.
        
        Args:
            df: DataFrame at this step
            step_name: Name of step (e.g., "step02_enriched_iv")
            step_description: Human-readable description
            
        Returns:
            Same DataFrame (no modifications)
        """
        # Increment step counter
        self.step_counter += 1
        step_num = f"{self.step_counter:02d}"
        
        # Filter to audit tickers (but keep all columns)
        df_audit = df[df['Ticker'].isin(self.audit_tickers)].copy()
        
        # Save CSV
        csv_path = AUDIT_STEPS_DIR / f"step{step_num}_{step_name}.csv"
        df_audit.to_csv(csv_path, index=False)
        
        # Log statistics
        logger.info(f"\n{'='*80}")
        logger.info(f"STEP {step_num}: {step_description}")
        logger.info(f"{'='*80}")
        logger.info(f"File: {csv_path}")
        logger.info(f"Rows: {len(df_audit)} (audit tickers only)")
        logger.info(f"Columns: {len(df_audit.columns)}")
        
        # Check for mandatory IV columns
        iv_cols_present = [c for c in MANDATORY_IV_COLUMNS if c in df_audit.columns]
        iv_cols_missing = [c for c in MANDATORY_IV_COLUMNS if c not in df_audit.columns]
        
        if iv_cols_present:
            logger.info(f"\n📊 IV Columns Present ({len(iv_cols_present)}):")
            for col in iv_cols_present:
                non_null = df_audit[col].notna().sum()
                pct = (non_null / len(df_audit) * 100) if len(df_audit) > 0 else 0
                logger.info(f"  {'✅' if non_null > 0 else '❌'} {col:30s}: {non_null:3d}/{len(df_audit)} ({pct:5.1f}%)")
        
        if iv_cols_missing:
            logger.info(f"\n⚠️  IV Columns Missing ({len(iv_cols_missing)}): {iv_cols_missing}")
        
        # Check for acceptance columns (if Step 12)
        if 'acceptance_status' in df_audit.columns:
            logger.info(f"\n🎯 Acceptance Status:")
            status_counts = df_audit['acceptance_status'].value_counts().to_dict()
            for status, count in status_counts.items():
                logger.info(f"  {status}: {count}")
        
        # Update per-ticker trace
        self._update_trace(df_audit, step_num, step_name, step_description)
        
        logger.info(f"{'='*80}\n")
        
        # Return original DataFrame (no filtering)
        return df
    
    
    def _update_trace(self, df: pd.DataFrame, step_num: str, step_name: str, step_description: str):
        """
        Update per-ticker trace data.
        
        Args:
            df: DataFrame at this step
            step_num: Step number
            step_name: Step name
            step_description: Step description
        """
        for ticker in self.audit_tickers:
            ticker_rows = df[df['Ticker'] == ticker]
            
            if len(ticker_rows) == 0:
                # Ticker dropped at this step
                self.trace_data[ticker].append({
                    'step': f"step{step_num}",
                    'step_name': step_name,
                    'description': step_description,
                    'rows': 0,
                    'status': 'DROPPED',
                    'acceptance_status': None,
                    'acceptance_reason': None,
                    'iv_rank_available': None,
                    'iv_history_days': None,
                    'iv_surface_source': None,
                    'iv_surface_age_days': None,
                    'notes': 'Ticker not present in this step'
                })
            else:
                # Ticker present - extract key fields
                row = ticker_rows.iloc[0]  # Take first row for multi-strategy tickers
                
                self.trace_data[ticker].append({
                    'step': f"step{step_num}",
                    'step_name': step_name,
                    'description': step_description,
                    'rows': len(ticker_rows),
                    'status': 'PRESENT',
                    'acceptance_status': row.get('acceptance_status'),
                    'acceptance_reason': row.get('acceptance_reason'),
                    'iv_rank_available': row.get('iv_rank_available'),
                    'iv_history_days': row.get('iv_history_days'),
                    'iv_surface_source': row.get('iv_surface_source'),
                    'iv_surface_age_days': row.get('iv_surface_age_days'),
                    'notes': ''
                })
    
    
    def generate_trace_tables(self):
        """
        Generate per-ticker trace tables (vertical progression).
        
        Creates one CSV per ticker showing step-by-step progression.
        """
        logger.info(f"\n{'='*80}")
        logger.info(f"GENERATING PER-TICKER TRACE TABLES")
        logger.info(f"{'='*80}")
        
        for ticker in self.audit_tickers:
            trace_df = pd.DataFrame(self.trace_data[ticker])
            trace_path = AUDIT_TRACE_DIR / f"{ticker}_trace.csv"
            trace_df.to_csv(trace_path, index=False)
            
            logger.info(f"✅ {ticker}: {trace_path}")
        
        logger.info(f"{'='*80}\n")
    
    
    def generate_summary(self):
        """
        Generate audit summary (not for decision-making, only for navigation).
        
        Creates AUDIT_NAVIGATION.md with manual inspection commands.
        """
        logger.info(f"\n{'='*80}")
        logger.info(f"AUDIT COMPLETE - EVIDENCE GENERATED")
        logger.info(f"{'='*80}")
        logger.info(f"\nTotal steps materialized: {self.step_counter}")
        logger.info(f"\nStep-by-step CSVs: {AUDIT_STEPS_DIR.resolve()}")
        logger.info(f"Per-ticker traces: {AUDIT_TRACE_DIR.resolve()}")
        logger.info(f"{'='*80}\n")
        
        # Create navigation guide
        nav_path = Path("AUDIT_NAVIGATION.md")
        with open(nav_path, 'w') as f:
            f.write(f"""# Forensic Audit Navigation Guide

**Audit Run:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  
**Tickers:** {', '.join(self.audit_tickers)}  
**Steps Materialized:** {self.step_counter}

## Directory Structure

```
audit_steps/    - One CSV per pipeline step
audit_trace/    - One CSV per ticker (vertical progression)
```

## Manual Inspection Workflow

### 1. Inspect Step-by-Step Transformations

```bash
# View all steps
ls -lh audit_steps/

# Inspect specific step (e.g., Step 1 snapshot enrichment)
head audit_steps/step01_snapshot_enriched.csv

# Check specific ticker at specific step
grep "^AAPL," audit_steps/step01_snapshot_enriched.csv
```

### 2. Track Ticker Progression

```bash
# View full vertical progression for AAPL
cat audit_trace/AAPL_trace.csv

# Compare two tickers side-by-side
paste audit_trace/AAPL_trace.csv audit_trace/MSFT_trace.csv | column -t -s ','
```

### 3. Verify IV Surface Rehydration

```bash
# Check IV columns in Step 1 (snapshot enrichment)
head -1 audit_steps/step01_snapshot_enriched.csv | tr ',' '\\n' | grep "^IV_"

# Extract IV surface for AAPL
grep "^AAPL," audit_steps/step01_snapshot_enriched.csv | \\
  awk -F',' '{{print "IV_7D:", $X, "IV_14D:", $Y, "IV_21D:", $Z}}'
```

### 4. Compare Steps (What Changed?)

```bash
# Count rows at each step
wc -l audit_steps/*.csv

# Compare Step 1 vs Step 2 (who got filtered?)
comm -23 \\
  <(tail -n +2 audit_steps/step01_snapshot_enriched.csv | cut -d',' -f1 | sort) \\
  <(tail -n +2 audit_steps/step02_ivhv_filtered.csv | cut -d',' -f1 | sort)
```

### 5. Acceptance Status Audit

```bash
# Check final acceptance status
csvcut -c Ticker,Strategy,acceptance_status,acceptance_reason,iv_rank_available,iv_history_days \\
  audit_steps/step09_acceptance_applied.csv
```

## Key Columns to Verify

### IV Surface (Step 1+)
- `IV_7_D_Call`, `IV_14_D_Call`, `IV_21_D_Call`, `IV_30_D_Call`, `IV_60_D_Call`, `IV_90_D_Call`
- `iv_surface_source` (should be "historical_latest")
- `iv_surface_age_days` (how fresh is the data?)

### IV Metadata (Step 2+)
- `iv_rank_available` (False = insufficient history)
- `iv_history_days` (need 120+ for IV Rank)

### Acceptance (Step 9+)
- `acceptance_status` (READY_NOW, STRUCTURALLY_READY, WAIT, AVOID, INCOMPLETE)
- `acceptance_reason` (why this status?)
- `confidence_band` (LOW, MEDIUM, HIGH, SUPREME)

## No System Explanations

**This file is NOT documentation.**  
**This file is NOT a tutorial.**  
**This file is NOT a decision guide.**

This is a NAVIGATION MAP for forensic inspection. The CSVs are the evidence.  
Your job: Open them. Read them. Make your own decisions.

---
*Generated by Pipeline Audit Mode*
""")
        
        logger.info(f"📄 Navigation guide: {nav_path.resolve()}")

        logger.info(f"Per-ticker traces: {AUDIT_TRACE_DIR.resolve()}")
        logger.info(f"\n📋 MANUAL INSPECTION WORKFLOW:")
        logger.info(f"  1. Inspect step-by-step progression:")
        logger.info(f"     $ head audit_steps/step02_enriched_iv.csv")
        logger.info(f"     $ grep '^AAPL,' audit_steps/step02_enriched_iv.csv")
        logger.info(f"")
        logger.info(f"  2. Check per-ticker evolution:")
        logger.info(f"     $ cat audit_trace/AAPL_trace.csv")
        logger.info(f"")
        logger.info(f"  3. Compare step-to-step changes:")
        logger.info(f"     $ diff audit_steps/step02_enriched_iv.csv audit_steps/step03_ivhv_gap.csv")
        logger.info(f"")
        logger.info(f"  4. Verify acceptance decisions:")
        logger.info(f"     $ grep '^AAPL,' audit_steps/step12_acceptance.csv")
        logger.info(f"")
        logger.info(f"🎯 Key columns to verify:")
        logger.info(f"  - IV surface: IV_7_D_Call, IV_14_D_Call, IV_21_D_Call, IV_30_D_Call, IV_60_D_Call")
        logger.info(f"  - IV metadata: iv_surface_source, iv_surface_age_days, iv_rank_available, iv_history_days")
        logger.info(f"  - Acceptance: acceptance_status, acceptance_reason, confidence_band")
        logger.info(f"{'='*80}\n")


def create_audit_mode(audit_tickers: List[str], snapshot_path: str) -> PipelineAuditMode:
    """
    Create and initialize pipeline audit mode.
    
    Args:
        audit_tickers: Fixed list of tickers to audit
        snapshot_path: Path to snapshot file
        
    Returns:
        PipelineAuditMode instance
    """
    return PipelineAuditMode(audit_tickers, snapshot_path)
