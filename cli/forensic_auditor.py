import os
import sys
import pandas as pd
import numpy as np
import re
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.data_contracts.config import (
    MANAGEMENT_SAFE_MODE,
    ACTIVE_MASTER_PATH,
    SNAPSHOT_DIR,
    DATA_DIR
)
from core.phase1_clean import OCC_OPTION_PATTERN

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger("ForensicAuditor")

class ForensicAuditor:
    """
    Forensic CLI Auditor for the Management Engine.
    Goal: Discovery, not correction.
    """

    def __init__(self, input_path: str = None):
        self.input_path = input_path or "data/brokerage_inputs/fidelity_positions.csv"
        self.report = []
        self.stats = {}

    def log_section(self, title: str):
        self.report.append(f"\n## {title}")
        print(f"\n>>> {title}")

    def log_info(self, msg: str):
        self.report.append(f"- [INFO] {msg}")

    def log_warning(self, msg: str):
        self.report.append(f"- [WARNING] {msg}")

    def log_critical(self, msg: str):
        self.report.append(f"- [CRITICAL] {msg}")

    def run_audit(self):
        self.report.append(f"# Management Engine Forensic Audit Report")
        self.report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.report.append(f"Safe Mode: {MANAGEMENT_SAFE_MODE}")

        try:
            self.audit_phase0_input_surface()
            self.audit_phase1_identity_parsing()
            self.audit_phase2_strategy_structure()
            self.audit_phase3_external_leaks()
            self.audit_phase4_data_joins()
            self.audit_phase5_temporal_consistency()
            self.audit_phase6_entry_freeze()
            self.audit_phase7_drift_engine()
            self.audit_phase8_pnl_attribution()
            self.audit_phase9_persona_governance()
            self.audit_phase10_dashboard_divergence()
        except Exception as e:
            self.log_critical(f"Audit aborted due to unexpected error: {str(e)}")
            import traceback
            self.report.append(f"```\n{traceback.format_exc()}\n```")

        self.save_report()

    def audit_phase0_input_surface(self):
        self.log_section("Phase 0: Input Surface Audit")
        path = Path(self.input_path)
        if not path.exists():
            self.log_critical(f"Input file missing: {path}")
            return

        # Raw inspection
        try:
            # Fidelity usually has 2 header rows
            df_raw = pd.read_csv(path, skiprows=2)
            self.log_info(f"Raw row count: {len(df_raw)}")
            self.log_info(f"Columns detected: {df_raw.columns.tolist()}")
            
            null_ratios = df_raw.isnull().mean()
            high_nulls = null_ratios[null_ratios > 0.5]
            if not high_nulls.empty:
                self.log_warning(f"High null ratios (>50%): {high_nulls.to_dict()}")

            # Check for expected Fidelity columns
            expected = ['Symbol', 'Quantity', 'Last', 'Bid', 'Ask', 'Account']
            missing = [c for c in expected if c not in df_raw.columns]
            if missing:
                self.log_critical(f"Missing expected Fidelity columns: {missing}")
            
            self.stats['phase0_raw_count'] = len(df_raw)
        except Exception as e:
            self.log_critical(f"Failed to read raw input: {e}")

    def audit_phase1_identity_parsing(self):
        self.log_section("Phase 1: Identity & Parsing Audit")
        from core.phase1_clean import phase1_load_and_clean_positions
        
        df, _ = phase1_load_and_clean_positions(
            input_path=self.input_path,
            save_snapshot=False
        )
        if df.empty:
            self.log_critical("Phase 1 produced empty DataFrame")
            return

        self.log_info(f"Cleaned row count: {len(df)}")
        
        # Identity Law: Symbol vs Underlying_Ticker
        options = df[df['AssetType'] == 'OPTION']
        stocks = df[df['AssetType'] == 'STOCK']
        
        self.log_info(f"Options: {len(options)}, Stocks: {len(stocks)}")
        
        # % of rows where Symbol != Underlying_Ticker
        # For options, Symbol should NEVER equal Underlying_Ticker
        # For stocks, Symbol SHOULD equal Underlying_Ticker
        option_diff_pct = (options['Symbol'] != options['Underlying_Ticker']).mean() * 100
        self.log_info(f"Option rows where Symbol != Underlying_Ticker: {option_diff_pct:.1f}% (Expected 100%)")

        # Detect OCC parsing failures (already handled by phase1_clean but we verify)
        mismatched_identity = options[options['Symbol'] == options['Underlying_Ticker']]
        if not mismatched_identity.empty:
            self.log_critical(f"Options where Symbol == Underlying_Ticker (Parsing Failure): {mismatched_identity['Symbol'].tolist()}")

        # Underlying_Ticker validation
        digit_tickers = df[df['Underlying_Ticker'].str.contains(r'\d', na=False)]
        if not digit_tickers.empty:
            self.log_warning(f"Underlying_Ticker contains digits (Potential Leak): {digit_tickers['Underlying_Ticker'].unique().tolist()}")

        # Longest ticker
        max_ticker_len = df['Underlying_Ticker'].str.len().max()
        self.log_info(f"Max Underlying_Ticker length: {max_ticker_len}")
        if max_ticker_len > 5:
            self.log_warning(f"Underlying_Ticker > 5 chars: {df[df['Underlying_Ticker'].str.len() > 5]['Underlying_Ticker'].unique().tolist()}")

        self.stats['phase1_df'] = df

    def audit_phase2_strategy_structure(self):
        self.log_section("Phase 2: Strategy & Structure Audit")
        df = self.stats.get('phase1_df')
        if df is None: return

        from core.phase2_parse import phase2_run_all
        try:
            df_p2 = phase2_run_all(df)
            self.log_info(f"Strategy coverage: {(df_p2['Strategy'] != 'UNKNOWN').mean()*100:.1f}%")
            
            unknowns = df_p2[df_p2['Strategy'] == 'UNKNOWN']
            if not unknowns.empty:
                self.log_warning(f"Unknown strategies ({len(unknowns)}): {unknowns['Symbol'].tolist()[:10]}")

            # Missing Leg validation
            # In Phase 2, Covered Calls should have 2 legs
            cc_trades = df_p2[df_p2['Strategy'] == 'Covered Call']
            for tid, group in cc_trades.groupby('TradeID'):
                if len(group) < 2:
                    self.log_warning(f"Covered Call {tid} has only {len(group)} leg(s)")
            
            self.stats['phase2_df'] = df_p2
        except Exception as e:
            self.log_critical(f"Phase 2 failed: {e}")

    def audit_phase3_external_leaks(self):
        self.log_section("Phase 3: External Dependency Leak Audit")
        # This is a static analysis + runtime check
        # We check if MANAGEMENT_SAFE_MODE is respected
        self.log_info(f"MANAGEMENT_SAFE_MODE is {MANAGEMENT_SAFE_MODE}")
        
        # Search for Yahoo calls in core/
        # (In a real auditor we might monkeypatch or use a tracer)
        # For now, we'll flag if we see any code that imports yfinance in core/management_engine
        
        leak_found = False
        for root, dirs, files in os.walk("core/management_engine"):
            for file in files:
                if file.endswith(".py"):
                    path = os.path.join(root, file)
                    with open(path, 'r') as f:
                        content = f.read()
                        if "yfinance" in content or "yf." in content:
                            self.log_critical(f"Potential Yahoo leak in {path}")
                            leak_found = True
        
        if not leak_found:
            self.log_info("No explicit Yahoo imports found in management_engine")

    def audit_phase4_data_joins(self):
        self.log_section("Phase 4: Historical Data Join Audit")
        df = self.stats.get('phase2_df')
        if df is None: return

        # Check IV/HV availability
        canonical_path = PROJECT_ROOT / "data" / "ivhv_timeseries" / "ivhv_timeseries_canonical.csv"
        if not canonical_path.exists():
            self.log_critical(f"Canonical IVHV file missing: {canonical_path}")
            return

        try:
            ivhv_df = pd.read_csv(canonical_path)
            self.log_info(f"Canonical IVHV rows: {len(ivhv_df)}")
            
            tickers = df['Underlying_Ticker'].unique()
            ivhv_tickers = ivhv_df['ticker'].unique()
            missing_ivhv = [t for t in tickers if t not in ivhv_tickers]
            if missing_ivhv:
                self.log_warning(f"Tickers missing from IVHV archive: {missing_ivhv}")
            else:
                self.log_info("All active tickers found in IVHV archive")
            
            # Check for join key case sensitivity
            if any(t.lower() in [it.lower() for it in ivhv_tickers] and t not in ivhv_tickers for t in tickers):
                self.log_warning("Potential case-sensitivity mismatch in IVHV join keys")

        except Exception as e:
            self.log_critical(f"Failed to audit IVHV joins: {e}")

    def audit_phase5_temporal_consistency(self):
        self.log_section("Phase 5: Temporal Consistency Audit")
        df = self.stats.get('phase2_df')
        if df is None: return

        # DTE < 0
        if 'DTE' in df.columns:
            expired = df[df['DTE'] < 0]
            if not expired.empty:
                self.log_warning(f"Positions with DTE < 0: {expired['Symbol'].tolist()}")

        # Snapshot_TS vs Now
        if 'Snapshot_TS' in df.columns:
            latest_ts = pd.to_datetime(df['Snapshot_TS']).max()
            self.log_info(f"Latest Snapshot_TS: {latest_ts}")
            if (datetime.now() - latest_ts).total_seconds() > 86400 * 7:
                self.log_warning("Snapshot is older than 7 days")

    def audit_phase6_entry_freeze(self):
        self.log_section("Phase 6: Entry Freeze Integrity Audit")
        if not ACTIVE_MASTER_PATH.exists():
            self.log_info("Active master not found, skipping freeze audit")
            return

        try:
            df_master = pd.read_csv(ACTIVE_MASTER_PATH)
            self.log_info(f"Active master rows: {len(df_master)}")
            
            entry_cols = [c for c in df_master.columns if c.endswith('_Entry') or c == 'Entry_Timestamp']
            self.log_info(f"Entry columns found: {entry_cols}")
            
            # Check for NaNs in entry columns for old trades
            if 'Entry_Timestamp' in df_master.columns:
                frozen = df_master[df_master['Entry_Timestamp'].notna()]
                for col in ['Delta_Entry', 'Underlying_Price_Entry']:
                    if col in df_master.columns:
                        nans = frozen[frozen[col].isna()]
                        if not nans.empty:
                            self.log_warning(f"Frozen trades missing {col}: {len(nans)} rows")
        except Exception as e:
            self.log_critical(f"Failed to audit entry freeze: {e}")

    def audit_phase7_drift_engine(self):
        self.log_section("Phase 7: Drift Engine Sanity Audit")
        db_path = PROJECT_ROOT / "data" / "pipeline.duckdb"
        if not db_path.exists():
            self.log_warning(f"DuckDB missing at {db_path}. Windowed drift will fail.")
            return

        try:
            import duckdb
            with duckdb.connect(str(db_path)) as con:
                tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").df()
                self.log_info(f"DuckDB tables: {tables['table_name'].tolist()}")
                
                if 'clean_legs' in tables['table_name'].values:
                    count = con.execute("SELECT count(*) FROM clean_legs").fetchone()[0]
                    self.log_info(f"clean_legs row count: {count}")
                    
                    # Check for monotonicity of Snapshot_TS
                    drift = con.execute("""
                        SELECT TradeID, count(*) as snapshots 
                        FROM clean_legs 
                        GROUP BY TradeID 
                        HAVING snapshots > 1
                    """).df()
                    self.log_info(f"Trades with >1 snapshot: {len(drift)}")
                else:
                    self.log_critical("Table 'clean_legs' missing from DuckDB")
        except Exception as e:
            self.log_critical(f"DuckDB audit failed: {e}")

    def audit_phase8_pnl_attribution(self):
        self.log_section("Phase 8: P&L Attribution Audit")
        # Why did attribution fail?
        df = self.stats.get('phase2_df')
        if df is None: return
        
        # Check if Greeks exist
        greeks = ['Delta', 'Gamma', 'Vega', 'Theta']
        missing_greeks = [g for g in greeks if g not in df.columns]
        if missing_greeks:
            self.log_warning(f"Current Greeks missing from input: {missing_greeks}")
        else:
            self.log_info("All primary Greeks present for attribution")
            
        # Check for entry Greeks in master if available
        if ACTIVE_MASTER_PATH.exists():
            df_master = pd.read_csv(ACTIVE_MASTER_PATH)
            entry_greeks = [f"{g}_Entry" for g in greeks]
            missing_entry = [eg for eg in entry_greeks if eg not in df_master.columns]
            if missing_entry:
                self.log_warning(f"Entry Greeks missing from master: {missing_entry}")
            else:
                self.log_info("Entry Greeks present in master")

    def audit_phase9_persona_governance(self):
        self.log_section("Phase 9: Persona Governance Audit")
        try:
            import agents.persona_engine as pe
            if hasattr(pe, 'TraderPersona'):
                self.log_info("TraderPersona class found in agents.persona_engine")
                # Check for common persona definitions in the file
                with open(pe.__file__, 'r') as f:
                    content = f.read()
                    personas = re.findall(r'(\w+)\s*=\s*TraderPersona\(', content)
                    if personas:
                        self.log_info(f"Detected persona definitions: {personas}")
                    else:
                        self.log_warning("No explicit TraderPersona instances detected in persona_engine.py")
            else:
                self.log_critical("TraderPersona class NOT found in agents.persona_engine")
        except Exception as e:
            self.log_critical(f"Failed to audit persona engine: {e}")

    def audit_phase10_dashboard_divergence(self):
        self.log_section("Phase 10: Dashboard Divergence Audit")
        dashboard_path = PROJECT_ROOT / "streamlit_app" / "dashboard.py"
        if not dashboard_path.exists():
            self.log_warning("Dashboard file missing")
            return

        with open(dashboard_path, 'r') as f:
            content = f.read()
            
        # Check if dashboard uses the same canonical paths
        if "ACTIVE_MASTER_PATH" in content:
            self.log_info("Dashboard references ACTIVE_MASTER_PATH")
        else:
            self.log_warning("Dashboard might be using hardcoded paths")

        if "load_canonical_ivhv" in content:
            self.log_warning("Dashboard references non-existent load_canonical_ivhv")

        # Forensic Discovery: Signature Mismatch Detection
        # The dashboard expects phase1_load_and_clean_positions(input_path=...)
        # but the core implementation might have changed its signature.
        try:
            import inspect
            import core.phase1_clean as p1c
            from core.phase1_clean import phase1_load_and_clean_positions
            self.log_info(f"core.phase1_clean source: {p1c.__file__}")
            sig = inspect.signature(phase1_load_and_clean_positions)
            self.log_info(f"phase1_load_and_clean_positions signature: {sig}")
            
            # Check if 'input_path' is a valid parameter
            if 'input_path' not in sig.parameters:
                self.log_critical("SIGNATURE MISMATCH: 'input_path' parameter missing from phase1_load_and_clean_positions")
                self.log_critical("This will cause the Dashboard to crash during management pipeline execution.")
            
            # Check dashboard calls
            calls = re.findall(r'phase1_load_and_clean_positions\((.*?)\)', content, re.DOTALL)
            for call in calls:
                # Clean up the call string (remove newlines and extra spaces)
                clean_call = " ".join(call.split())
                if "input_path=" in clean_call and "input_path" not in sig.parameters:
                    self.log_critical(f"Dashboard makes invalid call: phase1_load_and_clean_positions({clean_call})")
                elif "input_path=" in clean_call:
                    self.log_info(f"Dashboard call signature matches: phase1_load_and_clean_positions({clean_call})")
        except Exception as e:
            self.log_warning(f"Failed to audit function signatures: {e}")

    def save_report(self):
        report_path = "audit_report_forensic.md"
        with open(report_path, 'w') as f:
            f.write("\n".join(self.report))
        print(f"\n✅ Audit complete. Report saved to {report_path}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="Path to brokerage CSV")
    args = parser.parse_args()
    
    auditor = ForensicAuditor(input_path=args.input)
    auditor.run_audit()
