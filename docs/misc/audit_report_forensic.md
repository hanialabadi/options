# Management Engine Forensic Audit Report
Generated: 2026-01-11 15:04:20
Safe Mode: True

## Phase 0: Input Surface Audit
- [INFO] Raw row count: 43
- [INFO] Columns detected: ['Symbol', 'Quantity', 'UL Last', 'Last', 'Bid', 'Ask', '$ Total G/L', '% Total G/L', 'Basis', 'Earnings Date', 'Theta', 'Vega', 'Gamma', 'Delta', 'Rho', 'Time Val', 'Account']

## Phase 1: Identity & Parsing Audit
- [INFO] Cleaned row count: 41
- [INFO] Options: 20, Stocks: 21
- [INFO] Option rows where Symbol != Underlying_Ticker: 100.0% (Expected 100%)
- [INFO] Max Underlying_Ticker length: 4

## Phase 2: Strategy & Structure Audit
- [INFO] Strategy coverage: 100.0%

## Phase 3: External Dependency Leak Audit
- [INFO] MANAGEMENT_SAFE_MODE is True
- [INFO] No explicit Yahoo imports found in management_engine

## Phase 4: Historical Data Join Audit
- [INFO] Canonical IVHV rows: 1947
- [WARNING] Tickers missing from IVHV archive: ['ALGN', 'CHWY', 'DOCU', 'GDRX', 'GPC', 'MARA', 'MQ', 'MRNA', 'ROKU', 'SE', 'SOFI', 'TASK', 'TDOC', 'TWLO', 'UPST', 'UUUU']

## Phase 5: Temporal Consistency Audit
- [INFO] Latest Snapshot_TS: 2026-01-11 15:04:20.247428

## Phase 6: Entry Freeze Integrity Audit
- [INFO] Active master rows: 38
- [INFO] Entry columns found: ['Delta_Entry', 'Gamma_Entry', 'Vega_Entry', 'Theta_Entry', 'Premium_Entry', 'PCS_Entry', 'Capital_Deployed_Entry', 'Moneyness_Pct_Entry', 'DTE_Entry', 'BreakEven_Entry']

## Phase 7: Drift Engine Sanity Audit
- [INFO] DuckDB tables: ['clean_legs', 'master_active', 'runs', 'trade_first_seen']
- [INFO] clean_legs row count: 1867
- [INFO] Trades with >1 snapshot: 48

## Phase 8: P&L Attribution Audit
- [INFO] All primary Greeks present for attribution
- [INFO] Entry Greeks present in master

## Phase 9: Persona Governance Audit
- [INFO] TraderPersona class found in agents.persona_engine
- [INFO] Detected persona definitions: ['aggressive']

## Phase 10: Dashboard Divergence Audit
- [WARNING] Dashboard might be using hardcoded paths
- [INFO] core.phase1_clean source: /Users/haniabadi/Documents/Github/options/core/phase1_clean.py
- [INFO] phase1_load_and_clean_positions signature: (input_path: str = None, save_snapshot: bool = True) -> Tuple[pandas.core.frame.DataFrame, str]
- [INFO] Dashboard call signature matches: phase1_load_and_clean_positions(input_path=target_path, save_snapshot=True)
- [INFO] Dashboard call signature matches: phase1_load_and_clean_positions(input_path=target_path, save_snapshot=True)