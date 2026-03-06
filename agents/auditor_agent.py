"""
Holistic Auditor Agent

Audits active positions for data integrity, identifies weaknesses, 
and provides holistic scaling recommendations based on RAG and Persona.
"""

import pandas as pd
import numpy as np
import logging
import duckdb
from pathlib import Path
from datetime import datetime
from core.shared.data_contracts.config import PIPELINE_DB_PATH
from agents.persona_engine import TraderPersona, manage_position
from agents.chart_agent import pcs_engine_v3_unified

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HolisticAuditor:
    def __init__(self, db_path: str = None):
        self.db_path = db_path or str(PIPELINE_DB_PATH)
        self.persona = TraderPersona(
            name="Balanced_RAG",
            max_loss_pct=-30,
            max_gain_pct=50,
            scaling_factor=1.0,
            pcs_threshold=70
        )

    def load_active_state(self) -> pd.DataFrame:
        """Load active anchors joined with latest market data."""
        try:
            with duckdb.connect(self.db_path, read_only=True) as con:
                # 1. Get active anchors
                df_anchors = con.execute("SELECT * FROM entry_anchors WHERE Is_Active = TRUE").df()
                if df_anchors.empty:
                    return pd.DataFrame()
                
                # Deduplicate anchors by LegID (keep latest Entry_Snapshot_TS)
                df_anchors = df_anchors.sort_values('Entry_Snapshot_TS', ascending=False).drop_duplicates('LegID')

                # 2. Get latest market data from enriched_legs_v1 or clean_legs_v2
                df_market = pd.DataFrame()
                tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").df()['table_name'].tolist()
                
                for t in ['enriched_legs_v1', 'clean_legs_v2']:
                    if t in tables:
                        df_t = con.execute(f"""
                            WITH LatestSnap AS (
                                SELECT LegID, MAX(Snapshot_TS) as max_ts
                                FROM {t}
                                GROUP BY LegID
                            )
                            SELECT e.* FROM {t} e
                            JOIN LatestSnap l ON e.LegID = l.LegID AND e.Snapshot_TS = l.max_ts
                        """).df()
                        if not df_t.empty:
                            df_market = pd.concat([df_market, df_t]).drop_duplicates('LegID')

                if df_market.empty:
                    logger.warning("No market data found for active positions.")
                    return df_anchors

                # 3. Join
                # We prefer market data for current values, anchors for entry values
                market_cols = ['LegID', 'Last', 'Bid', 'Ask', 'UL Last', 'Delta', 'Gamma', 'Vega', 'Theta', 'IV', 'Snapshot_TS']
                market_cols = [c for c in market_cols if c in df_market.columns]
                
                df_joined = df_anchors.merge(
                    df_market[market_cols],
                    on='LegID',
                    how='left',
                    suffixes=('_anchor', '_market')
                )
                
                return df_joined
        except Exception as e:
            logger.error(f"Failed to load active state: {e}")
            return pd.DataFrame()

    def audit_integrity(self, df: pd.DataFrame) -> pd.DataFrame:
        """Identify data integrity weaknesses and mark for repair."""
        if df.empty:
            return df

        df = df.copy()
        df['Integrity_Score'] = 100
        df['Weaknesses'] = ""
        df['Needs_Repair'] = False

        # Check 1: Missing Entry Anchors
        critical_anchors = ['Underlying_Price_Entry', 'Delta_Entry', 'Entry_Snapshot_TS']
        for col in critical_anchors:
            missing_mask = df[col].isna()
            if missing_mask.any():
                df.loc[missing_mask, 'Integrity_Score'] -= 30
                df.loc[missing_mask, 'Weaknesses'] += f"Missing {col}; "
                df.loc[missing_mask, 'Needs_Repair'] = True

        # Check 2: Stale Market Data
        if 'Snapshot_TS' in df.columns:
            now = pd.Timestamp.now()
            df['Data_Age_Hours'] = (now - pd.to_datetime(df['Snapshot_TS'])).dt.total_seconds() / 3600
            stale_mask = df['Data_Age_Hours'] > 24
            if stale_mask.any():
                df.loc[stale_mask, 'Integrity_Score'] -= 20
                df.loc[stale_mask, 'Weaknesses'] += "Stale market data (>24h); "

        # Check 3: Price Drift Anomaly
        if 'UL Last' in df.columns and 'Underlying_Price_Entry' in df.columns:
            df['Price_Drift_Pct'] = (df['UL Last'] - df['Underlying_Price_Entry']) / df['Underlying_Price_Entry'] * 100
            extreme_drift = df['Price_Drift_Pct'].abs() > 50
            if extreme_drift.any():
                df.loc[extreme_drift, 'Weaknesses'] += "Extreme price drift (>50%); "

        return df

    def analyze_holistically(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ticker-level aggregation and holistic risk assessment."""
        if df.empty:
            return df

        # RAG: Ensure all columns expected by PCS engine exist to prevent KeyErrors
        expected_cols = {
            'PCS': 75, 'PCS_Drift': 0, 'Chart_CompositeScore': 80,
            'Vega': 0.0, 'Gamma': 0.0, 'Theta': 0.0, 'Delta': 0.0,
            'Held_ROI%': 0.0, 'Days_Held': 1, 'Exit_Flag': False,
            'Chart_Trend': 'Unknown', 'Strategy': 'Unknown'
        }
        for col, default in expected_cols.items():
            if col not in df.columns:
                df[col] = default
            else:
                df[col] = df[col].fillna(default)

        # Calculate individual leg health using PCS engine
        df = pcs_engine_v3_unified(df)

        # Aggregate by Ticker
        ticker_groups = df.groupby('Underlying_Ticker')
        
        for ticker, group in ticker_groups:
            total_delta = group['Delta'].sum() if 'Delta' in group.columns else 0
            total_vega = group['Vega'].sum() if 'Vega' in group.columns else 0
            avg_health = group['PCS_UnifiedScore'].mean()
            has_repair_need = group['Needs_Repair'].any()
            
            # Strategy Balance Check (RAG: Buy-Write Doctrine)
            has_stock = (group['AssetType'] == 'STOCK').any()
            has_short_call = ((group['AssetType'] == 'OPTION') & (group['Quantity'] < 0) & (group['Symbol'].str.contains('C'))).any()
            
            # CSP Strategic Analysis (Deferred Entry + Income Kicker)
            csp_legs = group[group['Strategy'].str.contains('CSP|Cash-Secured Put', case=False, na=False)]
            is_csp_ticker = not csp_legs.empty
            
            strategy_weakness = ""
            if has_short_call and not has_stock:
                strategy_weakness = "Naked Call Risk (Missing Stock); "
            
            # Holistic Conclusion
            conclusion = "STABLE"
            rationale = "Edge maintained."
            
            # CSP Continuation Value Logic
            if is_csp_ticker:
                # Assignment Acceptability: Based on holistic ticker health
                assignment_acceptable = avg_health > 70
                
                # Post-assignment CC path: If health is strong, assignment is a win (deferred entry)
                if assignment_acceptable:
                    rationale = "CSP Continuation: Assignment acceptable (deferred entry path valid). "
                    if avg_health > 80:
                        conclusion = "HOLD_FOR_REVERSION"
                        rationale += "Strong ticker health; preserving expectancy for premium regeneration."
                else:
                    strategy_weakness += "CSP Risk: Ticker health weakening; assignment undesirable. "

            if avg_health > 82 and not is_csp_ticker:
                if total_delta > 0.7:
                    conclusion = "POTENTIAL_SCALE_UP"
                    rationale = f"Strong health ({avg_health:.1f}) and high delta ({total_delta:.2f}). Trend support confirmed."
                else:
                    conclusion = "HOLD_STRONG"
                    rationale = f"Strong health ({avg_health:.1f}) but delta ({total_delta:.2f}) below scale-up threshold."
            elif avg_health < 55:
                conclusion = "POTENTIAL_SCALE_DOWN"
                rationale = f"Health breakdown ({avg_health:.1f}). PCS significantly below threshold."
            elif avg_health < 65 and conclusion != "HOLD_FOR_REVERSION":
                conclusion = "WATCH_CLOSELY"
                rationale = f"Health weakening ({avg_health:.1f}). Monitor for further drift."
            
            if has_repair_need:
                conclusion = "REPAIR_REQUIRED"
                rationale = "Missing critical entry anchors. Data integrity violation."
            
            if strategy_weakness:
                rationale = strategy_weakness + rationale

            for idx in group.index:
                df.at[idx, 'Holistic_Conclusion'] = conclusion
                df.at[idx, 'Holistic_Rationale'] = rationale
                df.at[idx, 'Ticker_Total_Delta'] = total_delta
                df.at[idx, 'Ticker_Total_Vega'] = total_vega

        return df

    def repair_anchors(self, df: pd.DataFrame):
        """Repair missing anchors by tracing back to earliest DB records."""
        needs_repair = df[df['Needs_Repair']]
        if needs_repair.empty:
            return

        logger.info(f"🔧 Attempting repair for {len(needs_repair)} positions...")
        from core.management.cycle1.snapshot.snapshot import _recover_historical_anchors
        
        with duckdb.connect(self.db_path) as con:
            df_repaired = _recover_historical_anchors(con, needs_repair)
            
            for _, row in df_repaired.iterrows():
                if pd.notna(row['Underlying_Price_Entry']):
                    # Handle potential suffix from join
                    orig_ts_col = 'Entry_Snapshot_TS_anchor' if 'Entry_Snapshot_TS_anchor' in row else 'Entry_Snapshot_TS'
                    
                    con.execute("""
                        UPDATE entry_anchors 
                        SET Underlying_Price_Entry = ?, 
                            Delta_Entry = ?, 
                            Gamma_Entry = ?, 
                            Vega_Entry = ?, 
                            Theta_Entry = ?, 
                            Rho_Entry = ?, 
                            IV_Entry = ?, 
                            Entry_Snapshot_TS = ?,
                            Entry_Timestamp = ?
                        WHERE LegID = ? AND Entry_Snapshot_TS = ?
                    """, [
                        row['Underlying_Price_Entry'], row['Delta_Entry'], row['Gamma_Entry'],
                        row['Vega_Entry'], row['Theta_Entry'], row['Rho_Entry'],
                        row['IV_Entry'], row['Entry_Snapshot_TS'], row['Entry_Timestamp'],
                        row['LegID'], row[orig_ts_col]
                    ])
                    logger.info(f"✅ Repaired anchors for {row['Symbol']} (LegID: {row['LegID']})")

    def run_audit(self, auto_repair: bool = False):
        """Main execution loop for the auditor."""
        logger.info("🚀 Starting Holistic Audit...")
        
        df = self.load_active_state()
        if df.empty:
            logger.info("No active positions to audit.")
            return

        df = self.audit_integrity(df)
        
        if auto_repair:
            self.repair_anchors(df)
            df = self.load_active_state()
            df = self.audit_integrity(df)

        df = self.analyze_holistically(df)

        print("\n" + "="*80)
        print("📊 HOLISTIC AUDIT SUMMARY")
        print("="*80)
        
        summary = df[['Symbol', 'Integrity_Score', 'Trade_Health_Tier', 'Holistic_Conclusion', 'Holistic_Rationale']]
        print(summary.to_string(index=False))
        
        repair_needed = df[df['Needs_Repair']]
        if not repair_needed.empty:
            print("\n🔧 REPAIR REQUIRED FOR:")
            print(repair_needed[['Symbol', 'Weaknesses']].to_string(index=False))
            
        scale_up = df[df['Holistic_Conclusion'] == 'POTENTIAL_SCALE_UP']['Underlying_Ticker'].unique()
        if len(scale_up) > 0:
            print(f"\n📈 SCALE UP CANDIDATES: {list(scale_up)}")

        scale_down = df[df['Holistic_Conclusion'] == 'POTENTIAL_SCALE_DOWN']['Underlying_Ticker'].unique()
        if len(scale_down) > 0:
            print(f"\n📉 SCALE DOWN CANDIDATES: {list(scale_down)}")
            
        print("="*80 + "\n")

if __name__ == "__main__":
    import sys
    auto_repair = "--repair" in sys.argv
    auditor = HolisticAuditor()
    auditor.run_audit(auto_repair=auto_repair)
