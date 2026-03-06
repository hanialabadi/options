"""
Trust-First Recommendation Engine

Implements the 10-point philosophy for earning trader trust before recommending action.
Focuses on data integrity, structural clarity, and honest failure transparency.
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from agents.chart_agent import pcs_engine_v3_unified
from agents.persona_engine import TraderPersona
from core.phase5_portfolio_limits import compute_portfolio_greeks, get_persona_limits

logger = logging.getLogger(__name__)

class TrustAgent:
    def __init__(self, persona: Optional[TraderPersona] = None):
        self.persona = persona or TraderPersona(
            name="conservative",
            max_loss_pct=-30,
            max_gain_pct=50,
            scaling_factor=1.0,
            pcs_threshold=70
        )

    def audit_data_integrity(self, row: pd.Series) -> Tuple[int, str, List[str]]:
        """
        1️⃣ DATA INTEGRITY — “AM I EVEN LOOKING AT THE TRUTH?”
        """
        score = 100
        status = "Entry data is valid"
        weaknesses = []
        
        # Critical anchors
        anchors = {
            'Underlying_Price_Entry': row.get('Underlying_Price_Entry'),
            'Delta_Entry': row.get('Delta_Entry'),
            'Entry_Snapshot_TS': row.get('Entry_Snapshot_TS')
        }
        
        missing = [k for k, v in anchors.items() if pd.isna(v)]
        
        if missing:
            score = 0
            status = f"Entry data is incomplete — conclusions downgraded. Missing: {', '.join(missing)}"
            weaknesses.append("MISSING_ANCHORS")
        
        # Check for repairs
        if row.get('ID_REPAIR') or row.get('HISTORICAL_RECOVERY'):
            status = f"Entry data was missing and has been repaired from {row.get('Entry_Snapshot_TS')}"
            weaknesses.append("REPAIRED")

        return score, status, weaknesses

    def classify_structure(self, row: pd.Series) -> str:
        """
        2️⃣ POSITION STRUCTURE — “WHAT EXACTLY IS THIS TRADE?”
        """
        symbol = str(row.get('Symbol', ''))
        asset_type = row.get('AssetType', 'UNKNOWN')
        strategy = str(row.get('Strategy', '')).upper()
        
        if asset_type == 'STOCK':
            return "This is a long equity position."
        
        if "CSP" in strategy or "CASH_SECURED_PUT" in strategy:
            return "This is a Cash-Secured Put: Deferred stock entry + income kicker."
            
        side = "long" if row.get('Quantity', 0) > 0 else "short"
        option_type = "call" if "C" in symbol else "put" if "P" in symbol else "option"
        dte = row.get('DTE', row.get('Days_To_Expiration', 0))
        
        if dte > 90:
            horizon = "long-dated"
        elif dte < 14:
            horizon = "short-term"
        else:
            horizon = "standard-cycle"
            
        if side == "short":
            return f"This is a {horizon} short premium position relying on decay."
        else:
            return f"This is a {horizon} directional {option_type}."

    def analyze_sensitivities(self, row: pd.Series) -> Tuple[str, str]:
        """
        3️⃣ SENSITIVITY CHECK — “WHAT IS ACTUALLY MOVING MY P&L?”
        """
        delta = abs(row.get('Delta', 0))
        vega = abs(row.get('Vega', 0))
        theta = abs(row.get('Theta', 0))
        
        greeks = {'Delta': delta, 'Vega': vega, 'Theta': theta}
        dominant = max(greeks, key=greeks.get)
        
        # Simple logic for impact
        pnl = row.get('Unrealized_PnL', 0)
        if pnl > 0:
            impact = "working"
        elif pnl < 0:
            impact = "actively hurting the trade"
        else:
            impact = "neutral"
            
        explanation = f"This trade is mainly driven by {dominant}. This dominance is {impact}."
        return dominant, explanation

    def check_time_intent(self, row: pd.Series) -> str:
        """
        4️⃣ TIME & INTENT — “IS THIS STILL WITHIN THE PLAN?”
        """
        days_held = row.get('Days_Held', 0)
        dte = row.get('DTE', 30)
        total_cycle = days_held + dte
        
        if total_cycle == 0: return "Cycle unknown."
        
        progress = days_held / total_cycle
        
        if progress < 0.25:
            return "The trade is still early relative to its horizon."
        elif progress < 0.75:
            return "The trade is mid-cycle and behaving as expected."
        else:
            return "The trade is late-cycle and requires tighter discipline."

    def evaluate_thesis(self, row: pd.Series) -> Tuple[str, str]:
        """
        5️⃣ THESIS CHECK — “IS MY ORIGINAL IDEA STILL TRUE?”
        """
        pcs_drift = row.get('PCS_Drift', 0)
        chart_regime = row.get('Chart_Regime', 'Neutral')
        
        # RAG: Robust ROI Calculation. 
        # Try multiple sources for ROI to ensure we don't miss a "broken" trade.
        roi = row.get('ROI_Pct')
        if pd.isna(roi):
            roi = row.get('Held_ROI%')
        
        if pd.isna(roi):
            # Fallback to P&L / Capital calculation
            pnl = row.get('Unrealized_PnL', 0)
            capital = row.get('Capital_Deployed', row.get('Basis', 0))
            if abs(capital) > 0.01:
                roi = pnl / abs(capital)
            else:
                roi = 0.0
        
        # RAG: ROI-Aware Thesis. If a trade is down significantly, the thesis is challenged regardless of Greeks.
        # A trade that is 80% broken MUST be flagged as BROKEN.
        if roi < -0.50 or pcs_drift < -25:
            state = "THESIS_BROKEN"
            reason = f"Severe drawdown ({roi:.1%}) or structural collapse has invalidated the trade logic."
        elif roi < -0.20 or pcs_drift < -10 or chart_regime in ['Bearish', 'Breakdown']:
            state = "THESIS_WEAKENING"
            reason = f"Price action, drawdown ({roi:.1%}), or structural drift is challenging the original idea."
        elif pcs_drift > 5 and chart_regime in ['Bullish', 'Accumulation'] and roi > -0.10:
            state = "THESIS_STRENGTHENING"
            reason = "Underlying trend is intact and quality metrics are improving."
        else:
            state = "THESIS_INTACT"
            reason = "Volatility and price remain within acceptable parameters."
            
        return state, reason

    def aggregate_ticker_thinking(self, ticker_df: pd.DataFrame) -> Tuple[str, str]:
        """
        6️⃣ TICKER-LEVEL THINKING — “AM I OVEREXPOSED WITHOUT REALIZING IT?”
        """
        total_delta = ticker_df.get('Delta', pd.Series([0]*len(ticker_df))).sum()
        pos_count = len(ticker_df)
        
        if pos_count > 3 or abs(total_delta) > 2.0:
            status = "Exposure is excessive — risk reduction advised"
            reason = f"Concentration in {ticker_df.iloc[0].get('Underlying_Ticker', 'UNKNOWN')} is high ({pos_count} positions, {total_delta:.2f} net delta)."
        elif pos_count > 1:
            status = "Exposure is elevated — scaling should be cautious"
            reason = f"Multiple positions in {ticker_df.iloc[0].get('Underlying_Ticker', 'UNKNOWN')} amplify risk."
        else:
            status = "Exposure is intentional and acceptable"
            reason = "Single position within normal limits."
            
        return status, reason

    def analyze_portfolio_context(self, df: pd.DataFrame) -> Tuple[str, str]:
        """
        7️⃣ PORTFOLIO CONTEXT — “DOES THIS FIT WITH EVERYTHING ELSE?”
        """
        portfolio_greeks = compute_portfolio_greeks(df)
        limits = get_persona_limits(self.persona.name if hasattr(self.persona, 'name') else 'conservative')
        
        # Check for fragility (e.g., high short vega or high net delta)
        fragility_signals = []
        if portfolio_greeks['net_vega'] < limits['max_short_vega']:
            fragility_signals.append("High Short Vega concentration")
        if abs(portfolio_greeks['net_delta']) > limits['max_net_delta']:
            fragility_signals.append("High directional exposure")
            
        if fragility_signals:
            status = "Portfolio fragility is elevated"
            reason = f"Current portfolio has {', '.join(fragility_signals)}. Adding risk here increases fragility."
        else:
            status = "Portfolio balance is acceptable"
            reason = "Trade fits within current portfolio risk limits."
            
        return status, reason

    def decide_action(self, 
                       integrity_score: int,
                       thesis_state: str,
                       health_score: float,
                       ticker_status: str,
                       portfolio_status: str,
                       row: Optional[pd.Series] = None) -> Tuple[str, str]:
        """
        8️⃣ ACTION DECISION — “WHAT SHOULD I ACTUALLY DO?”
        """
        if integrity_score < 100:
            return "REPAIR_REQUIRED", "Cannot judge trade with corrupted data memory."
            
        strategy = str(row.get('Strategy', '')).upper() if row is not None else ""
        is_csp = "CSP" in strategy or "CASH_SECURED_PUT" in strategy

        if thesis_state == "THESIS_BROKEN":
            return "EXIT", "Thesis is invalidated. Markets don't care about entry price."
            
        if thesis_state == "THESIS_WEAKENING":
            return "SCALE_DOWN", "Thesis is weakening. Risk reduction advised to preserve capital."

        if ticker_status.startswith("Exposure is excessive"):
            return "SCALE_DOWN", "Concentration risk outweighs individual trade merit."
            
        if portfolio_status.startswith("Portfolio fragility is elevated") and health_score < 80:
            return "WATCH_CLOSELY", "Portfolio risk is high; individual trade merit is insufficient to override fragility."

        # CSP Specific Trust Logic: Expectancy Preservation
        if is_csp and health_score > 75:
            pcs_drift = row.get('PCS_Drift', 0) if row is not None else 0
            if pcs_drift > 15:
                return "HOLD_FOR_REVERSION", "Extended success with intact structure; preserving expectancy for premium regeneration."

        if health_score > 80 and thesis_state == "THESIS_STRENGTHENING":
            if portfolio_status.startswith("Portfolio fragility is elevated"):
                return "HOLD", "Trade is strong but portfolio limits prevent scaling up."
            return "SCALE_UP", "High confidence in both structure and trend."
            
        if health_score < 60:
            return "WATCH_CLOSELY", "Position is drifting toward exit threshold."
            
        return "HOLD", "Thesis intact and risk is balanced."

    def process_position(self, row: pd.Series, ticker_df: pd.DataFrame, full_df: pd.DataFrame) -> Dict:
        """
        🔟 FINAL OUTPUT STANDARD (HUMAN-READABLE)
        """
        integrity_score, integrity_verdict, weaknesses = self.audit_data_integrity(row)
        
        # If integrity is not 100, we downgrade everything
        if integrity_score < 100:
            return {
                "Symbol": row.get('Symbol'),
                "Data Integrity": integrity_verdict,
                "Structure": "UNKNOWN",
                "Drivers": "UNKNOWN",
                "Time Context": "UNKNOWN",
                "Thesis": "UNKNOWN",
                "Ticker Context": "UNKNOWN",
                "Portfolio Context": "UNKNOWN",
                "Action": "REPAIR_REQUIRED",
                "Rationale": "Trust cannot be established due to missing historical anchors.",
                "Transparency": "CRITICAL: Missing historical anchors."
            }
            
        structure = self.classify_structure(row)
        dominant_greek, sensitivity = self.analyze_sensitivities(row)
        time_context = self.check_time_intent(row)
        thesis_state, thesis_reason = self.evaluate_thesis(row)
        ticker_status, ticker_reason = self.aggregate_ticker_thinking(ticker_df)
        portfolio_status, portfolio_reason = self.analyze_portfolio_context(full_df)
        
        # Use PCS for health
        health_score = row.get('PCS_UnifiedScore', 0)
        
        action, action_rationale = self.decide_action(
            integrity_score, thesis_state, health_score, ticker_status, portfolio_status, row
        )
        
        # 9️⃣ FAILURE TRANSPARENCY
        missing_info = []
        if pd.isna(row.get('Chart_Regime')): missing_info.append("Chart state")
        if pd.isna(row.get('IV')): missing_info.append("Greeks (IV)")
        if pd.isna(row.get('Days_Held')) and pd.isna(row.get('Entry_Snapshot_TS')): missing_info.append("Time context")
        
        transparency = ""
        if missing_info:
            transparency = f"NOTE: Missing {', '.join(missing_info)}. Confidence downgraded."

        return {
            "Symbol": row.get('Symbol'),
            "Data Integrity": integrity_verdict,
            "Structure": structure,
            "Drivers": sensitivity,
            "Time Context": time_context,
            "Thesis": f"{thesis_state}: {thesis_reason}",
            "Ticker Context": ticker_reason,
            "Portfolio Context": portfolio_reason,
            "Action": action,
            "Rationale": action_rationale,
            "Transparency": transparency
        }

def run_trust_audit(df: pd.DataFrame) -> List[Dict]:
    """Helper to run audit on a dataframe of positions."""
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        return []
    
    if not isinstance(df, pd.DataFrame):
        return []

    df = df.copy()
    
    # Ensure health scores exist using the robust engine
    df = pcs_engine_v3_unified(df)
    
    agent = TrustAgent()
    results = []
    
    # RAG: Noise Reduction. Filter out standalone stock legs if they are part of a multi-leg strategy.
    # We prefer to audit the OPTION leg as the primary representative of the strategy.
    if 'AssetType' in df.columns and 'Strategy' in df.columns:
        # Keep options, and keep stocks ONLY if they are not part of a multi-leg strategy
        # (i.e., Strategy == 'STOCK_ONLY')
        df_audit = df[
            (df['AssetType'] == 'OPTION') | 
            (df['Strategy'] == 'STOCK_ONLY')
        ].copy()
    else:
        df_audit = df.copy()

    # Ensure Underlying_Ticker exists for grouping
    if 'Underlying_Ticker' not in df_audit.columns:
        df_audit['Underlying_Ticker'] = 'UNKNOWN'
        
    for ticker in df_audit['Underlying_Ticker'].unique():
        ticker_df = df_audit[df_audit['Underlying_Ticker'] == ticker]
        for _, row in ticker_df.iterrows():
            results.append(agent.process_position(row, ticker_df, df_audit))
            
    return results

if __name__ == "__main__":
    # Test with dummy data
    test_df = pd.DataFrame([{
        'Symbol': 'AAPL250620C150',
        'Underlying_Ticker': 'AAPL',
        'AssetType': 'OPTION',
        'Quantity': 1,
        'Underlying_Price_Entry': 145.0,
        'Delta_Entry': 0.6,
        'Entry_Snapshot_TS': '2025-01-01',
        'Delta': 0.7,
        'Vega': 0.1,
        'Theta': -0.05,
        'Unrealized_PnL': 500,
        'Days_Held': 20,
        'DTE': 120,
        'PCS_Drift': 2,
        'Chart_Regime': 'Bullish',
        'PCS_UnifiedScore': 85,
        'IV': 0.25,
        'Strategy': 'Buy Call'
    }])
    
    recs = run_trust_audit(test_df)
    for r in recs:
        print("\n--- TRUST REPORT ---")
        for k, v in r.items():
            if v: print(f"{k}: {v}")
