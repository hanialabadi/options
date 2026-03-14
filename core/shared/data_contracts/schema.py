"""
Authoritative Schema Definitions

Enforces data contracts between Engines and UI.
Ensures the UI only consumes validated "Evidence Packets".

VOLATILITY DATA MODEL
=====================
Four distinct volatility layers, each with a current and entry (frozen) version.

IV_Contract (alias: IV_Now):
    Implied volatility of the specific option contract held.
    Source: Schwab /quotes API → contract["volatility"]
    Use: Vol Stop gate (per-contract IV spike detection), per-leg health.

IV_Underlying_30D (alias: IV_30D):
    ATM implied volatility of the underlying stock at ~30 DTE.
    Source: iv_term_history table (Schwab option chain → ATM call at ~30d expiry).
    Use: VRP drift (IV_Underlying_30D - HV_20D), Expected Move, regime classification.

IV_Rank (alias: IV_Percentile):
    Percentile rank of today's IV_Underlying_30D within its full history.
    Source: iv_term_history percentile query (min 20 trading days).
    Use: Chan strategy-aware exit, income eligibility, regime gates.

HV_20D:
    20-day realized (historical) volatility of the underlying stock.
    Source: Schwab price history / yfinance.
    Use: VRP baseline (IV - HV gap), vol regime detection.

Migration: Old names (IV_Now, IV_30D, IV_Percentile) are preserved as aliases.
           New names (IV_Contract, IV_Underlying_30D, IV_Rank) are canonical.
           Both exist in the schema during the transition period.
"""

import pandas as pd
import numpy as np # Import numpy for np.nan
import logging
from typing import List

logger = logging.getLogger(__name__)

# Authoritative column set for Management UI
MANAGEMENT_UI_COLUMNS = [
    # Identity
    "TradeID", "LegID", "Underlying_Ticker", "Symbol", "Strategy", "AssetType",
    "Account", "Entry_Structure",
    # Doctrine output
    "Action", "Decision_State", "Urgency", "Rationale", "Doctrine_Source",
    # Proposal-based resolution metadata (v2 doctrine)
    "Doctrine_State", "Resolution_Method", "Winning_Gate",
    "Proposals_Considered", "Proposals_Summary",
    # P&L
    "PnL_Total", "PnL_Unexplained", "PnL_Attribution_Quality",
    "$ Total G/L", "Total_GL_Decimal",
    # Position fields (Cycle 1 broker-reported)
    "Quantity", "Last", "Bid", "Ask",
    "UL Last", "Basis", "Strike", "Call/Put", "Expiration",
    "Earnings_Date",
    "Underlying_Price_Entry", "Premium_Entry",
    # Greeks
    "Delta", "Gamma", "Vega", "Theta", "Rho",
    "Delta_Entry", "Gamma_Entry", "Vega_Entry", "Theta_Entry",
    # IV / HV (see VOLATILITY DATA MODEL in module docstring for full lineage)
    # Canonical names (Phase 1 migration — aliases coexist with old names)
    "IV_Contract",          # per-contract option IV (= IV_Now)
    "IV_Underlying_30D",    # underlying ATM 30-day IV from iv_term_history (= IV_30D)
    "IV_Rank",              # percentile of IV_Underlying_30D in history (= IV_Percentile)
    # Legacy names (still populated — will be removed in Phase 3)
    "IV_30D", "IV_Entry", "HV_20D", "HV_20D_Source", "HV_20D_Age_Days",
    # Sigma-normalized z-scores (direction-adverse gate — Natenberg Ch.5 / Hull Ch.2)
    # ROC5_Z:     roc5 / (daily_sigma * sqrt(5)), None when HV unavailable (raw fallback)
    # Drift_Z:    Price_Drift_Pct / daily_sigma, None when HV unavailable
    # Sigma_Mode: True when z-score normalization active, False when raw % fallback
    "ROC5_Z", "Drift_Z", "Sigma_Mode",
    # Drift metrics
    "DTE", "Price_Drift_Pct", "Price_Drift_Abs",
    "Drift_Direction", "Dominant_Pressure", "Drift_Persistence",
    "Drift_Magnitude", "Lifecycle_Phase", "Attribution_Quality",
    "Days_In_Trade",
    # Drift engine authoritative action (DriftEngine._determine_action)
    "Drift_Action",
    # Greek ROC — normalized rate of change over 3D and 10D windows
    # ROC = (current - historical) / |historical|, clamped [-1, 1]
    # Feed: assess_signal_drift DEGRADED/VIOLATED thresholds (signed, thesis-aware)
    # 1D window: fallback for new positions with < 3 days history
    "Delta_ROC_1D", "Gamma_ROC_1D", "Vega_ROC_1D", "Theta_ROC_1D", "IV_ROC_1D",
    "Delta_ROC_3D", "Gamma_ROC_3D", "Vega_ROC_3D", "Theta_ROC_3D", "IV_ROC_3D",
    "Delta_ROC_10D", "Gamma_ROC_10D", "Vega_ROC_10D", "Theta_ROC_10D", "IV_ROC_10D",
    # ROC persistence: consecutive snapshots with negative Delta_ROC_3D (deterioration)
    # assess_signal_drift requires ROC_Persist_3D >= 2 before escalating to DEGRADED
    "ROC_Persist_3D",
    # Entry displacement (t_now − t₀): how far each metric has moved from freeze anchor.
    # Computed by compute_windowed_drift._compute_structural_drift() using entry_anchors.
    # Delta_Drift_Structural = Delta - Delta_Entry
    # Vega_Drift_Structural  = Vega  - Vega_Entry
    # IV_Drift_Structural    = IV_Now - IV_Entry  (normalized to decimal)
    # Price_Drift_Structural = UL_Last - Underlying_Price_Entry
    "Delta_Drift_Structural", "Vega_Drift_Structural",
    "IV_Drift_Structural", "Price_Drift_Structural",
    # Vol-state diagnostics
    # IV_vs_HV_Gap: IV_Now - HV_20D (positive=selling edge, negative=crush)
    # IV_Percentile: 0-100 rank of today's IV within its own history (≥20 days required)
    # IV_Percentile_Depth: number of trading days in IV history — < 45 = LOW CONFIDENCE
    "IV_vs_HV_Gap", "IV_Percentile", "IV_Percentile_Depth",
    # Equity Integrity State — lightweight structural deterioration for stock-backed positions.
    # Scores 7 signals (MA slopes, ROC20, drawdown zones, HV regime, ATR expansion).
    # States: HEALTHY | WEAKENING (1-2 signals) | BROKEN (3+ or critical drawdown).
    # Only populated on STOCK/EQUITY legs; options carry HEALTHY by default.
    "Equity_Integrity_State", "Equity_Integrity_Reason",
    # Position Trajectory — lifecycle-aware regime classification (Cycle 2.85).
    # Distinguishes "sideways income" from "chasing strikes" via stock trajectory + roll history.
    # States: SIDEWAYS_INCOME | TRENDING_CHASE | RECOVERY_GRIND | MEAN_REVERSION | NEUTRAL
    "Position_Regime", "Position_Regime_Reason",
    "Trajectory_Stock_Return", "Trajectory_MFE", "Trajectory_MAE",
    "Trajectory_Range_Ratio", "Trajectory_Strike_Crossings", "Trajectory_Slope",
    "Trajectory_Consecutive_Debit_Rolls", "Trajectory_Roll_Efficiency_Trend",
    "Trajectory_Total_Roll_Cost", "Trajectory_IV_Change",
    # Sector Relative Strength — z-score normalized vs sector ETF benchmark.
    # Computed by thesis_engine.compute_sector_relative_strength() on every run.
    # States: OUTPERFORMING | NEUTRAL | UNDERPERFORMING | MICRO_BREAKDOWN | BROKEN
    # BROKEN at 2σ+ divergence triggers Thesis_State → DEGRADED (Natenberg Ch.8).
    "Sector_Relative_Strength", "Sector_RS_ZScore", "Sector_Benchmark",
    # Thesis State — aggregate thesis health (computed by thesis_engine.py)
    # INTACT | DEGRADED | BROKEN | UNKNOWN
    "Thesis_State", "Thesis_Drawdown_Type", "Thesis_Summary",
    # Cross-signal convergence: long-vol structure + chop + IV compression = silent bleed
    "_Structural_Decay_Regime",
    # Chart states (11 states)
    "PriceStructure_State", "TrendIntegrity_State", "VolatilityState_State",
    "CompressionMaturity_State", "MomentumVelocity_State", "DirectionalBalance_State",
    "RangeEfficiency_State", "TimeframeAgreement_State", "GreekDominance_State",
    "AssignmentRisk_State", "RegimeStability_State",
    # Chart state temporal memory (persistence tracking — from state_drift_engine)
    "MomentumVelocity_State_Days", "MomentumVelocity_State_Prev", "MomentumVelocity_State_Change",
    "GreekDominance_State_Days", "GreekDominance_State_Prev", "GreekDominance_State_Change",
    "TrendIntegrity_State_Days", "TrendIntegrity_State_Prev", "TrendIntegrity_State_Change",
    "VolatilityState_State_Days", "VolatilityState_State_Prev", "VolatilityState_State_Change",
    "PriceStructure_State_Days", "PriceStructure_State_Prev", "PriceStructure_State_Change",
    "RegimeStability_State_Days", "RegimeStability_State_Prev", "RegimeStability_State_Change",
    # Entry Chart States (frozen at inception — for thesis persistence tracking)
    "Entry_Chart_State_PriceStructure", "Entry_Chart_State_TrendIntegrity",
    "Entry_Chart_State_VolatilityState", "Entry_Chart_State_CompressionMaturity",
    # Chart primitives (required inputs for MomentumVelocity and other state computations)
    "roc_5", "roc_10", "roc_20", "momentum_slope", "price_acceleration",
    "ema20_slope", "ema50_slope", "ema_alignment_score", "adx_14",
    "atr_14", "bb_width_pct", "bb_width_z", "kaufman_efficiency_ratio",
    # Scan-engine indicators (real values from technical_indicators DuckDB table)
    "rsi_14", "macd", "macd_signal", "slow_k_5_3", "slow_d_5_3",
    "choppiness_index", "hv_20d_percentile",
    # Raw price levels for pullback anchors (McMillan Ch.4: scale-up entry levels)
    "EMA9", "SMA20", "SMA50", "LowerBand_20", "UpperBand_20",
    # IV term structure (from iv_history.duckdb — Natenberg Ch.5/11)
    "iv_surface_shape", "iv_ts_slope_30_90", "iv_ts_slope_30_180",
    # Recovery analysis (Cycle 2 computed — Natenberg Ch.5)
    "HV_Daily_Move_1Sigma", "Recovery_Move_Required", "Recovery_Move_Per_Day",
    "Recovery_Feasibility",
    # Margin carry cost (Fidelity 10.375%/yr — McMillan Ch.3 / Passarelli Ch.6)
    # Daily_Margin_Cost: $ interest per day on this position's market value (silent P&L drain)
    # Margin_Coverage_Days: short premium only — theta income ÷ daily margin cost (>1.0 = covering carry)
    "Daily_Margin_Cost", "Margin_Coverage_Days",
    # Cumulative carry cost (MarginCarryCalculator — McMillan Ch.3 / Passarelli Ch.6)
    # Is_Retirement: True for Roth/IRA/401K — no margin interest applies
    "Is_Retirement",
    "Cumulative_Margin_Carry", "Carry_Adjusted_GL", "Carry_Adjusted_GL_Pct",
    "Carry_Theta_Ratio", "Carry_Classification",
    # BW/CC Efficiency Scorecard (BWEfficiencyCalculator)
    "Net_Yield_Annual_Pct", "Premium_vs_Carry_Ratio",
    "Days_Until_Carry_Eats_GL", "Carry_Efficiency_Grade",
    # Portfolio-level Greeks (from DriftEngine)
    "Portfolio_Net_Delta", "Portfolio_Net_Vega", "Portfolio_Net_Gamma", "Portfolio_Net_Theta",
    "Portfolio_Delta_Utilization_Pct", "Portfolio_Vega_Utilization_Pct",
    "Portfolio_Gamma_Utilization_Pct", "Portfolio_Theta_Utilization_Pct",
    # Portfolio risk flags (from check_portfolio_limits + sector concentration)
    "Portfolio_Risk_Flags", "Portfolio_State",
    # Sector classification (human-readable bucket from sector_benchmarks.py)
    "Sector_Bucket",
    # ETF detection flag (from config/sector_benchmarks.py — macro-vol context for CC)
    "Is_ETF",
    # Macro event proximity (from config/macro_calendar.py — static Fed/BLS/BEA schedule)
    "Days_To_Macro", "Macro_Next_Event", "Macro_Next_Type",
    "Macro_Next_Date", "Is_Macro_Week", "Macro_Density", "Macro_Strip",
    # Correlation risk (from analyze_correlation_risk in phase5_portfolio_limits.py)
    "Positions_On_Underlying", "Underlying_Concentration_Risk",
    "Strategy_Concentration", "Strategy_Correlation_Risk",
    # Smart price refresh (Feature B)
    "Price_Source", "Price_TS",
    # Live Greeks (Schwab chain refresh — market hours; transient, not frozen in anchors)
    "IV_Now", "Delta_Live", "Gamma_Live", "Vega_Live", "Theta_Live",
    "Greeks_Source", "Greeks_TS",
    # Pre-doctrine data integrity gate
    "Pre_Doctrine_Flag", "Pre_Doctrine_Detail",
    # Directional thesis price target — frozen at entry (Natenberg Ch.11: Thesis Satisfaction)
    # Price_Target_Entry: 1-sigma IV-implied target price for LONG_PUT (downside) or LONG_CALL (upside)
    # Formula: UL_Entry × (1 ∓ IV_Entry × √(DTE_Entry/252)) — frozen once, never re-computed
    # Feeds Gate 2.5 in _long_option_doctrine: if stock reaches this level + gain≥30% → EXIT/TRIM
    "Price_Target_Entry", "DTE_Entry",
    # Buy-Write cost basis tracking (McMillan Ch.3: progressive cost reduction across cycles)
    # Gross_Premium_Collected: total credits received before buyback costs
    # Total_Close_Cost: total paid to buy back calls early (debit rolls)
    # Has_Debit_Rolls: True when at least one cycle was closed at a debit
    # Roll_Net_Credit / Roll_Prior_Credit: net debit/credit of the most recent roll
    "Net_Cost_Basis_Per_Share", "Breakeven_Price", "Cumulative_Premium_Collected",
    "Gross_Premium_Collected", "Total_Close_Cost", "Has_Debit_Rolls",
    "Roll_Net_Credit", "Roll_Prior_Credit", "_cycle_count",
    # Fidelity-sourced position fields (additive — Cycle 1 expanded ingest)
    # Open_Int: open interest on the option leg — liquidity gate for roll candidates (OI < 500 = thin)
    # Intrinsic_Val: broker-reported intrinsic value — 0 for OTM, >0 for ITM (precise assignment risk)
    "Open_Int", "OI_Entry", "Intrinsic_Val",
    # OI Deterioration gate output (Murphy 0.704 — set by decision engine)
    "OI_Deterioration_Warning",
    # Vol/Regime context frozen at entry (RAG gap analysis — Bennett/Natenberg/Krishnan/Jabbour)
    # Canonical entry names (aliases coexist during migration)
    "IV_Contract_Entry",        # per-contract IV at entry (= IV_Entry)
    "IV_Underlying_30D_Entry",  # underlying ATM 30d IV at entry (= IV_30D_Entry)
    "IV_Rank_Entry",            # IV_Rank at entry (= IV_Percentile_Entry)
    # Legacy entry names (still populated)
    "IV_30D_Entry", "HV_20D_Entry", "IV_Percentile_Entry",
    "Regime_Entry", "Expected_Move_10D_Entry", "Daily_Margin_Cost_Entry",
    # Vol Stop gate output (Given 0.677 — IV rise >50% from entry on short-vol)
    "Vol_Stop_Warning",
    # VRP Drift (Bennett 0.719 — IV-HV gap drift from entry baseline)
    "VRP_Entry", "VRP_Now", "VRP_Drift",
    # Roll candidates (JSON — populated when Action=ROLL, ranked by delta fit + liquidity + cost)
    "Roll_Candidate_1", "Roll_Candidate_2", "Roll_Candidate_3",
    # Condition monitor (Feature A — populated by ConditionMonitor before doctrine)
    "_Active_Conditions", "_Condition_Resolved",
    # Data quality
    "Structural_Data_Complete", "Resolution_Reason",
    "Data_State", "Signal_State", "Structural_State", "Regime_State",
    # Scan provenance — per-contract link from Step12_Acceptance (injected by run_all.py ScanFeedback)
    # Populated when Ticker+Strike+Expiration+Option_Type matches a scan candidate.
    # Enables "why was this trade surfaced?" visibility in management.
    "Scan_DQS_Score", "Scan_Thesis", "Scan_Theory_Source", "Scan_Trade_Bias",
    "Scan_Gate_Reason", "Scan_Entry_Timing", "Scan_Confidence",
    # Forward expectancy (Cycle 2.6.5 — McMillan Ch.4: Forward Expectancy)
    # Expected_Move_10D:          1-sigma 10-day expected move (IV-based, not HV)
    # Required_Move_Breakeven:    distance from current price to breakeven strike
    # Required_Move_50pct:        50% recovery target (halfway to breakeven)
    # EV_Feasibility_Ratio:       Required_Move_Breakeven / Expected_Move_10D
    # EV_50pct_Feasibility_Ratio: Required_Move_50pct / Expected_Move_10D
    # Theta_Bleed_Daily_Pct:      abs(Theta) / Last × 100 (%/day of premium)
    # Theta_Opportunity_Cost_Flag: True when long-premium + bleed > 3%/day
    # Theta_Opportunity_Cost_Pct:  same as Theta_Bleed_Daily_Pct (schema alias)
    "Expected_Move_10D", "Required_Move_Breakeven", "Required_Move_50pct",
    "EV_Feasibility_Ratio", "EV_50pct_Feasibility_Ratio",
    # Profit_Cushion:       intrinsic value for ITM options (adverse move buffer in $)
    # Profit_Cushion_Ratio: Profit_Cushion / Expected_Move_10D (how many 10D sigmas of protection)
    "Profit_Cushion", "Profit_Cushion_Ratio",
    "Theta_Bleed_Daily_Pct", "Theta_Opportunity_Cost_Flag", "Theta_Opportunity_Cost_Pct",
    # Conviction decay (Cycle 2.95 — Passarelli Ch.2: Conviction Decay)
    # Delta_Deterioration_Streak: consecutive cycles with Delta_ROC_3D < -0.05
    # Conviction_Status:          STRENGTHENING | STABLE | WEAKENING | REVERSING
    # Conviction_Fade_Days:       total days in last 10 with deteriorating delta
    "Delta_Deterioration_Streak", "Conviction_Status", "Conviction_Fade_Days",
    # Action streak (Cycle 2.955 — auto-resolve persistent REVALIDATE / stale EXIT)
    # Prior_Action_Streak: consecutive calendar days where the most recent Action repeated
    "Prior_Action_Streak",
    # Signal coherence (Cycle 3 — Natenberg Ch.7: adjustment frequency / Passarelli Ch.5: deliberate timing)
    # Days_Since_Last_Roll: calendar days since current option leg was opened (from premium_ledger.opened_at)
    # Signal_Stability_Warning: intraday annotation when same-day run produced a different action
    "Days_Since_Last_Roll", "Signal_Stability_Warning",
    # Run metadata
    "run_id", "Snapshot_TS", "Schema_Hash", "ingest_context",
    # Capital architecture (Cycle 3 — regime gate + bucket classification)
    "Regime_Gate", "Capital_Bucket",
    # Regime × strategy family intelligence (Session B — Natenberg Ch.19, McMillan Ch.1, Passarelli Ch.2)
    "Regime_Strategy_Fit", "Regime_Strategy_Note",
    "Surface_Shape_Warning", "Surface_Shape_Warning_Note",
    # Weighting Wheel Assessment (Passarelli Ch.1: intentional assignment via CSP → CC cycle)
    # Wheel_Ready: all 4 conditions pass → assignment is a feature, not a failure
    # Wheel_Basis: effective cost basis per share (Net_Cost_Basis > Broker > Strike-Premium)
    # Wheel_IV_Ok: IV_Now ≥ 25% → enough premium to sell covered calls post-assignment
    # Wheel_Chart_Ok: chart structure not broken → stock worth owning
    # Wheel_Capital_Ok: portfolio delta utilization < 15% → capacity to absorb shares
    "Wheel_Ready", "Wheel_Note", "Wheel_Basis",
    "Wheel_IV_Ok", "Wheel_Chart_Ok", "Wheel_Capital_Ok",
    # Scale-up persistence (McMillan Ch.4: Pyramid on Strength)
    # Scale_Trigger_Price: pullback level (EMA9/SMA20/Lower BB) where add-on becomes actionable
    #   Persisted when Action=SCALE_UP so the NEXT run fires HIGH-urgency when UL touches it.
    # Scale_Add_Contracts: deterministic add-on size (½-size capped by EWMA-CVaR + delta util)
    "Scale_Trigger_Price", "Scale_Add_Contracts",
    # Pyramid tier tracking (Murphy: each add smaller than the last)
    # Pyramid_Tier: 0=base, 1=first add, 2=second add, 3=max (no more adds)
    # Winner_Lifecycle: THESIS_UNPROVEN | THESIS_CONFIRMED | CONVICTION_BUILDING
    #                   | FULL_POSITION | THESIS_EXHAUSTING
    "Pyramid_Tier", "Winner_Lifecycle",
    # Intraday advisory (BREAKOUT_UP / BREAKOUT_DOWN rolls only)
    # JSON blob: {proxy_verdict, proxy_summary, signals, notes, checklist}
    # Passarelli Ch.6: intraday execution timing affects fill quality.
    "Intraday_Advisory_JSON",
    # Portfolio Circuit Breaker (Phase 6 — McMillan Ch.3: Portfolio-level risk control)
    # Circuit_Breaker_State: OPEN | WARNING | TRIPPED
    # Circuit_Breaker_Reason: human-readable trigger explanation (empty when OPEN)
    "Circuit_Breaker_State", "Circuit_Breaker_Reason",
    # Exit Coordinator (Phase 6 — Passarelli Ch.6: execution sequencing)
    # Exit_Sequence: integer priority (1 = execute first), NaN when ≤3 exits
    # Exit_Priority_Reason: why this position has its sequence rank
    "Exit_Sequence", "Exit_Priority_Reason",
    # Exit Limit Pricer (Phase 1 — daily technical level targets for EXIT execution)
    # Uses delta approximation + EMA9/SMA20/BB to suggest limit prices instead of market orders.
    "Exit_Limit_Price", "Exit_Limit_Level",
    "Exit_Limit_Rationale", "Exit_Limit_Patience_Days",
    # Exit Optimal Window (Phase 2 — intraday execution timing for EXIT HIGH/CRITICAL)
    # Reuses Intraday_Advisory_JSON for display. These columns classify the window state.
    "Exit_Window_State", "Exit_Window_Reason",
    # Cross-Leg Direction Reversal Gate (Natenberg Ch.11 / Passarelli Ch.6)
    # Detects when executing EXIT on one leg flips the combined underlying delta direction.
    # Direction_Reversal_Warning: human-readable warning text (empty if no reversal)
    # Post_Exit_Net_Delta: combined net delta AFTER executing all EXITs on this underlying
    # Direction_Shift: e.g. "Neutral → Bullish" — direction label change
    "Direction_Reversal_Warning", "Post_Exit_Net_Delta", "Direction_Shift",
    # CC Opportunity Engine (Cycle 3 — idle stock positions)
    # Evaluates whether writing covered calls is currently favorable for each uncovered
    # stock position.  Written by cc_opportunity_engine.evaluate_cc_opportunities().
    # CC_Proposal_Status:    FAVORABLE | UNFAVORABLE | SCAN_MISS | ERROR
    # CC_Proposal_Verdict:   human-readable summary
    # CC_Unfavorable_Reason: pipe-delimited list of blocking reasons
    # CC_Watch_Signal:       condition to monitor before re-evaluating
    # CC_IV_Rank:            IV_Rank at time of scan (0-100)
    # CC_Regime:             vol regime label from scan (e.g. 'High Vol')
    # CC_Best_Ann_Yield:     best annualised yield across candidate calls
    # CC_Best_DTE_Bucket:    DTE bucket label for best candidate (e.g. '30-45D')
    # CC_Scan_TS:            scan timestamp string
    # CC_Candidate_1/2/3:    JSON blobs for ranked call candidates
    "CC_Proposal_Status", "CC_Proposal_Verdict", "CC_Unfavorable_Reason",
    "CC_Watch_Signal", "CC_IV_Rank", "CC_Regime", "CC_Best_Ann_Yield",
    "CC_Best_DTE_Bucket", "CC_Scan_TS",
    "CC_Candidate_1", "CC_Candidate_2", "CC_Candidate_3",
    # CC Recovery Mode (Cycle 3 — Jabbour Ch.4: recovery-aware covered call logic)
    # CC_Recovery_Mode:        INCOME | RECOVERY | DEEP_RECOVERY (based on drift from cost basis)
    # CC_Recovery_Gap:         gap to breakeven in $/share (0 when INCOME)
    # CC_Recovery_Monthly_Est: estimated monthly premium income (HV-based conservative estimate)
    # CC_Recovery_Months:      gap / monthly_est — months of rolling to close the gap
    "CC_Recovery_Mode", "CC_Recovery_Gap", "CC_Recovery_Monthly_Est", "CC_Recovery_Months",
    # CC Ladder (Cycle 3 — tiered partial-coverage for large positions)
    # Jabbour Ch.4: ratio writes/partial coverage; Passarelli Ch.6: credit aggregation
    "CC_Ladder_Eligible", "CC_Ladder_JSON",
    "CC_Ladder_Total_Lots", "CC_Ladder_Covered_Lots",
    "CC_Ladder_Tier_A_Lots", "CC_Ladder_Tier_B_Lots", "CC_Ladder_Tier_C_Lots",
    "CC_Ladder_Monthly_Est", "CC_Ladder_Income_Gap_Ratio", "CC_Ladder_Recovery_Months",
    # Decision Ledger (Cycle 3 — continuous trade memory)
    # Tracks execution confirmation and decision stability across the life of a position.
    # Execution_Pending:        True when a ROLL/EXIT has been executed but broker data not yet refreshed
    # Last_Execution_Action:    most recent executed action (ROLL/EXIT/TRIM)
    # Last_Execution_TS:        timestamp of the execution mark
    # Decision_Flip_Count_5D:   number of action changes in the last 5 calendar days (≥3 = instability)
    "Execution_Pending", "Last_Execution_Action", "Last_Execution_TS", "Decision_Flip_Count_5D",
    # Earnings history analytics (from earnings_stats table — DuckDB read-only, no live API)
    # Populated by run_all.py Earnings History Enrichment section
    "Earnings_Beat_Rate",              # float 0-1: fraction of BEAT quarters
    "Earnings_Avg_IV_Crush_Pct",       # float: mean IV crush across events (negative = crush)
    "Earnings_Avg_IV_Ramp_Pct",        # float: mean pre-earnings IV buildup
    "Earnings_Avg_Expected_Move_Pct",  # float: mean straddle-implied move
    "Earnings_Avg_Actual_Move_Pct",    # float: mean realized move
    "Earnings_Avg_Move_Ratio",         # float: actual/expected (>1=underpriced, <1=overpriced)
    "Earnings_Avg_Gap_Pct",            # float: mean |gap| on earnings day
    "Earnings_Last_Surprise_Pct",      # float: most recent EPS surprise %
    "Earnings_Track_Quarters",         # int: number of quarters with data

    # Earnings formation detection (Phase 1→2→3 analysis — DuckDB read-only)
    # Populated by run_all.py Earnings Formation Enrichment section
    "Earnings_Phase2_Start_Day",       # float: avg D-X when positioning begins (negative)
    "Earnings_Drift_Predicted_Gap_Rate",  # float 0-1: fraction of events where drift predicted gap
    "Earnings_Formation_Quality",      # str: COMPLETE/PARTIAL/INSUFFICIENT
    "Earnings_Current_Phase",          # str: QUIET/EARLY_POSITIONING/LATE_POSITIONING/IMMINENT

    # Execution Readiness (Phase 2 — calendar-aware timing from execution_readiness.py)
    # EXECUTE_NOW | STAGE_AND_RECHECK | WAIT_FOR_WINDOW | NOT_APPLICABLE
    "Execution_Readiness", "Execution_Readiness_Reason",

    # Monte Carlo Verdicts (Phase 3 — MC-informed wait/hold/assignment signals)
    "MC_Wait_Verdict", "MC_Wait_Reason", "MC_Wait_Note",
    "MC_Hold_Verdict",
    "MC_Assign_P_Expiry", "MC_Assign_P_Touch", "MC_Assign_Urgency", "MC_Assign_Note",

    # MC Optimal Exit Timing (mc_optimal_exit.py — day-by-day GBM peak EV)
    "MC_Optimal_Exit_DTE", "MC_Exit_Peak_EV", "MC_Exit_Terminal_EV",
    "MC_Exit_Theta_Crossover", "MC_Exit_Note",

    # Recovery Reconciler (Cycle 2.347 — ticker-level share coverage analysis)
    "Recovery_Total_Shares", "Recovery_Covered_Shares", "Recovery_Idle_Shares",
    "Recovery_Gap_Per_Share", "Recovery_Income_Baseline_Mo", "Recovery_Income_Full_Mo",
    "Recovery_Months_Baseline", "Recovery_Months_Full", "Recovery_Acceleration_Pct",
    "Recovery_Cover_Idle_Recommended", "Recovery_IV_HV_OK", "Recovery_CC_Favorable",

    # Doctrine diagnostics (orchestrator.py — visible in dashboard for debugging)
    "Uncertainty_Reasons", "Missing_Data_Fields", "Scan_Conflict",

    # Macro catalyst protection flag (long_option.py — extended macro window)
    # True when doctrine cleared prior EXIT due to imminent HIGH-impact macro event.
    # Used by MC EXIT_NOW guard in run_all.py to suppress hard override.
    "Macro_Catalyst_Protected",
]

def enforce_management_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Strictly filters and validates the dataframe for UI consumption.
    """
    if df.empty:
        return pd.DataFrame(columns=MANAGEMENT_UI_COLUMNS)
        
    # 1. Identify missing columns and fill with appropriate defaults
    for col in MANAGEMENT_UI_COLUMNS:
        if col not in df.columns:
            logger.warning(f"⚠️ Schema Violation: Missing column '{col}'. Filling with default.")
            # Determine default based on expected type (simplified for common numeric/string)
            if "PnL_" in col or "Delta" in col or "Gamma" in col or "Vega" in col or "Theta" in col or "IV_" in col or "HV_" in col or "Price_" in col or "Total_GL_Decimal" in col or "Basis" in col or "UL Last" in col or "Margin_" in col or "Daily_Margin" in col or "Recovery_" in col or col in ("Open_Int", "Intrinsic_Val", "Scan_DQS_Score", "Expected_Move_10D", "Required_Move_Breakeven", "Required_Move_50pct", "EV_Feasibility_Ratio", "EV_50pct_Feasibility_Ratio", "Theta_Bleed_Daily_Pct", "Theta_Opportunity_Cost_Pct", "Delta_Deterioration_Streak", "Conviction_Fade_Days", "Price_Target_Entry", "DTE_Entry", "Gross_Premium_Collected", "Total_Close_Cost", "Roll_Net_Credit", "Roll_Prior_Credit", "_cycle_count", "Decision_Flip_Count_5D"):
                df[col] = np.nan  # Numeric columns
            elif col in ("Days_To_Macro", "Macro_Density", "Days_Since_Last_Roll"):
                df[col] = np.nan  # Numeric macro proximity / signal coherence
            elif col in ("MC_Optimal_Exit_DTE", "MC_Exit_Peak_EV", "MC_Exit_Terminal_EV",
                         "MC_Exit_Theta_Crossover"):
                df[col] = np.nan  # Numeric MC optimal exit columns
            elif col in ("MC_Exit_Note",):
                df[col] = ""      # MC exit note — string
            elif col in ("Macro_Next_Event", "Macro_Next_Type", "Macro_Next_Date", "Macro_Strip",
                         "Signal_Stability_Warning"):
                df[col] = ""      # String macro context / signal coherence
            elif col in ("Has_Debit_Rolls", "Wheel_Ready", "Wheel_IV_Ok", "Wheel_Chart_Ok", "Wheel_Capital_Ok", "CC_Ladder_Eligible", "Is_ETF", "Is_Macro_Week", "Execution_Pending", "Macro_Catalyst_Protected"):
                df[col] = False   # Boolean default
            elif col in ("Wheel_Note",):
                df[col] = ""      # Wheel note: empty string default
            elif col in ("Wheel_Basis", "Scale_Trigger_Price"):
                df[col] = np.nan  # Numeric
            elif col in ("Scale_Add_Contracts",):
                df[col] = np.nan  # Integer (nullable via float NaN)
            elif col in ("Intraday_Advisory_JSON", "Exit_Priority_Reason", "Circuit_Breaker_Reason",
                         "CC_Ladder_JSON", "Exit_Limit_Level", "Exit_Limit_Rationale",
                         "Exit_Window_State", "Exit_Window_Reason"):
                df[col] = ""      # String, empty when inactive
            elif col in ("Exit_Limit_Price", "Exit_Limit_Patience_Days"):
                df[col] = np.nan  # Numeric — NaN when not computed
            elif col.startswith("CC_Ladder_") and col not in ("CC_Ladder_Eligible", "CC_Ladder_JSON"):
                df[col] = np.nan  # Numeric ladder columns
            elif col in ("Exit_Sequence", "Post_Exit_Net_Delta"):
                df[col] = np.nan  # Numeric sequence / delta (NaN when not computed)
            elif col in ("Direction_Reversal_Warning", "Direction_Shift"):
                df[col] = ""      # String, empty when no reversal detected
            elif col in ("Circuit_Breaker_State",):
                df[col] = "OPEN"  # Default breaker state
            elif col in ("Doctrine_State", "Resolution_Method", "Winning_Gate",
                         "Proposals_Summary"):
                df[col] = ""      # Proposal resolution metadata — empty when v1 path
            elif col in ("Proposals_Considered",):
                df[col] = np.nan  # Integer — NaN when v1 path
            elif col in ("Portfolio_Risk_Flags", "Sector_Bucket"):
                df[col] = ""      # Empty string default
            elif col in ("Portfolio_State",):
                df[col] = "NOMINAL"
            elif col in ("Underlying_Concentration_Risk", "Strategy_Correlation_Risk"):
                df[col] = "LOW"
            elif col in ("Portfolio_Gamma_Utilization_Pct", "Portfolio_Theta_Utilization_Pct",
                         "Positions_On_Underlying", "Strategy_Concentration"):
                df[col] = np.nan  # Numeric
            elif col in ("EMA9", "SMA20", "SMA50", "LowerBand_20", "UpperBand_20"):
                df[col] = 0.0    # Price levels — 0 means unavailable
            elif col in ("Earnings_Beat_Rate", "Earnings_Avg_IV_Crush_Pct",
                         "Earnings_Avg_IV_Ramp_Pct", "Earnings_Avg_Expected_Move_Pct",
                         "Earnings_Avg_Actual_Move_Pct", "Earnings_Avg_Move_Ratio",
                         "Earnings_Avg_Gap_Pct", "Earnings_Last_Surprise_Pct"):
                df[col] = np.nan  # Numeric earnings analytics
            elif col == "Earnings_Track_Quarters":
                df[col] = 0       # Integer — 0 means no data
            elif col in ("Earnings_Phase2_Start_Day", "Earnings_Drift_Predicted_Gap_Rate"):
                df[col] = np.nan  # Numeric formation analytics
            elif col in ("Earnings_Formation_Quality", "Earnings_Current_Phase"):
                df[col] = ""      # String — empty when no formation data
            elif col in ("ROC5_Z", "Drift_Z"):
                df[col] = np.nan  # Numeric z-scores — NaN for non-long-option strategies
            elif col in ("Sigma_Mode",):
                df[col] = False   # Boolean — False when z-score normalization not applicable
            elif col.startswith("Roll_Candidate_") or col == "Roll_Split_Suggestion":
                df[col] = None    # JSON columns — None (not "N/A") so DB IS NULL works
            else:
                df[col] = "N/A"   # String/categorical columns
            
    # 2. Return strictly ordered subset
    return df[MANAGEMENT_UI_COLUMNS].copy()
