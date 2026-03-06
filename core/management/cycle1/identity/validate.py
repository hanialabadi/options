"""
Phase 2C: Structural Validation Gate

This module performs read-only integrity checks on the output of Phase 2B
(strategy tagging). It does NOT mutate TradeID, Strategy, LegType, Account,
or Structure columns. It only appends validation flags.

Purpose:
--------
- Enforce structural contracts before Phase 3 enrichment runs
- Flag cross-account trades, missing legs, illegal combinations
- Provide diagnostic visibility without auto-correction

Architecture:
-------------
This is a GATE, not a fixer. If validation fails, flags are raised,
but no rows are dropped and no strategies are reassigned.

Phase 3 must remain unchanged. This validation runs inside Phase 2.
"""

import pandas as pd
import logging
from typing import List, Dict, Set, Tuple
from pathlib import Path
import numpy as np
from core.management.cycle1.identity.constants import (
    # Strategy constants
    STRATEGY_COVERED_CALL,
    STRATEGY_CSP,
    STRATEGY_LONG_STRADDLE,
    STRATEGY_LONG_STRANGLE,
    STRATEGY_BUY_CALL,
    STRATEGY_BUY_PUT,
    ALL_STRATEGIES,
    # LegType constants
    ALL_LEG_TYPES,
    # AssetType constants
    ASSET_TYPE_STOCK,
    ASSET_TYPE_OPTION,
    ALL_ASSET_TYPES,
    # OptionType constants
    OPTION_TYPE_CALL,
    OPTION_TYPE_PUT,
    ALL_OPTION_TYPES,
    # Stock classification constants
    STOCK_OPTION_STATUS_NOT_OPTIONABLE,
    STOCK_OPTION_STATUS_OPTIONABLE,
    OPTION_USAGE_NONE,
    OPTION_USAGE_ACTIVE,
)

# Configure logger for this module
logger = logging.getLogger(__name__)





def validate_covered_call_coverage(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate Covered Call coverage across the entire account/underlying.
    """
    for (account, underlying), group in df.groupby(["Account", "Underlying_Ticker"]):
        stock_shares = group[(group["AssetType"] == ASSET_TYPE_STOCK) & (group["Quantity"] > 0)]["Quantity"].sum()
        
        cc_short_calls = group[
            (group["Strategy"] == STRATEGY_COVERED_CALL) & 
            (group["AssetType"] == ASSET_TYPE_OPTION) & 
            (group["OptionType"] == OPTION_TYPE_CALL) & 
            (group["Quantity"] < 0)
        ]
        
        if cc_short_calls.empty:
            continue
            
        total_contracts = abs(cc_short_calls["Quantity"].sum())
        required_shares = total_contracts * 100
        
        df.loc[cc_short_calls.index, "Covered_Call_Stock_Shares"] = stock_shares
        df.loc[cc_short_calls.index, "Covered_Call_Contracts"] = total_contracts
        df.loc[cc_short_calls.index, "Covered_Call_Coverage_Ratio"] = stock_shares / required_shares if required_shares > 0 else np.inf
        
        if stock_shares < required_shares:
            for idx in cc_short_calls.index:
                existing_errors = str(df.loc[idx, "Validation_Errors"])
                new_error = "STRUCTURE_VIOLATION:Insufficient_Stock_Coverage"
                df.loc[idx, "Validation_Errors"] = f"{existing_errors} | {new_error}".strip(" | ")
                df.loc[idx, "Structure_Valid"] = False
                df.loc[idx, "Needs_Structural_Fix"] = True

    return df


def validate_structures(df: pd.DataFrame) -> pd.DataFrame:
    """
    Phase 2C: Structural validation gate.
    
    Performs read-only structural integrity checks on parsed positions:
    
    1. TradeID Integrity: Single account, single underlying per TradeID
    2. Strategy ↔ Leg Consistency: Verify legs match strategy expectations
    3. Structural Sanity: No illegal combinations (e.g., stock in straddles)
    """
    # === Input validation ===
    required_cols = [
        "TradeID", "Strategy", "Account", "Underlying_Ticker", 
        "AssetType", "OptionType", "LegType", "Strike", 
        "Expiration", "Quantity", "Symbol"
    ]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"❌ Phase 2C: Missing required columns: {missing}")
    
    # Initialize validation columns
    df["Structure_Valid"] = True
    df["Validation_Errors"] = ""
    df["Needs_Structural_Fix"] = False
    
    # Initialize metrics columns
    for col in ["Covered_Call_Stock_Shares", "Covered_Call_Contracts", "Covered_Call_Coverage_Ratio"]:
        df[col] = np.nan
    
    # === Step B: Detect Option Usage ===
    used_tickers = set(df[df['AssetType'] == ASSET_TYPE_OPTION]['Underlying_Ticker'].unique())
    df['Stock_Used_In_Options'] = df['Underlying_Ticker'].isin(used_tickers)
    
    # Initialize classification columns
    df['Option_Usage'] = OPTION_USAGE_NONE
    df.loc[df['Stock_Used_In_Options'], 'Option_Usage'] = OPTION_USAGE_ACTIVE
    
    # === PRE-VALIDATION: Enum constraint checks ===
    
    # Validate Strategy values
    invalid_strategies = ~df["Strategy"].isin(ALL_STRATEGIES)
    if invalid_strategies.any():
        invalid_vals = df.loc[invalid_strategies, "Strategy"].unique()
        raise ValueError(f"❌ FATAL: Invalid Strategy values detected: {list(invalid_vals)}")
    
    # Validate AssetType values
    invalid_assets = ~df["AssetType"].isin(ALL_ASSET_TYPES)
    if invalid_assets.any():
        invalid_vals = df.loc[invalid_assets, "AssetType"].unique()
        raise ValueError(f"❌ FATAL: Invalid AssetType values detected: {list(invalid_vals)}")
    
    # Validate LegType values
    invalid_legs = ~df["LegType"].isin(ALL_LEG_TYPES)
    if invalid_legs.any():
        invalid_vals = df.loc[invalid_legs, "LegType"].unique()
        raise ValueError(f"❌ FATAL: Invalid LegType values detected: {list(invalid_vals)}")
    
    # Validate OptionType values (for OPTIONS only)
    option_mask = df["AssetType"] == ASSET_TYPE_OPTION
    if option_mask.any():
        invalid_opttypes = ~df.loc[option_mask, "OptionType"].isin(ALL_OPTION_TYPES)
        if invalid_opttypes.any():
            invalid_vals = df.loc[option_mask][invalid_opttypes]["OptionType"].unique()
            raise ValueError(f"❌ FATAL: Invalid OptionType values for OPTION positions: {list(invalid_vals)}")
        
        # Validate Expiration is not NaT for OPTIONS
        nat_expiration = df.loc[option_mask, "Expiration"].isna()
        if nat_expiration.any():
            raise ValueError(f"❌ FATAL: OPTION positions have NaT Expiration.")
        
        # Validate Strike is valid
        invalid_strike = df.loc[option_mask, "Strike"].isna() | (df.loc[option_mask, "Strike"] <= 0)
        if invalid_strike.any():
            raise ValueError(f"❌ FATAL: OPTION positions have invalid Strike.")
    
    # Track errors per TradeID
    trade_errors: Dict[str, List[str]] = {}
    
    # === Single-pass validation: All checks in one groupby loop ===
    for trade_id, group in df.groupby("TradeID"):
        errors: List[str] = []
        strategy = group["Strategy"].iloc[0]
        
        # 1️⃣ TradeID Integrity Checks
        if group["Account"].nunique() > 1:
            errors.append("Cross_Account_TradeID")
            df.loc[group.index, "Needs_Structural_Fix"] = True
        
        if group["Underlying_Ticker"].nunique() > 1:
            errors.append("Mixed_Underlying")
            df.loc[group.index, "Needs_Structural_Fix"] = True
        
        # 2️⃣ Extract position types
        stock_legs = group[group["AssetType"] == "STOCK"]
        option_legs = group[group["AssetType"] == "OPTION"]
        call_legs = option_legs[option_legs["OptionType"] == OPTION_TYPE_CALL]
        put_legs = option_legs[option_legs["OptionType"] == OPTION_TYPE_PUT]
        short_calls = call_legs[call_legs["Quantity"] < 0]
        long_calls = call_legs[call_legs["Quantity"] > 0]
        short_puts = put_legs[put_legs["Quantity"] < 0]
        long_puts = put_legs[put_legs["Quantity"] > 0]
        
        # 3️⃣ Strategy ↔ Leg Consistency Checks
        
        # === Covered Call Validation ===
        if strategy == STRATEGY_COVERED_CALL:
            is_used = group["Stock_Used_In_Options"].iloc[0]
            if not is_used:
                continue

            if short_calls.empty:
                errors.append("Missing_Leg:SHORT_CALL")
                df.loc[group.index, "Needs_Structural_Fix"] = True
            else:
                if short_calls["Expiration"].nunique() > 1:
                    errors.append("Invalid_Structure:Multiple_Expirations")
                    df.loc[group.index, "Needs_Structural_Fix"] = True
            
            if not put_legs.empty:
                errors.append("Extra_Leg:PUT")
                df.loc[group.index, "Needs_Structural_Fix"] = True
        

        # === Cash-Secured Put Validation ===
        elif strategy == STRATEGY_CSP:
            if len(option_legs) != 1 or short_puts.empty or len(short_puts) != 1:
                errors.append("Invalid_Structure:CSP_Must_Be_Single_Short_Put")
                df.loc[group.index, "Needs_Structural_Fix"] = True
            if not stock_legs.empty:
                errors.append("Extra_Leg:STOCK")
                df.loc[group.index, "Needs_Structural_Fix"] = True
        
        # === Long Straddle Validation ===
        elif strategy == STRATEGY_LONG_STRADDLE:
            if len(call_legs) != 1 or len(put_legs) != 1:
                errors.append("Missing_Leg:Straddle_Incomplete")
                df.loc[group.index, "Needs_Structural_Fix"] = True
            else:
                if call_legs["Strike"].iloc[0] != put_legs["Strike"].iloc[0]:
                    errors.append("Strategy_Mismatch:Strikes_Differ")
                    df.loc[group.index, "Needs_Structural_Fix"] = True
            if not stock_legs.empty:
                errors.append("Illegal_Leg_Combination:Stock_In_Straddle")
                df.loc[group.index, "Needs_Structural_Fix"] = True
        
        # === Long Strangle Validation ===
        elif strategy == STRATEGY_LONG_STRANGLE:
            if len(call_legs) != 1 or len(put_legs) != 1:
                errors.append("Missing_Leg:Strangle_Incomplete")
                df.loc[group.index, "Needs_Structural_Fix"] = True
            else:
                if call_legs["Strike"].iloc[0] == put_legs["Strike"].iloc[0]:
                    errors.append("Strategy_Mismatch:Same_Strike_Not_Strangle")
                    df.loc[group.index, "Needs_Structural_Fix"] = True
            if not stock_legs.empty:
                errors.append("Illegal_Leg_Combination:Stock_In_Strangle")
                df.loc[group.index, "Needs_Structural_Fix"] = True

        # 4️⃣ Structural Sanity Checks
        if "LegCount" in group.columns:
            if group["LegCount"].iloc[0] != len(group):
                errors.append(f"LegCount_Mismatch:Expected={group['LegCount'].iloc[0]}_Actual={len(group)}")
                df.loc[group.index, "Needs_Structural_Fix"] = True
        
        if errors:
            trade_errors[trade_id] = sorted(errors)
    
    # === Apply validation results ===
    truly_invalid_trade_ids = []
    all_critical_errors = []

    for trade_id, errors in trade_errors.items():
        indices = df[df["TradeID"] == trade_id].index
        df.loc[indices, "Validation_Errors"] = " | ".join(errors)
        
        critical_errors = [
            e for e in errors 
            if not e.startswith("STRUCTURE_WARNING") 
            and not (e.startswith("Unknown_Strategy") and (df.loc[indices, "AssetType"] == ASSET_TYPE_STOCK).all())
        ]
        
        if critical_errors:
            df.loc[indices, "Structure_Valid"] = False
            truly_invalid_trade_ids.append(trade_id)
            all_critical_errors.extend(critical_errors)
        else:
            df.loc[indices, "Structure_Valid"] = True
            df.loc[indices, "Needs_Structural_Fix"] = False
    
    # === Summary reporting ===
    total_trades = df["TradeID"].nunique()
    if truly_invalid_trade_ids:
        logger.warning(f"Phase 2C Validation: {len(truly_invalid_trade_ids)}/{total_trades} TradeIDs invalid.")
    else:
        logger.info(f"Phase 2C Validation: All {total_trades} TradeIDs structurally valid.")
    
    # === Step D: Cross-Trade Coverage Validation ===
    df = validate_covered_call_coverage(df)
    
    return df


def get_validation_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Generate a summary report of validation results."""
    if "Structure_Valid" not in df.columns:
        raise ValueError("❌ Validation columns not found. Run validate_structures() first.")
    
    summary = df.groupby("TradeID").agg({
        "Strategy": "first",
        "Account": "first",
        "Underlying_Ticker": "first",
        "Structure_Valid": "first",
        "Validation_Errors": "first",
        "Needs_Structural_Fix": "any"
    }).reset_index()
    
    invalid = summary[~summary["Structure_Valid"]]
    if invalid.empty:
        logger.info("No structural validation errors found.")
        return summary
    else:
        logger.warning(f"{len(invalid)} TradeIDs with validation errors.")
        return invalid


def enforce_validation_gate(df: pd.DataFrame, strict: bool = True) -> pd.DataFrame:
    """Optional execution guard: Block Phase 3 if structural issues detected."""
    if "Structure_Valid" not in df.columns:
        raise ValueError("❌ Validation columns not found. Run validate_structures() first.")
    
    invalid_count = (~df["Structure_Valid"]).sum()
    if invalid_count == 0:
        logger.info("Validation gate: All structures valid, proceeding...")
        return df
    
    if strict:
        raise ValueError(f"❌ Cannot proceed to Phase 3: {invalid_count} positions failed validation.")
    else:
        logger.warning(f"VALIDATION WARNING: {invalid_count} positions failed validation but continuing...")
        return df
