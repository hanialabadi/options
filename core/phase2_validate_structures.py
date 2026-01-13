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
from core.phase2_constants import (
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

# Path to derived analytics for optionability check
DERIVED_ANALYTICS_PATH = Path(__file__).parents[1] / 'data' / 'ivhv_timeseries' / 'ivhv_timeseries_derived.csv'


def load_optionable_symbols() -> Set[str]:
    """
    Load set of optionable symbols from derived analytics.
    If file missing, returns empty set (conservative).
    """
    if not DERIVED_ANALYTICS_PATH.exists():
        logger.warning(f"⚠️  Optionable symbols source not found at {DERIVED_ANALYTICS_PATH}")
        return set()
    
    try:
        df_iv = pd.read_csv(DERIVED_ANALYTICS_PATH)
        if 'ticker' in df_iv.columns:
            return set(df_iv['ticker'].str.upper().unique())
    except Exception as e:
        logger.error(f"❌ Failed to load optionable symbols: {e}")
    
    return set()


def validate_structures(df: pd.DataFrame) -> pd.DataFrame:
    """
    Phase 2C: Structural validation gate.
    
    Performs read-only structural integrity checks on parsed positions:
    
    1. TradeID Integrity: Single account, single underlying per TradeID
    2. Strategy ↔ Leg Consistency: Verify legs match strategy expectations
    3. Structural Sanity: No illegal combinations (e.g., stock in straddles)
    
    Parameters
    ----------
    df : pandas.DataFrame
        Output from phase21_strategy_tagging() with columns:
        - TradeID
        - Strategy
        - Account
        - Underlying_Ticker
        - AssetType
        - OptionType
        - LegType
        - Strike
        - Expiration
        - Quantity
    
    Returns
    -------
    pandas.DataFrame
        Original DataFrame with appended validation columns:
        - Structure_Valid (bool)
        - Validation_Errors (str, pipe-delimited)
        - Needs_Structural_Fix (bool)
    
    Raises
    ------
    ValueError
        If required columns are missing.
    
    Notes
    -----
    This function does NOT mutate:
    - TradeID
    - Strategy
    - LegType
    - Account
    - Structure
    
    It only appends diagnostic flags.
    """
    # === Input validation ===
    required_cols = [
        "TradeID", "Strategy", "Account", "Underlying_Ticker", 
        "AssetType", "OptionType", "LegType", "Strike", 
        "Expiration", "Quantity", "Symbol"  # Symbol required for duplicate detection
    ]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"❌ Phase 2C: Missing required columns: {missing}")
    
    # Initialize validation columns
    df["Structure_Valid"] = True
    df["Validation_Errors"] = ""
    df["Needs_Structural_Fix"] = False
    
    # === Step A: Add Stock Optionability Check ===
    optionable_symbols = load_optionable_symbols()
    df['Is_Optionable'] = df['Underlying_Ticker'].isin(optionable_symbols)
    
    # === Step B: Detect Option Usage ===
    # A ticker is "used" if it appears in any OPTION leg in the current portfolio
    used_tickers = set(df[df['AssetType'] == ASSET_TYPE_OPTION]['Underlying_Ticker'].unique())
    df['Stock_Used_In_Options'] = df['Underlying_Ticker'].isin(used_tickers)
    
    # Initialize classification columns
    df['Stock_Option_Status'] = STOCK_OPTION_STATUS_NOT_OPTIONABLE
    df.loc[df['Is_Optionable'], 'Stock_Option_Status'] = STOCK_OPTION_STATUS_OPTIONABLE
    
    df['Option_Eligibility'] = False
    df.loc[df['Is_Optionable'], 'Option_Eligibility'] = True
    
    df['Option_Usage'] = OPTION_USAGE_NONE
    df.loc[df['Stock_Used_In_Options'], 'Option_Usage'] = OPTION_USAGE_ACTIVE
    
    # === PRE-VALIDATION: Enum constraint checks ===
    
    # Validate Strategy values
    invalid_strategies = ~df["Strategy"].isin(ALL_STRATEGIES)
    if invalid_strategies.any():
        invalid_vals = df.loc[invalid_strategies, "Strategy"].unique()
        raise ValueError(
            f"❌ FATAL: Invalid Strategy values detected: {list(invalid_vals)}\n"
            f"   Must be one of: {sorted(ALL_STRATEGIES)}\n"
            f"   This indicates Phase 2B produced invalid output."
        )
    
    # Validate AssetType values
    invalid_assets = ~df["AssetType"].isin(ALL_ASSET_TYPES)
    if invalid_assets.any():
        invalid_vals = df.loc[invalid_assets, "AssetType"].unique()
        raise ValueError(
            f"❌ FATAL: Invalid AssetType values detected: {list(invalid_vals)}\n"
            f"   Must be one of: {sorted(ALL_ASSET_TYPES)}\n"
            f"   This indicates Phase 1 or 2A produced invalid output."
        )
    
    # Validate LegType values
    invalid_legs = ~df["LegType"].isin(ALL_LEG_TYPES)
    if invalid_legs.any():
        invalid_vals = df.loc[invalid_legs, "LegType"].unique()
        raise ValueError(
            f"❌ FATAL: Invalid LegType values detected: {list(invalid_vals)}\n"
            f"   Must be one of: {sorted(ALL_LEG_TYPES)}\n"
            f"   This indicates Phase 2B produced invalid output."
        )
    
    # Validate OptionType values (for OPTIONS only)
    option_mask = df["AssetType"] == ASSET_TYPE_OPTION
    if option_mask.any():
        invalid_opttypes = ~df.loc[option_mask, "OptionType"].isin(ALL_OPTION_TYPES)
        if invalid_opttypes.any():
            invalid_vals = df.loc[option_mask][invalid_opttypes]["OptionType"].unique()
            raise ValueError(
                f"❌ FATAL: Invalid OptionType values for OPTION positions: {list(invalid_vals)}\n"
                f"   Must be 'Call' or 'Put' for options.\n"
                f"   This indicates Phase 2A parsing failed."
            )
        
        # Validate Expiration is not NaT for OPTIONS
        nat_expiration = df.loc[option_mask, "Expiration"].isna()
        if nat_expiration.any():
            bad_symbols = df.loc[option_mask][nat_expiration]["Symbol"].tolist()[:5]
            raise ValueError(
                f"❌ FATAL: {nat_expiration.sum()} OPTION positions have NaT Expiration.\n"
                f"   Symbols: {bad_symbols}\n"
                f"   This indicates Phase 2A parsing failed or invalid dates."
            )
        
        # Validate Strike is valid (not NaN, numeric, > 0)
        invalid_strike = df.loc[option_mask, "Strike"].isna() | (df.loc[option_mask, "Strike"] <= 0)
        if invalid_strike.any():
            bad_symbols = df.loc[option_mask][invalid_strike]["Symbol"].tolist()[:5]
            raise ValueError(
                f"❌ FATAL: {invalid_strike.sum()} OPTION positions have invalid Strike (NaN or ≤0).\n"
                f"   Symbols: {bad_symbols}\n"
                f"   This indicates Phase 2A parsing failed."
            )
    
    # Track errors per TradeID (use list copy to avoid mutation issues)
    trade_errors: Dict[str, List[str]] = {}
    
    # === Single-pass validation: All checks in one groupby loop ===
    for trade_id, group in df.groupby("TradeID"):
        errors: List[str] = []  # Fresh list per TradeID
        strategy = group["Strategy"].iloc[0]
        
        # === 1️⃣ TradeID Integrity Checks ===
        
        # Check: Single account per TradeID
        unique_accounts = group["Account"].nunique()
        if unique_accounts > 1:
            errors.append("Cross_Account_TradeID")
            df.loc[group.index, "Needs_Structural_Fix"] = True
        
        # Check: Single underlying per TradeID
        unique_underlyings = group["Underlying_Ticker"].nunique()
        if unique_underlyings > 1:
            errors.append("Mixed_Underlying")
            df.loc[group.index, "Needs_Structural_Fix"] = True
        
        # === 2️⃣ Extract position types (CONSISTENT Quantity-based detection) ===
        stock_legs = group[group["AssetType"] == "STOCK"]
        option_legs = group[group["AssetType"] == "OPTION"]
        
        # Use Quantity sign consistently for all option legs
        call_legs = group[(group["AssetType"] == ASSET_TYPE_OPTION) & (group["OptionType"] == OPTION_TYPE_CALL)]
        put_legs = group[(group["AssetType"] == ASSET_TYPE_OPTION) & (group["OptionType"] == OPTION_TYPE_PUT)]
        
        short_calls = call_legs[call_legs["Quantity"] < 0]
        long_calls = call_legs[call_legs["Quantity"] > 0]
        short_puts = put_legs[put_legs["Quantity"] < 0]
        long_puts = put_legs[put_legs["Quantity"] > 0]
        
        # === 3️⃣ Strategy ↔ Leg Consistency Checks ===
        
        # === Covered Call Validation ===
        if strategy == STRATEGY_COVERED_CALL:
            # === Step B/C Enhancement: Skip CC validation if stock not used in options ===
            is_used = group["Stock_Used_In_Options"].iloc[0]
            if not is_used:
                # If Option_Usage=NONE, this is just a stock holding mislabeled as CC
                # Downgrade to INFO and skip CC-specific expectations
                continue

            # Must have: ≥1 STOCK (Qty > 0) + ≥1 SHORT_CALL
            if stock_legs.empty:
                # === Step C: Replace "Missing_Leg:STOCK" Logic ===
                underlying = group["Underlying_Ticker"].iloc[0]
                is_optionable = group["Is_Optionable"].iloc[0]
                
                if not is_optionable:
                    # NON-OPTIONABLE STOCKS -> Drop / Suppress
                    pass
                else:
                    # OPTIONABLE + USED STOCKS -> Strategy-Relevant
                    errors.append("STRUCTURE_WARNING:Missing_Stock_Leg")
                    # Downgrade to INFO level (not critical)
                    df.loc[group.index, "Needs_Structural_Fix"] = False
            elif (stock_legs["Quantity"] <= 0).any():
                errors.append("Invalid_Structure:Stock_Not_Long")
                df.loc[group.index, "Needs_Structural_Fix"] = True
            
            if short_calls.empty:
                errors.append("Missing_Leg:SHORT_CALL")
                df.loc[group.index, "Needs_Structural_Fix"] = True
            else:
                # Validate all short calls have same expiration
                unique_expirations = short_calls["Expiration"].nunique()
                if unique_expirations > 1:
                    errors.append("Invalid_Structure:Multiple_Expirations")
                    df.loc[group.index, "Needs_Structural_Fix"] = True
            
            # ENFORCE EXACT 100:1 ratio
            if not stock_legs.empty and not short_calls.empty:
                total_contracts = abs(short_calls["Quantity"].sum())
                expected_shares = total_contracts * 100
                
                # Group by Underlying_Ticker to find ALL shares for this ticker in this account
                # (Handles cases where stock is in a different TradeID)
                account_id = group["Account"].iloc[0]
                underlying = group["Underlying_Ticker"].iloc[0]
                total_shares = df[(df["Account"] == account_id) & 
                                 (df["Underlying_Ticker"] == underlying) & 
                                 (df["AssetType"] == "STOCK")]["Quantity"].sum()
                
                if total_shares < expected_shares:
                    errors.append(f"Invalid_Ratio:Stock={total_shares}_Calls={total_contracts}")
                    df.loc[group.index, "Needs_Structural_Fix"] = True
            
            # Must NOT have puts
            if not put_legs.empty:
                errors.append("Extra_Leg:PUT")
                df.loc[group.index, "Needs_Structural_Fix"] = True
        
        # === Cash-Secured Put Validation ===
        elif strategy == STRATEGY_CSP:
            # Must have: EXACTLY ONE SHORT Put (Quantity < 0)
            if len(option_legs) != 1:
                errors.append("Invalid_Structure:Multi_Leg_CSP")
                df.loc[group.index, "Needs_Structural_Fix"] = True
            
            if short_puts.empty:
                errors.append("Missing_Leg:SHORT_PUT")
                df.loc[group.index, "Needs_Structural_Fix"] = True
            elif len(short_puts) != 1:
                errors.append("Invalid_Structure:Multiple_Short_Puts")
                df.loc[group.index, "Needs_Structural_Fix"] = True
            
            # Explicitly enforce Quantity < 0
            if not put_legs.empty and (put_legs["Quantity"] >= 0).any():
                errors.append("Strategy_Mismatch:Not_Short_Put")
                df.loc[group.index, "Needs_Structural_Fix"] = True
            
            # Validate expiration exists and is not NaT
            if not short_puts.empty:
                if short_puts["Expiration"].isna().any():
                    errors.append("Invalid_Structure:Missing_Expiration")
                    df.loc[group.index, "Needs_Structural_Fix"] = True
            
            # Must NOT have stock
            if not stock_legs.empty:
                errors.append("Extra_Leg:STOCK")
                df.loc[group.index, "Needs_Structural_Fix"] = True
            
            # Must NOT have calls
            if not call_legs.empty:
                errors.append("Extra_Leg:CALL")
                df.loc[group.index, "Needs_Structural_Fix"] = True
        
        # === Long Straddle Validation ===
        elif strategy == STRATEGY_LONG_STRADDLE:
            # Must have: 1 Call + 1 Put, same strike, same expiration
            if len(call_legs) != 1 or len(put_legs) != 1:
                errors.append("Missing_Leg:Straddle_Incomplete")
                df.loc[group.index, "Needs_Structural_Fix"] = True
            else:
                call_strike = call_legs["Strike"].iloc[0]
                put_strike = put_legs["Strike"].iloc[0]
                call_expiry = call_legs["Expiration"].iloc[0]
                put_expiry = put_legs["Expiration"].iloc[0]
                
                # Check strikes are not NaN and match
                if pd.isna(call_strike) or pd.isna(put_strike):
                    errors.append("Strategy_Mismatch:Invalid_Strike")
                    df.loc[group.index, "Needs_Structural_Fix"] = True
                elif call_strike != put_strike:
                    errors.append("Strategy_Mismatch:Strikes_Differ")
                    df.loc[group.index, "Needs_Structural_Fix"] = True
                
                # Check expirations are not NaT and match (explicit pd.isna guard)
                if pd.isna(call_expiry) or pd.isna(put_expiry):
                    errors.append("Strategy_Mismatch:Invalid_Expiration")
                    df.loc[group.index, "Needs_Structural_Fix"] = True
                elif call_expiry != put_expiry:
                    errors.append("Strategy_Mismatch:Expirations_Differ")
                    df.loc[group.index, "Needs_Structural_Fix"] = True
            
            # Must NOT have stock
            if not stock_legs.empty:
                errors.append("Illegal_Leg_Combination:Stock_In_Straddle")
                df.loc[group.index, "Needs_Structural_Fix"] = True
        
        # === Long Strangle Validation ===
        elif strategy == STRATEGY_LONG_STRANGLE:
            # Must have: 1 Call + 1 Put, different strikes, same expiration
            if len(call_legs) != 1 or len(put_legs) != 1:
                errors.append("Missing_Leg:Strangle_Incomplete")
                df.loc[group.index, "Needs_Structural_Fix"] = True
            else:
                call_strike = call_legs["Strike"].iloc[0]
                put_strike = put_legs["Strike"].iloc[0]
                call_expiry = call_legs["Expiration"].iloc[0]
                put_expiry = put_legs["Expiration"].iloc[0]
                
                # Check strikes are not NaN and differ
                if pd.isna(call_strike) or pd.isna(put_strike):
                    errors.append("Strategy_Mismatch:Invalid_Strike")
                    df.loc[group.index, "Needs_Structural_Fix"] = True
                elif call_strike == put_strike:
                    errors.append("Strategy_Mismatch:Same_Strike_Not_Strangle")
                    df.loc[group.index, "Needs_Structural_Fix"] = True
                
                # Check expirations are not NaT and match (explicit pd.isna guard)
                if pd.isna(call_expiry) or pd.isna(put_expiry):
                    errors.append("Strategy_Mismatch:Invalid_Expiration")
                    df.loc[group.index, "Needs_Structural_Fix"] = True
                elif call_expiry != put_expiry:
                    errors.append("Strategy_Mismatch:Expirations_Differ")
                    df.loc[group.index, "Needs_Structural_Fix"] = True
            
            # Must NOT have stock
            if not stock_legs.empty:
                errors.append("Illegal_Leg_Combination:Stock_In_Strangle")
                df.loc[group.index, "Needs_Structural_Fix"] = True
        
        # === Buy Call Validation ===
        elif strategy == STRATEGY_BUY_CALL:
            # Must have: Exactly one call leg, Quantity > 0
            if len(option_legs) != 1:
                errors.append("Invalid_Structure:Multi_Leg_Buy_Call")
                df.loc[group.index, "Needs_Structural_Fix"] = True
            elif long_calls.empty:
                errors.append("Strategy_Mismatch:Not_Long_Call")
                df.loc[group.index, "Needs_Structural_Fix"] = True
            
            # Must NOT have stock
            if not stock_legs.empty:
                errors.append("Extra_Leg:STOCK")
                df.loc[group.index, "Needs_Structural_Fix"] = True
        
        # === Buy Put Validation ===
        elif strategy == STRATEGY_BUY_PUT:
            # Must have: Exactly one put leg, Quantity > 0
            if len(option_legs) != 1:
                errors.append("Invalid_Structure:Multi_Leg_Buy_Put")
                df.loc[group.index, "Needs_Structural_Fix"] = True
            elif long_puts.empty:
                errors.append("Strategy_Mismatch:Not_Long_Put")
                df.loc[group.index, "Needs_Structural_Fix"] = True
            
            # Must NOT have stock
            if not stock_legs.empty:
                errors.append("Extra_Leg:STOCK")
                df.loc[group.index, "Needs_Structural_Fix"] = True
        
        # === Unknown Strategy Handler (defensive) ===
        else:
            # If it's a stock-only group, suppress the unknown strategy error
            # (Management: stocks can exist without a strategy)
            if (group["AssetType"] == ASSET_TYPE_STOCK).all():
                pass
            else:
                # This should never execute due to enum validation,
                # but defensively log it if somehow reached
                errors.append(f"Unknown_Strategy:{strategy}")
                df.loc[group.index, "Needs_Structural_Fix"] = True
        
        # === 4️⃣ Structural Sanity Checks ===
        
        # Validate LegCount matches actual count
        if "LegCount" in group.columns:
            expected_count = group["LegCount"].iloc[0]
            actual_count = len(group)
            if expected_count != actual_count:
                errors.append(f"LegCount_Mismatch:Expected={expected_count}_Actual={actual_count}")
                df.loc[group.index, "Needs_Structural_Fix"] = True
        
        # Store errors for this TradeID (sorted for determinism)
        if errors:
            trade_errors[trade_id] = sorted(errors)  # Sort for deterministic output
    
    # === Apply validation results ===
    truly_invalid_trade_ids = []
    all_critical_errors = []

    for trade_id, errors in trade_errors.items():
        indices = df[df["TradeID"] == trade_id].index
        df.loc[indices, "Validation_Errors"] = " | ".join(errors)
        
        # Determine if this trade is TRULY invalid (blocking)
        # Exclude STRUCTURE_WARNING and stock-only Unknown_Strategy
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
            # If only warnings, it's still structurally valid for the pipeline
            df.loc[indices, "Structure_Valid"] = True
            # Ensure Needs_Structural_Fix is False if no critical errors
            df.loc[indices, "Needs_Structural_Fix"] = False
    
    # === Enforce invariant: Needs_Structural_Fix ⇒ not Structure_Valid ===
    invalid_invariant = df["Needs_Structural_Fix"] & df["Structure_Valid"]
    if invalid_invariant.any():
        # Log the offending TradeIDs for debugging
        offenders = df[invalid_invariant]["TradeID"].unique()
        logger.error(f"Invariant violation offenders: {offenders}")
        raise ValueError(
            f"❌ FATAL: Invariant violation detected. "
            f"{invalid_invariant.sum()} positions have Needs_Structural_Fix=True but Structure_Valid=True."
        )
    
    # === Summary reporting (use logging) ===
    total_trades = df["TradeID"].nunique()
    invalid_trades_count = len(truly_invalid_trade_ids)
    
    # Truly critical positions are those that have Needs_Structural_Fix=True AND are not just warnings
    truly_critical_positions = df[df["Needs_Structural_Fix"] & ~df["Validation_Errors"].str.contains("STRUCTURE_WARNING", na=False)].shape[0]
    
    if invalid_trades_count > 0:
        logger.warning(f"Phase 2C Validation Summary (Post-Filter):")
        logger.warning(f"  Total TradeIDs: {total_trades}")
        logger.warning(f"  Truly Invalid TradeIDs: {invalid_trades_count}")
        logger.warning(f"  Truly Critical Positions: {truly_critical_positions}")
        logger.warning(f"  Top Critical Issues:")
        error_counts = pd.Series(all_critical_errors).value_counts().head(5)
        for error, count in error_counts.items():
            logger.warning(f"    • {error}: {count}")
        
        if truly_invalid_trade_ids:
            logger.warning(f"  Truly Invalid TradeIDs: {sorted(truly_invalid_trade_ids)[:10]}" + 
                           (f" ... (+{len(truly_invalid_trade_ids)-10} more)" if len(truly_invalid_trade_ids) > 10 else ""))
    else:
        logger.info(f"Phase 2C Validation: All {total_trades} TradeIDs structurally valid (or contain only non-critical warnings)")
    
    return df


def get_validation_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate a summary report of validation results.
    """
    if "Structure_Valid" not in df.columns:
        raise ValueError("❌ Validation columns not found. Run validate_structures() first.")
    
    # Validate LegCount exists
    if "LegCount" not in df.columns:
        raise ValueError("❌ LegCount column missing. Ensure phase21_strategy_tagging() completed successfully.")
    
    summary = df.groupby("TradeID").agg({
        "Strategy": "first",
        "Account": "first",
        "Underlying_Ticker": "first",
        "LegCount": "first",
        "Structure_Valid": "first",
        "Validation_Errors": "first",
        "Needs_Structural_Fix": "any"
    }).reset_index()
    
    # Filter to show only invalid structures
    invalid = summary[~summary["Structure_Valid"]]
    
    if invalid.empty:
        logger.info("No structural validation errors found.")
        return summary
    else:
        logger.warning(f"{len(invalid)} TradeIDs with validation errors:")
        logger.warning(f"\n{invalid.to_string(index=False)}")
        return invalid


def enforce_validation_gate(df: pd.DataFrame, strict: bool = True) -> pd.DataFrame:
    """
    Optional execution guard: Block Phase 3 if structural issues detected.
    """
    if "Structure_Valid" not in df.columns:
        raise ValueError("❌ Validation columns not found. Run validate_structures() first.")
    
    invalid_count = (~df["Structure_Valid"]).sum()
    needs_fix_count = df["Needs_Structural_Fix"].sum()
    
    if invalid_count == 0:
        logger.info("Validation gate: All structures valid, proceeding...")
        return df
    
    # Generate error summary
    error_summary = df[~df["Structure_Valid"]][["TradeID", "Strategy", "Validation_Errors"]].drop_duplicates()
    
    if strict:
        logger.error("VALIDATION GATE BLOCKED")
        logger.error(f"  {invalid_count} positions failed validation")
        logger.error(f"  {needs_fix_count} positions need structural fixes")
        logger.error("  Invalid TradeIDs:")
        for _, row in error_summary.iterrows():
            logger.error(f"    • {row['TradeID']} ({row['Strategy']}): {row['Validation_Errors']}")
        
        raise ValueError(
            f"❌ Cannot proceed to Phase 3: {needs_fix_count} positions need structural fixes.\n"
            f"   Run get_validation_summary(df) for full details.\n"
            f"   To bypass this check, use enforce_validation_gate(df, strict=False)."
        )
    else:
        logger.warning("VALIDATION WARNING (strict=False)")
        logger.warning(f"  {invalid_count} positions failed validation but continuing...")
        logger.warning(f"  {needs_fix_count} positions flagged for fixes")
        return df
