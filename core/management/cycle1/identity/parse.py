import pandas as pd
import re
import logging
from core.management.cycle1.identity.validate import validate_structures
from core.management.cycle1.identity.constants import (
    OCC_OPTION_PATTERN,
    # Strategy constants
    STRATEGY_UNKNOWN,
    STRATEGY_BUY_CALL,
    STRATEGY_BUY_PUT,
    STRATEGY_LEAPS_CALL,
    STRATEGY_LEAPS_PUT,
    STRATEGY_BUY_WRITE,
    STRATEGY_COVERED_CALL,
    STRATEGY_CSP,
    STRATEGY_LONG_STRADDLE,
    STRATEGY_LONG_STRANGLE,
    STRATEGY_STOCK,
    STRATEGY_DEFINITIONS,
    # Structure constants
    STRUCTURE_SINGLE_LEG,
    STRUCTURE_MULTI_LEG,
    STRUCTURE_STOCK_CALL,
    # LegType constants
    LEG_TYPE_STOCK,
    LEG_TYPE_SHORT_CALL,
    LEG_TYPE_LONG_CALL,
    LEG_TYPE_SHORT_PUT,
    LEG_TYPE_LONG_PUT,
    LEG_TYPE_UNKNOWN,
    # OptionType constants
    OPTION_TYPE_CALL,
    OPTION_TYPE_PUT,
)


# Configure logger
logger = logging.getLogger(__name__)


def filter_stock_only_positions(df: pd.DataFrame) -> pd.DataFrame:
    """
    RAG: Identity. Preserve all stock legs for strategy context.
    
    We no longer remove stock-only positions because they are required 
    to distinguish between BUY_WRITE and COVERED_CALL lifecycles.
    """
    return df


def phase2_parse_symbols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Phase 2A: Parse OCC-style option symbols into structured fields.

    This function extracts standardized option metadata from the raw
    'Symbol' column, including:
      - Option type (Call / Put)
      - Strike price
      - Expiration date

    Underlying_Ticker is already populated in Phase 1.
    """
    if "Symbol" not in df.columns:
        raise ValueError("❌ Missing 'Symbol' column in DataFrame.")

    # === Canonical Identity Law Enforcement ===
    if "Underlying_Ticker" not in df.columns:
        raise ValueError("❌ DATA CONTRACT VIOLATION: 'Underlying_Ticker' missing. Ensure Phase 1 normalization ran.")
    
    if df["Underlying_Ticker"].isna().any():
        bad_symbols = df[df["Underlying_Ticker"].isna()]["Symbol"].tolist()[:5]
        raise ValueError(f"❌ DATA CONTRACT VIOLATION: Null Underlying_Ticker detected for symbols: {bad_symbols}")

    def parse_symbol(sym: str) -> pd.Series:
        """
        Parse a single option symbol string into structured components.
        """
        sym = str(sym).strip() if pd.notnull(sym) else ""
        
        # RAG: Use authoritative regex to parse without mutation
        match = OCC_OPTION_PATTERN.match(sym)

        if match:
            yy = int(match.group(2))
            year = 2000 + yy
            month = match.group(3)
            day = match.group(4)
            opt_type = "Call" if match.group(5) == "C" else "Put"
            strike = float(match.group(6))
            
            expiration = pd.to_datetime(
                f"{year}-{month}-{day}", errors="coerce"
            )
            
            return pd.Series([opt_type, strike, expiration])

        # Fallback for unparsable symbols (Stocks)
        return pd.Series([None, None, None])

    parsed = df["Symbol"].apply(parse_symbol)
    parsed.columns = ["OptionType_Parsed", "Strike_Parsed", "Expiration_Parsed"]

    df = pd.concat([df, parsed], axis=1)
    
    # === Post-parse validation (ENFORCE structural correctness) ===
    if "AssetType" in df.columns:
        # Validate OPTION symbols parsed successfully
        option_mask = df["AssetType"] == "OPTION"
        if option_mask.any():
            # Check for missing OptionType (parse failure)
            unparsed = option_mask & df["OptionType_Parsed"].isna()
            if unparsed.any():
                error_symbols = df.loc[unparsed, 'Symbol'].tolist()[:5]
                raise ValueError(
                    f"❌ FATAL: {unparsed.sum()} OPTION symbols failed to parse.\n"
                    f"   Symbols: {error_symbols}"
                )
    
    # Map parsed fields to canonical names if they don't exist
    # RAG: McMillan (Identity). We prefer broker-provided fields but use parsed as fallback/validation.
    if "OptionType" not in df.columns:
        df["OptionType"] = df["OptionType_Parsed"]
    if "Strike" not in df.columns:
        df["Strike"] = df["Strike_Parsed"]
    if "Expiration" not in df.columns:
        df["Expiration"] = df["Expiration_Parsed"]
    
    # Ensure numeric and datetime types for canonical fields
    df["Strike"] = pd.to_numeric(df["Strike"], errors="coerce")
    # RAG: Use format='mixed' to handle potential variations and silence inference warnings.
    df["Expiration"] = pd.to_datetime(df["Expiration"], errors="coerce", format='mixed')

    return df


def assign_leg_ids(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign deterministic LegID within each TradeID.
    """
    df = df.copy()
    
    # Create sorting helper columns
    df["_asset_sort"] = df["AssetType"].map({"STOCK": 0, "OPTION": 1})
    df["_exp_sort"] = df["Expiration"].fillna(pd.Timestamp.max)  # NaT → last
    df["_strike_sort"] = df["Strike"].fillna(float('inf'))  # NaN → last
    df["_opt_sort"] = df["OptionType"].map({"Call": 0, "Put": 1}).fillna(2)
    df["_qty_sort"] = (df["Quantity"] < 0).astype(int)  # Long (False=0) before Short (True=1)
    
    # Sort within each TradeID (deterministic ordering)
    df = df.sort_values(
        by=["TradeID", "_asset_sort", "_exp_sort", "_strike_sort", "_opt_sort", "_qty_sort"],
        ascending=True
    )
    
    # Assign sequential leg number (1-indexed, resets per TradeID)
    df["_leg_num"] = df.groupby("TradeID").cumcount() + 1
    
    # Create stable LegID string (TradeID + leg position)
    df["LegID"] = df.apply(
        lambda row: f"{row['TradeID']}_L{row['_leg_num']}", 
        axis=1
    )
    
    # Drop helper columns
    df = df.drop(columns=["_asset_sort", "_exp_sort", "_strike_sort", "_opt_sort", "_qty_sort", "_leg_num"])
    
    return df


def assign_leg_roles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign semantic LegRole based on AssetType, OptionType, Quantity, and Strategy.
    """
    df = df.copy()
    
    def determine_leg_role(row: pd.Series) -> str:
        """Determine semantic leg role for a single position."""
        asset_type = row["AssetType"]
        strategy = row["Strategy"]
        qty = row["Quantity"]
        
        # Stock legs
        if asset_type == "STOCK":
            return "Stock_Long" if qty > 0 else "Stock_Short"
        
        # Option legs
        opt_type = row["OptionType"]
        direction = "Long" if qty > 0 else "Short"
        
        # Strategy-aware role assignment
        if strategy == STRATEGY_COVERED_CALL:
            if opt_type == "Call" and qty < 0:
                return "Short_Call"  # The call in a covered call
            return f"{direction}_{opt_type}"
        
        elif strategy == STRATEGY_LONG_STRADDLE:
            return f"Long_{opt_type}"  # Both legs are long
        
        elif strategy == STRATEGY_LONG_STRANGLE:
            return f"Long_{opt_type}"  # Both legs are long
        
        elif strategy == STRATEGY_CSP:
            return "Short_Put"  # Cash-secured put
        
        else:
            # Default: direction + type (e.g., Long_Call, Short_Put)
            return f"{direction}_{opt_type}"
    
    df["LegRole"] = df.apply(determine_leg_role, axis=1)
    
    return df


def validate_quantity_signs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate Quantity signs match LegRole expectations.
    """
    # Long positions must be positive
    long_mask = df["LegRole"].str.contains("Long", na=False)
    invalid_long = long_mask & (df["Quantity"] <= 0)
    if invalid_long.any():
        error_trades = df.loc[invalid_long, "TradeID"].unique()[:5].tolist()
        raise ValueError(
            f"❌ FATAL: {invalid_long.sum()} Long legs have Quantity ≤ 0.\n"
            f"   Affected trades: {error_trades}"
        )
    
    # Short positions must be negative
    short_mask = df["LegRole"].str.contains("Short", na=False)
    invalid_short = short_mask & (df["Quantity"] >= 0)
    if invalid_short.any():
        error_trades = df.loc[invalid_short, "TradeID"].unique()[:5].tolist()
        raise ValueError(
            f"❌ FATAL: {invalid_short.sum()} Short legs have Quantity ≥ 0.\n"
            f"   Affected trades: {error_trades}"
        )
    
    return df


def phase21_strategy_tagging(df: pd.DataFrame) -> pd.DataFrame:
    """
    Phase 2B: Infer strategy, structure, and leg metadata.
    """
    # Defensive copy to avoid mutating caller's DataFrame
    df = df.copy()
    
    # === Input validation ===
    required_cols = ["Underlying_Ticker", "OptionType", "Strike", "Expiration", "Quantity"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"❌ Missing required columns: {missing}")

    # Check for AssetType (added in Phase 1)
    if "AssetType" not in df.columns:
        raise ValueError("❌ Missing 'AssetType' column. Run Phase 1 first.")
    
    # CRITICAL GUARDRAIL: Account must be present
    if "Account" not in df.columns:
        raise ValueError("❌ FATAL: 'Account' column missing.")
    
    if df["Account"].isna().any():
        raise ValueError(f"❌ FATAL: {df['Account'].isna().sum()} positions missing Account identifier.")

    df["Strategy"] = STRATEGY_UNKNOWN
    df["Type"] = STRUCTURE_SINGLE_LEG
    if "TradeID" not in df.columns:
        df["TradeID"] = ""

    # === FIRST PASS: Detect stock + option packages (Mechanical Coverage) ===
    matched_indices = set()  # Track matched positions across all detection passes
    indices_to_remove = set() # Original stock legs that will be replaced by split legs
    new_rows = [] # To store split stock legs

    # Group by Account + Underlying_Ticker to evaluate shared inventory
    for (account, underlying), group in df.groupby(["Account", "Underlying_Ticker"]):
        # 1. Identify Stock Inventory Pool
        stock_legs = group[(group["AssetType"] == "STOCK") & (group["Quantity"] > 0)]
        stock_pool = stock_legs["Quantity"].sum()
        
        short_calls = group[
            (group["AssetType"] == "OPTION") & 
            (group["OptionType"] == "Call") & 
            (group["Quantity"] < 0)
        ].sort_values("Expiration", ascending=True)

        if short_calls.empty or stock_legs.empty:
            continue

        # 2. Consume Stock Inventory and Pair Legs
        available_shares = stock_pool
        stock_template = stock_legs.iloc[0].copy()
        
        # Mark original stock legs for removal
        indices_to_remove.update(stock_legs.index)

        for idx, row in short_calls.iterrows():
            contracts = abs(row["Quantity"])
            needed_shares = contracts * 100
            
            if available_shares <= 0:
                continue

            # Generate a stable TradeID for this pair
            expiration_fmt = pd.to_datetime(row["Expiration"]).strftime("%y%m%d")
            strike_fmt = f"{row['Strike']:.1f}".replace('.', 'p')
            account_id = str(row['Account']).split()[-1].replace('*', '').replace('-', '')
            pair_trade_id = f"{underlying}{expiration_fmt}_{strike_fmt}_CC_{account_id}"

            # Tag the option leg
            df.loc[idx, "Strategy"] = STRATEGY_COVERED_CALL
            df.loc[idx, "Type"] = STRUCTURE_STOCK_CALL
            df.loc[idx, "TradeID"] = pair_trade_id
            matched_indices.add(idx)

            # Create a paired stock leg
            paired_stock = stock_template.copy()
            actual_shares = min(needed_shares, available_shares)

            # Proportionally split basis, premium, and G/L columns.
            # Without scaling G/L, each split leg inherits the full 200-share G/L,
            # causing the ticker banner to double- (or triple-) count the stock loss.
            ratio = actual_shares / stock_pool
            for col in ["Basis", "Basis_Entry", "Premium", "$ Total G/L", "Total_GL_Decimal"]:
                if col in paired_stock and pd.notna(paired_stock[col]):
                    paired_stock[col] = paired_stock[col] * ratio

            paired_stock["Quantity"] = actual_shares
            paired_stock["TradeID"] = pair_trade_id
            paired_stock["Strategy"] = STRATEGY_COVERED_CALL
            paired_stock["Type"] = STRUCTURE_STOCK_CALL
            new_rows.append(paired_stock)
            
            available_shares -= actual_shares

        # If there's leftover stock, add it back as STOCK_ONLY
        if available_shares > 0:
            leftover_stock = stock_template.copy()
            ratio = available_shares / stock_pool
            for col in ["Basis", "Basis_Entry", "Premium", "$ Total G/L", "Total_GL_Decimal"]:
                if col in leftover_stock and pd.notna(leftover_stock[col]):
                    leftover_stock[col] = leftover_stock[col] * ratio

            leftover_stock["Quantity"] = available_shares
            leftover_stock["Strategy"] = STRATEGY_STOCK
            leftover_stock["Type"] = STRUCTURE_SINGLE_LEG
            leftover_stock["TradeID"] = "" # Will be generated in default pass
            new_rows.append(leftover_stock)

    # Remove original stock legs and add new split legs
    if indices_to_remove:
        df = df[~df.index.isin(indices_to_remove)].copy()
    if new_rows:
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
        # Update matched_indices to include new rows that are Covered Calls
        matched_indices.update(df[df["Strategy"] == STRATEGY_COVERED_CALL].index)

    # Group only OPTION positions
    options_only = df[df["AssetType"] == "OPTION"]
    grouped = options_only.groupby(["Account", "Underlying_Ticker", "Expiration"])
    
    # === SECOND PASS: Detect straddles and strangles (options only) ===
    for (account, underlying, expiry), group in grouped:
        # Skip positions already matched in first pass
        group = group[~group.index.isin(matched_indices)]
        
        calls = group[group["OptionType"] == "Call"]
        puts = group[group["OptionType"] == "Put"]

        # Long Straddle: same strike Call + Put
        for strike in group["Strike"].unique():
            call = calls[calls["Strike"] == strike]
            put = puts[puts["Strike"] == strike]

            if not call.empty and not put.empty:
                indices = list(pd.concat([call, put]).index)
                # Only tag if not already matched
                if not any(idx in matched_indices for idx in indices):
                    df.loc[indices, "Strategy"] = STRATEGY_LONG_STRADDLE
                    df.loc[indices, "Type"] = STRUCTURE_MULTI_LEG
                    matched_indices.update(indices)

        # Long Strangle: one call and one put, different strikes
        unmatched_calls = calls[~calls.index.isin(matched_indices)]
        unmatched_puts = puts[~puts.index.isin(matched_indices)]
        
        if len(unmatched_calls) > 0 and len(unmatched_puts) > 0:
            # Find the pair with MINIMUM strike difference for determinism
            min_diff = float('inf')
            best_call_idx = None
            best_put_idx = None
            
            for call_idx in unmatched_calls.index:
                call_strike = unmatched_calls.loc[call_idx, "Strike"]
                for put_idx in unmatched_puts.index:
                    put_strike = unmatched_puts.loc[put_idx, "Strike"]
                    diff = abs(call_strike - put_strike)
                    
                    # Only consider if strikes are different
                    if diff > 0 and diff < min_diff:
                        min_diff = diff
                        best_call_idx = call_idx
                        best_put_idx = put_idx
            
            if best_call_idx is not None and best_put_idx is not None:
                indices = [best_call_idx, best_put_idx]
                df.loc[indices, "Strategy"] = STRATEGY_LONG_STRANGLE
                df.loc[indices, "Type"] = STRUCTURE_MULTI_LEG
                matched_indices.update(indices)

    # === Default single-leg tagging (vectorized) ===
    unknown_mask = df["Strategy"] == STRATEGY_UNKNOWN
    
    # Tag unmatched stocks explicitly
    df.loc[unknown_mask & (df["AssetType"] == "STOCK"), "Strategy"] = STRATEGY_STOCK
    
    # Tag unmatched options (Differentiate LEAPS vs Short-term)
    if 'DTE' not in df.columns:
        now = pd.Timestamp.now().normalize()
        df['temp_dte'] = (pd.to_datetime(df['Expiration']) - now).dt.days
    else:
        df['temp_dte'] = pd.to_numeric(df['DTE'], errors='coerce')

    leaps_mask = df['temp_dte'].fillna(0) > 200
    
    df.loc[unknown_mask & (df["OptionType"] == "Call") & ~leaps_mask, "Strategy"] = STRATEGY_BUY_CALL
    df.loc[unknown_mask & (df["OptionType"] == "Call") & leaps_mask, "Strategy"] = STRATEGY_LEAPS_CALL
    df.loc[unknown_mask & (df["OptionType"] == "Put") & ~leaps_mask, "Strategy"] = STRATEGY_BUY_PUT
    df.loc[unknown_mask & (df["OptionType"] == "Put") & leaps_mask, "Strategy"] = STRATEGY_LEAPS_PUT

    if 'temp_dte' in df.columns:
        df = df.drop(columns=['temp_dte'])

    # For all other positions, generate TradeID normally
    def generate_trade_id_v2(row: pd.Series) -> str:
        """Enhanced TradeID with direction awareness and Account isolation."""
        if pd.notna(row.get("TradeID")) and row["TradeID"] != "":
            return row["TradeID"]

        account_str = str(row['Account'])
        try:
            account_id = account_str.split()[-1].replace('*', '').replace('-', '')
            if not account_id or not account_id.isalnum():
                import hashlib
                h = hashlib.md5(account_str.encode()).hexdigest()
                account_id = f"ACC{int(h, 16) % 999999:06d}"
        except:
            import hashlib
            h = hashlib.md5(account_str.encode()).hexdigest()
            account_id = f"ACC{int(h, 16) % 999999:06d}"

        if row["AssetType"] == "STOCK":
            return f"{row['Underlying_Ticker']}_STOCK_{account_id}"

        if pd.isna(row["Expiration"]):
            return f"{row['Underlying_Ticker']}_NOEXP_{account_id}"

        expiration_fmt = pd.to_datetime(row["Expiration"]).strftime("%y%m%d")
        direction = "Short" if row["Quantity"] < 0 else "Long"

        if row["Type"] == STRUCTURE_SINGLE_LEG:
            strike_fmt = f"{row['Strike']:.1f}".replace('.', 'p')
            return f"{row['Underlying_Ticker']}{expiration_fmt}_{strike_fmt}_{direction}_{account_id}"

        return f"{row['Underlying_Ticker']}{expiration_fmt}_{direction}_{account_id}"

    # Fill empty TradeIDs
    empty_mask = (df["TradeID"].isna()) | (df["TradeID"] == "")
    if empty_mask.any():
        df.loc[empty_mask, "TradeID"] = df[empty_mask].apply(generate_trade_id_v2, axis=1)

    # ═══════════════════════════════════════════════════════════
    # LEG IDENTITY & ROLE (Phase 2 - Structural Metadata)
    # ═══════════════════════════════════════════════════════════
    
    df = assign_leg_ids(df)
    df = assign_leg_roles(df)
    df["LegIndex"] = df.groupby("TradeID").cumcount() + 1
    df["Strike_Entry"] = df["Strike"]
    df["Expiration_Entry"] = df["Expiration"]
    df["Underlying_Price_Entry"] = df.get("UL Last", pd.NA)
    df = validate_quantity_signs(df)
    
    # ═══════════════════════════════════════════════════════════
    # END LEG IDENTITY & ROLE
    # ═══════════════════════════════════════════════════════════

    df["Structure"] = df["Type"]
    df.loc[df["Strategy"].str.contains("Straddle|Strangle", na=False), "Structure"] = STRUCTURE_MULTI_LEG

    # Quantity-based overrides
    csp_mask = (
        (df["Structure"] == STRUCTURE_SINGLE_LEG) & 
        (df["OptionType"] == "Put") & 
        (df["Quantity"] < 0) & 
        (df["Strategy"] == STRATEGY_BUY_PUT)
    )
    df.loc[csp_mask, "Strategy"] = STRATEGY_CSP

    cc_mask = (
        (df["Structure"] == STRUCTURE_SINGLE_LEG) & 
        (df["OptionType"] == "Call") & 
        (df["Quantity"] < 0) & 
        (df["Strategy"] == STRATEGY_BUY_CALL)
    )
    df.loc[cc_mask, "Strategy"] = STRATEGY_COVERED_CALL

    def assign_leg_type(row: pd.Series) -> str:
        if row["AssetType"] == "STOCK":
            return LEG_TYPE_STOCK
        elif row["AssetType"] == "OPTION":
            if row["OptionType"] == "Call":
                return LEG_TYPE_SHORT_CALL if row["Quantity"] < 0 else LEG_TYPE_LONG_CALL
            elif row["OptionType"] == "Put":
                return LEG_TYPE_SHORT_PUT if row["Quantity"] < 0 else LEG_TYPE_LONG_PUT
        return LEG_TYPE_UNKNOWN
    
    df["LegType"] = df.apply(assign_leg_type, axis=1)
    df["LegCount"] = df.groupby("TradeID")["Symbol"].transform("count")
    
    actual_leg_counts = df.groupby("TradeID")["LegID"].nunique()
    declared_leg_counts = df.groupby("TradeID")["LegCount"].first()
    
    if not actual_leg_counts.equals(declared_leg_counts):
        mismatches = actual_leg_counts[actual_leg_counts != declared_leg_counts]
        raise ValueError(f"❌ FATAL: LegCount mismatch for {len(mismatches)} trades.")
    
    if df["LegID"].isna().any():
        raise ValueError(f"❌ FATAL: {df['LegID'].isna().sum()} positions missing LegID.")
    if df["LegRole"].isna().any():
        raise ValueError(f"❌ FATAL: {df['LegRole'].isna().sum()} positions missing LegRole.")
    if df["LegIndex"].isna().any():
        raise ValueError(f"❌ FATAL: {df['LegIndex'].isna().sum()} positions missing LegIndex.")

    if "Premium" not in df.columns:
        raise ValueError("❌ FATAL: 'Premium' column missing from input.")
    
    option_mask = df["AssetType"] == "OPTION"
    option_missing_premium = option_mask & df["Premium"].isnull()
    if option_missing_premium.any():
        missing_count = option_missing_premium.sum()
        raise ValueError(f"❌ FATAL: {missing_count} OPTION positions missing Premium values.")
    
    df["Premium_Estimated"] = False

    if option_mask.any():
        invalid_options = option_mask & (
            df["Expiration"].isna() | 
            df["Strike"].isna() | 
            (df["Strike"] <= 0) |
            df["OptionType"].isna()
        )
        if invalid_options.any():
            raise ValueError(f"❌ FATAL: {invalid_options.sum()} OPTION positions have invalid parsed fields.")
    
    if df["TradeID"].isna().any() or (df["TradeID"] == "").any():
        raise ValueError(f"❌ FATAL: {(df['TradeID'].isna() | (df['TradeID'] == '')).sum()} positions have missing TradeID.")
    
    return df


def phase2_run_all(df: pd.DataFrame) -> pd.DataFrame:
    """
    Phase 2 Orchestrator: Run all parsing and strategy-tagging steps.
    """
    df = phase2_parse_symbols(df)
    df = phase21_strategy_tagging(df)
    df = filter_stock_only_positions(df)
    df = validate_structures(df)
    return df
