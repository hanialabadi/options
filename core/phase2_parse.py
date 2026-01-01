import pandas as pd
import re


def phase2_parse_symbols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Phase 2A: Parse OCC-style option symbols into structured fields.

    This function extracts standardized option metadata from the raw
    'Symbol' column, including:
      - Underlying ticker
      - Option type (Call / Put)
      - Strike price
      - Expiration date

    It assumes OCC-style symbols of the form:
        TICKERYYMMDD[C|P]STRIKE

    Example
    -------
    AAPL250118C150  →  AAPL, Call, 150.0, 2025-01-18

    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame expected to contain a 'Symbol' column.

    Returns
    -------
    pandas.DataFrame
        Original DataFrame with appended columns:
        - Underlying
        - OptionType
        - Strike
        - Expiration

    Raises
    ------
    ValueError
        If the 'Symbol' column is missing.
    """
    if "Symbol" not in df.columns:
        raise ValueError("❌ Missing 'Symbol' column in DataFrame.")

    # Regex for OCC-style option symbols
    pattern = re.compile(
        r"^([A-Z]+)(\d{2})(\d{2})(\d{2})([CP])(\d+(\.\d{1,2})?)$"
    )

    def parse_symbol(sym: str) -> pd.Series:
        """
        Parse a single option symbol string into structured components.
        """
        sym = str(sym).strip() if pd.notnull(sym) else ""
        match = pattern.match(sym)

        if match:
            ticker = match.group(1)
            year = "20" + match.group(2)
            month = match.group(3)
            day = match.group(4)
            opt_type = "Call" if match.group(5) == "C" else "Put"
            strike = float(match.group(6))
            expiration = pd.to_datetime(
                f"{year}-{month}-{day}", errors="coerce"
            )
            return pd.Series([ticker, opt_type, strike, expiration])

        # Fallback for unparsable symbols
        return pd.Series([sym, None, None, None])

    parsed = df["Symbol"].apply(parse_symbol)
    parsed.columns = ["Underlying", "OptionType", "Strike", "Expiration"]

    df = pd.concat([df, parsed], axis=1)
    return df


def phase21_strategy_tagging(df: pd.DataFrame) -> pd.DataFrame:
    """
    Phase 2B: Infer strategy, structure, and leg metadata.

    This function analyzes option legs grouped by:
      - Underlying
      - Expiration

    It infers high-level strategy intent such as:
      - Buy Call / Buy Put
      - Long Straddle
      - Long Strangle
      - Cash-Secured Put (CSP)
      - Covered Call (CC)

    It also attaches:
      - TradeID
      - LegType (Call-Leg / Put-Leg)
      - LegCount
      - Structure classification (Single-leg / Multi-leg)

    This phase is **heuristic and structural**:
    no PCS scoring, IV logic, or market judgment occurs here.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame expected to already include:
        - Underlying
        - OptionType
        - Strike
        - Expiration
        - Quantity

    Returns
    -------
    pandas.DataFrame
        DataFrame enriched with strategy, structure, and leg metadata.
    """
    # === Input validation ===
    required_cols = ["Underlying", "OptionType", "Strike", "Expiration", "Quantity"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"❌ Missing required columns: {missing}")

    df["Strategy"] = "Unknown"
    df["Type"] = "Single-leg"

    grouped = df.groupby(["Underlying", "Expiration"])

    # === Detect straddles and strangles ===
    matched_indices = set()  # Track already-matched pairs
    
    for (underlying, expiry), group in grouped:
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
                    df.loc[indices, "Strategy"] = "Long Straddle"
                    df.loc[indices, "Type"] = "Multi-leg"
                    matched_indices.update(indices)

        # Long Strangle: one call and one put, different strikes
        # Only match the first valid pair to avoid duplicates
        if len(calls) > 0 and len(puts) > 0:
            for call_idx, call_row in calls.iterrows():
                if call_idx in matched_indices:
                    continue
                for put_idx, put_row in puts.iterrows():
                    if put_idx in matched_indices:
                        continue
                    if call_row["Strike"] != put_row["Strike"]:
                        indices = [call_idx, put_idx]
                        df.loc[indices, "Strategy"] = "Long Strangle"
                        df.loc[indices, "Type"] = "Multi-leg"
                        matched_indices.update(indices)
                        break  # Only match first strangle pair

    # === Default single-leg tagging ===
    for idx, row in df[df["Strategy"] == "Unknown"].iterrows():
        if row["OptionType"] == "Call":
            df.at[idx, "Strategy"] = "Buy Call"
        elif row["OptionType"] == "Put":
            df.at[idx, "Strategy"] = "Buy Put"

    # === TradeID ===
    def generate_trade_id_v2(row: pd.Series) -> str:
        """Enhanced TradeID with direction awareness."""
        expiration_fmt = pd.to_datetime(row["Expiration"]).strftime("%y%m%d")
        strategy = row["Strategy"].replace(" ", "")
        direction = "Short" if row["Quantity"] < 0 else "Long"
        return f"{row['Underlying']}{expiration_fmt}_{direction}_{strategy}"

    df["TradeID"] = df.apply(generate_trade_id_v2, axis=1)

    # === Structure classification ===
    df["Structure"] = df["Strategy"].apply(
        lambda x: "Multi-leg"
        if "Straddle" in x or "Strangle" in x
        else "Single-leg"
    )

    # === Quantity-based overrides (ONLY for single-leg positions) ===
    csp_mask = (df["Structure"] == "Single-leg") & (df["OptionType"] == "Put") & (df["Quantity"] < 0) & (df["Strategy"] == "Buy Put")
    df.loc[csp_mask, "Strategy"] = "Cash-Secured Put"

    cc_mask = (df["Structure"] == "Single-leg") & (df["OptionType"] == "Call") & (df["Quantity"] < 0) & (df["Strategy"] == "Buy Call")
    df.loc[cc_mask, "Strategy"] = "Covered Call"

    # === Leg metadata ===
    df["LegType"] = df["OptionType"].map(
        {"Call": "Call-Leg", "Put": "Put-Leg"}
    )
    df["LegCount"] = df.groupby("TradeID")["Symbol"].transform("count")

    # === Premium estimation ===
    if "Premium" not in df.columns or df["Premium"].isnull().sum() > 0:
        if "Bid" in df.columns and "Ask" in df.columns:
            df["Premium"] = (df["Bid"] + df["Ask"]) / 2
            df["Premium_Estimated"] = True
            print("⚠️ 'Premium' estimated from Bid/Ask.")
        else:
            raise ValueError("❌ Cannot estimate Premium: missing Bid/Ask columns")
    else:
        df["Premium_Estimated"] = False

    return df


def phase2_run_all(df: pd.DataFrame) -> pd.DataFrame:
    """
    Phase 2 Orchestrator: Run all parsing and strategy-tagging steps.

    This is a convenience wrapper that executes:
      1. Symbol parsing (Phase 2A)
      2. Strategy inference and leg metadata (Phase 2B)

    Parameters
    ----------
    df : pandas.DataFrame
        Cleaned DataFrame from Phase 1.

    Returns
    -------
    pandas.DataFrame
        Fully parsed and strategy-tagged DataFrame ready for enrichment
        and PCS scoring in Phase 3.
    """
    df = phase2_parse_symbols(df)
    df = phase21_strategy_tagging(df)
    return df
