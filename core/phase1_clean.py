import pandas as pd
import re
import inspect
from datetime import datetime
from pathlib import Path
from typing import Tuple

# Canonical input path (future-proof for multi-broker support)
CANONICAL_INPUT_PATH = "data/brokerage_inputs/fidelity_positions.csv"
CANONICAL_SNAPSHOT_DIR = "data/snapshots/phase1"

# File parsing constants
FIDELITY_HEADER_ROWS = 2
MAX_SYMBOL_LENGTH = 50

# OCC option symbol pattern (allows optional leading minus for short positions)
# Strike price supports whole numbers and any decimal precision
OCC_OPTION_PATTERN = re.compile(r'^-?[A-Z]+\d{6}[CP]\d+(\.\d+)?$')


def phase1_load_and_clean_positions(
    input_path: Path,
    *,
    save_snapshot: bool = True
) -> Tuple[pd.DataFrame, str]:
    """
    Phase 1: Active Position Intake (Management Engine)

    Loads raw brokerage position data and produces a clean, minimal dataset
    suitable for active trade management (drift tracking, P/L monitoring).

    This is NOT a scanning phase. It does NOT:
    - Analyze IV
    - Classify strategies
    - Filter positions
    - Compute Greeks (beyond what broker provides)

    Input Contract
    --------------
    Reads from: data/brokerage_inputs/fidelity_positions.csv
    Expected columns:
        - Symbol (option symbol OR stock ticker)
        - Quantity
        - Last (current price)
        - Bid, Ask (current market)
        - $ Total G/L (P/L in dollars)
        - % Total G/L (P/L percentage, normalized to decimal: 12.3% → 0.123)
        - Basis (cost basis)
        - Theta, Vega, Delta, Gamma (broker Greeks - options only)
        - Volume, Open Int (market data)
        - Time Val, Intrinsic Val (options only)

    Output Schema
    -------------
    Keeps all broker-truth columns:
        - Symbol, Quantity, Last, Bid, Ask
        - $ Total G/L, Total_GL_Decimal (normalized % G/L as decimal)
        - Basis
        - Theta, Vega, Delta, Gamma (broker truth)
        - Volume, Open Int
        - Time Val, Intrinsic Val
        - Account (canonical account identifier)
        - AssetType (OPTION or STOCK)
        - Snapshot_TS (timestamp)

    Removes (will be derived elsewhere):
        - IV Mid, IV Bid, IV Ask (market-derived, time-sensitive)
        - Expiration, Strike, Call/Put (derivable from Symbol)
        - Earnings Date (not position-critical)

    Asset Type Classification
    -------------------------
    AssetType determined by Symbol pattern:
        - OPTION: Matches OCC pattern (TICKERYYMMDD[C|P]STRIKE)
        - STOCK: Everything else

    Notes
    -----
    Phase 1 now includes BOTH stock and option positions.
    This enables buy-write and covered call detection in Phase 2.
    No filtering, no strategy logic - pure broker data intake.

    Parameters
    ----------
    save_snapshot : bool, default True
        If True, saves a timestamped snapshot to data/snapshots/phase1/

    Returns
    -------
    Tuple[pd.DataFrame, str]
        - DataFrame: Cleaned position data
        - str: Snapshot file path (empty if not saved)

    Notes
    -----
    This function is deterministic and pure (no side effects except snapshot).
    Future Schwab integration will add columns; this schema is the foundation.
    """

    # Load raw CSV from canonical location or provided path
    if input_path is None:
        input_path = Path(CANONICAL_INPUT_PATH)
    
    input_path = Path(input_path)
    
    if not input_path.exists():
        print(f"❌ Input file not found: {input_path}")
        print(f"   Expected: {input_path.absolute()}")
        return pd.DataFrame(), ""

    try:
        # Skip Fidelity header rows (metadata at top of file)
        df = pd.read_csv(input_path, skiprows=FIDELITY_HEADER_ROWS)
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return pd.DataFrame(), ""

    # Normalize column names (strip whitespace, collapse multiple spaces)
    df.columns = df.columns.str.strip().str.replace(r'[\s]+', ' ', regex=True)

    # Type conversion helpers (callers verify column existence)
    def clean_percent(col: str) -> pd.Series:
        """Convert percentage strings (e.g. '12.3%') to normalized decimals (0.123)."""
        return pd.to_numeric(
            df[col].astype(str).str.replace('%', '', regex=False).str.replace(',', ''),
            errors='coerce'
        ) / 100

    def clean_money(col: str) -> pd.Series:
        """Convert currency-like strings to numeric floats. Handles parenthesis negatives."""
        series = df[col].astype(str).str.strip()
        # Detect parenthesis negatives: ($1,234.56) -> -1234.56
        is_negative = series.str.startswith('(') & series.str.endswith(')')
        # Remove parenthesis, $, commas, and --
        cleaned = series.str.replace('[()$,]', '', regex=True).str.replace('--', '', regex=False)
        result = pd.to_numeric(cleaned, errors='coerce')
        # Apply negative sign where parenthesis were detected
        result = result.where(~is_negative, -result)
        return result

    def clean_integer(col: str) -> pd.Series:
        """Convert integer-like strings to nullable integers."""
        return pd.to_numeric(
            df[col].astype(str).str.replace(',', ''),
            errors='coerce'
        ).astype('Int64')

    # Apply type conversions to management-critical columns
    if 'Quantity' in df.columns:
        df['Quantity'] = pd.to_numeric(
            df['Quantity'].astype(str).str.replace(',', ''),
            errors='coerce'
        )
    
    if '% Total G/L' in df.columns:
        df['% Total G/L'] = clean_percent('% Total G/L')
        # Rename to indicate normalization (12.3% -> 0.123)
        df.rename(columns={'% Total G/L': 'Total_GL_Decimal'}, inplace=True)
    
    if '$ Total G/L' in df.columns:
        df['$ Total G/L'] = clean_money('$ Total G/L')
    
    if 'UL Last' in df.columns:
        df['UL Last'] = clean_money('UL Last')

    for col in ['Volume', 'Open Int']:
        if col in df.columns:
            df[col] = clean_integer(col)
            # FAIL LOUD: Negative values are data integrity violations
            if (df[col] < 0).any():
                invalid_count = (df[col] < 0).sum()
                invalid_symbols = df.loc[df[col] < 0, 'Symbol'].tolist()[:5]
                raise ValueError(
                    f"❌ FATAL: {invalid_count} positions have negative {col}.\n"
                    f"   Sample symbols: {invalid_symbols}\n"
                    f"   {col} cannot be negative. This is a data quality violation.\n"
                    f"   Action: Fix broker export or exclude invalid positions."
                )

    # Validate Account column presence (CRITICAL GUARDRAIL)
    if 'Account' not in df.columns:
        raise ValueError(
            "❌ FATAL: 'Account' column missing from input CSV.\n"
            "   Trades cannot cross accounts. This is a data integrity requirement."
        )
    
    # Remove columns that don't belong in management phase
    columns_to_remove = [
        'IV Mid', 'IV Bid', 'IV Ask',      # Market-derived, time-sensitive
        'Earnings Date',                    # Not position-critical
        'Expiration', 'Strike',             # Derivable from Symbol (if present)
        'Call', 'Put', 'Type'               # Derivable from Symbol (if present)
    ]
    
    df = df.drop(columns=[col for col in columns_to_remove if col in df.columns], errors='ignore')

    # Remove footer rows (disclosure text, NaN symbols, invalid tickers)
    # Filter out rows with missing symbols, overly long symbols, or boilerplate text
    # Common Fidelity footer patterns: Disclosure, data and information, Total, etc.
    footer_patterns = r'(?i)disclosure|data and information|^total$|^account|^symbol'
    df = df[
        df['Symbol'].notna() & 
        (df['Symbol'].str.len() < MAX_SYMBOL_LENGTH) &
        (df['Symbol'].str.len() > 0) &
        ~df['Symbol'].str.contains(footer_patterns, case=False, na=False, regex=True)
    ].copy()
    
    # Remove rows where Quantity is NaN (invalid positions)
    df = df[df['Quantity'].notna()].copy()
    
    # Early exit if no positions remain after filtering
    if df.empty:
        print("⚠️  No positions found after filtering")
        return pd.DataFrame(), ""
    
    # Validate Account is present for all positions (CRITICAL GUARDRAIL)
    if df['Account'].isna().any():
        raise ValueError(
            f"❌ FATAL: {df['Account'].isna().sum()} positions missing Account identifier.\n"
            "   All positions must have an account. Data integrity violation."
        )
    
    # Classify asset type (OPTION vs STOCK vs UNKNOWN)
    # MUST happen before Premium derivation
    def classify_asset_type(symbol: str) -> str:
        """Determine if symbol is an OPTION or STOCK based on OCC pattern.
        
        Returns:
            'OPTION' if matches OCC pattern
            'STOCK' if valid stock ticker pattern (all caps, reasonable length)
            'UNKNOWN' if malformed or unrecognizable
        """
        if pd.isnull(symbol):
            return 'UNKNOWN'
        symbol_str = str(symbol).strip()
        
        # Check if it's an option
        if OCC_OPTION_PATTERN.match(symbol_str):
            return 'OPTION'
        
        # Check if it's a valid stock ticker (1-5 uppercase letters, no numbers)
        # This prevents malformed option symbols from being classified as STOCK
        if re.match(r'^[A-Z]{1,5}$', symbol_str):
            return 'STOCK'
        
        # Everything else is unknown (malformed option symbols, invalid tickers)
        return 'UNKNOWN'
    
    df['AssetType'] = df['Symbol'].apply(classify_asset_type)
    
    # === Canonical Underlying Ticker (Identity Law) ===
    def extract_underlying_ticker(row: pd.Series) -> str:
        symbol = str(row['Symbol']).strip()
        if row['AssetType'] == 'STOCK':
            return symbol
        # For options, extract the root ticker from OCC format (e.g. AAPL from AAPL250117C150)
        # OCC format: [TICKER][YYMMDD][C/P][STRIKE]
        match = re.match(r'^-?([A-Z]+)\d{6}[CP]\d+', symbol)
        if match:
            return match.group(1)
        return symbol # Fallback

    df['Underlying_Ticker'] = df.apply(extract_underlying_ticker, axis=1)
    
    # === Premium derivation (BROKER TRUTH) ===
    # Fidelity provides "Time Val" (extrinsic value) for options
    # Premium = Time Val + Intrinsic Val, but Fidelity doesn't export Intrinsic Val
    # Conservative approach: Use Time Val as Premium (understates ITM options, but is broker truth)
    # Future enhancement: Compute Intrinsic Val after symbol parsing (Phase 2)
    # NOTE: Time Val is ABSOLUTE VALUE (unsigned). Quantity sign determines credit/debit.
    if 'Time Val' in df.columns:
        df['Premium'] = df['Time Val']
        # For stock positions, Premium is NaN (not applicable)
        stock_mask = df['AssetType'] == 'STOCK'
        df.loc[stock_mask, 'Premium'] = pd.NA
        print(f"✅ Premium derived from Time Val (broker truth) for {(~stock_mask).sum()} option positions")
    else:
        # If Time Val missing, Premium must be provided by broker
        if 'Premium' not in df.columns:
            print("⚠️  Warning: 'Time Val' column not found and no 'Premium' column exists.")
            print("   Premium is required for Phase 2. Broker export may be incomplete.")
    
    # === Premium Sign Validation (DATA QUALITY GATE) ===
    # Premium should be POSITIVE (absolute value per share/contract)
    # Quantity sign determines cash flow direction:
    #   - Quantity < 0 (short) → credit received (cash IN)
    #   - Quantity > 0 (long) → debit paid (cash OUT)
    # Validate: Premium must be >= 0 for all options
    if 'Premium' in df.columns:
        option_mask = df['AssetType'] == 'OPTION'
        options_with_premium = option_mask & df['Premium'].notna()
        
        if options_with_premium.any():
            negative_premium = options_with_premium & (df['Premium'] < 0)
            
            if negative_premium.any():
                error_count = negative_premium.sum()
                error_samples = df.loc[negative_premium, ['Symbol', 'Quantity', 'Premium']].head(3)
                raise ValueError(
                    f"❌ FATAL: {error_count} option positions have NEGATIVE Premium.\n"
                    f"   Premium must be positive (absolute value per share/contract).\n"
                    f"   Quantity sign determines credit/debit nature.\n"
                    f"   Sample violations:\n{error_samples.to_string(index=False)}\n"
                    f"   Action: Fix broker export. Negative Premium values are invalid."
                )
    
    # Validate AssetType classification
    unknown_count = (df['AssetType'] == 'UNKNOWN').sum()
    if unknown_count > 0:
        print(f"⚠️  Warning: {unknown_count} positions with UNKNOWN AssetType")
        print(f"   Symbols: {df[df['AssetType'] == 'UNKNOWN']['Symbol'].tolist()[:5]}")

    # Add snapshot timestamp (required for drift tracking)
    # Evaluate timestamp once to ensure consistency between data and filename
    snapshot_timestamp = datetime.now()
    df['Snapshot_TS'] = snapshot_timestamp

    # Optional: Save snapshot
    snapshot_path = ""
    if save_snapshot:
        snapshot_dir = Path(CANONICAL_SNAPSHOT_DIR)
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp_str = snapshot_timestamp.strftime('%Y-%m-%d_%H-%M-%S')
        filename = f"phase1_positions_{timestamp_str}.csv"
        snapshot_path = str(snapshot_dir / filename)
        
        df.to_csv(snapshot_path, index=False)
        print(f"💾 Snapshot: {snapshot_path}")

    # Summary
    print(f"✅ Phase 1 complete: {len(df)} positions, {len(df.columns)} columns")

    return df, snapshot_path

# Parity Assertion: Enforce keyword-only configuration for Phase 1
EXPECTED_SIGNATURE = inspect.Signature([
    inspect.Parameter('input_path', inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=Path),
    inspect.Parameter('save_snapshot', inspect.Parameter.KEYWORD_ONLY, annotation=bool, default=True)
], return_annotation=Tuple[pd.DataFrame, str])

assert inspect.signature(phase1_load_and_clean_positions) == EXPECTED_SIGNATURE


# Backward compatibility alias (deprecated)
def phase1_load_and_clean_raw_v2(
    input_path: str = None,
    snapshot_dir: str = None,
    save_snapshot: bool = False
) -> Tuple[pd.DataFrame, str]:
    """
    DEPRECATED: Use phase1_load_and_clean_positions() instead.
    
    This function exists for backward compatibility only.
    It ignores input_path and snapshot_dir arguments and uses canonical paths.
    """
    import warnings
    warnings.warn(
        "phase1_load_and_clean_raw_v2() is deprecated. "
        "Use phase1_load_and_clean_positions() instead. "
        "Canonical input: data/brokerage_inputs/fidelity_positions.csv",
        DeprecationWarning,
        stacklevel=2
    )
    return phase1_load_and_clean_positions(
        input_path=Path(CANONICAL_INPUT_PATH),
        save_snapshot=save_snapshot
    )
