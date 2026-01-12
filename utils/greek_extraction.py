"""
Greek Extraction Utilities

Extracts Greeks from Contract_Symbols JSON to DataFrame columns.
This is Phase 1 of PCS redesign - makes Greeks accessible for scoring.

Usage:
    from utils.greek_extraction import extract_greeks_to_columns
    
    df = extract_greeks_to_columns(df)
    # Now df has Delta, Gamma, Vega, Theta, Rho, IV_Mid columns
"""

import pandas as pd
import json
import numpy as np
from typing import Dict, List, Any, Optional


def extract_greeks_to_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract Greeks from promoted_strike or Contract_Symbols JSON to DataFrame columns.
    
    PRIORITY: Use 'promoted_strike' if available (single strike for UI/execution).
    FALLBACK: Use 'Contract_Symbols' (multi-leg net Greeks for legacy support).
    
    Handles:
    - Single-leg strategies: Direct Greek assignment
    - Multi-leg strategies: Net Greeks (sum of legs) OR promoted strike
    - Missing data: NaN for clean handling
    
    Args:
        df: DataFrame with promoted_strike or Contract_Symbols column
    
    Returns:
        DataFrame with added columns: Delta, Gamma, Vega, Theta, Rho, IV_Mid,
                                       Promoted_Strike, Promoted_Reason
    
    Example promoted_strike JSON:
        '{"Strike": 150.0, "Option_Type": "Put", "Delta": -0.30, "Gamma": 0.05,
          "Vega": 0.20, "Theta": -0.10, "Promotion_Reason": "Credit Spread Short Strike"}'
    """
    
    # Save original promoted strike column if it exists (to avoid overwriting source data)
    promoted_strike_source_col = None
    if 'Promoted_Strike' in df.columns:
        promoted_strike_source_col = 'Promoted_Strike'
        # Temporarily rename to avoid overwriting during initialization
        df['_Promoted_Strike_Source'] = df['Promoted_Strike']
    elif 'promoted_strike' in df.columns:
        promoted_strike_source_col = 'promoted_strike'
    
    # Initialize columns if they don't exist
    for col in ['Delta', 'Gamma', 'Vega', 'Theta', 'Rho', 'IV_Mid', 'Promoted_Strike_Value']:
        if col not in df.columns:
            df[col] = np.nan
    if 'Promoted_Reason' not in df.columns:
        df['Promoted_Reason'] = ''
    
    # Extract Greeks for each row
    for idx, row in df.iterrows():
        # Check if Greeks already exist as non-null columns (e.g. from Step 9B Schwab)
        # We check for Delta and Vega specifically as they are critical for PCS
        if pd.notna(row.get('Delta')) and pd.notna(row.get('Vega')):
            # Greeks already present, skip extraction unless we want to force refresh
            continue

        # PRIORITY 1: Check for promoted_strike (NEW - check both lowercase and capitalized)
        promoted_json = None
        if 'promoted_strike' in df.columns and not pd.isna(row['promoted_strike']):
            promoted_json = row['promoted_strike']
        elif '_Promoted_Strike_Source' in df.columns and not pd.isna(row['_Promoted_Strike_Source']):
            promoted_json = row['_Promoted_Strike_Source']
        
        if promoted_json and promoted_json != '':
            try:
                # Parse promoted strike JSON
                if isinstance(promoted_json, str):
                    promoted = json.loads(promoted_json)
                elif isinstance(promoted_json, dict):
                    promoted = promoted_json
                else:
                    promoted = None
                
                if promoted and isinstance(promoted, dict):
                    # Extract Greeks from promoted strike (single strike)
                    df.at[idx, 'Delta'] = _safe_float(promoted.get('Delta'))
                    df.at[idx, 'Gamma'] = _safe_float(promoted.get('Gamma'))
                    df.at[idx, 'Vega'] = _safe_float(promoted.get('Vega'))
                    df.at[idx, 'Theta'] = _safe_float(promoted.get('Theta'))
                    df.at[idx, 'Rho'] = _safe_float(promoted.get('Rho'))
                    df.at[idx, 'IV_Mid'] = _safe_float(promoted.get('IV'))
                    df.at[idx, 'Promoted_Strike_Value'] = _safe_float(promoted.get('Strike'))
                    df.at[idx, 'Promoted_Reason'] = promoted.get('Promotion_Reason', '')
                    continue  # Skip fallback
                    
            except (json.JSONDecodeError, KeyError, TypeError, AttributeError) as e:
                # Fall through to legacy approach
                print(f"⚠️  Row {idx}: Failed to extract promoted_strike, falling back to Contract_Symbols - {e}")
        
        # FALLBACK: Use Contract_Symbols (LEGACY)
        contracts_json = row.get('Contract_Symbols')
        
        if pd.isna(contracts_json) or contracts_json == '':
            continue
            
        try:
            # Handle both string JSON and already-parsed objects
            if isinstance(contracts_json, str):
                contracts = json.loads(contracts_json)
            elif isinstance(contracts_json, list):
                contracts = contracts_json
            else:
                # Skip invalid types
                continue
            
            if not contracts or len(contracts) == 0:
                continue
            
            # Validate contracts are dictionaries
            if not all(isinstance(c, dict) for c in contracts):
                print(f"⚠️  Row {idx}: Contracts not in expected format (got {type(contracts[0])})")
                continue
            
            # Calculate net Greeks (sum across all legs)
            net_greeks = _calculate_net_greeks(contracts)
            
            # Assign to DataFrame
            df.at[idx, 'Delta'] = net_greeks.get('delta')
            df.at[idx, 'Gamma'] = net_greeks.get('gamma')
            df.at[idx, 'Vega'] = net_greeks.get('vega')
            df.at[idx, 'Theta'] = net_greeks.get('theta')
            df.at[idx, 'Rho'] = net_greeks.get('rho')
            df.at[idx, 'IV_Mid'] = net_greeks.get('iv_mid')
            
        except (json.JSONDecodeError, KeyError, TypeError, AttributeError) as e:
            # Log error but continue processing
            print(f"⚠️  Row {idx}: Failed to extract Greeks - {e}")
            continue
    
    # Clean up temporary column if created
    if '_Promoted_Strike_Source' in df.columns:
        df = df.drop(columns=['_Promoted_Strike_Source'])
    
    return df


def _calculate_net_greeks(contracts: List[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    """
    Calculate net Greeks from contract list.
    
    Single-leg: Direct Greek values
    Multi-leg: Sum of Greeks (respects buy/sell direction from sign)
    
    Args:
        contracts: List of contract dictionaries with Greeks
        
    Returns:
        Dictionary with net Greek values
    """
    
    if len(contracts) == 1:
        # Single-leg: Direct assignment
        return _extract_single_leg_greeks(contracts[0])
    else:
        # Multi-leg: Net Greeks (sum)
        return _extract_multi_leg_greeks(contracts)


def _extract_single_leg_greeks(contract: Dict[str, Any]) -> Dict[str, Optional[float]]:
    """Extract Greeks from single contract"""
    
    return {
        'delta': _safe_float(contract.get('delta')),
        'gamma': _safe_float(contract.get('gamma')),
        'vega': _safe_float(contract.get('vega')),
        'theta': _safe_float(contract.get('theta')),
        'rho': _safe_float(contract.get('rho')),
        'iv_mid': _safe_float(contract.get('mid_iv'))
    }


def _extract_multi_leg_greeks(contracts: List[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    """
    Calculate net Greeks for multi-leg strategies.
    
    Note: Greeks already include buy/sell direction (positive/negative).
    For short positions, Greeks are already negated in the data.
    """
    
    net_delta = 0.0
    net_gamma = 0.0
    net_vega = 0.0
    net_theta = 0.0
    net_rho = 0.0
    iv_values = []
    
    for contract in contracts:
        # Validate contract is a dictionary
        if not isinstance(contract, dict):
            continue
            
        # Sum Greeks (already signed correctly)
        delta = _safe_float(contract.get('delta'))
        gamma = _safe_float(contract.get('gamma'))
        vega = _safe_float(contract.get('vega'))
        theta = _safe_float(contract.get('theta'))
        rho = _safe_float(contract.get('rho'))
        
        if delta is not None:
            net_delta += delta
        if gamma is not None:
            net_gamma += gamma
        if vega is not None:
            net_vega += vega
        if theta is not None:
            net_theta += theta
        if rho is not None:
            net_rho += rho
        
        # Collect IV for averaging
        mid_iv = _safe_float(contract.get('mid_iv'))
        if mid_iv is not None:
            iv_values.append(mid_iv)
    
    # Average IV across legs (simple mean)
    avg_iv = sum(iv_values) / len(iv_values) if iv_values else None
    
    return {
        'delta': net_delta if net_delta != 0.0 else None,
        'gamma': net_gamma if net_gamma != 0.0 else None,
        'vega': net_vega if net_vega != 0.0 else None,
        'theta': net_theta if net_theta != 0.0 else None,
        'rho': net_rho if net_rho != 0.0 else None,
        'iv_mid': avg_iv
    }


def _safe_float(value: Any) -> Optional[float]:
    """Safely convert value to float, return None if invalid"""
    
    if value is None or value == '' or pd.isna(value):
        return None
    
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def validate_greek_extraction(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate Greek extraction quality.
    
    Returns:
        Dictionary with validation metrics
    """
    
    total = len(df)
    
    # Count rows with Greeks
    has_delta = df['Delta'].notna().sum()
    has_gamma = df['Gamma'].notna().sum()
    has_vega = df['Vega'].notna().sum()
    has_theta = df['Theta'].notna().sum()
    has_rho = df['Rho'].notna().sum()
    has_iv = df['IV_Mid'].notna().sum()
    
    # Greek ranges (sanity checks)
    delta_range = (df['Delta'].min(), df['Delta'].max()) if has_delta > 0 else (None, None)
    vega_range = (df['Vega'].min(), df['Vega'].max()) if has_vega > 0 else (None, None)
    
    return {
        'total_rows': total,
        'rows_with_delta': has_delta,
        'rows_with_gamma': has_gamma,
        'rows_with_vega': has_vega,
        'rows_with_theta': has_theta,
        'rows_with_rho': has_rho,
        'rows_with_iv': has_iv,
        'delta_coverage': f"{100 * has_delta / total:.1f}%" if total > 0 else "N/A",
        'vega_coverage': f"{100 * has_vega / total:.1f}%" if total > 0 else "N/A",
        'delta_range': delta_range,
        'vega_range': vega_range,
        'quality': 'GOOD' if has_delta >= 0.8 * total else 'PARTIAL' if has_delta > 0.5 * total else 'POOR'
    }


if __name__ == '__main__':
    # Quick test with sample data
    print("="*70)
    print("GREEK EXTRACTION TEST")
    print("="*70)
    print()
    
    # Sample data
    sample_data = {
        'Ticker': ['AAPL', 'AAPL', 'AAPL'],
        'Strategy': ['Long Call', 'Long Put', 'Long Straddle'],
        'Contract_Symbols': [
            '[{"symbol": "AAPL250214C180", "delta": 0.52, "gamma": 0.03, "vega": 0.25, "theta": -0.15, "rho": 0.08, "mid_iv": 0.30}]',
            '[{"symbol": "AAPL250214P180", "delta": -0.48, "gamma": 0.03, "vega": 0.25, "theta": -0.15, "rho": -0.07, "mid_iv": 0.32}]',
            '[{"symbol": "AAPL250214C180", "delta": 0.52, "gamma": 0.03, "vega": 0.25, "theta": -0.15, "rho": 0.08, "mid_iv": 0.30}, {"symbol": "AAPL250214P180", "delta": -0.48, "gamma": 0.03, "vega": 0.25, "theta": -0.15, "rho": -0.07, "mid_iv": 0.32}]'
        ]
    }
    
    df = pd.DataFrame(sample_data)
    
    print("Before extraction:")
    print(df[['Ticker', 'Strategy']].to_string(index=False))
    print()
    
    # Extract Greeks
    df = extract_greeks_to_columns(df)
    
    print("After extraction:")
    print(df[['Strategy', 'Delta', 'Gamma', 'Vega', 'Theta', 'IV_Mid']].to_string(index=False))
    print()
    
    # Validate
    validation = validate_greek_extraction(df)
    print("Validation:")
    for key, value in validation.items():
        print(f"  {key}: {value}")
    
    print()
    print("✅ Greek extraction working!")
