"""
Cycle 3: Strategy Resolver

Reconstructs interpretive strategy labels from structural leg data.
This ensures Cycle 1/2 remain strategy-agnostic while Cycle 3 can 
apply strategy-specific doctrine.
"""

import pandas as pd
import logging
from core.management.cycle1.identity.constants import (
    STRATEGY_UNKNOWN,
    STRATEGY_COVERED_CALL,
    STRATEGY_BUY_WRITE,
    STRATEGY_BUY_CALL,
    STRATEGY_BUY_PUT,
    STRATEGY_CSP,
    STRATEGY_LONG_STRADDLE,
    STRATEGY_LONG_STRANGLE,
    STRATEGY_STOCK,
    ASSET_TYPE_STOCK,
    ASSET_TYPE_OPTION
)

logger = logging.getLogger(__name__)

class StrategyResolver:
    """
    RAG Authority: McMillan (Classification)
    
    Deterministic mapping of leg structures to canonical strategies.
    """
    
    @staticmethod
    def resolve(df: pd.DataFrame) -> pd.DataFrame:
        """
        Resolves and attaches the 'Strategy' column to the DataFrame.
        
        Mandatory for Cycle 3 execution.
        """
        if df.empty:
            return df
            
        df = df.copy()
        
        # 1. Assign temporary LegRoles for resolution
        df = StrategyResolver._assign_temporary_roles(df)
        
        # 2. Group by TradeID and resolve
        strategies = {}
        for tid, group in df.groupby("TradeID"):
            # RAG: Absolute Authority. Strategy identity is an immutable inception property.
            # Order of precedence:
            # 1. Frozen Entry_Structure (Canonical)
            # 2. Inception Strategy (if present)
            # 3. Inference from legs (Fallback only)
            
            resolved_strat = None
            
            # 1. Check Entry_Structure (Tier 1 Authority)
            if 'Entry_Structure' in group.columns:
                valid_structures = group['Entry_Structure'].dropna().unique()
                # Filter out generic placeholders
                specific = [s for s in valid_structures if s not in ['Unknown', 'STOCK', 'OPTION', 'UNKNOWN']]
                if specific:
                    resolved_strat = specific[0]
                    # Promote LONG_CALL/LONG_PUT → LEAPS when current DTE > 200.
                    # Handles positions frozen before LEAPS classification was added.
                    if resolved_strat in ('LONG_CALL', 'LONG_PUT'):
                        _dte_max = group['DTE'].max() if 'DTE' in group.columns else 0
                        try:
                            _dte_max = float(_dte_max)
                        except (TypeError, ValueError):
                            _dte_max = 0.0
                        if _dte_max > 200:
                            resolved_strat = 'LEAPS_CALL' if resolved_strat == 'LONG_CALL' else 'LEAPS_PUT'

            # 2. Check existing Strategy column (Tier 2 Authority - Inception Label)
            if not resolved_strat and 'Strategy' in group.columns:
                existing_strats = group['Strategy'].dropna().unique()
                specific = [s for s in existing_strats if s not in ['Unknown', 'UNKNOWN', 'STOCK', 'OPTION']]
                if specific:
                    resolved_strat = specific[0]
            
            # 3. Fallback to inference (Tier 3 - Structural Reconstruction)
            if not resolved_strat:
                resolved_strat = StrategyResolver._identify_strategy(group)
                
            strategies[tid] = resolved_strat
            
        df['Strategy'] = df['TradeID'].map(strategies)
        
        # 3. Hard Failure Guard
        # RAG: Governance. We allow UNKNOWN to pass through to the Decision Engine
        # where it will be handled as a neutral HOLD. We log the failure but do NOT halt.
        unresolved = df[df['Strategy'] == STRATEGY_UNKNOWN]['TradeID'].unique()
        if len(unresolved) > 0:
            logger.warning(f"⚠️ STRATEGY RESOLUTION WARNING: Could not determine strategy for TradeIDs: {unresolved.tolist()}")
            
        # Cleanup temporary roles if they weren't in original df
        if 'LegRole' not in df.columns:
            # We keep it if it helps Cycle 3, but for now we just ensure Strategy is there
            pass
            
        return df

    @staticmethod
    def _assign_temporary_roles(df: pd.DataFrame) -> pd.DataFrame:
        """Internal role assignment for classification purposes."""
        def get_role(row):
            asset = row.get('AssetType')
            qty = row.get('Quantity', 0)
            opt_type = row.get('Call/Put')
            
            if asset == ASSET_TYPE_STOCK:
                return 'Stock_Long' if qty > 0 else 'Stock_Short'
            
            if asset == ASSET_TYPE_OPTION:
                if opt_type == 'Call':
                    return 'Long_Call' if qty > 0 else 'Short_Call'
                if opt_type == 'Put':
                    return 'Long_Put' if qty > 0 else 'Short_Put'
            return 'Unknown'

        df['LegRole'] = df.apply(get_role, axis=1)
        return df

    @staticmethod
    def _identify_strategy(group: pd.DataFrame) -> str:
        roles = sorted(group['LegRole'].tolist())
        dte = group['DTE'].max() if 'DTE' in group.columns else 0
        
        # Single-leg
        if len(group) == 1:
            role = roles[0]
            if role == 'Long_Call': 
                # RAG: Classification. LEAPS are directional but managed differently.
                return "LEAPS_CALL" if dte > 200 else STRATEGY_BUY_CALL
            if role == 'Long_Put': 
                return "LEAPS_PUT" if dte > 200 else STRATEGY_BUY_PUT
            if role == 'Short_Put': return STRATEGY_CSP
            if role == 'Short_Call': return STRATEGY_COVERED_CALL # Orphaned CC leg
            if role in ['Stock_Long', 'Stock_Short']: return STRATEGY_STOCK
            
        # Multi-leg
        if len(group) == 2:
            if 'Stock_Long' in roles and 'Short_Call' in roles:
                # LIMITATION: Cannot distinguish BUY_WRITE (stock + call entered together)
                # from COVERED_CALL (call written against pre-existing stock) without
                # entry timestamps. BUY_WRITE is the safer default — its doctrine
                # enforces stricter stock authority checks (Cohen Ch.3).
                # Tier 1 (Entry_Structure) resolves most cases before reaching here.
                return STRATEGY_BUY_WRITE
            if 'Long_Call' in roles and 'Long_Put' in roles:
                strikes = group['Strike'].nunique()
                return STRATEGY_LONG_STRADDLE if strikes == 1 else STRATEGY_LONG_STRANGLE
                
        return STRATEGY_UNKNOWN
