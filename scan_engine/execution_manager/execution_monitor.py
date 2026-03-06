import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

class ExecutionMonitor:
    """
    Manages the transition of strategies from Scan Suggestions to Execution-Ready Decisions.
    Handles MONITOR_READY and MONITOR_WAIT states, including confirmation rules and time-based expiration.
    """

    def __init__(self):
        # Define a comprehensive set of columns for all monitoring DataFrames
        common_cols = ['Ticker', 'Strategy_Name', 'monitor_status', 'monitor_reason', 'last_monitored_ts']
        ready_wait_cols = common_cols + ['confidence_band', 'entry_price_lower', 'entry_price_upper']
        wait_specific_cols = ready_wait_cols + ['watchlist_expiration_ts']

        self.monitor_ready_strategies: pd.DataFrame = pd.DataFrame(columns=ready_wait_cols)
        self.monitor_wait_strategies: pd.DataFrame = pd.DataFrame(columns=wait_specific_cols)
        self.archived_strategies: pd.DataFrame = pd.DataFrame(columns=common_cols)
        self.current_time = datetime.now() # Represents the current market time for evaluation

    def ingest_scan_suggestions(self, scan_results: pd.DataFrame):
        """
        Ingests strategies from the scan engine output.
        READY_NOW strategies go to monitor_ready.
        WAIT strategies go to monitor_wait.
        """
        if scan_results.empty:
            logger.info("No new scan suggestions to ingest.")
            return

        ready_now_suggestions = scan_results[scan_results['acceptance_status'] == 'READY_NOW'].copy()
        wait_suggestions = scan_results[scan_results['acceptance_status'] == 'WAIT'].copy()

        # Add initial monitoring metadata
        for df_subset in [ready_now_suggestions, wait_suggestions]:
            if not df_subset.empty:
                df_subset['monitor_status'] = df_subset['acceptance_status'].apply(lambda x: 'MONITOR_READY' if x == 'READY_NOW' else 'MONITOR_WAIT')
                df_subset['monitor_reason'] = 'Initial scan suggestion'
                df_subset['last_monitored_ts'] = self.current_time
                df_subset['watchlist_expiration_ts'] = df_subset.get('watchlist_expiration_ts', np.nan) # Preserve if exists, else NaN
                df_subset['entry_price_lower'] = df_subset.get('Entry_Band_Lower', np.nan) # Assuming scan_engine provides this
                df_subset['entry_price_upper'] = df_subset.get('Entry_Band_Upper', np.nan) # Assuming scan_engine provides this
                df_subset['confidence_band'] = df_subset.get('Confidence_Band', np.nan) # Add confidence_band

        # Deduplicate columns before concat to handle merged DataFrames with repeated column names
        ready_now_suggestions = ready_now_suggestions.loc[:, ~ready_now_suggestions.columns.duplicated()].reset_index(drop=True)
        wait_suggestions = wait_suggestions.loc[:, ~wait_suggestions.columns.duplicated()].reset_index(drop=True)
        self.monitor_ready_strategies = pd.concat([self.monitor_ready_strategies, ready_now_suggestions], ignore_index=True, sort=False)
        self.monitor_wait_strategies = pd.concat([self.monitor_wait_strategies, wait_suggestions], ignore_index=True, sort=False)

        logger.info(f"Ingested scan suggestions: {len(ready_now_suggestions)} READY_NOW -> MONITOR_READY, {len(wait_suggestions)} WAIT -> MONITOR_WAIT.")

    def update_market_context(self, current_time: datetime):
        """Updates the internal market time for time-based evaluations."""
        self.current_time = current_time
        logger.debug(f"ExecutionMonitor: Market context updated to {self.current_time}")

    def evaluate_monitoring_cycle(self, current_market_data: pd.DataFrame) -> pd.DataFrame:
        """
        Evaluates all monitored strategies against current market data and rules.
        Returns strategies ready for execution.
        """
        logger.info(f"Starting monitoring cycle at {self.current_time}...")
        execution_ready_candidates = pd.DataFrame()

        # 1. Evaluate MONITOR_WAIT strategies for promotion
        if not self.monitor_wait_strategies.empty:
            logger.info(f"Evaluating {len(self.monitor_wait_strategies)} MONITOR_WAIT strategies...")
            promoted_to_ready = []
            strategies_to_archive_from_wait = []
            for idx, strategy in self.monitor_wait_strategies.iterrows():
                if self._check_watchlist_expiration(strategy):
                    strategies_to_archive_from_wait.append(idx)
                    self._archive_strategy(strategy, "Watchlist expired without confirmation")
                elif self._check_wait_confirmation(strategy, current_market_data):
                    strategy['monitor_status'] = 'MONITOR_READY'
                    strategy['monitor_reason'] = 'Confirmation rules met'
                    strategy['last_monitored_ts'] = self.current_time
                    promoted_to_ready.append(strategy)
                    strategies_to_archive_from_wait.append(idx) # Remove from wait after promotion
            
            if promoted_to_ready:
                self.monitor_ready_strategies = pd.concat([self.monitor_ready_strategies, pd.DataFrame(promoted_to_ready)], ignore_index=True)
                logger.info(f"Promoted {len(promoted_to_ready)} strategies from MONITOR_WAIT to MONITOR_READY.")
            
            if strategies_to_archive_from_wait:
                self.monitor_wait_strategies.drop(index=strategies_to_archive_from_wait, inplace=True)
                self.monitor_wait_strategies.reset_index(drop=True, inplace=True)


        # 2. Evaluate MONITOR_READY strategies for execution
        if not self.monitor_ready_strategies.empty:
            logger.info(f"Evaluating {len(self.monitor_ready_strategies)} MONITOR_READY strategies...")
            ready_for_execution = []
            strategies_to_remove_from_ready = []
            for idx, strategy in self.monitor_ready_strategies.iterrows():
                if self._check_execution_readiness(strategy, current_market_data):
                    strategy['monitor_status'] = 'EXECUTION_PENDING'
                    strategy['monitor_reason'] = 'Execution conditions met'
                    strategy['last_monitored_ts'] = self.current_time
                    ready_for_execution.append(strategy)
                    strategies_to_remove_from_ready.append(idx)
                elif not self._check_strategy_validity(strategy, current_market_data):
                    strategies_to_remove_from_ready.append(idx)
                    self._archive_strategy(strategy, "Strategy validity expired during monitoring")
                elif self._check_unfavorable_conditions(strategy, current_market_data):
                    strategy['monitor_status'] = 'MONITOR_WAIT'
                    strategy['monitor_reason'] = 'Market conditions unfavorable, reverted to WAIT'
                    strategy['last_monitored_ts'] = self.current_time
                    self.monitor_wait_strategies = pd.concat([self.monitor_wait_strategies, pd.DataFrame([strategy])], ignore_index=True)
                    strategies_to_remove_from_ready.append(idx)

            if ready_for_execution:
                execution_ready_candidates = pd.concat([execution_ready_candidates, pd.DataFrame(ready_for_execution)], ignore_index=True)
                logger.info(f"Identified {len(ready_for_execution)} strategies ready for execution.")
            
            if strategies_to_remove_from_ready:
                self.monitor_ready_strategies.drop(index=strategies_to_remove_from_ready, inplace=True)
                self.monitor_ready_strategies.reset_index(drop=True, inplace=True)

        logger.info(f"Monitoring cycle complete. {len(execution_ready_candidates)} candidates for execution.")
        return execution_ready_candidates

    def _check_wait_confirmation(self, strategy: pd.Series, current_market_data: pd.DataFrame) -> bool:
        """
        Evaluates if a MONITOR_WAIT strategy's confirmation rules are met.
        This is where specific confirmation triggers (e.g., price pullback to support) would be implemented.
        """
        ticker_data = current_market_data[current_market_data['Ticker'] == strategy['Ticker']]
        if ticker_data.empty:
            return False

        current_price = ticker_data['last_price'].iloc[0]
        current_momentum = ticker_data.get('momentum_tag', 'UNKNOWN')
        current_structure = ticker_data.get('structure_bias', 'UNCLEAR')
        current_iv_rank = ticker_data.get('IV_Rank_XS', np.nan) # Assuming current IV Rank is available

        # Rule: Promotion from WAIT -> READY_NOW
        # This logic needs to be derived from the playbook's "confirmation rules" for WAIT strategies.
        # For now, a simplified example based on the acceptance_reason from the scan engine.
        wait_reason = strategy.get('acceptance_reason', '')
        strategy_type = strategy.get('Strategy_Type', 'UNKNOWN')
        
        # Example: If a directional strategy was waiting due to range-bound/slow regime, check if it's trending now
        if "Short-dated directional in range-bound/slow regime" in wait_reason:
            if current_structure == 'TRENDING' and current_momentum in ['STRONG_UP_DAY', 'STRONG_DOWN_DAY']:
                logger.debug(f"WAIT -> READY for {strategy['Ticker']} {strategy['Strategy_Name']}: Regime shifted to TRENDING.")
                return True
        
        # Example: If a LEAP was waiting due to non-sustained trend
        if "LEAP requires sustained trend or clear structural thesis" in wait_reason:
            if current_structure == 'TRENDING' and strategy.get('directional_bias') in ['BULLISH_STRONG', 'BEARISH_STRONG']:
                logger.debug(f"WAIT -> READY for {strategy['Ticker']} {strategy['Strategy_Name']}: LEAP now has sustained trend.")
                return True

        # Example: If a volatility strategy was waiting for IV maturity (assuming IV_Maturity_State is in current_market_data)
        if "MISSING_IV_HARD_GATE" in wait_reason and strategy_type == 'VOLATILITY':
            # This would require current_market_data to have IV_Maturity_State
            # For now, a placeholder: assume it matures after some time
            if pd.isna(strategy.get('watchlist_expiration_ts')):
                strategy['watchlist_expiration_ts'] = self.current_time + timedelta(days=120) # Wait for 120 days for maturity
            if self.current_time >= strategy['watchlist_expiration_ts']:
                logger.debug(f"WAIT -> READY for {strategy['Ticker']} {strategy['Strategy_Name']}: Volatility IV matured (simulated).")
                return True

        # Generic time-based promotion for other WAIT reasons (to prevent forever watchlists)
        if pd.isna(strategy.get('watchlist_expiration_ts')):
            strategy['watchlist_expiration_ts'] = self.current_time + timedelta(days=7) # Default 7 days wait for generic WAIT
        if self.current_time >= strategy['watchlist_expiration_ts']:
            logger.debug(f"WAIT -> READY for {strategy['Ticker']} {strategy['Strategy_Name']}: Generic WAIT expired (simulated).")
            return True

        return False

    def _check_watchlist_expiration(self, strategy: pd.Series) -> bool:
        """Checks if a WAIT strategy's watchlist period has expired."""
        # Playbook: "Time-Based Expiration" for WAIT strategies.
        # If confirmation trigger is not met within this timeframe, the strategy transitions to ARCHIVED.
        # Let's define a default watchlist expiry of 30 days if not explicitly set.
        if pd.isna(strategy.get('watchlist_expiration_ts')):
            strategy['watchlist_expiration_ts'] = strategy['last_monitored_ts'] + timedelta(days=30) # Default 30 days

        if self.current_time > strategy['watchlist_expiration_ts']:
            logger.debug(f"Strategy {strategy['Ticker']} {strategy['Strategy_Name']} watchlist expired at {self.current_time}.")
            return True
        return False

    def _check_execution_readiness(self, strategy: pd.Series, current_market_data: pd.DataFrame) -> bool:
        """
        Checks if a MONITOR_READY strategy meets real-time execution conditions.
        This includes pricing discipline, current market conditions, etc.
        """
        ticker_data = current_market_data[current_market_data['Ticker'] == strategy['Ticker']]
        if ticker_data.empty:
            return False

        current_price = ticker_data['last_price'].iloc[0]
        strategy_type = strategy.get('Strategy_Type', 'UNKNOWN')
        selected_strike = strategy.get('Selected_Strike', np.nan)
        entry_lower = strategy.get('entry_price_lower', np.nan)
        entry_upper = strategy.get('entry_price_upper', np.nan)

        # Pricing Discipline: Current price must be within the acceptable entry band
        if pd.notna(entry_lower) and pd.notna(entry_upper):
            if not (entry_lower <= current_price <= entry_upper):
                logger.debug(f"Strategy {strategy['Ticker']} {strategy['Strategy_Name']}: Price {current_price:.2f} outside entry band [{entry_lower:.2f}, {entry_upper:.2f}].")
                return False
        
        # Strategy-specific pricing checks (simplified)
        if strategy_type == 'INCOME':
            if strategy['Strategy_Name'] == 'Cash-Secured Put':
                # For CSP, we want the current price to be above the strike to collect premium without immediate assignment
                if current_price < selected_strike:
                    logger.debug(f"CSP {strategy['Ticker']}: Current price {current_price:.2f} below strike {selected_strike:.2f} - not ready for execution.")
                    return False
            elif strategy['Strategy_Name'] == 'Buy-Write':
                # For Buy-Write, we want the current price to be at an acceptable level for stock acquisition
                # Placeholder: assume current price is acceptable if within a small buffer of last_price
                if not (strategy['last_price'] * 0.98 <= current_price <= strategy['last_price'] * 1.02):
                    logger.debug(f"Buy-Write {strategy['Ticker']}: Current price {current_price:.2f} outside acceptable range for stock acquisition.")
                    return False
        
        elif strategy_type == 'DIRECTIONAL':
            # For directional, we want price moving in the desired direction
            # Placeholder: assume it's ready if price is moving up/down (needs more sophisticated signals)
            pass # For now, assume if price is in band, it's ready

        logger.debug(f"Strategy {strategy['Ticker']} {strategy['Strategy_Name']} meets execution readiness criteria.")
        return True

    def _check_strategy_validity(self, strategy: pd.Series, current_market_data: pd.DataFrame) -> bool:
        """
        Re-evaluates if the underlying conditions for the strategy's READY_NOW status
        are still valid based on current market data.
        This prevents "execution bias after a strategy is approved."
        """
        ticker_data = current_market_data[current_market_data['Ticker'] == strategy['Ticker']]
        if ticker_data.empty:
            return True # Cannot re-evaluate without data, assume valid for now

        # This would involve re-running a lightweight version of Phase 1 checks from the scan engine
        # For now, a placeholder: check if the primary directional bias or market regime has drastically changed.
        current_directional_bias = ticker_data.get('directional_bias', 'NEUTRAL') # Assuming current_market_data has this
        current_structure_bias = ticker_data.get('structure_bias', 'UNCLEAR') # Assuming current_market_data has this

        if strategy.get('Strategy_Type') == 'DIRECTIONAL':
            if strategy.get('Trade_Bias') == 'Bullish' and current_directional_bias == 'BEARISH_STRONG':
                logger.debug(f"Strategy {strategy['Ticker']} {strategy['Strategy_Name']} validity expired: Strong bearish shift.")
                return False
            if strategy.get('Trade_Bias') == 'Bearish' and current_directional_bias == 'BULLISH_STRONG':
                logger.debug(f"Strategy {strategy['Ticker']} {strategy['Strategy_Name']} validity expired: Strong bullish shift.")
                return False
            if current_structure_bias == 'RANGE_BOUND' and strategy.get('Actual_DTE', 0) < 90: # Short-dated in range-bound
                logger.debug(f"Strategy {strategy['Ticker']} {strategy['Strategy_Name']} validity expired: Short-dated in range-bound.")
                return False
        
        # Playbook: "If the fundamental or technical outlook for the stock changes significantly, close the position."
        # This is a simplified check. A full implementation would involve re-running parts of Step 5/6.
        return True

    def _check_unfavorable_conditions(self, strategy: pd.Series, current_market_data: pd.DataFrame) -> bool:
        """
        Checks if market conditions have become unfavorable, warranting a downgrade to MONITOR_WAIT.
        This prevents "chasing price" and "premature entries."
        """
        ticker_data = current_market_data[current_market_data['Ticker'] == strategy['Ticker']]
        if ticker_data.empty:
            return False

        current_price = ticker_data['last_price'].iloc[0]
        current_iv_rank = ticker_data.get('IV_Rank_XS', np.nan)
        current_momentum = ticker_data.get('momentum_tag', 'UNKNOWN')
        current_structure = ticker_data.get('structure_bias', 'UNCLEAR')

        # Playbook: "If the price moves outside the acceptable range, the strategy reverts to MONITOR_WAIT"
        # This is already handled in _check_execution_readiness, but we can add other conditions here.
        
        # Example: If IV collapses for a long option (vega risk)
        if strategy.get('Strategy_Type') == 'DIRECTIONAL' and ('Long Call' in strategy['Strategy_Name'] or 'Long Put' in strategy['Strategy_Name']):
            if pd.notna(current_iv_rank) and current_iv_rank > 70: # IV became very high after entry
                logger.debug(f"Strategy {strategy['Ticker']} {strategy['Strategy_Name']} downgraded to WAIT: IV became very high.")
                return True
        
        # Example: If a directional strategy's underlying suddenly enters a strong range-bound regime
        if strategy.get('Strategy_Type') == 'DIRECTIONAL' and current_structure == 'RANGE_BOUND' and strategy.get('Actual_DTE', 0) >= 90: # Long-dated in range-bound
            logger.debug(f"Strategy {strategy['Ticker']} {strategy['Strategy_Name']} downgraded to WAIT: Long-dated in range-bound regime.")
            return True

        return False

    def _archive_strategy(self, strategy: pd.Series, reason: str):
        """Archives a strategy, removing it from active monitoring."""
        strategy['monitor_status'] = 'ARCHIVED'
        strategy['monitor_reason'] = reason
        strategy['last_monitored_ts'] = self.current_time
        self.archived_strategies = pd.concat([self.archived_strategies, pd.DataFrame([strategy])], ignore_index=True)
        logger.info(f"Strategy {strategy['Ticker']} {strategy['Strategy_Name']} archived: {reason}")

    def get_monitoring_summary(self) -> Dict[str, Any]:
        """Provides a summary of strategies in different monitoring states."""
        return {
            "monitor_ready_count": len(self.monitor_ready_strategies),
            "monitor_wait_count": len(self.monitor_wait_strategies),
            "archived_count": len(self.archived_strategies),
            "monitor_ready_strategies": self.monitor_ready_strategies[['Ticker', 'Strategy_Name', 'monitor_reason', 'confidence_band', 'entry_price_lower', 'entry_price_upper']].to_dict(orient='records'),
            "monitor_wait_strategies": self.monitor_wait_strategies[['Ticker', 'Strategy_Name', 'monitor_reason', 'confidence_band', 'watchlist_expiration_ts', 'entry_price_lower', 'entry_price_upper']].to_dict(orient='records'),
            "archived_strategies": self.archived_strategies[['Ticker', 'Strategy_Name', 'monitor_reason']].to_dict(orient='records'),
        }
