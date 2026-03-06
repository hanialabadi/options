import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple

logger = logging.getLogger(__name__)

class OrderManager:
    """
    Manages the actual placement and lifecycle of trade orders.
    Handles EXECUTION_PENDING and ACTIVE_TRADE states.
    """

    def __init__(self):
        # Define all possible columns that might appear in these DataFrames
        all_possible_cols = [
            'Ticker', 'Strategy_Name', 'order_status', 'order_placed_ts',
            'fill_price', 'fill_ts', 'exit_price', 'exit_ts', 'pnl', 'exit_reason',
            'initial_premium', 'initial_implied_volatility', 'Selected_Strike', 'Actual_DTE'
        ]

        self.execution_pending_orders: pd.DataFrame = pd.DataFrame(columns=all_possible_cols)
        self.active_trades: pd.DataFrame = pd.DataFrame(columns=all_possible_cols)
        self.closed_trades: pd.DataFrame = pd.DataFrame(columns=all_possible_cols)
        self.current_time = datetime.now() # Represents the current market time for evaluation

    def update_market_context(self, current_time: datetime):
        """Updates the internal market time for time-based evaluations."""
        self.current_time = current_time
        logger.debug(f"OrderManager: Market context updated to {self.current_time}")

    def ingest_execution_candidates(self, execution_candidates: pd.DataFrame):
        """
        Ingests strategies identified as ready for execution by the ExecutionMonitor.
        These move to the EXECUTION_PENDING state.
        """
        if execution_candidates.empty:
            logger.info("No new execution candidates to ingest.")
            return

        # Add initial order metadata
        execution_candidates['order_status'] = 'EXECUTION_PENDING'
        execution_candidates['order_placed_ts'] = self.current_time
        execution_candidates['fill_price'] = np.nan
        execution_candidates['fill_ts'] = np.nan
        execution_candidates['exit_price'] = np.nan
        execution_candidates['exit_ts'] = np.nan
        execution_candidates['pnl'] = np.nan
        execution_candidates['exit_reason'] = np.nan
        execution_candidates['initial_premium'] = execution_candidates.apply(self._get_initial_premium, axis=1) # Calculate initial premium
        execution_candidates['initial_implied_volatility'] = execution_candidates.get('Implied_Volatility', np.nan) # Store initial IV

        # Ensure all columns from self.execution_pending_orders are present in execution_candidates
        # before concatenation, filling missing ones with NaN.
        missing_cols = set(self.execution_pending_orders.columns) - set(execution_candidates.columns)
        for col in missing_cols:
            execution_candidates[col] = np.nan

        self.execution_pending_orders = pd.concat([self.execution_pending_orders, execution_candidates], ignore_index=True, sort=False)
        logger.info(f"Ingested {len(execution_candidates)} strategies into EXECUTION_PENDING.")

    def process_orders(self, current_market_data: pd.DataFrame) -> List[Dict]:
        """
        Simulates placing and filling orders.
        Moves orders from EXECUTION_PENDING to ACTIVE_TRADE.
        """
        logger.info(f"Processing pending orders at {self.current_time}...")
        newly_active_trades = []

        if not self.execution_pending_orders.empty:
            filled_orders_indices = []
            for idx, order in self.execution_pending_orders.iterrows():
                # Simulate order fill based on current market data
                if self._simulate_order_fill(order, current_market_data):
                    order['order_status'] = 'FILLED'
                    order['fill_price'] = self._get_simulated_fill_price(order, current_market_data)
                    order['fill_ts'] = self.current_time
                    self.active_trades = pd.concat([self.active_trades, pd.DataFrame([order])], ignore_index=True)
                    newly_active_trades.append(order.to_dict())
                    filled_orders_indices.append(idx)
                    logger.info(f"Order for {order['Ticker']} {order['Strategy_Name']} filled at {order['fill_price']:.2f}.")
                else:
                    # Placeholder for orders that remain pending or are cancelled
                    pass
            
            if filled_orders_indices:
                self.execution_pending_orders.drop(index=filled_orders_indices, inplace=True)
                self.execution_pending_orders.reset_index(drop=True, inplace=True)

        logger.info(f"Processed pending orders. {len(newly_active_trades)} new active trades.")
        return newly_active_trades

    def manage_active_trades(self, current_market_data: pd.DataFrame):
        """
        Manages active trades, including monitoring for exit conditions (stop-loss, profit target) and roll opportunities.
        Moves trades from ACTIVE_TRADE to CLOSED_TRADES.
        """
        logger.info(f"Managing {len(self.active_trades)} active trades at {self.current_time}...")
        closed_trades_indices = []
        rolled_trades_indices = [] # Track trades that are rolled

        if not self.active_trades.empty:
            for idx, trade in self.active_trades.iterrows():
                # 1. Check for Roll Opportunities FIRST
                should_roll, roll_reason, new_strike, new_expiration_str = self._check_roll_conditions(trade, current_market_data)
                if should_roll:
                    # Simulate rolling the trade
                    trade['Selected_Strike'] = new_strike
                    trade['Selected_Expiration'] = new_expiration_str
                    trade['Actual_DTE'] = (pd.to_datetime(new_expiration_str) - self.current_time).days
                    trade['monitor_reason'] = f"Rolled: {roll_reason}"
                    trade['last_monitored_ts'] = self.current_time
                    # For simplicity, assume rolling incurs no additional cost/credit for now, just updates parameters
                    # In a real system, this would involve closing the old and opening a new, with PnL impact
                    rolled_trades_indices.append(idx)
                    logger.info(f"Trade for {trade['Ticker']} {trade['Strategy_Name']} rolled: {roll_reason}. New strike: {new_strike}, New Expiration: {new_expiration_str}")
                    continue # Skip exit checks for this trade, as it was rolled

                # 2. Check for Exit Conditions
                exit_triggered, exit_reason, exit_price = self._check_exit_conditions(trade, current_market_data)
                
                if exit_triggered:
                    trade['order_status'] = 'CLOSED'
                    trade['exit_price'] = exit_price
                    trade['exit_ts'] = self.current_time
                    trade['exit_reason'] = exit_reason
                    trade['pnl'] = self._calculate_pnl(trade)
                    self.closed_trades = pd.concat([self.closed_trades, pd.DataFrame([trade])], ignore_index=True)
                    closed_trades_indices.append(idx)
                    logger.info(f"Trade for {trade['Ticker']} {trade['Strategy_Name']} closed due to {exit_reason}. PnL: {trade['pnl']:.2f}.")
                else:
                    # Placeholder for other adjustments
                    pass
            
            # Remove closed and rolled trades from active_trades
            all_removed_indices = list(set(closed_trades_indices + rolled_trades_indices))
            if all_removed_indices:
                self.active_trades.drop(index=all_removed_indices, inplace=True)
                self.active_trades.reset_index(drop=True, inplace=True)

        logger.info(f"Active trade management complete. {len(closed_trades_indices)} trades closed, {len(rolled_trades_indices)} trades rolled.")

    def _simulate_order_fill(self, order: pd.Series, current_market_data: pd.DataFrame) -> bool:
        """
        Simulates whether an order gets filled.
        For simplicity, assume market orders fill immediately, limit orders fill if price is met.
        """
        ticker_data = current_market_data[current_market_data['Ticker'] == order['Ticker']]
        if ticker_data.empty:
            return False

        # For this conceptual framework, assume all EXECUTION_PENDING orders are filled immediately.
        # In a real system, this would involve checking bid/ask, order type, etc.
        return True

    def _get_simulated_fill_price(self, order: pd.Series, current_market_data: pd.DataFrame) -> float:
        """Simulates the fill price for an order."""
        ticker_data = current_market_data[current_market_data['Ticker'] == order['Ticker']]
        if ticker_data.empty:
            return np.nan
        
        # For simplicity, assume fill at current market price (last_price)
        return ticker_data['last_price'].iloc[0]

    def _check_roll_conditions(self, trade: pd.Series, current_market_data: pd.DataFrame) -> Tuple[bool, str, Optional[float], Optional[str]]:
        """
        Evaluates if an active trade should be rolled based on playbook rules.
        Returns (should_roll, roll_reason, new_strike, new_expiration_date_str).
        """
        ticker_data = current_market_data[current_market_data['Ticker'] == trade['Ticker']]
        if ticker_data.empty:
            return False, "", None, None

        current_price = ticker_data['last_price'].iloc[0]
        strategy_name = trade.get('Strategy_Name', 'UNKNOWN')
        selected_strike = trade.get('Selected_Strike', np.nan)
        actual_dte = trade.get('Actual_DTE', np.nan)
        trade_bias = trade.get('Trade_Bias', 'Neutral')

        # Placeholder for new expiration date (e.g., 30-60 days out)
        new_expiration_date = pd.to_datetime(trade['Selected_Expiration']) + timedelta(days=30)
        new_expiration_date_str = new_expiration_date.strftime('%Y-%m-%d')

        # CSP Roll Logic: If stock approaches/breaches strike, roll out and down
        if strategy_name == 'Cash-Secured Put' and pd.notna(selected_strike) and pd.notna(actual_dte):
            if actual_dte < 21 and current_price < selected_strike * 1.02: # DTE < 21 and price near/below strike
                # Propose rolling down to a lower strike (e.g., 5% lower) and out
                new_strike = selected_strike * 0.95
                return True, "Roll Out and Down (CSP)", new_strike, new_expiration_date_str

        # Buy-Write Roll Logic: If stock rallies significantly, roll out and up
        elif strategy_name == 'Buy-Write' and pd.notna(selected_strike) and pd.notna(actual_dte):
            if actual_dte < 21 and current_price > selected_strike * 1.05: # DTE < 21 and price well above strike
                # Propose rolling up to a higher strike (e.g., 5% higher) and out
                new_strike = selected_strike * 1.05
                return True, "Roll Out and Up (Buy-Write)", new_strike, new_expiration_date_str
            
        # Directional (Long Options/LEAPs) Roll Logic: Roll out to extend time if thesis still valid, or roll up/down in profit
        elif ('Long Call' in strategy_name or 'Long Put' in strategy_name) and pd.notna(actual_dte):
            current_pnl_pct = self._calculate_pnl(trade) / (trade['initial_premium'] * 100) if trade['initial_premium'] > 0 else 0

            if actual_dte < 30 and current_pnl_pct > -0.20: # Option is losing but not too much, and time is running out
                # Roll out to extend time
                return True, "Roll Out (Extend Time)", selected_strike, new_expiration_date_str
            elif current_pnl_pct > 0.50: # In significant profit (50%+)
                # Roll up/down and out to lock in gains and extend
                if trade_bias == 'Bullish': # Long Call
                    new_strike = selected_strike * 1.05 # Roll up
                else: # Long Put
                    new_strike = selected_strike * 0.95 # Roll down
                return True, "Roll Up/Down and Out (Profit Taking)", new_strike, new_expiration_date_str

        return False, "", None, None

    def _check_exit_conditions(self, trade: pd.Series, current_market_data: pd.DataFrame) -> Tuple[bool, str, float]:
        """
        Checks for stop-loss, profit target, or expiration for an active trade.
        Returns (exit_triggered, reason, exit_price).
        """
        ticker_data = current_market_data[current_market_data['Ticker'] == trade['Ticker']]
        if ticker_data.empty:
            return False, np.nan, np.nan

        current_price = ticker_data['last_price'].iloc[0]
        strategy_type = trade.get('Strategy_Type', 'UNKNOWN')
        strategy_name = trade.get('Strategy_Name', 'UNKNOWN')
        selected_strike = trade.get('Selected_Strike', np.nan)
        fill_price = trade.get('fill_price', np.nan)
        initial_premium = trade.get('initial_premium', np.nan)
        actual_dte = trade.get('Actual_DTE', np.nan)
        initial_iv = trade.get('initial_implied_volatility', np.nan)

        current_iv_rank = ticker_data.get('IV_Rank_XS', np.nan) # Assuming current IV Rank is available
        current_directional_bias = ticker_data.get('directional_bias', 'NEUTRAL') # Assuming current_market_data has this
        current_structure_bias = ticker_data.get('structure_bias', 'UNCLEAR') # Assuming current_market_data has this


        # 1. Time-based expiration (Hard Requirement for options)
        if pd.notna(trade.get('Selected_Expiration')) and self.current_time.date() >= pd.to_datetime(trade['Selected_Expiration']).date():
            if strategy_type == 'INCOME':
                if strategy_name == 'Cash-Secured Put':
                    if current_price < selected_strike: # Assigned
                        return True, "Expiration - Assigned", selected_strike
                    else: # Expired OTM
                        return True, "Expiration - OTM", current_price
                elif strategy_name == 'Buy-Write':
                    if current_price > selected_strike: # Assigned
                        return True, "Expiration - Assigned", selected_strike
                    else: # Call expired OTM
                        return True, "Expiration - OTM", current_price
            elif strategy_type == 'DIRECTIONAL' or strategy_type == 'VOLATILITY':
                # For long options, if expired, value is 0 if OTM, intrinsic if ITM
                if 'Long Call' in strategy_name and current_price > selected_strike:
                    return True, "Expiration - ITM", current_price - selected_strike # Intrinsic value
                elif 'Long Put' in strategy_name and current_price < selected_strike:
                    return True, "Expiration - ITM", selected_strike - current_price # Intrinsic value
                else:
                    return True, "Expiration - OTM", 0.0 # Worthless
            return True, "Expiration", current_price # Generic expiration

        # 2. Stop-Loss Logic (CRITICAL for directional, important for others)
        # For long options/debit spreads: 50% loss on premium paid
        if (strategy_type == 'DIRECTIONAL' or strategy_type == 'VOLATILITY') and pd.notna(initial_premium):
            current_option_value = self._get_simulated_option_value(trade, current_price, current_iv_rank) # Pass current_iv_rank
            if current_option_value < 0.5 * initial_premium: # 50% stop-loss on option value
                return True, "Stop-Loss (50% premium)", current_price
        
        # For CSP: if stock falls significantly below strike (e.g., 5% below strike)
        if strategy_name == 'Cash-Secured Put' and pd.notna(selected_strike):
            if current_price < selected_strike * 0.95: # 5% below strike as stop-loss proxy
                return True, "Stop-Loss (stock below strike)", current_price
        
        # For Buy-Write: if stock falls below a predefined stop-loss (e.g., 5% below stock acquisition price)
        if strategy_name == 'Buy-Write' and pd.notna(fill_price):
            if current_price < fill_price * 0.95: # 5% below stock acquisition price
                return True, "Stop-Loss (stock below acquisition)", current_price

        # 3. Profit Target Logic (for long options/debit spreads)
        # For long options/debit spreads: 100% profit on premium paid
        if (strategy_type == 'DIRECTIONAL' or strategy_type == 'VOLATILITY') and pd.notna(initial_premium):
            current_option_value = self._get_simulated_option_value(trade, current_price, current_iv_rank) # Pass current_iv_rank
            if current_option_value > 2.0 * initial_premium: # 100% profit target
                return True, "Profit Target (100% premium)", current_price
        
        # For CSP/Buy-Write: early close if stock rallies significantly (e.g., 5% above strike for CSP, or 5% above target for Buy-Write)
        if strategy_name == 'Cash-Secured Put' and pd.notna(selected_strike):
            if current_price > selected_strike * 1.05: # Stock rallied 5% above strike
                return True, "Profit Target (stock rallied)", current_price
        if strategy_name == 'Buy-Write' and pd.notna(selected_strike):
            if current_price > selected_strike * 1.05: # Stock rallied 5% above strike
                return True, "Profit Target (stock rallied)", current_price

        # 4. Thesis Invalidity / Structure Breaks
        if strategy_name in ['Long Call LEAP', 'Long Put LEAP']:
            # Playbook: "If the investment thesis is invalidated, exit the position."
            # Proxy: If a bullish LEAP enters a strong bearish trend or unclear structure for a sustained period.
            if trade.get('Trade_Bias') == 'Bullish' and (current_directional_bias == 'BEARISH_STRONG' or current_structure_bias == 'UNCLEAR'):
                return True, "Thesis Invalidated (Strong Bearish Shift/Unclear Structure)", current_price
            if trade.get('Trade_Bias') == 'Bearish' and (current_directional_bias == 'BULLISH_STRONG' or current_structure_bias == 'UNCLEAR'):
                return True, "Thesis Invalidated (Strong Bullish Shift/Unclear Structure)", current_price
        
        elif strategy_type == 'DIRECTIONAL' and actual_dte < 90: # Short-Dated Directional
            # Playbook: "If the underlying stock breaks a significant support/resistance level, exit the trade."
            # Proxy: If a bullish short-dated directional enters a strong bearish trend or range-bound structure.
            if trade.get('Trade_Bias') == 'Bullish' and (current_directional_bias == 'BEARISH_STRONG' or current_structure_bias == 'RANGE_BOUND'):
                return True, "Structure Break (Strong Bearish/Range-Bound)", current_price
            if trade.get('Trade_Bias') == 'Bearish' and (current_directional_bias == 'BULLISH_STRONG' or current_structure_bias == 'RANGE_BOUND'):
                return True, "Structure Break (Strong Bullish/Range-Bound)", current_price

        return False, np.nan, np.nan

    def _calculate_pnl(self, trade: pd.Series) -> float:
        """Calculates the PnL for a closed trade."""
        strategy_name = trade.get('Strategy_Name', 'UNKNOWN')
        fill_price = trade.get('fill_price', np.nan)
        exit_price = trade.get('exit_price', np.nan)
        initial_premium = trade.get('initial_premium', np.nan)
        selected_strike = trade.get('Selected_Strike', np.nan)
        last_price_at_scan = trade.get('last_price', np.nan) # Stock price at time of scan

        if pd.isna(fill_price) or pd.isna(exit_price) or pd.isna(initial_premium):
            return np.nan

        if strategy_name == 'Cash-Secured Put':
            if trade['exit_reason'] == 'Assignment':
                # PnL = premium collected - (strike - assigned_stock_price) * 100
                # Assuming exit_price is the assigned stock price (strike)
                return (initial_premium * 100) - ((selected_strike - exit_price) * 100)
            else: # Expiration OTM or early close
                return initial_premium * 100 # Premium collected
        
        elif strategy_name == 'Buy-Write':
            # PnL = (exit_stock_price - initial_stock_price) * 100 + premium collected
            # Assuming fill_price is the stock acquisition price, exit_price is stock sale price (strike if assigned)
            return (exit_price - last_price_at_scan) * 100 + (initial_premium * 100)
        
        elif 'Long Call' in strategy_name or 'Long Put' in strategy_name or 'Debit Spread' in strategy_name:
            # PnL = (exit_option_value - initial_option_value)
            # Simplified: (exit_price - fill_price) * 100 (assuming fill_price is option premium)
            return (exit_price - fill_price) * 100 # Very simplistic, needs option pricing model
        
        return np.nan

    def _get_initial_premium(self, strategy: pd.Series) -> float:
        """Derives the initial premium for an option strategy."""
        # For simplicity, assume 'Mid' from the scan results is the premium for single-leg options
        # For spreads, 'Mid' is the net debit/credit.
        # This needs to be more robust in a real system.
        return strategy.get('Mid', 0.0)

    def _get_simulated_option_value(self, trade: pd.Series, current_price: float, current_iv_rank: float) -> float:
        """
        Simulates the current value of an option for PnL and stop-loss/profit target checks.
        This is a highly simplified model. A real system would use an options pricing model.
        """
        strategy_name = trade.get('Strategy_Name', 'UNKNOWN')
        selected_strike = trade.get('Selected_Strike', np.nan)
        initial_premium = trade.get('initial_premium', np.nan)
        initial_iv = trade.get('initial_implied_volatility', np.nan)
        fill_ts = trade.get('fill_ts', self.current_time)
        actual_dte = trade.get('Actual_DTE', np.nan)

        if pd.isna(selected_strike) or pd.isna(initial_premium) or pd.isna(initial_iv) or pd.isna(actual_dte):
            return initial_premium # Cannot calculate, return initial premium

        # Simulate time decay (theta)
        days_passed = (self.current_time - fill_ts).days
        remaining_dte = max(0, actual_dte - days_passed)
        # Simple linear theta decay
        theta_factor = remaining_dte / actual_dte if actual_dte > 0 else 0

        # Simulate IV change (vega)
        # Assume IV_Rank_XS is a proxy for current IV level relative to historical
        # A higher IV_Rank means IV is currently higher, so option value is higher (for long options)
        # A lower IV_Rank means IV is currently lower, so option value is lower
        # This is a very rough approximation.
        iv_change_factor = 1.0
        if pd.notna(current_iv_rank):
            # For simplicity, let's assume initial_iv is a baseline, and current_iv_rank indicates deviation
            # If current_iv_rank is higher than some baseline (e.g., 50), IV has expanded, vice versa.
            # This needs to be tied to actual IV values, not just rank.
            # For now, a simple linear adjustment based on IV_Rank difference from 50.
            iv_change_factor = 1 + (current_iv_rank - 50) / 100.0 # Rough adjustment

        # Base value from initial premium
        simulated_value = initial_premium * theta_factor * iv_change_factor

        # Add intrinsic value if ITM
        if 'Long Call' in strategy_name:
            intrinsic_value = max(0, current_price - selected_strike)
            simulated_value += intrinsic_value
        elif 'Long Put' in strategy_name:
            intrinsic_value = max(0, selected_strike - current_price)
            simulated_value += intrinsic_value
        elif 'Debit Spread' in strategy_name:
            # For spreads, intrinsic value is more complex, but we can approximate directional movement
            if trade.get('Trade_Bias') == 'Bullish':
                simulated_value += max(0, current_price - selected_strike) * 0.2 # Simplified
            else:
                simulated_value += max(0, selected_strike - current_price) * 0.2 # Simplified
        
        # For income strategies, the option value is the premium collected, which decays
        # This is not used for stop-loss/profit target on the option itself, but on the underlying
        if trade.get('Strategy_Type') == 'INCOME':
            return initial_premium * theta_factor # Only decay premium collected

        return simulated_value

    def get_order_management_summary(self) -> Dict[str, Any]:
        """Provides a summary of orders and trades in different states."""
        return {
            "execution_pending_count": len(self.execution_pending_orders),
            "active_trades_count": len(self.active_trades),
            "closed_trades_count": len(self.closed_trades),
            "execution_pending_orders": self.execution_pending_orders[['Ticker', 'Strategy_Name', 'order_status', 'order_placed_ts']].to_dict(orient='records'),
            "active_trades": self.active_trades[['Ticker', 'Strategy_Name', 'order_status', 'fill_price', 'fill_ts', 'initial_premium', 'initial_implied_volatility', 'Selected_Strike', 'Actual_DTE']].to_dict(orient='records'),
            "closed_trades": self.closed_trades[['Ticker', 'Strategy_Name', 'order_status', 'exit_reason', 'pnl', 'fill_price', 'exit_price', 'initial_premium', 'initial_implied_volatility']].to_dict(orient='records'),
        }
