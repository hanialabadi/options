"""
Options Pricing Utilities - Black-Scholes Model

Purpose:
    Provides functions to calculate theoretical option prices (fair value)
    and implied volatility using the Black-Scholes model.

Design Principles:
    - Used as a normalization anchor, not a predictive signal.
    - Enables comparison across strikes and expirations.
    - Anchors entry bands for premium assessment.

References:
    - John C. Hull, "Options, Futures, and Other Derivatives"
    - Natenberg, "Option Volatility and Pricing"
"""

import numpy as np
from scipy.stats import norm
import logging

logger = logging.getLogger(__name__)

def black_scholes_price(
    S: float,    # Underlying asset price
    K: float,    # Strike price
    T: float,    # Time to expiration (in years)
    r: float,    # Risk-free interest rate (annual)
    sigma: float, # Implied volatility (annualized)
    option_type: str # 'call' or 'put'
) -> float:
    """
    Calculates the Black-Scholes option price.

    Args:
        S (float): Underlying asset price.
        K (float): Strike price.
        T (float): Time to expiration (in years).
        r (float): Risk-free interest rate (annual, e.g., 0.05 for 5%).
        sigma (float): Implied volatility (annualized, e.g., 0.20 for 20%).
        option_type (str): Type of option, 'call' or 'put'.

    Returns:
        float: The theoretical Black-Scholes price of the option.
    """
    if T <= 0:
        return max(0, S - K) if option_type == 'call' else max(0, K - S)
    if sigma <= 0:
        return max(0, S - K) if option_type == 'call' else max(0, K - S)

    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)

    if option_type == 'call':
        price = S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
    elif option_type == 'put':
        price = K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
    else:
        raise ValueError("option_type must be 'call' or 'put'")

    return price

def calculate_implied_volatility(
    S: float,    # Underlying asset price
    K: float,    # Strike price
    T: float,    # Time to expiration (in years)
    r: float,    # Risk-free interest rate (annual)
    market_price: float, # Market price of the option
    option_type: str, # 'call' or 'put'
    tolerance: float = 1e-5,
    max_iterations: int = 100
) -> float:
    """
    Calculates the implied volatility using the Newton-Raphson method.

    Args:
        S (float): Underlying asset price.
        K (float): Strike price.
        T (float): Time to expiration (in years).
        r (float): Risk-free interest rate (annual).
        market_price (float): The observed market price of the option.
        option_type (str): Type of option, 'call' or 'put'.
        tolerance (float): The desired accuracy for implied volatility.
        max_iterations (int): Maximum number of iterations for the Newton-Raphson method.

    Returns:
        float: The implied volatility (annualized) as a decimal (e.g., 0.20 for 20%).
               Returns NaN if convergence is not achieved or inputs are invalid.
    """
    if market_price <= 0 or T <= 0 or S <= 0 or K <= 0:
        return np.nan

    # Initial guess for volatility
    sigma = 0.5 # Start with 50% volatility

    for i in range(max_iterations):
        price_bs = black_scholes_price(S, K, T, r, sigma, option_type)
        vega = S * norm.pdf((np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))) * np.sqrt(T)

        if vega < 1e-6: # Avoid division by zero or very small vega
            sigma += 0.01 # Nudge sigma if vega is too small
            continue

        diff = price_bs - market_price
        if abs(diff) < tolerance:
            return sigma

        sigma = sigma - diff / vega
        if sigma < 0: # Volatility cannot be negative
            sigma = 0.01 # Reset to a small positive value

    logger.warning(f"Implied volatility calculation did not converge for S={S}, K={K}, T={T}, r={r}, market_price={market_price}, type={option_type}")
    return np.nan

# --- Constants for pipeline use ---
# Default risk-free rate (e.g., current 10-year Treasury yield or Fed Funds rate)
# This should ideally be dynamic, but a constant is a good starting point.
RISK_FREE_RATE = 0.05 # 5%

# Days in a year for time conversion
DAYS_IN_YEAR = 365.0
