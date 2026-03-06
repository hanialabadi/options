# Authoritative BUY_WRITE Doctrine Action Model

## 1. Governance Overview
This doctrine defines the deterministic, threshold-based control logic for managing `BUY_WRITE` positions. It is designed for automated execution within the Cycle 3 Recommendation Engine.

*   **Default State**: `HOLD`
*   **Primary Anchor**: `Underlying_Price_Entry` (Frozen at inception)
*   **Risk Metric**: `Price_Drift_Structural` (Spot - Anchor)

---

## 2. Canonical Thresholds

| Constant | Value | Description |
| :--- | :--- | :--- |
| `DRAWDOWN_LIMIT` | `-0.20` | 20% breach of frozen stock anchor |
| `DTE_THRESHOLD` | `7` | Days to expiration for tactical maintenance |
| `DELTA_THRESHOLD` | `0.70` | Short call delta for ITM defense |
| `YIELD_BUFFER` | `0.02` | 2% safety margin over financing cost |
| `PROFIT_THRESHOLD` | `0.05` | Remaining premium below which profit is "complete" |

---

## 3. Action Logic (Implementation Specification)

```python
def evaluate_buy_write_doctrine(position, financing_cost):
    """
    Deterministic action resolver for BUY_WRITE positions.
    
    Args:
        position: Object containing current spot, anchor, DTE, delta, and premium.
        financing_cost: Annualized cost of capital (e.g., 0.05 for 5%).
    """
    
    # 1. CRITICAL: Structural Drawdown (Hard Stop)
    # Rule: Price_Drift_Structural <= -0.20 * Underlying_Price_Entry
    if position.Price_Drift_Structural <= (-0.20 * position.Underlying_Price_Entry):
        return "EXIT_REQUIRED", "Structural drawdown limit breached (-20%)"

    # 2. Yield & Financing Analysis
    # Annualized Yield = (Remaining_Premium / Current_Value) * (365 / DTE)
    current_yield = (position.Remaining_Premium / position.Current_Value) * (365 / position.DTE)
    
    if current_yield < financing_cost:
        # Deterministic Yield Failure Resolution
        if can_roll_for_yield(position, target_yield=financing_cost + 0.02):
            return "ROLL_ELIGIBLE", "Yield below financing; qualifying roll available"
        else:
            return "EXIT_REQUIRED", "Negative carry; no qualifying roll available"

    # 3. Tactical Maintenance: Gamma Risk
    if position.DTE < 7:
        return "ROLL_ELIGIBLE", "Time-based maintenance (DTE < 7)"

    # 4. Tactical Maintenance: ITM Defense
    if position.Short_Call_Delta > 0.70:
        return "ROLL_ELIGIBLE", "ITM defense (Delta > 0.70)"

    # 5. Profit Completion (Awaiting Assignment)
    if position.Spot >= position.Short_Call_Strike and position.Remaining_Premium < 0.05:
        return "HOLD", "Profit complete; awaiting assignment"

    # 6. Governance Default
    return "HOLD", "Position within sensitivity envelope"
```

---

## 4. State Transition Table

| Current State | Trigger | Target State | Action |
| :--- | :--- | :--- | :--- |
| **VALID_HOLD** | `Price_Drift_Structural <= -20%` | **EXIT_REQUIRED** | EXIT |
| **VALID_HOLD** | `DTE < 7` OR `Delta > 0.70` | **ROLL_ELIGIBLE** | ROLL |
| **VALID_HOLD** | `Yield < Financing` AND `Roll_Yield >= Req` | **ROLL_ELIGIBLE** | ROLL |
| **VALID_HOLD** | `Yield < Financing` AND `Roll_Yield < Req` | **EXIT_REQUIRED** | EXIT |
| **ANY** | `Is_Active == FALSE` | **ARCHIVED** | NONE |

---

## 5. Compliance Statement
This model is strictly data-derived. It prohibits the use of intent inference, market forecasts, or discretionary overrides. All actions are justified solely by the relationship between the current market state and the frozen inception anchors.
