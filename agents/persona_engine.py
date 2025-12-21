# %% 📚 Imports
import pandas as pd
import numpy as np

# %% 🧠 Trader Persona Class
class TraderPersona:
    def __init__(self, name, max_loss_pct, max_gain_pct, scaling_factor, pcs_threshold):
        self.name = name
        self.max_loss_pct = max_loss_pct
        self.max_gain_pct = max_gain_pct
        self.scaling_factor = scaling_factor
        self.pcs_threshold = pcs_threshold

    def should_scale_in(self, pcs: float) -> bool:
        return pcs >= self.pcs_threshold

    def should_exit(self, pcs: float, price_change_pct: float) -> bool:
        return pcs < self.pcs_threshold or price_change_pct < self.max_loss_pct

    def should_trim(self, price_change_pct: float) -> bool:
        return price_change_pct >= self.max_gain_pct

# %% 📈 Optional PCS Calculation (if needed)
def calculate_pcs(row: pd.Series) -> float:
    score = 0
    score += min(row.get('Gamma', 0) * 1000, 25)
    score += min(row.get('Vega', 0) * 100, 20)
    score += row.get('Delta', 0) * 50
    score += min(row.get('IV', 0) / max(row.get('HV', 1), 0.1), 5)
    score += row.get('PCS', 0)
    return score

# %% 🧩 Trade Management Engine
def manage_position(df: pd.DataFrame, trader_persona: TraderPersona) -> list[dict]:
    """
    Applies persona logic across open trades. Returns list of action dicts.
    """
    recommendations = []

    for _, row in df.iterrows():
        pcs = row['PCS']
        price_change_pct = (row['Last'] - row['Entry_Price']) / max(row['Entry_Price'], 0.01) * 100

        if trader_persona.should_scale_in(pcs):
            recommendations.append({
                "Symbol": row['Symbol'],
                "Strategy": "Scale In",
                "PCS": pcs,
                "Action": "Scale In",
                "Position Size": 5000 * trader_persona.scaling_factor,
                "Rationale": f"{trader_persona.name}: PCS ≥ threshold"
            })

        if trader_persona.should_exit(pcs, price_change_pct):
            recommendations.append({
                "Symbol": row['Symbol'],
                "Strategy": "Exit",
                "PCS": pcs,
                "Action": "Exit",
                "Position Size": row['Quantity'] * row['Last'],
                "Rationale": f"{trader_persona.name}: PCS drop or large drawdown"
            })

        if trader_persona.should_trim(price_change_pct):
            recommendations.append({
                "Symbol": row['Symbol'],
                "Strategy": "Trim",
                "PCS": pcs,
                "Action": "Trim",
                "Position Size": 2500,
                "Rationale": f"{trader_persona.name}: Gain exceeded"
            })

    return recommendations

# %% 🧪 Run Standalone Example
if __name__ == "__main__":
    df = pd.DataFrame({
        'Symbol': ['META250725C295', 'AAPL250801P210'],
        'Underlying': ['META', 'AAPL'],
        'Entry_Price': [5.5, 3.0],
        'Last': [6.29, 2.60],
        'Quantity': [1, 1],
        'PCS': [75, 58]
    })

    aggressive = TraderPersona(name="Aggressive", max_loss_pct=-25, max_gain_pct=50, scaling_factor=1.2, pcs_threshold=70)
    recs = manage_position(df, aggressive)

    for r in recs:
        print(r)
