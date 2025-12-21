import pandas as pd

def summarize_trade(df, trade_id):
    row = df[df["TradeID"] == trade_id]
    if row.empty:
        return f"❌ TradeID {trade_id} not found."
    
    r = row.iloc[0]
    summary = f"""
🔎 Trade Summary for {trade_id}
───────────────────────────────
📈 Symbol: {r.get('Symbol')}
🧠 Strategy: {r.get('Strategy')}
📅 Expiration: {r.get('Expiration')}
🎯 Strike: {r.get('Strike')}
💰 Premium: {r.get('Premium')}

📊 PCS at Entry: {r.get('PCS_Entry')}
📊 PCS Now: {r.get('PCS')}
📉 Drift: {r.get('PCS_Drift'):.2f}
📉 Vega ROC: {r.get('Vega_ROC'):.2f}
💵 ROI: {r.get('Held_ROI%', 'n/a')}%

🏁 Outcome Tag: {r.get('OutcomeTag')}
"""
    return summary.strip()
