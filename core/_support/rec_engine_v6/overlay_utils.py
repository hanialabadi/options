import pandas as pd

def compute_gem_score(row):
    pcs = row.get("PCS", 0) or 0
    vega = row.get("Vega", 0) or 0
    gamma = row.get("Gamma", 0) or 0
    iv_gap = row.get("IVHV_Gap_Entry", 0) or 0
    chart_score = row.get("Chart_CompositeScore", 50) or 50

    # Adjust weights as needed
    score = 0.4 * pcs + 0.2 * chart_score + 0.2 * vega * 100 + 0.2 * gamma * 100

    tags = []
    if pcs >= 80:
        tags.append("✅ GEM Tier 1")
    elif pcs >= 70:
        tags.append("📘 GEM Tier 2")
    else:
        tags.append("⚠️ GEM Tier 3")

    if vega < 0.15 or iv_gap < 3:
        tags.append("⚠️ Vega/IV Weakness")

    return round(score, 2), ", ".join(tags)

def enrich_gem_overlay(df):
    df["GEM_Score"], df["GEM_Tags"] = zip(*df.apply(compute_gem_score, axis=1))
    return df
