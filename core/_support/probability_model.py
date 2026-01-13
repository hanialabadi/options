# probability_model.py
# 🎯 Predictive Model: Estimate Probability of Trade Success (ROI ≥ 50%)

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import classification_report, roc_auc_score
import joblib
import os

# === Step 1: Load Labeled Data (Closed trades only) ===
def load_labeled_data(path):
    df = pd.read_csv(path)
    df = df[df["OutcomeTag"].notna()]  # Only use labeled outcomes
    df = df[df["Held_ROI%"].notna()]   # Ensure ROI is filled
    df["Success"] = (df["Held_ROI%"] >= 50).astype(int)
    return df

# === Step 2: Select Features (fallback to available subset) ===
def build_feature_matrix(df):
    required_features = [
        "PCS", "PCS_SignalScore", "PCS_FinalScore",
        "Vega", "Gamma", "Delta", "Theta",
        "Vega_5D_DriftTail", "Gamma_3D_DriftTail", "PCS_Drift",
        "Chart_CompositeScore", "Days_Held",
        "IVHV_Gap", "IV_Drift", "Skew_Entry", "Kurtosis_Entry"
    ]
    available = [col for col in required_features if col in df.columns]
    if len(available) < 6:
        raise ValueError(f"Too few usable features in closed_log.csv: {available}")
    X = df[available].fillna(0)
    y = df["Success"]
    return X, y

# === Step 3: Train Model ===
def train_probability_model(df):
    X, y = build_feature_matrix(df)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)

    model = GradientBoostingClassifier(n_estimators=150, learning_rate=0.05, max_depth=4, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]

    print("🔍 Classification Report:")
    print(classification_report(y_test, y_pred))
    print("ROC AUC:", roc_auc_score(y_test, y_prob))

    joblib.dump(model, "trade_success_model.pkl")
    print("✅ Model saved to trade_success_model.pkl")
    return model

# === Step 4: Predict on Live Trades ===
def predict_success_probabilities(df_live):
    if not os.path.exists("trade_success_model.pkl"):
        print("⚠️ No model file found — skipping Success_Prob")
        df_live["Success_Prob"] = 0.5
        return df_live

    model = joblib.load("trade_success_model.pkl")
    model_features = getattr(model, "feature_names_in_", None)

    usable = [col for col in model_features if col in df_live.columns] if model_features is not None else []
    if len(usable) < 6:
        print(f"⚠️ Not enough usable features in live data: {usable}")
        df_live["Success_Prob"] = 0.5
        return df_live

    X_live = df_live[usable].fillna(0)
    df_live["Success_Prob"] = model.predict_proba(X_live)[:, 1]
    return df_live

if __name__ == "__main__":
    df_closed = load_labeled_data("/Users/haniabadi/Documents/Windows/Optionrec/closed_log.csv")
    train_probability_model(df_closed)
